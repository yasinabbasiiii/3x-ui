package job

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/mhsanaei/3x-ui/v2/config"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mhsanaei/3x-ui/v2/web/service"
	"github.com/mhsanaei/3x-ui/v2/web/websocket"
	"github.com/mhsanaei/3x-ui/v2/xray"

	"github.com/valyala/fasthttp"
)

// XrayTrafficJob collects and processes traffic statistics from Xray, updating the database and optionally informing external APIs.
type XrayTrafficJob struct {
	settingService  service.SettingService
	xrayService     service.XrayService
	inboundService  service.InboundService
	outboundService service.OutboundService
	mu              sync.Mutex
	nextRunAt       int64
	scheduleOffset  int
	scheduleIntv    int
}

// NewXrayTrafficJob creates a new traffic collection job instance.
func NewXrayTrafficJob() *XrayTrafficJob {
	return &XrayTrafficJob{
		nextRunAt:      -1,
		scheduleOffset: -1,
		scheduleIntv:   -1,
	}
}

// Run collects traffic statistics from Xray and updates the database, triggering restart if needed.
func (j *XrayTrafficJob) Run() {
	interval, offset := config.GetTrafficSaveSchedule()
	if interval <= 0 {
		return
	}

	now := time.Now().Unix()
	shouldRun := false
	j.mu.Lock()
	// Recalculate schedule when env-based values change or this is first run.
	if j.nextRunAt < 0 || j.scheduleIntv != interval || j.scheduleOffset != offset {
		j.scheduleIntv = interval
		j.scheduleOffset = offset
		j.nextRunAt = nextScheduledUnix(now, int64(interval), int64(offset))
	}
	if now >= j.nextRunAt {
		shouldRun = true
		// Advance to the next future slot; if delayed, skip missed slots safely.
		for j.nextRunAt <= now {
			j.nextRunAt += int64(interval)
		}
	}
	j.mu.Unlock()
	if !shouldRun {
		return
	}

	if !j.xrayService.IsXrayRunning() {
		return
	}
	traffics, clientTraffics, err := j.xrayService.GetXrayTraffic()
	if err != nil {
		return
	}
	logger.Infof("Traffic Count: %d", len(traffics))
	logger.Infof("ClientTraffic Count: %d", len(clientTraffics))

	err, needRestart0 := j.inboundService.AddTraffic(traffics, clientTraffics)
	if err != nil {
		logger.Warning("add inbound traffic failed:", err)
		j.notifyTrafficSaveDBError("inbound", err)
	}
	err, needRestart1 := j.outboundService.AddTraffic(traffics, clientTraffics)
	if err != nil {
		logger.Warning("add outbound traffic failed:", err)
		j.notifyTrafficSaveDBError("outbound", err)
	}
	if ExternalTrafficInformEnable, err := j.settingService.GetExternalTrafficInformEnable(); ExternalTrafficInformEnable {
		j.informTrafficToExternalAPI(traffics, clientTraffics)
	} else if err != nil {
		logger.Warning("get ExternalTrafficInformEnable failed:", err)
	}
	if needRestart0 || needRestart1 {
		j.xrayService.SetToNeedRestart()
	}

	// Get online clients and last online map for real-time status updates
	onlineClients := j.inboundService.GetOnlineClients()
	lastOnlineMap, err := j.inboundService.GetClientsLastOnline()
	if err != nil {
		logger.Warning("get clients last online failed:", err)
		lastOnlineMap = make(map[string]int64)
	}

	// Fetch updated inbounds from database with accumulated traffic values
	// This ensures frontend receives the actual total traffic, not just delta values
	updatedInbounds, err := j.inboundService.GetAllInbounds()
	if err != nil {
		logger.Warning("get all inbounds for websocket failed:", err)
	}

	updatedOutbounds, err := j.outboundService.GetOutboundsTraffic()
	if err != nil {
		logger.Warning("get all outbounds for websocket failed:", err)
	}

	// Broadcast traffic update via WebSocket with accumulated values from database
	trafficUpdate := map[string]any{
		"traffics":       traffics,
		"clientTraffics": clientTraffics,
		"onlineClients":  onlineClients,
		"lastOnlineMap":  lastOnlineMap,
	}
	websocket.BroadcastTraffic(trafficUpdate)

	// Broadcast full inbounds update for real-time UI refresh
	if updatedInbounds != nil {
		websocket.BroadcastInbounds(updatedInbounds)
	}

	if updatedOutbounds != nil {
		websocket.BroadcastOutbounds(updatedOutbounds)
	}

}

func nextScheduledUnix(now, interval, offset int64) int64 {
	if interval <= 0 {
		return now
	}
	if now < offset {
		return offset
	}
	delta := now - offset
	rem := delta % interval
	if rem == 0 {
		return now
	}
	return now + (interval - rem)
}

func (j *XrayTrafficJob) informTrafficToExternalAPI(inboundTraffics []*xray.Traffic, clientTraffics []*xray.ClientTraffic) {
	informURL, err := j.settingService.GetExternalTrafficInformURI()
	if err != nil {
		logger.Warning("get ExternalTrafficInformURI failed:", err)
		return
	}
	requestBody, err := json.Marshal(map[string]any{"clientTraffics": clientTraffics, "inboundTraffics": inboundTraffics})
	if err != nil {
		logger.Warning("parse client/inbound traffic failed:", err)
		return
	}
	request := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(request)
	request.Header.SetMethod("POST")
	request.Header.SetContentType("application/json; charset=UTF-8")
	request.SetBody([]byte(requestBody))
	request.SetRequestURI(informURL)
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(response)
	if err := fasthttp.Do(request, response); err != nil {
		logger.Warning("POST ExternalTrafficInformURI failed:", err)
	}
}

func (j *XrayTrafficJob) notifyTrafficSaveDBError(scope string, saveErr error) {
	if saveErr == nil {
		return
	}

	botToken := config.GetTrafficDBErrorTgBotToken()
	targetID := config.GetTrafficDBErrorTgUserID()
	if botToken == "" || targetID == "" {
		return
	}

	serverName, err := os.Hostname()
	if err != nil || serverName == "" {
		serverName = "unknown"
	}

	msg := fmt.Sprintf(
		"Traffic Save DB Error\nServer: %s\nScope: %s\nError: %s",
		serverName,
		scope,
		saveErr.Error(),
	)

	form := url.Values{}
	form.Set("chat_id", targetID)
	form.Set("text", msg)

	request := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(request)
	request.Header.SetMethod(fasthttp.MethodPost)
	request.Header.SetContentType("application/x-www-form-urlencoded")
	request.SetBodyString(form.Encode())
	request.SetRequestURI(fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", url.PathEscape(botToken)))

	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(response)

	if err := fasthttp.DoTimeout(request, response, 10*time.Second); err != nil {
		logger.Warning("send traffic db error to telegram failed:", err)
		return
	}
	if response.StatusCode() < 200 || response.StatusCode() >= 300 {
		logger.Warningf(
			"send traffic db error to telegram failed: status=%d body=%s",
			response.StatusCode(),
			string(response.Body()),
		)
	}
}
