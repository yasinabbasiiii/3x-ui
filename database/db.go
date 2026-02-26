// Package database provides database initialization, migration, and management utilities
// for the 3x-ui panel using GORM with SQLite.
package database

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"slices"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/mhsanaei/3x-ui/v2/config"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/util/crypto"
	"github.com/mhsanaei/3x-ui/v2/xray"

	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB
var xuiDB *gorm.DB
var xuiMySQLEnabled bool

const (
	defaultUsername = "admin"
	defaultPassword = "admin"
)

func initPanelModels() error {
	models := []any{
		&model.User{},
		&model.OutboundTraffics{},
		&model.Setting{},
		&model.InboundClientIps{},
		&model.HistoryOfSeeders{},
	}
	for _, model := range models {
		if err := db.AutoMigrate(model); err != nil {
			log.Printf("Error auto migrating model: %v", err)
			return err
		}
	}
	return nil
}

func initXUIModels() error {
	if xuiDB == nil {
		return errors.New("xui database is not initialized")
	}
	models := []any{
		&model.Inbound{},
		&xray.ClientTraffic{},
	}
	for _, model := range models {
		if err := xuiDB.AutoMigrate(model); err != nil {
			log.Printf("Error auto migrating xui model: %v", err)
			return err
		}
	}
	return nil
}

// bootstrapXUIDataFromSQLite copies existing x-ui runtime tables from SQLite to MySQL once,
// when MySQL is enabled and target tables are empty.
func bootstrapXUIDataFromSQLite() error {
	if !xuiMySQLEnabled || xuiDB == nil || db == nil {
		return nil
	}

	var inboundCount int64
	if err := xuiDB.Model(&model.Inbound{}).Count(&inboundCount).Error; err != nil {
		return err
	}
	var trafficCount int64
	if err := xuiDB.Model(&xray.ClientTraffic{}).Count(&trafficCount).Error; err != nil {
		return err
	}
	if inboundCount > 0 || trafficCount > 0 {
		return nil
	}

	if !db.Migrator().HasTable(&model.Inbound{}) || !db.Migrator().HasTable(&xray.ClientTraffic{}) {
		return nil
	}

	var inbounds []model.Inbound
	if err := db.Model(&model.Inbound{}).Find(&inbounds).Error; err != nil {
		return err
	}
	var traffics []xray.ClientTraffic
	if err := db.Model(&xray.ClientTraffic{}).Find(&traffics).Error; err != nil {
		return err
	}

	if len(inbounds) > 0 {
		if err := xuiDB.Create(&inbounds).Error; err != nil {
			return err
		}
	}
	if len(traffics) > 0 {
		if err := xuiDB.Create(&traffics).Error; err != nil {
			return err
		}
	}
	return nil
}

// ensureMySQLDatabase creates the target database if it does not exist.
func ensureMySQLDatabase(dsn string) error {
	cfg, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		return err
	}
	if cfg.DBName == "" {
		return nil
	}

	dbName := cfg.DBName
	cfg.DBName = ""
	sqlDB, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	escapedDBName := strings.ReplaceAll(dbName, "`", "``")
	_, err = sqlDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", escapedDBName))
	return err
}

// initUser creates a default admin user if the users table is empty.
func initUser() error {
	empty, err := isTableEmpty("users")
	if err != nil {
		log.Printf("Error checking if users table is empty: %v", err)
		return err
	}
	if empty {
		hashedPassword, err := crypto.HashPasswordAsBcrypt(defaultPassword)

		if err != nil {
			log.Printf("Error hashing default password: %v", err)
			return err
		}

		user := &model.User{
			Username: defaultUsername,
			Password: hashedPassword,
		}
		return db.Create(user).Error
	}
	return nil
}

// runSeeders migrates user passwords to bcrypt and records seeder execution to prevent re-running.
func runSeeders(isUsersEmpty bool) error {
	empty, err := isTableEmpty("history_of_seeders")
	if err != nil {
		log.Printf("Error checking if users table is empty: %v", err)
		return err
	}

	if empty && isUsersEmpty {
		hashSeeder := &model.HistoryOfSeeders{
			SeederName: "UserPasswordHash",
		}
		return db.Create(hashSeeder).Error
	} else {
		var seedersHistory []string
		db.Model(&model.HistoryOfSeeders{}).Pluck("seeder_name", &seedersHistory)

		if !slices.Contains(seedersHistory, "UserPasswordHash") && !isUsersEmpty {
			var users []model.User
			db.Find(&users)

			for _, user := range users {
				hashedPassword, err := crypto.HashPasswordAsBcrypt(user.Password)
				if err != nil {
					log.Printf("Error hashing password for user '%s': %v", user.Username, err)
					return err
				}
				db.Model(&user).Update("password", hashedPassword)
			}

			hashSeeder := &model.HistoryOfSeeders{
				SeederName: "UserPasswordHash",
			}
			return db.Create(hashSeeder).Error
		}
	}

	return nil
}

// isTableEmpty returns true if the named table contains zero rows.
func isTableEmpty(tableName string) (bool, error) {
	var count int64
	err := db.Table(tableName).Count(&count).Error
	return count == 0, err
}

// InitDB sets up the database connection, migrates models, and runs seeders.
func InitDB(dbPath string) error {
	dir := path.Dir(dbPath)
	err := os.MkdirAll(dir, fs.ModePerm)
	if err != nil {
		return err
	}

	var gormLogger logger.Interface

	if config.IsDebug() {
		gormLogger = logger.Default
	} else {
		gormLogger = logger.Discard
	}

	c := &gorm.Config{
		Logger: gormLogger,
	}
	db, err = gorm.Open(sqlite.Open(dbPath), c)
	if err != nil {
		return err
	}

	// SQLite keeps core panel tables.
	if err := initPanelModels(); err != nil {
		return err
	}

	// inbounds + client_traffics may optionally live in MySQL.
	xuiDB = db
	xuiMySQLEnabled = false
	if dsn := config.GetXUIMySQLDSN(); dsn != "" {
		if err := ensureMySQLDatabase(dsn); err != nil {
			return err
		}
		xuiDB, err = gorm.Open(mysql.Open(dsn), c)
		if err != nil {
			return err
		}
		xuiMySQLEnabled = true
	}

	if err := initXUIModels(); err != nil {
		return err
	}
	if err := bootstrapXUIDataFromSQLite(); err != nil {
		return err
	}

	isUsersEmpty, err := isTableEmpty("users")
	if err != nil {
		return err
	}

	if err := initUser(); err != nil {
		return err
	}
	return runSeeders(isUsersEmpty)
}

// CloseDB closes the database connection if it exists.
func CloseDB() error {
	var closeErr error

	if xuiDB != nil && xuiDB != db {
		sqlDB, err := xuiDB.DB()
		if err != nil {
			return err
		}
		if err := sqlDB.Close(); err != nil {
			closeErr = err
		}
	}

	if db != nil {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		if err := sqlDB.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// GetDB returns the global GORM database instance.
func GetDB() *gorm.DB {
	return db
}

// GetXUIDB returns the database for inbounds/client_traffics.
func GetXUIDB() *gorm.DB {
	if xuiDB != nil {
		return xuiDB
	}
	return db
}

// IsXUIMySQLEnabled reports whether inbounds/client_traffics are stored in MySQL.
func IsXUIMySQLEnabled() bool {
	return xuiMySQLEnabled
}

// IsNotFound checks if the given error is a GORM record not found error.
func IsNotFound(err error) bool {
	return err == gorm.ErrRecordNotFound
}

// IsSQLiteDB checks if the given file is a valid SQLite database by reading its signature.
func IsSQLiteDB(file io.ReaderAt) (bool, error) {
	signature := []byte("SQLite format 3\x00")
	buf := make([]byte, len(signature))
	_, err := file.ReadAt(buf, 0)
	if err != nil {
		return false, err
	}
	return bytes.Equal(buf, signature), nil
}

// Checkpoint performs a WAL checkpoint on the SQLite database to ensure data consistency.
func Checkpoint() error {
	// Update WAL
	err := db.Exec("PRAGMA wal_checkpoint;").Error
	if err != nil {
		return err
	}
	return nil
}

// ValidateSQLiteDB opens the provided sqlite DB path with a throw-away connection
// and runs a PRAGMA integrity_check to ensure the file is structurally sound.
// It does not mutate global state or run migrations.
func ValidateSQLiteDB(dbPath string) error {
	if _, err := os.Stat(dbPath); err != nil { // file must exist
		return err
	}
	gdb, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		return err
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	var res string
	if err := gdb.Raw("PRAGMA integrity_check;").Scan(&res).Error; err != nil {
		return err
	}
	if res != "ok" {
		return errors.New("sqlite integrity check failed: " + res)
	}
	return nil
}
