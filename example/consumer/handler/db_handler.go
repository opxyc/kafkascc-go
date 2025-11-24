package handler

// import (
// 	"context"
// 	"log"
// 	"os"

// 	"github.com/opxyc/kafkascc-go/consumer"
// 	logger "github.com/opxyc/kafkascc-go/logger/log"
// 	"gorm.io/driver/mysql"
// 	"gorm.io/gorm"
// )

// // DBHandler connects to MySQL and queries the untitled_table
// type DBHandler struct {
// 	db   *gorm.DB
// 	log  consumer.Logger
// 	topic string
// }

// // UntitledTable represents the structure of the untitled_table
// type UntitledTable struct {
// 	ID    uint `gorm:"primaryKey"`
// 	Limit int  `gorm:"column:limit"`
// }

// // TableName specifies the table name for GORM
// func (UntitledTable) TableName() string {
// 	return "untitled_table"
// }

// // NewDBHandler creates a new DBHandler with MySQL connection
// func NewDBHandler() *DBHandler {
// 	h := &DBHandler{
// 		log: logger.New(log.New(os.Stdout, "", 0)).With("component", "handler", "type", "db"),
// 	}

// 	// MySQL connection string
// 	dsn := "root:password@tcp(localhost:3306)/default?charset=utf8mb4&parseTime=True&loc=Local"

// 	// Connect to MySQL
// 	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
// 	if err != nil {
// 		h.log.Error("db-connection-failed", "event", "failed_to_connect_to_database", "error", err)
// 		// Return the handler even if connection fails, it will be handled in Handle method
// 		return h
// 	}

// 	h.db = db
// 	h.log.Info("db-connection-success", "event", "connected_to_database")

// 	return h
// }

// // SetTopic sets the topic for the handler
// func (h *DBHandler) SetTopic(topic string) {
// 	h.topic = topic
// 	h.log = h.log.With("topic", topic)
// }

// // Topic returns the current topic
// func (h *DBHandler) Topic() string {
// 	return h.topic
// }

// // Handle processes the message by querying the database
// func (h *DBHandler) Handle(ctx context.Context, message []byte) error {
// 	if h.db == nil {
// 		h.log.Error("db-not-initialized", "event", "database_not_initialized")
// 		return nil // Return nil to avoid retrying
// 	}

// 	var result UntitledTable

// 	// Query the first row from untitled_table
// 	err := h.db.WithContext(ctx).First(&result).Error
// 	if err != nil {
// 		if err == gorm.ErrRecordNotFound {
// 			h.log.Info("no-records-found", "event", "no_records_in_table")
// 			return nil
// 		}
// 		h.log.Error("db-query-failed",
// 			"event", "failed_to_query_database",
// 			"error", err.Error(),
// 			"trace_id", consumer.TIDFromCtx(ctx))
// 		return nil // Return nil to avoid retrying on database errors
// 	}

// 	h.log.Info("db-query-success",
// 		"event", "database_query_successful",
// 		"limit_value", result.Limit,
// 		"trace_id", consumer.TIDFromCtx(ctx))

// 	return nil
// }

// // Close closes the database connection
// func (h *DBHandler) Close() error {
// 	if h.db == nil {
// 		return nil
// 	}

// 	sqlDB, err := h.db.DB()
// 	if err != nil {
// 		return err
// 	}
// 	return sqlDB.Close()
// }
