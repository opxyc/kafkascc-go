package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/opxyc/kafkascc-go/consumer"
	logger "github.com/opxyc/kafkascc-go/logger/log"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// DBHTTPHandler connects to MySQL, queries the limit, and forwards the enhanced message to an API
type DBHTTPHandler struct {
	db      *gorm.DB
	client  *http.Client
	baseURL string
	log     consumer.Logger
	topic   string
}

// UntitledTable represents the structure of the untitled_table
type UntitledTable struct {
	ID    uint `gorm:"primaryKey"`
	Limit int  `gorm:"column:limit"`
}

// TableName specifies the table name for GORM
func (UntitledTable) TableName() string {
	return "untitled_table"
}

// NewDBHTTPHandler creates a new DBHTTPHandler with MySQL and HTTP client
func NewDBHTTPHandler(apiBaseURL string) *DBHTTPHandler {
	h := &DBHTTPHandler{
		client:  &http.Client{Timeout: 5 * time.Second},
		baseURL: strings.TrimRight(apiBaseURL, "/"),
		log:     logger.New(log.New(os.Stdout, "", 0)).With("component", "handler", "type", "db_http"),
	}

	// MySQL connection string
	dsn := "root:password@tcp(localhost:3306)/default?charset=utf8mb4&parseTime=True&loc=Local"

	// Connect to MySQL
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		h.log.Error("db-connection-failed", "event", "failed_to_connect_to_database", "error", err)
		// Return the handler even if connection fails, it will be handled in Handle method
		return h
	}

	h.db = db
	h.log.Info("db-connection-success", "event", "connected_to_database")

	return h
}

// SetTopic sets the topic for the handler
func (h *DBHTTPHandler) SetTopic(topic string) {
	h.topic = topic
	h.log = h.log.With("topic", topic)
}

// Topic returns the current topic
func (h *DBHTTPHandler) Topic() string {
	return h.topic
}

// MessagePayload represents the enhanced message structure
// Modify this to match your expected API payload structure
type MessagePayload struct {
	OriginalMessage json.RawMessage `json:"original_message"`
	LimitValue      int             `json:"limit_value"`
}

// Handle processes the message by querying the database and forwarding to the API
func (h *DBHTTPHandler) Handle(ctx context.Context, message []byte) error {
	// 1. Query the database for the limit value
	var limitValue int
	if h.db != nil {
		var result UntitledTable
		err := h.db.WithContext(ctx).First(&result).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			h.log.Error("db-query-failed",
				"event", "failed_to_query_database",
				"error", err.Error(),
				"trace_id", consumer.TIDFromCtx(ctx))
			return err
		}
		limitValue = result.Limit
	}

	// 2. Create enhanced payload with original message and limit value
	enhancedPayload := MessagePayload{
		OriginalMessage: message,
		LimitValue:      limitValue,
	}

	payloadBytes, err := json.Marshal(enhancedPayload)
	if err != nil {
		h.log.Error("json-marshal-failed",
			"event", "failed_to_marshal_enhanced_payload",
			"error", err.Error(),
			"trace_id", consumer.TIDFromCtx(ctx))
		return err
	}

	// 3. Forward to the API
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		h.baseURL+"/events",
		bytes.NewReader(payloadBytes),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return ErrUnavailable
	}

	return nil
}

// Close closes the database connection
func (h *DBHTTPHandler) Close() error {
	if h.db == nil {
		return nil
	}

	sqlDB, err := h.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
