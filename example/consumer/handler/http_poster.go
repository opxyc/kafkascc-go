package handler

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/opxyc/kafkascc-go/consumer"
	logger "github.com/opxyc/kafkascc-go/logger/log"
)

// APIPostHandler forwards messages to an external HTTP API
type APIPostHandler struct {
	client  *http.Client
	baseURL string
	topic   string
	log     consumer.Logger
}

func NewAPIPostHandler(baseURL string, l consumer.Logger) *APIPostHandler {
	if l == nil {
		l = logger.New(log.New(os.Stdout, "", 0))
	}
	return &APIPostHandler{
		client:  &http.Client{Timeout: 5 * time.Second},
		baseURL: strings.TrimRight(baseURL, "/"),
		log:     l.With("component", "handler", "type", "api_post"),
	}
}

func (h *APIPostHandler) SetTopic(topic string) {
	h.topic = topic
}

func (h *APIPostHandler) Topic() string {
	return h.topic
}

func (h *APIPostHandler) Handle(ctx context.Context, message []byte) error {
	// Example: perform a POST to base URL; replace path, method, body as needed.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+"/events", bytes.NewReader(message))
	if err != nil {
		h.log.Error("handler-request-build", "event", "handler_request_build", "topic", h.topic, "err", err.Error(), "trace_id", consumer.TIDFromCtx(ctx))
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		// Network/DNS/timeout errors => API unavailable
		h.log.Error("handler-network-error", "event", "handler_network_error", "topic", h.topic, "err", err.Error(), "trace_id", consumer.TIDFromCtx(ctx))
		return ErrUnavailable
	}

	defer resp.Body.Close()

	// Treat any non-2xx as unavailable to trigger coordinator retry/backoff
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		h.log.Error("handler-non-2xx", "event", "handler_non_2xx", "topic", h.topic, "status", resp.StatusCode, "trace_id", consumer.TIDFromCtx(ctx))
		return ErrUnavailable
	}

	h.log.Info("handler-ok", "event", "handler_ok", "topic", h.topic, "status", resp.StatusCode, "size", len(message), "trace_id", consumer.TIDFromCtx(ctx))
	return nil
}

func (h *APIPostHandler) Close() error {
	return nil
}
