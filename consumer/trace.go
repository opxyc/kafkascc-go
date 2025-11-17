package consumer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// trace id propagation via context
type ctxKey string

const TraceIDKey ctxKey = "trace_id"

// traceID returns a deterministic unique identifier for a Kafka message
// composed of topic-partition-offset. This lets us correlate lifecycle logs
// across fetch, processing, retry, and commit.
func traceID(topic string, m kafka.Message) string {
	return fmt.Sprintf("%s-%d-%d", topic, m.Partition, m.Offset)
}

// withTraceID stores a trace id into the context
func withTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}
