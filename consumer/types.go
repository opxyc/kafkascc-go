package consumer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Logger defines the behavior of the logger used across the app.
// It is intentionally minimal and zap-like without external deps.
type Logger interface {
	// With returns a child logger with additional bound fields.
	With(args ...any) Logger
	// Info logs an info level message with structured key-value pairs.
	Info(msg string, args ...any)
	// Error logs an error level message with structured key-value pairs.
	Error(msg string, args ...any)
}

// Reader abstracts the subset of kafka.Reader used by TopicConsumer for testability.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type MessageHandler interface {
	Handle(ctx context.Context, message []byte) error
	Topic() string
	Close() error
}

// TopicSetter allows components to inject the Kafka topic into handlers that care about it.
// Handlers that need the topic should implement this optional interface.
type TopicSetter interface {
	SetTopic(string)
}

// PausePredicate reports whether an error should pause consumption and trigger retries.
type PausePredicate func(error) bool

// MessageCommitter abstracts commit behavior for testability (kafka.Reader implements this interface).
type MessageCommitter interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// ProcessResult represents the result of processing a message by a worker.
type ProcessResult struct {
	Msg kafka.Message
	Err error
}

// job is a fetched Kafka message to be processed by workers
type job struct {
	msg kafka.Message
}

// result is the processing outcome of a job, consumed by the coordinator
type result struct {
	msg kafka.Message
	err error
}

// CommitCallback is a function that gets called after a message is successfully committed
// The function receives the committed message and any error that occurred during commit
// If the commit was successful, the error will be nil
type CommitCallback func(msg kafka.Message, err error)

type partState struct {
	next int64
	buf  map[int64]ProcessResult
}
