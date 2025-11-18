# Kafka SCC: Kafka Sequential Commit Consumer

A high-throughput, fault-tolerant Kafka consumer service designed for strict reliability requirements (e.g., AML, fraud detection, and compliance pipelines). It enforces in-order commits per partition, explicit manual commits, pause/retry on downstream unavailability, and graceful shutdown.

## Key Features

- Manual offset control with in-order commit per partition
- Pause-and-retry on downstream errors (respecting the `PausePredicate`) with exponential backoff + jitter
- Pluggable message handlers per topic
- Health gate to safely pause/resume fetching
- Structured logging with trace IDs for each message

## Sequential Commit Guarantee (SCC)

SCC means the consumer will commit offsets strictly in order per partition, and only after successful business processing:

- The `Coordinator` buffers out-of-order worker results and commits only the next expected offset.
- If a handler returns a pause-eligible error (e.g., API or DB unavailable during downstream processing), the same message is retried with exponential backoff and jitter until it succeeds; commit is deferred until then.
- Non-pausing errors are logged and committed to avoid stalling the pipeline.
- This yields at-least-once processing with strict in-partition ordering of commits. For end-to-end idempotency, make handlers idempotent or leverage downstream de-duplication.

## Terminology and Core Concepts

- **Gate (`consumer/gate.go`)**
  A lightweight health latch used to pause/resume fetching. When the gate is unhealthy (closed), the fetch loop blocks on `WaitUntilHealthy()`. When healthy (open), fetching proceeds. The Coordinator will flip the gate to unhealthy when the handler reports an error (respecting the `PausePredicate`), and back to healthy after a successful retry.
- **Coordinator (`consumer/coordinator.go`)**
  Ensures per-partition in-order commits. It buffers out-of-order results and only commits the next expected offset. On pause-eligible errors, it retries the same message with backoff until success; non-pausing errors are logged and committed to avoid blocking.
- **Consumer (`consumer/consumer.go`)**
  Orchestrates the pipeline: fetch -> workers -> coordinator. It owns the worker pool, result channel, and fetch loop. It guarantees deterministic draining on shutdown.
- **Worker**
  A goroutine that runs your `handler.MessageHandler` concurrently. It attaches a per-message `trace_id` to the context so logs and downstream calls can be correlated.
- **PausePredicate**
  A function `func(error) bool` that defines which handler errors should pause and trigger retries. Use `consumer.OrPauseErrors(...)` to compose them.

## Configuration (Consumer-only essentials)

For the consumer package, the only environment variables you typically need are:

- `KAFKA_BROKERS` — comma-separated broker list (default: `localhost:9094`)
- `KAFKA_GROUP_ID` — consumer group ID (default: `event-processor-group`)
- `KAFKA_TOPIC` — topic to consume

## Writing a Handler

Implement the interface:

```go
type MessageHandler interface {
    Handle(ctx context.Context, message []byte) error
    Topic() string
    Close() error
}
```

Example (posting to an external API): [`example/consumer/handler/http_poster.go`](example/consumer/handler/http_poster.go)

## Logging

The `consumer` package exposes a minimal, swappable interface:

```go
type Logger interface {
    With(args ...any) Logger
    Info(msg string, args ...any)
    Error(msg string, args ...any)
}
```

- A minimal logger is available in the `logger/log` package (which is an adapter for the standard `log` to match the interface).

You can implement your own logger adapter that satisfies this interface to plug in any logger of your choice. If no logger
is provided, it will use a no-op logger.

## Error Semantics and Reliability Guarantees

- **Pausing errors** (defined by `PausePredicate`) cause:

  - Gate flips to unhealthy
  - Coordinator retries the same message with exponential backoff + jitter until success
  - After success, the Gate flips back to healthy and the message is committed

- **Non-pausing errors**:

  - Logged and the message is committed
  - Consumption continues (no pipeline stall)

- **Ordering**:

  - Commits are strictly in order per partition (next expected offset only)
  - Out-of-order worker completions are buffered until the gap is filled

- **Shutdown**:

  - `Stop()` closes the reader first (to unblock `FetchMessage`) and cancels the context
  - The pipeline drains deterministically (workers finish, results channel drained, coordinator completes)

## Examples

The [`example/`](example/) directory contains simple programs to help you run the system end-to-end:

- `examples/consumer/` — showcases how to use the `consumer` package. It wires up a `consumer.Manager`, creates a topic consumer with a handler, and starts consumption. This is the primary example for integrating SCC in your app.
- `examples/api/` — a minimal Go HTTP server that exposes `POST /events` on `http://localhost:8090`. The example handler posts here so you can observe end-to-end flow. It is only a basic sink to make the demo work.
- `examples/producer/` — a dummy Kafka producer that sends synthetic JSON events to the topic (default `test-topic`) on broker `localhost:9094`. This is optional, just to populate the topic during demos.

## License

MIT
