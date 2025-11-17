# Producer Example

A simple Kafka producer that streams synthetic banking events to the `test-topic` on broker `localhost:9094`.

## Run

Ensure Kafka is reachable on `localhost:9094` and the topic `test-topic` exists (or is auto-created by your broker).

```bash
# From repo root
go run ./examples/producer
```

You should see output like:

```
Producing banking events to Kafka topic: test-topic
Sent: {"ts":"...","action":"money_transfer", ...}
...
```

## Configuration

This example uses in-code defaults:

- Broker: `localhost:9094`
- Topic: `test-topic`

To change these, edit `examples/producer/main.go` (`broker` and `topic` variables).

## Notes

- Uses `github.com/segmentio/kafka-go` writer with `LeastBytes` balancer.
- Sends one event roughly every 50ms.
