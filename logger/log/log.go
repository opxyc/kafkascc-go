package logger

import (
	"encoding/json"
	"log"
	"time"

	"github.com/opxyc/kafka-scc/consumer"
)

type stdJSONLogger struct {
	l    *log.Logger
	base map[string]any
}

// New wraps any *log.Logger and returns a consumer.Logger implementation.
func New(l *log.Logger) consumer.Logger {
	if l == nil {
		l = log.Default()
	}
	return &stdJSONLogger{
		l:    l,
		base: map[string]any{},
	}
}

func (s *stdJSONLogger) With(kv ...any) consumer.Logger {
	return &stdJSONLogger{
		l:    s.l,
		base: merge(s.base, kvToFields(kv...)),
	}
}

func (s *stdJSONLogger) Info(msg string, kv ...any) {
	s.emit("info", msg, kvToFields(kv...))
}

func (s *stdJSONLogger) Error(msg string, kv ...any) {
	s.emit("error", msg, kvToFields(kv...))
}

func (s *stdJSONLogger) emit(level, message string, fields map[string]any) {
	evt := make(map[string]any, len(s.base)+len(fields)+3)

	// merge base + call-specific fields
	for k, v := range s.base {
		evt[k] = v
	}
	for k, v := range fields {
		evt[k] = v
	}

	evt["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	evt["level"] = level
	evt["msg"] = message

	b, err := json.Marshal(evt)
	if err != nil {
		s.l.Printf(`{"level":"error","msg":"std logger marshal error","err":%q}`, err.Error())
		return
	}

	s.l.Println(string(b))
}

// merge creates a new map merging a and b (b wins on conflicts)
func merge(a, b map[string]any) map[string]any {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// kvToFields converts key-value pairs to a map.
// Odd key count drops last value, non-string keys are JSON-stringified.
func kvToFields(kv ...any) map[string]any {
	out := make(map[string]any)

	for i := 0; i+1 < len(kv); i += 2 {
		rawKey := kv[i]
		val := kv[i+1]

		var key string
		switch k := rawKey.(type) {
		case string:
			key = k
		default:
			// fallback: stringify non-string keys
			b, _ := json.Marshal(rawKey)
			key = string(b)
		}

		out[key] = val
	}

	return out
}
