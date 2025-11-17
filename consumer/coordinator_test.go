package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type mockCommitter struct {
	mu             sync.Mutex
	committed      []int64
	committedParts []int
}

func (m *mockCommitter) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range msgs {
		m.committed = append(m.committed, msg.Offset)
		m.committedParts = append(m.committedParts, msg.Partition)
	}
	return nil
}

// helper to create msg
func km(partition int, offset int64) kafka.Message {
	return kafka.Message{Partition: partition, Offset: offset}
}

func TestCoordinatorInOrderCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mc := &mockCommitter{}
	gate := NewGate(true)
	l := newTestLogger()
	coord := NewCoordinatorWithProcessor("topicA", mc, gate, cancel, nil, 10*time.Millisecond, 50*time.Millisecond, l, func(error) bool { return false })
	// starting offsets
	coord.SetStartOffset(0, 10)
	// feed out-of-order results
	_ = coord.Process(ctx, ProcessResult{Msg: km(0, 12), Err: nil})
	_ = coord.Process(ctx, ProcessResult{Msg: km(0, 11), Err: nil})
	_ = coord.Process(ctx, ProcessResult{Msg: km(0, 10), Err: nil})
	// allow a moment
	time.Sleep(10 * time.Millisecond)
	mc.mu.Lock()
	defer mc.mu.Unlock()
	want := []int64{10, 11, 12}
	if len(mc.committed) != len(want) {
		t.Fatalf("committed len=%d want %d", len(mc.committed), len(want))
	}
	for i, off := range want {
		if mc.committed[i] != off {
			t.Fatalf("committed[%d]=%d want %d", i, mc.committed[i], off)
		}
	}
}

type pauseErrType struct{ msg string }

func (e pauseErrType) Error() string { return e.msg }

func TestCoordinatorPausePredicateRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mc := &mockCommitter{}
	gate := NewGate(true)
	l := newTestLogger()
	pErr := pauseErrType{"pause"}
	attempts := 0
	processFn := func(ctx context.Context, msg kafka.Message) error {
		attempts++
		if attempts < 3 {
			return pErr
		}
		return nil
	}
	coord := NewCoordinatorWithProcessor("topicB", mc, gate, cancel, processFn, 5*time.Millisecond, 20*time.Millisecond, l, func(err error) bool { return errors.Is(err, pErr) })
	coord.SetStartOffset(0, 1)
	if err := coord.Process(ctx, ProcessResult{Msg: km(0, 1), Err: pErr}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// wait for retry to succeed
	time.Sleep(50 * time.Millisecond)
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.committed) != 1 || mc.committed[0] != 1 {
		t.Fatalf("expected commit of 1, got %v", mc.committed)
	}
	if attempts < 3 {
		t.Fatalf("expected at least 3 attempts, got %d", attempts)
	}
}

func TestCoordinatorNonPausingErrorIsCommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mc := &mockCommitter{}
	gate := NewGate(true)
	l := newTestLogger()
	nonPause := errors.New("non-pause")
	coord := NewCoordinatorWithProcessor("topicC", mc, gate, cancel, nil, 5*time.Millisecond, 20*time.Millisecond, l, func(error) bool { return false })
	coord.SetStartOffset(0, 5)
	if err := coord.Process(ctx, ProcessResult{Msg: km(0, 5), Err: nonPause}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// it should have committed despite the error
	time.Sleep(10 * time.Millisecond)
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.committed) != 1 || mc.committed[0] != 5 {
		t.Fatalf("expected commit of 5, got %v", mc.committed)
	}
}
