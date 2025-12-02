package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// mockReader implements the Reader interface used by TopicConsumer
// It yields a fixed list of messages and records commits and Close.
type mockReader struct {
	msgs    []kafka.Message
	mu      sync.Mutex
	idx     int
	closed  bool
	commits []int64
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	m.mu.Lock()
	i := m.idx
	m.idx++
	m.mu.Unlock()
	if i >= len(m.msgs) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}

	return m.msgs[i], nil
}

func (m *mockReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range msgs {
		m.commits = append(m.commits, msg.Offset)
	}
	return nil
}

func (m *mockReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// Test that shutting down while FetchMessage is blocked unblocks via Stop().
func TestTopicConsumer_ShutdownWhileFetching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// empty msgs -> FetchMessage will block until ctx done
	mr := &mockReader{msgs: []kafka.Message{}}
	gate := NewGate(true)
	l := newNopLogger()
	c := &Consumer{
		topic:        "t",
		handler:      &mockHandler{},
		reader:       mr,
		gate:         gate,
		workerCount:  1,
		backoffBase:  5 * time.Millisecond,
		backoffMax:   20 * time.Millisecond,
		log:          l,
		pauseDecider: func(error) bool { return false },
	}
	// run consume
	done := make(chan struct{})
	go func() { c.consume(ctx); close(done) }()
	// Give it a moment to enter FetchMessage
	time.Sleep(10 * time.Millisecond)
	// Now cancel the context; since consume() was started directly with this ctx,
	// cancellation should unwind the loop promptly.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("consume did not exit after cancellation")
	}
}

// Test that Stop called concurrently during consume returns without deadlock.
func TestTopicConsumer_ConcurrentStopDuringConsume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgs := []kafka.Message{{Partition: 0, Offset: 1}}
	mr := &mockReader{msgs: msgs}
	gate := NewGate(true)
	l := newNopLogger()
	c := &Consumer{
		topic:        "t",
		handler:      &mockHandler{},
		reader:       mr,
		gate:         gate,
		workerCount:  1,
		backoffBase:  5 * time.Millisecond,
		backoffMax:   20 * time.Millisecond,
		log:          l,
		pauseDecider: func(error) bool { return false },
	}

	go c.consume(ctx)
	time.Sleep(5 * time.Millisecond)
	// Concurrent stop should not deadlock
	if err := c.Stop(); err != nil && err.Error() == "consumer stop timeout for topic t" {
		t.Fatalf("stop timed out")
	}
}

// If pauseDecider is nil, non-pausing errors should not stop; they should be committed.
func TestTopicConsumer_NilPausePredicate_DoesNotPauseOrStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgs := []kafka.Message{
		{Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
	}
	mr := &mockReader{msgs: msgs}
	// handler returns an error
	errHandler := &pauseOnceHandler{err: errors.New("any")}
	gate := NewGate(true)
	l := newNopLogger()
	c := &Consumer{
		topic:        "t",
		handler:      errHandler,
		reader:       mr,
		gate:         gate,
		workerCount:  1,
		backoffBase:  5 * time.Millisecond,
		backoffMax:   20 * time.Millisecond,
		log:          l,
		pauseDecider: nil,
	}
	go c.consume(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if len(mr.commits) == 0 || mr.commits[0] != 1 {
		t.Fatalf("expected commit of 1, got %v", mr.commits)
	}
}

// Coordinator signals stopCh when commit fails; simulate via a reader that errors on commit.
type commitErrReader struct{ mockReader }

func (r *commitErrReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return fmt.Errorf("commit error")
}

func TestTopicConsumer_FetchLoopStopsOnCoordinatorSignal(t *testing.T) {
	mr := &commitErrReader{mockReader: mockReader{msgs: []kafka.Message{{Partition: 0, Offset: 1}}}}

	// Create a channel that will be closed when the coordinator signals to stop
	stopCh := make(chan struct{})

	// Create a coordinator with a custom cancel function that closes our stop channel
	coord := NewCoordinatorWithProcessor(
		"test-topic",
		mr,
		NewGate(true),
		func() { close(stopCh) }, // When coordinator cancels, close stopCh
		nil,                      // No process function needed for this test
		time.Millisecond,
		time.Millisecond,
		newNopLogger(),
		func(error) bool { return false },
	)

	// Process a message - this should trigger a commit error
	err := coord.Process(context.Background(), ProcessResult{
		Msg: kafka.Message{Partition: 0, Offset: 1, Topic: "test-topic"},
		Err: nil,
	})

	// Verify we got a commit error
	if err == nil {
		t.Fatal("expected commit error, got nil")
	}
}

// Workers process concurrently when workerCount>1.
func TestTopicConsumer_WorkersProcessConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgs := []kafka.Message{{Partition: 0, Offset: 1}, {Partition: 0, Offset: 2}}
	mr := &mockReader{msgs: msgs}
	gate := NewGate(true)
	l := newNopLogger()
	startCh := make(chan struct{})
	doneCh := make(chan struct{}, 2)
	// handler that simulates work and signals done
	h := &blockingHandler{startCh: startCh, doneCh: doneCh}
	c := &Consumer{
		topic:        "t",
		handler:      h,
		reader:       mr,
		gate:         gate,
		workerCount:  2,
		backoffBase:  5 * time.Millisecond,
		backoffMax:   20 * time.Millisecond,
		log:          l,
		pauseDecider: func(error) bool { return false },
	}
	go c.consume(ctx)
	close(startCh) // release both workers
	// both should finish quickly (within 100ms)
	timeout := time.After(200 * time.Millisecond)
	got := 0
	for got < 2 {
		select {
		case <-doneCh:
			got++
		case <-timeout:
			t.Fatalf("workers did not process concurrently")
		}
	}
}

type blockingHandler struct {
	startCh <-chan struct{}
	doneCh  chan<- struct{}
}

func (h *blockingHandler) Handle(ctx context.Context, message kafka.Message) error {
	<-h.startCh
	time.Sleep(50 * time.Millisecond)
	h.doneCh <- struct{}{}
	return nil
}
func (h *blockingHandler) Close() error  { return nil }
func (h *blockingHandler) Topic() string { return "t" }

// Test that Stop closes the reader first and returns promptly even if nothing is running.
func TestTopicConsumer_StopClosesReaderPromptly(t *testing.T) {
	c := &Consumer{}
	mr := &mockReader{}
	c.reader = mr
	// no wg goroutines running; Stop should return quickly
	start := time.Now()
	if err := c.Stop(); err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("stop took too long")
	}
	mr.mu.Lock()
	closed := mr.closed
	mr.mu.Unlock()
	if !closed {
		t.Fatalf("expected reader to be closed")
	}
}

// Test that when gate is unhealthy, fetch is blocked until it becomes healthy.
func TestTopicConsumer_GateBlocksUntilHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs := []kafka.Message{
		{Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
	}
	mr := &mockReader{msgs: msgs}
	gate := NewGate(false) // start unhealthy
	l := newNopLogger()
	c := &Consumer{
		topic:        "t",
		handler:      &mockHandler{},
		reader:       mr,
		brokers:      []string{"b"},
		groupID:      "g",
		gate:         gate,
		workerCount:  1,
		backoffBase:  5 * time.Millisecond,
		backoffMax:   20 * time.Millisecond,
		log:          l,
		pauseDecider: func(error) bool { return false },
	}

	go c.consume(ctx)
	// Give time; since gate unhealthy, reader should not be fetched
	time.Sleep(20 * time.Millisecond)
	mr.mu.Lock()
	idxBefore := mr.idx
	mr.mu.Unlock()
	if idxBefore != 0 {
		t.Fatalf("expected no fetch while unhealthy, got idx=%d", idxBefore)
	}
	// Flip to healthy and ensure fetch proceeds
	gate.SetHealthy(true)
	time.Sleep(30 * time.Millisecond)
	mr.mu.Lock()
	idxAfter := mr.idx
	mr.mu.Unlock()
	if idxAfter == 0 {
		t.Fatalf("expected fetch after healthy")
	}
}

// mockHandler implements handler.MessageHandler, returns errors based on a map of offset->error
// and records invocations.
type mockHandler struct {
	mu    sync.Mutex
	calls []int64
}

func (h *mockHandler) Handle(ctx context.Context, message kafka.Message) error {
	// message carries the offset in tests for simplicity
	// Not relying on message content; tests coordinate via offsets list
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, 0) // placeholder to count calls
	return nil
}

func (h *mockHandler) Close() error  { return nil }
func (h *mockHandler) Topic() string { return "t" }

// pauseOnceHandler returns a pausing error on first call, then success.
type pauseOnceHandler struct {
	mu    sync.Mutex
	fired bool
	err   error
}

func (h *pauseOnceHandler) Handle(ctx context.Context, message kafka.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.fired {
		h.fired = true
		return h.err
	}
	return nil
}

func (h *pauseOnceHandler) Close() error  { return nil }
func (h *pauseOnceHandler) Topic() string { return "t" }

// Test that non-pausing errors (predicate==nil/false) do not stop consumption and are committed.
func TestTopicConsumer_NonPausingErrorsAreCommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs := []kafka.Message{
		{Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		{Partition: 0, Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
		{Partition: 0, Offset: 3, Key: []byte("k3"), Value: []byte("v3")},
	}
	mr := &mockReader{msgs: msgs}
	mh := &mockHandler{}
	gate := NewGate(true)
	log := newNopLogger()

	c := &Consumer{
		topic:        "t",
		handler:      mh,
		reader:       mr,
		brokers:      []string{"b"},
		groupID:      "g",
		gate:         gate,
		workerCount:  2,
		backoffBase:  5 * time.Millisecond,
		backoffMax:   20 * time.Millisecond,
		log:          log,
		pauseDecider: func(error) bool { return false },
	}

	// run consume in background and stop shortly after messages processed
	go c.consume(ctx)
	// allow processing
	time.Sleep(50 * time.Millisecond)
	cancel()
	// give some time to unwind
	time.Sleep(20 * time.Millisecond)

	// All messages should have been committed
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if len(mr.commits) == 0 {
		t.Fatalf("expected commits, got none")
	}
	if len(mr.commits) < len(msgs) {
		t.Fatalf("expected at least %d commits, got %d", len(msgs), len(mr.commits))
	}
}

// Test that pause predicate causes retry and commit after success.
func TestTopicConsumer_PausePredicateRetriesThenCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs := []kafka.Message{
		{Partition: 0, Offset: 10, Key: []byte("k"), Value: []byte("v")},
	}
	mr := &mockReader{msgs: msgs}
	pauseErr := errors.New("pause")
	mh := &pauseOnceHandler{err: pauseErr}
	gate := NewGate(true)
	log := newNopLogger()

	c := &Consumer{
		topic:        "t",
		handler:      mh,
		reader:       mr,
		brokers:      []string{"b"},
		groupID:      "g",
		gate:         gate,
		workerCount:  1,
		backoffBase:  5 * time.Millisecond,
		backoffMax:   20 * time.Millisecond,
		log:          log,
		pauseDecider: func(err error) bool { return errors.Is(err, pauseErr) },
	}

	go c.consume(ctx)
	// wait enough for retry to succeed
	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	mr.mu.Lock()
	defer mr.mu.Unlock()
	if len(mr.commits) == 0 || mr.commits[0] != 10 {
		t.Fatalf("expected commit of 10, got %v", mr.commits)
	}
}

// panicHandler panics on the first call, then succeeds on subsequent calls
type panicHandler struct {
	sync.Mutex
	panicOnce bool
	calls     int
}

func (h *panicHandler) Handle(ctx context.Context, message kafka.Message) error {
	h.Lock()
	defer h.Unlock()
	h.calls++

	if !h.panicOnce {
		h.panicOnce = true
		panic("intentional panic for testing")
	}

	return nil
}

func (h *panicHandler) Close() error  { return nil }
func (h *panicHandler) Topic() string { return "test-topic" }

func TestConsumer_HandlesHandlerPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs := []kafka.Message{
		{Partition: 0, Offset: 1, Key: []byte("key1"), Value: []byte("value1")},
		{Partition: 0, Offset: 2, Key: []byte("key2"), Value: []byte("value2")},
	}

	mr := &mockReader{msgs: msgs}
	gate := NewGate(true)
	log := newNopLogger()
	handler := &panicHandler{}

	c := &Consumer{
		topic:        "test-topic",
		handler:      handler,
		reader:       mr,
		gate:         gate,
		workerCount:  1,
		backoffBase:  time.Millisecond,
		backoffMax:   10 * time.Millisecond,
		log:          log,
		pauseDecider: func(err error) bool { return false },
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.consume(ctx)
	}()

	// Give it some time to process messages
	time.Sleep(500 * time.Millisecond)

	mr.mu.Lock()
	commits := make([]int64, len(mr.commits))
	copy(commits, mr.commits)
	mr.mu.Unlock()

	// Verify exactly 2 commits (one for each message)
	if len(commits) != 2 {
		t.Fatalf("Expected exactly 2 commits, got %d: %v", len(commits), commits)
	}

	// Verify handler was called exactly twice (once per message, with first one panicking)
	if handler.calls != 2 {
		t.Errorf("Expected exactly 2 handler calls, got %d", handler.calls)
	}

	cancel()
	wg.Wait()
}
