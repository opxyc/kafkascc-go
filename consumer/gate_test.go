package consumer

import (
	"context"
	"testing"
	"time"
)

func TestGate_InitialHealthyUnhealthy(t *testing.T) {
	g1 := NewGate(true)
	if !g1.IsHealthy() {
		t.Fatalf("expected healthy=true")
	}

	g2 := NewGate(false)
	if g2.IsHealthy() {
		t.Fatalf("expected healthy=false")
	}
}

func TestGate_WaitUntilHealthy_UnblocksOnSetHealthy(t *testing.T) {
	g := NewGate(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = g.WaitUntilHealthy(ctx)
		close(done)
	}()

	// ensure it's waiting
	select {
	case <-done:
		t.Fatalf("returned early")
	case <-time.After(20 * time.Millisecond):
	}

	// flip healthy and expect return
	g.SetHealthy(true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("did not unblock after healthy")
	}
}

func TestGate_WaitUntilHealthy_CtxCancel(t *testing.T) {
	g := NewGate(false)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := g.WaitUntilHealthy(ctx); err == nil {
		t.Fatalf("expected context error")
	}

	n := 5
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			_ = g.WaitUntilHealthy(ctx)
			done <- struct{}{}
		}()
	}

	// ensure goroutines started
	time.Sleep(20 * time.Millisecond)
	g.SetHealthy(true)
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("waiter %d not unblocked", i)
		}
	}
}
