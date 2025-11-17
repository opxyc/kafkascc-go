package consumer

import (
	"context"
	"sync"
)

// Gate controls message consumption based on health
type Gate struct {
	mu      sync.Mutex
	healthy bool
	ready   chan struct{}
}

func NewGate(initial bool) *Gate {
	g := &Gate{healthy: initial}
	if initial {
		ch := make(chan struct{})
		close(ch)
		g.ready = ch
	} else {
		g.ready = make(chan struct{})
	}
	return g
}

// SetHealthy updates the health state
func (g *Gate) SetHealthy(v bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.healthy == v {
		return
	}
	g.healthy = v
	if v {
		// Close current channel to release any waiters
		select {
		case <-g.ready:
			// already closed
		default:
			close(g.ready)
		}
	} else {
		// Create a new channel that blocks waiters
		g.ready = make(chan struct{})
	}
}

func (g *Gate) IsHealthy() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.healthy
}

// WaitUntilHealthy blocks until healthy or context is done
func (g *Gate) WaitUntilHealthy(ctx context.Context) error {
	g.mu.Lock()
	ch := g.ready
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}
