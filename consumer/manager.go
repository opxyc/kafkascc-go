package consumer

import (
	"context"
	"fmt"
	"sync"
)

// Manager manages multiple consumers
type Manager struct {
	consumers map[string]*Consumer
	mu        sync.Mutex
}

// NewManager creates a new consumer manager
func NewManager() *Manager {
	return &Manager{
		consumers: make(map[string]*Consumer),
	}
}

// AddConsumer adds a new topic consumer to the manager
func (m *Manager) AddConsumer(consumer *Consumer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumers[consumer.topic] = consumer
}

// StartAll starts all registered consumers
func (m *Manager) StartAll(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, consumer := range m.consumers {
		go consumer.Start(ctx)
	}
}

// StopAll stops all registered consumers
func (m *Manager) StopAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, consumer := range m.consumers {
		if err := consumer.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("error stopping consumer for topic %s: %w", consumer.topic, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while stopping consumers: %v", errs)
	}

	return nil
}

// GetConsumer returns the consumer for a specific topic
func (m *Manager) GetConsumer(topic string) (*Consumer, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	consumer, exists := m.consumers[topic]
	return consumer, exists
}
