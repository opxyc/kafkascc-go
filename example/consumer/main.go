package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/opxyc/kafkascc-go/consumer"
	"github.com/opxyc/kafkascc-go/example/consumer/config"
	"github.com/opxyc/kafkascc-go/example/consumer/handler"
	l "github.com/opxyc/kafkascc-go/logger/log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	consumerManager := consumer.NewManager()
	gate := consumer.NewGate(true) // start healthy; will pause on API errors

	logger := l.New(log.Default()).With("service", "kafkascc-go")

	workerCount := 8
	testTopicConsumer := consumer.New(
		cfg.Kafka.Brokers,
		cfg.Kafka.GroupID,
		cfg.Kafka.Topic,
		// Handler that posts to the API and signals api.ErrUnavailable on outages
		handler.NewAPIPostHandler(cfg.APIBaseURL, logger),
		gate,
		workerCount,
		cfg.BackoffBase,
		cfg.BackoffMax,
		logger,
		consumer.OrPauseErrors(handler.ErrUnavailable),
	)

	consumerManager.AddConsumer(testTopicConsumer)

	log.Printf("Starting consumer for topic: %s", cfg.Kafka.Topic)
	log.Printf("Connecting to brokers: %v", cfg.Kafka.Brokers)

	consumerManager.StartAll(ctx)

	// Wait for termination signal
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("Received termination signal, shutting down...")
		cancel()
	case <-ctx.Done():
		log.Println("Context cancelled, shutting down...")
	}

	// Cleanup
	if err := consumerManager.StopAll(); err != nil {
		log.Printf("Error stopping consumers: %v", err)
	}

	log.Println("Consumers stopped")
}
