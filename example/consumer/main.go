package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/opxyc/kafkascc-go/consumer"
	"github.com/opxyc/kafkascc-go/example/consumer/config"
	"github.com/opxyc/kafkascc-go/example/consumer/handler"
	l "github.com/opxyc/kafkascc-go/logger/log"
	"github.com/segmentio/kafka-go"
)

// createCommitCallback creates a callback function that logs commit events for a topic
func createCommitCallback(logger *handler.CommitLogger, topic string) func(kafka.Message, error) {
	return func(msg kafka.Message, err error) {
		if err != nil {
			log.Printf("[ERROR] Error in commit callback: %v", err)
			return
		}

		if logger == nil {
			return
		}

		// Log the current working directory
		cwd, _ := os.Getwd()

		logDir := filepath.Join(cwd, "commit_logs")

		if _, err := os.Stat(logDir); os.IsNotExist(err) {
			if err := os.MkdirAll(logDir, 0755); err != nil {
				log.Printf("[ERROR] Failed to create log directory: %v", err)
			}
		}

		// Try to create a test file to check permissions
		testFile := filepath.Join(logDir, "test_write.log")
		if f, err := os.OpenFile(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			f.Close()
		} else {
			log.Printf("[ERROR] Failed to create test file: %v", err)
		}

		if err := logger.LogCommit(msg); err != nil {
			log.Printf("[ERROR] Error in LogCommit: %v", err)
		} else {
			// Verify the log file was created
			expectedLogFile := filepath.Join(logDir, topic+".commits.log")
			if _, err := os.Stat(expectedLogFile); err != nil {
				log.Printf("[ERROR] Expected log file does not exist: %s", expectedLogFile)
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Create commit logger with absolute path
	execDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current working directory: %v", err)
	}
	logDir := filepath.Join(execDir, "commit_logs")
	log.Printf("Creating commit logs in directory: %s", logDir)
	commitLogger, err := handler.NewCommitLogger(logDir)
	if err != nil {
		log.Fatalf("Failed to create commit logger: %v", err)
	}
	defer func() {
		if err := commitLogger.Close(); err != nil {
			log.Printf("Error closing commit logger: %v", err)
		}
	}()

	log.Printf("Commit logs will be written to: %s", logDir)

	consumerManager := consumer.NewManager()
	gate := consumer.NewGate(true) // start healthy; will pause on API errors

	logger := l.New(log.Default()).With("service", "kafkascc-go")

	workerCount := 4

	// Create consumers with commit callbacks
	deviceActivationTopic := "uat.mob.deviceactivation-json"
	deviceActivationHandler := consumer.New(
		cfg.Kafka.Brokers,
		"uat.mob.deviceactivation-json-consumer-group",
		deviceActivationTopic,
		// Handler that queries the database and then posts to the API
		handler.NewDBHTTPHandler(cfg.APIBaseURL),
		gate,
		workerCount,
		cfg.BackoffBase,
		cfg.BackoffMax,
		logger,
		consumer.OrPauseErrors(handler.ErrUnavailable),
		createCommitCallback(commitLogger, deviceActivationTopic),
	)

	paymentTransDetailsTopic := "uat.mob.paymenttransdetails-json"
	paymentTransDetailsHandler := consumer.New(
		cfg.Kafka.Brokers,
		"uat.mob.paymenttransdetails-json-consumer-group",
		paymentTransDetailsTopic,
		// Handler that queries the database and then posts to the API
		handler.NewDBHTTPHandler(cfg.APIBaseURL),
		gate,
		workerCount,
		cfg.BackoffBase,
		cfg.BackoffMax,
		logger,
		consumer.OrPauseErrors(handler.ErrUnavailable),
		createCommitCallback(commitLogger, paymentTransDetailsTopic),
	)

	wlAccountCloseHandler := consumer.New(
		cfg.Kafka.Brokers,
		"uat.mob.wlaccountclose-json-consumer-group",
		"uat.mob.wlaccountclose-json",
		// Handler that queries the database and then posts to the API
		handler.NewDBHTTPHandler(cfg.APIBaseURL),
		gate,
		workerCount,
		cfg.BackoffBase,
		cfg.BackoffMax,
		logger,
		consumer.OrPauseErrors(handler.ErrUnavailable),
		createCommitCallback(commitLogger, "uat.mob.wlaccountclose-json"),
	)

	customerMHandler := consumer.New(
		cfg.Kafka.Brokers,
		"uat.mob.customerm-json-consumer-group",
		"uat.mob.customerm-json",
		// Handler that queries the database and then posts to the API
		handler.NewDBHTTPHandler(cfg.APIBaseURL),
		gate,
		workerCount,
		cfg.BackoffBase,
		cfg.BackoffMax,
		logger,
		consumer.OrPauseErrors(handler.ErrUnavailable),
		createCommitCallback(commitLogger, "uat.mob.customerm-json"),
	)

	mbLoginHandler := consumer.New(
		cfg.Kafka.Brokers,
		"uat.mob.mblogin-json-consumer-group",
		"uat.mob.mblogin-json",
		// Handler that queries the database and then posts to the API
		handler.NewDBHTTPHandler(cfg.APIBaseURL),
		gate,
		workerCount,
		cfg.BackoffBase,
		cfg.BackoffMax,
		logger,
		consumer.OrPauseErrors(handler.ErrUnavailable),
		createCommitCallback(commitLogger, "uat.mob.mblogin-json"),
	)

	beneficiaryMHandler := consumer.New(
		cfg.Kafka.Brokers,
		"uat.mob.beneficiarym-json-consumer-group",
		"uat.mob.beneficiarym-json",
		// Handler that queries the database and then posts to the API
		handler.NewDBHTTPHandler(cfg.APIBaseURL),
		gate,
		workerCount,
		cfg.BackoffBase,
		cfg.BackoffMax,
		logger,
		consumer.OrPauseErrors(handler.ErrUnavailable),
		createCommitCallback(commitLogger, "uat.mob.beneficiarym-json"),
	)

	consumerManager.AddConsumer(deviceActivationHandler)
	consumerManager.AddConsumer(paymentTransDetailsHandler)
	consumerManager.AddConsumer(wlAccountCloseHandler)
	consumerManager.AddConsumer(customerMHandler)
	consumerManager.AddConsumer(mbLoginHandler)
	consumerManager.AddConsumer(beneficiaryMHandler)

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
