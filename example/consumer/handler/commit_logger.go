package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type CommitLogger struct {
	logDir string
	files  map[string]*os.File
	mu     sync.Mutex
}

// NewCommitLogger creates a new commit logger that writes to files in the specified directory
func NewCommitLogger(logDir string) (*CommitLogger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	return &CommitLogger{
		logDir: logDir,
		files:  make(map[string]*os.File),
	}, nil
}

// LogCommit logs a commit event for a message
func (cl *CommitLogger) LogCommit(msg kafka.Message) error {
	if msg.Topic == "" {
		return fmt.Errorf("message topic is empty")
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	// Create log entry
	entry := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       string(msg.Key),
	}

	// Convert to JSON
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("error marshaling log entry: %w", err)
	}
	data = append(data, '\n')

	// Get or create file for this topic
	file, err := cl.getOrCreateFile(msg.Topic)
	if err != nil {
		return fmt.Errorf("error getting log file: %w", err)
	}

	// Write to file and sync to ensure it's written to disk
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("error writing to log file: %w", err)
	}

	// Ensure the data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("error syncing log file: %w", err)
	}

	return nil
}

// getOrCreateFile returns a file handle for the given topic, creating it if necessary
func (cl *CommitLogger) getOrCreateFile(topic string) (*os.File, error) {
	if file, exists := cl.files[topic]; exists {
		return file, nil
	}

	// Ensure the log directory exists
	if err := os.MkdirAll(cl.logDir, 0755); err != nil {
		return nil, fmt.Errorf("error creating log directory: %w", err)
	}

	// Create a new file for this topic
	safeTopic := strings.ReplaceAll(topic, string(filepath.Separator), "_")
	logFile := filepath.Join(cl.logDir, fmt.Sprintf("%s.commits.log", safeTopic))
	
	// Open the file in append mode, create it if it doesn't exist
	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening log file %s: %w", logFile, err)
	}

	// Store the file handle
	cl.files[topic] = file
	return file, nil
}

// Close closes all open log files
func (cl *CommitLogger) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	var lastErr error
	for topic, file := range cl.files {
		if err := file.Close(); err != nil {
			log.Printf("Error closing log file for topic %s: %v", topic, err)
			lastErr = err
		}
		delete(cl.files, topic)
	}

	return lastErr
}
