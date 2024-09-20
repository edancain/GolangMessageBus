package datadictionary

import (
	"testing"
	"time"
	"bytes"

	"github.com/edancain/RocketLab/types"
	"github.com/edancain/RocketLab/bus/logger"
)

func TestStore(t *testing.T) {
	// Capture log output
	var logBuffer bytes.Buffer
	logger.ErrorLogger.SetOutput(&logBuffer)
	logger.InfoLogger.SetOutput(&logBuffer)
	logger.DebugLogger.SetOutput(&logBuffer)

	dd := NewDataDictionary()
	msg := types.Message{
		Timestamp: time.Now(),
		Topic:     "test-topic",
		Content:   "Hello, World!",
	}

	err := dd.Store(msg)
	if err != nil {
		t.Errorf("Store failed: %v", err)
	}

	if !bytes.Contains(logBuffer.Bytes(), []byte("New topic created in DataDictionary: test-topic")) {
		t.Error("Expected log message for new topic creation")
	}

	// Test storing more than maxMessagesPerTopic
	logBuffer.Reset()
	for i := 0; i < maxMessagesPerTopic; i++ {
		err = dd.Store(msg)
		if err != nil && i < maxMessagesPerTopic-1 {
			t.Errorf("Store failed unexpectedly: %v", err)
		}
	}

	if err == nil {
		t.Error("Expected error when exceeding maxMessagesPerTopic, got nil")
	}

	if !bytes.Contains(logBuffer.Bytes(), []byte("Max messages reached for topic: test-topic")) {
		t.Error("Expected log message for max messages reached")
	}
}

func TestGetMessages(t *testing.T) {
	dd := NewDataDictionary()
	now := time.Now()

	msgs := []types.Message{
		{Timestamp: now.Add(-2 * time.Hour), Topic: "test-topic", Content: "Old message"},
		{Timestamp: now.Add(-30 * time.Minute), Topic: "test-topic", Content: "Recent message"},
		{Timestamp: now.Add(30 * time.Minute), Topic: "test-topic", Content: "Future message"},
	}

	for _, msg := range msgs {
		err := dd.Store(msg)
		if err != nil {
			t.Errorf("Store failed: %v", err)
		}
	}

	retrieved := dd.GetMessages("test-topic", now.Add(-1*time.Hour), now)
	if len(retrieved) != 1 {
		t.Errorf("Expected 1 message, got %d", len(retrieved))
	}

	if retrieved[0].Content != "Recent message" {
		t.Errorf("Expected 'Recent message', got '%s'", retrieved[0].Content)
	}
}

func TestCleanupExpiredMessages(t *testing.T) {
	dd := NewDataDictionary()
	now := time.Now()

	msgs := []types.Message{
		{Timestamp: now.Add(-25 * time.Hour), Topic: "test-topic", Content: "Expired message"},
		{Timestamp: now.Add(-23 * time.Hour), Topic: "test-topic", Content: "Almost expired message"},
		{Timestamp: now, Topic: "test-topic", Content: "Current message"},
	}

	for _, msg := range msgs {
		err := dd.Store(msg)
		if err != nil {
			t.Errorf("Store failed: %v", err)
		}
	}

	dd.cleanupExpiredMessages()

	retrieved := dd.GetMessages("test-topic", now.Add(-26*time.Hour), now)
	if len(retrieved) != 2 {
		t.Errorf("Expected 2 messages after cleanup, got %d", len(retrieved))
	}

	for _, msg := range retrieved {
		if msg.Content == "Expired message" {
			t.Error("Found expired message that should have been cleaned up")
		}
	}
}

func TestLogLevels(t *testing.T) {
	tests := []struct {
		name      string
		logLevel  logger.LogLevel
		wantError bool
		wantInfo  bool
		wantDebug bool
	}{
		{"ErrorLevel", logger.LevelError, true, false, false},
		{"InfoLevel", logger.LevelInfo, true, true, false},
		{"DebugLevel", logger.LevelDebug, true, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.SetLogLevel(tt.logLevel)

			var errorBuffer, infoBuffer, debugBuffer bytes.Buffer
			logger.ErrorLogger.SetOutput(&errorBuffer)
			logger.InfoLogger.SetOutput(&infoBuffer)
			logger.DebugLogger.SetOutput(&debugBuffer)

			dd := NewDataDictionary()
			
			// Trigger all types of logs
			msg := types.Message{Timestamp: time.Now(), Topic: "test-topic", Content: "Test message"}
			for i := 0; i < maxMessagesPerTopic+1; i++ {
				dd.Store(msg)
			}

			gotError := errorBuffer.Len() > 0
			if gotError != tt.wantError {
				t.Errorf("Error logging: got %v, want %v", gotError, tt.wantError)
			}

			gotInfo := infoBuffer.Len() > 0
			if gotInfo != tt.wantInfo {
				t.Errorf("Info logging: got %v, want %v", gotInfo, tt.wantInfo)
			}

			gotDebug := debugBuffer.Len() > 0
			if gotDebug != tt.wantDebug {
				t.Errorf("Debug logging: got %v, want %v", gotDebug, tt.wantDebug)
			}
		})
	}
}