package ordereddelivery

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/edancain/OperationsSoftware/bus/logger"
	"github.com/edancain/OperationsSoftware/types"
)

type levelWriter struct {
	bytes.Buffer
	level logger.LogLevel
}

func (lw *levelWriter) Write(p []byte) (n int, err error) {
	if logger.GetLogLevel() >= lw.level {
		return lw.Buffer.Write(p)
	}
	return len(p), nil
}

func TestDeliverMessage(t *testing.T) {
	logger.SetLogLevel(logger.LevelDebug)
	errorWriter := &levelWriter{level: logger.LevelError}
	infoWriter := &levelWriter{level: logger.LevelInfo}
	debugWriter := &levelWriter{level: logger.LevelDebug}
	logger.ErrorLogger.SetOutput(errorWriter)
	logger.InfoLogger.SetOutput(infoWriter)
	logger.DebugLogger.SetOutput(debugWriter)

	odm := NewOrderedDeliveryManager()

	receivedMessages := make([]types.Message, 0)
	subscription := func(timestamp time.Time, content string) {
		receivedMessages = append(receivedMessages, types.Message{Timestamp: timestamp, Content: content})
	}

	now := time.Now()
	messages := []types.Message{
		{Timestamp: now.Add(2 * time.Second), Topic: "test-topic", Content: "Message 3"},
		{Timestamp: now.Add(1 * time.Second), Topic: "test-topic", Content: "Message 2"},
		{Timestamp: now, Topic: "test-topic", Content: "Message 1"},
	}

	for _, msg := range messages {
		err := odm.DeliverMessage(msg, subscription)
		if err != nil {
			t.Errorf("DeliverMessage failed: %v", err)
		}
	}

	time.Sleep(3 * time.Second) // Wait for all messages to be processed

	if len(receivedMessages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(receivedMessages))
	}

	for i, msg := range receivedMessages {
		expectedContent := fmt.Sprintf("Message %d", i+1)
		if msg.Content != expectedContent {
			t.Errorf("Expected message content %s, got %s", expectedContent, msg.Content)
		}
	}

	if !bytes.Contains(infoWriter.Bytes(), []byte("New topic queue created for: test-topic")) {
		t.Error("Expected log message for new topic queue creation")
	}
}

func TestMultipleTopics(t *testing.T) {
	logger.SetLogLevel(logger.LevelDebug)
	errorWriter := &levelWriter{level: logger.LevelError}
	infoWriter := &levelWriter{level: logger.LevelInfo}
	debugWriter := &levelWriter{level: logger.LevelDebug}
	logger.ErrorLogger.SetOutput(errorWriter)
	logger.InfoLogger.SetOutput(infoWriter)
	logger.DebugLogger.SetOutput(debugWriter)

	odm := NewOrderedDeliveryManager()

	receivedMessages := make(map[string][]types.Message)
	subscription := func(topic string) types.Subscription {
		return func(timestamp time.Time, content string) {
			receivedMessages[topic] = append(receivedMessages[topic], types.Message{Timestamp: timestamp, Topic: topic, Content: content})
		}
	}

	topics := []string{"topic1", "topic2"}
	now := time.Now()
	for _, topic := range topics {
		receivedMessages[topic] = make([]types.Message, 0)

		messages := []types.Message{
			{Timestamp: now.Add(2 * time.Second), Topic: topic, Content: "Message 3"},
			{Timestamp: now.Add(1 * time.Second), Topic: topic, Content: "Message 2"},
			{Timestamp: now, Topic: topic, Content: "Message 1"},
		}

		for _, msg := range messages {
			err := odm.DeliverMessage(msg, subscription(topic))
			if err != nil {
				t.Errorf("DeliverMessage failed for %s: %v", topic, err)
			}
		}
	}

	time.Sleep(3 * time.Second) // Wait for all messages to be processed

	for _, topic := range topics {
		if len(receivedMessages[topic]) != 3 {
			t.Errorf("Expected 3 messages for %s, got %d", topic, len(receivedMessages[topic]))
		}

		for i, msg := range receivedMessages[topic] {
			expectedContent := fmt.Sprintf("Message %d", i+1)
			if msg.Content != expectedContent {
				t.Errorf("Expected message content %s for %s, got %s", expectedContent, topic, msg.Content)
			}
		}

		if !bytes.Contains(infoWriter.Bytes(), []byte("New topic queue created for: "+topic)) {
			t.Errorf("Expected log message for new topic queue creation: %s", topic)
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
		{"ErrorLevel", logger.LevelError, false, false, false},
		{"InfoLevel", logger.LevelInfo, false, true, false},
		{"DebugLevel", logger.LevelDebug, false, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.SetLogLevel(tt.logLevel)

			errorWriter := &levelWriter{level: logger.LevelError}
			infoWriter := &levelWriter{level: logger.LevelInfo}
			debugWriter := &levelWriter{level: logger.LevelDebug}
			logger.ErrorLogger.SetOutput(errorWriter)
			logger.InfoLogger.SetOutput(infoWriter)
			logger.DebugLogger.SetOutput(debugWriter)

			odm := NewOrderedDeliveryManager()

			// Trigger all types of logs
			msg := types.Message{Timestamp: time.Now(), Topic: "test-topic", Content: "Test message"}
			err := odm.DeliverMessage(msg, func(timestamp time.Time, content string) {})
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Give some time for the goroutine to execute
			time.Sleep(50 * time.Millisecond)

			gotError := errorWriter.Len() > 0
			if gotError != tt.wantError {
				t.Errorf("Error logging: got %v, want %v", gotError, tt.wantError)
			}

			gotInfo := infoWriter.Len() > 0
			if gotInfo != tt.wantInfo {
				t.Errorf("Info logging: got %v, want %v", gotInfo, tt.wantInfo)
			}

			gotDebug := debugWriter.Len() > 0
			if gotDebug != tt.wantDebug {
				t.Errorf("Debug logging: got %v, want %v", gotDebug, tt.wantDebug)
			}

			// Print the log contents for debugging
			t.Logf("Error log: %s", errorWriter.String())
			t.Logf("Info log: %s", infoWriter.String())
			t.Logf("Debug log: %s", debugWriter.String())
		})
	}
}
