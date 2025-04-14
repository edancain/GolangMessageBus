package publisher

import (
	"bytes"
	"testing"
	"time"

	"github.com/edancain/GolangMessageBus/bus/logger"
	"github.com/edancain/GolangMessageBus/types"
)

// MockMessageBus is a mock implementation of the MessageBus interface for testing
type MockMessageBus struct {
	publishedMessages []types.Message
	removedPublishers []types.Publisher
}

func (m *MockMessageBus) PublishMessage(msg types.Message) error {
	m.publishedMessages = append(m.publishedMessages, msg)
	return nil
}

func (m *MockMessageBus) GetPublisher(topic string) types.Publisher {
	return NewPublisher(topic, m)
}

func (m *MockMessageBus) Subscribe(topic string, sub types.Subscription) func() {
	return func() {}
}

func (m *MockMessageBus) Stats(now time.Time) types.Stats {
	return types.Stats{}
}

func (m *MockMessageBus) RemovePublisher(p types.Publisher) {
	m.removedPublishers = append(m.removedPublishers, p)
}

func TestPublish(t *testing.T) {
	mockBus := &MockMessageBus{}
	pub := NewPublisher("test-topic", mockBus)

	testMessage := "Hello, World!"
	testTime := time.Now()
	err := pub.Publish(testTime, testMessage)
	if err != nil {
		t.Errorf("Publish failed: %v", err)
	}

	if len(mockBus.publishedMessages) != 1 {
		t.Errorf("Expected 1 published message, got %d", len(mockBus.publishedMessages))
	}

	publishedMsg := mockBus.publishedMessages[0]
	if publishedMsg.Topic != "test-topic" || publishedMsg.Content != testMessage || !publishedMsg.Timestamp.Equal(testTime) {
		t.Errorf("Published message does not match expected values")
	}
}

func TestClose(t *testing.T) {
	mockBus := &MockMessageBus{}
	pub := NewPublisher("test-topic", mockBus)

	pub.Close()

	if len(mockBus.removedPublishers) != 1 {
		t.Errorf("Expected 1 removed publisher, got %d", len(mockBus.removedPublishers))
	}

	// Attempt to publish after closing
	err := pub.Publish(time.Now(), "This should fail")
	if err == nil {
		t.Error("Expected error when publishing to closed publisher, got nil")
	}
}

func TestPublisherLogging(t *testing.T) {
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

			// Create buffers to capture log output
			var errorBuffer, infoBuffer, debugBuffer bytes.Buffer
			logger.ErrorLogger.SetOutput(&errorBuffer)
			logger.InfoLogger.SetOutput(&infoBuffer)
			logger.DebugLogger.SetOutput(&debugBuffer)

			// Create a mock MessageBus and a Publisher
			mockBus := &MockMessageBus{}
			pub := NewPublisher("test-topic", mockBus)

			// Publish a message
			err := pub.Publish(time.Now(), "test message")
			if err != nil {
				t.Fatalf("Publish failed: %v", err)
			}

			// Close the publisher
			pub.Close()

			// Attempt to publish after closing (should produce an error log)
			err = pub.Publish(time.Now(), "This should fail")
			if err == nil {
				t.Error("Expected an error, but got nil")
			}

			// Check Error logs
			gotError := errorBuffer.Len() > 0
			if gotError != tt.wantError {
				t.Errorf("Error logging: got %v, want %v", gotError, tt.wantError)
			}

			// Check Info logs
			gotInfo := infoBuffer.Len() > 0
			if gotInfo != tt.wantInfo {
				t.Errorf("Info logging: got %v, want %v", gotInfo, tt.wantInfo)
			}

			// Check Debug logs
			gotDebug := debugBuffer.Len() > 0
			if gotDebug != tt.wantDebug {
				t.Errorf("Debug logging: got %v, want %v", gotDebug, tt.wantDebug)
			}
		})
	}
}
