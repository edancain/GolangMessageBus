package publisher

import (
	"bytes"
	"testing"
	"time"

	"github.com/edancain/RocketLab/bus/logger"
	"github.com/edancain/RocketLab/types"
)

// MockMessageBus is a mock implementation of the MessageBus interface for testing
type MockMessageBus struct {
	publishedMessages []types.Message
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

func (m *MockMessageBus) RemovePublisher(p types.Publisher) {}

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

			// Check Error logs (should always be present)
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