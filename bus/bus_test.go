package bus

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/edancain/RocketLab/bus/logger"
	"github.com/edancain/RocketLab/types"
)

func init() {
	logger.SetLogLevel(logger.LevelError)
}

func TestNewMessageBus(t *testing.T) {
	mb := NewMessageBus()
	if mb == nil {
		t.Error("NewMessageBus returned nil")
	}
}

func TestGetPublisher(t *testing.T) {
	mb := NewMessageBus()
	pub := mb.GetPublisher("test-topic")
	if pub == nil {
		t.Error("GetPublisher returned nil")
	}
}

func TestSubscribe(t *testing.T) {
	mb := NewMessageBus()
	received := make(chan types.Message, 1)
	unsubscribe := mb.Subscribe("test-topic", func(timestamp time.Time, message string) {
		received <- types.Message{Timestamp: timestamp, Topic: "test-topic", Content: message}
	})

	pub := mb.GetPublisher("test-topic")
	testMessage := "Hello, World!"
	err := pub.Publish(time.Now(), testMessage)
	if err != nil {
		t.Errorf("Publish failed: %v", err)
	}

	select {
	case msg := <-received:
		if msg.Content != testMessage {
			t.Errorf("Expected message %s, got %s", testMessage, msg.Content)
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}

	unsubscribe()
}

func TestStats(t *testing.T) {
	mb := NewMessageBus()
	pub := mb.GetPublisher("test-topic")
	mb.Subscribe("test-topic", func(timestamp time.Time, message string) {})

	for i := 0; i < 10; i++ {
		err := pub.Publish(time.Now(), "test message")
		if err != nil {
			t.Errorf("Publish failed: %v", err)
		}
	}

	stats := mb.Stats(time.Now())
	if stats.TotalMessages != 10 {
		t.Errorf("Expected 10 total messages, got %d", stats.TotalMessages)
	}
	if stats.PublisherCount != 1 {
		t.Errorf("Expected 1 publisher, got %d", stats.PublisherCount)
	}
	if stats.SubscriptionCount != 1 {
		t.Errorf("Expected 1 subscription, got %d", stats.SubscriptionCount)
	}
}

func TestConcurrency(t *testing.T) {
	mb := NewMessageBus()
	numPublishers := 10
	messagesPerPublisher := 1000

	var wg sync.WaitGroup
	wg.Add(numPublishers)

	for i := 0; i < numPublishers; i++ {
		go func(pubID int) {
			defer wg.Done()
			pub := mb.GetPublisher(fmt.Sprintf("topic-%d", pubID))
			for j := 0; j < messagesPerPublisher; j++ {
				err := pub.Publish(time.Now(), fmt.Sprintf("message-%d", j))
				if err != nil {
					t.Errorf("Publish failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	stats := mb.Stats(time.Now())
	expectedMessages := numPublishers * messagesPerPublisher
	if stats.TotalMessages != expectedMessages {
		t.Errorf("Expected %d total messages, got %d", expectedMessages, stats.TotalMessages)
	}
	if stats.PublisherCount != numPublishers {
		t.Errorf("Expected %d publishers, got %d", numPublishers, stats.PublisherCount)
	}
}

func TestMessageBusLogging(t *testing.T) {
	tests := []struct {
		name      string
		wantError bool
		wantInfo  bool
		wantDebug bool
	}{
		{"ErrorLevel", true, false, false},
		{"InfoLevel", true, false, false},
		{"DebugLevel", true, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var errorBuffer, infoBuffer, debugBuffer bytes.Buffer
			logger.ErrorLogger.SetOutput(&errorBuffer)
			logger.InfoLogger.SetOutput(&infoBuffer)
			logger.DebugLogger.SetOutput(&debugBuffer)

			mb := NewMessageBus()
			pub := mb.GetPublisher("test-topic")
			mb.Subscribe("test-topic", func(timestamp time.Time, message string) {})

			// Force an error condition
			mb.(*messageBus).backPressure = &mockBackPressure{shouldError: true}

			err := pub.Publish(time.Now(), "test message")
			if err == nil {
				t.Error("Expected an error, but got nil")
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

			logger.ErrorLogger.SetOutput(os.Stderr)
			logger.InfoLogger.SetOutput(os.Stdout)
			logger.DebugLogger.SetOutput(os.Stdout)
		})
	}
}

type mockBackPressure struct {
	shouldError bool
}

func (m *mockBackPressure) CheckPressure(topic string) error {
	if m.shouldError {
		return errors.New("mock back pressure error")
	}
	return nil
}
