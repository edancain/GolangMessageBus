package datadictionary

import (
	"testing"
	"time"

	"your_module_path/bus"
)

func TestStore(t *testing.T) {
	dd := NewDataDictionary()
	msg := bus.Message{
		Timestamp: time.Now(),
		Topic:     "test-topic",
		Content:   "Hello, World!",
	}

	err := dd.Store(msg)
	if err != nil {
		t.Errorf("Store failed: %v", err)
	}

	// Test storing more than maxMessagesPerTopic
	for i := 0; i < maxMessagesPerTopic; i++ {
		err = dd.Store(msg)
		if err != nil && i < maxMessagesPerTopic-1 {
			t.Errorf("Store failed unexpectedly: %v", err)
		}
	}

	if err == nil {
		t.Error("Expected error when exceeding maxMessagesPerTopic, got nil")
	}
}

func TestGetMessages(t *testing.T) {
	dd := NewDataDictionary()
	now := time.Now()

	msgs := []bus.Message{
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

	msgs := []bus.Message{
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
