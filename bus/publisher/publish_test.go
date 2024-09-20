package publisher

import (
	"testing"
	"time"

	"github.com/edancain/RocketLab/bus"
)

type mockMessageBus struct {
	publishedMessages []bus.Message
	removedPublishers []bus.Publisher
}

func (m *mockMessageBus) GetPublisher(topic string) bus.Publisher {
	return NewPublisher(topic, m)
}

func (m *mockMessageBus) Subscribe(topic string, sub bus.Subscription) func() {
	return func() {}
}

func (m *mockMessageBus) Stats(now time.Time) bus.Stats {
	return bus.Stats{}
}

func (m *mockMessageBus) PublishMessage(msg bus.Message) error {
	m.publishedMessages = append(m.publishedMessages, msg)
	return nil
}

func (m *mockMessageBus) RemovePublisher(p bus.Publisher) {
	m.removedPublishers = append(m.removedPublishers, p)
}

func TestPublish(t *testing.T) {
	mockBus := &mockMessageBus{}
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
	mockBus := &mockMessageBus{}
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
