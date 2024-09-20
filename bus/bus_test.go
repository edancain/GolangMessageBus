package bus

import (
	"sync"
	"testing"
	"time"
)

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
	received := make(chan Message, 1)
	unsubscribe := mb.Subscribe("test-topic", func(timestamp time.Time, message string) {
		received <- Message{Timestamp: timestamp, Topic: "test-topic", Content: message}
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
