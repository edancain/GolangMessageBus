package ordereddelivery

import (
	"testing"
	"time"

	"your_module_path/bus"
)

func TestDeliverMessage(t *testing.T) {
	odm := NewOrderedDeliveryManager()

	receivedMessages := make([]bus.Message, 0)
	subscription := func(timestamp time.Time, content string) {
		receivedMessages = append(receivedMessages, bus.Message{Timestamp: timestamp, Content: content})
	}

	messages := []bus.Message{
		{Timestamp: time.Now().Add(2 * time.Second), Topic: "test-topic", Content: "Message 3"},
		{Timestamp: time.Now().Add(1 * time.Second), Topic: "test-topic", Content: "Message 2"},
		{Timestamp: time.Now(), Topic: "test-topic", Content: "Message 1"},
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
		expectedContent := "Message " + string(i+1)
		if msg.Content != expectedContent {
			t.Errorf("Expected message content %s, got %s", expectedContent, msg.Content)
		}
	}
}

func TestMultipleTopics(t *testing.T) {
	odm := NewOrderedDeliveryManager()

	receivedMessages := make(map[string][]bus.Message)
	subscription := func(topic string) bus.Subscription {
		return func(timestamp time.Time, content string) {
			receivedMessages[topic] = append(receivedMessages[topic], bus.Message{Timestamp: timestamp, Topic: topic, Content: content})
		}
	}

	topics := []string{"topic1", "topic2"}
	for _, topic := range topics {
		receivedMessages[topic] = make([]bus.Message, 0)

		messages := []bus.Message{
			{Timestamp: time.Now().Add(2 * time.Second), Topic: topic, Content: "Message 3"},
			{Timestamp: time.Now().Add(1 * time.Second), Topic: topic, Content: "Message 2"},
			{Timestamp: time.Now(), Topic: topic, Content: "Message 1"},
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
			expectedContent := "Message " + string(i+1)
			if msg.Content != expectedContent {
				t.Errorf("Expected message content %s for %s, got %s", expectedContent, topic, msg.Content)
			}
		}
	}
}
