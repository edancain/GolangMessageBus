package bus

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/edancain/OperationsSoftware/bus/logger"
	"github.com/edancain/OperationsSoftware/types"
)

func TestRocketTelemetrySimulation(t *testing.T) {
	logger.SetLogLevel(logger.LevelDebug)

	mb := NewMessageBus()
	topics := []string{"latitude", "longitude", "altitude", "temperature", "fuel-level", "velocity", "atmospheric_pressure", "air_density", "orientation"}
	messageCount := 1000 // Simulating a short burst of high-frequency data

	// Create publishers
	publishers := make([]types.Publisher, len(topics))
	for i, topic := range topics {
		publishers[i] = mb.GetPublisher(topic)
	}

	// Create subscribers
	receivedMessages := make(map[string]int)
	var mu sync.Mutex

	createSubscriber := func(name string) types.Subscription {
		return func(timestamp time.Time, message string) {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages[name]++
			logger.DebugLogger.Printf("Received message for %s: %s", name, message)
		}
	}

	unsubscribeFuncs := make(map[string]func())

	unsubscribeFuncs["latitude"] = mb.Subscribe("latitude", createSubscriber("position"))
	unsubscribeFuncs["longitude"] = mb.Subscribe("longitude", createSubscriber("position"))
	unsubscribeFuncs["altitude"] = mb.Subscribe("altitude", createSubscriber("position"))
	unsubscribeFuncs["orientation"] = mb.Subscribe("orientation", createSubscriber("position"))

	unsubscribeFuncs["temperature"] = mb.Subscribe("temperature", createSubscriber("status"))
	unsubscribeFuncs["fuel-level"] = mb.Subscribe("fuel-level", createSubscriber("status"))

	unsubscribeFuncs["velocity"] = mb.Subscribe("velocity", createSubscriber("environment"))
	unsubscribeFuncs["atmospheric_pressure"] = mb.Subscribe("atmospheric_pressure", createSubscriber("environment"))
	unsubscribeFuncs["air_density"] = mb.Subscribe("air_density", createSubscriber("environment"))

	// Simulate high frequency data publishing
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < messageCount; i++ {
		wg.Add(len(topics))
		for j, pub := range publishers {
			go func(p types.Publisher, topicIndex int) {
				defer wg.Done()
				timestamp := startTime.Add(time.Duration(i) * time.Millisecond)
				err := p.Publish(timestamp, fmt.Sprintf("Data point %d for %s", i, topics[topicIndex]))
				if err != nil {
					t.Errorf("Failed to publish message: %v", err)
				}
			}(pub, j)
		}
	}
	wg.Wait()

	// Wait for all messages to be processed
	time.Sleep(1 * time.Second)

	// Check stats
	stats := mb.Stats(time.Now())
	if stats.TotalMessages != messageCount*len(topics) {
		t.Errorf("Expected %d messages, got %d", messageCount*len(topics), stats.TotalMessages)
	}
	if stats.PublisherCount != len(topics) {
		t.Errorf("Expected %d publishers, got %d", len(topics), stats.PublisherCount)
	}
	expectedSubscriptions := 9 // 4 for position, 2 for status, 3 for environment
	if stats.SubscriptionCount != expectedSubscriptions {
		t.Errorf("Expected %d subscriptions, got %d", expectedSubscriptions, stats.SubscriptionCount)
	}

	// Check received messages
	expectedMessagesPerType := messageCount * 4 // 4 topics for position
	if receivedMessages["position"] != expectedMessagesPerType {
		t.Errorf("Expected %d position messages, got %d", expectedMessagesPerType, receivedMessages["position"])
	}
	expectedMessagesPerType = messageCount * 2 // 2 topics for status
	if receivedMessages["status"] != expectedMessagesPerType {
		t.Errorf("Expected %d status messages, got %d", expectedMessagesPerType, receivedMessages["status"])
	}
	expectedMessagesPerType = messageCount * 3 // 3 topics for environment
	if receivedMessages["environment"] != expectedMessagesPerType {
		t.Errorf("Expected %d environment messages, got %d", expectedMessagesPerType, receivedMessages["environment"])
	}

	// Check frequency
	for _, topic := range topics {
		if freq, exists := stats.TopicFrequency[topic]; !exists || freq < float64(messageCount)/60 {
			t.Errorf("Expected frequency for %s to be at least %f, got %f", topic, float64(messageCount)/60, freq)
		}
	}

	logger.InfoLogger.Printf("Test completed. Total messages: %d, Publishers: %d, Subscriptions: %d",
		stats.TotalMessages, stats.PublisherCount, stats.SubscriptionCount)

	// unsubscibe from a single topic.
	unsubscribeFuncs["latitude"]()

	// Unsubscribe from all topics, ensure that all subscriptions are cleaned up after the test, regardless of whether it passes or fails.
	t.Cleanup(func() {
		for _, unsubscribe := range unsubscribeFuncs {
			unsubscribe()
		}
	})
}
