package bus

import (
    "fmt"
    "sync"
    "testing"
    "time"

    "github.com/edancain/RocketLab/types"
)

// TestRocketTelemetrySimulation simulates a high-frequency telemetry data stream from a rocket.
// Note: In a real-world scenario, critical measurements might have redundant sensors and thus
// multiple publishers. For simplicity, this simulation uses a single publisher per data type.
func TestRocketTelemetrySimulation(t *testing.T) {
    mb := NewMessageBus()
    topics := []string{"latitude", "longitude", "altitude", "temperature", "fuel-level", "velocity", "atmospheric_pressure", "air_density", "orientation"}
    messageCount := 1000 // Simulating a short burst of high-frequency data
    
    // Create publishers
    publishers := make([]types.Publisher, len(topics))
    for i, topic := range topics {
        publishers[i] = mb.GetPublisher(topic)
    }

    // Create subscribers
    positionSubscriber := func(timestamp time.Time, message string) {
        // In a real scenario, this might update a 3D model
    }
    mb.Subscribe("latitude", positionSubscriber)
    mb.Subscribe("longitude", positionSubscriber)
    mb.Subscribe("altitude", positionSubscriber)
    mb.Subscribe("orientation", positionSubscriber)

    statusSubscriber := func(timestamp time.Time, message string) {
        // In a real scenario, this might update status displays
    }
    mb.Subscribe("temperature", statusSubscriber)
    mb.Subscribe("fuel-level", statusSubscriber)

    environmentSubscriber := func(timestamp time.Time, message string) {
        // In a real scenario, this might feed into flight calculations
    }
    mb.Subscribe("velocity", environmentSubscriber)
    mb.Subscribe("atmospheric_pressure", environmentSubscriber)
    mb.Subscribe("air_density", environmentSubscriber)

    // Simulate high frequency data publishing
    var wg sync.WaitGroup
    startTime := time.Now()
    for i := 0; i < messageCount; i++ {
        wg.Add(len(topics))
        for j, pub := range publishers {
            go func(p types.Publisher, topicIndex int) {
                defer wg.Done()
                timestamp := startTime.Add(time.Duration(i) * time.Millisecond)
                p.Publish(timestamp, fmt.Sprintf("Data point %d for %s", i, topics[topicIndex]))
            }(pub, j)
        }
    }
    wg.Wait()

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

    // Check frequency (stretch goal)
    for _, topic := range topics {
        if freq, exists := stats.TopicFrequency[topic]; !exists || freq < float64(messageCount)/60 {
            t.Errorf("Expected frequency for %s to be at least %f, got %f", topic, float64(messageCount)/60, freq)
        }
    }
}