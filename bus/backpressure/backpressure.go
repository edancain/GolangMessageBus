package backpressure

import (
	"fmt"
	"sync"
	"time"

	"github.com/edancain/GolangMessageBus/bus/logger"
)

// BackPressureManager:
// CheckPressure: If the number of messages for a topic exceeds the threshold within one second, the function returns an error.
// This error is then handled by the caller (the PublishMessage function of the messageBus), its intention is to prevent the message from being published.

// The rate for each topic is reset every second. This allows for bursts of messages as long as the average rate over time doesn't exceed the threshold.

// The use of a mutex (sync.RWMutex) ensures that the topicRates map can be safely accessed and modified in a concurrent environment.

// By using a map to store rates for each topic, this implementation allows different topics to have different traffic patterns without affecting each other.

// This mechanism works like a traffic controller:
// a: Allows messages to flow freely as long as they're within the defined rate limit.
// b: If too many messages come too quickly for a particular topic, it starts rejecting new messages for that topic.
// c: After a second passes, it "resets the counter," allowing messages to flow again.

// The intention of Back Pressure is to help prevent system overload by temporarily slowing down publishers when they're producing messages faster than the system can handle,
// while still allowing for short bursts of high activity.

// BackPressureManager handles scenarios where publishers outpace subscribers
type BackPressureManager struct {
	topicRates map[string]*rate // keeps track of the publishing rate for each topic. Each topic's rate is represented by a rate struct, which contains a
	// count of messages and a timestamp.
	mutex     sync.RWMutex
	threshold int // defines the maximum number of messages allowed per second for each topic.
}

type rate struct {
	count     int
	timestamp time.Time
}

func NewBackPressureManager(threshold int) *BackPressureManager {
	return &BackPressureManager{
		topicRates: make(map[string]*rate),
		threshold:  threshold,
	}
}

// CheckPressure checks if a new message can be published to a topic
func (bpm *BackPressureManager) CheckPressure(topic string) error {
	// acquire a lock to ensure thread-safe access to the topicRates map.
	bpm.mutex.Lock()
	defer bpm.mutex.Unlock()

	now := time.Now()
	if r, exists := bpm.topicRates[topic]; exists {
		// check if less than a second has passed since the last message
		if now.Sub(r.timestamp) < time.Second {
			r.count++
			// If the count exceeds the threshold, return an error, apply back pressure.
			if r.count > bpm.threshold {
				err := fmt.Errorf("back pressure applied: too many messages for topic %s", topic)
				if logger.GetLogLevel() >= logger.LevelError {
					logger.ErrorLogger.Println(err)
				}
				return err
			}
		} else {
			// If more than a second has passed, reset the count to 1 and update the timestamp.
			r.count = 1
			r.timestamp = now
		}
	} else {
		//If the topic doesn't exist, create a new rate entry for this topic with a count of 1 and the current timestamp.
		bpm.topicRates[topic] = &rate{count: 1, timestamp: now}
		if logger.GetLogLevel() >= logger.LevelInfo {
			logger.InfoLogger.Printf("New topic created in BackPressureManager: %s", topic)
		}
	}

	if logger.GetLogLevel() >= logger.LevelDebug {
		logger.DebugLogger.Printf("Message count for topic %s: %d", topic, bpm.topicRates[topic].count)
	}

	return nil
}
