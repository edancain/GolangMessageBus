package backpressure

import (
	"errors"
	"sync"
	"time"

	"github.com/edancain/RocketLab/bus/logger"
)

// BackPressureManager handles scenarios where publishers outpace subscribers
type BackPressureManager struct {
	topicRates map[string]*rate
	mutex      sync.RWMutex
	threshold int
}

type rate struct {
	count     int
	timestamp time.Time
}

// NewBackPressureManager creates a new BackPressureManager
func NewBackPressureManager(threshold int) *BackPressureManager {
	return &BackPressureManager{
		topicRates: make(map[string]*rate),
		threshold: threshold,
	}
}

// CheckPressure checks if a new message can be published to a topic
func (bpm *BackPressureManager) CheckPressure(topic string) error {
	bpm.mutex.Lock()
	defer bpm.mutex.Unlock()

	now := time.Now()
	if r, exists := bpm.topicRates[topic]; exists {
		if now.Sub(r.timestamp) < time.Second {
			if r.count > bpm.threshold { // Example threshold: 1000 messages per second
				logger.ErrorLogger.Printf("back pressure applied: too many messages")
				return errors.New("back pressure applied: too many messages")
			}
			r.count++
		} else {
			r.count = 1
			r.timestamp = now
		}
	} else {
			bpm.topicRates[topic] = &rate{count: 1, timestamp: now}
			if logger.IsInfoEnabled() {
				logger.InfoLogger.Printf("New topic added to BackPressureManager: %s", topic)
			}
	}
		
	if logger.IsDebugEnabled() {
			logger.DebugLogger.Printf("Topic %s: current count %d, threshold %d", topic, bpm.topicRates[topic].count, bpm.threshold)
	}
		
	return nil
}
