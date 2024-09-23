package backpressure

import (
	"fmt"
	"sync"
	"time"

	"github.com/edancain/RocketLab/bus/logger"
)

// BackPressureManager handles scenarios where publishers outpace subscribers
type BackPressureManager struct {
	topicRates map[string]*rate
	mutex      sync.RWMutex
	threshold  int
}

type rate struct {
	count     int
	timestamp time.Time
}

// NewBackPressureManager creates a new BackPressureManager
func NewBackPressureManager(threshold int) *BackPressureManager {
	return &BackPressureManager{
		topicRates: make(map[string]*rate),
		threshold:  threshold,
	}
}

// CheckPressure checks if a new message can be published to a topic
func (bpm *BackPressureManager) CheckPressure(topic string) error {
	bpm.mutex.Lock()
	defer bpm.mutex.Unlock()

	now := time.Now()
	if r, exists := bpm.topicRates[topic]; exists {
		if now.Sub(r.timestamp) < time.Second {
			r.count++
			if r.count > bpm.threshold {
				err := fmt.Errorf("back pressure applied: too many messages for topic %s", topic)
				if logger.GetLogLevel() >= logger.LevelError {
					logger.ErrorLogger.Println(err)
				}
				return err
			}
		} else {
			r.count = 1
			r.timestamp = now
		}
	} else {
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
