package datadictionary

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/edancain/RocketLab/bus/logger"
	"github.com/edancain/RocketLab/types"
)

const (
	maxMessagesPerTopic   = 10000
	messageExpirationTime = 24 * time.Hour
)

// DataDictionary stores messages for persistence and potential replay

// DataDictionary uses a map (messages) where keys are topic strings and values are pointers to list.List.
// Each list.List contains messages for a specific topic.
type DataDictionary struct {
	messages map[string]*list.List // map of topic to list of messages. Uses list.List for O(1) insertion at the end and efficient removal of elements.
	mutex    sync.RWMutex          // for thread-safe access to the map.
}

// NewDataDictionary creates a new DataDictionary
func NewDataDictionary() *DataDictionary {
	dd := &DataDictionary{
		messages: make(map[string]*list.List),
	}
	// Starts a goroutine for periodic cleanup
	go dd.periodicCleanup()
	return dd
}

// Store adds a new message to the DataDictionary
func (dd *DataDictionary) Store(msg types.Message) error {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	if _, exists := dd.messages[msg.Topic]; !exists {
		dd.messages[msg.Topic] = list.New()
		if logger.GetLogLevel() >= logger.LevelInfo {
			logger.InfoLogger.Printf("New topic created in DataDictionary: %s", msg.Topic)
		}
	}

	if dd.messages[msg.Topic].Len() >= maxMessagesPerTopic {
		err := fmt.Errorf("max messages per topic reached for topic %s", msg.Topic)
		if logger.GetLogLevel() >= logger.LevelError {
			logger.ErrorLogger.Printf("Failed to store message: %v", err)
		}
		return err
	}

	dd.messages[msg.Topic].PushBack(msg)
	if logger.GetLogLevel() >= logger.LevelDebug {
		logger.DebugLogger.Printf("Stored message for topic %s", msg.Topic)
	}
	return nil
}

// GetMessages retrieves messages for a given topic and time range
func (dd *DataDictionary) GetMessages(topic string, start, end time.Time) []types.Message {
	dd.mutex.RLock()
	defer dd.mutex.RUnlock()

	var result []types.Message // slice of matching messages.

	// Next if statement:
	// How msgList is assigned:
	// The line if msgList, exists := dd.messages[topic]; exists { is doing two things:
	// a. It's attempting to retrieve the list of messages for the given topic from the dd.messages map.
	// b. It's assigning the result to msgList and a boolean exists.
	// In Go, this is called a "comma ok" idiom. When you access a map with a key, it returns two values: the value associated with the key (if it exists) and a boolean indicating whether the key was present in the map.
	// So, msgList will be assigned the *list.List for the given topic if it exists in the map. If the topic doesn't exist in the map, msgList will be nil and exists will be false.
	if msgList, exists := dd.messages[topic]; exists {
		for e := msgList.Front(); e != nil; e = e.Next() {
			msg := e.Value.(types.Message)
			if msg.Timestamp.After(start) && msg.Timestamp.Before(end) {
				result = append(result, msg)
			}
		}
	}

	// Remember, in a production environment, you might want to add more edge cases, such as testing with the exact boundary
	// times, testing with a large number of messages, and testing concurrent access if that's a concern in your use case.

	logger.DebugLogger.Printf("Retrieved %d messages for topic %s between %v and %v", len(result), topic, start, end)

	// return a slice of matching messages.
	return result
}

// Runs every hour (using time.NewTicker).
// Periodically removes expired messages to free up memory.
func (dd *DataDictionary) periodicCleanup() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		dd.cleanupExpiredMessages()
	}
}

func (dd *DataDictionary) cleanupExpiredMessages() {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	now := time.Now()
	for topic, msgList := range dd.messages {
		var next *list.Element
		removedCount := 0
		for e := msgList.Front(); e != nil; e = next {
			next = e.Next()
			msg := e.Value.(types.Message)

			// Remove messages older than messageExpirationTime.
			if now.Sub(msg.Timestamp) > messageExpirationTime {
				msgList.Remove(e)
				removedCount++
			} else {
				// If we've found a message that's not expired, we can stop searching
				// Stops cleanup early if it encounters a non-expired message, assuming time-ordered messages.
				break
			}
		}

		// Remove empty topics
		if msgList.Len() == 0 {
			delete(dd.messages, topic)
			logger.InfoLogger.Printf("Removed empty topic %s", topic)
		}
		logger.DebugLogger.Printf("Cleaned up %d messages for topic %s", removedCount, topic)
	}
}
