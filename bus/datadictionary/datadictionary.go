package datadictionary

import (
	"sync"
    "time"
    "container/list"
    "errors"

    "github.com/edancain/RocketLab/bus/types"
	"github.com/edancain/RocketLab/bus/logger"
)

const (
	maxMessagesPerTopic   = 10000
	messageExpirationTime = 24 * time.Hour
)

// DataDictionary stores messages for persistence and potential replay
type DataDictionary struct {
	messages map[string]*list.List // map of topic to list of messages
	mutex    sync.RWMutex
}

// NewDataDictionary creates a new DataDictionary
func NewDataDictionary() *DataDictionary {
	dd := &DataDictionary{
		messages: make(map[string]*list.List),
	}
	go dd.periodicCleanup()
	return dd
}

// Store adds a new message to the DataDictionary
func (dd *DataDictionary) Store(msg bus.Message) error {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	if _, exists := dd.messages[msg.Topic]; !exists {
		dd.messages[msg.Topic] = list.New()
		if logger.IsInfoEnabled() {
			logger.InfoLogger.Printf("New topic created in DataDictionary: %s", msg.Topic)
		}
	}

	if dd.messages[msg.Topic].Len() >= maxMessagesPerTopic {
		logger.ErrorLogger.Printf("Max messages reached for topic: %s", msg.Topic)
		return errors.New("max messages per topic reached")
	}

	dd.messages[msg.Topic].PushBack(msg)
	
	if logger.IsDebugEnabled() {
		logger.DebugLogger.Printf("Message stored for topic %s, total messages: %d", msg.Topic, dd.messages[msg.Topic].Len())
	}
	
	return nil
}

// GetMessages retrieves messages for a given topic and time range
func (dd *DataDictionary) GetMessages(topic string, start, end time.Time) []types.Message {
	dd.mutex.RLock()
	defer dd.mutex.RUnlock()

	var result []types.Message
	if msgList, exists := dd.messages[topic]; exists {
		for e := msgList.Front(); e != nil; e = e.Next() {
			msg := e.Value.(types.Message)
			if msg.Timestamp.After(start) && msg.Timestamp.Before(end) {
				result = append(result, msg)
			}
		}
	}
	
	if logger.IsDebugEnabled() {
		logger.DebugLogger.Printf("Retrieved %d messages for topic %s between %v and %v", len(result), topic, start, end)
	}
	
	return result
}

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
			if now.Sub(msg.Timestamp) > messageExpirationTime {
				msgList.Remove(e)
				removedCount++
			}
		}
		if msgList.Len() == 0 {
			delete(dd.messages, topic)
			if logger.IsInfoEnabled() {
				logger.InfoLogger.Printf("Removed empty topic from DataDictionary: %s", topic)
			}
		}
		if logger.IsDebugEnabled() && removedCount > 0 {
			logger.DebugLogger.Printf("Cleaned up %d expired messages for topic %s", removedCount, topic)
		}
	}
}