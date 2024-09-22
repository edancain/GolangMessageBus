package datadictionary

import (
	"sync"
    "time"
    "container/list"
	"fmt"

    "github.com/edancain/RocketLab/types"
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
func (dd *DataDictionary) Store(msg types.Message) error {
    dd.mutex.Lock()
    defer dd.mutex.Unlock()

    if _, exists := dd.messages[msg.Topic]; !exists {
        dd.messages[msg.Topic] = list.New()
        logger.InfoLogger.Printf("New topic created in DataDictionary: %s", msg.Topic)
    }

    if dd.messages[msg.Topic].Len() >= maxMessagesPerTopic {
        err := fmt.Errorf("max messages per topic reached for topic %s", msg.Topic)
        logger.ErrorLogger.Printf("Failed to store message: %v", err)
        return err
    }

    dd.messages[msg.Topic].PushBack(msg)
    logger.DebugLogger.Printf("Stored message for topic %s", msg.Topic)
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
    
    logger.DebugLogger.Printf("Retrieved %d messages for topic %s between %v and %v", len(result), topic, start, end)
    
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
            } else {
                // If we've found a message that's not expired, we can stop searching
                break
            }
        }
        if msgList.Len() == 0 {
            delete(dd.messages, topic)
            logger.InfoLogger.Printf("Removed empty topic %s", topic)
        }
        logger.DebugLogger.Printf("Cleaned up %d messages for topic %s", removedCount, topic)
    }
}