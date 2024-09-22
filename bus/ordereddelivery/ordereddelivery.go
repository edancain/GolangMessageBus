package ordereddelivery

import (
	"container/heap"
	"sync"
	"time"

	"github.com/edancain/RocketLab/types"
	"github.com/edancain/RocketLab/bus/logger"
)

// OrderedDeliveryManager ensures messages are delivered in order
type OrderedDeliveryManager struct {
	topicQueues map[string]*priorityQueue
	mutex       sync.Mutex
}

// priorityQueue is a min-heap of Messages
type priorityQueue []types.Message

func (pq priorityQueue) Len() int { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Timestamp.Before(pq[j].Timestamp)
}
func (pq priorityQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }
func (pq *priorityQueue) Push(x interface{}) { *pq = append(*pq, x.(types.Message)) }
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

// NewOrderedDeliveryManager creates a new OrderedDeliveryManager
func NewOrderedDeliveryManager() *OrderedDeliveryManager {
	return &OrderedDeliveryManager{
		topicQueues: make(map[string]*priorityQueue),
	}
}

// DeliverMessage delivers a message to a subscriber in order
func (odm *OrderedDeliveryManager) DeliverMessage(msg types.Message, sub types.Subscription) error {
    odm.mutex.Lock()
    defer odm.mutex.Unlock()

    if _, exists := odm.topicQueues[msg.Topic]; !exists {
        pq := make(priorityQueue, 0)
        odm.topicQueues[msg.Topic] = &pq
        if logger.IsInfoEnabled() {
			logger.InfoLogger.Printf("New topic queue created for: %s", msg.Topic)
		}
		
    }

    queue := odm.topicQueues[msg.Topic]
    heap.Push(queue, msg)

    deliveredCount := 0
    for queue.Len() > 0 {
        nextMsg := heap.Pop(queue).(types.Message)
        if time.Since(nextMsg.Timestamp) >= 0 {
            sub(nextMsg.Timestamp, nextMsg.Content)
            deliveredCount++
        } else {
            heap.Push(queue, nextMsg)
        }
    }

    if logger.IsDebugEnabled() {
		logger.DebugLogger.Printf("Delivered %d messages for topic %s", deliveredCount, msg.Topic)
	}
    return nil
}