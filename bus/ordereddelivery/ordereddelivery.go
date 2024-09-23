package ordereddelivery

import (
	"container/heap"
	"sync"
	"time"

	"github.com/edancain/RocketLab/bus/logger"
	"github.com/edancain/RocketLab/types"
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
		heap.Init(odm.topicQueues[msg.Topic])
		if logger.IsInfoEnabled() {
			logger.InfoLogger.Printf("New topic queue created for: %s", msg.Topic)
		}
	}

	queue := odm.topicQueues[msg.Topic]
	heap.Push(queue, msg)

	if logger.IsDebugEnabled() {
		logger.DebugLogger.Printf("Message added to queue for topic %s", msg.Topic)
	}

	go odm.processQueue(msg.Topic, sub)

	return nil
}

func (odm *OrderedDeliveryManager) processQueue(topic string, sub types.Subscription) {
	odm.mutex.Lock()
	defer odm.mutex.Unlock()

	queue := odm.topicQueues[topic]
	deliveredCount := 0

	for queue.Len() > 0 {
		nextMsg := heap.Pop(queue).(types.Message)
		go func(m types.Message) {
			time.Sleep(time.Until(m.Timestamp))
			sub(m.Timestamp, m.Content)
			if logger.IsDebugEnabled() {
				logger.DebugLogger.Printf("Message delivered for topic %s: %s", topic, m.Content)
			}
		}(nextMsg)
		deliveredCount++
	}

	if logger.IsDebugEnabled() {
		logger.DebugLogger.Printf("Scheduled %d messages for delivery for topic %s", deliveredCount, topic)
	}
}
