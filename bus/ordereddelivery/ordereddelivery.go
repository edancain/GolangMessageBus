package ordereddelivery

import (
	"container/heap"
	"sync"
	"time"

	"github.com/edancain/OperationsSoftware/bus/logger"
	"github.com/edancain/OperationsSoftware/types"
)

// OrderedDeliveryManager ensures messages are delivered in order
type OrderedDeliveryManager struct {
	topicQueues map[string]*priorityQueue // A map where keys are topic strings and values are pointers to priorityQueues.
	mutex       sync.Mutex                // Used for thread-safe access to the topicQueues map.
}

// This is a custom implementation of a min-heap, specifically for Message types. It's based on the built-in slice type.
// The priorityQueue implementation:
// It's a min-heap, meaning the smallest element (earliest timestamp) is always at the top.
// Less(i, j int) is key: it defines the ordering. Here, earlier timestamps are "less than" later ones.
// Push and Pop operate on the end of the slice, but the heap package reorders elements to maintain the heap property.
type priorityQueue []types.Message

// Heap interface methods. The use of a heap data structure provides O(log n) time complexity for insertion and removal operations, making it efficient even for large numbers of messages.
func (pq priorityQueue) Len() int { return len(pq) } // Returns the length of the queue.
// Compares two elements. Here, it compares timestamps, making this a min-heap based on timestamp.
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Timestamp.Before(pq[j].Timestamp)
}
func (pq priorityQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }          // Swaps two elements in the queue.
func (pq *priorityQueue) Push(x interface{}) { *pq = append(*pq, x.(types.Message)) } // Adds a new element to the queue.

// Removes and returns the last element. (For a min-heap, this is counterintuitive,
// but the heap package uses this in combination with Less() to maintain heap property)
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

// Creates and returns a new OrderedDeliveryManager with an initialized topicQueues map.
func NewOrderedDeliveryManager() *OrderedDeliveryManager {
	return &OrderedDeliveryManager{
		topicQueues: make(map[string]*priorityQueue),
	}
}

// Locks the mutex for thread-safe access.
// Checks if a queue exists for the topic, creates one if it doesn't.
// Pushes the new message onto the appropriate queue.
// Starts a new goroutine to process the queue.
func (odm *OrderedDeliveryManager) DeliverMessage(msg types.Message, sub types.Subscription) error {
	odm.mutex.Lock()
	defer odm.mutex.Unlock()

	if _, exists := odm.topicQueues[msg.Topic]; !exists {
		pq := make(priorityQueue, 0)
		odm.topicQueues[msg.Topic] = &pq
		// heap.Init(queue) This establishes the heap property for a new queue.
		heap.Init(odm.topicQueues[msg.Topic])
		if logger.IsInfoEnabled() {
			logger.InfoLogger.Printf("New topic queue created for: %s", msg.Topic)
		}
	}

	queue := odm.topicQueues[msg.Topic]
	heap.Push(queue, msg) // This adds a new message and reorders the heap to maintain the property.

	if logger.IsDebugEnabled() {
		logger.DebugLogger.Printf("Message added to queue for topic %s", msg.Topic)
	}

	go odm.processQueue(msg.Topic, sub)

	return nil
}

// Locks the mutex.
// Pops messages from the queue and schedules them for delivery.
// Each message is delivered in a separate goroutine, with a sleep to ensure it's delivered at the right time.
func (odm *OrderedDeliveryManager) processQueue(topic string, sub types.Subscription) {
	odm.mutex.Lock()
	defer odm.mutex.Unlock()

	queue := odm.topicQueues[topic]
	deliveredCount := 0

	for queue.Len() > 0 {
		nextMsg := heap.Pop(queue).(types.Message) // This removes and returns the top (earliest) message.
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
