package bus

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/edancain/RocketLab/bus/backpressure"
	"github.com/edancain/RocketLab/bus/datadictionary"
	"github.com/edancain/RocketLab/bus/logger"
	"github.com/edancain/RocketLab/bus/ordereddelivery"
	"github.com/edancain/RocketLab/bus/publisher"
	"github.com/edancain/RocketLab/types"
)

// MessageBus is the central coordinator of the system
// Acts as a central coordinator, implementing the MessageBus Interface
// Uses a mixture of Mutuxes, channels and Atomic Operations.
// using a combination of mutexes (for general synchronization), atomic
// operations (for counters), and uses channels in the BackPressureManager and OrderedDeliveryManager.
type messageBus struct {
	publishers  map[string]map[types.Publisher]struct{}
	subscribers map[string][]types.Subscription
	messages    types.DataDictionary

	// The BackPressureManager monitors the rate of message production and consumption.
	backPressure    types.BackPressureManager
	orderedDelivery types.OrderedDeliveryManager
	mutex           sync.RWMutex
	pubCount        atomic.Int64
	subCount        atomic.Int64
	msgCount        atomic.Int64
}

// Initializes a new MessageBus instance
func NewMessageBus() types.MessageBus {
	return &messageBus{
		publishers:      make(map[string]map[types.Publisher]struct{}),
		subscribers:     make(map[string][]types.Subscription),
		messages:        datadictionary.NewDataDictionary(),
		backPressure:    backpressure.NewBackPressureManager(1000), // This should return types.BackPressureManager
		orderedDelivery: ordereddelivery.NewOrderedDeliveryManager(),
	}
}

// GetPublisher returns a new Publisher for the given topic
func (mb *messageBus) GetPublisher(topic string) types.Publisher {
	mb.mutex.Lock()
	defer mb.mutex.Unlock()

	if _, exists := mb.publishers[topic]; !exists {
		mb.publishers[topic] = make(map[types.Publisher]struct{})
	}

	// create a new Publisher instance
	pub := publisher.NewPublisher(topic, mb)
	mb.publishers[topic][pub] = struct{}{}

	mb.pubCount.Add(1)

	return pub
}

// Subscribe registers a new subscription for the given topic
func (mb *messageBus) Subscribe(topic string, sub types.Subscription) (unsubscribe func()) {
	mb.mutex.Lock()
	defer mb.mutex.Unlock()

	mb.subscribers[topic] = append(mb.subscribers[topic], sub)
	mb.subCount.Add(1)

	return func() {
		mb.mutex.Lock()
		defer mb.mutex.Unlock()
		for i, s := range mb.subscribers[topic] {
			if &s == &sub {
				mb.subscribers[topic] = append(mb.subscribers[topic][:i], mb.subscribers[topic][i+1:]...)
				mb.subCount.Add(-1)
				break
			}
		}
	}
}

// publish sends a message to all subscribers of the given topic
func (mb *messageBus) PublishMessage(msg types.Message) error {
	mb.mutex.RLock()
	defer mb.mutex.RUnlock()

	if err := mb.backPressure.CheckPressure(msg.Topic); err != nil {
		logger.ErrorLogger.Printf("Back pressure error for topic %s: %v", msg.Topic, err)
		return err
	}

	if err := mb.messages.Store(msg); err != nil {
		logger.ErrorLogger.Printf("Failed to store message for topic %s: %v", msg.Topic, err)
		return err
	}

	subscribers, exists := mb.subscribers[msg.Topic]
	if exists {
		for _, sub := range subscribers {
			// goroutine that creates a new goroutine for each subscriber to deliver the message asynchronously.
			go func(s types.Subscription) {
				if err := mb.orderedDelivery.DeliverMessage(msg, s); err != nil {
					logger.ErrorLogger.Printf("Error delivering message for topic %s: %v", msg.Topic, err)
				}
			}(sub)
		}
	}

	mb.msgCount.Add(1)
	return nil
}

// Stats returns the current stats of the MessageBus
func (mb *messageBus) Stats(now time.Time) types.Stats {
	mb.mutex.RLock()
	defer mb.mutex.RUnlock()

	return types.Stats{
		TotalMessages:     int(mb.msgCount.Load()),
		PublisherCount:    int(mb.pubCount.Load()),
		SubscriptionCount: int(mb.subCount.Load()),
		TopicFrequency:    mb.calculateTopicFrequency(now),
	}
}

// This method calculates the message frequency for each topic over the last 60 seconds. It's called by the Stats method and is protected by the same read lock.
func (mb *messageBus) calculateTopicFrequency(now time.Time) map[string]float64 {
	frequencies := make(map[string]float64)
	sixtySecondsAgo := now.Add(-60 * time.Second)

	for topic := range mb.subscribers {
		messages := mb.messages.GetMessages(topic, sixtySecondsAgo, now)
		if len(messages) > 0 {
			duration := messages[len(messages)-1].Timestamp.Sub(messages[0].Timestamp).Seconds()
			if duration > 0 {
				frequencies[topic] = float64(len(messages)) / duration
			}
		}
	}

	return frequencies
}

// This method removes a publisher from the publishers map and decrements the pubCount. It uses a write lock because it modifies the map structure.
func (mb *messageBus) RemovePublisher(p types.Publisher) {
	mb.mutex.Lock()
	defer mb.mutex.Unlock()

	for topic, publishers := range mb.publishers {
		if _, exists := publishers[p]; exists {
			delete(publishers, p)
			mb.pubCount.Add(-1)
			if len(publishers) == 0 {
				delete(mb.publishers, topic)
			}
			break
		}
	}
}
