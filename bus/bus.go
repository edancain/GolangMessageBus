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
    "github.com/edancain/RocketLab/bus/types"
)

// MessageBus is the central coordinator of the system
// Acts as a central coordinator, implementing the MessageBus Interface
// Uses a mixture of Mutuxes, channels and Atomic Operations.
// using a combination of mutexes (for general synchronization), atomic
// operations (for counters), and we'll use channels in the BackPressureManager and OrderedDeliveryManager.

// Incorporated the Mediator pattern in the MessageBus, which coordinates all interactions between publishers and subscribers.
// Mediator Pattern in MessageBus:
// The Mediator pattern is a behavioral design pattern that reduces coupling between components of a program by making
// them communicate indirectly, through a special mediator object. In our case, the MessageBus acts as the mediator.

// How it works:
// - Publishers and subscribers don't communicate directly with each other.
// - Instead, they interact only with the MessageBus.
// - Publishers send messages to the MessageBus.
// - The MessageBus then distributes these messages to the appropriate subscribers.
// - This centralized communication reduces dependencies between components and makes it easier to add new publishers or subscribers without affecting existing ones.
//
// By combining these different synchronization mechanisms (mutexes, atomic operations, and soon-to-be-implemented channels), we're aiming to create a system that is
// both thread-safe and performant, allowing for high concurrency while maintaining data integrity.
type messageBus struct {
	publishers  map[string]map[types.Publisher]struct{}
	subscribers map[string]map[types.Subscription]struct{}
	messages    types.DataDictionary

	// backPressure *BackPressureManager:
	// Back pressure is a mechanism to handle scenarios where the rate of incoming data exceeds the rate at which it can be processed.
	// How it works:
	// The BackPressureManager monitors the rate of message production and consumption.
	// If publishers are producing messages faster than subscribers can consume them, it can slow down or temporarily stop publishers to prevent system overload.
	// This helps maintain system stability and prevents memory exhaustion from buffering too many messages.
	backPressure types.BackPressureManager

	// orderedDelivery *OrderedDeliveryManager:
	// This component ensures that messages are delivered to subscribers in the correct order, which is crucial for maintaining data integrity in many systems.
	// How it works:
	// It may use techniques like message sequencing or timestamps to track the order of messages.
	// It can buffer out-of-order messages and deliver them only when all preceding messages have been processed.
	//  This is particularly important in scenarios where the order of events matters, like in your example of rocket telemetry data.
	orderedDelivery types.OrderedDeliveryManager

	// mutex sync.RWMutex:
	// This is a read-write mutex from Go's sync package, used for synchronization.
	// How it works:
	// It allows multiple readers to acquire the lock simultaneously, improving performance for read-heavy workloads.
	// When a writer needs to modify data, it waits for all readers to finish and then acquires exclusive access.
	// We use this to protect shared data structures in the MessageBus from concurrent access, preventing race conditions.
	mutex sync.RWMutex

	// pubCount, subCount, msgCount atomic.Int64:
	// These are atomic integers used for counting publishers, subscribers, and messages.
	// How they work:
	// Atomic operations allow us to perform certain operations on these counters without needing to use mutexes.
	// They're especially useful for simple increment/decrement operations that need to be thread-safe.
	// Using atomic operations for these counters can be more efficient than using mutexes, especially when the operation is very quick.
	pubCount atomic.Int64
	subCount atomic.Int64
	msgCount atomic.Int64
}

// NewMessageBus creates and returns a new MessageBus instance
func NewMessageBus() MessageBus {
	return &messageBus{
		publishers:      make(map[string]map[*Publisher]struct{}),
		subscribers:     make(map[string]map[Subscription]struct{}),
		messages:        datadictionary.NewDataDictionary(),
		backPressure:    backpressure.NewBackPressureManager(),
		orderedDelivery: ordereddelivery.NewOrderedDeliveryManager(),
	}
}

// GetPublisher returns a new Publisher for the given topic
// Factory Method pattern is used in the GetPublisher method to create new Publisher instances.
// The Factory Method pattern is a creational design pattern that provides an interface for
// creating objects in a superclass, but allows subclasses to alter the type of objects that will be created.
// How it works in our context:
// The GetPublisher method acts as a factory method for creating Publisher instances.
// Instead of directly creating a Publisher with new Publisher(), we use this method to encapsulate the creation process.
// This allows us to perform additional setup (like registering the publisher with the MessageBus) and potentially return different types of publishers if needed in the future.
// It provides a single point of control for creating publishers, making it easier to modify the creation process if requirements change.
func (mb *messageBus) GetPublisher(topic string) Publisher {
	mb.mutex.Lock()
	defer mb.mutex.Unlock()

	if _, exists := mb.publishers[topic]; !exists {
		mb.publishers[topic] = make(map[*Publisher]struct{})
	}

	pub := publisher.NewPubisher(topic, mb)
	mb.publishers[topic][pub] = struct{}{}
	mb.pubCount.Add(1)

	return pub
}

// Subscribe registers a new subscription for the given topic
func (mb *messageBus) Subscribe(topic string, sub Subscription) (unsubscribe func()) {
	mb.mutex.Lock()
	defer mb.mutex.Unlock()

	if _, exists := mb.subscribers[topic]; !exists {
		mb.subscribers[topic] = make(map[Subscription]struct{})
	}

	mb.subscribers[topic][sub] = struct{}{}
	mb.subCount.Add(1)

	return func() {
		mb.mutex.Lock()
		defer mb.mutex.Unlock()
		delete(mb.subscribers[topic], sub)
		mb.subCount.Add(-1)
	}
}

// publish sends a message to all subscribers of the given topic
// demonstrates error handling, returning errors for back pressure issues and message storage failures.
func (mb *messageBus) publish(msg Message) error {
	mb.mutex.RLock()
	defer mb.mutex.RUnlock()

	if err := mb.backPressure.CheckPressure(msg.Topic); err != nil {
		logger.ErrorLogger.Printf("Back pressure error for topic %s: %v", msg.Topic, err)
		return err
	}

	// message persistence with the DataDictionary
	if err := mb.messages.Store(msg); err != nil {
		logger.ErrorLogger.Printf("Failed to store message for topic %s: %v", msg.Topic, err)
		return err
	}

	subscribers, exists := mb.subscribers[msg.Topic]
	if !exists {
		return nil // No subscribers for this topic, which is not an error
	}

	for sub := range subscribers {
		go func(s Subscription) {
			if err := mb.orderedDelivery.DeliverMessage(msg, s); err != nil {
				logger.ErrorLogger.Printf("Error delivering message for topic %s: %v", msg.Topic, err)
			}
		}(sub)
	}

	mb.msgCount.Add(1)
	return nil
}

// Stats returns the current stats of the MessageBus
func (mb *messageBus) Stats(now time.Time) Stats {
	mb.mutex.RLock()
	defer mb.mutex.RUnlock()

	return Stats{
		TotalMessages:     int(mb.msgCount.Load()),
		PublisherCount:    int(mb.pubCount.Load()),
		SubscriptionCount: int(mb.subCount.Load()),
		TopicFrequency:    mb.calculateTopicFrequency(now),
	}
}

// calculateTopicFrequency calculates the frequency of messages for each topic
func (mb *messageBus) calculateTopicFrequency(now time.Time) map[string]float64 {
	// Implementation to be added
	return nil
}

func (mb *messageBus) RemovePublisher(p Publisher) {
	mb.mutex.Lock()
	defer mb.mutex.Unlock()

	for _, publishers := range mb.publishers {
		if _, exists := publishers[p]; exists {
			delete(publishers, p)
			mb.pubCount.Add(-1)
			break
		}
	}
}

func (mb *messageBus) calculateTopicFrequency(now time.Time) map[string]float64 {
	frequencies := make(map[string]float64)
	sixtySecondsAgo := now.Add(-60 * time.Second)

	for topic := range mb.subscribers {
		messages := mb.messages.GetMessages(topic, sixtySecondsAgo, now)
		frequencies[topic] = float64(len(messages)) / 60.0 // messages per second
	}

	return frequencies
}
