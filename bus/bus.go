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
	subscribers map[string][]types.Subscription
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
	// A mutex is a synchronization primitive used to protect shared resources from concurrent access. It ensures that only one goroutine can access the protected resource at a time.

	// sync.RWMutex: This is a reader/writer mutex. It allows multiple readers to access the resource simultaneously, but only one writer
	// can access it exclusively.
	mutex sync.RWMutex

	// pubCount, subCount, msgCount atomic.Int64:
	// These are atomic integers used for counting publishers, subscribers, and messages.
	// How they work:
	// Atomic operations allow us to perform certain operations on these counters without needing to use mutexes.
	// They're especially useful for simple increment/decrement operations that need to be thread-safe.
	// Using atomic operations for these counters can be more efficient than using mutexes, especially when the operation is very quick.

	// Atomic operations are indivisible operations that complete in a single step relative to other threads. They are used for
	// simple operations that need to be thread-safe without the overhead of a mutex.
	pubCount atomic.Int64
	subCount atomic.Int64
	msgCount atomic.Int64
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
// Factory Method pattern is used in the GetPublisher method to create new Publisher instances.
// The Factory Method pattern is a creational design pattern that provides an interface for
// creating objects in a superclass, but allows subclasses to alter the type of objects that will be created.
// How it works in our context:
// The GetPublisher method acts as a factory method for creating Publisher instances.
// Instead of directly creating a Publisher with new Publisher(), we use this method to encapsulate the creation process.
// This allows us to perform additional setup (like registering the publisher with the MessageBus) and potentially return different types of publishers if needed in the future.
// It provides a single point of control for creating publishers, making it easier to modify the creation process if requirements change.

// Locks the mutex to safely access and modify the publishers map.
// Creates a new publisher if it doesn't exist for the given topic.
// Increments the pubCount atomically.
func (mb *messageBus) GetPublisher(topic string) types.Publisher {
	mb.mutex.Lock()
	defer mb.mutex.Unlock()

	if _, exists := mb.publishers[topic]; !exists {
		mb.publishers[topic] = make(map[types.Publisher]struct{})
	}

	pub := publisher.NewPublisher(topic, mb)
	mb.publishers[topic][pub] = struct{}{}
	mb.pubCount.Add(1)

	return pub
}

// Subscribe registers a new subscription for the given topic
// Locks the mutex to safely modify the subscribers map.
// Appends the new subscription to the list of subscribers for the given topic.
// Increments the subCount atomically.
// Returns an unsubscribe function that, when called, will remove the subscription and decrement the subCount.
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
// demonstrates error handling, returning errors for back pressure issues and message storage failures.

// Uses a read lock, allowing multiple goroutines to publish concurrently.
// Checks for back pressure and stores the message.
// If subscribers exist for the topic, it starts a new goroutine for each subscriber to deliver the message.
// Increments the msgCount atomically.
func (mb *messageBus) PublishMessage(msg types.Message) error {
	// This acquires a read lock on the mutex.
	// Multiple goroutines can hold a read lock simultaneously, allowing for concurrent read operations.
	// If another goroutine is holding a write lock, this will block until the write lock is released.
	mb.mutex.RLock()

	//This defers the unlocking of the read lock until the current function returns.
	// defer ensures that the unlock operation is always executed, even if the function panics or returns early.
	// This helps prevent deadlocks by ensuring the lock is always released.
	defer mb.mutex.RUnlock()

	// The above The purpose of this pattern is to provide safe concurrent access to shared resources for read operations. It allows multiple readers to access the protected data simultaneously, improving performance in read-heavy scenarios.
	// Key points:

	// It's used for operations that only read shared data and don't modify it.
	// Multiple read locks can be held simultaneously by different goroutines.
	// A read lock blocks any attempt to acquire a write lock, but not other read locks.
	// This pattern ensures that the lock is always released, preventing potential deadlocks.

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

	mb.msgCount.Add(1) // Always increment the message count
	return nil
}

// Stats returns the current stats of the MessageBus
// This method returns the current statistics of the MessageBus. It uses a read lock to safely access the data and atomic loads to get the current counter values.
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
