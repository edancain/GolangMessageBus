package types

import "time"

// This defines the structure of a message in your system. It includes a timestamp, topic, and content.
type Message struct {
	Timestamp time.Time
	Topic     string
	Content   string
}

// This is a function type that defines how subscribers will receive messages.
type Subscription func(timestamp time.Time, message string)

// This structure holds statistical information about the message bus system.
type Stats struct {
	TotalMessages     int
	PublisherCount    int
	SubscriptionCount int
	TopicFrequency    map[string]float64
}

// Interface Declarations:
// This file defines interfaces for various components of the system. These are crucial for several reasons:
// a. Abstraction: Interfaces define behavior without specifying implementation. This allows for flexibility in how these behaviors are implemented.
// b. Decoupling: By programming to interfaces rather than concrete implementations, different parts of your system are less tightly coupled. This makes the system more modular and easier to maintain and extend.
// c. Testability: Interfaces make it easy to create mock implementations for testing.
// d. Polymorphism: Different implementations of an interface can be used interchangeably, allowing for runtime flexibility.

// Polymorphism:
// Functions can accept these interfaces as parameters, allowing them to work with any implementation of the interface.
// func ProcessMessages(bus MessageBus) {
//pub := bus.GetPublisher("some-topic")
// Use the publisher...
// }

type BackPressureManager interface {
	CheckPressure(topic string) error
}

type DataDictionary interface {
	Store(msg Message) error
	GetMessages(topic string, start, end time.Time) []Message
}

// This is the core interface of your system. It defines how publishers are created, how subscriptions are managed, how messages are published, and how statistics are retrieved.
type MessageBus interface {
	GetPublisher(topic string) Publisher
	Subscribe(topic string, sub Subscription) (unsubscribe func())
	Stats(now time.Time) Stats
	PublishMessage(msg Message) error
	RemovePublisher(p Publisher)
}

// Defines how messages are delivered in order to subscribers.
type OrderedDeliveryManager interface {
	DeliverMessage(msg Message, sub Subscription) error
}

type Publisher interface {
	Publish(timestamp time.Time, message string) error
	Close()
}
