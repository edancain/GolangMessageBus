package types

import "time"

type Message struct {
	Timestamp time.Time
	Topic     string
	Content   string
}

type Subscription func(timestamp time.Time, message string)

type Stats struct {
	TotalMessages     int
	PublisherCount    int
	SubscriptionCount int
	TopicFrequency    map[string]float64
}

type MessageBus interface {
    GetPublisher(topic string) Publisher
    Subscribe(topic string, sub Subscription) (unsubscribe func())
    Stats(now time.Time) Stats
    PublishMessage(msg Message) error
    RemovePublisher(p Publisher)
}

type OrderedDeliveryManager interface {
    DeliverMessage(msg Message, sub Subscription) error
}

type BackPressureManager interface {
    CheckPressure(topic string) error
}

type Publisher interface {
	Publish(timestamp time.Time, message string) error
	Close()
}
