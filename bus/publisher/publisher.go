package publisher

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/edancain/GolangMessageBus/bus/logger"
	"github.com/edancain/GolangMessageBus/types"
)

// Publisher represents a message publisher for a specific topic
type Publisher struct {
	topic  string           // The topic this publisher is associated with.
	bus    types.MessageBus // A reference to the MessageBus that this publisher will use to send messages.
	active atomic.Bool      // An atomic boolean to safely manage the active state of the publisher across goroutines.
}

func NewPublisher(topic string, bus types.MessageBus) types.Publisher {
	p := &Publisher{
		topic: topic,
		bus:   bus,
	}
	p.active.Store(true)
	return p
}

// Publish sends a new message to the MessageBus
func (p *Publisher) Publish(timestamp time.Time, message string) error {
	// Checks if the publisher is active before attempting to publish.
	if !p.active.Load() {
		err := errors.New("publisher is closed")
		logger.ErrorLogger.Printf("Attempt to publish on closed publisher for topic %s: %v", p.topic, err)
		return err

	}

	// Creates a new Message with the provided timestamp, topic and content.
	msg := types.Message{
		Timestamp: timestamp,
		Topic:     p.topic,
		Content:   message,
	}

	// Attempts to publish the message via the MessageBus.
	// Most likely reasons for a publish error would be:
	// 		- Back pressure being applied due to too many messages on a topic.
	// 		- The data dictionary reaching its maximum capacity for a topic.
	// 		- Any errors occurring in the OrderedDeliveryManager when trying to deliver the message.
	if err := p.bus.PublishMessage(msg); err != nil {
		logger.ErrorLogger.Printf("Failed to publish message on topic %s: %v", p.topic, err)
		return err
	}

	if logger.IsInfoEnabled() {
		logger.InfoLogger.Printf("Message published on topic: %s, timestamp: %v", p.topic, timestamp)
	}

	if logger.IsDebugEnabled() {
		logger.DebugLogger.Printf("Message published: %+v", msg)
	}

	return nil
}

// Close marks the publisher as inactive, removes it.
func (p *Publisher) Close() {
	p.active.Store(false)
	p.bus.RemovePublisher(p)
	if logger.IsInfoEnabled() {
		logger.InfoLogger.Printf("Publisher closed for topic: %s", p.topic)
	}
}
