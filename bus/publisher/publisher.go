package publisher

import (
	"sync/atomic"
	"time"
    "errors"

	"github.com/edancain/RocketLab/bus"
)

// Publisher represents a message publisher for a specific topic
type Publisher struct {
	topic  string
	bus    *bus.MessageBus
	active atomic.Bool
}

func NewPublisher(topic string, bus bus.MessageBus) *Publisher {
	p := &Publisher{
		topic: topic,
		bus:   bus,
	}
	p.active.Store(true)
	return p
}

// Publish sends a new message to the MessageBus
func (p *Publisher) Publish(timestamp time.Time, message string) error {
	if !p.active.Load() {
		return errors.New("publisher is closed")
	}

	msg := bus.Message{
		Timestamp: timestamp,
		Topic:     p.topic,
		Content:   message,
	}

	return p.bus.publish(msg)
}

// Close marks the publisher as inactive
func (p *Publisher) Close() {
	p.active.Store(false)
	p.bus.removePublisher(p)
}
