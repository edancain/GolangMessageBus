package publisher

import (
	"sync/atomic"
	"time"
    "errors"

	"github.com/edancain/RocketLab/bus/logger"
	"github.com/edancain/RocketLab/types"
)

// Publisher represents a message publisher for a specific topic
type Publisher struct {
	topic  string
	bus    types.MessageBus
	active atomic.Bool
}

func NewPublisher(topic string, bus types.MessageBus) *Publisher {
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
		err := errors.New("publisher is closed")
        logger.ErrorLogger.Printf("Attempt to publish on closed publisher for topic %s: %v", p.topic, err)
        return err

	}

	msg := types.Message{
		Timestamp: timestamp,
		Topic:     p.topic,
		Content:   message,
	}

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

// Close marks the publisher as inactive
func (p *Publisher) Close() {
	p.active.Store(false)
	p.bus.removePublisher(p)
	if logger.IsInfoEnabled() {
		logger.InfoLogger.Printf("Publisher closed for topic: %s", p.topic)
	}
}
