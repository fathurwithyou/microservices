package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/rabbitmq/amqp091-go"
)

const exchange = "hazard.events"

type Publisher struct {
	mu            sync.Mutex
	url           string
	logger        *slog.Logger
	connection    *amqp091.Connection
	channel       *amqp091.Channel
	confirmations <-chan amqp091.Confirmation
	closed        <-chan *amqp091.Error
}

func New(url string, logger *slog.Logger) *Publisher {
	return &Publisher{url: url, logger: logger}
}

func (p *Publisher) Publish(ctx context.Context, message domain.OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureChannelLocked(); err != nil {
		return err
	}
	confirmCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.channel.PublishWithContext(confirmCtx, exchange, message.RoutingKey, false, false, amqp091.Publishing{ContentType: "application/protobuf", DeliveryMode: amqp091.Persistent, CorrelationId: message.CorrelationID, MessageId: message.HazardID, Body: message.Payload}); err != nil {
		p.resetLocked()
		return err
	}
	select {
	case confirmation, ok := <-p.confirmations:
		if !ok {
			p.resetLocked()
			return errors.New("rabbitmq publisher confirmation channel closed")
		}
		if !confirmation.Ack {
			return errors.New("rabbitmq rejected published event")
		}
		p.logger.Info("event published", "component", "publisher", "event_id", message.HazardID, "correlation_id", message.CorrelationID)
		return nil
	case <-confirmCtx.Done():
		p.resetLocked()
		return confirmCtx.Err()
	case _, ok := <-p.closed:
		p.resetLocked()
		if !ok {
			return errors.New("rabbitmq publisher channel closed")
		}
		return errors.New("rabbitmq publisher channel closed unexpectedly")
	}
}

func (p *Publisher) ensureChannelLocked() error {
	if p.connection != nil && !p.connection.IsClosed() {
		return nil
	}
	p.resetLocked()
	connection, err := amqp091.Dial(p.url)
	if err != nil {
		return err
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return err
	}
	if err := channel.ExchangeDeclare(exchange, amqp091.ExchangeFanout, true, false, false, false, nil); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return err
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return err
	}
	p.connection = connection
	p.channel = channel
	p.confirmations = channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
	p.closed = channel.NotifyClose(make(chan *amqp091.Error, 1))
	return nil
}

func (p *Publisher) resetLocked() {
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.connection != nil {
		_ = p.connection.Close()
	}
	p.channel = nil
	p.connection = nil
	p.confirmations = nil
	p.closed = nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resetLocked()
	return nil
}
