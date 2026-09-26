package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName       = "hazard.events"
)

type Process func([]byte, string, bool) error

var ErrDeliveryChannelClosed = errors.New("rabbitmq delivery channel closed")

type deliveryChannel interface {
	Consume(string, string, bool, bool, bool, bool, amqp091.Table) (<-chan amqp091.Delivery, error)
	ExchangeDeclare(string, string, bool, bool, bool, bool, amqp091.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp091.Table) (amqp091.Queue, error)
	QueueBind(string, string, string, bool, amqp091.Table) error
	Close() error
}

type Consumer struct {
	connection *amqp091.Connection
	channel    deliveryChannel
	queue      string
	service    string
	logger     *slog.Logger
}

type ConsumerConfig struct {
	URL     string
	Queue   string
	Service string
	Logger  *slog.Logger
}

func New(cfg ConsumerConfig) (*Consumer, error) {
	connection, err := amqp091.Dial(cfg.URL)
	if err != nil {
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	consumer := &Consumer{connection: connection, channel: channel, queue: cfg.Queue, service: cfg.Service, logger: cfg.Logger}
	if err := consumer.declare(); err != nil {
		_ = consumer.Close()
		return nil, err
	}
	return consumer, nil
}

func (c *Consumer) Run(ctx context.Context, process Process) error {
	deliveries, err := c.channel.Consume(c.queue, c.service, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("register consumer: %w", err)
	}
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return ErrDeliveryChannelClosed
			}
			if delivery.Body == nil {
				continue
			}
			if err := process(delivery.Body, delivery.CorrelationId, delivery.Redelivered); err != nil {
				c.logger.Error("event decode failed", "service", c.service, "error", err)
				_ = delivery.Nack(false, false)
				continue
			}
			_ = delivery.Ack(false)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Consumer) Close() error {
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.connection != nil {
		return c.connection.Close()
	}
	return nil
}

func (c *Consumer) declare() error {
	if err := c.channel.ExchangeDeclare(exchangeName, amqp091.ExchangeFanout, true, false, false, false, nil); err != nil {
		return err
	}
	deadLetterExchange := c.queue + ".dlx"
	if err := c.channel.ExchangeDeclare(deadLetterExchange, amqp091.ExchangeFanout, true, false, false, false, nil); err != nil {
		return err
	}
	declaration, err := c.channel.QueueDeclare(c.queue, true, false, false, false, amqp091.Table{"x-dead-letter-exchange": deadLetterExchange})
	if err != nil {
		return err
	}
	if err := c.channel.QueueBind(declaration.Name, "", exchangeName, false, nil); err != nil {
		return err
	}
	deadLetterQueue, err := c.channel.QueueDeclare(c.queue+".dead-letter", true, false, false, false, nil)
	if err != nil {
		return err
	}
	return c.channel.QueueBind(deadLetterQueue.Name, "", deadLetterExchange, false, nil)
}
