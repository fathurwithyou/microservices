package rabbitmq

import (
	"context"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName       = "hazard.events"
	deadLetterExchange = "hazard.events.dlx"
)

type Process func([]byte, string, bool) error

type Consumer struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	queue      string
	service    string
	logger     *slog.Logger
}

func New(url, queue, service string, logger *slog.Logger) (*Consumer, error) {
	connection, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	consumer := &Consumer{connection: connection, channel: channel, queue: queue, service: service, logger: logger}
	if err := consumer.declare(); err != nil {
		_ = consumer.Close()
		return nil, err
	}
	return consumer, nil
}

func (c *Consumer) Run(ctx context.Context, process Process) {
	deliveries, err := c.channel.Consume(c.queue, c.service, false, false, false, false, nil)
	if err != nil {
		c.logger.Error("consumer registration failed", "service", c.service, "error", err)
		return
	}
	for {
		select {
		case delivery := <-deliveries:
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
			return
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
