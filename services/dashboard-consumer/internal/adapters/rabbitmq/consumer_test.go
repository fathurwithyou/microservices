package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/rabbitmq/amqp091-go"
)

type closedDeliveryChannel struct{}

func (closedDeliveryChannel) Consume(string, string, bool, bool, bool, bool, amqp091.Table) (<-chan amqp091.Delivery, error) {
	deliveries := make(chan amqp091.Delivery)
	close(deliveries)
	return deliveries, nil
}

func (closedDeliveryChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp091.Table) error {
	return nil
}

func (closedDeliveryChannel) QueueDeclare(string, bool, bool, bool, bool, amqp091.Table) (amqp091.Queue, error) {
	return amqp091.Queue{}, nil
}

func (closedDeliveryChannel) QueueBind(string, string, string, bool, amqp091.Table) error {
	return nil
}

func (closedDeliveryChannel) Close() error { return nil }

func TestConsumerRunReturnsWhenDeliveryChannelCloses(t *testing.T) {
	consumer := &Consumer{channel: closedDeliveryChannel{}}
	processed := false

	err := consumer.Run(context.Background(), func([]byte, string, bool) error {
		processed = true
		return nil
	})

	if !errors.Is(err, ErrDeliveryChannelClosed) {
		t.Fatalf("expected delivery channel error, got %v", err)
	}
	if processed {
		t.Fatal("closed delivery channel must not process a delivery")
	}
}
