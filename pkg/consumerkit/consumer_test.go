package consumerkit

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type fakeChannel struct {
	deliveries  chan amqp091.Delivery
	qosPrefetch int
	declared    []string
	consumeArgs consumeArgs
	closed      bool
}

type consumeArgs struct {
	queue   string
	tag     string
	autoAck bool
}

func newFakeChannel(deliveries ...amqp091.Delivery) *fakeChannel {
	channel := &fakeChannel{
		deliveries: make(chan amqp091.Delivery, len(deliveries)),
	}
	for _, delivery := range deliveries {
		channel.deliveries <- delivery
	}
	close(channel.deliveries)
	return channel
}

func (f *fakeChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp091.Table) (<-chan amqp091.Delivery, error) {
	f.consumeArgs = consumeArgs{queue: queue, tag: consumer, autoAck: autoAck}
	return f.deliveries, nil
}

func (f *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp091.Table) error {
	f.declared = append(f.declared, "exchange:"+name)
	return nil
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error) {
	f.declared = append(f.declared, "queue:"+name)
	return amqp091.Queue{Name: name}, nil
}

func (f *fakeChannel) QueueBind(name, key, exchange string, noWait bool, args amqp091.Table) error {
	f.declared = append(f.declared, "bind:"+name+"->"+exchange)
	return nil
}

func (f *fakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	f.qosPrefetch = prefetchCount
	return nil
}

func (f *fakeChannel) Close() error {
	f.closed = true
	return nil
}

func deliveryWith(body string) amqp091.Delivery {
	return amqp091.Delivery{Body: []byte(body), CorrelationId: "corr-1", Acknowledger: &fakeAcknowledger{}}
}

func testConsumer(t *testing.T, channel deliveryChannel) *Consumer {
	t.Helper()
	consumer, err := New(Config{URL: "amqp://fake", Queue: "test.hazard-events", Service: "test-consumer", Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	consumer.dial = func(context.Context) (deliveryChannel, func() error, error) {
		return channel, func() error { return nil }, nil
	}
	return consumer
}

func TestNewRejectsMissingConfig(t *testing.T) {
	_, err := New(Config{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New error = %v, want ErrInvalidConfig", err)
	}
}

func TestConsumeReturnsWhenDeliveryChannelCloses(t *testing.T) {
	channel := newFakeChannel()
	consumer := testConsumer(t, channel)

	processed := false
	started, err := consumer.consume(context.Background(), func(context.Context, Delivery) Outcome {
		processed = true
		return Ack
	})

	if !started {
		t.Fatal("session should count as started after Consume registration")
	}
	if !errors.Is(err, errDeliveryChannelClosed) {
		t.Fatalf("consume error = %v, want errDeliveryChannelClosed", err)
	}
	if processed {
		t.Fatal("closed delivery channel must not process a delivery")
	}
	if channel.qosPrefetch != DefaultPrefetch {
		t.Fatalf("prefetch = %d, want %d", channel.qosPrefetch, DefaultPrefetch)
	}
	if channel.consumeArgs.autoAck {
		t.Fatal("consume must use manual acknowledgement")
	}
}

func TestConsumeDeclaresTopology(t *testing.T) {
	channel := newFakeChannel()
	consumer := testConsumer(t, channel)

	if _, err := consumer.consume(context.Background(), func(context.Context, Delivery) Outcome { return Ack }); err == nil {
		t.Fatal("expected delivery channel closed error")
	}

	want := []string{
		"exchange:hazard.events",
		"exchange:hazard.events.dlx",
		"queue:test.hazard-events",
		"bind:test.hazard-events->hazard.events",
		"queue:test.hazard-events.dead-letter",
		"bind:test.hazard-events.dead-letter->hazard.events.dlx",
	}
	if strings.Join(channel.declared, ",") != strings.Join(want, ",") {
		t.Fatalf("declared topology = %v, want %v", channel.declared, want)
	}
}

type fakeAcknowledger struct {
	acks  []uint64
	nacks []nackRecord
}

type nackRecord struct {
	tag     uint64
	requeue bool
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	f.acks = append(f.acks, tag)
	return nil
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	f.nacks = append(f.nacks, nackRecord{tag: tag, requeue: requeue})
	return nil
}

func (f *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	return nil
}

func TestHandleMapsOutcomesToAcknowledgements(t *testing.T) {
	tests := []struct {
		name        string
		outcome     Outcome
		wantAck     bool
		wantNack    bool
		wantRequeue bool
	}{
		{name: "ack", outcome: Ack, wantAck: true},
		{name: "requeue", outcome: Requeue, wantNack: true, wantRequeue: true},
		{name: "deadletter", outcome: DeadLetter, wantNack: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acknowledger := &fakeAcknowledger{}
			consumer := testConsumer(t, newFakeChannel())
			delivery := amqp091.Delivery{DeliveryTag: 7, Body: []byte("body"), CorrelationId: "corr-1", Acknowledger: acknowledger}

			consumer.handle(context.Background(), func(context.Context, Delivery) Outcome { return tt.outcome }, delivery)

			if gotAck := len(acknowledger.acks) == 1; gotAck != tt.wantAck {
				t.Fatalf("acked = %v, want %v", gotAck, tt.wantAck)
			}
			if gotNack := len(acknowledger.nacks) == 1; gotNack != tt.wantNack {
				t.Fatalf("nacked = %v, want %v", gotNack, tt.wantNack)
			}
			if tt.wantNack && acknowledger.nacks[0].requeue != tt.wantRequeue {
				t.Fatalf("nack requeue = %v, want %v", acknowledger.nacks[0].requeue, tt.wantRequeue)
			}
		})
	}
}

func TestHandleAcksEmptyDelivery(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	consumer := testConsumer(t, newFakeChannel())
	processed := false
	delivery := amqp091.Delivery{DeliveryTag: 3, Acknowledger: acknowledger}

	consumer.handle(context.Background(), func(context.Context, Delivery) Outcome {
		processed = true
		return Ack
	}, delivery)

	if processed {
		t.Fatal("empty delivery must not reach the processor")
	}
	if len(acknowledger.acks) != 1 {
		t.Fatalf("empty delivery must be acked once, got %v", acknowledger.acks)
	}
}

func TestHandleRecoversPanicAsDeadLetter(t *testing.T) {
	consumer := testConsumer(t, newFakeChannel())
	outcome := consumer.process(context.Background(), func(context.Context, Delivery) Outcome {
		panic("boom")
	}, amqp091.Delivery{Body: []byte("body"), CorrelationId: "corr-panic"})
	if outcome != DeadLetter {
		t.Fatalf("panic outcome = %v, want DeadLetter", outcome)
	}
}

func TestConsumeProcessesDeliveryBody(t *testing.T) {
	channel := newFakeChannel(deliveryWith("payload"))
	consumer := testConsumer(t, channel)

	var received Delivery
	_, err := consumer.consume(context.Background(), func(_ context.Context, delivery Delivery) Outcome {
		received = delivery
		return Ack
	})
	if !errors.Is(err, errDeliveryChannelClosed) {
		t.Fatalf("consume error = %v, want errDeliveryChannelClosed", err)
	}
	if string(received.Body) != "payload" || received.CorrelationID != "corr-1" {
		t.Fatalf("received delivery = %+v, want body payload and corr-1", received)
	}
}

func TestConsumeStopsOnContextCancellation(t *testing.T) {
	deliveries := make(chan amqp091.Delivery)
	channel := &fakeChannel{deliveries: deliveries}
	consumer := testConsumer(t, channel)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started, err := consumer.consume(ctx, func(context.Context, Delivery) Outcome { return Ack })
	if !started {
		t.Fatal("session should count as started after Consume registration")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("consume error = %v, want context.Canceled", err)
	}
}

func TestRunReturnsNilOnContextCancellation(t *testing.T) {
	consumer := testConsumer(t, newFakeChannel())
	consumer.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, func(context.Context, Delivery) Outcome { return Ack })
	}()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestJitterWithinBounds(t *testing.T) {
	base := 2 * time.Second
	for range 100 {
		value := jitter(base)
		if value < base/2 || value > base {
			t.Fatalf("jitter(%v) = %v, want within [%v, %v]", base, value, base/2, base)
		}
	}
}
