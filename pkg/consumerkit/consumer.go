// Package consumerkit provides RabbitMQ consumer plumbing shared by
// services: connection lifecycle, queue topology, prefetch, acknowledgement
// policy, and reconnection with exponential backoff and jitter.
//
// Business processing is supplied by each service through a ProcessFunc;
// this package owns no domain logic. A ProcessFunc reports what should
// happen to a delivery through an explicit Outcome instead of a bare error,
// so transient failures can be requeued while poison messages are
// dead-lettered.
package consumerkit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// Topology and policy defaults.
const (
	// DefaultExchange is the fanout exchange carrying hazard events.
	DefaultExchange = "hazard.events"
	// DefaultDLXExchange is the fanout exchange receiving dead-lettered messages.
	DefaultDLXExchange = "hazard.events.dlx"
	// DefaultPrefetch is the QoS prefetch count applied to the channel.
	DefaultPrefetch = 16

	defaultReconnectMin = time.Second
	defaultReconnectMax = 30 * time.Second
)

// Outcome tells the consumer how to acknowledge a delivery.
type Outcome int

const (
	// Ack acknowledges successful processing.
	Ack Outcome = iota
	// Requeue nacks the delivery for later redelivery; use for transient
	// failures.
	Requeue
	// DeadLetter nacks the delivery without requeue so it is routed to the
	// dead-letter exchange; use for poison messages.
	DeadLetter
)

// Delivery is the transport-level view of one message handed to a
// ProcessFunc.
type Delivery struct {
	Body          []byte
	CorrelationID string
	Redelivered   bool
}

// ProcessFunc handles one delivery and reports the acknowledgement outcome.
// Panics are recovered by the consumer and treated as DeadLetter.
type ProcessFunc func(ctx context.Context, delivery Delivery) Outcome

// Config configures a Consumer.
type Config struct {
	// URL is the AMQP connection string. Required.
	URL string
	// Queue is the service-owned queue bound to the event exchange. Required.
	Queue string
	// Service is the service name used for logging and the AMQP consumer tag. Required.
	Service string
	// Exchange defaults to DefaultExchange.
	Exchange string
	// DLXExchange defaults to DefaultDLXExchange.
	DLXExchange string
	// Prefetch defaults to DefaultPrefetch.
	Prefetch int
	// ReconnectMin is the initial reconnect backoff. Defaults to 1s.
	ReconnectMin time.Duration
	// ReconnectMax caps the reconnect backoff. Defaults to 30s.
	ReconnectMax time.Duration
	// Logger receives structured logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// ErrInvalidConfig is returned by New for missing required configuration.
var ErrInvalidConfig = errors.New("consumerkit: invalid config")

// errDeliveryChannelClosed signals that the broker closed the delivery
// channel; Run uses it to trigger a reconnect.
var errDeliveryChannelClosed = errors.New("consumerkit: delivery channel closed")

// deliveryChannel is the subset of *amqp091.Channel used by the consumer.
// It exists as a test seam.
type deliveryChannel interface {
	Consume(string, string, bool, bool, bool, bool, amqp091.Table) (<-chan amqp091.Delivery, error)
	ExchangeDeclare(string, string, bool, bool, bool, bool, amqp091.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp091.Table) (amqp091.Queue, error)
	QueueBind(string, string, string, bool, amqp091.Table) error
	Qos(int, int, bool) error
	Close() error
}

// Consumer owns the reconnecting consume loop for one queue.
type Consumer struct {
	cfg Config
	// dial opens a channel plus a cleanup function; overridable in tests.
	dial func(context.Context) (deliveryChannel, func() error, error)
	// sleep waits for d or ctx cancellation; overridable in tests.
	sleep func(context.Context, time.Duration) error
}

// New validates cfg, applies defaults, and returns a Consumer. It does not
// connect; Run owns the connection lifecycle.
func New(cfg Config) (*Consumer, error) {
	if cfg.URL == "" || cfg.Queue == "" || cfg.Service == "" {
		return nil, fmt.Errorf("%w: URL, Queue, and Service are required", ErrInvalidConfig)
	}
	if cfg.Exchange == "" {
		cfg.Exchange = DefaultExchange
	}
	if cfg.DLXExchange == "" {
		cfg.DLXExchange = DefaultDLXExchange
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = DefaultPrefetch
	}
	if cfg.ReconnectMin <= 0 {
		cfg.ReconnectMin = defaultReconnectMin
	}
	if cfg.ReconnectMax <= 0 {
		cfg.ReconnectMax = defaultReconnectMax
	}
	if cfg.ReconnectMax < cfg.ReconnectMin {
		cfg.ReconnectMax = cfg.ReconnectMin
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	consumer := &Consumer{cfg: cfg}
	consumer.dial = consumer.dialAMQP
	consumer.sleep = sleepContext
	return consumer, nil
}

// Run consumes messages until ctx is cancelled, reconnecting with
// exponential backoff and jitter after failures. It returns nil when ctx is
// cancelled and an error only for configuration problems.
func (c *Consumer) Run(ctx context.Context, process ProcessFunc) error {
	if process == nil {
		return fmt.Errorf("%w: process function is required", ErrInvalidConfig)
	}
	backoff := c.cfg.ReconnectMin
	for {
		sessionStarted, err := c.consume(ctx, process)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.cfg.Logger.Error("consumer session stopped", "service", c.cfg.Service, "queue", c.cfg.Queue, "error", err)
		}
		if sessionStarted {
			backoff = c.cfg.ReconnectMin
		}
		delay := jitter(backoff)
		c.cfg.Logger.Warn("reconnecting consumer", "service", c.cfg.Service, "queue", c.cfg.Queue, "retry_in_ms", delay.Milliseconds())
		if err := c.sleep(ctx, delay); err != nil {
			return nil
		}
		backoff = min(backoff*2, c.cfg.ReconnectMax)
	}
}

// consume runs one broker session. The bool result reports whether the
// session reached the point of consuming deliveries, which Run uses to
// reset the reconnect backoff.
func (c *Consumer) consume(ctx context.Context, process ProcessFunc) (bool, error) {
	channel, closeSession, err := c.dial(ctx)
	if err != nil {
		return false, fmt.Errorf("connect broker: %w", err)
	}
	defer func() {
		if closeErr := closeSession(); closeErr != nil {
			c.cfg.Logger.Warn("session cleanup failed", "service", c.cfg.Service, "error", closeErr)
		}
	}()
	if err := c.declare(channel); err != nil {
		return false, fmt.Errorf("declare topology: %w", err)
	}
	deliveries, err := channel.Consume(c.cfg.Queue, c.cfg.Service, false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("register consumer: %w", err)
	}
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return true, errDeliveryChannelClosed
			}
			c.handle(ctx, process, delivery)
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
}

// handle processes one delivery and applies the acknowledgement outcome.
func (c *Consumer) handle(ctx context.Context, process ProcessFunc, delivery amqp091.Delivery) {
	if len(delivery.Body) == 0 {
		c.cfg.Logger.Warn("discarding empty delivery", "service", c.cfg.Service, "queue", c.cfg.Queue, "correlation_id", delivery.CorrelationId)
		if err := delivery.Ack(false); err != nil {
			c.cfg.Logger.Warn("ack failed for empty delivery", "service", c.cfg.Service, "error", err)
		}
		return
	}
	outcome := c.process(ctx, process, delivery)
	var err error
	switch outcome {
	case Requeue:
		err = delivery.Nack(false, true)
	case DeadLetter:
		err = delivery.Nack(false, false)
	default:
		err = delivery.Ack(false)
	}
	if err != nil {
		c.cfg.Logger.Warn("acknowledgement failed", "service", c.cfg.Service, "outcome", outcome, "correlation_id", delivery.CorrelationId, "error", err)
	}
}

// process invokes the ProcessFunc, converting panics into DeadLetter.
func (c *Consumer) process(ctx context.Context, process ProcessFunc, delivery amqp091.Delivery) (outcome Outcome) {
	defer func() {
		if recovered := recover(); recovered != nil {
			c.cfg.Logger.Error("processor panicked; dead-lettering delivery", "service", c.cfg.Service, "correlation_id", delivery.CorrelationId, "panic", recovered)
			outcome = DeadLetter
		}
	}()
	return process(ctx, Delivery{Body: delivery.Body, CorrelationID: delivery.CorrelationId, Redelivered: delivery.Redelivered})
}

// declare applies channel QoS and idempotently declares the exchange and
// queue topology, including the dead-letter path.
func (c *Consumer) declare(channel deliveryChannel) error {
	if err := channel.Qos(c.cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("set prefetch %d: %w", c.cfg.Prefetch, err)
	}
	if err := channel.ExchangeDeclare(c.cfg.Exchange, amqp091.ExchangeFanout, true, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.ExchangeDeclare(c.cfg.DLXExchange, amqp091.ExchangeFanout, true, false, false, false, nil); err != nil {
		return err
	}
	declaration, err := channel.QueueDeclare(c.cfg.Queue, true, false, false, false, amqp091.Table{"x-dead-letter-exchange": c.cfg.DLXExchange})
	if err != nil {
		return err
	}
	if err := channel.QueueBind(declaration.Name, "", c.cfg.Exchange, false, nil); err != nil {
		return err
	}
	deadLetterQueue, err := channel.QueueDeclare(c.cfg.Queue+".dead-letter", true, false, false, false, nil)
	if err != nil {
		return err
	}
	return channel.QueueBind(deadLetterQueue.Name, "", c.cfg.DLXExchange, false, nil)
}

// dialAMQP opens a real AMQP connection and channel.
func (c *Consumer) dialAMQP(_ context.Context) (deliveryChannel, func() error, error) {
	connection, err := amqp091.Dial(c.cfg.URL)
	if err != nil {
		return nil, nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	return channel, func() error {
		channelErr := channel.Close()
		connectionErr := connection.Close()
		return errors.Join(channelErr, connectionErr)
	}, nil
}

// jitter returns a random duration in [d/2, d] to desynchronize reconnects
// across service instances.
func jitter(d time.Duration) time.Duration {
	half := int64(d) / 2
	return time.Duration(half + rand.Int64N(half+1))
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
