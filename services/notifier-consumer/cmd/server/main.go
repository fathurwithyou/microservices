package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fathurwithyou/microservices/services/notifier-consumer/internal/adapters/rabbitmq"
	"github.com/fathurwithyou/microservices/services/notifier-consumer/internal/application"
	"github.com/fathurwithyou/microservices/services/notifier-consumer/internal/config"
	httptransport "github.com/fathurwithyou/microservices/services/notifier-consumer/internal/transport/http"
)

const (
	queueName   = "notifier.hazard-events"
	serviceName = "notifier-consumer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	consumer, err := rabbitmq.New(cfg.RabbitURL, queueName, serviceName, logger)
	if err != nil {
		logger.Error("broker connection failed", "service", serviceName, "error", err)
		return
	}
	defer consumer.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	healthServer := httptransport.NewHealthServer(cfg.HealthPort)
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server stopped", "service", serviceName, "error", err)
			stop()
		}
	}()
	consumer.Run(ctx, application.NewProcessor(serviceName, logger).Process)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = healthServer.Shutdown(shutdownCtx)
}
