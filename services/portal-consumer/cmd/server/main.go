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

	"github.com/fathurwithyou/microservices/services/portal-consumer/internal/adapters/rabbitmq"
	"github.com/fathurwithyou/microservices/services/portal-consumer/internal/application"
	"github.com/fathurwithyou/microservices/services/portal-consumer/internal/config"
	httptransport "github.com/fathurwithyou/microservices/services/portal-consumer/internal/transport/http"
)

const (
	queueName   = "portal.hazard-events"
	serviceName = "portal-consumer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var consumer *rabbitmq.Consumer
	for {
		candidate, err := rabbitmq.New(cfg.RabbitURL, queueName, serviceName, logger)
		if err == nil {
			consumer = candidate
			break
		}
		logger.Error("broker connection failed", "service", serviceName, "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return
		}
	}
	defer consumer.Close()
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
