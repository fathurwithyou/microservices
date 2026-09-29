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

	"github.com/fathurwithyou/microservices/pkg/consumerkit"
	"github.com/fathurwithyou/microservices/pkg/healthsrv"
	"github.com/fathurwithyou/microservices/services/dashboard-consumer/internal/application"
	"github.com/fathurwithyou/microservices/services/dashboard-consumer/internal/config"
)

const (
	queueName   = "dashboard.hazard-events"
	serviceName = "dashboard-consumer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration load failed", "service", serviceName, "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	healthServer := healthsrv.New(healthsrv.Config{Port: cfg.HealthPort, Service: serviceName})
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server stopped", "service", serviceName, "error", err)
			stop()
		}
	}()

	consumer, err := consumerkit.New(consumerkit.Config{
		URL:     cfg.RabbitURL,
		Queue:   queueName,
		Service: serviceName,
		Logger:  logger,
	})
	if err != nil {
		logger.Error("consumer setup failed", "service", serviceName, "error", err)
		os.Exit(1)
	}

	processor := application.NewProcessor(serviceName, logger)
	go func() {
		if err := consumer.Run(ctx, processor.Process); err != nil {
			logger.Error("consumer stopped", "service", serviceName, "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = healthServer.Shutdown(shutdownCtx)
}
