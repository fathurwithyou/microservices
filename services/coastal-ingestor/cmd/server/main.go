package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fathurwithyou/microservices/pkg/healthsrv"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/adapters/postgres"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/adapters/sensor"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/adapters/sink"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/application"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/config"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

// serviceName identifies this service in structured logs.
const serviceName = "coastal-ingestor"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	os.Exit(run(context.Background(), os.Args[1:], logger))
}

func run(ctx context.Context, args []string, logger *slog.Logger) int {
	if len(args) > 0 && args[0] != "serve" {
		logger.Error("unknown command", "service", serviceName, "command", args[0])
		return 2
	}
	if err := runServe(ctx, logger); err != nil {
		logger.Error("service stopped", "service", serviceName, "error", err)
		return 1
	}
	return 0
}

func runServe(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// Empty COASTAL_DATABASE_URL keeps the in-memory adapter for fixture runs.
	state := ports.State(memory.New())
	if cfg.DatabaseURL != "" {
		pgState, err := postgres.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("connect state store: %w", err)
		}
		defer pgState.Close()
		state = pgState
	}
	grpcSink, err := sink.NewGRPC(cfg.AggregatorAddr, cfg.InternalToken)
	if err != nil {
		return err
	}
	defer grpcSink.Close()
	ingestor := application.NewIngestor(
		application.IngestConfig{Interval: cfg.PollInterval},
		sensor.New(cfg.SensorURL, cfg.SensorToken, cfg.RequestTimeout),
		state,
		grpcSink,
		application.NewValidator(
			application.ValidatorConfig{
				// The sensor accelerates its own clock (PANTAI_SPEED), so a
				// virtual observed_at can legitimately run far ahead of the
				// pod clock. Staleness is still checked against MaxAge.
				MaxFutureSkew: 365 * 24 * time.Hour,
				MaxAge:        cfg.MaxAge,
			},
			time.Now,
			nil,
		),
		application.NewCircuitBreaker(application.CircuitConfig{
			FailureThreshold: cfg.CircuitFailureThreshold,
			FailureWindow:    cfg.CircuitFailureWindow,
			OpenTimeout:      cfg.CircuitOpenTimeout,
		}, time.Now),
		logger,
		time.Now,
	)

	healthServer := healthsrv.New(healthsrv.Config{Port: cfg.HealthPort, Service: serviceName})

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		ingestor.Run(ctx)
	}()

	go func() {
		logger.Info("health server started", "service", serviceName, "port", cfg.HealthPort)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server stopped", "service", serviceName, "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("health server shutdown", "service", serviceName, "error", err)
	}
	workers.Wait()
	logger.Info("service stopped", "service", serviceName, "reason", ctx.Err())
	return nil
}
