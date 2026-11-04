package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fathurwithyou/microservices/pkg/healthsrv"
	"github.com/fathurwithyou/microservices/pkg/observability"
	"github.com/fathurwithyou/microservices/services/auth/internal/adapters/postgres"
	"github.com/fathurwithyou/microservices/services/auth/internal/application"
	"github.com/fathurwithyou/microservices/services/auth/internal/config"
	grpctransport "github.com/fathurwithyou/microservices/services/auth/internal/transport/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		fatal(logger, "configuration failed", err)
	}
	shutdownTracing, err := observability.Setup(context.Background(), "auth")
	if err != nil {
		fatal(logger, "tracing initialization failed", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL, cfg.DatabaseWait)
	if err != nil {
		fatal(logger, "auth database initialization failed", err)
	}
	defer pool.Close()
	// os.Exit skips deferred calls, so every fatal path after a successful
	// Connect closes the pool explicitly before exiting.

	if err := postgres.Migrate(ctx, pool); err != nil {
		pool.Close()
		fatal(logger, "auth database migration failed", err)
	}
	authRepository := postgres.NewRepository(pool)
	if err := authRepository.EnsureClients(ctx, cfg.Clients); err != nil {
		pool.Close()
		fatal(logger, "auth client persistence failed", err)
	}
	manager, err := application.NewTokenManager(application.TokenManagerConfig{
		Repository:     authRepository,
		AccessTokenTTL: cfg.AccessTokenTTL,
	})
	if err != nil {
		pool.Close()
		fatal(logger, "auth token manager initialization failed", err)
	}

	listener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		pool.Close()
		fatal(logger, "listen failed", err)
	}
	server := grpctransport.NewServer(grpctransport.ServerConfig{
		Tokens:       manager,
		ServiceToken: cfg.InternalServiceToken,
		Logger:       logger,
	})
	metricsServer := healthsrv.New(healthsrv.Config{Port: cfg.MetricsPort, Service: "auth"})

	go func() {
		logger.Info("server started", "service", "auth")
		if err := server.Serve(listener); err != nil {
			logger.Error("server stopped", "service", "auth", "error", err)
			stop()
		}
	}()
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped", "service", "auth", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	server.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsServer.Shutdown(shutdownCtx)
	pool.Close()
}

// fatal logs the failure with the standard service fields and exits. Callers
// must run any needed cleanup before calling it, since os.Exit skips defers.
func fatal(logger *slog.Logger, message string, err error) {
	logger.Error(message, "service", "auth", "error", err)
	os.Exit(1)
}
