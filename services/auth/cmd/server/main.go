package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

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

	go func() {
		logger.Info("server started", "service", "auth")
		if err := server.Serve(listener); err != nil {
			logger.Error("server stopped", "service", "auth", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	server.GracefulStop()
	pool.Close()
}

// fatal logs the failure with the standard service fields and exits. Callers
// must run any needed cleanup before calling it, since os.Exit skips defers.
func fatal(logger *slog.Logger, message string, err error) {
	logger.Error(message, "service", "auth", "error", err)
	os.Exit(1)
}
