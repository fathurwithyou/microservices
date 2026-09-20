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
		logger.Error("configuration failed", "service", "auth", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL, cfg.DatabaseWait)
	if err != nil {
		logger.Error("auth database initialization failed", "service", "auth", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool); err != nil {
		logger.Error("auth database migration failed", "service", "auth", "error", err)
		os.Exit(1)
	}
	authStore := postgres.New(pool)
	if err := authStore.EnsureClients(ctx, cfg.Clients); err != nil {
		logger.Error("auth client persistence failed", "service", "auth", "error", err)
		os.Exit(1)
	}
	manager := application.NewTokenManager(cfg.AccessTokenTTL, authStore)

	listener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("listen failed", "service", "auth", "error", err)
		os.Exit(1)
	}
	server := grpctransport.NewServer(manager, cfg.InternalServiceToken, logger)

	go func() {
		logger.Info("server started", "service", "auth")
		if err := server.Serve(listener); err != nil {
			logger.Error("server stopped", "service", "auth", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	server.GracefulStop()
}
