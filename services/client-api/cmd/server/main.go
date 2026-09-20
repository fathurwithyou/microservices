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

	grpcadapter "github.com/fathurwithyou/microservices/services/client-api/internal/adapters/grpc"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
	"github.com/fathurwithyou/microservices/services/client-api/internal/config"
	httptransport "github.com/fathurwithyou/microservices/services/client-api/internal/transport/http"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "service", "client-api", "error", err)
		os.Exit(1)
	}

	clients, err := grpcadapter.Dial(cfg.AggregatorAddress, cfg.AuthAddress, cfg.InternalServiceToken)
	if err != nil {
		logger.Error("gRPC dial failed", "service", "client-api", "error", err)
		os.Exit(1)
	}
	defer clients.Close()
	api := application.NewAPI(clients.Auth, clients.Aggregator)
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           httptransport.NewRouter(api, cfg.MaxInflightRequests, cfg.RequestTimeout, logger),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		logger.Info("server started", "service", "client-api", "port", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "service", "client-api", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
