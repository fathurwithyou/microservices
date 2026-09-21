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

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/application"
	"github.com/fathurwithyou/microservices/services/pvmbg/internal/config"
	"github.com/fathurwithyou/microservices/services/pvmbg/internal/state"
	httptransport "github.com/fathurwithyou/microservices/services/pvmbg/internal/transport/http"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "service", "pvmbg", "error", err)
		os.Exit(1)
	}

	state := state.New(cfg.MinDelay, cfg.MaxDelay, cfg.SeedCount)
	producer := application.NewReportProducer(state, cfg.NewReportInterval)
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           httptransport.NewRouter(httptransport.RouterConfig{State: state, Token: cfg.Token, AdminKey: cfg.AdminKey, Logger: logger}),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go producer.Run(ctx)
	go func() {
		logger.Info("server started", "service", "pvmbg", "port", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "service", "pvmbg", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
