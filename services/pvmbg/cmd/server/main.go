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

// serviceName identifies this service in structured logs.
const serviceName = "pvmbg"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "service", serviceName, "error", err)
		os.Exit(1)
	}

	runtime, err := state.New(state.Config{MinDelay: cfg.MinDelay, MaxDelay: cfg.MaxDelay, SeedCount: cfg.SeedCount})
	if err != nil {
		logger.Error("state initialization failed", "service", serviceName, "error", err)
		os.Exit(1)
	}
	producer, err := application.NewReportProducer(application.Config{State: runtime, Interval: cfg.NewReportInterval})
	if err != nil {
		logger.Error("producer initialization failed", "service", serviceName, "error", err)
		os.Exit(1)
	}
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           httptransport.NewRouter(httptransport.RouterConfig{State: runtime, Token: cfg.Token, AdminKey: cfg.AdminKey, Logger: logger}),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		// WriteTimeout is 10s: PVMBG injects a random artificial delay of up
		// to 3s into /volcanic-reports, so the timeout needs more headroom
		// than BMKG's 5s.
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go producer.Run(ctx)
	go func() {
		logger.Info("server started", "service", serviceName, "port", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "service", serviceName, "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
