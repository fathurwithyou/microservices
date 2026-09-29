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

	"github.com/fathurwithyou/microservices/services/bmkg/internal/application"
	"github.com/fathurwithyou/microservices/services/bmkg/internal/config"
	"github.com/fathurwithyou/microservices/services/bmkg/internal/state"
	httptransport "github.com/fathurwithyou/microservices/services/bmkg/internal/transport/http"
)

// serviceName identifies this service in structured logs.
const serviceName = "bmkg"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "service", serviceName, "error", err)
		os.Exit(1)
	}

	memory := state.New(state.Config{SeedCount: cfg.SeedCount})
	producer := application.NewEventProducer(application.Config{State: memory, Interval: cfg.NewEventInterval})
	server := &http.Server{
		Addr: ":" + cfg.Port,
		Handler: httptransport.NewRouter(httptransport.RouterConfig{
			State:         memory,
			APIKey:        cfg.APIKey,
			ResponseDelay: cfg.ResponseDelay,
			Logger:        logger,
		}),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		// WriteTimeout is 5s: BMKG injects a fixed artificial delay
		// (default 100ms, configured 50-150ms) into /seismic-events, so
		// 5s leaves ample headroom for delay plus serialization.
		WriteTimeout: 5 * time.Second,
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
