package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fathurwithyou/microservices/pkg/healthsrv"
	"github.com/fathurwithyou/microservices/pkg/lease"
	feed "github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/adapters/feed"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/adapters/postgres"
	sink "github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/adapters/sink"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/application"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/config"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

// serviceName identifies this service in structured logs.
const serviceName = "satellite-ingestor"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	os.Exit(run(context.Background(), os.Args[1:], logger))
}

func run(ctx context.Context, args []string, logger *slog.Logger) int {
	command := "serve"
	if len(args) > 0 {
		command = args[0]
	}

	switch command {
	case "serve":
		if err := runServe(ctx, logger); err != nil {
			logger.Error("serve stopped", "service", serviceName, "error", err)
			return 1
		}
		return 0
	default:
		logger.Error("unknown command", "service", serviceName, "command", command)
		return 2
	}
}

func holderName(fallback string) string {
	if hostname := strings.TrimSpace(os.Getenv("HOSTNAME")); hostname != "" {
		return hostname
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return fallback
	}
	return hostname
}

func runServe(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	grpcSink, err := sink.NewGRPC(cfg.AggregatorAddr, cfg.InternalToken)
	if err != nil {
		return err
	}
	defer grpcSink.Close()

	var state ports.State = memory.New()
	var leadership application.Leadership
	if cfg.DatabaseURL != "" {
		pgState, err := postgres.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pgState.Close()
		state = pgState
		coordinator := lease.New(pgState.LeaseStore("satellite-ingestion"))
		leader := application.NewLeader(coordinator)
		go leader.Run(ctx, holderName("satellite-ingestor"), cfg.LeaderTTL, cfg.LeaderRenew, logger)
		leadership = leader
		logger.Info("durable state enabled", "service", serviceName, "component", "state")
	}

	stream := application.NewStream(application.StreamConfig{
		Feed: feed.New(feed.Config{
			URL:         cfg.FeedURL,
			Token:       cfg.FeedToken,
			IdleTimeout: cfg.IdleTimeout,
		}),
		State:      state,
		Sink:       grpcSink,
		Logger:     logger,
		LateWindow: cfg.LateWindow,
		Leadership: leadership,
		MinBackoff: cfg.MinBackoff,
		MaxBackoff: cfg.MaxBackoff,
	})

	// GET /status reports what P4 asks to be reported: lag, watermark,
	// accepted lateness, lone detections, late and low-confidence counts.
	healthServer := healthsrv.New(healthsrv.Config{Port: cfg.HealthPort, Service: serviceName})
	base := healthServer.Handler
	healthServer.Handler = http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/status" {
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(stream.Snapshot())
			return
		}
		base.ServeHTTP(response, request)
	})
	go func() {
		logger.Info("health server started", "service", serviceName, "port", cfg.HealthPort)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server stopped", "service", serviceName, "error", err)
			stop()
		}
	}()

	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(ctx) }()

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-streamErr:
		if serveErr != nil {
			stop()
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = healthServer.Shutdown(shutdownCtx)
	return serveErr
}
