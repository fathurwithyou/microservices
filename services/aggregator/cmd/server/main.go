package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/postgres"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/rabbitmq"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/sources"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/outbox"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/config"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ingest"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/store/memory"
	grpctransport "github.com/fathurwithyou/microservices/services/aggregator/internal/transport/grpc"
	httptransport "github.com/fathurwithyou/microservices/services/aggregator/internal/transport/http"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	os.Exit(run(context.Background(), os.Args[1:], logger))
}

func run(ctx context.Context, args []string, logger *slog.Logger) int {
	command := "serve"
	rest := args
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command = args[0]
		rest = args[1:]
	}
	switch command {
	case "serve":
		return runServe(ctx, rest, logger)
	case "migrate":
		return runMigrate(ctx, rest, logger)
	case "seed":
		return runSeed(ctx, rest, logger)
	default:
		logger.Error("unknown command", "service", "aggregator", "command", command)
		return 2
	}
}

func runServe(ctx context.Context, args []string, logger *slog.Logger) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	wait := flags.Duration("wait", 60*time.Second, "how long to wait for PostgreSQL during boot")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "service", "aggregator", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, closeStore, err := newStore(ctx, cfg, *wait)
	if err != nil {
		logger.Error("storage initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	defer closeStore()
	volcanoes, err := store.LoadVolcanoReferences(ctx)
	if err != nil {
		logger.Error("volcano reference loading failed", "service", "aggregator", "error", err)
		return 1
	}
	if len(volcanoes) == 0 {
		logger.Error("volcano reference table is empty", "service", "aggregator")
		return 1
	}
	publisher, err := newPublisher(cfg, logger)
	if err != nil {
		logger.Error("publisher initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	defer publisher.Close()
	poller := ingest.NewPoller(
		store,
		sources.New(cfg.BMKGURL, "bmkg", "X-BMKG-Key", cfg.BMKGKey, cfg.RequestTimeout, logger),
		sources.New(cfg.PVMBGURL, "pvmbg", "Authorization", "Bearer "+cfg.PVMBGToken, cfg.RequestTimeout, logger),
		cfg.PollInterval,
		cfg.RequestTimeout,
		logger,
		volcanoes,
	)
	outboxWorker := outbox.NewWorker(store, publisher, cfg.OutboxPollInterval, logger)
	queries := hazards.NewQueries(store)

	grpcListener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("gRPC listen failed", "service", "aggregator", "error", err)
		return 1
	}
	grpcServer := grpctransport.NewServer(queries, cfg.InternalServiceToken, logger)
	httpServer := httptransport.NewHealthServer(cfg.HTTPPort)

	go poller.Run(ctx)
	go outboxWorker.Run(ctx)
	go func() {
		logger.Info("gRPC server started", "service", "aggregator", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server stopped", "service", "aggregator", "error", err)
			stop()
		}
	}()
	go func() {
		logger.Info("HTTP health server started", "service", "aggregator", "port", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server stopped", "service", "aggregator", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	return 0
}

func runMigrate(ctx context.Context, args []string, logger *slog.Logger) int {
	if len(args) == 0 {
		logger.Error("migrate requires a subcommand", "service", "aggregator", "usage", "migrate up|down|status")
		return 2
	}
	subcommand, rest := args[0], args[1:]
	switch subcommand {
	case "up", "status":
		flags := flag.NewFlagSet("migrate "+subcommand, flag.ContinueOnError)
		wait := flags.Duration("wait", 60*time.Second, "how long to wait for PostgreSQL")
		if err := flags.Parse(rest); err != nil {
			return 2
		}
		databaseURL, err := databaseURLOrNoop(logger, "migrations")
		if err != nil || databaseURL == "" {
			return noOpExit(err)
		}
		if subcommand == "up" {
			if err := postgres.Up(ctx, databaseURL, *wait); err != nil {
				logger.Error("migration failed", "service", "aggregator", "error", err)
				return 1
			}
			logger.Info("migrations applied", "service", "aggregator")
			return 0
		}
		if err := postgres.Status(ctx, databaseURL, *wait); err != nil {
			logger.Error("migration status failed", "service", "aggregator", "error", err)
			return 1
		}
		return 0
	case "down":
		flags := flag.NewFlagSet("migrate down", flag.ContinueOnError)
		wait := flags.Duration("wait", 60*time.Second, "how long to wait for PostgreSQL")
		yes := flags.Bool("yes", false, "confirm rollback of the latest migration")
		if err := flags.Parse(rest); err != nil {
			return 2
		}
		databaseURL, err := databaseURLOrNoop(logger, "rollback")
		if err != nil || databaseURL == "" {
			return noOpExit(err)
		}
		if err := postgres.Down(ctx, databaseURL, *wait, *yes); err != nil {
			logger.Error("rollback failed", "service", "aggregator", "error", err)
			return 1
		}
		logger.Info("migration rolled back", "service", "aggregator")
		return 0
	default:
		logger.Error("unknown migrate subcommand", "service", "aggregator", "subcommand", subcommand)
		return 2
	}
}

func runSeed(ctx context.Context, args []string, logger *slog.Logger) int {
	flags := flag.NewFlagSet("seed", flag.ContinueOnError)
	wait := flags.Duration("wait", 60*time.Second, "how long to wait for PostgreSQL")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	databaseURL, err := databaseURLOrNoop(logger, "seeding")
	if err != nil || databaseURL == "" {
		return noOpExit(err)
	}
	pool, err := postgres.Connect(ctx, databaseURL, *wait)
	if err != nil {
		logger.Error("seed connection failed", "service", "aggregator", "error", err)
		return 1
	}
	defer pool.Close()
	if err := postgres.Seed(ctx, pool); err != nil {
		logger.Error("seeding failed", "service", "aggregator", "error", err)
		return 1
	}
	logger.Info("seeding completed", "service", "aggregator")
	return 0
}

func databaseURLOrNoop(logger *slog.Logger, action string) (string, error) {
	if isMemoryDriver() {
		logger.Info("STORE_DRIVER=memory: nothing to do", "service", "aggregator", "action", action)
		return "", nil
	}
	databaseURL := os.Getenv("AGGREGATOR_DATABASE_URL")
	if databaseURL == "" {
		return "", errors.New("AGGREGATOR_DATABASE_URL is required when STORE_DRIVER=postgres")
	}
	return databaseURL, nil
}

func noOpExit(err error) int {
	if err != nil {
		slog.Error("command failed", "service", "aggregator", "error", err)
		return 1
	}
	return 0
}

func isMemoryDriver() bool {
	driver := strings.ToLower(strings.TrimSpace(os.Getenv("STORE_DRIVER")))
	return driver == "memory"
}

func newStore(ctx context.Context, cfg config.Config, wait time.Duration) (ports.Store, func(), error) {
	switch strings.ToLower(cfg.StoreDriver) {
	case "postgres":
		if cfg.DatabaseURL == "" {
			return nil, nil, errors.New("AGGREGATOR_DATABASE_URL is required when STORE_DRIVER=postgres")
		}
		postgresStore, err := postgres.NewWithWait(ctx, cfg.DatabaseURL, wait)
		if err != nil {
			return nil, nil, fmt.Errorf("connect PostgreSQL: %w", err)
		}
		return postgresStore, postgresStore.Close, nil
	case "memory":
		return memory.New(), func() {}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported STORE_DRIVER %q", cfg.StoreDriver)
	}
}

func newPublisher(cfg config.Config, logger *slog.Logger) (ports.Publisher, error) {
	if cfg.RabbitURL == "" {
		return nil, errors.New("RABBITMQ_URL is required")
	}
	return rabbitmq.New(cfg.RabbitURL, logger), nil
}
