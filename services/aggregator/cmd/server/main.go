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

	"github.com/fathurwithyou/microservices/pkg/lease"
	"github.com/fathurwithyou/microservices/pkg/observability"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/postgres"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/rabbitmq"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/sources"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/analytics"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/audit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/failures"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/fieldreports"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	applicationingest "github.com/fathurwithyou/microservices/services/aggregator/internal/application/ingest"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/outbox"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/config"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/importer"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
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
	case "import-catalog":
		return runImportCatalog(ctx, rest, logger)
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
	shutdownTracing, err := observability.Setup(ctx, "aggregator")
	if err != nil {
		logger.Error("tracing initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	persistence, closePersistence, err := newPersistence(ctx, cfg, *wait)
	if err != nil {
		logger.Error("storage initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	defer closePersistence()
	volcanoes, err := persistence.LoadVolcanoReferences(ctx)
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
	bmkgClient, err := sources.New(sources.ClientConfig{BaseURL: cfg.BMKGURL, Name: "bmkg", Header: "X-BMKG-Key", Value: cfg.BMKGKey, Timeout: cfg.RequestTimeout, Logger: logger})
	if err != nil {
		logger.Error("bmkg client initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	pvmbgClient, err := sources.New(sources.ClientConfig{BaseURL: cfg.PVMBGURL, Name: "pvmbg", Header: "Authorization", Value: "Bearer " + cfg.PVMBGToken, Timeout: cfg.RequestTimeout, Logger: logger})
	if err != nil {
		logger.Error("pvmbg client initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	bmkgSource, err := applicationingest.NewBMKGSource(applicationingest.BMKGSourceConfig{Client: bmkgClient, Logger: logger})
	if err != nil {
		logger.Error("bmkg source initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	pvmbgSource, err := applicationingest.NewPVMBGSource(applicationingest.PVMBGSourceConfig{Client: pvmbgClient, Logger: logger, Volcanoes: volcanoes})
	if err != nil {
		logger.Error("pvmbg source initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	var (
		leadership applicationingest.Leadership
		leader     *applicationingest.Leader
	)
	if repository, ok := persistence.(*postgres.Repository); ok {
		coordinator := lease.New(repository.LeaseStore("ingestion"))
		leader = applicationingest.NewLeader(coordinator)
		go leader.Run(ctx, coordinator, leaderHolder(), cfg.LeaderTTL, cfg.LeaderRenew, logger)
		leadership = leader
	}
	poller, err := applicationingest.NewPoller(applicationingest.PollerConfig{
		Repository: persistence,
		Sources:    []applicationingest.Source{bmkgSource, pvmbgSource},
		Interval:   cfg.PollInterval,
		Timeout:    cfg.RequestTimeout,
		Leadership: leadership,
		Logger:     logger,
	})
	if err != nil {
		logger.Error("poller initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	outboxWorker, err := outbox.NewWorker(outbox.WorkerConfig{Repository: persistence, Publisher: publisher, Interval: cfg.OutboxPollInterval, Logger: logger})
	if err != nil {
		logger.Error("outbox worker initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	queries := hazards.NewQueries(persistence)
	auditQueries := audit.NewQueries(persistence)
	analyticsQueries := analytics.NewQueries(persistence)
	submitter, err := applicationingest.NewSubmitter(persistence, logger)
	if err != nil {
		logger.Error("hazard submitter initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	replayer, err := failures.New(failures.Config{Repository: persistence, Submitter: submitter, Volcanoes: volcanoes, References: persistence.LoadVolcanoReferences, Logger: logger})
	if err != nil {
		logger.Error("failure replayer initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	// Transient failures (for example a missing reference row) are retried
	// automatically; schema violations stay parked until an Ops replay.
	// Fence on the database clock, like the poller: a replica frozen past the
	// lease TTL must not keep retrying once another replica took over.
	retryAllowed := func(ctx context.Context) bool {
		if leader == nil {
			return true
		}
		valid, err := leader.StillLeader(ctx)
		if err != nil {
			logger.Warn("retry leadership check failed", "service", "aggregator", "error", err)
		}
		return valid
	}
	retryWorker, err := failures.NewRetryWorker(failures.RetryWorkerConfig{
		Replayer:    replayer,
		Recorder:    persistence,
		Interval:    cfg.FailureRetryInterval,
		MaxAttempts: cfg.FailureRetryMaxAttempts,
		Allowed:     retryAllowed,
		Logger:      logger,
	})
	if err != nil {
		logger.Error("failure retry worker initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	fieldReportService, err := fieldreports.New(fieldreports.Config{Repository: persistence, Logger: logger})
	if err != nil {
		logger.Error("field report service initialization failed", "service", "aggregator", "error", err)
		return 1
	}
	fieldReportWorker, err := fieldreports.NewWorker(fieldreports.WorkerConfig{Repository: persistence, Processor: fieldreports.NewInboxProcessor(logger), Logger: logger})
	if err != nil {
		logger.Error("field report worker initialization failed", "service", "aggregator", "error", err)
		return 1
	}

	grpcListener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("gRPC listen failed", "service", "aggregator", "error", err)
		return 1
	}
	grpcServer := grpctransport.NewServer(grpctransport.ServerConfig{Queries: queries, FieldReports: fieldReportService, Analytics: analyticsQueries, Submitter: submitter, Health: persistence, ServiceToken: cfg.InternalServiceToken, Logger: logger})
	httpServer := httptransport.NewServer(cfg.HTTPPort, auditQueries, cfg.AuditToken, cfg.OpsToken, replayer)

	go poller.Run(ctx)
	go outboxWorker.Run(ctx)
	go fieldReportWorker.Run(ctx)
	go retryWorker.Run(ctx)
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

func runImportCatalog(ctx context.Context, args []string, logger *slog.Logger) int {
	flags := flag.NewFlagSet("import-catalog", flag.ContinueOnError)
	path := flags.String("file", "", "JSON array or NDJSON catalog file")
	wait := flags.Duration("wait", 60*time.Second, "how long to wait for PostgreSQL")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *path == "" {
		logger.Error("catalog file is required", "service", "aggregator", "flag", "-file")
		return 2
	}
	databaseURL, err := databaseURLOrNoop(logger, "catalog import")
	if err != nil || databaseURL == "" {
		return noOpExit(err)
	}
	file, err := os.Open(*path)
	if err != nil {
		logger.Error("catalog open failed", "service", "aggregator", "error", err)
		return 1
	}
	defer file.Close()
	rows, err := importer.Read(file)
	if err != nil {
		logger.Error("catalog decode failed", "service", "aggregator", "error", err)
		return 1
	}
	events, failures := importer.Events(rows, time.Now().UTC())
	pool, err := postgres.Connect(ctx, databaseURL, *wait)
	if err != nil {
		logger.Error("catalog connection failed", "service", "aggregator", "error", err)
		return 1
	}
	defer pool.Close()
	inserted, err := postgres.NewRepositoryFromPool(pool).BulkPersistHazards(ctx, events)
	if err != nil {
		logger.Error("catalog insert failed", "service", "aggregator", "error", err)
		return 1
	}
	logger.Info("catalog import completed", "service", "aggregator",
		"rows", len(rows), "mapped", len(events), "inserted", inserted, "rejected", len(failures))
	for index, failure := range failures {
		if index >= 5 {
			logger.Warn("additional catalog rows rejected", "service", "aggregator", "remaining", len(failures)-index)
			break
		}
		logger.Warn("catalog row rejected", "service", "aggregator", "error", failure)
	}
	return 0
}

func databaseURLOrNoop(logger *slog.Logger, action string) (string, error) {
	if isMemoryDriver() {
		logger.Info("STORE_DRIVER=memory: nothing to do", "service", "aggregator", "action", action)
		return "", nil
	}
	databaseURL := os.Getenv("AGGREGATOR_DATABASE_URL")
	if databaseURL == "" {
		return "", errors.New("AGGREGATOR_DATABASE_URL is required for a database store driver")
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

func leaderHolder() string {
	if hostname := strings.TrimSpace(os.Getenv("HOSTNAME")); hostname != "" {
		return hostname
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "aggregator"
	}
	return hostname
}

type persistence interface {
	hazards.Repository
	audit.Repository
	analytics.Repository
	applicationingest.Repository
	outbox.Repository
	fieldreports.Repository
	failures.Repository
	failures.AttemptRecorder
	LoadVolcanoReferences(context.Context) (map[string]domain.VolcanoReference, error)
}

func newPersistence(ctx context.Context, cfg config.Config, wait time.Duration) (persistence, func(), error) {
	switch strings.ToLower(cfg.StoreDriver) {
	case "postgres", "cockroach":
		if cfg.DatabaseURL == "" {
			return nil, nil, errors.New("AGGREGATOR_DATABASE_URL is required for a database store driver")
		}
		postgresRepository, err := postgres.NewRepositoryWithWait(ctx, cfg.DatabaseURL, wait)
		if err != nil {
			return nil, nil, fmt.Errorf("connect database: %w", err)
		}
		return postgresRepository, postgresRepository.Close, nil
	case "memory":
		return memory.NewRepository(), func() {}, nil
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
