// satellite-feed is a local stand-in for the assistants' Satellite Feed
// (https://satellite.naufarrel.dev) used by M2 Problem 4.
//
//	satellite-feed            serve the feed
//	satellite-feed -dump      print the complete event log as NDJSON and exit
//	satellite-feed -stats     print scenario statistics and the special cases
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-feed/internal/feed"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dump := flag.Bool("dump", false, "print the complete event log as NDJSON and exit")
	stats := flag.Bool("stats", false, "print scenario statistics as JSON and exit")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "satellite-feed")
	fail := func(msg string, err error) {
		log.Error(msg, "error", err)
		os.Exit(1)
	}

	seed, err := strconv.ParseInt(env("FEED_SEED", "4031"), 10, 64)
	if err != nil {
		fail("FEED_SEED", err)
	}
	examSeed, err := strconv.ParseInt(env("FEED_EXAM_SEED", "20261112"), 10, 64)
	if err != nil {
		fail("FEED_EXAM_SEED", err)
	}
	speed, err := strconv.ParseFloat(env("FEED_SPEED", "1"), 64)
	if err != nil || speed < 0 {
		fail("FEED_SPEED", errors.New("must be a number >= 0"))
	}
	rate, err := strconv.ParseFloat(env("FEED_RATE", "1"), 64)
	if err != nil || rate <= 0 {
		fail("FEED_RATE", errors.New("must be a number > 0"))
	}
	start, err := time.Parse(time.RFC3339, env("FEED_VIRTUAL_START", "2026-11-12T00:00:00Z"))
	if err != nil {
		fail("FEED_VIRTUAL_START", err)
	}
	hours, err := strconv.Atoi(env("FEED_VIRTUAL_HOURS", "48"))
	if err != nil || hours < 1 {
		fail("FEED_VIRTUAL_HOURS", errors.New("must be an integer >= 1"))
	}
	heartbeat, err := time.ParseDuration(env("FEED_HEARTBEAT", "10s"))
	if err != nil || heartbeat <= 0 {
		fail("FEED_HEARTBEAT", errors.New("must be a positive duration such as 10s"))
	}
	maxConn, err := strconv.Atoi(env("FEED_MAX_CONNECTIONS", "2"))
	if err != nil || maxConn < 1 {
		fail("FEED_MAX_CONNECTIONS", errors.New("must be an integer >= 1"))
	}
	inclusive, err := strconv.ParseBool(env("FEED_FROM_INCLUSIVE", "true"))
	if err != nil {
		fail("FEED_FROM_INCLUSIVE", err)
	}
	shape := env("FEED_RETRACT_SHAPE", "key")
	if shape != "key" && shape != "fields" {
		fail("FEED_RETRACT_SHAPE", errors.New(`must be "key" or "fields"`))
	}
	cfg := feed.Config{Seed: seed, Start: start, Hours: hours, RateMilli: int(math.Round(rate * 1000))}

	if *dump || *stats {
		sc := feed.Generate(cfg)
		out := bufio.NewWriterSize(os.Stdout, 1<<20)
		if *dump {
			err = feed.Dump(sc, shape, func(b []byte) error { _, err := out.Write(b); return err })
		} else {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			err = enc.Encode(map[string]any{"seed": seed, "stats": sc.Stats, "specials": sc.Specials, "problems": sc.Problems})
		}
		if err == nil {
			err = out.Flush()
		}
		if err != nil {
			fail("write", err)
		}
		return
	}

	token := os.Getenv("FEED_TOKEN")
	if token == "" {
		fail("FEED_TOKEN", errors.New("is required"))
	}
	adminToken := os.Getenv("FEED_ADMIN_TOKEN")
	if adminToken == "" {
		log.Warn("FEED_ADMIN_TOKEN is empty: the admin API is disabled")
	}
	srv := feed.NewServer(feed.Options{
		Token: token, AdminToken: adminToken,
		Group: env("FEED_GROUP", "local"), Profile: env("FEED_PROFILE", "practice"),
		Seed: seed, ExamSeed: examSeed, Speed: speed, Start: start, Hours: hours, RateMilli: cfg.RateMilli,
		Heartbeat: heartbeat, MaxConnections: maxConn, FromInclusive: inclusive, RetractShape: shape,
		Tick: 20 * time.Millisecond, Logger: log,
	})
	addr := env("FEED_ADDR", ":8088")
	// No WriteTimeout: streams are long-lived and set their own write deadlines.
	httpSrv := &http.Server{Addr: addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		srv.Drop("shutdown")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	log.Info("listening", "addr", addr, "seed", seed, "speed", speed, "virtual_hours", hours, "rate", rate,
		"from_inclusive", inclusive, "retract_shape", shape, "max_connections", maxConn, "heartbeat", heartbeat.String())
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fail("serve", err)
	}
}
