package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nguyen-anh-hao/risk-engine/internal/handler"
	"github.com/nguyen-anh-hao/risk-engine/internal/journal"
	"github.com/nguyen-anh-hao/risk-engine/internal/metrics"
	"github.com/nguyen-anh-hao/risk-engine/internal/outbox"
	"github.com/nguyen-anh-hao/risk-engine/internal/service"
	"github.com/nguyen-anh-hao/risk-engine/internal/storage"
	"github.com/nguyen-anh-hao/risk-engine/internal/store"
	"github.com/nguyen-anh-hao/risk-engine/internal/telemetry"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func metricsMiddleware(next http.Handler) http.Handler {
	requests := metrics.Default.Counter(
		"http_requests_total", "Total HTTP requests.", "method", "path", "status")
	duration := metrics.Default.Histogram(
		"http_request_duration_seconds", "HTTP request latency in seconds.",
		[]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}, "method", "path")
	inFlight := metrics.Default.Gauge(
		"http_requests_in_flight", "HTTP requests currently being served.", "method")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		inFlight.Add(1, r.Method)
		defer inFlight.Add(-1, r.Method)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		requests.Inc(r.Method, r.URL.Path, strconv.Itoa(rec.status))
		duration.Observe(time.Since(start).Seconds(), r.Method, r.URL.Path)
	})
}

func main() {
	// Risk state is shared in Redis; the service must never keep balances in
	// process memory if it is going to run as multiple replicas.
	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "localhost:6379"),
		Password: env("REDIS_PASSWORD", ""),
		DB:       0,
	})
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	pingErr := rdb.Ping(pingCtx).Err()
	cancel()
	if pingErr != nil {
		slog.Error("redis unavailable; risk-engine requires shared state", "err", pingErr)
		os.Exit(1)
	}
	redisStore := store.NewRedis(rdb)
	defer func() { _ = redisStore.Close() }()
	if err := redisStore.Seed(context.Background(), storage.InitialBalances()); err != nil {
		slog.Error("balance seed failed", "err", err)
		os.Exit(1)
	}

	enc, err := outbox.NewEncoder(env("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "exchange.risk.RiskEvent")
	if err != nil {
		slog.Error("risk outbox Avro encoder unavailable", "err", err)
		os.Exit(1)
	}
	pg, err := store.NewPostgres(context.Background(), env("RISK_DB_DSN", "postgres://settlement:settlement@localhost:5432/risk?sslmode=disable"), enc)
	if err != nil {
		slog.Error("risk PostgreSQL unavailable", "err", err)
		os.Exit(1)
	}
	defer func() { _ = pg.Close() }()
	if err := pg.Seed(context.Background(), storage.InitialBalances()); err != nil {
		slog.Error("risk DB seed failed", "err", err)
		os.Exit(1)
	}
	redisStore.SetFallback(pg)
	if err := redisStore.RebuildMissing(context.Background(), pg); err != nil {
		slog.Warn("risk Redis projection rebuild failed", "err", err)
	}
	journalLen := metrics.Default.Gauge("trading_risk_journal_length", "risk:journal stream length.")
	journalPending := metrics.Default.Gauge("trading_risk_journal_pending", "risk:journal pending entries.")
	journalLag := metrics.Default.Gauge("trading_risk_journal_lag_seconds", "Age of the oldest pending risk:journal entry.")
	outboxPending := metrics.Default.Gauge("trading_outbox_pending", "Pending outbox rows.", "service")
	outboxAge := metrics.Default.Gauge("trading_outbox_oldest_age_seconds", "Oldest pending outbox event age.", "service")
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			n, p, lag, err := redisStore.JournalStats(context.Background())
			if err != nil {
				continue
			}
			journalLen.Set(float64(n))
			journalPending.Set(float64(p))
			journalLag.Set(lag)
			if pending, age, err := pg.OutboxStats(context.Background()); err == nil {
				outboxPending.Set(float64(pending), "risk-engine")
				outboxAge.Set(age, "risk-engine")
			}
		}
	}()
	go journal.New(rdb, redisStore, pg).Run(context.Background())

	svc := service.New(redisStore)
	h := handler.New(svc)

	// 1. Initialize structured logging first (slog -> OTLP gRPC -> Loki, plus JSON stdout).
	logShutdown, err := telemetry.InitLogging(env("OTEL_SERVICE_NAME", "risk-engine"), env("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	if err != nil {
		slog.Warn("OTLP logging disabled", "err", err)
		logShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = logShutdown(context.Background()) }()

	// 2. Initialize OpenTelemetry tracing (OTLP -> collector -> Jaeger).
	tpShutdown, err := telemetry.Init(env("OTEL_SERVICE_NAME", "risk-engine"), env("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
		tpShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = tpShutdown(context.Background()) }()

	root := http.NewServeMux()
	root.Handle("GET /metrics", metrics.Default.Handler())
	root.Handle("/", telemetry.NewHandler(metricsMiddleware(h), "risk-engine"))

	addr := ":" + env("RISK_ENGINE_PORT", "9002")
	slog.Info("risk-engine listening", "addr", addr)
	if err := http.ListenAndServe(addr, root); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
