package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/exchange/trading-system/services/risk-engine/internal/handler"
	"github.com/exchange/trading-system/services/risk-engine/internal/metrics"
	"github.com/exchange/trading-system/services/risk-engine/internal/service"
	"github.com/exchange/trading-system/services/risk-engine/internal/telemetry"
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

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		requests.Inc(r.Method, r.URL.Path, strconv.Itoa(rec.status))
		duration.Observe(time.Since(start).Seconds(), r.Method, r.URL.Path)
	})
}

func main() {
	svc := service.New()
	h := handler.New(svc)

	// Initialize OpenTelemetry tracing (OTLP -> collector -> Jaeger).
	tpShutdown, err := telemetry.Init(env("OTEL_SERVICE_NAME", "risk-engine"), env("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	if err != nil {
		log.Printf("[risk-engine] tracing disabled: %v", err)
		tpShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = tpShutdown(context.Background()) }()

	root := http.NewServeMux()
	root.Handle("GET /metrics", metrics.Default.Handler())
	root.Handle("/", telemetry.NewHandler(metricsMiddleware(h), "risk-engine"))

	addr := ":" + env("RISK_ENGINE_PORT", "9002")
	log.Printf("[risk-engine] listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, root))
}
