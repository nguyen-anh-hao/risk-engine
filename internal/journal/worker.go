// Package journal drains the risk:journal Redis Stream into PostgreSQL and the
// risk outbox. Redis remains the hot path; PostgreSQL is the materialized audit.
package journal

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
	"github.com/nguyen-anh-hao/risk-engine/internal/metrics"
	"github.com/nguyen-anh-hao/risk-engine/internal/store"
)

var journalApplyErrors = metrics.Default.Counter(
	"trading_risk_journal_apply_errors_total", "Risk journal DB/outbox apply failures.")

const (
	Stream = "risk:journal"
	Group  = "risk-journal"
)

type Worker struct {
	rdb      *redis.Client
	redis    *store.RedisStore
	pg       *store.Postgres
	consumer string
}

func New(rdb *redis.Client, redisStore *store.RedisStore, pg *store.Postgres) *Worker {
	host, _ := os.Hostname()
	return &Worker{rdb: rdb, redis: redisStore, pg: pg, consumer: host}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		if err := w.ensureGroup(ctx); err != nil {
			slog.Warn("risk journal group unavailable", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		w.claimStale(ctx)
		streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: Group, Consumer: w.consumer,
			Streams: []string{Stream, ">"}, Count: 20, Block: 2 * time.Second,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("risk journal read failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				w.handle(ctx, msg)
			}
		}
	}
}

func (w *Worker) ensureGroup(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, Stream, Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// claimStale takes over entries left pending by a previous consumer (for
// example after a container restart). Without this, those entries never reach
// the new consumer because XReadGroup only reads ">".
func (w *Worker) claimStale(ctx context.Context) {
	pending, err := w.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: Stream, Group: Group, Start: "-", End: "+", Count: 50,
	}).Result()
	if err != nil || len(pending) == 0 {
		return
	}
	ids := make([]string, 0, len(pending))
	for _, entry := range pending {
		ids = append(ids, entry.ID)
	}
	messages, err := w.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream: Stream, Group: Group, Consumer: w.consumer,
		MinIdle: 30 * time.Second, Messages: ids,
	}).Result()
	if err != nil {
		slog.Warn("risk journal stale claim failed", "err", err)
		return
	}
	for _, msg := range messages {
		w.handle(ctx, msg)
	}
}

func (w *Worker) handle(ctx context.Context, msg redis.XMessage) {
	raw, _ := msg.Values["payload"].(string)
	var ev domain.JournalEvent
	if err := json.Unmarshal([]byte(raw), &ev); err != nil {
		slog.Warn("risk journal bad payload", "id", msg.ID, "err", err)
		_ = w.rdb.XAck(ctx, Stream, Group, msg.ID).Err()
		return
	}
	balances, err := w.redis.Snapshot(ctx, ev.AccountID)
	if err != nil {
		slog.Warn("risk journal snapshot failed", "id", msg.ID, "err", err)
		return
	}
	for {
		if err := w.pg.ApplyJournal(ctx, ev, balances); err == nil {
			_ = w.rdb.XAck(ctx, Stream, Group, msg.ID).Err()
			return
		} else {
			journalApplyErrors.Inc()
			slog.Warn("risk journal DB apply failed, retrying", "id", msg.ID, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
