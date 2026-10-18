package store

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
	"github.com/nguyen-anh-hao/risk-engine/internal/outbox"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Postgres is the async materialized view + audit/outbox for risk-engine.
// Redis remains the hot path; this store is fed by the Redis journal worker.
type Postgres struct {
	pool *pgxpool.Pool
	enc  *outbox.Encoder
}

func NewPostgres(ctx context.Context, dsn string, enc *outbox.Encoder) (*Postgres, error) {
	if dsn == "" {
		return nil, fmt.Errorf("RISK_DB_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	p := &Postgres{pool: pool, enc: enc}
	if err := p.runMigrations(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

func (p *Postgres) runMigrations(ctx context.Context) error {
	files, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		if !f.IsDir() {
			names = append(names, f.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		if _, err := p.pool.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

func (p *Postgres) Seed(ctx context.Context, initial map[uint64]map[string]float64) error {
	for accountID, assets := range initial {
		for asset, amount := range assets {
			_, err := p.pool.Exec(ctx, `
				INSERT INTO risk_balances(account_id, asset, total, available, frozen)
				VALUES ($1,$2,$3::numeric,$3::numeric,0)
				ON CONFLICT (account_id, asset) DO NOTHING`,
				accountID, asset, amount)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func money(v float64) string { return strconv.FormatFloat(v, 'f', 8, 64) }

func numValue(n pgtype.Numeric) float64 {
	if !n.Valid {
		return 0
	}
	f, _ := n.Float64Value()
	return f.Float64
}

func (p *Postgres) GetBalance(ctx context.Context, accountID uint64, asset string) (domain.Balance, error) {
	var total, available, frozen pgtype.Numeric
	err := p.pool.QueryRow(ctx, `
		SELECT total, available, frozen FROM risk_balances
		WHERE account_id=$1 AND asset=$2`, accountID, asset).
		Scan(&total, &available, &frozen)
	if err == pgx.ErrNoRows {
		return domain.Balance{Asset: asset}, nil
	}
	if err != nil {
		return domain.Balance{}, err
	}
	return domain.Balance{Asset: asset, Total: numValue(total), Available: numValue(available), Frozen: numValue(frozen)}, nil
}

func (p *Postgres) ListBalances(ctx context.Context, accountID uint64) ([]domain.Balance, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT asset, total, available, frozen FROM risk_balances
		WHERE account_id=$1`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Balance{}
	for rows.Next() {
		var b domain.Balance
		var total, available, frozen pgtype.Numeric
		if err := rows.Scan(&b.Asset, &total, &available, &frozen); err != nil {
			return nil, err
		}
		b.Total, b.Available, b.Frozen = numValue(total), numValue(available), numValue(frozen)
		out = append(out, b)
	}
	return out, rows.Err()
}

// AccountBalance pairs an account id with one balance document.
type AccountBalance struct {
	AccountID uint64
	Balance   domain.Balance
}

func (p *Postgres) AllBalances(ctx context.Context) ([]AccountBalance, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT account_id, asset, total, available, frozen FROM risk_balances`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AccountBalance{}
	for rows.Next() {
		var rec AccountBalance
		var total, available, frozen pgtype.Numeric
		if err := rows.Scan(&rec.AccountID, &rec.Balance.Asset, &total, &available, &frozen); err != nil {
			return nil, err
		}
		rec.Balance.Total, rec.Balance.Available, rec.Balance.Frozen =
			numValue(total), numValue(available), numValue(frozen)
		out = append(out, rec)
	}
	return out, rows.Err()
}

// ApplyJournal materializes one Redis journal event into PostgreSQL and its
// Avro outbox. Duplicate event ids are no-ops.
func (p *Postgres) ApplyJournal(ctx context.Context, ev domain.JournalEvent, balances []domain.Balance) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	raw, _ := json.Marshal(ev)
	tag, err := tx.Exec(ctx, `
		INSERT INTO risk_events(event_id, event_type, payload)
		VALUES ($1,$2,$3::jsonb) ON CONFLICT (event_id) DO NOTHING`,
		ev.EventID, ev.EventType, string(raw))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}

	version := ev.OccurredAt
	if version == 0 {
		version = time.Now().UnixNano()
	}
	for _, b := range balances {
		if _, err := tx.Exec(ctx, `
			INSERT INTO risk_balances(account_id, asset, total, available, frozen, version, updated_at)
			VALUES ($1,$2,$3::numeric,$4::numeric,$5::numeric,$6,now())
			ON CONFLICT (account_id, asset) DO UPDATE SET
				total=EXCLUDED.total, available=EXCLUDED.available, frozen=EXCLUDED.frozen,
				version=GREATEST(risk_balances.version, EXCLUDED.version), updated_at=now()`,
			ev.AccountID, b.Asset, money(b.Total), money(b.Available), money(b.Frozen), version); err != nil {
			return err
		}

		payload, err := p.enc.Encode(outbox.RiskEvent{
			EventID: uuid.NewString(), EventType: ev.EventType,
			AccountID: int64(ev.AccountID), Asset: b.Asset,
			Total: money(b.Total), Available: money(b.Available), Frozen: money(b.Frozen),
			OrderID: int64(ev.OrderID), Amount: money(ev.Amount),
			OccurredAt: version, Version: version,
		})
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO outbox(aggregate_type, aggregate_id, event_type, payload)
			VALUES ('risk', $1, $2, $3)`,
			strconv.FormatUint(ev.AccountID, 10), ev.EventType, payload); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// OutboxStats exposes pending outbox backlog for monitoring.
func (p *Postgres) OutboxStats(ctx context.Context) (pending int64, oldestAgeSecs float64, err error) {
	err = p.pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(extract(epoch from now() - min(created_at)), 0)
		FROM outbox WHERE status='PENDING'`).Scan(&pending, &oldestAgeSecs)
	return
}
