package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
)

func journal(accountID uint64, eventType, asset string, amount, reserved, actual, fee, qty float64) string {
	b, _ := json.Marshal(domain.JournalEvent{
		EventID: uuid.NewString(), EventType: eventType, AccountID: accountID,
		Asset: asset, Amount: amount, Reserved: reserved, Actual: actual,
		Fee: fee, Qty: qty, OccurredAt: time.Now().UnixNano(),
	})
	return string(b)
}

func accountKey(accountID uint64) string {
	return fmt.Sprintf("risk:acct:%d", accountID)
}

// Lua scripts keep every balance mutation atomic across replicas. Balances are
// stored as a JSON document per asset in a Redis hash.
const luaHelpers = `
local function getb(key, field)
  local v = redis.call('HGET', key, field)
  if not v then return {asset=field, total=0, available=0, frozen=0} end
  return cjson.decode(v)
end
local function putb(key, field, b)
  redis.call('HSET', key, field, cjson.encode(b))
end
`

var reserveScript = redis.NewScript(luaHelpers + `
local b = getb(KEYS[1], ARGV[1])
local amt = tonumber(ARGV[2])
if b.available < amt then return 0 end
b.available = b.available - amt
b.frozen = b.frozen + amt
putb(KEYS[1], ARGV[1], b)
redis.call('XADD', 'risk:journal', 'MAXLEN', '~', '100000', '*', 'payload', ARGV[3])
return 1
`)

var releaseScript = redis.NewScript(luaHelpers + `
local b = getb(KEYS[1], ARGV[1])
local amt = tonumber(ARGV[2])
b.available = b.available + amt
b.frozen = b.frozen - amt
if b.frozen < 0 then b.frozen = 0 end
putb(KEYS[1], ARGV[1], b)
redis.call('XADD', 'risk:journal', 'MAXLEN', '~', '100000', '*', 'payload', ARGV[3])
return 1
`)

var applyBuyScript = redis.NewScript(luaHelpers + `
local reserved = tonumber(ARGV[1])
local actual = tonumber(ARGV[2])
local fee = tonumber(ARGV[3])
local qty = tonumber(ARGV[4])
local cash = getb(KEYS[1], 'CASH')
cash.total = cash.total - actual - fee
cash.frozen = cash.frozen - reserved
cash.available = cash.available + reserved - actual - fee
putb(KEYS[1], 'CASH', cash)
local stock = getb(KEYS[1], ARGV[5])
stock.total = stock.total + qty
stock.available = stock.available + qty
putb(KEYS[1], ARGV[5], stock)
redis.call('XADD', 'risk:journal', 'MAXLEN', '~', '100000', '*', 'payload', ARGV[6])
return 1
`)

var applySellScript = redis.NewScript(luaHelpers + `
local qty = tonumber(ARGV[1])
local proceeds = tonumber(ARGV[2])
local stock = getb(KEYS[1], ARGV[3])
stock.total = stock.total - qty
stock.frozen = stock.frozen - qty
putb(KEYS[1], ARGV[3], stock)
local cash = getb(KEYS[1], 'CASH')
cash.total = cash.total + proceeds
cash.available = cash.available + proceeds
putb(KEYS[1], 'CASH', cash)
redis.call('XADD', 'risk:journal', 'MAXLEN', '~', '100000', '*', 'payload', ARGV[4])
return 1
`)

var depositScript = redis.NewScript(luaHelpers + `
local b = getb(KEYS[1], ARGV[1])
local amt = tonumber(ARGV[2])
b.total = b.total + amt
b.available = b.available + amt
putb(KEYS[1], ARGV[1], b)
redis.call('XADD', 'risk:journal', 'MAXLEN', '~', '100000', '*', 'payload', ARGV[3])
return 1
`)

var withdrawScript = redis.NewScript(luaHelpers + `
local b = getb(KEYS[1], ARGV[1])
local amt = tonumber(ARGV[2])
if b.available < amt then return 0 end
b.total = b.total - amt
b.available = b.available - amt
putb(KEYS[1], ARGV[1], b)
redis.call('XADD', 'risk:journal', 'MAXLEN', '~', '100000', '*', 'payload', ARGV[3])
return 1
`)

type RedisStore struct {
	rdb      *redis.Client
	fallback *Postgres
}

func NewRedis(rdb *redis.Client) *RedisStore { return &RedisStore{rdb: rdb} }

// SetFallback enables PostgreSQL reads when a Redis projection key is missing.
func (s *RedisStore) SetFallback(pg *Postgres) { s.fallback = pg }

// RebuildMissing hydrates Redis keys that were lost without overwriting live
// balances. It is safe to run on every replica at startup.
func (s *RedisStore) RebuildMissing(ctx context.Context, pg *Postgres) error {
	rows, err := pg.AllBalances(ctx)
	if err != nil {
		return err
	}
	for _, rec := range rows {
		raw, err := json.Marshal(rec.Balance)
		if err != nil {
			return err
		}
		if err := s.rdb.HSetNX(ctx, accountKey(rec.AccountID), rec.Balance.Asset, raw).Err(); err != nil {
			return err
		}
	}
	return nil
}

// Seed inserts missing initial balances without overwriting live state.
func (s *RedisStore) Seed(ctx context.Context, initial map[uint64]map[string]float64) error {
	for accountID, balances := range initial {
		key := accountKey(accountID)
		for asset, amount := range balances {
			b := domain.Balance{Asset: asset, Total: amount, Available: amount}
			raw, err := json.Marshal(b)
			if err != nil {
				return err
			}
			if err := s.rdb.HSetNX(ctx, key, asset, raw).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RedisStore) Get(ctx context.Context, accountID uint64, asset string) (domain.Balance, error) {
	raw, err := s.rdb.HGet(ctx, accountKey(accountID), asset).Bytes()
	if err == redis.Nil {
		if s.fallback != nil {
			return s.fallback.GetBalance(ctx, accountID, asset)
		}
		return domain.Balance{Asset: asset}, nil
	}
	if err != nil {
		return domain.Balance{}, err
	}
	var b domain.Balance
	if err := json.Unmarshal(raw, &b); err != nil {
		return domain.Balance{}, err
	}
	return b, nil
}

func (s *RedisStore) List(ctx context.Context, accountID uint64) ([]domain.Balance, error) {
	items, err := s.rdb.HGetAll(ctx, accountKey(accountID)).Result()
	if err != nil && s.fallback != nil {
		return s.fallback.ListBalances(ctx, accountID)
	}
	if err != nil {
		return nil, err
	}
	if len(items) == 0 && s.fallback != nil {
		return s.fallback.ListBalances(ctx, accountID)
	}
	out := make([]domain.Balance, 0, len(items))
	for _, raw := range items {
		var b domain.Balance
		if json.Unmarshal([]byte(raw), &b) == nil {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *RedisStore) Reserve(ctx context.Context, accountID uint64, asset string, amount float64) error {
	ok, err := reserveScript.Run(ctx, s.rdb, []string{accountKey(accountID)},
		asset, amount, journal(accountID, "BalanceReserved", asset, amount, 0, 0, 0, 0)).Int()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrInsufficient
	}
	return nil
}

func (s *RedisStore) Release(ctx context.Context, accountID uint64, asset string, amount float64) error {
	return releaseScript.Run(ctx, s.rdb, []string{accountKey(accountID)},
		asset, amount, journal(accountID, "BalanceReleased", asset, amount, 0, 0, 0, 0)).Err()
}

func (s *RedisStore) ApplyBuy(ctx context.Context, accountID uint64, stockAsset string, reserved, actual, fee, qty float64) error {
	return applyBuyScript.Run(ctx, s.rdb, []string{accountKey(accountID)},
		reserved, actual, fee, qty, stockAsset,
		journal(accountID, "BalanceAppliedBuy", stockAsset, 0, reserved, actual, fee, qty)).Err()
}

func (s *RedisStore) ApplySell(ctx context.Context, accountID uint64, stockAsset string, qty, proceeds float64) error {
	return applySellScript.Run(ctx, s.rdb, []string{accountKey(accountID)},
		qty, proceeds, stockAsset,
		journal(accountID, "BalanceAppliedSell", stockAsset, proceeds, 0, 0, 0, qty)).Err()
}

func (s *RedisStore) Deposit(ctx context.Context, accountID uint64, asset string, amount float64) error {
	return depositScript.Run(ctx, s.rdb, []string{accountKey(accountID)},
		asset, amount, journal(accountID, "BalanceDeposited", asset, amount, 0, 0, 0, 0)).Err()
}

func (s *RedisStore) Withdraw(ctx context.Context, accountID uint64, asset string, amount float64) error {
	ok, err := withdrawScript.Run(ctx, s.rdb, []string{accountKey(accountID)},
		asset, amount, journal(accountID, "BalanceWithdrawn", asset, amount, 0, 0, 0, 0)).Int()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrInsufficient
	}
	return nil
}

// JournalStats exposes Redis Stream backlog/lag for monitoring.
//
// Lag is the age of the oldest unacknowledged entry, not the time since the
// last event was produced. The previous implementation used the stream's
// last-generated id, which grew without bound whenever the engine was idle and
// said nothing about the DB materialization being behind.
func (s *RedisStore) JournalStats(ctx context.Context) (length, pending int64, lagSeconds float64, err error) {
	length, err = s.rdb.XLen(ctx, "risk:journal").Result()
	if err != nil {
		return
	}
	pend, perr := s.rdb.XPending(ctx, "risk:journal", "risk-journal").Result()
	if perr == nil && pend != nil {
		pending = pend.Count
	}
	if pending == 0 {
		return
	}
	oldest, oerr := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "risk:journal",
		Group:  "risk-journal",
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()
	if oerr != nil || len(oldest) == 0 {
		return
	}
	if ms, ok := streamIDMillis(oldest[0].ID); ok {
		lagSeconds = time.Since(time.UnixMilli(ms)).Seconds()
		if lagSeconds < 0 {
			lagSeconds = 0
		}
	}
	return
}

// streamIDMillis extracts the millisecond timestamp from a Redis Stream id
// such as 1792428815540-0.
func streamIDMillis(id string) (int64, bool) {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) == 0 {
		return 0, false
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	return ms, err == nil
}

// Snapshot returns all balances for the journal worker's DB materialization.
func (s *RedisStore) Snapshot(ctx context.Context, accountID uint64) ([]domain.Balance, error) {
	items, err := s.rdb.HGetAll(ctx, accountKey(accountID)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.Balance, 0, len(items))
	for _, raw := range items {
		var b domain.Balance
		if json.Unmarshal([]byte(raw), &b) == nil {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *RedisStore) Close() error { return s.rdb.Close() }
