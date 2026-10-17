package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
)

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
return 1
`)

var releaseScript = redis.NewScript(luaHelpers + `
local b = getb(KEYS[1], ARGV[1])
local amt = tonumber(ARGV[2])
b.available = b.available + amt
b.frozen = b.frozen - amt
if b.frozen < 0 then b.frozen = 0 end
putb(KEYS[1], ARGV[1], b)
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
return 1
`)

var depositScript = redis.NewScript(luaHelpers + `
local b = getb(KEYS[1], ARGV[1])
local amt = tonumber(ARGV[2])
b.total = b.total + amt
b.available = b.available + amt
putb(KEYS[1], ARGV[1], b)
return 1
`)

var withdrawScript = redis.NewScript(luaHelpers + `
local b = getb(KEYS[1], ARGV[1])
local amt = tonumber(ARGV[2])
if b.available < amt then return 0 end
b.total = b.total - amt
b.available = b.available - amt
putb(KEYS[1], ARGV[1], b)
return 1
`)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *RedisStore { return &RedisStore{rdb: rdb} }

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

func (s *RedisStore) Reserve(ctx context.Context, accountID uint64, asset string, amount float64) error {
	ok, err := reserveScript.Run(ctx, s.rdb, []string{accountKey(accountID)}, asset, amount).Int()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrInsufficient
	}
	return nil
}

func (s *RedisStore) Release(ctx context.Context, accountID uint64, asset string, amount float64) error {
	return releaseScript.Run(ctx, s.rdb, []string{accountKey(accountID)}, asset, amount).Err()
}

func (s *RedisStore) ApplyBuy(ctx context.Context, accountID uint64, stockAsset string, reserved, actual, fee, qty float64) error {
	return applyBuyScript.Run(ctx, s.rdb, []string{accountKey(accountID)},
		reserved, actual, fee, qty, stockAsset).Err()
}

func (s *RedisStore) ApplySell(ctx context.Context, accountID uint64, stockAsset string, qty, proceeds float64) error {
	return applySellScript.Run(ctx, s.rdb, []string{accountKey(accountID)}, qty, proceeds, stockAsset).Err()
}

func (s *RedisStore) Deposit(ctx context.Context, accountID uint64, asset string, amount float64) error {
	return depositScript.Run(ctx, s.rdb, []string{accountKey(accountID)}, asset, amount).Err()
}

func (s *RedisStore) Withdraw(ctx context.Context, accountID uint64, asset string, amount float64) error {
	ok, err := withdrawScript.Run(ctx, s.rdb, []string{accountKey(accountID)}, asset, amount).Int()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrInsufficient
	}
	return nil
}

func (s *RedisStore) Close() error { return s.rdb.Close() }
