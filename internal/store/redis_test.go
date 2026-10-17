package store

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisReserveApplyAreAtomicAndShared(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	st := NewRedis(rdb)
	ctx := context.Background()

	if err := st.Seed(ctx, map[uint64]map[string]float64{
		1: {"CASH": 100, "STOCK:AAPL": 10},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Reserve(ctx, 1, "CASH", 40); err != nil {
		t.Fatal(err)
	}
	cash, _ := st.Get(ctx, 1, "CASH")
	if cash.Available != 60 || cash.Frozen != 40 {
		t.Fatalf("after reserve cash = %+v", cash)
	}
	if err := st.Reserve(ctx, 1, "CASH", 61); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("over-reserve err = %v", err)
	}
	if err := st.ApplyBuy(ctx, 1, "STOCK:AAPL", 40, 30, 0.03, 3); err != nil {
		t.Fatal(err)
	}
	cash, _ = st.Get(ctx, 1, "CASH")
	if cash.Total != 69.97 || cash.Available != 69.97 || cash.Frozen != 0 {
		t.Fatalf("after apply cash = %+v", cash)
	}
	stock, _ := st.Get(ctx, 1, "STOCK:AAPL")
	if stock.Total != 13 || stock.Available != 13 {
		t.Fatalf("after apply stock = %+v", stock)
	}
	if err := st.Withdraw(ctx, 1, "CASH", 100); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("over-withdraw err = %v", err)
	}
}
