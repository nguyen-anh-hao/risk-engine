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
func TestStreamIDMillis(t *testing.T) {
	tests := []struct {
		id   string
		want int64
		ok   bool
	}{
		{"1792428815540-0", 1792428815540, true},
		{"1792428815540", 1792428815540, true},
		{"0-1", 0, true},
		{"not-a-stream-id", 0, false},
		{"", 0, false},
	}
	for _, tc := range tests {
		got, ok := streamIDMillis(tc.id)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("streamIDMillis(%q) = (%d, %v), want (%d, %v)", tc.id, got, ok, tc.want, tc.ok)
		}
	}
}

