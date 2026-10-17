// Package store owns risk-engine balances. The Redis implementation is the
// production one so every risk-engine replica sees the same balances and
// reservations; the memory implementation exists for tests only.
package store

import (
	"context"
	"errors"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
)

// ErrInsufficient is returned when an atomic reserve/withdraw would take an
// available balance below zero.
var ErrInsufficient = errors.New("insufficient balance")

// Store is the shared balance repository.
type Store interface {
	Seed(ctx context.Context, initial map[uint64]map[string]float64) error
	Get(ctx context.Context, accountID uint64, asset string) (domain.Balance, error)
	List(ctx context.Context, accountID uint64) ([]domain.Balance, error)
	Reserve(ctx context.Context, accountID uint64, asset string, amount float64) error
	Release(ctx context.Context, accountID uint64, asset string, amount float64) error
	ApplyBuy(ctx context.Context, accountID uint64, stockAsset string, reserved, actual, fee, qty float64) error
	ApplySell(ctx context.Context, accountID uint64, stockAsset string, qty, proceeds float64) error
	Deposit(ctx context.Context, accountID uint64, asset string, amount float64) error
	Withdraw(ctx context.Context, accountID uint64, asset string, amount float64) error
	Close() error
}
