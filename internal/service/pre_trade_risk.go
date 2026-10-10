package service

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/exchange/trading-system/services/risk-engine/internal/domain"
	"github.com/exchange/trading-system/services/risk-engine/internal/storage"
)

// CheckRequest is the pre-trade risk check input. `Fee` is the worst-case fee
// the caller estimates for the order (price * quantity * taker rate); it is
// included in the buying-power check and in the amount frozen by Reserve so the
// account can never end up negative after settlement.
type CheckRequest struct {
	AccountID uint64 `json:"account_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Type      string `json:"type"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Fee       string `json:"fee"`
}

// CheckResponse is the pre-trade risk check result.
type CheckResponse struct {
	Approved     bool   `json:"approved"`
	RejectReason string `json:"reject_reason"`
	BuyingPower  string `json:"buying_power"`
}

// ApplyRequest applies a settled trade to shadow balances. `Reserved` is the
// exact cash amount that was frozen for this fill at order time (limit/reference
// price * quantity * (1 + taker rate)); Apply releases it and deducts the actual
// cost, returning any price-improvement back to the available balance.
type ApplyRequest struct {
	AccountID uint64 `json:"account_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Fee       string `json:"fee"`
	Reserved  string `json:"reserved"`
}

// RiskService owns the in-memory shadow balances and the pre-trade risk logic.
type RiskService struct {
	mu       sync.RWMutex
	accounts map[uint64]*domain.Account
}

func New() *RiskService {
	s := &RiskService{accounts: map[uint64]*domain.Account{}}
	for id, balances := range storage.InitialBalances() {
		acct := domain.NewAccount(id)
		for asset, amount := range balances {
			acct.Update(asset, func(b *domain.Balance) {
				b.Total = amount
				b.Available = amount
			})
		}
		s.accounts[id] = acct
	}
	return s
}

func (s *RiskService) account(id uint64) *domain.Account {
	s.mu.RLock()
	acct := s.accounts[id]
	s.mu.RUnlock()
	if acct == nil {
		s.mu.Lock()
		acct = domain.NewAccount(id)
		s.accounts[id] = acct
		s.mu.Unlock()
	}
	return acct
}

func available(b *domain.Balance) float64 {
	if b == nil {
		return 0
	}
	return b.Available
}

// Check validates margin/buying power without mutating state. BUY orders (both
// LIMIT and MARKET) require a positive reference price and sufficient cash for
// price*quantity + the worst-case fee; SELL orders require sufficient stock.
func (s *RiskService) Check(req CheckRequest) CheckResponse {
	price, priceOK := parseFloat(req.Price)
	qty, qtyOK := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)
	acct := s.account(req.AccountID)

	buyingPower := available(acct.Get("CASH"))

	if !qtyOK || qty <= 0 {
		return CheckResponse{Approved: false, RejectReason: "quantity must be positive", BuyingPower: ftos(buyingPower)}
	}

	if strings.EqualFold(req.Side, "BUY") {
		if !priceOK || price <= 0 {
			return CheckResponse{Approved: false, RejectReason: "buy order requires a valid price", BuyingPower: ftos(buyingPower)}
		}
		cost := price*qty + fee
		if buyingPower < cost {
			return CheckResponse{Approved: false, RejectReason: "insufficient buying power", BuyingPower: ftos(buyingPower)}
		}
	} else {
		asset := "STOCK:" + req.Symbol
		if available(acct.Get(asset)) < qty {
			return CheckResponse{Approved: false, RejectReason: "insufficient stock balance", BuyingPower: ftos(buyingPower)}
		}
	}
	return CheckResponse{Approved: true, BuyingPower: ftos(buyingPower)}
}

// Reserve freezes funds for an accepted order. BUY freezes cash for the
// worst-case cost (price*quantity + fee); SELL freezes the stock quantity.
func (s *RiskService) Reserve(req CheckRequest) error {
	price, _ := parseFloat(req.Price)
	qty, _ := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)
	acct := s.account(req.AccountID)

	if strings.EqualFold(req.Side, "BUY") {
		cost := price*qty + fee
		acct.Update("CASH", func(b *domain.Balance) { b.Available -= cost; b.Frozen += cost })
	} else {
		asset := "STOCK:" + req.Symbol
		acct.Update(asset, func(b *domain.Balance) { b.Available -= qty; b.Frozen += qty })
	}
	return nil
}

// Release unfreezes funds when an order is cancelled or rejected.
func (s *RiskService) Release(req CheckRequest) error {
	price, _ := parseFloat(req.Price)
	qty, _ := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)
	acct := s.account(req.AccountID)

	if strings.EqualFold(req.Side, "BUY") {
		cost := price*qty + fee
		acct.Update("CASH", func(b *domain.Balance) { b.Available += cost; b.Frozen -= cost })
	} else {
		asset := "STOCK:" + req.Symbol
		acct.Update(asset, func(b *domain.Balance) { b.Available += qty; b.Frozen -= qty })
	}
	return nil
}

// Apply settles a filled trade onto the shadow balances. For BUY it releases the
// exact `Reserved` amount frozen at order time and deducts the actual cost,
// returning any price/fee improvement to the available balance (this keeps
// Total == Available + Frozen). For SELL it debits the frozen stock and credits
// the net cash proceeds.
func (s *RiskService) Apply(req ApplyRequest) error {
	price, _ := parseFloat(req.Price)
	qty, _ := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)
	reserved, _ := parseFloat(req.Reserved)
	acct := s.account(req.AccountID)

	if strings.EqualFold(req.Side, "BUY") {
		actual := price * qty
		acct.Update("CASH", func(b *domain.Balance) {
			b.Total -= actual + fee
			b.Frozen -= reserved
			b.Available += reserved - actual - fee
		})
		asset := "STOCK:" + req.Symbol
		acct.Update(asset, func(b *domain.Balance) { b.Total += qty; b.Available += qty })
	} else {
		proceeds := price*qty - fee
		asset := "STOCK:" + req.Symbol
		acct.Update(asset, func(b *domain.Balance) { b.Total -= qty; b.Frozen -= qty })
		acct.Update("CASH", func(b *domain.Balance) { b.Total += proceeds; b.Available += proceeds })
	}
	return nil
}

// Deposit credits an asset into the account (faucet / treasury).
func (s *RiskService) Deposit(accountID uint64, asset string, amount float64) {
	acct := s.account(accountID)
	acct.Update(asset, func(b *domain.Balance) { b.Total += amount; b.Available += amount })
}

// Withdraw debits an asset if the available balance suffices.
func (s *RiskService) Withdraw(accountID uint64, asset string, amount float64) error {
	acct := s.account(accountID)
	if available(acct.Get(asset)) < amount {
		return errInsufficient
	}
	acct.Update(asset, func(b *domain.Balance) { b.Total -= amount; b.Available -= amount })
	return nil
}

// GetBalances returns a snapshot of all balances for an account.
func (s *RiskService) GetBalances(accountID uint64) []domain.Balance {
	return s.account(accountID).Snapshot()
}

func ftos(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// parseFloat parses a JSON money string, reporting whether it was valid.
func parseFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

var errInsufficient = errors.New("insufficient balance")
