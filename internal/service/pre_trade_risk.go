package service

import (
	"context"
	"strconv"
	"strings"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
	"github.com/nguyen-anh-hao/risk-engine/internal/store"
)

// CheckRequest is the pre-trade risk check input. `Fee` is the worst-case fee
// the caller estimates for the order (price * quantity * taker rate).
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

// ApplyRequest applies a settled trade to shadow balances.
type ApplyRequest struct {
	AccountID uint64 `json:"account_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Fee       string `json:"fee"`
	Reserved  string `json:"reserved"`
}

// RiskService is stateless: all balances/reservations live in the shared store.
type RiskService struct {
	st store.Store
}

func New(st store.Store) *RiskService {
	return &RiskService{st: st}
}

func (s *RiskService) Check(ctx context.Context, req CheckRequest) (CheckResponse, error) {
	price, priceOK := parseFloat(req.Price)
	qty, qtyOK := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)

	cash, err := s.st.Get(ctx, req.AccountID, "CASH")
	if err != nil {
		return CheckResponse{}, err
	}
	buyingPower := cash.Available

	if !qtyOK || qty <= 0 {
		return CheckResponse{Approved: false, RejectReason: "quantity must be positive", BuyingPower: ftos(buyingPower)}, nil
	}

	if strings.EqualFold(req.Side, "BUY") {
		if !priceOK || price <= 0 {
			return CheckResponse{Approved: false, RejectReason: "buy order requires a valid price", BuyingPower: ftos(buyingPower)}, nil
		}
		if buyingPower < price*qty+fee {
			return CheckResponse{Approved: false, RejectReason: "insufficient buying power", BuyingPower: ftos(buyingPower)}, nil
		}
		return CheckResponse{Approved: true, BuyingPower: ftos(buyingPower)}, nil
	}

	stock, err := s.st.Get(ctx, req.AccountID, "STOCK:"+req.Symbol)
	if err != nil {
		return CheckResponse{}, err
	}
	if stock.Available < qty {
		return CheckResponse{Approved: false, RejectReason: "insufficient stock position", BuyingPower: ftos(buyingPower)}, nil
	}
	return CheckResponse{Approved: true, BuyingPower: ftos(buyingPower)}, nil
}

// Reserve atomically freezes worst-case funds. The store rejects an
// insufficient reserve, so concurrent orders cannot overdraw an account.
func (s *RiskService) Reserve(ctx context.Context, req CheckRequest) error {
	price, _ := parseFloat(req.Price)
	qty, _ := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)
	if strings.EqualFold(req.Side, "BUY") {
		return s.st.Reserve(ctx, req.AccountID, "CASH", price*qty+fee)
	}
	return s.st.Reserve(ctx, req.AccountID, "STOCK:"+req.Symbol, qty)
}

// Release unfreezes funds when an order is cancelled or rejected.
func (s *RiskService) Release(ctx context.Context, req CheckRequest) error {
	price, _ := parseFloat(req.Price)
	qty, _ := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)
	if strings.EqualFold(req.Side, "BUY") {
		return s.st.Release(ctx, req.AccountID, "CASH", price*qty+fee)
	}
	return s.st.Release(ctx, req.AccountID, "STOCK:"+req.Symbol, qty)
}

// Apply settles a filled trade atomically on both cash and stock balances.
func (s *RiskService) Apply(ctx context.Context, req ApplyRequest) error {
	price, _ := parseFloat(req.Price)
	qty, _ := parseFloat(req.Quantity)
	fee, _ := parseFloat(req.Fee)
	reserved, _ := parseFloat(req.Reserved)
	stockAsset := "STOCK:" + req.Symbol

	if strings.EqualFold(req.Side, "BUY") {
		return s.st.ApplyBuy(ctx, req.AccountID, stockAsset, reserved, price*qty, fee, qty)
	}
	return s.st.ApplySell(ctx, req.AccountID, stockAsset, qty, price*qty-fee)
}

func (s *RiskService) Deposit(ctx context.Context, accountID uint64, asset string, amount float64) error {
	return s.st.Deposit(ctx, accountID, asset, amount)
}

func (s *RiskService) Withdraw(ctx context.Context, accountID uint64, asset string, amount float64) error {
	return s.st.Withdraw(ctx, accountID, asset, amount)
}

func (s *RiskService) GetBalances(ctx context.Context, accountID uint64) ([]domain.Balance, error) {
	return s.st.List(ctx, accountID)
}

func ftos(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

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
