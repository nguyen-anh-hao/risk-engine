// Package handler exposes the risk engine over HTTP/JSON.
package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
	"github.com/nguyen-anh-hao/risk-engine/internal/service"
)

type Handler struct {
	svc *service.RiskService
}

// New wires the HTTP routes for the risk engine.
func New(svc *service.RiskService) http.Handler {
	h := &Handler{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /check", h.check)
	mux.HandleFunc("POST /reserve", h.reserve)
	mux.HandleFunc("POST /release", h.release)
	mux.HandleFunc("POST /apply", h.apply)
	mux.HandleFunc("POST /deposit", h.deposit)
	mux.HandleFunc("POST /withdraw", h.withdraw)
	mux.HandleFunc("GET /balances", h.balances)
	mux.HandleFunc("GET /health", h.health)
	return mux
}

type balanceOut struct {
	Asset     string `json:"asset"`
	Total     string `json:"total"`
	Available string `json:"available"`
	Frozen    string `json:"frozen"`
}

func toBalanceOut(b domain.Balance) balanceOut {
	return balanceOut{
		Asset:     b.Asset,
		Total:     strconv.FormatFloat(b.Total, 'f', -1, 64),
		Available: strconv.FormatFloat(b.Available, 'f', -1, 64),
		Frozen:    strconv.FormatFloat(b.Frozen, 'f', -1, 64),
	}
}

func (h *Handler) check(w http.ResponseWriter, r *http.Request) {
	var req service.CheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	writeJSON(w, h.svc.Check(req))
}

func (h *Handler) reserve(w http.ResponseWriter, r *http.Request) {
	var req service.CheckRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	_ = h.svc.Reserve(req)
	writeJSON(w, map[string]bool{"success": true})
}

func (h *Handler) release(w http.ResponseWriter, r *http.Request) {
	var req service.CheckRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	_ = h.svc.Release(req)
	writeJSON(w, map[string]bool{"success": true})
}

func (h *Handler) apply(w http.ResponseWriter, r *http.Request) {
	var req service.ApplyRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	_ = h.svc.Apply(req)
	writeJSON(w, map[string]bool{"success": true})
}

func (h *Handler) deposit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AccountID uint64 `json:"account_id"`
		Asset     string `json:"asset"`
		Amount    string `json:"amount"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	amount, _ := strconv.ParseFloat(req.Amount, 64)
	h.svc.Deposit(req.AccountID, req.Asset, amount)
	writeJSON(w, map[string]bool{"success": true})
}

func (h *Handler) withdraw(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AccountID uint64 `json:"account_id"`
		Asset     string `json:"asset"`
		Amount    string `json:"amount"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	amount, _ := strconv.ParseFloat(req.Amount, 64)
	if err := h.svc.Withdraw(req.AccountID, req.Asset, amount); err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "reason": err.Error()})
		return
	}
	writeJSON(w, map[string]bool{"success": true})
}

func (h *Handler) balances(w http.ResponseWriter, r *http.Request) {
	accountID, _ := strconv.ParseUint(r.URL.Query().Get("account_id"), 10, 64)
	bs := h.svc.GetBalances(accountID)
	out := make([]balanceOut, 0, len(bs))
	for _, b := range bs {
		out = append(out, toBalanceOut(b))
	}
	writeJSON(w, out)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
