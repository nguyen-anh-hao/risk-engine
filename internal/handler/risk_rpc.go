// Package handler exposes the risk engine over HTTP/JSON.
package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/nguyen-anh-hao/risk-engine/internal/domain"
	"github.com/nguyen-anh-hao/risk-engine/internal/service"
	"github.com/nguyen-anh-hao/risk-engine/internal/store"
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
	resp, err := h.svc.Check(r.Context(), req)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "balance store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) reserve(w http.ResponseWriter, r *http.Request) {
	var req service.CheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request")
		return
	}
	if err := h.svc.Reserve(r.Context(), req); err != nil {
		if errors.Is(err, store.ErrInsufficient) {
			writeErr(w, http.StatusConflict, "insufficient balance")
			return
		}
		writeErr(w, http.StatusServiceUnavailable, "balance store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) release(w http.ResponseWriter, r *http.Request) {
	var req service.CheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request")
		return
	}
	if err := h.svc.Release(r.Context(), req); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "balance store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) apply(w http.ResponseWriter, r *http.Request) {
	var req service.ApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request")
		return
	}
	if err := h.svc.Apply(r.Context(), req); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "balance store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) deposit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AccountID uint64 `json:"account_id"`
		Asset     string `json:"asset"`
		Amount    string `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request")
		return
	}
	amount, _ := strconv.ParseFloat(req.Amount, 64)
	if err := h.svc.Deposit(r.Context(), req.AccountID, req.Asset, amount); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "balance store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) withdraw(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AccountID uint64 `json:"account_id"`
		Asset     string `json:"asset"`
		Amount    string `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request")
		return
	}
	amount, _ := strconv.ParseFloat(req.Amount, 64)
	if err := h.svc.Withdraw(r.Context(), req.AccountID, req.Asset, amount); err != nil {
		if errors.Is(err, store.ErrInsufficient) {
			writeJSON(w, http.StatusOK, map[string]interface{}{"success": false, "reason": err.Error()})
			return
		}
		writeErr(w, http.StatusServiceUnavailable, "balance store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) balances(w http.ResponseWriter, r *http.Request) {
	accountID, _ := strconv.ParseUint(r.URL.Query().Get("account_id"), 10, 64)
	bs, err := h.svc.GetBalances(r.Context(), accountID)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "balance store unavailable")
		return
	}
	out := make([]balanceOut, 0, len(bs))
	for _, b := range bs {
		out = append(out, toBalanceOut(b))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
