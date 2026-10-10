// Package domain contains the risk engine's in-memory shadow-balance model.
package domain

// Balance is the shadow view of a single asset for an account:
// Total = Available + Frozen.
type Balance struct {
	Asset     string  `json:"asset"`
	Total     float64 `json:"total"`
	Available float64 `json:"available"`
	Frozen    float64 `json:"frozen"`
}
