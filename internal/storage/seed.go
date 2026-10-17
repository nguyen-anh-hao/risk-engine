// Package storage contains the deterministic initial balance seed.
// Balances themselves live in Redis; this file only bootstraps demo accounts.
package storage

// InitialBalances maps accountID -> asset -> amount.
func InitialBalances() map[uint64]map[string]float64 {
	return map[uint64]map[string]float64{
		1: {"CASH": 1000000, "STOCK:AAPL": 1000, "STOCK:MSFT": 500},
		2: {"CASH": 1000000, "STOCK:AAPL": 500, "STOCK:TSLA": 300},
	}
}
