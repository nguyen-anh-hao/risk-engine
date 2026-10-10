// Package storage contains the PostgreSQL sync/fallback for the risk engine.
// In production the risk engine loads its initial shadow-balance snapshot from
// PostgreSQL and periodically reconciles; here it returns deterministic mock
// data so the service can run standalone.
package storage

// InitialBalances maps accountID -> asset -> amount.
func InitialBalances() map[uint64]map[string]float64 {
	return map[uint64]map[string]float64{
		1: {"CASH": 1000000, "STOCK:AAPL": 1000, "STOCK:MSFT": 500},
		2: {"CASH": 1000000, "STOCK:AAPL": 500, "STOCK:TSLA": 300},
	}
}
