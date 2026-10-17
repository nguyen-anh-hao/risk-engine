// Package domain contains the risk engine's balance model. The documents are
// persisted in Redis, not in process memory.
package domain

// Balance is the shadow view of a single asset for an account:
// Total = Available + Frozen.
type Balance struct {
	Asset     string  `json:"asset"`
	Total     float64 `json:"total"`
	Available float64 `json:"available"`
	Frozen    float64 `json:"frozen"`
}

// JournalEvent is written to the risk:journal Redis Stream atomically with the
// balance mutation. A worker drains it into PostgreSQL + the Avro outbox.
type JournalEvent struct {
	EventID    string  `json:"event_id"`
	EventType  string  `json:"event_type"`
	AccountID  uint64  `json:"account_id"`
	Asset      string  `json:"asset"`
	OrderID    uint64  `json:"order_id,omitempty"`
	Amount     float64 `json:"amount,omitempty"`
	Reserved   float64 `json:"reserved,omitempty"`
	Actual     float64 `json:"actual,omitempty"`
	Fee        float64 `json:"fee,omitempty"`
	Qty        float64 `json:"qty,omitempty"`
	OccurredAt int64   `json:"occurred_at"`
}
