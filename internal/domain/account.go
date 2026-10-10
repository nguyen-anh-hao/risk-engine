package domain

import "sync"

// Account holds the shadow balances of a single account. Balances are guarded
// by a per-account mutex so the pre-trade risk path can run concurrently across
// accounts without a global lock.
type Account struct {
	ID       uint64
	mu       sync.RWMutex
	balances map[string]*Balance
}

func NewAccount(id uint64) *Account {
	return &Account{ID: id, balances: map[string]*Balance{}}
}

// Get returns the balance for an asset, or nil if the account never held it.
func (a *Account) Get(asset string) *Balance {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.balances[asset]
}

// Ensure returns (creating if necessary) the balance for an asset.
func (a *Account) Ensure(asset string) *Balance {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.balances[asset]
	if !ok {
		b = &Balance{Asset: asset}
		a.balances[asset] = b
	}
	return b
}

// Update mutates a balance under lock, returning the new value.
func (a *Account) Update(asset string, fn func(*Balance)) *Balance {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.balances[asset]
	if !ok {
		b = &Balance{Asset: asset}
		a.balances[asset] = b
	}
	fn(b)
	return b
}

// Snapshot returns a copy of all balances (safe to serialize).
func (a *Account) Snapshot() []Balance {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]Balance, 0, len(a.balances))
	for _, b := range a.balances {
		out = append(out, *b)
	}
	return out
}
