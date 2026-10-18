# risk-engine

## Purpose

Guards the exchange against insolvency by checking and freezing buying power before an order is allowed to match. It owns the live balance view used by the trading path, not the authoritative ledger. It does not price portfolios, compute scenario margin, or book settled money.

## Domain knowledge

Every asset has an available amount and a reserved amount, and only the available part can be spent. A buy checks that available cash covers price times quantity plus the estimated fee, while a sell checks that available stock covers the quantity. A reservation freezes the worst case, an application consumes the reservation and moves the assets, and a release unfreezes what was never used.

Buy application consumes the exact amount that was frozen rather than the fill price alone, so a price improvement returns the difference to the buyer. Sell application removes stock and credits cash net of the seller fee. Every balance mutation is atomic, which is what prevents two concurrent orders from overdrawing the same account.

The live view is a Redis projection backed by a journal; PostgreSQL holds materialized snapshots and the audit trail. A reservation belongs to one order and must be consumed or released exactly once. The service charges no margin of its own and trusts the fee estimate supplied with the order.

## Decisions and consequences

**Redis hot path with Lua atomicity.** Check, reserve, apply and release run as single Redis scripts, so concurrent requests cannot interleave. The cost is a hard dependency on Redis and a network hop on the critical path.

**Write-behind journal to PostgreSQL.** Each mutation appends to a Redis stream that a worker drains into balance snapshots and an outbox. This keeps the hot path fast, at the price of materialized-view lag and a separate rebuild path.

**Transactional outbox with CDC.** Risk events are written with the balance change and relayed to Kafka, so downstream consumers never see a balance change without its event. The cost is an outbox table and a CDC dependency.

**Stateless service.** No business state lives in process memory, so any replica can serve any request and restarts are safe. The consequence is that Redis durability, not the process, defines the recovery point.

**Linear cash and stock checks.** The current model is deliberately simple and predictable. Portfolio margin, value at risk and haircuts are not modeled, so the engine cannot protect against correlated multi-asset risk yet.

## Operations and known limits

Startup requires Redis, PostgreSQL and the schema registry; missing dependencies are fatal rather than silently local. On startup, missing balance keys are restored from PostgreSQL without overwriting live values, so a partial Redis loss does not reset an active account. Redis runs with append-only persistence because losing it loses the live view.

The journal worker can lag, and that lag is the first signal to watch; a growing journal means balances are drifting from their audit view. Per-order reservation durability lives with the order records, not here, so this service cannot reconstruct a reservation that was never journaled. Metrics expose journal lag and balance state, the service listens on port 9002, and the health endpoint reflects process liveness only.

## Future direction

Business: portfolio-level margin, scenario margin and haircuts are the intended evolution; the current linear model is a placeholder.

Technical: gRPC to remove JSON overhead, then sharded in-process state with a write-ahead log to eliminate the Redis round trip. Both changes must preserve the atomic reserve semantics.

Boundaries: a dedicated margin service could own scenario calculations while this service keeps the hot balance check.
