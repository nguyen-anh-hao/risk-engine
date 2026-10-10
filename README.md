# Risk Engine (`risk-engine`)

> **In-Memory Pre-Trade & Post-Trade Risk Management Service** built with Go.

---

## 1. Primary Responsibility
The Risk Engine guarantees market integrity and protects the broker/exchange from insolvency. Every order must pass pre-trade validation (margin checks, purchasing power, order value bounds) before it is forwarded to the matching engine. It maintains in-memory shadow balances and enforces credit locks (freeze/unfreeze balance).

---

## 2. Boundaries & I/O
- **Ingress:** Called synchronously by `api-gateway` (`POST /check`, `POST /reserve`, `POST /release`).
- **Data Stores:** 
  - **Hot Cache:** Redis for fast distributed balance validation.
  - **Cold Store:** PostgreSQL for durable state checkpoints.
- **Contract Reference:** `trading-contracts/proto/risk/risk_check.proto`.

---

## 3. Quick Start (Run in 2 minutes)

```bash
# 1. Start dependent Redis instance
docker run -d --name risk-redis -p 6379:6379 redis:alpine

# 2. Run service natively
go run ./cmd/main.go
```

Verify health:
```bash
curl http://localhost:9002/health
```

---

## 4. Executable Documentation (`requests.http`)
Use VS Code REST Client or cURL:

```http
### Health Check
GET http://localhost:9002/health

### Pre-trade Risk Check
POST http://localhost:9002/check
Content-Type: application/json

{
  "account_id": 1001,
  "symbol": "AAPL",
  "side": "BUY",
  "type": "LIMIT",
  "price": "150.00",
  "quantity": "10"
}

### Read Account Balances
GET http://localhost:9002/balances?account_id=1001
```

---

## 5. Engineering Roadmap (Scale & Optimization Focused)

| Version | Phase | Optimization Target & Algorithmic Problem |
| :--- | :--- | :--- |
| **v1.0.0** *(Current)* | **Baseline Risk Engine** | In-memory balance lookup with Redis sync. Simple cash & stock spot checks via HTTP/JSON. |
| **v1.1.0** | **gRPC & Protobuf Migration** | Switch from HTTP REST to native gRPC server with Server Reflection. Reduces serialization time by 4x. |
| **v1.2.0** | **Portfolio Margin & Haircut Algorithms** | Replace simple linear cash check with cross-asset portfolio margin (VaR - Value at Risk / SPAN margin simulation) computed in sub-millisecond time. Implement Redis Lua scripts to atomize lock & verify checks under high concurrent burst orders. |
| **v2.0.0** | **Sharded Lock-Free In-Memory State** | Eliminate Redis network roundtrips by pinning accounts to CPU-core sharded Go channels (Actor Model / LMAX pattern). State persisted via append-only write-ahead log (WAL) on NVMe SSD with zero lock contention. Throughput: > 500,000 risk checks/sec. |
