CREATE TABLE IF NOT EXISTS risk_balances (
    account_id          BIGINT NOT NULL,
    asset               TEXT NOT NULL,
    total               NUMERIC(30,8) NOT NULL DEFAULT 0,
    available           NUMERIC(30,8) NOT NULL DEFAULT 0,
    frozen              NUMERIC(30,8) NOT NULL DEFAULT 0,
    version             BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, asset)
);

CREATE TABLE IF NOT EXISTS risk_reservations (
    order_id            BIGINT PRIMARY KEY,
    account_id          BIGINT NOT NULL,
    asset               TEXT NOT NULL,
    amount              NUMERIC(30,8) NOT NULL,
    status              TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS risk_events (
    event_id            UUID PRIMARY KEY,
    event_type          TEXT NOT NULL,
    payload             JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type      TEXT NOT NULL,
    aggregate_id        TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    payload             BYTEA NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at        TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'PENDING',
    retry_count         INT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_risk_outbox_pending ON outbox(status, created_at);
