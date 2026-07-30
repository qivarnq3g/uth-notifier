CREATE TABLE donation_intents (
    order_code BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    telegram_chat_id BIGINT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount BETWEEN 10000 AND 10000000),
    status TEXT NOT NULL CHECK (status IN ('creating', 'pending', 'paid', 'cancelled', 'expired', 'failed')),
    payment_link_id TEXT UNIQUE,
    checkout_url TEXT,
    qr_code TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    failure_detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    paid_at TIMESTAMPTZ
);

CREATE INDEX donation_intents_chat_created_idx
    ON donation_intents (telegram_chat_id, created_at DESC);

CREATE INDEX donation_intents_pending_idx
    ON donation_intents (status, expires_at)
    WHERE status IN ('creating', 'pending');

CREATE TABLE donation_transactions (
    reference TEXT PRIMARY KEY,
    order_code BIGINT NOT NULL REFERENCES donation_intents(order_code),
    payment_link_id TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    transaction_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX donation_transactions_order_idx
    ON donation_transactions (order_code, transaction_at DESC);
