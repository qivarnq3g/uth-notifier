CREATE TABLE subscribers (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    telegram_chat_id BIGINT NOT NULL UNIQUE,
    display_name TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    next_send_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deactivated_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE worker_leases (
    lease_key TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE campaigns (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    classification_id BIGINT NOT NULL UNIQUE REFERENCES classifications(id),
    post_id BIGINT NOT NULL REFERENCES posts(id),
    message_text TEXT NOT NULL CHECK (char_length(message_text) BETWEEN 1 AND 4096),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE deliveries (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id BIGINT NOT NULL REFERENCES campaigns(id),
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sending', 'retry', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    telegram_message_id BIGINT,
    sent_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campaign_id, subscriber_id)
);

CREATE INDEX deliveries_pending_idx ON deliveries (available_at, id)
WHERE status IN ('pending', 'retry', 'sending');
CREATE INDEX deliveries_completed_idx ON deliveries (updated_at)
WHERE status IN ('sent', 'failed');
CREATE INDEX subscribers_inactive_idx ON subscribers (updated_at) WHERE NOT active;

CREATE TABLE delivery_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id BIGINT NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    outcome TEXT NOT NULL,
    telegram_error_code INTEGER,
    detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (delivery_id, attempt)
);
