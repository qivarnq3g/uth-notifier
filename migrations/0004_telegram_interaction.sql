CREATE TABLE telegram_update_state (
    consumer_key TEXT PRIMARY KEY,
    next_update_id BIGINT NOT NULL CHECK (next_update_id >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
