CREATE TABLE edge_inbox_events (
    event_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_key TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence >= 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    last_error TEXT,
    UNIQUE (event_type, aggregate_key, sequence)
);

CREATE INDEX edge_inbox_pending_idx
    ON edge_inbox_events (aggregate_key, sequence, imported_at)
    WHERE processed_at IS NULL;
