ALTER TABLE edge_inbox_events
    ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN dead_lettered_at TIMESTAMPTZ;

DROP INDEX edge_inbox_pending_idx;

CREATE INDEX edge_inbox_pending_idx
    ON edge_inbox_events (event_type, available_at, aggregate_key, sequence, imported_at)
    WHERE processed_at IS NULL AND dead_lettered_at IS NULL;

CREATE INDEX edge_inbox_dead_letter_idx
    ON edge_inbox_events (dead_lettered_at DESC)
    WHERE dead_lettered_at IS NOT NULL;
