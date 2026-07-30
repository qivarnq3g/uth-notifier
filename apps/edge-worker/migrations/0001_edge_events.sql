CREATE TABLE edge_events (
    event_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_key TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    occurred_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'publishing', 'acknowledged')),
    lease_owner TEXT,
    lease_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    acknowledged_at TEXT
);

CREATE INDEX edge_events_pending_idx
    ON edge_events (state, sequence, created_at);

CREATE INDEX edge_events_lease_idx
    ON edge_events (lease_expires_at)
    WHERE state = 'publishing';
