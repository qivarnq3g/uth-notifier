CREATE TABLE operational_alert_state (
    alert_key TEXT PRIMARY KEY,
    observed_status TEXT NOT NULL CHECK (observed_status IN ('healthy', 'degraded', 'failed')),
    observed_since TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notified_status TEXT CHECK (notified_status IN ('healthy', 'degraded', 'failed')),
    notified_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
