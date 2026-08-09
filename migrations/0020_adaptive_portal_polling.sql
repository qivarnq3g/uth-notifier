ALTER TABLE portal_notice_state
    ADD COLUMN poll_mode TEXT NOT NULL DEFAULT 'steady',
    ADD COLUMN next_poll_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN burst_until TIMESTAMPTZ,
    ADD COLUMN cooldown_reason TEXT,
    ADD COLUMN last_polled_at TIMESTAMPTZ,
    ADD COLUMN last_poll_outcome TEXT,
    ADD COLUMN last_http_status INTEGER,
    ADD CONSTRAINT portal_notice_state_poll_mode_check
        CHECK (poll_mode IN ('steady', 'burst', 'cooldown')),
    ADD CONSTRAINT portal_notice_state_http_status_check
        CHECK (last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599),
    ADD CONSTRAINT portal_notice_state_poll_state_check CHECK (
        (poll_mode = 'steady' AND burst_until IS NULL AND cooldown_reason IS NULL)
        OR (poll_mode = 'burst' AND burst_until IS NOT NULL AND cooldown_reason IS NULL)
        OR (poll_mode = 'cooldown' AND burst_until IS NULL AND cooldown_reason IS NOT NULL)
    );
