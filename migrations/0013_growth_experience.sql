ALTER TABLE subscribers
    ADD COLUMN acquisition_source TEXT,
    ADD COLUMN onboarding_completed_at TIMESTAMPTZ,
    ADD COLUMN notification_scope TEXT NOT NULL DEFAULT 'all'
        CHECK (notification_scope IN ('drl', 'all')),
    ADD COLUMN delivery_mode TEXT NOT NULL DEFAULT 'instant'
        CHECK (delivery_mode IN ('instant', 'daily')),
    ADD COLUMN quiet_hours_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN next_digest_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN donation_prompted_at TIMESTAMPTZ;

UPDATE subscribers
SET onboarding_completed_at = created_at
WHERE active;

ALTER TABLE campaigns
    ADD COLUMN post_url TEXT,
    ADD COLUMN action_url TEXT,
    ADD COLUMN explicit_drl BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE product_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id BIGINT REFERENCES subscribers(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL CHECK (char_length(event_type) BETWEEN 1 AND 100),
    campaign_id BIGINT REFERENCES campaigns(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX product_events_type_occurred_idx
    ON product_events (event_type, occurred_at DESC);

CREATE INDEX product_events_subscriber_occurred_idx
    ON product_events (subscriber_id, occurred_at DESC);

CREATE TABLE notification_feedback (
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    campaign_id BIGINT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    value TEXT NOT NULL CHECK (value IN ('useful', 'irrelevant')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (subscriber_id, campaign_id)
);

CREATE TABLE digest_batches (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    message_text TEXT NOT NULL CHECK (char_length(message_text) BETWEEN 1 AND 4096),
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
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX digest_batches_pending_idx ON digest_batches (available_at, id)
WHERE status IN ('pending', 'retry', 'sending');

CREATE TABLE digest_items (
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    campaign_id BIGINT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    digest_batch_id BIGINT REFERENCES digest_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (subscriber_id, campaign_id)
);

CREATE INDEX digest_items_unbatched_idx ON digest_items (subscriber_id, created_at)
WHERE digest_batch_id IS NULL;
