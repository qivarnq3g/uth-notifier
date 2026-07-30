CREATE TABLE source_suggestions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    telegram_chat_id BIGINT NOT NULL,
    submitted_url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected')),
    admin_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX source_suggestions_pending_url_idx
ON source_suggestions (submitted_url)
WHERE status = 'pending';

CREATE INDEX source_suggestions_status_created_idx
ON source_suggestions (status, created_at);
