CREATE TABLE manual_review_resolutions (
    classification_id BIGINT PRIMARY KEY REFERENCES classifications(id),
    action TEXT NOT NULL CHECK (action IN ('send', 'skip')),
    reviewed_by_chat_id BIGINT NOT NULL,
    reason TEXT,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX manual_review_resolutions_reviewed_idx
ON manual_review_resolutions (reviewed_at DESC);
