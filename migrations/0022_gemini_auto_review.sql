CREATE TABLE IF NOT EXISTS ai_review_learning_examples (
    id BIGSERIAL PRIMARY KEY,
    classification_id BIGINT REFERENCES classifications(id) ON DELETE SET NULL,
    post_id BIGINT REFERENCES posts(id) ON DELETE SET NULL,
    post_text TEXT NOT NULL,
    source_name TEXT NOT NULL,
    ai_decision TEXT NOT NULL CHECK (ai_decision IN ('send', 'skip')),
    ai_reason TEXT NOT NULL,
    admin_decision TEXT NOT NULL CHECK (admin_decision IN ('send', 'skip')),
    admin_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ai_review_learning_examples_created_idx
ON ai_review_learning_examples (created_at DESC);
