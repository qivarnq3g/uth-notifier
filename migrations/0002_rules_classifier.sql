CREATE TABLE classifications (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id),
    schema_version TEXT NOT NULL,
    input_content_hash TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('rejected', 'matched_explicit', 'manual_review')),
    score INTEGER NOT NULL,
    confidence_basis_points INTEGER NOT NULL CHECK (confidence_basis_points BETWEEN 0 AND 10000),
    matched_rules JSONB NOT NULL,
    classifier_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    classified_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (post_id, input_content_hash, classifier_version, config_hash)
);

CREATE INDEX classifications_post_created_idx ON classifications (post_id, created_at DESC);
CREATE INDEX classifications_decision_created_idx ON classifications (decision, created_at DESC);

CREATE TABLE classification_features (
    classification_id BIGINT PRIMARY KEY REFERENCES classifications(id) ON DELETE CASCADE,
    explicit_drl BOOLEAN NOT NULL,
    registration_call BOOLEAN NOT NULL,
    form_link BOOLEAN NOT NULL,
    future_event_time BOOLEAN NOT NULL,
    future_deadline BOOLEAN NOT NULL,
    location BOOLEAN NOT NULL,
    target_students BOOLEAN NOT NULL,
    approved_source BOOLEAN NOT NULL,
    negative_commercial BOOLEAN NOT NULL,
    past_event BOOLEAN NOT NULL,
    extracted JSONB NOT NULL
);

CREATE TABLE dead_letters (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    original_outbox_event_id BIGINT NOT NULL UNIQUE REFERENCES outbox_events(id),
    event_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL CHECK (attempts > 0),
    last_error TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX dead_letters_failed_idx ON dead_letters (failed_at);
CREATE INDEX outbox_processed_idx ON outbox_events (processed_at) WHERE processed_at IS NOT NULL;

CREATE INDEX outbox_classifier_pending_idx ON outbox_events (available_at, id)
WHERE processed_at IS NULL AND event_type IN ('facebook_post.discovered', 'facebook_post.updated');
