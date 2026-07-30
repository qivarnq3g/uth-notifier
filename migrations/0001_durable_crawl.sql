CREATE TABLE sources (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    schedule_interval_seconds INTEGER NOT NULL CHECK (schedule_interval_seconds > 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    next_crawl_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX sources_due_idx ON sources (next_crawl_at, id) WHERE enabled;

CREATE TABLE crawler_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES sources(id),
    contract_source_id TEXT,
    fetched_at TIMESTAMPTZ NOT NULL,
    health TEXT NOT NULL CHECK (health IN ('healthy', 'degraded', 'failed')),
    selected_strategy TEXT,
    post_count INTEGER NOT NULL CHECK (post_count >= 0),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX crawler_runs_source_created_idx ON crawler_runs (source_id, created_at DESC);
CREATE INDEX crawler_runs_created_idx ON crawler_runs (created_at);

CREATE TABLE crawler_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crawler_run_id BIGINT NOT NULL REFERENCES crawler_runs(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    strategy TEXT NOT NULL,
    outcome TEXT NOT NULL,
    status INTEGER,
    latency_ms BIGINT NOT NULL CHECK (latency_ms >= 0),
    bytes_received BIGINT NOT NULL CHECK (bytes_received >= 0),
    final_url TEXT,
    posts_found INTEGER NOT NULL CHECK (posts_found >= 0),
    newest_post_at TIMESTAMPTZ,
    parse_stats JSONB NOT NULL,
    error TEXT,
    UNIQUE (crawler_run_id, ordinal)
);

CREATE TABLE posts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES sources(id),
    external_post_id TEXT NOT NULL,
    current_content_hash TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    text TEXT NOT NULL,
    media JSONB NOT NULL,
    outbound_links JSONB NOT NULL,
    crawl_strategy TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_id, external_post_id)
);

CREATE INDEX posts_published_idx ON posts (published_at DESC);

CREATE TABLE post_revisions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id),
    content_hash TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    text TEXT NOT NULL,
    media JSONB NOT NULL,
    outbound_links JSONB NOT NULL,
    crawl_strategy TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (post_id, content_hash)
);

CREATE TABLE outbox_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX outbox_pending_idx ON outbox_events (available_at, id) WHERE processed_at IS NULL;
