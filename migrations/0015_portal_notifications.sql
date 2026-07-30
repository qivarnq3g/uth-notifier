CREATE TABLE portal_notice_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_seen_portal_id BIGINT NOT NULL CHECK (last_seen_portal_id >= 0),
    initialized_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE portal_notices (
    portal_id BIGINT PRIMARY KEY CHECK (portal_id > 0),
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 1000),
    displayed_at TIMESTAMPTZ NOT NULL,
    article_url TEXT,
    attachment_url TEXT,
    attachment_file_name TEXT,
    attachment_content_type TEXT,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (article_url IS NULL OR char_length(article_url) <= 2048),
    CHECK (attachment_url IS NULL OR char_length(attachment_url) <= 2048),
    CHECK (attachment_file_name IS NULL OR char_length(attachment_file_name) <= 255),
    CHECK (attachment_content_type IS NULL OR char_length(attachment_content_type) <= 255)
);

ALTER TABLE campaigns
    ALTER COLUMN classification_id DROP NOT NULL,
    ALTER COLUMN post_id DROP NOT NULL,
    ADD COLUMN portal_notice_id BIGINT UNIQUE REFERENCES portal_notices(portal_id),
    ADD COLUMN attachment_url TEXT,
    ADD COLUMN attachment_file_name TEXT,
    ADD COLUMN attachment_content_type TEXT,
    ADD COLUMN telegram_file_id TEXT,
    ADD CONSTRAINT campaigns_source_check CHECK (
        (classification_id IS NOT NULL AND post_id IS NOT NULL AND portal_notice_id IS NULL)
        OR
        (classification_id IS NULL AND post_id IS NULL AND portal_notice_id IS NOT NULL)
    ),
    ADD CONSTRAINT campaigns_attachment_url_length_check
        CHECK (attachment_url IS NULL OR char_length(attachment_url) <= 2048),
    ADD CONSTRAINT campaigns_attachment_file_name_length_check
        CHECK (attachment_file_name IS NULL OR char_length(attachment_file_name) <= 255),
    ADD CONSTRAINT campaigns_attachment_content_type_length_check
        CHECK (attachment_content_type IS NULL OR char_length(attachment_content_type) <= 255),
    ADD CONSTRAINT campaigns_telegram_file_id_length_check
        CHECK (telegram_file_id IS NULL OR char_length(telegram_file_id) <= 512);

CREATE INDEX portal_notices_discovered_idx ON portal_notices (discovered_at DESC);
