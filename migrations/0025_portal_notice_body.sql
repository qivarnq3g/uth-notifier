ALTER TABLE portal_notices
    ADD COLUMN body TEXT NOT NULL DEFAULT '',
    ADD COLUMN body_fetched_at TIMESTAMPTZ,
    ADD COLUMN body_fetch_attempts SMALLINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT portal_notices_body_fetch_attempts_check
        CHECK (body_fetch_attempts BETWEEN 0 AND 3),
    ADD CONSTRAINT portal_notices_body_byte_limit
        CHECK (octet_length(body) <= 65536);
