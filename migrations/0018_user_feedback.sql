CREATE TABLE user_feedback_input_state (
    telegram_chat_id BIGINT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_feedback_messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id BIGINT REFERENCES subscribers(id) ON DELETE SET NULL,
    telegram_chat_id BIGINT NOT NULL,
    sender_label TEXT NOT NULL CHECK (char_length(sender_label) BETWEEN 1 AND 256),
    message TEXT NOT NULL CHECK (char_length(message) BETWEEN 1 AND 2000),
    admin_notified_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX user_feedback_messages_pending_idx
    ON user_feedback_messages (created_at, id)
    WHERE admin_notified_at IS NULL;
