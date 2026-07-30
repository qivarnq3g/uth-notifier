CREATE TABLE donation_amount_input_state (
    telegram_chat_id BIGINT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX donation_amount_input_state_expires_idx
    ON donation_amount_input_state (expires_at);
