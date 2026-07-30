ALTER TABLE user_feedback_messages
    ADD COLUMN telegram_update_id BIGINT;

CREATE UNIQUE INDEX user_feedback_messages_update_idx
    ON user_feedback_messages (telegram_chat_id, telegram_update_id)
    WHERE telegram_update_id IS NOT NULL;
