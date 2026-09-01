ALTER TABLE campaigns
    DROP CONSTRAINT campaigns_message_text_check,
    ADD CONSTRAINT campaigns_message_text_check
        CHECK (octet_length(message_text) BETWEEN 1 AND 16384);

ALTER TABLE digest_batches
    DROP CONSTRAINT digest_batches_message_text_check,
    ADD CONSTRAINT digest_batches_message_text_check
        CHECK (octet_length(message_text) BETWEEN 1 AND 16384);
