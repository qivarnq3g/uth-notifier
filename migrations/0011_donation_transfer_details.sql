ALTER TABLE donation_intents
    ADD COLUMN bank_bin TEXT,
    ADD COLUMN account_number TEXT,
    ADD COLUMN account_name TEXT,
    ADD COLUMN transfer_description TEXT;
