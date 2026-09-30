ALTER TABLE edge_inbox_events
    DROP CONSTRAINT IF EXISTS edge_inbox_events_event_type_aggregate_key_sequence_key;

CREATE INDEX edge_inbox_aggregate_sequence_idx
    ON edge_inbox_events (event_type, aggregate_key, sequence);
