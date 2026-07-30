ALTER TABLE deliveries
    ADD COLUMN failure_class TEXT;

ALTER TABLE digest_batches
    ADD COLUMN failure_class TEXT;

UPDATE deliveries AS delivery
SET failure_class = 'recipient_unavailable'
FROM subscribers AS subscriber
WHERE delivery.subscriber_id = subscriber.id
  AND delivery.status = 'failed'
  AND NOT subscriber.active
  AND delivery.last_error IS NOT NULL
  AND subscriber.deactivated_reason = delivery.last_error;

UPDATE deliveries
SET failure_class = 'terminal_unknown'
WHERE status = 'failed' AND failure_class IS NULL;

UPDATE digest_batches
SET failure_class = 'terminal_unknown'
WHERE status = 'failed';

ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_failure_class_check CHECK (
        (status = 'failed') = (failure_class IS NOT NULL)
        AND (failure_class IS NULL OR failure_class IN (
            'recipient_unavailable',
            'retry_exhausted',
            'request_rejected',
            'terminal_unknown'
        ))
    );

ALTER TABLE digest_batches
    ADD CONSTRAINT digest_batches_failure_class_check CHECK (
        (status = 'failed') = (failure_class IS NOT NULL)
        AND (failure_class IS NULL OR failure_class IN (
            'recipient_unavailable',
            'retry_exhausted',
            'request_rejected',
            'terminal_unknown'
        ))
    );
