INSERT INTO manual_review_resolutions
    (classification_id, action, reviewed_by_chat_id, reason)
SELECT classification.id, 'skip', 0, '[Auto] post older than 3 days'
FROM classifications AS classification
JOIN post_revisions AS revision
  ON revision.post_id = classification.post_id
 AND revision.content_hash = classification.input_content_hash
WHERE classification.decision = 'manual_review'
  AND revision.published_at <= CURRENT_TIMESTAMP - INTERVAL '3 days'
  AND NOT EXISTS (
      SELECT 1 FROM manual_review_resolutions AS resolution
      WHERE resolution.classification_id = classification.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM campaigns AS campaign
      WHERE campaign.post_id = classification.post_id
  )
ON CONFLICT (classification_id) DO NOTHING;

INSERT INTO manual_review_resolutions
    (classification_id, action, reviewed_by_chat_id, reason)
SELECT classification.id, 'skip', 0, '[Auto] post already has a notification campaign'
FROM classifications AS classification
WHERE classification.decision = 'manual_review'
  AND NOT EXISTS (
      SELECT 1 FROM manual_review_resolutions AS resolution
      WHERE resolution.classification_id = classification.id
  )
  AND EXISTS (
      SELECT 1 FROM campaigns AS campaign
      WHERE campaign.post_id = classification.post_id
  )
ON CONFLICT (classification_id) DO NOTHING;

UPDATE outbox_events AS event
SET processed_at = NULL,
    available_at = CURRENT_TIMESTAMP,
    lease_owner = NULL,
    lease_expires_at = NULL,
    attempts = 0,
    last_error = NULL
FROM classifications AS classification
WHERE event.event_key = 'classification:' || classification.id::text
  AND event.event_type = 'classification.completed'
  AND event.processed_at IS NOT NULL
  AND classification.decision = 'manual_review'
  AND NOT EXISTS (
      SELECT 1 FROM manual_review_resolutions AS resolution
      WHERE resolution.classification_id = classification.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM campaigns AS campaign
      WHERE campaign.post_id = classification.post_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM dead_letters AS dead_letter
      WHERE dead_letter.original_outbox_event_id = event.id
  );
