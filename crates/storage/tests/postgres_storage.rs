use chrono::{TimeZone, Utc};
use sqlx::postgres::PgPoolOptions;
use uth_classifier::RuleClassifier;
use uth_domain::{
    ClassificationResult, CrawlReport, EDGE_EVENT_SCHEMA_VERSION, EdgeEvent, FacebookPost,
    MediaItem, POST_SCHEMA_VERSION, REPORT_SCHEMA_VERSION,
};
use uth_storage::{
    AiLearningFeedbackPayload, CrawlStore, DeliveryFailureClass, DonationIntentPaymentLink,
    DonationPayment, FailureDisposition, ManualReviewAction, ManualReviewNotification,
    ManualReviewOverrideOutcome, NotificationContent, OperationalAlertKind, PortalNoticeRecord,
    PortalPollState, SourceSeed, USER_STOP_REASON,
};

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing to a disposable PostgreSQL database"]
async fn durable_pipeline_is_idempotent_and_handles_classifier_failures() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let store = CrawlStore::connect(&database_url, 2).await.unwrap();
    store.migrate().await.unwrap();
    sqlx::query("TRUNCATE operational_alert_state, outbox_events, post_revisions, posts, crawler_runs, sources RESTART IDENTITY CASCADE")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store
            .observe_operational_alert("primary", "healthy", 60)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .observe_operational_alert("primary", "degraded", 60)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query(
        "UPDATE operational_alert_state SET observed_since = CURRENT_TIMESTAMP - INTERVAL '61 seconds' WHERE alert_key = 'primary'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let degraded_alert = store
        .observe_operational_alert("primary", "degraded", 60)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(degraded_alert.kind, OperationalAlertKind::Degraded);
    assert!(
        store
            .complete_operational_alert(&degraded_alert)
            .await
            .unwrap()
    );
    assert!(
        store
            .observe_operational_alert("primary", "degraded", 60)
            .await
            .unwrap()
            .is_none()
    );
    let failed_alert = store
        .observe_operational_alert("primary", "failed", 60)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed_alert.kind, OperationalAlertKind::Failed);
    assert!(
        store
            .complete_operational_alert(&failed_alert)
            .await
            .unwrap()
    );
    assert!(
        store
            .observe_operational_alert("primary", "healthy", 60)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query(
        "UPDATE operational_alert_state SET observed_since = CURRENT_TIMESTAMP - INTERVAL '61 seconds' WHERE alert_key = 'primary'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let recovered_alert = store
        .observe_operational_alert("primary", "healthy", 60)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_alert.kind, OperationalAlertKind::Recovered);
    assert!(
        store
            .complete_operational_alert(&recovered_alert)
            .await
            .unwrap()
    );
    assert!(
        store
            .observe_operational_alert("primary", "healthy", 60)
            .await
            .unwrap()
            .is_none()
    );
    store
        .upsert_sources(&[SourceSeed {
            key: "source-a".to_owned(),
            name: "Source A".to_owned(),
            url: "https://www.facebook.com/source.a".to_owned(),
            schedule_interval_seconds: 300,
        }])
        .await
        .unwrap();

    let owner = "integration-test";
    let source = claim(&store, owner).await;
    assert!(source.initial_crawl);
    assert_eq!(
        store.release_source_leases("another-owner").await.unwrap(),
        0
    );
    assert_eq!(store.release_source_leases(owner).await.unwrap(), 1);
    let source = claim(&store, owner).await;
    assert!(source.initial_crawl);
    let first_report = healthy_report(
        "sha256:first",
        "Mời sinh viên đăng ký hoạt động điểm rèn luyện. Hạn đăng ký 25/07/2026.",
    );
    let first = store
        .persist_report(&source, owner, &first_report, 300, true, true)
        .await
        .unwrap();
    assert_eq!(first.inserted, 1);
    assert_eq!(first.outbox_events, 1);

    make_due(&pool).await;
    let source = claim(&store, owner).await;
    assert!(!source.initial_crawl);
    let repeated = store
        .persist_report(&source, owner, &first_report, 300, true, true)
        .await
        .unwrap();
    assert_eq!(repeated.unchanged, 1);
    assert_eq!(repeated.outbox_events, 0);

    make_due(&pool).await;
    let source = claim(&store, owner).await;
    let mut rotated_identity_report = first_report.clone();
    rotated_identity_report.posts[0].external_post_id = "pfbid-rotated".to_owned();
    rotated_identity_report.posts[0].canonical_url =
        "https://www.facebook.com/source.a/posts/pfbid-rotated".to_owned();
    let rotated_identity = store
        .persist_report(&source, owner, &rotated_identity_report, 300, true, true)
        .await
        .unwrap();
    assert_eq!(rotated_identity.inserted, 0);
    assert_eq!(rotated_identity.unchanged, 1);
    assert_eq!(rotated_identity.outbox_events, 0);
    let post_count_after_rotation: i64 = sqlx::query_scalar("SELECT count(*) FROM posts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(post_count_after_rotation, 1);

    make_due(&pool).await;
    let source = claim(&store, owner).await;
    let mut updated_report = healthy_report("sha256:second", "Updated text");
    updated_report.posts[0].external_post_id = "pfbid-rotated".to_owned();
    updated_report.posts[0].canonical_url =
        "https://www.facebook.com/source.a/posts/pfbid-rotated".to_owned();
    let updated = store
        .persist_report(&source, owner, &updated_report, 300, true, true)
        .await
        .unwrap();
    assert_eq!(updated.updated, 1);
    assert_eq!(updated.outbox_events, 1);

    make_due(&pool).await;
    let source = claim(&store, owner).await;
    let degraded_report = CrawlReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        source_url: source.url.clone(),
        source_id: "facebook:source.a".to_owned(),
        fetched_at: Utc::now().to_rfc3339(),
        selected_strategy: None,
        health: "degraded".to_owned(),
        post_count: 0,
        attempts: Vec::new(),
        posts: Vec::new(),
        changes: None,
    };
    store
        .persist_report(&source, owner, &degraded_report, 60, true, true)
        .await
        .unwrap();

    let crawl_history = store.crawl_history(20, 0).await.unwrap();
    let degraded_run = crawl_history
        .iter()
        .find(|run| run.health == "degraded" && run.post_count == 0)
        .expect("empty degraded crawl must be retained");
    let degraded_detail = store
        .crawl_history_item(degraded_run.run_id)
        .await
        .unwrap()
        .expect("retained crawl must have a detail record");
    assert_eq!(degraded_detail.run.attempt_count, 0);
    assert!(degraded_detail.attempts.is_empty());

    let post_count: i64 = sqlx::query_scalar("SELECT count(*) FROM posts")
        .fetch_one(&pool)
        .await
        .unwrap();
    let revision_count: i64 = sqlx::query_scalar("SELECT count(*) FROM post_revisions")
        .fetch_one(&pool)
        .await
        .unwrap();
    let event_count: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    let failure_count: i32 = sqlx::query_scalar("SELECT failure_count FROM sources")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(post_count, 1);
    assert_eq!(revision_count, 2);
    assert_eq!(event_count, 2);
    assert_eq!(failure_count, 1);

    let event = store
        .claim_classification_events(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let database_post_id = event.payload["database_post_id"].as_i64().unwrap();
    let event_post: FacebookPost = serde_json::from_value(event.payload["post"].clone()).unwrap();
    let classification = classification(&event_post);
    let mut mismatched = classification.clone();
    mismatched.external_post_id = "wrong-post".to_owned();
    assert!(
        store
            .complete_classification(&event, owner, database_post_id, &mismatched)
            .await
            .is_err()
    );
    let completed = store
        .complete_classification(&event, owner, database_post_id, &classification)
        .await
        .unwrap();
    assert!(completed.classification_inserted);
    assert!(completed.completion_event_inserted);

    let poison = store
        .claim_classification_events(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let disposition = store
        .fail_classification_event(&poison, owner, "invalid payload", 2, 1)
        .await
        .unwrap();
    assert_eq!(disposition, FailureDisposition::RetryScheduled);
    sqlx::query("UPDATE outbox_events SET available_at = CURRENT_TIMESTAMP WHERE id = $1")
        .bind(poison.id)
        .execute(&pool)
        .await
        .unwrap();
    let poison = store
        .claim_classification_events(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let disposition = store
        .fail_classification_event(&poison, owner, "invalid payload", 2, 1)
        .await
        .unwrap();
    assert_eq!(disposition, FailureDisposition::DeadLettered);

    let classification_count: i64 = sqlx::query_scalar("SELECT count(*) FROM classifications")
        .fetch_one(&pool)
        .await
        .unwrap();
    let feature_count: i64 = sqlx::query_scalar("SELECT count(*) FROM classification_features")
        .fetch_one(&pool)
        .await
        .unwrap();
    let dead_letter_count: i64 = sqlx::query_scalar("SELECT count(*) FROM dead_letters")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(classification_count, 1);
    assert_eq!(feature_count, 1);
    assert_eq!(dead_letter_count, 1);
    let failed_health = store.operational_health(3, 60).await.unwrap();
    assert_eq!(failed_health.status, "failed");
    assert_eq!(failed_health.dead_letters, 1);
    assert_eq!(failed_health.pending_notification_events, 1);

    sqlx::query("UPDATE dead_letters SET failed_at = CURRENT_TIMESTAMP - INTERVAL '2 days'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE outbox_events SET processed_at = CURRENT_TIMESTAMP - INTERVAL '2 days' \
         WHERE processed_at IS NOT NULL",
    )
    .execute(&pool)
    .await
    .unwrap();
    let retention = store.apply_classifier_retention(1, 1).await.unwrap();
    assert_eq!(retention.dead_letters_deleted, 1);
    assert_eq!(retention.processed_outbox_events_deleted, 2);

    store
        .upsert_subscriber(123_456_789, Some("Test recipient"))
        .await
        .unwrap();
    store
        .update_subscriber_preferences(123_456_789, None, None, Some(false))
        .await
        .unwrap();
    let subscriber = store.subscriber(123_456_789).await.unwrap().unwrap();
    assert!(subscriber.active);
    assert_eq!(subscriber.display_name.as_deref(), Some("Test recipient"));
    assert_eq!(
        store
            .telegram_next_update_id("integration-test")
            .await
            .unwrap(),
        0
    );
    store
        .advance_telegram_update_id("integration-test", 42)
        .await
        .unwrap();
    assert_eq!(
        store
            .telegram_next_update_id("integration-test")
            .await
            .unwrap(),
        42
    );
    let suggestion = store
        .submit_source_suggestion(123_456_789, "https://www.facebook.com/proposed.page/")
        .await
        .unwrap();
    assert!(suggestion.created);
    let duplicate = store
        .submit_source_suggestion(123_456_789, "https://www.facebook.com/proposed.page/")
        .await
        .unwrap();
    assert!(!duplicate.created);
    assert_eq!(duplicate.id, suggestion.id);
    assert_eq!(
        store
            .list_source_suggestions(Some("pending"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .approve_source_suggestion(suggestion.id, "Proposed Page")
            .await
            .unwrap()
    );
    assert!(
        store
            .list_enabled_sources()
            .await
            .unwrap()
            .iter()
            .any(|source| source.name == "Proposed Page")
    );
    let rejected = store
        .submit_source_suggestion(123_456_789, "https://www.facebook.com/rejected.page/")
        .await
        .unwrap();
    assert!(
        store
            .reject_source_suggestion(rejected.id, "Không phù hợp")
            .await
            .unwrap()
    );
    let notification_event = store
        .claim_notification_events(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let classification_id = notification_event.payload["database_classification_id"]
        .as_i64()
        .unwrap();
    let post_id = notification_event.payload["database_post_id"]
        .as_i64()
        .unwrap();
    let notification = NotificationContent {
        message_text: "Test notification",
        post_url: Some("https://www.facebook.com/test/posts/1"),
        action_url: Some("https://forms.gle/test"),
        explicit_drl: true,
    };
    let plan = store
        .plan_notification(
            &notification_event,
            owner,
            classification_id,
            post_id,
            Some(&notification),
        )
        .await
        .unwrap();
    assert!(plan.campaign_created);
    assert_eq!(plan.deliveries_created, 1);
    let duplicate_classification_id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO classifications \
         (post_id, schema_version, input_content_hash, decision, score, \
          confidence_basis_points, matched_rules, classifier_version, config_hash, classified_at) \
         VALUES ($1, 'classification.v1', 'sha256:second', 'matched_explicit', 10, 10000, \
          '[]'::jsonb, 'duplicate-campaign-test', \
          'sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', \
          CURRENT_TIMESTAMP) RETURNING id",
    )
    .bind(post_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outbox_events \
         (event_key, event_type, aggregate_type, aggregate_id, payload) \
         VALUES ('duplicate-campaign-test', 'classification.completed', 'facebook_post', $2::text, \
          jsonb_build_object('database_classification_id', $1, 'database_post_id', $2, \
          'classification', jsonb_build_object('decision', 'matched_explicit')))",
    )
    .bind(duplicate_classification_id)
    .bind(post_id)
    .execute(&pool)
    .await
    .unwrap();
    let duplicate_event = store
        .claim_notification_events(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let duplicate_plan = store
        .plan_notification(
            &duplicate_event,
            owner,
            duplicate_classification_id,
            post_id,
            Some(&notification),
        )
        .await
        .unwrap();
    assert!(duplicate_plan.skipped);
    assert!(!duplicate_plan.campaign_created);
    assert_eq!(duplicate_plan.deliveries_created, 0);
    let campaign_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM campaigns WHERE post_id = $1")
            .bind(post_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(campaign_count, 1);
    let delivery = store
        .claim_deliveries(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(delivery.telegram_chat_id, 123_456_789);
    assert_eq!(delivery.post_url.as_deref(), notification.post_url);
    assert_eq!(delivery.action_url.as_deref(), notification.action_url);
    store
        .retry_delivery(&delivery, owner, 1, Some(429), "rate limited")
        .await
        .unwrap();
    sqlx::query("UPDATE deliveries SET available_at = CURRENT_TIMESTAMP")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE subscribers SET next_send_at = CURRENT_TIMESTAMP")
        .execute(&pool)
        .await
        .unwrap();
    let delivery = store
        .claim_deliveries(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    store
        .complete_delivery(&delivery, owner, 777, None)
        .await
        .unwrap();
    let delivery_status: String = sqlx::query_scalar("SELECT status FROM deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    let attempt_count: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_attempts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(delivery_status, "sent");
    assert_eq!(attempt_count, 2);
    store
        .upsert_subscriber(123_456_790, Some("Unavailable recipient"))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO deliveries (campaign_id, subscriber_id) \
         SELECT $1, id FROM subscribers WHERE telegram_chat_id = $2",
    )
    .bind(delivery.campaign_id)
    .bind(123_456_790_i64)
    .execute(&pool)
    .await
    .unwrap();
    let unavailable_delivery = store
        .claim_deliveries(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    store
        .fail_delivery(
            &unavailable_delivery,
            owner,
            Some(403),
            "Forbidden: bot was blocked by the user",
            DeliveryFailureClass::RecipientUnavailable,
        )
        .await
        .unwrap();
    let unavailable_failure = sqlx::query_as::<_, (String, bool)>(
        "SELECT delivery.failure_class, subscriber.active \
         FROM deliveries AS delivery \
         JOIN subscribers AS subscriber ON subscriber.id = delivery.subscriber_id \
         WHERE delivery.id = $1",
    )
    .bind(unavailable_delivery.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        unavailable_failure,
        ("recipient_unavailable".to_owned(), false)
    );
    let health = store.operational_health(3, 60).await.unwrap();
    assert_eq!(health.failed_deliveries, 0);
    sqlx::query("DELETE FROM deliveries WHERE id = $1")
        .bind(unavailable_delivery.id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM subscribers WHERE telegram_chat_id = $1")
        .bind(123_456_790_i64)
        .execute(&pool)
        .await
        .unwrap();

    store
        .upsert_subscriber(123_456_791, Some("Retry exhausted recipient"))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO deliveries (campaign_id, subscriber_id) \
         SELECT $1, id FROM subscribers WHERE telegram_chat_id = $2",
    )
    .bind(delivery.campaign_id)
    .bind(123_456_791_i64)
    .execute(&pool)
    .await
    .unwrap();
    let exhausted_delivery = store
        .claim_deliveries(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    store
        .fail_delivery(
            &exhausted_delivery,
            owner,
            None,
            "Telegram request timed out",
            DeliveryFailureClass::RetryExhausted,
        )
        .await
        .unwrap();
    let health = store.operational_health(3, 60).await.unwrap();
    assert_eq!(health.failed_deliveries, 1);
    assert_eq!(health.status, "failed");
    sqlx::query("DELETE FROM deliveries WHERE id = $1")
        .bind(exhausted_delivery.id)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .open_campaign_action(123_456_789, delivery.campaign_id)
            .await
            .unwrap()
            .as_deref(),
        notification.action_url
    );
    let feedback = store
        .record_notification_feedback(123_456_789, delivery.campaign_id, "useful")
        .await
        .unwrap();
    assert!(feedback.should_prompt_donation);
    let metrics = store.growth_metrics().await.unwrap();
    assert_eq!(metrics.notifications_delivered_7d, 1);
    assert_eq!(metrics.cta_clicks_7d, 1);
    assert_eq!(metrics.useful_feedback_7d, 1);
    store
        .begin_user_feedback_input(
            123_456_789,
            chrono::Utc::now() + chrono::Duration::minutes(10),
        )
        .await
        .unwrap();
    assert!(store.user_feedback_input_active(123_456_789).await.unwrap());
    let feedback_id = store
        .record_user_feedback(
            123_456_789,
            9001,
            "Test user (@test_user)",
            "Thông báo này đến hơi trễ.",
        )
        .await
        .unwrap();
    assert!(!store.user_feedback_input_active(123_456_789).await.unwrap());
    let pending_feedback = store.pending_user_feedback(10).await.unwrap();
    assert_eq!(pending_feedback.len(), 1);
    assert_eq!(pending_feedback[0].id, feedback_id);
    assert_eq!(pending_feedback[0].telegram_chat_id, 123_456_789);
    assert_eq!(pending_feedback[0].message, "Thông báo này đến hơi trễ.");
    assert_eq!(
        store
            .record_user_feedback(
                123_456_789,
                9001,
                "Changed sender",
                "Duplicate update should be ignored.",
            )
            .await
            .unwrap(),
        feedback_id
    );
    assert_eq!(store.pending_user_feedback(10).await.unwrap().len(), 1);
    store
        .mark_user_feedback_attempt(feedback_id, "temporary admin delivery failure")
        .await
        .unwrap();
    assert_eq!(
        store.pending_user_feedback(10).await.unwrap()[0].attempts,
        1
    );
    assert!(
        store
            .mark_user_feedback_notified(feedback_id)
            .await
            .unwrap()
    );
    assert!(store.pending_user_feedback(10).await.unwrap().is_empty());
    let (feedback_total, feedback_history) = store.user_feedback_history(10, 0).await.unwrap();
    assert_eq!(feedback_total, 1);
    assert_eq!(feedback_history.len(), 1);
    assert_eq!(feedback_history[0].id, feedback_id);
    assert!(feedback_history[0].admin_notified_at.is_some());
    store
        .update_subscriber_preferences(123_456_789, None, Some("daily"), Some(true))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE subscribers SET next_digest_at = CURRENT_TIMESTAMP, quiet_hours_enabled = FALSE WHERE telegram_chat_id = $1",
    )
    .bind(123_456_789_i64)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO digest_items (subscriber_id, campaign_id) \
         SELECT id, $1 FROM subscribers WHERE telegram_chat_id = $2",
    )
    .bind(delivery.campaign_id)
    .bind(123_456_789_i64)
    .execute(&pool)
    .await
    .unwrap();
    let digest_preparation = store.prepare_due_digests(10).await.unwrap();
    assert_eq!(digest_preparation.batches_created, 1);
    assert_eq!(digest_preparation.items_batched, 1);
    assert_eq!(digest_preparation.duplicate_items_collapsed, 0);
    let digest = store
        .claim_digest_deliveries(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(digest.message_text.contains("Bản tin hoạt động lúc 07:30"));
    store
        .complete_digest_delivery(&digest, owner, 778)
        .await
        .unwrap();
    store
        .upsert_subscriber(123_456_792, Some("Unavailable digest recipient"))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO digest_batches (subscriber_id, message_text) \
         SELECT id, 'Unavailable digest' FROM subscribers WHERE telegram_chat_id = $1",
    )
    .bind(123_456_792_i64)
    .execute(&pool)
    .await
    .unwrap();
    let unavailable_digest = store
        .claim_digest_deliveries(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    store
        .fail_digest_delivery(
            &unavailable_digest,
            owner,
            "Forbidden: bot was blocked by the user",
            DeliveryFailureClass::RecipientUnavailable,
        )
        .await
        .unwrap();
    let unavailable_digest_failure = sqlx::query_as::<_, (String, bool)>(
        "SELECT batch.failure_class, subscriber.active \
         FROM digest_batches AS batch \
         JOIN subscribers AS subscriber ON subscriber.id = batch.subscriber_id \
         WHERE batch.id = $1",
    )
    .bind(unavailable_digest.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        unavailable_digest_failure,
        ("recipient_unavailable".to_owned(), false)
    );
    let health = store.operational_health(3, 60).await.unwrap();
    assert_eq!(health.failed_digest_batches, 0);
    sqlx::query("DELETE FROM digest_batches WHERE id = $1")
        .bind(unavailable_digest.id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM subscribers WHERE telegram_chat_id = $1")
        .bind(123_456_792_i64)
        .execute(&pool)
        .await
        .unwrap();

    store
        .upsert_subscriber(123_456_793, Some("Rejected digest recipient"))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO digest_batches (subscriber_id, message_text) \
         SELECT id, 'Rejected digest' FROM subscribers WHERE telegram_chat_id = $1",
    )
    .bind(123_456_793_i64)
    .execute(&pool)
    .await
    .unwrap();
    let rejected_digest = store
        .claim_digest_deliveries(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    store
        .fail_digest_delivery(
            &rejected_digest,
            owner,
            "Telegram rejected the digest",
            DeliveryFailureClass::RequestRejected,
        )
        .await
        .unwrap();
    let health = store.operational_health(3, 60).await.unwrap();
    assert_eq!(health.failed_digest_batches, 1);
    assert_eq!(health.status, "failed");
    sqlx::query("DELETE FROM digest_batches WHERE id = $1")
        .bind(rejected_digest.id)
        .execute(&pool)
        .await
        .unwrap();
    store
        .update_subscriber_preferences(123_456_789, None, Some("instant"), Some(false))
        .await
        .unwrap();
    let manual_classification_id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO classifications \
         (post_id, schema_version, input_content_hash, decision, score, \
          confidence_basis_points, matched_rules, classifier_version, config_hash, classified_at) \
         VALUES ($1, 'classification.v1', 'sha256:second', 'manual_review', 4, 6500, \
          '[\"feature.form_link\"]'::jsonb, 'manual-review-test', \
          'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', \
          CURRENT_TIMESTAMP) RETURNING id",
    )
    .bind(post_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let reviews = store.pending_manual_reviews(5, 0).await.unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].classification_id, manual_classification_id);
    assert_eq!(reviews[0].post.content_hash, "sha256:second");
    assert!(
        store
            .resolve_manual_review(
                manual_classification_id,
                999,
                123_456_789,
                ManualReviewAction::Send,
                None,
                Some(ManualReviewNotification {
                    message_text: "Manual notification",
                    post_url: "https://www.facebook.com/test/posts/1",
                }),
            )
            .await
            .is_err()
    );
    let resolution = store
        .resolve_manual_review(
            manual_classification_id,
            123_456_789,
            123_456_789,
            ManualReviewAction::Send,
            None,
            Some(ManualReviewNotification {
                message_text: "Manual notification",
                post_url: "https://www.facebook.com/test/posts/1",
            }),
        )
        .await
        .unwrap();
    assert!(resolution.resolved);
    assert!(!resolution.campaign_created);
    assert_eq!(resolution.deliveries_created, 0);
    assert!(store.pending_manual_reviews(5, 0).await.unwrap().is_empty());
    let repeated = store
        .resolve_manual_review(
            manual_classification_id,
            123_456_789,
            123_456_789,
            ManualReviewAction::Send,
            None,
            Some(ManualReviewNotification {
                message_text: "Manual notification",
                post_url: "https://www.facebook.com/test/posts/1",
            }),
        )
        .await
        .unwrap();
    assert!(!repeated.resolved);
    let duplicate_manual_classification_id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO classifications \
         (post_id, schema_version, input_content_hash, decision, score, \
          confidence_basis_points, matched_rules, classifier_version, config_hash, classified_at) \
         VALUES ($1, 'classification.v1', 'sha256:first', 'manual_review', 4, 6500, \
          '[\"feature.form_link\"]'::jsonb, 'manual-review-duplicate-test', \
          'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', \
          CURRENT_TIMESTAMP) RETURNING id",
    )
    .bind(post_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        store
            .inherit_duplicate_manual_review_resolution(duplicate_manual_classification_id)
            .await
            .unwrap()
    );
    assert!(
        !store
            .inherit_duplicate_manual_review_resolution(duplicate_manual_classification_id)
            .await
            .unwrap()
    );
    assert!(store.pending_manual_reviews(5, 0).await.unwrap().is_empty());
    let inherited_action: String = sqlx::query_scalar(
        "SELECT action FROM manual_review_resolutions WHERE classification_id = $1",
    )
    .bind(duplicate_manual_classification_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(inherited_action, "skip");
    make_due(&pool).await;
    let source = claim(&store, owner).await;
    let mut plugin_report = healthy_report("sha256:plugin", "Plugin fallback text");
    plugin_report.posts[0].external_post_id = "pfbid-rotated".to_owned();
    let plugin_transition = store
        .persist_report(&source, owner, &plugin_report, 300, true, true)
        .await
        .unwrap();
    assert_eq!(plugin_transition.unchanged, 0);
    assert_eq!(plugin_transition.updated, 1);
    assert_eq!(plugin_transition.outbox_events, 1);
    let stored_post_count: i64 = sqlx::query_scalar("SELECT count(*) FROM posts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored_post_count, 1);
    let latest = store.latest_posts(5, 0).await.unwrap();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].post.external_post_id, "pfbid-rotated");
    assert_eq!(
        store
            .latest_post(latest[0].database_post_id)
            .await
            .unwrap()
            .unwrap()
            .post
            .text,
        "Plugin fallback text"
    );
    make_due(&pool).await;
    let source = claim(&store, owner).await;
    store
        .persist_report(&source, owner, &degraded_report, 60, true, true)
        .await
        .unwrap();
    assert!(
        store
            .acquire_worker_lease("telegram_delivery", owner, 60)
            .await
            .unwrap()
    );
    assert!(
        !store
            .acquire_worker_lease("telegram_delivery", "other-worker", 60)
            .await
            .unwrap()
    );
    assert!(
        !store
            .release_worker_lease("telegram_delivery", "other-worker")
            .await
            .unwrap()
    );
    assert!(
        store
            .release_worker_lease("telegram_delivery", owner)
            .await
            .unwrap()
    );
    assert!(
        store
            .acquire_worker_lease("telegram_delivery", "other-worker", 60)
            .await
            .unwrap()
    );
    let transient_health = store.operational_health(3, 60).await.unwrap();
    assert_eq!(transient_health.status, "degraded");
    assert_eq!(transient_health.sources_never_crawled, 1);
    assert_eq!(transient_health.sources_with_failures, 1);
    assert_eq!(transient_health.sources_alerting, 0);
    assert_eq!(transient_health.pending_deliveries, 0);
    assert!(transient_health.telegram_worker_active);
    for _ in 0..2 {
        make_due(&pool).await;
        let source = claim(&store, owner).await;
        store
            .persist_report(&source, owner, &degraded_report, 60, true, true)
            .await
            .unwrap();
    }
    let source_alert_health = store.operational_health(3, 60).await.unwrap();
    assert_eq!(source_alert_health.status, "degraded");
    assert_eq!(source_alert_health.sources_alerting, 1);
    store
        .upsert_sources(&[SourceSeed {
            key: "baseline-source".to_owned(),
            name: "Baseline Source".to_owned(),
            url: "https://www.facebook.com/baseline.source".to_owned(),
            schedule_interval_seconds: 300,
        }])
        .await
        .unwrap();
    sqlx::query(
        "UPDATE sources SET next_crawl_at = CURRENT_TIMESTAMP + INTERVAL '1 day' \
         WHERE source_key <> 'baseline-source'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let baseline_source = claim(&store, owner).await;
    assert!(baseline_source.initial_crawl);
    let baseline_report = healthy_report_for(
        "baseline.source",
        "sha256:baseline",
        "Mời sinh viên đăng ký tham gia hoạt động điểm rèn luyện.",
    );
    let baseline = store
        .persist_report(&baseline_source, owner, &baseline_report, 300, false, false)
        .await
        .unwrap();
    assert_eq!(baseline.inserted, 1);
    assert_eq!(baseline.outbox_events, 0);
    sqlx::query(
        "UPDATE sources SET next_crawl_at = CURRENT_TIMESTAMP WHERE source_key = 'baseline-source'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let baseline_source = claim(&store, owner).await;
    let changed_baseline_report = healthy_report_for(
        "baseline.source",
        "sha256:baseline-changed",
        "Nội dung lịch sử thay đổi do parser.",
    );
    let changed_baseline = store
        .persist_report(
            &baseline_source,
            owner,
            &changed_baseline_report,
            300,
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(changed_baseline.updated, 1);
    assert_eq!(changed_baseline.outbox_events, 0);
    sqlx::query(
        "UPDATE deliveries SET sent_at = CURRENT_TIMESTAMP - INTERVAL '2 days', \
         updated_at = CURRENT_TIMESTAMP - INTERVAL '2 days'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let delivery_retention = store.apply_delivery_retention(1, 1, 1).await.unwrap();
    assert_eq!(delivery_retention.deliveries_deleted, 1);
    store
        .deactivate_subscriber(123_456_789, "test complete")
        .await
        .unwrap();
    sqlx::query("UPDATE subscribers SET updated_at = CURRENT_TIMESTAMP - INTERVAL '2 days'")
        .execute(&pool)
        .await
        .unwrap();
    let delivery_retention = store.apply_delivery_retention(1, 1, 1).await.unwrap();
    assert_eq!(delivery_retention.inactive_subscribers_deleted, 1);

    sqlx::query("TRUNCATE edge_inbox_events")
        .execute(&pool)
        .await
        .unwrap();
    let later_event = edge_event(11, "/stop");
    let earlier_event = edge_event(10, "/start");
    let imported = store
        .import_edge_events(&[later_event.clone(), earlier_event.clone()])
        .await
        .unwrap();
    assert_eq!(imported.imported, 2);
    let duplicate = store
        .import_edge_events(std::slice::from_ref(&earlier_event))
        .await
        .unwrap();
    assert_eq!(duplicate.duplicates, 1);
    let sequences = sqlx::query_scalar::<_, i64>(
        "SELECT sequence FROM edge_inbox_events ORDER BY aggregate_key, sequence",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(sequences, [10, 11]);
    let pending = store.pending_telegram_edge_events(1).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_id, "telegram:10");
    assert_eq!(pending[0].payload["message"]["text"], "/start");
    store
        .complete_edge_event(&pending[0].event_id)
        .await
        .unwrap();
    let pending = store.pending_telegram_edge_events(100).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_id, "telegram:11");
    assert_eq!(pending[0].attempts, 0);
    assert_eq!(
        store
            .fail_edge_event(&pending[0].event_id, "temporary", 2, 1)
            .await
            .unwrap(),
        FailureDisposition::RetryScheduled
    );
    assert!(
        store
            .pending_telegram_edge_events(100)
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query(
        "UPDATE edge_inbox_events SET available_at = CURRENT_TIMESTAMP WHERE event_id = 'telegram:11'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let pending = store.pending_telegram_edge_events(100).await.unwrap();
    assert_eq!(pending[0].attempts, 1);
    assert_eq!(
        store
            .fail_edge_event(&pending[0].event_id, "poison", 2, 1)
            .await
            .unwrap(),
        FailureDisposition::DeadLettered
    );
    assert!(
        store
            .pending_telegram_edge_events(100)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.apply_edge_inbox_retention(30).await.unwrap(), 0);
    let mut conflicting = earlier_event;
    conflicting.payload["message"]["text"] = serde_json::json!("/stop");
    assert!(store.import_edge_events(&[conflicting]).await.is_err());
}

async fn claim(store: &CrawlStore, owner: &str) -> uth_storage::ClaimedSource {
    store
        .claim_due_sources(owner, 1, 60)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing to a disposable PostgreSQL database"]
async fn portal_notices_reach_stopped_users_and_reuse_uploaded_documents() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let store = CrawlStore::connect(&database_url, 2).await.unwrap();
    store.migrate().await.unwrap();
    sqlx::query(
        "TRUNCATE portal_notice_state, portal_notices, subscribers RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .unwrap();

    store.upsert_subscriber(101, Some("active")).await.unwrap();
    store.upsert_subscriber(102, Some("stopped")).await.unwrap();
    store.upsert_subscriber(103, Some("removed")).await.unwrap();
    assert!(
        store
            .deactivate_subscriber(102, USER_STOP_REASON)
            .await
            .unwrap()
    );
    assert!(
        store
            .deactivate_subscriber(103, "removed by administrator")
            .await
            .unwrap()
    );
    assert!(store.initialize_portal_notice_cursor(100).await.unwrap());
    let cursor_updated_at_before = sqlx::query_scalar::<_, chrono::DateTime<Utc>>(
        "SELECT updated_at FROM portal_notice_state WHERE singleton = TRUE",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let displayed_at = Utc.with_ymd_and_hms(2026, 7, 27, 3, 0, 0).unwrap();
    let next_poll_at = Utc.with_ymd_and_hms(2026, 7, 27, 3, 5, 0).unwrap();
    let burst_until = Utc.with_ymd_and_hms(2026, 7, 27, 3, 20, 0).unwrap();
    store
        .update_portal_poll_state(&PortalPollState {
            mode: "burst".to_owned(),
            next_poll_at,
            burst_until: Some(burst_until),
            cooldown_reason: None,
            last_polled_at: Some(displayed_at),
            last_poll_outcome: Some("new_notices".to_owned()),
            last_http_status: Some(200),
        })
        .await
        .unwrap();
    let poll_state = store.portal_poll_state().await.unwrap().unwrap();
    assert_eq!(poll_state.mode, "burst");
    assert_eq!(poll_state.next_poll_at, next_poll_at);
    assert_eq!(poll_state.burst_until, Some(burst_until));
    let cursor_updated_at_after = sqlx::query_scalar::<_, chrono::DateTime<Utc>>(
        "SELECT updated_at FROM portal_notice_state WHERE singleton = TRUE",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cursor_updated_at_after, cursor_updated_at_before);

    let outcome = store
        .plan_portal_notice(
            &PortalNoticeRecord {
                portal_id: 101,
                title: "Thông báo kiểm thử",
                displayed_at,
                article_url: Some("https://daotao.ut.edu.vn/thong-bao/101"),
                attachment_url: Some("https://portal.ut.edu.vn/api/v1/notification/getFile/101"),
                attachment_file_name: Some("thong-bao.pdf"),
                attachment_content_type: Some("application/pdf"),
            },
            "Thông báo bắt buộc từ Portal UTH",
        )
        .await
        .unwrap();
    assert!(outcome.notice_created);
    assert!(outcome.campaign_created);
    assert_eq!(outcome.deliveries_created, 2);
    assert_eq!(store.portal_notice_cursor().await.unwrap(), Some(101));

    assert!(store.portal_notice_exists(101).await.unwrap());
    assert!(!store.portal_notice_exists(99).await.unwrap());
    assert_eq!(
        store
            .unobserved_portal_notice_ids(&[101, 99, 102])
            .await
            .unwrap(),
        vec![99, 102]
    );

    let duplicate_outcome = store
        .plan_portal_notice(
            &PortalNoticeRecord {
                portal_id: 101,
                title: "Thông báo trùng lặp",
                displayed_at,
                article_url: None,
                attachment_url: None,
                attachment_file_name: None,
                attachment_content_type: None,
            },
            "Thông báo bắt buộc từ Portal UTH",
        )
        .await
        .unwrap();
    assert!(duplicate_outcome.skipped);
    assert!(!duplicate_outcome.notice_created);
    assert!(!duplicate_outcome.campaign_created);

    let out_of_order_outcome = store
        .plan_portal_notice(
            &PortalNoticeRecord {
                portal_id: 99,
                title: "Thông báo ID nhỏ hơn cursor",
                displayed_at,
                article_url: None,
                attachment_url: None,
                attachment_file_name: None,
                attachment_content_type: None,
            },
            "Thông báo bắt buộc từ Portal UTH",
        )
        .await
        .unwrap();
    assert!(out_of_order_outcome.notice_created);
    assert!(out_of_order_outcome.campaign_created);
    assert_eq!(store.portal_notice_cursor().await.unwrap(), Some(101));

    let owner = "portal-test-worker";
    let first_batch = store.claim_deliveries(owner, 10, 60).await.unwrap();
    assert_eq!(first_batch.len(), 1);
    let first = &first_batch[0];
    assert_eq!(first.portal_notice_id, Some(101));
    assert!(matches!(first.telegram_chat_id, 101 | 102));
    assert!(first.telegram_file_id.is_none());
    store
        .complete_delivery(first, owner, 501, Some("telegram-portal-file-id"))
        .await
        .unwrap();

    let second_batch = store.claim_deliveries(owner, 10, 60).await.unwrap();
    assert_eq!(second_batch.len(), 1);
    let second = &second_batch[0];
    assert_eq!(second.portal_notice_id, Some(101));
    assert_ne!(second.telegram_chat_id, first.telegram_chat_id);
    assert_eq!(
        second.telegram_file_id.as_deref(),
        Some("telegram-portal-file-id")
    );
    store
        .complete_delivery(second, owner, 502, None)
        .await
        .unwrap();

    sqlx::query("DELETE FROM deliveries")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE subscribers SET updated_at = CURRENT_TIMESTAMP - INTERVAL '2 days' WHERE NOT active",
    )
    .execute(&pool)
    .await
    .unwrap();
    let retention = store.apply_delivery_retention(1, 1, 1).await.unwrap();
    assert_eq!(retention.inactive_subscribers_deleted, 1);
    let stopped_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM subscribers WHERE telegram_chat_id = 102)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(stopped_exists);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing to a disposable PostgreSQL database"]
async fn donation_payments_are_validated_and_idempotent() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let store = CrawlStore::connect(&database_url, 2).await.unwrap();
    store.migrate().await.unwrap();
    sqlx::query(
        "TRUNCATE donation_amount_input_state, donation_transactions, donation_intents RESTART IDENTITY CASCADE",
    )
        .execute(&pool)
        .await
        .unwrap();
    let amount_input_expires_at = Utc::now() + chrono::Duration::minutes(10);
    store
        .begin_donation_amount_input(123456789, amount_input_expires_at)
        .await
        .unwrap();
    assert!(store.donation_amount_input_active(123456789).await.unwrap());
    assert!(store.clear_donation_amount_input(123456789).await.unwrap());
    assert!(!store.donation_amount_input_active(123456789).await.unwrap());
    let expires_at = Utc.with_ymd_and_hms(2026, 7, 24, 12, 30, 0).unwrap();
    let intent = store
        .create_donation_intent(123456789, 50_000, expires_at)
        .await
        .unwrap();
    store
        .mark_donation_intent_pending(
            intent.order_code,
            &DonationIntentPaymentLink {
                bank_bin: "970418",
                account_number: "V3CASE123456",
                account_name: "HOANG VIET QUANG",
                transfer_description: "PREFIX UTH000001",
                payment_link_id: "payment-link-1",
                checkout_url: "https://pay.payos.vn/web/payment-link-1",
                qr_code: "000201010212",
            },
        )
        .await
        .unwrap();
    let transaction_at = Utc.with_ymd_and_hms(2026, 7, 24, 12, 5, 0).unwrap();
    let payload = serde_json::json!({
        "orderCode": intent.order_code,
        "amount": 50000,
        "reference": "reference-1"
    });
    let first = store
        .record_donation_payment(&DonationPayment {
            order_code: intent.order_code,
            payment_link_id: "payment-link-1",
            reference: "reference-1",
            amount: 50_000,
            currency: "VND",
            transaction_at,
            payload: &payload,
        })
        .await
        .unwrap();
    assert!(first.transaction_created);
    assert!(first.intent_marked_paid);
    assert_eq!(first.telegram_chat_id, 123456789);
    let duplicate = store
        .record_donation_payment(&DonationPayment {
            order_code: intent.order_code,
            payment_link_id: "payment-link-1",
            reference: "reference-1",
            amount: 50_000,
            currency: "VND",
            transaction_at,
            payload: &payload,
        })
        .await
        .unwrap();
    assert!(!duplicate.transaction_created);
    assert!(!duplicate.intent_marked_paid);
    assert!(
        store
            .record_donation_payment(&DonationPayment {
                order_code: intent.order_code,
                payment_link_id: "payment-link-1",
                reference: "reference-2",
                amount: 49_000,
                currency: "VND",
                transaction_at,
                payload: &payload,
            },)
            .await
            .is_err()
    );
}

async fn make_due(pool: &sqlx::PgPool) {
    sqlx::query("UPDATE sources SET next_crawl_at = CURRENT_TIMESTAMP, lease_owner = NULL, lease_expires_at = NULL")
        .execute(pool)
        .await
        .unwrap();
}

fn healthy_report(content_hash: &str, text: &str) -> CrawlReport {
    healthy_report_for("source.a", content_hash, text)
}

fn healthy_report_for(source_name: &str, content_hash: &str, text: &str) -> CrawlReport {
    let fetched_at = Utc::now().to_rfc3339();
    let source_id = format!("facebook:{source_name}");
    let source_url = format!("https://www.facebook.com/{source_name}");
    CrawlReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        source_url,
        source_id: source_id.clone(),
        fetched_at: fetched_at.clone(),
        selected_strategy: Some("standard".to_owned()),
        health: "healthy".to_owned(),
        post_count: 1,
        attempts: Vec::new(),
        posts: vec![FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id,
            platform: "facebook".to_owned(),
            external_post_id: "post-1".to_owned(),
            canonical_url: format!("https://www.facebook.com/{source_name}/posts/post-1"),
            published_at: "2026-07-19T02:00:17+00:00".to_owned(),
            text: text.to_owned(),
            media: vec![MediaItem {
                kind: "image".to_owned(),
                url: "https://example.edu/image.jpg".to_owned(),
                alt_text: None,
            }],
            outbound_links: vec!["https://example.edu/event".to_owned()],
            content_hash: content_hash.to_owned(),
            crawl_strategy: "standard".to_owned(),
            fetched_at,
        }],
        changes: None,
    }
}

fn classification(post: &FacebookPost) -> ClassificationResult {
    let classifier =
        RuleClassifier::from_bytes(include_bytes!("../../../config/classifier-rules.v1.json"))
            .unwrap();
    classifier
        .classify(
            post,
            true,
            Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap(),
        )
        .unwrap()
}

fn edge_event(sequence: i64, text: &str) -> EdgeEvent {
    EdgeEvent {
        schema_version: EDGE_EVENT_SCHEMA_VERSION.to_owned(),
        event_id: format!("telegram:{sequence}"),
        event_type: "telegram.update".to_owned(),
        aggregate_key: "telegram-chat:123456789".to_owned(),
        sequence,
        occurred_at: "2026-07-22T00:00:00Z".to_owned(),
        payload: serde_json::json!({
            "update_id": sequence,
            "message": {
                "chat": {"id": 123456789, "type": "private"},
                "text": text
            }
        }),
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing to a disposable PostgreSQL database"]
async fn ai_learning_examples_and_manual_review_override() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let store = CrawlStore::connect(&database_url, 2).await.unwrap();
    store.migrate().await.unwrap();

    let feedback_id = store
        .record_ai_learning_feedback(AiLearningFeedbackPayload {
            classification_id: None,
            post_id: None,
            post_text: "Tuyển dụng việc làm thêm ngoài trường",
            source_name: "CLB Việc làm",
            ai_decision: "send",
            ai_reason: "Có từ khóa việc làm",
            admin_decision: "skip",
            admin_notes: Some("Không phải đối tác UTH"),
        })
        .await
        .unwrap();
    assert!(feedback_id > 0);

    let examples = store.latest_ai_learning_examples(5).await.unwrap();
    assert!(!examples.is_empty());
    let latest = &examples[0];
    assert_eq!(latest.source_name, "CLB Việc làm");
    assert_eq!(latest.ai_decision, "send");
    assert_eq!(latest.admin_decision, "skip");
    assert_eq!(
        latest.admin_notes.as_deref(),
        Some("Không phải đối tác UTH")
    );

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ai_review_learning_examples")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(count >= 1);

    let override_err = store
        .override_manual_review_resolution(
            999_999,
            123_456_789,
            123_456_789,
            ManualReviewAction::Skip,
            Some("Override test"),
            None,
        )
        .await;
    assert!(override_err.is_err());
    assert!(!ManualReviewOverrideOutcome::default().overridden);
}
