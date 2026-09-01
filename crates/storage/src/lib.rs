use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Postgres, Row, Transaction};
use uth_crawler::facebook::media_url_hash_identity;
use uth_domain::{
    BrowserAttemptMetadata, CLASSIFICATION_SCHEMA_VERSION, ClassificationDecision,
    ClassificationResult, CrawlReport, EDGE_EVENT_SCHEMA_VERSION, EdgeEvent, FacebookPost,
    MediaItem, POST_SCHEMA_VERSION, REPORT_SCHEMA_VERSION, TELEGRAM_MESSAGE_LIMIT,
    TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT,
};

pub const USER_STOP_REASON: &str = "Người dùng tắt bằng lệnh Telegram";

const DIGEST_SUMMARY_LIMIT: usize = 650;
const DIGEST_LINK_LIMIT: usize = 1_000;
const DIGEST_ITEM_FETCH_LIMIT: i64 = 600;
const DIGEST_OVERFLOW_TEXT: &str = "\n\nCác tin còn lại sẽ xuất hiện trong bản tin kế tiếp.";

#[derive(Debug, Clone)]
pub struct SourceSeed {
    pub key: String,
    pub name: String,
    pub url: String,
    pub schedule_interval_seconds: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawlStrategyHealthSample {
    pub strategy: String,
    pub attempts: u64,
    pub healthy: u64,
}

#[derive(Debug, Clone)]
pub struct CrawlHistoryRecord {
    pub run_id: i64,
    pub source_key: String,
    pub source_name: String,
    pub fetched_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub health: String,
    pub selected_strategy: Option<String>,
    pub post_count: i32,
    pub attempt_count: i64,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CrawlAttemptHistoryRecord {
    pub ordinal: i32,
    pub strategy: String,
    pub outcome: String,
    pub status: Option<i32>,
    pub latency_ms: i64,
    pub bytes_received: i64,
    pub posts_found: i32,
    pub newest_post_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub browser: Option<BrowserAttemptMetadata>,
}

#[derive(Debug, Clone)]
pub struct CrawlHistoryDetail {
    pub run: CrawlHistoryRecord,
    pub attempts: Vec<CrawlAttemptHistoryRecord>,
}

#[derive(Debug, Clone)]
pub struct ClaimedSource {
    pub id: i64,
    pub key: String,
    pub name: String,
    pub url: String,
    pub failure_count: u32,
    pub schedule_interval_seconds: u64,
    pub unchanged_crawl_count: u32,
    pub initial_crawl: bool,
    pub reconciliation_required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveSchedule {
    pub active_interval_seconds: u64,
    pub idle_interval_seconds: u64,
    pub active_unchanged_crawls: u32,
    pub idle_after_unchanged_crawls: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduledPersistOutcome {
    pub persistence: PersistOutcome,
    pub next_delay_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledPersistOptions {
    pub next_delay_seconds: u64,
    pub emit_post_events: bool,
    pub allow_historical_events: bool,
    pub adaptive_schedule: Option<AdaptiveSchedule>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistOutcome {
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    pub outbox_events: usize,
}

#[derive(Debug, Clone)]
pub struct ClaimedClassificationEvent {
    pub id: i64,
    pub event_key: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub attempts: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassificationPersistOutcome {
    pub classification_inserted: bool,
    pub completion_event_inserted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeImportOutcome {
    pub imported: usize,
    pub duplicates: usize,
}

#[derive(Debug, Clone)]
pub struct PendingEdgeEvent {
    pub event_id: String,
    pub payload: serde_json::Value,
    pub attempts: u32,
}

#[derive(Debug, Clone)]
pub struct DonationIntent {
    pub order_code: i64,
    pub amount: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DonationPaymentOutcome {
    pub transaction_created: bool,
    pub intent_marked_paid: bool,
    pub telegram_chat_id: i64,
}

#[derive(Debug, Clone)]
pub struct DonationPayment<'a> {
    pub order_code: i64,
    pub payment_link_id: &'a str,
    pub reference: &'a str,
    pub amount: i64,
    pub currency: &'a str,
    pub transaction_at: DateTime<Utc>,
    pub payload: &'a serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct DonationIntentPaymentLink<'a> {
    pub bank_bin: &'a str,
    pub account_number: &'a str,
    pub account_name: &'a str,
    pub transfer_description: &'a str,
    pub payment_link_id: &'a str,
    pub checkout_url: &'a str,
    pub qr_code: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDisposition {
    RetryScheduled,
    DeadLettered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryFailureClass {
    RecipientUnavailable,
    RetryExhausted,
    RequestRejected,
}

impl DeliveryFailureClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::RecipientUnavailable => "recipient_unavailable",
            Self::RetryExhausted => "retry_exhausted",
            Self::RequestRejected => "request_rejected",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifierRetentionOutcome {
    pub dead_letters_deleted: u64,
    pub processed_outbox_events_deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberRecord {
    pub telegram_chat_id: i64,
    pub display_name: Option<String>,
    pub active: bool,
    pub deactivated_reason: Option<String>,
    pub acquisition_source: Option<String>,
    pub onboarding_completed: bool,
    pub notification_scope: String,
    pub delivery_mode: String,
    pub quiet_hours_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    pub key: String,
    pub name: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSuggestionRecord {
    pub id: i64,
    pub telegram_chat_id: i64,
    pub submitted_url: String,
    pub status: String,
    pub admin_note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub reviewed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSuggestionOutcome {
    pub id: i64,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserFeedbackRecord {
    pub id: i64,
    pub telegram_chat_id: i64,
    pub sender_label: String,
    pub message: String,
    pub attempts: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserFeedbackHistoryRecord {
    pub id: i64,
    pub telegram_chat_id: i64,
    pub sender_label: String,
    pub message: String,
    pub admin_notified_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualReviewRecord {
    pub classification_id: i64,
    pub database_post_id: i64,
    pub source_name: String,
    pub score: i32,
    pub confidence_basis_points: u16,
    pub matched_rules: Vec<String>,
    pub classified_at: DateTime<Utc>,
    pub post: FacebookPost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestPostRecord {
    pub database_post_id: i64,
    pub source_name: String,
    pub post: FacebookPost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualReviewAction {
    Send,
    Skip,
}

#[derive(Debug, Clone, Copy)]
pub struct ManualReviewNotification<'a> {
    pub message_text: &'a str,
    pub post_url: &'a str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualReviewResolutionOutcome {
    pub resolved: bool,
    pub campaign_created: bool,
    pub deliveries_created: u64,
}

#[derive(Debug, Clone)]
pub struct ClaimedNotificationEvent {
    pub id: i64,
    pub event_key: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct NotificationContent<'a> {
    pub message_text: &'a str,
    pub post_url: Option<&'a str>,
    pub action_url: Option<&'a str>,
    pub explicit_drl: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationPlanOutcome {
    pub campaign_created: bool,
    pub deliveries_created: u64,
    pub skipped: bool,
}

#[derive(Debug, Clone)]
pub struct ClaimedDelivery {
    pub id: i64,
    pub campaign_id: i64,
    pub subscriber_id: i64,
    pub telegram_chat_id: i64,
    pub message_text: String,
    pub post_url: Option<String>,
    pub action_url: Option<String>,
    pub portal_notice_id: Option<i64>,
    pub attachment_url: Option<String>,
    pub attachment_file_name: Option<String>,
    pub attachment_content_type: Option<String>,
    pub telegram_file_id: Option<String>,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub struct PortalNoticeRecord<'a> {
    pub portal_id: i64,
    pub title: &'a str,
    pub displayed_at: DateTime<Utc>,
    pub article_url: Option<&'a str>,
    pub attachment_url: Option<&'a str>,
    pub attachment_file_name: Option<&'a str>,
    pub attachment_content_type: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct PortalNoticeHistoryRecord {
    pub portal_id: i64,
    pub title: String,
    pub displayed_at: DateTime<Utc>,
    pub article_url: Option<String>,
    pub attachment_url: Option<String>,
    pub attachment_content_type: Option<String>,
    pub discovered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortalPollState {
    pub mode: String,
    pub next_poll_at: DateTime<Utc>,
    pub burst_until: Option<DateTime<Utc>>,
    pub cooldown_reason: Option<String>,
    pub last_polled_at: Option<DateTime<Utc>>,
    pub last_poll_outcome: Option<String>,
    pub last_http_status: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PortalNoticePlanOutcome {
    pub notice_created: bool,
    pub campaign_created: bool,
    pub deliveries_created: u64,
    pub skipped: bool,
}

#[derive(Debug, Clone)]
pub struct ClaimedDigestDelivery {
    pub id: i64,
    pub subscriber_id: i64,
    pub telegram_chat_id: i64,
    pub message_text: String,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DigestPreparationOutcome {
    pub batches_created: u64,
    pub items_batched: u64,
    pub duplicate_items_collapsed: u64,
}

#[derive(Debug, Clone)]
struct DigestCandidate {
    campaign_id: i64,
    post_id: i64,
    summary: String,
    link: Option<String>,
}

#[derive(Debug, Clone)]
struct DigestEntry {
    campaign_ids: Vec<i64>,
    summary: String,
    link: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DigestBatchPlan {
    campaign_ids: Vec<i64>,
    message_text: String,
    duplicate_items_collapsed: usize,
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    if limit <= 1 {
        return "…".chars().take(limit).collect();
    }
    let mut output = value.chars().take(limit - 1).collect::<String>();
    output.push('…');
    output
}

fn digest_header(item_count: usize) -> String {
    format!("Bản tin hoạt động lúc 07:30: {item_count} tin mới")
}

fn render_digest_entry(item_number: usize, entry: &DigestEntry) -> String {
    match entry.link.as_deref() {
        Some(link) => format!("\n\n{item_number}. {}\n{link}", entry.summary),
        None => format!("\n\n{item_number}. {}", entry.summary),
    }
}

fn build_digest_batch(
    candidates: Vec<DigestCandidate>,
    has_unloaded_items: bool,
) -> Result<Option<DigestBatchPlan>> {
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut identities: HashMap<i64, usize> = HashMap::with_capacity(candidates.len());
    let mut entries: Vec<DigestEntry> = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if let Some(index) = identities.get(&candidate.post_id).copied() {
            entries[index].campaign_ids.push(candidate.campaign_id);
            entries[index].summary = candidate.summary;
            entries[index].link = candidate.link;
        } else {
            let index = entries.len();
            identities.insert(candidate.post_id, index);
            entries.push(DigestEntry {
                campaign_ids: vec![candidate.campaign_id],
                summary: candidate.summary,
                link: candidate.link,
            });
        }
    }

    let mut selected_count = 0_usize;
    let mut rendered_entries = String::new();
    for entry in &entries {
        let next_count = selected_count + 1;
        let rendered_entry = render_digest_entry(next_count, entry);
        let has_remaining = next_count < entries.len() || has_unloaded_items;
        let candidate_length = digest_header(next_count).chars().count()
            + rendered_entries.chars().count()
            + rendered_entry.chars().count()
            + if has_remaining {
                DIGEST_OVERFLOW_TEXT.chars().count()
            } else {
                0
            };
        if candidate_length > TELEGRAM_MESSAGE_LIMIT {
            break;
        }
        rendered_entries.push_str(&rendered_entry);
        selected_count = next_count;
    }
    if selected_count == 0 {
        bail!("a bounded digest entry exceeds the Telegram message limit");
    }

    let has_remaining = selected_count < entries.len() || has_unloaded_items;
    let mut message_text = format!("{}{}", digest_header(selected_count), rendered_entries);
    if has_remaining {
        message_text.push_str(DIGEST_OVERFLOW_TEXT);
    }
    if message_text.chars().count() > TELEGRAM_MESSAGE_LIMIT {
        bail!("digest builder exceeded the Telegram message limit");
    }
    if message_text.len() > TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT {
        bail!("digest builder exceeded the UTF-8 storage limit");
    }

    let selected_entries = &entries[..selected_count];
    let campaign_ids = selected_entries
        .iter()
        .flat_map(|entry| entry.campaign_ids.iter().copied())
        .collect::<Vec<_>>();
    let duplicate_items_collapsed = campaign_ids.len().saturating_sub(selected_count);
    Ok(Some(DigestBatchPlan {
        campaign_ids,
        message_text,
        duplicate_items_collapsed,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackOutcome {
    pub first_response: bool,
    pub should_prompt_donation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrowthMetrics {
    pub starts_7d: i64,
    pub onboarding_completed_7d: i64,
    pub active_subscribers: i64,
    pub notifications_delivered_7d: i64,
    pub cta_clicks_7d: i64,
    pub useful_feedback_7d: i64,
    pub irrelevant_feedback_7d: i64,
    pub donations_paid_7d: i64,
    pub donation_amount_7d: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRetentionOutcome {
    pub deliveries_deleted: u64,
    pub inactive_subscribers_deleted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalHealth {
    pub schema_version: String,
    pub generated_at: DateTime<Utc>,
    pub status: String,
    pub enabled_sources: i64,
    pub sources_never_crawled: i64,
    pub stale_sources: i64,
    pub sources_with_failures: i64,
    pub sources_alerting: i64,
    pub pending_classification_events: i64,
    pub oldest_classification_event_age_seconds: Option<i64>,
    pub pending_notification_events: i64,
    pub oldest_notification_event_age_seconds: Option<i64>,
    pub dead_letters: i64,
    pub pending_deliveries: i64,
    pub oldest_pending_delivery_age_seconds: Option<i64>,
    pub failed_deliveries: i64,
    pub pending_digest_batches: i64,
    pub failed_digest_batches: i64,
    pub pending_edge_events: i64,
    pub dead_lettered_edge_events: i64,
    pub pending_donation_intents: i64,
    pub failed_donation_intents: i64,
    pub active_subscribers: i64,
    pub pending_source_suggestions: i64,
    pub pending_manual_reviews: i64,
    pub telegram_worker_active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalAlertKind {
    Degraded,
    Failed,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalAlertCandidate {
    pub alert_key: String,
    pub observed_status: String,
    pub kind: OperationalAlertKind,
}

#[derive(Clone)]
pub struct CrawlStore {
    pool: PgPool,
}

impl CrawlStore {
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        if max_connections == 0 {
            bail!("max database connections must be at least 1");
        }
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .context("failed to connect to PostgreSQL")?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("../../migrations")
            .run(&self.pool)
            .await
            .context("failed to apply PostgreSQL migrations")
    }

    pub async fn import_edge_events(&self, events: &[EdgeEvent]) -> Result<EdgeImportOutcome> {
        if events.is_empty() {
            return Ok(EdgeImportOutcome::default());
        }
        let mut transaction = self.pool.begin().await?;
        let mut outcome = EdgeImportOutcome::default();
        for event in events {
            event.validate().map_err(anyhow::Error::msg)?;
            if event.schema_version != EDGE_EVENT_SCHEMA_VERSION {
                bail!("unsupported edge event schema {}", event.schema_version);
            }
            let occurred_at = parse_timestamp(&event.occurred_at, "edge event occurred_at")?;
            let inserted = sqlx::query(
                "INSERT INTO edge_inbox_events \
                 (event_id, schema_version, event_type, aggregate_key, sequence, occurred_at, payload) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(&event.event_id)
            .bind(&event.schema_version)
            .bind(&event.event_type)
            .bind(&event.aggregate_key)
            .bind(event.sequence)
            .bind(occurred_at)
            .bind(&event.payload)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if inserted == 0 {
                let identical = sqlx::query_scalar::<_, bool>(
                    "SELECT schema_version = $2 AND event_type = $3 AND aggregate_key = $4 \
                            AND sequence = $5 AND occurred_at = $6 AND payload = $7 \
                     FROM edge_inbox_events WHERE event_id = $1",
                )
                .bind(&event.event_id)
                .bind(&event.schema_version)
                .bind(&event.event_type)
                .bind(&event.aggregate_key)
                .bind(event.sequence)
                .bind(occurred_at)
                .bind(&event.payload)
                .fetch_optional(&mut *transaction)
                .await?
                .unwrap_or(false);
                if !identical {
                    bail!("edge event ID conflicts with different content");
                }
                outcome.duplicates += 1;
                continue;
            }
            outcome.imported += 1;
        }
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn pending_telegram_edge_events(
        &self,
        limit: usize,
    ) -> Result<Vec<PendingEdgeEvent>> {
        if limit == 0 || limit > 100 {
            bail!("edge inbox batch size must be between 1 and 100");
        }
        let limit = i64::try_from(limit)?;
        let rows = sqlx::query(
            "SELECT event.event_id, event.payload, event.attempts FROM edge_inbox_events AS event \
             WHERE event.event_type = 'telegram.update' AND event.processed_at IS NULL \
               AND event.dead_lettered_at IS NULL AND event.available_at <= CURRENT_TIMESTAMP \
               AND NOT EXISTS (SELECT 1 FROM edge_inbox_events AS earlier \
                   WHERE earlier.event_type = event.event_type \
                     AND earlier.aggregate_key = event.aggregate_key \
                     AND earlier.sequence < event.sequence \
                     AND earlier.processed_at IS NULL AND earlier.dead_lettered_at IS NULL) \
             ORDER BY event.imported_at, event.sequence LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(PendingEdgeEvent {
                    event_id: row.try_get("event_id")?,
                    payload: row.try_get("payload")?,
                    attempts: u32::try_from(row.try_get::<i32, _>("attempts")?)?,
                })
            })
            .collect()
    }

    pub async fn pending_payos_edge_events(&self, limit: usize) -> Result<Vec<PendingEdgeEvent>> {
        if limit == 0 || limit > 100 {
            bail!("edge inbox batch size must be between 1 and 100");
        }
        let rows = sqlx::query(
            "SELECT event.event_id, event.payload, event.attempts FROM edge_inbox_events AS event \
             WHERE event.event_type = 'payos.payment' AND event.processed_at IS NULL \
               AND event.dead_lettered_at IS NULL AND event.available_at <= CURRENT_TIMESTAMP \
               AND NOT EXISTS (SELECT 1 FROM edge_inbox_events AS earlier \
                   WHERE earlier.event_type = event.event_type \
                     AND earlier.aggregate_key = event.aggregate_key \
                     AND earlier.sequence < event.sequence \
                     AND earlier.processed_at IS NULL AND earlier.dead_lettered_at IS NULL) \
             ORDER BY event.imported_at, event.sequence LIMIT $1",
        )
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(PendingEdgeEvent {
                    event_id: row.try_get("event_id")?,
                    payload: row.try_get("payload")?,
                    attempts: u32::try_from(row.try_get::<i32, _>("attempts")?)?,
                })
            })
            .collect()
    }

    pub async fn create_donation_intent(
        &self,
        telegram_chat_id: i64,
        amount: i64,
        expires_at: DateTime<Utc>,
    ) -> Result<DonationIntent> {
        if telegram_chat_id == 0 || !(10_000..=10_000_000).contains(&amount) {
            bail!("donation intent values are outside safe bounds");
        }
        let row = sqlx::query(
            "INSERT INTO donation_intents \
             (telegram_chat_id, amount, status, expires_at) \
             VALUES ($1, $2, 'creating', $3) \
             RETURNING order_code, amount, expires_at",
        )
        .bind(telegram_chat_id)
        .bind(amount)
        .bind(expires_at)
        .fetch_one(&self.pool)
        .await?;
        self.record_product_event(
            telegram_chat_id,
            "donation_intent_created",
            None,
            json!({"amount": amount}),
        )
        .await?;
        Ok(DonationIntent {
            order_code: row.try_get("order_code")?,
            amount: row.try_get("amount")?,
            expires_at: row.try_get("expires_at")?,
        })
    }

    pub async fn begin_donation_amount_input(
        &self,
        telegram_chat_id: i64,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        if telegram_chat_id == 0 || expires_at <= Utc::now() {
            bail!("donation amount input values are invalid");
        }
        sqlx::query(
            "INSERT INTO donation_amount_input_state (telegram_chat_id, expires_at) \
             VALUES ($1, $2) \
             ON CONFLICT (telegram_chat_id) DO UPDATE SET \
                 expires_at = EXCLUDED.expires_at, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(telegram_chat_id)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn donation_amount_input_active(&self, telegram_chat_id: i64) -> Result<bool> {
        if telegram_chat_id == 0 {
            bail!("Telegram chat ID must be non-zero");
        }
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM donation_amount_input_state \
             WHERE telegram_chat_id = $1 AND expires_at > CURRENT_TIMESTAMP)",
        )
        .bind(telegram_chat_id)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn clear_donation_amount_input(&self, telegram_chat_id: i64) -> Result<bool> {
        if telegram_chat_id == 0 {
            bail!("Telegram chat ID must be non-zero");
        }
        Ok(
            sqlx::query("DELETE FROM donation_amount_input_state WHERE telegram_chat_id = $1")
                .bind(telegram_chat_id)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
    }

    pub async fn begin_user_feedback_input(
        &self,
        telegram_chat_id: i64,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        if telegram_chat_id == 0 || expires_at <= Utc::now() {
            bail!("user feedback input values are invalid");
        }
        sqlx::query(
            "INSERT INTO user_feedback_input_state (telegram_chat_id, expires_at) \
             VALUES ($1, $2) \
             ON CONFLICT (telegram_chat_id) DO UPDATE SET \
                 expires_at = EXCLUDED.expires_at, created_at = CURRENT_TIMESTAMP",
        )
        .bind(telegram_chat_id)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn user_feedback_input_active(&self, telegram_chat_id: i64) -> Result<bool> {
        if telegram_chat_id == 0 {
            bail!("Telegram chat ID must be non-zero");
        }
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM user_feedback_input_state \
             WHERE telegram_chat_id = $1 AND expires_at > CURRENT_TIMESTAMP)",
        )
        .bind(telegram_chat_id)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn clear_user_feedback_input(&self, telegram_chat_id: i64) -> Result<bool> {
        if telegram_chat_id == 0 {
            bail!("Telegram chat ID must be non-zero");
        }
        Ok(
            sqlx::query("DELETE FROM user_feedback_input_state WHERE telegram_chat_id = $1")
                .bind(telegram_chat_id)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
    }

    pub async fn record_user_feedback(
        &self,
        telegram_chat_id: i64,
        telegram_update_id: i64,
        sender_label: &str,
        message: &str,
    ) -> Result<i64> {
        let sender_label = sender_label.trim();
        let message = message.trim();
        if telegram_chat_id == 0
            || telegram_update_id < 0
            || sender_label.is_empty()
            || sender_label.chars().count() > 256
            || sender_label.contains('\0')
            || message.is_empty()
            || message.chars().count() > 2_000
            || message.contains('\0')
        {
            bail!("user feedback values are invalid");
        }
        let mut transaction = self.pool.begin().await?;
        if let Some(id) = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM user_feedback_messages \
             WHERE telegram_chat_id = $1 AND telegram_update_id = $2",
        )
        .bind(telegram_chat_id)
        .bind(telegram_update_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            sqlx::query("DELETE FROM user_feedback_input_state WHERE telegram_chat_id = $1")
                .bind(telegram_chat_id)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
            return Ok(id);
        }
        let recent_count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM user_feedback_messages \
             WHERE telegram_chat_id = $1 AND created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'",
        )
        .bind(telegram_chat_id)
        .fetch_one(&mut *transaction)
        .await?;
        if recent_count >= 5 {
            bail!("user feedback rate limit exceeded");
        }
        let inserted_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO user_feedback_messages \
             (subscriber_id, telegram_chat_id, telegram_update_id, sender_label, message) \
             VALUES ((SELECT id FROM subscribers WHERE telegram_chat_id = $1), $1, $2, $3, $4) \
             ON CONFLICT (telegram_chat_id, telegram_update_id) \
                 WHERE telegram_update_id IS NOT NULL DO NOTHING \
             RETURNING id",
        )
        .bind(telegram_chat_id)
        .bind(telegram_update_id)
        .bind(sender_label)
        .bind(message)
        .fetch_optional(&mut *transaction)
        .await?;
        let (id, inserted) = match inserted_id {
            Some(id) => (id, true),
            None => {
                let id = sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM user_feedback_messages \
                     WHERE telegram_chat_id = $1 AND telegram_update_id = $2",
                )
                .bind(telegram_chat_id)
                .bind(telegram_update_id)
                .fetch_one(&mut *transaction)
                .await?;
                (id, false)
            }
        };
        sqlx::query("DELETE FROM user_feedback_input_state WHERE telegram_chat_id = $1")
            .bind(telegram_chat_id)
            .execute(&mut *transaction)
            .await?;
        if inserted {
            sqlx::query(
                "INSERT INTO product_events (subscriber_id, event_type, metadata) \
                 SELECT id, 'user_feedback_submitted', jsonb_build_object('feedback_id', $2) \
                 FROM subscribers WHERE telegram_chat_id = $1",
            )
            .bind(telegram_chat_id)
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(id)
    }

    pub async fn pending_user_feedback(&self, limit: usize) -> Result<Vec<UserFeedbackRecord>> {
        if limit == 0 || limit > 100 {
            bail!("user feedback batch size must be between 1 and 100");
        }
        let rows = sqlx::query(
            "SELECT id, telegram_chat_id, sender_label, message, attempts FROM user_feedback_messages \
             WHERE admin_notified_at IS NULL ORDER BY created_at, id LIMIT $1",
        )
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(UserFeedbackRecord {
                    id: row.try_get("id")?,
                    telegram_chat_id: row.try_get("telegram_chat_id")?,
                    sender_label: row.try_get("sender_label")?,
                    message: row.try_get("message")?,
                    attempts: row.try_get("attempts")?,
                })
            })
            .collect()
    }

    pub async fn user_feedback_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(i64, Vec<UserFeedbackHistoryRecord>)> {
        if limit == 0 || limit > 100 {
            bail!("user feedback history batch size must be between 1 and 100");
        }
        let total = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM user_feedback_messages")
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(
            "SELECT id, telegram_chat_id, sender_label, message, admin_notified_at, created_at \
             FROM user_feedback_messages ORDER BY created_at DESC, id DESC LIMIT $1 OFFSET $2",
        )
        .bind(i64::try_from(limit)?)
        .bind(i64::try_from(offset)?)
        .fetch_all(&self.pool)
        .await?;
        let records = rows
            .into_iter()
            .map(|row| UserFeedbackHistoryRecord {
                id: row.get("id"),
                telegram_chat_id: row.get("telegram_chat_id"),
                sender_label: row.get("sender_label"),
                message: row.get("message"),
                admin_notified_at: row.get("admin_notified_at"),
                created_at: row.get("created_at"),
            })
            .collect();
        Ok((total, records))
    }

    pub async fn mark_user_feedback_notified(&self, id: i64) -> Result<bool> {
        if id <= 0 {
            bail!("user feedback ID must be positive");
        }
        Ok(sqlx::query(
            "UPDATE user_feedback_messages SET admin_notified_at = CURRENT_TIMESTAMP, \
                    updated_at = CURRENT_TIMESTAMP, last_error = NULL \
                 WHERE id = $1 AND admin_notified_at IS NULL",
        )
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1)
    }

    pub async fn mark_user_feedback_attempt(&self, id: i64, detail: &str) -> Result<()> {
        if id <= 0 {
            bail!("user feedback ID must be positive");
        }
        let detail = detail.chars().take(1_000).collect::<String>();
        sqlx::query(
            "UPDATE user_feedback_messages SET attempts = attempts + 1, last_error = $2, \
                updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND admin_notified_at IS NULL",
        )
        .bind(id)
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_donation_intent_pending(
        &self,
        order_code: i64,
        payment: &DonationIntentPaymentLink<'_>,
    ) -> Result<()> {
        if payment.bank_bin.is_empty()
            || payment.bank_bin.len() > 20
            || payment.account_number.is_empty()
            || payment.account_number.len() > 100
            || payment.account_name.is_empty()
            || payment.account_name.chars().count() > 200
            || payment.transfer_description.is_empty()
            || payment.transfer_description.chars().count() > 200
            || payment.payment_link_id.is_empty()
            || payment.payment_link_id.len() > 200
            || payment.checkout_url.is_empty()
            || payment.checkout_url.len() > 2048
            || payment.qr_code.is_empty()
            || payment.qr_code.len() > 4096
        {
            bail!("payment link values are outside safe bounds");
        }
        let updated = sqlx::query(
            "UPDATE donation_intents \
             SET status = 'pending', bank_bin = $2, account_number = $3, account_name = $4, \
                 transfer_description = $5, payment_link_id = $6, checkout_url = $7, qr_code = $8, \
                 updated_at = CURRENT_TIMESTAMP, failure_detail = NULL \
             WHERE order_code = $1 AND status = 'creating'",
        )
        .bind(order_code)
        .bind(payment.bank_bin)
        .bind(payment.account_number)
        .bind(payment.account_name)
        .bind(payment.transfer_description)
        .bind(payment.payment_link_id)
        .bind(payment.checkout_url)
        .bind(payment.qr_code)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if updated != 1 {
            bail!("donation intent was not in creating state");
        }
        Ok(())
    }

    pub async fn mark_donation_intent_failed(
        &self,
        order_code: i64,
        failure_detail: &str,
    ) -> Result<()> {
        let detail = failure_detail.chars().take(1000).collect::<String>();
        let updated = sqlx::query(
            "UPDATE donation_intents \
             SET status = 'failed', failure_detail = $2, updated_at = CURRENT_TIMESTAMP \
             WHERE order_code = $1 AND status = 'creating'",
        )
        .bind(order_code)
        .bind(detail)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if updated != 1 {
            bail!("donation intent was not in creating state");
        }
        Ok(())
    }

    pub async fn record_donation_payment(
        &self,
        payment: &DonationPayment<'_>,
    ) -> Result<DonationPaymentOutcome> {
        if payment.order_code <= 0
            || payment.payment_link_id.is_empty()
            || payment.payment_link_id.len() > 200
            || payment.reference.is_empty()
            || payment.reference.len() > 200
            || payment.amount <= 0
            || payment.currency != "VND"
            || !payment.payload.is_object()
        {
            bail!("payOS payment values are invalid");
        }
        let mut transaction = self.pool.begin().await?;
        let intent = sqlx::query(
            "SELECT telegram_chat_id, amount, payment_link_id, status FROM donation_intents \
             WHERE order_code = $1 FOR UPDATE",
        )
        .bind(payment.order_code)
        .fetch_optional(&mut *transaction)
        .await?
        .context("payOS payment does not match a donation intent")?;
        let expected_amount: i64 = intent.try_get("amount")?;
        let telegram_chat_id: i64 = intent.try_get("telegram_chat_id")?;
        let expected_link: Option<String> = intent.try_get("payment_link_id")?;
        let status: String = intent.try_get("status")?;
        if expected_amount != payment.amount
            || expected_link.as_deref() != Some(payment.payment_link_id)
        {
            bail!("payOS payment does not match the intent amount or payment link");
        }
        let inserted = sqlx::query(
            "INSERT INTO donation_transactions \
             (reference, order_code, payment_link_id, amount, currency, transaction_at, payload) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (reference) DO NOTHING",
        )
        .bind(payment.reference)
        .bind(payment.order_code)
        .bind(payment.payment_link_id)
        .bind(payment.amount)
        .bind(payment.currency)
        .bind(payment.transaction_at)
        .bind(payment.payload)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        if !inserted {
            let identical = sqlx::query_scalar::<_, bool>(
                "SELECT order_code = $2 AND payment_link_id = $3 AND amount = $4 \
                        AND currency = $5 AND transaction_at = $6 \
                 FROM donation_transactions WHERE reference = $1",
            )
            .bind(payment.reference)
            .bind(payment.order_code)
            .bind(payment.payment_link_id)
            .bind(payment.amount)
            .bind(payment.currency)
            .bind(payment.transaction_at)
            .fetch_one(&mut *transaction)
            .await?;
            if !identical {
                bail!("payOS transaction reference conflicts with different content");
            }
        }
        let marked_paid = if status == "paid" {
            false
        } else {
            sqlx::query(
                "UPDATE donation_intents \
                 SET status = 'paid', paid_at = $2, updated_at = CURRENT_TIMESTAMP \
                 WHERE order_code = $1 AND status IN ('creating', 'pending')",
            )
            .bind(payment.order_code)
            .bind(payment.transaction_at)
            .execute(&mut *transaction)
            .await?
            .rows_affected()
                == 1
        };
        if inserted {
            let subscriber_id = sqlx::query_scalar::<_, i64>(
                "SELECT id FROM subscribers WHERE telegram_chat_id = $1",
            )
            .bind(telegram_chat_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(subscriber_id) = subscriber_id {
                sqlx::query(
                    "INSERT INTO product_events (subscriber_id, event_type, metadata) VALUES ($1, 'donation_paid', $2)",
                )
                .bind(subscriber_id)
                .bind(json!({"amount": payment.amount, "currency": payment.currency}))
                .execute(&mut *transaction)
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(DonationPaymentOutcome {
            transaction_created: inserted,
            intent_marked_paid: marked_paid,
            telegram_chat_id,
        })
    }

    pub async fn complete_edge_event(&self, event_id: &str) -> Result<()> {
        if event_id.is_empty() {
            bail!("edge event ID must not be empty");
        }
        let updated = sqlx::query(
            "UPDATE edge_inbox_events SET processed_at = CURRENT_TIMESTAMP, last_error = NULL \
             WHERE event_id = $1 AND processed_at IS NULL",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if updated != 1 {
            bail!("edge event was not pending");
        }
        Ok(())
    }

    pub async fn reject_edge_event(&self, event_id: &str, error: &str) -> Result<()> {
        if event_id.is_empty() {
            bail!("edge event ID must not be empty");
        }
        let error = error.chars().take(1000).collect::<String>();
        let updated = sqlx::query(
            "UPDATE edge_inbox_events \
             SET processed_at = CURRENT_TIMESTAMP, last_error = $2 \
             WHERE event_id = $1 AND processed_at IS NULL",
        )
        .bind(event_id)
        .bind(error)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if updated != 1 {
            bail!("edge event was not pending");
        }
        Ok(())
    }

    pub async fn fail_edge_event(
        &self,
        event_id: &str,
        error: &str,
        max_attempts: u32,
        retry_delay_seconds: u64,
    ) -> Result<FailureDisposition> {
        if event_id.is_empty() || max_attempts == 0 || retry_delay_seconds == 0 {
            bail!("edge event failure values are invalid");
        }
        let error = error.chars().take(1_000).collect::<String>();
        let row = sqlx::query(
            "UPDATE edge_inbox_events SET attempts = attempts + 1, last_error = $2, \
                available_at = CURRENT_TIMESTAMP + make_interval(secs => $4::double precision), \
                dead_lettered_at = CASE WHEN attempts + 1 >= $3 THEN CURRENT_TIMESTAMP ELSE NULL END, \
                processed_at = CASE WHEN attempts + 1 >= $3 THEN CURRENT_TIMESTAMP ELSE NULL END \
             WHERE event_id = $1 AND processed_at IS NULL AND dead_lettered_at IS NULL \
             RETURNING dead_lettered_at IS NOT NULL AS dead_lettered",
        )
        .bind(event_id)
        .bind(error)
        .bind(i32::try_from(max_attempts)?)
        .bind(i64::try_from(retry_delay_seconds)?)
        .fetch_optional(&self.pool)
        .await?
        .context("edge event was not pending")?;
        Ok(if row.get("dead_lettered") {
            FailureDisposition::DeadLettered
        } else {
            FailureDisposition::RetryScheduled
        })
    }

    pub async fn apply_edge_inbox_retention(&self, retention_days: i32) -> Result<u64> {
        if retention_days < 1 {
            bail!("edge inbox retention must be at least one day");
        }
        let deleted = sqlx::query(
            "DELETE FROM edge_inbox_events WHERE event_id IN (\
                SELECT event_id FROM edge_inbox_events \
                WHERE processed_at < CURRENT_TIMESTAMP - make_interval(days => $1) \
                ORDER BY processed_at LIMIT 1000\
             )",
        )
        .bind(retention_days)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(deleted)
    }

    pub async fn operational_health(
        &self,
        alert_after_failures: u32,
        backlog_stale_seconds: u64,
    ) -> Result<OperationalHealth> {
        if alert_after_failures == 0 || backlog_stale_seconds == 0 {
            bail!("health thresholds must be at least 1");
        }
        let alert_after_failures = i32::try_from(alert_after_failures)?;
        let backlog_stale_seconds = i64::try_from(backlog_stale_seconds)?;
        let row = sqlx::query(
            "SELECT CURRENT_TIMESTAMP AS generated_at, \
                (SELECT count(*) FROM sources WHERE enabled) AS enabled_sources, \
                (SELECT count(*) FROM sources AS source WHERE source.enabled AND NOT EXISTS ( \
                    SELECT 1 FROM crawler_runs AS run WHERE run.source_id = source.id \
                )) AS sources_never_crawled, \
                (SELECT count(*) FROM sources WHERE enabled AND \
                    next_crawl_at < CURRENT_TIMESTAMP - make_interval(secs => GREATEST( \
                        schedule_interval_seconds::bigint * 2, $2::bigint \
                    )::double precision)) AS stale_sources, \
                (SELECT count(*) FROM sources WHERE enabled AND failure_count > 0) \
                    AS sources_with_failures, \
                (SELECT count(*) FROM sources AS source WHERE source.enabled AND ( \
                    source.failure_count >= $1 OR ( \
                        SELECT count(*) FILTER (WHERE run.health <> 'healthy') >= $1 \
                            AND count(*) FILTER (WHERE run.health <> 'healthy') * 2 >= count(*) \
                        FROM crawler_runs AS run \
                        WHERE run.source_id = source.id AND \
                            run.created_at >= CURRENT_TIMESTAMP - \
                                make_interval(secs => $2::double precision) \
                    ) \
                )) AS sources_alerting, \
                (SELECT count(*) FROM outbox_events WHERE processed_at IS NULL AND \
                    event_type IN ('facebook_post.discovered', 'facebook_post.updated')) \
                    AS pending_classification_events, \
                (SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - min(created_at)))::bigint \
                    FROM outbox_events WHERE processed_at IS NULL AND \
                    event_type IN ('facebook_post.discovered', 'facebook_post.updated')) \
                    AS oldest_classification_event_age_seconds, \
                (SELECT count(*) FROM outbox_events WHERE processed_at IS NULL AND \
                    event_type = 'classification.completed') AS pending_notification_events, \
                (SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - min(created_at)))::bigint \
                    FROM outbox_events WHERE processed_at IS NULL AND \
                    event_type = 'classification.completed') \
                    AS oldest_notification_event_age_seconds, \
                (SELECT count(*) FROM dead_letters) AS dead_letters, \
                (SELECT count(*) FROM deliveries WHERE status IN ('pending', 'retry', 'sending')) \
                    AS pending_deliveries, \
                (SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - min(created_at)))::bigint \
                    FROM deliveries WHERE status IN ('pending', 'retry', 'sending')) \
                    AS oldest_pending_delivery_age_seconds, \
                 (SELECT count(*) FROM deliveries WHERE status = 'failed' AND \
                    failure_class IS DISTINCT FROM 'recipient_unavailable') AS failed_deliveries, \
                 (SELECT count(*) FROM digest_batches WHERE status IN ('pending', 'retry', 'sending')) AS pending_digest_batches, \
                 (SELECT count(*) FROM digest_batches WHERE status = 'failed' AND \
                    failure_class IS DISTINCT FROM 'recipient_unavailable') AS failed_digest_batches, \
                 (SELECT count(*) FROM edge_inbox_events WHERE processed_at IS NULL AND dead_lettered_at IS NULL) AS pending_edge_events, \
                 (SELECT count(*) FROM edge_inbox_events WHERE dead_lettered_at IS NOT NULL) AS dead_lettered_edge_events, \
                 (SELECT count(*) FROM donation_intents WHERE status IN ('creating', 'pending')) AS pending_donation_intents, \
                 (SELECT count(*) FROM donation_intents WHERE status = 'failed') AS failed_donation_intents, \
                (SELECT count(*) FROM subscribers WHERE active) AS active_subscribers, \
                (SELECT count(*) FROM source_suggestions WHERE status = 'pending') \
                    AS pending_source_suggestions, \
                (SELECT count(*) FROM classifications AS classification \
                    LEFT JOIN manual_review_resolutions AS resolution \
                        ON resolution.classification_id = classification.id \
                    WHERE classification.decision = 'manual_review' \
                        AND resolution.classification_id IS NULL) AS pending_manual_reviews, \
                EXISTS(SELECT 1 FROM worker_leases WHERE lease_key = 'telegram_delivery' \
                    AND expires_at > CURRENT_TIMESTAMP) AS telegram_worker_active",
        )
        .bind(alert_after_failures)
        .bind(backlog_stale_seconds)
        .fetch_one(&self.pool)
        .await?;
        let mut health = OperationalHealth {
            schema_version: "operational-health.v1".to_owned(),
            generated_at: row.get("generated_at"),
            status: String::new(),
            enabled_sources: row.get("enabled_sources"),
            sources_never_crawled: row.get("sources_never_crawled"),
            stale_sources: row.get("stale_sources"),
            sources_with_failures: row.get("sources_with_failures"),
            sources_alerting: row.get("sources_alerting"),
            pending_classification_events: row.get("pending_classification_events"),
            oldest_classification_event_age_seconds: row
                .get("oldest_classification_event_age_seconds"),
            pending_notification_events: row.get("pending_notification_events"),
            oldest_notification_event_age_seconds: row.get("oldest_notification_event_age_seconds"),
            dead_letters: row.get("dead_letters"),
            pending_deliveries: row.get("pending_deliveries"),
            oldest_pending_delivery_age_seconds: row.get("oldest_pending_delivery_age_seconds"),
            failed_deliveries: row.get("failed_deliveries"),
            pending_digest_batches: row.get("pending_digest_batches"),
            failed_digest_batches: row.get("failed_digest_batches"),
            pending_edge_events: row.get("pending_edge_events"),
            dead_lettered_edge_events: row.get("dead_lettered_edge_events"),
            pending_donation_intents: row.get("pending_donation_intents"),
            failed_donation_intents: row.get("failed_donation_intents"),
            active_subscribers: row.get("active_subscribers"),
            pending_source_suggestions: row.get("pending_source_suggestions"),
            pending_manual_reviews: row.get("pending_manual_reviews"),
            telegram_worker_active: row.get("telegram_worker_active"),
        };
        let stale_backlog = [
            health.oldest_classification_event_age_seconds,
            health.oldest_notification_event_age_seconds,
            health.oldest_pending_delivery_age_seconds,
        ]
        .into_iter()
        .flatten()
        .any(|age| age > backlog_stale_seconds);
        let systemic_source_failure =
            source_alerts_are_systemic(health.enabled_sources, health.sources_alerting);
        health.status = if systemic_source_failure
            || health.dead_letters > 0
            || health.failed_deliveries > 0
            || health.failed_digest_batches > 0
            || health.dead_lettered_edge_events > 0
            || health.failed_donation_intents > 0
        {
            "failed"
        } else if health.enabled_sources == 0
            || health.sources_never_crawled > 0
            || health.stale_sources > 0
            || health.sources_alerting > 0
            || stale_backlog
            || (health.active_subscribers > 0 && !health.telegram_worker_active)
        {
            "degraded"
        } else {
            "healthy"
        }
        .to_owned();
        Ok(health)
    }

    pub async fn recent_crawl_strategy_health(
        &self,
        window_seconds: u64,
        sample_size: u32,
    ) -> Result<Vec<CrawlStrategyHealthSample>> {
        if window_seconds == 0 || sample_size == 0 {
            bail!("strategy health window and sample size must be at least 1");
        }
        let window_seconds = i64::try_from(window_seconds)?;
        let sample_size = i64::from(sample_size);
        let rows = sqlx::query(
            "WITH ranked AS ( \
                SELECT attempt.strategy, attempt.outcome, \
                    row_number() OVER ( \
                        PARTITION BY attempt.strategy ORDER BY attempt.id DESC \
                    ) AS sample_rank \
                FROM crawler_attempts AS attempt \
                JOIN crawler_runs AS run ON run.id = attempt.crawler_run_id \
                WHERE run.created_at >= CURRENT_TIMESTAMP - \
                    make_interval(secs => $1::double precision) \
            ) \
            SELECT strategy, count(*) AS attempts, \
                count(*) FILTER (WHERE outcome = 'healthy') AS healthy \
            FROM ranked WHERE sample_rank <= $2 \
            GROUP BY strategy ORDER BY strategy",
        )
        .bind(window_seconds)
        .bind(sample_size)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CrawlStrategyHealthSample {
                    strategy: row.try_get("strategy")?,
                    attempts: u64::try_from(row.try_get::<i64, _>("attempts")?)?,
                    healthy: u64::try_from(row.try_get::<i64, _>("healthy")?)?,
                })
            })
            .collect()
    }

    pub async fn crawl_history(&self, limit: i64, offset: i64) -> Result<Vec<CrawlHistoryRecord>> {
        if !(1..=20).contains(&limit) || offset < 0 {
            bail!("crawl history limit must be between 1 and 20 and offset must be non-negative");
        }
        let rows = sqlx::query(
            "SELECT run.id AS run_id, source.source_key, source.name AS source_name, \
                    run.fetched_at, run.created_at, run.health, run.selected_strategy, \
                    run.post_count, run.error, COUNT(attempt.id)::BIGINT AS attempt_count \
             FROM crawler_runs AS run \
             JOIN sources AS source ON source.id = run.source_id \
             LEFT JOIN crawler_attempts AS attempt ON attempt.crawler_run_id = run.id \
             GROUP BY run.id, source.source_key, source.name, run.fetched_at, run.created_at, \
                      run.health, run.selected_strategy, run.post_count, run.error \
             ORDER BY run.created_at DESC, run.id DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CrawlHistoryRecord {
                    run_id: row.get("run_id"),
                    source_key: row.get("source_key"),
                    source_name: row.get("source_name"),
                    fetched_at: row.get("fetched_at"),
                    created_at: row.get("created_at"),
                    health: row.get("health"),
                    selected_strategy: row.get("selected_strategy"),
                    post_count: row.get("post_count"),
                    attempt_count: row.get("attempt_count"),
                    error: row.get("error"),
                })
            })
            .collect()
    }

    pub async fn crawl_history_item(&self, run_id: i64) -> Result<Option<CrawlHistoryDetail>> {
        if run_id <= 0 {
            bail!("crawl run ID must be positive");
        }
        let run = sqlx::query(
            "SELECT run.id AS run_id, source.source_key, source.name AS source_name, \
                    run.fetched_at, run.created_at, run.health, run.selected_strategy, \
                    run.post_count, run.error, COUNT(attempt.id)::BIGINT AS attempt_count \
             FROM crawler_runs AS run \
             JOIN sources AS source ON source.id = run.source_id \
             LEFT JOIN crawler_attempts AS attempt ON attempt.crawler_run_id = run.id \
             WHERE run.id = $1 \
             GROUP BY run.id, source.source_key, source.name, run.fetched_at, run.created_at, \
                      run.health, run.selected_strategy, run.post_count, run.error",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(run) = run else {
            return Ok(None);
        };
        let run = CrawlHistoryRecord {
            run_id: run.get("run_id"),
            source_key: run.get("source_key"),
            source_name: run.get("source_name"),
            fetched_at: run.get("fetched_at"),
            created_at: run.get("created_at"),
            health: run.get("health"),
            selected_strategy: run.get("selected_strategy"),
            post_count: run.get("post_count"),
            attempt_count: run.get("attempt_count"),
            error: run.get("error"),
        };
        let rows = sqlx::query(
            "SELECT ordinal, strategy, outcome, status, latency_ms, bytes_received, \
                    posts_found, newest_post_at, error, browser_metadata \
             FROM crawler_attempts WHERE crawler_run_id = $1 ORDER BY ordinal",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        let attempts = rows
            .into_iter()
            .map(|row| -> Result<CrawlAttemptHistoryRecord> {
                let browser_metadata =
                    row.try_get::<Option<serde_json::Value>, _>("browser_metadata")?;
                Ok(CrawlAttemptHistoryRecord {
                    ordinal: row.get("ordinal"),
                    strategy: row.get("strategy"),
                    outcome: row.get("outcome"),
                    status: row.get("status"),
                    latency_ms: row.get("latency_ms"),
                    bytes_received: row.get("bytes_received"),
                    posts_found: row.get("posts_found"),
                    newest_post_at: row.get("newest_post_at"),
                    error: row.get("error"),
                    browser: browser_metadata.map(serde_json::from_value).transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(CrawlHistoryDetail { run, attempts }))
    }

    pub async fn observe_operational_alert(
        &self,
        alert_key: &str,
        status: &str,
        transition_grace_seconds: u64,
    ) -> Result<Option<OperationalAlertCandidate>> {
        if alert_key.trim().is_empty()
            || !matches!(status, "healthy" | "degraded" | "failed")
            || transition_grace_seconds == 0
        {
            bail!("operational alert key, status, and grace period must be valid");
        }
        let transition_grace_seconds = i64::try_from(transition_grace_seconds)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO operational_alert_state \
                (alert_key, observed_status, notified_status, notified_at) \
             VALUES ($1, $2, CASE WHEN $2 = 'healthy' THEN 'healthy' END, \
                CASE WHEN $2 = 'healthy' THEN CURRENT_TIMESTAMP END) \
             ON CONFLICT (alert_key) DO NOTHING",
        )
        .bind(alert_key)
        .bind(status)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE operational_alert_state SET observed_status = $2, \
                observed_since = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP \
             WHERE alert_key = $1 AND observed_status <> $2",
        )
        .bind(alert_key)
        .bind(status)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "SELECT observed_status, notified_status, \
                observed_since <= CURRENT_TIMESTAMP - \
                    make_interval(secs => $2::double precision) AS grace_elapsed \
             FROM operational_alert_state WHERE alert_key = $1 FOR UPDATE",
        )
        .bind(alert_key)
        .bind(transition_grace_seconds)
        .fetch_one(&mut *transaction)
        .await?;
        let observed_status: String = row.get("observed_status");
        let notified_status: Option<String> = row.get("notified_status");
        let grace_elapsed: bool = row.get("grace_elapsed");
        let kind =
            operational_alert_kind(&observed_status, notified_status.as_deref(), grace_elapsed);
        transaction.commit().await?;
        Ok(kind.map(|kind| OperationalAlertCandidate {
            alert_key: alert_key.to_owned(),
            observed_status,
            kind,
        }))
    }

    pub async fn complete_operational_alert(
        &self,
        candidate: &OperationalAlertCandidate,
    ) -> Result<bool> {
        let affected = sqlx::query(
            "UPDATE operational_alert_state SET notified_status = $2, \
                notified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP \
             WHERE alert_key = $1 AND observed_status = $2",
        )
        .bind(&candidate.alert_key)
        .bind(&candidate.observed_status)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected == 1)
    }

    pub async fn upsert_sources(&self, sources: &[SourceSeed]) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for source in sources {
            if source.schedule_interval_seconds <= 0 {
                bail!("source schedule interval must be at least 1 second");
            }
            sqlx::query(
                "INSERT INTO sources (source_key, name, url, schedule_interval_seconds) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (source_key) DO UPDATE SET \
                    name = EXCLUDED.name, url = EXCLUDED.url, \
                    schedule_interval_seconds = EXCLUDED.schedule_interval_seconds, \
                    updated_at = CURRENT_TIMESTAMP",
            )
            .bind(&source.key)
            .bind(&source.name)
            .bind(&source.url)
            .bind(source.schedule_interval_seconds)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn claim_due_sources(
        &self,
        owner: &str,
        limit: i64,
        lease_seconds: i64,
    ) -> Result<Vec<ClaimedSource>> {
        if owner.is_empty() || limit <= 0 || lease_seconds <= 0 {
            bail!("owner, claim limit, and lease duration must be valid");
        }
        let rows = sqlx::query(
            "WITH due AS ( \
                SELECT id, NOT EXISTS ( \
                    SELECT 1 FROM crawler_runs WHERE crawler_runs.source_id = sources.id \
                      AND crawler_runs.health = 'healthy' \
                ) AS initial_crawl FROM sources \
                WHERE enabled \
                  AND next_crawl_at <= CURRENT_TIMESTAMP \
                  AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP) \
                ORDER BY next_crawl_at, id \
                FOR UPDATE SKIP LOCKED \
                LIMIT $1 \
             ) \
             UPDATE sources AS source SET \
                lease_owner = $2, \
                lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                updated_at = CURRENT_TIMESTAMP \
             FROM due WHERE source.id = due.id \
             RETURNING source.id, source.source_key, source.name, source.url, \
                       source.failure_count, source.schedule_interval_seconds, \
                       source.unchanged_crawl_count, due.initial_crawl, EXISTS ( \
                           SELECT 1 FROM crawler_runs AS degraded \
                           WHERE degraded.source_id = source.id \
                             AND degraded.health <> 'healthy' \
                             AND degraded.fetched_at >= CURRENT_TIMESTAMP - INTERVAL '6 hours' \
                             AND NOT EXISTS ( \
                                 SELECT 1 FROM crawler_runs AS recovered \
                                 WHERE recovered.source_id = source.id \
                                   AND recovered.health = 'healthy' \
                                   AND recovered.post_count >= 2 \
                                   AND recovered.fetched_at > degraded.fetched_at \
                             ) \
                       ) AS reconciliation_required",
        )
        .bind(limit)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let failure_count = u32::try_from(row.get::<i32, _>("failure_count"))?;
                let schedule_interval_seconds =
                    u64::try_from(row.get::<i32, _>("schedule_interval_seconds"))?;
                let unchanged_crawl_count =
                    u32::try_from(row.get::<i32, _>("unchanged_crawl_count"))?;
                Ok(ClaimedSource {
                    id: row.get("id"),
                    key: row.get("source_key"),
                    name: row.get("name"),
                    url: row.get("url"),
                    failure_count,
                    schedule_interval_seconds,
                    unchanged_crawl_count,
                    initial_crawl: row.get("initial_crawl"),
                    reconciliation_required: row.get("reconciliation_required"),
                })
            })
            .collect()
    }

    pub async fn release_source_leases(&self, owner: &str) -> Result<u64> {
        if owner.is_empty() {
            bail!("source lease owner must not be empty");
        }
        Ok(sqlx::query(
            "UPDATE sources SET lease_owner = NULL, lease_expires_at = NULL, \
                updated_at = CURRENT_TIMESTAMP WHERE lease_owner = $1",
        )
        .bind(owner)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    pub async fn persist_report(
        &self,
        source: &ClaimedSource,
        owner: &str,
        report: &CrawlReport,
        next_delay_seconds: u64,
        emit_post_events: bool,
        allow_historical_events: bool,
    ) -> Result<PersistOutcome> {
        Ok(self
            .persist_scheduled_report(
                source,
                owner,
                report,
                ScheduledPersistOptions {
                    next_delay_seconds,
                    emit_post_events,
                    allow_historical_events,
                    adaptive_schedule: None,
                },
            )
            .await?
            .persistence)
    }

    pub async fn persist_scheduled_report(
        &self,
        source: &ClaimedSource,
        owner: &str,
        report: &CrawlReport,
        options: ScheduledPersistOptions,
    ) -> Result<ScheduledPersistOutcome> {
        if report.schema_version != REPORT_SCHEMA_VERSION {
            bail!("unsupported crawl report schema {}", report.schema_version);
        }
        if report.source_url != source.url {
            bail!("crawl report source URL does not match claimed source");
        }
        let fetched_at = parse_timestamp(&report.fetched_at, "report fetched_at")?;
        let mut transaction = self.pool.begin().await?;
        ensure_lease(&mut transaction, source.id, owner).await?;
        let run_id = insert_run(&mut transaction, source.id, report, fetched_at).await?;
        insert_attempts(&mut transaction, run_id, report).await?;
        let mut outcome = PersistOutcome::default();
        if report.health == "healthy" && report.posts.is_empty() {
            bail!("healthy crawl report must contain at least one post");
        }
        if !report.posts.is_empty() {
            let historical_cutoff = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT min(fetched_at) FROM crawler_runs WHERE source_id = $1 AND health = 'healthy'",
            )
            .bind(source.id)
            .fetch_one(&mut *transaction)
            .await?
            .unwrap_or(fetched_at);
            for post in &report.posts {
                if post.source_id != report.source_id {
                    bail!("post source ID does not match crawl report source ID");
                }
                persist_post(
                    &mut transaction,
                    source.id,
                    post,
                    options.emit_post_events,
                    options.allow_historical_events,
                    historical_cutoff,
                    &mut outcome,
                )
                .await?;
            }
        }
        let (next_delay_seconds, unchanged_crawl_count) = match options.adaptive_schedule {
            Some(schedule) if report.health == "healthy" => {
                adaptive_next_schedule(source, &outcome, schedule)?
            }
            _ => (options.next_delay_seconds, source.unchanged_crawl_count),
        };
        finish_source(
            &mut transaction,
            source.id,
            owner,
            report.health == "healthy",
            next_delay_seconds,
            unchanged_crawl_count,
        )
        .await?;
        transaction.commit().await?;
        Ok(ScheduledPersistOutcome {
            persistence: outcome,
            next_delay_seconds,
        })
    }

    pub async fn persist_failure(
        &self,
        source: &ClaimedSource,
        owner: &str,
        error: &str,
        next_delay_seconds: u64,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        ensure_lease(&mut transaction, source.id, owner).await?;
        sqlx::query(
            "INSERT INTO crawler_runs \
             (source_id, fetched_at, health, post_count, error) \
             VALUES ($1, CURRENT_TIMESTAMP, 'failed', 0, $2)",
        )
        .bind(source.id)
        .bind(error)
        .execute(&mut *transaction)
        .await?;
        finish_source(
            &mut transaction,
            source.id,
            owner,
            false,
            next_delay_seconds,
            source.unchanged_crawl_count,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn apply_retention(&self, run_retention_days: i32) -> Result<u64> {
        if run_retention_days <= 0 {
            bail!("run retention must be at least 1 day");
        }
        let result = sqlx::query(
            "DELETE FROM crawler_runs \
             WHERE created_at < CURRENT_TIMESTAMP - make_interval(days => $1) \
               AND id NOT IN ( \
                   SELECT DISTINCT ON (source_id) id FROM crawler_runs \
                   ORDER BY source_id, created_at DESC \
               )",
        )
        .bind(run_retention_days)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn claim_classification_events(
        &self,
        owner: &str,
        limit: i64,
        lease_seconds: i64,
    ) -> Result<Vec<ClaimedClassificationEvent>> {
        if owner.is_empty() || limit <= 0 || lease_seconds <= 0 {
            bail!("owner, claim limit, and lease duration must be valid");
        }
        let rows = sqlx::query(
            "WITH pending AS ( \
                SELECT id FROM outbox_events \
                WHERE processed_at IS NULL \
                  AND event_type IN ('facebook_post.discovered', 'facebook_post.updated') \
                  AND available_at <= CURRENT_TIMESTAMP \
                  AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP) \
                ORDER BY available_at, id \
                FOR UPDATE SKIP LOCKED \
                LIMIT $1 \
             ) \
             UPDATE outbox_events AS event SET \
                lease_owner = $2, \
                lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                attempts = attempts + 1 \
             FROM pending WHERE event.id = pending.id \
             RETURNING event.id, event.event_key, event.event_type, event.payload, event.attempts",
        )
        .bind(limit)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ClaimedClassificationEvent {
                    id: row.get("id"),
                    event_key: row.get("event_key"),
                    event_type: row.get("event_type"),
                    payload: row.get("payload"),
                    attempts: u32::try_from(row.get::<i32, _>("attempts"))?,
                })
            })
            .collect()
    }

    pub async fn complete_classification(
        &self,
        event: &ClaimedClassificationEvent,
        owner: &str,
        database_post_id: i64,
        result: &ClassificationResult,
    ) -> Result<ClassificationPersistOutcome> {
        validate_classification(result)?;
        validate_classification_event(event, database_post_id, result)?;
        let classified_at = parse_timestamp(&result.classified_at, "classified_at")?;
        let mut transaction = self.pool.begin().await?;
        ensure_outbox_lease(&mut transaction, event.id, owner).await?;
        let post_revision_matches = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS( \
                 SELECT 1 FROM posts \
                 JOIN post_revisions ON post_revisions.post_id = posts.id \
                 WHERE posts.id = $1 AND post_revisions.content_hash = $2 \
             )",
        )
        .bind(database_post_id)
        .bind(&result.input_content_hash)
        .fetch_one(&mut *transaction)
        .await?;
        if !post_revision_matches {
            bail!("classification does not match a persisted post revision");
        }
        let existing_id = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM classifications \
             WHERE post_id = $1 AND input_content_hash = $2 \
               AND classifier_version = $3 AND config_hash = $4",
        )
        .bind(database_post_id)
        .bind(&result.input_content_hash)
        .bind(&result.classifier_version)
        .bind(&result.config_hash)
        .fetch_optional(&mut *transaction)
        .await?;
        let decision = decision_name(&result.decision);
        let (classification_id, classification_inserted) = match existing_id {
            Some(id) => (id, false),
            None => {
                let id = sqlx::query_scalar::<_, i64>(
                    "INSERT INTO classifications \
                     (post_id, schema_version, input_content_hash, decision, score, \
                      confidence_basis_points, matched_rules, classifier_version, config_hash, \
                      classified_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id",
                )
                .bind(database_post_id)
                .bind(&result.schema_version)
                .bind(&result.input_content_hash)
                .bind(decision)
                .bind(result.score)
                .bind(i32::from(result.confidence_basis_points))
                .bind(serde_json::to_value(&result.matched_rules)?)
                .bind(&result.classifier_version)
                .bind(&result.config_hash)
                .bind(classified_at)
                .fetch_one(&mut *transaction)
                .await?;
                sqlx::query(
                    "INSERT INTO classification_features \
                     (classification_id, explicit_drl, registration_call, form_link, \
                      future_event_time, future_deadline, location, target_students, \
                      approved_source, negative_commercial, past_event, extracted) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                )
                .bind(id)
                .bind(result.features.explicit_drl)
                .bind(result.features.registration_call)
                .bind(result.features.form_link)
                .bind(result.features.future_event_time)
                .bind(result.features.future_deadline)
                .bind(result.features.location)
                .bind(result.features.target_students)
                .bind(result.features.approved_source)
                .bind(result.features.negative_commercial)
                .bind(result.features.past_event)
                .bind(&result.extracted)
                .execute(&mut *transaction)
                .await?;
                (id, true)
            }
        };
        let completion_key = format!("classification:{classification_id}");
        let completion_payload = json!({
            "classification": result,
            "database_classification_id": classification_id,
            "database_post_id": database_post_id
        });
        let completion_event_inserted = sqlx::query(
            "INSERT INTO outbox_events \
             (event_key, event_type, aggregate_type, aggregate_id, payload) \
             VALUES ($1, 'classification.completed', 'facebook_post', $2, $3) \
             ON CONFLICT (event_key) DO NOTHING",
        )
        .bind(completion_key)
        .bind(&result.external_post_id)
        .bind(completion_payload)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        mark_outbox_processed(&mut transaction, event.id, owner).await?;
        transaction.commit().await?;
        Ok(ClassificationPersistOutcome {
            classification_inserted,
            completion_event_inserted,
        })
    }

    pub async fn fail_classification_event(
        &self,
        event: &ClaimedClassificationEvent,
        owner: &str,
        error: &str,
        max_attempts: u32,
        retry_delay_seconds: u64,
    ) -> Result<FailureDisposition> {
        if max_attempts == 0 || retry_delay_seconds == 0 {
            bail!("max attempts and retry delay must be at least 1");
        }
        let error = error.chars().take(4_000).collect::<String>();
        let mut transaction = self.pool.begin().await?;
        ensure_outbox_lease(&mut transaction, event.id, owner).await?;
        let disposition = if event.attempts >= max_attempts {
            sqlx::query(
                "INSERT INTO dead_letters \
                 (original_outbox_event_id, event_key, event_type, payload, attempts, last_error) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (original_outbox_event_id) DO NOTHING",
            )
            .bind(event.id)
            .bind(&event.event_key)
            .bind(&event.event_type)
            .bind(&event.payload)
            .bind(i32::try_from(event.attempts)?)
            .bind(&error)
            .execute(&mut *transaction)
            .await?;
            mark_outbox_processed(&mut transaction, event.id, owner).await?;
            FailureDisposition::DeadLettered
        } else {
            let affected = sqlx::query(
                "UPDATE outbox_events SET \
                    lease_owner = NULL, lease_expires_at = NULL, \
                    available_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                    last_error = $4 \
                 WHERE id = $1 AND lease_owner = $2 AND processed_at IS NULL",
            )
            .bind(event.id)
            .bind(owner)
            .bind(i64::try_from(retry_delay_seconds)?)
            .bind(&error)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if affected != 1 {
                bail!("failed to release classification event for retry");
            }
            FailureDisposition::RetryScheduled
        };
        transaction.commit().await?;
        Ok(disposition)
    }

    pub async fn apply_classifier_retention(
        &self,
        dead_letter_retention_days: i32,
        processed_event_retention_days: i32,
    ) -> Result<ClassifierRetentionOutcome> {
        if dead_letter_retention_days <= 0 || processed_event_retention_days <= 0 {
            bail!("classifier retention periods must be at least 1 day");
        }
        let mut transaction = self.pool.begin().await?;
        let dead_letters_deleted = sqlx::query(
            "DELETE FROM dead_letters \
             WHERE failed_at < CURRENT_TIMESTAMP - make_interval(days => $1)",
        )
        .bind(dead_letter_retention_days)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let processed_outbox_events_deleted = sqlx::query(
            "DELETE FROM outbox_events \
             WHERE processed_at IS NOT NULL \
               AND processed_at < CURRENT_TIMESTAMP - make_interval(days => $1) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM dead_letters \
                   WHERE original_outbox_event_id = outbox_events.id \
               )",
        )
        .bind(processed_event_retention_days)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(ClassifierRetentionOutcome {
            dead_letters_deleted,
            processed_outbox_events_deleted,
        })
    }

    pub async fn upsert_subscriber(
        &self,
        telegram_chat_id: i64,
        display_name: Option<&str>,
    ) -> Result<()> {
        if telegram_chat_id == 0 {
            bail!("Telegram chat ID must not be zero");
        }
        sqlx::query(
            "INSERT INTO subscribers (telegram_chat_id, display_name, onboarding_completed_at) \
             VALUES ($1, $2, CURRENT_TIMESTAMP) \
             ON CONFLICT (telegram_chat_id) DO UPDATE SET \
                display_name = COALESCE(EXCLUDED.display_name, subscribers.display_name), \
                active = TRUE, deactivated_reason = NULL, \
                onboarding_completed_at = COALESCE(subscribers.onboarding_completed_at, CURRENT_TIMESTAMP), \
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(telegram_chat_id)
        .bind(display_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn begin_subscriber_onboarding(
        &self,
        telegram_chat_id: i64,
        display_name: Option<&str>,
        acquisition_source: Option<&str>,
    ) -> Result<bool> {
        if telegram_chat_id == 0
            || acquisition_source.is_some_and(|value| {
                value.is_empty()
                    || value.len() > 64
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            })
        {
            bail!("subscriber onboarding values are invalid");
        }
        let completed = sqlx::query_scalar::<_, bool>(
            "INSERT INTO subscribers (telegram_chat_id, display_name, active, acquisition_source) \
             VALUES ($1, $2, FALSE, $3) \
             ON CONFLICT (telegram_chat_id) DO UPDATE SET \
                display_name = COALESCE(EXCLUDED.display_name, subscribers.display_name), \
                acquisition_source = COALESCE(subscribers.acquisition_source, EXCLUDED.acquisition_source), \
                updated_at = CURRENT_TIMESTAMP \
             RETURNING onboarding_completed_at IS NOT NULL",
        )
        .bind(telegram_chat_id)
        .bind(display_name)
        .bind(acquisition_source)
        .fetch_one(&self.pool)
        .await?;
        self.record_product_event(
            telegram_chat_id,
            "bot_started",
            None,
            acquisition_source
                .map(|value| json!({"source": value}))
                .unwrap_or_else(|| json!({})),
        )
        .await?;
        Ok(completed)
    }

    pub async fn complete_subscriber_onboarding(
        &self,
        telegram_chat_id: i64,
        notification_scope: &str,
    ) -> Result<()> {
        if !matches!(notification_scope, "drl" | "all") {
            bail!("notification scope is invalid");
        }
        let mut transaction = self.pool.begin().await?;
        let already_completed = sqlx::query_scalar::<_, bool>(
            "SELECT onboarding_completed_at IS NOT NULL FROM subscribers WHERE telegram_chat_id = $1 FOR UPDATE",
        )
        .bind(telegram_chat_id)
        .fetch_optional(&mut *transaction)
        .await?
        .context("subscriber onboarding state was not found")?;
        sqlx::query(
            "UPDATE subscribers SET active = TRUE, onboarding_completed_at = COALESCE(onboarding_completed_at, CURRENT_TIMESTAMP), \
                notification_scope = $2, deactivated_reason = NULL, updated_at = CURRENT_TIMESTAMP \
             WHERE telegram_chat_id = $1",
        )
        .bind(telegram_chat_id)
        .bind(notification_scope)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        if already_completed {
            Ok(())
        } else {
            self.record_product_event(
                telegram_chat_id,
                "onboarding_completed",
                None,
                json!({"notification_scope": notification_scope}),
            )
            .await
        }
    }

    pub async fn update_subscriber_preferences(
        &self,
        telegram_chat_id: i64,
        notification_scope: Option<&str>,
        delivery_mode: Option<&str>,
        quiet_hours_enabled: Option<bool>,
    ) -> Result<()> {
        if notification_scope.is_some_and(|value| !matches!(value, "drl" | "all"))
            || delivery_mode.is_some_and(|value| !matches!(value, "instant" | "daily"))
        {
            bail!("subscriber preferences are invalid");
        }
        let changed = sqlx::query(
            "UPDATE subscribers SET notification_scope = COALESCE($2, notification_scope), \
                delivery_mode = COALESCE($3, delivery_mode), \
                quiet_hours_enabled = COALESCE($4, quiet_hours_enabled), \
                next_digest_at = CASE WHEN $3 = 'daily' THEN \
                    CASE WHEN (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok')::time < TIME '07:30' \
                      THEN (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') + INTERVAL '7 hours 30 minutes') AT TIME ZONE 'Asia/Bangkok' \
                      ELSE (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 day 7 hours 30 minutes') AT TIME ZONE 'Asia/Bangkok' END \
                    ELSE next_digest_at END, \
                updated_at = CURRENT_TIMESTAMP WHERE telegram_chat_id = $1",
        )
        .bind(telegram_chat_id)
        .bind(notification_scope)
        .bind(delivery_mode)
        .bind(quiet_hours_enabled)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed != 1 {
            bail!("subscriber was not found");
        }
        self.record_product_event(
            telegram_chat_id,
            "preference_updated",
            None,
            json!({
                "notification_scope": notification_scope,
                "delivery_mode": delivery_mode,
                "quiet_hours_enabled": quiet_hours_enabled
            }),
        )
        .await
    }

    pub async fn latest_notification_sample(&self) -> Result<Option<String>> {
        sqlx::query_scalar(
            "SELECT message_text FROM campaigns ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn record_notification_feedback(
        &self,
        telegram_chat_id: i64,
        campaign_id: i64,
        value: &str,
    ) -> Result<FeedbackOutcome> {
        if campaign_id <= 0 || !matches!(value, "useful" | "irrelevant") {
            bail!("notification feedback is invalid");
        }
        let mut transaction = self.pool.begin().await?;
        let subscriber = sqlx::query(
            "SELECT id, donation_prompted_at IS NULL AS can_prompt, \
                    EXISTS(SELECT 1 FROM deliveries WHERE subscriber_id = subscribers.id AND campaign_id = $2) AS received \
             FROM subscribers WHERE telegram_chat_id = $1",
        )
        .bind(telegram_chat_id)
        .bind(campaign_id)
        .fetch_optional(&mut *transaction)
        .await?
        .context("subscriber was not found")?;
        let subscriber_id: i64 = subscriber.get("id");
        if !subscriber.get::<bool, _>("received") {
            bail!("campaign was not delivered to this subscriber");
        }
        let first_response = sqlx::query(
            "INSERT INTO notification_feedback (subscriber_id, campaign_id, value) VALUES ($1, $2, $3) \
             ON CONFLICT (subscriber_id, campaign_id) DO NOTHING",
        )
        .bind(subscriber_id)
        .bind(campaign_id)
        .bind(value)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        if !first_response {
            sqlx::query(
                "UPDATE notification_feedback SET value = $3, updated_at = CURRENT_TIMESTAMP \
                 WHERE subscriber_id = $1 AND campaign_id = $2",
            )
            .bind(subscriber_id)
            .bind(campaign_id)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
        }
        let should_prompt_donation = value == "useful" && subscriber.get::<bool, _>("can_prompt");
        if should_prompt_donation {
            sqlx::query(
                "UPDATE subscribers SET donation_prompted_at = CURRENT_TIMESTAMP WHERE id = $1 AND donation_prompted_at IS NULL",
            )
            .bind(subscriber_id)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "INSERT INTO product_events (subscriber_id, event_type, campaign_id, metadata) VALUES ($1, 'notification_feedback', $2, $3)",
        )
        .bind(subscriber_id)
        .bind(campaign_id)
        .bind(json!({"value": value}))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(FeedbackOutcome {
            first_response,
            should_prompt_donation,
        })
    }

    pub async fn open_campaign_action(
        &self,
        telegram_chat_id: i64,
        campaign_id: i64,
    ) -> Result<Option<String>> {
        if campaign_id <= 0 {
            bail!("campaign ID must be positive");
        }
        let row = sqlx::query(
            "SELECT subscribers.id AS subscriber_id, campaigns.action_url \
             FROM subscribers \
             JOIN deliveries ON deliveries.subscriber_id = subscribers.id \
             JOIN campaigns ON campaigns.id = deliveries.campaign_id \
             WHERE subscribers.telegram_chat_id = $1 AND campaigns.id = $2",
        )
        .bind(telegram_chat_id)
        .bind(campaign_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let action_url: Option<String> = row.get("action_url");
        if action_url.is_some() {
            sqlx::query(
                "INSERT INTO product_events (subscriber_id, event_type, campaign_id) VALUES ($1, 'notification_cta_clicked', $2)",
            )
            .bind(row.get::<i64, _>("subscriber_id"))
            .bind(campaign_id)
            .execute(&self.pool)
            .await?;
        }
        Ok(action_url)
    }

    pub async fn record_product_event(
        &self,
        telegram_chat_id: i64,
        event_type: &str,
        campaign_id: Option<i64>,
        metadata: serde_json::Value,
    ) -> Result<()> {
        if event_type.is_empty() || event_type.len() > 100 || !metadata.is_object() {
            bail!("product event is invalid");
        }
        sqlx::query(
            "INSERT INTO product_events (subscriber_id, event_type, campaign_id, metadata) \
             SELECT id, $2, $3, $4 FROM subscribers WHERE telegram_chat_id = $1",
        )
        .bind(telegram_chat_id)
        .bind(event_type)
        .bind(campaign_id)
        .bind(metadata)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn growth_metrics(&self) -> Result<GrowthMetrics> {
        let row = sqlx::query(
            "SELECT \
                (SELECT count(*) FROM product_events WHERE event_type = 'bot_started' AND occurred_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS starts_7d, \
                (SELECT count(*) FROM product_events WHERE event_type = 'onboarding_completed' AND occurred_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS onboarding_completed_7d, \
                (SELECT count(*) FROM subscribers WHERE active) AS active_subscribers, \
                (SELECT count(*) FROM product_events WHERE event_type = 'notification_delivered' AND occurred_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS notifications_delivered_7d, \
                (SELECT count(*) FROM product_events WHERE event_type = 'notification_cta_clicked' AND occurred_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS cta_clicks_7d, \
                (SELECT count(*) FROM notification_feedback WHERE value = 'useful' AND updated_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS useful_feedback_7d, \
                (SELECT count(*) FROM notification_feedback WHERE value = 'irrelevant' AND updated_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS irrelevant_feedback_7d, \
                (SELECT count(*) FROM donation_transactions WHERE transaction_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS donations_paid_7d, \
                (SELECT COALESCE(sum(amount), 0)::bigint FROM donation_transactions WHERE transaction_at >= CURRENT_TIMESTAMP - INTERVAL '7 days') AS donation_amount_7d",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(GrowthMetrics {
            starts_7d: row.get("starts_7d"),
            onboarding_completed_7d: row.get("onboarding_completed_7d"),
            active_subscribers: row.get("active_subscribers"),
            notifications_delivered_7d: row.get("notifications_delivered_7d"),
            cta_clicks_7d: row.get("cta_clicks_7d"),
            useful_feedback_7d: row.get("useful_feedback_7d"),
            irrelevant_feedback_7d: row.get("irrelevant_feedback_7d"),
            donations_paid_7d: row.get("donations_paid_7d"),
            donation_amount_7d: row.get("donation_amount_7d"),
        })
    }

    pub async fn acquire_worker_lease(
        &self,
        lease_key: &str,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool> {
        if lease_key.is_empty() || owner.is_empty() || lease_seconds <= 0 {
            bail!("worker lease key, owner, and duration must be valid");
        }
        let acquired = sqlx::query_scalar::<_, String>(
            "INSERT INTO worker_leases (lease_key, owner, expires_at) \
             VALUES ($1, $2, CURRENT_TIMESTAMP + make_interval(secs => $3::double precision)) \
             ON CONFLICT (lease_key) DO UPDATE SET owner = EXCLUDED.owner, \
                expires_at = EXCLUDED.expires_at, updated_at = CURRENT_TIMESTAMP \
             WHERE worker_leases.owner = EXCLUDED.owner \
                OR worker_leases.expires_at <= CURRENT_TIMESTAMP \
             RETURNING owner",
        )
        .bind(lease_key)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_optional(&self.pool)
        .await?;
        Ok(acquired.as_deref() == Some(owner))
    }

    pub async fn release_worker_lease(&self, lease_key: &str, owner: &str) -> Result<bool> {
        if lease_key.is_empty() || owner.is_empty() {
            bail!("worker lease key and owner must be valid");
        }
        let released = sqlx::query("DELETE FROM worker_leases WHERE lease_key = $1 AND owner = $2")
            .bind(lease_key)
            .bind(owner)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(released == 1)
    }

    pub async fn deactivate_subscriber(&self, telegram_chat_id: i64, reason: &str) -> Result<bool> {
        let affected = sqlx::query(
            "UPDATE subscribers SET active = FALSE, deactivated_reason = $2, \
             updated_at = CURRENT_TIMESTAMP WHERE telegram_chat_id = $1 AND active",
        )
        .bind(telegram_chat_id)
        .bind(reason.chars().take(1_000).collect::<String>())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected == 1)
    }

    pub async fn deactivate_subscribers_except(
        &self,
        telegram_chat_id: i64,
        reason: &str,
    ) -> Result<u64> {
        if telegram_chat_id == 0 || reason.trim().is_empty() {
            bail!("chat ID and deactivation reason must be valid");
        }
        let affected = sqlx::query(
            "UPDATE subscribers SET active = FALSE, deactivated_reason = $2, \
             updated_at = CURRENT_TIMESTAMP WHERE telegram_chat_id <> $1 AND active",
        )
        .bind(telegram_chat_id)
        .bind(reason.chars().take(1_000).collect::<String>())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected)
    }

    pub async fn list_subscribers(&self) -> Result<Vec<SubscriberRecord>> {
        let rows = sqlx::query(
            "SELECT telegram_chat_id, display_name, active, deactivated_reason, acquisition_source, \
                    onboarding_completed_at IS NOT NULL AS onboarding_completed, notification_scope, \
                    delivery_mode, quiet_hours_enabled \
             FROM subscribers ORDER BY telegram_chat_id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| SubscriberRecord {
                telegram_chat_id: row.get("telegram_chat_id"),
                display_name: row.get("display_name"),
                active: row.get("active"),
                deactivated_reason: row.get("deactivated_reason"),
                acquisition_source: row.get("acquisition_source"),
                onboarding_completed: row.get("onboarding_completed"),
                notification_scope: row.get("notification_scope"),
                delivery_mode: row.get("delivery_mode"),
                quiet_hours_enabled: row.get("quiet_hours_enabled"),
            })
            .collect())
    }

    pub async fn subscriber(&self, telegram_chat_id: i64) -> Result<Option<SubscriberRecord>> {
        if telegram_chat_id == 0 {
            bail!("Telegram chat ID must not be zero");
        }
        let row = sqlx::query(
            "SELECT telegram_chat_id, display_name, active, deactivated_reason, acquisition_source, \
                    onboarding_completed_at IS NOT NULL AS onboarding_completed, notification_scope, \
                    delivery_mode, quiet_hours_enabled \
             FROM subscribers WHERE telegram_chat_id = $1",
        )
        .bind(telegram_chat_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| SubscriberRecord {
            telegram_chat_id: row.get("telegram_chat_id"),
            display_name: row.get("display_name"),
            active: row.get("active"),
            deactivated_reason: row.get("deactivated_reason"),
            acquisition_source: row.get("acquisition_source"),
            onboarding_completed: row.get("onboarding_completed"),
            notification_scope: row.get("notification_scope"),
            delivery_mode: row.get("delivery_mode"),
            quiet_hours_enabled: row.get("quiet_hours_enabled"),
        }))
    }

    pub async fn list_enabled_sources(&self) -> Result<Vec<SourceRecord>> {
        let rows = sqlx::query(
            "SELECT source_key, name, url FROM sources WHERE enabled ORDER BY name, source_key",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| SourceRecord {
                key: row.get("source_key"),
                name: row.get("name"),
                url: row.get("url"),
            })
            .collect())
    }

    pub async fn submit_source_suggestion(
        &self,
        telegram_chat_id: i64,
        submitted_url: &str,
    ) -> Result<SourceSuggestionOutcome> {
        if telegram_chat_id == 0 || submitted_url.is_empty() || submitted_url.len() > 2_048 {
            bail!("source suggestion chat ID and URL must be valid");
        }
        let inserted = sqlx::query_scalar::<_, i64>(
            "INSERT INTO source_suggestions (telegram_chat_id, submitted_url) VALUES ($1, $2) \
             ON CONFLICT (submitted_url) WHERE status = 'pending' DO NOTHING RETURNING id",
        )
        .bind(telegram_chat_id)
        .bind(submitted_url)
        .fetch_optional(&self.pool)
        .await?;
        match inserted {
            Some(id) => Ok(SourceSuggestionOutcome { id, created: true }),
            None => {
                let id = sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM source_suggestions \
                     WHERE submitted_url = $1 AND status = 'pending' ORDER BY id LIMIT 1",
                )
                .bind(submitted_url)
                .fetch_one(&self.pool)
                .await?;
                Ok(SourceSuggestionOutcome { id, created: false })
            }
        }
    }

    pub async fn list_source_suggestions(
        &self,
        status: Option<&str>,
    ) -> Result<Vec<SourceSuggestionRecord>> {
        if status.is_some_and(|value| !matches!(value, "pending" | "approved" | "rejected")) {
            bail!("source suggestion status is invalid");
        }
        let rows = sqlx::query(
            "SELECT id, telegram_chat_id, submitted_url, status, admin_note, created_at, reviewed_at \
             FROM source_suggestions WHERE $1::text IS NULL OR status = $1 \
             ORDER BY created_at, id",
        )
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| SourceSuggestionRecord {
                id: row.get("id"),
                telegram_chat_id: row.get("telegram_chat_id"),
                submitted_url: row.get("submitted_url"),
                status: row.get("status"),
                admin_note: row.get("admin_note"),
                created_at: row.get("created_at"),
                reviewed_at: row.get("reviewed_at"),
            })
            .collect())
    }

    pub async fn approve_source_suggestion(&self, id: i64, name: &str) -> Result<bool> {
        if id <= 0 || name.trim().is_empty() {
            bail!("source suggestion ID and approved name must be valid");
        }
        let mut transaction = self.pool.begin().await?;
        let suggestion = sqlx::query(
            "SELECT submitted_url FROM source_suggestions \
             WHERE id = $1 AND status = 'pending' FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(suggestion) = suggestion else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let submitted_url: String = suggestion.get("submitted_url");
        sqlx::query(
            "INSERT INTO sources (source_key, name, url, schedule_interval_seconds) \
             VALUES ($1, $2, $3, 300) ON CONFLICT (source_key) DO UPDATE SET \
                name = EXCLUDED.name, url = EXCLUDED.url, enabled = TRUE, \
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(format!("facebook:suggestion:{id}"))
        .bind(name.trim())
        .bind(submitted_url)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE source_suggestions SET status = 'approved', admin_note = $2, \
                reviewed_at = CURRENT_TIMESTAMP WHERE id = $1",
        )
        .bind(id)
        .bind(format!("Đã duyệt với tên {}", name.trim()))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn reject_source_suggestion(&self, id: i64, reason: &str) -> Result<bool> {
        if id <= 0 || reason.trim().is_empty() {
            bail!("source suggestion ID and rejection reason must be valid");
        }
        let affected = sqlx::query(
            "UPDATE source_suggestions SET status = 'rejected', admin_note = $2, \
                reviewed_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND status = 'pending'",
        )
        .bind(id)
        .bind(reason.trim().chars().take(1_000).collect::<String>())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected == 1)
    }

    pub async fn telegram_next_update_id(&self, consumer_key: &str) -> Result<i64> {
        if consumer_key.is_empty() {
            bail!("Telegram update consumer key must not be empty");
        }
        let next_update_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO telegram_update_state (consumer_key, next_update_id) VALUES ($1, 0) \
             ON CONFLICT (consumer_key) DO UPDATE SET consumer_key = EXCLUDED.consumer_key \
             RETURNING next_update_id",
        )
        .bind(consumer_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(next_update_id)
    }

    pub async fn advance_telegram_update_id(
        &self,
        consumer_key: &str,
        next_update_id: i64,
    ) -> Result<()> {
        if consumer_key.is_empty() || next_update_id < 0 {
            bail!("Telegram update consumer key and offset must be valid");
        }
        sqlx::query(
            "INSERT INTO telegram_update_state (consumer_key, next_update_id) VALUES ($1, $2) \
             ON CONFLICT (consumer_key) DO UPDATE SET \
                next_update_id = GREATEST(telegram_update_state.next_update_id, EXCLUDED.next_update_id), \
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(consumer_key)
        .bind(next_update_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn pending_manual_reviews(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ManualReviewRecord>> {
        if !(1..=20).contains(&limit) || offset < 0 {
            bail!("manual review limit must be between 1 and 20 and offset must be non-negative");
        }
        let rows = sqlx::query(
            "SELECT classification.id AS classification_id, classification.post_id, \
                    source.name AS source_name, source.source_key, post.external_post_id, \
                    revision.canonical_url, revision.published_at, revision.text, \
                    revision.media, revision.outbound_links, revision.crawl_strategy, \
                    revision.fetched_at, classification.input_content_hash, \
                    classification.score, classification.confidence_basis_points, \
                    classification.matched_rules, classification.classified_at \
             FROM classifications AS classification \
             JOIN posts AS post ON post.id = classification.post_id \
             JOIN sources AS source ON source.id = post.source_id \
             JOIN post_revisions AS revision ON revision.post_id = post.id \
                AND revision.content_hash = classification.input_content_hash \
             LEFT JOIN manual_review_resolutions AS resolution \
                ON resolution.classification_id = classification.id \
             WHERE classification.decision = 'manual_review' \
               AND resolution.classification_id IS NULL \
             ORDER BY classification.classified_at, classification.id \
             LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(manual_review_from_row).collect()
    }

    pub async fn latest_posts(&self, limit: i64, offset: i64) -> Result<Vec<LatestPostRecord>> {
        if !(1..=20).contains(&limit) || offset < 0 {
            bail!("latest post limit must be between 1 and 20 and offset must be non-negative");
        }
        let rows = sqlx::query(
            "SELECT post.id AS post_id, source.name AS source_name, source.source_key, \
                    post.external_post_id, post.canonical_url, post.published_at, post.text, \
                    post.media, post.outbound_links, post.current_content_hash, \
                    post.crawl_strategy, post.last_seen_at \
             FROM posts AS post JOIN sources AS source ON source.id = post.source_id \
             WHERE source.enabled ORDER BY post.published_at DESC, post.id DESC \
             LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(latest_post_from_row).collect()
    }

    pub async fn latest_post(&self, database_post_id: i64) -> Result<Option<LatestPostRecord>> {
        if database_post_id <= 0 {
            bail!("latest post database ID must be positive");
        }
        let row = sqlx::query(
            "SELECT post.id AS post_id, source.name AS source_name, source.source_key, \
                    post.external_post_id, post.canonical_url, post.published_at, post.text, \
                    post.media, post.outbound_links, post.current_content_hash, \
                    post.crawl_strategy, post.last_seen_at \
             FROM posts AS post JOIN sources AS source ON source.id = post.source_id \
             WHERE source.enabled AND post.id = $1",
        )
        .bind(database_post_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(latest_post_from_row).transpose()
    }

    pub async fn manual_review(
        &self,
        classification_id: i64,
    ) -> Result<Option<ManualReviewRecord>> {
        if classification_id <= 0 {
            bail!("manual review classification ID must be positive");
        }
        let row = sqlx::query(
            "SELECT classification.id AS classification_id, classification.post_id, \
                    source.name AS source_name, source.source_key, post.external_post_id, \
                    revision.canonical_url, revision.published_at, revision.text, \
                    revision.media, revision.outbound_links, revision.crawl_strategy, \
                    revision.fetched_at, classification.input_content_hash, \
                    classification.score, classification.confidence_basis_points, \
                    classification.matched_rules, classification.classified_at \
             FROM classifications AS classification \
             JOIN posts AS post ON post.id = classification.post_id \
             JOIN sources AS source ON source.id = post.source_id \
             JOIN post_revisions AS revision ON revision.post_id = post.id \
                AND revision.content_hash = classification.input_content_hash \
             LEFT JOIN manual_review_resolutions AS resolution \
                ON resolution.classification_id = classification.id \
             WHERE classification.id = $1 AND classification.decision = 'manual_review' \
               AND resolution.classification_id IS NULL",
        )
        .bind(classification_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(manual_review_from_row).transpose()
    }

    pub async fn inherit_duplicate_manual_review_resolution(
        &self,
        classification_id: i64,
    ) -> Result<bool> {
        if classification_id <= 0 {
            bail!("manual review classification ID must be positive");
        }
        let inserted = sqlx::query(
            "WITH target AS ( \
                SELECT classification.id, classification.post_id, \
                       classification.input_content_hash, post.source_id, \
                       revision.published_at \
                FROM classifications AS classification \
                JOIN posts AS post ON post.id = classification.post_id \
                JOIN post_revisions AS revision ON revision.post_id = classification.post_id \
                   AND revision.content_hash = classification.input_content_hash \
                LEFT JOIN manual_review_resolutions AS resolution \
                   ON resolution.classification_id = classification.id \
                WHERE classification.id = $1 AND classification.decision = 'manual_review' \
                  AND resolution.classification_id IS NULL \
             ), prior AS ( \
                SELECT prior_classification.id, prior_resolution.reviewed_by_chat_id \
                FROM target \
                JOIN classifications AS prior_classification ON prior_classification.id <> target.id \
                JOIN posts AS prior_post ON prior_post.id = prior_classification.post_id \
                   AND prior_post.source_id = target.source_id \
                JOIN post_revisions AS prior_revision \
                   ON prior_revision.post_id = prior_classification.post_id \
                  AND prior_revision.content_hash = prior_classification.input_content_hash \
                JOIN manual_review_resolutions AS prior_resolution \
                   ON prior_resolution.classification_id = prior_classification.id \
                WHERE prior_classification.post_id = target.post_id \
                   OR (prior_classification.input_content_hash = target.input_content_hash \
                       AND prior_revision.published_at = target.published_at) \
                ORDER BY prior_resolution.reviewed_at, prior_classification.id \
                LIMIT 1 \
             ) \
             INSERT INTO manual_review_resolutions \
                (classification_id, action, reviewed_by_chat_id, reason) \
             SELECT target.id, 'skip', prior.reviewed_by_chat_id, \
                    'duplicate content inherited from classification ' || prior.id \
             FROM target CROSS JOIN prior \
             ON CONFLICT (classification_id) DO NOTHING",
        )
        .bind(classification_id)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1;
        Ok(inserted)
    }

    pub async fn resolve_manual_review(
        &self,
        classification_id: i64,
        actor_chat_id: i64,
        authorized_admin_chat_id: i64,
        action: ManualReviewAction,
        reason: Option<&str>,
        notification: Option<ManualReviewNotification<'_>>,
    ) -> Result<ManualReviewResolutionOutcome> {
        if classification_id <= 0 || actor_chat_id == 0 || authorized_admin_chat_id == 0 {
            bail!("manual review IDs must be valid");
        }
        if actor_chat_id != authorized_admin_chat_id {
            bail!("manual review action is restricted to the configured administrator");
        }
        let reason = reason.map(str::trim).filter(|value| !value.is_empty());
        if reason.is_some_and(|value| value.chars().count() > 1_000) {
            bail!("manual review reason exceeds 1000 characters");
        }
        match (action, notification) {
            (ManualReviewAction::Send, Some(content))
                if (1..=4_096).contains(&content.message_text.chars().count())
                    && !content.post_url.is_empty() => {}
            (ManualReviewAction::Skip, None) => {}
            (ManualReviewAction::Send, _) => {
                bail!("sending a manual review requires a valid Telegram message and post URL")
            }
            (ManualReviewAction::Skip, _) => {
                bail!("skipping a manual review cannot create a Telegram message or post URL")
            }
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT classification.post_id, classification.decision, resolution.action \
             FROM classifications AS classification \
             LEFT JOIN manual_review_resolutions AS resolution \
                ON resolution.classification_id = classification.id \
             WHERE classification.id = $1 FOR UPDATE OF classification",
        )
        .bind(classification_id)
        .fetch_optional(&mut *transaction)
        .await?
        .context("manual review classification was not found")?;
        let decision: String = row.get("decision");
        if decision != "manual_review" {
            bail!("classification is not eligible for manual review");
        }
        if row.get::<Option<String>, _>("action").is_some() {
            transaction.rollback().await?;
            return Ok(ManualReviewResolutionOutcome::default());
        }
        let post_id: i64 = row.get("post_id");
        sqlx::query("SELECT id FROM posts WHERE id = $1 FOR UPDATE")
            .bind(post_id)
            .execute(&mut *transaction)
            .await?;
        let prior_campaign = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM campaigns WHERE post_id = $1 ORDER BY id LIMIT 1",
        )
        .bind(post_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if action == ManualReviewAction::Send && prior_campaign.is_some() {
            sqlx::query(
                "INSERT INTO manual_review_resolutions \
                 (classification_id, action, reviewed_by_chat_id, reason) \
                 VALUES ($1, 'skip', $2, 'post already has a notification campaign')",
            )
            .bind(classification_id)
            .bind(actor_chat_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(ManualReviewResolutionOutcome {
                resolved: true,
                ..ManualReviewResolutionOutcome::default()
            });
        }
        let action_name = match action {
            ManualReviewAction::Send => "send",
            ManualReviewAction::Skip => "skip",
        };
        sqlx::query(
            "INSERT INTO manual_review_resolutions \
             (classification_id, action, reviewed_by_chat_id, reason) VALUES ($1, $2, $3, $4)",
        )
        .bind(classification_id)
        .bind(action_name)
        .bind(actor_chat_id)
        .bind(reason)
        .execute(&mut *transaction)
        .await?;
        let mut outcome = ManualReviewResolutionOutcome {
            resolved: true,
            ..ManualReviewResolutionOutcome::default()
        };
        if let Some(notification) = notification {
            let campaign_id = sqlx::query_scalar::<_, i64>(
                "INSERT INTO campaigns \
                 (classification_id, post_id, message_text, post_url, action_url, explicit_drl) \
                 SELECT $1, $2, $3, $4, \
                    COALESCE(features.extracted -> 'form_links' ->> 0, revision.outbound_links ->> 0), \
                    COALESCE(features.explicit_drl, FALSE) \
                 FROM classifications AS classification \
                 LEFT JOIN classification_features AS features ON features.classification_id = classification.id \
                 JOIN post_revisions AS revision ON revision.post_id = classification.post_id \
                    AND revision.content_hash = classification.input_content_hash \
                 WHERE classification.id = $1 RETURNING id",
            )
            .bind(classification_id)
            .bind(post_id)
            .bind(notification.message_text)
            .bind(notification.post_url)
            .fetch_one(&mut *transaction)
            .await?;
            outcome.campaign_created = true;
            outcome.deliveries_created = sqlx::query(
                "INSERT INTO deliveries (campaign_id, subscriber_id, available_at) \
                 SELECT $1, subscriber.id, \
                    CASE \
                      WHEN subscriber.quiet_hours_enabled AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') >= 22 \
                        THEN (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 day 7 hours') AT TIME ZONE 'Asia/Bangkok' \
                      WHEN subscriber.quiet_hours_enabled AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') < 7 \
                        THEN (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') + INTERVAL '7 hours') AT TIME ZONE 'Asia/Bangkok' \
                      ELSE CURRENT_TIMESTAMP \
                    END \
                 FROM subscribers AS subscriber \
                 JOIN campaigns AS campaign ON campaign.id = $1 \
                 WHERE subscriber.active AND subscriber.onboarding_completed_at IS NOT NULL \
                   AND subscriber.delivery_mode = 'instant' \
                   AND (subscriber.notification_scope = 'all' OR campaign.explicit_drl) \
                 ON CONFLICT (campaign_id, subscriber_id) DO NOTHING",
            )
            .bind(campaign_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            sqlx::query(
                "INSERT INTO digest_items (subscriber_id, campaign_id) \
                 SELECT subscriber.id, $1 FROM subscribers AS subscriber \
                 JOIN campaigns AS campaign ON campaign.id = $1 \
                 WHERE subscriber.active AND subscriber.onboarding_completed_at IS NOT NULL \
                   AND subscriber.delivery_mode = 'daily' \
                   AND (subscriber.notification_scope = 'all' OR campaign.explicit_drl) \
                 ON CONFLICT (subscriber_id, campaign_id) DO NOTHING",
            )
            .bind(campaign_id)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn claim_notification_events(
        &self,
        owner: &str,
        limit: i64,
        lease_seconds: i64,
    ) -> Result<Vec<ClaimedNotificationEvent>> {
        if owner.is_empty() || limit <= 0 || lease_seconds <= 0 {
            bail!("owner, claim limit, and lease duration must be valid");
        }
        let rows = sqlx::query(
            "WITH pending AS ( \
                SELECT id FROM outbox_events \
                WHERE processed_at IS NULL AND event_type = 'classification.completed' \
                  AND available_at <= CURRENT_TIMESTAMP \
                  AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP) \
                ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT $1 \
             ) \
             UPDATE outbox_events AS event SET \
                lease_owner = $2, \
                lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                attempts = attempts + 1 \
             FROM pending WHERE event.id = pending.id \
             RETURNING event.id, event.event_key, event.event_type, event.payload, event.attempts",
        )
        .bind(limit)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ClaimedNotificationEvent {
                    id: row.get("id"),
                    event_key: row.get("event_key"),
                    event_type: row.get("event_type"),
                    payload: row.get("payload"),
                    attempts: u32::try_from(row.get::<i32, _>("attempts"))?,
                })
            })
            .collect()
    }

    pub async fn load_post_revision(
        &self,
        database_post_id: i64,
        source_id: &str,
        external_post_id: &str,
        content_hash: &str,
    ) -> Result<FacebookPost> {
        let row = sqlx::query(
            "SELECT post_revisions.canonical_url, \
                    post_revisions.published_at, post_revisions.text, post_revisions.media, \
                    post_revisions.outbound_links, post_revisions.crawl_strategy, \
                    post_revisions.fetched_at \
             FROM posts JOIN post_revisions ON post_revisions.post_id = posts.id \
             WHERE posts.id = $1 AND post_revisions.content_hash = $2",
        )
        .bind(database_post_id)
        .bind(content_hash)
        .fetch_optional(&self.pool)
        .await?
        .context("notification post revision was not found")?;
        Ok(FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: source_id.to_owned(),
            platform: "facebook".to_owned(),
            external_post_id: external_post_id.to_owned(),
            canonical_url: row.get("canonical_url"),
            published_at: row.get::<DateTime<Utc>, _>("published_at").to_rfc3339(),
            text: row.get("text"),
            media: serde_json::from_value(row.get("media"))?,
            outbound_links: serde_json::from_value(row.get("outbound_links"))?,
            content_hash: content_hash.to_owned(),
            crawl_strategy: row.get("crawl_strategy"),
            fetched_at: row.get::<DateTime<Utc>, _>("fetched_at").to_rfc3339(),
        })
    }

    pub async fn source_name_for_post(&self, database_post_id: i64) -> Result<String> {
        if database_post_id <= 0 {
            bail!("post ID must be positive");
        }
        sqlx::query_scalar(
            "SELECT sources.name FROM posts JOIN sources ON sources.id = posts.source_id WHERE posts.id = $1",
        )
        .bind(database_post_id)
        .fetch_optional(&self.pool)
        .await?
            .context("notification source was not found")
    }

    pub async fn portal_notice_cursor(&self) -> Result<Option<i64>> {
        sqlx::query_scalar(
            "SELECT last_seen_portal_id FROM portal_notice_state WHERE singleton = TRUE",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn portal_poll_state(&self) -> Result<Option<PortalPollState>> {
        let row = sqlx::query(
            "SELECT poll_mode, next_poll_at, burst_until, cooldown_reason, last_polled_at, \
                    last_poll_outcome, last_http_status \
             FROM portal_notice_state WHERE singleton = TRUE",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| PortalPollState {
            mode: row.get("poll_mode"),
            next_poll_at: row.get("next_poll_at"),
            burst_until: row.get("burst_until"),
            cooldown_reason: row.get("cooldown_reason"),
            last_polled_at: row.get("last_polled_at"),
            last_poll_outcome: row.get("last_poll_outcome"),
            last_http_status: row.get("last_http_status"),
        }))
    }

    pub async fn update_portal_poll_state(&self, state: &PortalPollState) -> Result<()> {
        let valid_shape = match state.mode.as_str() {
            "steady" => state.burst_until.is_none() && state.cooldown_reason.is_none(),
            "burst" => state.burst_until.is_some() && state.cooldown_reason.is_none(),
            "cooldown" => state.burst_until.is_none() && state.cooldown_reason.is_some(),
            _ => false,
        };
        if !valid_shape
            || state
                .cooldown_reason
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.chars().count() > 100)
            || state
                .last_poll_outcome
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.chars().count() > 100)
            || state
                .last_http_status
                .is_some_and(|status| !(100..=599).contains(&status))
        {
            bail!("Portal poll state is invalid");
        }
        let rows = sqlx::query(
            "UPDATE portal_notice_state SET poll_mode = $1, next_poll_at = $2, \
                    burst_until = $3, cooldown_reason = $4, last_polled_at = $5, \
                    last_poll_outcome = $6, last_http_status = $7 \
             WHERE singleton = TRUE",
        )
        .bind(&state.mode)
        .bind(state.next_poll_at)
        .bind(state.burst_until)
        .bind(&state.cooldown_reason)
        .bind(state.last_polled_at)
        .bind(&state.last_poll_outcome)
        .bind(state.last_http_status)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if rows != 1 {
            bail!("Portal notice cursor is not initialized");
        }
        Ok(())
    }

    pub async fn initialize_portal_notice_cursor(&self, latest_portal_id: i64) -> Result<bool> {
        if latest_portal_id < 0 {
            bail!("Portal notice cursor cannot be negative");
        }
        let inserted = sqlx::query(
            "INSERT INTO portal_notice_state (singleton, last_seen_portal_id) VALUES (TRUE, $1) \
             ON CONFLICT (singleton) DO NOTHING",
        )
        .bind(latest_portal_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(inserted == 1)
    }

    pub async fn plan_portal_notice(
        &self,
        notice: &PortalNoticeRecord<'_>,
        message_text: &str,
    ) -> Result<PortalNoticePlanOutcome> {
        validate_portal_notice(notice, message_text)?;
        let mut transaction = self.pool.begin().await?;
        let cursor = sqlx::query_scalar::<_, i64>(
            "SELECT last_seen_portal_id FROM portal_notice_state \
             WHERE singleton = TRUE FOR UPDATE",
        )
        .fetch_optional(&mut *transaction)
        .await?
        .context("Portal notice cursor is not initialized")?;
        if notice.portal_id <= cursor {
            transaction.rollback().await?;
            return Ok(PortalNoticePlanOutcome {
                skipped: true,
                ..PortalNoticePlanOutcome::default()
            });
        }
        let notice_created = sqlx::query(
            "INSERT INTO portal_notices \
             (portal_id, title, displayed_at, article_url, attachment_url, \
              attachment_file_name, attachment_content_type) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (portal_id) DO NOTHING",
        )
        .bind(notice.portal_id)
        .bind(notice.title)
        .bind(notice.displayed_at)
        .bind(notice.article_url)
        .bind(notice.attachment_url)
        .bind(notice.attachment_file_name)
        .bind(notice.attachment_content_type)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        let campaign_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO campaigns \
             (portal_notice_id, message_text, post_url, attachment_url, \
              attachment_file_name, attachment_content_type) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (portal_notice_id) DO NOTHING RETURNING id",
        )
        .bind(notice.portal_id)
        .bind(message_text)
        .bind(notice.article_url.or(notice.attachment_url))
        .bind(notice.attachment_url)
        .bind(notice.attachment_file_name)
        .bind(notice.attachment_content_type)
        .fetch_optional(&mut *transaction)
        .await?;
        let mut outcome = PortalNoticePlanOutcome {
            notice_created,
            campaign_created: campaign_id.is_some(),
            ..PortalNoticePlanOutcome::default()
        };
        if let Some(campaign_id) = campaign_id {
            outcome.deliveries_created = sqlx::query(
                "INSERT INTO deliveries (campaign_id, subscriber_id) \
                 SELECT $1, subscriber.id FROM subscribers AS subscriber \
                 WHERE subscriber.onboarding_completed_at IS NOT NULL \
                   AND (subscriber.active OR subscriber.deactivated_reason = $2) \
                 ON CONFLICT (campaign_id, subscriber_id) DO NOTHING",
            )
            .bind(campaign_id)
            .bind(USER_STOP_REASON)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        } else {
            outcome.skipped = true;
        }
        sqlx::query(
            "UPDATE portal_notice_state SET last_seen_portal_id = GREATEST(last_seen_portal_id, $1), \
                updated_at = CURRENT_TIMESTAMP WHERE singleton = TRUE",
        )
        .bind(notice.portal_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn archive_portal_notice(&self, notice: &PortalNoticeRecord<'_>) -> Result<bool> {
        validate_portal_notice_record(notice)?;
        let inserted = sqlx::query(
            "INSERT INTO portal_notices \
             (portal_id, title, displayed_at, article_url, attachment_url, \
              attachment_file_name, attachment_content_type) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (portal_id) DO NOTHING",
        )
        .bind(notice.portal_id)
        .bind(notice.title)
        .bind(notice.displayed_at)
        .bind(notice.article_url)
        .bind(notice.attachment_url)
        .bind(notice.attachment_file_name)
        .bind(notice.attachment_content_type)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(inserted == 1)
    }

    pub async fn portal_notice_history(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<PortalNoticeHistoryRecord>> {
        if !(1..=20).contains(&limit) || offset < 0 {
            bail!(
                "Portal notice history limit must be between 1 and 20 and offset must be non-negative"
            );
        }
        let rows = sqlx::query(
            "SELECT portal_id, title, displayed_at, article_url, attachment_url, \
                    attachment_content_type, discovered_at \
             FROM portal_notices ORDER BY portal_id DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| PortalNoticeHistoryRecord {
                portal_id: row.get("portal_id"),
                title: row.get("title"),
                displayed_at: row.get("displayed_at"),
                article_url: row.get("article_url"),
                attachment_url: row.get("attachment_url"),
                attachment_content_type: row.get("attachment_content_type"),
                discovered_at: row.get("discovered_at"),
            })
            .collect())
    }

    pub async fn portal_notice_history_item(
        &self,
        portal_id: i64,
    ) -> Result<Option<PortalNoticeHistoryRecord>> {
        if portal_id <= 0 {
            bail!("Portal notice ID must be positive");
        }
        let row = sqlx::query(
            "SELECT portal_id, title, displayed_at, article_url, attachment_url, \
                    attachment_content_type, discovered_at \
             FROM portal_notices WHERE portal_id = $1",
        )
        .bind(portal_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| PortalNoticeHistoryRecord {
            portal_id: row.get("portal_id"),
            title: row.get("title"),
            displayed_at: row.get("displayed_at"),
            article_url: row.get("article_url"),
            attachment_url: row.get("attachment_url"),
            attachment_content_type: row.get("attachment_content_type"),
            discovered_at: row.get("discovered_at"),
        }))
    }

    pub async fn set_portal_notice_attachment(
        &self,
        portal_id: i64,
        attachment_url: &str,
        attachment_content_type: &str,
    ) -> Result<bool> {
        if portal_id <= 0 {
            bail!("Portal notice ID must be positive");
        }
        let record = PortalNoticeRecord {
            portal_id,
            title: "attachment",
            displayed_at: Utc::now(),
            article_url: None,
            attachment_url: Some(attachment_url),
            attachment_file_name: None,
            attachment_content_type: Some(attachment_content_type),
        };
        validate_portal_notice_record(&record)?;
        let mut transaction = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE portal_notices SET attachment_url = $2, attachment_content_type = $3 \
             WHERE portal_id = $1 AND attachment_url IS NULL",
        )
        .bind(portal_id)
        .bind(attachment_url)
        .bind(attachment_content_type)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        if updated {
            sqlx::query(
                "UPDATE campaigns SET attachment_url = $2, attachment_content_type = $3 \
                 WHERE portal_notice_id = $1 AND attachment_url IS NULL AND telegram_file_id IS NULL",
            )
            .bind(portal_id)
            .bind(attachment_url)
            .bind(attachment_content_type)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(updated)
    }

    pub async fn plan_notification(
        &self,
        event: &ClaimedNotificationEvent,
        owner: &str,
        database_classification_id: i64,
        database_post_id: i64,
        content: Option<&NotificationContent<'_>>,
    ) -> Result<NotificationPlanOutcome> {
        let mut transaction = self.pool.begin().await?;
        ensure_outbox_lease(&mut transaction, event.id, owner).await?;
        validate_notification_event(
            event,
            database_classification_id,
            database_post_id,
            content.is_some(),
        )?;
        let mut outcome = NotificationPlanOutcome::default();
        if let Some(content) = content {
            let message_text = content.message_text;
            let length = message_text.chars().count();
            if !(1..=4_096).contains(&length) {
                bail!("Telegram notification must contain 1 to 4096 characters");
            }
            sqlx::query("SELECT id FROM posts WHERE id = $1 FOR UPDATE")
                .bind(database_post_id)
                .execute(&mut *transaction)
                .await?;
            let existing = sqlx::query(
                "SELECT id, classification_id FROM campaigns \
                 WHERE post_id = $1 ORDER BY (classification_id = $2) DESC, id LIMIT 1",
            )
            .bind(database_post_id)
            .bind(database_classification_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let campaign_id = match existing {
                Some(row)
                    if row.get::<i64, _>("classification_id") == database_classification_id =>
                {
                    row.get("id")
                }
                Some(_) => {
                    outcome.skipped = true;
                    mark_outbox_processed(&mut transaction, event.id, owner).await?;
                    transaction.commit().await?;
                    return Ok(outcome);
                }
                None => {
                    outcome.campaign_created = true;
                    sqlx::query_scalar::<_, i64>(
                        "INSERT INTO campaigns \
                         (classification_id, post_id, message_text, post_url, action_url, explicit_drl) \
                         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
                    )
                    .bind(database_classification_id)
                    .bind(database_post_id)
                    .bind(message_text)
                    .bind(content.post_url)
                    .bind(content.action_url)
                    .bind(content.explicit_drl)
                    .fetch_one(&mut *transaction)
                    .await?
                }
            };
            outcome.deliveries_created = sqlx::query(
                "INSERT INTO deliveries (campaign_id, subscriber_id, available_at) \
                 SELECT $1, id, \
                    CASE \
                      WHEN quiet_hours_enabled AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') >= 22 \
                        THEN (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 day 7 hours') AT TIME ZONE 'Asia/Bangkok' \
                      WHEN quiet_hours_enabled AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') < 7 \
                        THEN (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') + INTERVAL '7 hours') AT TIME ZONE 'Asia/Bangkok' \
                      ELSE CURRENT_TIMESTAMP \
                    END \
                 FROM subscribers WHERE active AND onboarding_completed_at IS NOT NULL \
                   AND delivery_mode = 'instant' \
                   AND (notification_scope = 'all' OR $2) \
                 ON CONFLICT (campaign_id, subscriber_id) DO NOTHING",
            )
            .bind(campaign_id)
            .bind(content.explicit_drl)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            sqlx::query(
                "INSERT INTO digest_items (subscriber_id, campaign_id) \
                 SELECT id, $1 FROM subscribers WHERE active AND onboarding_completed_at IS NOT NULL \
                   AND delivery_mode = 'daily' AND (notification_scope = 'all' OR $2) \
                 ON CONFLICT (subscriber_id, campaign_id) DO NOTHING",
            )
            .bind(campaign_id)
            .bind(content.explicit_drl)
            .execute(&mut *transaction)
            .await?;
        } else {
            outcome.skipped = true;
        }
        mark_outbox_processed(&mut transaction, event.id, owner).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn fail_notification_event(
        &self,
        event: &ClaimedNotificationEvent,
        owner: &str,
        error: &str,
        max_attempts: u32,
        retry_delay_seconds: u64,
    ) -> Result<FailureDisposition> {
        self.fail_classification_event(
            &ClaimedClassificationEvent {
                id: event.id,
                event_key: event.event_key.clone(),
                event_type: event.event_type.clone(),
                payload: event.payload.clone(),
                attempts: event.attempts,
            },
            owner,
            error,
            max_attempts,
            retry_delay_seconds,
        )
        .await
    }

    pub async fn prepare_due_digests(&self, limit: i64) -> Result<DigestPreparationOutcome> {
        if limit <= 0 {
            bail!("digest preparation limit must be positive");
        }
        let subscriber_ids = sqlx::query_scalar::<_, i64>(
            "SELECT subscribers.id FROM subscribers \
             WHERE active AND delivery_mode = 'daily' AND next_digest_at <= CURRENT_TIMESTAMP \
               AND (NOT quiet_hours_enabled OR (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok')::time >= TIME '07:00' \
                    AND (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok')::time < TIME '22:00') \
               AND EXISTS (SELECT 1 FROM digest_items WHERE subscriber_id = subscribers.id AND digest_batch_id IS NULL) \
             ORDER BY next_digest_at, id LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut outcome = DigestPreparationOutcome::default();
        for subscriber_id in subscriber_ids {
            let mut transaction = self.pool.begin().await?;
            let rows = sqlx::query(
            "SELECT item.campaign_id, campaign.post_id, \
                    campaign.message_text, campaign.action_url, campaign.post_url \
                 FROM digest_items AS item JOIN campaigns AS campaign ON campaign.id = item.campaign_id \
                 WHERE item.subscriber_id = $1 AND item.digest_batch_id IS NULL \
                 ORDER BY item.created_at, item.campaign_id LIMIT $2 FOR UPDATE OF item",
        )
        .bind(subscriber_id)
        .bind(DIGEST_ITEM_FETCH_LIMIT + 1)
            .fetch_all(&mut *transaction)
            .await?;
            if rows.is_empty() {
                transaction.rollback().await?;
                continue;
            }
            let has_unloaded_items = rows.len() > usize::try_from(DIGEST_ITEM_FETCH_LIMIT)?;
            let candidates = rows
                .into_iter()
                .take(usize::try_from(DIGEST_ITEM_FETCH_LIMIT)?)
                .map(|row| {
                    let text: String = row.get("message_text");
                    let link = row
                        .get::<Option<String>, _>("action_url")
                        .or_else(|| row.get::<Option<String>, _>("post_url"))
                        .map(|value| truncate_chars(&value, DIGEST_LINK_LIMIT))
                        .filter(|value| !value.is_empty());
                    DigestCandidate {
                        campaign_id: row.get("campaign_id"),
                        post_id: row.get("post_id"),
                        summary: truncate_chars(&text, DIGEST_SUMMARY_LIMIT),
                        link,
                    }
                })
                .collect::<Vec<_>>();
            let Some(plan) = build_digest_batch(candidates, has_unloaded_items)? else {
                transaction.rollback().await?;
                continue;
            };
            let batch_id = sqlx::query_scalar::<_, i64>(
                "INSERT INTO digest_batches (subscriber_id, message_text) VALUES ($1, $2) RETURNING id",
            )
            .bind(subscriber_id)
            .bind(&plan.message_text)
            .fetch_one(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE digest_items SET digest_batch_id = $3 WHERE subscriber_id = $1 AND campaign_id = ANY($2)",
            )
            .bind(subscriber_id)
            .bind(&plan.campaign_ids)
            .bind(batch_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE subscribers SET next_digest_at = \
                    (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 day 7 hours 30 minutes') AT TIME ZONE 'Asia/Bangkok', \
                    updated_at = CURRENT_TIMESTAMP WHERE id = $1",
            )
            .bind(subscriber_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            outcome.batches_created += 1;
            outcome.items_batched += u64::try_from(plan.campaign_ids.len())?;
            outcome.duplicate_items_collapsed += u64::try_from(plan.duplicate_items_collapsed)?;
        }
        Ok(outcome)
    }

    pub async fn claim_digest_deliveries(
        &self,
        owner: &str,
        limit: i64,
        lease_seconds: i64,
    ) -> Result<Vec<ClaimedDigestDelivery>> {
        if owner.is_empty() || limit <= 0 || lease_seconds <= 0 {
            bail!("digest claim values are invalid");
        }
        let rows = sqlx::query(
            "WITH pending AS ( \
                SELECT batch.id FROM digest_batches AS batch \
                JOIN subscribers ON subscribers.id = batch.subscriber_id \
                WHERE batch.status IN ('pending', 'retry', 'sending') AND batch.available_at <= CURRENT_TIMESTAMP \
                  AND subscribers.active AND (batch.lease_expires_at IS NULL OR batch.lease_expires_at <= CURRENT_TIMESTAMP) \
                ORDER BY batch.available_at, batch.id FOR UPDATE OF batch SKIP LOCKED LIMIT $1 \
             ) UPDATE digest_batches AS batch SET status = 'sending', lease_owner = $2, \
                lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP \
             FROM pending WHERE batch.id = pending.id \
             RETURNING batch.id, batch.subscriber_id, batch.message_text, batch.attempts, \
                (SELECT telegram_chat_id FROM subscribers WHERE id = batch.subscriber_id) AS telegram_chat_id",
        )
        .bind(limit)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ClaimedDigestDelivery {
                    id: row.get("id"),
                    subscriber_id: row.get("subscriber_id"),
                    telegram_chat_id: row.get("telegram_chat_id"),
                    message_text: row.get("message_text"),
                    attempt: u32::try_from(row.get::<i32, _>("attempts"))?,
                })
            })
            .collect()
    }

    pub async fn complete_digest_delivery(
        &self,
        delivery: &ClaimedDigestDelivery,
        owner: &str,
        telegram_message_id: i64,
    ) -> Result<()> {
        let affected = sqlx::query(
            "UPDATE digest_batches SET status = 'sent', telegram_message_id = $3, sent_at = CURRENT_TIMESTAMP, \
                lease_owner = NULL, lease_expires_at = NULL, last_error = NULL, failure_class = NULL, \
                updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND lease_owner = $2 AND lease_expires_at > CURRENT_TIMESTAMP",
        )
        .bind(delivery.id)
        .bind(owner)
        .bind(telegram_message_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected != 1 {
            bail!("digest delivery lease was lost");
        }
        Ok(())
    }

    pub async fn retry_digest_delivery(
        &self,
        delivery: &ClaimedDigestDelivery,
        owner: &str,
        delay_seconds: u64,
        detail: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE digest_batches SET status = 'retry', available_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                lease_owner = NULL, lease_expires_at = NULL, last_error = $4, failure_class = NULL, \
                updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND lease_owner = $2",
        )
        .bind(delivery.id)
        .bind(owner)
        .bind(i64::try_from(delay_seconds.max(1))?)
        .bind(detail.chars().take(1_000).collect::<String>())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_digest_delivery(
        &self,
        delivery: &ClaimedDigestDelivery,
        owner: &str,
        detail: &str,
        failure_class: DeliveryFailureClass,
    ) -> Result<()> {
        let detail = detail.chars().take(1_000).collect::<String>();
        let mut transaction = self.pool.begin().await?;
        let affected = sqlx::query(
            "UPDATE digest_batches SET status = 'failed', lease_owner = NULL, lease_expires_at = NULL, \
                last_error = $3, failure_class = $4, updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND lease_owner = $2",
        )
        .bind(delivery.id)
        .bind(owner)
        .bind(&detail)
        .bind(failure_class.as_str())
        .execute(&mut *transaction)
        .await?;
        if affected.rows_affected() != 1 {
            bail!("digest delivery lease was lost");
        }
        if failure_class == DeliveryFailureClass::RecipientUnavailable {
            sqlx::query(
                "UPDATE subscribers SET active = FALSE, deactivated_reason = $2, \
                    updated_at = CURRENT_TIMESTAMP WHERE id = $1",
            )
            .bind(delivery.subscriber_id)
            .bind(&detail)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn claim_deliveries(
        &self,
        owner: &str,
        limit: i64,
        lease_seconds: i64,
    ) -> Result<Vec<ClaimedDelivery>> {
        if owner.is_empty() || limit <= 0 || lease_seconds <= 0 {
            bail!("owner, claim limit, and lease duration must be valid");
        }
        let rows = sqlx::query(
            "WITH pending AS ( \
                SELECT delivery.id FROM deliveries AS delivery \
                JOIN subscribers ON subscribers.id = delivery.subscriber_id \
                JOIN campaigns AS campaign ON campaign.id = delivery.campaign_id \
                WHERE delivery.status IN ('pending', 'retry', 'sending') \
                  AND delivery.available_at <= CURRENT_TIMESTAMP \
                  AND (subscribers.active OR (campaign.portal_notice_id IS NOT NULL \
                       AND subscribers.deactivated_reason = $4)) \
                  AND subscribers.next_send_at <= CURRENT_TIMESTAMP \
                  AND (campaign.portal_notice_id IS NULL OR campaign.telegram_file_id IS NOT NULL \
                       OR delivery.id = ( \
                           SELECT seed.id FROM deliveries AS seed \
                           JOIN subscribers AS seed_subscriber \
                             ON seed_subscriber.id = seed.subscriber_id \
                           WHERE seed.campaign_id = delivery.campaign_id \
                             AND seed.status IN ('pending', 'retry', 'sending') \
                             AND seed.available_at <= CURRENT_TIMESTAMP \
                             AND (seed_subscriber.active \
                                  OR seed_subscriber.deactivated_reason = $4) \
                             AND seed_subscriber.next_send_at <= CURRENT_TIMESTAMP \
                             AND (seed.lease_expires_at IS NULL \
                                  OR seed.lease_expires_at <= CURRENT_TIMESTAMP) \
                           ORDER BY seed.available_at, seed.id LIMIT 1 \
                       )) \
                  AND (delivery.lease_expires_at IS NULL \
                       OR delivery.lease_expires_at <= CURRENT_TIMESTAMP) \
                  AND delivery.id = ( \
                      SELECT candidate.id FROM deliveries AS candidate \
                      JOIN campaigns AS candidate_campaign ON candidate_campaign.id = candidate.campaign_id \
                      WHERE candidate.subscriber_id = delivery.subscriber_id \
                        AND candidate.status IN ('pending', 'retry', 'sending') \
                        AND candidate.available_at <= CURRENT_TIMESTAMP \
                        AND (subscribers.active OR (candidate_campaign.portal_notice_id IS NOT NULL \
                             AND subscribers.deactivated_reason = $4)) \
                        AND (candidate.lease_expires_at IS NULL \
                             OR candidate.lease_expires_at <= CURRENT_TIMESTAMP) \
                      ORDER BY candidate.available_at, candidate.id LIMIT 1 \
                  ) \
                ORDER BY delivery.available_at, delivery.id \
                FOR UPDATE OF delivery SKIP LOCKED LIMIT $1 \
             ) \
             UPDATE deliveries AS delivery SET status = 'sending', lease_owner = $2, \
                lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP \
             FROM pending WHERE delivery.id = pending.id \
             RETURNING delivery.id, delivery.campaign_id, delivery.subscriber_id, delivery.attempts, \
                (SELECT telegram_chat_id FROM subscribers WHERE id = delivery.subscriber_id) \
                    AS telegram_chat_id, \
                (SELECT message_text FROM campaigns WHERE id = delivery.campaign_id) AS message_text, \
                (SELECT post_url FROM campaigns WHERE id = delivery.campaign_id) AS post_url, \
                (SELECT action_url FROM campaigns WHERE id = delivery.campaign_id) AS action_url, \
                (SELECT portal_notice_id FROM campaigns WHERE id = delivery.campaign_id) AS portal_notice_id, \
                (SELECT attachment_url FROM campaigns WHERE id = delivery.campaign_id) AS attachment_url, \
                (SELECT attachment_file_name FROM campaigns WHERE id = delivery.campaign_id) AS attachment_file_name, \
                (SELECT attachment_content_type FROM campaigns WHERE id = delivery.campaign_id) AS attachment_content_type, \
                (SELECT telegram_file_id FROM campaigns WHERE id = delivery.campaign_id) AS telegram_file_id",
        )
        .bind(limit)
        .bind(owner)
        .bind(lease_seconds)
        .bind(USER_STOP_REASON)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ClaimedDelivery {
                    id: row.get("id"),
                    campaign_id: row.get("campaign_id"),
                    subscriber_id: row.get("subscriber_id"),
                    telegram_chat_id: row.get("telegram_chat_id"),
                    message_text: row.get("message_text"),
                    post_url: row.get("post_url"),
                    action_url: row.get("action_url"),
                    portal_notice_id: row.get("portal_notice_id"),
                    attachment_url: row.get("attachment_url"),
                    attachment_file_name: row.get("attachment_file_name"),
                    attachment_content_type: row.get("attachment_content_type"),
                    telegram_file_id: row.get("telegram_file_id"),
                    attempt: u32::try_from(row.get::<i32, _>("attempts"))?,
                })
            })
            .collect()
    }

    pub async fn complete_delivery(
        &self,
        delivery: &ClaimedDelivery,
        owner: &str,
        telegram_message_id: i64,
        telegram_file_id: Option<&str>,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        ensure_delivery_lease(&mut transaction, delivery.id, owner).await?;
        insert_delivery_attempt(&mut transaction, delivery, "sent", None, None).await?;
        sqlx::query(
            "UPDATE deliveries SET status = 'sent', telegram_message_id = $3, \
                sent_at = CURRENT_TIMESTAMP, lease_owner = NULL, lease_expires_at = NULL, \
                last_error = NULL, failure_class = NULL, updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND lease_owner = $2",
        )
        .bind(delivery.id)
        .bind(owner)
        .bind(telegram_message_id)
        .execute(&mut *transaction)
        .await?;
        if let Some(telegram_file_id) = telegram_file_id {
            if telegram_file_id.is_empty() || telegram_file_id.chars().count() > 512 {
                bail!("Telegram file ID is invalid");
            }
            sqlx::query(
                "UPDATE campaigns SET telegram_file_id = COALESCE(telegram_file_id, $2) \
                 WHERE id = $1 AND portal_notice_id IS NOT NULL",
            )
            .bind(delivery.campaign_id)
            .bind(telegram_file_id)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "INSERT INTO product_events (subscriber_id, event_type, campaign_id) VALUES ($1, 'notification_delivered', $2)",
        )
        .bind(delivery.subscriber_id)
        .bind(delivery.campaign_id)
        .execute(&mut *transaction)
        .await?;
        advance_subscriber_rate_limit(&mut transaction, delivery.subscriber_id).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn retry_delivery(
        &self,
        delivery: &ClaimedDelivery,
        owner: &str,
        delay_seconds: u64,
        error_code: Option<i32>,
        detail: &str,
    ) -> Result<()> {
        if delay_seconds == 0 {
            bail!("delivery retry delay must be at least 1 second");
        }
        let detail = detail.chars().take(1_000).collect::<String>();
        let mut transaction = self.pool.begin().await?;
        ensure_delivery_lease(&mut transaction, delivery.id, owner).await?;
        insert_delivery_attempt(
            &mut transaction,
            delivery,
            "retry",
            error_code,
            Some(&detail),
        )
        .await?;
        sqlx::query(
            "UPDATE deliveries SET status = 'retry', \
                available_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision), \
                lease_owner = NULL, lease_expires_at = NULL, last_error = $4, failure_class = NULL, \
                updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND lease_owner = $2",
        )
        .bind(delivery.id)
        .bind(owner)
        .bind(i64::try_from(delay_seconds)?)
        .bind(detail)
        .execute(&mut *transaction)
        .await?;
        advance_subscriber_rate_limit(&mut transaction, delivery.subscriber_id).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn fail_delivery(
        &self,
        delivery: &ClaimedDelivery,
        owner: &str,
        error_code: Option<i32>,
        detail: &str,
        failure_class: DeliveryFailureClass,
    ) -> Result<()> {
        let detail = detail.chars().take(1_000).collect::<String>();
        let mut transaction = self.pool.begin().await?;
        ensure_delivery_lease(&mut transaction, delivery.id, owner).await?;
        insert_delivery_attempt(
            &mut transaction,
            delivery,
            "failed",
            error_code,
            Some(&detail),
        )
        .await?;
        sqlx::query(
            "UPDATE deliveries SET status = 'failed', lease_owner = NULL, \
                lease_expires_at = NULL, last_error = $3, failure_class = $4, \
                updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND lease_owner = $2",
        )
        .bind(delivery.id)
        .bind(owner)
        .bind(&detail)
        .bind(failure_class.as_str())
        .execute(&mut *transaction)
        .await?;
        if failure_class == DeliveryFailureClass::RecipientUnavailable {
            sqlx::query(
                "UPDATE subscribers SET active = FALSE, deactivated_reason = $2, \
                 updated_at = CURRENT_TIMESTAMP WHERE id = $1",
            )
            .bind(delivery.subscriber_id)
            .bind(&detail)
            .execute(&mut *transaction)
            .await?;
        } else {
            advance_subscriber_rate_limit(&mut transaction, delivery.subscriber_id).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn migrate_delivery_chat(
        &self,
        delivery: &ClaimedDelivery,
        owner: &str,
        new_chat_id: i64,
        detail: &str,
    ) -> Result<()> {
        if new_chat_id == 0 {
            bail!("migrated Telegram chat ID must not be zero");
        }
        let mut transaction = self.pool.begin().await?;
        ensure_delivery_lease(&mut transaction, delivery.id, owner).await?;
        let conflict = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM subscribers WHERE telegram_chat_id = $1 AND id <> $2)",
        )
        .bind(new_chat_id)
        .bind(delivery.subscriber_id)
        .fetch_one(&mut *transaction)
        .await?;
        if conflict {
            bail!("migrated Telegram chat ID already belongs to another subscriber");
        }
        sqlx::query(
            "UPDATE subscribers SET telegram_chat_id = $2, updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1",
        )
        .bind(delivery.subscriber_id)
        .bind(new_chat_id)
        .execute(&mut *transaction)
        .await?;
        let detail = detail.chars().take(1_000).collect::<String>();
        insert_delivery_attempt(
            &mut transaction,
            delivery,
            "chat_migrated",
            None,
            Some(&detail),
        )
        .await?;
        sqlx::query(
            "UPDATE deliveries SET status = 'retry', available_at = CURRENT_TIMESTAMP + INTERVAL '1 second', \
                lease_owner = NULL, lease_expires_at = NULL, last_error = $3, failure_class = NULL, \
                updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND lease_owner = $2",
        )
        .bind(delivery.id)
        .bind(owner)
        .bind(detail)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn apply_delivery_retention(
        &self,
        sent_delivery_days: i32,
        failed_delivery_days: i32,
        inactive_subscriber_days: i32,
    ) -> Result<DeliveryRetentionOutcome> {
        if sent_delivery_days <= 0 || failed_delivery_days <= 0 || inactive_subscriber_days <= 0 {
            bail!("delivery retention periods must be at least 1 day");
        }
        let mut transaction = self.pool.begin().await?;
        let deliveries_deleted = sqlx::query(
            "DELETE FROM deliveries WHERE \
                (status = 'sent' AND sent_at < CURRENT_TIMESTAMP - make_interval(days => $1)) \
                OR (status = 'failed' AND updated_at < CURRENT_TIMESTAMP - make_interval(days => $2))",
        )
        .bind(sent_delivery_days)
        .bind(failed_delivery_days)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let inactive_subscribers_deleted = sqlx::query(
            "DELETE FROM subscribers WHERE NOT active \
               AND deactivated_reason <> $2 \
               AND updated_at < CURRENT_TIMESTAMP - make_interval(days => $1) \
               AND NOT EXISTS (SELECT 1 FROM deliveries WHERE subscriber_id = subscribers.id)",
        )
        .bind(inactive_subscriber_days)
        .bind(USER_STOP_REASON)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        sqlx::query(
            "DELETE FROM donation_amount_input_state WHERE expires_at <= CURRENT_TIMESTAMP",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query("DELETE FROM user_feedback_input_state WHERE expires_at <= CURRENT_TIMESTAMP")
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "DELETE FROM product_events WHERE occurred_at < CURRENT_TIMESTAMP - INTERVAL '180 days'",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM notification_feedback WHERE updated_at < CURRENT_TIMESTAMP - INTERVAL '180 days'",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM user_feedback_messages WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '180 days'",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM digest_items WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '90 days'",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM digest_batches WHERE updated_at < CURRENT_TIMESTAMP - INTERVAL '90 days'",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(DeliveryRetentionOutcome {
            deliveries_deleted,
            inactive_subscribers_deleted,
        })
    }
}

async fn ensure_outbox_lease(
    transaction: &mut Transaction<'_, Postgres>,
    event_id: i64,
    owner: &str,
) -> Result<()> {
    let valid = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS( \
             SELECT 1 FROM outbox_events \
             WHERE id = $1 AND lease_owner = $2 AND lease_expires_at > CURRENT_TIMESTAMP \
               AND processed_at IS NULL \
         )",
    )
    .bind(event_id)
    .bind(owner)
    .fetch_one(&mut **transaction)
    .await?;
    if !valid {
        bail!("outbox lease is missing, expired, or owned by another worker");
    }
    Ok(())
}

async fn mark_outbox_processed(
    transaction: &mut Transaction<'_, Postgres>,
    event_id: i64,
    owner: &str,
) -> Result<()> {
    let affected = sqlx::query(
        "UPDATE outbox_events SET processed_at = CURRENT_TIMESTAMP, \
            lease_owner = NULL, lease_expires_at = NULL, last_error = NULL \
         WHERE id = $1 AND lease_owner = $2 AND processed_at IS NULL",
    )
    .bind(event_id)
    .bind(owner)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        bail!("failed to mark outbox event processed");
    }
    Ok(())
}

fn validate_classification(result: &ClassificationResult) -> Result<()> {
    if result.schema_version != CLASSIFICATION_SCHEMA_VERSION {
        bail!(
            "unsupported classification schema {}",
            result.schema_version
        );
    }
    if result.external_post_id.is_empty()
        || result.input_content_hash.is_empty()
        || result.classifier_version.is_empty()
        || !is_sha256(&result.config_hash)
        || result.confidence_basis_points > 10_000
    {
        bail!("classification contains invalid required values");
    }
    Ok(())
}

fn validate_classification_event(
    event: &ClaimedClassificationEvent,
    database_post_id: i64,
    result: &ClassificationResult,
) -> Result<()> {
    let payload_post_id = event
        .payload
        .get("database_post_id")
        .and_then(serde_json::Value::as_i64);
    let payload_post = event.payload.get("post");
    let payload_external_id = payload_post
        .and_then(|post| post.get("external_post_id"))
        .and_then(serde_json::Value::as_str);
    let payload_content_hash = payload_post
        .and_then(|post| post.get("content_hash"))
        .and_then(serde_json::Value::as_str);
    let payload_source_id = payload_post
        .and_then(|post| post.get("source_id"))
        .and_then(serde_json::Value::as_str);
    if payload_post_id != Some(database_post_id)
        || payload_external_id != Some(result.external_post_id.as_str())
        || payload_content_hash != Some(result.input_content_hash.as_str())
        || payload_source_id != Some(result.post_source_id.as_str())
    {
        bail!("classification result does not match claimed event payload");
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn decision_name(decision: &ClassificationDecision) -> &'static str {
    match decision {
        ClassificationDecision::Rejected => "rejected",
        ClassificationDecision::MatchedExplicit => "matched_explicit",
        ClassificationDecision::ManualReview => "manual_review",
    }
}

fn manual_review_from_row(row: PgRow) -> Result<ManualReviewRecord> {
    let confidence_basis_points = u16::try_from(row.get::<i32, _>("confidence_basis_points"))
        .context("manual review confidence is outside the supported range")?;
    Ok(ManualReviewRecord {
        classification_id: row.get("classification_id"),
        database_post_id: row.get("post_id"),
        source_name: row.get("source_name"),
        score: row.get("score"),
        confidence_basis_points,
        matched_rules: serde_json::from_value(row.get("matched_rules"))?,
        classified_at: row.get("classified_at"),
        post: FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: row.get("source_key"),
            platform: "facebook".to_owned(),
            external_post_id: row.get("external_post_id"),
            canonical_url: row.get("canonical_url"),
            published_at: row.get::<DateTime<Utc>, _>("published_at").to_rfc3339(),
            text: row.get("text"),
            media: serde_json::from_value(row.get("media"))?,
            outbound_links: serde_json::from_value(row.get("outbound_links"))?,
            content_hash: row.get("input_content_hash"),
            crawl_strategy: row.get("crawl_strategy"),
            fetched_at: row.get::<DateTime<Utc>, _>("fetched_at").to_rfc3339(),
        },
    })
}

fn latest_post_from_row(row: PgRow) -> Result<LatestPostRecord> {
    Ok(LatestPostRecord {
        database_post_id: row.get("post_id"),
        source_name: row.get("source_name"),
        post: FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: row.get("source_key"),
            platform: "facebook".to_owned(),
            external_post_id: row.get("external_post_id"),
            canonical_url: row.get("canonical_url"),
            published_at: row.get::<DateTime<Utc>, _>("published_at").to_rfc3339(),
            text: row.get("text"),
            media: serde_json::from_value(row.get("media"))?,
            outbound_links: serde_json::from_value(row.get("outbound_links"))?,
            content_hash: row.get("current_content_hash"),
            crawl_strategy: row.get("crawl_strategy"),
            fetched_at: row.get::<DateTime<Utc>, _>("last_seen_at").to_rfc3339(),
        },
    })
}

async fn ensure_lease(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: i64,
    owner: &str,
) -> Result<()> {
    let valid = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS( \
             SELECT 1 FROM sources \
             WHERE id = $1 AND lease_owner = $2 AND lease_expires_at > CURRENT_TIMESTAMP \
         )",
    )
    .bind(source_id)
    .bind(owner)
    .fetch_one(&mut **transaction)
    .await?;
    if !valid {
        bail!("source lease is missing, expired, or owned by another worker");
    }
    Ok(())
}

async fn insert_run(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: i64,
    report: &CrawlReport,
    fetched_at: DateTime<Utc>,
) -> Result<i64> {
    let post_count =
        i32::try_from(report.post_count).context("post count exceeds database range")?;
    sqlx::query_scalar(
        "INSERT INTO crawler_runs \
         (source_id, contract_source_id, fetched_at, health, selected_strategy, post_count) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(source_id)
    .bind(&report.source_id)
    .bind(fetched_at)
    .bind(&report.health)
    .bind(&report.selected_strategy)
    .bind(post_count)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to insert crawler run")
}

async fn insert_attempts(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: i64,
    report: &CrawlReport,
) -> Result<()> {
    for (ordinal, attempt) in report.attempts.iter().enumerate() {
        let newest_post_at = attempt
            .newest_post_at
            .as_deref()
            .map(|value| parse_timestamp(value, "attempt newest_post_at"))
            .transpose()?;
        let browser_metadata = attempt
            .browser
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?;
        sqlx::query(
            "INSERT INTO crawler_attempts \
             (crawler_run_id, ordinal, strategy, outcome, status, latency_ms, bytes_received, \
              final_url, posts_found, newest_post_at, parse_stats, error, browser_metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(run_id)
        .bind(i32::try_from(ordinal)?)
        .bind(&attempt.strategy)
        .bind(&attempt.outcome)
        .bind(attempt.status.map(i32::from))
        .bind(i64::try_from(attempt.latency_ms)?)
        .bind(i64::try_from(attempt.bytes_received)?)
        .bind(&attempt.final_url)
        .bind(i32::try_from(attempt.posts_found)?)
        .bind(newest_post_at)
        .bind(serde_json::to_value(&attempt.parse)?)
        .bind(&attempt.error)
        .bind(browser_metadata)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn persist_post(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: i64,
    post: &FacebookPost,
    emit_event: bool,
    allow_historical_events: bool,
    historical_cutoff: DateTime<Utc>,
    outcome: &mut PersistOutcome,
) -> Result<()> {
    if post.schema_version != POST_SCHEMA_VERSION {
        bail!("unsupported post schema {}", post.schema_version);
    }
    let published_at = parse_timestamp(&post.published_at, "post published_at")?;
    let fetched_at = parse_timestamp(&post.fetched_at, "post fetched_at")?;
    let mut existing = sqlx::query(
        "SELECT id, external_post_id, current_content_hash, text, media, outbound_links FROM posts \
         WHERE source_id = $1 AND (external_post_id = $2 OR canonical_url = $3) \
         ORDER BY (external_post_id = $2) DESC, (canonical_url = $3) DESC, id \
         LIMIT 1 FOR UPDATE",
    )
    .bind(source_id)
    .bind(&post.external_post_id)
    .bind(&post.canonical_url)
    .fetch_optional(&mut **transaction)
    .await?;
    if existing.is_none() {
        let mut timestamp_matches = sqlx::query(
            "SELECT id, external_post_id, current_content_hash, text, media, outbound_links \
             FROM posts WHERE source_id = $1 AND published_at = $2 \
             ORDER BY last_seen_at DESC, id FOR UPDATE",
        )
        .bind(source_id)
        .bind(published_at)
        .fetch_all(&mut **transaction)
        .await?;
        let mut matching_index = None;
        for (index, candidate) in timestamp_matches.iter().enumerate() {
            if same_persisted_post_content(candidate, post)? {
                matching_index = Some(index);
                break;
            }
        }
        if let Some(index) = matching_index {
            existing = Some(timestamp_matches.swap_remove(index));
        }
    }
    let media = serde_json::to_value(&post.media)?;
    let outbound_links = serde_json::to_value(&post.outbound_links)?;
    let (post_id, event_type, write_revision) = match existing {
        None => {
            let id = sqlx::query_scalar::<_, i64>(
                "INSERT INTO posts \
                 (source_id, external_post_id, current_content_hash, canonical_url, published_at, \
                  text, media, outbound_links, crawl_strategy, first_seen_at, last_seen_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10) RETURNING id",
            )
            .bind(source_id)
            .bind(&post.external_post_id)
            .bind(&post.content_hash)
            .bind(&post.canonical_url)
            .bind(published_at)
            .bind(&post.text)
            .bind(&media)
            .bind(&outbound_links)
            .bind(&post.crawl_strategy)
            .bind(fetched_at)
            .fetch_one(&mut **transaction)
            .await?;
            outcome.inserted += 1;
            (id, Some("facebook_post.discovered"), true)
        }
        Some(row) if row.get::<String, _>("external_post_id") != post.external_post_id => {
            let id = row.get("id");
            let content_changed = !same_persisted_post_content(&row, post)?;
            sqlx::query(
                "UPDATE posts SET external_post_id = $2, current_content_hash = $3, \
                 canonical_url = $4, published_at = $5, text = $6, media = $7, \
                 outbound_links = $8, crawl_strategy = $9, last_seen_at = $10, \
                 updated_at = CURRENT_TIMESTAMP WHERE id = $1",
            )
            .bind(id)
            .bind(&post.external_post_id)
            .bind(&post.content_hash)
            .bind(&post.canonical_url)
            .bind(published_at)
            .bind(&post.text)
            .bind(&media)
            .bind(&outbound_links)
            .bind(&post.crawl_strategy)
            .bind(fetched_at)
            .execute(&mut **transaction)
            .await?;
            if content_changed {
                outcome.updated += 1;
                (id, Some("facebook_post.updated"), true)
            } else {
                outcome.unchanged += 1;
                (id, None, true)
            }
        }
        Some(row) if row.get::<String, _>("current_content_hash") == post.content_hash => {
            let id = row.get("id");
            sqlx::query(
                "UPDATE posts SET external_post_id = $2, canonical_url = $3, published_at = $4, \
                 crawl_strategy = $5, last_seen_at = $6, updated_at = CURRENT_TIMESTAMP WHERE id = $1",
            )
            .bind(id)
            .bind(&post.external_post_id)
            .bind(&post.canonical_url)
            .bind(published_at)
            .bind(&post.crawl_strategy)
            .bind(fetched_at)
            .execute(&mut **transaction)
            .await?;
            outcome.unchanged += 1;
            (id, None, false)
        }
        Some(row) => {
            let id = row.get("id");
            let content_changed = !same_persisted_post_content(&row, post)?;
            sqlx::query(
                "UPDATE posts SET external_post_id = $2, current_content_hash = $3, \
                 canonical_url = $4, published_at = $5, text = $6, media = $7, \
                 outbound_links = $8, crawl_strategy = $9, last_seen_at = $10, \
                 updated_at = CURRENT_TIMESTAMP WHERE id = $1",
            )
            .bind(id)
            .bind(&post.external_post_id)
            .bind(&post.content_hash)
            .bind(&post.canonical_url)
            .bind(published_at)
            .bind(&post.text)
            .bind(&media)
            .bind(&outbound_links)
            .bind(&post.crawl_strategy)
            .bind(fetched_at)
            .execute(&mut **transaction)
            .await?;
            if content_changed {
                outcome.updated += 1;
                (id, Some("facebook_post.updated"), true)
            } else {
                outcome.unchanged += 1;
                (id, None, true)
            }
        }
    };
    if write_revision {
        sqlx::query(
            "INSERT INTO post_revisions \
             (post_id, content_hash, canonical_url, published_at, text, media, outbound_links, \
              crawl_strategy, fetched_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (post_id, content_hash) DO NOTHING",
        )
        .bind(post_id)
        .bind(&post.content_hash)
        .bind(&post.canonical_url)
        .bind(published_at)
        .bind(&post.text)
        .bind(&media)
        .bind(&outbound_links)
        .bind(&post.crawl_strategy)
        .bind(fetched_at)
        .execute(&mut **transaction)
        .await?;
    }
    if let Some(event_type) = event_type
        && emit_event
        && (allow_historical_events || published_at > historical_cutoff)
    {
        let event_key = format!(
            "facebook-post:{}:{}:{}",
            source_id, post.external_post_id, post.content_hash
        );
        let payload = json!({"post": post, "database_post_id": post_id});
        let inserted = sqlx::query(
            "INSERT INTO outbox_events \
             (event_key, event_type, aggregate_type, aggregate_id, payload) \
             VALUES ($1, $2, 'facebook_post', $3, $4) \
             ON CONFLICT (event_key) DO NOTHING",
        )
        .bind(event_key)
        .bind(event_type)
        .bind(&post.external_post_id)
        .bind(payload)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        outcome.outbox_events += usize::try_from(inserted)?;
    }
    Ok(())
}

fn same_persisted_post_content(row: &PgRow, post: &FacebookPost) -> Result<bool> {
    let media = serde_json::from_value::<Vec<MediaItem>>(row.get("media"))?;
    let outbound_links = serde_json::from_value::<Vec<String>>(row.get("outbound_links"))?;
    Ok(same_post_content(
        row.get("text"),
        &media,
        &outbound_links,
        post,
    ))
}

fn same_post_content(
    text: &str,
    media: &[MediaItem],
    outbound_links: &[String],
    post: &FacebookPost,
) -> bool {
    text == post.text
        && outbound_links == post.outbound_links
        && media_identities(media) == media_identities(&post.media)
}

fn media_identities(media: &[MediaItem]) -> Vec<(String, String, Option<String>)> {
    let mut identities = media
        .iter()
        .map(|item| {
            (
                item.kind.clone(),
                media_url_hash_identity(&item.url),
                item.alt_text.clone(),
            )
        })
        .collect::<Vec<_>>();
    identities.sort();
    identities
}

async fn finish_source(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: i64,
    owner: &str,
    healthy: bool,
    next_delay_seconds: u64,
    unchanged_crawl_count: u32,
) -> Result<()> {
    let failure_increment = if healthy { 0_i32 } else { 1_i32 };
    let failure_reset = healthy;
    let affected = sqlx::query(
        "UPDATE sources SET \
            failure_count = CASE WHEN $3 THEN 0 ELSE failure_count + $4 END, \
            next_crawl_at = CURRENT_TIMESTAMP + make_interval(secs => $5::double precision), \
            unchanged_crawl_count = $6, \
            lease_owner = NULL, lease_expires_at = NULL, updated_at = CURRENT_TIMESTAMP \
         WHERE id = $1 AND lease_owner = $2",
    )
    .bind(source_id)
    .bind(owner)
    .bind(failure_reset)
    .bind(failure_increment)
    .bind(i64::try_from(next_delay_seconds)?)
    .bind(i32::try_from(unchanged_crawl_count)?)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        bail!("failed to release claimed source");
    }
    Ok(())
}

fn adaptive_next_schedule(
    source: &ClaimedSource,
    outcome: &PersistOutcome,
    schedule: AdaptiveSchedule,
) -> Result<(u64, u32)> {
    if schedule.active_interval_seconds == 0
        || schedule.active_interval_seconds > source.schedule_interval_seconds
        || source.schedule_interval_seconds > schedule.idle_interval_seconds
        || schedule.active_unchanged_crawls == 0
        || schedule.idle_after_unchanged_crawls <= schedule.active_unchanged_crawls
    {
        bail!("adaptive crawl schedule is invalid");
    }
    if source.initial_crawl {
        return Ok((
            source.schedule_interval_seconds,
            source.unchanged_crawl_count,
        ));
    }
    let unchanged_crawl_count = if outcome.inserted > 0 {
        0
    } else {
        source.unchanged_crawl_count.saturating_add(1)
    };
    let next_delay_seconds = if unchanged_crawl_count < schedule.active_unchanged_crawls {
        schedule.active_interval_seconds
    } else if unchanged_crawl_count < schedule.idle_after_unchanged_crawls {
        source.schedule_interval_seconds
    } else {
        schedule.idle_interval_seconds
    };
    Ok((next_delay_seconds, unchanged_crawl_count))
}

fn validate_notification_event(
    event: &ClaimedNotificationEvent,
    database_classification_id: i64,
    database_post_id: i64,
    will_send: bool,
) -> Result<()> {
    let payload_classification_id = event
        .payload
        .get("database_classification_id")
        .and_then(serde_json::Value::as_i64);
    let payload_post_id = event
        .payload
        .get("database_post_id")
        .and_then(serde_json::Value::as_i64);
    let decision = event
        .payload
        .get("classification")
        .and_then(|classification| classification.get("decision"))
        .and_then(serde_json::Value::as_str);
    if database_classification_id <= 0
        || database_post_id <= 0
        || payload_classification_id != Some(database_classification_id)
        || payload_post_id != Some(database_post_id)
        || will_send != (decision == Some("matched_explicit"))
    {
        bail!("notification plan does not match claimed classification event");
    }
    Ok(())
}

fn validate_portal_notice(notice: &PortalNoticeRecord<'_>, message_text: &str) -> Result<()> {
    validate_portal_notice_record(notice)?;
    if !(1..=1_024).contains(&message_text.chars().count()) {
        bail!("Portal notice message is invalid");
    }
    Ok(())
}

fn validate_portal_notice_record(notice: &PortalNoticeRecord<'_>) -> Result<()> {
    let title_length = notice.title.trim().chars().count();
    if notice.portal_id <= 0 || !(1..=1_000).contains(&title_length) {
        bail!("Portal notice values are invalid");
    }
    for raw_url in [notice.article_url, notice.attachment_url]
        .into_iter()
        .flatten()
    {
        let parsed = url::Url::parse(raw_url).context("Portal notice URL is invalid")?;
        if raw_url.chars().count() > 2_048
            || parsed.scheme() != "https"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            bail!("Portal notice URL is not an approved HTTPS URL");
        }
    }
    if notice.attachment_url.is_none()
        && (notice.attachment_file_name.is_some() || notice.attachment_content_type.is_some())
    {
        bail!("Portal attachment metadata requires an attachment URL");
    }
    if notice
        .attachment_file_name
        .is_some_and(|value| value.is_empty() || value.chars().count() > 255)
        || notice
            .attachment_content_type
            .is_some_and(|value| value.is_empty() || value.chars().count() > 255)
    {
        bail!("Portal attachment metadata is invalid");
    }
    Ok(())
}

async fn ensure_delivery_lease(
    transaction: &mut Transaction<'_, Postgres>,
    delivery_id: i64,
    owner: &str,
) -> Result<()> {
    let valid = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS( \
             SELECT 1 FROM deliveries WHERE id = $1 AND status = 'sending' \
               AND lease_owner = $2 AND lease_expires_at > CURRENT_TIMESTAMP \
         )",
    )
    .bind(delivery_id)
    .bind(owner)
    .fetch_one(&mut **transaction)
    .await?;
    if !valid {
        bail!("delivery lease is missing, expired, or owned by another worker");
    }
    Ok(())
}

async fn insert_delivery_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &ClaimedDelivery,
    outcome: &str,
    error_code: Option<i32>,
    detail: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_attempts \
         (delivery_id, attempt, outcome, telegram_error_code, detail) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(delivery.id)
    .bind(i32::try_from(delivery.attempt)?)
    .bind(outcome)
    .bind(error_code)
    .bind(detail)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn advance_subscriber_rate_limit(
    transaction: &mut Transaction<'_, Postgres>,
    subscriber_id: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE subscribers SET next_send_at = CURRENT_TIMESTAMP + INTERVAL '1 second', \
         updated_at = CURRENT_TIMESTAMP WHERE id = $1",
    )
    .bind(subscriber_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn parse_timestamp(value: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid {field}"))
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn operational_alert_kind(
    observed_status: &str,
    notified_status: Option<&str>,
    grace_elapsed: bool,
) -> Option<OperationalAlertKind> {
    match observed_status {
        "failed" if notified_status != Some("failed") => Some(OperationalAlertKind::Failed),
        "degraded" if grace_elapsed && !matches!(notified_status, Some("degraded" | "failed")) => {
            Some(OperationalAlertKind::Degraded)
        }
        "healthy" if grace_elapsed && matches!(notified_status, Some("degraded" | "failed")) => {
            Some(OperationalAlertKind::Recovered)
        }
        _ => None,
    }
}

fn source_alerts_are_systemic(enabled_sources: i64, sources_alerting: i64) -> bool {
    enabled_sources > 0 && sources_alerting >= 3 && sources_alerting * 4 >= enabled_sources
}

#[cfg(test)]
mod tests {
    use super::{
        AdaptiveSchedule, ClaimedSource, DigestCandidate, OperationalAlertKind, PersistOutcome,
        adaptive_next_schedule, build_digest_batch, operational_alert_kind, same_post_content,
        source_alerts_are_systemic,
    };
    use uth_domain::{
        FacebookPost, MediaItem, POST_SCHEMA_VERSION, TELEGRAM_MESSAGE_LIMIT,
        TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT,
    };

    fn source(unchanged_crawl_count: u32, initial_crawl: bool) -> ClaimedSource {
        ClaimedSource {
            id: 1,
            key: "facebook:test".to_owned(),
            name: "Test".to_owned(),
            url: "https://www.facebook.com/test/".to_owned(),
            failure_count: 0,
            schedule_interval_seconds: 300,
            unchanged_crawl_count,
            initial_crawl,
            reconciliation_required: false,
        }
    }

    fn schedule() -> AdaptiveSchedule {
        AdaptiveSchedule {
            active_interval_seconds: 120,
            idle_interval_seconds: 480,
            active_unchanged_crawls: 3,
            idle_after_unchanged_crawls: 6,
        }
    }

    #[test]
    fn new_post_activates_fast_schedule() {
        let outcome = PersistOutcome {
            inserted: 1,
            ..PersistOutcome::default()
        };

        assert_eq!(
            adaptive_next_schedule(&source(6, false), &outcome, schedule()).unwrap(),
            (120, 0)
        );
    }

    #[test]
    fn unchanged_crawls_move_through_bounded_schedule_tiers() {
        let outcome = PersistOutcome {
            unchanged: 1,
            ..PersistOutcome::default()
        };

        assert_eq!(
            adaptive_next_schedule(&source(0, false), &outcome, schedule()).unwrap(),
            (120, 1)
        );
        assert_eq!(
            adaptive_next_schedule(&source(2, false), &outcome, schedule()).unwrap(),
            (300, 3)
        );
        assert_eq!(
            adaptive_next_schedule(&source(5, false), &outcome, schedule()).unwrap(),
            (480, 6)
        );
    }

    #[test]
    fn baseline_uses_normal_schedule_without_resetting_history() {
        let outcome = PersistOutcome {
            inserted: 1,
            ..PersistOutcome::default()
        };

        assert_eq!(
            adaptive_next_schedule(&source(6, true), &outcome, schedule()).unwrap(),
            (300, 6)
        );
    }

    #[test]
    fn operational_recovery_requires_the_full_grace_period() {
        assert_eq!(
            operational_alert_kind("healthy", Some("failed"), false),
            None
        );
        assert_eq!(
            operational_alert_kind("healthy", Some("failed"), true),
            Some(OperationalAlertKind::Recovered)
        );
        assert_eq!(
            operational_alert_kind("healthy", Some("degraded"), true),
            Some(OperationalAlertKind::Recovered)
        );
    }

    #[test]
    fn source_alerts_require_a_systemic_quorum_for_failed_health() {
        assert!(!source_alerts_are_systemic(43, 1));
        assert!(!source_alerts_are_systemic(43, 10));
        assert!(source_alerts_are_systemic(43, 11));
        assert!(!source_alerts_are_systemic(4, 2));
        assert!(source_alerts_are_systemic(4, 3));
    }

    #[test]
    fn digest_collapses_identical_campaigns_without_losing_audit_ids() {
        let mut candidates = (1..=6)
            .map(|campaign_id| DigestCandidate {
                campaign_id,
                post_id: 324,
                summary: "ộ".repeat(650),
                link: Some("https://www.facebook.com/posts/324".to_owned()),
            })
            .collect::<Vec<_>>();
        candidates.push(DigestCandidate {
            campaign_id: 7,
            post_id: 386,
            summary: "đ".repeat(650),
            link: Some("https://www.facebook.com/posts/386".to_owned()),
        });

        let plan = build_digest_batch(candidates, false).unwrap().unwrap();

        assert_eq!(plan.campaign_ids, (1..=7).collect::<Vec<_>>());
        assert_eq!(plan.duplicate_items_collapsed, 5);
        assert!(plan.message_text.contains("\n\n1. "));
        assert!(plan.message_text.contains("\n\n2. "));
        assert!(!plan.message_text.contains("\n\n3. "));
        assert!(!plan.message_text.contains("Các tin còn lại"));
        assert!(plan.message_text.chars().count() <= TELEGRAM_MESSAGE_LIMIT);
    }

    #[test]
    fn digest_collapses_different_revisions_of_the_same_post() {
        let plan = build_digest_batch(
            vec![
                DigestCandidate {
                    campaign_id: 1,
                    post_id: 324,
                    summary: "old revision".to_owned(),
                    link: Some("https://www.facebook.com/old".to_owned()),
                },
                DigestCandidate {
                    campaign_id: 2,
                    post_id: 324,
                    summary: "latest revision".to_owned(),
                    link: Some("https://www.facebook.com/latest".to_owned()),
                },
            ],
            false,
        )
        .unwrap()
        .unwrap();

        assert_eq!(plan.campaign_ids, [1, 2]);
        assert_eq!(plan.duplicate_items_collapsed, 1);
        assert!(plan.message_text.contains("latest revision"));
        assert!(!plan.message_text.contains("old revision"));
    }

    #[test]
    fn digest_splits_long_unique_entries_at_the_exact_character_limit() {
        let candidates = (1..=10)
            .map(|campaign_id| DigestCandidate {
                campaign_id,
                post_id: campaign_id,
                summary: "ữ".repeat(650),
                link: Some(format!("https://example.com/{}", "x".repeat(980))),
            })
            .collect::<Vec<_>>();

        let plan = build_digest_batch(candidates, false).unwrap().unwrap();

        assert!(!plan.campaign_ids.is_empty());
        assert!(plan.campaign_ids.len() < 10);
        assert!(plan.message_text.contains("Các tin còn lại"));
        assert!(plan.message_text.chars().count() <= TELEGRAM_MESSAGE_LIMIT);
        assert!(plan.message_text.len() > TELEGRAM_MESSAGE_LIMIT);
        assert!(plan.message_text.len() <= TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT);
    }

    #[test]
    fn digest_marks_unloaded_rows_without_exceeding_the_limit() {
        let plan = build_digest_batch(
            vec![DigestCandidate {
                campaign_id: 1,
                post_id: 1,
                summary: "Tin mới".to_owned(),
                link: None,
            }],
            true,
        )
        .unwrap()
        .unwrap();

        assert_eq!(plan.campaign_ids, [1]);
        assert!(plan.message_text.contains("Các tin còn lại"));
        assert!(plan.message_text.chars().count() <= TELEGRAM_MESSAGE_LIMIT);
    }

    #[test]
    fn persisted_content_ignores_facebook_cdn_signature_transition() {
        let stored_media = [MediaItem {
            kind: "image".to_owned(),
            url: "https://scontent.fsgn16-1.fna.fbcdn.net/v/t39.30808-6/image.jpg?oh=old&oe=AAAA"
                .to_owned(),
            alt_text: Some("Ảnh".to_owned()),
        }];
        let post = post_with_media(
            "https://scontent.fhan14-3.fna.fbcdn.net/v/t39.30808-6/image.jpg?oh=new&oe=BBBB",
        );

        assert!(same_post_content("Nội dung", &stored_media, &[], &post));
    }

    #[test]
    fn persisted_content_detects_real_media_change() {
        let stored_media = [MediaItem {
            kind: "image".to_owned(),
            url: "https://scontent.fsgn16-1.fna.fbcdn.net/v/t39.30808-6/old.jpg?oh=value"
                .to_owned(),
            alt_text: Some("Ảnh".to_owned()),
        }];
        let post = post_with_media(
            "https://scontent.fsgn16-1.fna.fbcdn.net/v/t39.30808-6/new.jpg?oh=value",
        );

        assert!(!same_post_content("Nội dung", &stored_media, &[], &post));
    }

    fn post_with_media(url: &str) -> FacebookPost {
        FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: "facebook:test".to_owned(),
            platform: "facebook".to_owned(),
            external_post_id: "1".to_owned(),
            canonical_url: "https://www.facebook.com/test/posts/1".to_owned(),
            published_at: "2026-07-25T00:00:00+00:00".to_owned(),
            text: "Nội dung".to_owned(),
            media: vec![MediaItem {
                kind: "image".to_owned(),
                url: url.to_owned(),
                alt_text: Some("Ảnh".to_owned()),
            }],
            outbound_links: Vec::new(),
            content_hash: "sha256:test".to_owned(),
            crawl_strategy: "test".to_owned(),
            fetched_at: "2026-07-25T00:00:00+00:00".to_owned(),
        }
    }
}
