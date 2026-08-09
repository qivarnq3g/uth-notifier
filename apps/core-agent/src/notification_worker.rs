use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, FixedOffset, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use uth_delivery::{
    TELEGRAM_DOCUMENT_TIMEOUT_SECONDS, TelegramChat, TelegramClient, TelegramConfigurationOutcome,
    TelegramIncomingMessage, TelegramSendOutcome, TelegramUpdatesOutcome, TelegramUserLink,
    delivery_post_url, humanize_notification_sample, render_notification,
    render_structured_notification,
};
use uth_domain::{ClassificationDecision, ClassificationResult};
use uth_storage::{
    ClaimedDelivery, ClaimedDigestDelivery, ClaimedNotificationEvent, CrawlHistoryDetail,
    CrawlHistoryRecord, CrawlStore, DeliveryFailureClass, DeliveryRetentionOutcome,
    DonationIntentPaymentLink, DonationPayment, FailureDisposition, LatestPostRecord,
    ManualReviewAction, ManualReviewNotification, ManualReviewRecord, NotificationContent,
    NotificationPlanOutcome, OperationalAlertKind, OperationalHealth, PortalNoticeHistoryRecord,
    PortalNoticePlanOutcome, PortalNoticeRecord, PortalPollState, USER_STOP_REASON,
    UserFeedbackHistoryRecord,
};

use crate::payos::PayOsClient;
use crate::portal::{
    PortalAttachment, PortalClient, PortalFailureKind, PortalNotice, TELEGRAM_DOCUMENT_LIMIT,
    classify_portal_error, render_portal_notification,
};

const DONATION_AMOUNT_INPUT_TTL_SECONDS: i64 = 600;
const USER_FEEDBACK_INPUT_TTL_SECONDS: i64 = 600;
const TELEGRAM_WORKER_LEASE_KEY: &str = "telegram_delivery";
const PORTAL_HISTORY_BACKFILL_SIZE: usize = 20;

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn new() -> Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .context("failed to listen for SIGINT")?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("failed to listen for SIGTERM")?,
        })
    }

    async fn wait(&mut self) -> Result<()> {
        tokio::select! {
            received = self.interrupt.recv() => {
                received.context("SIGINT listener closed unexpectedly")?;
            }
            received = self.terminate.recv() => {
                received.context("SIGTERM listener closed unexpectedly")?;
            }
        }
        Ok(())
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    fn new() -> Result<Self> {
        Ok(Self)
    }

    async fn wait(&mut self) -> Result<()> {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for shutdown signal")
    }
}

const SUPPORT_GROUP_INVITATION: &str =
    "Tham gia nhóm hỗ trợ và nhận cập nhật mới nhất tại: https://t.me/uth_notifier_group";

#[derive(Debug, clap::Args)]
pub struct NotifyArgs {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,

    #[arg(long, env = "TELEGRAM_BOT_TOKEN", hide_env_values = true)]
    bot_token: String,

    #[arg(long, env = "TELEGRAM_ADMIN_CHAT_ID")]
    admin_chat_id: Option<i64>,

    #[arg(long, env = "TELEGRAM_ADMIN_ONLY", default_value_t = false)]
    admin_only: bool,

    #[arg(long, env = "DONATE_VIETQR_URL", hide_env_values = true)]
    donate_vietqr_url: Option<String>,

    #[arg(long, env = "DONATE_MESSAGE", hide_env_values = true)]
    donate_message: Option<String>,

    #[arg(long, env = "DONATE_BANK_ACCOUNT", hide_env_values = true)]
    donate_bank_account: Option<String>,

    #[arg(long, env = "PAYOS_CLIENT_ID", hide_env_values = true)]
    payos_client_id: Option<String>,

    #[arg(long, env = "PAYOS_API_KEY", hide_env_values = true)]
    payos_api_key: Option<String>,

    #[arg(long, env = "PAYOS_CHECKSUM_KEY", hide_env_values = true)]
    payos_checksum_key: Option<String>,

    #[arg(
        long,
        env = "PAYOS_API_BASE",
        default_value = "https://api-merchant.payos.vn"
    )]
    payos_api_base: url::Url,

    #[arg(long, env = "PAYOS_RETURN_URL")]
    payos_return_url: Option<url::Url>,

    #[arg(long, env = "PAYOS_CANCEL_URL")]
    payos_cancel_url: Option<url::Url>,

    #[arg(long, default_value_t = 1800)]
    donation_link_ttl: i64,

    #[arg(
        long,
        env = "TELEGRAM_UPDATES_SOURCE",
        value_enum,
        default_value_t = TelegramUpdatesSource::Polling
    )]
    telegram_updates_source: TelegramUpdatesSource,

    #[arg(long, default_value_t = 5)]
    concurrency: usize,

    #[arg(long, default_value_t = 100)]
    plan_batch_size: usize,

    #[arg(long, default_value_t = 120)]
    lease_duration: u64,

    #[arg(long, default_value_t = 1)]
    poll_interval: u64,

    #[arg(long, default_value_t = 15)]
    request_timeout: u64,

    #[arg(long, default_value_t = 25)]
    messages_per_second: u32,

    #[arg(long, default_value_t = 5)]
    max_attempts: u32,

    #[arg(long, default_value_t = 30)]
    base_retry_delay: u64,

    #[arg(long, default_value_t = 900)]
    max_retry_delay: u64,

    #[arg(long, default_value_t = 90)]
    sent_delivery_retention_days: i32,

    #[arg(long, default_value_t = 30)]
    failed_delivery_retention_days: i32,

    #[arg(long, default_value_t = 90)]
    inactive_subscriber_retention_days: i32,

    #[arg(long, default_value_t = 86_400)]
    retention_interval: u64,

    #[arg(long, default_value_t = 3)]
    health_alert_after_failures: u32,

    #[arg(long, default_value_t = 900)]
    health_backlog_stale_seconds: u64,

    #[arg(long, default_value_t = 900)]
    health_alert_grace_seconds: u64,

    #[arg(long, default_value_t = 30)]
    health_alert_interval: u64,

    #[arg(long, env = "PORTAL_NOTIFICATIONS_ENABLED", default_value_t = true)]
    portal_notifications_enabled: bool,

    #[arg(
        long,
        env = "PORTAL_API_BASE",
        default_value = "https://portal.ut.edu.vn/api/v1/"
    )]
    portal_api_base: url::Url,

    #[arg(long, default_value_t = 300)]
    portal_poll_interval: u64,

    #[arg(long, default_value_t = 60)]
    portal_burst_interval: u64,

    #[arg(long, default_value_t = 900)]
    portal_burst_duration: u64,

    #[arg(long, default_value_t = 21_600)]
    portal_forbidden_cooldown: u64,

    #[arg(long, default_value_t = 1_800)]
    portal_rate_limit_cooldown: u64,

    #[arg(long, default_value_t = 900)]
    portal_failure_cooldown: u64,

    #[arg(long, default_value_t = 10)]
    portal_jitter_percent: u32,

    #[arg(long, default_value_t = 20)]
    portal_page_size: usize,

    #[arg(long, default_value_t = 20)]
    portal_max_pages: usize,

    #[arg(long, default_value_t = 15)]
    portal_request_timeout: u64,

    #[arg(long, default_value_t = 120)]
    portal_file_timeout: u64,

    #[arg(long, default_value_t = TELEGRAM_DOCUMENT_LIMIT)]
    portal_max_file_bytes: usize,

    #[arg(long)]
    once: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TelegramUpdatesSource {
    Polling,
    Edge,
}

#[derive(Clone, Copy)]
struct InteractionContext<'a> {
    admin_chat_id: Option<i64>,
    admin_only: bool,
    health_alert_after_failures: u32,
    health_backlog_stale_seconds: u64,
    donation: &'a DonationConfig,
    payos: Option<&'a PayOsClient>,
    donation_link_ttl: i64,
}

#[derive(Debug, Clone)]
struct DonationConfig {
    vietqr_url: Option<String>,
    message: Option<String>,
    bank_account: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct SubscriberArgs {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,

    #[command(subcommand)]
    command: SubscriberCommand,
}

#[derive(Debug, clap::Subcommand)]
enum SubscriberCommand {
    Add {
        #[arg(long)]
        chat_id: i64,
        #[arg(long)]
        name: Option<String>,
    },
    Remove {
        #[arg(long)]
        chat_id: i64,
    },
    List,
}

#[derive(Debug, clap::Args)]
pub struct SuggestionArgs {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,

    #[command(subcommand)]
    command: SuggestionCommand,
}

#[derive(Debug, clap::Subcommand)]
enum SuggestionCommand {
    List {
        #[arg(long, default_value = "pending")]
        status: String,
    },
    Approve {
        #[arg(long)]
        id: i64,
        #[arg(long)]
        name: String,
    },
    Reject {
        #[arg(long)]
        id: i64,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Debug, Serialize)]
struct NotificationCycleReport {
    schema_version: String,
    generated_at: String,
    planned_events: usize,
    campaigns_created: usize,
    deliveries_created: u64,
    planning_skipped: usize,
    planning_failed: usize,
    claimed_deliveries: usize,
    sent: usize,
    retry_scheduled: usize,
    permanently_failed: usize,
    chat_migrated: usize,
    authentication_failed: bool,
    retention_applied: bool,
    retention: DeliveryRetentionOutcome,
    interaction: InteractionCycleReport,
    operational_alert: OperationalAlertReport,
    digest: DigestCycleReport,
    portal: PortalCycleReport,
    plans: Vec<PlanReport>,
    deliveries: Vec<DeliveryReport>,
}

#[derive(Debug, Default, Serialize)]
struct DigestCycleReport {
    batches_created: u64,
    items_batched: u64,
    duplicate_items_collapsed: u64,
    claimed: usize,
    sent: usize,
    retry_scheduled: usize,
    failed: usize,
    preparation_error: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct PortalCycleReport {
    checked: bool,
    mode: String,
    next_poll_at: Option<String>,
    cooldown_reason: Option<String>,
    baseline_initialized: bool,
    history_archived: usize,
    notices_found: usize,
    notices_created: usize,
    campaigns_created: usize,
    deliveries_created: u64,
    attachment_discovery_errors: usize,
    attachment_discovery_error: Option<String>,
    failure_kind: Option<String>,
    http_status: Option<u16>,
    retry_after_seconds: Option<u64>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct PortalPollConfig {
    steady_interval: u64,
    burst_interval: u64,
    burst_duration: u64,
    forbidden_cooldown: u64,
    rate_limit_cooldown: u64,
    failure_cooldown: u64,
    jitter_percent: u32,
}

impl From<&NotifyArgs> for PortalPollConfig {
    fn from(args: &NotifyArgs) -> Self {
        Self {
            steady_interval: args.portal_poll_interval,
            burst_interval: args.portal_burst_interval,
            burst_duration: args.portal_burst_duration,
            forbidden_cooldown: args.portal_forbidden_cooldown,
            rate_limit_cooldown: args.portal_rate_limit_cooldown,
            failure_cooldown: args.portal_failure_cooldown,
            jitter_percent: args.portal_jitter_percent,
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct InteractionCycleReport {
    received: usize,
    processed: usize,
    replied: usize,
    admin_notified: usize,
    suggestions_created: usize,
    ignored: usize,
    authentication_failed: bool,
    error: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct OperationalAlertReport {
    status: Option<String>,
    kind: Option<OperationalAlertKind>,
    outcome: Option<String>,
    authentication_failed: bool,
    error: Option<String>,
}

struct CycleInputs {
    apply_retention: bool,
    interaction: InteractionCycleReport,
    operational_alert: OperationalAlertReport,
    portal: PortalCycleReport,
}

#[derive(Debug, Serialize)]
struct PlanReport {
    event_key: String,
    outcome: Option<NotificationPlanOutcome>,
    failure_disposition: Option<FailureDisposition>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct DeliveryReport {
    delivery_id: i64,
    chat_id: i64,
    attempt: u32,
    outcome: String,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NotificationEventPayload {
    classification: ClassificationResult,
    database_classification_id: i64,
    database_post_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PayOsPaymentPayload {
    order_code: i64,
    amount: i64,
    reference: String,
    transaction_date_time: String,
    currency: String,
    payment_link_id: String,
    description: String,
    code: String,
}

pub async fn run(args: NotifyArgs) -> Result<()> {
    validate_args(&args)?;
    let donation = DonationConfig {
        vietqr_url: normalized_optional_value(args.donate_vietqr_url.as_deref()),
        message: normalized_optional_value(args.donate_message.as_deref()),
        bank_account: normalized_optional_value(args.donate_bank_account.as_deref()),
    };
    let max_connections = u32::try_from(args.concurrency.saturating_add(3))?;
    let store = CrawlStore::connect(&args.database_url, max_connections).await?;
    store.migrate().await?;
    let payos = match (
        args.payos_client_id.clone(),
        args.payos_api_key.clone(),
        args.payos_checksum_key.clone(),
        args.payos_return_url.clone(),
        args.payos_cancel_url.clone(),
    ) {
        (
            Some(client_id),
            Some(api_key),
            Some(checksum_key),
            Some(return_url),
            Some(cancel_url),
        ) => Some(PayOsClient::new(
            client_id,
            api_key,
            checksum_key,
            args.payos_api_base.clone(),
            return_url,
            cancel_url,
            Duration::from_secs(args.request_timeout),
        )?),
        (None, None, None, None, None) => None,
        _ => bail!("all payOS credentials and return URLs must be configured together"),
    };
    if args.admin_only {
        store
            .deactivate_subscribers_except(
                args.admin_chat_id.context(
                    "TELEGRAM_ADMIN_CHAT_ID is required when TELEGRAM_ADMIN_ONLY is enabled",
                )?,
                "Bot đang chạy thử nghiệm giới hạn cho quản trị viên",
            )
            .await?;
    }
    let telegram = TelegramClient::new(&args.bot_token, Duration::from_secs(args.request_timeout))
        .map_err(anyhow::Error::msg)?;
    let portal = PortalClient::new(
        args.portal_api_base.clone(),
        Duration::from_secs(args.portal_request_timeout),
        Duration::from_secs(args.portal_file_timeout),
        args.portal_max_file_bytes,
    )?;
    configure_bot_commands(&telegram, args.admin_chat_id).await?;
    let owner = format!("uth-notify-{}", std::process::id());
    let worker_lease_seconds = i64::try_from(
        args.lease_duration
            .max(args.request_timeout.saturating_mul(args.concurrency as u64))
            .max(
                args.portal_file_timeout
                    .saturating_add(TELEGRAM_DOCUMENT_TIMEOUT_SECONDS),
            )
            .max(60),
    )?;
    let mut next_retention_at = Instant::now();
    let mut next_health_alert_at = Instant::now();
    let mut portal_poll_state = store.portal_poll_state().await?.unwrap_or(PortalPollState {
        mode: "steady".to_owned(),
        next_poll_at: Utc::now(),
        burst_until: None,
        cooldown_reason: None,
        last_polled_at: None,
        last_poll_outcome: None,
        last_http_status: None,
    });
    let mut shutdown_signals = ShutdownSignals::new()?;
    loop {
        let acquired_lease = tokio::select! {
            biased;
            result = shutdown_signals.wait() => {
                result?;
                return Ok(());
            }
            result = store.acquire_worker_lease(
                TELEGRAM_WORKER_LEASE_KEY,
                &owner,
                worker_lease_seconds,
            ) => result?,
        };
        if !acquired_lease {
            if args.once {
                bail!("another Telegram notification worker is active");
            }
            tokio::select! {
                biased;
                result = shutdown_signals.wait() => {
                    result?;
                    return Ok(());
                }
                () = tokio::time::sleep(Duration::from_secs(args.poll_interval)) => {}
            }
            continue;
        }
        let now = Instant::now();
        let apply_retention = now >= next_retention_at;
        let iteration = async {
            run_payos_payment_cycle(&store, &telegram).await?;
            let portal_now = Utc::now();
            let portal_report = if args.portal_notifications_enabled
                && portal_now >= portal_poll_state.next_poll_at
            {
                let mut report = run_portal_cycle(&store, &portal, &args).await;
                let next_state = next_portal_poll_state(
                    &portal_poll_state,
                    &report,
                    portal_now,
                    PortalPollConfig::from(&args),
                )?;
                if store.portal_notice_cursor().await?.is_some() {
                    store.update_portal_poll_state(&next_state).await?;
                }
                portal_poll_state = next_state;
                apply_portal_poll_state(&mut report, &portal_poll_state);
                report
            } else {
                portal_cycle_report(&portal_poll_state)
            };
            let interaction = run_interaction_cycle(
                &store,
                &telegram,
                &portal,
                args.telegram_updates_source,
                InteractionContext {
                    admin_chat_id: args.admin_chat_id,
                    admin_only: args.admin_only,
                    health_alert_after_failures: args.health_alert_after_failures,
                    health_backlog_stale_seconds: args.health_backlog_stale_seconds,
                    donation: &donation,
                    payos: payos.as_ref(),
                    donation_link_ttl: args.donation_link_ttl,
                },
            )
            .await?;
            let operational_alert = if now >= next_health_alert_at {
                next_health_alert_at = now
                    .checked_add(Duration::from_secs(args.health_alert_interval))
                    .context("health alert interval exceeds monotonic clock range")?;
                run_operational_alert_cycle(
                    &store,
                    &telegram,
                    args.admin_chat_id,
                    args.health_alert_after_failures,
                    args.health_backlog_stale_seconds,
                    args.health_alert_grace_seconds,
                )
                .await?
            } else {
                OperationalAlertReport {
                    outcome: Some("not_checked".to_owned()),
                    ..OperationalAlertReport::default()
                }
            };
            let report = run_cycle(
                &store,
                &telegram,
                &portal,
                &owner,
                &args,
                CycleInputs {
                    apply_retention,
                    interaction,
                    operational_alert,
                    portal: portal_report,
                },
            )
            .await?;
            if apply_retention {
                next_retention_at = now
                    .checked_add(Duration::from_secs(args.retention_interval))
                    .context("retention interval exceeds monotonic clock range")?;
            }
            let authentication_failed = report.authentication_failed;
            println!("{}", serde_json::to_string(&report)?);
            if authentication_failed {
                bail!("Telegram authentication failed; verify TELEGRAM_BOT_TOKEN");
            }
            Result::<()>::Ok(())
        }
        .await;
        if let Err(error) = iteration {
            if let Err(release_error) = store
                .release_worker_lease(TELEGRAM_WORKER_LEASE_KEY, &owner)
                .await
            {
                return Err(error.context(format!(
                    "failed to release Telegram worker lease after cycle error: {release_error}"
                )));
            }
            return Err(error);
        }
        if args.once {
            store
                .release_worker_lease(TELEGRAM_WORKER_LEASE_KEY, &owner)
                .await?;
            return Ok(());
        }
        tokio::select! {
            biased;
            result = shutdown_signals.wait() => {
                result?;
                store
                    .release_worker_lease(TELEGRAM_WORKER_LEASE_KEY, &owner)
                    .await?;
                return Ok(());
            }
            () = tokio::time::sleep(Duration::from_secs(args.poll_interval)) => {}
        }
    }
}

async fn configure_bot_commands(
    telegram: &TelegramClient,
    admin_chat_id: Option<i64>,
) -> Result<()> {
    configure_command_scope(telegram, None).await?;
    if let Some(admin_chat_id) = admin_chat_id {
        configure_command_scope(telegram, Some(admin_chat_id)).await?;
    }
    Ok(())
}

async fn configure_command_scope(
    telegram: &TelegramClient,
    admin_chat_id: Option<i64>,
) -> Result<()> {
    for attempt in 1..=3_u64 {
        let outcome = match admin_chat_id {
            Some(admin_chat_id) => telegram.configure_admin_commands(admin_chat_id).await,
            None => telegram.configure_commands().await,
        };
        match outcome {
            TelegramConfigurationOutcome::Applied => return Ok(()),
            TelegramConfigurationOutcome::RetryAfter { seconds, .. } => {
                if attempt == 3 {
                    bail!("Telegram chưa nhận được danh sách lệnh mới");
                }
                tokio::time::sleep(Duration::from_secs(seconds.min(30))).await;
            }
            TelegramConfigurationOutcome::TransientFailure { .. } => {
                if attempt == 3 {
                    bail!("Không thể cập nhật danh sách lệnh Telegram sau 3 lần thử");
                }
                tokio::time::sleep(Duration::from_secs(attempt)).await;
            }
            TelegramConfigurationOutcome::PermanentFailure { detail } => {
                bail!("Telegram từ chối danh sách lệnh: {detail}");
            }
            TelegramConfigurationOutcome::AuthenticationFailure { .. } => {
                bail!("Telegram không chấp nhận token hiện tại");
            }
        }
    }
    bail!("Không thể cập nhật danh sách lệnh Telegram")
}

pub async fn run_subscriber(args: SubscriberArgs) -> Result<()> {
    let store = CrawlStore::connect(&args.database_url, 2).await?;
    store.migrate().await?;
    match args.command {
        SubscriberCommand::Add { chat_id, name } => {
            store.upsert_subscriber(chat_id, name.as_deref()).await?;
            println!(
                "{}",
                serde_json::json!({"status":"active","chat_id":chat_id})
            );
        }
        SubscriberCommand::Remove { chat_id } => {
            let changed = store
                .deactivate_subscriber(chat_id, "removed by administrator")
                .await?;
            println!(
                "{}",
                serde_json::json!({"status":"inactive","chat_id":chat_id,"changed":changed})
            );
        }
        SubscriberCommand::List => {
            println!(
                "{}",
                serde_json::to_string(&store.list_subscribers().await?)?
            );
        }
    }
    Ok(())
}

pub async fn run_suggestion(args: SuggestionArgs) -> Result<()> {
    let store = CrawlStore::connect(&args.database_url, 2).await?;
    store.migrate().await?;
    match args.command {
        SuggestionCommand::List { status } => {
            let status = if status == "all" {
                None
            } else {
                Some(status.as_str())
            };
            println!(
                "{}",
                serde_json::to_string(&store.list_source_suggestions(status).await?)?
            );
        }
        SuggestionCommand::Approve { id, name } => {
            let changed = store.approve_source_suggestion(id, &name).await?;
            println!(
                "{}",
                serde_json::json!({"id":id,"status":"approved","changed":changed})
            );
        }
        SuggestionCommand::Reject { id, reason } => {
            let changed = store.reject_source_suggestion(id, &reason).await?;
            println!(
                "{}",
                serde_json::json!({"id":id,"status":"rejected","changed":changed})
            );
        }
    }
    Ok(())
}

async fn run_digest_cycle(
    store: &CrawlStore,
    telegram: &TelegramClient,
    owner: &str,
    args: &NotifyArgs,
) -> Result<DigestCycleReport> {
    let mut report = DigestCycleReport::default();
    match store
        .prepare_due_digests(i64::try_from(args.plan_batch_size)?)
        .await
    {
        Ok(preparation) => {
            report.batches_created = preparation.batches_created;
            report.items_batched = preparation.items_batched;
            report.duplicate_items_collapsed = preparation.duplicate_items_collapsed;
        }
        Err(error) => {
            report.preparation_error =
                Some(error.to_string().chars().take(1_000).collect::<String>());
        }
    }
    let deliveries = store
        .claim_digest_deliveries(
            owner,
            i64::try_from(args.concurrency)?,
            i64::try_from(args.lease_duration)?,
        )
        .await?;
    let claimed = deliveries.len();
    report.claimed = claimed;
    for delivery in deliveries {
        let outcome = telegram
            .send_message(delivery.telegram_chat_id, &delivery.message_text)
            .await;
        match outcome {
            TelegramSendOutcome::Sent { message_id, .. } => {
                store
                    .complete_digest_delivery(&delivery, owner, message_id)
                    .await?;
                report.sent += 1;
            }
            TelegramSendOutcome::RetryAfter { seconds, detail } => {
                handle_digest_failure(store, owner, &delivery, args, seconds, &detail, &mut report)
                    .await?;
            }
            TelegramSendOutcome::TransientFailure { detail }
            | TelegramSendOutcome::ChatMigrated { detail, .. } => {
                let delay = retry_delay_seconds(
                    delivery.attempt,
                    args.base_retry_delay,
                    args.max_retry_delay,
                );
                handle_digest_failure(store, owner, &delivery, args, delay, &detail, &mut report)
                    .await?;
            }
            TelegramSendOutcome::PermanentFailure { deactivate, detail } => {
                let failure_class = if deactivate {
                    DeliveryFailureClass::RecipientUnavailable
                } else {
                    DeliveryFailureClass::RequestRejected
                };
                store
                    .fail_digest_delivery(&delivery, owner, &detail, failure_class)
                    .await?;
                report.failed += 1;
            }
            TelegramSendOutcome::AuthenticationFailure { detail } => {
                bail!("Telegram authentication failed while sending digest: {detail}")
            }
        }
    }
    Ok(report)
}

fn portal_failure_name(kind: PortalFailureKind) -> &'static str {
    match kind {
        PortalFailureKind::Forbidden => "forbidden",
        PortalFailureKind::RateLimited => "rate_limited",
        PortalFailureKind::Server => "server",
        PortalFailureKind::Network => "network",
        PortalFailureKind::Other => "other",
    }
}

fn portal_cycle_report(state: &PortalPollState) -> PortalCycleReport {
    let mut report = PortalCycleReport::default();
    apply_portal_poll_state(&mut report, state);
    report
}

fn apply_portal_poll_state(report: &mut PortalCycleReport, state: &PortalPollState) {
    report.mode.clone_from(&state.mode);
    report.next_poll_at = Some(state.next_poll_at.to_rfc3339());
    report.cooldown_reason.clone_from(&state.cooldown_reason);
}

fn next_portal_poll_state(
    current: &PortalPollState,
    report: &PortalCycleReport,
    polled_at: DateTime<Utc>,
    config: PortalPollConfig,
) -> Result<PortalPollState> {
    let status = report.http_status.map(i32::from);
    if report.error.is_some() {
        let failure = report.failure_kind.as_deref().unwrap_or("other");
        let delay = match failure {
            "forbidden" => config.forbidden_cooldown,
            "rate_limited" => report
                .retry_after_seconds
                .unwrap_or(config.rate_limit_cooldown)
                .max(1),
            _ => config.failure_cooldown,
        };
        return Ok(PortalPollState {
            mode: "cooldown".to_owned(),
            next_poll_at: add_portal_delay(polled_at, delay)?,
            burst_until: None,
            cooldown_reason: Some(failure.to_owned()),
            last_polled_at: Some(polled_at),
            last_poll_outcome: Some(format!("error_{failure}")),
            last_http_status: status,
        });
    }

    let seed = polled_at.timestamp_millis().unsigned_abs();
    if report.notices_found > 0 {
        let burst_until = add_portal_delay(polled_at, config.burst_duration)?;
        return Ok(PortalPollState {
            mode: "burst".to_owned(),
            next_poll_at: add_portal_delay(
                polled_at,
                jittered_portal_delay(config.burst_interval, config.jitter_percent, seed),
            )?,
            burst_until: Some(burst_until),
            cooldown_reason: None,
            last_polled_at: Some(polled_at),
            last_poll_outcome: Some("new_notices".to_owned()),
            last_http_status: status,
        });
    }

    if current.mode == "burst" && current.burst_until.is_some_and(|until| until > polled_at) {
        return Ok(PortalPollState {
            mode: "burst".to_owned(),
            next_poll_at: add_portal_delay(
                polled_at,
                jittered_portal_delay(config.burst_interval, config.jitter_percent, seed),
            )?,
            burst_until: current.burst_until,
            cooldown_reason: None,
            last_polled_at: Some(polled_at),
            last_poll_outcome: Some("unchanged".to_owned()),
            last_http_status: status,
        });
    }

    Ok(PortalPollState {
        mode: "steady".to_owned(),
        next_poll_at: add_portal_delay(
            polled_at,
            jittered_portal_delay(config.steady_interval, config.jitter_percent, seed),
        )?,
        burst_until: None,
        cooldown_reason: None,
        last_polled_at: Some(polled_at),
        last_poll_outcome: Some("unchanged".to_owned()),
        last_http_status: status,
    })
}

fn jittered_portal_delay(base_seconds: u64, percent: u32, seed: u64) -> u64 {
    let window = base_seconds.saturating_mul(u64::from(percent)) / 100;
    if window == 0 {
        return base_seconds;
    }
    let width = window.saturating_mul(2).saturating_add(1);
    base_seconds
        .saturating_sub(window)
        .saturating_add(seed % width)
        .max(1)
}

fn add_portal_delay(timestamp: DateTime<Utc>, seconds: u64) -> Result<DateTime<Utc>> {
    let seconds = i64::try_from(seconds).context("Portal poll delay exceeds timestamp range")?;
    timestamp
        .checked_add_signed(ChronoDuration::seconds(seconds))
        .context("Portal poll timestamp exceeds supported range")
}

async fn run_portal_cycle(
    store: &CrawlStore,
    portal: &PortalClient,
    args: &NotifyArgs,
) -> PortalCycleReport {
    let mut report = PortalCycleReport {
        checked: true,
        ..PortalCycleReport::default()
    };
    let result = async {
        report.history_archived =
            archive_recent_portal_history_if_empty(store, portal, &mut report).await?;
        let Some(cursor) = store.portal_notice_cursor().await? else {
            let latest_portal_id = portal.latest_portal_id(1).await?;
            report.baseline_initialized = store
                .initialize_portal_notice_cursor(latest_portal_id)
                .await?;
            return Result::<()>::Ok(());
        };
        let latest_portal_id = portal.latest_portal_id(1).await?;
        if latest_portal_id <= cursor {
            return Ok(());
        }
        let notice_ids = portal
            .notice_ids_after(cursor, args.portal_page_size, args.portal_max_pages)
            .await?;
        report.notices_found = notice_ids.len();
        for portal_id in notice_ids {
            let mut notice = portal.fetch_notice(portal_id).await?;
            enrich_portal_notice_attachment(portal, &mut notice, &mut report).await;
            let message_text = render_portal_notification(&notice);
            let outcome = store
                .plan_portal_notice(
                    &PortalNoticeRecord {
                        portal_id: notice.portal_id,
                        title: &notice.title,
                        displayed_at: notice.displayed_at,
                        article_url: notice.article_url.as_deref(),
                        attachment_url: notice.attachment_url.as_deref(),
                        attachment_file_name: None,
                        attachment_content_type: notice.attachment_content_type.as_deref(),
                    },
                    &message_text,
                )
                .await?;
            accumulate_portal_plan(&mut report, &outcome);
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => report.http_status = Some(200),
        Err(error) => {
            let failure = classify_portal_error(&error);
            report.failure_kind = Some(portal_failure_name(failure.kind).to_owned());
            report.http_status = failure.status;
            report.retry_after_seconds = failure.retry_after_seconds;
            report.error = Some(error.to_string().chars().take(1_000).collect());
        }
    }
    report
}

async fn archive_recent_portal_history_if_empty(
    store: &CrawlStore,
    portal: &PortalClient,
    report: &mut PortalCycleReport,
) -> Result<usize> {
    if !store.portal_notice_history(1, 0).await?.is_empty() {
        return Ok(0);
    }
    let portal_ids = portal
        .recent_portal_ids(PORTAL_HISTORY_BACKFILL_SIZE)
        .await?;
    let mut notices = Vec::with_capacity(portal_ids.len());
    for portal_id in portal_ids {
        let mut notice = portal.fetch_notice(portal_id).await?;
        enrich_portal_notice_attachment(portal, &mut notice, report).await;
        notices.push(notice);
    }
    let mut archived = 0;
    for notice in notices {
        archived += usize::from(
            store
                .archive_portal_notice(&PortalNoticeRecord {
                    portal_id: notice.portal_id,
                    title: &notice.title,
                    displayed_at: notice.displayed_at,
                    article_url: notice.article_url.as_deref(),
                    attachment_url: notice.attachment_url.as_deref(),
                    attachment_file_name: None,
                    attachment_content_type: notice.attachment_content_type.as_deref(),
                })
                .await?,
        );
    }
    Ok(archived)
}

async fn enrich_portal_notice_attachment(
    portal: &PortalClient,
    notice: &mut PortalNotice,
    report: &mut PortalCycleReport,
) {
    let Some(article_url) = notice.article_url.as_deref() else {
        return;
    };
    if notice.attachment_url.is_some() {
        return;
    }
    match portal.discover_article_attachment(article_url).await {
        Ok(Some(attachment_url)) => {
            notice.attachment_url = Some(attachment_url);
            notice.attachment_content_type = Some("application/pdf".to_owned());
        }
        Ok(None) => {}
        Err(error) => {
            report.attachment_discovery_errors += 1;
            if report.attachment_discovery_error.is_none() {
                report.attachment_discovery_error =
                    Some(error.to_string().chars().take(1_000).collect());
            }
        }
    }
}

fn accumulate_portal_plan(report: &mut PortalCycleReport, outcome: &PortalNoticePlanOutcome) {
    report.notices_created += usize::from(outcome.notice_created);
    report.campaigns_created += usize::from(outcome.campaign_created);
    report.deliveries_created += outcome.deliveries_created;
}

async fn handle_digest_failure(
    store: &CrawlStore,
    owner: &str,
    delivery: &ClaimedDigestDelivery,
    args: &NotifyArgs,
    delay: u64,
    detail: &str,
    report: &mut DigestCycleReport,
) -> Result<()> {
    if delivery.attempt >= args.max_attempts {
        store
            .fail_digest_delivery(
                delivery,
                owner,
                detail,
                DeliveryFailureClass::RetryExhausted,
            )
            .await?;
        report.failed += 1;
    } else {
        store
            .retry_digest_delivery(delivery, owner, delay, detail)
            .await?;
        report.retry_scheduled += 1;
    }
    Ok(())
}

async fn run_cycle(
    store: &CrawlStore,
    telegram: &TelegramClient,
    portal_client: &PortalClient,
    owner: &str,
    args: &NotifyArgs,
    inputs: CycleInputs,
) -> Result<NotificationCycleReport> {
    let CycleInputs {
        apply_retention,
        interaction,
        operational_alert,
        portal,
    } = inputs;
    let digest = run_digest_cycle(store, telegram, owner, args).await?;
    let events = store
        .claim_notification_events(
            owner,
            i64::try_from(args.plan_batch_size)?,
            i64::try_from(args.lease_duration)?,
        )
        .await?;
    let planned_events = events.len();
    let mut plans = Vec::with_capacity(events.len());
    for event in events {
        plans.push(plan_event(store, telegram, owner, event, args).await);
    }
    let deliveries = store
        .claim_deliveries(
            owner,
            i64::try_from(args.concurrency)?,
            i64::try_from(
                args.lease_duration.max(
                    args.portal_file_timeout
                        .saturating_add(TELEGRAM_DOCUMENT_TIMEOUT_SECONDS),
                ),
            )?,
        )
        .await?;
    let claimed_deliveries = deliveries.len();
    let spacing_nanoseconds = 1_000_000_000_u64 / u64::from(args.messages_per_second);
    let mut interval = tokio::time::interval(Duration::from_nanos(spacing_nanoseconds.max(1)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut tasks = JoinSet::new();
    for delivery in deliveries {
        interval.tick().await;
        let store = store.clone();
        let telegram = telegram.clone();
        let portal_client = portal_client.clone();
        let owner = owner.to_owned();
        let max_attempts = args.max_attempts;
        let retry_delay = retry_delay_seconds(
            delivery.attempt,
            args.base_retry_delay,
            args.max_retry_delay,
        );
        tasks.spawn(async move {
            send_delivery(
                &store,
                &telegram,
                &portal_client,
                &owner,
                delivery,
                max_attempts,
                retry_delay,
            )
            .await
        });
    }
    let mut delivery_reports = Vec::with_capacity(claimed_deliveries);
    while let Some(result) = tasks.join_next().await {
        delivery_reports.push(result.context("Telegram delivery task panicked")?);
    }
    delivery_reports.sort_by_key(|report| report.delivery_id);
    let retention = if apply_retention {
        store
            .apply_delivery_retention(
                args.sent_delivery_retention_days,
                args.failed_delivery_retention_days,
                args.inactive_subscriber_retention_days,
            )
            .await?
    } else {
        DeliveryRetentionOutcome::default()
    };
    Ok(NotificationCycleReport {
        schema_version: "notification-worker-cycle.v1".to_owned(),
        generated_at: Utc::now().to_rfc3339(),
        planned_events,
        campaigns_created: plans
            .iter()
            .filter(|plan| {
                plan.outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.campaign_created)
            })
            .count(),
        deliveries_created: plans
            .iter()
            .filter_map(|plan| plan.outcome.as_ref())
            .map(|outcome| outcome.deliveries_created)
            .sum(),
        planning_skipped: plans
            .iter()
            .filter(|plan| plan.outcome.as_ref().is_some_and(|outcome| outcome.skipped))
            .count(),
        planning_failed: plans.iter().filter(|plan| plan.error.is_some()).count(),
        claimed_deliveries,
        sent: count_delivery_outcome(&delivery_reports, "sent"),
        retry_scheduled: count_delivery_outcome(&delivery_reports, "retry_scheduled"),
        permanently_failed: count_delivery_outcome(&delivery_reports, "permanently_failed"),
        chat_migrated: count_delivery_outcome(&delivery_reports, "chat_migrated"),
        authentication_failed: interaction.authentication_failed
            || operational_alert.authentication_failed
            || delivery_reports
                .iter()
                .any(|report| report.outcome == "authentication_failed"),
        retention_applied: apply_retention,
        retention,
        interaction,
        operational_alert,
        digest,
        portal,
        plans,
        deliveries: delivery_reports,
    })
}

async fn run_operational_alert_cycle(
    store: &CrawlStore,
    telegram: &TelegramClient,
    admin_chat_id: Option<i64>,
    health_alert_after_failures: u32,
    health_backlog_stale_seconds: u64,
    health_alert_grace_seconds: u64,
) -> Result<OperationalAlertReport> {
    let Some(admin_chat_id) = admin_chat_id else {
        return Ok(OperationalAlertReport {
            outcome: Some("disabled".to_owned()),
            ..OperationalAlertReport::default()
        });
    };
    let health = store
        .operational_health(health_alert_after_failures, health_backlog_stale_seconds)
        .await?;
    let Some(candidate) = store
        .observe_operational_alert("primary", &health.status, health_alert_grace_seconds)
        .await?
    else {
        return Ok(OperationalAlertReport {
            status: Some(health.status),
            outcome: Some("not_due".to_owned()),
            ..OperationalAlertReport::default()
        });
    };
    let message = render_operational_alert(candidate.kind, &health);
    let mut report = OperationalAlertReport {
        status: Some(health.status),
        kind: Some(candidate.kind),
        ..OperationalAlertReport::default()
    };
    match telegram.send_message(admin_chat_id, &message).await {
        TelegramSendOutcome::Sent { .. } => {
            report.outcome = Some(
                if store.complete_operational_alert(&candidate).await? {
                    "sent"
                } else {
                    "state_changed"
                }
                .to_owned(),
            );
        }
        TelegramSendOutcome::RetryAfter { detail, .. }
        | TelegramSendOutcome::TransientFailure { detail } => {
            report.outcome = Some("retry".to_owned());
            report.error = Some(detail);
        }
        TelegramSendOutcome::AuthenticationFailure { detail } => {
            report.outcome = Some("authentication_failed".to_owned());
            report.authentication_failed = true;
            report.error = Some(detail);
        }
        TelegramSendOutcome::PermanentFailure { detail, .. }
        | TelegramSendOutcome::ChatMigrated { detail, .. } => {
            report.outcome = Some("failed".to_owned());
            report.error = Some(detail);
        }
    }
    Ok(report)
}

async fn run_interaction_cycle(
    store: &CrawlStore,
    telegram: &TelegramClient,
    portal: &PortalClient,
    updates_source: TelegramUpdatesSource,
    context: InteractionContext<'_>,
) -> Result<InteractionCycleReport> {
    if updates_source == TelegramUpdatesSource::Edge {
        return run_edge_interaction_cycle(store, telegram, portal, context).await;
    }
    let consumer_key = "telegram_private_commands";
    let offset = store.telegram_next_update_id(consumer_key).await?;
    let updates = match telegram.get_updates(offset, 100).await {
        TelegramUpdatesOutcome::Received { updates } => updates,
        TelegramUpdatesOutcome::RetryAfter { seconds, detail } => {
            return Ok(InteractionCycleReport {
                error: Some(format!("Telegram yêu cầu chờ {seconds} giây: {detail}")),
                ..InteractionCycleReport::default()
            });
        }
        TelegramUpdatesOutcome::TransientFailure { detail } => {
            return Ok(InteractionCycleReport {
                error: Some(detail),
                ..InteractionCycleReport::default()
            });
        }
        TelegramUpdatesOutcome::AuthenticationFailure { detail } => {
            return Ok(InteractionCycleReport {
                authentication_failed: true,
                error: Some(detail),
                ..InteractionCycleReport::default()
            });
        }
    };
    let received = updates.len();
    let Some(next_update_id) = updates
        .iter()
        .map(|update| update.update_id)
        .max()
        .and_then(|update_id| update_id.checked_add(1))
    else {
        let mut report = InteractionCycleReport {
            received,
            ..InteractionCycleReport::default()
        };
        merge_feedback_delivery_report(
            &mut report,
            deliver_pending_user_feedback(store, telegram, context.admin_chat_id).await?,
        );
        return Ok(report);
    };
    let (mut report, complete) =
        process_interaction_updates(store, telegram, portal, updates, offset == 0, context).await?;
    merge_feedback_delivery_report(
        &mut report,
        deliver_pending_user_feedback(store, telegram, context.admin_chat_id).await?,
    );
    if complete {
        store
            .advance_telegram_update_id(consumer_key, next_update_id)
            .await?;
    }
    Ok(report)
}

#[derive(Debug, Default)]
struct FeedbackDeliveryReport {
    sent: usize,
    authentication_failed: bool,
    error: Option<String>,
}

fn merge_feedback_delivery_report(
    report: &mut InteractionCycleReport,
    feedback: FeedbackDeliveryReport,
) {
    report.admin_notified += feedback.sent;
    report.authentication_failed |= feedback.authentication_failed;
    if report.error.is_none() {
        report.error = feedback.error;
    }
}

async fn deliver_pending_user_feedback(
    store: &CrawlStore,
    telegram: &TelegramClient,
    admin_chat_id: Option<i64>,
) -> Result<FeedbackDeliveryReport> {
    let Some(admin_chat_id) = admin_chat_id else {
        return Ok(FeedbackDeliveryReport::default());
    };
    let mut report = FeedbackDeliveryReport::default();
    for feedback in store.pending_user_feedback(10).await? {
        let text = format!(
            "Phản hồi người dùng #{}\nNgười gửi: {}\nTelegram chat ID: {}\n\n{}",
            feedback.id, feedback.sender_label, feedback.telegram_chat_id, feedback.message
        );
        match telegram
            .send_admin_feedback(admin_chat_id, &text, feedback.telegram_chat_id)
            .await
        {
            TelegramSendOutcome::Sent { .. } => {
                if store.mark_user_feedback_notified(feedback.id).await? {
                    report.sent += 1;
                }
            }
            TelegramSendOutcome::AuthenticationFailure { detail } => {
                store
                    .mark_user_feedback_attempt(feedback.id, &detail)
                    .await?;
                report.authentication_failed = true;
                report.error = Some(detail);
                break;
            }
            TelegramSendOutcome::RetryAfter { detail, .. }
            | TelegramSendOutcome::ChatMigrated { detail, .. }
            | TelegramSendOutcome::PermanentFailure { detail, .. }
            | TelegramSendOutcome::TransientFailure { detail } => {
                store
                    .mark_user_feedback_attempt(feedback.id, &detail)
                    .await?;
                report.error = Some(detail);
                break;
            }
        }
    }
    Ok(report)
}

async fn process_interaction_updates(
    store: &CrawlStore,
    telegram: &TelegramClient,
    portal: &PortalClient,
    updates: Vec<uth_delivery::TelegramUpdate>,
    collapse_initial_updates: bool,
    context: InteractionContext<'_>,
) -> Result<(InteractionCycleReport, bool)> {
    let received = updates.len();
    let mut private_messages = Vec::new();
    let mut ignored = 0;
    if collapse_initial_updates {
        let mut latest_private_messages = BTreeMap::new();
        for update in updates {
            match interaction_from_update(update) {
                Some((update_id, message, callback_query_id)) => {
                    latest_private_messages
                        .insert(message.chat.id, (update_id, message, callback_query_id));
                }
                _ => ignored += 1,
            }
        }
        ignored += received.saturating_sub(ignored + latest_private_messages.len());
        private_messages.extend(latest_private_messages.into_values());
        private_messages.sort_by_key(|(update_id, _, _)| *update_id);
    } else {
        for update in updates {
            match interaction_from_update(update) {
                Some(interaction) => private_messages.push(interaction),
                _ => ignored += 1,
            }
        }
    }
    let mut processed = 0;
    let mut replied = 0;
    let mut admin_notified = 0;
    let mut suggestions_created = 0;
    let mut interaction_error = None;
    for (update_id, message, callback_query_id) in private_messages {
        if context.admin_only && Some(message.chat.id) != context.admin_chat_id {
            ignored += 1;
            continue;
        }
        if let Some(callback_query_id) = callback_query_id.as_deref() {
            let _ = telegram.answer_callback_query(callback_query_id).await;
        }
        processed += 1;
        let mut reply = match build_interaction_reply(store, portal, update_id, &message, context)
            .await
        {
            Ok(reply) => reply,
            Err(error) => {
                interaction_error = Some(error.to_string());
                simple_reply(
                    "Yêu cầu này không còn hiệu lực hoặc chưa thể xử lý. Vui lòng mở /start để tiếp tục."
                        .to_owned(),
                )
            }
        };
        if reply.suggestion_created {
            suggestions_created += 1;
        }
        let send_outcome = if let Some(document) = reply.portal_document.take() {
            let PortalAttachment {
                bytes,
                file_name,
                content_type,
            } = document.attachment;
            telegram
                .upload_portal_document(
                    message.chat.id,
                    &reply.user_text,
                    &file_name,
                    &content_type,
                    bytes,
                    reply.portal_source_url.as_deref(),
                )
                .await
        } else {
            match reply.photo_png.as_deref() {
                Some(photo) => {
                    telegram
                        .send_photo(message.chat.id, &reply.user_text, photo)
                        .await
                }
                None if reply.donation_prompt => {
                    telegram
                        .send_donation_prompt(message.chat.id, &reply.user_text)
                        .await
                }
                None if reply.onboarding_prompt => {
                    telegram
                        .send_onboarding_prompt(message.chat.id, &reply.user_text)
                        .await
                }
                None if reply.settings_prompt => {
                    telegram
                        .send_settings_prompt(message.chat.id, &reply.user_text)
                        .await
                }
                None if reply.portal_source_url.is_some() => {
                    telegram
                        .send_portal_notification(
                            message.chat.id,
                            &reply.user_text,
                            reply.portal_source_url.as_deref(),
                        )
                        .await
                }
                None if !reply.user_links.is_empty() => {
                    telegram
                        .send_message_with_user_links(
                            message.chat.id,
                            &reply.user_text,
                            &reply.user_links,
                        )
                        .await
                }
                None => {
                    telegram
                        .send_message(message.chat.id, &reply.user_text)
                        .await
                }
            }
        };
        match send_outcome {
            TelegramSendOutcome::Sent { .. } => {
                replied += 1;
                if reply.complete_donation_amount_input {
                    store.clear_donation_amount_input(message.chat.id).await?;
                }
            }
            TelegramSendOutcome::PermanentFailure { deactivate, detail } => {
                if deactivate {
                    store
                        .deactivate_subscriber(message.chat.id, &detail)
                        .await?;
                }
                ignored += 1;
            }
            TelegramSendOutcome::RetryAfter { seconds, detail } => {
                return Ok((
                    InteractionCycleReport {
                        received,
                        processed,
                        replied,
                        admin_notified,
                        suggestions_created,
                        ignored,
                        error: Some(format!("Telegram yêu cầu chờ {seconds} giây: {detail}")),
                        ..InteractionCycleReport::default()
                    },
                    false,
                ));
            }
            TelegramSendOutcome::TransientFailure { detail }
            | TelegramSendOutcome::ChatMigrated { detail, .. } => {
                return Ok((
                    InteractionCycleReport {
                        received,
                        processed,
                        replied,
                        admin_notified,
                        suggestions_created,
                        ignored,
                        error: Some(detail),
                        ..InteractionCycleReport::default()
                    },
                    false,
                ));
            }
            TelegramSendOutcome::AuthenticationFailure { detail } => {
                return Ok((
                    InteractionCycleReport {
                        received,
                        processed,
                        replied,
                        admin_notified,
                        suggestions_created,
                        ignored,
                        authentication_failed: true,
                        error: Some(detail),
                    },
                    false,
                ));
            }
        }
        if let (Some(admin_chat_id), Some(admin_text)) = (context.admin_chat_id, reply.admin_text) {
            match telegram.send_message(admin_chat_id, &admin_text).await {
                TelegramSendOutcome::Sent { .. } => admin_notified += 1,
                TelegramSendOutcome::AuthenticationFailure { detail } => {
                    interaction_error = Some(detail);
                }
                TelegramSendOutcome::RetryAfter { detail, .. }
                | TelegramSendOutcome::ChatMigrated { detail, .. }
                | TelegramSendOutcome::PermanentFailure { detail, .. }
                | TelegramSendOutcome::TransientFailure { detail } => {
                    interaction_error = Some(detail);
                }
            }
        }
    }
    Ok((
        InteractionCycleReport {
            received,
            processed,
            replied,
            admin_notified,
            suggestions_created,
            ignored,
            authentication_failed: false,
            error: interaction_error,
        },
        true,
    ))
}

fn interaction_from_update(
    update: uth_delivery::TelegramUpdate,
) -> Option<(i64, TelegramIncomingMessage, Option<String>)> {
    if let Some(message) = update.message
        && message.chat.kind == "private"
        && message.text.is_some()
    {
        return Some((update.update_id, message, None));
    }
    let callback_query = update.callback_query?;
    let mut message = callback_query.message?;
    if message.chat.kind != "private" {
        return None;
    }
    message.text = callback_query.data;
    message.text.as_ref()?;
    Some((update.update_id, message, Some(callback_query.id)))
}

async fn run_edge_interaction_cycle(
    store: &CrawlStore,
    telegram: &TelegramClient,
    portal: &PortalClient,
    context: InteractionContext<'_>,
) -> Result<InteractionCycleReport> {
    let Some(event) = store.pending_telegram_edge_events(1).await?.pop() else {
        let mut report = InteractionCycleReport::default();
        merge_feedback_delivery_report(
            &mut report,
            deliver_pending_user_feedback(store, telegram, context.admin_chat_id).await?,
        );
        return Ok(report);
    };
    let update = match serde_json::from_value(event.payload) {
        Ok(update) => update,
        Err(error) => {
            let detail = format!("edge inbox contains an invalid Telegram update: {error}");
            store
                .fail_edge_event(
                    &event.event_id,
                    &detail,
                    3,
                    retry_delay_seconds(event.attempts + 1, 5, 60),
                )
                .await?;
            return Ok(InteractionCycleReport {
                error: Some(detail),
                ..InteractionCycleReport::default()
            });
        }
    };
    let (mut report, complete) =
        match process_interaction_updates(store, telegram, portal, vec![update], false, context)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let detail = error.to_string();
                store
                    .fail_edge_event(
                        &event.event_id,
                        &detail,
                        3,
                        retry_delay_seconds(event.attempts + 1, 5, 60),
                    )
                    .await?;
                return Ok(InteractionCycleReport {
                    error: Some(detail),
                    ..InteractionCycleReport::default()
                });
            }
        };
    merge_feedback_delivery_report(
        &mut report,
        deliver_pending_user_feedback(store, telegram, context.admin_chat_id).await?,
    );
    if complete {
        store.complete_edge_event(&event.event_id).await?;
    } else {
        store
            .fail_edge_event(
                &event.event_id,
                report
                    .error
                    .as_deref()
                    .unwrap_or("Telegram interaction did not complete"),
                5,
                retry_delay_seconds(event.attempts + 1, 5, 300),
            )
            .await?;
    }
    Ok(report)
}

struct InteractionReply {
    user_text: String,
    user_links: Vec<TelegramUserLink>,
    photo_png: Option<Vec<u8>>,
    portal_document: Option<PortalDocumentReply>,
    portal_source_url: Option<String>,
    donation_prompt: bool,
    onboarding_prompt: bool,
    settings_prompt: bool,
    complete_donation_amount_input: bool,
    admin_text: Option<String>,
    suggestion_created: bool,
}

struct PortalDocumentReply {
    attachment: PortalAttachment,
}

struct UserFeedbackPage {
    text: String,
    user_links: Vec<TelegramUserLink>,
}

async fn build_interaction_reply(
    store: &CrawlStore,
    portal: &PortalClient,
    update_id: i64,
    message: &TelegramIncomingMessage,
    context: InteractionContext<'_>,
) -> Result<InteractionReply> {
    let text = message.text.as_deref().unwrap_or_default();
    let command = resolve_interaction_command(store, message.chat.id, text).await?;
    match command {
        InteractionCommand::Start(acquisition_source) => {
            let existing = store.subscriber(message.chat.id).await?;
            let was_active = existing.as_ref().is_some_and(|subscriber| subscriber.active);
            let display_name = existing
                .as_ref()
                .and_then(|subscriber| subscriber.display_name.clone())
                .or_else(|| telegram_display_name(&message.chat));
            let onboarding_completed = store
                .begin_subscriber_onboarding(
                    message.chat.id,
                    display_name.as_deref(),
                    acquisition_source.as_deref(),
                )
                .await?;
            if onboarding_completed {
                store
                    .upsert_subscriber(message.chat.id, display_name.as_deref())
                    .await?;
                return Ok(InteractionReply {
                    user_text: if was_active {
                        with_support_group(
                            "Thông báo đang bật. Bạn có thể xem một tin mẫu hoặc đổi loại hoạt động và cách nhận bên dưới.",
                        )
                    } else {
                        with_support_group(
                            "Đã bật lại thông báo với cài đặt trước đây. Bạn có thể xem một tin mẫu hoặc điều chỉnh bên dưới.",
                        )
                    },
                    user_links: Vec::new(),
                    photo_png: None,
                    portal_document: None,
                    portal_source_url: None,
                    donation_prompt: false,
                    onboarding_prompt: false,
                    settings_prompt: true,
                    complete_donation_amount_input: false,
                    admin_text: None,
                    suggestion_created: false,
                });
            }
            let greeting = display_name
                .as_deref()
                .map(|name| format!("Chào {name}. "))
                .unwrap_or_default();
            let source_count = store.list_enabled_sources().await?.len();
            Ok(InteractionReply {
                user_text: with_support_group(&format!(
                    "{greeting}UTH Notifier đang lọc {source_count} nguồn công khai để tìm hoạt động, biểu mẫu đăng ký và thông tin điểm rèn luyện.\n\nBạn muốn nhận loại tin nào? Bạn có thể đổi lại bất cứ lúc nào."
                )),
                user_links: Vec::new(),
                photo_png: None,
                portal_document: None,
                portal_source_url: None,
                donation_prompt: false,
                onboarding_prompt: true,
                settings_prompt: false,
                complete_donation_amount_input: false,
                admin_text: None,
                suggestion_created: false,
            })
        }
        InteractionCommand::OnboardScope(scope) => {
            store
                .complete_subscriber_onboarding(message.chat.id, scope)
                .await?;
            Ok(simple_reply(if scope == "drl" {
                "Đã bật thông báo cho các hoạt động có nhắc tới điểm rèn luyện. Bạn có thể đổi trong Cài đặt.".to_owned()
            } else {
                "Đã bật thông báo cho mọi hoạt động phù hợp. Bạn có thể đổi trong Cài đặt.".to_owned()
            }))
        }
        InteractionCommand::OnboardSample => {
            store
                .record_product_event(message.chat.id, "sample_viewed", None, serde_json::json!({}))
                .await?;
            let sample = store.latest_notification_sample().await?.map(|message| {
                humanize_notification_sample(&message)
            }).unwrap_or_else(|| {
                "Chưa có tin mẫu trong hệ thống. Bạn vẫn có thể chọn loại tin để bắt đầu nhận bài mới.".to_owned()
            });
            Ok(InteractionReply {
                user_text: format!("Tin mẫu gần nhất:\n\n{sample}\n\nChọn loại tin bạn muốn nhận:"),
                user_links: Vec::new(),
                photo_png: None,
                portal_document: None,
                portal_source_url: None,
                donation_prompt: false,
                onboarding_prompt: true,
                settings_prompt: false,
                complete_donation_amount_input: false,
                admin_text: None,
                suggestion_created: false,
            })
        }
        InteractionCommand::Settings => {
            let subscriber = store.subscriber(message.chat.id).await?;
            if subscriber.is_none() {
                return Ok(InteractionReply {
                    user_text: "Bạn chưa bật thông báo. Hãy chọn loại tin muốn nhận để bắt đầu."
                        .to_owned(),
                    user_links: Vec::new(),
                    photo_png: None,
                    portal_document: None,
                    portal_source_url: None,
                    donation_prompt: false,
                    onboarding_prompt: true,
                    settings_prompt: false,
                    complete_donation_amount_input: false,
                    admin_text: None,
                    suggestion_created: false,
                });
            }
            let status = subscriber.map(|value| {
                format!(
                    "Loại hoạt động: {}\nCách nhận: {}\nGiờ yên lặng (22:00–07:00): {}",
                    if value.notification_scope == "drl" { "Chỉ tin có điểm rèn luyện" } else { "Mọi hoạt động phù hợp" },
                    if value.delivery_mode == "daily" { "Một bản tin lúc 07:30" } else { "Từng tin ngay khi phát hiện" },
                    if value.quiet_hours_enabled { "Bật" } else { "Tắt" }
                )
            }).unwrap_or_else(|| "Bạn cần chọn Bật thông báo trước.".to_owned());
            Ok(InteractionReply {
                user_text: format!("Cài đặt hiện tại:\n\n{status}\n\nThông báo mới từ Portal UTH là bắt buộc và không bị ảnh hưởng bởi các lựa chọn này.\n\nChọn mục muốn thay đổi:"),
                user_links: Vec::new(),
                photo_png: None,
                portal_document: None,
                portal_source_url: None,
                donation_prompt: false,
                onboarding_prompt: false,
                settings_prompt: true,
                complete_donation_amount_input: false,
                admin_text: None,
                suggestion_created: false,
            })
        }
        InteractionCommand::SettingsSample => {
            store
                .record_product_event(
                    message.chat.id,
                    "sample_viewed",
                    None,
                    serde_json::json!({"entry_point": "settings"}),
                )
                .await?;
            let sample = store
                .latest_notification_sample()
                .await?
                .map(|message| humanize_notification_sample(&message))
                .unwrap_or_else(|| {
                    "Chưa có tin mẫu trong hệ thống. Bot sẽ báo khi có hoạt động phù hợp mới."
                        .to_owned()
                });
            Ok(InteractionReply {
                user_text: format!("Tin mẫu gần nhất:\n\n{sample}\n\nBạn có thể tiếp tục đổi cài đặt bên dưới:"),
                user_links: Vec::new(),
                photo_png: None,
                portal_document: None,
                portal_source_url: None,
                donation_prompt: false,
                onboarding_prompt: false,
                settings_prompt: true,
                complete_donation_amount_input: false,
                admin_text: None,
                suggestion_created: false,
            })
        }
        InteractionCommand::SettingsScope(scope) => {
            store.update_subscriber_preferences(message.chat.id, Some(scope), None, None).await?;
            Ok(simple_reply(if scope == "drl" {
                "Từ giờ, bot chỉ gửi hoạt động có nhắc tới điểm rèn luyện.".to_owned()
            } else {
                "Từ giờ, bot gửi mọi hoạt động được đánh giá là phù hợp.".to_owned()
            }))
        }
        InteractionCommand::SettingsMode(mode) => {
            store.update_subscriber_preferences(message.chat.id, None, Some(mode), None).await?;
            Ok(simple_reply(if mode == "daily" {
                "Từ giờ, bot sẽ gom các hoạt động mới phù hợp và gửi một bản tin lúc 07:30 mỗi ngày. Nếu không có tin mới, bot sẽ không gửi.".to_owned()
            } else {
                "Từ giờ, bot sẽ gửi từng hoạt động phù hợp ngay khi phát hiện, theo cài đặt giờ yên lặng của bạn.".to_owned()
            }))
        }
        InteractionCommand::SettingsQuiet(enabled) => {
            store.update_subscriber_preferences(message.chat.id, None, None, Some(enabled)).await?;
            Ok(simple_reply(if enabled {
                "Đã bật giờ yên lặng từ 22:00 đến 07:00. Tin cần gửi ngay trong khoảng này sẽ được giữ lại đến sau 07:00.".to_owned()
            } else {
                "Đã tắt giờ yên lặng. Bot sẽ gửi theo cách nhận bạn đã chọn.".to_owned()
            }))
        }
        InteractionCommand::UserFeedbackPrompt => {
            let expires_at = Utc::now()
                .checked_add_signed(ChronoDuration::seconds(USER_FEEDBACK_INPUT_TTL_SECONDS))
                .context("user feedback input expiry is outside timestamp range")?;
            store
                .begin_user_feedback_input(message.chat.id, expires_at)
                .await?;
            Ok(simple_reply(
                "Bạn muốn góp ý điều gì? Hãy gửi nội dung trong tin nhắn tiếp theo. Bạn có 10 phút; gửi /cancel nếu đổi ý."
                    .to_owned(),
            ))
        }
        InteractionCommand::UserFeedback(feedback) => {
            let sender_label = telegram_sender_label(&message.chat);
            store
                .record_user_feedback(message.chat.id, update_id, &sender_label, &feedback)
                .await?;
            Ok(simple_reply(
                "Mình đã nhận feedback và chuyển cho quản trị viên. Cảm ơn bạn đã góp ý.".to_owned(),
            ))
        }
        InteractionCommand::Feedback { campaign_id, value } => {
            let outcome = store.record_notification_feedback(message.chat.id, campaign_id, value).await?;
            if outcome.should_prompt_donation && context.payos.is_some() {
                Ok(InteractionReply {
                    user_text: "Cảm ơn phản hồi của bạn. Nếu thông báo này đã giúp ích, bạn có thể hỗ trợ một phần chi phí vận hành. Việc ủng hộ hoàn toàn tự nguyện và không ảnh hưởng quyền dùng bot.".to_owned(),
                user_links: Vec::new(),
                photo_png: None,
                portal_document: None,
                portal_source_url: None,
                donation_prompt: true,
                    onboarding_prompt: false,
                    settings_prompt: false,
                    complete_donation_amount_input: false,
                    admin_text: None,
                    suggestion_created: false,
                })
            } else {
                Ok(simple_reply(if value == "useful" {
                    "Đã ghi nhận đây là thông báo hữu ích.".to_owned()
                } else {
                    "Đã ghi nhận. Phản hồi này sẽ được dùng để cải thiện độ phù hợp.".to_owned()
                }))
            }
        }
        InteractionCommand::OpenAction(campaign_id) => {
            let action = store
                .open_campaign_action(message.chat.id, campaign_id)
                .await?;
            Ok(simple_reply(match action {
                Some(url) => format!("Mở biểu mẫu đăng ký:\n{url}"),
                None => "Link đăng ký không còn khả dụng. Bạn có thể mở bài gốc để kiểm tra.".to_owned(),
            }))
        }
        InteractionCommand::Metrics => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(admin_only_reply());
            }
            let metrics = store.growth_metrics().await?;
            Ok(simple_reply(format!(
                "Kết quả 7 ngày gần nhất\n\nLượt bắt đầu dùng bot: {}\nLượt hoàn tất thiết lập: {}\nNgười đang bật thông báo: {}\nThông báo đã gửi: {}\nLượt mở đăng ký: {}\nPhản hồi hữu ích: {}\nPhản hồi không phù hợp: {}\nLượt ủng hộ thành công: {}\nTổng tiền ủng hộ: {}",
                metrics.starts_7d,
                metrics.onboarding_completed_7d,
                metrics.active_subscribers,
                metrics.notifications_delivered_7d,
                metrics.cta_clicks_7d,
                metrics.useful_feedback_7d,
                metrics.irrelevant_feedback_7d,
                metrics.donations_paid_7d,
                format_vnd(metrics.donation_amount_7d)
            )))
        }
        InteractionCommand::FeedbackHistory(page) => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(admin_only_reply());
            }
            let page_size = 5_usize;
            let offset = page.saturating_sub(1).saturating_mul(page_size);
            let (total, feedback) = store
                .user_feedback_history(page_size, offset)
                .await?;
            let feedback_page = render_user_feedback_page(&feedback, total, page, page_size);
            let mut reply = simple_reply(feedback_page.text);
            reply.user_links = feedback_page.user_links;
            Ok(reply)
        }
        InteractionCommand::Help => {
            Ok(simple_reply(render_help(
                Some(message.chat.id) == context.admin_chat_id,
            )))
        }
        InteractionCommand::Admin => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(admin_only_reply());
            }
            Ok(simple_reply(
                "Công cụ quản trị\n\n- /reviews: bài đang chờ duyệt\n- /pending: đề xuất nguồn đang chờ\n- /latest: bài thu thập gần đây\n- /feedbacks: toàn bộ feedback đã gửi\n- /crawl_history: lịch sử tất cả lần crawl\n- /crawl_run_ID: chi tiết một lần crawl\n- /portal_history: lịch sử crawl Portal\n- /metrics: kết quả 7 ngày\n\nDùng /portal_notice_ID để xem thông báo Portal và nhận tệp đính kèm nếu có. Các tin chi tiết khác sẽ cung cấp nút hoặc lệnh duyệt tương ứng."
                    .to_owned(),
            ))
        }
        InteractionCommand::CrawlHistory(page) => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(admin_only_reply());
            }
            let page_size = 8_usize;
            let offset = page.saturating_sub(1).saturating_mul(page_size);
            let runs = store
                .crawl_history(i64::try_from(page_size + 1)?, i64::try_from(offset)?)
                .await?;
            Ok(simple_reply(render_crawl_history_page(&runs, page, page_size)))
        }
        InteractionCommand::CrawlRun(run_id) => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(admin_only_reply());
            }
            let body = match store.crawl_history_item(run_id).await? {
                Some(detail) => render_crawl_history_detail(&detail),
                None => format!("KhÃ´ng tÃ¬m tháº¥y láº§n crawl #{run_id} trong lá»‹ch sá»­."),
            };
            Ok(simple_reply(body))
        }
        InteractionCommand::PortalHistory(page) => {
            let page_size = 8_usize;
            let offset = page.saturating_sub(1).saturating_mul(page_size);
            let notices = store
                .portal_notice_history(i64::try_from(page_size + 1)?, i64::try_from(offset)?)
                .await?;
            Ok(simple_reply(render_portal_notice_history_page(
                &notices, page, page_size,
            )))
        }
        InteractionCommand::PortalNotice(portal_id) => {
            let Some(mut notice) = store.portal_notice_history_item(portal_id).await? else {
                return Ok(simple_reply(format!(
                    "Không tìm thấy thông báo Portal #{portal_id} trong lịch sử crawl."
                )));
            };
            if notice.attachment_url.is_none()
                && let Some(article_url) = notice.article_url.as_deref()
                && let Ok(Some(attachment_url)) = portal.discover_article_attachment(article_url).await
            {
                let content_type = "application/pdf";
                store
                    .set_portal_notice_attachment(notice.portal_id, &attachment_url, content_type)
                    .await?;
                notice.attachment_url = Some(attachment_url);
                notice.attachment_content_type = Some(content_type.to_owned());
            }
            let source_url = notice
                .article_url
                .clone()
                .or_else(|| notice.attachment_url.clone());
            let mut reply = InteractionReply {
                user_text: render_portal_notice_history_detail(&notice),
                user_links: Vec::new(),
                photo_png: None,
                portal_document: None,
                portal_source_url: source_url,
                donation_prompt: false,
                onboarding_prompt: false,
                settings_prompt: false,
                complete_donation_amount_input: false,
                admin_text: None,
                suggestion_created: false,
            };
            if let Some(attachment_url) = notice.attachment_url.as_deref() {
                match portal
                    .download_attachment(
                        notice.portal_id,
                        attachment_url,
                        notice.attachment_content_type.as_deref(),
                    )
                    .await
                {
                    Ok(attachment) => reply.portal_document = Some(PortalDocumentReply { attachment }),
                    Err(error) => reply.user_text.push_str(&format!(
                        "\n\nKhông tải được tệp đính kèm lúc này: {}",
                        error.to_string().chars().take(300).collect::<String>()
                    )),
                }
            }
            Ok(reply)
        }
        InteractionCommand::Donate => Ok(InteractionReply {
            user_text: render_donation(context.donation, context.payos.is_some()),
            user_links: Vec::new(),
            photo_png: None,
            portal_document: None,
            portal_source_url: None,
            donation_prompt: context.payos.is_some(),
            onboarding_prompt: false,
            settings_prompt: false,
            complete_donation_amount_input: false,
            admin_text: None,
            suggestion_created: false,
        }),
        InteractionCommand::PromptDonationAmount => {
            if context.payos.is_none() {
                return Ok(simple_reply(render_donation(context.donation, false)));
            }
            let expires_at = Utc::now()
                .checked_add_signed(ChronoDuration::seconds(
                    DONATION_AMOUNT_INPUT_TTL_SECONDS,
                ))
                .context("donation amount input expiry is outside timestamp range")?;
            store
                .begin_donation_amount_input(message.chat.id, expires_at)
                .await?;
            Ok(simple_reply(
                "Bạn muốn ủng hộ bao nhiêu? Hãy gửi số tiền từ 10.000 đến 10.000.000 VND. Ví dụ: 35000, 35.000 hoặc 35k. Gửi /cancel nếu bạn đổi ý."
                    .to_owned(),
            ))
        }
        InteractionCommand::InvalidDonationAmount => Ok(simple_reply(
            "Số tiền chưa hợp lệ. Hãy nhập từ 10.000 đến 10.000.000 VND, ví dụ 35000, 35.000 hoặc 35k. Gửi /cancel nếu bạn đổi ý."
                .to_owned(),
        )),
        InteractionCommand::Cancel => {
            if store.clear_user_feedback_input(message.chat.id).await? {
                Ok(simple_reply("Đã hủy gửi feedback.".to_owned()))
            } else {
                Ok(simple_reply("Đã hủy nhập số tiền ủng hộ.".to_owned()))
            }
        }
        InteractionCommand::DeclineDonation => Ok(simple_reply(
            "Không sao cả. Bạn cứ dùng UTH Notifier bình thường nhé.".to_owned(),
        )),
        command @ InteractionCommand::DonateAmount(amount)
        | command @ InteractionCommand::CustomDonationAmount(amount) => {
            let custom_amount = matches!(command, InteractionCommand::CustomDonationAmount(_));
            let Some(payos) = context.payos else {
                return Ok(simple_reply(render_donation(context.donation, false)));
            };
            let expires_at = Utc::now()
                .checked_add_signed(ChronoDuration::seconds(context.donation_link_ttl))
                .context("donation link expiry is outside timestamp range")?;
            let intent = store
                .create_donation_intent(message.chat.id, amount, expires_at)
                .await?;
            let payment = match payos
                .create_payment_link(intent.order_code, intent.amount, expires_at.timestamp())
                .await
            {
                Ok(payment) => payment,
                Err(error) => {
                    store
                        .mark_donation_intent_failed(intent.order_code, &error.to_string())
                        .await?;
                    return Ok(simple_reply(render_donation(context.donation, false)));
                }
            };
            store
                .mark_donation_intent_pending(
                    intent.order_code,
                    &DonationIntentPaymentLink {
                        bank_bin: &payment.bank_bin,
                        account_number: &payment.account_number,
                        account_name: &payment.account_name,
                        transfer_description: &payment.description,
                        payment_link_id: &payment.payment_link_id,
                        checkout_url: &payment.checkout_url,
                        qr_code: &payment.qr_code,
                    },
                )
                .await?;
            Ok(InteractionReply {
                user_text: format!(
                    "Quét QR để ủng hộ {}\n\nNgân hàng nhận: {}\nNgười nhận: {}\n{}\nNội dung chuyển khoản: {}\n\nMở trang thanh toán:\n{}\n\nQR có hiệu lực trong {} phút. Số tiền và nội dung đã được điền sẵn; bot sẽ tự động xác nhận khi giao dịch thành công.",
                    format_vnd(payment.amount),
                    display_bank(&payment.bank_bin),
                    payment.account_name,
                    render_payment_account(context.donation),
                    payment.description,
                    payment.checkout_url,
                    context.donation_link_ttl / 60
                ),
                user_links: Vec::new(),
                photo_png: Some(payment.qr_png),
                portal_document: None,
                portal_source_url: None,
                donation_prompt: false,
                onboarding_prompt: false,
                settings_prompt: false,
                complete_donation_amount_input: custom_amount,
                admin_text: None,
                suggestion_created: false,
            })
        }
        InteractionCommand::Status => {
            let subscriber = store.subscriber(message.chat.id).await?;
            let subscription = match subscriber {
                Some(subscriber) if subscriber.active =>
                    "Bạn đang nhận tin hoạt động và thông báo bắt buộc từ Portal UTH.".to_owned(),
                Some(_) => "Bạn đang tắt tin hoạt động nhưng vẫn nhận thông báo bắt buộc từ Portal UTH. Chọn Bật thông báo khi muốn nhận lại tin hoạt động.".to_owned(),
                None => "Bạn chưa thiết lập bot. Chọn Bật thông báo để bắt đầu.".to_owned(),
            };
            let health = store
                .operational_health(
                    context.health_alert_after_failures,
                    context.health_backlog_stale_seconds,
                )
                .await?;
            let detailed = Some(message.chat.id) == context.admin_chat_id;
            Ok(simple_reply(format!(
                "{subscription}\n\n{}",
                render_operational_health(&health, detailed)
            )))
        }
        InteractionCommand::Stop => {
            store
                .deactivate_subscriber(message.chat.id, USER_STOP_REASON)
                .await?;
            Ok(simple_reply(
                "Đã tắt thông báo hoạt động. Thông báo bắt buộc từ Portal UTH vẫn được gửi. Khi cần, bạn chỉ việc chọn Bật thông báo để nhận lại tin hoạt động.".to_owned(),
            ))
        }
        InteractionCommand::Pages(page) => {
            let sources = store.list_enabled_sources().await?;
            Ok(simple_reply(render_source_page(&sources, page)))
        }
        InteractionCommand::Suggest(None) | InteractionCommand::Contact => Ok(simple_reply(
            "Muốn đề xuất thêm trang, bạn gửi theo mẫu này:\n/suggest https://www.facebook.com/ten.trang\n\nQuản trị viên sẽ kiểm tra trước khi thêm."
                .to_owned(),
        )),
        InteractionCommand::Suggest(Some(value)) => {
            let Some(url) = normalize_facebook_page_url(&value) else {
                return Ok(simple_reply(
                    "Link này chưa đúng. Bạn hãy gửi link của một trang Facebook công khai."
                        .to_owned(),
                ));
            };
            let outcome = store
                .submit_source_suggestion(message.chat.id, &url)
                .await?;
            if outcome.created {
                let sender = telegram_display_name(&message.chat)
                    .unwrap_or_else(|| format!("Người dùng {}", message.chat.id));
                Ok(InteractionReply {
                    user_text: "Mình đã nhận đề xuất. Quản trị viên sẽ kiểm tra trước khi thêm trang."
                        .to_owned(),
                    user_links: Vec::new(),
                    photo_png: None,
                    portal_document: None,
                    portal_source_url: None,
                    donation_prompt: false,
                    onboarding_prompt: false,
                    settings_prompt: false,
                    complete_donation_amount_input: false,
                    admin_text: context.admin_chat_id.map(|_| {
                        format!(
                            "Có đề xuất trang mới\nMã: #{}\nNgười gửi: {}\nLink: {}\n\nDuyệt: /approve {} Tên trang\nTừ chối: /reject {} Lý do",
                            outcome.id, sender, url, outcome.id, outcome.id
                        )
                    }),
                    suggestion_created: true,
                })
            } else {
                Ok(simple_reply(
                    "Trang này đã được đề xuất và đang chờ quản trị viên xem xét.".to_owned(),
                ))
            }
        }
        InteractionCommand::Pending => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(simple_reply(
                    "Bạn chọn Đề xuất trang nếu muốn gửi thêm một nguồn mới.".to_owned(),
                ));
            }
            let suggestions = store.list_source_suggestions(Some("pending")).await?;
            let body = if suggestions.is_empty() {
                "Hiện không có đề xuất nào đang chờ xét.".to_owned()
            } else {
                let entries = suggestions
                    .iter()
                    .take(10)
                    .map(|suggestion| format!("#{} - {}", suggestion.id, suggestion.submitted_url))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("Các đề xuất đang chờ xét:\n{entries}")
            };
            Ok(simple_reply(body))
        }
        InteractionCommand::Approve { id, name } => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(simple_reply(
                    "Lệnh này chỉ dành cho quản trị viên.".to_owned(),
                ));
            }
            let changed = store.approve_source_suggestion(id, &name).await?;
            Ok(simple_reply(if changed {
                format!("Đã duyệt đề xuất #{id} và thêm trang vào danh sách theo dõi.")
            } else {
                format!("Không tìm thấy đề xuất #{id} đang chờ xét.")
            }))
        }
        InteractionCommand::Reject { id, reason } => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(simple_reply(
                    "Lệnh này chỉ dành cho quản trị viên.".to_owned(),
                ));
            }
            let changed = store.reject_source_suggestion(id, &reason).await?;
            Ok(simple_reply(if changed {
                format!("Đã từ chối đề xuất #{id}.")
            } else {
                format!("Không tìm thấy đề xuất #{id} đang chờ xét.")
            }))
        }
        InteractionCommand::Reviews(page) => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(admin_only_reply());
            }
            let page_size = 5_usize;
            let offset = page.saturating_sub(1).saturating_mul(page_size);
            let reviews = store
                .pending_manual_reviews(i64::try_from(page_size + 1)?, i64::try_from(offset)?)
                .await?;
            Ok(simple_reply(render_manual_review_page(
                &reviews, page, page_size,
            )))
        }
        InteractionCommand::Review(id) => {
            if Some(message.chat.id) != context.admin_chat_id {
                return Ok(admin_only_reply());
            }
            let body = match store.manual_review(id).await? {
                Some(review) => render_manual_review_detail(&review),
                None => format!("Không tìm thấy bài #{id} đang chờ duyệt."),
            };
            Ok(simple_reply(body))
        }
        InteractionCommand::ReviewSend(id) => {
            let Some(authorized_admin_chat_id) =
                context.admin_chat_id.filter(|admin| *admin == message.chat.id)
            else {
                return Ok(admin_only_reply());
            };
            let Some(review) = store.manual_review(id).await? else {
                return Ok(simple_reply(format!(
                    "Không tìm thấy bài #{id} đang chờ duyệt. Bài có thể đã được xử lý."
                )));
            };
            let notification = render_notification(&review.post);
            let post_url = delivery_post_url(&review.post);
            let outcome = store
                .resolve_manual_review(
                    id,
                    message.chat.id,
                    authorized_admin_chat_id,
                    ManualReviewAction::Send,
                    None,
                    Some(ManualReviewNotification {
                        message_text: &notification,
                        post_url: &post_url,
                    }),
                )
                .await?;
            Ok(simple_reply(if outcome.campaign_created {
                format!(
                    "Đã duyệt bài #{id}. Đã tạo {} lượt gửi cho người nhận đang hoạt động.",
                    outcome.deliveries_created
                )
            } else if outcome.resolved {
                format!("Bài #{id} đã được gửi trước đó, không tạo lượt gửi mới.")
            } else {
                format!("Bài #{id} đã được xử lý trước đó, không tạo lượt gửi trùng.")
            }))
        }
        InteractionCommand::ReviewSkip { id, reason } => {
            let Some(authorized_admin_chat_id) =
                context.admin_chat_id.filter(|admin| *admin == message.chat.id)
            else {
                return Ok(admin_only_reply());
            };
            if store.manual_review(id).await?.is_none() {
                return Ok(simple_reply(format!(
                    "Không tìm thấy bài #{id} đang chờ duyệt. Bài có thể đã được xử lý."
                )));
            }
            let outcome = store
                .resolve_manual_review(
                    id,
                    message.chat.id,
                    authorized_admin_chat_id,
                    ManualReviewAction::Skip,
                    reason.as_deref(),
                    None,
                )
                .await?;
            Ok(simple_reply(if outcome.resolved {
                format!("Đã bỏ qua bài #{id}. Không có thông báo nào được tạo.")
            } else {
                format!("Bài #{id} đã được xử lý trước đó.")
            }))
        }
        InteractionCommand::Latest(page) => {
            let page_size = 5_usize;
            let offset = page.saturating_sub(1).saturating_mul(page_size);
            let posts = store
                .latest_posts(i64::try_from(page_size + 1)?, i64::try_from(offset)?)
                .await?;
            Ok(simple_reply(render_latest_post_page(
                &posts, page, page_size,
            )))
        }
        InteractionCommand::LatestPost(id) => {
            let body = match store.latest_post(id).await? {
                Some(post) => render_latest_post_detail(&post),
                None => format!("Không tìm thấy bài #{id} trong các nguồn đang bật."),
            };
            Ok(simple_reply(body))
        }
        InteractionCommand::Usage(message) => Ok(simple_reply(message.to_owned())),
        InteractionCommand::Unknown => Ok(simple_reply(
            "Mình chưa hiểu. Bạn chọn một nút bên dưới hoặc gửi /help nhé.".to_owned(),
        )),
    }
}

fn admin_only_reply() -> InteractionReply {
    simple_reply("Lệnh này chỉ dành cho quản trị viên.".to_owned())
}

fn render_manual_review_page(
    reviews: &[ManualReviewRecord],
    requested_page: usize,
    page_size: usize,
) -> String {
    if reviews.is_empty() {
        return "Hiện không có bài nào đang chờ duyệt.".to_owned();
    }
    let has_next = reviews.len() > page_size;
    let entries = reviews
        .iter()
        .take(page_size)
        .map(|review| {
            format!(
                "#{} - {}\n{}\nXem: /review_{}",
                review.classification_id,
                review.source_name,
                shorten_text(review.post.text.trim(), 160),
                review.classification_id
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let navigation = if has_next {
        format!("\n\nTrang sau: /reviews_{}", requested_page + 1)
    } else if requested_page > 1 {
        format!("\n\nTrang trước: /reviews_{}", requested_page - 1)
    } else {
        String::new()
    };
    format!("Bài đang chờ duyệt - trang {requested_page}\n\n{entries}{navigation}")
}

fn render_manual_review_detail(review: &ManualReviewRecord) -> String {
    let rules = if review.matched_rules.is_empty() {
        "không có".to_owned()
    } else {
        review
            .matched_rules
            .iter()
            .map(|rule| display_classification_signal(rule))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "BÀI CẦN DUYỆT #{}\nNguồn: {}\nĐăng lúc: {}\nMức phù hợp: {}\nĐộ chắc chắn: {:.2}%\nDấu hiệu nhận biết: {}\n\n{}\n\n{}\n\nGửi: /review_send_{}\nBỏ qua: /review_skip_{}",
        review.classification_id,
        review.source_name,
        format_vietnam_datetime(&review.post.published_at),
        review.score,
        f64::from(review.confidence_basis_points) / 100.0,
        rules,
        shorten_text(review.post.text.trim(), 2_000),
        delivery_post_url(&review.post),
        review.classification_id,
        review.classification_id
    )
}

fn render_crawl_history_page(
    runs: &[CrawlHistoryRecord],
    requested_page: usize,
    page_size: usize,
) -> String {
    if runs.is_empty() {
        return "Chưa có lịch sử crawl.".to_owned();
    }
    let has_next = runs.len() > page_size;
    let entries = runs
        .iter()
        .take(page_size)
        .map(|run| {
            let strategy = run.selected_strategy.as_deref().unwrap_or("không xác định");
            let error = run
                .error
                .as_deref()
                .map(|value| format!("\nLỗi: {}", shorten_text(value.trim(), 180)))
                .unwrap_or_default();
            format!(
                "#{} - {}\nThời gian: {}\nKết quả: {}\nChiến lược: {}\nBài tìm thấy: {} | Lượt thử: {}\nChi tiết: /crawl_run_{}{}",
                run.run_id,
                shorten_text(run.source_name.trim(), 120),
                format_history_datetime(&run.fetched_at),
                display_crawl_health(&run.health),
                strategy,
                run.post_count,
                run.attempt_count,
                run.run_id,
                error
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let navigation = if has_next {
        format!("\n\nTrang sau: /crawl_history_{}", requested_page + 1)
    } else if requested_page > 1 {
        format!("\n\nTrang trước: /crawl_history_{}", requested_page - 1)
    } else {
        String::new()
    };
    format!("Lịch sử crawl - trang {requested_page}\n\n{entries}{navigation}")
}

fn render_user_feedback_page(
    feedback: &[UserFeedbackHistoryRecord],
    total: i64,
    requested_page: usize,
    page_size: usize,
) -> UserFeedbackPage {
    if total == 0 {
        return UserFeedbackPage {
            text: "Chưa có feedback nào được gửi.".to_owned(),
            user_links: Vec::new(),
        };
    }
    let mut text = format!("Toàn bộ feedback đã gửi: {total}\nTrang {requested_page}");
    let mut user_links = Vec::with_capacity(feedback.len());
    for item in feedback {
        let status = if item.admin_notified_at.is_some() {
            "đã chuyển cho admin"
        } else {
            "đang chờ chuyển cho admin"
        };
        text.push_str(&format!(
            "\n\n#{} - {}\nChat ID: {}\nGửi lúc: {}\nTrạng thái: {}\n\n{}\n",
            item.id,
            shorten_text(item.sender_label.trim(), 120),
            item.telegram_chat_id,
            format_history_datetime(&item.created_at),
            status,
            shorten_text(item.message.trim(), 400)
        ));
        let contact_label = format!("Mở cuộc trò chuyện với người gửi #{}", item.id);
        let offset = text.encode_utf16().count();
        text.push_str(&contact_label);
        user_links.push(TelegramUserLink {
            offset,
            length: contact_label.encode_utf16().count(),
            user_chat_id: item.telegram_chat_id,
        });
    }
    let has_next =
        requested_page.saturating_mul(page_size) < usize::try_from(total).unwrap_or(usize::MAX);
    if has_next {
        text.push_str(&format!("\n\nTrang sau: /feedbacks_{}", requested_page + 1));
    } else if requested_page > 1 {
        text.push_str(&format!(
            "\n\nTrang trước: /feedbacks_{}",
            requested_page - 1
        ));
    }
    UserFeedbackPage { text, user_links }
}

fn render_crawl_history_detail(detail: &CrawlHistoryDetail) -> String {
    let run = &detail.run;
    let strategy = run.selected_strategy.as_deref().unwrap_or("không xác định");
    let error = run
        .error
        .as_deref()
        .map(|value| format!("\nLỗi chung: {}", shorten_text(value.trim(), 260)))
        .unwrap_or_default();
    let attempts = if detail.attempts.is_empty() {
        "\n\nKhông có thông tin lượt thử chi tiết.".to_owned()
    } else {
        detail
            .attempts
            .iter()
            .map(|attempt| {
                let status = attempt
                    .status
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned());
                let error = attempt
                    .error
                    .as_deref()
                    .map(|value| format!("; lỗi: {}", shorten_text(value.trim(), 180)))
                    .unwrap_or_default();
                let browser = attempt
                    .browser
                    .as_ref()
                    .map(|metadata| {
                        let mut fallback = metadata
                            .network_fallback_reason
                            .as_deref()
                            .map(|value| {
                                format!(" | fallback: {}", shorten_text(value.trim(), 120))
                            })
                            .unwrap_or_default();
                        let origin = metadata
                            .discovered_post_origin
                            .as_deref()
                            .unwrap_or("-");
                        fallback.push_str(&format!(
                            " | bài chọn: {} | DOM chưa chuẩn hóa: {}",
                            origin,
                            if metadata.newest_dom_post_unresolved {
                                "có"
                            } else {
                                "không"
                            }
                        ));
                        format!(
                            "Mạng browser: yêu cầu {} | thực tế {} | đích {} | login route: {} | hộp thoại: {}/{}{}",
                            metadata.network_requested_mode,
                            metadata.network_effective_mode,
                            metadata.network_remote_family,
                            if metadata.login_route_detected { "có" } else { "không" },
                            if metadata.login_overlay_detected { "có" } else { "không" },
                            if metadata.login_overlay_dismissed { "đã đóng" } else { "chưa đóng" },
                            fallback
                        )
                    })
                    .unwrap_or_default();
                let error = if browser.is_empty() {
                    error
                } else if error.is_empty() {
                    format!("; {}", browser)
                } else {
                    format!("{}; {}", error, browser)
                };
                format!(
                    "\nLượt {} - {}\nKết quả: {} | HTTP: {} | Bài: {} | {} ms | {} bytes{}",
                    attempt.ordinal + 1,
                    attempt.strategy,
                    display_crawl_health(&attempt.outcome),
                    status,
                    attempt.posts_found,
                    attempt.latency_ms,
                    attempt.bytes_received,
                    error
                )
            })
            .collect::<String>()
    };
    let rendered = format!(
        "LẦN CRAWL #{}\nNguồn: {} ({})\nThời gian: {}\nKết quả: {}\nChiến lược được chọn: {}\nBài tìm thấy: {}\nSố lượt thử: {}{}\n\nChi tiết lượt thử:{}",
        run.run_id,
        shorten_text(run.source_name.trim(), 120),
        shorten_text(run.source_key.trim(), 120),
        format_history_datetime(&run.fetched_at),
        display_crawl_health(&run.health),
        strategy,
        run.post_count,
        run.attempt_count,
        error,
        attempts
    );
    shorten_text(&rendered, 4_000)
}

fn display_crawl_health(value: &str) -> &str {
    match value {
        "healthy" => "thành công",
        "degraded" => "suy giảm",
        "failed" => "lỗi",
        other => other,
    }
}

fn format_history_datetime(value: &DateTime<Utc>) -> String {
    format_vietnam_datetime(&value.to_rfc3339())
}

fn render_portal_notice_history_page(
    notices: &[PortalNoticeHistoryRecord],
    requested_page: usize,
    page_size: usize,
) -> String {
    if notices.is_empty() {
        return "Chưa có lịch sử crawl thông báo Portal.".to_owned();
    }
    let has_next = notices.len() > page_size;
    let entries = notices
        .iter()
        .take(page_size)
        .map(|notice| {
            let attachment = if notice.attachment_url.is_some() {
                "\nTệp đính kèm: có"
            } else {
                ""
            };
            format!(
                "#{} - {}\n{}{}\nChi tiết: /portal_notice_{}",
                notice.portal_id,
                format_vietnam_datetime(&notice.displayed_at.to_rfc3339()),
                shorten_text(notice.title.trim(), 180),
                attachment,
                notice.portal_id
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let navigation = if has_next {
        format!("\n\nTrang sau: /portal_history_{}", requested_page + 1)
    } else if requested_page > 1 {
        format!("\n\nTrang trước: /portal_history_{}", requested_page - 1)
    } else {
        String::new()
    };
    format!("Lịch sử crawl Portal - trang {requested_page}\n\n{entries}{navigation}")
}

fn render_portal_notice_history_detail(notice: &PortalNoticeHistoryRecord) -> String {
    let source = notice
        .article_url
        .as_deref()
        .or(notice.attachment_url.as_deref())
        .map(|url| shorten_text(url, 280))
        .unwrap_or_else(|| "không có".to_owned());
    let attachment = if notice.attachment_url.is_some() {
        "có, bot gửi kèm ngay sau khi tải"
    } else {
        "không có"
    };
    format!(
        "THÔNG BÁO PORTAL #{}\nTiêu đề: {}\nNgày đăng: {}\nĐã crawl: {}\nTệp đính kèm: {}\nThông báo gốc: {}",
        notice.portal_id,
        shorten_text(notice.title.trim(), 500),
        format_vietnam_datetime(&notice.displayed_at.to_rfc3339()),
        format_vietnam_datetime(&notice.discovered_at.to_rfc3339()),
        attachment,
        source
    )
}

fn render_latest_post_page(
    posts: &[LatestPostRecord],
    requested_page: usize,
    page_size: usize,
) -> String {
    if posts.is_empty() {
        return "Hiện chưa có bài nào đã được lưu.".to_owned();
    }
    let has_next = posts.len() > page_size;
    let entries = posts
        .iter()
        .take(page_size)
        .map(|record| {
            format!(
                "#{} - {}\n{}\n{}\nChi tiết: /latest_post_{}",
                record.database_post_id,
                record.source_name,
                format_vietnam_datetime(&record.post.published_at),
                shorten_text(record.post.text.trim(), 180),
                record.database_post_id
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let navigation = if has_next {
        format!("\n\nTrang sau: /latest_{}", requested_page + 1)
    } else if requested_page > 1 {
        format!("\n\nTrang trước: /latest_{}", requested_page - 1)
    } else {
        String::new()
    };
    format!("Bài mới nhất - trang {requested_page}\n\n{entries}{navigation}")
}

fn render_latest_post_detail(record: &LatestPostRecord) -> String {
    format!(
        "BÀI #{}\nNguồn: {}\nĐăng lúc: {}\nKiểm tra gần nhất: {}\n\n{}\n\n{}",
        record.database_post_id,
        record.source_name,
        format_vietnam_datetime(&record.post.published_at),
        format_vietnam_datetime(&record.post.fetched_at),
        shorten_text(record.post.text.trim(), 3_000),
        delivery_post_url(&record.post)
    )
}

fn format_vietnam_datetime(value: &str) -> String {
    let Some(offset) = FixedOffset::east_opt(7 * 60 * 60) else {
        return value.to_owned();
    };
    DateTime::parse_from_rfc3339(value)
        .map(|date| {
            date.with_timezone(&offset)
                .format("%H:%M ngày %d/%m/%Y")
                .to_string()
        })
        .unwrap_or_else(|_| value.to_owned())
}

fn display_classification_signal(rule: &str) -> &str {
    match rule {
        "feature.explicit_drl" => "có nhắc điểm rèn luyện",
        "feature.registration_call" => "có lời mời đăng ký",
        "feature.form_link" => "có biểu mẫu đăng ký",
        "feature.future_event_time" => "có thời gian diễn ra sắp tới",
        "feature.future_deadline" => "có hạn đăng ký sắp tới",
        "feature.location" => "có địa điểm",
        "feature.target_students" => "hướng tới sinh viên",
        "feature.approved_source" => "nguồn đã được duyệt",
        "feature.negative_commercial" => "có dấu hiệu thương mại",
        "feature.past_event" => "có thể là hoạt động đã qua",
        "hard.deadline_passed" => "hạn đăng ký đã qua",
        "hard.completed_summary" => "bài tổng kết hoạt động đã diễn ra",
        "hard.post_too_old" => "bài đăng đã quá cũ",
        "hard.unapproved_source" => "nguồn chưa được duyệt",
        "decision.insufficient_evidence" => "chưa đủ thông tin để tự động gửi",
        "decision.no_actionable_evidence" => "không thấy lời mời tham gia rõ ràng",
        _ => "dấu hiệu khác",
    }
}

fn shorten_text(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    let mut output = value
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>();
    output.push('…');
    output
}

fn render_help(is_admin: bool) -> String {
    let public_history = "\n\nLịch sử công khai:\n- /latest: xem các bài Facebook mới thu thập.\n- /portal_history: xem thông báo Portal đã crawl.\n- /portal_notice_ID: mở một thông báo Portal và nhận tệp đính kèm nếu có.";
    let admin = if is_admin {
        "\n\nBạn là quản trị viên. Chọn /admin để mở các công cụ quản trị."
    } else {
        ""
    };
    with_support_group(&format!(
        "UTH Notifier báo cho bạn khi tìm thấy hoạt động phù hợp từ các trang công khai đang theo dõi. Bot cũng gửi mọi thông báo mới từ Portal UTH và gửi kèm tệp khi Portal có đính kèm. Tin Portal là bắt buộc, không phụ thuộc loại hoạt động, cách nhận, giờ yên lặng hoặc trạng thái tạm dừng.\n\nTrong Cài đặt:\n- Loại hoạt động: chỉ tin có điểm rèn luyện hoặc mọi hoạt động phù hợp.\n- Nhận ngay: bot gửi từng tin khi phát hiện.\n- Bản tin lúc 07:30: bot gom tin mới và gửi một lần mỗi ngày. Ngày không có tin phù hợp, bot sẽ không gửi.\n- Giờ yên lặng: nếu bật, tin hoạt động cần gửi ngay từ 22:00 đến 07:00 sẽ được giữ lại đến sau 07:00.\n- Tạm dừng tin hoạt động: ngừng nhận tin hoạt động cho tới khi bạn bật lại.\n- Xem một tin mẫu: xem trước dạng thông báo hoạt động bot sẽ gửi.\n\nCác nút chính:\n- Trang đang theo dõi: xem các nguồn bot đang kiểm tra.\n- Đề xuất trang: gửi link Facebook công khai để quản trị viên xem xét.\n- Gửi phản hồi: gửi góp ý trực tiếp cho quản trị viên.\n- Cài đặt: xem và thay đổi cách nhận thông báo hoạt động.\n- Trợ giúp: mở lại hướng dẫn này.\n- Ủng hộ: chủ động hỗ trợ chi phí vận hành; việc ủng hộ hoàn toàn tự nguyện và không ảnh hưởng quyền dùng bot.{public_history}{admin}"
    ))
}

fn with_support_group(text: &str) -> String {
    format!("{text}\n\n{SUPPORT_GROUP_INVITATION}")
}

fn simple_reply(user_text: String) -> InteractionReply {
    InteractionReply {
        user_text,
        user_links: Vec::new(),
        photo_png: None,
        portal_document: None,
        portal_source_url: None,
        donation_prompt: false,
        onboarding_prompt: false,
        settings_prompt: false,
        complete_donation_amount_input: false,
        admin_text: None,
        suggestion_created: false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InteractionCommand {
    Start(Option<String>),
    OnboardScope(&'static str),
    OnboardSample,
    Settings,
    SettingsSample,
    SettingsScope(&'static str),
    SettingsMode(&'static str),
    SettingsQuiet(bool),
    UserFeedbackPrompt,
    UserFeedback(String),
    Feedback {
        campaign_id: i64,
        value: &'static str,
    },
    OpenAction(i64),
    Metrics,
    FeedbackHistory(usize),
    Admin,
    Help,
    Status,
    Stop,
    Pages(usize),
    Suggest(Option<String>),
    Contact,
    Donate,
    PromptDonationAmount,
    CustomDonationAmount(i64),
    InvalidDonationAmount,
    Cancel,
    DeclineDonation,
    DonateAmount(i64),
    CrawlHistory(usize),
    CrawlRun(i64),
    PortalHistory(usize),
    PortalNotice(i64),
    Pending,
    Approve {
        id: i64,
        name: String,
    },
    Reject {
        id: i64,
        reason: String,
    },
    Reviews(usize),
    Review(i64),
    ReviewSend(i64),
    ReviewSkip {
        id: i64,
        reason: Option<String>,
    },
    Latest(usize),
    LatestPost(i64),
    Usage(&'static str),
    Unknown,
}

async fn resolve_interaction_command(
    store: &CrawlStore,
    telegram_chat_id: i64,
    text: &str,
) -> Result<InteractionCommand> {
    let command = parse_interaction_command(text);
    if command != InteractionCommand::Unknown {
        if !matches!(command, InteractionCommand::PromptDonationAmount) {
            store.clear_donation_amount_input(telegram_chat_id).await?;
        }
        if !matches!(
            command,
            InteractionCommand::UserFeedbackPrompt | InteractionCommand::Cancel
        ) {
            store.clear_user_feedback_input(telegram_chat_id).await?;
        }
        return Ok(command);
    }
    if store.user_feedback_input_active(telegram_chat_id).await? {
        return Ok(InteractionCommand::UserFeedback(text.trim().to_owned()));
    }
    if !store.donation_amount_input_active(telegram_chat_id).await? {
        return Ok(InteractionCommand::Unknown);
    }
    Ok(parse_donation_amount(text)
        .map(InteractionCommand::CustomDonationAmount)
        .unwrap_or(InteractionCommand::InvalidDonationAmount))
}

fn parse_interaction_command(text: &str) -> InteractionCommand {
    let trimmed = text.trim();
    let normalized = trimmed.to_lowercase();
    let mut parts = trimmed.split_whitespace();
    let command = parts
        .next()
        .unwrap_or_default()
        .split('@')
        .next()
        .unwrap_or_default()
        .to_lowercase();
    match normalized.as_str() {
        "bật thông báo" => InteractionCommand::Start(None),
        "trợ giúp" => InteractionCommand::Help,
        "trạng thái" => InteractionCommand::Status,
        "tắt thông báo" => InteractionCommand::Stop,
        "trang đang theo dõi" => InteractionCommand::Pages(1),
        "đề xuất trang" => InteractionCommand::Suggest(None),
        "liên hệ quản trị" => InteractionCommand::Contact,
        "gửi phản hồi" => InteractionCommand::UserFeedbackPrompt,
        "ủng hộ" => InteractionCommand::Donate,
        "cài đặt" => InteractionCommand::Settings,
        "10.000 vnd" => InteractionCommand::DonateAmount(10_000),
        "20.000 vnd" => InteractionCommand::DonateAmount(20_000),
        "50.000 vnd" => InteractionCommand::DonateAmount(50_000),
        "tùy tâm" => InteractionCommand::PromptDonationAmount,
        "để sau" => InteractionCommand::DeclineDonation,
        _ => match command.as_str() {
            "/start" => InteractionCommand::Start(parts.next().and_then(valid_start_parameter)),
            "/settings" => InteractionCommand::Settings,
            "/settings_sample" => InteractionCommand::SettingsSample,
            "/onboard_drl" => InteractionCommand::OnboardScope("drl"),
            "/onboard_all" => InteractionCommand::OnboardScope("all"),
            "/onboard_sample" => InteractionCommand::OnboardSample,
            "/settings_scope_drl" => InteractionCommand::SettingsScope("drl"),
            "/settings_scope_all" => InteractionCommand::SettingsScope("all"),
            "/settings_mode_instant" => InteractionCommand::SettingsMode("instant"),
            "/settings_mode_daily" => InteractionCommand::SettingsMode("daily"),
            "/settings_quiet_on" => InteractionCommand::SettingsQuiet(true),
            "/settings_quiet_off" => InteractionCommand::SettingsQuiet(false),
            value if value.starts_with("/useful_") => {
                parse_positive_command_suffix(value, "/useful_")
                    .map(|campaign_id| InteractionCommand::Feedback {
                        campaign_id,
                        value: "useful",
                    })
                    .unwrap_or(InteractionCommand::Unknown)
            }
            value if value.starts_with("/irrelevant_") => {
                parse_positive_command_suffix(value, "/irrelevant_")
                    .map(|campaign_id| InteractionCommand::Feedback {
                        campaign_id,
                        value: "irrelevant",
                    })
                    .unwrap_or(InteractionCommand::Unknown)
            }
            value if value.starts_with("/open_") => parse_positive_command_suffix(value, "/open_")
                .map(InteractionCommand::OpenAction)
                .unwrap_or(InteractionCommand::Unknown),
            "/metrics" => InteractionCommand::Metrics,
            "/feedbacks" => InteractionCommand::FeedbackHistory(
                parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1)
                    .max(1),
            ),
            value if value.starts_with("/feedbacks_") => InteractionCommand::FeedbackHistory(
                value
                    .strip_prefix("/feedbacks_")
                    .and_then(|page| page.parse::<usize>().ok())
                    .unwrap_or(1)
                    .max(1),
            ),
            "/crawl_history" => InteractionCommand::CrawlHistory(
                parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1)
                    .max(1),
            ),
            value if value.starts_with("/crawl_history_") => InteractionCommand::CrawlHistory(
                value
                    .strip_prefix("/crawl_history_")
                    .and_then(|page| page.parse::<usize>().ok())
                    .unwrap_or(1)
                    .max(1),
            ),
            "/crawl_run" => parse_positive_id(parts.next())
                .map(InteractionCommand::CrawlRun)
                .unwrap_or(InteractionCommand::Usage(
                    "Thiếu mã lần crawl. Hãy dùng /crawl_run_ID.",
                )),
            value if value.starts_with("/crawl_run_") => value
                .strip_prefix("/crawl_run_")
                .and_then(|id| id.parse::<i64>().ok())
                .filter(|id| *id > 0)
                .map(InteractionCommand::CrawlRun)
                .unwrap_or(InteractionCommand::Usage(
                    "Mã lần crawl không hợp lệ. Hãy dùng /crawl_history.",
                )),
            "/portal_history" => InteractionCommand::PortalHistory(
                parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|page| *page > 0)
                    .unwrap_or(1),
            ),
            "/portal_notice" => parts
                .next()
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|id| *id > 0)
                .map(InteractionCommand::PortalNotice)
                .unwrap_or(InteractionCommand::Usage(
                    "Thiếu mã Portal. Hãy dùng /portal_notice_ID.",
                )),
            value if value.starts_with("/portal_history_") => InteractionCommand::PortalHistory(
                value
                    .strip_prefix("/portal_history_")
                    .and_then(|page| page.parse::<usize>().ok())
                    .filter(|page| *page > 0)
                    .unwrap_or(1),
            ),
            value if value.starts_with("/portal_notice_") => value
                .strip_prefix("/portal_notice_")
                .and_then(|id| id.parse::<i64>().ok())
                .filter(|id| *id > 0)
                .map(InteractionCommand::PortalNotice)
                .unwrap_or(InteractionCommand::Usage(
                    "Mã Portal không hợp lệ. Hãy dùng /portal_notice_ID.",
                )),
            "/admin" => InteractionCommand::Admin,
            "/help" => InteractionCommand::Help,
            "/status" => InteractionCommand::Status,
            "/stop" => InteractionCommand::Stop,
            "/pages" => InteractionCommand::Pages(
                parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1)
                    .max(1),
            ),
            value if value.starts_with("/pages_") => InteractionCommand::Pages(
                value
                    .trim_start_matches("/pages_")
                    .parse::<usize>()
                    .unwrap_or(1)
                    .max(1),
            ),
            "/suggest" => InteractionCommand::Suggest(parts.next().map(str::to_owned)),
            "/contact" => InteractionCommand::Contact,
            "/feedback" => {
                let message = parts.collect::<Vec<_>>().join(" ");
                if message.trim().is_empty() {
                    InteractionCommand::UserFeedbackPrompt
                } else {
                    InteractionCommand::UserFeedback(message)
                }
            }
            "/cancel" => InteractionCommand::Cancel,
            "/donate" => match parts.next() {
                None => InteractionCommand::Donate,
                Some(value) => parse_donation_amount(value)
                    .map(InteractionCommand::DonateAmount)
                    .unwrap_or(InteractionCommand::Usage(
                        "Số tiền phải từ 10.000 đến 10.000.000 VND.",
                    )),
            },
            "/donate_custom" => InteractionCommand::PromptDonationAmount,
            "/donate_later" => InteractionCommand::DeclineDonation,
            value if value.starts_with("/donate_") => value
                .strip_prefix("/donate_")
                .and_then(parse_donation_amount)
                .map(InteractionCommand::DonateAmount)
                .unwrap_or(InteractionCommand::Usage(
                    "Số tiền phải từ 10.000 đến 10.000.000 VND.",
                )),
            "/pending" => InteractionCommand::Pending,
            "/approve" => parse_review_command(parts, true),
            "/reject" => parse_review_command(parts, false),
            "/reviews" => InteractionCommand::Reviews(
                parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1)
                    .max(1),
            ),
            value if value.starts_with("/reviews_") => InteractionCommand::Reviews(
                value
                    .trim_start_matches("/reviews_")
                    .parse::<usize>()
                    .unwrap_or(1)
                    .max(1),
            ),
            "/review" => parse_positive_id(parts.next())
                .map(InteractionCommand::Review)
                .unwrap_or(InteractionCommand::Usage(
                    "Thiếu mã bài. Dùng /reviews để xem danh sách bài cần duyệt.",
                )),
            value if value.starts_with("/review_send_") => {
                parse_positive_command_suffix(value, "/review_send_")
                    .map(InteractionCommand::ReviewSend)
                    .unwrap_or(InteractionCommand::Usage(
                        "Mã bài không hợp lệ. Dùng /reviews để xem lại danh sách.",
                    ))
            }
            value if value.starts_with("/review_skip_") => {
                parse_positive_command_suffix(value, "/review_skip_")
                    .map(|id| InteractionCommand::ReviewSkip { id, reason: None })
                    .unwrap_or(InteractionCommand::Usage(
                        "Mã bài không hợp lệ. Dùng /reviews để xem lại danh sách.",
                    ))
            }
            "/review_send" => parse_positive_id(parts.next())
                .map(InteractionCommand::ReviewSend)
                .unwrap_or(InteractionCommand::Usage(
                    "Thiếu mã bài. Hãy bấm lệnh /review_send_ID trong tin chi tiết.",
                )),
            "/review_skip" => {
                let Some(id) = parse_positive_id(parts.next()) else {
                    return InteractionCommand::Usage(
                        "Thiếu mã bài. Hãy bấm lệnh /review_skip_ID trong tin chi tiết.",
                    );
                };
                let reason = parts.collect::<Vec<_>>().join(" ");
                InteractionCommand::ReviewSkip {
                    id,
                    reason: (!reason.is_empty()).then_some(reason),
                }
            }
            value if value.starts_with("/review_") => {
                parse_positive_command_suffix(value, "/review_")
                    .map(InteractionCommand::Review)
                    .unwrap_or(InteractionCommand::Usage(
                        "Mã bài không hợp lệ. Dùng /reviews để xem lại danh sách.",
                    ))
            }
            "/latest" => InteractionCommand::Latest(
                parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1)
                    .max(1),
            ),
            value if value.starts_with("/latest_post_") => {
                parse_positive_command_suffix(value, "/latest_post_")
                    .map(InteractionCommand::LatestPost)
                    .unwrap_or(InteractionCommand::Usage(
                        "Mã bài không hợp lệ. Dùng /latest để xem lại danh sách.",
                    ))
            }
            "/latest_post" => parse_positive_id(parts.next())
                .map(InteractionCommand::LatestPost)
                .unwrap_or(InteractionCommand::Usage(
                    "Thiếu mã bài. Dùng /latest để xem danh sách bài.",
                )),
            value if value.starts_with("/latest_") => InteractionCommand::Latest(
                value
                    .trim_start_matches("/latest_")
                    .parse::<usize>()
                    .unwrap_or(1)
                    .max(1),
            ),
            _ if trimmed.starts_with("http://") || trimmed.starts_with("https://") => {
                InteractionCommand::Suggest(Some(trimmed.to_owned()))
            }
            _ => InteractionCommand::Unknown,
        },
    }
}

fn parse_positive_id(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|id| *id > 0)
}

fn valid_start_parameter(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')))
    .then(|| value.to_owned())
}

fn parse_positive_command_suffix(value: &str, prefix: &str) -> Option<i64> {
    value
        .strip_prefix(prefix)
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|id| *id > 0)
}

fn parse_donation_amount(value: &str) -> Option<i64> {
    if value.chars().count() > 32 {
        return None;
    }
    let compact = value
        .trim()
        .to_lowercase()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    let normalized = ["vnd", "đồng", "đ", "d"]
        .into_iter()
        .find_map(|suffix| compact.strip_suffix(suffix))
        .unwrap_or(compact.as_str());
    let (digits, multiplier) = normalized
        .strip_suffix('k')
        .map(|value| (value, 1_000_i64))
        .unwrap_or((normalized, 1));
    let digits = if multiplier == 1_000 {
        (!digits.is_empty() && digits.chars().all(|character| character.is_ascii_digit()))
            .then(|| digits.to_owned())?
    } else {
        normalize_grouped_amount_digits(digits)?
    };
    digits
        .parse::<i64>()
        .ok()
        .and_then(|amount| amount.checked_mul(multiplier))
        .filter(|amount| (10_000..=10_000_000).contains(amount))
}

fn normalize_grouped_amount_digits(value: &str) -> Option<String> {
    if !value.is_empty() && value.chars().all(|character| character.is_ascii_digit()) {
        return Some(value.to_owned());
    }
    let separator = ['.', ',', '_']
        .into_iter()
        .find(|separator| value.contains(*separator))?;
    if ['.', ',', '_']
        .into_iter()
        .any(|candidate| candidate != separator && value.contains(candidate))
    {
        return None;
    }
    let groups = value.split(separator).collect::<Vec<_>>();
    if groups.len() < 2
        || !(1..=3).contains(&groups[0].len())
        || !groups.iter().all(|group| {
            !group.is_empty() && group.chars().all(|character| character.is_ascii_digit())
        })
        || groups.iter().skip(1).any(|group| group.len() != 3)
    {
        return None;
    }
    Some(groups.concat())
}

fn parse_review_command<'a>(
    mut parts: impl Iterator<Item = &'a str>,
    approve: bool,
) -> InteractionCommand {
    let Some(id) = parts.next().and_then(|value| value.parse::<i64>().ok()) else {
        return InteractionCommand::Unknown;
    };
    let detail = parts.collect::<Vec<_>>().join(" ");
    if detail.is_empty() {
        return InteractionCommand::Unknown;
    }
    if approve {
        InteractionCommand::Approve { id, name: detail }
    } else {
        InteractionCommand::Reject { id, reason: detail }
    }
}

fn render_source_page(sources: &[uth_storage::SourceRecord], requested_page: usize) -> String {
    if sources.is_empty() {
        return "Hiện chưa có trang nào trong danh sách theo dõi.".to_owned();
    }
    let page_size = 8;
    let page_count = sources.len().div_ceil(page_size);
    let page = requested_page.min(page_count).max(1);
    let start = (page - 1) * page_size;
    let entries = sources[start..sources.len().min(start + page_size)]
        .iter()
        .enumerate()
        .map(|(index, source)| {
            format!(
                "{}. {}\n{}",
                start + index + 1,
                shorten_chars(&source.name, 120),
                display_source_url(source)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let navigation = match (page > 1, page < page_count) {
        (true, true) => format!(
            "\n\nTrang trước: /pages_{}\nTrang sau: /pages_{}",
            page - 1,
            page + 1
        ),
        (true, false) => format!("\n\nTrang trước: /pages_{}", page - 1),
        (false, true) => format!("\n\nTrang sau: /pages_{}", page + 1),
        (false, false) => String::new(),
    };
    format!(
        "Đang theo dõi {} trang, trang {}/{}:\n\n{}{}",
        sources.len(),
        page,
        page_count,
        entries,
        navigation
    )
}

fn display_source_url(source: &uth_storage::SourceRecord) -> String {
    if source.url.chars().count() <= 120 {
        return source.url.clone();
    }
    let source_id = source.key.trim_start_matches("facebook:");
    if source_id.bytes().all(|byte| byte.is_ascii_digit()) {
        format!("https://www.facebook.com/{source_id}")
    } else {
        shorten_chars(&source.url, 120)
    }
}

fn shorten_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    let mut shortened = value
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>();
    shortened.push('…');
    shortened
}

fn normalize_facebook_page_url(value: &str) -> Option<String> {
    if value.len() > 500 {
        return None;
    }
    let mut url = url::Url::parse(value).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let host = url.host_str()?.to_lowercase();
    if host != "facebook.com" && !host.ends_with(".facebook.com") {
        return None;
    }
    let path = url.path().to_lowercase();
    if path == "/"
        || [
            "/posts/", "/reel/", "/reels/", "/videos/", "/photo", "/share/",
        ]
        .iter()
        .any(|part| path.contains(part))
    {
        return None;
    }
    url.set_scheme("https").ok()?;
    url.set_host(Some("www.facebook.com")).ok()?;
    url.set_port(None).ok()?;
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

fn telegram_display_name(chat: &TelegramChat) -> Option<String> {
    let name = [chat.first_name.as_deref(), chat.last_name.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    if name.is_empty() { None } else { Some(name) }
}

fn telegram_sender_label(chat: &TelegramChat) -> String {
    let display_name = telegram_display_name(chat);
    let username = chat.username.as_deref().map(|value| format!("@{value}"));
    match (display_name, username) {
        (Some(name), Some(username)) => format!("{name} ({username})"),
        (Some(name), None) => name,
        (None, Some(username)) => username,
        (None, None) => format!("Người dùng {}", chat.id),
    }
}

async fn plan_event(
    store: &CrawlStore,
    telegram: &TelegramClient,
    owner: &str,
    event: ClaimedNotificationEvent,
    args: &NotifyArgs,
) -> PlanReport {
    let result = plan_event_inner(store, telegram, owner, &event, args.admin_chat_id).await;
    match result {
        Ok(outcome) => PlanReport {
            event_key: event.event_key,
            outcome: Some(outcome),
            failure_disposition: None,
            error: None,
        },
        Err(error) => {
            let message = error.to_string();
            let delay =
                retry_delay_seconds(event.attempts, args.base_retry_delay, args.max_retry_delay);
            let disposition = store
                .fail_notification_event(&event, owner, &message, args.max_attempts, delay)
                .await;
            PlanReport {
                event_key: event.event_key,
                outcome: None,
                failure_disposition: disposition.as_ref().ok().copied(),
                error: Some(match disposition {
                    Ok(_) => message,
                    Err(storage_error) => {
                        format!("{message}; failed to persist failure: {storage_error:#}")
                    }
                }),
            }
        }
    }
}

async fn plan_event_inner(
    store: &CrawlStore,
    telegram: &TelegramClient,
    owner: &str,
    event: &ClaimedNotificationEvent,
    admin_chat_id: Option<i64>,
) -> Result<NotificationPlanOutcome> {
    let payload: NotificationEventPayload = serde_json::from_value(event.payload.clone())
        .context("invalid notification event payload")?;
    let mut post_url = None;
    let mut action_url = None;
    let explicit_drl = payload.classification.features.explicit_drl;
    let message = match payload.classification.decision {
        ClassificationDecision::MatchedExplicit => {
            let post = store
                .load_post_revision(
                    payload.database_post_id,
                    &payload.classification.post_source_id,
                    &payload.classification.external_post_id,
                    &payload.classification.input_content_hash,
                )
                .await?;
            let source_name = store.source_name_for_post(payload.database_post_id).await?;
            post_url = Some(delivery_post_url(&post));
            action_url = payload
                .classification
                .extracted
                .get("form_links")
                .and_then(serde_json::Value::as_array)
                .and_then(|links| links.first())
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            Some(render_structured_notification(
                &post,
                &payload.classification,
                &source_name,
            ))
        }
        ClassificationDecision::ManualReview => {
            let inherited = store
                .inherit_duplicate_manual_review_resolution(payload.database_classification_id)
                .await?;
            if !inherited
                && let Some(admin_chat_id) = admin_chat_id
                && let Some(review) = store
                    .manual_review(payload.database_classification_id)
                    .await?
            {
                match telegram
                    .send_message(admin_chat_id, &render_manual_review_detail(&review))
                    .await
                {
                    TelegramSendOutcome::Sent { .. } => {}
                    TelegramSendOutcome::RetryAfter { seconds, detail } => {
                        bail!("Telegram yêu cầu chờ {seconds} giây khi báo bài cần duyệt: {detail}")
                    }
                    TelegramSendOutcome::ChatMigrated { detail, .. }
                    | TelegramSendOutcome::PermanentFailure { detail, .. }
                    | TelegramSendOutcome::TransientFailure { detail }
                    | TelegramSendOutcome::AuthenticationFailure { detail } => {
                        bail!("không thể báo bài cần duyệt cho admin: {detail}")
                    }
                }
            }
            None
        }
        ClassificationDecision::Rejected => None,
    };
    let content = message.as_deref().map(|message_text| NotificationContent {
        message_text,
        post_url: post_url.as_deref(),
        action_url: action_url.as_deref(),
        explicit_drl,
    });
    store
        .plan_notification(
            event,
            owner,
            payload.database_classification_id,
            payload.database_post_id,
            content.as_ref(),
        )
        .await
}

async fn send_delivery(
    store: &CrawlStore,
    telegram: &TelegramClient,
    portal_client: &PortalClient,
    owner: &str,
    delivery: ClaimedDelivery,
    max_attempts: u32,
    retry_delay: u64,
) -> DeliveryReport {
    let outcome = if let Some(portal_notice_id) = delivery.portal_notice_id {
        if let Some(attachment_url) = delivery.attachment_url.as_deref() {
            if let Some(file_id) = delivery.telegram_file_id.as_deref() {
                telegram
                    .send_portal_document_by_id(
                        delivery.telegram_chat_id,
                        &delivery.message_text,
                        file_id,
                        delivery.post_url.as_deref(),
                    )
                    .await
            } else {
                match portal_client
                    .download_attachment(
                        portal_notice_id,
                        attachment_url,
                        delivery.attachment_content_type.as_deref(),
                    )
                    .await
                {
                    Ok(attachment) => {
                        telegram
                            .upload_portal_document(
                                delivery.telegram_chat_id,
                                &delivery.message_text,
                                &attachment.file_name,
                                &attachment.content_type,
                                attachment.bytes,
                                delivery.post_url.as_deref(),
                            )
                            .await
                    }
                    Err(error) => TelegramSendOutcome::TransientFailure {
                        detail: error.to_string().chars().take(1_000).collect(),
                    },
                }
            }
        } else {
            telegram
                .send_portal_notification(
                    delivery.telegram_chat_id,
                    &delivery.message_text,
                    delivery.post_url.as_deref(),
                )
                .await
        }
    } else {
        telegram
            .send_notification(
                delivery.telegram_chat_id,
                &delivery.message_text,
                delivery.campaign_id,
                delivery.action_url.as_deref(),
                delivery.post_url.as_deref(),
            )
            .await
    };
    let result = match outcome {
        TelegramSendOutcome::Sent {
            message_id,
            file_id,
        } => store
            .complete_delivery(&delivery, owner, message_id, file_id.as_deref())
            .await
            .map(|()| ("sent", None)),
        TelegramSendOutcome::RetryAfter { seconds, detail } => {
            if delivery.attempt >= max_attempts {
                store
                    .fail_delivery(
                        &delivery,
                        owner,
                        Some(429),
                        &detail,
                        DeliveryFailureClass::RetryExhausted,
                    )
                    .await
                    .map(|()| ("permanently_failed", Some(detail)))
            } else {
                store
                    .retry_delivery(&delivery, owner, seconds, Some(429), &detail)
                    .await
                    .map(|()| ("retry_scheduled", Some(detail)))
            }
        }
        TelegramSendOutcome::ChatMigrated {
            new_chat_id,
            detail,
        } => store
            .migrate_delivery_chat(&delivery, owner, new_chat_id, &detail)
            .await
            .map(|()| ("chat_migrated", Some(detail))),
        TelegramSendOutcome::PermanentFailure { deactivate, detail } => {
            let failure_class = if deactivate {
                DeliveryFailureClass::RecipientUnavailable
            } else {
                DeliveryFailureClass::RequestRejected
            };
            store
                .fail_delivery(&delivery, owner, None, &detail, failure_class)
                .await
                .map(|()| ("permanently_failed", Some(detail)))
        }
        TelegramSendOutcome::TransientFailure { detail } => {
            if delivery.attempt >= max_attempts {
                store
                    .fail_delivery(
                        &delivery,
                        owner,
                        None,
                        &detail,
                        DeliveryFailureClass::RetryExhausted,
                    )
                    .await
                    .map(|()| ("permanently_failed", Some(detail)))
            } else {
                store
                    .retry_delivery(&delivery, owner, retry_delay, None, &detail)
                    .await
                    .map(|()| ("retry_scheduled", Some(detail)))
            }
        }
        TelegramSendOutcome::AuthenticationFailure { detail } => store
            .retry_delivery(&delivery, owner, 300, Some(401), &detail)
            .await
            .map(|()| ("authentication_failed", Some(detail))),
    };
    match result {
        Ok((outcome, error)) => DeliveryReport {
            delivery_id: delivery.id,
            chat_id: delivery.telegram_chat_id,
            attempt: delivery.attempt,
            outcome: outcome.to_owned(),
            error,
        },
        Err(error) => DeliveryReport {
            delivery_id: delivery.id,
            chat_id: delivery.telegram_chat_id,
            attempt: delivery.attempt,
            outcome: "persistence_failed".to_owned(),
            error: Some(error.to_string()),
        },
    }
}

fn validate_args(args: &NotifyArgs) -> Result<()> {
    if args.concurrency == 0
        || args.plan_batch_size == 0
        || args.lease_duration == 0
        || args.poll_interval == 0
        || args.request_timeout == 0
        || args.messages_per_second == 0
        || args.messages_per_second > 25
        || args.max_attempts == 0
        || args.base_retry_delay == 0
        || args.max_retry_delay < args.base_retry_delay
        || args.sent_delivery_retention_days <= 0
        || args.failed_delivery_retention_days <= 0
        || args.inactive_subscriber_retention_days <= 0
        || args.retention_interval == 0
        || args.health_alert_after_failures == 0
        || args.health_backlog_stale_seconds == 0
        || args.health_alert_grace_seconds == 0
        || args.health_alert_interval == 0
        || args.portal_poll_interval == 0
        || args.portal_poll_interval > 86_400
        || args.portal_burst_interval == 0
        || args.portal_burst_interval > args.portal_poll_interval
        || args.portal_burst_duration < args.portal_burst_interval
        || args.portal_burst_duration > 86_400
        || args.portal_forbidden_cooldown == 0
        || args.portal_forbidden_cooldown > 2_592_000
        || args.portal_rate_limit_cooldown == 0
        || args.portal_rate_limit_cooldown > 2_592_000
        || args.portal_failure_cooldown == 0
        || args.portal_failure_cooldown > 2_592_000
        || args.portal_jitter_percent > 25
        || !(1..=100).contains(&args.portal_page_size)
        || args.portal_max_pages == 0
        || args.portal_request_timeout == 0
        || args.portal_file_timeout == 0
        || args.portal_max_file_bytes == 0
        || args.portal_max_file_bytes > TELEGRAM_DOCUMENT_LIMIT
        || args.admin_chat_id == Some(0)
        || !(300..=86_400).contains(&args.donation_link_ttl)
    {
        bail!("notification limits, intervals, attempts, and retry bounds must be valid");
    }
    let donate_url = normalized_optional_value(args.donate_vietqr_url.as_deref());
    let donate_message = normalized_optional_value(args.donate_message.as_deref());
    let donate_bank_account = normalized_optional_value(args.donate_bank_account.as_deref());
    if let Some(value) = donate_url {
        let parsed =
            url::Url::parse(&value).context("DONATE_VIETQR_URL must be a valid HTTPS URL")?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || value.chars().count() > 2_048
        {
            bail!("DONATE_VIETQR_URL must be a valid HTTPS URL without credentials");
        }
    }
    if donate_message
        .as_ref()
        .is_some_and(|value| value.chars().count() > 1_000 || value.chars().any(|c| c == '\0'))
    {
        bail!("DONATE_MESSAGE must not exceed 1000 characters or contain NUL");
    }
    if donate_bank_account.as_ref().is_some_and(|value| {
        !(6..=20).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        bail!("DONATE_BANK_ACCOUNT must contain 6 to 20 digits");
    }
    Ok(())
}

fn normalized_optional_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn render_donation(config: &DonationConfig, payos_enabled: bool) -> String {
    if payos_enabled {
        let message = config
            .message
            .as_deref()
            .map(|value| format!("\n\n{value}"))
            .unwrap_or_default();
        return format!(
            "Nếu UTH Notifier hữu ích với bạn, bạn có thể chọn một mức gợi ý hoặc chọn Tùy tâm để nhập số tiền phù hợp.{message}\n\nBot sẽ gửi QR payOS đã điền sẵn số tiền và nội dung chuyển khoản, rồi tự động xác nhận giao dịch.\n\nViệc ủng hộ hoàn toàn tự nguyện và không ảnh hưởng quyền dùng bot."
        );
    }
    let Some(vietqr_url) = config.vietqr_url.as_deref() else {
        return "Tính năng ủng hộ hiện chưa được cấu hình. Vui lòng thử lại sau.".to_owned();
    };
    let message = config
        .message
        .as_deref()
        .map(|value| format!("\n\n{value}"))
        .unwrap_or_default();
    format!(
        "Ủng hộ chi phí vận hành UTH Notifier\n\nMở hoặc quét mã VietQR:\n{vietqr_url}{message}\n\nĐây là khoản ủng hộ tự nguyện. Bot không tự động xác nhận giao dịch."
    )
}

fn format_vnd(amount: i64) -> String {
    let digits = amount.to_string();
    let mut output = String::with_capacity(digits.len() + digits.len() / 3 + 4);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            output.push('.');
        }
        output.push(character);
    }
    output.push_str(" VND");
    output
}

fn display_bank(bin: &str) -> String {
    match bin {
        "970418" => "BIDV".to_owned(),
        _ => "Ngân hàng liên kết với payOS".to_owned(),
    }
}

fn render_payment_account(config: &DonationConfig) -> String {
    match config.bank_account.as_deref() {
        Some(bank_account) => format!("STK ngân hàng: {bank_account}"),
        None => "STK ngân hàng: xem trên trang thanh toán payOS".to_owned(),
    }
}

async fn run_payos_payment_cycle(store: &CrawlStore, telegram: &TelegramClient) -> Result<()> {
    for event in store.pending_payos_edge_events(10).await? {
        let payload: PayOsPaymentPayload = match serde_json::from_value(event.payload.clone()) {
            Ok(payload) => payload,
            Err(error) => {
                store
                    .fail_edge_event(
                        &event.event_id,
                        &format!("edge inbox contains an invalid payOS payment: {error}"),
                        3,
                        retry_delay_seconds(event.attempts + 1, 5, 60),
                    )
                    .await?;
                continue;
            }
        };
        if payload.code != "00" {
            store.complete_edge_event(&event.event_id).await?;
            continue;
        }
        let transaction_at = match DateTime::parse_from_str(
            &format!("{}+07:00", payload.transaction_date_time),
            "%Y-%m-%d %H:%M:%S%:z",
        ) {
            Ok(value) => value.with_timezone(&Utc),
            Err(error) => {
                store
                    .fail_edge_event(
                        &event.event_id,
                        &format!("payOS transaction date has an invalid format: {error}"),
                        3,
                        retry_delay_seconds(event.attempts + 1, 5, 60),
                    )
                    .await?;
                continue;
            }
        };
        let outcome = match store
            .record_donation_payment(&DonationPayment {
                order_code: payload.order_code,
                payment_link_id: &payload.payment_link_id,
                reference: &payload.reference,
                amount: payload.amount,
                currency: &payload.currency,
                transaction_at,
                payload: &event.payload,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                store
                    .reject_edge_event(&event.event_id, &error.to_string())
                    .await?;
                continue;
            }
        };
        let send_succeeded = match telegram
            .send_message(
                outcome.telegram_chat_id,
                &format!(
                    "Đã nhận khoản ủng hộ.\n\nSố tiền: {}\nNội dung: {}\nMã giao dịch: {}\nThời gian: {}\n\nCảm ơn bạn đã hỗ trợ chi phí vận hành UTH Notifier.",
                    format_vnd(payload.amount),
                    payload.description,
                    payload.reference,
                    format_vietnam_datetime(&transaction_at.to_rfc3339())
                ),
            )
            .await
        {
            TelegramSendOutcome::Sent { .. }
            | TelegramSendOutcome::PermanentFailure { .. }
            | TelegramSendOutcome::ChatMigrated { .. } => true,
            TelegramSendOutcome::RetryAfter { .. }
            | TelegramSendOutcome::TransientFailure { .. } => false,
            TelegramSendOutcome::AuthenticationFailure { detail } => {
                bail!("Telegram authentication failed while confirming donation: {detail}")
            }
        };
        if send_succeeded {
            store.complete_edge_event(&event.event_id).await?;
        } else {
            store
                .fail_edge_event(
                    &event.event_id,
                    "Telegram chưa nhận được xác nhận thanh toán",
                    5,
                    retry_delay_seconds(event.attempts + 1, 5, 300),
                )
                .await?;
        }
    }
    Ok(())
}

fn retry_delay_seconds(attempt: u32, base: u64, maximum: u64) -> u64 {
    let exponent = attempt.saturating_sub(1).min(20);
    base.saturating_mul(1_u64 << exponent).min(maximum)
}

fn count_delivery_outcome(reports: &[DeliveryReport], outcome: &str) -> usize {
    reports
        .iter()
        .filter(|report| report.outcome == outcome)
        .count()
}

fn render_operational_health(health: &OperationalHealth, detailed: bool) -> String {
    let summary = match health.status.as_str() {
        "healthy" => "Hệ thống đang hoạt động ổn định.",
        "degraded" => "Hệ thống đang chậm hoặc có thành phần cần kiểm tra.",
        _ => "Hệ thống đang có lỗi cần quản trị viên xử lý.",
    };
    if !detailed {
        return summary.to_owned();
    }
    format!(
        "{summary}\nNguồn: {} đang bật, {} chưa được kiểm tra, {} quá lâu chưa cập nhật, {} đang lỗi, {} đang cảnh báo.\nViệc đang chờ: {} bài cần phân loại, {} bài cần lập thông báo, {} tin cần gửi, {} bản tin 07:30, {} sự kiện nhận vào, {} khoản ủng hộ.\nViệc thất bại: {} sự kiện xử lý, {} lượt gửi, {} bản tin 07:30, {} sự kiện nhận vào, {} khoản ủng hộ.\nBộ phận gửi Telegram: {}. Đề xuất trang chờ duyệt: {}. Bài chờ duyệt: {}.",
        health.enabled_sources,
        health.sources_never_crawled,
        health.stale_sources,
        health.sources_with_failures,
        health.sources_alerting,
        health.pending_classification_events,
        health.pending_notification_events,
        health.pending_deliveries,
        health.pending_digest_batches,
        health.pending_edge_events,
        health.pending_donation_intents,
        health.dead_letters,
        health.failed_deliveries,
        health.failed_digest_batches,
        health.dead_lettered_edge_events,
        health.failed_donation_intents,
        if health.telegram_worker_active {
            "đang chạy"
        } else {
            "chưa sẵn sàng"
        },
        health.pending_source_suggestions,
        health.pending_manual_reviews
    )
}

fn render_operational_alert(kind: OperationalAlertKind, health: &OperationalHealth) -> String {
    let heading = match kind {
        OperationalAlertKind::Degraded => {
            "Hệ thống cần kiểm tra: trạng thái suy giảm đã kéo dài quá ngưỡng."
        }
        OperationalAlertKind::Failed => {
            "Hệ thống có lỗi cần xử lý: một ngưỡng cảnh báo nghiêm trọng đã bị vượt qua."
        }
        OperationalAlertKind::Recovered => "Hệ thống đã phục hồi và hoạt động ổn định.",
    };
    format!("{heading}\n\n{}", render_operational_health(health, true))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use uth_storage::{
        CrawlAttemptHistoryRecord, CrawlHistoryDetail, CrawlHistoryRecord, OperationalHealth,
        PortalNoticeHistoryRecord, PortalPollState, SourceRecord, UserFeedbackHistoryRecord,
    };

    use super::{
        DonationConfig, InteractionCommand, PortalCycleReport, PortalPollConfig,
        SUPPORT_GROUP_INVITATION, display_bank, next_portal_poll_state,
        normalize_facebook_page_url, parse_donation_amount, parse_interaction_command,
        render_crawl_history_detail, render_crawl_history_page, render_donation, render_help,
        render_operational_alert, render_operational_health, render_payment_account,
        render_portal_notice_history_detail, render_portal_notice_history_page, render_source_page,
        render_user_feedback_page, retry_delay_seconds,
    };
    use uth_storage::OperationalAlertKind;

    #[test]
    fn retry_delay_is_bounded() {
        assert_eq!(retry_delay_seconds(1, 30, 900), 30);
        assert_eq!(retry_delay_seconds(2, 30, 900), 60);
        assert_eq!(retry_delay_seconds(30, 30, 900), 900);
    }

    #[test]
    fn portal_polling_enters_and_exits_a_bounded_burst() {
        let polled_at = Utc.with_ymd_and_hms(2026, 8, 9, 10, 0, 0).unwrap();
        let config = portal_poll_config();
        let steady = portal_poll_state("steady", polled_at, None);
        let found = PortalCycleReport {
            checked: true,
            notices_found: 2,
            http_status: Some(200),
            ..PortalCycleReport::default()
        };

        let burst = next_portal_poll_state(&steady, &found, polled_at, config).unwrap();
        assert_eq!(burst.mode, "burst");
        assert_eq!(burst.next_poll_at, polled_at + ChronoDuration::seconds(60));
        assert_eq!(
            burst.burst_until,
            Some(polled_at + ChronoDuration::seconds(900))
        );

        let unchanged = PortalCycleReport {
            checked: true,
            http_status: Some(200),
            ..PortalCycleReport::default()
        };
        let within_burst = polled_at + ChronoDuration::seconds(60);
        let continued = next_portal_poll_state(&burst, &unchanged, within_burst, config).unwrap();
        assert_eq!(continued.mode, "burst");
        assert_eq!(
            continued.next_poll_at,
            within_burst + ChronoDuration::seconds(60)
        );

        let after_burst = polled_at + ChronoDuration::seconds(901);
        let steady_again =
            next_portal_poll_state(&continued, &unchanged, after_burst, config).unwrap();
        assert_eq!(steady_again.mode, "steady");
        assert_eq!(
            steady_again.next_poll_at,
            after_burst + ChronoDuration::seconds(300)
        );
    }

    #[test]
    fn portal_polling_cools_down_without_jitter_after_access_denial() {
        let polled_at = Utc.with_ymd_and_hms(2026, 8, 9, 10, 0, 0).unwrap();
        let state = portal_poll_state("steady", polled_at, None);
        let forbidden = PortalCycleReport {
            checked: true,
            failure_kind: Some("forbidden".to_owned()),
            http_status: Some(403),
            error: Some("Portal returned HTTP 403".to_owned()),
            ..PortalCycleReport::default()
        };

        let cooldown =
            next_portal_poll_state(&state, &forbidden, polled_at, portal_poll_config()).unwrap();
        assert_eq!(cooldown.mode, "cooldown");
        assert_eq!(cooldown.cooldown_reason.as_deref(), Some("forbidden"));
        assert_eq!(
            cooldown.next_poll_at,
            polled_at + ChronoDuration::seconds(21_600)
        );
        assert_eq!(cooldown.last_http_status, Some(403));
    }

    #[test]
    fn portal_polling_honors_rate_limit_retry_after() {
        let polled_at = Utc.with_ymd_and_hms(2026, 8, 9, 10, 0, 0).unwrap();
        let state = portal_poll_state("steady", polled_at, None);
        let rate_limited = PortalCycleReport {
            checked: true,
            failure_kind: Some("rate_limited".to_owned()),
            http_status: Some(429),
            retry_after_seconds: Some(120),
            error: Some("Portal returned HTTP 429".to_owned()),
            ..PortalCycleReport::default()
        };

        let cooldown =
            next_portal_poll_state(&state, &rate_limited, polled_at, portal_poll_config()).unwrap();
        assert_eq!(
            cooldown.next_poll_at,
            polled_at + ChronoDuration::seconds(120)
        );
    }

    fn portal_poll_config() -> PortalPollConfig {
        PortalPollConfig {
            steady_interval: 300,
            burst_interval: 60,
            burst_duration: 900,
            forbidden_cooldown: 21_600,
            rate_limit_cooldown: 1_800,
            failure_cooldown: 900,
            jitter_percent: 0,
        }
    }

    fn portal_poll_state(
        mode: &str,
        next_poll_at: chrono::DateTime<Utc>,
        burst_until: Option<chrono::DateTime<Utc>>,
    ) -> PortalPollState {
        PortalPollState {
            mode: mode.to_owned(),
            next_poll_at,
            burst_until,
            cooldown_reason: None,
            last_polled_at: None,
            last_poll_outcome: None,
            last_http_status: None,
        }
    }

    #[test]
    fn parses_vietnamese_buttons_and_telegram_commands() {
        assert_eq!(
            parse_interaction_command("Trạng thái"),
            InteractionCommand::Status
        );
        assert_eq!(
            parse_interaction_command("Bật thông báo"),
            InteractionCommand::Start(None)
        );
        assert_eq!(
            parse_interaction_command("/start lop_k47"),
            InteractionCommand::Start(Some("lop_k47".to_owned()))
        );
        assert_eq!(
            parse_interaction_command("/onboard_drl"),
            InteractionCommand::OnboardScope("drl")
        );
        assert_eq!(
            parse_interaction_command("/settings_mode_daily"),
            InteractionCommand::SettingsMode("daily")
        );
        assert_eq!(
            parse_interaction_command("/settings_sample"),
            InteractionCommand::SettingsSample
        );
        assert_eq!(
            parse_interaction_command("/feedback"),
            InteractionCommand::UserFeedbackPrompt
        );
        assert_eq!(
            parse_interaction_command("/feedbacks"),
            InteractionCommand::FeedbackHistory(1)
        );
        assert_eq!(
            parse_interaction_command("/feedbacks_2"),
            InteractionCommand::FeedbackHistory(2)
        );
        assert_eq!(
            parse_interaction_command("/feedback Tin nhắn gửi bị trễ"),
            InteractionCommand::UserFeedback("Tin nhắn gửi bị trễ".to_owned())
        );
        assert_eq!(
            parse_interaction_command("Gửi phản hồi"),
            InteractionCommand::UserFeedbackPrompt
        );
        assert_eq!(
            parse_interaction_command("/useful_17"),
            InteractionCommand::Feedback {
                campaign_id: 17,
                value: "useful"
            }
        );
        assert_eq!(
            parse_interaction_command("/stop@uth_notifier_bot"),
            InteractionCommand::Stop
        );
        assert_eq!(
            parse_interaction_command("Trợ giúp"),
            InteractionCommand::Help
        );
        assert_eq!(
            parse_interaction_command("Trang đang theo dõi"),
            InteractionCommand::Pages(1)
        );
        assert_eq!(
            parse_interaction_command("Ủng hộ"),
            InteractionCommand::Donate
        );
        assert_eq!(
            parse_interaction_command("/donate@uth_notifier_bot"),
            InteractionCommand::Donate
        );
        assert_eq!(
            parse_interaction_command("/donate_50000"),
            InteractionCommand::DonateAmount(50_000)
        );
        assert_eq!(
            parse_interaction_command("/donate 35k"),
            InteractionCommand::DonateAmount(35_000)
        );
        assert_eq!(
            parse_interaction_command("/donate_custom"),
            InteractionCommand::PromptDonationAmount
        );
        assert_eq!(
            parse_interaction_command("/cancel"),
            InteractionCommand::Cancel
        );
        assert_eq!(
            parse_interaction_command("/donate 12.345"),
            InteractionCommand::DonateAmount(12_345)
        );
        assert_eq!(
            parse_interaction_command("10.000 VND"),
            InteractionCommand::DonateAmount(10_000)
        );
        assert_eq!(
            parse_interaction_command("Để sau"),
            InteractionCommand::DeclineDonation
        );
        assert_eq!(
            parse_interaction_command("Tùy tâm"),
            InteractionCommand::PromptDonationAmount
        );
        assert_eq!(
            parse_interaction_command("/donate_later"),
            InteractionCommand::DeclineDonation
        );
        assert!(matches!(
            parse_interaction_command("/donate_9999"),
            InteractionCommand::Usage(_)
        ));
        assert_eq!(parse_donation_amount("35.000 VND"), Some(35_000));
        assert_eq!(parse_donation_amount("35k"), Some(35_000));
        assert_eq!(parse_donation_amount("35k đ"), Some(35_000));
        assert_eq!(parse_donation_amount("35,5k"), None);
        assert_eq!(parse_donation_amount("35,5"), None);
        assert_eq!(parse_donation_amount("10,000.000"), None);
        assert_eq!(parse_donation_amount("9999"), None);
        assert_eq!(parse_donation_amount("10000001"), None);
        assert_eq!(
            parse_interaction_command("/pages 3"),
            InteractionCommand::Pages(3)
        );
        assert_eq!(
            parse_interaction_command("/pages_4"),
            InteractionCommand::Pages(4)
        );
        assert_eq!(
            parse_interaction_command("/suggest https://facebook.com/itclubuth"),
            InteractionCommand::Suggest(Some("https://facebook.com/itclubuth".to_owned()))
        );
        assert_eq!(
            parse_interaction_command("/approve 7 Câu lạc bộ Công nghệ"),
            InteractionCommand::Approve {
                id: 7,
                name: "Câu lạc bộ Công nghệ".to_owned()
            }
        );
        assert_eq!(
            parse_interaction_command("/reviews 2"),
            InteractionCommand::Reviews(2)
        );
        assert_eq!(
            parse_interaction_command("/portal_history"),
            InteractionCommand::PortalHistory(1)
        );
        assert_eq!(
            parse_interaction_command("/portal_history_2"),
            InteractionCommand::PortalHistory(2)
        );
        assert_eq!(
            parse_interaction_command("/crawl_history"),
            InteractionCommand::CrawlHistory(1)
        );
        assert_eq!(
            parse_interaction_command("/crawl_history_2"),
            InteractionCommand::CrawlHistory(2)
        );
        assert_eq!(
            parse_interaction_command("/crawl_run 14"),
            InteractionCommand::CrawlRun(14)
        );
        assert_eq!(
            parse_interaction_command("/crawl_run_14"),
            InteractionCommand::CrawlRun(14)
        );
        assert_eq!(
            parse_interaction_command("/portal_notice 1438"),
            InteractionCommand::PortalNotice(1438)
        );
        assert_eq!(
            parse_interaction_command("/portal_notice_1438"),
            InteractionCommand::PortalNotice(1438)
        );
        assert_eq!(
            parse_interaction_command("/review 14"),
            InteractionCommand::Review(14)
        );
        assert_eq!(
            parse_interaction_command("/review_send 14"),
            InteractionCommand::ReviewSend(14)
        );
        assert_eq!(
            parse_interaction_command("/review_send_14"),
            InteractionCommand::ReviewSend(14)
        );
        assert_eq!(
            parse_interaction_command("/review_14"),
            InteractionCommand::Review(14)
        );
        assert_eq!(
            parse_interaction_command("/review_skip_14"),
            InteractionCommand::ReviewSkip {
                id: 14,
                reason: None
            }
        );
        assert_eq!(
            parse_interaction_command("/review_skip 14 Không liên quan"),
            InteractionCommand::ReviewSkip {
                id: 14,
                reason: Some("Không liên quan".to_owned())
            }
        );
        assert_eq!(
            parse_interaction_command("/review_send -1"),
            InteractionCommand::Usage(
                "Thiếu mã bài. Hãy bấm lệnh /review_send_ID trong tin chi tiết."
            )
        );
        assert_eq!(
            parse_interaction_command("/latest 3"),
            InteractionCommand::Latest(3)
        );
        assert_eq!(
            parse_interaction_command("/latest_3"),
            InteractionCommand::Latest(3)
        );
        assert_eq!(
            parse_interaction_command("/reviews_2"),
            InteractionCommand::Reviews(2)
        );
        assert_eq!(
            parse_interaction_command("/latest_post 25"),
            InteractionCommand::LatestPost(25)
        );
        assert_eq!(
            parse_interaction_command("/latest_post_25"),
            InteractionCommand::LatestPost(25)
        );
    }

    #[test]
    fn renders_portal_history_with_attachment_and_navigation() {
        let notice = PortalNoticeHistoryRecord {
            portal_id: 1438,
            title: "Thông báo kiểm thử".to_owned(),
            displayed_at: Utc::now(),
            article_url: Some("https://portal.ut.edu.vn/example".to_owned()),
            attachment_url: Some(
                "https://portal.ut.edu.vn/api/v1/notification/getFile/1438".to_owned(),
            ),
            attachment_content_type: Some("application/pdf".to_owned()),
            discovered_at: Utc::now(),
        };
        let page = render_portal_notice_history_page(&[notice.clone(), notice.clone()], 1, 1);
        let detail = render_portal_notice_history_detail(&notice);

        assert!(page.contains("/portal_notice_1438"));
        assert!(page.contains("Tệp đính kèm: có"));
        assert!(page.contains("/portal_history_2"));
        assert!(detail.contains("THÔNG BÁO PORTAL #1438"));
        assert!(detail.contains("bot gửi kèm"));
        assert!(detail.chars().count() <= 1_024);
    }

    #[test]
    fn renders_user_feedback_history_with_navigation() {
        let feedback = UserFeedbackHistoryRecord {
            id: 7,
            telegram_chat_id: 123,
            sender_label: "Người gửi".to_owned(),
            message: "Nội dung góp ý".to_owned(),
            admin_notified_at: Some(Utc::now()),
            created_at: Utc::now(),
        };
        let page = render_user_feedback_page(&[feedback], 6, 1, 5);
        assert!(page.text.contains("Toàn bộ feedback đã gửi: 6"));
        assert!(page.text.contains("#7 - Người gửi"));
        assert!(page.text.contains("Mở cuộc trò chuyện với người gửi #7"));
        assert!(page.text.contains("/feedbacks_2"));
        assert_eq!(page.user_links.len(), 1);
        assert_eq!(page.user_links[0].user_chat_id, 123);
    }

    #[test]
    fn renders_crawl_history_including_empty_runs_and_attempts() {
        let run = CrawlHistoryRecord {
            run_id: 31,
            source_key: "facebook:123".to_owned(),
            source_name: "Nguồn thử".to_owned(),
            fetched_at: Utc::now(),
            created_at: Utc::now(),
            health: "degraded".to_owned(),
            selected_strategy: None,
            post_count: 0,
            attempt_count: 1,
            error: Some("không tìm thấy bài".to_owned()),
        };
        let detail = CrawlHistoryDetail {
            run: run.clone(),
            attempts: vec![CrawlAttemptHistoryRecord {
                ordinal: 0,
                strategy: "http".to_owned(),
                outcome: "empty".to_owned(),
                status: Some(200),
                latency_ms: 120,
                bytes_received: 450,
                posts_found: 0,
                newest_post_at: None,
                error: None,
                browser: Some(uth_domain::BrowserAttemptMetadata {
                    network_requested_mode: "prefer_ipv4".to_owned(),
                    network_effective_mode: "ipv4".to_owned(),
                    network_remote_family: "ipv4".to_owned(),
                    network_fallback_reason: None,
                    login_overlay_detected: true,
                    login_overlay_dismissed: true,
                    login_route_detected: false,
                    discovered_post_origin: Some("dom".to_owned()),
                    newest_dom_post_unresolved: false,
                }),
            }],
        };
        let page = render_crawl_history_page(&[run], 1, 1);
        let rendered_detail = render_crawl_history_detail(&detail);
        assert!(rendered_detail.contains("prefer_ipv4"));
        assert!(rendered_detail.contains("ipv4"));

        assert!(page.contains("Bài tìm thấy: 0"));
        assert!(page.contains("/crawl_run_31"));
        assert!(rendered_detail.contains("LẦN CRAWL #31"));
        assert!(rendered_detail.contains("HTTP: 200"));
        assert!(rendered_detail.contains("Bài: 0"));
        assert!(rendered_detail.chars().count() <= 4_000);
    }

    #[test]
    fn help_explains_notification_choices_without_internal_terms() {
        let user = render_help(false);
        assert!(user.contains("Nhận ngay"));
        assert!(user.contains("Bản tin lúc 07:30"));
        assert!(user.contains("Ngày không có tin phù hợp, bot sẽ không gửi"));
        assert!(user.contains("Giờ yên lặng"));
        assert!(user.contains(SUPPORT_GROUP_INVITATION));
        assert!(!user.contains("delivery_mode"));
        assert!(!user.contains("campaign"));
        assert!(!user.contains("/admin"));
        assert!(user.contains("/latest"));
        assert!(user.contains("/portal_history"));
        assert!(user.contains("Gửi phản hồi"));

        let admin = render_help(true);
        assert!(admin.contains("/admin"));
    }

    #[test]
    fn renders_configured_and_disabled_donation() {
        let disabled = render_donation(
            &DonationConfig {
                vietqr_url: None,
                message: None,
                bank_account: None,
            },
            false,
        );
        assert!(disabled.contains("chưa được cấu hình"));

        let configured = render_donation(
            &DonationConfig {
                vietqr_url: Some("https://img.vietqr.io/image/test.png".to_owned()),
                message: Some("Cảm ơn bạn đã đồng hành.".to_owned()),
                bank_account: None,
            },
            false,
        );
        assert!(configured.contains("https://img.vietqr.io/image/test.png"));
        assert!(configured.contains("Cảm ơn bạn đã đồng hành."));
        assert!(configured.contains("không tự động xác nhận giao dịch"));

        let payos = render_donation(
            &DonationConfig {
                vietqr_url: None,
                message: None,
                bank_account: None,
            },
            true,
        );
        assert!(payos.contains("chọn một mức gợi ý"));
        assert!(!payos.contains("35.000"));
        assert!(payos.contains("Tùy tâm"));
    }

    #[test]
    fn displays_only_the_linked_bank_account() {
        let details = render_payment_account(&DonationConfig {
            vietqr_url: None,
            message: None,
            bank_account: Some("1234567890".to_owned()),
        });

        assert_eq!(details, "STK ngân hàng: 1234567890");
        assert!(!details.contains("payOS"));
    }

    #[test]
    fn hides_bank_bin_from_user_facing_text() {
        assert_eq!(display_bank("970418"), "BIDV");
        assert_eq!(display_bank("000000"), "Ngân hàng liên kết với payOS");
    }

    #[test]
    fn validates_facebook_page_suggestions() {
        assert_eq!(
            normalize_facebook_page_url("http://facebook.com/ITClubUTH/?ref=test"),
            Some("https://www.facebook.com/ITClubUTH/".to_owned())
        );
        assert!(
            normalize_facebook_page_url("https://www.facebook.com/ITClubUTH/posts/123").is_none()
        );
        assert!(normalize_facebook_page_url("https://example.com/page").is_none());
    }

    #[test]
    fn renders_short_and_clear_source_pages() {
        let sources = (1..=9)
            .map(|number| SourceRecord {
                key: format!("source-{number}"),
                name: format!("Trang hoạt động {number}"),
                url: format!("https://www.facebook.com/page{number}/"),
            })
            .collect::<Vec<_>>();
        let first_page = render_source_page(&sources, 1);
        let second_page = render_source_page(&sources, 2);

        assert!(first_page.contains("Đang theo dõi 9 trang, trang 1/2"));
        assert!(first_page.contains("Trang sau: /pages_2"));
        assert!(second_page.contains("9. Trang hoạt động 9"));
        assert!(second_page.contains("Trang trước: /pages_1"));
    }

    #[test]
    fn renders_detailed_health_only_for_admin() {
        let health = OperationalHealth {
            schema_version: "operational-health.v1".to_owned(),
            generated_at: Utc::now(),
            status: "degraded".to_owned(),
            enabled_sources: 4,
            sources_never_crawled: 1,
            stale_sources: 1,
            sources_with_failures: 1,
            sources_alerting: 0,
            pending_classification_events: 2,
            oldest_classification_event_age_seconds: Some(30),
            pending_notification_events: 0,
            oldest_notification_event_age_seconds: None,
            dead_letters: 0,
            pending_deliveries: 1,
            oldest_pending_delivery_age_seconds: Some(20),
            failed_deliveries: 0,
            pending_digest_batches: 1,
            failed_digest_batches: 0,
            pending_edge_events: 1,
            dead_lettered_edge_events: 0,
            pending_donation_intents: 1,
            failed_donation_intents: 0,
            active_subscribers: 2,
            pending_source_suggestions: 3,
            pending_manual_reviews: 4,
            telegram_worker_active: true,
        };
        let user = render_operational_health(&health, false);
        let admin = render_operational_health(&health, true);

        assert!(!user.contains("Việc đang chờ"));
        assert!(admin.contains("Việc đang chờ: 2 bài cần phân loại"));
        assert!(admin.contains("Đề xuất trang chờ duyệt: 3"));
        assert!(admin.contains("Bài chờ duyệt: 4"));
        let alert = render_operational_alert(OperationalAlertKind::Degraded, &health);
        assert!(alert.contains("suy giảm đã kéo dài quá ngưỡng"));
        assert!(alert.contains("Việc đang chờ: 2 bài cần phân loại"));
    }
}
