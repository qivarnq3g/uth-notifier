use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use uth_classifier::RuleClassifier;
use uth_domain::{ClassificationDecision, FacebookPost};
use uth_storage::{
    ClaimedClassificationEvent, ClassificationPersistOutcome, ClassifierRetentionOutcome,
    CrawlStore, FailureDisposition,
};

#[derive(Debug, clap::Args)]
pub struct ClassifyArgs {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,

    #[arg(long, default_value = "config/classifier-rules.v1.json")]
    config: PathBuf,

    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    #[arg(long, default_value_t = 120)]
    lease_duration: u64,

    #[arg(long, default_value_t = 15)]
    poll_interval: u64,

    #[arg(long, default_value_t = 5)]
    max_attempts: u32,

    #[arg(long, default_value_t = 30)]
    base_retry_delay: u64,

    #[arg(long, default_value_t = 900)]
    max_retry_delay: u64,

    #[arg(long, default_value_t = 30)]
    dead_letter_retention_days: i32,

    #[arg(long, default_value_t = 30)]
    processed_event_retention_days: i32,

    #[arg(long, default_value_t = 86_400)]
    retention_interval: u64,

    #[arg(long)]
    once: bool,
}

#[derive(Debug, Serialize)]
struct ClassificationCycleReport {
    schema_version: String,
    generated_at: String,
    classifier_version: String,
    config_hash: String,
    claimed: usize,
    rejected: usize,
    matched_explicit: usize,
    manual_review: usize,
    retry_scheduled: usize,
    dead_lettered: usize,
    failed: usize,
    retention_applied: bool,
    retention: ClassifierRetentionOutcome,
    events: Vec<ClassificationEventReport>,
}

#[derive(Debug, Serialize)]
struct ClassificationEventReport {
    event_key: String,
    attempt: u32,
    decision: Option<String>,
    persistence: Option<ClassificationPersistOutcome>,
    failure_disposition: Option<FailureDisposition>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClassificationEventPayload {
    post: FacebookPost,
    database_post_id: i64,
}

pub async fn run(args: ClassifyArgs) -> Result<()> {
    validate_args(&args)?;
    let raw = fs::read(&args.config)
        .with_context(|| format!("failed to read classifier config {}", args.config.display()))?;
    let classifier = RuleClassifier::from_bytes(&raw)?;
    let max_connections = u32::try_from(args.concurrency.saturating_add(2))?;
    let store = CrawlStore::connect(&args.database_url, max_connections).await?;
    store.migrate().await?;
    let owner = format!("uth-classifier-{}", std::process::id());
    let mut next_retention_at = Instant::now();
    loop {
        let now = Instant::now();
        let apply_retention = now >= next_retention_at;
        let report = run_cycle(&store, &classifier, &owner, &args, apply_retention).await?;
        if apply_retention {
            next_retention_at = now
                .checked_add(Duration::from_secs(args.retention_interval))
                .context("retention interval exceeds monotonic clock range")?;
        }
        println!("{}", serde_json::to_string(&report)?);
        if args.once {
            return Ok(());
        }
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("failed to listen for shutdown signal")?;
                return Ok(());
            }
            () = tokio::time::sleep(Duration::from_secs(args.poll_interval)) => {}
        }
    }
}

async fn run_cycle(
    store: &CrawlStore,
    classifier: &RuleClassifier,
    owner: &str,
    args: &ClassifyArgs,
    apply_retention: bool,
) -> Result<ClassificationCycleReport> {
    let events = store
        .claim_classification_events(
            owner,
            i64::try_from(args.concurrency)?,
            i64::try_from(args.lease_duration)?,
        )
        .await?;
    let claimed = events.len();
    let mut tasks = JoinSet::new();
    for event in events {
        let store = store.clone();
        let classifier = classifier.clone();
        let owner = owner.to_owned();
        let max_attempts = args.max_attempts;
        let retry_delay =
            retry_delay_seconds(event.attempts, args.base_retry_delay, args.max_retry_delay);
        tasks.spawn(async move {
            process_event(
                &store,
                &classifier,
                &owner,
                event,
                max_attempts,
                retry_delay,
            )
            .await
        });
    }
    let mut event_reports = Vec::with_capacity(claimed);
    while let Some(result) = tasks.join_next().await {
        event_reports.push(result.context("classification task panicked")?);
    }
    event_reports.sort_by(|left, right| left.event_key.cmp(&right.event_key));
    let retention = if apply_retention {
        store
            .apply_classifier_retention(
                args.dead_letter_retention_days,
                args.processed_event_retention_days,
            )
            .await?
    } else {
        ClassifierRetentionOutcome::default()
    };
    Ok(ClassificationCycleReport {
        schema_version: "classification-worker-cycle.v1".to_owned(),
        generated_at: Utc::now().to_rfc3339(),
        classifier_version: classifier.classifier_version().to_owned(),
        config_hash: classifier.config_hash().to_owned(),
        claimed,
        rejected: count_decision(&event_reports, "rejected"),
        matched_explicit: count_decision(&event_reports, "matched_explicit"),
        manual_review: count_decision(&event_reports, "manual_review"),
        retry_scheduled: count_disposition(&event_reports, FailureDisposition::RetryScheduled),
        dead_lettered: count_disposition(&event_reports, FailureDisposition::DeadLettered),
        failed: event_reports
            .iter()
            .filter(|report| report.error.is_some())
            .count(),
        retention_applied: apply_retention,
        retention,
        events: event_reports,
    })
}

async fn process_event(
    store: &CrawlStore,
    classifier: &RuleClassifier,
    owner: &str,
    event: ClaimedClassificationEvent,
    max_attempts: u32,
    retry_delay: u64,
) -> ClassificationEventReport {
    let result = classify_payload(classifier, &event.payload).map(|(post_id, result)| {
        let decision = decision_name(&result.decision).to_owned();
        (post_id, result, decision)
    });
    match result {
        Ok((post_id, classification, decision)) => {
            match store
                .complete_classification(&event, owner, post_id, &classification)
                .await
            {
                Ok(persistence) => ClassificationEventReport {
                    event_key: event.event_key,
                    attempt: event.attempts,
                    decision: Some(decision),
                    persistence: Some(persistence),
                    failure_disposition: None,
                    error: None,
                },
                Err(error) => {
                    failure_report(
                        store,
                        owner,
                        event,
                        max_attempts,
                        retry_delay,
                        format!("failed to persist classification: {error:#}"),
                    )
                    .await
                }
            }
        }
        Err(error) => {
            failure_report(
                store,
                owner,
                event,
                max_attempts,
                retry_delay,
                error.to_string(),
            )
            .await
        }
    }
}

fn classify_payload(
    classifier: &RuleClassifier,
    payload: &serde_json::Value,
) -> Result<(i64, uth_domain::ClassificationResult)> {
    let payload: ClassificationEventPayload =
        serde_json::from_value(payload.clone()).context("invalid classification event payload")?;
    if payload.database_post_id <= 0 {
        bail!("classification event contains invalid database_post_id");
    }
    let result = classifier.classify(&payload.post, true, Utc::now())?;
    Ok((payload.database_post_id, result))
}

async fn failure_report(
    store: &CrawlStore,
    owner: &str,
    event: ClaimedClassificationEvent,
    max_attempts: u32,
    retry_delay: u64,
    error: String,
) -> ClassificationEventReport {
    let disposition = store
        .fail_classification_event(&event, owner, &error, max_attempts, retry_delay)
        .await;
    ClassificationEventReport {
        event_key: event.event_key,
        attempt: event.attempts,
        decision: None,
        persistence: None,
        failure_disposition: disposition.as_ref().ok().copied(),
        error: Some(match disposition {
            Ok(_) => error,
            Err(storage_error) => format!("{error}; failed to persist failure: {storage_error:#}"),
        }),
    }
}

fn validate_args(args: &ClassifyArgs) -> Result<()> {
    if args.concurrency == 0
        || args.lease_duration == 0
        || args.poll_interval == 0
        || args.max_attempts == 0
        || args.base_retry_delay == 0
        || args.max_retry_delay < args.base_retry_delay
        || args.dead_letter_retention_days <= 0
        || args.processed_event_retention_days <= 0
        || args.retention_interval == 0
    {
        bail!("classifier concurrency, intervals, attempts, and retry bounds must be valid");
    }
    Ok(())
}

fn retry_delay_seconds(attempt: u32, base: u64, maximum: u64) -> u64 {
    let exponent = attempt.saturating_sub(1).min(20);
    base.saturating_mul(1_u64 << exponent).min(maximum)
}

fn decision_name(decision: &ClassificationDecision) -> &'static str {
    match decision {
        ClassificationDecision::Rejected => "rejected",
        ClassificationDecision::MatchedExplicit => "matched_explicit",
        ClassificationDecision::ManualReview => "manual_review",
    }
}

fn count_decision(reports: &[ClassificationEventReport], decision: &str) -> usize {
    reports
        .iter()
        .filter(|report| report.decision.as_deref() == Some(decision))
        .count()
}

fn count_disposition(
    reports: &[ClassificationEventReport],
    disposition: FailureDisposition,
) -> usize {
    reports
        .iter()
        .filter(|report| report.failure_disposition == Some(disposition))
        .count()
}

#[cfg(test)]
mod tests {
    use super::retry_delay_seconds;

    #[test]
    fn retry_delay_is_bounded() {
        assert_eq!(retry_delay_seconds(1, 30, 900), 30);
        assert_eq!(retry_delay_seconds(2, 30, 900), 60);
        assert_eq!(retry_delay_seconds(30, 30, 900), 900);
    }
}
