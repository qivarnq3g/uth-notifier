mod browser;
mod classifier_evaluation;
mod classifier_review;
mod classifier_worker;
mod edge_reconciler;
pub mod gemini_reviewer;
mod notification_worker;
mod operational_health;
mod payos;
mod portal;
mod strategy_circuit;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use url::Url;
use uth_crawler::facebook::{DEFAULT_MINIMUM_YIELD, STRATEGIES, diff_posts, probe};
use uth_domain::CrawlReport;
use uth_storage::{
    AdaptiveSchedule, ClaimedSource, CrawlStore, PersistOutcome, ScheduledPersistOptions,
    SourceSeed,
};

use crate::strategy_circuit::{
    StrategyCircuitBreaker, StrategyCircuitPolicy, StrategyCircuitSnapshot, StrategySelection,
};

#[cfg(unix)]
struct SchedulerShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl SchedulerShutdownSignals {
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
struct SchedulerShutdownSignals;

#[cfg(not(unix))]
impl SchedulerShutdownSignals {
    fn new() -> Result<Self> {
        Ok(Self)
    }

    async fn wait(&mut self) -> Result<()> {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for shutdown signal")
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "uth-agent",
    version,
    about = "UTH Activity Notifier core agent"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Crawl a public Facebook Page without a login session")]
    Crawl(CrawlArgs),
    #[command(about = "Crawl every source in a Facebook source-selection JSON file")]
    CrawlAll(CrawlAllArgs),
    #[command(about = "Run the durable PostgreSQL-backed crawl scheduler")]
    CrawlScheduled(CrawlScheduledArgs),
    #[command(about = "Classify pending post events with explainable rules")]
    Classify(classifier_worker::ClassifyArgs),
    #[command(about = "Evaluate classifier quality against a labeled dataset")]
    EvaluateClassifier(classifier_evaluation::EvaluateClassifierArgs),
    #[command(about = "Prepare a healthy crawl report for human classifier review")]
    PrepareClassifierReview(classifier_review::PrepareClassifierReviewArgs),
    #[command(about = "Finalize human labels into a classifier evaluation dataset")]
    FinalizeClassifierReview(classifier_review::FinalizeClassifierReviewArgs),
    #[command(about = "Plan and send Telegram notifications")]
    Notify(Box<notification_worker::NotifyArgs>),
    #[command(about = "Approve one pending manual review and queue notifications")]
    ReviewSend(notification_worker::ReviewSendArgs),
    #[command(about = "Manage Telegram notification recipients")]
    Subscriber(notification_worker::SubscriberArgs),
    #[command(about = "Review Facebook Page suggestions from Telegram users")]
    Suggestion(notification_worker::SuggestionArgs),
    #[command(about = "Report PostgreSQL-backed operational health")]
    Health(operational_health::HealthArgs),
    #[command(about = "Import durable edge events into PostgreSQL and acknowledge them")]
    ReconcileEdge(edge_reconciler::ReconcileEdgeArgs),
}

#[derive(Debug, clap::Args)]
struct CrawlScheduledArgs {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,

    #[arg(help = "Source-selection JSON containing recommended_sources")]
    input: PathBuf,

    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    #[arg(long, default_value_t = 300)]
    schedule_interval: u64,

    #[arg(long)]
    no_adaptive_schedule: bool,

    #[arg(long, default_value_t = 120)]
    active_schedule_interval: u64,

    #[arg(long, default_value_t = 480)]
    idle_schedule_interval: u64,

    #[arg(long, default_value_t = 3)]
    active_unchanged_crawls: u32,

    #[arg(long, default_value_t = 6)]
    idle_after_unchanged_crawls: u32,

    #[arg(long, default_value_t = 60)]
    base_backoff: u64,

    #[arg(long, default_value_t = 3600)]
    max_backoff: u64,

    #[arg(long, default_value_t = 600)]
    lease_duration: u64,

    #[arg(long, default_value_t = 15)]
    poll_interval: u64,

    #[arg(long, default_value_t = 30)]
    run_retention_days: i32,

    #[arg(long, default_value_t = 86_400)]
    retention_interval: u64,

    #[arg(long, default_value_t = 3)]
    alert_after_failures: u32,

    #[arg(long)]
    once: bool,

    #[arg(long)]
    notify_existing_posts: bool,

    #[arg(long, default_value_t = 35)]
    timeout: u64,

    #[arg(long, default_value_t = 1)]
    minimum_yield: usize,

    #[arg(long, default_value = "auto")]
    strategy: String,

    #[arg(long, default_value_t = 10)]
    strategy_circuit_failure_threshold: u32,

    #[arg(long, default_value_t = 900)]
    strategy_circuit_cooldown: u64,

    #[arg(long, default_value_t = 86_400)]
    strategy_circuit_history_window: u64,

    #[arg(long)]
    probe_all: bool,

    #[arg(long)]
    no_browser_fallback: bool,

    #[arg(long, default_value = "node")]
    node: PathBuf,

    #[arg(long, default_value = "apps/browser-agent/src/post.ts")]
    browser_script: PathBuf,

    #[arg(long, default_value_t = 60)]
    browser_timeout: u64,

    #[arg(long, default_value_t = 2)]
    browser_retries: usize,
}

#[derive(Debug, clap::Args)]
struct CrawlArgs {
    #[arg(help = "Public Facebook Page URL")]
    url: String,

    #[arg(
        long,
        default_value_t = 10,
        help = "Maximum posts written to the report"
    )]
    limit: usize,

    #[arg(
        long,
        default_value_t = 35,
        help = "Timeout per HTTP strategy in seconds"
    )]
    timeout: u64,

    #[arg(
        long,
        default_value_t = DEFAULT_MINIMUM_YIELD,
        help = "Posts required before a strategy is healthy"
    )]
    minimum_yield: usize,

    #[arg(long, default_value = "auto", help = "Strategy name or auto")]
    strategy: String,

    #[arg(long, help = "Try every HTTP strategy even after a healthy result")]
    probe_all: bool,

    #[arg(
        long,
        help = "Run Playwright history sweep when HTTP returns a small window"
    )]
    browser_fallback: bool,

    #[arg(long, default_value = "node", help = "Node.js executable")]
    node: PathBuf,

    #[arg(
        long,
        default_value = "apps/browser-agent/src/post.ts",
        help = "Browser fallback TypeScript entrypoint"
    )]
    browser_script: PathBuf,

    #[arg(
        long,
        default_value_t = 60,
        help = "Browser fallback timeout in seconds"
    )]
    browser_timeout: u64,

    #[arg(long, default_value_t = 2, help = "Maximum browser fallback attempts")]
    browser_retries: usize,

    #[arg(long, help = "Compare with a previous crawl report")]
    baseline: Option<PathBuf>,

    #[arg(long, help = "Write JSON to this file")]
    output: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
struct CrawlAllArgs {
    #[arg(help = "Source-selection JSON containing recommended_sources")]
    input: PathBuf,

    #[arg(
        long,
        default_value = "results/crawl-all",
        help = "Directory for per-source reports and batch summary"
    )]
    output_dir: PathBuf,

    #[arg(long, default_value_t = 4, help = "Maximum concurrent sources")]
    concurrency: usize,

    #[arg(long, default_value_t = 10, help = "Maximum posts per source report")]
    limit: usize,

    #[arg(
        long,
        default_value_t = 35,
        help = "Timeout per HTTP strategy in seconds"
    )]
    timeout: u64,

    #[arg(long, default_value_t = 1, help = "Posts required for healthy status")]
    minimum_yield: usize,

    #[arg(long, default_value = "auto", help = "Strategy name or auto")]
    strategy: String,

    #[arg(long, help = "Try every HTTP strategy after a healthy result")]
    probe_all: bool,

    #[arg(long, help = "Disable Playwright fallback")]
    no_browser_fallback: bool,

    #[arg(long, default_value = "node", help = "Node.js executable")]
    node: PathBuf,

    #[arg(
        long,
        default_value = "apps/browser-agent/src/post.ts",
        help = "Browser fallback TypeScript entrypoint"
    )]
    browser_script: PathBuf,

    #[arg(
        long,
        default_value_t = 60,
        help = "Browser fallback timeout in seconds"
    )]
    browser_timeout: u64,

    #[arg(
        long,
        default_value_t = 2,
        help = "Maximum browser attempts per source"
    )]
    browser_retries: usize,
}

#[derive(Debug, Clone)]
struct CrawlSettings {
    strategies: Vec<String>,
    timeout: Duration,
    minimum_yield: usize,
    stop_after_success: bool,
    browser_fallback: bool,
    node: PathBuf,
    browser_script: PathBuf,
    browser_timeout: Duration,
    browser_retries: usize,
}

#[derive(Debug, Clone, Copy)]
struct ScheduledSourcePolicy {
    base_backoff: u64,
    max_backoff: u64,
    alert_after_failures: u32,
    notify_existing_posts: bool,
    adaptive_schedule: Option<AdaptiveSchedule>,
}

#[derive(Debug, Deserialize)]
struct SourceSelection {
    recommended_sources: Vec<SelectedSource>,
}

#[derive(Debug, Clone, Deserialize)]
struct SelectedSource {
    id: String,
    name: String,
    url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FacebookCrawlTarget {
    configured_url: String,
    presentation_url: String,
    fallback_url: Option<String>,
    presentation_kind: &'static str,
    expected_source_id: String,
}

#[derive(Debug, Serialize)]
struct BatchReport {
    schema_version: String,
    generated_at: String,
    input: String,
    output_dir: String,
    source_count: usize,
    succeeded: usize,
    failed: usize,
    browser_fallback_used: usize,
    sources: Vec<BatchSourceReport>,
}

#[derive(Debug, Serialize)]
struct BatchSourceReport {
    id: String,
    name: String,
    url: String,
    source_id: Option<String>,
    health: String,
    post_count: usize,
    selected_strategy: Option<String>,
    newest_post_at: Option<String>,
    newest_post_url: Option<String>,
    output: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SchedulerCycleReport {
    schema_version: String,
    generated_at: String,
    claimed: usize,
    healthy: usize,
    degraded: usize,
    failed: usize,
    inserted: usize,
    updated: usize,
    unchanged: usize,
    outbox_events: usize,
    retention_applied: bool,
    retention_deleted_runs: u64,
    strategy_circuits: Vec<StrategyCircuitSnapshot>,
    sources: Vec<SchedulerSourceReport>,
}

#[derive(Debug, Serialize)]
struct SchedulerSourceReport {
    source_key: String,
    source_name: String,
    crawl_presentation: String,
    health: String,
    next_delay_seconds: u64,
    consecutive_failures: u32,
    alert: bool,
    initial_crawl: bool,
    events_suppressed: bool,
    strategies_attempted: Vec<String>,
    strategies_skipped: Vec<String>,
    persistence: PersistOutcome,
    error: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Crawl(args) => crawl(args).await,
        Command::CrawlAll(args) => crawl_all(args).await,
        Command::CrawlScheduled(args) => crawl_scheduled(args).await,
        Command::Classify(args) => classifier_worker::run(args).await,
        Command::EvaluateClassifier(args) => classifier_evaluation::run(args),
        Command::PrepareClassifierReview(args) => classifier_review::run(args),
        Command::FinalizeClassifierReview(args) => classifier_review::finalize(args),
        Command::Notify(args) => notification_worker::run(*args).await,
        Command::ReviewSend(args) => notification_worker::run_review_send(args).await,
        Command::Subscriber(args) => notification_worker::run_subscriber(args).await,
        Command::Suggestion(args) => notification_worker::run_suggestion(args).await,
        Command::Health(args) => operational_health::run(args).await,
        Command::ReconcileEdge(args) => edge_reconciler::run(args).await,
    }
}

async fn crawl_scheduled(args: CrawlScheduledArgs) -> Result<()> {
    validate_scheduler_args(&args)?;
    let selection = read_source_selection(&args.input)?;
    let schedule_interval_seconds = i32::try_from(args.schedule_interval)
        .context("schedule interval exceeds PostgreSQL integer range")?;
    let seeds = selection
        .recommended_sources
        .iter()
        .map(|source| SourceSeed {
            key: facebook_source_key(&source.id),
            name: source.name.clone(),
            url: source.url.clone(),
            schedule_interval_seconds,
        })
        .collect::<Vec<_>>();
    let max_connections = u32::try_from(args.concurrency.saturating_add(2))?;
    let store = CrawlStore::connect(&args.database_url, max_connections).await?;
    store.migrate().await?;
    store.upsert_sources(&seeds).await?;
    let settings = CrawlSettings {
        strategies: resolve_strategies(&args.strategy)?,
        timeout: Duration::from_secs(args.timeout),
        minimum_yield: args.minimum_yield,
        stop_after_success: !args.probe_all,
        browser_fallback: !args.no_browser_fallback,
        node: args.node.clone(),
        browser_script: args.browser_script.clone(),
        browser_timeout: Duration::from_secs(args.browser_timeout),
        browser_retries: args.browser_retries,
    };
    let strategy_circuit = if settings.browser_fallback {
        let history = store
            .recent_crawl_strategy_health(
                args.strategy_circuit_history_window,
                args.strategy_circuit_failure_threshold,
            )
            .await?;
        Some(Arc::new(Mutex::new(StrategyCircuitBreaker::new(
            &settings.strategies,
            &history,
            StrategyCircuitPolicy {
                failure_threshold: args.strategy_circuit_failure_threshold,
                cooldown: Duration::from_secs(args.strategy_circuit_cooldown),
            },
            Instant::now(),
        ))))
    } else {
        None
    };
    let owner = format!("uth-agent-{}", std::process::id());
    let mut next_retention_at = Instant::now();
    let mut shutdown_signals = SchedulerShutdownSignals::new()?;
    loop {
        let now = Instant::now();
        let apply_retention = now >= next_retention_at;
        let report = tokio::select! {
            biased;
            result = shutdown_signals.wait() => {
                result?;
                store.release_source_leases(&owner).await?;
                return Ok(());
            }
            result = run_scheduler_cycle(
                &store,
                &owner,
                &settings,
                strategy_circuit.as_ref(),
                &args,
                apply_retention,
            ) => result?,
        };
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
            biased;
            result = shutdown_signals.wait() => {
                result?;
                store.release_source_leases(&owner).await?;
                return Ok(());
            }
            () = tokio::time::sleep(Duration::from_secs(args.poll_interval)) => {}
        }
    }
}

async fn run_scheduler_cycle(
    store: &CrawlStore,
    owner: &str,
    settings: &CrawlSettings,
    strategy_circuit: Option<&Arc<Mutex<StrategyCircuitBreaker>>>,
    args: &CrawlScheduledArgs,
    apply_retention: bool,
) -> Result<SchedulerCycleReport> {
    let claim_limit = i64::try_from(args.concurrency)?;
    let lease_seconds = i64::try_from(args.lease_duration)?;
    let claimed = store
        .claim_due_sources(owner, claim_limit, lease_seconds)
        .await?;
    let claimed_count = claimed.len();
    let mut tasks = JoinSet::new();
    for source in claimed {
        let store = store.clone();
        let owner = owner.to_owned();
        let settings = settings.clone();
        let strategy_circuit = strategy_circuit.cloned();
        let policy = ScheduledSourcePolicy {
            base_backoff: args.base_backoff,
            max_backoff: args.max_backoff,
            alert_after_failures: args.alert_after_failures,
            notify_existing_posts: args.notify_existing_posts,
            adaptive_schedule: (!args.no_adaptive_schedule).then_some(AdaptiveSchedule {
                active_interval_seconds: args.active_schedule_interval,
                idle_interval_seconds: args.idle_schedule_interval,
                active_unchanged_crawls: args.active_unchanged_crawls,
                idle_after_unchanged_crawls: args.idle_after_unchanged_crawls,
            }),
        };
        tasks.spawn(async move {
            run_scheduled_source(&store, &owner, &source, &settings, strategy_circuit, policy).await
        });
    }
    let mut sources = Vec::with_capacity(claimed_count);
    while let Some(result) = tasks.join_next().await {
        sources.push(result.context("scheduled crawl task panicked")?);
    }
    sources.sort_by(|left, right| left.source_key.cmp(&right.source_key));
    let retention_deleted_runs = if apply_retention {
        store.apply_retention(args.run_retention_days).await?
    } else {
        0
    };
    let strategy_circuits = match strategy_circuit {
        Some(strategy_circuit) => strategy_circuit.lock().await.snapshots(Instant::now()),
        None => Vec::new(),
    };
    Ok(SchedulerCycleReport {
        schema_version: "crawl-scheduler-cycle.v1".to_owned(),
        generated_at: Utc::now().to_rfc3339(),
        claimed: claimed_count,
        healthy: sources
            .iter()
            .filter(|source| source.health == "healthy")
            .count(),
        degraded: sources
            .iter()
            .filter(|source| source.health == "degraded")
            .count(),
        failed: sources
            .iter()
            .filter(|source| source.health == "failed")
            .count(),
        inserted: sources
            .iter()
            .map(|source| source.persistence.inserted)
            .sum(),
        updated: sources
            .iter()
            .map(|source| source.persistence.updated)
            .sum(),
        unchanged: sources
            .iter()
            .map(|source| source.persistence.unchanged)
            .sum(),
        outbox_events: sources
            .iter()
            .map(|source| source.persistence.outbox_events)
            .sum(),
        retention_applied: apply_retention,
        retention_deleted_runs,
        strategy_circuits,
        sources,
    })
}

async fn run_scheduled_source(
    store: &CrawlStore,
    owner: &str,
    source: &ClaimedSource,
    settings: &CrawlSettings,
    strategy_circuit: Option<Arc<Mutex<StrategyCircuitBreaker>>>,
    policy: ScheduledSourcePolicy,
) -> SchedulerSourceReport {
    let strategy_selection = match &strategy_circuit {
        Some(strategy_circuit) => strategy_circuit
            .lock()
            .await
            .select(&settings.strategies, Instant::now()),
        None => StrategySelection {
            enabled: settings.strategies.clone(),
            skipped: Vec::new(),
        },
    };
    let mut selected_settings = CrawlSettings {
        strategies: strategy_selection.enabled.clone(),
        ..settings.clone()
    };
    if source.failure_count > 0 || source.reconciliation_required {
        selected_settings.minimum_yield = selected_settings.minimum_yield.max(2);
    }
    let (crawl_result, crawl_presentation) = match facebook_crawl_target(&source.key, &source.url) {
        Ok(target) => {
            let crawl_presentation = target.presentation_kind.to_owned();
            (
                crawl_facebook_target(&target, &selected_settings).await,
                crawl_presentation,
            )
        }
        Err(error) => (Err(error), "invalid".to_owned()),
    };
    let outcomes = crawl_result
        .as_ref()
        .map(|report| {
            report
                .attempts
                .iter()
                .map(|attempt| (attempt.strategy.clone(), attempt.outcome.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    if let Some(strategy_circuit) = &strategy_circuit {
        strategy_circuit.lock().await.observe(
            &strategy_selection.enabled,
            &outcomes,
            crawl_result.is_err(),
            Instant::now(),
        );
    }
    let strategies_attempted = crawl_result
        .as_ref()
        .map(|report| {
            report
                .attempts
                .iter()
                .map(|attempt| attempt.strategy.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        })
        .unwrap_or_else(|_| strategy_selection.enabled.clone());
    match crawl_result {
        Ok(report) => {
            let healthy = report.health == "healthy";
            let next_delay_seconds = if healthy {
                source.schedule_interval_seconds
            } else {
                retry_delay_seconds(
                    &source.key,
                    source.failure_count.saturating_add(1),
                    policy.base_backoff,
                    policy.max_backoff,
                )
            };
            let health = report.health.clone();
            let consecutive_failures = if healthy {
                0
            } else {
                source.failure_count.saturating_add(1)
            };
            let events_suppressed = source.initial_crawl && !policy.notify_existing_posts;
            match store
                .persist_scheduled_report(
                    source,
                    owner,
                    &report,
                    ScheduledPersistOptions {
                        next_delay_seconds,
                        emit_post_events: !events_suppressed,
                        allow_historical_events: policy.notify_existing_posts,
                        adaptive_schedule: policy.adaptive_schedule,
                    },
                )
                .await
            {
                Ok(result) => SchedulerSourceReport {
                    source_key: source.key.clone(),
                    source_name: source.name.clone(),
                    crawl_presentation: crawl_presentation.clone(),
                    health,
                    next_delay_seconds: result.next_delay_seconds,
                    consecutive_failures,
                    alert: consecutive_failures >= policy.alert_after_failures,
                    initial_crawl: source.initial_crawl,
                    events_suppressed,
                    strategies_attempted: strategies_attempted.clone(),
                    strategies_skipped: strategy_selection.skipped.clone(),
                    persistence: result.persistence,
                    error: None,
                },
                Err(error) => SchedulerSourceReport {
                    source_key: source.key.clone(),
                    source_name: source.name.clone(),
                    crawl_presentation: crawl_presentation.clone(),
                    health: "failed".to_owned(),
                    next_delay_seconds,
                    consecutive_failures,
                    alert: true,
                    initial_crawl: source.initial_crawl,
                    events_suppressed,
                    strategies_attempted: strategies_attempted.clone(),
                    strategies_skipped: strategy_selection.skipped.clone(),
                    persistence: PersistOutcome::default(),
                    error: Some(format!("failed to persist crawl report: {error:#}")),
                },
            }
        }
        Err(error) => {
            let next_delay_seconds = retry_delay_seconds(
                &source.key,
                source.failure_count.saturating_add(1),
                policy.base_backoff,
                policy.max_backoff,
            );
            let error = error.to_string();
            let consecutive_failures = source.failure_count.saturating_add(1);
            let persistence_error = store
                .persist_failure(source, owner, &error, next_delay_seconds)
                .await
                .err();
            SchedulerSourceReport {
                source_key: source.key.clone(),
                source_name: source.name.clone(),
                crawl_presentation,
                health: "failed".to_owned(),
                next_delay_seconds,
                consecutive_failures,
                alert: persistence_error.is_some()
                    || consecutive_failures >= policy.alert_after_failures,
                initial_crawl: source.initial_crawl,
                events_suppressed: source.initial_crawl && !policy.notify_existing_posts,
                strategies_attempted,
                strategies_skipped: strategy_selection.skipped,
                persistence: PersistOutcome::default(),
                error: Some(match persistence_error {
                    Some(persistence_error) => {
                        format!("{error}; failed to persist failure: {persistence_error:#}")
                    }
                    None => error,
                }),
            }
        }
    }
}

fn read_source_selection(path: &Path) -> Result<SourceSelection> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let selection = serde_json::from_str(raw.trim_start_matches('\u{feff}'))
        .with_context(|| format!("invalid source-selection JSON {}", path.display()))?;
    Ok(selection)
}

fn validate_scheduler_args(args: &CrawlScheduledArgs) -> Result<()> {
    validate_limits(1, args.minimum_yield, args.concurrency)?;
    if args.schedule_interval == 0
        || args.base_backoff == 0
        || args.max_backoff < args.base_backoff
        || args.lease_duration == 0
        || args.poll_interval == 0
        || args.run_retention_days <= 0
        || args.retention_interval == 0
        || args.alert_after_failures == 0
        || args.strategy_circuit_failure_threshold == 0
        || args.strategy_circuit_cooldown == 0
        || args.strategy_circuit_history_window == 0
    {
        bail!(
            "scheduler intervals, backoff, lease, poll, retention, and strategy circuit settings must be valid"
        );
    }
    if !args.no_adaptive_schedule
        && (args.active_schedule_interval == 0
            || args.active_schedule_interval > args.schedule_interval
            || args.schedule_interval > args.idle_schedule_interval
            || args.active_unchanged_crawls == 0
            || args.idle_after_unchanged_crawls <= args.active_unchanged_crawls)
    {
        bail!(
            "adaptive schedule must satisfy active <= normal <= idle and 0 < active-unchanged-crawls < idle-after-unchanged-crawls"
        );
    }
    if !args.no_browser_fallback && args.browser_retries == 0 {
        bail!("browser-retries must be at least 1 when browser fallback is enabled");
    }
    let required_lease = required_scheduler_lease_seconds(
        args.timeout,
        args.browser_timeout,
        args.browser_retries,
        args.no_browser_fallback,
    );
    if args.lease_duration < required_lease {
        bail!("lease-duration must be at least {required_lease} seconds for configured timeouts");
    }
    Ok(())
}

fn required_scheduler_lease_seconds(
    timeout: u64,
    browser_timeout: u64,
    browser_retries: usize,
    no_browser_fallback: bool,
) -> u64 {
    let browser_budget = if no_browser_fallback {
        0
    } else {
        browser_timeout.saturating_mul(browser_retries as u64)
    };
    timeout
        .saturating_mul(STRATEGIES.len() as u64)
        .saturating_add(browser_budget)
        .saturating_mul(2)
        .saturating_add(30)
}

fn retry_delay_seconds(source_key: &str, failure_count: u32, base: u64, maximum: u64) -> u64 {
    let exponent = failure_count.saturating_sub(1).min(20);
    let backoff = base.saturating_mul(1_u64 << exponent).min(maximum);
    let jitter_window = (backoff / 5).max(1);
    let hash = source_key
        .bytes()
        .fold(1_469_598_103_934_665_603_u64, |state, byte| {
            state
                .wrapping_mul(1_099_511_628_211)
                .wrapping_add(u64::from(byte))
        });
    backoff
        .saturating_sub(jitter_window / 2)
        .saturating_add(hash % jitter_window)
        .min(maximum)
}

async fn crawl(args: CrawlArgs) -> Result<()> {
    validate_limits(args.limit, args.minimum_yield, 1)?;
    if args.browser_fallback && args.browser_retries == 0 {
        bail!("browser-retries must be at least 1 when browser fallback is enabled");
    }
    let settings = CrawlSettings {
        strategies: resolve_strategies(&args.strategy)?,
        timeout: Duration::from_secs(args.timeout),
        minimum_yield: args.minimum_yield,
        stop_after_success: !args.probe_all,
        browser_fallback: args.browser_fallback,
        node: args.node,
        browser_script: args.browser_script,
        browser_timeout: Duration::from_secs(args.browser_timeout),
        browser_retries: args.browser_retries,
    };
    let mut report = crawl_source(&args.url, &settings).await?;

    if let Some(path) = args.baseline {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read baseline {}", path.display()))?;
        let previous: Value = serde_json::from_str(raw.trim_start_matches('\u{feff}'))
            .with_context(|| format!("invalid baseline JSON {}", path.display()))?;
        report.changes = Some(diff_posts(&report.posts, &previous));
    }

    let failed = report.posts.is_empty();
    report.posts.truncate(args.limit);
    if let Some(path) = args.output {
        write_json(&path, &report)?;
        println!("{}", path.canonicalize()?.display());
    } else {
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    if failed {
        bail!("crawl failed: no usable public posts");
    }
    Ok(())
}

async fn crawl_all(args: CrawlAllArgs) -> Result<()> {
    validate_limits(args.limit, args.minimum_yield, args.concurrency)?;
    if !args.no_browser_fallback && args.browser_retries == 0 {
        bail!("browser-retries must be at least 1 when browser fallback is enabled");
    }
    let raw = fs::read_to_string(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;
    let selection: SourceSelection = serde_json::from_str(raw.trim_start_matches('\u{feff}'))
        .with_context(|| format!("invalid source-selection JSON {}", args.input.display()))?;
    if selection.recommended_sources.is_empty() {
        bail!("source selection contains no recommended_sources");
    }
    fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("failed to create {}", args.output_dir.display()))?;

    let settings = CrawlSettings {
        strategies: resolve_strategies(&args.strategy)?,
        timeout: Duration::from_secs(args.timeout),
        minimum_yield: args.minimum_yield,
        stop_after_success: !args.probe_all,
        browser_fallback: !args.no_browser_fallback,
        node: args.node,
        browser_script: args.browser_script,
        browser_timeout: Duration::from_secs(args.browser_timeout),
        browser_retries: args.browser_retries,
    };
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let mut tasks = JoinSet::new();
    for (index, source) in selection.recommended_sources.into_iter().enumerate() {
        let semaphore = Arc::clone(&semaphore);
        let settings = settings.clone();
        let output_dir = args.output_dir.clone();
        tasks.spawn(async move {
            let permit = semaphore.acquire_owned().await;
            let result = match permit {
                Ok(permit) => {
                    let result =
                        crawl_batch_source(source, &output_dir, &settings, args.limit).await;
                    drop(permit);
                    result
                }
                Err(error) => BatchSourceReport {
                    id: source.id,
                    name: source.name,
                    url: source.url,
                    source_id: None,
                    health: "failed".to_owned(),
                    post_count: 0,
                    selected_strategy: None,
                    newest_post_at: None,
                    newest_post_url: None,
                    output: None,
                    error: Some(error.to_string()),
                },
            };
            (index, result)
        });
    }

    let mut completed = Vec::new();
    while let Some(result) = tasks.join_next().await {
        completed.push(result.context("batch crawl task failed")?);
    }
    completed.sort_by_key(|(index, _)| *index);
    let sources = completed
        .into_iter()
        .map(|(_, source)| source)
        .collect::<Vec<_>>();
    let succeeded = sources
        .iter()
        .filter(|source| source.post_count > 0)
        .count();
    let browser_fallback_used = sources
        .iter()
        .filter(|source| source.selected_strategy.as_deref() == Some("browser_playwright"))
        .count();
    let batch = BatchReport {
        schema_version: "facebook-crawl-batch-report.v1".to_owned(),
        generated_at: Utc::now().to_rfc3339(),
        input: args.input.display().to_string(),
        output_dir: args.output_dir.display().to_string(),
        source_count: sources.len(),
        succeeded,
        failed: sources.len() - succeeded,
        browser_fallback_used,
        sources,
    };
    let summary_path = args.output_dir.join("summary.json");
    write_json(&summary_path, &batch)?;
    println!("{}", summary_path.canonicalize()?.display());
    if batch.failed > 0 {
        bail!("{} of {} sources failed", batch.failed, batch.source_count);
    }
    Ok(())
}

async fn crawl_batch_source(
    source: SelectedSource,
    output_dir: &Path,
    settings: &CrawlSettings,
    limit: usize,
) -> BatchSourceReport {
    let source_key = facebook_source_key(&source.id);
    let crawl_result = match facebook_crawl_target(&source_key, &source.url) {
        Ok(target) => crawl_facebook_target(&target, settings).await,
        Err(error) => Err(error),
    };
    match crawl_result {
        Ok(mut report) => {
            let post_count = report.post_count;
            let newest_post_at = report.posts.first().map(|post| post.published_at.clone());
            let newest_post_url = report.posts.first().map(|post| post.canonical_url.clone());
            let error = (post_count == 0).then(|| {
                report
                    .attempts
                    .iter()
                    .rev()
                    .find_map(|attempt| attempt.error.clone())
                    .or_else(|| {
                        report.attempts.last().map(|attempt| {
                            format!("no usable posts; final outcome {}", attempt.outcome)
                        })
                    })
                    .unwrap_or_else(|| "no crawl attempts recorded".to_owned())
            });
            report.posts.truncate(limit);
            let output_path = output_dir.join(format!("{}.json", safe_file_stem(&source.id)));
            let write_result = write_json(&output_path, &report);
            BatchSourceReport {
                id: source.id,
                name: source.name,
                url: source.url,
                source_id: Some(report.source_id),
                health: report.health,
                post_count,
                selected_strategy: report.selected_strategy,
                newest_post_at,
                newest_post_url,
                output: write_result
                    .as_ref()
                    .ok()
                    .map(|()| output_path.display().to_string()),
                error: write_result.err().map(|error| error.to_string()).or(error),
            }
        }
        Err(error) => BatchSourceReport {
            id: source.id,
            name: source.name,
            url: source.url,
            source_id: None,
            health: "failed".to_owned(),
            post_count: 0,
            selected_strategy: None,
            newest_post_at: None,
            newest_post_url: None,
            output: None,
            error: Some(error.to_string()),
        },
    }
}

fn facebook_crawl_target(source_key: &str, configured_url: &str) -> Result<FacebookCrawlTarget> {
    let expected_source_id = facebook_source_key(source_key);
    let numeric_id = expected_source_id
        .strip_prefix("facebook:")
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .context("Facebook source key must contain a verified numeric page ID")?;
    let parsed = Url::parse(configured_url).context("configured Facebook source URL is invalid")?;
    let host = parsed
        .host_str()
        .map(str::to_ascii_lowercase)
        .context("configured Facebook source URL has no host")?;
    if parsed.scheme() != "https"
        || !matches!(
            host.as_str(),
            "facebook.com" | "www.facebook.com" | "m.facebook.com" | "mbasic.facebook.com"
        )
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
    {
        bail!("configured source must be an unauthenticated HTTPS Facebook URL");
    }
    let configured_numeric_route = parsed
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>())
        .is_some_and(|segments| {
            segments.len() >= 3
                && segments[0].eq_ignore_ascii_case("people")
                && segments[2] == numeric_id
        });
    let numeric_profile_url = format!("https://www.facebook.com/profile.php?id={numeric_id}");
    let alias = parsed
        .path()
        .split('/')
        .find(|segment| !segment.is_empty())
        .filter(|segment| {
            !matches!(
                segment.to_ascii_lowercase().as_str(),
                "people" | "pages" | "profile.php"
            )
        });
    let (presentation_url, fallback_url, presentation_kind) = if configured_numeric_route {
        (configured_url.to_owned(), None, "configured_numeric_route")
    } else if let Some(alias) = alias {
        (
            format!("https://www.facebook.com/people/{alias}/{numeric_id}/"),
            Some(numeric_profile_url),
            "numeric_people_with_profile_fallback",
        )
    } else {
        (numeric_profile_url, None, "numeric_profile")
    };
    Ok(FacebookCrawlTarget {
        configured_url: configured_url.to_owned(),
        presentation_url,
        fallback_url,
        presentation_kind,
        expected_source_id,
    })
}

async fn crawl_facebook_target(
    target: &FacebookCrawlTarget,
    settings: &CrawlSettings,
) -> Result<CrawlReport> {
    let report = crawl_source(&target.presentation_url, settings).await?;
    let mut report = finalize_facebook_crawl_report(report, target)?;
    if report.health != "healthy"
        && let Some(fallback_url) = &target.fallback_url
    {
        let fallback_report = crawl_source(fallback_url, settings).await?;
        let fallback_report = finalize_facebook_crawl_report(fallback_report, target)?;
        report = merge_presentation_reports(report, fallback_report, settings.minimum_yield);
    }
    Ok(report)
}

fn merge_presentation_reports(
    mut primary: CrawlReport,
    mut fallback: CrawlReport,
    minimum_yield: usize,
) -> CrawlReport {
    let fallback_is_better = report_quality(&fallback) > report_quality(&primary);
    let mut attempts = std::mem::take(&mut primary.attempts);
    attempts.append(&mut fallback.attempts);
    let alternate_posts = if fallback_is_better {
        std::mem::take(&mut primary.posts)
    } else {
        std::mem::take(&mut fallback.posts)
    };
    let mut selected = if fallback_is_better {
        fallback
    } else {
        primary
    };
    for post in alternate_posts {
        if let Some(existing) = selected.posts.iter_mut().find(|existing| {
            existing.external_post_id == post.external_post_id
                || existing.canonical_url == post.canonical_url
                || (existing.published_at == post.published_at
                    && existing.content_hash == post.content_hash)
        }) {
            if post.text.len() > existing.text.len() {
                *existing = post;
            }
        } else {
            selected.posts.push(post);
        }
    }
    selected
        .posts
        .sort_by(|left, right| right.published_at.cmp(&left.published_at));
    selected.attempts = attempts;
    selected.post_count = selected.posts.len();
    if !selected.posts.is_empty() {
        selected.health = if selected.post_count >= minimum_yield {
            "healthy".to_owned()
        } else {
            "degraded".to_owned()
        };
    }
    selected
}

fn report_quality(report: &CrawlReport) -> (u8, usize) {
    let health = match report.health.as_str() {
        "healthy" => 3,
        "degraded" => 2,
        "failed" => 1,
        _ => 0,
    };
    (health, report.post_count)
}

fn finalize_facebook_crawl_report(
    mut report: CrawlReport,
    target: &FacebookCrawlTarget,
) -> Result<CrawlReport> {
    if report.source_id != target.expected_source_id {
        bail!("crawl presentation returned an unexpected Facebook source ID");
    }
    if report
        .posts
        .iter()
        .any(|post| post.source_id != target.expected_source_id)
    {
        bail!("crawl presentation returned a post from another Facebook source");
    }
    report.source_url.clone_from(&target.configured_url);
    Ok(report)
}

async fn crawl_source(url: &str, settings: &CrawlSettings) -> Result<CrawlReport> {
    let mut report = probe(
        url,
        &settings.strategies,
        settings.timeout,
        settings.stop_after_success,
        settings.minimum_yield,
        None,
    )
    .await?;
    let browser_verification_required = url.to_ascii_lowercase().contains("facebook.com/people/");
    if settings.browser_fallback
        && (report.posts.len() < browser::BROWSER_HISTORY_TARGET || browser_verification_required)
    {
        for _ in 0..settings.browser_retries {
            browser::apply_browser_fallback(
                &mut report,
                &settings.node,
                &settings.browser_script,
                settings.browser_timeout,
                settings.minimum_yield,
            )
            .await;
            if report.selected_strategy.as_deref() == Some("browser_playwright") {
                break;
            }
        }
        if browser_verification_required
            && report.selected_strategy.as_deref() != Some("browser_playwright")
        {
            report.health = "degraded".to_owned();
        }
    }
    Ok(report)
}

fn resolve_strategies(strategy: &str) -> Result<Vec<String>> {
    if strategy == "auto" {
        Ok(STRATEGIES.iter().map(|value| (*value).to_owned()).collect())
    } else if STRATEGIES.contains(&strategy) {
        Ok(vec![strategy.to_owned()])
    } else {
        bail!(
            "unknown strategy; expected auto or one of: {}",
            STRATEGIES.join(", ")
        )
    }
}

fn facebook_source_key(id: &str) -> String {
    let id = id.trim();
    if id.starts_with("facebook:") {
        id.to_owned()
    } else {
        format!("facebook:{id}")
    }
}

fn validate_limits(limit: usize, minimum_yield: usize, concurrency: usize) -> Result<()> {
    if limit == 0 || minimum_yield == 0 || concurrency == 0 {
        bail!("limit, minimum-yield, and concurrency must be at least 1");
    }
    Ok(())
}

fn safe_file_stem(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "source".to_owned()
    } else {
        sanitized
    }
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| *parent != Path::new("")) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = serde_json::to_string_pretty(value)? + "\n";
    fs::write(path, rendered).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use uth_domain::{Attempt, CrawlReport, FacebookPost, POST_SCHEMA_VERSION, ParseStats};

    use super::{
        facebook_crawl_target, facebook_source_key, finalize_facebook_crawl_report,
        merge_presentation_reports, required_scheduler_lease_seconds, retry_delay_seconds,
    };

    #[test]
    fn normalizes_facebook_source_key_once() {
        assert_eq!(facebook_source_key("123"), "facebook:123");
        assert_eq!(facebook_source_key("facebook:123"), "facebook:123");
    }

    #[test]
    fn derives_numeric_people_presentation_with_profile_fallback() {
        let target = facebook_crawl_target(
            "facebook:100064352813128",
            "https://www.facebook.com/clbangdtt/",
        )
        .unwrap();

        assert_eq!(
            target.presentation_url,
            "https://www.facebook.com/people/clbangdtt/100064352813128/"
        );
        assert_eq!(
            target.fallback_url.as_deref(),
            Some("https://www.facebook.com/profile.php?id=100064352813128")
        );
        assert_eq!(
            target.presentation_kind,
            "numeric_people_with_profile_fallback"
        );
        assert_eq!(target.expected_source_id, "facebook:100064352813128");
        assert_eq!(target.configured_url, "https://www.facebook.com/clbangdtt/");
    }

    #[test]
    fn preserves_verified_people_presentation() {
        let configured_url = "https://www.facebook.com/people/Test-Page/100064352813128/";
        let target = facebook_crawl_target("facebook:100064352813128", configured_url).unwrap();

        assert_eq!(target.presentation_url, configured_url);
        assert_eq!(target.fallback_url, None);
        assert_eq!(target.presentation_kind, "configured_numeric_route");
    }

    #[test]
    fn preserves_numeric_profile_when_no_alias_is_configured() {
        let configured_url = "https://www.facebook.com/profile.php?id=100064352813128";
        let target = facebook_crawl_target("facebook:100064352813128", configured_url).unwrap();

        assert_eq!(target.presentation_url, configured_url);
        assert_eq!(target.fallback_url, None);
        assert_eq!(target.presentation_kind, "numeric_profile");
    }

    #[test]
    fn production_source_config_has_verified_numeric_presentations() {
        let selection: super::SourceSelection =
            serde_json::from_str(include_str!("../../../config/facebook-sources.v1.json")).unwrap();
        let targets = selection
            .recommended_sources
            .iter()
            .map(|source| facebook_crawl_target(&source.id, &source.url).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(targets.len(), 43);
        assert_eq!(
            targets
                .iter()
                .filter(|target| target.presentation_kind == "configured_numeric_route")
                .count(),
            3
        );
        assert_eq!(
            targets
                .iter()
                .filter(|target| {
                    target.presentation_kind == "numeric_people_with_profile_fallback"
                })
                .count(),
            40
        );
    }

    #[test]
    fn scheduler_lease_covers_primary_and_fallback_presentations() {
        assert_eq!(required_scheduler_lease_seconds(35, 60, 2, false), 550);
        assert_eq!(required_scheduler_lease_seconds(35, 60, 2, true), 310);
    }

    #[test]
    fn presentation_fallback_keeps_both_attempt_sequences() {
        let primary = test_crawl_report("degraded", "primary");
        let fallback = test_crawl_report("healthy", "fallback");

        let merged = merge_presentation_reports(primary, fallback, 1);

        assert_eq!(merged.health, "healthy");
        assert_eq!(merged.selected_strategy.as_deref(), Some("fallback"));
        assert_eq!(merged.attempts.len(), 2);
        assert_eq!(merged.attempts[0].strategy, "primary");
        assert_eq!(merged.attempts[1].strategy, "fallback");
    }

    #[test]
    fn presentation_fallback_combines_distinct_sparse_windows() {
        let mut primary = test_crawl_report("degraded", "primary");
        primary
            .posts
            .push(test_facebook_post("101", "2026-09-01T03:00:00Z"));
        primary.post_count = 1;
        let mut fallback = test_crawl_report("degraded", "fallback");
        fallback
            .posts
            .push(test_facebook_post("100", "2026-09-01T02:00:00Z"));
        fallback.post_count = 1;

        let merged = merge_presentation_reports(primary, fallback, 2);

        assert_eq!(merged.health, "healthy");
        assert_eq!(merged.post_count, 2);
        assert_eq!(merged.posts[0].external_post_id, "101");
        assert_eq!(merged.posts[1].external_post_id, "100");
        assert_eq!(merged.attempts.len(), 2);
    }

    fn test_facebook_post(external_post_id: &str, published_at: &str) -> FacebookPost {
        FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: "facebook:100064352813128".to_owned(),
            platform: "facebook".to_owned(),
            external_post_id: external_post_id.to_owned(),
            canonical_url: format!(
                "https://www.facebook.com/100064352813128/posts/{external_post_id}"
            ),
            published_at: published_at.to_owned(),
            text: format!("Post {external_post_id}"),
            media: Vec::new(),
            outbound_links: Vec::new(),
            content_hash: format!("hash-{external_post_id}"),
            crawl_strategy: "browser_playwright".to_owned(),
            fetched_at: "2026-09-01T04:00:00Z".to_owned(),
        }
    }

    fn test_crawl_report(health: &str, strategy: &str) -> CrawlReport {
        CrawlReport {
            schema_version: "facebook-crawl-report.v1".to_owned(),
            source_url: "https://www.facebook.com/example/".to_owned(),
            source_id: "facebook:100064352813128".to_owned(),
            fetched_at: "2026-07-29T00:00:00Z".to_owned(),
            selected_strategy: Some(strategy.to_owned()),
            health: health.to_owned(),
            post_count: 0,
            attempts: vec![Attempt {
                strategy: strategy.to_owned(),
                outcome: health.to_owned(),
                status: None,
                latency_ms: 1,
                bytes_received: 0,
                final_url: None,
                posts_found: 0,
                newest_post_at: None,
                parse: ParseStats::default(),
                error: None,
                browser: None,
            }],
            posts: Vec::new(),
            changes: None,
        }
    }

    #[test]
    fn rejects_unverified_or_non_facebook_crawl_targets() {
        assert!(
            facebook_crawl_target("facebook:clbangdtt", "https://www.facebook.com/clbangdtt/")
                .is_err()
        );
        assert!(
            facebook_crawl_target("facebook:100064352813128", "https://example.com/page").is_err()
        );
        assert!(
            facebook_crawl_target(
                "facebook:100064352813128",
                "http://www.facebook.com/clbangdtt/"
            )
            .is_err()
        );
    }

    #[test]
    fn restores_configured_url_without_changing_numeric_source_identity() {
        let target = facebook_crawl_target(
            "facebook:100064352813128",
            "https://www.facebook.com/clbangdtt/",
        )
        .unwrap();
        let report = CrawlReport {
            schema_version: "facebook-crawl-report.v1".to_owned(),
            source_url: target.presentation_url.clone(),
            source_id: target.expected_source_id.clone(),
            fetched_at: "2026-07-29T00:00:00Z".to_owned(),
            selected_strategy: None,
            health: "failed".to_owned(),
            post_count: 0,
            attempts: Vec::new(),
            posts: Vec::new(),
            changes: None,
        };

        let restored = finalize_facebook_crawl_report(report, &target).unwrap();

        assert_eq!(restored.source_url, target.configured_url);
        assert_eq!(restored.source_id, target.expected_source_id);
    }

    #[test]
    fn retry_delay_is_bounded_and_increases() {
        let first = retry_delay_seconds("source-a", 1, 60, 3_600);
        let second = retry_delay_seconds("source-a", 2, 60, 3_600);
        let saturated = retry_delay_seconds("source-a", 30, 60, 3_600);

        assert!((54..=65).contains(&first));
        assert!(second > first);
        assert!(saturated <= 3_600);
    }

    #[test]
    fn retry_delay_has_stable_per_source_jitter() {
        let first = retry_delay_seconds("source-a", 3, 60, 3_600);
        let repeated = retry_delay_seconds("source-a", 3, 60, 3_600);
        let other = retry_delay_seconds("source-b", 3, 60, 3_600);

        assert_eq!(first, repeated);
        assert_ne!(first, other);
    }
}
