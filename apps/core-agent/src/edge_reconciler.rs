use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Args;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;
use uth_domain::EdgeEvent;
use uth_storage::{CrawlStore, EdgeImportOutcome};

#[derive(Debug, Args)]
pub struct ReconcileEdgeArgs {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,

    #[arg(long, env = "EDGE_URL")]
    edge_url: Url,

    #[arg(long, env = "EDGE_SYNC_TOKEN", hide_env_values = true)]
    sync_token: String,

    #[arg(long, default_value_t = 100)]
    batch_size: usize,

    #[arg(long, default_value_t = 60)]
    lease_duration: u64,

    #[arg(long, default_value_t = 15)]
    poll_interval: u64,

    #[arg(long, default_value_t = 15)]
    request_timeout: u64,

    #[arg(long, default_value_t = 30)]
    processed_retention_days: i32,

    #[arg(long)]
    once: bool,
}

#[derive(Debug, Deserialize)]
struct PullResponse {
    events: Vec<EdgeEvent>,
}

#[derive(Debug, Serialize)]
struct AckRequest<'a> {
    owner: &'a str,
    event_ids: &'a [String],
}

#[derive(Debug, Deserialize)]
struct AckResponse {
    acknowledged: usize,
}

#[derive(Debug, Serialize)]
struct ReconcileReport {
    schema_version: &'static str,
    pulled: usize,
    imported: usize,
    duplicates: usize,
    acknowledged: usize,
    retained_deleted: u64,
}

pub async fn run(args: ReconcileEdgeArgs) -> Result<()> {
    validate_args(&args)?;
    let store = CrawlStore::connect(&args.database_url, 3).await?;
    store.migrate().await?;
    let client = Client::builder()
        .timeout(Duration::from_secs(args.request_timeout))
        .user_agent("uth-notifier-edge-reconciler/0.1")
        .build()?;
    let owner = format!("uth-edge-{}", std::process::id());
    loop {
        let events = pull_events(&client, &args, &owner).await?;
        let pulled = events.len();
        let outcome = store.import_edge_events(&events).await?;
        let event_ids = events
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>();
        if !event_ids.is_empty() {
            acknowledge_events(&client, &args, &owner, &event_ids).await?;
        }
        let retained_deleted = store
            .apply_edge_inbox_retention(args.processed_retention_days)
            .await?;
        println!(
            "{}",
            serde_json::to_string(&report(pulled, outcome, event_ids.len(), retained_deleted))?
        );
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

async fn pull_events(
    client: &Client,
    args: &ReconcileEdgeArgs,
    owner: &str,
) -> Result<Vec<EdgeEvent>> {
    let mut url = args.edge_url.clone();
    url.set_path("/internal/events");
    url.set_query(None);
    url.query_pairs_mut()
        .append_pair("owner", owner)
        .append_pair("limit", &args.batch_size.to_string())
        .append_pair("lease_seconds", &args.lease_duration.to_string());
    for attempt in 1..=3_u64 {
        match client
            .get(url.clone())
            .bearer_auth(&args.sync_token)
            .send()
            .await
        {
            Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                bail!("edge service rejected EDGE_SYNC_TOKEN");
            }
            Ok(response) if response.status().is_success() => {
                let response = response
                    .json::<PullResponse>()
                    .await
                    .context("edge service returned an invalid pull response")?;
                if response.events.len() > args.batch_size {
                    bail!("edge service returned more events than requested");
                }
                for event in &response.events {
                    event.validate().map_err(anyhow::Error::msg)?;
                }
                return Ok(response.events);
            }
            Ok(response) if response.status().is_server_error() || response.status() == 429 => {}
            Ok(response) => bail!("edge pull failed with HTTP {}", response.status()),
            Err(_) if attempt < 3 => {}
            Err(error) => return Err(error).context("edge pull failed after bounded retries"),
        }
        tokio::time::sleep(Duration::from_secs(attempt)).await;
    }
    bail!("edge pull failed after bounded retries")
}

async fn acknowledge_events(
    client: &Client,
    args: &ReconcileEdgeArgs,
    owner: &str,
    event_ids: &[String],
) -> Result<()> {
    let mut url = args.edge_url.clone();
    url.set_path("/internal/ack");
    url.set_query(None);
    let body = AckRequest { owner, event_ids };
    for attempt in 1..=3_u64 {
        match client
            .post(url.clone())
            .bearer_auth(&args.sync_token)
            .json(&body)
            .send()
            .await
        {
            Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                bail!("edge service rejected EDGE_SYNC_TOKEN");
            }
            Ok(response) if response.status().is_success() => {
                let acknowledged = response
                    .json::<AckResponse>()
                    .await
                    .context("edge service returned an invalid acknowledgement")?
                    .acknowledged;
                if acknowledged != event_ids.len() {
                    bail!(
                        "edge service acknowledged {acknowledged} of {} events",
                        event_ids.len()
                    );
                }
                return Ok(());
            }
            Ok(response) if response.status().is_server_error() || response.status() == 429 => {}
            Ok(response) => bail!(
                "edge acknowledgement failed with HTTP {}",
                response.status()
            ),
            Err(_) if attempt < 3 => {}
            Err(error) => {
                return Err(error).context("edge acknowledgement failed after bounded retries");
            }
        }
        tokio::time::sleep(Duration::from_secs(attempt)).await;
    }
    bail!("edge acknowledgement failed after bounded retries")
}

fn validate_args(args: &ReconcileEdgeArgs) -> Result<()> {
    if args.edge_url.scheme() != "https" && args.edge_url.host_str() != Some("127.0.0.1") {
        bail!("edge URL must use HTTPS except for local loopback development");
    }
    if args.sync_token.len() < 32
        || args.sync_token.len() > 256
        || args.batch_size == 0
        || args.batch_size > 100
        || args.lease_duration == 0
        || args.lease_duration > 300
        || args.poll_interval == 0
        || args.request_timeout == 0
        || args.processed_retention_days < 1
    {
        bail!("edge reconciler arguments are outside safe bounds");
    }
    Ok(())
}

fn report(
    pulled: usize,
    outcome: EdgeImportOutcome,
    acknowledged: usize,
    retained_deleted: u64,
) -> ReconcileReport {
    ReconcileReport {
        schema_version: "edge-reconcile-cycle.v1",
        pulled,
        imported: outcome.imported,
        duplicates: outcome.duplicates,
        acknowledged,
        retained_deleted,
    }
}
