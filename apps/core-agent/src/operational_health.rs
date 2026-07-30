use anyhow::{Result, bail};
use uth_storage::CrawlStore;

#[derive(Debug, clap::Args)]
pub struct HealthArgs {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,

    #[arg(long, default_value_t = 3)]
    alert_after_failures: u32,

    #[arg(long, default_value_t = 900)]
    backlog_stale_seconds: u64,

    #[arg(long)]
    require_healthy: bool,
}

pub async fn run(args: HealthArgs) -> Result<()> {
    let store = CrawlStore::connect(&args.database_url, 2).await?;
    store.migrate().await?;
    let health = store
        .operational_health(args.alert_after_failures, args.backlog_stale_seconds)
        .await?;
    println!("{}", serde_json::to_string_pretty(&health)?);
    if args.require_healthy && health.status != "healthy" {
        bail!("operational health is {}", health.status);
    }
    Ok(())
}
