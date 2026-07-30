use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::process::Command;
use tokio::time::timeout;
use uth_crawler::facebook::{PostHint, classify_outcome, extract_posts_with_hint};
use uth_domain::{Attempt, BrowserAttemptMetadata, CrawlReport, FacebookPost, ParseStats};

const BROWSER_STRATEGY: &str = "browser_playwright";
pub const BROWSER_HISTORY_TARGET: usize = 20;

#[derive(Debug, Deserialize)]
struct BrowserSnapshot {
    schema_version: String,
    source_url: String,
    final_url: String,
    fetched_at: String,
    status: Option<u16>,
    latency_ms: u128,
    discovered_post_url: Option<String>,
    discovered_published_at: Option<String>,
    discovered_external_post_id: Option<String>,
    discovered_text: Option<String>,
    #[serde(default)]
    network_requested_mode: Option<String>,
    #[serde(default)]
    network_effective_mode: Option<String>,
    #[serde(default)]
    network_remote_family: Option<String>,
    #[serde(default)]
    network_fallback_reason: Option<String>,
    #[serde(default)]
    login_overlay_detected: Option<bool>,
    #[serde(default)]
    login_overlay_dismissed: Option<bool>,
    #[serde(default)]
    login_route_detected: Option<bool>,
    #[serde(default)]
    discovered_post_origin: Option<String>,
    #[serde(default)]
    newest_dom_post_unresolved: Option<bool>,
    html: String,
}

fn browser_attempt_metadata(snapshot: &BrowserSnapshot) -> Option<BrowserAttemptMetadata> {
    if snapshot.network_requested_mode.is_none()
        && snapshot.network_effective_mode.is_none()
        && snapshot.network_remote_family.is_none()
        && snapshot.network_fallback_reason.is_none()
        && snapshot.login_overlay_detected.is_none()
        && snapshot.login_overlay_dismissed.is_none()
        && snapshot.login_route_detected.is_none()
        && snapshot.discovered_post_origin.is_none()
        && snapshot.newest_dom_post_unresolved.is_none()
    {
        return None;
    }
    Some(BrowserAttemptMetadata {
        network_requested_mode: snapshot
            .network_requested_mode
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        network_effective_mode: snapshot
            .network_effective_mode
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        network_remote_family: snapshot
            .network_remote_family
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        network_fallback_reason: snapshot.network_fallback_reason.clone(),
        login_overlay_detected: snapshot.login_overlay_detected.unwrap_or(false),
        login_overlay_dismissed: snapshot.login_overlay_dismissed.unwrap_or(false),
        login_route_detected: snapshot.login_route_detected.unwrap_or(false),
        discovered_post_origin: snapshot.discovered_post_origin.clone(),
        newest_dom_post_unresolved: snapshot.newest_dom_post_unresolved.unwrap_or(false),
    })
}

pub async fn apply_browser_fallback(
    report: &mut CrawlReport,
    node: &Path,
    script: &Path,
    browser_timeout: Duration,
    minimum_yield: usize,
) {
    let started = Instant::now();
    let mut command = Command::new(node);
    if script.extension().and_then(|extension| extension.to_str()) == Some("ts") {
        command.arg("--experimental-strip-types");
    }
    command
        .arg(script)
        .arg(&report.source_url)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = match timeout(browser_timeout, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            push_failure(report, started.elapsed().as_millis(), error.to_string());
            return;
        }
        Err(_) => {
            push_failure(
                report,
                started.elapsed().as_millis(),
                format!(
                    "browser fallback exceeded {} seconds",
                    browser_timeout.as_secs()
                ),
            );
            return;
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        push_failure(
            report,
            started.elapsed().as_millis(),
            format!(
                "browser process exited with {}: {}",
                output.status,
                stderr.trim().chars().take(2_000).collect::<String>()
            ),
        );
        return;
    }

    let snapshot = match serde_json::from_slice::<BrowserSnapshot>(&output.stdout) {
        Ok(snapshot) if snapshot.schema_version == "facebook-browser-snapshot.v1" => snapshot,
        Ok(snapshot) => {
            push_failure(
                report,
                started.elapsed().as_millis(),
                format!(
                    "unsupported browser snapshot schema {}",
                    snapshot.schema_version
                ),
            );
            return;
        }
        Err(error) => {
            push_failure(report, started.elapsed().as_millis(), error.to_string());
            return;
        }
    };

    if snapshot.source_url != report.source_url {
        push_failure(
            report,
            snapshot.latency_ms,
            "browser snapshot source URL mismatch".to_owned(),
        );
        return;
    }

    let bytes_received = snapshot.html.len();
    let browser = browser_attempt_metadata(&snapshot);
    let newest_dom_post_unresolved = snapshot.newest_dom_post_unresolved.unwrap_or(false);
    let final_url_is_login_wall = snapshot.final_url.to_ascii_lowercase().contains("/login/");
    let hint = (!final_url_is_login_wall)
        .then(|| {
            snapshot
                .discovered_post_url
                .as_deref()
                .map(|canonical_url| PostHint {
                    canonical_url,
                    published_at: snapshot.discovered_published_at.as_deref(),
                    external_post_id: snapshot.discovered_external_post_id.as_deref(),
                    text: snapshot.discovered_text.as_deref(),
                })
        })
        .flatten();
    let (posts, stats) = match extract_posts_with_hint(
        &snapshot.html,
        &report.source_url,
        BROWSER_STRATEGY,
        &snapshot.fetched_at,
        hint.as_ref(),
    ) {
        Ok(result) => result,
        Err(error) => {
            report.attempts.push(Attempt {
                strategy: BROWSER_STRATEGY.to_owned(),
                outcome: "parse_failure".to_owned(),
                status: snapshot.status,
                latency_ms: snapshot.latency_ms,
                bytes_received,
                final_url: Some(snapshot.final_url),
                posts_found: 0,
                newest_post_at: None,
                parse: ParseStats::default(),
                error: Some(error.to_string()),
                browser,
            });
            return;
        }
    };
    let outcome = if newest_dom_post_unresolved {
        "sparse"
    } else {
        classify_outcome(snapshot.status.unwrap_or(200), &stats, minimum_yield)
    };
    report.attempts.push(Attempt {
        strategy: BROWSER_STRATEGY.to_owned(),
        outcome: outcome.to_owned(),
        status: snapshot.status,
        latency_ms: snapshot.latency_ms,
        bytes_received,
        final_url: Some(snapshot.final_url),
        posts_found: posts.len(),
        newest_post_at: posts.first().map(|post| post.published_at.clone()),
        parse: stats,
        error: None,
        browser,
    });
    if !posts.is_empty() {
        merge_posts(&mut report.posts, posts);
        report.fetched_at = snapshot.fetched_at;
        report.selected_strategy = Some(BROWSER_STRATEGY.to_owned());
        report.post_count = report.posts.len();
        report.health = if newest_dom_post_unresolved {
            "degraded".to_owned()
        } else if report.posts.len() >= minimum_yield {
            "healthy".to_owned()
        } else {
            "degraded".to_owned()
        };
    }
}

fn merge_posts(existing: &mut Vec<FacebookPost>, incoming: Vec<FacebookPost>) {
    for post in incoming {
        if let Some(index) = existing
            .iter()
            .position(|current| current.external_post_id == post.external_post_id)
        {
            existing[index] = post;
        } else {
            existing.push(post);
        }
    }
    existing.sort_by(|left, right| right.published_at.cmp(&left.published_at));
}

fn push_failure(report: &mut CrawlReport, latency_ms: u128, error: String) {
    report.attempts.push(Attempt {
        strategy: BROWSER_STRATEGY.to_owned(),
        outcome: "network_error".to_owned(),
        status: None,
        latency_ms,
        bytes_received: 0,
        final_url: None,
        posts_found: 0,
        newest_post_at: None,
        parse: ParseStats::default(),
        error: Some(error),
        browser: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post(id: &str, published_at: &str) -> FacebookPost {
        FacebookPost {
            schema_version: "facebook-post.v1".to_owned(),
            source_id: "facebook:test".to_owned(),
            platform: "facebook".to_owned(),
            external_post_id: id.to_owned(),
            canonical_url: format!("https://www.facebook.com/test/posts/{id}"),
            published_at: published_at.to_owned(),
            text: id.to_owned(),
            media: Vec::new(),
            outbound_links: Vec::new(),
            content_hash: format!("sha256:{id}"),
            crawl_strategy: "fixture".to_owned(),
            fetched_at: "2026-07-28T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn browser_posts_merge_into_existing_report() {
        let mut existing = vec![post("same", "2026-07-28T01:00:00Z")];
        merge_posts(
            &mut existing,
            vec![
                post("same", "2026-07-28T01:00:00Z"),
                post("new", "2026-07-28T02:00:00Z"),
            ],
        );
        assert_eq!(existing.len(), 2);
        assert_eq!(existing[0].external_post_id, "new");
        assert_eq!(existing[1].external_post_id, "same");
    }
}
