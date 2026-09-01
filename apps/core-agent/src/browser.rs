use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::time::timeout;
use uth_crawler::facebook::{PostHint, classify_outcome, extract_posts_with_hint};
use uth_domain::{Attempt, BrowserAttemptMetadata, CrawlReport, FacebookPost, ParseStats};

const BROWSER_STRATEGY: &str = "browser_playwright";
pub const BROWSER_HISTORY_TARGET: usize = 20;

#[derive(Debug)]
struct BrowserProcessOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Debug)]
enum BrowserProcessError {
    Execution(String),
    TimedOut(Option<String>),
}

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

async fn read_browser_pipe<R>(mut pipe: R) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    pipe.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

async fn collect_browser_pipe(
    pipe_name: &str,
    task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
) -> Result<Vec<u8>, String> {
    task.await
        .map_err(|error| format!("browser {pipe_name} reader task failed: {error}"))?
        .map_err(|error| format!("browser {pipe_name} read failed: {error}"))
}

#[cfg(unix)]
fn configure_browser_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_browser_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn kill_browser_process_group(process_group_id: Option<u32>) -> Result<(), String> {
    let process_group_id = process_group_id
        .ok_or_else(|| "browser process ID is unavailable".to_owned())
        .and_then(|value| {
            i32::try_from(value).map_err(|_| "browser process ID exceeds i32".to_owned())
        })?;
    let result = unsafe { libc::kill(-process_group_id, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(format!("browser process-group kill failed: {error}"))
    }
}

#[cfg(not(unix))]
fn kill_browser_process_group(_process_group_id: Option<u32>) -> Result<(), String> {
    Ok(())
}

async fn terminate_browser_process(
    child: &mut Child,
    process_group_id: Option<u32>,
) -> Result<(), String> {
    let mut errors = Vec::new();
    if let Err(error) = kill_browser_process_group(process_group_id) {
        errors.push(error);
    }
    if let Err(error) = child.kill().await
        && error.kind() != std::io::ErrorKind::InvalidInput
    {
        errors.push(format!("browser direct-child kill failed: {error}"));
    }
    if let Err(error) = child.wait().await {
        errors.push(format!("browser direct-child reap failed: {error}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn run_browser_process(
    command: &mut Command,
    browser_timeout: Duration,
) -> Result<BrowserProcessOutput, BrowserProcessError> {
    let browser_temp_directory = tempfile::Builder::new()
        .prefix("uth-browser-run-")
        .tempdir()
        .map_err(|error| {
            BrowserProcessError::Execution(format!(
                "failed to create browser temporary directory: {error}"
            ))
        })?;
    command
        .env("TMPDIR", browser_temp_directory.path())
        .env("TMP", browser_temp_directory.path())
        .env("TEMP", browser_temp_directory.path());
    let result = run_browser_process_in_temp(command, browser_timeout).await;
    let cleanup_error = browser_temp_directory
        .close()
        .err()
        .map(|error| format!("browser temporary-directory cleanup failed: {error}"));
    match (result, cleanup_error) {
        (Ok(output), None) => Ok(output),
        (Ok(_), Some(error)) => Err(BrowserProcessError::Execution(error)),
        (Err(BrowserProcessError::Execution(error)), Some(cleanup)) => Err(
            BrowserProcessError::Execution(format!("{error}; {cleanup}")),
        ),
        (Err(BrowserProcessError::TimedOut(existing)), Some(cleanup)) => {
            let cleanup = existing
                .map(|error| format!("{error}; {cleanup}"))
                .unwrap_or(cleanup);
            Err(BrowserProcessError::TimedOut(Some(cleanup)))
        }
        (Err(error), None) => Err(error),
    }
}

async fn run_browser_process_in_temp(
    command: &mut Command,
    browser_timeout: Duration,
) -> Result<BrowserProcessOutput, BrowserProcessError> {
    configure_browser_process_group(command);
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| BrowserProcessError::Execution(error.to_string()))?;
    let process_group_id = child.id();
    let stdout = child.stdout.take().ok_or_else(|| {
        BrowserProcessError::Execution("browser stdout pipe is unavailable".to_owned())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        BrowserProcessError::Execution("browser stderr pipe is unavailable".to_owned())
    })?;
    let stdout_task = tokio::spawn(read_browser_pipe(stdout));
    let stderr_task = tokio::spawn(read_browser_pipe(stderr));

    let status = match timeout(browser_timeout, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            let cleanup_error = terminate_browser_process(&mut child, process_group_id)
                .await
                .err();
            let _ = collect_browser_pipe("stdout", stdout_task).await;
            let _ = collect_browser_pipe("stderr", stderr_task).await;
            let detail = cleanup_error
                .map(|cleanup| format!("{error}; {cleanup}"))
                .unwrap_or_else(|| error.to_string());
            return Err(BrowserProcessError::Execution(detail));
        }
        Err(_) => {
            let mut cleanup_errors = Vec::new();
            if let Err(error) = terminate_browser_process(&mut child, process_group_id).await {
                cleanup_errors.push(error);
            }
            if let Err(error) = collect_browser_pipe("stdout", stdout_task).await {
                cleanup_errors.push(error);
            }
            if let Err(error) = collect_browser_pipe("stderr", stderr_task).await {
                cleanup_errors.push(error);
            }
            return Err(BrowserProcessError::TimedOut(
                (!cleanup_errors.is_empty()).then(|| cleanup_errors.join("; ")),
            ));
        }
    };
    let stdout = collect_browser_pipe("stdout", stdout_task)
        .await
        .map_err(BrowserProcessError::Execution)?;
    let stderr = collect_browser_pipe("stderr", stderr_task)
        .await
        .map_err(BrowserProcessError::Execution)?;
    Ok(BrowserProcessOutput {
        status,
        stdout,
        stderr,
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

    let output = match run_browser_process(&mut command, browser_timeout).await {
        Ok(output) => output,
        Err(BrowserProcessError::Execution(error)) => {
            push_failure(report, started.elapsed().as_millis(), error);
            return;
        }
        Err(BrowserProcessError::TimedOut(cleanup_error)) => {
            let cleanup_detail = cleanup_error
                .map(|error| format!("; browser cleanup failed: {error}"))
                .unwrap_or_default();
            push_failure(
                report,
                started.elapsed().as_millis(),
                format!(
                    "browser fallback exceeded {} seconds{}",
                    browser_timeout.as_secs(),
                    cleanup_detail
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

    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[cfg(unix)]
    #[tokio::test]
    async fn browser_timeout_terminates_background_descendants() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let task_directory = std::env::temp_dir().join(format!(
            "uth-agent-browser-process-test-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&task_directory).unwrap();
        let descendant_pid_path = task_directory.join("descendant.pid");
        let browser_temp_path = task_directory.join("browser-temp.path");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(
                "printf %s \"$TMPDIR\" > \"$2\"; case \"$TMPDIR\" in */uth-browser-run-*) mkdir -p \"$TMPDIR/playwright-test-residue\";; esac; sleep 30 & echo $! > \"$1\"; wait",
            )
            .arg("sh")
            .arg(&descendant_pid_path)
            .arg(&browser_temp_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let result = run_browser_process(&mut command, Duration::from_millis(500)).await;

        assert!(matches!(result, Err(BrowserProcessError::TimedOut(_))));
        let descendant_pid = fs::read_to_string(&descendant_pid_path)
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let descendant_stat_path = Path::new("/proc")
            .join(descendant_pid.to_string())
            .join("stat");
        for _ in 0..40 {
            let state = fs::read_to_string(&descendant_stat_path)
                .ok()
                .and_then(|stat| stat.split_whitespace().nth(2).map(str::to_owned));
            if state.is_none() || state.as_deref() == Some("Z") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let state = fs::read_to_string(descendant_stat_path)
            .ok()
            .and_then(|stat| stat.split_whitespace().nth(2).map(str::to_owned));
        assert!(state.is_none() || state.as_deref() == Some("Z"));
        let browser_temp_directory = fs::read_to_string(&browser_temp_path).unwrap();
        assert!(
            Path::new(&browser_temp_directory)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("uth-browser-run-")
        );
        assert!(!Path::new(&browser_temp_directory).exists());
        fs::remove_file(descendant_pid_path).unwrap();
        fs::remove_file(browser_temp_path).unwrap();
        fs::remove_dir(task_directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn browser_process_collects_stdout_and_stderr() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("printf stdout; printf stderr >&2")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = run_browser_process(&mut command, Duration::from_secs(2))
            .await
            .unwrap();

        assert!(output.status.success());
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");
    }
}
