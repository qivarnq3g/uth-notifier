//! Public Facebook Page crawler with normalization and health diagnostics.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use reqwest::header::{ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, USER_AGENT};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;
use uth_domain::{
    Attempt, CrawlReport, FacebookPost, MediaItem, POST_SCHEMA_VERSION, ParseStats,
    REPORT_SCHEMA_VERSION,
};

pub const DEFAULT_MINIMUM_YIELD: usize = 5;
pub const STRATEGIES: [&str; 4] = ["standard", "polite", "bingbot", "googlebot"];
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_JSON_DEPTH: usize = 128;
const MAX_JSON_NODES: usize = 200_000;

const STANDARD_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36";
const POLITE_UA: &str = "UTHActivityNotifier/0.1 (public-page availability probe)";

const LOGIN_WALL_MARKERS: [&str; 4] = [
    "login_form",
    "checkpoint",
    "you must log in to continue",
    "bạn phải đăng nhập để tiếp tục",
];

#[derive(Debug, Error)]
pub enum CrawlerError {
    #[error("invalid Facebook Page URL: {0}")]
    InvalidPageUrl(String),
    #[error("unknown crawl strategy: {0}")]
    UnknownStrategy(String),
    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("HTTP response exceeds {0} bytes")]
    ResponseTooLarge(u64),
}

#[derive(Debug, Default)]
struct PostAccumulator {
    timestamps: BTreeSet<i64>,
    permalinks: BTreeSet<String>,
    text_candidates: BTreeSet<(u8, String)>,
    outbound_links: BTreeSet<String>,
    media: BTreeSet<(String, String, Option<String>)>,
}

#[derive(Debug)]
struct FetchResponse {
    body: String,
    status: u16,
    final_url: String,
}

#[derive(Debug)]
pub struct PostHint<'a> {
    pub canonical_url: &'a str,
    pub published_at: Option<&'a str>,
    pub external_post_id: Option<&'a str>,
    pub text: Option<&'a str>,
}

pub fn page_slug(page_url: &str) -> Result<String, CrawlerError> {
    let parsed =
        Url::parse(page_url).map_err(|error| CrawlerError::InvalidPageUrl(error.to_string()))?;
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    if !matches!(parsed.scheme(), "http" | "https")
        || !(host == "facebook.com" || host.ends_with(".facebook.com"))
    {
        return Err(CrawlerError::InvalidPageUrl(page_url.to_owned()));
    }
    if parsed.path().trim_end_matches('/') == "/profile.php" {
        return parsed
            .query_pairs()
            .find_map(|(key, value)| {
                (key == "id"
                    && !value.is_empty()
                    && value.bytes().all(|byte| byte.is_ascii_digit()))
                .then(|| value.into_owned())
            })
            .ok_or_else(|| CrawlerError::InvalidPageUrl(page_url.to_owned()));
    }
    let segments = parsed
        .path_segments()
        .map(|segments| {
            segments
                .filter(|segment| !segment.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let identity = match segments.as_slice() {
        ["people", _, id, ..] if id.chars().all(|character| character.is_ascii_digit()) => *id,
        [first, ..] => *first,
        [] => return Err(CrawlerError::InvalidPageUrl(page_url.to_owned())),
    };
    Ok(identity.to_owned())
}

pub fn source_id(page_url: &str) -> Result<String, CrawlerError> {
    Ok(format!("facebook:{}", page_slug(page_url)?.to_lowercase()))
}

pub fn canonicalize_url(raw: &str) -> Option<String> {
    let mut parsed = Url::parse(&raw.replace("\\/", "/")).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    if matches!(
        host.as_str(),
        "m.facebook.com" | "mbasic.facebook.com" | "facebook.com"
    ) {
        parsed.set_host(Some("www.facebook.com")).ok()?;
    }
    if matches!(parsed.scheme(), "http" | "https") {
        parsed.set_scheme("https").ok()?;
    }
    let facebook_host = host == "facebook.com" || host.ends_with(".facebook.com");
    let permalink_query = (facebook_host && parsed.path() == "/permalink.php").then(|| {
        let mut story_fbid = None;
        let mut id = None;
        for (key, value) in parsed.query_pairs() {
            match key.as_ref() {
                "story_fbid" => story_fbid = Some(value.into_owned()),
                "id" => id = Some(value.into_owned()),
                _ => {}
            }
        }
        (story_fbid, id)
    });
    if facebook_host {
        parsed.set_query(None);
    } else {
        let query = parsed
            .query_pairs()
            .filter(|(key, _)| {
                let key = key.to_ascii_lowercase();
                !key.starts_with("utm_")
                    && !matches!(key.as_str(), "fbclid" | "gclid" | "ref" | "ref_src")
            })
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        parsed.set_query(None);
        if !query.is_empty() {
            parsed.query_pairs_mut().extend_pairs(query);
        }
    }
    parsed.set_fragment(None);
    let normalized_path = parsed
        .path()
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    parsed.set_path(&format!("/{normalized_path}"));
    if let Some((Some(story_fbid), Some(id))) = permalink_query {
        parsed
            .query_pairs_mut()
            .append_pair("story_fbid", &story_fbid)
            .append_pair("id", &id);
    }
    let mut result = parsed.to_string();
    if result.ends_with('/') && !normalized_path.is_empty() {
        result.pop();
    }
    Some(result)
}

fn permalink_matches_source(url: &str, identity: &str) -> bool {
    let Ok(parsed) = Url::parse(url) else {
        return false;
    };
    if parsed.path_segments().is_some_and(|segments| {
        segments
            .into_iter()
            .any(|segment| segment.eq_ignore_ascii_case(identity))
    }) {
        return true;
    }
    parsed.path() == "/permalink.php"
        && parsed
            .query_pairs()
            .any(|(key, value)| key == "id" && value == identity)
}

fn post_locator(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    if let Some(value) = parsed
        .query_pairs()
        .find_map(|(key, value)| (key == "story_fbid").then(|| value.into_owned()))
    {
        return Some(value);
    }
    let segments = parsed.path_segments()?.collect::<Vec<_>>();
    segments
        .windows(2)
        .find(|window| matches!(window[0], "posts" | "videos" | "reel"))
        .map(|window| window[1].to_owned())
}

fn user_agent(strategy: &str) -> Result<&'static str, CrawlerError> {
    match strategy {
        "standard" => Ok(STANDARD_UA),
        "polite" => Ok(POLITE_UA),
        "bingbot" | "googlebot" => Ok(STANDARD_UA),
        other => Err(CrawlerError::UnknownStrategy(other.to_owned())),
    }
}

fn string_value(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.to_owned()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn message_text(value: Option<&Value>) -> Option<String> {
    let text = match value? {
        Value::String(text) => text,
        Value::Object(object) => object.get("text")?.as_str()?,
        _ => return None,
    }
    .trim();
    (!text.is_empty()).then(|| text.to_owned())
}

fn is_http_url(value: &str) -> bool {
    Url::parse(value)
        .map(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
        .unwrap_or(false)
}

fn is_external_url(value: &str) -> bool {
    let Ok(parsed) = Url::parse(value) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    !(host == "facebook.com"
        || host.ends_with(".facebook.com")
        || host.ends_with(".fbcdn.net")
        || host.ends_with(".fbsbx.com"))
}

fn nested_uri(node: &Value, paths: &[&[&str]]) -> Option<String> {
    for path in paths {
        let mut current = node;
        let mut found = true;
        for key in *path {
            let Some(next) = current.get(*key) else {
                found = false;
                break;
            };
            current = next;
        }
        if found
            && let Some(uri) = current.as_str()
            && is_http_url(uri)
        {
            return Some(uri.replace("\\/", "/"));
        }
    }
    None
}

fn find_ascii_case_insensitive(haystack: &[u8], start: usize, needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || start >= haystack.len() || needle.len() > haystack.len() - start {
        return None;
    }
    haystack[start..]
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
        .map(|offset| start + offset)
}

fn tag_has_json_type(tag: &[u8]) -> bool {
    let mut cursor = b"<script".len();
    while cursor < tag.len() {
        while cursor < tag.len() && (tag[cursor].is_ascii_whitespace() || tag[cursor] == b'>') {
            cursor += 1;
        }
        let name_start = cursor;
        while cursor < tag.len()
            && !tag[cursor].is_ascii_whitespace()
            && !matches!(tag[cursor], b'=' | b'>')
        {
            cursor += 1;
        }
        if name_start == cursor {
            cursor += 1;
            continue;
        }
        let name = &tag[name_start..cursor];
        while cursor < tag.len() && tag[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= tag.len() || tag[cursor] != b'=' {
            continue;
        }
        cursor += 1;
        while cursor < tag.len() && tag[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= tag.len() {
            break;
        }
        let quote = matches!(tag[cursor], b'\'' | b'"').then(|| tag[cursor]);
        if quote.is_some() {
            cursor += 1;
        }
        let value_start = cursor;
        while cursor < tag.len()
            && match quote {
                Some(quote) => tag[cursor] != quote,
                None => !tag[cursor].is_ascii_whitespace() && tag[cursor] != b'>',
            }
        {
            cursor += 1;
        }
        let value = &tag[value_start..cursor];
        if quote.is_some() && cursor < tag.len() {
            cursor += 1;
        }
        if name.eq_ignore_ascii_case(b"type") && value.eq_ignore_ascii_case(b"application/json") {
            return true;
        }
    }
    false
}

fn extract_json_scripts(html: &str) -> Vec<&str> {
    let bytes = html.as_bytes();
    let mut scripts = Vec::new();
    let mut cursor = 0;
    while let Some(start) = find_ascii_case_insensitive(bytes, cursor, b"<script") {
        let Some(relative_tag_end) = bytes[start..].iter().position(|byte| *byte == b'>') else {
            break;
        };
        let tag_end = start + relative_tag_end;
        let content_start = tag_end + 1;
        let Some(content_end) = find_ascii_case_insensitive(bytes, content_start, b"</script>")
        else {
            break;
        };
        if tag_has_json_type(&bytes[start..=tag_end]) {
            scripts.push(&html[content_start..content_end]);
        }
        cursor = content_end + b"</script>".len();
    }
    scripts
}

fn walk_graph(
    node: &Value,
    accumulators: &mut BTreeMap<String, PostAccumulator>,
    inherited_post_id: &str,
    in_attachment: bool,
    depth: usize,
    visited: &mut usize,
) {
    if depth > MAX_JSON_DEPTH || *visited >= MAX_JSON_NODES {
        return;
    }
    *visited += 1;
    match node {
        Value::Array(items) => {
            for item in items {
                walk_graph(
                    item,
                    accumulators,
                    inherited_post_id,
                    in_attachment,
                    depth + 1,
                    visited,
                );
            }
        }
        Value::Object(object) => {
            let raw_post_id = string_value(
                object
                    .get("post_id")
                    .or_else(|| object.get("top_level_post_id")),
            );
            let post_id = raw_post_id
                .as_deref()
                .unwrap_or(inherited_post_id)
                .to_owned();
            let plausible_id =
                post_id.len() >= 8 && post_id.chars().all(|char| char.is_ascii_digit());

            if plausible_id {
                let item = accumulators.entry(post_id.clone()).or_default();
                for key in ["creation_time", "publish_time"] {
                    if let Some(timestamp) = object.get(key).and_then(Value::as_i64)
                        && timestamp > 0
                    {
                        item.timestamps.insert(timestamp);
                    }
                }

                for key in ["url", "permalink_url"] {
                    if let Some(value) = object.get(key).and_then(Value::as_str)
                        && is_http_url(value)
                        && ["/posts/", "/videos/", "/reel/", "story_fbid="]
                            .iter()
                            .any(|marker| value.contains(marker))
                        && let Some(normalized) = canonicalize_url(value)
                    {
                        item.permalinks.insert(normalized);
                    }
                }

                for (key, score) in [
                    ("message", 100),
                    ("message_context", 80),
                    ("title", 50),
                    ("text", 40),
                ] {
                    if let Some(text) = message_text(object.get(key))
                        && text.chars().count() >= 20
                    {
                        item.text_candidates.insert((score, text));
                    }
                }

                for key in ["external_url", "web_link", "href", "url"] {
                    if let Some(value) = object.get(key).and_then(Value::as_str)
                        && is_http_url(value)
                        && let Some(normalized) = canonicalize_url(value)
                        && is_external_url(&normalized)
                    {
                        item.outbound_links.insert(normalized);
                    }
                }

                let typename = object
                    .get("__typename")
                    .or_else(|| object.get("media_type"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if in_attachment && matches!(typename.as_str(), "photo" | "video" | "image") {
                    let kind = if typename == "video" {
                        "video"
                    } else {
                        "image"
                    };
                    let mut media_url = nested_uri(
                        node,
                        &[
                            &["image", "uri"],
                            &["photo_image", "uri"],
                            &["preferred_thumbnail", "image", "uri"],
                            &["thumbnailImage", "uri"],
                        ],
                    );
                    if media_url.is_none() && kind == "video" {
                        media_url = object
                            .get("playable_url")
                            .or_else(|| object.get("browser_native_hd_url"))
                            .and_then(Value::as_str)
                            .filter(|value| is_http_url(value))
                            .map(str::to_owned);
                    }
                    if let Some(media_url) = media_url {
                        let alt_text = message_text(object.get("accessibility_caption"));
                        item.media.insert((kind.to_owned(), media_url, alt_text));
                    }
                }
            }

            let next_id = if plausible_id {
                post_id.as_str()
            } else {
                inherited_post_id
            };
            for (key, value) in object {
                let child_attachment = in_attachment
                    || matches!(
                        key.as_str(),
                        "attachments"
                            | "attachment"
                            | "media"
                            | "all_subattachments"
                            | "subattachments"
                    );
                walk_graph(
                    value,
                    accumulators,
                    next_id,
                    child_attachment,
                    depth + 1,
                    visited,
                );
            }
        }
        _ => {}
    }
}

fn content_hash(text: &str, media: &[MediaItem], outbound_links: &[String]) -> String {
    let normalized_text = text
        .trim()
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    let stable_media = media
        .iter()
        .map(|item| {
            json!({
                "kind": item.kind,
                "url": media_url_hash_identity(&item.url),
                "alt_text": item.alt_text,
            })
        })
        .collect::<Vec<_>>();
    let stable = json!({
        "media": stable_media,
        "outbound_links": outbound_links,
        "text": normalized_text,
    });
    let encoded = serde_json::to_vec(&stable).expect("stable post content is serializable");
    format!("sha256:{:x}", Sha256::digest(encoded))
}

pub fn media_url_hash_identity(raw: &str) -> String {
    let normalized = canonicalize_url(raw).unwrap_or_else(|| raw.to_owned());
    let Ok(parsed) = Url::parse(&normalized) else {
        return normalized;
    };
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    if host == "fbcdn.net" || host.ends_with(".fbcdn.net") {
        return format!("https://fbcdn.net{}", parsed.path());
    }
    normalized
}

pub fn extract_posts(
    html: &str,
    page_url: &str,
    strategy: &str,
    fetched_at: &str,
) -> Result<(Vec<FacebookPost>, ParseStats), CrawlerError> {
    extract_posts_with_hint(html, page_url, strategy, fetched_at, None)
}

pub fn extract_posts_with_hint(
    html: &str,
    page_url: &str,
    strategy: &str,
    fetched_at: &str,
    hint: Option<&PostHint<'_>>,
) -> Result<(Vec<FacebookPost>, ParseStats), CrawlerError> {
    let slug = page_slug(page_url)?.to_lowercase();
    let source_id = source_id(page_url)?;
    let scripts = extract_json_scripts(html);
    let canonical_hint = hint.and_then(|value| canonicalize_url(value.canonical_url));
    let hint_locator = canonical_hint.as_deref().and_then(post_locator);
    let hint_timestamp = hint
        .and_then(|value| value.published_at)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp());

    let mut accumulators = BTreeMap::new();
    let mut parsed_count = 0;
    let mut malformed_count = 0;
    for script in &scripts {
        match serde_json::from_str::<Value>(script) {
            Ok(root) => {
                parsed_count += 1;
                let mut visited = 0;
                walk_graph(&root, &mut accumulators, "", false, 0, &mut visited);
            }
            Err(_) => malformed_count += 1,
        }
    }

    let candidate_count = accumulators.len();
    let mut missing_timestamp = 0;
    let mut missing_url = 0;
    let mut posts = Vec::new();
    for (post_id, item) in accumulators {
        let mut matching_urls = item
            .permalinks
            .iter()
            .filter(|url| permalink_matches_source(url, &slug))
            .cloned()
            .collect::<Vec<_>>();
        let matches_hint = hint.and_then(|value| value.external_post_id) == Some(post_id.as_str())
            || hint_locator.as_ref().is_some_and(|locator| {
                item.permalinks
                    .iter()
                    .any(|url| post_locator(url).as_ref() == Some(locator))
            });
        if matching_urls.is_empty()
            && let Some(hint) = &canonical_hint
            && hint_locator.is_some()
            && matches_hint
        {
            matching_urls.push(hint.clone());
        }
        let Some(timestamp) = item
            .timestamps
            .last()
            .copied()
            .or_else(|| matches_hint.then_some(hint_timestamp).flatten())
        else {
            missing_timestamp += 1;
            continue;
        };
        if matching_urls.is_empty() {
            missing_url += 1;
            continue;
        }
        let published_at = DateTime::<Utc>::from_timestamp(timestamp, 0)
            .map(|value| value.to_rfc3339())
            .ok_or_else(|| {
                CrawlerError::InvalidPageUrl(format!("invalid timestamp {timestamp}"))
            })?;
        let text = item
            .text_candidates
            .iter()
            .max_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| left.1.len().cmp(&right.1.len()))
            })
            .map(|(_, text)| text.clone())
            .or_else(|| {
                matches_hint
                    .then_some(hint.and_then(|value| value.text))
                    .flatten()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
            })
            .unwrap_or_default();
        let media = item
            .media
            .into_iter()
            .map(|(kind, url, alt_text)| MediaItem {
                kind,
                url,
                alt_text,
            })
            .collect::<Vec<_>>();
        let outbound_links = item.outbound_links.into_iter().collect::<Vec<_>>();
        let hash = content_hash(&text, &media, &outbound_links);
        let canonical_url = matching_urls
            .into_iter()
            .min_by_key(String::len)
            .expect("matching URL was checked as non-empty");
        posts.push(FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: source_id.clone(),
            platform: "facebook".to_owned(),
            external_post_id: post_id,
            canonical_url,
            published_at,
            text,
            media,
            outbound_links,
            content_hash: hash,
            crawl_strategy: strategy.to_owned(),
            fetched_at: fetched_at.to_owned(),
        });
    }
    if posts.is_empty()
        && let (Some(hint), Some(canonical_url), Some(locator), Some(timestamp)) =
            (hint, canonical_hint, hint_locator, hint_timestamp)
        && hint.external_post_id == Some(locator.as_str())
        && permalink_matches_source(&canonical_url, &slug)
        && let Some(text) = hint.text.map(str::trim).filter(|value| !value.is_empty())
    {
        let published_at = DateTime::<Utc>::from_timestamp(timestamp, 0)
            .map(|value| value.to_rfc3339())
            .ok_or_else(|| {
                CrawlerError::InvalidPageUrl(format!("invalid timestamp {timestamp}"))
            })?;
        let media = Vec::new();
        let outbound_links = Vec::new();
        posts.push(FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: source_id.clone(),
            platform: "facebook".to_owned(),
            external_post_id: locator,
            canonical_url,
            published_at,
            text: text.to_owned(),
            content_hash: content_hash(text, &media, &outbound_links),
            media,
            outbound_links,
            crawl_strategy: strategy.to_owned(),
            fetched_at: fetched_at.to_owned(),
        });
    }
    posts.sort_by(|left, right| right.published_at.cmp(&left.published_at));

    let lower_html = html.to_lowercase();
    let login_wall_detected = posts.is_empty()
        && LOGIN_WALL_MARKERS
            .iter()
            .any(|marker| lower_html.contains(marker));
    let stats = ParseStats {
        json_scripts: scripts.len(),
        json_scripts_parsed: parsed_count,
        malformed_json_scripts: malformed_count,
        candidate_post_ids: candidate_count,
        valid_posts: posts.len(),
        rejected_missing_timestamp: missing_timestamp,
        rejected_foreign_or_missing_url: missing_url,
        login_wall_detected,
    };
    Ok((posts, stats))
}

pub fn classify_outcome(status: u16, stats: &ParseStats, minimum_yield: usize) -> &'static str {
    if status >= 400 {
        "http_error"
    } else if stats.valid_posts >= minimum_yield {
        "healthy"
    } else if stats.valid_posts > 0 {
        "sparse"
    } else if stats.login_wall_detected {
        "login_wall"
    } else if stats.json_scripts > 0 && stats.malformed_json_scripts == stats.json_scripts {
        "parse_failure"
    } else {
        "empty"
    }
}

async fn fetch(
    client: &reqwest::Client,
    page_url: &str,
    strategy: &str,
) -> Result<FetchResponse, CrawlerError> {
    let response = client
        .get(page_url)
        .header(USER_AGENT, user_agent(strategy)?)
        .header(
            ACCEPT,
            "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        )
        .header(ACCEPT_LANGUAGE, "vi-VN,vi;q=0.9,en;q=0.7")
        .header(CACHE_CONTROL, "no-cache")
        .send()
        .await?;
    let status = response.status().as_u16();
    let final_url = response.url().to_string();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES)
    {
        return Err(CrawlerError::ResponseTooLarge(MAX_RESPONSE_BYTES));
    }
    let bytes = response.bytes().await?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(CrawlerError::ResponseTooLarge(MAX_RESPONSE_BYTES));
    }
    let body = String::from_utf8_lossy(&bytes).into_owned();
    Ok(FetchResponse {
        body,
        status,
        final_url,
    })
}

pub async fn probe(
    page_url: &str,
    strategies: &[String],
    timeout: Duration,
    stop_after_success: bool,
    minimum_yield: usize,
    fetched_at: Option<String>,
) -> Result<CrawlReport, CrawlerError> {
    page_slug(page_url)?;
    let fetched_at = fetched_at.unwrap_or_else(|| Utc::now().to_rfc3339());
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::limited(8))
        .build()?;
    let mut attempts = Vec::new();
    let mut best_posts = Vec::new();
    let mut selected_strategy = None;

    for strategy in strategies {
        let started = Instant::now();
        match fetch(&client, page_url, strategy).await {
            Ok(response) => {
                let bytes_received = response.body.len();
                let (posts, stats) =
                    extract_posts(&response.body, page_url, strategy, &fetched_at)?;
                let outcome = classify_outcome(response.status, &stats, minimum_yield).to_owned();
                attempts.push(Attempt {
                    strategy: strategy.clone(),
                    outcome: outcome.clone(),
                    status: Some(response.status),
                    latency_ms: started.elapsed().as_millis(),
                    bytes_received,
                    final_url: Some(response.final_url),
                    posts_found: posts.len(),
                    newest_post_at: posts.first().map(|post| post.published_at.clone()),
                    parse: stats,
                    error: None,
                    browser: None,
                });
                if posts.len() > best_posts.len() {
                    best_posts = posts;
                    selected_strategy = Some(strategy.clone());
                }
                if outcome == "healthy" && stop_after_success {
                    break;
                }
            }
            Err(error) => attempts.push(Attempt {
                strategy: strategy.clone(),
                outcome: "network_error".to_owned(),
                status: None,
                latency_ms: started.elapsed().as_millis(),
                bytes_received: 0,
                final_url: None,
                posts_found: 0,
                newest_post_at: None,
                parse: ParseStats::default(),
                error: Some(error.to_string()),
                browser: None,
            }),
        }
    }
    let health = if best_posts.len() >= minimum_yield {
        "healthy"
    } else if best_posts.is_empty() {
        "failed"
    } else {
        "degraded"
    };
    Ok(CrawlReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        source_url: page_url.to_owned(),
        source_id: source_id(page_url)?,
        fetched_at,
        selected_strategy,
        health: health.to_owned(),
        post_count: best_posts.len(),
        attempts,
        posts: best_posts,
        changes: None,
    })
}

pub fn diff_posts(current: &[FacebookPost], previous_document: &Value) -> Value {
    let mut previous = BTreeMap::new();
    if let Some(posts) = previous_document.get("posts").and_then(Value::as_array) {
        for post in posts {
            let id = post
                .get("external_post_id")
                .or_else(|| post.get("post_id"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !id.is_empty() {
                previous.insert(id.to_owned(), post);
            }
        }
    }
    let current_by_id = current
        .iter()
        .map(|post| (post.external_post_id.as_str(), post))
        .collect::<BTreeMap<_, _>>();
    let mut new_ids = Vec::new();
    let mut updated_ids = Vec::new();
    let mut unchanged_ids = Vec::new();
    for (id, post) in &current_by_id {
        match previous.get(*id) {
            None => new_ids.push((*id).to_owned()),
            Some(old)
                if old.get("content_hash").and_then(Value::as_str) == Some(&post.content_hash) =>
            {
                unchanged_ids.push((*id).to_owned());
            }
            Some(_) => updated_ids.push((*id).to_owned()),
        }
    }
    let missing_ids = previous
        .keys()
        .filter(|id| !current_by_id.contains_key(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    json!({
        "new": new_ids,
        "updated": updated_ids,
        "unchanged": unchanged_ids,
        "missing_from_current_window": missing_ids,
        "summary": {
            "new": new_ids.len(),
            "updated": updated_ids.len(),
            "unchanged": unchanged_ids.len(),
            "missing_from_current_window": missing_ids.len(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_URL: &str = "https://www.facebook.com/hoisinhvien.com.vn";
    const SUCCESS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/facebook/timeline_success.html"
    ));
    const LOGIN_WALL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/facebook/login_wall.html"
    ));
    const MALFORMED: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/facebook/malformed_payload.html"
    ));

    #[test]
    fn extracts_normalizes_and_deduplicates_contract() {
        let (posts, stats) =
            extract_posts(SUCCESS, PAGE_URL, "fixture", "2026-07-19T03:00:00+00:00").unwrap();
        assert_eq!(stats.json_scripts, 1);
        assert_eq!(stats.candidate_post_ids, 2);
        assert_eq!(stats.valid_posts, 1);
        assert_eq!(stats.rejected_foreign_or_missing_url, 1);
        assert_eq!(posts.len(), 1);
        let post = &posts[0];
        assert_eq!(post.schema_version, "facebook-post.v1");
        assert_eq!(post.source_id, "facebook:hoisinhvien.com.vn");
        assert_eq!(post.external_post_id, "1448197277342081");
        assert_eq!(post.published_at, "2026-07-19T02:00:17+00:00");
        assert_eq!(
            post.canonical_url,
            "https://www.facebook.com/hoisinhvien.com.vn/posts/pfbid-example"
        );
        assert_eq!(post.outbound_links, ["https://example.edu.vn/dang-ky"]);
        assert_eq!(post.media.len(), 1);
        assert_eq!(post.media[0].kind, "image");
        assert!(post.content_hash.starts_with("sha256:"));
    }

    #[test]
    fn content_hash_ignores_fetch_metadata() {
        let (first, _) =
            extract_posts(SUCCESS, PAGE_URL, "first", "2026-01-01T00:00:00+00:00").unwrap();
        let (second, _) =
            extract_posts(SUCCESS, PAGE_URL, "second", "2026-07-19T00:00:00+00:00").unwrap();
        assert_eq!(first[0].content_hash, second[0].content_hash);
    }

    #[test]
    fn content_hash_ignores_facebook_cdn_host_and_signature_changes() {
        let first = [MediaItem {
            kind: "image".to_owned(),
            url: "https://scontent.fsgn16-1.fna.fbcdn.net/v/t39.30808-6/image.jpg?_nc_sid=abc&oh=first&oe=AAAA".to_owned(),
            alt_text: Some("Ảnh thông báo".to_owned()),
        }];
        let second = [MediaItem {
            kind: "image".to_owned(),
            url: "https://scontent.fhan14-3.fna.fbcdn.net/v/t39.30808-6/image.jpg?_nc_sid=def&oh=second&oe=BBBB".to_owned(),
            alt_text: Some("Ảnh thông báo".to_owned()),
        }];

        assert_eq!(
            content_hash("Nội dung", &first, &[]),
            content_hash("Nội dung", &second, &[])
        );
    }

    #[test]
    fn content_hash_detects_media_path_and_external_query_changes() {
        let first = [MediaItem {
            kind: "image".to_owned(),
            url: "https://scontent.fsgn16-1.fna.fbcdn.net/v/t39.30808-6/first.jpg?oh=value"
                .to_owned(),
            alt_text: None,
        }];
        let second = [MediaItem {
            kind: "image".to_owned(),
            url: "https://scontent.fsgn16-1.fna.fbcdn.net/v/t39.30808-6/second.jpg?oh=value"
                .to_owned(),
            alt_text: None,
        }];

        assert_ne!(
            content_hash("Nội dung", &first, &[]),
            content_hash("Nội dung", &second, &[])
        );
        assert_ne!(
            content_hash(
                "Nội dung",
                &[],
                &["https://example.edu/register?token=first".to_owned()]
            ),
            content_hash(
                "Nội dung",
                &[],
                &["https://example.edu/register?token=second".to_owned()]
            )
        );
    }

    #[test]
    fn detects_login_wall_and_malformed_json() {
        let (login_posts, login_stats) =
            extract_posts(LOGIN_WALL, PAGE_URL, "fixture", "now").unwrap();
        let (malformed_posts, malformed_stats) =
            extract_posts(MALFORMED, PAGE_URL, "fixture", "now").unwrap();
        assert!(login_posts.is_empty());
        assert!(login_stats.login_wall_detected);
        assert!(malformed_posts.is_empty());
        assert_eq!(malformed_stats.malformed_json_scripts, 1);
        assert_eq!(classify_outcome(200, &login_stats, 1), "login_wall");
        assert_eq!(classify_outcome(200, &malformed_stats, 1), "parse_failure");
    }

    #[test]
    fn canonical_url_removes_tracking_and_normalizes_host() {
        assert_eq!(
            canonicalize_url("http://m.facebook.com/page/posts/1?ref=share#comments").unwrap(),
            "https://www.facebook.com/page/posts/1"
        );
    }

    #[test]
    fn external_url_preserves_required_query_parameters() {
        assert_eq!(
            canonicalize_url("https://example.edu/register?token=abc123&student=42#section")
                .unwrap(),
            "https://example.edu/register?token=abc123&student=42"
        );
    }

    #[test]
    fn canonical_permalink_keeps_post_and_owner_identity() {
        assert_eq!(
            canonicalize_url(
                "https://www.facebook.com/permalink.php?story_fbid=pfbid-example&id=61566022178073&ref=share"
            )
            .unwrap(),
            "https://www.facebook.com/permalink.php?story_fbid=pfbid-example&id=61566022178073"
        );
        assert!(permalink_matches_source(
            "https://www.facebook.com/permalink.php?story_fbid=pfbid-example&id=61566022178073",
            "61566022178073"
        ));
        assert_eq!(
            post_locator("https://www.facebook.com/page/posts/pfbid-example?tracking=discarded"),
            Some("pfbid-example".to_owned())
        );
    }

    #[test]
    fn browser_hint_correlates_alias_with_numeric_owner_url() {
        let html = r#"<script type="application/json">{
            "post_id":"122201523998534072",
            "url":"https://www.facebook.com/61566022178073/posts/pfbid-browser"
        }</script>"#;
        let canonical_url = "https://www.facebook.com/example.alias/posts/pfbid-browser";
        let hint = PostHint {
            canonical_url,
            published_at: Some("2026-04-08T12:00:36+00:00"),
            external_post_id: Some("122201523998534072"),
            text: Some("Thong bao nghien cuu khoa hoc sinh vien co noi dung hop le."),
        };
        let (posts, stats) = extract_posts_with_hint(
            html,
            "https://www.facebook.com/example.alias/",
            "browser_playwright",
            "2026-04-08T12:00:36+00:00",
            Some(&hint),
        )
        .unwrap();
        assert_eq!(stats.valid_posts, 1);
        assert_eq!(posts[0].canonical_url, canonical_url);
        assert_eq!(posts[0].external_post_id, "122201523998534072");
        assert_eq!(posts[0].published_at, "2026-04-08T12:00:36+00:00");
        assert!(!posts[0].text.is_empty());
    }

    #[test]
    fn complete_browser_hint_recovers_page_plugin_post_without_json() {
        let canonical_url = "https://www.facebook.com/example.alias/posts/pfbid-plugin";
        let hint = PostHint {
            canonical_url,
            published_at: Some("2026-04-08T12:00:36+00:00"),
            external_post_id: Some("pfbid-plugin"),
            text: Some("Thông báo hoạt động sinh viên từ Page Plugin."),
        };
        let (posts, stats) = extract_posts_with_hint(
            "<html><body>public page plugin</body></html>",
            "https://www.facebook.com/example.alias/",
            "browser_playwright",
            "2026-04-08T12:01:00+00:00",
            Some(&hint),
        )
        .unwrap();
        assert_eq!(stats.valid_posts, 1);
        assert_eq!(posts[0].external_post_id, "pfbid-plugin");
        assert_eq!(posts[0].canonical_url, canonical_url);
        assert_eq!(posts[0].published_at, "2026-04-08T12:00:36+00:00");
        assert_eq!(
            posts[0].text,
            "Thông báo hoạt động sinh viên từ Page Plugin."
        );
    }

    #[test]
    fn complete_browser_hint_matches_mixed_case_page_owner() {
        let hint = PostHint {
            canonical_url: "https://www.facebook.com/Example.Alias/posts/pfbid-mixed",
            published_at: Some("2026-04-08T12:00:36+00:00"),
            external_post_id: Some("pfbid-mixed"),
            text: Some("Bài đăng từ permalink có username viết hoa."),
        };
        let (posts, _) = extract_posts_with_hint(
            "<html></html>",
            "https://www.facebook.com/example.alias/",
            "browser_playwright",
            "2026-04-08T12:01:00+00:00",
            Some(&hint),
        )
        .unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].external_post_id, "pfbid-mixed");
    }

    #[test]
    fn people_page_uses_numeric_identity() {
        let url = "https://www.facebook.com/people/example-page/61566022178073/";
        assert_eq!(page_slug(url).unwrap(), "61566022178073");
        assert_eq!(source_id(url).unwrap(), "facebook:61566022178073");
    }

    #[test]
    fn numeric_profile_page_uses_query_identity() {
        let url = "https://www.facebook.com/profile.php?id=100064352813128";
        assert_eq!(page_slug(url).unwrap(), "100064352813128");
        assert_eq!(source_id(url).unwrap(), "facebook:100064352813128");
        assert!(page_slug("https://www.facebook.com/profile.php?id=alias").is_err());
        assert!(page_slug("https://www.facebook.com/profile.php").is_err());
    }

    #[test]
    fn json_script_scanner_handles_attribute_order_quotes_and_case() {
        let html = r#"
            <script nonce="a" TYPE = 'application/json' data-sjs>{"one":1}</script>
            <script type="text/javascript">{"ignored":true}</script>
            <SCRIPT data-x type=application/json>{"two":2}</SCRIPT>
        "#;
        assert_eq!(extract_json_scripts(html), [r#"{"one":1}"#, r#"{"two":2}"#]);
    }

    #[test]
    fn diff_classifies_all_change_types() {
        let (mut current, _) = extract_posts(SUCCESS, PAGE_URL, "fixture", "now").unwrap();
        let original = current[0].clone();
        let mut updated = original.clone();
        updated.external_post_id = "2".to_owned();
        updated.content_hash = "new-hash".to_owned();
        let mut new = original.clone();
        new.external_post_id = "3".to_owned();
        current = vec![original, updated, new];
        let previous = json!({"posts": [
            {"external_post_id": "1448197277342081", "content_hash": current[0].content_hash},
            {"external_post_id": "2", "content_hash": "old-hash"},
            {"external_post_id": "4", "content_hash": "missing"}
        ]});
        let changes = diff_posts(&current, &previous);
        assert_eq!(changes["new"], json!(["3"]));
        assert_eq!(changes["updated"], json!(["2"]));
        assert_eq!(changes["unchanged"], json!(["1448197277342081"]));
        assert_eq!(changes["missing_from_current_window"], json!(["4"]));
    }
}
