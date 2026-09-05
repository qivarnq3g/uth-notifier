use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, FixedOffset, Utc};
use regex::Regex;
use reqwest::header::{
    ACCEPT, ACCEPT_LANGUAGE, CONTENT_DISPOSITION, CONTENT_TYPE, REFERER, RETRY_AFTER,
    UPGRADE_INSECURE_REQUESTS, USER_AGENT,
};
use serde::Deserialize;
use url::Url;

const PORTAL_JSON_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;
const PORTAL_ARTICLE_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;
const PORTAL_REQUEST_ATTEMPTS: usize = 3;
const PORTAL_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const PORTAL_ARTICLE_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36 Edg/150.0.0.0";
const PORTAL_ARTICLE_ACCEPT: &str =
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
const PORTAL_ARTICLE_ACCEPT_LANGUAGE: &str = "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7";
pub const TELEGRAM_DOCUMENT_LIMIT: usize = 50 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortalNotice {
    pub portal_id: i64,
    pub title: String,
    pub displayed_at: DateTime<Utc>,
    pub article_url: Option<String>,
    pub attachment_url: Option<String>,
    pub attachment_content_type: Option<String>,
}

#[derive(Debug)]
pub struct PortalAttachment {
    pub bytes: Vec<u8>,
    pub file_name: String,
    pub content_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalFailureKind {
    Forbidden,
    RateLimited,
    Server,
    Network,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortalFailure {
    pub kind: PortalFailureKind,
    pub status: Option<u16>,
    pub retry_after_seconds: Option<u64>,
}

#[derive(Debug)]
struct PortalHttpError {
    status: u16,
    retry_after_seconds: Option<u64>,
}

impl fmt::Display for PortalHttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Portal returned HTTP {}", self.status)
    }
}

impl std::error::Error for PortalHttpError {}

#[derive(Clone)]
pub struct PortalClient {
    client: reqwest::Client,
    base_url: Url,
    file_timeout: Duration,
    max_file_bytes: usize,
}

#[derive(Deserialize)]
struct ApiEnvelope<T> {
    success: bool,
    body: T,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NoticePage {
    content: Vec<NoticeSummary>,
    total_pages: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NoticeSummary {
    id: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NoticeDetail {
    id: i64,
    tieu_de: String,
    noi_dung: String,
    ngay_hien_thi: DateTime<Utc>,
    noi_dung_url: Option<String>,
    noi_dung_file_type: Option<String>,
}

impl PortalClient {
    pub fn new(
        base_url: Url,
        request_timeout: Duration,
        file_timeout: Duration,
        max_file_bytes: usize,
    ) -> Result<Self> {
        if request_timeout.is_zero()
            || file_timeout.is_zero()
            || max_file_bytes == 0
            || max_file_bytes > TELEGRAM_DOCUMENT_LIMIT
        {
            bail!("Portal timeout and file-size limits are invalid");
        }
        validate_api_base(&base_url)?;
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()
            .context("failed to create Portal HTTP client")?;
        Ok(Self {
            client,
            base_url,
            file_timeout,
            max_file_bytes,
        })
    }

    pub async fn latest_portal_id(&self, page_size: usize) -> Result<i64> {
        let page = self.fetch_page(1, page_size).await?;
        Ok(page
            .content
            .into_iter()
            .map(|item| item.id)
            .max()
            .unwrap_or(0))
    }

    pub async fn recent_portal_ids(&self, page_size: usize) -> Result<Vec<i64>> {
        let page = self.fetch_page(1, page_size).await?;
        let mut ids = page
            .content
            .into_iter()
            .map(|item| item.id)
            .filter(|id| *id > 0)
            .collect::<Vec<_>>();
        ids.sort_unstable_by(|left, right| right.cmp(left));
        ids.dedup();
        Ok(ids)
    }

    pub async fn notice_ids_after(
        &self,
        last_seen_portal_id: i64,
        page_size: usize,
        max_pages: usize,
    ) -> Result<Vec<i64>> {
        if last_seen_portal_id < 0 || page_size == 0 || max_pages == 0 {
            bail!("Portal scan parameters are invalid");
        }
        let mut notices = BTreeMap::new();
        let mut reached_boundary = false;
        for page_number in 1..=max_pages {
            let page = self.fetch_page(page_number, page_size).await?;
            let empty = page.content.is_empty();
            for item in page.content {
                if item.id == last_seen_portal_id {
                    reached_boundary = true;
                    break;
                }
                if item.id > 0 {
                    notices.insert(item.id, ());
                }
            }
            if reached_boundary || empty || page_number >= page.total_pages {
                return Ok(notices.into_keys().collect());
            }
        }
        bail!("Portal notice scan exceeded the configured page limit")
    }

    pub async fn fetch_notice(&self, portal_id: i64) -> Result<PortalNotice> {
        if portal_id <= 0 {
            bail!("Portal notice ID must be positive");
        }
        let url = self
            .base_url
            .join(&format!("notification/{portal_id}"))
            .context("failed to build Portal notice URL")?;
        let envelope = self.get_json::<ApiEnvelope<NoticeDetail>>(url).await?;
        if !envelope.success || envelope.body.id != portal_id {
            bail!("Portal returned an invalid notice detail");
        }
        let title = envelope.body.tieu_de.trim().to_owned();
        if title.is_empty() || title.chars().count() > 1_000 {
            bail!("Portal notice title is invalid");
        }
        let article_url = extract_first_https_link(&envelope.body.noi_dung);
        let attachment_url = envelope
            .body
            .noi_dung_url
            .as_deref()
            .map(validate_attachment_url)
            .transpose()?
            .map(|url| url.to_string());
        let attachment_content_type = envelope
            .body
            .noi_dung_file_type
            .as_deref()
            .map(normalize_content_type)
            .transpose()?;
        Ok(PortalNotice {
            portal_id,
            title,
            displayed_at: envelope.body.ngay_hien_thi,
            article_url,
            attachment_url,
            attachment_content_type,
        })
    }

    pub async fn download_attachment(
        &self,
        portal_id: i64,
        raw_url: &str,
        expected_content_type: Option<&str>,
    ) -> Result<PortalAttachment> {
        if portal_id <= 0 {
            bail!("Portal notice ID must be positive");
        }
        let url = validate_attachment_url(raw_url)?;
        let mut response = self
            .client
            .get(url)
            .timeout(self.file_timeout)
            .send()
            .await
            .context("failed to download Portal attachment")?
            .error_for_status()
            .context("Portal attachment returned an error status")?;
        let final_url = validate_attachment_url(response.url().as_str())?;
        if response
            .content_length()
            .is_some_and(|length| length > self.max_file_bytes as u64)
        {
            bail!("Portal attachment exceeds the Telegram file-size limit");
        }
        let response_content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content_type = response_content_type
            .or(expected_content_type)
            .unwrap_or("application/octet-stream");
        let content_type = normalize_content_type(content_type)?;
        if matches!(content_type.as_str(), "text/html" | "application/json") {
            bail!("Portal attachment returned a non-file content type");
        }
        let disposition = response
            .headers()
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok());
        let file_name = attachment_file_name(disposition, portal_id, &content_type, &final_url);
        let mut bytes = Vec::with_capacity(
            response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(0)
                .min(self.max_file_bytes),
        );
        while let Some(chunk) = response
            .chunk()
            .await
            .context("failed to read Portal attachment")?
        {
            if chunk.len() > self.max_file_bytes.saturating_sub(bytes.len()) {
                bail!("Portal attachment exceeds the Telegram file-size limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            bail!("Portal attachment is empty");
        }
        Ok(PortalAttachment {
            bytes,
            file_name,
            content_type,
        })
    }

    pub async fn discover_article_attachment(
        &self,
        raw_article_url: &str,
    ) -> Result<Option<String>> {
        let url = validate_article_url(raw_article_url)?;
        let mut last_error = None;
        for attempt in 1..=PORTAL_REQUEST_ATTEMPTS {
            match self.get_article_html_once(url.clone()).await {
                Ok(html) => return Ok(extract_first_daotao_pdf_link(&html)),
                Err(error) if attempt < PORTAL_REQUEST_ATTEMPTS => {
                    last_error = Some(error);
                    tokio::time::sleep(PORTAL_RETRY_BASE_DELAY * u32::try_from(attempt)?).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.context("Portal article request retry state is invalid")?)
    }

    async fn fetch_page(&self, page: usize, size: usize) -> Result<NoticePage> {
        if page == 0 || size == 0 || size > 100 {
            bail!("Portal page parameters are invalid");
        }
        let mut url = self
            .base_url
            .join("notification")
            .context("failed to build Portal notice-list URL")?;
        url.query_pairs_mut()
            .append_pair("page", &page.to_string())
            .append_pair("size", &size.to_string());
        let envelope = self.get_json::<ApiEnvelope<NoticePage>>(url).await?;
        if !envelope.success {
            bail!("Portal notice-list request was unsuccessful");
        }
        Ok(envelope.body)
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, url: Url) -> Result<T> {
        let mut last_error = None;
        for attempt in 1..=PORTAL_REQUEST_ATTEMPTS {
            match self.get_json_once(url.clone()).await {
                Ok(value) => return Ok(value),
                Err(error)
                    if attempt < PORTAL_REQUEST_ATTEMPTS
                        && matches!(
                            classify_portal_error(&error).kind,
                            PortalFailureKind::Server | PortalFailureKind::Network
                        ) =>
                {
                    last_error = Some(error);
                    tokio::time::sleep(PORTAL_RETRY_BASE_DELAY * u32::try_from(attempt)?).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.context("Portal request retry state is invalid")?)
    }

    async fn get_json_once<T: for<'de> Deserialize<'de>>(&self, url: Url) -> Result<T> {
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .context("Portal request failed")?;
        if !response.status().is_success() {
            let retry_after_seconds = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok());
            return Err(PortalHttpError {
                status: response.status().as_u16(),
                retry_after_seconds,
            }
            .into());
        }
        if response
            .content_length()
            .is_some_and(|length| length > PORTAL_JSON_RESPONSE_LIMIT as u64)
        {
            bail!("Portal JSON response exceeded the size limit");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("failed to read Portal response")?
        {
            if chunk.len() > PORTAL_JSON_RESPONSE_LIMIT.saturating_sub(bytes.len()) {
                bail!("Portal JSON response exceeded the size limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).context("Portal returned invalid JSON")
    }

    async fn get_article_html_once(&self, url: Url) -> Result<String> {
        let mut response = self
            .client
            .get(url)
            .header(USER_AGENT, PORTAL_ARTICLE_USER_AGENT)
            .header(ACCEPT, PORTAL_ARTICLE_ACCEPT)
            .header(ACCEPT_LANGUAGE, PORTAL_ARTICLE_ACCEPT_LANGUAGE)
            .header(REFERER, "https://portal.ut.edu.vn/")
            .header(UPGRADE_INSECURE_REQUESTS, "1")
            .send()
            .await
            .context("Portal article request failed")?
            .error_for_status()
            .context("Portal article returned an error status")?;
        validate_article_url(response.url().as_str())?;
        if response
            .content_length()
            .is_some_and(|length| length > PORTAL_ARTICLE_RESPONSE_LIMIT as u64)
        {
            bail!("Portal article response exceeded the size limit");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("failed to read Portal article response")?
        {
            if chunk.len() > PORTAL_ARTICLE_RESPONSE_LIMIT.saturating_sub(bytes.len()) {
                bail!("Portal article response exceeded the size limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        let html = String::from_utf8(bytes).context("Portal article was not valid UTF-8")?;
        if html.contains("cdn-cgi/challenge-platform")
            || html.contains("<title>Just a moment...</title>")
        {
            bail!("Portal article returned a Cloudflare challenge")
        }
        Ok(html)
    }
}

pub fn classify_portal_error(error: &anyhow::Error) -> PortalFailure {
    for cause in error.chain() {
        if let Some(http) = cause.downcast_ref::<PortalHttpError>() {
            let kind = match http.status {
                403 => PortalFailureKind::Forbidden,
                429 => PortalFailureKind::RateLimited,
                500..=599 => PortalFailureKind::Server,
                _ => PortalFailureKind::Other,
            };
            return PortalFailure {
                kind,
                status: Some(http.status),
                retry_after_seconds: http.retry_after_seconds,
            };
        }
        if let Some(request) = cause.downcast_ref::<reqwest::Error>()
            && (request.is_timeout()
                || request.is_connect()
                || request.is_request()
                || request.is_body())
        {
            return PortalFailure {
                kind: PortalFailureKind::Network,
                status: request.status().map(|status| status.as_u16()),
                retry_after_seconds: None,
            };
        }
    }
    PortalFailure {
        kind: PortalFailureKind::Other,
        status: None,
        retry_after_seconds: None,
    }
}

pub fn render_portal_notification(notice: &PortalNotice) -> String {
    let displayed_at = FixedOffset::east_opt(7 * 60 * 60)
        .map(|offset| {
            notice
                .displayed_at
                .with_timezone(&offset)
                .format("%d/%m/%Y")
                .to_string()
        })
        .unwrap_or_else(|| notice.displayed_at.format("%d/%m/%Y").to_string());
    let header = "Thông báo từ Cổng đào tạo UTH (Portal)";
    let footer = format!("\n\nNgày đăng: {displayed_at}");
    let reserved = header.chars().count() + footer.chars().count() + 2;
    let title = truncate_chars(&notice.title, 1_024_usize.saturating_sub(reserved));
    format!("{header}\n\n{title}{footer}")
}

fn validate_api_base(url: &Url) -> Result<()> {
    if !url.username().is_empty() || url.password().is_some() || url.cannot_be_a_base() {
        bail!("Portal API base URL is invalid");
    }
    let secure = url.scheme() == "https";
    let loopback = url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| matches!(host, "127.0.0.1" | "localhost" | "[::1]" | "::1"));
    if !secure && !loopback {
        bail!("Portal API base URL must use HTTPS");
    }
    Ok(())
}

fn validate_attachment_url(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).context("Portal attachment URL is invalid")?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("Portal attachment URL is outside the approved public endpoint");
    }
    let attachment_id = url
        .path()
        .strip_prefix("/api/v1/notification/getFile/")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0);
    let portal_file = url.host_str() == Some("portal.ut.edu.vn") && attachment_id.is_some();
    let daotao_pdf = is_daotao_pdf_upload_url(&url);
    if !portal_file && !daotao_pdf {
        bail!("Portal attachment URL is outside the approved public endpoint");
    }
    Ok(url)
}

fn validate_article_url(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).context("Portal article URL is invalid")?;
    if url.scheme() != "https"
        || url.host_str() != Some("daotao.ut.edu.vn")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || raw.chars().count() > 2_048
    {
        bail!("Portal article URL is outside the approved public endpoint");
    }
    Ok(url)
}

fn is_daotao_pdf_upload_url(url: &Url) -> bool {
    if url.host_str() != Some("daotao.ut.edu.vn") {
        return false;
    }
    let Some(segments) = url.path_segments() else {
        return false;
    };
    let segments = segments.collect::<Vec<_>>();
    let ["wp-content", "uploads", year, month, file_name] = segments.as_slice() else {
        return false;
    };
    year.len() == 4
        && year.bytes().all(|byte| byte.is_ascii_digit())
        && month.len() == 2
        && month
            .parse::<u8>()
            .is_ok_and(|value| (1..=12).contains(&value))
        && !file_name.is_empty()
        && file_name.len() <= 255
        && file_name.to_ascii_lowercase().ends_with(".pdf")
}

fn extract_first_https_link(html: &str) -> Option<String> {
    let expression = Regex::new(r#"(?i)href\s*=\s*["']([^"']+)["']"#).ok()?;
    expression
        .captures_iter(html)
        .filter_map(|capture| capture.get(1))
        .map(|value| value.as_str().replace("&amp;", "&"))
        .filter_map(|value| Url::parse(&value).ok())
        .find(|url| {
            url.scheme() == "https"
                && matches!(
                    url.host_str(),
                    Some("daotao.ut.edu.vn" | "portal.ut.edu.vn")
                )
                && url.username().is_empty()
                && url.password().is_none()
        })
        .map(|url| url.to_string())
}

fn extract_first_daotao_pdf_link(html: &str) -> Option<String> {
    let attribute_expression = Regex::new(r#"(?i)(?:href|src)\s*=\s*["']([^"']+)["']"#).ok()?;
    let embedded_expression = Regex::new(r#"(?i)https:(?://|\\/\\/)[^"'<> \t\r\n]+?\.pdf"#).ok()?;
    let attribute_links = attribute_expression
        .captures_iter(html)
        .filter_map(|capture| capture.get(1))
        .map(|value| value.as_str());
    let embedded_links = embedded_expression
        .find_iter(html)
        .map(|value| value.as_str());
    attribute_links
        .chain(embedded_links)
        .map(|value| value.replace("\\/", "/").replace("&amp;", "&"))
        .filter_map(|value| validate_attachment_url(&value).ok())
        .find(is_daotao_pdf_upload_url)
        .map(|url| url.to_string())
}

fn normalize_content_type(value: &str) -> Result<String> {
    let normalized = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 255
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'+' | b'-'))
        || !normalized.contains('/')
    {
        bail!("Portal attachment content type is invalid");
    }
    Ok(normalized)
}

fn attachment_file_name(
    disposition: Option<&str>,
    portal_id: i64,
    content_type: &str,
    source_url: &Url,
) -> String {
    let provided = disposition.and_then(|value| {
        value.split(';').find_map(|part| {
            part.trim()
                .strip_prefix("filename=")
                .map(|name| name.trim().trim_matches('"'))
        })
    });
    let source_file_name = is_daotao_pdf_upload_url(source_url)
        .then(|| source_url.path_segments().and_then(Iterator::last))
        .flatten();
    let sanitized = source_file_name
        .or(provided)
        .map(sanitize_attachment_file_name)
        .filter(|name| !name.trim().is_empty());
    sanitized.unwrap_or_else(|| {
        let extension = match content_type {
            "application/pdf" => "pdf",
            "image/png" => "png",
            "image/jpeg" => "jpg",
            "application/zip" => "zip",
            _ => "bin",
        };
        format!("portal-uth-{portal_id}.{extension}")
    })
}

fn sanitize_attachment_file_name(name: &str) -> String {
    name.chars()
        .filter(|character| !character.is_control() && !matches!(character, '/' | '\\' | ':'))
        .take(180)
        .collect()
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::TimeZone;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use url::Url;

    use super::{
        PortalClient, PortalFailureKind, PortalNotice, attachment_file_name, classify_portal_error,
        extract_first_daotao_pdf_link, extract_first_https_link, render_portal_notification,
        validate_attachment_url,
    };

    #[tokio::test]
    async fn retries_a_failed_portal_list_request_within_the_bound() {
        let (base_url, server) = mock_server(vec![
            (503, r#"{"success":false,"body":{}}"#),
            (
                200,
                r#"{"success":true,"body":{"content":[{"id":321}],"totalPages":1}}"#,
            ),
        ])
        .await;
        let client = PortalClient::new(
            Url::parse(&base_url).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            1024,
        )
        .unwrap();

        assert_eq!(client.latest_portal_id(10).await.unwrap(), 321);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /notification?page=1&size=10"));
    }

    #[tokio::test]
    async fn does_not_retry_rate_limit_and_preserves_retry_after() {
        let (base_url, server) = mock_server_with_headers(
            429,
            "Retry-After: 120\r\n",
            r#"{"success":false,"body":{}}"#,
        )
        .await;
        let client = PortalClient::new(
            Url::parse(&base_url).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            1024,
        )
        .unwrap();

        let error = client.latest_portal_id(1).await.unwrap_err();
        let failure = classify_portal_error(&error);
        assert_eq!(failure.kind, PortalFailureKind::RateLimited);
        assert_eq!(failure.status, Some(429));
        assert_eq!(failure.retry_after_seconds, Some(120));
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn returns_recent_portal_ids_in_descending_order_without_duplicates() {
        let (base_url, server) = mock_server(vec![(
            200,
            r#"{"success":true,"body":{"content":[{"id":320},{"id":322},{"id":322},{"id":321}],"totalPages":1}}"#,
        )])
        .await;
        let client = PortalClient::new(
            Url::parse(&base_url).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            1024,
        )
        .unwrap();

        assert_eq!(
            client.recent_portal_ids(20).await.unwrap(),
            vec![322, 321, 320]
        );
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("GET /notification?page=1&size=20"));
    }

    #[tokio::test]
    async fn catches_up_every_id_across_bounded_pages_in_ascending_order() {
        let (base_url, server) = mock_server(vec![
            (
                200,
                r#"{"success":true,"body":{"content":[{"id":105},{"id":104}],"totalPages":2}}"#,
            ),
            (
                200,
                r#"{"success":true,"body":{"content":[{"id":103},{"id":102}],"totalPages":2}}"#,
            ),
        ])
        .await;
        let client = PortalClient::new(
            Url::parse(&base_url).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            1024,
        )
        .unwrap();

        assert_eq!(
            client.notice_ids_after(103, 2, 2).await.unwrap(),
            vec![104, 105]
        );
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("GET /notification?page=1&size=2"));
        assert!(requests[1].starts_with("GET /notification?page=2&size=2"));
    }

    #[tokio::test]
    async fn catches_up_when_feed_ids_are_not_monotonic() {
        let (base_url, server) = mock_server(vec![(
            200,
            r#"{"success":true,"body":{"content":[{"id":1443},{"id":1444}],"totalPages":1}}"#,
        )])
        .await;
        let client = PortalClient::new(
            Url::parse(&base_url).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            1024,
        )
        .unwrap();

        assert_eq!(
            client.notice_ids_after(1444, 2, 1).await.unwrap(),
            vec![1443]
        );
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET /notification?page=1&size=2"));
    }

    #[tokio::test]
    async fn parses_public_portal_detail_and_official_attachment() {
        let (base_url, server) = mock_server(vec![(
            200,
            r#"{"success":true,"body":{"id":321,"tieuDe":"  Thông báo mới  ","noiDung":"<a href=\"https://daotao.ut.edu.vn/thong-bao/321\">Xem</a>","ngayHienThi":"2026-07-27T03:00:00Z","noiDungUrl":"https://portal.ut.edu.vn/api/v1/notification/getFile/2","noiDungFileType":"application/pdf"}}"#,
        )])
        .await;
        let client = PortalClient::new(
            Url::parse(&base_url).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            1024,
        )
        .unwrap();

        let notice = client.fetch_notice(321).await.unwrap();
        assert_eq!(notice.title, "Thông báo mới");
        assert_eq!(
            notice.article_url.as_deref(),
            Some("https://daotao.ut.edu.vn/thong-bao/321")
        );
        assert_eq!(
            notice.attachment_url.as_deref(),
            Some("https://portal.ut.edu.vn/api/v1/notification/getFile/2")
        );
        assert_eq!(
            notice.attachment_content_type.as_deref(),
            Some("application/pdf")
        );
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("GET /notification/321"));
    }

    #[test]
    fn extracts_the_public_article_link_from_portal_html() {
        let html = r#"<a href="https://example.com/not-the-article">ngoài</a><p>Xem <a href="https://daotao.ut.edu.vn/thong-bao/?a=1&amp;b=2">tại đây</a></p>"#;
        assert_eq!(
            extract_first_https_link(html).as_deref(),
            Some("https://daotao.ut.edu.vn/thong-bao/?a=1&b=2")
        );
    }

    #[test]
    fn restricts_attachment_urls_to_the_exact_public_endpoint_shape() {
        assert!(
            validate_attachment_url("https://portal.ut.edu.vn/api/v1/notification/getFile/2")
                .is_ok()
        );
        assert!(
            validate_attachment_url(
                "https://portal.ut.edu.vn/api/v1/notification/getFile/2?token=value"
            )
            .is_err()
        );
        assert!(
            validate_attachment_url("https://example.com/api/v1/notification/getFile/2").is_err()
        );
        assert!(
            validate_attachment_url("https://portal.ut.edu.vn/api/v1/notification/getFile/0")
                .is_err()
        );
        assert!(
            validate_attachment_url(
                "https://daotao.ut.edu.vn/wp-content/uploads/2026/06/thong-bao.pdf"
            )
            .is_ok()
        );
        assert!(
            validate_attachment_url(
                "https://daotao.ut.edu.vn/wp-content/uploads/2026/13/thong-bao.pdf"
            )
            .is_err()
        );
        assert!(
            validate_attachment_url(
                "https://daotao.ut.edu.vn/wp-content/uploads/2026/06/thong-bao.docx"
            )
            .is_err()
        );
    }

    #[test]
    fn extracts_a_pdf_embedded_in_a_public_article() {
        let html = r#"<script>var book={"source":"https:\/\/daotao.ut.edu.vn\/wp-content\/uploads\/2026\/06\/thong-bao.pdf"};</script>"#;
        assert_eq!(
            extract_first_daotao_pdf_link(html).as_deref(),
            Some("https://daotao.ut.edu.vn/wp-content/uploads/2026/06/thong-bao.pdf")
        );
    }

    #[test]
    fn keeps_the_wordpress_pdf_name_instead_of_a_server_generated_name() {
        let article_pdf = Url::parse(
            "https://daotao.ut.edu.vn/wp-content/uploads/2026/06/Quyet-dinh-cong-nhan.pdf",
        )
        .unwrap();
        assert_eq!(
            attachment_file_name(
                Some("attachment; filename=portal-1438-U2i6Cb.pdf"),
                1438,
                "application/pdf",
                &article_pdf,
            ),
            "Quyet-dinh-cong-nhan.pdf"
        );
    }

    #[test]
    fn renders_a_bounded_plain_text_portal_caption() {
        let notice = PortalNotice {
            portal_id: 1,
            title: "A".repeat(2_000),
            displayed_at: chrono::Utc.with_ymd_and_hms(2026, 7, 27, 1, 2, 3).unwrap(),
            article_url: None,
            attachment_url: None,
            attachment_content_type: None,
        };
        let message = render_portal_notification(&notice);
        assert!(message.chars().count() <= 1_024);
        assert!(message.contains("27/07/2026"));
    }

    async fn mock_server(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4_096];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let reason = if status == 200 { "OK" } else { "Error" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                requests.push(String::from_utf8_lossy(&request).into_owned());
            }
            requests
        });
        (format!("http://{address}/"), task)
    }

    async fn mock_server_with_headers(
        status: u16,
        headers: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4_096];
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status} Error\r\n{headers}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            vec![String::from_utf8_lossy(&request).into_owned()]
        });
        (format!("http://{address}/"), task)
    }
}
