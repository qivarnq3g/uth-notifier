//! Versioned domain contracts shared by crawler producers and future consumers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const POST_SCHEMA_VERSION: &str = "facebook-post.v1";
pub const REPORT_SCHEMA_VERSION: &str = "facebook-crawl-report.v1";
pub const CLASSIFICATION_SCHEMA_VERSION: &str = "classification.v1";
pub const EDGE_EVENT_SCHEMA_VERSION: &str = "edge-event.v1";
pub const TELEGRAM_MESSAGE_LIMIT: usize = 4_096;
pub const TELEGRAM_MESSAGE_UTF8_BYTE_LIMIT: usize = TELEGRAM_MESSAGE_LIMIT * 4;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MediaItem {
    pub kind: String,
    pub url: String,
    pub alt_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacebookPost {
    pub schema_version: String,
    pub source_id: String,
    pub platform: String,
    pub external_post_id: String,
    pub canonical_url: String,
    pub published_at: String,
    pub text: String,
    pub media: Vec<MediaItem>,
    pub outbound_links: Vec<String>,
    pub content_hash: String,
    pub crawl_strategy: String,
    pub fetched_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParseStats {
    pub json_scripts: usize,
    pub json_scripts_parsed: usize,
    pub malformed_json_scripts: usize,
    pub candidate_post_ids: usize,
    pub valid_posts: usize,
    pub rejected_missing_timestamp: usize,
    pub rejected_foreign_or_missing_url: usize,
    pub login_wall_detected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserAttemptMetadata {
    pub network_requested_mode: String,
    pub network_effective_mode: String,
    pub network_remote_family: String,
    pub network_fallback_reason: Option<String>,
    pub login_overlay_detected: bool,
    pub login_overlay_dismissed: bool,
    pub login_route_detected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_post_origin: Option<String>,
    #[serde(default)]
    pub newest_dom_post_unresolved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub strategy: String,
    pub outcome: String,
    pub status: Option<u16>,
    pub latency_ms: u128,
    pub bytes_received: usize,
    pub final_url: Option<String>,
    pub posts_found: usize,
    pub newest_post_at: Option<String>,
    pub parse: ParseStats,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser: Option<BrowserAttemptMetadata>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrawlReport {
    pub schema_version: String,
    pub source_url: String,
    pub source_id: String,
    pub fetched_at: String,
    pub selected_strategy: Option<String>,
    pub health: String,
    pub post_count: usize,
    pub attempts: Vec<Attempt>,
    pub posts: Vec<FacebookPost>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassificationDecision {
    Rejected,
    MatchedExplicit,
    ManualReview,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassificationFeatures {
    pub explicit_drl: bool,
    pub registration_call: bool,
    pub form_link: bool,
    pub future_event_time: bool,
    pub future_deadline: bool,
    pub location: bool,
    pub target_students: bool,
    pub approved_source: bool,
    pub negative_commercial: bool,
    pub past_event: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClassificationResult {
    pub schema_version: String,
    pub post_source_id: String,
    pub external_post_id: String,
    pub input_content_hash: String,
    pub decision: ClassificationDecision,
    pub score: i32,
    pub confidence_basis_points: u16,
    pub matched_rules: Vec<String>,
    pub features: ClassificationFeatures,
    pub extracted: Value,
    pub classifier_version: String,
    pub config_hash: String,
    pub classified_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeEvent {
    pub schema_version: String,
    pub event_id: String,
    pub event_type: String,
    pub aggregate_key: String,
    pub sequence: i64,
    pub occurred_at: String,
    pub payload: Value,
}

impl EdgeEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != EDGE_EVENT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported edge event schema {}",
                self.schema_version
            ));
        }
        if self.event_id.is_empty()
            || self.event_id.len() > 200
            || self.event_type.is_empty()
            || self.event_type.len() > 100
            || self.aggregate_key.is_empty()
            || self.aggregate_key.len() > 200
            || self.sequence < 0
            || self.occurred_at.is_empty()
            || !self.payload.is_object()
        {
            return Err("edge event contains invalid required values".to_owned());
        }
        Ok(())
    }
}

pub fn telegram_edge_event(payload: Value, occurred_at: String) -> Result<EdgeEvent, String> {
    let update_id = payload
        .get("update_id")
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or_else(|| "Telegram update_id must be a non-negative integer".to_owned())?;
    let chat_id = telegram_chat_id(&payload);
    let aggregate_key = chat_id
        .map(|value| format!("telegram-chat:{value}"))
        .unwrap_or_else(|| format!("telegram-update:{update_id}"));
    let event = EdgeEvent {
        schema_version: EDGE_EVENT_SCHEMA_VERSION.to_owned(),
        event_id: format!("telegram:{update_id}"),
        event_type: "telegram.update".to_owned(),
        aggregate_key,
        sequence: update_id,
        occurred_at,
        payload,
    };
    event.validate()?;
    Ok(event)
}

fn telegram_chat_id(payload: &Value) -> Option<i64> {
    [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
    ]
    .iter()
    .find_map(|field| {
        payload
            .get(field)
            .and_then(|value| value.get("chat"))
            .and_then(|value| value.get("id"))
            .and_then(Value::as_i64)
    })
    .or_else(|| {
        payload
            .get("callback_query")
            .and_then(|value| value.get("message"))
            .and_then(|value| value.get("chat"))
            .and_then(|value| value.get("id"))
            .and_then(Value::as_i64)
    })
}

#[cfg(test)]
mod edge_tests {
    use serde_json::json;

    use super::{EDGE_EVENT_SCHEMA_VERSION, telegram_edge_event};

    #[test]
    fn telegram_event_uses_chat_as_aggregate() {
        let event = telegram_edge_event(
            json!({"update_id": 42, "message": {"chat": {"id": 7}}}),
            "2026-07-22T00:00:00Z".to_owned(),
        )
        .unwrap();
        assert_eq!(event.schema_version, EDGE_EVENT_SCHEMA_VERSION);
        assert_eq!(event.event_id, "telegram:42");
        assert_eq!(event.aggregate_key, "telegram-chat:7");
        assert_eq!(event.sequence, 42);
    }

    #[test]
    fn telegram_event_rejects_invalid_update_id() {
        assert!(telegram_edge_event(json!({"update_id": -1}), "now".to_owned()).is_err());
    }
}
