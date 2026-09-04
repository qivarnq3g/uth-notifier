use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use url::Url;
use uth_storage::AiLearningExample;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_TEMPERATURE: f64 = 0.1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeminiReviewDecision {
    Send,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiReviewOutput {
    pub decision: GeminiReviewDecision,
    pub reason: String,
    pub confidence: f64,
}

#[derive(Clone)]
pub struct GeminiReviewerClient {
    client: Client,
    api_key: String,
    model: String,
    api_base: Url,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateRequest {
    system_instruction: GeminiContent,
    contents: Vec<GeminiContent>,
    generation_config: GeminiGenerationConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct GeminiContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GeminiPart {
    text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerationConfig {
    temperature: f64,
    response_mime_type: &'static str,
    response_schema: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct GeminiGenerateResponse {
    candidates: Option<Vec<GeminiCandidate>>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    content: Option<GeminiContent>,
}

#[derive(Debug, Deserialize)]
struct RawReviewDecision {
    decision: String,
    reason: String,
    confidence: Option<f64>,
}

impl GeminiReviewerClient {
    pub fn new(api_key: String, model: String, api_base: Url) -> Self {
        let client = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            client,
            api_key,
            model,
            api_base,
        }
    }

    pub async fn review_post(
        &self,
        source_name: &str,
        post_text: &str,
        post_url: &str,
        learning_examples: &[AiLearningExample],
    ) -> Result<GeminiReviewOutput> {
        let base = self.api_base.as_str().trim_end_matches('/');
        let endpoint = format!(
            "{base}/v1beta/models/{}:generateContent?key={}",
            self.model, self.api_key
        );
        let prompt = build_user_prompt(source_name, post_text, post_url, learning_examples);
        let request_payload = GeminiGenerateRequest {
            system_instruction: GeminiContent {
                role: None,
                parts: vec![GeminiPart {
                    text: system_instruction().to_owned(),
                }],
            },
            contents: vec![GeminiContent {
                role: Some("user".to_owned()),
                parts: vec![GeminiPart { text: prompt }],
            }],
            generation_config: GeminiGenerationConfig {
                temperature: DEFAULT_TEMPERATURE,
                response_mime_type: "application/json",
                response_schema: serde_json::json!({
                    "type": "OBJECT",
                    "properties": {
                        "decision": {
                            "type": "STRING",
                            "enum": ["send", "skip"]
                        },
                        "reason": {
                            "type": "STRING"
                        },
                        "confidence": {
                            "type": "NUMBER"
                        }
                    },
                    "required": ["decision", "reason"]
                }),
            },
        };

        let response = self
            .client
            .post(&endpoint)
            .json(&request_payload)
            .send()
            .await
            .context("failed to send request to Gemini API")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            bail!("Gemini API returned error status {status}: {error_text}");
        }

        let response_body: GeminiGenerateResponse = response
            .json()
            .await
            .context("failed to parse Gemini API JSON response")?;

        let raw_text = response_body
            .candidates
            .as_ref()
            .and_then(|c| c.first())
            .and_then(|c| c.content.as_ref())
            .and_then(|c| c.parts.first())
            .map(|p| p.text.as_str())
            .context("Gemini API response contained no candidate text parts")?;

        let raw_decision: RawReviewDecision = serde_json::from_str(raw_text)
            .with_context(|| format!("failed to deserialize structured decision JSON: {raw_text}"))?;

        let decision = match raw_decision.decision.to_lowercase().as_str() {
            "send" => GeminiReviewDecision::Send,
            "skip" => GeminiReviewDecision::Skip,
            other => bail!("unexpected decision from Gemini API: {other}"),
        };

        Ok(GeminiReviewOutput {
            decision,
            reason: raw_decision.reason,
            confidence: raw_decision.confidence.unwrap_or(0.9).clamp(0.0, 1.0),
        })
    }
}

fn system_instruction() -> &'static str {
    "Bạn là hệ thống AI phân loại và duyệt bài đăng tự động cho kênh thông báo sinh viên Đại học Giao thông vận tải TP.HCM (UTH).\n\
     Nhiệm vụ của bạn: Xác định xem bài đăng Facebook này có PHÙ HỢP để gửi thông báo đến toàn thể sinh viên UTH hay không.\n\n\
     Tiêu chuẩn ĐƯỢC DUYỆT (decision = 'send'):\n\
     1. Hoạt động, sự kiện có cấp Điểm Rèn Luyện (ĐRL) hoặc Công tác xã hội (CTXH) cho sinh viên.\n\
     2. Thông báo học bổng, trợ cấp học tập, miễn giảm học phí cho sinh viên.\n\
     3. Cuộc thi học thuật, nghiên cứu khoa học, đổi mới sáng tạo do trường/khoa/đoàn hội tổ chức hoặc đồng tổ chức.\n\
     4. Cơ hội thực tập, việc làm, ngày hội tuyển dụng chính thức từ các doanh nghiệp uy tín dành cho sinh viên UTH.\n\
     5. Hoạt động tình nguyện, chiến dịch tình nguyện Mùa hè xanh, Xuân tình nguyện, hiến máu nhân đạo.\n\
     6. Thông báo đào tạo, khảo sát, quy chế học vụ quan trọng của trường.\n\n\
     Tiêu chuẩn BỎ QUA (decision = 'skip'):\n\
     1. Quảng cáo dịch vụ, khóa học ngoài trường không có xác nhận của UTH hoặc không rõ nguồn gốc.\n\
     2. Tuyển dụng đa cấp, làm việc online không rõ ràng, spam tuyển dụng.\n\
     3. Tâm sự, meme, bài viết cá nhân, tìm đồ thất lạc, bài hát/văn nghệ không có giá trị thông tin chung.\n\
     4. Thông báo nội bộ họp câu lạc bộ/đội nhóm chỉ dành cho thành viên kín của CLB đó, không mở cho sinh viên toàn trường và không có ĐRL.\n\
     5. Bài đăng chỉ có hình ảnh không có nội dung văn bản cụ thể hoặc bài chia sẻ lại không có thông tin hữu ích."
}

fn build_user_prompt(
    source_name: &str,
    post_text: &str,
    post_url: &str,
    learning_examples: &[AiLearningExample],
) -> String {
    let mut prompt = String::new();
    if !learning_examples.is_empty() {
        prompt.push_str("CÁC VÍ DỤ BÀI HỌC SỬA SAI TỪ QUẢN TRỊ VIÊN (BẮT BUỘC ƯU TIÊN TUÂN THEO CÁC TIỀN LỆ NÀY):\n\n");
        for (i, example) in learning_examples.iter().enumerate() {
            prompt.push_str(&format!(
                "Ví dụ {}:\n- Nguồn: {}\n- Bài viết: {}\n- AI từng đoán: {} (Lý do: {})\n- Quản trị viên đã sửa thành: {}{}\n=> BÀI HỌC: Với các bài có tính chất tương tự, quyết định bắt buộc phải là '{}'.\n\n",
                i + 1,
                example.source_name,
                shorten_text(&example.post_text, 300),
                example.ai_decision,
                example.ai_reason,
                example.admin_decision,
                example.admin_notes.as_deref().map(|n| format!(" - Ghi chú: {n}")).unwrap_or_default(),
                example.admin_decision
            ));
        }
        prompt.push_str("---\n\n");
    }

    prompt.push_str(&format!(
        "Hãy đánh giá bài viết sau:\nNguồn: {}\nLink: {}\nNội dung bài viết:\n{}",
        source_name,
        post_url,
        post_text.trim()
    ));

    prompt
}

fn shorten_text(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max_chars {
        trimmed.to_owned()
    } else {
        let mut result = String::new();
        for ch in trimmed.chars().take(max_chars) {
            result.push(ch);
        }
        result.push_str("...");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_prompt_without_learning_examples() {
        let prompt = build_user_prompt(
            "Đoàn trường UTH",
            "Hội thảo nghiên cứu khoa học sinh viên 2026",
            "https://facebook.com/1",
            &[],
        );
        assert!(prompt.contains("Đoàn trường UTH"));
        assert!(prompt.contains("Hội thảo nghiên cứu khoa học"));
        assert!(!prompt.contains("VÍ DỤ BÀI HỌC"));
    }

    #[test]
    fn build_prompt_with_learning_examples() {
        let examples = vec![AiLearningExample {
            id: 1,
            classification_id: Some(10),
            post_id: Some(20),
            post_text: "Tuyển cộng tác viên chốt đơn online".to_owned(),
            source_name: "CLB Việc làm".to_owned(),
            ai_decision: "send".to_owned(),
            ai_reason: "Có từ khóa việc làm".to_owned(),
            admin_decision: "skip".to_owned(),
            admin_notes: Some("Lừa đảo online".to_owned()),
            created_at: chrono::Utc::now(),
        }];

        let prompt = build_user_prompt(
            "CLB Marketing",
            "Cuộc thi Marketing sáng tạo 2026",
            "https://facebook.com/2",
            &examples,
        );
        assert!(prompt.contains("CÁC VÍ DỤ BÀI HỌC"));
        assert!(prompt.contains("Lừa đảo online"));
        assert!(prompt.contains("Cuộc thi Marketing sáng tạo 2026"));
    }

    #[test]
    fn parse_valid_structured_decision() {
        let json = r#"{"decision":"send","reason":"Có cấp ĐRL và học bổng","confidence":0.95}"#;
        let parsed: RawReviewDecision = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.decision, "send");
        assert_eq!(parsed.reason, "Có cấp ĐRL và học bổng");
        assert_eq!(parsed.confidence, Some(0.95));
    }
}
