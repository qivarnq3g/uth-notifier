use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, FixedOffset, Utc};
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
        published_at: &str,
        current_time: Option<DateTime<FixedOffset>>,
        learning_examples: &[AiLearningExample],
    ) -> Result<GeminiReviewOutput> {
        let base = self.api_base.as_str().trim_end_matches('/');
        let endpoint = format!("{base}/v1beta/models/{}:generateContent", self.model);
        let now = current_time.unwrap_or_else(|| {
            let offset = FixedOffset::east_opt(7 * 3600)
                .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
            Utc::now().with_timezone(&offset)
        });
        let prompt = build_user_prompt(
            source_name,
            post_text,
            post_url,
            published_at,
            now,
            learning_examples,
        );
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
                            "type": "STRING",
                            "description": "Lý do bằng Tiếng Việt chuẩn CÓ DẤU đầy đủ giải thích vì sao chọn send hoặc skip"
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
            .header("x-goog-api-key", &self.api_key)
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

        let raw_decision: RawReviewDecision =
            serde_json::from_str(raw_text).with_context(|| {
                format!("failed to deserialize structured decision JSON: {raw_text}")
            })?;

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
     QUY ĐỊNH BẮT BUỘC VỀ NGÔN NGỮ:\n\
     - Trường 'reason' trong kết quả JSON BẮT BUỘC phải được viết hoàn toàn bằng TIẾNG VIỆT CHUẨN CÓ DẤU đầy đủ (ví dụ: 'Bài viết này là hoạt động của trường khác...' - ĐÚNG; tuyệt đối KHÔNG ĐƯỢC viết không dấu như: 'Bai viet nay la hoat dong cua truong khac...' - SAI).\n\
     - Câu văn giải thích lý do phải ngắn gọn, súc tích, lịch sự, chuẩn ngữ pháp tiếng Việt, không dùng teencode hay từ ngữ viết tắt cẩu thả.\n\n\
     QUY ĐỊNH BẮT BUỘC VỀ THỜI GIAN & TÍNH KỊP THỜI (TRÁNH GỬI BÀI CŨ):\n\
     - Luôn đối chiếu 'THỜI GIAN ĐĂNG BÀI' và các mốc ngày giờ trong bài viết với 'THỜI ĐIỂM HIỆN TẠI':\n\
     - BẮT BUỘC BỎ QUA (decision = 'skip') nếu:\n\
       1. Bài viết đã đăng cách đây từ 3 ngày trở lên so với hiện tại (thông tin đã cũ, không còn kịp thời, tránh làm phiền sinh viên).\n\
       2. Mốc thời gian diễn ra sự kiện, hoạt động hoặc hạn chót đăng ký/nộp hồ sơ trong bài viết ĐÃ QUA trong quá khứ so với thời điểm hiện tại.\n\
       3. Bài viết là bài tổng kết, nhìn lại (recap), cảm ơn sau sự kiện mà không có thông báo hay quyền lợi mới nào tiếp diễn cho sinh viên.\n\
     - Chỉ xem xét duyệt (decision = 'send') khi bài viết vừa đăng gần đây (trong vòng 1-2 ngày) VÀ các hoạt động, sự kiện, hạn chót vẫn còn ở tương lai so với thời điểm hiện tại.\n\n\
     Tiêu chuẩn ĐƯỢC DUYỆT (decision = 'send'):\n\
     1. Hoạt động, sự kiện có cấp Điểm Rèn Luyện (ĐRL) hoặc Công tác xã hội (CTXH) cho sinh viên.\n\
     2. Thông báo học bổng, trợ cấp học tập, miễn giảm học phí cho sinh viên.\n\
     3. Cuộc thi học thuật, nghiên cứu khoa học, đổi mới sáng tạo do trường/khoa/đoàn hội tổ chức hoặc đồng tổ chức.\n\
     4. Cơ hội thực tập, việc làm, ngày hội tuyển dụng chính thức từ các doanh nghiệp uy tín dành cho sinh viên UTH.\n\
     5. Hoạt động tình nguyện, chiến dịch tình nguyện Mùa hè xanh, Xuân tình nguyện, hiến máu nhân đạo.\n\
     6. Thông báo đào tạo, khảo sát, quy chế học vụ quan trọng của trường.\n\n\
     Tiêu chuẩn BỎ QUA (decision = 'skip'):\n\
     1. Bài đăng quá cũ (từ 3 ngày trước trở lên) hoặc sự kiện/hạn chót đã trôi qua.\n\
     2. Bài viết của trường khác hoặc tổ chức ngoài trường không mang lại quyền lợi chung cho sinh viên UTH.\n\
     3. Quảng cáo dịch vụ, khóa học thương mại ngoài trường không có xác nhận của UTH hoặc không rõ nguồn gốc.\n\
     4. Tuyển dụng đa cấp, làm việc online không rõ ràng, spam tuyển dụng.\n\
     5. Tâm sự, meme, bài viết cá nhân, tìm đồ thất lạc, bài hát/văn nghệ, bài nhìn lại khoảnh khắc/recap sự kiện đã qua.\n\
     6. Thông báo nội bộ họp câu lạc bộ/đội nhóm chỉ dành cho thành viên kín của CLB đó, không mở cho sinh viên toàn trường và không có ĐRL.\n\
     7. Bài đăng chỉ có hình ảnh không có nội dung văn bản cụ thể hoặc bài chia sẻ lại không có thông tin hữu ích."
}

fn format_relative_post_age(published_at: &str, now: DateTime<FixedOffset>) -> String {
    if let Ok(dt) = DateTime::parse_from_rfc3339(published_at) {
        let dt_vn = dt.with_timezone(&now.timezone());
        let diff = now.signed_duration_since(dt);
        let relative = if diff.num_minutes() < 1 {
            "vừa đăng".to_owned()
        } else if diff.num_minutes() < 60 {
            format!("{} phút trước", diff.num_minutes())
        } else if diff.num_hours() < 24 {
            format!("{} giờ trước", diff.num_hours())
        } else {
            let days = diff.num_days();
            format!("{days} ngày trước")
        };
        format!("{} ({})", dt_vn.format("%H:%M ngày %d/%m/%Y"), relative)
    } else {
        published_at.to_owned()
    }
}

fn build_user_prompt(
    source_name: &str,
    post_text: &str,
    post_url: &str,
    published_at: &str,
    now: DateTime<FixedOffset>,
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

    let current_time_str = now.format("%H:%M ngày %d/%m/%Y (Giờ Việt Nam)").to_string();
    let published_str = format_relative_post_age(published_at, now);

    prompt.push_str(&format!(
        "THỜI ĐIỂM HIỆN TẠI: {}\n\
         THỜI GIAN ĐĂNG BÀI: {}\n\n\
         Hãy đánh giá bài viết sau:\n\
         Nguồn: {}\n\
         Link: {}\n\
         Nội dung bài viết:\n\
         {}\n\n\
         LƯU Ý BẮT BUỘC:\n\
         1. Trường 'reason' BẮT BUỘC phải viết bằng TIẾNG VIỆT CHUẨN CÓ DẤU đầy đủ, không viết tắt, không dùng tiếng Việt không dấu.\n\
         2. Đối chiếu kỹ thời điểm hiện tại và thời gian đăng bài: Nếu bài đã đăng quá cũ (từ 3 ngày trước trở lên) hoặc hạn chót/thời gian diễn ra sự kiện đã qua trong quá khứ so với thời điểm hiện tại, bắt buộc chọn decision = 'skip'.",
        current_time_str,
        published_str,
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

    fn test_vn_now() -> DateTime<FixedOffset> {
        let offset = FixedOffset::east_opt(7 * 3600).unwrap();
        DateTime::parse_from_rfc3339("2026-09-05T10:00:00+07:00")
            .unwrap()
            .with_timezone(&offset)
    }

    #[test]
    fn format_relative_post_age_variations() {
        let now = test_vn_now();
        assert_eq!(
            format_relative_post_age("2026-09-05T09:59:45+07:00", now),
            "09:59 ngày 05/09/2026 (vừa đăng)"
        );
        assert_eq!(
            format_relative_post_age("2026-09-05T09:30:00+07:00", now),
            "09:30 ngày 05/09/2026 (30 phút trước)"
        );
        assert_eq!(
            format_relative_post_age("2026-09-05T05:00:00+07:00", now),
            "05:00 ngày 05/09/2026 (5 giờ trước)"
        );
        assert_eq!(
            format_relative_post_age("2026-09-01T10:00:00+07:00", now),
            "10:00 ngày 01/09/2026 (4 ngày trước)"
        );
        assert_eq!(
            format_relative_post_age("invalid-date", now),
            "invalid-date"
        );
    }

    #[test]
    fn build_prompt_without_learning_examples() {
        let now = test_vn_now();
        let prompt = build_user_prompt(
            "Đoàn trường UTH",
            "Hội thảo nghiên cứu khoa học sinh viên 2026",
            "https://facebook.com/1",
            "2026-09-05T08:00:00+07:00",
            now,
            &[],
        );
        assert!(prompt.contains("THỜI ĐIỂM HIỆN TẠI: 10:00 ngày 05/09/2026"));
        assert!(prompt.contains("THỜI GIAN ĐĂNG BÀI: 08:00 ngày 05/09/2026 (2 giờ trước)"));
        assert!(prompt.contains("Đoàn trường UTH"));
        assert!(prompt.contains("Hội thảo nghiên cứu khoa học"));
        assert!(prompt.contains("TIẾNG VIỆT CHUẨN CÓ DẤU"));
        assert!(prompt.contains("từ 3 ngày trước trở lên"));
        assert!(!prompt.contains("VÍ DỤ BÀI HỌC"));
    }

    #[test]
    fn build_prompt_with_learning_examples() {
        let now = test_vn_now();
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
            "2026-09-01T10:00:00+07:00",
            now,
            &examples,
        );
        assert!(prompt.contains("THỜI ĐIỂM HIỆN TẠI: 10:00 ngày 05/09/2026"));
        assert!(prompt.contains("THỜI GIAN ĐĂNG BÀI: 10:00 ngày 01/09/2026 (4 ngày trước)"));
        assert!(prompt.contains("CÁC VÍ DỤ BÀI HỌC"));
        assert!(prompt.contains("Lừa đảo online"));
        assert!(prompt.contains("Cuộc thi Marketing sáng tạo 2026"));
    }

    #[test]
    fn system_instruction_contains_language_and_age_rules() {
        let instruction = system_instruction();
        assert!(instruction.contains("TIẾNG VIỆT CHUẨN CÓ DẤU"));
        assert!(instruction.contains("KHÔNG ĐƯỢC viết không dấu"));
        assert!(instruction.contains("từ 3 ngày trở lên"));
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
