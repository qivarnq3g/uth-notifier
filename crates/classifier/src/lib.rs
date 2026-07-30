use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;
use uth_domain::{
    CLASSIFICATION_SCHEMA_VERSION, ClassificationDecision, ClassificationFeatures,
    ClassificationResult, FacebookPost,
};

mod evaluation;

pub use evaluation::{
    EVALUATION_DATASET_SCHEMA_VERSION, EVALUATION_REPORT_SCHEMA_VERSION, EvaluationCase,
    EvaluationDataset, EvaluationFailure, EvaluationReport,
};

const CONFIG_SCHEMA_VERSION: &str = "classifier-rules.v1";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleConfig {
    schema_version: String,
    classifier_version: String,
    max_post_age_days: i64,
    explicit_match_score: i32,
    registration_form_match_score: i32,
    weights: FeatureWeights,
    keywords: KeywordConfig,
    form_hosts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureWeights {
    explicit_drl: i32,
    registration_call: i32,
    form_link: i32,
    future_event_time: i32,
    future_deadline: i32,
    location: i32,
    target_students: i32,
    approved_source: i32,
    negative_commercial: i32,
    past_event: i32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeywordConfig {
    explicit_drl: Vec<String>,
    registration_call: Vec<String>,
    location: Vec<String>,
    target_students: Vec<String>,
    negative_commercial: Vec<String>,
    completed_summary: Vec<String>,
    deadline_context: Vec<String>,
    event_context: Vec<String>,
}

#[derive(Clone)]
pub struct RuleClassifier {
    config: RuleConfig,
    config_hash: String,
    date_pattern: Regex,
}

impl RuleClassifier {
    pub fn from_bytes(raw: &[u8]) -> Result<Self> {
        let config: RuleConfig = serde_json::from_slice(raw).context("invalid rule config JSON")?;
        validate_config(&config)?;
        let config_hash = format!("sha256:{:x}", Sha256::digest(raw));
        let date_pattern = Regex::new(
            r"(?x)\b(?P<date>(?P<year>20\d{2})[-/.](?P<month>1[0-2]|0?[1-9])[-/.](?P<day>3[01]|[12]\d|0?[1-9])|(?P<day_vi>3[01]|[12]\d|0?[1-9])[-/.](?P<month_vi>1[0-2]|0?[1-9])(?:[-/.](?P<year_vi>20\d{2}))?)\b",
        )?;
        Ok(Self {
            config,
            config_hash,
            date_pattern,
        })
    }

    pub fn config_hash(&self) -> &str {
        &self.config_hash
    }

    pub fn classifier_version(&self) -> &str {
        &self.config.classifier_version
    }

    pub fn classify(
        &self,
        post: &FacebookPost,
        approved_source: bool,
        now: DateTime<Utc>,
    ) -> Result<ClassificationResult> {
        let published_at = DateTime::parse_from_rfc3339(&post.published_at)
            .context("invalid post published_at")?
            .with_timezone(&Utc);
        let normalized = normalize_vietnamese(&post.text.to_lowercase());
        let form_links = post
            .outbound_links
            .iter()
            .filter(|link| self.is_form_link(link))
            .cloned()
            .collect::<Vec<_>>();
        let today = now.date_naive();
        let dates = self.extract_dates(&normalized, today);
        let future_deadline = dates
            .iter()
            .any(|date| date.deadline_context && date.value >= today);
        let future_event_time = dates
            .iter()
            .any(|date| date.event_context && date.value >= today);
        let past_deadline = dates.iter().any(|date| date.deadline_context)
            && dates
                .iter()
                .filter(|date| date.deadline_context)
                .all(|date| date.value < today);
        let past_event = dates.iter().any(|date| date.event_context)
            && dates
                .iter()
                .filter(|date| date.event_context)
                .all(|date| date.value < today);
        let features = ClassificationFeatures {
            explicit_drl: contains_any(&normalized, &self.config.keywords.explicit_drl),
            registration_call: contains_any(&normalized, &self.config.keywords.registration_call),
            form_link: !form_links.is_empty(),
            future_event_time,
            future_deadline,
            location: contains_any(&normalized, &self.config.keywords.location),
            target_students: contains_any(&normalized, &self.config.keywords.target_students),
            approved_source,
            negative_commercial: contains_any(
                &normalized,
                &self.config.keywords.negative_commercial,
            ),
            past_event,
        };
        let completed_summary = contains_any(&normalized, &self.config.keywords.completed_summary);
        let post_too_old =
            now.signed_duration_since(published_at).num_days() > self.config.max_post_age_days;
        let mut matched_rules = matched_feature_rules(&features);
        if past_deadline {
            matched_rules.push("hard.deadline_passed".to_owned());
        }
        if completed_summary {
            matched_rules.push("hard.completed_summary".to_owned());
        }
        if post_too_old {
            matched_rules.push("hard.post_too_old".to_owned());
        }
        if !approved_source {
            matched_rules.push("hard.unapproved_source".to_owned());
        }
        let score = score_features(&features, &self.config.weights);
        let hard_reject = !approved_source
            || post_too_old
            || past_deadline
            || (features.past_event && !features.future_deadline)
            || features.negative_commercial
            || completed_summary;
        let explicit_match = score >= self.config.explicit_match_score
            && features.explicit_drl
            && features.registration_call;
        let registration_form_match = score >= self.config.registration_form_match_score
            && features.registration_call
            && features.form_link
            && features.target_students
            && (features.future_event_time || features.future_deadline || features.location);
        let decision = if hard_reject {
            ClassificationDecision::Rejected
        } else if explicit_match {
            matched_rules.push("decision.explicit_threshold".to_owned());
            ClassificationDecision::MatchedExplicit
        } else if registration_form_match {
            matched_rules.push("decision.registration_form_threshold".to_owned());
            ClassificationDecision::MatchedExplicit
        } else if features.explicit_drl || features.registration_call || features.form_link {
            matched_rules.push("decision.insufficient_evidence".to_owned());
            ClassificationDecision::ManualReview
        } else {
            matched_rules.push("decision.no_actionable_evidence".to_owned());
            ClassificationDecision::Rejected
        };
        let confidence_basis_points = decision_confidence(
            &decision,
            score,
            if registration_form_match {
                self.config.registration_form_match_score
            } else {
                self.config.explicit_match_score
            },
            matched_rules
                .iter()
                .filter(|rule| rule.starts_with("hard."))
                .count(),
        );
        let extracted_dates = dates
            .iter()
            .map(|date| {
                json!({
                    "date": date.value.to_string(),
                    "deadline_context": date.deadline_context,
                    "event_context": date.event_context
                })
            })
            .collect::<Vec<_>>();
        Ok(ClassificationResult {
            schema_version: CLASSIFICATION_SCHEMA_VERSION.to_owned(),
            post_source_id: post.source_id.clone(),
            external_post_id: post.external_post_id.clone(),
            input_content_hash: post.content_hash.clone(),
            decision,
            score,
            confidence_basis_points,
            matched_rules,
            features,
            extracted: json!({
                "dates": extracted_dates,
                "form_links": form_links,
                "past_deadline": past_deadline,
                "completed_summary": completed_summary,
                "post_too_old": post_too_old
            }),
            classifier_version: self.config.classifier_version.clone(),
            config_hash: self.config_hash.clone(),
            classified_at: now.to_rfc3339(),
        })
    }

    fn is_form_link(&self, link: &str) -> bool {
        let Ok(url) = Url::parse(link) else {
            return false;
        };
        let Some(host) = url.host_str() else {
            return false;
        };
        self.config
            .form_hosts
            .iter()
            .any(|configured| host == configured || host.ends_with(&format!(".{configured}")))
            || ["dang-ky", "dangky", "register", "registration"]
                .iter()
                .any(|marker| url.path().to_ascii_lowercase().contains(marker))
    }

    fn extract_dates(&self, text: &str, today: NaiveDate) -> Vec<ExtractedDate> {
        self.date_pattern
            .captures_iter(text)
            .filter_map(|captures| {
                let matched = captures.name("date")?;
                let date = if let Some(year) = captures.name("year") {
                    parse_date(
                        year.as_str(),
                        captures.name("month")?.as_str(),
                        captures.name("day")?.as_str(),
                    )
                } else if let Some(year) = captures.name("year_vi") {
                    parse_date(
                        year.as_str(),
                        captures.name("month_vi")?.as_str(),
                        captures.name("day_vi")?.as_str(),
                    )
                } else {
                    infer_yearless_date(
                        captures.name("month_vi")?.as_str(),
                        captures.name("day_vi")?.as_str(),
                        today,
                    )
                }?;
                let context = surrounding_text(text, matched.start(), matched.end(), 64);
                Some(ExtractedDate {
                    value: date,
                    deadline_context: contains_any(
                        &context,
                        &self.config.keywords.deadline_context,
                    ),
                    event_context: contains_any(&context, &self.config.keywords.event_context),
                })
            })
            .collect()
    }
}

struct ExtractedDate {
    value: NaiveDate,
    deadline_context: bool,
    event_context: bool,
}

fn validate_config(config: &RuleConfig) -> Result<()> {
    if config.schema_version != CONFIG_SCHEMA_VERSION {
        bail!("unsupported rule config schema {}", config.schema_version);
    }
    if config.classifier_version.trim().is_empty()
        || config.max_post_age_days <= 0
        || config.explicit_match_score <= 0
        || config.registration_form_match_score <= 0
        || config.form_hosts.is_empty()
    {
        bail!("rule config contains invalid required values");
    }
    Ok(())
}

fn parse_date(year: &str, month: &str, day: &str) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(year.parse().ok()?, month.parse().ok()?, day.parse().ok()?)
}

fn infer_yearless_date(month: &str, day: &str, today: NaiveDate) -> Option<NaiveDate> {
    let month = month.parse().ok()?;
    let day = day.parse().ok()?;
    let current = NaiveDate::from_ymd_opt(today.year(), month, day)?;
    let distance = current.signed_duration_since(today).num_days();
    if distance < -183 {
        NaiveDate::from_ymd_opt(today.year() + 1, month, day)
    } else if distance > 183 {
        NaiveDate::from_ymd_opt(today.year() - 1, month, day)
    } else {
        Some(current)
    }
}

fn normalize_vietnamese(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            'à' | 'á' | 'ạ' | 'ả' | 'ã' | 'â' | 'ầ' | 'ấ' | 'ậ' | 'ẩ' | 'ẫ' | 'ă' | 'ằ' | 'ắ'
            | 'ặ' | 'ẳ' | 'ẵ' => 'a',
            'è' | 'é' | 'ẹ' | 'ẻ' | 'ẽ' | 'ê' | 'ề' | 'ế' | 'ệ' | 'ể' | 'ễ' => {
                'e'
            }
            'ì' | 'í' | 'ị' | 'ỉ' | 'ĩ' => 'i',
            'ò' | 'ó' | 'ọ' | 'ỏ' | 'õ' | 'ô' | 'ồ' | 'ố' | 'ộ' | 'ổ' | 'ỗ' | 'ơ' | 'ờ' | 'ớ'
            | 'ợ' | 'ở' | 'ỡ' => 'o',
            'ù' | 'ú' | 'ụ' | 'ủ' | 'ũ' | 'ư' | 'ừ' | 'ứ' | 'ự' | 'ử' | 'ữ' => {
                'u'
            }
            'ỳ' | 'ý' | 'ỵ' | 'ỷ' | 'ỹ' => 'y',
            'đ' => 'd',
            _ => character,
        })
        .collect()
}

fn surrounding_text(text: &str, start: usize, end: usize, radius: usize) -> String {
    let mut context_start = start.saturating_sub(radius);
    while !text.is_char_boundary(context_start) {
        context_start += 1;
    }
    let mut context_end = end.saturating_add(radius).min(text.len());
    while !text.is_char_boundary(context_end) {
        context_end -= 1;
    }
    text[context_start..context_end].to_owned()
}

fn contains_any(text: &str, keywords: &[String]) -> bool {
    keywords
        .iter()
        .any(|keyword| text.contains(&normalize_vietnamese(&keyword.to_lowercase())))
}

fn matched_feature_rules(features: &ClassificationFeatures) -> Vec<String> {
    let pairs = [
        (features.explicit_drl, "feature.explicit_drl"),
        (features.registration_call, "feature.registration_call"),
        (features.form_link, "feature.form_link"),
        (features.future_event_time, "feature.future_event_time"),
        (features.future_deadline, "feature.future_deadline"),
        (features.location, "feature.location"),
        (features.target_students, "feature.target_students"),
        (features.approved_source, "feature.approved_source"),
        (features.negative_commercial, "feature.negative_commercial"),
        (features.past_event, "feature.past_event"),
    ];
    pairs
        .into_iter()
        .filter(|(matched, _)| *matched)
        .map(|(_, name)| name.to_owned())
        .collect()
}

fn score_features(features: &ClassificationFeatures, weights: &FeatureWeights) -> i32 {
    let pairs = [
        (features.explicit_drl, weights.explicit_drl),
        (features.registration_call, weights.registration_call),
        (features.form_link, weights.form_link),
        (features.future_event_time, weights.future_event_time),
        (features.future_deadline, weights.future_deadline),
        (features.location, weights.location),
        (features.target_students, weights.target_students),
        (features.approved_source, weights.approved_source),
        (features.negative_commercial, weights.negative_commercial),
        (features.past_event, weights.past_event),
    ];
    pairs
        .into_iter()
        .filter(|(matched, _)| *matched)
        .map(|(_, weight)| weight)
        .sum()
}

fn decision_confidence(
    decision: &ClassificationDecision,
    score: i32,
    threshold: i32,
    hard_rule_count: usize,
) -> u16 {
    match decision {
        ClassificationDecision::Rejected => {
            8_500_u16.saturating_add(u16::try_from(hard_rule_count.min(3)).unwrap_or(3) * 500)
        }
        ClassificationDecision::MatchedExplicit => {
            let margin = score.saturating_sub(threshold).clamp(0, 10);
            8_000_u16.saturating_add(u16::try_from(margin).unwrap_or(10) * 150)
        }
        ClassificationDecision::ManualReview => 5_000,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use uth_domain::{ClassificationDecision, FacebookPost, POST_SCHEMA_VERSION};

    use super::RuleClassifier;

    const CONFIG: &[u8] = include_bytes!("../../../config/classifier-rules.v1.json");

    #[test]
    fn explicit_future_activity_matches() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let mut post = post(
            "sha256:explicit",
            "Mời sinh viên đăng ký tham gia hoạt động được cộng điểm rèn luyện. Hạn đăng ký 25/07/2026. Địa điểm Hội trường A.",
        );
        post.outbound_links = vec!["https://forms.gle/example".to_owned()];
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_eq!(result.decision, ClassificationDecision::MatchedExplicit);
        assert!(result.features.explicit_drl);
        assert!(result.features.future_deadline);
        assert!(result.features.form_link);
    }

    #[test]
    fn online_course_activity_with_explicit_drl_matches() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let mut post = post(
            "sha256:online-course",
            "Hoạt động dành cho sinh viên được đề xuất cộng điểm rèn luyện. Thời gian từ ngày 29/07/2026 đến ngày 31/07/2026. Hình thức: Tham gia trực tuyến trên hệ thống Courses UTH.",
        );
        post.outbound_links = vec!["https://courses.ut.edu.vn/course/view.php?id=29965".to_owned()];
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_eq!(result.decision, ClassificationDecision::MatchedExplicit);
        assert!(result.features.explicit_drl);
        assert!(result.features.registration_call);
        assert!(result.features.future_event_time);
        assert!(!result.features.form_link);
    }

    #[test]
    fn commercial_post_is_rejected() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let post = post(
            "sha256:commercial",
            "Tuyển nhân viên bán hàng, đăng ký ngay để nhận khuyến mãi.",
        );
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_eq!(result.decision, ClassificationDecision::Rejected);
        assert!(result.features.negative_commercial);
    }

    #[test]
    fn expired_deadline_is_rejected() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let post = post(
            "sha256:past",
            "Hoạt động điểm rèn luyện dành cho sinh viên. Hạn đăng ký 10/07/2026.",
        );
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_eq!(result.decision, ClassificationDecision::Rejected);
        assert_eq!(result.extracted["past_deadline"], true);
    }

    #[test]
    fn actionable_post_without_explicit_drl_requires_review() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let post = post(
            "sha256:ambiguous",
            "Mời sinh viên đăng ký tham gia chương trình tại Hội trường A.",
        );
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_eq!(result.decision, ClassificationDecision::ManualReview);
    }

    #[test]
    fn trusted_student_registration_form_matches_without_explicit_drl() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let mut post = post(
            "sha256:registration-form",
            "Sinh viên đăng ký tham gia chương trình trực tuyến tại link đăng ký.",
        );
        post.outbound_links = vec!["https://docs.google.com/forms/d/e/example/viewform".to_owned()];
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_eq!(result.decision, ClassificationDecision::MatchedExplicit);
        assert!(!result.features.explicit_drl);
        assert!(result.features.registration_call);
        assert!(result.features.form_link);
        assert!(result.features.target_students);
        assert!(
            result
                .matched_rules
                .contains(&"decision.registration_form_threshold".to_owned())
        );
    }

    #[test]
    fn accentless_text_and_yearless_deadline_are_understood() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let mut post = post(
            "sha256:accentless",
            "Moi sinh vien dang ky tham gia hoat dong diem ren luyen. Han dang ky 25/07.",
        );
        post.outbound_links = vec!["https://example.edu/dang-ky/hoat-dong".to_owned()];
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_eq!(result.decision, ClassificationDecision::MatchedExplicit);
        assert!(result.features.explicit_drl);
        assert!(result.features.future_deadline);
        assert!(result.features.form_link);
    }

    #[test]
    fn historical_event_does_not_override_future_deadline() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let mut post = post(
            "sha256:mixed-dates",
            "Chuong trinh ky niem ngay 10/07/2026. Moi sinh vien dang ky diem ren luyen, han dang ky 25/07/2026.",
        );
        post.outbound_links = vec!["https://forms.gle/example".to_owned()];
        let result = classifier.classify(&post, true, now()).unwrap();

        assert_ne!(result.decision, ClassificationDecision::Rejected);
        assert!(result.features.future_deadline);
    }

    #[test]
    fn expired_han_chot_is_rejected_without_parsing_academic_year() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let post = post(
            "sha256:expired-han-chot",
            "[ULC] GÓC NHẮC NHỞ - ĐÁNH GIÁ KẾT QUẢ RÈN LUYỆN SINH VIÊN HK2 (2025-2026). Hạn chót: Trước ngày 26/07/2026.",
        );
        let result = classifier
            .classify(
                &post,
                true,
                Utc.with_ymd_and_hms(2026, 7, 28, 2, 0, 0).unwrap(),
            )
            .unwrap();

        assert_eq!(result.decision, ClassificationDecision::Rejected);
        assert_eq!(result.extracted["past_deadline"], true);
        assert_eq!(result.extracted["dates"].as_array().unwrap().len(), 1);
        assert_eq!(result.extracted["dates"][0]["date"], "2026-07-26");
    }

    #[test]
    fn academic_year_range_is_not_a_date() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let post = post(
            "sha256:academic-year",
            "Thông báo đánh giá rèn luyện học kỳ 2 năm học 2025-2026.",
        );
        let result = classifier.classify(&post, true, now()).unwrap();

        assert!(result.extracted["dates"].as_array().unwrap().is_empty());
    }

    #[test]
    fn config_hash_is_stable() {
        let first = RuleClassifier::from_bytes(CONFIG).unwrap();
        let second = RuleClassifier::from_bytes(CONFIG).unwrap();

        assert_eq!(first.config_hash(), second.config_hash());
        assert!(first.config_hash().starts_with("sha256:"));
    }

    fn now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap()
    }

    fn post(content_hash: &str, text: &str) -> FacebookPost {
        FacebookPost {
            schema_version: POST_SCHEMA_VERSION.to_owned(),
            source_id: "facebook:source.a".to_owned(),
            platform: "facebook".to_owned(),
            external_post_id: "post-1".to_owned(),
            canonical_url: "https://www.facebook.com/source.a/posts/post-1".to_owned(),
            published_at: "2026-07-18T02:00:17+00:00".to_owned(),
            text: text.to_owned(),
            media: Vec::new(),
            outbound_links: Vec::new(),
            content_hash: content_hash.to_owned(),
            crawl_strategy: "standard".to_owned(),
            fetched_at: "2026-07-19T02:00:17+00:00".to_owned(),
        }
    }
}
