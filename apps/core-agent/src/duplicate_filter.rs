use std::collections::BTreeSet;

use url::Url;
use uth_classifier::normalize_vietnamese;
use uth_storage::RecentCampaignCandidate;

use crate::gemini_reviewer::DuplicateCampaignContext;

const MAX_DUPLICATE_CANDIDATES: usize = 4;
const MAX_EXCERPT_CHARS: usize = 320;
const MIN_EXACT_TOKEN_COUNT: usize = 15;
const EXACT_JACCARD_THRESHOLD: f64 = 0.93;
const EXACT_CONTAINMENT_THRESHOLD: f64 = 0.95;
const MAX_CONTAINMENT_EXTRA_TOKENS: usize = 10;
const SUSPECTED_TOKEN_JACCARD_THRESHOLD: f64 = 0.30;
const SUSPECTED_SHINGLE_JACCARD_THRESHOLD: f64 = 0.25;
const SUSPECTED_FIRST_LINE_JACCARD_THRESHOLD: f64 = 0.65;
const MIN_FIRST_LINE_TOKENS: usize = 4;
const DEFER_ON_AI_ERROR_SIMILARITY: f64 = 0.70;
const DEFER_ON_AI_ERROR_SHARED_LINK_SIMILARITY: f64 = 0.45;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactDuplicateMatch {
    pub campaign_id: i64,
    pub source_name: String,
    pub similarity_percent: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DuplicateCandidate {
    pub context: DuplicateCampaignContext,
    pub has_shared_link: bool,
    pub similarity: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct DuplicateAssessment {
    pub exact_match: Option<ExactDuplicateMatch>,
    pub suspected_candidates: Vec<DuplicateCandidate>,
    pub defer_on_ai_error: bool,
}

pub fn assess_cross_page_duplicates(
    candidate_text: &str,
    candidate_outbound_links: &[String],
    candidate_action_url: Option<&str>,
    recent_campaigns: &[RecentCampaignCandidate],
) -> DuplicateAssessment {
    if recent_campaigns.is_empty() {
        return DuplicateAssessment::default();
    }

    let candidate_links = extract_identifying_urls(
        candidate_text,
        candidate_outbound_links,
        candidate_action_url,
    );
    let candidate_tokens = normalized_tokens(candidate_text);
    let candidate_token_set: BTreeSet<&str> = candidate_tokens.iter().map(String::as_str).collect();
    let candidate_shingles = token_shingles(&candidate_tokens, 3);
    let candidate_numbers = extract_number_tokens(candidate_text);
    let candidate_first_line = first_meaningful_line_tokens(candidate_text);

    let mut exact_match: Option<(f64, ExactDuplicateMatch)> = None;
    let mut suspected = Vec::new();

    for existing in recent_campaigns {
        let existing_links = extract_identifying_urls(
            &existing.text,
            &existing.outbound_links,
            existing.action_url.as_deref(),
        );
        let shared_links: Vec<String> = candidate_links
            .intersection(&existing_links)
            .cloned()
            .collect();
        let has_shared_link = !shared_links.is_empty();

        let existing_tokens = normalized_tokens(&existing.text);
        let existing_token_set: BTreeSet<&str> =
            existing_tokens.iter().map(String::as_str).collect();
        let token_jaccard = set_jaccard(&candidate_token_set, &existing_token_set);
        let containment = set_containment(&existing_token_set, &candidate_token_set);
        let existing_shingles = token_shingles(&existing_tokens, 3);
        let shingle_jaccard = set_jaccard(&candidate_shingles, &existing_shingles);
        let first_line_jaccard = set_jaccard(
            &candidate_first_line,
            &first_meaningful_line_tokens(&existing.text),
        );

        let same_numbers = candidate_numbers == extract_number_tokens(&existing.text);
        let extra_tokens: Vec<String> = candidate_tokens
            .iter()
            .filter(|token| !existing_token_set.contains(token.as_str()))
            .cloned()
            .collect();

        let is_exact = candidate_tokens.len() >= MIN_EXACT_TOKEN_COUNT
            && existing_tokens.len() >= MIN_EXACT_TOKEN_COUNT
            && same_numbers
            && !contains_update_cue(&extra_tokens)
            && (token_jaccard >= EXACT_JACCARD_THRESHOLD
                || (containment >= EXACT_CONTAINMENT_THRESHOLD
                    && extra_tokens.len() <= MAX_CONTAINMENT_EXTRA_TOKENS));

        let combined_similarity = token_jaccard.max(shingle_jaccard);
        if is_exact {
            let score = combined_similarity.max(containment);
            let similarity_percent = (score * 100.0).round().clamp(0.0, 100.0) as u8;
            let replace = exact_match
                .as_ref()
                .is_none_or(|(best_score, _)| score > *best_score);
            if replace {
                exact_match = Some((
                    score,
                    ExactDuplicateMatch {
                        campaign_id: existing.campaign_id,
                        source_name: existing.source_name.clone(),
                        similarity_percent,
                    },
                ));
            }
        }

        let is_suspected = has_shared_link
            || token_jaccard >= SUSPECTED_TOKEN_JACCARD_THRESHOLD
            || shingle_jaccard >= SUSPECTED_SHINGLE_JACCARD_THRESHOLD
            || (candidate_first_line.len() >= MIN_FIRST_LINE_TOKENS
                && first_line_jaccard >= SUSPECTED_FIRST_LINE_JACCARD_THRESHOLD);

        if is_suspected {
            suspected.push(DuplicateCandidate {
                context: DuplicateCampaignContext {
                    campaign_id: existing.campaign_id,
                    source_name: existing.source_name.clone(),
                    sent_at: existing.created_at,
                    shared_links,
                    text_excerpt: truncate_chars(&existing.text, MAX_EXCERPT_CHARS),
                },
                has_shared_link,
                similarity: combined_similarity,
            });
        }
    }

    suspected.sort_by(|left, right| {
        right
            .has_shared_link
            .cmp(&left.has_shared_link)
            .then_with(|| {
                right
                    .similarity
                    .partial_cmp(&left.similarity)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| right.context.sent_at.cmp(&left.context.sent_at))
            .then_with(|| right.context.campaign_id.cmp(&left.context.campaign_id))
    });
    suspected.truncate(MAX_DUPLICATE_CANDIDATES);

    let defer_on_ai_error = suspected.iter().any(|candidate| {
        candidate.similarity >= DEFER_ON_AI_ERROR_SIMILARITY
            || (candidate.has_shared_link
                && candidate.similarity >= DEFER_ON_AI_ERROR_SHARED_LINK_SIMILARITY)
    });

    DuplicateAssessment {
        exact_match: exact_match.map(|(_, matched)| matched),
        suspected_candidates: suspected,
        defer_on_ai_error,
    }
}

pub fn duplicate_contexts(assessment: &DuplicateAssessment) -> Vec<DuplicateCampaignContext> {
    assessment
        .suspected_candidates
        .iter()
        .map(|candidate| candidate.context.clone())
        .collect()
}

fn extract_identifying_urls(
    text: &str,
    outbound_links: &[String],
    action_url: Option<&str>,
) -> BTreeSet<String> {
    let mut urls = BTreeSet::new();
    for raw in action_url
        .into_iter()
        .chain(outbound_links.iter().map(String::as_str))
    {
        if let Some(normalized) = normalize_comparable_url(raw)
            && is_identifying_url(&normalized)
        {
            urls.insert(normalized.to_string());
        }
    }
    for word in text.split_whitespace() {
        let trimmed = word.trim_matches(|ch: char| {
            matches!(
                ch,
                '.' | ','
                    | ';'
                    | ':'
                    | '!'
                    | '?'
                    | ')'
                    | ']'
                    | '}'
                    | '"'
                    | '\''
                    | '>'
                    | '<'
                    | '('
                    | '['
            )
        });
        if (trimmed.starts_with("http://") || trimmed.starts_with("https://"))
            && let Some(normalized) = normalize_comparable_url(trimmed)
            && is_identifying_url(&normalized)
        {
            urls.insert(normalized.to_string());
        }
    }
    urls
}

fn normalize_comparable_url(raw: &str) -> Option<Url> {
    let parsed = Url::parse(raw.trim()).ok()?;
    normalize_comparable_url_depth(parsed, 0)
}

fn normalize_comparable_url_depth(mut url: Url, depth: u8) -> Option<Url> {
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let host = url.host_str()?.to_ascii_lowercase();
    if depth == 0
        && matches!(host.as_str(), "l.facebook.com" | "lm.facebook.com")
        && url.path() == "/l.php"
        && let Some((_, target)) = url.query_pairs().find(|(key, _)| key == "u")
        && let Ok(unwrapped) = Url::parse(&target)
    {
        return normalize_comparable_url_depth(unwrapped, 1);
    }

    let canonical_host = match host.as_str() {
        "facebook.com" | "m.facebook.com" | "mbasic.facebook.com" => "www.facebook.com",
        other => other,
    };
    url.set_scheme("https").ok()?;
    url.set_host(Some(canonical_host)).ok()?;
    url.set_port(None).ok()?;
    url.set_fragment(None);

    let trimmed_path = url.path().trim_end_matches('/').to_owned();
    if trimmed_path.is_empty() {
        url.set_path("/");
    } else {
        url.set_path(&trimmed_path);
    }

    let mut kept_pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| !is_tracking_query_param(key))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    kept_pairs.sort();
    if kept_pairs.is_empty() {
        url.set_query(None);
    } else {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in kept_pairs {
            serializer.append_pair(&key, &value);
        }
        let query = serializer.finish();
        url.set_query(Some(&query));
    }

    Some(url)
}

fn is_tracking_query_param(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "fbclid" | "mibextid" | "ref" | "ref_src" | "usp" | "gclid" | "s" | "si" | "igsh" | "h"
    ) || lower.starts_with("utm_")
        || lower.starts_with("__cft__")
        || lower.starts_with("__tn__")
        || lower.starts_with("c[")
}

fn is_identifying_url(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    if host == "www.facebook.com" {
        let path = url.path().to_ascii_lowercase();
        let has_post_path = path.contains("/posts/")
            || path.starts_with("/permalink.php")
            || path.starts_with("/story.php")
            || path.starts_with("/events/")
            || path.starts_with("/photo")
            || path.starts_with("/share/");
        let has_post_query = url
            .query_pairs()
            .any(|(key, _)| matches!(key.as_ref(), "story_fbid" | "fbid"));
        return has_post_path || has_post_query;
    }
    !(url.path() == "/" && url.query().is_none())
}

fn normalized_tokens(text: &str) -> Vec<String> {
    let without_urls_and_tags: String = text
        .split_whitespace()
        .filter(|word| {
            !word.starts_with("http://") && !word.starts_with("https://") && !word.starts_with('#')
        })
        .collect::<Vec<_>>()
        .join(" ");
    let ascii = normalize_vietnamese(&without_urls_and_tags.to_lowercase());
    ascii
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| {
            !token.is_empty()
                && (token.chars().count() >= 2 || token.chars().all(|ch| ch.is_ascii_digit()))
        })
        .map(str::to_owned)
        .collect()
}

fn first_meaningful_line_tokens(text: &str) -> BTreeSet<String> {
    for line in text.lines() {
        let tokens = normalized_tokens(line);
        if tokens.len() >= 2 {
            return tokens.into_iter().collect();
        }
    }
    BTreeSet::new()
}

fn token_shingles(tokens: &[String], width: usize) -> BTreeSet<Vec<&str>> {
    if tokens.len() < width {
        return BTreeSet::new();
    }
    tokens
        .windows(width)
        .map(|window| window.iter().map(String::as_str).collect())
        .collect()
}

fn set_jaccard<T: Ord>(left: &BTreeSet<T>, right: &BTreeSet<T>) -> f64 {
    if left.is_empty() && right.is_empty() {
        return 0.0;
    }
    let intersection = left.intersection(right).count();
    let union = left.len() + right.len() - intersection;
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn set_containment<T: Ord>(
    subset_candidate: &BTreeSet<T>,
    superset_candidate: &BTreeSet<T>,
) -> f64 {
    if subset_candidate.is_empty() {
        return 0.0;
    }
    let intersection = subset_candidate.intersection(superset_candidate).count();
    intersection as f64 / subset_candidate.len() as f64
}

fn extract_number_tokens(text: &str) -> BTreeSet<String> {
    let mut numbers = BTreeSet::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if word.starts_with("http://") || word.starts_with("https://") || word.starts_with('#') {
            continue;
        }
        for ch in word.chars() {
            if ch.is_ascii_digit() {
                current.push(ch);
            } else if !current.is_empty() {
                numbers.insert(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            numbers.insert(std::mem::take(&mut current));
        }
    }
    numbers
}

fn contains_update_cue(tokens: &[String]) -> bool {
    if tokens.is_empty() {
        return false;
    }
    let joined = format!(" {} ", tokens.join(" "));
    const CUES: &[&str] = &[
        " doi ngay ",
        " doi lich ",
        " hoan ",
        " thay doi ",
        " doi dia diem ",
        " cap nhat ",
        " bo sung ",
        " gia han ",
        " mo them ",
        " dot 2 ",
        " dot 3 ",
        " vong 2 ",
        " vong 3 ",
        " ban ket ",
        " chung ket ",
        " ket qua ",
        " danh sach ",
        " han cuoi ",
        " tap 2 ",
        " tap 3 ",
        " ky 2 ",
        " ky 3 ",
        " phan 2 ",
        " phan 3 ",
        " series ",
    ];
    CUES.iter().any(|cue| joined.contains(cue))
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let mut out: String = collapsed.chars().take(max_chars).collect();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use uth_storage::RecentCampaignCandidate;

    use super::assess_cross_page_duplicates;

    fn campaign(
        campaign_id: i64,
        post_id: i64,
        source_id: i64,
        source_name: &str,
        text: &str,
        action_url: Option<&str>,
        outbound_links: Vec<&str>,
    ) -> RecentCampaignCandidate {
        let now = Utc::now();
        RecentCampaignCandidate {
            campaign_id,
            post_id,
            source_id,
            source_name: source_name.to_owned(),
            post_url: Some(format!(
                "https://www.facebook.com/{source_id}/posts/{post_id}"
            )),
            action_url: action_url.map(str::to_owned),
            outbound_links: outbound_links.into_iter().map(str::to_owned).collect(),
            published_at: now - Duration::hours(6),
            created_at: now - Duration::hours(5),
            text: text.to_owned(),
        }
    }

    #[test]
    fn detects_exact_cross_page_repost_even_with_short_share_prefix() {
        let original = "THÔNG BÁO PHÁT ĐỘNG HỘI NGHỊ SINH VIÊN NGHIÊN CỨU KHOA HỌC UTH NĂM HỌC 2026 - 2027. Đối tượng tham gia: toàn thể sinh viên UTH có nguyện vọng rèn luyện điểm rèn luyện và nghiên cứu học thuật. Thời gian đăng ký từ 20/11/2026 đến 10/12/2026 qua biểu mẫu trực tuyến. Hãy theo dõi thông tin chi tiết và nộp đề tài đúng hạn.";
        let repost = format!("Chia sẻ thông tin từ Đoàn trường:\n{original}");
        let recent = vec![campaign(
            101,
            1001,
            10,
            "Đoàn Thanh niên - Hội Sinh viên UTH",
            original,
            Some("https://forms.gle/nckh2026"),
            vec!["https://forms.gle/nckh2026"],
        )];

        let assessment = assess_cross_page_duplicates(
            &repost,
            &["https://forms.gle/nckh2026".to_owned()],
            Some("https://forms.gle/nckh2026"),
            &recent,
        );

        let exact = assessment.exact_match.expect("expected exact repost match");
        assert_eq!(exact.campaign_id, 101);
        assert_eq!(exact.source_name, "Đoàn Thanh niên - Hội Sinh viên UTH");
        assert!(exact.similarity_percent >= 90);
    }

    #[test]
    fn flags_rewritten_cross_post_sharing_unwrapped_form_link_for_ai() {
        let prior = campaign(
            205,
            2001,
            20,
            "Tuổi trẻ Địa lý - Đô thị",
            "Hội thi Tìm hiểu Chỉ thị 07-CT/TW dành cho sinh viên các trường trên địa bàn TP.HCM. Tham gia thi trực tuyến từ 20/11/2026 đến 30/11/2026 tại MyAloha để nhận giấy chứng nhận.",
            Some("https://myaloha.vn/cuoc-thi/nhan-van-07-tim-hieu-chi-thi-so-07-cttw-147621"),
            vec!["https://myaloha.vn/cuoc-thi/nhan-van-07-tim-hieu-chi-thi-so-07-cttw-147621"],
        );
        let rewritten = "Đoàn khoa Lịch sử phát động cuộc thi trực tuyến Tìm hiểu Chỉ thị 07 đến toàn thể các bạn sinh viên. Thời gian làm bài đến hết 30/11/2026. Truy cập cổng thi ngay tại link bên dưới.";
        let wrapped_link = "https://l.facebook.com/l.php?u=https%3A%2F%2Fmyaloha.vn%2Fcuoc-thi%2Fnhan-van-07-tim-hieu-chi-thi-so-07-cttw-147621%3Ffbclid%3DIwZXh0bg&h=ATP123";

        let assessment =
            assess_cross_page_duplicates(rewritten, &[wrapped_link.to_owned()], None, &[prior]);

        assert!(assessment.exact_match.is_none());
        assert_eq!(assessment.suspected_candidates.len(), 1);
        assert_eq!(assessment.suspected_candidates[0].context.campaign_id, 205);
        assert!(assessment.suspected_candidates[0].has_shared_link);
        assert_eq!(
            assessment.suspected_candidates[0].context.shared_links,
            vec!["https://myaloha.vn/cuoc-thi/nhan-van-07-tim-hieu-chi-thi-so-07-cttw-147621"]
        );
    }

    #[test]
    fn ignores_generic_fanpage_and_root_domain_links() {
        let prior = campaign(
            301,
            3001,
            30,
            "Đoàn Thanh niên UTH",
            "Giải chạy bộ sinh viên UTH Run 2026 ngày 01/12/2026 tại Cơ sở 3 với nhiều phần quà hấp dẫn.",
            None,
            vec!["https://www.facebook.com/doanhoiuth/", "https://ut.edu.vn/"],
        );
        let unrelated = "Cuộc thi Hùng biện Tiếng Anh UTH Speak Up 2026 diễn ra ngày 15/12/2026 tại Hội trường lớn. Đăng ký ngay để nhận điểm rèn luyện.";

        let assessment = assess_cross_page_duplicates(
            unrelated,
            &[
                "https://www.facebook.com/doanhoiuth?ref=bookmarks".to_owned(),
                "https://ut.edu.vn/".to_owned(),
            ],
            None,
            &[prior],
        );

        assert!(assessment.exact_match.is_none());
        assert!(assessment.suspected_candidates.is_empty());
    }

    #[test]
    fn never_auto_skips_postponement_or_series_sharing_same_form_or_template() {
        let prior_workshop = campaign(
            452,
            4001,
            40,
            "Đoàn Hội Báo Chí & Truyền Thông",
            "Mời sinh viên đăng ký tham gia Workshop kỹ năng truyền thông Báo thức nghề diễn ra ngày 24/11/2026. Link đăng ký: https://forms.gle/pBvRe6M4H5dzLvqG8",
            Some("https://forms.gle/pBvRe6M4H5dzLvqG8"),
            vec!["https://forms.gle/pBvRe6M4H5dzLvqG8"],
        );
        let postponement = "THÔNG BÁO DỜI NGÀY TỔ CHỨC WORKSHOP BÁO THỨC NGHỀ từ 24/11/2026 sang 01/12/2026. Sinh viên tiếp tục đăng ký tại https://forms.gle/pBvRe6M4H5dzLvqG8";

        let assessment = assess_cross_page_duplicates(
            postponement,
            &["https://forms.gle/pBvRe6M4H5dzLvqG8".to_owned()],
            Some("https://forms.gle/pBvRe6M4H5dzLvqG8"),
            &[prior_workshop],
        );

        assert!(assessment.exact_match.is_none());
        assert_eq!(assessment.suspected_candidates.len(), 1);
        assert!(!assessment.defer_on_ai_error);

        let prior_minigame_1 = campaign(
            454,
            4002,
            41,
            "Nhà Văn hóa Sinh viên TP. Hồ Chí Minh",
            "SERIES BẢN ĐỒ VĂN HÓA - MINIGAME 01: Khám phá di sản miền Bắc. Thời gian: 20/11/2026 - 22/11/2026. Bước 1 like fanpage, bước 2 bình luận đáp án, bước 3 điền form nhận giấy chứng nhận.",
            Some("https://forms.gle/series2026"),
            vec!["https://forms.gle/series2026"],
        );
        let minigame_2 = "SERIES BẢN ĐỒ VĂN HÓA - MINIGAME 02: Khám phá di sản miền Trung. Thời gian: 23/11/2026 - 25/11/2026. Bước 1 like fanpage, bước 2 bình luận đáp án, bước 3 điền form nhận giấy chứng nhận.";

        let series_assessment = assess_cross_page_duplicates(
            minigame_2,
            &["https://forms.gle/series2026".to_owned()],
            Some("https://forms.gle/series2026"),
            &[prior_minigame_1],
        );

        assert!(series_assessment.exact_match.is_none());
        assert_eq!(series_assessment.suspected_candidates.len(), 1);
    }

    #[test]
    fn limits_suspected_candidates_and_marks_high_overlap_for_ai_deferral() {
        let shared = "Cuộc thi Khởi nghiệp Đổi mới sáng tạo Sinh viên TP.HCM 2026 dành cho tất cả sinh viên đại học cao đẳng đăng ký dự thi ý tưởng sáng tạo đến hết ngày 30/11/2026 để nhận giải thưởng và chứng nhận.";
        let near_copy = "CLB Khởi nghiệp giới thiệu Cuộc thi Khởi nghiệp Đổi mới sáng tạo Sinh viên TP.HCM 2026 dành cho tất cả sinh viên đại học cao đẳng đăng ký dự thi ý tưởng sáng tạo đến hết ngày 30/11/2026 để nhận giải thưởng và chứng nhận cùng cơ hội giao lưu.";
        let recent = (1..=6)
            .map(|index| {
                campaign(
                    500 + index,
                    5000 + index,
                    50 + index,
                    &format!("Page {index}"),
                    shared,
                    Some("https://forms.gle/startup2026"),
                    vec!["https://forms.gle/startup2026"],
                )
            })
            .collect::<Vec<_>>();

        let assessment = assess_cross_page_duplicates(
            near_copy,
            &["https://forms.gle/startup2026".to_owned()],
            Some("https://forms.gle/startup2026"),
            &recent,
        );

        assert!(assessment.suspected_candidates.len() <= 4);
        assert!(assessment.defer_on_ai_error);
    }
}
