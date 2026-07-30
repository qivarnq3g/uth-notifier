use std::collections::BTreeMap;
use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uth_classifier::{
    EVALUATION_DATASET_SCHEMA_VERSION, EvaluationCase, EvaluationDataset, RuleClassifier,
};
use uth_domain::{ClassificationDecision, CrawlReport, REPORT_SCHEMA_VERSION};

const REVIEW_SCHEMA_VERSION: &str = "classifier-review-bundle.v1";
const HUMAN_LABELS_SCHEMA_VERSION: &str = "classifier-human-labels.v1";

#[derive(Debug, clap::Args)]
pub struct PrepareClassifierReviewArgs {
    #[arg(help = "Healthy facebook-crawl-report.v1 input")]
    input: PathBuf,

    #[arg(long, default_value = "config/classifier-rules.v1.json")]
    config: PathBuf,

    #[arg(long)]
    output: PathBuf,

    #[arg(long)]
    markdown_output: PathBuf,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    approved_source: bool,
}

#[derive(Debug, clap::Args)]
pub struct FinalizeClassifierReviewArgs {
    #[arg(help = "classifier-review-bundle.v1 input")]
    review: PathBuf,

    #[arg(help = "classifier-human-labels.v1 input")]
    labels: PathBuf,

    #[arg(long)]
    output_review: PathBuf,

    #[arg(long)]
    output_dataset: PathBuf,

    #[arg(long)]
    markdown_output: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanLabels {
    schema_version: String,
    source_id: String,
    labels: Vec<HumanLabel>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanLabel {
    review_number: usize,
    decision: ClassificationDecision,
    reason: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReviewBundle {
    schema_version: String,
    created_at: String,
    source_id: String,
    source_url: String,
    approved_source: bool,
    classifier_version: String,
    config_hash: String,
    case_count: usize,
    reviewed_at: Option<String>,
    cases: Vec<ReviewCase>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReviewCase {
    review_number: usize,
    external_post_id: String,
    canonical_url: String,
    published_at: String,
    evaluated_at: String,
    text: String,
    outbound_links: Vec<String>,
    predicted_decision: ClassificationDecision,
    score: i32,
    confidence_basis_points: u16,
    matched_rules: Vec<String>,
    human_decision: Option<ClassificationDecision>,
    reviewer_note: Option<String>,
}

pub fn run(args: PrepareClassifierReviewArgs) -> Result<()> {
    let config = fs::read(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let report_bytes = fs::read(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;
    let classifier = RuleClassifier::from_bytes(&config)?;
    let report: CrawlReport = serde_json::from_slice(&report_bytes)
        .with_context(|| format!("invalid crawl report {}", args.input.display()))?;
    validate_report(&report)?;
    let now = Utc::now();
    let cases = report
        .posts
        .iter()
        .enumerate()
        .map(|(index, post)| {
            let evaluated_at = DateTime::parse_from_rfc3339(&post.published_at)
                .with_context(|| format!("invalid published_at for {}", post.external_post_id))?
                .with_timezone(&Utc);
            let result = classifier.classify(post, args.approved_source, evaluated_at)?;
            Ok(ReviewCase {
                review_number: index + 1,
                external_post_id: post.external_post_id.clone(),
                canonical_url: post.canonical_url.clone(),
                published_at: post.published_at.clone(),
                evaluated_at: evaluated_at.to_rfc3339(),
                text: post.text.clone(),
                outbound_links: post.outbound_links.clone(),
                predicted_decision: result.decision,
                score: result.score,
                confidence_basis_points: result.confidence_basis_points,
                matched_rules: result.matched_rules,
                human_decision: None,
                reviewer_note: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let bundle = ReviewBundle {
        schema_version: REVIEW_SCHEMA_VERSION.to_owned(),
        created_at: now.to_rfc3339(),
        source_id: report.source_id,
        source_url: report.source_url,
        approved_source: args.approved_source,
        classifier_version: classifier.classifier_version().to_owned(),
        config_hash: classifier.config_hash().to_owned(),
        case_count: cases.len(),
        reviewed_at: None,
        cases,
    };
    let json = serde_json::to_string_pretty(&bundle)? + "\n";
    write_file(&args.output, &json)?;
    write_file(&args.markdown_output, &render_markdown(&bundle)?)?;
    Ok(())
}

pub fn finalize(args: FinalizeClassifierReviewArgs) -> Result<()> {
    let review_bytes = fs::read(&args.review)
        .with_context(|| format!("failed to read {}", args.review.display()))?;
    let label_bytes = fs::read(&args.labels)
        .with_context(|| format!("failed to read {}", args.labels.display()))?;
    let mut review: ReviewBundle = serde_json::from_slice(&review_bytes)
        .with_context(|| format!("invalid review bundle {}", args.review.display()))?;
    let labels: HumanLabels = serde_json::from_slice(&label_bytes)
        .with_context(|| format!("invalid human labels {}", args.labels.display()))?;
    validate_finalization(&review, &labels)?;
    let mut labels_by_number = labels
        .labels
        .into_iter()
        .map(|label| (label.review_number, label))
        .collect::<BTreeMap<_, _>>();
    for case in &mut review.cases {
        let label = labels_by_number
            .remove(&case.review_number)
            .with_context(|| format!("missing label for review {}", case.review_number))?;
        case.human_decision = Some(label.decision);
        case.reviewer_note = Some(label.reason);
    }
    if !labels_by_number.is_empty() {
        bail!("human labels contain unknown review numbers");
    }
    review.reviewed_at = Some(Utc::now().to_rfc3339());
    let dataset = EvaluationDataset {
        schema_version: EVALUATION_DATASET_SCHEMA_VERSION.to_owned(),
        evaluated_at: review.created_at.clone(),
        cases: review
            .cases
            .iter()
            .map(|case| EvaluationCase {
                id: format!("{}:{}", review.source_id, case.external_post_id),
                text: case.text.clone(),
                outbound_links: case.outbound_links.clone(),
                approved_source: review.approved_source,
                published_at: case.published_at.clone(),
                evaluated_at: Some(case.evaluated_at.clone()),
                expected_decision: case.human_decision.clone().unwrap(),
            })
            .collect(),
    };
    write_file(
        &args.output_review,
        &(serde_json::to_string_pretty(&review)? + "\n"),
    )?;
    write_file(
        &args.output_dataset,
        &(serde_json::to_string_pretty(&dataset)? + "\n"),
    )?;
    write_file(&args.markdown_output, &render_markdown(&review)?)?;
    Ok(())
}

fn validate_report(report: &CrawlReport) -> Result<()> {
    if report.schema_version != REPORT_SCHEMA_VERSION {
        bail!("unsupported crawl report schema {}", report.schema_version);
    }
    if report.health != "healthy" {
        bail!("review input must be a healthy crawl report");
    }
    if report.posts.is_empty() {
        bail!("review input must contain at least one post");
    }
    Ok(())
}

fn validate_finalization(review: &ReviewBundle, labels: &HumanLabels) -> Result<()> {
    if review.schema_version != REVIEW_SCHEMA_VERSION {
        bail!("unsupported review schema {}", review.schema_version);
    }
    if labels.schema_version != HUMAN_LABELS_SCHEMA_VERSION {
        bail!("unsupported human labels schema {}", labels.schema_version);
    }
    if review.source_id != labels.source_id {
        bail!("human labels source does not match review source");
    }
    if review.case_count != review.cases.len() || review.case_count != labels.labels.len() {
        bail!("review and human label counts do not match");
    }
    let unique_numbers = labels
        .labels
        .iter()
        .map(|label| label.review_number)
        .collect::<std::collections::HashSet<_>>();
    if unique_numbers.len() != labels.labels.len() {
        bail!("human labels contain duplicate review numbers");
    }
    if labels
        .labels
        .iter()
        .any(|label| label.reason.trim().is_empty())
    {
        bail!("every human label must include a reason");
    }
    Ok(())
}

fn render_markdown(bundle: &ReviewBundle) -> Result<String> {
    let mut output = String::new();
    writeln!(output, "# Review classifier")?;
    writeln!(output)?;
    writeln!(output, "- Nguồn: {}", bundle.source_url)?;
    writeln!(output, "- Số bài: {}", bundle.case_count)?;
    writeln!(
        output,
        "- Nhãn hợp lệ: `matched_explicit`, `manual_review`, `rejected`"
    )?;
    writeln!(
        output,
        "- Thay `pending` bằng nhãn con người và thêm ghi chú nếu cần."
    )?;
    for case in &bundle.cases {
        writeln!(output)?;
        writeln!(output, "## {:02}", case.review_number)?;
        writeln!(output)?;
        writeln!(
            output,
            "- Dự đoán: `{}`",
            decision_name(&case.predicted_decision)
        )?;
        let human_decision = case
            .human_decision
            .as_ref()
            .map(decision_name)
            .unwrap_or("pending");
        writeln!(output, "- Nhãn con người: `{human_decision}`")?;
        writeln!(output, "- Điểm: {}", case.score)?;
        writeln!(output, "- Thời gian: {}", case.published_at)?;
        writeln!(output, "- Thời điểm chấm: {}", case.evaluated_at)?;
        writeln!(output, "- URL: {}", case.canonical_url)?;
        writeln!(
            output,
            "- Ghi chú: {}",
            case.reviewer_note.as_deref().unwrap_or("")
        )?;
        writeln!(output)?;
        for line in case.text.lines() {
            writeln!(output, "> {line}")?;
        }
    }
    Ok(output)
}

fn decision_name(decision: &ClassificationDecision) -> &'static str {
    match decision {
        ClassificationDecision::Rejected => "rejected",
        ClassificationDecision::MatchedExplicit => "matched_explicit",
        ClassificationDecision::ManualReview => "manual_review",
    }
}

fn write_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| *parent != Path::new("")) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use uth_domain::ClassificationDecision;

    use super::{ReviewBundle, ReviewCase, render_markdown};

    #[test]
    fn markdown_contains_review_fields_and_post_text() {
        let bundle = ReviewBundle {
            schema_version: "classifier-review-bundle.v1".to_owned(),
            created_at: "2026-07-20T12:00:00+00:00".to_owned(),
            source_id: "facebook:test".to_owned(),
            source_url: "https://www.facebook.com/test".to_owned(),
            approved_source: true,
            classifier_version: "rules-test".to_owned(),
            config_hash: "sha256:test".to_owned(),
            case_count: 1,
            reviewed_at: None,
            cases: vec![ReviewCase {
                review_number: 1,
                external_post_id: "post-1".to_owned(),
                canonical_url: "https://www.facebook.com/test/posts/post-1".to_owned(),
                published_at: "2026-07-20T10:00:00+00:00".to_owned(),
                evaluated_at: "2026-07-20T10:00:00+00:00".to_owned(),
                text: "Dòng một\nDòng hai".to_owned(),
                outbound_links: Vec::new(),
                predicted_decision: ClassificationDecision::ManualReview,
                score: 3,
                confidence_basis_points: 5_000,
                matched_rules: Vec::new(),
                human_decision: None,
                reviewer_note: None,
            }],
        };
        let markdown = render_markdown(&bundle).unwrap();

        assert!(markdown.contains("## 01"));
        assert!(markdown.contains("Nhãn con người: `pending`"));
        assert!(markdown.contains("> Dòng một\n> Dòng hai"));
    }
}
