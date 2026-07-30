use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uth_domain::{ClassificationDecision, FacebookPost, POST_SCHEMA_VERSION};

use crate::RuleClassifier;

pub const EVALUATION_DATASET_SCHEMA_VERSION: &str = "classifier-evaluation-dataset.v1";
pub const EVALUATION_REPORT_SCHEMA_VERSION: &str = "classifier-evaluation-report.v1";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationDataset {
    pub schema_version: String,
    pub evaluated_at: String,
    pub cases: Vec<EvaluationCase>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationCase {
    pub id: String,
    pub text: String,
    pub outbound_links: Vec<String>,
    pub approved_source: bool,
    pub published_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<String>,
    pub expected_decision: ClassificationDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvaluationReport {
    pub schema_version: String,
    pub dataset_schema_version: String,
    pub evaluated_at: String,
    pub per_case_evaluation_times: bool,
    pub classifier_version: String,
    pub config_hash: String,
    pub case_count: usize,
    pub exact_decision_matches: usize,
    pub exact_decision_accuracy_basis_points: u16,
    pub expected_notifications: usize,
    pub predicted_notifications: usize,
    pub true_positives: usize,
    pub false_positives: usize,
    pub false_negatives: usize,
    pub true_negatives: usize,
    pub notification_precision_basis_points: Option<u16>,
    pub notification_recall_basis_points: Option<u16>,
    pub notification_f1_basis_points: Option<u16>,
    pub predicted_manual_review: usize,
    pub failures: Vec<EvaluationFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvaluationFailure {
    pub case_id: String,
    pub expected_decision: ClassificationDecision,
    pub actual_decision: ClassificationDecision,
    pub score: i32,
    pub matched_rules: Vec<String>,
}

impl RuleClassifier {
    pub fn evaluate(&self, dataset: &EvaluationDataset) -> Result<EvaluationReport> {
        validate_dataset(dataset)?;
        let default_evaluated_at = DateTime::parse_from_rfc3339(&dataset.evaluated_at)
            .context("invalid evaluation evaluated_at")?
            .with_timezone(&Utc);
        let mut exact_decision_matches = 0;
        let mut expected_notifications = 0;
        let mut predicted_notifications = 0;
        let mut true_positives = 0;
        let mut false_positives = 0;
        let mut false_negatives = 0;
        let mut true_negatives = 0;
        let mut predicted_manual_review = 0;
        let mut failures = Vec::new();

        for case in &dataset.cases {
            let evaluated_at = case
                .evaluated_at
                .as_deref()
                .map(DateTime::parse_from_rfc3339)
                .transpose()
                .with_context(|| format!("invalid evaluated_at for case {}", case.id))?
                .map(|value| value.with_timezone(&Utc))
                .unwrap_or(default_evaluated_at);
            let post = evaluation_post(case, &dataset.evaluated_at);
            let result = self.classify(&post, case.approved_source, evaluated_at)?;
            let expected_notification =
                case.expected_decision == ClassificationDecision::MatchedExplicit;
            let predicted_notification = result.decision == ClassificationDecision::MatchedExplicit;

            expected_notifications += usize::from(expected_notification);
            predicted_notifications += usize::from(predicted_notification);
            predicted_manual_review +=
                usize::from(result.decision == ClassificationDecision::ManualReview);
            match (expected_notification, predicted_notification) {
                (true, true) => true_positives += 1,
                (false, true) => false_positives += 1,
                (true, false) => false_negatives += 1,
                (false, false) => true_negatives += 1,
            }

            if result.decision == case.expected_decision {
                exact_decision_matches += 1;
            } else {
                failures.push(EvaluationFailure {
                    case_id: case.id.clone(),
                    expected_decision: case.expected_decision.clone(),
                    actual_decision: result.decision,
                    score: result.score,
                    matched_rules: result.matched_rules,
                });
            }
        }

        let case_count = dataset.cases.len();
        Ok(EvaluationReport {
            schema_version: EVALUATION_REPORT_SCHEMA_VERSION.to_owned(),
            dataset_schema_version: dataset.schema_version.clone(),
            evaluated_at: dataset.evaluated_at.clone(),
            per_case_evaluation_times: dataset.cases.iter().any(|case| case.evaluated_at.is_some()),
            classifier_version: self.classifier_version().to_owned(),
            config_hash: self.config_hash().to_owned(),
            case_count,
            exact_decision_matches,
            exact_decision_accuracy_basis_points: basis_points(exact_decision_matches, case_count)
                .unwrap_or(0),
            expected_notifications,
            predicted_notifications,
            true_positives,
            false_positives,
            false_negatives,
            true_negatives,
            notification_precision_basis_points: basis_points(
                true_positives,
                true_positives + false_positives,
            ),
            notification_recall_basis_points: basis_points(
                true_positives,
                true_positives + false_negatives,
            ),
            notification_f1_basis_points: basis_points(
                2 * true_positives,
                2 * true_positives + false_positives + false_negatives,
            ),
            predicted_manual_review,
            failures,
        })
    }
}

fn validate_dataset(dataset: &EvaluationDataset) -> Result<()> {
    if dataset.schema_version != EVALUATION_DATASET_SCHEMA_VERSION {
        bail!(
            "unsupported evaluation dataset schema {}",
            dataset.schema_version
        );
    }
    if dataset.cases.is_empty() {
        bail!("evaluation dataset must contain at least one case");
    }
    let mut ids = HashSet::with_capacity(dataset.cases.len());
    for case in &dataset.cases {
        if case.id.trim().is_empty() || case.text.trim().is_empty() {
            bail!("evaluation case ID and text must not be empty");
        }
        if !ids.insert(case.id.as_str()) {
            bail!("duplicate evaluation case ID {}", case.id);
        }
    }
    Ok(())
}

fn evaluation_post(case: &EvaluationCase, fetched_at: &str) -> FacebookPost {
    let content_hash = format!(
        "sha256:{:x}",
        Sha256::digest(format!("{}\n{}", case.id, case.text).as_bytes())
    );
    FacebookPost {
        schema_version: POST_SCHEMA_VERSION.to_owned(),
        source_id: "facebook:evaluation".to_owned(),
        platform: "facebook".to_owned(),
        external_post_id: case.id.clone(),
        canonical_url: format!("https://www.facebook.com/evaluation/posts/{}", case.id),
        published_at: case.published_at.clone(),
        text: case.text.clone(),
        media: Vec::new(),
        outbound_links: case.outbound_links.clone(),
        content_hash,
        crawl_strategy: "evaluation_fixture".to_owned(),
        fetched_at: fetched_at.to_owned(),
    }
}

fn basis_points(numerator: usize, denominator: usize) -> Option<u16> {
    if denominator == 0 {
        return None;
    }
    let value = numerator.saturating_mul(10_000) / denominator;
    Some(u16::try_from(value.min(10_000)).unwrap_or(10_000))
}

#[cfg(test)]
mod tests {
    use uth_domain::ClassificationDecision;

    use super::{EvaluationCase, EvaluationDataset, basis_points};
    use crate::RuleClassifier;

    const CONFIG: &[u8] = include_bytes!("../../../config/classifier-rules.v1.json");
    const CASES: &[u8] = include_bytes!("../../../tests/fixtures/classifier/rules_cases.v1.json");

    #[test]
    fn fixture_report_has_no_regressions() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let dataset: EvaluationDataset = serde_json::from_slice(CASES).unwrap();
        let report = classifier.evaluate(&dataset).unwrap();

        assert_eq!(report.case_count, 13);
        assert_eq!(report.exact_decision_matches, 13);
        assert_eq!(report.notification_precision_basis_points, Some(10_000));
        assert_eq!(report.notification_recall_basis_points, Some(10_000));
        assert!(report.failures.is_empty());
    }

    #[test]
    fn report_exposes_false_positive_and_false_negative() {
        let classifier = RuleClassifier::from_bytes(CONFIG).unwrap();
        let dataset = EvaluationDataset {
            schema_version: "classifier-evaluation-dataset.v1".to_owned(),
            evaluated_at: "2026-07-19T12:00:00+00:00".to_owned(),
            cases: vec![
                evaluation_case(
                    "false-positive",
                    "Mời sinh viên đăng ký tham gia hoạt động điểm rèn luyện trước ngày 25/07/2026.",
                    ClassificationDecision::Rejected,
                ),
                evaluation_case(
                    "false-negative",
                    "Thông tin sinh hoạt tháng này dành cho sinh viên.",
                    ClassificationDecision::MatchedExplicit,
                ),
            ],
        };
        let report = classifier.evaluate(&dataset).unwrap();

        assert_eq!(report.false_positives, 1);
        assert_eq!(report.false_negatives, 1);
        assert_eq!(report.notification_precision_basis_points, Some(0));
        assert_eq!(report.notification_recall_basis_points, Some(0));
        assert_eq!(report.failures.len(), 2);
    }

    #[test]
    fn empty_denominator_has_no_metric() {
        assert_eq!(basis_points(0, 0), None);
    }

    fn evaluation_case(
        id: &str,
        text: &str,
        expected_decision: ClassificationDecision,
    ) -> EvaluationCase {
        EvaluationCase {
            id: id.to_owned(),
            text: text.to_owned(),
            outbound_links: Vec::new(),
            approved_source: true,
            published_at: "2026-07-18T02:00:17+00:00".to_owned(),
            evaluated_at: None,
            expected_decision,
        }
    }
}
