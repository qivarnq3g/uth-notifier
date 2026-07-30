use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use uth_classifier::{EvaluationDataset, RuleClassifier};

#[derive(Debug, clap::Args)]
pub struct EvaluateClassifierArgs {
    #[arg(long, default_value = "config/classifier-rules.v1.json")]
    config: PathBuf,

    #[arg(long, default_value = "tests/fixtures/classifier/rules_cases.v1.json")]
    dataset: PathBuf,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long)]
    minimum_precision_basis_points: Option<u16>,

    #[arg(long)]
    minimum_recall_basis_points: Option<u16>,
}

pub fn run(args: EvaluateClassifierArgs) -> Result<()> {
    validate_threshold(args.minimum_precision_basis_points, "precision")?;
    validate_threshold(args.minimum_recall_basis_points, "recall")?;
    let config = fs::read(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let dataset_bytes = fs::read(&args.dataset)
        .with_context(|| format!("failed to read {}", args.dataset.display()))?;
    let classifier = RuleClassifier::from_bytes(&config)?;
    let dataset: EvaluationDataset = serde_json::from_slice(&dataset_bytes)
        .with_context(|| format!("invalid evaluation dataset {}", args.dataset.display()))?;
    let report = classifier.evaluate(&dataset)?;
    let rendered = serde_json::to_string_pretty(&report)? + "\n";
    if let Some(path) = args.output {
        write_report(&path, &rendered)?;
    } else {
        print!("{rendered}");
    }
    enforce_threshold(
        "precision",
        report.notification_precision_basis_points,
        args.minimum_precision_basis_points,
    )?;
    enforce_threshold(
        "recall",
        report.notification_recall_basis_points,
        args.minimum_recall_basis_points,
    )?;
    Ok(())
}

fn validate_threshold(threshold: Option<u16>, name: &str) -> Result<()> {
    if threshold.is_some_and(|value| value > 10_000) {
        bail!("minimum {name} must be between 0 and 10000 basis points");
    }
    Ok(())
}

fn enforce_threshold(name: &str, actual: Option<u16>, minimum: Option<u16>) -> Result<()> {
    let Some(minimum) = minimum else {
        return Ok(());
    };
    let Some(actual) = actual else {
        bail!("{name} is undefined because the evaluation dataset has no applicable cases");
    };
    if actual < minimum {
        bail!("{name} {actual} basis points is below required minimum {minimum}");
    }
    Ok(())
}

fn write_report(path: &Path, rendered: &str) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| *parent != Path::new("")) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(path, rendered).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{enforce_threshold, validate_threshold};

    #[test]
    fn rejects_invalid_or_unmet_thresholds() {
        assert!(validate_threshold(Some(10_001), "precision").is_err());
        assert!(enforce_threshold("recall", Some(9_000), Some(9_500)).is_err());
        assert!(enforce_threshold("recall", None, Some(9_500)).is_err());
    }
}
