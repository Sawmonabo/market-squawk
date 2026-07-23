//! Bounded CLI execution boundary for point-in-time feature-dataset publication.

#[path = "cli_dataset_request.rs"]
mod request_dto;

use std::path::{Path, PathBuf};

use market_squawk_platform::{UserAuthorizedInputRoot, UserOwnedInputEvidence};
use serde_json::{Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use self::request_dto::DatasetBuildRequestDto;
use super::LocalProduct;
use crate::ResearchServiceError;

const MAXIMUM_REQUEST_BYTES: u64 = 8 * 1024 * 1024;

/// Closed request-file, contract-admission, or dataset-publication failure.
#[derive(Debug, Error)]
pub enum CliDatasetError {
    /// Mutating dataset publication was not explicitly confirmed.
    #[error("dataset build requires explicit confirmation")]
    ConfirmationRequired,
    /// The selected request is not a bounded no-follow regular file.
    #[error("dataset request file is not an admitted bounded regular file")]
    RequestFile,
    /// JSON did not match the closed dataset-build schema.
    #[error("dataset request JSON is malformed or contains unsupported fields")]
    RequestJson,
    /// One or more values violated the domain request contract.
    #[error("dataset request violates point-in-time, rights, or resource invariants")]
    InvalidRequest,
    /// The production research service rejected or failed publication.
    #[error("dataset build failed: {0}")]
    Build(#[from] ResearchServiceError),
}

/// Admits caller-materialized feature/label examples and publishes one immutable PIT dataset.
///
/// The data service independently verifies selectors, source generations, universe evidence,
/// temporal cutoffs, adjustment evidence, rights, and limits. This boundary does not calculate
/// feature values from canonical observations. The same admitted low-level build contract is
/// suitable for both `Dataset.Build` and `Feature.Build`.
pub(super) async fn build_point_in_time_dataset(
    product: &LocalProduct,
    request: &Path,
    confirmed: bool,
) -> Result<Value, CliDatasetError> {
    if !confirmed {
        return Err(CliDatasetError::ConfirmationRequired);
    }
    let (bytes, ownership) = read_request(request)?;
    let request: DatasetBuildRequestDto =
        serde_json::from_slice(bytes.as_bytes()).map_err(|_| CliDatasetError::RequestJson)?;
    let admitted = request.into_domain(ownership)?;
    let built = product
        .research()
        .build_dataset(admitted, CancellationToken::new())
        .await?;
    let manifest = built.manifest();
    let splits = built.split_counts();
    Ok(json!({
        "manifest": {
            "dataset": manifest.dataset_id().as_str(),
            "version": manifest.manifest_version(),
            "schema": manifest.schema().name(),
            "schemaVersion": manifest.schema_version().get(),
            "schemaFingerprintSha256": encode_hex(manifest.schema().fingerprint()),
            "contentSha256": encode_hex(manifest.content_hash().bytes()),
        },
        "buildSpecSha256": encode_hex(built.build_spec_digest().digest().bytes()),
        "policySha256": encode_hex(built.policy_digest().bytes()),
        "universeSha256": encode_hex(built.universe_digest().bytes()),
        "splitExamples": {
            "train": splits.train_examples(),
            "validation": splits.validation_examples(),
            "test": splits.test_examples(),
        },
    }))
}

fn read_request(
    path: &Path,
) -> Result<(market_squawk_platform::BoundedInput, UserOwnedInputEvidence), CliDatasetError> {
    let absolute = std::path::absolute(path).map_err(|_| CliDatasetError::RequestFile)?;
    if absolute
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CliDatasetError::RequestFile);
    }
    let parent = absolute.parent().ok_or(CliDatasetError::RequestFile)?;
    let name = absolute.file_name().ok_or(CliDatasetError::RequestFile)?;
    let (root, authority) = UserAuthorizedInputRoot::open_with_ownership_authority(parent)
        .map_err(|_| CliDatasetError::RequestFile)?;
    let input = root
        .resolve(PathBuf::from(name))
        .and_then(|file| file.open_bounded(MAXIMUM_REQUEST_BYTES))
        .and_then(|file| file.read_bounded())
        .map_err(|_| CliDatasetError::RequestFile)?;
    let ownership = authority
        .issue_manifest_evidence(&input)
        .map_err(|_| CliDatasetError::RequestFile)?;
    Ok((input, ownership))
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
