//! Bounded CLI boundary for immutable phase-one point-in-time derived generations.

#[path = "cli_dataset_request.rs"]
mod request_dto;

use std::path::{Path, PathBuf};

use market_squawk_data::FeatureLabelDataset;
use market_squawk_platform::{UserAuthorizedInputRoot, UserOwnedInputEvidence};
use serde_json::{Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use self::request_dto::PhaseOneDerivedGenerationRequestDto;
use super::LocalProduct;
use crate::ResearchServiceError;

const MAXIMUM_REQUEST_BYTES: u64 = 8 * 1024 * 1024;

/// Closed request-file, contract-admission, or phase-one generation failure.
#[derive(Debug, Error)]
pub enum CliDatasetError {
    /// Mutating phase-one generation publication was not explicitly confirmed.
    #[error("phase-one derived-generation build requires explicit confirmation")]
    ConfirmationRequired,
    /// The selected request is not a bounded no-follow regular file.
    #[error("phase-one generation request is not an admitted bounded regular file")]
    RequestFile,
    /// JSON did not match the closed phase-one generation request schema.
    #[error("phase-one generation request JSON is malformed or contains unsupported fields")]
    RequestJson,
    /// One or more values violated the domain request contract.
    #[error("phase-one generation request violates point-in-time, rights, or resource invariants")]
    InvalidRequest,
    /// The research service rejected or failed the phase-one generation publication.
    #[error("phase-one derived-generation build failed: {0}")]
    PhaseOneDerivedGeneration(#[from] ResearchServiceError),
    /// The immutable generation could not reproduce its bounded canonical phase-one descriptor.
    #[error("phase-one generation descriptor could not be reproduced: {0}")]
    PhaseOneDescriptor(#[from] market_squawk_data::DatasetBuildError),
}

/// Admits caller-materialized examples and publishes one immutable phase-one PIT generation.
///
/// The data service independently verifies selectors, source generations, universe evidence,
/// temporal cutoffs, adjustment evidence, rights, and limits. This boundary does not calculate
/// feature values from canonical observations. This operation issues no product receipt or
/// admission; its result records only that operation-scoped disposition. The same low-level
/// phase-one contract is used by both dataset and feature CLI commands.
pub(super) async fn build_phase_one_derived_generation(
    product: &LocalProduct,
    request: &Path,
    confirmed: bool,
) -> Result<Value, CliDatasetError> {
    if !confirmed {
        return Err(CliDatasetError::ConfirmationRequired);
    }
    let built = build_phase_one_derived_generation_from_file(product, request).await?;
    let manifest = built.manifest();
    let splits = built.split_counts();
    let phase_one_descriptor_sha256 = built.python_export()?.content_hash();
    // This returned result records only that this phase-one operation did not issue product
    // admission before returning. Any receipt-backed product admission is a separate
    // Analysis.GetFeatureDatasets authority.
    Ok(json!({
        "publicationStage": "phase_one_derived_generation",
        "productAdmission": "not_admitted_by_phase_one_operation_at_completion",
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
        "phaseOneDescriptorSha256": encode_hex(phase_one_descriptor_sha256.bytes()),
        "splitExamples": {
            "train": splits.train_examples(),
            "validation": splits.validation_examples(),
            "test": splits.test_examples(),
        },
    }))
}

/// Admits one bounded request file and publishes its immutable phase-one PIT generation.
///
/// Callers must establish their own explicit mutation authority before invoking this shared
/// path. The returned generation remains queryable by exact manifest after restart but is not a
/// product admission.
pub(crate) async fn build_phase_one_derived_generation_from_file(
    product: &LocalProduct,
    request: &Path,
) -> Result<FeatureLabelDataset, CliDatasetError> {
    let (bytes, ownership) = read_request(request)?;
    let request: PhaseOneDerivedGenerationRequestDto =
        serde_json::from_slice(bytes.as_bytes()).map_err(|_| CliDatasetError::RequestJson)?;
    let admitted = request.into_domain(Some(ownership))?;
    product
        .research()
        .build_phase_one_derived_generation(admitted, CancellationToken::new())
        .await
        .map_err(Into::into)
}

/// Admits an inline phase-one request only with independent reviewed-terms evidence.
///
/// Local-file ownership remains available solely through the retained file authority. This
/// conversion creates no product receipt or issuer authority.
pub(crate) fn admit_inline_phase_one_derived_generation_request(
    registration: &serde_json::Map<String, Value>,
) -> Result<market_squawk_data::DatasetBuildRequest, CliDatasetError> {
    let request: PhaseOneDerivedGenerationRequestDto =
        serde_json::from_value(Value::Object(registration.clone()))
            .map_err(|_| CliDatasetError::RequestJson)?;
    request.into_domain(None)
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
