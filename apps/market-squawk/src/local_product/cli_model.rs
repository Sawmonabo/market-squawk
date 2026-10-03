//! Closed, bounded CLI admission for production model bundles.

use std::path::{Component, Path, PathBuf};

use market_squawk_modeling::{
    BundleError, MAX_BUNDLE_AUTHORITY_BYTES, ModelAdmissionError, OnnxPolicyError,
};
use market_squawk_platform::{BoundedInput, InputFileError, UserAuthorizedInputRoot};
use serde_json::{Value, json};
use thiserror::Error;

use super::LocalProduct;
use crate::application::model::runtime::{
    ModelAdmissionDisposition, ModelAdmissionRequest, ProductionModelRuntimeError,
};

const MAXIMUM_REQUEST_BYTES: u64 = 8 * 1024 * 1024;

/// CLI model-admission request, authority, policy, or runtime failure.
#[derive(Debug, Error)]
pub enum CliModelAdmissionError {
    /// Durable model admission was not explicitly confirmed.
    #[error("model admission requires explicit confirmation")]
    ConfirmationRequired,
    /// No signed production training release was configured at process composition.
    #[error("production model runtime is not configured")]
    RuntimeNotConfigured,
    /// The request path could not be made absolute without changing its meaning.
    #[error("model admission request path is invalid: {0}")]
    RequestPath(#[source] std::io::Error),
    /// The absolute request path contained no safe regular-file coordinate.
    #[error("model admission request path is not a safe regular-file coordinate")]
    UnsafeRequestPath,
    /// The request was not an unchanged, bounded, no-follow regular file.
    #[error("model admission request file is not admissible: {0}")]
    RequestFile(#[source] InputFileError),
    /// JSON did not match the closed request schema.
    #[error("model admission request JSON is invalid: {0}")]
    RequestJson(#[source] serde_json::Error),
    /// The request schema is not supported by this release.
    #[error("model admission request schema version is unsupported")]
    UnsupportedSchemaVersion,
    /// One SHA-256 value was not canonical lowercase hexadecimal.
    #[error("model admission request contains an invalid SHA-256 digest")]
    InvalidDigest,
    /// The independent authority path was not an explicit safe absolute path.
    #[error("model bundle authority path must be an explicit safe absolute path")]
    AuthorityPath,
    /// The independent authority was not disjoint, unchanged, bounded, and no-follow.
    #[error("model bundle authority file is not admissible: {0}")]
    AuthorityFile(#[source] InputFileError),
    /// The metadata reference was outside the controlled bundle grammar.
    #[error("model bundle metadata reference is invalid: {0}")]
    Metadata(#[source] BundleError),
    /// The exact point-in-time dataset authority was invalid.
    #[error("model dataset admission authority is invalid: {0}")]
    Dataset(#[source] ModelAdmissionError),
    /// The closed ONNX runtime policy was invalid.
    #[error("model ONNX admission policy is invalid: {0}")]
    OnnxPolicy(#[source] OnnxPolicyError),
    /// Candidate verification, durable publication, or backend construction failed.
    #[error("production model admission failed: {0}")]
    Admission(#[source] ProductionModelRuntimeError),
}

/// Admits one exact model bundle through the configured production runtime.
///
/// The request path is the only ambient CLI input. Candidate paths remain relative to the
/// prepared artifact capability, while the independent authority document is read exactly once
/// from a disjoint user-authorized no-follow root.
pub(super) fn admit_model_bundle(
    product: &LocalProduct,
    request_path: &Path,
    confirmed: bool,
) -> Result<Value, CliModelAdmissionError> {
    if !confirmed {
        return Err(CliModelAdmissionError::ConfirmationRequired);
    }
    let runtime = product
        .model_runtime()
        .ok_or(CliModelAdmissionError::RuntimeNotConfigured)?;
    let request_input = read_request(request_path)?;
    let authority_path = ModelAdmissionRequest::authority_path_from_json(request_input.as_bytes())
        .map_err(CliModelAdmissionError::Admission)?;
    let authority_bytes = read_authority(product, &authority_path)?;
    let admission = ModelAdmissionRequest::decode_json(request_input.as_bytes(), authority_bytes)
        .map_err(CliModelAdmissionError::Admission)?;
    let receipt = runtime
        .admit(admission)
        .map_err(CliModelAdmissionError::Admission)?;
    let disposition = match receipt.disposition() {
        ModelAdmissionDisposition::Inserted => "inserted",
        ModelAdmissionDisposition::AlreadyAdmitted => "already_admitted",
    };

    Ok(json!({
        "modelId": receipt.model_id().to_string(),
        "bundleId": receipt.bundle_id().as_str(),
        "bundleVersion": receipt.bundle_version().get(),
        "disposition": disposition,
        "metadataSha256": encode_hex(receipt.metadata_sha256().bytes()),
        "artifactSha256": encode_hex(receipt.artifact_sha256().bytes()),
        "trainingRunSha256": encode_hex(receipt.training_run_sha256().bytes()),
        "authoritySha256": encode_hex(receipt.authority_sha256().bytes()),
        "datasetSelectionSha256": encode_hex(receipt.dataset_selection_sha256().bytes()),
    }))
}

fn read_request(path: &Path) -> Result<BoundedInput, CliModelAdmissionError> {
    let absolute = std::path::absolute(path).map_err(CliModelAdmissionError::RequestPath)?;
    let (parent, name) =
        split_safe_absolute_file(&absolute).ok_or(CliModelAdmissionError::UnsafeRequestPath)?;
    let root =
        UserAuthorizedInputRoot::open(parent).map_err(CliModelAdmissionError::RequestFile)?;
    root.resolve(PathBuf::from(name))
        .and_then(|file| file.open_bounded(MAXIMUM_REQUEST_BYTES))
        .and_then(|file| file.read_bounded())
        .map_err(CliModelAdmissionError::RequestFile)
}

fn read_authority(
    product: &LocalProduct,
    path: &Path,
) -> Result<Box<[u8]>, CliModelAdmissionError> {
    let (parent, name) =
        split_safe_absolute_file(path).ok_or(CliModelAdmissionError::AuthorityPath)?;
    let root =
        UserAuthorizedInputRoot::open(parent).map_err(CliModelAdmissionError::AuthorityFile)?;
    root.ensure_disjoint_root(product.paths().root())
        .map_err(CliModelAdmissionError::AuthorityFile)?;
    let maximum_bytes = u64::try_from(MAX_BUNDLE_AUTHORITY_BYTES)
        .map_err(|_| CliModelAdmissionError::AuthorityPath)?;
    root.resolve(PathBuf::from(name))
        .and_then(|file| file.open_bounded(maximum_bytes))
        .and_then(|file| file.read_bounded())
        .map(BoundedInput::into_bytes)
        .map_err(CliModelAdmissionError::AuthorityFile)
}

fn split_safe_absolute_file(path: &Path) -> Option<(&Path, &std::ffi::OsStr)> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| component == Component::ParentDir)
    {
        return None;
    }
    Some((path.parent()?, path.file_name()?))
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
