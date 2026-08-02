//! Closed, bounded CLI admission for production model bundles.

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use market_squawk_data::{CatalogEndpointIdentity, Sha256Digest};
use market_squawk_domain::Timestamp;
use market_squawk_modeling::{
    BundleError, BundleMetadataRef, MAX_BUNDLE_AUTHORITY_BYTES, ModelAdmissionError,
    ModelOutputSemantics, OnnxFallbackPolicy, OnnxModelPolicy, OnnxPolicyError,
    PythonDatasetAdmissionAuthority,
};
use market_squawk_platform::{BoundedInput, InputFileError, UserAuthorizedInputRoot};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use super::LocalProduct;
use crate::application::model::runtime::{
    ModelAdmissionDisposition, ModelAdmissionRequest, ModelBackendAdmission,
    ProductionModelRuntimeError,
};

const MAXIMUM_REQUEST_BYTES: u64 = 8 * 1024 * 1024;
const REQUEST_SCHEMA_VERSION: u16 = 1;

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
    let request: ModelAdmissionRequestDto = serde_json::from_slice(request_input.as_bytes())
        .map_err(CliModelAdmissionError::RequestJson)?;
    if request.schema_version != REQUEST_SCHEMA_VERSION {
        return Err(CliModelAdmissionError::UnsupportedSchemaVersion);
    }

    let authority_sha256 = digest(&request.authority.sha256)?;
    let metadata = BundleMetadataRef::try_new(
        request.metadata.relative_path,
        digest(&request.metadata.sha256)?,
    )
    .map_err(CliModelAdmissionError::Metadata)?;
    let dataset = request.dataset.into_domain()?;
    let backend = request.backend.into_domain()?;
    let authority_bytes = read_authority(product, &request.authority.path)?;
    let admission = ModelAdmissionRequest::try_new(
        request.candidate_directory,
        metadata,
        authority_bytes,
        authority_sha256,
        dataset,
        backend,
    )
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelAdmissionRequestDto {
    schema_version: u16,
    candidate_directory: String,
    metadata: MetadataReferenceDto,
    authority: AuthorityReferenceDto,
    dataset: DatasetAuthorityDto,
    backend: BackendAdmissionDto,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MetadataReferenceDto {
    relative_path: String,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthorityReferenceDto {
    path: PathBuf,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetAuthorityDto {
    export_sha256: String,
    as_of_unix_nanos: i64,
    selection_sha256: String,
    catalog_identity_sha256: String,
}

impl DatasetAuthorityDto {
    fn into_domain(self) -> Result<PythonDatasetAdmissionAuthority, CliModelAdmissionError> {
        let catalog_identity =
            CatalogEndpointIdentity::try_from_bytes(parse_sha256(&self.catalog_identity_sha256)?)
                .ok_or(ModelAdmissionError::InvalidDatasetAuthority)
                .map_err(CliModelAdmissionError::Dataset)?;
        PythonDatasetAdmissionAuthority::try_new(
            digest(&self.export_sha256)?,
            Timestamp::from_unix_nanos(self.as_of_unix_nanos),
            digest(&self.selection_sha256)?,
            catalog_identity,
        )
        .map_err(CliModelAdmissionError::Dataset)
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BackendAdmissionDto {
    Native,
    Onnx {
        #[serde(rename = "modelSha256")]
        model_sha256: String,
        opset: u32,
        #[serde(rename = "inputShape")]
        input_shape: Vec<usize>,
        #[serde(rename = "outputShape")]
        output_shape: Vec<usize>,
        #[serde(rename = "outputSemantics")]
        output_semantics: Option<OnnxOutputSemanticsDto>,
        #[serde(rename = "inferenceDeadlineMillis")]
        inference_deadline_millis: u64,
        fallback: OnnxFallbackDto,
    },
}

impl BackendAdmissionDto {
    fn into_domain(self) -> Result<ModelBackendAdmission, CliModelAdmissionError> {
        match self {
            Self::Native => Ok(ModelBackendAdmission::Native),
            Self::Onnx {
                model_sha256,
                opset,
                input_shape,
                output_shape,
                output_semantics,
                inference_deadline_millis,
                fallback,
            } => {
                let model_digest = digest(&model_sha256)?;
                let inference_deadline = Duration::from_millis(inference_deadline_millis);
                let fallback = fallback.into_domain();
                let policy = if let Some(output_semantics) = output_semantics {
                    OnnxModelPolicy::try_new_with_output_semantics(
                        model_digest,
                        opset,
                        &input_shape,
                        &output_shape,
                        output_semantics.into_domain(),
                        inference_deadline,
                        fallback,
                    )
                } else {
                    OnnxModelPolicy::try_new(
                        model_digest,
                        opset,
                        &input_shape,
                        &output_shape,
                        inference_deadline,
                        fallback,
                    )
                }
                .map_err(CliModelAdmissionError::OnnxPolicy)?;
                Ok(ModelBackendAdmission::Onnx(policy))
            }
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnnxFallbackDto {
    NoAction,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnnxOutputSemanticsDto {
    Regression,
    BinaryProbability,
}

impl OnnxOutputSemanticsDto {
    const fn into_domain(self) -> ModelOutputSemantics {
        match self {
            Self::Regression => ModelOutputSemantics::Regression,
            Self::BinaryProbability => ModelOutputSemantics::BinaryProbability,
        }
    }
}

impl OnnxFallbackDto {
    const fn into_domain(self) -> OnnxFallbackPolicy {
        match self {
            Self::NoAction => OnnxFallbackPolicy::NoAction,
        }
    }
}

fn digest(value: &str) -> Result<Sha256Digest, CliModelAdmissionError> {
    Ok(Sha256Digest::new(parse_sha256(value)?))
}

fn parse_sha256(value: &str) -> Result<[u8; 32], CliModelAdmissionError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CliModelAdmissionError::InvalidDigest);
    }
    let mut output = [0_u8; 32];
    for (target, pair) in output.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = nibble(pair[0]).ok_or(CliModelAdmissionError::InvalidDigest)?;
        let low = nibble(pair[1]).ok_or(CliModelAdmissionError::InvalidDigest)?;
        *target = (high << 4) | low;
    }
    Ok(output)
}

const fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
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
