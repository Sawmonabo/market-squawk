//! Canonical bounded model-admission index and immutable-coordinate conflict rules.

use std::num::NonZeroU64;
use std::path::{Component, Path};
use std::str::FromStr;
use std::time::Duration;

use market_squawk_data::{CatalogEndpointIdentity, FeatureDatasetProductContract, Sha256Digest};
use market_squawk_domain::{ModelId, Timestamp};
use market_squawk_modeling::{
    BundleId, BundleMetadataRef, ModelOutputSemantics, OnnxFallbackPolicy, OnnxModelPolicy,
    PythonDatasetAdmissionAuthority,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const MAXIMUM_AUTHORITY_BYTES: usize = 256 * 1024;
const MAXIMUM_CANDIDATE_DIRECTORY_BYTES: usize = 512;
const MAXIMUM_CANDIDATE_DIRECTORY_DEPTH: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum StoredRuntimePolicy {
    Native,
    Onnx {
        policy: OnnxModelPolicy,
        inference_deadline_nanos: u64,
    },
}

impl StoredRuntimePolicy {
    pub(super) fn try_onnx(policy: OnnxModelPolicy) -> Result<Self, ModelRuntimeIndexError> {
        if !policy.output_semantics_bound() {
            return Err(ModelRuntimeIndexError::InvalidRecord);
        }
        let inference_deadline_nanos = u64::try_from(policy.inference_deadline().as_nanos())
            .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?;
        Ok(Self::Onnx {
            policy,
            inference_deadline_nanos,
        })
    }
}

/// Only the owning job context can attach this binding to a runtime request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct TrainingJobBinding {
    pub(super) id: uuid::Uuid,
    pub(super) generation: NonZeroU64,
    pub(super) input_sha256: [u8; 32],
    pub(super) stderr_bytes: u64,
    pub(super) stderr_sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::application::model) struct IndexAdmission {
    pub(in crate::application::model) candidate_directory: Box<str>,
    pub(in crate::application::model) metadata_path: Box<str>,
    pub(in crate::application::model) metadata_sha256: Sha256Digest,
    pub(in crate::application::model) authority_bytes: Box<[u8]>,
    pub(in crate::application::model) authority_sha256: Sha256Digest,
    pub(in crate::application::model) dataset_export_sha256: Sha256Digest,
    pub(in crate::application::model) dataset_product_contract: FeatureDatasetProductContract,
    pub(in crate::application::model) dataset_as_of: Timestamp,
    pub(in crate::application::model) dataset_selection_sha256: Sha256Digest,
    pub(in crate::application::model) catalog_identity: CatalogEndpointIdentity,
    pub(in crate::application::model) model_id: ModelId,
    pub(in crate::application::model) bundle_id: BundleId,
    pub(in crate::application::model) bundle_version: NonZeroU64,
    pub(in crate::application::model) artifact_sha256: Sha256Digest,
    pub(in crate::application::model) training_run_sha256: Sha256Digest,
    pub(in crate::application::model) training_environment_sha256: Sha256Digest,
    pub(in crate::application::model) output_binding_sha256: Sha256Digest,
    pub(in crate::application::model) runtime_policy: StoredRuntimePolicy,
    pub(in crate::application::model) product_summary: serde_json::Value,
    pub(super) training_job: Option<TrainingJobBinding>,
}

impl IndexAdmission {
    pub(super) fn encode_record(&self) -> Result<Box<[u8]>, ModelRuntimeIndexError> {
        self.validate()?;
        serde_json::to_vec(&EntryView::from(self))
            .map(Vec::into_boxed_slice)
            .map_err(|_| ModelRuntimeIndexError::InvalidRecord)
    }

    pub(super) fn decode_record(bytes: &[u8]) -> Result<Self, ModelRuntimeIndexError> {
        let wire: EntryWire =
            serde_json::from_slice(bytes).map_err(|_| ModelRuntimeIndexError::InvalidRecord)?;
        let entry = wire.into_admission()?;
        entry.validate()?;
        if entry.encode_record()?.as_ref() != bytes {
            return Err(ModelRuntimeIndexError::InvalidRecord);
        }
        Ok(entry)
    }

    pub(super) fn dataset_authority(
        &self,
    ) -> Result<PythonDatasetAdmissionAuthority, ModelRuntimeIndexError> {
        PythonDatasetAdmissionAuthority::try_new(
            self.dataset_export_sha256,
            self.dataset_as_of,
            self.dataset_selection_sha256,
            self.catalog_identity,
            self.dataset_product_contract,
        )
        .map_err(|_| ModelRuntimeIndexError::InvalidRecord)
    }

    pub(super) fn validate(&self) -> Result<(), ModelRuntimeIndexError> {
        validate_candidate_directory(&self.candidate_directory)?;
        BundleMetadataRef::try_new(&self.metadata_path, self.metadata_sha256)
            .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?;
        self.dataset_authority()?;
        let summary = self
            .product_summary
            .as_object()
            .ok_or(ModelRuntimeIndexError::InvalidRecord)?;
        if summary.len() != 3
            || summary
                .get("modelToken")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse::<uuid::Uuid>().ok())
                .is_none()
            || summary
                .get("label")
                .and_then(serde_json::Value::as_str)
                .is_none_or(|value| value.is_empty() || value.len() > 1024)
            || !matches!(
                summary
                    .get("evidenceState")
                    .and_then(serde_json::Value::as_str),
                Some("sufficient" | "limited" | "unavailable")
            )
        {
            return Err(ModelRuntimeIndexError::InvalidRecord);
        }
        if self.authority_bytes.is_empty()
            || self.authority_bytes.len() > MAXIMUM_AUTHORITY_BYTES
            || Sha256Digest::new(Sha256::digest(&self.authority_bytes).into())
                != self.authority_sha256
            || [
                self.metadata_sha256,
                self.dataset_export_sha256,
                self.dataset_selection_sha256,
                self.artifact_sha256,
                self.training_run_sha256,
                self.training_environment_sha256,
                self.output_binding_sha256,
            ]
            .iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(ModelRuntimeIndexError::InvalidRecord);
        }
        if let StoredRuntimePolicy::Onnx { policy, .. } = &self.runtime_policy
            && (policy.model_digest() != self.artifact_sha256
                || !policy.output_semantics_bound()
                || policy.fallback() != OnnxFallbackPolicy::NoAction)
        {
            return Err(ModelRuntimeIndexError::InvalidRecord);
        }
        if let Some(job) = &self.training_job {
            if job.id.is_nil()
                || job.input_sha256 == [0; 32]
                || job.stderr_sha256 == [0; 32]
                || job.stderr_bytes
                    > market_squawk_modeling::MAX_TRAINING_WORKER_STDERR_BYTES as u64
                || self.candidate_directory.as_ref()
                    != format!(
                        "models/training-{}/generation-{}/candidate",
                        job.id, job.generation
                    )
            {
                return Err(ModelRuntimeIndexError::InvalidRecord);
            }
        }
        Ok(())
    }

    pub(super) fn training_result_sha256(&self) -> Option<Sha256Digest> {
        let job = self.training_job.as_ref()?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/model-training-result/v1\0");
        for digest in [
            self.metadata_sha256,
            self.artifact_sha256,
            self.training_run_sha256,
            self.authority_sha256,
            self.dataset_export_sha256,
            self.dataset_selection_sha256,
        ] {
            hash.update(digest.bytes());
        }
        hash.update(job.stderr_bytes.to_be_bytes());
        hash.update(job.stderr_sha256);
        Some(Sha256Digest::new(hash.finalize().into()))
    }
}

pub(super) fn validate_candidate_directory(value: &str) -> Result<(), ModelRuntimeIndexError> {
    let path = Path::new(value);
    let mut depth = 0_usize;
    let components_valid = path.components().all(|component| {
        depth = depth.saturating_add(1);
        matches!(
            component,
            Component::Normal(value)
                if value.to_str().is_some_and(|value| {
                    !value.is_empty()
                        && value.len() <= 255
                        && value.bytes().all(|byte| {
                            byte.is_ascii_lowercase()
                                || byte.is_ascii_digit()
                                || matches!(byte, b'-' | b'_' | b'.')
                        })
                })
        )
    });
    if value.is_empty()
        || value.len() > MAXIMUM_CANDIDATE_DIRECTORY_BYTES
        || value.contains(['\\', ':'])
        || path.is_absolute()
        || depth == 0
        || depth > MAXIMUM_CANDIDATE_DIRECTORY_DEPTH
        || !components_valid
    {
        return Err(ModelRuntimeIndexError::InvalidRecord);
    }
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EntryView<'a> {
    candidate_directory: &'a str,
    metadata_path: &'a str,
    metadata_sha256: String,
    authority_hex: String,
    authority_sha256: String,
    dataset_export_sha256: String,
    dataset_product_contract: &'static str,
    dataset_as_of_unix_nanos: i64,
    dataset_selection_sha256: String,
    catalog_identity_sha256: String,
    model_id: String,
    bundle_id: &'a str,
    bundle_version: u64,
    artifact_sha256: String,
    training_run_sha256: String,
    training_environment_sha256: String,
    output_binding_sha256: String,
    runtime_policy: RuntimePolicyView<'a>,
    training_job: Option<&'a TrainingJobBinding>,
    product_summary: &'a serde_json::Value,
}

impl<'a> From<&'a IndexAdmission> for EntryView<'a> {
    fn from(value: &'a IndexAdmission) -> Self {
        Self {
            candidate_directory: &value.candidate_directory,
            metadata_path: &value.metadata_path,
            metadata_sha256: encode_hex(value.metadata_sha256.bytes()),
            authority_hex: encode_hex_slice(&value.authority_bytes),
            authority_sha256: encode_hex(value.authority_sha256.bytes()),
            dataset_export_sha256: encode_hex(value.dataset_export_sha256.bytes()),
            dataset_product_contract: value.dataset_product_contract.identity(),
            dataset_as_of_unix_nanos: value.dataset_as_of.unix_nanos(),
            dataset_selection_sha256: encode_hex(value.dataset_selection_sha256.bytes()),
            catalog_identity_sha256: encode_hex(value.catalog_identity.bytes()),
            model_id: value.model_id.as_uuid().to_string(),
            bundle_id: value.bundle_id.as_str(),
            bundle_version: value.bundle_version.get(),
            artifact_sha256: encode_hex(value.artifact_sha256.bytes()),
            training_run_sha256: encode_hex(value.training_run_sha256.bytes()),
            training_environment_sha256: encode_hex(value.training_environment_sha256.bytes()),
            output_binding_sha256: encode_hex(value.output_binding_sha256.bytes()),
            runtime_policy: RuntimePolicyView::from(&value.runtime_policy),
            training_job: value.training_job.as_ref(),
            product_summary: &value.product_summary,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RuntimePolicyView<'a> {
    Native,
    Onnx {
        model_sha256: String,
        opset: u32,
        input_shape: &'a [usize],
        output_shape: &'a [usize],
        output_semantics: &'static str,
        forecast_horizons: Option<&'a [u32]>,
        inference_deadline_nanos: u64,
        fallback: &'static str,
        policy_sha256: String,
    },
}

impl<'a> From<&'a StoredRuntimePolicy> for RuntimePolicyView<'a> {
    fn from(value: &'a StoredRuntimePolicy) -> Self {
        match value {
            StoredRuntimePolicy::Native => Self::Native,
            StoredRuntimePolicy::Onnx {
                policy,
                inference_deadline_nanos,
            } => Self::Onnx {
                model_sha256: encode_hex(policy.model_digest().bytes()),
                opset: policy.opset(),
                input_shape: policy.input_shape(),
                output_shape: policy.output_shape(),
                output_semantics: match policy.output_semantics() {
                    ModelOutputSemantics::Regression => "regression",
                    ModelOutputSemantics::BinaryProbability => "binary_probability",
                },
                forecast_horizons: policy.forecast_horizons(),
                inference_deadline_nanos: *inference_deadline_nanos,
                fallback: "no_action",
                policy_sha256: encode_hex(policy.policy_digest()),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EntryWire {
    candidate_directory: String,
    metadata_path: String,
    metadata_sha256: String,
    authority_hex: String,
    authority_sha256: String,
    dataset_export_sha256: String,
    dataset_product_contract: String,
    dataset_as_of_unix_nanos: i64,
    dataset_selection_sha256: String,
    catalog_identity_sha256: String,
    model_id: String,
    bundle_id: String,
    bundle_version: u64,
    artifact_sha256: String,
    training_run_sha256: String,
    training_environment_sha256: String,
    output_binding_sha256: String,
    runtime_policy: RuntimePolicyWire,
    #[serde(deserialize_with = "Option::deserialize")]
    training_job: Option<TrainingJobBinding>,
    product_summary: serde_json::Value,
}

impl EntryWire {
    fn into_admission(self) -> Result<IndexAdmission, ModelRuntimeIndexError> {
        let artifact_sha256 = Sha256Digest::new(decode_hex(&self.artifact_sha256)?);
        Ok(IndexAdmission {
            candidate_directory: self.candidate_directory.into(),
            metadata_path: self.metadata_path.into(),
            metadata_sha256: Sha256Digest::new(decode_hex(&self.metadata_sha256)?),
            authority_bytes: decode_hex_slice(&self.authority_hex)?.into_boxed_slice(),
            authority_sha256: Sha256Digest::new(decode_hex(&self.authority_sha256)?),
            dataset_export_sha256: Sha256Digest::new(decode_hex(&self.dataset_export_sha256)?),
            dataset_product_contract: FeatureDatasetProductContract::from_identity(
                &self.dataset_product_contract,
            )
            .ok_or(ModelRuntimeIndexError::InvalidRecord)?,
            dataset_as_of: Timestamp::from_unix_nanos(self.dataset_as_of_unix_nanos),
            dataset_selection_sha256: Sha256Digest::new(decode_hex(
                &self.dataset_selection_sha256,
            )?),
            catalog_identity: CatalogEndpointIdentity::try_from_bytes(decode_hex(
                &self.catalog_identity_sha256,
            )?)
            .ok_or(ModelRuntimeIndexError::InvalidRecord)?,
            model_id: ModelId::from_str(&self.model_id)
                .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?,
            bundle_id: BundleId::try_new(&self.bundle_id)
                .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?,
            bundle_version: NonZeroU64::new(self.bundle_version)
                .ok_or(ModelRuntimeIndexError::InvalidRecord)?,
            artifact_sha256,
            training_run_sha256: Sha256Digest::new(decode_hex(&self.training_run_sha256)?),
            training_environment_sha256: Sha256Digest::new(decode_hex(
                &self.training_environment_sha256,
            )?),
            output_binding_sha256: Sha256Digest::new(decode_hex(&self.output_binding_sha256)?),
            runtime_policy: self.runtime_policy.into_policy(artifact_sha256)?,
            training_job: self.training_job,
            product_summary: self.product_summary,
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RuntimePolicyWire {
    Native,
    Onnx {
        model_sha256: String,
        opset: u32,
        input_shape: Vec<usize>,
        output_shape: Vec<usize>,
        output_semantics: String,
        #[serde(deserialize_with = "Option::deserialize")]
        forecast_horizons: Option<Vec<u32>>,
        inference_deadline_nanos: u64,
        fallback: String,
        policy_sha256: String,
    },
}

impl RuntimePolicyWire {
    fn into_policy(
        self,
        artifact_sha256: Sha256Digest,
    ) -> Result<StoredRuntimePolicy, ModelRuntimeIndexError> {
        match self {
            Self::Native => Ok(StoredRuntimePolicy::Native),
            Self::Onnx {
                model_sha256,
                opset,
                input_shape,
                output_shape,
                output_semantics,
                forecast_horizons,
                inference_deadline_nanos,
                fallback,
                policy_sha256,
            } => {
                if fallback != "no_action"
                    || Sha256Digest::new(decode_hex(&model_sha256)?) != artifact_sha256
                {
                    return Err(ModelRuntimeIndexError::InvalidRecord);
                }
                let deadline = Duration::from_nanos(inference_deadline_nanos);
                let policy = match (output_semantics.as_str(), forecast_horizons.as_deref()) {
                    ("regression", Some(horizons)) => OnnxModelPolicy::try_new_forecast(
                        artifact_sha256,
                        opset,
                        &input_shape,
                        &output_shape,
                        horizons,
                        deadline,
                        OnnxFallbackPolicy::NoAction,
                    ),
                    ("regression", None) => OnnxModelPolicy::try_new_with_output_semantics(
                        artifact_sha256,
                        opset,
                        &input_shape,
                        &output_shape,
                        ModelOutputSemantics::Regression,
                        deadline,
                        OnnxFallbackPolicy::NoAction,
                    ),
                    ("binary_probability", None) => OnnxModelPolicy::try_new_with_output_semantics(
                        artifact_sha256,
                        opset,
                        &input_shape,
                        &output_shape,
                        ModelOutputSemantics::BinaryProbability,
                        deadline,
                        OnnxFallbackPolicy::NoAction,
                    ),
                    _ => return Err(ModelRuntimeIndexError::InvalidRecord),
                }
                .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?;
                if policy.policy_digest() != decode_hex(&policy_sha256)? {
                    return Err(ModelRuntimeIndexError::InvalidRecord);
                }
                StoredRuntimePolicy::try_onnx(policy)
            }
        }
    }
}

fn encode_hex(bytes: [u8; 32]) -> String {
    encode_hex_slice(&bytes)
}

fn encode_hex_slice(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex(value: &str) -> Result<[u8; 32], ModelRuntimeIndexError> {
    let bytes = decode_hex_slice(value)?;
    bytes
        .try_into()
        .map_err(|_| ModelRuntimeIndexError::InvalidRecord)
}

fn decode_hex_slice(value: &str) -> Result<Vec<u8>, ModelRuntimeIndexError> {
    if value.is_empty()
        || !value.len().is_multiple_of(2)
        || value
            .bytes()
            .any(|byte| !(byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
    {
        return Err(ModelRuntimeIndexError::InvalidRecord);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(value.len() / 2)
        .map_err(|_| ModelRuntimeIndexError::ResourceExhausted)?;
    for pair in value.as_bytes().chunks_exact(2) {
        let high = nibble(pair[0]).ok_or(ModelRuntimeIndexError::InvalidRecord)?;
        let low = nibble(pair[1]).ok_or(ModelRuntimeIndexError::InvalidRecord)?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

const fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

/// Durable model-index validation or immutable admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ModelRuntimeIndexError {
    /// Persisted bytes are noncanonical, corrupt, or internally inconsistent.
    #[error("model runtime index is corrupt")]
    CorruptIndex,
    /// One record contains an invalid path, identity, digest, authority, or runtime policy.
    #[error("model runtime index record is invalid")]
    InvalidRecord,
    /// Count, encoded-byte, or allocation bounds were exhausted.
    #[error("model runtime index resource ceiling was exceeded")]
    ResourceExhausted,
}

#[cfg(test)]
impl IndexAdmission {
    fn fixture(directory: u8) -> Result<Self, ModelRuntimeIndexError> {
        // This proof exercises opaque index retention, not model-authority admission. Keep the
        // fixture deliberately non-semantic so it cannot masquerade as an obsolete authority wire.
        let authority_bytes = b"opaque-current-authority-fixture"
            .to_vec()
            .into_boxed_slice();
        let authority_sha256 = Sha256Digest::new(Sha256::digest(&authority_bytes).into());
        let catalog_identity = CatalogEndpointIdentity::try_from_bytes([3; 32])
            .ok_or(ModelRuntimeIndexError::InvalidRecord)?;
        Ok(Self {
            candidate_directory: format!("models/candidate-{directory}").into(),
            metadata_path: "bundle.json".into(),
            metadata_sha256: Sha256Digest::new([4; 32]),
            authority_bytes,
            authority_sha256,
            dataset_export_sha256: Sha256Digest::new([5; 32]),
            dataset_product_contract: FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1,
            dataset_as_of: Timestamp::from_unix_nanos(10),
            dataset_selection_sha256: Sha256Digest::new([6; 32]),
            catalog_identity,
            model_id: ModelId::from_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
                .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?,
            bundle_id: BundleId::try_new("fixture-model")
                .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?,
            bundle_version: NonZeroU64::MIN,
            artifact_sha256: Sha256Digest::new([7; 32]),
            training_run_sha256: Sha256Digest::new([8; 32]),
            training_environment_sha256: Sha256Digest::new([9; 32]),
            output_binding_sha256: Sha256Digest::new([10; 32]),
            runtime_policy: StoredRuntimePolicy::Native,
            training_job: None,
            product_summary: serde_json::json!({"modelToken":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","label":"fixture","evidenceState":"limited"}),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::IndexAdmission;

    #[test]
    fn model_admission_record_round_trip_rejects_noncanonical_and_changed_summary()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = IndexAdmission::fixture(1)?;
        let encoded = first.encode_record()?;
        let recovered = IndexAdmission::decode_record(&encoded)?;
        assert_eq!(recovered, first);
        let mut noncanonical = encoded.to_vec();
        noncanonical.push(b' ');
        assert!(IndexAdmission::decode_record(&noncanonical).is_err());
        let mut invalid = first;
        invalid.product_summary["evidenceState"] = serde_json::json!("unsupported");
        assert!(invalid.encode_record().is_err());

        // Canonical inventory durability, not candidate/weight admission: reuse this
        // existing opaque model-owner fixture through the real sole catalog authority.
        use market_squawk_data::{
            AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
            CatalogLimit, CatalogResultLimits, ModelInventoryError, ModelInventoryRecord,
            ObjectStoreConfig,
        };
        use market_squawk_platform::LocalPaths;
        use std::{num::NonZeroU64, time::Duration};
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("model-inventory"))?;
        let config = CatalogConfig::try_new(
            paths.catalog()?.clone(),
            Duration::from_millis(250),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?;
        let objects = ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?;
        let service = AnalyticalDataService::initialize(
            CatalogAuthority::open(config.clone())?,
            AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
            paths.artifacts()?.clone(),
            objects,
        )?;
        let inventory = service.model_inventory();
        let record = |version: u8| -> Result<ModelInventoryRecord, Box<dyn std::error::Error>> {
            let mut admission = IndexAdmission::fixture(version)?;
            admission.bundle_version =
                NonZeroU64::new(u64::from(version)).ok_or("nonzero version")?;
            let model_token = uuid::Uuid::from_bytes([version; 16]);
            admission.product_summary["modelToken"] = serde_json::json!(model_token);
            Ok(ModelInventoryRecord {
                model_id: admission.model_id,
                model_token,
                bundle_id: admission.bundle_id.as_str().to_owned(),
                bundle_version: admission.bundle_version,
                candidate_directory: admission.candidate_directory.to_string(),
                record: admission.encode_record()?,
            })
        };
        for version in 1..=65 {
            let (head, inserted) = inventory.publish(&record(version)?)?;
            assert!(inserted);
            assert_eq!(head.sequence, u64::from(version));
        }
        let fence = inventory.head()?;
        let first_page = inventory.page(fence, 0)?;
        assert_eq!(first_page.len(), 32);
        for (version, entry) in (1..=32).zip(&first_page) {
            assert_eq!(entry.admission, record(version)?);
        }
        let continuation = first_page
            .last()
            .ok_or("nonempty first page")?
            .head
            .sequence;
        let (current_head, inserted) = inventory.publish(&record(66)?)?;
        assert!(inserted);
        assert_eq!(current_head.sequence, 66);
        // Drop every capability owning the writer before reopening this same physical catalog.
        drop(inventory);
        drop(service);
        let reopened = AnalyticalDataService::open(
            CatalogAuthority::open(config)?,
            AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
            paths.artifacts()?.clone(),
            objects,
        )?;
        let inventory = reopened.model_inventory();
        assert_eq!(inventory.head()?, current_head);
        inventory.verify(current_head)?;
        inventory.verify(fence)?;
        let second_page = inventory.page(fence, continuation)?;
        assert_eq!(second_page.len(), 32);
        for (version, entry) in (33..=64).zip(&second_page) {
            assert_eq!(entry.admission, record(version)?);
        }
        let last_page = inventory.page(
            fence,
            second_page
                .last()
                .ok_or("nonempty second page")?
                .head
                .sequence,
        )?;
        assert_eq!(last_page.len(), 1);
        assert_eq!(last_page[0].admission, record(65)?);
        assert!(inventory.page(fence, 65)?.is_empty());
        let unseen = record(66)?;
        assert!(
            inventory
                .get(fence, &unseen.bundle_id, unseen.bundle_version)?
                .is_none()
        );
        assert_eq!(
            inventory
                .by_token(current_head, unseen.model_token)?
                .ok_or("exact new token")?
                .admission,
            unseen
        );
        let retained = record(65)?;
        assert_eq!(inventory.publish(&retained)?, (current_head, false));
        let mut changed = retained.clone();
        let mut admission = IndexAdmission::decode_record(&changed.record)?;
        admission.product_summary["label"] = serde_json::json!("changed immutable row");
        changed.record = admission.encode_record()?;
        assert!(matches!(
            inventory.publish(&changed),
            Err(ModelInventoryError::Conflict)
        ));
        assert_eq!(inventory.head()?, current_head);
        assert_eq!(
            inventory
                .get(fence, &retained.bundle_id, retained.bundle_version)?
                .ok_or("exact retained version")?
                .admission,
            retained
        );
        Ok(())
    }
}
