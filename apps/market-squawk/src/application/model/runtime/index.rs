//! Canonical bounded model-admission index and immutable-coordinate conflict rules.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Component, Path};
use std::str::FromStr;
use std::time::Duration;

use market_squawk_data::{CatalogEndpointIdentity, Sha256Digest};
use market_squawk_domain::{ModelId, Timestamp};
use market_squawk_modeling::{
    BundleId, BundleMetadataRef, MAX_MODEL_REGISTRY_GENERATIONS, ModelOutputSemantics,
    OnnxFallbackPolicy, OnnxModelPolicy,
};
use market_squawk_platform::LocalAuthorityStateStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const INDEX_SCHEMA_VERSION: u16 = 1;
const HARD_MAXIMUM_INDEX_GENERATIONS: usize = 256;
const STANDARD_MAXIMUM_INDEX_GENERATIONS: usize = 64;
const STANDARD_MAXIMUM_INDEX_BYTES: usize = 7 * 1024 * 1024;
const MAXIMUM_AUTHORITY_BYTES: usize = 256 * 1024;
const MAXIMUM_CANDIDATE_DIRECTORY_BYTES: usize = 512;
const MAXIMUM_CANDIDATE_DIRECTORY_DEPTH: usize = 32;

/// Count and encoded-byte ceilings for the durable model-admission index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelRuntimeIndexLimits {
    maximum_generations: NonZeroUsize,
    maximum_index_bytes: NonZeroUsize,
}

impl ModelRuntimeIndexLimits {
    /// Constructs limits no greater than the process registry and authority-store ceilings.
    ///
    /// # Errors
    ///
    /// Rejects more than 256 generations or an index larger than the two-copy store can commit.
    pub fn try_new(
        maximum_generations: NonZeroUsize,
        maximum_index_bytes: NonZeroUsize,
    ) -> Result<Self, ModelRuntimeIndexError> {
        if maximum_generations.get() > HARD_MAXIMUM_INDEX_GENERATIONS
            || maximum_generations.get() > MAX_MODEL_REGISTRY_GENERATIONS
            || maximum_index_bytes.get() > LocalAuthorityStateStore::maximum_payload_bytes()
        {
            return Err(ModelRuntimeIndexError::InvalidLimits);
        }
        Ok(Self {
            maximum_generations,
            maximum_index_bytes,
        })
    }

    /// Returns bounded local production defaults.
    #[must_use]
    pub const fn standard() -> Self {
        Self {
            maximum_generations: match NonZeroUsize::new(STANDARD_MAXIMUM_INDEX_GENERATIONS) {
                Some(value) => value,
                None => NonZeroUsize::MIN,
            },
            maximum_index_bytes: match NonZeroUsize::new(STANDARD_MAXIMUM_INDEX_BYTES) {
                Some(value) => value,
                None => NonZeroUsize::MIN,
            },
        }
    }

    pub(super) const fn maximum_generations(self) -> NonZeroUsize {
        self.maximum_generations
    }

    pub(super) const fn maximum_index_bytes(self) -> NonZeroUsize {
        self.maximum_index_bytes
    }
}

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
        let inference_deadline_nanos = u64::try_from(policy.inference_deadline().as_nanos())
            .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?;
        Ok(Self::Onnx {
            policy,
            inference_deadline_nanos,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IndexAdmission {
    pub(super) candidate_directory: Box<str>,
    pub(super) metadata_path: Box<str>,
    pub(super) metadata_sha256: Sha256Digest,
    pub(super) authority_bytes: Box<[u8]>,
    pub(super) authority_sha256: Sha256Digest,
    pub(super) dataset_export_sha256: Sha256Digest,
    pub(super) dataset_as_of: Timestamp,
    pub(super) dataset_selection_sha256: Sha256Digest,
    pub(super) catalog_identity: CatalogEndpointIdentity,
    pub(super) model_id: ModelId,
    pub(super) bundle_id: BundleId,
    pub(super) bundle_version: NonZeroU64,
    pub(super) artifact_sha256: Sha256Digest,
    pub(super) training_run_sha256: Sha256Digest,
    pub(super) training_environment_sha256: Sha256Digest,
    pub(super) runtime_policy: StoredRuntimePolicy,
}

impl IndexAdmission {
    pub(super) fn validate(&self) -> Result<(), ModelRuntimeIndexError> {
        validate_candidate_directory(&self.candidate_directory)?;
        BundleMetadataRef::try_new(&self.metadata_path, self.metadata_sha256)
            .map_err(|_| ModelRuntimeIndexError::InvalidRecord)?;
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
            ]
            .iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(ModelRuntimeIndexError::InvalidRecord);
        }
        if let StoredRuntimePolicy::Onnx { policy, .. } = &self.runtime_policy
            && (policy.model_digest() != self.artifact_sha256
                || policy.fallback() != OnnxFallbackPolicy::NoAction)
        {
            return Err(ModelRuntimeIndexError::InvalidRecord);
        }
        Ok(())
    }

    fn coordinate(&self) -> (&BundleId, NonZeroU64) {
        (&self.bundle_id, self.bundle_version)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ModelRuntimeIndex {
    entries: Vec<IndexAdmission>,
}

impl ModelRuntimeIndex {
    pub(super) const fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub(super) fn decode(
        bytes: &[u8],
        limits: ModelRuntimeIndexLimits,
    ) -> Result<Self, ModelRuntimeIndexError> {
        validate_limits(limits)?;
        if bytes.len() > limits.maximum_index_bytes.get() {
            return Err(ModelRuntimeIndexError::CorruptIndex);
        }
        let wire: IndexWire =
            serde_json::from_slice(bytes).map_err(|_| ModelRuntimeIndexError::CorruptIndex)?;
        if wire.schema_version != INDEX_SCHEMA_VERSION
            || wire.entries.len() > limits.maximum_generations.get()
        {
            return Err(ModelRuntimeIndexError::CorruptIndex);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(wire.entries.len())
            .map_err(|_| ModelRuntimeIndexError::ResourceExhausted)?;
        for entry in wire.entries {
            let admission = entry.into_admission()?;
            admission.validate()?;
            entries.push(admission);
        }
        if entries
            .windows(2)
            .any(|pair| pair[0].coordinate() >= pair[1].coordinate())
            || has_series_conflict(&entries)
        {
            return Err(ModelRuntimeIndexError::CorruptIndex);
        }
        let index = Self { entries };
        if index
            .encode(limits)
            .map_err(|_| ModelRuntimeIndexError::CorruptIndex)?
            != bytes
        {
            return Err(ModelRuntimeIndexError::CorruptIndex);
        }
        Ok(index)
    }

    pub(super) fn encode(
        &self,
        limits: ModelRuntimeIndexLimits,
    ) -> Result<Vec<u8>, ModelRuntimeIndexError> {
        validate_limits(limits)?;
        if self.entries.len() > limits.maximum_generations.get()
            || has_series_conflict(&self.entries)
        {
            return Err(ModelRuntimeIndexError::ResourceExhausted);
        }
        for entry in &self.entries {
            entry.validate()?;
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.entries.len())
            .map_err(|_| ModelRuntimeIndexError::ResourceExhausted)?;
        entries.extend(self.entries.iter().map(EntryView::from));
        let bytes = serde_json::to_vec(&IndexView {
            schema_version: INDEX_SCHEMA_VERSION,
            entries,
        })
        .map_err(|_| ModelRuntimeIndexError::CorruptIndex)?;
        if bytes.len() > limits.maximum_index_bytes.get()
            || bytes.len() > LocalAuthorityStateStore::maximum_payload_bytes()
        {
            return Err(ModelRuntimeIndexError::ResourceExhausted);
        }
        Ok(bytes)
    }

    pub(super) fn try_insert(
        &mut self,
        admission: IndexAdmission,
        limits: ModelRuntimeIndexLimits,
    ) -> Result<bool, ModelRuntimeIndexError> {
        validate_limits(limits)?;
        admission.validate()?;
        match self
            .entries
            .binary_search_by(|entry| entry.coordinate().cmp(&admission.coordinate()))
        {
            Ok(position) if self.entries[position] == admission => return Ok(false),
            Ok(_) => return Err(ModelRuntimeIndexError::ImmutableConflict),
            Err(_) if self.entries.len() >= limits.maximum_generations.get() => {
                return Err(ModelRuntimeIndexError::ResourceExhausted);
            }
            Err(_) => {}
        }
        if self.entries.iter().any(|entry| {
            (entry.bundle_id == admission.bundle_id && entry.model_id != admission.model_id)
                || (entry.model_id == admission.model_id && entry.bundle_id != admission.bundle_id)
                || entry.candidate_directory == admission.candidate_directory
        }) {
            return Err(ModelRuntimeIndexError::ImmutableConflict);
        }
        let position = self
            .entries
            .binary_search_by(|entry| entry.coordinate().cmp(&admission.coordinate()))
            .unwrap_or_else(|position| position);
        self.entries
            .try_reserve_exact(1)
            .map_err(|_| ModelRuntimeIndexError::ResourceExhausted)?;
        self.entries.insert(position, admission);
        if let Err(error) = self.encode(limits) {
            self.entries.remove(position);
            return Err(error);
        }
        Ok(true)
    }

    pub(super) fn entries(&self) -> &[IndexAdmission] {
        &self.entries
    }
}

fn validate_limits(limits: ModelRuntimeIndexLimits) -> Result<(), ModelRuntimeIndexError> {
    ModelRuntimeIndexLimits::try_new(limits.maximum_generations, limits.maximum_index_bytes)
        .map(|_| ())
}

fn has_series_conflict(entries: &[IndexAdmission]) -> bool {
    entries.iter().enumerate().any(|(position, entry)| {
        entries[position + 1..].iter().any(|other| {
            (entry.bundle_id == other.bundle_id && entry.model_id != other.model_id)
                || (entry.model_id == other.model_id && entry.bundle_id != other.bundle_id)
                || entry.candidate_directory == other.candidate_directory
        })
    })
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
struct IndexView<'a> {
    schema_version: u16,
    entries: Vec<EntryView<'a>>,
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
    dataset_as_of_unix_nanos: i64,
    dataset_selection_sha256: String,
    catalog_identity_sha256: String,
    model_id: String,
    bundle_id: &'a str,
    bundle_version: u64,
    artifact_sha256: String,
    training_run_sha256: String,
    training_environment_sha256: String,
    runtime_policy: RuntimePolicyView<'a>,
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
            dataset_as_of_unix_nanos: value.dataset_as_of.unix_nanos(),
            dataset_selection_sha256: encode_hex(value.dataset_selection_sha256.bytes()),
            catalog_identity_sha256: encode_hex(value.catalog_identity.bytes()),
            model_id: value.model_id.as_uuid().to_string(),
            bundle_id: value.bundle_id.as_str(),
            bundle_version: value.bundle_version.get(),
            artifact_sha256: encode_hex(value.artifact_sha256.bytes()),
            training_run_sha256: encode_hex(value.training_run_sha256.bytes()),
            training_environment_sha256: encode_hex(value.training_environment_sha256.bytes()),
            runtime_policy: RuntimePolicyView::from(&value.runtime_policy),
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
        #[serde(skip_serializing_if = "Option::is_none")]
        output_semantics: Option<&'static str>,
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
                output_semantics: policy.output_semantics_bound().then_some(
                    match policy.output_semantics() {
                        ModelOutputSemantics::Regression => "regression",
                        ModelOutputSemantics::BinaryProbability => "binary_probability",
                    },
                ),
                inference_deadline_nanos: *inference_deadline_nanos,
                fallback: "no_action",
                policy_sha256: encode_hex(policy.policy_digest()),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexWire {
    schema_version: u16,
    entries: Vec<EntryWire>,
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
    dataset_as_of_unix_nanos: i64,
    dataset_selection_sha256: String,
    catalog_identity_sha256: String,
    model_id: String,
    bundle_id: String,
    bundle_version: u64,
    artifact_sha256: String,
    training_run_sha256: String,
    training_environment_sha256: String,
    runtime_policy: RuntimePolicyWire,
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
            runtime_policy: self.runtime_policy.into_policy(artifact_sha256)?,
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
        output_semantics: Option<String>,
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
                let policy = match output_semantics.as_deref() {
                    None => OnnxModelPolicy::try_new(
                        artifact_sha256,
                        opset,
                        &input_shape,
                        &output_shape,
                        deadline,
                        OnnxFallbackPolicy::NoAction,
                    ),
                    Some("regression") => OnnxModelPolicy::try_new_with_output_semantics(
                        artifact_sha256,
                        opset,
                        &input_shape,
                        &output_shape,
                        ModelOutputSemantics::Regression,
                        deadline,
                        OnnxFallbackPolicy::NoAction,
                    ),
                    Some("binary_probability") => OnnxModelPolicy::try_new_with_output_semantics(
                        artifact_sha256,
                        opset,
                        &input_shape,
                        &output_shape,
                        ModelOutputSemantics::BinaryProbability,
                        deadline,
                        OnnxFallbackPolicy::NoAction,
                    ),
                    Some(_) => return Err(ModelRuntimeIndexError::InvalidRecord),
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
    /// Configured bounds exceed fixed process or persistence ceilings.
    #[error("model runtime index limits are invalid")]
    InvalidLimits,
    /// Persisted bytes are noncanonical, corrupt, or internally inconsistent.
    #[error("model runtime index is corrupt")]
    CorruptIndex,
    /// One record contains an invalid path, identity, digest, authority, or runtime policy.
    #[error("model runtime index record is invalid")]
    InvalidRecord,
    /// An immutable coordinate, model series, bundle series, or candidate root was reused.
    #[error("model runtime immutable admission conflicts")]
    ImmutableConflict,
    /// Count, encoded-byte, or allocation bounds were exhausted.
    #[error("model runtime index resource ceiling was exceeded")]
    ResourceExhausted,
}

#[cfg(test)]
impl IndexAdmission {
    fn fixture(directory: u8) -> Result<Self, ModelRuntimeIndexError> {
        let authority_bytes = br#"{"schema_version":5}"#.to_vec().into_boxed_slice();
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
            runtime_policy: StoredRuntimePolicy::Native,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{IndexAdmission, ModelRuntimeIndex, ModelRuntimeIndexLimits};

    #[test]
    fn model_runtime_index_replay_is_canonical_and_conflicts_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = ModelRuntimeIndexLimits::standard();
        let first = IndexAdmission::fixture(1)?;
        let conflict = IndexAdmission::fixture(2)?;
        let mut index = ModelRuntimeIndex::empty();

        assert_eq!(index.try_insert(first.clone(), limits), Ok(true));
        assert_eq!(index.try_insert(first, limits), Ok(false));
        assert!(index.try_insert(conflict, limits).is_err());

        let encoded = index.encode(limits)?;
        let recovered = ModelRuntimeIndex::decode(&encoded, limits)?;
        assert_eq!(recovered.encode(limits)?, encoded);
        Ok(())
    }
}
