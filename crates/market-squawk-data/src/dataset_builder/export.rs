//! Canonical bounded Task 11 export descriptor for Python research consumers.

use market_squawk_domain::Timestamp;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::DatasetBuildError;
use super::model::{
    ComponentKind, ComponentScope, CorporateActionSensitivity, DatasetSplitCounts,
    FeatureLabelComponentSpec, FeatureLabelDataset, MissingValuePolicy,
};
use crate::{DatasetManifestRef, GenerationParentRelation, PointInTimeRevisionMode, Sha256Digest};

/// Maximum exact bytes in one Task 11 feature/label export descriptor.
pub const MAX_FEATURE_LABEL_EXPORT_BYTES: usize = 1024 * 1024;

/// Exact canonical descriptor and its caller-pinned SHA-256 identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureLabelPythonExport {
    bytes: Box<[u8]>,
    content_hash: Sha256Digest,
}

impl FeatureLabelPythonExport {
    /// Returns exact descriptor bytes produced by Task 11.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the SHA-256 identity callers must pin at the Python boundary.
    #[must_use]
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }
}

pub(super) fn encode(
    dataset: &FeatureLabelDataset,
) -> Result<FeatureLabelPythonExport, DatasetBuildError> {
    let manifest = dataset.manifest();
    let [train_end, validation_end, test_end] = dataset.split_policy.boundaries();
    let wire = ExportWire {
        components: dataset.component_specs.iter().map(component_wire).collect(),
        dataset: DatasetWire {
            build_spec_sha256: hex(dataset.build_spec_digest.digest()),
            dataset_id: manifest.dataset_id().as_str(),
            manifest_sha256: hex(manifest.content_hash()),
            manifest_version: manifest.manifest_version(),
            policy_sha256: hex(dataset.policy_digest),
            schema_name: manifest.schema().name(),
            schema_sha256: hex_bytes(manifest.schema().fingerprint()),
            schema_version: manifest.schema().version().get(),
            universe_id: dataset.universe_id.as_str(),
            universe_sha256: hex(dataset.universe_digest),
        },
        missing_value_policy: missing_value_name(dataset.missing_value_policy),
        objects: dataset
            .pinned
            .objects()
            .iter()
            .map(|value| ObjectWire {
                artifact_id: value.artifact_id().to_string(),
                lineage_sha256: hex(value.object().lineage_digest()),
                path: value.relative_reference(),
                row_count: value.object().row_count(),
                sha256: hex(value.object().content_hash()),
                size_bytes: value.object().size_bytes(),
            })
            .collect(),
        parents: dataset
            .pinned
            .parents()
            .iter()
            .map(|parent| ParentWire {
                manifest: manifest_wire(parent.manifest()),
                relation: relation_name(parent.relation()),
            })
            .collect(),
        point_in_time: PointInTimeWire {
            revision_mode: match dataset.point_in_time_policy.revision_mode() {
                PointInTimeRevisionMode::LatestKnown => "latest_known",
                PointInTimeRevisionMode::AllKnown => "all_known",
            },
            version: dataset.point_in_time_policy.version().get(),
        },
        schema_version: 2,
        split_counts: split_counts_wire(dataset.split_counts),
        split_policy: SplitPolicyWire {
            test_end_unix_nanos: nanos(test_end),
            train_end_unix_nanos: nanos(train_end),
            validation_end_unix_nanos: nanos(validation_end),
        },
    };
    let bytes = serde_json::to_vec(&wire).map_err(|_| DatasetBuildError::ExportEncoding)?;
    if bytes.is_empty() || bytes.len() > MAX_FEATURE_LABEL_EXPORT_BYTES {
        return Err(DatasetBuildError::ExportEncoding);
    }
    Ok(FeatureLabelPythonExport {
        content_hash: Sha256Digest::new(Sha256::digest(&bytes).into()),
        bytes: bytes.into_boxed_slice(),
    })
}

#[derive(Serialize)]
struct ExportWire<'a> {
    components: Vec<ComponentWire<'a>>,
    dataset: DatasetWire<'a>,
    missing_value_policy: &'static str,
    objects: Vec<ObjectWire<'a>>,
    parents: Vec<ParentWire<'a>>,
    point_in_time: PointInTimeWire,
    schema_version: u32,
    split_counts: SplitCountsWire,
    split_policy: SplitPolicyWire,
}

#[derive(Serialize)]
struct DatasetWire<'a> {
    build_spec_sha256: String,
    dataset_id: &'a str,
    manifest_sha256: String,
    manifest_version: u64,
    policy_sha256: String,
    schema_name: &'a str,
    schema_sha256: String,
    schema_version: u16,
    universe_id: &'a str,
    universe_sha256: String,
}

#[derive(Serialize)]
struct ManifestWire<'a> {
    dataset_id: &'a str,
    manifest_sha256: String,
    manifest_version: u64,
    schema_name: &'a str,
    schema_sha256: String,
    schema_version: u16,
}

#[derive(Serialize)]
struct ParentWire<'a> {
    manifest: ManifestWire<'a>,
    relation: &'static str,
}

#[derive(Serialize)]
struct ObjectWire<'a> {
    artifact_id: String,
    lineage_sha256: String,
    path: &'a str,
    row_count: u64,
    sha256: String,
    size_bytes: u64,
}

#[derive(Serialize)]
struct ComponentWire<'a> {
    corporate_action_sensitivity: &'static str,
    kind: &'static str,
    name: &'a str,
    scope: &'static str,
    version: u32,
}

#[derive(Serialize)]
struct PointInTimeWire {
    revision_mode: &'static str,
    version: u32,
}

#[derive(Serialize)]
struct SplitCountsWire {
    test: usize,
    train: usize,
    validation: usize,
}

#[derive(Serialize)]
struct SplitPolicyWire {
    test_end_unix_nanos: i64,
    train_end_unix_nanos: i64,
    validation_end_unix_nanos: i64,
}

fn manifest_wire(manifest: &DatasetManifestRef) -> ManifestWire<'_> {
    ManifestWire {
        dataset_id: manifest.dataset_id().as_str(),
        manifest_sha256: hex(manifest.content_hash()),
        manifest_version: manifest.manifest_version(),
        schema_name: manifest.schema().name(),
        schema_sha256: hex_bytes(manifest.schema().fingerprint()),
        schema_version: manifest.schema().version().get(),
    }
}

fn component_wire(spec: &FeatureLabelComponentSpec) -> ComponentWire<'_> {
    ComponentWire {
        corporate_action_sensitivity: match spec.corporate_actions() {
            CorporateActionSensitivity::NotApplicable => "not_applicable",
            CorporateActionSensitivity::RequiresAdjustment => "requires_adjustment",
        },
        kind: match spec.kind() {
            ComponentKind::Feature => "feature",
            ComponentKind::Label => "label",
        },
        name: spec.name(),
        scope: match spec.scope() {
            ComponentScope::Instrument => "instrument",
            ComponentScope::Account => "account",
            ComponentScope::Global => "global",
        },
        version: spec.version().get(),
    }
}

const fn split_counts_wire(counts: DatasetSplitCounts) -> SplitCountsWire {
    SplitCountsWire {
        test: counts.test_examples(),
        train: counts.train_examples(),
        validation: counts.validation_examples(),
    }
}

const fn missing_value_name(policy: MissingValuePolicy) -> &'static str {
    match policy {
        MissingValuePolicy::Reject => "reject",
        MissingValuePolicy::Preserve => "preserve",
        MissingValuePolicy::DropExample => "drop_example",
    }
}

const fn relation_name(relation: GenerationParentRelation) -> &'static str {
    match relation {
        GenerationParentRelation::AppendPredecessor => "append_predecessor",
        GenerationParentRelation::CompactionPredecessor => "compaction_predecessor",
        GenerationParentRelation::DerivedInput => "derived_input",
    }
}

const fn nanos(value: Timestamp) -> i64 {
    value.unix_nanos()
}

fn hex(digest: Sha256Digest) -> String {
    hex_bytes(digest.bytes())
}

fn hex_bytes(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
