//! Canonical bounded Task 11 export descriptor for Python research consumers.

use market_squawk_domain::{CalendarDate, FundamentalCadence};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::DatasetBuildError;
use super::model::{
    ComponentKind, ComponentScope, CorporateActionSensitivity, DatasetSplitCounts,
    FeatureLabelComponentSpec, FeatureLabelDataset, FeatureLabelMeasurement,
    FeatureLabelMeasurementBinding, MissingValuePolicy,
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
    let wire = ExportWire {
        study: dataset
            .study_policy()
            .copied()
            .zip(dataset.source_snapshot_digest())
            .map(|(policy, digest)| super::study::StudyWire::new(policy, digest)),
        components: dataset
            .component_specs
            .iter()
            .map(|spec| component_wire(spec, &dataset.label_measurements))
            .collect(),
        dataset: DatasetWire {
            build_spec_sha256: hex(dataset.build_spec_digest.digest()),
            dataset_id: manifest.dataset_id().as_str(),
            manifest_sha256: hex(manifest.content_hash()),
            manifest_version: manifest.manifest_version(),
            policy_sha256: hex(dataset.policy_digest),
            population_basis: dataset.population_basis(),
            price_input_origin: dataset.price_input_origin(),
            population_member_count: dataset.population_member_count(),
            population_unavailable: dataset.population_unavailable(),
            population_partition: dataset.population_partition(),
            population_source_use: dataset.population_source_use(),
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
        schema_version: 4,
        split_counts: split_counts_wire(dataset.split_counts),
        split_policy: SplitPolicyWire::new(dataset.split_policy)?,
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
    study: Option<super::study::StudyWire>,
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
    population_basis: super::DatasetPopulationBasis,
    price_input_origin: Option<super::DatasetPriceInputOrigin>,
    population_member_count: usize,
    population_unavailable: &'a [super::CurrentPopulationInputUnavailable],
    population_partition: Option<&'a crate::DatasetPopulationPartition>,
    population_source_use: Option<&'a crate::DatasetPopulationSourceUse>,
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
    measurement: Option<MeasurementWire>,
    name: &'a str,
    scope: &'static str,
    target: TargetWire,
    version: u32,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
enum MeasurementWire {
    #[serde(rename = "financial_amount")]
    FinancialAmount {
        role: super::FinancialAmountRole,
        basis: super::FinancialAmountBasis,
        currency: String,
        share_convention: Option<super::FinancialShareConvention>,
    },
    #[serde(rename = "price")]
    Price { currency: String },
    #[serde(rename = "return")]
    Return,
    #[serde(rename = "probability")]
    Probability,
    #[serde(rename = "other_regression")]
    OtherRegression,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
enum TargetWire {
    #[serde(rename = "fixed_horizon_event")]
    FixedHorizonEvent {
        horizon_nanos: u64,
        origin_basis: super::FixedHorizonOriginBasis,
        event: super::ProbabilityEventTarget,
    },
    #[serde(rename = "financial_period")]
    FinancialPeriod {
        cadence: FundamentalCadence,
        periods_ahead: std::num::NonZeroU16,
    },
    #[serde(rename = "not_applicable")]
    NotApplicable,
    #[serde(rename = "fixed_horizon_terminal")]
    FixedHorizonTerminal {
        horizon_nanos: u64,
        origin_basis: super::FixedHorizonOriginBasis,
    },
    #[serde(rename = "unsupported")]
    Unsupported,
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
#[serde(tag = "kind", rename_all = "snake_case")]
enum SplitPolicyWire {
    ExactTime {
        test_end_unix_nanos: i64,
        train_end_unix_nanos: i64,
        validation_end_unix_nanos: i64,
    },
    FiscalDates {
        test_end: CalendarDate,
        train_end: CalendarDate,
        validation_end: CalendarDate,
    },
}
impl SplitPolicyWire {
    fn new(value: super::ChronologicalSplitPolicy) -> Result<Self, DatasetBuildError> {
        if let Some([train, validation, test]) = value.timestamp_boundaries() {
            Ok(Self::ExactTime {
                train_end_unix_nanos: train.unix_nanos(),
                validation_end_unix_nanos: validation.unix_nanos(),
                test_end_unix_nanos: test.unix_nanos(),
            })
        } else if let Some([train_end, validation_end, test_end]) = value.fiscal_boundaries() {
            Ok(Self::FiscalDates {
                train_end,
                validation_end,
                test_end,
            })
        } else {
            Err(DatasetBuildError::ExportEncoding)
        }
    }
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

fn component_wire<'a>(
    spec: &'a FeatureLabelComponentSpec,
    bindings: &'a [FeatureLabelMeasurementBinding],
) -> ComponentWire<'a> {
    let binding = bindings.iter().find(|binding| binding.label() == spec);
    let measurement = binding.map(|binding| match binding.measurement() {
        FeatureLabelMeasurement::Price { currency } => MeasurementWire::Price {
            currency: currency.as_str().to_owned(),
        },
        FeatureLabelMeasurement::FinancialAmount {
            role,
            basis,
            currency,
            share_convention,
        } => MeasurementWire::FinancialAmount {
            role,
            basis,
            currency: currency.as_str().to_owned(),
            share_convention,
        },
        FeatureLabelMeasurement::Return => MeasurementWire::Return,
        FeatureLabelMeasurement::Probability => MeasurementWire::Probability,
        FeatureLabelMeasurement::OtherRegression => MeasurementWire::OtherRegression,
    });
    ComponentWire {
        corporate_action_sensitivity: match spec.corporate_actions() {
            CorporateActionSensitivity::NotApplicable => "not_applicable",
            CorporateActionSensitivity::RequiresAdjustment => "requires_adjustment",
        },
        kind: match spec.kind() {
            ComponentKind::Feature => "feature",
            ComponentKind::Label => "label",
        },
        measurement,
        name: spec.name(),
        scope: match spec.scope() {
            ComponentScope::Instrument => "instrument",
            ComponentScope::Account => "account",
            ComponentScope::Global => "global",
        },
        target: match (
            spec.kind(),
            binding.and_then(|value| value.target_horizon()),
        ) {
            (ComponentKind::Feature, _) => TargetWire::NotApplicable,
            (
                ComponentKind::Label,
                Some(super::DatasetTargetHorizon::FiscalPeriods {
                    cadence,
                    periods_ahead,
                }),
            ) => TargetWire::FinancialPeriod {
                cadence,
                periods_ahead,
            },
            (ComponentKind::Label, Some(super::DatasetTargetHorizon::ExactElapsed(horizon))) => {
                match binding.and_then(|value| value.fixed_horizon_origin_basis()) {
                    Some(origin_basis) => {
                        match binding.and_then(|value| value.probability_event_target()) {
                            Some(event) => TargetWire::FixedHorizonEvent {
                                horizon_nanos: horizon.as_nanos() as u64,
                                origin_basis,
                                event,
                            },
                            None => TargetWire::FixedHorizonTerminal {
                                horizon_nanos: horizon.as_nanos() as u64,
                                origin_basis,
                            },
                        }
                    }
                    None => TargetWire::Unsupported,
                }
            }
            (ComponentKind::Label, None) => TargetWire::Unsupported,
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
