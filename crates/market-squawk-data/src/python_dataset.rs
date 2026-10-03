//! Read-only catalog verification and immutable selection receipts for Python research.

#[path = "python_dataset/descriptor.rs"]
mod descriptor;
#[path = "python_dataset/probability.rs"]
mod probability;
pub use probability::ProbabilityLabelObservation;
#[path = "python_dataset/verify.rs"]
mod verify;

use std::{num::NonZeroU64, time::Instant};

use market_squawk_domain::{
    Currency, InstrumentId, ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ArrowConversionError, CatalogEndpointIdentity, CatalogError, DatasetBuildSpecDigest,
    DatasetManifestRef, DatasetSplitCounts, FeatureDatasetProductContract,
    FeatureDatasetProductionReceiptV1, FeatureLabelComponentSpec, FeatureLabelMeasurement,
    FeatureLabelMeasurementBinding, Sha256Digest, UniverseId,
};

const MAX_PYTHON_DATASET_ROWS: usize = 100_000;
const MAX_PYTHON_DATASET_BYTES: usize = 256 * 1024 * 1024;

/// Durable Python-dataset registration or read-only verification failure.
#[derive(Debug, Error)]
pub enum PythonDatasetCatalogError {
    #[error("population source research use is unavailable: {0}")]
    PopulationResearchUse(#[source] Box<crate::ResearchUseCatalogError>),
    /// The requested export is absent from the selected catalog.
    #[error("Python dataset admission is unknown")]
    UnknownAdmission,
    /// Catalog, descriptor, generation, object, or selected-row identities disagree.
    #[error("Python dataset admission evidence is corrupt")]
    CorruptAdmission,
    /// Typed producer evidence was empty, repeated, reserved, or outside its closed bound.
    #[error("feature-dataset production evidence is invalid")]
    InvalidProductionEvidence,
    /// A generation was already paired with a different semantic production identity.
    #[error("feature-dataset production admission conflicts with retained evidence")]
    ConflictingProductionAdmission,
    /// Canonical receipt serialization failed or exceeded its fixed byte bound.
    #[error("feature-dataset production receipt could not be canonically encoded")]
    ProductionReceiptEncoding,
    /// Fresh ResearchUse authority expired before the final atomic admission transaction.
    #[error("feature-dataset production research authority expired before admission")]
    ResearchAuthorizationExpired,
    /// A caller-selected count, byte, or elapsed-time bound was exceeded.
    #[error("Python dataset verification limit was exceeded")]
    LimitExceeded,
    /// The caller cancelled verification.
    #[error("Python dataset verification was cancelled")]
    Cancelled,
    /// The caller-selected monotonic deadline elapsed.
    #[error("Python dataset verification deadline elapsed")]
    DeadlineExceeded,
    /// Local path authority rejected the configured catalog or artifact root.
    #[error("Python dataset local path authority failed: {0}")]
    Path(#[from] market_squawk_platform::PathError),
    /// A controlled artifact reference or open failed.
    #[error("Python dataset artifact authority failed: {0}")]
    Artifact(#[from] market_squawk_platform::ArtifactPathError),
    /// The hardened catalog rejected the operation.
    #[error("Python dataset catalog operation failed: {0}")]
    Catalog(#[from] CatalogError),
    /// SQLite rejected the bounded transaction or query.
    #[error("Python dataset SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// A controlled object read failed.
    #[error("Python dataset object read failed: {0}")]
    Io(#[from] std::io::Error),
    /// Parquet metadata or decoding failed.
    #[error("Python dataset Parquet decoding failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Registered Arrow validation failed.
    #[error("Python dataset Arrow validation failed: {0}")]
    Arrow(#[from] ArrowConversionError),
    /// Arrow decoding failed before registered-schema validation.
    #[error("Python dataset Arrow decoding failed: {0}")]
    ArrowDecode(#[from] arrow::error::ArrowError),
}

/// Explicit aggregate resource limits for one native dataset verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PythonDatasetVerificationLimits {
    max_rows: usize,
    max_bytes: usize,
}

impl PythonDatasetVerificationLimits {
    /// Constructs bounded selected-row and aggregate-memory limits.
    pub fn try_new(max_rows: usize, max_bytes: usize) -> Result<Self, PythonDatasetCatalogError> {
        if max_rows == 0
            || max_rows > MAX_PYTHON_DATASET_ROWS
            || max_bytes == 0
            || max_bytes > MAX_PYTHON_DATASET_BYTES
        {
            return Err(PythonDatasetCatalogError::LimitExceeded);
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }

    pub(crate) const fn max_rows(self) -> usize {
        self.max_rows
    }

    pub(crate) const fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

/// Exact value variant retained by one canonical feature/label row.
#[derive(Clone, Debug, PartialEq)]
pub enum PythonDatasetValue {
    /// Finite statistical floating-point value represented by exact IEEE bits.
    Float(f64),
    /// Exact decimal mantissa and nonnegative scale.
    Decimal { mantissa: i128, scale: u8 },
    /// Explicit bounded missing-value reason.
    Missing(Box<str>),
}

/// One canonical selected row accepted for opaque-receipt revalidation.
#[derive(Clone, Debug, PartialEq)]
pub struct PythonDatasetRow {
    example_id: Box<str>,
    instrument_id: [u8; 16],
    source_selection_as_of: Timestamp,
    label_selection_as_of: Option<Timestamp>,
    decision_coordinate: ResearchTemporalCoordinate,
    observed_effective_at: Option<Timestamp>,
    label_effective_at: Option<Timestamp>,
    target_coordinate_kind: u8,
    split: u8,
    component_kind: u8,
    component_name: Box<str>,
    component_version: u32,
    value: PythonDatasetValue,
    unit: Option<Box<str>>,
    currency: Option<Box<str>>,
    lineage: [u8; 32],
    input_epoch_json: Option<Box<[u8]>>,
}

impl PythonDatasetRow {
    /// Constructs one exact row after applying the same closed grammar as Task 11 publication.
    #[allow(
        clippy::too_many_arguments,
        reason = "each typed feature/label column remains an independently checked identity"
    )]
    pub fn try_new(
        example_id: &str,
        instrument_id: [u8; 16],
        source_selection_as_of: Timestamp,
        label_selection_as_of: Option<Timestamp>,
        decision_coordinate: ResearchTemporalCoordinate,
        observed_effective_at: Option<Timestamp>,
        label_effective_at: Option<Timestamp>,
        target_coordinate_kind: u8,
        split: u8,
        component_kind: u8,
        component_name: &str,
        component_version: u32,
        value: PythonDatasetValue,
        unit: Option<&str>,
        currency: Option<&str>,
        lineage: [u8; 32],
        input_epoch_json: Option<&[u8]>,
    ) -> Result<Self, PythonDatasetCatalogError> {
        let instrument = Uuid::from_bytes(instrument_id);
        let target_coordinates_valid = match (
            target_coordinate_kind,
            observed_effective_at,
            label_effective_at,
        ) {
            (1 | 3 | 5, Some(observed), Some(target)) => target > observed,
            (2 | 4, None, None) => true,
            _ => false,
        };
        match (target_coordinate_kind, input_epoch_json) {
            (3 | 4 | 5, Some(bytes)) => {
                let epoch = crate::FeatureDatasetInputEpoch::decode(bytes)
                    .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                epoch
                    .validate_label_selection(
                        label_selection_as_of,
                        component_kind,
                        matches!(&value, PythonDatasetValue::Missing(_)),
                    )
                    .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                if epoch.example_id() != example_id
                    || epoch.instrument_id().as_uuid().into_bytes() != instrument_id
                    || epoch.source_selection_as_of() != source_selection_as_of
                    || epoch.decision_coordinate() != &decision_coordinate
                    || epoch.target_origin() != observed_effective_at
                    || epoch.target_at() != label_effective_at
                    || (epoch.financial_period().is_some() != (target_coordinate_kind == 4))
                    || epoch.fixed_horizon_origin_basis()
                        != match target_coordinate_kind {
                            3 => Some(crate::FixedHorizonOriginBasis::CompletedBarClose),
                            5 => Some(
                                crate::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar,
                            ),
                            _ => None,
                        }
                    || (epoch.population_basis()
                        == crate::DatasetPopulationBasis::CurrentListedSnapshot
                        && split != 3)
                {
                    return Err(PythonDatasetCatalogError::CorruptAdmission);
                }
                if target_coordinate_kind == 4 {
                    let measurement = FeatureLabelMeasurement::try_from_parts(unit, currency)
                        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                    if epoch.financial_measurement() != Some(measurement)
                        || !matches!(&value, PythonDatasetValue::Decimal { .. })
                    {
                        return Err(PythonDatasetCatalogError::CorruptAdmission);
                    }
                    if component_kind == 1 {
                        let amount = epoch
                            .current_financial_amount()
                            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                        if !matches!(&value,PythonDatasetValue::Decimal {mantissa,scale} if *mantissa==amount.mantissa() && u32::from(*scale)==amount.scale())
                        {
                            return Err(PythonDatasetCatalogError::CorruptAdmission);
                        }
                    }
                }
            }
            (1 | 2, None)
                if decision_coordinate.exact_timestamp() == Some(source_selection_as_of)
                    && label_selection_as_of
                        .is_some_and(|label| label > source_selection_as_of) => {}
            _ => return Err(PythonDatasetCatalogError::CorruptAdmission),
        }
        let positive_currency_value = target_coordinate_kind == 4
            || currency.is_none()
            || match &value {
                PythonDatasetValue::Float(value) => *value > 0.0,
                PythonDatasetValue::Decimal { mantissa, .. } => *mantissa > 0,
                PythonDatasetValue::Missing(_) => false,
            };
        if !canonical_identifier(example_id, 256)
            || InstrumentId::try_from(instrument).is_err()
            || !target_coordinates_valid
            || !positive_currency_value
            || !matches!(split, 1..=3)
            || !matches!(component_kind, 1..=2)
            || !canonical_identifier(component_name, 256)
            || component_version == 0
            || lineage == [0; 32]
            || !unit.is_none_or(canonical_unit)
            || !currency.is_none_or(|value| {
                Currency::try_from(value).is_ok_and(|parsed| parsed.as_str() == value)
            })
            || !valid_value(&value)
            || (matches!(&value, PythonDatasetValue::Missing(_))
                && (unit.is_some() || currency.is_some()))
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        Ok(Self {
            example_id: example_id.into(),
            instrument_id,
            source_selection_as_of,
            label_selection_as_of,
            decision_coordinate,
            observed_effective_at,
            label_effective_at,
            target_coordinate_kind,
            split,
            component_kind,
            component_name: component_name.into(),
            component_version,
            value,
            unit: unit.map(Into::into),
            currency: currency.map(Into::into),
            lineage,
            input_epoch_json: input_epoch_json.map(Into::into),
        })
    }

    /// A label is selectable only after its independently retained acquisition cutoff.
    pub(crate) fn available_as_of(&self, as_of: Timestamp) -> bool {
        self.source_selection_as_of <= as_of
            && (self.component_kind != 2
                || self
                    .label_selection_as_of
                    .is_some_and(|known| known <= as_of))
    }

    fn target_horizon(
        &self,
    ) -> Result<Option<crate::DatasetTargetHorizon>, PythonDatasetCatalogError> {
        if self.target_coordinate_kind == 4 {
            let epoch = crate::FeatureDatasetInputEpoch::decode(
                self.input_epoch_json
                    .as_deref()
                    .ok_or(PythonDatasetCatalogError::CorruptAdmission)?,
            )
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
            Ok(Some(
                epoch
                    .study_policy()
                    .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?
                    .target_horizon(),
            ))
        } else {
            Ok(self.fixed_horizon_nanos().map(|v| {
                crate::DatasetTargetHorizon::ExactElapsed(std::time::Duration::from_nanos(v.get()))
            }))
        }
    }
    fn fixed_horizon_origin_basis(&self) -> Option<crate::FixedHorizonOriginBasis> {
        match self.target_coordinate_kind {
            1 => Some(crate::FixedHorizonOriginBasis::ExactEffectiveTimestamp),
            3 => Some(crate::FixedHorizonOriginBasis::CompletedBarClose),
            5 => Some(crate::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar),
            _ => None,
        }
    }

    fn fixed_horizon_nanos(&self) -> Option<NonZeroU64> {
        self.observed_effective_at
            .zip(self.label_effective_at)
            .and_then(|(observed, target)| target.unix_nanos().checked_sub(observed.unix_nanos()))
            .and_then(|value| u64::try_from(value).ok())
            .and_then(NonZeroU64::new)
    }
}

/// Native catalog/object proof for one exact point-in-time row selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonDatasetIdentity {
    manifest: DatasetManifestRef,
    build_spec_digest: DatasetBuildSpecDigest,
    universe_digest: Sha256Digest,
    policy_digest: Sha256Digest,
    universe_id: UniverseId,
    population_basis: crate::DatasetPopulationBasis,
    population_member_count: usize,
    population_unavailable: Box<[crate::CurrentPopulationInputUnavailable]>,
    population_partition: Option<crate::DatasetPopulationPartition>,
    population_source_use: Option<crate::DatasetPopulationSourceUse>,
    study_policy: Option<crate::DatasetStudyPolicy>,
    source_snapshot_digest: Option<Sha256Digest>,
    split_policy: crate::ChronologicalSplitPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PythonFeatureDatasetSummary {
    pub(crate) probability_event_target: Option<crate::ProbabilityEventTarget>,
    pub(crate) identity: PythonDatasetIdentity,
    pub(crate) split_counts: DatasetSplitCounts,
}

pub(crate) fn feature_dataset_summary(
    descriptor_bytes: &[u8],
    export_sha256: Sha256Digest,
) -> Result<PythonFeatureDatasetSummary, PythonDatasetCatalogError> {
    if descriptor_bytes.is_empty()
        || descriptor_bytes.len() > crate::MAX_FEATURE_LABEL_EXPORT_BYTES
        || Sha256Digest::new(Sha256::digest(descriptor_bytes).into()) != export_sha256
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let descriptor = descriptor::Descriptor::parse(descriptor_bytes)?;
    let identity = descriptor.identity()?;
    let split_counts = DatasetSplitCounts::from_parts(
        descriptor.split_counts.train,
        descriptor.split_counts.validation,
        descriptor.split_counts.test,
    );
    let mut probability_event_target = None;
    for component in &descriptor.components {
        if let Some(event) = component.probability_event_target()? {
            if probability_event_target.replace(event).is_some() {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
        }
    }
    Ok(PythonFeatureDatasetSummary {
        identity,
        split_counts,
        probability_event_target,
    })
}

impl PythonDatasetIdentity {
    /// Returns the source-owned population qualification for this publication.
    pub const fn population_basis(&self) -> crate::DatasetPopulationBasis {
        self.population_basis
    }
    /// Returns the complete admitted population count, including unavailable inputs.
    pub const fn population_member_count(&self) -> usize {
        self.population_member_count
    }
    /// Returns actual producer-unavailable inputs retained once for the dataset.
    pub fn population_unavailable(&self) -> &[crate::CurrentPopulationInputUnavailable] {
        &self.population_unavailable
    }
    /// Returns the exact bounded partition of the one source-admitted population.
    pub const fn population_partition(&self) -> Option<&crate::DatasetPopulationPartition> {
        self.population_partition.as_ref()
    }
    pub const fn population_source_use(&self) -> Option<&crate::DatasetPopulationSourceUse> {
        self.population_source_use.as_ref()
    }
    pub const fn study_policy(&self) -> Option<&crate::DatasetStudyPolicy> {
        self.study_policy.as_ref()
    }
    pub const fn source_snapshot_digest(&self) -> Option<Sha256Digest> {
        self.source_snapshot_digest
    }
    pub const fn split_policy(&self) -> crate::ChronologicalSplitPolicy {
        self.split_policy
    }

    /// Returns the exact registered feature/label generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the producer-owned complete build identity.
    pub const fn build_spec_digest(&self) -> DatasetBuildSpecDigest {
        self.build_spec_digest
    }

    /// Returns the historical-universe content identity.
    pub const fn universe_digest(&self) -> Sha256Digest {
        self.universe_digest
    }

    /// Returns the point-in-time and transformation-policy identity.
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    /// Returns the human-stable historical-universe identity.
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }
}

/// Native catalog/object proof for one exact point-in-time row selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonDatasetSelection {
    local_root: std::path::PathBuf,
    identity: PythonDatasetIdentity,
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    descriptor: Box<[u8]>,
    production_receipt: FeatureDatasetProductionReceiptV1,
    product_contract: FeatureDatasetProductContract,
    selection_sha256: Sha256Digest,
    selected_rows: usize,
    as_of: Timestamp,
    label_measurements: Box<[FeatureLabelMeasurementBinding]>,
    probability_observations: Option<probability::ProbabilityObservedDataset>,
}

impl PythonDatasetSelection {
    pub const fn population_basis(&self) -> crate::DatasetPopulationBasis {
        self.identity.population_basis()
    }
    pub const fn population_member_count(&self) -> usize {
        self.identity.population_member_count()
    }
    pub fn population_unavailable(&self) -> &[crate::CurrentPopulationInputUnavailable] {
        self.identity.population_unavailable()
    }
    pub const fn population_partition(&self) -> Option<&crate::DatasetPopulationPartition> {
        self.identity.population_partition()
    }
    pub const fn population_source_use(&self) -> Option<&crate::DatasetPopulationSourceUse> {
        self.identity.population_source_use()
    }
    pub const fn study_policy(&self) -> Option<&crate::DatasetStudyPolicy> {
        self.identity.study_policy()
    }
    pub const fn source_snapshot_digest(&self) -> Option<Sha256Digest> {
        self.identity.source_snapshot_digest()
    }
    pub const fn split_policy(&self) -> crate::ChronologicalSplitPolicy {
        self.identity.split_policy()
    }

    /// Returns the canonical local root derived by retained platform path authority.
    pub fn local_root(&self) -> &std::path::Path {
        &self.local_root
    }

    /// Returns the exact producer-owned generation and build identities.
    pub const fn identity(&self) -> &PythonDatasetIdentity {
        &self.identity
    }

    /// Returns the exact catalog endpoint selected by operator configuration.
    pub const fn catalog_identity(&self) -> CatalogEndpointIdentity {
        self.catalog_identity
    }

    /// Returns the producer-registered descriptor identity.
    pub const fn export_sha256(&self) -> Sha256Digest {
        self.export_sha256
    }

    /// Returns the exact producer-registered descriptor bytes.
    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Returns the immutable receipt required for product/model admission of this dataset.
    pub const fn production_receipt(&self) -> &FeatureDatasetProductionReceiptV1 {
        &self.production_receipt
    }

    /// Returns the exact closed recipe and independently authorized consumer use.
    pub const fn product_contract(&self) -> FeatureDatasetProductContract {
        self.product_contract
    }

    /// Returns the independently derived canonical selected-row identity.
    pub const fn selection_sha256(&self) -> Sha256Digest {
        self.selection_sha256
    }

    /// Original complete-case observations retained only after the same native scan verifies.
    pub fn probability_label_observations(
        &self,
        label: &FeatureLabelComponentSpec,
    ) -> Option<&[ProbabilityLabelObservation]> {
        self.probability_observations
            .as_ref()
            .filter(|value| &value.label == label)
            .map(|value| value.observations.as_ref())
    }

    /// Canonical feature order for the exact retained original observation vectors.
    pub fn probability_feature_specs(&self) -> &[FeatureLabelComponentSpec] {
        self.probability_observations
            .as_ref()
            .map_or(&[], |value| value.feature_specs.as_ref())
    }

    /// Exact closed event independently read from the original admitted target descriptor.
    pub fn label_probability_event_target(
        &self,
        label: &FeatureLabelComponentSpec,
    ) -> Option<crate::ProbabilityEventTarget> {
        self.label_measurements
            .iter()
            .find(|binding| binding.label() == label)
            .and_then(FeatureLabelMeasurementBinding::probability_event_target)
    }

    /// Returns the exact selected component-row count.
    pub const fn selected_rows(&self) -> usize {
        self.selected_rows
    }

    /// Returns the exact point-in-time cutoff bound into the selection digest.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the row-rederived measurement for one exact label after the descriptor and complete
    /// object scan agreed.
    #[must_use]
    pub fn label_measurement(
        &self,
        label: &FeatureLabelComponentSpec,
    ) -> Option<FeatureLabelMeasurement> {
        self.label_measurements
            .iter()
            .find(|binding| binding.label() == label)
            .map(FeatureLabelMeasurementBinding::measurement)
    }

    /// Returns the native row-rederived horizon for one admitted label.
    pub fn label_target_horizon(
        &self,
        label: &FeatureLabelComponentSpec,
    ) -> Option<crate::DatasetTargetHorizon> {
        self.label_measurements
            .iter()
            .find(|binding| binding.label() == label)
            .and_then(FeatureLabelMeasurementBinding::target_horizon)
    }

    /// Returns the exact row-rederived positive terminal offset for one admitted label.
    #[must_use]
    pub fn label_fixed_horizon_nanos(
        &self,
        label: &FeatureLabelComponentSpec,
    ) -> Option<NonZeroU64> {
        self.label_measurements
            .iter()
            .find(|binding| binding.label() == label)
            .and_then(FeatureLabelMeasurementBinding::fixed_horizon_nanos)
    }

    /// Returns the exact row-rederived origin basis for an admitted terminal label.
    pub fn label_fixed_horizon_origin_basis(
        &self,
        label: &FeatureLabelComponentSpec,
    ) -> Option<crate::FixedHorizonOriginBasis> {
        self.label_measurements
            .iter()
            .find(|binding| binding.label() == label)
            .and_then(FeatureLabelMeasurementBinding::fixed_horizon_origin_basis)
    }

    /// Starts a streaming rehash against this immutable receipt.
    pub fn revalidation(&self) -> PythonDatasetSelectionRevalidation {
        PythonDatasetSelectionRevalidation {
            hash: selection_hash_prefix(self.catalog_identity, self.export_sha256, self.as_of),
            expected: self.selection_sha256,
            expected_rows: self.selected_rows,
            rows: 0,
        }
    }
}

/// Streaming selected-row revalidation used immediately before training and export.
#[derive(Clone, Debug)]
pub struct PythonDatasetSelectionRevalidation {
    hash: Sha256,
    expected: Sha256Digest,
    expected_rows: usize,
    rows: usize,
}

impl PythonDatasetSelectionRevalidation {
    /// Adds one canonical row in retained object/batch/row order.
    pub fn update(&mut self, row: &PythonDatasetRow) -> Result<(), PythonDatasetCatalogError> {
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
        if self.rows > self.expected_rows {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        self.hash.update(row_digest(row));
        Ok(())
    }

    /// Proves exact row count and canonical identity equality.
    pub fn finish(mut self) -> Result<(), PythonDatasetCatalogError> {
        if self.rows != self.expected_rows {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        self.hash.update(
            u64::try_from(self.rows)
                .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?
                .to_be_bytes(),
        );
        if Sha256Digest::new(self.hash.finalize().into()) != self.expected {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        Ok(())
    }
}

/// Resolves and verifies one receipt-admitted export from an operator-selected local root.
pub fn verify_python_dataset(
    local_root: impl AsRef<std::path::Path>,
    export_sha256: Sha256Digest,
    expected_contract: FeatureDatasetProductContract,
    as_of: Timestamp,
    limits: PythonDatasetVerificationLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PythonDatasetSelection, PythonDatasetCatalogError> {
    verify::verify(
        local_root.as_ref(),
        export_sha256,
        expected_contract,
        as_of,
        limits,
        deadline,
        cancellation,
    )
}

pub(crate) fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PythonDatasetCatalogError> {
    if cancellation.is_cancelled() {
        Err(PythonDatasetCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(PythonDatasetCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn new_selection_hasher(
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    as_of: Timestamp,
) -> Sha256 {
    selection_hash_prefix(catalog_identity, export_sha256, as_of)
}

pub(crate) fn finish_selection_hash(
    mut hash: Sha256,
    selected_rows: usize,
) -> Result<Sha256Digest, PythonDatasetCatalogError> {
    hash.update(
        u64::try_from(selected_rows)
            .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?
            .to_be_bytes(),
    );
    Ok(Sha256Digest::new(hash.finalize().into()))
}

pub(crate) fn update_selection_hash(hash: &mut Sha256, row: &PythonDatasetRow) {
    hash.update(row_digest(row));
}

fn selection_hash_prefix(
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    as_of: Timestamp,
) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/python-dataset-selection/v2");
    hash.update(catalog_identity.bytes());
    hash.update(export_sha256.bytes());
    hash.update(as_of.unix_nanos().to_be_bytes());
    hash
}

fn row_digest(row: &PythonDatasetRow) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/python-dataset-row/v2");
    update_bytes(&mut hash, row.example_id.as_bytes());
    hash.update(row.instrument_id);
    hash.update(row.source_selection_as_of.unix_nanos().to_be_bytes());
    update_optional_timestamp(&mut hash, row.label_selection_as_of);
    if let Some(value) = row.decision_coordinate.exact_timestamp() {
        hash.update([1]);
        hash.update(value.unix_nanos().to_be_bytes());
    } else if let Some(value) = row.decision_coordinate.calendar_date_value() {
        hash.update([2]);
        hash.update(value.days_since_unix_epoch().to_be_bytes());
    }
    hash.update([row.target_coordinate_kind]);
    update_optional_timestamp(&mut hash, row.observed_effective_at);
    update_optional_timestamp(&mut hash, row.label_effective_at);
    hash.update([row.split, row.component_kind]);
    update_bytes(&mut hash, row.component_name.as_bytes());
    hash.update(row.component_version.to_be_bytes());
    match &row.value {
        PythonDatasetValue::Float(value) => {
            hash.update([1]);
            hash.update(value.to_bits().to_be_bytes());
        }
        PythonDatasetValue::Decimal { mantissa, scale } => {
            hash.update([2]);
            hash.update(mantissa.to_be_bytes());
            hash.update([*scale]);
        }
        PythonDatasetValue::Missing(reason) => {
            hash.update([3]);
            update_bytes(&mut hash, reason.as_bytes());
        }
    }
    update_optional(&mut hash, row.unit.as_deref());
    update_optional(&mut hash, row.currency.as_deref());
    hash.update(row.lineage);
    if let Some(bytes) = &row.input_epoch_json {
        hash.update([1]);
        update_bytes(&mut hash, bytes);
    } else {
        hash.update([0]);
    }
    hash.finalize().into()
}

fn update_optional_timestamp(hash: &mut Sha256, value: Option<Timestamp>) {
    if let Some(value) = value {
        hash.update([1]);
        hash.update(value.unix_nanos().to_be_bytes());
    } else {
        hash.update([0]);
    }
}

fn update_optional(hash: &mut Sha256, value: Option<&str>) {
    if let Some(value) = value {
        hash.update([1]);
        update_bytes(hash, value.as_bytes());
    } else {
        hash.update([0]);
    }
}

fn update_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value);
}

fn valid_value(value: &PythonDatasetValue) -> bool {
    match value {
        PythonDatasetValue::Float(value) => value.is_finite(),
        PythonDatasetValue::Decimal { scale, .. } => *scale <= 28,
        PythonDatasetValue::Missing(reason) => canonical_identifier(reason, 256),
    }
}

fn canonical_identifier(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn canonical_unit(value: &str) -> bool {
    SourceIdentifier::try_from(value).is_ok()
        && value.len() <= 32
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b'%')
        })
}

pub(crate) fn input_epoch_bytes(
    batch: &arrow::record_batch::RecordBatch,
    index: usize,
) -> Result<Option<&[u8]>, PythonDatasetCatalogError> {
    use arrow::array::Array as _;
    let values = batch
        .column_by_name("input_epoch_json")
        .and_then(|array| array.as_any().downcast_ref::<arrow::array::BinaryArray>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    Ok((!values.is_null(index)).then(|| values.value(index)))
}

/// Decode the two mutually exclusive physical columns without changing temporal precision.
pub(crate) fn decision_coordinate(
    batch: &arrow::record_batch::RecordBatch,
    index: usize,
) -> Result<ResearchTemporalCoordinate, PythonDatasetCatalogError> {
    use arrow::array::Array as _;
    let exact = batch
        .column_by_name("decision_at")
        .and_then(|a| {
            a.as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        })
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    let dates = batch
        .column_by_name("decision_on")
        .and_then(|a| a.as_any().downcast_ref::<arrow::array::Date32Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    match (exact.is_null(index), dates.is_null(index)) {
        (false, true) => Ok(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(exact.value(index)),
        )),
        (true, false) => {
            let epoch = crate::FeatureDatasetInputEpoch::decode(
                input_epoch_bytes(batch, index)?
                    .ok_or(PythonDatasetCatalogError::CorruptAdmission)?,
            )
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
            let date = epoch
                .decision_coordinate()
                .calendar_date_value()
                .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
            if date.days_since_unix_epoch() != dates.value(index) {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
            Ok(ResearchTemporalCoordinate::calendar_date(date))
        }
        _ => Err(PythonDatasetCatalogError::CorruptAdmission),
    }
}

pub(crate) fn canonical_row(
    batch: &arrow::record_batch::RecordBatch,
    index: usize,
) -> Result<PythonDatasetRow, PythonDatasetCatalogError> {
    verify::row(batch, index)
}
