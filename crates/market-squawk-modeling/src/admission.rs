//! Python-produced bundle authority verification shared by the validator and local application.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::str::FromStr;
use std::time::Instant;

use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies, FeatureRegistry,
    MissingValuePolicy, ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_data::{
    CatalogEndpointIdentity, ComponentKind, ComponentScope, CorporateActionSensitivity,
    FeatureDatasetProductContract, FeatureLabelComponentSpec, FeatureLabelMeasurement,
    PythonDatasetCatalogError, PythonDatasetSelection, PythonDatasetVerificationLimits,
    Sha256Digest, verify_python_dataset,
};
use market_squawk_domain::{Currency, ModelId, RoundingPolicy, Timestamp};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    BundleError, BundleExpectations, BundleId, BundleMetadataRef, ControlledModelRoot,
    ForecastCentralStatistic, ForecastEstimatorProfile, ForecastMeasurement, ForecastOutputBinding,
    ForecastTargetMeaning, ForecastTrainingObjective, ForecastTransform, ModelBundle,
    ModelOutputSemantics, TrainingDatasetIdentity, TrainingPeriod, VerifiedTrainingEnvironment,
};

/// Maximum exact independent authority-document bytes admitted before parsing.
pub const MAX_BUNDLE_AUTHORITY_BYTES: usize = 256 * 1024;
const FEATURE_IMPLEMENTATION_REVISION: &str = "task14-python-v1";

/// Exact catalog and point-in-time selection identity required by a Python model candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PythonDatasetAdmissionAuthority {
    export_sha256: Sha256Digest,
    as_of: Timestamp,
    selection_sha256: Sha256Digest,
    catalog_identity: CatalogEndpointIdentity,
}

impl PythonDatasetAdmissionAuthority {
    /// Constructs the exact dataset-selection authority carried by the Python receipt.
    ///
    /// # Errors
    ///
    /// Rejects reserved zero export or selection digests.
    pub fn try_new(
        export_sha256: Sha256Digest,
        as_of: Timestamp,
        selection_sha256: Sha256Digest,
        catalog_identity: CatalogEndpointIdentity,
    ) -> Result<Self, ModelAdmissionError> {
        if export_sha256.bytes() == [0; 32] || selection_sha256.bytes() == [0; 32] {
            return Err(ModelAdmissionError::InvalidDatasetAuthority);
        }
        Ok(Self {
            export_sha256,
            as_of,
            selection_sha256,
            catalog_identity,
        })
    }

    /// Returns the registered immutable export identity.
    #[must_use]
    pub const fn export_sha256(self) -> Sha256Digest {
        self.export_sha256
    }

    /// Returns the exact point-in-time selection cutoff.
    #[must_use]
    pub const fn as_of(self) -> Timestamp {
        self.as_of
    }

    /// Returns the independently derived selected-row identity.
    #[must_use]
    pub const fn selection_sha256(self) -> Sha256Digest {
        self.selection_sha256
    }

    /// Returns the selected hardened-catalog endpoint identity.
    #[must_use]
    pub const fn catalog_identity(self) -> CatalogEndpointIdentity {
        self.catalog_identity
    }

    fn verify(
        self,
        local_root: &Path,
        limits: PythonDatasetVerificationLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PythonDatasetSelection, ModelAdmissionError> {
        let selection = verify_python_dataset(
            local_root,
            self.export_sha256,
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1,
            self.as_of,
            limits,
            deadline,
            cancellation,
        )?;
        if selection.selection_sha256() != self.selection_sha256
            || selection.catalog_identity() != self.catalog_identity
        {
            return Err(ModelAdmissionError::InvalidDatasetAuthority);
        }
        Ok(selection)
    }
}

/// Exact bounded independent bundle-authority document retained for restart validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BundleAuthorityDocument {
    bytes: Box<[u8]>,
    sha256: Sha256Digest,
}

impl BundleAuthorityDocument {
    /// Returns the exact authority bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the exact authority-document digest.
    #[must_use]
    pub const fn sha256(&self) -> Sha256Digest {
        self.sha256
    }

    /// Consumes the document into its exact authority bytes.
    #[must_use]
    pub fn into_bytes(self) -> Box<[u8]> {
        self.bytes
    }
}

/// Code-owned feature registry required by every Python-produced bundle admission.
#[derive(Debug)]
pub struct ProductionFeatureRegistry {
    registry: FeatureRegistry,
}

impl ProductionFeatureRegistry {
    /// Constructs the complete versioned batch-feature registry used by Python training.
    ///
    /// # Errors
    ///
    /// Returns [`ModelAdmissionError::FeatureRegistry`] if the code-owned catalog cannot be
    /// constructed or registered within its fixed memory ceiling.
    pub fn try_new() -> Result<Self, ModelAdmissionError> {
        let config = BatchFeatureCatalogConfig::try_new(
            NonZeroU32::new(252).ok_or(ModelAdmissionError::FeatureRegistry)?,
            NonZeroU32::new(950_000).ok_or(ModelAdmissionError::FeatureRegistry)?,
            6,
            BatchFeaturePolicies::new(
                VarianceConvention::Sample,
                MissingValuePolicy::Reject,
                WeightPolicy::PositiveNormalized,
                RoundingPolicy::NearestEven,
                ShockComposition::Compounded,
            ),
        )
        .map_err(|_| ModelAdmissionError::FeatureRegistry)?;
        let catalog = BatchFeatureCatalog::try_new(config, FEATURE_IMPLEMENTATION_REVISION)
            .map_err(|_| ModelAdmissionError::FeatureRegistry)?;
        let mut registry = FeatureRegistry::try_new(
            BatchFeatureCatalog::minimum_registry_capacity(),
            NonZeroUsize::new(4 * 1024 * 1024).ok_or(ModelAdmissionError::FeatureRegistry)?,
        )
        .map_err(|_| ModelAdmissionError::FeatureRegistry)?;
        catalog
            .try_register(&mut registry)
            .map_err(|_| ModelAdmissionError::FeatureRegistry)?;
        Ok(Self { registry })
    }

    /// Returns immutable access to the exact code-owned feature registry.
    ///
    /// This grants neither metadata registration nor model admission authority.
    pub const fn feature_registry(&self) -> &FeatureRegistry {
        &self.registry
    }
}

/// One fully revalidated candidate plus its independent durable admission authority.
#[derive(Debug)]
pub struct ValidatedModelCandidate {
    bundle: ModelBundle,
    authority: BundleAuthorityDocument,
    dataset: PythonDatasetAdmissionAuthority,
}

impl ValidatedModelCandidate {
    /// Consumes the validation result into the immutable model bundle.
    #[must_use]
    pub fn into_bundle(self) -> ModelBundle {
        self.bundle
    }

    /// Returns the independent authority document.
    #[must_use]
    pub const fn authority(&self) -> &BundleAuthorityDocument {
        &self.authority
    }

    /// Returns the independently reverified dataset-selection authority.
    #[must_use]
    pub const fn dataset_authority(&self) -> PythonDatasetAdmissionAuthority {
        self.dataset
    }

    /// Splits the validation result into its runtime bundle and durable authorities.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ModelBundle,
        BundleAuthorityDocument,
        PythonDatasetAdmissionAuthority,
    ) {
        (self.bundle, self.authority, self.dataset)
    }
}

/// Revalidates a Python candidate against the currently verified training release.
#[allow(
    clippy::too_many_arguments,
    reason = "filesystem, dataset, training, cancellation, and deadline authorities remain explicit"
)]
pub fn verify_model_candidate(
    root: &ControlledModelRoot,
    metadata: &BundleMetadataRef,
    authority_bytes: &[u8],
    authority_sha256: Sha256Digest,
    dataset_root: &Path,
    dataset: PythonDatasetAdmissionAuthority,
    training_environment: &VerifiedTrainingEnvironment,
    feature_registry: &ProductionFeatureRegistry,
    dataset_limits: PythonDatasetVerificationLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<ValidatedModelCandidate, ModelAdmissionError> {
    let selection = dataset.verify(dataset_root, dataset_limits, deadline, cancellation)?;
    let (authority, expectations) = authority(
        authority_bytes,
        authority_sha256,
        &selection,
        Some(training_environment),
    )?;
    let bundle = ModelBundle::load(
        root,
        metadata,
        &expectations,
        feature_registry.feature_registry(),
    )?;
    verify_feature_order(bundle.metadata())?;
    Ok(ValidatedModelCandidate {
        bundle,
        authority,
        dataset,
    })
}

/// Revalidates a durably admitted candidate from its exact persisted authorities.
///
/// The training-environment identity is recovered from the two-copy authority document that was
/// accepted only after initial verification. The selected dataset is still independently
/// reverified from the current local catalog and artifacts on every restart.
#[allow(
    clippy::too_many_arguments,
    reason = "filesystem, dataset, cancellation, and deadline authorities remain explicit"
)]
pub fn recover_model_candidate(
    root: &ControlledModelRoot,
    metadata: &BundleMetadataRef,
    authority_bytes: &[u8],
    authority_sha256: Sha256Digest,
    dataset_root: &Path,
    dataset: PythonDatasetAdmissionAuthority,
    feature_registry: &ProductionFeatureRegistry,
    dataset_limits: PythonDatasetVerificationLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<ValidatedModelCandidate, ModelAdmissionError> {
    let selection = dataset.verify(dataset_root, dataset_limits, deadline, cancellation)?;
    let (authority, expectations) = authority(authority_bytes, authority_sha256, &selection, None)?;
    let bundle = ModelBundle::load(
        root,
        metadata,
        &expectations,
        feature_registry.feature_registry(),
    )?;
    verify_feature_order(bundle.metadata())?;
    Ok(ValidatedModelCandidate {
        bundle,
        authority,
        dataset,
    })
}

/// Returns whether model metadata names the single admitted V1 coefficient vector exactly.
#[must_use]
pub fn has_price_return_macro_context_feature_order_v1(metadata: &crate::ModelMetadata) -> bool {
    let contract =
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1;
    let macros = contract.macro_components();
    let Some((price_return, retained_macros)) = metadata.features().split_first() else {
        return false;
    };
    price_return.key().name() == "research.price-return"
        && price_return.key().version() == NonZeroU32::MIN
        && retained_macros.len() == macros.len()
        && retained_macros
            .iter()
            .zip(macros)
            .enumerate()
            .all(|(position, (binding, expected))| {
                usize::from(expected.position()) == position
                    && binding.key().name() == expected.component_name()
                    && binding.key().version() == NonZeroU32::MIN
            })
}

fn verify_feature_order(metadata: &crate::ModelMetadata) -> Result<(), ModelAdmissionError> {
    if has_price_return_macro_context_feature_order_v1(metadata) {
        Ok(())
    } else {
        Err(ModelAdmissionError::InvalidAuthority)
    }
}

fn authority(
    bytes: &[u8],
    expected_sha256: Sha256Digest,
    selection: &PythonDatasetSelection,
    environment: Option<&VerifiedTrainingEnvironment>,
) -> Result<(BundleAuthorityDocument, BundleExpectations), ModelAdmissionError> {
    if bytes.is_empty() || bytes.len() > MAX_BUNDLE_AUTHORITY_BYTES {
        return Err(ModelAdmissionError::InvalidAuthority);
    }
    let observed = Sha256Digest::new(Sha256::digest(bytes).into());
    if observed != expected_sha256 || observed.bytes() == [0; 32] {
        return Err(ModelAdmissionError::InvalidAuthority);
    }
    let wire: ExpectationsWire =
        serde_json::from_slice(bytes).map_err(|_| ModelAdmissionError::InvalidAuthority)?;
    let expectations = expectations(&wire, selection, environment)?;
    Ok((
        BundleAuthorityDocument {
            bytes: bytes.into(),
            sha256: observed,
        },
        expectations,
    ))
}

fn expectations(
    wire: &ExpectationsWire,
    selection: &PythonDatasetSelection,
    environment: Option<&VerifiedTrainingEnvironment>,
) -> Result<BundleExpectations, ModelAdmissionError> {
    let output_semantics = match wire.output_semantics.as_str() {
        "regression" => ModelOutputSemantics::Regression,
        "binary_probability" => ModelOutputSemantics::BinaryProbability,
        _ => return Err(ModelAdmissionError::InvalidAuthority),
    };
    if wire.schema_version != 8
        || wire.label.kind != "label"
        || !dataset_matches_selection(&wire.dataset, selection)?
        || wire.universe_id != selection.identity().universe_id().as_str()
    {
        return Err(ModelAdmissionError::InvalidAuthority);
    }
    let environment_hash = parse_hex(&wire.training_environment_sha256)?;
    if environment.is_some_and(|environment| {
        wire.training_code_revision != environment.training_code_revision()
            || environment_hash != environment.receipt_sha256()
    }) {
        return Err(ModelAdmissionError::InvalidAuthority);
    }
    let identity = selection.identity();
    let dataset = TrainingDatasetIdentity::try_new(
        identity.manifest().clone(),
        identity.build_spec_digest(),
        identity.universe_digest(),
        identity.policy_digest(),
        selection.catalog_identity(),
        selection.export_sha256(),
        selection.selection_sha256(),
        selection.as_of(),
        NonZeroU64::new(
            u64::try_from(selection.selected_rows())
                .map_err(|_| ModelAdmissionError::InvalidAuthority)?,
        )
        .ok_or(ModelAdmissionError::InvalidAuthority)?,
    )
    .map_err(|_| ModelAdmissionError::InvalidAuthority)?;
    let scope = match wire.label.scope.as_str() {
        "instrument" => ComponentScope::Instrument,
        "account" => ComponentScope::Account,
        "global" => ComponentScope::Global,
        _ => return Err(ModelAdmissionError::InvalidAuthority),
    };
    let actions = match wire.label.corporate_action_sensitivity.as_str() {
        "not_applicable" => CorporateActionSensitivity::NotApplicable,
        "requires_adjustment" => CorporateActionSensitivity::RequiresAdjustment,
        _ => return Err(ModelAdmissionError::InvalidAuthority),
    };
    let label = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        scope,
        actions,
        &wire.label.name,
        NonZeroU32::new(wire.label.version).ok_or(ModelAdmissionError::InvalidAuthority)?,
    )
    .map_err(|_| ModelAdmissionError::InvalidAuthority)?;
    let model_id =
        ModelId::from_str(&wire.model_id).map_err(|_| ModelAdmissionError::InvalidAuthority)?;
    let bundle_id =
        BundleId::try_new(&wire.bundle_id).map_err(|_| ModelAdmissionError::InvalidAuthority)?;
    let bundle_version =
        NonZeroU64::new(wire.bundle_version).ok_or(ModelAdmissionError::InvalidAuthority)?;
    let training_period = TrainingPeriod::try_new(
        Timestamp::from_unix_nanos(wire.training_period.start_unix_nanos),
        Timestamp::from_unix_nanos(wire.training_period.end_unix_nanos),
    )
    .map_err(|_| ModelAdmissionError::InvalidAuthority)?;
    let training_environment_hash = Sha256Digest::new(environment_hash);
    let bundle_metadata_hash = Sha256Digest::new(parse_hex(&wire.bundle_metadata_sha256)?);
    let artifact_hash = Sha256Digest::new(parse_hex(&wire.artifact_sha256)?);
    let training_run_hash = Sha256Digest::new(parse_hex(&wire.training_run_sha256)?);
    let output_binding = output_binding(
        &wire.output_measurement,
        &wire.output_statistic,
        output_semantics,
        &label,
        selection,
    )?;
    BundleExpectations::try_new_with_output_binding(
        model_id,
        bundle_id,
        bundle_version,
        dataset,
        identity.universe_id().clone(),
        training_period,
        label,
        &wire.training_code_revision,
        training_environment_hash,
        bundle_metadata_hash,
        artifact_hash,
        training_run_hash,
        output_binding,
    )
    .map_err(|_| ModelAdmissionError::InvalidAuthority)
}

fn output_binding(
    wire: &OutputMeasurementWire,
    statistic_wire: &OutputStatisticWire,
    output_semantics: ModelOutputSemantics,
    label: &FeatureLabelComponentSpec,
    selection: &PythonDatasetSelection,
) -> Result<ForecastOutputBinding, ModelAdmissionError> {
    let measurement = match wire {
        OutputMeasurementWire::Price { currency } => {
            let encoded = currency.as_str();
            let currency =
                Currency::try_from(encoded).map_err(|_| ModelAdmissionError::InvalidAuthority)?;
            if currency.as_str() != encoded {
                return Err(ModelAdmissionError::InvalidAuthority);
            }
            FeatureLabelMeasurement::Price { currency }
        }
        OutputMeasurementWire::Return => FeatureLabelMeasurement::Return,
        OutputMeasurementWire::Probability => FeatureLabelMeasurement::Probability,
        OutputMeasurementWire::OtherRegression => FeatureLabelMeasurement::OtherRegression,
    };
    if selection.label_measurement(label) != Some(measurement) {
        return Err(ModelAdmissionError::InvalidAuthority);
    }
    let measurement = match measurement {
        FeatureLabelMeasurement::Price { currency } => ForecastMeasurement::Price { currency },
        FeatureLabelMeasurement::Return => ForecastMeasurement::Return,
        FeatureLabelMeasurement::Probability => ForecastMeasurement::Probability,
        FeatureLabelMeasurement::OtherRegression => ForecastMeasurement::OtherRegression,
    };
    let expected_target = selection
        .label_fixed_horizon_nanos(label)
        .map_or(ForecastTargetMeaning::Unsupported, |horizon_nanos| {
            ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos }
        });
    let target = match statistic_wire.target {
        TargetWire::FixedHorizonTerminal { horizon_nanos } => {
            ForecastTargetMeaning::FixedHorizonTerminal {
                horizon_nanos: NonZeroU64::new(horizon_nanos)
                    .ok_or(ModelAdmissionError::InvalidAuthority)?,
            }
        }
        TargetWire::Unsupported => ForecastTargetMeaning::Unsupported,
    };
    if target != expected_target {
        return Err(ModelAdmissionError::InvalidAuthority);
    }
    let central_statistic = match statistic_wire.statistic.as_str() {
        "model_estimated_conditional_mean" => {
            ForecastCentralStatistic::ModelEstimatedConditionalMean
        }
        "unavailable" => ForecastCentralStatistic::Unavailable,
        _ => return Err(ModelAdmissionError::InvalidAuthority),
    };
    let target_transform = transform(&statistic_wire.target_transform)?;
    let output_transform = transform(&statistic_wire.output_transform)?;
    let objective = match statistic_wire.objective.as_str() {
        "squared_error" => ForecastTrainingObjective::SquaredError,
        "binary_cross_entropy" => ForecastTrainingObjective::BinaryCrossEntropy,
        _ => return Err(ModelAdmissionError::InvalidAuthority),
    };
    let estimator = match statistic_wire.estimator {
        EstimatorWire::SealedDirectLeastSquaresV1 => {
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1
        }
        EstimatorWire::SealedDirectRidgeV1 { ridge_alpha } => {
            if !ridge_alpha.is_finite() || ridge_alpha < 0.0 {
                return Err(ModelAdmissionError::InvalidAuthority);
            }
            ForecastEstimatorProfile::SealedDirectRidgeV1 {
                ridge_alpha_bits: ridge_alpha.to_bits(),
            }
        }
        EstimatorWire::SealedBinaryLogisticV1 => ForecastEstimatorProfile::SealedBinaryLogisticV1,
    };
    ForecastOutputBinding::try_from_admitted_model(
        output_semantics,
        measurement,
        central_statistic,
        target,
        target_transform,
        output_transform,
        objective,
        estimator,
        label.clone(),
    )
    .map_err(|_| ModelAdmissionError::InvalidAuthority)
}

fn transform(value: &str) -> Result<ForecastTransform, ModelAdmissionError> {
    match value {
        "identity" => Ok(ForecastTransform::Identity),
        "logistic" => Ok(ForecastTransform::Logistic),
        _ => Err(ModelAdmissionError::InvalidAuthority),
    }
}

fn dataset_matches_selection(
    wire: &DatasetWire,
    selection: &PythonDatasetSelection,
) -> Result<bool, ModelAdmissionError> {
    let identity = selection.identity();
    let manifest = identity.manifest();
    Ok(wire.dataset_id == manifest.dataset_id().as_str()
        && wire.manifest_version == manifest.manifest_version()
        && wire.schema_name == manifest.schema().name()
        && wire.schema_version == manifest.schema().version().get()
        && parse_hex(&wire.schema_sha256)? == manifest.schema().fingerprint()
        && parse_hex(&wire.manifest_sha256)? == manifest.content_hash().bytes()
        && parse_hex(&wire.build_spec_sha256)? == identity.build_spec_digest().digest().bytes()
        && parse_hex(&wire.universe_sha256)? == identity.universe_digest().bytes()
        && parse_hex(&wire.policy_sha256)? == identity.policy_digest().bytes()
        && parse_hex(&wire.catalog_identity_sha256)? == selection.catalog_identity().bytes()
        && parse_hex(&wire.export_sha256)? == selection.export_sha256().bytes()
        && parse_hex(&wire.selection_sha256)? == selection.selection_sha256().bytes()
        && wire.selection_as_of_unix_nanos == selection.as_of().unix_nanos()
        && wire.selected_component_rows
            == u64::try_from(selection.selected_rows())
                .map_err(|_| ModelAdmissionError::InvalidAuthority)?)
}

fn parse_hex(value: &str) -> Result<[u8; 32], ModelAdmissionError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ModelAdmissionError::InvalidAuthority);
    }
    let mut bytes = [0_u8; 32];
    for (target, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = nibble(pair[0]).ok_or(ModelAdmissionError::InvalidAuthority)?;
        let low = nibble(pair[1]).ok_or(ModelAdmissionError::InvalidAuthority)?;
        *target = (high << 4) | low;
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectationsWire {
    schema_version: u32,
    output_semantics: String,
    output_measurement: OutputMeasurementWire,
    output_statistic: OutputStatisticWire,
    model_id: String,
    bundle_id: String,
    bundle_version: u64,
    dataset: DatasetWire,
    universe_id: String,
    training_period: PeriodWire,
    label: LabelWire,
    training_code_revision: String,
    training_environment_sha256: String,
    bundle_metadata_sha256: String,
    artifact_sha256: String,
    training_run_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputStatisticWire {
    statistic: String,
    target: TargetWire,
    target_transform: String,
    output_transform: String,
    objective: String,
    estimator: EstimatorWire,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum TargetWire {
    #[serde(rename = "fixed_horizon_terminal")]
    FixedHorizonTerminal { horizon_nanos: u64 },
    #[serde(rename = "unsupported")]
    Unsupported,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum EstimatorWire {
    #[serde(rename = "sealed_direct_least_squares_v1")]
    SealedDirectLeastSquaresV1,
    #[serde(rename = "sealed_direct_ridge_v1")]
    SealedDirectRidgeV1 { ridge_alpha: f64 },
    #[serde(rename = "sealed_binary_logistic_v1")]
    SealedBinaryLogisticV1,
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum OutputMeasurementWire {
    #[serde(rename = "price")]
    Price { currency: String },
    #[serde(rename = "return")]
    Return,
    #[serde(rename = "probability")]
    Probability,
    #[serde(rename = "other_regression")]
    OtherRegression,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetWire {
    catalog_identity_sha256: String,
    dataset_id: String,
    export_sha256: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_sha256: String,
    manifest_sha256: String,
    build_spec_sha256: String,
    selected_component_rows: u64,
    selection_as_of_unix_nanos: i64,
    selection_sha256: String,
    universe_sha256: String,
    policy_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeriodWire {
    start_unix_nanos: i64,
    end_unix_nanos: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LabelWire {
    kind: String,
    scope: String,
    corporate_action_sensitivity: String,
    name: String,
    version: u32,
}

/// Python-candidate verification failed before registry or backend publication.
#[derive(Debug, Error)]
pub enum ModelAdmissionError {
    /// Dataset receipt identities were incomplete or disagreed with native catalog evidence.
    #[error("model dataset-selection authority is invalid")]
    InvalidDatasetAuthority,
    /// Independent expectation bytes, identities, or relationships were invalid.
    #[error("model bundle authority is invalid")]
    InvalidAuthority,
    /// Code-owned batch feature contracts could not be constructed.
    #[error("model feature registry is unavailable")]
    FeatureRegistry,
    /// Native point-in-time dataset verification failed.
    #[error("model dataset verification failed: {0}")]
    Dataset(#[from] PythonDatasetCatalogError),
    /// Exact bundle bytes or relationships failed closed.
    #[error("model bundle validation failed: {0}")]
    Bundle(#[from] BundleError),
}
