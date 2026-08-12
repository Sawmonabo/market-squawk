//! Composition-sealed publication for the first closed feature-dataset recipe.

use std::cmp::Ordering;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MarketBarAdjustment, ResearchTemporalCoordinate,
    SourceIdentifier, Timestamp,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::admission::{
    self, FeatureDatasetProductionEvidenceBinding, FeatureDatasetProductionEvidenceV1,
};
use super::{
    ComponentAdjustmentEvidence, ComponentKind, ComponentScope, ComponentValue,
    CorporateActionSensitivity, DatasetBuildError, DatasetBuildRequest, FeatureLabelComponentSpec,
    FeatureLabelDataset, FeatureLabelMeasurement, MissingValuePolicy,
};
use crate::{
    AnalyticalDataService, CorporateActionAdjustment, DatasetBuildSpecDigest, ObservationFamilyKey,
    ResearchUse, Sha256Digest,
};

/// Exact product contract for the first code-owned price-return recipe.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FeatureDatasetProductContract {
    /// Feature and forward-return rows admitted only for local analysis and inference.
    PriceReturnFixedHorizonForwardReturnAnalysisV1,
    /// The same closed row recipe admitted only for model training.
    PriceReturnFixedHorizonForwardReturnTrainingV1,
}

impl FeatureDatasetProductContract {
    /// Returns the stable exact contract identity persisted with every admission.
    pub const fn identity(self) -> &'static str {
        match self {
            Self::PriceReturnFixedHorizonForwardReturnAnalysisV1 => {
                "market-squawk.feature-dataset.price-return-fixed-horizon-forward-return.analysis/v1"
            }
            Self::PriceReturnFixedHorizonForwardReturnTrainingV1 => {
                "market-squawk.feature-dataset.price-return-fixed-horizon-forward-return.training/v1"
            }
        }
    }

    /// Returns the sole independently authorized research use admitted by this contract.
    pub const fn required_use(self) -> ResearchUse {
        match self {
            Self::PriceReturnFixedHorizonForwardReturnAnalysisV1 => ResearchUse::LocalAnalysis,
            Self::PriceReturnFixedHorizonForwardReturnTrainingV1 => ResearchUse::Train,
        }
    }

    pub(crate) fn from_identity(value: &str) -> Option<Self> {
        match value {
            "market-squawk.feature-dataset.price-return-fixed-horizon-forward-return.analysis/v1" => {
                Some(Self::PriceReturnFixedHorizonForwardReturnAnalysisV1)
            }
            "market-squawk.feature-dataset.price-return-fixed-horizon-forward-return.training/v1" => {
                Some(Self::PriceReturnFixedHorizonForwardReturnTrainingV1)
            }
            _ => None,
        }
    }
}

/// Fixed-field application-producer attestation for one exact closed-recipe output.
///
/// This value carries no catalog authority. It records the exact authority and derivation digests
/// attested by the code-owned application producer; only the non-duplicable publisher issued by
/// analytical-service composition can turn it into a durable product admission.
#[derive(Debug, Eq, PartialEq)]
pub struct FeatureDatasetProductionProofV1 {
    build_spec: DatasetBuildSpecDigest,
    policy: Sha256Digest,
    universe: Sha256Digest,
    universe_membership_content: EvidenceDigest,
    universe_membership_audit: EvidenceDigest,
    instrument_population_query: EvidenceDigest,
    instrument_population_receipt: EvidenceDigest,
    completed_session_request: EvidenceDigest,
    completed_session_receipt: EvidenceDigest,
    completed_session_currentness: EvidenceDigest,
    feature_point_in_time_content: EvidenceDigest,
    feature_point_in_time_audit: EvidenceDigest,
    label_point_in_time_content: EvidenceDigest,
    label_point_in_time_audit: EvidenceDigest,
    return_kernel_output: EvidenceDigest,
    fixed_horizon_nanos: NonZeroU64,
    instrument_count: NonZeroU32,
    example_count: NonZeroU32,
    attested_at: Timestamp,
    currentness_expires_at: Timestamp,
}

impl FeatureDatasetProductionProofV1 {
    /// Constructs a bounded proof with no caller-selected producer, kind, schema, or revision.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent authority and derivation coordinates remain explicitly typed"
    )]
    pub fn try_new(
        build_spec: DatasetBuildSpecDigest,
        policy: Sha256Digest,
        universe: Sha256Digest,
        universe_membership_content: EvidenceDigest,
        universe_membership_audit: EvidenceDigest,
        instrument_population_query: EvidenceDigest,
        instrument_population_receipt: EvidenceDigest,
        completed_session_request: EvidenceDigest,
        completed_session_receipt: EvidenceDigest,
        completed_session_currentness: EvidenceDigest,
        feature_point_in_time_content: EvidenceDigest,
        feature_point_in_time_audit: EvidenceDigest,
        label_point_in_time_content: EvidenceDigest,
        label_point_in_time_audit: EvidenceDigest,
        return_kernel_output: EvidenceDigest,
        fixed_horizon_nanos: NonZeroU64,
        instrument_count: NonZeroU32,
        example_count: NonZeroU32,
        attested_at: Timestamp,
        currentness_expires_at: Timestamp,
    ) -> Result<Self, FeatureDatasetProductionError> {
        for evidence in [
            universe_membership_content,
            universe_membership_audit,
            instrument_population_query,
            instrument_population_receipt,
            completed_session_request,
            completed_session_receipt,
            completed_session_currentness,
            feature_point_in_time_content,
            feature_point_in_time_audit,
            label_point_in_time_content,
            label_point_in_time_audit,
            return_kernel_output,
        ] {
            require_evidence(evidence)?;
        }
        if build_spec.digest().bytes() == [0; 32]
            || policy.bytes() == [0; 32]
            || universe.bytes() == [0; 32]
            || attested_at >= currentness_expires_at
        {
            return Err(FeatureDatasetProductionError::InvalidProof);
        }
        Ok(Self {
            build_spec,
            policy,
            universe,
            universe_membership_content,
            universe_membership_audit,
            instrument_population_query,
            instrument_population_receipt,
            completed_session_request,
            completed_session_receipt,
            completed_session_currentness,
            feature_point_in_time_content,
            feature_point_in_time_audit,
            label_point_in_time_content,
            label_point_in_time_audit,
            return_kernel_output,
            fixed_horizon_nanos,
            instrument_count,
            example_count,
            attested_at,
            currentness_expires_at,
        })
    }
}

/// Whether one closed-recipe publication created or exactly replayed its atomic admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureDatasetProductionPublicationDisposition {
    /// The descriptor and canonical producer receipt were atomically published by this call.
    Published,
    /// The exact closed product identity was already retained and revalidated.
    Replay,
}

/// Complete result of one composition-authorized closed-recipe publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureDatasetProductionPublication {
    contract: FeatureDatasetProductContract,
    receipt: super::FeatureDatasetProductionReceiptV1,
    disposition: FeatureDatasetProductionPublicationDisposition,
}

impl FeatureDatasetProductionPublication {
    /// Returns the exact closed recipe and consumer-use contract.
    pub const fn contract(&self) -> FeatureDatasetProductContract {
        self.contract
    }

    /// Returns the immutable canonical receipt retained in the catalog transaction.
    pub const fn receipt(&self) -> &super::FeatureDatasetProductionReceiptV1 {
        &self.receipt
    }

    /// Returns whether this call published or exactly replayed the product.
    pub const fn disposition(&self) -> FeatureDatasetProductionPublicationDisposition {
        self.disposition
    }
}

/// Sole session-bound final publisher issued while composing one analytical service.
///
/// This type is intentionally not `Clone`, `Default`, or serializable. It has no public
/// constructor and no service/builder getter. Possession proves that root composition consumed the
/// exclusive pre-service [`crate::CatalogAuthority`]; the catalog's single-writer guard prevents a
/// second running composition from independently minting the same authority.
pub struct FeatureDatasetProductionPublisher {
    catalog_session: Uuid,
    _exclusive_composition_authority: Box<()>,
}

impl fmt::Debug for FeatureDatasetProductionPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FeatureDatasetProductionPublisher")
            .field("catalog_session", &"[SEALED CATALOG SESSION]")
            .field("authority", &"[EXCLUSIVE PRODUCTION PUBLISHER]")
            .finish()
    }
}

impl FeatureDatasetProductionPublisher {
    pub(crate) fn for_composition(catalog_session: Uuid) -> Self {
        Self {
            catalog_session,
            _exclusive_composition_authority: Box::new(()),
        }
    }

    /// Revalidates the exact request, closed recipe, typed proof, and catalog session before the
    /// internal atomic descriptor/receipt registration.
    ///
    /// An exact replay revalidates fresh catalog research/output authority but returns the
    /// immutable retained producer attestation, even after that attestation's original
    /// currentness window. A changed or freshly timestamped attestation is a different production
    /// identity and conflicts with an already admitted generation.
    pub fn publish(
        &self,
        service: &AnalyticalDataService,
        contract: FeatureDatasetProductContract,
        request: &DatasetBuildRequest,
        dataset: &FeatureLabelDataset,
        proof: FeatureDatasetProductionProofV1,
        cancellation: &CancellationToken,
    ) -> Result<FeatureDatasetProductionPublication, FeatureDatasetProductionError> {
        if service.catalog_session_id() != self.catalog_session {
            return Err(FeatureDatasetProductionError::CatalogSessionMismatch);
        }
        validate_closed_recipe(contract, request, dataset, &proof)?;
        let producer_evidence = producer_evidence(proof)?;
        let builder = service.dataset_builder();
        let admission = admission::register(
            &builder,
            self.catalog_session,
            contract,
            request,
            dataset,
            producer_evidence,
            cancellation,
        )?;
        let disposition = match admission.disposition() {
            admission::FeatureDatasetProductionAdmissionDisposition::Published => {
                FeatureDatasetProductionPublicationDisposition::Published
            }
            admission::FeatureDatasetProductionAdmissionDisposition::Replay => {
                FeatureDatasetProductionPublicationDisposition::Replay
            }
        };
        Ok(FeatureDatasetProductionPublication {
            contract,
            receipt: admission.into_receipt(),
            disposition,
        })
    }
}

/// One-time analytical-service composition result that transfers the sole publisher separately.
pub struct FeatureDatasetProductionComposition {
    service: AnalyticalDataService,
    publisher: FeatureDatasetProductionPublisher,
}

impl fmt::Debug for FeatureDatasetProductionComposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FeatureDatasetProductionComposition")
            .field("service", &self.service)
            .field("publisher", &self.publisher)
            .finish()
    }
}

impl FeatureDatasetProductionComposition {
    pub(crate) fn new(service: AnalyticalDataService) -> Self {
        let publisher =
            FeatureDatasetProductionPublisher::for_composition(service.catalog_session_id());
        Self { service, publisher }
    }

    /// Separates ordinary analytical services from the single code-owned publisher capability.
    pub fn into_parts(self) -> (AnalyticalDataService, FeatureDatasetProductionPublisher) {
        (self.service, self.publisher)
    }
}

/// Closed-recipe proof or authority failure.
#[derive(Debug, Error)]
pub enum FeatureDatasetProductionError {
    /// The typed producer proof is empty, expired, inconsistent, or outside the closed recipe.
    #[error("feature-dataset production proof is invalid")]
    InvalidProof,
    /// Request, values, selectors, policy, or use do not match the selected product contract.
    #[error("feature-dataset request does not match the closed product contract")]
    ContractMismatch,
    /// Publisher and analytical service do not belong to the same exclusive catalog session.
    #[error("feature-dataset publisher belongs to a different catalog session")]
    CatalogSessionMismatch,
    /// Final catalog registration or authority revalidation failed closed.
    #[error("feature-dataset publication failed: {0}")]
    Dataset(#[from] DatasetBuildError),
}

const FEATURE_COMPONENT_NAME: &str = "research.price-return";
const LABEL_COMPONENT_NAME: &str = "research.fixed-horizon-forward-return";
const RECIPE_IMPLEMENTATION_REVISION: &str = "price-return-fixed-horizon-forward-return-v1";
const PRODUCER_ID: &str = "market-squawk-application-feature-dataset-producer";

fn validate_closed_recipe(
    contract: FeatureDatasetProductContract,
    request: &DatasetBuildRequest,
    dataset: &FeatureLabelDataset,
    proof: &FeatureDatasetProductionProofV1,
) -> Result<(), FeatureDatasetProductionError> {
    let feature = expected_component(ComponentKind::Feature, FEATURE_COMPONENT_NAME)?;
    let label = expected_component(ComponentKind::Label, LABEL_COMPONENT_NAME)?;
    let expected_specs = [feature, label];
    let expected_policy = crate::CorporateActionPolicy::new(
        CorporateActionAdjustment::SplitAdjusted,
        NonZeroU32::MIN,
    );
    // Source families are deliberately raw. The producer derives returns only after applying the
    // exact split-adjustment plan whose content/audit identities are retained on each component;
    // accepting provider-adjusted families would make that basis ambiguous and risk applying it
    // twice.
    if request.intended_use() != contract.required_use()
        || request.inputs().component_specs() != expected_specs
        || dataset.component_specs.as_ref() != expected_specs
        || request.policy().corporate_actions() != expected_policy
        || request.policy().missing_values() != MissingValuePolicy::Reject
        || request.policy().implementation_revision().as_str() != RECIPE_IMPLEMENTATION_REVISION
        || proof.build_spec != request.build_spec_digest()
        || proof.build_spec != dataset.build_spec_digest()
        || proof.policy != request.policy_digest()
        || proof.policy != dataset.policy_digest()
        || proof.universe != request.universe_digest()
        || proof.universe != dataset.universe_digest()
        || usize::try_from(proof.example_count.get()).ok()
            != Some(request.inputs().examples().len())
        || dataset.label_measurements().len() != 1
        || dataset.label_measurements()[0].label() != &expected_specs[1]
        || dataset.label_measurements()[0].measurement() != FeatureLabelMeasurement::Return
        || dataset.label_measurements()[0].fixed_horizon_nanos() != Some(proof.fixed_horizon_nanos)
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }

    let mut instruments = Vec::new();
    instruments
        .try_reserve_exact(request.inputs().examples().len())
        .map_err(|_| FeatureDatasetProductionError::InvalidProof)?;
    for example in request.inputs().examples() {
        instruments.push(example.instrument_id());
        validate_example(example, expected_policy, proof.fixed_horizon_nanos)?;
    }
    instruments.sort_unstable();
    instruments.dedup();
    if usize::try_from(proof.instrument_count.get()).ok() != Some(instruments.len())
        || instruments.iter().any(|instrument| {
            !request
                .inputs()
                .universe_memberships()
                .iter()
                .any(|membership| membership.instrument_id() == *instrument)
        })
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    Ok(())
}

fn expected_component(
    kind: ComponentKind,
    name: &str,
) -> Result<FeatureLabelComponentSpec, FeatureDatasetProductionError> {
    FeatureLabelComponentSpec::try_new(
        kind,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        name,
        NonZeroU32::MIN,
    )
    .map_err(|_| FeatureDatasetProductionError::ContractMismatch)
}

fn validate_example(
    example: &super::DatasetExample,
    expected_policy: crate::CorporateActionPolicy,
    expected_horizon_nanos: NonZeroU64,
) -> Result<(), FeatureDatasetProductionError> {
    let [feature, label] = example.components() else {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    };
    validate_return_value(feature.value())?;
    validate_return_value(label.value())?;
    validate_adjustment(feature.adjustment(), expected_policy)?;
    validate_adjustment(label.adjustment(), expected_policy)?;
    let [feature_left, feature_right] = feature.selectors() else {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    };
    let [label_terminal] = label.selectors() else {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    };
    let feature_current = current_and_prior(
        feature_left.family(),
        feature_right.family(),
        example.effective_cutoff(),
    )?;
    if market_bar_effective(label_terminal.family())? != example.label_effective_cutoff()
        || !same_market_bar_series(feature_current, label_terminal.family())
        || fixed_horizon_nanos(example.effective_cutoff(), example.label_effective_cutoff())?
            != expected_horizon_nanos
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    Ok(())
}

fn validate_return_value(value: &ComponentValue) -> Result<(), FeatureDatasetProductionError> {
    let (unit, currency) = match value {
        ComponentValue::Float { unit, currency, .. }
        | ComponentValue::Decimal { unit, currency, .. } => (unit.as_ref(), currency.as_ref()),
        ComponentValue::Missing { .. } => {
            return Err(FeatureDatasetProductionError::ContractMismatch);
        }
    };
    if unit.map(SourceIdentifier::as_str) != Some(super::FEATURE_LABEL_RETURN_UNIT)
        || currency.is_some()
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    Ok(())
}

fn validate_adjustment(
    adjustment: &ComponentAdjustmentEvidence,
    expected_policy: crate::CorporateActionPolicy,
) -> Result<(), FeatureDatasetProductionError> {
    match adjustment {
        ComponentAdjustmentEvidence::Applied {
            policy,
            plan_content,
            plan_audit,
            implementation_evidence,
        } if *policy == expected_policy
            && plan_content.bytes() != [0; 32]
            && plan_audit.bytes() != [0; 32]
            && implementation_evidence.bytes() != [0; 32] =>
        {
            Ok(())
        }
        ComponentAdjustmentEvidence::Raw
        | ComponentAdjustmentEvidence::NotApplicable
        | ComponentAdjustmentEvidence::Applied { .. } => {
            Err(FeatureDatasetProductionError::ContractMismatch)
        }
    }
}

fn current_and_prior<'family>(
    left: &'family ObservationFamilyKey,
    right: &'family ObservationFamilyKey,
    current: &ResearchTemporalCoordinate,
) -> Result<&'family ObservationFamilyKey, FeatureDatasetProductionError> {
    if !same_market_bar_series(left, right) {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    let left_effective = market_bar_effective(left)?;
    let right_effective = market_bar_effective(right)?;
    match (
        left_effective == current,
        right_effective == current,
        left_effective.partial_cmp(current),
        right_effective.partial_cmp(current),
    ) {
        (true, false, _, Some(Ordering::Greater)) | (false, true, Some(Ordering::Greater), _) => {
            Err(FeatureDatasetProductionError::ContractMismatch)
        }
        (true, false, _, Some(Ordering::Less)) => Ok(left),
        (false, true, Some(Ordering::Less), _) => Ok(right),
        _ => Err(FeatureDatasetProductionError::ContractMismatch),
    }
}

fn fixed_horizon_nanos(
    current: &ResearchTemporalCoordinate,
    terminal: &ResearchTemporalCoordinate,
) -> Result<NonZeroU64, FeatureDatasetProductionError> {
    let current = current
        .exact_timestamp()
        .ok_or(FeatureDatasetProductionError::ContractMismatch)?;
    let terminal = terminal
        .exact_timestamp()
        .ok_or(FeatureDatasetProductionError::ContractMismatch)?;
    let nanos = terminal
        .unix_nanos()
        .checked_sub(current.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .and_then(NonZeroU64::new)
        .ok_or(FeatureDatasetProductionError::ContractMismatch)?;
    Ok(nanos)
}

fn market_bar_effective(
    family: &ObservationFamilyKey,
) -> Result<&ResearchTemporalCoordinate, FeatureDatasetProductionError> {
    match family {
        ObservationFamilyKey::MarketBar {
            adjustment: MarketBarAdjustment::Raw,
            effective,
            ..
        } => Ok(effective),
        _ => Err(FeatureDatasetProductionError::ContractMismatch),
    }
}

fn same_market_bar_series(left: &ObservationFamilyKey, right: &ObservationFamilyKey) -> bool {
    match (left, right) {
        (
            ObservationFamilyKey::MarketBar {
                source_id: left_source,
                instrument_id: left_instrument,
                venue_id: left_venue,
                provider_instrument_id: left_provider_instrument,
                feed: left_feed,
                interval: left_interval,
                adjustment: left_adjustment,
                timestamp_basis: left_timestamp_basis,
                session: left_session,
                ..
            },
            ObservationFamilyKey::MarketBar {
                source_id: right_source,
                instrument_id: right_instrument,
                venue_id: right_venue,
                provider_instrument_id: right_provider_instrument,
                feed: right_feed,
                interval: right_interval,
                adjustment: right_adjustment,
                timestamp_basis: right_timestamp_basis,
                session: right_session,
                ..
            },
        ) => {
            left_source == right_source
                && left_instrument == right_instrument
                && left_venue == right_venue
                && left_provider_instrument == right_provider_instrument
                && left_feed == right_feed
                && left_interval == right_interval
                && *left_adjustment == MarketBarAdjustment::Raw
                && left_adjustment == right_adjustment
                && left_timestamp_basis == right_timestamp_basis
                && left_session == right_session
        }
        _ => false,
    }
}

fn producer_evidence(
    proof: FeatureDatasetProductionProofV1,
) -> Result<FeatureDatasetProductionEvidenceV1, FeatureDatasetProductionError> {
    let mut bindings = Vec::new();
    bindings
        .try_reserve_exact(12)
        .map_err(|_| FeatureDatasetProductionError::InvalidProof)?;
    for (kind, evidence) in [
        (
            "universe-membership-content",
            proof.universe_membership_content,
        ),
        ("universe-membership-audit", proof.universe_membership_audit),
        (
            "instrument-population-query",
            proof.instrument_population_query,
        ),
        (
            "instrument-population-receipt",
            proof.instrument_population_receipt,
        ),
        ("completed-session-request", proof.completed_session_request),
        ("completed-session-receipt", proof.completed_session_receipt),
        (
            "completed-session-currentness",
            proof.completed_session_currentness,
        ),
        (
            "feature-point-in-time-content",
            proof.feature_point_in_time_content,
        ),
        (
            "feature-point-in-time-audit",
            proof.feature_point_in_time_audit,
        ),
        (
            "label-point-in-time-content",
            proof.label_point_in_time_content,
        ),
        ("label-point-in-time-audit", proof.label_point_in_time_audit),
        ("return-kernel-output", proof.return_kernel_output),
    ] {
        bindings.push(FeatureDatasetProductionEvidenceBinding::try_new(
            SourceIdentifier::try_from(kind)
                .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
            NonZeroU32::MIN,
            evidence,
        )?);
    }
    FeatureDatasetProductionEvidenceV1::try_new(
        SourceIdentifier::try_from(PRODUCER_ID)
            .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
        SourceIdentifier::try_from(RECIPE_IMPLEMENTATION_REVISION)
            .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
        proof.attested_at,
        proof.currentness_expires_at,
        bindings,
    )
    .map_err(Into::into)
}

fn require_evidence(evidence: EvidenceDigest) -> Result<(), FeatureDatasetProductionError> {
    if evidence.bytes() == [0; 32]
        || !matches!(
            evidence.algorithm(),
            DigestAlgorithm::Sha256 | DigestAlgorithm::Blake3
        )
    {
        Err(FeatureDatasetProductionError::InvalidProof)
    } else {
        Ok(())
    }
}

impl From<crate::PythonDatasetCatalogError> for FeatureDatasetProductionError {
    fn from(error: crate::PythonDatasetCatalogError) -> Self {
        Self::Dataset(DatasetBuildError::PythonDataset(error))
    }
}
