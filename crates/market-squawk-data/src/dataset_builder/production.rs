//! Composition-sealed publication for the first closed feature-dataset recipe.

use std::cmp::Ordering;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

pub use market_squawk_domain::FeatureDatasetMacroComponentDescriptor;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MarketBarAdjustment, ResearchTemporalCoordinate,
    SourceIdentifier, Timestamp, feature_dataset_macro_components_v1,
};
use sha2::{Digest as _, Sha256};
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
    AnalyticalDataService, CorporateActionAdjustment, DatasetBuildSpecDigest, DatasetManifestRef,
    DerivedGenerationParents, ObservationFamilyKey, ResearchUse, Sha256Digest,
};

/// Exact product contract for the code-owned price-return and Macro-context recipe.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FeatureDatasetProductContract {
    PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1,
    PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1,
    PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1,
    PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1,
    PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1,
    PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1,

    /// Price-return, neutral Macro, and forward-return rows for local analysis and inference.
    PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
    /// The same closed Macro-enriched row recipe admitted only for model training.
    PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1,
    /// Feature-only scoring origins with a predeclared target and no label observations.
    PriceReturnMacroContextFixedHorizonStudyInputsV1,
    FinancialAmountFiscalPeriodsTrainingV1,
    FinancialAmountFiscalPeriodsStudyInputsV1,
}

impl FeatureDatasetProductContract {
    /// Returns the stable exact contract identity persisted with every admission.
    pub const fn identity(self) -> &'static str {
        match self {
            Self::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-price-higher.training/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-price-higher.analysis/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-benchmark-outperformance.training/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-benchmark-outperformance.analysis/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-profit-after-costs.training/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-profit-after-costs.analysis/v1"
            }
            Self::FinancialAmountFiscalPeriodsTrainingV1 => {
                "market-squawk.feature-dataset.native-fiscal-financial-amount.training/v1"
            }
            Self::FinancialAmountFiscalPeriodsStudyInputsV1 => {
                "market-squawk.feature-dataset.native-fiscal-financial-amount.study-inputs/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonStudyInputsV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon.study-inputs/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.analysis/v1"
            }
            Self::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1 => {
                "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.training/v1"
            }
        }
    }

    /// Returns the sole independently authorized research use admitted by this contract.
    pub const fn required_use(self) -> ResearchUse {
        match self {
            Self::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1
            | Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1
            | Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1
            | Self::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1
            | Self::PriceReturnMacroContextFixedHorizonStudyInputsV1
            | Self::FinancialAmountFiscalPeriodsStudyInputsV1 => ResearchUse::LocalAnalysis,
            Self::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
            | Self::FinancialAmountFiscalPeriodsTrainingV1 => ResearchUse::Train,
        }
    }

    /// Returns the exact provider-neutral Macro component contract in economic curve order.
    ///
    /// This is the sole code-owned mapping shared by current Macro selection, immutable feature
    /// preparation, training, inference, and product-consumer validation.
    pub const fn macro_components(self) -> &'static [FeatureDatasetMacroComponentDescriptor] {
        if self.is_financial() {
            &[]
        } else {
            feature_dataset_macro_components_v1()
        }
    }

    /// Returns the exact instrument feature name admitted by the V1 recipe.
    pub const fn feature_component_name(self) -> &'static str {
        if self.is_financial() {
            super::financial::FINANCIAL_FEATURE
        } else {
            FEATURE_COMPONENT_NAME
        }
    }

    /// Returns the exact forward-return label name admitted by the V1 recipe.
    pub const fn label_component_name(self) -> &'static str {
        match self {
            Self::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1 => {
                "research.fixed-horizon-price-higher"
            }
            Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1 => {
                "research.fixed-horizon-benchmark-outperformance"
            }
            Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1 => {
                "research.fixed-horizon-profit-after-costs"
            }
            _ if self.is_financial() => super::financial::FINANCIAL_LABEL,
            _ => LABEL_COMPONENT_NAME,
        }
    }

    pub const fn is_probability(self) -> bool {
        matches!(
            self,
            Self::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1
                | Self::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1
                | Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1
                | Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1
                | Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1
                | Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1
        )
    }

    pub fn admits_probability_target(self, target: super::ProbabilityEventTarget) -> bool {
        self.is_probability() && self.label_component_name() == target.label_component_name()
    }

    /// Returns the exact producer implementation revision admitted by the V1 recipe.
    pub const fn implementation_revision(self) -> &'static str {
        match self {
            Self::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1 => {
                "price-return-macro-context-fixed-horizon-price-higher-v1"
            }
            Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1 => {
                "price-return-macro-context-fixed-horizon-benchmark-outperformance-v1"
            }
            Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1
            | Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1 => {
                "price-return-macro-context-fixed-horizon-profit-after-costs-v1"
            }
            Self::FinancialAmountFiscalPeriodsTrainingV1 => super::financial::FINANCIAL_RECIPE,
            Self::FinancialAmountFiscalPeriodsStudyInputsV1 => {
                super::financial::FINANCIAL_STUDY_RECIPE
            }
            Self::PriceReturnMacroContextFixedHorizonStudyInputsV1 => STUDY_IMPLEMENTATION_REVISION,
            _ => RECIPE_IMPLEMENTATION_REVISION,
        }
    }

    pub const fn purpose(self) -> super::DatasetBuildPurpose {
        match self {
            Self::FinancialAmountFiscalPeriodsStudyInputsV1
            | Self::PriceReturnMacroContextFixedHorizonStudyInputsV1 => {
                super::DatasetBuildPurpose::StudyInputs
            }
            _ => super::DatasetBuildPurpose::Training,
        }
    }

    pub const fn is_financial(self) -> bool {
        matches!(
            self,
            Self::FinancialAmountFiscalPeriodsTrainingV1
                | Self::FinancialAmountFiscalPeriodsStudyInputsV1
        )
    }

    pub fn from_identity(value: &str) -> Option<Self> {
        match value {
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-price-higher.training/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-price-higher.analysis/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-benchmark-outperformance.training/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-benchmark-outperformance.analysis/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-profit-after-costs.training/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-profit-after-costs.analysis/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1)
            }
            "market-squawk.feature-dataset.native-fiscal-financial-amount.training/v1" => {
                Some(Self::FinancialAmountFiscalPeriodsTrainingV1)
            }
            "market-squawk.feature-dataset.native-fiscal-financial-amount.study-inputs/v1" => {
                Some(Self::FinancialAmountFiscalPeriodsStudyInputsV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon.study-inputs/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonStudyInputsV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.analysis/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1)
            }
            "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.training/v1" => {
                Some(Self::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1)
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
struct CompletedCloseProductionProof {
    implementation_revision: SourceIdentifier,
    probability_event: Option<super::ProbabilityEventTarget>,
    probability_derivation_digest: Option<Sha256Digest>,
    probability_subject: Option<super::model::ProbabilitySubjectComposition>,
    study_policy: super::DatasetStudyPolicy,
    source_snapshot_digest: Sha256Digest,
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
    macro_context_evidence: EvidenceDigest,
    macro_parent_manifests: DerivedGenerationParents,
    macro_parent_set: EvidenceDigest,
    label_point_in_time_content: Option<EvidenceDigest>,
    label_point_in_time_audit: Option<EvidenceDigest>,
    return_kernel_output: EvidenceDigest,
    fixed_horizon_nanos: NonZeroU64,
    origin_basis: super::DatasetPriceInputOrigin,
    named_session_source_evidence: Option<EvidenceDigest>,
    instrument_count: NonZeroU32,
    example_count: NonZeroU32,
    attested_at: Timestamp,
    currentness_expires_at: Timestamp,
}

impl CompletedCloseProductionProof {
    /// Derives request-owned identities and cardinalities while accepting only exact external
    /// evidence produced by the application recipe.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent authority and derivation evidence remains explicitly typed"
    )]
    pub fn try_from_request_evidence(
        request: &DatasetBuildRequest,
        universe_membership_content: EvidenceDigest,
        universe_membership_audit: EvidenceDigest,
        instrument_population_query: EvidenceDigest,
        instrument_population_receipt: EvidenceDigest,
        completed_session_request: EvidenceDigest,
        completed_session_receipt: EvidenceDigest,
        completed_session_currentness: EvidenceDigest,
        feature_point_in_time_content: EvidenceDigest,
        feature_point_in_time_audit: EvidenceDigest,
        macro_context_evidence: EvidenceDigest,
        macro_parent_manifests: Vec<DatasetManifestRef>,
        label_point_in_time_content: Option<EvidenceDigest>,
        label_point_in_time_audit: Option<EvidenceDigest>,
        return_kernel_output: EvidenceDigest,
        attested_at: Timestamp,
        currentness_expires_at: Timestamp,
    ) -> Result<Self, FeatureDatasetProductionError> {
        let first = request
            .inputs()
            .examples()
            .first()
            .ok_or(FeatureDatasetProductionError::InvalidProof)?;
        let retained_fixed_horizon_nanos = example_horizon_nanos(first)?;
        if request.inputs().examples().iter().any(|example| {
            !example_horizon_nanos(example).is_ok_and(|value| value == retained_fixed_horizon_nanos)
        }) {
            return Err(FeatureDatasetProductionError::InvalidProof);
        }
        let (origin_basis, named_session_source_evidence) = request_origin_evidence(request)?;
        let mut instruments = request
            .inputs()
            .examples()
            .iter()
            .map(super::DatasetExample::instrument_id)
            .collect::<Vec<_>>();
        instruments.sort_unstable();
        instruments.dedup();
        let instrument_count = u32::try_from(instruments.len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(FeatureDatasetProductionError::InvalidProof)?;
        let example_count = u32::try_from(request.inputs().examples().len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(FeatureDatasetProductionError::InvalidProof)?;
        Self::try_new(
            request.policy().implementation_revision().clone(),
            probability_request_evidence(request)?,
            request.inputs().probability_subject(),
            *request
                .policy()
                .study_policy()
                .ok_or(FeatureDatasetProductionError::InvalidProof)?,
            super::canonical::source_snapshot_digest(request)
                .ok_or(FeatureDatasetProductionError::InvalidProof)?,
            request.build_spec_digest(),
            request.policy_digest(),
            request.universe_digest(),
            universe_membership_content,
            universe_membership_audit,
            instrument_population_query,
            instrument_population_receipt,
            completed_session_request,
            completed_session_receipt,
            completed_session_currentness,
            feature_point_in_time_content,
            feature_point_in_time_audit,
            macro_context_evidence,
            macro_parent_manifests,
            label_point_in_time_content,
            label_point_in_time_audit,
            return_kernel_output,
            retained_fixed_horizon_nanos,
            origin_basis,
            named_session_source_evidence,
            instrument_count,
            example_count,
            attested_at,
            currentness_expires_at,
        )
    }

    /// Constructs a bounded proof with no caller-selected producer, kind, schema, or revision.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent authority and derivation coordinates remain explicitly typed"
    )]
    fn try_new(
        implementation_revision: SourceIdentifier,
        probability_evidence: Option<(super::ProbabilityEventTarget, Sha256Digest)>,
        probability_subject: Option<super::model::ProbabilitySubjectComposition>,
        study_policy: super::DatasetStudyPolicy,
        source_snapshot_digest: Sha256Digest,
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
        macro_context_evidence: EvidenceDigest,
        macro_parent_manifests: Vec<DatasetManifestRef>,
        label_point_in_time_content: Option<EvidenceDigest>,
        label_point_in_time_audit: Option<EvidenceDigest>,
        return_kernel_output: EvidenceDigest,
        fixed_horizon_nanos: NonZeroU64,
        origin_basis: super::DatasetPriceInputOrigin,
        named_session_source_evidence: Option<EvidenceDigest>,
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
            macro_context_evidence,
            return_kernel_output,
        ] {
            require_evidence(evidence)?;
        }
        for evidence in [label_point_in_time_content, label_point_in_time_audit]
            .into_iter()
            .flatten()
        {
            require_evidence(evidence)?;
        }
        if source_snapshot_digest.bytes() == [0; 32]
            || match study_policy.purpose() {
                super::DatasetBuildPurpose::Training => {
                    label_point_in_time_content.is_none() || label_point_in_time_audit.is_none()
                }
                super::DatasetBuildPurpose::StudyInputs => {
                    label_point_in_time_content.is_some() || label_point_in_time_audit.is_some()
                }
            }
        {
            return Err(FeatureDatasetProductionError::InvalidProof);
        }
        let macro_parent_manifests = DerivedGenerationParents::try_new(macro_parent_manifests)
            .map_err(|_| FeatureDatasetProductionError::InvalidProof)?;
        let macro_parent_set = macro_parent_set_digest(macro_parent_manifests.as_slice());
        require_evidence(macro_parent_set)?;
        if build_spec.digest().bytes() == [0; 32]
            || policy.bytes() == [0; 32]
            || universe.bytes() == [0; 32]
            || attested_at >= currentness_expires_at
        {
            return Err(FeatureDatasetProductionError::InvalidProof);
        }
        Ok(Self {
            implementation_revision,
            probability_event: probability_evidence.map(|value| value.0),
            probability_derivation_digest: probability_evidence.map(|value| value.1),
            probability_subject,
            study_policy,
            source_snapshot_digest,
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
            macro_context_evidence,
            macro_parent_manifests,
            macro_parent_set,
            label_point_in_time_content,
            label_point_in_time_audit,
            return_kernel_output,
            fixed_horizon_nanos,
            origin_basis,
            named_session_source_evidence,
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
pub(super) const RECIPE_IMPLEMENTATION_REVISION: &str =
    "price-return-macro-context-fixed-horizon-forward-return-v1";
pub(super) const STUDY_IMPLEMENTATION_REVISION: &str =
    "price-return-macro-context-fixed-horizon-study-inputs-v1";
const PRODUCER_ID: &str = "market-squawk-application-feature-dataset-producer";
fn validate_closed_price_recipe(
    contract: FeatureDatasetProductContract,
    request: &DatasetBuildRequest,
    dataset: &FeatureLabelDataset,
    proof: &CompletedCloseProductionProof,
) -> Result<(), FeatureDatasetProductionError> {
    let (origin_basis, named_session_source_evidence) = request_origin_evidence(request)?;
    if proof.origin_basis != origin_basis
        || proof.named_session_source_evidence != named_session_source_evidence
        || dataset.price_input_origin() != Some(origin_basis)
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    let expected_specs = expected_components(contract)?;
    let study = request
        .policy()
        .study_policy()
        .ok_or(FeatureDatasetProductionError::ContractMismatch)?;
    if study.purpose() != contract.purpose()
        || *study != proof.study_policy
        || dataset.study_policy() != Some(study)
        || dataset.source_snapshot_digest() != Some(proof.source_snapshot_digest)
        || super::canonical::source_snapshot_digest(request) != Some(proof.source_snapshot_digest)
        || study.target_horizon().exact_elapsed().map(|v| v.as_nanos())
            != Some(u128::from(proof.fixed_horizon_nanos.get()))
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    let probability_evidence = probability_request_evidence(request)?;
    if proof.probability_subject != request.inputs().probability_subject()
        || proof.probability_subject.is_some_and(|subject| {
            contract
                != FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
                || probability_evidence.is_some()
                || subject.horizon_nanos != proof.fixed_horizon_nanos
                || request
                    .inputs()
                    .examples()
                    .iter()
                    .any(|example| example.instrument_id() != subject.subject)
        })
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    let preserve_missing_macro = contract.is_probability() || proof.probability_subject.is_some();
    if proof.implementation_revision.as_str() != contract.implementation_revision()
        || probability_evidence.map(|value| value.0) != proof.probability_event
        || probability_evidence.map(|value| value.1) != proof.probability_derivation_digest
        || match proof.probability_event {
            Some(target) => !contract.admits_probability_target(target),
            None => contract.is_probability(),
        }
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    let measurements_valid = match study.purpose() {
        super::DatasetBuildPurpose::StudyInputs => dataset.label_measurements().is_empty(),
        super::DatasetBuildPurpose::Training => {
            dataset.label_measurements().len() == 1
                && dataset.label_measurements()[0].label().name() == contract.label_component_name()
                && dataset.label_measurements()[0].measurement()
                    == if contract.is_probability() {
                        FeatureLabelMeasurement::Probability
                    } else {
                        FeatureLabelMeasurement::Return
                    }
                && dataset.label_measurements()[0].probability_event_target()
                    == proof.probability_event
                && dataset.label_measurements()[0].fixed_horizon_nanos()
                    == Some(proof.fixed_horizon_nanos)
                && dataset.label_measurements()[0].fixed_horizon_origin_basis()
                    == match proof.origin_basis {
                        super::DatasetPriceInputOrigin::CompletedBarClose => {
                            Some(super::FixedHorizonOriginBasis::CompletedBarClose)
                        }
                        super::DatasetPriceInputOrigin::NamedSessionCloseForNominalDailyBar => {
                            Some(
                                super::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar,
                            )
                        }
                        super::DatasetPriceInputOrigin::MixedCompletedAndNamedSessionCloses => None,
                    }
        }
    };
    let expected_policy = crate::CorporateActionPolicy::new(
        CorporateActionAdjustment::SplitAdjusted,
        NonZeroU32::MIN,
    );
    // Source families are deliberately raw. The producer derives returns only after applying the
    // exact split-adjustment plan whose content/audit identities are retained on each component;
    // accepting provider-adjusted families would make that basis ambiguous and risk applying it
    // twice.
    if request.intended_use() != contract.required_use()
        || request.inputs().component_specs() != expected_specs.as_slice()
        || dataset.component_specs.as_ref() != expected_specs.as_slice()
        || request.policy().corporate_actions() != expected_policy
        || request.policy().missing_values()
            != if preserve_missing_macro {
                MissingValuePolicy::Preserve
            } else {
                MissingValuePolicy::Reject
            }
        || request.policy().implementation_revision().as_str() != contract.implementation_revision()
        || proof.build_spec != request.build_spec_digest()
        || proof.build_spec != dataset.build_spec_digest()
        || proof.policy != request.policy_digest()
        || proof.policy != dataset.policy_digest()
        || proof.universe != request.universe_digest()
        || proof.universe != dataset.universe_digest()
        || proof
            .macro_parent_manifests
            .as_slice()
            .iter()
            .any(|parent| !request.inputs().parents().contains(parent))
        || usize::try_from(proof.example_count.get()).ok()
            != Some(request.inputs().examples().len())
        || !measurements_valid
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }

    let mut instruments = Vec::new();
    instruments
        .try_reserve_exact(request.inputs().examples().len())
        .map_err(|_| FeatureDatasetProductionError::InvalidProof)?;
    for example in request.inputs().examples() {
        instruments.push(example.instrument_id());
        validate_example(
            example,
            expected_policy,
            proof.fixed_horizon_nanos,
            study.purpose(),
            preserve_missing_macro,
        )?;
    }
    instruments.sort_unstable();
    instruments.dedup();
    if usize::try_from(proof.instrument_count.get()).ok() != Some(instruments.len())
        || instruments
            .iter()
            .any(|instrument| !request.inputs().population_contains(*instrument))
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    Ok(())
}

fn expected_component(
    kind: ComponentKind,
    scope: ComponentScope,
    corporate_actions: CorporateActionSensitivity,
    name: &str,
) -> Result<FeatureLabelComponentSpec, FeatureDatasetProductionError> {
    FeatureLabelComponentSpec::try_new(kind, scope, corporate_actions, name, NonZeroU32::MIN)
        .map_err(|_| FeatureDatasetProductionError::ContractMismatch)
}

fn expected_components(
    contract: FeatureDatasetProductContract,
) -> Result<Vec<FeatureLabelComponentSpec>, FeatureDatasetProductionError> {
    let mut components = Vec::new();
    components
        .try_reserve_exact(feature_dataset_macro_components_v1().len() + 2)
        .map_err(|_| FeatureDatasetProductionError::InvalidProof)?;
    components.push(expected_component(
        ComponentKind::Feature,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        FEATURE_COMPONENT_NAME,
    )?);
    for (position, definition) in feature_dataset_macro_components_v1().iter().enumerate() {
        if usize::from(definition.position()) != position {
            return Err(FeatureDatasetProductionError::ContractMismatch);
        }
        components.push(expected_component(
            ComponentKind::Feature,
            ComponentScope::Global,
            CorporateActionSensitivity::NotApplicable,
            definition.component_name(),
        )?);
    }
    if contract.purpose() == super::DatasetBuildPurpose::Training {
        components.push(expected_component(
            ComponentKind::Label,
            ComponentScope::Instrument,
            CorporateActionSensitivity::RequiresAdjustment,
            contract.label_component_name(),
        )?);
    }
    components.sort_unstable();
    if components.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    Ok(components)
}

fn validate_example(
    example: &super::DatasetExample,
    expected_policy: crate::CorporateActionPolicy,
    expected_horizon_nanos: NonZeroU64,
    purpose: super::DatasetBuildPurpose,
    preserve_missing_macro: bool,
) -> Result<(), FeatureDatasetProductionError> {
    let feature = component_by_name(example, FEATURE_COMPONENT_NAME)?;
    validate_return_value(feature.value())?;
    validate_adjustment(feature.adjustment(), expected_policy)?;
    if feature.selection_effective_cutoff() != example.effective_cutoff()
        || feature.label_selection_effective_cutoff().is_some()
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    let [left, right] = feature.selectors() else {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    };
    let current = current_and_prior(
        left.family(),
        right.family(),
        feature.selection_effective_cutoff(),
    )?;
    if example_horizon_nanos(example)? != expected_horizon_nanos {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    if purpose == super::DatasetBuildPurpose::Training {
        let label = if let Some(derivation) = example.probability_derivation() {
            derivation.validate(example)?;
            derivation.original_label()
        } else {
            component_by_name(example, LABEL_COMPONENT_NAME)?
        };
        validate_return_value(label.value())?;
        validate_adjustment(label.adjustment(), expected_policy)?;
        let [terminal] = label.selectors() else {
            return Err(FeatureDatasetProductionError::ContractMismatch);
        };
        let nominal_terminal = example
            .named_session_origin()
            .and_then(|origin| origin.target_native_date())
            .map(ResearchTemporalCoordinate::calendar_date);
        let terminal_cutoff = nominal_terminal
            .as_ref()
            .or_else(|| example.label_effective_cutoff());
        if label.selection_effective_cutoff() != example.effective_cutoff()
            || label.label_selection_effective_cutoff() != terminal_cutoff
            || market_bar_effective(terminal.family())?
                .partial_cmp(terminal_cutoff.ok_or(FeatureDatasetProductionError::InvalidProof)?)
                .is_none_or(|order| order == Ordering::Greater)
            || !same_market_bar_series(current, terminal.family())
        {
            return Err(FeatureDatasetProductionError::ContractMismatch);
        }
    }
    for definition in feature_dataset_macro_components_v1() {
        validate_macro_component(
            component_by_name(example, definition.component_name())?,
            *definition,
            preserve_missing_macro,
        )?;
    }
    Ok(())
}

fn component_by_name<'example>(
    example: &'example super::DatasetExample,
    name: &str,
) -> Result<&'example super::FeatureLabelComponentInput, FeatureDatasetProductionError> {
    let mut matching = example
        .components()
        .iter()
        .filter(|component| component.spec().name() == name);
    let component = matching
        .next()
        .ok_or(FeatureDatasetProductionError::ContractMismatch)?;
    if matching.next().is_some() {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    Ok(component)
}

fn validate_macro_component(
    component: &super::FeatureLabelComponentInput,
    definition: FeatureDatasetMacroComponentDescriptor,
    preserve_missing: bool,
) -> Result<(), FeatureDatasetProductionError> {
    match component.value() {
        ComponentValue::Decimal { unit, currency, .. }
            if unit.as_ref().map(SourceIdentifier::as_str) == Some(definition.unit())
                && currency.is_none() => {}
        ComponentValue::Missing { .. } if preserve_missing => {}
        _ => return Err(FeatureDatasetProductionError::ContractMismatch),
    }
    let [selector] = component.selectors() else {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    };
    let ObservationFamilyKey::Macro { effective, .. } = selector.family() else {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    };
    if effective != component.selection_effective_cutoff()
        || component.label_selection_effective_cutoff().is_some()
        || component.adjustment() != &ComponentAdjustmentEvidence::NotApplicable
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
    if left_effective
        .partial_cmp(current)
        .is_none_or(|order| order == Ordering::Greater)
        || right_effective
            .partial_cmp(current)
            .is_none_or(|order| order == Ordering::Greater)
    {
        return Err(FeatureDatasetProductionError::ContractMismatch);
    }
    match left_effective.partial_cmp(right_effective) {
        Some(Ordering::Greater) => Ok(left),
        Some(Ordering::Less) => Ok(right),
        _ => Err(FeatureDatasetProductionError::ContractMismatch),
    }
}

fn example_horizon_nanos(
    example: &super::DatasetExample,
) -> Result<NonZeroU64, FeatureDatasetProductionError> {
    let (current, terminal) = example
        .exact_target_coordinates()
        .ok_or(FeatureDatasetProductionError::ContractMismatch)?;
    terminal
        .unix_nanos()
        .checked_sub(current.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .and_then(NonZeroU64::new)
        .ok_or(FeatureDatasetProductionError::ContractMismatch)
}

fn request_origin_evidence(
    request: &DatasetBuildRequest,
) -> Result<(super::DatasetPriceInputOrigin, Option<EvidenceDigest>), FeatureDatasetProductionError>
{
    let examples = request.inputs().examples();
    let mask = examples.iter().fold(0, |mask, example| {
        mask | if example.nominal_daily_source().is_some() {
            2
        } else {
            1
        }
    });
    let basis = super::DatasetPriceInputOrigin::from_mask(mask)
        .ok_or(FeatureDatasetProductionError::InvalidProof)?;
    if mask == 1 {
        return Ok((basis, None));
    }
    let study = request
        .policy()
        .study_policy()
        .ok_or(FeatureDatasetProductionError::InvalidProof)?;
    let current = study.basis() == market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown
        && study.purpose() == super::DatasetBuildPurpose::StudyInputs;
    if !current
        && study.basis() != market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot
    {
        return Err(FeatureDatasetProductionError::InvalidProof);
    }
    if mask == 3
        && (!current
            || request.inputs().population_basis()
                != super::DatasetPopulationBasis::CurrentListedSnapshot)
    {
        return Err(FeatureDatasetProductionError::InvalidProof);
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/named-session-daily-production-sources/v2");
    hash.update([mask]);
    hash.update((examples.len() as u64).to_be_bytes());
    for example in examples {
        study.validate_example(example)?;
        hash_text(&mut hash, example.example_id());
        if let Some(source) = example.nominal_daily_source() {
            hash.update([2]);
            hash.update(source.origin.digest().bytes());
        } else {
            hash.update([1]);
        }
    }
    Ok((
        basis,
        Some(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            hash.finalize().into(),
        )),
    ))
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
                nominal_ruleset: left_nominal_ruleset,
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
                nominal_ruleset: right_nominal_ruleset,
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
                && left_nominal_ruleset == right_nominal_ruleset
        }
        _ => false,
    }
}

fn macro_parent_set_digest(parents: &[DatasetManifestRef]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/macro-context-parent-set/v1");
    hash.update((parents.len() as u128).to_be_bytes());
    for parent in parents {
        hash_text(&mut hash, parent.dataset_id().as_str());
        hash.update(parent.manifest_version().to_be_bytes());
        hash_text(&mut hash, parent.schema().name());
        hash.update(parent.schema().version().get().to_be_bytes());
        hash.update(parent.schema().fingerprint());
        hash.update(parent.content_hash().bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_text(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u128).to_be_bytes());
    hash.update(value.as_bytes());
}

fn price_producer_evidence(
    proof: CompletedCloseProductionProof,
) -> Result<FeatureDatasetProductionEvidenceV1, FeatureDatasetProductionError> {
    let mut bindings = Vec::new();
    bindings
        .try_reserve_exact(19)
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
        ("macro-context-evidence", proof.macro_context_evidence),
        ("macro-context-parent-set", proof.macro_parent_set),
        ("return-kernel-output", proof.return_kernel_output),
    ] {
        bindings.push(FeatureDatasetProductionEvidenceBinding::try_new(
            SourceIdentifier::try_from(kind)
                .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
            NonZeroU32::MIN,
            evidence,
        )?);
    }
    for (kind, evidence) in [
        (
            "label-point-in-time-content",
            proof.label_point_in_time_content,
        ),
        ("label-point-in-time-audit", proof.label_point_in_time_audit),
        (
            "named-session-daily-source-associations",
            proof.named_session_source_evidence,
        ),
        (
            "probability-event-original-derivation",
            proof
                .probability_derivation_digest
                .map(|value| EvidenceDigest::new(DigestAlgorithm::Sha256, value.bytes())),
        ),
        (
            "probability-subject-composition",
            proof.probability_subject.map(|subject| {
                let mut hash = Sha256::new();
                hash.update(b"market-squawk/probability-subject-composition/v1\0");
                hash.update(subject.event.digest().bytes());
                hash.update(subject.subject.as_uuid().as_bytes());
                hash.update(subject.horizon_nanos.get().to_be_bytes());
                hash.update(proof.build_spec.digest().bytes());
                hash.update(proof.source_snapshot_digest.bytes());
                EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
            }),
        ),
        (
            "historical-study-policy",
            Some(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                super::canonical::study_policy_digest(&proof.study_policy).bytes(),
            )),
        ),
        (
            "source-snapshot",
            Some(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                proof.source_snapshot_digest.bytes(),
            )),
        ),
    ] {
        if let Some(evidence) = evidence {
            bindings.push(FeatureDatasetProductionEvidenceBinding::try_new(
                SourceIdentifier::try_from(kind)
                    .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
                NonZeroU32::MIN,
                evidence,
            )?);
        }
    }
    FeatureDatasetProductionEvidenceV1::try_new(
        SourceIdentifier::try_from(PRODUCER_ID)
            .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
        proof.implementation_revision,
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

/// Closed producer proof for the actual source family; a request alone grants no publication right.
#[derive(Debug, Eq, PartialEq)]
pub struct FeatureDatasetProductionProofV1 {
    source: ProductionProofSource,
}
#[derive(Debug, Eq, PartialEq)]
enum ProductionProofSource {
    CompletedBarClose(CompletedCloseProductionProof),
    FinancialPeriod(FinancialProductionProof),
}
#[derive(Debug, Eq, PartialEq)]
struct FinancialProductionProof {
    study: super::DatasetStudyPolicy,
    source_snapshot: Sha256Digest,
    build: DatasetBuildSpecDigest,
    policy: Sha256Digest,
    universe: Sha256Digest,
    source_evidence: Sha256Digest,
    attested_at: Timestamp,
    expires_at: Timestamp,
}
impl FeatureDatasetProductionProofV1 {
    #[allow(
        clippy::too_many_arguments,
        reason = "independent source authority receipts remain explicit"
    )]
    pub fn try_from_request_evidence(
        request: &DatasetBuildRequest,
        universe_membership_content: EvidenceDigest,
        universe_membership_audit: EvidenceDigest,
        instrument_population_query: EvidenceDigest,
        instrument_population_receipt: EvidenceDigest,
        completed_session_request: EvidenceDigest,
        completed_session_receipt: EvidenceDigest,
        completed_session_currentness: EvidenceDigest,
        feature_point_in_time_content: EvidenceDigest,
        feature_point_in_time_audit: EvidenceDigest,
        macro_context_evidence: EvidenceDigest,
        macro_parent_manifests: Vec<DatasetManifestRef>,
        label_point_in_time_content: Option<EvidenceDigest>,
        label_point_in_time_audit: Option<EvidenceDigest>,
        return_kernel_output: EvidenceDigest,
        attested_at: Timestamp,
        currentness_expires_at: Timestamp,
    ) -> Result<Self, FeatureDatasetProductionError> {
        Ok(Self {
            source: ProductionProofSource::CompletedBarClose(
                CompletedCloseProductionProof::try_from_request_evidence(
                    request,
                    universe_membership_content,
                    universe_membership_audit,
                    instrument_population_query,
                    instrument_population_receipt,
                    completed_session_request,
                    completed_session_receipt,
                    completed_session_currentness,
                    feature_point_in_time_content,
                    feature_point_in_time_audit,
                    macro_context_evidence,
                    macro_parent_manifests,
                    label_point_in_time_content,
                    label_point_in_time_audit,
                    return_kernel_output,
                    attested_at,
                    currentness_expires_at,
                )?,
            ),
        })
    }

    pub fn try_from_financial_request(
        request: &DatasetBuildRequest,
        attested_at: Timestamp,
        currentness_expires_at: Timestamp,
    ) -> Result<Self, FeatureDatasetProductionError> {
        let study = *request
            .policy()
            .study_policy()
            .ok_or(FeatureDatasetProductionError::InvalidProof)?;
        if !matches!(
            study.target_horizon(),
            super::DatasetTargetHorizon::FiscalPeriods { .. }
        ) || attested_at < study.snapshot_as_of()
            || currentness_expires_at <= attested_at
        {
            return Err(FeatureDatasetProductionError::InvalidProof);
        }
        Ok(Self {
            source: ProductionProofSource::FinancialPeriod(FinancialProductionProof {
                study,
                source_snapshot: super::canonical::source_snapshot_digest(request)
                    .ok_or(FeatureDatasetProductionError::InvalidProof)?,
                build: request.build_spec_digest(),
                policy: request.policy_digest(),
                universe: request.universe_digest(),
                source_evidence: financial_request_evidence(request)?,
                attested_at,
                expires_at: currentness_expires_at,
            }),
        })
    }
}
fn financial_request_evidence(
    request: &DatasetBuildRequest,
) -> Result<Sha256Digest, FeatureDatasetProductionError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/native-fiscal-production-sources/v1");
    for example in request.inputs().examples() {
        let source = example
            .financial_source()
            .ok_or(FeatureDatasetProductionError::InvalidProof)?;
        source.binding.validate()?;
        hash.update(source.binding.source_selection_digest().bytes());
        hash.update(source.binding.identity_receipt_digest().bytes());
        hash.update(source.binding.observed_ordinal().to_be_bytes());
        hash.update(source.binding.target_ordinal().to_be_bytes());
        for row in source.binding.duration_chain() {
            hash.update(row.canonical_row_digest().bytes());
        }
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}
fn validate_closed_recipe(
    contract: FeatureDatasetProductContract,
    request: &DatasetBuildRequest,
    dataset: &FeatureLabelDataset,
    proof: &FeatureDatasetProductionProofV1,
) -> Result<(), FeatureDatasetProductionError> {
    match &proof.source {
        ProductionProofSource::CompletedBarClose(proof) if !contract.is_financial() => {
            validate_closed_price_recipe(contract, request, dataset, proof)
        }
        ProductionProofSource::FinancialPeriod(proof) if contract.is_financial() => {
            let study = request
                .policy()
                .study_policy()
                .ok_or(FeatureDatasetProductionError::ContractMismatch)?;
            let mut specs = vec![super::financial::component_spec(ComponentKind::Feature)?];
            if contract.purpose() == super::DatasetBuildPurpose::Training {
                specs.push(super::financial::component_spec(ComponentKind::Label)?);
            }
            specs.sort_unstable();
            if *study != proof.study
                || study.purpose() != contract.purpose()
                || dataset.study_policy() != Some(study)
                || super::canonical::source_snapshot_digest(request) != Some(proof.source_snapshot)
                || dataset.source_snapshot_digest() != Some(proof.source_snapshot)
                || request.build_spec_digest() != proof.build
                || dataset.build_spec_digest() != proof.build
                || request.policy_digest() != proof.policy
                || dataset.policy_digest() != proof.policy
                || request.universe_digest() != proof.universe
                || dataset.universe_digest() != proof.universe
                || request.intended_use() != contract.required_use()
                || request.inputs().component_specs() != specs.as_slice()
                || dataset.component_specs.as_ref() != specs.as_slice()
                || request.policy().corporate_actions().adjustment()
                    != CorporateActionAdjustment::Raw
                || request.policy().missing_values() != MissingValuePolicy::Reject
                || request.policy().implementation_revision().as_str()
                    != contract.implementation_revision()
                || financial_request_evidence(request)? != proof.source_evidence
            {
                return Err(FeatureDatasetProductionError::ContractMismatch);
            }
            let mut measurement = None;
            for example in request.inputs().examples() {
                study.validate_example(example)?;
                let source = example
                    .financial_source()
                    .ok_or(FeatureDatasetProductionError::InvalidProof)?;
                if measurement.is_some_and(|v| v != source.source.measurement) {
                    return Err(FeatureDatasetProductionError::ContractMismatch);
                }
                measurement = Some(source.source.measurement);
            }
            if contract.purpose() == super::DatasetBuildPurpose::StudyInputs {
                if !dataset.label_measurements().is_empty() {
                    return Err(FeatureDatasetProductionError::ContractMismatch);
                }
            } else if dataset.label_measurements().len() != 1
                || Some(dataset.label_measurements()[0].measurement()) != measurement
                || dataset.label_measurements()[0].target_horizon() != Some(study.target_horizon())
            {
                return Err(FeatureDatasetProductionError::ContractMismatch);
            }
            Ok(())
        }
        _ => Err(FeatureDatasetProductionError::ContractMismatch),
    }
}
fn producer_evidence(
    proof: FeatureDatasetProductionProofV1,
) -> Result<FeatureDatasetProductionEvidenceV1, FeatureDatasetProductionError> {
    match proof.source {
        ProductionProofSource::CompletedBarClose(value) => price_producer_evidence(value),
        ProductionProofSource::FinancialPeriod(value) => {
            let mut bindings = Vec::new();
            bindings
                .try_reserve_exact(3)
                .map_err(|_| FeatureDatasetProductionError::InvalidProof)?;
            for (kind, digest) in [
                ("native-fiscal-source-selection", value.source_evidence),
                (
                    "historical-study-policy",
                    super::canonical::study_policy_digest(&value.study),
                ),
                ("source-snapshot", value.source_snapshot),
            ] {
                bindings.push(FeatureDatasetProductionEvidenceBinding::try_new(
                    SourceIdentifier::try_from(kind)
                        .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
                    NonZeroU32::MIN,
                    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.bytes()),
                )?);
            }
            FeatureDatasetProductionEvidenceV1::try_new(
                SourceIdentifier::try_from(PRODUCER_ID)
                    .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
                SourceIdentifier::try_from(
                    if value.study.purpose() == super::DatasetBuildPurpose::StudyInputs {
                        super::financial::FINANCIAL_STUDY_RECIPE
                    } else {
                        super::financial::FINANCIAL_RECIPE
                    },
                )
                .map_err(|_| FeatureDatasetProductionError::InvalidProof)?,
                value.attested_at,
                value.expires_at,
                bindings,
            )
            .map_err(Into::into)
        }
    }
}

fn probability_request_evidence(
    request: &DatasetBuildRequest,
) -> Result<Option<(super::ProbabilityEventTarget, Sha256Digest)>, FeatureDatasetProductionError> {
    let examples = request.inputs().examples();
    let Some(first) = examples.first() else {
        return Err(FeatureDatasetProductionError::InvalidProof);
    };
    let target = first.probability_event_target();
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/probability-production-original-derivations/v1\0");
    for example in examples {
        if example.probability_event_target() != target {
            return Err(FeatureDatasetProductionError::ContractMismatch);
        }
        if let Some(derivation) = example.probability_derivation() {
            derivation.validate(example)?;
            hash_text(&mut hash, example.example_id());
            hash.update(example.instrument_id().as_uuid().as_bytes());
            hash.update(derivation.digest().bytes());
        }
    }
    Ok(target.map(|value| (value, Sha256Digest::new(hash.finalize().into()))))
}
