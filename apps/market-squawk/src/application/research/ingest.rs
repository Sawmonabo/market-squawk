//! Registry-authorized extraction and rights-bound analytical publication.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use market_squawk_data::{
    CommittedDataset, DatasetId, DatasetManifestRef, IngestError, IngestPrecommitAuthority,
    PersistedProviderCaptureBindingEvidence, RightsBasis, RightsDecisionInput, SourceOperation,
    extraction_provider_payload_digest,
};
use market_squawk_domain::{
    CompanyIdentityObservation, EvidenceDigest, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::SealedResearchJournalStore;
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority,
    ExtractionAuthorityError, ExtractionBatch, ExtractionRequest, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, MAX_DISCOVERY_OBJECTS, MAX_EXTRACTION_RECORDS,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCaptureSealRequest, ProviderNativeLineageBatch, ProviderNativeLineageImplementation,
    RegisteredSource, RegistryError, SealedProviderCaptureBinding, SealedProviderCaptureMaterial,
    SourceClass, SourceError, SourceMetadata, SourceObject, SourceObjectCaptureIdentity,
    SourceProtocolViolation, built_in_provider_profiles,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ResearchIngestCommitAuthority, ResearchIngestCoordinator, ResearchSourceDiscoveryCoordinator,
    encode_hex, manifest_value,
};
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

use super::super::domain_support::{DomainCall, DomainLifecycle};

const STANDARD_EXTRACTION_DURATION: Duration = Duration::from_secs(60);
const STANDARD_DISCOVERY_RECEIPT_RETENTION: Duration = Duration::from_secs(5 * 60);
const MAXIMUM_PREPUBLISHED_RESEARCH_SOURCES: usize = 64;

mod alpaca_historical;
mod bea;
mod bls;
mod bls_live;
mod census;
mod crypto_market;
mod eia;
mod fred;
mod iex_hist;
mod official_options_reference;
mod provider_runtime;
mod schwab_market;
mod sec_fund;
mod sec_fundamentals;
mod sec_live;
mod selection;
mod tiingo;
mod treasury;
mod yahoo_enrichment;

pub(crate) use alpaca_historical::{
    AlpacaHistoricalAuthorizedPlan, AlpacaHistoricalPlanAdmissionError,
    AlpacaHistoricalPlanReceipt, AlpacaHistoricalSourceMutationAuthority,
    AlpacaHistoricalSourceSlotError,
};
pub(crate) use bea::{
    BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION, BeaDoctorActivationState, BeaLivePublicationError,
    BeaMacroApplicationClosure, BeaMacroApplicationError, BeaMacroCapabilityState,
    BeaMacroPlanPublication, BeaProviderPeriodLatestKnownDto, BeaProviderPeriodLatestKnownRequest,
    BeaRegionalLiveComposition, BeaRegionalLiveOutcome, BeaRegionalLiveRequest,
    BeaRegionalLiveRuntime, BeaRegisteredSource, BeaSetupRequiredDto, BeaSetupRequiredKind,
    BeaUnavailableDto, BeaUnavailableReason,
};
pub(crate) use bls::{
    BLS_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION, BlsMacroApplicationClosure,
    BlsMacroApplicationError, BlsMacroCapabilityState, BlsMacroPlanPublication,
    BlsMacroUnavailableReason, BlsPreparedMacroPlan, BlsProviderPeriodLatestKnownDto,
    BlsProviderPeriodLatestKnownRequest, BlsSealFirstExtractionLimits,
    BlsWholePlanApplicationHandoff,
};
pub(crate) use bls_live::{
    BlsLiveComposition, BlsLiveOutcome, BlsLivePublicationError, BlsLiveRequest, BlsLiveRuntime,
    BlsLiveSource,
};
pub(crate) use census::{
    CENSUS_QUARTERLY_POINT_IN_TIME_OPERATION, CensusLiveComposition, CensusMacroApplicationClosure,
    CensusMacroApplicationError, CensusPublicationReceipt, CensusQuarterlyPointInTimeRequest,
    CensusQuarterlyRestartReceipt, CensusRegisteredSource, CensusRestartSelector,
    CensusSealFirstExtractionLimits,
};
pub(crate) use crypto_market::{
    CoinbaseMarketApplicationOutcome, CryptoCommittedRowIngress, CryptoMarketPublicationClosure,
    CryptoMarketPublicationError, CryptoMarketSurface, CryptoPendingFrameIngress,
    CryptoPublicationRendezvousLimits, KrakenMarketApplicationOutcome,
    KrakenSealedRawCanonicalUnavailable, MarketEventDurableRead, MarketEventDurableReadWriter,
    MarketEventPointInTimeReceipt, MarketEventPointInTimeSelector, MarketEventPublicationReceipt,
    MarketEventReadError, MarketEventRestartReceipt, MarketEventRestartSelector,
    MarketEventSealedReceiptEvidence,
};
pub(crate) use eia::{
    EIA_MACRO_CALENDAR_POINT_IN_TIME_OPERATION, EIA_MACRO_PROVIDER_PERIOD_POINT_IN_TIME_OPERATION,
    EiaApplicationAcquisitionLimits, EiaLiveComposition, EiaMacroApplicationClosure,
    EiaMacroApplicationError, EiaMacroEffectiveCutoff, EiaMacroPointInTimeRequest,
    EiaMacroPublicationReceipt, EiaMacroRestartReceipt, EiaMacroRestartSelector,
    EiaRegisteredSource,
};
pub(crate) use fred::{FredProductionPublicationError, FredPublishedGenerationHandoff};
pub(crate) use iex_hist::{
    IexHistApplicationError, IexHistApplicationLane, IexHistCaptureSealHandoff,
    IexHistCaptureSealRequirements, IexHistCatalogSealHandoff, IexHistClockStatus,
    IexHistExactJobPreview, IexHistExplicitJobRequest, IexHistInstrumentIdentityBlocker,
    IexHistInstrumentIdentityStatus, IexHistJobAuthority, IexHistJobStatus,
    IexHistPhysicalArtifact, IexHistPhysicalSealRequirement, IexHistPublicationAvailability,
    IexHistPublicationBlocker, IexHistPublicationBlockers, IexHistResearchJobLeaf,
    IexHistSelectionStatus,
};
pub(crate) use official_options_reference::{
    OfficialOptionsReferenceApplicationBinding, OfficialOptionsReferenceApplicationError,
    OfficialOptionsReferenceApplicationStage, OfficialOptionsReferenceCatalogCommitInput,
    OfficialOptionsReferenceClosureCommitment, OfficialOptionsReferenceObjectCommitment,
    OfficialOptionsReferenceStrictObjectCommitment,
};
pub use provider_runtime::ResearchProviderRuntimeGeneration;
pub(crate) use provider_runtime::{
    CryptoMarketPublicationAuthority, ResearchProviderPublicationOperation,
    ResearchProviderRuntimeMutationAuthority, ResearchProviderRuntimeReplacement,
    SchwabRestQuotePublicationPackage,
};
use provider_runtime::{ResearchProviderAdmission, ResearchProviderPublicationLease};
pub(crate) use schwab_market::{
    SchwabMarketPublicationError, SchwabRestQuoteGenerationAuthority,
    SchwabRestQuotePostSealFailure, SchwabRestQuoteSourceHealthOutcome,
};
pub(crate) use sec_fund::{
    SEC_NCEN_FUND_POINT_IN_TIME_OPERATION, SEC_NPORT_FUND_POINT_IN_TIME_OPERATION,
    SecFundApplicationBridge, SecFundApplicationError, SecFundPublicationReceipt,
    SecNcenFundPublicationReceipt, SecNcenFundRestartSelector, SecNportFundPublicationReceipt,
    SecNportFundRestartSelector,
};
pub(crate) use sec_live::{SecLiveFundApplicationError, SecLiveFundRequest, SecLiveFundSource};
use selection::{PreparedRetainedSelection, RetainedDiscoverySelections};
pub use selection::{
    ResearchSourceDiscovery, ResearchSourceDiscoveryObject, ResearchSourceObjectListing,
};
pub(crate) use tiingo::{
    TIINGO_EOD_MARKET_BAR_POINT_IN_TIME_OPERATION, TIINGO_FUND_NAV_POINT_IN_TIME_OPERATION,
    TiingoEodApplicationOutcome, TiingoEodRestartSelector, TiingoFundNavApplicationOutcome,
    TiingoFundNavRestartSelector, TiingoLatestApplicationError,
};
pub(crate) use treasury::{
    TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION, TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION,
    TreasuryApplicationClosure, TreasuryApplicationError, TreasuryDailyRatesLatestKnownReceipt,
    TreasuryDailyRatesLatestKnownRequest, TreasuryFiscalDataLatestKnownReceipt,
    TreasuryFiscalDataLatestKnownRequest, TreasuryMacroPublicationReceipt,
    TreasuryMacroRestartSelector, TreasurySelectedObjectRequest,
};

/// Fixed operation ceilings applied independently of transport result limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchExtractionLimits {
    discovery_objects: NonZeroU16,
    records: NonZeroU32,
    bytes: NonZeroU64,
    operation_duration: Duration,
    discovery_receipt_retention: Duration,
}

impl ResearchExtractionLimits {
    /// Constructs bounded discovery, extraction, and receipt-retention ceilings.
    ///
    /// # Errors
    ///
    /// Rejects any ceiling above the canonical in-memory extraction contracts or either zero
    /// duration.
    pub fn try_new(
        discovery_objects: NonZeroU16,
        records: NonZeroU32,
        bytes: NonZeroU64,
        operation_duration: Duration,
        discovery_receipt_retention: Duration,
    ) -> Result<Self, ResearchIngestCompositionError> {
        if usize::from(discovery_objects.get()) > MAX_DISCOVERY_OBJECTS
            || usize::try_from(records.get()).map_or(true, |value| value > MAX_EXTRACTION_RECORDS)
            || bytes.get() > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
            || operation_duration.is_zero()
            || discovery_receipt_retention.is_zero()
        {
            return Err(ResearchIngestCompositionError::InvalidLimits);
        }
        Ok(Self {
            discovery_objects,
            records,
            bytes,
            operation_duration,
            discovery_receipt_retention,
        })
    }

    /// Returns conservative production defaults bounded to one in-memory publication batch.
    pub fn standard() -> Self {
        Self {
            discovery_objects: NonZeroU16::new(MAX_DISCOVERY_OBJECTS as u16)
                .unwrap_or(NonZeroU16::MIN),
            records: NonZeroU32::new(MAX_EXTRACTION_RECORDS as u32).unwrap_or(NonZeroU32::MIN),
            bytes: NonZeroU64::new(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES).unwrap_or(NonZeroU64::MIN),
            operation_duration: STANDARD_EXTRACTION_DURATION,
            discovery_receipt_retention: STANDARD_DISCOVERY_RECEIPT_RETENTION,
        }
    }
}

impl Default for ResearchExtractionLimits {
    fn default() -> Self {
        Self::standard()
    }
}

/// Immutable evidence required to admit persistence for one exact source payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchRightsAuthority {
    source_id: SourceId,
    basis: RightsBasis,
    parent_authorization_evidence: EvidenceDigest,
    authorization_evidence: EvidenceDigest,
    authorization_expires_at: Option<Timestamp>,
    exact_subjects: Option<BTreeSet<SourceIdentifier>>,
    permitted_operations: BTreeSet<SourceOperation>,
}

impl ResearchRightsAuthority {
    /// Returns the exact source namespace this authority may bind.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Binds non-zero terms or ownership evidence to one source namespace.
    ///
    /// # Errors
    ///
    /// Rejects zero evidence digests; payload identity and expiry are revalidated per extraction.
    pub fn try_new(
        source_id: SourceId,
        basis: RightsBasis,
        authorization_evidence: EvidenceDigest,
        authorization_expires_at: Option<Timestamp>,
    ) -> Result<Self, ResearchIngestCompositionError> {
        if basis.digest().bytes() == [0; 32] || authorization_evidence.bytes() == [0; 32] {
            return Err(ResearchIngestCompositionError::InvalidRightsEvidence);
        }
        Ok(Self {
            source_id,
            basis,
            parent_authorization_evidence: authorization_evidence,
            authorization_evidence,
            authorization_expires_at,
            exact_subjects: None,
            permitted_operations: BTreeSet::from([SourceOperation::Persist]),
        })
    }

    /// Binds a source-wide provider authority to an exact affirmative operation set.
    pub(crate) fn try_new_source_wide(
        source_id: SourceId,
        basis: RightsBasis,
        authorization_evidence: EvidenceDigest,
        authorization_expires_at: Option<Timestamp>,
        permitted_operations: Vec<SourceOperation>,
    ) -> Result<Self, ResearchIngestCompositionError> {
        let operation_count = permitted_operations.len();
        let permitted_operations = permitted_operations.into_iter().collect::<BTreeSet<_>>();
        if basis.digest().bytes() == [0; 32]
            || authorization_evidence.bytes() == [0; 32]
            || permitted_operations.is_empty()
            || permitted_operations.len() != operation_count
            || !permitted_operations.contains(&SourceOperation::Persist)
        {
            return Err(ResearchIngestCompositionError::InvalidRightsEvidence);
        }
        Ok(Self {
            source_id,
            basis,
            parent_authorization_evidence: authorization_evidence,
            authorization_evidence,
            authorization_expires_at,
            exact_subjects: None,
            permitted_operations,
        })
    }

    /// Binds one subordinate authority to its parent activation and exact subject scope.
    ///
    /// # Errors
    ///
    /// Rejects zero evidence, empty or duplicated subject/operation scopes, or a scope that does
    /// not affirmatively include persistence.
    #[allow(
        clippy::too_many_arguments,
        reason = "parent, subordinate, scope, operations, and expiry are independent authority dimensions"
    )]
    pub fn try_new_scoped(
        source_id: SourceId,
        basis: RightsBasis,
        parent_authorization_evidence: EvidenceDigest,
        authorization_evidence: EvidenceDigest,
        authorization_expires_at: Timestamp,
        exact_subjects: Vec<SourceIdentifier>,
        permitted_operations: Vec<SourceOperation>,
    ) -> Result<Self, ResearchIngestCompositionError> {
        let subject_count = exact_subjects.len();
        let exact_subjects = exact_subjects.into_iter().collect::<BTreeSet<_>>();
        let operation_count = permitted_operations.len();
        let permitted_operations = permitted_operations.into_iter().collect::<BTreeSet<_>>();
        if basis.digest().bytes() == [0; 32]
            || parent_authorization_evidence.bytes() == [0; 32]
            || authorization_evidence.bytes() == [0; 32]
            || exact_subjects.is_empty()
            || exact_subjects.len() != subject_count
            || permitted_operations.is_empty()
            || permitted_operations.len() != operation_count
            || !permitted_operations.contains(&SourceOperation::Persist)
        {
            return Err(ResearchIngestCompositionError::InvalidRightsEvidence);
        }
        Ok(Self {
            source_id,
            basis,
            parent_authorization_evidence,
            authorization_evidence,
            authorization_expires_at: Some(authorization_expires_at),
            exact_subjects: Some(exact_subjects),
            permitted_operations,
        })
    }

    pub(crate) fn decision(
        &self,
        payload_digest: EvidenceDigest,
        retrieved_at: Timestamp,
    ) -> Result<RightsDecisionInput, ServiceError> {
        self.validate_at(retrieved_at)?;
        Ok(RightsDecisionInput {
            source_id: self.source_id.clone(),
            payload_digest,
            retrieved_at,
            basis: self.basis.clone(),
            authorization_evidence: self.authorization_evidence,
            authorization_expires_at: self.authorization_expires_at,
            permitted_operations: self.permitted_operations.iter().copied().collect(),
        })
    }

    fn discovery_evidence(
        &self,
        observed_at: Timestamp,
    ) -> Result<ResearchSourceDiscoveryRights, ServiceError> {
        self.validate_at(observed_at)?;
        Ok(ResearchSourceDiscoveryRights {
            basis_reference: self.basis.reference().to_owned(),
            basis_digest: self.basis.digest(),
            root_identity_digest: self.basis.root_identity_digest(),
            parent_authorization_evidence: self.parent_authorization_evidence,
            authorization_evidence: self.authorization_evidence,
            authorization_expires_at: self.authorization_expires_at,
            source_wide: self.exact_subjects.is_none(),
            exact_subjects: self
                .exact_subjects
                .as_ref()
                .map_or_else(Vec::new, |subjects| subjects.iter().cloned().collect()),
            persistence_operation_admitted: self
                .permitted_operations
                .contains(&SourceOperation::Persist),
            model_training_operation_admitted: self
                .permitted_operations
                .contains(&SourceOperation::Train),
        })
    }

    fn validate_at(&self, observed_at: Timestamp) -> Result<(), ServiceError> {
        if self
            .authorization_expires_at
            .is_some_and(|expiry| expiry <= observed_at)
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }

    fn validate_subject(&self, subject: Option<&SourceIdentifier>) -> Result<(), ServiceError> {
        match (&self.exact_subjects, subject) {
            (None, _) => Ok(()),
            (Some(subjects), Some(subject)) if subjects.contains(subject) => Ok(()),
            (Some(_), _) => Err(ServiceError::Unauthorized),
        }
    }

    fn matches_discovery_evidence(&self, evidence: &ResearchSourceDiscoveryRights) -> bool {
        evidence.basis_reference == self.basis.reference()
            && evidence.basis_digest == self.basis.digest()
            && evidence.root_identity_digest == self.basis.root_identity_digest()
            && evidence.parent_authorization_evidence == self.parent_authorization_evidence
            && evidence.authorization_evidence == self.authorization_evidence
            && evidence.authorization_expires_at == self.authorization_expires_at
            && evidence.source_wide == self.exact_subjects.is_none()
            && evidence.exact_subjects
                == self
                    .exact_subjects
                    .as_ref()
                    .map_or_else(Vec::new, |subjects| subjects.iter().cloned().collect())
            && evidence.persistence_operation_admitted
                == self
                    .permitted_operations
                    .contains(&SourceOperation::Persist)
            && evidence.model_training_operation_admitted
                == self.permitted_operations.contains(&SourceOperation::Train)
    }
}

/// Retained persistence-rights evidence for one provider discovery result.
///
/// Discovery does not manufacture a payload-specific rights decision. The existing ingestion
/// consumer rebinds this authority to the exact extracted payload digest before publication.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchSourceDiscoveryRights {
    basis_reference: String,
    basis_digest: EvidenceDigest,
    root_identity_digest: Option<EvidenceDigest>,
    parent_authorization_evidence: EvidenceDigest,
    authorization_evidence: EvidenceDigest,
    authorization_expires_at: Option<Timestamp>,
    source_wide: bool,
    exact_subjects: Vec<SourceIdentifier>,
    persistence_operation_admitted: bool,
    model_training_operation_admitted: bool,
}

impl ResearchSourceDiscoveryRights {
    /// Returns the canonical terms URL or path-free local ownership reference.
    pub fn basis_reference(&self) -> &str {
        &self.basis_reference
    }

    /// Returns the exact terms or owned-manifest content digest.
    pub const fn basis_digest(&self) -> EvidenceDigest {
        self.basis_digest
    }

    /// Returns the path-free local input-root identity when the basis is user-owned.
    pub const fn root_identity_digest(&self) -> Option<EvidenceDigest> {
        self.root_identity_digest
    }

    /// Returns the parent onboarding rights decision from which this authority was derived.
    pub const fn parent_authorization_evidence(&self) -> EvidenceDigest {
        self.parent_authorization_evidence
    }

    /// Returns the exact activation or owner-authorization evidence.
    pub const fn authorization_evidence(&self) -> EvidenceDigest {
        self.authorization_evidence
    }

    /// Returns the retained authorization expiry, when one exists.
    pub const fn authorization_expires_at(&self) -> Option<Timestamp> {
        self.authorization_expires_at
    }

    /// Returns whether the authority covers the complete registered source namespace.
    pub const fn source_wide(&self) -> bool {
        self.source_wide
    }

    /// Returns the exact provider subjects when this is a subordinate scoped authority.
    pub fn exact_subjects(&self) -> &[SourceIdentifier] {
        &self.exact_subjects
    }

    /// Returns whether the source-level persistence operation was admitted.
    ///
    /// The existing ingest gate still binds that authority to the exact extracted payload.
    pub const fn persistence_operation_admitted(&self) -> bool {
        self.persistence_operation_admitted
    }

    /// Returns whether the same exact authority admits local model training.
    pub const fn model_training_operation_admitted(&self) -> bool {
        self.model_training_operation_admitted
    }
}

/// Adapter revision evidence failed to align with one normalized extraction batch.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("research extraction revision evidence is invalid")]
pub struct ResearchRevisionPlanError;

/// One source-agnostic analytical batch plus optional adapter-owned reference metadata.
#[derive(Debug)]
pub struct ManagedExtraction {
    batch: ExtractionBatch,
    company_identity: Option<CompanyIdentityObservation>,
    capture_material: Option<ProviderCaptureMaterial>,
}

#[derive(Debug)]
struct ManagedProviderNativePublication {
    native_lineage: ProviderNativeLineageBatch,
    row_capture_page_ordinals: Vec<u16>,
}

#[derive(Debug)]
pub struct ManagedExtractionWithNative {
    handoff: ManagedExtractionHandoff,
}

#[derive(Debug)]
enum ManagedExtractionHandoff {
    Pending {
        extraction: ManagedExtraction,
        provider_native: Option<ManagedProviderNativePublication>,
    },
    Provider {
        sealed_capture: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
    },
}

impl ManagedExtraction {
    fn analytical_only(batch: ExtractionBatch) -> Self {
        Self {
            batch,
            company_identity: None,
            capture_material: None,
        }
    }
}

impl ManagedExtractionWithNative {
    fn pending(
        extraction: ManagedExtraction,
        provider_native: Option<ManagedProviderNativePublication>,
    ) -> Self {
        Self {
            handoff: ManagedExtractionHandoff::Pending {
                extraction,
                provider_native,
            },
        }
    }

    fn provider(
        sealed_capture: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
    ) -> Self {
        Self {
            handoff: ManagedExtractionHandoff::Provider {
                sealed_capture,
                revisions,
            },
        }
    }
}

/// Least-privilege application capability for physically sealing provider capture material.
///
/// The capability cannot publish a dataset, read an existing raw object, or outlive the current
/// bounded operation. Adapter composition receives it only while extraction remains under the
/// coordinator's cancellation and deadline authority.
#[derive(Clone)]
pub struct ManagedProviderCaptureAuthority {
    research: Arc<ResearchService>,
    deadline: Instant,
}

impl ManagedProviderCaptureAuthority {
    fn new(research: Arc<ResearchService>, deadline: Instant) -> Self {
        Self { research, deadline }
    }

    async fn seal(
        &self,
        request: ProviderCaptureSealRequest,
        cancellation: &CancellationToken,
    ) -> Result<SealedProviderCaptureMaterial, ExtractionSourceError> {
        self.research
            .seal_provider_capture(request, cancellation, self.deadline)
            .await
            .map_err(map_managed_capture_error)
    }
}

impl fmt::Debug for ManagedProviderCaptureAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedProviderCaptureAuthority")
            .field("research", &"[APPLICATION-OWNED CAPTURE AUTHORITY]")
            .field("deadline", &self.deadline)
            .finish()
    }
}

fn map_managed_capture_error(error: ResearchServiceError) -> ExtractionSourceError {
    match error {
        ResearchServiceError::Ingest(IngestError::Cancelled) => ExtractionSourceError::Cancelled,
        ResearchServiceError::Ingest(IngestError::DeadlineExceeded) => {
            ExtractionSourceError::DeadlineExceeded
        }
        _ => invalid_capture_protocol(),
    }
}

/// One bounded discovery batch plus exact provider bytes that must follow its selected object.
#[derive(Debug)]
pub struct ManagedDiscovery {
    batch: DiscoveryBatch,
    capture_material: Option<ProviderCaptureMaterial>,
}

/// Source-neutral extraction failure plus an optional closed protocol reason for internal logs.
pub struct ManagedDiscoveryDiagnosticError {
    source: ExtractionSourceError,
    protocol: Option<SourceProtocolViolation>,
}

impl From<ExtractionSourceError> for ManagedDiscoveryDiagnosticError {
    fn from(source: ExtractionSourceError) -> Self {
        Self {
            source,
            protocol: None,
        }
    }
}

impl From<market_squawk_adapter_fred::FredDiscoveryError> for ManagedDiscoveryDiagnosticError {
    fn from(error: market_squawk_adapter_fred::FredDiscoveryError) -> Self {
        let protocol = error.protocol_violation();
        Self {
            source: error.into_source_error(),
            protocol,
        }
    }
}

impl ManagedDiscovery {
    fn inspection_only(batch: DiscoveryBatch) -> Self {
        Self {
            batch,
            capture_material: None,
        }
    }

    fn with_capture(
        batch: DiscoveryBatch,
        capture_material: ProviderCaptureMaterial,
    ) -> Result<Self, ExtractionSourceError> {
        let [object] = batch.objects() else {
            return Err(invalid_capture_protocol());
        };
        let receipt = capture_material.receipt();
        let capture_identity = SourceObjectCaptureIdentity::try_from_capture(receipt)
            .map_err(|_error| invalid_capture_protocol())?;
        if receipt.source_id() != object.source_id()
            || receipt.metadata_revision() != object.metadata_revision()
            || receipt.dataset() != object.dataset()
            || capture_identity != object.capture_identity()
        {
            return Err(invalid_capture_protocol());
        }
        Ok(Self {
            batch,
            capture_material: Some(capture_material),
        })
    }
}

/// Production extraction adapter plus its source-specific revision authority.
pub trait ManagedResearchExtractionSource: ExtractionSource + Send + Sync + 'static {
    /// Discovers source objects while retaining exact response material when extraction consumes
    /// a discovery-time provider representation.
    fn discover_managed(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedDiscovery, ExtractionSourceError>> {
        let discovered = self.discover(authority, request, cancellation);
        Box::pin(async move { discovered.await.map(ManagedDiscovery::inspection_only) })
    }

    /// Discovers source objects while retaining only a closed payload-free diagnostic reason.
    fn discover_managed_diagnostic(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedDiscovery, ManagedDiscoveryDiagnosticError>> {
        let discovered = self.discover_managed(authority, request, cancellation);
        Box::pin(async move {
            discovered
                .await
                .map_err(ManagedDiscoveryDiagnosticError::from)
        })
    }

    /// Extracts one analytical batch with source-specific reference metadata from the same bytes.
    fn extract_managed(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtraction, ExtractionSourceError>> {
        if !matches!(
            self.metadata().source_class(),
            SourceClass::LocalFile | SourceClass::PortfolioExport
        ) {
            return Box::pin(async { Err(invalid_capture_protocol()) });
        }
        let extracted = self.extract(authority, request, cancellation);
        Box::pin(async move { extracted.await.map(ManagedExtraction::analytical_only) })
    }

    /// Extracts canonical and exact adapter-native publication material as one owned handoff.
    ///
    /// Most sources use the canonical-only default. Provider adapters whose durable publication
    /// requires native lineage override this method so neither lineage nor the exact row-to-page
    /// map can be discarded between adapter extraction and application raw sealing.
    fn extract_managed_with_native(
        &self,
        _capture: ManagedProviderCaptureAuthority,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtractionWithNative, ExtractionSourceError>> {
        let extracted = self.extract_managed(authority, request, cancellation);
        Box::pin(async move {
            extracted
                .await
                .map(|extraction| ManagedExtractionWithNative::pending(extraction, None))
        })
    }

    /// Returns the exact provider dataset used to begin discovery, when one fixed dataset is
    /// carried by the admitted adapter configuration.
    fn discovery_dataset_identifier(&self) -> Option<&SourceIdentifier> {
        None
    }

    /// Returns an exact provider rights subject for a dataset, when the adapter uses scoped rights.
    fn rights_subject(
        &self,
        _dataset: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ResearchRevisionPlanError> {
        Ok(None)
    }

    /// Returns the storage-safe local analytical identity for one exact provider batch.
    ///
    /// The default preserves an already storage-safe provider dataset exactly. Adapters whose
    /// provider grammar uses other delimiters must override this mapping without changing the
    /// provider request or record provenance.
    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        DatasetId::try_from(batch.request().object().dataset().as_str())
            .map_err(|_error| ResearchRevisionPlanError)
    }

    /// Returns provider revision evidence, or `None` only for a user-owned local source.
    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError>;
}

fn invalid_capture_protocol() -> ExtractionSourceError {
    SourceError::InvalidProtocolState.into()
}

fn bind_managed_provider_native_capture(
    batch: ExtractionBatch,
    capture_material: ProviderCaptureMaterial,
    native_lineage: ProviderNativeLineageBatch,
    row_capture_page_ordinals: Vec<u16>,
) -> Result<ManagedExtractionWithNative, ExtractionSourceError> {
    if !capture_material_matches_batch(&capture_material, &batch)
        || native_lineage.validate(&batch).is_err()
        || row_capture_page_ordinals.len() != batch.records().len()
        || row_capture_page_ordinals
            .iter()
            .any(|ordinal| usize::from(*ordinal) >= capture_material.receipt().pages().len())
    {
        return Err(invalid_capture_protocol());
    }
    Ok(ManagedExtractionWithNative::pending(
        ManagedExtraction {
            batch,
            company_identity: None,
            capture_material: Some(capture_material),
        },
        Some(ManagedProviderNativePublication {
            native_lineage,
            row_capture_page_ordinals,
        }),
    ))
}

fn capture_material_matches_batch(
    capture_material: &ProviderCaptureMaterial,
    batch: &ExtractionBatch,
) -> bool {
    let receipt = capture_material.receipt();
    let object = batch.request().object();
    receipt.source_id() == object.source_id()
        && receipt.metadata_revision() == object.metadata_revision()
        && receipt.dataset() == object.dataset()
        && SourceObjectCaptureIdentity::try_from_capture(receipt)
            .is_ok_and(|identity| identity == object.capture_identity())
}

/// Rights-bound static research adapter sealed before its coordinator is published.
///
/// This value is intentionally non-cloneable. It can only be consumed by a prepublication
/// coordinator or local-product constructor and cannot recover runtime mutation authority.
pub struct PrepublishedResearchSourceRegistration {
    profile: SourceIdentifier,
    metadata: SourceMetadata,
    source: Box<dyn ManagedResearchExtractionSource>,
    rights: ResearchRightsAuthority,
}

impl PrepublishedResearchSourceRegistration {
    /// Seals one embedded, user-owned, or licensed adapter for prepublication composition.
    ///
    /// # Errors
    ///
    /// Rejects source/rights identity mismatch and every code-owned onboarding profile.
    pub fn try_new<S>(
        profile: SourceIdentifier,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<Self, ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        let metadata = source.metadata().clone();
        if metadata.source_id() != &rights.source_id {
            return Err(ResearchIngestCompositionError::SourceRightsMismatch);
        }
        let built_in_profiles = built_in_provider_profiles()
            .map_err(|_error| ResearchIngestCompositionError::ProviderProfilesUnavailable)?;
        if built_in_profiles.get(profile.as_str()).is_some() {
            return Err(ResearchIngestCompositionError::BuiltInProfileRequiresOnboarding);
        }
        Ok(Self {
            profile,
            metadata,
            source: Box::new(source),
            rights,
        })
    }

    fn validate(
        &self,
        built_in_profiles: &market_squawk_sources::ProviderProfileRegistry,
    ) -> Result<(), ResearchIngestCompositionError> {
        if self.source.metadata() != &self.metadata
            || self.metadata.source_id() != &self.rights.source_id
        {
            return Err(ResearchIngestCompositionError::SourceRightsMismatch);
        }
        if built_in_profiles.get(self.profile.as_str()).is_some() {
            return Err(ResearchIngestCompositionError::BuiltInProfileRequiresOnboarding);
        }
        Ok(())
    }
}

impl fmt::Debug for PrepublishedResearchSourceRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrepublishedResearchSourceRegistration")
            .field("profile", &self.profile)
            .field("source_id", self.metadata.source_id())
            .finish_non_exhaustive()
    }
}

impl ManagedResearchExtractionSource for market_squawk_adapter_sec::SecEdgarSource {
    fn discover_managed(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedDiscovery, ExtractionSourceError>> {
        let discovered = self.discover_with_capture(authority, request, cancellation);
        Box::pin(async move {
            let result = discovered.await?;
            let (batch, capture_material) = result.into_parts();
            ManagedDiscovery::with_capture(batch, capture_material)
        })
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        market_squawk_adapter_sec::SecEdgarSource::revision_plan(self, batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

impl ManagedResearchExtractionSource for market_squawk_adapter_fred::FredSource {
    fn discover_managed_diagnostic(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedDiscovery, ManagedDiscoveryDiagnosticError>> {
        let discovered = self.discover_with_diagnostic(authority, request, cancellation);
        Box::pin(async move {
            discovered
                .await
                .map(ManagedDiscovery::inspection_only)
                .map_err(ManagedDiscoveryDiagnosticError::from)
        })
    }

    fn extract_managed_with_native(
        &self,
        _capture: ManagedProviderCaptureAuthority,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtractionWithNative, ExtractionSourceError>> {
        let extracted = self.extract_with_capture(authority, request, cancellation);
        Box::pin(async move {
            let (batch, capture, native_lineage, row_capture_page_ordinals) =
                extracted.await?.try_into_common_publication()?;
            bind_managed_provider_native_capture(
                batch,
                capture,
                native_lineage,
                row_capture_page_ordinals,
            )
        })
    }

    fn rights_subject(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ResearchRevisionPlanError> {
        market_squawk_adapter_fred::FredSource::series_identifier(dataset)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let identifier = market_squawk_adapter_fred::FredSource::analytical_dataset_identifier(
            batch.request().object().dataset(),
        )
        .map_err(|_error| ResearchRevisionPlanError)?;
        DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        market_squawk_adapter_fred::FredSource::revision_plan(self, batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

impl ManagedResearchExtractionSource for market_squawk_adapter_bls::BlsSource {
    fn discovery_dataset_identifier(&self) -> Option<&SourceIdentifier> {
        Some(self.dataset())
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let identifier = market_squawk_adapter_bls::BlsSource::analytical_dataset_identifier(
            batch.request().object().dataset(),
        )
        .map_err(|_error| ResearchRevisionPlanError)?;
        DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        market_squawk_adapter_bls::BlsSource::revision_plan(self, batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

impl ManagedResearchExtractionSource for market_squawk_adapter_treasury::TreasurySource {
    fn extract_managed_with_native(
        &self,
        _capture: ManagedProviderCaptureAuthority,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtractionWithNative, ExtractionSourceError>> {
        let extracted = self.extract_with_capture(authority, request, cancellation);
        Box::pin(async move {
            let output = extracted.await?;
            let (batch, capture_material, native_lineage, row_capture_page_ordinals) = output
                .try_into_common_publication()
                .map_err(|_error| invalid_capture_protocol())?;
            bind_managed_provider_native_capture(
                batch,
                capture_material,
                native_lineage,
                row_capture_page_ordinals,
            )
        })
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let identifier = self
            .analytical_dataset_identifier(batch.request().object().dataset())
            .map_err(|_error| ResearchRevisionPlanError)?;
        DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        market_squawk_adapter_treasury::TreasurySource::revision_plan(self, batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

impl ManagedResearchExtractionSource for market_squawk_adapter_federal_reserve::BoardSource {
    fn extract_managed_with_native(
        &self,
        capture: ManagedProviderCaptureAuthority,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtractionWithNative, ExtractionSourceError>> {
        Box::pin(async move {
            let (pending, seal_request) = self
                .extract_for_sealing(authority, request, cancellation.clone())
                .await
                .map_err(map_board_extraction_error)?;
            let sealed = capture.seal(seal_request, &cancellation).await?;
            let publication = pending
                .try_rejoin(sealed)
                .map_err(map_board_extraction_error)?;
            let (_parsed, _receipt, revisions, sealed_capture) =
                publication.into_root_publication_parts();
            Ok(ManagedExtractionWithNative::provider(
                sealed_capture,
                revisions,
            ))
        })
    }

    fn discovery_dataset_identifier(&self) -> Option<&SourceIdentifier> {
        Some(self.profile().dataset())
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let identifier = self
            .analytical_dataset_identifier(batch.request().object().dataset())
            .map_err(|_error| ResearchRevisionPlanError)?;
        DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        market_squawk_adapter_federal_reserve::BoardSource::revision_plan(self, batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

fn map_board_extraction_error(
    error: market_squawk_adapter_federal_reserve::BoardExtractionError,
) -> ExtractionSourceError {
    match error {
        market_squawk_adapter_federal_reserve::BoardExtractionError::Source(error) => error,
        market_squawk_adapter_federal_reserve::BoardExtractionError::Capture(_)
        | market_squawk_adapter_federal_reserve::BoardExtractionError::NativeLineage(_)
        | market_squawk_adapter_federal_reserve::BoardExtractionError::CaptureBodyTooLarge {
            ..
        }
        | market_squawk_adapter_federal_reserve::BoardExtractionError::RawCapture(_) => {
            invalid_capture_protocol()
        }
    }
}

impl ManagedResearchExtractionSource for market_squawk_adapter_files::FileExtractionSource {
    fn revision_plan(
        &self,
        _batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        Ok(None)
    }
}

impl ManagedResearchExtractionSource
    for market_squawk_adapter_portfolio::PortfolioManifestExtractionSource
{
    fn revision_plan(
        &self,
        _batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        Ok(None)
    }
}

struct RegisteredExtractionSource {
    source: Arc<dyn ManagedResearchExtractionSource>,
    typed_capability: RegisteredTypedSourceCapability,
    metadata: SourceMetadata,
    registration: Box<RegisteredSource>,
    rights: ResearchRightsAuthority,
    generation: Option<ResearchProviderRuntimeGeneration>,
    admission: ResearchProviderAdmission,
}

#[derive(Clone)]
enum RegisteredTypedSourceCapability {
    None,
    TreasuryAllHistory(Arc<market_squawk_adapter_treasury::TreasurySource>),
}

impl RegisteredTypedSourceCapability {
    const fn same_kind(&self, candidate: &Self) -> bool {
        matches!(
            (self, candidate),
            (Self::None, Self::None) | (Self::TreasuryAllHistory(_), Self::TreasuryAllHistory(_))
        )
    }
}

/// Closed erased-plus-typed source pair moved as one runtime capability.
struct RegisteredSourceCapability {
    erased: Arc<dyn ManagedResearchExtractionSource>,
    typed: RegisteredTypedSourceCapability,
}

impl RegisteredSourceCapability {
    fn erased<S>(source: S) -> Self
    where
        S: ManagedResearchExtractionSource,
    {
        Self::erased_arc(Arc::new(source))
    }

    const fn erased_arc(source: Arc<dyn ManagedResearchExtractionSource>) -> Self {
        Self {
            erased: source,
            typed: RegisteredTypedSourceCapability::None,
        }
    }

    fn treasury(source: Arc<market_squawk_adapter_treasury::TreasurySource>) -> Self {
        let erased: Arc<dyn ManagedResearchExtractionSource> = source.clone();
        Self {
            erased,
            typed: RegisteredTypedSourceCapability::TreasuryAllHistory(source),
        }
    }
}

struct RegisteredPublicationSource {
    metadata: SourceMetadata,
    registered_at: Timestamp,
    registration: Box<RegisteredSource>,
    rights: ResearchRightsAuthority,
    generation: ResearchProviderRuntimeGeneration,
    admission: ResearchProviderAdmission,
}

struct CoordinatorAuthority {
    registry: Option<AuthoritativeSourceRegistry>,
    sources: BTreeMap<SourceIdentifier, RegisteredExtractionSource>,
    publication_sources: BTreeMap<SourceIdentifier, RegisteredPublicationSource>,
    pending_replacements: BTreeMap<SourceIdentifier, Uuid>,
    selections: RetainedDiscoverySelections,
    alpaca_historical: alpaca_historical::AlpacaHistoricalSourceSlot,
}

/// Sole production coordinator for source discovery, extraction, and analytical publication.
pub struct ProductionResearchIngestCoordinator {
    research: Arc<ResearchService>,
    limits: ResearchExtractionLimits,
    lifecycle: Arc<DomainLifecycle>,
    authority: Arc<Mutex<CoordinatorAuthority>>,
}

/// One lifecycle-tracked, exact-generation capability for a provider macro operation.
///
/// The capability is intentionally linear at the application boundary: registry extraction,
/// subject-qualified rights, provider-generation cancellation, and the matching publication lease
/// are acquired together and remain live for the same request lifetime.
pub(super) struct ProviderMacroOperationAuthority {
    _call: DomainCall,
    research: Arc<ResearchService>,
    generation: ResearchProviderRuntimeGeneration,
    extraction: ExtractionAuthority,
    rights: ResearchRightsAuthority,
    publication: Arc<ResearchProviderPublicationLease>,
    cancellation: CancellationToken,
    watcher: JoinHandle<()>,
    operation_deadline: Instant,
}

/// One exact-generation Treasury source bundled with the common macro-operation authority.
pub(super) struct TreasuryAllHistoryOperationAuthority {
    source: Arc<market_squawk_adapter_treasury::TreasurySource>,
    common: ProviderMacroOperationAuthority,
}

impl TreasuryAllHistoryOperationAuthority {
    pub(super) fn source(
        &self,
    ) -> Result<&Arc<market_squawk_adapter_treasury::TreasurySource>, ServiceError> {
        self.common.ensure_live()?;
        Ok(&self.source)
    }

    pub(super) const fn common(&self) -> &ProviderMacroOperationAuthority {
        &self.common
    }
}

impl ProviderMacroOperationAuthority {
    pub(super) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    pub(super) fn extraction(&self) -> ExtractionAuthority {
        self.extraction.clone()
    }

    pub(super) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(super) const fn operation_deadline(&self) -> Instant {
        self.operation_deadline
    }

    pub(super) fn provider_deadline(&self) -> Result<Timestamp, ServiceError> {
        wall_deadline(self.operation_deadline, &self.cancellation)
    }

    pub(super) fn ensure_live(&self) -> Result<(), ServiceError> {
        ensure_operation_live(self.operation_deadline, &self.cancellation)?;
        self.publication
            .validate_precommit()
            .map_err(|_error| ServiceError::Unavailable)
    }

    pub(super) fn rights_decision(
        &self,
        payload_digest: EvidenceDigest,
        observed_at: Timestamp,
    ) -> Result<RightsDecisionInput, ServiceError> {
        self.ensure_live()?;
        self.rights.decision(payload_digest, observed_at)
    }

    pub(super) fn publication_authority(&self) -> Arc<dyn IngestPrecommitAuthority> {
        self.publication.clone()
    }

    /// Commits one exact provider capture/native/canonical binding through the shared research
    /// publication path and immediately reconstructs its restart coordinate from the catalog.
    pub(super) async fn publish_single_binding(
        &self,
        analytical_dataset: DatasetId,
        binding: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
        expected_implementation: ProviderNativeLineageImplementation,
        observed_at: Timestamp,
    ) -> Result<ProviderMacroSinglePublication, ProviderMacroPublicationError> {
        self.ensure_live()?;
        binding.validate()?;
        let capture = binding.capture_evidence();
        let schema = binding.native_lineage().schema();
        if capture.source_id() != self.generation.metadata().source_id()
            || capture.metadata_revision() != self.generation.metadata().revision()
            || schema.implementation() != expected_implementation
            || binding.record_count() == 0
            || binding.record_count() != revisions.len()
            || !revisions.is_locally_observed()
            || !revisions.native_lineage_required()
        {
            return Err(ProviderMacroPublicationError::InvalidBinding);
        }
        let binding_digest = binding.evidence_digest().evidence();
        let payload_digest = extraction_provider_payload_digest(binding.batch());
        let rights = self.rights_decision(payload_digest, observed_at)?;
        let ingest = ResearchIngestRequest::with_provider_publication(
            self.generation.metadata().clone(),
            rights,
            analytical_dataset,
            binding,
            revisions,
        )?
        .with_precommit_authority(self.publication_authority());
        let committed = self
            .research
            .ingest(ingest, self.cancellation.clone())
            .await?;
        let restart = ProviderMacroRestartBinding::try_reopen(
            self.research.as_ref(),
            committed.manifest().clone(),
            self.generation.metadata().source_id(),
            expected_implementation,
        )?;
        if restart.binding_digest != binding_digest {
            return Err(ProviderMacroPublicationError::RestartMismatch);
        }
        Ok(ProviderMacroSinglePublication { committed, restart })
    }
}

impl Drop for ProviderMacroOperationAuthority {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.watcher.abort();
    }
}

/// Catalog-reconstructible coordinate for a single-binding provider macro generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderMacroRestartBinding {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    source_id: SourceId,
    metadata_revision: SourceIdentifier,
    provider_dataset: SourceIdentifier,
    record_count: usize,
    native_implementation: &'static str,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
}

impl ProviderMacroRestartBinding {
    pub(super) fn try_reopen(
        research: &ResearchService,
        manifest: DatasetManifestRef,
        expected_source: &SourceId,
        expected_implementation: ProviderNativeLineageImplementation,
    ) -> Result<Self, ProviderMacroPublicationError> {
        let binding_digests = research
            .analytical()
            .provider_capture_binding_digests(&manifest)?;
        let [binding_digest] = binding_digests.as_slice() else {
            return Err(ProviderMacroPublicationError::RestartMismatch);
        };
        let store = research.provider_capture_store();
        let evidence = research.analytical().provider_capture_binding_evidence(
            &manifest,
            *binding_digest,
            store.as_ref(),
        )?;
        let implementation = macro_native_implementation_name(expected_implementation)
            .ok_or(ProviderMacroPublicationError::InvalidBinding)?;
        if evidence.binding_digest() != *binding_digest
            || evidence.capture().source_id() != expected_source
            || evidence.record_count() == 0
            || evidence.record_count() != evidence.rows().len()
            || evidence.native_lineage().implementation() != implementation
            || evidence.native_lineage().row_count() != evidence.record_count()
        {
            return Err(ProviderMacroPublicationError::RestartMismatch);
        }
        let _pinned = research.analytical().pinned(&manifest)?;
        Ok(Self {
            manifest,
            binding_digest: *binding_digest,
            source_id: evidence.capture().source_id().clone(),
            metadata_revision: evidence
                .capture()
                .metadata_revision()
                .as_source_identifier()
                .clone(),
            provider_dataset: evidence.capture().dataset().clone(),
            record_count: evidence.record_count(),
            native_implementation: implementation,
            native_schema_version: evidence.native_lineage().version(),
            native_schema_fingerprint: evidence.native_lineage().fingerprint(),
        })
    }

    pub(super) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(super) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    pub(super) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(super) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(super) fn evidence(
        &self,
        research: &ResearchService,
    ) -> Result<PersistedProviderCaptureBindingEvidence, ProviderMacroPublicationError> {
        let store = research.provider_capture_store();
        let evidence = research.analytical().provider_capture_binding_evidence(
            &self.manifest,
            self.binding_digest,
            store.as_ref(),
        )?;
        if evidence.capture().source_id() != &self.source_id
            || evidence
                .capture()
                .metadata_revision()
                .as_source_identifier()
                != &self.metadata_revision
            || evidence.capture().dataset() != &self.provider_dataset
            || evidence.record_count() != self.record_count
            || evidence.native_lineage().implementation() != self.native_implementation
            || evidence.native_lineage().version() != self.native_schema_version
            || evidence.native_lineage().fingerprint() != self.native_schema_fingerprint
        {
            return Err(ProviderMacroPublicationError::RestartMismatch);
        }
        Ok(evidence)
    }
}

pub(super) struct ProviderMacroSinglePublication {
    committed: CommittedDataset,
    restart: ProviderMacroRestartBinding,
}

impl ProviderMacroSinglePublication {
    pub(super) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(super) fn into_parts(self) -> (CommittedDataset, ProviderMacroRestartBinding) {
        (self.committed, self.restart)
    }
}

#[derive(Debug, Error)]
pub(super) enum ProviderMacroPublicationError {
    #[error("provider macro operation authority is unavailable")]
    Service(#[from] ServiceError),
    #[error("provider macro sealed binding is invalid")]
    Capture(#[from] ProviderCaptureError),
    #[error("provider macro research publication failed")]
    Research(#[from] ResearchServiceError),
    #[error("provider macro catalog reconstruction failed")]
    Ingest(#[from] IngestError),
    #[error("provider macro binding does not match the exact operation generation")]
    InvalidBinding,
    #[error("provider macro generation could not be reconstructed exactly")]
    RestartMismatch,
}

const fn macro_native_implementation_name(
    implementation: ProviderNativeLineageImplementation,
) -> Option<&'static str> {
    match implementation {
        ProviderNativeLineageImplementation::CensusTabularV1 => Some("census_tabular_v1"),
        ProviderNativeLineageImplementation::EiaSeriesV1 => Some("eia_series_v1"),
        _ => None,
    }
}

impl ProductionResearchIngestCoordinator {
    /// Binds restart-durable source authority to the sole analytical publication service.
    #[must_use]
    pub fn new(
        registry: AuthoritativeSourceRegistry,
        research: Arc<ResearchService>,
        limits: ResearchExtractionLimits,
    ) -> Self {
        Self {
            research,
            limits,
            lifecycle: DomainLifecycle::new(),
            authority: Arc::new(Mutex::new(CoordinatorAuthority {
                registry: Some(registry),
                sources: BTreeMap::new(),
                publication_sources: BTreeMap::new(),
                pending_replacements: BTreeMap::new(),
                selections: RetainedDiscoverySelections::new(),
                alpaca_historical: alpaca_historical::AlpacaHistoricalSourceSlot::absent(),
            })),
        }
    }

    /// Acquires every authority needed by one provider macro operation as one exact-generation
    /// capability. No extraction authority, rights value, cancellation token, or publication
    /// lease is returned independently.
    pub(super) async fn acquire_provider_macro_operation(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        provider_dataset: &SourceIdentifier,
        context: &RequestContext,
    ) -> Result<ProviderMacroOperationAuthority, ServiceError> {
        self.acquire_provider_macro_operation_with_registered_capability(
            generation,
            provider_dataset,
            context,
        )
        .await
        .map(|(operation, _capability)| operation)
    }

    /// Acquires the exact registered Treasury allocation together with the common macro
    /// operation authority. The typed source never crosses the ingestion boundary independently.
    pub(super) async fn acquire_treasury_all_history_operation(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        provider_dataset: &SourceIdentifier,
        context: &RequestContext,
    ) -> Result<TreasuryAllHistoryOperationAuthority, ServiceError> {
        let (common, capability) = self
            .acquire_provider_macro_operation_with_registered_capability(
                generation,
                provider_dataset,
                context,
            )
            .await?;
        let RegisteredTypedSourceCapability::TreasuryAllHistory(source) = capability else {
            return Err(ServiceError::Unavailable);
        };
        let operation = TreasuryAllHistoryOperationAuthority { source, common };
        operation.common.ensure_live()?;
        Ok(operation)
    }

    /// Reconstructs one completed Treasury acquisition only through the exact registered source
    /// generation and the application-owned sealed raw store.
    pub(super) async fn restore_treasury_all_history_completion(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        provider_dataset: &SourceIdentifier,
        checkpoint: &[u8],
        store: Arc<SealedResearchJournalStore>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_adapter_treasury::TreasuryAllHistoryAcquisitionCompletion, ServiceError>
    {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(operation_state_error(deadline, cancellation));
        }
        let source = self.treasury_all_history_restore_source(generation, provider_dataset)?;
        let restored = source
            .restore_all_history_backfill(checkpoint, store, deadline, cancellation)
            .await
            .map_err(map_treasury_restore_error)?;
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(operation_state_error(deadline, cancellation));
        }
        let current = self.treasury_all_history_restore_source(generation, provider_dataset)?;
        if !Arc::ptr_eq(&source, &current) {
            return Err(ServiceError::Unavailable);
        }
        restored
            .acquisition_completion()
            .map_err(|_error| ServiceError::Unavailable)
    }

    fn treasury_all_history_restore_source(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        provider_dataset: &SourceIdentifier,
    ) -> Result<Arc<market_squawk_adapter_treasury::TreasurySource>, ServiceError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        let registered = authority
            .sources
            .get(generation.profile())
            .ok_or(ServiceError::NotFound)?;
        if registered.generation.as_ref() != Some(generation)
            || registered.metadata != *generation.metadata()
            || registered.source.metadata() != generation.metadata()
            || registered.registration.source_id() != generation.metadata().source_id()
            || registered.registration.revision() != generation.metadata().revision()
            || !registered
                .admission
                .admits_generation(generation)
                .map_err(|_error| ServiceError::Unavailable)?
        {
            return Err(ServiceError::Unavailable);
        }
        registered.rights.validate_at(system_timestamp()?)?;
        let subject = registered
            .source
            .rights_subject(provider_dataset)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        registered.rights.validate_subject(subject.as_ref())?;
        let RegisteredTypedSourceCapability::TreasuryAllHistory(source) =
            registered.typed_capability.clone()
        else {
            return Err(ServiceError::Unavailable);
        };
        Ok(source)
    }

    async fn acquire_provider_macro_operation_with_registered_capability(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        provider_dataset: &SourceIdentifier,
        context: &RequestContext,
    ) -> Result<
        (
            ProviderMacroOperationAuthority,
            RegisteredTypedSourceCapability,
        ),
        ServiceError,
    > {
        let call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let operation_deadline = operation_deadline(context, self.limits.operation_duration)?;
        let (extraction, rights, admission, typed_capability) = {
            let authority = self
                .authority
                .lock()
                .map_err(|_error| ServiceError::Unavailable)?;
            let registry = authority
                .registry
                .as_ref()
                .ok_or(ServiceError::Unavailable)?;
            let registered = authority
                .sources
                .get(generation.profile())
                .ok_or(ServiceError::NotFound)?;
            if registered.generation.as_ref() != Some(generation)
                || registered.metadata != *generation.metadata()
                || registered.source.metadata() != generation.metadata()
                || registered.registration.source_id() != generation.metadata().source_id()
                || registered.registration.revision() != generation.metadata().revision()
                || !registered
                    .admission
                    .admits_generation(generation)
                    .map_err(|_error| ServiceError::Unavailable)?
            {
                return Err(ServiceError::Unavailable);
            }
            registered.rights.validate_at(system_timestamp()?)?;
            let subject = registered
                .source
                .rights_subject(provider_dataset)
                .map_err(|_error| ServiceError::InvalidRequest)?;
            registered.rights.validate_subject(subject.as_ref())?;
            let extraction = registry
                .extraction_authority(&registered.registration, registered.source.as_ref())
                .map_err(map_registry_error)?;
            (
                extraction,
                registered.rights.clone(),
                registered.admission.clone(),
                registered.typed_capability.clone(),
            )
        };
        let cancellation = CancellationToken::new();
        let signal = cancellation.clone();
        let caller = context.cancellation().clone();
        let shutdown = self.lifecycle.shutdown_token().clone();
        let revoked = admission.cancellation().clone();
        let watched_caller = caller.clone();
        let watched_shutdown = shutdown.clone();
        let watched_revoked = revoked.clone();
        let watcher = tokio::spawn(async move {
            tokio::select! {
                biased;
                () = watched_caller.cancelled() => signal.cancel(),
                () = watched_shutdown.cancelled() => signal.cancel(),
                () = watched_revoked.cancelled() => signal.cancel(),
                () = tokio::time::sleep_until(operation_deadline.into()) => signal.cancel(),
            }
        });
        let lease = admission.acquire_publication_lease();
        tokio::pin!(lease);
        let publication = tokio::select! {
            biased;
            () = caller.cancelled() => {
                cancellation.cancel();
                watcher.abort();
                return Err(ServiceError::Cancelled);
            }
            () = shutdown.cancelled() => {
                cancellation.cancel();
                watcher.abort();
                return Err(ServiceError::Unavailable);
            }
            () = revoked.cancelled() => {
                cancellation.cancel();
                watcher.abort();
                return Err(ServiceError::Unavailable);
            }
            () = tokio::time::sleep_until(operation_deadline.into()) => {
                cancellation.cancel();
                watcher.abort();
                return Err(ServiceError::DeadlineExceeded);
            }
            () = cancellation.cancelled() => {
                watcher.abort();
                if caller.is_cancelled() {
                    return Err(ServiceError::Cancelled);
                }
                if Instant::now() >= operation_deadline {
                    return Err(ServiceError::DeadlineExceeded);
                }
                return Err(ServiceError::Unavailable);
            }
            result = lease.as_mut() => result.map(Arc::new),
        };
        let publication = match publication {
            Ok(publication) => publication,
            Err(_error) => {
                cancellation.cancel();
                watcher.abort();
                return Err(ServiceError::Unavailable);
            }
        };
        if publication.validate_precommit().is_err() {
            cancellation.cancel();
            watcher.abort();
            return Err(ServiceError::Unavailable);
        }
        let operation = ProviderMacroOperationAuthority {
            _call: call,
            research: Arc::clone(&self.research),
            generation: generation.clone(),
            extraction,
            rights,
            publication,
            cancellation,
            watcher,
            operation_deadline,
        };
        operation.ensure_live()?;
        Ok((operation, typed_capability))
    }

    #[cfg(test)]
    pub(crate) async fn treasury_all_history_source_identity_for_test(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        provider_dataset: &SourceIdentifier,
    ) -> Result<usize, ServiceError> {
        let structure = market_squawk_services::JsonStructureLimits::try_new(16, 4096, 64, 64)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let limits = ServiceLimits::try_new(4096, 8, 4096, 8, structure)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let context = RequestContext::new(
            market_squawk_services::RequestId::String(Arc::from(
                "test.treasury-all-history-source-identity",
            )),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
            limits,
        );
        let operation = self
            .acquire_treasury_all_history_operation(generation, provider_dataset, &context)
            .await?;
        Ok(Arc::as_ptr(&operation.source) as usize)
    }

    /// Consumes a bounded static adapter composition before publishing the coordinator.
    ///
    /// No post-publication static registration handle is retained or exposed.
    ///
    /// # Errors
    ///
    /// Rejects oversized, duplicate, changed, built-in, rights-mismatched, or registry-rejected
    /// registrations without returning a coordinator.
    pub fn try_new_with_prepublished_sources<I>(
        registry: AuthoritativeSourceRegistry,
        research: Arc<ResearchService>,
        limits: ResearchExtractionLimits,
        registrations: I,
    ) -> Result<Self, ResearchIngestCompositionError>
    where
        I: IntoIterator<Item = PrepublishedResearchSourceRegistration>,
    {
        let built_in_profiles = built_in_provider_profiles()
            .map_err(|_error| ResearchIngestCompositionError::ProviderProfilesUnavailable)?;
        let mut registrations_by_profile = BTreeMap::new();
        for registration in registrations {
            if registrations_by_profile.len() >= MAXIMUM_PREPUBLISHED_RESEARCH_SOURCES {
                return Err(ResearchIngestCompositionError::PrepublishedSourceLimitExceeded);
            }
            registration.validate(&built_in_profiles)?;
            if registrations_by_profile
                .insert(registration.profile.clone(), registration)
                .is_some()
            {
                return Err(ResearchIngestCompositionError::DuplicateProfile);
            }
        }

        let coordinator = Self::new(registry, research, limits);
        for registration in registrations_by_profile.into_values() {
            coordinator.register_prepublished_source(registration)?;
        }
        Ok(coordinator)
    }

    /// Constructs the coordinator and both disjoint runtime mutation capabilities.
    ///
    /// Generic provider activation receives only the first capability. The market-runtime
    /// registry owns the second capability for the one reserved Alpaca-history slot.
    pub(crate) fn try_new_with_runtime_authorities<I>(
        registry: AuthoritativeSourceRegistry,
        research: Arc<ResearchService>,
        limits: ResearchExtractionLimits,
        registrations: I,
    ) -> Result<
        (
            Arc<Self>,
            ResearchProviderRuntimeMutationAuthority,
            AlpacaHistoricalSourceMutationAuthority,
        ),
        ResearchIngestCompositionError,
    >
    where
        I: IntoIterator<Item = PrepublishedResearchSourceRegistration>,
    {
        let coordinator = Arc::new(Self::try_new_with_prepublished_sources(
            registry,
            research,
            limits,
            registrations,
        )?);
        let generic = ResearchProviderRuntimeMutationAuthority::new(Arc::clone(&coordinator));
        let alpaca = AlpacaHistoricalSourceMutationAuthority::new(Arc::clone(&coordinator));
        Ok((coordinator, generic, alpaca))
    }

    /// Captures registry tombstones and clean durable budget checkpoints under the sole
    /// coordinator mutation fence.
    ///
    /// Runtime adapters, discovery receipts, pending replacement handles, active request markers,
    /// and permits are intentionally absent from the returned owner-issued image.
    pub(crate) fn retain_provider_metadata_registry_backup(
        &self,
    ) -> Result<Box<[u8]>, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        authority
            .registry
            .as_ref()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .retain_clean_restart_backup_bytes()
            .map_err(Into::into)
    }

    fn register_source_inner<S>(
        &self,
        profile: SourceIdentifier,
        source: S,
        rights: ResearchRightsAuthority,
        generation: Option<ResearchProviderRuntimeGeneration>,
    ) -> Result<(), ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        let metadata = source.metadata().clone();
        self.register_source_capability_inner(
            profile,
            metadata,
            RegisteredSourceCapability::erased(source),
            rights,
            generation,
        )
    }

    fn register_prepublished_source(
        &self,
        registration: PrepublishedResearchSourceRegistration,
    ) -> Result<(), ResearchIngestCompositionError> {
        let PrepublishedResearchSourceRegistration {
            profile,
            metadata,
            source,
            rights,
        } = registration;
        self.register_source_capability_inner(
            profile,
            metadata,
            RegisteredSourceCapability::erased_arc(Arc::from(source)),
            rights,
            None,
        )
    }

    fn register_source_capability_inner(
        &self,
        profile: SourceIdentifier,
        metadata: SourceMetadata,
        capability: RegisteredSourceCapability,
        rights: ResearchRightsAuthority,
        generation: Option<ResearchProviderRuntimeGeneration>,
    ) -> Result<(), ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if metadata.source_id() != &rights.source_id {
            return Err(ResearchIngestCompositionError::SourceRightsMismatch);
        }
        if profile
            == alpaca_historical::canonical_alpaca_historical_profile()
                .map_err(|_error| ResearchIngestCompositionError::InvalidRuntimeGeneration)?
        {
            return Err(ResearchIngestCompositionError::BuiltInProfileRequiresOnboarding);
        }
        let registered_at = system_timestamp()
            .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
        let admission = ResearchProviderAdmission::new(generation.as_ref())?;
        let mut authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.lifecycle.shutdown_token().is_cancelled() || authority.registry.is_none() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.sources.contains_key(&profile)
            || authority.publication_sources.contains_key(&profile)
        {
            return Err(ResearchIngestCompositionError::DuplicateProfile);
        }
        let registration = authority
            .registry
            .as_mut()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .register_or_resume_exact(metadata.clone(), registered_at)?;
        let RegisteredSourceCapability { erased, typed } = capability;
        authority.sources.insert(
            profile,
            RegisteredExtractionSource {
                source: erased,
                typed_capability: typed,
                metadata,
                registration: Box::new(registration),
                rights,
                generation,
                admission,
            },
        );
        Ok(())
    }

    /// Returns whether this process currently owns a callable adapter for the exact profile.
    pub fn is_profile_registered(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<bool, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.registry.is_none() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        Ok(authority
            .sources
            .get(profile)
            .is_some_and(|source| source.admission.ensure_live().is_ok()))
    }

    /// Returns the exact fixed discovery dataset carried by one callable provider adapter.
    pub fn registered_discovery_dataset(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.registry.is_none() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let Some(source) = authority.sources.get(profile) else {
            return Ok(None);
        };
        if source.admission.ensure_live().is_err() {
            return Ok(None);
        }
        Ok(source.source.discovery_dataset_identifier().cloned())
    }

    /// Lists exact source objects from one registered provider without minting receipts.
    ///
    /// This read crosses the same registry, adapter, rights, metadata, cancellation, and deadline
    /// checks as receipt-minting discovery but retains no selection authority.
    pub async fn list_registered_objects(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
    ) -> Result<ResearchSourceObjectListing, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let operation_deadline = operation_deadline(context, self.limits.operation_duration)?;
        let operation = self.lifecycle.shutdown_token().child_token();
        let (prepared, discovery, _observed_monotonic, _observed_wall) = self
            .discover_registered_batch(
                profile,
                dataset,
                effective_at,
                max_results,
                context,
                &operation,
                operation_deadline,
            )
            .await?;
        ResearchSourceObjectListing::new(profile.clone(), prepared.metadata, discovery.batch)
    }

    /// Discovers exact source objects from one already registered provider profile.
    ///
    /// This is the public producer for [`ResearchIngestCoordinator::ingest`]. It uses the same
    /// registry-minted authority and returns bounded, request-bound objects with opaque,
    /// single-use selection receipts plus exact metadata and retained persistence-rights evidence.
    /// The adapter response body and any credential material remain behind their owning
    /// boundaries. Point-in-time discovery remains closed until the ingestion consumer can bind
    /// the same effective-time coordinate.
    ///
    /// # Errors
    ///
    /// Returns a typed service error when the profile is inactive, a point-in-time coordinate is
    /// supplied, the requested limit exceeds configured coordinator authority, retained rights
    /// expired, adapter lineage does not match registration, or cancellation, shutdown, or the
    /// deadline wins.
    pub async fn discover_registered_objects(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
    ) -> Result<ResearchSourceDiscovery, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let operation_deadline = operation_deadline(context, self.limits.operation_duration)?;
        let operation = self.lifecycle.shutdown_token().child_token();
        let (prepared, discovery, observed_monotonic, observed_wall) = self
            .discover_registered_batch(
                profile,
                dataset,
                effective_at,
                max_results,
                context,
                &operation,
                operation_deadline,
            )
            .await?;
        let mut authority = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        if authority.registry.is_none() {
            return Err(ServiceError::Unavailable);
        }
        let retained = authority.selections.mint(
            profile,
            &prepared.metadata,
            &prepared.rights,
            &prepared.admission,
            discovery.batch,
            discovery.capture_material,
            self.limits.discovery_receipt_retention,
            observed_monotonic,
            observed_wall,
            operation_deadline,
        );
        if matches!(retained, Err(ServiceError::DeadlineExceeded)) {
            operation.cancel();
        }
        retained
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one exact discovery request and its shared lifecycle bounds remain explicit"
    )]
    async fn discover_registered_batch(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
        operation: &CancellationToken,
        operation_deadline: Instant,
    ) -> Result<(PreparedExtraction, ManagedDiscovery, Instant, Timestamp), ServiceError> {
        if effective_at.is_some() || max_results.get() > self.limits.discovery_objects.get() {
            return Err(ServiceError::InvalidRequest);
        }
        let prepared = self.prepare(profile)?;
        prepared.rights.validate_at(system_timestamp()?)?;
        let subject = prepared
            .source
            .rights_subject(dataset)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        prepared.rights.validate_subject(subject.as_ref())?;
        let deadline = wall_deadline(operation_deadline, operation)?;
        let request = DiscoveryRequest::try_new(dataset.clone(), None, max_results, deadline)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let discovery = await_extraction(
            prepared.source.discover_managed(
                prepared.authority.clone(),
                request,
                operation.clone(),
            ),
            context,
            operation,
            &prepared.admission,
            operation_deadline,
        )
        .await?;
        ensure_operation_live(operation_deadline, operation)?;
        prepared
            .admission
            .ensure_live()
            .map_err(|_error| ServiceError::Unavailable)?;
        if discovery.batch.objects().iter().any(|object| {
            object.source_id() != prepared.metadata.source_id()
                || object.metadata_revision() != prepared.metadata.revision()
        }) {
            return Err(ServiceError::InvalidResult);
        }
        let observed_monotonic = Instant::now();
        let observed_wall = system_timestamp()?;
        prepared.rights.validate_at(observed_wall)?;
        Ok((prepared, discovery, observed_monotonic, observed_wall))
    }

    /// Extracts one exact object from an already registered profile without analytical
    /// publication.
    ///
    /// The same registry-minted source authority, bounded discovery, exact object selection,
    /// extraction limits, revision validation, rights-expiry check, cancellation, and deadline
    /// handling used by [`ResearchIngestCoordinator::ingest`] are applied before the batch is
    /// returned. This boundary does not write a research dataset.
    ///
    /// # Errors
    ///
    /// Returns a typed service error when the profile or object is absent, authority or rights are
    /// invalid, results are ambiguous, a source violates its contract, or cancellation, shutdown,
    /// or the deadline wins.
    pub async fn extract_registered_batch(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        context: &RequestContext,
    ) -> Result<ExtractionBatch, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let operation_deadline = operation_deadline(context, self.limits.operation_duration)?;
        let operation = self.lifecycle.shutdown_token().child_token();
        let extracted = self
            .extract_exact(
                profile,
                dataset,
                object_id,
                context,
                &operation,
                operation_deadline,
            )
            .await?;
        extracted
            .publication
            .into_local_batch()
            .ok_or(ServiceError::InvalidResult)
    }

    fn prepare(&self, profile: &SourceIdentifier) -> Result<PreparedExtraction, ServiceError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        let registry = authority
            .registry
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let registered = authority
            .sources
            .get(profile)
            .ok_or(ServiceError::NotFound)?;
        registered
            .admission
            .ensure_live()
            .map_err(|_error| ServiceError::Unavailable)?;
        let extraction = registry
            .extraction_authority(&registered.registration, registered.source.as_ref())
            .map_err(map_registry_error)?;
        Ok(PreparedExtraction {
            source: Arc::clone(&registered.source),
            metadata: registered.metadata.clone(),
            rights: registered.rights.clone(),
            authority: extraction,
            admission: registered.admission.clone(),
        })
    }

    async fn extract_exact(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        context: &RequestContext,
        operation: &CancellationToken,
        operation_deadline: Instant,
    ) -> Result<AuthorizedExtraction, ServiceError> {
        let prepared = self.prepare(profile)?;
        let deadline = wall_deadline(operation_deadline, operation)?;
        let discovery_request = DiscoveryRequest::try_new(
            dataset.clone(),
            None,
            self.limits.discovery_objects,
            deadline,
        )
        .map_err(|_error| ServiceError::InvalidRequest)?;
        let discovery = await_extraction(
            prepared.source.discover_managed(
                prepared.authority.clone(),
                discovery_request,
                operation.clone(),
            ),
            context,
            operation,
            &prepared.admission,
            operation_deadline,
        )
        .await?;
        let mut matches = discovery
            .batch
            .objects()
            .iter()
            .filter(|object| object.object_id() == object_id);
        let object = matches.next().cloned().ok_or(ServiceError::NotFound)?;
        if matches.next().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        self.extract_prepared_object(
            prepared,
            object,
            discovery.capture_material,
            context,
            operation,
            operation_deadline,
            deadline,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "single-use receipt coordinates and one operation deadline remain explicit"
    )]
    async fn extract_selected(
        &self,
        receipt: &str,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        context: &RequestContext,
        operation: &CancellationToken,
        operation_deadline: Instant,
    ) -> Result<AuthorizedExtraction, ServiceError> {
        ensure_operation_live(operation_deadline, operation)?;
        let observed_monotonic = Instant::now();
        let observed_wall = system_timestamp()?;
        let prepared = {
            let mut authority = self
                .authority
                .lock()
                .map_err(|_error| ServiceError::Unavailable)?;
            authority.consume_discovery_selection(
                receipt,
                profile,
                dataset,
                object_id,
                observed_monotonic,
                observed_wall,
            )?
        };
        let deadline = wall_deadline(operation_deadline, operation)?;
        let PreparedRetainedSelection {
            source,
            metadata,
            rights,
            authority,
            object,
            admission,
            capture_material,
        } = prepared;
        self.extract_prepared_object(
            PreparedExtraction {
                source,
                metadata,
                rights,
                authority,
                admission,
            },
            object,
            capture_material,
            context,
            operation,
            operation_deadline,
            deadline,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact selection and one operation deadline remain explicit"
    )]
    async fn extract_prepared_object(
        &self,
        prepared: PreparedExtraction,
        object: SourceObject,
        discovery_capture: Option<ProviderCaptureMaterial>,
        context: &RequestContext,
        operation: &CancellationToken,
        operation_deadline: Instant,
        deadline: Timestamp,
    ) -> Result<AuthorizedExtraction, ServiceError> {
        self.extract_prepared_object_diagnostic(
            prepared,
            object,
            discovery_capture,
            context,
            operation,
            operation_deadline,
            deadline,
        )
        .await
        .map_err(ProviderOperationDiagnostic::service_error)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact extraction inputs and one diagnostic phase remain explicit"
    )]
    async fn extract_prepared_object_diagnostic(
        &self,
        prepared: PreparedExtraction,
        object: SourceObject,
        discovery_capture: Option<ProviderCaptureMaterial>,
        context: &RequestContext,
        operation: &CancellationToken,
        operation_deadline: Instant,
        deadline: Timestamp,
    ) -> Result<AuthorizedExtraction, ProviderOperationDiagnostic> {
        let phase = ProviderOperationPhase::Extraction;
        let service_diagnostic = |error| ProviderOperationDiagnostic::from_service(phase, error);
        let invalid_result = || {
            ProviderOperationDiagnostic::new(phase, ProviderOperationFailureClass::InvalidResult)
        };
        let unauthorized =
            || ProviderOperationDiagnostic::new(phase, ProviderOperationFailureClass::Unauthorized);
        prepared
            .rights
            .validate_at(system_timestamp().map_err(service_diagnostic)?)
            .map_err(|_error| unauthorized())?;
        let subject = prepared
            .source
            .rights_subject(object.dataset())
            .map_err(|_error| {
                ProviderOperationDiagnostic::new(
                    phase,
                    ProviderOperationFailureClass::InvalidRequest,
                )
            })?;
        prepared
            .rights
            .validate_subject(subject.as_ref())
            .map_err(|_error| unauthorized())?;
        let extraction_request =
            ExtractionRequest::try_new(object, self.limits.records, self.limits.bytes, deadline)
                .map_err(|_error| {
                    ProviderOperationDiagnostic::new(
                        phase,
                        ProviderOperationFailureClass::InvalidRequest,
                    )
                })?;
        let capture =
            ManagedProviderCaptureAuthority::new(Arc::clone(&self.research), operation_deadline);
        let managed = await_extraction_diagnostic(
            prepared.source.extract_managed_with_native(
                capture,
                prepared.authority,
                extraction_request,
                operation.clone(),
            ),
            context,
            operation,
            &prepared.admission,
            operation_deadline,
            phase,
        )
        .await?;
        let local_source = matches!(
            prepared.metadata.source_class(),
            SourceClass::LocalFile | SourceClass::PortfolioExport
        );
        let (publication, company_identity, revisions) = match managed.handoff {
            ManagedExtractionHandoff::Pending {
                extraction:
                    ManagedExtraction {
                        batch,
                        company_identity,
                        capture_material: extraction_capture,
                    },
                provider_native,
            } => {
                let (batch, capture_material) = match (discovery_capture, extraction_capture) {
                    (Some(_), Some(_)) => return Err(invalid_result()),
                    (Some(capture_material), None) => {
                        let batch = batch
                            .try_bind_provider_capture(capture_material.receipt())
                            .map_err(|_error| invalid_result())?;
                        (batch, Some(capture_material))
                    }
                    (None, capture_material) => (batch, capture_material),
                };
                if local_source == capture_material.is_some()
                    || !capture_material
                        .as_ref()
                        .is_none_or(|capture| capture_material_matches_batch(capture, &batch))
                {
                    return Err(invalid_result());
                }
                let revisions = prepared
                    .source
                    .revision_plan(&batch)
                    .map_err(|_error| invalid_result())?;
                let publication = match (local_source, capture_material, provider_native) {
                    (true, None, None) => ManagedPublication::Local(batch),
                    (false, Some(capture_material), provider_native) => {
                        ManagedPublication::PendingProvider(ManagedPendingProviderPublication {
                            batch,
                            capture_material,
                            provider_native,
                        })
                    }
                    _ => return Err(invalid_result()),
                };
                (publication, company_identity, revisions)
            }
            ManagedExtractionHandoff::Provider {
                sealed_capture,
                revisions,
            } => {
                if local_source || discovery_capture.is_some() || sealed_capture.validate().is_err()
                {
                    return Err(invalid_result());
                }
                (
                    ManagedPublication::Provider(sealed_capture),
                    None,
                    Some(revisions),
                )
            }
        };
        let batch = publication.batch();
        let analytical_dataset = prepared
            .source
            .analytical_dataset(batch)
            .map_err(|_error| invalid_result())?;
        let payload_digest = extraction_provider_payload_digest(batch);
        let retrieved_at = system_timestamp().map_err(service_diagnostic)?;
        let rights = prepared
            .rights
            .decision(payload_digest, retrieved_at)
            .map_err(|_error| unauthorized())?;
        ensure_operation_live(operation_deadline, operation).map_err(service_diagnostic)?;
        prepared.admission.ensure_live().map_err(|_error| {
            ProviderOperationDiagnostic::new(
                phase,
                ProviderOperationFailureClass::ProviderAuthority,
            )
        })?;
        Ok(AuthorizedExtraction {
            metadata: prepared.metadata,
            publication,
            company_identity,
            revisions,
            analytical_dataset,
            payload_digest,
            rights,
            admission: prepared.admission,
        })
    }

    fn close_registry(&self) -> Result<(), ServiceError> {
        let registry = {
            let mut authority = self
                .authority
                .lock()
                .map_err(|_error| ServiceError::Unavailable)?;
            authority.selections.clear();
            authority.pending_replacements.clear();
            for registered in authority.sources.values() {
                registered.admission.revoke();
            }
            authority.sources.clear();
            authority.registry.take()
        };
        registry
            .map(AuthoritativeSourceRegistry::shutdown)
            .transpose()
            .map(|_closed| ())
            .map_err(map_registry_error)
    }
}

impl std::fmt::Debug for ProductionResearchIngestCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let configured_sources = self
            .authority
            .lock()
            .map(|authority| authority.sources.len())
            .unwrap_or_default();
        formatter
            .debug_struct("ProductionResearchIngestCoordinator")
            .field("research", &"[ANALYTICAL AUTHORITY]")
            .field("limits", &self.limits)
            .field("lifecycle", &self.lifecycle)
            .field("configured_sources", &configured_sources)
            .finish()
    }
}

#[async_trait]
impl ResearchSourceDiscoveryCoordinator for ProductionResearchIngestCoordinator {
    fn maximum_discovery_objects(&self) -> NonZeroU16 {
        self.limits.discovery_objects
    }

    fn registered_discovery_dataset(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ServiceError> {
        ProductionResearchIngestCoordinator::registered_discovery_dataset(self, profile)
            .map_err(|_error| ServiceError::Unavailable)
    }

    fn revoke_discovery_receipts(
        &self,
        discovery: &ResearchSourceDiscovery,
    ) -> Result<(), ServiceError> {
        self.authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .selections
            .revoke(discovery)
    }

    async fn list_registered_objects(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
    ) -> Result<ResearchSourceObjectListing, ServiceError> {
        ProductionResearchIngestCoordinator::list_registered_objects(
            self,
            profile,
            dataset,
            effective_at,
            max_results,
            context,
        )
        .await
    }

    async fn discover_registered_objects(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
    ) -> Result<ResearchSourceDiscovery, ServiceError> {
        ProductionResearchIngestCoordinator::discover_registered_objects(
            self,
            profile,
            dataset,
            effective_at,
            max_results,
            context,
        )
        .await
    }
}

struct PreparedExtraction {
    source: Arc<dyn ManagedResearchExtractionSource>,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
    authority: market_squawk_sources::ExtractionAuthority,
    admission: ResearchProviderAdmission,
}

enum ManagedPublication {
    Local(ExtractionBatch),
    PendingProvider(ManagedPendingProviderPublication),
    Provider(SealedProviderCaptureBinding),
}

impl ManagedPublication {
    const fn batch(&self) -> &ExtractionBatch {
        match self {
            Self::Local(batch) => batch,
            Self::PendingProvider(pending) => &pending.batch,
            Self::Provider(sealed_capture) => sealed_capture.batch(),
        }
    }

    fn into_local_batch(self) -> Option<ExtractionBatch> {
        match self {
            Self::Local(batch) => Some(batch),
            Self::PendingProvider(_) | Self::Provider(_) => None,
        }
    }

    fn into_pending_provider(self) -> Option<ManagedPendingProviderPublication> {
        match self {
            Self::PendingProvider(pending) => Some(pending),
            Self::Local(_) | Self::Provider(_) => None,
        }
    }
}

struct ManagedPendingProviderPublication {
    batch: ExtractionBatch,
    capture_material: ProviderCaptureMaterial,
    provider_native: Option<ManagedProviderNativePublication>,
}

struct AuthorizedExtraction {
    metadata: SourceMetadata,
    publication: ManagedPublication,
    company_identity: Option<CompanyIdentityObservation>,
    revisions: Option<ExtractionRevisionPlan>,
    analytical_dataset: DatasetId,
    payload_digest: EvidenceDigest,
    rights: RightsDecisionInput,
    admission: ResearchProviderAdmission,
}

struct ChainedIngestPrecommitAuthority {
    provider: Arc<dyn IngestPrecommitAuthority>,
    additional: Arc<dyn ResearchIngestCommitAuthority>,
}

impl fmt::Debug for ChainedIngestPrecommitAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChainedIngestPrecommitAuthority")
            .field("provider", &"[PROVIDER PUBLICATION AUTHORITY]")
            .field("additional", &"[ADDITIONAL COMMIT AUTHORITY]")
            .finish()
    }
}

impl IngestPrecommitAuthority for ChainedIngestPrecommitAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.provider.validate_precommit()?;
        self.additional.validate_precommit()
    }
}

impl ProductionResearchIngestCoordinator {
    async fn ingest_inner(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
        additional: Option<Arc<dyn ResearchIngestCommitAuthority>>,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let operation_deadline = operation_deadline(context, self.limits.operation_duration)?;
        let profile = required_identifier(request, "provider")?;
        let dataset = required_identifier(request, "dataset")?;
        let object_id = required_identifier(request, "object")?;
        let receipt = required_string(request, "discoveryReceipt")?;
        let operation = self.lifecycle.shutdown_token().child_token();
        let extracted = self
            .extract_selected(
                receipt,
                &profile,
                &dataset,
                &object_id,
                context,
                &operation,
                operation_deadline,
            )
            .await?;
        let AuthorizedExtraction {
            metadata: source_metadata,
            publication,
            company_identity,
            revisions,
            analytical_dataset,
            payload_digest,
            rights,
            admission,
        } = extracted;
        let provider: Arc<dyn IngestPrecommitAuthority> = Arc::new(
            admission
                .acquire_publication_lease()
                .await
                .map_err(|_error| ServiceError::Unavailable)?,
        );
        let precommit: Arc<dyn IngestPrecommitAuthority> = match &additional {
            Some(additional) => Arc::new(ChainedIngestPrecommitAuthority {
                provider,
                additional: Arc::clone(additional),
            }),
            None => provider,
        };
        let ingest = match (publication, revisions) {
            (ManagedPublication::Provider(sealed_capture), Some(revisions)) => {
                ResearchIngestRequest::with_provider_publication(
                    source_metadata.clone(),
                    rights,
                    analytical_dataset,
                    sealed_capture,
                    revisions,
                )
            }
            (ManagedPublication::Local(batch), None) => ResearchIngestRequest::locally_observed(
                source_metadata.clone(),
                rights,
                analytical_dataset,
                batch,
            ),
            (ManagedPublication::PendingProvider(_), Some(_)) => {
                return Err(ServiceError::Unavailable);
            }
            _ => return Err(ServiceError::InvalidResult),
        }
        .map_err(map_research_error)?;
        let ingest = match company_identity {
            Some(company_identity) => ingest
                .with_company_identity(company_identity)
                .map_err(map_research_error)?,
            None => ingest,
        }
        .with_precommit_authority(precommit);
        let committed = await_publication(
            self.research.ingest(ingest, operation.clone()),
            context,
            &operation,
            &admission,
            operation_deadline,
        )
        .await?;
        if let Some(additional) = &additional {
            additional.commit_succeeded();
        }
        let manifest = committed.manifest();
        let plan = committed.pinned().plan();
        let coverage = json!({
            "sourceId": source_metadata.source_id(),
            "provider": source_metadata.provider(),
            "profile": profile,
            "providerDataset": dataset,
            "objectId": object_id,
            "metadataRevision": source_metadata.revision(),
            "payloadDigest": encode_hex(payload_digest.bytes()),
            "manifest": manifest_value(manifest),
        });
        let quality = json!({
            "qualityCeiling": source_metadata.quality_ceiling(),
            "recordLevelProvenance": true,
            "executionEligible": false,
        });
        let metadata = ToolResultMetadata::try_complete(coverage, quality)
            .map_err(|_error| ServiceError::InvalidResult)?;
        let content = json!({
            "manifest": manifest_value(manifest),
            "rowCount": plan.row_count(),
            "totalBytes": plan.total_bytes(),
            "objectCount": plan.objects().len(),
            "lineageDigest": encode_hex(plan.lineage_digest().bytes()),
        });
        let result = TypedToolResult::try_new(content, 1, metadata, limits)?;
        ensure_operation_live(operation_deadline, &operation)?;
        admission
            .ensure_live()
            .map_err(|_error| ServiceError::Unavailable)?;
        Ok(result)
    }
}

#[async_trait]
impl ResearchIngestCoordinator for ProductionResearchIngestCoordinator {
    async fn ingest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        self.ingest_inner(request, context, limits, None).await
    }

    async fn ingest_with_precommit(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
        additional: Arc<dyn ResearchIngestCommitAuthority>,
    ) -> Result<TypedToolResult, ServiceError> {
        self.ingest_inner(request, context, limits, Some(additional))
            .await
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.lifecycle.finish_shutdown(deadline).await?;
        alpaca_historical::drain_before_registry_close(self, deadline)
            .await
            .map_err(|error| match error {
                AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded => {
                    ServiceError::DeadlineExceeded
                }
                AlpacaHistoricalSourceSlotError::WaitCancelled => ServiceError::Cancelled,
                _ => ServiceError::Unavailable,
            })?;
        self.close_registry()
    }
}

/// Internal operation phase retained only by provider diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderOperationPhase {
    Runtime,
    Discovery,
    Extraction,
    RawSeal,
}

/// Closed, payload-free cause retained after provider and storage errors cross the app boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderOperationFailureClass {
    Cancelled,
    DeadlineExceeded,
    Unauthorized,
    InvalidRequest,
    NotFound,
    InvalidResult,
    Capacity,
    RuntimeUnavailable,
    ProviderNetwork,
    ProviderNetworkPolicy,
    ProviderUnavailable,
    ProviderRateLimited,
    ProviderBudgetUnavailable,
    ProviderResponseLimit,
    ProviderProtocol,
    ProviderGenerationChanged,
    ProviderAuthority,
    TrustedTime,
    CaptureUnavailable,
    StorageUnavailable,
    PublicationAuthority,
    Internal,
}

/// Secret-free internal detail for logs and diagnostics, never an application response DTO.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ProviderOperationDiagnostic {
    phase: ProviderOperationPhase,
    failure: ProviderOperationFailureClass,
    protocol: Option<SourceProtocolViolation>,
}

impl ProviderOperationDiagnostic {
    const fn new(phase: ProviderOperationPhase, failure: ProviderOperationFailureClass) -> Self {
        Self {
            phase,
            failure,
            protocol: None,
        }
    }

    fn from_extraction(phase: ProviderOperationPhase, error: ExtractionSourceError) -> Self {
        Self {
            phase,
            failure: classify_extraction_error(error),
            protocol: None,
        }
    }

    fn from_managed_discovery(
        phase: ProviderOperationPhase,
        error: ManagedDiscoveryDiagnosticError,
    ) -> Self {
        let mut diagnostic = Self::from_extraction(phase, error.source);
        diagnostic.protocol = error.protocol;
        diagnostic
    }

    const fn from_service(phase: ProviderOperationPhase, error: ServiceError) -> Self {
        let failure = match error {
            ServiceError::InvalidRequest => ProviderOperationFailureClass::InvalidRequest,
            ServiceError::NotFound => ProviderOperationFailureClass::NotFound,
            ServiceError::Unauthorized => ProviderOperationFailureClass::Unauthorized,
            ServiceError::ResourceExhausted => ProviderOperationFailureClass::Capacity,
            ServiceError::Cancelled => ProviderOperationFailureClass::Cancelled,
            ServiceError::DeadlineExceeded => ProviderOperationFailureClass::DeadlineExceeded,
            ServiceError::Unavailable => ProviderOperationFailureClass::RuntimeUnavailable,
            ServiceError::InvalidResult => ProviderOperationFailureClass::InvalidResult,
            ServiceError::Internal => ProviderOperationFailureClass::Internal,
        };
        Self::new(phase, failure)
    }

    const fn service_error(self) -> ServiceError {
        match self.failure {
            ProviderOperationFailureClass::Cancelled => ServiceError::Cancelled,
            ProviderOperationFailureClass::DeadlineExceeded => ServiceError::DeadlineExceeded,
            ProviderOperationFailureClass::Unauthorized => ServiceError::Unauthorized,
            ProviderOperationFailureClass::InvalidRequest => ServiceError::InvalidRequest,
            ProviderOperationFailureClass::NotFound => ServiceError::NotFound,
            ProviderOperationFailureClass::InvalidResult => ServiceError::InvalidResult,
            ProviderOperationFailureClass::Capacity => ServiceError::ResourceExhausted,
            ProviderOperationFailureClass::Internal => ServiceError::Internal,
            ProviderOperationFailureClass::RuntimeUnavailable
            | ProviderOperationFailureClass::ProviderNetwork
            | ProviderOperationFailureClass::ProviderNetworkPolicy
            | ProviderOperationFailureClass::ProviderUnavailable
            | ProviderOperationFailureClass::ProviderRateLimited
            | ProviderOperationFailureClass::ProviderBudgetUnavailable
            | ProviderOperationFailureClass::ProviderResponseLimit
            | ProviderOperationFailureClass::ProviderProtocol
            | ProviderOperationFailureClass::ProviderGenerationChanged
            | ProviderOperationFailureClass::ProviderAuthority
            | ProviderOperationFailureClass::TrustedTime
            | ProviderOperationFailureClass::CaptureUnavailable
            | ProviderOperationFailureClass::StorageUnavailable
            | ProviderOperationFailureClass::PublicationAuthority => ServiceError::Unavailable,
        }
    }
}

impl fmt::Display for ProviderOperationDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provider operation failed")
    }
}

impl fmt::Debug for ProviderOperationDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut diagnostic = formatter.debug_struct("ProviderOperationDiagnostic");
        diagnostic
            .field("phase", &self.phase)
            .field("failure", &self.failure);
        if let Some(protocol) = self.protocol {
            diagnostic.field("protocol", &protocol);
        }
        diagnostic.finish()
    }
}

impl std::error::Error for ProviderOperationDiagnostic {}

trait IntoProviderOperationDiagnostic {
    fn into_provider_operation_diagnostic(
        self,
        phase: ProviderOperationPhase,
    ) -> ProviderOperationDiagnostic;
}

impl IntoProviderOperationDiagnostic for ExtractionSourceError {
    fn into_provider_operation_diagnostic(
        self,
        phase: ProviderOperationPhase,
    ) -> ProviderOperationDiagnostic {
        ProviderOperationDiagnostic::from_extraction(phase, self)
    }
}

impl IntoProviderOperationDiagnostic for ManagedDiscoveryDiagnosticError {
    fn into_provider_operation_diagnostic(
        self,
        phase: ProviderOperationPhase,
    ) -> ProviderOperationDiagnostic {
        ProviderOperationDiagnostic::from_managed_discovery(phase, self)
    }
}

fn operation_state_error(deadline: Instant, cancellation: &CancellationToken) -> ServiceError {
    if cancellation.is_cancelled() {
        ServiceError::Cancelled
    } else if Instant::now() >= deadline {
        ServiceError::DeadlineExceeded
    } else {
        ServiceError::Unavailable
    }
}

fn map_treasury_restore_error(
    error: market_squawk_adapter_treasury::TreasurySourceError,
) -> ServiceError {
    match error {
        market_squawk_adapter_treasury::TreasurySourceError::Cancelled => ServiceError::Cancelled,
        market_squawk_adapter_treasury::TreasurySourceError::DeadlineExceeded => {
            ServiceError::DeadlineExceeded
        }
        _ => ServiceError::Unavailable,
    }
}

async fn await_extraction<T>(
    future: impl Future<Output = Result<T, ExtractionSourceError>>,
    context: &RequestContext,
    operation: &CancellationToken,
    admission: &ResearchProviderAdmission,
    operation_deadline: Instant,
) -> Result<T, ServiceError> {
    await_extraction_diagnostic(
        future,
        context,
        operation,
        admission,
        operation_deadline,
        ProviderOperationPhase::Extraction,
    )
    .await
    .map_err(ProviderOperationDiagnostic::service_error)
}

async fn await_extraction_diagnostic<T, Error>(
    future: impl Future<Output = Result<T, Error>>,
    context: &RequestContext,
    operation: &CancellationToken,
    admission: &ResearchProviderAdmission,
    operation_deadline: Instant,
    phase: ProviderOperationPhase,
) -> Result<T, ProviderOperationDiagnostic>
where
    Error: IntoProviderOperationDiagnostic,
{
    if Instant::now() >= operation_deadline {
        operation.cancel();
        return Err(ProviderOperationDiagnostic::new(
            phase,
            ProviderOperationFailureClass::DeadlineExceeded,
        ));
    }
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => {
            operation.cancel();
            Err(ProviderOperationDiagnostic::new(
                phase,
                ProviderOperationFailureClass::Cancelled,
            ))
        }
        () = operation.cancelled() => Err(ProviderOperationDiagnostic::new(
            phase,
            ProviderOperationFailureClass::RuntimeUnavailable,
        )),
        () = admission.cancellation().cancelled() => {
            operation.cancel();
            Err(ProviderOperationDiagnostic::new(
                phase,
                ProviderOperationFailureClass::ProviderAuthority,
            ))
        }
        () = tokio::time::sleep_until(operation_deadline.into()) => {
            operation.cancel();
            Err(ProviderOperationDiagnostic::new(
                phase,
                ProviderOperationFailureClass::DeadlineExceeded,
            ))
        }
        result = future => result.map_err(|error| {
            error.into_provider_operation_diagnostic(phase)
        }),
    }
}

async fn await_publication<T>(
    future: impl Future<Output = Result<T, ResearchServiceError>>,
    context: &RequestContext,
    operation: &CancellationToken,
    admission: &ResearchProviderAdmission,
    operation_deadline: Instant,
) -> Result<T, ServiceError> {
    await_publication_diagnostic(
        future,
        context,
        operation,
        admission,
        operation_deadline,
        ProviderOperationPhase::RawSeal,
    )
    .await
    .map_err(ProviderOperationDiagnostic::service_error)
}

async fn await_publication_diagnostic<T>(
    future: impl Future<Output = Result<T, ResearchServiceError>>,
    context: &RequestContext,
    operation: &CancellationToken,
    admission: &ResearchProviderAdmission,
    operation_deadline: Instant,
    phase: ProviderOperationPhase,
) -> Result<T, ProviderOperationDiagnostic> {
    if Instant::now() >= operation_deadline {
        operation.cancel();
        return Err(ProviderOperationDiagnostic::new(
            phase,
            ProviderOperationFailureClass::DeadlineExceeded,
        ));
    }
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => {
            operation.cancel();
            Err(ProviderOperationDiagnostic::new(
                phase,
                ProviderOperationFailureClass::Cancelled,
            ))
        }
        () = operation.cancelled() => Err(ProviderOperationDiagnostic::new(
            phase,
            ProviderOperationFailureClass::RuntimeUnavailable,
        )),
        () = admission.cancellation().cancelled() => {
            operation.cancel();
            Err(ProviderOperationDiagnostic::new(
                phase,
                ProviderOperationFailureClass::ProviderAuthority,
            ))
        }
        () = tokio::time::sleep_until(operation_deadline.into()) => {
            operation.cancel();
            Err(ProviderOperationDiagnostic::new(
                phase,
                ProviderOperationFailureClass::DeadlineExceeded,
            ))
        }
        result = future => result.map_err(|error| {
            ProviderOperationDiagnostic::new(phase, classify_research_error(error))
        }),
    }
}

fn required_identifier(
    request: &TypedToolRequest,
    field: &str,
) -> Result<SourceIdentifier, ServiceError> {
    request
        .arguments()
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(|value| {
            SourceIdentifier::try_from(value).map_err(|_error| ServiceError::InvalidRequest)
        })
}

fn required_string<'a>(
    request: &'a TypedToolRequest,
    field: &str,
) -> Result<&'a str, ServiceError> {
    request
        .arguments()
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

fn operation_deadline(
    context: &RequestContext,
    maximum_duration: Duration,
) -> Result<Instant, ServiceError> {
    let now = Instant::now();
    if now >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let coordinator_deadline = now
        .checked_add(maximum_duration)
        .ok_or(ServiceError::Internal)?;
    Ok(context.deadline().min(coordinator_deadline))
}

fn ensure_operation_live(
    operation_deadline: Instant,
    operation: &CancellationToken,
) -> Result<(), ServiceError> {
    if Instant::now() >= operation_deadline {
        operation.cancel();
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

fn wall_deadline(
    operation_deadline: Instant,
    operation: &CancellationToken,
) -> Result<Timestamp, ServiceError> {
    let now = Instant::now();
    if now >= operation_deadline {
        operation.cancel();
        return Err(ServiceError::DeadlineExceeded);
    }
    let remaining = operation_deadline.saturating_duration_since(now);
    let nanos = i64::try_from(remaining.as_nanos()).map_err(|_error| ServiceError::Internal)?;
    system_timestamp()?
        .checked_add_nanos(nanos)
        .map_err(|_error| ServiceError::Internal)
}

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ServiceError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_error| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn classify_extraction_error(error: ExtractionSourceError) -> ProviderOperationFailureClass {
    match error {
        ExtractionSourceError::DeadlineExceeded => ProviderOperationFailureClass::DeadlineExceeded,
        ExtractionSourceError::Cancelled
        | ExtractionSourceError::Source(SourceError::Cancelled) => {
            ProviderOperationFailureClass::Cancelled
        }
        ExtractionSourceError::Source(error) => classify_source_error(error),
        ExtractionSourceError::Contract(_) => ProviderOperationFailureClass::InvalidRequest,
        ExtractionSourceError::Authority(error) => classify_extraction_authority_error(error),
    }
}

fn classify_source_error(error: SourceError) -> ProviderOperationFailureClass {
    match error {
        SourceError::Unauthorized => ProviderOperationFailureClass::Unauthorized,
        SourceError::Cancelled => ProviderOperationFailureClass::Cancelled,
        SourceError::Network | SourceError::ConnectionIdle => {
            ProviderOperationFailureClass::ProviderNetwork
        }
        SourceError::ProviderUnavailable => ProviderOperationFailureClass::ProviderUnavailable,
        SourceError::BudgetWaitUntil { .. } => ProviderOperationFailureClass::ProviderRateLimited,
        SourceError::BudgetUnavailable { .. } => {
            ProviderOperationFailureClass::ProviderBudgetUnavailable
        }
        SourceError::GenerationResynchronizationRequired => {
            ProviderOperationFailureClass::ProviderGenerationChanged
        }
        SourceError::SessionNotCurrent | SourceError::GenerationAuthorityMismatch => {
            ProviderOperationFailureClass::ProviderAuthority
        }
        SourceError::TrustedTimeUnavailable | SourceError::TrustedTimeDiscontinuity => {
            ProviderOperationFailureClass::TrustedTime
        }
        SourceError::Sink(_) | SourceError::CaptureNotHealthy => {
            ProviderOperationFailureClass::CaptureUnavailable
        }
        SourceError::FrameTooLarge { .. } => ProviderOperationFailureClass::ProviderResponseLimit,
        SourceError::InvalidProtocolState | SourceError::FrameIdentityExhausted => {
            ProviderOperationFailureClass::ProviderProtocol
        }
    }
}

fn classify_extraction_authority_error(
    error: ExtractionAuthorityError,
) -> ProviderOperationFailureClass {
    match error {
        ExtractionAuthorityError::NotCurrent | ExtractionAuthorityError::NotEffective => {
            ProviderOperationFailureClass::ProviderAuthority
        }
        ExtractionAuthorityError::TrustedTimeUnavailable
        | ExtractionAuthorityError::TrustedTimeDiscontinuous => {
            ProviderOperationFailureClass::TrustedTime
        }
        ExtractionAuthorityError::NetworkDenied
        | ExtractionAuthorityError::NetworkPolicy(_)
        | ExtractionAuthorityError::RequestTargetMismatch => {
            ProviderOperationFailureClass::ProviderNetworkPolicy
        }
        ExtractionAuthorityError::BudgetWaitUntil { .. } => {
            ProviderOperationFailureClass::ProviderRateLimited
        }
        ExtractionAuthorityError::BudgetNotConfigured
        | ExtractionAuthorityError::BudgetUnavailable { .. } => {
            ProviderOperationFailureClass::ProviderBudgetUnavailable
        }
    }
}

fn map_registry_error(_error: RegistryError) -> ServiceError {
    ServiceError::Unavailable
}

fn classify_ingest_error(error: IngestError) -> ProviderOperationFailureClass {
    match error {
        IngestError::Cancelled => ProviderOperationFailureClass::Cancelled,
        IngestError::DeadlineExceeded => ProviderOperationFailureClass::DeadlineExceeded,
        IngestError::RevisionEvidenceMismatch
        | IngestError::RevisionEvidenceRequired
        | IngestError::InvalidDataset
        | IngestError::InvalidProviderMacroPlan
        | IngestError::ProviderLogicalFundRequired
        | IngestError::ContentIdentity(_) => ProviderOperationFailureClass::InvalidResult,
        IngestError::PublicationAuthorityRevoked | IngestError::AuthorityTransitionRejected => {
            ProviderOperationFailureClass::PublicationAuthority
        }
        IngestError::Parquet(_)
        | IngestError::Manifest(_)
        | IngestError::Catalog(_)
        | IngestError::SecFundJob(_)
        | IngestError::ListingReference(_) => ProviderOperationFailureClass::StorageUnavailable,
        IngestError::ProviderCaptureRequired
        | IngestError::ProviderCapture(_)
        | IngestError::SealedProviderCapture(_)
        | IngestError::ProviderMarketEventSelection(_)
        | IngestError::ProviderCaptureRecoveryWorkerUnavailable => {
            ProviderOperationFailureClass::CaptureUnavailable
        }
        IngestError::AuthorityLockPoisoned => ProviderOperationFailureClass::RuntimeUnavailable,
        IngestError::Plan(_)
        | IngestError::Arrow(_)
        | IngestError::Serialization(_)
        | IngestError::RevisionAuthority(_)
        | IngestError::CatalogCompositionMismatch
        | IngestError::UnknownSource
        | IngestError::UnknownReservation
        | IngestError::ReservationPayloadMismatch
        | IngestError::PersistRightsRequired
        | IngestError::TerminalRun
        | IngestError::IncompleteSuccessfulRun
        | IngestError::ReplayConflict => ProviderOperationFailureClass::PublicationAuthority,
    }
}

fn map_research_error(error: ResearchServiceError) -> ServiceError {
    ProviderOperationDiagnostic::new(
        ProviderOperationPhase::RawSeal,
        classify_research_error(error),
    )
    .service_error()
}

fn classify_research_error(error: ResearchServiceError) -> ProviderOperationFailureClass {
    match error {
        ResearchServiceError::Ingest(error) => classify_ingest_error(error),
        ResearchServiceError::Rights(_) | ResearchServiceError::IngestAuthorityMismatch => {
            ProviderOperationFailureClass::Unauthorized
        }
        ResearchServiceError::IdentityOverflow => ProviderOperationFailureClass::Internal,
        ResearchServiceError::Path(_)
        | ResearchServiceError::Catalog(_)
        | ResearchServiceError::Manifest(_)
        | ResearchServiceError::ProviderCaptureStore(_)
        | ResearchServiceError::Dataset(_) => ProviderOperationFailureClass::StorageUnavailable,
        ResearchServiceError::ProviderCaptureSealWorkerUnavailable => {
            ProviderOperationFailureClass::CaptureUnavailable
        }
        ResearchServiceError::ProviderOnboarding(_) => {
            ProviderOperationFailureClass::RuntimeUnavailable
        }
    }
}

/// Source-registration or extraction-composition failure.
#[derive(Debug, Error)]
pub enum ResearchIngestCompositionError {
    /// An operation ceiling exceeds canonical extraction bounds.
    #[error("research extraction limits are invalid")]
    InvalidLimits,
    /// Rights evidence is empty or structurally invalid.
    #[error("research persistence rights evidence is invalid")]
    InvalidRightsEvidence,
    /// Rights evidence names a different source than the adapter metadata.
    #[error("research persistence rights do not match adapter source identity")]
    SourceRightsMismatch,
    /// A profile identity is already bound to an adapter.
    #[error("research extraction profile is already registered")]
    DuplicateProfile,
    /// Static composition exceeded the code-owned adapter ceiling.
    #[error("prepublished research source limit exceeded")]
    PrepublishedSourceLimitExceeded,
    /// A code-owned provider profile can only be published through exact onboarding.
    #[error("built-in research provider profile requires onboarding")]
    BuiltInProfileRequiresOnboarding,
    /// The code-owned provider profile registry could not be constructed.
    #[error("built-in research provider profiles are unavailable")]
    ProviderProfilesUnavailable,
    /// Shutdown has closed registration authority.
    #[error("research extraction coordinator is shutting down")]
    ShuttingDown,
    /// The process wall clock cannot provide a bounded timestamp.
    #[error("research extraction trusted time is unavailable")]
    TrustedTimeUnavailable,
    /// In-process source authority serialization is unavailable.
    #[error("research extraction authority is unavailable")]
    AuthorityUnavailable,
    /// A provider adapter is not bound to a complete non-secret runtime generation.
    #[error("research provider runtime generation is invalid")]
    InvalidRuntimeGeneration,
    /// A registered source does not expose generation-bound provider authority.
    #[error("research provider runtime generation is unavailable")]
    RuntimeGenerationUnavailable,
    /// A provider replacement is not an exact, fully constructed successor.
    #[error("research provider runtime replacement is invalid")]
    InvalidRuntimeReplacement,
    /// Another replacement already owns the exact provider publication slot.
    #[error("research provider runtime replacement is already in progress")]
    ReplacementInProgress,
    /// The expected runtime generation is no longer callable.
    #[error("research provider runtime generation is stale")]
    StaleRuntimeGeneration,
    /// A replacement cannot publish while its predecessor still admits requests.
    #[error("research provider runtime generation is still callable")]
    RuntimeGenerationStillCallable,
    /// The restart-durable source registry rejected registration.
    #[error("research source registration failed: {0}")]
    Registry(#[from] RegistryError),
}
