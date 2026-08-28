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
    DatasetId, IngestError, IngestPrecommitAuthority, RightsBasis, RightsDecisionInput,
    SourceOperation, extraction_provider_payload_digest,
};
use market_squawk_domain::{
    CompanyIdentityObservation, DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, DiscoveryBatch, DiscoveryRequest, ExtractionBatch,
    ExtractionRequest, ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    MAX_DISCOVERY_OBJECTS, MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
    ProviderCaptureMaterial, RegisteredSource, RegistryError, SourceClass, SourceError,
    SourceMetadata, SourceObject, SourceObjectCaptureIdentity, built_in_provider_profiles,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ResearchIngestCommitAuthority, ResearchIngestCoordinator, ResearchSourceDiscoveryCoordinator,
    encode_hex, manifest_value,
};
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

use super::super::domain_support::DomainLifecycle;

const STANDARD_EXTRACTION_DURATION: Duration = Duration::from_secs(60);
const STANDARD_DISCOVERY_RECEIPT_RETENTION: Duration = Duration::from_secs(5 * 60);
const MAXIMUM_PREPUBLISHED_RESEARCH_SOURCES: usize = 64;

mod alpaca_historical;
mod bea;
mod bls;
mod crypto_market;
mod iex_hist;
mod provider_runtime;
mod schwab_market;
mod sec_fund;
mod selection;
mod tiingo;
mod yahoo_enrichment;

pub(crate) use alpaca_historical::{
    AlpacaHistoricalAuthorizedPlan, AlpacaHistoricalPlanAdmissionError,
    AlpacaHistoricalPlanReceipt, AlpacaHistoricalSourceMutationAuthority,
    AlpacaHistoricalSourceSlotError,
};
pub(crate) use bea::{
    BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION, BeaDoctorActivationState,
    BeaMacroApplicationClosure, BeaMacroApplicationError, BeaMacroCapabilityState,
    BeaMacroPlanPublication, BeaProviderPeriodLatestKnownDto, BeaProviderPeriodLatestKnownRequest,
    BeaSetupRequiredDto, BeaSetupRequiredKind, BeaUnavailableDto, BeaUnavailableReason,
};
pub(crate) use bls::{
    BLS_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION, BlsMacroApplicationClosure,
    BlsMacroApplicationError, BlsMacroCapabilityState, BlsMacroPlanPublication,
    BlsMacroUnavailableReason, BlsPreparedMacroPlan, BlsProviderPeriodLatestKnownDto,
    BlsProviderPeriodLatestKnownRequest, BlsSealFirstExtractionLimits,
    BlsWholePlanApplicationHandoff,
};
pub(crate) use crypto_market::{
    CoinbaseMarketApplicationOutcome, CryptoMarketEventPublicationReceipt,
    CryptoMarketEventRestartReceipt, CryptoMarketEventRestartSelector,
    CryptoMarketPublicationClosure, CryptoMarketPublicationError,
    CryptoMarketSealedReceiptEvidence, CryptoMarketSelectorAvailability,
    CryptoMarketSelectorDependency, CryptoMarketSurface, KrakenMarketApplicationOutcome,
    KrakenSealedRawCanonicalUnavailable,
};
pub(crate) use iex_hist::{
    IexHistApplicationError, IexHistApplicationLane, IexHistCaptureSealHandoff,
    IexHistCaptureSealRequirements, IexHistCatalogSealHandoff, IexHistClockStatus,
    IexHistExactJobPreview, IexHistExplicitJobRequest, IexHistInstrumentIdentityBlocker,
    IexHistInstrumentIdentityStatus, IexHistJobAuthority, IexHistJobStatus,
    IexHistPhysicalArtifact, IexHistPhysicalSealRequirement, IexHistPublicationAvailability,
    IexHistPublicationBlocker, IexHistPublicationBlockers, IexHistResearchJobLeaf,
    IexHistSelectionStatus,
};
pub(crate) use sec_fund::{
    SEC_NCEN_FUND_POINT_IN_TIME_OPERATION, SEC_NPORT_FUND_POINT_IN_TIME_OPERATION,
    SecFundApplicationBridge, SecFundApplicationError, SecFundPublicationReceipt,
    SecNcenFundPublicationReceipt, SecNcenFundRestartSelector, SecNportFundPublicationReceipt,
    SecNportFundRestartSelector,
};
pub use provider_runtime::ResearchProviderRuntimeGeneration;
use provider_runtime::{ResearchProviderAdmission, ResearchProviderPublicationLease};
pub(crate) use provider_runtime::{
    ResearchProviderRuntimeMutationAuthority, ResearchProviderRuntimeReplacement,
};
use selection::{PreparedRetainedSelection, RetainedDiscoverySelections};
pub use selection::{
    ResearchSourceDiscovery, ResearchSourceDiscoveryObject, ResearchSourceObjectListing,
};
pub(crate) use tiingo::{
    TIINGO_EOD_MARKET_BAR_POINT_IN_TIME_OPERATION, TIINGO_FUND_NAV_POINT_IN_TIME_OPERATION,
    TiingoEodApplicationOutcome, TiingoEodRestartSelector, TiingoFundNavApplicationOutcome,
    TiingoFundNavRestartSelector, TiingoLatestApplicationError, TiingoLatestApplicationState,
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

impl ManagedExtraction {
    fn analytical_only(batch: ExtractionBatch) -> Self {
        Self {
            batch,
            company_identity: None,
            capture_material: None,
        }
    }
}

/// One bounded discovery batch plus exact provider bytes that must follow its selected object.
#[derive(Debug)]
pub struct ManagedDiscovery {
    batch: DiscoveryBatch,
    capture_material: Option<ProviderCaptureMaterial>,
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

fn bind_single_provider_capture(
    batch: ExtractionBatch,
    capture_material: ProviderCaptureMaterial,
) -> Result<ManagedExtraction, ExtractionSourceError> {
    let batch = batch
        .try_bind_provider_capture(capture_material.receipt())
        .map_err(|_error| invalid_capture_protocol())?;
    Ok(ManagedExtraction {
        batch,
        company_identity: None,
        capture_material: Some(capture_material),
    })
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

fn bind_provider_capture_graph(
    batch: ExtractionBatch,
    graph_purpose: &'static [u8],
    components: Vec<ProviderCaptureMaterial>,
) -> Result<ManagedExtraction, ExtractionSourceError> {
    let graph_identity = provider_capture_graph_identity(&batch, graph_purpose, &components)?;
    let dataset = batch.request().object().dataset().clone();
    let capture_material =
        ProviderCaptureMaterial::try_combine_request_graph(dataset, graph_identity, components)
            .map_err(|_error| invalid_capture_protocol())?;
    bind_single_provider_capture(batch, capture_material)
}

fn provider_capture_graph_identity(
    batch: &ExtractionBatch,
    graph_purpose: &'static [u8],
    components: &[ProviderCaptureMaterial],
) -> Result<EvidenceDigest, ExtractionSourceError> {
    let object = batch.request().object();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/provider-request-graph-composition/v1\0");
    update_capture_graph_field(&mut digest, graph_purpose)?;
    update_capture_graph_field(&mut digest, object.source_id().as_str().as_bytes())?;
    update_capture_graph_field(
        &mut digest,
        object
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    update_capture_graph_field(&mut digest, object.dataset().as_str().as_bytes())?;
    let component_count =
        u64::try_from(components.len()).map_err(|_error| invalid_capture_protocol())?;
    digest.update(component_count.to_be_bytes());
    for component in components {
        update_capture_graph_field(
            &mut digest,
            component.receipt().dataset().as_str().as_bytes(),
        )?;
        digest.update(component.receipt().request_set_identity().bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn update_capture_graph_field(
    digest: &mut Sha256,
    value: &[u8],
) -> Result<(), ExtractionSourceError> {
    let length = u64::try_from(value.len()).map_err(|_error| invalid_capture_protocol())?;
    digest.update(length.to_be_bytes());
    digest.update(value);
    Ok(())
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
    fn extract_managed(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtraction, ExtractionSourceError>> {
        let extracted = self.extract_with_capture(authority, request, cancellation);
        Box::pin(async move {
            let (batch, captures) = extracted.await?.into_parts();
            bind_provider_capture_graph(
                batch,
                b"fred-series-metadata-and-observation-page/v1",
                captures.into_vec(),
            )
        })
    }

    fn rights_subject(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ResearchRevisionPlanError> {
        market_squawk_adapter_fred::FredSource::rights_subject_identifier(dataset)
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
    fn extract_managed(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtraction, ExtractionSourceError>> {
        let extracted = self.extract_with_capture(authority, request, cancellation);
        Box::pin(async move {
            let output = extracted.await?;
            let (batch, capture_material) = output
                .try_into_common_publication()
                .map_err(|_error| invalid_capture_protocol())?;
            bind_single_provider_capture(batch, capture_material)
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
    fn extract_managed(
        &self,
        authority: market_squawk_sources::ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtraction, ExtractionSourceError>> {
        let extracted = self.extract_with_evidence(authority, request, cancellation);
        Box::pin(async move {
            let output = extracted.await.map_err(|error| {
                match error {
                market_squawk_adapter_federal_reserve::BoardExtractionError::Source(error) => {
                    error
                }
                market_squawk_adapter_federal_reserve::BoardExtractionError::Capture(_)
                | market_squawk_adapter_federal_reserve::BoardExtractionError::CaptureBodyTooLarge {
                    ..
                }
                | market_squawk_adapter_federal_reserve::BoardExtractionError::RawCapture(_) => {
                    invalid_capture_protocol()
                }
            }
            })?;
            let (batch, _parsed, _receipt, capture_material) = output.into_parts();
            bind_single_provider_capture(batch, capture_material)
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
    metadata: SourceMetadata,
    registration: Box<RegisteredSource>,
    rights: ResearchRightsAuthority,
    generation: Option<ResearchProviderRuntimeGeneration>,
    admission: ResearchProviderAdmission,
}

struct CoordinatorAuthority {
    registry: Option<AuthoritativeSourceRegistry>,
    sources: BTreeMap<SourceIdentifier, RegisteredExtractionSource>,
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
                pending_replacements: BTreeMap::new(),
                selections: RetainedDiscoverySelections::new(),
                alpaca_historical: alpaca_historical::AlpacaHistoricalSourceSlot::absent(),
            })),
        }
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
        self.register_source_arc_inner(profile, metadata, Arc::new(source), rights, generation)
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
        self.register_source_arc_inner(profile, metadata, Arc::from(source), rights, None)
    }

    fn register_source_arc_inner(
        &self,
        profile: SourceIdentifier,
        metadata: SourceMetadata,
        source: Arc<dyn ManagedResearchExtractionSource>,
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
        if authority.sources.contains_key(&profile) {
            return Err(ResearchIngestCompositionError::DuplicateProfile);
        }
        let registration = authority
            .registry
            .as_mut()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .register_or_resume_exact(metadata.clone(), registered_at)?;
        authority.sources.insert(
            profile,
            RegisteredExtractionSource {
                source,
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
        self.extract_exact(
            profile,
            dataset,
            object_id,
            context,
            &operation,
            operation_deadline,
        )
        .await
        .map(|extracted| extracted.batch)
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
        prepared.rights.validate_at(system_timestamp()?)?;
        let subject = prepared
            .source
            .rights_subject(object.dataset())
            .map_err(|_error| ServiceError::InvalidRequest)?;
        prepared.rights.validate_subject(subject.as_ref())?;
        let extraction_request =
            ExtractionRequest::try_new(object, self.limits.records, self.limits.bytes, deadline)
                .map_err(|_error| ServiceError::InvalidRequest)?;
        let managed = await_extraction(
            prepared.source.extract_managed(
                prepared.authority,
                extraction_request,
                operation.clone(),
            ),
            context,
            operation,
            &prepared.admission,
            operation_deadline,
        )
        .await?;
        let ManagedExtraction {
            batch,
            company_identity,
            capture_material: extraction_capture,
        } = managed;
        let (batch, capture_material) = match (discovery_capture, extraction_capture) {
            (Some(_), Some(_)) => return Err(ServiceError::InvalidResult),
            (Some(capture_material), None) => {
                let batch = batch
                    .try_bind_provider_capture(capture_material.receipt())
                    .map_err(|_error| ServiceError::InvalidResult)?;
                (batch, Some(capture_material))
            }
            (None, capture_material) => (batch, capture_material),
        };
        let local_source = matches!(
            prepared.metadata.source_class(),
            SourceClass::LocalFile | SourceClass::PortfolioExport
        );
        if local_source == capture_material.is_some()
            || !capture_material
                .as_ref()
                .is_none_or(|capture| capture_material_matches_batch(capture, &batch))
        {
            return Err(ServiceError::InvalidResult);
        }
        let revisions = prepared
            .source
            .revision_plan(&batch)
            .map_err(|_error| ServiceError::InvalidResult)?;
        let analytical_dataset = prepared
            .source
            .analytical_dataset(&batch)
            .map_err(|_error| ServiceError::InvalidResult)?;
        let payload_digest = extraction_provider_payload_digest(&batch);
        let retrieved_at = system_timestamp()?;
        let rights = prepared.rights.decision(payload_digest, retrieved_at)?;
        ensure_operation_live(operation_deadline, operation)?;
        prepared
            .admission
            .ensure_live()
            .map_err(|_error| ServiceError::Unavailable)?;
        Ok(AuthorizedExtraction {
            metadata: prepared.metadata,
            batch,
            company_identity,
            capture_material,
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

struct AuthorizedExtraction {
    metadata: SourceMetadata,
    batch: ExtractionBatch,
    company_identity: Option<CompanyIdentityObservation>,
    capture_material: Option<ProviderCaptureMaterial>,
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
            batch,
            company_identity,
            capture_material,
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
        let ingest = match (revisions, capture_material) {
            (Some(_revisions), Some(_capture_material)) => return Err(ServiceError::Unavailable),
            (None, None) => ResearchIngestRequest::locally_observed(
                source_metadata.clone(),
                rights,
                analytical_dataset,
                batch,
            ),
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

async fn await_extraction<T>(
    future: impl Future<Output = Result<T, ExtractionSourceError>>,
    context: &RequestContext,
    operation: &CancellationToken,
    admission: &ResearchProviderAdmission,
    operation_deadline: Instant,
) -> Result<T, ServiceError> {
    if Instant::now() >= operation_deadline {
        operation.cancel();
        return Err(ServiceError::DeadlineExceeded);
    }
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => {
            operation.cancel();
            Err(ServiceError::Cancelled)
        }
        () = operation.cancelled() => Err(ServiceError::Unavailable),
        () = admission.cancellation().cancelled() => {
            operation.cancel();
            Err(ServiceError::Unavailable)
        }
        () = tokio::time::sleep_until(operation_deadline.into()) => {
            operation.cancel();
            Err(ServiceError::DeadlineExceeded)
        }
        result = future => result.map_err(map_extraction_error),
    }
}

async fn await_publication<T>(
    future: impl Future<Output = Result<T, ResearchServiceError>>,
    context: &RequestContext,
    operation: &CancellationToken,
    admission: &ResearchProviderAdmission,
    operation_deadline: Instant,
) -> Result<T, ServiceError> {
    if Instant::now() >= operation_deadline {
        operation.cancel();
        return Err(ServiceError::DeadlineExceeded);
    }
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => {
            operation.cancel();
            Err(ServiceError::Cancelled)
        }
        () = operation.cancelled() => Err(ServiceError::Unavailable),
        () = admission.cancellation().cancelled() => {
            operation.cancel();
            Err(ServiceError::Unavailable)
        }
        () = tokio::time::sleep_until(operation_deadline.into()) => {
            operation.cancel();
            Err(ServiceError::DeadlineExceeded)
        }
        result = future => result.map_err(map_research_error),
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

fn map_extraction_error(error: ExtractionSourceError) -> ServiceError {
    match error {
        ExtractionSourceError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ExtractionSourceError::Cancelled
        | ExtractionSourceError::Source(SourceError::Cancelled) => ServiceError::Cancelled,
        ExtractionSourceError::Source(SourceError::Unauthorized) => ServiceError::Unauthorized,
        ExtractionSourceError::Contract(_) => ServiceError::InvalidRequest,
        ExtractionSourceError::Source(_) | ExtractionSourceError::Authority(_) => {
            ServiceError::Unavailable
        }
    }
}

fn map_registry_error(_error: RegistryError) -> ServiceError {
    ServiceError::Unavailable
}

fn map_ingest_error(error: IngestError) -> ServiceError {
    match error {
        IngestError::Cancelled => ServiceError::Cancelled,
        IngestError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        IngestError::RevisionEvidenceMismatch
        | IngestError::RevisionEvidenceRequired
        | IngestError::InvalidDataset
        | IngestError::InvalidProviderMacroPlan
        | IngestError::ProviderLogicalFundRequired
        | IngestError::ContentIdentity(_) => ServiceError::InvalidResult,
        IngestError::PublicationAuthorityRevoked
        | IngestError::Plan(_)
        | IngestError::Parquet(_)
        | IngestError::Arrow(_)
        | IngestError::Manifest(_)
        | IngestError::Catalog(_)
        | IngestError::ListingReference(_)
        | IngestError::Serialization(_)
        | IngestError::RevisionAuthority(_)
        | IngestError::AuthorityTransitionRejected
        | IngestError::CatalogCompositionMismatch
        | IngestError::UnknownSource
        | IngestError::UnknownReservation
        | IngestError::ReservationPayloadMismatch
        | IngestError::PersistRightsRequired
        | IngestError::TerminalRun
        | IngestError::IncompleteSuccessfulRun
        | IngestError::ReplayConflict
        | IngestError::ProviderCaptureRequired
        | IngestError::ProviderCapture(_)
        | IngestError::SealedProviderCapture(_)
        | IngestError::ProviderMarketEventSelection(_)
        | IngestError::ProviderCaptureRecoveryWorkerUnavailable
        | IngestError::AuthorityLockPoisoned => ServiceError::Unavailable,
    }
}

fn map_research_error(error: ResearchServiceError) -> ServiceError {
    match error {
        ResearchServiceError::Ingest(error) => map_ingest_error(error),
        ResearchServiceError::Rights(_) | ResearchServiceError::IngestAuthorityMismatch => {
            ServiceError::Unauthorized
        }
        ResearchServiceError::IdentityOverflow => ServiceError::Internal,
        ResearchServiceError::Path(_)
        | ResearchServiceError::Catalog(_)
        | ResearchServiceError::Manifest(_)
        | ResearchServiceError::ProviderCaptureStore(_)
        | ResearchServiceError::ProviderCaptureSealWorkerUnavailable
        | ResearchServiceError::ProviderOnboarding(_)
        | ResearchServiceError::Dataset(_) => ServiceError::Unavailable,
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
