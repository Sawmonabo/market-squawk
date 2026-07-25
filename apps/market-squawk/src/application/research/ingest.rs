//! Registry-authorized extraction and rights-bound analytical publication.

use std::{
    collections::BTreeMap,
    future::Future,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_data::{
    IngestError, RightsBasis, RightsDecisionInput, SourceOperation, extraction_batch_digest,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, DiscoveryBatch, DiscoveryRequest, ExtractionBatch,
    ExtractionRequest, ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    MAX_DISCOVERY_OBJECTS, MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
    RegisteredSource, RegistryError, SourceError, SourceMetadata, SourceObject,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ResearchIngestCoordinator, ResearchSourceDiscoveryCoordinator, encode_hex, manifest_value,
};
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

use super::super::domain_support::DomainLifecycle;

const STANDARD_EXTRACTION_DURATION: Duration = Duration::from_secs(60);

mod provider_runtime;
mod selection;

use provider_runtime::ResearchProviderAdmission;
pub use provider_runtime::{
    PreparedResearchProviderReplacement, ResearchProviderRuntimeGeneration,
};
use selection::{PreparedRetainedSelection, RetainedDiscoverySelections};
pub use selection::{
    ResearchSourceDiscovery, ResearchSourceDiscoveryObject, ResearchSourceObjectListing,
};

/// Fixed operation ceilings applied independently of transport result limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchExtractionLimits {
    discovery_objects: NonZeroU16,
    records: NonZeroU32,
    bytes: NonZeroU64,
    duration: Duration,
}

impl ResearchExtractionLimits {
    /// Constructs bounded discovery and extraction ceilings.
    ///
    /// # Errors
    ///
    /// Rejects any ceiling above the canonical in-memory extraction contracts or a zero duration.
    pub fn try_new(
        discovery_objects: NonZeroU16,
        records: NonZeroU32,
        bytes: NonZeroU64,
        duration: Duration,
    ) -> Result<Self, ResearchIngestCompositionError> {
        if usize::from(discovery_objects.get()) > MAX_DISCOVERY_OBJECTS
            || usize::try_from(records.get()).map_or(true, |value| value > MAX_EXTRACTION_RECORDS)
            || bytes.get() > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
            || duration.is_zero()
        {
            return Err(ResearchIngestCompositionError::InvalidLimits);
        }
        Ok(Self {
            discovery_objects,
            records,
            bytes,
            duration,
        })
    }

    /// Returns conservative production defaults bounded to one in-memory publication batch.
    pub fn standard() -> Self {
        Self {
            discovery_objects: NonZeroU16::new(MAX_DISCOVERY_OBJECTS as u16)
                .unwrap_or(NonZeroU16::MIN),
            records: NonZeroU32::new(MAX_EXTRACTION_RECORDS as u32).unwrap_or(NonZeroU32::MIN),
            bytes: NonZeroU64::new(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES).unwrap_or(NonZeroU64::MIN),
            duration: STANDARD_EXTRACTION_DURATION,
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
    authorization_evidence: EvidenceDigest,
    authorization_expires_at: Option<Timestamp>,
}

impl ResearchRightsAuthority {
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
            authorization_evidence,
            authorization_expires_at,
        })
    }

    fn decision(
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
            permitted_operations: vec![SourceOperation::Persist],
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
            authorization_evidence: self.authorization_evidence,
            authorization_expires_at: self.authorization_expires_at,
            persistence_operation_admitted: true,
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

    fn matches_discovery_evidence(&self, evidence: &ResearchSourceDiscoveryRights) -> bool {
        evidence.basis_reference == self.basis.reference()
            && evidence.basis_digest == self.basis.digest()
            && evidence.root_identity_digest == self.basis.root_identity_digest()
            && evidence.authorization_evidence == self.authorization_evidence
            && evidence.authorization_expires_at == self.authorization_expires_at
            && evidence.persistence_operation_admitted
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
    authorization_evidence: EvidenceDigest,
    authorization_expires_at: Option<Timestamp>,
    persistence_operation_admitted: bool,
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

    /// Returns the exact activation or owner-authorization evidence.
    pub const fn authorization_evidence(&self) -> EvidenceDigest {
        self.authorization_evidence
    }

    /// Returns the retained authorization expiry, when one exists.
    pub const fn authorization_expires_at(&self) -> Option<Timestamp> {
        self.authorization_expires_at
    }

    /// Returns whether the source-level persistence operation was admitted.
    ///
    /// The existing ingest gate still binds that authority to the exact extracted payload.
    pub const fn persistence_operation_admitted(&self) -> bool {
        self.persistence_operation_admitted
    }
}

/// Adapter revision evidence failed to align with one normalized extraction batch.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("research extraction revision evidence is invalid")]
pub struct ResearchRevisionPlanError;

/// Production extraction adapter plus its source-specific revision authority.
pub trait ManagedResearchExtractionSource: ExtractionSource + Send + Sync + 'static {
    /// Returns provider revision evidence, or `None` only for a user-owned local source.
    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError>;
}

impl ManagedResearchExtractionSource for market_squawk_adapter_sec::SecEdgarSource {
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
    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        market_squawk_adapter_treasury::TreasurySource::revision_plan(self, batch)
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
    registration: RegisteredSource,
    rights: ResearchRightsAuthority,
    generation: Option<ResearchProviderRuntimeGeneration>,
    admission: ResearchProviderAdmission,
}

struct CoordinatorAuthority {
    registry: Option<AuthoritativeSourceRegistry>,
    sources: BTreeMap<SourceIdentifier, RegisteredExtractionSource>,
    pending_replacements: BTreeMap<SourceIdentifier, Uuid>,
    selections: RetainedDiscoverySelections,
}

/// Sole production coordinator for source discovery, extraction, and analytical publication.
pub struct ProductionResearchIngestCoordinator {
    research: Arc<ResearchService>,
    limits: ResearchExtractionLimits,
    lifecycle: Arc<DomainLifecycle>,
    authority: Mutex<CoordinatorAuthority>,
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
            authority: Mutex::new(CoordinatorAuthority {
                registry: Some(registry),
                sources: BTreeMap::new(),
                pending_replacements: BTreeMap::new(),
                selections: RetainedDiscoverySelections::new(),
            }),
        }
    }

    /// Registers one exact adapter revision and its retained persistence-rights evidence.
    ///
    /// Registration is intentionally explicit so onboarding can construct an adapter only after
    /// every required credential, endpoint, terms, and local-root capability is available.
    ///
    /// # Errors
    ///
    /// Rejects duplicate profile identities, source/rights mismatch, shutdown, or durable source
    /// registry failure without publishing a callable adapter entry.
    pub fn register_source<S>(
        &self,
        profile: SourceIdentifier,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        self.register_source_inner(profile, source, rights, None)
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
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let metadata = source.metadata().clone();
        if metadata.source_id() != &rights.source_id {
            return Err(ResearchIngestCompositionError::SourceRightsMismatch);
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
                source: Arc::new(source),
                metadata,
                registration,
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
        Ok(authority.sources.contains_key(profile))
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
        let operation_deadline = operation_deadline(context, self.limits.duration)?;
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
        ResearchSourceObjectListing::new(profile.clone(), prepared.metadata, discovery)
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
        let operation_deadline = operation_deadline(context, self.limits.duration)?;
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
            discovery,
            self.limits.duration,
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
    ) -> Result<(PreparedExtraction, DiscoveryBatch, Instant, Timestamp), ServiceError> {
        if effective_at.is_some() || max_results.get() > self.limits.discovery_objects.get() {
            return Err(ServiceError::InvalidRequest);
        }
        let prepared = self.prepare(profile)?;
        prepared.rights.validate_at(system_timestamp()?)?;
        let deadline = wall_deadline(operation_deadline, operation)?;
        let request = DiscoveryRequest::try_new(dataset.clone(), None, max_results, deadline)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let discovery = await_extraction(
            prepared
                .source
                .discover(prepared.authority.clone(), request, operation.clone()),
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
        if discovery.objects().iter().any(|object| {
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
        let operation_deadline = operation_deadline(context, self.limits.duration)?;
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
            prepared.source.discover(
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
        context: &RequestContext,
        operation: &CancellationToken,
        operation_deadline: Instant,
        deadline: Timestamp,
    ) -> Result<AuthorizedExtraction, ServiceError> {
        let extraction_request =
            ExtractionRequest::try_new(object, self.limits.records, self.limits.bytes, deadline)
                .map_err(|_error| ServiceError::InvalidRequest)?;
        let batch = await_extraction(
            prepared
                .source
                .extract(prepared.authority, extraction_request, operation.clone()),
            context,
            operation,
            &prepared.admission,
            operation_deadline,
        )
        .await?;
        let revisions = prepared
            .source
            .revision_plan(&batch)
            .map_err(|_error| ServiceError::InvalidResult)?;
        let payload_digest = extraction_batch_digest(&batch).map_err(map_ingest_error)?;
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
            revisions,
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
    revisions: Option<ExtractionRevisionPlan>,
    payload_digest: EvidenceDigest,
    rights: RightsDecisionInput,
    admission: ResearchProviderAdmission,
}

#[async_trait]
impl ResearchIngestCoordinator for ProductionResearchIngestCoordinator {
    async fn ingest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let operation_deadline = operation_deadline(context, self.limits.duration)?;
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
            revisions,
            payload_digest,
            rights,
            admission,
        } = extracted;
        let idempotency_key = ingest_identity(&profile, &dataset, &object_id, payload_digest);
        let ingest = match revisions {
            Some(revisions) => ResearchIngestRequest::with_provider_revisions(
                source_metadata.clone(),
                rights,
                idempotency_key,
                batch,
                revisions,
            ),
            None => ResearchIngestRequest::locally_observed(
                source_metadata.clone(),
                rights,
                idempotency_key,
                batch,
            ),
        }
        .map_err(map_research_error)?;
        let committed = await_publication(
            self.research.ingest(ingest, operation.clone()),
            context,
            &operation,
            &admission,
            operation_deadline,
        )
        .await?;
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

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let drained = self.lifecycle.finish_shutdown(deadline).await;
        let closed = self.close_registry();
        drained.and(closed)
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

fn ingest_identity(
    profile: &SourceIdentifier,
    dataset: &SourceIdentifier,
    object: &SourceIdentifier,
    payload: EvidenceDigest,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/research-ingest/v1");
    update_identity(&mut digest, profile.as_str());
    update_identity(&mut digest, dataset.as_str());
    update_identity(&mut digest, object.as_str());
    digest.update([match payload.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(payload.bytes());
    format!("research-v1-{}", encode_hex(digest.finalize().into()))
}

fn update_identity(digest: &mut Sha256, value: &str) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value.as_bytes());
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
        | IngestError::ContentIdentity(_) => ServiceError::InvalidResult,
        IngestError::Plan(_)
        | IngestError::Parquet(_)
        | IngestError::Arrow(_)
        | IngestError::Manifest(_)
        | IngestError::Catalog(_)
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
        | IngestError::AuthorityLockPoisoned => ServiceError::Unavailable,
    }
}

fn map_research_error(error: ResearchServiceError) -> ServiceError {
    match error {
        ResearchServiceError::Ingest(error) => map_ingest_error(error),
        ResearchServiceError::Rights(_) | ResearchServiceError::IngestAuthorityMismatch => {
            ServiceError::Unauthorized
        }
        ResearchServiceError::Path(_)
        | ResearchServiceError::Catalog(_)
        | ResearchServiceError::Manifest(_)
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
