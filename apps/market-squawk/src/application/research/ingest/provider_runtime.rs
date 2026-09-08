//! Generation-bound publication of callable research-provider adapters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use market_squawk_adapter_schwab::SchwabOAuthAuthorityReceipt;
use market_squawk_data::{DatasetId, IngestError, IngestPrecommitAuthority, SourceOperation};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{SecretGeneration, SecretRef};
use market_squawk_sources::{
    ProviderCapabilityRevision, SEC_EDGAR_PROFILE_ID, SEC_EDGAR_SOURCE_ID,
    SchwabMarketDataDoctorReceiptV1, SourceMetadata, SourceMetadataProvider,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::schwab_market::{
    SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET, SchwabMarketPublicationClosure,
    SchwabMarketPublicationError, SchwabRestQuoteGenerationAuthority,
};
use super::{
    CryptoMarketPublicationClosure, ManagedResearchExtractionSource, MarketEventDurableRead,
    MarketEventDurableReadWriter, MarketEventPointInTimeSelector,
    ProductionResearchIngestCoordinator, ResearchIngestCompositionError, ResearchRightsAuthority,
};
use crate::provider_onboarding::SchwabOAuthMarketAuthority;

/// Exact non-secret generation identity for one callable research-provider adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchProviderRuntimeGeneration {
    profile: SourceIdentifier,
    session_id: Uuid,
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    credential_generation: Option<SecretGeneration>,
    secret_reference: Option<SecretRef>,
    authority_effective_at: Timestamp,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
}

/// Exact Schwab REST quote publication generation and its sole neutral durable-read channel.
///
/// The generation authority and writer are consumed by the provider runtime while the paired read
/// is installed into the provider-neutral selector registry. Keeping all three in one package
/// prevents a runtime from combining publication and point-in-time capabilities from different
/// source generations.
#[derive(Debug)]
pub(crate) struct SchwabRestQuotePublicationPackage {
    generation: Arc<SchwabRestQuoteGenerationAuthority>,
    durable_writer: MarketEventDurableReadWriter,
    durable_read: MarketEventDurableRead,
}

impl SchwabRestQuotePublicationPackage {
    pub(crate) const fn durable_read(&self) -> &MarketEventDurableRead {
        &self.durable_read
    }

    pub(crate) fn into_runtime_parts(
        self,
    ) -> (
        Arc<SchwabRestQuoteGenerationAuthority>,
        MarketEventDurableReadWriter,
    ) {
        (self.generation, self.durable_writer)
    }
}

impl ResearchProviderRuntimeGeneration {
    /// Binds one adapter candidate to onboarding, secret, metadata, and rights authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "runtime authority dimensions remain explicit in one validated constructor"
    )]
    pub fn try_new(
        profile: SourceIdentifier,
        session_id: Uuid,
        capability_revision: ProviderCapabilityRevision,
        capability_digest: EvidenceDigest,
        credential_generation: Option<SecretGeneration>,
        secret_reference: Option<SecretRef>,
        authority_effective_at: Timestamp,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
    ) -> Result<Self, ResearchIngestCompositionError> {
        let secret_binding_valid = match (credential_generation, secret_reference.as_ref()) {
            (None, None) => true,
            (Some(generation), Some(reference)) => reference.generation() == generation,
            (None, Some(_)) | (Some(_), None) => false,
        };
        if profile.as_str().is_empty()
            || session_id.is_nil()
            || capability_digest.bytes() == [0; 32]
            || !secret_binding_valid
            || metadata.source_id() != &rights.source_id
            || !metadata.is_effective_at(authority_effective_at)
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        Ok(Self {
            profile,
            session_id,
            capability_revision,
            capability_digest,
            credential_generation,
            secret_reference,
            authority_effective_at,
            metadata,
            rights,
        })
    }

    /// Returns the profile/surface identity selecting the runtime slot.
    pub const fn profile(&self) -> &SourceIdentifier {
        &self.profile
    }

    /// Returns the durable onboarding session.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the exact capability revision.
    pub const fn capability_revision(&self) -> ProviderCapabilityRevision {
        self.capability_revision
    }

    /// Returns the exact canonical capability evidence.
    pub const fn capability_digest(&self) -> EvidenceDigest {
        self.capability_digest
    }

    /// Returns the exact credential generation, when this surface uses one.
    pub const fn credential_generation(&self) -> Option<SecretGeneration> {
        self.credential_generation
    }

    /// Returns the opaque exact-generation secret reference, when credential-backed.
    pub const fn secret_reference(&self) -> Option<&SecretRef> {
        self.secret_reference.as_ref()
    }

    /// Returns the exact source metadata retained by this adapter.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the durable instant from which this exact generation is effective.
    pub const fn authority_effective_at(&self) -> Timestamp {
        self.authority_effective_at
    }

    /// Returns the exact admitted rights decision bound into this generation.
    pub const fn rights_authorization_evidence(&self) -> EvidenceDigest {
        self.rights.authorization_evidence
    }

    /// Returns the parent onboarding rights decision that admitted this subordinate authority.
    pub const fn parent_rights_authorization_evidence(&self) -> EvidenceDigest {
        self.rights.parent_authorization_evidence
    }

    /// Returns the finite subordinate authority expiry, when this generation is time-bounded.
    pub const fn rights_authorization_expires_at(&self) -> Option<Timestamp> {
        self.rights.authorization_expires_at
    }

    /// Returns the exact provider subjects, or `None` for a source-wide authority.
    pub const fn rights_exact_subjects(
        &self,
    ) -> Option<&std::collections::BTreeSet<SourceIdentifier>> {
        self.rights.exact_subjects.as_ref()
    }

    /// Returns whether the subordinate authority admits one operation.
    pub fn rights_admits(&self, operation: SourceOperation) -> bool {
        self.rights.permitted_operations.contains(&operation)
    }

    /// Returns the stable callable slot shared by legitimate generations of one provider source.
    pub fn slot_identity_digest(&self) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct RuntimeSlotWire<'a> {
            profile: &'a SourceIdentifier,
            source_id: &'a market_squawk_domain::SourceId,
        }

        digest_runtime_wire(
            b"market-squawk/research-provider-runtime-slot/v1\0",
            &RuntimeSlotWire {
                profile: &self.profile,
                source_id: self.metadata.source_id(),
            },
        )
    }

    /// Returns a canonical digest of every non-secret exact-generation authority dimension.
    pub fn generation_digest(&self) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct RuntimeGenerationWire<'a> {
            slot_identity_digest: EvidenceDigest,
            profile: &'a SourceIdentifier,
            session_id: Uuid,
            capability_revision: ProviderCapabilityRevision,
            capability_digest: EvidenceDigest,
            credential_generation: Option<SecretGeneration>,
            secret_reference: Option<&'a SecretRef>,
            authority_effective_at: Timestamp,
            metadata: &'a SourceMetadata,
            rights_source_id: &'a market_squawk_domain::SourceId,
            rights_basis_reference: &'a str,
            rights_basis_digest: EvidenceDigest,
            rights_root_identity_digest: Option<EvidenceDigest>,
            rights_parent_authorization_evidence: EvidenceDigest,
            rights_authorization_evidence: EvidenceDigest,
            rights_authorization_expires_at: Option<market_squawk_domain::Timestamp>,
            rights_contract_digest: EvidenceDigest,
        }

        digest_runtime_wire(
            b"market-squawk/research-provider-runtime-generation/v3\0",
            &RuntimeGenerationWire {
                slot_identity_digest: self.slot_identity_digest()?,
                profile: &self.profile,
                session_id: self.session_id,
                capability_revision: self.capability_revision,
                capability_digest: self.capability_digest,
                credential_generation: self.credential_generation,
                secret_reference: self.secret_reference.as_ref(),
                authority_effective_at: self.authority_effective_at,
                metadata: &self.metadata,
                rights_source_id: &self.rights.source_id,
                rights_basis_reference: self.rights.basis.reference(),
                rights_basis_digest: self.rights.basis.digest(),
                rights_root_identity_digest: self.rights.basis.root_identity_digest(),
                rights_parent_authorization_evidence: self.rights.parent_authorization_evidence,
                rights_authorization_evidence: self.rights.authorization_evidence,
                rights_authorization_expires_at: self.rights.authorization_expires_at,
                rights_contract_digest: rights_contract_digest(&self.rights),
            },
        )
    }

    fn is_exact_successor_of(
        &self,
        expected: &Self,
    ) -> Result<bool, ResearchIngestCompositionError> {
        if self.profile != expected.profile
            || self.metadata.source_id() != expected.metadata.source_id()
            || self.slot_identity_digest()? != expected.slot_identity_digest()?
            || self.generation_digest()? == expected.generation_digest()?
            || self.authority_effective_at <= expected.authority_effective_at
            || self.capability_revision < expected.capability_revision
        {
            return Ok(false);
        }
        if self.session_id != expected.session_id {
            return Ok(true);
        }
        Ok(self.capability_revision == expected.capability_revision
            && self.capability_digest == expected.capability_digest
            && match (
                expected.credential_generation,
                self.credential_generation,
                expected.secret_reference.as_ref(),
                self.secret_reference.as_ref(),
            ) {
                (
                    Some(prior),
                    Some(candidate),
                    Some(prior_reference),
                    Some(candidate_reference),
                ) => {
                    prior
                        .get()
                        .checked_add(1)
                        .is_some_and(|next| next == candidate.get())
                        && prior_reference != candidate_reference
                }
                _ => false,
            })
    }
}

fn rights_contract_digest(rights: &ResearchRightsAuthority) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/research-rights-contract/v1\0");
    digest.update(rights.parent_authorization_evidence.bytes());
    digest.update(rights.authorization_evidence.bytes());
    match rights.authorization_expires_at {
        Some(expires_at) => {
            digest.update([1]);
            digest.update(expires_at.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
    match &rights.exact_subjects {
        Some(subjects) => {
            digest.update([1]);
            digest.update(
                u32::try_from(subjects.len())
                    .unwrap_or(u32::MAX)
                    .to_be_bytes(),
            );
            for subject in subjects {
                update_digest_part(&mut digest, subject.as_str().as_bytes());
            }
        }
        None => digest.update([0]),
    }
    digest.update(
        u32::try_from(rights.permitted_operations.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for operation in &rights.permitted_operations {
        digest.update([source_operation_tag(*operation)]);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn update_digest_part(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

const fn source_operation_tag(operation: SourceOperation) -> u8 {
    match operation {
        SourceOperation::Retrieve => 1,
        SourceOperation::Display => 2,
        SourceOperation::Persist => 3,
        SourceOperation::Cache => 4,
        SourceOperation::Redistribute => 5,
        SourceOperation::Train => 6,
    }
}

fn digest_runtime_wire<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| ResearchIngestCompositionError::InvalidRuntimeGeneration)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

/// Per-generation, process-local authority checked before and during every provider request.
#[derive(Clone, Debug)]
pub(super) struct ResearchProviderAdmission {
    generation_digest: Option<EvidenceDigest>,
    source_id: Option<SourceId>,
    metadata_revision: Option<MetadataRevision>,
    state: Arc<ResearchProviderAdmissionState>,
    cancellation: CancellationToken,
}

const ADMISSION_PENDING: u8 = 0;
const ADMISSION_ACTIVE: u8 = 1;
const ADMISSION_REVOKING: u8 = 2;
const ADMISSION_DRAINED: u8 = 3;

#[derive(Debug)]
struct ResearchProviderAdmissionState {
    phase: AtomicU8,
    publication_barrier: Arc<RwLock<()>>,
}

/// Exact-generation lease retained across the durable research publication boundary.
pub(super) struct ResearchProviderPublicationLease {
    admission: ResearchProviderAdmission,
    _publication: OwnedRwLockReadGuard<()>,
}

impl std::fmt::Debug for ResearchProviderPublicationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderPublicationLease")
            .field("generation_digest", &self.admission.generation_digest)
            .finish_non_exhaustive()
    }
}

impl ResearchProviderPublicationLease {
    pub(super) fn validate_precommit(&self) -> Result<(), ResearchIngestCompositionError> {
        self.admission.ensure_live()
    }
}

impl IngestPrecommitAuthority for ResearchProviderPublicationLease {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        ResearchProviderPublicationLease::validate_precommit(self)
            .map_err(|_error| IngestError::PublicationAuthorityRevoked)
    }
}

/// Coordinator-owned exact-generation authority spanning specialized provider network, raw seal,
/// and final analytical commit without exposing the registry or publication lease separately.
pub(crate) struct ResearchProviderPublicationOperation {
    generation: ResearchProviderRuntimeGeneration,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    source_registered_at: Timestamp,
    publication: Arc<ResearchProviderPublicationLease>,
    cancellation: CancellationToken,
    watcher: JoinHandle<()>,
}

/// Application-minted, exact-generation authority for one crypto canonical-publication lane.
///
/// All fields remain private so callers can retain and use the authority but cannot substitute a
/// source, dataset, rights grant, publication lease, or research service.
pub(crate) struct CryptoMarketPublicationAuthority {
    operation: ResearchProviderPublicationOperation,
    publication: Arc<CryptoMarketPublicationClosure>,
    analytical_dataset: DatasetId,
    precommit: Arc<dyn IngestPrecommitAuthority>,
}

impl CryptoMarketPublicationAuthority {
    pub(crate) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        self.operation.generation()
    }

    pub(crate) fn publication(&self) -> Arc<CryptoMarketPublicationClosure> {
        self.publication.clone()
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    pub(crate) fn precommit_authority(&self) -> Arc<dyn IngestPrecommitAuthority> {
        self.precommit.clone()
    }

    /// Mints the sole source- and dataset-bound durable-read handoff for this runtime generation.
    pub(crate) fn durable_read_capability(
        &self,
    ) -> (MarketEventDurableReadWriter, MarketEventDurableRead) {
        let point_in_time = self
            .publication
            .point_in_time_selector(self.analytical_dataset.clone());
        MarketEventDurableRead::channel(point_in_time)
    }

    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        self.operation.cancellation()
    }

    pub(crate) fn validate_precommit(&self) -> Result<(), ResearchIngestCompositionError> {
        self.operation.validate_precommit()
    }
}

impl std::fmt::Debug for CryptoMarketPublicationAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CryptoMarketPublicationAuthority")
            .field("generation", self.operation.generation())
            .field("analytical_dataset", &self.analytical_dataset)
            .finish_non_exhaustive()
    }
}

impl ResearchProviderPublicationOperation {
    pub(crate) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    pub(crate) const fn source(&self) -> &SourceMetadata {
        &self.source
    }

    pub(crate) const fn rights(&self) -> &ResearchRightsAuthority {
        &self.rights
    }

    pub(crate) const fn source_registered_at(&self) -> Timestamp {
        self.source_registered_at
    }

    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(crate) fn precommit_authority(&self) -> Arc<dyn IngestPrecommitAuthority> {
        self.publication.clone()
    }

    pub(crate) fn validate_precommit(&self) -> Result<(), ResearchIngestCompositionError> {
        if self.cancellation.is_cancelled() {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.publication.validate_precommit()
    }
}

impl Drop for ResearchProviderPublicationOperation {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.watcher.abort();
    }
}

impl std::fmt::Debug for ResearchProviderPublicationOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderPublicationOperation")
            .field("generation", &self.generation)
            .field("source_id", self.source.source_id())
            .finish_non_exhaustive()
    }
}

impl ResearchProviderAdmission {
    pub(super) fn new(
        generation: Option<&ResearchProviderRuntimeGeneration>,
    ) -> Result<Self, ResearchIngestCompositionError> {
        Self::with_phase(generation, ADMISSION_ACTIVE)
    }

    fn new_pending(
        generation: &ResearchProviderRuntimeGeneration,
    ) -> Result<Self, ResearchIngestCompositionError> {
        Self::with_phase(Some(generation), ADMISSION_PENDING)
    }

    /// Creates one pending admission for a sealed, non-generic parent generation.
    ///
    /// Alpaca history is subordinate to a market-runtime group rather than to a generic research
    /// provider generation. Keeping this constructor digest-only prevents that private parent from
    /// changing the generic generation wire identity or successor protocol.
    pub(super) fn new_pending_for_parent_digest(
        parent_digest: EvidenceDigest,
    ) -> Result<Self, ResearchIngestCompositionError> {
        if parent_digest.algorithm() != DigestAlgorithm::Sha256 || parent_digest.bytes() == [0; 32]
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        Ok(Self {
            generation_digest: Some(parent_digest),
            source_id: None,
            metadata_revision: None,
            state: Arc::new(ResearchProviderAdmissionState {
                phase: AtomicU8::new(ADMISSION_PENDING),
                publication_barrier: Arc::new(RwLock::new(())),
            }),
            cancellation: CancellationToken::new(),
        })
    }

    fn with_phase(
        generation: Option<&ResearchProviderRuntimeGeneration>,
        phase: u8,
    ) -> Result<Self, ResearchIngestCompositionError> {
        let (generation_digest, source_id, metadata_revision) = match generation {
            Some(generation) => (
                Some(generation.generation_digest()?),
                Some(generation.metadata().source_id().clone()),
                Some(generation.metadata().revision().clone()),
            ),
            None => (None, None, None),
        };
        Ok(Self {
            generation_digest,
            source_id,
            metadata_revision,
            state: Arc::new(ResearchProviderAdmissionState {
                phase: AtomicU8::new(phase),
                publication_barrier: Arc::new(RwLock::new(())),
            }),
            cancellation: CancellationToken::new(),
        })
    }

    pub(super) fn ensure_live(&self) -> Result<(), ResearchIngestCompositionError> {
        if self.cancellation.is_cancelled()
            || self.state.phase.load(Ordering::Acquire) != ADMISSION_ACTIVE
        {
            Err(ResearchIngestCompositionError::StaleRuntimeGeneration)
        } else {
            Ok(())
        }
    }

    /// Returns whether this live admission was minted for the exact source, metadata revision,
    /// and complete non-secret runtime generation supplied by the caller.
    pub(super) fn admits_generation(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
    ) -> Result<bool, ResearchIngestCompositionError> {
        self.ensure_live()?;
        let metadata = generation.metadata();
        Ok(
            self.generation_digest == Some(generation.generation_digest()?)
                && self.source_id.as_ref() == Some(metadata.source_id())
                && self.metadata_revision.as_ref() == Some(metadata.revision())
                && metadata.source_id() == &generation.rights.source_id
                && metadata.is_effective_at(generation.authority_effective_at()),
        )
    }

    pub(super) fn matches(&self, other: &Self) -> bool {
        self.generation_digest == other.generation_digest
            && self.source_id == other.source_id
            && self.metadata_revision == other.metadata_revision
            && Arc::ptr_eq(&self.state, &other.state)
    }

    pub(super) fn revoke(&self) {
        self.begin_revocation();
    }

    fn begin_revocation(&self) {
        let mut phase = self.state.phase.load(Ordering::Acquire);
        while matches!(phase, ADMISSION_PENDING | ADMISSION_ACTIVE) {
            match self.state.phase.compare_exchange(
                phase,
                ADMISSION_REVOKING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => phase = current,
            }
        }
        self.cancellation.cancel();
    }

    pub(super) fn ensure_pending(&self) -> Result<(), ResearchIngestCompositionError> {
        if self.cancellation.is_cancelled()
            || self.state.phase.load(Ordering::Acquire) != ADMISSION_PENDING
        {
            Err(ResearchIngestCompositionError::StaleRuntimeGeneration)
        } else {
            Ok(())
        }
    }

    pub(super) fn activate_pending(&self) {
        debug_assert!(!self.cancellation.is_cancelled());
        let prior = self.state.phase.swap(ADMISSION_ACTIVE, Ordering::AcqRel);
        debug_assert_eq!(prior, ADMISSION_PENDING);
    }

    pub(super) async fn acquire_publication_lease(
        &self,
    ) -> Result<ResearchProviderPublicationLease, ResearchIngestCompositionError> {
        self.ensure_live()?;
        let publication = Arc::clone(&self.state.publication_barrier)
            .read_owned()
            .await;
        self.ensure_live()?;
        Ok(ResearchProviderPublicationLease {
            admission: self.clone(),
            _publication: publication,
        })
    }

    pub(super) async fn revoke_and_drain(&self) {
        self.begin_revocation();
        let publication = Arc::clone(&self.state.publication_barrier)
            .write_owned()
            .await;
        self.state.phase.store(ADMISSION_DRAINED, Ordering::Release);
        drop(publication);
    }

    pub(super) fn revocation_drained(&self) -> bool {
        self.state.phase.load(Ordering::Acquire) == ADMISSION_DRAINED
    }

    pub(super) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// One callable Schwab generation bound to both generic provider and exact OAuth currentness.
struct SchwabCompositeMarketRuntimeAdmission {
    generation_digest: EvidenceDigest,
    admission: ResearchProviderAdmission,
    oauth: SchwabOAuthMarketAuthority,
    oauth_receipt: SchwabOAuthAuthorityReceipt,
}

#[cfg(test)]
pub(super) fn test_schwab_composite_market_runtime_admission(
    generation: &ResearchProviderRuntimeGeneration,
    oauth: SchwabOAuthMarketAuthority,
    oauth_receipt: SchwabOAuthAuthorityReceipt,
) -> Result<
    Arc<dyn super::schwab_market::SchwabMarketRuntimeAdmission>,
    ResearchIngestCompositionError,
> {
    let generation_digest = generation.generation_digest()?;
    Ok(Arc::new(SchwabCompositeMarketRuntimeAdmission {
        generation_digest,
        admission: ResearchProviderAdmission::new(Some(generation))?,
        oauth,
        oauth_receipt,
    }))
}

impl SchwabCompositeMarketRuntimeAdmission {
    fn ensure_exact_current(&self) -> Result<(), ResearchIngestCompositionError> {
        self.admission.ensure_live()?;
        if self.admission.generation_digest != Some(self.generation_digest) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.oauth
            .validate_current_receipt(self.oauth_receipt)
            .map_err(|_error| ResearchIngestCompositionError::StaleRuntimeGeneration)?;
        self.admission.ensure_live()
    }
}

impl super::schwab_market::SchwabMarketRuntimeAdmission for SchwabCompositeMarketRuntimeAdmission {
    fn generation_digest(&self) -> Option<EvidenceDigest> {
        self.ensure_exact_current()
            .ok()
            .map(|()| self.generation_digest)
    }

    fn ensure_live(&self) -> Result<(), ResearchIngestCompositionError> {
        self.ensure_exact_current()
    }

    fn validate_oauth_current(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<(), ResearchIngestCompositionError> {
        self.admission.ensure_live()?;
        if receipt != self.oauth_receipt {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.oauth
            .validate_current_receipt(receipt)
            .map_err(|_error| ResearchIngestCompositionError::StaleRuntimeGeneration)?;
        self.ensure_exact_current()
    }

    fn cancellation(&self) -> &CancellationToken {
        self.admission.cancellation()
    }

    fn acquire_publication_lease(
        &self,
    ) -> BoxFuture<'_, Result<ResearchProviderPublicationLease, ResearchIngestCompositionError>>
    {
        Box::pin(async move {
            self.ensure_exact_current()?;
            let lease = self.admission.acquire_publication_lease().await?;
            self.ensure_exact_current()?;
            Ok(lease)
        })
    }

    fn revoke(&self) {
        self.admission.revoke();
    }

    fn revoke_and_drain(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.admission.revoke_and_drain().await;
        })
    }

    fn revocation_drained(&self) -> bool {
        self.admission.revocation_drained()
    }
}

impl std::fmt::Debug for SchwabCompositeMarketRuntimeAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabCompositeMarketRuntimeAdmission")
            .field("generation_digest", &self.generation_digest)
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .field("oauth_generation", &self.oauth_receipt.generation().get())
            .finish_non_exhaustive()
    }
}

/// Fully constructed replacement held outside the callable runtime until exact finalization.
struct PreparedResearchProviderReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_capability: Option<super::RegisteredSourceCapability>,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl PreparedResearchProviderReplacement {
    /// Revokes and drains only the token-bound predecessor retained by this transaction.
    async fn revoke_predecessor(&mut self) -> Result<(), ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let admission = {
            let mut authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            let current = authority
                .sources
                .get(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation.as_ref() != Some(&self.expected)
                || current.metadata != self.expected.metadata
                || current.rights != self.expected.rights
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.revoke();
            let admission = current.admission.clone();
            authority.selections.revoke_profile(&self.profile);
            admission
        };
        admission.revoke_and_drain().await;
        Ok(())
    }

    /// Restores the exact predecessor retained by this token without publishing the candidate.
    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        let authority = &mut *authority;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let replacement_admission = {
            let current = authority
                .sources
                .get(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation.as_ref() != Some(&self.expected)
                || current.metadata != self.expected.metadata
                || current.rights != self.expected.rights
                || current.registration.source_id() != current.metadata.source_id()
                || current.registration.revision() != current.metadata.revision()
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            if current.admission.revocation_drained() {
                Some(ResearchProviderAdmission::new(Some(&self.expected))?)
            } else {
                current.admission.ensure_live()?;
                None
            }
        };
        self.candidate_admission.revoke();
        if let Some(admission) = replacement_admission {
            let current = authority
                .sources
                .get_mut(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            current.admission = admission;
        }
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    /// Transfers the validated candidate into a still-non-callable committed capability.
    fn commit(
        &mut self,
    ) -> Result<CommittedResearchProviderReplacement, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .sources
            .get(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&self.expected)
            || current.metadata != self.expected.metadata
            || current.rights != self.expected.rights
            || current.registration.source_id() != current.metadata.source_id()
            || current.registration.revision() != current.metadata.revision()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if !current.admission.revocation_drained() {
            return Err(ResearchIngestCompositionError::RuntimeGenerationStillCallable);
        }
        drop(authority);
        let candidate_capability = self
            .candidate_capability
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        self.completed = true;
        Ok(CommittedResearchProviderReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile: self.profile.clone(),
            token: self.token,
            expected: self.expected.clone(),
            candidate: self.candidate.clone(),
            candidate_capability: Some(candidate_capability),
            candidate_admission: self.candidate_admission.clone(),
            completed: false,
        })
    }
}

impl std::fmt::Debug for PreparedResearchProviderReplacement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedResearchProviderReplacement")
            .field("profile", &self.profile)
            .field("expected", &self.expected)
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

impl Drop for PreparedResearchProviderReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        let Ok(mut authority) = self.coordinator.authority.lock() else {
            tracing::error!(
                profile = self.profile.as_str(),
                "provider replacement admission could not be released"
            );
            return;
        };
        if authority.pending_replacements.get(&self.profile) == Some(&self.token) {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Token-bound candidate retained pending until higher-level durable authority is exact.
struct CommittedResearchProviderReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_capability: Option<super::RegisteredSourceCapability>,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl CommittedResearchProviderReplacement {
    /// Cancels the pending candidate and re-admits the exact retained predecessor.
    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let predecessor_admission = ResearchProviderAdmission::new(Some(&self.expected))?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&self.expected)
            || current.metadata != self.expected.metadata
            || current.rights != self.expected.rights
            || current.registration.source_id() != current.metadata.source_id()
            || current.registration.revision() != current.metadata.revision()
            || !current.admission.revocation_drained()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.candidate_admission.revoke();
        current.admission = predecessor_admission;
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    /// Publishes and activates the candidate after higher-level durable authority is exact.
    fn finalize(
        &mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let super::CoordinatorAuthority {
            registry,
            sources,
            publication_sources: _,
            pending_replacements,
            selections: _,
            alpaca_historical: _,
        } = &mut *authority;
        let current = sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&self.expected)
            || current.metadata != self.expected.metadata
            || current.rights != self.expected.rights
            || current.registration.source_id() != current.metadata.source_id()
            || current.registration.revision() != current.metadata.revision()
            || !current.admission.revocation_drained()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let candidate_capability = self
            .candidate_capability
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        let replacement_registration = if current.metadata == self.candidate.metadata {
            None
        } else {
            let registered_at = super::system_timestamp()
                .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
            Some(
                registry
                    .as_mut()
                    .ok_or(ResearchIngestCompositionError::ShuttingDown)?
                    .replace_metadata(
                        &current.registration,
                        self.candidate.metadata.clone(),
                        registered_at,
                    )?,
            )
        };
        let super::RegisteredSourceCapability { erased, typed } = candidate_capability;
        current.source = erased;
        current.typed_capability = typed;
        current.metadata = self.candidate.metadata.clone();
        if let Some(registration) = replacement_registration {
            current.registration = Box::new(registration);
        }
        current.rights = self.candidate.rights.clone();
        current.generation = Some(self.candidate.clone());
        current.admission = self.candidate_admission.clone();
        let removed = pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.candidate_admission.activate_pending();
        self.completed = true;
        Ok(self.candidate.clone())
    }
}

impl std::fmt::Debug for CommittedResearchProviderReplacement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedResearchProviderReplacement")
            .field("profile", &self.profile)
            .field("expected", &self.expected)
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

impl Drop for CommittedResearchProviderReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        let Ok(mut authority) = self.coordinator.authority.lock() else {
            tracing::error!(
                profile = self.profile.as_str(),
                "committed provider replacement could not be failed closed"
            );
            return;
        };
        if authority.pending_replacements.get(&self.profile) == Some(&self.token) {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Pending exact-generation replacement for a specialized publication-only provider.
struct PreparedResearchProviderPublicationReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_rights: ResearchRightsAuthority,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl PreparedResearchProviderPublicationReplacement {
    async fn revoke_predecessor(&mut self) -> Result<(), ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let predecessor = {
            let authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            if self.coordinator.lifecycle.shutdown_token().is_cancelled()
                || authority.pending_replacements.get(&self.profile) != Some(&self.token)
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            let current = authority
                .publication_sources
                .get(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation != self.expected
                || current.metadata != *self.expected.metadata()
                || current.rights != self.expected.rights
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.revoke();
            current.admission.clone()
        };
        predecessor.revoke_and_drain().await;
        Ok(())
    }

    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .publication_sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected || current.metadata != *self.expected.metadata() {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.candidate_admission.revoke();
        if current.admission.revocation_drained() {
            current.admission = ResearchProviderAdmission::new(Some(&self.expected))?;
        } else {
            current.admission.ensure_live()?;
        }
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    fn commit(
        &mut self,
    ) -> Result<CommittedResearchProviderPublicationReplacement, ResearchIngestCompositionError>
    {
        self.candidate_admission.ensure_pending()?;
        let authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .publication_sources
            .get(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected
            || current.metadata != *self.expected.metadata()
            || !current.admission.revocation_drained()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        drop(authority);
        self.completed = true;
        Ok(CommittedResearchProviderPublicationReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile: self.profile.clone(),
            token: self.token,
            expected: self.expected.clone(),
            candidate: self.candidate.clone(),
            candidate_rights: self.candidate_rights.clone(),
            candidate_admission: self.candidate_admission.clone(),
            completed: false,
        })
    }
}

impl Drop for PreparedResearchProviderPublicationReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        if let Ok(mut authority) = self.coordinator.authority.lock()
            && authority.pending_replacements.get(&self.profile) == Some(&self.token)
        {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Committed, still-non-callable specialized provider candidate.
struct CommittedResearchProviderPublicationReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_rights: ResearchRightsAuthority,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl CommittedResearchProviderPublicationReplacement {
    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let predecessor_admission = ResearchProviderAdmission::new(Some(&self.expected))?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .publication_sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected || !current.admission.revocation_drained() {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.candidate_admission.revoke();
        current.admission = predecessor_admission;
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    fn finalize(
        &mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let registered_at = super::system_timestamp()
            .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
        if !self.candidate.metadata().is_effective_at(registered_at) {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let super::CoordinatorAuthority {
            registry,
            sources: _,
            publication_sources,
            pending_replacements,
            selections: _,
            alpaca_historical: _,
        } = &mut *authority;
        let current = publication_sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected
            || !current.admission.revocation_drained()
            || current.registration.source_id() != self.expected.metadata().source_id()
            || current.registration.revision() != self.expected.metadata().revision()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if current.metadata != *self.candidate.metadata() {
            current.registration = Box::new(
                registry
                    .as_mut()
                    .ok_or(ResearchIngestCompositionError::ShuttingDown)?
                    .replace_metadata(
                        current.registration.as_ref(),
                        self.candidate.metadata().clone(),
                        registered_at,
                    )?,
            );
            current.registered_at = registered_at;
        }
        current.metadata = self.candidate.metadata().clone();
        current.rights = self.candidate_rights.clone();
        current.generation = self.candidate.clone();
        current.admission = self.candidate_admission.clone();
        let removed = pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.candidate_admission.activate_pending();
        self.completed = true;
        Ok(self.candidate.clone())
    }
}

impl Drop for CommittedResearchProviderPublicationReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        if let Ok(mut authority) = self.coordinator.authority.lock()
            && authority.pending_replacements.get(&self.profile) == Some(&self.token)
        {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Non-cloneable mutation authority minted with one exact production coordinator.
///
/// The coordinator itself exposes only read-only provider-generation inspection. Every provider
/// source-map mutation requires this value, which is moved into the application-owned adapter
/// activation boundary at composition.
pub(crate) struct ResearchProviderRuntimeMutationAuthority {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
}

/// Unforgeable proof that SEC live-fund composition came from the locked coordinator registry.
pub(super) struct SecLiveFundCoordinatorSeal {
    _private: (),
}

/// Opaque token-bound replacement whose transitions are available only through its minting
/// [`ResearchProviderRuntimeMutationAuthority`].
pub(crate) struct ResearchProviderRuntimeReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    state: Option<ResearchProviderRuntimeReplacementState>,
}

enum ResearchProviderRuntimeReplacementState {
    Prepared(PreparedResearchProviderReplacement),
    Committed(CommittedResearchProviderReplacement),
    PreparedPublication(PreparedResearchProviderPublicationReplacement),
    CommittedPublication(CommittedResearchProviderPublicationReplacement),
}

impl ResearchProviderRuntimeMutationAuthority {
    pub(super) fn new(coordinator: Arc<ProductionResearchIngestCoordinator>) -> Self {
        Self { coordinator }
    }

    fn require_bound(
        &self,
        transaction: &ResearchProviderRuntimeReplacement,
    ) -> Result<(), ResearchIngestCompositionError> {
        if Arc::ptr_eq(&self.coordinator, &transaction.coordinator) {
            Ok(())
        } else {
            Err(ResearchIngestCompositionError::StaleRuntimeGeneration)
        }
    }

    /// Atomically registers one exact SEC source and composes its sole live-fund authority.
    ///
    /// No callable coordinator entry is published until registry registration, extraction
    /// authority, and application bridge composition all succeed. A post-registration failure
    /// releases only that exact process registration while preserving its clean-resumable durable
    /// metadata history.
    pub(crate) fn register_sec_live_fund_source(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: Arc<market_squawk_adapter_sec::SecEdgarSource>,
        rights: ResearchRightsAuthority,
        identity_authority_source_id: market_squawk_domain::SourceId,
    ) -> Result<super::sec_live::SecLiveFundSource, super::sec_live::SecLiveFundApplicationError>
    {
        let metadata =
            market_squawk_sources::SourceMetadataProvider::metadata(source.as_ref()).clone();
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || generation.profile().as_str() != SEC_EDGAR_PROFILE_ID
            || metadata.source_id().as_str() != SEC_EDGAR_SOURCE_ID
            || metadata != *generation.metadata()
            || rights != generation.rights
            || rights.source_id() != metadata.source_id()
            || identity_authority_source_id != *metadata.source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration.into());
        }
        let registered_at = super::system_timestamp()
            .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
        let admission = ResearchProviderAdmission::new(Some(&generation))?;
        let generation_digest = generation.generation_digest()?;
        let source_erased: Arc<dyn ManagedResearchExtractionSource> = source.clone();
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown.into());
        }
        if authority.sources.contains_key(generation.profile()) {
            return Err(ResearchIngestCompositionError::DuplicateProfile.into());
        }
        let (registration, operation) = {
            let registry = authority
                .registry
                .as_mut()
                .ok_or(ResearchIngestCompositionError::ShuttingDown)?;
            let registration = registry
                .register_or_resume_exact(metadata.clone(), registered_at)
                .map_err(ResearchIngestCompositionError::from)?;
            let composition = (|| {
                let extraction = registry
                    .extraction_authority(&registration, source.as_ref())
                    .map_err(ResearchIngestCompositionError::from)?;
                let operation = super::sec_live::SecLiveFundSource::from_coordinator(
                    SecLiveFundCoordinatorSeal { _private: () },
                    Arc::clone(&source),
                    extraction,
                    generation.clone(),
                    admission.clone(),
                    rights.clone(),
                    Arc::clone(&self.coordinator.research),
                    identity_authority_source_id,
                )?;
                admission.ensure_live()?;
                if admission.generation_digest != Some(generation_digest)
                    || source_erased.metadata() != &metadata
                {
                    return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration.into());
                }
                Ok(operation)
            })();
            match composition {
                Ok(operation) => (registration, operation),
                Err(error) => {
                    registry
                        .release_process_registration_exact(&registration)
                        .map_err(ResearchIngestCompositionError::from)?;
                    return Err(error);
                }
            }
        };
        authority.sources.insert(
            generation.profile().clone(),
            super::RegisteredExtractionSource {
                source: source_erased,
                typed_capability: super::RegisteredTypedSourceCapability::None,
                metadata,
                registration: Box::new(registration),
                rights,
                generation: Some(generation),
                admission,
            },
        );
        Ok(operation)
    }

    /// Atomically binds one exact registered Schwab publication generation to its sole REST quote
    /// publisher and provider-neutral durable-read channel.
    #[allow(
        clippy::too_many_arguments,
        reason = "generation, doctor, OAuth, analytical dataset, and operation lifetime remain explicit"
    )]
    pub(crate) fn bind_schwab_rest_quote_publication_package(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        doctor: SchwabMarketDataDoctorReceiptV1,
        oauth: SchwabOAuthMarketAuthority,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
        analytical_dataset: DatasetId,
        operation_timeout: Duration,
    ) -> Result<SchwabRestQuotePublicationPackage, SchwabMarketPublicationError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || generation.profile().as_str() != market_squawk_sources::SCHWAB_MARKET_DATA_SURFACE_ID
            || oauth.session_id() != generation.session_id()
            || analytical_dataset.as_str() != SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET
            || operation_timeout.is_zero()
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let generation_digest = generation.generation_digest()?;
        oauth
            .validate_current_receipt(oauth_receipt)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        let package = {
            let authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            if self.coordinator.lifecycle.shutdown_token().is_cancelled()
                || authority.registry.is_none()
            {
                return Err(ResearchIngestCompositionError::ShuttingDown.into());
            }
            let current = authority
                .publication_sources
                .get(generation.profile())
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation != *generation
                || current.metadata != *generation.metadata()
                || current.rights != generation.rights
                || current.registration.source_id() != generation.metadata().source_id()
                || current.registration.revision() != generation.metadata().revision()
                || current.admission.generation_digest != Some(generation_digest)
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration.into());
            }
            current.admission.ensure_live()?;
            oauth
                .validate_current_receipt(oauth_receipt)
                .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
            let admission = Arc::new(SchwabCompositeMarketRuntimeAdmission {
                generation_digest,
                admission: current.admission.clone(),
                oauth,
                oauth_receipt,
            });
            admission.ensure_exact_current()?;
            let closure = Arc::new(SchwabMarketPublicationClosure::try_new(
                Arc::clone(&self.coordinator.research),
                generation.clone(),
                current.rights.clone(),
                doctor,
                admission,
            )?);
            let generation_authority =
                closure.bind_rest_quote_sink(operation_timeout, analytical_dataset.clone())?;
            let point_in_time = MarketEventPointInTimeSelector::new(
                Arc::clone(&self.coordinator.research),
                analytical_dataset,
                generation.metadata().source_id().clone(),
            );
            let (durable_writer, durable_read) = MarketEventDurableRead::channel(point_in_time);
            SchwabRestQuotePublicationPackage {
                generation: generation_authority,
                durable_writer,
                durable_read,
            }
        };
        Ok(package)
    }

    /// Registers one provider adapter bound to an exact onboarding/runtime generation.
    pub(crate) fn register_provider_publication_generation(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || generation.metadata().source_id() != rights.source_id()
            || rights != generation.rights
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let profile = generation.profile().clone();
        let registered_at = super::system_timestamp()
            .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
        if !generation.metadata().is_effective_at(registered_at) {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
            || authority.sources.contains_key(&profile)
            || authority.pending_replacements.contains_key(&profile)
        {
            return Err(ResearchIngestCompositionError::DuplicateProfile);
        }
        let super::CoordinatorAuthority {
            registry,
            sources: _,
            publication_sources,
            pending_replacements: _,
            selections: _,
            alpaca_historical: _,
        } = &mut *authority;
        if let Some(current) = publication_sources.get_mut(&profile) {
            if current.generation == generation {
                if current.metadata != *generation.metadata()
                    || current.rights != rights
                    || current.registration.source_id() != generation.metadata().source_id()
                    || current.registration.revision() != generation.metadata().revision()
                    || (!current.admission.revocation_drained()
                        && current.admission.ensure_live().is_err())
                {
                    return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
                }
                if current.admission.revocation_drained() {
                    current.admission = ResearchProviderAdmission::new(Some(&generation))?;
                }
                return Ok(generation);
            }
            if !current.admission.revocation_drained()
                || !generation.is_exact_successor_of(&current.generation)?
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            if current.metadata != *generation.metadata() {
                current.registration = Box::new(
                    registry
                        .as_mut()
                        .ok_or(ResearchIngestCompositionError::ShuttingDown)?
                        .replace_metadata(
                            current.registration.as_ref(),
                            generation.metadata().clone(),
                            registered_at,
                        )?,
                );
            }
            current.metadata = generation.metadata().clone();
            current.registered_at = registered_at;
            current.rights = rights;
            current.generation = generation.clone();
            current.admission = ResearchProviderAdmission::new(Some(&generation))?;
            return Ok(generation);
        }
        let registration = registry
            .as_mut()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .register_or_resume_exact(generation.metadata().clone(), registered_at)?;
        let admission = ResearchProviderAdmission::new(Some(&generation))?;
        publication_sources.insert(
            profile,
            super::RegisteredPublicationSource {
                metadata: generation.metadata().clone(),
                registered_at,
                registration: Box::new(registration),
                rights,
                generation: generation.clone(),
                admission,
            },
        );
        Ok(generation)
    }

    /// Registers one provider adapter bound to an exact onboarding/runtime generation.
    pub(crate) fn register_provider_source<S>(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        if source.metadata() != generation.metadata()
            || rights != generation.rights
            || &rights.source_id != source.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        self.coordinator.register_source_inner(
            generation.profile().clone(),
            source,
            rights,
            Some(generation),
        )
    }

    /// Registers one Treasury adapter while retaining its exact typed allocation beside the
    /// erased extraction source.
    pub(crate) fn register_treasury_provider_source(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: Arc<market_squawk_adapter_treasury::TreasurySource>,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError> {
        if source.metadata() != generation.metadata()
            || rights != generation.rights
            || &rights.source_id != source.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let metadata = source.metadata().clone();
        self.coordinator.register_source_capability_inner(
            generation.profile().clone(),
            metadata,
            super::RegisteredSourceCapability::treasury(source),
            rights,
            Some(generation),
        )
    }
}

impl ProductionResearchIngestCoordinator {
    /// Acquires the sole non-forgeable crypto canonical-publication authority for one exact active
    /// runtime generation.
    pub(crate) async fn acquire_crypto_market_publication_authority(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        caller: CancellationToken,
        deadline: Instant,
        analytical_dataset: DatasetId,
    ) -> Result<CryptoMarketPublicationAuthority, ResearchIngestCompositionError> {
        if analytical_dataset.as_str() != "market_squawk.market_events" {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let operation = self
            .acquire_provider_publication_operation(generation, caller, deadline)
            .await?;
        let publication = Arc::new(
            CryptoMarketPublicationClosure::try_new(
                self.research.clone(),
                operation.source().clone(),
                operation.rights().clone(),
                operation.source_registered_at(),
            )
            .map_err(|_error| ResearchIngestCompositionError::InvalidRuntimeGeneration)?,
        );
        let precommit = operation.precommit_authority();
        let authority = CryptoMarketPublicationAuthority {
            operation,
            publication,
            analytical_dataset,
            precommit,
        };
        authority.validate_precommit()?;
        Ok(authority)
    }

    /// Acquires one exact specialized-provider admission and retains its cancellation and
    /// publication lease across network, raw sealing, and final commit.
    pub(crate) async fn acquire_provider_publication_operation(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        caller: CancellationToken,
        deadline: Instant,
    ) -> Result<ResearchProviderPublicationOperation, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() || caller.is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let generation_digest = generation.generation_digest()?;
        let (source, rights, source_registered_at, admission) = {
            let authority = self
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            let current = authority
                .publication_sources
                .get(generation.profile())
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation != *generation
                || current.metadata != *generation.metadata()
                || current.rights != generation.rights
                || current.registration.source_id() != generation.metadata().source_id()
                || current.registration.revision() != generation.metadata().revision()
                || current.admission.generation_digest != Some(generation_digest)
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.ensure_live()?;
            (
                current.metadata.clone(),
                current.rights.clone(),
                current.registered_at,
                current.admission.clone(),
            )
        };
        let cancellation = CancellationToken::new();
        let signal = cancellation.clone();
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
                () = tokio::time::sleep_until(deadline.into()) => signal.cancel(),
            }
        });
        let lease = admission.acquire_publication_lease();
        tokio::pin!(lease);
        let publication = tokio::select! {
            biased;
            () = caller.cancelled() => Err(ResearchIngestCompositionError::StaleRuntimeGeneration),
            () = shutdown.cancelled() => Err(ResearchIngestCompositionError::ShuttingDown),
            () = revoked.cancelled() => Err(ResearchIngestCompositionError::StaleRuntimeGeneration),
            () = tokio::time::sleep_until(deadline.into()) => Err(ResearchIngestCompositionError::StaleRuntimeGeneration),
            result = lease.as_mut() => result,
        };
        let publication = match publication {
            Ok(publication) => Arc::new(publication),
            Err(error) => {
                cancellation.cancel();
                watcher.abort();
                return Err(error);
            }
        };
        let operation = ResearchProviderPublicationOperation {
            generation: generation.clone(),
            source,
            rights,
            source_registered_at,
            publication,
            cancellation,
            watcher,
        };
        operation.validate_precommit()?;
        Ok(operation)
    }

    /// Returns one coherent, nonblocking count of callable provider runtime generations.
    pub fn active_provider_runtime_count(&self) -> Result<usize, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let authority = self
            .authority
            .try_lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        let extraction = authority
            .sources
            .values()
            .try_fold(0_usize, |count, source| {
                if source.generation.is_some() && source.admission.ensure_live().is_ok() {
                    count
                        .checked_add(1)
                        .ok_or(ResearchIngestCompositionError::AuthorityUnavailable)
                } else {
                    Ok(count)
                }
            })?;
        authority
            .publication_sources
            .values()
            .try_fold(extraction, |count, source| {
                if source.admission.ensure_live().is_ok() {
                    count
                        .checked_add(1)
                        .ok_or(ResearchIngestCompositionError::AuthorityUnavailable)
                } else {
                    Ok(count)
                }
            })
    }

    /// Returns the exact callable generation currently published for one provider profile.
    pub fn provider_runtime_generation(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<ResearchProviderRuntimeGeneration>, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if let Some(source) = authority.sources.get(profile) {
            if source.admission.ensure_live().is_err() {
                return Ok(None);
            }
            return source
                .generation
                .clone()
                .map(Some)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable);
        }
        let Some(source) = authority.publication_sources.get(profile) else {
            return Ok(None);
        };
        if source.admission.ensure_live().is_err() {
            return Ok(None);
        }
        Ok(Some(source.generation.clone()))
    }
}

impl ResearchProviderRuntimeMutationAuthority {
    /// Prepares an exact replacement for a specialized provider that publishes but does not expose
    /// generic extraction authority.
    pub(crate) fn prepare_provider_publication_replacement(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || !candidate.is_exact_successor_of(&expected)?
            || rights != candidate.rights
            || rights.source_id() != candidate.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        let profile = candidate.profile().clone();
        let token = Uuid::new_v4();
        let candidate_admission = ResearchProviderAdmission::new_pending(&candidate)?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.registry.is_none() || authority.sources.contains_key(&profile) {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        if authority.pending_replacements.contains_key(&profile) {
            return Err(ResearchIngestCompositionError::ReplacementInProgress);
        }
        let current = authority
            .publication_sources
            .get(&profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != expected
            || current.metadata != *expected.metadata()
            || current.rights != expected.rights
            || current.admission.ensure_live().is_err()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        authority
            .pending_replacements
            .insert(profile.clone(), token);
        let prepared = PreparedResearchProviderPublicationReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile,
            token,
            expected: expected.clone(),
            candidate: candidate.clone(),
            candidate_rights: rights,
            candidate_admission,
            completed: false,
        };
        Ok(ResearchProviderRuntimeReplacement {
            coordinator: Arc::clone(&self.coordinator),
            expected,
            candidate,
            state: Some(ResearchProviderRuntimeReplacementState::PreparedPublication(prepared)),
        })
    }

    /// Prepares an exact expected-old to exact-new adapter replacement without publishing it.
    pub(crate) fn prepare_provider_replacement<S>(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        self.prepare_provider_replacement_capability(
            expected,
            candidate,
            super::RegisteredSourceCapability::erased(source),
            rights,
        )
    }

    /// Prepares an exact Treasury successor while retaining the candidate's one typed allocation.
    pub(crate) fn prepare_treasury_provider_replacement(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: Arc<market_squawk_adapter_treasury::TreasurySource>,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError> {
        self.prepare_provider_replacement_capability(
            expected,
            candidate,
            super::RegisteredSourceCapability::treasury(source),
            rights,
        )
    }

    fn prepare_provider_replacement_capability(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        candidate_capability: super::RegisteredSourceCapability,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || !candidate.is_exact_successor_of(&expected)?
            || candidate_capability.erased.metadata() != candidate.metadata()
            || rights != candidate.rights
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        let profile = candidate.profile().clone();
        let token = Uuid::new_v4();
        let candidate_admission = ResearchProviderAdmission::new_pending(&candidate)?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.contains_key(&profile) {
            return Err(ResearchIngestCompositionError::ReplacementInProgress);
        }
        let current = authority
            .sources
            .get(&profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&expected)
            || current.metadata != expected.metadata
            || current.rights != expected.rights
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if !current
            .typed_capability
            .same_kind(&candidate_capability.typed)
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        authority
            .pending_replacements
            .insert(profile.clone(), token);
        let prepared = PreparedResearchProviderReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile,
            token,
            expected: expected.clone(),
            candidate: candidate.clone(),
            candidate_capability: Some(candidate_capability),
            candidate_admission,
            completed: false,
        };
        Ok(ResearchProviderRuntimeReplacement {
            coordinator: Arc::clone(&self.coordinator),
            expected,
            candidate,
            state: Some(ResearchProviderRuntimeReplacementState::Prepared(prepared)),
        })
    }

    pub(crate) async fn revoke_predecessor(
        &self,
        transaction: &mut ResearchProviderRuntimeReplacement,
    ) -> Result<(), ResearchIngestCompositionError> {
        self.require_bound(transaction)?;
        match transaction.state.as_mut() {
            Some(ResearchProviderRuntimeReplacementState::Prepared(prepared)) => {
                prepared.revoke_predecessor().await
            }
            Some(ResearchProviderRuntimeReplacementState::PreparedPublication(prepared)) => {
                prepared.revoke_predecessor().await
            }
            Some(
                ResearchProviderRuntimeReplacementState::Committed(_)
                | ResearchProviderRuntimeReplacementState::CommittedPublication(_),
            )
            | None => Err(ResearchIngestCompositionError::InvalidRuntimeReplacement),
        }
    }

    pub(crate) fn commit(
        &self,
        transaction: &mut ResearchProviderRuntimeReplacement,
    ) -> Result<(), ResearchIngestCompositionError> {
        self.require_bound(transaction)?;
        let state = transaction
            .state
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        match state {
            ResearchProviderRuntimeReplacementState::Prepared(mut prepared) => {
                match prepared.commit() {
                    Ok(committed) => {
                        transaction.state = Some(
                            ResearchProviderRuntimeReplacementState::Committed(committed),
                        );
                        Ok(())
                    }
                    Err(error) => {
                        transaction.state =
                            Some(ResearchProviderRuntimeReplacementState::Prepared(prepared));
                        Err(error)
                    }
                }
            }
            ResearchProviderRuntimeReplacementState::Committed(committed) => {
                transaction.state = Some(ResearchProviderRuntimeReplacementState::Committed(
                    committed,
                ));
                Ok(())
            }
            ResearchProviderRuntimeReplacementState::PreparedPublication(mut prepared) => {
                match prepared.commit() {
                    Ok(committed) => {
                        transaction.state = Some(
                            ResearchProviderRuntimeReplacementState::CommittedPublication(
                                committed,
                            ),
                        );
                        Ok(())
                    }
                    Err(error) => {
                        transaction.state = Some(
                            ResearchProviderRuntimeReplacementState::PreparedPublication(prepared),
                        );
                        Err(error)
                    }
                }
            }
            ResearchProviderRuntimeReplacementState::CommittedPublication(committed) => {
                transaction.state =
                    Some(ResearchProviderRuntimeReplacementState::CommittedPublication(committed));
                Ok(())
            }
        }
    }

    pub(crate) fn rollback(
        &self,
        mut transaction: ResearchProviderRuntimeReplacement,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.require_bound(&transaction)?;
        match transaction
            .state
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?
        {
            ResearchProviderRuntimeReplacementState::Prepared(prepared) => prepared.rollback(),
            ResearchProviderRuntimeReplacementState::Committed(committed) => committed.rollback(),
            ResearchProviderRuntimeReplacementState::PreparedPublication(prepared) => {
                prepared.rollback()
            }
            ResearchProviderRuntimeReplacementState::CommittedPublication(committed) => {
                committed.rollback()
            }
        }
    }

    pub(crate) fn finalize(
        &self,
        transaction: &mut ResearchProviderRuntimeReplacement,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.require_bound(transaction)?;
        match transaction.state.as_mut() {
            Some(ResearchProviderRuntimeReplacementState::Committed(committed)) => {
                committed.finalize()
            }
            Some(ResearchProviderRuntimeReplacementState::CommittedPublication(committed)) => {
                committed.finalize()
            }
            Some(
                ResearchProviderRuntimeReplacementState::Prepared(_)
                | ResearchProviderRuntimeReplacementState::PreparedPublication(_),
            )
            | None => Err(ResearchIngestCompositionError::InvalidRuntimeReplacement),
        }
    }

    /// Drains and releases one exact SEC generation without durably revoking its source history.
    pub(crate) async fn revoke_sec_provider_generation_and_release(
        &self,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<(), ResearchIngestCompositionError> {
        if expected.profile().as_str() != SEC_EDGAR_PROFILE_ID
            || expected.metadata().source_id().as_str() != SEC_EDGAR_SOURCE_ID
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let admission = {
            let mut authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            let current = authority
                .sources
                .get(expected.profile())
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation.as_ref() != Some(expected)
                || current.metadata != *expected.metadata()
                || current.registration.source_id() != expected.metadata().source_id()
                || current.registration.revision() != expected.metadata().revision()
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.revoke();
            let admission = current.admission.clone();
            authority.selections.revoke_profile(expected.profile());
            admission
        };
        admission.revoke_and_drain().await;

        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        let super::CoordinatorAuthority {
            registry,
            sources,
            publication_sources: _,
            pending_replacements: _,
            selections: _,
            alpaca_historical: _,
        } = &mut *authority;
        let current = sources
            .get(expected.profile())
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(expected)
            || !current.admission.matches(&admission)
            || !current.admission.revocation_drained()
            || current.metadata != *expected.metadata()
            || current.registration.source_id() != expected.metadata().source_id()
            || current.registration.revision() != expected.metadata().revision()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        registry
            .as_mut()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .release_process_registration_exact(current.registration.as_ref())?;
        let removed = sources
            .remove(expected.profile())
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if removed.generation.as_ref() != Some(expected) || !removed.admission.matches(&admission) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        Ok(())
    }

    /// Revokes exactly one callable generation and every retained receipt minted from it.
    pub(crate) async fn revoke_provider_generation(
        &self,
        profile: &SourceIdentifier,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<(), ResearchIngestCompositionError> {
        let admission = {
            let mut authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            if let Some(current) = authority.publication_sources.get(profile) {
                if &current.generation != expected {
                    return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
                }
                current.admission.revoke();
                current.admission.clone()
            } else {
                let current = authority
                    .sources
                    .get(profile)
                    .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
                if current.generation.as_ref() != Some(expected) {
                    return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
                }
                current.admission.revoke();
                let admission = current.admission.clone();
                authority.selections.revoke_profile(profile);
                admission
            }
        };
        admission.revoke_and_drain().await;
        Ok(())
    }
}

impl ResearchProviderRuntimeReplacement {
    pub(crate) const fn expected(&self) -> &ResearchProviderRuntimeGeneration {
        &self.expected
    }
}

impl std::fmt::Debug for ResearchProviderRuntimeMutationAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderRuntimeMutationAuthority")
            .field("coordinator", &"[SEALED]")
            .finish()
    }
}

impl std::fmt::Debug for ResearchProviderRuntimeReplacement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderRuntimeReplacement")
            .field("expected", &self.expected)
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::path::Path;
    use std::pin::Pin;
    use std::str::FromStr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use market_squawk_adapter_schwab::{
        AccessTokenAdmission, CallbackOutcome, OAuthCallback, ProtectedSchwabOAuthAuthority,
        ProviderIdentifier, QuoteField, QuoteRequest, RequestAdmission, ResponseHeaderEvidence,
        RestExecutionOutcome, RestTransportBounds, SchwabHttpWire, SchwabHttpWireRequest,
        SchwabHttpWireResponse, SchwabOAuthAuthorityConfiguration, SchwabOAuthInteraction,
        SchwabOAuthSecretPolicy, SchwabOAuthWire, SchwabOAuthWireError, SchwabOAuthWireRequest,
        SchwabOAuthWireResponse, SchwabRestExecutor, SchwabTransportTelemetry,
        TransientAccessToken,
    };
    use market_squawk_data::{
        CatalogConfig, CatalogResultLimits, ObjectStoreConfig, RightsBasis, SourceOperation,
    };
    use market_squawk_domain::{
        AssetClass, AssignmentVerification, AuthorizationBasis, ChecksumCapability, CoverageDelay,
        Currency, DataQuality, DeliveryEvidence, Denomination, DigestAlgorithm, EffectiveInterval,
        EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier, ExternalIdentifierRecord,
        ExternalIdentifierRecordInput, IdentifierEntitlement, IdentifierRightsPolicyReference,
        InstrumentDefinition, InstrumentDefinitionInput, InstrumentDefinitionRevision,
        InstrumentId, IntegrityRule, LotSize, MetadataRevision, ProviderChannel,
        ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
        ProviderInstrumentId, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
        SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier,
        TickSize, Ticker, Timestamp, TradingStatus, VenueId,
    };
    use market_squawk_platform::{
        EncryptedFileSecretStore, LocalPaths, SecretCancellation, SecretGeneration,
        SecretInteractionPolicy, SecretKey, SecretOperationControl, SecretRef, SecretStore,
        SecretValue,
    };
    use market_squawk_sources::{
        AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
        ChecksumValidationProfile, CoverageTopology, EndpointPolicy, FreshnessPolicy,
        HistoricalCapability, InstrumentCoverage, LiveCoverageDeclaration, LiveCoverageRule,
        LiveProtocolProfile, NetworkAccessPolicy, ProviderBudgetPolicy, ProviderCapabilityRevision,
        ProviderNumericPolicy, RuntimeCapabilityDisposition, SCHWAB_MARKET_DATA_SURFACE_ID,
        SchwabMarketDataDoctorObservation, SchwabMarketDataDoctorReceiptInput,
        SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily, SchwabMarketDataFamilyEvidence,
        SchwabUserPreferenceDoctorEvidence, SemanticInterpretationProfile,
        SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
        SourceMetadataInput, SourceProtocolProfile,
    };
    use rust_decimal::Decimal;

    use crate::ResearchService;
    use crate::application::market_runtime::{
        SchwabRestQuoteInstrumentBinding, SchwabRestQuoteProducer, SchwabRestQuoteSealFirstSink,
        SchwabRestQuoteSourceEvidence,
    };
    use crate::live_source::{
        SchwabRestQuoteCurrentBridge, SchwabRestQuoteCurrentPublication,
        SchwabRestQuoteCurrentRequest,
    };
    use crate::provider_activation::{
        MarketInstrumentBinding, MarketInstrumentReferenceBinding, MarketSubscriptionPriority,
        SchwabMarketDataAccountActivation, SchwabMarketDataActivationError,
    };
    use crate::provider_onboarding::SchwabOAuthMarketAuthority;

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn exact_generation_revocation_drains_the_publication_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        let admission = ResearchProviderAdmission::new(None)?;
        let publication = admission.acquire_publication_lease().await?;
        let cancellation = admission.cancellation().clone();
        let revoking = admission.clone();
        let drain = tokio::spawn(async move {
            revoking.revoke_and_drain().await;
        });

        cancellation.cancelled().await;
        assert!(!drain.is_finished());
        assert!(publication.validate_precommit().is_err());

        drop(publication);
        tokio::time::timeout(Duration::from_secs(1), drain).await??;
        assert!(admission.revocation_drained());
        Ok(())
    }

    #[tokio::test]
    async fn schwab_quote_attempt_retains_exact_epoch_through_precommit_and_fails_closed()
    -> TestResult {
        let directory = tempfile::tempdir()?;
        let session_id = Uuid::new_v4();
        let (oauth, wire, secret_reference) =
            scripted_market_authority(directory.path().join("stable-oauth"), session_id, 1_800, 60)
                .await?;
        let (token, epoch) =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&oauth, 1).await?;
        let oauth_receipt = epoch.receipt();
        let (durable, evidence, binding) = quote_publication_fixture(
            directory.path(),
            session_id,
            secret_reference,
            oauth.clone(),
            oauth_receipt,
        )?;
        let generation = market_squawk_domain::ConnectionGeneration::new(1)?;
        let sink = SchwabRestQuoteSealFirstSink::new(
            Arc::clone(&durable),
            Arc::new(PublishedCurrentBridge(generation)),
        );
        let accepted = SchwabRestQuoteProducer::publish_test_completed_response(
            &sink,
            executed_quote(token, oauth_receipt.access_issued_at_unix_seconds()).await?,
            evidence.clone(),
            vec![binding.clone()],
            epoch,
            generation,
        )
        .await?;
        assert_eq!(accepted.published(), 1);
        assert_eq!(wire.exchange_count(), 1);

        let (token, revoked_epoch) =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&oauth, 1).await?;
        let completed =
            executed_quote(token, oauth_receipt.access_issued_at_unix_seconds()).await?;
        oauth.revoke_test_authority();
        assert!(
            revoked_epoch
                .validate_current(revoked_epoch.receipt())
                .is_err()
        );
        assert!(
            SchwabRestQuoteProducer::publish_test_completed_response(
                &sink,
                completed,
                evidence,
                vec![binding],
                revoked_epoch,
                generation,
            )
            .await
            .is_err()
        );
        assert!(matches!(
            durable.latest_source_health()?,
            Some(super::super::schwab_market::SchwabRestQuoteSourceHealthOutcome::PostSealPublicationUnavailable {
                sealed_receipt_digest: Some(_),
                ..
            })
        ));

        let (rotating, rotating_wire, _secret_reference) =
            scripted_market_authority(directory.path().join("rotating-oauth"), session_id, 30, 300)
                .await?;
        let mut quote_dispatches = 0_u8;
        let attempt =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&rotating, 1).await;
        if attempt.is_ok() {
            quote_dispatches += 1;
        }
        assert!(matches!(
            attempt,
            Err(SchwabMarketDataActivationError::DoctorRenewalRequired)
        ));
        assert_eq!(rotating_wire.exchange_count(), 2);
        assert_eq!(quote_dispatches, 0);
        Ok(())
    }

    #[derive(Debug)]
    struct PublishedCurrentBridge(market_squawk_domain::ConnectionGeneration);

    impl SchwabRestQuoteCurrentBridge for PublishedCurrentBridge {
        fn publish_current(
            &self,
            _request: SchwabRestQuoteCurrentRequest<'_>,
        ) -> SchwabRestQuoteCurrentPublication {
            SchwabRestQuoteCurrentPublication::Published {
                observations: 1,
                source_generation: self.0,
            }
        }
    }

    #[derive(Debug)]
    struct ScriptedSchwabOAuthWire {
        initial_lifetime_seconds: u64,
        exchanges: AtomicUsize,
    }

    impl ScriptedSchwabOAuthWire {
        fn exchange_count(&self) -> usize {
            self.exchanges.load(Ordering::SeqCst)
        }
    }

    impl SchwabOAuthWire for ScriptedSchwabOAuthWire {
        fn exchange(
            &self,
            _request: SchwabOAuthWireRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move {
                let attempt = self.exchanges.fetch_add(1, Ordering::SeqCst);
                let body = match attempt {
                    0 => format!(
                        r#"{{"access_token":"initial-access","refresh_token":"initial-refresh","token_type":"Bearer","expires_in":{},"scope":"market-data"}}"#,
                        self.initial_lifetime_seconds
                    )
                    .into_bytes(),
                    1 => br#"{"access_token":"rotated-access","refresh_token":"rotated-refresh","token_type":"Bearer","expires_in":1800,"scope":"market-data"}"#
                        .to_vec(),
                    _ => return Err(SchwabOAuthWireError::Protocol),
                };
                SchwabOAuthWireResponse::try_new(200, body, nonzero(4 * 1024))
            })
        }
    }

    async fn scripted_market_authority(
        root: impl AsRef<Path>,
        session_id: Uuid,
        initial_lifetime_seconds: u64,
        refresh_early_seconds: u64,
    ) -> Result<
        (
            SchwabOAuthMarketAuthority,
            Arc<ScriptedSchwabOAuthWire>,
            SecretRef,
        ),
        Box<dyn std::error::Error>,
    > {
        let root = root.as_ref();
        let secrets = Arc::new(EncryptedFileSecretStore::try_open(
            root.join("secrets"),
            SecretValue::new("schwab publication attempt test unlock".to_owned())?,
        )?);
        let control = SecretOperationControl::try_new(
            "schwab-publication-attempt-test",
            Instant::now() + Duration::from_secs(30),
            0,
            SecretInteractionPolicy::Forbid,
            SecretCancellation::new(),
        )?;
        let application_credential = secrets.create(
            &SecretKey::try_new("market-squawk.schwab", "test-application")?,
            SecretGeneration::new(1)?,
            SecretValue::new(
                r#"{"version":1,"app_key":"test-app-key","app_secret":"test-app-secret"}"#
                    .to_owned(),
            )?,
            &control,
        )?;
        let wire = Arc::new(ScriptedSchwabOAuthWire {
            initial_lifetime_seconds,
            exchanges: AtomicUsize::new(0),
        });
        let authority = Arc::new(
            ProtectedSchwabOAuthAuthority::try_open(
                root.join("authority"),
                SchwabOAuthAuthorityConfiguration::try_new(
                    secrets,
                    wire.clone(),
                    application_credential.clone(),
                    SchwabOAuthSecretPolicy::try_new(Duration::from_secs(30), 0)?,
                    parse_bounds(),
                    AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1)),
                    refresh_early_seconds,
                )?,
            )
            .await?,
        );
        let callback = match OAuthCallback::parse(
            "https://127.0.0.1:8182/?code=one-time&state=publication-attempt",
            "publication-attempt",
            RequestAdmission::new(nonzero(4 * 1024), NonZeroUsize::MIN),
        )? {
            CallbackOutcome::Authorized(callback) => callback,
            CallbackOutcome::Denied { .. } => return Err("test callback was denied".into()),
        };
        let issued_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let receipt = authority
            .complete_authorization(&callback, issued_at, SchwabOAuthInteraction::Background)
            .await?;
        Ok((
            SchwabOAuthMarketAuthority::from_test_authority(
                session_id,
                receipt,
                Arc::clone(&authority),
            ),
            wire,
            application_credential,
        ))
    }

    #[derive(Debug)]
    struct QuoteHttpWire(Mutex<Option<SchwabHttpWireResponse>>);

    impl SchwabHttpWire for QuoteHttpWire {
        fn get<'a>(
            &'a self,
            _request: SchwabHttpWireRequest<'a>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            SchwabHttpWireResponse,
                            market_squawk_adapter_schwab::SchwabTransportError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.0
                    .lock()
                    .map_err(|_poisoned| {
                        market_squawk_adapter_schwab::SchwabTransportError::Protocol
                    })?
                    .take()
                    .ok_or(market_squawk_adapter_schwab::SchwabTransportError::Protocol)
            })
        }
    }

    async fn executed_quote(
        token: TransientAccessToken,
        source_seconds: u64,
    ) -> Result<market_squawk_adapter_schwab::ExecutedRestResponse, Box<dyn std::error::Error>>
    {
        let request = QuoteRequest::try_new(
            vec![ProviderIdentifier::try_new("AAPL".to_owned())?],
            vec![QuoteField::Quote],
            None,
            RequestAdmission::new(nonzero(4 * 1024), NonZeroUsize::MIN),
        )?;
        let body = Bytes::from(format!(
            r#"{{"AAPL":{{"assetMainType":"EQUITY","realtime":true,"quote":{{"bidPrice":100.12,"askPrice":100.13,"bidSize":2,"askSize":3,"quoteTime":{}}}}}}}"#,
            source_seconds.saturating_mul(1_000)
        ));
        let bounds = RestTransportBounds::try_new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(2),
            nonzero(64 * 1024),
            nonzero(8),
            nonzero(2 * 1024),
        )?;
        let response = SchwabHttpWireResponse::try_new(
            200,
            request.request().url().to_owned(),
            Some(u64::try_from(body.len())?),
            vec![ResponseHeaderEvidence::try_new(
                "content-type".to_owned(),
                b"application/json".to_vec(),
            )?],
            body,
            bounds,
        )?;
        let executor = SchwabRestExecutor::try_new(
            Arc::new(QuoteHttpWire(Mutex::new(Some(response)))),
            bounds,
            parse_bounds(),
            AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1)),
            SchwabTransportTelemetry::default(),
        )?;
        match executor
            .execute(request.request(), &token, CancellationToken::new())
            .await?
        {
            RestExecutionOutcome::Accepted(response) => Ok(response),
            _ => Err("mock quote response was not accepted".into()),
        }
    }

    fn quote_publication_fixture(
        root: &Path,
        session_id: Uuid,
        secret_reference: SecretRef,
        oauth: SchwabOAuthMarketAuthority,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<
        (
            Arc<super::super::schwab_market::SchwabRestQuoteGenerationAuthority>,
            SchwabRestQuoteSourceEvidence,
            SchwabRestQuoteInstrumentBinding,
        ),
        Box<dyn std::error::Error>,
    > {
        let instrument_id = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
        let source_id = SourceId::try_from("schwab-trader-api")?;
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let product = ProviderProduct::new(SourceIdentifier::try_from("schwab-rest")?);
        let channel = ProviderChannel::new(SourceIdentifier::try_from("schwab-rest-quotes")?);
        let metadata = quote_metadata(
            source_id.clone(),
            instrument_id,
            effective,
            product.clone(),
            channel.clone(),
        )?;
        let binding = quote_binding(source_id.clone(), instrument_id, effective)?;
        let capability_digest = digest(31);
        let parent_rights = digest(32);
        let rights = ResearchRightsAuthority::try_new_scoped(
            source_id,
            RightsBasis::reviewed_terms("https://developer.schwab.com/terms", digest(33))?,
            parent_rights,
            digest(34),
            timestamp_seconds(oauth_receipt.refresh_expires_at_unix_seconds())?,
            vec![SourceIdentifier::try_from("schwab-rest-quotes-aapl")?],
            vec![SourceOperation::Persist],
        )?;
        let generation = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from(SCHWAB_MARKET_DATA_SURFACE_ID)?,
            session_id,
            ProviderCapabilityRevision::new(1)?,
            capability_digest,
            Some(secret_reference.generation()),
            Some(secret_reference),
            timestamp_seconds(oauth_receipt.access_issued_at_unix_seconds())?,
            metadata.clone(),
            rights.clone(),
        )?;
        let doctor = doctor_receipt(session_id, capability_digest, parent_rights, oauth_receipt)?;
        let paths = LocalPaths::prepare(root.join("research"))?;
        let research = Arc::new(ResearchService::open_or_initialize(
            &paths,
            CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                market_squawk_data::CatalogLimit::new(64)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?);
        let durable = super::super::schwab_market::SchwabRestQuoteGenerationAuthority::bind_test_rest_quote_sink(
            research,
            generation,
            rights,
            doctor.clone(),
            oauth,
            oauth_receipt,
            Duration::from_secs(5),
        )?;
        let evidence =
            SchwabRestQuoteSourceEvidence::try_new(metadata, VenueId::try_from("schwab")?, doctor)?;
        Ok((durable, evidence, binding))
    }

    fn quote_metadata(
        source_id: SourceId,
        instrument_id: InstrumentId,
        effective: EffectiveInterval,
        product: ProviderProduct,
        channel: ProviderChannel,
    ) -> Result<SourceMetadata, Box<dyn std::error::Error>> {
        let provider = SourceIdentifier::try_from("schwab-trader-api")?;
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            AuthorizationBasis::new(SourceIdentifier::try_from("schwab-test-account")?),
            ExactPayloadEvidence::from_content_digest(digest(2)),
            effective,
        );
        let live = LiveCoverageDeclaration::try_new(
            product,
            channel,
            vec![LiveCoverageRule::try_new(
                market_squawk_domain::LiveEventClass::Quote,
                None,
                SnapshotApplicability::NotApplicable {
                    metadata_rule: rule("schwab-rest-quote-snapshot")?,
                },
            )?],
        )?;
        let budget = ProviderBudgetPolicy::try_new(
            BudgetScope::for_authorization(provider.clone(), &authorization)?,
            NonZeroU32::new(20).ok_or("invalid request budget")?,
            NonZeroU64::new(15 * 60 * 1_000_000_000).ok_or("invalid budget window")?,
            NonZeroU16::new(1).ok_or("invalid concurrency budget")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("invalid backoff")?,
                NonZeroU64::new(60_000_000_000).ok_or("invalid backoff cap")?,
                1_000,
            )?,
        )?;
        Ok(SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            source_id,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("schwab-rest-quote-v1")?),
                ExactPayloadEvidence::from_content_digest(digest(3)),
            ),
            SourceClass::Broker,
            provider,
            authorization,
            SourceCoverage::try_instrument(
                ExactPayloadEvidence::from_content_digest(digest(4)),
                effective,
                vec![AssetClass::Index],
                CoverageTopology::single_venue(VenueId::try_from("schwab")?),
                InstrumentCoverage::enumerated(vec![instrument_id])?,
                Some(live),
                CoverageDelay::RealTime,
                DeliveryEvidence::AuthorizedBroker,
            )?,
            DataQuality::DirectUnverified,
            NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([
                "https://api.schwabapi.com/marketdata/v1/quotes",
            ])?),
            FreshnessPolicy::try_new(
                60_000_000_000,
                60_000_000_000,
                60_000_000_000,
                60_000_000_000,
                1_000_000_000,
            )?,
            Some(budget),
            SourceCapabilities::new(
                true,
                true,
                SequenceCapability::Unsupported,
                ChecksumCapability::Unsupported,
                HistoricalCapability::None,
                true,
            ),
            SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
                rule("schwab-rest-decoder")?,
                SemanticInterpretationProfile::new(
                    rule("schwab-rest-aggressor")?,
                    rule("schwab-rest-auction")?,
                    rule("schwab-rest-status")?,
                    rule("schwab-rest-corporate-action")?,
                ),
                rule("schwab-rest-timestamp")?,
                SequenceValidationProfile::Unsupported {
                    rule: rule("schwab-rest-no-sequence")?,
                },
                ChecksumValidationProfile::Unsupported {
                    rule: rule("schwab-rest-no-checksum")?,
                },
                true,
                ProviderNumericPolicy::ExactDecimalLexeme,
            ))),
        ))?)
    }

    fn quote_binding(
        source_id: SourceId,
        instrument_id: InstrumentId,
        effective: EffectiveInterval,
    ) -> Result<SchwabRestQuoteInstrumentBinding, Box<dyn std::error::Error>> {
        let provider_identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
            instrument_id,
            source_id: source_id.clone(),
            provider_instrument_id: ProviderInstrumentId::try_from("AAPL")?,
            evidence: ProviderIdentityEvidence::from_content_digest(digest(10)),
            source_timestamp: Some(Timestamp::from_unix_nanos(0)),
            observed_at: Timestamp::from_unix_nanos(1),
            metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                "schwab-provider-identity-v1",
            )?),
            validity: effective,
            supersedes: None,
        });
        let identifier = ExternalIdentifierRecord::new(ExternalIdentifierRecordInput {
            identifier: ExternalIdentifier::Ticker(Ticker::try_from("AAPL")?),
            assignment_verification: AssignmentVerification::VerifiedAssigned,
            source_id: SourceId::try_from("reference-master")?,
            source_evidence: ExactPayloadEvidence::from_content_digest(digest(11)),
            source_timestamp: Some(Timestamp::from_unix_nanos(0)),
            observed_at: Timestamp::from_unix_nanos(1),
            validity: effective,
            rights_policy: IdentifierRightsPolicyReference::new(
                SourceIdentifier::try_from("reference-personal-use-v1")?,
                IdentifierEntitlement::LicensedInternalUse,
                SourceIdentifier::try_from("https://example.test/reference")?,
            ),
        });
        let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
            instrument_id,
            definition_revision: InstrumentDefinitionRevision::try_from(1_u64)?,
            asset_class: AssetClass::Index,
            primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
            quote_currency: Currency::try_from("USD")?,
            tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
            lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
            contract_multiplier: Decimal::ONE,
            venue_mappings: Vec::new(),
            provider_identities: vec![provider_identity.clone()],
            identifiers: vec![identifier.clone()],
            trading_status: TradingStatus::Active,
        })?;
        SchwabRestQuoteInstrumentBinding::try_new(
            MarketInstrumentBinding::try_new(
                MarketSubscriptionPriority::CurrentlyViewed,
                definition,
                provider_identity,
                MarketInstrumentReferenceBinding::AssignedExternalIdentifier(identifier),
            )?,
            &source_id,
        )
        .map_err(Into::into)
    }

    fn doctor_receipt(
        session_id: Uuid,
        capability_digest: EvidenceDigest,
        rights_decision_digest: EvidenceDigest,
        oauth: SchwabOAuthAuthorityReceipt,
    ) -> Result<SchwabMarketDataDoctorReceiptV1, Box<dyn std::error::Error>> {
        let completed_at = timestamp_seconds(oauth.access_issued_at_unix_seconds())?;
        let access_expires_at = timestamp_seconds(oauth.access_expires_at_unix_seconds())?;
        let refresh_expires_at = timestamp_seconds(oauth.refresh_expires_at_unix_seconds())?;
        let families = schwab_families()
            .into_iter()
            .map(|family| {
                let available = family == SchwabMarketDataFamily::Quotes;
                SchwabMarketDataFamilyEvidence {
                    family,
                    disposition: if available {
                        RuntimeCapabilityDisposition::Available
                    } else {
                        RuntimeCapabilityDisposition::NotProbed
                    },
                    disposition_evidence_sha256: digest(20),
                    observation_sha256: available.then(|| digest(21)),
                    observed_at: available.then_some(completed_at),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(SchwabMarketDataDoctorReceiptV1::try_new(
            SchwabMarketDataDoctorReceiptInput {
                surface_id: SourceIdentifier::try_from(SCHWAB_MARKET_DATA_SURFACE_ID)?,
                session_identifier: SourceIdentifier::try_from(session_id.to_string())?,
                application_credential_generation: SecretGeneration::new(1)?,
                application_credential_reference_sha256: oauth
                    .credential_authority()
                    .application_credential_reference_sha256(),
                capability_revision: ProviderCapabilityRevision::new(1)?,
                capability_digest,
                public_configuration_digest: digest(22),
                rights_decision_digest,
                rate_policy_digest: digest(23),
                data_quality: DataQuality::DirectUnverified,
                observation: SchwabMarketDataDoctorObservation {
                    provider_observation_origin:
                        SchwabMarketDataDoctorObservation::provider_observed_origin()?,
                    access_token_generation: oauth.generation().get(),
                    access_issued_at: completed_at,
                    access_expires_at,
                    refresh_authorized_at: timestamp_seconds(
                        oauth.refresh_authorized_at_unix_seconds(),
                    )?,
                    refresh_expires_at,
                    user_preference: SchwabUserPreferenceDoctorEvidence {
                        endpoint_contract_sha256: digest(24),
                        request_sha256: digest(25),
                        response_sha256: digest(26),
                        status_code: 200,
                        response_bytes: 1,
                        received_at: completed_at,
                        latency_nanos: 1,
                        market_data_principal_sha256: digest(27),
                        streamer_bootstrap_sha256: digest(28),
                        market_data_offer_sha256: None,
                    },
                    quote_delay: Some(CoverageDelay::RealTime),
                    families,
                    completed_at,
                },
                exclusive_expires_at: Timestamp::from_unix_nanos(
                    completed_at
                        .unix_nanos()
                        .checked_add(SchwabMarketDataDoctorReceiptV1::VALIDITY_NANOS)
                        .ok_or("doctor expiry overflow")?
                        .min(access_expires_at.unix_nanos())
                        .min(refresh_expires_at.unix_nanos()),
                ),
                predecessor_digest: None,
            },
        )?)
    }

    fn schwab_families() -> [SchwabMarketDataFamily; 19] {
        use SchwabMarketDataFamily::*;
        [
            Quotes,
            PriceHistory,
            OptionChains,
            ExpirationChains,
            Movers,
            MarketHours,
            Instruments,
            LevelOneEquities,
            LevelOneOptions,
            LevelOneFutures,
            LevelOneFuturesOptions,
            LevelOneForex,
            NyseBook,
            NasdaqBook,
            OptionsBook,
            ChartEquity,
            ChartFutures,
            ScreenerEquity,
            ScreenerOption,
        ]
    }

    fn parse_bounds() -> market_squawk_adapter_schwab::ParseBounds {
        market_squawk_adapter_schwab::ParseBounds::new(
            nonzero(64 * 1024),
            nonzero(64),
            nonzero(2_048),
            nonzero(16),
            32,
            8 * 1024,
        )
    }

    fn timestamp_seconds(seconds: u64) -> Result<Timestamp, Box<dyn std::error::Error>> {
        Ok(Timestamp::from_unix_nanos(i64::try_from(
            seconds.checked_mul(1_000_000_000).ok_or("clock overflow")?,
        )?))
    }

    fn rule(value: &str) -> Result<IntegrityRule, Box<dyn std::error::Error>> {
        Ok(IntegrityRule::new(
            SourceIdentifier::try_from(value)?,
            RuleVersion::new(1)?,
        ))
    }

    fn digest(byte: u8) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
    }
}
