//! Generation-bound publication of callable research-provider adapters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use market_squawk_data::{IngestError, IngestPrecommitAuthority};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{SecretGeneration, SecretRef};
use market_squawk_sources::{ProviderCapabilityRevision, SourceMetadata};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    ResearchIngestCompositionError, ResearchRightsAuthority,
};

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
            rights_authorization_evidence: EvidenceDigest,
            rights_authorization_expires_at: Option<market_squawk_domain::Timestamp>,
        }

        digest_runtime_wire(
            b"market-squawk/research-provider-runtime-generation/v2\0",
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
                rights_authorization_evidence: self.rights.authorization_evidence,
                rights_authorization_expires_at: self.rights.authorization_expires_at,
            },
        )
    }

    /// Compatibility alias for the exact generation digest.
    pub fn identity_digest(&self) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
        self.generation_digest()
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
    state: Arc<ResearchProviderAdmissionState>,
    cancellation: CancellationToken,
}

const ADMISSION_ACTIVE: u8 = 0;
const ADMISSION_REVOKING: u8 = 1;
const ADMISSION_DRAINED: u8 = 2;

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

impl ResearchProviderAdmission {
    pub(super) fn new(
        generation: Option<&ResearchProviderRuntimeGeneration>,
    ) -> Result<Self, ResearchIngestCompositionError> {
        Ok(Self {
            generation_digest: generation
                .map(ResearchProviderRuntimeGeneration::generation_digest)
                .transpose()?,
            state: Arc::new(ResearchProviderAdmissionState {
                phase: AtomicU8::new(ADMISSION_ACTIVE),
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

    pub(super) fn matches(&self, other: &Self) -> bool {
        self.generation_digest == other.generation_digest && Arc::ptr_eq(&self.state, &other.state)
    }

    pub(super) fn revoke(&self) {
        self.begin_revocation();
    }

    fn begin_revocation(&self) {
        let _phase = self.state.phase.compare_exchange(
            ADMISSION_ACTIVE,
            ADMISSION_REVOKING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.cancellation.cancel();
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

/// Fully constructed replacement held outside the callable runtime until exact commit.
pub struct PreparedResearchProviderReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_source: Option<Arc<dyn ManagedResearchExtractionSource>>,
    candidate_admission: ResearchProviderAdmission,
    committed: bool,
}

impl PreparedResearchProviderReplacement {
    /// Returns the exact expected old runtime identity.
    pub const fn expected(&self) -> &ResearchProviderRuntimeGeneration {
        &self.expected
    }

    /// Returns the fully validated candidate runtime identity.
    pub const fn candidate(&self) -> &ResearchProviderRuntimeGeneration {
        &self.candidate
    }

    /// Publishes the prebuilt source in one serialized in-memory swap.
    pub fn commit(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
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
        let current = authority
            .sources
            .get(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&self.expected) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if !current.admission.revocation_drained() {
            return Err(ResearchIngestCompositionError::RuntimeGenerationStillCallable);
        }
        let candidate_source = self
            .candidate_source
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        let replacement_registration = if current.metadata == self.candidate.metadata {
            None
        } else {
            let registered_at = super::system_timestamp()
                .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
            let registry = authority
                .registry
                .as_mut()
                .ok_or(ResearchIngestCompositionError::ShuttingDown)?;
            Some(registry.replace_metadata(
                &current.registration,
                self.candidate.metadata.clone(),
                registered_at,
            )?)
        };
        let removed = authority.pending_replacements.remove(&self.profile);
        if removed != Some(self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        current.source = candidate_source;
        current.metadata = self.candidate.metadata.clone();
        if let Some(registration) = replacement_registration {
            current.registration = registration;
        }
        current.rights = self.candidate.rights.clone();
        current.generation = Some(self.candidate.clone());
        current.admission = self.candidate_admission.clone();
        self.committed = true;
        Ok(self.candidate.clone())
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
        if self.committed {
            return;
        }
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

impl ProductionResearchIngestCoordinator {
    /// Registers one provider adapter bound to an exact onboarding/runtime generation.
    pub fn register_provider_source<S>(
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
        self.register_source_inner(
            generation.profile().clone(),
            source,
            rights,
            Some(generation),
        )
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
        let Some(source) = authority.sources.get(profile) else {
            return Ok(None);
        };
        source
            .generation
            .clone()
            .map(Some)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)
    }

    /// Prepares an exact expected-old to exact-new adapter replacement without publishing it.
    pub fn prepare_provider_replacement<S>(
        self: &Arc<Self>,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<PreparedResearchProviderReplacement, ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        if self.lifecycle.shutdown_token().is_cancelled()
            || !candidate.is_exact_successor_of(&expected)?
            || source.metadata() != candidate.metadata()
            || rights != candidate.rights
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        let profile = candidate.profile().clone();
        let token = Uuid::new_v4();
        let candidate_source: Arc<dyn ManagedResearchExtractionSource> = Arc::new(source);
        let candidate_admission = ResearchProviderAdmission::new(Some(&candidate))?;
        let mut authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.lifecycle.shutdown_token().is_cancelled() || authority.registry.is_none() {
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
        authority
            .pending_replacements
            .insert(profile.clone(), token);
        Ok(PreparedResearchProviderReplacement {
            coordinator: Arc::clone(self),
            profile,
            token,
            expected,
            candidate,
            candidate_source: Some(candidate_source),
            candidate_admission,
            committed: false,
        })
    }

    /// Revokes exactly one callable generation and every retained receipt minted from it.
    pub async fn revoke_provider_generation(
        &self,
        profile: &SourceIdentifier,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<(), ResearchIngestCompositionError> {
        let admission = {
            let mut authority = self
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
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
        };
        admission.revoke_and_drain().await;
        Ok(())
    }

    /// Restores request admission for an unchanged exact generation after an aborted replacement.
    pub fn restore_provider_generation(
        &self,
        profile: &SourceIdentifier,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        let mut authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.lifecycle.shutdown_token().is_cancelled() || authority.registry.is_none() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.contains_key(profile) {
            return Err(ResearchIngestCompositionError::ReplacementInProgress);
        }
        let current = authority
            .sources
            .get_mut(profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(expected)
            || current.metadata != expected.metadata
            || current.rights != expected.rights
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if current.admission.revocation_drained() {
            current.admission = ResearchProviderAdmission::new(Some(expected))?;
        } else {
            current.admission.ensure_live()?;
        }
        Ok(expected.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

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
}
