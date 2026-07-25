//! Generation-bound publication of callable research-provider adapters.

use std::sync::Arc;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_platform::{SecretGeneration, SecretRef};
use market_squawk_sources::{ProviderCapabilityRevision, SourceMetadata};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
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

    /// Returns the exact credential generation, when this surface uses one.
    pub const fn credential_generation(&self) -> Option<SecretGeneration> {
        self.credential_generation
    }

    /// Returns the exact source metadata retained by this adapter.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns a canonical digest of every non-secret runtime authority dimension.
    pub fn identity_digest(&self) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct RuntimeGenerationWire<'a> {
            profile: &'a SourceIdentifier,
            session_id: Uuid,
            capability_revision: ProviderCapabilityRevision,
            capability_digest: EvidenceDigest,
            credential_generation: Option<SecretGeneration>,
            secret_reference: Option<&'a SecretRef>,
            metadata: &'a SourceMetadata,
            rights_source_id: &'a market_squawk_domain::SourceId,
            rights_basis_reference: &'a str,
            rights_basis_digest: EvidenceDigest,
            rights_root_identity_digest: Option<EvidenceDigest>,
            rights_authorization_evidence: EvidenceDigest,
            rights_authorization_expires_at: Option<market_squawk_domain::Timestamp>,
        }

        let bytes = serde_json::to_vec(&RuntimeGenerationWire {
            profile: &self.profile,
            session_id: self.session_id,
            capability_revision: self.capability_revision,
            capability_digest: self.capability_digest,
            credential_generation: self.credential_generation,
            secret_reference: self.secret_reference.as_ref(),
            metadata: &self.metadata,
            rights_source_id: &self.rights.source_id,
            rights_basis_reference: self.rights.basis.reference(),
            rights_basis_digest: self.rights.basis.digest(),
            rights_root_identity_digest: self.rights.basis.root_identity_digest(),
            rights_authorization_evidence: self.rights.authorization_evidence,
            rights_authorization_expires_at: self.rights.authorization_expires_at,
        })
        .map_err(|_| ResearchIngestCompositionError::InvalidRuntimeGeneration)?;
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(bytes).into(),
        ))
    }

    fn is_exact_successor_of(&self, expected: &Self) -> bool {
        self.profile == expected.profile
            && self.session_id == expected.session_id
            && self.capability_revision == expected.capability_revision
            && self.capability_digest == expected.capability_digest
            && self.metadata == expected.metadata
            && self.rights == expected.rights
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
            }
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
            || current.metadata != self.candidate.metadata
            || current.rights != self.candidate.rights
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let candidate_source = self
            .candidate_source
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        let removed = authority.pending_replacements.remove(&self.profile);
        if removed != Some(self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        current.source = candidate_source;
        current.generation = Some(self.candidate.clone());
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
            || !candidate.is_exact_successor_of(&expected)
            || source.metadata() != candidate.metadata()
            || rights != candidate.rights
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        let profile = candidate.profile().clone();
        let token = Uuid::new_v4();
        let candidate_source: Arc<dyn ManagedResearchExtractionSource> = Arc::new(source);
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
            || current.metadata != candidate.metadata
            || current.rights != candidate.rights
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
            committed: false,
        })
    }
}
