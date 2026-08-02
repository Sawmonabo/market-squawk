//! Bounded pre-feed source-generation authority registration.

use std::collections::HashMap;

use market_squawk_domain::{ConnectionGeneration, Timestamp};
use market_squawk_sources::CurrentSourceAuthorityLease;

use super::LiveApplyError;
use crate::authority::{
    AuthorityError, GenerationLease, GenerationLeaseOwner, RegistryLifecycleLease,
    RegistryLifecycleOwner,
};

const MAX_CURRENT_SOURCE_GENERATIONS: usize = 64;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SourceGenerationKey {
    metadata_revision: market_squawk_domain::MetadataRevision,
    session_id: market_squawk_sources::SessionId,
    generation: ConnectionGeneration,
}

impl SourceGenerationKey {
    fn from_lease(source: &CurrentSourceAuthorityLease) -> Self {
        let binding = source.binding();
        Self {
            metadata_revision: binding.metadata_revision().clone(),
            session_id: binding.session_id().clone(),
            generation: binding.connection_generation(),
        }
    }
}

#[derive(Debug)]
struct GenerationEntry {
    key: SourceGenerationKey,
    source: CurrentSourceAuthorityLease,
    owner: GenerationLeaseOwner,
}

/// Exact O(1) producer binding minted only from an opaque current-source lease.
#[derive(Clone, Debug)]
pub(crate) struct GenerationAdmission {
    source: CurrentSourceAuthorityLease,
    generation: GenerationLease,
    registry: RegistryLifecycleLease,
}

impl GenerationAdmission {
    pub(crate) fn invalidate_on_admission_failure(&self) {
        self.generation.invalidate();
    }

    pub(crate) const fn source(&self) -> &CurrentSourceAuthorityLease {
        &self.source
    }

    pub(super) fn generation(&self) -> GenerationLease {
        self.generation.clone()
    }

    pub(crate) fn validate_at(&self, at: Timestamp) -> Result<(), LiveApplyError> {
        self.source.validate_at(at)?;
        self.registry.validate().map_err(AuthorityError::from)?;
        self.generation.validate().map_err(AuthorityError::from)?;
        Ok(())
    }

    /// Returns a conservative checked charge for the admission handle and shared identities.
    pub(crate) fn retained_bytes(&self) -> Result<usize, LiveApplyError> {
        std::mem::size_of::<Self>()
            .checked_add(market_squawk_domain::SourceId::MAX_LENGTH)
            .and_then(|value| value.checked_add(market_squawk_domain::VenueId::MAX_LENGTH))
            .and_then(|value| {
                value.checked_add(2 * market_squawk_domain::SourceIdentifier::MAX_LENGTH)
            })
            .ok_or(LiveApplyError::SnapshotRetainedSizeOverflow)
    }
}

/// Shared actor-exit degradation handle for all admissions minted by one registry.
#[derive(Clone, Debug)]
pub(crate) struct GenerationRegistryExitHandle(RegistryLifecycleLease);

impl GenerationRegistryExitHandle {
    pub(crate) fn invalidate(&self) {
        self.0.invalidate();
    }
}

/// Bounded control-path registry used before producers open/feed their queues.
#[derive(Debug)]
pub(crate) struct GenerationAuthorityRegistry {
    generations: HashMap<market_squawk_domain::SourceId, GenerationEntry>,
    maximum: usize,
    lifecycle: RegistryLifecycleOwner,
}

impl GenerationAuthorityRegistry {
    pub(crate) fn try_new(maximum: usize) -> Result<Self, LiveApplyError> {
        if maximum == 0 || maximum > MAX_CURRENT_SOURCE_GENERATIONS {
            return Err(LiveApplyError::InvalidGenerationCapacity);
        }
        let mut generations = HashMap::new();
        generations
            .try_reserve(maximum)
            .map_err(|_| LiveApplyError::Allocation)?;
        Ok(Self {
            generations,
            maximum,
            lifecycle: RegistryLifecycleOwner::new(1),
        })
    }

    /// Binds the current-source lease, reusing one generation allocation across health epochs.
    pub(crate) fn bind_current(
        &mut self,
        source: &CurrentSourceAuthorityLease,
        at: Timestamp,
    ) -> Result<GenerationAdmission, LiveApplyError> {
        source.validate_at(at)?;
        self.lifecycle
            .lease()
            .validate()
            .map_err(AuthorityError::from)?;
        let key = SourceGenerationKey::from_lease(source);
        let source_id = source.binding().source_id();
        if let Some(existing) = self.generations.get_mut(source_id) {
            if !existing.source.shares_registry_lineage_with(source) {
                return Err(LiveApplyError::GenerationAdmissionTransplant);
            }
            if existing.key == key {
                if !existing
                    .source
                    .binding()
                    .shares_allocation_with(source.binding())
                {
                    return Err(LiveApplyError::GenerationAdmissionTransplant);
                }
                existing.source = source.clone();
                return Ok(GenerationAdmission {
                    source: source.clone(),
                    generation: existing.owner.lease(),
                    registry: self.lifecycle.lease(),
                });
            }
            if existing.key.generation >= key.generation {
                return Err(LiveApplyError::GenerationNotAdvanced);
            }
            let owner = GenerationLeaseOwner::new(key.generation.get());
            let admission = GenerationAdmission {
                source: source.clone(),
                generation: owner.lease(),
                registry: self.lifecycle.lease(),
            };
            existing.owner.invalidate();
            *existing = GenerationEntry {
                key,
                source: source.clone(),
                owner,
            };
            return Ok(admission);
        }
        if self.generations.len() >= self.maximum {
            return Err(LiveApplyError::GenerationCapacityExhausted);
        }
        let owner = GenerationLeaseOwner::new(key.generation.get());
        let admission = GenerationAdmission {
            source: source.clone(),
            generation: owner.lease(),
            registry: self.lifecycle.lease(),
        };
        self.generations.insert(
            source_id.clone(),
            GenerationEntry {
                key,
                source: source.clone(),
                owner,
            },
        );
        Ok(admission)
    }

    pub(crate) fn exit_handle(&self) -> GenerationRegistryExitHandle {
        GenerationRegistryExitHandle(self.lifecycle.lease())
    }

    pub(crate) fn invalidate_all(&mut self) {
        self.lifecycle.invalidate();
        for entry in self.generations.values_mut() {
            entry.owner.invalidate();
        }
    }
}

impl Drop for GenerationAuthorityRegistry {
    fn drop(&mut self) {
        self.invalidate_all();
    }
}
