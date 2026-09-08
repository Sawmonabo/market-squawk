//! Transactional source/venue/instrument trading-status authority.

use std::collections::HashMap;

use market_squawk_domain::{ConnectionGeneration, SourceId, TradingStatus};
use market_squawk_sources::{
    CurrentProviderObservation, CurrentStreamKey, ProviderObservationPayload,
};

use super::LiveApplyError;
use crate::authority::{
    AuthorityError, StatusLease, StatusLeaseOwner, StatusRevisionLease, StatusRevisionOwner,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct StatusKey {
    source_id: SourceId,
    venue: market_squawk_domain::VenueId,
    instrument: market_squawk_domain::InstrumentId,
}

impl StatusKey {
    fn from_current(current: &CurrentProviderObservation) -> Self {
        Self {
            source_id: current.stream_key().source_id().clone(),
            venue: current.stream_key().venue().clone(),
            instrument: current.stream_key().instrument(),
        }
    }

    pub(super) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(super) const fn venue(&self) -> &market_squawk_domain::VenueId {
        &self.venue
    }

    pub(super) const fn instrument(&self) -> market_squawk_domain::InstrumentId {
        self.instrument
    }
}

#[derive(Debug)]
pub(super) struct SharedStatus {
    generation: ConnectionGeneration,
    status: TradingStatus,
    allocation_version: u64,
    allocation: StatusLeaseOwner,
    revision: StatusRevisionOwner,
    expected_revision: u64,
}

impl SharedStatus {
    fn binding(&self) -> StatusBinding {
        StatusBinding {
            status: self.status,
            allocation: self.allocation.lease(),
            revision: self.revision.lease(),
            expected_revision: self.expected_revision,
        }
    }

    fn invalidate(&mut self) {
        self.allocation.invalidate();
        self.revision.invalidate();
    }
}

#[derive(Clone, Debug)]
pub(super) struct StatusBinding {
    pub(super) status: TradingStatus,
    pub(super) allocation: StatusLease,
    pub(super) revision: StatusRevisionLease,
    pub(super) expected_revision: u64,
}

/// A status decision prepared without mutating shared cross-channel state.
#[derive(Debug)]
pub(super) enum StagedStatus {
    Existing(StatusBinding),
    Upsert {
        key: StatusKey,
        expected_existing: Option<StatusLease>,
        replacement: SharedStatus,
        binding: StatusBinding,
    },
}

impl StagedStatus {
    pub(super) const fn status(&self) -> TradingStatus {
        match self {
            Self::Existing(binding) => binding.status,
            Self::Upsert { binding, .. } => binding.status,
        }
    }
}

/// Bounded status allocations shared by every channel for a source/venue/instrument tuple.
#[derive(Debug)]
pub(super) struct StatusBook {
    entries: HashMap<StatusKey, SharedStatus>,
    maximum: usize,
}

impl StatusBook {
    pub(super) fn try_new(maximum: usize) -> Result<Self, LiveApplyError> {
        let mut entries = HashMap::new();
        entries
            .try_reserve(maximum)
            .map_err(|_| LiveApplyError::Allocation)?;
        Ok(Self { entries, maximum })
    }

    /// Stages any shared status transition. No existing allocation changes here.
    pub(super) fn stage(
        &self,
        current: &CurrentProviderObservation,
        default_status: TradingStatus,
    ) -> Result<StagedStatus, LiveApplyError> {
        let key = StatusKey::from_current(current);
        let generation = current.evidence().binding().connection_generation();
        let existing = self.entries.get(&key);
        if let Some(shared) = existing
            && shared.generation != generation
        {
            return Err(LiveApplyError::StatusGenerationMismatch);
        }

        let current_status = existing.map_or(default_status, |shared| shared.status);
        let next_status = status_transition(current.observation().payload(), current_status);
        let rotates = next_status.is_some();
        if !rotates && let Some(shared) = existing {
            return Ok(StagedStatus::Existing(shared.binding()));
        }
        if existing.is_none() && self.entries.len() >= self.maximum {
            return Err(LiveApplyError::StatusCapacityExhausted);
        }

        let allocation_version = existing.map_or(Ok(1), |shared| {
            shared
                .allocation_version
                .checked_add(1)
                .ok_or(LiveApplyError::StatusRevisionExhausted)
        })?;
        let mut replacement = SharedStatus {
            generation,
            status: next_status.unwrap_or(current_status),
            allocation_version,
            allocation: StatusLeaseOwner::new(allocation_version),
            revision: StatusRevisionOwner::new(),
            expected_revision: 0,
        };
        if rotates {
            replacement.expected_revision = replacement
                .revision
                .advance()
                .map_err(AuthorityError::from)?;
        }
        let binding = replacement.binding();
        Ok(StagedStatus::Upsert {
            key,
            expected_existing: existing.map(|shared| shared.allocation.lease()),
            replacement,
            binding,
        })
    }

    /// Revalidates all existing status authority while the staged transition is still reversible.
    pub(super) fn validate_staged(&self, staged: &StagedStatus) -> Result<(), LiveApplyError> {
        match staged {
            StagedStatus::Existing(binding) => {
                binding
                    .allocation
                    .validate()
                    .map_err(AuthorityError::from)?;
                binding
                    .revision
                    .validate(binding.expected_revision)
                    .map_err(AuthorityError::from)?;
                Ok(())
            }
            StagedStatus::Upsert {
                key,
                expected_existing,
                ..
            } => match (self.entries.get(key), expected_existing) {
                (Some(existing), Some(expected))
                    if existing.allocation.lease().shares_allocation_with(expected) =>
                {
                    Ok(())
                }
                (None, None) => Ok(()),
                _ => Err(LiveApplyError::StatusCommitConflict),
            },
        }
    }

    /// Publishes a validated stage after stream commit; single-writer ownership makes this
    /// infallible and prevents a half-committed cross-channel transition.
    pub(super) fn commit(&mut self, staged: StagedStatus) -> StatusBinding {
        match staged {
            StagedStatus::Existing(binding) => binding,
            StagedStatus::Upsert {
                key,
                expected_existing: _,
                replacement,
                binding,
            } => {
                if let Some(existing) = self.entries.get_mut(&key) {
                    existing.invalidate();
                }
                self.entries.insert(key, replacement);
                binding
            }
        }
    }

    /// Invalidates and removes all old-generation status allocations for one source.
    pub(super) fn invalidate_source(&mut self, source_id: &SourceId) {
        self.entries.retain(|key, status| {
            if key.source_id() == source_id {
                status.invalidate();
                false
            } else {
                true
            }
        });
    }

    pub(super) fn invalidate_all(&mut self) {
        for status in self.entries.values_mut() {
            status.invalidate();
        }
    }

    pub(super) fn iter(
        &self,
    ) -> impl Iterator<Item = (&StatusKey, ConnectionGeneration, TradingStatus, u64)> {
        self.entries.iter().map(|(key, status)| {
            (
                key,
                status.generation,
                status.status,
                status.allocation_version,
            )
        })
    }

    pub(super) fn status_for_stream(
        &self,
        stream: &CurrentStreamKey,
    ) -> Option<(TradingStatus, u64)> {
        self.entries
            .get(&StatusKey {
                source_id: stream.source_id().clone(),
                venue: stream.venue().clone(),
                instrument: stream.instrument(),
            })
            .map(|status| (status.status, status.allocation_version))
    }

    #[cfg(test)]
    pub(super) fn set_allocation_version_for_test(
        &mut self,
        stream: &CurrentStreamKey,
        allocation_version: u64,
    ) -> Result<(), LiveApplyError> {
        let status = self
            .entries
            .get_mut(&StatusKey {
                source_id: stream.source_id().clone(),
                venue: stream.venue().clone(),
                instrument: stream.instrument(),
            })
            .ok_or(LiveApplyError::StatusCommitConflict)?;
        status.allocation_version = allocation_version;
        Ok(())
    }
}

fn status_transition(
    payload: &ProviderObservationPayload,
    current: TradingStatus,
) -> Option<TradingStatus> {
    match payload {
        ProviderObservationPayload::TradingHalt { transition, .. } => Some(match transition {
            market_squawk_domain::HaltTransition::Halted => TradingStatus::Halted,
            market_squawk_domain::HaltTransition::Resumed => TradingStatus::Active,
        }),
        ProviderObservationPayload::InstrumentStatus { trading_status, .. } => {
            Some(*trading_status)
        }
        // A corporate action may change instrument executability outside the market-data channel.
        // Rotate authority even when the current status value remains unchanged.
        ProviderObservationPayload::CorporateAction { .. } => Some(current),
        _ => None,
    }
}
