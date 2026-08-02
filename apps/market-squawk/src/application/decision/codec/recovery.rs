use std::collections::BTreeMap;

use market_squawk_decisions::{AppendOutcome, CandidateRecord, DecisionAuthority};
use market_squawk_modeling::ProductionFeatureRegistry;

use super::super::DecisionApplicationError;
use super::candidate::ExecutionWire;
use super::wire::{WIRE_VERSION, WireEnvelope, WireRecord};

#[derive(Debug)]
pub(in crate::application::decision) struct RecoveryContext {
    registry: ProductionFeatureRegistry,
    candidates: BTreeMap<String, CandidateRecord>,
}

impl RecoveryContext {
    pub(in crate::application::decision) fn try_new() -> Result<Self, DecisionApplicationError> {
        Ok(Self {
            registry: ProductionFeatureRegistry::try_new()
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            candidates: BTreeMap::new(),
        })
    }

    pub(in crate::application::decision) fn apply(
        &mut self,
        authority: &mut DecisionAuthority,
        kind: i64,
        key: &str,
        payload: &[u8],
    ) -> Result<(), DecisionApplicationError> {
        self.apply_inner(authority, kind, key, payload)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }

    fn apply_inner(
        &mut self,
        authority: &mut DecisionAuthority,
        kind: i64,
        key: &str,
        payload: &[u8],
    ) -> Result<(), DecisionApplicationError> {
        let envelope: WireEnvelope = serde_json::from_slice(payload)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        if envelope.version != WIRE_VERSION || envelope.record.kind() != kind {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        match envelope.record {
            WireRecord::Screen(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let screen = wire.decode(self.registry.feature_registry())?;
                let expected = authority
                    .repository()
                    .screens()
                    .filter(|candidate| candidate.revision().id() == screen.revision().id())
                    .map(|candidate| candidate.revision().revision())
                    .max_by_key(|revision| revision.get());
                ensure_appended(authority.save_screen(expected, screen)?)
            }
            WireRecord::Execution(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let expected = wire.clone();
                let (run, candidates, selected_at) =
                    wire.decode(self.registry.feature_registry())?;
                let execution = authority.run_screen(run, candidates, selected_at)?;
                if ExecutionWire::from_execution(&execution, selected_at)? != expected {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                for candidate in execution.candidates() {
                    self.candidates
                        .entry(candidate.record().id().as_str().to_owned())
                        .or_insert_with(|| candidate.record().clone());
                }
                Ok(())
            }
            WireRecord::Dossier(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let candidate = self
                    .candidates
                    .get(wire.candidate_key())
                    .ok_or(DecisionApplicationError::InvalidPersistentState)?;
                ensure_appended(authority.append_dossier(wire.decode(candidate)?)?)
            }
            WireRecord::Target(wire) => {
                let wire = *wire;
                if wire.key()? != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let target = wire.decode()?;
                let expected = authority
                    .repository()
                    .target_revisions(target.target().id())
                    .map(|candidate| candidate.target().revision())
                    .max_by_key(|revision| revision.get());
                ensure_appended(match expected {
                    None => authority.create_target(target)?,
                    Some(expected) => authority.reevaluate_target(expected, target)?,
                })
            }
            WireRecord::Review(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let (target_id, revision) = wire.target_coordinate()?;
                let target = authority
                    .get_target(&target_id, revision)?
                    .target()
                    .target()
                    .clone();
                ensure_appended(authority.review_target(wire.decode(&target)?)?)
            }
            WireRecord::Invalidation(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let (target_id, revision) = wire.target_coordinate()?;
                let target = authority
                    .get_target(&target_id, revision)?
                    .target()
                    .target()
                    .clone();
                ensure_appended(authority.invalidate_target(wire.decode(&target)?)?)
            }
        }
    }
}

fn ensure_appended(outcome: AppendOutcome) -> Result<(), DecisionApplicationError> {
    if outcome == AppendOutcome::Appended {
        Ok(())
    } else {
        Err(DecisionApplicationError::InvalidPersistentState)
    }
}
