//! Bounded validation and application of explicit futures-roll graphs.

use market_squawk_domain::Timestamp;

use super::{
    ContractRollEvidence, DerivativeDecisionRecord, DerivativeLifecycle,
    DerivativeLifecycleEvidence, DerivativeSelectionDecision, evidence_available, find_lifecycle,
};
use crate::{UniverseError, UniverseSnapshot};

pub(super) fn apply_roll_graph(
    rolls: &[ContractRollEvidence],
    base: &UniverseSnapshot,
    lifecycles: &[DerivativeLifecycleEvidence],
    decisions: &mut [DerivativeDecisionRecord],
    as_of: Timestamp,
) -> Result<(), UniverseError> {
    let mut incoming = Vec::new();
    incoming
        .try_reserve_exact(rolls.len())
        .map_err(|_| UniverseError::AllocationFailed)?;
    for roll in rolls {
        incoming.push(roll.mapping.to_instrument_id());
        validate_roll_endpoints(roll, base, lifecycles, as_of)?;
    }
    incoming.sort_unstable();

    let mut visited = Vec::new();
    visited
        .try_reserve_exact(rolls.len())
        .map_err(|_| UniverseError::AllocationFailed)?;
    visited.resize(rolls.len(), false);
    let mut visited_count = 0_usize;
    for root_index in 0..rolls.len() {
        let root = rolls[root_index].mapping.from_instrument_id();
        if incoming.binary_search(&root).is_ok() {
            continue;
        }
        let mut edge_index = root_index;
        loop {
            if visited[edge_index] {
                return Err(UniverseError::ContractRollCycle {
                    instrument_id: rolls[edge_index].mapping.from_instrument_id(),
                });
            }
            visited[edge_index] = true;
            visited_count = visited_count
                .checked_add(1)
                .ok_or(UniverseError::CanonicalEncodingOverflow)?;
            let mapping = rolls[edge_index].mapping;
            let source_index = decisions
                .binary_search_by_key(&mapping.from_instrument_id(), |record| record.instrument_id)
                .map_err(|_| UniverseError::RollSourceUnavailable {
                    instrument_id: mapping.from_instrument_id(),
                })?;
            decisions[source_index].decision = DerivativeSelectionDecision::Rolled {
                to_instrument_id: mapping.to_instrument_id(),
                effective_at: mapping.effective_at(),
            };
            match rolls.binary_search_by_key(&mapping.to_instrument_id(), |roll| {
                roll.mapping.from_instrument_id()
            }) {
                Ok(next_index) => edge_index = next_index,
                Err(_) => {
                    let terminal_index = decisions
                        .binary_search_by_key(&mapping.to_instrument_id(), |record| {
                            record.instrument_id
                        })
                        .map_err(|_| UniverseError::RollTargetUnavailable {
                            instrument_id: mapping.to_instrument_id(),
                        })?;
                    if decisions[terminal_index].decision != DerivativeSelectionDecision::Active {
                        return Err(UniverseError::RollTargetUnavailable {
                            instrument_id: mapping.to_instrument_id(),
                        });
                    }
                    break;
                }
            }
        }
    }
    if visited_count != rolls.len() {
        let first_cycle = visited
            .iter()
            .position(|was_visited| !was_visited)
            .and_then(|index| rolls.get(index))
            .map(|roll| roll.mapping.from_instrument_id())
            .ok_or(UniverseError::CanonicalEncodingOverflow)?;
        return Err(UniverseError::ContractRollCycle {
            instrument_id: first_cycle,
        });
    }
    Ok(())
}

pub(super) fn reject_ambiguous_rolls(values: &[ContractRollEvidence]) -> Result<(), UniverseError> {
    for pair in values.windows(2) {
        if pair[0].mapping.from_instrument_id() == pair[1].mapping.from_instrument_id() {
            return Err(UniverseError::AmbiguousContractRoll {
                instrument_id: pair[0].mapping.from_instrument_id(),
            });
        }
    }
    let mut incoming = Vec::new();
    incoming
        .try_reserve_exact(values.len())
        .map_err(|_| UniverseError::AllocationFailed)?;
    for value in values {
        incoming.push(value.mapping.to_instrument_id());
    }
    incoming.sort_unstable();
    if let Some(pair) = incoming.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(UniverseError::AmbiguousContractRoll {
            instrument_id: pair[0],
        });
    }
    Ok(())
}

fn validate_roll_endpoints(
    roll: &ContractRollEvidence,
    base: &UniverseSnapshot,
    lifecycles: &[DerivativeLifecycleEvidence],
    as_of: Timestamp,
) -> Result<(), UniverseError> {
    let mapping = roll.mapping;
    let from = mapping.from_instrument_id();
    let to = mapping.to_instrument_id();
    if base.membership(from).is_none() {
        return Err(UniverseError::RollSourceUnavailable {
            instrument_id: from,
        });
    }
    if base.membership(to).is_none() {
        return Err(UniverseError::RollTargetUnavailable { instrument_id: to });
    }
    let from_lifecycle =
        find_lifecycle(lifecycles, from).ok_or(UniverseError::MissingDerivativeLifecycle {
            instrument_id: from,
        })?;
    let to_lifecycle = find_lifecycle(lifecycles, to)
        .ok_or(UniverseError::MissingDerivativeLifecycle { instrument_id: to })?;
    if !evidence_available(&from_lifecycle.availability, as_of) {
        return Err(UniverseError::RollSourceUnavailable {
            instrument_id: from,
        });
    }
    if !evidence_available(&to_lifecycle.availability, as_of) {
        return Err(UniverseError::RollTargetUnavailable { instrument_id: to });
    }
    if !matches!(&from_lifecycle.lifecycle, DerivativeLifecycle::Future(_))
        || !matches!(&to_lifecycle.lifecycle, DerivativeLifecycle::Future(_))
    {
        return Err(UniverseError::RollRequiresFutures {
            from_instrument_id: from,
            to_instrument_id: to,
        });
    }
    Ok(())
}
