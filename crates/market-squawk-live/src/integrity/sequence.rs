//! Generation-owned provider sequence progression.

use market_squawk_domain::{
    ConnectionGeneration, IntegrityEvidenceError, IntegrityRule, SequenceCapability,
    SequenceEvidence, SequenceNumber, SequenceValidationRule,
};
use market_squawk_sources::{ProviderSequenceEvidence, SequenceValidationProfile};
use thiserror::Error;

/// Transactional sequence validator for one exact connection generation.
#[derive(Clone, Debug)]
pub struct SequenceTracker {
    generation: ConnectionGeneration,
    profile: SequenceProfile,
    snapshot_seen: bool,
    snapshot_sequence: Option<SequenceNumber>,
    last_sequence: Option<SequenceNumber>,
}

#[derive(Clone, Debug)]
enum SequenceProfile {
    Unsupported {
        rule: IntegrityRule,
    },
    Provided {
        rule: IntegrityRule,
        progression: SequenceValidationRule,
    },
}

impl SequenceTracker {
    /// Binds a tracker to one immutable metadata profile and generation.
    pub fn new(generation: ConnectionGeneration, profile: &SequenceValidationProfile) -> Self {
        let profile = match profile {
            SequenceValidationProfile::Unsupported { rule } => {
                SequenceProfile::Unsupported { rule: rule.clone() }
            }
            SequenceValidationProfile::Provided { rule, progression } => {
                SequenceProfile::Provided {
                    rule: rule.clone(),
                    progression: *progression,
                }
            }
        };
        Self {
            generation,
            profile,
            snapshot_seen: false,
            snapshot_sequence: None,
            last_sequence: None,
        }
    }

    /// Validates and commits the generation's initializing snapshot sequence.
    ///
    /// # Errors
    ///
    /// Rejects duplicate snapshots, evidence/profile mismatch, and invalid construction.
    pub fn validate_snapshot(
        &mut self,
        observed: &ProviderSequenceEvidence,
    ) -> Result<SequenceEvidence, SequenceValidationError> {
        if self.snapshot_seen {
            return Err(SequenceValidationError::SnapshotAlreadyInitialized);
        }
        let evidence = match (&self.profile, observed) {
            (
                SequenceProfile::Unsupported { rule: expected },
                ProviderSequenceEvidence::Unsupported { rule: found },
            ) if expected == found => SequenceEvidence::unsupported(self.generation),
            (
                SequenceProfile::Provided {
                    rule: expected,
                    progression,
                },
                ProviderSequenceEvidence::Provided { value, rule: found },
            ) if expected == found => SequenceEvidence::validate(
                SequenceCapability::Provided,
                Some(expected.clone()),
                *progression,
                self.generation,
                Some(*value),
                None,
                Some(*value),
            )?,
            _ => return Err(SequenceValidationError::ProfileMismatch),
        };
        self.snapshot_seen = true;
        self.snapshot_sequence = evidence.observed_sequence();
        self.last_sequence = evidence.observed_sequence();
        Ok(evidence)
    }

    /// Validates and commits one subsequent provider sequence.
    ///
    /// Failed validation never advances state.
    ///
    /// # Errors
    ///
    /// Rejects deltas before snapshot initialization, duplicates, regressions, gaps under a
    /// consecutive profile, counter exhaustion, and evidence/profile mismatch.
    pub fn validate_delta(
        &mut self,
        observed: &ProviderSequenceEvidence,
    ) -> Result<SequenceEvidence, SequenceValidationError> {
        if !self.snapshot_seen {
            return Err(SequenceValidationError::SnapshotRequired);
        }
        let evidence = match (&self.profile, observed) {
            (
                SequenceProfile::Unsupported { rule: expected },
                ProviderSequenceEvidence::Unsupported { rule: found },
            ) if expected == found => SequenceEvidence::unsupported(self.generation),
            (
                SequenceProfile::Provided {
                    rule: expected,
                    progression,
                },
                ProviderSequenceEvidence::Provided { value, rule: found },
            ) if expected == found => {
                let previous = self
                    .last_sequence
                    .ok_or(SequenceValidationError::SnapshotRequired)?;
                validate_progression(previous, *value, *progression)?;
                SequenceEvidence::validate(
                    SequenceCapability::Provided,
                    Some(expected.clone()),
                    *progression,
                    self.generation,
                    self.snapshot_sequence,
                    Some(previous),
                    Some(*value),
                )?
            }
            _ => return Err(SequenceValidationError::ProfileMismatch),
        };
        if let Some(value) = evidence.observed_sequence() {
            self.last_sequence = Some(value);
        }
        Ok(evidence)
    }

    /// Validates one non-book event without inventing a snapshot requirement.
    ///
    /// The first provided value establishes the stream cursor; subsequent values obey the exact
    /// metadata progression rule. Unsupported sequence capability remains explicit evidence.
    pub fn validate_non_book(
        &mut self,
        observed: &ProviderSequenceEvidence,
    ) -> Result<SequenceEvidence, SequenceValidationError> {
        let evidence = match (&self.profile, observed) {
            (
                SequenceProfile::Unsupported { rule: expected },
                ProviderSequenceEvidence::Unsupported { rule: found },
            ) if expected == found => SequenceEvidence::unsupported(self.generation),
            (
                SequenceProfile::Provided {
                    rule: expected,
                    progression,
                },
                ProviderSequenceEvidence::Provided { value, rule: found },
            ) if expected == found => {
                if let Some(previous) = self.last_sequence {
                    validate_progression(previous, *value, *progression)?;
                }
                SequenceEvidence::validate(
                    SequenceCapability::Provided,
                    Some(expected.clone()),
                    *progression,
                    self.generation,
                    self.last_sequence.is_none().then_some(*value),
                    self.last_sequence,
                    Some(*value),
                )?
            }
            _ => return Err(SequenceValidationError::ProfileMismatch),
        };
        if let Some(value) = evidence.observed_sequence() {
            self.last_sequence = Some(value);
        }
        Ok(evidence)
    }

    /// Returns the last successfully committed sequence.
    pub const fn last_sequence(&self) -> Option<SequenceNumber> {
        self.last_sequence
    }
}

fn validate_progression(
    previous: SequenceNumber,
    observed: SequenceNumber,
    progression: SequenceValidationRule,
) -> Result<(), SequenceValidationError> {
    if observed == previous {
        return Err(SequenceValidationError::Duplicate { previous });
    }
    if observed < previous {
        return Err(SequenceValidationError::Regression { previous, observed });
    }
    if progression == SequenceValidationRule::Consecutive {
        let expected = previous
            .checked_next()
            .map_err(|_| SequenceValidationError::CounterExhausted)?;
        if observed != expected {
            return Err(SequenceValidationError::Gap { previous, observed });
        }
    }
    Ok(())
}

/// Provider sequence progression failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SequenceValidationError {
    /// A delta arrived before snapshot initialization.
    #[error("sequence delta requires a current-generation snapshot")]
    SnapshotRequired,
    /// A second snapshot attempted to reset the same allocation.
    #[error("snapshot sequence is already initialized for this generation")]
    SnapshotAlreadyInitialized,
    /// Observation and immutable metadata profile differ.
    #[error("sequence evidence does not match the metadata profile")]
    ProfileMismatch,
    /// Provider repeated the prior sequence.
    #[error("duplicate sequence {previous:?}")]
    Duplicate { previous: SequenceNumber },
    /// Provider sequence regressed.
    #[error("sequence regressed from {previous:?} to {observed:?}")]
    Regression {
        previous: SequenceNumber,
        observed: SequenceNumber,
    },
    /// Consecutive profile skipped one or more values.
    #[error("sequence gap after {previous:?}: observed {observed:?}")]
    Gap {
        previous: SequenceNumber,
        observed: SequenceNumber,
    },
    /// Sequence progression would wrap its bounded integer.
    #[error("sequence counter exhausted")]
    CounterExhausted,
    /// Shared domain evidence rejected a relational contradiction.
    #[error("sequence evidence construction failed: {0}")]
    Evidence(#[from] IntegrityEvidenceError),
}
