//! Conservative derivative lifecycle and explicit-roll composition.

use std::cmp::Ordering;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, ContractRollMapping, EvidenceDigest, FuturesLifecycleDates,
    InstrumentId, OccOptionIdentity, SourceIdentifier, Timestamp,
};

use super::{UniverseError, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot};
use crate::{DatasetManifestRef, Sha256Digest};

#[path = "derivatives/canonical.rs"]
mod canonical;
#[path = "derivatives/retained.rs"]
mod retained;
#[path = "derivatives/roll.rs"]
mod roll;

use canonical::{
    audit_hash, compare_civil_dates, compare_lifecycle_evidence, compare_roll_evidence,
    content_hash,
};
use retained::{RetainedCapacities, retained_bytes};
use roll::{apply_roll_graph, reject_ambiguous_rolls};

/// Venue-calendar civil date corresponding to the snapshot instant for one instrument.
///
/// The versioned `calendar_rule` identifies the caller's venue/time-zone conversion. Keeping the
/// civil date separate from [`Timestamp`] prevents a date-only lifecycle field from acquiring an
/// invented intraday boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivativeCivilDate {
    instrument_id: InstrumentId,
    date: CalendarDate,
    calendar_rule: SourceIdentifier,
}

impl DerivativeCivilDate {
    /// Binds one instrument's venue civil date to an explicit calendar-rule revision.
    pub const fn new(
        instrument_id: InstrumentId,
        date: CalendarDate,
        calendar_rule: SourceIdentifier,
    ) -> Self {
        Self {
            instrument_id,
            date,
            calendar_rule,
        }
    }

    /// Returns the stable instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the venue civil date without assigning an intraday time.
    pub const fn date(&self) -> CalendarDate {
        self.date
    }

    /// Returns the versioned venue-calendar conversion rule.
    pub const fn calendar_rule(&self) -> &SourceIdentifier {
        &self.calendar_rule
    }
}

/// Exact date-level lifecycle fields retained for one derivative instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DerivativeLifecycle {
    /// OCC identity plus a source-evidenced, century-resolved expiration date.
    Option {
        /// Syntactically validated OCC/OSI identity.
        identity: OccOptionIdentity,
        /// Full expiration date whose final two year digits, month, and day match the identity.
        expiration_date: CalendarDate,
    },
    /// Futures lifecycle dates retained exactly as supplied by reference data.
    Future(FuturesLifecycleDates),
}

/// Manifest- and payload-bound derivative lifecycle evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivativeLifecycleEvidence {
    instrument_id: InstrumentId,
    lifecycle: DerivativeLifecycle,
    availability: AvailabilityEvidence,
    source_manifest: DatasetManifestRef,
    evidence_digest: EvidenceDigest,
}

impl DerivativeLifecycleEvidence {
    /// Constructs option lifecycle evidence and validates the full date against the OCC identity.
    ///
    /// # Errors
    ///
    /// Returns [`UniverseError::OptionExpirationMismatch`] rather than inferring the unresolved
    /// OCC century or accepting a conflicting month/day.
    pub fn try_option(
        instrument_id: InstrumentId,
        identity: OccOptionIdentity,
        expiration_date: CalendarDate,
        availability: AvailabilityEvidence,
        source_manifest: DatasetManifestRef,
        evidence_digest: EvidenceDigest,
    ) -> Result<Self, UniverseError> {
        let year_suffix = u8::try_from(expiration_date.year() % 100)
            .map_err(|_| UniverseError::CanonicalEncodingOverflow)?;
        if identity.expiration_yy() != year_suffix
            || identity.expiration_month() != expiration_date.month()
            || identity.expiration_day() != expiration_date.day()
        {
            return Err(UniverseError::OptionExpirationMismatch { instrument_id });
        }
        Ok(Self {
            instrument_id,
            lifecycle: DerivativeLifecycle::Option {
                identity,
                expiration_date,
            },
            availability,
            source_manifest,
            evidence_digest,
        })
    }

    /// Constructs futures lifecycle evidence without deriving absent date fields.
    pub const fn future(
        instrument_id: InstrumentId,
        lifecycle: FuturesLifecycleDates,
        availability: AvailabilityEvidence,
        source_manifest: DatasetManifestRef,
        evidence_digest: EvidenceDigest,
    ) -> Self {
        Self {
            instrument_id,
            lifecycle: DerivativeLifecycle::Future(lifecycle),
            availability,
            source_manifest,
            evidence_digest,
        }
    }

    /// Returns the stable instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact derivative lifecycle fields.
    pub const fn lifecycle(&self) -> &DerivativeLifecycle {
        &self.lifecycle
    }

    /// Returns the lifecycle evidence's point-in-time availability.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns the immutable source dataset generation.
    pub const fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }

    /// Returns the exact source-record digest.
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Exact manifest- and payload-bound evidence for one explicit contract roll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractRollEvidence {
    mapping: ContractRollMapping,
    availability: AvailabilityEvidence,
    source_manifest: DatasetManifestRef,
    evidence_digest: EvidenceDigest,
}

impl ContractRollEvidence {
    /// Binds an already validated domain roll mapping to immutable source lineage.
    pub const fn new(
        mapping: ContractRollMapping,
        availability: AvailabilityEvidence,
        source_manifest: DatasetManifestRef,
        evidence_digest: EvidenceDigest,
    ) -> Self {
        Self {
            mapping,
            availability,
            source_manifest,
            evidence_digest,
        }
    }

    /// Returns the explicit domain roll mapping.
    pub const fn mapping(&self) -> ContractRollMapping {
        self.mapping
    }

    /// Returns the roll evidence's point-in-time availability.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns the immutable source dataset generation.
    pub const fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }

    /// Returns the exact source-record digest.
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Date-level boundary governing a derivative lifecycle decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DerivativeBoundary {
    /// Full option expiration date validated against the OCC identity.
    OptionExpiration,
    /// Futures last-trade date.
    FuturesLastTrade,
    /// Futures expiration date used when it is earlier than other supplied termination fields.
    FuturesExpiration,
    /// Futures maturity date used when it is earlier than other supplied termination fields.
    FuturesMaturity,
}

/// Fail-closed decision for one otherwise point-in-time-admitted derivative membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DerivativeSelectionDecision {
    /// The contract is before every evidenced termination or roll boundary.
    Active,
    /// The query is on a date-only boundary whose intraday ordering is unknowable.
    SameDateUnresolved {
        /// Boundary that cannot be ordered within the civil date.
        boundary: DerivativeBoundary,
        /// Exact source civil date.
        date: CalendarDate,
    },
    /// An option is after its evidenced expiration date.
    OptionExpired {
        /// Always [`DerivativeBoundary::OptionExpiration`].
        boundary: DerivativeBoundary,
        /// Exact source civil date.
        date: CalendarDate,
    },
    /// A future is after its earliest evidenced termination date and has no usable explicit roll.
    FutureExpiredWithoutRoll {
        /// Earliest supplied futures termination boundary.
        boundary: DerivativeBoundary,
        /// Exact source civil date.
        date: CalendarDate,
    },
    /// No source-evidenced termination boundary was supplied.
    MissingTerminationBoundary,
    /// Lifecycle evidence was not point-in-time available and cannot qualify the contract.
    LifecycleEvidenceUnavailable,
    /// An explicit, available roll mapping replaced the source contract.
    Rolled {
        /// Exact mapped successor; it is never inferred from symbol or maturity.
        to_instrument_id: InstrumentId,
        /// Exact roll instant from the persisted mapping.
        effective_at: Timestamp,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DerivativeDecisionRecord {
    instrument_id: InstrumentId,
    decision: DerivativeSelectionDecision,
}

/// Derivative-aware universe result composed over an immutable general universe snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivativeUniverseSnapshot {
    base: UniverseSnapshot,
    lifecycle_evidence: Vec<DerivativeLifecycleEvidence>,
    civil_dates: Vec<DerivativeCivilDate>,
    roll_evidence: Vec<ContractRollEvidence>,
    decisions: Vec<DerivativeDecisionRecord>,
    active_instruments: Vec<InstrumentId>,
    content_hash: Sha256Digest,
    audit_hash: Sha256Digest,
    retained_bytes: usize,
}

impl DerivativeUniverseSnapshot {
    /// Builds a bounded derivative lifecycle overlay on the ordinary point-in-time universe.
    ///
    /// Each admitted membership requires exactly one lifecycle record and one venue civil date.
    /// Date-only boundaries exclude same-date queries as unresolved. Only an explicit, available
    /// [`ContractRollMapping`] can replace a future, and its target must itself be an active,
    /// admitted future. Inputs remain immutable and are retained with exact manifest/digest lineage.
    ///
    /// # Errors
    ///
    /// Rejects missing, duplicate, ambiguous, non-futures, or unavailable roll targets; work or
    /// retained-memory limit excess; and every error returned by [`UniverseSnapshot::try_build`].
    pub fn try_build(
        universe_id: UniverseId,
        as_of: Timestamp,
        candidates: Vec<UniverseMembership>,
        mut lifecycle_evidence: Vec<DerivativeLifecycleEvidence>,
        mut civil_dates: Vec<DerivativeCivilDate>,
        mut roll_evidence: Vec<ContractRollEvidence>,
        limits: UniverseLimits,
    ) -> Result<Self, UniverseError> {
        require_input_limit(lifecycle_evidence.len(), limits)?;
        require_input_limit(civil_dates.len(), limits)?;
        require_input_limit(roll_evidence.len(), limits)?;
        let base = UniverseSnapshot::try_build(universe_id, as_of, candidates, limits)?;

        lifecycle_evidence.retain(|evidence| base.membership(evidence.instrument_id).is_some());
        civil_dates.retain(|date| base.membership(date.instrument_id).is_some());
        lifecycle_evidence.sort_by(compare_lifecycle_evidence);
        civil_dates.sort_by(compare_civil_dates);
        roll_evidence.sort_by(compare_roll_evidence);
        reject_duplicate_lifecycle(&lifecycle_evidence)?;
        reject_duplicate_civil_dates(&civil_dates)?;

        let mut decisions = Vec::new();
        decisions
            .try_reserve_exact(base.memberships().len())
            .map_err(|_| UniverseError::AllocationFailed)?;
        for membership in base.memberships() {
            let instrument_id = membership.instrument_id();
            let lifecycle = find_lifecycle(&lifecycle_evidence, instrument_id)
                .ok_or(UniverseError::MissingDerivativeLifecycle { instrument_id })?;
            let civil_date = find_civil_date(&civil_dates, instrument_id)
                .ok_or(UniverseError::MissingDerivativeCivilDate { instrument_id })?;
            decisions.push(DerivativeDecisionRecord {
                instrument_id,
                decision: initial_decision(lifecycle, civil_date, as_of),
            });
        }

        roll_evidence.retain(|evidence| {
            evidence.mapping.effective_at() <= as_of
                && evidence_available(&evidence.availability, as_of)
        });
        reject_ambiguous_rolls(&roll_evidence)?;
        apply_roll_graph(
            &roll_evidence,
            &base,
            &lifecycle_evidence,
            &mut decisions,
            as_of,
        )?;

        let mut active_instruments = Vec::new();
        active_instruments
            .try_reserve_exact(decisions.len())
            .map_err(|_| UniverseError::AllocationFailed)?;
        for record in &decisions {
            if record.decision == DerivativeSelectionDecision::Active {
                active_instruments.push(record.instrument_id);
            }
        }
        let content_hash = content_hash(&base, &active_instruments, &decisions)?;
        let audit_hash = audit_hash(
            &base,
            &lifecycle_evidence,
            &civil_dates,
            &roll_evidence,
            &decisions,
        )?;
        let retained_bytes = retained_bytes(
            &base,
            &lifecycle_evidence,
            &civil_dates,
            &roll_evidence,
            RetainedCapacities {
                lifecycle: lifecycle_evidence.capacity(),
                civil_dates: civil_dates.capacity(),
                rolls: roll_evidence.capacity(),
                decisions: decisions.capacity(),
                active: active_instruments.capacity(),
            },
        )?;
        if retained_bytes > limits.max_retained_bytes() {
            return Err(UniverseError::RetainedByteLimitExceeded {
                limit: limits.max_retained_bytes(),
                required: retained_bytes,
            });
        }
        Ok(Self {
            base,
            lifecycle_evidence,
            civil_dates,
            roll_evidence,
            decisions,
            active_instruments,
            content_hash,
            audit_hash,
            retained_bytes,
        })
    }

    /// Returns the complete underlying PIT membership snapshot and its original exclusions.
    pub const fn base_snapshot(&self) -> &UniverseSnapshot {
        &self.base
    }

    /// Returns lifecycle-qualified stable identities in deterministic order.
    pub fn active_instrument_ids(&self) -> &[InstrumentId] {
        &self.active_instruments
    }

    /// Returns whether the instrument remains active after derivative lifecycle composition.
    pub fn contains(&self, instrument_id: InstrumentId) -> bool {
        self.active_instruments
            .binary_search(&instrument_id)
            .is_ok()
    }

    /// Returns the immutable base membership when lifecycle composition admits the instrument.
    pub fn membership(&self, instrument_id: InstrumentId) -> Option<&UniverseMembership> {
        self.contains(instrument_id)
            .then(|| self.base.membership(instrument_id))
            .flatten()
    }

    /// Returns the typed lifecycle decision for an otherwise PIT-admitted derivative.
    pub fn decision(&self, instrument_id: InstrumentId) -> Option<DerivativeSelectionDecision> {
        self.decisions
            .binary_search_by_key(&instrument_id, |record| record.instrument_id)
            .ok()
            .map(|index| self.decisions[index].decision)
    }

    /// Returns the exact roll evidence applied to the source contract, when present.
    pub fn resolved_roll(&self, instrument_id: InstrumentId) -> Option<&ContractRollEvidence> {
        self.roll_evidence
            .binary_search_by_key(&instrument_id, |record| record.mapping.from_instrument_id())
            .ok()
            .map(|index| &self.roll_evidence[index])
    }

    /// Returns retained lifecycle source evidence in stable instrument order.
    pub fn lifecycle_evidence(&self) -> &[DerivativeLifecycleEvidence] {
        &self.lifecycle_evidence
    }

    /// Returns retained venue civil dates and versioned calendar rules.
    pub fn civil_dates(&self) -> &[DerivativeCivilDate] {
        &self.civil_dates
    }

    /// Returns only point-in-time-available, effective roll evidence consumed by this result.
    pub fn roll_evidence(&self) -> &[ContractRollEvidence] {
        &self.roll_evidence
    }

    /// Returns the canonical identity of active contracts and explicit roll decisions.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    /// Returns the canonical identity of all retained decision inputs and outcomes.
    pub const fn audit_hash(&self) -> Sha256Digest {
        self.audit_hash
    }

    /// Returns checked Rust-visible bytes retained by this composed result.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

fn require_input_limit(observed: usize, limits: UniverseLimits) -> Result<(), UniverseError> {
    if observed > limits.max_candidates() {
        Err(UniverseError::DerivativeInputLimitExceeded {
            limit: limits.max_candidates(),
            observed,
        })
    } else {
        Ok(())
    }
}

fn find_lifecycle(
    values: &[DerivativeLifecycleEvidence],
    instrument_id: InstrumentId,
) -> Option<&DerivativeLifecycleEvidence> {
    values
        .binary_search_by_key(&instrument_id, DerivativeLifecycleEvidence::instrument_id)
        .ok()
        .map(|index| &values[index])
}

fn find_civil_date(
    values: &[DerivativeCivilDate],
    instrument_id: InstrumentId,
) -> Option<&DerivativeCivilDate> {
    values
        .binary_search_by_key(&instrument_id, DerivativeCivilDate::instrument_id)
        .ok()
        .map(|index| &values[index])
}

fn initial_decision(
    evidence: &DerivativeLifecycleEvidence,
    civil_date: &DerivativeCivilDate,
    as_of: Timestamp,
) -> DerivativeSelectionDecision {
    if !evidence_available(&evidence.availability, as_of) {
        return DerivativeSelectionDecision::LifecycleEvidenceUnavailable;
    }
    let boundary = match &evidence.lifecycle {
        DerivativeLifecycle::Option {
            expiration_date, ..
        } => Some((DerivativeBoundary::OptionExpiration, *expiration_date)),
        DerivativeLifecycle::Future(lifecycle) => futures_boundary(lifecycle),
    };
    let Some((boundary, boundary_date)) = boundary else {
        return DerivativeSelectionDecision::MissingTerminationBoundary;
    };
    match civil_date.date.cmp(&boundary_date) {
        Ordering::Less => DerivativeSelectionDecision::Active,
        Ordering::Equal => DerivativeSelectionDecision::SameDateUnresolved {
            boundary,
            date: boundary_date,
        },
        Ordering::Greater => match &evidence.lifecycle {
            DerivativeLifecycle::Option { .. } => DerivativeSelectionDecision::OptionExpired {
                boundary,
                date: boundary_date,
            },
            DerivativeLifecycle::Future(_) => {
                DerivativeSelectionDecision::FutureExpiredWithoutRoll {
                    boundary,
                    date: boundary_date,
                }
            }
        },
    }
}

fn futures_boundary(
    lifecycle: &FuturesLifecycleDates,
) -> Option<(DerivativeBoundary, CalendarDate)> {
    [
        (
            DerivativeBoundary::FuturesLastTrade,
            lifecycle.last_trade_date(),
        ),
        (
            DerivativeBoundary::FuturesExpiration,
            lifecycle.expiration_date(),
        ),
        (
            DerivativeBoundary::FuturesMaturity,
            lifecycle.maturity_date(),
        ),
    ]
    .into_iter()
    .filter_map(|(boundary, date)| date.map(|date| (boundary, date)))
    .min_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| boundary_rank(left.0).cmp(&boundary_rank(right.0)))
    })
}

fn evidence_available(evidence: &AvailabilityEvidence, as_of: Timestamp) -> bool {
    match evidence {
        AvailabilityEvidence::Evidenced { available_at, .. } => *available_at <= as_of,
        AvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at <= as_of,
        AvailabilityEvidence::Inferred { .. } | AvailabilityEvidence::Unknown => false,
    }
}

fn reject_duplicate_lifecycle(values: &[DerivativeLifecycleEvidence]) -> Result<(), UniverseError> {
    if let Some(pair) = values
        .windows(2)
        .find(|pair| pair[0].instrument_id == pair[1].instrument_id)
    {
        Err(UniverseError::DuplicateDerivativeLifecycle {
            instrument_id: pair[0].instrument_id,
        })
    } else {
        Ok(())
    }
}

fn reject_duplicate_civil_dates(values: &[DerivativeCivilDate]) -> Result<(), UniverseError> {
    if let Some(pair) = values
        .windows(2)
        .find(|pair| pair[0].instrument_id == pair[1].instrument_id)
    {
        Err(UniverseError::DuplicateDerivativeCivilDate {
            instrument_id: pair[0].instrument_id,
        })
    } else {
        Ok(())
    }
}

const fn boundary_rank(value: DerivativeBoundary) -> u8 {
    match value {
        DerivativeBoundary::OptionExpiration => 0,
        DerivativeBoundary::FuturesLastTrade => 1,
        DerivativeBoundary::FuturesExpiration => 2,
        DerivativeBoundary::FuturesMaturity => 3,
    }
}
