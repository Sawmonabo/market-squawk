//! Versioned deterministic hierarchy classification.

use std::mem::size_of;
use std::sync::Arc;

use market_squawk_domain::{DataQuality, FairValueHierarchy};

use crate::approval::OverrideId;
use crate::measurement::{
    InputId, InputInstrumentRelation, InputObservability, InputSignificance, MarketAccess,
    MarketActivity, MeasurementId, PriceAdjustment, ValuationInput, ValuationMeasurement,
};
use crate::{CanonicalHasher, FairValueError, FairValueEvidenceHash, checked_add};

const CURRENT_RULESET_VERSION: u32 = 1;

digest_id!(
    /// SHA-256 commitment to the exact code-owned classification semantics and parameters.
    RulesetHash
);
digest_id!(
    /// SHA-256 content identity of one immutable classification decision.
    DecisionId
);

/// Every predicate evaluated for every retained valuation input.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Predicate {
    /// Input affects classification of the measurement in its entirety.
    SignificantInput,
    /// Input subject is the measured instrument.
    SubjectInstrumentMatches,
    /// Referenced instrument is identical to the measured instrument.
    IdenticalInstrument,
    /// Input is a quoted price.
    QuotedPrice,
    /// Input is used without adjustment.
    UnadjustedPrice,
    /// Market is active at the measurement date.
    ActiveMarket,
    /// Reporting entity can access the market at the measurement date.
    AccessibleMarket,
    /// Source observation is not after the measurement instant.
    MeasurementDateRelevant,
    /// Source observation is within the ruleset's quote-age bound.
    WithinFreshnessLimit,
    /// Source-specific fair-value evidence verification passed.
    SourceEvidenceVerified,
    /// Evidence names a market venue.
    VenueEvidencePresent,
    /// Input and measurement currencies match.
    CurrencyMatches,
    /// Input and measurement decimal scales match.
    ScaleMatches,
    /// Quoted input and resulting measurement amounts match exactly.
    AmountMatches,
    /// Independent delivery quality is not a class excluded from automated Level 1 assessment.
    QualitySupportsLevel1,
}

impl Predicate {
    /// Canonical predicate order committed by ruleset v1.
    pub const ALL: [Self; 15] = [
        Self::SignificantInput,
        Self::SubjectInstrumentMatches,
        Self::IdenticalInstrument,
        Self::QuotedPrice,
        Self::UnadjustedPrice,
        Self::ActiveMarket,
        Self::AccessibleMarket,
        Self::MeasurementDateRelevant,
        Self::WithinFreshnessLimit,
        Self::SourceEvidenceVerified,
        Self::VenueEvidencePresent,
        Self::CurrencyMatches,
        Self::ScaleMatches,
        Self::AmountMatches,
        Self::QualitySupportsLevel1,
    ];
}

/// One immutable truth-table cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PredicateResult {
    input_id: InputId,
    predicate: Predicate,
    passed: bool,
}

impl PredicateResult {
    /// Returns the input evaluated.
    pub const fn input_id(self) -> InputId {
        self.input_id
    }

    /// Returns the predicate evaluated.
    pub const fn predicate(self) -> Predicate {
        self.predicate
    }

    /// Returns the deterministic result.
    pub const fn passed(self) -> bool {
        self.passed
    }
}

/// Stable reason code explaining classification or an explicit override.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DecisionReasonCode {
    /// No significant input supports a hierarchy conclusion.
    NoSignificantInput,
    /// Referenced instrument is similar or a proxy.
    NotIdenticalInstrument,
    /// Input is not a quoted price.
    NotQuotedPrice,
    /// Source price was adjusted.
    AdjustedPrice,
    /// Market was inactive or not assessed.
    MarketNotActive,
    /// Market was inaccessible or not assessed.
    MarketNotAccessible,
    /// Evidence was observed after the measurement instant.
    PostMeasurementEvidence,
    /// Evidence exceeds the ruleset freshness bound.
    EvidenceTooOld,
    /// Source-specific fair-value verification did not pass.
    SourceEvidenceUnverified,
    /// A market venue is not present in the evidence origin.
    VenueEvidenceMissing,
    /// Input and measurement currencies differ.
    CurrencyMismatch,
    /// Input and measurement scales differ.
    ScaleMismatch,
    /// Quoted input and measurement amounts differ.
    AmountMismatch,
    /// Delivery quality is not admitted for automated Level 1 assessment.
    QualityNotLevel1,
    /// Evidence was explicitly stale.
    EvidenceStale,
    /// Evidence was explicitly quarantined.
    EvidenceQuarantined,
    /// A significant unobservable input controls the measurement.
    UnobservableSignificantInput,
    /// A separately governed override selected the hierarchy.
    OverrideApplied,
}

/// One reason tied to an input when applicable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecisionReason {
    input_id: Option<InputId>,
    code: DecisionReasonCode,
}

impl DecisionReason {
    /// Returns the affected input, or `None` for a measurement-level reason.
    pub const fn input_id(self) -> Option<InputId> {
        self.input_id
    }

    /// Returns stable reason code.
    pub const fn code(self) -> DecisionReasonCode {
        self.code
    }
}

/// Immutable provenance of a classification conclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionBasis {
    /// Deterministic result of the named ruleset.
    Rules,
    /// Explicit judgment based on an immutable earlier rules decision.
    Override {
        /// Original deterministic decision.
        base_decision_id: DecisionId,
        /// Immutable override record.
        override_id: OverrideId,
    },
}

/// Code-owned converged ASC 820/IFRS 13 classification rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassificationRuleset {
    version: u32,
    max_quote_age_nanos: u64,
    market_activity_policy: crate::MarketActivityPolicy,
    hash: RulesetHash,
}

impl ClassificationRuleset {
    /// Constructs the current code-owned ruleset with a caller-selected positive quote-age bound.
    ///
    /// # Errors
    ///
    /// Rejects zero or values above signed timestamp arithmetic range.
    pub fn current(max_quote_age_nanos: u64) -> Result<Self, FairValueError> {
        if max_quote_age_nanos == 0 || max_quote_age_nanos > i64::MAX as u64 {
            return Err(FairValueError::InvalidRuleset);
        }
        let market_activity_policy =
            crate::MarketActivityPolicy::try_new(10, 1_000, 4_096, 300_000_000_000)?;
        let mut hash = CanonicalHasher::new(b"market-squawk/asc820-ifrs13-ruleset/v1");
        hash.u32(CURRENT_RULESET_VERSION);
        hash.u64(max_quote_age_nanos);
        hash.fixed(market_activity_policy.hash().bytes());
        for predicate in Predicate::ALL {
            hash.u8(predicate_tag(predicate));
        }
        // This exact list proves that accounting hierarchy is not a conversion from DataQuality.
        hash.u8(1); // DirectVerified is admissible evidence.
        hash.u8(2); // DirectUnverified may be independently verified for fair-value use.
        Ok(Self {
            version: CURRENT_RULESET_VERSION,
            max_quote_age_nanos,
            market_activity_policy,
            hash: RulesetHash(hash.finish()),
        })
    }

    /// Returns code-owned semantic version.
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns exact semantic and parameter identity.
    pub const fn hash(&self) -> RulesetHash {
        self.hash
    }

    /// Returns maximum accepted source age at the measurement instant.
    pub const fn max_quote_age_nanos(&self) -> u64 {
        self.max_quote_age_nanos
    }

    /// Returns the code-owned activity assessment policy bound into this ruleset.
    pub const fn market_activity_policy(&self) -> crate::MarketActivityPolicy {
        self.market_activity_policy
    }

    pub(crate) fn classify(
        &self,
        measurement: &ValuationMeasurement,
    ) -> Result<ClassificationDecision, FairValueError> {
        let result_capacity = measurement
            .inputs()
            .len()
            .checked_mul(Predicate::ALL.len())
            .ok_or(FairValueError::Arithmetic)?;
        let mut truth_table = Vec::new();
        truth_table
            .try_reserve_exact(result_capacity)
            .map_err(|_| FairValueError::Arithmetic)?;
        let mut reasons = Vec::new();
        reasons
            .try_reserve_exact(result_capacity)
            .map_err(|_| FairValueError::Arithmetic)?;
        let mut significant_count = 0_usize;
        let mut hierarchy = FairValueHierarchy::Level1;

        for input in measurement.inputs() {
            let results = evaluate_input(self, measurement, input);
            truth_table.extend(results);
            if input.significance() != InputSignificance::Significant {
                continue;
            }
            significant_count = significant_count
                .checked_add(1)
                .ok_or(FairValueError::Arithmetic)?;
            for result in results {
                if !result.passed()
                    && let Some(code) = predicate_reason(result.predicate())
                {
                    reasons.push(DecisionReason {
                        input_id: Some(input.id()),
                        code,
                    });
                }
            }
            match input.data_quality() {
                DataQuality::Stale => reasons.push(DecisionReason {
                    input_id: Some(input.id()),
                    code: DecisionReasonCode::EvidenceStale,
                }),
                DataQuality::Quarantined => reasons.push(DecisionReason {
                    input_id: Some(input.id()),
                    code: DecisionReasonCode::EvidenceQuarantined,
                }),
                _ => {}
            }

            let input_hierarchy = input_hierarchy(input, &results);
            if input_hierarchy == FairValueHierarchy::Level3 {
                reasons.push(DecisionReason {
                    input_id: Some(input.id()),
                    code: DecisionReasonCode::UnobservableSignificantInput,
                });
            }
            hierarchy = lower_hierarchy(hierarchy, input_hierarchy);
        }

        if significant_count == 0 {
            hierarchy = FairValueHierarchy::Unclassified;
            reasons.push(DecisionReason {
                input_id: None,
                code: DecisionReasonCode::NoSignificantInput,
            });
        }
        ClassificationDecision::new(ClassificationDecisionSpec {
            measurement_id: measurement.id(),
            evidence_hash: measurement.evidence_hash(),
            ruleset_version: self.version,
            ruleset_hash: self.hash,
            hierarchy,
            basis: DecisionBasis::Rules,
            truth_table,
            reasons,
        })
    }
}

struct ClassificationDecisionSpec {
    measurement_id: MeasurementId,
    evidence_hash: FairValueEvidenceHash,
    ruleset_version: u32,
    ruleset_hash: RulesetHash,
    hierarchy: FairValueHierarchy,
    basis: DecisionBasis,
    truth_table: Vec<PredicateResult>,
    reasons: Vec<DecisionReason>,
}

/// Immutable classification and its complete truth/reason evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassificationDecision {
    id: DecisionId,
    measurement_id: MeasurementId,
    evidence_hash: FairValueEvidenceHash,
    ruleset_version: u32,
    ruleset_hash: RulesetHash,
    hierarchy: FairValueHierarchy,
    basis: DecisionBasis,
    truth_table: Arc<[PredicateResult]>,
    reasons: Arc<[DecisionReason]>,
    retained_bytes: usize,
}

impl ClassificationDecision {
    fn new(spec: ClassificationDecisionSpec) -> Result<Self, FairValueError> {
        let mut hash = CanonicalHasher::new(b"market-squawk/classification-decision/v1");
        hash.fixed(spec.measurement_id.bytes());
        hash.fixed(spec.evidence_hash.bytes());
        hash.u32(spec.ruleset_version);
        hash.fixed(spec.ruleset_hash.bytes());
        hash.u8(hierarchy_tag(spec.hierarchy));
        hash_basis(&mut hash, spec.basis);
        hash.u64(u64::try_from(spec.truth_table.len()).map_err(|_| FairValueError::Arithmetic)?);
        for result in &spec.truth_table {
            hash.fixed(result.input_id().bytes());
            hash.u8(predicate_tag(result.predicate()));
            hash.u8(u8::from(result.passed()));
        }
        hash.u64(u64::try_from(spec.reasons.len()).map_err(|_| FairValueError::Arithmetic)?);
        for reason in &spec.reasons {
            match reason.input_id() {
                Some(input_id) => {
                    hash.u8(1);
                    hash.fixed(input_id.bytes());
                }
                None => hash.u8(0),
            }
            hash.u8(reason_tag(reason.code()));
        }
        let truth_bytes = size_of::<PredicateResult>()
            .checked_mul(spec.truth_table.len())
            .ok_or(FairValueError::Arithmetic)?;
        let reason_bytes = size_of::<DecisionReason>()
            .checked_mul(spec.reasons.len())
            .ok_or(FairValueError::Arithmetic)?;
        let retained_bytes =
            checked_add(size_of::<Self>(), checked_add(truth_bytes, reason_bytes)?)?;
        Ok(Self {
            id: DecisionId(hash.finish()),
            measurement_id: spec.measurement_id,
            evidence_hash: spec.evidence_hash,
            ruleset_version: spec.ruleset_version,
            ruleset_hash: spec.ruleset_hash,
            hierarchy: spec.hierarchy,
            basis: spec.basis,
            truth_table: Arc::from(spec.truth_table.into_boxed_slice()),
            reasons: Arc::from(spec.reasons.into_boxed_slice()),
            retained_bytes,
        })
    }

    pub(crate) fn overridden(
        base: &Self,
        override_id: OverrideId,
        hierarchy: FairValueHierarchy,
    ) -> Result<Self, FairValueError> {
        let mut reasons = base.reasons.to_vec();
        reasons.push(DecisionReason {
            input_id: None,
            code: DecisionReasonCode::OverrideApplied,
        });
        Self::new(ClassificationDecisionSpec {
            measurement_id: base.measurement_id,
            evidence_hash: base.evidence_hash,
            ruleset_version: base.ruleset_version,
            ruleset_hash: base.ruleset_hash,
            hierarchy,
            basis: DecisionBasis::Override {
                base_decision_id: base.id,
                override_id,
            },
            truth_table: base.truth_table.to_vec(),
            reasons,
        })
    }

    /// Returns immutable decision identity.
    pub const fn id(&self) -> DecisionId {
        self.id
    }

    /// Returns measured record identity.
    pub const fn measurement_id(&self) -> MeasurementId {
        self.measurement_id
    }

    /// Returns exact evidence-set identity without exposing mutable evidence.
    pub const fn evidence_hash(&self) -> FairValueEvidenceHash {
        self.evidence_hash
    }

    /// Returns code-owned ruleset version.
    pub const fn ruleset_version(&self) -> u32 {
        self.ruleset_version
    }

    /// Returns exact ruleset identity.
    pub const fn ruleset_hash(&self) -> RulesetHash {
        self.ruleset_hash
    }

    /// Returns hierarchy conclusion.
    pub const fn hierarchy(&self) -> FairValueHierarchy {
        self.hierarchy
    }

    /// Returns whether rules or an explicit override produced the conclusion.
    pub const fn basis(&self) -> DecisionBasis {
        self.basis
    }

    /// Returns the complete input/predicate truth table in canonical order.
    pub fn truth_table(&self) -> &[PredicateResult] {
        &self.truth_table
    }

    /// Returns complete deterministic reasons.
    pub fn reasons(&self) -> &[DecisionReason] {
        &self.reasons
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

fn evaluate_input(
    ruleset: &ClassificationRuleset,
    measurement: &ValuationMeasurement,
    input: &ValuationInput,
) -> [PredicateResult; Predicate::ALL.len()] {
    let measurement_time = measurement.measurement_at().unix_nanos();
    let evidence_time = input.evidence().relevance_timestamp();
    let evidence = input.evidence();
    let relevant = evidence
        .available_at()
        .is_some_and(|value| value.unix_nanos() <= measurement_time)
        && [
            evidence.source_timestamp(),
            evidence.effective_at(),
            evidence.published_at(),
        ]
        .into_iter()
        .flatten()
        .all(|value| value.unix_nanos() <= measurement_time);
    let fresh = evidence_time.is_some_and(|value| {
        relevant
            && i128::from(measurement_time) - i128::from(value.unix_nanos())
                <= i128::from(ruleset.max_quote_age_nanos())
    });
    let values = [
        input.significance() == InputSignificance::Significant,
        input.subject_instrument_id() == measurement.instrument_id(),
        input.relationship() == InputInstrumentRelation::Identical,
        input.observability() == InputObservability::QuotedPrice,
        input.adjustment() == PriceAdjustment::None,
        input.market_activity() == MarketActivity::Active
            && input.evidence().origin().market_activity_policy_hash()
                == Some(ruleset.market_activity_policy().hash().bytes()),
        input.market_access() == MarketAccess::Accessible,
        relevant,
        fresh,
        input.evidence().verification() == crate::EvidenceVerification::Verified,
        input.evidence().origin().is_market() && input.evidence().origin().venue_id().is_some(),
        input.amount().money().currency() == measurement.amount().money().currency(),
        input.amount().scale() == measurement.amount().scale(),
        input.amount() == measurement.amount(),
        quality_supports_level1(input.data_quality()),
    ];
    std::array::from_fn(|index| PredicateResult {
        input_id: input.id(),
        predicate: Predicate::ALL[index],
        passed: values[index],
    })
}

fn input_hierarchy(
    input: &ValuationInput,
    results: &[PredicateResult; Predicate::ALL.len()],
) -> FairValueHierarchy {
    let pass = |predicate| {
        results
            .iter()
            .find(|result| result.predicate() == predicate)
            .is_some_and(|result| result.passed())
    };
    let usable = pass(Predicate::SubjectInstrumentMatches)
        && pass(Predicate::MeasurementDateRelevant)
        && pass(Predicate::CurrencyMatches)
        && pass(Predicate::ScaleMatches)
        && input.data_quality() != DataQuality::Quarantined;
    if !usable {
        FairValueHierarchy::Unclassified
    } else if Predicate::ALL
        .into_iter()
        .filter(|predicate| *predicate != Predicate::SignificantInput)
        .all(pass)
    {
        FairValueHierarchy::Level1
    } else if input.observability() == InputObservability::Unobservable
        || input.adjustment() == PriceAdjustment::Unobservable
    {
        FairValueHierarchy::Level3
    } else {
        FairValueHierarchy::Level2
    }
}

const fn lower_hierarchy(
    current: FairValueHierarchy,
    candidate: FairValueHierarchy,
) -> FairValueHierarchy {
    if matches!(current, FairValueHierarchy::Unclassified)
        || matches!(candidate, FairValueHierarchy::Unclassified)
    {
        FairValueHierarchy::Unclassified
    } else if matches!(current, FairValueHierarchy::Level3)
        || matches!(candidate, FairValueHierarchy::Level3)
    {
        FairValueHierarchy::Level3
    } else if matches!(current, FairValueHierarchy::Level2)
        || matches!(candidate, FairValueHierarchy::Level2)
    {
        FairValueHierarchy::Level2
    } else {
        FairValueHierarchy::Level1
    }
}

const fn quality_supports_level1(quality: DataQuality) -> bool {
    matches!(
        quality,
        DataQuality::DirectVerified | DataQuality::DirectUnverified
    )
}

const fn predicate_reason(predicate: Predicate) -> Option<DecisionReasonCode> {
    match predicate {
        Predicate::SignificantInput | Predicate::SubjectInstrumentMatches => None,
        Predicate::IdenticalInstrument => Some(DecisionReasonCode::NotIdenticalInstrument),
        Predicate::QuotedPrice => Some(DecisionReasonCode::NotQuotedPrice),
        Predicate::UnadjustedPrice => Some(DecisionReasonCode::AdjustedPrice),
        Predicate::ActiveMarket => Some(DecisionReasonCode::MarketNotActive),
        Predicate::AccessibleMarket => Some(DecisionReasonCode::MarketNotAccessible),
        Predicate::MeasurementDateRelevant => Some(DecisionReasonCode::PostMeasurementEvidence),
        Predicate::WithinFreshnessLimit => Some(DecisionReasonCode::EvidenceTooOld),
        Predicate::SourceEvidenceVerified => Some(DecisionReasonCode::SourceEvidenceUnverified),
        Predicate::VenueEvidencePresent => Some(DecisionReasonCode::VenueEvidenceMissing),
        Predicate::CurrencyMatches => Some(DecisionReasonCode::CurrencyMismatch),
        Predicate::ScaleMatches => Some(DecisionReasonCode::ScaleMismatch),
        Predicate::AmountMatches => Some(DecisionReasonCode::AmountMismatch),
        Predicate::QualitySupportsLevel1 => Some(DecisionReasonCode::QualityNotLevel1),
    }
}

const fn predicate_tag(value: Predicate) -> u8 {
    value as u8 + 1
}

const fn reason_tag(value: DecisionReasonCode) -> u8 {
    value as u8 + 1
}

const fn hierarchy_tag(value: FairValueHierarchy) -> u8 {
    match value {
        FairValueHierarchy::Level1 => 1,
        FairValueHierarchy::Level2 => 2,
        FairValueHierarchy::Level3 => 3,
        FairValueHierarchy::Unclassified => 4,
    }
}

fn hash_basis(hash: &mut CanonicalHasher, basis: DecisionBasis) {
    match basis {
        DecisionBasis::Rules => hash.u8(1),
        DecisionBasis::Override {
            base_decision_id,
            override_id,
        } => {
            hash.u8(2);
            hash.fixed(base_decision_id.bytes());
            hash.fixed(override_id.bytes());
        }
    }
}
