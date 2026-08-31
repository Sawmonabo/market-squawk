//! Causal, provider-neutral harmonic price-pattern evidence over exact adjusted bars.
//!
//! The V1 ratio constants encode the definitions published on HarmonicTrader's official pattern
//! pages. Cypher alone uses TradingView's documented `1.13..=1.414` C-extension convention because
//! that pattern has competing conventions. No source prose or chart artwork is retained here.

use std::num::NonZeroU64;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, PriceTicks, Timestamp};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Maximum adjusted bars admitted to one in-process harmonic classification.
pub const MAX_HARMONIC_BARS: usize = 4_096;
/// Bars required after a candidate swing before the code-owned pivot rule confirms it.
pub const HARMONIC_PIVOT_CONFIRMATION_BARS: usize = 1;

/// Number of ordered X, A, B, C, and D pivots in the normalized input contract.
pub const HARMONIC_PIVOT_COUNT: usize = 5;
const RATIO_SCALE: u32 = 1_000_000;
const POINT_TOLERANCE: HarmonicRatio = HarmonicRatio::new(30_000);

/// Closed V1 harmonic-pattern taxonomy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HarmonicPatternKind {
    /// Four-leg equality pattern; the leading X pivot supplies bounded context.
    AbCd,
    /// Gartley XABCD retracement pattern.
    Gartley,
    /// Bat XABCD retracement pattern.
    Bat,
    /// Butterfly XABCD extension pattern.
    Butterfly,
    /// Crab XABCD extension pattern.
    Crab,
    /// Deep Crab XABCD extension pattern.
    DeepCrab,
    /// Cypher XABCD pattern using the code-owned C-extension convention.
    Cypher,
    /// Shark structure normalized from O-X-A-B-C into the X-A-B-C-D field names.
    Shark,
}

/// Expected reversal direction after the final confirmed pivot.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HarmonicDirection {
    /// The final pivot is a low and the pattern describes possible upward reversal evidence.
    Bullish,
    /// The final pivot is a high and the pattern describes possible downward reversal evidence.
    Bearish,
}

/// Causal swing classification attached to one selected bar.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HarmonicPivotKind {
    /// Selected bar high.
    High,
    /// Selected bar low.
    Low,
}

/// One exact adjusted OHLC bar selected by the provider-neutral history authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicBar {
    observed_at: Timestamp,
    available_at: Timestamp,
    open: PriceTicks,
    high: PriceTicks,
    low: PriceTicks,
    close: PriceTicks,
}

impl HarmonicBar {
    /// Constructs one adjusted bar without performing sequence-level checks.
    #[must_use]
    pub const fn new(
        observed_at: Timestamp,
        available_at: Timestamp,
        open: PriceTicks,
        high: PriceTicks,
        low: PriceTicks,
        close: PriceTicks,
    ) -> Self {
        Self {
            observed_at,
            available_at,
            open,
            high,
            low,
            close,
        }
    }

    /// Returns the bar observation time.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns when the exact adjusted bar became available.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Returns the adjusted open.
    #[must_use]
    pub const fn open(self) -> PriceTicks {
        self.open
    }

    /// Returns the adjusted high.
    #[must_use]
    pub const fn high(self) -> PriceTicks {
        self.high
    }

    /// Returns the adjusted low.
    #[must_use]
    pub const fn low(self) -> PriceTicks {
        self.low
    }

    /// Returns the adjusted close.
    #[must_use]
    pub const fn close(self) -> PriceTicks {
        self.close
    }
}

/// Caller-selected causal pivot referencing one exact bar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicPivot {
    bar_index: u32,
    kind: HarmonicPivotKind,
    confirmed_at: Timestamp,
}

impl HarmonicPivot {
    /// Constructs a pivot reference and its causal confirmation time.
    #[must_use]
    pub const fn new(bar_index: u32, kind: HarmonicPivotKind, confirmed_at: Timestamp) -> Self {
        Self {
            bar_index,
            kind,
            confirmed_at,
        }
    }

    /// Returns the zero-based selected-bar index.
    #[must_use]
    pub const fn bar_index(self) -> u32 {
        self.bar_index
    }

    /// Returns the swing kind.
    #[must_use]
    pub const fn kind(self) -> HarmonicPivotKind {
        self.kind
    }

    /// Returns the first time the pivot was causally confirmed.
    #[must_use]
    pub const fn confirmed_at(self) -> Timestamp {
        self.confirmed_at
    }
}

/// Exact parent identities and instrument/timeframe binding for one classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicEvidenceBinding {
    instrument_id: InstrumentId,
    timeframe_nanos: NonZeroU64,
    parent_manifest: EvidenceDigest,
    adjustment_identity: EvidenceDigest,
}

impl HarmonicEvidenceBinding {
    /// Constructs the immutable parent binding.
    #[must_use]
    pub const fn new(
        instrument_id: InstrumentId,
        timeframe_nanos: NonZeroU64,
        parent_manifest: EvidenceDigest,
        adjustment_identity: EvidenceDigest,
    ) -> Self {
        Self {
            instrument_id,
            timeframe_nanos,
            parent_manifest,
            adjustment_identity,
        }
    }

    /// Returns the provider-neutral instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact bar cadence in nanoseconds.
    #[must_use]
    pub const fn timeframe_nanos(self) -> NonZeroU64 {
        self.timeframe_nanos
    }

    /// Returns the exact selected adjusted-bar manifest.
    #[must_use]
    pub const fn parent_manifest(self) -> EvidenceDigest {
        self.parent_manifest
    }

    /// Returns the exact adjustment/session policy identity applied to the bars.
    #[must_use]
    pub const fn adjustment_identity(self) -> EvidenceDigest {
        self.adjustment_identity
    }
}

/// Bounded borrowed input for one deterministic classification.
#[derive(Clone, Copy, Debug)]
pub struct HarmonicPatternInput<'a> {
    binding: HarmonicEvidenceBinding,
    bars: &'a [HarmonicBar],
    pivots: [HarmonicPivot; HARMONIC_PIVOT_COUNT],
    decision_cutoff: Timestamp,
    expires_at: Timestamp,
}

impl<'a> HarmonicPatternInput<'a> {
    /// Constructs a borrowed input; [`classify_harmonic_pattern`] performs all admission checks.
    #[must_use]
    pub const fn new(
        binding: HarmonicEvidenceBinding,
        bars: &'a [HarmonicBar],
        pivots: [HarmonicPivot; HARMONIC_PIVOT_COUNT],
        decision_cutoff: Timestamp,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            binding,
            bars,
            pivots,
            decision_cutoff,
            expires_at,
        }
    }
}

/// Exact nonnegative ratio represented without floating-point conversion.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HarmonicFraction {
    numerator: u64,
    denominator: NonZeroU64,
}

impl HarmonicFraction {
    fn from_lengths(numerator: u64, denominator: u64) -> Result<Self, HarmonicPatternError> {
        let denominator =
            NonZeroU64::new(denominator).ok_or(HarmonicPatternError::ZeroLengthLeg)?;
        let divisor = greatest_common_divisor(numerator, denominator.get());
        Ok(Self {
            numerator: numerator / divisor,
            denominator: NonZeroU64::new(denominator.get() / divisor)
                .ok_or(HarmonicPatternError::ZeroLengthLeg)?,
        })
    }

    /// Returns the reduced numerator.
    #[must_use]
    pub const fn numerator(self) -> u64 {
        self.numerator
    }

    /// Returns the reduced positive denominator.
    #[must_use]
    pub const fn denominator(self) -> NonZeroU64 {
        self.denominator
    }

    fn is_within(self, constraint: HarmonicRatioConstraint) -> bool {
        constraint.accepted.iter().any(|band| {
            let lower = band
                .lower
                .parts_per_million
                .saturating_sub(constraint.tolerance.parts_per_million);
            let upper = band
                .upper
                .parts_per_million
                .saturating_add(constraint.tolerance.parts_per_million);
            let scaled = u128::from(self.numerator) * u128::from(RATIO_SCALE);
            let denominator = u128::from(self.denominator.get());
            scaled >= u128::from(lower) * denominator && scaled <= u128::from(upper) * denominator
        })
    }
}

/// Fixed parts-per-million ratio used by code-owned pattern rules.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HarmonicRatio {
    parts_per_million: u32,
}

impl HarmonicRatio {
    const fn new(parts_per_million: u32) -> Self {
        Self { parts_per_million }
    }

    /// Returns the exact parts-per-million value, where `1_000_000` is one whole unit.
    #[must_use]
    pub const fn parts_per_million(self) -> u32 {
        self.parts_per_million
    }
}

/// Inclusive code-owned ratio band.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HarmonicRatioBand {
    lower: HarmonicRatio,
    upper: HarmonicRatio,
}

impl HarmonicRatioBand {
    const fn point(parts_per_million: u32) -> Self {
        Self {
            lower: HarmonicRatio::new(parts_per_million),
            upper: HarmonicRatio::new(parts_per_million),
        }
    }

    const fn range(lower: u32, upper: u32) -> Self {
        Self {
            lower: HarmonicRatio::new(lower),
            upper: HarmonicRatio::new(upper),
        }
    }

    /// Returns the inclusive lower bound.
    #[must_use]
    pub const fn lower(self) -> HarmonicRatio {
        self.lower
    }

    /// Returns the inclusive upper bound.
    #[must_use]
    pub const fn upper(self) -> HarmonicRatio {
        self.upper
    }
}

/// Exact leg ratio measured by one code-owned constraint.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HarmonicRatioMeasurement {
    /// `|AB| / |XA|`.
    AbOverXa,
    /// `|BC| / |AB|`.
    BcOverAb,
    /// `|CD| / |BC|`.
    CdOverBc,
    /// `|CD| / |AB|`.
    CdOverAb,
    /// `|AD| / |XA|`.
    AdOverXa,
    /// `|XC| / |XA|`.
    XcOverXa,
    /// `|CD| / |XC|`.
    CdOverXc,
}

/// One ratio rule with explicit accepted bands and an exact symmetric tolerance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicRatioConstraint {
    measurement: HarmonicRatioMeasurement,
    accepted: &'static [HarmonicRatioBand],
    tolerance: HarmonicRatio,
}

impl HarmonicRatioConstraint {
    /// Returns the measured leg relationship.
    #[must_use]
    pub const fn measurement(self) -> HarmonicRatioMeasurement {
        self.measurement
    }

    /// Returns the closed accepted bands before tolerance is applied.
    #[must_use]
    pub const fn accepted(self) -> &'static [HarmonicRatioBand] {
        self.accepted
    }

    /// Returns the symmetric parts-per-million tolerance.
    #[must_use]
    pub const fn tolerance(self) -> HarmonicRatio {
        self.tolerance
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompletionProjection {
    FromAOverXa,
    FromCOverAb,
    FromCOverXc,
}

/// Complete code-owned ratio rule used for one classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicPatternRule {
    kind: HarmonicPatternKind,
    constraints: &'static [HarmonicRatioConstraint],
    completion: CompletionProjection,
    completion_constraint: usize,
}

impl HarmonicPatternRule {
    /// Returns the closed pattern kind.
    #[must_use]
    pub const fn kind(self) -> HarmonicPatternKind {
        self.kind
    }

    /// Returns every conjunctive ratio constraint.
    #[must_use]
    pub const fn constraints(self) -> &'static [HarmonicRatioConstraint] {
        self.constraints
    }
}

/// Exact ratio measurements retained with every pattern classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicRatioMeasurements {
    ab_over_xa: HarmonicFraction,
    bc_over_ab: HarmonicFraction,
    cd_over_bc: HarmonicFraction,
    cd_over_ab: HarmonicFraction,
    ad_over_xa: HarmonicFraction,
    xc_over_xa: HarmonicFraction,
    cd_over_xc: HarmonicFraction,
}

impl HarmonicRatioMeasurements {
    /// Returns one exact reduced measurement.
    #[must_use]
    pub const fn get(self, measurement: HarmonicRatioMeasurement) -> HarmonicFraction {
        match measurement {
            HarmonicRatioMeasurement::AbOverXa => self.ab_over_xa,
            HarmonicRatioMeasurement::BcOverAb => self.bc_over_ab,
            HarmonicRatioMeasurement::CdOverBc => self.cd_over_bc,
            HarmonicRatioMeasurement::CdOverAb => self.cd_over_ab,
            HarmonicRatioMeasurement::AdOverXa => self.ad_over_xa,
            HarmonicRatioMeasurement::XcOverXa => self.xc_over_xa,
            HarmonicRatioMeasurement::CdOverXc => self.cd_over_xc,
        }
    }
}

/// Fully resolved causal pivot retained in output evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicPivotEvidence {
    bar_index: u32,
    kind: HarmonicPivotKind,
    observed_at: Timestamp,
    available_at: Timestamp,
    confirmed_at: Timestamp,
    price: PriceTicks,
}

impl HarmonicPivotEvidence {
    /// Returns the selected bar index.
    #[must_use]
    pub const fn bar_index(self) -> u32 {
        self.bar_index
    }

    /// Returns the pivot kind.
    #[must_use]
    pub const fn kind(self) -> HarmonicPivotKind {
        self.kind
    }

    /// Returns the selected bar's observation time.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns when the selected bar became available.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Returns when this pivot became causally confirmed.
    #[must_use]
    pub const fn confirmed_at(self) -> Timestamp {
        self.confirmed_at
    }

    /// Returns the exact adjusted high or low used by the pivot.
    #[must_use]
    pub const fn price(self) -> PriceTicks {
        self.price
    }
}

/// Inclusive projected pattern-completion zone in exact price ticks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicCompletionZone {
    lower: PriceTicks,
    upper: PriceTicks,
}

impl HarmonicCompletionZone {
    /// Returns the inclusive lower tick.
    #[must_use]
    pub const fn lower(self) -> PriceTicks {
        self.lower
    }

    /// Returns the inclusive upper tick.
    #[must_use]
    pub const fn upper(self) -> PriceTicks {
        self.upper
    }

    /// Returns whether the exact price lies in the completion zone.
    #[must_use]
    pub const fn contains(self, price: PriceTicks) -> bool {
        price.get() >= self.lower.get() && price.get() <= self.upper.get()
    }
}

/// Closed proof that harmonic evidence has no execution authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HarmonicExecutionAuthority {
    /// No order, routing, or execution authority is granted.
    None,
}

/// Closed proof that a detected pattern does not itself establish confidence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HarmonicConfidenceAuthority {
    /// Confidence must come from separate calibrated out-of-sample evidence.
    None,
}

/// Immutable causal harmonic-pattern evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicPatternEvidence {
    binding: HarmonicEvidenceBinding,
    kind: HarmonicPatternKind,
    direction: HarmonicDirection,
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
    ratios: HarmonicRatioMeasurements,
    completion_zone: HarmonicCompletionZone,
    confirmation_cutoff: Timestamp,
    decision_cutoff: Timestamp,
    expires_at: Timestamp,
    invalidation: PriceTicks,
    implementation_identity: EvidenceDigest,
    evidence_digest: EvidenceDigest,
}

impl HarmonicPatternEvidence {
    /// Returns the immutable instrument, timeframe, and parent binding.
    #[must_use]
    pub const fn binding(self) -> HarmonicEvidenceBinding {
        self.binding
    }

    /// Returns the classified pattern kind.
    #[must_use]
    pub const fn kind(self) -> HarmonicPatternKind {
        self.kind
    }

    /// Returns the expected reversal direction.
    #[must_use]
    pub const fn direction(self) -> HarmonicDirection {
        self.direction
    }

    /// Returns the ordered X, A, B, C, and D pivot evidence.
    #[must_use]
    pub const fn pivots(self) -> [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT] {
        self.pivots
    }

    /// Returns every exact reduced leg ratio.
    #[must_use]
    pub const fn ratios(self) -> HarmonicRatioMeasurements {
        self.ratios
    }

    /// Returns the code-owned rule that admitted the pattern.
    #[must_use]
    pub const fn rule(self) -> &'static HarmonicPatternRule {
        rule_for(self.kind)
    }

    /// Returns the projected completion zone.
    #[must_use]
    pub const fn completion_zone(self) -> HarmonicCompletionZone {
        self.completion_zone
    }

    /// Returns the final pivot confirmation time.
    #[must_use]
    pub const fn confirmation_cutoff(self) -> Timestamp {
        self.confirmation_cutoff
    }

    /// Returns the decision cutoff that bounded all input information.
    #[must_use]
    pub const fn decision_cutoff(self) -> Timestamp {
        self.decision_cutoff
    }

    /// Returns the exclusive evidence expiry.
    #[must_use]
    pub const fn expires_at(self) -> Timestamp {
        self.expires_at
    }

    /// Returns the exact price level that invalidates this pattern evidence.
    #[must_use]
    pub const fn invalidation(self) -> PriceTicks {
        self.invalidation
    }

    /// Returns the identity of the complete compiled V1 rule set.
    #[must_use]
    pub const fn implementation_identity(self) -> EvidenceDigest {
        self.implementation_identity
    }

    /// Returns the SHA-256 digest binding inputs and derived evidence.
    #[must_use]
    pub const fn evidence_digest(self) -> EvidenceDigest {
        self.evidence_digest
    }

    /// Returns the closed no-execution authority marker.
    #[must_use]
    pub const fn execution_authority(self) -> HarmonicExecutionAuthority {
        HarmonicExecutionAuthority::None
    }

    /// Returns the closed no-confidence authority marker.
    #[must_use]
    pub const fn confidence_authority(self) -> HarmonicConfidenceAuthority {
        HarmonicConfidenceAuthority::None
    }
}

/// Harmonic input, arithmetic, or classification failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum HarmonicPatternError {
    /// The input bar window is outside the fixed bound.
    #[error("harmonic bar count is outside the admitted bound")]
    InvalidBarCount,
    /// One bar has invalid time or adjusted OHLC ordering.
    #[error("harmonic adjusted bar is invalid")]
    InvalidBar,
    /// Bar observations are not strictly increasing.
    #[error("harmonic bars are not strictly time ordered")]
    NonMonotonicBars,
    /// A parent digest is not a nonzero SHA-256 identity.
    #[error("harmonic parent evidence identity is invalid")]
    InvalidParentIdentity,
    /// Evidence expiry is not strictly after the decision cutoff.
    #[error("harmonic evidence expiry is invalid")]
    InvalidExpiry,
    /// A pivot references a bar outside the supplied window.
    #[error("harmonic pivot references an unavailable bar")]
    PivotOutOfRange,
    /// A bar or pivot was observed, available, or confirmed after the decision cutoff.
    #[error("harmonic input exceeds the decision cutoff")]
    FutureInformation,
    /// Pivot bar indices, observation times, or confirmations are not strictly increasing.
    #[error("harmonic pivots are not causally monotonic")]
    NonMonotonicPivots,
    /// Pivot kinds do not alternate high and low.
    #[error("harmonic pivots do not alternate")]
    NonAlternatingPivots,
    /// Pivot prices do not move in the direction declared by their alternating kinds.
    #[error("harmonic pivot price structure is invalid")]
    InvalidPivotStructure,
    /// A selected pivot is not a strict local extremum under the code-owned confirmation rule.
    #[error("harmonic pivot is not confirmed by the supplied adjusted bars")]
    UnconfirmedPivot,
    /// A ratio denominator is zero.
    #[error("harmonic pattern contains a zero-length leg")]
    ZeroLengthLeg,
    /// No closed V1 rule accepts the supplied pivots.
    #[error("no admitted harmonic pattern matches the supplied pivots")]
    NoMatchingPattern,
    /// Exact completion-zone or invalidation arithmetic overflowed.
    #[error("harmonic exact arithmetic overflow")]
    ArithmeticOverflow,
}

const B_382: HarmonicRatioBand = HarmonicRatioBand::point(382_000);
const B_500: HarmonicRatioBand = HarmonicRatioBand::point(500_000);
const B_618: HarmonicRatioBand = HarmonicRatioBand::point(618_000);
const B_786: HarmonicRatioBand = HarmonicRatioBand::point(786_000);
const B_886: HarmonicRatioBand = HarmonicRatioBand::point(886_000);
const B_1000: HarmonicRatioBand = HarmonicRatioBand::point(1_000_000);
const B_1270: HarmonicRatioBand = HarmonicRatioBand::point(1_270_000);
const B_1618: HarmonicRatioBand = HarmonicRatioBand::point(1_618_000);

const ABCD_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    constraint(
        HarmonicRatioMeasurement::BcOverAb,
        &[HarmonicRatioBand::range(382_000, 886_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverBc,
        &[HarmonicRatioBand::range(1_130_000, 2_618_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverAb,
        &[B_1000],
        POINT_TOLERANCE,
    ),
];
const GARTLEY_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    constraint(
        HarmonicRatioMeasurement::AbOverXa,
        &[B_618],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::BcOverAb,
        &[HarmonicRatioBand::range(382_000, 886_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverBc,
        &[B_1270, B_1618],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverAb,
        &[B_1000],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::AdOverXa,
        &[B_786],
        POINT_TOLERANCE,
    ),
];
const BAT_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    constraint(
        HarmonicRatioMeasurement::AbOverXa,
        &[B_382, B_500],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::BcOverAb,
        &[HarmonicRatioBand::range(382_000, 886_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverBc,
        &[HarmonicRatioBand::range(1_618_000, 2_618_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverAb,
        &[B_1000, B_1270],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::AdOverXa,
        &[B_886],
        POINT_TOLERANCE,
    ),
];
const BUTTERFLY_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    constraint(
        HarmonicRatioMeasurement::AbOverXa,
        &[B_786],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::BcOverAb,
        &[HarmonicRatioBand::range(382_000, 886_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverBc,
        &[HarmonicRatioBand::range(1_618_000, 2_618_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverAb,
        &[B_1000, B_1270, B_1618],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::AdOverXa,
        &[B_1270],
        POINT_TOLERANCE,
    ),
];
const CRAB_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    constraint(
        HarmonicRatioMeasurement::AbOverXa,
        &[HarmonicRatioBand::range(382_000, 618_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::BcOverAb,
        &[HarmonicRatioBand::range(382_000, 886_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverBc,
        &[HarmonicRatioBand::range(2_240_000, 3_618_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverAb,
        &[B_1000, B_1270, B_1618],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::AdOverXa,
        &[B_1618],
        POINT_TOLERANCE,
    ),
];
const DEEP_CRAB_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    constraint(
        HarmonicRatioMeasurement::AbOverXa,
        &[B_886],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::BcOverAb,
        &[HarmonicRatioBand::range(382_000, 886_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverBc,
        &[HarmonicRatioBand::range(2_240_000, 3_618_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverAb,
        &[B_1000, B_1270],
        POINT_TOLERANCE,
    ),
    constraint(
        HarmonicRatioMeasurement::AdOverXa,
        &[B_1618],
        POINT_TOLERANCE,
    ),
];
const CYPHER_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    constraint(
        HarmonicRatioMeasurement::AbOverXa,
        &[HarmonicRatioBand::range(382_000, 618_000)],
        HarmonicRatio::new(0),
    ),
    // V1 deliberately binds TradingView's documented 1.13-1.414 C extension of XA.
    constraint(
        HarmonicRatioMeasurement::XcOverXa,
        &[HarmonicRatioBand::range(1_130_000, 1_414_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverXc,
        &[B_786],
        POINT_TOLERANCE,
    ),
];
const SHARK_CONSTRAINTS: &[HarmonicRatioConstraint] = &[
    // The official O-X-A-B-C anchors are normalized into this module's X-A-B-C-D names.
    constraint(
        HarmonicRatioMeasurement::BcOverAb,
        &[HarmonicRatioBand::range(1_130_000, 1_618_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::CdOverBc,
        &[HarmonicRatioBand::range(1_618_000, 2_240_000)],
        HarmonicRatio::new(0),
    ),
    constraint(
        HarmonicRatioMeasurement::AdOverXa,
        &[HarmonicRatioBand::range(886_000, 1_130_000)],
        HarmonicRatio::new(0),
    ),
];

const RULES: [HarmonicPatternRule; 8] = [
    rule(
        HarmonicPatternKind::DeepCrab,
        DEEP_CRAB_CONSTRAINTS,
        CompletionProjection::FromAOverXa,
        4,
    ),
    rule(
        HarmonicPatternKind::Crab,
        CRAB_CONSTRAINTS,
        CompletionProjection::FromAOverXa,
        4,
    ),
    rule(
        HarmonicPatternKind::Butterfly,
        BUTTERFLY_CONSTRAINTS,
        CompletionProjection::FromAOverXa,
        4,
    ),
    rule(
        HarmonicPatternKind::Bat,
        BAT_CONSTRAINTS,
        CompletionProjection::FromAOverXa,
        4,
    ),
    rule(
        HarmonicPatternKind::Gartley,
        GARTLEY_CONSTRAINTS,
        CompletionProjection::FromAOverXa,
        4,
    ),
    rule(
        HarmonicPatternKind::Cypher,
        CYPHER_CONSTRAINTS,
        CompletionProjection::FromCOverXc,
        2,
    ),
    rule(
        HarmonicPatternKind::Shark,
        SHARK_CONSTRAINTS,
        CompletionProjection::FromAOverXa,
        2,
    ),
    rule(
        HarmonicPatternKind::AbCd,
        ABCD_CONSTRAINTS,
        CompletionProjection::FromCOverAb,
        2,
    ),
];

const fn constraint(
    measurement: HarmonicRatioMeasurement,
    accepted: &'static [HarmonicRatioBand],
    tolerance: HarmonicRatio,
) -> HarmonicRatioConstraint {
    HarmonicRatioConstraint {
        measurement,
        accepted,
        tolerance,
    }
}

const fn rule(
    kind: HarmonicPatternKind,
    constraints: &'static [HarmonicRatioConstraint],
    completion: CompletionProjection,
    completion_constraint: usize,
) -> HarmonicPatternRule {
    HarmonicPatternRule {
        kind,
        constraints,
        completion,
        completion_constraint,
    }
}

const fn rule_for(kind: HarmonicPatternKind) -> &'static HarmonicPatternRule {
    match kind {
        HarmonicPatternKind::DeepCrab => &RULES[0],
        HarmonicPatternKind::Crab => &RULES[1],
        HarmonicPatternKind::Butterfly => &RULES[2],
        HarmonicPatternKind::Bat => &RULES[3],
        HarmonicPatternKind::Gartley => &RULES[4],
        HarmonicPatternKind::Cypher => &RULES[5],
        HarmonicPatternKind::Shark => &RULES[6],
        HarmonicPatternKind::AbCd => &RULES[7],
    }
}

/// Classifies one exact causal XABCD pivot structure using the closed V1 rule set.
///
/// A successful result is derived research evidence only. It grants neither confidence nor order
/// authority. The function rejects any selected bar or pivot whose observation, availability, or
/// confirmation exceeds `decision_cutoff`.
///
/// # Errors
///
/// Returns a typed error for invalid parents, bars, causality, pivot structure, arithmetic, or when
/// no closed rule accepts the measured ratios.
pub fn classify_harmonic_pattern(
    input: HarmonicPatternInput<'_>,
) -> Result<HarmonicPatternEvidence, HarmonicPatternError> {
    validate_binding(input.binding)?;
    validate_bars(input.bars, input.decision_cutoff)?;
    if input.expires_at <= input.decision_cutoff {
        return Err(HarmonicPatternError::InvalidExpiry);
    }
    let pivots = resolve_pivots(input.bars, input.pivots, input.decision_cutoff)?;
    let direction = validate_pivot_structure(pivots)?;
    let ratios = measure_ratios(pivots)?;
    let rule = RULES
        .iter()
        .find(|rule| {
            rule.constraints
                .iter()
                .all(|constraint| ratios.get(constraint.measurement).is_within(*constraint))
        })
        .ok_or(HarmonicPatternError::NoMatchingPattern)?;
    let completion_zone = completion_zone(*rule, direction, pivots)?;
    if !completion_zone.contains(pivots[4].price) {
        return Err(HarmonicPatternError::NoMatchingPattern);
    }
    let invalidation = invalidation(direction, completion_zone, pivots)?;
    let implementation_identity = implementation_identity();
    let derived = DerivedEvidence {
        rule: *rule,
        direction,
        pivots,
        ratios,
        completion_zone,
        invalidation,
        implementation_identity,
    };
    let evidence_digest = evidence_digest(input, &derived);
    Ok(HarmonicPatternEvidence {
        binding: input.binding,
        kind: rule.kind,
        direction,
        pivots,
        ratios,
        completion_zone,
        confirmation_cutoff: pivots[4].confirmed_at,
        decision_cutoff: input.decision_cutoff,
        expires_at: input.expires_at,
        invalidation,
        implementation_identity,
        evidence_digest,
    })
}

fn validate_binding(binding: HarmonicEvidenceBinding) -> Result<(), HarmonicPatternError> {
    for digest in [binding.parent_manifest, binding.adjustment_identity] {
        if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
            return Err(HarmonicPatternError::InvalidParentIdentity);
        }
    }
    Ok(())
}

fn validate_bars(
    bars: &[HarmonicBar],
    decision_cutoff: Timestamp,
) -> Result<(), HarmonicPatternError> {
    if !(HARMONIC_PIVOT_COUNT..=MAX_HARMONIC_BARS).contains(&bars.len()) {
        return Err(HarmonicPatternError::InvalidBarCount);
    }
    let mut previous_observed = None;
    for bar in bars {
        let prices = [
            bar.open.get(),
            bar.high.get(),
            bar.low.get(),
            bar.close.get(),
        ];
        if prices.into_iter().any(|price| price <= 0)
            || bar.low.get() >= bar.high.get()
            || bar.open.get() < bar.low.get()
            || bar.open.get() > bar.high.get()
            || bar.close.get() < bar.low.get()
            || bar.close.get() > bar.high.get()
            || bar.observed_at > bar.available_at
        {
            return Err(HarmonicPatternError::InvalidBar);
        }
        if bar.observed_at > decision_cutoff || bar.available_at > decision_cutoff {
            return Err(HarmonicPatternError::FutureInformation);
        }
        if previous_observed.is_some_and(|previous| previous >= bar.observed_at) {
            return Err(HarmonicPatternError::NonMonotonicBars);
        }
        previous_observed = Some(bar.observed_at);
    }
    Ok(())
}

fn resolve_pivots(
    bars: &[HarmonicBar],
    pivots: [HarmonicPivot; HARMONIC_PIVOT_COUNT],
    decision_cutoff: Timestamp,
) -> Result<[HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT], HarmonicPatternError> {
    let mut resolved = [HarmonicPivotEvidence {
        bar_index: 0,
        kind: HarmonicPivotKind::Low,
        observed_at: Timestamp::from_unix_nanos(0),
        available_at: Timestamp::from_unix_nanos(0),
        confirmed_at: Timestamp::from_unix_nanos(0),
        price: PriceTicks::new(0),
    }; HARMONIC_PIVOT_COUNT];
    for (index, pivot) in pivots.into_iter().enumerate() {
        let bar = bars
            .get(
                usize::try_from(pivot.bar_index)
                    .map_err(|_| HarmonicPatternError::PivotOutOfRange)?,
            )
            .ok_or(HarmonicPatternError::PivotOutOfRange)?;
        let pivot_index =
            usize::try_from(pivot.bar_index).map_err(|_| HarmonicPatternError::PivotOutOfRange)?;
        let left_index = pivot_index
            .checked_sub(HARMONIC_PIVOT_CONFIRMATION_BARS)
            .ok_or(HarmonicPatternError::UnconfirmedPivot)?;
        let right_index = pivot_index
            .checked_add(HARMONIC_PIVOT_CONFIRMATION_BARS)
            .ok_or(HarmonicPatternError::PivotOutOfRange)?;
        let left = bars
            .get(left_index)
            .ok_or(HarmonicPatternError::UnconfirmedPivot)?;
        let right = bars
            .get(right_index)
            .ok_or(HarmonicPatternError::UnconfirmedPivot)?;
        let is_confirmed_extremum = match pivot.kind {
            HarmonicPivotKind::High => bar.high > left.high && bar.high > right.high,
            HarmonicPivotKind::Low => bar.low < left.low && bar.low < right.low,
        };
        if !is_confirmed_extremum {
            return Err(HarmonicPatternError::UnconfirmedPivot);
        }
        if pivot.confirmed_at < right.available_at || pivot.confirmed_at > decision_cutoff {
            return Err(HarmonicPatternError::FutureInformation);
        }
        resolved[index] = HarmonicPivotEvidence {
            bar_index: pivot.bar_index,
            kind: pivot.kind,
            observed_at: bar.observed_at,
            available_at: bar.available_at,
            confirmed_at: pivot.confirmed_at,
            price: match pivot.kind {
                HarmonicPivotKind::High => bar.high,
                HarmonicPivotKind::Low => bar.low,
            },
        };
    }
    for pair in resolved.windows(2) {
        if pair[0].bar_index >= pair[1].bar_index
            || pair[0].observed_at >= pair[1].observed_at
            || pair[0].confirmed_at >= pair[1].confirmed_at
        {
            return Err(HarmonicPatternError::NonMonotonicPivots);
        }
        if pair[0].kind == pair[1].kind {
            return Err(HarmonicPatternError::NonAlternatingPivots);
        }
    }
    Ok(resolved)
}

fn validate_pivot_structure(
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
) -> Result<HarmonicDirection, HarmonicPatternError> {
    let direction = match pivots.map(|pivot| pivot.kind) {
        [
            HarmonicPivotKind::Low,
            HarmonicPivotKind::High,
            HarmonicPivotKind::Low,
            HarmonicPivotKind::High,
            HarmonicPivotKind::Low,
        ] => HarmonicDirection::Bullish,
        [
            HarmonicPivotKind::High,
            HarmonicPivotKind::Low,
            HarmonicPivotKind::High,
            HarmonicPivotKind::Low,
            HarmonicPivotKind::High,
        ] => HarmonicDirection::Bearish,
        _ => return Err(HarmonicPatternError::NonAlternatingPivots),
    };
    for pair in pivots.windows(2) {
        let ordered = match (pair[0].kind, pair[1].kind) {
            (HarmonicPivotKind::Low, HarmonicPivotKind::High) => pair[0].price < pair[1].price,
            (HarmonicPivotKind::High, HarmonicPivotKind::Low) => pair[0].price > pair[1].price,
            _ => false,
        };
        if !ordered {
            return Err(HarmonicPatternError::InvalidPivotStructure);
        }
    }
    Ok(direction)
}

fn measure_ratios(
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
) -> Result<HarmonicRatioMeasurements, HarmonicPatternError> {
    let xa = distance(pivots[0].price, pivots[1].price);
    let ab = distance(pivots[1].price, pivots[2].price);
    let bc = distance(pivots[2].price, pivots[3].price);
    let cd = distance(pivots[3].price, pivots[4].price);
    let ad = distance(pivots[1].price, pivots[4].price);
    let xc = distance(pivots[0].price, pivots[3].price);
    Ok(HarmonicRatioMeasurements {
        ab_over_xa: HarmonicFraction::from_lengths(ab, xa)?,
        bc_over_ab: HarmonicFraction::from_lengths(bc, ab)?,
        cd_over_bc: HarmonicFraction::from_lengths(cd, bc)?,
        cd_over_ab: HarmonicFraction::from_lengths(cd, ab)?,
        ad_over_xa: HarmonicFraction::from_lengths(ad, xa)?,
        xc_over_xa: HarmonicFraction::from_lengths(xc, xa)?,
        cd_over_xc: HarmonicFraction::from_lengths(cd, xc)?,
    })
}

fn completion_zone(
    rule: HarmonicPatternRule,
    direction: HarmonicDirection,
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
) -> Result<HarmonicCompletionZone, HarmonicPatternError> {
    let constraint = rule
        .constraints
        .get(rule.completion_constraint)
        .ok_or(HarmonicPatternError::ArithmeticOverflow)?;
    let mut minimum = u32::MAX;
    let mut maximum = 0;
    for band in constraint.accepted {
        minimum = minimum.min(
            band.lower
                .parts_per_million
                .saturating_sub(constraint.tolerance.parts_per_million),
        );
        maximum = maximum.max(
            band.upper
                .parts_per_million
                .saturating_add(constraint.tolerance.parts_per_million),
        );
    }
    if minimum > maximum {
        return Err(HarmonicPatternError::ArithmeticOverflow);
    }
    let (anchor, base) = match rule.completion {
        CompletionProjection::FromAOverXa => {
            (pivots[1].price, distance(pivots[0].price, pivots[1].price))
        }
        CompletionProjection::FromCOverAb => {
            (pivots[3].price, distance(pivots[1].price, pivots[2].price))
        }
        CompletionProjection::FromCOverXc => {
            (pivots[3].price, distance(pivots[0].price, pivots[3].price))
        }
    };
    project_zone(anchor, base, minimum, maximum, direction)
}

fn project_zone(
    anchor: PriceTicks,
    base: u64,
    minimum: u32,
    maximum: u32,
    direction: HarmonicDirection,
) -> Result<HarmonicCompletionZone, HarmonicPatternError> {
    let minimum_floor = scale_floor(base, minimum)?;
    let maximum_ceil = scale_ceil(base, maximum)?;
    let anchor = i128::from(anchor.get());
    let (lower, upper) = match direction {
        HarmonicDirection::Bullish => (
            anchor
                .checked_sub(maximum_ceil)
                .ok_or(HarmonicPatternError::ArithmeticOverflow)?,
            anchor
                .checked_sub(minimum_floor)
                .ok_or(HarmonicPatternError::ArithmeticOverflow)?,
        ),
        HarmonicDirection::Bearish => (
            anchor
                .checked_add(minimum_floor)
                .ok_or(HarmonicPatternError::ArithmeticOverflow)?,
            anchor
                .checked_add(maximum_ceil)
                .ok_or(HarmonicPatternError::ArithmeticOverflow)?,
        ),
    };
    Ok(HarmonicCompletionZone {
        lower: PriceTicks::new(
            i64::try_from(lower).map_err(|_| HarmonicPatternError::ArithmeticOverflow)?,
        ),
        upper: PriceTicks::new(
            i64::try_from(upper).map_err(|_| HarmonicPatternError::ArithmeticOverflow)?,
        ),
    })
}

fn invalidation(
    direction: HarmonicDirection,
    completion_zone: HarmonicCompletionZone,
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
) -> Result<PriceTicks, HarmonicPatternError> {
    let value = match direction {
        HarmonicDirection::Bullish => completion_zone
            .lower
            .get()
            .min(pivots[0].price.get())
            .min(pivots[4].price.get())
            .checked_sub(1),
        HarmonicDirection::Bearish => completion_zone
            .upper
            .get()
            .max(pivots[0].price.get())
            .max(pivots[4].price.get())
            .checked_add(1),
    }
    .ok_or(HarmonicPatternError::ArithmeticOverflow)?;
    Ok(PriceTicks::new(value))
}

fn distance(left: PriceTicks, right: PriceTicks) -> u64 {
    left.get().abs_diff(right.get())
}

fn scale_floor(base: u64, ratio: u32) -> Result<i128, HarmonicPatternError> {
    let product = u128::from(base)
        .checked_mul(u128::from(ratio))
        .ok_or(HarmonicPatternError::ArithmeticOverflow)?;
    i128::try_from(product / u128::from(RATIO_SCALE))
        .map_err(|_| HarmonicPatternError::ArithmeticOverflow)
}

fn scale_ceil(base: u64, ratio: u32) -> Result<i128, HarmonicPatternError> {
    let product = u128::from(base)
        .checked_mul(u128::from(ratio))
        .ok_or(HarmonicPatternError::ArithmeticOverflow)?;
    let scale = u128::from(RATIO_SCALE);
    let rounded = product
        .checked_add(scale - 1)
        .ok_or(HarmonicPatternError::ArithmeticOverflow)?
        / scale;
    i128::try_from(rounded).map_err(|_| HarmonicPatternError::ArithmeticOverflow)
}

fn greatest_common_divisor(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left.max(1)
}

fn implementation_identity() -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/harmonic-pattern-rules/v1\0");
    for rule in RULES {
        hasher.update([pattern_code(rule.kind)]);
        hasher.update([completion_code(rule.completion)]);
        hasher.update(bounded_len(rule.completion_constraint).to_be_bytes());
        hasher.update(bounded_len(rule.constraints.len()).to_be_bytes());
        for constraint in rule.constraints {
            hasher.update([measurement_code(constraint.measurement)]);
            hasher.update(constraint.tolerance.parts_per_million.to_be_bytes());
            hasher.update(bounded_len(constraint.accepted.len()).to_be_bytes());
            for band in constraint.accepted {
                hasher.update(band.lower.parts_per_million.to_be_bytes());
                hasher.update(band.upper.parts_per_million.to_be_bytes());
            }
        }
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

struct DerivedEvidence {
    rule: HarmonicPatternRule,
    direction: HarmonicDirection,
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
    ratios: HarmonicRatioMeasurements,
    completion_zone: HarmonicCompletionZone,
    invalidation: PriceTicks,
    implementation_identity: EvidenceDigest,
}

fn evidence_digest(input: HarmonicPatternInput<'_>, derived: &DerivedEvidence) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/harmonic-pattern-evidence/v1\0");
    hasher.update(input.binding.instrument_id.as_uuid().as_bytes());
    hasher.update(input.binding.timeframe_nanos.get().to_be_bytes());
    hash_digest(&mut hasher, input.binding.parent_manifest);
    hash_digest(&mut hasher, input.binding.adjustment_identity);
    hasher.update(input.decision_cutoff.unix_nanos().to_be_bytes());
    hasher.update(input.expires_at.unix_nanos().to_be_bytes());
    hasher.update(bounded_len(input.bars.len()).to_be_bytes());
    for bar in input.bars {
        hasher.update(bar.observed_at.unix_nanos().to_be_bytes());
        hasher.update(bar.available_at.unix_nanos().to_be_bytes());
        for price in [bar.open, bar.high, bar.low, bar.close] {
            hasher.update(price.get().to_be_bytes());
        }
    }
    hasher.update([pattern_code(derived.rule.kind)]);
    hasher.update([direction_code(derived.direction)]);
    for pivot in derived.pivots {
        hasher.update(pivot.bar_index.to_be_bytes());
        hasher.update([pivot_kind_code(pivot.kind)]);
        hasher.update(pivot.observed_at.unix_nanos().to_be_bytes());
        hasher.update(pivot.available_at.unix_nanos().to_be_bytes());
        hasher.update(pivot.confirmed_at.unix_nanos().to_be_bytes());
        hasher.update(pivot.price.get().to_be_bytes());
    }
    for measurement in [
        HarmonicRatioMeasurement::AbOverXa,
        HarmonicRatioMeasurement::BcOverAb,
        HarmonicRatioMeasurement::CdOverBc,
        HarmonicRatioMeasurement::CdOverAb,
        HarmonicRatioMeasurement::AdOverXa,
        HarmonicRatioMeasurement::XcOverXa,
        HarmonicRatioMeasurement::CdOverXc,
    ] {
        let ratio = derived.ratios.get(measurement);
        hasher.update([measurement_code(measurement)]);
        hasher.update(ratio.numerator.to_be_bytes());
        hasher.update(ratio.denominator.get().to_be_bytes());
    }
    hasher.update(derived.completion_zone.lower.get().to_be_bytes());
    hasher.update(derived.completion_zone.upper.get().to_be_bytes());
    hasher.update(derived.invalidation.get().to_be_bytes());
    hash_digest(&mut hasher, derived.implementation_identity);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn hash_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([digest_algorithm_code(digest.algorithm())]);
    hasher.update(digest.bytes());
}

fn bounded_len(value: usize) -> u32 {
    match u32::try_from(value) {
        Ok(value) => value,
        Err(_) => u32::MAX,
    }
}

const fn pattern_code(kind: HarmonicPatternKind) -> u8 {
    match kind {
        HarmonicPatternKind::AbCd => 0,
        HarmonicPatternKind::Gartley => 1,
        HarmonicPatternKind::Bat => 2,
        HarmonicPatternKind::Butterfly => 3,
        HarmonicPatternKind::Crab => 4,
        HarmonicPatternKind::DeepCrab => 5,
        HarmonicPatternKind::Cypher => 6,
        HarmonicPatternKind::Shark => 7,
    }
}

const fn direction_code(direction: HarmonicDirection) -> u8 {
    match direction {
        HarmonicDirection::Bullish => 0,
        HarmonicDirection::Bearish => 1,
    }
}

const fn pivot_kind_code(kind: HarmonicPivotKind) -> u8 {
    match kind {
        HarmonicPivotKind::High => 0,
        HarmonicPivotKind::Low => 1,
    }
}

const fn measurement_code(measurement: HarmonicRatioMeasurement) -> u8 {
    match measurement {
        HarmonicRatioMeasurement::AbOverXa => 0,
        HarmonicRatioMeasurement::BcOverAb => 1,
        HarmonicRatioMeasurement::CdOverBc => 2,
        HarmonicRatioMeasurement::CdOverAb => 3,
        HarmonicRatioMeasurement::AdOverXa => 4,
        HarmonicRatioMeasurement::XcOverXa => 5,
        HarmonicRatioMeasurement::CdOverXc => 6,
    }
}

const fn completion_code(projection: CompletionProjection) -> u8 {
    match projection {
        CompletionProjection::FromAOverXa => 0,
        CompletionProjection::FromCOverAb => 1,
        CompletionProjection::FromCOverXc => 2,
    }
}

const fn digest_algorithm_code(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 0,
        DigestAlgorithm::Blake3 => 1,
    }
}
