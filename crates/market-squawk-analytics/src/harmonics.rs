//! Causal, provider-neutral harmonic price-pattern evidence over exact adjusted bars.
//!
//! The V1 ratio constants encode the definitions published on HarmonicTrader's official pattern
//! pages. Cypher alone uses TradingView's documented `1.13..=1.414` C-extension convention because
//! that pattern has competing conventions. No source prose or chart artwork is retained here.

use std::num::NonZeroU64;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, PriceTicks, Timestamp};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{FeatureImplementationDigest, KnownFeatureImplementation};

/// Maximum adjusted bars admitted to one in-process harmonic classification.
pub const MAX_HARMONIC_BARS: usize = 4_096;
/// Bars required after a candidate swing before the code-owned pivot rule confirms it.
pub const HARMONIC_PIVOT_CONFIRMATION_BARS: usize = 1;
/// Number of bars required to contain five independently confirmable pivots.
pub const MIN_HARMONIC_BARS: usize = HARMONIC_PIVOT_COUNT + (2 * HARMONIC_PIVOT_CONFIRMATION_BARS);
/// Number of ordered X, A, B, C, and D pivots in one evidence result.
pub const HARMONIC_PIVOT_COUNT: usize = 5;
/// Code-owned registry key for this derived-feature family.
pub const HARMONIC_PATTERN_FEATURE_NAME: &str = "technical.harmonic-pattern";

pub(crate) const HARMONIC_IMPLEMENTATION_IDENTITY: &str = concat!(
    "market-squawk-analytics::harmonics@v1;",
    "bounds-bars7to4096@v1;taxonomy-and-ordered-precedence@v1;",
    "ratio-bands-and-tolerances-scale1000000ppm@v1;bat-valid-b-lt618000ppm@v1;",
    "selector-strict-local-radius1-outside-max-excursion-high-tie-",
    "same-kind-most-extreme-earlier-tie-latest-five@v1;",
    "confirmation-max-left-selected-right-observed-and-available@v1;",
    "measurement-absolute-leg-reduced-rational@v1;",
    "completion-outward-floor-ceil@v1;",
    "invalidation-abcd-prz-d-other-x-prz-d@v1;",
    "expiry-decision-plus-five-timeframes@v1;",
    "bat-preference-382-or500-plusminus30000ppm@v1"
);

const RATIO_SCALE: u32 = 1_000_000;
const POINT_TOLERANCE: HarmonicRatio = HarmonicRatio::new(30_000);
const HARMONIC_EVIDENCE_EXPIRY_BARS: u64 = 5;

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
    decision_cutoff: Timestamp,
}

impl<'a> HarmonicPatternInput<'a> {
    /// Constructs a borrowed input; [`classify_harmonic_pattern`] performs all admission checks.
    #[must_use]
    pub const fn new(
        binding: HarmonicEvidenceBinding,
        bars: &'a [HarmonicBar],
        decision_cutoff: Timestamp,
    ) -> Self {
        Self {
            binding,
            bars,
            decision_cutoff,
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

/// Code-owned validity evidence for the admitted ratio structure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HarmonicPatternQuality {
    /// Every required ratio is valid, but a documented preferred Bat B ratio is absent.
    Valid,
    /// Every required ratio is valid and the Bat B ratio matches a documented preferred band.
    PreferredBatB,
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
    quality: HarmonicPatternQuality,
    confirmation_cutoff: Timestamp,
    decision_cutoff: Timestamp,
    expires_at: Timestamp,
    invalidation: PriceTicks,
    implementation_identity: FeatureImplementationDigest,
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

    /// Returns code-owned ratio-quality evidence; it grants no confidence authority.
    #[must_use]
    pub const fn quality(self) -> HarmonicPatternQuality {
        self.quality
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
    pub const fn implementation_identity(self) -> FeatureImplementationDigest {
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
    /// Fewer than five alternating code-owned pivots exist in the admitted bar window.
    #[error("harmonic bar window contains too few confirmed pivots")]
    InsufficientPivots,
    /// A ratio denominator is zero.
    #[error("harmonic pattern contains a zero-length leg")]
    ZeroLengthLeg,
    /// No closed V1 rule accepts the supplied pivots.
    #[error("no admitted harmonic pattern matches the supplied pivots")]
    NoMatchingPattern,
    /// Exact completion-zone or invalidation arithmetic overflowed.
    #[error("harmonic exact arithmetic overflow")]
    ArithmeticOverflow,
    /// The code-owned feature implementation identity could not be represented.
    #[error("harmonic implementation identity is invalid")]
    InvalidImplementationIdentity,
}

const B_382: HarmonicRatioBand = HarmonicRatioBand::point(382_000);
const B_500: HarmonicRatioBand = HarmonicRatioBand::point(500_000);
const B_618: HarmonicRatioBand = HarmonicRatioBand::point(618_000);
const B_786: HarmonicRatioBand = HarmonicRatioBand::point(786_000);
const B_886: HarmonicRatioBand = HarmonicRatioBand::point(886_000);
const B_1000: HarmonicRatioBand = HarmonicRatioBand::point(1_000_000);
const B_1270: HarmonicRatioBand = HarmonicRatioBand::point(1_270_000);
const B_1618: HarmonicRatioBand = HarmonicRatioBand::point(1_618_000);
const BAT_PREFERRED_B_BANDS: &[HarmonicRatioBand] = &[B_382, B_500];
const BAT_PREFERRED_B_CONSTRAINT: HarmonicRatioConstraint = constraint(
    HarmonicRatioMeasurement::AbOverXa,
    BAT_PREFERRED_B_BANDS,
    POINT_TOLERANCE,
);

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
        &[HarmonicRatioBand::range(0, 617_999)],
        HarmonicRatio::new(0),
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

/// Selects and classifies one exact causal XABCD pivot structure using the closed V1 rule set.
///
/// A successful result is derived research evidence only. It grants neither confidence nor order
/// authority. Pivot selection, outside-bar resolution, confirmation, five-pivot precedence, and
/// expiry are code-owned. The function rejects any bar or derived pivot whose observation,
/// availability, or confirmation exceeds `decision_cutoff`.
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
    let pivots = select_pivots(input.bars, input.decision_cutoff)?;
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
    let quality = pattern_quality(rule.kind, ratios);
    let invalidation = invalidation(rule.kind, direction, completion_zone, pivots)?;
    let expires_at = evidence_expiry(input.binding, input.decision_cutoff)?;
    let implementation_identity = KnownFeatureImplementation::BatchHarmonicPatterns
        .implementation_digest()
        .map_err(|_| HarmonicPatternError::InvalidImplementationIdentity)?;
    let derived = DerivedEvidence {
        rule: *rule,
        direction,
        pivots,
        ratios,
        completion_zone,
        quality,
        expires_at,
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
        quality,
        confirmation_cutoff: pivots[4].confirmed_at,
        decision_cutoff: input.decision_cutoff,
        expires_at,
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
    if !(MIN_HARMONIC_BARS..=MAX_HARMONIC_BARS).contains(&bars.len()) {
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

fn select_pivots(
    bars: &[HarmonicBar],
    decision_cutoff: Timestamp,
) -> Result<[HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT], HarmonicPatternError> {
    let mut canonical: Vec<HarmonicPivotEvidence> =
        Vec::with_capacity(bars.len().saturating_sub(2));
    for pivot_index in HARMONIC_PIVOT_CONFIRMATION_BARS
        ..bars.len().saturating_sub(HARMONIC_PIVOT_CONFIRMATION_BARS)
    {
        let left = bars[pivot_index - HARMONIC_PIVOT_CONFIRMATION_BARS];
        let bar = bars[pivot_index];
        let right = bars[pivot_index + HARMONIC_PIVOT_CONFIRMATION_BARS];
        let Some(kind) = selected_pivot_kind(left, bar, right) else {
            continue;
        };
        let confirmed_at = [
            left.observed_at,
            left.available_at,
            bar.observed_at,
            bar.available_at,
            right.observed_at,
            right.available_at,
        ]
        .into_iter()
        .max()
        .ok_or(HarmonicPatternError::ArithmeticOverflow)?;
        if confirmed_at > decision_cutoff {
            return Err(HarmonicPatternError::FutureInformation);
        }
        let candidate = HarmonicPivotEvidence {
            bar_index: u32::try_from(pivot_index)
                .map_err(|_| HarmonicPatternError::ArithmeticOverflow)?,
            kind,
            observed_at: bar.observed_at,
            available_at: bar.available_at,
            confirmed_at,
            price: match kind {
                HarmonicPivotKind::High => bar.high,
                HarmonicPivotKind::Low => bar.low,
            },
        };
        if let Some(previous) = canonical.last_mut()
            && previous.kind == candidate.kind
        {
            if is_more_extreme(candidate, *previous) {
                *previous = candidate;
            }
            continue;
        }
        canonical.push(candidate);
    }

    let selected_start = canonical
        .len()
        .checked_sub(HARMONIC_PIVOT_COUNT)
        .ok_or(HarmonicPatternError::InsufficientPivots)?;
    let selected: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT] = canonical[selected_start..]
        .try_into()
        .map_err(|_| HarmonicPatternError::InsufficientPivots)?;
    for pair in selected.windows(2) {
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
    Ok(selected)
}

fn selected_pivot_kind(
    left: HarmonicBar,
    selected: HarmonicBar,
    right: HarmonicBar,
) -> Option<HarmonicPivotKind> {
    let is_high = selected.high > left.high && selected.high > right.high;
    let is_low = selected.low < left.low && selected.low < right.low;
    match (is_high, is_low) {
        (true, false) => Some(HarmonicPivotKind::High),
        (false, true) => Some(HarmonicPivotKind::Low),
        (false, false) => None,
        (true, true) => {
            let high_excursion = selected
                .high
                .get()
                .abs_diff(left.high.get())
                .min(selected.high.get().abs_diff(right.high.get()));
            let low_excursion = selected
                .low
                .get()
                .abs_diff(left.low.get())
                .min(selected.low.get().abs_diff(right.low.get()));
            Some(if high_excursion >= low_excursion {
                HarmonicPivotKind::High
            } else {
                HarmonicPivotKind::Low
            })
        }
    }
}

const fn is_more_extreme(
    candidate: HarmonicPivotEvidence,
    previous: HarmonicPivotEvidence,
) -> bool {
    match candidate.kind {
        HarmonicPivotKind::High => candidate.price.get() > previous.price.get(),
        HarmonicPivotKind::Low => candidate.price.get() < previous.price.get(),
    }
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

fn pattern_quality(
    kind: HarmonicPatternKind,
    ratios: HarmonicRatioMeasurements,
) -> HarmonicPatternQuality {
    if kind == HarmonicPatternKind::Bat
        && ratios
            .get(HarmonicRatioMeasurement::AbOverXa)
            .is_within(BAT_PREFERRED_B_CONSTRAINT)
    {
        HarmonicPatternQuality::PreferredBatB
    } else {
        HarmonicPatternQuality::Valid
    }
}

fn evidence_expiry(
    binding: HarmonicEvidenceBinding,
    decision_cutoff: Timestamp,
) -> Result<Timestamp, HarmonicPatternError> {
    let lifetime = binding
        .timeframe_nanos
        .get()
        .checked_mul(HARMONIC_EVIDENCE_EXPIRY_BARS)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(HarmonicPatternError::ArithmeticOverflow)?;
    decision_cutoff
        .unix_nanos()
        .checked_add(lifetime)
        .map(Timestamp::from_unix_nanos)
        .ok_or(HarmonicPatternError::ArithmeticOverflow)
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
    kind: HarmonicPatternKind,
    direction: HarmonicDirection,
    completion_zone: HarmonicCompletionZone,
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
) -> Result<PriceTicks, HarmonicPatternError> {
    let value = match direction {
        HarmonicDirection::Bullish => {
            let structural_low = completion_zone.lower.get().min(pivots[4].price.get());
            let invalidation_base = if kind == HarmonicPatternKind::AbCd {
                structural_low
            } else {
                structural_low.min(pivots[0].price.get())
            };
            invalidation_base.checked_sub(1)
        }
        HarmonicDirection::Bearish => {
            let structural_high = completion_zone.upper.get().max(pivots[4].price.get());
            let invalidation_base = if kind == HarmonicPatternKind::AbCd {
                structural_high
            } else {
                structural_high.max(pivots[0].price.get())
            };
            invalidation_base.checked_add(1)
        }
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

struct DerivedEvidence {
    rule: HarmonicPatternRule,
    direction: HarmonicDirection,
    pivots: [HarmonicPivotEvidence; HARMONIC_PIVOT_COUNT],
    ratios: HarmonicRatioMeasurements,
    completion_zone: HarmonicCompletionZone,
    quality: HarmonicPatternQuality,
    expires_at: Timestamp,
    invalidation: PriceTicks,
    implementation_identity: FeatureImplementationDigest,
}

fn evidence_digest(input: HarmonicPatternInput<'_>, derived: &DerivedEvidence) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/harmonic-pattern-evidence/v1\0");
    hasher.update(input.binding.instrument_id.as_uuid().as_bytes());
    hasher.update(input.binding.timeframe_nanos.get().to_be_bytes());
    hash_digest(&mut hasher, input.binding.parent_manifest);
    hash_digest(&mut hasher, input.binding.adjustment_identity);
    hasher.update(input.decision_cutoff.unix_nanos().to_be_bytes());
    hasher.update(derived.expires_at.unix_nanos().to_be_bytes());
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
    hasher.update([quality_code(derived.quality)]);
    hasher.update(derived.invalidation.get().to_be_bytes());
    hasher.update(derived.implementation_identity.as_bytes());
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

const fn quality_code(quality: HarmonicPatternQuality) -> u8 {
    match quality {
        HarmonicPatternQuality::Valid => 0,
        HarmonicPatternQuality::PreferredBatB => 1,
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

const fn digest_algorithm_code(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 0,
        DigestAlgorithm::Blake3 => 1,
    }
}
