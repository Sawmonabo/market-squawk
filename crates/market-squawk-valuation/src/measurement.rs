//! Exact valuation inputs and measurements.

use std::mem::size_of;
use std::sync::Arc;

use market_squawk_domain::{DataQuality, InstrumentId, Money, Timestamp};
use rust_decimal::Decimal;

use crate::{
    CanonicalHasher, FairValueError, FairValueEvidence, FairValueEvidenceHash, checked_add,
};

const MAX_ACTOR_ID_BYTES: usize = 128;
const HARD_MAX_MEASUREMENT_INPUTS: usize = 4_096;

digest_id!(
    /// SHA-256 content identity of one valuation input.
    InputId
);
digest_id!(
    /// SHA-256 content identity of one immutable valuation measurement.
    MeasurementId
);

/// Bounded audit actor identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActorId(Box<str>);

impl ActorId {
    /// Returns the validated identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.0.len()
    }
}

impl TryFrom<&str> for ActorId {
    type Error = FairValueError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.len() > MAX_ACTOR_ID_BYTES
            || value
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            Err(FairValueError::InvalidActorId)
        } else {
            Ok(Self(value.into()))
        }
    }
}

/// Exact currency amount plus the declared accounting decimal scale.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ValuationAmount {
    money: Money,
    scale: u8,
}

impl ValuationAmount {
    /// Validates that the declared scale can represent the exact amount without rounding.
    ///
    /// # Errors
    ///
    /// Rejects scales above [`Decimal::MAX_SCALE`] or below the exact amount's scale.
    pub fn try_new(money: Money, scale: u8) -> Result<Self, FairValueError> {
        if u32::from(scale) > Decimal::MAX_SCALE || money.amount().scale() > u32::from(scale) {
            Err(FairValueError::InvalidAmount)
        } else {
            Ok(Self { money, scale })
        }
    }

    /// Returns exact currency-aware money.
    pub const fn money(self) -> Money {
        self.money
    }

    /// Returns declared decimal places.
    pub const fn scale(self) -> u8 {
        self.scale
    }

    pub(crate) fn hash_into(self, hash: &mut CanonicalHasher) {
        hash.bytes(self.money.currency().as_str().as_bytes());
        hash.bytes(&self.money.amount().mantissa().to_be_bytes());
        hash.u32(self.money.amount().scale());
        hash.u8(self.scale);
    }
}

/// Relationship between the measured instrument and the referenced input instrument.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum InputInstrumentRelation {
    /// The input names the exact internal instrument being measured.
    Identical,
    /// The input names a comparable but different instrument.
    Similar,
    /// The input names a proxy instrument selected by policy or judgment.
    Proxy,
}

/// Accounting observability of an input, independent of delivery quality.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum InputObservability {
    /// A quoted price rather than a model-derived scalar.
    QuotedPrice,
    /// An observable input other than a qualifying Level 1 quote.
    Observable,
    /// An unobservable market-participant assumption.
    Unobservable,
}

/// Whether an input affects classification of the measurement in its entirety.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum InputSignificance {
    /// The input is significant to the measurement in its entirety.
    Significant,
    /// The input is not significant to the measurement in its entirety.
    NotSignificant,
}

/// Evidenced market activity conclusion at the measurement date.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MarketActivity {
    /// Transactions occur with sufficient frequency and volume for ongoing pricing evidence.
    Active,
    /// Activity is insufficient for an active-market conclusion.
    Inactive,
    /// Market activity has not been assessed.
    NotAssessed,
}

/// Reporting-entity market accessibility at the measurement date.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MarketAccess {
    /// The reporting entity can access the market.
    Accessible,
    /// The reporting entity cannot access the market.
    Inaccessible,
    /// Accessibility has not been assessed.
    NotAssessed,
}

/// Adjustment applied to the source input.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PriceAdjustment {
    /// Source input is used without adjustment.
    None,
    /// Adjustment is based entirely on observable inputs.
    Observable,
    /// Adjustment uses a significant unobservable input.
    Unobservable,
}

/// Valuation technique used to produce the measurement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValuationMethod {
    /// Unadjusted quoted market price.
    QuotedMarketPrice,
    /// Market-comparable technique.
    MarketApproach,
    /// Present-value or other income technique.
    IncomeApproach,
    /// Current-replacement-cost technique.
    CostApproach,
}

/// Untrusted fields used to construct one immutable valuation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationInputSpec {
    /// Instrument being measured.
    pub subject_instrument_id: InstrumentId,
    /// Instrument named by the source evidence.
    pub reference_instrument_id: InstrumentId,
    /// Relationship between the two instrument identities.
    pub relationship: InputInstrumentRelation,
    /// Exact input amount and accounting scale.
    pub amount: ValuationAmount,
    /// Significance to the measurement in its entirety.
    pub significance: InputSignificance,
    /// Accounting observability.
    pub observability: InputObservability,
    /// Source-input adjustment.
    pub adjustment: PriceAdjustment,
    /// Market activity conclusion.
    pub market_activity: MarketActivity,
    /// Reporting-entity market access conclusion.
    pub market_access: MarketAccess,
    /// Independent source quality classification.
    pub data_quality: DataQuality,
    /// Complete immutable source evidence.
    pub evidence: FairValueEvidence,
}

/// One immutable valuation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationInput {
    id: InputId,
    subject_instrument_id: InstrumentId,
    reference_instrument_id: InstrumentId,
    relationship: InputInstrumentRelation,
    amount: ValuationAmount,
    significance: InputSignificance,
    observability: InputObservability,
    adjustment: PriceAdjustment,
    market_activity: MarketActivity,
    market_access: MarketAccess,
    data_quality: DataQuality,
    evidence: FairValueEvidence,
    retained_bytes: usize,
}

impl ValuationInput {
    /// Checks relationship invariants and derives an immutable content identity.
    ///
    /// # Errors
    ///
    /// Rejects an `Identical` relation across different identities or a non-identical relation
    /// that reuses the measured identity.
    pub fn try_new(spec: ValuationInputSpec) -> Result<Self, FairValueError> {
        let same = spec.subject_instrument_id == spec.reference_instrument_id;
        if same != (spec.relationship == InputInstrumentRelation::Identical) {
            return Err(FairValueError::InvalidInstrumentRelationship);
        }
        let retained_bytes = checked_add(size_of::<Self>(), spec.evidence.retained_bytes())?;
        let mut hash = CanonicalHasher::new(b"market-squawk/valuation-input/v1");
        hash.bytes(spec.subject_instrument_id.as_uuid().as_bytes());
        hash.bytes(spec.reference_instrument_id.as_uuid().as_bytes());
        hash.u8(relation_tag(spec.relationship));
        spec.amount.hash_into(&mut hash);
        hash.u8(significance_tag(spec.significance));
        hash.u8(observability_tag(spec.observability));
        hash.u8(adjustment_tag(spec.adjustment));
        hash.u8(activity_tag(spec.market_activity));
        hash.u8(access_tag(spec.market_access));
        hash.u8(quality_tag(spec.data_quality));
        hash.fixed(spec.evidence.hash().bytes());
        Ok(Self {
            id: InputId(hash.finish()),
            subject_instrument_id: spec.subject_instrument_id,
            reference_instrument_id: spec.reference_instrument_id,
            relationship: spec.relationship,
            amount: spec.amount,
            significance: spec.significance,
            observability: spec.observability,
            adjustment: spec.adjustment,
            market_activity: spec.market_activity,
            market_access: spec.market_access,
            data_quality: spec.data_quality,
            evidence: spec.evidence,
            retained_bytes,
        })
    }

    /// Returns immutable input identity.
    pub const fn id(&self) -> InputId {
        self.id
    }

    /// Returns measured instrument identity.
    pub const fn subject_instrument_id(&self) -> InstrumentId {
        self.subject_instrument_id
    }

    /// Returns referenced instrument identity.
    pub const fn reference_instrument_id(&self) -> InstrumentId {
        self.reference_instrument_id
    }

    /// Returns instrument relationship.
    pub const fn relationship(&self) -> InputInstrumentRelation {
        self.relationship
    }

    /// Returns exact input amount.
    pub const fn amount(&self) -> ValuationAmount {
        self.amount
    }

    /// Returns input significance.
    pub const fn significance(&self) -> InputSignificance {
        self.significance
    }

    /// Returns accounting observability.
    pub const fn observability(&self) -> InputObservability {
        self.observability
    }

    /// Returns source-input adjustment.
    pub const fn adjustment(&self) -> PriceAdjustment {
        self.adjustment
    }

    /// Returns market activity conclusion.
    pub const fn market_activity(&self) -> MarketActivity {
        self.market_activity
    }

    /// Returns market access conclusion.
    pub const fn market_access(&self) -> MarketAccess {
        self.market_access
    }

    /// Returns independent data-quality classification.
    pub const fn data_quality(&self) -> DataQuality {
        self.data_quality
    }

    /// Returns immutable source evidence.
    pub const fn evidence(&self) -> &FairValueEvidence {
        &self.evidence
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Untrusted fields used to construct an immutable measurement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationMeasurementSpec {
    /// Instrument being measured.
    pub instrument_id: InstrumentId,
    /// Resulting exact amount and accounting scale.
    pub amount: ValuationAmount,
    /// Fair-value measurement instant.
    pub measurement_at: Timestamp,
    /// Preparation completion instant.
    pub prepared_at: Timestamp,
    /// Preparer audit identity.
    pub prepared_by: ActorId,
    /// Valuation technique.
    pub method: ValuationMethod,
    /// Complete source inputs.
    pub inputs: Vec<ValuationInput>,
}

/// Immutable fair-value measurement awaiting deterministic classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationMeasurement {
    id: MeasurementId,
    evidence_hash: FairValueEvidenceHash,
    instrument_id: InstrumentId,
    amount: ValuationAmount,
    measurement_at: Timestamp,
    prepared_at: Timestamp,
    prepared_by: ActorId,
    method: ValuationMethod,
    inputs: Arc<[ValuationInput]>,
    retained_bytes: usize,
}

impl ValuationMeasurement {
    /// Validates input completeness, canonicalizes order, and derives all content identities.
    ///
    /// # Errors
    ///
    /// Rejects empty/excessive input sets, duplicate inputs, mismatched subjects, evidence not
    /// ingested by preparation time, or preparation before the measurement instant.
    pub fn try_new(mut spec: ValuationMeasurementSpec) -> Result<Self, FairValueError> {
        if spec.inputs.is_empty() || spec.inputs.len() > HARD_MAX_MEASUREMENT_INPUTS {
            return Err(FairValueError::InvalidMeasurement);
        }
        if spec.prepared_at < spec.measurement_at {
            return Err(FairValueError::InvalidTime);
        }
        if spec.inputs.iter().any(|input| {
            input.subject_instrument_id() != spec.instrument_id
                || input.evidence().ingested_at() > spec.prepared_at
        }) {
            return Err(FairValueError::InvalidMeasurement);
        }
        spec.inputs.sort_by_key(ValuationInput::id);
        if spec
            .inputs
            .windows(2)
            .any(|pair| pair[0].id() == pair[1].id())
        {
            return Err(FairValueError::DuplicateInput);
        }

        let mut evidence_hash = CanonicalHasher::new(b"market-squawk/valuation-evidence-set/v1");
        evidence_hash
            .u64(u64::try_from(spec.inputs.len()).map_err(|_| FairValueError::Arithmetic)?);
        for input in &spec.inputs {
            evidence_hash.fixed(input.evidence().hash().bytes());
        }
        let evidence_hash = FairValueEvidenceHash(evidence_hash.finish());

        let mut hash = CanonicalHasher::new(b"market-squawk/valuation-measurement/v1");
        hash.bytes(spec.instrument_id.as_uuid().as_bytes());
        spec.amount.hash_into(&mut hash);
        hash.i64(spec.measurement_at.unix_nanos());
        hash.i64(spec.prepared_at.unix_nanos());
        hash.bytes(spec.prepared_by.as_str().as_bytes());
        hash.u8(method_tag(spec.method));
        hash.fixed(evidence_hash.bytes());
        hash.u64(u64::try_from(spec.inputs.len()).map_err(|_| FairValueError::Arithmetic)?);
        for input in &spec.inputs {
            hash.fixed(input.id().bytes());
        }

        let dynamic = spec.inputs.iter().try_fold(
            checked_add(
                spec.prepared_by.retained_bytes(),
                size_of::<ValuationInput>() * spec.inputs.len(),
            )?,
            |total, input| checked_add(total, input.retained_bytes()),
        )?;
        let retained_bytes = checked_add(size_of::<Self>(), dynamic)?;
        Ok(Self {
            id: MeasurementId(hash.finish()),
            evidence_hash,
            instrument_id: spec.instrument_id,
            amount: spec.amount,
            measurement_at: spec.measurement_at,
            prepared_at: spec.prepared_at,
            prepared_by: spec.prepared_by,
            method: spec.method,
            inputs: Arc::from(spec.inputs.into_boxed_slice()),
            retained_bytes,
        })
    }

    /// Returns immutable measurement identity.
    pub const fn id(&self) -> MeasurementId {
        self.id
    }

    /// Returns combined immutable source-evidence identity.
    pub const fn evidence_hash(&self) -> FairValueEvidenceHash {
        self.evidence_hash
    }

    /// Returns measured instrument.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns exact measurement amount.
    pub const fn amount(&self) -> ValuationAmount {
        self.amount
    }

    /// Returns measurement instant.
    pub const fn measurement_at(&self) -> Timestamp {
        self.measurement_at
    }

    /// Returns preparation completion time.
    pub const fn prepared_at(&self) -> Timestamp {
        self.prepared_at
    }

    /// Returns preparer audit identity.
    pub const fn prepared_by(&self) -> &ActorId {
        &self.prepared_by
    }

    /// Returns valuation technique.
    pub const fn method(&self) -> ValuationMethod {
        self.method
    }

    /// Returns inputs in immutable content-identity order.
    pub fn inputs(&self) -> &[ValuationInput] {
        &self.inputs
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

pub(crate) const fn relation_tag(value: InputInstrumentRelation) -> u8 {
    match value {
        InputInstrumentRelation::Identical => 1,
        InputInstrumentRelation::Similar => 2,
        InputInstrumentRelation::Proxy => 3,
    }
}

pub(crate) const fn observability_tag(value: InputObservability) -> u8 {
    match value {
        InputObservability::QuotedPrice => 1,
        InputObservability::Observable => 2,
        InputObservability::Unobservable => 3,
    }
}

pub(crate) const fn significance_tag(value: InputSignificance) -> u8 {
    match value {
        InputSignificance::Significant => 1,
        InputSignificance::NotSignificant => 2,
    }
}

pub(crate) const fn adjustment_tag(value: PriceAdjustment) -> u8 {
    match value {
        PriceAdjustment::None => 1,
        PriceAdjustment::Observable => 2,
        PriceAdjustment::Unobservable => 3,
    }
}

pub(crate) const fn activity_tag(value: MarketActivity) -> u8 {
    match value {
        MarketActivity::Active => 1,
        MarketActivity::Inactive => 2,
        MarketActivity::NotAssessed => 3,
    }
}

pub(crate) const fn access_tag(value: MarketAccess) -> u8 {
    match value {
        MarketAccess::Accessible => 1,
        MarketAccess::Inaccessible => 2,
        MarketAccess::NotAssessed => 3,
    }
}

pub(crate) const fn method_tag(value: ValuationMethod) -> u8 {
    match value {
        ValuationMethod::QuotedMarketPrice => 1,
        ValuationMethod::MarketApproach => 2,
        ValuationMethod::IncomeApproach => 3,
        ValuationMethod::CostApproach => 4,
    }
}

pub(crate) const fn quality_tag(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}
