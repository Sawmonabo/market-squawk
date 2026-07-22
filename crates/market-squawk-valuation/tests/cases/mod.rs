mod classification;
mod workflow;

use market_squawk_domain::{
    Currency, DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, Money, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_valuation::{
    ActorId, EvidenceOrigin, EvidenceVerification, FairValueEvidence, FairValueEvidenceInput,
    FairValueLimitInput, FairValueLimits, FairValueService, InputInstrumentRelation,
    InputObservability, InputSignificance, MarketAccess, MarketActivity, PriceAdjustment,
    ValuationAmount, ValuationInput, ValuationInputSpec, ValuationMeasurement,
    ValuationMeasurementSpec, ValuationMethod,
};
use rust_decimal::Decimal;

#[derive(Clone, Copy)]
struct Scenario {
    relation: InputInstrumentRelation,
    observability: InputObservability,
    adjustment: PriceAdjustment,
    activity: MarketActivity,
    access: MarketAccess,
    quality: DataQuality,
    verification: EvidenceVerification,
    source_timestamp: i64,
    input_currency: &'static str,
    input_scale: u8,
    input_amount: i64,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            relation: InputInstrumentRelation::Identical,
            observability: InputObservability::QuotedPrice,
            adjustment: PriceAdjustment::None,
            activity: MarketActivity::Active,
            access: MarketAccess::Accessible,
            quality: DataQuality::DirectUnverified,
            verification: EvidenceVerification::Verified,
            source_timestamp: 950,
            input_currency: "USD",
            input_scale: 2,
            input_amount: 100,
        }
    }
}

fn instrument() -> InstrumentId {
    "9f3914d3-9ef4-42f7-a707-3f2dcde861d1"
        .parse()
        .expect("static non-nil instrument UUID")
}

fn other_instrument() -> InstrumentId {
    "d2505fc4-c26f-45ce-a596-5b2d0769b3fe"
        .parse()
        .expect("static non-nil instrument UUID")
}

fn actor(value: &str) -> ActorId {
    ActorId::try_from(value).expect("static actor identity")
}

fn amount(value: i64, currency: &str, scale: u8) -> ValuationAmount {
    ValuationAmount::try_new(
        Money::new(
            Decimal::from(value),
            Currency::try_from(currency).expect("static currency"),
        ),
        scale,
    )
    .expect("valid valuation amount")
}

fn input(scenario: Scenario, payload_byte: u8, significance: InputSignificance) -> ValuationInput {
    let reference = if scenario.relation == InputInstrumentRelation::Identical {
        instrument()
    } else {
        other_instrument()
    };
    let source_timestamp = Timestamp::from_unix_nanos(scenario.source_timestamp);
    let evidence = FairValueEvidence::try_new(FairValueEvidenceInput {
        source_id: SourceId::try_from("test.market").expect("static source"),
        source_identifier: SourceIdentifier::try_from("quote-1").expect("static record"),
        payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [payload_byte; 32]),
        origin: EvidenceOrigin::Market {
            venue_id: VenueId::try_from("XNYS").expect("static venue"),
        },
        source_timestamp,
        available_at: Timestamp::from_unix_nanos(scenario.source_timestamp + 10),
        ingested_at: Timestamp::from_unix_nanos(scenario.source_timestamp + 20),
        verification: scenario.verification,
    })
    .expect("valid evidence");

    ValuationInput::try_new(ValuationInputSpec {
        subject_instrument_id: instrument(),
        reference_instrument_id: reference,
        relationship: scenario.relation,
        amount: amount(
            scenario.input_amount,
            scenario.input_currency,
            scenario.input_scale,
        ),
        significance,
        observability: scenario.observability,
        adjustment: scenario.adjustment,
        market_activity: scenario.activity,
        market_access: scenario.access,
        data_quality: scenario.quality,
        evidence,
    })
    .expect("valid valuation input")
}

fn measurement(inputs: Vec<ValuationInput>) -> ValuationMeasurement {
    ValuationMeasurement::try_new(ValuationMeasurementSpec {
        instrument_id: instrument(),
        amount: amount(100, "USD", 2),
        measurement_at: Timestamp::from_unix_nanos(1_000),
        prepared_at: Timestamp::from_unix_nanos(1_100),
        prepared_by: actor("preparer-1"),
        method: ValuationMethod::QuotedMarketPrice,
        inputs,
    })
    .expect("valid measurement")
}

fn service(max_query_results: usize) -> FairValueService {
    let limits = FairValueLimits::try_new(FairValueLimitInput {
        max_measurements: 64,
        max_inputs_per_measurement: 16,
        max_records_per_family: 128,
        max_query_results,
        max_retained_bytes: 2 * 1024 * 1024,
    })
    .expect("valid service limits");
    FairValueService::new(limits)
}
