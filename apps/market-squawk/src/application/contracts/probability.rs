//! Closed inert event-plan coordinates. Admission never substitutes for original source reopening.
use market_squawk_data::ProbabilityEventTarget;
use market_squawk_domain::Timestamp;
use market_squawk_services::ToolInputError;
use serde_json::{Map, Value, json};
use super::{ArgumentKind, admit_argument as common, argument_schema};

#[derive(Clone, Copy)]
pub(super) enum Argument { Plan, ForecastSelection, CanonicalId }
fn closed(fields: Vec<(&str, Value)>) -> Value {
    let required: Vec<_> = fields.iter().map(|(key, _)| *key).collect();
    let properties: Map<_, _> = fields.iter().map(|(key, value)| ((*key).to_owned(), value.clone())).collect();
    json!({"type":"object","additionalProperties":false,"properties":properties,"required":required})
}
fn nullable(value: Value) -> Value { json!({"oneOf":[{"type":"null"},value]}) }
fn integer(minimum: i64, maximum: i64) -> Value { json!({"type":"integer","minimum":minimum,"maximum":maximum}) }
fn unsigned() -> Value { json!({"type":"integer","minimum":0,"maximum":u64::MAX}) }
fn fixed(value: Value, n: usize) -> Value { json!({"type":"array","minItems":n,"maxItems":n,"items":value}) }
fn bytes() -> Value { fixed(integer(0,255),32) }
fn time() -> Value { integer(i64::MIN,i64::MAX) }
fn enumeration(values: &[&str]) -> Value { json!({"type":"string","enum":values}) }
fn id() -> Value {
    // The supported UUID format already enforces canonical spelling in output validation.
    json!({"type":"string","format":"uuid",
        "not":{"type":"string","const":"00000000-0000-0000-0000-000000000000"}})
}
fn selection_token() -> Value {
    // Existing product tokens use the common opaque scalar. Exact prefix, original identity and
    // Find membership remain checked by request admission and the source-owned reopener.
    let mut schema = argument_schema(ArgumentKind::OpaqueProductToken);
    schema["minLength"] = json!(39);
    schema["maxLength"] = json!(39);
    schema
}
fn event_kind() -> Value { enumeration(&["price_higher","benchmark_outperformance","profit_after_costs"]) }
fn cost_policy() -> Value {
    closed(vec![
        ("version",json!({"type":"integer","const":1})),("execution_policy_version",json!({"type":"integer","const":3})),
        ("fee_basis_points",integer(0,10_000)),("slippage_basis_points",integer(0,10_000)),
        ("maximum_random_slippage_basis_points",integer(0,10_000)),("maximum_participation_basis_points",integer(1,10_000)),
        ("latency_nanos",integer(1,i64::MAX)),("allow_partial_fills",json!({"type":"boolean"})),
        ("fee_decimal_scale",integer(0,28)),("reporting_currency",json!({"type":"string","pattern":"^[A-Z]{3}$"})),
        ("quantity_lots",integer(1,i64::MAX)),("maximum_entry_lag_nanos",integer(1,i64::MAX)),
        ("maximum_exit_lag_nanos",integer(1,i64::MAX)),("seed",unsigned()),
        ("execution_basis",enumeration(&["observed_quote_depth","completed_daily_bar"])),
        ("daily_bar_assumed_spread_basis_points",nullable(integer(0,10_000))),
        ("liquidity_priority",json!({"type":"string","const":"signal_time_then_order_id"})),
        ("convention",json!({"type":"string","const":"long_round_trip_total_wealth_including_entitlements"})),
    ])
}
/// Actual data-owned enum serialization, distinct from the human-readable forecast target DTO.
fn event() -> Value {
    json!({"oneOf":[
        closed(vec![("kind",json!({"type":"string","const":"price_higher"}))]),
        closed(vec![("kind",json!({"type":"string","const":"benchmark_outperformance"})),("benchmark_instrument_id",id()),
            ("benchmark_definition",closed(vec![("algorithm",enumeration(&["sha256","blake3"])),("bytes",bytes())]))]),
        closed(vec![("kind",json!({"type":"string","const":"profit_after_costs"})),("policy",cost_policy())])
    ]})
}
fn request() -> Value {
    closed(vec![
        ("instrumentId",id()),("sourceCutoffUnixNanos",argument_schema(ArgumentKind::UnixNanosText)),
        ("financialProfile",argument_schema(ArgumentKind::AnalyticalProfileResolution)),("eventKind",event_kind()),
        ("benchmarkInstrumentId",nullable(id())),("sourceActionReference",argument_schema(ArgumentKind::SourceActionReference)),
        ("findMember",nullable(argument_schema(ArgumentKind::FindMemberContext))),
        ("selectionToken",nullable(selection_token())),
    ])
}
fn plan() -> Value {
    closed(vec![("version",json!({"type":"integer","const":1})),("request",request()),("event",event()),
        ("sourceSelections",json!({"type":"array","minItems":1,"maxItems":2,"items":bytes()})),
        ("populationStart",time()),("populationEnd",time()),("splitEnds",fixed(time(),3)),("digest",bytes())])
}
pub(super) fn schema(argument: Argument) -> Value {
    match argument {
        Argument::Plan => plan(),
        Argument::CanonicalId => id(),
        Argument::ForecastSelection => closed(vec![("event",event()),("modelToken",id()),
            ("analysisManifest",super::output::forecast_current_feature_input()["properties"]["manifest"].clone())]),
    }
}
pub(super) fn preparation_output() -> Value {
    let count = unsigned;
    let coverage = closed(vec![
        ("originalOrigins",count()),("outsidePartitions",count()),("missingSubjectTerminal",count()),
        ("missingBenchmark",count()),("unavailableCostOutcome",count()),("boundaryCensored",count()),
        ("retainedExamples",count()),("splitCounts",fixed(count(),3)),("completeClasses",fixed(fixed(count(),2),3)),
        ("missingFeatures",count()),("featureCount",count()),
    ]);
    let availability = json!({"oneOf":[
        closed(vec![("state",json!({"type":"string","const":"ready"})),("reason",json!({"type":"null"}))]),
        closed(vec![("state",json!({"type":"string","const":"unavailable"})),("reason",enumeration(&[
            "source_clock_mismatch","source_evidence_unavailable","subject_inputs_unavailable",
            "current_features_unavailable","insufficient_event_evidence","insufficient_calibration"]))])
    ]});
    closed(vec![("eventKind",event_kind()),("availability",availability),("plan",nullable(plan())),
        ("event",nullable(event())),("currentFeatureInput",nullable(super::output::forecast_current_feature_input())),
        ("instrumentId",id()),("sourceCutoffUnixNanos",argument_schema(ArgumentKind::UnixNanosText)),
        ("financialProfileDigest",argument_schema(ArgumentKind::Sha256)),("coverage",nullable(coverage))])
}
fn object<'a>(value: &'a Value, fields: &[&str]) -> Result<&'a Map<String, Value>, ToolInputError> {
    value.as_object().filter(|v|v.len()==fields.len() && fields.iter().all(|key|v.contains_key(*key))).ok_or(ToolInputError::Invalid)
}
fn parse<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, ToolInputError> {
    serde_json::from_value(value.clone()).map_err(|_|ToolInputError::Invalid)
}
fn event_value(value: &Value) -> Result<ProbabilityEventTarget, ToolInputError> {
    let event: ProbabilityEventTarget = parse(value)?;
    event.validate().map_err(|_|ToolInputError::Invalid)?;
    Ok(event)
}
fn optional(value: &Value, kind: ArgumentKind) -> Result<(),ToolInputError> {
    if value.is_null() { Ok(()) } else { common(value,kind) }
}
pub(super) fn admit_argument(value: &Value, argument: Argument) -> Result<(), ToolInputError> {
    match argument {
        Argument::CanonicalId => common(value, ArgumentKind::ActionToken)?,
        Argument::ForecastSelection => {
            let v=object(value,&["event","modelToken","analysisManifest"])?;
            event_value(&v["event"])?; common(&v["modelToken"],ArgumentKind::ActionToken)?;
            parse::<market_squawk_modeling::ForecastArtifactManifestRecord>(&v["analysisManifest"])?
                .typed().map_err(|_|ToolInputError::Invalid)?;
        }
        Argument::Plan => {
            let v=object(value,&["version","request","event","sourceSelections","populationStart","populationEnd","splitEnds","digest"])?;
            if v["version"].as_u64()!=Some(1) || parse::<[u8;32]>(&v["digest"])?==[0;32] { return Err(ToolInputError::Invalid); }
            let event=event_value(&v["event"])?;
            let sources:Vec<[u8;32]>=parse(&v["sourceSelections"])?;
            let expected=if matches!(event,ProbabilityEventTarget::BenchmarkOutperformance{..}) {2}else{1};
            if sources.len()!=expected || sources.contains(&[0;32]) { return Err(ToolInputError::Invalid); }
            let _:Timestamp=parse(&v["populationStart"])?;let _:Timestamp=parse(&v["populationEnd"])?;
            let _:[Timestamp;3]=parse(&v["splitEnds"])?;
            let r=object(&v["request"],&["instrumentId","sourceCutoffUnixNanos","financialProfile","eventKind","benchmarkInstrumentId","sourceActionReference","findMember","selectionToken"])?;
            for (key,kind) in [("instrumentId",ArgumentKind::ActionToken),("sourceCutoffUnixNanos",ArgumentKind::UnixNanosText),
                ("financialProfile",ArgumentKind::AnalyticalProfileResolution),("eventKind",ArgumentKind::Enumeration(&["price_higher","benchmark_outperformance","profit_after_costs"])),
                ("sourceActionReference",ArgumentKind::SourceActionReference)] {common(&r[key],kind)?;}
            optional(&r["benchmarkInstrumentId"],ArgumentKind::ActionToken)?;
            optional(&r["findMember"],ArgumentKind::FindMemberContext)?;
            optional(&r["selectionToken"],ArgumentKind::MarketSelectionToken)?;
            if r["findMember"].is_null()!=r["selectionToken"].is_null() {return Err(ToolInputError::Invalid);}
        }
    }
    Ok(())
}
pub(super) fn admit_request(operation: &str, args: &Map<String,Value>) -> Result<(),ToolInputError> {
    let valid=match operation {
        "Analysis.PrepareProbabilityEvent" => args.contains_key("findMember")==args.contains_key("selectionToken"),
        "Analysis.StartProbabilityDataset" => (args.get("part").and_then(Value::as_str)==Some("subject_inputs")) != args.contains_key("subjectDatasetJob"),
        "Model.PrepareInvestmentForecast" => !args.contains_key("probabilitySelection") || args.contains_key("currentFeatureInput"),
        _=>true,
    };
    if valid {Ok(())}else{Err(ToolInputError::Invalid)}
}
pub(super) fn selector_schema(operation: &str) -> Option<Value> {
    match operation {
        "Analysis.PrepareProbabilityEvent" => Some(json!([
            {"required":["findMember","selectionToken"]},
            {"not":{"anyOf":[{"required":["findMember"]},{"required":["selectionToken"]}]}}
        ])),
        "Analysis.StartProbabilityDataset" => Some(json!([
            {"properties":{"part":{"type":"string","const":"subject_inputs"}},"not":{"required":["subjectDatasetJob"]}},
            {"properties":{"part":{"enum":["training","analysis"]}},"required":["subjectDatasetJob"]}
        ])),
        "Model.PrepareInvestmentForecast" => Some(json!([
            {"not":{"required":["probabilitySelection"]}},
            {"required":["probabilitySelection","currentFeatureInput"]}
        ])),
        _=>None,
    }
}
