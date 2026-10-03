//! Three independent, source-owned event training and forecast lifecycles.
use super::*;
const KINDS: [&str; 3] = [
    "price_higher",
    "benchmark_outperformance",
    "profit_after_costs",
];
pub(super) fn is_step(step: Step) -> bool {
    matches!(
        step,
        Step::ProbabilityPlan
            | Step::ProbabilitySubject
            | Step::ProbabilityPrepared
            | Step::ProbabilityTrainingDataset
            | Step::ProbabilityAnalysisDataset
            | Step::ProbabilityTraining
            | Step::PrepareProbabilityForecast
            | Step::ProbabilityForecast
    )
}
pub(super) fn valid(work: &DriverState) -> bool {
    if work.probability_index > 3 || (is_step(work.step) && work.probability_index == 3) {
        return false;
    }
    work.receipts.iter().all(|(key, receipt)| {
        if receipt.operation != "Analysis.PrepareProbabilityEvent" {
            return true;
        }
        let Some(index) = key
            .rsplit('-')
            .next()
            .and_then(|value| value.parse::<usize>().ok())
        else {
            return false;
        };
        KINDS
            .get(index)
            .is_some_and(|kind| preparation(receipt, kind).is_ok())
    })
}
fn preparation(receipt: &Receipt, kind: &str) -> Result<bool, WorkflowError> {
    let body = &receipt.body;
    if body.as_object().is_none_or(|value| value.len() != 9)
        || body.get("eventKind").and_then(Value::as_str) != Some(kind)
        || receipt.arguments.get("eventKind") != body.get("eventKind")
        || body.get("instrumentId") != receipt.arguments.get("instrumentId")
        || body.get("sourceCutoffUnixNanos") != receipt.arguments.get("sourceCutoffUnixNanos")
        || body.get("financialProfileDigest")
            != receipt
                .arguments
                .get("financialProfile")
                .and_then(|profile| profile.get("configurationDigest"))
        || !body
            .get("financialProfileDigest")
            .and_then(Value::as_str)
            .is_some_and(valid_digest)
    {
        return Err(WorkflowError::internal());
    }
    match body.pointer("/availability/state").and_then(Value::as_str) {
        Some("ready")
            if body.pointer("/availability/reason") == Some(&Value::Null)
                && body.get("plan").is_some_and(Value::is_object)
                && body.get("event").is_some_and(Value::is_object) =>
        {
            let event: market_squawk_data::ProbabilityEventTarget =
                serde_json::from_value(body["event"].clone())
                    .map_err(|_| WorkflowError::internal())?;
            event.validate().map_err(|_| WorkflowError::internal())?;
            if body.pointer("/event/kind").and_then(Value::as_str) != Some(kind)
                || body
                    .get("currentFeatureInput")
                    .is_none_or(|value| !value.is_null() && !value.is_object())
                || receipt.arguments.get("subjectDatasetJob").is_some()
                    && !body
                        .get("currentFeatureInput")
                        .is_some_and(Value::is_object)
            {
                return Err(WorkflowError::internal());
            }
            Ok(true)
        }
        Some("unavailable")
            if body
                .pointer("/availability/reason")
                .and_then(Value::as_str)
                .is_some_and(|reason| super::super::valid_identifier(reason, 128)) =>
        {
            Ok(false)
        }
        _ => Err(WorkflowError::internal()),
    }
}
pub(super) fn next_invocation(
    run: &WorkflowRun,
) -> Result<(&'static str, Value, bool), WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    let kind = KINDS
        .get(work.probability_index)
        .ok_or_else(WorkflowError::internal)?;
    Ok(match work.step {
        Step::ProbabilityPlan | Step::ProbabilityPrepared => {
            let mut input = work.input(run)?;
            input["eventKind"] = json!(kind);
            if let Some(benchmark) = work.benchmark_instrument_id {
                input["benchmarkInstrumentId"] = json!(benchmark);
            }
            input["sourceActionReference"] = work.initial_source_action_reference()?.clone();
            if work.find.is_some() {
                input["findMember"] = serde_json::to_value(current_find_member(work)?)
                    .map_err(|_| WorkflowError::internal())?;
                input["selectionToken"] = json!(work.selection_token);
            }
            input["sourceCutoffUnixNanos"] = json!(work.price_forecast_cutoff()?);
            if work.step == Step::ProbabilityPrepared {
                input["subjectDatasetJob"] = Value::Object(job_arguments(&job_from_receipt(
                    work.receipt(Step::ProbabilitySubject)?,
                )?)?);
            }
            ("Analysis.PrepareProbabilityEvent", input, false)
        }
        Step::ProbabilitySubject
        | Step::ProbabilityTrainingDataset
        | Step::ProbabilityAnalysisDataset => {
            let step = if work.step == Step::ProbabilitySubject {
                Step::ProbabilityPlan
            } else {
                Step::ProbabilityPrepared
            };
            let mut input = json!({"plan":work.receipt(step)?.body.get("plan").ok_or_else(WorkflowError::internal)?,
                "part":match work.step { Step::ProbabilitySubject=>"subject_inputs", Step::ProbabilityTrainingDataset=>"training", _=>"analysis" }});
            if work.step != Step::ProbabilitySubject {
                input["subjectDatasetJob"] = Value::Object(job_arguments(&job_from_receipt(
                    work.receipt(Step::ProbabilitySubject)?,
                )?)?);
            }
            ("Analysis.StartProbabilityDataset", input, true)
        }
        Step::ProbabilityTraining => {
            let job = job_from_receipt(work.receipt(Step::ProbabilityTrainingDataset)?)?;
            (
                "Model.StartPreparedTraining",
                json!({"datasetJobId":job.job_id,"datasetJobGeneration":generation_number(&job)?,"financialProfile":DriverState::profile(run)?}),
                true,
            )
        }
        Step::PrepareProbabilityForecast => {
            let prepared = work.receipt(Step::ProbabilityPrepared)?;
            let mut input = work.input(run)?;
            input["sourceCutoffUnixNanos"] = json!(work.price_forecast_cutoff()?);
            input["probabilitySelection"] = json!({"event":prepared.body.get("event").ok_or_else(WorkflowError::internal)?,
                "modelToken":work.receipt(Step::ProbabilityTraining)?.body.pointer("/model/modelToken").ok_or_else(WorkflowError::internal)?,
                "analysisManifest":analysis_manifest(work.receipt(Step::ProbabilityAnalysisDataset)?)?});
            input["currentFeatureInput"] = prepared
                .body
                .get("currentFeatureInput")
                .filter(|value| value.is_object())
                .cloned()
                .ok_or_else(WorkflowError::internal)?;
            if let Some(find) = &work.find {
                input["forecastCohort"] = find
                    .preparation
                    .get("forecastCohort")
                    .filter(|value| value.is_object())
                    .cloned()
                    .ok_or_else(WorkflowError::internal)?;
                input["currentFeatureInput"] = find
                    .candidates
                    .get(find.candidate_index)
                    .and_then(|candidate| candidate.get("currentFeatureInput"))
                    .filter(|value| value.is_object())
                    .cloned()
                    .ok_or_else(WorkflowError::internal)?;
            }
            ("Model.PrepareInvestmentForecast", input, false)
        }
        Step::ProbabilityForecast => (
            "Model.StartPreparedForecast",
            json!({"confirmationToken":work.receipt(Step::PrepareProbabilityForecast)?.body.pointer("/forecast/confirmationToken").and_then(Value::as_str).ok_or_else(WorkflowError::internal)?}),
            true,
        ),
        _ => return Err(WorkflowError::internal()),
    })
}
pub(super) fn apply(
    work: &mut DriverState,
    receipt: &Receipt,
    profile: &Option<financial_profiles::FinancialResolution>,
    now: &str,
) -> Result<(), WorkflowError> {
    let expected = serde_json::to_value(profile.as_ref().ok_or_else(WorkflowError::internal)?)
        .map_err(|_| WorkflowError::internal())?;
    match work.step {
        Step::ProbabilityPlan | Step::ProbabilityPrepared => {
            if receipt
                .body
                .get("instrumentId")
                .and_then(Value::as_str)
                .and_then(|value| value.parse().ok())
                != work.instrument_id
                || receipt.body.get("financialProfileDigest") != expected.get("configurationDigest")
                || receipt
                    .body
                    .get("sourceCutoffUnixNanos")
                    .and_then(Value::as_str)
                    != Some(work.price_forecast_cutoff()?)
            {
                return Err(WorkflowError::internal());
            }
            work.step = if !preparation(receipt, KINDS[work.probability_index])? {
                Step::ProbabilityPlan
            } else if work.step == Step::ProbabilityPlan {
                Step::ProbabilitySubject
            } else {
                Step::ProbabilityTrainingDataset
            };
        }
        Step::ProbabilitySubject => work.step = Step::ProbabilityPrepared,
        Step::ProbabilityTrainingDataset => work.step = Step::ProbabilityAnalysisDataset,
        Step::ProbabilityAnalysisDataset => work.step = Step::ProbabilityTraining,
        Step::ProbabilityTraining => work.step = Step::PrepareProbabilityForecast,
        Step::PrepareProbabilityForecast => {
            let prepared = price_preparation(receipt)?;
            if Some(prepared.instrument_id) != work.instrument_id
                || prepared.source_cutoff_unix_nanos != work.price_forecast_cutoff()?
                || receipt.body.get("financialProfileDigest") != expected.get("configurationDigest")
            {
                return Err(WorkflowError::internal());
            }
            if matches!(
                prepared.availability,
                PriceForecastAvailability::Unavailable { .. }
            ) {
                work.step = Step::ProbabilityPlan;
            } else {
                if receipt.body.pointer("/forecast/model/modelToken")
                    != work
                        .receipt(Step::ProbabilityTraining)?
                        .body
                        .pointer("/model/modelToken")
                    || receipt
                        .body
                        .pointer("/forecast/expiresAtUnixNanos")
                        .and_then(Value::as_str)
                        .and_then(|value| value.parse::<u64>().ok())
                        .is_none_or(|expires| {
                            now.parse::<u64>().ok().is_none_or(|value| value >= expires)
                        })
                {
                    return Err(WorkflowError::internal());
                }
                work.step = Step::ProbabilityForecast;
            }
        }
        Step::ProbabilityForecast => {
            if receipt.body.get("financialProfileDigest") != expected.get("configurationDigest")
                || receipt.body.get("requestSha256")
                    != work
                        .receipt(Step::PrepareProbabilityForecast)?
                        .body
                        .get("requestSha256")
            {
                return Err(WorkflowError::internal());
            }
            exact_forecast_reference(receipt)?;
            work.step = Step::ProbabilityPlan;
        }
        _ => return Err(WorkflowError::internal()),
    }
    Ok(())
}
pub(super) fn after_receipt(work: &mut DriverState, step: Step) -> Result<(), WorkflowError> {
    if !is_step(step) || work.step != Step::ProbabilityPlan {
        return Ok(());
    }
    // A completed forecast reopens its original producers. Keep its exact reference, or retain
    // the original source-assessed absence. Do not accumulate redundant dataset response bodies.
    let keep = work.key(step);
    let suffix = format!("-{}", work.probability_index);
    work.receipts.retain(|key, _| {
        !key.ends_with(&suffix)
            || !(key.starts_with("Probability") || key.starts_with("PrepareProbability"))
            || key == &keep
    });
    work.probability_index += 1;
    if work.probability_index == KINDS.len() {
        work.step = Step::FiscalPlan;
    }
    Ok(())
}
pub(super) fn publication(work: &DriverState) -> Result<Value, WorkflowError> {
    let mut result = Map::new();
    for (index, key) in ["priceHigher", "benchmarkOutperformance", "profitAfterCosts"]
        .into_iter()
        .enumerate()
    {
        result.insert(
            key.to_owned(),
            work.receipts
                .get(&format!("ProbabilityForecast-{index}"))
                .map(exact_forecast_reference)
                .transpose()?
                .unwrap_or(Value::Null),
        );
    }
    Ok(Value::Object(result))
}

fn analysis_manifest(receipt: &Receipt) -> Result<Value, WorkflowError> {
    let wire = receipt
        .body
        .pointer("/dataset/manifest")
        .ok_or_else(WorkflowError::internal)?;
    let field = |name| wire.get(name).cloned().ok_or_else(WorkflowError::internal);
    Ok(
        json!({"dataset":field("dataset")?,"manifestVersion":field("version")?,
        "schema":{"name":field("schema")?,"version":field("schemaVersion")?,"fingerprint":field("schemaFingerprintSha256")?},
        "contentHash":field("contentSha256")?}),
    )
}
