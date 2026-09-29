//! Closed reference admission for the existing original-source historical study owners.
//! Shape checks never replace source, completed-job or physical artifact reopening.
use std::collections::HashSet;

use market_squawk_services::{ArtifactReference, ToolInputError};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use super::{ArgumentKind, admit_argument as admit_common, output::historical_study as wire};
use crate::application::{
    HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES, HISTORICAL_FISCAL_MAXIMUM_PAGES,
    HISTORICAL_FISCAL_PAGE_SIZE, HistoricalFiscalJobReference, HistoricalFiscalPageReference,
    analysis::HistoricalStudyPlanReferenceV1, fiscal_projection_targets,
    research::HistoricalFiscalRecipeReference,
};

#[derive(Clone, Copy)]
pub(super) enum Argument {
    Plan,
    Job,
    PriceExample,
    PageOrdinal,
    FiscalOrigin,
    CompletedFolds,
    FiscalJobs,
    FiscalPage,
    FiscalPages,
}

pub(super) fn schema(argument: Argument) -> Value {
    match argument {
        Argument::Plan => wire::plan_reference(),
        Argument::Job => wire::job_reference(),
        Argument::PriceExample => wire::price_example(),
        Argument::PageOrdinal => wire::page_ordinal(),
        Argument::FiscalOrigin => wire::fiscal_origin(),
        Argument::CompletedFolds => json!({"type":"array","minItems":3,"maxItems":3,
            "items":wire::completed_fold()}),
        Argument::FiscalJobs => {
            json!({"type":"array","maxItems":HISTORICAL_FISCAL_PAGE_SIZE * fiscal_projection_targets().len(),
            "items":wire::completed_target()})
        }
        Argument::FiscalPage => wire::page_reference(),
        Argument::FiscalPages => {
            json!({"type":"array","minItems":1,"maxItems":HISTORICAL_FISCAL_MAXIMUM_PAGES,
            "items":wire::page_reference()})
        }
    }
}

/// Mirror only the existing service's selector branches; no alternate final fiscalJobs wire.
pub(super) fn selector_schema(operation: &str) -> Option<Value> {
    match operation {
        "Analysis.GetHistoricalStudyPlan" => Some(json!([
            {"not":{"anyOf":[{"required":["studyInputJob"]},{"required":["priceExampleId"]},{"required":["pageOrdinal"]},{"required":["fiscalPage"]}]}},
            {"required":["studyInputJob"],"not":{"anyOf":[{"required":["priceExampleId"]},{"required":["fiscalPage"]}]}},
            {"required":["studyInputJob","priceExampleId"],"not":{"anyOf":[{"required":["pageOrdinal"]},{"required":["fiscalPage"]}]}},
            {"required":["studyInputJob","pageOrdinal","fiscalPage"],"not":{"required":["priceExampleId"]}},
        ])),
        "Analysis.StartHistoricalStudyDataset" => Some(json!([
            {"properties":{"part":{"const":"training"}},"required":["foldIndex"],"not":{"required":["fiscal"]}},
            {"properties":{"part":{"const":"studyInputs"}},"not":{"anyOf":[{"required":["foldIndex"]},{"required":["fiscal"]}]}},
            {"required":["fiscal"],"not":{"required":["foldIndex"]}},
        ])),
        "Model.StartHistoricalStudyTraining" => Some(json!([
            {"required":["foldIndex"],"not":{"required":["fiscal"]}},
            {"required":["fiscal"],"not":{"required":["foldIndex"]}},
        ])),
        _ => None,
    }
}

pub(super) fn admit_request(
    operation: &str,
    arguments: &Map<String, Value>,
) -> Result<(), ToolInputError> {
    let valid = match operation {
        "Analysis.GetHistoricalStudyPlan" => {
            let actions: crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference =
                decode(field(arguments, "sourceActionReference")?)?;
            let cutoff = field(arguments, "sourceCutoffUnixNanos")?
                .as_str()
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or(ToolInputError::Invalid)?;
            if actions.knowledge_cutoff().unix_nanos() != cutoff {
                return Err(ToolInputError::Invalid);
            }
            let job = arguments.contains_key("studyInputJob");
            let example = arguments.contains_key("priceExampleId");
            let page = arguments.contains_key("pageOrdinal");
            if let Some(saved) = arguments.get("fiscalPage") {
                job && page && !example && saved.get("pageOrdinal") == arguments.get("pageOrdinal")
            } else {
                (!example && !page || job) && !(example && page)
            }
        }
        "Analysis.StartHistoricalStudyDataset" => {
            let fold = arguments.contains_key("foldIndex");
            let fiscal = arguments.contains_key("fiscal");
            if fiscal {
                !fold
            } else {
                match arguments.get("part").and_then(Value::as_str) {
                    Some("training") => fold,
                    Some("studyInputs") => !fold,
                    _ => false,
                }
            }
        }
        "Model.StartHistoricalStudyTraining" => {
            arguments.contains_key("foldIndex") != arguments.contains_key("fiscal")
        }
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(ToolInputError::Invalid)
    }
}

pub(super) fn admit_argument(value: &Value, argument: Argument) -> Result<(), ToolInputError> {
    match argument {
        Argument::Plan => {
            let plan: HistoricalStudyPlanReferenceV1 = decode(value)?;
            let cutoff = plan.source_cutoff().map_err(|_| ToolInputError::Invalid)?;
            if value.get("version").and_then(Value::as_u64) != Some(1)
                || value["benchmarks"]["version"].as_u64() != Some(1)
                || plan.source_action_reference().knowledge_cutoff() != cutoff
                || decode::<[u8; 32]>(&value["planDigest"])? == [0; 32]
            {
                return Err(ToolInputError::Invalid);
            }
            Ok(())
        }
        Argument::Job => admit_job(value),
        Argument::PriceExample => value
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 256 && s.is_ascii())
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        Argument::PageOrdinal => admit_common(
            value,
            ArgumentKind::Unsigned {
                minimum: 0,
                maximum: (HISTORICAL_FISCAL_MAXIMUM_PAGES - 1) as u64,
            },
        ),
        Argument::FiscalOrigin => {
            let fields = object(value, &["studyInputJob", "priceExampleId", "targetId"])?;
            admit_job(field(fields, "studyInputJob")?)?;
            admit_argument(field(fields, "priceExampleId")?, Argument::PriceExample)?;
            admit_target(field(fields, "targetId")?)
        }
        Argument::CompletedFolds => {
            let folds = array(value, 3, 3)?;
            let mut jobs = HashSet::new();
            for fold in folds {
                let fields = object(fold, &["jobId", "generation", "datasetJob"])?;
                let job = json!({"jobId":field(fields,"jobId")?,"generation":field(fields,"generation")?});
                admit_job(&job)?;
                admit_job(field(fields, "datasetJob")?)?;
                if !jobs.insert((
                    field(fields, "jobId")?.as_str(),
                    field(fields, "generation")?.as_u64(),
                )) {
                    return Err(ToolInputError::Invalid);
                }
            }
            Ok(())
        }
        Argument::FiscalJobs => {
            let targets = array(
                value,
                0,
                HISTORICAL_FISCAL_PAGE_SIZE * fiscal_projection_targets().len(),
            )?;
            let mut keys = HashSet::new();
            for target in targets {
                let fields = object(
                    target,
                    &[
                        "priceExampleId",
                        "targetId",
                        "trainingDatasetJob",
                        "inputDatasetJob",
                        "trainingJob",
                    ],
                )?;
                admit_argument(field(fields, "priceExampleId")?, Argument::PriceExample)?;
                admit_target(field(fields, "targetId")?)?;
                for key in ["trainingDatasetJob", "inputDatasetJob", "trainingJob"] {
                    admit_job(field(fields, key)?)?;
                }
                // Identical to the source completion owner's actual encoded-entry bound.
                if serde_json::to_vec(target)
                    .map_err(|_| ToolInputError::Invalid)?
                    .len()
                    > 393
                    || !keys.insert((
                        field(fields, "priceExampleId")?.as_str(),
                        field(fields, "targetId")?.as_str(),
                    ))
                {
                    return Err(ToolInputError::Invalid);
                }
            }
            Ok(())
        }
        Argument::FiscalPage => admit_page(value).map(|_| ()),
        Argument::FiscalPages => {
            let pages = array(value, 1, HISTORICAL_FISCAL_MAXIMUM_PAGES)?;
            let mut identities = HashSet::new();
            let mut original_binding = None;
            for (ordinal, value) in pages.iter().enumerate() {
                let (binding, count, artifact) = admit_page(value)?;
                if original_binding.is_some_and(|original| original != binding)
                    || value["pageOrdinal"].as_u64() != Some(ordinal as u64)
                    || (ordinal + 1 < pages.len() && count != HISTORICAL_FISCAL_PAGE_SIZE as u64)
                {
                    return Err(ToolInputError::Invalid);
                }
                original_binding = Some(binding);
                if !identities.insert(artifact.sha256().to_owned()) {
                    return Err(ToolInputError::Invalid);
                }
            }
            Ok(())
        }
    }
}

fn admit_page(value: &Value) -> Result<([u8; 32], u64, ArtifactReference), ToolInputError> {
    let _: HistoricalFiscalPageReference = decode(value)?;
    let binding: [u8; 32] = decode(&value["bindingDigest"])?;
    let count = value["originCount"]
        .as_u64()
        .ok_or(ToolInputError::Invalid)?;
    admit_argument(&value["pageOrdinal"], Argument::PageOrdinal)?;
    if binding == [0; 32] || count == 0 || count > HISTORICAL_FISCAL_PAGE_SIZE as u64 {
        return Err(ToolInputError::Invalid);
    }
    let reference: HistoricalFiscalRecipeReference = decode(&value["artifact"])?;
    let artifact = reference.artifact().map_err(|_| ToolInputError::Invalid)?;
    if artifact.byte_count() > HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES {
        return Err(ToolInputError::Invalid);
    }
    Ok((binding, count, artifact))
}

fn admit_job(value: &Value) -> Result<(), ToolInputError> {
    let reference: HistoricalFiscalJobReference = decode(value)?;
    let id = uuid::Uuid::parse_str(&reference.job_id).map_err(|_| ToolInputError::Invalid)?;
    if reference.generation == 0 || id.hyphenated().to_string() != reference.job_id {
        return Err(ToolInputError::Invalid);
    }
    Ok(())
}
fn admit_target(value: &Value) -> Result<(), ToolInputError> {
    let id = value.as_str().ok_or(ToolInputError::Invalid)?;
    if fiscal_projection_targets()
        .iter()
        .any(|target| target.target_id == id)
    {
        Ok(())
    } else {
        Err(ToolInputError::Invalid)
    }
}
fn decode<T: DeserializeOwned>(value: &Value) -> Result<T, ToolInputError> {
    serde_json::from_value(value.clone()).map_err(|_| ToolInputError::Invalid)
}
fn object<'a>(value: &'a Value, names: &[&str]) -> Result<&'a Map<String, Value>, ToolInputError> {
    let fields = value.as_object().ok_or(ToolInputError::Invalid)?;
    if fields.len() != names.len() || names.iter().any(|key| !fields.contains_key(*key)) {
        return Err(ToolInputError::Invalid);
    }
    Ok(fields)
}
fn field<'a>(fields: &'a Map<String, Value>, name: &str) -> Result<&'a Value, ToolInputError> {
    fields.get(name).ok_or(ToolInputError::Invalid)
}
fn array(value: &Value, minimum: usize, maximum: usize) -> Result<&Vec<Value>, ToolInputError> {
    value
        .as_array()
        .filter(|items| items.len() >= minimum && items.len() <= maximum)
        .ok_or(ToolInputError::Invalid)
}
