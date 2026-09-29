//! Closed reference-only input shapes for the sole saved investment generation request.

use market_squawk_services::ToolInputError;
use serde_json::{Map, Value, json};

use crate::application::decision::investment_request::{
    CandidateReference, ForecastReference, GenerateRequest, ProbabilityForecastReferences,
    ProfileBinding, StudyReference, WorkflowBinding, timestamp, validate_canonical_request,
};

use super::{ArgumentKind, argument_schema};

#[derive(Clone, Copy)]
pub(super) enum Argument {
    SourceCutoff,
    ProfileBinding,
    WorkflowBinding,
    OptionalMarket,
    OptionalForecast,
    OptionalSourceAction,
    OptionalFundamentalShareSources,
    ProbabilityForecasts,
    OptionalBenchmarkInstrument,
    FinancialForecasts,
    OptionalStudy,
    OptionalCandidate,
}

pub(super) fn schema(argument: Argument) -> Value {
    match argument {
        Argument::SourceCutoff => json!({
            "type": "string", "minLength": 1, "maxLength": 19,
            "pattern": "^[1-9][0-9]{0,18}$",
        }),
        Argument::ProfileBinding => json!({
            "type": "object", "additionalProperties": false,
            "required": ["profileId", "revision", "contentSha256"],
            "properties": {
                "profileId": canonical_uuid(),
                "revision": argument_schema(ArgumentKind::Unsigned {
                    minimum: 1, maximum: u64::from(u32::MAX),
                }),
                "contentSha256": exact_digest(),
            },
        }),
        Argument::WorkflowBinding => json!({
            "type": "object", "additionalProperties": false,
            "required": ["workflowId", "revision", "contentSha256"],
            "properties": {
                "workflowId": canonical_uuid(),
                "revision": argument_schema(ArgumentKind::Unsigned {
                    minimum: 1, maximum: u64::from(u32::MAX),
                }),
                "contentSha256": exact_digest(),
            },
        }),
        Argument::OptionalMarket => json!({
            "oneOf": [{"type": "null"}, argument_schema(ArgumentKind::MarketInvestmentReference)],
        }),
        Argument::OptionalForecast => json!({
            "oneOf": [{"type": "null"}, forecast_reference()],
        }),
        Argument::OptionalSourceAction => json!({
            "oneOf": [{"type": "null"}, crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference::json_schema()],
        }),
        Argument::OptionalFundamentalShareSources => json!({
            "oneOf": [{"type": "null"}, {
                "type": "string", "minLength": 1,
                "maxLength": crate::application::fair_value::MAX_FUNDAMENTAL_SHARE_SOURCE_BYTES,
                "contentMediaType": "application/json",
            }],
        }),
        Argument::ProbabilityForecasts => json!({
            "type": "object", "additionalProperties": false,
            "required": ["priceHigher", "benchmarkOutperformance", "profitAfterCosts"],
            "properties": {
                "priceHigher": schema(Argument::OptionalForecast),
                "benchmarkOutperformance": schema(Argument::OptionalForecast),
                "profitAfterCosts": schema(Argument::OptionalForecast),
            },
        }),
        Argument::OptionalBenchmarkInstrument => json!({
            "oneOf": [{"type": "null"}, canonical_uuid()],
        }),
        Argument::FinancialForecasts => json!({
            "type": "array", "minItems": 0, "maxItems": 16,
            "items": forecast_reference(),
        }),
        Argument::OptionalStudy => json!({
            "oneOf": [{"type": "null"}, {
                "type": "object", "additionalProperties": false,
                "required": ["requestDigest", "evidenceDigest"],
                "properties": {
                    "requestDigest": exact_digest(),
                    "evidenceDigest": exact_digest(),
                },
            }],
        }),
        Argument::OptionalCandidate => json!({
            "oneOf": [{"type": "null"}, {
                "type": "object", "additionalProperties": false,
                "required": ["candidateId", "screenRunId", "evidenceDigest"],
                "properties": {
                    "candidateId": decision_identifier(),
                    "screenRunId": decision_identifier(),
                    "evidenceDigest": exact_digest(),
                },
            }],
        }),
    }
}

fn forecast_reference() -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["jobId", "generation", "forecastToken", "requestSha256"],
        "properties": {
            "jobId": canonical_uuid(),
            "generation": argument_schema(ArgumentKind::Unsigned {
                minimum: 1, maximum: u64::MAX,
            }),
            "forecastToken": canonical_uuid(),
            "requestSha256": exact_digest(),
        },
    })
}

fn canonical_uuid() -> Value {
    let mut schema = argument_schema(ArgumentKind::ActionToken);
    schema["not"] = json!({"const": "00000000-0000-0000-0000-000000000000"});
    schema
}

fn exact_digest() -> Value {
    let mut schema = argument_schema(ArgumentKind::Sha256);
    schema["not"] = json!({"const": "0".repeat(64)});
    schema
}

fn decision_identifier() -> Value {
    json!({
        "type": "string", "minLength": 1,
        "maxLength": market_squawk_decisions::MAX_DECISION_ID_BYTES,
        "pattern": "^[a-z][a-z0-9._-]*$",
    })
}

/// Decode the original closed field types; the final whole-request check owns their semantics.
pub(super) fn admit_argument(value: &Value, argument: Argument) -> Result<(), ToolInputError> {
    match argument {
        Argument::SourceCutoff => {
            return value
                .as_str()
                .ok_or(ToolInputError::Invalid)
                .and_then(|value| {
                    timestamp(value)
                        .map(|_| ())
                        .map_err(|_| ToolInputError::Invalid)
                });
        }
        Argument::ProfileBinding => {
            serde_json::from_value::<ProfileBinding>(value.clone()).map(drop)
        }
        Argument::WorkflowBinding => {
            serde_json::from_value::<WorkflowBinding>(value.clone()).map(drop)
        }
        Argument::OptionalMarket => {
            serde_json::from_value::<Option<crate::application::market_selection::MarketInvestmentReadReference>>(value.clone()).map(drop)
        }
        Argument::OptionalForecast => {
            serde_json::from_value::<Option<ForecastReference>>(value.clone()).map(drop)
        }
        Argument::OptionalSourceAction => {
            serde_json::from_value::<Option<crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference>>(value.clone()).map(drop)
        }
        Argument::OptionalFundamentalShareSources => {
            return if value.is_null() {
                Ok(())
            } else {
                let sources = value.as_str().ok_or(ToolInputError::Invalid)?;
                if sources.len() > crate::application::fair_value::MAX_FUNDAMENTAL_SHARE_SOURCE_BYTES {
                    return Err(ToolInputError::Invalid);
                }
                crate::application::fair_value::validate_fundamental_share_sources(sources.as_bytes())
                    .map_err(|_| ToolInputError::Invalid)
            };
        }
        Argument::ProbabilityForecasts => {
            serde_json::from_value::<ProbabilityForecastReferences>(value.clone()).map(drop)
        }
        Argument::OptionalBenchmarkInstrument => {
            serde_json::from_value::<Option<market_squawk_domain::InstrumentId>>(value.clone()).map(drop)
        }
        Argument::FinancialForecasts => {
            if value.as_array().is_none_or(|forecasts| forecasts.len() > 16) {
                return Err(ToolInputError::Invalid);
            }
            serde_json::from_value::<Vec<ForecastReference>>(value.clone()).map(drop)
        }
        Argument::OptionalStudy => {
            serde_json::from_value::<Option<StudyReference>>(value.clone()).map(drop)
        }
        Argument::OptionalCandidate => {
            serde_json::from_value::<Option<CandidateReference>>(value.clone()).map(drop)
        }
    }
    .map_err(|_| ToolInputError::Invalid)
}

/// Validate the sole request's canonical semantic identity after common transport admission.
/// These inert references still require the generation service's original source reopen checks.
pub(super) fn admit_request(arguments: &Map<String, Value>) -> Result<(), ToolInputError> {
    let mut business = arguments.clone();
    business.remove("confirm");
    business.remove("resultLimits");
    let request: GenerateRequest =
        serde_json::from_value(Value::Object(business)).map_err(|_| ToolInputError::Invalid)?;
    // Normalize optional fields and JSON key order exactly as publication and recovery do.
    let canonical = serde_json::to_vec(&request).map_err(|_| ToolInputError::Invalid)?;
    validate_canonical_request(&canonical).map_err(|_| ToolInputError::Invalid)
}
