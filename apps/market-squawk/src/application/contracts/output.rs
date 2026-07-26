//! Code-owned structured-result schema families for the production operation registry.

use serde_json::{Map, Value, json};

pub(super) fn output_data_schema(operation: &str) -> Option<Value> {
    let schema = match operation {
        "Source.Register" => closed(
            vec![
                ("profile", record()),
                ("outcome", enumeration(&["inserted", "replay"])),
            ],
            &["profile", "outcome"],
        ),
        "Source.Setup" => closed(
            vec![
                ("registration", record()),
                ("officialHandoff", record()),
                ("portal", record()),
                ("currentSession", nullable(record())),
            ],
            &[
                "registration",
                "officialHandoff",
                "portal",
                "currentSession",
            ],
        ),
        "Source.GetStatus" => nullable_rows(signature(vec![
            ("profile", record()),
            ("currentSession", nullable(record())),
            ("runtime", record()),
        ])),
        "Source.GetCoverage" => nullable_rows(signature(vec![
            ("surfaceId", text()),
            ("releaseState", text()),
            ("declaredCoverage", text()),
            ("qualityCeiling", text()),
            ("rights", array(text())),
            ("runtimeCoverage", record()),
        ])),
        "Source.GetHealth" => nullable_rows(signature(vec![
            ("surfaceId", text()),
            ("onboardingState", nullable(text())),
            ("runtimeHealth", record()),
        ])),
        "Source.ListObjects" => closed(
            vec![
                ("profile", text()),
                ("metadata", record()),
                ("request", record()),
                ("objects", array(record())),
            ],
            &["profile", "metadata", "request", "objects"],
        ),
        "Source.Discover" => closed(
            vec![
                ("profile", text()),
                ("metadata", record()),
                ("rights", record()),
                ("request", record()),
                ("objects", array(record())),
                ("receipts_survive_restart", boolean()),
            ],
            &[
                "profile",
                "metadata",
                "rights",
                "request",
                "objects",
                "receipts_survive_restart",
            ],
        ),
        "Market.GetSnapshot" => market_rows(&["sourceId", "instrumentId", "phase", "book"]),
        "Market.GetTrades" => market_rows(&[
            "sourceId",
            "instrumentId",
            "stableTradeId",
            "priceTicks",
            "quantityLots",
        ]),
        "Market.GetQuotes" => {
            market_rows(&["sourceId", "instrumentId", "bid", "ask", "stateEvaluatedAt"])
        }
        "Market.GetBooks" => market_rows(&[
            "sourceId",
            "instrumentId",
            "asOf",
            "stateEvaluatedAt",
            "book",
        ]),
        "Market.GetQuality" => market_rows(&[
            "sourceId",
            "instrumentId",
            "referenceAt",
            "stateBidDepth",
            "stateAskDepth",
        ]),
        "Market.GetComparisons" => market_rows(&[
            "instrumentId",
            "observationCount",
            "comparable",
            "observations",
        ]),
        "Research.ListDatasets" => nullable(page(generation())),
        "Research.GetManifest" => generation(),
        "Research.GetHistory"
        | "Research.GetAlternativeData"
        | "Fundamental.GetFilings"
        | "Fundamental.GetFacts"
        | "Fundamental.GetStatements"
        | "Fundamental.GetRatios"
        | "Macro.ListSeries"
        | "Macro.GetObservations"
        | "Macro.GetVintages"
        | "Macro.GetRevisions" => observation_result(),
        "Research.IngestSource" => closed(
            vec![
                ("manifest", manifest()),
                ("rowCount", unsigned()),
                ("totalBytes", unsigned()),
                ("objectCount", unsigned()),
                ("lineageDigest", text()),
            ],
            &[
                "manifest",
                "rowCount",
                "totalBytes",
                "objectCount",
                "lineageDigest",
            ],
        ),
        "Portfolio.Import" => closed(
            vec![
                ("accountId", text()),
                ("revisionId", text()),
                ("disposition", text()),
                ("sourceId", text()),
                ("effectiveAtUnixNanos", text()),
                ("availableAtUnixNanos", nullable(text())),
                ("artifactSha256", text()),
                ("rawEvidenceRetained", boolean()),
                ("reconciliationDiscrepancies", unsigned()),
            ],
            &[
                "accountId",
                "revisionId",
                "disposition",
                "sourceId",
                "effectiveAtUnixNanos",
                "availableAtUnixNanos",
                "artifactSha256",
                "rawEvidenceRetained",
                "reconciliationDiscrepancies",
            ],
        ),
        "Portfolio.GetHoldings" => array(signature(vec![
            ("instrument_id", text()),
            ("market_value", record()),
            ("revisionId", text()),
        ])),
        "Portfolio.GetTransactions" => array(signature(vec![
            ("broker_transaction_id", text()),
            ("kind", text()),
            ("revisionId", text()),
        ])),
        "Portfolio.GetPerformance" => signature(vec![
            ("accountId", text()),
            ("revisionId", text()),
            ("policy", text()),
            ("currentValue", record()),
        ]),
        "Portfolio.GetExposure" => signature(vec![
            ("accountId", text()),
            ("revisionId", text()),
            ("policy", text()),
            ("instrument", array(record())),
            ("currency", array(record())),
            ("sector", array(record())),
            ("factor", array(record())),
        ]),
        "Portfolio.GetRisk" => signature(vec![
            ("accountId", text()),
            ("revisionId", text()),
            ("policy", text()),
            ("confidence", number()),
            ("scenario", record()),
        ]),
        "Analysis.GetReturns" => closed(
            vec![
                ("manifest", manifest()),
                ("returnKind", enumeration(&["price", "total"])),
                ("values", array(number())),
            ],
            &["manifest", "returnKind", "values"],
        ),
        "Analysis.GetFactors" => closed(
            vec![
                ("manifest", manifest()),
                ("intercept", record()),
                ("exposures", array(record())),
                ("rSquared", record()),
            ],
            &["manifest", "intercept", "exposures", "rSquared"],
        ),
        "Analysis.GetValuation" => closed(
            vec![
                ("manifest", manifest()),
                ("measure", constant("valuation_multiple")),
                ("value", text()),
                ("unit", text()),
                ("decimalPolicy", record()),
            ],
            &["manifest", "measure", "value", "unit", "decimalPolicy"],
        ),
        "Analysis.GetScenarios" => closed(
            vec![
                ("manifest", manifest()),
                ("contributions", array(record())),
                ("total", record()),
            ],
            &["manifest", "contributions", "total"],
        ),
        "Analysis.GetFeatureDatasets" => nullable(page(signature(vec![(
            "kind",
            enumeration(&["feature_contract", "feature_dataset"]),
        )]))),
        "Analysis.GetBacktests" | "Analysis.RunBacktest" => backtest_record(),
        "Analysis.ReadArtifact" => closed(
            vec![
                ("artifact", internal_artifact()),
                ("offset", unsigned()),
                ("returnedBytes", unsigned()),
                ("contentBase64", text()),
                ("nextOffset", unsigned()),
                ("complete", boolean()),
            ],
            &[
                "artifact",
                "offset",
                "returnedBytes",
                "contentBase64",
                "nextOffset",
                "complete",
            ],
        ),
        "Model.GetMetadata" => signature(vec![
            ("modelId", text()),
            ("bundleId", text()),
            ("bundleVersion", unsigned()),
            ("trainingRunHash", text()),
            ("features", array(record())),
            ("decisionThresholds", record()),
        ]),
        "Model.ListBundles" => closed(
            vec![(
                "bundles",
                array(signature(vec![
                    ("modelId", text()),
                    ("bundleId", text()),
                    ("bundleVersion", unsigned()),
                ])),
            )],
            &["bundles"],
        ),
        "Model.Evaluate" => model_output(true),
        "Model.Predict" => model_output(false),
        "FairValue.ListMeasurements" => closed(
            vec![("measurements", array(measurement()))],
            &["measurements"],
        ),
        "FairValue.GetClassification" => closed(
            vec![
                ("measurement", measurement()),
                ("classification", classification()),
            ],
            &["measurement", "classification"],
        ),
        "FairValue.Explain" => closed(
            vec![
                ("classification", classification()),
                ("truthTable", array(record())),
                ("reasons", array(record())),
            ],
            &["classification", "truthTable", "reasons"],
        ),
        "FairValue.GetEvidence" => closed(
            vec![
                ("measurementId", text()),
                ("evidenceHash", text()),
                ("inputs", array(record())),
            ],
            &["measurementId", "evidenceHash", "inputs"],
        ),
        "FairValue.GetApprovalStatus" => closed(
            vec![
                ("measurementId", text()),
                ("at", text()),
                ("approvals", array(record())),
            ],
            &["measurementId", "at", "approvals"],
        ),
        "FairValue.Measure" => closed(
            vec![
                ("measurement", measurement()),
                ("classification", classification()),
                ("measurementReplay", boolean()),
                ("classificationReplay", boolean()),
            ],
            &[
                "measurement",
                "classification",
                "measurementReplay",
                "classificationReplay",
            ],
        ),
        "FairValue.Classify" => closed(
            vec![
                ("classification", classification()),
                ("classificationReplay", boolean()),
            ],
            &["classification", "classificationReplay"],
        ),
        "FairValue.Approve" => closed(vec![("approval", record())], &["approval"]),
        "FairValue.ApproveMarketAccess" | "FairValue.GetMarketAccess" => {
            closed(vec![("marketAccess", record())], &["marketAccess"])
        }
        "Bot.GetStatus" => bot_status(),
        "Bot.Start" => closed(
            vec![("state", constant("running")), ("provider", text())],
            &["state", "provider"],
        ),
        "Bot.Stop" | "Risk.TriggerKillSwitch" => closed(
            vec![
                ("state", constant("stopped")),
                ("shutdownComplete", boolean()),
                ("reason", text()),
            ],
            &["state", "shutdownComplete", "reason"],
        ),
        "Execution.GetOrders" => nullable_rows(signature(vec![
            ("orderId", text()),
            ("state", text()),
            ("requestedLots", unsigned()),
        ])),
        "Execution.GetFills" => nullable_rows(signature(vec![
            ("sequence", unsigned()),
            ("orderId", text()),
            ("quantityLots", unsigned()),
        ])),
        "Execution.Cancel" => closed(
            vec![
                ("orderId", text()),
                (
                    "status",
                    enumeration(&["pending", "canceled", "already_terminal"]),
                ),
                ("observedAt", integer()),
                ("cumulativeFilledLots", unsigned()),
                ("averageFillPriceTicks", nullable(integer())),
                ("maximumFillPriceTicks", nullable(integer())),
                ("cumulativeFees", money()),
            ],
            &[
                "orderId",
                "status",
                "observedAt",
                "cumulativeFilledLots",
                "averageFillPriceTicks",
                "maximumFillPriceTicks",
                "cumulativeFees",
            ],
        ),
        "Execution.Reconcile" => closed(
            vec![
                ("observedAt", integer()),
                ("orderCount", unsigned()),
                ("accountCount", unsigned()),
                ("sourceBound", boolean()),
                ("reconciliationRequired", boolean()),
            ],
            &[
                "observedAt",
                "orderCount",
                "accountCount",
                "sourceBound",
                "reconciliationRequired",
            ],
        ),
        _ => return None,
    };
    Some(schema)
}

fn market_rows(required: &[&str]) -> Value {
    let fields = required
        .iter()
        .map(|name| (*name, market_field(name)))
        .collect();
    nullable_rows(signature(fields))
}

fn market_field(name: &str) -> Value {
    match name {
        "phase" | "sourceId" | "instrumentId" | "stableTradeId" | "asOf" | "stateEvaluatedAt" => {
            text()
        }
        "book" => record(),
        "bid" | "ask" => nullable(record()),
        "observations" => array(record()),
        "comparable" => boolean(),
        "observationCount" | "stateBidDepth" | "stateAskDepth" | "priceTicks" | "quantityLots" => {
            integer()
        }
        "referenceAt" => text(),
        _ => record(),
    }
}

fn observation_result() -> Value {
    one_of(vec![
        null(),
        closed(
            vec![
                ("manifest", manifest()),
                ("arrowIpcBytes", unsigned()),
                ("rows", array(record())),
            ],
            &["manifest", "arrowIpcBytes", "rows"],
        ),
        closed(
            vec![("manifest", manifest()), ("artifact", query_artifact())],
            &["manifest", "artifact"],
        ),
    ])
}

fn generation() -> Value {
    closed(
        vec![
            ("manifest", manifest()),
            ("sourceId", text()),
            ("generationKind", text()),
            ("buildSpecDigest", nullable(text())),
            ("pythonExportSha256", nullable(text())),
            ("parents", array(record())),
            ("rowCount", unsigned()),
            ("totalBytes", unsigned()),
            ("lineageDigest", text()),
            ("objectCount", unsigned()),
        ],
        &[
            "manifest",
            "sourceId",
            "generationKind",
            "buildSpecDigest",
            "pythonExportSha256",
            "parents",
            "rowCount",
            "totalBytes",
            "lineageDigest",
            "objectCount",
        ],
    )
}

fn manifest() -> Value {
    signature(vec![("schema", record()), ("contentHash", text())])
}

fn query_artifact() -> Value {
    closed(
        vec![
            ("artifactId", text()),
            ("sha256", text()),
            ("byteCount", unsigned()),
            ("mediaType", text()),
            ("rowCount", unsigned()),
        ],
        &["artifactId", "sha256", "byteCount", "mediaType", "rowCount"],
    )
}

fn internal_artifact() -> Value {
    closed(
        vec![
            ("artifactId", text()),
            ("sha256", text()),
            ("byteCount", unsigned()),
            ("mediaType", text()),
        ],
        &["artifactId", "sha256", "byteCount", "mediaType"],
    )
}

fn page(item: Value) -> Value {
    closed(
        vec![
            ("items", array(item)),
            ("hasMore", boolean()),
            ("nextAfterDataset", nullable(text())),
        ],
        &["items", "hasMore", "nextAfterDataset"],
    )
}

fn backtest_record() -> Value {
    closed(
        vec![
            ("recordVersion", unsigned()),
            ("runId", text()),
            ("datasetIdentity", text()),
            ("objectGraphDigest", text()),
            ("executionAssumptionDigest", text()),
            ("cohortAuthorityDigest", nullable(text())),
            ("cohortUniverseDigest", nullable(text())),
            ("seed", unsigned()),
            ("selectionCriterion", text()),
            ("status", record()),
        ],
        &[
            "recordVersion",
            "runId",
            "datasetIdentity",
            "objectGraphDigest",
            "executionAssumptionDigest",
            "cohortAuthorityDigest",
            "cohortUniverseDigest",
            "seed",
            "selectionCriterion",
            "status",
        ],
    )
}

fn model_output(evaluation: bool) -> Value {
    let mut fields = vec![
        ("modelId", text()),
        ("bundleId", text()),
        ("bundleVersion", unsigned()),
        ("trainingDataset", manifest()),
        ("featureSemanticDigests", array(text())),
        ("score", number()),
        ("confidence", number()),
        ("decision", text()),
        ("executionAuthority", constant("none")),
        ("inferenceFailureBehavior", constant("no_action")),
    ];
    if evaluation {
        fields.push(("evaluationEvidence", record()));
        fields.push(("validationMetrics", array(record())));
    }
    signature(fields)
}

fn measurement() -> Value {
    signature(vec![
        ("measurementId", text()),
        ("evidenceHash", text()),
        ("accountId", text()),
        ("instrumentId", text()),
        ("inputCount", unsigned()),
    ])
}

fn classification() -> Value {
    signature(vec![
        ("decisionId", text()),
        ("measurementId", text()),
        ("rulesetVersion", unsigned()),
        ("hierarchy", text()),
    ])
}

fn money() -> Value {
    closed(
        vec![("amount", text()), ("currency", text())],
        &["amount", "currency"],
    )
}

fn bot_status() -> Value {
    one_of(vec![
        closed(
            vec![
                ("state", constant("stopped")),
                ("lastShutdownComplete", nullable(boolean())),
            ],
            &["state", "lastShutdownComplete"],
        ),
        closed(vec![("state", constant("starting"))], &["state"]),
        closed(vec![("state", constant("stopping"))], &["state"]),
        closed(
            vec![
                ("state", constant("failed")),
                ("provider", text()),
                ("requiresStop", constant_bool(true)),
            ],
            &["state", "provider", "requiresStop"],
        ),
        closed(
            vec![
                ("state", constant("running")),
                ("sequence", unsigned()),
                ("complete", boolean()),
                ("reconciliationRequired", boolean()),
                ("financialReconciliationCurrent", boolean()),
                ("orders", unsigned()),
                ("fills", unsigned()),
                ("positions", unsigned()),
            ],
            &[
                "state",
                "sequence",
                "complete",
                "reconciliationRequired",
                "financialReconciliationCurrent",
                "orders",
                "fills",
                "positions",
            ],
        ),
    ])
}

fn nullable_rows(item: Value) -> Value {
    one_of(vec![null(), array(item)])
}

fn nullable(schema: Value) -> Value {
    one_of(vec![null(), schema])
}

fn one_of(variants: Vec<Value>) -> Value {
    json!({"oneOf": variants})
}

fn closed(fields: Vec<(&str, Value)>, required: &[&str]) -> Value {
    object(fields, required, false)
}

fn signature(fields: Vec<(&str, Value)>) -> Value {
    let required = fields.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    object(fields, &required, true)
}

fn object(fields: Vec<(&str, Value)>, required: &[&str], additional: bool) -> Value {
    let properties = fields
        .into_iter()
        .map(|(name, schema)| (name.to_owned(), schema))
        .collect::<Map<_, _>>();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": additional,
    })
}

fn record() -> Value {
    json!({"type": "object", "minProperties": 1, "additionalProperties": true})
}

fn array(items: Value) -> Value {
    json!({"type": "array", "items": items})
}

fn text() -> Value {
    json!({"type": "string"})
}

fn boolean() -> Value {
    json!({"type": "boolean"})
}

fn number() -> Value {
    json!({"type": "number"})
}

fn integer() -> Value {
    json!({"type": "integer"})
}

fn unsigned() -> Value {
    json!({"type": "integer", "minimum": 0})
}

fn null() -> Value {
    json!({"type": "null"})
}

fn enumeration(values: &[&str]) -> Value {
    json!({"type": "string", "enum": values})
}

fn constant(value: &str) -> Value {
    json!({"type": "string", "const": value})
}

fn constant_bool(value: bool) -> Value {
    json!({"type": "boolean", "const": value})
}

#[cfg(test)]
mod tests {
    use super::output_data_schema;

    #[test]
    fn every_production_operation_has_a_code_owned_data_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        for operation in super::super::OPERATION_SPECS {
            assert!(
                output_data_schema(operation.name).is_some(),
                "missing output data contract for {}",
                operation.name
            );
        }
        let capabilities = super::super::application_capabilities()?;
        assert_eq!(
            capabilities.tools().len(),
            super::super::OPERATION_SPECS.len()
        );
        assert!(capabilities.tools().iter().all(|descriptor| {
            descriptor.output_schema().get("type") == Some(&serde_json::json!("object"))
                && descriptor.output_schema().get("oneOf").is_some()
        }));
        Ok(())
    }
}
