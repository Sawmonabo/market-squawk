//! Code-owned structured-result schema families for the production operation registry.

use serde_json::{Map, Value, json};

use market_squawk_sources::FRED_ALFRED_API_SURFACE_ID;

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
            ("providerDatasetIdentifier", nullable(text())),
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
        "Source.Inspect" => closed(
            vec![
                ("provider", constant(FRED_ALFRED_API_SURFACE_ID)),
                ("onboardingSessionId", uuid()),
                ("datasetIdentifier", bounded_text(512)),
                ("objectId", bounded_text(512)),
                ("pageIndex", bounded_unsigned(63)),
                ("pageEvidence", fred_page_evidence()),
                ("receivedAt", timestamp()),
                (
                    "observations",
                    bounded_array(fred_macro_observation(), 1_024),
                ),
            ],
            &[
                "provider",
                "onboardingSessionId",
                "datasetIdentifier",
                "objectId",
                "pageIndex",
                "pageEvidence",
                "receivedAt",
                "observations",
            ],
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

fn bounded_array(items: Value, maximum: usize) -> Value {
    json!({"type": "array", "maxItems": maximum, "items": items})
}

fn fixed_array(items: Value, length: usize) -> Value {
    json!({"type": "array", "minItems": length, "maxItems": length, "items": items})
}

fn text() -> Value {
    json!({"type": "string"})
}

fn bounded_text(maximum: usize) -> Value {
    json!({"type": "string", "minLength": 1, "maxLength": maximum})
}

fn uuid() -> Value {
    json!({"type": "string", "format": "uuid"})
}

fn timestamp() -> Value {
    json!({"type": "string", "format": "date-time"})
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

fn bounded_unsigned(maximum: u64) -> Value {
    json!({"type": "integer", "minimum": 0, "maximum": maximum})
}

fn bounded_unsigned_range(minimum: u64, maximum: u64) -> Value {
    json!({"type": "integer", "minimum": minimum, "maximum": maximum})
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

fn constant_unsigned(value: u64) -> Value {
    json!({"type": "integer", "const": value})
}

fn constant_bool(value: bool) -> Value {
    json!({"type": "boolean", "const": value})
}

fn fred_page_evidence() -> Value {
    closed(
        vec![
            (
                "content_digest",
                closed(
                    vec![
                        ("algorithm", constant("sha256")),
                        ("bytes", fixed_array(bounded_unsigned(255), 32)),
                    ],
                    &["algorithm", "bytes"],
                ),
            ),
            (
                "version_pinned_locator",
                closed(
                    vec![
                        ("reference", bounded_text(512)),
                        (
                            "version",
                            json!({"type": "string", "minLength": 64, "maxLength": 64}),
                        ),
                    ],
                    &["reference", "version"],
                ),
            ),
        ],
        &["content_digest", "version_pinned_locator"],
    )
}

fn fred_macro_observation() -> Value {
    closed(
        vec![
            ("observation", constant("macro")),
            (
                "payload",
                one_of(vec![
                    closed(
                        vec![
                            ("context", fred_research_context()),
                            ("series", bounded_text(512)),
                            ("value", bounded_text(128)),
                            ("unit", bounded_text(512)),
                        ],
                        &["context", "series", "value", "unit"],
                    ),
                    closed(
                        vec![
                            ("context", fred_research_context()),
                            ("series", bounded_text(512)),
                            (
                                "missing",
                                one_of(vec![
                                    closed(vec![("marker", bounded_text(512))], &["marker"]),
                                    closed(
                                        vec![
                                            ("marker", bounded_text(512)),
                                            ("reason", bounded_text(512)),
                                        ],
                                        &["marker", "reason"],
                                    ),
                                ]),
                            ),
                            ("unit", bounded_text(512)),
                        ],
                        &["context", "series", "missing", "unit"],
                    ),
                ]),
            ),
        ],
        &["observation", "payload"],
    )
}

fn fred_research_context() -> Value {
    closed(
        vec![
            ("provenance", fred_research_provenance()),
            ("time", fred_research_time()),
        ],
        &["provenance", "time"],
    )
}

fn fred_research_provenance() -> Value {
    closed(
        vec![
            ("schema_version", constant_unsigned(1)),
            ("source_id", bounded_text(128)),
            ("instrument_id", null()),
            ("venue_id", null()),
            ("source_identifier", bounded_text(512)),
            ("source_timestamp", null()),
            ("received_at", integer()),
            ("ingested_at", integer()),
            ("quality", constant("official_delayed")),
            (
                "payload_reference",
                closed(
                    vec![
                        ("kind", constant("content_hash")),
                        (
                            "value",
                            closed(
                                vec![
                                    ("algorithm", constant("sha256")),
                                    ("digest", fixed_array(bounded_unsigned(255), 32)),
                                ],
                                &["algorithm", "digest"],
                            ),
                        ),
                    ],
                    &["kind", "value"],
                ),
            ),
            (
                "availability",
                closed(
                    vec![
                        ("kind", constant("local_first_observed")),
                        ("observed_at", integer()),
                    ],
                    &["kind", "observed_at"],
                ),
            ),
        ],
        &[
            "schema_version",
            "source_id",
            "instrument_id",
            "venue_id",
            "source_identifier",
            "source_timestamp",
            "received_at",
            "ingested_at",
            "quality",
            "payload_reference",
            "availability",
        ],
    )
}

fn fred_research_time() -> Value {
    closed(
        vec![
            ("schema_version", constant_unsigned(2)),
            ("effective", fred_calendar_coordinate()),
            ("published", fred_calendar_coordinate()),
            ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("superseded", nullable(fred_calendar_coordinate())),
        ],
        &[
            "schema_version",
            "effective",
            "published",
            "revision",
            "superseded",
        ],
    )
}

fn fred_calendar_coordinate() -> Value {
    closed(
        vec![
            ("schema_version", constant_unsigned(2)),
            (
                "coordinate",
                closed(
                    vec![
                        ("precision", constant("calendar_date")),
                        (
                            "value",
                            closed(
                                vec![
                                    ("year", bounded_unsigned_range(1, 9_999)),
                                    ("month", bounded_unsigned_range(1, 12)),
                                    ("day", bounded_unsigned_range(1, 31)),
                                ],
                                &["year", "month", "day"],
                            ),
                        ),
                    ],
                    &["precision", "value"],
                ),
            ),
        ],
        &["schema_version", "coordinate"],
    )
}

#[cfg(test)]
mod tests {
    use super::output_data_schema;
    use market_squawk_services::{
        JsonStructureLimits, ServiceContractError, ServiceLimits, ToolResultMetadata,
        TypedToolResult,
    };
    use market_squawk_sources::FRED_ALFRED_API_SURFACE_ID;
    use serde_json::{Value, json};

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

    #[test]
    fn source_inspection_rejects_missing_or_extra_nested_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let capabilities = super::super::application_capabilities()?;
        let Some(descriptor) = capabilities
            .tools()
            .iter()
            .find(|descriptor| descriptor.name() == "Source.Inspect")
        else {
            return Err("Source.Inspect descriptor is missing".into());
        };
        let valid = fred_inspection_data();
        assert!(
            fred_inspection_result(valid.clone())?
                .validate_for(descriptor)
                .is_ok()
        );

        let mut missing = valid.clone();
        let Some(page_evidence) = missing
            .get_mut("pageEvidence")
            .and_then(Value::as_object_mut)
        else {
            return Err("valid fixture lacks page evidence".into());
        };
        page_evidence.remove("content_digest");
        assert!(matches!(
            fred_inspection_result(missing)?.validate_for(descriptor),
            Err(ServiceContractError::SourceEvidencePolicy)
        ));

        let mut extra = valid;
        let Some(context) = extra
            .pointer_mut("/observations/0/payload/context")
            .and_then(Value::as_object_mut)
        else {
            return Err("valid fixture lacks observation context".into());
        };
        context.insert("unexpected".to_owned(), Value::Bool(true));
        assert!(matches!(
            fred_inspection_result(extra)?.validate_for(descriptor),
            Err(ServiceContractError::SourceEvidencePolicy)
        ));
        Ok(())
    }

    fn fred_inspection_result(data: Value) -> Result<TypedToolResult, Box<dyn std::error::Error>> {
        let limits = ServiceLimits::try_new(
            1024 * 1024,
            1_024,
            1024 * 1024,
            1_024,
            JsonStructureLimits::try_new(32, 64 * 1024, 10_000, 2_000)?,
        )?;
        let metadata = ToolResultMetadata::try_complete(
            json!({"provider": FRED_ALFRED_API_SURFACE_ID}),
            json!({"quality": "official_delayed"}),
        )?;
        Ok(TypedToolResult::try_new(data, 1, metadata, limits)?)
    }

    fn fred_inspection_data() -> Value {
        let digest = vec![7_u8; 32];
        let coordinate = |year, month, day| {
            json!({
                "schema_version": 2,
                "coordinate": {
                    "precision": "calendar_date",
                    "value": {"year": year, "month": month, "day": day}
                }
            })
        };
        json!({
            "provider": FRED_ALFRED_API_SURFACE_ID,
            "onboardingSessionId": "c127919d-6540-47f8-9f6b-902523578cb5",
            "datasetIdentifier": "fred:series:UNRATE",
            "objectId": "fred-page-v2:0:1:1:1:1:fixture",
            "pageIndex": 0,
            "pageEvidence": {
                "content_digest": {"algorithm": "sha256", "bytes": digest},
                "version_pinned_locator": {
                    "reference": "https://api.stlouisfed.org/fred/series/observations",
                    "version": "0707070707070707070707070707070707070707070707070707070707070707"
                }
            },
            "receivedAt": "2026-07-26T12:34:56.123456789Z",
            "observations": [{
                "observation": "macro",
                "payload": {
                    "context": {
                        "provenance": {
                            "schema_version": 1,
                            "source_id": "fred",
                            "instrument_id": null,
                            "venue_id": null,
                            "source_identifier": "fred:UNRATE:2026-06-01:2026-07-03",
                            "source_timestamp": null,
                            "received_at": 1_000,
                            "ingested_at": 1_001,
                            "quality": "official_delayed",
                            "payload_reference": {
                                "kind": "content_hash",
                                "value": {"algorithm": "sha256", "digest": vec![9_u8; 32]}
                            },
                            "availability": {
                                "kind": "local_first_observed",
                                "observed_at": 1_000
                            }
                        },
                        "time": {
                            "schema_version": 2,
                            "effective": coordinate(2026, 6, 1),
                            "published": coordinate(2026, 7, 3),
                            "revision": 739_435,
                            "superseded": null
                        }
                    },
                    "series": "UNRATE",
                    "value": "4.1",
                    "unit": "fred-unit:v1:Percent"
                }
            }]
        })
    }
}
