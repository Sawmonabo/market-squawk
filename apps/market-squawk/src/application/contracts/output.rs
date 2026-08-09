//! Code-owned structured-result schema families for the production operation registry.

use serde_json::{Map, Value, json};

use market_squawk_sources::FRED_ALFRED_API_SURFACE_ID;

pub(super) fn output_data_schema(operation: &str) -> Option<Value> {
    let schema = match operation {
        "Job.List" => closed(
            vec![
                ("jobs", bounded_array(job_view(), 1_024)),
                ("next", nullable(text())),
            ],
            &["jobs", "next"],
        ),
        "Job.Get" | "Job.Cancel" | "Job.Confirm" => job_view(),
        "Job.Retry" => job_receipt(),
        "Job.Watch" => closed(
            vec![
                ("events", bounded_array(record(), 4_096)),
                ("next", nullable(unsigned())),
            ],
            &["events", "next"],
        ),
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
        "Source.GetStatus" => nullable_rows(closed(
            vec![
                ("profile", record()),
                ("currentSession", nullable(record())),
                ("providerDatasetIdentifier", nullable(text())),
                (
                    "lifecycleSupport",
                    enumeration(&["managed", "not_applicable"]),
                ),
                ("lifecycle", nullable(source_lifecycle_status())),
                ("runtime", record()),
            ],
            &[
                "profile",
                "currentSession",
                "providerDatasetIdentifier",
                "lifecycleSupport",
                "lifecycle",
                "runtime",
            ],
        )),
        "Source.GetCoverage" => nullable_rows(signature(vec![
            ("surfaceId", text()),
            ("releaseState", text()),
            ("declaredCoverage", text()),
            ("qualityCeiling", text()),
            ("rights", bounded_array(data_use_right(), 6)),
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
        "Source.Start"
        | "Source.Stop"
        | "Source.Retry"
        | "Source.Resynchronize"
        | "Source.Verify"
        | "Source.Reconfigure"
        | "Source.Remove" => source_lifecycle_receipt(),
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
        "Market.GetUnifiedFeed" => market_rows(&[
            "instrumentId",
            "symbol",
            "symbolVenueId",
            "assetClass",
            "quoteCurrency",
            "definitionRevision",
            "tickSize",
            "lotSize",
            "availability",
            "confidence",
            "quote",
            "orderBook",
            "selectedSource",
            "alternatives",
            "selectionReceipt",
        ]),
        "Market.SearchUniverse" => market_rows(&[
            "referenceId",
            "symbol",
            "name",
            "venueId",
            "assetClass",
            "referenceOnly",
            "isEtf",
            "roundLotSize",
            "directoryPresence",
            "quality",
            "effectiveAt",
            "availableAt",
            "sourceId",
            "providerId",
            "sourcePayloadSha256",
            "matchKind",
            "quoteAvailability",
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
        "Research.StartIngestSource"
        | "Research.CommitStagedFile"
        | "Research.StartDatasetBuild"
        | "Research.StartExport"
        | "Analysis.StartScenarioBatch"
        | "Analysis.StartFeatureDatasetBuild"
        | "Analysis.StartPreparedFeatureDatasetBuild"
        | "Analysis.StartPreparedBacktest"
        | "Analysis.StartBacktest"
        | "Decision.RunScreen" => job_receipt(),
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
        "Research.PreviewStagedFile" => closed(
            vec![
                ("previewId", sha256()),
                ("sha256", sha256()),
                ("format", enumeration(&["csv", "json", "ndjson", "parquet"])),
                ("rowCount", unsigned()),
                (
                    "columns",
                    bounded_array(research_file_preview_column(), 256),
                ),
                (
                    "sampleRows",
                    bounded_array(bounded_array(research_file_preview_cell(), 256), 20),
                ),
            ],
            &[
                "previewId",
                "sha256",
                "format",
                "rowCount",
                "columns",
                "sampleRows",
            ],
        ),
        "Research.DiscardStagedFile" => closed(
            vec![
                ("previewId", sha256()),
                ("status", enumeration(&["discarded"])),
            ],
            &["previewId", "status"],
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
        "Portfolio.PreviewStagedImport" => closed(
            vec![
                ("previewId", text()),
                ("digest", text()),
                ("preview", record()),
            ],
            &["previewId", "digest", "preview"],
        ),
        "Portfolio.ApproveStagedImport" => closed(
            vec![
                ("approvalId", uuid()),
                ("previewId", text()),
                ("previewDigest", text()),
                ("status", enumeration(&["approved", "promoting"])),
            ],
            &["approvalId", "previewId", "previewDigest", "status"],
        ),
        "Portfolio.CommitStagedImport" => closed(
            vec![
                ("approvalId", uuid()),
                ("previewId", text()),
                ("previewDigest", text()),
                ("receipt", record()),
                ("status", enumeration(&["committed"])),
            ],
            &[
                "approvalId",
                "previewId",
                "previewDigest",
                "receipt",
                "status",
            ],
        ),
        "Portfolio.DiscardStagedImport" => closed(
            vec![
                ("previewId", text()),
                ("status", enumeration(&["discarded"])),
            ],
            &["previewId", "status"],
        ),
        "Portfolio.ListAccounts" | "Portfolio.ListRevisions" => nullable_rows(record()),
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
        "Portfolio.GetAttribution"
        | "Portfolio.EvaluateScenario"
        | "Portfolio.EvaluateScenarioBatch"
        | "Portfolio.ProposeRebalance"
        | "Portfolio.EvaluateCandidateImpact" => portfolio_advanced_report(),
        "Analysis.GetReturns" => closed(
            vec![
                ("manifest", manifest()),
                ("returnKind", enumeration(&["price", "total"])),
                ("values", array(number())),
            ],
            &["manifest", "returnKind", "values"],
        ),
        "Analysis.Lookup" => closed(
            vec![
                ("query", text()),
                ("matches", bounded_array(record(), 64)),
                ("categories", bounded_array(record(), 10)),
                ("truncated", boolean()),
            ],
            &["query", "matches", "categories", "truncated"],
        ),
        "Analysis.GetDecisionOverview" => closed(
            vec![
                ("providers", record()),
                ("datasets", record()),
                ("screens", record()),
                ("jobs", record()),
                ("commands", record()),
                ("unavailable", bounded_array(record(), 4)),
            ],
            &[
                "providers",
                "datasets",
                "screens",
                "jobs",
                "commands",
                "unavailable",
            ],
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
        "Analysis.GetFeatureDatasetPreparationOptions" => closed(
            vec![
                ("catalogGeneration", text()),
                ("datasets", bounded_array(record(), 256)),
            ],
            &["catalogGeneration", "datasets"],
        ),
        "Analysis.PreviewFeatureDatasetBuild" => closed(
            vec![
                ("receipt", record()),
                ("dataset", text()),
                ("source", text()),
                ("instrumentId", uuid()),
                ("intendedUse", enumeration(&["local_analysis", "train"])),
                ("examples", unsigned()),
                ("trainExamples", unsigned()),
                ("validationExamples", unsigned()),
                ("testExamples", unsigned()),
                ("observedFrom", timestamp()),
                ("observedThrough", timestamp()),
                ("buildSpecSha256", text()),
                ("evidence", bounded_array(text(), 4_096)),
            ],
            &[
                "receipt",
                "dataset",
                "source",
                "instrumentId",
                "intendedUse",
                "examples",
                "trainExamples",
                "validationExamples",
                "testExamples",
                "observedFrom",
                "observedThrough",
                "buildSpecSha256",
                "evidence",
            ],
        ),
        "Analysis.GetBacktestPreparation" => backtest_preparation_options(),
        "Analysis.PreviewBacktest" => backtest_preparation_preview(),
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
            ("admissionEvidence", record()),
            ("runtimeHealth", record()),
            ("trainingEvidence", record()),
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
        "Model.StartTraining" | "Model.StartPreparedForecast" => job_receipt(),
        "Model.GetForecastPreparation" => closed(
            vec![
                ("runtimeGenerationSha256", text()),
                ("models", bounded_array(record(), 4_096)),
            ],
            &["runtimeGenerationSha256", "models"],
        ),
        "Model.PrepareForecast" => closed(
            vec![("receipt", record()), ("preview", record())],
            &["receipt", "preview"],
        ),
        "Model.StartForecast" => job_receipt(),
        "Model.GenerateForecast" | "Model.GetForecast" => forecast_vintage(),
        "Model.ListForecasts" => closed(
            vec![
                ("forecasts", bounded_array(record(), 4_096)),
                ("available", unsigned()),
                ("truncated", boolean()),
            ],
            &["forecasts", "available", "truncated"],
        ),
        "Model.GetForecastOutcomes" => closed(
            vec![
                ("vintageId", text()),
                ("outcomes", bounded_array(record(), 4_096)),
                ("available", unsigned()),
                ("truncated", boolean()),
            ],
            &["vintageId", "outcomes", "available", "truncated"],
        ),
        "Decision.SaveScreen"
        | "Decision.CreateDossier"
        | "Decision.CreateTargetSet"
        | "Decision.ReviewTargetSet"
        | "Decision.ReevaluateTargetSet" => closed(
            vec![("outcome", enumeration(&["appended", "already_present"]))],
            &["outcome"],
        ),
        "Decision.GetDossierPreparation" => closed(
            vec![
                ("candidateId", text()),
                ("screenRunId", text()),
                ("instrumentId", uuid()),
                ("selectedAt", timestamp()),
                ("requiredEvidence", bounded_array(text(), 4)),
                ("portfolioImpactAvailable", boolean()),
                ("forecastOptions", bounded_array(record(), 64)),
                ("fairValueOptions", bounded_array(record(), 64)),
            ],
            &[
                "candidateId",
                "screenRunId",
                "instrumentId",
                "selectedAt",
                "requiredEvidence",
                "portfolioImpactAvailable",
                "forecastOptions",
                "fairValueOptions",
            ],
        ),
        "Decision.PrepareDossier" => closed(
            vec![
                ("receiptId", uuid()),
                ("dossierId", text()),
                ("candidateId", text()),
                ("screenRunId", text()),
                ("instrumentId", uuid()),
                ("evidence", bounded_array(text(), 6)),
                ("forecastSelector", nullable(text())),
                ("fairValueSelector", nullable(text())),
                ("assembledAt", timestamp()),
                ("receiptExpiresAt", timestamp()),
            ],
            &[
                "receiptId",
                "dossierId",
                "candidateId",
                "screenRunId",
                "instrumentId",
                "evidence",
                "forecastSelector",
                "fairValueSelector",
                "assembledAt",
                "receiptExpiresAt",
            ],
        ),
        "Decision.GetDossier" => signature(vec![
            ("id", text()),
            ("candidateId", text()),
            ("instrumentId", text()),
            ("assembledAt", integer()),
            ("evidence", record()),
            ("references", array(record())),
        ]),
        "Decision.GetTargetPreparation" => closed(
            vec![
                ("dossierId", text()),
                ("instrumentId", uuid()),
                ("assembledAt", timestamp()),
                ("forecastOptions", bounded_array(record(), 4_096)),
                ("fairValueAvailable", boolean()),
                ("portfolioAvailable", boolean()),
                ("referenceMarks", bounded_array(record(), 4_096)),
            ],
            &[
                "dossierId",
                "instrumentId",
                "assembledAt",
                "forecastOptions",
                "fairValueAvailable",
                "portfolioAvailable",
                "referenceMarks",
            ],
        ),
        "Decision.PrepareTargetSet" => closed(
            vec![
                ("receiptId", uuid()),
                ("receiptExpiresAt", timestamp()),
                ("targetId", text()),
                ("revision", unsigned()),
                ("dossierId", text()),
                ("instrumentId", uuid()),
                ("intent", enumeration(&["buy", "sell", "hold"])),
                ("referenceMark", record()),
                ("referenceMarkObservedAt", timestamp()),
                ("referenceMarkQuality", text()),
                ("referenceMarkSource", text()),
                ("prices", record()),
                ("method", text()),
                ("assumptions", bounded_array(record(), 4_096)),
                ("thesis", text()),
                ("risks", bounded_array(text(), 4_096)),
                ("invalidationConditions", bounded_array(text(), 4_096)),
                ("createdAt", timestamp()),
                ("horizonAt", timestamp()),
                ("expiresAt", timestamp()),
                ("reviewDueAt", timestamp()),
                ("author", text()),
                ("rulesetVersion", unsigned()),
                ("forecastSelected", boolean()),
                ("fairValueSelected", boolean()),
                ("portfolioSelected", boolean()),
            ],
            &[
                "receiptId",
                "receiptExpiresAt",
                "targetId",
                "revision",
                "dossierId",
                "instrumentId",
                "intent",
                "referenceMark",
                "referenceMarkObservedAt",
                "referenceMarkQuality",
                "referenceMarkSource",
                "prices",
                "method",
                "assumptions",
                "thesis",
                "risks",
                "invalidationConditions",
                "createdAt",
                "horizonAt",
                "expiresAt",
                "reviewDueAt",
                "author",
                "rulesetVersion",
                "forecastSelected",
                "fairValueSelected",
                "portfolioSelected",
            ],
        ),
        "Decision.GetTargetSet" => closed(
            vec![
                ("target", record()),
                ("status", text()),
                ("latestReview", nullable(record())),
                ("latestInvalidation", nullable(record())),
            ],
            &["target", "status", "latestReview", "latestInvalidation"],
        ),
        "Decision.ListScreens" => closed(
            vec![("screens", bounded_array(record(), 4_096))],
            &["screens"],
        ),
        "Decision.ListScreenRuns" => closed(
            vec![
                (
                    "runs",
                    bounded_array(
                        signature(vec![
                            ("id", text()),
                            ("screenId", text()),
                            ("screenRevision", unsigned()),
                            ("asOf", timestamp()),
                            ("datasetIdentity", text()),
                            ("universeIdentity", text()),
                            ("candidateCount", unsigned()),
                        ]),
                        1_000,
                    ),
                ),
                ("nextAfter", nullable(text())),
            ],
            &["runs", "nextAfter"],
        ),
        "Decision.GetCandidates" => closed(
            vec![("candidates", bounded_array(record(), 4_096))],
            &["candidates"],
        ),
        "Decision.ListCandidateDossiers" => closed(
            vec![
                ("dossiers", bounded_array(record(), 1_000)),
                ("nextAfter", nullable(text())),
            ],
            &["dossiers", "nextAfter"],
        ),
        "Decision.ListTargetSets" => closed(
            vec![("targets", bounded_array(record(), 4_096))],
            &["targets"],
        ),
        "Decision.ListTargetIndex" => closed(
            vec![
                (
                    "targets",
                    bounded_array(
                        signature(vec![
                            ("id", text()),
                            ("revision", unsigned()),
                            ("instrumentId", uuid()),
                            (
                                "status",
                                enumeration(&[
                                    "pending_review",
                                    "active",
                                    "rejected",
                                    "needs_changes",
                                    "needs_review",
                                    "superseded",
                                ]),
                            ),
                        ]),
                        1_000,
                    ),
                ),
                ("nextAfter", nullable(text())),
            ],
            &["targets", "nextAfter"],
        ),
        "Decision.GetTargetSetStatus" => closed(
            vec![(
                "status",
                enumeration(&[
                    "pending_review",
                    "active",
                    "rejected",
                    "needs_changes",
                    "needs_review",
                    "superseded",
                ]),
            )],
            &["status"],
        ),
        "Operations.ListBackups" => closed(
            vec![
                ("revision", unsigned()),
                ("manifests", bounded_array(record(), 64)),
                ("nextAfterBackupId", nullable(text())),
                ("pendingDeletions", unsigned()),
            ],
            &[
                "revision",
                "manifests",
                "nextAfterBackupId",
                "pendingDeletions",
            ],
        ),
        "Operations.GetBackup" => closed(
            vec![
                ("formatVersion", unsigned()),
                ("backupId", text()),
                ("snapshot", record()),
                ("ownership", record()),
                ("analyticalReceipt", record()),
                ("components", bounded_array(record(), 9)),
                ("encryption", one_of(vec![text(), record()])),
                ("manifestSha256", text()),
            ],
            &[
                "formatVersion",
                "backupId",
                "snapshot",
                "ownership",
                "analyticalReceipt",
                "components",
                "encryption",
                "manifestSha256",
            ],
        ),
        "Operations.GetRuntimeStatus" => closed(
            vec![
                ("ready", constant_bool(true)),
                (
                    "workspace",
                    closed(
                        vec![
                            ("workspaceId", uuid()),
                            ("generation", bounded_unsigned_range(1, u64::MAX)),
                        ],
                        &["workspaceId", "generation"],
                    ),
                ),
                (
                    "workspaceSchemaVersion",
                    bounded_unsigned_range(1, u64::from(u32::MAX)),
                ),
                ("availableDiskBytes", unsigned()),
                ("runningJobs", bounded_unsigned(u64::from(u32::MAX))),
                ("runningMutationJobs", bounded_unsigned(u64::from(u32::MAX))),
                ("activeSources", bounded_unsigned(u64::from(u32::MAX))),
                ("connectedClients", bounded_unsigned(u64::from(u32::MAX))),
                ("paperExecutionActive", boolean()),
                ("executionReconciliationPending", boolean()),
            ],
            &[
                "ready",
                "workspace",
                "workspaceSchemaVersion",
                "availableDiskBytes",
                "runningJobs",
                "runningMutationJobs",
                "activeSources",
                "connectedClients",
                "paperExecutionActive",
                "executionReconciliationPending",
            ],
        ),
        "Operations.GetUpdateStatus" => closed(
            vec![
                ("availability", text()),
                ("currentGeneration", unsigned()),
                ("knownGoodVersion", text()),
                ("stagedCandidate", nullable(record())),
                ("lastCheckedAt", nullable(integer())),
                ("recoveryRequired", boolean()),
            ],
            &[
                "availability",
                "currentGeneration",
                "knownGoodVersion",
                "stagedCandidate",
                "lastCheckedAt",
                "recoveryRequired",
            ],
        ),
        "Operations.StartBackup"
        | "Operations.StartBackupVerification"
        | "Operations.StartBackupRetention"
        | "Operations.StartRestore"
        | "Operations.StartWorkspaceSwitch"
        | "Operations.StartUpdate"
        | "Operations.StartProgramRollback" => job_receipt(),
        "Operations.PreviewBackupRetention"
        | "Operations.PreviewRestore"
        | "Operations.PreviewWorkspaceSwitch"
        | "Operations.CheckForUpdates"
        | "Operations.PreviewUpdate"
        | "Operations.PreviewProgramRollback"
        | "Operations.PreviewSettingsChange"
        | "Operations.PreviewSettingsRollback" => operations_preview(),
        "Operations.ListWorkspaces" => closed(
            vec![
                ("active", record()),
                ("workspaces", bounded_array(record(), 64)),
                ("nextAfterWorkspaceId", nullable(uuid())),
            ],
            &["active", "workspaces", "nextAfterWorkspaceId"],
        ),
        "Operations.QueryLogs" => closed(
            vec![
                ("records", bounded_array(record(), 10_000)),
                ("nextAfterSequence", nullable(unsigned())),
            ],
            &["records", "nextAfterSequence"],
        ),
        "Operations.ExportLogs" => closed(
            vec![
                ("artifactReference", text()),
                ("byteLength", unsigned()),
                ("sha256", text()),
            ],
            &["artifactReference", "byteLength", "sha256"],
        ),
        "Operations.GetSettings" => closed(
            vec![
                ("revision", unsigned()),
                ("entries", bounded_array(record(), 16)),
                ("digest", text()),
            ],
            &["revision", "entries", "digest"],
        ),
        "Operations.ApplySettingsChange" | "Operations.RollbackSettings" => closed(
            vec![
                ("previousRevision", unsigned()),
                ("activeRevision", unsigned()),
                ("activeDigest", text()),
                (
                    "restartImpact",
                    enumeration(&["none", "service_reload", "service_restart"]),
                ),
                ("rolledBackFromRevision", nullable(unsigned())),
            ],
            &[
                "previousRevision",
                "activeRevision",
                "activeDigest",
                "restartImpact",
                "rolledBackFromRevision",
            ],
        ),
        "Setup.GetStatus" => closed(
            vec![
                ("formatVersion", unsigned()),
                ("catalog", setup_plan_catalog()),
                ("currentRevision", unsigned()),
                ("acceptedPlan", nullable(setup_accepted_plan())),
            ],
            &[
                "formatVersion",
                "catalog",
                "currentRevision",
                "acceptedPlan",
            ],
        ),
        "Setup.PreviewPlan" => closed(
            vec![
                ("formatVersion", unsigned()),
                ("previewId", uuid()),
                ("ownerWorkspace", uuid()),
                ("currentRevision", unsigned()),
                ("planDigest", text()),
                ("plan", setup_plan()),
                (
                    "includedCapabilities",
                    bounded_array(setup_capability(), 17),
                ),
                (
                    "externalContacts",
                    bounded_array(setup_external_contact(), 8),
                ),
                (
                    "reversibleLocalChanges",
                    bounded_array(setup_reversible_change(), 9),
                ),
                ("expectedTime", setup_time_estimate()),
                ("expectedDisk", setup_disk_estimate()),
                ("safeSkipSteps", bounded_array(setup_step_id(), 7)),
                ("issuedAtUnixSeconds", unsigned()),
                ("expiresAtUnixSeconds", unsigned()),
                ("previewSha256", text()),
            ],
            &[
                "formatVersion",
                "previewId",
                "ownerWorkspace",
                "currentRevision",
                "planDigest",
                "plan",
                "includedCapabilities",
                "externalContacts",
                "reversibleLocalChanges",
                "expectedTime",
                "expectedDisk",
                "safeSkipSteps",
                "issuedAtUnixSeconds",
                "expiresAtUnixSeconds",
                "previewSha256",
            ],
        ),
        "Setup.ApplyPlan" => closed(
            vec![
                ("revision", unsigned()),
                ("digest", text()),
                ("acceptedAtUnixSeconds", unsigned()),
            ],
            &["revision", "digest", "acceptedAtUnixSeconds"],
        ),
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
        "FairValue.ProposeOverride" => closed(
            vec![("override", record()), ("classification", classification())],
            &["override", "classification"],
        ),
        "FairValue.RevokeApproval" => closed(vec![("approval", record())], &["approval"]),
        "FairValue.ListAuditEvents" => closed(
            vec![
                ("events", bounded_array(record(), 10_000)),
                ("totalEventCount", unsigned()),
                ("nextCursor", nullable(record())),
            ],
            &["events", "totalEventCount", "nextCursor"],
        ),
        "FairValue.ApproveMarketAccess" | "FairValue.GetMarketAccess" => {
            closed(vec![("marketAccess", record())], &["marketAccess"])
        }
        "Bot.GetStatus" => bot_status(),
        "Bot.Start" => closed(
            vec![
                ("state", constant("running")),
                ("provider", text()),
                ("strategyMode", enumeration(&["manual", "book_imbalance"])),
            ],
            &["state", "provider", "strategyMode"],
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
            ("targetReference", nullable(manual_paper_target_reference())),
        ])),
        "Execution.GetFills" => nullable_rows(signature(vec![
            ("sequence", unsigned()),
            ("orderId", text()),
            ("quantityLots", unsigned()),
        ])),
        "Execution.GetManualPaperTargets" => closed(
            vec![("targets", bounded_array(manual_paper_target(), 100))],
            &["targets"],
        ),
        "Execution.SubmitManualPaperDraft" => closed(
            vec![
                ("state", constant("accepted")),
                ("targetId", bounded_text(128)),
                (
                    "targetRevision",
                    bounded_unsigned_range(1, u64::from(u32::MAX)),
                ),
            ],
            &["state", "targetId", "targetRevision"],
        ),
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
        "phase"
        | "sourceId"
        | "instrumentId"
        | "stableTradeId"
        | "asOf"
        | "stateEvaluatedAt"
        | "referenceId"
        | "symbol"
        | "name"
        | "venueId"
        | "symbolVenueId"
        | "assetClass"
        | "quoteCurrency"
        | "tickSize"
        | "lotSize"
        | "availability"
        | "confidence"
        | "directoryPresence"
        | "quality"
        | "effectiveAt"
        | "availableAt"
        | "providerId"
        | "sourcePayloadSha256"
        | "matchKind"
        | "quoteAvailability" => text(),
        "book" | "quote" | "selectionReceipt" => record(),
        "bid" | "ask" | "selectedSource" | "orderBook" => nullable(record()),
        "observations" | "alternatives" => array(record()),
        "comparable" | "referenceOnly" | "isEtf" => boolean(),
        "observationCount" | "stateBidDepth" | "stateAskDepth" | "priceTicks" | "quantityLots"
        | "definitionRevision" | "roundLotSize" => integer(),
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

fn research_file_preview_column() -> Value {
    closed(
        vec![
            ("name", bounded_text(256)),
            (
                "kind",
                enumeration(&["exact_decimal", "text", "mixed", "unsupported", "null"]),
            ),
            ("nullable", boolean()),
        ],
        &["name", "kind", "nullable"],
    )
}

fn research_file_preview_cell() -> Value {
    closed(
        vec![
            (
                "kind",
                enumeration(&["text", "null", "unsupported", "missing"]),
            ),
            ("value", nullable(bounded_text(256))),
            ("truncated", boolean()),
        ],
        &["kind", "value", "truncated"],
    )
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

fn job_receipt() -> Value {
    closed(
        vec![
            ("jobId", uuid()),
            ("generation", bounded_unsigned(u64::MAX)),
            ("sequence", unsigned()),
            ("state", constant("queued")),
        ],
        &["jobId", "generation", "sequence", "state"],
    )
}

fn operations_preview() -> Value {
    closed(
        vec![
            ("previewId", uuid()),
            ("previewDigest", text()),
            ("expiresAt", timestamp()),
            ("evidence", record()),
        ],
        &["previewId", "previewDigest", "expiresAt", "evidence"],
    )
}

fn setup_plan_catalog() -> Value {
    closed(
        vec![
            ("formatVersion", unsigned()),
            ("goals", bounded_array(text(), 8)),
            ("starterPlans", bounded_array(text(), 7)),
            ("recommendedStarterPlan", text()),
        ],
        &[
            "formatVersion",
            "goals",
            "starterPlans",
            "recommendedStarterPlan",
        ],
    )
}

fn setup_accepted_plan() -> Value {
    closed(
        vec![
            ("revision", unsigned()),
            ("digest", text()),
            ("acceptedAtUnixSeconds", unsigned()),
            ("plan", setup_plan()),
        ],
        &["revision", "digest", "acceptedAtUnixSeconds", "plan"],
    )
}

fn setup_plan() -> Value {
    closed(
        vec![
            ("formatVersion", unsigned()),
            ("revision", unsigned()),
            ("selection", setup_plan_selection()),
            ("steps", fixed_array(setup_plan_step(), 11)),
        ],
        &["formatVersion", "revision", "selection", "steps"],
    )
}

fn setup_plan_selection() -> Value {
    closed(
        vec![
            ("goals", bounded_array(setup_goal(), 8)),
            ("starterPlan", setup_starter_plan()),
        ],
        &["goals", "starterPlan"],
    )
}

fn setup_plan_step() -> Value {
    closed(
        vec![
            ("id", setup_step_id()),
            ("outcome", setup_outcome()),
            (
                "disposition",
                enumeration(&["included", "available_to_finish_later"]),
            ),
            ("requiredInput", setup_required_input()),
            (
                "externalContacts",
                bounded_array(setup_external_contact(), 8),
            ),
            ("reversibleLocalChange", nullable(setup_reversible_change())),
            ("expectedActiveMinutes", unsigned()),
            ("diskImpact", setup_disk_impact()),
            (
                "safeSkip",
                enumeration(&[
                    "not_skippable",
                    "capability_remains_installed_and_available",
                ]),
            ),
            ("choice", setup_step_choice()),
        ],
        &[
            "id",
            "outcome",
            "disposition",
            "requiredInput",
            "externalContacts",
            "reversibleLocalChange",
            "expectedActiveMinutes",
            "diskImpact",
            "safeSkip",
            "choice",
        ],
    )
}

fn setup_step_choice() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("goals")),
                ("starter_plan", setup_starter_plan()),
                ("goals", bounded_array(setup_goal(), 8)),
            ],
            &["kind", "starter_plan", "goals"],
        ),
        closed(
            vec![
                ("kind", constant("storage")),
                ("retention_days", unsigned()),
                ("workspace_soft_limit_bytes", unsigned()),
                (
                    "time_policy",
                    constant("point_in_time_with_first_observed_locally_provenance"),
                ),
            ],
            &[
                "kind",
                "retention_days",
                "workspace_soft_limit_bytes",
                "time_policy",
            ],
        ),
        closed(
            vec![
                ("kind", constant("providers")),
                ("outcomes", bounded_array(setup_provider_outcome(), 6)),
            ],
            &["kind", "outcomes"],
        ),
        closed(
            vec![
                ("kind", constant("imports")),
                ("formats", bounded_array(setup_import_format(), 5)),
                ("preserve_source_identity", boolean()),
                ("require_reconciliation_receipt", boolean()),
            ],
            &[
                "kind",
                "formats",
                "preserve_source_identity",
                "require_reconciliation_receipt",
            ],
        ),
        closed(
            vec![
                ("kind", constant("model_runtime")),
                ("managed_python", boolean()),
                ("native_inference", boolean()),
                ("onnx_inference", boolean()),
            ],
            &[
                "kind",
                "managed_python",
                "native_inference",
                "onnx_inference",
            ],
        ),
        closed(
            vec![
                ("kind", constant("paper_risk")),
                ("starts_stopped", boolean()),
                ("paper_only", boolean()),
                ("central_risk_required", boolean()),
            ],
            &[
                "kind",
                "starts_stopped",
                "paper_only",
                "central_risk_required",
            ],
        ),
        setup_client_choice("claude_code"),
        setup_client_choice("codex"),
        closed(
            vec![
                ("kind", constant("backup")),
                ("retention_count", unsigned()),
                ("verify_after_create", boolean()),
            ],
            &["kind", "retention_count", "verify_after_create"],
        ),
        closed(
            vec![
                ("kind", constant("review")),
                ("show_gaps_and_reversible_changes", boolean()),
            ],
            &["kind", "show_gaps_and_reversible_changes"],
        ),
        closed(
            vec![
                ("kind", constant("first_useful_result")),
                ("result", setup_first_result()),
                ("target_minutes", unsigned()),
            ],
            &["kind", "result", "target_minutes"],
        ),
    ])
}

fn setup_client_choice(kind: &'static str) -> Value {
    closed(
        vec![
            ("kind", constant(kind)),
            ("separate_client_credential", boolean()),
            ("require_real_safe_read", boolean()),
        ],
        &[
            "kind",
            "separate_client_credential",
            "require_real_safe_read",
        ],
    )
}

fn setup_time_estimate() -> Value {
    closed(
        vec![
            ("expectedActiveMinutes", unsigned()),
            ("firstUseTargetMinutes", unsigned()),
            ("includesExternalWait", boolean()),
        ],
        &[
            "expectedActiveMinutes",
            "firstUseTargetMinutes",
            "includesExternalWait",
        ],
    )
}

fn setup_disk_estimate() -> Value {
    closed(
        vec![
            ("workspaceSoftLimitBytes", unsigned()),
            ("includedImpacts", bounded_array(setup_disk_impact(), 3)),
        ],
        &["workspaceSoftLimitBytes", "includedImpacts"],
    )
}

fn setup_goal() -> Value {
    enumeration(&[
        "everything_recommended",
        "explore_public_markets",
        "research_investments",
        "manage_portfolio",
        "build_and_evaluate_models",
        "practice_paper_execution",
        "use_claude_code",
        "use_codex",
    ])
}

fn setup_starter_plan() -> Value {
    enumeration(&[
        "everything_recommended",
        "public_markets",
        "research",
        "portfolio",
        "models",
        "paper_practice",
        "ai_clients",
    ])
}

fn setup_step_id() -> Value {
    enumeration(&[
        "goals_and_starter_plan",
        "storage_retention_time_and_disk",
        "public_and_zero_fee_providers",
        "file_and_portfolio_import",
        "model_runtime",
        "paper_and_risk",
        "claude_code",
        "codex",
        "backup",
        "review",
        "first_useful_result",
    ])
}

fn setup_outcome() -> Value {
    enumeration(&[
        "durable_resumable_plan",
        "governed_workspace_budget",
        "quality_labeled_provider_evidence",
        "receipt_bound_local_data",
        "verified_local_model_runtime",
        "stopped_paper_under_central_risk",
        "verified_claude_code_mcp",
        "verified_codex_mcp",
        "verified_recovery_point",
        "capability_gap_review",
        "first_useful_result",
    ])
}

fn setup_required_input() -> Value {
    enumeration(&[
        "none",
        "local_confirmation",
        "local_disk",
        "zero_fee_account_or_provider_key",
        "owned_file",
        "detected_local_client",
    ])
}

fn setup_external_contact() -> Value {
    enumeration(&[
        "coinbase_public_api",
        "kraken_public_api",
        "securities_and_exchange_commission",
        "bureau_of_labor_statistics",
        "united_states_treasury",
        "federal_reserve_bank_of_st_louis",
        "claude_code_official_cli",
        "codex_official_cli",
    ])
}

fn setup_reversible_change() -> Value {
    enumeration(&[
        "accept_workspace_plan",
        "configure_workspace_retention_and_budget",
        "activate_or_remove_provider_sessions",
        "import_or_remove_derived_local_data",
        "configure_or_reset_model_runtime",
        "configure_stopped_paper_account_and_risk_defaults",
        "register_or_disconnect_claude_code",
        "register_or_disconnect_codex",
        "create_or_remove_backup_policy",
    ])
}

fn setup_disk_impact() -> Value {
    enumeration(&[
        "no_additional_product_bytes",
        "variable_within_workspace_soft_limit",
        "variable_backup_destination",
    ])
}

fn setup_provider_outcome() -> Value {
    enumeration(&[
        "coinbase_public_market_snapshot",
        "kraken_public_market_snapshot",
        "sec_filing_research",
        "bls_macro_research",
        "treasury_rates_research",
        "fred_alfred_authorized_research",
    ])
}

fn setup_import_format() -> Value {
    enumeration(&["csv", "json", "ndjson", "parquet", "portfolio_file"])
}

fn setup_first_result() -> Value {
    enumeration(&[
        "verified_public_market_snapshot",
        "point_in_time_research_result",
        "reconciled_portfolio_summary",
        "admitted_model_forecast",
        "stopped_paper_and_risk_review",
        "verified_mcp_safe_read",
    ])
}

fn setup_capability() -> Value {
    enumeration(&[
        "managed_workspace",
        "retention_and_disk_budget",
        "public_market_data",
        "filing_research",
        "macro_research",
        "controlled_file_import",
        "portfolio_import",
        "managed_python_runtime",
        "native_model_inference",
        "onnx_model_inference",
        "paper_only_execution",
        "central_risk",
        "claude_code_mcp",
        "codex_mcp",
        "verified_backup",
        "capability_review",
        "first_useful_result",
    ])
}

fn source_lifecycle_receipt() -> Value {
    signature(vec![
        ("operationId", text()),
        ("provider", text()),
        ("action", text()),
        ("disposition", text()),
        ("state", text()),
        ("stateRevision", unsigned()),
        ("previousGeneration", nullable(unsigned())),
        ("currentGeneration", nullable(unsigned())),
        ("runtimeGenerationSha256", nullable(text())),
        ("coverage", nullable(text())),
        ("integrity", nullable(text())),
        ("quality", nullable(text())),
        ("rateBudget", record()),
        ("authorization", text()),
        ("availability", text()),
        ("rightsEvidence", nullable(record())),
        ("blocker", nullable(text())),
        ("publicConfigurationSha256", nullable(text())),
        ("observedAt", timestamp()),
    ])
}

fn source_lifecycle_status() -> Value {
    closed(
        vec![
            ("provider", text()),
            ("stateRevision", unsigned()),
            (
                "state",
                enumeration(&[
                    "stopped",
                    "starting",
                    "active",
                    "resynchronizing",
                    "blocked",
                    "removed",
                ]),
            ),
            ("configurationSessionId", nullable(uuid())),
            ("currentGeneration", nullable(unsigned())),
            ("runtimeGenerationSha256", nullable(text())),
            ("publicConfigurationSha256", nullable(text())),
            (
                "blocker",
                nullable(enumeration(&[
                    "credential",
                    "rights",
                    "rate_budget",
                    "integrity",
                    "provider_availability",
                    "reconciliation",
                    "stale_precondition",
                ])),
            ),
            ("observedAt", timestamp()),
        ],
        &[
            "provider",
            "stateRevision",
            "state",
            "configurationSessionId",
            "currentGeneration",
            "runtimeGenerationSha256",
            "publicConfigurationSha256",
            "blocker",
            "observedAt",
        ],
    )
}

fn portfolio_advanced_report() -> Value {
    signature(vec![
        ("accountId", text()),
        ("revisionId", text()),
        ("policy", text()),
        ("effectiveAtUnixNanos", text()),
        ("availableAtUnixNanos", nullable(text())),
        ("markEvidence", record()),
    ])
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

fn forecast_vintage() -> Value {
    signature(vec![
        ("vintageId", text()),
        ("requestHash", text()),
        ("instrumentId", text()),
        ("modelId", text()),
        ("bundleId", text()),
        ("bundleVersion", unsigned()),
        ("observedThroughUnixNanos", integer()),
        ("availableAtUnixNanos", integer()),
        ("createdAtUnixNanos", integer()),
        ("expiresAtUnixNanos", integer()),
        ("horizonPoints", unsigned()),
        ("horizonStepNanos", unsigned()),
        ("quality", text()),
        ("observedHistory", bounded_array(record(), 4_096)),
        ("points", bounded_array(record(), 4_096)),
        ("driftMonitoring", record()),
        ("calibration", nullable(record())),
        ("limitations", bounded_array(text(), 256)),
        ("unavailableReason", nullable(text())),
        ("controlledArtifact", nullable(record())),
    ])
}

fn backtest_preparation_options() -> Value {
    closed(
        vec![
            (
                "datasets",
                bounded_array(
                    closed(
                        vec![
                            ("id", bounded_text(256)),
                            ("label", bounded_text(160)),
                            ("immutableGeneration", bounded_text(64)),
                            ("instrumentCount", unsigned()),
                            (
                                "periods",
                                bounded_array(
                                    closed(
                                        vec![
                                            ("id", bounded_text(256)),
                                            ("label", bounded_text(256)),
                                            ("startsAt", timestamp()),
                                            ("endsAt", timestamp()),
                                        ],
                                        &["id", "label", "startsAt", "endsAt"],
                                    ),
                                    2,
                                ),
                            ),
                        ],
                        &[
                            "id",
                            "label",
                            "immutableGeneration",
                            "instrumentCount",
                            "periods",
                        ],
                    ),
                    4_096,
                ),
            ),
            ("strategies", bounded_array(backtest_named_option(), 16)),
            ("costPolicies", bounded_array(backtest_named_option(), 16)),
            ("seeds", bounded_array(backtest_named_option(), 16)),
            ("portfolios", bounded_array(backtest_named_option(), 16)),
            ("comparisons", bounded_array(backtest_named_option(), 16)),
            ("defaultLimitPolicy", bounded_text(1_024)),
        ],
        &[
            "datasets",
            "strategies",
            "costPolicies",
            "seeds",
            "portfolios",
            "comparisons",
            "defaultLimitPolicy",
        ],
    )
}

fn backtest_named_option() -> Value {
    closed(
        vec![
            ("id", bounded_text(256)),
            ("label", bounded_text(256)),
            ("description", bounded_text(1_024)),
        ],
        &["id", "label", "description"],
    )
}

fn backtest_preparation_preview() -> Value {
    closed(
        vec![
            (
                "receipt",
                closed(
                    vec![("receiptId", uuid()), ("preparationDigest", sha256())],
                    &["receiptId", "preparationDigest"],
                ),
            ),
            ("expiresAt", timestamp()),
            ("dataset", bounded_text(512)),
            ("period", bounded_text(512)),
            ("strategy", bounded_text(256)),
            ("costPolicy", bounded_text(256)),
            ("deterministicSeed", bounded_text(256)),
            ("portfolio", bounded_text(256)),
            ("comparison", bounded_text(256)),
            ("evidence", bounded_array(bounded_text(2_048), 64)),
            ("assumptions", bounded_array(bounded_text(2_048), 64)),
        ],
        &[
            "receipt",
            "expiresAt",
            "dataset",
            "period",
            "strategy",
            "costPolicy",
            "deterministicSeed",
            "portfolio",
            "comparison",
            "evidence",
            "assumptions",
        ],
    )
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

fn manual_paper_target() -> Value {
    closed(
        vec![
            ("targetId", bounded_text(128)),
            (
                "targetRevision",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("instrumentId", uuid()),
            ("status", constant("active")),
            ("thesis", bounded_text(4_096)),
            ("expiresAt", integer()),
            ("reviewDueAt", integer()),
            (
                "route",
                closed(
                    vec![("venueId", bounded_text(128)), ("instrumentId", uuid())],
                    &["venueId", "instrumentId"],
                ),
            ),
            ("ladder", fixed_array(manual_paper_ladder_entry(), 10)),
        ],
        &[
            "targetId",
            "targetRevision",
            "instrumentId",
            "status",
            "thesis",
            "expiresAt",
            "reviewDueAt",
            "route",
            "ladder",
        ],
    )
}

fn manual_paper_ladder_entry() -> Value {
    closed(
        vec![
            (
                "level",
                enumeration(&[
                    "downside",
                    "add",
                    "entry_lower",
                    "entry_upper",
                    "base",
                    "trim_lower",
                    "trim_upper",
                    "exit_lower",
                    "exit_upper",
                    "upside",
                ]),
            ),
            ("label", bounded_text(96)),
            ("value", money()),
        ],
        &["level", "label", "value"],
    )
}

fn manual_paper_target_reference() -> Value {
    closed(
        vec![
            ("targetId", bounded_text(128)),
            ("revision", bounded_unsigned_range(1, u64::MAX)),
            ("contentSha256", sha256()),
        ],
        &["targetId", "revision", "contentSha256"],
    )
}

fn job_view() -> Value {
    closed(
        vec![
            ("jobId", uuid()),
            ("generation", bounded_unsigned_range(1, u64::MAX)),
            ("sequence", unsigned()),
            ("kind", text()),
            ("state", text()),
            ("phase", nullable(text())),
            ("completedUnits", nullable(unsigned())),
            ("totalUnits", nullable(unsigned())),
            ("cancellationRequested", boolean()),
            ("result", nullable(record())),
            ("failure", nullable(record())),
            ("updatedAt", integer()),
            ("recovery", nullable(text())),
        ],
        &[
            "jobId",
            "generation",
            "sequence",
            "kind",
            "state",
            "phase",
            "completedUnits",
            "totalUnits",
            "cancellationRequested",
            "result",
            "failure",
            "updatedAt",
            "recovery",
        ],
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
                ("strategyMode", enumeration(&["manual", "book_imbalance"])),
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
                "strategyMode",
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

fn data_use_right() -> Value {
    closed(
        vec![
            (
                "operation",
                enumeration(&[
                    "retrieve",
                    "display",
                    "persist",
                    "model_training",
                    "export",
                    "redistribute",
                ]),
            ),
            (
                "admission",
                enumeration(&["admitted", "pending", "blocked"]),
            ),
        ],
        &["operation", "admission"],
    )
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

fn sha256() -> Value {
    json!({
        "type": "string",
        "minLength": 64,
        "maxLength": 64
    })
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
