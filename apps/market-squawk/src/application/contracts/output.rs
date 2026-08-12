//! Code-owned structured-result schema families for the production operation registry.

use serde_json::{Map, Value, json};

use market_squawk_adapter_federal_reserve::{
    BOARD_DDP_SOURCE_ID, h15_treasury_constant_maturities_dashboard_series,
};
use market_squawk_decisions::{
    RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED, RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM,
};
use market_squawk_sources::FRED_ALFRED_API_SURFACE_ID;

pub(super) fn output_data_schema(operation: &str) -> Option<Value> {
    let schema = match operation {
        "Source.ImportCredentialBundle" => closed(
            vec![
                ("schema", constant("market-squawk-provider-credentials/v1")),
                (
                    "providers",
                    bounded_array(provider_credential_import_disposition(), 17),
                ),
            ],
            &["schema", "providers"],
        ),
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
        "Market.GetUnifiedFeed" => unified_market_rows(),
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
        | "Fundamental.GetRatios" => observation_result(),
        "Macro.GetDashboard" => macro_dashboard(),
        "Macro.ListSeries"
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
        "Portfolio.GetRecommendationSetup" => recommendation_setup_status(),
        "Portfolio.PreviewRecommendationSetup" => recommendation_setup_preview(),
        "Portfolio.CommitRecommendationSetup" => recommendation_setup_receipt(),
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
        | "Portfolio.ProposeRebalance" => portfolio_advanced_report(),
        "Portfolio.EvaluateCandidateImpact" => portfolio_candidate_impact(),
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
        "Model.SelectLatestValidForecast" => latest_valid_forecast(),
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
        "Decision.GetInvestmentAnalysis" => investment_analysis(),
        "Decision.ListInvestmentAnalyses" => investment_analysis_page(),
        "Decision.GetRecommendationTrackRecord" => recommendation_track_record(),
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

fn recommendation_setup_status() -> Value {
    closed(
        vec![
            ("workspaceId", uuid()),
            ("state", enumeration(&["ready", "setup_required"])),
            (
                "setupRequiredReason",
                nullable(enumeration(&[
                    "no_default_account",
                    "ambiguous_accounts",
                    "portfolio_evidence_unavailable",
                    "profile_review_required",
                ])),
            ),
            ("authority", recommendation_setup_authority()),
            (
                "accountSelection",
                nullable(recommendation_selected_account()),
            ),
            (
                "allocationProfile",
                nullable(recommendation_allocation_profile()),
            ),
            ("portfolioCatalog", recommendation_portfolio_catalog()),
        ],
        &[
            "workspaceId",
            "state",
            "setupRequiredReason",
            "authority",
            "accountSelection",
            "allocationProfile",
            "portfolioCatalog",
        ],
    )
}

fn recommendation_setup_authority() -> Value {
    closed(
        vec![
            ("revision", unsigned()),
            ("digest", sha256()),
            ("transitionAtUnixNanos", nullable(integer_text())),
            ("configurationDigest", nullable(sha256())),
        ],
        &[
            "revision",
            "digest",
            "transitionAtUnixNanos",
            "configurationDigest",
        ],
    )
}

fn recommendation_selected_account() -> Value {
    closed(
        vec![
            ("setupRevision", bounded_unsigned_range(1, u64::MAX)),
            ("accountId", uuid()),
            ("reportingCurrency", recommendation_currency()),
            ("confirmedPortfolioRevisionSha256", sha256()),
            ("confirmedCatalogDigestSha256", sha256()),
            ("confirmedAtUnixNanos", integer_text()),
            ("digest", sha256()),
        ],
        &[
            "setupRevision",
            "accountId",
            "reportingCurrency",
            "confirmedPortfolioRevisionSha256",
            "confirmedCatalogDigestSha256",
            "confirmedAtUnixNanos",
            "digest",
        ],
    )
}

fn recommendation_allocation_profile() -> Value {
    closed(
        vec![
            ("setupRevision", bounded_unsigned_range(1, u64::MAX)),
            ("accountId", uuid()),
            ("reportingCurrency", recommendation_currency()),
            (
                "preferredPositionWeightLowerBps",
                bounded_unsigned_range(1, 10_000),
            ),
            (
                "preferredPositionWeightUpperBps",
                bounded_unsigned_range(1, 10_000),
            ),
            ("minimumCashReserve", recommendation_money()),
            (
                "maximumDownsideLossBpsOfMarkedEquity",
                bounded_unsigned_range(1, 10_000),
            ),
            ("availableInvestmentHorizonNanos", positive_integer_text()),
            ("acceptedAtUnixNanos", integer_text()),
            ("reviewDueAtUnixNanos", integer_text()),
            ("digest", sha256()),
        ],
        &[
            "setupRevision",
            "accountId",
            "reportingCurrency",
            "preferredPositionWeightLowerBps",
            "preferredPositionWeightUpperBps",
            "minimumCashReserve",
            "maximumDownsideLossBpsOfMarkedEquity",
            "availableInvestmentHorizonNanos",
            "acceptedAtUnixNanos",
            "reviewDueAtUnixNanos",
            "digest",
        ],
    )
}

fn recommendation_portfolio_catalog() -> Value {
    closed(
        vec![
            ("digest", sha256()),
            ("accountCount", bounded_unsigned(256)),
            (
                "accounts",
                bounded_array(
                    closed(
                        vec![
                            ("accountId", uuid()),
                            ("portfolioRevisionSha256", sha256()),
                            ("reportingCurrency", recommendation_currency()),
                            ("effectiveAtUnixNanos", integer_text()),
                            ("availableAtUnixNanos", nullable(integer_text())),
                            ("sourceId", bounded_text(256)),
                            ("sourceCoverage", bounded_array(bounded_text(256), 4_096)),
                            ("artifactSha256", sha256()),
                        ],
                        &[
                            "accountId",
                            "portfolioRevisionSha256",
                            "reportingCurrency",
                            "effectiveAtUnixNanos",
                            "availableAtUnixNanos",
                            "sourceId",
                            "sourceCoverage",
                            "artifactSha256",
                        ],
                    ),
                    256,
                ),
            ),
        ],
        &["digest", "accountCount", "accounts"],
    )
}

fn recommendation_setup_preview() -> Value {
    closed(
        vec![
            ("workspaceId", uuid()),
            ("previewId", uuid()),
            ("previewDigest", sha256()),
            ("currentRevision", unsigned()),
            ("resultingRevision", bounded_unsigned_range(1, u64::MAX)),
            ("currentAuthorityDigest", sha256()),
            ("catalogDigest", sha256()),
            ("kind", constant("configure")),
            (
                "accountSelection",
                closed(
                    vec![
                        ("accountId", uuid()),
                        ("portfolioRevisionSha256", sha256()),
                        ("reportingCurrency", recommendation_currency()),
                    ],
                    &["accountId", "portfolioRevisionSha256", "reportingCurrency"],
                ),
            ),
            (
                "allocationProfile",
                closed(
                    vec![
                        (
                            "preferredPositionWeightLowerBps",
                            bounded_unsigned_range(1, 10_000),
                        ),
                        (
                            "preferredPositionWeightUpperBps",
                            bounded_unsigned_range(1, 10_000),
                        ),
                        ("minimumCashReserve", recommendation_money()),
                        (
                            "maximumDownsideLossBpsOfMarkedEquity",
                            bounded_unsigned_range(1, 10_000),
                        ),
                        ("availableInvestmentHorizonNanos", positive_integer_text()),
                    ],
                    &[
                        "preferredPositionWeightLowerBps",
                        "preferredPositionWeightUpperBps",
                        "minimumCashReserve",
                        "maximumDownsideLossBpsOfMarkedEquity",
                        "availableInvestmentHorizonNanos",
                    ],
                ),
            ),
            ("issuedAtUnixNanos", integer_text()),
            ("expiresAtUnixNanos", integer_text()),
        ],
        &[
            "workspaceId",
            "previewId",
            "previewDigest",
            "currentRevision",
            "resultingRevision",
            "currentAuthorityDigest",
            "catalogDigest",
            "kind",
            "accountSelection",
            "allocationProfile",
            "issuedAtUnixNanos",
            "expiresAtUnixNanos",
        ],
    )
}

fn recommendation_setup_receipt() -> Value {
    closed(
        vec![
            ("workspaceId", uuid()),
            ("revision", bounded_unsigned_range(1, u64::MAX)),
            ("authorityDigest", sha256()),
            ("configured", constant_bool(true)),
            ("acceptedAtUnixNanos", integer_text()),
        ],
        &[
            "workspaceId",
            "revision",
            "authorityDigest",
            "configured",
            "acceptedAtUnixNanos",
        ],
    )
}

fn recommendation_money() -> Value {
    closed(
        vec![
            ("amount", canonical_decimal_text()),
            ("currency", recommendation_currency()),
        ],
        &["amount", "currency"],
    )
}

fn recommendation_currency() -> Value {
    json!({
        "type": "string",
        "minLength": 3,
        "maxLength": 3,
        "pattern": "^[A-Z]{3}$",
    })
}

fn provider_credential_import_disposition() -> Value {
    closed(
        vec![
            ("provider", bounded_text(128)),
            ("enabled", boolean()),
            (
                "disposition",
                enumeration(&[
                    "credential_stored_unverified",
                    "probe_required",
                    "disabled",
                    "profile_unavailable",
                ]),
            ),
            ("onboardingSessionId", nullable(uuid())),
        ],
        &["provider", "enabled", "disposition", "onboardingSessionId"],
    )
}

fn investment_analysis() -> Value {
    closed(
        vec![
            ("analysisId", investment_analysis_sha256()),
            (
                "executionEligibility",
                constant("research_only_execution_ineligible"),
            ),
            ("policy", investment_analysis_policy()),
            ("evidence", investment_analysis_evidence()),
            ("evidenceDigest", investment_analysis_sha256()),
            ("publication", nullable(investment_analysis_publication())),
            (
                "projection",
                nullable(investment_analysis_outcome_projection()),
            ),
            ("sizing", nullable(investment_analysis_sizing_projection())),
            (
                "realizedOutcome",
                nullable(recommendation_outcome_current()),
            ),
            ("result", investment_analysis_result()),
        ],
        &[
            "analysisId",
            "executionEligibility",
            "policy",
            "evidence",
            "evidenceDigest",
            "publication",
            "projection",
            "sizing",
            "realizedOutcome",
            "result",
        ],
    )
}

fn investment_analysis_publication() -> Value {
    closed(
        vec![
            ("publicationId", investment_analysis_sha256()),
            ("publishedAt", integer()),
            (
                "executionEligibility",
                constant("research_only_execution_ineligible"),
            ),
            (
                "analyticalProfile",
                investment_analysis_analytical_profile(),
            ),
            ("workflow", investment_analysis_workflow()),
            (
                "accountSetup",
                closed(
                    vec![
                        ("accountId", uuid()),
                        ("distinctFromAnalyticalProfile", constant_bool(true)),
                    ],
                    &["accountId", "distinctFromAnalyticalProfile"],
                ),
            ),
            (
                "outcomeProjectionDigest",
                nullable(investment_analysis_sha256()),
            ),
            (
                "sizingProjectionDigest",
                nullable(investment_analysis_sha256()),
            ),
        ],
        &[
            "publicationId",
            "publishedAt",
            "executionEligibility",
            "analyticalProfile",
            "workflow",
            "accountSetup",
            "outcomeProjectionDigest",
            "sizingProjectionDigest",
        ],
    )
}

fn investment_analysis_analytical_profile() -> Value {
    closed(
        vec![
            ("profileId", bounded_text(256)),
            ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("contentDigest", investment_analysis_content_identity()),
        ],
        &["profileId", "revision", "contentDigest"],
    )
}

fn investment_analysis_workflow() -> Value {
    closed(
        vec![
            ("workflowId", bounded_text(256)),
            ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("contentDigest", investment_analysis_content_identity()),
        ],
        &["workflowId", "revision", "contentDigest"],
    )
}

fn investment_analysis_outcome_projection() -> Value {
    closed(
        vec![
            ("resultDigest", investment_analysis_sha256()),
            ("proposalId", investment_analysis_sha256()),
            ("derivationDigest", investment_analysis_sha256()),
            (
                "authority",
                constant("analysis_only_no_mutation_no_execution"),
            ),
            ("executionEligible", constant_bool(false)),
            ("mark", investment_analysis_money()),
            ("horizonAt", integer()),
            ("downside", investment_analysis_gross_mark_relative_range()),
            ("base", investment_analysis_gross_mark_relative_range()),
            ("upside", investment_analysis_gross_mark_relative_range()),
            (
                "netPnl",
                investment_analysis_unavailable_disclosure(
                    "exact_forward_cost_evidence_not_supplied",
                ),
            ),
            (
                "benchmarkReturn",
                investment_analysis_unavailable_disclosure(
                    "exact_proposal_time_benchmark_evidence_not_supplied",
                ),
            ),
            (
                "afterTaxPnl",
                investment_analysis_unavailable_disclosure("exact_tax_evidence_not_supplied"),
            ),
        ],
        &[
            "resultDigest",
            "proposalId",
            "derivationDigest",
            "authority",
            "executionEligible",
            "mark",
            "horizonAt",
            "downside",
            "base",
            "upside",
            "netPnl",
            "benchmarkReturn",
            "afterTaxPnl",
        ],
    )
}

fn investment_analysis_gross_mark_relative_range() -> Value {
    closed(
        vec![
            ("priceRange", investment_analysis_price_range()),
            (
                "grossReturnFromMark",
                closed(
                    vec![
                        ("lowerNumerator", investment_analysis_money()),
                        ("upperNumerator", investment_analysis_money()),
                        ("denominator", investment_analysis_money()),
                    ],
                    &["lowerNumerator", "upperNumerator", "denominator"],
                ),
            ),
        ],
        &["priceRange", "grossReturnFromMark"],
    )
}

fn investment_analysis_sizing_projection() -> Value {
    closed(
        vec![
            ("resultDigest", investment_analysis_sha256()),
            ("proposalId", investment_analysis_sha256()),
            ("derivationDigest", investment_analysis_sha256()),
            (
                "authority",
                constant("analysis_only_no_mutation_no_execution"),
            ),
            ("executionEligible", constant_bool(false)),
            ("evaluatedAt", integer()),
            ("currentLots", bounded_unsigned(i64::MAX as u64)),
            ("hardFeasibleLots", investment_analysis_feasible_lots()),
            ("preferredFeasibleLots", investment_analysis_feasible_lots()),
            ("selectedTargetLots", null()),
            ("orderQuantity", null()),
        ],
        &[
            "resultDigest",
            "proposalId",
            "derivationDigest",
            "authority",
            "executionEligible",
            "evaluatedAt",
            "currentLots",
            "hardFeasibleLots",
            "preferredFeasibleLots",
            "selectedTargetLots",
            "orderQuantity",
        ],
    )
}

fn investment_analysis_feasible_lots() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("available")),
                ("lower", bounded_unsigned(i64::MAX as u64)),
                ("upper", bounded_unsigned(i64::MAX as u64)),
            ],
            &["kind", "lower", "upper"],
        ),
        closed(
            vec![
                ("kind", constant("unavailable")),
                (
                    "reasons",
                    bounded_nonempty_array(investment_analysis_sizing_unavailable_reason(), 8),
                ),
            ],
            &["kind", "reasons"],
        ),
    ])
}

fn investment_analysis_sizing_unavailable_reason() -> Value {
    enumeration(&[
        "capacity_not_supplied",
        "capacity_not_yet_available",
        "capacity_expired",
        "capacity_range_contains_no_lots",
        "cash_reserve_exceeds_gross_liquidatable_value",
        "no_hard_feasible_lot_intersection",
        "preferred_weight_range_contains_no_lots",
        "no_preferred_feasible_lot_intersection",
    ])
}

fn recommendation_outcome_current() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("pending")),
                (
                    "reason",
                    enumeration(&["awaiting_horizon", "awaiting_outcome_evidence"]),
                ),
                ("seriesId", investment_analysis_sha256()),
                ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
                ("statusDigest", investment_analysis_sha256()),
                ("evaluatedAt", integer()),
                ("executionEligible", constant_bool(false)),
            ],
            &[
                "kind",
                "reason",
                "seriesId",
                "revision",
                "statusDigest",
                "evaluatedAt",
                "executionEligible",
            ],
        ),
        closed(
            vec![
                ("kind", constant("unavailable")),
                (
                    "reason",
                    enumeration(&[
                        "analysis_unavailable",
                        "outcome_observation_unavailable",
                        "ambiguous_outcome_observation",
                        "incomplete_outcome_observation",
                        "corporate_action_evidence_unavailable",
                    ]),
                ),
                ("seriesId", investment_analysis_sha256()),
                ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
                ("statusDigest", investment_analysis_sha256()),
                ("evaluatedAt", integer()),
                ("executionEligible", constant_bool(false)),
            ],
            &[
                "kind",
                "reason",
                "seriesId",
                "revision",
                "statusDigest",
                "evaluatedAt",
                "executionEligible",
            ],
        ),
        closed(
            vec![
                ("kind", constant("completed")),
                ("metric", constant("gross_instrument_price_return")),
                ("startMark", investment_analysis_money()),
                ("endpointPrice", investment_analysis_money()),
                ("grossPriceReturn", canonical_decimal_text()),
                ("observedAt", integer()),
                ("availableAt", integer()),
                (
                    "selectionReceiptIdentity",
                    investment_analysis_content_identity(),
                ),
                (
                    "selectedObservationIdentity",
                    investment_analysis_content_identity(),
                ),
                (
                    "corporateActionEvidenceIdentity",
                    investment_analysis_content_identity(),
                ),
                (
                    "netReturn",
                    investment_analysis_unavailable_disclosure(
                        "exact_realized_cost_evidence_not_supplied",
                    ),
                ),
                (
                    "benchmarkReturn",
                    investment_analysis_unavailable_disclosure(
                        "exact_benchmark_outcome_evidence_not_supplied",
                    ),
                ),
                (
                    "afterTaxReturn",
                    investment_analysis_unavailable_disclosure("exact_tax_evidence_not_supplied"),
                ),
                (
                    "settlement",
                    investment_analysis_unavailable_disclosure(
                        "no_execution_or_settlement_evidence",
                    ),
                ),
                ("seriesId", investment_analysis_sha256()),
                ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
                ("statusDigest", investment_analysis_sha256()),
                ("evaluatedAt", integer()),
                ("executionEligible", constant_bool(false)),
            ],
            &[
                "kind",
                "metric",
                "startMark",
                "endpointPrice",
                "grossPriceReturn",
                "observedAt",
                "availableAt",
                "selectionReceiptIdentity",
                "selectedObservationIdentity",
                "corporateActionEvidenceIdentity",
                "netReturn",
                "benchmarkReturn",
                "afterTaxReturn",
                "settlement",
                "seriesId",
                "revision",
                "statusDigest",
                "evaluatedAt",
                "executionEligible",
            ],
        ),
    ])
}

fn investment_analysis_unavailable_disclosure(reason: &'static str) -> Value {
    closed(
        vec![
            ("kind", constant("unavailable")),
            ("reason", constant(reason)),
        ],
        &["kind", "reason"],
    )
}

fn recommendation_track_record() -> Value {
    closed(
        vec![
            (
                "analyticalProfile",
                investment_analysis_analytical_profile(),
            ),
            ("horizonNanos", bounded_integer_range(1, i64::MAX)),
            ("evaluatedAt", integer()),
            (
                "analysisUnavailableCount",
                bounded_unsigned(u64::from(u32::MAX)),
            ),
            (
                "minimumCompletedSamples",
                constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED)),
            ),
            (
                "minimumCoveragePpm",
                constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM)),
            ),
            (
                "groups",
                fixed_array(recommendation_track_record_group(), 6),
            ),
            ("forecastCalibrationIncluded", constant_bool(false)),
            ("executionPerformanceIncluded", constant_bool(false)),
        ],
        &[
            "analyticalProfile",
            "horizonNanos",
            "evaluatedAt",
            "analysisUnavailableCount",
            "minimumCompletedSamples",
            "minimumCoveragePpm",
            "groups",
            "forecastCalibrationIncluded",
            "executionPerformanceIncluded",
        ],
    )
}

fn recommendation_track_record_group() -> Value {
    closed(
        vec![
            (
                "cohort",
                enumeration(&["buy", "add", "hold", "trim", "sell", "no_action_control"]),
            ),
            ("publicationCount", bounded_unsigned(u64::from(u32::MAX))),
            ("dueCount", bounded_unsigned(u64::from(u32::MAX))),
            ("completedCount", bounded_unsigned(u64::from(u32::MAX))),
            ("pendingCount", bounded_unsigned(u64::from(u32::MAX))),
            ("unavailableCount", bounded_unsigned(u64::from(u32::MAX))),
            ("coveragePpm", bounded_unsigned(1_000_000)),
            ("performance", recommendation_track_record_performance()),
        ],
        &[
            "cohort",
            "publicationCount",
            "dueCount",
            "completedCount",
            "pendingCount",
            "unavailableCount",
            "coveragePpm",
            "performance",
        ],
    )
}

fn recommendation_track_record_performance() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("unavailable")),
                ("reason", constant("no_due_outcomes")),
            ],
            &["kind", "reason"],
        ),
        closed(
            vec![
                ("kind", constant("unavailable")),
                ("reason", constant("insufficient_completed_samples")),
                (
                    "required",
                    constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED)),
                ),
                ("actual", bounded_unsigned(u64::from(u32::MAX))),
            ],
            &["kind", "reason", "required", "actual"],
        ),
        closed(
            vec![
                ("kind", constant("unavailable")),
                ("reason", constant("insufficient_coverage")),
                (
                    "requiredPpm",
                    constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM)),
                ),
                ("actualPpm", bounded_unsigned(1_000_000)),
            ],
            &["kind", "reason", "requiredPpm", "actualPpm"],
        ),
        closed(
            vec![
                ("kind", constant("available")),
                ("metric", constant("mean_gross_instrument_price_return")),
                ("meanGrossPriceReturn", canonical_decimal_text()),
                ("positiveOutcomes", bounded_unsigned(u64::from(u32::MAX))),
                ("zeroOutcomes", bounded_unsigned(u64::from(u32::MAX))),
                ("negativeOutcomes", bounded_unsigned(u64::from(u32::MAX))),
            ],
            &[
                "kind",
                "metric",
                "meanGrossPriceReturn",
                "positiveOutcomes",
                "zeroOutcomes",
                "negativeOutcomes",
            ],
        ),
    ])
}

fn investment_analysis_page() -> Value {
    closed(
        vec![
            ("completeness", enumeration(&["complete", "truncated"])),
            ("returnedCount", bounded_unsigned(1_000)),
            ("availableCount", bounded_unsigned(4_096)),
            (
                "nextAfterAnalysisId",
                nullable(investment_analysis_sha256()),
            ),
            (
                "analyses",
                bounded_array(investment_analysis_locator(), 1_000),
            ),
        ],
        &[
            "completeness",
            "returnedCount",
            "availableCount",
            "nextAfterAnalysisId",
            "analyses",
        ],
    )
}

fn investment_analysis_policy() -> Value {
    closed(
        vec![
            ("version", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("digest", investment_analysis_sha256()),
            (
                "actionZoneSemanticsVersion",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("horizonNanos", integer()),
            ("proposalLifetimeNanos", integer()),
            ("assumptions", fixed_array(bounded_text(4_096), 3)),
            (
                "invalidationConditions",
                fixed_array(bounded_text(4_096), 3),
            ),
            ("limitations", fixed_array(bounded_text(4_096), 3)),
        ],
        &[
            "version",
            "digest",
            "actionZoneSemanticsVersion",
            "horizonNanos",
            "proposalLifetimeNanos",
            "assumptions",
            "invalidationConditions",
            "limitations",
        ],
    )
}

fn investment_analysis_evidence() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("currency", investment_analysis_currency()),
            ("accountId", uuid()),
            ("asOf", integer()),
            ("market", nullable(investment_analysis_market_evidence())),
            (
                "priceForecast",
                nullable(investment_analysis_forecast_evidence()),
            ),
            (
                "valuation",
                nullable(investment_analysis_valuation_evidence()),
            ),
            (
                "backtest",
                nullable(investment_analysis_backtest_evidence()),
            ),
            (
                "liquidity",
                nullable(investment_analysis_liquidity_evidence()),
            ),
            (
                "portfolioRisk",
                nullable(investment_analysis_portfolio_risk_evidence()),
            ),
        ],
        &[
            "instrumentId",
            "currency",
            "accountId",
            "asOf",
            "market",
            "priceForecast",
            "valuation",
            "backtest",
            "liquidity",
            "portfolioRisk",
        ],
    )
}

fn investment_analysis_market_evidence() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("price", investment_analysis_money()),
            ("quality", investment_analysis_data_quality()),
            (
                "priceKind",
                enumeration(&["last_trade", "checked_bid_ask_midpoint"]),
            ),
            ("adjustmentBasis", constant("unadjusted_spot")),
            (
                "selectionReceiptIdentity",
                investment_analysis_content_identity(),
            ),
            (
                "selectedObservationIdentity",
                investment_analysis_content_identity(),
            ),
            ("window", investment_analysis_evidence_window()),
        ],
        &[
            "instrumentId",
            "price",
            "quality",
            "priceKind",
            "adjustmentBasis",
            "selectionReceiptIdentity",
            "selectedObservationIdentity",
            "window",
        ],
    )
}

fn investment_analysis_forecast_evidence() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("cases", investment_analysis_price_cases()),
            ("ranges", investment_analysis_forecast_ranges()),
            ("horizonAt", integer()),
            (
                "expectedTerminal",
                nullable(investment_analysis_expected_terminal()),
            ),
            ("vintageId", investment_analysis_sha256()),
            (
                "outputBindingIdentity",
                investment_analysis_content_identity(),
            ),
            (
                "calibrationIdentity",
                investment_analysis_content_identity(),
            ),
            ("outcomeSetIdentity", investment_analysis_content_identity()),
            (
                "calibration",
                closed(
                    vec![
                        ("nominalCoveragePpm", bounded_unsigned(1_000_000)),
                        ("realizedCoveragePpm", bounded_unsigned(1_000_000)),
                        (
                            "completedOutcomes",
                            bounded_unsigned_range(1, u64::from(u32::MAX)),
                        ),
                    ],
                    &[
                        "nominalCoveragePpm",
                        "realizedCoveragePpm",
                        "completedOutcomes",
                    ],
                ),
            ),
            ("window", investment_analysis_evidence_window()),
        ],
        &[
            "instrumentId",
            "cases",
            "ranges",
            "horizonAt",
            "expectedTerminal",
            "vintageId",
            "outputBindingIdentity",
            "calibrationIdentity",
            "outcomeSetIdentity",
            "calibration",
            "window",
        ],
    )
}

fn investment_analysis_expected_terminal() -> Value {
    closed(
        vec![
            ("statistic", constant("model_estimated_conditional_mean")),
            ("price", investment_analysis_money()),
            ("horizonAt", integer()),
            ("statisticIdentity", investment_analysis_content_identity()),
        ],
        &["statistic", "price", "horizonAt", "statisticIdentity"],
    )
}

fn investment_analysis_valuation_evidence() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("fairValue", investment_analysis_money()),
            ("basis", constant("per_instrument_unit")),
            ("horizonAt", integer()),
            ("measurementId", investment_analysis_sha256()),
            ("classificationDecisionId", investment_analysis_sha256()),
            ("selectionReceiptHash", investment_analysis_sha256()),
            ("window", investment_analysis_evidence_window()),
        ],
        &[
            "instrumentId",
            "fairValue",
            "basis",
            "horizonAt",
            "measurementId",
            "classificationDecisionId",
            "selectionReceiptHash",
            "window",
        ],
    )
}

fn investment_analysis_backtest_evidence() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("currency", investment_analysis_currency()),
            ("outcomeHorizonNanos", integer()),
            ("netReturnBasisPoints", integer()),
            ("maxDrawdownBasisPoints", integer()),
            ("feeBasisPoints", integer()),
            ("slippageBasisPoints", integer()),
            ("maximumRandomSlippageBasisPoints", integer()),
            (
                "observations",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("trials", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("stabilityPpm", bounded_unsigned(1_000_000)),
            ("simulationCutoffAt", integer()),
            ("datasetIdentity", investment_analysis_content_identity()),
            ("commandIdentity", investment_analysis_content_identity()),
            ("terminalIdentity", investment_analysis_content_identity()),
            ("reportIdentity", investment_analysis_content_identity()),
            ("cohortIdentity", investment_analysis_content_identity()),
            ("costModelIdentity", investment_analysis_content_identity()),
            ("window", investment_analysis_evidence_window()),
        ],
        &[
            "instrumentId",
            "currency",
            "outcomeHorizonNanos",
            "netReturnBasisPoints",
            "maxDrawdownBasisPoints",
            "feeBasisPoints",
            "slippageBasisPoints",
            "maximumRandomSlippageBasisPoints",
            "observations",
            "trials",
            "stabilityPpm",
            "simulationCutoffAt",
            "datasetIdentity",
            "commandIdentity",
            "terminalIdentity",
            "reportIdentity",
            "cohortIdentity",
            "costModelIdentity",
            "window",
        ],
    )
}

fn investment_analysis_liquidity_evidence() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("currency", investment_analysis_currency()),
            ("quotedSpreadBasisPoints", integer()),
            ("capacityPpm", bounded_unsigned(1_000_000)),
            ("quality", investment_analysis_data_quality()),
            ("assessmentIdentity", investment_analysis_content_identity()),
            ("window", investment_analysis_evidence_window()),
        ],
        &[
            "instrumentId",
            "currency",
            "quotedSpreadBasisPoints",
            "capacityPpm",
            "quality",
            "assessmentIdentity",
            "window",
        ],
    )
}

fn investment_analysis_portfolio_risk_evidence() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("accountId", uuid()),
            ("currency", investment_analysis_currency()),
            ("portfolioRevision", investment_analysis_sha256()),
            ("positionState", investment_analysis_position_state()),
            ("riskCapacityPpm", bounded_unsigned(1_000_000)),
            ("riskReportIdentity", investment_analysis_content_identity()),
            ("window", investment_analysis_evidence_window()),
        ],
        &[
            "instrumentId",
            "accountId",
            "currency",
            "portfolioRevision",
            "positionState",
            "riskCapacityPpm",
            "riskReportIdentity",
            "window",
        ],
    )
}

fn investment_analysis_position_state() -> Value {
    one_of(vec![
        closed(vec![("kind", constant("no_position"))], &["kind"]),
        closed(
            vec![
                ("kind", constant("position")),
                ("addAllowed", boolean()),
                ("trimAllowed", boolean()),
                ("exitAllowed", boolean()),
            ],
            &["kind", "addAllowed", "trimAllowed", "exitAllowed"],
        ),
    ])
}

fn investment_analysis_result() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("generated")),
                ("proposalId", investment_analysis_sha256()),
                ("derivationDigest", investment_analysis_sha256()),
                ("action", investment_analysis_action()),
                ("priceLadder", investment_analysis_price_ladder()),
                (
                    "actionZoneSemantics",
                    investment_analysis_action_zone_semantics(),
                ),
                (
                    "evidenceReliability",
                    investment_analysis_evidence_reliability(),
                ),
                ("horizonAt", integer()),
                ("expiresAt", integer()),
            ],
            &[
                "kind",
                "proposalId",
                "derivationDigest",
                "action",
                "priceLadder",
                "actionZoneSemantics",
                "evidenceReliability",
                "horizonAt",
                "expiresAt",
            ],
        ),
        closed(
            vec![
                ("kind", constant("no_action")),
                ("proposalId", investment_analysis_sha256()),
                ("derivationDigest", investment_analysis_sha256()),
                ("reason", investment_analysis_no_action_reason()),
                (
                    "invalidators",
                    fixed_array(investment_analysis_invalidator(), 1),
                ),
                (
                    "evidenceReliability",
                    investment_analysis_evidence_reliability(),
                ),
                ("horizonAt", integer()),
                ("expiresAt", integer()),
            ],
            &[
                "kind",
                "proposalId",
                "derivationDigest",
                "reason",
                "invalidators",
                "evidenceReliability",
                "horizonAt",
                "expiresAt",
            ],
        ),
        closed(
            vec![
                ("kind", constant("unavailable")),
                ("reason", investment_analysis_unavailable_reason()),
                ("horizonAt", integer()),
                ("expiresAt", integer()),
            ],
            &["kind", "reason", "horizonAt", "expiresAt"],
        ),
    ])
}

fn investment_analysis_price_ladder() -> Value {
    closed(
        vec![
            ("cases", investment_analysis_price_cases()),
            (
                "ranges",
                closed(
                    vec![
                        ("downside", investment_analysis_price_range()),
                        ("base", investment_analysis_price_range()),
                        ("upside", investment_analysis_price_range()),
                        ("entry", investment_analysis_price_range()),
                        ("add", investment_analysis_price_range()),
                        ("trim", investment_analysis_price_range()),
                        ("exit", investment_analysis_price_range()),
                    ],
                    &["downside", "base", "upside", "entry", "add", "trim", "exit"],
                ),
            ),
            ("addCase", investment_analysis_money()),
        ],
        &["cases", "ranges", "addCase"],
    )
}

fn investment_analysis_action_zone_semantics() -> Value {
    closed(
        vec![
            ("version", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("referenceZone", nullable(investment_analysis_price_range())),
            (
                "triggerFloorExclusive",
                nullable(investment_analysis_money()),
            ),
            (
                "triggerFloorInclusive",
                nullable(investment_analysis_money()),
            ),
            (
                "triggerCeilingInclusive",
                nullable(investment_analysis_money()),
            ),
        ],
        &[
            "version",
            "referenceZone",
            "triggerFloorExclusive",
            "triggerFloorInclusive",
            "triggerCeilingInclusive",
        ],
    )
}

fn investment_analysis_evidence_reliability() -> Value {
    closed(
        vec![
            (
                "meaning",
                constant("policy_weighted_evidence_reliability_v1"),
            ),
            ("valuePpm", bounded_unsigned(1_000_000)),
            (
                "components",
                fixed_array(
                    closed(
                        vec![
                            (
                                "kind",
                                enumeration(&[
                                    "forecast_calibration",
                                    "valuation_agreement",
                                    "backtest_stability",
                                    "market_integrity",
                                    "liquidity_capacity",
                                    "portfolio_risk_capacity",
                                ]),
                            ),
                            ("valuePpm", bounded_unsigned(1_000_000)),
                            ("weightPpm", bounded_unsigned(1_000_000)),
                        ],
                        &["kind", "valuePpm", "weightPpm"],
                    ),
                    6,
                ),
            ),
        ],
        &["meaning", "valuePpm", "components"],
    )
}

fn investment_analysis_locator() -> Value {
    closed(
        vec![
            ("analysisId", investment_analysis_sha256()),
            ("proposalId", nullable(investment_analysis_sha256())),
            ("derivationDigest", nullable(investment_analysis_sha256())),
            ("instrumentId", uuid()),
            ("accountId", uuid()),
            ("currency", investment_analysis_currency()),
            ("asOf", integer()),
            ("horizonAt", integer()),
            ("expiresAt", integer()),
            ("policyDigest", investment_analysis_sha256()),
            ("evidenceDigest", investment_analysis_sha256()),
            ("outcome", investment_analysis_locator_outcome()),
        ],
        &[
            "analysisId",
            "proposalId",
            "derivationDigest",
            "instrumentId",
            "accountId",
            "currency",
            "asOf",
            "horizonAt",
            "expiresAt",
            "policyDigest",
            "evidenceDigest",
            "outcome",
        ],
    )
}

fn investment_analysis_locator_outcome() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("generated")),
                ("action", investment_analysis_action()),
            ],
            &["kind", "action"],
        ),
        closed(
            vec![
                ("kind", constant("no_action")),
                ("reason", investment_analysis_no_action_reason()),
            ],
            &["kind", "reason"],
        ),
        closed(
            vec![
                ("kind", constant("unavailable")),
                ("reason", investment_analysis_unavailable_reason()),
            ],
            &["kind", "reason"],
        ),
    ])
}

fn investment_analysis_unavailable_reason() -> Value {
    one_of(vec![
        investment_analysis_evidence_reason("missing_evidence"),
        closed(
            vec![
                ("kind", constant("instrument_mismatch")),
                ("evidence", investment_analysis_evidence_kind()),
                ("expected", uuid()),
                ("actual", uuid()),
            ],
            &["kind", "evidence", "expected", "actual"],
        ),
        closed(
            vec![
                ("kind", constant("currency_mismatch")),
                ("evidence", investment_analysis_evidence_kind()),
                ("expected", investment_analysis_currency()),
                ("actual", investment_analysis_currency()),
            ],
            &["kind", "evidence", "expected", "actual"],
        ),
        closed(
            vec![
                ("kind", constant("account_mismatch")),
                ("expected", uuid()),
                ("actual", uuid()),
            ],
            &["kind", "expected", "actual"],
        ),
        investment_analysis_evidence_reason("not_available_at_cutoff"),
        investment_analysis_evidence_reason("expired_evidence"),
        investment_analysis_evidence_reason("stale_evidence"),
        closed(
            vec![
                ("kind", constant("rejected_quality")),
                ("evidence", investment_analysis_evidence_kind()),
                ("quality", investment_analysis_data_quality()),
            ],
            &["kind", "evidence", "quality"],
        ),
        investment_analysis_horizon_reason("forecast_horizon_mismatch"),
        investment_analysis_horizon_reason("valuation_horizon_mismatch"),
        closed(
            vec![
                ("kind", constant("backtest_horizon_mismatch")),
                ("expectedNanos", integer()),
                ("actualNanos", integer()),
            ],
            &["kind", "expectedNanos", "actualNanos"],
        ),
        investment_analysis_count_reason("insufficient_forecast_outcomes"),
        closed(
            vec![
                ("kind", constant("unsupported_forecast_coverage")),
                ("minimumPpm", bounded_unsigned(1_000_000)),
                ("maximumPpm", bounded_unsigned(1_000_000)),
                ("actualPpm", bounded_unsigned(1_000_000)),
            ],
            &["kind", "minimumPpm", "maximumPpm", "actualPpm"],
        ),
        investment_analysis_count_reason("insufficient_backtest_observations"),
        investment_analysis_count_reason("insufficient_backtest_trials"),
        closed(
            vec![("kind", constant("reserved_portfolio_revision"))],
            &["kind"],
        ),
    ])
}

fn investment_analysis_evidence_reason(kind: &'static str) -> Value {
    closed(
        vec![
            ("kind", constant(kind)),
            ("evidence", investment_analysis_evidence_kind()),
        ],
        &["kind", "evidence"],
    )
}

fn investment_analysis_horizon_reason(kind: &'static str) -> Value {
    closed(
        vec![
            ("kind", constant(kind)),
            ("expected", integer()),
            ("actual", integer()),
        ],
        &["kind", "expected", "actual"],
    )
}

fn investment_analysis_count_reason(kind: &'static str) -> Value {
    closed(
        vec![
            ("kind", constant(kind)),
            ("required", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("actual", bounded_unsigned_range(1, u64::from(u32::MAX))),
        ],
        &["kind", "required", "actual"],
    )
}

fn investment_analysis_price_cases() -> Value {
    closed(
        vec![
            ("downside", investment_analysis_money()),
            ("base", investment_analysis_money()),
            ("upside", investment_analysis_money()),
        ],
        &["downside", "base", "upside"],
    )
}

fn investment_analysis_forecast_ranges() -> Value {
    closed(
        vec![
            ("downside", investment_analysis_price_range()),
            ("base", investment_analysis_price_range()),
            ("upside", investment_analysis_price_range()),
        ],
        &["downside", "base", "upside"],
    )
}

fn investment_analysis_price_range() -> Value {
    closed(
        vec![
            ("lower", investment_analysis_money()),
            ("upper", investment_analysis_money()),
        ],
        &["lower", "upper"],
    )
}

fn investment_analysis_money() -> Value {
    closed(
        vec![
            ("amount", canonical_decimal_text()),
            ("currency", investment_analysis_currency()),
        ],
        &["amount", "currency"],
    )
}

fn investment_analysis_evidence_window() -> Value {
    closed(
        vec![
            ("observedAt", integer()),
            ("availableAt", integer()),
            ("expiresAt", integer()),
            ("contentIdentity", investment_analysis_content_identity()),
        ],
        &["observedAt", "availableAt", "expiresAt", "contentIdentity"],
    )
}

fn investment_analysis_content_identity() -> Value {
    closed(
        vec![
            ("algorithm", enumeration(&["sha256", "blake3"])),
            ("digest", investment_analysis_sha256()),
        ],
        &["algorithm", "digest"],
    )
}

fn investment_analysis_action() -> Value {
    enumeration(&["buy", "add", "hold", "trim", "sell"])
}

fn investment_analysis_no_action_reason() -> Value {
    enumeration(&[
        "conflicting_forecast_and_valuation",
        "backtest_below_policy",
        "liquidity_below_policy",
        "portfolio_risk_below_policy",
        "evidence_reliability_below_policy",
        "position_state_not_actionable",
        "generated_price_order_collapsed",
    ])
}

fn investment_analysis_invalidator() -> Value {
    enumeration(&[
        "forecast_valuation_conflict",
        "backtest_policy_breach",
        "liquidity_policy_breach",
        "portfolio_risk_policy_breach",
        "evidence_reliability_policy_breach",
        "position_state_incompatible",
        "generated_price_order_collapsed",
    ])
}

fn investment_analysis_evidence_kind() -> Value {
    enumeration(&[
        "market",
        "price_forecast",
        "valuation",
        "backtest",
        "liquidity",
        "portfolio_risk",
    ])
}

fn investment_analysis_data_quality() -> Value {
    enumeration(&[
        "direct_verified",
        "direct_unverified",
        "official_delayed",
        "aggregated",
        "indicative",
        "modeled",
        "estimated",
        "stale",
        "quarantined",
    ])
}

fn investment_analysis_currency() -> Value {
    json!({
        "type": "string",
        "minLength": 3,
        "maxLength": 3,
        "pattern": "^[A-Z]{3}$"
    })
}

fn investment_analysis_sha256() -> Value {
    json!({
        "type": "string",
        "minLength": 64,
        "maxLength": 64,
        "pattern": "^[0-9a-f]{64}$"
    })
}

fn market_rows(required: &[&str]) -> Value {
    let fields = required
        .iter()
        .map(|name| (*name, market_field(name)))
        .collect();
    nullable_rows(signature(fields))
}

fn unified_market_rows() -> Value {
    nullable_rows(closed(
        vec![
            ("instrumentId", uuid()),
            ("symbol", bounded_text(256)),
            ("symbolKind", bounded_text(64)),
            ("symbolVenueId", nullable(bounded_text(128))),
            ("assetClass", bounded_text(64)),
            ("quoteCurrency", bounded_text(32)),
            ("definitionKind", bounded_text(64)),
            ("definitionRevision", nullable(unsigned())),
            ("referenceRevision", nullable(bounded_text(256))),
            ("permanentFigi", nullable(bounded_text(64))),
            ("displayName", nullable(bounded_text(512))),
            ("tickSize", nullable(bounded_text(128))),
            ("lotSize", nullable(bounded_text(128))),
            ("executionTermsAvailable", boolean()),
            ("referenceEvidence", nullable(record())),
            ("availability", bounded_text(128)),
            ("confidence", bounded_text(256)),
            ("quote", record()),
            ("orderBook", nullable(order_level_book())),
            ("marketObservation", market_investment_observation()),
            ("selectedSource", nullable(record())),
            ("alternatives", bounded_array(record(), 8)),
            ("selectionReceipt", market_selection_receipt()),
        ],
        &[
            "instrumentId",
            "symbol",
            "symbolKind",
            "symbolVenueId",
            "assetClass",
            "quoteCurrency",
            "definitionKind",
            "definitionRevision",
            "referenceRevision",
            "permanentFigi",
            "displayName",
            "tickSize",
            "lotSize",
            "executionTermsAvailable",
            "referenceEvidence",
            "availability",
            "confidence",
            "quote",
            "orderBook",
            "marketObservation",
            "selectedSource",
            "alternatives",
            "selectionReceipt",
        ],
    ))
}

fn order_level_book() -> Value {
    closed(
        vec![
            ("depth", constant("order_level")),
            ("revision", unsigned_integer_text()),
            (
                "phase",
                enumeration(&["awaiting_snapshot", "healthy", "quarantined"]),
            ),
            ("quarantineReason", nullable(bounded_text(128))),
            ("quality", market_quality()),
            (
                "freshness",
                enumeration(&["uninitialized", "fresh", "stale"]),
            ),
            ("lastMarketAt", nullable(canonical_market_timestamp())),
            ("availableAt", canonical_market_timestamp()),
            ("usableForSelection", boolean()),
            ("totalOrderCount", unsigned()),
            ("returnedOrderCount", unsigned()),
            ("sampleTruncated", boolean()),
            ("samplePolicy", constant("stable_provider_order_id_prefix")),
            ("orders", bounded_array(order_level_order(), 64)),
        ],
        &[
            "depth",
            "revision",
            "phase",
            "quarantineReason",
            "quality",
            "freshness",
            "lastMarketAt",
            "availableAt",
            "usableForSelection",
            "totalOrderCount",
            "returnedOrderCount",
            "sampleTruncated",
            "samplePolicy",
            "orders",
        ],
    )
}

fn order_level_order() -> Value {
    closed(
        vec![
            ("orderId", bounded_text(512)),
            ("side", enumeration(&["bid", "ask"])),
            ("price", canonical_decimal_text()),
            ("priceTicks", integer_text()),
            ("quantity", canonical_decimal_text()),
            ("quantityLots", integer_text()),
            (
                "providerOrderTimestamp",
                nullable(canonical_market_timestamp()),
            ),
            (
                "providerPriority",
                nullable(closed(
                    vec![
                        ("value", unsigned_integer_text()),
                        ("rule", bounded_text(512)),
                    ],
                    &["value", "rule"],
                )),
            ),
            ("firstSeenIn", enumeration(&["snapshot", "update"])),
            ("lastUpdatedIn", enumeration(&["snapshot", "update"])),
            ("lastSourceTimestamp", canonical_market_timestamp()),
            ("lastReceivedAt", canonical_market_timestamp()),
            ("arrivalOrdinal", unsigned_integer_text()),
        ],
        &[
            "orderId",
            "side",
            "price",
            "priceTicks",
            "quantity",
            "quantityLots",
            "providerOrderTimestamp",
            "providerPriority",
            "firstSeenIn",
            "lastUpdatedIn",
            "lastSourceTimestamp",
            "lastReceivedAt",
            "arrivalOrdinal",
        ],
    )
}

fn market_investment_observation() -> Value {
    one_of(vec![
        closed(
            vec![
                ("availability", constant("available")),
                ("instrumentId", uuid()),
                ("mark", market_investment_mark()),
                ("selectionDigest", sha256_evidence_digest()),
                ("selectedAt", canonical_market_timestamp()),
                ("generation", nullable(positive_integer_text())),
                ("quality", market_quality()),
                ("depth", nullable(market_depth())),
                ("coverage", market_coverage()),
                ("integrity", market_integrity()),
                ("features", market_feature_availability()),
            ],
            &[
                "availability",
                "instrumentId",
                "mark",
                "selectionDigest",
                "selectedAt",
                "generation",
                "quality",
                "depth",
                "coverage",
                "integrity",
                "features",
            ],
        ),
        closed(
            vec![
                ("availability", constant("unavailable")),
                (
                    "reason",
                    enumeration(&["no_eligible_source", "no_fresh_last_trade_or_midpoint"]),
                ),
            ],
            &["availability", "reason"],
        ),
    ])
}

fn market_investment_mark() -> Value {
    closed(
        vec![
            ("value", canonical_decimal_text()),
            (
                "currency",
                json!({"type": "string", "pattern": "^[A-Z]{3}$"}),
            ),
            (
                "basis",
                enumeration(&["fresh_last_trade", "fresh_bid_ask_midpoint"]),
            ),
            ("evidenceIdentity", sha256_evidence_digest()),
            ("freshUntil", nullable(canonical_market_timestamp())),
        ],
        &[
            "value",
            "currency",
            "basis",
            "evidenceIdentity",
            "freshUntil",
        ],
    )
}

fn market_feature_availability() -> Value {
    one_of(vec![
        closed(
            vec![
                ("availability", constant("available")),
                ("sourceId", bounded_text(128)),
                ("venueId", bounded_text(64)),
                ("instrumentId", uuid()),
                ("generation", positive_integer_text()),
                ("availableAt", canonical_market_timestamp()),
                ("contentDigest", evidence_digest()),
                ("valueCount", unsigned()),
            ],
            &[
                "availability",
                "sourceId",
                "venueId",
                "instrumentId",
                "generation",
                "availableAt",
                "contentDigest",
                "valueCount",
            ],
        ),
        closed(
            vec![
                ("availability", constant("unavailable")),
                (
                    "reason",
                    enumeration(&[
                        "source_does_not_publish_live_features",
                        "incomplete_snapshot",
                        "no_exact_source_generation",
                        "available_after_selection",
                        "incomplete_value_set",
                    ]),
                ),
            ],
            &["availability", "reason"],
        ),
    ])
}

fn market_selection_receipt() -> Value {
    closed(
        vec![
            ("policyRevision", bounded_unsigned_range(1, 4_294_967_295)),
            ("policyCandidateLimit", bounded_unsigned_range(1, 4_096)),
            ("policyDigest", sha256_evidence_digest()),
            ("selectionDigest", sha256_evidence_digest()),
            ("selectedAt", canonical_market_timestamp()),
            ("eligibleCount", bounded_unsigned(4_096)),
            ("rejectedCount", bounded_unsigned(4_096)),
            ("availableAlternativeCount", bounded_unsigned(4_096)),
            ("returnedAlternativeCount", bounded_unsigned(8)),
            ("alternativesComplete", boolean()),
            (
                "selectionClass",
                nullable(enumeration(&["exact_requirements", "admitted_downgrade"])),
            ),
            (
                "downgradeDimensions",
                bounded_array(market_downgrade_dimension(), 5),
            ),
        ],
        &[
            "policyRevision",
            "policyCandidateLimit",
            "policyDigest",
            "selectionDigest",
            "selectedAt",
            "eligibleCount",
            "rejectedCount",
            "availableAlternativeCount",
            "returnedAlternativeCount",
            "alternativesComplete",
            "selectionClass",
            "downgradeDimensions",
        ],
    )
}

fn market_downgrade_dimension() -> Value {
    one_of(vec![
        closed(
            vec![
                ("dimension", constant("timing")),
                ("required", market_timing()),
                ("selected", market_timing()),
            ],
            &["dimension", "required", "selected"],
        ),
        closed(
            vec![
                ("dimension", constant("depth")),
                ("minimum", market_depth()),
                ("selected", nullable(market_depth())),
            ],
            &["dimension", "minimum", "selected"],
        ),
        closed(
            vec![
                ("dimension", constant("quality")),
                ("minimum", market_quality()),
                ("selected", market_quality()),
            ],
            &["dimension", "minimum", "selected"],
        ),
        closed(
            vec![
                ("dimension", constant("coverage")),
                ("required", market_coverage()),
                ("selected", market_coverage()),
            ],
            &["dimension", "required", "selected"],
        ),
        closed(
            vec![
                ("dimension", constant("freshness")),
                ("maximumAgeNanos", unsigned()),
                ("selectedAgeNanos", unsigned()),
            ],
            &["dimension", "maximumAgeNanos", "selectedAgeNanos"],
        ),
    ])
}

fn evidence_digest() -> Value {
    closed(
        vec![
            ("algorithm", enumeration(&["sha256", "blake3"])),
            (
                "bytes",
                json!({"type": "string", "pattern": "^[0-9a-f]{64}$"}),
            ),
        ],
        &["algorithm", "bytes"],
    )
}

fn sha256_evidence_digest() -> Value {
    closed(
        vec![
            ("algorithm", constant("sha256")),
            (
                "bytes",
                json!({"type": "string", "pattern": "^[0-9a-f]{64}$"}),
            ),
        ],
        &["algorithm", "bytes"],
    )
}

fn canonical_market_timestamp() -> Value {
    json!({
        "type": "string",
        "format": "date-time",
        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"
    })
}

fn canonical_decimal_text() -> Value {
    json!({
        "type": "string",
        "pattern": "^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]*[1-9])?$"
    })
}

fn macro_dashboard() -> Value {
    closed(
        vec![
            (
                "schemaIdentity",
                constant("market-squawk-macro-dashboard/v1"),
            ),
            ("binding", macro_dashboard_binding()),
            ("release", macro_dashboard_release()),
            ("selection", macro_dashboard_selection()),
            (
                "observations",
                fixed_array(macro_dashboard_observation(), 11),
            ),
        ],
        &[
            "schemaIdentity",
            "binding",
            "release",
            "selection",
            "observations",
        ],
    )
}

fn macro_dashboard_binding() -> Value {
    closed(
        vec![
            (
                "surfaceId",
                constant("federal-reserve-board.data-download-program"),
            ),
            ("sourceId", constant(BOARD_DDP_SOURCE_ID)),
            ("providerDatasetId", bounded_text(512)),
            ("analyticalDatasetId", analytical_dataset_identifier()),
            ("manifest", macro_dashboard_manifest()),
            ("objectGraphDigest", lowercase_sha256()),
            ("queryIdentity", lowercase_sha256()),
            ("resultDigest", lowercase_sha256()),
        ],
        &[
            "surfaceId",
            "sourceId",
            "providerDatasetId",
            "analyticalDatasetId",
            "manifest",
            "objectGraphDigest",
            "queryIdentity",
            "resultDigest",
        ],
    )
}

fn macro_dashboard_manifest() -> Value {
    closed(
        vec![
            ("datasetId", analytical_dataset_identifier()),
            (
                "manifestVersion",
                one_of(vec![
                    json!({"type": "string", "pattern": "^[1-9][0-9]*$"}),
                    json!({"type": "integer", "minimum": 1}),
                ]),
            ),
            (
                "schema",
                closed(
                    vec![
                        ("name", bounded_text(256)),
                        ("version", bounded_unsigned_range(1, u16::MAX.into())),
                        ("fingerprint", lowercase_sha256()),
                    ],
                    &["name", "version", "fingerprint"],
                ),
            ),
            ("contentHash", lowercase_sha256()),
        ],
        &["datasetId", "manifestVersion", "schema", "contentHash"],
    )
}

fn macro_dashboard_release() -> Value {
    closed(
        vec![
            ("code", constant("H15")),
            ("title", constant("H.15 Selected Interest Rates")),
            ("family", constant("h15-treasury-constant-maturities")),
            ("frequency", constant("business_daily")),
            ("quality", constant("official_delayed")),
        ],
        &["code", "title", "family", "frequency", "quality"],
    )
}

fn macro_dashboard_selection() -> Value {
    closed(
        vec![
            ("policy", constant("latest_known_by_series_as_of_cutoff_v1")),
            ("evaluatedAt", timestamp()),
            ("selectionDigest", lowercase_sha256()),
            ("returnedSeries", constant_unsigned(11)),
            ("availableSeries", bounded_unsigned(11)),
            ("missingSeries", bounded_unsigned(11)),
            ("complete", constant_bool(true)),
        ],
        &[
            "policy",
            "evaluatedAt",
            "selectionDigest",
            "returnedSeries",
            "availableSeries",
            "missingSeries",
            "complete",
        ],
    )
}

fn macro_dashboard_observation() -> Value {
    closed(
        vec![
            ("slot", macro_dashboard_slot()),
            ("label", bounded_text(256)),
            ("maturityOrder", bounded_unsigned_range(1, 11)),
            ("seriesId", bounded_text(512)),
            ("unitId", bounded_text(512)),
            ("unitPresentation", constant("percent_per_year")),
            ("effectiveDate", exact_length_text(10)),
            ("availableAt", timestamp()),
            ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("observation", macro_dashboard_value()),
            ("sourceIdentifier", bounded_text(512)),
            ("sourcePayloadDigest", lowercase_sha256()),
        ],
        &[
            "slot",
            "label",
            "maturityOrder",
            "seriesId",
            "unitId",
            "unitPresentation",
            "effectiveDate",
            "availableAt",
            "revision",
            "observation",
            "sourceIdentifier",
            "sourcePayloadDigest",
        ],
    )
}

fn macro_dashboard_slot() -> Value {
    let slots = h15_treasury_constant_maturities_dashboard_series()
        .iter()
        .map(|descriptor| descriptor.slot())
        .collect::<Vec<_>>();
    json!({"type": "string", "enum": slots})
}

fn macro_dashboard_value() -> Value {
    one_of(vec![
        closed(
            vec![
                ("state", constant("observed")),
                ("decimal", canonical_decimal_text()),
            ],
            &["state", "decimal"],
        ),
        closed(
            vec![
                ("state", constant("missing")),
                ("marker", bounded_text(128)),
                ("reason", nullable(bounded_text(512))),
            ],
            &["state", "marker", "reason"],
        ),
    ])
}

fn analytical_dataset_identifier() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 256
    })
}

fn positive_integer_text() -> Value {
    json!({"type": "string", "pattern": "^[1-9][0-9]*$"})
}

fn unsigned_integer_text() -> Value {
    json!({"type": "string", "pattern": "^[0-9]+$"})
}

fn integer_text() -> Value {
    json!({"type": "string", "pattern": "^-?[0-9]+$"})
}

fn market_timing() -> Value {
    enumeration(&["real_time", "delayed", "end_of_day", "historical", "stored"])
}

fn market_depth() -> Value {
    enumeration(&["top_of_book", "price_level", "order_level"])
}

fn market_quality() -> Value {
    enumeration(&[
        "direct_verified",
        "direct_unverified",
        "official_delayed",
        "aggregated",
        "indicative",
        "modeled",
        "estimated",
        "stale",
        "quarantined",
    ])
}

fn market_coverage() -> Value {
    enumeration(&[
        "consolidated",
        "multi_venue_partial",
        "single_venue",
        "benchmark",
        "reference",
        "user_owned",
    ])
}

fn market_integrity() -> Value {
    enumeration(&[
        "verified",
        "unverified",
        "not_applicable",
        "failed",
        "quarantined",
    ])
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
        "book" | "quote" => record(),
        "marketObservation" => market_investment_observation(),
        "selectionReceipt" => market_selection_receipt(),
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
            ("cohortAuthorityDigest", text()),
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

fn portfolio_candidate_impact() -> Value {
    closed(
        vec![
            ("accountId", uuid()),
            ("revisionId", sha256()),
            ("setupEvidence", portfolio_candidate_setup_evidence()),
            ("policy", constant("selected_market_candidate_impact_v3")),
            ("evidenceSchemaVersion", constant_unsigned(1)),
            ("evidenceDigest", sha256_evidence_digest()),
            (
                "portfolioEvidence",
                portfolio_candidate_portfolio_evidence(),
            ),
            ("instrumentId", uuid()),
            (
                "positionState",
                enumeration(&["zero_position", "existing_holding"]),
            ),
            ("currentQuantity", canonical_decimal_text()),
            ("proposedQuantity", canonical_decimal_text()),
            ("currentMarketValue", recommendation_money()),
            ("proposedMarketValue", recommendation_money()),
            ("capitalChange", recommendation_money()),
            ("portfolioValue", recommendation_money()),
            (
                "portfolioValueBasis",
                constant("source_reported_holdings_with_selected_candidate_revalued"),
            ),
            ("instrumentTerms", portfolio_candidate_instrument_terms()),
            ("costEvidence", portfolio_candidate_cost_evidence()),
            ("concentration", portfolio_candidate_concentration()),
            ("scenario", portfolio_candidate_scenario()),
            ("markEvidence", portfolio_candidate_mark_evidence()),
            ("availability", portfolio_candidate_availability()),
            ("riskAdvisory", portfolio_candidate_risk_advisory()),
            ("authority", portfolio_candidate_authority()),
        ],
        &[
            "accountId",
            "revisionId",
            "setupEvidence",
            "policy",
            "evidenceSchemaVersion",
            "evidenceDigest",
            "portfolioEvidence",
            "instrumentId",
            "positionState",
            "currentQuantity",
            "proposedQuantity",
            "currentMarketValue",
            "proposedMarketValue",
            "capitalChange",
            "portfolioValue",
            "portfolioValueBasis",
            "instrumentTerms",
            "costEvidence",
            "concentration",
            "scenario",
            "markEvidence",
            "availability",
            "riskAdvisory",
            "authority",
        ],
    )
}

fn portfolio_candidate_setup_evidence() -> Value {
    closed(
        vec![
            ("setupRevision", positive_integer_text()),
            ("setupDigest", sha256()),
            ("configurationDigest", sha256()),
            ("profileDigest", sha256()),
            ("catalogDigest", sha256()),
        ],
        &[
            "setupRevision",
            "setupDigest",
            "configurationDigest",
            "profileDigest",
            "catalogDigest",
        ],
    )
}

fn portfolio_candidate_portfolio_evidence() -> Value {
    closed(
        vec![
            ("revisionId", sha256()),
            ("effectiveAtUnixNanos", integer_text()),
            ("availableAtUnixNanos", integer_text()),
            ("sourceId", bounded_text(256)),
            ("sourceCoverage", bounded_array(bounded_text(256), 4_096)),
            ("artifactSha256", sha256()),
        ],
        &[
            "revisionId",
            "effectiveAtUnixNanos",
            "availableAtUnixNanos",
            "sourceId",
            "sourceCoverage",
            "artifactSha256",
        ],
    )
}

fn portfolio_candidate_instrument_terms() -> Value {
    closed(
        vec![
            ("definitionRevision", positive_integer_text()),
            ("priceTick", canonical_decimal_text()),
            ("lotSize", canonical_decimal_text()),
            ("quoteCurrency", recommendation_currency()),
            (
                "settlementDenomination",
                portfolio_candidate_settlement_denomination(),
            ),
            ("contractMultiplier", canonical_decimal_text()),
        ],
        &[
            "definitionRevision",
            "priceTick",
            "lotSize",
            "quoteCurrency",
            "settlementDenomination",
            "contractMultiplier",
        ],
    )
}

fn portfolio_candidate_settlement_denomination() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("currency")),
                ("currency", recommendation_currency()),
            ],
            &["kind", "currency"],
        ),
        closed(
            vec![("kind", constant("asset")), ("instrumentId", uuid())],
            &["kind", "instrumentId"],
        ),
    ])
}

fn portfolio_candidate_cost_evidence() -> Value {
    closed(
        vec![
            ("fees", portfolio_candidate_cost()),
            ("slippage", portfolio_candidate_cost()),
        ],
        &["fees", "slippage"],
    )
}

fn portfolio_candidate_cost() -> Value {
    one_of(vec![
        closed(
            vec![
                ("status", constant("available")),
                ("amount", recommendation_money()),
                ("evidenceDigest", evidence_digest()),
            ],
            &["status", "amount", "evidenceDigest"],
        ),
        closed(
            vec![
                ("status", constant("unavailable")),
                ("reason", enumeration(&["exact_fees", "exact_slippage"])),
            ],
            &["status", "reason"],
        ),
    ])
}

fn portfolio_candidate_concentration() -> Value {
    closed(
        vec![
            ("current", canonical_decimal_text()),
            ("proposed", canonical_decimal_text()),
            ("change", canonical_decimal_text()),
        ],
        &["current", "proposed", "change"],
    )
}

fn portfolio_candidate_scenario() -> Value {
    closed(
        vec![
            ("scope", constant("candidate_position_only")),
            ("shock", canonical_decimal_text()),
            ("currentImpact", recommendation_money()),
            ("proposedImpact", recommendation_money()),
            ("marginalImpact", recommendation_money()),
        ],
        &[
            "scope",
            "shock",
            "currentImpact",
            "proposedImpact",
            "marginalImpact",
        ],
    )
}

fn portfolio_candidate_mark_evidence() -> Value {
    closed(
        vec![
            ("status", constant("fresh_selected_market_observation")),
            ("instrumentId", uuid()),
            ("unitMark", recommendation_money()),
            ("markKind", enumeration(&["last_trade", "midpoint"])),
            (
                "quality",
                enumeration(&[
                    "direct_verified",
                    "direct_unverified",
                    "official_delayed",
                    "aggregated",
                    "indicative",
                    "modeled",
                    "estimated",
                ]),
            ),
            ("sourceId", bounded_text(256)),
            ("observationDigest", evidence_digest()),
            ("observedAtUnixNanos", integer_text()),
            ("availableAtUnixNanos", integer_text()),
            ("freshUntilUnixNanosExclusive", integer_text()),
            ("evaluatedAtUnixNanos", integer_text()),
            ("portfolioRevisionId", sha256()),
            ("selection", portfolio_candidate_selection()),
        ],
        &[
            "status",
            "instrumentId",
            "unitMark",
            "markKind",
            "quality",
            "sourceId",
            "observationDigest",
            "observedAtUnixNanos",
            "availableAtUnixNanos",
            "freshUntilUnixNanosExclusive",
            "evaluatedAtUnixNanos",
            "portfolioRevisionId",
            "selection",
        ],
    )
}

fn portfolio_candidate_selection() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("sourceId", bounded_text(256)),
            (
                "policyRevision",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("policyDigest", evidence_digest()),
            ("receiptDigest", evidence_digest()),
            ("sourceStateRevision", nullable(unsigned_integer_text())),
            ("selectedAtUnixNanos", integer_text()),
        ],
        &[
            "instrumentId",
            "sourceId",
            "policyRevision",
            "policyDigest",
            "receiptDigest",
            "sourceStateRevision",
            "selectedAtUnixNanos",
        ],
    )
}

fn portfolio_candidate_availability() -> Value {
    let unavailable = |reason| {
        closed(
            vec![
                ("status", constant("unavailable")),
                ("reason", constant(reason)),
            ],
            &["status", "reason"],
        )
    };
    closed(
        vec![
            (
                "portfolioWideSelectedMarks",
                unavailable("portfolio_wide_selected_market_marks"),
            ),
            ("liquidity", unavailable("exact_selected_source_liquidity")),
            (
                "settlementBackedSizing",
                unavailable("settlement_backed_sizing"),
            ),
            (
                "factorClassification",
                unavailable("exact_factor_classification"),
            ),
        ],
        &[
            "portfolioWideSelectedMarks",
            "liquidity",
            "settlementBackedSizing",
            "factorClassification",
        ],
    )
}

fn portfolio_candidate_risk_advisory() -> Value {
    let risk_check = enumeration(&[
        "selected_account",
        "current_portfolio_revision",
        "fresh_selected_mark",
        "instrument_terms",
        "position_lot_alignment",
        "portfolio_wide_selected_marks",
        "liquidity",
        "settlement_backed_sizing",
        "fees",
        "slippage",
    ]);
    closed(
        vec![
            ("outcome", constant("indeterminate_at_evaluation")),
            ("evaluatedAtUnixNanos", integer_text()),
            ("checksEvaluated", bounded_array(risk_check.clone(), 10)),
            ("checksUnavailable", bounded_array(risk_check, 10)),
            ("evidenceDigest", evidence_digest()),
            ("authority", constant("analysis_only")),
            ("reservation", constant_bool(false)),
            ("order", constant_bool(false)),
        ],
        &[
            "outcome",
            "evaluatedAtUnixNanos",
            "checksEvaluated",
            "checksUnavailable",
            "evidenceDigest",
            "authority",
            "reservation",
            "order",
        ],
    )
}

fn portfolio_candidate_authority() -> Value {
    closed(
        vec![
            ("analysisOnly", constant_bool(true)),
            ("portfolioMutation", constant_bool(false)),
            ("executionAuthority", constant_bool(false)),
            ("riskAuthority", constant("analysis_only")),
            ("reservation", constant_bool(false)),
            ("order", constant_bool(false)),
            ("riskApprovalRequiredBeforeAnyOrder", constant_bool(true)),
        ],
        &[
            "analysisOnly",
            "portfolioMutation",
            "executionAuthority",
            "riskAuthority",
            "reservation",
            "order",
            "riskApprovalRequiredBeforeAnyOrder",
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

fn latest_valid_forecast() -> Value {
    one_of(vec![
        closed(
            vec![
                ("status", constant("available")),
                ("evidence", available_forecast_evidence()),
                ("selectionReceipt", forecast_selection_receipt()),
            ],
            &["status", "evidence", "selectionReceipt"],
        ),
        closed(
            vec![
                ("status", constant("unavailable")),
                ("evidence", unavailable_forecast_evidence()),
                ("selectionReceipt", forecast_selection_receipt()),
            ],
            &["status", "evidence", "selectionReceipt"],
        ),
    ])
}

fn available_forecast_evidence() -> Value {
    closed(
        vec![
            ("vintageId", lowercase_sha256()),
            ("instrumentId", uuid()),
            ("outputBinding", available_forecast_output_binding()),
            ("model", selected_forecast_model()),
            ("forecastArtifact", selected_forecast_artifact()),
            ("freshness", selected_forecast_freshness()),
            ("points", fixed_array(selected_price_forecast_point(), 1)),
            ("calibration", nullable(selected_forecast_calibration())),
        ],
        &[
            "vintageId",
            "instrumentId",
            "outputBinding",
            "model",
            "forecastArtifact",
            "freshness",
            "points",
            "calibration",
        ],
    )
}

fn unavailable_forecast_evidence() -> Value {
    closed(
        vec![
            ("vintageId", lowercase_sha256()),
            ("instrumentId", uuid()),
            ("outputBinding", selected_forecast_output_binding()),
            ("model", selected_forecast_model()),
            ("forecastArtifact", selected_forecast_artifact()),
            ("freshness", selected_forecast_freshness()),
            (
                "reason",
                enumeration(&[
                    "return_measurement",
                    "probability_measurement",
                    "other_regression_measurement",
                    "terminal_horizon_unavailable",
                    "central_statistic_unavailable",
                ]),
            ),
        ],
        &[
            "vintageId",
            "instrumentId",
            "outputBinding",
            "model",
            "forecastArtifact",
            "freshness",
            "reason",
        ],
    )
}

fn available_forecast_output_binding() -> Value {
    closed(
        vec![
            ("identitySha256", lowercase_sha256()),
            ("measurement", constant("price")),
            ("currency", currency_code()),
            (
                "centralStatistic",
                constant("model_estimated_conditional_mean"),
            ),
            ("target", constant("fixed_horizon_terminal")),
            ("terminalHorizonNanos", positive_integer_text()),
        ],
        &[
            "identitySha256",
            "measurement",
            "currency",
            "centralStatistic",
            "target",
            "terminalHorizonNanos",
        ],
    )
}

fn selected_forecast_output_binding() -> Value {
    closed(
        vec![
            ("identitySha256", lowercase_sha256()),
            (
                "measurement",
                enumeration(&["price", "return", "probability", "other_regression"]),
            ),
            ("currency", nullable(currency_code())),
            (
                "centralStatistic",
                enumeration(&["model_estimated_conditional_mean", "unavailable"]),
            ),
            (
                "target",
                enumeration(&["fixed_horizon_terminal", "unsupported"]),
            ),
            ("terminalHorizonNanos", nullable(positive_integer_text())),
        ],
        &[
            "identitySha256",
            "measurement",
            "currency",
            "centralStatistic",
            "target",
            "terminalHorizonNanos",
        ],
    )
}

fn selected_forecast_model() -> Value {
    closed(
        vec![
            ("modelId", uuid()),
            ("bundleId", bounded_text(256)),
            ("bundleVersion", positive_integer_text()),
            ("metadataSha256", lowercase_sha256()),
            ("modelArtifactSha256", lowercase_sha256()),
            ("trainingRunSha256", lowercase_sha256()),
        ],
        &[
            "modelId",
            "bundleId",
            "bundleVersion",
            "metadataSha256",
            "modelArtifactSha256",
            "trainingRunSha256",
        ],
    )
}

fn selected_forecast_artifact() -> Value {
    closed(
        vec![
            ("artifactId", bounded_text(160)),
            ("sha256", lowercase_sha256()),
            ("byteCount", positive_integer_text()),
            (
                "mediaType",
                json!({
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                    "pattern": "^[A-Za-z0-9/.+\\-]+$",
                }),
            ),
        ],
        &["artifactId", "sha256", "byteCount", "mediaType"],
    )
}

fn selected_forecast_freshness() -> Value {
    closed(
        vec![
            ("asOfUnixNanos", integer_text()),
            ("observedThroughUnixNanos", integer_text()),
            ("availableAtUnixNanos", integer_text()),
            ("createdAtUnixNanos", integer_text()),
            ("expiresAtUnixNanos", integer_text()),
            ("availableAtOrBeforeAsOf", constant_bool(true)),
            ("publishedAtOrBeforeAsOf", constant_bool(true)),
            ("unexpiredAtAsOf", constant_bool(true)),
        ],
        &[
            "asOfUnixNanos",
            "observedThroughUnixNanos",
            "availableAtUnixNanos",
            "createdAtUnixNanos",
            "expiresAtUnixNanos",
            "availableAtOrBeforeAsOf",
            "publishedAtOrBeforeAsOf",
            "unexpiredAtAsOf",
        ],
    )
}

fn selected_price_forecast_point() -> Value {
    closed(
        vec![
            ("targetAtUnixNanos", integer_text()),
            ("central", selected_forecast_decimal()),
            (
                "coverageIntervals",
                nullable(fixed_array(selected_forecast_coverage_interval(), 3)),
            ),
        ],
        &["targetAtUnixNanos", "central", "coverageIntervals"],
    )
}

fn selected_forecast_decimal() -> Value {
    closed(
        vec![
            ("mantissa", integer_text()),
            ("scale", bounded_unsigned(12)),
        ],
        &["mantissa", "scale"],
    )
}

fn selected_forecast_coverage_interval() -> Value {
    closed(
        vec![
            (
                "targetCoverageBasisPoints",
                one_of(vec![
                    constant_unsigned(5_000),
                    constant_unsigned(8_000),
                    constant_unsigned(9_500),
                ]),
            ),
            ("lower", selected_forecast_decimal()),
            ("upper", selected_forecast_decimal()),
            (
                "semantics",
                constant("marginal_coverage_interval_not_scenario_probability"),
            ),
        ],
        &["targetCoverageBasisPoints", "lower", "upper", "semantics"],
    )
}

fn selected_forecast_calibration() -> Value {
    closed(
        vec![
            ("identitySha256", lowercase_sha256()),
            (
                "method",
                enumeration(&["mapie_enbpi", "mapie_aci", "residual_quantile"]),
            ),
            (
                "window",
                closed(
                    vec![
                        ("startUnixNanos", integer_text()),
                        ("endUnixNanos", integer_text()),
                        ("observationCount", positive_integer_text()),
                    ],
                    &["startUnixNanos", "endUnixNanos", "observationCount"],
                ),
            ),
            ("policyArtifact", selected_calibration_artifact()),
            ("residualArtifact", selected_calibration_artifact()),
            (
                "coverageBands",
                fixed_array(selected_calibration_coverage_band(), 3),
            ),
            ("dependenceAssumptions", bounded_text(512)),
            (
                "semantics",
                constant("empirical_marginal_coverage_not_scenario_probability"),
            ),
        ],
        &[
            "identitySha256",
            "method",
            "window",
            "policyArtifact",
            "residualArtifact",
            "coverageBands",
            "dependenceAssumptions",
            "semantics",
        ],
    )
}

fn selected_calibration_artifact() -> Value {
    closed(
        vec![
            ("sha256", lowercase_sha256()),
            ("byteCount", positive_integer_text()),
        ],
        &["sha256", "byteCount"],
    )
}

fn selected_calibration_coverage_band() -> Value {
    closed(
        vec![
            (
                "targetCoverageBasisPoints",
                one_of(vec![
                    constant_unsigned(5_000),
                    constant_unsigned(8_000),
                    constant_unsigned(9_500),
                ]),
            ),
            ("lowerOffsetIeee754Hex", ieee754_hex()),
            ("upperOffsetIeee754Hex", ieee754_hex()),
            ("realizedCoveredCount", unsigned_integer_text()),
            ("realizedObservationCount", positive_integer_text()),
        ],
        &[
            "targetCoverageBasisPoints",
            "lowerOffsetIeee754Hex",
            "upperOffsetIeee754Hex",
            "realizedCoveredCount",
            "realizedObservationCount",
        ],
    )
}

fn forecast_selection_receipt() -> Value {
    closed(
        vec![
            (
                "schema",
                constant("market-squawk/forecast-selection-receipt/v1"),
            ),
            ("policyRevision", constant_unsigned(1)),
            (
                "selectionOrder",
                constant("newest_created_at_observed_through_available_at_then_lowest_vintage_id"),
            ),
            ("instrumentId", uuid()),
            ("asOfUnixNanos", integer_text()),
            ("consideredVintageCount", bounded_unsigned(100_000)),
            ("retainedVintageHardCeiling", bounded_unsigned(100_000)),
            ("eligibleVintageCount", bounded_unsigned(100_000)),
            ("competingEligibleVintageCount", bounded_unsigned(100_000)),
            ("selectionComplete", constant_bool(true)),
            ("selectedVintageId", lowercase_sha256()),
            ("selectedCreatedAtUnixNanos", integer_text()),
            ("selectedObservedThroughUnixNanos", integer_text()),
            ("selectedAvailableAtUnixNanos", integer_text()),
            ("selectedExpiresAtUnixNanos", integer_text()),
            ("receiptDigestSha256", lowercase_sha256()),
        ],
        &[
            "schema",
            "policyRevision",
            "selectionOrder",
            "instrumentId",
            "asOfUnixNanos",
            "consideredVintageCount",
            "retainedVintageHardCeiling",
            "eligibleVintageCount",
            "competingEligibleVintageCount",
            "selectionComplete",
            "selectedVintageId",
            "selectedCreatedAtUnixNanos",
            "selectedObservedThroughUnixNanos",
            "selectedAvailableAtUnixNanos",
            "selectedExpiresAtUnixNanos",
            "receiptDigestSha256",
        ],
    )
}

fn lowercase_sha256() -> Value {
    json!({
        "type": "string",
        "minLength": 64,
        "maxLength": 64,
        "pattern": "^[0-9a-f]{64}$",
    })
}

fn ieee754_hex() -> Value {
    json!({
        "type": "string",
        "minLength": 16,
        "maxLength": 16,
        "pattern": "^[0-9a-f]{16}$",
    })
}

fn currency_code() -> Value {
    json!({
        "type": "string",
        "minLength": 3,
        "maxLength": 3,
        "pattern": "^[A-Z]{3}$",
    })
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
                (
                    "provider",
                    enumeration(&["coinbase", "coinbase-direct", "kraken"]),
                ),
                ("requiresStop", constant_bool(true)),
            ],
            &["state", "provider", "requiresStop"],
        ),
        closed(
            vec![
                ("state", constant("failed")),
                (
                    "reason",
                    constant("paper action-hook cleanup requires retry"),
                ),
                ("requiresStop", constant_bool(true)),
            ],
            &["state", "reason", "requiresStop"],
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
                (
                    "accounts",
                    paper_status_bounded_evidence(paper_status_account()),
                ),
                (
                    "cash",
                    paper_status_bounded_evidence(paper_status_cash_balance()),
                ),
                (
                    "positionEvidence",
                    paper_status_bounded_evidence(paper_status_position()),
                ),
                ("configurationDigestSha256", sha256()),
                ("simulation", paper_status_simulation()),
                ("reconciliation", paper_status_reconciliation()),
                ("riskLimits", paper_status_risk_limits()),
                ("riskDecisions", paper_status_risk_decisions()),
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
                "accounts",
                "cash",
                "positionEvidence",
                "configurationDigestSha256",
                "simulation",
                "reconciliation",
                "riskLimits",
                "riskDecisions",
            ],
        ),
    ])
}

fn paper_status_bounded_evidence(items: Value) -> Value {
    closed(
        vec![
            ("rows", array(items)),
            ("returnedItems", unsigned()),
            ("availableItems", unsigned()),
        ],
        &["rows", "returnedItems", "availableItems"],
    )
}

fn paper_status_account() -> Value {
    closed(
        vec![
            ("accountId", uuid()),
            ("revision", bounded_unsigned_range(1, u64::MAX)),
            ("eligible", boolean()),
            ("currency", text()),
            ("settledCapital", money()),
            ("markedEquity", money()),
            ("peakMarkedEquity", money()),
            ("grossExposure", money()),
            ("unrealizedPnl", money()),
            ("realizedPnl", money()),
            ("realizedLoss", money()),
            ("drawdown", money()),
            ("markDigestSha256", sha256()),
        ],
        &[
            "accountId",
            "revision",
            "eligible",
            "currency",
            "settledCapital",
            "markedEquity",
            "peakMarkedEquity",
            "grossExposure",
            "unrealizedPnl",
            "realizedPnl",
            "realizedLoss",
            "drawdown",
            "markDigestSha256",
        ],
    )
}

fn paper_status_cash_balance() -> Value {
    closed(
        vec![("accountId", uuid()), ("balance", money())],
        &["accountId", "balance"],
    )
}

fn paper_status_position() -> Value {
    closed(
        vec![
            ("accountId", uuid()),
            ("instrumentId", uuid()),
            ("lots", integer()),
            ("costBasis", money()),
        ],
        &["accountId", "instrumentId", "lots", "costBasis"],
    )
}

fn paper_status_simulation() -> Value {
    closed(
        vec![
            ("configurationVersion", bounded_unsigned_range(1, u64::MAX)),
            ("minimumLatencyNanos", paper_status_nonnegative_integer()),
            ("maximumLatencyNanos", paper_status_nonnegative_integer()),
            ("cancelLatencyNanos", paper_status_nonnegative_integer()),
            ("maximumMarkAgeNanos", paper_status_positive_integer()),
            (
                "maximumParticipationBasisPoints",
                bounded_unsigned_range(1, 10_000),
            ),
            ("impactBasisPointsPerLevel", bounded_unsigned(10_000)),
            ("makerFeeBasisPoints", bounded_unsigned(10_000)),
            ("takerFeeBasisPoints", bounded_unsigned(10_000)),
            ("minimumFee", money()),
            ("maximumFee", nullable(money())),
        ],
        &[
            "configurationVersion",
            "minimumLatencyNanos",
            "maximumLatencyNanos",
            "cancelLatencyNanos",
            "maximumMarkAgeNanos",
            "maximumParticipationBasisPoints",
            "impactBasisPointsPerLevel",
            "makerFeeBasisPoints",
            "takerFeeBasisPoints",
            "minimumFee",
            "maximumFee",
        ],
    )
}

fn paper_status_reconciliation() -> Value {
    closed(
        vec![
            ("snapshotSequence", unsigned()),
            ("snapshotComplete", boolean()),
            ("configurationDigestSha256", sha256()),
            ("reconciliationRequired", boolean()),
            ("financialReconciliationCurrent", boolean()),
            ("activeOrderCount", unsigned()),
            ("archivedOrderCount", unsigned()),
            ("fillCount", unsigned()),
            ("accountCount", unsigned()),
            ("cashBalanceCount", unsigned()),
            ("positionCount", unsigned()),
        ],
        &[
            "snapshotSequence",
            "snapshotComplete",
            "configurationDigestSha256",
            "reconciliationRequired",
            "financialReconciliationCurrent",
            "activeOrderCount",
            "archivedOrderCount",
            "fillCount",
            "accountCount",
            "cashBalanceCount",
            "positionCount",
        ],
    )
}

fn paper_status_risk_limits() -> Value {
    closed(
        vec![
            ("currency", text()),
            ("eligibleInstruments", paper_status_bounded_evidence(uuid())),
            ("maximumPositionLots", paper_status_positive_integer()),
            ("maximumOrderNotional", money()),
            ("maximumGrossExposure", money()),
            ("maximumLeverageBasisPoints", bounded_unsigned(1_000_000)),
            ("minimumCapital", money()),
            ("maximumLoss", money()),
            ("maximumDrawdown", money()),
            ("maximumFeeBasisPoints", bounded_unsigned(10_000)),
            ("maximumPriceDeviationBasisPoints", bounded_unsigned(10_000)),
            ("maximumSlippageBasisPoints", bounded_unsigned(10_000)),
            (
                "maximumOrdersPerWindow",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("orderRateWindowNanos", paper_status_positive_integer()),
            ("reservationTtlNanos", paper_status_positive_integer()),
            ("allowShort", boolean()),
            ("killSwitch", boolean()),
        ],
        &[
            "currency",
            "eligibleInstruments",
            "maximumPositionLots",
            "maximumOrderNotional",
            "maximumGrossExposure",
            "maximumLeverageBasisPoints",
            "minimumCapital",
            "maximumLoss",
            "maximumDrawdown",
            "maximumFeeBasisPoints",
            "maximumPriceDeviationBasisPoints",
            "maximumSlippageBasisPoints",
            "maximumOrdersPerWindow",
            "orderRateWindowNanos",
            "reservationTtlNanos",
            "allowShort",
            "killSwitch",
        ],
    )
}

fn paper_status_positive_integer() -> Value {
    json!({"type": "integer", "minimum": 1, "maximum": i64::MAX})
}

fn paper_status_nonnegative_integer() -> Value {
    json!({"type": "integer", "minimum": 0, "maximum": i64::MAX})
}

fn paper_status_risk_decisions() -> Value {
    closed(
        vec![
            ("records", array(paper_status_risk_decision())),
            ("returnedItems", unsigned()),
            ("availableItems", unsigned()),
            ("totalPublished", unsigned()),
            ("oldestSequence", nullable(unsigned())),
            ("latestSequence", nullable(unsigned())),
            ("cursorExpired", boolean()),
            ("nextCursor", nullable(unsigned())),
        ],
        &[
            "records",
            "returnedItems",
            "availableItems",
            "totalPublished",
            "oldestSequence",
            "latestSequence",
            "cursorExpired",
            "nextCursor",
        ],
    )
}

fn paper_status_risk_decision() -> Value {
    closed(
        vec![
            ("sequence", bounded_unsigned_range(1, u64::MAX)),
            (
                "kind",
                enumeration(&[
                    "risk_rejected",
                    "risk_approved",
                    "dispatch_rejected",
                    "dispatch_known_failure",
                    "dispatch_accepted",
                    "dispatch_uncertain",
                    "cancel_accepted",
                    "cancel_terminal",
                    "reconciliation_observed",
                ]),
            ),
            ("approvalId", uuid()),
            ("orderId", uuid()),
            ("accountId", uuid()),
            ("instrumentId", uuid()),
            ("strategyId", uuid()),
            ("modelId", nullable(uuid())),
            ("intentDigestSha256", sha256()),
            ("assessmentDigestSha256", nullable(sha256())),
            ("evidenceBindingDigestSha256", nullable(sha256())),
            ("executionIdentityDigestSha256", nullable(sha256())),
            ("portfolioContentDigestSha256", nullable(sha256())),
            ("maximumExecutionPriceTicks", nullable(integer())),
            ("riskPolicyDigestSha256", sha256()),
            (
                "riskPolicyRulesetVersion",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("marketObservedAt", integer()),
            ("validUntil", integer()),
            ("observedAt", integer()),
            (
                "reasons",
                bounded_array(paper_status_execution_audit_reason(), 64),
            ),
        ],
        &[
            "sequence",
            "kind",
            "approvalId",
            "orderId",
            "accountId",
            "instrumentId",
            "strategyId",
            "modelId",
            "intentDigestSha256",
            "assessmentDigestSha256",
            "evidenceBindingDigestSha256",
            "executionIdentityDigestSha256",
            "portfolioContentDigestSha256",
            "maximumExecutionPriceTicks",
            "riskPolicyDigestSha256",
            "riskPolicyRulesetVersion",
            "marketObservedAt",
            "validUntil",
            "observedAt",
            "reasons",
        ],
    )
}

fn paper_status_execution_audit_reason() -> Value {
    one_of(vec![
        enumeration(&[
            "queue_count_saturated",
            "queue_bytes_saturated",
            "task_ownership_saturated",
            "duplicate_approval",
            "registry_capacity",
            "registry_unavailable",
            "clock_failure",
            "approval_invalid",
            "portfolio_revision_invalid",
            "adapter_rejected",
            "adapter_known_failure",
            "adapter_uncertain",
            "receipt_mismatch",
            "observation_timestamp_invalid",
            "unexpected_reconciliation_order",
            "reconciliation_required",
            "account_replacement_rejected",
            "pending_reconciliation_capacity",
            "operation_deadline_exceeded",
            "audit_reason_overflow",
        ]),
        closed(vec![("risk", paper_status_risk_rejection())], &["risk"]),
    ])
}

fn paper_status_risk_rejection() -> Value {
    one_of(vec![
        enumeration(&[
            "clock_failure",
            "clock_rollback",
            "authority",
            "approval_identity",
            "audit_unavailable",
            "policy_expired",
            "market_depth_unavailable",
            "source_quality",
            "source_ineligible",
            "source_stale",
            "market_timestamp_in_future",
            "market_predates_signal",
            "instrument_not_trading",
            "instrument_definition_mismatch",
            "intent_expired",
            "invalid_reference_price",
            "order_price_limit",
            "stop_not_triggered",
            "intent_slippage_limit",
            "policy_slippage_limit",
            "price_deviation_limit",
        ]),
        closed(
            vec![("account", paper_status_account_risk_violation())],
            &["account"],
        ),
        closed(
            vec![("portfolio", paper_status_portfolio_read_error())],
            &["portfolio"],
        ),
    ])
}

fn paper_status_account_risk_violation() -> Value {
    enumeration(&[
        "kill_switch",
        "account_not_found",
        "account_ineligible",
        "reconciliation_required",
        "instrument_ineligible",
        "currency_mismatch",
        "portfolio_state_mismatch",
        "unsupported_settlement",
        "intent_expired",
        "intent_lifetime_exceeded",
        "duplicate_client_order",
        "duplicate_order",
        "idempotency_capacity",
        "idempotency_revision_exhausted",
        "reservation_capacity",
        "order_rate_limit",
        "order_notional_limit",
        "position_limit",
        "insufficient_position",
        "insufficient_cash",
        "exposure_limit",
        "leverage_limit",
        "capital_limit",
        "loss_limit",
        "drawdown_limit",
        "arithmetic_overflow",
        "account_coordinator_busy",
        "account_coordinator_poisoned",
        "clock_failure",
    ])
}

fn paper_status_portfolio_read_error() -> Value {
    enumeration(&[
        "revoked_capability",
        "missing_account",
        "stale_revision",
        "revoked_revision",
        "query_bound",
        "currency_mismatch",
        "incomplete_basis",
        "content_mismatch",
        "publication_rollback",
        "publication_history_exhausted",
        "publication_generation_exhausted",
        "publication_unavailable",
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

fn bounded_nonempty_array(items: Value, maximum: usize) -> Value {
    json!({"type": "array", "minItems": 1, "maxItems": maximum, "items": items})
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

fn exact_length_text(length: usize) -> Value {
    json!({"type": "string", "minLength": length, "maxLength": length})
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

fn bounded_integer_range(minimum: i64, maximum: i64) -> Value {
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
