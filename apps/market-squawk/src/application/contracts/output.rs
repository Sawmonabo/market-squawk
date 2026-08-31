//! Code-owned structured-result schema families for the production operation registry.

use serde_json::{Map, Value, json};

use market_squawk_adapter_federal_reserve::{
    BOARD_DDP_SOURCE_ID, h15_treasury_constant_maturities_dashboard_series,
};
use market_squawk_decisions::RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED;
use market_squawk_sources::FRED_ALFRED_API_SURFACE_ID;

use super::super::research::{
    MACRO_GET_CONTEXT, MAX_MARKET_HISTORY_BARS, TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION,
    TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION,
};
use super::{
    PRODUCT_LOOKUP_ACTION_OPEN_INVESTMENT, PRODUCT_LOOKUP_ACTION_OPEN_SAVED_SCREEN,
    PRODUCT_LOOKUP_CATEGORIES, PRODUCT_LOOKUP_CATEGORY_INVESTMENT,
    PRODUCT_LOOKUP_CATEGORY_SAVED_SCREEN, PRODUCT_LOOKUP_QUERY_MAXIMUM_CHARACTERS,
};
use crate::provider_activation::FRED_ALFRED_READ_OPERATION;

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
                ("runtime", source_runtime_status()),
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
        "Source.GetCoverage" => source_coverage_rows(),
        "Source.GetHealth" => source_health_rows(),
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
        "Market.GetTrades" => market_trade_rows(),
        "Market.GetQuotes" => market_quote_rows(),
        "Market.GetBooks" => market_book_rows(),
        "Market.GetQuality" => market_rows(&[
            "sourceId",
            "instrumentId",
            "referenceAt",
            "stateBidDepth",
            "stateAskDepth",
        ]),
        "Market.GetComparisons" => market_comparison_rows(),
        "Market.GetUnifiedFeed" => unified_market_rows(),
        "Market.GetOverview" => market_product_page(),
        "Market.GetInstrument" => market_product_selection(),
        "Market.GetHistory" => market_history_result(),
        "Market.SearchUniverse" => market_search_page(),
        "Research.ListDatasets" => nullable(page(generation())),
        "Research.GetManifest" => generation(),
        "Research.GetHistory"
        | "Research.GetAlternativeData"
        | "Fundamental.GetFilings"
        | "Fundamental.GetFacts"
        | "Fundamental.GetStatements"
        | "Fundamental.GetRatios" => observation_result(),
        MACRO_GET_CONTEXT => macro_context(),
        "Macro.GetDashboard" => macro_dashboard(),
        FRED_ALFRED_READ_OPERATION => fred_alfred_latest_known(),
        TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION
        | TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION => treasury_latest_known(operation),
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
        | "Analysis.StartBacktest"
        | "Decision.RunScreen" => job_receipt(),
        "Analysis.StartPreparedBacktest" => queued_product_start(),
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
                ("reviewToken", uuid()),
                ("accountId", text()),
                ("state", enumeration(&["ready", "already_saved"])),
                ("recordCount", unsigned()),
                ("transactionCount", unsigned()),
                ("dataIssueCount", unsigned()),
                ("transactions", array(portfolio_import_transaction())),
                ("requiresCorporateActionReview", boolean()),
            ],
            &[
                "reviewToken",
                "accountId",
                "state",
                "recordCount",
                "transactionCount",
                "dataIssueCount",
                "transactions",
                "requiresCorporateActionReview",
            ],
        ),
        "Portfolio.ApproveStagedImport" => closed(
            vec![
                ("approvalToken", uuid()),
                ("reviewToken", uuid()),
                ("status", enumeration(&["approved", "promoting"])),
            ],
            &["approvalToken", "reviewToken", "status"],
        ),
        "Portfolio.CommitStagedImport" => {
            closed(vec![("accepted", constant_bool(true))], &["accepted"])
        }
        "Portfolio.DiscardStagedImport" => closed(
            vec![
                ("reviewToken", uuid()),
                ("status", enumeration(&["discarded"])),
            ],
            &["reviewToken", "status"],
        ),
        "Portfolio.GetRecommendationSetup" => recommendation_setup_status(),
        "Portfolio.PreviewRecommendationSetup" => recommendation_setup_preview(),
        "Portfolio.CommitRecommendationSetup" => recommendation_setup_receipt(),
        "Portfolio.ListAccounts" => array(portfolio_account()),
        "Portfolio.ListRevisions" => nullable_rows(portfolio_snapshot()),
        "Portfolio.GetHoldings" => array(portfolio_holding()),
        "Portfolio.GetTransactions" => array(portfolio_transaction()),
        "Portfolio.GetPerformance" => portfolio_performance(),
        "Portfolio.GetExposure" => portfolio_exposure(),
        "Portfolio.GetRisk" => portfolio_risk(),
        "Portfolio.GetAttribution" => portfolio_attribution(),
        "Portfolio.EvaluateScenario" => portfolio_scenario(false),
        "Portfolio.EvaluateScenarioBatch" => portfolio_scenario(true),
        "Portfolio.ProposeRebalance" => portfolio_rebalance(),
        "Portfolio.EvaluateCandidateImpact" => portfolio_candidate_impact(),
        "Analysis.GetReturns" => closed(
            vec![
                ("manifest", manifest()),
                ("returnKind", enumeration(&["price", "total"])),
                ("values", array(number())),
            ],
            &["manifest", "returnKind", "values"],
        ),
        "Analysis.Lookup" => product_lookup_result(),
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
        "Analysis.ListProductBacktests" => backtest_activity_page(),
        "Analysis.GetProductBacktest" => product_backtest_result(),
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
        "Model.ListBundles" => model_evidence_page(),
        "Model.ListProductActivity" => model_activity_page(),
        "Model.Evaluate" => model_output(true),
        "Model.Predict" => model_output(false),
        "Model.StartTraining" => job_receipt(),
        "Model.StartPreparedForecast" => queued_product_start(),
        "Model.GetForecastPreparation" => forecast_preparation_options(),
        "Model.PrepareForecast" => forecast_preparation_preview(),
        "Model.GenerateForecast" | "Model.GetForecast" => product_forecast_detail(),
        "Model.SelectLatestValidForecast" => latest_valid_forecast(),
        "Model.ListForecasts" => product_forecast_page(),
        "Model.GetForecastOutcomes" => product_forecast_outcomes(),
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
                ("selectedAt", canonical_market_timestamp()),
                ("requiredEvidence", bounded_array(text(), 4)),
                ("portfolioImpactAvailable", boolean()),
                (
                    "forecastOptions",
                    bounded_array(dossier_forecast_option(), 64),
                ),
                (
                    "fairValueOptions",
                    bounded_array(dossier_fair_value_option(), 64),
                ),
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
                ("assembledAt", canonical_market_timestamp()),
                ("receiptExpiresAt", canonical_market_timestamp()),
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
            ("assembledAt", canonical_market_timestamp()),
            ("evidence", record()),
            ("references", array(record())),
        ]),
        "Decision.GetTargetPreparation" => closed(
            vec![
                ("dossierId", text()),
                ("instrumentId", uuid()),
                ("assembledAt", canonical_market_timestamp()),
                ("forecastOptions", bounded_array(record(), 4_096)),
                ("fairValueAvailable", boolean()),
                ("portfolioAvailable", boolean()),
                (
                    "referenceMarks",
                    bounded_array(target_reference_mark_option(), 4_096),
                ),
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
                ("receiptExpiresAt", canonical_market_timestamp()),
                ("targetId", text()),
                ("revision", unsigned()),
                ("dossierId", text()),
                ("instrumentId", uuid()),
                ("intent", enumeration(&["buy", "sell", "hold"])),
                ("referenceMark", record()),
                ("referenceMarkObservedAt", canonical_market_timestamp()),
                ("referenceMarkQuality", text()),
                ("referenceMarkSource", text()),
                ("prices", record()),
                ("method", text()),
                ("assumptions", bounded_array(record(), 4_096)),
                ("thesis", text()),
                ("risks", bounded_array(text(), 4_096)),
                ("invalidationConditions", bounded_array(text(), 4_096)),
                ("createdAt", canonical_market_timestamp()),
                ("horizonAt", canonical_market_timestamp()),
                ("expiresAt", canonical_market_timestamp()),
                ("reviewDueAt", canonical_market_timestamp()),
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
        "Decision.GetScreen" => product_lookup_saved_screen_match(),
        "Decision.ListScreenRuns" => closed(
            vec![
                (
                    "runs",
                    bounded_array(
                        signature(vec![
                            ("id", text()),
                            ("screenId", text()),
                            ("screenRevision", unsigned()),
                            ("asOf", canonical_market_timestamp()),
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
        "FairValue.GetWorkspace" => fair_value_workspace(),
        "FairValue.PreviewGovernanceAction" => fair_value_governance_preview_result(),
        "FairValue.CommitGovernanceAction" => fair_value_governance_commit(),
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
                ("measurement", fair_value_measurement_detail()),
                ("created", boolean()),
                ("classified", boolean()),
            ],
            &["measurement", "created", "classified"],
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
        "Bot.GetStartPreparation" => paper_start_preparation(),
        "Bot.PrepareStart" => paper_start_preview(),
        "Bot.Start" => paper_start_result(),
        "Bot.Stop" | "Risk.TriggerKillSwitch" => paper_stop_result(),
        "Execution.GetOrders" => nullable_rows(paper_order()),
        "Execution.GetFills" => nullable_rows(paper_fill()),
        "Execution.GetManualPaperTargets" => closed(
            vec![("targets", bounded_array(manual_paper_target(), 100))],
            &["targets"],
        ),
        "Execution.PrepareManualPaperDraft" => manual_paper_preview(),
        "Execution.SubmitManualPaperDraft" => closed(
            vec![
                ("accepted", constant_bool(true)),
                ("message", bounded_text(512)),
            ],
            &["accepted", "message"],
        ),
        "Execution.Cancel" => paper_cancel_result(),
        "Execution.Reconcile" => closed(
            vec![
                ("observedAt", integer()),
                ("ordersChecked", unsigned()),
                ("accountsChecked", unsigned()),
                ("marketDataReady", boolean()),
                ("reconciliationRequired", boolean()),
            ],
            &[
                "observedAt",
                "ordersChecked",
                "accountsChecked",
                "marketDataReady",
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
    closed_complete(vec![
        ("actionToken", investment_analysis_action_token()),
        ("investment", investment_analysis_display()),
        ("portfolioLabel", bounded_text(128)),
        ("currency", investment_analysis_currency()),
        ("recommendation", investment_analysis_recommendation()),
        ("horizon", investment_analysis_horizon()),
        ("priceSummary", investment_analysis_price_summary()),
        (
            "reasons",
            bounded_nonempty_array(investment_analysis_product_text(), 32),
        ),
        (
            "risks",
            bounded_array(investment_analysis_product_text(), 32),
        ),
        (
            "assumptions",
            bounded_array(investment_analysis_product_text(), 32),
        ),
        (
            "invalidators",
            bounded_array(investment_analysis_product_text(), 32),
        ),
        ("evidenceSummary", investment_analysis_evidence_summary()),
        (
            "analyticalEvidence",
            investment_analysis_analytical_evidence(),
        ),
        ("liquidity", investment_analysis_liquidity()),
        ("portfolioContext", investment_analysis_portfolio_context()),
        (
            "outcomeProjection",
            nullable(investment_analysis_outcome_projection()),
        ),
        ("sizing", nullable(investment_analysis_sizing_projection())),
        (
            "virtualPaperEligibility",
            investment_analysis_virtual_paper_eligibility(),
        ),
        (
            "realizedOutcome",
            nullable(recommendation_outcome_current()),
        ),
        (
            "trackRecordActionToken",
            nullable(investment_analysis_action_token()),
        ),
    ])
}

fn investment_analysis_action_token() -> Value {
    json!({
        "type": "string",
        "format": "uuid",
        "not": {
            "type": "string",
            "const": "00000000-0000-0000-0000-000000000000",
        },
    })
}

fn investment_analysis_display() -> Value {
    closed_complete(vec![
        ("symbol", nullable(bounded_text(64))),
        ("name", nullable(investment_analysis_product_text())),
    ])
}

fn investment_analysis_recommendation() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("action")),
            ("action", investment_analysis_action()),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("abstain")),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_horizon() -> Value {
    closed_complete(vec![
        ("informationCurrentThrough", canonical_market_timestamp()),
        ("endsAt", canonical_market_timestamp()),
        ("expiresAt", canonical_market_timestamp()),
    ])
}

fn investment_analysis_price_summary() -> Value {
    closed_complete(vec![
        ("current", nullable(investment_analysis_money())),
        ("fairValue", nullable(investment_analysis_money())),
        ("scenarios", nullable(investment_analysis_scenario_ranges())),
        (
            "actionRanges",
            nullable(investment_analysis_action_ranges()),
        ),
    ])
}

fn investment_analysis_scenario_ranges() -> Value {
    closed_complete(vec![
        ("endsAt", canonical_market_timestamp()),
        ("downside", investment_analysis_price_range()),
        ("base", investment_analysis_price_range()),
        ("upside", investment_analysis_price_range()),
    ])
}

fn investment_analysis_action_ranges() -> Value {
    closed_complete(vec![
        ("entry", investment_analysis_price_range()),
        ("add", investment_analysis_price_range()),
        ("trim", investment_analysis_price_range()),
        ("exit", investment_analysis_price_range()),
    ])
}

fn investment_analysis_price_range() -> Value {
    closed_complete(vec![
        ("lower", investment_analysis_money()),
        ("upper", investment_analysis_money()),
    ])
}

fn investment_analysis_money() -> Value {
    closed_complete(vec![
        ("amount", investment_analysis_positive_decimal()),
        ("currency", investment_analysis_currency()),
    ])
}

fn investment_analysis_evidence_summary() -> Value {
    closed_complete(vec![
        ("coverage", investment_analysis_coverage()),
        ("calibration", investment_analysis_calibration()),
        ("outOfSample", investment_analysis_out_of_sample()),
        (
            "historicalTest",
            nullable(investment_analysis_historical_test()),
        ),
        ("costs", investment_analysis_cost_summary()),
        ("uncertainty", investment_analysis_uncertainty()),
    ])
}

fn investment_analysis_coverage() -> Value {
    closed_complete(vec![
        ("availableCount", bounded_unsigned(10)),
        ("possibleCount", constant_unsigned(10)),
        (
            "items",
            fixed_array(investment_analysis_coverage_item(), 10),
        ),
        ("summary", investment_analysis_product_text()),
    ])
}

fn investment_analysis_coverage_item() -> Value {
    closed_complete(vec![
        (
            "kind",
            enumeration(&[
                "current_market",
                "broader_research",
                "price_pattern",
                "forecast",
                "financial_model",
                "valuation",
                "historical_test",
                "out_of_sample",
                "liquidity",
                "portfolio_risk",
            ]),
        ),
        ("state", enumeration(&["available", "unavailable"])),
    ])
}

fn investment_analysis_calibration() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("nominalCoveragePercent", investment_analysis_percentage()),
            ("realizedCoveragePercent", investment_analysis_percentage()),
            (
                "completedOutcomes",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_out_of_sample() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            (
                "completedObservations",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            (
                "totalSignals",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("folds", bounded_unsigned_range(1, u64::from(u32::MAX))),
            (
                "completionCoveragePercent",
                investment_analysis_percentage(),
            ),
            ("evaluatedFrom", canonical_market_timestamp()),
            ("evaluatedThrough", canonical_market_timestamp()),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_historical_test() -> Value {
    closed_complete(vec![
        ("netReturnPercent", canonical_decimal_text()),
        ("maximumDrawdownPercent", investment_analysis_percentage()),
        (
            "observations",
            bounded_unsigned_range(1, u64::from(u32::MAX)),
        ),
        ("trials", bounded_unsigned_range(1, u64::from(u32::MAX))),
        ("stabilityPercent", investment_analysis_percentage()),
        ("evaluatedThrough", canonical_market_timestamp()),
        ("summary", investment_analysis_product_text()),
    ])
}

fn investment_analysis_cost_summary() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("modeled")),
            ("feePercent", investment_analysis_percentage()),
            ("slippagePercent", investment_analysis_percentage()),
            (
                "maximumRandomSlippagePercent",
                investment_analysis_percentage(),
            ),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_uncertainty() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            (
                "evidenceReliabilityPercent",
                investment_analysis_percentage(),
            ),
            (
                "components",
                fixed_array(investment_analysis_uncertainty_component(), 6),
            ),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_uncertainty_component() -> Value {
    closed_complete(vec![
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
        ("reliabilityPercent", investment_analysis_percentage()),
    ])
}

fn investment_analysis_evidence_family() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_analytical_evidence() -> Value {
    closed_complete(vec![
        ("currentMarket", investment_analysis_evidence_family()),
        ("broaderResearch", investment_analysis_evidence_family()),
        ("pricePattern", investment_analysis_evidence_family()),
        ("forecast", investment_analysis_evidence_family()),
        ("financialModel", investment_analysis_evidence_family()),
        ("valuation", investment_analysis_evidence_family()),
        ("historicalTest", investment_analysis_evidence_family()),
        ("outOfSample", investment_analysis_evidence_family()),
        ("liquidity", investment_analysis_evidence_family()),
        ("portfolioRisk", investment_analysis_evidence_family()),
        (
            "combination",
            closed_complete(vec![
                ("state", enumeration(&["multi_evidence", "insufficient"])),
                ("summary", investment_analysis_product_text()),
            ]),
        ),
    ])
}

fn investment_analysis_liquidity() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("quotedSpreadPercent", canonical_decimal_text()),
            (
                "policyRelativeCapacityPercent",
                investment_analysis_percentage(),
            ),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_portfolio_context() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("portfolioLabel", bounded_text(128)),
            (
                "positionState",
                enumeration(&["no_position", "current_position"]),
            ),
            ("riskCapacityPercent", investment_analysis_percentage()),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_virtual_paper_eligibility() -> Value {
    closed_complete(vec![
        ("state", constant("not_eligible")),
        ("executionAuthority", constant("none")),
        ("requiresExplicitPaperApproval", constant_bool(true)),
        ("requiresFreshRiskCheck", constant_bool(true)),
        ("summary", investment_analysis_product_text()),
    ])
}

fn investment_analysis_outcome_projection() -> Value {
    closed_complete(vec![
        ("startingPrice", investment_analysis_money()),
        ("endsAt", canonical_market_timestamp()),
        (
            "positionScale",
            nullable(closed_complete(vec![
                ("quantityLots", unsigned_integer_text()),
                ("summary", investment_analysis_product_text()),
            ])),
        ),
        ("downside", investment_analysis_price_change_range()),
        ("base", investment_analysis_price_change_range()),
        ("upside", investment_analysis_price_change_range()),
        ("expectedReturn", investment_analysis_expected_return()),
        (
            "expectedGrossPricePnl",
            investment_analysis_expected_gross_price_pnl(),
        ),
        (
            "netPnl",
            investment_analysis_unavailable_projection_metric(),
        ),
        (
            "benchmarkReturn",
            investment_analysis_unavailable_projection_metric(),
        ),
        (
            "afterTaxPnl",
            investment_analysis_unavailable_projection_metric(),
        ),
        (
            "limitations",
            bounded_nonempty_array(investment_analysis_product_text(), 8),
        ),
    ])
}

fn investment_analysis_price_change_range() -> Value {
    closed(
        vec![
            ("priceRange", investment_analysis_price_range()),
            (
                "absolutePriceChange",
                investment_analysis_signed_money_range(),
            ),
            ("grossPricePnl", investment_analysis_gross_price_pnl()),
            (
                "priceChangePercent",
                closed_complete(vec![
                    ("lower", canonical_decimal_text()),
                    ("upper", canonical_decimal_text()),
                ]),
            ),
        ],
        &["priceRange", "absolutePriceChange", "grossPricePnl"],
    )
}

fn investment_analysis_signed_money() -> Value {
    closed_complete(vec![
        ("amount", canonical_decimal_text()),
        ("currency", investment_analysis_currency()),
    ])
}

fn investment_analysis_signed_money_range() -> Value {
    closed_complete(vec![
        ("lower", investment_analysis_signed_money()),
        ("upper", investment_analysis_signed_money()),
    ])
}

fn investment_analysis_gross_price_pnl() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("range", investment_analysis_signed_money_range()),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_expected_return() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            (
                "grossPriceReturnPercent",
                nullable(canonical_decimal_text()),
            ),
            (
                "exactRatio",
                closed_complete(vec![
                    ("numerator", investment_analysis_signed_money()),
                    ("denominator", investment_analysis_money()),
                ]),
            ),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_expected_gross_price_pnl() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("amount", investment_analysis_signed_money()),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_unavailable_projection_metric() -> Value {
    closed_complete(vec![
        ("state", constant("unavailable")),
        ("summary", investment_analysis_product_text()),
    ])
}

fn investment_analysis_sizing_projection() -> Value {
    closed_complete(vec![
        ("evaluatedAt", canonical_market_timestamp()),
        ("currentLots", unsigned_integer_text()),
        ("hardFeasibleLots", investment_analysis_feasible_lots()),
        ("preferredFeasibleLots", investment_analysis_feasible_lots()),
        ("summary", investment_analysis_product_text()),
    ])
}

fn investment_analysis_feasible_lots() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("available")),
            ("lower", unsigned_integer_text()),
            ("upper", unsigned_integer_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            (
                "reasons",
                bounded_nonempty_array(investment_analysis_product_text(), 8),
            ),
        ]),
    ])
}

fn recommendation_outcome_current() -> Value {
    closed_complete(vec![
        ("evaluatedAt", canonical_market_timestamp()),
        ("result", recommendation_outcome_result()),
    ])
}

fn recommendation_outcome_result() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("pending")),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("completed")),
            ("metric", constant("gross_instrument_price_return")),
            ("startMark", investment_analysis_money()),
            ("endpointPrice", investment_analysis_money()),
            ("grossPriceReturnPercent", canonical_decimal_text()),
            ("observedAt", canonical_market_timestamp()),
            ("availableAt", canonical_market_timestamp()),
            (
                "limitations",
                bounded_nonempty_array(investment_analysis_product_text(), 8),
            ),
        ]),
    ])
}

fn recommendation_track_record() -> Value {
    closed_complete(vec![
        ("actionToken", investment_analysis_action_token()),
        ("evaluatedAt", canonical_market_timestamp()),
        (
            "unavailableAnalysisCount",
            bounded_unsigned(u64::from(u32::MAX)),
        ),
        (
            "minimumCompletedSamples",
            constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED)),
        ),
        ("minimumCoveragePercent", constant("80")),
        ("groups", recommendation_track_record_groups()),
        ("forecastCalibrationIncluded", constant_bool(false)),
        ("executionResultsIncluded", constant_bool(false)),
        ("summary", investment_analysis_product_text()),
    ])
}

fn recommendation_track_record_groups() -> Value {
    json!({
        "type": "array",
        "minItems": 6,
        "maxItems": 6,
        "prefixItems": [
            recommendation_track_record_group("buy"),
            recommendation_track_record_group("add"),
            recommendation_track_record_group("hold"),
            recommendation_track_record_group("trim"),
            recommendation_track_record_group("sell"),
            recommendation_track_record_group("abstain"),
        ],
        "items": false,
    })
}

fn recommendation_track_record_group(action: &'static str) -> Value {
    closed_complete(vec![
        ("action", constant(action)),
        ("recommendationCount", bounded_unsigned(u64::from(u32::MAX))),
        ("dueCount", bounded_unsigned(u64::from(u32::MAX))),
        ("completedCount", bounded_unsigned(u64::from(u32::MAX))),
        ("pendingCount", bounded_unsigned(u64::from(u32::MAX))),
        ("unavailableCount", bounded_unsigned(u64::from(u32::MAX))),
        ("coveragePercent", investment_analysis_percentage()),
        ("performance", recommendation_track_record_performance()),
    ])
}

fn recommendation_track_record_performance() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
            (
                "required",
                constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED)),
            ),
            ("actual", bounded_unsigned(u64::from(u32::MAX))),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", investment_analysis_product_text()),
            ("requiredPercent", constant("80")),
            ("actualPercent", investment_analysis_percentage()),
        ]),
        closed_complete(vec![
            ("kind", constant("available")),
            ("meanGrossPriceReturnPercent", canonical_decimal_text()),
            ("positiveOutcomes", bounded_unsigned(u64::from(u32::MAX))),
            ("unchangedOutcomes", bounded_unsigned(u64::from(u32::MAX))),
            ("negativeOutcomes", bounded_unsigned(u64::from(u32::MAX))),
            ("summary", investment_analysis_product_text()),
        ]),
    ])
}

fn investment_analysis_page() -> Value {
    closed_complete(vec![
        ("completeness", enumeration(&["complete", "truncated"])),
        ("returnedCount", bounded_unsigned(1_000)),
        ("availableCount", bounded_unsigned(4_096)),
        (
            "nextAfterActionToken",
            nullable(investment_analysis_action_token()),
        ),
        (
            "analyses",
            bounded_array(investment_analysis_locator(), 1_000),
        ),
    ])
}

fn investment_analysis_locator() -> Value {
    closed_complete(vec![
        ("actionToken", investment_analysis_action_token()),
        ("investment", investment_analysis_display()),
        ("portfolioLabel", bounded_text(128)),
        ("currency", investment_analysis_currency()),
        ("horizon", investment_analysis_horizon()),
        ("recommendation", investment_analysis_recommendation()),
    ])
}

fn investment_analysis_action() -> Value {
    enumeration(&["buy", "add", "hold", "trim", "sell"])
}

fn investment_analysis_product_text() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 2_048,
        "pattern": "\\S",
    })
}

fn investment_analysis_positive_decimal() -> Value {
    json!({
        "type": "string",
        "pattern": "^(?:0|[1-9][0-9]*)(?:\\.[0-9]*[1-9])?$",
        "not": {
            "type": "string",
            "const": "0",
        },
    })
}

fn investment_analysis_percentage() -> Value {
    json!({
        "type": "string",
        "pattern": "^(?:(?:0|[1-9]|[1-9][0-9])(?:\\.[0-9]*[1-9])?|100)$",
    })
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

fn dossier_forecast_option() -> Value {
    closed_complete(vec![
        ("selector", bounded_text(256)),
        ("modelId", bounded_text(256)),
        ("bundleId", bounded_text(256)),
        ("bundleVersion", positive_integer()),
        ("observedThrough", canonical_market_timestamp()),
        ("createdAt", canonical_market_timestamp()),
        ("expiresAt", canonical_market_timestamp()),
        (
            "horizonPoints",
            bounded_unsigned_range(1, u64::from(u16::MAX)),
        ),
        ("horizonStepNanos", positive_integer()),
        ("calibrated", boolean()),
        ("quality", constant("modeled")),
    ])
}

fn dossier_fair_value_option() -> Value {
    closed_complete(vec![
        ("selector", bounded_text(256)),
        ("accountId", bounded_text(256)),
        (
            "amount",
            closed_complete(vec![
                ("amount", canonical_decimal_text()),
                ("currency", investment_analysis_currency()),
                ("scale", bounded_unsigned(28)),
                (
                    "amountBasis",
                    enumeration(&[
                        "per_instrument_unit",
                        "reporting_entity_total",
                        "position_total",
                    ]),
                ),
            ]),
        ),
        ("measurementAt", canonical_market_timestamp()),
        ("preparedAt", canonical_market_timestamp()),
        (
            "method",
            enumeration(&[
                "quoted_market_price",
                "market_approach",
                "income_approach",
                "cost_approach",
            ]),
        ),
        (
            "hierarchy",
            enumeration(&["level_1", "level_2", "level_3", "unclassified"]),
        ),
        ("reasonCount", bounded_unsigned(10_000)),
    ])
}

fn target_reference_mark_option() -> Value {
    closed_complete(vec![
        ("selector", uuid()),
        ("price", investment_analysis_money()),
        ("observedAt", canonical_market_timestamp()),
        ("quality", market_quality()),
        ("source", bounded_text(256)),
    ])
}

fn source_coverage_rows() -> Value {
    nullable_rows(closed_complete(vec![
        ("surfaceId", bounded_text(512)),
        (
            "releaseState",
            enumeration(&[
                "available",
                "rights_limited",
                "refresh_required",
                "rights_blocked",
            ]),
        ),
        ("declaredCoverage", text()),
        ("qualityCeiling", market_quality()),
        ("rights", bounded_array(data_use_right(), 6)),
        ("runtimeCoverage", source_runtime_coverage()),
    ]))
}

fn source_runtime_coverage() -> Value {
    one_of(vec![
        closed_complete(vec![("state", constant("not_established"))]),
        closed_complete(vec![
            ("state", constant("established")),
            ("sourceId", bounded_text(128)),
            ("venueId", bounded_text(64)),
            ("instrumentId", uuid()),
            ("providerProduct", bounded_text(512)),
            ("providerChannel", bounded_text(512)),
            ("eventClass", live_event_class()),
            ("marketDepth", nullable(market_depth())),
            ("delay", market_coverage_delay()),
            (
                "consolidation",
                enumeration(&["single_venue", "partial", "consolidated"]),
            ),
            ("effectiveFromUnixNanos", integer_text()),
            ("effectiveUntilUnixNanos", nullable(integer_text())),
            ("metadataRevision", bounded_text(512)),
            ("status", market_coverage_status()),
        ]),
    ])
}

fn source_health_rows() -> Value {
    nullable_rows(closed_complete(vec![
        ("surfaceId", bounded_text(512)),
        ("onboardingState", nullable(source_onboarding_state())),
        ("runtimeHealth", source_runtime_health()),
    ]))
}

fn source_onboarding_state() -> Value {
    enumeration(&[
        "unavailable",
        "anonymous_available",
        "user_action_required",
        "credential_imported_unverified",
        "protocol_validated",
        "stored_unverified",
        "secret_reconciliation_required",
        "verified_least_privilege",
        "rights_admission_pending",
        "runtime_verification_pending",
        "active_scoped",
        "renewal_required",
        "refresh_required",
        "rotation_pending",
        "revocation_unconfirmed",
        "indeterminate_remote_state",
        "cleanup_required",
        "blocked",
    ])
}

fn source_runtime_health() -> Value {
    one_of(vec![
        closed_complete(vec![("state", constant("not_active"))]),
        closed_complete(vec![
            ("state", constant("active")),
            ("sourceId", bounded_text(128)),
            ("venueId", bounded_text(64)),
            ("instrumentId", uuid()),
            ("connectionGeneration", positive_integer_text()),
            ("sessionId", bounded_text(512)),
            ("healthEpoch", positive_integer_text()),
            ("stateRevision", positive_integer_text()),
            ("assessmentId", bounded_text(512)),
            ("bindingDigest", investment_analysis_sha256()),
            ("connection", market_connection_liveness()),
            ("transportFreshness", market_transport_freshness()),
            ("marketFreshness", market_observation_freshness()),
            (
                "sourceTimestampFreshness",
                market_source_timestamp_freshness(),
            ),
            ("streamIntegrity", source_stream_integrity()),
            ("captureIntegrity", market_capture_integrity()),
            ("coverageStatus", market_coverage_status()),
            ("quality", market_quality()),
            ("observedAtUnixNanos", integer_text()),
            ("qualificationEvaluatedAtUnixNanos", integer_text()),
            ("qualificationValidUntilUnixNanos", integer_text()),
        ]),
    ])
}

fn source_stream_integrity() -> Value {
    enumeration(&[
        "initializing",
        "synchronizing",
        "validating",
        "healthy",
        "stale",
        "gap_detected",
        "checksum_failed",
        "divergent",
        "quarantined",
    ])
}

fn market_rows(required: &[&str]) -> Value {
    let fields = required
        .iter()
        .map(|name| (*name, market_field(name)))
        .collect();
    nullable_rows(signature(fields))
}

fn market_trade_rows() -> Value {
    let mut fields = market_detail_identity_fields();
    fields.extend([
        ("sourceIdentifier", bounded_text(512)),
        ("stableTradeId", bounded_text(512)),
        ("tradeConnectionGeneration", positive_integer_text()),
        ("priceTicks", integer_text()),
        ("quantityLots", unsigned_integer_text()),
        ("aggressorSide", enumeration(&["buy", "sell", "unknown"])),
        (
            "takerOrderType",
            nullable(enumeration(&["limit", "market"])),
        ),
        ("sourceTimestamp", nullable(canonical_market_timestamp())),
        ("receivedAt", canonical_market_timestamp()),
        ("availableAt", canonical_market_timestamp()),
        ("ingestedAt", canonical_market_timestamp()),
        ("recordedQuality", market_quality()),
        ("currentDisplayQuality", market_quality()),
        ("recordedCoverage", market_coverage_status()),
        ("assessmentId", bounded_text(512)),
        ("qualificationEvaluatedAt", canonical_market_timestamp()),
        ("qualificationValidUntil", canonical_market_timestamp()),
        ("freshAtReference", boolean()),
        ("payloadDigest", evidence_digest()),
        ("bindingDigest", investment_analysis_sha256()),
        (
            "tradeTradingStatus",
            enumeration(&["active", "halted", "inactive", "delisted"]),
        ),
        ("committedStateRevision", positive_integer_text()),
        ("authority", constant("not_exposed")),
    ]);
    nullable_rows(closed_complete(fields))
}

fn market_quote_rows() -> Value {
    let mut fields = market_detail_identity_fields();
    fields.extend([
        ("bid", nullable(market_detail_level())),
        ("ask", nullable(market_detail_level())),
        ("sourceTimestamp", null()),
        ("asOf", canonical_market_timestamp()),
        ("stateEvaluatedAt", canonical_market_timestamp()),
        ("recordedQuality", market_quality()),
        ("currentDisplayQuality", market_quality()),
        ("crossed", boolean()),
        ("authority", constant("not_exposed")),
    ]);
    nullable_rows(closed_complete(fields))
}

fn market_book_rows() -> Value {
    let mut fields = market_detail_identity_fields();
    fields.extend([
        ("asOf", canonical_market_timestamp()),
        ("stateEvaluatedAt", canonical_market_timestamp()),
        ("book", market_detail_book()),
        ("currentDisplayQuality", market_quality()),
    ]);
    nullable_rows(closed_complete(fields))
}

fn market_comparison_rows() -> Value {
    nullable_rows(closed_complete(vec![
        ("instrumentId", uuid()),
        ("observationCount", bounded_unsigned(10_000_000)),
        ("comparable", boolean()),
        (
            "observations",
            bounded_array(market_comparison_observation(), 10_000_000),
        ),
        ("authority", constant("not_exposed")),
    ]))
}

fn market_detail_identity_fields() -> Vec<(&'static str, Value)> {
    vec![
        ("sourceId", bounded_text(128)),
        ("venueId", bounded_text(64)),
        ("instrumentId", uuid()),
        ("providerProduct", bounded_text(512)),
        ("providerChannel", bounded_text(512)),
        ("connectionGeneration", positive_integer_text()),
        ("stateRevision", unsigned_integer_text()),
        ("shardId", bounded_text(32)),
        ("shardSnapshotRevision", positive_integer_text()),
    ]
}

fn market_detail_level() -> Value {
    closed_complete(vec![
        ("priceTicks", integer_text()),
        ("quantityLots", unsigned_integer_text()),
    ])
}

fn market_detail_dimension() -> Value {
    closed_complete(vec![
        (
            "completeness",
            enumeration(&["complete", "truncated", "unavailable"]),
        ),
        ("available", bounded_unsigned(u64::from(u32::MAX))),
        ("returned", bounded_unsigned(u64::from(u32::MAX))),
        ("configuredLimit", bounded_unsigned(u64::from(u32::MAX))),
    ])
}

fn market_detail_book() -> Value {
    closed_complete(vec![
        ("configuredDepth", bounded_unsigned(u64::from(u32::MAX))),
        ("stateBidDepth", bounded_unsigned(u64::from(u32::MAX))),
        ("stateAskDepth", bounded_unsigned(u64::from(u32::MAX))),
        ("snapshotBidDimension", market_detail_dimension()),
        ("snapshotAskDimension", market_detail_dimension()),
        ("resultBidDimension", market_detail_dimension()),
        ("resultAskDimension", market_detail_dimension()),
        ("bids", bounded_array(market_detail_level(), 10_000)),
        ("asks", bounded_array(market_detail_level(), 10_000)),
    ])
}

fn market_comparison_observation() -> Value {
    closed_complete(vec![
        ("sourceId", bounded_text(128)),
        ("venueId", bounded_text(64)),
        ("providerProduct", bounded_text(512)),
        ("providerChannel", bounded_text(512)),
        ("bid", nullable(market_detail_level())),
        ("ask", nullable(market_detail_level())),
        ("midpoint", nullable(market_comparison_midpoint())),
        ("asOf", canonical_market_timestamp()),
        ("stateEvaluatedAt", canonical_market_timestamp()),
        ("recordedQuality", market_quality()),
        ("currentDisplayQuality", market_quality()),
    ])
}

fn market_comparison_midpoint() -> Value {
    closed_complete(vec![
        ("numeratorTicks", integer_text()),
        ("denominator", constant("2")),
    ])
}

fn unified_market_rows() -> Value {
    nullable_rows(closed(
        vec![
            ("instrumentId", uuid()),
            ("symbol", bounded_text(512)),
            (
                "symbolKind",
                enumeration(&[
                    "venue_symbol",
                    "provider_subscription_symbol",
                    "instrument_id",
                ]),
            ),
            ("symbolVenueId", nullable(bounded_text(64))),
            ("assetClass", market_asset_class()),
            ("quoteCurrency", investment_analysis_currency()),
            (
                "definitionKind",
                enumeration(&["execution_and_market_data", "execution", "market_data"]),
            ),
            ("definitionRevision", nullable(positive_integer_text())),
            ("referenceRevision", nullable(bounded_text(512))),
            ("displayName", nullable(bounded_text(512))),
            ("tickSize", nullable(canonical_decimal_text())),
            ("lotSize", nullable(canonical_decimal_text())),
            ("executionTermsAvailable", boolean()),
            ("executionEligible", constant_bool(false)),
            (
                "definitionRevisionDigest",
                nullable(nonzero_sha256_evidence_digest()),
            ),
            ("referenceEvidence", nullable(market_reference_evidence())),
            (
                "availability",
                enumeration(&[
                    "Live",
                    "Delayed",
                    "End of day",
                    "Stored data",
                    "Stale",
                    "Unavailable",
                ]),
            ),
            (
                "confidence",
                enumeration(&[
                    "Verified",
                    "Direct, unverified",
                    "Official delayed",
                    "Aggregated",
                    "Indicative",
                    "Modeled",
                    "Estimated",
                    "Stale",
                    "Unavailable",
                    "No eligible source",
                ]),
            ),
            ("quote", unified_market_quote()),
            ("orderBook", nullable(order_level_book())),
            ("analyticalReadiness", constant("runtime_display_only")),
            ("marketObservation", unified_market_observation()),
            ("selectedSource", nullable(unified_selected_market_source())),
            (
                "alternatives",
                bounded_array(market_source_alternative(), 8),
            ),
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
            "displayName",
            "tickSize",
            "lotSize",
            "executionTermsAvailable",
            "executionEligible",
            "definitionRevisionDigest",
            "referenceEvidence",
            "availability",
            "confidence",
            "quote",
            "orderBook",
            "analyticalReadiness",
            "marketObservation",
            "selectedSource",
            "alternatives",
            "selectionReceipt",
        ],
    ))
}

fn market_product_page() -> Value {
    closed_complete(vec![
        ("data", bounded_array(market_product_row(), 100)),
        (
            "page",
            closed_complete(vec![
                ("hasMore", boolean()),
                ("nextPageToken", nullable(market_token("page"))),
            ]),
        ),
    ])
}

fn market_product_selection() -> Value {
    closed_complete(vec![
        ("data", bounded_nonempty_array(market_product_row(), 1)),
        (
            "page",
            closed_complete(vec![
                ("hasMore", constant_bool(false)),
                ("nextPageToken", json!({"type": "null"})),
            ]),
        ),
    ])
}

fn market_history_result() -> Value {
    closed_complete(vec![
        (
            "data",
            nullable(closed_complete(vec![
                ("historyToken", market_token("history")),
                ("currency", investment_analysis_currency()),
                (
                    "bars",
                    bounded_nonempty_array(market_product_history_bar(), 1_000),
                ),
                ("partial", boolean()),
            ])),
        ),
        (
            "unavailableReason",
            nullable(enumeration(&[
                "not_selected",
                "not_available",
                "temporarily_unavailable",
            ])),
        ),
    ])
}

fn market_product_row() -> Value {
    closed_complete(vec![
        ("selectionToken", market_token("market")),
        ("historyToken", nullable(market_token("history"))),
        (
            "identity",
            closed_complete(vec![
                ("symbol", nullable(bounded_text(64))),
                ("name", nullable(bounded_text(256))),
                ("assetClass", market_asset_class()),
            ]),
        ),
        (
            "price",
            nullable(closed_complete(vec![
                ("value", canonical_decimal_text()),
                ("currency", investment_analysis_currency()),
            ])),
        ),
        ("changePercent", nullable(canonical_decimal_text())),
        ("asOf", nullable(canonical_market_timestamp())),
        (
            "availability",
            enumeration(&["current", "delayed", "previous_close", "unavailable"]),
        ),
    ])
}

fn market_search_page() -> Value {
    closed_complete(vec![
        (
            "data",
            bounded_array(
                closed_complete(vec![
                    ("selectionToken", market_token("market")),
                    ("symbol", nullable(bounded_text(64))),
                    ("name", nullable(bounded_text(256))),
                    (
                        "kind",
                        enumeration(&[
                            "stock",
                            "fund",
                            "bond",
                            "option",
                            "future",
                            "currency",
                            "crypto",
                            "commodity",
                            "index",
                            "cash",
                        ]),
                    ),
                ]),
                100,
            ),
        ),
        (
            "page",
            closed_complete(vec![
                ("hasMore", boolean()),
                ("nextPageToken", nullable(market_token("page"))),
            ]),
        ),
    ])
}

fn market_token(prefix: &str) -> Value {
    json!({
        "type": "string",
        "minLength": prefix.len() + 33,
        "maxLength": prefix.len() + 87,
    })
}

fn market_product_history_bar() -> Value {
    closed_complete(vec![
        ("startsAt", canonical_market_timestamp()),
        ("endsAt", canonical_market_timestamp()),
        ("open", canonical_decimal_text()),
        ("high", canonical_decimal_text()),
        ("low", canonical_decimal_text()),
        ("close", canonical_decimal_text()),
        ("volume", canonical_decimal_text()),
    ])
}

fn market_history_series(kind: &'static str, reason: Option<&'static str>) -> Value {
    let mut fields = vec![
        ("kind", constant(kind)),
        ("instrumentId", uuid()),
        ("period", constant("daily")),
        ("range", constant("latest_complete_window")),
        ("session", constant("completed_trading_sessions")),
        ("adjustment", constant("fully_adjusted")),
        ("currency", investment_analysis_currency()),
        ("coverage", market_history_coverage()),
        ("quality", market_history_quality()),
        (
            "bars",
            bounded_nonempty_array(
                market_history_bar(),
                usize::try_from(MAX_MARKET_HISTORY_BARS).unwrap_or(10_000),
            ),
        ),
    ];
    if let Some(reason) = reason {
        fields.push(("reason", constant(reason)));
    }
    closed_complete(fields)
}

fn market_history_status(kind: &'static str, reasons: &[&str]) -> Value {
    closed_complete(vec![
        ("kind", constant(kind)),
        ("instrumentId", uuid()),
        ("period", constant("daily")),
        ("range", constant("latest_complete_window")),
        ("session", constant("completed_trading_sessions")),
        ("adjustment", constant("fully_adjusted")),
        ("reason", enumeration(reasons)),
    ])
}

fn market_history_coverage() -> Value {
    let maximum = u64::from(MAX_MARKET_HISTORY_BARS);
    closed_complete(vec![
        ("selectedStart", canonical_market_timestamp()),
        ("selectedEndExclusive", canonical_market_timestamp()),
        ("materializedStart", canonical_market_timestamp()),
        ("materializedEndExclusive", canonical_market_timestamp()),
        ("returnedStart", canonical_market_timestamp()),
        ("returnedEndExclusive", canonical_market_timestamp()),
        ("materializedBars", bounded_unsigned(maximum)),
        ("returnedBars", bounded_unsigned(maximum)),
    ])
}

fn market_history_quality() -> Value {
    closed_complete(vec![
        (
            "confidence",
            enumeration(&["high", "moderate", "limited", "stale"]),
        ),
        ("completeTradingSessions", constant_bool(true)),
        (
            "use",
            closed_complete(vec![
                ("charts", constant_bool(true)),
                ("currentResearch", constant_bool(true)),
                ("pointInTimeBacktests", constant_bool(false)),
                ("retrospectiveTraining", constant_bool(false)),
            ]),
        ),
    ])
}

fn market_history_bar() -> Value {
    closed_complete(vec![
        ("periodStart", canonical_market_timestamp()),
        ("periodEndExclusive", canonical_market_timestamp()),
        ("open", canonical_decimal_text()),
        ("high", canonical_decimal_text()),
        ("low", canonical_decimal_text()),
        ("close", canonical_decimal_text()),
        ("volume", canonical_decimal_text()),
        ("tradeCount", nullable(bounded_unsigned(u64::MAX))),
        ("vwap", nullable(canonical_decimal_text())),
    ])
}

fn market_product_current_price() -> Value {
    closed_complete(vec![
        ("value", canonical_decimal_text()),
        ("currency", investment_analysis_currency()),
        ("basis", enumeration(&["last_trade", "bid_ask_midpoint"])),
        ("observedAt", nullable(canonical_market_timestamp())),
        ("currentThrough", nullable(canonical_market_timestamp())),
    ])
}

fn market_product_quote() -> Value {
    closed_complete(vec![
        ("bidPrice", nullable(canonical_decimal_text())),
        ("bidSize", nullable(canonical_decimal_text())),
        ("askPrice", nullable(canonical_decimal_text())),
        ("askSize", nullable(canonical_decimal_text())),
        ("midPrice", nullable(canonical_decimal_text())),
        ("lastPrice", nullable(canonical_decimal_text())),
        ("lastSize", nullable(canonical_decimal_text())),
        ("quoteObservedAt", nullable(canonical_market_timestamp())),
        ("lastObservedAt", nullable(canonical_market_timestamp())),
    ])
}

fn market_product_state() -> Value {
    closed_complete(vec![
        ("freshness", enumeration(&["fresh", "stale", "unavailable"])),
        ("updatedAt", canonical_market_timestamp()),
        ("currentThrough", nullable(canonical_market_timestamp())),
    ])
}

fn market_product_depth_summary() -> Value {
    closed_complete(vec![
        (
            "kind",
            enumeration(&["top_of_book", "price_level", "order_level", "none"]),
        ),
        ("bidLevels", bounded_unsigned(u64::from(u32::MAX))),
        ("askLevels", bounded_unsigned(u64::from(u32::MAX))),
        (
            "individualOrderCount",
            bounded_unsigned(u64::from(u32::MAX)),
        ),
        ("truncated", boolean()),
    ])
}

fn market_product_depth_details() -> Value {
    closed_complete(vec![
        (
            "kind",
            enumeration(&["top_of_book", "price_level", "order_level", "none"]),
        ),
        ("bids", bounded_array(market_product_level(), 64)),
        ("asks", bounded_array(market_product_level(), 64)),
        (
            "individualOrders",
            nullable(market_product_individual_orders()),
        ),
    ])
}

fn market_product_level() -> Value {
    closed_complete(vec![
        ("price", canonical_decimal_text()),
        ("quantity", canonical_decimal_text()),
    ])
}

fn market_product_individual_orders() -> Value {
    closed_complete(vec![
        ("bidOrders", bounded_array(market_product_level(), 64)),
        ("askOrders", bounded_array(market_product_level(), 64)),
        ("totalCount", bounded_unsigned(u64::from(u32::MAX))),
        ("returnedCount", bounded_unsigned(128)),
        ("truncated", boolean()),
    ])
}

fn market_reference_evidence() -> Value {
    closed(
        vec![
            ("referenceRevision", bounded_text(512)),
            ("referencePayloadDigest", evidence_digest()),
            ("quoteCurrencyPayloadDigest", evidence_digest()),
            (
                "referencePayloadLocator",
                nullable(market_payload_locator()),
            ),
            (
                "quoteCurrencyPayloadLocator",
                nullable(market_payload_locator()),
            ),
            ("effectiveFrom", canonical_market_timestamp()),
            ("effectiveUntil", nullable(canonical_market_timestamp())),
        ],
        &[
            "referenceRevision",
            "referencePayloadDigest",
            "quoteCurrencyPayloadDigest",
            "referencePayloadLocator",
            "quoteCurrencyPayloadLocator",
            "effectiveFrom",
            "effectiveUntil",
        ],
    )
}

fn market_payload_locator() -> Value {
    closed(
        vec![
            ("reference", bounded_text(512)),
            ("version", bounded_text(512)),
        ],
        &["reference", "version"],
    )
}

fn unified_market_quote() -> Value {
    closed(
        vec![
            ("bidPrice", nullable(canonical_decimal_text())),
            ("bidPriceProviderLexeme", nullable(bounded_text(128))),
            ("bidSize", nullable(canonical_decimal_text())),
            ("bidSizeProviderLexeme", nullable(bounded_text(128))),
            ("askPrice", nullable(canonical_decimal_text())),
            ("askPriceProviderLexeme", nullable(bounded_text(128))),
            ("askSize", nullable(canonical_decimal_text())),
            ("askSizeProviderLexeme", nullable(bounded_text(128))),
            ("midPrice", nullable(canonical_decimal_text())),
            (
                "midPriceBasis",
                nullable(constant("calculated_from_selected_bid_and_ask")),
            ),
            ("lastPrice", nullable(canonical_decimal_text())),
            ("lastPriceProviderLexeme", nullable(bounded_text(128))),
            ("lastSize", nullable(canonical_decimal_text())),
            ("lastSizeProviderLexeme", nullable(bounded_text(128))),
            (
                "lastSourceTimestamp",
                nullable(canonical_market_timestamp()),
            ),
            ("lastReceivedAt", nullable(canonical_market_timestamp())),
            ("lastAvailableAt", nullable(canonical_market_timestamp())),
            ("lastQuality", nullable(market_quality())),
            ("lastFreshAtSelection", nullable(boolean())),
            (
                "quoteEvidence",
                one_of(vec![
                    null(),
                    display_market_observation_evidence(),
                    kraken_market_projection_evidence(),
                ]),
            ),
            (
                "tradeEvidence",
                nullable(display_market_observation_evidence()),
            ),
        ],
        &[
            "bidPrice",
            "bidPriceProviderLexeme",
            "bidSize",
            "bidSizeProviderLexeme",
            "askPrice",
            "askPriceProviderLexeme",
            "askSize",
            "askSizeProviderLexeme",
            "midPrice",
            "midPriceBasis",
            "lastPrice",
            "lastPriceProviderLexeme",
            "lastSize",
            "lastSizeProviderLexeme",
            "lastSourceTimestamp",
            "lastReceivedAt",
            "lastAvailableAt",
            "lastQuality",
            "lastFreshAtSelection",
            "quoteEvidence",
            "tradeEvidence",
        ],
    )
}

fn display_market_observation_evidence() -> Value {
    closed(
        vec![
            ("sourceIdentifier", bounded_text(512)),
            ("sourceTimestamp", nullable(canonical_market_timestamp())),
            ("effectiveAt", canonical_market_timestamp()),
            ("effectiveTimeBasis", enumeration(&["provider", "received"])),
            ("receivedAt", canonical_market_timestamp()),
            ("availableAt", canonical_market_timestamp()),
            ("metadataRevision", bounded_text(512)),
            ("recordedQuality", market_quality()),
            ("currentDisplayQuality", market_quality()),
            ("displayDepth", nullable(market_depth())),
            ("connectionGeneration", positive_integer_text()),
            ("sessionId", bounded_text(512)),
            ("frameId", positive_integer_text()),
            ("payloadDigest", evidence_digest()),
            ("captureIntegrity", market_capture_integrity()),
            ("decoderRule", bounded_text(512)),
            (
                "decoderRuleVersion",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("timestampRule", bounded_text(512)),
            (
                "timestampRuleVersion",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("availability", display_market_availability()),
            ("coverage", display_market_coverage_evidence()),
        ],
        &[
            "sourceIdentifier",
            "sourceTimestamp",
            "effectiveAt",
            "effectiveTimeBasis",
            "receivedAt",
            "availableAt",
            "metadataRevision",
            "recordedQuality",
            "currentDisplayQuality",
            "displayDepth",
            "connectionGeneration",
            "sessionId",
            "frameId",
            "payloadDigest",
            "captureIntegrity",
            "decoderRule",
            "decoderRuleVersion",
            "timestampRule",
            "timestampRuleVersion",
            "availability",
            "coverage",
        ],
    )
}

fn display_market_coverage_evidence() -> Value {
    closed(
        vec![
            ("providerProduct", bounded_text(512)),
            ("providerChannel", bounded_text(512)),
            ("eventClass", live_event_class()),
            ("declaredDepth", nullable(market_depth())),
            ("delay", market_coverage_delay()),
            (
                "consolidation",
                enumeration(&["single_venue", "partial", "consolidated"]),
            ),
            (
                "delivery",
                enumeration(&["direct_venue", "authorized_broker", "indirect", "unknown"]),
            ),
            ("status", market_coverage_status()),
            ("staticEvidenceDigest", evidence_digest()),
            ("runtimeEvidenceDigest", nullable(evidence_digest())),
            ("effectiveFrom", canonical_market_timestamp()),
            ("effectiveUntil", nullable(canonical_market_timestamp())),
        ],
        &[
            "providerProduct",
            "providerChannel",
            "eventClass",
            "declaredDepth",
            "delay",
            "consolidation",
            "delivery",
            "status",
            "staticEvidenceDigest",
            "runtimeEvidenceDigest",
            "effectiveFrom",
            "effectiveUntil",
        ],
    )
}

fn display_market_availability() -> Value {
    one_of(vec![
        closed(
            vec![
                ("state", constant("fresh")),
                ("staleAfter", canonical_market_timestamp()),
                ("expiresAfter", canonical_market_timestamp()),
            ],
            &["state", "staleAfter", "expiresAfter"],
        ),
        closed(
            vec![
                ("state", constant("stale")),
                ("staleAfter", canonical_market_timestamp()),
                ("expiresAfter", canonical_market_timestamp()),
            ],
            &["state", "staleAfter", "expiresAfter"],
        ),
        closed(
            vec![
                ("state", constant("expired")),
                ("expiredAfter", canonical_market_timestamp()),
            ],
            &["state", "expiredAfter"],
        ),
        closed(
            vec![
                ("state", constant("quarantined")),
                ("failure", bounded_text(512)),
            ],
            &["state", "failure"],
        ),
    ])
}

fn kraken_market_projection_evidence() -> Value {
    closed(
        vec![
            ("surfaceId", bounded_text(512)),
            ("providerId", bounded_text(512)),
            ("providerSymbol", bounded_text(512)),
            ("sourceId", bounded_text(128)),
            ("venueId", bounded_text(64)),
            ("instrumentId", uuid()),
            ("providerInstrument", bounded_text(256)),
            ("connectionGeneration", positive_integer_text()),
            ("batchIdentifier", bounded_text(512)),
            ("revision", unsigned_integer_text()),
            (
                "phase",
                enumeration(&["awaiting_snapshot", "healthy", "quarantined"]),
            ),
            (
                "quarantineReason",
                nullable(order_level_quarantine_reason()),
            ),
            ("quality", market_quality()),
            ("sourceDepth", constant("order_level")),
            ("projectionDepth", constant("price_level")),
            ("executionTerms", market_execution_terms()),
            (
                "freshness",
                enumeration(&["uninitialized", "fresh", "stale"]),
            ),
            ("lastMarketAt", nullable(canonical_market_timestamp())),
            ("sourceTimestamp", canonical_market_timestamp()),
            ("receivedAt", canonical_market_timestamp()),
            ("availableAt", canonical_market_timestamp()),
            ("providerSequence", nullable(unsigned_integer_text())),
            ("diagnosticOrdinal", nullable(unsigned_integer_text())),
            ("sequenceEvidence", market_sequence_evidence()),
            ("checksumEvidence", market_checksum_evidence()),
            ("bidLevelCount", bounded_unsigned(2_000_000)),
            ("askLevelCount", bounded_unsigned(2_000_000)),
        ],
        &[
            "surfaceId",
            "providerId",
            "providerSymbol",
            "sourceId",
            "venueId",
            "instrumentId",
            "providerInstrument",
            "connectionGeneration",
            "batchIdentifier",
            "revision",
            "phase",
            "quarantineReason",
            "quality",
            "sourceDepth",
            "projectionDepth",
            "executionTerms",
            "freshness",
            "lastMarketAt",
            "sourceTimestamp",
            "receivedAt",
            "availableAt",
            "providerSequence",
            "diagnosticOrdinal",
            "sequenceEvidence",
            "checksumEvidence",
            "bidLevelCount",
            "askLevelCount",
        ],
    )
}

fn market_execution_terms() -> Value {
    closed(
        vec![
            ("instrumentId", uuid()),
            ("definitionRevision", positive_integer_text()),
            ("priceTick", canonical_decimal_text()),
            ("lotSize", canonical_decimal_text()),
            ("quoteCurrency", investment_analysis_currency()),
            ("settlementDenomination", market_denomination()),
            ("contractMultiplier", canonical_decimal_text()),
        ],
        &[
            "instrumentId",
            "definitionRevision",
            "priceTick",
            "lotSize",
            "quoteCurrency",
            "settlementDenomination",
            "contractMultiplier",
        ],
    )
}

fn market_denomination() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("currency")),
                ("value", investment_analysis_currency()),
            ],
            &["kind", "value"],
        ),
        closed(
            vec![("kind", constant("asset")), ("value", uuid())],
            &["kind", "value"],
        ),
    ])
}

fn market_sequence_evidence() -> Value {
    one_of(vec![
        closed(
            vec![
                ("capability", constant("provided")),
                ("rule", market_integrity_rule()),
                (
                    "validation_rule",
                    enumeration(&["consecutive", "monotonic"]),
                ),
                ("connection_generation", positive_integer_text()),
                ("snapshot_sequence", nullable(unsigned_integer_text())),
                ("previous_sequence", nullable(unsigned_integer_text())),
                ("observed_sequence", unsigned_integer_text()),
                ("integrity", enumeration(&["valid", "invalid"])),
            ],
            &[
                "capability",
                "rule",
                "validation_rule",
                "connection_generation",
                "snapshot_sequence",
                "previous_sequence",
                "observed_sequence",
                "integrity",
            ],
        ),
        closed(
            vec![
                ("capability", constant("provided")),
                ("rule", market_integrity_rule()),
                (
                    "validation_rule",
                    enumeration(&["consecutive", "monotonic"]),
                ),
                ("connection_generation", positive_integer_text()),
                ("snapshot_sequence", nullable(unsigned_integer_text())),
                ("previous_sequence", null()),
                ("observed_sequence", null()),
                ("integrity", constant("uninitialized")),
            ],
            &[
                "capability",
                "rule",
                "validation_rule",
                "connection_generation",
                "snapshot_sequence",
                "previous_sequence",
                "observed_sequence",
                "integrity",
            ],
        ),
        closed(
            vec![
                ("capability", constant("unsupported")),
                ("rule", null()),
                ("validation_rule", null()),
                ("connection_generation", positive_integer_text()),
                ("snapshot_sequence", null()),
                ("previous_sequence", null()),
                ("observed_sequence", null()),
                ("integrity", constant("not_supported")),
            ],
            &[
                "capability",
                "rule",
                "validation_rule",
                "connection_generation",
                "snapshot_sequence",
                "previous_sequence",
                "observed_sequence",
                "integrity",
            ],
        ),
    ])
}

fn market_checksum_evidence() -> Value {
    one_of(vec![
        closed(
            vec![
                ("capability", constant("provided")),
                ("rule", market_integrity_rule()),
                ("connection_generation", positive_integer_text()),
                ("target", market_checksum_target()),
                ("expected", unsigned_integer_text()),
                ("computed", unsigned_integer_text()),
                ("integrity", enumeration(&["valid", "failed"])),
            ],
            &[
                "capability",
                "rule",
                "connection_generation",
                "target",
                "expected",
                "computed",
                "integrity",
            ],
        ),
        closed(
            vec![
                ("capability", constant("provided")),
                ("rule", market_integrity_rule()),
                ("connection_generation", positive_integer_text()),
                ("target", market_checksum_target()),
                ("expected", null()),
                ("computed", null()),
                ("integrity", constant("unchecked")),
            ],
            &[
                "capability",
                "rule",
                "connection_generation",
                "target",
                "expected",
                "computed",
                "integrity",
            ],
        ),
        closed(
            vec![
                ("capability", constant("unsupported")),
                ("rule", null()),
                ("connection_generation", positive_integer_text()),
                ("target", null()),
                ("expected", null()),
                ("computed", null()),
                ("integrity", constant("not_supported")),
            ],
            &[
                "capability",
                "rule",
                "connection_generation",
                "target",
                "expected",
                "computed",
                "integrity",
            ],
        ),
    ])
}

fn market_integrity_rule() -> Value {
    closed(
        vec![
            ("provider_rule", bounded_text(512)),
            ("version", bounded_unsigned_range(1, u64::from(u32::MAX))),
        ],
        &["provider_rule", "version"],
    )
}

fn market_checksum_target() -> Value {
    one_of(vec![
        closed(
            vec![
                ("kind", constant("book")),
                (
                    "scope",
                    closed(
                        vec![
                            ("depth", market_depth()),
                            (
                                "level_count",
                                bounded_unsigned_range(1, u64::from(u32::MAX)),
                            ),
                            ("provider_scope", bounded_text(512)),
                        ],
                        &["depth", "level_count", "provider_scope"],
                    ),
                ),
            ],
            &["kind", "scope"],
        ),
        closed(
            vec![
                ("kind", constant("payload")),
                (
                    "scope",
                    closed(
                        vec![("provider_scope", bounded_text(512))],
                        &["provider_scope"],
                    ),
                ),
            ],
            &["kind", "scope"],
        ),
    ])
}

fn unified_selected_market_source() -> Value {
    one_of(vec![
        live_selected_market_source(),
        display_selected_market_source(),
        kraken_selected_market_source(),
    ])
}

fn live_selected_market_source() -> Value {
    closed(
        vec![
            ("surfaceId", bounded_text(512)),
            ("providerId", bounded_text(512)),
            ("sourceId", bounded_text(128)),
            ("venueId", nullable(bounded_text(64))),
            ("providerProduct", bounded_text(512)),
            ("providerChannel", bounded_text(512)),
            ("timing", market_timing()),
            ("depth", nullable(market_depth())),
            ("depthLabel", market_depth_label()),
            ("quality", market_quality()),
            ("coverage", market_coverage()),
            ("health", market_source_health()),
            ("healthObservedAt", canonical_market_timestamp()),
            ("stateRevision", unsigned_integer_text()),
            ("shardId", uuid()),
            ("shardSnapshotRevision", positive_integer_text()),
            ("snapshotPublishedAt", canonical_market_timestamp()),
            ("providerBudget", market_source_budget()),
            ("rights", market_source_rights()),
            ("freshness", live_selected_source_freshness()),
            ("integrity", live_selected_source_integrity()),
        ],
        &[
            "surfaceId",
            "providerId",
            "sourceId",
            "venueId",
            "providerProduct",
            "providerChannel",
            "timing",
            "depth",
            "depthLabel",
            "quality",
            "coverage",
            "health",
            "healthObservedAt",
            "stateRevision",
            "shardId",
            "shardSnapshotRevision",
            "snapshotPublishedAt",
            "providerBudget",
            "rights",
            "freshness",
            "integrity",
        ],
    )
}

fn display_selected_market_source() -> Value {
    closed(
        vec![
            ("surfaceId", bounded_text(512)),
            ("providerId", bounded_text(512)),
            ("providerSymbol", bounded_text(512)),
            ("sourceId", bounded_text(128)),
            ("venueId", bounded_text(64)),
            ("providerProduct", bounded_text(512)),
            ("providerChannel", bounded_text(512)),
            ("timing", market_timing()),
            ("depth", nullable(market_depth())),
            ("depthLabel", market_depth_label()),
            ("quality", market_quality()),
            ("coverage", market_coverage()),
            ("coverageStatus", market_coverage_status()),
            ("health", market_source_health()),
            ("healthObservedAt", canonical_market_timestamp()),
            ("stateRevision", unsigned_integer_text()),
            ("snapshotPublishedAt", canonical_market_timestamp()),
            ("providerBudget", market_source_budget()),
            ("rights", market_source_rights()),
            ("freshness", display_selected_source_freshness()),
            ("integrity", display_selected_source_integrity()),
            ("status", nullable(display_selected_source_status())),
        ],
        &[
            "surfaceId",
            "providerId",
            "providerSymbol",
            "sourceId",
            "venueId",
            "providerProduct",
            "providerChannel",
            "timing",
            "depth",
            "depthLabel",
            "quality",
            "coverage",
            "coverageStatus",
            "health",
            "healthObservedAt",
            "stateRevision",
            "snapshotPublishedAt",
            "providerBudget",
            "rights",
            "freshness",
            "integrity",
            "status",
        ],
    )
}

fn kraken_selected_market_source() -> Value {
    closed(
        vec![
            ("surfaceId", bounded_text(512)),
            ("providerId", bounded_text(512)),
            ("providerSymbol", bounded_text(512)),
            ("sourceId", bounded_text(128)),
            ("venueId", bounded_text(64)),
            ("providerProduct", bounded_text(512)),
            ("providerChannel", bounded_text(512)),
            ("timing", market_timing()),
            ("depth", nullable(market_depth())),
            ("depthLabel", market_depth_label()),
            ("sourceDepth", constant("order_level")),
            ("projectionDepth", constant("price_level")),
            ("quality", market_quality()),
            ("qualityCeiling", constant("direct_unverified")),
            ("coverage", market_coverage()),
            ("health", market_source_health()),
            ("healthObservedAt", canonical_market_timestamp()),
            ("stateRevision", unsigned_integer_text()),
            ("snapshotPublishedAt", canonical_market_timestamp()),
            ("executionEligible", constant_bool(false)),
            ("providerBudget", market_source_budget()),
            ("rights", market_source_rights()),
            ("freshness", kraken_selected_source_freshness()),
            ("integrity", kraken_selected_source_integrity()),
            ("sourceMetadataEvidence", market_source_metadata_evidence()),
        ],
        &[
            "surfaceId",
            "providerId",
            "providerSymbol",
            "sourceId",
            "venueId",
            "providerProduct",
            "providerChannel",
            "timing",
            "depth",
            "depthLabel",
            "sourceDepth",
            "projectionDepth",
            "quality",
            "qualityCeiling",
            "coverage",
            "health",
            "healthObservedAt",
            "stateRevision",
            "snapshotPublishedAt",
            "executionEligible",
            "providerBudget",
            "rights",
            "freshness",
            "integrity",
            "sourceMetadataEvidence",
        ],
    )
}

fn market_source_budget() -> Value {
    closed(
        vec![
            (
                "availability",
                enumeration(&[
                    "not_required",
                    "open",
                    "interactive_only",
                    "exhausted",
                    "unknown",
                ]),
            ),
            ("observedAt", canonical_market_timestamp()),
        ],
        &["availability", "observedAt"],
    )
}

fn market_source_rights() -> Value {
    closed(
        vec![
            ("decisionId", bounded_text(512)),
            ("state", enumeration(&["admitted", "unknown", "denied"])),
            ("decidedAt", canonical_market_timestamp()),
            ("effectiveFrom", nullable(canonical_market_timestamp())),
            ("effectiveUntil", nullable(canonical_market_timestamp())),
            ("snapshotDisplayPermitted", boolean()),
        ],
        &[
            "decisionId",
            "state",
            "decidedAt",
            "effectiveFrom",
            "effectiveUntil",
            "snapshotDisplayPermitted",
        ],
    )
}

fn live_selected_source_freshness() -> Value {
    closed(
        vec![
            ("ageNanos", unsigned_integer_text()),
            ("sourceTimestamp", nullable(canonical_market_timestamp())),
            ("receivedAt", canonical_market_timestamp()),
            ("availableAt", canonical_market_timestamp()),
            ("ingestedAt", canonical_market_timestamp()),
            ("sourceValidUntil", canonical_market_timestamp()),
            ("freshAtSelection", boolean()),
        ],
        &[
            "ageNanos",
            "sourceTimestamp",
            "receivedAt",
            "availableAt",
            "ingestedAt",
            "sourceValidUntil",
            "freshAtSelection",
        ],
    )
}

fn live_selected_source_integrity() -> Value {
    closed(
        vec![
            ("state", market_integrity()),
            ("assessedAt", canonical_market_timestamp()),
            ("connectionGeneration", nullable(positive_integer_text())),
            (
                "phase",
                enumeration(&[
                    "disconnected",
                    "awaiting_snapshot",
                    "synchronizing",
                    "healthy",
                    "quarantined",
                ]),
            ),
            ("generationCurrent", boolean()),
            ("snapshotInitialized", boolean()),
            ("lastSequence", nullable(unsigned_integer_text())),
            ("runtimeEvidence", nullable(market_runtime_evidence())),
        ],
        &[
            "state",
            "assessedAt",
            "connectionGeneration",
            "phase",
            "generationCurrent",
            "snapshotInitialized",
            "lastSequence",
            "runtimeEvidence",
        ],
    )
}

fn display_selected_source_freshness() -> Value {
    closed(
        vec![
            ("ageNanos", unsigned_integer_text()),
            ("sourceTimestamp", nullable(canonical_market_timestamp())),
            ("effectiveAt", canonical_market_timestamp()),
            ("receivedAt", canonical_market_timestamp()),
            ("availableAt", canonical_market_timestamp()),
            ("ingestedAt", canonical_market_timestamp()),
            ("sourceValidUntil", nullable(canonical_market_timestamp())),
            ("freshAtSelection", boolean()),
            ("selectedAt", canonical_market_timestamp()),
            ("availability", display_market_availability()),
        ],
        &[
            "ageNanos",
            "sourceTimestamp",
            "effectiveAt",
            "receivedAt",
            "availableAt",
            "ingestedAt",
            "sourceValidUntil",
            "freshAtSelection",
            "selectedAt",
            "availability",
        ],
    )
}

fn display_selected_source_integrity() -> Value {
    closed(
        vec![
            ("state", market_integrity()),
            ("assessedAt", canonical_market_timestamp()),
            ("connectionGeneration", positive_integer_text()),
            (
                "phase",
                enumeration(&["healthy", "stale", "expired", "quarantined"]),
            ),
            ("generationCurrent", null()),
            ("snapshotInitialized", boolean()),
            ("lastSequence", null()),
            ("terminalFailure", nullable(bounded_text(512))),
            ("runtimeEvidence", display_market_observation_evidence()),
        ],
        &[
            "state",
            "assessedAt",
            "connectionGeneration",
            "phase",
            "generationCurrent",
            "snapshotInitialized",
            "lastSequence",
            "terminalFailure",
            "runtimeEvidence",
        ],
    )
}

fn display_selected_source_status() -> Value {
    closed(
        vec![
            (
                "payload",
                one_of(vec![
                    closed(
                        vec![
                            ("kind", constant("trading_halt")),
                            ("providerStatus", bounded_text(512)),
                            ("transition", enumeration(&["halted", "resumed"])),
                            ("reason", bounded_text(512)),
                        ],
                        &["kind", "providerStatus", "transition", "reason"],
                    ),
                    closed(
                        vec![
                            ("kind", constant("instrument")),
                            ("providerStatus", bounded_text(512)),
                            (
                                "tradingStatus",
                                enumeration(&["active", "halted", "inactive", "delisted"]),
                            ),
                        ],
                        &["kind", "providerStatus", "tradingStatus"],
                    ),
                ]),
            ),
            ("evidence", display_market_observation_evidence()),
        ],
        &["payload", "evidence"],
    )
}

fn kraken_selected_source_freshness() -> Value {
    closed(
        vec![
            ("ageNanos", unsigned_integer_text()),
            ("state", enumeration(&["uninitialized", "fresh", "stale"])),
            ("lastMarketAt", nullable(canonical_market_timestamp())),
            ("sourceTimestamp", canonical_market_timestamp()),
            ("effectiveAt", canonical_market_timestamp()),
            ("receivedAt", canonical_market_timestamp()),
            ("availableAt", canonical_market_timestamp()),
            ("ingestedAt", canonical_market_timestamp()),
            ("sourceValidUntil", null()),
            ("freshAtSelection", boolean()),
            ("selectedAt", canonical_market_timestamp()),
        ],
        &[
            "ageNanos",
            "state",
            "lastMarketAt",
            "sourceTimestamp",
            "effectiveAt",
            "receivedAt",
            "availableAt",
            "ingestedAt",
            "sourceValidUntil",
            "freshAtSelection",
            "selectedAt",
        ],
    )
}

fn kraken_selected_source_integrity() -> Value {
    closed(
        vec![
            ("state", market_integrity()),
            ("assessedAt", canonical_market_timestamp()),
            ("connectionGeneration", positive_integer_text()),
            (
                "phase",
                enumeration(&["awaiting_snapshot", "healthy", "quarantined"]),
            ),
            ("generationCurrent", constant_bool(true)),
            ("snapshotInitialized", boolean()),
            ("lastSequence", nullable(unsigned_integer_text())),
            ("runtimeEvidence", kraken_market_projection_evidence()),
        ],
        &[
            "state",
            "assessedAt",
            "connectionGeneration",
            "phase",
            "generationCurrent",
            "snapshotInitialized",
            "lastSequence",
            "runtimeEvidence",
        ],
    )
}

fn market_runtime_evidence() -> Value {
    closed(
        vec![
            ("sessionId", bounded_text(512)),
            ("assessmentId", bounded_text(512)),
            ("bindingDigest", investment_analysis_sha256()),
            ("connection", market_connection_liveness()),
            ("transportFreshness", market_transport_freshness()),
            ("marketFreshness", market_observation_freshness()),
            ("sourceFreshness", market_source_timestamp_freshness()),
            (
                "streamIntegrity",
                enumeration(&[
                    "initializing",
                    "synchronizing",
                    "validating",
                    "healthy",
                    "stale",
                    "gap_detected",
                    "checksum_failed",
                    "divergent",
                    "quarantined",
                ]),
            ),
            ("captureIntegrity", market_capture_integrity()),
            ("coverageStatus", market_coverage_status()),
            ("healthObservedAt", canonical_market_timestamp()),
            ("qualificationEvaluatedAt", canonical_market_timestamp()),
            ("qualificationValidUntil", canonical_market_timestamp()),
        ],
        &[
            "sessionId",
            "assessmentId",
            "bindingDigest",
            "connection",
            "transportFreshness",
            "marketFreshness",
            "sourceFreshness",
            "streamIntegrity",
            "captureIntegrity",
            "coverageStatus",
            "healthObservedAt",
            "qualificationEvaluatedAt",
            "qualificationValidUntil",
        ],
    )
}

fn market_connection_liveness() -> Value {
    one_of(vec![
        constant("connecting"),
        externally_tagged_timestamp("live", "last_activity_at"),
        externally_tagged_timestamp("stale", "last_activity_at"),
        externally_tagged_timestamp("disconnected", "disconnected_at"),
    ])
}

fn market_transport_freshness() -> Value {
    one_of(vec![
        constant("uninitialized"),
        externally_tagged_timestamp("fresh", "last_transport_at"),
        externally_tagged_timestamp("stale", "last_transport_at"),
    ])
}

fn market_observation_freshness() -> Value {
    one_of(vec![
        constant("uninitialized"),
        externally_tagged_timestamp("fresh", "last_market_at"),
        externally_tagged_timestamp("stale", "last_market_at"),
    ])
}

fn market_source_timestamp_freshness() -> Value {
    one_of(vec![
        constant("uninitialized"),
        externally_tagged_timestamp("fresh", "last_source_at"),
        externally_tagged_timestamp("stale", "last_source_at"),
    ])
}

fn externally_tagged_timestamp(variant: &'static str, field: &'static str) -> Value {
    closed(
        vec![(variant, closed(vec![(field, integer_text())], &[field]))],
        &[variant],
    )
}

fn market_source_alternative() -> Value {
    closed(
        vec![
            ("surfaceId", bounded_text(512)),
            ("providerId", bounded_text(512)),
            ("sourceId", bounded_text(128)),
            ("venueId", nullable(bounded_text(64))),
            ("providerProduct", bounded_text(512)),
            ("providerChannel", bounded_text(512)),
            ("timing", market_timing()),
            ("depth", nullable(market_depth())),
            ("quality", market_quality()),
            ("coverage", market_coverage()),
            ("freshnessAgeNanos", unsigned_integer_text()),
            (
                "downgradeDimensions",
                bounded_array(market_downgrade_dimension(), 5),
            ),
        ],
        &[
            "surfaceId",
            "providerId",
            "sourceId",
            "venueId",
            "providerProduct",
            "providerChannel",
            "timing",
            "depth",
            "quality",
            "coverage",
            "freshnessAgeNanos",
            "downgradeDimensions",
        ],
    )
}

fn market_source_metadata_evidence() -> Value {
    closed(
        vec![
            ("schemaVersion", constant_unsigned(1)),
            ("sourceId", bounded_text(128)),
            ("providerId", bounded_text(512)),
            ("sourceClass", constant("exchange")),
            ("metadataRevision", bounded_text(512)),
            ("metadataPayloadDigest", evidence_digest()),
            ("metadataPayloadLocator", nullable(market_payload_locator())),
            ("qualityCeiling", constant("direct_unverified")),
            ("coverage", kraken_source_metadata_coverage()),
        ],
        &[
            "schemaVersion",
            "sourceId",
            "providerId",
            "sourceClass",
            "metadataRevision",
            "metadataPayloadDigest",
            "metadataPayloadLocator",
            "qualityCeiling",
            "coverage",
        ],
    )
}

fn kraken_source_metadata_coverage() -> Value {
    closed(
        vec![
            ("payloadDigest", evidence_digest()),
            ("payloadLocator", nullable(market_payload_locator())),
            ("effectiveFrom", canonical_market_timestamp()),
            ("effectiveUntil", nullable(canonical_market_timestamp())),
            ("assetClasses", fixed_array(constant("crypto"), 1)),
            (
                "topology",
                closed(
                    vec![
                        ("kind", constant("single_venue")),
                        ("venues", fixed_array(bounded_text(64), 1)),
                    ],
                    &["kind", "venues"],
                ),
            ),
            (
                "instruments",
                closed(
                    vec![
                        ("kind", constant("enumerated")),
                        ("instruments", bounded_nonempty_array(uuid(), 4_096)),
                    ],
                    &["kind", "instruments"],
                ),
            ),
            ("live", kraken_live_coverage()),
            (
                "delay",
                closed(vec![("kind", constant("real_time"))], &["kind"]),
            ),
            ("delivery", constant("direct_venue")),
        ],
        &[
            "payloadDigest",
            "payloadLocator",
            "effectiveFrom",
            "effectiveUntil",
            "assetClasses",
            "topology",
            "instruments",
            "live",
            "delay",
            "delivery",
        ],
    )
}

fn kraken_live_coverage() -> Value {
    closed(
        vec![
            ("provider_product", bounded_text(512)),
            ("provider_channel", bounded_text(512)),
            (
                "rules",
                fixed_array(
                    closed(
                        vec![
                            ("event_class", enumeration(&["book_snapshot", "book_delta"])),
                            ("depth", constant("order_level")),
                            (
                                "snapshot_applicability",
                                closed(vec![("kind", constant("required"))], &["kind"]),
                            ),
                        ],
                        &["event_class", "depth", "snapshot_applicability"],
                    ),
                    2,
                ),
            ),
        ],
        &["provider_product", "provider_channel", "rules"],
    )
}

fn market_asset_class() -> Value {
    enumeration(&[
        "equity",
        "fixed_income",
        "option",
        "future",
        "foreign_exchange",
        "crypto",
        "commodity",
        "fund",
        "index",
        "cash",
    ])
}

fn market_source_health() -> Value {
    enumeration(&["healthy", "degraded", "unavailable", "quarantined"])
}

fn market_depth_label() -> Value {
    enumeration(&[
        "Best quote",
        "Price-level book",
        "Order-level book",
        "Benchmark",
        "No market book",
    ])
}

fn market_coverage_status() -> Value {
    enumeration(&["sufficient", "insufficient", "unknown"])
}

fn market_capture_integrity() -> Value {
    enumeration(&["disabled", "healthy", "incomplete"])
}

fn live_event_class() -> Value {
    enumeration(&[
        "trade",
        "quote",
        "book_snapshot",
        "book_delta",
        "auction",
        "trading_halt",
        "instrument_status",
        "corporate_action",
    ])
}

fn market_coverage_delay() -> Value {
    one_of(vec![
        closed(vec![("kind", constant("real_time"))], &["kind"]),
        closed(
            vec![
                ("kind", constant("delayed")),
                ("value", positive_integer_text()),
            ],
            &["kind", "value"],
        ),
    ])
}

fn order_level_quarantine_reason() -> Value {
    enumeration(&[
        "route_mismatch",
        "sequence",
        "checksum",
        "snapshot",
        "mutation",
        "book",
        "resource",
    ])
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
            (
                "quarantineReason",
                nullable(order_level_quarantine_reason()),
            ),
            ("quality", market_quality()),
            (
                "freshness",
                enumeration(&["uninitialized", "fresh", "stale"]),
            ),
            ("lastMarketAt", nullable(canonical_market_timestamp())),
            ("availableAt", canonical_market_timestamp()),
            ("usableForSelection", boolean()),
            ("totalOrderCount", bounded_unsigned(2_000_000)),
            ("returnedOrderCount", bounded_unsigned(64)),
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

fn unified_market_observation() -> Value {
    closed(
        vec![
            ("availability", constant("unavailable")),
            (
                "reason",
                enumeration(&["no_eligible_source", "durable_pit_evidence_not_established"]),
            ),
        ],
        &["availability", "reason"],
    )
}

fn market_selection_receipt() -> Value {
    closed(
        vec![
            ("policyRevision", bounded_unsigned_range(1, 4_294_967_295)),
            ("policyCandidateLimit", bounded_unsigned_range(1, 4_096)),
            ("policyDigest", sha256_evidence_digest()),
            ("selectionDigest", sha256_evidence_digest()),
            (
                "definitionRevisionDigest",
                nullable(nonzero_sha256_evidence_digest()),
            ),
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
            "definitionRevisionDigest",
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
                ("maximumAgeNanos", unsigned_integer_text()),
                ("selectedAgeNanos", unsigned_integer_text()),
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

fn nonzero_sha256_evidence_digest() -> Value {
    closed(
        vec![
            ("algorithm", constant("sha256")),
            (
                "bytes",
                json!({
                    "type": "string",
                    "pattern": "^[0-9a-f]{64}$",
                    "not": {
                        "type": "string",
                        "const": "0000000000000000000000000000000000000000000000000000000000000000"
                    }
                }),
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

fn macro_context() -> Value {
    closed(
        vec![
            (
                "availability",
                enumeration(&["available", "partial", "unavailable"]),
            ),
            ("selection", macro_context_selection()),
            ("confidence", macro_context_confidence()),
            ("coverage", macro_context_coverage()),
            ("observations", fixed_array(macro_context_observation(), 12)),
        ],
        &[
            "availability",
            "selection",
            "confidence",
            "coverage",
            "observations",
        ],
    )
}

fn macro_context_selection() -> Value {
    closed(
        vec![
            ("knowledgeCutoff", canonical_market_timestamp()),
            ("effectiveDateCutoff", exact_calendar_date()),
            ("evaluatedAt", canonical_market_timestamp()),
            ("complete", boolean()),
        ],
        &[
            "knowledgeCutoff",
            "effectiveDateCutoff",
            "evaluatedAt",
            "complete",
        ],
    )
}

fn macro_context_confidence() -> Value {
    closed(
        vec![
            (
                "level",
                enumeration(&["moderate", "limited", "unavailable"]),
            ),
            ("summary", bounded_text(512)),
        ],
        &["level", "summary"],
    )
}

fn macro_context_coverage() -> Value {
    closed(
        vec![
            ("requested", constant_unsigned(12)),
            ("observed", bounded_unsigned(12)),
            ("missing", bounded_unsigned(12)),
            ("unavailable", bounded_unsigned(12)),
        ],
        &["requested", "observed", "missing", "unavailable"],
    )
}

fn macro_context_observation() -> Value {
    let indicators = [
        ("us-government-yield-1m", "1-month government bond yield"),
        ("us-government-yield-3m", "3-month government bond yield"),
        ("us-government-yield-6m", "6-month government bond yield"),
        ("us-government-yield-1y", "1-year government bond yield"),
        ("us-government-yield-2y", "2-year government bond yield"),
        ("us-government-yield-3y", "3-year government bond yield"),
        ("us-government-yield-5y", "5-year government bond yield"),
        ("us-government-yield-7y", "7-year government bond yield"),
        ("us-government-yield-10y", "10-year government bond yield"),
        ("us-government-yield-20y", "20-year government bond yield"),
        ("us-government-yield-30y", "30-year government bond yield"),
    ];
    let mut schemas = indicators
        .into_iter()
        .map(|(indicator_id, label)| {
            macro_context_indicator(
                indicator_id,
                label,
                "interest_rates",
                "business_daily",
                "not_applicable",
                "percent_per_year",
                "Percent per year",
            )
        })
        .collect::<Vec<_>>();
    schemas.push(macro_context_indicator(
        "us-unemployment-rate",
        "U.S. unemployment rate",
        "labor_market",
        "monthly",
        "seasonally_adjusted",
        "percent_of_labor_force",
        "Percent of labor force",
    ));
    one_of(schemas)
}

fn macro_context_indicator(
    indicator_id: &'static str,
    label: &'static str,
    category: &'static str,
    frequency: &'static str,
    seasonal_adjustment: &'static str,
    unit_code: &'static str,
    unit_label: &'static str,
) -> Value {
    closed(
        vec![
            ("indicatorId", constant(indicator_id)),
            ("label", constant(label)),
            ("category", constant(category)),
            ("frequency", constant(frequency)),
            ("seasonalAdjustment", constant(seasonal_adjustment)),
            (
                "unit",
                closed(
                    vec![
                        ("code", constant(unit_code)),
                        ("label", constant(unit_label)),
                        ("symbol", constant("%")),
                    ],
                    &["code", "label", "symbol"],
                ),
            ),
            ("effectiveDate", nullable(exact_calendar_date())),
            ("recorded", macro_context_recorded()),
            ("availableAt", nullable(canonical_market_timestamp())),
            (
                "revision",
                nullable(bounded_unsigned_range(1, u64::from(u32::MAX))),
            ),
            ("supersededAfter", nullable(exact_calendar_date())),
            ("value", macro_context_value()),
            (
                "availability",
                enumeration(&["available", "missing", "unavailable"]),
            ),
            ("confidence", macro_context_confidence()),
        ],
        &[
            "indicatorId",
            "label",
            "category",
            "frequency",
            "seasonalAdjustment",
            "unit",
            "effectiveDate",
            "recorded",
            "availableAt",
            "revision",
            "supersededAfter",
            "value",
            "availability",
            "confidence",
        ],
    )
}

fn macro_context_recorded() -> Value {
    one_of(vec![
        closed(
            vec![
                ("state", constant("known")),
                ("date", exact_calendar_date()),
            ],
            &["state", "date"],
        ),
        closed(vec![("state", constant("not_supplied"))], &["state"]),
    ])
}

fn macro_context_value() -> Value {
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
                ("reason", enumeration(&["not_reported", "unavailable"])),
                ("explanation", bounded_text(512)),
            ],
            &["state", "reason", "explanation"],
        ),
    ])
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

fn fred_alfred_latest_known() -> Value {
    let envelope = |variant_fields: Vec<(&str, Value)>, required: &[&str]| {
        let mut fields = vec![
            (
                "schemaIdentity",
                constant("market-squawk-fred-alfred-operation/v1"),
            ),
            ("operation", constant(FRED_ALFRED_READ_OPERATION)),
        ];
        fields.extend(variant_fields);
        closed(fields, required)
    };
    one_of(vec![
        envelope(
            vec![
                ("state", constant("setup_required")),
                ("reason", constant("desired_activation_absent")),
            ],
            &["schemaIdentity", "operation", "state", "reason"],
        ),
        envelope(
            vec![
                ("state", constant("unavailable")),
                ("reason", constant("exact_provider_dataset_absent")),
            ],
            &["schemaIdentity", "operation", "state", "reason"],
        ),
        envelope(
            vec![
                ("state", constant("unavailable")),
                ("reason", constant("exact_manifest_absent")),
                ("binding", fred_alfred_provider_binding()),
            ],
            &["schemaIdentity", "operation", "state", "reason", "binding"],
        ),
        envelope(
            vec![
                ("state", constant("ready")),
                ("binding", fred_alfred_provider_binding()),
                ("generation", fred_alfred_generation()),
            ],
            &[
                "schemaIdentity",
                "operation",
                "state",
                "binding",
                "generation",
            ],
        ),
        envelope(
            vec![
                ("state", constant("ready")),
                ("generation", fred_alfred_generation()),
                ("result", fred_alfred_point_in_time_read()),
            ],
            &[
                "schemaIdentity",
                "operation",
                "state",
                "generation",
                "result",
            ],
        ),
    ])
}

fn treasury_latest_known(operation: &str) -> Value {
    let base = |mut fields: Vec<(&str, Value)>, required: &[&str]| {
        let mut common = vec![
            (
                "schemaIdentity",
                constant("market-squawk-treasury-latest-known-operation/v1"),
            ),
            ("operation", constant(operation)),
        ];
        common.append(&mut fields);
        closed(common, required)
    };
    one_of(vec![
        base(
            vec![
                ("state", constant("setup_required")),
                ("reason", constant("desired_activation_absent")),
                ("generations", null()),
            ],
            &[
                "schemaIdentity",
                "operation",
                "state",
                "reason",
                "generations",
            ],
        ),
        base(
            vec![
                ("state", constant("unavailable")),
                ("reason", constant("exact_manifest_absent")),
                ("generations", null()),
            ],
            &[
                "schemaIdentity",
                "operation",
                "state",
                "reason",
                "generations",
            ],
        ),
        base(
            vec![
                ("state", constant("ready")),
                ("reason", constant("manifest_bound")),
                (
                    "generations",
                    bounded_nonempty_array(fred_alfred_generation(), 32),
                ),
            ],
            &[
                "schemaIdentity",
                "operation",
                "state",
                "reason",
                "generations",
            ],
        ),
        base(
            vec![
                ("state", constant("ready")),
                ("result", treasury_latest_known_result()),
            ],
            &["schemaIdentity", "operation", "state", "result"],
        ),
    ])
}

fn treasury_latest_known_result() -> Value {
    closed(
        vec![
            ("generation", fred_alfred_generation()),
            ("selectionDigest", lowercase_sha256()),
            ("observations", bounded_nonempty_array(record(), 32)),
        ],
        &["generation", "selectionDigest", "observations"],
    )
}

fn fred_alfred_provider_binding() -> Value {
    closed(
        vec![
            ("surfaceId", constant(FRED_ALFRED_API_SURFACE_ID)),
            ("providerDatasetId", bounded_text(512)),
            ("analyticalDatasetId", analytical_dataset_identifier()),
        ],
        &["surfaceId", "providerDatasetId", "analyticalDatasetId"],
    )
}

fn fred_alfred_generation() -> Value {
    closed(
        vec![
            ("manifestVersion", positive_integer_text()),
            ("schema", fred_alfred_schema_identity()),
            ("contentHash", nonzero_lowercase_sha256()),
        ],
        &["manifestVersion", "schema", "contentHash"],
    )
}

fn fred_alfred_schema_identity() -> Value {
    closed(
        vec![
            ("name", bounded_text(256)),
            ("version", bounded_unsigned_range(1, u64::from(u16::MAX))),
            ("fingerprint", nonzero_lowercase_sha256()),
        ],
        &["name", "version", "fingerprint"],
    )
}

fn fred_alfred_point_in_time_read() -> Value {
    closed(
        vec![
            (
                "schemaIdentity",
                constant("market-squawk-fred-alfred-point-in-time/v1"),
            ),
            ("binding", fred_alfred_read_binding()),
            ("selection", fred_alfred_read_selection()),
            ("observation", fred_alfred_read_observation()),
        ],
        &["schemaIdentity", "binding", "selection", "observation"],
    )
}

fn fred_alfred_read_binding() -> Value {
    closed(
        vec![
            ("provider", fred_alfred_read_provider()),
            ("manifest", fred_alfred_read_manifest()),
            ("objectGraphDigest", nonzero_lowercase_sha256()),
            ("queryIdentity", nonzero_lowercase_sha256()),
            ("resultDigest", nonzero_lowercase_sha256()),
        ],
        &[
            "provider",
            "manifest",
            "objectGraphDigest",
            "queryIdentity",
            "resultDigest",
        ],
    )
}

fn fred_alfred_read_provider() -> Value {
    closed(
        vec![
            ("surfaceId", constant(FRED_ALFRED_API_SURFACE_ID)),
            ("sourceId", constant("fred-fred-alfred.api-v1-v2")),
            ("providerDatasetId", bounded_text(512)),
            ("analyticalDatasetId", analytical_dataset_identifier()),
            ("seriesId", bounded_text(512)),
        ],
        &[
            "surfaceId",
            "sourceId",
            "providerDatasetId",
            "analyticalDatasetId",
            "seriesId",
        ],
    )
}

fn fred_alfred_read_manifest() -> Value {
    closed(
        vec![
            ("datasetId", analytical_dataset_identifier()),
            ("manifestVersion", positive_integer_text()),
            ("schema", fred_alfred_schema_identity()),
            ("contentHash", nonzero_lowercase_sha256()),
        ],
        &["datasetId", "manifestVersion", "schema", "contentHash"],
    )
}

fn fred_alfred_read_selection() -> Value {
    closed(
        vec![
            ("policy", constant("latest_known_by_series_as_of_cutoff_v1")),
            ("knowledgeCutoff", canonical_market_timestamp()),
            ("effectiveDateCutoff", exact_calendar_date()),
            ("evaluatedAt", canonical_market_timestamp()),
            ("selectionDigest", nonzero_lowercase_sha256()),
            ("complete", constant_bool(true)),
        ],
        &[
            "policy",
            "knowledgeCutoff",
            "effectiveDateCutoff",
            "evaluatedAt",
            "selectionDigest",
            "complete",
        ],
    )
}

fn fred_alfred_read_observation() -> Value {
    closed(
        vec![
            ("seriesId", bounded_text(512)),
            ("unitId", bounded_text(512)),
            ("effectiveDate", exact_calendar_date()),
            ("publishedVintage", exact_calendar_date()),
            ("supersededAfter", nullable(exact_calendar_date())),
            ("availableAt", canonical_market_timestamp()),
            ("receivedAt", canonical_market_timestamp()),
            ("ingestedAt", canonical_market_timestamp()),
            ("revision", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("value", fred_alfred_read_value()),
            ("sourceIdentifier", bounded_text(512)),
            ("rawPageDigest", nonzero_lowercase_sha256()),
            ("quality", constant("official_delayed")),
        ],
        &[
            "seriesId",
            "unitId",
            "effectiveDate",
            "publishedVintage",
            "supersededAfter",
            "availableAt",
            "receivedAt",
            "ingestedAt",
            "revision",
            "value",
            "sourceIdentifier",
            "rawPageDigest",
            "quality",
        ],
    )
}

fn fred_alfred_read_value() -> Value {
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
                ("marker", bounded_text(512)),
                ("reason", nullable(bounded_text(512))),
            ],
            &["state", "marker", "reason"],
        ),
    ])
}

fn exact_calendar_date() -> Value {
    json!({
        "type": "string",
        "format": "date",
        "minLength": 10,
        "maxLength": 10,
        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    })
}

fn nonzero_lowercase_sha256() -> Value {
    json!({
        "type": "string",
        "minLength": 64,
        "maxLength": 64,
        "pattern": "^[0-9a-f]{64}$",
        "not": {
            "type": "string",
            "const": "0000000000000000000000000000000000000000000000000000000000000000",
        },
    })
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
    json!({"type": "string", "pattern": "^(?:0|[1-9][0-9]*)$"})
}

fn integer_text() -> Value {
    json!({"type": "string", "pattern": "^(?:0|-?[1-9][0-9]*)$"})
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
        "marketObservation" => unified_market_observation(),
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
    one_of(vec![
        generation_variant("ingest", false),
        generation_variant("compaction", false),
        generation_variant("derived", true),
    ])
}

fn generation_variant(kind: &'static str, phase_one: bool) -> Value {
    let mut fields = vec![
        ("manifest", manifest()),
        ("sourceId", text()),
        ("generationKind", constant(kind)),
        ("buildSpecDigest", if phase_one { sha256() } else { null() }),
        ("parents", array(record())),
        ("rowCount", unsigned()),
        ("totalBytes", unsigned()),
        ("lineageDigest", text()),
        ("objectCount", unsigned()),
    ];
    let mut required = vec![
        "manifest",
        "sourceId",
        "generationKind",
        "buildSpecDigest",
        "parents",
        "rowCount",
        "totalBytes",
        "lineageDigest",
        "objectCount",
    ];
    if phase_one {
        fields.extend([
            ("publicationStage", constant("phase_one_derived_generation")),
            ("phaseOneDescriptorSha256", nullable(lowercase_sha256())),
            (
                "productAdmission",
                constant("not_established_on_this_surface"),
            ),
        ]);
        required.extend([
            "publicationStage",
            "phaseOneDescriptorSha256",
            "productAdmission",
        ]);
    }
    closed(fields, &required)
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

fn source_runtime_status() -> Value {
    one_of(vec![
        closed(vec![("state", constant("not_active"))], &["state"]),
        closed(
            vec![
                ("state", constant("active_group")),
                ("runtimeGenerationSha256", investment_analysis_sha256()),
                ("qualifiedRuntimeRecordCount", constant_unsigned(0)),
            ],
            &[
                "state",
                "runtimeGenerationSha256",
                "qualifiedRuntimeRecordCount",
            ],
        ),
        closed(
            vec![
                ("state", constant("active")),
                ("sourceId", bounded_text(128)),
                ("venueId", bounded_text(64)),
                ("instrumentId", uuid()),
                ("providerProduct", bounded_text(512)),
                ("providerChannel", bounded_text(512)),
                ("connectionGeneration", positive_integer_text()),
                ("sessionId", bounded_text(512)),
                ("healthEpoch", positive_integer_text()),
                ("stateRevision", positive_integer_text()),
                ("assessmentId", bounded_text(512)),
                ("bindingDigest", investment_analysis_sha256()),
                ("connection", market_connection_liveness()),
                (
                    "integrity",
                    enumeration(&[
                        "initializing",
                        "synchronizing",
                        "validating",
                        "healthy",
                        "stale",
                        "gap_detected",
                        "checksum_failed",
                        "divergent",
                        "quarantined",
                    ]),
                ),
                ("quality", market_quality()),
                ("observedAtUnixNanos", integer_text()),
                ("qualificationEvaluatedAtUnixNanos", integer_text()),
                ("qualificationValidUntilUnixNanos", integer_text()),
            ],
            &[
                "state",
                "sourceId",
                "venueId",
                "instrumentId",
                "providerProduct",
                "providerChannel",
                "connectionGeneration",
                "sessionId",
                "healthEpoch",
                "stateRevision",
                "assessmentId",
                "bindingDigest",
                "connection",
                "integrity",
                "quality",
                "observedAtUnixNanos",
                "qualificationEvaluatedAtUnixNanos",
                "qualificationValidUntilUnixNanos",
            ],
        ),
    ])
}

fn source_lifecycle_receipt() -> Value {
    signature(vec![
        ("operationId", text()),
        ("provider", text()),
        ("action", text()),
        ("disposition", text()),
        ("state", text()),
        ("stateRevision", positive_integer_text()),
        ("previousGeneration", nullable(positive_integer_text())),
        ("currentGeneration", nullable(positive_integer_text())),
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
        ("configurationSessionId", nullable(uuid())),
        ("doctor", nullable(source_doctor_evidence())),
        ("startEligibility", source_start_eligibility()),
        ("observedAt", timestamp()),
    ])
}

fn source_lifecycle_status() -> Value {
    closed(
        vec![
            ("provider", text()),
            ("stateRevision", positive_integer_text()),
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
            ("currentGeneration", nullable(positive_integer_text())),
            ("runtimeGenerationSha256", nullable(text())),
            ("publicConfigurationSha256", nullable(text())),
            ("doctor", nullable(source_doctor_evidence())),
            ("startEligibility", source_start_eligibility()),
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
            "doctor",
            "startEligibility",
            "blocker",
            "observedAt",
        ],
    )
}

fn source_start_eligibility() -> Value {
    enumeration(&[
        "eligible",
        "already_active",
        "doctor_required",
        "doctor_expired",
        "credential_stale",
        "reconciliation_required",
        "provider_unavailable",
        "not_applicable",
    ])
}

fn source_doctor_evidence() -> Value {
    closed(
        vec![
            (
                "schema",
                constant("market-squawk.alpaca-paper-iex-doctor/v1"),
            ),
            ("receiptSha256", sha256()),
            ("surfaceId", constant("alpaca.basic-market-data")),
            ("onboardingSessionId", uuid()),
            ("credentialGeneration", positive_integer_text()),
            ("realm", constant("paper")),
            ("marketDataPrincipalSha256", sha256()),
            (
                "principalSemantics",
                constant("non_trading_market_data_credential_principal_not_brokerage_account"),
            ),
            ("capabilityRevision", positive_integer_text()),
            ("capabilitySha256", sha256()),
            ("publicConfigurationSha256", sha256()),
            ("rightsDecisionSha256", sha256()),
            ("ratePolicySha256", sha256()),
            ("doctorRevision", bounded_text(128)),
            ("doctorContractSha256", sha256()),
            ("dataQuality", constant("direct_unverified")),
            ("verifiedAt", timestamp()),
            ("exclusiveExpiresAt", timestamp()),
            ("current", boolean()),
            ("capabilities", source_doctor_capabilities()),
        ],
        &[
            "schema",
            "receiptSha256",
            "surfaceId",
            "onboardingSessionId",
            "credentialGeneration",
            "realm",
            "marketDataPrincipalSha256",
            "principalSemantics",
            "capabilityRevision",
            "capabilitySha256",
            "publicConfigurationSha256",
            "rightsDecisionSha256",
            "ratePolicySha256",
            "doctorRevision",
            "doctorContractSha256",
            "dataQuality",
            "verifiedAt",
            "exclusiveExpiresAt",
            "current",
            "capabilities",
        ],
    )
}

fn source_doctor_capabilities() -> Value {
    closed(
        vec![
            (
                "iexLatestQuote",
                source_doctor_probe(source_doctor_quote_observation()),
            ),
            (
                "iexSnapshotBatch",
                source_doctor_probe(source_doctor_batch_observation()),
            ),
            (
                "iexWebSocket",
                source_doctor_probe(source_doctor_stream_observation()),
            ),
            (
                "iexHistoricalBars",
                source_doctor_probe(source_doctor_history_observation()),
            ),
            (
                "iexUtcCalendar",
                source_doctor_probe(source_doctor_calendar_observation()),
            ),
            (
                "additional",
                fixed_array(source_doctor_additional_capability(), 13),
            ),
        ],
        &[
            "iexLatestQuote",
            "iexSnapshotBatch",
            "iexWebSocket",
            "iexHistoricalBars",
            "iexUtcCalendar",
            "additional",
        ],
    )
}

fn source_doctor_probe(observation: Value) -> Value {
    one_of(vec![
        closed(
            vec![
                ("disposition", constant("available")),
                ("evidenceSha256", sha256()),
                ("observation", observation.clone()),
            ],
            &["disposition", "evidenceSha256", "observation"],
        ),
        closed(
            vec![
                ("disposition", constant("degraded")),
                ("evidenceSha256", sha256()),
                ("observation", observation.clone()),
            ],
            &["disposition", "evidenceSha256", "observation"],
        ),
        closed(
            vec![
                ("disposition", constant("unavailable")),
                ("evidenceSha256", sha256()),
                ("observation", nullable(observation)),
            ],
            &["disposition", "evidenceSha256", "observation"],
        ),
        closed(
            vec![
                ("disposition", constant("not_probed")),
                ("evidenceSha256", sha256()),
                ("observation", null()),
            ],
            &["disposition", "evidenceSha256", "observation"],
        ),
    ])
}

fn source_doctor_quote_observation() -> Value {
    closed(
        vec![
            ("http", source_doctor_http_evidence()),
            ("semanticResultSha256", sha256()),
            ("quoteTimestamp", nullable(timestamp())),
        ],
        &["http", "semanticResultSha256", "quoteTimestamp"],
    )
}

fn source_doctor_batch_observation() -> Value {
    closed(
        vec![
            ("http", source_doctor_http_evidence()),
            ("semanticResultSha256", sha256()),
            ("requested", bounded_unsigned(101)),
            ("returned", bounded_unsigned(101)),
            ("valid", bounded_unsigned(101)),
            ("missing", bounded_unsigned(101)),
            ("unexpected", bounded_unsigned(101)),
            ("duplicate", bounded_unsigned(101)),
            ("invalid", bounded_unsigned(101)),
            ("requestedSetSha256", sha256()),
            ("returnedSetSha256", sha256()),
            ("missingSetSha256", sha256()),
            ("unexpectedSetSha256", sha256()),
        ],
        &[
            "http",
            "semanticResultSha256",
            "requested",
            "returned",
            "valid",
            "missing",
            "unexpected",
            "duplicate",
            "invalid",
            "requestedSetSha256",
            "returnedSetSha256",
            "missingSetSha256",
            "unexpectedSetSha256",
        ],
    )
}

fn source_doctor_stream_observation() -> Value {
    closed(
        vec![
            ("endpointContractSha256", sha256()),
            ("requestSha256", sha256()),
            ("connectedFrameSha256", sha256()),
            ("authenticatedFrameSha256", sha256()),
            ("subscriptionFrameSha256", sha256()),
            ("semanticResultSha256", sha256()),
            ("handshakeStatus", bounded_unsigned_range(100, 599)),
            ("handshakeRate", source_doctor_rate_evidence()),
            ("subscribedTrades", bounded_unsigned(26)),
            ("subscribedQuotes", bounded_unsigned(26)),
            ("framesObserved", bounded_unsigned(26)),
            ("bytesObserved", unsigned_integer_text()),
            ("authenticatedAt", timestamp()),
            ("subscribedAt", timestamp()),
            ("closeSent", boolean()),
            ("cleanCloseObserved", boolean()),
            ("completedAt", timestamp()),
        ],
        &[
            "endpointContractSha256",
            "requestSha256",
            "connectedFrameSha256",
            "authenticatedFrameSha256",
            "subscriptionFrameSha256",
            "semanticResultSha256",
            "handshakeStatus",
            "handshakeRate",
            "subscribedTrades",
            "subscribedQuotes",
            "framesObserved",
            "bytesObserved",
            "authenticatedAt",
            "subscribedAt",
            "closeSent",
            "cleanCloseObserved",
            "completedAt",
        ],
    )
}

fn source_doctor_history_observation() -> Value {
    closed(
        vec![
            ("endpointContractSha256", sha256()),
            ("requestSha256", sha256()),
            ("semanticResultSha256", sha256()),
            ("startDate", calendar_date()),
            ("endDate", calendar_date()),
            ("pages", bounded_unsigned(8)),
            ("bars", unsigned()),
            ("distinctDates", unsigned()),
            ("firstBarTimestamp", nullable(timestamp())),
            ("lastBarTimestamp", nullable(timestamp())),
            ("returnedDatesSha256", sha256()),
            ("paginationGraphSha256", sha256()),
            ("terminalPagination", boolean()),
            (
                "pageEvidence",
                bounded_array(source_doctor_history_page(), 8),
            ),
        ],
        &[
            "endpointContractSha256",
            "requestSha256",
            "semanticResultSha256",
            "startDate",
            "endDate",
            "pages",
            "bars",
            "distinctDates",
            "firstBarTimestamp",
            "lastBarTimestamp",
            "returnedDatesSha256",
            "paginationGraphSha256",
            "terminalPagination",
            "pageEvidence",
        ],
    )
}

fn source_doctor_history_page() -> Value {
    closed(
        vec![
            ("http", source_doctor_http_evidence()),
            ("requestPageTokenSha256", nullable(sha256())),
            ("responsePageTokenSha256", nullable(sha256())),
        ],
        &["http", "requestPageTokenSha256", "responsePageTokenSha256"],
    )
}

fn source_doctor_calendar_observation() -> Value {
    closed(
        vec![
            ("http", source_doctor_http_evidence()),
            ("semanticResultSha256", sha256()),
            ("startDate", calendar_date()),
            ("endDate", calendar_date()),
            ("sessions", unsigned()),
            ("historyDates", unsigned()),
            ("matchedDates", unsigned()),
            ("missingHistoryDates", unsigned()),
            ("unexpectedHistoryDates", unsigned()),
            ("sessionDatesSha256", sha256()),
            ("historyDatesSha256", sha256()),
            ("exactDateReconciliation", boolean()),
        ],
        &[
            "http",
            "semanticResultSha256",
            "startDate",
            "endDate",
            "sessions",
            "historyDates",
            "matchedDates",
            "missingHistoryDates",
            "unexpectedHistoryDates",
            "sessionDatesSha256",
            "historyDatesSha256",
            "exactDateReconciliation",
        ],
    )
}

fn source_doctor_http_evidence() -> Value {
    closed(
        vec![
            ("endpointContractSha256", sha256()),
            ("requestSha256", sha256()),
            ("status", bounded_unsigned_range(100, 599)),
            ("bodySha256", sha256()),
            ("bytes", unsigned_integer_text()),
            ("receivedAt", timestamp()),
            ("latencyNanos", unsigned_integer_text()),
            ("rate", source_doctor_rate_evidence()),
        ],
        &[
            "endpointContractSha256",
            "requestSha256",
            "status",
            "bodySha256",
            "bytes",
            "receivedAt",
            "latencyNanos",
            "rate",
        ],
    )
}

fn source_doctor_rate_evidence() -> Value {
    closed(
        vec![
            ("limit", source_doctor_observed_unsigned()),
            ("remaining", source_doctor_observed_unsigned()),
            ("reset_unix_seconds", source_doctor_observed_integer()),
            ("retry_after", source_doctor_observed_retry_after()),
        ],
        &["limit", "remaining", "reset_unix_seconds", "retry_after"],
    )
}

fn source_doctor_observed_unsigned() -> Value {
    one_of(vec![
        closed(vec![("state", constant("missing"))], &["state"]),
        closed(
            vec![("state", constant("observed")), ("value", unsigned())],
            &["state", "value"],
        ),
    ])
}

fn source_doctor_observed_integer() -> Value {
    one_of(vec![
        closed(vec![("state", constant("missing"))], &["state"]),
        closed(
            vec![("state", constant("observed")), ("value", integer_text())],
            &["state", "value"],
        ),
    ])
}

fn source_doctor_observed_retry_after() -> Value {
    one_of(vec![
        closed(vec![("state", constant("missing"))], &["state"]),
        closed(
            vec![
                ("state", constant("observed")),
                (
                    "value",
                    one_of(vec![
                        closed(
                            vec![
                                ("kind", constant("delay_seconds")),
                                ("value", unsigned_integer_text()),
                            ],
                            &["kind", "value"],
                        ),
                        closed(
                            vec![
                                ("kind", constant("at_unix_seconds")),
                                ("value", integer_text()),
                            ],
                            &["kind", "value"],
                        ),
                    ]),
                ),
            ],
            &["state", "value"],
        ),
    ])
}

fn source_doctor_additional_capability() -> Value {
    closed(
        vec![
            (
                "capability",
                enumeration(&[
                    "options_rest",
                    "options_stream",
                    "fixed_income",
                    "corporate_actions",
                    "sip",
                    "nbbo",
                    "opra",
                    "price_level_depth",
                    "order_level_depth",
                    "brokerage_account",
                    "positions",
                    "orders",
                    "trading",
                ]),
            ),
            ("disposition", enumeration(&["not_probed", "unavailable"])),
            ("evidenceSha256", sha256()),
        ],
        &["capability", "disposition", "evidenceSha256"],
    )
}

fn calendar_date() -> Value {
    closed(
        vec![
            ("year", bounded_unsigned_range(1, u16::MAX as u64)),
            ("month", bounded_unsigned_range(1, 12)),
            ("day", bounded_unsigned_range(1, 31)),
        ],
        &["year", "month", "day"],
    )
}

fn portfolio_import_transaction() -> Value {
    closed_complete(vec![
        ("recordToken", uuid()),
        (
            "category",
            enumeration(&[
                "trade",
                "cash_transfer",
                "income",
                "fee",
                "corporate_action",
            ]),
        ),
        ("amount", money()),
        ("quantity", nullable(text())),
        ("occurredAtUnixNanos", text()),
        (
            "interpretationOptions",
            array(portfolio_interpretation_option()),
        ),
        ("eligibleLotCount", unsigned()),
    ])
}

fn portfolio_interpretation_option() -> Value {
    closed_complete(vec![
        ("value", text()),
        ("label", text()),
        ("requiresLotSelection", boolean()),
    ])
}

fn portfolio_snapshot() -> Value {
    closed_complete(vec![
        ("snapshotToken", uuid()),
        ("effectiveAtUnixNanos", text()),
        ("availableAtUnixNanos", nullable(text())),
        ("holdingCount", unsigned()),
        ("transactionCount", unsigned()),
        ("dataIssueCount", unsigned()),
        ("dataState", enumeration(&["ready", "needs_review"])),
    ])
}

fn portfolio_account() -> Value {
    closed_complete(vec![
        ("accountToken", opaque_product_token()),
        ("displayName", bounded_text(256)),
        ("currency", text()),
        ("holdings", unsigned()),
        ("dataIssues", unsigned()),
    ])
}

fn portfolio_holding() -> Value {
    closed_complete(vec![
        ("accountId", text()),
        ("snapshotToken", uuid()),
        ("instrumentId", text()),
        ("currency", text()),
        ("quantity", text()),
        ("lotSize", text()),
        ("marketValue", money()),
        ("asOfUnixNanos", text()),
        ("costBasis", portfolio_cost_basis()),
        ("price", portfolio_price_state()),
    ])
}

fn portfolio_cost_basis() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("amount", money()),
            ("method", portfolio_lot_method()),
        ]),
        closed_complete(vec![("state", constant("not_available"))]),
        closed_complete(vec![
            ("state", constant("needs_review")),
            ("choices", array(money())),
            ("method", portfolio_lot_method()),
        ]),
    ])
}

fn portfolio_lot_method() -> Value {
    enumeration(&[
        "First in, first out",
        "Last in, first out",
        "Average cost",
        "Specific lots",
    ])
}

fn portfolio_price_state() -> Value {
    closed_complete(vec![
        ("asOfUnixNanos", text()),
        (
            "state",
            enumeration(&["reported", "current", "stale", "not_available"]),
        ),
        ("confidence", portfolio_confidence()),
        ("explanation", text()),
    ])
}

fn portfolio_transaction() -> Value {
    closed_complete(vec![
        ("transactionToken", uuid()),
        ("accountId", text()),
        ("snapshotToken", uuid()),
        ("instrumentId", nullable(text())),
        (
            "category",
            enumeration(&[
                "trade",
                "cash_transfer",
                "income",
                "fee",
                "corporate_action",
            ]),
        ),
        ("amount", money()),
        ("quantity", nullable(text())),
        ("occurredAtUnixNanos", text()),
        ("lotMethod", nullable(portfolio_lot_method())),
    ])
}

fn portfolio_confidence() -> Value {
    enumeration(&["limited", "moderate", "strong"])
}

fn portfolio_report_fields() -> Vec<(&'static str, Value)> {
    vec![
        ("accountId", text()),
        ("snapshotToken", uuid()),
        ("effectiveAtUnixNanos", text()),
        ("availableAtUnixNanos", nullable(text())),
        ("dataConfidence", portfolio_confidence()),
    ]
}

fn portfolio_performance() -> Value {
    let mut fields = portfolio_report_fields();
    fields.extend([
        ("currentValue", money()),
        ("historyStatus", text()),
        ("timeWeightedReturn", text()),
        ("moneyWeightedReturn", text()),
        ("periods", unsigned()),
        ("accountingEvidence", portfolio_accounting_evidence()),
    ]);
    closed(
        fields,
        &[
            "accountId",
            "snapshotToken",
            "effectiveAtUnixNanos",
            "availableAtUnixNanos",
            "dataConfidence",
            "currentValue",
            "accountingEvidence",
        ],
    )
}

fn portfolio_accounting_evidence() -> Value {
    closed_complete(vec![
        (
            "cash",
            closed_complete(vec![
                ("amount", money()),
                ("observedAtUnixNanos", text()),
                ("status", constant("available")),
            ]),
        ),
        ("reportedMarketValue", money()),
        ("unrealizedGain", portfolio_measured_accounting()),
        ("realizedGain", portfolio_measured_accounting()),
        ("income", portfolio_measured_accounting()),
        ("fees", portfolio_measured_accounting()),
        (
            "reconciliation",
            closed_complete(vec![
                ("status", enumeration(&["clear", "needs_review"])),
                ("discrepancies", array(portfolio_reconciliation_detail())),
            ]),
        ),
    ])
}

fn portfolio_measured_accounting() -> Value {
    closed(
        vec![
            (
                "status",
                enumeration(&["available", "partial", "not_available"]),
            ),
            ("amount", money()),
        ],
        &["status"],
    )
}

fn portfolio_reconciliation_detail() -> Value {
    closed_complete(vec![
        (
            "field",
            enumeration(&["cash", "market_value", "cost_basis"]),
        ),
        ("supplied", money()),
        ("calculated", money()),
        ("currency", text()),
        (
            "tolerance",
            closed_complete(vec![("kind", constant("absolute")), ("amount", money())]),
        ),
    ])
}

fn portfolio_exposure() -> Value {
    let mut fields = portfolio_report_fields();
    fields.extend([
        ("instrument", array(portfolio_exposure_instrument())),
        ("currency", array(portfolio_exposure_currency())),
        ("sector", array(portfolio_exposure_classification())),
        ("factor", array(portfolio_exposure_classification())),
        ("net", money()),
        ("gross", money()),
        ("calculationStatus", text()),
        ("classificationStatus", text()),
    ]);
    closed(
        fields,
        &[
            "accountId",
            "snapshotToken",
            "effectiveAtUnixNanos",
            "availableAtUnixNanos",
            "dataConfidence",
            "instrument",
            "currency",
            "sector",
            "factor",
        ],
    )
}

fn portfolio_exposure_instrument() -> Value {
    closed_complete(vec![("instrumentId", text()), ("amount", money())])
}

fn portfolio_exposure_currency() -> Value {
    closed_complete(vec![("currency", text()), ("amount", money())])
}

fn portfolio_exposure_classification() -> Value {
    closed_complete(vec![("classification", text()), ("amount", money())])
}

fn portfolio_risk() -> Value {
    closed_complete(vec![
        ("accountName", bounded_text(256)),
        ("asOf", timestamp()),
        ("availableAt", timestamp()),
        ("horizon", bounded_text(160)),
        ("coverage", portfolio_risk_coverage()),
        ("measures", fixed_array(portfolio_risk_measure(), 3)),
        ("stress", portfolio_risk_stress()),
        ("recommendation", portfolio_risk_recommendation()),
    ])
}

fn portfolio_risk_coverage() -> Value {
    closed_complete(vec![
        (
            "state",
            enumeration(&["complete", "partial", "unavailable"]),
        ),
        ("observations", unsigned()),
        ("period", bounded_text(160)),
        ("explanation", bounded_text(4_096)),
    ])
}

fn portfolio_risk_measure() -> Value {
    closed_complete(vec![
        (
            "label",
            enumeration(&[
                "Value at risk",
                "Expected shortfall",
                "Annualized volatility",
            ]),
        ),
        ("value", nullable(percentage_text())),
        (
            "status",
            enumeration(&["available", "insufficient_history", "unavailable"]),
        ),
        ("explanation", bounded_text(4_096)),
    ])
}

fn portfolio_risk_stress() -> Value {
    closed_complete(vec![
        ("label", bounded_text(160)),
        ("impact", nullable(money())),
        (
            "status",
            enumeration(&["available", "incomplete", "unavailable"]),
        ),
        ("explanation", bounded_text(4_096)),
        (
            "assumptions",
            bounded_nonempty_array(bounded_text(4_096), 12),
        ),
    ])
}

fn portfolio_risk_recommendation() -> Value {
    closed_complete(vec![
        (
            "action",
            enumeration(&["buy", "add", "hold", "trim", "sell", "abstain"]),
        ),
        ("horizon", bounded_text(160)),
        ("summary", bounded_text(4_096)),
        ("ranges", bounded_array(portfolio_risk_range(), 8)),
        ("reasons", bounded_nonempty_array(bounded_text(4_096), 12)),
        ("risks", bounded_nonempty_array(bounded_text(4_096), 12)),
        (
            "assumptions",
            bounded_nonempty_array(bounded_text(4_096), 12),
        ),
        (
            "invalidators",
            bounded_nonempty_array(bounded_text(4_096), 12),
        ),
        ("validity", portfolio_recommendation_validity()),
        ("uncertainty", portfolio_recommendation_uncertainty()),
    ])
}

fn portfolio_risk_range() -> Value {
    closed_complete(vec![
        ("label", bounded_text(160)),
        ("lower", money()),
        ("upper", money()),
    ])
}

fn portfolio_recommendation_validity() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("expiresAt", timestamp()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("explanation", bounded_text(4_096)),
        ]),
    ])
}

fn portfolio_recommendation_uncertainty() -> Value {
    closed_complete(vec![
        (
            "level",
            enumeration(&["low", "moderate", "high", "unavailable"]),
        ),
        ("explanation", bounded_text(4_096)),
        (
            "outOfSampleEvidence",
            enumeration(&["sufficient", "limited", "unavailable"]),
        ),
        (
            "calibration",
            enumeration(&["supported", "limited", "unavailable"]),
        ),
        (
            "tradingCosts",
            enumeration(&["included", "partial", "unavailable"]),
        ),
        (
            "pointInTimeInputs",
            enumeration(&["supported", "partial", "unavailable"]),
        ),
    ])
}

fn portfolio_attribution() -> Value {
    let mut fields = portfolio_report_fields();
    fields.extend([
        ("baselineSnapshotToken", uuid()),
        ("baselineEffectiveAtUnixNanos", text()),
        ("baselineAvailableAtUnixNanos", nullable(text())),
        ("contributions", array(portfolio_contribution())),
        ("total", money()),
        ("explanation", text()),
    ]);
    closed_complete(fields)
}

fn portfolio_contribution() -> Value {
    closed_complete(vec![("instrumentId", text()), ("amount", money())])
}

fn portfolio_evaluated_scenario() -> Value {
    closed_complete(vec![
        ("id", text()),
        ("composition", enumeration(&["additive", "compounded"])),
        ("contributions", array(portfolio_contribution())),
        ("total", money()),
    ])
}

fn portfolio_scenario(batch: bool) -> Value {
    let mut fields = portfolio_report_fields();
    if batch {
        fields.push(("scenarios", array(portfolio_evaluated_scenario())));
    } else {
        fields.push(("scenario", portfolio_evaluated_scenario()));
    }
    closed_complete(fields)
}

fn portfolio_rebalance() -> Value {
    let mut fields = portfolio_report_fields();
    fields.extend([
        (
            "trades",
            array(closed_complete(vec![
                ("instrumentId", text()),
                ("valueChange", money()),
            ])),
        ),
        ("projectedCash", money()),
        ("turnover", text()),
        ("constrained", boolean()),
    ]);
    closed_complete(fields)
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
    closed_complete(vec![
        ("accountId", text()),
        ("instrumentId", text()),
        ("positionState", enumeration(&["new", "existing"])),
        ("currentQuantity", text()),
        ("proposedQuantity", text()),
        ("currentMarketValue", money()),
        ("proposedMarketValue", money()),
        ("capitalChange", money()),
        ("portfolioValue", money()),
        ("instrumentTerms", portfolio_product_instrument_terms()),
        ("costs", portfolio_product_costs()),
        ("concentration", portfolio_candidate_concentration()),
        ("scenario", portfolio_product_candidate_scenario()),
        ("price", portfolio_product_candidate_price()),
        ("missingInformation", array(text())),
        ("riskAssessment", portfolio_product_risk_assessment()),
        ("updatedAtUnixNanos", text()),
        ("analysisOnly", constant_bool(true)),
    ])
}

fn portfolio_product_instrument_terms() -> Value {
    closed_complete(vec![
        ("priceTick", text()),
        ("lotSize", text()),
        ("quoteCurrency", text()),
        ("contractMultiplier", text()),
    ])
}

fn portfolio_product_costs() -> Value {
    closed_complete(vec![
        ("fees", portfolio_product_cost()),
        ("slippage", portfolio_product_cost()),
    ])
}

fn portfolio_product_cost() -> Value {
    one_of(vec![
        closed_complete(vec![("state", constant("available")), ("amount", money())]),
        closed_complete(vec![("state", constant("not_available"))]),
    ])
}

fn portfolio_product_candidate_scenario() -> Value {
    closed_complete(vec![
        ("shock", text()),
        ("currentImpact", money()),
        ("proposedImpact", money()),
        ("marginalImpact", money()),
    ])
}

fn portfolio_product_candidate_price() -> Value {
    closed_complete(vec![
        ("amount", money()),
        ("asOfUnixNanos", text()),
        ("state", constant("current")),
        ("method", enumeration(&["Last trade", "Bid-ask midpoint"])),
        ("confidence", portfolio_confidence()),
    ])
}

fn portfolio_product_risk_assessment() -> Value {
    closed_complete(vec![
        ("state", constant("incomplete")),
        ("evaluatedAtUnixNanos", text()),
        ("checksCompleted", unsigned()),
        ("checksUnavailable", unsigned()),
    ])
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

fn queued_product_start() -> Value {
    closed(vec![("state", constant("queued"))], &["state"])
}

fn model_evidence_page() -> Value {
    closed(
        vec![("models", bounded_array(model_evidence(), 4_096))],
        &["models"],
    )
}

fn model_evidence() -> Value {
    closed(
        vec![
            ("modelToken", uuid()),
            ("label", bounded_text(240)),
            ("objective", enumeration(&["numeric_outcome", "likelihood"])),
            ("intendedUse", bounded_text(4_096)),
            (
                "evidenceState",
                enumeration(&["sufficient", "limited", "unavailable"]),
            ),
            ("training", model_training_evidence()),
            ("validation", bounded_array(model_validation_evidence(), 64)),
            ("coverage", bounded_array(model_coverage_evidence(), 64)),
            ("limitations", bounded_array(bounded_text(4_096), 256)),
            ("unavailableBehavior", constant("no_action")),
            ("analysisOnly", constant_bool(true)),
        ],
        &[
            "modelToken",
            "label",
            "objective",
            "intendedUse",
            "evidenceState",
            "training",
            "validation",
            "coverage",
            "limitations",
            "unavailableBehavior",
            "analysisOnly",
        ],
    )
}

fn model_training_evidence() -> Value {
    closed(
        vec![
            ("observedFromUnixNanos", integer_text()),
            ("observedThroughUnixNanos", integer_text()),
            ("availableAtUnixNanos", integer_text()),
            ("trainingObservations", unsigned()),
            ("validationObservations", unsigned()),
            ("outOfSampleObservations", unsigned()),
            ("rollingOutOfSampleFolds", unsigned()),
            ("evaluatedHorizons", unsigned()),
        ],
        &[
            "observedFromUnixNanos",
            "observedThroughUnixNanos",
            "availableAtUnixNanos",
            "trainingObservations",
            "validationObservations",
            "outOfSampleObservations",
            "rollingOutOfSampleFolds",
            "evaluatedHorizons",
        ],
    )
}

fn model_validation_evidence() -> Value {
    closed(
        vec![
            ("label", bounded_text(200)),
            ("value", canonical_decimal_text()),
            ("interpretation", bounded_text(1_000)),
        ],
        &["label", "value", "interpretation"],
    )
}

fn model_coverage_evidence() -> Value {
    closed(
        vec![
            ("label", bounded_text(200)),
            (
                "state",
                enumeration(&["evaluated", "limited", "unavailable"]),
            ),
            ("interpretation", bounded_text(1_000)),
        ],
        &["label", "state", "interpretation"],
    )
}

fn model_activity_page() -> Value {
    closed(
        vec![("activities", bounded_array(model_activity(), 1_024))],
        &["activities"],
    )
}

fn model_activity() -> Value {
    closed(
        vec![
            ("activityToken", uuid()),
            ("label", bounded_text(240)),
            (
                "state",
                enumeration(&["queued", "running", "completed", "failed"]),
            ),
            ("progressPercent", nullable(canonical_decimal_text())),
            ("updatedAtUnixNanos", integer_text()),
        ],
        &[
            "activityToken",
            "label",
            "state",
            "progressPercent",
            "updatedAtUnixNanos",
        ],
    )
}

fn product_forecast_page() -> Value {
    closed(
        vec![
            (
                "forecasts",
                bounded_array(product_forecast_summary(), 4_096),
            ),
            ("available", unsigned()),
            ("truncated", boolean()),
        ],
        &["forecasts", "available", "truncated"],
    )
}

fn product_forecast_investment() -> Value {
    closed_complete(vec![
        ("name", bounded_text(240)),
        ("symbol", nullable(bounded_text(64))),
        ("description", bounded_text(400)),
    ])
}

fn product_forecast_target() -> Value {
    closed_complete(vec![
        ("label", bounded_text(200)),
        ("meaning", bounded_text(1_000)),
        (
            "valueKind",
            enumeration(&["market_price", "percentage_return", "probability"]),
        ),
        ("unitLabel", bounded_text(80)),
        ("currencyCode", nullable(currency_code())),
    ])
}

fn product_forecast_amount() -> Value {
    closed_complete(vec![
        ("exact", canonical_decimal_text()),
        ("formatted", bounded_text(120)),
    ])
}

fn product_forecast_model_evidence() -> Value {
    closed_complete(vec![
        ("modelToken", uuid()),
        (
            "overall",
            enumeration(&["sufficient", "limited", "unavailable"]),
        ),
        (
            "pitInputs",
            enumeration(&["sufficient", "limited", "unavailable"]),
        ),
        (
            "outOfSample",
            enumeration(&["sufficient", "limited", "unavailable"]),
        ),
        (
            "horizonAlignment",
            enumeration(&["sufficient", "limited", "unavailable"]),
        ),
        (
            "calibration",
            enumeration(&["calibrated", "limited", "unavailable"]),
        ),
        ("interpretation", bounded_text(1_000)),
    ])
}

fn product_forecast_summary() -> Value {
    closed(
        vec![
            ("forecastToken", uuid()),
            ("investment", product_forecast_investment()),
            ("target", product_forecast_target()),
            ("modelEvidence", product_forecast_model_evidence()),
            ("observedThroughUnixNanos", integer_text()),
            ("createdAtUnixNanos", integer_text()),
            ("expiresAtUnixNanos", integer_text()),
            ("horizon", product_forecast_horizon()),
            ("historicalObservationCount", unsigned()),
            ("limitations", bounded_array(bounded_text(4_096), 256)),
        ],
        &[
            "forecastToken",
            "investment",
            "target",
            "modelEvidence",
            "observedThroughUnixNanos",
            "createdAtUnixNanos",
            "expiresAtUnixNanos",
            "horizon",
            "historicalObservationCount",
            "limitations",
        ],
    )
}

fn product_forecast_horizon() -> Value {
    closed_complete(vec![
        ("label", bounded_text(200)),
        ("description", bounded_text(1_000)),
        ("points", bounded_unsigned_range(1, 512)),
    ])
}

fn product_forecast_detail() -> Value {
    closed(
        vec![
            ("forecastToken", uuid()),
            ("investment", product_forecast_investment()),
            ("target", product_forecast_target()),
            ("modelEvidence", product_forecast_model_evidence()),
            ("observedThroughUnixNanos", integer_text()),
            ("availableAtUnixNanos", integer_text()),
            ("createdAtUnixNanos", integer_text()),
            ("expiresAtUnixNanos", integer_text()),
            ("horizon", product_forecast_horizon()),
            (
                "observedHistory",
                bounded_array(product_forecast_observation(), 4_096),
            ),
            (
                "estimates",
                bounded_nonempty_array(product_forecast_estimate(), 512),
            ),
            ("calibration", nullable(product_forecast_calibration())),
            ("limitations", bounded_array(bounded_text(4_096), 256)),
            ("unavailableBehavior", constant("no_action")),
            ("outcomeMonitoring", product_forecast_monitoring()),
            ("analysisOnly", constant_bool(true)),
        ],
        &[
            "forecastToken",
            "investment",
            "target",
            "modelEvidence",
            "observedThroughUnixNanos",
            "availableAtUnixNanos",
            "createdAtUnixNanos",
            "expiresAtUnixNanos",
            "horizon",
            "observedHistory",
            "estimates",
            "calibration",
            "limitations",
            "unavailableBehavior",
            "outcomeMonitoring",
            "analysisOnly",
        ],
    )
}

fn product_forecast_observation() -> Value {
    closed(
        vec![
            ("observedAtUnixNanos", integer_text()),
            ("availableAtUnixNanos", integer_text()),
            ("value", product_forecast_amount()),
        ],
        &["observedAtUnixNanos", "availableAtUnixNanos", "value"],
    )
}

fn product_forecast_estimate() -> Value {
    closed(
        vec![
            ("targetAtUnixNanos", integer_text()),
            ("central", product_forecast_amount()),
            ("ranges", nullable(product_forecast_ranges())),
        ],
        &["targetAtUnixNanos", "central", "ranges"],
    )
}

fn product_forecast_ranges() -> Value {
    let range = || {
        closed(
            vec![
                ("lower", product_forecast_amount()),
                ("upper", product_forecast_amount()),
            ],
            &["lower", "upper"],
        )
    };
    closed(
        vec![("likely", range()), ("wider", range()), ("stress", range())],
        &["likely", "wider", "stress"],
    )
}

fn product_forecast_calibration() -> Value {
    closed(
        vec![
            ("windowStartUnixNanos", integer_text()),
            ("windowEndUnixNanos", integer_text()),
            ("observationCount", positive_integer()),
            (
                "coverage",
                fixed_array(
                    closed(
                        vec![
                            ("targetCoveragePercent", product_forecast_amount()),
                            ("realizedCovered", unsigned()),
                            ("realizedTotal", positive_integer()),
                        ],
                        &["targetCoveragePercent", "realizedCovered", "realizedTotal"],
                    ),
                    3,
                ),
            ),
            ("interpretation", bounded_text(2_000)),
            ("assumptions", bounded_text(2_000)),
        ],
        &[
            "windowStartUnixNanos",
            "windowEndUnixNanos",
            "observationCount",
            "coverage",
            "interpretation",
            "assumptions",
        ],
    )
}

fn product_forecast_monitoring() -> Value {
    closed(
        vec![
            (
                "state",
                enumeration(&["awaiting_outcomes", "outcomes_available"]),
            ),
            ("observedCount", unsigned()),
            ("includedCount", unsigned()),
            ("truncated", boolean()),
            (
                "meanAbsoluteError",
                nullable(product_forecast_rounded_amount()),
            ),
            ("interpretation", bounded_text(2_000)),
        ],
        &[
            "state",
            "observedCount",
            "includedCount",
            "truncated",
            "meanAbsoluteError",
            "interpretation",
        ],
    )
}

fn product_forecast_rounded_amount() -> Value {
    closed_complete(vec![
        ("value", product_forecast_amount()),
        (
            "rounding",
            closed_complete(vec![
                ("state", enumeration(&["exact", "rounded"])),
                ("decimalPlaces", bounded_unsigned_range(0, 18)),
                ("mode", constant("half_even")),
            ]),
        ),
    ])
}

fn product_forecast_outcomes() -> Value {
    closed(
        vec![
            ("forecastToken", uuid()),
            (
                "outcomes",
                bounded_array(
                    closed(
                        vec![
                            ("targetAtUnixNanos", integer_text()),
                            ("observedAtUnixNanos", integer_text()),
                            ("availableAtUnixNanos", integer_text()),
                            ("actual", product_forecast_amount()),
                            ("signedError", product_forecast_amount()),
                            ("absoluteError", product_forecast_amount()),
                        ],
                        &[
                            "targetAtUnixNanos",
                            "observedAtUnixNanos",
                            "availableAtUnixNanos",
                            "actual",
                            "signedError",
                            "absoluteError",
                        ],
                    ),
                    4_096,
                ),
            ),
            ("available", unsigned()),
            ("truncated", boolean()),
        ],
        &["forecastToken", "outcomes", "available", "truncated"],
    )
}

fn backtest_activity_page() -> Value {
    closed(
        vec![("activities", bounded_array(backtest_activity(), 1_000))],
        &["activities"],
    )
}

fn backtest_activity() -> Value {
    closed(
        vec![
            ("backtestToken", uuid()),
            ("label", bounded_text(240)),
            ("startedAt", canonical_market_timestamp()),
            ("updatedAt", canonical_market_timestamp()),
            (
                "state",
                enumeration(&["queued", "running", "completed", "failed"]),
            ),
            ("progressPercent", nullable(canonical_decimal_text())),
        ],
        &[
            "backtestToken",
            "label",
            "startedAt",
            "updatedAt",
            "state",
            "progressPercent",
        ],
    )
}

fn product_backtest_result() -> Value {
    one_of(vec![
        completed_product_backtest(),
        unavailable_product_backtest(),
    ])
}

fn completed_product_backtest() -> Value {
    closed(
        vec![
            ("state", constant("completed")),
            ("backtestToken", uuid()),
            ("label", bounded_text(240)),
            ("completedAt", canonical_market_timestamp()),
            ("expiresAt", nullable(canonical_market_timestamp())),
            ("investmentUniverse", bounded_text(400)),
            ("method", bounded_text(240)),
            (
                "period",
                closed(
                    vec![
                        ("startsAt", canonical_market_timestamp()),
                        ("endsAt", canonical_market_timestamp()),
                    ],
                    &["startsAt", "endsAt"],
                ),
            ),
            ("pointInTimeEvidence", backtest_point_in_time_evidence()),
            ("outOfSampleEvidence", backtest_out_of_sample_evidence()),
            ("performance", backtest_performance()),
            ("costs", backtest_costs()),
            ("execution", backtest_execution()),
            ("comparison", nullable(backtest_comparison())),
            (
                "uncertainty",
                enumeration(&["supported", "limited", "unavailable"]),
            ),
            ("interpretation", bounded_text(4_000)),
            ("limitations", bounded_array(bounded_text(2_000), 64)),
            ("invalidators", bounded_array(bounded_text(2_000), 64)),
            ("analysisOnly", constant_bool(true)),
        ],
        &[
            "state",
            "backtestToken",
            "label",
            "completedAt",
            "expiresAt",
            "investmentUniverse",
            "method",
            "period",
            "pointInTimeEvidence",
            "outOfSampleEvidence",
            "performance",
            "costs",
            "execution",
            "comparison",
            "uncertainty",
            "interpretation",
            "limitations",
            "invalidators",
            "analysisOnly",
        ],
    )
}

fn unavailable_product_backtest() -> Value {
    closed(
        vec![
            ("state", constant("unavailable")),
            ("backtestToken", uuid()),
            ("label", bounded_text(240)),
            ("reason", bounded_text(2_000)),
            ("limitations", bounded_array(bounded_text(2_000), 64)),
            ("unavailableBehavior", constant("no_action")),
        ],
        &[
            "state",
            "backtestToken",
            "label",
            "reason",
            "limitations",
            "unavailableBehavior",
        ],
    )
}

fn backtest_point_in_time_evidence() -> Value {
    closed(
        vec![
            (
                "state",
                enumeration(&["verified", "limited", "unavailable"]),
            ),
            ("informationCutoff", canonical_market_timestamp()),
            ("observedFrom", canonical_market_timestamp()),
            ("observedThrough", canonical_market_timestamp()),
            ("observationCount", unsigned()),
            ("coveragePercent", nullable(canonical_decimal_text())),
            ("interpretation", bounded_text(2_000)),
        ],
        &[
            "state",
            "informationCutoff",
            "observedFrom",
            "observedThrough",
            "observationCount",
            "coveragePercent",
            "interpretation",
        ],
    )
}

fn backtest_out_of_sample_evidence() -> Value {
    closed(
        vec![
            (
                "state",
                enumeration(&["evaluated", "limited", "not_evaluated"]),
            ),
            ("foldCount", unsigned()),
            ("observationCount", unsigned()),
            ("method", bounded_text(500)),
            (
                "probabilityOfOverfittingPercent",
                nullable(canonical_decimal_text()),
            ),
            (
                "deflatedPerformanceProbabilityPercent",
                nullable(canonical_decimal_text()),
            ),
            ("expectedMaximumSharpe", nullable(canonical_decimal_text())),
            ("interpretation", bounded_text(2_000)),
        ],
        &[
            "state",
            "foldCount",
            "observationCount",
            "method",
            "probabilityOfOverfittingPercent",
            "deflatedPerformanceProbabilityPercent",
            "expectedMaximumSharpe",
            "interpretation",
        ],
    )
}

fn backtest_performance() -> Value {
    closed(
        vec![
            ("totalReturnPercent", canonical_decimal_text()),
            (
                "annualizedReturnPercent",
                nullable(canonical_decimal_text()),
            ),
            (
                "annualizedVolatilityPercent",
                nullable(canonical_decimal_text()),
            ),
            ("maximumDrawdownPercent", canonical_decimal_text()),
            ("sharpeRatio", nullable(canonical_decimal_text())),
            ("winRatePercent", nullable(canonical_decimal_text())),
            ("turnoverPercent", nullable(canonical_decimal_text())),
        ],
        &[
            "totalReturnPercent",
            "annualizedReturnPercent",
            "annualizedVolatilityPercent",
            "maximumDrawdownPercent",
            "sharpeRatio",
            "winRatePercent",
            "turnoverPercent",
        ],
    )
}

fn backtest_costs() -> Value {
    closed(
        vec![
            ("fees", bounded_text(200)),
            ("spread", bounded_text(200)),
            ("slippage", bounded_text(200)),
            ("latency", bounded_text(200)),
            ("participationLimit", bounded_text(200)),
            ("partialFills", bounded_text(200)),
            ("totalCostPercent", canonical_decimal_text()),
        ],
        &[
            "fees",
            "spread",
            "slippage",
            "latency",
            "participationLimit",
            "partialFills",
            "totalCostPercent",
        ],
    )
}

fn backtest_execution() -> Value {
    closed(
        vec![
            ("fillCount", unsigned()),
            ("partialFillCount", unsigned()),
            ("noActionCount", unsigned()),
        ],
        &["fillCount", "partialFillCount", "noActionCount"],
    )
}

fn backtest_comparison() -> Value {
    closed(
        vec![
            ("label", bounded_text(240)),
            ("totalReturnPercent", canonical_decimal_text()),
            ("excessReturnPercent", canonical_decimal_text()),
        ],
        &["label", "totalReturnPercent", "excessReturnPercent"],
    )
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
    one_of(vec![
        forecast_selection_receipt_variant(
            closed(vec![("kind", constant("any_valid"))], &["kind"]),
            null(),
        ),
        forecast_selection_receipt_variant(
            closed(
                vec![
                    ("kind", constant("exact_calibrated_conditional_mean_price")),
                    ("horizonNanos", positive_integer_text()),
                ],
                &["kind", "horizonNanos"],
            ),
            integer_text(),
        ),
    ])
}

fn forecast_selection_receipt_variant(
    qualification: Value,
    selected_terminal_target: Value,
) -> Value {
    closed(
        vec![
            (
                "schema",
                constant("market-squawk/forecast-selection-receipt/v2"),
            ),
            ("policyRevision", constant_unsigned(2)),
            (
                "selectionOrder",
                constant("newest_created_at_observed_through_available_at_then_lowest_vintage_id"),
            ),
            ("qualification", qualification),
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
            (
                "selectedTerminalTargetAtUnixNanos",
                selected_terminal_target,
            ),
            ("receiptDigestSha256", lowercase_sha256()),
        ],
        &[
            "schema",
            "policyRevision",
            "selectionOrder",
            "qualification",
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
            "selectedTerminalTargetAtUnixNanos",
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

fn forecast_preparation_options() -> Value {
    closed_complete(vec![(
        "models",
        bounded_array(forecast_preparation_model(true), 4_096),
    )])
}

fn forecast_preparation_model(include_histories: bool) -> Value {
    let mut fields = vec![
        ("modelToken", uuid()),
        ("name", bounded_text(200)),
        ("objective", enumeration(&["numeric_outcome", "likelihood"])),
        ("target", product_forecast_target()),
        ("modelEvidence", product_forecast_model_evidence()),
        ("intendedUse", bounded_text(4_096)),
        ("limitations", bounded_array(bounded_text(4_096), 256)),
        ("unavailableBehavior", constant("no_action")),
    ];
    if include_histories {
        fields.push((
            "histories",
            bounded_nonempty_array(forecast_preparation_history(), 4_096),
        ));
    }
    closed_complete(fields)
}

fn forecast_preparation_history() -> Value {
    closed_complete(vec![
        ("historyToken", uuid()),
        ("label", bounded_text(200)),
        (
            "investments",
            bounded_nonempty_array(forecast_preparation_investment(), 4_096),
        ),
        (
            "horizons",
            bounded_nonempty_array(forecast_preparation_horizon(true), 64),
        ),
    ])
}

fn forecast_preparation_investment() -> Value {
    closed_complete(vec![
        ("investmentToken", uuid()),
        ("label", bounded_text(240)),
        ("observedFromUnixNanos", integer_text()),
        ("observedThroughUnixNanos", integer_text()),
        ("availableAtUnixNanos", integer_text()),
        ("observationCount", bounded_unsigned_range(1, 4_096)),
    ])
}

fn forecast_preparation_horizon(include_token: bool) -> Value {
    let mut fields = vec![
        ("label", bounded_text(200)),
        ("description", bounded_text(1_000)),
    ];
    if include_token {
        fields.insert(0, ("horizonToken", uuid()));
    }
    closed_complete(fields)
}

fn forecast_preparation_preview() -> Value {
    closed_complete(vec![
        ("confirmationToken", uuid()),
        ("expiresAtUnixNanos", integer_text()),
        ("model", forecast_preparation_model(false)),
        ("investmentToken", uuid()),
        ("instrumentLabel", bounded_text(240)),
        ("observedFromUnixNanos", integer_text()),
        ("observedThroughUnixNanos", integer_text()),
        ("availableAtUnixNanos", integer_text()),
        ("observationCount", bounded_unsigned_range(1, 4_096)),
        ("horizon", forecast_preparation_horizon(false)),
        ("limitations", bounded_array(bounded_text(4_096), 256)),
        ("analysisOnly", constant_bool(true)),
    ])
}

fn backtest_preparation_options() -> Value {
    closed(
        vec![
            (
                "histories",
                bounded_array(
                    closed(
                        vec![
                            ("historyToken", bounded_text(256)),
                            ("label", bounded_text(200)),
                            ("investmentCount", positive_integer()),
                            (
                                "periods",
                                bounded_nonempty_array(
                                    closed(
                                        vec![
                                            ("periodToken", bounded_text(256)),
                                            ("label", bounded_text(240)),
                                            ("startsAt", timestamp()),
                                            ("endsAt", timestamp()),
                                        ],
                                        &["periodToken", "label", "startsAt", "endsAt"],
                                    ),
                                    8,
                                ),
                            ),
                        ],
                        &["historyToken", "label", "investmentCount", "periods"],
                    ),
                    4_096,
                ),
            ),
            (
                "methods",
                bounded_nonempty_array(backtest_named_option(), 16),
            ),
            (
                "costPlans",
                bounded_nonempty_array(backtest_named_option(), 16),
            ),
            (
                "portfolios",
                bounded_nonempty_array(backtest_named_option(), 16),
            ),
            (
                "comparisons",
                bounded_nonempty_array(backtest_named_option(), 16),
            ),
            ("guidance", bounded_text(1_000)),
        ],
        &[
            "histories",
            "methods",
            "costPlans",
            "portfolios",
            "comparisons",
            "guidance",
        ],
    )
}

fn backtest_named_option() -> Value {
    closed(
        vec![
            ("token", bounded_text(256)),
            ("label", bounded_text(200)),
            ("description", bounded_text(1_000)),
        ],
        &["token", "label", "description"],
    )
}

fn backtest_preparation_preview() -> Value {
    closed(
        vec![
            ("confirmationToken", bounded_text(256)),
            ("expiresAt", timestamp()),
            ("investmentUniverse", bounded_text(400)),
            ("period", bounded_text(300)),
            ("method", bounded_text(200)),
            ("costs", backtest_cost_assumptions()),
            ("portfolio", bounded_text(200)),
            ("comparison", bounded_text(300)),
            (
                "pointInTimeEvidence",
                enumeration(&["verified", "limited", "unavailable"]),
            ),
            ("outOfSamplePlan", bounded_text(1_000)),
            ("evidence", bounded_nonempty_array(bounded_text(1_000), 16)),
            (
                "assumptions",
                bounded_nonempty_array(bounded_text(1_000), 16),
            ),
            ("limitations", bounded_array(bounded_text(1_000), 32)),
            ("analysisOnly", constant_bool(true)),
        ],
        &[
            "confirmationToken",
            "expiresAt",
            "investmentUniverse",
            "period",
            "method",
            "costs",
            "portfolio",
            "comparison",
            "pointInTimeEvidence",
            "outOfSamplePlan",
            "evidence",
            "assumptions",
            "limitations",
            "analysisOnly",
        ],
    )
}

fn backtest_cost_assumptions() -> Value {
    closed_complete(vec![
        ("fees", bounded_text(200)),
        ("spread", bounded_text(200)),
        ("slippage", bounded_text(200)),
        ("latency", bounded_text(200)),
        ("participationLimit", bounded_text(200)),
        ("partialFills", bounded_text(200)),
    ])
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

fn fair_value_governance_preview_result() -> Value {
    closed_complete(vec![("preview", fair_value_governance_preview())])
}

fn fair_value_governance_preview() -> Value {
    closed_complete(vec![
        ("previewId", uuid()),
        (
            "requiredRoles",
            fixed_array(fair_value_governance_role(), 1),
        ),
        ("distinctPrincipalCount", bounded_unsigned_range(1, 2)),
        ("eligiblePrincipalIds", bounded_nonempty_array(uuid(), 64)),
        ("expiresAt", timestamp()),
        ("effects", fixed_array(fair_value_governance_effect(), 1)),
    ])
}

fn fair_value_governance_commit() -> Value {
    closed_complete(vec![
        ("confirmationToken", bounded_text(256)),
        ("previewId", uuid()),
        ("committedAt", timestamp()),
        ("effects", fixed_array(fair_value_governance_effect(), 1)),
    ])
}

fn fair_value_governance_role() -> Value {
    enumeration(&[
        "fairValueApprover",
        "fairValueOverrideApprover",
        "fairValueRevoker",
        "fairValueMarketAccessApprover",
    ])
}

fn fair_value_governance_effect() -> Value {
    closed_complete(vec![(
        "kind",
        enumeration(&[
            "fairValueApproval",
            "fairValueOverride",
            "fairValueApprovalRevocation",
            "fairValueMarketAccess",
        ]),
    )])
}

fn fair_value_workspace() -> Value {
    closed(
        vec![
            (
                "measurements",
                bounded_array(fair_value_measurement_summary(), 10_000),
            ),
            (
                "selectedMeasurement",
                nullable(fair_value_measurement_detail()),
            ),
        ],
        &["measurements", "selectedMeasurement"],
    )
}

fn fair_value_amount() -> Value {
    closed(
        vec![
            ("amount", bounded_text(96)),
            ("currency", bounded_text(3)),
            ("scale", bounded_unsigned(28)),
            (
                "amountBasis",
                enumeration(&[
                    "per_instrument_unit",
                    "reporting_entity_total",
                    "position_total",
                ]),
            ),
        ],
        &["amount", "currency", "scale", "amountBasis"],
    )
}

fn fair_value_classification() -> Value {
    closed(
        vec![
            ("classificationToken", uuid()),
            (
                "hierarchy",
                enumeration(&["level_1", "level_2", "level_3", "unclassified"]),
            ),
            (
                "basis",
                closed(
                    vec![("kind", enumeration(&["rules", "override"]))],
                    &["kind"],
                ),
            ),
            ("checkCount", bounded_unsigned(10_000)),
            ("reasonCount", bounded_unsigned(10_000)),
        ],
        &[
            "classificationToken",
            "hierarchy",
            "basis",
            "checkCount",
            "reasonCount",
        ],
    )
}

fn fair_value_measurement_summary() -> Value {
    closed(
        vec![
            ("measurementToken", uuid()),
            ("accountId", uuid()),
            ("instrumentId", uuid()),
            ("amount", fair_value_amount()),
            ("measurementAt", timestamp()),
            ("preparedAt", timestamp()),
            ("preparedBy", bounded_text(256)),
            (
                "method",
                enumeration(&[
                    "quoted_market_price",
                    "market_approach",
                    "income_approach",
                    "cost_approach",
                ]),
            ),
            ("inputCount", bounded_unsigned(10_000)),
            ("classification", nullable(fair_value_classification())),
        ],
        &[
            "measurementToken",
            "accountId",
            "instrumentId",
            "amount",
            "measurementAt",
            "preparedAt",
            "preparedBy",
            "method",
            "inputCount",
            "classification",
        ],
    )
}

fn fair_value_measurement_detail() -> Value {
    closed(
        vec![
            ("measurementToken", uuid()),
            ("accountId", uuid()),
            ("instrumentId", uuid()),
            ("amount", fair_value_amount()),
            ("measurementAt", timestamp()),
            ("preparedAt", timestamp()),
            ("preparedBy", bounded_text(256)),
            (
                "method",
                enumeration(&[
                    "quoted_market_price",
                    "market_approach",
                    "income_approach",
                    "cost_approach",
                ]),
            ),
            ("inputCount", bounded_unsigned(10_000)),
            ("classification", nullable(fair_value_classification())),
            ("inputs", bounded_array(fair_value_product_input(), 10_000)),
            ("explanation", nullable(fair_value_product_explanation())),
            (
                "approvals",
                bounded_array(fair_value_product_approval(), 10_000),
            ),
        ],
        &[
            "measurementToken",
            "accountId",
            "instrumentId",
            "amount",
            "measurementAt",
            "preparedAt",
            "preparedBy",
            "method",
            "inputCount",
            "classification",
            "inputs",
            "explanation",
            "approvals",
        ],
    )
}

fn fair_value_product_explanation() -> Value {
    closed(
        vec![
            (
                "checks",
                bounded_array(
                    closed(
                        vec![
                            ("inputToken", uuid()),
                            ("check", bounded_text(128)),
                            ("passed", boolean()),
                        ],
                        &["inputToken", "check", "passed"],
                    ),
                    10_000,
                ),
            ),
            (
                "reasons",
                bounded_array(
                    closed(
                        vec![
                            ("inputToken", nullable(uuid())),
                            ("reason", bounded_text(128)),
                        ],
                        &["inputToken", "reason"],
                    ),
                    10_000,
                ),
            ),
        ],
        &["checks", "reasons"],
    )
}

fn fair_value_product_input() -> Value {
    closed(
        vec![
            ("inputToken", uuid()),
            ("marketInputToken", nullable(uuid())),
            ("referenceInstrumentId", uuid()),
            (
                "relationship",
                enumeration(&["identical", "similar", "proxy"]),
            ),
            ("amount", fair_value_amount()),
            (
                "significance",
                enumeration(&["significant", "not_significant"]),
            ),
            (
                "observability",
                enumeration(&["quoted_price", "observable", "unobservable"]),
            ),
            (
                "adjustment",
                enumeration(&["none", "observable", "unobservable"]),
            ),
            (
                "marketActivity",
                enumeration(&["active", "inactive", "not_assessed"]),
            ),
            (
                "marketAccess",
                enumeration(&["accessible", "inaccessible", "not_assessed"]),
            ),
            (
                "marketAccessAssessment",
                nullable(fair_value_product_market_access()),
            ),
            ("dataQuality", bounded_text(64)),
            (
                "useAssessment",
                nullable(fair_value_product_use_assessment()),
            ),
            ("evidence", fair_value_product_evidence()),
        ],
        &[
            "inputToken",
            "marketInputToken",
            "referenceInstrumentId",
            "relationship",
            "amount",
            "significance",
            "observability",
            "adjustment",
            "marketActivity",
            "marketAccess",
            "marketAccessAssessment",
            "dataQuality",
            "useAssessment",
            "evidence",
        ],
    )
}

fn fair_value_product_use_assessment() -> Value {
    closed(
        vec![
            (
                "relationship",
                enumeration(&["identical", "similar", "proxy"]),
            ),
            (
                "observability",
                enumeration(&["quoted_price", "observable", "unobservable"]),
            ),
            (
                "adjustment",
                enumeration(&["none", "observable", "unobservable"]),
            ),
            ("rationale", bounded_text(4_096)),
            ("assessedBy", bounded_text(256)),
            ("assessedAt", timestamp()),
        ],
        &[
            "relationship",
            "observability",
            "adjustment",
            "rationale",
            "assessedBy",
            "assessedAt",
        ],
    )
}

fn fair_value_product_evidence() -> Value {
    closed(
        vec![
            ("kind", bounded_text(64)),
            ("label", bounded_text(128)),
            ("observedAt", nullable(timestamp())),
            ("effectiveAt", nullable(timestamp())),
            ("publishedAt", nullable(timestamp())),
            ("availableAt", nullable(timestamp())),
            ("receivedAt", nullable(timestamp())),
            ("validUntil", nullable(timestamp())),
            ("recordedAt", timestamp()),
            ("verification", enumeration(&["verified", "unverified"])),
        ],
        &[
            "kind",
            "label",
            "observedAt",
            "effectiveAt",
            "publishedAt",
            "availableAt",
            "receivedAt",
            "validUntil",
            "recordedAt",
            "verification",
        ],
    )
}

fn fair_value_product_market_access() -> Value {
    closed(
        vec![
            (
                "conclusion",
                enumeration(&["accessible", "inaccessible", "not_assessed"]),
            ),
            ("effectiveFrom", timestamp()),
            ("effectiveUntil", timestamp()),
            ("rationale", bounded_text(4_096)),
            ("preparedBy", bounded_text(256)),
            ("preparedAt", timestamp()),
            ("approvedBy", bounded_text(256)),
            ("approvedAt", timestamp()),
        ],
        &[
            "conclusion",
            "effectiveFrom",
            "effectiveUntil",
            "rationale",
            "preparedBy",
            "preparedAt",
            "approvedBy",
            "approvedAt",
        ],
    )
}

fn fair_value_product_approval() -> Value {
    closed(
        vec![
            ("approvalToken", uuid()),
            ("approvedBy", bounded_text(256)),
            ("approvedAt", timestamp()),
            ("expiresAt", timestamp()),
            (
                "status",
                enumeration(&["not_yet_effective", "active", "expired", "revoked"]),
            ),
            (
                "revocation",
                nullable(closed(
                    vec![
                        ("revokedBy", bounded_text(256)),
                        ("revokedAt", timestamp()),
                        ("reason", bounded_text(4_096)),
                    ],
                    &["revokedBy", "revokedAt", "reason"],
                )),
            ),
        ],
        &[
            "approvalToken",
            "approvedBy",
            "approvedAt",
            "expiresAt",
            "status",
            "revocation",
        ],
    )
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
    closed_complete(vec![
        ("targetToken", opaque_product_token()),
        ("investment", product_investment()),
        ("thesis", bounded_text(4_096)),
        ("expiresAt", timestamp()),
        ("reviewDueAt", timestamp()),
        ("ladder", fixed_array(manual_paper_ladder_entry(), 10)),
        ("sideChoices", bounded_nonempty_array(paper_choice(), 2)),
        ("orderChoices", fixed_array(manual_paper_order_choice(), 4)),
    ])
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

fn paper_choice() -> Value {
    closed_complete(vec![
        ("value", bounded_text(64)),
        ("label", bounded_text(96)),
        ("explanation", bounded_text(1_000)),
    ])
}

fn manual_paper_order_choice() -> Value {
    closed_complete(vec![
        (
            "value",
            enumeration(&["market", "limit", "stop", "stop_limit"]),
        ),
        ("label", bounded_text(96)),
        ("explanation", bounded_text(1_000)),
        ("requiresLimitLevel", boolean()),
        ("requiresStopLevel", boolean()),
        (
            "timeInForceChoices",
            bounded_nonempty_array(paper_choice(), 4),
        ),
    ])
}

fn product_investment() -> Value {
    closed(
        vec![
            ("name", bounded_text(256)),
            ("symbol", nullable(bounded_text(64))),
        ],
        &["name", "symbol"],
    )
}

fn paper_start_preparation() -> Value {
    closed_complete(vec![
        ("virtualCashChoices", fixed_array(paper_cash_choice(), 3)),
        ("costChoices", fixed_array(paper_cost_choice(), 3)),
        ("modeChoices", fixed_array(paper_mode_choice(), 2)),
    ])
}

fn paper_cash_choice() -> Value {
    closed_complete(vec![
        ("choiceToken", opaque_product_token()),
        ("label", bounded_text(96)),
        ("amount", money()),
        ("explanation", bounded_text(1_000)),
    ])
}

fn paper_cost_choice() -> Value {
    closed_complete(vec![
        ("choiceToken", opaque_product_token()),
        ("label", bounded_text(96)),
        ("estimatedTradingCost", percentage_text()),
        ("explanation", bounded_text(1_000)),
    ])
}

fn paper_mode_choice() -> Value {
    closed_complete(vec![
        ("choiceToken", opaque_product_token()),
        ("label", bounded_text(96)),
        ("explanation", bounded_text(1_000)),
    ])
}

fn paper_start_preview() -> Value {
    closed_complete(vec![
        ("confirmationToken", opaque_product_token()),
        ("expiresAt", timestamp()),
        ("virtualCash", money()),
        ("estimatedTradingCost", percentage_text()),
        ("modeLabel", bounded_text(96)),
        ("safeguards", fixed_array(bounded_text(1_000), 3)),
    ])
}

fn manual_paper_preview() -> Value {
    closed_complete(vec![
        ("confirmationToken", opaque_product_token()),
        ("expiresAt", timestamp()),
        ("investment", product_investment()),
        ("direction", enumeration(&["Buy", "Sell"])),
        (
            "orderApproach",
            enumeration(&["Market", "Limit", "Stop", "Stop limit"]),
        ),
        ("quantity", bounded_text(128)),
        (
            "duration",
            enumeration(&[
                "Today",
                "Until cancelled",
                "Fill now or cancel",
                "All now or cancel",
            ]),
        ),
        ("limitCondition", nullable(paper_price_condition())),
        ("stopCondition", nullable(paper_price_condition())),
        ("safeguards", manual_paper_safeguards()),
        ("simulationWarning", bounded_text(1_000)),
    ])
}

fn paper_price_condition() -> Value {
    closed_complete(vec![("label", bounded_text(96)), ("value", money())])
}

fn manual_paper_safeguards() -> Value {
    closed_complete(vec![
        ("maximumOrderValue", money()),
        ("maximumSlippage", percentage_text()),
        ("shorting", enumeration(&["allowed", "disabled"])),
    ])
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
        closed_complete(vec![
            ("sessionAvailability", constant("ready")),
            ("safeguards", constant("active")),
        ]),
        closed_complete(vec![
            ("sessionAvailability", constant("unavailable")),
            ("safeguards", enumeration(&["active", "action_needed"])),
        ]),
        closed_complete(vec![
            ("sessionAvailability", constant("active")),
            ("safeguards", enumeration(&["active", "action_needed"])),
            ("modeLabel", bounded_text(96)),
            ("accountUpdate", enumeration(&["complete", "incomplete"])),
            (
                "accounts",
                paper_status_bounded_evidence(paper_status_account()),
            ),
            (
                "positions",
                paper_status_bounded_evidence(paper_status_position()),
            ),
            ("safety", paper_status_safety()),
            (
                "recentDecisions",
                paper_status_bounded_evidence(paper_status_decision()),
            ),
            ("reconciliation", paper_status_reconciliation()),
        ]),
    ])
}

fn paper_start_result() -> Value {
    closed_complete(vec![
        ("sessionAvailability", constant("active")),
        ("safeguards", constant("active")),
        ("modeLabel", bounded_text(96)),
        ("message", bounded_text(512)),
    ])
}

fn paper_stop_result() -> Value {
    closed_complete(vec![
        ("sessionAvailability", constant("ready")),
        ("safeguards", constant("active")),
        ("message", bounded_text(512)),
    ])
}

fn paper_status_bounded_evidence(items: Value) -> Value {
    closed(
        vec![
            ("rows", bounded_array(items, 100_000)),
            ("returnedItems", bounded_unsigned(100_000)),
            ("availableItems", bounded_unsigned(100_000)),
        ],
        &["rows", "returnedItems", "availableItems"],
    )
}

fn paper_status_account() -> Value {
    closed_complete(vec![
        ("displayName", bounded_text(256)),
        ("eligible", boolean()),
        ("settledCapital", money()),
        ("markedEquity", money()),
        ("peakMarkedEquity", money()),
        ("grossExposure", money()),
        ("unrealizedPnl", money()),
        ("realizedPnl", money()),
        ("maximumDrawdown", money()),
    ])
}

fn paper_status_position() -> Value {
    closed_complete(vec![
        ("accountName", bounded_text(256)),
        ("investment", product_investment()),
        ("quantity", bounded_text(128)),
        ("costBasis", money()),
    ])
}

fn paper_status_safety() -> Value {
    closed_complete(vec![
        ("maximumOrderValue", money()),
        ("maximumTotalExposure", money()),
        ("maximumPosition", bounded_text(160)),
        ("leverageLimit", percentage_text()),
        ("minimumCapital", money()),
        ("maximumLoss", money()),
        ("maximumDrawdown", money()),
        ("maximumFees", percentage_text()),
        ("maximumPriceDeviation", percentage_text()),
        ("maximumSlippage", percentage_text()),
        ("orderPace", bounded_text(160)),
        ("shorting", enumeration(&["allowed", "disabled"])),
        ("emergencyStop", enumeration(&["engaged", "clear"])),
        (
            "eligibleInvestments",
            paper_status_bounded_evidence(product_investment()),
        ),
    ])
}

fn paper_status_reconciliation() -> Value {
    closed_complete(vec![
        (
            "state",
            enumeration(&["incomplete", "current", "action_needed"]),
        ),
        ("activeOrders", unsigned()),
        ("completedOrders", unsigned()),
        ("fills", unsigned()),
        ("accounts", unsigned()),
        ("positions", unsigned()),
    ])
}

fn paper_status_decision() -> Value {
    closed_complete(vec![
        (
            "outcome",
            enumeration(&[
                "declined",
                "approved",
                "accepted",
                "needs_review",
                "cancel_requested",
                "cancelled",
                "reconciled",
            ]),
        ),
        ("investment", product_investment()),
        ("marketObservedAt", timestamp()),
        ("validUntil", timestamp()),
        ("observedAt", timestamp()),
        (
            "reasons",
            bounded_array(paper_status_execution_audit_reason(), 14),
        ),
    ])
}

fn paper_status_execution_audit_reason() -> Value {
    enumeration(&[
        "The virtual order was declined.",
        "The order result needs review before continuing.",
        "The account needs reconciliation before another order.",
        "The order or account changed before the check completed.",
        "Paper trading is temporarily unavailable. Try again.",
        "Market data is unavailable or too old.",
        "The investment cannot be traded right now.",
        "The order is no longer valid at current conditions.",
        "The order is outside the active price and slippage limits.",
        "Paper trading is paused by the emergency stop.",
        "Available cash or holdings are insufficient.",
        "The investment is not eligible for paper trading.",
        "The order is outside the active safety limits.",
        "The virtual account is not eligible for paper trading.",
    ])
}

fn paper_order() -> Value {
    closed_complete(vec![
        ("actionToken", opaque_product_token()),
        (
            "state",
            enumeration(&[
                "waiting",
                "accepted",
                "partially_filled",
                "filled",
                "cancel_requested",
                "cancelled",
                "declined",
                "expired",
            ]),
        ),
        ("investment", product_investment()),
        ("direction", enumeration(&["buy", "sell"])),
        ("requestedQuantity", bounded_text(128)),
        ("filledQuantity", bounded_text(128)),
        ("averageFillPrice", nullable(money())),
        ("maximumExecutionPrice", money()),
        ("maximumSlippage", percentage_text()),
        ("fees", money()),
        ("acceptedAt", timestamp()),
        ("expiresAt", timestamp()),
        ("targetLinked", boolean()),
        ("cancellationAvailable", boolean()),
    ])
}

fn paper_fill() -> Value {
    closed_complete(vec![
        ("investment", product_investment()),
        ("quantity", bounded_text(128)),
        ("averagePrice", money()),
        ("maximumPrice", money()),
        ("notional", money()),
        ("fee", money()),
        ("occurredAt", timestamp()),
    ])
}

fn paper_cancel_result() -> Value {
    closed_complete(vec![
        ("actionToken", opaque_product_token()),
        (
            "state",
            enumeration(&["pending", "cancelled", "already_complete"]),
        ),
        ("observedAt", timestamp()),
        ("filledQuantity", bounded_text(128)),
        ("averageFillPrice", nullable(money())),
        ("fees", money()),
    ])
}

fn nullable_rows(item: Value) -> Value {
    one_of(vec![null(), array(item)])
}

fn product_lookup_result() -> Value {
    closed(
        vec![
            (
                "query",
                bounded_text(PRODUCT_LOOKUP_QUERY_MAXIMUM_CHARACTERS),
            ),
            ("matches", bounded_array(product_lookup_match(), 64)),
            (
                "categories",
                bounded_array(product_lookup_category_status(), 7),
            ),
            ("truncated", boolean()),
        ],
        &["query", "matches", "categories", "truncated"],
    )
}

fn product_lookup_match() -> Value {
    one_of(vec![
        product_lookup_match_for(
            PRODUCT_LOOKUP_CATEGORY_INVESTMENT,
            closed(
                vec![
                    ("action", constant(PRODUCT_LOOKUP_ACTION_OPEN_INVESTMENT)),
                    ("instrumentId", uuid()),
                ],
                &["action", "instrumentId"],
            ),
        ),
        product_lookup_saved_screen_match(),
    ])
}

fn product_lookup_saved_screen_match() -> Value {
    product_lookup_match_for(
        PRODUCT_LOOKUP_CATEGORY_SAVED_SCREEN,
        closed(
            vec![
                ("action", constant(PRODUCT_LOOKUP_ACTION_OPEN_SAVED_SCREEN)),
                ("screenId", product_lookup_screen_id()),
            ],
            &["action", "screenId"],
        ),
    )
}

fn product_lookup_match_for(category: &str, destination: Value) -> Value {
    closed(
        vec![
            ("category", constant(category)),
            ("title", bounded_text(2_048)),
            ("subtitle", bounded_text(2_048)),
            ("destination", destination),
        ],
        &["category", "title", "subtitle", "destination"],
    )
}

fn product_lookup_category_status() -> Value {
    one_of(vec![
        closed(
            vec![
                ("category", product_lookup_category()),
                ("state", constant("available")),
            ],
            &["category", "state"],
        ),
        closed(
            vec![
                ("category", product_lookup_category()),
                ("state", constant("unavailable")),
                ("message", bounded_text(256)),
            ],
            &["category", "state", "message"],
        ),
    ])
}

fn product_lookup_category() -> Value {
    enumeration(PRODUCT_LOOKUP_CATEGORIES)
}

fn product_lookup_screen_id() -> Value {
    bounded_text(128)
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

fn closed_complete(fields: Vec<(&str, Value)>) -> Value {
    let required = fields.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    object(fields, &required, false)
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

fn percentage_text() -> Value {
    json!({
        "type": "string",
        "minLength": 2,
        "maxLength": 64,
        "pattern": r"^-?[0-9]+(?:\.[0-9]+)?%$"
    })
}

fn opaque_product_token() -> Value {
    json!({
        "type": "string",
        "minLength": 16,
        "maxLength": 512,
        "pattern": "^[a-z][a-z0-9_]{15,511}$"
    })
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

fn positive_integer() -> Value {
    bounded_unsigned_range(1, u64::MAX)
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
            if let Err(error) = super::super::descriptor_for(*operation) {
                panic!(
                    "invalid output data contract for {}: {error}",
                    operation.name
                );
            }
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
