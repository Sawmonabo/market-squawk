//! Closed descriptor registry and common request admission for the local application.

mod output;

use std::collections::HashSet;

use chrono::DateTime;
use market_squawk_execution::MAX_PAPER_FEE_BASIS_POINTS;
use market_squawk_services::{
    NDJSON_ARTIFACT_MEDIA_TYPE, PARQUET_ARTIFACT_MEDIA_TYPE, ScopeRequirement, ServiceCapabilities,
    ServiceCapabilityError, ServiceDomain, SourceEvidencePolicy, ToolArtifactPolicy,
    ToolAuthorization, ToolContract, ToolDescriptor, ToolEffects, ToolInputError, ToolResultPolicy,
    ToolScope,
};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use self::output::output_data_schema;

/// Exact contract version shared by CLI and MCP for the first local release.
pub const APPLICATION_CONTRACT_VERSION: &str = "1";

const MAXIMUM_INSTRUMENTS: usize = 256;
const MAXIMUM_SOURCES: usize = 32;
const MAXIMUM_RESULT_ITEMS: u64 = 100_000;
const MAXIMUM_RESULT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_IDENTIFIER_BYTES: usize = 256;
const MAXIMUM_TEXT_BYTES: usize = 4 * 1024;
const MAXIMUM_FAIR_VALUE_INPUTS: usize = 4_096;
const MAXIMUM_FAIR_VALUE_ACTOR_BYTES: usize = 128;
const MAXIMUM_FAIR_VALUE_ROW_OFFSET: u64 = 999_999;
const MAXIMUM_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_ARTIFACT_CHUNK_BYTES: u64 = 32 * 1024;
const MAXIMUM_SOURCE_INSPECTION_PAGE_INDEX: u64 = 63;
const MAXIMUM_SOURCE_INSPECTION_RECORDS: u64 = 1_024;
const MAXIMUM_FORECAST_VALIDITY_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;

const LOCAL_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::Required,
    ScopeRequirement::NotApplicable,
);
const SOURCE_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::Required,
    ScopeRequirement::Optional,
);
const SOURCE_DISCOVERY_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::Required,
    ScopeRequirement::Required,
);
const DATA_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::Optional,
    ScopeRequirement::Optional,
    ScopeRequirement::Required,
    ScopeRequirement::Optional,
);
const JOB_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
);
const PORTFOLIO_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::Optional,
    ScopeRequirement::Optional,
    ScopeRequirement::Required,
    ScopeRequirement::Optional,
);
const PORTFOLIO_CANDIDATE_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::Required,
    ScopeRequirement::NotApplicable,
);

const NO_ARGUMENTS: &[ArgumentSpec] = &[];
const LIST_DATASETS_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::optional(
    "afterDataset",
    ArgumentKind::Identifier,
)];
const DATASET_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::required("dataset", ArgumentKind::Identifier)];
const OPTIONAL_DATASET_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::optional("dataset", ArgumentKind::Identifier)];
const FEATURE_DATASET_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("dataset", ArgumentKind::Identifier),
    ArgumentSpec::optional("afterDataset", ArgumentKind::Identifier),
];
const FEATURE_DATASET_PREVIEW_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("catalogGeneration", ArgumentKind::Sha256),
    ArgumentSpec::required("dataset", ArgumentKind::Identifier),
    ArgumentSpec::required("intendedUse", ArgumentKind::Identifier),
];
const FEATURE_DATASET_PREPARED_START_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("receipt", ArgumentKind::Object)];
const ANALYSIS_LOOKUP_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("query", ArgumentKind::Text),
    ArgumentSpec::optional("categories", ArgumentKind::Array),
];
const MARKET_UNIVERSE_SEARCH_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::optional("query", ArgumentKind::Text)];
const PROVIDER_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::required("provider", ArgumentKind::Identifier)];
const MACRO_DASHBOARD_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required(
        "provider",
        ArgumentKind::Enumeration(&["federal-reserve-board.data-download-program"]),
    ),
    ArgumentSpec::required("release", ArgumentKind::Enumeration(&["h15"])),
];
const PROVIDER_CREDENTIAL_BUNDLE_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("inputTicketId", ArgumentKind::Uuid)];
const SOURCE_DISCOVERY_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("provider", ArgumentKind::Identifier),
    ArgumentSpec::required("dataset", ArgumentKind::Identifier),
];
const SOURCE_INSPECTION_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("provider", ArgumentKind::Identifier),
    ArgumentSpec::required("onboardingSessionId", ArgumentKind::Uuid),
    ArgumentSpec::required("datasetIdentifier", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "pageIndex",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: MAXIMUM_SOURCE_INSPECTION_PAGE_INDEX,
        },
    ),
    ArgumentSpec::required(
        "maxRecords",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: MAXIMUM_SOURCE_INSPECTION_RECORDS,
        },
    ),
];
const SOURCE_LIFECYCLE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("provider", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "expectedStateRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::optional(
        "expectedGeneration",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::optional("expectedRuntimeGenerationSha256", ArgumentKind::Sha256),
    ArgumentSpec::optional("onboardingSessionId", ArgumentKind::Uuid),
    ArgumentSpec::optional("publicConfigurationSha256", ArgumentKind::Sha256),
    ArgumentSpec::optional("reason", ArgumentKind::Identifier),
];
const ACCOUNT_ARGUMENT: &[ArgumentSpec] = &[ArgumentSpec::required(
    "accountId",
    ArgumentKind::Identifier,
)];
const PORTFOLIO_IMPORT_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("artifactId", ArgumentKind::Identifier),
];
const PORTFOLIO_IMPORT_PREVIEW_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("inputTicketId", ArgumentKind::Uuid),
];
const PORTFOLIO_IMPORT_APPROVAL_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("previewId", ArgumentKind::Sha256),
    ArgumentSpec::required("previewDigest", ArgumentKind::Sha256),
    ArgumentSpec::required(
        "interpretations",
        ArgumentKind::PortfolioImportInterpretations,
    ),
];
const PORTFOLIO_IMPORT_COMMIT_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("approvalId", ArgumentKind::Uuid)];
const PORTFOLIO_IMPORT_DISCARD_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("previewId", ArgumentKind::Sha256)];
const RESEARCH_FILE_PREVIEW_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("inputTicketId", ArgumentKind::Uuid),
    ArgumentSpec::required(
        "format",
        ArgumentKind::Enumeration(&["csv", "json", "ndjson", "parquet"]),
    ),
];
const RESEARCH_FILE_COMMIT_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("previewId", ArgumentKind::Sha256),
    ArgumentSpec::required("mapping", ArgumentKind::ResearchFileMapping),
];
const RESEARCH_FILE_DISCARD_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("previewId", ArgumentKind::Sha256)];
const LIST_ACCOUNTS_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::optional(
    "afterAccountId",
    ArgumentKind::Identifier,
)];
const RECOMMENDATION_SETUP_PREVIEW_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required(
        "expectedRevision",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required("accountId", ArgumentKind::Uuid),
    ArgumentSpec::required(
        "allocationProfile",
        ArgumentKind::RecommendationAllocationProfile,
    ),
];
const RECOMMENDATION_SETUP_COMMIT_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("previewId", ArgumentKind::Uuid),
    ArgumentSpec::required("previewDigest", ArgumentKind::Sha256),
];
const LIST_REVISIONS_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::optional("afterRevisionId", ArgumentKind::Sha256),
];
const PORTFOLIO_ATTRIBUTION_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("baselineRevisionId", ArgumentKind::Sha256),
];
const PORTFOLIO_SCENARIO_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("scenario", ArgumentKind::Object),
];
const PORTFOLIO_SCENARIO_BATCH_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("scenarios", ArgumentKind::Array),
];
const PORTFOLIO_REBALANCE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("proposal", ArgumentKind::Object),
];
const PORTFOLIO_CANDIDATE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("instrumentId", ArgumentKind::Identifier),
    ArgumentSpec::required("proposedQuantity", ArgumentKind::Decimal),
    ArgumentSpec::required("scenarioShock", ArgumentKind::Decimal),
];
const MODEL_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::required("modelId", ArgumentKind::Identifier)];
const MODEL_EVALUATION_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("modelId", ArgumentKind::Identifier),
    ArgumentSpec::required("input", ArgumentKind::Object),
];
const MODEL_TRAINING_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("configTicketId", ArgumentKind::Uuid),
    ArgumentSpec::required("authorityTicketId", ArgumentKind::Uuid),
];
const MODEL_FORECAST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("modelId", ArgumentKind::Identifier),
    ArgumentSpec::required("request", ArgumentKind::ForecastRequest),
];
const MODEL_FORECAST_PREPARATION_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("selection", ArgumentKind::Object)];
const MODEL_FORECAST_PREPARED_START_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("receipt", ArgumentKind::Object)];
const MODEL_FORECAST_ID_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("vintageId", ArgumentKind::Sha256)];
const MODEL_LATEST_VALID_FORECAST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("instrumentId", ArgumentKind::Uuid),
    ArgumentSpec::required("asOf", ArgumentKind::Timestamp),
];
const DECISION_SAVE_SCREEN_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional(
        "expectedRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u32::MAX as u64,
        },
    ),
    ArgumentSpec::required("screen", ArgumentKind::Object),
];
const DECISION_RUN_SCREEN_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("screenId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "screenRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u32::MAX as u64,
        },
    ),
    ArgumentSpec::required("datasetManifest", ArgumentKind::Object),
    ArgumentSpec::required("asOf", ArgumentKind::Timestamp),
];
const DECISION_LIST_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required(
    "limit",
    ArgumentKind::Unsigned {
        minimum: 1,
        maximum: 4_096,
    },
)];
const DECISION_RUN_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("runId", ArgumentKind::Identifier)];
const DECISION_RUN_LIST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("afterRunId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 1_000,
        },
    ),
];
const DECISION_DOSSIER_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required(
    "dossierId",
    ArgumentKind::Identifier,
)];
const DECISION_DOSSIER_LIST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("candidateId", ArgumentKind::Identifier),
    ArgumentSpec::optional("afterDossierId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 1_000,
        },
    ),
];
const DECISION_DOSSIER_PREPARATION_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required(
    "candidateId",
    ArgumentKind::Identifier,
)];
const DECISION_DOSSIER_PREVIEW_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("draft", ArgumentKind::Object)];
const DECISION_DOSSIER_COMMIT_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("receiptId", ArgumentKind::Uuid)];
const DECISION_TARGET_PREPARATION_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required(
    "dossierId",
    ArgumentKind::Identifier,
)];
const DECISION_TARGET_PREVIEW_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("draft", ArgumentKind::Object)];
const DECISION_TARGET_COMMIT_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("receiptId", ArgumentKind::Uuid)];
const DECISION_TARGET_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("targetId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "revision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u32::MAX as u64,
        },
    ),
];
const DECISION_TARGET_LIST_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("targetId", ArgumentKind::Identifier)];
const DECISION_TARGET_INDEX_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("afterTargetId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 1_000,
        },
    ),
];
const DECISION_TARGET_REVIEW_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("review", ArgumentKind::Object)];
const DECISION_INVESTMENT_ANALYSIS_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("analysisId", ArgumentKind::Sha256)];
const DECISION_INVESTMENT_ANALYSIS_LIST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("afterAnalysisId", ArgumentKind::Sha256),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 1_000,
        },
    ),
];
const DECISION_RECOMMENDATION_TRACK_RECORD_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("profileId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "profileRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u32::MAX as u64,
        },
    ),
    ArgumentSpec::required("profileDigest", ArgumentKind::Sha256),
    ArgumentSpec::required(
        "horizonNanos",
        ArgumentKind::Signed {
            minimum: 1,
            maximum: i64::MAX,
        },
    ),
    ArgumentSpec::required(
        "evaluatedAtUnixNanos",
        ArgumentKind::Signed {
            minimum: i64::MIN,
            maximum: i64::MAX,
        },
    ),
];
const OPERATIONS_BACKUP_LIST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("afterBackupId", ArgumentKind::Sha256),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 64,
        },
    ),
];
const OPERATIONS_BACKUP_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("backupId", ArgumentKind::Sha256)];
const OPERATIONS_RETENTION_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required(
    "keepLatest",
    ArgumentKind::Unsigned {
        minimum: 1,
        maximum: 128,
    },
)];
const OPERATIONS_WORKSPACE_LIST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("afterWorkspaceId", ArgumentKind::Uuid),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 64,
        },
    ),
];
const OPERATIONS_WORKSPACE_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("workspaceId", ArgumentKind::Uuid)];
const OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("previewId", ArgumentKind::Uuid),
    ArgumentSpec::required("previewDigest", ArgumentKind::Sha256),
];
const OPERATIONS_LOG_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("from", ArgumentKind::Timestamp),
    ArgumentSpec::optional("through", ArgumentKind::Timestamp),
    ArgumentSpec::optional(
        "minimumSeverity",
        ArgumentKind::Enumeration(&["trace", "debug", "info", "warn", "error"]),
    ),
    ArgumentSpec::optional(
        "domain",
        ArgumentKind::Enumeration(&[
            "application",
            "source",
            "market",
            "research",
            "portfolio",
            "model",
            "backtest",
            "execution",
            "risk",
            "fair_value",
            "mcp",
            "lifecycle",
        ]),
    ),
    ArgumentSpec::optional("sourceId", ArgumentKind::Identifier),
    ArgumentSpec::optional("jobId", ArgumentKind::Identifier),
    ArgumentSpec::optional("correlationId", ArgumentKind::Identifier),
    ArgumentSpec::optional("search", ArgumentKind::Text),
    ArgumentSpec::optional(
        "afterSequence",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 10_000,
        },
    ),
];
const OPERATIONS_SETTINGS_CHANGE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required(
        "expectedRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required("changes", ArgumentKind::SettingsChanges),
];
const OPERATIONS_SETTINGS_ROLLBACK_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required(
        "expectedRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required(
        "targetRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
];
const SETUP_PLAN_PREVIEW_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required(
        "expectedRevision",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required("selection", ArgumentKind::Object),
];
const SETUP_PLAN_CONFIRMATION_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("previewId", ArgumentKind::Uuid),
    ArgumentSpec::required("previewSha256", ArgumentKind::Sha256),
];
const MEASUREMENT_ARGUMENT: &[ArgumentSpec] = &[ArgumentSpec::required(
    "measurementId",
    ArgumentKind::Identifier,
)];
const FAIR_VALUE_STATUS_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("measurementId", ArgumentKind::Identifier),
    ArgumentSpec::required("at", ArgumentKind::Timestamp),
];
const FAIR_VALUE_MEASUREMENT_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required(
    "measurement",
    ArgumentKind::FairValueMeasurement,
)];
const FAIR_VALUE_APPROVAL_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("measurementId", ArgumentKind::Identifier),
    ArgumentSpec::required("decisionId", ArgumentKind::Identifier),
    ArgumentSpec::required("approvedBy", ArgumentKind::Identifier),
    ArgumentSpec::required("approvedAt", ArgumentKind::Timestamp),
    ArgumentSpec::required("expiresAt", ArgumentKind::Timestamp),
];
const FAIR_VALUE_OVERRIDE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("measurementId", ArgumentKind::Identifier),
    ArgumentSpec::required("decisionId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "requestedHierarchy",
        ArgumentKind::Enumeration(&["level_2", "level_3"]),
    ),
    ArgumentSpec::required("justification", ArgumentKind::Text),
    ArgumentSpec::required("preparedBy", ArgumentKind::Identifier),
    ArgumentSpec::required("preparedAt", ArgumentKind::Timestamp),
    ArgumentSpec::required("expiresAt", ArgumentKind::Timestamp),
];
const FAIR_VALUE_REVOCATION_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("approvalId", ArgumentKind::Identifier),
    ArgumentSpec::required("revokedBy", ArgumentKind::Identifier),
    ArgumentSpec::required("revokedAt", ArgumentKind::Timestamp),
    ArgumentSpec::required("reason", ArgumentKind::Text),
];
const FAIR_VALUE_AUDIT_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("after", ArgumentKind::Object),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 10_000,
        },
    ),
];
const MARKET_ACCESS_ARGUMENT: &[ArgumentSpec] = &[ArgumentSpec::required(
    "assessmentId",
    ArgumentKind::Identifier,
)];
const FAIR_VALUE_MARKET_ACCESS_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("venueId", ArgumentKind::Identifier),
    ArgumentSpec::required("instrumentId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "conclusion",
        ArgumentKind::Enumeration(&["accessible", "inaccessible"]),
    ),
    ArgumentSpec::required("effectiveFrom", ArgumentKind::Timestamp),
    ArgumentSpec::required("effectiveUntil", ArgumentKind::Timestamp),
    ArgumentSpec::required("rationale", ArgumentKind::Text),
    ArgumentSpec::required("preparedBy", ArgumentKind::Identifier),
    ArgumentSpec::required("preparedAt", ArgumentKind::Timestamp),
    ArgumentSpec::required("approvedBy", ArgumentKind::Identifier),
    ArgumentSpec::required("approvedAt", ArgumentKind::Timestamp),
];
const BACKTEST_RUN_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("registration", ArgumentKind::Object)];
const BACKTEST_PREPARATION_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("selection", ArgumentKind::Object)];
const BACKTEST_PREPARED_START_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("receipt", ArgumentKind::Object)];
const DATASET_BUILD_ARGUMENTS: &[ArgumentSpec] =
    &[ArgumentSpec::required("registration", ArgumentKind::Object)];
const RUN_ARGUMENT: &[ArgumentSpec] = &[ArgumentSpec::required("runId", ArgumentKind::Identifier)];
const ARTIFACT_READ_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("artifactId", ArgumentKind::Identifier),
    ArgumentSpec::required("sha256", ArgumentKind::Sha256),
    ArgumentSpec::required(
        "byteCount",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: MAXIMUM_ARTIFACT_BYTES,
        },
    ),
    ArgumentSpec::required(
        "mediaType",
        ArgumentKind::Enumeration(&[
            "application/json",
            PARQUET_ARTIFACT_MEDIA_TYPE,
            NDJSON_ARTIFACT_MEDIA_TYPE,
        ]),
    ),
    ArgumentSpec::required(
        "offset",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: MAXIMUM_ARTIFACT_BYTES,
        },
    ),
    ArgumentSpec::required(
        "maximumBytes",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: MAXIMUM_ARTIFACT_CHUNK_BYTES,
        },
    ),
];
const BOT_START_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required(
        "provider",
        ArgumentKind::Enumeration(&["coinbase", "coinbase-direct", "kraken"]),
    ),
    ArgumentSpec::optional("providerSessionId", ArgumentKind::Identifier),
    ArgumentSpec::optional(
        "strategyMode",
        ArgumentKind::Enumeration(&["manual", "book_imbalance"]),
    ),
    ArgumentSpec::required("initialCash", ArgumentKind::Decimal),
    ArgumentSpec::required(
        "feeBasisPoints",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: MAX_PAPER_FEE_BASIS_POINTS,
        },
    ),
];
const BOT_STOP_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required("reason", ArgumentKind::Text)];
const MANUAL_PAPER_DRAFT_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("targetId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "targetRevision",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u32::MAX as u64,
        },
    ),
    ArgumentSpec::required("side", ArgumentKind::Enumeration(&["buy", "sell"])),
    ArgumentSpec::required(
        "orderType",
        ArgumentKind::Enumeration(&["market", "limit", "stop", "stop_limit"]),
    ),
    ArgumentSpec::required("quantityLots", ArgumentKind::PositiveLotQuantity),
    ArgumentSpec::optional(
        "limitTargetLevel",
        ArgumentKind::Enumeration(MANUAL_PAPER_TARGET_LEVELS),
    ),
    ArgumentSpec::optional(
        "stopTargetLevel",
        ArgumentKind::Enumeration(MANUAL_PAPER_TARGET_LEVELS),
    ),
    ArgumentSpec::required(
        "timeInForce",
        ArgumentKind::Enumeration(&[
            "day",
            "good_til_cancelled",
            "immediate_or_cancel",
            "fill_or_kill",
        ]),
    ),
];
const MANUAL_PAPER_TARGET_LEVELS: &[&str] = &[
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
];
const ORDER_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::required("orderId", ArgumentKind::Identifier)];
const INGEST_SOURCE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("provider", ArgumentKind::Identifier),
    ArgumentSpec::required("object", ArgumentKind::Identifier),
    ArgumentSpec::required("dataset", ArgumentKind::Identifier),
    ArgumentSpec::required("discoveryReceipt", ArgumentKind::Identifier),
];
const JOB_LIST_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::optional("afterJobId", ArgumentKind::Identifier),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 1_024,
        },
    ),
];
const JOB_GET_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("jobId", ArgumentKind::Uuid),
    ArgumentSpec::required(
        "generation",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
];
const JOB_WATCH_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("jobId", ArgumentKind::Uuid),
    ArgumentSpec::required(
        "generation",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required(
        "afterSequence",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required(
        "limit",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: 4_096,
        },
    ),
];
const JOB_MUTATION_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("jobId", ArgumentKind::Uuid),
    ArgumentSpec::required(
        "generation",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required(
        "expectedSequence",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: u64::MAX,
        },
    ),
];
const JOB_CONFIRM_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("jobId", ArgumentKind::Uuid),
    ArgumentSpec::required(
        "generation",
        ArgumentKind::Unsigned {
            minimum: 1,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required(
        "expectedSequence",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: u64::MAX,
        },
    ),
    ArgumentSpec::required("identity", ArgumentKind::Identifier),
    ArgumentSpec::required("digest", ArgumentKind::Sha256),
];

const OPERATION_SPECS: &[OperationSpec] = &[
    read(
        "Job.List",
        "List bounded latest job generations in stable identity order.",
        ServiceDomain::Job,
        JOB_SCOPE,
        JOB_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Job.Get",
        "Return one durable sanitized job generation.",
        ServiceDomain::Job,
        JOB_SCOPE,
        JOB_GET_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Job.Watch",
        "Return a bounded ordered page of durable job events.",
        ServiceDomain::Job,
        JOB_SCOPE,
        JOB_WATCH_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Job.Cancel",
        "Request cooperative cancellation of one exact job generation.",
        ServiceDomain::Job,
        JOB_SCOPE,
        JOB_MUTATION_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Job.Confirm",
        "Confirm one exact generation-bound job request.",
        ServiceDomain::Job,
        JOB_SCOPE,
        JOB_CONFIRM_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Job.Retry",
        "Start the next bounded generation after a retryable terminal failure.",
        ServiceDomain::Job,
        JOB_SCOPE,
        JOB_MUTATION_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.ImportCredentialBundle",
        "Claim one native-staged provider credential bundle, transfer enabled secrets into the protected store, and return only secret-free provider dispositions.",
        ServiceDomain::Source,
        LOCAL_SCOPE,
        PROVIDER_CREDENTIAL_BUNDLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.Register",
        "Register one code-supported provider capability in the local catalog.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        PROVIDER_ARGUMENT,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Source.GetStatus",
        "Return bounded configured and onboarding state for local providers.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Source.GetCoverage",
        "Return explicit provider, venue, instrument, and delay coverage.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Source.GetHealth",
        "Return bounded source connection, integrity, and freshness health.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    mutation(
        "Source.Setup",
        "Start or resume capability-gated local provider onboarding.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        PROVIDER_ARGUMENT,
        ToolAuthorization::LocalConfirmation,
    ),
    source_listing(
        "Source.ListObjects",
        "List bounded exact provider objects without minting ingestion authority.",
        ServiceDomain::Source,
        SOURCE_DISCOVERY_SCOPE,
        SOURCE_DISCOVERY_ARGUMENTS,
    ),
    source_listing(
        "Source.Inspect",
        "Inspect one bounded provider page without persisting provider data.",
        ServiceDomain::Source,
        SOURCE_DISCOVERY_SCOPE,
        SOURCE_INSPECTION_ARGUMENTS,
    ),
    source_discovery(
        "Source.Discover",
        "Discover bounded exact provider objects and receipt-bound ingestion authority.",
        ServiceDomain::Source,
        SOURCE_DISCOVERY_SCOPE,
        SOURCE_DISCOVERY_ARGUMENTS,
    ),
    mutation(
        "Source.Start",
        "Start one admitted source configuration under exact state revision fencing.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        SOURCE_LIFECYCLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.Stop",
        "Stop source activity while preserving registration and retained data.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        SOURCE_LIFECYCLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.Retry",
        "Retry one blocked source lifecycle phase under the existing provider budget.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        SOURCE_LIFECYCLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.Resynchronize",
        "Invalidate one source generation and establish a verified successor.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        SOURCE_LIFECYCLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.Verify",
        "Revalidate source readiness without starting runtime activity.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        SOURCE_LIFECYCLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.Reconfigure",
        "Activate an already prepared public source configuration generation.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        SOURCE_LIFECYCLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Source.Remove",
        "Revoke source runtime authority under the selected local cleanup contract.",
        ServiceDomain::Source,
        SOURCE_SCOPE,
        SOURCE_LIFECYCLE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Market.GetSnapshot",
        "Return current bounded market state with explicit coverage and quality evidence.",
        ServiceDomain::Market,
        DATA_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read_data("Market.GetTrades", "Return bounded trade observations."),
    read_data("Market.GetQuotes", "Return bounded quote observations."),
    read_data("Market.GetBooks", "Return bounded order-book observations."),
    read(
        "Market.GetQuality",
        "Return source and instrument data-quality state.",
        ServiceDomain::Market,
        DATA_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read_data(
        "Market.GetComparisons",
        "Compare bounded observations across requested sources.",
    ),
    read_data(
        "Market.GetUnifiedFeed",
        "Return one source-preserving market view per exact instrument.",
    ),
    read(
        "Market.SearchUniverse",
        "Search the bounded admitted market reference universe without creating tradable instruments.",
        ServiceDomain::Market,
        DATA_SCOPE,
        MARKET_UNIVERSE_SEARCH_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Research.ListDatasets",
        "List immutable local analytical dataset generations.",
        ServiceDomain::Research,
        LOCAL_SCOPE,
        LIST_DATASETS_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Research.GetManifest",
        "Return one immutable analytical dataset manifest.",
        ServiceDomain::Research,
        LOCAL_SCOPE,
        DATASET_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Research.GetHistory",
        "Return bounded point-in-time research observations and revisions.",
        ServiceDomain::Research,
        DATA_SCOPE,
        DATASET_ARGUMENT,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Research.GetAlternativeData",
        "Return bounded alternative-data observations from an immutable dataset.",
        ServiceDomain::Research,
        DATA_SCOPE,
        DATASET_ARGUMENT,
        SourceEvidencePolicy::Required,
    ),
    source_ingest(
        "Research.StartIngestSource",
        "Start durable extraction and ingestion of one receipt-admitted provider object.",
        ServiceDomain::Research,
        SOURCE_SCOPE,
        INGEST_SOURCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    source_ingest(
        "Research.IngestSource",
        "Compatibility wait for durable extraction and ingestion under the caller deadline.",
        ServiceDomain::Research,
        SOURCE_SCOPE,
        INGEST_SOURCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Research.PreviewStagedFile",
        "Claim one staged user-owned research file and return a bounded path-free preview.",
        ServiceDomain::Research,
        LOCAL_SCOPE,
        RESEARCH_FILE_PREVIEW_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    source_evidence_mutation(
        "Research.CommitStagedFile",
        "Bind one guided mapping to exact staged bytes and start durable research ingestion.",
        ServiceDomain::Research,
        LOCAL_SCOPE,
        RESEARCH_FILE_COMMIT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Research.DiscardStagedFile",
        "Discard one uncommitted user-owned research-file preview.",
        ServiceDomain::Research,
        LOCAL_SCOPE,
        RESEARCH_FILE_DISCARD_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Research.StartDatasetBuild",
        "Start one durable point-in-time dataset publication from an admitted build registration.",
        ServiceDomain::Research,
        LOCAL_SCOPE,
        DATASET_BUILD_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Research.StartExport",
        "Start one durable manifest-pinned research export to controlled local artifacts.",
        ServiceDomain::Research,
        DATA_SCOPE,
        DATASET_ARGUMENT,
        ToolAuthorization::LocalConfirmation,
    ),
    read_observations(
        "Fundamental.GetFilings",
        "Return bounded filing observations.",
        ServiceDomain::Fundamental,
    ),
    read_observations(
        "Fundamental.GetFacts",
        "Return bounded reported fundamental facts.",
        ServiceDomain::Fundamental,
    ),
    read_observations(
        "Fundamental.GetStatements",
        "Return bounded normalized financial-statement observations.",
        ServiceDomain::Fundamental,
    ),
    read_observations(
        "Fundamental.GetRatios",
        "Return bounded point-in-time fundamental ratios.",
        ServiceDomain::Fundamental,
    ),
    OperationSpec {
        name: "Macro.GetDashboard",
        description: "Return the exact latest-known Federal Reserve Board H.15 Treasury constant-maturity dashboard.",
        domain: ServiceDomain::Macro,
        scope: LOCAL_SCOPE,
        arguments: MACRO_DASHBOARD_ARGUMENTS,
        authorization: ToolAuthorization::ReadOnly,
        source_evidence: SourceEvidencePolicy::Required,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: false,
        idempotent: true,
        open_world: false,
    },
    read_observations(
        "Macro.ListSeries",
        "List bounded macroeconomic series represented by a dataset.",
        ServiceDomain::Macro,
    ),
    read_observations(
        "Macro.GetObservations",
        "Return bounded macroeconomic observations.",
        ServiceDomain::Macro,
    ),
    read_observations(
        "Macro.GetVintages",
        "Return bounded point-in-time macroeconomic vintages.",
        ServiceDomain::Macro,
    ),
    read_observations(
        "Macro.GetRevisions",
        "Return bounded macroeconomic revision history.",
        ServiceDomain::Macro,
    ),
    mutation(
        "Portfolio.Import",
        "Import one controlled portfolio artifact and preserve reconciliation evidence.",
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        PORTFOLIO_IMPORT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Portfolio.PreviewStagedImport",
        "Claim one staged portfolio file and return a bounded server-owned interpretation preview.",
        ServiceDomain::Portfolio,
        LOCAL_SCOPE,
        PORTFOLIO_IMPORT_PREVIEW_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    idempotent_mutation(
        "Portfolio.ApproveStagedImport",
        "Bind selected interpretations to one exact server-owned portfolio preview.",
        ServiceDomain::Portfolio,
        LOCAL_SCOPE,
        PORTFOLIO_IMPORT_APPROVAL_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Portfolio.CommitStagedImport",
        "Commit one durably approved portfolio import through the portfolio authority.",
        ServiceDomain::Portfolio,
        LOCAL_SCOPE,
        PORTFOLIO_IMPORT_COMMIT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Portfolio.DiscardStagedImport",
        "Discard one unapproved server-owned portfolio import preview.",
        ServiceDomain::Portfolio,
        LOCAL_SCOPE,
        PORTFOLIO_IMPORT_DISCARD_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Portfolio.GetRecommendationSetup",
        "Return the explicit selected recommendation account and numeric allocation setup.",
        ServiceDomain::Portfolio,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Portfolio.PreviewRecommendationSetup",
        "Preview one explicit account selection and same-account numeric allocation profile.",
        ServiceDomain::Portfolio,
        LOCAL_SCOPE,
        RECOMMENDATION_SETUP_PREVIEW_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Portfolio.CommitRecommendationSetup",
        "Commit one exact recommendation-setup preview after local confirmation.",
        ServiceDomain::Portfolio,
        LOCAL_SCOPE,
        RECOMMENDATION_SETUP_COMMIT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Portfolio.ListAccounts",
        "List bounded portfolio accounts with their current immutable revisions.",
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        LIST_ACCOUNTS_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Portfolio.ListRevisions",
        "List bounded append-only revisions for one portfolio account.",
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        LIST_REVISIONS_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read_portfolio(
        "Portfolio.GetHoldings",
        "Return bounded current holdings under an exact revision.",
    ),
    read_portfolio(
        "Portfolio.GetTransactions",
        "Return bounded normalized portfolio transactions.",
    ),
    read_portfolio(
        "Portfolio.GetPerformance",
        "Return point-in-time portfolio performance.",
    ),
    read_portfolio(
        "Portfolio.GetExposure",
        "Return point-in-time instrument, sector, factor, and currency exposure.",
    ),
    read_portfolio(
        "Portfolio.GetRisk",
        "Return point-in-time portfolio risk and scenarios.",
    ),
    read(
        "Portfolio.GetAttribution",
        "Return source-mark change attribution between two immutable revisions.",
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        PORTFOLIO_ATTRIBUTION_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Portfolio.EvaluateScenario",
        "Evaluate one bounded exact scenario over pinned holdings.",
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        PORTFOLIO_SCENARIO_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Portfolio.EvaluateScenarioBatch",
        "Evaluate a bounded batch of exact scenarios over pinned holdings.",
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        PORTFOLIO_SCENARIO_BATCH_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Portfolio.ProposeRebalance",
        "Produce a non-executable rebalance proposal over one pinned revision.",
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        PORTFOLIO_REBALANCE_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Portfolio.EvaluateCandidateImpact",
        "Evaluate read-only exposure and scenario impact for one candidate against the exact server-selected account and market evidence; analysis only.",
        ServiceDomain::Portfolio,
        PORTFOLIO_CANDIDATE_SCOPE,
        PORTFOLIO_CANDIDATE_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read_analysis(
        "Analysis.GetReturns",
        "Return bounded price and total returns.",
    ),
    read(
        "Analysis.Lookup",
        "Search bounded installed-product indexes and report unavailable categories explicitly.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        ANALYSIS_LOOKUP_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Analysis.GetDecisionOverview",
        "Return a bounded current overview of local decision-support authorities.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read_analysis("Analysis.GetFactors", "Return bounded factor estimates."),
    read_analysis(
        "Analysis.GetValuation",
        "Return bounded analytical valuation measures.",
    ),
    read_analysis(
        "Analysis.GetScenarios",
        "Return bounded scenario and stress-analysis outputs.",
    ),
    mutation(
        "Analysis.StartScenarioBatch",
        "Start one durable deterministic scenario and stress-analysis batch.",
        ServiceDomain::Analysis,
        DATA_SCOPE,
        DATASET_ARGUMENT,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Analysis.GetFeatureDatasets",
        "Return registered feature contracts and immutable feature datasets.",
        ServiceDomain::Analysis,
        DATA_SCOPE,
        FEATURE_DATASET_ARGUMENTS,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Analysis.GetFeatureDatasetPreparationOptions",
        "Return bounded point-in-time feature-dataset build choices derived by the data owner.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Analysis.PreviewFeatureDatasetBuild",
        "Validate one guided dataset choice and retain its exact build behind a one-use receipt.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        FEATURE_DATASET_PREVIEW_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Analysis.StartPreparedFeatureDatasetBuild",
        "Consume one exact dataset-build receipt and start its durable job.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        FEATURE_DATASET_PREPARED_START_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Analysis.StartFeatureDatasetBuild",
        "Start one durable point-in-time feature and label dataset publication.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        DATASET_BUILD_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Analysis.GetBacktests",
        "Return governed backtest experiment metadata and results.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        RUN_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Analysis.GetBacktestPreparation",
        "Return bounded guided choices over current point-in-time feature datasets.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Analysis.PreviewBacktest",
        "Prepare one exact governed backtest registration behind a one-use receipt.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        BACKTEST_PREPARATION_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Analysis.StartPreparedBacktest",
        "Consume one exact backtest receipt and start its durable job.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        BACKTEST_PREPARED_START_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Analysis.StartBacktest",
        "Start one durable governed point-in-time backtest experiment.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        BACKTEST_RUN_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Analysis.RunBacktest",
        "Compatibility wait for a durable governed backtest under the caller deadline.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        BACKTEST_RUN_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    artifact_read(
        "Analysis.ReadArtifact",
        "Read one verified bounded chunk from an opaque local analytical artifact.",
        ARTIFACT_READ_ARGUMENTS,
    ),
    read(
        "Model.GetMetadata",
        "Return complete admitted model metadata and validation evidence.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Model.ListBundles",
        "List admitted immutable model bundle generations.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Model.Evaluate",
        "Evaluate an admitted local model and retain bounded evaluation evidence.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_EVALUATION_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Model.Predict",
        "Run bounded local inference; every failure produces no automated action.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_EVALUATION_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Model.StartTraining",
        "Start one durable governed training run from exact native-streamed inputs.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_TRAINING_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Model.GetForecastPreparation",
        "Return admitted models and data-owner-qualified forecast choices.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Model.PrepareForecast",
        "Prepare exact point-in-time forecast evidence behind a one-use receipt.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_FORECAST_PREPARATION_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Model.StartPreparedForecast",
        "Consume one exact forecast receipt and start its durable job.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_FORECAST_PREPARED_START_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Model.StartForecast",
        "Start one durable point-in-time forecast generation.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_FORECAST_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Model.GenerateForecast",
        "Generate and durably publish one point-in-time forecast vintage.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_FORECAST_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Model.GetForecast",
        "Return one immutable forecast vintage by exact identity.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_FORECAST_ID_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Model.SelectLatestValidForecast",
        "Select and fully revalidate the newest nonexpired forecast for one exact instrument and point in time.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_LATEST_VALID_FORECAST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Model.ListForecasts",
        "List bounded immutable forecast vintages.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Model.GetForecastOutcomes",
        "Return bounded realized outcomes for one forecast vintage.",
        ServiceDomain::Model,
        LOCAL_SCOPE,
        MODEL_FORECAST_ID_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    idempotent_mutation(
        "Decision.SaveScreen",
        "Save one validated point-in-time investment screen revision.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_SAVE_SCREEN_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    idempotent_mutation(
        "Decision.RunScreen",
        "Run one immutable point-in-time screen and retain ranked candidates.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_RUN_SCREEN_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Decision.ListScreens",
        "List bounded saved investment screens.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.ListScreenRuns",
        "List bounded immutable saved-screen runs using an exact continuation cursor.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_RUN_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.GetCandidates",
        "Return ranked candidates for one exact screen run.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_RUN_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.GetDossier",
        "Return one point-in-time investment dossier.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_DOSSIER_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.ListCandidateDossiers",
        "List bounded retained dossiers for one exact candidate using an exact continuation cursor.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_DOSSIER_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.GetDossierPreparation",
        "Return the exact retained evidence available for one candidate dossier.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_DOSSIER_PREPARATION_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.PrepareDossier",
        "Assemble one candidate dossier preview behind a bounded one-use receipt.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_DOSSIER_PREVIEW_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    idempotent_mutation(
        "Decision.CreateDossier",
        "Consume one exact preparation receipt and retain its immutable candidate dossier.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_DOSSIER_COMMIT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Decision.GetTargetPreparation",
        "Return bounded producer-owned evidence available for one target judgment.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_PREPARATION_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.PrepareTargetSet",
        "Validate one human target judgment and return a bounded one-use preview.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_PREVIEW_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    idempotent_mutation(
        "Decision.CreateTargetSet",
        "Consume one exact preparation receipt and create its immutable target series.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_COMMIT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Decision.GetTargetSet",
        "Return one exact governed investment target revision.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.ListTargetSets",
        "List bounded revisions for one governed investment target.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.ListTargetIndex",
        "List bounded current heads of governed investment-target series using an exact cursor.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_INDEX_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    idempotent_mutation(
        "Decision.ReviewTargetSet",
        "Record one immutable review of a governed investment target revision.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_REVIEW_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    idempotent_mutation(
        "Decision.ReevaluateTargetSet",
        "Consume one exact preparation receipt and append its governed successor revision.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_COMMIT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Decision.GetTargetSetStatus",
        "Return the current governed status of one exact target revision.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_TARGET_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.GetInvestmentAnalysis",
        "Return one exact retained generated, no-action, or unavailable investment analysis.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_INVESTMENT_ANALYSIS_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.ListInvestmentAnalyses",
        "List bounded retained investment-analysis locators in durable append order.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_INVESTMENT_ANALYSIS_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Decision.GetRecommendationTrackRecord",
        "Return sample- and coverage-governed realized outcomes grouped by one exact analytical profile, recommendation horizon, and action or no-action control.",
        ServiceDomain::Decision,
        LOCAL_SCOPE,
        DECISION_RECOMMENDATION_TRACK_RECORD_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Operations.ListBackups",
        "List bounded verified product backups and retention state.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_BACKUP_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Operations.GetBackup",
        "Return one exact verified product-backup manifest.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_BACKUP_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.StartBackup",
        "Start one durable complete product-backup job.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Operations.StartBackupVerification",
        "Start durable verification of one exact retained backup.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_BACKUP_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.PreviewBackupRetention",
        "Preview exact backups selected by the bounded retention policy.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_RETENTION_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.StartBackupRetention",
        "Start one exact preview-bound durable backup-retention job.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.PreviewRestore",
        "Preview restoring one exact backup into a new fenced workspace.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_BACKUP_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.StartRestore",
        "Start one exact preview-bound durable restore and workspace-switch job.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.ListWorkspaces",
        "List bounded local workspaces and the active generation.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_WORKSPACE_LIST_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Operations.PreviewWorkspaceSwitch",
        "Preview a fenced local workspace switch and its blockers.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_WORKSPACE_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.StartWorkspaceSwitch",
        "Start one exact preview-bound durable workspace switch.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.GetRuntimeStatus",
        "Return one complete path-free snapshot of installed service and workspace activity.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Operations.GetUpdateStatus",
        "Return current trusted update, known-good, and recovery state.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.CheckForUpdates",
        "Check trusted metadata and stage only an admitted update candidate.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.PreviewUpdate",
        "Preview activation of the currently staged trusted update.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.StartUpdate",
        "Start exact preview-bound update activation and health verification.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.PreviewProgramRollback",
        "Preview rollback to the verified known-good program generation.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.StartProgramRollback",
        "Start exact preview-bound rollback of program files without restoring data.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.QueryLogs",
        "Query bounded redacted local structured logs.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_LOG_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.ExportLogs",
        "Publish a bounded redacted log export to controlled artifacts.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_LOG_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.GetSettings",
        "Return typed effective settings with origin and restart impact.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Operations.PreviewSettingsChange",
        "Preview a typed settings change at one exact revision.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_SETTINGS_CHANGE_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.ApplySettingsChange",
        "Apply one exact preview-bound typed settings change.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Operations.PreviewSettingsRollback",
        "Preview restoring retained typed settings as a new monotonic revision.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_SETTINGS_ROLLBACK_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Operations.RollbackSettings",
        "Apply one exact preview-bound typed settings rollback.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        OPERATIONS_PREVIEW_REFERENCE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Setup.GetStatus",
        "Return the closed setup catalog and exact durable accepted plan.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Setup.PreviewPlan",
        "Preview one complete workspace-bound guided setup plan without changing authority.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        SETUP_PLAN_PREVIEW_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Setup.ApplyPlan",
        "Accept one exact one-use setup-plan preview after explicit local confirmation.",
        ServiceDomain::Operations,
        LOCAL_SCOPE,
        SETUP_PLAN_CONFIRMATION_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "FairValue.ListMeasurements",
        "List bounded immutable fair-value measurements.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "FairValue.GetClassification",
        "Return one measurement classification and ruleset identity.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        MEASUREMENT_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "FairValue.Explain",
        "Explain one evidence-bound hierarchy classification.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        MEASUREMENT_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "FairValue.GetEvidence",
        "Return bounded evidence linked to one measurement.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        MEASUREMENT_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "FairValue.GetApprovalStatus",
        "Return approval and revocation status for one measurement.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        FAIR_VALUE_STATUS_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    idempotent_mutation(
        "FairValue.Measure",
        "Create one immutable fair-value measurement from admitted evidence.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        FAIR_VALUE_MEASUREMENT_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    idempotent_mutation(
        "FairValue.Classify",
        "Classify one immutable measurement using the code-owned hierarchy ruleset.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        MEASUREMENT_ARGUMENT,
        ToolAuthorization::LocalConfirmation,
    ),
    idempotent_mutation(
        "FairValue.Approve",
        "Approve an eligible measurement through the controlled review workflow.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        FAIR_VALUE_APPROVAL_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "FairValue.ProposeOverride",
        "Propose an expiring Level 2 or Level 3 override with retained justification.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        FAIR_VALUE_OVERRIDE_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "FairValue.RevokeApproval",
        "Revoke one exact valuation approval with actor, time, and reason evidence.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        FAIR_VALUE_REVOCATION_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "FairValue.ListAuditEvents",
        "List a bounded hash-verified page of fair-value audit events.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        FAIR_VALUE_AUDIT_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    idempotent_mutation(
        "FairValue.ApproveMarketAccess",
        "Create or supersede one dual-approved account, venue, and instrument access assessment.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        FAIR_VALUE_MARKET_ACCESS_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "FairValue.GetMarketAccess",
        "Return one immutable dual-approved market-access assessment.",
        ServiceDomain::FairValue,
        LOCAL_SCOPE,
        MARKET_ACCESS_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Bot.GetStatus",
        "Return controlled paper-operation lifecycle and risk status.",
        ServiceDomain::Bot,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Bot.Start",
        "Start an explicitly configured local paper operation.",
        ServiceDomain::Bot,
        LOCAL_SCOPE,
        BOT_START_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    mutation(
        "Bot.Stop",
        "Stop the current local paper operation and durably reconcile it.",
        ServiceDomain::Bot,
        LOCAL_SCOPE,
        BOT_STOP_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
    read(
        "Execution.GetOrders",
        "Return bounded paper orders and state transitions.",
        ServiceDomain::Execution,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Execution.GetFills",
        "Return bounded paper fills.",
        ServiceDomain::Execution,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    read(
        "Execution.GetManualPaperTargets",
        "Return active governed investment targets compatible with the running manual paper routes.",
        ServiceDomain::Execution,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Execution.SubmitManualPaperDraft",
        "Submit one governed target-backed draft to a bounded manual paper route; central risk still decides whether any order may dispatch.",
        ServiceDomain::Execution,
        LOCAL_SCOPE,
        MANUAL_PAPER_DRAFT_ARGUMENTS,
        ToolAuthorization::RiskMediated,
    ),
    mutation(
        "Execution.Cancel",
        "Cancel one tracked paper order through dispatcher-owned authority.",
        ServiceDomain::Execution,
        LOCAL_SCOPE,
        ORDER_ARGUMENT,
        ToolAuthorization::RiskMediated,
    ),
    mutation(
        "Execution.Reconcile",
        "Reconcile paper orders, fills, balances, and positions through the dispatcher.",
        ServiceDomain::Execution,
        LOCAL_SCOPE,
        NO_ARGUMENTS,
        ToolAuthorization::RiskMediated,
    ),
    mutation(
        "Risk.TriggerKillSwitch",
        "Stop only the current local paper operation with an explicit reason.",
        ServiceDomain::Bot,
        LOCAL_SCOPE,
        BOT_STOP_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
    ),
];

/// Builds the complete deterministic application-service capability set.
///
/// # Errors
///
/// Returns a capability error if a code-owned descriptor violates the shared contract.
pub fn application_capabilities() -> Result<ServiceCapabilities, ServiceCapabilityError> {
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(OPERATION_SPECS.len())
        .map_err(|_| ServiceCapabilityError::TooManyTools {
            maximum: OPERATION_SPECS.len(),
        })?;
    for spec in OPERATION_SPECS {
        let schema = schema_for(*spec);
        let output_schema =
            output_data_schema(spec.name).ok_or(ServiceCapabilityError::InvalidOutputSchema)?;
        let effects = ToolEffects::try_new(
            matches!(spec.authorization, ToolAuthorization::ReadOnly),
            spec.destructive,
            spec.idempotent,
            spec.open_world,
        )?;
        let contract = ToolContract::new(
            spec.domain,
            spec.authorization,
            spec.scope,
            ToolResultPolicy::new(spec.source_evidence, spec.artifact),
        );
        let operation = *spec;
        descriptors.push(ToolDescriptor::try_new_with_output(
            spec.name,
            APPLICATION_CONTRACT_VERSION,
            spec.description,
            schema,
            output_schema,
            contract,
            effects,
            move |arguments: &Map<String, Value>| admit(operation, arguments),
        )?);
    }
    ServiceCapabilities::try_new(descriptors)
}

#[derive(Clone, Copy)]
struct OperationSpec {
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
    authorization: ToolAuthorization,
    source_evidence: SourceEvidencePolicy,
    artifact: ToolArtifactPolicy,
    destructive: bool,
    idempotent: bool,
    open_world: bool,
}

const fn read(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
    source_evidence: SourceEvidencePolicy,
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain,
        scope,
        arguments,
        authorization: ToolAuthorization::ReadOnly,
        source_evidence,
        artifact: ToolArtifactPolicy::OpaqueOnOverflow,
        destructive: false,
        idempotent: true,
        open_world: false,
    }
}

const fn mutation(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
    authorization: ToolAuthorization,
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain,
        scope,
        arguments,
        authorization,
        source_evidence: SourceEvidencePolicy::NotApplicable,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: true,
        idempotent: false,
        open_world: false,
    }
}

const fn source_evidence_mutation(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
    authorization: ToolAuthorization,
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain,
        scope,
        arguments,
        authorization,
        source_evidence: SourceEvidencePolicy::Required,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: true,
        idempotent: false,
        open_world: false,
    }
}

const fn idempotent_mutation(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
    authorization: ToolAuthorization,
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain,
        scope,
        arguments,
        authorization,
        source_evidence: SourceEvidencePolicy::NotApplicable,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: true,
        idempotent: true,
        open_world: false,
    }
}

const fn source_discovery(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain,
        scope,
        arguments,
        authorization: ToolAuthorization::LocalConfirmation,
        source_evidence: SourceEvidencePolicy::Required,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: false,
        idempotent: false,
        open_world: true,
    }
}

const fn source_listing(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain,
        scope,
        arguments,
        authorization: ToolAuthorization::ReadOnly,
        source_evidence: SourceEvidencePolicy::Required,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: false,
        idempotent: true,
        open_world: true,
    }
}

const fn source_ingest(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
    arguments: &'static [ArgumentSpec],
    authorization: ToolAuthorization,
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain,
        scope,
        arguments,
        authorization,
        source_evidence: SourceEvidencePolicy::Required,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: false,
        idempotent: true,
        open_world: true,
    }
}

const fn read_data(name: &'static str, description: &'static str) -> OperationSpec {
    read(
        name,
        description,
        ServiceDomain::Market,
        DATA_SCOPE,
        OPTIONAL_DATASET_ARGUMENT,
        SourceEvidencePolicy::Required,
    )
}

const fn read_observations(
    name: &'static str,
    description: &'static str,
    domain: ServiceDomain,
) -> OperationSpec {
    read(
        name,
        description,
        domain,
        DATA_SCOPE,
        DATASET_ARGUMENT,
        SourceEvidencePolicy::Required,
    )
}

const fn read_portfolio(name: &'static str, description: &'static str) -> OperationSpec {
    read(
        name,
        description,
        ServiceDomain::Portfolio,
        PORTFOLIO_SCOPE,
        ACCOUNT_ARGUMENT,
        SourceEvidencePolicy::Required,
    )
}

const fn read_analysis(name: &'static str, description: &'static str) -> OperationSpec {
    read(
        name,
        description,
        ServiceDomain::Analysis,
        DATA_SCOPE,
        DATASET_ARGUMENT,
        SourceEvidencePolicy::Required,
    )
}

const fn artifact_read(
    name: &'static str,
    description: &'static str,
    arguments: &'static [ArgumentSpec],
) -> OperationSpec {
    OperationSpec {
        name,
        description,
        domain: ServiceDomain::Analysis,
        scope: LOCAL_SCOPE,
        arguments,
        authorization: ToolAuthorization::ReadOnly,
        source_evidence: SourceEvidencePolicy::NotApplicable,
        artifact: ToolArtifactPolicy::InlineOnly,
        destructive: false,
        idempotent: true,
        open_world: false,
    }
}

#[derive(Clone, Copy)]
struct ArgumentSpec {
    name: &'static str,
    required: bool,
    kind: ArgumentKind,
}

impl ArgumentSpec {
    const fn required(name: &'static str, kind: ArgumentKind) -> Self {
        Self {
            name,
            required: true,
            kind,
        }
    }

    const fn optional(name: &'static str, kind: ArgumentKind) -> Self {
        Self {
            name,
            required: false,
            kind,
        }
    }
}

#[derive(Clone, Copy)]
enum ArgumentKind {
    Identifier,
    Uuid,
    Sha256,
    Text,
    Decimal,
    PositiveLotQuantity,
    Object,
    Array,
    Timestamp,
    FairValueMeasurement,
    ForecastRequest,
    PortfolioImportInterpretations,
    RecommendationAllocationProfile,
    ResearchFileMapping,
    SettingsChanges,
    Enumeration(&'static [&'static str]),
    Signed { minimum: i64, maximum: i64 },
    Unsigned { minimum: u64, maximum: u64 },
}

fn schema_for(spec: OperationSpec) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    insert_scope_schema(&mut properties, &mut required, spec.scope);
    for argument in spec.arguments {
        properties.insert(argument.name.to_owned(), argument_schema(argument.kind));
        if argument.required {
            required.push(Value::String(argument.name.to_owned()));
        }
    }
    if !matches!(spec.authorization, ToolAuthorization::ReadOnly) {
        properties.insert(
            "confirm".to_owned(),
            json!({"type": "boolean", "const": true}),
        );
        required.push(Value::String("confirm".to_owned()));
    }
    let mut schema = Map::new();
    schema.insert("type".to_owned(), Value::String("object".to_owned()));
    schema.insert("properties".to_owned(), Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".to_owned(), Value::Array(required));
    }
    schema.insert("additionalProperties".to_owned(), Value::Bool(false));
    Value::Object(schema)
}

fn insert_scope_schema(
    properties: &mut Map<String, Value>,
    required: &mut Vec<Value>,
    scope: ToolScope,
) {
    insert_scoped_property(
        properties,
        required,
        "instrumentIds",
        scope.instruments(),
        json!({
            "type": "array",
            "minItems": 1,
            "maxItems": MAXIMUM_INSTRUMENTS,
            "uniqueItems": true,
            "items": {"type": "string", "format": "uuid"}
        }),
    );
    insert_scoped_property(
        properties,
        required,
        "timeRange",
        scope.time_range(),
        json!({
            "type": "object",
            "properties": {
                "start": {"type": "string", "format": "date-time"},
                "end": {"type": "string", "format": "date-time"}
            },
            "required": ["start", "end"],
            "additionalProperties": false
        }),
    );
    insert_scoped_property(
        properties,
        required,
        "resultLimits",
        scope.result_limits(),
        json!({
            "type": "object",
            "properties": {
                "maximumItems": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAXIMUM_RESULT_ITEMS
                },
                "maximumBytes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAXIMUM_RESULT_BYTES
                }
            },
            "required": ["maximumItems", "maximumBytes"],
            "additionalProperties": false
        }),
    );
    insert_scoped_property(
        properties,
        required,
        "sourceCoverage",
        scope.source_coverage(),
        json!({
            "type": "array",
            "minItems": 1,
            "maxItems": MAXIMUM_SOURCES,
            "uniqueItems": true,
            "items": {"type": "string", "minLength": 1, "maxLength": 256}
        }),
    );
}

fn insert_scoped_property(
    properties: &mut Map<String, Value>,
    required: &mut Vec<Value>,
    name: &str,
    requirement: ScopeRequirement,
    schema: Value,
) {
    if matches!(requirement, ScopeRequirement::NotApplicable) {
        return;
    }
    properties.insert(name.to_owned(), schema);
    if matches!(requirement, ScopeRequirement::Required) {
        required.push(Value::String(name.to_owned()));
    }
}

fn argument_schema(kind: ArgumentKind) -> Value {
    match kind {
        ArgumentKind::Identifier => json!({
            "type": "string",
            "minLength": 1,
            "maxLength": MAXIMUM_IDENTIFIER_BYTES
        }),
        ArgumentKind::Uuid => json!({
            "type": "string",
            "format": "uuid"
        }),
        ArgumentKind::Sha256 => json!({
            "type": "string",
            "minLength": 64,
            "maxLength": 64,
            "pattern": "^[0-9a-f]{64}$"
        }),
        ArgumentKind::Text => json!({
            "type": "string",
            "minLength": 1,
            "maxLength": MAXIMUM_TEXT_BYTES
        }),
        ArgumentKind::Decimal => json!({
            "type": "string",
            "minLength": 1,
            "maxLength": 128
        }),
        ArgumentKind::PositiveLotQuantity => json!({
            "type": "string",
            "minLength": 1,
            "maxLength": 19,
            "pattern": "^[1-9][0-9]{0,18}$"
        }),
        ArgumentKind::Object => json!({"type": "object", "minProperties": 1}),
        ArgumentKind::Array => json!({"type": "array", "minItems": 1}),
        ArgumentKind::Timestamp => json!({"type": "string", "format": "date-time"}),
        ArgumentKind::FairValueMeasurement => fair_value_measurement_schema(),
        ArgumentKind::ForecastRequest => forecast_request_schema(),
        ArgumentKind::PortfolioImportInterpretations => portfolio_import_interpretations_schema(),
        ArgumentKind::RecommendationAllocationProfile => recommendation_allocation_profile_schema(),
        ArgumentKind::ResearchFileMapping => research_file_mapping_schema(),
        ArgumentKind::SettingsChanges => settings_changes_schema(),
        ArgumentKind::Enumeration(values) => json!({"type": "string", "enum": values}),
        ArgumentKind::Signed { minimum, maximum } => json!({
            "type": "integer",
            "minimum": minimum,
            "maximum": maximum
        }),
        ArgumentKind::Unsigned { minimum, maximum } => json!({
            "type": "integer",
            "minimum": minimum,
            "maximum": maximum
        }),
    }
}

fn admit(spec: OperationSpec, arguments: &Map<String, Value>) -> Result<(), ToolInputError> {
    let mut allowed = HashSet::new();
    allowed
        .try_reserve(5_usize.saturating_add(spec.arguments.len()))
        .map_err(|_| ToolInputError::Invalid)?;
    admit_scope(arguments, spec.scope, &mut allowed)?;
    for argument in spec.arguments {
        allowed.insert(argument.name);
        match arguments.get(argument.name) {
            Some(value) => admit_argument(value, argument.kind)?,
            None if argument.required => return Err(ToolInputError::Invalid),
            None => {}
        }
    }
    if !matches!(spec.authorization, ToolAuthorization::ReadOnly) {
        allowed.insert("confirm");
        if arguments.get("confirm") != Some(&Value::Bool(true)) {
            return Err(ToolInputError::Invalid);
        }
    }
    if arguments.keys().any(|key| !allowed.contains(key.as_str())) {
        return Err(ToolInputError::Invalid);
    }
    Ok(())
}

fn admit_scope(
    arguments: &Map<String, Value>,
    scope: ToolScope,
    allowed: &mut HashSet<&'static str>,
) -> Result<(), ToolInputError> {
    admit_scoped(
        arguments,
        allowed,
        "instrumentIds",
        scope.instruments(),
        admit_instruments,
    )?;
    admit_scoped(
        arguments,
        allowed,
        "timeRange",
        scope.time_range(),
        admit_time_range,
    )?;
    admit_scoped(
        arguments,
        allowed,
        "resultLimits",
        scope.result_limits(),
        admit_result_limits,
    )?;
    admit_scoped(
        arguments,
        allowed,
        "sourceCoverage",
        scope.source_coverage(),
        admit_sources,
    )
}

fn admit_scoped(
    arguments: &Map<String, Value>,
    allowed: &mut HashSet<&'static str>,
    name: &'static str,
    requirement: ScopeRequirement,
    validator: fn(&Value) -> Result<(), ToolInputError>,
) -> Result<(), ToolInputError> {
    match requirement {
        ScopeRequirement::Required => {
            allowed.insert(name);
            arguments
                .get(name)
                .ok_or(ToolInputError::Invalid)
                .and_then(validator)
        }
        ScopeRequirement::Optional => {
            allowed.insert(name);
            arguments.get(name).map_or(Ok(()), validator)
        }
        ScopeRequirement::NotApplicable => {
            if arguments.contains_key(name) {
                Err(ToolInputError::Invalid)
            } else {
                Ok(())
            }
        }
    }
}

fn admit_instruments(value: &Value) -> Result<(), ToolInputError> {
    let values = value.as_array().ok_or(ToolInputError::Invalid)?;
    if values.is_empty() || values.len() > MAXIMUM_INSTRUMENTS {
        return Err(ToolInputError::Invalid);
    }
    let mut unique = HashSet::new();
    for value in values {
        let instrument = value.as_str().ok_or(ToolInputError::Invalid)?;
        let parsed = Uuid::parse_str(instrument).map_err(|_| ToolInputError::Invalid)?;
        if !unique.insert(parsed) {
            return Err(ToolInputError::Invalid);
        }
    }
    Ok(())
}

fn admit_time_range(value: &Value) -> Result<(), ToolInputError> {
    let range = value.as_object().ok_or(ToolInputError::Invalid)?;
    if range.len() != 2 || range.keys().any(|key| key != "start" && key != "end") {
        return Err(ToolInputError::Invalid);
    }
    let start = range
        .get("start")
        .and_then(Value::as_str)
        .ok_or(ToolInputError::Invalid)?;
    let end = range
        .get("end")
        .and_then(Value::as_str)
        .ok_or(ToolInputError::Invalid)?;
    let start = DateTime::parse_from_rfc3339(start).map_err(|_| ToolInputError::Invalid)?;
    let end = DateTime::parse_from_rfc3339(end).map_err(|_| ToolInputError::Invalid)?;
    if start > end {
        return Err(ToolInputError::Invalid);
    }
    Ok(())
}

fn admit_result_limits(value: &Value) -> Result<(), ToolInputError> {
    let limits = value.as_object().ok_or(ToolInputError::Invalid)?;
    if limits.len() != 2
        || limits
            .keys()
            .any(|key| key != "maximumItems" && key != "maximumBytes")
    {
        return Err(ToolInputError::Invalid);
    }
    let items = limits
        .get("maximumItems")
        .and_then(Value::as_u64)
        .ok_or(ToolInputError::Invalid)?;
    let bytes = limits
        .get("maximumBytes")
        .and_then(Value::as_u64)
        .ok_or(ToolInputError::Invalid)?;
    if items == 0 || items > MAXIMUM_RESULT_ITEMS || bytes == 0 || bytes > MAXIMUM_RESULT_BYTES {
        return Err(ToolInputError::Invalid);
    }
    Ok(())
}

fn admit_sources(value: &Value) -> Result<(), ToolInputError> {
    let values = value.as_array().ok_or(ToolInputError::Invalid)?;
    if values.is_empty() || values.len() > MAXIMUM_SOURCES {
        return Err(ToolInputError::Invalid);
    }
    let mut unique = HashSet::new();
    for value in values {
        let source = value.as_str().ok_or(ToolInputError::Invalid)?;
        if !valid_identifier(source) || !unique.insert(source) {
            return Err(ToolInputError::Invalid);
        }
    }
    Ok(())
}

fn admit_argument(value: &Value, kind: ArgumentKind) -> Result<(), ToolInputError> {
    match kind {
        ArgumentKind::Identifier => value
            .as_str()
            .filter(|value| valid_identifier(value))
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Uuid => value
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Sha256 => value
            .as_str()
            .filter(|value| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            })
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Text => value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= MAXIMUM_TEXT_BYTES)
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Decimal => value
            .as_str()
            .filter(|value| value.len() <= 128)
            .and_then(|value| value.parse::<rust_decimal::Decimal>().ok())
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::PositiveLotQuantity => value
            .as_str()
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= 19
                    && value.bytes().all(|byte| byte.is_ascii_digit())
                    && !value.starts_with('0')
            })
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Object => value
            .as_object()
            .filter(|value| !value.is_empty())
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Array => value
            .as_array()
            .filter(|values| !values.is_empty())
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Timestamp => admit_timestamp(value),
        ArgumentKind::FairValueMeasurement => admit_fair_value_measurement(value),
        ArgumentKind::ForecastRequest => admit_forecast_request(value),
        ArgumentKind::PortfolioImportInterpretations => {
            admit_portfolio_import_interpretations(value)
        }
        ArgumentKind::RecommendationAllocationProfile => {
            admit_recommendation_allocation_profile(value)
        }
        ArgumentKind::ResearchFileMapping => admit_research_file_mapping(value),
        ArgumentKind::SettingsChanges => admit_settings_changes(value),
        ArgumentKind::Enumeration(values) => value
            .as_str()
            .filter(|value| values.contains(value))
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Signed { minimum, maximum } => value
            .as_i64()
            .filter(|value| (*value >= minimum) && (*value <= maximum))
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Unsigned { minimum, maximum } => value
            .as_u64()
            .filter(|value| (*value >= minimum) && (*value <= maximum))
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
    }
}

fn recommendation_allocation_profile_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "preferredPositionWeightLowerBps": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10_000,
            },
            "preferredPositionWeightUpperBps": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10_000,
            },
            "minimumCashReserve": {
                "type": "object",
                "properties": {
                    "amount": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 128,
                    },
                    "currency": {
                        "type": "string",
                        "minLength": 3,
                        "maxLength": 3,
                        "pattern": "^[A-Z]{3}$",
                    },
                },
                "required": ["amount", "currency"],
                "additionalProperties": false,
            },
            "maximumDownsideLossBpsOfMarkedEquity": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10_000,
            },
            "availableInvestmentHorizonDays": {
                "type": "integer",
                "minimum": 1,
                "maximum": 3_650,
            },
        },
        "required": [
            "preferredPositionWeightLowerBps",
            "preferredPositionWeightUpperBps",
            "minimumCashReserve",
            "maximumDownsideLossBpsOfMarkedEquity",
            "availableInvestmentHorizonDays",
        ],
        "additionalProperties": false,
    })
}

fn admit_recommendation_allocation_profile(value: &Value) -> Result<(), ToolInputError> {
    const ALLOWED: [&str; 5] = [
        "preferredPositionWeightLowerBps",
        "preferredPositionWeightUpperBps",
        "minimumCashReserve",
        "maximumDownsideLossBpsOfMarkedEquity",
        "availableInvestmentHorizonDays",
    ];
    let profile = value.as_object().ok_or(ToolInputError::Invalid)?;
    if profile.len() != ALLOWED.len()
        || profile.keys().any(|name| !ALLOWED.contains(&name.as_str()))
    {
        return Err(ToolInputError::Invalid);
    }
    let lower = bounded_u64(profile, "preferredPositionWeightLowerBps", 1, 10_000)?;
    let upper = bounded_u64(profile, "preferredPositionWeightUpperBps", 1, 10_000)?;
    if lower > upper {
        return Err(ToolInputError::Invalid);
    }
    bounded_u64(profile, "maximumDownsideLossBpsOfMarkedEquity", 1, 10_000)?;
    bounded_u64(profile, "availableInvestmentHorizonDays", 1, 3_650)?;
    let reserve = profile
        .get("minimumCashReserve")
        .and_then(Value::as_object)
        .ok_or(ToolInputError::Invalid)?;
    if reserve.len() != 2
        || reserve
            .keys()
            .any(|name| name != "amount" && name != "currency")
    {
        return Err(ToolInputError::Invalid);
    }
    let amount = reserve
        .get("amount")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .and_then(|value| value.parse::<rust_decimal::Decimal>().ok())
        .filter(|value| !value.is_sign_negative())
        .ok_or(ToolInputError::Invalid)?;
    let _ = amount;
    reserve
        .get("currency")
        .and_then(Value::as_str)
        .filter(|value| value.len() == 3 && value.bytes().all(|byte| byte.is_ascii_uppercase()))
        .map(|_| ())
        .ok_or(ToolInputError::Invalid)
}

fn bounded_u64(
    object: &Map<String, Value>,
    name: &str,
    minimum: u64,
    maximum: u64,
) -> Result<u64, ToolInputError> {
    object
        .get(name)
        .and_then(Value::as_u64)
        .filter(|value| *value >= minimum && *value <= maximum)
        .ok_or(ToolInputError::Invalid)
}

fn portfolio_import_interpretations_schema() -> Value {
    json!({
        "type": "array",
        "minItems": 1,
        "maxItems": 100_000,
        "items": {
            "type": "object",
            "properties": {
                "recordId": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAXIMUM_IDENTIFIER_BYTES,
                },
                "interpretation": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                },
                "rationale": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAXIMUM_TEXT_BYTES,
                },
                "selectedLotIndexes": {
                    "type": "array",
                    "maxItems": 100_000,
                    "uniqueItems": true,
                    "items": {"type": "integer", "minimum": 0, "maximum": 99_999},
                },
            },
            "required": ["recordId", "interpretation", "rationale"],
            "additionalProperties": false,
        },
    })
}

fn research_file_mapping_schema() -> Value {
    let source_field = || {
        json!({
            "type": "string",
            "minLength": 1,
            "maxLength": MAXIMUM_IDENTIFIER_BYTES,
        })
    };
    let identifier = || {
        json!({
            "type": "string",
            "minLength": 1,
            "maxLength": MAXIMUM_IDENTIFIER_BYTES,
            "pattern": "^[A-Za-z0-9_.:/-]+$",
        })
    };
    json!({
        "type": "object",
        "properties": {
            "dataset": identifier(),
            "identityField": source_field(),
            "fields": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": {
                    "type": "object",
                    "properties": {
                        "source": source_field(),
                        "field": identifier(),
                        "decimalScale": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 28,
                        },
                        "unit": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 64,
                        },
                    },
                    "required": ["source", "field", "decimalScale"],
                    "additionalProperties": false,
                },
            },
            "effectiveAt": {"type": "string", "format": "date-time"},
            "publishedAt": {"type": "string", "format": "date-time"},
            "effectiveField": source_field(),
            "publishedField": source_field(),
            "availableField": source_field(),
            "revisionField": source_field(),
            "revisionNumberField": source_field(),
            "supersededField": source_field(),
            "instrumentId": {"type": "string", "format": "uuid"},
            "universe": identifier(),
        },
        "required": ["dataset", "identityField", "fields", "effectiveAt"],
        "additionalProperties": false,
    })
}

fn admit_research_file_mapping(value: &Value) -> Result<(), ToolInputError> {
    const OPTIONAL_SOURCE_FIELDS: [&str; 6] = [
        "effectiveField",
        "publishedField",
        "availableField",
        "revisionField",
        "revisionNumberField",
        "supersededField",
    ];
    const ALLOWED: [&str; 13] = [
        "dataset",
        "identityField",
        "fields",
        "effectiveAt",
        "publishedAt",
        "effectiveField",
        "publishedField",
        "availableField",
        "revisionField",
        "revisionNumberField",
        "supersededField",
        "instrumentId",
        "universe",
    ];
    let mapping = value.as_object().ok_or(ToolInputError::Invalid)?;
    if mapping.len() < 4
        || mapping.len() > ALLOWED.len()
        || mapping.keys().any(|key| !ALLOWED.contains(&key.as_str()))
        || !mapping
            .get("dataset")
            .and_then(Value::as_str)
            .is_some_and(valid_identifier)
        || !mapping
            .get("identityField")
            .and_then(Value::as_str)
            .is_some_and(valid_source_field)
    {
        return Err(ToolInputError::Invalid);
    }
    admit_timestamp(mapping.get("effectiveAt").ok_or(ToolInputError::Invalid)?)?;
    if let Some(published) = mapping.get("publishedAt") {
        admit_timestamp(published)?;
    }
    for name in OPTIONAL_SOURCE_FIELDS {
        if let Some(value) = mapping.get(name)
            && !value.as_str().is_some_and(valid_source_field)
        {
            return Err(ToolInputError::Invalid);
        }
    }
    if mapping
        .get("universe")
        .is_some_and(|value| !value.as_str().is_some_and(valid_identifier))
    {
        return Err(ToolInputError::Invalid);
    }
    let instrument = mapping.get("instrumentId");
    if instrument
        .and_then(Value::as_str)
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| ToolInputError::Invalid)?
        .is_none()
        && mapping.contains_key("universe")
    {
        return Err(ToolInputError::Invalid);
    }
    let fields = mapping
        .get("fields")
        .and_then(Value::as_array)
        .filter(|fields| !fields.is_empty() && fields.len() <= 64)
        .ok_or(ToolInputError::Invalid)?;
    let mut source_fields = HashSet::new();
    let mut output_fields = HashSet::new();
    for field in fields {
        let field = field.as_object().ok_or(ToolInputError::Invalid)?;
        if field.len() < 3
            || field.len() > 4
            || field
                .keys()
                .any(|key| !matches!(key.as_str(), "source" | "field" | "decimalScale" | "unit"))
        {
            return Err(ToolInputError::Invalid);
        }
        let source = field
            .get("source")
            .and_then(Value::as_str)
            .filter(|value| valid_source_field(value))
            .ok_or(ToolInputError::Invalid)?;
        let output = field
            .get("field")
            .and_then(Value::as_str)
            .filter(|value| valid_identifier(value))
            .ok_or(ToolInputError::Invalid)?;
        field
            .get("decimalScale")
            .and_then(Value::as_u64)
            .filter(|value| *value <= 28)
            .ok_or(ToolInputError::Invalid)?;
        if !source_fields.insert(source)
            || !output_fields.insert(output)
            || field.get("unit").is_some_and(|unit| {
                !unit
                    .as_str()
                    .is_some_and(|value| !value.trim().is_empty() && value.len() <= 64)
            })
        {
            return Err(ToolInputError::Invalid);
        }
    }
    Ok(())
}

fn admit_portfolio_import_interpretations(value: &Value) -> Result<(), ToolInputError> {
    let selections = value
        .as_array()
        .filter(|selections| !selections.is_empty() && selections.len() <= 100_000)
        .ok_or(ToolInputError::Invalid)?;
    for selection in selections {
        let selection = selection.as_object().ok_or(ToolInputError::Invalid)?;
        if selection.len() < 3
            || selection.len() > 4
            || selection.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "recordId" | "interpretation" | "rationale" | "selectedLotIndexes"
                )
            })
        {
            return Err(ToolInputError::Invalid);
        }
        let text_is_valid = |name: &str, maximum: usize| {
            selection
                .get(name)
                .and_then(Value::as_str)
                .map(str::trim)
                .is_some_and(|value| !value.is_empty() && value.len() <= maximum)
        };
        if !text_is_valid("recordId", MAXIMUM_IDENTIFIER_BYTES)
            || !text_is_valid("interpretation", 128)
            || !text_is_valid("rationale", MAXIMUM_TEXT_BYTES)
        {
            return Err(ToolInputError::Invalid);
        }
        if let Some(indexes) = selection.get("selectedLotIndexes") {
            let indexes = indexes
                .as_array()
                .filter(|indexes| indexes.len() <= 100_000)
                .ok_or(ToolInputError::Invalid)?;
            let mut unique = HashSet::new();
            unique
                .try_reserve(indexes.len())
                .map_err(|_| ToolInputError::Invalid)?;
            if indexes.iter().any(|index| {
                index
                    .as_u64()
                    .filter(|index| *index <= 99_999)
                    .is_none_or(|index| !unique.insert(index))
            }) {
                return Err(ToolInputError::Invalid);
            }
        }
    }
    Ok(())
}

fn settings_changes_schema() -> Value {
    let variant = |kind: &'static str, value: Value| {
        json!({
            "type": "object",
            "properties": {
                "kind": {"const": kind},
                "value": value,
            },
            "required": ["kind", "value"],
            "additionalProperties": false,
        })
    };
    json!({
        "type": "array",
        "minItems": 1,
        "maxItems": 16,
        "items": {
            "oneOf": [
                variant("log_retention_days", json!({"type": "integer", "minimum": 1, "maximum": 365})),
                variant("log_minimum_severity", json!({"type": "string", "enum": ["trace", "debug", "info", "warn", "error"]})),
                variant("update_channel", json!({"type": "string", "enum": ["stable", "preview"]})),
                variant("automatic_update_checks", json!({"type": "boolean"})),
                variant("storage_soft_limit_bytes", json!({"type": "integer", "minimum": 1_073_741_824_u64, "maximum": 17_592_186_044_416_u64})),
                variant("default_query_row_limit", json!({"type": "integer", "minimum": 100, "maximum": 1_000_000})),
                variant("maximum_concurrent_jobs", json!({"type": "integer", "minimum": 1, "maximum": 64})),
                variant("market_freshness_millis", json!({"type": "integer", "minimum": 250, "maximum": 600_000})),
                variant("backup_retention_count", json!({"type": "integer", "minimum": 1, "maximum": 64})),
            ]
        }
    })
}

fn admit_settings_changes(value: &Value) -> Result<(), ToolInputError> {
    let changes = value.as_array().ok_or(ToolInputError::Invalid)?;
    if changes.is_empty() || changes.len() > 16 {
        return Err(ToolInputError::Invalid);
    }
    let mut kinds = HashSet::new();
    for change in changes {
        let change = change.as_object().ok_or(ToolInputError::Invalid)?;
        if change.len() != 2 || !change.contains_key("value") {
            return Err(ToolInputError::Invalid);
        }
        let kind = change
            .get("kind")
            .and_then(Value::as_str)
            .filter(|kind| kinds.insert(*kind))
            .ok_or(ToolInputError::Invalid)?;
        let setting = change.get("value").ok_or(ToolInputError::Invalid)?;
        let valid = match kind {
            "log_retention_days" => unsigned_in(setting, 1, 365),
            "log_minimum_severity" => {
                string_in(setting, &["trace", "debug", "info", "warn", "error"])
            }
            "update_channel" => string_in(setting, &["stable", "preview"]),
            "automatic_update_checks" => setting.is_boolean(),
            "storage_soft_limit_bytes" => unsigned_in(setting, 1_073_741_824, 17_592_186_044_416),
            "default_query_row_limit" => unsigned_in(setting, 100, 1_000_000),
            "maximum_concurrent_jobs" => unsigned_in(setting, 1, 64),
            "market_freshness_millis" => unsigned_in(setting, 250, 600_000),
            "backup_retention_count" => unsigned_in(setting, 1, 64),
            _ => false,
        };
        if !valid {
            return Err(ToolInputError::Invalid);
        }
    }
    Ok(())
}

fn unsigned_in(value: &Value, minimum: u64, maximum: u64) -> bool {
    value
        .as_u64()
        .is_some_and(|value| (minimum..=maximum).contains(&value))
}

fn string_in(value: &Value, allowed: &[&str]) -> bool {
    value.as_str().is_some_and(|value| allowed.contains(&value))
}

fn forecast_request_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "instrumentId": {"type": "string", "format": "uuid"},
            "bundleId": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAXIMUM_IDENTIFIER_BYTES
            },
            "bundleVersion": {"type": "integer", "minimum": 1},
            "observedThroughUnixNanos": {"type": "integer"},
            "availableAtUnixNanos": {"type": "integer"},
            "horizonPoints": {
                "type": "integer",
                "minimum": 1,
                "maximum": market_squawk_modeling::MAX_FORECAST_POINTS
            },
            "horizonStepNanos": {"type": "integer", "minimum": 1},
            "decimalScale": {
                "type": "integer",
                "minimum": 0,
                "maximum": market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE
            },
            "validityNanos": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAXIMUM_FORECAST_VALIDITY_NANOS
            },
            "observedHistory": {
                "type": "array",
                "minItems": 1,
                "maxItems": market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS,
                "items": {
                    "type": "object",
                    "properties": {
                        "observedAtUnixNanos": {"type": "integer"},
                        "availableAtUnixNanos": {"type": "integer"},
                        "mantissa": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 40,
                            "pattern": "^-?[0-9]+$"
                        },
                        "sourcePitHash": {
                            "type": "string",
                            "minLength": 64,
                            "maxLength": 64,
                            "pattern": "^[0-9a-f]{64}$"
                        },
                        "quality": {
                            "type": "string",
                            "enum": [
                                "direct_verified",
                                "direct_unverified",
                                "official_delayed",
                                "aggregated",
                                "indicative",
                                "estimated",
                                "stale",
                                "quarantined"
                            ]
                        }
                    },
                    "required": [
                        "observedAtUnixNanos",
                        "availableAtUnixNanos",
                        "mantissa",
                        "sourcePitHash",
                        "quality"
                    ],
                    "additionalProperties": false
                }
            },
            "inputs": {
                "type": "array",
                "minItems": 1,
                "maxItems": market_squawk_modeling::MAX_FORECAST_POINTS,
                "items": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": market_squawk_modeling::MAX_MODEL_FEATURES,
                    "items": {"type": "number"}
                }
            }
        },
        "required": [
            "instrumentId",
            "bundleId",
            "bundleVersion",
            "observedThroughUnixNanos",
            "availableAtUnixNanos",
            "horizonPoints",
            "horizonStepNanos",
            "decimalScale",
            "validityNanos",
            "observedHistory",
            "inputs"
        ],
        "additionalProperties": false
    })
}

fn admit_forecast_request(value: &Value) -> Result<(), ToolInputError> {
    const FIELDS: [&str; 11] = [
        "instrumentId",
        "bundleId",
        "bundleVersion",
        "observedThroughUnixNanos",
        "availableAtUnixNanos",
        "horizonPoints",
        "horizonStepNanos",
        "decimalScale",
        "validityNanos",
        "observedHistory",
        "inputs",
    ];
    const OBSERVED_FIELDS: [&str; 5] = [
        "observedAtUnixNanos",
        "availableAtUnixNanos",
        "mantissa",
        "sourcePitHash",
        "quality",
    ];
    const OBSERVED_QUALITIES: [&str; 8] = [
        "direct_verified",
        "direct_unverified",
        "official_delayed",
        "aggregated",
        "indicative",
        "estimated",
        "stale",
        "quarantined",
    ];

    let request = value.as_object().ok_or(ToolInputError::Invalid)?;
    if request.len() != FIELDS.len()
        || request.keys().any(|name| !FIELDS.contains(&name.as_str()))
        || request
            .get("instrumentId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .is_none()
        || request
            .get("bundleId")
            .and_then(Value::as_str)
            .is_none_or(|value| !valid_identifier(value))
        || request
            .get("bundleVersion")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        || request
            .get("observedThroughUnixNanos")
            .and_then(Value::as_i64)
            .is_none()
        || request
            .get("availableAtUnixNanos")
            .and_then(Value::as_i64)
            .is_none()
    {
        return Err(ToolInputError::Invalid);
    }
    let horizon_points = request
        .get("horizonPoints")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| (1..=market_squawk_modeling::MAX_FORECAST_POINTS).contains(value))
        .ok_or(ToolInputError::Invalid)?;
    if request
        .get("horizonStepNanos")
        .and_then(Value::as_u64)
        .is_none_or(|value| value == 0)
        || request
            .get("decimalScale")
            .and_then(Value::as_u64)
            .is_none_or(|value| {
                value > u64::from(market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE)
            })
        || request
            .get("validityNanos")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0 || value > MAXIMUM_FORECAST_VALIDITY_NANOS)
    {
        return Err(ToolInputError::Invalid);
    }
    let observed = request
        .get("observedHistory")
        .and_then(Value::as_array)
        .filter(|values| {
            !values.is_empty()
                && values.len() <= market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS
        })
        .ok_or(ToolInputError::Invalid)?;
    for point in observed {
        let point = point.as_object().ok_or(ToolInputError::Invalid)?;
        let hash = point.get("sourcePitHash").and_then(Value::as_str);
        if point.len() != OBSERVED_FIELDS.len()
            || point
                .keys()
                .any(|name| !OBSERVED_FIELDS.contains(&name.as_str()))
            || point
                .get("observedAtUnixNanos")
                .and_then(Value::as_i64)
                .is_none()
            || point
                .get("availableAtUnixNanos")
                .and_then(Value::as_i64)
                .is_none()
            || point
                .get("mantissa")
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<i128>().ok())
                .is_none()
            || hash.is_none_or(|value| {
                value == "0000000000000000000000000000000000000000000000000000000000000000"
                    || value.len() != 64
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            })
            || point
                .get("quality")
                .and_then(Value::as_str)
                .is_none_or(|value| !OBSERVED_QUALITIES.contains(&value))
        {
            return Err(ToolInputError::Invalid);
        }
    }
    let inputs = request
        .get("inputs")
        .and_then(Value::as_array)
        .filter(|rows| rows.len() == horizon_points)
        .ok_or(ToolInputError::Invalid)?;
    if inputs.iter().any(|row| {
        row.as_array().is_none_or(|values| {
            values.is_empty()
                || values.len() > market_squawk_modeling::MAX_MODEL_FEATURES
                || values.iter().any(|value| value.as_f64().is_none())
        })
    }) {
        return Err(ToolInputError::Invalid);
    }
    Ok(())
}

fn fair_value_measurement_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "accountId": {"type": "string", "format": "uuid"},
            "instrumentId": {"type": "string", "format": "uuid"},
            "amount": {"type": "string", "minLength": 1, "maxLength": 128},
            "currency": {
                "type": "string",
                "minLength": 3,
                "maxLength": 3,
                "pattern": "^[A-Za-z]{3}$"
            },
            "scale": {"type": "integer", "minimum": 0, "maximum": 28},
            "amountBasis": {
                "type": "string",
                "enum": [
                    "per_instrument_unit",
                    "reporting_entity_total",
                    "position_total"
                ]
            },
            "measurementAt": {"type": "string", "format": "date-time"},
            "preparedAt": {"type": "string", "format": "date-time"},
            "preparedBy": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAXIMUM_FAIR_VALUE_ACTOR_BYTES
            },
            "method": {
                "type": "string",
                "enum": [
                    "quoted_market_price",
                    "market_approach",
                    "income_approach",
                    "cost_approach"
                ]
            },
            "producerReceipts": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAXIMUM_FAIR_VALUE_INPUTS,
                "items": {
                    "type": "object",
                    "properties": {
                        "producer": {
                            "type": "string",
                            "enum": ["research", "analytics", "portfolio"]
                        },
                        "receiptId": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": MAXIMUM_IDENTIFIER_BYTES
                        },
                        "significance": {
                            "type": "string",
                            "enum": ["significant", "not_significant"]
                        }
                    },
                    "required": ["producer", "receiptId", "significance"],
                    "additionalProperties": false
                }
            },
            "producerSelections": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAXIMUM_FAIR_VALUE_INPUTS,
                "items": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "producer": {"type": "string", "const": "live"},
                                "venueId": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": MAXIMUM_IDENTIFIER_BYTES
                                },
                                "selection": {
                                    "type": "string",
                                    "enum": ["trade", "bid", "ask"]
                                },
                                "significance": {
                                    "type": "string",
                                    "enum": ["significant", "not_significant"]
                                }
                            },
                            "required": ["producer", "venueId", "selection", "significance"],
                            "additionalProperties": false
                        },
                        {
                            "type": "object",
                            "properties": {
                                "producer": {
                                    "type": "string",
                                    "enum": ["research", "analytics"]
                                },
                                "datasetId": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": MAXIMUM_IDENTIFIER_BYTES
                                },
                                "row": {
                                    "type": "integer",
                                    "minimum": 0,
                                    "maximum": MAXIMUM_FAIR_VALUE_ROW_OFFSET
                                },
                                "significance": {
                                    "type": "string",
                                    "enum": ["significant", "not_significant"]
                                }
                            },
                            "required": ["producer", "datasetId", "row", "significance"],
                            "additionalProperties": false
                        },
                        {
                            "type": "object",
                            "properties": {
                                "producer": {"type": "string", "const": "portfolio"},
                                "significance": {
                                    "type": "string",
                                    "enum": ["significant", "not_significant"]
                                }
                            },
                            "required": ["producer", "significance"],
                            "additionalProperties": false
                        }
                    ]
                }
            }
        },
        "required": [
            "accountId",
            "instrumentId",
            "amount",
            "currency",
            "scale",
            "amountBasis",
            "measurementAt",
            "preparedAt",
            "preparedBy",
            "method"
        ],
        "anyOf": [
            {"required": ["producerReceipts"]},
            {"required": ["producerSelections"]}
        ],
        "additionalProperties": false
    })
}

fn admit_timestamp(value: &Value) -> Result<(), ToolInputError> {
    value
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .filter(|value| value.timestamp_nanos_opt().is_some())
        .map(|_| ())
        .ok_or(ToolInputError::Invalid)
}

fn admit_fair_value_measurement(value: &Value) -> Result<(), ToolInputError> {
    const REQUIRED: [&str; 10] = [
        "accountId",
        "instrumentId",
        "amount",
        "currency",
        "scale",
        "amountBasis",
        "measurementAt",
        "preparedAt",
        "preparedBy",
        "method",
    ];
    let measurement = value.as_object().ok_or(ToolInputError::Invalid)?;
    if REQUIRED
        .iter()
        .any(|required| !measurement.contains_key(*required))
        || measurement.keys().any(|key| {
            !matches!(
                key.as_str(),
                "accountId"
                    | "instrumentId"
                    | "amount"
                    | "currency"
                    | "scale"
                    | "amountBasis"
                    | "measurementAt"
                    | "preparedAt"
                    | "preparedBy"
                    | "method"
                    | "producerReceipts"
                    | "producerSelections"
            )
        })
    {
        return Err(ToolInputError::Invalid);
    }
    for identity in ["accountId", "instrumentId"] {
        if measurement
            .get(identity)
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .filter(|value| !value.is_nil())
            .is_none()
        {
            return Err(ToolInputError::Invalid);
        }
    }
    admit_argument(
        measurement.get("amount").ok_or(ToolInputError::Invalid)?,
        ArgumentKind::Decimal,
    )?;
    let currency = measurement
        .get("currency")
        .and_then(Value::as_str)
        .ok_or(ToolInputError::Invalid)?;
    if currency.len() != 3 || !currency.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return Err(ToolInputError::Invalid);
    }
    if measurement
        .get("scale")
        .and_then(Value::as_u64)
        .is_none_or(|scale| scale > 28)
    {
        return Err(ToolInputError::Invalid);
    }
    if measurement
        .get("amountBasis")
        .and_then(Value::as_str)
        .is_none_or(|basis| {
            !matches!(
                basis,
                "per_instrument_unit" | "reporting_entity_total" | "position_total"
            )
        })
    {
        return Err(ToolInputError::Invalid);
    }
    admit_timestamp(
        measurement
            .get("measurementAt")
            .ok_or(ToolInputError::Invalid)?,
    )?;
    admit_timestamp(
        measurement
            .get("preparedAt")
            .ok_or(ToolInputError::Invalid)?,
    )?;
    let prepared_by = measurement
        .get("preparedBy")
        .and_then(Value::as_str)
        .ok_or(ToolInputError::Invalid)?;
    if prepared_by.is_empty()
        || prepared_by.len() > MAXIMUM_FAIR_VALUE_ACTOR_BYTES
        || prepared_by
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(ToolInputError::Invalid);
    }
    if measurement
        .get("method")
        .and_then(Value::as_str)
        .is_none_or(|method| {
            !matches!(
                method,
                "quoted_market_price" | "market_approach" | "income_approach" | "cost_approach"
            )
        })
    {
        return Err(ToolInputError::Invalid);
    }
    let receipts = optional_fair_value_array(measurement, "producerReceipts")?;
    let selections = optional_fair_value_array(measurement, "producerSelections")?;
    let input_count = receipts
        .len()
        .checked_add(selections.len())
        .ok_or(ToolInputError::Invalid)?;
    if input_count == 0 || input_count > MAXIMUM_FAIR_VALUE_INPUTS {
        return Err(ToolInputError::Invalid);
    }
    for receipt in receipts {
        admit_fair_value_receipt(receipt)?;
    }
    for selection in selections {
        admit_fair_value_selection(selection)?;
    }
    Ok(())
}

fn optional_fair_value_array<'value>(
    measurement: &'value Map<String, Value>,
    field: &str,
) -> Result<&'value [Value], ToolInputError> {
    measurement.get(field).map_or(Ok(&[][..]), |value| {
        value
            .as_array()
            .filter(|values| !values.is_empty())
            .map(Vec::as_slice)
            .ok_or(ToolInputError::Invalid)
    })
}

fn admit_fair_value_receipt(value: &Value) -> Result<(), ToolInputError> {
    let receipt = value.as_object().ok_or(ToolInputError::Invalid)?;
    if receipt.len() != 3
        || receipt
            .keys()
            .any(|key| !matches!(key.as_str(), "producer" | "receiptId" | "significance"))
        || receipt
            .get("producer")
            .and_then(Value::as_str)
            .is_none_or(|producer| !matches!(producer, "research" | "analytics" | "portfolio"))
        || receipt
            .get("receiptId")
            .and_then(Value::as_str)
            .is_none_or(|identifier| !valid_identifier(identifier))
    {
        return Err(ToolInputError::Invalid);
    }
    admit_fair_value_significance(receipt)
}

fn admit_fair_value_selection(value: &Value) -> Result<(), ToolInputError> {
    let selection = value.as_object().ok_or(ToolInputError::Invalid)?;
    match selection.get("producer").and_then(Value::as_str) {
        Some("live") => {
            if selection.len() != 4
                || selection.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "producer" | "venueId" | "selection" | "significance"
                    )
                })
                || selection
                    .get("venueId")
                    .and_then(Value::as_str)
                    .is_none_or(|identifier| !valid_identifier(identifier))
                || selection
                    .get("selection")
                    .and_then(Value::as_str)
                    .is_none_or(|selection| !matches!(selection, "trade" | "bid" | "ask"))
            {
                return Err(ToolInputError::Invalid);
            }
        }
        Some("research" | "analytics") => {
            if selection.len() != 4
                || selection.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "producer" | "datasetId" | "row" | "significance"
                    )
                })
                || selection
                    .get("datasetId")
                    .and_then(Value::as_str)
                    .is_none_or(|identifier| !valid_identifier(identifier))
                || selection
                    .get("row")
                    .and_then(Value::as_u64)
                    .is_none_or(|row| row > MAXIMUM_FAIR_VALUE_ROW_OFFSET)
            {
                return Err(ToolInputError::Invalid);
            }
        }
        Some("portfolio") => {
            if selection.len() != 2
                || selection
                    .keys()
                    .any(|key| !matches!(key.as_str(), "producer" | "significance"))
            {
                return Err(ToolInputError::Invalid);
            }
        }
        _ => return Err(ToolInputError::Invalid),
    }
    admit_fair_value_significance(selection)
}

fn admit_fair_value_significance(value: &Map<String, Value>) -> Result<(), ToolInputError> {
    if value
        .get("significance")
        .and_then(Value::as_str)
        .is_some_and(|value| matches!(value, "significant" | "not_significant"))
    {
        Ok(())
    } else {
        Err(ToolInputError::Invalid)
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAXIMUM_IDENTIFIER_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn valid_source_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAXIMUM_IDENTIFIER_BYTES
        && !value.chars().any(char::is_control)
}
