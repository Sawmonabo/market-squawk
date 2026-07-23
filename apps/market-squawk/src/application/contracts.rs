//! Closed descriptor registry and common request admission for the local application.

use std::collections::HashSet;

use chrono::DateTime;
use market_squawk_services::{
    ScopeRequirement, ServiceCapabilities, ServiceCapabilityError, ServiceDomain,
    SourceEvidencePolicy, ToolArtifactPolicy, ToolAuthorization, ToolContract, ToolDescriptor,
    ToolEffects, ToolInputError, ToolResultPolicy, ToolScope,
};
use serde_json::{Map, Value, json};
use uuid::Uuid;

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
const DATA_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::Optional,
    ScopeRequirement::Optional,
    ScopeRequirement::Required,
    ScopeRequirement::Optional,
);
const PORTFOLIO_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::Optional,
    ScopeRequirement::Optional,
    ScopeRequirement::Required,
    ScopeRequirement::Optional,
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
const PROVIDER_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::required("provider", ArgumentKind::Identifier)];
const ACCOUNT_ARGUMENT: &[ArgumentSpec] = &[ArgumentSpec::required(
    "accountId",
    ArgumentKind::Identifier,
)];
const PORTFOLIO_IMPORT_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("accountId", ArgumentKind::Identifier),
    ArgumentSpec::required("artifactId", ArgumentKind::Identifier),
];
const MODEL_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::required("modelId", ArgumentKind::Identifier)];
const MODEL_EVALUATION_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("modelId", ArgumentKind::Identifier),
    ArgumentSpec::required("input", ArgumentKind::Object),
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
const RUN_ARGUMENT: &[ArgumentSpec] = &[ArgumentSpec::required("runId", ArgumentKind::Identifier)];
const BOT_START_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required(
        "provider",
        ArgumentKind::Enumeration(&["coinbase", "kraken"]),
    ),
    ArgumentSpec::required("initialCash", ArgumentKind::Decimal),
    ArgumentSpec::required(
        "feeBasisPoints",
        ArgumentKind::Unsigned {
            minimum: 0,
            maximum: 100_000,
        },
    ),
];
const BOT_STOP_ARGUMENTS: &[ArgumentSpec] = &[ArgumentSpec::required("reason", ArgumentKind::Text)];
const ORDER_ARGUMENT: &[ArgumentSpec] =
    &[ArgumentSpec::required("orderId", ArgumentKind::Identifier)];
const INGEST_SOURCE_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec::required("provider", ArgumentKind::Identifier),
    ArgumentSpec::required("object", ArgumentKind::Identifier),
    ArgumentSpec::required("dataset", ArgumentKind::Identifier),
];

const OPERATION_SPECS: &[OperationSpec] = &[
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
    mutation(
        "Research.IngestSource",
        "Extract and ingest one configured provider object under retained rights authority.",
        ServiceDomain::Research,
        SOURCE_SCOPE,
        INGEST_SOURCE_ARGUMENTS,
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
    read_analysis(
        "Analysis.GetReturns",
        "Return bounded price and total returns.",
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
    read(
        "Analysis.GetFeatureDatasets",
        "Return registered feature contracts and immutable feature datasets.",
        ServiceDomain::Analysis,
        DATA_SCOPE,
        OPTIONAL_DATASET_ARGUMENT,
        SourceEvidencePolicy::Required,
    ),
    read(
        "Analysis.GetBacktests",
        "Return governed backtest experiment metadata and results.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        RUN_ARGUMENT,
        SourceEvidencePolicy::NotApplicable,
    ),
    mutation(
        "Analysis.RunBacktest",
        "Run one governed point-in-time backtest experiment.",
        ServiceDomain::Analysis,
        LOCAL_SCOPE,
        BACKTEST_RUN_ARGUMENTS,
        ToolAuthorization::LocalConfirmation,
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
        let effects = if matches!(spec.authorization, ToolAuthorization::ReadOnly) {
            ToolEffects::read_only_closed_world()
        } else {
            ToolEffects::try_new(false, spec.destructive, spec.idempotent, false)?
        };
        let contract = ToolContract::new(
            spec.domain,
            spec.authorization,
            spec.scope,
            ToolResultPolicy::new(spec.source_evidence, spec.artifact),
        );
        let operation = *spec;
        descriptors.push(ToolDescriptor::try_new(
            spec.name,
            APPLICATION_CONTRACT_VERSION,
            spec.description,
            schema,
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
    Text,
    Decimal,
    Object,
    Timestamp,
    FairValueMeasurement,
    Enumeration(&'static [&'static str]),
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
        ArgumentKind::Object => json!({"type": "object", "minProperties": 1}),
        ArgumentKind::Timestamp => json!({"type": "string", "format": "date-time"}),
        ArgumentKind::FairValueMeasurement => fair_value_measurement_schema(),
        ArgumentKind::Enumeration(values) => json!({"type": "string", "enum": values}),
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
        ArgumentKind::Object => value
            .as_object()
            .filter(|value| !value.is_empty())
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Timestamp => admit_timestamp(value),
        ArgumentKind::FairValueMeasurement => admit_fair_value_measurement(value),
        ArgumentKind::Enumeration(values) => value
            .as_str()
            .filter(|value| values.contains(value))
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
        ArgumentKind::Unsigned { minimum, maximum } => value
            .as_u64()
            .filter(|value| (*value >= minimum) && (*value <= maximum))
            .map(|_| ())
            .ok_or(ToolInputError::Invalid),
    }
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
    const REQUIRED: [&str; 9] = [
        "accountId",
        "instrumentId",
        "amount",
        "currency",
        "scale",
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
