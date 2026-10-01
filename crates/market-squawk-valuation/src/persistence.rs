//! Canonical catalog payloads, append operations, and semantic recovery.

mod recovery;
mod write;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use market_squawk_analytics::FeatureKey;
use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, FairValueCatalogLink,
    FairValueCatalogOperation, FairValueCatalogRecord, FairValueCatalogSnapshot,
    FairValueLinkRelation, FairValueOperationKind, FairValueRecordKind, MarketEventCommitRef,
    Sha256Digest,
};
use market_squawk_domain::{
    Currency, DataQuality, DigestAlgorithm, EvidenceDigest, FairValueHierarchy, InstrumentId,
    Money, SchemaVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::approval::{ApprovalRevocation, ValuationApproval, ValuationOverride};
use crate::evidence::FairValueEvidenceParts;
use crate::measurement::ValuationInputSpec;
use crate::{
    ActorId, ApprovalRevocationId, ApprovedMarketAccess, ClassificationDecision,
    ClassificationRuleset, DecisionBasis, DecisionId, EvidenceOrigin, EvidenceVerification,
    FairValueError, FairValueEvidence, FairValueEvidenceHash, InputId, InputInstrumentRelation,
    InputObservability, InputSignificance, InputUseAssessment, MarketAccess,
    MarketAccessAssessmentId, MarketActivity, MeasurementId, OverrideId, PriceAdjustment,
    ValuationAmount, ValuationAmountBasis, ValuationApprovalId, ValuationInput,
    ValuationMeasurement, ValuationMeasurementSpec, ValuationMethod,
};

const PAYLOAD_VERSION: u16 = 2;

pub(crate) use recovery::{recover, recover_with_forecasts};
pub(crate) use write::{
    approval_operation, classify_operation, market_access_operation, override_operation,
    revocation_operation,
};

#[derive(Debug)]
pub(crate) struct RecoveredState {
    pub(crate) measurements: BTreeMap<MeasurementId, std::sync::Arc<ValuationMeasurement>>,
    pub(crate) decisions: BTreeMap<DecisionId, std::sync::Arc<ClassificationDecision>>,
    pub(crate) overrides: BTreeMap<OverrideId, std::sync::Arc<ValuationOverride>>,
    pub(crate) approvals: BTreeMap<ValuationApprovalId, std::sync::Arc<ValuationApproval>>,
    pub(crate) revocations: BTreeMap<ValuationApprovalId, std::sync::Arc<ApprovalRevocation>>,
    pub(crate) market_access:
        BTreeMap<MarketAccessAssessmentId, std::sync::Arc<ApprovedMarketAccess>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct EvidencePayload {
    version: u16,
    source_id: String,
    source_identifier: String,
    payload_algorithm: u8,
    payload_digest: [u8; 32],
    origin: OriginPayload,
    source_timestamp_ns: Option<i64>,
    effective_at_ns: Option<i64>,
    published_at_ns: Option<i64>,
    available_at_ns: Option<i64>,
    received_at_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    qualification_evaluated_at_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    qualification_valid_until_ns: Option<i64>,
    ingested_at_ns: i64,
    verification: u8,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "origin", rename_all = "snake_case", deny_unknown_fields)]
enum OriginPayload {
    ForecastDistribution {
        source: Box<ForecastReferencePayload>,
        #[serde(deserialize_with = "deserialize_required_option")]
        ordinal: Option<u32>,
        financial_origin: bool,
    },
    PublishedMarket {
        commit: MarketEventCommitPayload,
        selection_digest: [u8; 32],
        publication_digest: [u8; 32],
        publication_row: u32,
        canonical_event_digest: [u8; 32],
        canonical_event: String,
        canonical_price_authority: String,
        definition_content: [u8; 32],
        definition_audit: [u8; 32],
        knowledge_at_ns: i64,
        commit_available_at_ns: i64,
        origin_committed_at_ns: i64,
    },
    Market {
        venue_id: String,
        assessment_id: String,
        binding_digest: [u8; 32],
        canonical_state_algorithm: u8,
        canonical_state_digest: [u8; 32],
        committed_state_revision: u64,
        definition_revision: u64,
        activity_policy_hash: [u8; 32],
        activity_set_hash: [u8; 32],
        #[serde(default, skip_serializing_if = "Option::is_none")]
        publication: Option<MarketPublicationPayload>,
    },
    Research {
        manifest: ManifestPayload,
        object_graph_algorithm: u8,
        object_graph_digest: [u8; 32],
        query_algorithm: u8,
        query_digest: [u8; 32],
        result_algorithm: u8,
        result_digest: [u8; 32],
        row: u64,
        revision: u32,
    },
    Analytics {
        feature_name: String,
        feature_version: u32,
        semantic_digest: [u8; 32],
        manifest: ManifestPayload,
        object_graph_algorithm: u8,
        object_graph_digest: [u8; 32],
        query_algorithm: u8,
        query_digest: [u8; 32],
        result_algorithm: u8,
        result_digest: [u8; 32],
        row: u64,
        revision: u32,
    },
    Portfolio {
        revision: [u8; 32],
        account_id: String,
        quantity_mantissa: String,
        quantity_scale: u32,
        point_in_time_digest: [u8; 32],
    },
    Fundamental {
        manifest: ManifestPayload,
        origin_digest: [u8; 32],
        request_digest: [u8; 32],
        selection_digest: [u8; 32],
        result_digest: [u8; 32],
        company_security_digest: [u8; 32],
        row: u32,
        canonical_row_digest: [u8; 32],
        knowledge_at_ns: i64,
        generation_completed_at_ns: i64,
        canonical_observation: String,
    },
    AutomaticValuation {
        receipt: Box<AutomaticReceiptPayload>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastReferencePayload {
    identity: [u8; 32],
    distribution_identity: [u8; 32],
    vintage_id: [u8; 32],
    forecast_artifact_hash: [u8; 32],
    metadata_hash: [u8; 32],
    instrument_id: String,
    training_manifest: ManifestPayload,
    serving_manifest: ManifestPayload,
    parent_manifests: Vec<ManifestPayload>,
    serving_source: String,
    serving_graph: [u8; 32],
    serving_query: [u8; 32],
    serving_result: [u8; 32],
    serving_feature: [u8; 32],
    #[serde(deserialize_with = "deserialize_required_option")]
    origin_bar_digest: Option<[u8; 32]>,
    #[serde(deserialize_with = "deserialize_required_option")]
    financial_epoch_digest: Option<[u8; 32]>,
    #[serde(deserialize_with = "deserialize_required_option")]
    current_price_epoch_digest: Option<[u8; 32]>,
    knowledge_at_ns: i64,
    selected_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MarketPublicationPayload {
    qualified_input_id: [u8; 32],
    qualified_amount: AmountPayload,
    commit: MarketEventCommitPayload,
    selection_digest: [u8; 32],
    publication_digest: [u8; 32],
    publication_row: u32,
    coordinate_digest: [u8; 32],
    canonical_event_digest: [u8; 32],
    canonical_event: String,
    knowledge_at_ns: i64,
    commit_available_at_ns: i64,
    origin_committed_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AutomaticReceiptPayload {
    id: [u8; 32],
    input_set_id: [u8; 32],
    method: u8,
    periods_per_year: Option<u32>,
    account_id: String,
    instrument_id: String,
    company_security: String,
    peer_identities: Vec<String>,
    rights_decision: [u8; 32],
    rights_graph: [u8; 32],
    rights_input_digest: [u8; 32],
    rights_expires_at_ns: i64,
    admitted_input_manifests: Vec<ManifestPayload>,
    admitted_event_inputs: Vec<EventRightsAdmissionPayload>,
    current_market_input: [u8; 32],
    method_base_input: Option<[u8; 32]>,
    inputs: Vec<AutomaticInputPayload>,
    assumptions: Vec<AutomaticAssumptionPayload>,
    #[serde(deserialize_with = "deserialize_required_option")]
    macro_assumptions: Option<AutomaticMacroAssumptionsPayload>,
    #[serde(deserialize_with = "deserialize_required_option")]
    residual_terminal: Option<ResidualIncomeTerminalPayload>,
    intermediates: Vec<AutomaticIntermediatePayload>,
    lower: AmountPayload,
    central: AmountPayload,
    upper: AmountPayload,
    rounding: market_squawk_domain::RoundingPolicy,
    maximum_periods: u32,
    method_selection_receipt: Option<[u8; 32]>,
    forecast_horizon_nanos: Option<u64>,
    forecast_terminal_at_ns: Option<i64>,
    measurement_at_ns: i64,
    calculated_at_ns: i64,
    calculated_by: String,
    expires_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ResidualIncomeTerminalPayload {
    convention: u8,
    terminal_period: u32,
    current_book_input: [u8; 32],
    final_income_input: [u8; 32],
    final_opening_book_input: [u8; 32],
    annual_rate_identity: [u8; 32],
    annual_cost_of_equity_mantissa: String,
    annual_cost_of_equity_scale: u32,
    continuing_value_sensitivity_mantissa: String,
    continuing_value_sensitivity_scale: u32,
    identity: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AutomaticInputPayload {
    input_id: [u8; 32],
    input: InputPayload,
    evidence: EvidencePayload,
    market_access: Option<MarketAccessPayload>,
    selection_receipt: [u8; 32],
    rights_input_digest: [u8; 32],
    knowledge_at_ns: i64,
    expires_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AutomaticMacroAssumptionsPayload {
    maturity: u8,
    annual_yield_mantissa: String,
    annual_yield_scale: u32,
    context_identity: [u8; 32],
    evidence_identity: [u8; 32],
    knowledge_cutoff_ns: i64,
    effective_date_cutoff: market_squawk_domain::CalendarDate,
    available_at_ns: i64,
    expires_at_ns: i64,
    premium: AutomaticAssumptionPayload,
    #[serde(deserialize_with = "deserialize_required_option")]
    premium_source: Option<Vec<u8>>,
    premium_parents: Vec<ManifestPayload>,
    rate: AutomaticAssumptionPayload,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AutomaticAssumptionPayload {
    kind: u8,
    identifier: String,
    mantissa: String,
    scale: u32,
    evidence: [u8; 32],
    available_at_ns: i64,
    expires_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AutomaticIntermediatePayload {
    kind: u8,
    sequence: u32,
    instrument_id: String,
    primary_input: [u8; 32],
    secondary_input: Option<[u8; 32]>,
    amount_mantissa: String,
    amount_scale: u32,
    adjustment_mantissa: String,
    adjustment_scale: u32,
    factor_mantissa: String,
    factor_scale: u32,
    result_mantissa: String,
    result_scale: u32,
    evidence: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MarketEventCommitPayload {
    dataset_id: String,
    sequence: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: [u8; 32],
    content_hash: [u8; 32],
    available_at_ns: i64,
    publication_digest: [u8; 32],
    row_count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct EventRightsAdmissionPayload {
    commit: MarketEventCommitPayload,
    inputs: Vec<EventUseInputPayload>,
    rights_input_digest: [u8; 32],
    decision_digest: [u8; 32],
    evaluated_at_ns: i64,
    expires_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct EventUseInputPayload {
    publication_digest: [u8; 32],
    publication_kind: String,
    row_ordinal: u32,
    coordinate_digest: [u8; 32],
    canonical_event_digest: [u8; 32],
    source_id: String,
    origin_committed_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestPayload {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: [u8; 32],
    content_hash: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct InputPayload {
    version: u16,
    subject_instrument_id: String,
    reference_instrument_id: String,
    relationship: u8,
    amount: AmountPayload,
    significance: u8,
    observability: u8,
    adjustment: u8,
    market_activity: u8,
    market_access: u8,
    data_quality: u8,
    evidence_id: [u8; 32],
    use_assessment: Option<UseAssessmentPayload>,
    market_access_id: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AmountPayload {
    mantissa: String,
    decimal_scale: u32,
    currency: String,
    accounting_scale: u8,
    basis: u8,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct UseAssessmentPayload {
    subject_instrument_id: String,
    relationship: u8,
    observability: u8,
    adjustment: u8,
    rationale: String,
    assessed_by: String,
    assessed_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MeasurementPayload {
    version: u16,
    account_id: String,
    instrument_id: String,
    amount: AmountPayload,
    measurement_at_ns: i64,
    prepared_at_ns: i64,
    prepared_by: String,
    method: u8,
    input_ids: Vec<[u8; 32]>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "basis", rename_all = "snake_case", deny_unknown_fields)]
enum DecisionPayload {
    Rules {
        version: u16,
        measurement_id: [u8; 32],
        max_quote_age_nanos: u64,
        ruleset_version: u32,
    },
    Override {
        version: u16,
        base_decision_id: [u8; 32],
        override_id: [u8; 32],
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct OverridePayload {
    version: u16,
    base_decision_id: [u8; 32],
    requested_hierarchy: u8,
    justification: String,
    prepared_by: String,
    prepared_at_ns: i64,
    expires_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ApprovalPayload {
    version: u16,
    decision_id: [u8; 32],
    approved_by: String,
    approved_at_ns: i64,
    expires_at_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RevocationPayload {
    version: u16,
    approval_id: [u8; 32],
    revoked_by: String,
    revoked_at_ns: i64,
    reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MarketAccessPayload {
    version: u16,
    account_id: String,
    venue_id: String,
    instrument_id: String,
    conclusion: u8,
    effective_from_ns: i64,
    effective_until_ns: i64,
    rationale: String,
    prepared_by: String,
    prepared_at_ns: i64,
    approved_by: String,
    approved_at_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    supersedes_id: Option<[u8; 32]>,
}

// A nullable current-contract field is mandatory even when its value is null.
fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
