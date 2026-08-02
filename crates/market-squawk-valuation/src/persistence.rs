//! Canonical catalog payloads, append operations, and semantic recovery.

mod recovery;
mod write;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use market_squawk_analytics::FeatureKey;
use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, FairValueCatalogLink,
    FairValueCatalogOperation, FairValueCatalogRecord, FairValueCatalogSnapshot,
    FairValueLinkRelation, FairValueOperationKind, FairValueRecordKind, Sha256Digest,
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
    ValuationAmount, ValuationApprovalId, ValuationInput, ValuationMeasurement,
    ValuationMeasurementSpec, ValuationMethod,
};

const PAYLOAD_VERSION: u16 = 1;

pub(crate) use recovery::recover;
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ruleset_version: Option<u32>,
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
