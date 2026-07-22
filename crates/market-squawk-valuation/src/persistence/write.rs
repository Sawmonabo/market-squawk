//! Canonical immutable records, relationships, and append operations.

use super::recovery::{
    amount_payload, digest_algorithm_tag, hierarchy_tag, manifest_payload, use_assessment_payload,
};
use super::*;

pub(crate) fn classify_operation(
    measurement: &ValuationMeasurement,
    decision: &ClassificationDecision,
    ruleset: &ClassificationRuleset,
) -> Result<FairValueCatalogOperation, FairValueError> {
    let mut records = Vec::new();
    let mut links = Vec::new();
    for input in measurement.inputs() {
        records.push(evidence_record(input.evidence())?);
        records.push(input_record(input)?);
        links.push(link(
            FairValueRecordKind::Evidence,
            input.evidence().hash().bytes(),
            FairValueLinkRelation::EvidenceToInput,
            FairValueRecordKind::Input,
            input.id().bytes(),
        )?);
        links.push(link(
            FairValueRecordKind::Input,
            input.id().bytes(),
            FairValueLinkRelation::InputToMeasurement,
            FairValueRecordKind::Measurement,
            measurement.id().bytes(),
        )?);
        if let Some(access) = input.market_access_assessment() {
            links.push(link(
                FairValueRecordKind::MarketAccess,
                access.id().bytes(),
                FairValueLinkRelation::MarketAccessToInput,
                FairValueRecordKind::Input,
                input.id().bytes(),
            )?);
        }
    }
    records.push(measurement_record(measurement)?);
    records.push(record(
        FairValueRecordKind::Decision,
        decision.id().bytes(),
        &DecisionPayload::Rules {
            version: PAYLOAD_VERSION,
            measurement_id: measurement.id().bytes(),
            max_quote_age_nanos: ruleset.max_quote_age_nanos(),
        },
    )?);
    links.push(link(
        FairValueRecordKind::Measurement,
        measurement.id().bytes(),
        FairValueLinkRelation::MeasurementToDecision,
        FairValueRecordKind::Decision,
        decision.id().bytes(),
    )?);
    operation(
        FairValueOperationKind::Classify,
        measurement.prepared_by(),
        measurement.prepared_at(),
        records,
        links,
    )
}

pub(crate) fn override_operation(
    value: &ValuationOverride,
    decision: &ClassificationDecision,
) -> Result<FairValueCatalogOperation, FairValueError> {
    operation(
        FairValueOperationKind::ProposeOverride,
        value.prepared_by(),
        value.prepared_at(),
        vec![
            record(
                FairValueRecordKind::Override,
                value.id().bytes(),
                &OverridePayload {
                    version: PAYLOAD_VERSION,
                    base_decision_id: value.base_decision_id().bytes(),
                    requested_hierarchy: hierarchy_tag(value.requested_hierarchy()),
                    justification: value.justification().to_owned(),
                    prepared_by: value.prepared_by().as_str().to_owned(),
                    prepared_at_ns: value.prepared_at().unix_nanos(),
                    expires_at_ns: value.expires_at().unix_nanos(),
                },
            )?,
            record(
                FairValueRecordKind::Decision,
                decision.id().bytes(),
                &DecisionPayload::Override {
                    version: PAYLOAD_VERSION,
                    base_decision_id: value.base_decision_id().bytes(),
                    override_id: value.id().bytes(),
                },
            )?,
        ],
        vec![
            link(
                FairValueRecordKind::Decision,
                value.base_decision_id().bytes(),
                FairValueLinkRelation::DecisionToOverride,
                FairValueRecordKind::Override,
                value.id().bytes(),
            )?,
            link(
                FairValueRecordKind::Override,
                value.id().bytes(),
                FairValueLinkRelation::OverrideToDecision,
                FairValueRecordKind::Decision,
                decision.id().bytes(),
            )?,
        ],
    )
}

pub(crate) fn approval_operation(
    value: &ValuationApproval,
) -> Result<FairValueCatalogOperation, FairValueError> {
    operation(
        FairValueOperationKind::Approve,
        value.approved_by(),
        value.approved_at(),
        vec![record(
            FairValueRecordKind::Approval,
            value.id().bytes(),
            &ApprovalPayload {
                version: PAYLOAD_VERSION,
                decision_id: value.decision_id().bytes(),
                approved_by: value.approved_by().as_str().to_owned(),
                approved_at_ns: value.approved_at().unix_nanos(),
                expires_at_ns: value.expires_at().unix_nanos(),
            },
        )?],
        vec![link(
            FairValueRecordKind::Decision,
            value.decision_id().bytes(),
            FairValueLinkRelation::DecisionToApproval,
            FairValueRecordKind::Approval,
            value.id().bytes(),
        )?],
    )
}

pub(crate) fn revocation_operation(
    value: &ApprovalRevocation,
) -> Result<FairValueCatalogOperation, FairValueError> {
    operation(
        FairValueOperationKind::Revoke,
        value.revoked_by(),
        value.revoked_at(),
        vec![record(
            FairValueRecordKind::Revocation,
            value.id().bytes(),
            &RevocationPayload {
                version: PAYLOAD_VERSION,
                approval_id: value.approval_id().bytes(),
                revoked_by: value.revoked_by().as_str().to_owned(),
                revoked_at_ns: value.revoked_at().unix_nanos(),
                reason: value.reason().to_owned(),
            },
        )?],
        vec![link(
            FairValueRecordKind::Approval,
            value.approval_id().bytes(),
            FairValueLinkRelation::ApprovalToRevocation,
            FairValueRecordKind::Revocation,
            value.id().bytes(),
        )?],
    )
}

pub(crate) fn market_access_operation(
    value: &ApprovedMarketAccess,
) -> Result<FairValueCatalogOperation, FairValueError> {
    operation(
        FairValueOperationKind::ApproveMarketAccess,
        value.approved_by(),
        value.approved_at(),
        vec![market_access_record(value)?],
        Vec::new(),
    )
}

fn evidence_record(value: &FairValueEvidence) -> Result<FairValueCatalogRecord, FairValueError> {
    record(
        FairValueRecordKind::Evidence,
        value.hash().bytes(),
        &evidence_payload(value)?,
    )
}

fn input_record(value: &ValuationInput) -> Result<FairValueCatalogRecord, FairValueError> {
    record(
        FairValueRecordKind::Input,
        value.id().bytes(),
        &InputPayload {
            version: PAYLOAD_VERSION,
            subject_instrument_id: value.subject_instrument_id().to_string(),
            reference_instrument_id: value.reference_instrument_id().to_string(),
            relationship: crate::measurement::relation_tag(value.relationship()),
            amount: amount_payload(value.amount()),
            significance: crate::measurement::significance_tag(value.significance()),
            observability: crate::measurement::observability_tag(value.observability()),
            adjustment: crate::measurement::adjustment_tag(value.adjustment()),
            market_activity: crate::measurement::activity_tag(value.market_activity()),
            market_access: crate::measurement::access_tag(value.market_access()),
            data_quality: crate::measurement::quality_tag(value.data_quality()),
            evidence_id: value.evidence().hash().bytes(),
            use_assessment: value.use_assessment().map(use_assessment_payload),
            market_access_id: value
                .market_access_assessment()
                .map(|item| item.id().bytes()),
        },
    )
}

fn measurement_record(
    value: &ValuationMeasurement,
) -> Result<FairValueCatalogRecord, FairValueError> {
    record(
        FairValueRecordKind::Measurement,
        value.id().bytes(),
        &MeasurementPayload {
            version: PAYLOAD_VERSION,
            account_id: value.account_id().to_string(),
            instrument_id: value.instrument_id().to_string(),
            amount: amount_payload(value.amount()),
            measurement_at_ns: value.measurement_at().unix_nanos(),
            prepared_at_ns: value.prepared_at().unix_nanos(),
            prepared_by: value.prepared_by().as_str().to_owned(),
            method: crate::measurement::method_tag(value.method()),
            input_ids: value
                .inputs()
                .iter()
                .map(|input| input.id().bytes())
                .collect(),
        },
    )
}

fn market_access_record(
    value: &ApprovedMarketAccess,
) -> Result<FairValueCatalogRecord, FairValueError> {
    record(
        FairValueRecordKind::MarketAccess,
        value.id().bytes(),
        &MarketAccessPayload {
            version: PAYLOAD_VERSION,
            account_id: value.account_id().to_string(),
            venue_id: value.venue_id().as_str().to_owned(),
            instrument_id: value.instrument_id().to_string(),
            conclusion: crate::measurement::access_tag(value.conclusion()),
            effective_from_ns: value.effective_from().unix_nanos(),
            effective_until_ns: value.effective_until().unix_nanos(),
            rationale: value.rationale().to_owned(),
            prepared_by: value.prepared_by().as_str().to_owned(),
            prepared_at_ns: value.prepared_at().unix_nanos(),
            approved_by: value.approved_by().as_str().to_owned(),
            approved_at_ns: value.approved_at().unix_nanos(),
        },
    )
}

fn evidence_payload(value: &FairValueEvidence) -> Result<EvidencePayload, FairValueError> {
    Ok(EvidencePayload {
        version: PAYLOAD_VERSION,
        source_id: value.source_id().as_str().to_owned(),
        source_identifier: value.source_identifier().as_str().to_owned(),
        payload_algorithm: digest_algorithm_tag(value.payload_digest().algorithm()),
        payload_digest: value.payload_digest().bytes(),
        origin: origin_payload(value.origin())?,
        source_timestamp_ns: value.source_timestamp().map(Timestamp::unix_nanos),
        effective_at_ns: value.effective_at().map(Timestamp::unix_nanos),
        published_at_ns: value.published_at().map(Timestamp::unix_nanos),
        available_at_ns: value.available_at().map(Timestamp::unix_nanos),
        received_at_ns: value.received_at().map(Timestamp::unix_nanos),
        ingested_at_ns: value.ingested_at().unix_nanos(),
        verification: match value.verification() {
            EvidenceVerification::Verified => 1,
            EvidenceVerification::Unverified => 2,
        },
    })
}

fn origin_payload(value: &EvidenceOrigin) -> Result<OriginPayload, FairValueError> {
    Ok(match value {
        EvidenceOrigin::Market {
            venue_id,
            assessment_id,
            binding_digest,
            canonical_state_digest,
            committed_state_revision,
            definition_revision,
            activity_policy_hash,
            activity_set_hash,
        } => OriginPayload::Market {
            venue_id: venue_id.as_str().to_owned(),
            assessment_id: assessment_id.as_str().to_owned(),
            binding_digest: *binding_digest,
            canonical_state_algorithm: digest_algorithm_tag(canonical_state_digest.algorithm()),
            canonical_state_digest: canonical_state_digest.bytes(),
            committed_state_revision: *committed_state_revision,
            definition_revision: *definition_revision,
            activity_policy_hash: *activity_policy_hash,
            activity_set_hash: *activity_set_hash,
        },
        EvidenceOrigin::Research {
            manifest,
            object_graph_digest,
            query_identity,
            result_digest,
            row,
            revision,
        } => OriginPayload::Research {
            manifest: manifest_payload(manifest),
            object_graph_algorithm: digest_algorithm_tag(object_graph_digest.algorithm()),
            object_graph_digest: object_graph_digest.bytes(),
            query_algorithm: digest_algorithm_tag(query_identity.algorithm()),
            query_digest: query_identity.bytes(),
            result_algorithm: digest_algorithm_tag(result_digest.algorithm()),
            result_digest: result_digest.bytes(),
            row: u64::try_from(*row).map_err(|_| FairValueError::Arithmetic)?,
            revision: *revision,
        },
        EvidenceOrigin::Analytics {
            feature_key,
            semantic_digest,
            manifest,
            object_graph_digest,
            query_identity,
            result_digest,
            row,
            revision,
        } => OriginPayload::Analytics {
            feature_name: feature_key.name().to_owned(),
            feature_version: feature_key.version().get(),
            semantic_digest: *semantic_digest,
            manifest: manifest_payload(manifest),
            object_graph_algorithm: digest_algorithm_tag(object_graph_digest.algorithm()),
            object_graph_digest: object_graph_digest.bytes(),
            query_algorithm: digest_algorithm_tag(query_identity.algorithm()),
            query_digest: query_identity.bytes(),
            result_algorithm: digest_algorithm_tag(result_digest.algorithm()),
            result_digest: result_digest.bytes(),
            row: u64::try_from(*row).map_err(|_| FairValueError::Arithmetic)?,
            revision: *revision,
        },
        EvidenceOrigin::Portfolio {
            revision,
            account_id,
            position_quantity,
            point_in_time_digest,
        } => OriginPayload::Portfolio {
            revision: *revision,
            account_id: account_id.to_string(),
            quantity_mantissa: position_quantity.mantissa().to_string(),
            quantity_scale: position_quantity.scale(),
            point_in_time_digest: *point_in_time_digest,
        },
    })
}

fn record<T: Serialize>(
    kind: FairValueRecordKind,
    id: [u8; 32],
    payload: &T,
) -> Result<FairValueCatalogRecord, FairValueError> {
    FairValueCatalogRecord::try_new(
        kind,
        id,
        serde_json::to_vec(payload).map_err(|_| FairValueError::Persistence)?,
    )
    .map_err(|_| FairValueError::Persistence)
}

fn link(
    source_kind: FairValueRecordKind,
    source_id: [u8; 32],
    relation: FairValueLinkRelation,
    target_kind: FairValueRecordKind,
    target_id: [u8; 32],
) -> Result<FairValueCatalogLink, FairValueError> {
    FairValueCatalogLink::try_new(source_kind, source_id, relation, target_kind, target_id)
        .map_err(|_| FairValueError::Persistence)
}

fn operation(
    kind: FairValueOperationKind,
    actor: &ActorId,
    business_at: Timestamp,
    records: Vec<FairValueCatalogRecord>,
    links: Vec<FairValueCatalogLink>,
) -> Result<FairValueCatalogOperation, FairValueError> {
    FairValueCatalogOperation::try_new(kind, actor.as_str(), business_at, records, links)
        .map_err(|_| FairValueError::Persistence)
}
