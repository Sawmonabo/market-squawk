//! Stable bounded JSON views of immutable fair-value records.

use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_data::DatasetManifestRef;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, FairValueHierarchy, Timestamp,
};
use market_squawk_valuation::{
    ApprovalRevocation, ApprovalStatus, ClassificationDecision, DecisionBasis, DecisionReason,
    DecisionReasonCode, EvidenceOrigin, EvidenceVerification, FairValueEvidence,
    InputInstrumentRelation, InputObservability, InputSignificance, MarketAccess, MarketActivity,
    Predicate, PredicateResult, PriceAdjustment, ValuationAmount, ValuationAmountBasis,
    ValuationApproval, ValuationInput, ValuationMeasurement, ValuationMethod,
};
use serde_json::{Value, json};

use super::super::domain_support::encode_hex;

pub(super) fn measurement_value(measurement: &ValuationMeasurement) -> Value {
    json!({
        "measurementId": measurement.id().to_string(),
        "evidenceHash": measurement.evidence_hash().to_string(),
        "accountId": measurement.account_id().to_string(),
        "instrumentId": measurement.instrument_id().to_string(),
        "amount": amount_value(measurement.amount()),
        "measurementAt": timestamp_value(measurement.measurement_at()),
        "preparedAt": timestamp_value(measurement.prepared_at()),
        "preparedBy": measurement.prepared_by().as_str(),
        "method": method_name(measurement.method()),
        "inputCount": measurement.inputs().len()
    })
}

pub(super) fn classification_value(decision: &ClassificationDecision) -> Value {
    json!({
        "decisionId": decision.id().to_string(),
        "measurementId": decision.measurement_id().to_string(),
        "evidenceHash": decision.evidence_hash().to_string(),
        "rulesetVersion": decision.ruleset_version(),
        "rulesetHash": decision.ruleset_hash().to_string(),
        "hierarchy": hierarchy_name(decision.hierarchy()),
        "basis": basis_value(decision.basis()),
        "truthTableItemCount": decision.truth_table().len(),
        "reasonCount": decision.reasons().len()
    })
}

pub(super) fn predicate_result_value(result: PredicateResult) -> Value {
    json!({
        "inputId": result.input_id().to_string(),
        "predicate": predicate_name(result.predicate()),
        "passed": result.passed()
    })
}

pub(super) fn explanation_reason_value(reason: DecisionReason) -> Value {
    json!({
        "inputId": reason.input_id().map(|value| value.to_string()),
        "code": reason_name(reason.code())
    })
}

pub(super) fn evidence_value(input: &ValuationInput) -> Value {
    let evidence = input.evidence();
    json!({
        "inputId": input.id().to_string(),
        "subjectInstrumentId": input.subject_instrument_id().to_string(),
        "referenceInstrumentId": input.reference_instrument_id().to_string(),
        "relationship": relation_name(input.relationship()),
        "amount": amount_value(input.amount()),
        "significance": significance_name(input.significance()),
        "observability": observability_name(input.observability()),
        "adjustment": adjustment_name(input.adjustment()),
        "marketActivity": activity_name(input.market_activity()),
        "marketAccess": access_name(input.market_access()),
        "marketAccessAssessment": input
            .market_access_assessment()
            .map(market_access_assessment_value),
        "dataQuality": quality_name(input.data_quality()),
        "useAssessment": input.use_assessment().map(|assessment| {
            json!({
                "assessmentHash": assessment.hash().to_string(),
                "subjectInstrumentId": assessment.subject_instrument_id().to_string(),
                "relationship": relation_name(assessment.relationship()),
                "observability": observability_name(assessment.observability()),
                "adjustment": adjustment_name(assessment.adjustment()),
                "rationale": assessment.rationale(),
                "assessedBy": assessment.assessed_by().as_str(),
                "assessedAt": timestamp_value(assessment.assessed_at())
            })
        }),
        "evidence": fair_value_evidence_value(evidence)
    })
}

pub(super) fn approval_value(
    approval: &ValuationApproval,
    status: ApprovalStatus,
    revocation: Option<&ApprovalRevocation>,
) -> Value {
    json!({
        "approvalId": approval.id().to_string(),
        "decisionId": approval.decision_id().to_string(),
        "measurementId": approval.measurement_id().to_string(),
        "overrideId": approval.override_id().map(|value| value.to_string()),
        "approvedBy": approval.approved_by().as_str(),
        "approvedAt": timestamp_value(approval.approved_at()),
        "expiresAt": timestamp_value(approval.expires_at()),
        "status": approval_status_name(status),
        "revocation": revocation.map(|value| {
            json!({
                "revocationId": value.id().to_string(),
                "approvalId": value.approval_id().to_string(),
                "revokedBy": value.revoked_by().as_str(),
                "revokedAt": timestamp_value(value.revoked_at()),
                "reason": value.reason()
            })
        })
    })
}

pub(super) fn timestamp_value(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn fair_value_evidence_value(evidence: &FairValueEvidence) -> Value {
    json!({
        "evidenceHash": evidence.hash().to_string(),
        "sourceId": evidence.source_id().as_str(),
        "sourceIdentifier": evidence.source_identifier().as_str(),
        "payloadDigest": digest_value(evidence.payload_digest()),
        "origin": origin_value(evidence.origin()),
        "sourceTimestamp": evidence.source_timestamp().map(timestamp_value),
        "effectiveAt": evidence.effective_at().map(timestamp_value),
        "publishedAt": evidence.published_at().map(timestamp_value),
        "availableAt": evidence.available_at().map(timestamp_value),
        "receivedAt": evidence.received_at().map(timestamp_value),
        "qualificationEvaluatedAt": evidence
            .qualification_evaluated_at()
            .map(timestamp_value),
        "qualificationValidUntil": evidence
            .qualification_valid_until()
            .map(timestamp_value),
        "ingestedAt": timestamp_value(evidence.ingested_at()),
        "verification": match evidence.verification() {
            EvidenceVerification::Verified => "verified",
            EvidenceVerification::Unverified => "unverified"
        }
    })
}

fn origin_value(origin: &EvidenceOrigin) -> Value {
    match origin {
        EvidenceOrigin::Market {
            venue_id,
            assessment_id,
            binding_digest,
            canonical_state_digest,
            committed_state_revision,
            definition_revision,
            activity_policy_hash,
            activity_set_hash,
        } => json!({
            "kind": "live",
            "venueId": venue_id.as_str(),
            "assessmentId": assessment_id.as_str(),
            "bindingDigest": encode_hex(*binding_digest),
            "canonicalStateDigest": digest_value(*canonical_state_digest),
            "committedStateRevision": committed_state_revision,
            "definitionRevision": definition_revision,
            "activityPolicyHash": encode_hex(*activity_policy_hash),
            "activitySetHash": encode_hex(*activity_set_hash)
        }),
        EvidenceOrigin::Research {
            manifest,
            object_graph_digest,
            query_identity,
            result_digest,
            row,
            revision,
        } => json!({
            "kind": "research",
            "manifest": manifest_value(manifest),
            "objectGraphDigest": digest_value(*object_graph_digest),
            "queryIdentity": digest_value(*query_identity),
            "resultDigest": digest_value(*result_digest),
            "row": row,
            "revision": revision
        }),
        EvidenceOrigin::Analytics {
            feature_key,
            semantic_digest,
            manifest,
            object_graph_digest,
            query_identity,
            result_digest,
            row,
            revision,
        } => json!({
            "kind": "analytics",
            "feature": {
                "name": feature_key.name(),
                "version": feature_key.version().get(),
                "semanticDigest": encode_hex(*semantic_digest)
            },
            "manifest": manifest_value(manifest),
            "objectGraphDigest": digest_value(*object_graph_digest),
            "queryIdentity": digest_value(*query_identity),
            "resultDigest": digest_value(*result_digest),
            "row": row,
            "revision": revision
        }),
        EvidenceOrigin::Portfolio {
            revision,
            account_id,
            position_quantity,
            point_in_time_digest,
        } => json!({
            "kind": "portfolio",
            "revision": encode_hex(*revision),
            "accountId": account_id.to_string(),
            "positionQuantity": position_quantity.to_string(),
            "pointInTimeDigest": encode_hex(*point_in_time_digest)
        }),
    }
}

pub(super) fn market_access_assessment_value(
    assessment: &market_squawk_valuation::ApprovedMarketAccess,
) -> Value {
    json!({
        "assessmentId": assessment.id().to_string(),
        "accountId": assessment.account_id().to_string(),
        "venueId": assessment.venue_id().as_str(),
        "instrumentId": assessment.instrument_id().to_string(),
        "conclusion": access_name(assessment.conclusion()),
        "effectiveFrom": timestamp_value(assessment.effective_from()),
        "effectiveUntil": timestamp_value(assessment.effective_until()),
        "rationale": assessment.rationale(),
        "preparedBy": assessment.prepared_by().as_str(),
        "preparedAt": timestamp_value(assessment.prepared_at()),
        "approvedBy": assessment.approved_by().as_str(),
        "approvedAt": timestamp_value(assessment.approved_at()),
        "supersedes": assessment.supersedes().map(|value| value.to_string())
    })
}

fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "datasetId": manifest.dataset_id().as_str(),
        "manifestVersion": manifest.manifest_version(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema_version().get(),
            "fingerprint": encode_hex(manifest.schema().fingerprint())
        },
        "contentHash": encode_hex(manifest.content_hash().bytes())
    })
}

fn digest_value(digest: EvidenceDigest) -> Value {
    json!({
        "algorithm": match digest.algorithm() {
            DigestAlgorithm::Sha256 => "sha256",
            DigestAlgorithm::Blake3 => "blake3"
        },
        "digest": encode_hex(digest.bytes())
    })
}

fn amount_value(amount: ValuationAmount) -> Value {
    json!({
        "amount": amount.money().amount().to_string(),
        "currency": amount.money().currency().as_str(),
        "scale": amount.scale(),
        "amountBasis": match amount.basis() {
            ValuationAmountBasis::PerInstrumentUnit => "per_instrument_unit",
            ValuationAmountBasis::ReportingEntityTotal => "reporting_entity_total",
            ValuationAmountBasis::PositionTotal => "position_total",
        }
    })
}

fn basis_value(basis: DecisionBasis) -> Value {
    match basis {
        DecisionBasis::Rules => json!({"kind": "rules"}),
        DecisionBasis::Override {
            base_decision_id,
            override_id,
        } => json!({
            "kind": "override",
            "baseDecisionId": base_decision_id.to_string(),
            "overrideId": override_id.to_string()
        }),
    }
}

const fn hierarchy_name(value: FairValueHierarchy) -> &'static str {
    match value {
        FairValueHierarchy::Level1 => "level_1",
        FairValueHierarchy::Level2 => "level_2",
        FairValueHierarchy::Level3 => "level_3",
        FairValueHierarchy::Unclassified => "unclassified",
    }
}

const fn method_name(value: ValuationMethod) -> &'static str {
    match value {
        ValuationMethod::QuotedMarketPrice => "quoted_market_price",
        ValuationMethod::MarketApproach => "market_approach",
        ValuationMethod::IncomeApproach => "income_approach",
        ValuationMethod::CostApproach => "cost_approach",
    }
}

const fn relation_name(value: InputInstrumentRelation) -> &'static str {
    match value {
        InputInstrumentRelation::Identical => "identical",
        InputInstrumentRelation::Similar => "similar",
        InputInstrumentRelation::Proxy => "proxy",
    }
}

const fn significance_name(value: InputSignificance) -> &'static str {
    match value {
        InputSignificance::Significant => "significant",
        InputSignificance::NotSignificant => "not_significant",
    }
}

const fn observability_name(value: InputObservability) -> &'static str {
    match value {
        InputObservability::QuotedPrice => "quoted_price",
        InputObservability::Observable => "observable",
        InputObservability::Unobservable => "unobservable",
    }
}

const fn adjustment_name(value: PriceAdjustment) -> &'static str {
    match value {
        PriceAdjustment::None => "none",
        PriceAdjustment::Observable => "observable",
        PriceAdjustment::Unobservable => "unobservable",
    }
}

const fn activity_name(value: MarketActivity) -> &'static str {
    match value {
        MarketActivity::Active => "active",
        MarketActivity::Inactive => "inactive",
        MarketActivity::NotAssessed => "not_assessed",
    }
}

const fn access_name(value: MarketAccess) -> &'static str {
    match value {
        MarketAccess::Accessible => "accessible",
        MarketAccess::Inaccessible => "inaccessible",
        MarketAccess::NotAssessed => "not_assessed",
    }
}

const fn quality_name(value: DataQuality) -> &'static str {
    match value {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

const fn approval_status_name(value: ApprovalStatus) -> &'static str {
    match value {
        ApprovalStatus::NotYetEffective => "not_yet_effective",
        ApprovalStatus::Active => "active",
        ApprovalStatus::Expired => "expired",
        ApprovalStatus::Revoked => "revoked",
    }
}

const fn predicate_name(value: Predicate) -> &'static str {
    match value {
        Predicate::SignificantInput => "significant_input",
        Predicate::SubjectInstrumentMatches => "subject_instrument_matches",
        Predicate::IdenticalInstrument => "identical_instrument",
        Predicate::QuotedPrice => "quoted_price",
        Predicate::UnadjustedPrice => "unadjusted_price",
        Predicate::ActiveMarket => "active_market",
        Predicate::AccessibleMarket => "accessible_market",
        Predicate::MeasurementDateRelevant => "measurement_date_relevant",
        Predicate::WithinFreshnessLimit => "within_freshness_limit",
        Predicate::SourceEvidenceVerified => "source_evidence_verified",
        Predicate::VenueEvidencePresent => "venue_evidence_present",
        Predicate::CurrencyMatches => "currency_matches",
        Predicate::ScaleMatches => "scale_matches",
        Predicate::AmountMatches => "amount_matches",
        Predicate::QualitySupportsLevel1 => "quality_supports_level_1",
    }
}

const fn reason_name(value: DecisionReasonCode) -> &'static str {
    match value {
        DecisionReasonCode::NoSignificantInput => "no_significant_input",
        DecisionReasonCode::NotIdenticalInstrument => "not_identical_instrument",
        DecisionReasonCode::NotQuotedPrice => "not_quoted_price",
        DecisionReasonCode::AdjustedPrice => "adjusted_price",
        DecisionReasonCode::MarketNotActive => "market_not_active",
        DecisionReasonCode::MarketNotAccessible => "market_not_accessible",
        DecisionReasonCode::PostMeasurementEvidence => "post_measurement_evidence",
        DecisionReasonCode::EvidenceTooOld => "evidence_too_old",
        DecisionReasonCode::SourceEvidenceUnverified => "source_evidence_unverified",
        DecisionReasonCode::VenueEvidenceMissing => "venue_evidence_missing",
        DecisionReasonCode::CurrencyMismatch => "currency_mismatch",
        DecisionReasonCode::ScaleMismatch => "scale_mismatch",
        DecisionReasonCode::AmountMismatch => "amount_mismatch",
        DecisionReasonCode::QualityNotLevel1 => "quality_not_level_1",
        DecisionReasonCode::EvidenceStale => "evidence_stale",
        DecisionReasonCode::EvidenceQuarantined => "evidence_quarantined",
        DecisionReasonCode::UnobservableSignificantInput => "unobservable_significant_input",
        DecisionReasonCode::OverrideApplied => "override_applied",
    }
}
