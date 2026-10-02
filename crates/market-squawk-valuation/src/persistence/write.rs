//! Canonical immutable records, relationships, and append operations.

use super::recovery::{
    amount_payload, digest_algorithm_tag, hierarchy_tag, manifest_payload,
    market_event_commit_payload, use_assessment_payload,
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
            ruleset_version: ruleset.version(),
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
        &input_payload(value),
    )
}

fn input_payload(value: &ValuationInput) -> InputPayload {
    InputPayload {
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
    }
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
        &market_access_payload(value),
    )
}

fn market_access_payload(value: &ApprovedMarketAccess) -> MarketAccessPayload {
    MarketAccessPayload {
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
        supersedes_id: value.supersedes().map(MarketAccessAssessmentId::bytes),
    }
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
        qualification_evaluated_at_ns: value
            .qualification_evaluated_at()
            .map(Timestamp::unix_nanos),
        qualification_valid_until_ns: value.qualification_valid_until().map(Timestamp::unix_nanos),
        ingested_at_ns: value.ingested_at().unix_nanos(),
        verification: match value.verification() {
            EvidenceVerification::Verified => 1,
            EvidenceVerification::Unverified => 2,
        },
    })
}

fn origin_payload(value: &EvidenceOrigin) -> Result<OriginPayload, FairValueError> {
    Ok(match value {
        EvidenceOrigin::ForecastDistribution { evidence } => {
            let value = evidence.source().reference();
            OriginPayload::ForecastDistribution {
                source: Box::new(ForecastReferencePayload {
                    identity: value.identity().bytes(),
                    distribution_identity: value.distribution_identity().bytes(),
                    vintage_id: value.vintage_id().bytes(),
                    forecast_artifact_hash: value.forecast_artifact_hash().bytes(),
                    metadata_hash: value.metadata_hash().bytes(),
                    instrument_id: value.instrument_id().to_string(),
                    training_manifest: manifest_payload(value.training_manifest()),
                    serving_manifest: manifest_payload(value.serving_manifest()),
                    parent_manifests: value
                        .parent_manifests()
                        .iter()
                        .map(manifest_payload)
                        .collect(),
                    serving_source: value.serving_source().as_str().to_owned(),
                    serving_graph: value.serving_graph().bytes(),
                    serving_query: value.serving_query().bytes(),
                    serving_result: value.serving_result().bytes(),
                    serving_feature: value.serving_feature().bytes(),
                    origin_bar_digest: match value.source_origin() {
                        crate::ForecastValuationOriginIdentity::CompletedBar(digest) => {
                            Some(digest.bytes())
                        }
                        crate::ForecastValuationOriginIdentity::FinancialEpoch(_)
                        | crate::ForecastValuationOriginIdentity::CurrentPriceEpoch(_) => None,
                    },
                    financial_epoch_digest: match value.source_origin() {
                        crate::ForecastValuationOriginIdentity::FinancialEpoch(digest) => {
                            Some(digest.bytes())
                        }
                        crate::ForecastValuationOriginIdentity::CompletedBar(_)
                        | crate::ForecastValuationOriginIdentity::CurrentPriceEpoch(_) => None,
                    },
                    current_price_epoch_digest: match value.source_origin() {
                        crate::ForecastValuationOriginIdentity::CurrentPriceEpoch(digest) => {
                            Some(digest.bytes())
                        }
                        _ => None,
                    },
                    knowledge_at_ns: value.knowledge_at().unix_nanos(),
                    selected_at_ns: value.selected_at().unix_nanos(),
                }),
                ordinal: evidence
                    .ordinal()
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| FairValueError::Arithmetic)?,
                financial_origin: evidence.selection()
                    == crate::ForecastValuationValueSelection::FinancialOrigin,
            }
        }
        EvidenceOrigin::PublishedMarket { evidence } => OriginPayload::PublishedMarket {
            commit: market_event_commit_payload(&evidence.commit),
            selection_digest: evidence.selection_digest.bytes(),
            publication_digest: evidence.publication_digest.bytes(),
            publication_row: evidence.publication_row,
            canonical_event_digest: evidence.canonical_event_digest.bytes(),
            canonical_event: evidence.canonical_event.to_string(),
            canonical_price_authority: evidence.canonical_price_authority.to_string(),
            definition_content: evidence.definition_content.bytes(),
            definition_audit: evidence.definition_audit.bytes(),
            knowledge_at_ns: evidence.knowledge_at.unix_nanos(),
            commit_available_at_ns: evidence.commit_available_at.unix_nanos(),
            origin_committed_at_ns: evidence.origin_committed_at.unix_nanos(),
        },
        EvidenceOrigin::Market {
            venue_id,
            assessment_id,
            binding_digest,
            canonical_state_digest,
            committed_state_revision,
            definition_revision,
            activity_policy_hash,
            activity_set_hash,
            publication,
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
            publication: publication.as_ref().map(|value| MarketPublicationPayload {
                qualified_input_id: value.qualified_input_id.bytes(),
                qualified_amount: amount_payload(value.qualified_amount),
                commit: market_event_commit_payload(&value.commit),
                selection_digest: value.selection_digest.bytes(),
                publication_digest: value.publication_digest.bytes(),
                publication_row: value.publication_row,
                coordinate_digest: value.coordinate_digest.bytes(),
                canonical_event_digest: value.canonical_event_digest.bytes(),
                canonical_event: value.canonical_event.to_string(),
                knowledge_at_ns: value.knowledge_at.unix_nanos(),
                commit_available_at_ns: value.commit_available_at.unix_nanos(),
                origin_committed_at_ns: value.origin_committed_at.unix_nanos(),
            }),
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
        EvidenceOrigin::Fundamental {
            manifest,
            origin_digest,
            request_digest,
            selection_digest,
            result_digest,
            company_security_digest,
            canonical_company_security,
            canonical_company_observation,
            company_observation_digest,
            row,
            canonical_row_digest,
            knowledge_at,
            generation_completed_at,
            canonical_observation,
        } => OriginPayload::Fundamental {
            manifest: manifest_payload(manifest),
            origin_digest: origin_digest.bytes(),
            request_digest: request_digest.bytes(),
            selection_digest: selection_digest.bytes(),
            result_digest: result_digest.bytes(),
            company_security_digest: company_security_digest.bytes(),
            canonical_company_security: canonical_company_security.to_string(),
            canonical_company_observation: canonical_company_observation.to_string(),
            company_observation_digest: company_observation_digest.bytes(),
            row: *row,
            canonical_row_digest: canonical_row_digest.bytes(),
            knowledge_at_ns: knowledge_at.unix_nanos(),
            generation_completed_at_ns: generation_completed_at.unix_nanos(),
            canonical_observation: canonical_observation.to_string(),
        },
        EvidenceOrigin::AutomaticValuation { receipt } => OriginPayload::AutomaticValuation {
            receipt: Box::new(automatic_receipt_payload(receipt)?),
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

fn automatic_receipt_payload(
    value: &crate::AutomaticValuationMethodReceipt,
) -> Result<AutomaticReceiptPayload, FairValueError> {
    let mut inputs = Vec::new();
    inputs
        .try_reserve_exact(value.inputs().len())
        .map_err(|_| FairValueError::Arithmetic)?;
    for input in value.inputs() {
        if matches!(
            input.input().evidence().origin(),
            EvidenceOrigin::AutomaticValuation { .. }
        ) {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        inputs.push(AutomaticInputPayload {
            input_id: input.input().id().bytes(),
            input: input_payload(input.input()),
            evidence: evidence_payload(input.input().evidence())?,
            market_access: input
                .input()
                .market_access_assessment()
                .map(market_access_payload),
            selection_receipt: input.selection_receipt().bytes(),
            rights_input_digest: input.rights_input_digest().bytes(),
            knowledge_at_ns: input.knowledge_at().unix_nanos(),
            expires_at_ns: input.expires_at().unix_nanos(),
        });
    }
    let assumptions = value
        .assumptions()
        .iter()
        .map(automatic_assumption_payload)
        .collect();
    let intermediates = value
        .intermediates()
        .iter()
        .map(|item| AutomaticIntermediatePayload {
            kind: match item.kind() {
                crate::AutomaticValuationIntermediateKind::DiscountedCashFlow => 1,
                crate::AutomaticValuationIntermediateKind::DiscountedTerminalValue => 2,
                crate::AutomaticValuationIntermediateKind::WeightedComparableMultiple => 3,
                crate::AutomaticValuationIntermediateKind::ComparableSubjectValue => 4,
                crate::AutomaticValuationIntermediateKind::DiscountedResidualIncome => 5,
                crate::AutomaticValuationIntermediateKind::ProbabilityWeightedForecast => 6,
            },
            sequence: item.sequence(),
            instrument_id: item.instrument_id().to_string(),
            primary_input: item.primary_input().bytes(),
            secondary_input: item.secondary_input().map(InputId::bytes),
            amount_mantissa: item.amount().mantissa().to_string(),
            amount_scale: item.amount().scale(),
            adjustment_mantissa: item.adjustment().mantissa().to_string(),
            adjustment_scale: item.adjustment().scale(),
            factor_mantissa: item.factor().mantissa().to_string(),
            factor_scale: item.factor().scale(),
            result_mantissa: item.result().mantissa().to_string(),
            result_scale: item.result().scale(),
            evidence: item.evidence().bytes(),
        })
        .collect();
    Ok(AutomaticReceiptPayload {
        id: value.id().bytes(),
        input_set_id: value.input_set_id().bytes(),
        method: match value.method() {
            crate::AutomaticValuationMethod::DiscountedCashFlow => 1,
            crate::AutomaticValuationMethod::ComparableCompanies => 2,
            crate::AutomaticValuationMethod::ResidualIncome => 3,
            crate::AutomaticValuationMethod::ForecastDistribution => 4,
        },
        periods_per_year: value.periods_per_year().map(NonZeroU32::get),
        account_id: value.account_id().to_string(),
        instrument_id: value.instrument_id().to_string(),
        company_security: identity_payload(value.company_security())?,
        peer_identities: value
            .peer_identities()
            .iter()
            .map(identity_payload)
            .collect::<Result<Vec<_>, _>>()?,
        rights_decision: value.rights_decision().bytes(),
        rights_graph: value.rights_graph().bytes(),
        rights_input_digest: value.rights_input_digest().bytes(),
        rights_expires_at_ns: value.rights_expires_at().unix_nanos(),
        admitted_input_manifests: value
            .admitted_input_manifests()
            .iter()
            .map(manifest_payload)
            .collect(),
        admitted_event_inputs: value.admitted_event_inputs().iter().map(|admission| {
            EventRightsAdmissionPayload {
                commit: market_event_commit_payload(admission.commit()),
                inputs: admission.inputs().iter().map(|input| EventUseInputPayload {
                    publication_digest: input.publication_digest().bytes(),
                    publication_kind: input.publication_kind().as_str().to_owned(),
                    row_ordinal: input.row_ordinal(),
                    coordinate_digest: input.coordinate_digest().bytes(),
                    canonical_event_digest: input.canonical_event_digest().bytes(),
                    source_id: input.source_id().as_str().to_owned(),
                    origin_committed_at_ns: input.origin_committed_at().unix_nanos(),
                }).collect(),
                rights_input_digest: admission.rights_input_digest().bytes(),
                decision_digest: admission.decision_digest().bytes(),
                evaluated_at_ns: admission.evaluated_at().unix_nanos(),
                expires_at_ns: admission.expires_at().unix_nanos(),
            }
        }).collect(),
        current_market_input: value.current_market_input().bytes(),
        method_base_input: value.method_base_input().map(InputId::bytes),
        inputs,
        assumptions,
        macro_assumptions: value.macro_assumptions().map(|binding| {
            let reference = binding.reference();
            AutomaticMacroAssumptionsPayload {
                maturity: match reference.maturity() {
                    crate::MacroRateMaturity::TenYear => 1,
                    crate::MacroRateMaturity::ThirtyYear => 2,
                },
                annual_yield_mantissa: reference.annual_yield_percent().mantissa().to_string(),
                annual_yield_scale: reference.annual_yield_percent().scale(),
                context_identity: reference.context_identity().bytes(),
                evidence_identity: reference.evidence_identity().bytes(),
                knowledge_cutoff_ns: reference.knowledge_cutoff().unix_nanos(),
                effective_date_cutoff: reference.effective_date_cutoff(),
                available_at_ns: reference.available_at().unix_nanos(),
                expires_at_ns: reference.expires_at().unix_nanos(),
                premium: automatic_assumption_payload(binding.premium()),
                premium_source: binding.premium_source_reference().map(<[u8]>::to_vec),
                premium_parents: binding
                    .premium_parent_manifests()
                    .iter()
                    .map(manifest_payload)
                    .collect(),
                rate: automatic_assumption_payload(binding.assumption()),
            }
        }),
        residual_terminal: value.residual_terminal().map(|condition| {
            ResidualIncomeTerminalPayload {
            convention: match condition.convention() {
                crate::ResidualIncomeTerminalConvention::ZeroAbnormalEarningsAfterExplicitHorizon => 1,
            },
            terminal_period: condition.terminal_period().get(),
            current_book_input: condition.current_book_input().bytes(),
            final_income_input: condition.final_income_input().bytes(),
            final_opening_book_input: condition.final_opening_book_input().bytes(),
            annual_rate_identity: condition.annual_rate_identity().bytes(),
            annual_cost_of_equity_mantissa: condition.annual_cost_of_equity().mantissa().to_string(),
            annual_cost_of_equity_scale: condition.annual_cost_of_equity().scale(),
            continuing_value_sensitivity_mantissa: condition.continuing_value_sensitivity().mantissa().to_string(),
            continuing_value_sensitivity_scale: condition.continuing_value_sensitivity().scale(),
            identity: condition.identity().bytes(),
            }
        }),
        intermediates,
        lower: amount_payload(value.range().lower()),
        central: amount_payload(value.range().central()),
        upper: amount_payload(value.range().upper()),
        rounding: value.arithmetic_policy().rounding(),
        maximum_periods: u32::try_from(value.arithmetic_policy().maximum_periods())
            .map_err(|_| FairValueError::Arithmetic)?,
        method_selection_receipt: value.method_selection_receipt().map(EvidenceDigest::bytes),
        forecast_horizon_nanos: value
            .forecast_horizon_nanos()
            .map(std::num::NonZeroU64::get),
        forecast_terminal_at_ns: value.forecast_terminal_at().map(Timestamp::unix_nanos),
        measurement_at_ns: value.measurement_at().unix_nanos(),
        calculated_at_ns: value.calculated_at().unix_nanos(),
        calculated_by: value.calculated_by().as_str().to_owned(),
        expires_at_ns: value.expires_at().unix_nanos(),
    })
}

fn identity_payload(
    value: &market_squawk_data::CompanySecurityIdentitySelectionReceipt,
) -> Result<String, FairValueError> {
    String::from_utf8(
        value
            .canonical_bytes()
            .map_err(|_| FairValueError::Persistence)?,
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

fn automatic_assumption_payload(
    item: &crate::AutomaticValuationAssumption,
) -> AutomaticAssumptionPayload {
    AutomaticAssumptionPayload {
        kind: match item.kind() {
            crate::AutomaticValuationAssumptionKind::DiscountRate => 1,
            crate::AutomaticValuationAssumptionKind::ComparableWeight => 2,
            crate::AutomaticValuationAssumptionKind::CostOfEquity => 3,
            crate::AutomaticValuationAssumptionKind::ForecastProbability => 4,
            crate::AutomaticValuationAssumptionKind::UncertaintyLower => 5,
            crate::AutomaticValuationAssumptionKind::UncertaintyUpper => 6,
            crate::AutomaticValuationAssumptionKind::TerminalGrowth => 7,
        },
        identifier: item.identifier().to_owned(),
        mantissa: item.value().mantissa().to_string(),
        scale: item.value().scale(),
        evidence: item.evidence().bytes(),
        available_at_ns: item.available_at().unix_nanos(),
        expires_at_ns: item.expires_at().unix_nanos(),
    }
}
