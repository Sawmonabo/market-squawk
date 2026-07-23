//! Bounded canonical payload decoding and semantic state reconstruction.

use super::*;

pub(crate) fn recover(
    snapshot: &FairValueCatalogSnapshot,
) -> Result<RecoveredState, FairValueError> {
    validate_operation_coverage(snapshot)?;
    let mut evidence = BTreeMap::new();
    let mut input_payloads: BTreeMap<InputId, InputPayload> = BTreeMap::new();
    let mut measurement_payloads: BTreeMap<MeasurementId, MeasurementPayload> = BTreeMap::new();
    let mut decision_payloads: BTreeMap<DecisionId, DecisionPayload> = BTreeMap::new();
    let mut override_payloads: BTreeMap<OverrideId, OverridePayload> = BTreeMap::new();
    let mut approval_payloads: BTreeMap<ValuationApprovalId, ApprovalPayload> = BTreeMap::new();
    let mut revocation_payloads: BTreeMap<ApprovalRevocationId, RevocationPayload> =
        BTreeMap::new();
    let mut market_access = BTreeMap::new();
    for record in snapshot.records() {
        match record.kind() {
            FairValueRecordKind::Evidence => {
                let value = evidence_from_payload(canonical(record.payload())?)?;
                ensure_id(value.hash().bytes(), record.id())?;
                insert_unique(&mut evidence, value.hash(), value)?;
            }
            FairValueRecordKind::Input => {
                insert_unique(
                    &mut input_payloads,
                    InputId(record.id()),
                    canonical(record.payload())?,
                )?;
            }
            FairValueRecordKind::Measurement => {
                insert_unique(
                    &mut measurement_payloads,
                    MeasurementId(record.id()),
                    canonical(record.payload())?,
                )?;
            }
            FairValueRecordKind::Decision => {
                insert_unique(
                    &mut decision_payloads,
                    DecisionId(record.id()),
                    canonical(record.payload())?,
                )?;
            }
            FairValueRecordKind::Override => {
                insert_unique(
                    &mut override_payloads,
                    OverrideId(record.id()),
                    canonical(record.payload())?,
                )?;
            }
            FairValueRecordKind::Approval => {
                insert_unique(
                    &mut approval_payloads,
                    ValuationApprovalId(record.id()),
                    canonical(record.payload())?,
                )?;
            }
            FairValueRecordKind::Revocation => {
                insert_unique(
                    &mut revocation_payloads,
                    ApprovalRevocationId(record.id()),
                    canonical(record.payload())?,
                )?;
            }
            FairValueRecordKind::MarketAccess => {
                let value = access_from_payload(canonical(record.payload())?)?;
                ensure_id(value.id().bytes(), record.id())?;
                insert_unique(&mut market_access, value.id(), std::sync::Arc::new(value))?;
            }
        }
    }
    crate::access::validate_market_access_lineage(&market_access)?;
    let mut inputs = BTreeMap::new();
    for (id, payload) in input_payloads {
        let value = input_from_payload(payload, &evidence, &market_access)?;
        ensure_id(value.id().bytes(), id.bytes())?;
        insert_unique(&mut inputs, id, value)?;
    }
    let mut measurements = BTreeMap::new();
    for (id, payload) in measurement_payloads {
        let value = measurement_from_payload(payload, &inputs)?;
        ensure_id(value.id().bytes(), id.bytes())?;
        insert_unique(&mut measurements, id, std::sync::Arc::new(value))?;
    }
    let mut decisions = BTreeMap::new();
    for (id, payload) in &decision_payloads {
        if let DecisionPayload::Rules {
            version,
            measurement_id,
            max_quote_age_nanos,
            ruleset_version,
        } = payload
        {
            ensure_version(*version)?;
            let measurement = measurements
                .get(&MeasurementId(*measurement_id))
                .ok_or(FairValueError::CorruptPersistence)?;
            let value = ClassificationRuleset::versioned(
                ruleset_version.unwrap_or(1),
                *max_quote_age_nanos,
            )?
            .classify(measurement)?;
            ensure_id(value.id().bytes(), id.bytes())?;
            insert_unique(&mut decisions, *id, std::sync::Arc::new(value))?;
        }
    }
    let mut overrides = BTreeMap::new();
    for (id, payload) in override_payloads {
        ensure_version(payload.version)?;
        let base = decisions
            .get(&DecisionId(payload.base_decision_id))
            .ok_or(FairValueError::CorruptPersistence)?;
        if base.basis() != DecisionBasis::Rules {
            return Err(FairValueError::CorruptPersistence);
        }
        let measurement = measurements
            .get(&base.measurement_id())
            .ok_or(FairValueError::CorruptPersistence)?;
        if Timestamp::from_unix_nanos(payload.prepared_at_ns) < measurement.prepared_at() {
            return Err(FairValueError::CorruptPersistence);
        }
        let value = ValuationOverride::try_new(
            base,
            hierarchy_from_tag(payload.requested_hierarchy)?,
            &payload.justification,
            actor(&payload.prepared_by)?,
            Timestamp::from_unix_nanos(payload.prepared_at_ns),
            Timestamp::from_unix_nanos(payload.expires_at_ns),
        )?;
        ensure_id(value.id().bytes(), id.bytes())?;
        insert_unique(&mut overrides, id, std::sync::Arc::new(value))?;
    }
    for (id, payload) in decision_payloads {
        if let DecisionPayload::Override {
            version,
            base_decision_id,
            override_id,
        } = payload
        {
            ensure_version(version)?;
            let base = decisions
                .get(&DecisionId(base_decision_id))
                .ok_or(FairValueError::CorruptPersistence)?;
            let value_override = overrides
                .get(&OverrideId(override_id))
                .ok_or(FairValueError::CorruptPersistence)?;
            let value = ClassificationDecision::overridden(
                base,
                value_override.id(),
                value_override.requested_hierarchy(),
            )?;
            ensure_id(value.id().bytes(), id.bytes())?;
            insert_unique(&mut decisions, id, std::sync::Arc::new(value))?;
        }
    }
    let mut approvals = BTreeMap::new();
    for (id, payload) in approval_payloads {
        ensure_version(payload.version)?;
        let decision = decisions
            .get(&DecisionId(payload.decision_id))
            .ok_or(FairValueError::CorruptPersistence)?;
        let measurement = measurements
            .get(&decision.measurement_id())
            .ok_or(FairValueError::CorruptPersistence)?;
        let approved_by = actor(&payload.approved_by)?;
        let approved_at = Timestamp::from_unix_nanos(payload.approved_at_ns);
        let expires_at = Timestamp::from_unix_nanos(payload.expires_at_ns);
        if approved_at < measurement.prepared_at() || &approved_by == measurement.prepared_by() {
            return Err(FairValueError::CorruptPersistence);
        }
        if let DecisionBasis::Override { override_id, .. } = decision.basis() {
            let value_override = overrides
                .get(&override_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            if &approved_by == value_override.prepared_by()
                || approved_at < value_override.prepared_at()
                || expires_at > value_override.expires_at()
            {
                return Err(FairValueError::CorruptPersistence);
            }
        }
        let value = ValuationApproval::try_new(
            decision,
            match decision.basis() {
                DecisionBasis::Rules => None,
                DecisionBasis::Override { override_id, .. } => Some(override_id),
            },
            approved_by,
            approved_at,
            expires_at,
        )?;
        ensure_id(value.id().bytes(), id.bytes())?;
        insert_unique(&mut approvals, id, std::sync::Arc::new(value))?;
    }
    let mut revocations = BTreeMap::new();
    for (id, payload) in revocation_payloads {
        ensure_version(payload.version)?;
        let approval_id = ValuationApprovalId(payload.approval_id);
        let approval = approvals
            .get(&approval_id)
            .ok_or(FairValueError::CorruptPersistence)?;
        let value = ApprovalRevocation::try_new(
            approval,
            actor(&payload.revoked_by)?,
            Timestamp::from_unix_nanos(payload.revoked_at_ns),
            &payload.reason,
        )?;
        ensure_id(value.id().bytes(), id.bytes())?;
        if revocations
            .insert(approval_id, std::sync::Arc::new(value))
            .is_some()
        {
            return Err(FairValueError::CorruptPersistence);
        }
    }
    Ok(RecoveredState {
        measurements,
        decisions,
        overrides,
        approvals,
        revocations,
        market_access,
    })
}

fn evidence_from_payload(payload: EvidencePayload) -> Result<FairValueEvidence, FairValueError> {
    ensure_version(payload.version)?;
    FairValueEvidence::try_from_parts(FairValueEvidenceParts {
        source_id: SourceId::try_from(payload.source_id.as_str())
            .map_err(|_| FairValueError::CorruptPersistence)?,
        source_identifier: SourceIdentifier::try_from(payload.source_identifier.as_str())
            .map_err(|_| FairValueError::CorruptPersistence)?,
        payload_digest: digest(payload.payload_algorithm, payload.payload_digest)?,
        origin: origin_from_payload(payload.origin)?,
        source_timestamp: time(payload.source_timestamp_ns),
        effective_at: time(payload.effective_at_ns),
        published_at: time(payload.published_at_ns),
        available_at: time(payload.available_at_ns),
        received_at: time(payload.received_at_ns),
        qualification_evaluated_at: time(payload.qualification_evaluated_at_ns),
        qualification_valid_until: time(payload.qualification_valid_until_ns),
        ingested_at: Timestamp::from_unix_nanos(payload.ingested_at_ns),
        verification: match payload.verification {
            1 => EvidenceVerification::Verified,
            2 => EvidenceVerification::Unverified,
            _ => return Err(FairValueError::CorruptPersistence),
        },
    })
}

fn origin_from_payload(value: OriginPayload) -> Result<EvidenceOrigin, FairValueError> {
    Ok(match value {
        OriginPayload::Market {
            venue_id,
            assessment_id,
            binding_digest,
            canonical_state_algorithm,
            canonical_state_digest,
            committed_state_revision,
            definition_revision,
            activity_policy_hash,
            activity_set_hash,
        } => EvidenceOrigin::Market {
            venue_id: VenueId::try_from(venue_id.as_str())
                .map_err(|_| FairValueError::CorruptPersistence)?,
            assessment_id: SourceIdentifier::try_from(assessment_id.as_str())
                .map_err(|_| FairValueError::CorruptPersistence)?,
            binding_digest,
            canonical_state_digest: digest(canonical_state_algorithm, canonical_state_digest)?,
            committed_state_revision,
            definition_revision,
            activity_policy_hash,
            activity_set_hash,
        },
        OriginPayload::Research {
            manifest,
            object_graph_algorithm,
            object_graph_digest,
            query_algorithm,
            query_digest,
            result_algorithm,
            result_digest,
            row,
            revision,
        } => EvidenceOrigin::Research {
            manifest: manifest_from_payload(manifest)?,
            object_graph_digest: digest(object_graph_algorithm, object_graph_digest)?,
            query_identity: digest(query_algorithm, query_digest)?,
            result_digest: digest(result_algorithm, result_digest)?,
            row: usize::try_from(row).map_err(|_| FairValueError::CorruptPersistence)?,
            revision,
        },
        OriginPayload::Analytics {
            feature_name,
            feature_version,
            semantic_digest,
            manifest,
            object_graph_algorithm,
            object_graph_digest,
            query_algorithm,
            query_digest,
            result_algorithm,
            result_digest,
            row,
            revision,
        } => EvidenceOrigin::Analytics {
            feature_key: FeatureKey::try_new(
                &feature_name,
                NonZeroU32::new(feature_version).ok_or(FairValueError::CorruptPersistence)?,
            )
            .map_err(|_| FairValueError::CorruptPersistence)?,
            semantic_digest,
            manifest: manifest_from_payload(manifest)?,
            object_graph_digest: digest(object_graph_algorithm, object_graph_digest)?,
            query_identity: digest(query_algorithm, query_digest)?,
            result_digest: digest(result_algorithm, result_digest)?,
            row: usize::try_from(row).map_err(|_| FairValueError::CorruptPersistence)?,
            revision,
        },
        OriginPayload::Portfolio {
            revision,
            account_id,
            quantity_mantissa,
            quantity_scale,
            point_in_time_digest,
        } => EvidenceOrigin::Portfolio {
            revision,
            account_id: account_id
                .parse()
                .map_err(|_| FairValueError::CorruptPersistence)?,
            position_quantity: decimal(&quantity_mantissa, quantity_scale)?,
            point_in_time_digest,
        },
    })
}

fn input_from_payload(
    payload: InputPayload,
    evidence: &BTreeMap<FairValueEvidenceHash, FairValueEvidence>,
    access: &BTreeMap<MarketAccessAssessmentId, std::sync::Arc<ApprovedMarketAccess>>,
) -> Result<ValuationInput, FairValueError> {
    ensure_version(payload.version)?;
    let market_access_assessment = payload
        .market_access_id
        .map(|id| {
            access
                .get(&MarketAccessAssessmentId(id))
                .map(|value| value.as_ref().clone())
                .ok_or(FairValueError::CorruptPersistence)
        })
        .transpose()?;
    ValuationInput::try_from_spec(ValuationInputSpec {
        subject_instrument_id: instrument(&payload.subject_instrument_id)?,
        reference_instrument_id: instrument(&payload.reference_instrument_id)?,
        relationship: relation_from_tag(payload.relationship)?,
        amount: amount_from_payload(payload.amount)?,
        significance: significance_from_tag(payload.significance)?,
        observability: observability_from_tag(payload.observability)?,
        adjustment: adjustment_from_tag(payload.adjustment)?,
        market_activity: activity_from_tag(payload.market_activity)?,
        market_access: access_from_tag(payload.market_access)?,
        market_access_assessment,
        data_quality: quality_from_tag(payload.data_quality)?,
        evidence: evidence
            .get(&FairValueEvidenceHash(payload.evidence_id))
            .cloned()
            .ok_or(FairValueError::CorruptPersistence)?,
        use_assessment: payload
            .use_assessment
            .map(use_assessment_from_payload)
            .transpose()?,
    })
}

fn measurement_from_payload(
    payload: MeasurementPayload,
    inputs: &BTreeMap<InputId, ValuationInput>,
) -> Result<ValuationMeasurement, FairValueError> {
    ensure_version(payload.version)?;
    let values = payload
        .input_ids
        .into_iter()
        .map(|id| {
            inputs
                .get(&InputId(id))
                .cloned()
                .ok_or(FairValueError::CorruptPersistence)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ValuationMeasurement::try_new(ValuationMeasurementSpec {
        account_id: payload
            .account_id
            .parse()
            .map_err(|_| FairValueError::CorruptPersistence)?,
        instrument_id: instrument(&payload.instrument_id)?,
        amount: amount_from_payload(payload.amount)?,
        measurement_at: Timestamp::from_unix_nanos(payload.measurement_at_ns),
        prepared_at: Timestamp::from_unix_nanos(payload.prepared_at_ns),
        prepared_by: actor(&payload.prepared_by)?,
        method: method_from_tag(payload.method)?,
        inputs: values,
    })
}

fn access_from_payload(
    payload: MarketAccessPayload,
) -> Result<ApprovedMarketAccess, FairValueError> {
    ensure_version(payload.version)?;
    ApprovedMarketAccess::try_new(
        payload
            .account_id
            .parse()
            .map_err(|_| FairValueError::CorruptPersistence)?,
        VenueId::try_from(payload.venue_id.as_str())
            .map_err(|_| FairValueError::CorruptPersistence)?,
        instrument(&payload.instrument_id)?,
        access_from_tag(payload.conclusion)?,
        Timestamp::from_unix_nanos(payload.effective_from_ns),
        Timestamp::from_unix_nanos(payload.effective_until_ns),
        &payload.rationale,
        actor(&payload.prepared_by)?,
        Timestamp::from_unix_nanos(payload.prepared_at_ns),
        actor(&payload.approved_by)?,
        Timestamp::from_unix_nanos(payload.approved_at_ns),
        payload.supersedes_id.map(MarketAccessAssessmentId),
    )
}

fn validate_operation_coverage(snapshot: &FairValueCatalogSnapshot) -> Result<(), FairValueError> {
    let retained = snapshot
        .records()
        .iter()
        .map(|value| (value.kind(), value.id()))
        .collect::<BTreeSet<_>>();
    let referenced = snapshot
        .audit()
        .iter()
        .flat_map(|event| event.records().iter().copied())
        .collect::<BTreeSet<_>>();
    if retained != referenced {
        return Err(FairValueError::CorruptPersistence);
    }
    Ok(())
}

fn canonical<T>(payload: &[u8]) -> Result<T, FairValueError>
where
    T: DeserializeOwned + Serialize,
{
    let value: T =
        serde_json::from_slice(payload).map_err(|_| FairValueError::CorruptPersistence)?;
    let canonical = serde_json::to_vec(&value).map_err(|_| FairValueError::CorruptPersistence)?;
    if canonical != payload {
        return Err(FairValueError::CorruptPersistence);
    }
    Ok(value)
}

pub(super) fn amount_payload(value: ValuationAmount) -> AmountPayload {
    AmountPayload {
        mantissa: value.money().amount().mantissa().to_string(),
        decimal_scale: value.money().amount().scale(),
        currency: value.money().currency().as_str().to_owned(),
        accounting_scale: value.scale(),
    }
}

fn amount_from_payload(value: AmountPayload) -> Result<ValuationAmount, FairValueError> {
    ValuationAmount::try_new(
        Money::new(
            decimal(&value.mantissa, value.decimal_scale)?,
            Currency::try_from(value.currency.as_str())
                .map_err(|_| FairValueError::CorruptPersistence)?,
        ),
        value.accounting_scale,
    )
}

pub(super) fn use_assessment_payload(value: &InputUseAssessment) -> UseAssessmentPayload {
    UseAssessmentPayload {
        subject_instrument_id: value.subject_instrument_id().to_string(),
        relationship: crate::measurement::relation_tag(value.relationship()),
        observability: crate::measurement::observability_tag(value.observability()),
        adjustment: crate::measurement::adjustment_tag(value.adjustment()),
        rationale: value.rationale().to_owned(),
        assessed_by: value.assessed_by().as_str().to_owned(),
        assessed_at_ns: value.assessed_at().unix_nanos(),
    }
}

fn use_assessment_from_payload(
    value: UseAssessmentPayload,
) -> Result<InputUseAssessment, FairValueError> {
    InputUseAssessment::try_new(
        instrument(&value.subject_instrument_id)?,
        relation_from_tag(value.relationship)?,
        observability_from_tag(value.observability)?,
        adjustment_from_tag(value.adjustment)?,
        &value.rationale,
        actor(&value.assessed_by)?,
        Timestamp::from_unix_nanos(value.assessed_at_ns),
    )
}

pub(super) fn manifest_payload(value: &DatasetManifestRef) -> ManifestPayload {
    ManifestPayload {
        dataset_id: value.dataset_id().as_str().to_owned(),
        manifest_version: value.manifest_version(),
        schema_name: value.schema().name().to_owned(),
        schema_version: value.schema_version().get(),
        schema_fingerprint: value.schema().fingerprint(),
        content_hash: value.content_hash().bytes(),
    }
}

fn manifest_from_payload(value: ManifestPayload) -> Result<DatasetManifestRef, FairValueError> {
    let schema = DatasetSchemaRef::try_new(
        value.schema_name,
        SchemaVersion::new(value.schema_version).map_err(|_| FairValueError::CorruptPersistence)?,
        value.schema_fingerprint,
    )
    .map_err(|_| FairValueError::CorruptPersistence)?;
    DatasetSchemaRegistry::local()
        .resolve(&schema)
        .map_err(|_| FairValueError::CorruptPersistence)?;
    DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(value.dataset_id.as_str())
            .map_err(|_| FairValueError::CorruptPersistence)?,
        value.manifest_version,
        schema,
        Sha256Digest::new(value.content_hash),
    )
    .map_err(|_| FairValueError::CorruptPersistence)
}

pub(super) fn digest_algorithm_tag(value: DigestAlgorithm) -> u8 {
    match value {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

fn digest(tag: u8, bytes: [u8; 32]) -> Result<EvidenceDigest, FairValueError> {
    if bytes == [0; 32] {
        return Err(FairValueError::CorruptPersistence);
    }
    Ok(EvidenceDigest::new(
        match tag {
            1 => DigestAlgorithm::Sha256,
            2 => DigestAlgorithm::Blake3,
            _ => return Err(FairValueError::CorruptPersistence),
        },
        bytes,
    ))
}

fn decimal(mantissa: &str, scale: u32) -> Result<Decimal, FairValueError> {
    Decimal::try_from_i128_with_scale(
        mantissa
            .parse()
            .map_err(|_| FairValueError::CorruptPersistence)?,
        scale,
    )
    .map_err(|_| FairValueError::CorruptPersistence)
}

fn actor(value: &str) -> Result<ActorId, FairValueError> {
    ActorId::try_from(value).map_err(|_| FairValueError::CorruptPersistence)
}

fn instrument(value: &str) -> Result<InstrumentId, FairValueError> {
    value
        .parse()
        .map_err(|_| FairValueError::CorruptPersistence)
}

fn time(value: Option<i64>) -> Option<Timestamp> {
    value.map(Timestamp::from_unix_nanos)
}

fn ensure_version(value: u16) -> Result<(), FairValueError> {
    if value == PAYLOAD_VERSION {
        Ok(())
    } else {
        Err(FairValueError::CorruptPersistence)
    }
}

fn ensure_id(actual: [u8; 32], expected: [u8; 32]) -> Result<(), FairValueError> {
    if actual == expected {
        Ok(())
    } else {
        Err(FairValueError::CorruptPersistence)
    }
}

fn insert_unique<K: Ord, V>(
    values: &mut BTreeMap<K, V>,
    key: K,
    value: V,
) -> Result<(), FairValueError> {
    if values.insert(key, value).is_some() {
        Err(FairValueError::CorruptPersistence)
    } else {
        Ok(())
    }
}

fn relation_from_tag(value: u8) -> Result<InputInstrumentRelation, FairValueError> {
    match value {
        1 => Ok(InputInstrumentRelation::Identical),
        2 => Ok(InputInstrumentRelation::Similar),
        3 => Ok(InputInstrumentRelation::Proxy),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

fn observability_from_tag(value: u8) -> Result<InputObservability, FairValueError> {
    match value {
        1 => Ok(InputObservability::QuotedPrice),
        2 => Ok(InputObservability::Observable),
        3 => Ok(InputObservability::Unobservable),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

fn significance_from_tag(value: u8) -> Result<InputSignificance, FairValueError> {
    match value {
        1 => Ok(InputSignificance::Significant),
        2 => Ok(InputSignificance::NotSignificant),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

fn adjustment_from_tag(value: u8) -> Result<PriceAdjustment, FairValueError> {
    match value {
        1 => Ok(PriceAdjustment::None),
        2 => Ok(PriceAdjustment::Observable),
        3 => Ok(PriceAdjustment::Unobservable),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

fn activity_from_tag(value: u8) -> Result<MarketActivity, FairValueError> {
    match value {
        1 => Ok(MarketActivity::Active),
        2 => Ok(MarketActivity::Inactive),
        3 => Ok(MarketActivity::NotAssessed),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

fn access_from_tag(value: u8) -> Result<MarketAccess, FairValueError> {
    match value {
        1 => Ok(MarketAccess::Accessible),
        2 => Ok(MarketAccess::Inaccessible),
        3 => Ok(MarketAccess::NotAssessed),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

fn method_from_tag(value: u8) -> Result<ValuationMethod, FairValueError> {
    match value {
        1 => Ok(ValuationMethod::QuotedMarketPrice),
        2 => Ok(ValuationMethod::MarketApproach),
        3 => Ok(ValuationMethod::IncomeApproach),
        4 => Ok(ValuationMethod::CostApproach),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

fn quality_from_tag(value: u8) -> Result<DataQuality, FairValueError> {
    match value {
        1 => Ok(DataQuality::DirectVerified),
        2 => Ok(DataQuality::DirectUnverified),
        3 => Ok(DataQuality::OfficialDelayed),
        4 => Ok(DataQuality::Aggregated),
        5 => Ok(DataQuality::Indicative),
        6 => Ok(DataQuality::Modeled),
        7 => Ok(DataQuality::Estimated),
        8 => Ok(DataQuality::Stale),
        9 => Ok(DataQuality::Quarantined),
        _ => Err(FairValueError::CorruptPersistence),
    }
}

pub(super) fn hierarchy_tag(value: FairValueHierarchy) -> u8 {
    match value {
        FairValueHierarchy::Level1 => 1,
        FairValueHierarchy::Level2 => 2,
        FairValueHierarchy::Level3 => 3,
        FairValueHierarchy::Unclassified => 4,
    }
}

fn hierarchy_from_tag(value: u8) -> Result<FairValueHierarchy, FairValueError> {
    match value {
        1 => Ok(FairValueHierarchy::Level1),
        2 => Ok(FairValueHierarchy::Level2),
        3 => Ok(FairValueHierarchy::Level3),
        4 => Ok(FairValueHierarchy::Unclassified),
        _ => Err(FairValueError::CorruptPersistence),
    }
}
