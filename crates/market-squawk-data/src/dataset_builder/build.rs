//! Bounded read, temporal admission, Arrow construction, and derived publication.

use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float64Array, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, ResearchObservation, ResearchTemporalCoordinate,
    SourceIdentifier,
};
use market_squawk_sources::CanonicalObservationPayload;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::model::{
    ComponentKind, ComponentValue, DatasetBuildRequest, DatasetExample, DatasetSplit,
    DatasetSplitCounts, FeatureLabelComponentInput, FeatureLabelDataset, MissingValuePolicy,
};
use super::{DatasetBuildError, DatasetBuilderService, canonical};
use crate::{
    ArtifactRecord, AuthorizedResearchUse, CorporateActionPlan, CorporateActionRecord,
    DatasetArrowBatch, DatasetManifestRecord, DatasetSchemaRegistry, DerivedOutputObjectInput,
    FeatureLabelBatchBindings, GenerationParentRelation, IngestIdentity, ManifestObject,
    ManifestPlan, PinnedDataset, PointInTimeCandidate, PointInTimeRequest, PointInTimeSelection,
    PointInTimeService, ResearchArrowBatch, ResearchUseRequest, Sha256Digest, SourceOperation,
    UniverseSnapshot,
};

#[derive(Debug)]
struct PreparedRows<'request> {
    rows: Vec<OutputRow<'request>>,
    split_counts: DatasetSplitCounts,
}

#[derive(Debug)]
struct OutputRow<'request> {
    example: &'request DatasetExample,
    split: DatasetSplit,
    component: &'request FeatureLabelComponentInput,
    lineage: Sha256Digest,
}

pub(super) async fn build(
    builder: &DatasetBuilderService<'_>,
    request: DatasetBuildRequest,
    cancellation: CancellationToken,
) -> Result<FeatureLabelDataset, DatasetBuildError> {
    let deadline = Instant::now()
        .checked_add(request.limits().max_duration())
        .ok_or(DatasetBuildError::DeadlineExceeded)?;
    check_control(&cancellation, deadline)?;
    authorize_research_use(builder, &request, &cancellation)?;
    if let Some(existing) = matching_existing(builder, &request)? {
        return Ok(result_from_existing(
            &request,
            expected_split_counts(&request)?,
            existing,
        ));
    }
    let (candidates, actions, input_bytes) =
        read_inputs(builder, &request, &cancellation, deadline).await?;
    ensure_retained_limit(
        &request,
        [
            input_bytes,
            input_bytes,
            request.retained_bytes(),
            candidates
                .capacity()
                .checked_mul(size_of::<PointInTimeCandidate>())
                .ok_or(DatasetBuildError::LimitExceeded)?,
        ],
    )?;
    let prepared = prepare_rows(&request, &candidates, &actions, &cancellation, deadline).await?;
    let row_bytes = prepared
        .rows
        .capacity()
        .checked_mul(size_of::<OutputRow<'_>>())
        .ok_or(DatasetBuildError::LimitExceeded)?;
    ensure_retained_limit(&request, [input_bytes, request.retained_bytes(), row_bytes])?;
    let (batch, lineage_digest) = feature_label_batch(&request, &prepared.rows)?;
    ensure_retained_limit(
        &request,
        [
            input_bytes,
            request.retained_bytes(),
            row_bytes,
            batch.record_batch().get_array_memory_size(),
        ],
    )?;
    check_control(&cancellation, deadline)?;

    let _operation = await_deadline(deadline, builder.operation_gate.acquire(&cancellation))
        .await?
        .ok_or(DatasetBuildError::Cancelled)?;
    check_control(&cancellation, deadline)?;
    let authorization = authorize_research_use(builder, &request, &cancellation)?;
    if let Some(existing) = matching_existing(builder, &request)? {
        return Ok(result_from_existing(
            &request,
            prepared.split_counts,
            existing,
        ));
    }
    check_control(&cancellation, deadline)?;
    let store = builder.service.object_store();
    let publication = await_deadline(deadline, store.begin_publication(&cancellation)).await??;
    let published = await_deadline(
        deadline,
        store.publish_dataset_under_lease(&batch, &cancellation, &publication),
    )
    .await??;
    if !store.verify(&published)? {
        return Err(DatasetBuildError::Parquet(
            crate::ParquetStoreError::ObjectMetadataMismatch,
        ));
    }
    check_control(&cancellation, deadline)?;

    let manifest_object = ManifestObject::try_new(
        published.content_hash(),
        published.row_count(),
        published.size_bytes(),
        lineage_digest,
    )?;
    let plan = ManifestPlan::derive(
        request.output_dataset().clone(),
        vec![manifest_object.clone()],
        1,
    )?;
    let schema = batch.schema_ref().clone();
    let dataset_name = SourceIdentifier::try_from(request.output_dataset().as_str())
        .map_err(|_| DatasetBuildError::InvalidRequest)?;
    let derived = {
        let authority = builder
            .authority
            .lock()
            .map_err(|_| DatasetBuildError::AuthorityLockPoisoned)?;
        let rights = authority.admit_source_rights(
            request
                .output_authorization()
                .rights_decision(published.content_hash(), published.created_at()),
        )?;
        let reservation = authority.reserve_ingest(
            &IngestIdentity::try_new(
                request.output_authorization().source_id().clone(),
                EvidenceDigest::new(DigestAlgorithm::Sha256, published.content_hash().bytes()),
                SourceOperation::Persist,
                output_idempotency_key(&request),
            )?,
            &rights,
        )?;
        let created_at = published.created_at().max(reservation.requested_at());
        let artifact = ArtifactRecord::try_new(
            published.relative_reference(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, published.content_hash().bytes()),
            published.size_bytes(),
            created_at,
        )?;
        let anchor = DatasetManifestRecord::try_new(
            dataset_name,
            schema.version(),
            artifact.artifact_id(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, plan.content_hash().bytes()),
            created_at,
        );
        let durable = authority.publish_artifact_manifest(&reservation, &artifact, &anchor)?;
        let bound = authority.bind_derived_output_object(
            &reservation,
            DerivedOutputObjectInput::try_new(
                durable.artifact().artifact_id(),
                published.content_hash(),
                published.row_count(),
                published.size_bytes(),
                lineage_digest,
            )?,
        )?;
        let input = authorization.prepare_derived_publication(
            request.build_spec_digest(),
            schema,
            plan,
            vec![bound],
            durable.artifact().artifact_id(),
        )?;
        check_control(&cancellation, deadline)?;
        authority.publish_derived_generation(input)?
    };
    drop(publication);
    check_control(&cancellation, deadline)?;
    let pinned = builder.service.pinned(derived.manifest())?;
    Ok(FeatureLabelDataset {
        pinned,
        build_spec_digest: request.build_spec_digest(),
        policy_digest: request.policy_digest(),
        universe_digest: request.universe_digest(),
        split_counts: prepared.split_counts,
    })
}

async fn read_inputs(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(Vec<PointInTimeCandidate>, Vec<CorporateActionRecord>, usize), DatasetBuildError> {
    let research_schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    let mut candidates = Vec::new();
    let mut actions = Vec::new();
    let mut input_rows = 0_usize;
    let mut retained_bytes = 0_usize;
    let store = builder.service.object_store();
    for parent in request.inputs().parents() {
        check_control(cancellation, deadline)?;
        let pinned = builder.service.pinned(parent)?;
        if pinned.manifest().schema() != &research_schema {
            return Err(DatasetBuildError::InvalidInputGeneration);
        }
        let batches =
            await_deadline(deadline, store.read_pinned_async(&pinned, cancellation)).await??;
        for batch in batches {
            input_rows = input_rows
                .checked_add(batch.num_rows())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            retained_bytes = retained_bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            if input_rows > request.limits().max_input_rows()
                || retained_bytes > request.limits().max_retained_bytes()
            {
                return Err(DatasetBuildError::LimitExceeded);
            }
            let observations = ResearchArrowBatch::try_from_record_batch(batch)?.observations()?;
            candidates
                .try_reserve(observations.len())
                .map_err(|_| DatasetBuildError::LimitExceeded)?;
            for observation in observations {
                if let ResearchObservation::CorporateAction(action) = &observation {
                    if actions.len() >= request.limits().corporate_actions().max_actions().get() {
                        return Err(DatasetBuildError::LimitExceeded);
                    }
                    actions
                        .try_reserve(1)
                        .map_err(|_| DatasetBuildError::LimitExceeded)?;
                    let payload = CanonicalObservationPayload::try_from_observation(&observation)
                        .map_err(|_| DatasetBuildError::PointInTime)?;
                    actions.push(CorporateActionRecord::new(
                        action.clone(),
                        parent.clone(),
                        canonical::corporate_action_evidence(parent, payload.identity()),
                    ));
                }
                candidates.push(PointInTimeCandidate::new(observation, parent.clone()));
            }
        }
    }
    if candidates.is_empty() {
        return Err(DatasetBuildError::InvalidInputGeneration);
    }
    Ok((candidates, actions, retained_bytes))
}

async fn prepare_rows<'request>(
    request: &'request DatasetBuildRequest,
    candidates: &[PointInTimeCandidate],
    actions: &[CorporateActionRecord],
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<PreparedRows<'request>, DatasetBuildError> {
    let mut rows = Vec::new();
    let row_capacity = request
        .inputs()
        .examples()
        .len()
        .checked_mul(request.inputs().component_specs().len())
        .ok_or(DatasetBuildError::LimitExceeded)?;
    rows.try_reserve(row_capacity)
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    let mut split_counts = DatasetSplitCounts::default();
    let selector = PointInTimeService::new();
    for example in request.inputs().examples() {
        check_control(cancellation, deadline)?;
        let split = request
            .policy()
            .split()
            .split_for(example.cutoff_at())
            .ok_or(DatasetBuildError::TemporalLeakage)?;
        let universe = UniverseSnapshot::try_build(
            request.inputs().universe_id().clone(),
            example.cutoff_at(),
            request.inputs().universe_memberships().to_vec(),
            request.limits().universe(),
        )?;
        if !universe.contains(example.instrument_id()) {
            return Err(DatasetBuildError::InstrumentOutsideUniverse);
        }
        let relevant_actions = actions_for_instrument(actions, example.instrument_id())?;
        let feature_actions = CorporateActionPlan::try_build(
            request.policy().corporate_actions(),
            example.cutoff_at(),
            example.cutoff_at(),
            relevant_actions.clone(),
            request.limits().corporate_actions(),
        )?;
        let label_actions = CorporateActionPlan::try_build(
            request.policy().corporate_actions(),
            example.label_cutoff_at(),
            example.label_cutoff_at(),
            relevant_actions,
            request.limits().corporate_actions(),
        )?;
        if !feature_actions.conflicts().is_empty() || !label_actions.conflicts().is_empty() {
            return Err(DatasetBuildError::UnresolvedCorporateAction);
        }
        let feature_request =
            point_in_time_request(request, example.cutoff_at(), example.cutoff_at(), None)?;
        let label_request = point_in_time_request(
            request,
            example.label_cutoff_at(),
            example.cutoff_at(),
            Some(example.label_cutoff_at()),
        )?;
        let features = selector
            .select(&feature_request, candidates, cancellation, deadline)
            .await
            .map_err(|_| DatasetBuildError::PointInTime)?;
        let labels = selector
            .select(&label_request, candidates, cancellation, deadline)
            .await
            .map_err(|_| DatasetBuildError::PointInTime)?;
        let mut example_rows = Vec::new();
        example_rows
            .try_reserve(example.components().len())
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        let mut drop_example = false;
        for component in example.components() {
            let (selection, action_plan) = match component.spec().kind() {
                ComponentKind::Feature => (&features, &feature_actions),
                ComponentKind::Label => (&labels, &label_actions),
            };
            let evidence = resolve_component_evidence(component, selection)?;
            if component.value().is_missing() {
                match request.policy().missing_values() {
                    MissingValuePolicy::Reject => {
                        return Err(DatasetBuildError::MissingValueRejected);
                    }
                    MissingValuePolicy::Preserve => {}
                    MissingValuePolicy::DropExample => drop_example = true,
                }
            }
            let lineage = canonical::row_lineage_digest(
                request,
                example,
                split,
                component,
                selection.content_identity(),
                selection.audit_identity(),
                &evidence,
                universe.content_hash(),
                universe.audit_hash(),
                action_plan.content_hash(),
                action_plan.audit_hash(),
            );
            example_rows.push(OutputRow {
                example,
                split,
                component,
                lineage,
            });
        }
        if !drop_example {
            split_counts.record(split);
            rows.extend(example_rows);
        }
        if rows.len() > request.limits().max_output_rows() {
            return Err(DatasetBuildError::LimitExceeded);
        }
    }
    if rows.is_empty() {
        return Err(DatasetBuildError::EmptyDataset);
    }
    Ok(PreparedRows { rows, split_counts })
}

fn point_in_time_request(
    request: &DatasetBuildRequest,
    as_of: market_squawk_domain::Timestamp,
    effective_cutoff: market_squawk_domain::Timestamp,
    label_cutoff: Option<market_squawk_domain::Timestamp>,
) -> Result<PointInTimeRequest, DatasetBuildError> {
    PointInTimeRequest::try_new(
        request.policy().point_in_time(),
        as_of,
        Some(ResearchTemporalCoordinate::exact(as_of)),
        ResearchTemporalCoordinate::exact(effective_cutoff),
        label_cutoff.map(ResearchTemporalCoordinate::exact),
        request.limits().point_in_time(),
    )
    .map_err(|_| DatasetBuildError::InvalidRequest)
}

fn actions_for_instrument(
    actions: &[CorporateActionRecord],
    instrument_id: InstrumentId,
) -> Result<Vec<CorporateActionRecord>, DatasetBuildError> {
    let mut relevant = Vec::new();
    for action in actions {
        if action.observation().context().provenance().instrument_id() == Some(instrument_id) {
            relevant
                .try_reserve(1)
                .map_err(|_| DatasetBuildError::LimitExceeded)?;
            relevant.push(action.clone());
        }
    }
    Ok(relevant)
}

fn resolve_component_evidence(
    component: &FeatureLabelComponentInput,
    selection: &PointInTimeSelection<'_>,
) -> Result<Vec<Sha256Digest>, DatasetBuildError> {
    let mut evidence = Vec::new();
    for selector in component.selectors() {
        let mut matches = selection.records().iter().filter(|record| {
            record
                .candidate()
                .family_key()
                .is_ok_and(|family| &family == selector.family())
        });
        let first = matches.next();
        if matches.next().is_some()
            || (component.value().is_missing() && first.is_some())
            || (!component.value().is_missing() && first.is_none())
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        if let Some(record) = first {
            evidence.push(record.evidence_identity());
        }
    }
    evidence.sort_unstable();
    evidence.dedup();
    Ok(evidence)
}

fn feature_label_batch(
    request: &DatasetBuildRequest,
    rows: &[OutputRow<'_>],
) -> Result<(DatasetArrowBatch, Sha256Digest), DatasetBuildError> {
    let registry = DatasetSchemaRegistry::local();
    let schema_ref = registry.canonical_feature_labels()?;
    let dataset = SourceIdentifier::try_from(request.output_dataset().as_str())
        .map_err(|_| DatasetBuildError::InvalidRequest)?;
    let schema = registry.bind_feature_labels(
        &schema_ref,
        &FeatureLabelBatchBindings::new(
            dataset,
            request.build_spec_digest().digest().bytes(),
            request.universe_digest().bytes(),
            request.policy_digest().bytes(),
        ),
    )?;
    let mut float_values = Vec::with_capacity(rows.len());
    let mut decimal_values = Vec::with_capacity(rows.len());
    let mut decimal_scales = Vec::with_capacity(rows.len());
    let mut units = Vec::with_capacity(rows.len());
    let mut currencies = Vec::with_capacity(rows.len());
    let mut missing = Vec::with_capacity(rows.len());
    for row in rows {
        match row.component.value() {
            ComponentValue::Float {
                value,
                unit,
                currency,
            } => {
                float_values.push(Some(*value));
                decimal_values.push(None);
                decimal_scales.push(None);
                units.push(unit.as_ref().map(|value| value.as_str().to_owned()));
                currencies.push(currency.map(|value| value.as_str().to_owned()));
                missing.push(None);
            }
            ComponentValue::Decimal {
                value,
                unit,
                currency,
            } => {
                float_values.push(None);
                decimal_values.push(Some(value.mantissa()));
                decimal_scales.push(Some(
                    u8::try_from(value.scale()).map_err(|_| DatasetBuildError::InvalidRequest)?,
                ));
                units.push(unit.as_ref().map(|value| value.as_str().to_owned()));
                currencies.push(currency.map(|value| value.as_str().to_owned()));
                missing.push(None);
            }
            ComponentValue::Missing { reason } => {
                float_values.push(None);
                decimal_values.push(None);
                decimal_scales.push(None);
                units.push(None);
                currencies.push(None);
                missing.push(Some(reason.as_str().to_owned()));
            }
        }
    }
    let decimal = Decimal128Array::from(decimal_values)
        .with_precision_and_scale(38, 0)
        .map_err(crate::ArrowConversionError::from)?;
    let lineages =
        FixedSizeBinaryArray::try_from_iter(rows.iter().map(|row| row.lineage.bytes().to_vec()))
            .map_err(crate::ArrowConversionError::from)?;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.example.example_id()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter()
                .map(|row| row.example.instrument_id().to_string()),
        )),
        Arc::new(
            TimestampNanosecondArray::from_iter_values(
                rows.iter().map(|row| row.example.cutoff_at().unix_nanos()),
            )
            .with_timezone_utc(),
        ),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.split.name()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.component.spec().kind().name()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.component.spec().name()),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.component.spec().version().get()),
        )),
        Arc::new(Float64Array::from(float_values)),
        Arc::new(decimal),
        Arc::new(UInt8Array::from(decimal_scales)),
        Arc::new(StringArray::from(units)),
        Arc::new(StringArray::from(currencies)),
        Arc::new(StringArray::from(missing)),
        Arc::new(lineages),
    ];
    let record_batch =
        RecordBatch::try_new(schema, arrays).map_err(crate::ArrowConversionError::from)?;
    let batch = DatasetArrowBatch::try_new(schema_ref, record_batch)?;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/feature-label-object-lineage/v1");
    hash.update(request.build_spec_digest().digest().bytes());
    for row in rows {
        hash.update(row.lineage.bytes());
    }
    Ok((batch, Sha256Digest::new(hash.finalize().into())))
}

fn matching_existing(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
) -> Result<Option<PinnedDataset>, DatasetBuildError> {
    let Some(existing) = builder
        .service
        .matching_derived_build(request.output_dataset(), request.build_spec_digest())?
    else {
        return Ok(None);
    };
    let feature_schema = DatasetSchemaRegistry::local().canonical_feature_labels()?;
    let parents_match = existing.parents().len() == request.inputs().parents().len()
        && existing
            .parents()
            .iter()
            .zip(request.inputs().parents())
            .all(|(retained, requested)| {
                retained.relation() == GenerationParentRelation::DerivedInput
                    && retained.manifest() == requested
            });
    if existing.manifest().schema() != &feature_schema || !parents_match {
        return Err(DatasetBuildError::InvalidInputGeneration);
    }
    Ok(Some(existing))
}

fn authorize_research_use(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    cancellation: &CancellationToken,
) -> Result<AuthorizedResearchUse, DatasetBuildError> {
    let authority = builder
        .authority
        .lock()
        .map_err(|_| DatasetBuildError::AuthorityLockPoisoned)?;
    authority
        .authorize_research_use(
            ResearchUseRequest::try_new(
                request.inputs().parents().to_vec(),
                request.intended_use(),
                request.research_use_limits(),
            )?,
            cancellation,
        )
        .map_err(Into::into)
}

fn expected_split_counts(
    request: &DatasetBuildRequest,
) -> Result<DatasetSplitCounts, DatasetBuildError> {
    let mut counts = DatasetSplitCounts::default();
    for example in request.inputs().examples() {
        let has_missing = example
            .components()
            .iter()
            .any(|component| component.value().is_missing());
        match (request.policy().missing_values(), has_missing) {
            (MissingValuePolicy::Reject, true) => {
                return Err(DatasetBuildError::MissingValueRejected);
            }
            (MissingValuePolicy::DropExample, true) => continue,
            (MissingValuePolicy::Reject, false)
            | (MissingValuePolicy::Preserve, _)
            | (MissingValuePolicy::DropExample, false) => {}
        }
        let split = request
            .policy()
            .split()
            .split_for(example.cutoff_at())
            .ok_or(DatasetBuildError::TemporalLeakage)?;
        counts.record(split);
    }
    Ok(counts)
}

fn ensure_retained_limit(
    request: &DatasetBuildRequest,
    retained: impl IntoIterator<Item = usize>,
) -> Result<(), DatasetBuildError> {
    let total = retained.into_iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(value)
            .ok_or(DatasetBuildError::LimitExceeded)
    })?;
    if total > request.limits().max_retained_bytes() {
        Err(DatasetBuildError::LimitExceeded)
    } else {
        Ok(())
    }
}

fn result_from_existing(
    request: &DatasetBuildRequest,
    split_counts: DatasetSplitCounts,
    pinned: PinnedDataset,
) -> FeatureLabelDataset {
    FeatureLabelDataset {
        pinned,
        build_spec_digest: request.build_spec_digest(),
        policy_digest: request.policy_digest(),
        universe_digest: request.universe_digest(),
        split_counts,
    }
}

fn output_idempotency_key(request: &DatasetBuildRequest) -> String {
    let digest = request.build_spec_digest().digest().bytes();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ignored = write!(&mut encoded, "{byte:02x}");
    }
    format!(
        "feature-label:{}:{encoded}",
        request.output_dataset().as_str()
    )
}

fn check_control(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), DatasetBuildError> {
    if cancellation.is_cancelled() {
        Err(DatasetBuildError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(DatasetBuildError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

async fn await_deadline<T, F>(deadline: Instant, future: F) -> Result<T, DatasetBuildError>
where
    F: Future<Output = T>,
{
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|_| DatasetBuildError::DeadlineExceeded)
}
