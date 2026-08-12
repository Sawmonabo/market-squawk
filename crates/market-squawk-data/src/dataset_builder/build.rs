//! Bounded read, temporal admission, Arrow construction, and derived publication.

use std::future::Future;
use std::io;
use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
    UInt8Array, UInt32Array, builder::FixedSizeBinaryBuilder,
};
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    AvailabilityEvidence, CorporateActionObservation, DigestAlgorithm, EvidenceDigest,
    ResearchObservation, ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::model::{
    ComponentAdjustmentEvidence, ComponentKind, ComponentValue, CorporateActionSensitivity,
    DatasetBuildRequest, DatasetExample, DatasetSplit, DatasetSplitCounts,
    FeatureLabelComponentInput, FeatureLabelDataset, FeatureLabelMeasurement,
    FeatureLabelMeasurementBinding, MissingValuePolicy,
};
use super::{
    DatasetBuildError, DatasetBuildPrecommitAuthority, DatasetBuilderService, admission, canonical,
};
use crate::schema::{
    FEATURE_LABEL_COMPONENT_NAME_BYTES, FEATURE_LABEL_CURRENCY_BYTES,
    FEATURE_LABEL_EXAMPLE_ID_BYTES, FEATURE_LABEL_MISSING_REASON_BYTES, FEATURE_LABEL_UNIT_BYTES,
};
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

#[derive(Clone, Copy, Debug)]
struct BuildRetainedBudget {
    limit: usize,
    retained: usize,
}

impl BuildRetainedBudget {
    const fn new(limit: usize) -> Self {
        Self { limit, retained: 0 }
    }

    fn charge(&mut self, bytes: usize) -> Result<(), DatasetBuildError> {
        let retained = self
            .retained
            .checked_add(bytes)
            .ok_or(DatasetBuildError::LimitExceeded)?;
        if retained > self.limit {
            return Err(DatasetBuildError::LimitExceeded);
        }
        self.retained = retained;
        Ok(())
    }

    fn release(&mut self, bytes: usize) -> Result<(), DatasetBuildError> {
        self.retained = self
            .retained
            .checked_sub(bytes)
            .ok_or(DatasetBuildError::LimitExceeded)?;
        Ok(())
    }

    fn remaining(self) -> Result<usize, DatasetBuildError> {
        self.limit
            .checked_sub(self.retained)
            .ok_or(DatasetBuildError::LimitExceeded)
    }
}

pub(super) async fn build(
    builder: &DatasetBuilderService<'_>,
    request: DatasetBuildRequest,
    cancellation: CancellationToken,
    precommit_authority: Option<Arc<dyn DatasetBuildPrecommitAuthority>>,
) -> Result<FeatureLabelDataset, DatasetBuildError> {
    let deadline = Instant::now()
        .checked_add(request.limits().max_duration())
        .ok_or(DatasetBuildError::DeadlineExceeded)?;
    check_control(&cancellation, deadline)?;
    authorize_research_use(builder, &request, &cancellation)?;
    let mut budget = BuildRetainedBudget::new(request.limits().max_retained_bytes());
    budget.charge(request.retained_bytes())?;
    let label_measurements = derive_label_measurements(&request, &mut budget)?;
    if let Some(existing) = matching_existing(builder, &request)? {
        authorize_existing_output(builder, &request, &existing, &cancellation)?;
        return admit_result(
            builder,
            result_from_existing(
                &request,
                expected_split_counts(&request)?,
                existing,
                label_measurements,
            ),
        );
    }
    let candidates = read_inputs(builder, &request, &cancellation, deadline, &mut budget).await?;
    let prepared =
        prepare_rows(&request, &candidates, &cancellation, deadline, &mut budget).await?;
    let output_admission = feature_label_output_admission(&prepared.rows)?;
    budget.charge(output_admission)?;
    let (batch, lineage_digest) = feature_label_batch(&request, &prepared.rows)?;
    if batch.record_batch().get_array_memory_size() > output_admission {
        return Err(DatasetBuildError::LimitExceeded);
    }
    check_control(&cancellation, deadline)?;

    let _operation = await_deadline(deadline, builder.operation_gate.acquire(&cancellation))
        .await?
        .ok_or(DatasetBuildError::Cancelled)?;
    check_control(&cancellation, deadline)?;
    let authorization = authorize_research_use(builder, &request, &cancellation)?;
    if let Some(existing) = matching_existing(builder, &request)? {
        authorize_existing_output(builder, &request, &existing, &cancellation)?;
        return admit_result(
            builder,
            result_from_existing(
                &request,
                prepared.split_counts,
                existing,
                label_measurements,
            ),
        );
    }
    check_control(&cancellation, deadline)?;
    let store = builder.service.object_store();
    let publication = await_deadline(deadline, store.begin_publication(&cancellation)).await??;
    let staged = await_deadline(
        deadline,
        store.stage_dataset_under_lease(&batch, &cancellation, &publication),
    )
    .await??;
    let reservation = {
        let authority = builder
            .authority
            .lock()
            .map_err(|_| DatasetBuildError::AuthorityLockPoisoned)?;
        let rights = authority.admit_source_rights(
            request
                .output_authorization()
                .rights_decision(staged.content_hash(), staged.created_at()),
        )?;
        authority.reserve_ingest(
            &IngestIdentity::try_new(
                request.output_authorization().source_id().clone(),
                EvidenceDigest::new(DigestAlgorithm::Sha256, staged.content_hash().bytes()),
                SourceOperation::Persist,
                output_idempotency_key(&request),
            )?,
            &rights,
        )?
    };
    check_control(&cancellation, deadline)?;
    let published = store.finalize_staged_under_lease(staged, &publication)?;
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
        if let Some(precommit_authority) = precommit_authority.as_deref() {
            precommit_authority.validate_precommit()?;
        }
        let derived = authority.publish_derived_generation(input)?;
        if let Some(precommit_authority) = precommit_authority.as_deref() {
            precommit_authority.commit_succeeded();
        }
        derived
    };
    drop(publication);
    check_control(&cancellation, deadline)?;
    let pinned = builder.service.pinned(derived.manifest())?;
    admit_result(
        builder,
        FeatureLabelDataset {
            pinned,
            build_spec_digest: request.build_spec_digest(),
            policy_digest: request.policy_digest(),
            universe_digest: request.universe_digest(),
            split_counts: prepared.split_counts,
            universe_id: request.inputs().universe_id().clone(),
            split_policy: request.policy().split(),
            point_in_time_policy: request.policy().point_in_time(),
            missing_value_policy: request.policy().missing_values(),
            component_specs: request
                .inputs()
                .component_specs()
                .to_vec()
                .into_boxed_slice(),
            label_measurements,
        },
    )
}

pub(super) fn validate_request_authority(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    cancellation: &CancellationToken,
) -> Result<(), DatasetBuildError> {
    authorize_research_use(builder, request, cancellation).map(|_authorization| ())
}

fn admit_result(
    builder: &DatasetBuilderService<'_>,
    dataset: FeatureLabelDataset,
) -> Result<FeatureLabelDataset, DatasetBuildError> {
    admission::register(builder, &dataset)?;
    Ok(dataset)
}

async fn read_inputs(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    cancellation: &CancellationToken,
    deadline: Instant,
    budget: &mut BuildRetainedBudget,
) -> Result<Vec<PointInTimeCandidate>, DatasetBuildError> {
    let research_schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    let mut candidates = Vec::new();
    let mut input_rows = 0_usize;
    let store = builder.service.object_store();
    for parent in request.inputs().parents() {
        check_control(cancellation, deadline)?;
        let pinned = builder.service.pinned(parent)?;
        if pinned.manifest().schema() != &research_schema {
            return Err(DatasetBuildError::InvalidInputGeneration);
        }
        let remaining_rows = request
            .limits()
            .max_input_rows()
            .checked_sub(input_rows)
            .ok_or(DatasetBuildError::LimitExceeded)?;
        let remaining_bytes = budget.remaining()?;
        let read_budget = remaining_bytes / 2;
        if remaining_rows == 0 || read_budget == 0 {
            return Err(DatasetBuildError::LimitExceeded);
        }
        let batches = await_deadline(
            deadline,
            store.read_pinned_bounded_async(&pinned, remaining_rows, read_budget, cancellation),
        )
        .await?
        .map_err(|error| match error {
            crate::ParquetStoreError::ReadLimitExceeded => DatasetBuildError::LimitExceeded,
            other => DatasetBuildError::Parquet(other),
        })?;
        let batch_bytes = batches.iter().try_fold(0_usize, |total, batch| {
            record_batch_retained_bytes(batch).and_then(|bytes| {
                total
                    .checked_add(bytes)
                    .ok_or(DatasetBuildError::LimitExceeded)
            })
        })?;
        budget.charge(batch_bytes)?;
        for batch in batches {
            input_rows = input_rows
                .checked_add(batch.num_rows())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            if input_rows > request.limits().max_input_rows() {
                return Err(DatasetBuildError::LimitExceeded);
            }
            let batch_retained = record_batch_retained_bytes(&batch)?;
            reserve_candidate_capacity(&mut candidates, batch.num_rows(), budget)?;
            let manifest_bytes = manifest_dynamic_bytes(parent).and_then(|bytes| {
                bytes
                    .checked_mul(batch.num_rows())
                    .ok_or(DatasetBuildError::LimitExceeded)
            })?;
            budget.charge(manifest_bytes)?;
            let (observations, observation_bytes) =
                ResearchArrowBatch::decode_record_batch_bounded(batch, budget.remaining()?)
                    .map_err(|error| match error {
                        crate::ArrowConversionError::RetainedLimitExceeded
                        | crate::ArrowConversionError::AllocationFailure
                        | crate::ArrowConversionError::RetainedSizeOverflow => {
                            DatasetBuildError::LimitExceeded
                        }
                        other => DatasetBuildError::Arrow(other),
                    })?;
            budget.charge(observation_bytes)?;
            candidates
                .len()
                .checked_add(observations.len())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            for observation in observations {
                candidates.push(PointInTimeCandidate::new(observation, parent.clone()));
            }
            budget.release(batch_retained)?;
        }
    }
    if candidates.is_empty() {
        return Err(DatasetBuildError::InvalidInputGeneration);
    }
    Ok(candidates)
}

fn record_batch_retained_bytes(batch: &RecordBatch) -> Result<usize, DatasetBuildError> {
    batch
        .get_array_memory_size()
        .checked_add(size_of::<RecordBatch>())
        .and_then(|bytes| {
            batch
                .num_columns()
                .checked_mul(size_of::<ArrayRef>())
                .and_then(|columns| bytes.checked_add(columns))
        })
        .ok_or(DatasetBuildError::LimitExceeded)
}

fn reserve_candidate_capacity(
    candidates: &mut Vec<PointInTimeCandidate>,
    additional: usize,
    budget: &mut BuildRetainedBudget,
) -> Result<(), DatasetBuildError> {
    let required = candidates
        .len()
        .checked_add(additional)
        .ok_or(DatasetBuildError::LimitExceeded)?;
    let admitted_capacity = required.saturating_sub(candidates.capacity());
    let admitted_bytes = admitted_capacity
        .checked_mul(size_of::<PointInTimeCandidate>())
        .ok_or(DatasetBuildError::LimitExceeded)?;
    budget.charge(admitted_bytes)?;
    let previous_capacity = candidates.capacity();
    candidates
        .try_reserve_exact(additional)
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    let actual_growth = candidates
        .capacity()
        .checked_sub(previous_capacity)
        .and_then(|capacity| capacity.checked_mul(size_of::<PointInTimeCandidate>()))
        .ok_or(DatasetBuildError::LimitExceeded)?;
    if actual_growth > admitted_bytes {
        budget.charge(actual_growth - admitted_bytes)?;
    }
    Ok(())
}

fn manifest_dynamic_bytes(
    manifest: &crate::DatasetManifestRef,
) -> Result<usize, DatasetBuildError> {
    manifest
        .dataset_id()
        .as_str()
        .len()
        .checked_add(manifest.schema().name().len())
        .ok_or(DatasetBuildError::LimitExceeded)
}

fn membership_vector_admission(
    memberships: &[crate::UniverseMembership],
) -> Result<usize, DatasetBuildError> {
    memberships.iter().try_fold(
        memberships
            .len()
            .checked_mul(size_of::<crate::UniverseMembership>())
            .ok_or(DatasetBuildError::LimitExceeded)?,
        |total, membership| {
            let availability = match membership.availability() {
                AvailabilityEvidence::Evidenced { evidence, .. } => evidence.as_str().len(),
                AvailabilityEvidence::Inferred { method, .. } => method.as_str().len(),
                AvailabilityEvidence::LocalFirstObserved { .. } | AvailabilityEvidence::Unknown => {
                    0
                }
            };
            total
                .checked_add(manifest_dynamic_bytes(membership.source_manifest())?)
                .and_then(|bytes| bytes.checked_add(availability))
                .ok_or(DatasetBuildError::LimitExceeded)
        },
    )
}

fn bounded_universe_limits(
    request: &DatasetBuildRequest,
    remaining_bytes: usize,
) -> Result<crate::UniverseLimits, DatasetBuildError> {
    let configured = request.limits().universe();
    crate::UniverseLimits::try_new(
        configured.max_candidates(),
        configured.max_retained_bytes().min(remaining_bytes),
    )
    .map_err(|_| DatasetBuildError::LimitExceeded)
}

fn corporate_action_record_admission(
    observation: &CorporateActionObservation,
    manifest: &crate::DatasetManifestRef,
) -> Result<usize, DatasetBuildError> {
    let mut writer = CountingWriter::default();
    serde_json::to_writer(&mut writer, observation)
        .map_err(|_| DatasetBuildError::InvalidRequest)?;
    size_of::<CorporateActionRecord>()
        .checked_add(writer.bytes)
        .and_then(|bytes| bytes.checked_add(manifest_dynamic_bytes(manifest).ok()?))
        .ok_or(DatasetBuildError::LimitExceeded)
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl io::Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("retained-byte count overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn prepare_rows<'request>(
    request: &'request DatasetBuildRequest,
    candidates: &[PointInTimeCandidate],
    cancellation: &CancellationToken,
    deadline: Instant,
    budget: &mut BuildRetainedBudget,
) -> Result<PreparedRows<'request>, DatasetBuildError> {
    let mut rows = Vec::new();
    let row_capacity = request
        .inputs()
        .examples()
        .len()
        .checked_mul(request.inputs().component_specs().len())
        .ok_or(DatasetBuildError::LimitExceeded)?;
    let row_bytes = row_capacity
        .checked_mul(size_of::<OutputRow<'_>>())
        .ok_or(DatasetBuildError::LimitExceeded)?;
    budget.charge(row_bytes)?;
    rows.try_reserve_exact(row_capacity)
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
        let feature_request = point_in_time_request(
            request,
            example.cutoff_at(),
            example.effective_cutoff().clone(),
            None,
            budget.remaining()?,
        )?;
        let features = selector
            .select(&feature_request, candidates, cancellation, deadline)
            .await
            .map_err(|_| DatasetBuildError::PointInTime)?;
        budget.charge(features.retained_bytes())?;
        let label_request = point_in_time_request(
            request,
            example.label_cutoff_at(),
            example.effective_cutoff().clone(),
            Some(example.label_effective_cutoff().clone()),
            budget.remaining()?,
        )?;
        let labels = selector
            .select(&label_request, candidates, cancellation, deadline)
            .await
            .map_err(|_| DatasetBuildError::PointInTime)?;
        budget.charge(labels.retained_bytes())?;
        let universe_request = point_in_time_request(
            request,
            example.cutoff_at(),
            ResearchTemporalCoordinate::exact(example.cutoff_at()),
            None,
            budget.remaining()?,
        )?;
        let universe_evidence = selector
            .select(&universe_request, candidates, cancellation, deadline)
            .await
            .map_err(|_| DatasetBuildError::PointInTime)?;
        budget.charge(universe_evidence.retained_bytes())?;
        let universe_limits = bounded_universe_limits(request, budget.remaining()?)?;
        let universe = UniverseSnapshot::try_build(
            request.inputs().universe_id().clone(),
            example.cutoff_at(),
            validated_universe_memberships(request, &universe_evidence, budget.remaining()?)?,
            universe_limits,
        )?;
        budget.charge(universe.retained_bytes())?;
        if !universe.contains(example.instrument_id()) {
            return Err(DatasetBuildError::InstrumentOutsideUniverse);
        }
        let feature_actions =
            action_plan_from_selection(request, example, &features, false, budget.remaining()?)?;
        budget.charge(feature_actions.retained_bytes())?;
        let label_actions =
            action_plan_from_selection(request, example, &labels, true, budget.remaining()?)?;
        budget.charge(label_actions.retained_bytes())?;
        if !feature_actions.conflicts().is_empty() || !label_actions.conflicts().is_empty() {
            return Err(DatasetBuildError::UnresolvedCorporateAction);
        }
        let mut example_rows = Vec::new();
        let example_row_bytes = example
            .components()
            .len()
            .checked_mul(size_of::<OutputRow<'_>>())
            .ok_or(DatasetBuildError::LimitExceeded)?;
        budget.charge(example_row_bytes)?;
        example_rows
            .try_reserve_exact(example.components().len())
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        let mut drop_example = false;
        for component in example.components() {
            let (selection, action_plan) = match component.spec().kind() {
                ComponentKind::Feature => (&features, &feature_actions),
                ComponentKind::Label => (&labels, &label_actions),
            };
            validate_component_adjustment(
                component,
                action_plan,
                request.policy().corporate_actions(),
            )?;
            let evidence_bytes = component
                .selectors()
                .len()
                .checked_mul(size_of::<Sha256Digest>())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            budget.charge(evidence_bytes)?;
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
            budget.release(evidence_bytes)?;
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
        budget.release(example_row_bytes)?;
        budget.release(label_actions.retained_bytes())?;
        budget.release(feature_actions.retained_bytes())?;
        budget.release(universe.retained_bytes())?;
        budget.release(universe_evidence.retained_bytes())?;
        budget.release(labels.retained_bytes())?;
        budget.release(features.retained_bytes())?;
    }
    if rows.is_empty() {
        return Err(DatasetBuildError::EmptyDataset);
    }
    Ok(PreparedRows { rows, split_counts })
}

fn validated_universe_memberships(
    request: &DatasetBuildRequest,
    selection: &PointInTimeSelection<'_>,
    max_retained_bytes: usize,
) -> Result<Vec<crate::UniverseMembership>, DatasetBuildError> {
    let admission = membership_vector_admission(request.inputs().universe_memberships())?;
    if admission > max_retained_bytes {
        return Err(DatasetBuildError::LimitExceeded);
    }
    let mut validated = Vec::new();
    validated
        .try_reserve_exact(request.inputs().universe_memberships().len())
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    for claimed in request.inputs().universe_memberships() {
        let mut matches = selection.records().iter().filter(|record| {
            let ResearchObservation::UniverseMembership(observed) =
                record.candidate().observation()
            else {
                return false;
            };
            let context = observed.context();
            record.candidate().source_manifest() == claimed.source_manifest()
                && context.provenance().instrument_id() == Some(claimed.instrument_id())
                && observed.universe().as_str() == request.inputs().universe_id().as_str()
                && observed.effective_interval() == claimed.effective_interval()
                && context.provenance().availability() == claimed.availability()
                && record.payload_identity().bytes() == claimed.evidence_digest().bytes()
        });
        if matches.next().is_none() || matches.next().is_some() {
            return Err(DatasetBuildError::UniverseEvidenceMismatch);
        }
        validated.push(claimed.clone());
    }
    Ok(validated)
}

fn validate_component_adjustment(
    component: &FeatureLabelComponentInput,
    plan: &CorporateActionPlan,
    policy: crate::CorporateActionPolicy,
) -> Result<(), DatasetBuildError> {
    let valid = match (component.spec().corporate_actions(), component.adjustment()) {
        (CorporateActionSensitivity::NotApplicable, ComponentAdjustmentEvidence::NotApplicable) => {
            true
        }
        (CorporateActionSensitivity::RequiresAdjustment, ComponentAdjustmentEvidence::Raw) => {
            policy.adjustment() == crate::CorporateActionAdjustment::Raw
        }
        (
            CorporateActionSensitivity::RequiresAdjustment,
            ComponentAdjustmentEvidence::Applied {
                policy: applied_policy,
                plan_content,
                plan_audit,
                ..
            },
        ) => {
            policy.adjustment() != crate::CorporateActionAdjustment::Raw
                && *applied_policy == policy
                && *plan_content == plan.content_hash()
                && *plan_audit == plan.audit_hash()
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(DatasetBuildError::ComponentAdjustmentMismatch)
    }
}

fn point_in_time_request(
    request: &DatasetBuildRequest,
    as_of: market_squawk_domain::Timestamp,
    effective_cutoff: ResearchTemporalCoordinate,
    label_cutoff: Option<ResearchTemporalCoordinate>,
    remaining_bytes: usize,
) -> Result<PointInTimeRequest, DatasetBuildError> {
    let configured = request.limits().point_in_time();
    let retained_bytes = configured.max_retained_bytes().min(remaining_bytes);
    let limits = crate::PointInTimeLimits::try_new(
        configured.max_candidates(),
        configured.max_families(),
        configured.max_conflicts(),
        configured.max_result_rows(),
        retained_bytes,
    )
    .map_err(|_| DatasetBuildError::LimitExceeded)?;
    PointInTimeRequest::try_new(
        request.policy().point_in_time(),
        as_of,
        None,
        effective_cutoff,
        label_cutoff,
        limits,
    )
    .map_err(|_| DatasetBuildError::InvalidRequest)
}

fn action_plan_from_selection(
    request: &DatasetBuildRequest,
    example: &DatasetExample,
    selection: &PointInTimeSelection<'_>,
    label: bool,
    remaining_bytes: usize,
) -> Result<CorporateActionPlan, DatasetBuildError> {
    let mut relevant = Vec::new();
    let mut admission = 0_usize;
    for record in selection.records() {
        let ResearchObservation::CorporateAction(action) = record.candidate().observation() else {
            continue;
        };
        if action.context().provenance().instrument_id() == Some(example.instrument_id()) {
            admission = admission
                .checked_add(corporate_action_record_admission(
                    action,
                    record.candidate().source_manifest(),
                )?)
                .ok_or(DatasetBuildError::LimitExceeded)?;
        }
    }
    if admission > remaining_bytes {
        return Err(DatasetBuildError::LimitExceeded);
    }
    for record in selection.records() {
        let ResearchObservation::CorporateAction(action) = record.candidate().observation() else {
            continue;
        };
        if action.context().provenance().instrument_id() == Some(example.instrument_id()) {
            if relevant.len() >= request.limits().corporate_actions().max_actions().get() {
                return Err(DatasetBuildError::LimitExceeded);
            }
            relevant
                .try_reserve(1)
                .map_err(|_| DatasetBuildError::LimitExceeded)?;
            relevant.push(CorporateActionRecord::new(
                action.clone(),
                record.candidate().source_manifest().clone(),
                EvidenceDigest::new(DigestAlgorithm::Sha256, record.evidence_identity().bytes()),
            ));
        }
    }
    let cutoff = if label {
        example.label_cutoff_at()
    } else {
        example.cutoff_at()
    };
    let configured = request.limits().corporate_actions();
    let retained_bytes = configured.max_retained_bytes().get().min(remaining_bytes);
    let limits = crate::CorporateActionLimits::try_new(
        configured.max_actions(),
        NonZeroUsize::new(retained_bytes).ok_or(DatasetBuildError::LimitExceeded)?,
    )
    .map_err(|_| DatasetBuildError::LimitExceeded)?;
    CorporateActionPlan::try_build(
        request.policy().corporate_actions(),
        cutoff,
        cutoff,
        relevant,
        limits,
    )
    .map_err(DatasetBuildError::from)
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
    let mut float_values = bounded_output_vec(rows.len())?;
    let mut decimal_values = bounded_output_vec(rows.len())?;
    let mut decimal_scales = bounded_output_vec(rows.len())?;
    let mut units = bounded_output_vec(rows.len())?;
    let mut currencies = bounded_output_vec(rows.len())?;
    let mut missing = bounded_output_vec(rows.len())?;
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
    let mut target_coordinate_kinds = bounded_output_vec(rows.len())?;
    for row in rows {
        target_coordinate_kinds.push(if exact_terminal_coordinates(row.example).is_some() {
            1
        } else {
            2
        });
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(fixed_text_array(
            rows.iter().map(|row| Some(row.example.example_id())),
            FEATURE_LABEL_EXAMPLE_ID_BYTES,
        )?),
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                rows.iter()
                    .map(|row| row.example.instrument_id().as_uuid().into_bytes().to_vec()),
            )
            .map_err(crate::ArrowConversionError::from)?,
        ),
        Arc::new(
            TimestampNanosecondArray::from_iter_values(
                rows.iter().map(|row| row.example.cutoff_at().unix_nanos()),
            )
            .with_timezone_utc(),
        ),
        Arc::new(
            TimestampNanosecondArray::from_iter(rows.iter().map(|row| {
                exact_terminal_coordinates(row.example)
                    .map(|(observed, _target)| observed.unix_nanos())
            }))
            .with_timezone_utc(),
        ),
        Arc::new(
            TimestampNanosecondArray::from_iter(rows.iter().map(|row| {
                exact_terminal_coordinates(row.example)
                    .map(|(_observed, target)| target.unix_nanos())
            }))
            .with_timezone_utc(),
        ),
        Arc::new(UInt8Array::from_iter_values(
            target_coordinate_kinds.iter().copied(),
        )),
        Arc::new(UInt8Array::from_iter_values(rows.iter().map(
            |row| match row.split {
                DatasetSplit::Train => 1,
                DatasetSplit::Validation => 2,
                DatasetSplit::Test => 3,
            },
        ))),
        Arc::new(UInt8Array::from_iter_values(
            rows.iter().map(|row| row.component.spec().kind().tag()),
        )),
        Arc::new(fixed_text_array(
            rows.iter().map(|row| Some(row.component.spec().name())),
            FEATURE_LABEL_COMPONENT_NAME_BYTES,
        )?),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.component.spec().version().get()),
        )),
        Arc::new(Float64Array::from(float_values)),
        Arc::new(decimal),
        Arc::new(UInt8Array::from(decimal_scales)),
        Arc::new(fixed_text_array(
            units.iter().map(|value| value.as_deref()),
            FEATURE_LABEL_UNIT_BYTES,
        )?),
        Arc::new(fixed_text_array(
            currencies.iter().map(|value| value.as_deref()),
            FEATURE_LABEL_CURRENCY_BYTES,
        )?),
        Arc::new(fixed_text_array(
            missing.iter().map(|value| value.as_deref()),
            FEATURE_LABEL_MISSING_REASON_BYTES,
        )?),
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

fn fixed_text_array<'value, Values>(
    values: Values,
    width: i32,
) -> Result<FixedSizeBinaryArray, DatasetBuildError>
where
    Values: IntoIterator<Item = Option<&'value str>>,
    Values::IntoIter: ExactSizeIterator,
{
    let width = usize::try_from(width).map_err(|_| DatasetBuildError::InvalidRequest)?;
    let values = values.into_iter();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(
        values.len(),
        i32::try_from(width).map_err(|_| DatasetBuildError::InvalidRequest)?,
    );
    let mut padded = vec![0_u8; width];
    for value in values {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        let bytes = value.as_bytes();
        if bytes.is_empty() || bytes.len() > width || bytes.contains(&0) {
            return Err(DatasetBuildError::InvalidRequest);
        }
        padded.fill(0);
        padded[..bytes.len()].copy_from_slice(bytes);
        builder
            .append_value(&padded)
            .map_err(crate::ArrowConversionError::from)?;
    }
    Ok(builder.finish())
}

fn bounded_output_vec<T>(capacity: usize) -> Result<Vec<T>, DatasetBuildError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    Ok(values)
}

fn feature_label_output_admission(rows: &[OutputRow<'_>]) -> Result<usize, DatasetBuildError> {
    let fixed = rows
        .len()
        .checked_mul(1024)
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or(DatasetBuildError::LimitExceeded)?;
    Ok(fixed)
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

fn authorize_existing_output(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    existing: &PinnedDataset,
    cancellation: &CancellationToken,
) -> Result<(), DatasetBuildError> {
    if cancellation.is_cancelled() {
        return Err(DatasetBuildError::Cancelled);
    }
    let [object] = existing.objects() else {
        return Err(DatasetBuildError::InvalidInputGeneration);
    };
    let authority = builder
        .authority
        .lock()
        .map_err(|_| DatasetBuildError::AuthorityLockPoisoned)?;
    authority.admit_source_rights(
        request
            .output_authorization()
            .rights_decision(object.object().content_hash(), current_timestamp()?),
    )?;
    Ok(())
}

fn current_timestamp() -> Result<market_squawk_domain::Timestamp, DatasetBuildError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DatasetBuildError::InvalidRequest)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_| DatasetBuildError::InvalidRequest)?;
    Ok(market_squawk_domain::Timestamp::from_unix_nanos(nanos))
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

fn derive_label_measurements(
    request: &DatasetBuildRequest,
    budget: &mut BuildRetainedBudget,
) -> Result<Box<[FeatureLabelMeasurementBinding]>, DatasetBuildError> {
    let specs = request.inputs().component_specs();
    let observed_bytes = size_of::<Option<FeatureLabelMeasurement>>()
        .checked_add(size_of::<FixedHorizonState>())
        .and_then(|bytes| bytes.checked_mul(specs.len()))
        .ok_or(DatasetBuildError::LimitExceeded)?;
    budget.charge(observed_bytes)?;
    let mut observed = Vec::new();
    observed
        .try_reserve_exact(specs.len())
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    observed.resize(specs.len(), None);
    let mut horizons = Vec::new();
    horizons
        .try_reserve_exact(specs.len())
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    horizons.resize(specs.len(), FixedHorizonState::Unseen);
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
        for (index, component) in example.components().iter().enumerate() {
            if component.spec().kind() != ComponentKind::Label {
                continue;
            }
            let Some(measurement) = FeatureLabelMeasurement::try_from_value(component.value())?
            else {
                continue;
            };
            if observed[index].is_some_and(|retained| retained != measurement) {
                return Err(DatasetBuildError::InvalidRequest);
            }
            observed[index] = Some(measurement);
            horizons[index].observe(example);
        }
    }
    let binding_count = specs
        .iter()
        .zip(&observed)
        .filter(|(spec, measurement)| spec.kind() == ComponentKind::Label && measurement.is_some())
        .count();
    let binding_bytes = size_of::<FeatureLabelMeasurementBinding>()
        .checked_mul(binding_count)
        .and_then(|bytes| {
            specs
                .iter()
                .zip(&observed)
                .filter(|(spec, measurement)| {
                    spec.kind() == ComponentKind::Label && measurement.is_some()
                })
                .try_fold(bytes, |total, (spec, _)| {
                    total.checked_add(spec.name().len())
                })
        })
        .ok_or(DatasetBuildError::LimitExceeded)?;
    budget.charge(binding_bytes)?;
    let mut bindings = Vec::new();
    bindings
        .try_reserve_exact(binding_count)
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    for ((spec, measurement), horizon) in specs.iter().zip(observed).zip(horizons) {
        if spec.kind() == ComponentKind::Label {
            if let Some(measurement) = measurement {
                bindings.push(FeatureLabelMeasurementBinding::try_new(
                    spec.clone(),
                    measurement,
                    horizon.fixed(),
                )?);
            }
        }
    }
    budget.release(observed_bytes)?;
    Ok(bindings.into_boxed_slice())
}

#[derive(Clone, Copy)]
enum FixedHorizonState {
    Unseen,
    Fixed(NonZeroU64),
    Unsupported,
}

impl FixedHorizonState {
    fn observe(&mut self, example: &DatasetExample) {
        let candidate = exact_terminal_coordinates(example)
            .and_then(|(observed, target)| target.unix_nanos().checked_sub(observed.unix_nanos()))
            .and_then(|value| u64::try_from(value).ok())
            .and_then(NonZeroU64::new);
        *self = match (*self, candidate) {
            (Self::Unseen, Some(value)) => Self::Fixed(value),
            (Self::Fixed(expected), Some(value)) if value == expected => Self::Fixed(expected),
            (Self::Unsupported, _) | (_, None) | (Self::Fixed(_), Some(_)) => Self::Unsupported,
        };
    }

    const fn fixed(self) -> Option<NonZeroU64> {
        match self {
            Self::Fixed(value) => Some(value),
            Self::Unseen | Self::Unsupported => None,
        }
    }
}

fn exact_terminal_coordinates(example: &DatasetExample) -> Option<(Timestamp, Timestamp)> {
    let observed = example.effective_cutoff().exact_timestamp()?;
    let target = example.label_effective_cutoff().exact_timestamp()?;
    (target > observed).then_some((observed, target))
}

fn result_from_existing(
    request: &DatasetBuildRequest,
    split_counts: DatasetSplitCounts,
    pinned: PinnedDataset,
    label_measurements: Box<[FeatureLabelMeasurementBinding]>,
) -> FeatureLabelDataset {
    FeatureLabelDataset {
        pinned,
        build_spec_digest: request.build_spec_digest(),
        policy_digest: request.policy_digest(),
        universe_digest: request.universe_digest(),
        split_counts,
        universe_id: request.inputs().universe_id().clone(),
        split_policy: request.policy().split(),
        point_in_time_policy: request.policy().point_in_time(),
        missing_value_policy: request.policy().missing_values(),
        component_specs: request
            .inputs()
            .component_specs()
            .to_vec()
            .into_boxed_slice(),
        label_measurements,
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
