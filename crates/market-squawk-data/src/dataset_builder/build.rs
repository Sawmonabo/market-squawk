//! Bounded read, temporal admission, Arrow construction, and derived publication.

use std::future::Future;
use std::io;
use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float64Array,
    TimestampNanosecondArray, UInt8Array, UInt32Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder},
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
use super::{DatasetBuildError, DatasetBuildPrecommitAuthority, DatasetBuilderService, canonical};
use crate::schema::{
    FEATURE_LABEL_COMPONENT_NAME_BYTES, FEATURE_LABEL_CURRENCY_BYTES,
    FEATURE_LABEL_EXAMPLE_ID_BYTES, FEATURE_LABEL_MISSING_REASON_BYTES, FEATURE_LABEL_UNIT_BYTES,
};
use crate::{
    ArtifactRecord, AuthorizedResearchUse, CorporateActionPlan, CorporateActionRecord,
    DatasetArrowBatch, DatasetManifestRecord, DatasetSchemaRegistry, DerivedOutputObjectInput,
    FeatureLabelBatchBindings, GenerationParentRelation, IngestIdentity, ManifestObject,
    ManifestPlan, PinnedDataset, PointInTimeRequest, RegisteredRightsGrant, ResearchArrowBatch,
    ResearchUseRequest, Sha256Digest, SourceOperation, UniverseSnapshot,
};

struct PreparedRows {
    split_counts: DatasetSplitCounts,
    lineage_digest: Sha256Digest,
}

#[derive(Debug)]
struct OutputRow<'request> {
    example: &'request DatasetExample,
    split: DatasetSplit,
    component: &'request FeatureLabelComponentInput,
    lineage: Sha256Digest,
    input_epoch_json: Option<Arc<[u8]>>,
}

struct ComponentWindowSelection {
    kind: ComponentKind,
    knowledge_cutoff: Timestamp,
    effective_cutoff: ResearchTemporalCoordinate,
    label_effective_cutoff: Option<ResearchTemporalCoordinate>,
    selection: crate::pit::disk::Selection,
    action_plan: CorporateActionPlan,
}

impl ComponentWindowSelection {
    fn matches(&self, example: &DatasetExample, component: &FeatureLabelComponentInput) -> bool {
        self.kind == component.spec().kind()
            && component_knowledge_cutoff(example, component)
                .is_ok_and(|cutoff| self.knowledge_cutoff == cutoff)
            && &self.effective_cutoff == component.selection_effective_cutoff()
            && self.label_effective_cutoff.as_ref() == component.label_selection_effective_cutoff()
    }
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
    if let Some(policy) = request.policy().study_policy() {
        if policy.snapshot_as_of() > current_timestamp()? {
            return Err(DatasetBuildError::TemporalLeakage);
        }
    }
    if let Some(population) = request.inputs().current_population() {
        let authority = builder.authority.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => {
                DatasetBuildError::Catalog(crate::CatalogError::AuthorityBusy)
            }
            std::sync::TryLockError::Poisoned(_) => DatasetBuildError::AuthorityLockPoisoned,
        })?;
        population.validate_research_use_in_catalog(
            &authority,
            request.intended_use(),
            deadline,
            &cancellation,
        )?;
    }
    authorize_research_use(builder, &request, &cancellation)?;
    let mut budget = BuildRetainedBudget::new(request.limits().max_retained_bytes());
    budget.charge(request.retained_bytes())?;
    let label_measurements = derive_label_measurements(&request, &mut budget)?;
    if let Some(existing) = matching_existing(builder, &request)? {
        drop(authorize_existing_output(
            builder,
            &request,
            &existing,
            &cancellation,
        )?);
        return result_from_existing(
            &request,
            expected_split_counts(&request)?,
            existing,
            label_measurements,
        );
    }
    let mut candidates =
        read_inputs(builder, &request, &cancellation, deadline, &mut budget).await?;
    let mut source_index_bytes = 0_usize;
    {
        let mut financial_sources = Vec::new();
        for example in request.inputs().examples() {
            if let Some(financial) = example.financial_source() {
                let identity = Arc::as_ptr(&financial.source);
                if !financial_sources.contains(&identity) {
                    check_control(&cancellation, deadline)?;
                    financial
                        .source
                        .revalidate(&candidates, deadline, &cancellation)?;
                    if financial_sources.len() == financial_sources.capacity() {
                        let previous = financial_sources.capacity();
                        let pointer_bytes =
                            size_of::<*const super::financial::FinancialSeriesSource>();
                        budget.charge(pointer_bytes)?;
                        financial_sources
                            .try_reserve_exact(1)
                            .map_err(|_| DatasetBuildError::LimitExceeded)?;
                        let growth = (financial_sources.capacity() - previous)
                            .checked_mul(pointer_bytes)
                            .ok_or(DatasetBuildError::LimitExceeded)?;
                        if growth > pointer_bytes {
                            budget.charge(growth - pointer_bytes)?;
                        }
                        source_index_bytes = source_index_bytes
                            .checked_add(growth)
                            .ok_or(DatasetBuildError::LimitExceeded)?;
                    }
                    financial_sources.push(identity);
                }
            }
        }
    }
    budget.release(source_index_bytes)?;
    check_control(&cancellation, deadline)?;
    let _operation = await_deadline(deadline, builder.operation_gate.acquire(&cancellation))
        .await?
        .ok_or(DatasetBuildError::Cancelled)?;
    let authorization = authorize_research_use(builder, &request, &cancellation)?;
    if let Some(existing) = matching_existing(builder, &request)? {
        drop(authorize_existing_output(
            builder,
            &request,
            &existing,
            &cancellation,
        )?);
        return result_from_existing(
            &request,
            expected_split_counts(&request)?,
            existing,
            label_measurements,
        );
    }
    let store = builder.service.object_store();
    let publication = await_deadline(deadline, store.begin_publication(&cancellation)).await??;
    let (schema, arrow_schema) = feature_label_schema(&request)?;
    let writer_bytes = (budget.remaining()? / 3).min(32 * 1024 * 1024);
    budget.charge(writer_bytes)?;
    let mut writer = await_deadline(
        deadline,
        store.begin_dataset_writer_under_lease(
            arrow_schema,
            writer_bytes,
            &cancellation,
            &publication,
        ),
    )
    .await??;
    let prepared = prepare_rows(
        &request,
        &mut candidates,
        &cancellation,
        deadline,
        &mut budget,
        &mut writer,
        writer_bytes,
    )
    .await?;
    // Every source and disposition has been verified before the staged writer can finish.
    drop(candidates);
    let lineage_digest = prepared.lineage_digest;
    let staged = await_deadline(deadline, writer.finish()).await??;
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
        let durable = authority.publish_artifact_manifest(
            &reservation,
            std::slice::from_ref(&artifact),
            &anchor,
        )?;
        let bound = authority.bind_derived_output_object(
            &reservation,
            DerivedOutputObjectInput::try_new(
                durable.artifacts()[0].artifact_id(),
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
            durable.artifacts()[0].artifact_id(),
        )?;
        check_control(&cancellation, deadline)?;
        if let Some(precommit_authority) = precommit_authority.as_deref() {
            precommit_authority.validate_precommit()?;
        }
        if let Some(population) = request.inputs().current_population() {
            population.validate_research_use_in_catalog(
                &authority,
                request.intended_use(),
                deadline,
                &cancellation,
            )?;
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
    Ok(FeatureLabelDataset {
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
        study_policy: request.policy().study_policy().copied(),
        source_snapshot_digest: canonical::source_snapshot_digest(&request),
        population_basis: request.inputs().population_basis(),
        price_input_origin: price_input_origin(&request),
        population_member_count: request.inputs().population_member_count(),
        population_unavailable: request
            .inputs()
            .population_unavailable()
            .to_vec()
            .into_boxed_slice(),
        population_partition: request.inputs().population_partition().cloned(),
        population_source_use: request
            .inputs()
            .current_population()
            .map(|population| population.source_use(request.intended_use()))
            .transpose()?,
    })
}

pub(super) fn validate_request_authority(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    cancellation: &CancellationToken,
) -> Result<(), DatasetBuildError> {
    authorize_research_use(builder, request, cancellation).map(|_authorization| ())
}

async fn read_inputs(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    cancellation: &CancellationToken,
    deadline: Instant,
    budget: &mut BuildRetainedBudget,
) -> Result<crate::pit::disk::CandidateStore, DatasetBuildError> {
    let research_schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    let working_bytes = (budget.remaining()? / 4).min(32 * 1024 * 1024);
    budget.charge(working_bytes)?;
    let mut candidates = crate::pit::disk::CandidateStore::new(
        builder.service.object_store().operation_scratch()?,
        working_bytes,
        request.limits().max_spill_bytes(),
        cancellation,
        deadline,
    )
    .map_err(DatasetBuildError::IndexedPointInTime)?;
    let mut input_rows = 0_usize;
    let store = builder.service.object_store();
    for parent in request.inputs().parents() {
        check_control(cancellation, deadline)?;
        let pinned = builder.service.pinned(parent)?;
        let proof_parent = request.inputs().examples().iter().any(|example| {
            example
                .source_price_plan()
                .and_then(|plan| plan.source_split_admission())
                .is_some_and(|coverage| coverage.source_manifests().contains(parent))
        });
        let selected_price_parent = request.inputs().examples().iter().any(|example| {
            example
                .nominal_daily_source()
                .is_some_and(|source| &source.manifest == parent)
                || example
                    .timestamp_history_source()
                    .is_some_and(|source| source.manifest() == parent)
        });
        if proof_parent && !selected_price_parent {
            continue;
        }
        if pinned.manifest().schema() != &research_schema {
            return Err(DatasetBuildError::InvalidInputGeneration);
        }
        let mut cursor =
            store.pinned_batch_cursor(&pinned, 1024, budget.remaining()? / 2, cancellation)?;
        while let Some(batch) = await_deadline(deadline, cursor.next_batch()).await?? {
            check_control(cancellation, deadline)?;
            input_rows = input_rows
                .checked_add(batch.num_rows())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            if input_rows > request.limits().max_input_rows() {
                return Err(DatasetBuildError::LimitExceeded);
            }
            let batch_bytes = record_batch_retained_bytes(&batch)?;
            budget.charge(batch_bytes)?;
            let (observations, observation_bytes) =
                ResearchArrowBatch::decode_record_batch_bounded(batch, budget.remaining()?)
                    .map_err(|error| match error {
                        crate::ArrowConversionError::RetainedLimitExceeded { .. }
                        | crate::ArrowConversionError::AllocationFailure
                        | crate::ArrowConversionError::RetainedSizeOverflow => {
                            DatasetBuildError::LimitExceeded
                        }
                        other => DatasetBuildError::Arrow(other),
                    })?;
            budget.charge(observation_bytes)?;
            if let Some(policy) = request.policy().study_policy() {
                for observation in &observations {
                    if observation_context(observation).provenance().ingested_at()
                        > policy.snapshot_as_of()
                    {
                        return Err(DatasetBuildError::TemporalLeakage);
                    }
                }
            }
            candidates
                .append(observations, parent)
                .map_err(DatasetBuildError::IndexedPointInTime)?;
            budget.release(observation_bytes)?;
            budget.release(batch_bytes)?;
        }
    }
    if candidates.len() == 0 {
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

async fn prepare_rows(
    request: &DatasetBuildRequest,
    candidates: &mut crate::pit::disk::CandidateStore,
    cancellation: &CancellationToken,
    deadline: Instant,
    budget: &mut BuildRetainedBudget,
    writer: &mut crate::parquet_store::StreamingParquetWriter,
    writer_bytes: usize,
) -> Result<PreparedRows, DatasetBuildError> {
    // Reuse a bounded output chunk across examples so Parquet footer metadata does not
    // grow by one row group for each tiny example.
    let row_capacity = request
        .inputs()
        .examples()
        .len()
        .checked_mul(request.inputs().component_specs().len())
        .ok_or(DatasetBuildError::LimitExceeded)?
        .min(8192);
    let row_bytes = row_capacity
        .checked_mul(size_of::<OutputRow<'_>>())
        .ok_or(DatasetBuildError::LimitExceeded)?;
    budget.charge(row_bytes)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_capacity)
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    // Leave room for encoder expansion, schema/footer metadata and its active row group.
    let chunk_target = (budget.remaining()? / 4)
        .min(writer_bytes / 8)
        .min(4 * 1024 * 1024);
    let mut chunk_admission = 0_usize;
    let mut epoch_bytes = 0_usize;
    let mut output_rows = 0_usize;
    let mut split_counts = DatasetSplitCounts::default();
    let mut lineage = Sha256::new();
    lineage.update(b"market-squawk/feature-label-object-lineage/v1");
    lineage.update(request.build_spec_digest().digest().bytes());
    for example in request.inputs().examples() {
        check_control(cancellation, deadline)?;
        let split = request
            .policy()
            .split()
            .split_for(&request.policy().chronological_at(example))
            .ok_or(DatasetBuildError::TemporalLeakage)?;
        let universe_evidence = if request.inputs().current_population().is_none() {
            let universe_request = point_in_time_request(
                request,
                example.source_selection_as_of(),
                ResearchTemporalCoordinate::exact(example.source_selection_as_of()),
                None,
                budget.remaining()?,
            )?;
            let selected = candidates
                .select(&universe_request, |candidate| {
                    matches!(
                        candidate.observation(),
                        ResearchObservation::UniverseMembership(_)
                    )
                })
                .map_err(DatasetBuildError::IndexedPointInTime)?;
            budget.charge(selected.retained_bytes())?;
            Some(selected)
        } else {
            None
        };
        let universe = if let Some(population) = request.inputs().current_population() {
            if population.membership_as_of() != example.source_selection_as_of() {
                return Err(DatasetBuildError::UniverseEvidenceMismatch);
            }
            PreparedUniverse::Current(population)
        } else {
            let evidence = universe_evidence
                .as_ref()
                .ok_or(DatasetBuildError::UniverseEvidenceMismatch)?;
            PreparedUniverse::Historical(UniverseSnapshot::try_build(
                request.inputs().universe_id().clone(),
                example.source_selection_as_of(),
                validated_universe_memberships(request, evidence, budget.remaining()?)?,
                bounded_universe_limits(request, budget.remaining()?)?,
            )?)
        };
        budget.charge(universe.retained_bytes())?;
        if !universe.contains(example.instrument_id()) {
            return Err(DatasetBuildError::InstrumentOutsideUniverse);
        }

        let window_bytes = example
            .components()
            .len()
            .checked_mul(size_of::<ComponentWindowSelection>())
            .ok_or(DatasetBuildError::LimitExceeded)?;
        budget.charge(window_bytes)?;
        let mut windows = Vec::new();
        windows
            .try_reserve_exact(example.components().len())
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        for component in example.components() {
            if windows
                .iter()
                .any(|window: &ComponentWindowSelection| window.matches(example, component))
            {
                continue;
            }
            let knowledge_cutoff = component_knowledge_cutoff(example, component)?;
            let component_request = point_in_time_request(
                request,
                knowledge_cutoff,
                component.selection_effective_cutoff().clone(),
                component.label_selection_effective_cutoff().cloned(),
                budget.remaining()?,
            )?;
            let selection = candidates
                .select(&component_request, |candidate| {
                    retain_example_evidence(example, candidate)
                })
                .map_err(DatasetBuildError::IndexedPointInTime)?;
            budget.charge(selection.retained_bytes())?;
            let label = component.spec().kind() == ComponentKind::Label;
            let action_selection = if request.policy().study_policy().is_some()
                && example.financial_source().is_none()
                && example.source_price_plan().is_none()
            {
                let end = if label {
                    example
                        .label_effective_cutoff()
                        .and_then(ResearchTemporalCoordinate::exact_timestamp)
                        .ok_or(DatasetBuildError::InvalidRequest)?
                } else {
                    example
                        .decision_at()
                        .ok_or(DatasetBuildError::InvalidRequest)?
                };
                let action_request = point_in_time_request(
                    request,
                    knowledge_cutoff,
                    ResearchTemporalCoordinate::exact(end),
                    None,
                    budget.remaining()?,
                )?;
                let action_selection = candidates.select(&action_request, |candidate| {
                    matches!(candidate.observation(), ResearchObservation::CorporateAction(action)
                        if action.context().provenance().instrument_id() == Some(example.instrument_id()))
                }).map_err(DatasetBuildError::IndexedPointInTime)?;
                budget.charge(action_selection.retained_bytes())?;
                Some(action_selection)
            } else {
                None
            };
            let action_plan = action_plan_from_selection(
                request,
                example,
                action_selection.as_ref().unwrap_or(&selection),
                label,
                budget.remaining()?,
            )?;
            if let Some(action_selection) = action_selection {
                budget.release(action_selection.retained_bytes())?;
            }
            budget.charge(action_plan_owned_bytes(&action_plan)?)?;
            if !action_plan.conflicts().is_empty() {
                return Err(DatasetBuildError::UnresolvedCorporateAction);
            }
            windows.push(ComponentWindowSelection {
                kind: component.spec().kind(),
                knowledge_cutoff,
                effective_cutoff: component.selection_effective_cutoff().clone(),
                label_effective_cutoff: component.label_selection_effective_cutoff().cloned(),
                selection,
                action_plan,
            });
        }
        let input_epoch_json = if example.financial_source().is_some() {
            let workspace = super::epoch::MAX_INPUT_EPOCH_BYTES * 8;
            budget.charge(workspace)?;
            let epoch = super::FeatureDatasetInputEpoch::from_financial(
                example,
                *request
                    .policy()
                    .study_policy()
                    .ok_or(DatasetBuildError::InvalidRequest)?,
                canonical::source_snapshot_digest(request)
                    .ok_or(DatasetBuildError::InvalidRequest)?,
                universe.content_hash(),
                universe.audit_hash(),
                request.inputs().population_basis(),
                current_timestamp()?,
            )?;
            let bytes = epoch.encode()?;
            drop(epoch);
            budget.release(workspace)?;
            budget.charge(bytes.len())?;
            Some(Arc::<[u8]>::from(bytes))
        } else if completed_close_recipe(request) {
            let workspace = super::epoch::MAX_INPUT_EPOCH_BYTES * 8;
            budget.charge(workspace)?;
            let epoch =
                completed_close_epoch(request, example, &windows, &universe, current_timestamp()?)?;
            let bytes = epoch.encode()?;
            drop(epoch);
            budget.release(workspace)?;
            budget.charge(bytes.len())?;
            Some(Arc::<[u8]>::from(bytes))
        } else {
            None
        };
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
            let mut matching_windows = windows
                .iter()
                .filter(|window| window.matches(example, component));
            let window = matching_windows
                .next()
                .ok_or(DatasetBuildError::InvalidRequest)?;
            if matching_windows.next().is_some() {
                return Err(DatasetBuildError::InvalidRequest);
            }
            validate_component_adjustment(
                component,
                &window.action_plan,
                request.policy().corporate_actions(),
            )?;
            let evidence_bytes = component
                .selectors()
                .len()
                .checked_mul(size_of::<Sha256Digest>())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            budget.charge(evidence_bytes)?;
            let evidence = resolve_component_evidence(component, &window.selection)?;
            if let Some(financial) = example.financial_source() {
                financial.validate_component(component, &window.selection)?;
            }
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
                window.selection.content_identity(),
                window.selection.audit_identity(),
                &evidence,
                universe.content_hash(),
                universe.audit_hash(),
                window.action_plan.content_hash(),
                window.action_plan.audit_hash(),
            );
            budget.release(evidence_bytes)?;
            example_rows.push(OutputRow {
                example,
                split,
                component,
                lineage,
                input_epoch_json: input_epoch_json.clone(),
            });
        }
        if !drop_example {
            output_rows = output_rows
                .checked_add(example_rows.len())
                .ok_or(DatasetBuildError::LimitExceeded)?;
            if output_rows > request.limits().max_output_rows() {
                return Err(DatasetBuildError::LimitExceeded);
            }
            let admission =
                feature_label_output_admission(&example_rows)?.saturating_sub(64 * 1024);
            if example_rows.len() > row_capacity {
                return Err(DatasetBuildError::LimitExceeded);
            }
            if !rows.is_empty()
                && (rows.len() + example_rows.len() > row_capacity
                    || chunk_admission
                        .checked_add(admission)
                        .is_none_or(|bytes| bytes > chunk_target))
            {
                flush_output_rows(
                    request,
                    &mut rows,
                    &mut epoch_bytes,
                    budget,
                    writer,
                    deadline,
                    &mut lineage,
                )
                .await?;
                chunk_admission = 0;
            }
            chunk_admission = chunk_admission
                .checked_add(admission)
                .ok_or(DatasetBuildError::LimitExceeded)?;
            epoch_bytes = epoch_bytes
                .checked_add(input_epoch_json.as_ref().map_or(0, |bytes| bytes.len()))
                .ok_or(DatasetBuildError::LimitExceeded)?;
            rows.append(&mut example_rows);
            split_counts.record(split);
        } else if let Some(bytes) = &input_epoch_json {
            budget.release(bytes.len())?;
        }
        drop(example_rows);
        drop(input_epoch_json);
        budget.release(example_row_bytes)?;
        for window in &windows {
            budget.release(action_plan_owned_bytes(&window.action_plan)?)?;
            budget.release(window.selection.retained_bytes())?;
        }
        budget.release(window_bytes)?;
        budget.release(universe.retained_bytes())?;
        if let Some(evidence) = universe_evidence {
            budget.release(evidence.retained_bytes())?;
        }
    }
    flush_output_rows(
        request,
        &mut rows,
        &mut epoch_bytes,
        budget,
        writer,
        deadline,
        &mut lineage,
    )
    .await?;
    drop(rows);
    budget.release(row_bytes)?;
    if output_rows == 0 {
        return Err(DatasetBuildError::EmptyDataset);
    }
    Ok(PreparedRows {
        split_counts,
        lineage_digest: Sha256Digest::new(lineage.finalize().into()),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the writer, working allocation and canonical lineage share one publication lifetime"
)]
async fn flush_output_rows(
    request: &DatasetBuildRequest,
    rows: &mut Vec<OutputRow<'_>>,
    epoch_bytes: &mut usize,
    budget: &mut BuildRetainedBudget,
    writer: &mut crate::parquet_store::StreamingParquetWriter,
    deadline: Instant,
    lineage: &mut Sha256,
) -> Result<(), DatasetBuildError> {
    if rows.is_empty() {
        return Ok(());
    }
    let admission = feature_label_output_admission(rows)?;
    budget.charge(admission)?;
    let batch = feature_label_batch(request, rows)?;
    if batch.record_batch().get_array_memory_size() > admission {
        return Err(DatasetBuildError::LimitExceeded);
    }
    await_deadline(deadline, writer.write_dataset_batch(&batch)).await??;
    for row in rows.iter() {
        lineage.update(row.lineage.bytes());
    }
    drop(batch);
    rows.clear();
    budget.release(admission)?;
    budget.release(std::mem::take(epoch_bytes))?;
    Ok(())
}

fn retain_example_evidence(
    example: &DatasetExample,
    candidate: &crate::PointInTimeCandidate,
) -> bool {
    if matches!(candidate.observation(), ResearchObservation::CorporateAction(action)
        if action.context().provenance().instrument_id() == Some(example.instrument_id()))
    {
        return true;
    }
    candidate.family_key().is_ok_and(|family| {
        example.components().iter().any(|component| {
            component
                .selectors()
                .iter()
                .any(|selector| selector.family() == &family)
        })
    })
}

enum PreparedUniverse<'a> {
    Historical(UniverseSnapshot),
    Current(&'a crate::CurrentListedPopulation),
}
impl PreparedUniverse<'_> {
    fn content_hash(&self) -> Sha256Digest {
        match self {
            Self::Historical(v) => v.content_hash(),
            Self::Current(v) => v.content_digest(),
        }
    }
    fn audit_hash(&self) -> Sha256Digest {
        match self {
            Self::Historical(v) => v.audit_hash(),
            Self::Current(v) => v.audit_digest(),
        }
    }
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Historical(v) => v.retained_bytes(),
            Self::Current(_) => 0,
        }
    }
    fn contains(&self, id: market_squawk_domain::InstrumentId) -> bool {
        match self {
            Self::Historical(v) => v.contains(id),
            Self::Current(v) => v.contains(id),
        }
    }
}

fn component_knowledge_cutoff(
    example: &DatasetExample,
    component: &FeatureLabelComponentInput,
) -> Result<Timestamp, DatasetBuildError> {
    match component.spec().kind() {
        ComponentKind::Feature => Ok(example.source_selection_as_of()),
        ComponentKind::Label => example
            .label_selection_as_of()
            .ok_or(DatasetBuildError::InvalidRequest),
    }
}

fn validated_universe_memberships(
    request: &DatasetBuildRequest,
    selection: &crate::pit::disk::Selection,
    max_retained_bytes: usize,
) -> Result<Vec<crate::UniverseMembership>, DatasetBuildError> {
    let memberships = request
        .inputs()
        .universe_memberships()
        .ok_or(DatasetBuildError::UniverseEvidenceMismatch)?;
    let admission = membership_vector_admission(memberships)?;
    if admission > max_retained_bytes {
        return Err(DatasetBuildError::LimitExceeded);
    }
    let mut validated = Vec::new();
    validated
        .try_reserve_exact(memberships.len())
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    for claimed in memberships {
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
    selection: &crate::pit::disk::Selection,
    label: bool,
    remaining_bytes: usize,
) -> Result<CorporateActionPlan, DatasetBuildError> {
    if let Some(source) = example.source_price_plan() {
        let knowledge = if label {
            example
                .label_selection_as_of()
                .ok_or(DatasetBuildError::InvalidRequest)?
        } else {
            example.source_selection_as_of()
        };
        let valuation = if label {
            example
                .label_effective_cutoff()
                .and_then(ResearchTemporalCoordinate::exact_timestamp)
                .ok_or(DatasetBuildError::InvalidRequest)?
        } else if example.timestamp_history_source().is_some() {
            // The feature's economic endpoint is the genuine provider completion. Decision lag
            // does not extend original history coverage or change its adjustment basis.
            example
                .effective_cutoff()
                .exact_timestamp()
                .ok_or(DatasetBuildError::InvalidRequest)?
        } else if request.policy().study_policy().is_some_and(|study| {
            study.basis() == market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown
                && study.purpose() == super::DatasetBuildPurpose::StudyInputs
        }) {
            // Current inference retains the unit basis of the authentic completed session.
            // Acquisition time is the knowledge boundary, not a replacement economic origin.
            example
                .exact_target_coordinates()
                .map(|value| value.0)
                .ok_or(DatasetBuildError::InvalidRequest)?
        } else {
            example
                .decision_at()
                .ok_or(DatasetBuildError::InvalidRequest)?
        };
        let coverage = source
            .source_split_admission()
            .ok_or(DatasetBuildError::ComponentAdjustmentMismatch)?;
        if knowledge != coverage.knowledge_cutoff() || valuation > source.valuation_cutoff() {
            return Err(DatasetBuildError::ComponentAdjustmentMismatch);
        }
        let policy = request.policy().corporate_actions();
        let required = source.source_split_projection_limits(
            policy,
            example.instrument_id(),
            knowledge,
            valuation,
        )?;
        let configured = request.limits().corporate_actions();
        let available = remaining_bytes
            .checked_add(coverage.retained_bytes())
            .ok_or(DatasetBuildError::LimitExceeded)?;
        if required.max_actions() > configured.max_actions()
            || required.max_retained_bytes() > configured.max_retained_bytes()
            || required.max_retained_bytes().get() > available
        {
            return Err(DatasetBuildError::LimitExceeded);
        }
        return source
            .try_project_source_split_plan(
                policy,
                example.instrument_id(),
                knowledge,
                valuation,
                required,
            )
            .map_err(DatasetBuildError::from);
    }
    if example.nominal_daily_source().is_some() || example.timestamp_history_source().is_some() {
        return Err(DatasetBuildError::ComponentAdjustmentMismatch);
    }
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
        example
            .label_selection_as_of()
            .ok_or(DatasetBuildError::InvalidRequest)?
    } else {
        example.source_selection_as_of()
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
        if example.financial_source().is_none()
            && request.policy().study_policy().is_some_and(|policy| {
                policy.basis()
                    == market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot
            })
        {
            if label {
                example
                    .label_effective_cutoff()
                    .and_then(ResearchTemporalCoordinate::exact_timestamp)
                    .ok_or(DatasetBuildError::InvalidRequest)?
            } else {
                example
                    .decision_at()
                    .ok_or(DatasetBuildError::InvalidRequest)?
            }
        } else {
            cutoff
        },
        relevant,
        limits,
    )
    .map_err(DatasetBuildError::from)
}

// The request retains each shared original proof pool once. A projection owns only its
// per-window records/steps; charging the same Arc-backed proof for each component would make
// a valid bounded source request fail according to its number of macro components.
fn action_plan_owned_bytes(plan: &CorporateActionPlan) -> Result<usize, DatasetBuildError> {
    plan.retained_bytes()
        .checked_sub(
            plan.source_split_admission()
                .map_or(0, |coverage| coverage.retained_bytes()),
        )
        .ok_or(DatasetBuildError::LimitExceeded)
}

fn resolve_component_evidence(
    component: &FeatureLabelComponentInput,
    selection: &crate::pit::disk::Selection,
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

fn price_input_origin(request: &DatasetBuildRequest) -> Option<super::DatasetPriceInputOrigin> {
    if !completed_close_recipe(request) {
        return None;
    }
    let mask = request.inputs().examples().iter().fold(0, |mask, example| {
        mask | if example.nominal_daily_source().is_some() {
            2
        } else {
            1
        }
    });
    super::DatasetPriceInputOrigin::from_mask(mask)
}

fn completed_close_recipe(request: &DatasetBuildRequest) -> bool {
    request.policy().implementation_revision().as_str()
        == super::production::RECIPE_IMPLEMENTATION_REVISION
        || request.policy().implementation_revision().as_str()
            == super::production::STUDY_IMPLEMENTATION_REVISION
        || matches!(
            request.policy().implementation_revision().as_str(),
            "price-return-macro-context-fixed-horizon-price-higher-v1"
                | "price-return-macro-context-fixed-horizon-benchmark-outperformance-v1"
                | "price-return-macro-context-fixed-horizon-profit-after-costs-v1"
        )
}

fn completed_close_epoch(
    request: &DatasetBuildRequest,
    example: &DatasetExample,
    windows: &[ComponentWindowSelection],
    universe: &PreparedUniverse<'_>,
    calculated_at: Timestamp,
) -> Result<super::FeatureDatasetInputEpoch, DatasetBuildError> {
    let (origin, terminal) =
        exact_terminal_coordinates(example).ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
    let feature = example
        .components()
        .iter()
        .find(|component| {
            component.spec().kind() == ComponentKind::Feature
                && component.spec().name() == "research.price-return"
        })
        .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
    if feature.selectors().len() != 2 {
        return Err(DatasetBuildError::ComponentEvidenceMismatch);
    }
    let study = request
        .policy()
        .study_policy()
        .ok_or(DatasetBuildError::InvalidRequest)?;
    let feature_window = windows
        .iter()
        .find(|window| window.matches(example, feature))
        .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
    let mut current = None;
    let mut prior_count = 0;
    for selector in feature.selectors() {
        let mut selected = feature_window.selection.records().iter().filter(|record| {
            record
                .candidate()
                .family_key()
                .is_ok_and(|family| &family == selector.family())
        });
        let record = selected
            .next()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        if selected.next().is_some() {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let ResearchObservation::MarketBar(bar) = record.candidate().observation() else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        let close = selected_price_close(example, bar, record.candidate().source_manifest())?;
        if close == origin {
            if current.replace((record, bar)).is_some() {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        } else if close < origin {
            prior_count += 1;
        } else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
    }
    if prior_count != 1 {
        return Err(DatasetBuildError::ComponentEvidenceMismatch);
    }
    if study.purpose() == super::DatasetBuildPurpose::Training {
        let label = example
            .components()
            .iter()
            .find(|component| {
                component.spec().kind() == ComponentKind::Label
                    && component.spec().name()
                        == example
                            .probability_event_target()
                            .map_or("research.fixed-horizon-forward-return", |value| {
                                value.label_component_name()
                            })
            })
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        if label.selectors().len() != 1 {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let label_window = windows
            .iter()
            .find(|window| window.matches(example, label))
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let mut selected = label_window.selection.records().iter().filter(|record| {
            record
                .candidate()
                .family_key()
                .is_ok_and(|family| &family == label.selectors()[0].family())
        });
        let record = selected
            .next()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        if selected.next().is_some() {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let ResearchObservation::MarketBar(bar) = record.candidate().observation() else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        if selected_price_close(example, bar, record.candidate().source_manifest())? != terminal {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
    }
    let (record, bar) = current.ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
    super::FeatureDatasetInputEpoch::from_selected(
        example,
        bar,
        record.candidate().source_manifest(),
        record.evidence_identity(),
        feature_window.selection.content_identity(),
        feature_window.selection.audit_identity(),
        universe.content_hash(),
        universe.audit_hash(),
        request.inputs().population_basis(),
        feature.adjustment(),
        calculated_at,
        &feature_window.action_plan,
        *study,
        canonical::source_snapshot_digest(request).ok_or(DatasetBuildError::InvalidRequest)?,
    )
}

fn feature_label_schema(
    request: &DatasetBuildRequest,
) -> Result<(crate::DatasetSchemaRef, arrow::datatypes::SchemaRef), DatasetBuildError> {
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
    Ok((schema_ref, schema))
}

fn feature_label_batch(
    request: &DatasetBuildRequest,
    rows: &[OutputRow<'_>],
) -> Result<DatasetArrowBatch, DatasetBuildError> {
    let (schema_ref, schema) = feature_label_schema(request)?;
    let mut float_values = bounded_output_vec(rows.len())?;
    let mut decimal_values = bounded_output_vec(rows.len())?;
    let mut decimal_scales = bounded_output_vec(rows.len())?;
    let mut units = bounded_output_vec(rows.len())?;
    let mut currencies = bounded_output_vec(rows.len())?;
    let mut missing = bounded_output_vec(rows.len())?;
    // Admission already charges the complete epoch payload. Reserve that exact allocation
    // rather than retaining the geometric growth of BinaryArray's iterator constructor.
    let mut input_epochs =
        BinaryBuilder::with_capacity(rows.len(), input_epoch_payload_bytes(rows)?);
    for row in rows {
        input_epochs.append_option(row.input_epoch_json.as_deref());
        match row.component.value() {
            ComponentValue::Float {
                value,
                unit,
                currency,
            } => {
                float_values.push(Some(*value));
                decimal_values.push(None);
                decimal_scales.push(None);
                units.push(unit.as_ref().map(|value| value.as_str()));
                currencies.push(currency.as_ref().map(|value| value.as_str()));
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
                units.push(unit.as_ref().map(|value| value.as_str()));
                currencies.push(currency.as_ref().map(|value| value.as_str()));
                missing.push(None);
            }
            ComponentValue::Missing { reason } => {
                float_values.push(None);
                decimal_values.push(None);
                decimal_scales.push(None);
                units.push(None);
                currencies.push(None);
                missing.push(Some(reason.as_str()));
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
        target_coordinate_kinds.push(if row.example.nominal_daily_source().is_some() {
            5
        } else if row.example.financial_source().is_some() {
            4
        } else if row.input_epoch_json.is_some() {
            3
        } else if exact_terminal_coordinates(row.example).is_some() {
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
                rows.iter()
                    .map(|row| row.example.source_selection_as_of().unix_nanos()),
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
            units.iter().copied(),
            FEATURE_LABEL_UNIT_BYTES,
        )?),
        Arc::new(fixed_text_array(
            currencies.iter().copied(),
            FEATURE_LABEL_CURRENCY_BYTES,
        )?),
        Arc::new(fixed_text_array(
            missing.iter().copied(),
            FEATURE_LABEL_MISSING_REASON_BYTES,
        )?),
        Arc::new(
            TimestampNanosecondArray::from_iter(
                rows.iter()
                    .map(|row| row.example.decision_at().map(Timestamp::unix_nanos)),
            )
            .with_timezone_utc(),
        ),
        Arc::new(
            TimestampNanosecondArray::from_iter(rows.iter().map(|row| {
                row.example
                    .label_selection_as_of()
                    .map(Timestamp::unix_nanos)
            }))
            .with_timezone_utc(),
        ),
        Arc::new(lineages),
        Arc::new(input_epochs.finish()),
        Arc::new(Date32Array::from_iter(rows.iter().map(|row| {
            row.example
                .decision_coordinate()
                .calendar_date_value()
                .map(|d| d.days_since_unix_epoch())
        }))),
    ];
    let record_batch =
        RecordBatch::try_new(schema, arrays).map_err(crate::ArrowConversionError::from)?;
    DatasetArrowBatch::try_new(schema_ref, record_batch).map_err(Into::into)
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
    fixed
        .checked_add(input_epoch_payload_bytes(rows)?)
        .ok_or(DatasetBuildError::LimitExceeded)
}

fn input_epoch_payload_bytes(rows: &[OutputRow<'_>]) -> Result<usize, DatasetBuildError> {
    let bytes = rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(row.input_epoch_json.as_ref().map_or(0, |value| value.len()))
            .ok_or(DatasetBuildError::LimitExceeded)
    })?;
    // Binary arrays have signed 32-bit offsets; refuse before allocating or appending.
    i32::try_from(bytes).map_err(|_| DatasetBuildError::LimitExceeded)?;
    Ok(bytes)
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

pub(super) fn authorize_existing_output(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    existing: &PinnedDataset,
    cancellation: &CancellationToken,
) -> Result<RegisteredRightsGrant, DatasetBuildError> {
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
    authority
        .admit_source_rights(
            request
                .output_authorization()
                .rights_decision(object.object().content_hash(), current_timestamp()?),
        )
        .map_err(Into::into)
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
            .split_for(&request.policy().chronological_at(example))
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
                let mut binding = FeatureLabelMeasurementBinding::try_new(
                    spec.clone(),
                    measurement,
                    if let Some(study) = request.policy().study_policy() {
                        match study.target_horizon() {
                            super::DatasetTargetHorizon::FiscalPeriods { .. } => {
                                Some(study.target_horizon())
                            }
                            _ => horizon.fixed().map(|h| {
                                super::DatasetTargetHorizon::ExactElapsed(
                                    std::time::Duration::from_nanos(h.get()),
                                )
                            }),
                        }
                    } else {
                        horizon.fixed().map(|h| {
                            super::DatasetTargetHorizon::ExactElapsed(
                                std::time::Duration::from_nanos(h.get()),
                            )
                        })
                    },
                    horizon.fixed().map(|_| {
                        if request.inputs().examples()[0]
                            .nominal_daily_source()
                            .is_some()
                        {
                            super::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar
                        } else if completed_close_recipe(request) {
                            super::FixedHorizonOriginBasis::CompletedBarClose
                        } else {
                            super::FixedHorizonOriginBasis::ExactEffectiveTimestamp
                        }
                    }),
                )?;
                let event = request.inputs().examples()[0].probability_event_target();
                if request
                    .inputs()
                    .examples()
                    .iter()
                    .any(|example| example.probability_event_target() != event)
                {
                    return Err(DatasetBuildError::InvalidRequest);
                }
                if let Some(event) = event {
                    binding = binding.try_with_probability_event(event)?;
                }
                bindings.push(binding);
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
    example.exact_target_coordinates()
}

fn selected_price_close(
    example: &DatasetExample,
    bar: &market_squawk_domain::MarketBarObservation,
    manifest: &crate::DatasetManifestRef,
) -> Result<Timestamp, DatasetBuildError> {
    if let Some(source) = example.nominal_daily_source() {
        source.origin.selected_close(bar, manifest)
    } else if let Some(source) = example.timestamp_history_source() {
        source.selected_close(bar, manifest)
    } else {
        bar.completed_at()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)
    }
}

fn result_from_existing(
    request: &DatasetBuildRequest,
    split_counts: DatasetSplitCounts,
    pinned: PinnedDataset,
    label_measurements: Box<[FeatureLabelMeasurementBinding]>,
) -> Result<FeatureLabelDataset, DatasetBuildError> {
    Ok(FeatureLabelDataset {
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
        study_policy: request.policy().study_policy().copied(),
        source_snapshot_digest: canonical::source_snapshot_digest(request),
        population_basis: request.inputs().population_basis(),
        price_input_origin: price_input_origin(request),
        population_member_count: request.inputs().population_member_count(),
        population_unavailable: request
            .inputs()
            .population_unavailable()
            .to_vec()
            .into_boxed_slice(),
        population_partition: request.inputs().population_partition().cloned(),
        population_source_use: request
            .inputs()
            .current_population()
            .map(|population| population.source_use(request.intended_use()))
            .transpose()?,
    })
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

fn observation_context(
    observation: &ResearchObservation,
) -> &market_squawk_domain::ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        ResearchObservation::Macro(value) => value.context(),
        ResearchObservation::MarketBar(value) => value.context(),
        ResearchObservation::FundNav(value) => value.context(),
        ResearchObservation::MarketCalendar(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateActionSource(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::UniverseMembership(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
    }
}
