//! Manifest-bound source discovery, extraction, and canonical row mapping.

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};
use std::path::Path;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use market_squawk_domain::{
    AlternativeDataObservation, AvailabilityEvidence as DomainAvailabilityEvidence, DataQuality,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, PayloadReference,
    ResearchContext, ResearchObservation, ResearchProvenance, ResearchProvenanceInput,
    ResearchTemporalCoordinate, ResearchTime, RevisionNumber, SourceIdentifier, Timestamp,
    UniverseMembershipObservation, VersionPinnedSourceLocator,
};
use market_squawk_platform::{
    BoundedInput, ControlledInputFileError, InputReadCheckpoint, InputReadControl,
    InputReadControlError, UserAuthorizedInputRoot,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionBatchAccumulator, ExtractionRecord,
    ExtractionRequest, ExtractionSource, ExtractionSourceError, NetworkAccessPolicy, SourceClass,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::clock::{ExtractionClock, RequestDeadline, SystemExtractionClock};
use crate::contracts::{
    ExtractionLimits, FileAdapterError, ParseBudget, ParsedRow, ParserLimit, SourceRowLimit,
};
use crate::manifest::{FileFormat, FileObjectSpec, FileSourceManifest};
use crate::representation::FileRepresentationAuthority;
use crate::{csv, database, excel, json, ofx, parquet, xml};

const MAX_CONCURRENT_BLOCKING_OPERATIONS: usize = 4;
const MAX_CONCURRENT_DEADLINE_SAMPLES: usize = 4;
static BLOCKING_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_BLOCKING_OPERATIONS)));
static DEADLINE_SAMPLING_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_DEADLINE_SAMPLES)));

struct FileInputReadControl<'a> {
    cancellation: &'a CancellationToken,
    deadline: RequestDeadline,
    clock: &'a dyn ExtractionClock,
}

impl InputReadControl for FileInputReadControl<'_> {
    fn checkpoint(&self, _checkpoint: InputReadCheckpoint) -> Result<(), InputReadControlError> {
        if self.cancellation.is_cancelled() {
            return Err(InputReadControlError::Cancelled);
        }
        match self.deadline.checkpoint(self.clock) {
            Ok(()) => Ok(()),
            Err(FileAdapterError::DeadlineExceeded) => Err(InputReadControlError::DeadlineExceeded),
            Err(FileAdapterError::ClockFailure) => Err(InputReadControlError::Unavailable),
            Err(_) => Err(InputReadControlError::Unavailable),
        }
    }
}

/// A manifest-bound extraction source over one user-authorized local root.
#[derive(Clone)]
pub struct FileExtractionSource {
    metadata: Arc<SourceMetadata>,
    root: UserAuthorizedInputRoot,
    manifest: Arc<FileSourceManifest>,
    limits: ExtractionLimits,
    representation: Arc<FileRepresentationAuthority>,
    clock: Arc<dyn ExtractionClock>,
}

impl fmt::Debug for FileExtractionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileExtractionSource")
            .field("metadata", &self.metadata)
            .field("root", &"[USER-AUTHORIZED INPUT ROOT]")
            .field("objects", &self.manifest.objects.len())
            .field("limits", &self.limits)
            .field("representation", &"[DURABLE EXACT-OBJECT AUTHORITY]")
            .field("clock", &"[PAIRED EXTRACTION CLOCK]")
            .finish()
    }
}

impl FileExtractionSource {
    /// Constructs an immutable source bound to exact manifest bytes and one local root.
    ///
    /// `representation_state_root` is a controlled, writable local directory that must be disjoint
    /// from the user-authorized input root. The source holds its authority store exclusively for
    /// its lifetime. State is namespaced by source identity, metadata revision, and manifest digest
    /// so reconstruction can recover exact-object availability and operation-time evidence without
    /// trusting caller-provided provenance.
    ///
    /// # Errors
    ///
    /// Rejects non-local/networked metadata, mismatched manifest evidence, duplicate objects,
    /// unsafe row policies, unsupported manifest versions, overlapping authority/input roots, an
    /// authority namespace mismatch, corrupt or oversized authority state, and concurrent access to
    /// the same authority directory.
    pub fn try_new(
        metadata: SourceMetadata,
        root: UserAuthorizedInputRoot,
        representation_state_root: impl AsRef<Path>,
        manifest_input: BoundedInput,
        limits: ExtractionLimits,
    ) -> Result<Self, FileAdapterError> {
        Self::try_new_with_clock(
            metadata,
            root,
            representation_state_root,
            manifest_input,
            limits,
            Arc::new(SystemExtractionClock),
        )
    }

    /// Constructs a source with an explicitly injected paired wall/monotonic clock.
    ///
    /// The paired clock controls request deadlines and derives exact-object first-observed,
    /// operation receive, and ingestion times.
    ///
    /// # Errors
    ///
    /// Applies the same metadata, manifest, row-policy, and durable authority validation as
    /// [`Self::try_new`].
    pub fn try_new_with_clock(
        metadata: SourceMetadata,
        root: UserAuthorizedInputRoot,
        representation_state_root: impl AsRef<Path>,
        manifest_input: BoundedInput,
        limits: ExtractionLimits,
        clock: Arc<dyn ExtractionClock>,
    ) -> Result<Self, FileAdapterError> {
        if metadata.source_class() != SourceClass::LocalFile
            || !matches!(metadata.network_policy(), NetworkAccessPolicy::Denied)
            || !metadata.capabilities().extraction()
            || metadata.quality_ceiling() == DataQuality::DirectVerified
        {
            return Err(FileAdapterError::MetadataPolicyMismatch);
        }
        let expected = metadata
            .revision_evidence()
            .payload_evidence()
            .content_digest();
        if manifest_input.digest() != expected {
            return Err(FileAdapterError::ManifestEvidenceMismatch);
        }
        let manifest_digest = manifest_input.digest();
        let manifest = FileSourceManifest::parse(manifest_input.as_bytes(), limits)?;
        manifest.validate()?;
        let representation = FileRepresentationAuthority::try_open(
            &root,
            representation_state_root,
            metadata.source_id(),
            metadata.revision(),
            manifest_digest,
        )?;
        Ok(Self {
            metadata: Arc::new(metadata),
            root,
            manifest: Arc::new(manifest),
            limits,
            representation: Arc::new(representation),
            clock,
        })
    }

    /// Discovers exact manifest objects under current registry authority.
    ///
    /// Authority is checked at admission, after each blocking input read, and before publication.
    pub async fn discover_files(
        &self,
        authority: &ExtractionAuthority,
        request: &DiscoveryRequest,
        cancellation: &CancellationToken,
    ) -> Result<DiscoveryBatch, FileAdapterError> {
        self.validate_authority(authority)?;
        let deadline = self
            .seal_request_deadline(request.deadline(), cancellation)
            .await?;
        let permit = Self::acquire_blocking_slot(cancellation, deadline).await?;
        let source = self.clone();
        let worker_authority = authority.clone();
        let request = request.clone();
        let worker_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            source.discover_files_blocking(
                &worker_authority,
                &request,
                &worker_cancellation,
                deadline,
            )
        });
        let batch = Self::await_blocking(worker, cancellation, deadline).await?;
        self.validate_authority(authority)?;
        Ok(batch)
    }

    fn discover_files_blocking(
        &self,
        authority: &ExtractionAuthority,
        request: &DiscoveryRequest,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<DiscoveryBatch, FileAdapterError> {
        self.validate_authority(authority)?;
        self.check_control(cancellation, deadline)?;
        let mut objects = Vec::new();
        for specification in self
            .manifest
            .objects
            .iter()
            .filter(|object| &object.dataset == request.dataset())
            .filter(|object| {
                request.effective_at().is_none_or(|effective_at| {
                    object.effective_at <= effective_at
                        && object
                            .superseded_at
                            .is_none_or(|superseded_at| effective_at < superseded_at)
                })
            })
            .take(usize::from(request.max_results()))
        {
            self.check_control(cancellation, deadline)?;
            let input = self.read_object(specification, cancellation, deadline)?;
            self.validate_authority(authority)?;
            let sampled_at = self.control_timestamp(cancellation, deadline)?;
            let availability = self.bind_object_availability(
                &specification.dataset,
                &specification.object_id,
                input.digest(),
                input.identity().size_bytes(),
                sampled_at,
            )?;
            let effective =
                EffectiveInterval::new(specification.effective_at, specification.superseded_at)
                    .map_err(|_| FileAdapterError::InvalidManifest)?;
            let evidence = object_evidence(specification, input.digest())?;
            objects.push(
                SourceObject::try_new_with_availability(
                    self.metadata.source_id().clone(),
                    self.metadata.revision().clone(),
                    request,
                    specification.object_id.clone(),
                    specification.format.media_type()?,
                    evidence,
                    effective,
                    specification.published_at,
                    availability,
                    Some(input.identity().size_bytes()),
                )
                .map_err(|_| FileAdapterError::Contract)?,
            );
        }
        let batch =
            DiscoveryBatch::try_new(request, objects).map_err(|_| FileAdapterError::Contract)?;
        self.validate_authority(authority)?;
        self.check_control(cancellation, deadline)?;
        Ok(batch)
    }

    /// Extracts one object under current registry authority into canonical observations.
    ///
    /// Authority is checked at admission, after the blocking input read, and before publication.
    pub async fn extract_file(
        &self,
        authority: &ExtractionAuthority,
        request: &ExtractionRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExtractionBatch, FileAdapterError> {
        self.validate_authority(authority)?;
        let deadline = self
            .seal_request_deadline(request.deadline(), cancellation)
            .await?;
        let permit = Self::acquire_blocking_slot(cancellation, deadline).await?;
        let source = self.clone();
        let worker_authority = authority.clone();
        let request = request.clone();
        let worker_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            source.extract_file_blocking(
                &worker_authority,
                &request,
                &worker_cancellation,
                deadline,
            )
        });
        let batch = Self::await_blocking(worker, cancellation, deadline).await?;
        self.validate_authority(authority)?;
        Ok(batch)
    }

    fn extract_file_blocking(
        &self,
        authority: &ExtractionAuthority,
        request: &ExtractionRequest,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<ExtractionBatch, FileAdapterError> {
        self.validate_authority(authority)?;
        self.check_control(cancellation, deadline)?;
        let specification = self
            .manifest
            .objects
            .iter()
            .find(|object| {
                object.object_id == *request.object().object_id()
                    && object.dataset == *request.object().dataset()
            })
            .ok_or(FileAdapterError::ObjectNotFound)?;
        let expected_effective =
            EffectiveInterval::new(specification.effective_at, specification.superseded_at)
                .map_err(|_| FileAdapterError::InvalidManifest)?;
        if request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
            || request.object().dataset() != &specification.dataset
            || request.object().object_id() != &specification.object_id
            || request.object().media_type() != &specification.format.media_type()?
            || request.object().effective_interval() != expected_effective
            || request.object().published_at() != specification.published_at
        {
            return Err(FileAdapterError::ObjectLineageMismatch);
        }
        let input = self.read_object(specification, cancellation, deadline)?;
        self.validate_authority(authority)?;
        let sampled_received_at = self.control_timestamp(cancellation, deadline)?;
        if request.object().evidence() != &object_evidence(specification, input.digest())?
            || request.object().expected_bytes() != Some(input.identity().size_bytes())
        {
            return Err(FileAdapterError::ObjectEvidenceMismatch);
        }
        self.verify_object_availability(
            request.object().dataset(),
            request.object().object_id(),
            input.digest(),
            input.identity().size_bytes(),
            request.object().availability(),
        )?;
        if request
            .object()
            .availability()
            .reported_at()
            .is_some_and(|available_at| available_at > sampled_received_at)
        {
            return Err(FileAdapterError::ClockFailure);
        }
        let row_limit = SourceRowLimit::from_output_limit(
            request.max_records(),
            specification.row_policy.fields.len(),
            self.limits.input.max_records,
        )?;
        let rows = self.parse_rows(
            specification,
            input.as_bytes(),
            cancellation,
            deadline,
            row_limit,
        )?;
        self.check_control(cancellation, deadline)?;
        let sampled_ingested_at = self.control_timestamp(cancellation, deadline)?;
        let operation_times = self.representation.operation_times(
            request.object().dataset(),
            request.object().object_id(),
            input.digest(),
            input.identity().size_bytes(),
            request.object().availability(),
            sampled_received_at,
            sampled_ingested_at,
        )?;
        let batch = self.rows_to_batch(
            request,
            specification,
            rows,
            operation_times,
            cancellation,
            deadline,
        )?;
        self.validate_authority(authority)?;
        Ok(batch)
    }

    async fn acquire_blocking_slot(
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<OwnedSemaphorePermit, FileAdapterError> {
        let slots = Arc::clone(&BLOCKING_SLOTS);
        let expiry = tokio::time::Instant::from_std(deadline.monotonic_expiry());
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(FileAdapterError::Cancelled),
            () = tokio::time::sleep_until(expiry) => Err(FileAdapterError::DeadlineExceeded),
            permit = slots.acquire_owned() => {
                permit.map_err(|_| FileAdapterError::BlockingTaskFailed)
            }
        }
    }

    async fn await_blocking<T>(
        mut worker: tokio::task::JoinHandle<Result<T, FileAdapterError>>,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<T, FileAdapterError> {
        let expiry = tokio::time::Instant::from_std(deadline.monotonic_expiry());
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                // Tokio cannot stop a running blocking closure. Aborting detaches it; the owned
                // semaphore permit remains inside the closure until that worker actually exits.
                worker.abort();
                Err(FileAdapterError::Cancelled)
            }
            () = tokio::time::sleep_until(expiry) => {
                worker.abort();
                Err(FileAdapterError::DeadlineExceeded)
            }
            result = &mut worker => {
                result.map_err(|_| FileAdapterError::BlockingTaskFailed)?
            }
        }
    }

    async fn seal_request_deadline(
        &self,
        wall_deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RequestDeadline, FileAdapterError> {
        let admission_expiry = tokio::time::Instant::now()
            .checked_add(self.limits.input.max_elapsed)
            .ok_or(FileAdapterError::ClockFailure)?;
        let sampling_slots = Arc::clone(&DEADLINE_SAMPLING_SLOTS);
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(FileAdapterError::Cancelled),
            () = tokio::time::sleep_until(admission_expiry) => {
                return Err(FileAdapterError::DeadlineExceeded);
            }
            permit = sampling_slots.acquire_owned() => {
                permit.map_err(|_| FileAdapterError::BlockingTaskFailed)?
            }
        };
        let clock = Arc::clone(&self.clock);
        let monotonic_expiry = admission_expiry.into_std();
        let mut sampling = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            RequestDeadline::seal(clock.as_ref(), wall_deadline, monotonic_expiry)
        });
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                // A running blocking call cannot be force-cancelled. Its owned permit keeps the
                // detached population strictly bounded until the trusted clock call returns.
                sampling.abort();
                Err(FileAdapterError::Cancelled)
            }
            () = tokio::time::sleep_until(admission_expiry) => {
                // Aborting a running blocking call detaches it. Its owned permit remains held
                // until the trusted clock returns, so the detached population stays bounded.
                sampling.abort();
                Err(FileAdapterError::DeadlineExceeded)
            }
            result = &mut sampling => {
                result.map_err(|_| FileAdapterError::BlockingTaskFailed)?
            }
        }
    }

    fn bind_object_availability(
        &self,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
        size_bytes: u64,
        sampled_at: Timestamp,
    ) -> Result<AvailabilityEvidence, FileAdapterError> {
        self.representation
            .bind_object(dataset, object_id, digest, size_bytes, sampled_at)
    }

    fn verify_object_availability(
        &self,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
        size_bytes: u64,
        availability: &AvailabilityEvidence,
    ) -> Result<(), FileAdapterError> {
        self.representation
            .verify_object(dataset, object_id, digest, size_bytes, availability)
    }

    fn read_object(
        &self,
        specification: &FileObjectSpec,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<BoundedInput, FileAdapterError> {
        let verified = self
            .root
            .resolve(&specification.path)
            .and_then(|file| file.open_bounded(self.limits.source_bytes()))
            .map_err(|_| FileAdapterError::InputCapability)?;
        verified
            .read_bounded_with_control(&FileInputReadControl {
                cancellation,
                deadline,
                clock: self.clock.as_ref(),
            })
            .map_err(map_controlled_input_error)
    }

    fn parse_rows(
        &self,
        specification: &FileObjectSpec,
        bytes: &[u8],
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
        row_limit: SourceRowLimit,
    ) -> Result<Vec<ParsedRow>, FileAdapterError> {
        let mut budget = ParseBudget::new(
            self.limits,
            cancellation,
            Arc::clone(&self.clock),
            deadline,
            row_limit,
        );
        let rows = match &specification.format {
            FileFormat::Csv { delimiter, .. } => csv::parse(bytes, *delimiter, &mut budget),
            FileFormat::Tsv { .. } => csv::parse(bytes, b'\t', &mut budget),
            FileFormat::Json { .. } => json::parse_json(bytes, &mut budget),
            FileFormat::Ndjson { .. } => json::parse_ndjson(bytes, &mut budget),
            FileFormat::Xml { record_element, .. } => {
                xml::parse(bytes, record_element, &mut budget)
            }
            FileFormat::Excel { formula_policy } => {
                excel::parse(bytes, *formula_policy, &mut budget)
            }
            FileFormat::Parquet { .. } => parquet::parse(bytes, &mut budget),
            FileFormat::Sqlite {
                table,
                columns,
                order_by,
            } => database::parse(bytes, table, columns, order_by, &mut budget),
            FileFormat::Ofx {
                account_id,
                currency,
            }
            | FileFormat::Qfx {
                account_id,
                currency,
            } => ofx::parse(bytes, account_id, currency, &mut budget),
        }?;
        let mut identities = BTreeSet::new();
        for row in &rows {
            let identity = row
                .fields
                .get(&specification.row_policy.identity_field)
                .ok_or(FileAdapterError::InvalidRecord)?
                .as_text()?;
            if identities.contains(identity) {
                return Err(FileAdapterError::DuplicateField);
            }
            budget.set_entry::<&str>()?;
            let _ = identities.insert(identity);
        }
        Ok(rows)
    }

    fn rows_to_batch(
        &self,
        request: &ExtractionRequest,
        specification: &FileObjectSpec,
        rows: Vec<ParsedRow>,
        operation_times: (Timestamp, Timestamp),
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<ExtractionBatch, FileAdapterError> {
        let (received_at, ingested_at) = operation_times;
        let record_availability = request.object().availability().clone();
        let domain_availability = domain_availability(&record_availability);
        let maximum_records = usize::try_from(request.max_records())
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::Records))?
            .min(self.limits.input.max_records);
        let expected = rows
            .len()
            .checked_mul(specification.row_policy.fields.len())
            .and_then(|records| {
                records.checked_add(usize::from(specification.universe_membership.is_some()))
            })
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::Records))?;
        if expected > maximum_records {
            return Err(FileAdapterError::ExtractionContract(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                },
            ));
        }
        let mut batch = ExtractionBatchAccumulator::try_new(request)
            .map_err(FileAdapterError::ExtractionContract)?;
        if let Some(membership) = &specification.universe_membership {
            let instrument_id = specification
                .instrument_binding
                .instrument_id()
                .ok_or(FileAdapterError::InvalidManifest)?;
            let interval = EffectiveInterval::new(membership.starts_at, membership.ends_at)
                .map_err(|_| FileAdapterError::InvalidManifest)?;
            let source_record = membership_reference(specification)?;
            let context = ResearchContext::new(
                ResearchProvenance::try_new(ResearchProvenanceInput {
                    source_id: self.metadata.source_id().clone(),
                    instrument_id: Some(instrument_id),
                    venue_id: None,
                    source_identifier: source_record,
                    source_timestamp: None,
                    received_at,
                    ingested_at,
                    quality: self.metadata.quality_ceiling(),
                    payload_reference: PayloadReference::SourceReference(
                        specification.object_id.clone(),
                    ),
                    availability: domain_availability.clone(),
                })
                .map_err(|_| FileAdapterError::Contract)?,
                ResearchTime::try_new_with_coordinates(
                    ResearchTemporalCoordinate::exact(membership.starts_at),
                    specification
                        .published_at
                        .map(ResearchTemporalCoordinate::exact),
                    RevisionNumber::new(specification.revision_number)
                        .map_err(|_| FileAdapterError::InvalidManifest)?,
                    specification
                        .superseded_at
                        .map(ResearchTemporalCoordinate::exact),
                )
                .map_err(|_| FileAdapterError::Contract)?,
            )
            .map_err(|_| FileAdapterError::Contract)?;
            let observation = ResearchObservation::UniverseMembership(
                UniverseMembershipObservation::new(context, membership.universe.clone(), interval)
                    .map_err(|_| FileAdapterError::Contract)?,
            );
            let payload =
                serde_json::to_vec(&observation).map_err(|_| FileAdapterError::Contract)?;
            let evidence =
                EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
            batch
                .push(
                    ExtractionRecord::try_new_with_time(
                        request,
                        SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
                            .map_err(|_| FileAdapterError::Contract)?,
                        ExactPayloadEvidence::from_content_digest(evidence),
                        ResearchTemporalCoordinate::exact(membership.starts_at),
                        specification
                            .published_at
                            .map(ResearchTemporalCoordinate::exact),
                        record_availability.clone(),
                        specification.revision.clone(),
                        specification
                            .superseded_at
                            .map(ResearchTemporalCoordinate::exact),
                        Bytes::from(payload),
                    )
                    .map_err(FileAdapterError::ExtractionContract)?,
                )
                .map_err(FileAdapterError::ExtractionContract)?;
        }
        for row in rows {
            self.check_control(cancellation, deadline)?;
            let row_id = row
                .fields
                .get(&specification.row_policy.identity_field)
                .ok_or(FileAdapterError::InvalidRecord)?
                .as_text()?;
            let source_row =
                SourceIdentifier::try_from(row_id).map_err(|_| FileAdapterError::InvalidRecord)?;
            let payload_reference = row_reference(specification, &row)?;
            for mapping in &specification.row_policy.fields {
                if received_at > ingested_at {
                    return Err(FileAdapterError::ClockFailure);
                }
                let text = row
                    .fields
                    .get(&mapping.source)
                    .ok_or(FileAdapterError::InvalidRecord)?
                    .as_text()?;
                let value = parse_decimal_lexeme(text)?;
                if value.scale() != mapping.decimal_scale {
                    return Err(FileAdapterError::DecimalScaleMismatch);
                }
                let context = ResearchContext::new(
                    ResearchProvenance::try_new(ResearchProvenanceInput {
                        source_id: self.metadata.source_id().clone(),
                        instrument_id: specification.instrument_binding.instrument_id(),
                        venue_id: None,
                        source_identifier: source_row.clone(),
                        source_timestamp: None,
                        received_at,
                        ingested_at,
                        quality: self.metadata.quality_ceiling(),
                        payload_reference: PayloadReference::SourceReference(
                            payload_reference.clone(),
                        ),
                        availability: domain_availability.clone(),
                    })
                    .map_err(|_| FileAdapterError::Contract)?,
                    ResearchTime::try_new_with_coordinates(
                        specification.record_time.effective.clone(),
                        specification.record_time.published.clone(),
                        RevisionNumber::new(specification.revision_number)
                            .map_err(|_| FileAdapterError::InvalidManifest)?,
                        specification.record_time.superseded.clone(),
                    )
                    .map_err(|_| FileAdapterError::Contract)?,
                )
                .map_err(|_| FileAdapterError::Contract)?;
                let observation =
                    ResearchObservation::AlternativeData(AlternativeDataObservation::new(
                        context,
                        specification.dataset.clone(),
                        mapping.field.clone(),
                        value,
                        mapping.unit.clone(),
                    ));
                let payload =
                    serde_json::to_vec(&observation).map_err(|_| FileAdapterError::Contract)?;
                let evidence =
                    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
                batch
                    .push(
                        ExtractionRecord::try_new_with_time(
                            request,
                            SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
                                .map_err(|_| FileAdapterError::Contract)?,
                            ExactPayloadEvidence::from_content_digest(evidence),
                            specification.record_time.effective.clone(),
                            specification.record_time.published.clone(),
                            record_availability.clone(),
                            specification.revision.clone(),
                            specification.record_time.superseded.clone(),
                            Bytes::from(payload),
                        )
                        .map_err(FileAdapterError::ExtractionContract)?,
                    )
                    .map_err(FileAdapterError::ExtractionContract)?;
            }
        }
        self.check_control(cancellation, deadline)?;
        let finished = batch
            .finish()
            .map_err(FileAdapterError::ExtractionContract)?;
        self.check_control(cancellation, deadline)?;
        Ok(finished)
    }

    fn check_control(
        &self,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<(), FileAdapterError> {
        if cancellation.is_cancelled() {
            return Err(FileAdapterError::Cancelled);
        }
        deadline.checkpoint(self.clock.as_ref())
    }

    fn validate_authority(&self, authority: &ExtractionAuthority) -> Result<(), FileAdapterError> {
        if authority.metadata() != self.metadata.as_ref() {
            return Err(FileAdapterError::AuthorityMismatch);
        }
        authority.validate_current().map_err(FileAdapterError::from)
    }

    fn control_timestamp(
        &self,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<Timestamp, FileAdapterError> {
        if cancellation.is_cancelled() {
            return Err(FileAdapterError::Cancelled);
        }
        deadline.trusted_timestamp(self.clock.as_ref())
    }
}

fn domain_availability(availability: &AvailabilityEvidence) -> DomainAvailabilityEvidence {
    match availability {
        AvailabilityEvidence::Observed {
            available_at,
            evidence,
        } => DomainAvailabilityEvidence::Evidenced {
            available_at: *available_at,
            evidence: evidence.clone(),
        },
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            DomainAvailabilityEvidence::LocalFirstObserved {
                observed_at: *observed_at,
            }
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => DomainAvailabilityEvidence::Inferred {
            inferred_at: *inferred_at,
            method: method.clone(),
        },
        AvailabilityEvidence::Unknown => DomainAvailabilityEvidence::Unknown,
    }
}

fn map_controlled_input_error(error: ControlledInputFileError) -> FileAdapterError {
    match error {
        ControlledInputFileError::Input(_) => FileAdapterError::InputCapability,
        ControlledInputFileError::Control(InputReadControlError::Cancelled) => {
            FileAdapterError::Cancelled
        }
        ControlledInputFileError::Control(InputReadControlError::DeadlineExceeded) => {
            FileAdapterError::DeadlineExceeded
        }
        ControlledInputFileError::Control(InputReadControlError::Unavailable) => {
            FileAdapterError::ClockFailure
        }
    }
}

fn row_reference(
    specification: &FileObjectSpec,
    row: &ParsedRow,
) -> Result<SourceIdentifier, FileAdapterError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/local-file-row/v2");
    hasher.update(specification.manifest_schema_version.to_be_bytes());
    hash_identifier(&mut hasher, &specification.dataset)?;
    hash_identifier(&mut hasher, &specification.object_id)?;
    specification.instrument_binding.bind_identity(&mut hasher);
    hasher.update(row.canonical_row_sha256);
    let digest = hasher.finalize();
    let mut reference = String::from("local-object-row:canonical-sha256:");
    for byte in digest {
        write!(&mut reference, "{byte:02x}").map_err(|_| FileAdapterError::Contract)?;
    }
    SourceIdentifier::try_from(reference).map_err(|_| FileAdapterError::Contract)
}

fn membership_reference(
    specification: &FileObjectSpec,
) -> Result<SourceIdentifier, FileAdapterError> {
    let membership = specification
        .universe_membership
        .as_ref()
        .ok_or(FileAdapterError::InvalidManifest)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/local-file-universe-membership/v1");
    hasher.update(specification.manifest_schema_version.to_be_bytes());
    hash_identifier(&mut hasher, &specification.dataset)?;
    hash_identifier(&mut hasher, &specification.object_id)?;
    specification.instrument_binding.bind_identity(&mut hasher);
    hash_identifier(&mut hasher, &membership.universe)?;
    hasher.update(membership.starts_at.unix_nanos().to_be_bytes());
    match membership.ends_at {
        Some(ends_at) => {
            hasher.update([1]);
            hasher.update(ends_at.unix_nanos().to_be_bytes());
        }
        None => hasher.update([0]),
    }
    let mut reference = String::from("local-object-membership:sha256:");
    for byte in hasher.finalize() {
        write!(&mut reference, "{byte:02x}").map_err(|_| FileAdapterError::Contract)?;
    }
    SourceIdentifier::try_from(reference).map_err(|_| FileAdapterError::Contract)
}

fn object_evidence(
    specification: &FileObjectSpec,
    content_digest: EvidenceDigest,
) -> Result<ExactPayloadEvidence, FileAdapterError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/local-file-object-binding/v1");
    hasher.update(specification.manifest_schema_version.to_be_bytes());
    hash_identifier(&mut hasher, &specification.dataset)?;
    hash_identifier(&mut hasher, &specification.object_id)?;
    specification.instrument_binding.bind_identity(&mut hasher);
    let digest = hasher.finalize();
    let mut version = String::from("sha256:");
    for byte in digest {
        write!(&mut version, "{byte:02x}").map_err(|_| FileAdapterError::Contract)?;
    }
    Ok(ExactPayloadEvidence::with_version_pinned_locator(
        content_digest,
        VersionPinnedSourceLocator::new(
            SourceIdentifier::try_from("market-squawk-local-file-object:v3")
                .map_err(|_| FileAdapterError::Contract)?,
            SourceIdentifier::try_from(version).map_err(|_| FileAdapterError::Contract)?,
        ),
    ))
}

fn hash_identifier(
    hasher: &mut Sha256,
    identifier: &SourceIdentifier,
) -> Result<(), FileAdapterError> {
    let bytes = identifier.as_str().as_bytes();
    hasher.update(
        u64::try_from(bytes.len())
            .map_err(|_| FileAdapterError::Contract)?
            .to_be_bytes(),
    );
    hasher.update(bytes);
    Ok(())
}

fn parse_decimal_lexeme(value: &str) -> Result<Decimal, FileAdapterError> {
    let Some(exponent_index) = value.find(['e', 'E']) else {
        return Decimal::from_str_exact(value).map_err(|_| FileAdapterError::InvalidDecimal);
    };
    let (base, exponent) = value.split_at(exponent_index);
    let exponent = exponent
        .get(1..)
        .ok_or(FileAdapterError::InvalidDecimal)?
        .parse::<i32>()
        .map_err(|_| FileAdapterError::InvalidDecimal)?;
    let (negative, unsigned) = match base.as_bytes().first() {
        Some(b'-') => (true, base.get(1..).ok_or(FileAdapterError::InvalidDecimal)?),
        Some(b'+') => (
            false,
            base.get(1..).ok_or(FileAdapterError::InvalidDecimal)?,
        ),
        _ => (false, base),
    };
    let (whole, fractional) = match unsigned.split_once('.') {
        Some((whole, fractional))
            if !whole.is_empty() && !fractional.is_empty() && !fractional.contains('.') =>
        {
            (whole, fractional)
        }
        Some(_) => return Err(FileAdapterError::InvalidDecimal),
        None if !unsigned.is_empty() => (unsigned, ""),
        None => return Err(FileAdapterError::InvalidDecimal),
    };
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(FileAdapterError::InvalidDecimal);
    }
    let scale = i32::try_from(fractional.len())
        .map_err(|_| FileAdapterError::InvalidDecimal)?
        .checked_sub(exponent)
        .ok_or(FileAdapterError::InvalidDecimal)?;
    if scale > i32::try_from(Decimal::MAX_SCALE).map_err(|_| FileAdapterError::InvalidDecimal)? {
        return Err(FileAdapterError::InvalidDecimal);
    }
    let extra_zeroes = if scale < 0 {
        scale
            .checked_neg()
            .ok_or(FileAdapterError::InvalidDecimal)?
    } else {
        0
    };
    if extra_zeroes
        > i32::try_from(Decimal::MAX_SCALE).map_err(|_| FileAdapterError::InvalidDecimal)?
    {
        return Err(FileAdapterError::InvalidDecimal);
    }
    let mut canonical = String::new();
    let zeroes = usize::try_from(extra_zeroes).map_err(|_| FileAdapterError::InvalidDecimal)?;
    let capacity = usize::from(negative)
        .checked_add(whole.len())
        .and_then(|bytes| bytes.checked_add(fractional.len()))
        .and_then(|bytes| bytes.checked_add(zeroes))
        .and_then(|bytes| bytes.checked_add(1))
        .ok_or(FileAdapterError::InvalidDecimal)?;
    canonical
        .try_reserve_exact(capacity)
        .map_err(|_| FileAdapterError::InvalidDecimal)?;
    if negative {
        canonical.push('-');
    }
    canonical.push_str(whole);
    canonical.push_str(fractional);
    canonical.extend(std::iter::repeat_n('0', zeroes));
    let mut decimal =
        Decimal::from_str_exact(&canonical).map_err(|_| FileAdapterError::InvalidDecimal)?;
    if scale > 0 {
        decimal
            .set_scale(u32::try_from(scale).map_err(|_| FileAdapterError::InvalidDecimal)?)
            .map_err(|_| FileAdapterError::InvalidDecimal)?;
    }
    Ok(decimal)
}

impl SourceMetadataProvider for FileExtractionSource {
    fn metadata(&self) -> &SourceMetadata {
        self.metadata.as_ref()
    }
}

impl ExtractionSource for FileExtractionSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> futures_util::future::BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(async move {
            self.discover_files(&authority, &request, &cancellation)
                .await
                .map_err(map_extraction_error)
        })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> futures_util::future::BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        Box::pin(async move {
            self.extract_file(&authority, &request, &cancellation)
                .await
                .map_err(map_extraction_error)
        })
    }
}

fn map_extraction_error(error: FileAdapterError) -> ExtractionSourceError {
    match error {
        FileAdapterError::Cancelled => ExtractionSourceError::Cancelled,
        FileAdapterError::DeadlineExceeded
        | FileAdapterError::LimitExceeded(ParserLimit::Elapsed) => {
            ExtractionSourceError::DeadlineExceeded
        }
        FileAdapterError::ClockFailure => {
            ExtractionSourceError::Source(SourceError::TrustedTimeUnavailable)
        }
        FileAdapterError::BlockingTaskFailed => {
            ExtractionSourceError::Source(SourceError::InvalidProtocolState)
        }
        FileAdapterError::ExtractionContract(error) => ExtractionSourceError::Contract(error),
        FileAdapterError::Authority(error) => ExtractionSourceError::Authority(error),
        _ => ExtractionSourceError::Source(SourceError::InvalidProtocolState),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::clock::{ExtractionClockError, ExtractionClockReading};

    #[derive(Debug)]
    struct FixedClock(ExtractionClockReading);

    impl ExtractionClock for FixedClock {
        fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
            Ok(self.0)
        }
    }

    #[tokio::test]
    async fn saturated_blocking_lane_honors_presealed_deadline() -> Result<(), Box<dyn Error>> {
        let mut permits = Vec::new();
        for _ in 0..MAX_CONCURRENT_BLOCKING_OPERATIONS {
            permits.push(Arc::clone(&BLOCKING_SLOTS).try_acquire_owned()?);
        }
        let origin = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .ok_or("test monotonic origin underflow")?;
        let clock = FixedClock(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(0),
            origin,
        ));
        let deadline = RequestDeadline::seal(
            &clock,
            Timestamp::from_unix_nanos(10),
            origin
                .checked_add(Duration::from_secs(1))
                .ok_or("test deadline overflow")?,
        )?;
        assert!(matches!(
            FileExtractionSource::acquire_blocking_slot(&CancellationToken::new(), deadline).await,
            Err(FileAdapterError::DeadlineExceeded)
        ));
        assert_eq!(permits.len(), MAX_CONCURRENT_BLOCKING_OPERATIONS);
        assert_eq!(
            map_controlled_input_error(market_squawk_platform::ControlledInputFileError::Control(
                market_squawk_platform::InputReadControlError::Cancelled,
            ),),
            FileAdapterError::Cancelled
        );
        assert_eq!(
            map_controlled_input_error(market_squawk_platform::ControlledInputFileError::Control(
                market_squawk_platform::InputReadControlError::DeadlineExceeded,
            ),),
            FileAdapterError::DeadlineExceeded
        );
        assert_eq!(
            map_controlled_input_error(market_squawk_platform::ControlledInputFileError::Control(
                market_squawk_platform::InputReadControlError::Unavailable,
            ),),
            FileAdapterError::ClockFailure
        );
        Ok(())
    }
}
