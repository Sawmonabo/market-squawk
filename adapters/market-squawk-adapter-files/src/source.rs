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
    BoundedInput, ControlledImportInputRoot, ControlledInputFileError, InputReadCheckpoint,
    InputReadControl, InputReadControlError, UserAuthorizedInputRoot,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionBatchAccumulator, ExtractionRecord,
    ExtractionRequest, ExtractionSource, ExtractionSourceError, NetworkAccessPolicy, SourceClass,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::clock::{ExtractionClock, RequestDeadline, SystemExtractionClock};
use crate::contracts::{
    ExtractionLimits, FileAdapterError, ParseBudget, ParsedRow, ParserLimit, SourceRowLimit,
};
use crate::manifest::{FileObjectSpec, FileSourceManifest};
use crate::parse::{parse_decimal_lexeme, parse_rows};
use crate::representation::FileRepresentationAuthority;

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

/// A manifest-bound extraction source over one retained local input root.
#[derive(Clone)]
pub struct FileExtractionSource {
    metadata: Arc<SourceMetadata>,
    root: FileInputRoot,
    manifest: Arc<FileSourceManifest>,
    limits: ExtractionLimits,
    representation: Arc<FileRepresentationAuthority>,
    clock: Arc<dyn ExtractionClock>,
}

#[derive(Clone)]
enum FileInputRoot {
    UserAuthorized(UserAuthorizedInputRoot),
    ControlledImport(ControlledImportInputRoot),
}

impl FileInputRoot {
    fn resolve(
        &self,
        relative: impl AsRef<Path>,
    ) -> Result<market_squawk_platform::InputFileCapability, market_squawk_platform::InputFileError>
    {
        match self {
            Self::UserAuthorized(root) => root.resolve(relative),
            Self::ControlledImport(root) => root.resolve(relative),
        }
    }

    fn ensure_disjoint_root(&self, candidate: &Path) -> Result<(), FileAdapterError> {
        match self {
            Self::UserAuthorized(root) => root.ensure_disjoint_root(candidate),
            Self::ControlledImport(root) => root.ensure_disjoint_root(candidate),
        }
        .map_err(|_| FileAdapterError::RepresentationAuthorityScope)
    }
}

impl fmt::Debug for FileExtractionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileExtractionSource")
            .field("metadata", &self.metadata)
            .field("root", &"[RETAINED LOCAL INPUT ROOT]")
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
        Self::try_new_with_input_root(
            metadata,
            FileInputRoot::UserAuthorized(root),
            representation_state_root.as_ref(),
            manifest_input,
            limits,
            clock,
        )
    }

    /// Constructs an immutable source over a committed controlled-import directory.
    ///
    /// The input root must originate beneath the retained artifact repository through
    /// [`market_squawk_platform::ArtifactRoot::open_controlled_import_root`]. This constructor does
    /// not claim original user-root ownership; imported ownership/admission evidence remains a
    /// separate durable data-layer decision.
    ///
    /// # Errors
    ///
    /// Applies the same metadata, manifest, row-policy, exact-object, disjoint-state, and durable
    /// representation validation as [`Self::try_new`].
    pub fn try_new_controlled_import(
        metadata: SourceMetadata,
        root: ControlledImportInputRoot,
        representation_state_root: impl AsRef<Path>,
        manifest_input: BoundedInput,
        limits: ExtractionLimits,
    ) -> Result<Self, FileAdapterError> {
        Self::try_new_controlled_import_with_clock(
            metadata,
            root,
            representation_state_root,
            manifest_input,
            limits,
            Arc::new(SystemExtractionClock),
        )
    }

    /// Constructs a controlled-import source with an explicitly injected paired clock.
    ///
    /// # Errors
    ///
    /// Applies the same validation as [`Self::try_new_controlled_import`].
    pub fn try_new_controlled_import_with_clock(
        metadata: SourceMetadata,
        root: ControlledImportInputRoot,
        representation_state_root: impl AsRef<Path>,
        manifest_input: BoundedInput,
        limits: ExtractionLimits,
        clock: Arc<dyn ExtractionClock>,
    ) -> Result<Self, FileAdapterError> {
        Self::try_new_with_input_root(
            metadata,
            FileInputRoot::ControlledImport(root),
            representation_state_root.as_ref(),
            manifest_input,
            limits,
            clock,
        )
    }

    fn try_new_with_input_root(
        metadata: SourceMetadata,
        root: FileInputRoot,
        representation_state_root: &Path,
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
        root.ensure_disjoint_root(representation_state_root)?;
        let representation = FileRepresentationAuthority::try_open(
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
        let rows = parse_rows(&specification.format, bytes, &mut budget)?;
        validate_mapped_rows(specification, &rows, &mut budget)?;
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
        let object_domain_availability = domain_availability(&record_availability);
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
                    availability: object_domain_availability.clone(),
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
            let row_time = resolve_row_time(
                specification,
                &row,
                &record_availability,
                &payload_reference,
            )?;
            if received_at > ingested_at {
                return Err(FileAdapterError::ClockFailure);
            }
            for mapping in &specification.row_policy.fields {
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
                        availability: row_time.domain_availability.clone(),
                    })
                    .map_err(|_| FileAdapterError::Contract)?,
                    row_time.research_time.clone(),
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
                            row_time.research_time.effective().clone(),
                            row_time.research_time.published().cloned(),
                            row_time.availability.clone(),
                            row_time.revision.clone(),
                            row_time.research_time.superseded().cloned(),
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

struct ResolvedRowTime {
    research_time: ResearchTime,
    availability: AvailabilityEvidence,
    domain_availability: DomainAvailabilityEvidence,
    revision: SourceIdentifier,
}

pub(crate) fn validate_mapped_rows(
    specification: &FileObjectSpec,
    rows: &[ParsedRow],
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let mut identities = BTreeSet::new();
    let fallback_availability = AvailabilityEvidence::Unknown;
    for row in rows {
        let identity = row
            .fields
            .get(&specification.row_policy.identity_field)
            .ok_or(FileAdapterError::InvalidRecord)?
            .as_text()?;
        SourceIdentifier::try_from(identity).map_err(|_| FileAdapterError::InvalidRecord)?;
        if identities.contains(identity) {
            return Err(FileAdapterError::DuplicateField);
        }
        budget.set_entry::<&str>()?;
        let _ = identities.insert(identity);
        let row_evidence = row_reference(specification, row)?;
        let _ = resolve_row_time(specification, row, &fallback_availability, &row_evidence)?;
        for mapping in &specification.row_policy.fields {
            let text = row
                .fields
                .get(&mapping.source)
                .ok_or(FileAdapterError::InvalidRecord)?
                .as_text()?;
            let value = parse_decimal_lexeme(text)?;
            if value.scale() != mapping.decimal_scale {
                return Err(FileAdapterError::DecimalScaleMismatch);
            }
        }
    }
    Ok(())
}

fn resolve_row_time(
    specification: &FileObjectSpec,
    row: &ParsedRow,
    object_availability: &AvailabilityEvidence,
    row_evidence: &SourceIdentifier,
) -> Result<ResolvedRowTime, FileAdapterError> {
    let Some(fields) = specification.row_time.as_ref() else {
        let availability = object_availability.clone();
        return Ok(ResolvedRowTime {
            research_time: ResearchTime::try_new_with_coordinates(
                specification.record_time.effective.clone(),
                specification.record_time.published.clone(),
                RevisionNumber::new(specification.revision_number)
                    .map_err(|_| FileAdapterError::InvalidManifest)?,
                specification.record_time.superseded.clone(),
            )
            .map_err(|_| FileAdapterError::InvalidManifest)?,
            domain_availability: domain_availability(&availability),
            availability,
            revision: specification.revision.clone(),
        });
    };
    let effective = fields.effective_field.as_deref().map_or_else(
        || Ok(specification.record_time.effective.clone()),
        |field| mapped_timestamp(row, field).map(ResearchTemporalCoordinate::exact),
    )?;
    let published = fields.published_field.as_deref().map_or_else(
        || Ok(specification.record_time.published.clone()),
        |field| {
            mapped_timestamp(row, field)
                .map(ResearchTemporalCoordinate::exact)
                .map(Some)
        },
    )?;
    let superseded = fields.superseded_field.as_deref().map_or_else(
        || Ok(specification.record_time.superseded.clone()),
        |field| {
            mapped_timestamp(row, field)
                .map(ResearchTemporalCoordinate::exact)
                .map(Some)
        },
    )?;
    let revision_number = fields.revision_number_field.as_deref().map_or_else(
        || {
            RevisionNumber::new(specification.revision_number)
                .map_err(|_| FileAdapterError::InvalidManifest)
        },
        |field| parse_revision_number(mapped_text(row, field)?),
    )?;
    let revision = fields.revision_field.as_deref().map_or_else(
        || Ok(specification.revision.clone()),
        |field| {
            SourceIdentifier::try_from(mapped_text(row, field)?)
                .map_err(|_| FileAdapterError::InvalidRecord)
        },
    )?;
    let availability = if let Some(field) = fields.available_field.as_deref() {
        AvailabilityEvidence::Observed {
            available_at: mapped_timestamp(row, field)?,
            evidence: row_evidence.clone(),
        }
    } else {
        object_availability.clone()
    };
    let research_time =
        ResearchTime::try_new_with_coordinates(effective, published, revision_number, superseded)
            .map_err(|_| FileAdapterError::InvalidRecord)?;
    Ok(ResolvedRowTime {
        research_time,
        domain_availability: domain_availability(&availability),
        availability,
        revision,
    })
}

fn mapped_text<'a>(row: &'a ParsedRow, field: &str) -> Result<&'a str, FileAdapterError> {
    row.fields
        .get(field)
        .ok_or(FileAdapterError::InvalidRecord)?
        .as_text()
}

fn mapped_timestamp(row: &ParsedRow, field: &str) -> Result<Timestamp, FileAdapterError> {
    parse_unix_nanos(mapped_text(row, field)?)
}

fn parse_unix_nanos(value: &str) -> Result<Timestamp, FileAdapterError> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    if digits.is_empty()
        || value.starts_with('+')
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(FileAdapterError::InvalidRecord);
    }
    value
        .parse::<i64>()
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| FileAdapterError::InvalidRecord)
}

fn parse_revision_number(value: &str) -> Result<RevisionNumber, FileAdapterError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(FileAdapterError::InvalidRecord);
    }
    let value = value
        .parse::<u32>()
        .map_err(|_| FileAdapterError::InvalidRecord)?;
    RevisionNumber::new(value).map_err(|_| FileAdapterError::InvalidRecord)
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
