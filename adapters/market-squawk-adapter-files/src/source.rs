//! Manifest-bound source discovery, extraction, and canonical row mapping.

use std::collections::{BTreeSet, HashMap};
use std::fmt::{self, Write as _};
use std::str::FromStr as _;
use std::sync::{Arc, LazyLock, Mutex};

use bytes::Bytes;
use market_squawk_domain::{
    AlternativeDataObservation, AvailabilityEvidence as DomainAvailabilityEvidence, DataQuality,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, PayloadReference,
    ResearchContext, ResearchObservation, ResearchProvenance, ResearchProvenanceInput,
    ResearchTime, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    BoundedInput, ControlledInputFileError, InputReadCheckpoint, InputReadControl,
    InputReadControlError, UserAuthorizedInputRoot,
};
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryBatch, DiscoveryRequest, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest, ExtractionSource,
    ExtractionSourceError, NetworkAccessPolicy, SourceClass, SourceError, SourceMetadata,
    SourceMetadataProvider, SourceObject,
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
use crate::{csv, database, excel, json, ofx, parquet, xml};

const RECORD_SCHEMA: &str = "market-squawk-research-v1";
const MAX_CONCURRENT_BLOCKING_OPERATIONS: usize = 4;
const MAX_CONCURRENT_DEADLINE_SAMPLES: usize = 4;
const MAX_RETAINED_OBJECT_VERSIONS: usize = 16_384;
static BLOCKING_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_BLOCKING_OPERATIONS)));
static DEADLINE_SAMPLING_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_DEADLINE_SAMPLES)));

#[derive(Debug, Default)]
struct ObjectAvailabilityState {
    exact_observations: HashMap<(SourceIdentifier, EvidenceDigest), Timestamp>,
    latest_by_object: HashMap<SourceIdentifier, Timestamp>,
}

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
    availability: Arc<Mutex<ObjectAvailabilityState>>,
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
            .field("retained_object_versions", &"[BOUNDED AVAILABILITY STATE]")
            .field("clock", &"[PAIRED EXTRACTION CLOCK]")
            .finish()
    }
}

impl FileExtractionSource {
    /// Constructs an immutable source bound to exact manifest bytes and one local root.
    ///
    /// # Errors
    ///
    /// Rejects non-local/networked metadata, mismatched manifest evidence, duplicate objects,
    /// unsafe row policies, and unsupported manifest versions.
    pub fn try_new(
        metadata: SourceMetadata,
        root: UserAuthorizedInputRoot,
        manifest_input: BoundedInput,
        limits: ExtractionLimits,
    ) -> Result<Self, FileAdapterError> {
        Self::try_new_with_clock(
            metadata,
            root,
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
    /// Applies the same metadata, manifest, and row-policy validation as [`Self::try_new`].
    pub fn try_new_with_clock(
        metadata: SourceMetadata,
        root: UserAuthorizedInputRoot,
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
        let manifest_bytes = u64::try_from(manifest_input.as_bytes().len())
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::SourceBytes))?;
        if manifest_bytes > limits.source_bytes() {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::SourceBytes));
        }
        let expected = metadata
            .revision_evidence()
            .payload_evidence()
            .content_digest();
        if manifest_input.digest() != expected {
            return Err(FileAdapterError::ManifestEvidenceMismatch);
        }
        let manifest: FileSourceManifest = serde_json::from_slice(manifest_input.as_bytes())
            .map_err(|_| FileAdapterError::InvalidManifest)?;
        manifest.validate()?;
        Ok(Self {
            metadata: Arc::new(metadata),
            root,
            manifest: Arc::new(manifest),
            limits,
            availability: Arc::new(Mutex::new(ObjectAvailabilityState::default())),
            clock,
        })
    }

    /// Discovers exact manifest objects with fresh no-follow reads on a bounded blocking lane.
    pub async fn discover_files(
        &self,
        request: &DiscoveryRequest,
        cancellation: &CancellationToken,
    ) -> Result<DiscoveryBatch, FileAdapterError> {
        let deadline = self
            .seal_request_deadline(request.deadline(), cancellation)
            .await?;
        let permit = Self::acquire_blocking_slot(cancellation, deadline).await?;
        let source = self.clone();
        let request = request.clone();
        let cancellation = cancellation.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            source.discover_files_blocking(&request, &cancellation, deadline)
        })
        .await
        .map_err(|_| FileAdapterError::BlockingTaskFailed)?
    }

    fn discover_files_blocking(
        &self,
        request: &DiscoveryRequest,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<DiscoveryBatch, FileAdapterError> {
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
            let sampled_at = self.control_timestamp(cancellation, deadline)?;
            let availability = self.bind_object_availability(
                &specification.object_id,
                input.digest(),
                sampled_at,
            )?;
            let effective =
                EffectiveInterval::new(specification.effective_at, specification.superseded_at)
                    .map_err(|_| FileAdapterError::InvalidManifest)?;
            objects.push(
                SourceObject::try_new_with_availability(
                    self.metadata.source_id().clone(),
                    self.metadata.revision().clone(),
                    request,
                    specification.object_id.clone(),
                    specification.format.media_type()?,
                    ExactPayloadEvidence::from_content_digest(input.digest()),
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
        self.check_control(cancellation, deadline)?;
        Ok(batch)
    }

    /// Extracts one object on a bounded blocking lane into canonical observations.
    pub async fn extract_file(
        &self,
        request: &ExtractionRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExtractionBatch, FileAdapterError> {
        let deadline = self
            .seal_request_deadline(request.deadline(), cancellation)
            .await?;
        let permit = Self::acquire_blocking_slot(cancellation, deadline).await?;
        let source = self.clone();
        let request = request.clone();
        let cancellation = cancellation.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            source.extract_file_blocking(&request, &cancellation, deadline)
        })
        .await
        .map_err(|_| FileAdapterError::BlockingTaskFailed)?
    }

    fn extract_file_blocking(
        &self,
        request: &ExtractionRequest,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<ExtractionBatch, FileAdapterError> {
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
        let received_at = self.control_timestamp(cancellation, deadline)?;
        if request.object().evidence().content_digest() != input.digest()
            || request.object().expected_bytes() != Some(input.identity().size_bytes())
        {
            return Err(FileAdapterError::ObjectEvidenceMismatch);
        }
        self.verify_object_availability(
            request.object().object_id(),
            input.digest(),
            request.object().availability(),
        )?;
        if request
            .object()
            .availability()
            .reported_at()
            .is_some_and(|available_at| available_at > received_at)
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
        self.rows_to_batch(
            request,
            specification,
            rows,
            received_at,
            cancellation,
            deadline,
        )
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
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
        sampled_at: Timestamp,
    ) -> Result<AvailabilityEvidence, FileAdapterError> {
        let mut state = self
            .availability
            .lock()
            .map_err(|_| FileAdapterError::AvailabilityStateUnavailable)?;
        let key = (object_id.clone(), digest);
        if let Some(observed_at) = state.exact_observations.get(&key) {
            return Ok(AvailabilityEvidence::LocalFirstObserved {
                observed_at: *observed_at,
            });
        }
        if state.exact_observations.len() >= MAX_RETAINED_OBJECT_VERSIONS {
            return Err(FileAdapterError::AvailabilityStateExhausted);
        }
        let observed_at = match state.latest_by_object.get(object_id) {
            Some(previous) if sampled_at <= *previous => previous
                .checked_add_nanos(1)
                .map_err(|_| FileAdapterError::ClockFailure)?,
            _ => sampled_at,
        };
        let _ = state.exact_observations.insert(key, observed_at);
        let _ = state
            .latest_by_object
            .insert(object_id.clone(), observed_at);
        Ok(AvailabilityEvidence::LocalFirstObserved { observed_at })
    }

    fn verify_object_availability(
        &self,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
        availability: &AvailabilityEvidence,
    ) -> Result<(), FileAdapterError> {
        let state = self
            .availability
            .lock()
            .map_err(|_| FileAdapterError::AvailabilityStateUnavailable)?;
        let retained = state
            .exact_observations
            .get(&(object_id.clone(), digest))
            .copied();
        match (retained, availability) {
            (Some(retained), AvailabilityEvidence::LocalFirstObserved { observed_at })
                if retained == *observed_at =>
            {
                Ok(())
            }
            _ => Err(FileAdapterError::ObjectAvailabilityMismatch),
        }
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
            self.clock.as_ref(),
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
        received_at: Timestamp,
        cancellation: &CancellationToken,
        deadline: RequestDeadline,
    ) -> Result<ExtractionBatch, FileAdapterError> {
        let record_availability = request.object().availability().clone();
        let domain_availability = domain_availability(&record_availability);
        let maximum_records = usize::try_from(request.max_records())
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::Records))?
            .min(self.limits.input.max_records);
        let expected = rows
            .len()
            .checked_mul(specification.row_policy.fields.len())
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
                let ingested_at = self.control_timestamp(cancellation, deadline)?;
                if received_at > ingested_at {
                    return Err(FileAdapterError::ClockFailure);
                }
                let text = row
                    .fields
                    .get(&mapping.source)
                    .ok_or(FileAdapterError::InvalidRecord)?
                    .as_text()?;
                let value =
                    Decimal::from_str(text).map_err(|_| FileAdapterError::InvalidDecimal)?;
                if value.scale() != mapping.decimal_scale {
                    return Err(FileAdapterError::DecimalScaleMismatch);
                }
                let context = ResearchContext::new(
                    ResearchProvenance::try_new(ResearchProvenanceInput {
                        source_id: self.metadata.source_id().clone(),
                        instrument_id: None,
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
                    ResearchTime::new(
                        specification.effective_at,
                        specification.published_at,
                        RevisionNumber::new(specification.revision_number)
                            .map_err(|_| FileAdapterError::InvalidManifest)?,
                        specification.superseded_at,
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
                        ExtractionRecord::try_new(
                            request,
                            SourceIdentifier::try_from(RECORD_SCHEMA)
                                .map_err(|_| FileAdapterError::Contract)?,
                            ExactPayloadEvidence::from_content_digest(evidence),
                            specification.effective_at,
                            specification.published_at,
                            record_availability.clone(),
                            specification.revision.clone(),
                            specification.superseded_at,
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
    hasher.update(specification.object_id.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(row.canonical_row_sha256);
    let digest = hasher.finalize();
    let mut reference = String::from("local-object-row:canonical-sha256:");
    for byte in digest {
        write!(&mut reference, "{byte:02x}").map_err(|_| FileAdapterError::Contract)?;
    }
    SourceIdentifier::try_from(reference).map_err(|_| FileAdapterError::Contract)
}

impl SourceMetadataProvider for FileExtractionSource {
    fn metadata(&self) -> &SourceMetadata {
        self.metadata.as_ref()
    }
}

impl ExtractionSource for FileExtractionSource {
    fn discover(
        &self,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> futures_util::future::BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(async move {
            self.discover_files(&request, &cancellation)
                .await
                .map_err(map_extraction_error)
        })
    }

    fn extract(
        &self,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> futures_util::future::BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        Box::pin(async move {
            self.extract_file(&request, &cancellation)
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
