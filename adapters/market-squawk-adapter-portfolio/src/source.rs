//! Manifest-bound extraction over exact user-owned portfolio records.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    BoundedInput, ControlledInputFileError, InputFileError, InputReadCheckpoint, InputReadControl,
    InputReadControlError, LocalAuthorityStateStore, SecretReference, UserAuthorizedInputRoot,
};
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionError,
    ExtractionRecord, ExtractionRequest, ExtractionSource, ExtractionSourceError,
    MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, NetworkAccessPolicy, SourceClass,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject,
    payload_matches_exact_evidence,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::wire::RawEnvelopeWire;
use crate::{PortfolioExtractionSource, PortfolioImportError, PortfolioImportLimits};

const MANIFEST_SCHEMA_VERSION: u16 = 1;
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const RAW_RECORD_SCHEMA: &str = "market-squawk-portfolio-raw-v1";
const MEDIA_TYPE: &str = "application-market-squawk-portfolio-manifest-json";
const AVAILABILITY_EVIDENCE: &str = "user-owned-portfolio-manifest-observed";

/// Local portfolio source bound to one exact manifest beneath a retained user root.
pub struct PortfolioManifestExtractionSource {
    metadata: SourceMetadata,
    root: UserAuthorizedInputRoot,
    manifest_reference: PathBuf,
    manifest_bytes: Arc<[u8]>,
    manifest_digest: EvidenceDigest,
    manifest: Arc<PortfolioManifest>,
    importer: Arc<Mutex<PortfolioExtractionSource>>,
}

impl PortfolioManifestExtractionSource {
    /// Opens a source after validating exact manifest, metadata, and durable archive authority.
    ///
    /// The manifest contains versioned raw portfolio JSON payloads as JSON strings, preserving
    /// their exact bytes. Discovery and extraction re-open the same relative manifest beneath the
    /// retained root with no-follow two-pass reads. Accounting and normalization are delegated to
    /// [`PortfolioExtractionSource`].
    ///
    /// # Errors
    ///
    /// Rejects ambient/network metadata, a foreign or changed manifest, unsafe root references,
    /// malformed or excessive raw records, and unavailable durable importer state.
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is a distinct metadata, input, archive, or capacity authority"
    )]
    pub fn try_new(
        metadata: SourceMetadata,
        root: UserAuthorizedInputRoot,
        manifest_reference: impl AsRef<Path>,
        manifest_input: BoundedInput,
        archive: LocalAuthorityStateStore,
        credential: Option<SecretReference>,
        limits: PortfolioImportLimits,
    ) -> Result<Self, PortfolioManifestSourceError> {
        validate_metadata(&metadata, manifest_input.digest())?;
        let manifest_reference = manifest_reference.as_ref().to_path_buf();
        let reopened = root
            .resolve(&manifest_reference)?
            .open_bounded(MAX_MANIFEST_BYTES)?
            .read_bounded()?;
        if reopened.digest() != manifest_input.digest()
            || reopened.as_bytes() != manifest_input.as_bytes()
        {
            return Err(PortfolioManifestSourceError::ManifestChanged);
        }
        let manifest = PortfolioManifest::parse(manifest_input.as_bytes(), limits)?;
        let importer = PortfolioExtractionSource::try_new(
            metadata.source_id().clone(),
            metadata.revision().clone(),
            metadata.quality_ceiling(),
            archive,
            credential,
            limits,
        )?;
        let manifest_digest = manifest_input.digest();
        let manifest_bytes = Arc::<[u8]>::from(manifest_input.into_bytes());
        Ok(Self {
            metadata,
            root,
            manifest_reference,
            manifest_bytes,
            manifest_digest,
            manifest: Arc::new(manifest),
            importer: Arc::new(Mutex::new(importer)),
        })
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), ExtractionSourceError> {
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            return Err(SourceError::InvalidProtocolState.into());
        }
        Ok(())
    }

    async fn revalidate_manifest(
        &self,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<(), ExtractionSourceError> {
        let root = self.root.clone();
        let reference = self.manifest_reference.clone();
        let expected_bytes = Arc::clone(&self.manifest_bytes);
        let expected_digest = self.manifest_digest;
        let operation_cancellation = cancellation.clone();
        let result = tokio::task::spawn_blocking(move || {
            let control = ManifestReadControl {
                deadline,
                cancellation: operation_cancellation,
            };
            let input = root
                .resolve(reference)
                .map_err(PortfolioManifestSourceError::from)?
                .open_bounded(MAX_MANIFEST_BYTES)
                .map_err(PortfolioManifestSourceError::from)?
                .read_bounded_with_control(&control)
                .map_err(map_controlled_input)?;
            if input.digest() != expected_digest || input.as_bytes() != expected_bytes.as_ref() {
                return Err(PortfolioManifestSourceError::ManifestChanged);
            }
            Ok(())
        })
        .await
        .map_err(|_| SourceError::InvalidProtocolState)?;
        result.map_err(map_runtime_error)
    }

    async fn discover_manifest(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        ensure_open(request.deadline(), &cancellation)?;
        if request.dataset() != &self.manifest.dataset {
            return DiscoveryBatch::try_new(&request, Vec::new()).map_err(Into::into);
        }
        self.revalidate_manifest(request.deadline(), cancellation.clone())
            .await?;
        authority.validate_current()?;
        ensure_open(request.deadline(), &cancellation)?;
        let effective = self.manifest.effective;
        if request.effective_at().is_some_and(|at| {
            at < effective.starts_at() || effective.ends_at().is_some_and(|ends_at| at >= ends_at)
        }) {
            return DiscoveryBatch::try_new(&request, Vec::new()).map_err(Into::into);
        }
        let object = self
            .manifest
            .source_object(&self.metadata, self.manifest_digest, &request)?;
        DiscoveryBatch::try_new(&request, vec![object]).map_err(Into::into)
    }

    async fn extract_manifest(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_sources::ExtractionBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        ensure_open(request.deadline(), &cancellation)?;
        self.revalidate_manifest(request.deadline(), cancellation.clone())
            .await?;
        authority.validate_current()?;
        ensure_open(request.deadline(), &cancellation)?;
        if !self
            .manifest
            .matches_object(&self.metadata, self.manifest_digest, request.object())?
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let raw = self.manifest.raw_batch(&request, &cancellation)?;
        let importer = Arc::clone(&self.importer);
        let imported = tokio::task::spawn_blocking(move || {
            importer
                .lock()
                .map_err(|_| PortfolioImportError::ArchiveUnavailable)?
                .import_batch(&raw)
                .map(|imported| imported.normalized_batch().clone())
        })
        .await
        .map_err(|_| SourceError::InvalidProtocolState)?
        .map_err(|_| SourceError::InvalidProtocolState)?;
        authority.validate_current()?;
        ensure_open(request.deadline(), &cancellation)?;
        Ok(imported)
    }
}

impl std::fmt::Debug for PortfolioManifestExtractionSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PortfolioManifestExtractionSource")
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("root", &"[USER-AUTHORIZED]")
            .field("manifest", &"[EXACT RETAINED BYTES]")
            .field("record_count", &self.manifest.records.len())
            .field("importer", &"[DURABLE PORTFOLIO AUTHORITY]")
            .finish()
    }
}

impl SourceMetadataProvider for PortfolioManifestExtractionSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for PortfolioManifestExtractionSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<DiscoveryBatch, ExtractionSourceError>> + Send + '_>>
    {
        Box::pin(self.discover_manifest(authority, request, cancellation))
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<market_squawk_sources::ExtractionBatch, ExtractionSourceError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(self.extract_manifest(authority, request, cancellation))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PortfolioManifestWire {
    schema_version: u16,
    dataset: String,
    object_id: String,
    effective_at_unix_nanos: i64,
    #[serde(default)]
    effective_until_unix_nanos: Option<i64>,
    #[serde(default)]
    published_at_unix_nanos: Option<i64>,
    available_at_unix_nanos: i64,
    records: Vec<PortfolioManifestRecordWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PortfolioManifestRecordWire {
    revision: String,
    payload: String,
}

struct PortfolioManifest {
    dataset: SourceIdentifier,
    object_id: SourceIdentifier,
    effective: EffectiveInterval,
    published_at: Option<Timestamp>,
    availability: AvailabilityEvidence,
    records: Vec<PortfolioManifestRecord>,
    payload_bytes: u64,
}

struct PortfolioManifestRecord {
    revision: SourceIdentifier,
    payload: Bytes,
    evidence: ExactPayloadEvidence,
}

impl PortfolioManifest {
    fn parse(
        bytes: &[u8],
        limits: PortfolioImportLimits,
    ) -> Result<Self, PortfolioManifestSourceError> {
        if bytes.is_empty()
            || u64::try_from(bytes.len()).map_or(true, |len| len > MAX_MANIFEST_BYTES)
        {
            return Err(PortfolioManifestSourceError::InvalidManifest);
        }
        let wire: PortfolioManifestWire = serde_json::from_slice(bytes)
            .map_err(|_| PortfolioManifestSourceError::InvalidManifest)?;
        if wire.schema_version != MANIFEST_SCHEMA_VERSION
            || wire.records.is_empty()
            || wire.records.len() > limits.max_archive_records
            || wire.records.len() > MAX_EXTRACTION_RECORDS
        {
            return Err(PortfolioManifestSourceError::InvalidManifest);
        }
        let dataset = identifier(wire.dataset)?;
        let object_id = identifier(wire.object_id)?;
        let effective = EffectiveInterval::new(
            Timestamp::from_unix_nanos(wire.effective_at_unix_nanos),
            wire.effective_until_unix_nanos
                .map(Timestamp::from_unix_nanos),
        )
        .map_err(|_| PortfolioManifestSourceError::InvalidManifest)?;
        let published_at = wire.published_at_unix_nanos.map(Timestamp::from_unix_nanos);
        let available_at = Timestamp::from_unix_nanos(wire.available_at_unix_nanos);
        if published_at.is_some_and(|published| available_at < published) {
            return Err(PortfolioManifestSourceError::InvalidManifest);
        }
        let availability = AvailabilityEvidence::Observed {
            available_at,
            evidence: identifier(AVAILABILITY_EVIDENCE.to_owned())?,
        };
        let mut payload_bytes = 0_u64;
        let mut records = Vec::new();
        records
            .try_reserve_exact(wire.records.len())
            .map_err(|_| PortfolioManifestSourceError::Allocation)?;
        for record in wire.records {
            let payload = Bytes::from(record.payload);
            serde_json::from_slice::<RawEnvelopeWire>(&payload)
                .map_err(|_| PortfolioManifestSourceError::InvalidManifest)?;
            let bytes = u64::try_from(payload.len())
                .map_err(|_| PortfolioManifestSourceError::InvalidManifest)?;
            payload_bytes = payload_bytes
                .checked_add(bytes)
                .ok_or(PortfolioManifestSourceError::InvalidManifest)?;
            if payload_bytes > limits.max_archive_bytes
                || payload_bytes > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
            {
                return Err(PortfolioManifestSourceError::InvalidManifest);
            }
            let digest =
                EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
            records.push(PortfolioManifestRecord {
                revision: identifier(record.revision)?,
                payload,
                evidence: ExactPayloadEvidence::from_content_digest(digest),
            });
        }
        Ok(Self {
            dataset,
            object_id,
            effective,
            published_at,
            availability,
            records,
            payload_bytes,
        })
    }

    fn source_object(
        &self,
        metadata: &SourceMetadata,
        manifest_digest: EvidenceDigest,
        request: &DiscoveryRequest,
    ) -> Result<SourceObject, ExtractionSourceError> {
        SourceObject::try_new_with_availability(
            metadata.source_id().clone(),
            metadata.revision().clone(),
            request,
            self.object_id.clone(),
            identifier(MEDIA_TYPE.to_owned()).map_err(map_runtime_error)?,
            ExactPayloadEvidence::from_content_digest(manifest_digest),
            self.effective,
            self.published_at,
            self.availability.clone(),
            Some(self.payload_bytes),
        )
        .map_err(Into::into)
    }

    fn matches_object(
        &self,
        metadata: &SourceMetadata,
        manifest_digest: EvidenceDigest,
        object: &SourceObject,
    ) -> Result<bool, ExtractionSourceError> {
        let request = DiscoveryRequest::try_new(
            object.dataset().clone(),
            None,
            std::num::NonZeroU16::MIN,
            Timestamp::from_unix_nanos(i64::MAX),
        )?;
        let expected = self.source_object(metadata, manifest_digest, &request)?;
        Ok(object.source_id() == expected.source_id()
            && object.metadata_revision() == expected.metadata_revision()
            && object.dataset() == expected.dataset()
            && object.object_id() == expected.object_id()
            && object.media_type() == expected.media_type()
            && object.evidence() == expected.evidence()
            && object.effective_interval() == expected.effective_interval()
            && object.published_at() == expected.published_at()
            && object.availability() == expected.availability()
            && object.expected_bytes() == expected.expected_bytes())
    }

    fn raw_batch(
        &self,
        request: &ExtractionRequest,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_sources::ExtractionBatch, ExtractionSourceError> {
        let mut batch = market_squawk_sources::ExtractionBatchAccumulator::try_new(request)?;
        for record in &self.records {
            ensure_open(request.deadline(), cancellation)?;
            if !payload_matches_exact_evidence(&record.payload, &record.evidence) {
                return Err(SourceError::InvalidProtocolState.into());
            }
            batch.push(ExtractionRecord::try_new(
                request,
                identifier(RAW_RECORD_SCHEMA.to_owned()).map_err(map_runtime_error)?,
                record.evidence.clone(),
                self.effective.starts_at(),
                self.published_at,
                self.availability.clone(),
                record.revision.clone(),
                None,
                record.payload.clone(),
            )?)?;
        }
        batch.finish().map_err(Into::into)
    }
}

struct ManifestReadControl {
    deadline: Timestamp,
    cancellation: CancellationToken,
}

impl InputReadControl for ManifestReadControl {
    fn checkpoint(&self, _checkpoint: InputReadCheckpoint) -> Result<(), InputReadControlError> {
        if self.cancellation.is_cancelled() {
            return Err(InputReadControlError::Cancelled);
        }
        let now = system_timestamp().map_err(|_| InputReadControlError::Unavailable)?;
        if now >= self.deadline {
            Err(InputReadControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

fn validate_metadata(
    metadata: &SourceMetadata,
    manifest_digest: EvidenceDigest,
) -> Result<(), PortfolioManifestSourceError> {
    if metadata.source_class() != SourceClass::PortfolioExport
        || !matches!(metadata.network_policy(), NetworkAccessPolicy::Denied)
        || metadata.coverage().domain() != market_squawk_sources::CoverageDomain::Portfolio
        || !metadata.capabilities().extraction()
        || metadata.capabilities().live()
        || metadata.quality_ceiling() == DataQuality::DirectVerified
        || metadata
            .revision_evidence()
            .payload_evidence()
            .content_digest()
            != manifest_digest
    {
        return Err(PortfolioManifestSourceError::InvalidMetadata);
    }
    Ok(())
}

fn ensure_open(
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(), ExtractionSourceError> {
    if cancellation.is_cancelled() {
        return Err(ExtractionSourceError::Cancelled);
    }
    if system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)? >= deadline {
        return Err(ExtractionSourceError::DeadlineExceeded);
    }
    Ok(())
}

fn system_timestamp() -> Result<Timestamp, PortfolioManifestSourceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PortfolioManifestSourceError::Clock)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_| PortfolioManifestSourceError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn identifier(value: String) -> Result<SourceIdentifier, PortfolioManifestSourceError> {
    SourceIdentifier::try_from(value).map_err(|_| PortfolioManifestSourceError::InvalidManifest)
}

fn map_controlled_input(error: ControlledInputFileError) -> PortfolioManifestSourceError {
    match error {
        ControlledInputFileError::Control(InputReadControlError::Cancelled) => {
            PortfolioManifestSourceError::Cancelled
        }
        ControlledInputFileError::Control(InputReadControlError::DeadlineExceeded) => {
            PortfolioManifestSourceError::DeadlineExceeded
        }
        ControlledInputFileError::Control(InputReadControlError::Unavailable) => {
            PortfolioManifestSourceError::Clock
        }
        ControlledInputFileError::Input(error) => error.into(),
    }
}

fn map_runtime_error(error: PortfolioManifestSourceError) -> ExtractionSourceError {
    match error {
        PortfolioManifestSourceError::Cancelled => ExtractionSourceError::Cancelled,
        PortfolioManifestSourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        PortfolioManifestSourceError::Contract(error) => error.into(),
        _ => SourceError::InvalidProtocolState.into(),
    }
}

/// Portfolio manifest, capability, archive, or extraction construction failure.
#[derive(Debug, Error)]
pub enum PortfolioManifestSourceError {
    /// Source metadata is not exact local portfolio extraction authority.
    #[error("portfolio manifest source metadata is invalid")]
    InvalidMetadata,
    /// Manifest schema, bounds, identifiers, timestamps, or raw records are invalid.
    #[error("portfolio manifest is invalid")]
    InvalidManifest,
    /// The retained manifest changed beneath the user-authorized root.
    #[error("portfolio manifest changed")]
    ManifestChanged,
    /// A bounded allocation failed.
    #[error("portfolio manifest allocation failed")]
    Allocation,
    /// The caller cancelled manifest validation.
    #[error("portfolio manifest operation was cancelled")]
    Cancelled,
    /// The operation deadline elapsed.
    #[error("portfolio manifest operation exceeded its deadline")]
    DeadlineExceeded,
    /// Trusted local time is unavailable.
    #[error("portfolio manifest trusted time is unavailable")]
    Clock,
    /// User-root resolution or two-pass input validation failed.
    #[error(transparent)]
    Input(#[from] InputFileError),
    /// Portfolio raw archive or normalization rejected the source.
    #[error(transparent)]
    Import(#[from] PortfolioImportError),
    /// Canonical extraction construction rejected the source.
    #[error(transparent)]
    Contract(#[from] ExtractionError),
}
