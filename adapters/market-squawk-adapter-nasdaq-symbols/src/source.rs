use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AssetClass, CoverageDelay, DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    AuthorizationMode, AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, CoverageDomain,
    DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest, ExtractionSource,
    ExtractionSourceError, HistoricalCapability, SourceClass, SourceError, SourceMetadata,
    SourceMetadataProvider, SourceObject, SourceProtocolProfile, payload_matches_exact_evidence,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::client::{NasdaqHttpClient, RetrievedDirectory, ensure_deadline_open, system_timestamp};
use crate::model::{NasdaqDirectoryKind, NasdaqListingRecord};
use crate::parser::{NasdaqParseError, ParsedDirectory, parse_directory};

/// Official current Nasdaq-listed Symbol Directory object.
pub const NASDAQ_LISTED_URL: &str = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt";
/// Official current other-exchange-listed Symbol Directory object.
pub const OTHER_LISTED_URL: &str = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt";
/// Stable dataset namespace used for explicit discovery requests.
pub const NASDAQ_SYMBOL_DIRECTORY_DATASET: &str = "nasdaq.symbol-directory.us-listed.v1";
/// Provider identity required by the adapter's immutable source metadata.
pub const NASDAQ_SYMBOL_DIRECTORY_PROVIDER: &str = "nasdaq-trader-symbol-directory";
/// Exact listing-venue MICs represented by the two current official files.
pub const NASDAQ_SYMBOL_DIRECTORY_VENUES: [&str; 7] =
    ["XNAS", "XASE", "XNYS", "ARCX", "XCHI", "BATS", "IEXG"];

const MAX_CACHED_OBJECTS: usize = 4;
const MEDIA_TYPE: &str = "text/plain";

/// Exact, fixed-endpoint Symbol Directory source configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NasdaqSymbolDirectoryConfig {
    dataset: SourceIdentifier,
}

impl NasdaqSymbolDirectoryConfig {
    /// Constructs the stable complete current-directory dataset contract.
    ///
    /// # Errors
    ///
    /// Returns [`NasdaqSymbolDirectorySourceError::InvalidConfiguration`] if the code-owned
    /// dataset identity no longer satisfies domain bounds.
    pub fn try_new() -> Result<Self, NasdaqSymbolDirectorySourceError> {
        let dataset = SourceIdentifier::try_from(NASDAQ_SYMBOL_DIRECTORY_DATASET)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidConfiguration)?;
        Ok(Self { dataset })
    }

    /// Returns the exact dataset identity callers must use for discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }
}

/// Bounded health for one independently fetched directory file.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NasdaqDirectoryHealth {
    last_attempt_at: Option<Timestamp>,
    last_success_at: Option<Timestamp>,
    last_payload_digest: Option<[u8; 32]>,
    consecutive_failures: u32,
}

impl NasdaqDirectoryHealth {
    /// Returns the most recent attempted request time.
    pub const fn last_attempt_at(self) -> Option<Timestamp> {
        self.last_attempt_at
    }

    /// Returns the most recent fully validated response time.
    pub const fn last_success_at(self) -> Option<Timestamp> {
        self.last_success_at
    }

    /// Returns the SHA-256 digest of the most recent fully validated exact file.
    pub const fn last_payload_digest(self) -> Option<[u8; 32]> {
        self.last_payload_digest
    }

    /// Returns consecutive failed fetch or validation attempts.
    pub const fn consecutive_failures(self) -> u32 {
        self.consecutive_failures
    }
}

#[derive(Debug, Default)]
struct HealthState {
    nasdaq_listed: NasdaqDirectoryHealth,
    other_listed: NasdaqDirectoryHealth,
}

impl HealthState {
    fn get(&self, kind: NasdaqDirectoryKind) -> NasdaqDirectoryHealth {
        match kind {
            NasdaqDirectoryKind::NasdaqListed => self.nasdaq_listed,
            NasdaqDirectoryKind::OtherListed => self.other_listed,
        }
    }

    fn get_mut(&mut self, kind: NasdaqDirectoryKind) -> &mut NasdaqDirectoryHealth {
        match kind {
            NasdaqDirectoryKind::NasdaqListed => &mut self.nasdaq_listed,
            NasdaqDirectoryKind::OtherListed => &mut self.other_listed,
        }
    }
}

#[derive(Clone, Debug)]
struct CachedDirectory {
    object_id: SourceIdentifier,
    retrieved: RetrievedDirectory,
    parsed: ParsedDirectory,
}

#[derive(Debug, Default)]
struct DirectoryCache {
    entries: VecDeque<Arc<CachedDirectory>>,
}

impl DirectoryCache {
    fn find(&self, object_id: &SourceIdentifier) -> Option<Arc<CachedDirectory>> {
        self.entries
            .iter()
            .find(|entry| &entry.object_id == object_id)
            .cloned()
    }

    fn insert(&mut self, entry: Arc<CachedDirectory>) {
        self.entries
            .retain(|cached| cached.object_id.as_str() != entry.object_id.as_str());
        self.entries.push_front(entry);
        self.entries.truncate(MAX_CACHED_OBJECTS);
    }
}

/// Registered, authority-bound Nasdaq Trader Symbol Directory extraction producer.
pub struct NasdaqSymbolDirectorySource {
    metadata: SourceMetadata,
    config: NasdaqSymbolDirectoryConfig,
    http: NasdaqHttpClient,
    cache: Mutex<DirectoryCache>,
    health: Mutex<HealthState>,
}

impl std::fmt::Debug for NasdaqSymbolDirectorySource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NasdaqSymbolDirectorySource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("dataset", self.config.dataset())
            .finish_non_exhaustive()
    }
}

impl NasdaqSymbolDirectorySource {
    /// Binds the fixed official endpoints to immutable registered source metadata.
    ///
    /// # Errors
    ///
    /// Fails closed unless metadata declares an extraction-only, delayed, official-quality,
    /// multi-venue reference source and explicitly allowlists both current files.
    pub fn try_new(
        metadata: SourceMetadata,
        config: NasdaqSymbolDirectoryConfig,
    ) -> Result<Self, NasdaqSymbolDirectorySourceError> {
        Self::validate_metadata(&metadata)?;
        let http = NasdaqHttpClient::try_new(&metadata)?;
        Ok(Self {
            metadata,
            config,
            http,
            cache: Mutex::new(DirectoryCache::default()),
            health: Mutex::new(HealthState::default()),
        })
    }

    /// Returns the exact dataset identity callers must use for discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        self.config.dataset()
    }

    /// Returns a bounded health snapshot for one independent official file.
    ///
    /// # Errors
    ///
    /// Fails closed if local health synchronization is poisoned.
    pub fn health(
        &self,
        kind: NasdaqDirectoryKind,
    ) -> Result<NasdaqDirectoryHealth, NasdaqSymbolDirectorySourceError> {
        self.health
            .lock()
            .map(|health| health.get(kind))
            .map_err(|_| NasdaqSymbolDirectorySourceError::HealthUnavailable)
    }

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.dataset() != self.config.dataset()
            || request.effective_at().is_some()
            || request.max_results() < 2
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        ensure_deadline_open(request.deadline())?;
        let mut objects = Vec::with_capacity(2);
        for kind in [
            NasdaqDirectoryKind::NasdaqListed,
            NasdaqDirectoryKind::OtherListed,
        ] {
            let entry = self
                .fetch_validated(&authority, kind, request.deadline(), &cancellation)
                .await?;
            objects.push(self.source_object(&request, &entry)?);
            self.cache
                .lock()
                .map_err(|_| SourceError::InvalidProtocolState)?
                .insert(entry);
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        authority.validate_current()?;
        DiscoveryBatch::try_new(&request, objects).map_err(Into::into)
    }

    async fn extract_impl(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExtractionBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        self.validate_extraction_request(&request)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        let (kind, object_digest) = parse_object_id(request.object().object_id())?;
        let cached = self
            .cache
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?
            .find(request.object().object_id());
        let entry = match cached {
            Some(entry) => entry,
            None => {
                let fetched = self
                    .fetch_validated(&authority, kind, request.deadline(), &cancellation)
                    .await?;
                if fetched.object_id.as_str() != request.object().object_id().as_str() {
                    return Err(SourceError::GenerationResynchronizationRequired.into());
                }
                self.cache
                    .lock()
                    .map_err(|_| SourceError::InvalidProtocolState)?
                    .insert(Arc::clone(&fetched));
                fetched
            }
        };
        if entry.retrieved.kind != kind
            || entry.parsed.kind != kind
            || entry.retrieved.sha256_hex.as_str() != object_digest
            || request.object().evidence() != &exact_evidence(&entry.retrieved.bytes)
            || !payload_matches_exact_evidence(&entry.retrieved.bytes, request.object().evidence())
            || request.object().expected_bytes() != u64::try_from(entry.retrieved.bytes.len()).ok()
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        let record_count = entry.parsed.rows.len();
        if record_count > request.max_records() as usize {
            return Err(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                }
                .into(),
            );
        }

        let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let source_last_modified_at = request
            .object()
            .published_at()
            .ok_or(SourceError::InvalidProtocolState)?;
        let first_observed_at = match request.object().availability() {
            AvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
            _ => return Err(SourceError::InvalidProtocolState.into()),
        };
        let source_evidence = request.object().evidence().clone();
        let mut batch = ExtractionBatchAccumulator::try_new(&request)?;
        for (index, row) in entry.parsed.rows.iter().enumerate() {
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled);
            }
            if index.is_multiple_of(256) {
                ensure_deadline_open(request.deadline())?;
                authority.validate_current()?;
            }
            let normalized = NasdaqListingRecord::try_new(
                row.row_number,
                entry.parsed.file_creation_time.clone(),
                source_last_modified_at,
                first_observed_at,
                source_evidence.clone(),
                row.fields.clone(),
            )
            .map_err(|_| SourceError::InvalidProtocolState)?;
            let payload = serde_json::to_vec(&normalized)
                .map(Bytes::from)
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let evidence = exact_evidence(&payload);
            let revision = SourceIdentifier::try_from(format!(
                "nasdaq-symbols:{}:row-{}:{object_digest}",
                kind.object_component(),
                row.row_number
            ))
            .map_err(|_| SourceError::InvalidProtocolState)?;
            let record = ExtractionRecord::try_new(
                &request,
                schema.clone(),
                evidence,
                source_last_modified_at,
                Some(source_last_modified_at),
                request.object().availability().clone(),
                revision,
                None,
                payload,
            )?;
            batch.push(record)?;
        }
        ensure_deadline_open(request.deadline())?;
        authority.validate_current()?;
        batch.finish().map_err(Into::into)
    }

    fn source_object(
        &self,
        request: &DiscoveryRequest,
        entry: &CachedDirectory,
    ) -> Result<SourceObject, ExtractionSourceError> {
        let evidence = exact_evidence(&entry.retrieved.bytes);
        let effective = EffectiveInterval::new(entry.retrieved.last_modified_at, None)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let expected_bytes = u64::try_from(entry.retrieved.bytes.len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        SourceObject::try_new_with_availability(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            request,
            entry.object_id.clone(),
            SourceIdentifier::try_from(MEDIA_TYPE)
                .map_err(|_| SourceError::InvalidProtocolState)?,
            evidence,
            effective,
            Some(entry.retrieved.last_modified_at),
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: entry.retrieved.received_at,
            },
            Some(expected_bytes),
        )
        .map_err(Into::into)
    }

    async fn fetch_validated(
        &self,
        authority: &ExtractionAuthority,
        kind: NasdaqDirectoryKind,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Arc<CachedDirectory>, ExtractionSourceError> {
        self.record_attempt(kind)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let result = async {
            let retrieved = self
                .http
                .fetch(&self.metadata, authority, kind, deadline, cancellation)
                .await?;
            let parsed =
                parse_directory(kind, &retrieved.bytes, cancellation).map_err(map_parse_error)?;
            let object_id = SourceIdentifier::try_from(format!(
                "nasdaq-symbols:{}:{}",
                kind.object_component(),
                retrieved.sha256_hex
            ))
            .map_err(|_| SourceError::InvalidProtocolState)?;
            Ok(Arc::new(CachedDirectory {
                object_id,
                retrieved,
                parsed,
            }))
        }
        .await;
        self.record_result(kind, &result)?;
        result
    }

    fn validate_extraction_request(
        &self,
        request: &ExtractionRequest,
    ) -> Result<(), ExtractionSourceError> {
        let object = request.object();
        let availability_matches = matches!(
            object.availability(),
            AvailabilityEvidence::LocalFirstObserved { observed_at }
                if *observed_at >= object.effective_interval().starts_at()
        );
        if object.source_id() != self.metadata.source_id()
            || object.metadata_revision() != self.metadata.revision()
            || object.dataset() != self.config.dataset()
            || object.media_type().as_str() != MEDIA_TYPE
            || object.effective_interval().ends_at().is_some()
            || object.published_at() != Some(object.effective_interval().starts_at())
            || !availability_matches
            || object.expected_bytes().is_none()
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        Ok(())
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

    fn validate_metadata(
        metadata: &SourceMetadata,
    ) -> Result<(), NasdaqSymbolDirectorySourceError> {
        let asset_classes = metadata.coverage().asset_classes();
        let required_assets = asset_classes.len() == 2
            && asset_classes.contains(&AssetClass::Equity)
            && asset_classes.contains(&AssetClass::Fund);
        let required_venues =
            NASDAQ_SYMBOL_DIRECTORY_VENUES
                .iter()
                .try_fold(true, |all_present, mic| {
                    let venue = VenueId::try_from(*mic)
                        .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
                    Ok::<_, NasdaqSymbolDirectorySourceError>(
                        all_present && metadata.coverage().topology().contains_venue(&venue),
                    )
                })?;
        if metadata.source_class() != SourceClass::Exchange
            || metadata.provider().as_str() != NASDAQ_SYMBOL_DIRECTORY_PROVIDER
            || metadata.coverage().domain() != CoverageDomain::Instruments
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || metadata.authorization().mode() != AuthorizationMode::PublicInterface
            || !required_assets
            || !metadata.coverage().topology().is_consolidated()
            || metadata.coverage().topology().venues().len() != NASDAQ_SYMBOL_DIRECTORY_VENUES.len()
            || !required_venues
            || !matches!(metadata.coverage().delay(), CoverageDelay::Delayed(_))
            || metadata.coverage().delivery() != DeliveryEvidence::Indirect
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || metadata.capabilities().source_timestamps()
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(NasdaqSymbolDirectorySourceError::InvalidMetadata);
        }
        metadata
            .network_policy()
            .authorize(NASDAQ_LISTED_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        metadata
            .network_policy()
            .authorize(OTHER_LISTED_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        Ok(())
    }

    fn record_attempt(
        &self,
        kind: NasdaqDirectoryKind,
    ) -> Result<(), NasdaqSymbolDirectorySourceError> {
        let now = system_timestamp()?;
        let mut health = self
            .health
            .lock()
            .map_err(|_| NasdaqSymbolDirectorySourceError::HealthUnavailable)?;
        health.get_mut(kind).last_attempt_at = Some(now);
        Ok(())
    }

    fn record_result(
        &self,
        kind: NasdaqDirectoryKind,
        result: &Result<Arc<CachedDirectory>, ExtractionSourceError>,
    ) -> Result<(), ExtractionSourceError> {
        let mut health = self
            .health
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let value = health.get_mut(kind);
        match result {
            Ok(entry) => {
                value.last_success_at = Some(entry.retrieved.received_at);
                value.last_payload_digest = Some(Sha256::digest(&entry.retrieved.bytes).into());
                value.consecutive_failures = 0;
            }
            Err(_) => {
                value.consecutive_failures = value.consecutive_failures.saturating_add(1);
            }
        }
        Ok(())
    }
}

impl SourceMetadataProvider for NasdaqSymbolDirectorySource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for NasdaqSymbolDirectorySource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(self.discover_impl(authority, request, cancellation))
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        Box::pin(self.extract_impl(authority, request, cancellation))
    }
}

/// Adapter configuration, metadata, local state, or clock failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NasdaqSymbolDirectorySourceError {
    /// The code-owned dataset contract could not be constructed.
    #[error("invalid Nasdaq Symbol Directory configuration")]
    InvalidConfiguration,
    /// Registered metadata is incompatible with this exact adapter contract.
    #[error("Nasdaq Symbol Directory metadata is incompatible with the adapter")]
    InvalidMetadata,
    /// Local bounded health state is unavailable.
    #[error("Nasdaq Symbol Directory health is unavailable")]
    HealthUnavailable,
    /// The process clock could not produce a bounded Unix-nanosecond observation.
    #[error("Nasdaq Symbol Directory observation clock is unavailable")]
    Clock,
}

fn parse_object_id(
    object_id: &SourceIdentifier,
) -> Result<(NasdaqDirectoryKind, &str), ExtractionSourceError> {
    let mut fields = object_id.as_str().split(':');
    if fields.next() != Some("nasdaq-symbols") {
        return Err(SourceError::InvalidProtocolState.into());
    }
    let kind = match fields.next() {
        Some("nasdaq-listed") => NasdaqDirectoryKind::NasdaqListed,
        Some("other-listed") => NasdaqDirectoryKind::OtherListed,
        _ => return Err(SourceError::InvalidProtocolState.into()),
    };
    let digest = fields.next().ok_or(SourceError::InvalidProtocolState)?;
    if fields.next().is_some()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SourceError::InvalidProtocolState.into());
    }
    Ok((kind, digest))
}

fn exact_evidence(payload: &[u8]) -> ExactPayloadEvidence {
    let digest = Sha256::digest(payload);
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.into(),
    ))
}

fn map_parse_error(error: NasdaqParseError) -> ExtractionSourceError {
    match error {
        NasdaqParseError::Cancelled => ExtractionSourceError::Cancelled,
        NasdaqParseError::BodyTooLarge { max } => {
            ExtractionSourceError::Source(SourceError::FrameTooLarge { max })
        }
        _ => ExtractionSourceError::Source(SourceError::InvalidProtocolState),
    }
}
