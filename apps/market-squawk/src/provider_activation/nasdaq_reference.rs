//! Session-only official U.S. listing-reference discovery for unified Markets.
//!
//! Nasdaq Trader's current directory is useful symbology, not a quote, book, trading-status, or
//! execution source. The current terms evidence does not admit silent durable incorporation, so
//! this service deliberately uses the production bounded-extraction registry and keeps normalized
//! rows only in process memory. A restart reacquires the two official files.

use std::cmp::Ordering;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use market_squawk_adapter_nasdaq_symbols::{
    MAX_DIRECTORY_RECORDS, NASDAQ_LISTED_URL, NASDAQ_SYMBOL_DIRECTORY_PROVIDER,
    NASDAQ_SYMBOL_DIRECTORY_VENUES, NasdaqListingRecord, NasdaqModelError,
    NasdaqSymbolDirectoryConfig, NasdaqSymbolDirectorySource, NasdaqSymbolDirectorySourceError,
    OTHER_LISTED_URL,
};
use market_squawk_domain::{
    AssetClass, AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, ProviderInstrumentId, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    CURRENT_RESEARCH_RECORD_SCHEMA, CoverageTopology, DiscoveryRequest, EndpointPolicy,
    ExtractionAuthority, ExtractionError, ExtractionRequest, ExtractionSource,
    ExtractionSourceError, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, NetworkAccessPolicy, ProviderBudgetPolicy,
    ProviderRateAuthority, RegistryError, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceMetadataProvider, SourceProtocolProfile,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::application::{
    MarketReferenceMatchKind, MarketReferenceRecord, MarketReferenceSearchAuthority,
    MarketReferenceSearchPage,
};

const DIRECTORY_OBJECT_COUNT: u16 = 2;
const MAXIMUM_SEARCH_ROWS: usize = 100;
pub(crate) const MAXIMUM_SELECTED_LISTING_IDENTITIES: usize = 250;
const DIRECTORY_DELAY_NANOS: u64 = 60 * 1_000_000_000;
const DAY_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;
const MINUTE_NANOS: u64 = 60 * 1_000_000_000;
const DEFAULT_OVERVIEW_SYMBOLS: [&str; 8] =
    ["SPY", "QQQ", "DIA", "IWM", "VTI", "AAPL", "MSFT", "NVDA"];

/// Exact current-directory key accepted by bounded identity enrichment.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct NasdaqListingKey {
    symbol: ProviderInstrumentId,
    mic: VenueId,
}

impl NasdaqListingKey {
    pub(crate) const fn new(symbol: ProviderInstrumentId, mic: VenueId) -> Self {
        Self { symbol, mic }
    }

    pub(crate) const fn symbol(&self) -> &ProviderInstrumentId {
        &self.symbol
    }

    pub(crate) const fn mic(&self) -> &VenueId {
        &self.mic
    }
}

/// Session-only current listing identity with the exact directory evidence that established it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NasdaqCurrentListing {
    key: NasdaqListingKey,
    asset_class: AssetClass,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    source_payload_evidence: ExactPayloadEvidence,
    source_timestamp: Timestamp,
    observed_at: Timestamp,
}

impl NasdaqCurrentListing {
    pub(crate) const fn key(&self) -> &NasdaqListingKey {
        &self.key
    }

    pub(crate) const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub(crate) const fn source_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_payload_evidence
    }

    pub(crate) const fn source_timestamp(&self) -> Timestamp {
        self.source_timestamp
    }

    pub(crate) const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}

/// One process-local source registry and normalized official-directory snapshot.
pub(crate) struct NasdaqReferenceUniverseService {
    source: NasdaqSymbolDirectorySource,
    extraction: ExtractionAuthority,
    registry: StdMutex<Option<AuthoritativeSourceRegistry>>,
    snapshot: RwLock<Option<Arc<ReferenceUniverseSnapshot>>>,
    refresh: Mutex<()>,
    lifecycle: CancellationToken,
}

impl std::fmt::Debug for NasdaqReferenceUniverseService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NasdaqReferenceUniverseService")
            .field("source_id", self.source.metadata().source_id())
            .field("dataset", self.source.dataset())
            .finish_non_exhaustive()
    }
}

impl NasdaqReferenceUniverseService {
    /// Creates one bounded non-durable extraction authority on the product-wide provider budget.
    pub(crate) fn try_new(
        provider_rate: ProviderRateAuthority,
    ) -> Result<Self, NasdaqReferenceUniverseError> {
        let metadata = source_metadata()?;
        let source = NasdaqSymbolDirectorySource::try_new(
            metadata.clone(),
            NasdaqSymbolDirectoryConfig::try_new()?,
        )?;
        let resolver = Arc::new(provider_rate.clone());
        let mut registry = AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
            resolver,
            provider_rate,
        )?;
        let registered = registry.register(metadata, system_timestamp()?)?;
        let extraction = registry.extraction_authority(&registered, &source)?;
        Ok(Self {
            source,
            extraction,
            registry: StdMutex::new(Some(registry)),
            snapshot: RwLock::new(None),
            refresh: Mutex::new(()),
            lifecycle: CancellationToken::new(),
        })
    }

    /// Projects the immutable source declaration used to qualify identity-resolution evidence.
    pub(super) fn reference_identity_metadata(&self) -> &SourceMetadata {
        self.source.metadata()
    }

    async fn snapshot(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<ReferenceUniverseSnapshot>, NasdaqReferenceUniverseError> {
        ensure_open(deadline, cancellation, &self.lifecycle)?;
        if let Some(snapshot) = self.snapshot.read().await.as_ref().cloned() {
            return Ok(snapshot);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(NasdaqReferenceUniverseError::DeadlineExceeded);
        }
        let refresh = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(NasdaqReferenceUniverseError::Cancelled),
            () = self.lifecycle.cancelled() => return Err(NasdaqReferenceUniverseError::ShuttingDown),
            result = tokio::time::timeout(remaining, self.refresh.lock()) => {
                result.map_err(|_| NasdaqReferenceUniverseError::DeadlineExceeded)?
            }
        };
        if let Some(snapshot) = self.snapshot.read().await.as_ref().cloned() {
            drop(refresh);
            return Ok(snapshot);
        }

        let operation = self.lifecycle.child_token();
        let loaded = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                operation.cancel();
                Err(NasdaqReferenceUniverseError::Cancelled)
            }
            () = self.lifecycle.cancelled() => {
                operation.cancel();
                Err(NasdaqReferenceUniverseError::ShuttingDown)
            }
            result = self.load_snapshot(deadline, operation.clone()) => result,
        }?;
        ensure_open(deadline, cancellation, &self.lifecycle)?;
        let loaded = Arc::new(loaded);
        *self.snapshot.write().await = Some(Arc::clone(&loaded));
        drop(refresh);
        Ok(loaded)
    }

    /// Resolves a sorted, deduplicated, caller-selected subset against one current snapshot.
    ///
    /// Missing keys are deliberately omitted so the coordinator can retain an explicit
    /// per-listing `not current` result. This method never enumerates the full snapshot to a
    /// downstream provider.
    pub(crate) async fn selected_current_listings(
        &self,
        keys: &[NasdaqListingKey],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<NasdaqCurrentListing>, NasdaqReferenceUniverseError> {
        if keys.is_empty()
            || keys.len() > MAXIMUM_SELECTED_LISTING_IDENTITIES
            || keys.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(NasdaqReferenceUniverseError::InvalidSelection);
        }
        let snapshot = self.snapshot(deadline, cancellation).await?;
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(keys.len())
            .map_err(|_| NasdaqReferenceUniverseError::Capacity)?;
        for key in keys {
            ensure_open(deadline, cancellation, &self.lifecycle)?;
            if let Ok(index) = snapshot.records.binary_search_by(|record| {
                record
                    .listing
                    .key
                    .symbol
                    .cmp(&key.symbol)
                    .then_with(|| record.listing.key.mic.cmp(&key.mic))
            }) {
                selected.push(snapshot.records[index].listing.clone());
            }
        }
        Ok(selected)
    }

    /// Returns the code-owned default overview in its declared display order.
    ///
    /// Missing current-directory rows are deliberately omitted. Each retained value carries the
    /// exact symbol, MIC, asset class, and source evidence from the current snapshot; this method
    /// does not infer an identity. Callers that require key ordering must sort and deduplicate the
    /// returned [`NasdaqListingKey`] values before passing them to a sorted-key API.
    pub(crate) async fn default_overview_current_listings(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<NasdaqCurrentListing>, NasdaqReferenceUniverseError> {
        let snapshot = self.snapshot(deadline, cancellation).await?;
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(DEFAULT_OVERVIEW_SYMBOLS.len())
            .map_err(|_| NasdaqReferenceUniverseError::Capacity)?;
        for wanted in DEFAULT_OVERVIEW_SYMBOLS {
            ensure_open(deadline, cancellation, &self.lifecycle)?;
            if let Some(record) = snapshot
                .records
                .iter()
                .find(|record| record.symbol.eq_ignore_ascii_case(wanted))
            {
                selected.push(record.listing.clone());
            }
        }
        Ok(selected)
    }

    async fn load_snapshot(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ReferenceUniverseSnapshot, NasdaqReferenceUniverseError> {
        let provider_deadline = wall_deadline(deadline)?;
        let maximum_objects = NonZeroU16::new(DIRECTORY_OBJECT_COUNT)
            .ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?;
        let discovery = self
            .source
            .discover(
                self.extraction.clone(),
                DiscoveryRequest::try_new(
                    self.source.dataset().clone(),
                    None,
                    maximum_objects,
                    provider_deadline,
                )?,
                cancellation.clone(),
            )
            .await?;
        if discovery.objects().len() != usize::from(DIRECTORY_OBJECT_COUNT) {
            return Err(NasdaqReferenceUniverseError::IncompleteDirectory);
        }

        let maximum_records = u32::try_from(MAX_DIRECTORY_RECORDS)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?;
        let maximum_bytes = NonZeroU64::new(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES)
            .ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?;
        let maximum_total = MAX_DIRECTORY_RECORDS
            .checked_mul(usize::from(DIRECTORY_OBJECT_COUNT))
            .ok_or(NasdaqReferenceUniverseError::Capacity)?;
        let mut records = Vec::new();
        records
            .try_reserve(maximum_total)
            .map_err(|_| NasdaqReferenceUniverseError::Capacity)?;

        for object in discovery.objects() {
            ensure_operation(deadline, &cancellation)?;
            if object.source_id() != self.source.metadata().source_id()
                || object.metadata_revision() != self.source.metadata().revision()
                || object.dataset() != self.source.dataset()
            {
                return Err(NasdaqReferenceUniverseError::SourceBinding);
            }
            let batch = self
                .source
                .extract(
                    self.extraction.clone(),
                    ExtractionRequest::try_new(
                        object.clone(),
                        maximum_records,
                        maximum_bytes,
                        provider_deadline,
                    )?,
                    cancellation.clone(),
                )
                .await?;
            for (index, extracted) in batch.records().iter().enumerate() {
                if index.is_multiple_of(256) {
                    ensure_operation(deadline, &cancellation)?;
                }
                if extracted.schema().as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
                    || extracted.object_id() != object.object_id()
                    || extracted.object_evidence() != object.evidence()
                {
                    return Err(NasdaqReferenceUniverseError::SourceBinding);
                }
                let record = NasdaqListingRecord::from_json(extracted.payload())?;
                if record.source_payload_evidence() != object.evidence()
                    || record.source_last_modified_at()
                        != object
                            .published_at()
                            .ok_or(NasdaqReferenceUniverseError::SourceBinding)?
                {
                    return Err(NasdaqReferenceUniverseError::SourceBinding);
                }
                if record.provider_fields().is_test_issue() {
                    continue;
                }
                records.push(ReferenceUniverseRecord::try_from_record(
                    self.source.metadata(),
                    record,
                )?);
            }
        }
        if records.is_empty() || records.len() > maximum_total {
            return Err(NasdaqReferenceUniverseError::IncompleteDirectory);
        }
        records.sort_unstable_by(compare_reference_records);
        if records
            .windows(2)
            .any(|pair| pair[0].symbol == pair[1].symbol && pair[0].venue_id == pair[1].venue_id)
        {
            return Err(NasdaqReferenceUniverseError::DuplicateIdentity);
        }
        Ok(ReferenceUniverseSnapshot {
            records: records.into_boxed_slice(),
        })
    }

    fn search_snapshot(
        snapshot: &ReferenceUniverseSnapshot,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketReferenceSearchPage, ServiceError> {
        if maximum_rows == 0 || maximum_rows > MAXIMUM_SEARCH_ROWS {
            return Err(ServiceError::InvalidRequest);
        }
        let query = query.trim();
        if query.len() > 256 || query.chars().any(char::is_control) {
            return Err(ServiceError::InvalidRequest);
        }
        if query.is_empty() {
            return default_overview(snapshot, maximum_rows, deadline, cancellation);
        }

        let mut buckets: [Vec<(usize, MarketReferenceMatchKind)>; 5] =
            std::array::from_fn(|_| Vec::new());
        let retained = maximum_rows.saturating_add(1);
        let mut available = 0_usize;
        for (index, record) in snapshot.records.iter().enumerate() {
            if index.is_multiple_of(256) {
                ensure_search_open(deadline, cancellation)?;
            }
            let Some((rank, kind)) = match_record(record, query) else {
                continue;
            };
            available = available
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            if buckets[rank].len() < retained {
                buckets[rank].push((index, kind));
            }
        }
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(maximum_rows.min(available))
            .map_err(|_| ServiceError::ResourceExhausted)?;
        'ranks: for bucket in buckets {
            for (index, kind) in bucket {
                if selected.len() == maximum_rows {
                    break 'ranks;
                }
                selected.push(
                    snapshot.records[index]
                        .presentation
                        .clone()
                        .with_match_kind(kind),
                );
            }
        }
        ensure_search_open(deadline, cancellation)?;
        MarketReferenceSearchPage::try_new(selected, available, available > maximum_rows)
    }
}

#[async_trait]
impl MarketReferenceSearchAuthority for NasdaqReferenceUniverseService {
    async fn search(
        &self,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketReferenceSearchPage, ServiceError> {
        let snapshot = self
            .snapshot(deadline, cancellation)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "official U.S. listing reference is unavailable");
                map_service_error(&error)
            })?;
        Self::search_snapshot(&snapshot, query, maximum_rows, deadline, cancellation)
    }

    fn begin_shutdown(&self) {
        self.lifecycle.cancel();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.lifecycle.cancel();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let _refresh = tokio::time::timeout(remaining, self.refresh.lock())
            .await
            .map_err(|_| ServiceError::DeadlineExceeded)?;
        let registry = self
            .registry
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
            .take();
        if let Some(registry) = registry {
            registry.shutdown().map_err(|_| ServiceError::Unavailable)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ReferenceUniverseSnapshot {
    records: Box<[ReferenceUniverseRecord]>,
}

#[derive(Debug)]
struct ReferenceUniverseRecord {
    listing: NasdaqCurrentListing,
    presentation: MarketReferenceRecord,
    symbol: String,
    security_name: String,
    venue_id: VenueId,
    cqs_symbol: Option<String>,
    nasdaq_symbol: Option<String>,
}

impl ReferenceUniverseRecord {
    fn try_from_record(
        metadata: &SourceMetadata,
        record: NasdaqListingRecord,
    ) -> Result<Self, NasdaqReferenceUniverseError> {
        let fields = record.provider_fields();
        let symbol = record.primary_symbol().as_str().to_owned();
        let venue_id = record.listing_venue().clone();
        let reference_id = SourceIdentifier::try_from(format!(
            "nasdaq-reference:{}:{symbol}",
            venue_id.as_str().to_ascii_lowercase(),
        ))
        .map_err(|_| NasdaqReferenceUniverseError::SourceBinding)?;
        let security_name = fields.display_name().to_owned();
        let asset_class = if fields.is_etf() {
            AssetClass::Fund
        } else {
            AssetClass::Equity
        };
        let presentation = MarketReferenceRecord::try_new(
            reference_id,
            symbol.clone(),
            security_name.clone(),
            venue_id.clone(),
            asset_class,
            fields.is_etf(),
            fields.round_lot_size(),
            record.quality(),
            record.source_last_modified_at(),
            record.first_observed_at(),
            metadata.source_id().clone(),
            metadata.provider().clone(),
            record.source_payload_evidence().content_digest(),
            MarketReferenceMatchKind::DefaultOverview,
        )
        .map_err(|_| NasdaqReferenceUniverseError::SourceBinding)?;
        let listing = NasdaqCurrentListing {
            key: NasdaqListingKey::new(
                ProviderInstrumentId::try_from(symbol.as_str())
                    .map_err(|_| NasdaqReferenceUniverseError::SourceBinding)?,
                venue_id.clone(),
            ),
            asset_class,
            source_id: metadata.source_id().clone(),
            metadata_revision: metadata.revision().clone(),
            source_payload_evidence: record.source_payload_evidence().clone(),
            source_timestamp: record.source_last_modified_at(),
            observed_at: record.first_observed_at(),
        };
        Ok(Self {
            listing,
            presentation,
            symbol,
            security_name,
            venue_id,
            cqs_symbol: fields.cqs_symbol().map(str::to_owned),
            nasdaq_symbol: fields.nasdaq_symbol().map(str::to_owned),
        })
    }
}

fn default_overview(
    snapshot: &ReferenceUniverseSnapshot,
    maximum_rows: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<MarketReferenceSearchPage, ServiceError> {
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(maximum_rows.min(DEFAULT_OVERVIEW_SYMBOLS.len()))
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for wanted in DEFAULT_OVERVIEW_SYMBOLS {
        ensure_search_open(deadline, cancellation)?;
        if let Some(record) = snapshot
            .records
            .iter()
            .find(|record| record.symbol.eq_ignore_ascii_case(wanted))
        {
            if selected.len() == maximum_rows {
                break;
            }
            selected.push(
                record
                    .presentation
                    .clone()
                    .with_match_kind(MarketReferenceMatchKind::DefaultOverview),
            );
        }
    }
    let available = selected.len();
    MarketReferenceSearchPage::try_new(selected, available, false)
}

fn match_record(
    record: &ReferenceUniverseRecord,
    query: &str,
) -> Option<(usize, MarketReferenceMatchKind)> {
    if record.symbol.eq_ignore_ascii_case(query)
        || record
            .cqs_symbol
            .as_deref()
            .is_some_and(|symbol| symbol.eq_ignore_ascii_case(query))
        || record
            .nasdaq_symbol
            .as_deref()
            .is_some_and(|symbol| symbol.eq_ignore_ascii_case(query))
    {
        return Some((0, MarketReferenceMatchKind::ExactSymbol));
    }
    if starts_with_case_insensitive(&record.symbol, query) {
        return Some((1, MarketReferenceMatchKind::SymbolPrefix));
    }
    if starts_with_case_insensitive(&record.security_name, query) {
        return Some((2, MarketReferenceMatchKind::SecurityNamePrefix));
    }
    if contains_case_insensitive(&record.symbol, query)
        || record
            .cqs_symbol
            .as_deref()
            .is_some_and(|symbol| contains_case_insensitive(symbol, query))
        || record
            .nasdaq_symbol
            .as_deref()
            .is_some_and(|symbol| contains_case_insensitive(symbol, query))
    {
        return Some((3, MarketReferenceMatchKind::SymbolContains));
    }
    contains_case_insensitive(&record.security_name, query)
        .then_some((4, MarketReferenceMatchKind::SecurityNameContains))
}

fn starts_with_case_insensitive(value: &str, query: &str) -> bool {
    if query.is_ascii() {
        value
            .as_bytes()
            .get(..query.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(query.as_bytes()))
    } else {
        value.starts_with(query)
    }
}

fn contains_case_insensitive(value: &str, query: &str) -> bool {
    if query.is_ascii() {
        value
            .as_bytes()
            .windows(query.len())
            .any(|window| window.eq_ignore_ascii_case(query.as_bytes()))
    } else {
        value.contains(query)
    }
}

fn compare_reference_records(
    left: &ReferenceUniverseRecord,
    right: &ReferenceUniverseRecord,
) -> Ordering {
    left.symbol
        .as_str()
        .cmp(right.symbol.as_str())
        .then_with(|| left.venue_id.as_str().cmp(right.venue_id.as_str()))
}

fn source_metadata() -> Result<SourceMetadata, NasdaqReferenceUniverseError> {
    let evidence = contract_evidence();
    let provider = SourceIdentifier::try_from(NASDAQ_SYMBOL_DIRECTORY_PROVIDER)
        .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?;
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)
        .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(
            SourceIdentifier::try_from("official-public-interface")
                .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?,
        ),
        evidence.clone(),
        effective,
    );
    let venues = NASDAQ_SYMBOL_DIRECTORY_VENUES
        .iter()
        .map(|venue| {
            VenueId::try_from(*venue)
                .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let endpoint = EndpointPolicy::try_new([NASDAQ_LISTED_URL, OTHER_LISTED_URL])
        .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider.clone()),
        NonZeroU32::new(8).ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?,
        NonZeroU64::new(MINUTE_NANOS).ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?,
        NonZeroU16::new(1).ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000)
                .ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?,
            NonZeroU64::new(MINUTE_NANOS)
                .ok_or(NasdaqReferenceUniverseError::InvalidConfiguration)?,
            2_000,
        )
        .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?,
    )
    .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?;
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("nasdaq-trader-symbol-directory-reference")
            .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(
                SourceIdentifier::try_from("nasdaq-symbol-directory-session-v1")
                    .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?,
            ),
            evidence.clone(),
        ),
        SourceClass::Exchange,
        provider,
        authorization,
        SourceCoverage::try_instrument(
            evidence,
            effective,
            vec![AssetClass::Equity, AssetClass::Fund],
            CoverageTopology::consolidated(venues)
                .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?,
            InstrumentCoverage::partial(),
            None,
            CoverageDelay::Delayed(DIRECTORY_DELAY_NANOS),
            DeliveryEvidence::Indirect,
        )
        .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Allowlisted(endpoint),
        FreshnessPolicy::try_new(DAY_NANOS, DAY_NANOS, DAY_NANOS, DAY_NANOS, MINUTE_NANOS)
            .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::Historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)
}

fn contract_evidence() -> ExactPayloadEvidence {
    let digest: [u8; 32] = Sha256::digest(
        b"market-squawk/nasdaq-symbol-directory/session-reference/v1\0nasdaqlisted.txt\0otherlisted.txt\0reference-only\0non-persistent",
    )
    .into();
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(DigestAlgorithm::Sha256, digest))
}

fn wall_deadline(deadline: Instant) -> Result<Timestamp, NasdaqReferenceUniverseError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(NasdaqReferenceUniverseError::DeadlineExceeded);
    }
    let nanos = i64::try_from(remaining.as_nanos())
        .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)?;
    system_timestamp()?
        .checked_add_nanos(nanos)
        .map_err(|_| NasdaqReferenceUniverseError::InvalidConfiguration)
}

fn system_timestamp() -> Result<Timestamp, NasdaqReferenceUniverseError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NasdaqReferenceUniverseError::Clock)?;
    let nanos = u128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(NasdaqReferenceUniverseError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn ensure_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), NasdaqReferenceUniverseError> {
    if cancellation.is_cancelled() {
        Err(NasdaqReferenceUniverseError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(NasdaqReferenceUniverseError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn ensure_open(
    deadline: Instant,
    cancellation: &CancellationToken,
    lifecycle: &CancellationToken,
) -> Result<(), NasdaqReferenceUniverseError> {
    if lifecycle.is_cancelled() {
        Err(NasdaqReferenceUniverseError::ShuttingDown)
    } else {
        ensure_operation(deadline, cancellation)
    }
}

fn ensure_search_open(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_service_error(error: &NasdaqReferenceUniverseError) -> ServiceError {
    match error {
        NasdaqReferenceUniverseError::Cancelled => ServiceError::Cancelled,
        NasdaqReferenceUniverseError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        NasdaqReferenceUniverseError::Capacity => ServiceError::ResourceExhausted,
        NasdaqReferenceUniverseError::ShuttingDown => ServiceError::Unavailable,
        _ => ServiceError::Unavailable,
    }
}

/// Bounded session-only listing-reference construction or retrieval failure.
#[derive(Debug, Error)]
pub(crate) enum NasdaqReferenceUniverseError {
    #[error("Nasdaq reference configuration is invalid")]
    InvalidConfiguration,
    #[error("Nasdaq selected listing keys are empty, unbounded, duplicated, or unordered")]
    InvalidSelection,
    #[error("Nasdaq reference source binding is invalid")]
    SourceBinding,
    #[error("Nasdaq reference files did not form one complete directory")]
    IncompleteDirectory,
    #[error("Nasdaq reference directory contains a duplicate listing identity")]
    DuplicateIdentity,
    #[error("Nasdaq reference memory capacity is unavailable")]
    Capacity,
    #[error("Nasdaq reference wall clock is unavailable")]
    Clock,
    #[error("Nasdaq reference operation was cancelled")]
    Cancelled,
    #[error("Nasdaq reference operation deadline elapsed")]
    DeadlineExceeded,
    #[error("Nasdaq reference service is shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Configuration(#[from] NasdaqSymbolDirectorySourceError),
    #[error(transparent)]
    Request(#[from] ExtractionError),
    #[error(transparent)]
    Source(#[from] ExtractionSourceError),
    #[error(transparent)]
    Record(#[from] NasdaqModelError),
}
