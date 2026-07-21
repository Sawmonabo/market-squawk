use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    SourceIdentifier,
};
use market_squawk_sources::{
    AuthorizationMode, DiscoveryBatch, DiscoveryRequest, ExtractionRequest, ExtractionSourceError,
    HistoricalCapability, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, RegisteredSource, SourceClass,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject, SourceProtocolProfile,
    payload_matches_exact_evidence,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::client::{BlsHttpClient, RetrievedBlsPage, ensure_deadline_open};
use crate::observations::canonical_payloads;
use crate::{
    BlsAccessTier, BlsAuthorization, BlsRequestLimits, BlsRequestPlan, BlsSeriesMetadata,
    BlsSourceError,
};

const NANOS_PER_DAY: u64 = 86_400_000_000_000;
const REQUESTS_PER_TEN_SECONDS: u16 = 50;
const CACHE_ENTRY_OVERHEAD_BYTES: usize = 512;

/// Exact, deterministic BLS source configuration bound into its dataset identity.
#[derive(Clone, Debug)]
pub struct BlsSourceConfig {
    authorization: BlsAuthorization,
    plan: BlsRequestPlan,
    series_metadata: BTreeMap<String, BlsSeriesMetadata>,
    dataset: SourceIdentifier,
}

#[derive(Debug)]
struct PageCache {
    limit: u64,
    retained_bytes: u64,
    pages: BTreeMap<String, CachedBlsPage>,
}

#[derive(Clone, Debug)]
struct CachedBlsPage {
    bytes: Arc<[u8]>,
    received_at: market_squawk_domain::Timestamp,
    sha256_hex: String,
}

impl PageCache {
    fn new() -> Self {
        Self::with_limit(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES)
    }

    fn with_limit(limit: u64) -> Self {
        Self {
            limit,
            retained_bytes: 0,
            pages: BTreeMap::new(),
        }
    }

    fn insert(
        &mut self,
        object_id: &SourceIdentifier,
        page: &RetrievedBlsPage,
    ) -> Result<bool, SourceError> {
        if self.pages.contains_key(object_id.as_str()) {
            return Ok(true);
        }
        let bytes = Self::retained_charge(object_id, page)?;
        let next = self
            .retained_bytes
            .checked_add(bytes)
            .ok_or(SourceError::FrameTooLarge {
                max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
            })?;
        if next > self.limit {
            return Ok(false);
        }
        self.retained_bytes = next;
        self.pages.insert(
            object_id.as_str().to_owned(),
            CachedBlsPage {
                bytes: Arc::from(page.bytes.as_ref()),
                received_at: page.received_at,
                sha256_hex: page.sha256_hex.clone(),
            },
        );
        Ok(true)
    }

    fn retained_charge(
        object_id: &SourceIdentifier,
        page: &RetrievedBlsPage,
    ) -> Result<u64, SourceError> {
        let charge = page
            .bytes
            .len()
            .checked_add(object_id.as_str().len())
            .and_then(|bytes| bytes.checked_add(page.sha256_hex.len()))
            .and_then(|bytes| bytes.checked_add(size_of::<CachedBlsPage>()))
            .and_then(|bytes| bytes.checked_add(CACHE_ENTRY_OVERHEAD_BYTES))
            .ok_or(SourceError::FrameTooLarge {
                max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
            })?;
        u64::try_from(charge).map_err(|_| SourceError::FrameTooLarge {
            max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
        })
    }
}

impl std::fmt::Debug for RetrievedBlsPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetrievedBlsPage")
            .field("bytes", &self.bytes.len())
            .field("received_at", &self.received_at)
            .field("sha256_hex", &self.sha256_hex)
            .finish_non_exhaustive()
    }
}

/// A normalized BLS response page retaining local availability and exact source evidence.
#[derive(Clone, Debug)]
pub struct BlsNormalizedPage {
    received_at: market_squawk_domain::Timestamp,
    source_payload_sha256: String,
    payloads: Vec<Bytes>,
}

impl BlsNormalizedPage {
    /// Returns the process-local first-observation time for this exact source response.
    pub const fn received_at(&self) -> market_squawk_domain::Timestamp {
        self.received_at
    }

    /// Returns the lowercase SHA-256 identity of the exact provider response.
    pub fn source_payload_sha256(&self) -> &str {
        &self.source_payload_sha256
    }

    /// Returns deterministic normalized record payloads without fabricated temporal precision.
    pub fn payloads(&self) -> &[Bytes] {
        &self.payloads
    }
}

/// Registered, allowlisted, budget-coordinated BLS extraction producer.
pub struct BlsSource {
    metadata: SourceMetadata,
    budget: market_squawk_sources::SharedProviderBudget,
    config: BlsSourceConfig,
    http: BlsHttpClient,
    cache: Mutex<PageCache>,
}

impl std::fmt::Debug for BlsSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsSource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("dataset", self.config.dataset())
            .field("authorization", self.config.authorization())
            .finish_non_exhaustive()
    }
}

impl BlsSource {
    /// Binds a provider configuration to registry-issued budget authority and exact metadata.
    ///
    /// # Errors
    ///
    /// Fails closed unless metadata declares an official-agency, macroeconomic, historical,
    /// extraction-only source with an allowlisted endpoint and the matching authorization mode.
    pub fn try_new(
        metadata: SourceMetadata,
        registered: &RegisteredSource,
        config: BlsSourceConfig,
    ) -> Result<Self, BlsSourceError> {
        let expected_mode = match config.authorization() {
            BlsAuthorization::PublicV1 => AuthorizationMode::PublicInterface,
            BlsAuthorization::RegisteredV2(_) => AuthorizationMode::UserAuthorized,
        };
        let budget_policy = metadata
            .budget_policy()
            .ok_or(BlsSourceError::InvalidMetadata)?;
        if registered.source_id() != metadata.source_id()
            || registered.revision() != metadata.revision()
            || metadata.source_class() != SourceClass::OfficialAgency
            || metadata.coverage().domain() != market_squawk_sources::CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || metadata.authorization().mode() != expected_mode
            || budget_policy.requests_per_window()
                > u32::from(
                    config
                        .plan()
                        .limits()
                        .daily_queries()
                        .min(REQUESTS_PER_TEN_SECONDS),
                )
            || budget_policy.window_nanos() < NANOS_PER_DAY
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(BlsSourceError::InvalidMetadata);
        }
        let budget = registered
            .budget()
            .cloned()
            .ok_or(BlsSourceError::InvalidMetadata)?;
        let http = BlsHttpClient::try_new(&metadata, config.authorization().clone())?;
        Ok(Self {
            metadata,
            budget,
            config,
            http,
            cache: Mutex::new(PageCache::new()),
        })
    }

    /// Returns the exact request-plan-bound dataset callers use for discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        self.config.dataset()
    }

    /// Fetches and discovers exact response objects under bounded network and cache limits.
    pub async fn discover_pages(
        &self,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        if request.dataset() != self.config.dataset()
            || request.effective_at().is_some()
            || self.config.plan().chunks().len() > usize::from(request.max_results())
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let mut discovered = Vec::with_capacity(self.config.plan().chunks().len());
        for (index, chunk) in self.config.plan().chunks().iter().enumerate() {
            let page = self
                .http
                .fetch(
                    &self.metadata,
                    &self.budget,
                    chunk,
                    request.deadline(),
                    &cancellation,
                )
                .await?;
            if page.response.is_partial() {
                return Err(SourceError::GenerationResynchronizationRequired.into());
            }
            let evidence = exact_evidence(&page.bytes);
            let object_id = SourceIdentifier::try_from(format!("bls:{index}:{}", page.sha256_hex))
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let effective = EffectiveInterval::new(page.received_at, None)
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let expected_bytes =
                u64::try_from(page.bytes.len()).map_err(|_| SourceError::InvalidProtocolState)?;
            discovered.push(SourceObject::try_new(
                self.metadata.source_id().clone(),
                self.metadata.revision().clone(),
                &request,
                object_id.clone(),
                SourceIdentifier::try_from("application/json")
                    .map_err(|_| SourceError::InvalidProtocolState)?,
                evidence,
                effective,
                None,
                Some(expected_bytes),
            )?);
            self.cache
                .lock()
                .map_err(|_| SourceError::InvalidProtocolState)?
                .insert(&object_id, &page)?;
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        DiscoveryBatch::try_new(&request, discovered).map_err(Into::into)
    }

    /// Returns normalized payloads for one exact discovered object.
    ///
    /// A missing in-memory page is lawfully re-fetched using the bound chunk; the operation fails
    /// if the provider response no longer matches the discovered content digest.
    pub async fn normalized_page(
        &self,
        request: &ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<BlsNormalizedPage, ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        if request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
            || request.object().dataset() != self.config.dataset()
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let (chunk_index, object_digest) = parse_object_id(request.object().object_id())?;
        let chunk = self
            .config
            .plan()
            .chunks()
            .get(chunk_index)
            .ok_or(SourceError::InvalidProtocolState)?;
        let cached = self
            .cache
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?
            .pages
            .get(request.object().object_id().as_str())
            .cloned();
        let page = match cached {
            Some(page) => {
                let bytes = Bytes::from_owner(page.bytes);
                let requested = chunk
                    .series()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let response = crate::BlsResponse::parse_for_request(
                    &bytes,
                    self.config.tier(),
                    &requested,
                    chunk.start_year(),
                    chunk.end_year(),
                )
                .map_err(|_| SourceError::InvalidProtocolState)?;
                RetrievedBlsPage {
                    bytes,
                    response,
                    received_at: page.received_at,
                    sha256_hex: page.sha256_hex,
                }
            }
            None => {
                let page = self
                    .http
                    .fetch(
                        &self.metadata,
                        &self.budget,
                        chunk,
                        request.deadline(),
                        &cancellation,
                    )
                    .await?;
                if !payload_matches_exact_evidence(&page.bytes, request.object().evidence()) {
                    return Err(SourceError::GenerationResynchronizationRequired.into());
                }
                self.cache
                    .lock()
                    .map_err(|_| SourceError::InvalidProtocolState)?
                    .insert(request.object().object_id(), &page)?;
                page
            }
        };
        if page.response.is_partial()
            || page.sha256_hex != object_digest
            || !payload_matches_exact_evidence(&page.bytes, request.object().evidence())
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        let payloads = canonical_payloads(&page.response, page.received_at, &page.sha256_hex)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if payloads.len() > request.max_records() as usize {
            return Err(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                }
                .into(),
            );
        }
        let payload_bytes = payloads.iter().try_fold(0_u64, |total, payload| {
            u64::try_from(payload.len())
                .ok()
                .and_then(|length| total.checked_add(length))
                .ok_or(SourceError::InvalidProtocolState)
        })?;
        if payload_bytes > request.max_bytes() {
            return Err(market_squawk_sources::ExtractionError::ByteLimitExceeded {
                requested: request.max_bytes(),
            }
            .into());
        }
        Ok(BlsNormalizedPage {
            received_at: page.received_at,
            source_payload_sha256: page.sha256_hex,
            payloads,
        })
    }
}

impl SourceMetadataProvider for BlsSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

fn exact_evidence(payload: &[u8]) -> ExactPayloadEvidence {
    let digest = Sha256::digest(payload);
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.into(),
    ))
}

fn parse_object_id(object_id: &SourceIdentifier) -> Result<(usize, &str), ExtractionSourceError> {
    let mut fields = object_id.as_str().split(':');
    if fields.next() != Some("bls") {
        return Err(SourceError::InvalidProtocolState.into());
    }
    let index = fields
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(SourceError::InvalidProtocolState)?;
    let digest = fields.next().ok_or(SourceError::InvalidProtocolState)?;
    if fields.next().is_some()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SourceError::InvalidProtocolState.into());
    }
    Ok((index, digest))
}

impl BlsSourceConfig {
    /// Builds the bounded request plan and a dataset identity that binds every request semantic.
    ///
    /// # Errors
    ///
    /// Rejects malformed series/year plans or a dataset identity outside domain bounds.
    pub fn try_new(
        authorization: BlsAuthorization,
        series_metadata: Vec<BlsSeriesMetadata>,
        start_year: u16,
        end_year: u16,
    ) -> Result<Self, BlsSourceError> {
        let tier = authorization.tier();
        let mut metadata_by_series = BTreeMap::new();
        for metadata in series_metadata {
            let series_id = metadata.series_id().to_owned();
            if metadata_by_series.insert(series_id, metadata).is_some() {
                return Err(BlsSourceError::InvalidSeriesMetadata);
            }
        }
        let series = metadata_by_series.keys().cloned().collect::<Vec<_>>();
        let plan = BlsRequestPlan::try_new(tier, series, start_year, end_year)
            .map_err(|_| BlsSourceError::InvalidConfiguration)?;
        if plan.chunks().len()
            > usize::from(plan.limits().daily_queries().min(REQUESTS_PER_TEN_SECONDS))
        {
            return Err(BlsSourceError::InvalidConfiguration);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/bls-request-plan/v2");
        hash.update(match tier {
            BlsAccessTier::PublicV1 => b"public-v1".as_slice(),
            BlsAccessTier::RegisteredV2 => b"registered-v2".as_slice(),
        });
        let chunk_count =
            u16::try_from(plan.chunks().len()).map_err(|_| BlsSourceError::InvalidConfiguration)?;
        hash.update(chunk_count.to_be_bytes());
        for chunk in plan.chunks() {
            hash.update(chunk.start_year().to_be_bytes());
            hash.update(chunk.end_year().to_be_bytes());
            let series_count = u16::try_from(chunk.series().len())
                .map_err(|_| BlsSourceError::InvalidConfiguration)?;
            hash.update(series_count.to_be_bytes());
            for identifier in chunk.series() {
                let length = u16::try_from(identifier.len())
                    .map_err(|_| BlsSourceError::InvalidConfiguration)?;
                hash.update(length.to_be_bytes());
                hash.update(identifier.as_bytes());
            }
        }
        let metadata_count = u16::try_from(metadata_by_series.len())
            .map_err(|_| BlsSourceError::InvalidSeriesMetadata)?;
        hash.update(metadata_count.to_be_bytes());
        for metadata in metadata_by_series.values() {
            hash_field(&mut hash, metadata.series_id().as_bytes())?;
            hash_field(
                &mut hash,
                metadata.authorization_reference().as_str().as_bytes(),
            )?;
            let content_digest = metadata.evidence().content_digest();
            hash.update(match content_digest.algorithm() {
                DigestAlgorithm::Sha256 => b"sha256".as_slice(),
                DigestAlgorithm::Blake3 => b"blake3".as_slice(),
            });
            hash.update(content_digest.bytes());
        }
        let digest = hash.finalize();
        let tier_label = match tier {
            BlsAccessTier::PublicV1 => "public-v1",
            BlsAccessTier::RegisteredV2 => "registered-v2",
        };
        let dataset = SourceIdentifier::try_from(format!("bls:timeseries:{tier_label}:{digest:x}"))
            .map_err(|_| BlsSourceError::InvalidConfiguration)?;
        Ok(Self {
            authorization,
            plan,
            series_metadata: metadata_by_series,
            dataset,
        })
    }

    /// Returns the exact request-plan-bound dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact provider tier used by this request plan.
    pub const fn tier(&self) -> BlsAccessTier {
        self.plan.tier()
    }

    /// Returns documented and enforced limits used by metadata and request composition.
    pub const fn limits(&self) -> BlsRequestLimits {
        self.plan.limits()
    }

    /// Returns the number of provider requests required for one complete discovery.
    pub fn chunk_count(&self) -> usize {
        self.plan.chunks().len()
    }

    /// Returns exact user-authorized semantic metadata for a configured series.
    pub fn series_metadata(&self, series_id: &str) -> Option<&BlsSeriesMetadata> {
        self.series_metadata.get(series_id)
    }

    pub(crate) const fn authorization(&self) -> &BlsAuthorization {
        &self.authorization
    }

    pub(crate) const fn plan(&self) -> &BlsRequestPlan {
        &self.plan
    }
}

fn hash_field(hash: &mut Sha256, value: &[u8]) -> Result<(), BlsSourceError> {
    let length = u16::try_from(value.len()).map_err(|_| BlsSourceError::InvalidSeriesMetadata)?;
    hash.update(length.to_be_bytes());
    hash.update(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use market_squawk_domain::{SourceIdentifier, Timestamp};

    use super::{PageCache, RetrievedBlsPage, parse_object_id};
    use crate::{BlsAccessTier, BlsResponse};

    fn page(
        bytes: &'static [u8],
        digest: &str,
    ) -> Result<RetrievedBlsPage, Box<dyn std::error::Error>> {
        Ok(RetrievedBlsPage {
            bytes: Bytes::from_static(bytes),
            response: BlsResponse::parse(
                include_bytes!("../fixtures/series.json"),
                BlsAccessTier::PublicV1,
            )?,
            received_at: Timestamp::from_unix_nanos(1),
            sha256_hex: digest.to_owned(),
        })
    }

    #[test]
    fn object_id_requires_exact_lowercase_sha256() -> Result<(), Box<dyn std::error::Error>> {
        let lowercase = SourceIdentifier::try_from(format!("bls:0:{}", "a".repeat(64)))?;
        assert_eq!(parse_object_id(&lowercase)?.0, 0);

        let uppercase = SourceIdentifier::try_from(format!("bls:0:{}", "A".repeat(64)))?;
        assert!(parse_object_id(&uppercase).is_err());
        Ok(())
    }

    #[test]
    fn full_cache_skips_new_pages_without_crossing_its_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let first_id = SourceIdentifier::try_from("bls:first")?;
        let second_id = SourceIdentifier::try_from("bls:second")?;

        let first = page(b"1234", "first")?;
        let first_charge = PageCache::retained_charge(&first_id, &first)?;
        let mut cache = PageCache::with_limit(first_charge);

        assert!(cache.insert(&first_id, &first)?);
        assert!(!cache.insert(&second_id, &page(b"5", "second")?)?);
        assert_eq!(cache.retained_bytes, first_charge);
        assert!(cache.pages.contains_key(first_id.as_str()));
        assert!(!cache.pages.contains_key(second_id.as_str()));
        Ok(())
    }
}
