use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::VecDeque;
#[cfg(test)]
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    SourceIdentifier, Timestamp,
};
#[cfg(test)]
use market_squawk_sources::AuthoritativeSourceRegistry;
use market_squawk_sources::{
    AuthorizationMode, BudgetWindowSemantics, CoverageDomain, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionRequest, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, HistoricalCapability, ProviderBudgetPolicy,
    SourceClass, SourceError, SourceMetadata, SourceMetadataProvider, SourceObject,
    SourceProtocolProfile, payload_matches_exact_evidence,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::client::{BlsHttpClient, RetrievedBlsPage, ensure_deadline_open, system_timestamp};
use crate::{
    BlsAccessTier, BlsAuthorization, BlsRequestLimits, BlsRequestPlan, BlsSeriesMetadata,
    BlsSourceError,
};

const NANOS_PER_DAY: u64 = 86_400_000_000_000;
const NANOS_PER_TEN_SECONDS: u64 = 10_000_000_000;
const REQUESTS_PER_TEN_SECONDS: u16 = 50;

mod normalize;
mod state;

use normalize::canonical_records;
use state::PageCache;
pub use state::{BlsNormalizedPage, BlsSourceHealth};

/// Exact, deterministic BLS source configuration bound into its dataset identity.
#[derive(Clone, Debug)]
pub struct BlsSourceConfig {
    authorization: BlsAuthorization,
    plan: BlsRequestPlan,
    series_metadata: BTreeMap<String, BlsSeriesMetadata>,
    dataset: SourceIdentifier,
}

/// Registered, allowlisted, budget-coordinated BLS extraction producer.
pub struct BlsSource {
    metadata: SourceMetadata,
    config: BlsSourceConfig,
    http: BlsHttpClient,
    cache: Mutex<PageCache>,
    health: Mutex<BlsSourceHealth>,
    #[cfg(test)]
    publication_actions: Mutex<VecDeque<Option<AuthoritativeSourceRegistry>>>,
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
    /// Builds honest local-observation revision authority for a normalized BLS batch.
    ///
    /// The BLS timeseries response does not publish a per-observation version or publication
    /// coordinate. Market Squawk therefore binds revisions to exact canonical content and local
    /// observation order instead of fabricating provider chronology.
    ///
    /// # Errors
    ///
    /// Returns [`BlsSourceError::InvalidMetadata`] when the batch belongs to another source
    /// registration and [`BlsSourceError::RevisionAuthority`] when bounded evidence construction
    /// fails.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, BlsSourceError> {
        if batch.request().object().source_id() != self.metadata.source_id()
            || batch.request().object().metadata_revision() != self.metadata.revision()
        {
            return Err(BlsSourceError::InvalidMetadata);
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len()).map_err(Into::into)
    }

    /// Binds a provider configuration to exact immutable metadata.
    ///
    /// # Errors
    ///
    /// Fails closed unless metadata declares an official-agency, macroeconomic, historical,
    /// extraction-only source with an allowlisted endpoint and the matching authorization mode.
    pub fn try_new(
        metadata: SourceMetadata,
        config: BlsSourceConfig,
    ) -> Result<Self, BlsSourceError> {
        Self::validate_metadata(&metadata, &config)?;
        let http = BlsHttpClient::try_new(&metadata, config.authorization().clone())?;
        Ok(Self {
            metadata,
            config,
            http,
            cache: Mutex::new(PageCache::new()),
            health: Mutex::new(BlsSourceHealth::new()),
            #[cfg(test)]
            publication_actions: Mutex::new(VecDeque::new()),
        })
    }

    #[cfg(test)]
    fn try_new_with_transport(
        metadata: SourceMetadata,
        config: BlsSourceConfig,
        transport: Arc<dyn crate::client::BlsTransport>,
    ) -> Result<Self, BlsSourceError> {
        Self::validate_metadata(&metadata, &config)?;
        let http = BlsHttpClient::try_new_with_transport(
            &metadata,
            config.authorization().clone(),
            transport,
        )?;
        Ok(Self {
            metadata,
            config,
            http,
            cache: Mutex::new(PageCache::new()),
            health: Mutex::new(BlsSourceHealth::new()),
            publication_actions: Mutex::new(VecDeque::new()),
        })
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
        config: &BlsSourceConfig,
    ) -> Result<(), BlsSourceError> {
        let expected_mode = match config.authorization() {
            BlsAuthorization::PublicV1 => AuthorizationMode::PublicInterface,
            BlsAuthorization::RegisteredV2(_) => AuthorizationMode::UserAuthorized,
        };
        let budget_policy = metadata
            .budget_policy()
            .ok_or(BlsSourceError::InvalidMetadata)?;
        if metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != "us-bls"
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || metadata.authorization().mode() != expected_mode
            || !budget_matches_provider_limits(budget_policy, config.limits())
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(BlsSourceError::InvalidMetadata);
        }
        metadata
            .network_policy()
            .authorize(config.authorization().endpoint())
            .map_err(|_| BlsSourceError::InvalidMetadata)
    }

    /// Returns the exact request-plan-bound dataset callers use for discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        self.config.dataset()
    }

    /// Returns a bounded copy of local producer health.
    ///
    /// # Errors
    ///
    /// Fails closed if health synchronization was poisoned.
    pub fn health(&self) -> Result<BlsSourceHealth, BlsSourceError> {
        self.health
            .lock()
            .map(|health| *health)
            .map_err(|_| BlsSourceError::HealthUnavailable)
    }

    /// Fetches and discovers exact response objects under bounded network and cache limits.
    pub async fn discover_pages(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.dataset() != self.config.dataset()
            || request.effective_at().is_some()
            || self.config.plan().chunks().len() > usize::from(request.max_results())
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let mut discovered = Vec::with_capacity(self.config.plan().chunks().len());
        for (index, chunk) in self.config.plan().chunks().iter().enumerate() {
            let page = self
                .fetch_page(&authority, chunk, request.deadline(), &cancellation)
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
        #[cfg(test)]
        self.apply_test_publication_action()?;
        authority.validate_current()?;
        DiscoveryBatch::try_new(&request, discovered).map_err(Into::into)
    }

    /// Returns normalized payloads for one exact discovered object.
    ///
    /// A missing in-memory page is lawfully re-fetched using the bound chunk; the operation fails
    /// if the provider response no longer matches the discovered content digest.
    pub async fn normalized_page(
        &self,
        authority: &ExtractionAuthority,
        request: &ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<BlsNormalizedPage, ExtractionSourceError> {
        self.validate_authority(authority)?;
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
                    .fetch_page(authority, chunk, request.deadline(), &cancellation)
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
        let observed_at = request.object().effective_interval().starts_at();
        let ingested_at = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        let records = canonical_records(
            &self.metadata,
            &self.config,
            &page.response,
            &page.bytes,
            observed_at,
            ingested_at,
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        if records.len() > request.max_records() as usize {
            return Err(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                }
                .into(),
            );
        }
        let payload_bytes = records.iter().try_fold(0_u64, |total, record| {
            u64::try_from(record.payload.len())
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
        let payloads = records
            .iter()
            .map(|record| record.payload.clone())
            .collect();
        #[cfg(test)]
        self.apply_test_publication_action()?;
        authority.validate_current()?;
        Ok(BlsNormalizedPage {
            received_at: observed_at,
            source_payload_sha256: page.sha256_hex,
            exact_payload: page.bytes,
            payloads,
            records,
        })
    }

    async fn extract_impl(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExtractionBatch, ExtractionSourceError> {
        let page = self
            .normalized_page(&authority, &request, cancellation)
            .await?;
        let schema = SourceIdentifier::try_from("market-squawk-research-v3")
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let records = page
            .records
            .into_iter()
            .map(|record| {
                market_squawk_sources::ExtractionRecord::try_new_with_time(
                    &request,
                    schema.clone(),
                    record.evidence,
                    record.effective,
                    None,
                    record.availability,
                    record.revision,
                    None,
                    record.payload,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = ExtractionBatch::try_new(&request, records)?;
        #[cfg(test)]
        self.apply_test_publication_action()?;
        authority.validate_current()?;
        Ok(batch)
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

    async fn fetch_page(
        &self,
        authority: &ExtractionAuthority,
        chunk: &crate::BlsRequestChunk,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedBlsPage, ExtractionSourceError> {
        self.record_attempt()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let result = self
            .http
            .fetch(&self.metadata, authority, chunk, deadline, cancellation)
            .await;
        self.record_result(&result)?;
        result
    }

    fn record_attempt(&self) -> Result<(), BlsSourceError> {
        let now = system_timestamp()?;
        let mut health = self
            .health
            .lock()
            .map_err(|_| BlsSourceError::HealthUnavailable)?;
        health.last_attempt_at = Some(now);
        Ok(())
    }

    fn record_result(
        &self,
        result: &Result<RetrievedBlsPage, ExtractionSourceError>,
    ) -> Result<(), ExtractionSourceError> {
        let mut health = self
            .health
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        match result {
            Ok(page) => {
                health.last_success_at = Some(page.received_at);
                health.last_payload_digest = Some(Sha256::digest(&page.bytes).into());
                health.consecutive_failures = 0;
            }
            Err(_) => {
                health.consecutive_failures = health.consecutive_failures.saturating_add(1);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn queue_test_publication_action(
        &self,
        registry_to_drop: Option<AuthoritativeSourceRegistry>,
    ) -> Result<(), BlsSourceError> {
        self.publication_actions
            .lock()
            .map_err(|_| BlsSourceError::HealthUnavailable)?
            .push_back(registry_to_drop);
        Ok(())
    }

    #[cfg(test)]
    fn apply_test_publication_action(&self) -> Result<(), ExtractionSourceError> {
        let registry_to_drop = self
            .publication_actions
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?
            .pop_front()
            .flatten();
        drop(registry_to_drop);
        Ok(())
    }
}

impl SourceMetadataProvider for BlsSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for BlsSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(self.discover_pages(authority, request, cancellation))
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

fn budget_matches_provider_limits(policy: &ProviderBudgetPolicy, limits: BlsRequestLimits) -> bool {
    let Some(short) = policy.window(0) else {
        return false;
    };
    let Some(daily) = policy.window(1) else {
        return false;
    };
    policy.window_count() == 2
        && short.requests_per_window() == u32::from(REQUESTS_PER_TEN_SECONDS)
        && short.window_nanos() == NANOS_PER_TEN_SECONDS
        && short.semantics() == BudgetWindowSemantics::Sliding
        && daily.requests_per_window() == u32::from(limits.daily_queries())
        && daily.window_nanos() == NANOS_PER_DAY
        && daily.semantics() == BudgetWindowSemantics::Sliding
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
#[path = "source/tests.rs"]
mod tests;
