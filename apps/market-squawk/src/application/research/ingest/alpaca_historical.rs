//! One generation-bound Alpaca historical source with bounded immutable click-plan admission.

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_alpaca::{
    AlpacaHistoricalBarTimeAuthority, AlpacaHistoricalEquityConfig,
    AlpacaHistoricalEquityDatasetPlan, AlpacaHistoricalEquityPreflightPlan,
    AlpacaHistoricalEquityPreflightReceipt, AlpacaHistoricalEquitySource,
};
use market_squawk_data::DatasetId;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MarketDataInstrumentDefinition, ProviderInstrumentId,
    SourceIdentifier,
};
use market_squawk_sources::{
    AuthorizationMode, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch,
    ExtractionRequest, ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    HttpRequestBounds, SourceError, SourceMetadata, SourceMetadataProvider,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    ManagedExtraction, ManagedResearchExtractionSource, ResearchRevisionPlanError,
    bind_provider_capture_graph,
};
use crate::{
    application::market_runtime::{
        AlpacaHistoricalCompositeCalendarAuthority, AlpacaHistoricalRuntimeCapability,
    },
    provider_activation::ProviderMarketAccount,
};

const MAXIMUM_ALPACA_HISTORICAL_PLANS: usize = 4_096;
const MAXIMUM_ALPACA_HISTORICAL_RETAINED_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// Sole mutation authority for the immutable plan directory of one account generation.
///
/// This value is intentionally non-cloneable. It carries no credentials, provider-rate budget,
/// registry mutation, or source-replacement authority.
pub(crate) struct AlpacaHistoricalPlanDirectoryAuthority {
    inner: Arc<AlpacaHistoricalPlanDirectoryInner>,
}

/// Secret-free source handle intended for one later serialized registry installation.
///
/// The handle is intentionally non-cloneable. Discovery and extraction consume only retained raw
/// preflight bytes; its opaque runtime capability supplies currentness and is unusable after the
/// owner drains and clears the separate credential-bearing authority.
pub(crate) struct AlpacaHistoricalManagedSource {
    inner: Arc<AlpacaHistoricalPlanDirectoryInner>,
}

struct AlpacaHistoricalPlanDirectoryInner {
    metadata: SourceMetadata,
    request_bounds: HttpRequestBounds,
    parent_digest: EvidenceDigest,
    runtime: AlpacaHistoricalRuntimeCapability,
    admission: tokio::sync::Mutex<()>,
    plans: Mutex<Vec<Arc<AlpacaHistoricalPlanRecord>>>,
}

#[derive(Debug)]
struct AlpacaHistoricalPlanRecord {
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    parent_digest: EvidenceDigest,
    plan_digest: EvidenceDigest,
    config: AlpacaHistoricalEquityConfig,
    canonical_instrument: MarketDataInstrumentDefinition,
    preflight: Arc<AlpacaHistoricalEquityPreflightReceipt>,
    bar_time_authority: Arc<AlpacaHistoricalCompositeCalendarAuthority>,
    retained_response_bytes: usize,
}

impl AlpacaHistoricalPlanRecord {
    fn same_identity(&self, other: &Self) -> bool {
        self.provider_dataset == other.provider_dataset
            && self.analytical_dataset == other.analytical_dataset
            && self.parent_digest == other.parent_digest
            && self.plan_digest == other.plan_digest
            && self.config == other.config
            && self.canonical_instrument == other.canonical_instrument
            && self.preflight.digest() == other.preflight.digest()
            && self.preflight.as_ref() == other.preflight.as_ref()
            && self.bar_time_authority.preflight_digest()
                == other.bar_time_authority.preflight_digest()
            && self.bar_time_authority.series_semantics()
                == other.bar_time_authority.series_semantics()
            && self.retained_response_bytes == other.retained_response_bytes
    }
}

/// Non-forgeable proof that one exact content-addressed plan was admitted.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AlpacaHistoricalPlanReceipt {
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    parent_digest: EvidenceDigest,
    plan_digest: EvidenceDigest,
    _private: (),
}

impl AlpacaHistoricalPlanReceipt {
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    pub(crate) const fn parent_digest(&self) -> EvidenceDigest {
        self.parent_digest
    }

    pub(crate) const fn plan_digest(&self) -> EvidenceDigest {
        self.plan_digest
    }
}

impl AlpacaHistoricalPlanDirectoryAuthority {
    /// Constructs the sole source/directory pair for one exact active account generation.
    ///
    /// The account runtime permanently claims one wrapper before return. Reinvocation for the
    /// same generation fails instead of manufacturing a new source profile.
    pub(crate) async fn try_new(
        runtime: AlpacaHistoricalRuntimeCapability,
        metadata: SourceMetadata,
        request_bounds: HttpRequestBounds,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Self, AlpacaHistoricalManagedSource), AlpacaHistoricalPlanAdmissionError> {
        runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        validate_parent_binding(&runtime, &metadata)?;
        AlpacaHistoricalEquityConfig::validate_parent_metadata(&metadata, request_bounds)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::ParentBindingMismatch)?;
        let parent_digest = parent_binding_digest(&runtime, &metadata, request_bounds)?;
        let mut plans = Vec::new();
        plans
            .try_reserve_exact(MAXIMUM_ALPACA_HISTORICAL_PLANS)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::CapacityUnavailable)?;
        if !runtime
            .try_claim_plan_source()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?
        {
            return Err(AlpacaHistoricalPlanAdmissionError::SourceAlreadyClaimed);
        }
        let inner = Arc::new(AlpacaHistoricalPlanDirectoryInner {
            metadata,
            request_bounds,
            parent_digest,
            runtime,
            admission: tokio::sync::Mutex::new(()),
            plans: Mutex::new(plans),
        });
        Ok((
            Self {
                inner: Arc::clone(&inner),
            },
            AlpacaHistoricalManagedSource { inner },
        ))
    }

    /// Admits or idempotently resolves one exact FIGI-backed click plan.
    pub(crate) async fn admit_plan(
        &self,
        preflight_plan: AlpacaHistoricalEquityPreflightPlan,
        canonical_instrument: MarketDataInstrumentDefinition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalPlanReceipt, AlpacaHistoricalPlanAdmissionError> {
        // A single generation owns this directory's mutation authority. Serializing the bounded
        // network preflight with publication prevents concurrent identical clicks from minting
        // different observation-time receipts before either immutable record becomes visible.
        let _admission = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            admission = self.inner.admission.lock() => admission,
        };
        self.inner
            .runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let timeframe = preflight_plan
            .timeframe()
            .provider_identifier()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
        if timeframe.as_str() != "1Day" {
            return Err(AlpacaHistoricalPlanAdmissionError::CalendarUnavailable);
        }
        AlpacaHistoricalEquitySource::validate_one_preflight_instrument(
            &self.inner.metadata,
            &preflight_plan,
            &canonical_instrument,
        )
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidInstrumentAuthority)?;
        {
            let plans = self
                .inner
                .plans
                .lock()
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::DirectoryUnavailable)?;
            if let Some(existing) = plans
                .iter()
                .find(|record| record.preflight.plan() == &preflight_plan)
            {
                if existing.canonical_instrument == canonical_instrument {
                    return Ok(receipt(existing));
                }
                return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision);
            }
            if plans.len() == MAXIMUM_ALPACA_HISTORICAL_PLANS {
                return Err(AlpacaHistoricalPlanAdmissionError::PlanCapacityExceeded);
            }
        }
        let provider_instrument_id =
            ProviderInstrumentId::try_from(preflight_plan.mapping().symbol().to_owned())
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidInstrumentAuthority)?;
        let preflight = self
            .inner
            .runtime
            .preflight_plan(
                preflight_plan,
                self.inner.request_bounds,
                deadline,
                cancellation,
            )
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::PreflightUnavailable)?;
        let bar_time_authority = self
            .inner
            .runtime
            .compose_returned_bar_calendar(
                &preflight,
                preflight.plan().mapping().instrument(),
                provider_instrument_id,
                self.inner.request_bounds,
                deadline,
                cancellation,
            )
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::CalendarUnavailable)?;
        let plan = AlpacaHistoricalEquityDatasetPlan::bind_preflight(
            preflight.plan().clone(),
            bar_time_authority.series_semantics().clone(),
        );
        let config = AlpacaHistoricalEquityConfig::try_bind_one_plan(
            self.inner.metadata.clone(),
            plan,
            self.inner.request_bounds,
        )
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
        let provider_dataset = exactly_one_provider_dataset(&config)?;
        let analytical_identifier =
            AlpacaHistoricalEquitySource::one_plan_analytical_dataset_identifier(
                &config,
                &canonical_instrument,
            )
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidInstrumentAuthority)?;
        let analytical_dataset = DatasetId::try_from(analytical_identifier.as_str())
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
        let plan_digest = plan_binding_digest(
            self.inner.parent_digest,
            &config,
            &canonical_instrument,
            &provider_dataset,
            &analytical_dataset,
            preflight.digest(),
            bar_time_authority.preflight_digest(),
        )?;
        let retained_response_bytes = preflight
            .total_response_bytes()
            .checked_add(bar_time_authority.retained_response_bytes())
            .filter(|bytes| *bytes <= MAXIMUM_ALPACA_HISTORICAL_RETAINED_RESPONSE_BYTES)
            .ok_or(AlpacaHistoricalPlanAdmissionError::RetainedResponseCapacityExceeded)?;
        let candidate = Arc::new(AlpacaHistoricalPlanRecord {
            provider_dataset,
            analytical_dataset,
            parent_digest: self.inner.parent_digest,
            plan_digest,
            config,
            canonical_instrument,
            preflight,
            bar_time_authority,
            retained_response_bytes,
        });

        self.inner
            .runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let record = {
            let mut plans = self
                .inner
                .plans
                .lock()
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::DirectoryUnavailable)?;
            let position = plans.binary_search_by(|record| {
                record
                    .provider_dataset
                    .as_str()
                    .cmp(candidate.provider_dataset.as_str())
            });
            match position {
                Ok(index) if plans[index].same_identity(candidate.as_ref()) => {
                    Arc::clone(&plans[index])
                }
                Ok(_index) => return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision),
                Err(_index)
                    if plans
                        .iter()
                        .any(|record| record.plan_digest == candidate.plan_digest) =>
                {
                    return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision);
                }
                Err(_index) if plans.len() == MAXIMUM_ALPACA_HISTORICAL_PLANS => {
                    return Err(AlpacaHistoricalPlanAdmissionError::PlanCapacityExceeded);
                }
                Err(index) => {
                    let retained = plans
                        .iter()
                        .try_fold(candidate.retained_response_bytes, |total, record| {
                            total.checked_add(record.retained_response_bytes)
                        });
                    if retained.is_none_or(|bytes| {
                        bytes > MAXIMUM_ALPACA_HISTORICAL_RETAINED_RESPONSE_BYTES
                    }) {
                        return Err(
                            AlpacaHistoricalPlanAdmissionError::RetainedResponseCapacityExceeded,
                        );
                    }
                    plans.insert(index, Arc::clone(&candidate));
                    candidate
                }
            }
        };
        self.inner
            .runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        Ok(receipt(record.as_ref()))
    }
}

impl SourceMetadataProvider for AlpacaHistoricalManagedSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.inner.metadata
    }
}

impl ExtractionSource for AlpacaHistoricalManagedSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        let record = match self.inner.plan(request.dataset()) {
            Ok(record) => record,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let runtime = self.inner.runtime.clone();
        let bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority> =
            record.bar_time_authority.clone();
        let preflight = Arc::clone(&record.preflight);
        Box::pin(async move {
            runtime
                .discover_plan(
                    record.config.clone(),
                    record.canonical_instrument.clone(),
                    bar_time_authority,
                    preflight,
                    authority,
                    request,
                    cancellation,
                )
                .await
        })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let record = match self.inner.plan(request.object().dataset()) {
            Ok(record) => record,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let runtime = self.inner.runtime.clone();
        let bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority> =
            record.bar_time_authority.clone();
        let preflight = Arc::clone(&record.preflight);
        Box::pin(async move {
            runtime
                .extract_plan(
                    record.config.clone(),
                    record.canonical_instrument.clone(),
                    bar_time_authority,
                    preflight,
                    authority,
                    request,
                    cancellation,
                )
                .await
        })
    }
}

impl ManagedResearchExtractionSource for AlpacaHistoricalManagedSource {
    fn extract_managed(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtraction, ExtractionSourceError>> {
        let record = match self.inner.plan(request.object().dataset()) {
            Ok(record) => record,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let runtime = self.inner.runtime.clone();
        let bar_time_authority = Arc::clone(&record.bar_time_authority);
        let preflight = Arc::clone(&record.preflight);
        Box::pin(async move {
            let output = runtime
                .extract_plan_with_capture(
                    record.config.clone(),
                    record.canonical_instrument.clone(),
                    bar_time_authority,
                    preflight,
                    authority,
                    request,
                    cancellation,
                )
                .await?;
            let (batch, bar_capture, calendar_capture) = output.into_parts();
            bind_provider_capture_graph(
                batch,
                b"alpaca-iex-historical-bars-and-calendar/v1",
                vec![bar_capture, calendar_capture],
            )
        })
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let record = self
            .inner
            .plan(batch.request().object().dataset())
            .map_err(|_error| ResearchRevisionPlanError)?;
        let identifier = self
            .inner
            .runtime
            .analytical_dataset_for_plan(&record.config, &record.canonical_instrument, batch)
            .map_err(|_error| ResearchRevisionPlanError)?;
        let dataset =
            DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)?;
        if dataset != record.analytical_dataset {
            return Err(ResearchRevisionPlanError);
        }
        Ok(dataset)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        let record = self
            .inner
            .plan(batch.request().object().dataset())
            .map_err(|_error| ResearchRevisionPlanError)?;
        self.inner
            .runtime
            .revision_plan_for_plan(&record.config, batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

impl AlpacaHistoricalPlanDirectoryInner {
    fn plan(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<Arc<AlpacaHistoricalPlanRecord>, ExtractionSourceError> {
        if self.runtime.is_revoked() {
            return Err(SourceError::SessionNotCurrent.into());
        }
        let plans = self
            .plans
            .lock()
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        let index = plans
            .binary_search_by(|record| record.provider_dataset.as_str().cmp(dataset.as_str()))
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        Ok(Arc::clone(&plans[index]))
    }
}

impl fmt::Debug for AlpacaHistoricalManagedSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalManagedSource")
            .field("source_id", self.inner.metadata.source_id())
            .field("revision", self.inner.metadata.revision())
            .field("parent_digest", &self.inner.parent_digest)
            .field("credentials", &"[RUNTIME-OWNED; NOT RETAINED BY PLAN]")
            .field("revoked", &self.inner.runtime.is_revoked())
            .finish_non_exhaustive()
    }
}

/// Closed fail-safe outcomes for source construction and immutable plan admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum AlpacaHistoricalPlanAdmissionError {
    #[error("Alpaca historical account runtime is unavailable or stale")]
    RuntimeUnavailable,
    #[error("this Alpaca account generation already claimed its historical source")]
    SourceAlreadyClaimed,
    #[error("Alpaca historical parent metadata is not bound to this account generation")]
    ParentBindingMismatch,
    #[error("the exact governed Alpaca calendar is unavailable")]
    CalendarUnavailable,
    #[error("the exact terminal Alpaca historical preflight is unavailable")]
    PreflightUnavailable,
    #[error("the Alpaca historical plan is invalid")]
    InvalidPlan,
    #[error("the canonical FIGI/provider identity does not authorize this plan")]
    InvalidInstrumentAuthority,
    #[error("the fixed plan directory allocation is unavailable")]
    CapacityUnavailable,
    #[error("the fixed Alpaca historical plan directory is full")]
    PlanCapacityExceeded,
    #[error("retained exact historical responses exceed the directory byte budget")]
    RetainedResponseCapacityExceeded,
    #[error("a content-addressed plan identity collision was detected")]
    IdentityCollision,
    #[error("the plan directory is unavailable")]
    DirectoryUnavailable,
    #[error("canonical plan evidence could not be serialized")]
    Serialization,
}

fn validate_parent_binding(
    runtime: &AlpacaHistoricalRuntimeCapability,
    metadata: &SourceMetadata,
) -> Result<(), AlpacaHistoricalPlanAdmissionError> {
    let binding = runtime.account_binding();
    if binding.account() != ProviderMarketAccount::AlpacaBasic
        || runtime.surface_id().as_str() != ProviderMarketAccount::AlpacaBasic.surface_id()
        || runtime.onboarding_session_id().is_nil()
        || metadata.provider().as_str() != "alpaca-market-data"
        || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
        || metadata.authorization().basis().as_source_identifier() != binding.subject()
        || metadata.authorization().evidence().content_digest() != binding.verification_evidence()
    {
        return Err(AlpacaHistoricalPlanAdmissionError::ParentBindingMismatch);
    }
    Ok(())
}

fn exactly_one_provider_dataset(
    config: &AlpacaHistoricalEquityConfig,
) -> Result<SourceIdentifier, AlpacaHistoricalPlanAdmissionError> {
    let mut datasets = config.provider_dataset_identifiers();
    let dataset = datasets
        .next()
        .cloned()
        .ok_or(AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
    if datasets.next().is_some() {
        return Err(AlpacaHistoricalPlanAdmissionError::InvalidPlan);
    }
    Ok(dataset)
}

fn parent_binding_digest(
    runtime: &AlpacaHistoricalRuntimeCapability,
    metadata: &SourceMetadata,
    request_bounds: HttpRequestBounds,
) -> Result<EvidenceDigest, AlpacaHistoricalPlanAdmissionError> {
    let metadata = serde_json::to_vec(metadata)
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let request_bounds = serde_json::to_vec(&request_bounds)
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-parent-generation/v1\0");
    hash_bytes(&mut digest, runtime.surface_id().as_str().as_bytes());
    digest.update(runtime.onboarding_session_id().as_bytes());
    digest.update(runtime.credential_generation().get().to_be_bytes());
    hash_bytes(
        &mut digest,
        runtime.account_binding().subject().as_str().as_bytes(),
    );
    digest.update(runtime.account_binding().verification_evidence().bytes());
    digest.update(runtime.account_digest().bytes());
    digest.update(runtime.public_configuration_digest().bytes());
    digest.update(runtime.runtime_evidence_digest().bytes());
    digest.update([match runtime.trading_api_environment() {
        market_squawk_adapter_alpaca::AlpacaTradingApiEnvironment::Live => 1,
        market_squawk_adapter_alpaca::AlpacaTradingApiEnvironment::Paper => 2,
    }]);
    hash_bytes(&mut digest, &metadata);
    hash_bytes(&mut digest, &request_bounds);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn plan_binding_digest(
    parent_digest: EvidenceDigest,
    config: &AlpacaHistoricalEquityConfig,
    canonical_instrument: &MarketDataInstrumentDefinition,
    provider_dataset: &SourceIdentifier,
    analytical_dataset: &DatasetId,
    preflight_digest: EvidenceDigest,
    authority_preflight_digest: EvidenceDigest,
) -> Result<EvidenceDigest, AlpacaHistoricalPlanAdmissionError> {
    if preflight_digest != authority_preflight_digest {
        return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision);
    }
    let metadata = serde_json::to_vec(config.metadata())
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let canonical_instrument = serde_json::to_vec(canonical_instrument)
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-admitted-plan/v1\0");
    digest.update(parent_digest.bytes());
    hash_bytes(&mut digest, provider_dataset.as_str().as_bytes());
    hash_bytes(&mut digest, analytical_dataset.as_str().as_bytes());
    digest.update(preflight_digest.bytes());
    hash_bytes(&mut digest, &metadata);
    hash_bytes(&mut digest, &canonical_instrument);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn receipt(record: &AlpacaHistoricalPlanRecord) -> AlpacaHistoricalPlanReceipt {
    AlpacaHistoricalPlanReceipt {
        provider_dataset: record.provider_dataset.clone(),
        analytical_dataset: record.analytical_dataset.clone(),
        parent_digest: record.parent_digest,
        plan_digest: record.plan_digest,
        _private: (),
    }
}
