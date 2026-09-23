//! Production composition for bounded Tiingo Starter daily NAV and EOD operations.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use market_squawk_adapter_tiingo::{
    TiingoAdapterError, TiingoApiToken, TiingoEodBarTimeAuthority, TiingoEodContractEvidence,
    TiingoEodInstrumentAuthority, TiingoFundContext, TiingoFundNavContractEvidence,
    TiingoHttpSource, TiingoHttpSourceError, TiingoLatestPublicationError, TiingoQuotaError,
    TiingoQuotaWindows, TiingoSchemaCircuitState, TiingoTicker, prepare_latest_publication,
    tiingo_provider_rate_declaration,
};
use market_squawk_data::{
    AnalyticalFundNavOutput, AnalyticalFundNavReadRequest, AnalyticalMarketBarOutput,
    AnalyticalMarketBarReadRequest, DatasetId, DatasetManifestRef,
    PersistedProviderCaptureBindingEvidence, QueryLimits,
};
use market_squawk_domain::{EvidenceDigest, SourceId, SourceIdentifier, TimeError, Timestamp};
use market_squawk_sources::{ExtractionRequest, ProviderRateAvailability, SourceMetadata};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::ProviderAdapterActivation;
use crate::application::{
    ProductionResearchIngestCoordinator, ResearchProviderPublicationOperation,
    ResearchProviderRuntimeGeneration, ResearchRightsAuthority,
};
use crate::provider_rate::DurableTiingoProviderAuthority;
use crate::{ProviderActivationLease, ProviderOnboardingError};

pub(crate) const TIINGO_FUND_NAV_OPERATION: &str = "Research.GetTiingoFundNav";
pub(crate) const TIINGO_EOD_OPERATION: &str = "Markets.GetTiingoEod";
pub(super) const TIINGO_SURFACE: &str = "tiingo.starter-eod-nav";
const TIINGO_SOURCE_ID: &str = "tiingo-starter";
const TIINGO_NATIVE_CONTRACT_REVISION: &str = "tiingo-daily-native-v1";
const HOUR_NANOS: i64 = 3_600_000_000_000;
const DAY_NANOS: i64 = 86_400_000_000_000;
const CONSERVATIVE_MONTH_NANOS: i64 = 2_764_800_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TiingoProductAvailability {
    SetupRequired,
    Available,
    Unavailable,
}

/// Sanitized product status. Credential bytes, source clients, durable-store keys, and paths are
/// structurally absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TiingoProductStatus {
    pub(crate) availability: TiingoProductAvailability,
    pub(crate) schema_circuit: Option<TiingoSchemaCircuitState>,
    pub(crate) monthly_quota_is_durable: bool,
    pub(crate) history_checkpoints_are_durable: bool,
    pub(crate) shared_budget: Option<ProviderRateAvailability>,
    pub(crate) monthly_unique_symbols_exhausted: bool,
    pub(crate) monthly_bandwidth_exhausted: bool,
    pub(crate) monthly_overage_until: Option<Timestamp>,
    pub(crate) monthly_resets_at: Option<Timestamp>,
    pub(crate) last_provider_rate_limit_observed_at: Option<Timestamp>,
    pub(crate) provider_retry_after_was_present: bool,
}

impl TiingoProductStatus {
    fn setup_required() -> Self {
        Self {
            availability: TiingoProductAvailability::SetupRequired,
            schema_circuit: None,
            monthly_quota_is_durable: true,
            history_checkpoints_are_durable: true,
            shared_budget: None,
            monthly_unique_symbols_exhausted: false,
            monthly_bandwidth_exhausted: false,
            monthly_overage_until: None,
            monthly_resets_at: None,
            last_provider_rate_limit_observed_at: None,
            provider_retry_after_was_present: false,
        }
    }

    fn unavailable() -> Self {
        Self {
            availability: TiingoProductAvailability::Unavailable,
            ..Self::setup_required()
        }
    }
}

/// Fixed latest operation. Mutual-fund NAV and exchange-listed EOD are separate variants and
/// cannot substitute for one another.
pub(crate) enum TiingoLatestOperation {
    FundNav {
        ticker: TiingoTicker,
        metadata_event_id: Uuid,
        latest_event_id: Uuid,
        connection_id: Uuid,
        context: TiingoFundContext,
        contract: TiingoFundNavContractEvidence,
        extraction_request: ExtractionRequest,
        analytical_dataset: DatasetId,
    },
    Eod {
        ticker: TiingoTicker,
        metadata_event_id: Uuid,
        latest_event_id: Uuid,
        connection_id: Uuid,
        instrument: TiingoEodInstrumentAuthority,
        contract: TiingoEodContractEvidence,
        bar_time_authority: Arc<dyn TiingoEodBarTimeAuthority>,
        extraction_request: ExtractionRequest,
        analytical_dataset: DatasetId,
    },
}

impl fmt::Debug for TiingoLatestOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FundNav { ticker, .. } => formatter
                .debug_struct("TiingoLatestOperation::FundNav")
                .field("ticker", ticker)
                .finish_non_exhaustive(),
            Self::Eod { ticker, .. } => formatter
                .debug_struct("TiingoLatestOperation::Eod")
                .field("ticker", ticker)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TiingoCanonicalFamily {
    FundNav,
    EodMarketBar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TiingoUnavailableReason {
    UnsupportedMetadataCoverage,
    EmptyLatestResponse,
    NoCompleteEodSurface,
}

/// Closed durable result with no provider client or state-store capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TiingoLatestOperationOutcome {
    Published {
        family: TiingoCanonicalFamily,
        restart: TiingoRestartCoordinates,
        provider_dataset: SourceIdentifier,
        records: usize,
    },
    Unavailable {
        family: TiingoCanonicalFamily,
        reason: TiingoUnavailableReason,
        sealed_capture_receipt: EvidenceDigest,
        returned_rows: u32,
        surface_gaps: u32,
    },
}

/// Complete durable coordinates for exact-manifest NAV/EOD restart verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TiingoRestartCoordinates {
    pub(crate) family: TiingoCanonicalFamily,
    pub(crate) manifest: DatasetManifestRef,
    pub(crate) binding_digest: EvidenceDigest,
    pub(crate) source_id: SourceId,
    pub(crate) expected_record_count: usize,
    pub(crate) native_schema_version: u16,
    pub(crate) native_schema_fingerprint: EvidenceDigest,
}

impl TiingoRestartCoordinates {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
}

#[derive(Debug)]
pub(crate) enum TiingoRestartRequest {
    FundNav {
        request: AnalyticalFundNavReadRequest,
        limits: QueryLimits,
        deadline: Instant,
    },
    Eod {
        request: AnalyticalMarketBarReadRequest,
        limits: QueryLimits,
        deadline: Instant,
    },
}

#[derive(Debug)]
pub(crate) enum TiingoRestartOutcome {
    FundNav {
        evidence: PersistedProviderCaptureBindingEvidence,
        nav: AnalyticalFundNavOutput,
    },
    Eod {
        evidence: PersistedProviderCaptureBindingEvidence,
        bars: AnalyticalMarketBarOutput,
    },
}

pub(super) struct TiingoProductActivation {
    lease: ProviderActivationLease,
    source: Arc<TiingoHttpSource>,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
    generation: ResearchProviderRuntimeGeneration,
    durable: Arc<DurableTiingoProviderAuthority>,
    credential_generation: u64,
    entitlement_generation: SourceIdentifier,
}

impl fmt::Debug for TiingoProductActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TiingoProductActivation")
            .field("surface_id", self.lease.surface_id())
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("credential", &"[SECRET-STORE ONLY]")
            .field("source", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl TiingoProductActivation {
    pub(super) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    pub(super) fn matches(
        &self,
        lease: &ProviderActivationLease,
        metadata: &SourceMetadata,
    ) -> bool {
        self.lease.same_authority_as(lease)
            && self.metadata.source_id() == metadata.source_id()
            && self.metadata.revision() == metadata.revision()
    }

    pub(super) fn try_new(
        lease: ProviderActivationLease,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
        generation: ResearchProviderRuntimeGeneration,
        token: TiingoApiToken,
        provider_rate: &market_squawk_sources::ProviderRateAuthority,
    ) -> Result<Arc<Self>, TiingoProductError> {
        if lease.surface_id().as_str() != TIINGO_SURFACE
            || metadata.source_id().as_str() != TIINGO_SOURCE_ID
            || generation.profile().as_str() != TIINGO_SURFACE
            || generation.metadata() != &metadata
        {
            return Err(TiingoProductError::InvalidOperation);
        }
        let credential_generation = lease
            .generation()
            .ok_or(TiingoProductError::InvalidOperation)?;
        let entitlement_generation = SourceIdentifier::try_from(format!(
            "tiingo-entitlement-generation-{}",
            credential_generation.get()
        ))
        .map_err(|_| TiingoProductError::InvalidOperation)?;
        let native_contract_revision = SourceIdentifier::try_from(TIINGO_NATIVE_CONTRACT_REVISION)
            .map_err(|_| TiingoProductError::InvalidOperation)?;
        let initial_observed_at = provider_rate.extension_clock_timestamp()?;
        let windows = TiingoQuotaWindows::try_new(
            initial_observed_at,
            initial_observed_at.checked_add_nanos(HOUR_NANOS)?,
            initial_observed_at.checked_add_nanos(DAY_NANOS)?,
            initial_observed_at.checked_add_nanos(CONSERVATIVE_MONTH_NANOS)?,
        )?;
        let declaration = tiingo_provider_rate_declaration()?;
        let durable = Arc::new(DurableTiingoProviderAuthority::try_new(
            provider_rate.clone(),
            &declaration,
            initial_observed_at,
            windows,
        )?);
        let source = Arc::new(TiingoHttpSource::try_new(
            token,
            provider_rate,
            durable.clone(),
            SourceId::try_from(TIINGO_SOURCE_ID)
                .map_err(|_| TiingoProductError::InvalidOperation)?,
            metadata.revision().clone(),
            native_contract_revision,
            entitlement_generation.clone(),
        )?);
        Ok(Arc::new(Self {
            lease,
            source,
            metadata,
            rights,
            generation,
            durable,
            credential_generation: credential_generation.get(),
            entitlement_generation,
        }))
    }

    async fn status(&self) -> TiingoProductStatus {
        match (
            self.source.schema_circuit_state().await,
            self.source.provider_rate_availability(),
            self.durable.status(),
        ) {
            (Ok(schema_circuit), Ok(shared_budget), Ok(durable)) => TiingoProductStatus {
                availability: if matches!(&schema_circuit, TiingoSchemaCircuitState::Closed)
                    && matches!(shared_budget, ProviderRateAvailability::Available)
                    && !durable.monthly_unique_symbols_exhausted
                    && !durable.monthly_bandwidth_exhausted
                    && durable.monthly_overage_until.is_none()
                {
                    TiingoProductAvailability::Available
                } else {
                    TiingoProductAvailability::Unavailable
                },
                schema_circuit: Some(schema_circuit),
                monthly_quota_is_durable: true,
                history_checkpoints_are_durable: true,
                shared_budget: Some(shared_budget),
                monthly_unique_symbols_exhausted: durable.monthly_unique_symbols_exhausted,
                monthly_bandwidth_exhausted: durable.monthly_bandwidth_exhausted,
                monthly_overage_until: durable.monthly_overage_until,
                monthly_resets_at: Some(durable.monthly_resets_at),
                last_provider_rate_limit_observed_at: durable.last_provider_rate_limit_observed_at,
                provider_retry_after_was_present: durable.provider_retry_after_was_present,
            },
            _ => TiingoProductStatus::unavailable(),
        }
    }

    async fn execute(
        &self,
        research: &ProductionResearchIngestCoordinator,
        operation: TiingoLatestOperation,
        publication: ResearchProviderPublicationOperation,
        deadline: Timestamp,
        seal_deadline: Instant,
    ) -> Result<TiingoLatestOperationOutcome, TiingoProductError> {
        match operation {
            TiingoLatestOperation::FundNav {
                ticker,
                metadata_event_id,
                latest_event_id,
                connection_id,
                context,
                contract,
                extraction_request,
                analytical_dataset,
            } => {
                if context.ticker() != &ticker
                    || contract.source_id() != self.metadata.source_id()
                    || contract.source_contract_revision() != self.metadata.revision()
                    || contract.entitlement_generation().get() != self.credential_generation
                    || contract.entitlement_generation_identity() != &self.entitlement_generation
                {
                    return Err(TiingoProductError::InvalidOperation);
                }
                publication
                    .validate_precommit()
                    .map_err(|_| TiingoProductError::Unavailable)?;
                let cancellation = publication.cancellation().clone();
                let metadata = self
                    .source
                    .fetch_metadata(ticker.clone(), deadline, &cancellation)
                    .await?;
                let latest = self
                    .source
                    .fetch_latest(ticker, deadline, &cancellation)
                    .await?;
                let observed_at = latest.decoded().evidence().received_at();
                let ingested_at = latest.decoded().evidence().decoded_at();
                let (pending, seal_request) = prepare_latest_publication(
                    metadata,
                    latest,
                    metadata_event_id,
                    latest_event_id,
                    connection_id,
                )?;
                research
                    .publish_tiingo_fund_nav(
                        publication.source().clone(),
                        publication.rights().clone(),
                        publication.source_registered_at(),
                        pending,
                        seal_request,
                        context,
                        contract,
                        extraction_request,
                        analytical_dataset,
                        observed_at,
                        ingested_at,
                        publication.precommit_authority(),
                        cancellation,
                        seal_deadline,
                    )
                    .await
            }
            TiingoLatestOperation::Eod {
                ticker,
                metadata_event_id,
                latest_event_id,
                connection_id,
                instrument,
                contract,
                bar_time_authority,
                extraction_request,
                analytical_dataset,
            } => {
                if instrument.ticker() != &ticker
                    || contract.source_id() != self.metadata.source_id()
                    || contract.source_contract_revision() != self.metadata.revision()
                    || contract.entitlement_generation().get() != self.credential_generation
                    || contract.entitlement_generation_identity() != &self.entitlement_generation
                {
                    return Err(TiingoProductError::InvalidOperation);
                }
                publication
                    .validate_precommit()
                    .map_err(|_| TiingoProductError::Unavailable)?;
                let cancellation = publication.cancellation().clone();
                let metadata = self
                    .source
                    .fetch_metadata(ticker.clone(), deadline, &cancellation)
                    .await?;
                let latest = self
                    .source
                    .fetch_latest(ticker, deadline, &cancellation)
                    .await?;
                let observed_at = latest.decoded().evidence().received_at();
                let ingested_at = latest.decoded().evidence().decoded_at();
                let (pending, seal_request) = prepare_latest_publication(
                    metadata,
                    latest,
                    metadata_event_id,
                    latest_event_id,
                    connection_id,
                )?;
                research
                    .publish_tiingo_eod(
                        publication.source().clone(),
                        publication.rights().clone(),
                        publication.source_registered_at(),
                        pending,
                        seal_request,
                        instrument,
                        contract,
                        bar_time_authority,
                        extraction_request,
                        analytical_dataset,
                        observed_at,
                        ingested_at,
                        publication.precommit_authority(),
                        cancellation,
                        seal_deadline,
                    )
                    .await
            }
        }
    }
}

impl ProviderAdapterActivation {
    pub(crate) async fn tiingo_status(&self) -> TiingoProductStatus {
        let activation = self
            .tiingo
            .read()
            .ok()
            .and_then(|slot| slot.as_ref().cloned());
        let Some(activation) = activation else {
            return TiingoProductStatus::setup_required();
        };
        match self
            .onboarding
            .activation_lease(activation.lease.session_id())
        {
            Ok(current)
                if current.same_authority_as(&activation.lease)
                    && matches!(
                        self.research
                            .provider_runtime_generation(activation.generation().profile()),
                        Ok(Some(generation)) if generation == *activation.generation()
                    ) =>
            {
                activation.status().await
            }
            Ok(_) | Err(ProviderOnboardingError::ActivationUnavailable) => {
                TiingoProductStatus::unavailable()
            }
            Err(_) => TiingoProductStatus::unavailable(),
        }
    }

    pub(crate) async fn execute_tiingo_latest(
        &self,
        operation: TiingoLatestOperation,
        deadline: Timestamp,
        seal_deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TiingoLatestOperationOutcome, TiingoProductError> {
        let activation = self
            .tiingo
            .read()
            .map_err(|_| TiingoProductError::Unavailable)?
            .as_ref()
            .cloned()
            .ok_or(TiingoProductError::SetupRequired)?;
        let onboarding = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding.require_active(&activation.lease)?;
        let publication = self
            .research
            .acquire_provider_publication_operation(
                activation.generation(),
                cancellation,
                seal_deadline,
            )
            .await
            .map_err(|_| TiingoProductError::Unavailable)?;
        // The exact generation publication lease now owns currentness through commit. Releasing
        // the broader mutation fence lets unlink or rotation revoke it, cancel the request, and
        // wait for the retained precommit guard to drain.
        drop(onboarding);
        activation
            .execute(
                self.research.as_ref(),
                operation,
                publication,
                deadline,
                seal_deadline,
            )
            .await
    }

    /// Reopens one exact Tiingo NAV/EOD generation without reading a credential or acquiring
    /// provider quota.
    pub(crate) async fn reopen_tiingo_publication(
        &self,
        coordinates: TiingoRestartCoordinates,
        request: TiingoRestartRequest,
        cancellation: CancellationToken,
    ) -> Result<TiingoRestartOutcome, TiingoProductError> {
        self.research
            .reopen_tiingo_publication(coordinates, request, cancellation)
            .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TiingoProductError {
    #[error("Tiingo setup is required")]
    SetupRequired,
    #[error("Tiingo is unavailable")]
    Unavailable,
    #[error("Tiingo NAV/EOD operation is invalid")]
    InvalidOperation,
    #[error("Tiingo adapter rejected the bounded operation")]
    Adapter(#[from] TiingoAdapterError),
    #[error("Tiingo quota windows are invalid")]
    Quota(#[from] TiingoQuotaError),
    #[error("Tiingo source or durable quota authority failed closed")]
    Source(#[from] TiingoHttpSourceError),
    #[error("Tiingo latest publication graph is invalid")]
    Latest(#[from] TiingoLatestPublicationError),
    #[error("Tiingo application sealing or publication failed closed")]
    Application,
    #[error("Tiingo activation clock cannot be represented")]
    Clock(#[from] TimeError),
    #[error("Tiingo durable provider authority failed closed")]
    Authority(#[from] market_squawk_adapter_tiingo::TiingoProviderAuthorityError),
    #[error("Tiingo shared provider declaration is invalid")]
    ProviderRate(#[from] market_squawk_sources::BudgetPoolError),
    #[error("Tiingo shared provider-rate clock is unavailable")]
    ProviderRateState(#[from] market_squawk_sources::ProviderRateStoreError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
}
