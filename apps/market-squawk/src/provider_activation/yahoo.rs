//! Production composition for explicit-demand Yahoo experimental enrichment.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use market_squawk_adapter_yahoo::{
    AdapterBounds, AdmissionPolicy, ChartInterval, ChartWindow, CircuitSnapshot, ExplicitDemand,
    ExplicitDemandPurpose, LookupKind, MAX_YAHOO_DURABLE_CACHE_BODY_BYTES, PINNED_YFINANCE_COMMIT,
    PINNED_YFINANCE_VERSION, YahooAdapterError, YahooDurableStateError, YahooDurableStateStore,
    YahooExecutionDisposition, YahooExecutionLimits, YahooHistoricalPublicationRequest,
    YahooHttpFailure, YahooHttpFailureKind, YahooHttpSession, YahooHttpSessionConfig, YahooLocale,
    YahooOptionPublicationRequest, YahooParsedResponse, YahooPublicationBinding,
    YahooPublicationBridgeError, YahooQuotePublicationRequest, YahooRequestPlan,
    YahooRequestPlanner, YahooTarget,
};
use market_squawk_data::{
    AnalyticalMarketBarOutput, AnalyticalMarketBarReadRequest, DatasetId, DatasetManifestRef,
    PersistedProviderCaptureBindingEvidence, PersistedProviderOptionMarketBindingEvidence,
    PersistedProviderPublicationEvidence, ProviderMarketEventArrowBatch,
    ProviderOptionMarketArrowBatch, QueryLimits,
};
use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_sources::OptionMarketBatchKind;
use market_squawk_sources::SourceMetadata;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::ProviderAdapterActivation;
use crate::application::{
    ProductionResearchIngestCoordinator, ResearchProviderPublicationOperation,
    ResearchProviderRuntimeGeneration, ResearchRightsAuthority,
};
use crate::{ProviderActivationLease, ProviderOnboardingError};

pub(crate) const YAHOO_ENRICHMENT_OPERATION: &str = "Research.GetYahooEnrichment";
pub(super) const YAHOO_SURFACE: &str = "yahoo-finance.experimental-enrichment";
const YAHOO_AUTHORITY_DIRECTORY: &str = "provider-yahoo-enrichment-v1";

// Product safety bounds, not claimed Yahoo quotas. The adaptive admission authority records actual
// attempts and exact provider recovery instructions independently of these ceilings.
const MAX_SYMBOLS_PER_OPERATION: usize = 16;
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_RECORDS_PER_RESPONSE: usize = 20_000;
const MAX_OPTION_CONTRACTS: usize = 10_000;
const MAX_OPTION_EXPIRATIONS: usize = 256;
const MAX_FUND_HOLDINGS: usize = 1_024;
const MAX_STRING_BYTES: usize = 4_096;
const MAX_CRUMB_BYTES: usize = 4_096;
const MAX_CACHE_ENTRIES: usize = 32;
const MAX_CACHE_BYTES: usize = MAX_YAHOO_DURABLE_CACHE_BODY_BYTES;
const MAX_REDIRECTS: usize = 5;
const MAX_ATTEMPT_RECEIPTS: usize = 32;
const FALLBACK_CIRCUIT_COOLDOWN_MS: u64 = 5 * 60 * 1_000;
const FALLBACK_CIRCUIT_MAX_JITTER_MS: u64 = 60 * 1_000;
const REPEATED_FAILURE_THRESHOLD: u32 = 3;

/// Closed application availability for the optional Yahoo enrichment lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum YahooProductAvailability {
    SetupRequired,
    Available,
    Unavailable,
}

/// Sanitized adaptive status. Provider request keys, URLs, clients, and state paths never escape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct YahooEnrichmentStatus {
    pub(crate) availability: YahooProductAvailability,
    pub(crate) pinned_client_version: &'static str,
    pub(crate) pinned_client_commit: &'static str,
    pub(crate) actual_http_attempts: u64,
    pub(crate) cache_hits: u64,
    pub(crate) coalesced_callers: u64,
    pub(crate) observed_429s: u64,
    pub(crate) circuit: Option<CircuitSnapshot>,
}

impl YahooEnrichmentStatus {
    fn setup_required() -> Self {
        Self {
            availability: YahooProductAvailability::SetupRequired,
            pinned_client_version: PINNED_YFINANCE_VERSION,
            pinned_client_commit: PINNED_YFINANCE_COMMIT,
            actual_http_attempts: 0,
            cache_hits: 0,
            coalesced_callers: 0,
            observed_429s: 0,
            circuit: None,
        }
    }

    fn unavailable() -> Self {
        Self {
            availability: YahooProductAvailability::Unavailable,
            ..Self::setup_required()
        }
    }
}

/// Semantic explicit-demand operation. No URL, request plan, session, or retry authority is
/// caller-selectable.
#[derive(Debug)]
pub(crate) enum YahooExplicitOperation {
    Quotes {
        operation_id: String,
        requested_at: Timestamp,
        targets: Vec<YahooTarget>,
        event_id: Uuid,
        connection_id: Uuid,
        canonical: YahooQuotePublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
    },
    History {
        operation_id: String,
        requested_at: Timestamp,
        target: YahooTarget,
        window: ChartWindow,
        interval: ChartInterval,
        include_pre_post: bool,
        event_id: Uuid,
        connection_id: Uuid,
        canonical: YahooHistoricalPublicationRequest,
        analytical_dataset: DatasetId,
    },
    Options {
        operation_id: String,
        requested_at: Timestamp,
        target: YahooTarget,
        expiration_unix_seconds: Option<i64>,
        event_id: Uuid,
        connection_id: Uuid,
        canonical: YahooOptionPublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
    },
    Reference {
        operation_id: String,
        requested_at: Timestamp,
        target: YahooTarget,
        event_id: Uuid,
        connection_id: Uuid,
    },
    Fund {
        operation_id: String,
        requested_at: Timestamp,
        target: YahooTarget,
        event_id: Uuid,
        connection_id: Uuid,
    },
    Search {
        operation_id: String,
        requested_at: Timestamp,
        text: String,
        requested_results: usize,
        event_id: Uuid,
        connection_id: Uuid,
    },
    Lookup {
        operation_id: String,
        requested_at: Timestamp,
        text: String,
        kind: LookupKind,
        requested_results: usize,
        event_id: Uuid,
        connection_id: Uuid,
    },
}

impl YahooExplicitOperation {
    fn plan_and_publication(
        self,
        planner: &YahooRequestPlanner,
    ) -> Result<YahooPlannedOperation, YahooProductError> {
        let planned = match self {
            Self::Quotes {
                operation_id,
                requested_at,
                targets,
                event_id,
                connection_id,
                canonical,
                analytical_dataset,
                idempotency_key,
            } => YahooPlannedOperation {
                plan: planner.quote(
                    demand(
                        operation_id,
                        requested_at,
                        ExplicitDemandPurpose::ViewedInstrument,
                    )?,
                    targets,
                )?,
                event_id,
                connection_id,
                publication: YahooApplicationPublicationRequest::Quotes {
                    canonical,
                    analytical_dataset,
                    idempotency_key,
                },
            },
            Self::History {
                operation_id,
                requested_at,
                target,
                window,
                interval,
                include_pre_post,
                event_id,
                connection_id,
                canonical,
                analytical_dataset,
            } => YahooPlannedOperation {
                plan: planner.chart_history(
                    demand(
                        operation_id,
                        requested_at,
                        ExplicitDemandPurpose::TargetedHistory,
                    )?,
                    vec![target],
                    window,
                    interval,
                    include_pre_post,
                )?,
                event_id,
                connection_id,
                publication: YahooApplicationPublicationRequest::Historical {
                    canonical,
                    analytical_dataset,
                },
            },
            Self::Options {
                operation_id,
                requested_at,
                target,
                expiration_unix_seconds,
                event_id,
                connection_id,
                canonical,
                analytical_dataset,
                idempotency_key,
            } => YahooPlannedOperation {
                plan: planner.option_chain(
                    demand(
                        operation_id,
                        requested_at,
                        ExplicitDemandPurpose::OptionsEnrichment,
                    )?,
                    target,
                    expiration_unix_seconds,
                )?,
                event_id,
                connection_id,
                publication: YahooApplicationPublicationRequest::Options {
                    canonical,
                    analytical_dataset,
                    idempotency_key,
                },
            },
            Self::Reference {
                operation_id,
                requested_at,
                target,
                event_id,
                connection_id,
            } => YahooPlannedOperation {
                plan: planner.reference(
                    demand(
                        operation_id,
                        requested_at,
                        ExplicitDemandPurpose::IndexOrFundDetail,
                    )?,
                    target,
                )?,
                event_id,
                connection_id,
                publication: YahooApplicationPublicationRequest::ProviderHint,
            },
            Self::Fund {
                operation_id,
                requested_at,
                target,
                event_id,
                connection_id,
            } => YahooPlannedOperation {
                plan: planner.fund(
                    demand(
                        operation_id,
                        requested_at,
                        ExplicitDemandPurpose::IndexOrFundDetail,
                    )?,
                    target,
                )?,
                event_id,
                connection_id,
                publication: YahooApplicationPublicationRequest::ProviderHint,
            },
            Self::Search {
                operation_id,
                requested_at,
                text,
                requested_results,
                event_id,
                connection_id,
            } => YahooPlannedOperation {
                plan: planner.search(
                    demand(
                        operation_id,
                        requested_at,
                        ExplicitDemandPurpose::SearchOrLookup,
                    )?,
                    text,
                    requested_results,
                )?,
                event_id,
                connection_id,
                publication: YahooApplicationPublicationRequest::ProviderHint,
            },
            Self::Lookup {
                operation_id,
                requested_at,
                text,
                kind,
                requested_results,
                event_id,
                connection_id,
            } => YahooPlannedOperation {
                plan: planner.lookup(
                    demand(
                        operation_id,
                        requested_at,
                        ExplicitDemandPurpose::SearchOrLookup,
                    )?,
                    text,
                    kind,
                    requested_results,
                )?,
                event_id,
                connection_id,
                publication: YahooApplicationPublicationRequest::ProviderHint,
            },
        };
        if planned.plan.requests.len() != 1 {
            return Err(YahooProductError::InvalidOperation);
        }
        Ok(planned)
    }
}

struct YahooPlannedOperation {
    plan: YahooRequestPlan,
    event_id: Uuid,
    connection_id: Uuid,
    publication: YahooApplicationPublicationRequest,
}

/// Application publication authority passed only to the Yahoo-specific application leaf.
#[derive(Debug)]
pub(crate) enum YahooApplicationPublicationRequest {
    Historical {
        canonical: YahooHistoricalPublicationRequest,
        analytical_dataset: DatasetId,
    },
    Quotes {
        canonical: YahooQuotePublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
    },
    Options {
        canonical: YahooOptionPublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
    },
    ProviderHint,
}

/// Durable publication summary with no provider transport or storage capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum YahooPublicationSummary {
    Published {
        restart: YahooRestartCoordinates,
        provider_dataset: SourceIdentifier,
        records: usize,
    },
    SealedUnavailable {
        sealed_capture_receipt: EvidenceDigest,
        abstentions: usize,
    },
    ProviderHint {
        sealed_capture_receipt: EvidenceDigest,
    },
}

/// Complete durable coordinates needed to revalidate one exact immutable Yahoo generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum YahooRestartCoordinates {
    Historical {
        manifest: DatasetManifestRef,
        binding_digest: EvidenceDigest,
        source_id: market_squawk_domain::SourceId,
        expected_record_count: usize,
    },
    Quotes {
        manifest: DatasetManifestRef,
        publication_digest: EvidenceDigest,
        source_id: market_squawk_domain::SourceId,
        expected_event_count: usize,
    },
    Options {
        manifest: DatasetManifestRef,
        publication_digest: EvidenceDigest,
        publication_kind: OptionMarketBatchKind,
        source_id: market_squawk_domain::SourceId,
        expected_option_row_count: usize,
    },
}

impl YahooRestartCoordinates {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        match self {
            Self::Historical { manifest, .. }
            | Self::Quotes { manifest, .. }
            | Self::Options { manifest, .. } => manifest,
        }
    }
}

#[derive(Debug)]
pub(crate) enum YahooRestartRequest {
    Historical {
        request: AnalyticalMarketBarReadRequest,
        limits: QueryLimits,
        deadline: Instant,
    },
    Quotes,
    Options,
}

#[derive(Debug)]
pub(crate) enum YahooRestartOutcome {
    Historical {
        evidence: PersistedProviderCaptureBindingEvidence,
        bars: AnalyticalMarketBarOutput,
    },
    Quotes {
        evidence: PersistedProviderPublicationEvidence,
        events: ProviderMarketEventArrowBatch,
    },
    Options {
        evidence: PersistedProviderOptionMarketBindingEvidence,
        batch: ProviderOptionMarketArrowBatch,
    },
}

/// Typed explicit-demand response. It remains supplemental and never carries decision authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct YahooEnrichmentOperationOutcome {
    pub(crate) disposition: YahooExecutionDisposition,
    pub(crate) response: YahooParsedResponse,
    pub(crate) publication: Option<YahooPublicationSummary>,
}

pub(super) struct YahooProductActivation {
    lease: ProviderActivationLease,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
    generation: ResearchProviderRuntimeGeneration,
    planner: YahooRequestPlanner,
    session: YahooHttpSession,
}

impl fmt::Debug for YahooProductActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("YahooProductActivation")
            .field("surface_id", self.lease.surface_id())
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("session", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl YahooProductActivation {
    pub(super) fn matches(
        &self,
        lease: &ProviderActivationLease,
        metadata: &SourceMetadata,
    ) -> bool {
        self.lease.same_authority_as(lease)
            && self.metadata.source_id() == metadata.source_id()
            && self.metadata.revision() == metadata.revision()
    }

    pub(super) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    pub(super) fn try_new(
        lease: ProviderActivationLease,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
        generation: ResearchProviderRuntimeGeneration,
        control_root: &Path,
    ) -> Result<Arc<Self>, YahooProductError> {
        if lease.surface_id().as_str() != YAHOO_SURFACE
            || generation.profile().as_str() != YAHOO_SURFACE
            || generation.metadata() != &metadata
        {
            return Err(YahooProductError::InvalidOperation);
        }
        let bounds = AdapterBounds {
            max_symbols_per_operation: MAX_SYMBOLS_PER_OPERATION,
            max_response_bytes: MAX_RESPONSE_BYTES,
            max_records_per_response: MAX_RECORDS_PER_RESPONSE,
            max_option_contracts: MAX_OPTION_CONTRACTS,
            max_option_expirations: MAX_OPTION_EXPIRATIONS,
            max_fund_holdings: MAX_FUND_HOLDINGS,
            max_string_bytes: MAX_STRING_BYTES,
        };
        let planner =
            YahooRequestPlanner::new(bounds, YahooLocale::new("en", "US", MAX_STRING_BYTES)?)?;
        let config = YahooHttpSessionConfig {
            adapter_bounds: bounds,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(20),
            total_timeout: Duration::from_secs(30),
            max_session_response_bytes: MAX_RESPONSE_BYTES,
            max_crumb_bytes: MAX_CRUMB_BYTES,
            max_cache_entries: MAX_CACHE_ENTRIES,
            max_cache_bytes: MAX_CACHE_BYTES,
            max_redirects: MAX_REDIRECTS,
            max_attempt_receipts: MAX_ATTEMPT_RECEIPTS,
            admission_policy: AdmissionPolicy::new(
                FALLBACK_CIRCUIT_COOLDOWN_MS,
                FALLBACK_CIRCUIT_MAX_JITTER_MS,
                REPEATED_FAILURE_THRESHOLD,
            )?,
        };
        let store = YahooDurableStateStore::try_open(control_root.join(YAHOO_AUTHORITY_DIRECTORY))?;
        let session = YahooHttpSession::new_with_durable_state(config, store)?;
        Ok(Arc::new(Self {
            lease,
            metadata,
            rights,
            generation,
            planner,
            session,
        }))
    }

    fn status(&self) -> YahooEnrichmentStatus {
        let Ok(snapshot) = self.session.admission().snapshot() else {
            return YahooEnrichmentStatus::unavailable();
        };
        let availability = if matches!(snapshot.circuit, CircuitSnapshot::Open { .. }) {
            YahooProductAvailability::Unavailable
        } else {
            YahooProductAvailability::Available
        };
        YahooEnrichmentStatus {
            availability,
            pinned_client_version: PINNED_YFINANCE_VERSION,
            pinned_client_commit: PINNED_YFINANCE_COMMIT,
            actual_http_attempts: snapshot.actual_http_attempts_total,
            cache_hits: snapshot.cache_hits_total,
            coalesced_callers: snapshot.coalesced_callers_total,
            observed_429s: snapshot.http_429_total,
            circuit: Some(snapshot.circuit),
        }
    }

    async fn execute(
        &self,
        research: &ProductionResearchIngestCoordinator,
        operation: YahooExplicitOperation,
        publication: ResearchProviderPublicationOperation,
        maximum_cache_age: Duration,
        deadline: Instant,
    ) -> Result<YahooEnrichmentOperationOutcome, YahooProductError> {
        if publication.generation() != &self.generation {
            return Err(YahooProductError::Unavailable);
        }
        publication
            .validate_precommit()
            .map_err(|_| YahooProductError::Unavailable)?;
        let cancellation = publication.cancellation().clone();
        let planned = operation.plan_and_publication(&self.planner)?;
        let mut results = self
            .session
            .execute_plan(
                planned.plan,
                YahooExecutionLimits {
                    deadline,
                    maximum_cache_age,
                },
                &cancellation,
            )
            .await?;
        let result = results.pop().ok_or(YahooProductError::InvalidOperation)?;
        if !results.is_empty() {
            return Err(YahooProductError::InvalidOperation);
        }
        let disposition = result.disposition();
        let response = result.parsed_response().clone();
        let publication_summary = if disposition == YahooExecutionDisposition::Network {
            let binding = YahooPublicationBinding::try_new(
                self.metadata.source_id().clone(),
                self.metadata.revision().clone(),
                planned.event_id,
                planned.connection_id,
            )?;
            let pending = result.into_pending_publication(binding)?;
            Some(
                research
                    .publish_yahoo_enrichment(
                        publication.source().clone(),
                        publication.rights().clone(),
                        publication.source_registered_at(),
                        pending,
                        planned.publication,
                        publication.precommit_authority(),
                        cancellation,
                        deadline,
                    )
                    .await?,
            )
        } else {
            None
        };
        Ok(YahooEnrichmentOperationOutcome {
            disposition,
            response,
            publication: publication_summary,
        })
    }
}

impl ProviderAdapterActivation {
    /// Returns exact setup/availability and adaptive evidence without provider request or path
    /// authority.
    pub(crate) fn yahoo_enrichment_status(&self) -> YahooEnrichmentStatus {
        let activation = self
            .yahoo
            .read()
            .ok()
            .and_then(|slot| slot.as_ref().cloned());
        let Some(activation) = activation else {
            return YahooEnrichmentStatus::setup_required();
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
                activation.status()
            }
            Ok(_) | Err(ProviderOnboardingError::ActivationUnavailable) => {
                YahooEnrichmentStatus::unavailable()
            }
            Err(_) => YahooEnrichmentStatus::unavailable(),
        }
    }

    /// Executes one explicit request through the sole adaptive lane and application publisher.
    pub(crate) async fn execute_yahoo_enrichment(
        &self,
        operation: YahooExplicitOperation,
        maximum_cache_age: Duration,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<YahooEnrichmentOperationOutcome, YahooProductError> {
        let activation = self
            .yahoo
            .read()
            .map_err(|_| YahooProductError::Unavailable)?
            .as_ref()
            .cloned()
            .ok_or(YahooProductError::SetupRequired)?;
        let onboarding = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding.require_active(&activation.lease)?;
        let publication = self
            .research
            .acquire_provider_publication_operation(activation.generation(), cancellation, deadline)
            .await
            .map_err(|_| YahooProductError::Unavailable)?;
        // The exact generation publication lease now owns currentness through commit. Releasing
        // the broader mutation fence lets unlink or rotation revoke it, cancel the request, and
        // wait for the retained precommit guard to drain.
        drop(onboarding);
        activation
            .execute(
                self.research.as_ref(),
                operation,
                publication,
                maximum_cache_age,
                deadline,
            )
            .await
    }

    /// Reopens one exact durable Yahoo generation without restoring a provider session.
    pub(crate) async fn reopen_yahoo_publication(
        &self,
        coordinates: YahooRestartCoordinates,
        request: YahooRestartRequest,
        cancellation: CancellationToken,
    ) -> Result<YahooRestartOutcome, YahooProductError> {
        self.research
            .reopen_yahoo_publication(coordinates, request, cancellation)
            .await
    }
}

fn demand(
    operation_id: String,
    requested_at: Timestamp,
    purpose: ExplicitDemandPurpose,
) -> Result<ExplicitDemand, YahooProductError> {
    let requested_at_unix_ms = requested_at.unix_nanos() / 1_000_000;
    ExplicitDemand::new(
        operation_id,
        requested_at_unix_ms,
        purpose,
        MAX_STRING_BYTES,
    )
    .map_err(Into::into)
}

/// Closed Yahoo composition or operation failure.
#[derive(Debug, thiserror::Error)]
pub enum YahooProductError {
    #[error("Yahoo enrichment setup is required")]
    SetupRequired,
    #[error("Yahoo enrichment is unavailable")]
    Unavailable,
    #[error("Yahoo explicit operation is invalid")]
    InvalidOperation,
    #[error("Yahoo adapter rejected the bounded semantic request")]
    Adapter(#[from] YahooAdapterError),
    #[error("Yahoo durable adaptive state is unavailable")]
    Durable(#[from] YahooDurableStateError),
    #[error("Yahoo adaptive HTTP lane refused or failed the request")]
    Http(#[from] YahooHttpFailure),
    #[error("Yahoo adaptive HTTP lane configuration is invalid")]
    HttpConfiguration(#[from] YahooHttpFailureKind),
    #[error("Yahoo raw publication binding is invalid")]
    Publication(#[from] YahooPublicationBridgeError),
    #[error("Yahoo application sealing or publication failed closed")]
    Application,
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
}
