//! Application-owned EIA acquisition, raw sealing, publication, and exact PIT restart.
//!
//! The EIA adapter owns authenticated request construction, provider-schema validation, offset
//! pagination, and canonical Macro normalization. This leaf owns the application transitions that
//! the adapter deliberately cannot own: every provider page is sealed through the sole research
//! journal before continuation, the sealed native/canonical handoff is published through the
//! shared immutable research service, and later reads remain pinned to the exact manifest and raw
//! binding. No credential, rate counter, raw store, or publication authority is duplicated here.

use std::{
    collections::BTreeMap,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_eia::{
    EIA_MAX_CANONICAL_PUBLICATION_OBSERVATIONS, EiaActivatedProvider, EiaDataPageTransition,
    EiaDataTransportReceipt, EiaError, EiaLifecycleError, EiaNativePublishedSeriesPrecision,
    EiaPublicationCandidate, decode_eia_native_published_series_coordinate,
    eia_data_dataset_identifier,
};
use market_squawk_data::{
    AnalyticalMacroLatestKnownOutput, AnalyticalMacroLatestKnownRequest,
    AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadError, CommittedDataset, DatasetId,
    DatasetManifestRef, PersistedProviderCaptureBindingEvidence, QueryLimits,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    ResearchPeriod, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch,
    ExtractionRequest, ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, ProviderCaptureError,
    ProviderNativeLineageImplementation, SourceError, SourceMetadata, SourceMetadataProvider,
    SourceObject,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    ProviderMacroOperationAuthority, ProviderMacroPublicationError, ProviderMacroRestartBinding,
    ResearchProviderRuntimeGeneration, ResearchRevisionPlanError,
};
use crate::{ResearchService, ResearchServiceError};

/// Fixed typed operation for calendar-date EIA energy/macro PIT reads.
pub(crate) const EIA_MACRO_CALENDAR_POINT_IN_TIME_OPERATION: &str =
    "Macro.GetEiaCalendarPointInTime";

/// Fixed typed operation for provider-period EIA energy/macro PIT reads.
pub(crate) const EIA_MACRO_PROVIDER_PERIOD_POINT_IN_TIME_OPERATION: &str =
    "Macro.GetEiaProviderPeriodPointInTime";

/// Application ceilings independent of the adapter's stricter route and transport bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EiaApplicationAcquisitionLimits {
    max_pages: NonZeroU16,
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
}

impl EiaApplicationAcquisitionLimits {
    /// Constructs explicit page, canonical-record, and simultaneous retained-byte ceilings.
    pub(crate) fn try_new(
        max_pages: NonZeroU16,
        max_records: NonZeroU32,
        max_bytes: NonZeroU64,
    ) -> Result<Self, EiaMacroApplicationError> {
        if usize::try_from(max_records.get()).map_or(true, |records| {
            records > EIA_MAX_CANONICAL_PUBLICATION_OBSERVATIONS
        }) || max_bytes.get() > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
        {
            return Err(EiaMacroApplicationError::InvalidLimits);
        }
        Ok(Self {
            max_pages,
            max_records,
            max_bytes,
        })
    }

    /// Returns the maximum number of provider pages the application will admit.
    pub(crate) const fn max_pages(self) -> u16 {
        self.max_pages.get()
    }

    /// Returns the maximum number of canonical observations admitted to one generation.
    pub(crate) const fn max_records(self) -> u32 {
        self.max_records.get()
    }

    /// Returns the maximum simultaneous retained bytes admitted to extraction publication.
    pub(crate) const fn max_bytes(self) -> u64 {
        self.max_bytes.get()
    }
}

/// EIA source, rights, raw-store, and immutable research composition.
pub(crate) struct EiaMacroApplicationClosure {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    provider: Arc<EiaActivatedProvider>,
    provider_dataset: SourceIdentifier,
    generation: ResearchProviderRuntimeGeneration,
}

/// Same-instance activated EIA registration and typed publication composition.
pub(crate) struct EiaLiveComposition {
    registered_source: EiaRegisteredSource,
    closure: EiaMacroApplicationClosure,
}

impl EiaLiveComposition {
    pub(crate) fn try_new(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        provider: EiaActivatedProvider,
        generation: ResearchProviderRuntimeGeneration,
    ) -> Result<Self, EiaMacroApplicationError> {
        if provider.source_metadata() != generation.metadata()
            || !generation
                .metadata()
                .is_effective_at(generation.authority_effective_at())
        {
            return Err(EiaMacroApplicationError::AuthorityInvalid);
        }
        let provider_dataset =
            eia_data_dataset_identifier(provider.contract()).map_err(EiaLifecycleError::from)?;
        let provider = Arc::new(provider);
        Ok(Self {
            registered_source: EiaRegisteredSource {
                provider: Arc::clone(&provider),
                provider_dataset: provider_dataset.clone(),
            },
            closure: EiaMacroApplicationClosure {
                coordinator,
                provider,
                provider_dataset,
                generation,
            },
        })
    }

    pub(crate) fn into_parts(self) -> (EiaRegisteredSource, EiaMacroApplicationClosure) {
        (self.registered_source, self.closure)
    }
}

/// Registry-facing activation wrapper. Typed EIA pagination remains in the paired closure.
pub(crate) struct EiaRegisteredSource {
    provider: Arc<EiaActivatedProvider>,
    provider_dataset: SourceIdentifier,
}

impl SourceMetadataProvider for EiaRegisteredSource {
    fn metadata(&self) -> &SourceMetadata {
        self.provider.source_metadata()
    }
}

impl ExtractionSource for EiaRegisteredSource {
    fn discover(
        &self,
        _authority: ExtractionAuthority,
        _request: DiscoveryRequest,
        _cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }

    fn extract(
        &self,
        _authority: ExtractionAuthority,
        _request: ExtractionRequest,
        _cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }
}

impl ManagedResearchExtractionSource for EiaRegisteredSource {
    fn discovery_dataset_identifier(&self) -> Option<&SourceIdentifier> {
        Some(&self.provider_dataset)
    }

    fn rights_subject(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ResearchRevisionPlanError> {
        if dataset != &self.provider_dataset {
            return Err(ResearchRevisionPlanError);
        }
        Ok(Some(
            self.provider
                .doctor_report()
                .authorization_subject()
                .clone(),
        ))
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        if batch.request().object().source_id() != self.provider.source_metadata().source_id()
            || batch.request().object().dataset() != &self.provider_dataset
        {
            return Err(ResearchRevisionPlanError);
        }
        ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

impl std::fmt::Debug for EiaMacroApplicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EiaMacroApplicationClosure")
            .field("profile", self.generation.profile())
            .field("source_id", self.generation.metadata().source_id())
            .field("metadata_revision", self.generation.metadata().revision())
            .finish_non_exhaustive()
    }
}

impl EiaMacroApplicationClosure {
    /// Acquires every exact page, seals it before continuation, and publishes one generation.
    ///
    /// The coordinator mints the current registry extraction authority for the paired activation.
    /// EIA's transport consumes that authority for its shared provider-rate permits before every
    /// authenticated send. A partial acquisition can leave only sealed raw pages; it can never
    /// publish a partial canonical generation.
    pub(crate) async fn acquire_seal_publish(
        &self,
        analytical_dataset: DatasetId,
        limits: EiaApplicationAcquisitionLimits,
        context: &RequestContext,
    ) -> Result<EiaMacroPublicationReceipt, EiaMacroApplicationError> {
        let operation = self
            .coordinator
            .acquire_provider_macro_operation(&self.generation, &self.provider_dataset, context)
            .await?;
        let provider = self.provider.as_ref();
        self.validate_current(provider, &operation)?;
        let authority = operation.extraction();
        let provider_deadline = operation.provider_deadline()?;
        let cancellation = operation.cancellation().clone();
        let operation_deadline = operation.operation_deadline();

        let max_publication_bytes = usize::try_from(limits.max_bytes())
            .map_err(|_| EiaMacroApplicationError::InvalidLimits)?;
        let mut cursor = provider.begin_bounded_retrieval(
            &authority,
            provider_deadline,
            limits.max_pages(),
            limits.max_records(),
            max_publication_bytes,
        )?;
        let mut admitted_observations = 0_u64;
        let mut admitted_retained_bytes = 0_u64;
        let retrieval = loop {
            if cancellation.is_cancelled() {
                return Err(EiaMacroApplicationError::Cancelled);
            }
            if Instant::now() >= operation_deadline {
                return Err(EiaMacroApplicationError::DeadlineExceeded);
            }
            if cursor.next_ordinal() >= limits.max_pages() {
                return Err(EiaMacroApplicationError::AcquisitionLimitExceeded);
            }

            let fetch = provider.fetch_next_retrieval_page(
                &authority,
                cursor,
                provider_deadline,
                cancellation.clone(),
            );
            tokio::pin!(fetch);
            let pending = tokio::select! {
                biased;
                result = fetch.as_mut() => result?,
                () = cancellation.cancelled() => {
                    return Err(EiaMacroApplicationError::Cancelled);
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(operation_deadline)) => {
                    cancellation.cancel();
                    return Err(EiaMacroApplicationError::DeadlineExceeded);
                }
            };
            let page_observations = pending.page_material().data_receipt().observation_count();
            let raw_retained_bytes = pending
                .page_material()
                .raw_page()
                .http_receipt()
                .retained_bytes();
            let canonical_retained_bytes = u64::try_from(
                pending
                    .page_material()
                    .data_receipt()
                    .publication_retained_bytes(),
            )
            .map_err(|_error| EiaMacroApplicationError::AcquisitionLimitExceeded)?;
            let page_retained_bytes = raw_retained_bytes
                .checked_add(canonical_retained_bytes)
                .ok_or(EiaMacroApplicationError::AcquisitionLimitExceeded)?;
            let projected_observations = admitted_observations
                .checked_add(page_observations)
                .ok_or(EiaMacroApplicationError::AcquisitionLimitExceeded)?;
            let projected_retained_bytes = admitted_retained_bytes
                .checked_add(page_retained_bytes)
                .ok_or(EiaMacroApplicationError::AcquisitionLimitExceeded)?;
            let exceeds_limit = projected_observations > u64::from(limits.max_records())
                || projected_retained_bytes > limits.max_bytes();
            let (rejoin, seal_request) = pending.into_parts();
            // A completed provider response outranks outer cancellation. Preserve its exact raw
            // body before reporting cancellation or permitting any continuation.
            let raw_seal = CancellationToken::new();
            let sealed = self
                .coordinator
                .research
                .seal_provider_capture(seal_request, &raw_seal, operation_deadline)
                .await?;
            if exceeds_limit {
                return Err(EiaMacroApplicationError::AcquisitionLimitExceeded);
            }
            admitted_observations = projected_observations;
            admitted_retained_bytes = projected_retained_bytes;
            if cancellation.is_cancelled() {
                return Err(EiaMacroApplicationError::Cancelled);
            }
            if Instant::now() >= operation_deadline {
                return Err(EiaMacroApplicationError::DeadlineExceeded);
            }
            match provider.rejoin_retrieval_page(
                &authority,
                rejoin,
                sealed,
                provider_deadline,
                &cancellation,
            )? {
                EiaDataPageTransition::More(next) => cursor = next,
                EiaDataPageTransition::Complete(complete) => break complete,
            }
        };

        let transport = retrieval.transport_receipt();
        if retrieval.sealed_page_count() == 0
            || retrieval.sealed_page_count() > usize::from(limits.max_pages())
            || transport.observations() == 0
            || transport.observations() > u64::from(limits.max_records())
            || transport.retained_bytes() > limits.max_bytes()
        {
            return Err(EiaMacroApplicationError::AcquisitionLimitExceeded);
        }

        let candidate = provider.publication_candidate_bounded(
            &authority,
            retrieval.into_publication_rejoin(),
            provider_deadline,
            max_publication_bytes,
            &cancellation,
        )?;
        self.publish_candidate(
            provider,
            candidate,
            transport,
            analytical_dataset,
            limits,
            &operation,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "capture-bound candidate, transport evidence, immutable target, bounds, and commit authority remain explicit"
    )]
    async fn publish_candidate(
        &self,
        provider: &EiaActivatedProvider,
        candidate: EiaPublicationCandidate,
        transport: EiaDataTransportReceipt,
        analytical_dataset: DatasetId,
        limits: EiaApplicationAcquisitionLimits,
        operation: &ProviderMacroOperationAuthority,
    ) -> Result<EiaMacroPublicationReceipt, EiaMacroApplicationError> {
        self.validate_current(provider, operation)?;
        candidate.rejoin().validate(self.generation.metadata())?;
        let normalization_admitted_at = candidate.rejoin().normalization_admitted_at();
        if normalization_admitted_at < self.generation.authority_effective_at()
            || !self
                .generation
                .metadata()
                .is_effective_at(normalization_admitted_at)
        {
            return Err(EiaMacroApplicationError::AuthorityInvalid);
        }
        let provider_dataset = candidate.rejoin().provider_dataset().clone();
        if provider_dataset != self.provider_dataset {
            return Err(EiaMacroApplicationError::AuthorityInvalid);
        }
        let query_digest = evidence_digest(candidate.rejoin().query_digest().bytes());
        let expected_record_count = usize::try_from(candidate.rejoin().canonical_record_count())
            .map_err(|_| EiaMacroApplicationError::InvalidCandidate)?;
        let retained_bytes = u64::try_from(candidate.rejoin().publication_retained_bytes())
            .map_err(|_| EiaMacroApplicationError::InvalidCandidate)?;
        if expected_record_count == 0
            || expected_record_count
                > usize::try_from(limits.max_records())
                    .map_err(|_| EiaMacroApplicationError::InvalidLimits)?
            || retained_bytes > limits.max_bytes()
        {
            return Err(EiaMacroApplicationError::AcquisitionLimitExceeded);
        }
        let series_coordinates = published_series_coordinates(&candidate)?;
        let extraction_request = extraction_request(&candidate, limits)?;
        let shared = candidate.try_into_shared_publication(extraction_request)?;
        shared
            .policy_evidence()
            .validate(self.generation.metadata())?;
        let (policy, revisions, binding) = shared.into_parts();
        policy.validate(self.generation.metadata())?;
        binding.validate()?;
        if binding.capture_evidence().source_id() != self.generation.metadata().source_id()
            || binding.capture_evidence().metadata_revision()
                != self.generation.metadata().revision()
            || binding.capture_evidence().dataset() != &provider_dataset
            || binding.capture_evidence().request_set_identity() != query_digest
            || binding.batch().records().len() != expected_record_count
            || binding.native_lineage().schema().implementation()
                != ProviderNativeLineageImplementation::EiaSeriesV1
            || revisions.len() != expected_record_count
            || !revisions.is_locally_observed()
            || !revisions.native_lineage_required()
        {
            return Err(EiaMacroApplicationError::InvalidCandidate);
        }
        let native_series_coordinates = decoded_series_coordinates(
            binding
                .native_lineage()
                .rows()
                .iter()
                .map(|row| row.semantic_payload().as_ref()),
        )
        .map_err(|_error| EiaMacroApplicationError::InvalidCandidate)?;
        if native_series_coordinates != series_coordinates {
            return Err(EiaMacroApplicationError::InvalidCandidate);
        }

        let publication = operation
            .publish_single_binding(
                analytical_dataset,
                binding,
                revisions,
                ProviderNativeLineageImplementation::EiaSeriesV1,
                normalization_admitted_at,
            )
            .await?;
        let (committed, binding) = publication.into_parts();
        let restart = EiaMacroRestartSelector::from_published_binding(binding, series_coordinates);
        Ok(EiaMacroPublicationReceipt {
            committed,
            restart,
            transport,
            normalization_admitted_at,
        })
    }

    fn validate_current(
        &self,
        provider: &EiaActivatedProvider,
        operation: &ProviderMacroOperationAuthority,
    ) -> Result<(), EiaMacroApplicationError> {
        if provider.source_metadata() != self.generation.metadata()
            || operation.generation() != &self.generation
            || !self
                .generation
                .metadata()
                .is_effective_at(self.generation.authority_effective_at())
        {
            return Err(EiaMacroApplicationError::AuthorityInvalid);
        }
        operation.ensure_live()?;
        Ok(())
    }
}

fn extraction_request(
    candidate: &EiaPublicationCandidate,
    limits: EiaApplicationAcquisitionLimits,
) -> Result<ExtractionRequest, EiaMacroApplicationError> {
    let rejoin = candidate.rejoin();
    let discovery = DiscoveryRequest::try_new(
        rejoin.provider_dataset().clone(),
        None,
        NonZeroU16::MIN,
        rejoin.normalization_admitted_at(),
    )
    .map_err(|_| EiaMacroApplicationError::InvalidCandidate)?;
    let source_object = SourceObject::try_new_with_availability(
        rejoin.source_metadata().source_id().clone(),
        rejoin.source_metadata().revision().clone(),
        &discovery,
        rejoin.source_object_id()?,
        SourceIdentifier::try_from("application/json")
            .map_err(|_| EiaMacroApplicationError::InvalidCandidate)?,
        ExactPayloadEvidence::from_content_digest(rejoin.capture_content_digest()),
        EffectiveInterval::new(rejoin.acquisition_receipt().first_received_at(), None)
            .map_err(|_| EiaMacroApplicationError::InvalidCandidate)?,
        None,
        AvailabilityEvidence::LocalFirstObserved {
            observed_at: rejoin.acquisition_receipt().last_received_at(),
        },
        Some(rejoin.capture_receipt().total_body_bytes()),
    )
    .map_err(|_| EiaMacroApplicationError::InvalidCandidate)?;
    ExtractionRequest::try_new(
        source_object,
        limits.max_records,
        limits.max_bytes,
        rejoin.normalization_admitted_at(),
    )
    .map_err(|_| EiaMacroApplicationError::InvalidCandidate)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EiaPublishedSeriesCoordinate {
    canonical_series: SourceIdentifier,
    precision: EiaPublishedSeriesPrecision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EiaPublishedSeriesPrecision {
    CalendarDate,
    SourcePeriod { scheme: SourceIdentifier },
}

fn published_series_coordinates(
    candidate: &EiaPublicationCandidate,
) -> Result<Box<[EiaPublishedSeriesCoordinate]>, EiaMacroApplicationError> {
    let mut coordinates = BTreeMap::new();
    for observation in candidate.observations() {
        let macro_observation = observation.observation();
        let effective = macro_observation.context().time().effective();
        let precision = if effective.calendar_date_value().is_some() {
            EiaPublishedSeriesPrecision::CalendarDate
        } else if let Some(period) = effective.source_period_value() {
            EiaPublishedSeriesPrecision::SourcePeriod {
                scheme: period.scheme().clone(),
            }
        } else {
            return Err(EiaMacroApplicationError::InvalidCandidate);
        };
        match coordinates.entry(macro_observation.series().clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(precision);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &precision => {
                return Err(EiaMacroApplicationError::InvalidCandidate);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    Ok(coordinates
        .into_iter()
        .map(
            |(canonical_series, precision)| EiaPublishedSeriesCoordinate {
                canonical_series,
                precision,
            },
        )
        .collect::<Vec<_>>()
        .into_boxed_slice())
}

/// Reconstructs the selectable series/time-precision index from exact persisted native rows.
fn persisted_series_coordinates(
    evidence: &PersistedProviderCaptureBindingEvidence,
) -> Result<Box<[EiaPublishedSeriesCoordinate]>, EiaMacroApplicationError> {
    decoded_series_coordinates(
        evidence
            .rows()
            .iter()
            .map(|row| row.native_semantic_payload()),
    )
}

fn decoded_series_coordinates<'a>(
    payloads: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Box<[EiaPublishedSeriesCoordinate]>, EiaMacroApplicationError> {
    let mut coordinates = BTreeMap::new();
    for payload in payloads {
        let decoded = decode_eia_native_published_series_coordinate(payload)
            .map_err(|_error| EiaMacroApplicationError::RestartInvalid)?;
        let canonical_series = decoded.canonical_series().clone();
        let precision = match decoded.precision() {
            EiaNativePublishedSeriesPrecision::CalendarDate => {
                EiaPublishedSeriesPrecision::CalendarDate
            }
            EiaNativePublishedSeriesPrecision::SourcePeriod { scheme } => {
                EiaPublishedSeriesPrecision::SourcePeriod {
                    scheme: scheme.clone(),
                }
            }
        };
        match coordinates.entry(canonical_series) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(precision);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &precision => {
                return Err(EiaMacroApplicationError::RestartInvalid);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    if coordinates.is_empty() {
        return Err(EiaMacroApplicationError::RestartInvalid);
    }
    Ok(coordinates
        .into_iter()
        .map(
            |(canonical_series, precision)| EiaPublishedSeriesCoordinate {
                canonical_series,
                precision,
            },
        )
        .collect::<Vec<_>>()
        .into_boxed_slice())
}

/// Exact immutable EIA generation plus its sealed raw/native replay coordinates.
#[derive(Debug)]
pub(crate) struct EiaMacroPublicationReceipt {
    committed: CommittedDataset,
    restart: EiaMacroRestartSelector,
    transport: EiaDataTransportReceipt,
    normalization_admitted_at: Timestamp,
}

impl EiaMacroPublicationReceipt {
    /// Returns the immutable Parquet/manifest publication.
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    /// Returns the sole selector accepted for exact PIT/restart reads.
    pub(crate) const fn restart_selector(&self) -> &EiaMacroRestartSelector {
        &self.restart
    }

    /// Returns actual request, returned-row, observation, missing, byte, and latency evidence.
    pub(crate) const fn transport(&self) -> EiaDataTransportReceipt {
        self.transport
    }

    /// Returns the trusted clock at which the sealed chain entered canonical normalization.
    pub(crate) const fn normalization_admitted_at(&self) -> Timestamp {
        self.normalization_admitted_at
    }
}

/// Exact immutable EIA generation reconstructed from catalog binding and native row semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EiaMacroRestartSelector {
    binding: ProviderMacroRestartBinding,
    series_coordinates: Box<[EiaPublishedSeriesCoordinate]>,
}

impl EiaMacroRestartSelector {
    pub(crate) fn try_reopen(
        research: &ResearchService,
        manifest: DatasetManifestRef,
        expected_source: &SourceId,
    ) -> Result<Self, EiaMacroApplicationError> {
        let binding = ProviderMacroRestartBinding::try_reopen(
            research,
            manifest,
            expected_source,
            ProviderNativeLineageImplementation::EiaSeriesV1,
        )?;
        Self::from_binding(research, binding)
    }

    fn from_binding(
        research: &ResearchService,
        binding: ProviderMacroRestartBinding,
    ) -> Result<Self, EiaMacroApplicationError> {
        let evidence = binding.evidence(research)?;
        let series_coordinates = persisted_series_coordinates(&evidence)?;
        Ok(Self {
            binding,
            series_coordinates,
        })
    }

    fn from_published_binding(
        binding: ProviderMacroRestartBinding,
        series_coordinates: Box<[EiaPublishedSeriesCoordinate]>,
    ) -> Self {
        Self {
            binding,
            series_coordinates,
        }
    }

    /// Returns the exact immutable generation.
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.binding.manifest()
    }

    /// Returns the sole source-rights owner of the immutable generation.
    pub(crate) const fn source_id(&self) -> &SourceId {
        self.binding.source_id()
    }

    /// Returns the sole persisted raw/native binding identity.
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest()
    }

    /// Returns the provider query-bound dataset identity.
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        self.binding.provider_dataset()
    }

    /// Returns whether an exact published series uses provider-supplied calendar dates.
    pub(crate) fn is_calendar_date_series(&self, series: &SourceIdentifier) -> bool {
        self.series_coordinate(series).is_some_and(|coordinate| {
            matches!(
                coordinate.precision,
                EiaPublishedSeriesPrecision::CalendarDate
            )
        })
    }

    /// Returns the exact source-period scheme required to build a non-calendar cutoff.
    ///
    /// EIA series retain distinct provider-period schemes; callers must not derive this opaque
    /// identity from ticker-like text or coerce it into a calendar date.
    pub(crate) fn provider_period_scheme(
        &self,
        series: &SourceIdentifier,
    ) -> Option<&SourceIdentifier> {
        match &self.series_coordinate(series)?.precision {
            EiaPublishedSeriesPrecision::SourcePeriod { scheme } => Some(scheme),
            EiaPublishedSeriesPrecision::CalendarDate => None,
        }
    }

    /// Constructs the only calendar/provider-period PIT requests this selector will accept.
    pub(crate) fn try_point_in_time_request(
        &self,
        series: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_cutoff: EiaMacroEffectiveCutoff,
    ) -> Result<EiaMacroPointInTimeRequest, EiaMacroApplicationError> {
        self.validate_selected_series(&series, &effective_cutoff)?;
        match effective_cutoff {
            EiaMacroEffectiveCutoff::CalendarDate(effective_date_cutoff) => {
                let request = AnalyticalMacroLatestKnownRequest::try_new(
                    self.binding.manifest().clone(),
                    self.binding.source_id().clone(),
                    knowledge_cutoff,
                    effective_date_cutoff,
                    series,
                )?;
                Ok(EiaMacroPointInTimeRequest::Calendar(request))
            }
            EiaMacroEffectiveCutoff::ProviderPeriod(effective_period_cutoff) => {
                let source_series = AnalyticalMacroSourceQualifiedSeries::new(
                    self.binding.source_id().clone(),
                    series,
                );
                let request = AnalyticalMacroProviderPeriodLatestKnownRequest::try_new(
                    self.binding.manifest().clone(),
                    source_series,
                    knowledge_cutoff,
                    effective_period_cutoff,
                )?;
                Ok(EiaMacroPointInTimeRequest::ProviderPeriod(request))
            }
        }
    }

    /// Reopens exact raw/native evidence and performs one fixed manifest-pinned PIT selection.
    pub(crate) async fn reopen_point_in_time(
        &self,
        research: &ResearchService,
        request: EiaMacroPointInTimeRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<EiaMacroRestartReceipt, EiaMacroApplicationError> {
        self.validate_request(&request)?;
        let evidence = self.verify_persisted_binding(research)?;
        match request {
            EiaMacroPointInTimeRequest::Calendar(request) => {
                let output = research
                    .analytical_reader()
                    .read_macro_latest_known_snapshot(request, limits, deadline, cancellation)
                    .await?;
                self.validate_calendar_output(&output)?;
                Ok(EiaMacroRestartReceipt::Calendar { evidence, output })
            }
            EiaMacroPointInTimeRequest::ProviderPeriod(request) => {
                let output = research
                    .analytical_reader()
                    .read_macro_provider_period_latest_known_snapshot(
                        request,
                        limits,
                        deadline,
                        cancellation,
                    )
                    .await?;
                self.validate_provider_period_output(&output)?;
                Ok(EiaMacroRestartReceipt::ProviderPeriod { evidence, output })
            }
        }
    }

    fn verify_persisted_binding(
        &self,
        research: &ResearchService,
    ) -> Result<PersistedProviderCaptureBindingEvidence, EiaMacroApplicationError> {
        let evidence = self.binding.evidence(research)?;
        if persisted_series_coordinates(&evidence)? != self.series_coordinates {
            return Err(EiaMacroApplicationError::RestartInvalid);
        }
        Ok(evidence)
    }

    fn validate_selected_series(
        &self,
        selected: &AnalyticalMacroSeriesAllowlist,
        cutoff: &EiaMacroEffectiveCutoff,
    ) -> Result<(), EiaMacroApplicationError> {
        for selected_series in selected.series() {
            let coordinate = self
                .series_coordinate(selected_series)
                .ok_or(EiaMacroApplicationError::SeriesNotPublished)?;
            let valid = match (&coordinate.precision, cutoff) {
                (
                    EiaPublishedSeriesPrecision::CalendarDate,
                    EiaMacroEffectiveCutoff::CalendarDate(_),
                ) => true,
                (
                    EiaPublishedSeriesPrecision::SourcePeriod { scheme },
                    EiaMacroEffectiveCutoff::ProviderPeriod(period),
                ) => scheme == period.scheme(),
                _ => false,
            };
            if !valid {
                return Err(EiaMacroApplicationError::TemporalPrecisionMismatch);
            }
        }
        Ok(())
    }

    fn series_coordinate(
        &self,
        series: &SourceIdentifier,
    ) -> Option<&EiaPublishedSeriesCoordinate> {
        self.series_coordinates
            .binary_search_by(|coordinate| coordinate.canonical_series.cmp(series))
            .ok()
            .and_then(|index| self.series_coordinates.get(index))
    }

    fn validate_request(
        &self,
        request: &EiaMacroPointInTimeRequest,
    ) -> Result<(), EiaMacroApplicationError> {
        match request {
            EiaMacroPointInTimeRequest::Calendar(request) => {
                if request.manifest() != self.binding.manifest()
                    || request.source_id() != self.binding.source_id()
                {
                    return Err(EiaMacroApplicationError::RestartInvalid);
                }
                self.validate_selected_series(
                    request.series_allowlist(),
                    &EiaMacroEffectiveCutoff::CalendarDate(request.effective_date_cutoff()),
                )
            }
            EiaMacroPointInTimeRequest::ProviderPeriod(request) => {
                if request.manifest() != self.binding.manifest()
                    || request.source_series().source_id() != self.binding.source_id()
                {
                    return Err(EiaMacroApplicationError::RestartInvalid);
                }
                self.validate_selected_series(
                    request.source_series().series_allowlist(),
                    &EiaMacroEffectiveCutoff::ProviderPeriod(
                        request.effective_period_cutoff().clone(),
                    ),
                )
            }
        }
    }

    fn validate_calendar_output(
        &self,
        output: &AnalyticalMacroLatestKnownOutput,
    ) -> Result<(), EiaMacroApplicationError> {
        if output.source_id() != self.binding.source_id()
            || output.output().manifest() != self.binding.manifest()
            || output.observations().iter().any(|observation| {
                observation
                    .context()
                    .time()
                    .effective()
                    .calendar_date_value()
                    .is_none()
            })
        {
            return Err(EiaMacroApplicationError::RestartInvalid);
        }
        Ok(())
    }

    fn validate_provider_period_output(
        &self,
        output: &AnalyticalMacroProviderPeriodLatestKnownOutput,
    ) -> Result<(), EiaMacroApplicationError> {
        if output.source_id() != self.binding.source_id()
            || output.output().manifest() != self.binding.manifest()
            || output.observations().iter().any(|observation| {
                observation
                    .context()
                    .time()
                    .effective()
                    .source_period_value()
                    .is_none_or(|period| period.scheme() != output.period_scheme())
            })
        {
            return Err(EiaMacroApplicationError::RestartInvalid);
        }
        Ok(())
    }
}

/// Exact effective cutoff preserving EIA's calendar-date or provider-period precision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EiaMacroEffectiveCutoff {
    CalendarDate(CalendarDate),
    ProviderPeriod(ResearchPeriod),
}

/// Closed fixed-template PIT request bound to one exact EIA selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EiaMacroPointInTimeRequest {
    Calendar(AnalyticalMacroLatestKnownRequest),
    ProviderPeriod(AnalyticalMacroProviderPeriodLatestKnownRequest),
}

impl EiaMacroPointInTimeRequest {
    /// Returns the fixed operation identity without exposing SQL or a physical path.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        match self {
            Self::Calendar(_) => EIA_MACRO_CALENDAR_POINT_IN_TIME_OPERATION,
            Self::ProviderPeriod(_) => EIA_MACRO_PROVIDER_PERIOD_POINT_IN_TIME_OPERATION,
        }
    }

    /// Returns the minimum bounded query rows required to retain a saturation sentinel.
    pub(crate) fn required_query_rows(&self) -> u64 {
        match self {
            Self::Calendar(request) => request.required_query_rows(),
            Self::ProviderPeriod(request) => request.required_query_rows(),
        }
    }
}

/// Exact raw/native and typed PIT evidence reopened after publication or process restart.
#[derive(Debug)]
pub(crate) enum EiaMacroRestartReceipt {
    Calendar {
        evidence: PersistedProviderCaptureBindingEvidence,
        output: AnalyticalMacroLatestKnownOutput,
    },
    ProviderPeriod {
        evidence: PersistedProviderCaptureBindingEvidence,
        output: AnalyticalMacroProviderPeriodLatestKnownOutput,
    },
}

impl EiaMacroRestartReceipt {
    /// Returns the exact sealed raw/native binding evidence common to either time precision.
    pub(crate) const fn evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        match self {
            Self::Calendar { evidence, .. } | Self::ProviderPeriod { evidence, .. } => evidence,
        }
    }
}

fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

/// Failure before or during EIA acquisition, sealing, publication, or exact PIT restart.
#[derive(Debug, Error)]
pub(crate) enum EiaMacroApplicationError {
    #[error("EIA source, registry, rights, or publication authority is not current")]
    AuthorityInvalid,
    #[error("EIA application acquisition limits are invalid")]
    InvalidLimits,
    #[error("EIA acquisition crossed an application page, record, or retained-byte limit")]
    AcquisitionLimitExceeded,
    #[error("EIA canonical publication candidate is not internally consistent")]
    InvalidCandidate,
    #[error("EIA point-in-time request selected a series absent from the exact generation")]
    SeriesNotPublished,
    #[error("EIA point-in-time cutoff does not preserve the selected series time precision")]
    TemporalPrecisionMismatch,
    #[error("EIA exact restart selector did not reproduce its immutable raw/native generation")]
    RestartInvalid,
    #[error("EIA application operation was cancelled")]
    Cancelled,
    #[error("EIA application operation exceeded its monotonic deadline")]
    DeadlineExceeded,
    #[error("EIA adapter lifecycle rejected acquisition or activation evidence")]
    Lifecycle(#[from] EiaLifecycleError),
    #[error("EIA adapter rejected canonical or capture evidence")]
    Adapter(#[from] EiaError),
    #[error("EIA sealed provider binding is invalid")]
    Capture(#[from] ProviderCaptureError),
    #[error("EIA provider macro publication failed")]
    Publication(#[from] ProviderMacroPublicationError),
    #[error("EIA application authority is unavailable")]
    Service(#[from] ServiceError),
    #[error("EIA application research composition failed")]
    Research(#[from] ResearchServiceError),
    #[error("EIA exact-manifest typed read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
}
