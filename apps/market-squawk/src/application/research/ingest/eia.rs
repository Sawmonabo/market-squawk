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
    MacroObservation, ResearchPeriod, SourceId, SourceIdentifier, Timestamp,
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

    /// Returns the complete sorted canonical series set available to neutral macro consumers.
    ///
    /// Provider route, facet, unit, and request details remain inside the persisted native
    /// evidence. Callers receive only the canonical identifiers needed to construct a fixed typed
    /// PIT request.
    pub(crate) fn published_series(&self) -> impl ExactSizeIterator<Item = &SourceIdentifier> + '_ {
        self.series_coordinates
            .iter()
            .map(|coordinate| &coordinate.canonical_series)
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
                Ok(EiaMacroRestartReceipt::calendar(evidence, output))
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
                Ok(EiaMacroRestartReceipt::provider_period(evidence, output))
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
pub(crate) struct EiaMacroRestartReceipt {
    inner: EiaMacroRestartReceiptInner,
}

#[derive(Debug)]
enum EiaMacroRestartReceiptInner {
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
    fn calendar(
        evidence: PersistedProviderCaptureBindingEvidence,
        output: AnalyticalMacroLatestKnownOutput,
    ) -> Self {
        Self {
            inner: EiaMacroRestartReceiptInner::Calendar { evidence, output },
        }
    }

    fn provider_period(
        evidence: PersistedProviderCaptureBindingEvidence,
        output: AnalyticalMacroProviderPeriodLatestKnownOutput,
    ) -> Self {
        Self {
            inner: EiaMacroRestartReceiptInner::ProviderPeriod { evidence, output },
        }
    }

    /// Returns the fixed application operation that produced this exact analytical selection.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        match &self.inner {
            EiaMacroRestartReceiptInner::Calendar { .. } => {
                EIA_MACRO_CALENDAR_POINT_IN_TIME_OPERATION
            }
            EiaMacroRestartReceiptInner::ProviderPeriod { .. } => {
                EIA_MACRO_PROVIDER_PERIOD_POINT_IN_TIME_OPERATION
            }
        }
    }

    /// Returns the exact sealed raw/native binding evidence common to either time precision.
    pub(crate) const fn evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        match &self.inner {
            EiaMacroRestartReceiptInner::Calendar { evidence, .. }
            | EiaMacroRestartReceiptInner::ProviderPeriod { evidence, .. } => evidence,
        }
    }

    /// Returns the sole source-rights owner of the selected canonical Macro rows.
    pub(crate) const fn source_id(&self) -> &SourceId {
        match &self.inner {
            EiaMacroRestartReceiptInner::Calendar { output, .. } => output.source_id(),
            EiaMacroRestartReceiptInner::ProviderPeriod { output, .. } => output.source_id(),
        }
    }

    /// Returns the exact immutable parent generation used by downstream research work.
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        match &self.inner {
            EiaMacroRestartReceiptInner::Calendar { output, .. } => output.output().manifest(),
            EiaMacroRestartReceiptInner::ProviderPeriod { output, .. } => {
                output.output().manifest()
            }
        }
    }

    /// Returns provider-neutral latest-known Macro rows with exact values, units, clocks,
    /// revisions, missingness, and effective-time precision.
    pub(crate) fn observations(&self) -> &[MacroObservation] {
        match &self.inner {
            EiaMacroRestartReceiptInner::Calendar { output, .. } => output.observations(),
            EiaMacroRestartReceiptInner::ProviderPeriod { output, .. } => output.observations(),
        }
    }

    /// Returns the code-owned identity of the complete manifest-pinned PIT selection.
    pub(crate) const fn selection_digest(&self) -> EvidenceDigest {
        match &self.inner {
            EiaMacroRestartReceiptInner::Calendar { output, .. } => output.selection_digest(),
            EiaMacroRestartReceiptInner::ProviderPeriod { output, .. } => output.selection_digest(),
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

#[cfg(all(test, debug_assertions))]
mod tests {
    use std::{
        num::{NonZeroU16, NonZeroU32, NonZeroU64},
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use market_squawk_adapter_eia::{
        EiaActivatedProvider, EiaApiKey, EiaClockField, EiaDataFieldContract,
        EiaDataFieldContractInput, EiaDataQuery, EiaDataQueryInput, EiaDatasetProfile, EiaError,
        EiaFacetFilter, EiaFacetValue, EiaFieldId, EiaMissingPolicy, EiaRoute, EiaSort,
        EiaSortDirection, EiaSourceTransport, EiaTransportLimits, EiaUnitSource, EiaValueKind,
        eia_api_endpoint_rules, eia_application_provider_budget, run_eia_doctor,
    };
    use market_squawk_data::{
        AnalyticalMacroSeriesAllowlist, CatalogConfig, CatalogResultLimits, DatasetId,
        ObjectStoreConfig, QueryLimits, RightsBasis, SourceOperation, SqliteProviderRateStore,
    };
    use market_squawk_domain::{
        AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
        ResearchPeriod, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId,
        SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::LocalPaths;
    use market_squawk_services::{JsonStructureLimits, RequestContext, RequestId, ServiceLimits};
    use market_squawk_sources::{
        AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy,
        BudgetScope, CoverageDomain, EndpointPolicy, FreshnessPolicy, HistoricalCapability,
        NetworkAccessPolicy, ProviderCapabilityRevision, ProviderRateAuthority, SourceCapabilities,
        SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
    };
    use sha2::{Digest as _, Sha256};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        EIA_MACRO_PROVIDER_PERIOD_POINT_IN_TIME_OPERATION, EiaApplicationAcquisitionLimits,
        EiaLiveComposition, EiaMacroEffectiveCutoff, EiaMacroRestartSelector, EiaRegisteredSource,
        ProductionResearchIngestCoordinator, ResearchProviderRuntimeGeneration, evidence_digest,
    };
    use crate::ResearchService;
    use crate::application::{
        ResearchExtractionLimits, ResearchProviderRuntimeMutationAuthority, ResearchRightsAuthority,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    /// One intentionally ignored credentialed journey proves the real EIA pull, every raw seal,
    /// immutable canonical publication, manifest-pinned typed PIT selection, and same-root process
    /// restart. It remains outside routine tests because it requires an operator-provided key and
    /// consumes the shared one-request-per-second provider lane.
    #[tokio::test]
    #[ignore = "requires EIA_API_KEY and performs one bounded live EIA application journey"]
    async fn live_eia_price_seals_publishes_reads_and_restarts() -> TestResult {
        let key = std::env::var("EIA_API_KEY")?;
        let now = current_timestamp()?;
        let query = live_query()?;
        let (metadata, subject) = live_source_metadata(now, &query, &key)?;
        let transport = EiaSourceTransport::try_new(
            metadata.clone(),
            EiaApiKey::try_new(key)?,
            EiaTransportLimits::try_new(
                market_squawk_adapter_eia::EiaParseLimits::production_defaults(),
                1024 * 1024,
                1,
                1024 * 1024,
            )?,
        )?;

        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("research"))?;
        let research = Arc::new(open_research(&paths)?);
        let provider_rate = ProviderRateAuthority::try_new(Arc::new(
            SqliteProviderRateStore::try_open(temporary.path().join("provider-rate.sqlite3"))?,
        ))?;
        provider_rate.bind_authorization_subject(
            metadata.authorization().mode(),
            metadata.authorization().evidence().content_digest(),
            &subject,
        )?;
        let mut doctor_registry =
            AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
                Arc::new(provider_rate.clone()),
                provider_rate.clone(),
            )?;
        let doctor_registration = doctor_registry.register(metadata.clone(), now)?;
        let doctor_authority =
            doctor_registry.extraction_authority(&doctor_registration, &transport)?;
        let provider_deadline = current_timestamp()?.checked_add_nanos(45_000_000_000)?;
        let doctor = run_eia_doctor(
            transport,
            &doctor_authority,
            EiaDatasetProfile::try_for_macro(
                query,
                vec![EiaDataFieldContract::new(EiaDataFieldContractInput {
                    field: field("price")?,
                    value_kind: EiaValueKind::Decimal,
                    unit_source: EiaUnitSource::RowField,
                    missing_policy: EiaMissingPolicy::try_new(
                        ["NA".to_owned(), "--".to_owned()],
                        true,
                    )?,
                })],
                vec![field("stateDescription")?, field("sectorName")?],
                Vec::<EiaClockField>::new(),
            )?,
            provider_deadline,
            CancellationToken::new(),
        )
        .await?;
        let capability_digest = evidence_digest(doctor.report().report_digest().bytes());
        let authorization_expires_at = doctor.report().expires_at();
        let (pending, seal_requests) = doctor.into_sealing_parts()?;
        let mut sealed = Vec::with_capacity(seal_requests.len());
        for request in seal_requests.into_vec() {
            sealed.push(
                research
                    .seal_provider_capture(
                        request,
                        &CancellationToken::new(),
                        Instant::now() + Duration::from_secs(15),
                    )
                    .await?,
            );
        }
        let activated = EiaActivatedProvider::try_activate(pending, sealed)?;
        drop(doctor_authority);
        drop(doctor_registration);
        drop(doctor_registry);

        let rights = ResearchRightsAuthority::try_new_scoped(
            metadata.source_id().clone(),
            RightsBasis::reviewed_terms(
                "https://www.eia.gov/opendata/documentation.php",
                digest_bytes(b"eia-personal-research-rights-v1"),
            )?,
            digest_bytes(b"eia-personal-research-parent-authority-v1"),
            metadata.authorization().evidence().content_digest(),
            authorization_expires_at,
            vec![subject],
            vec![SourceOperation::Persist],
        )?;
        let generation = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from("eia.api-v2")?,
            Uuid::new_v4(),
            ProviderCapabilityRevision::new(1)?,
            capability_digest,
            None,
            None,
            now,
            metadata,
            rights.clone(),
        )?;
        let registry = AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
            Arc::new(provider_rate.clone()),
            provider_rate,
        )?;
        let (coordinator, mutation, alpaca) =
            ProductionResearchIngestCoordinator::try_new_with_runtime_authorities(
                registry,
                Arc::clone(&research),
                ResearchExtractionLimits::standard(),
                std::iter::empty(),
            )?;
        let composition =
            EiaLiveComposition::try_new(Arc::clone(&coordinator), activated, generation.clone())?;
        let (source, runtime) = composition.into_parts();
        register_source(&mutation, generation, source, rights)?;

        let operation_deadline = Instant::now() + Duration::from_secs(45);
        let publication = runtime
            .acquire_seal_publish(
                DatasetId::try_from("market_squawk.research_observations")?,
                EiaApplicationAcquisitionLimits::try_new(
                    NonZeroU16::new(1).ok_or("invalid EIA page bound")?,
                    NonZeroU32::new(24).ok_or("invalid EIA record bound")?,
                    NonZeroU64::new(16 * 1024 * 1024).ok_or("invalid EIA byte bound")?,
                )?,
                &request_context(operation_deadline)?,
            )
            .await?;
        assert_eq!(publication.committed().pinned().plan().row_count(), 24);
        assert_eq!(publication.transport().requests(), 1);
        assert_eq!(publication.transport().returned_rows(), 24);
        assert_eq!(publication.transport().observations(), 24);
        let selector = publication.restart_selector().clone();
        assert_eq!(selector.manifest(), publication.committed().manifest());
        let mut published_series = selector.published_series();
        let series = published_series
            .next()
            .ok_or("the exact EIA profile published no canonical series")?
            .clone();
        if published_series.next().is_some() {
            return Err("the exact EIA profile must publish one canonical series".into());
        }
        let scheme = selector
            .provider_period_scheme(&series)
            .ok_or("EIA monthly series lost provider-period precision")?
            .clone();
        let allowlist =
            AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![series])?;
        let expected_series = allowlist.series()[0].clone();
        let cutoff = current_timestamp()?;
        let effective = ResearchPeriod::try_new(
            scheme,
            2025,
            NonZeroU16::new(12).ok_or("invalid EIA month")?,
            SourceIdentifier::try_from("2025-12")?,
        )?;
        let request = selector.try_point_in_time_request(
            allowlist.clone(),
            cutoff,
            EiaMacroEffectiveCutoff::ProviderPeriod(effective.clone()),
        )?;
        let query_limits = query_limits()?;
        let read = selector
            .reopen_point_in_time(
                research.as_ref(),
                request,
                query_limits,
                Instant::now() + Duration::from_secs(10),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(read.evidence().binding_digest(), selector.binding_digest());
        assert_eq!(read.evidence().record_count(), 24);
        assert_eq!(read.evidence().physical_claims().len(), 1);
        assert_eq!(
            read.operation_identity(),
            EIA_MACRO_PROVIDER_PERIOD_POINT_IN_TIME_OPERATION
        );
        assert_eq!(read.source_id(), selector.source_id());
        assert_eq!(read.manifest(), selector.manifest());
        assert_eq!(read.observations().len(), 1);
        assert_eq!(read.observations()[0].series(), &expected_series);
        let binding_evidence = read.evidence().clone();
        let selected_observation = read.observations()[0].clone();
        let selection_digest = read.selection_digest();
        assert_eq!(selection_digest.algorithm(), DigestAlgorithm::Sha256);
        assert_ne!(selection_digest.bytes(), [0; 32]);
        let manifest = selector.manifest().clone();
        let expected_source = selector.source_id().clone();

        drop(read);
        drop(selector);
        drop(publication);
        drop(runtime);
        drop(mutation);
        drop(alpaca);
        drop(coordinator);
        drop(research);

        let reopened = Arc::new(open_research(&paths)?);
        let selector = EiaMacroRestartSelector::try_reopen(
            reopened.as_ref(),
            manifest.clone(),
            &expected_source,
        )?;
        assert_eq!(
            selector.published_series().collect::<Vec<_>>(),
            vec![&expected_series]
        );
        let request = selector.try_point_in_time_request(
            allowlist,
            cutoff,
            EiaMacroEffectiveCutoff::ProviderPeriod(effective),
        )?;
        let read = selector
            .reopen_point_in_time(
                reopened.as_ref(),
                request,
                query_limits,
                Instant::now() + Duration::from_secs(10),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(selector.manifest(), &manifest);
        assert_eq!(read.evidence(), &binding_evidence);
        assert_eq!(read.source_id(), &expected_source);
        assert_eq!(read.manifest(), &manifest);
        assert_eq!(
            read.observations(),
            std::slice::from_ref(&selected_observation)
        );
        assert_eq!(read.selection_digest(), selection_digest);
        Ok(())
    }

    fn live_query() -> TestResult<EiaDataQuery> {
        Ok(EiaDataQuery::try_new(EiaDataQueryInput {
            route: EiaRoute::try_from("electricity/retail-sales")?,
            data_fields: vec![field("price")?],
            facets: vec![
                EiaFacetFilter::try_new(field("sectorid")?, vec![EiaFacetValue::try_from("RES")?])?,
                EiaFacetFilter::try_new(field("stateid")?, vec![EiaFacetValue::try_from("US")?])?,
            ],
            frequency: field("monthly")?,
            start: Some("2024-01".to_owned()),
            end: Some("2025-12".to_owned()),
            sorts: vec![
                EiaSort::new(field("period")?, EiaSortDirection::Ascending),
                EiaSort::new(field("stateid")?, EiaSortDirection::Ascending),
                EiaSort::new(field("sectorid")?, EiaSortDirection::Ascending),
                EiaSort::new(field("stateDescription")?, EiaSortDirection::Ascending),
                EiaSort::new(field("sectorName")?, EiaSortDirection::Ascending),
            ],
            length: 24,
        })?)
    }

    fn live_source_metadata(
        now: Timestamp,
        query: &EiaDataQuery,
        key: &str,
    ) -> TestResult<(SourceMetadata, SourceIdentifier)> {
        let effective = EffectiveInterval::new(now.checked_sub_nanos(1_000_000_000)?, None)?;
        let credential_evidence = digest_bytes(
            [
                b"market-squawk/eia-live-credential-generation/v1\0".as_slice(),
                key.as_bytes(),
            ]
            .concat()
            .as_slice(),
        );
        let evidence = ExactPayloadEvidence::from_content_digest(credential_evidence);
        let provider = SourceIdentifier::try_from("us-eia")?;
        let subject = SourceIdentifier::try_from("eia-personal-research-key")?;
        let basis = AuthorizationBasis::new(subject.clone());
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            basis.clone(),
            evidence.clone(),
            effective,
        );
        let endpoint = EndpointPolicy::try_from_api_rules(
            eia_api_endpoint_rules(query)?,
            market_squawk_sources::HttpRequestBounds::default(),
        )?;
        let budget = eia_application_provider_budget(
            BudgetScope::with_authorization_account(
                provider.clone(),
                basis.as_source_identifier().clone(),
            ),
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000_000).ok_or("invalid EIA backoff")?,
                NonZeroU64::new(3_600_000_000_000).ok_or("invalid EIA max backoff")?,
                0,
            )?,
        )?;
        let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            SourceId::try_from("us-eia-api-v2")?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("eia-api-v2-live-v1")?),
                evidence.clone(),
            ),
            SourceClass::OfficialAgency,
            provider,
            authorization,
            SourceCoverage::try_non_instrument(
                evidence,
                effective,
                CoverageDomain::Macroeconomic,
                CoverageDelay::Delayed(1),
                DeliveryEvidence::Unknown,
            )?,
            DataQuality::OfficialDelayed,
            NetworkAccessPolicy::Allowlisted(endpoint),
            FreshnessPolicy::try_new(
                60_000_000_000,
                60_000_000_000,
                60_000_000_000,
                60_000_000_000,
                1_000_000_000,
            )?,
            Some(budget),
            SourceCapabilities::new(
                false,
                true,
                SequenceCapability::Unsupported,
                ChecksumCapability::Unsupported,
                HistoricalCapability::RevisionPreserving,
                false,
            ),
            SourceProtocolProfile::NotLive,
        ))?;
        Ok((metadata, subject))
    }

    fn register_source(
        mutation: &ResearchProviderRuntimeMutationAuthority,
        generation: ResearchProviderRuntimeGeneration,
        source: EiaRegisteredSource,
        rights: ResearchRightsAuthority,
    ) -> Result<(), super::super::ResearchIngestCompositionError> {
        mutation.register_provider_source(generation, source, rights)
    }

    fn field(value: &str) -> Result<EiaFieldId, EiaError> {
        EiaFieldId::try_from(value)
    }

    fn open_research(paths: &LocalPaths) -> TestResult<ResearchService> {
        Ok(ResearchService::open_or_initialize(
            paths,
            CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                market_squawk_data::CatalogLimit::new(64)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?)
    }

    fn query_limits() -> TestResult<QueryLimits> {
        Ok(QueryLimits::try_new(
            32,
            1024 * 1024,
            8 * 1024 * 1024,
            8,
            1024,
            1024,
            Duration::from_secs(10),
        )?)
    }

    fn request_context(deadline: Instant) -> TestResult<RequestContext> {
        let structure = JsonStructureLimits::try_new(16, 4096, 64, 64)?;
        let limits = ServiceLimits::try_new(4096, 8, 4096, 8, structure)?;
        Ok(RequestContext::new(
            RequestId::String(Arc::from("test.eia-live-publication")),
            CancellationToken::new(),
            deadline,
            limits,
        ))
    }

    fn current_timestamp() -> TestResult<Timestamp> {
        let nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }

    fn digest_bytes(bytes: &[u8]) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
    }
}
