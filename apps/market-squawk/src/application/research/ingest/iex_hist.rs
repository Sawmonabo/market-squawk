//! Application-owned explicit IEX HIST research-job composition.
//!
//! This leaf admits only one operator- or research-selected feed/date object on the adapter's cold
//! lane, preserves the adapter's durable capacity/checkpoint authorities, requires common-store
//! seals for the catalog/provider object/expanded PCAP, and carries a continuity-complete typed
//! decode into an atomic common-publication contract and exact manifest-pinned restart reader. It
//! cannot report product availability until shared composition supplies IEX-native lineage and a
//! date-effective canonical [`market_squawk_domain::InstrumentId`] mapping.
//!
//! Nothing in this module schedules an archive, treats IEX venue history as live data, or upgrades
//! it to SIP, NBBO, consolidated, or market-wide evidence.

use std::{fs::File, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_adapter_iex_hist::{
    ByteAdmissionLimits, CatalogError, CatalogFetch, ColdJobPlan, ColdJobTrigger, DecodeLimits,
    DecodeSummary, ExactFileRequest, FeedKind, FeedVersion, IexEventSink, IexHistBarInterval,
    IexHistCapacityAuthority, IexHistCapacityCategory, IexHistCapacityDisposition,
    IexHistCapacityError, IexHistCatalogPhysicalSealEvidence, IexHistCheckpointError,
    IexHistCheckpointStore, IexHistColdTransport, IexHistCompletePhysicalSeal,
    IexHistCompleteSealError, IexHistDecodedSealedCapture, IexHistDerivedBarError,
    IexHistDerivedBarsHandoff, IexHistDownloadOutcome, IexHistDplcDistributionAuthority,
    IexHistDurableJob, IexHistExecutionPermit, IexHistJobPhase, IexHistPlanner,
    IexHistRecoveryAction, IexHistResumeCandidate, IexHistSealedCatalog,
    IexHistSealedMaterializedCapture, IexHistSharedPhysicalSealReceipt, IexHistTerminalEvidence,
    IexHistTerminalReason, IexHistTransportError, IexHistTrustedClockReading,
    IexHistTypedHandoffBuilder, MaterializedIexCapture, PcapMaterializationReceipt,
    PcapObjectEncoding, PlanError, ScheduleLane, Sha256Digest, TradeDate, TransportTelemetry,
    TransportVersion,
};
use market_squawk_data::{
    AnalyticalMarketBarOutput, AnalyticalMarketBarReadLimit, AnalyticalMarketBarReadRequest,
    AnalyticalReadError, DatasetId, DatasetManifestRef, DatasetSchemaRegistry,
    MarketBarEffectiveRange, QueryLimits,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, SourceId, Timestamp, VenueId,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::ResearchService;

/// IEX HIST has one code-owned scheduling classification.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum IexHistApplicationLane {
    /// On-demand, low-priority, historical IEX venue evidence.
    ColdHistoricalResearch,
}

/// Exact authority that initiated an explicit selected-file job.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum IexHistJobAuthority {
    Operator,
    ResearchJob,
}

impl From<IexHistJobAuthority> for ColdJobTrigger {
    fn from(value: IexHistJobAuthority) -> Self {
        match value {
            IexHistJobAuthority::Operator => Self::Operator,
            IexHistJobAuthority::ResearchJob => Self::ResearchJob,
        }
    }
}

/// Complete explicit coordinates and immutable resource ceilings for one job preview.
#[derive(Clone, Debug)]
pub(crate) struct IexHistExplicitJobRequest {
    selection: ExactFileRequest,
    authority: IexHistJobAuthority,
    byte_limits: ByteAdmissionLimits,
    decode_limits: DecodeLimits,
}

impl IexHistExplicitJobRequest {
    /// Creates an exact request. Catalog selection and the adapter planner validate the filename,
    /// version family, T+1 availability, rolling window, expansion, and full storage footprint.
    #[allow(
        clippy::too_many_arguments,
        reason = "feed/date/version/object coordinates and independent byte ceilings stay explicit"
    )]
    pub(crate) fn new(
        trade_date: TradeDate,
        feed: FeedKind,
        feed_version: FeedVersion,
        transport_version: TransportVersion,
        object_encoding: PcapObjectEncoding,
        file_name: impl Into<String>,
        authority: IexHistJobAuthority,
        byte_limits: ByteAdmissionLimits,
        decode_limits: DecodeLimits,
    ) -> Self {
        Self {
            selection: ExactFileRequest {
                trade_date,
                feed,
                feed_version,
                transport_version,
                object_encoding,
                file_name: file_name.into(),
            },
            authority,
            byte_limits,
            decode_limits,
        }
    }

    pub(crate) const fn selection(&self) -> &ExactFileRequest {
        &self.selection
    }

    pub(crate) const fn authority(&self) -> IexHistJobAuthority {
        self.authority
    }
}

/// One exact catalog response awaiting application-owned physical sealing.
///
/// The adapter's durable capacity permit remains inside `fetch`. Dropping this value records an
/// interruption through the existing authority; a caller cannot mistake a parsed catalog for a
/// durably sealed generation.
#[derive(Debug)]
pub(crate) struct IexHistCatalogSealHandoff {
    fetch: CatalogFetch,
    requirement: IexHistPhysicalSealRequirement,
}

impl IexHistCatalogSealHandoff {
    fn try_new(fetch: CatalogFetch) -> Result<Self, IexHistApplicationError> {
        let receipt = fetch.receipt();
        let received_at =
            Timestamp::from_unix_nanos(receipt.observation.retrieved_clock().unix_nanos());
        let requirement = IexHistPhysicalSealRequirement {
            artifact: IexHistPhysicalArtifact::CatalogJson,
            content_sha256: receipt.body_sha256,
            exact_bytes: receipt.body_bytes,
            parent_sha256: receipt.observation.receipt_sha256(),
            received_at,
        };
        if fetch.exact_body().is_empty()
            || u64::try_from(fetch.exact_body().len()).ok() != Some(requirement.exact_bytes)
            || Sha256Digest::of(fetch.exact_body()) != requirement.content_sha256
        {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        Ok(Self { fetch, requirement })
    }

    pub(crate) fn exact_body(&self) -> &[u8] {
        self.fetch.exact_body()
    }

    pub(crate) const fn physical_requirement(&self) -> &IexHistPhysicalSealRequirement {
        &self.requirement
    }

    /// Rejoins the common-store receipt and settles catalog capacity before descriptor selection
    /// authority becomes available.
    pub(crate) fn try_rejoin<R>(
        self,
        physical: R,
    ) -> Result<IexHistSealedCatalog<R>, IexHistApplicationError>
    where
        R: IexHistSharedPhysicalSealReceipt,
    {
        self.fetch
            .try_bind_physical_seal(physical)
            .map_err(Into::into)
    }
}

/// One exact selected-file authority inseparable from its physically sealed catalog generation.
///
/// Opening the durable checkpoint consumes this value, so no public application path can discard
/// the catalog seal and continue with a naked plan.
#[derive(Debug)]
pub(crate) struct IexHistExactJobPreview<R> {
    plan: ColdJobPlan,
    sealed_catalog: IexHistSealedCatalog<R>,
}

impl<R> IexHistExactJobPreview<R> {
    pub(crate) const fn plan(&self) -> &ColdJobPlan {
        &self.plan
    }

    /// Retains the exact catalog raw-object receipt that selected this descriptor.
    pub(crate) const fn sealed_catalog(&self) -> &IexHistSealedCatalog<R> {
        &self.sealed_catalog
    }

    pub(crate) fn status(&self) -> Result<IexHistSelectionStatus, IexHistApplicationError> {
        IexHistSelectionStatus::from_plan(&self.plan)
    }
}

/// One completed adapter materialization awaiting durable adoption of every physical artifact.
///
/// The retained capacity permit cannot be settled as complete until the shared storage owner has
/// sealed both expectations and durably joined its receipt to the provider-local checkpoint.
#[derive(Debug)]
pub(crate) struct IexHistCaptureSealHandoff {
    capture: Box<MaterializedIexCapture>,
    requirements: IexHistCaptureSealRequirements,
}

impl IexHistCaptureSealHandoff {
    pub(crate) fn try_new(
        plan: &ColdJobPlan,
        capture: Box<MaterializedIexCapture>,
    ) -> Result<Self, IexHistApplicationError> {
        validate_application_plan(plan)?;
        let receipt = capture.materialization();
        let completed_at = Timestamp::from_unix_nanos(receipt.completed_at_unix_nanos());
        let provider_object = IexHistPhysicalSealRequirement {
            artifact: IexHistPhysicalArtifact::ProviderObject,
            content_sha256: receipt.compressed_sha256(),
            exact_bytes: receipt.compressed_bytes(),
            parent_sha256: receipt.receipt_sha256(),
            received_at: completed_at,
        };
        let expanded_pcap = IexHistPhysicalSealRequirement {
            artifact: IexHistPhysicalArtifact::ExpandedPcap,
            content_sha256: receipt.pcap_sha256(),
            exact_bytes: receipt.pcap_bytes(),
            parent_sha256: receipt.receipt_sha256(),
            received_at: completed_at,
        };
        if provider_object.exact_bytes == 0
            || expanded_pcap.exact_bytes < 24
            || provider_object.content_sha256 == Sha256Digest::of(b"")
            || expanded_pcap.content_sha256 == Sha256Digest::of(b"")
        {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        Ok(Self {
            capture,
            requirements: IexHistCaptureSealRequirements {
                provider_object,
                expanded_pcap,
            },
        })
    }

    pub(crate) const fn materialization(&self) -> &PcapMaterializationReceipt {
        self.capture.materialization()
    }

    pub(crate) const fn physical_requirements(&self) -> &IexHistCaptureSealRequirements {
        &self.requirements
    }

    /// Reopens the exact provider object for the common content-addressed sealer while this
    /// handoff retains the materialization permit.
    pub(crate) fn reopen_provider_object(&self) -> std::io::Result<File> {
        self.capture.reopen_provider_object()
    }

    /// Reopens the exact expanded PCAP for the common content-addressed sealer while this handoff
    /// retains the materialization permit.
    pub(crate) fn reopen_expanded_pcap(&self) -> std::io::Result<File> {
        self.capture.reopen_pcap()
    }

    /// Rejoins both common-store receipts to the exact materialization while retaining the
    /// original one-transfer lease for decode and publication.
    pub(crate) fn try_rejoin<R, S: IexHistCheckpointStore>(
        self,
        job: &IexHistDurableJob<S>,
        provider_object: R,
        expanded_pcap: R,
    ) -> Result<IexHistSealedMaterializedCapture<R>, IexHistApplicationError>
    where
        R: IexHistSharedPhysicalSealReceipt,
    {
        validate_application_plan(job.plan())?;
        if !catalog_seal_matches_plan(*job.catalog_physical_seal(), job.plan()) {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        self.capture
            .try_bind_complete_physical_seal(job.plan(), provider_object, expanded_pcap)
            .map_err(Into::into)
    }
}

/// Physical object class that must be sealed without changing its bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum IexHistPhysicalArtifact {
    CatalogJson,
    ProviderObject,
    ExpandedPcap,
}

/// Byte-exact handoff requirement for the existing shared storage owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IexHistPhysicalSealRequirement {
    artifact: IexHistPhysicalArtifact,
    content_sha256: Sha256Digest,
    exact_bytes: u64,
    parent_sha256: Sha256Digest,
    received_at: Timestamp,
}

impl IexHistPhysicalSealRequirement {
    pub(crate) const fn artifact(self) -> IexHistPhysicalArtifact {
        self.artifact
    }

    pub(crate) const fn content_sha256(self) -> Sha256Digest {
        self.content_sha256
    }

    pub(crate) const fn exact_bytes(self) -> u64 {
        self.exact_bytes
    }

    pub(crate) const fn parent_sha256(self) -> Sha256Digest {
        self.parent_sha256
    }

    /// Local receipt/completion clock. This is not exchange event time or analytical availability.
    pub(crate) const fn received_at(self) -> Timestamp {
        self.received_at
    }
}

/// Atomic complete-file sealing requirement. Both members must be adopted before decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IexHistCaptureSealRequirements {
    provider_object: IexHistPhysicalSealRequirement,
    expanded_pcap: IexHistPhysicalSealRequirement,
}

impl IexHistCaptureSealRequirements {
    pub(crate) const fn provider_object(self) -> IexHistPhysicalSealRequirement {
        self.provider_object
    }

    pub(crate) const fn expanded_pcap(self) -> IexHistPhysicalSealRequirement {
        self.expanded_pcap
    }
}

/// Application-owned leaf for bounded, explicitly initiated IEX HIST work.
#[derive(Debug)]
pub(crate) struct IexHistResearchJobLeaf {
    transport: Arc<IexHistColdTransport>,
}

impl IexHistResearchJobLeaf {
    pub(crate) const fn new(transport: Arc<IexHistColdTransport>) -> Self {
        Self { transport }
    }

    /// Fetches one bounded catalog generation and immediately converts it to a mandatory physical
    /// sealing handoff. This performs no descriptor selection and no archive scheduling.
    pub(crate) async fn fetch_catalog(
        &self,
        capacity: &dyn IexHistCapacityAuthority,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
    ) -> Result<IexHistCatalogSealHandoff, IexHistApplicationError> {
        let fetch = self
            .transport
            .fetch_catalog(capacity, deadline_unix_nanos, cancellation)
            .await?;
        IexHistCatalogSealHandoff::try_new(fetch)
    }

    /// Selects exactly one descriptor while retaining its sealed catalog as one-use job authority.
    /// The adapter enforces cold-only, one transfer, no automatic catch-up, T+1/window admission,
    /// and complete network/temp/durable/Arrow/Parquet/manifest/free-reserve accounting.
    pub(crate) fn preview_exact_job<R>(
        &self,
        catalog: IexHistSealedCatalog<R>,
        request: IexHistExplicitJobRequest,
        dplc_distribution: Option<&dyn IexHistDplcDistributionAuthority>,
    ) -> Result<IexHistExactJobPreview<R>, IexHistApplicationError> {
        let selected = catalog.catalog().select(&request.selection)?;
        let plan = IexHistPlanner::plan(
            selected,
            request.authority.into(),
            request.byte_limits,
            request.decode_limits,
            dplc_distribution,
        )?;
        validate_application_plan(&plan)?;
        Ok(IexHistExactJobPreview {
            plan,
            sealed_catalog: catalog,
        })
    }

    /// Opens or restores the adapter's provider-local durable checkpoint after the shared catalog
    /// sealer has accepted the exact parent bytes. No parallel checkpoint store is introduced.
    pub(crate) fn open_checkpoint<R, S: IexHistCheckpointStore>(
        &self,
        selected_job: IexHistExactJobPreview<R>,
        store: S,
    ) -> Result<IexHistDurableJob<S>, IexHistApplicationError> {
        let IexHistExactJobPreview {
            plan,
            sealed_catalog,
        } = selected_job;
        validate_application_plan(&plan)?;
        IexHistDurableJob::try_open(&plan, sealed_catalog.physical_evidence(), store)
            .map_err(Into::into)
    }

    /// Reopens the adapter's complete plan and evidence from the existing durable checkpoint seam.
    pub(crate) fn restore_checkpoint<S: IexHistCheckpointStore>(
        &self,
        store: S,
    ) -> Result<IexHistDurableJob<S>, IexHistApplicationError> {
        let job = IexHistDurableJob::restore(store)?;
        validate_application_plan(job.plan())?;
        Ok(job)
    }

    /// Executes one exact selected file only from a sealed-catalog-bound durable job.
    pub(crate) async fn download_selected<S: IexHistCheckpointStore>(
        &self,
        job: &IexHistDurableJob<S>,
        capacity: &dyn IexHistCapacityAuthority,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
    ) -> Result<IexHistDownloadOutcome, IexHistApplicationError> {
        validate_application_plan(job.plan())?;
        if !catalog_seal_matches_plan(*job.catalog_physical_seal(), job.plan()) {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        self.transport
            .download_materialize(job.plan(), capacity, deadline_unix_nanos, cancellation)
            .await
            .map_err(Into::into)
    }

    /// Revalidates and resumes one shared-store-adopted exact prefix for the same durable job.
    pub(crate) async fn resume_selected<S: IexHistCheckpointStore>(
        &self,
        job: &IexHistDurableJob<S>,
        capacity: &dyn IexHistCapacityAuthority,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
        candidate: IexHistResumeCandidate,
    ) -> Result<IexHistDownloadOutcome, IexHistApplicationError> {
        validate_application_plan(job.plan())?;
        if !catalog_seal_matches_plan(*job.catalog_physical_seal(), job.plan()) {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        self.transport
            .resume_materialize(
                job.plan(),
                capacity,
                deadline_unix_nanos,
                cancellation,
                candidate,
            )
            .await
            .map_err(Into::into)
    }

    /// Builds a truthful application status without promoting provider-local capture/decode
    /// evidence to a canonical generation or product result.
    pub(crate) fn status<S: IexHistCheckpointStore>(
        &self,
        job: &IexHistDurableJob<S>,
    ) -> Result<IexHistJobStatus, IexHistApplicationError> {
        IexHistJobStatus::from_job(job)
    }

    /// Converts a materialized adapter result into the complete shared-sealer handoff. No method
    /// here can decode from an unsealed temporary file or settle publication as complete.
    pub(crate) fn require_capture_seal<S: IexHistCheckpointStore>(
        &self,
        job: &IexHistDurableJob<S>,
        capture: Box<MaterializedIexCapture>,
    ) -> Result<IexHistCaptureSealHandoff, IexHistApplicationError> {
        if !catalog_seal_matches_plan(*job.catalog_physical_seal(), job.plan()) {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        IexHistCaptureSealHandoff::try_new(job.plan(), capture)
    }

    /// Records a complete shared-sealed capture before any decode is allowed.
    pub(crate) fn record_sealed_capture<S: IexHistCheckpointStore, R>(
        &self,
        job: &mut IexHistDurableJob<S>,
        sealed: &IexHistSealedMaterializedCapture<R>,
        observed_clock: IexHistTrustedClockReading,
    ) -> Result<(), IexHistApplicationError> {
        if sealed.plan().plan_sha256() != job.plan().plan_sha256()
            || !catalog_seal_matches_plan(*job.catalog_physical_seal(), job.plan())
        {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        let plan = job.plan().clone();
        job.record_capture(
            &plan,
            sealed.materialization().clone(),
            sealed.physical().evidence(),
            observed_clock,
        )?;
        Ok(())
    }

    /// Decodes only a complete raw rejoin, preserving one selected-file capacity lease from
    /// acquisition through the typed publication handoff.
    async fn decode_sealed_capture<S, R>(
        &self,
        sealed: IexHistSealedMaterializedCapture<R>,
        cancellation: &CancellationToken,
        sink: S,
    ) -> Result<IexHistDecodedSealedCapture<S, R>, IexHistApplicationError>
    where
        S: IexEventSink,
        R: IexHistSharedPhysicalSealReceipt,
    {
        self.transport
            .decode_rejoined_sealed_pcap(sealed, cancellation, sink)
            .await
            .map_err(Into::into)
    }

    /// Performs the sole production IEX bar decode and retains the original selected-file lease
    /// for the shared canonical publication transaction.
    ///
    /// The typed builder commits only after whole-file continuity succeeds. The derived handoff
    /// then applies the adapter's exact trade/trade-break calculation without inventing session,
    /// consolidated-market, adjustment, or canonical-instrument semantics.
    pub(crate) async fn prepare_trade_bar_publication<S: IexHistCheckpointStore, R>(
        &self,
        job: &mut IexHistDurableJob<S>,
        sealed: IexHistSealedMaterializedCapture<R>,
        interval: IexHistBarInterval,
        source_id: SourceId,
        analytical_dataset: DatasetId,
        cancellation: &CancellationToken,
    ) -> Result<IexHistCanonicalPublicationHandoff<R>, IexHistApplicationError>
    where
        R: IexHistSharedPhysicalSealReceipt,
    {
        if sealed.plan().plan_sha256() != job.plan().plan_sha256()
            || job.capture_evidence() != Some(sealed.materialization())
            || !catalog_seal_matches_plan(*job.catalog_physical_seal(), job.plan())
        {
            return Err(IexHistApplicationError::InvalidPhysicalHandoff);
        }
        let builder = IexHistTypedHandoffBuilder::try_new(sealed.plan(), sealed.materialization())?;
        let decoded = self
            .decode_sealed_capture(sealed, cancellation, builder)
            .await?;
        let plan = decoded.plan().clone();
        let materialization = decoded.materialization().clone();
        let summary = decoded.summary().clone();
        job.record_decoded(&plan, &materialization, summary)?;
        IexHistCanonicalPublicationHandoff::try_from_decoded(
            decoded,
            *job.catalog_physical_seal(),
            interval,
            source_id,
            analytical_dataset,
        )
    }

    /// The current product boundary is deliberately closed. Product availability can be added only
    /// by the shared-data owner after every blocker has a non-forgeable exact receipt.
    pub(crate) const fn publication_availability(&self) -> IexHistPublicationAvailability {
        IexHistPublicationAvailability::Unavailable(IexHistPublicationBlockers::current())
    }
}

/// Linear publication candidate that still owns the selected-file capacity lease.
///
/// It contains no storage implementation. A shared publication authority may inspect the exact
/// sealed raw parents and provider-native bars, but only this value can account publication bytes
/// and settle the original admission as complete.
pub(crate) struct IexHistCanonicalPublicationHandoff<R> {
    source_id: SourceId,
    analytical_dataset: DatasetId,
    plan: ColdJobPlan,
    catalog_physical_seal: IexHistCatalogPhysicalSealEvidence,
    materialization: PcapMaterializationReceipt,
    summary: DecodeSummary,
    derived: IexHistDerivedBarsHandoff,
    telemetry: TransportTelemetry,
    capacity_permit: IexHistExecutionPermit,
    physical: IexHistCompletePhysicalSeal<R>,
}

impl<R> std::fmt::Debug for IexHistCanonicalPublicationHandoff<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IexHistCanonicalPublicationHandoff")
            .field("source_id", &self.source_id)
            .field("analytical_dataset", &self.analytical_dataset)
            .field("plan_sha256", &self.plan.plan_sha256())
            .field(
                "capture_receipt_sha256",
                &self.materialization.receipt_sha256(),
            )
            .field("decode_summary_sha256", &self.summary.summary_sha256())
            .field("derived_handoff_sha256", &self.derived.handoff_sha256())
            .field("bar_count", &self.derived.bars().len())
            .field("physical_seal_sha256", &self.physical.seal_sha256())
            .finish_non_exhaustive()
    }
}

impl<R> IexHistCanonicalPublicationHandoff<R> {
    fn try_from_decoded(
        decoded: IexHistDecodedSealedCapture<IexHistTypedHandoffBuilder, R>,
        catalog_physical_seal: IexHistCatalogPhysicalSealEvidence,
        interval: IexHistBarInterval,
        source_id: SourceId,
        analytical_dataset: DatasetId,
    ) -> Result<Self, IexHistApplicationError> {
        let (plan, materialization, summary, builder, telemetry, capacity_permit, physical) =
            decoded.into_parts();
        if !catalog_seal_matches_plan(catalog_physical_seal, &plan) {
            let settlement_error = capacity_permit
                .settle(IexHistCapacityDisposition::Quarantined(
                    IexHistTerminalReason::DownstreamIntegrityFault,
                ))
                .err();
            return Err(IexHistApplicationError::DerivedPreparation {
                error: IexHistDerivedPreparationError::InvalidHandoff,
                settlement_error,
            });
        }
        let typed = match builder.try_into_handoff(summary.clone()) {
            Ok(typed) => typed,
            Err(error) => {
                let settlement_error = capacity_permit
                    .settle(IexHistCapacityDisposition::Quarantined(
                        IexHistTerminalReason::DownstreamIntegrityFault,
                    ))
                    .err();
                return Err(IexHistApplicationError::DerivedPreparation {
                    error: IexHistDerivedPreparationError::Decode(error),
                    settlement_error,
                });
            }
        };
        let derived = match typed.try_into_derived_bars(interval) {
            Ok(derived) => derived,
            Err(error) => {
                let disposition = if matches!(error, IexHistDerivedBarError::Capacity) {
                    IexHistCapacityDisposition::Failed
                } else {
                    IexHistCapacityDisposition::Quarantined(
                        IexHistTerminalReason::DownstreamIntegrityFault,
                    )
                };
                let settlement_error = capacity_permit.settle(disposition).err();
                return Err(IexHistApplicationError::DerivedPreparation {
                    error: IexHistDerivedPreparationError::Bars(error),
                    settlement_error,
                });
            }
        };
        if derived.bars().is_empty() {
            let settlement_error = capacity_permit
                .settle(IexHistCapacityDisposition::Failed)
                .err();
            return Err(IexHistApplicationError::DerivedPreparation {
                error: IexHistDerivedPreparationError::NoEligibleTrades,
                settlement_error,
            });
        }
        if derived.source().plan().plan_sha256() != plan.plan_sha256()
            || derived.source().capture().receipt_sha256() != materialization.receipt_sha256()
            || derived.source().summary().summary_sha256() != summary.summary_sha256()
        {
            let settlement_error = capacity_permit
                .settle(IexHistCapacityDisposition::Quarantined(
                    IexHistTerminalReason::DownstreamIntegrityFault,
                ))
                .err();
            return Err(IexHistApplicationError::DerivedPreparation {
                error: IexHistDerivedPreparationError::InvalidHandoff,
                settlement_error,
            });
        }
        Ok(Self {
            source_id,
            analytical_dataset,
            plan,
            catalog_physical_seal,
            materialization,
            summary,
            derived,
            telemetry,
            capacity_permit,
            physical,
        })
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn plan(&self) -> &ColdJobPlan {
        &self.plan
    }

    pub(crate) const fn materialization(&self) -> &PcapMaterializationReceipt {
        &self.materialization
    }

    pub(crate) const fn summary(&self) -> &DecodeSummary {
        &self.summary
    }

    pub(crate) const fn derived(&self) -> &IexHistDerivedBarsHandoff {
        &self.derived
    }

    pub(crate) const fn physical(&self) -> &IexHistCompletePhysicalSeal<R> {
        &self.physical
    }

    fn publication_input(&self) -> IexHistCanonicalPublicationInput<'_, R> {
        IexHistCanonicalPublicationInput {
            source_id: &self.source_id,
            analytical_dataset: &self.analytical_dataset,
            plan: &self.plan,
            catalog_physical_seal: self.catalog_physical_seal,
            materialization: &self.materialization,
            summary: &self.summary,
            derived: &self.derived,
            physical: &self.physical,
        }
    }

    /// Invokes the existing shared publisher, validates its immutable receipt, accounts exact
    /// actual bytes, and only then releases the original selected-file lease as complete.
    pub(crate) async fn publish<A>(
        self,
        authority: &A,
        cancellation: CancellationToken,
    ) -> Result<
        IexHistPublishedBars<R>,
        IexHistPublicationError<<A as IexHistRestartLineageAuthority>::Error>,
    >
    where
        R: IexHistSharedPhysicalSealReceipt + Send + Sync,
        A: IexHistCanonicalPublicationAuthority<R>,
    {
        let receipt = match authority
            .publish(self.publication_input(), cancellation)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                let settlement_error = self
                    .capacity_permit
                    .settle(IexHistCapacityDisposition::Failed)
                    .err();
                return Err(IexHistPublicationError::Authority {
                    error,
                    settlement_error,
                });
            }
        };
        if !receipt.validates_against(&self) {
            let settlement_error = self
                .capacity_permit
                .settle(IexHistCapacityDisposition::Quarantined(
                    IexHistTerminalReason::DownstreamIntegrityFault,
                ))
                .err();
            return Err(IexHistPublicationError::InvalidReceipt {
                receipt: Box::new(receipt),
                settlement_error,
            });
        }
        let Self {
            source_id: _,
            plan,
            catalog_physical_seal,
            materialization,
            summary,
            telemetry,
            mut capacity_permit,
            physical,
            analytical_dataset: _,
            derived: _,
        } = self;
        for (category, bytes) in [
            (IexHistCapacityCategory::CanonicalArrow, receipt.arrow_bytes),
            (
                IexHistCapacityCategory::ImmutableParquet,
                receipt.parquet_bytes,
            ),
            (
                IexHistCapacityCategory::ManifestAndAtomicOverhead,
                receipt.manifest_and_atomic_bytes,
            ),
        ] {
            if let Err(error) = capacity_permit.record_usage(category, bytes) {
                let settlement_error = capacity_permit
                    .settle(IexHistCapacityDisposition::Quarantined(
                        IexHistTerminalReason::ResourceLimitExceeded,
                    ))
                    .err();
                return Err(IexHistPublicationError::CapacityAfterCommit {
                    error,
                    receipt: Box::new(receipt),
                    settlement_error,
                });
            }
        }
        let restart = IexHistRestartSelector {
            publication: receipt,
        };
        let lineage = match authority.revalidate(&restart) {
            Ok(lineage) => lineage,
            Err(error) => {
                let settlement_error = capacity_permit
                    .settle(IexHistCapacityDisposition::Failed)
                    .err();
                return Err(IexHistPublicationError::PostCommitRevalidation {
                    error,
                    receipt: Box::new(restart.publication),
                    settlement_error,
                });
            }
        };
        if !restart.validates_restart_lineage(&lineage) {
            let settlement_error = capacity_permit
                .settle(IexHistCapacityDisposition::Quarantined(
                    IexHistTerminalReason::DownstreamIntegrityFault,
                ))
                .err();
            return Err(IexHistPublicationError::InvalidReceipt {
                receipt: Box::new(restart.publication),
                settlement_error,
            });
        }
        let receipt = restart.publication;
        if let Err(error) = capacity_permit.settle(IexHistCapacityDisposition::Completed) {
            return Err(IexHistPublicationError::SettlementAfterCommit {
                error,
                receipt: Box::new(receipt),
            });
        }
        Ok(IexHistPublishedBars {
            plan,
            catalog_physical_seal,
            materialization,
            summary,
            telemetry,
            physical,
            receipt,
        })
    }
}

/// Borrowed, permit-free view supplied to the shared publication authority.
///
/// Keeping the lease out of this view prevents the shared publisher from independently settling
/// or duplicating the selected-file admission.
pub(crate) struct IexHistCanonicalPublicationInput<'a, R> {
    source_id: &'a SourceId,
    analytical_dataset: &'a DatasetId,
    plan: &'a ColdJobPlan,
    catalog_physical_seal: IexHistCatalogPhysicalSealEvidence,
    materialization: &'a PcapMaterializationReceipt,
    summary: &'a DecodeSummary,
    derived: &'a IexHistDerivedBarsHandoff,
    physical: &'a IexHistCompletePhysicalSeal<R>,
}

impl<'a, R> IexHistCanonicalPublicationInput<'a, R> {
    pub(crate) const fn source_id(&self) -> &'a SourceId {
        self.source_id
    }

    pub(crate) const fn analytical_dataset(&self) -> &'a DatasetId {
        self.analytical_dataset
    }

    pub(crate) const fn plan(&self) -> &'a ColdJobPlan {
        self.plan
    }

    pub(crate) const fn catalog_physical_seal(&self) -> IexHistCatalogPhysicalSealEvidence {
        self.catalog_physical_seal
    }

    pub(crate) const fn materialization(&self) -> &'a PcapMaterializationReceipt {
        self.materialization
    }

    pub(crate) const fn summary(&self) -> &'a DecodeSummary {
        self.summary
    }

    pub(crate) const fn derived(&self) -> &'a IexHistDerivedBarsHandoff {
        self.derived
    }

    pub(crate) const fn canonical_feed_identifier(&self) -> &'static str {
        canonical_feed_identifier(self.plan.selected_file().feed())
    }

    pub(crate) const fn canonical_interval_identifier(&self) -> &'static str {
        canonical_interval_identifier(self.derived.interval())
    }

    pub(crate) const fn physical(&self) -> &'a IexHistCompletePhysicalSeal<R> {
        self.physical
    }

    /// Returns the sole accepted identity of a sorted date-effective symbol mapping set.
    ///
    /// The shared publisher uses this before its atomic commit so the returned receipt cannot
    /// substitute an opaque, authority-chosen mapping digest for the exact mappings it publishes.
    pub(crate) fn expected_mapping_set_sha256(
        &self,
        instruments: &[IexHistPublishedInstrument],
    ) -> EvidenceDigest {
        published_mapping_set_identity(instruments)
    }

    /// Returns the exact raw/native/derived binding the shared publisher must persist.
    ///
    /// This identity includes every retained native event serialization and each bar's ordered
    /// contributing-event digest. The publisher may write its objects in any supported physical
    /// layout, but its durable lineage record and receipt must retain this value unchanged.
    pub(crate) fn expected_persisted_binding_sha256(
        &self,
        instruments: &[IexHistPublishedInstrument],
    ) -> EvidenceDigest {
        persisted_native_lineage_identity(self, instruments)
    }
}

/// Shared-authority seam for the existing raw/native/canonical publication transaction.
///
/// Implementations must use the common provider-capture store, Arrow converter, Parquet store,
/// manifest catalog, rights decision, and precommit authority. Returning `Ok` means the immutable
/// manifest and every exact raw/native parent are already durably committed; an implementation
/// must never substitute provider text for a date-effective canonical `InstrumentId`. The mapping
/// set and persisted native binding returned by the input must be written in that same atomic
/// commit and copied exactly into the immutable receipt.
#[async_trait]
pub(crate) trait IexHistCanonicalPublicationAuthority<R>:
    IexHistRestartLineageAuthority + Send + Sync
where
    R: IexHistSharedPhysicalSealReceipt + Send + Sync,
{
    async fn publish(
        &self,
        input: IexHistCanonicalPublicationInput<'_, R>,
        cancellation: CancellationToken,
    ) -> Result<IexHistImmutablePublicationReceipt, <Self as IexHistRestartLineageAuthority>::Error>;
}

/// One source symbol's verified date-effective canonical identity and exact published row count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IexHistPublishedInstrument {
    symbol: String,
    instrument_id: InstrumentId,
    bar_count: u64,
    first_effective_at: Timestamp,
    last_effective_at: Timestamp,
    mapping_evidence: EvidenceDigest,
}

impl IexHistPublishedInstrument {
    pub(crate) fn try_new(
        symbol: impl Into<String>,
        instrument_id: InstrumentId,
        bar_count: u64,
        first_effective_at: Timestamp,
        last_effective_at: Timestamp,
        mapping_evidence: EvidenceDigest,
    ) -> Result<Self, IexHistApplicationError> {
        let symbol = symbol.into();
        if symbol.is_empty()
            || symbol.len() > 64
            || bar_count == 0
            || first_effective_at > last_effective_at
            || !valid_sha256_evidence(mapping_evidence)
        {
            return Err(IexHistApplicationError::InvalidCanonicalPublicationReceipt);
        }
        Ok(Self {
            symbol,
            instrument_id,
            bar_count,
            first_effective_at,
            last_effective_at,
            mapping_evidence,
        })
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn bar_count(&self) -> u64 {
        self.bar_count
    }

    pub(crate) const fn effective_range(&self) -> (Timestamp, Timestamp) {
        (self.first_effective_at, self.last_effective_at)
    }

    pub(crate) const fn mapping_evidence(&self) -> EvidenceDigest {
        self.mapping_evidence
    }
}

/// Immutable shared-publication receipt bound to every IEX cold-job parent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IexHistImmutablePublicationReceipt {
    manifest: DatasetManifestRef,
    source_id: SourceId,
    venue_id: VenueId,
    trade_date: TradeDate,
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    interval: IexHistBarInterval,
    plan_sha256: Sha256Digest,
    catalog_seal_sha256: Sha256Digest,
    capture_receipt_sha256: Sha256Digest,
    physical_seal_sha256: Sha256Digest,
    decode_summary_sha256: Sha256Digest,
    provider_content_sha256: Sha256Digest,
    derived_handoff_sha256: Sha256Digest,
    mapping_set_sha256: EvidenceDigest,
    persisted_binding_sha256: EvidenceDigest,
    canonical_content_sha256: EvidenceDigest,
    locally_available_at: Timestamp,
    row_count: u64,
    instruments: Box<[IexHistPublishedInstrument]>,
    arrow_bytes: u64,
    parquet_bytes: u64,
    manifest_and_atomic_bytes: u64,
    receipt_sha256: EvidenceDigest,
}

impl IexHistImmutablePublicationReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "the shared receipt commits every immutable manifest, parent, clock, and byte coordinate"
    )]
    pub(crate) fn try_new(
        manifest: DatasetManifestRef,
        source_id: SourceId,
        venue_id: VenueId,
        trade_date: TradeDate,
        feed: FeedKind,
        feed_version: FeedVersion,
        transport_version: TransportVersion,
        interval: IexHistBarInterval,
        plan_sha256: Sha256Digest,
        catalog_seal_sha256: Sha256Digest,
        capture_receipt_sha256: Sha256Digest,
        physical_seal_sha256: Sha256Digest,
        decode_summary_sha256: Sha256Digest,
        provider_content_sha256: Sha256Digest,
        derived_handoff_sha256: Sha256Digest,
        mapping_set_sha256: EvidenceDigest,
        persisted_binding_sha256: EvidenceDigest,
        canonical_content_sha256: EvidenceDigest,
        locally_available_at: Timestamp,
        row_count: u64,
        instruments: Vec<IexHistPublishedInstrument>,
        arrow_bytes: u64,
        parquet_bytes: u64,
        manifest_and_atomic_bytes: u64,
    ) -> Result<Self, IexHistApplicationError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| IexHistApplicationError::InvalidCanonicalPublicationReceipt)?;
        if manifest.schema() != &canonical
            || venue_id.as_str() != "iex"
            || row_count == 0
            || instruments.is_empty()
            || arrow_bytes == 0
            || parquet_bytes == 0
            || manifest_and_atomic_bytes == 0
            || !valid_sha256_evidence(mapping_set_sha256)
            || mapping_set_sha256 != published_mapping_set_identity(&instruments)
            || !valid_sha256_evidence(persisted_binding_sha256)
            || !valid_sha256_evidence(canonical_content_sha256)
            || manifest.content_hash().bytes() != canonical_content_sha256.bytes()
            || !valid_iex_sha256(plan_sha256)
            || !valid_iex_sha256(catalog_seal_sha256)
            || !valid_iex_sha256(capture_receipt_sha256)
            || !valid_iex_sha256(physical_seal_sha256)
            || !valid_iex_sha256(decode_summary_sha256)
            || !valid_iex_sha256(provider_content_sha256)
            || !valid_iex_sha256(derived_handoff_sha256)
            || instruments.iter().any(|entry| entry.bar_count == 0)
            || instruments.windows(2).any(|pair| {
                pair[0].symbol >= pair[1].symbol || pair[0].instrument_id == pair[1].instrument_id
            })
            || instruments.iter().enumerate().any(|(index, entry)| {
                instruments[index + 1..]
                    .iter()
                    .any(|later| later.instrument_id == entry.instrument_id)
            })
            || instruments
                .iter()
                .try_fold(0_u64, |total, entry| total.checked_add(entry.bar_count))
                != Some(row_count)
        {
            return Err(IexHistApplicationError::InvalidCanonicalPublicationReceipt);
        }
        let instruments = instruments.into_boxed_slice();
        let receipt_sha256 = publication_receipt_identity(
            &manifest,
            &source_id,
            &venue_id,
            trade_date,
            feed,
            feed_version,
            transport_version,
            interval,
            plan_sha256,
            catalog_seal_sha256,
            capture_receipt_sha256,
            physical_seal_sha256,
            decode_summary_sha256,
            provider_content_sha256,
            derived_handoff_sha256,
            mapping_set_sha256,
            persisted_binding_sha256,
            canonical_content_sha256,
            locally_available_at,
            row_count,
            &instruments,
            arrow_bytes,
            parquet_bytes,
            manifest_and_atomic_bytes,
        );
        Ok(Self {
            manifest,
            source_id,
            venue_id,
            trade_date,
            feed,
            feed_version,
            transport_version,
            interval,
            plan_sha256,
            catalog_seal_sha256,
            capture_receipt_sha256,
            physical_seal_sha256,
            decode_summary_sha256,
            provider_content_sha256,
            derived_handoff_sha256,
            mapping_set_sha256,
            persisted_binding_sha256,
            canonical_content_sha256,
            locally_available_at,
            row_count,
            instruments,
            arrow_bytes,
            parquet_bytes,
            manifest_and_atomic_bytes,
            receipt_sha256,
        })
    }

    fn validates_against<R>(&self, handoff: &IexHistCanonicalPublicationHandoff<R>) -> bool {
        let selected = handoff.plan.selected_file();
        let bars = handoff.derived.bars();
        let publication_input = handoff.publication_input();
        let latest_completed_at = bars
            .iter()
            .map(|bar| bar.bucket_end_unix_nanos())
            .max()
            .map(Timestamp::from_unix_nanos);
        self.manifest.dataset_id() == &handoff.analytical_dataset
            && self.source_id == handoff.source_id
            && self.trade_date == selected.trade_date()
            && self.feed == selected.feed()
            && self.feed_version == selected.feed_version()
            && self.transport_version == selected.transport_version()
            && self.interval == handoff.derived.interval()
            && self.plan_sha256 == handoff.plan.plan_sha256()
            && self.catalog_seal_sha256 == handoff.plan_catalog_seal_sha256()
            && self.capture_receipt_sha256 == handoff.materialization.receipt_sha256()
            && self.physical_seal_sha256 == handoff.physical.seal_sha256()
            && self.decode_summary_sha256 == handoff.summary.summary_sha256()
            && self.provider_content_sha256 == handoff.derived.provider_content_sha256()
            && self.derived_handoff_sha256 == handoff.derived.handoff_sha256()
            && self.mapping_set_sha256
                == publication_input.expected_mapping_set_sha256(&self.instruments)
            && self.persisted_binding_sha256
                == publication_input.expected_persisted_binding_sha256(&self.instruments)
            && u64::try_from(bars.len()).ok() == Some(self.row_count)
            && self.locally_available_at
                >= Timestamp::from_unix_nanos(handoff.materialization.completed_at_unix_nanos())
            && latest_completed_at
                .is_some_and(|completed_at| self.locally_available_at >= completed_at)
            && self.instruments.iter().all(|entry| {
                let first = bars.iter().find(|bar| bar.symbol() == entry.symbol);
                let last = bars.iter().rev().find(|bar| bar.symbol() == entry.symbol);
                let count = bars
                    .iter()
                    .filter(|bar| bar.symbol() == entry.symbol)
                    .count();
                u64::try_from(count).ok() == Some(entry.bar_count)
                    && first.is_some_and(|bar| {
                        bar.bucket_start_unix_nanos() == entry.first_effective_at.unix_nanos()
                    })
                    && last.is_some_and(|bar| {
                        bar.bucket_start_unix_nanos() == entry.last_effective_at.unix_nanos()
                    })
            })
            && bars.iter().all(|bar| {
                self.instruments
                    .iter()
                    .any(|entry| entry.symbol == bar.symbol())
            })
            && publication_receipt_identity(
                &self.manifest,
                &self.source_id,
                &self.venue_id,
                self.trade_date,
                self.feed,
                self.feed_version,
                self.transport_version,
                self.interval,
                self.plan_sha256,
                self.catalog_seal_sha256,
                self.capture_receipt_sha256,
                self.physical_seal_sha256,
                self.decode_summary_sha256,
                self.provider_content_sha256,
                self.derived_handoff_sha256,
                self.mapping_set_sha256,
                self.persisted_binding_sha256,
                self.canonical_content_sha256,
                self.locally_available_at,
                self.row_count,
                &self.instruments,
                self.arrow_bytes,
                self.parquet_bytes,
                self.manifest_and_atomic_bytes,
            ) == self.receipt_sha256
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn locally_available_at(&self) -> Timestamp {
        self.locally_available_at
    }

    pub(crate) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) const fn instruments(&self) -> &[IexHistPublishedInstrument] {
        &self.instruments
    }

    pub(crate) const fn receipt_sha256(&self) -> EvidenceDigest {
        self.receipt_sha256
    }
}

impl<R> IexHistCanonicalPublicationHandoff<R> {
    fn plan_catalog_seal_sha256(&self) -> Sha256Digest {
        self.catalog_physical_seal.seal_sha256()
    }
}

fn catalog_seal_matches_plan(seal: IexHistCatalogPhysicalSealEvidence, plan: &ColdJobPlan) -> bool {
    let selected = plan.selected_file();
    let observation = selected.catalog_observation();
    seal.catalog_observation_receipt_sha256() == observation.receipt_sha256()
        && seal.storage_root_sha256() == observation.attempt().storage_root_sha256()
        && seal.object_sha256() == selected.catalog_sha256()
        && seal.object_bytes() == selected.catalog_bytes()
        && seal.object_bytes() != 0
        && seal
            .physical_receipt_sha256()
            .as_bytes()
            .iter()
            .any(|byte| *byte != 0)
}

/// Completed immutable IEX bar generation retaining its exact sealed raw parents.
pub(crate) struct IexHistPublishedBars<R> {
    plan: ColdJobPlan,
    catalog_physical_seal: IexHistCatalogPhysicalSealEvidence,
    materialization: PcapMaterializationReceipt,
    summary: DecodeSummary,
    telemetry: TransportTelemetry,
    physical: IexHistCompletePhysicalSeal<R>,
    receipt: IexHistImmutablePublicationReceipt,
}

impl<R> std::fmt::Debug for IexHistPublishedBars<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IexHistPublishedBars")
            .field("manifest", self.receipt.manifest())
            .field("receipt_sha256", &self.receipt.receipt_sha256())
            .field("plan_sha256", &self.plan.plan_sha256())
            .field("physical_seal_sha256", &self.physical.seal_sha256())
            .finish_non_exhaustive()
    }
}

impl<R> IexHistPublishedBars<R> {
    pub(crate) const fn receipt(&self) -> &IexHistImmutablePublicationReceipt {
        &self.receipt
    }

    pub(crate) const fn plan(&self) -> &ColdJobPlan {
        &self.plan
    }

    pub(crate) const fn materialization(&self) -> &PcapMaterializationReceipt {
        &self.materialization
    }

    pub(crate) const fn catalog_physical_seal(&self) -> IexHistCatalogPhysicalSealEvidence {
        self.catalog_physical_seal
    }

    pub(crate) const fn summary(&self) -> &DecodeSummary {
        &self.summary
    }

    pub(crate) const fn telemetry(&self) -> &TransportTelemetry {
        &self.telemetry
    }

    pub(crate) const fn physical(&self) -> &IexHistCompletePhysicalSeal<R> {
        &self.physical
    }

    pub(crate) fn restart_selector(&self) -> IexHistRestartSelector {
        IexHistRestartSelector {
            publication: self.receipt.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum IexHistDerivedPreparationError {
    Decode(market_squawk_adapter_iex_hist::DecodeError),
    Bars(IexHistDerivedBarError),
    NoEligibleTrades,
    InvalidHandoff,
}

/// Publication failure distinguishes a precommit refusal from a committed receipt failure.
#[derive(Debug)]
pub(crate) enum IexHistPublicationError<E> {
    Authority {
        error: E,
        settlement_error: Option<IexHistCapacityError>,
    },
    InvalidReceipt {
        receipt: Box<IexHistImmutablePublicationReceipt>,
        settlement_error: Option<IexHistCapacityError>,
    },
    /// The manifest commit returned, but its exact raw/native binding could not be reopened before
    /// the original selected-file capacity lease was released.
    PostCommitRevalidation {
        error: E,
        receipt: Box<IexHistImmutablePublicationReceipt>,
        settlement_error: Option<IexHistCapacityError>,
    },
    CapacityAfterCommit {
        error: IexHistCapacityError,
        receipt: Box<IexHistImmutablePublicationReceipt>,
        settlement_error: Option<IexHistCapacityError>,
    },
    SettlementAfterCommit {
        error: IexHistCapacityError,
        receipt: Box<IexHistImmutablePublicationReceipt>,
    },
}

fn valid_sha256_evidence(value: EvidenceDigest) -> bool {
    value.algorithm() == DigestAlgorithm::Sha256 && value.bytes().iter().any(|byte| *byte != 0)
}

fn valid_iex_sha256(value: Sha256Digest) -> bool {
    value.as_bytes().iter().any(|byte| *byte != 0)
}

fn published_mapping_set_identity(instruments: &[IexHistPublishedInstrument]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/iex-hist-published-mapping-set/v1");
    hash.update(
        u64::try_from(instruments.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for instrument in instruments {
        hash_length_prefixed(&mut hash, instrument.symbol.as_bytes());
        hash.update(instrument.instrument_id.as_uuid().as_bytes());
        hash.update(instrument.bar_count.to_le_bytes());
        hash.update(instrument.first_effective_at.unix_nanos().to_le_bytes());
        hash.update(instrument.last_effective_at.unix_nanos().to_le_bytes());
        hash.update(instrument.mapping_evidence.bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn persisted_native_lineage_identity<R>(
    input: &IexHistCanonicalPublicationInput<'_, R>,
    instruments: &[IexHistPublishedInstrument],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/iex-hist-persisted-native-lineage/v1");
    hash_length_prefixed(&mut hash, input.source_id.as_str().as_bytes());
    hash_length_prefixed(&mut hash, input.analytical_dataset.as_str().as_bytes());
    for digest in [
        input.plan.plan_sha256(),
        input.catalog_physical_seal.seal_sha256(),
        input.materialization.receipt_sha256(),
        input.summary.summary_sha256(),
        input.physical.seal_sha256(),
        input.derived.source().provider_content_sha256(),
        input.derived.source().physical_evidence_sha256(),
        input.derived.calculation_sha256(),
        input.derived.provider_content_sha256(),
        input.derived.handoff_sha256(),
    ] {
        hash.update(digest.as_bytes());
    }
    hash.update([bar_interval_identity_tag(input.derived.interval())]);
    hash.update(published_mapping_set_identity(instruments).bytes());
    let events = input.derived.source().events();
    hash.update(
        u64::try_from(events.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for event in events {
        hash.update(event.ordinal().to_le_bytes());
        hash.update(
            u64::try_from(event.native_serialized_bytes().len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hash.update(event.native_serialized_sha256().as_bytes());
        hash.update(event.provider_content_sha256().as_bytes());
    }
    let bars = input.derived.bars();
    hash.update(u64::try_from(bars.len()).unwrap_or(u64::MAX).to_le_bytes());
    for bar in bars {
        hash_length_prefixed(&mut hash, bar.symbol().as_bytes());
        hash.update(bar.bucket_start_unix_nanos().to_le_bytes());
        hash.update(bar.bucket_end_unix_nanos().to_le_bytes());
        hash.update(bar.bar_sha256().as_bytes());
        hash.update(bar.contributing_event_count().to_le_bytes());
        hash.update(bar.contributing_events_sha256().as_bytes());
        hash.update(bar.source_provider_content_sha256().as_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_length_prefixed(hash: &mut Sha256, value: &[u8]) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    hash.update(value);
}

#[allow(
    clippy::too_many_arguments,
    reason = "publication identity intentionally commits every immutable parent and actual-byte field"
)]
fn publication_receipt_identity(
    manifest: &DatasetManifestRef,
    source_id: &SourceId,
    venue_id: &VenueId,
    trade_date: TradeDate,
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    interval: IexHistBarInterval,
    plan_sha256: Sha256Digest,
    catalog_seal_sha256: Sha256Digest,
    capture_receipt_sha256: Sha256Digest,
    physical_seal_sha256: Sha256Digest,
    decode_summary_sha256: Sha256Digest,
    provider_content_sha256: Sha256Digest,
    derived_handoff_sha256: Sha256Digest,
    mapping_set_sha256: EvidenceDigest,
    persisted_binding_sha256: EvidenceDigest,
    canonical_content_sha256: EvidenceDigest,
    locally_available_at: Timestamp,
    row_count: u64,
    instruments: &[IexHistPublishedInstrument],
    arrow_bytes: u64,
    parquet_bytes: u64,
    manifest_and_atomic_bytes: u64,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/iex-hist-canonical-publication/v1");
    hash.update(manifest.dataset_id().as_str().as_bytes());
    hash.update(manifest.manifest_version().to_le_bytes());
    hash.update(manifest.schema().name().as_bytes());
    hash.update(manifest.schema_version().get().to_le_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
    hash.update(source_id.to_string().as_bytes());
    hash.update(venue_id.to_string().as_bytes());
    hash.update(trade_date.compact().as_bytes());
    hash.update([feed_identity_tag(feed)]);
    hash.update([feed_version_identity_tag(feed_version)]);
    hash.update([transport_version_identity_tag(transport_version)]);
    hash.update([bar_interval_identity_tag(interval)]);
    for digest in [
        plan_sha256,
        catalog_seal_sha256,
        capture_receipt_sha256,
        physical_seal_sha256,
        decode_summary_sha256,
        provider_content_sha256,
        derived_handoff_sha256,
    ] {
        hash.update(digest.as_bytes());
    }
    hash.update(mapping_set_sha256.bytes());
    hash.update(persisted_binding_sha256.bytes());
    hash.update(canonical_content_sha256.bytes());
    hash.update(locally_available_at.unix_nanos().to_le_bytes());
    hash.update(row_count.to_le_bytes());
    hash.update(
        u64::try_from(instruments.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for instrument in instruments {
        hash.update(instrument.symbol.as_bytes());
        hash.update(instrument.instrument_id.as_uuid().as_bytes());
        hash.update(instrument.bar_count.to_le_bytes());
        hash.update(instrument.first_effective_at.unix_nanos().to_le_bytes());
        hash.update(instrument.last_effective_at.unix_nanos().to_le_bytes());
        hash.update(instrument.mapping_evidence.bytes());
    }
    hash.update(arrow_bytes.to_le_bytes());
    hash.update(parquet_bytes.to_le_bytes());
    hash.update(manifest_and_atomic_bytes.to_le_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

const fn feed_identity_tag(value: FeedKind) -> u8 {
    match value {
        FeedKind::Tops => 1,
        FeedKind::Deep => 2,
        FeedKind::DeepPlusDpls => 3,
        FeedKind::DeepPlusDplc => 4,
    }
}

const fn canonical_feed_identifier(value: FeedKind) -> &'static str {
    match value {
        FeedKind::Tops => "iex-hist-tops",
        FeedKind::Deep => "iex-hist-deep",
        FeedKind::DeepPlusDpls => "iex-hist-dpls",
        FeedKind::DeepPlusDplc => "iex-hist-dplc",
    }
}

const fn canonical_interval_identifier(value: IexHistBarInterval) -> &'static str {
    match value {
        IexHistBarInterval::OneMinute => "one_minute_utc",
    }
}

const fn feed_version_identity_tag(value: FeedVersion) -> u8 {
    match value {
        FeedVersion::Tops1_6 => 1,
        FeedVersion::Deep1_0 => 2,
        FeedVersion::DeepPlusDpls1_0 => 3,
        FeedVersion::DeepPlusDplc1 => 4,
    }
}

const fn transport_version_identity_tag(value: TransportVersion) -> u8 {
    match value {
        TransportVersion::IexTp1 => 1,
    }
}

const fn bar_interval_identity_tag(value: IexHistBarInterval) -> u8 {
    match value {
        IexHistBarInterval::OneMinute => 1,
    }
}

/// Catalog evidence proving restart reopened the exact IEX raw/native parent binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IexHistRestartLineageEvidence {
    manifest: DatasetManifestRef,
    publication_receipt_sha256: EvidenceDigest,
    persisted_binding_sha256: EvidenceDigest,
    source_id: SourceId,
    venue_id: VenueId,
    catalog_seal_sha256: Sha256Digest,
    physical_seal_sha256: Sha256Digest,
    plan_sha256: Sha256Digest,
    capture_receipt_sha256: Sha256Digest,
    decode_summary_sha256: Sha256Digest,
    provider_content_sha256: Sha256Digest,
    derived_handoff_sha256: Sha256Digest,
    mapping_set_sha256: EvidenceDigest,
    canonical_content_sha256: EvidenceDigest,
    locally_available_at: Timestamp,
    row_count: u64,
}

impl IexHistRestartLineageEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "restart evidence commits every catalog-revalidated IEX lineage coordinate"
    )]
    pub(crate) fn try_new(
        manifest: DatasetManifestRef,
        publication_receipt_sha256: EvidenceDigest,
        persisted_binding_sha256: EvidenceDigest,
        source_id: SourceId,
        venue_id: VenueId,
        catalog_seal_sha256: Sha256Digest,
        physical_seal_sha256: Sha256Digest,
        plan_sha256: Sha256Digest,
        capture_receipt_sha256: Sha256Digest,
        decode_summary_sha256: Sha256Digest,
        provider_content_sha256: Sha256Digest,
        derived_handoff_sha256: Sha256Digest,
        mapping_set_sha256: EvidenceDigest,
        canonical_content_sha256: EvidenceDigest,
        locally_available_at: Timestamp,
        row_count: u64,
    ) -> Result<Self, IexHistApplicationError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| IexHistApplicationError::InvalidCanonicalPublicationReceipt)?;
        if manifest.schema() != &canonical
            || row_count == 0
            || !valid_sha256_evidence(publication_receipt_sha256)
            || !valid_sha256_evidence(persisted_binding_sha256)
            || !valid_sha256_evidence(mapping_set_sha256)
            || !valid_sha256_evidence(canonical_content_sha256)
            || manifest.content_hash().bytes() != canonical_content_sha256.bytes()
            || venue_id.as_str() != "iex"
            || !valid_iex_sha256(catalog_seal_sha256)
            || !valid_iex_sha256(physical_seal_sha256)
            || !valid_iex_sha256(plan_sha256)
            || !valid_iex_sha256(capture_receipt_sha256)
            || !valid_iex_sha256(decode_summary_sha256)
            || !valid_iex_sha256(provider_content_sha256)
            || !valid_iex_sha256(derived_handoff_sha256)
        {
            return Err(IexHistApplicationError::InvalidCanonicalPublicationReceipt);
        }
        Ok(Self {
            manifest,
            publication_receipt_sha256,
            persisted_binding_sha256,
            source_id,
            venue_id,
            catalog_seal_sha256,
            physical_seal_sha256,
            plan_sha256,
            capture_receipt_sha256,
            decode_summary_sha256,
            provider_content_sha256,
            derived_handoff_sha256,
            mapping_set_sha256,
            canonical_content_sha256,
            locally_available_at,
            row_count,
        })
    }
}

/// Shared catalog authority needed to prove restart lineage independently of local coordinates.
pub(crate) trait IexHistRestartLineageAuthority {
    type Error: Send;

    fn revalidate(
        &self,
        selector: &IexHistRestartSelector,
    ) -> Result<IexHistRestartLineageEvidence, Self::Error>;
}

/// Exact immutable manifest and raw/native coordinates for restart-safe PIT reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IexHistRestartSelector {
    publication: IexHistImmutablePublicationReceipt,
}

impl IexHistRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.publication.manifest()
    }

    pub(crate) const fn publication_receipt_sha256(&self) -> EvidenceDigest {
        self.publication.receipt_sha256()
    }

    /// Revalidates the persisted raw/native parent, then runs the existing exact-manifest typed
    /// market-bar reader for one admitted canonical instrument and knowledge cutoff.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, PIT range, query ceilings, cancellation, and deadline stay explicit"
    )]
    pub(crate) async fn reopen<A: IexHistRestartLineageAuthority>(
        &self,
        authority: &A,
        research: &ResearchService,
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        read_limit: AnalyticalMarketBarReadLimit,
        query_limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<IexHistRestartReceipt, IexHistRestartError<A::Error>> {
        if knowledge_cutoff < self.publication.locally_available_at {
            return Err(IexHistRestartError::NotYetAvailable);
        }
        let instrument = self
            .publication
            .instruments
            .iter()
            .find(|entry| entry.instrument_id == instrument_id)
            .ok_or(IexHistRestartError::InstrumentNotPublished)?;
        let expected_rows = instrument.bar_count;
        let effective_range = MarketBarEffectiveRange::try_new(
            instrument.first_effective_at,
            instrument.last_effective_at,
        )
        .map_err(IexHistRestartError::Analytical)?;
        let lineage = authority
            .revalidate(self)
            .map_err(IexHistRestartError::Authority)?;
        if !self.validates_restart_lineage(&lineage) {
            return Err(IexHistRestartError::LineageMismatch);
        }
        let request = AnalyticalMarketBarReadRequest::try_new(
            self.publication.manifest.clone(),
            instrument_id,
            knowledge_cutoff,
            Some(effective_range),
            read_limit,
        )
        .map_err(IexHistRestartError::Analytical)?;
        let bars = research
            .analytical_reader()
            .read_market_bars(request, query_limits, deadline, cancellation)
            .await
            .map_err(IexHistRestartError::Analytical)?;
        if bars.source_id() != &self.publication.source_id
            || bars.output().manifest() != &self.publication.manifest
            || u64::try_from(bars.bars().len()).ok() != Some(expected_rows)
            || bars.bars().iter().any(|bar| {
                bar.context().provenance().venue_id() != Some(&self.publication.venue_id)
                    || bar.feed().as_str() != canonical_feed_identifier(self.publication.feed)
                    || bar.interval().as_str()
                        != canonical_interval_identifier(self.publication.interval)
            })
        {
            return Err(IexHistRestartError::TypedReadMismatch);
        }
        let history_handoff_sha256 = neutral_history_handoff_identity(
            &self.publication,
            &lineage,
            instrument_id,
            knowledge_cutoff,
            effective_range,
            &bars,
        );
        Ok(IexHistRestartReceipt {
            instrument_id,
            knowledge_cutoff,
            effective_range,
            history_handoff_sha256,
            lineage,
            bars,
        })
    }

    fn validates_restart_lineage(&self, evidence: &IexHistRestartLineageEvidence) -> bool {
        evidence.manifest == self.publication.manifest
            && evidence.publication_receipt_sha256 == self.publication.receipt_sha256
            && evidence.persisted_binding_sha256 == self.publication.persisted_binding_sha256
            && evidence.source_id == self.publication.source_id
            && evidence.venue_id == self.publication.venue_id
            && evidence.catalog_seal_sha256 == self.publication.catalog_seal_sha256
            && evidence.physical_seal_sha256 == self.publication.physical_seal_sha256
            && evidence.plan_sha256 == self.publication.plan_sha256
            && evidence.capture_receipt_sha256 == self.publication.capture_receipt_sha256
            && evidence.decode_summary_sha256 == self.publication.decode_summary_sha256
            && evidence.provider_content_sha256 == self.publication.provider_content_sha256
            && evidence.derived_handoff_sha256 == self.publication.derived_handoff_sha256
            && evidence.mapping_set_sha256 == self.publication.mapping_set_sha256
            && evidence.canonical_content_sha256 == self.publication.canonical_content_sha256
            && evidence.locally_available_at == self.publication.locally_available_at
            && evidence.row_count == self.publication.row_count
    }
}

/// Raw/native restart evidence plus a provider-neutral, manifest-pinned typed history handoff.
#[derive(Debug)]
pub(crate) struct IexHistRestartReceipt {
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    effective_range: MarketBarEffectiveRange,
    history_handoff_sha256: EvidenceDigest,
    lineage: IexHistRestartLineageEvidence,
    bars: AnalyticalMarketBarOutput,
}

impl IexHistRestartReceipt {
    /// Returns the stable canonical instrument selected for this history.
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the inclusive point-in-time knowledge cutoff used by the exact read.
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the exact inclusive effective-time range delivered to research consumers.
    pub(crate) const fn effective_range(&self) -> MarketBarEffectiveRange {
        self.effective_range
    }

    /// Returns the opaque immutable evidence identity for feature/backtest input pinning.
    pub(crate) const fn history_handoff_sha256(&self) -> EvidenceDigest {
        self.history_handoff_sha256
    }

    /// Returns diagnostic raw/native restart evidence. Ordinary feature and backtest consumers do
    /// not need to interpret these provider coordinates.
    pub(crate) const fn lineage(&self) -> &IexHistRestartLineageEvidence {
        &self.lineage
    }

    /// Returns the existing provider-neutral analytical market-bar output.
    pub(crate) const fn bars(&self) -> &AnalyticalMarketBarOutput {
        &self.bars
    }

    /// Consumes the restart receipt into a provider-neutral history handoff for feature
    /// construction or governed-backtest input preparation.
    ///
    /// Governed backtests still require the existing canonical feature-label publication and
    /// pinned-input authority; this handoff deliberately does not bypass either boundary.
    pub(crate) fn into_neutral_history(self) -> NeutralMarketBarHistoryHandoff {
        NeutralMarketBarHistoryHandoff {
            instrument_id: self.instrument_id,
            knowledge_cutoff: self.knowledge_cutoff,
            effective_range: self.effective_range,
            history_handoff_sha256: self.history_handoff_sha256,
            bars: self.bars,
        }
    }
}

/// Provider-neutral typed history and opaque lineage evidence for downstream research work.
#[derive(Debug)]
pub(crate) struct NeutralMarketBarHistoryHandoff {
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    effective_range: MarketBarEffectiveRange,
    history_handoff_sha256: EvidenceDigest,
    bars: AnalyticalMarketBarOutput,
}

impl NeutralMarketBarHistoryHandoff {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn effective_range(&self) -> MarketBarEffectiveRange {
        self.effective_range
    }

    /// Returns the raw/native/publication/query evidence identity downstream derived datasets
    /// should retain as an input parent.
    pub(crate) const fn history_handoff_sha256(&self) -> EvidenceDigest {
        self.history_handoff_sha256
    }

    pub(crate) const fn bars(&self) -> &AnalyticalMarketBarOutput {
        &self.bars
    }

    pub(crate) fn into_bars(self) -> AnalyticalMarketBarOutput {
        self.bars
    }
}

fn neutral_history_handoff_identity(
    publication: &IexHistImmutablePublicationReceipt,
    lineage: &IexHistRestartLineageEvidence,
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    effective_range: MarketBarEffectiveRange,
    bars: &AnalyticalMarketBarOutput,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/neutral-market-bar-history-handoff/v1");
    hash.update(publication.receipt_sha256.bytes());
    hash.update(lineage.persisted_binding_sha256.bytes());
    hash.update(lineage.canonical_content_sha256.bytes());
    hash.update(instrument_id.as_uuid().as_bytes());
    hash.update(knowledge_cutoff.unix_nanos().to_le_bytes());
    hash.update(effective_range.start().unix_nanos().to_le_bytes());
    hash.update(effective_range.end().unix_nanos().to_le_bytes());
    hash.update(bars.output().object_graph_digest().bytes());
    hash.update(bars.output().query_identity().bytes());
    hash.update(bars.output().result_digest().bytes());
    hash.update(
        u64::try_from(bars.bars().len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

/// Exact restart/PIT failure; pre-availability absence is distinct from integrity failure.
#[derive(Debug)]
pub(crate) enum IexHistRestartError<E> {
    Authority(E),
    NotYetAvailable,
    InstrumentNotPublished,
    LineageMismatch,
    TypedReadMismatch,
    Analytical(AnalyticalReadError),
}

fn validate_application_plan(plan: &ColdJobPlan) -> Result<(), IexHistApplicationError> {
    if plan.lane() != ScheduleLane::Cold
        || plan.automatic_archive_catch_up()
        || plan.max_parallel_transfers() != 1
        || !matches!(
            plan.trigger(),
            ColdJobTrigger::Operator | ColdJobTrigger::ResearchJob
        )
        || plan.selected_file().feed_version().feed() != plan.selected_file().feed()
        || plan.decode_contract().feed() != plan.selected_file().feed()
        || plan.decode_contract().feed_version() != plan.selected_file().feed_version()
        || plan.decode_contract().transport_version() != plan.selected_file().transport_version()
        || plan.required_disk_bytes()? == 0
    {
        return Err(IexHistApplicationError::InvalidApplicationPlan);
    }
    Ok(())
}

/// Exact selected-file identity exposed to an operator/research job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IexHistSelectionStatus {
    plan_sha256: Sha256Digest,
    catalog_sha256: Sha256Digest,
    catalog_received_at: Timestamp,
    descriptor_sha256: Sha256Digest,
    trade_date: TradeDate,
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    object_encoding: PcapObjectEncoding,
    file_name: String,
    authority: IexHistJobAuthority,
    lane: IexHistApplicationLane,
    required_disk_bytes: u64,
}

impl IexHistSelectionStatus {
    fn from_plan(plan: &ColdJobPlan) -> Result<Self, IexHistApplicationError> {
        validate_application_plan(plan)?;
        let selected = plan.selected_file();
        Ok(Self {
            plan_sha256: plan.plan_sha256(),
            catalog_sha256: selected.catalog_sha256(),
            catalog_received_at: Timestamp::from_unix_nanos(
                selected.catalog_retrieved_at_unix_nanos(),
            ),
            descriptor_sha256: selected.descriptor_sha256(),
            trade_date: selected.trade_date(),
            feed: selected.feed(),
            feed_version: selected.feed_version(),
            transport_version: selected.transport_version(),
            object_encoding: selected.object_encoding(),
            file_name: selected.file_name().to_owned(),
            authority: match plan.trigger() {
                ColdJobTrigger::Operator => IexHistJobAuthority::Operator,
                ColdJobTrigger::ResearchJob => IexHistJobAuthority::ResearchJob,
            },
            lane: IexHistApplicationLane::ColdHistoricalResearch,
            required_disk_bytes: plan.required_disk_bytes()?,
        })
    }

    pub(crate) const fn plan_sha256(&self) -> Sha256Digest {
        self.plan_sha256
    }

    pub(crate) const fn catalog_sha256(&self) -> Sha256Digest {
        self.catalog_sha256
    }

    /// Local catalog receipt clock, not provider event time or analytical availability.
    pub(crate) const fn catalog_received_at(&self) -> Timestamp {
        self.catalog_received_at
    }

    pub(crate) const fn descriptor_sha256(&self) -> Sha256Digest {
        self.descriptor_sha256
    }

    pub(crate) const fn trade_date(&self) -> TradeDate {
        self.trade_date
    }

    pub(crate) const fn feed(&self) -> FeedKind {
        self.feed
    }

    pub(crate) const fn feed_version(&self) -> FeedVersion {
        self.feed_version
    }

    pub(crate) const fn transport_version(&self) -> TransportVersion {
        self.transport_version
    }

    pub(crate) const fn object_encoding(&self) -> PcapObjectEncoding {
        self.object_encoding
    }

    pub(crate) fn file_name(&self) -> &str {
        &self.file_name
    }

    pub(crate) const fn authority(&self) -> IexHistJobAuthority {
        self.authority
    }

    pub(crate) const fn lane(&self) -> IexHistApplicationLane {
        self.lane
    }

    pub(crate) const fn required_disk_bytes(&self) -> u64 {
        self.required_disk_bytes
    }
}

/// Local clocks and provider event-time bounds retained without conflation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IexHistClockStatus {
    catalog_received_at: Timestamp,
    response_started_at: Option<Timestamp>,
    raw_completed_at: Option<Timestamp>,
    first_provider_event_at_unix_nanos: Option<i64>,
    last_provider_event_at_unix_nanos: Option<i64>,
    locally_available_at: Option<Timestamp>,
}

impl IexHistClockStatus {
    pub(crate) const fn catalog_received_at(self) -> Timestamp {
        self.catalog_received_at
    }

    pub(crate) const fn response_started_at(self) -> Option<Timestamp> {
        self.response_started_at
    }

    pub(crate) const fn raw_completed_at(self) -> Option<Timestamp> {
        self.raw_completed_at
    }

    pub(crate) const fn provider_event_bounds_unix_nanos(self) -> Option<(i64, i64)> {
        match (
            self.first_provider_event_at_unix_nanos,
            self.last_provider_event_at_unix_nanos,
        ) {
            (Some(first), Some(last)) => Some((first, last)),
            _ => None,
        }
    }

    /// Always `None` until an immutable canonical generation and manifest are durably published.
    pub(crate) const fn locally_available_at(self) -> Option<Timestamp> {
        self.locally_available_at
    }
}

/// Provider-local progress plus the closed application publication result.
#[derive(Clone, Debug)]
pub(crate) struct IexHistJobStatus {
    selection: IexHistSelectionStatus,
    checkpoint_version: u64,
    provider_phase: IexHistJobPhase,
    recovery_action: IexHistRecoveryAction,
    capture: Option<PcapMaterializationReceipt>,
    decode: Option<DecodeSummary>,
    terminal: Option<IexHistTerminalEvidence>,
    clocks: IexHistClockStatus,
    identity: IexHistInstrumentIdentityStatus,
    availability: IexHistPublicationAvailability,
}

impl IexHistJobStatus {
    fn from_job<S: IexHistCheckpointStore>(
        job: &IexHistDurableJob<S>,
    ) -> Result<Self, IexHistApplicationError> {
        let selection = IexHistSelectionStatus::from_plan(job.plan())?;
        let capture = job.capture_evidence().cloned();
        let decode = job.decode_evidence().cloned();
        let terminal = job.terminal_evidence().cloned();
        let clocks = IexHistClockStatus {
            catalog_received_at: selection.catalog_received_at,
            response_started_at: capture.as_ref().map(|receipt| {
                Timestamp::from_unix_nanos(receipt.response_started_at_unix_nanos())
            }),
            raw_completed_at: capture
                .as_ref()
                .map(|receipt| Timestamp::from_unix_nanos(receipt.completed_at_unix_nanos())),
            first_provider_event_at_unix_nanos: decode
                .as_ref()
                .map(|summary| summary.first_source_time.value()),
            last_provider_event_at_unix_nanos: decode
                .as_ref()
                .map(|summary| summary.last_source_time.value()),
            locally_available_at: None,
        };
        Ok(Self {
            selection,
            checkpoint_version: job.state_version(),
            provider_phase: job.phase(),
            recovery_action: job.recovery_action(),
            capture,
            decode,
            terminal,
            clocks,
            identity: IexHistInstrumentIdentityStatus::Unavailable(
                IexHistInstrumentIdentityBlocker::CanonicalProviderSymbolResolutionMissing,
            ),
            availability: IexHistPublicationAvailability::Unavailable(
                IexHistPublicationBlockers::current(),
            ),
        })
    }

    pub(crate) const fn selection(&self) -> &IexHistSelectionStatus {
        &self.selection
    }

    pub(crate) const fn checkpoint_version(&self) -> u64 {
        self.checkpoint_version
    }

    pub(crate) const fn provider_phase(&self) -> IexHistJobPhase {
        self.provider_phase
    }

    pub(crate) const fn recovery_action(&self) -> IexHistRecoveryAction {
        self.recovery_action
    }

    pub(crate) const fn capture(&self) -> Option<&PcapMaterializationReceipt> {
        self.capture.as_ref()
    }

    /// Retains exact feed/transport implementations, per-channel session, terminal sequence and
    /// stream-offset continuity, and first/last provider event clocks.
    pub(crate) const fn decode(&self) -> Option<&DecodeSummary> {
        self.decode.as_ref()
    }

    pub(crate) const fn terminal(&self) -> Option<&IexHistTerminalEvidence> {
        self.terminal.as_ref()
    }

    pub(crate) const fn clocks(&self) -> IexHistClockStatus {
        self.clocks
    }

    pub(crate) const fn instrument_identity(&self) -> IexHistInstrumentIdentityStatus {
        self.identity
    }

    pub(crate) const fn availability(&self) -> IexHistPublicationAvailability {
        self.availability
    }
}

/// Instrument identity is intentionally closed rather than falling back to ticker text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IexHistInstrumentIdentityStatus {
    Unavailable(IexHistInstrumentIdentityBlocker),
}

/// Exact missing identity authority for canonical IEX events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IexHistInstrumentIdentityBlocker {
    /// The shared reference catalog has no date-effective IEX provider-symbol-to-InstrumentId
    /// resolver bound to this exact feed/date generation.
    CanonicalProviderSymbolResolutionMissing,
}

/// Closed product availability. There is deliberately no `Available` variant in the current
/// application contract, so provider-local success cannot escape as a dashboard/model claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IexHistPublicationAvailability {
    Unavailable(IexHistPublicationBlockers),
}

/// Exact release-owned dependencies that must all be closed before availability exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IexHistPublicationBlockers {
    blockers: [IexHistPublicationBlocker; 3],
}

impl IexHistPublicationBlockers {
    const fn current() -> Self {
        Self {
            blockers: [
                IexHistPublicationBlocker::IexNativeLineageSchema,
                IexHistPublicationBlocker::InstrumentIdCanonicalMapper,
                IexHistPublicationBlocker::ImmutableCanonicalGenerationPublisher,
            ],
        }
    }

    pub(crate) const fn as_slice(&self) -> &[IexHistPublicationBlocker] {
        &self.blockers
    }
}

/// One exact shared application/data dependency, with no permissions or entitlement inference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IexHistPublicationBlocker {
    /// Shared provider-native lineage does not yet admit the IEX HIST decoder schema.
    IexNativeLineageSchema,
    /// Every provider symbol must resolve to a date-effective internal InstrumentId; ticker-only
    /// identity is forbidden.
    InstrumentIdCanonicalMapper,
    /// No IEX-qualified Arrow-to-immutable-Parquet publisher exists.
    ImmutableCanonicalGenerationPublisher,
}

/// Closed application-leaf failure.
#[derive(Debug, Error)]
pub(crate) enum IexHistApplicationError {
    #[error("IEX HIST application plan violates cold explicit-selection policy")]
    InvalidApplicationPlan,
    #[error("IEX HIST physical-seal handoff does not match exact adapter evidence")]
    InvalidPhysicalHandoff,
    #[error("IEX HIST canonical publication receipt is invalid")]
    InvalidCanonicalPublicationReceipt,
    #[error("IEX HIST derived-bar preparation failed")]
    DerivedPreparation {
        error: IexHistDerivedPreparationError,
        settlement_error: Option<IexHistCapacityError>,
    },
    #[error(transparent)]
    Decode(#[from] market_squawk_adapter_iex_hist::DecodeError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Checkpoint(#[from] IexHistCheckpointError),
    #[error(transparent)]
    CompleteSeal(#[from] IexHistCompleteSealError),
    #[error(transparent)]
    Transport(#[from] IexHistTransportError),
}
