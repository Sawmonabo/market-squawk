//! Application-owned explicit IEX HIST research-job composition.
//!
//! This leaf deliberately stops at the exact boundary the current shared layer can support. It
//! admits only one operator- or research-selected feed/date object on the adapter's cold lane,
//! preserves the adapter's durable capacity/checkpoint authorities, and exposes one-use physical
//! sealing handoffs. It cannot report product availability: the repository does not yet provide a
//! non-forgeable complete-artifact seal for the catalog/provider object/expanded PCAP, an
//! IEX-native canonical mapping keyed by [`market_squawk_domain::InstrumentId`], or an immutable generation plus
//! point-in-time/restart selector.
//!
//! Nothing in this module schedules an archive, treats IEX venue history as live data, or upgrades
//! it to SIP, NBBO, consolidated, or market-wide evidence.

use std::sync::Arc;

use market_squawk_adapter_iex_hist::{
    ByteAdmissionLimits, Catalog, CatalogError, CatalogFetch, ColdJobPlan, ColdJobTrigger,
    DecodeLimits, DecodeSummary, ExactFileRequest, FeedKind, FeedVersion, IexHistCapacityAuthority,
    IexHistCheckpointError, IexHistCheckpointStore, IexHistColdTransport,
    IexHistDplcDistributionAuthority, IexHistDurableJob, IexHistJobPhase, IexHistPlanner,
    IexHistRecoveryAction, IexHistTerminalEvidence, IexHistTransportError, MaterializedIexCapture,
    PcapMaterializationReceipt, PcapObjectEncoding, PlanError, ScheduleLane, Sha256Digest,
    TradeDate, TransportVersion,
};
use market_squawk_domain::Timestamp;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

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
        let receipt = fetch.catalog().receipt();
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

    pub(crate) const fn catalog(&self) -> &Catalog {
        self.fetch.catalog()
    }

    pub(crate) fn exact_body(&self) -> &[u8] {
        self.fetch.exact_body()
    }

    pub(crate) const fn physical_requirement(&self) -> &IexHistPhysicalSealRequirement {
        &self.requirement
    }

    /// Releases the complete one-use adapter handoff only to the future shared physical-seal
    /// integration owner. That owner must seal the exact body before settling its retained permit.
    pub(crate) fn into_shared_sealer_handoff(self) -> CatalogFetch {
        self.fetch
    }
}

/// One exact selected-file candidate. It is not execution authority: the parent catalog still has
/// to cross the shared physical-seal boundary before the plan may be opened as a durable job.
#[derive(Clone, Debug)]
pub(crate) struct IexHistExactJobPreview {
    plan: ColdJobPlan,
}

impl IexHistExactJobPreview {
    pub(crate) const fn plan(&self) -> &ColdJobPlan {
        &self.plan
    }

    pub(crate) fn status(&self) -> Result<IexHistSelectionStatus, IexHistApplicationError> {
        IexHistSelectionStatus::from_plan(&self.plan)
    }

    /// Transfers the exact immutable plan to the shared catalog-seal integration owner. This leaf
    /// intentionally has no method that turns the preview directly into a runnable job.
    pub(crate) fn into_plan_for_sealed_catalog(self) -> ColdJobPlan {
        self.plan
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
        capture: Box<MaterializedIexCapture>,
    ) -> Result<Self, IexHistApplicationError> {
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

    /// Releases the one-use files and permit only to the future shared complete-artifact sealer.
    pub(crate) fn into_shared_sealer_handoff(self) -> Box<MaterializedIexCapture> {
        self.capture
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

    /// Produces a non-runnable preview for exactly one descriptor in one exact catalog generation.
    /// The adapter enforces cold-only, one transfer, no automatic catch-up, T+1/window admission,
    /// and complete network/temp/durable/Arrow/Parquet/manifest/free-reserve accounting.
    pub(crate) fn preview_exact_job(
        &self,
        catalog: &Catalog,
        request: IexHistExplicitJobRequest,
        dplc_distribution: Option<&dyn IexHistDplcDistributionAuthority>,
    ) -> Result<IexHistExactJobPreview, IexHistApplicationError> {
        let selected = catalog.select(&request.selection)?;
        let plan = IexHistPlanner::plan(
            selected,
            request.authority.into(),
            request.byte_limits,
            request.decode_limits,
            dplc_distribution,
        )?;
        validate_application_plan(&plan)?;
        Ok(IexHistExactJobPreview { plan })
    }

    /// Opens or restores the adapter's provider-local durable checkpoint after the shared catalog
    /// sealer has accepted the exact parent bytes. No parallel checkpoint store is introduced.
    pub(crate) fn open_checkpoint<S: IexHistCheckpointStore>(
        &self,
        sealed_catalog_plan: &ColdJobPlan,
        store: S,
    ) -> Result<IexHistDurableJob<S>, IexHistApplicationError> {
        validate_application_plan(sealed_catalog_plan)?;
        IexHistDurableJob::try_open(sealed_catalog_plan, store).map_err(Into::into)
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
    pub(crate) fn require_capture_seal(
        &self,
        capture: Box<MaterializedIexCapture>,
    ) -> Result<IexHistCaptureSealHandoff, IexHistApplicationError> {
        IexHistCaptureSealHandoff::try_new(capture)
    }

    /// The current product boundary is deliberately closed. Product availability can be added only
    /// by the shared-data owner after every blocker has a non-forgeable exact receipt.
    pub(crate) const fn publication_availability(&self) -> IexHistPublicationAvailability {
        IexHistPublicationAvailability::Unavailable(IexHistPublicationBlockers::current())
    }
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
    blockers: [IexHistPublicationBlocker; 6],
}

impl IexHistPublicationBlockers {
    const fn current() -> Self {
        Self {
            blockers: [
                IexHistPublicationBlocker::CompleteRawPhysicalSealReceipt,
                IexHistPublicationBlocker::IexNativeLineageSchema,
                IexHistPublicationBlocker::InstrumentIdCanonicalMapper,
                IexHistPublicationBlocker::ImmutableCanonicalGenerationPublisher,
                IexHistPublicationBlocker::ManifestBoundPointInTimeSelector,
                IexHistPublicationBlocker::RestartVerifiedTypedRead,
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
    /// Shared storage must seal catalog, provider object, and expanded PCAP and return one
    /// non-forgeable joined receipt; the adapter's resumable-prefix receipt alone is insufficient.
    CompleteRawPhysicalSealReceipt,
    /// Shared provider-native lineage does not yet admit the IEX HIST decoder schema.
    IexNativeLineageSchema,
    /// Every provider symbol must resolve to a date-effective internal InstrumentId; ticker-only
    /// identity is forbidden.
    InstrumentIdCanonicalMapper,
    /// No IEX-qualified Arrow-to-immutable-Parquet publisher exists.
    ImmutableCanonicalGenerationPublisher,
    /// No selector can choose an exact IEX generation by internal identity and PIT cutoff.
    ManifestBoundPointInTimeSelector,
    /// No typed read reopens and revalidates that exact manifest after process restart.
    RestartVerifiedTypedRead,
}

/// Closed application-leaf failure.
#[derive(Debug, Error)]
pub(crate) enum IexHistApplicationError {
    #[error("IEX HIST application plan violates cold explicit-selection policy")]
    InvalidApplicationPlan,
    #[error("IEX HIST physical-seal handoff does not match exact adapter evidence")]
    InvalidPhysicalHandoff,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Checkpoint(#[from] IexHistCheckpointError),
    #[error(transparent)]
    Transport(#[from] IexHistTransportError),
}
