//! Bounded IEX HIST catalog selection, cold-job transport, and venue-specific PCAP decoding.
//!
//! The adapter fetches one catalog generation or one explicitly selected feed/date file at a time;
//! it deliberately supplies no archive mirroring loop. Downloads remain behind cancellation,
//! deadline, byte, disk, checksum, representation, and versioned decode boundaries. Provider-local
//! durable job state retains exact plan/capture/decode/recovery evidence through a shared
//! checkpoint seam, while resumable claims support a strong-validator exact range only after
//! shared physical storage adopts and reopens the rehashed prefix. Neither surface owns
//! canonical/PIT publication or analytical-generation authority.
//! Decoded observations are explicitly historical IEX-venue evidence; they are never SIP, NBBO,
//! or market-wide depth.

mod catalog;
mod decode;
mod durable;
mod model;
mod planning;
mod receipt;
mod transport;

pub use catalog::{
    Catalog, CatalogError, CatalogFile, CatalogReceipt, ExactFileRequest, SelectedFileReceipt,
};
pub use decode::{
    DecodeActuals, DecodeChannelContract, DecodeChannelRole, DecodeContract, DecodeError,
    DecodeFailure, DecodeLimits, DecodeSummary, DecodedIexEventEnvelope,
    DplcChannelDistributionContract, IexEventSink, IexHistBarInterval, IexHistDerivedBarError,
    IexHistDerivedBarsHandoff, IexHistTypedEvent, IexHistTypedHandoff, IexHistTypedHandoffBuilder,
    IexHistVenueTradeBar, PcapStreamDecoder,
};
pub use durable::{
    IexHistCapturePhysicalSealEvidence, IexHistCatalogPhysicalSealEvidence, IexHistCheckpointError,
    IexHistCheckpointStore, IexHistCheckpointStoreError, IexHistDurableJob, IexHistJobPhase,
    IexHistReactivationRequirement, IexHistRecoveryAction, IexHistResumeClaim,
    IexHistResumeClaimError, IexHistRetryDisposition, IexHistTerminalCoordinate,
    IexHistTerminalDisposition, IexHistTerminalError, IexHistTerminalEvidence,
    IexHistTerminalPhase,
};
pub use model::{
    AuctionImbalanceSide, AuctionType, DecodedIexEvent, EpochNanos, FeedKind, FeedVersion,
    IexEvent, IexVenueSemantics, LuldTier, OperationalHaltStatus, OrderSide, PcapObjectEncoding,
    PriceLevelSide, PriceType, PriceUnits1e4, RetailLiquidityIndicator, SecurityEventCode,
    Sha256Digest, ShortSalePriceTestDetail, SystemEventCode, TradeDate, TradingStatus,
    TransportVersion,
};
pub use planning::{
    ByteAdmissionLimits, ColdJobPlan, ColdJobTrigger, IEX_HIST_PROVIDER_LANE,
    IexHistAuthorityClockSample, IexHistCapacityAuthority, IexHistCapacityCategory,
    IexHistCapacityDisposition, IexHistCapacityError, IexHistCapacityFootprint,
    IexHistCapacityLease, IexHistCapacityOperation, IexHistCapacityRequest,
    IexHistCapacitySettlement, IexHistCapacityUsage, IexHistCatalogObservationReceipt,
    IexHistDecodeAttemptEvidence, IexHistDplcDistributionAuthority, IexHistDplcDistributionError,
    IexHistExecutionAttempt, IexHistExecutionPermit, IexHistPlanner, IexHistTerminalReason,
    IexHistTrustedClockReading, PlanError, ResumePolicy, ScheduleLane,
};
pub use receipt::{
    CaptureChronologyDisposition, CaptureClockAnomaly, CaptureError, PcapMaterializationReceipt,
};
pub use transport::{
    CatalogFetch, IEX_HIST_CATALOG_URL, IexHistAdoptedResume, IexHistColdTransport,
    IexHistCompletePhysicalSeal, IexHistCompleteSealError, IexHistDecodedSealedCapture,
    IexHistDownloadOutcome, IexHistPendingResume, IexHistResumeAdoptionBindingError,
    IexHistResumeAdoptionError, IexHistResumeAdoptionReceipt, IexHistResumeAdoptionRequest,
    IexHistResumeCandidate, IexHistResumeCause, IexHistResumePhysicalAdopter,
    IexHistResumeTelemetryEvidence, IexHistSealedCatalog, IexHistSealedMaterializedCapture,
    IexHistSharedPhysicalSealReceipt, IexHistTransportConfig, IexHistTransportError,
    MaterializedIexCapture, RetryObservation, RetryPolicy, StagedCaptureFiles, TransportErrorKind,
    TransportTelemetry,
};

#[cfg(test)]
mod tests;
