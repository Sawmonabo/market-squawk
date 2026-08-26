//! Bounded IEX HIST catalog selection, cold-job transport, and venue-specific PCAP decoding.
//!
//! The adapter fetches one catalog generation or one explicitly selected feed/date file at a time;
//! it deliberately supplies no archive mirroring loop. Downloads remain behind cancellation,
//! deadline, byte, disk, checksum, representation, and versioned decode boundaries. Provider-local
//! durable state retains a restorable immutable plan, exact attempt/capture/decode evidence, and
//! closed recovery outcomes; no byte-range resume or analytical publication authority is claimed.
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
    DecodeFailure, DecodeLimits, DecodeSummary, DplcChannelDistributionContract, IexEventSink,
    PcapStreamDecoder,
};
pub use durable::{
    IexHistCheckpointError, IexHistCheckpointStore, IexHistCheckpointStoreError,
    IexHistDurableJob, IexHistJobPhase, IexHistReactivationRequirement,
    IexHistRecoveryAction, IexHistRetryDisposition, IexHistTerminalDisposition,
    IexHistTerminalCoordinate, IexHistTerminalError, IexHistTerminalEvidence,
    IexHistTerminalPhase,
};
pub use model::{
    AuctionImbalanceSide, AuctionType, DecodedIexEvent, EpochNanos, FeedKind, FeedVersion,
    IexEvent, IexVenueSemantics, LuldTier, OperationalHaltStatus, OrderSide,
    PcapObjectEncoding, PriceLevelSide, PriceType, PriceUnits1e4,
    RetailLiquidityIndicator, SecurityEventCode, Sha256Digest, ShortSalePriceTestDetail,
    SystemEventCode, TradeDate, TradingStatus, TransportVersion,
};
pub use planning::{
    ByteAdmissionLimits, ColdJobPlan, ColdJobTrigger, IexHistCapacityAuthority,
    IexHistAuthorityClockSample, IexHistCapacityCategory, IexHistCapacityDisposition,
    IexHistCapacityError, IexHistCapacityFootprint, IexHistCapacityLease,
    IexHistCapacityOperation, IexHistCapacityRequest, IexHistCapacitySettlement,
    IexHistCapacityUsage, IexHistCatalogObservationReceipt, IexHistDecodeAttemptEvidence,
    IexHistDplcDistributionAuthority, IexHistDplcDistributionError, IexHistExecutionAttempt,
    IexHistExecutionPermit, IexHistPlanner, IexHistTerminalReason, IexHistTrustedClockReading,
    PlanError, ResumePolicy, ScheduleLane, IEX_HIST_PROVIDER_LANE,
};
pub use receipt::{
    CaptureChronologyDisposition, CaptureClockAnomaly, CaptureError, PcapMaterializationReceipt,
};
pub use transport::{
    CatalogFetch, DecodedIexCapture, IEX_HIST_CATALOG_URL, IexHistColdTransport,
    IexHistTransportConfig, IexHistTransportError, MaterializedIexCapture, RetryObservation,
    RetryPolicy, StagedCaptureFiles, TransportErrorKind, TransportTelemetry,
};

#[cfg(test)]
mod tests;
