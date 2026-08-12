//! Bounded IEX HIST catalog selection, cold-job transport, and venue-specific PCAP decoding.
//!
//! The adapter fetches one catalog generation or one explicitly selected feed/date file at a time;
//! it deliberately supplies no archive mirroring loop. Downloads remain behind cancellation,
//! deadline, byte, disk, checksum, gzip, and versioned decode boundaries. Decoded observations are
//! explicitly historical IEX-venue evidence; they are never SIP, NBBO, or market-wide depth.

mod catalog;
mod decode;
mod model;
mod planning;
mod receipt;
mod transport;

pub use catalog::{
    Catalog, CatalogError, CatalogFile, CatalogReceipt, CatalogTransportMetadata, ExactFileRequest,
    SelectedFileReceipt,
};
pub use decode::{DecodeError, DecodeLimits, DecodeSummary, IexEventSink, PcapStreamDecoder};
pub use model::{
    DecodedIexEvent, EpochNanos, FeedKind, FeedVersion, IexEvent, IexVenueSemantics,
    PriceLevelSide, PriceType, PriceUnits1e4, Sha256Digest, SystemEventCode, TradeDate,
    TradingStatus, TransportVersion,
};
pub use planning::{
    ByteAdmissionLimits, ColdJobPlan, ColdJobTrigger, IexHistPlanner, PlanError, ResumePolicy,
    ScheduleLane,
};
pub use receipt::{
    CaptureError, CaptureResponseMetadata, GzipPcapReceiptBuilder, PcapMaterializationReceipt,
};
pub use transport::{
    CatalogFetch, DownloadedIexCapture, IEX_HIST_CATALOG_URL, IexHistColdTransport,
    IexHistTransportConfig, IexHistTransportError, RetryObservation, RetryPolicy,
    StagedCaptureFiles, TransportErrorKind, TransportTelemetry,
};

#[cfg(test)]
mod tests;
