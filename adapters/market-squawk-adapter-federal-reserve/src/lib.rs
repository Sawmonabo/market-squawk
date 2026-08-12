//! Bounded, transport-free parsing and publication contracts for official Federal Reserve Board
//! Data Download Program (DDP) and Board-hosted statistical-release files.
//!
//! This crate deliberately performs no network I/O and invents no generic API or provider rate.
//! The application scheduler must admit the exact [`BoardFileRequest`] through the shared provider
//! budget, retain the returned bytes, and then pass those bytes to this adapter core.

mod contract;
mod digest;
mod error;
mod model;
mod parse;
mod publication;
mod source;
mod transport;

pub use contract::{
    BOARD_DDP_SOURCE_ID, BOARD_NATIVE_CONTRACT_VERSION, BoardArtifactContract, BoardArtifactKind,
    BoardDatasetContract, BoardDatasetFamily, BoardFileFormat, BoardFileRequest, BoardFrequency,
    BoardRelease, BoardRouteLifecycle, BoardSeriesContract, BoardSeriesLifecycle, BoardSeriesScope,
    SdmxPackageContract,
};
pub use error::BoardAdapterError;
pub use model::{
    BoardArtifactReceipt, BoardMissingValue, BoardObservation, BoardPeriod, BoardPeriodValue,
    BoardSdmxHeader, BoardSeries, BoardValue, ParsedBoardDataset,
};
pub use parse::{BoardParseLimits, parse_board_file, parse_csv, parse_sdmx_xml, parse_sdmx_zip};
pub use publication::{
    BoardObservationChange, BoardPublicationEvent, BoardPublicationEventKind,
    BoardPublicationOutcome, BoardPublicationReceipt, BoardPublicationTiming, BoardPublisher,
    BoardRevisionEvidence, BoardSeriesReplacement, BoardVintageCapability,
};
pub use source::{
    BoardDatasetProfile, BoardExtractionError, BoardExtractionOutput, BoardSource,
    BoardSourceError, BoardSourceHealth, BoardStructuralArtifact,
};
pub use transport::{
    BoardConditionalRequest, BoardHttpReceipt, BoardHttpValidators, BoardNotModifiedReceipt,
    BoardRetrievalOutcome, BoardRetrievedFile,
};

#[cfg(test)]
mod tests;
