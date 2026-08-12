//! Bounded acquisition, parsing, and publication contracts for official Federal Reserve Board Data
//! Download Program (DDP) and Board-hosted statistical-release files.
//!
//! HTTPS acquisition is admitted only through application-issued shared extraction authority. The
//! crate invents no generic API or numeric provider rate, and rich extraction retains exact bytes
//! for the application's shared raw-capture seal before canonical publication.

mod contract;
mod digest;
mod error;
mod model;
mod parse;
mod publication;
mod source;
mod transport;

pub use contract::{
    BOARD_DDP_SOURCE_ID, BOARD_H15_TREASURY_CONSTANT_MATURITIES_DOCTOR_PROBE_URL,
    BOARD_H15_TREASURY_CONSTANT_MATURITIES_PRODUCTION_URL, BOARD_NATIVE_CONTRACT_VERSION,
    BoardArtifactContract, BoardArtifactKind, BoardDatasetContract, BoardDatasetFamily,
    BoardFileFormat, BoardFileRequest, BoardFrequency, BoardH15DashboardSeriesDescriptor,
    BoardRelease, BoardRouteLifecycle, BoardSeriesContract, BoardSeriesLifecycle, BoardSeriesScope,
    SdmxPackageContract, h15_treasury_constant_maturities_canonical_unit_identifier,
    h15_treasury_constant_maturities_dashboard_series,
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
#[cfg(all(feature = "scripted-transport-fixture", debug_assertions))]
pub use transport::{
    BoardScriptedCsvResponse, BoardScriptedDoctorExecutor, BoardScriptedHttpRequest,
    BoardScriptedHttpResponse, BoardScriptedTransportCounters, BoardScriptedTransportError,
    BoardScriptedTransportFactory,
};

#[cfg(test)]
mod tests;
