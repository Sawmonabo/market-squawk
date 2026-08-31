//! Bounded FRED and ALFRED extraction under the shared source authority.

mod client;
mod release;
mod series;
mod vintages;

pub use client::{
    FredApiKey, FredDiscoveryError, FredExtractedPage, FredExtractionOutput,
    FredPageObjectIdentity, FredReleaseExtraction, FredReleaseExtractionPage, FredSeriesMetadata,
    FredSeriesMetadataDocument, FredSource, FredSourceError, FredVintageExtraction,
    FredVintageExtractionPage, MAX_FRED_EPHEMERAL_PAGE_RECORDS, fred_observations_endpoint_rule,
    fred_release_observations_v2_endpoint_rule, fred_series_endpoint_rule,
    fred_vintage_dates_endpoint_rule,
};
pub use release::{
    FredReleaseCursor, FredReleaseMetadata, FredReleaseObservation, FredReleaseObservationPage,
    FredReleaseSeries, FredReleaseSource, MAX_FRED_V2_RELEASE_PAGE_OBSERVATIONS,
};
pub use series::{FredObservation, FredObservationPage, FredParseLimits, FredProtocolError};
pub use vintages::FredVintagePage;
