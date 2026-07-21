//! Bounded FRED and ALFRED extraction with fail-closed per-series rights.

mod client;
mod rights;
mod series;
mod vintages;

pub use client::{
    FredApiKey, FredExtractedPage, FredSeriesMetadata, FredSeriesMetadataDocument, FredSource,
    FredSourceError, fred_series_endpoint_rule,
};
pub use rights::{
    FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact, FredRightsDecision,
    FredRightsDisposition, FredRightsError, FredRightsPolicy, FredSeriesRightsGrant,
    FredTermsDocumentBytes, FredTermsDocumentEvidence, FredTermsDocumentRole, FredTermsEvidence,
    MAX_FRED_TERMS_DOCUMENT_BYTES, Sha256Digest,
};
pub use series::{FredObservation, FredObservationPage, FredParseLimits, FredProtocolError};
pub use vintages::FredVintagePage;
