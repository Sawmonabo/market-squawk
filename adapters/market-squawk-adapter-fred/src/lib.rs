//! Bounded FRED and ALFRED extraction with fail-closed per-series rights.

mod client;
mod rights;
mod series;
mod vintages;

pub use client::{FredApiKey, FredExtractedPage, FredSource, FredSourceError};
pub use rights::{
    FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact, FredRightsDecision,
    FredRightsDisposition, FredRightsError, FredRightsPolicy, FredSeriesRightsGrant,
    FredTermsEvidence, Sha256Digest,
};
pub use series::{FredObservation, FredObservationPage, FredParseLimits, FredProtocolError};
pub use vintages::FredVintagePage;
