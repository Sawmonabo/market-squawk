//! Bounded FRED and ALFRED extraction with fail-closed per-series rights.

mod rights;
mod series;
mod vintages;

pub use rights::{
    FredOperation, FredRightsArtifact, FredRightsDecision, FredRightsDisposition, FredRightsError,
    FredRightsPolicy, FredSeriesRightsGrant, FredTermsEvidence, Sha256Digest,
};
pub use series::{FredObservation, FredObservationPage, FredParseLimits, FredProtocolError};
pub use vintages::FredVintagePage;
