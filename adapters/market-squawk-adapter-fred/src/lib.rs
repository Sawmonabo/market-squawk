//! Bounded FRED and ALFRED extraction with fail-closed per-series rights.

mod client;
mod rights;
mod series;
mod vintages;

pub use client::{
    FredApiKey, FredExtractedPage, FredExtractionOutput, FredPageObjectIdentity,
    FredSeriesMetadata, FredSeriesMetadataDocument, FredSource, FredSourceError,
    MAX_FRED_EPHEMERAL_PAGE_RECORDS, fred_series_endpoint_rule,
};
pub use rights::{
    CURRENT_FRED_RIGHTS_ARTIFACT_BYTE_LENGTH, CURRENT_FRED_RIGHTS_ARTIFACT_SHA256,
    CURRENT_UNRATE_RIGHTS_ARTIFACT_BYTE_LENGTH, CURRENT_UNRATE_RIGHTS_ARTIFACT_SHA256,
    FredDurableAuthority, FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact,
    FredRightsDecision, FredRightsDisposition, FredRightsError, FredRightsPolicy,
    FredSeriesRightsBasis, FredSeriesRightsEvidence, FredSeriesRightsGrant,
    FredServicePermissionChannel, FredServicePermissionEvidence, FredServicePermissionReview,
    FredTermsDocumentBytes, FredTermsDocumentEvidence, FredTermsDocumentRole, FredTermsEvidence,
    MAX_FRED_SERIES_RIGHTS_EVIDENCE_BYTES, MAX_FRED_SERVICE_PERMISSION_BYTES,
    MAX_FRED_TERMS_DOCUMENT_BYTES, Sha256Digest,
};
pub use series::{FredObservation, FredObservationPage, FredParseLimits, FredProtocolError};
pub use vintages::FredVintagePage;
