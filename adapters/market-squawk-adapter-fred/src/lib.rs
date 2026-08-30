//! Bounded FRED and ALFRED extraction with fail-closed per-series rights.

mod client;
mod release;
mod rights;
mod series;
mod vintages;

pub use client::{
    FredApiKey, FredExtractedPage, FredExtractionOutput, FredPageObjectIdentity,
    FredReleaseExtraction, FredReleaseExtractionPage, FredSeriesMetadata,
    FredSeriesMetadataDocument, FredSource, FredSourceError, FredVintageExtraction,
    FredVintageExtractionPage, MAX_FRED_EPHEMERAL_PAGE_RECORDS, fred_observations_endpoint_rule,
    fred_release_observations_v2_endpoint_rule, fred_series_endpoint_rule,
    fred_vintage_dates_endpoint_rule,
};
pub use release::{
    FredReleaseCursor, FredReleaseMetadata, FredReleaseObservation, FredReleaseObservationPage,
    FredReleaseSeries, FredReleaseSource, MAX_FRED_V2_RELEASE_PAGE_OBSERVATIONS,
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
