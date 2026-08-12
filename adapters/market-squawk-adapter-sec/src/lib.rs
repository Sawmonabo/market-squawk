//! Bounded SEC EDGAR submissions, filing, and XBRL extraction.

mod client;
mod composite;
mod evidence_store;
mod extraction;
mod json;
mod normalize;
mod representation_registry;
mod xbrl;

pub use client::{
    RetrievedCompanyFacts, RetrievedSecBytes, RetrievedSubmissions, RetrievedXbrlDocument,
    SecClientError, SecContact, SecEdgarSource, SecExtractionHealth, SecExtractionHealthState,
    SecObjectLocator,
};
pub use composite::SecCompositeBounds;
pub use evidence_store::{RawEvidenceError, RawEvidenceStore};
pub use extraction::{SecDiscoveryResult, SecExtractionResult};
pub use json::{
    CompanyFactOccurrence, CompanyFactPeriod, CompanyFactsDocument, SecFiling, SecFormerName,
    SecParserError, SecParserLimits, SecSubmissionCompanyMetadata, SecTickerExchangePair,
    SubmissionsArchive, SubmissionsDocument, reconcile_submissions,
    reconcile_submissions_with_cancellation,
};
pub use normalize::{
    SecNormalizationError, normalize_company_facts, normalize_company_facts_with_cancellation,
    normalize_filings, normalize_filings_with_cancellation,
};
pub use representation_registry::{
    SecHttpValidators, SecRepresentation, SecRepresentationError, SecRepresentationLimits,
    SecRepresentationRegistry,
};
pub use xbrl::{
    ParsedXbrlDocument, SecXbrlError, XbrlDocumentContext, XbrlDocumentParser,
    XbrlNonnumericOccurrence, XbrlNumericFact,
};
