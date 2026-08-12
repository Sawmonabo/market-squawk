use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::TiingoEndpointFamily;

/// A bounded non-success response retained without collapsing provider status or bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoProviderFailure {
    status: u16,
    response_bytes: Box<[u8]>,
    body_digest: EvidenceDigest,
}

impl TiingoProviderFailure {
    pub(crate) fn new(status: u16, body: &[u8]) -> Self {
        let bytes: [u8; 32] = Sha256::digest(body).into();
        Self {
            status,
            response_bytes: body.into(),
            body_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, bytes),
        }
    }

    /// Returns the exact HTTP status supplied by Tiingo.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the exact bounded provider error body for raw-evidence publication.
    pub fn response_bytes(&self) -> &[u8] {
        &self.response_bytes
    }

    /// Returns the SHA-256 identity of the exact provider error body.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }
}

/// Closed reason that a previously admitted Tiingo native schema no longer matched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoSchemaChangeReason {
    /// The response root was not the documented object or array.
    InvalidTopLevel,
    /// A required provider field was absent.
    MissingField,
    /// The provider added a field outside the reviewed native schema.
    UnknownField,
    /// A field had a different JSON kind from the reviewed contract.
    InvalidFieldType,
    /// A field value violated the reviewed exact-value or clock contract.
    InvalidFieldValue,
    /// Rows were duplicated, out of order, or outside their request page.
    InvalidRowSequence,
    /// The provider returned more rows than the request's code-owned bound.
    RowLimitExceeded,
    /// The returned ticker differed from the exact requested provider instrument.
    SymbolMismatch,
}

/// Evidence that trips the provider-native schema circuit until an explicit reviewed reset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoSchemaChange {
    endpoint: TiingoEndpointFamily,
    reason: TiingoSchemaChangeReason,
    response_digest: EvidenceDigest,
    observed_at: Timestamp,
}

impl TiingoSchemaChange {
    pub(crate) const fn new(
        endpoint: TiingoEndpointFamily,
        reason: TiingoSchemaChangeReason,
        response_digest: EvidenceDigest,
        observed_at: Timestamp,
    ) -> Self {
        Self {
            endpoint,
            reason,
            response_digest,
            observed_at,
        }
    }

    /// Returns the response family whose native contract changed.
    pub const fn endpoint(&self) -> TiingoEndpointFamily {
        self.endpoint
    }

    /// Returns the closed schema failure class.
    pub const fn reason(&self) -> TiingoSchemaChangeReason {
        self.reason
    }

    /// Returns the exact response-body identity that opened the circuit.
    pub const fn response_digest(&self) -> EvidenceDigest {
        self.response_digest
    }

    /// Returns when the changed response was first decoded locally.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}

/// Tiingo adapter-core failure.
#[derive(Debug, Error)]
pub enum TiingoAdapterError {
    /// The token was empty, oversized, or unsafe to place in one sensitive header.
    #[error("invalid Tiingo API token")]
    InvalidToken,
    /// A provider ticker crossed the code-owned identifier grammar or byte bound.
    #[error("invalid Tiingo ticker")]
    InvalidTicker,
    /// A request date interval was inverted or could not be represented safely.
    #[error("invalid Tiingo date range")]
    InvalidDateRange,
    /// The requested history would exceed the code-owned application page ceiling.
    #[error("Tiingo history request exceeds the application page limit")]
    HistoryTooLarge,
    /// The code-owned URL or HTTP request could not be constructed.
    #[error("failed to construct the Tiingo request")]
    RequestBuild,
    /// The exact response body crossed the request-specific byte ceiling.
    #[error("Tiingo response exceeded its byte limit")]
    BodyTooLarge,
    /// Receive/decode clocks regressed.
    #[error("invalid Tiingo response chronology")]
    InvalidChronology,
    /// Tiingo returned a bounded non-success response.
    #[error("Tiingo returned a bounded non-success response")]
    Provider(TiingoProviderFailure),
    /// The provider-native schema circuit is already open.
    #[error("Tiingo native-schema circuit is open")]
    SchemaCircuitOpen,
    /// The current body violated the reviewed provider-native contract and opened the circuit.
    #[error("Tiingo native schema changed")]
    SchemaChanged(TiingoSchemaChange),
    /// Exact fund/share-class context was incompatible with the provider row.
    #[error("invalid Tiingo fund normalization context")]
    InvalidFundContext,
}

pub(crate) type SchemaResult<T> = Result<T, TiingoSchemaChangeReason>;
