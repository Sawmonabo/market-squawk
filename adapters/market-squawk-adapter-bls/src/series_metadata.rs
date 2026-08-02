use bytes::Bytes;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier,
};
use market_squawk_sources::payload_matches_exact_evidence;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::BlsSourceError;
use crate::chunks::is_valid_identifier_byte;

const SERIES_METADATA_SCHEMA_VERSION: u16 = 1;
const MAX_SERIES_METADATA_BYTES: usize = 4 * 1024;
const MAX_TITLE_BYTES: usize = 512;

/// Exact, user-authorized semantic metadata for one BLS series.
///
/// The Public Data API does not provide universal unit metadata with its observations. Callers
/// must therefore provide a separately verified metadata record whose exact bytes, content
/// digest, and authorization reference remain bound to the request plan. The adapter never
/// infers a unit from a series identifier, title, or numeric values.
#[derive(Clone, Eq, PartialEq)]
pub struct BlsSeriesMetadata {
    series_id: String,
    title: String,
    unit: SourceIdentifier,
    frequency: SourceIdentifier,
    seasonal_adjustment: SourceIdentifier,
    measure: SourceIdentifier,
    exact_payload: Bytes,
    evidence: ExactPayloadEvidence,
    authorization_reference: SourceIdentifier,
}

/// Explicit user-verified semantic fields for one BLS series.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsSeriesMetadataInput {
    series_id: String,
    title: String,
    unit: SourceIdentifier,
    frequency: SourceIdentifier,
    seasonal_adjustment: SourceIdentifier,
    measure: SourceIdentifier,
}

impl std::fmt::Debug for BlsSeriesMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsSeriesMetadata")
            .field("series_id", &self.series_id)
            .field("title", &self.title)
            .field("unit", &self.unit)
            .field("frequency", &self.frequency)
            .field("seasonal_adjustment", &self.seasonal_adjustment)
            .field("measure", &self.measure)
            .field("exact_payload_bytes", &self.exact_payload.len())
            .field("evidence", &self.evidence)
            .field("authorization_reference", &self.authorization_reference)
            .finish()
    }
}

impl BlsSeriesMetadata {
    /// Canonically encodes explicit user-verified fields and binds them to an authorization record.
    ///
    /// # Errors
    ///
    /// Returns [`BlsSourceError::InvalidSeriesMetadata`] when canonical encoding fails or any field
    /// violates the same exact-payload contract used by file-based activation.
    pub fn from_verified_input(
        input: BlsSeriesMetadataInput,
        authorization_reference: SourceIdentifier,
    ) -> Result<Self, BlsSourceError> {
        let wire = BlsSeriesMetadataWire {
            schema_version: SERIES_METADATA_SCHEMA_VERSION,
            series_id: input.series_id,
            title: input.title,
            unit: input.unit,
            frequency: input.frequency,
            seasonal_adjustment: input.seasonal_adjustment,
            measure: input.measure,
        };
        let exact_payload = serde_json::to_vec(&wire)
            .map(Bytes::from)
            .map_err(|_error| BlsSourceError::InvalidSeriesMetadata)?;
        let digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&exact_payload).into(),
        );
        Self::parse_exact(
            exact_payload,
            ExactPayloadEvidence::from_content_digest(digest),
            authorization_reference,
        )
    }

    /// Parses a bounded metadata record and binds its exact bytes to caller authorization.
    ///
    /// # Errors
    ///
    /// Returns [`BlsSourceError::InvalidSeriesMetadata`] when the payload is empty or oversized,
    /// its exact evidence does not match, its schema is unsupported, or a semantic field violates
    /// its canonical bounds.
    pub fn parse_exact(
        exact_payload: Bytes,
        evidence: ExactPayloadEvidence,
        authorization_reference: SourceIdentifier,
    ) -> Result<Self, BlsSourceError> {
        if exact_payload.is_empty()
            || exact_payload.len() > MAX_SERIES_METADATA_BYTES
            || !payload_matches_exact_evidence(&exact_payload, &evidence)
        {
            return Err(BlsSourceError::InvalidSeriesMetadata);
        }
        let wire: BlsSeriesMetadataWire = serde_json::from_slice(&exact_payload)
            .map_err(|_| BlsSourceError::InvalidSeriesMetadata)?;
        if wire.schema_version != SERIES_METADATA_SCHEMA_VERSION
            || !valid_series_id(&wire.series_id)
            || !valid_title(&wire.title)
        {
            return Err(BlsSourceError::InvalidSeriesMetadata);
        }
        Ok(Self {
            series_id: wire.series_id,
            title: wire.title,
            unit: wire.unit,
            frequency: wire.frequency,
            seasonal_adjustment: wire.seasonal_adjustment,
            measure: wire.measure,
            exact_payload,
            evidence,
            authorization_reference,
        })
    }

    /// Returns the exact BLS series identifier.
    pub fn series_id(&self) -> &str {
        &self.series_id
    }

    /// Returns the user-verified series title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the explicit canonical unit; it is never inferred by the adapter.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }

    /// Returns the explicit observation frequency.
    pub const fn frequency(&self) -> &SourceIdentifier {
        &self.frequency
    }

    /// Returns the explicit seasonal-adjustment semantic.
    pub const fn seasonal_adjustment(&self) -> &SourceIdentifier {
        &self.seasonal_adjustment
    }

    /// Returns the explicit measure semantic.
    pub const fn measure(&self) -> &SourceIdentifier {
        &self.measure
    }

    /// Returns the exact user-supplied metadata bytes retained for audit and persistence.
    pub fn exact_payload(&self) -> &[u8] {
        &self.exact_payload
    }

    /// Returns algorithm-qualified evidence for the exact retained bytes.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns the caller-supplied authorization or approval record identity.
    pub const fn authorization_reference(&self) -> &SourceIdentifier {
        &self.authorization_reference
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BlsSeriesMetadataWire {
    schema_version: u16,
    series_id: String,
    title: String,
    unit: SourceIdentifier,
    frequency: SourceIdentifier,
    seasonal_adjustment: SourceIdentifier,
    measure: SourceIdentifier,
}

fn valid_series_id(series_id: &str) -> bool {
    !series_id.is_empty()
        && series_id.len() <= 50
        && series_id.bytes().all(is_valid_identifier_byte)
}

fn valid_title(title: &str) -> bool {
    !title.is_empty()
        && title.len() <= MAX_TITLE_BYTES
        && title.trim() == title
        && !title.chars().any(char::is_control)
}
