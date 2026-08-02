//! Exact version, semantic-payload, and provider-order evidence.

use std::cmp::Ordering;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ResearchTemporalCoordinate, ResearchTemporalPrecision,
};
use sha2::{Digest as _, Sha256};

use super::ObservedRevisionError;

const PROVIDER_VERSION_DOMAIN: &[u8] = b"market-squawk/observed-version/provider/v1";
const LOCAL_VERSION_DOMAIN: &[u8] = b"market-squawk/observed-version/local-content/v1";
const SEMANTIC_PAYLOAD_DOMAIN: &[u8] = b"market-squawk/observed-payload/v1";
const PROVIDER_ORDER_DOMAIN: &[u8] = b"market-squawk/observed-provider-order/v1";
const MAX_PROVIDER_ORDER_TIE_BREAKER_BYTES: usize = 4 * 1024;

/// Maximum exact provider-token evidence retained for one provider-supplied observed version.
pub const MAX_OBSERVED_VERSION_EVIDENCE_BYTES: usize = 8 * 1024;
/// Maximum canonical semantic payload evidence retained for one observed version.
pub const MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES: usize = 64 * 1024;

/// Authority used to interpret exact observed version evidence.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ObservedVersionKind {
    /// The provider supplied a stable record/version token independent of response content.
    ProviderSupplied,
    /// Market Squawk observed a new exact canonical row content version.
    LocallyObservedContent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExactObservedEvidence {
    exact_bytes: Box<[u8]>,
    identity: EvidenceDigest,
}

impl ExactObservedEvidence {
    fn try_new(
        domain: &[u8],
        field: &'static str,
        bytes: &[u8],
        max: usize,
    ) -> Result<Self, ObservedRevisionError> {
        if bytes.is_empty() {
            return Err(ObservedRevisionError::EmptyEvidence { field });
        }
        if bytes.len() > max {
            return Err(ObservedRevisionError::EvidenceLimitExceeded { field, max });
        }
        let mut exact = Vec::new();
        exact
            .try_reserve_exact(bytes.len())
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;
        exact.extend_from_slice(bytes);
        let identity = domain_separated_sha256(domain, bytes)?;
        Ok(Self {
            exact_bytes: exact.into_boxed_slice(),
            identity,
        })
    }

    fn exact_bytes(&self) -> &[u8] {
        &self.exact_bytes
    }

    const fn identity(&self) -> EvidenceDigest {
        self.identity
    }

    const fn retained_bytes(&self) -> usize {
        self.exact_bytes.len()
    }
}

/// Bounded exact version evidence whose identity is independent of semantic response content.
///
/// Provider-supplied and locally observed identities use separate fixed domains. In both cases
/// the exact input bytes are retained so digest hits can be compared without trusting the digest
/// alone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedVersionEvidence {
    kind: ObservedVersionKind,
    evidence: ExactObservedEvidence,
}

impl ObservedVersionEvidence {
    /// Constructs a provider-token version identity.
    ///
    /// Only `provider_token` is hashed. Semantic payload bytes are accepted by
    /// [`ObservedSemanticPayload`] through a separate constructor and therefore cannot silently
    /// influence this identity.
    pub fn provider_supplied(provider_token: &[u8]) -> Result<Self, ObservedRevisionError> {
        Ok(Self {
            kind: ObservedVersionKind::ProviderSupplied,
            evidence: ExactObservedEvidence::try_new(
                PROVIDER_VERSION_DOMAIN,
                "provider_version",
                provider_token,
                MAX_OBSERVED_VERSION_EVIDENCE_BYTES,
            )?,
        })
    }

    /// Constructs a local version identity from the same exact canonical semantic payload.
    ///
    /// Taking the payload contract directly prevents callers from minting unrelated local version
    /// tokens. [`crate::ObservedRevisionRecord`] revalidates the exact-byte binding when the
    /// version and payload are combined.
    pub fn locally_observed_content(
        semantic_payload: &ObservedSemanticPayload,
    ) -> Result<Self, ObservedRevisionError> {
        Ok(Self {
            kind: ObservedVersionKind::LocallyObservedContent,
            evidence: ExactObservedEvidence::try_new(
                LOCAL_VERSION_DOMAIN,
                "local_content_version",
                semantic_payload.exact_evidence(),
                MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES,
            )?,
        })
    }

    /// Returns the closed version-authority class.
    pub const fn kind(&self) -> ObservedVersionKind {
        self.kind
    }

    /// Returns the domain-separated SHA-256 version identity.
    pub const fn identity(&self) -> EvidenceDigest {
        self.evidence.identity()
    }

    /// Returns the complete token or canonical-row evidence retained for digest collision checks.
    pub fn exact_evidence(&self) -> &[u8] {
        self.evidence.exact_bytes()
    }

    pub(super) const fn retained_bytes(&self) -> usize {
        self.evidence.retained_bytes()
    }
}

/// Bounded exact semantic payload evidence for conflict detection and durable replay validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedSemanticPayload(ExactObservedEvidence);

impl ObservedSemanticPayload {
    /// Copies and hashes one exact canonical semantic payload.
    ///
    /// # Errors
    ///
    /// Rejects empty evidence, values above [`MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES`], and bounded
    /// allocation or length failures.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, ObservedRevisionError> {
        ExactObservedEvidence::try_new(
            SEMANTIC_PAYLOAD_DOMAIN,
            "semantic_payload",
            bytes,
            MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES,
        )
        .map(Self)
    }

    /// Returns the domain-separated SHA-256 semantic identity.
    pub const fn identity(&self) -> EvidenceDigest {
        self.0.identity()
    }

    /// Returns complete canonical payload evidence retained for collision-safe comparison.
    pub fn exact_evidence(&self) -> &[u8] {
        self.0.exact_bytes()
    }
}

/// Explicit provider ordering for multiple versions observed in one family batch.
///
/// The primary coordinate retains its true precision. `tie_breaker` is an exact stable provider
/// token used only when two versions share the same coordinate; it never changes version identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedProviderOrder {
    coordinate: ResearchTemporalCoordinate,
    tie_breaker: ExactObservedEvidence,
}

impl ObservedProviderOrder {
    /// Constructs a provider order from a typed coordinate and exact stable tie-breaker.
    ///
    /// # Errors
    ///
    /// Rejects an empty or oversized tie-breaker and checked byte/allocation failures.
    pub fn try_new(
        coordinate: ResearchTemporalCoordinate,
        tie_breaker: &[u8],
    ) -> Result<Self, ObservedRevisionError> {
        Ok(Self {
            coordinate,
            tie_breaker: ExactObservedEvidence::try_new(
                PROVIDER_ORDER_DOMAIN,
                "provider_order_tie_breaker",
                tie_breaker,
                MAX_PROVIDER_ORDER_TIE_BREAKER_BYTES,
            )?,
        })
    }

    /// Returns the source-authored ordering coordinate.
    pub const fn coordinate(&self) -> &ResearchTemporalCoordinate {
        &self.coordinate
    }

    /// Returns complete provider tie-break evidence for collision-safe comparison.
    pub fn exact_tie_breaker(&self) -> &[u8] {
        self.tie_breaker.exact_bytes()
    }

    pub(super) fn checked_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.coordinate.partial_cmp(&other.coordinate)? {
            Ordering::Equal => Some(
                self.tie_breaker
                    .exact_bytes()
                    .cmp(other.tie_breaker.exact_bytes()),
            ),
            ordering => Some(ordering),
        }
    }

    pub(super) fn retained_bytes(&self) -> Result<usize, ObservedRevisionError> {
        let coordinate_bytes = match self.coordinate.precision() {
            ResearchTemporalPrecision::ExactTimestamp | ResearchTemporalPrecision::CalendarDate => {
                0
            }
            ResearchTemporalPrecision::SourcePeriod => {
                let period = self
                    .coordinate
                    .source_period_value()
                    .ok_or(ObservedRevisionError::CorruptAuthorityState)?;
                period
                    .scheme()
                    .retained_bytes()
                    .checked_add(period.code().retained_bytes())
                    .ok_or(ObservedRevisionError::ByteCountOverflow)?
            }
        };
        coordinate_bytes
            .checked_add(self.tie_breaker.retained_bytes())
            .ok_or(ObservedRevisionError::ByteCountOverflow)
    }
}

fn domain_separated_sha256(
    domain: &[u8],
    exact_evidence: &[u8],
) -> Result<EvidenceDigest, ObservedRevisionError> {
    let domain_length =
        u64::try_from(domain.len()).map_err(|_| ObservedRevisionError::ByteCountOverflow)?;
    let evidence_length = u64::try_from(exact_evidence.len())
        .map_err(|_| ObservedRevisionError::ByteCountOverflow)?;
    let mut digest = Sha256::new();
    digest.update(domain_length.to_le_bytes());
    digest.update(domain);
    digest.update(evidence_length.to_le_bytes());
    digest.update(exact_evidence);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}
