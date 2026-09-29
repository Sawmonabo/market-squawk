//! Immutable application-owned comparison recipe bound to the analysis identity.

use super::InvestmentProposalError;
use crate::DecisionContentDigest;
use market_squawk_domain::{Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, Timestamp};
use sha2::{Digest as _, Sha256};

/// Bounded canonical application record, not source-read or financial admission authority.
/// The application validates its closed schema and reopens the exact source receipts on use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SavedBenchmarkComparisonEvidence {
    instrument_id: InstrumentId,
    currency: Currency,
    source_cutoff: Timestamp,
    observed_through: Timestamp,
    canonical_record: Box<[u8]>,
    digest: DecisionContentDigest,
}

impl SavedBenchmarkComparisonEvidence {
    /// Three source recipes and two bounded catalog labels fit below this audit envelope.
    /// This bounds reference bytes only; it never limits the underlying historical source.
    pub const MAXIMUM_RECORD_BYTES: usize = 16 * 1024;

    /// Commits the exact application record and original economic coordinates.
    pub fn try_new(
        instrument_id: InstrumentId,
        currency: Currency,
        source_cutoff: Timestamp,
        observed_through: Timestamp,
        canonical_record: Box<[u8]>,
    ) -> Result<Self, InvestmentProposalError> {
        if canonical_record.is_empty()
            || canonical_record.len() > Self::MAXIMUM_RECORD_BYTES
            || observed_through > source_cutoff
        {
            return Err(InvestmentProposalError::InvalidEvidenceMetric);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/saved-benchmark-comparison/v1\0");
        hash.update(instrument_id.as_uuid().as_bytes());
        hash.update(currency.as_str().as_bytes());
        hash.update(source_cutoff.unix_nanos().to_be_bytes());
        hash.update(observed_through.unix_nanos().to_be_bytes());
        hash.update(&canonical_record);
        let digest = DecisionContentDigest::try_new(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            hash.finalize().into(),
        ))
        .map_err(|_| InvestmentProposalError::ReservedIdentity)?;
        Ok(Self {
            instrument_id,
            currency,
            source_cutoff,
            observed_through,
            canonical_record,
            digest,
        })
    }

    /// Original subject; comparison membership remains in the exact application record.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Common denomination used for the source comparison.
    #[must_use]
    pub const fn currency(&self) -> Currency {
        self.currency
    }
    /// Original knowledge cutoff, never the restart time.
    #[must_use]
    pub const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
    /// Last allowed observation, including the original forecast origin where present.
    #[must_use]
    pub const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }
    /// Exact bounded application record for strict decoding.
    #[must_use]
    pub fn canonical_record(&self) -> &[u8] {
        &self.canonical_record
    }
    /// Commitment included in the analysis evidence digest.
    #[must_use]
    pub const fn digest(&self) -> DecisionContentDigest {
        self.digest
    }
}
