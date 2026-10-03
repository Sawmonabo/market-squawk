//! Immutable original forecast/history binding; application replay owns read authority.

use super::{InvestmentProposalError, ProposalForecastVintageId};
use crate::DecisionContentDigest;
use market_squawk_domain::{Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, Timestamp};
use sha2::{Digest as _, Sha256};

/// Bounded inert application recipe committed by the saved investment analysis.
/// Decoding these bytes cannot mint a source plan or a forecast-basis history proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SavedForecastChartEvidence {
    instrument_id: InstrumentId,
    currency: Currency,
    source_cutoff: Timestamp,
    origin_at: Timestamp,
    vintage_id: ProposalForecastVintageId,
    basis_identity: DecisionContentDigest,
    history_identity: DecisionContentDigest,
    canonical_record: Box<[u8]>,
    digest: DecisionContentDigest,
}
impl SavedForecastChartEvidence {
    /// Bounded exact source/calendar recipes and their replay commitments.
    pub const MAXIMUM_RECORD_BYTES: usize = 64 * 1024;

    /// Retains an application-validated closed record, without granting source authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "original forecast and source commitments"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        currency: Currency,
        source_cutoff: Timestamp,
        origin_at: Timestamp,
        vintage_id: ProposalForecastVintageId,
        basis_identity: DecisionContentDigest,
        history_identity: DecisionContentDigest,
        canonical_record: Box<[u8]>,
    ) -> Result<Self, InvestmentProposalError> {
        if canonical_record.is_empty()
            || canonical_record.len() > Self::MAXIMUM_RECORD_BYTES
            || origin_at > source_cutoff
        {
            return Err(InvestmentProposalError::InvalidEvidenceMetric);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/saved-forecast-chart/v1\0");
        hash.update(instrument_id.as_uuid().as_bytes());
        hash.update(currency.as_str().as_bytes());
        hash.update(source_cutoff.unix_nanos().to_be_bytes());
        hash.update(origin_at.unix_nanos().to_be_bytes());
        hash.update(vintage_id.bytes());
        hash.update(basis_identity.evidence_digest().bytes());
        hash.update(history_identity.evidence_digest().bytes());
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
            origin_at,
            vintage_id,
            basis_identity,
            history_identity,
            canonical_record,
            digest,
        })
    }
    /// Exact forecast subject.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Original quote currency.
    #[must_use]
    pub const fn currency(&self) -> Currency {
        self.currency
    }
    /// Original source knowledge cutoff, never restart time.
    #[must_use]
    pub const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
    /// Original forecast source observation.
    #[must_use]
    pub const fn origin_at(&self) -> Timestamp {
        self.origin_at
    }
    /// Original immutable price vintage.
    #[must_use]
    pub const fn vintage_id(&self) -> ProposalForecastVintageId {
        self.vintage_id
    }
    /// Exact original split-share unit identity.
    #[must_use]
    pub const fn basis_identity(&self) -> DecisionContentDigest {
        self.basis_identity
    }
    /// Exact replayed history identity, including gaps and original source parents.
    #[must_use]
    pub const fn history_identity(&self) -> DecisionContentDigest {
        self.history_identity
    }
    /// Exact bounded application record for strict decoding and source reopening.
    #[must_use]
    pub fn canonical_record(&self) -> &[u8] {
        &self.canonical_record
    }
    /// Commitment included in the investment evidence identity.
    #[must_use]
    pub const fn digest(&self) -> DecisionContentDigest {
        self.digest
    }
}
