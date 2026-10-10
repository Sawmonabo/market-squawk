//! Exact physically sealed Instruments detail evidence for the shared reference publisher.
//!
//! This projection is provider-native. It does not assign an InstrumentId, a listing, a currency,
//! an identifier entitlement, or canonical assignment verification.

use std::fmt;

use market_squawk_domain::{
    Cusip, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, Timestamp,
};
use market_squawk_sources::{ProviderWholeCaptureToken, SealedProviderCaptureSetReceipt};
use thiserror::Error;
use url::Url;

use crate::transport::SchwabRestPayload;
use crate::{
    RawRestResponseReceipt, ReadOnlyRoute, SchwabCanonicalError, SchwabCanonicalField,
    SchwabCaptureCoordinates, SchwabInstrumentCandidate, SchwabSealedRestResponse,
    canonicalize_instrument_candidates,
};

/// One exact native security assertion inseparable from the consumed original capture authority.
/// Neither this type nor its capture can be constructed, cloned, or deserialized by application
/// callers. The existing shared catalog publisher remains the canonical identity owner.
pub struct SchwabSealedInstrumentReference {
    coordinates: SchwabCaptureCoordinates,
    receipt: RawRestResponseReceipt,
    token: ProviderWholeCaptureToken,
    candidate: SchwabInstrumentCandidate,
    requested_cusip: Cusip,
    received_at: Timestamp,
}

impl fmt::Debug for SchwabSealedInstrumentReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabSealedInstrumentReference")
            .field("route", &self.receipt.route())
            .field("source", self.coordinates.source_id())
            .field("received_at", &self.received_at)
            .field("native_reference", &"EXACT SEALED PROVIDER ASSERTION")
            .finish_non_exhaustive()
    }
}

impl SchwabSealedInstrumentReference {
    /// Consumes one exact detail lookup, rejecting search ambiguity and partial/unexpected rows.
    /// All native optional fields retain their original Absent/Null/Value states.
    pub fn try_from_detail(
        response: SchwabSealedRestResponse,
        expected_cusip: &Cusip,
    ) -> Result<Self, SchwabInstrumentReferenceError> {
        if response.route() != ReadOnlyRoute::InstrumentByCusip
            || response.receipt().status() != 200
            || response.accounting().requested != 1
            || response.accounting().returned != 1
            || response.accounting().missing != 0
            || response.accounting().unexpected != 0
            || response.accounting().provider_records != 1
        {
            return Err(SchwabInstrumentReferenceError::ScopeMismatch);
        }
        let request_url = Url::parse(response.receipt().request_url())
            .map_err(|_| SchwabInstrumentReferenceError::ScopeMismatch)?;
        if request_url
            .path_segments()
            .and_then(|segments| segments.last())
            != Some(expected_cusip.as_str())
        {
            return Err(SchwabInstrumentReferenceError::ScopeMismatch);
        }
        let received_at = response
            .receipt()
            .received_at_unix_millis()
            .checked_mul(1_000_000)
            .and_then(|nanos| i64::try_from(nanos).ok())
            .map(Timestamp::from_unix_nanos)
            .ok_or(SchwabInstrumentReferenceError::InvalidClock)?;
        let parts = response.into_parts();
        let SchwabRestPayload::Instruments(parsed) = &parts.payload else {
            return Err(SchwabInstrumentReferenceError::ScopeMismatch);
        };
        // The consuming seal rejoin already proved this equality against the physical body.
        // Preserve the check at the family-specific projection boundary as well.
        if parsed.raw_sha256() != parts.receipt.body_sha256() {
            return Err(SchwabInstrumentReferenceError::EvidenceMismatch);
        }
        let mut candidates = canonicalize_instrument_candidates(parsed)?;
        if candidates.len() != 1 {
            return Err(SchwabInstrumentReferenceError::ScopeMismatch);
        }
        let candidate = candidates
            .pop()
            .ok_or(SchwabInstrumentReferenceError::ScopeMismatch)?;
        if !matches!(&candidate.cusip, SchwabCanonicalField::Value(value) if value.as_ref() == expected_cusip.as_str())
            || candidate.response_sha256 != parts.receipt.body_sha256()
        {
            return Err(SchwabInstrumentReferenceError::EvidenceMismatch);
        }
        Ok(Self {
            coordinates: parts.coordinates,
            receipt: parts.receipt,
            token: parts.token,
            candidate,
            requested_cusip: expected_cusip.clone(),
            received_at,
        })
    }

    pub const fn candidate(&self) -> &SchwabInstrumentCandidate {
        &self.candidate
    }
    pub const fn requested_cusip(&self) -> &Cusip {
        &self.requested_cusip
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn coordinates(&self) -> &SchwabCaptureCoordinates {
        &self.coordinates
    }
    pub const fn receipt(&self) -> &RawRestResponseReceipt {
        &self.receipt
    }
    pub fn evidence(&self) -> ExactPayloadEvidence {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            self.receipt.body_sha256(),
        ))
    }
    pub fn persisted_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        self.token.persisted_receipt()
    }

    /// Transfers the original one-use physical capture into the shared catalog publication.
    /// Borrowed native fields must be checked before consuming this projection.
    pub fn into_capture_token(self) -> ProviderWholeCaptureToken {
        self.token
    }
}

#[derive(Debug, Error)]
pub enum SchwabInstrumentReferenceError {
    #[error("instrument detail response does not cover the exact selected lookup")]
    ScopeMismatch,
    #[error("instrument detail does not match the retained physical response")]
    EvidenceMismatch,
    #[error("instrument detail receipt time cannot be represented")]
    InvalidClock,
    #[error(transparent)]
    Canonical(#[from] SchwabCanonicalError),
}
