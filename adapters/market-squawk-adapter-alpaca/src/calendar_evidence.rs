//! One unchanged digest recipe for physically replayed completed calendar evidence.

use crate::{AlpacaCalendarMarket, AlpacaRetainedCalendarSessions};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest as _, Sha256};

impl AlpacaRetainedCalendarSessions {
    /// Computes reconstruction evidence, not calendar or publication authority. The serving
    /// owner must verify the original metadata and creating publication before admitting it.
    pub fn completed_session_evidence_digest(
        &self,
        metadata_content: EvidenceDigest,
        published_at: Option<Timestamp>,
    ) -> EvidenceDigest {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/alpaca-retained-completed-calendar/v1\0");
        if self.market() == AlpacaCalendarMarket::Iex {
            digest.update(crate::calendar_decode::ALPACA_IEX_DAILY_AGGREGATION_RULE);
        } else {
            digest.update(b"market-squawk/alpaca-listed-market-native-sessions/v1\0");
        }
        digest.update(chrono_tz::IANA_TZDB_VERSION.as_bytes());
        digest.update(metadata_content.bytes());
        digest.update(self.capture_receipt_digest().bytes());
        digest.update(self.request_identity().bytes());
        if let Some(published_at) = published_at {
            digest.update(b"calendar-publication-clock\0");
            digest.update(published_at.unix_nanos().to_be_bytes());
        }
        digest.update(self.complete_from().unix_nanos().to_be_bytes());
        digest.update(self.complete_until().unix_nanos().to_be_bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
    }
}
