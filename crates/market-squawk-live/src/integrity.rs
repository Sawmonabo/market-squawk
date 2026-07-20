//! Provider sequence, snapshot, and checksum validation.

#[path = "integrity/sequence.rs"]
mod sequence;

pub(crate) use market_squawk_sources::ExactChecksumLevel;
pub use market_squawk_sources::{
    ChecksumValidationError, KRAKEN_V2_CANONICALIZATION_ID, KRAKEN_V2_SCOPE_ID,
    ResolvedChecksumValidator, kraken_v2_crc32,
};
pub use sequence::{SequenceTracker, SequenceValidationError};
