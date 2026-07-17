//! Provider sequence, snapshot, and checksum validation.

#[path = "integrity/checksum.rs"]
mod checksum;
#[path = "integrity/sequence.rs"]
mod sequence;

pub(crate) use checksum::ExactChecksumLevel;
pub use checksum::{
    ChecksumValidationError, KRAKEN_V2_CANONICALIZATION_ID, KRAKEN_V2_SCOPE_ID,
    ResolvedChecksumValidator, kraken_v2_crc32,
};
pub use sequence::{SequenceTracker, SequenceValidationError};
