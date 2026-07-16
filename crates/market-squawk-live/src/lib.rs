//! Deterministic instrument-owned live state and current execution authority.

mod authority;
mod book;
mod integrity;
mod normalization;
mod processor;
mod provider_book;
mod qualification;
mod state;

pub use authority::{AuthorityError, ConsumedLiveAuthority, LiveExecutionCapability};
pub use book::{BookError, BookSide, DepthLimit, LevelUpdate, MAX_BOOK_MESSAGE_ITEMS, ScaledBook};
pub use integrity::{
    ChecksumValidationError, KRAKEN_V2_CANONICALIZATION_ID, KRAKEN_V2_SCOPE_ID,
    ResolvedChecksumValidator, SequenceTracker, SequenceValidationError, kraken_v2_crc32,
};
pub use normalization::{
    NormalizationError, normalize_delta_quantity, normalize_positive_quantity, normalize_price,
};
pub use state::{GenerationPhase, GenerationStateError, GenerationStateMachine};
