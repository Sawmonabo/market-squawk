//! Closed provider checksum canonicalizer dispatch.

use std::num::NonZeroU16;

use crc32fast::Hasher;
use market_squawk_domain::{IntegrityRule, MarketDepth};
use market_squawk_sources::{
    ChecksumAlgorithm, ChecksumValidationProfile, ProviderBookLevel, ProviderChecksumEvidence,
};
use rust_decimal::Decimal;
use thiserror::Error;

/// Closed identity for Kraken WebSocket v2 book canonicalization revision 1.
pub const KRAKEN_V2_CANONICALIZATION_ID: &str = "kraken-ws-v2-book-checksum-v1";
/// Closed identity for Kraken's asks-then-bids, top-ten checksum scope.
pub const KRAKEN_V2_SCOPE_ID: &str = "asks-low-to-high-bids-high-to-low-top-10";

/// A resolved closed checksum implementation.
///
/// Resolution occurs once per mutable stream. Unknown algorithm/canonicalization/scope tuples are
/// rejected; no generic or best-effort checksum fallback exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedChecksumValidator {
    rule: IntegrityRule,
    level_count: NonZeroU16,
}

impl ResolvedChecksumValidator {
    /// Resolves a metadata profile to a closed implementation.
    ///
    /// # Errors
    ///
    /// Rejects unsupported tuples and a configured book depth smaller than the checksum scope.
    pub fn resolve(
        profile: &ChecksumValidationProfile,
        configured_depth: usize,
    ) -> Result<Self, ChecksumValidationError> {
        let ChecksumValidationProfile::Provided {
            rule,
            algorithm,
            canonicalization,
            scope,
            book_scope,
        } = profile
        else {
            return Err(ChecksumValidationError::UnsupportedProfile);
        };
        let Some(book_scope) = book_scope else {
            return Err(ChecksumValidationError::UnsupportedProfile);
        };
        let Some(level_count) = book_scope.level_count() else {
            return Err(ChecksumValidationError::UnsupportedProfile);
        };
        if *algorithm != ChecksumAlgorithm::Crc32IsoHdlc
            || canonicalization.as_str() != KRAKEN_V2_CANONICALIZATION_ID
            || scope.as_str() != KRAKEN_V2_SCOPE_ID
            || book_scope.depth() != MarketDepth::PriceLevel
            || level_count.get() != 10
        {
            return Err(ChecksumValidationError::UnsupportedProfile);
        }
        let required = usize::from(level_count.get());
        if configured_depth < required {
            return Err(ChecksumValidationError::InsufficientRetainedDepth {
                configured: configured_depth,
                required,
            });
        }
        Ok(Self {
            rule: rule.clone(),
            level_count,
        })
    }

    /// Validates an exact supplied checksum over the complete committed candidate.
    ///
    /// `asks` must be low-to-high and `bids` high-to-low. Exact provider lexemes, including
    /// trailing zeros, are retained by [`ProviderBookLevel`] and used directly.
    ///
    /// # Errors
    ///
    /// Rejects evidence/profile transplants, malformed checksum values, invalid ordering, and
    /// checksum mismatches.
    pub fn validate(
        &self,
        asks: &[ProviderBookLevel],
        bids: &[ProviderBookLevel],
        evidence: &ProviderChecksumEvidence,
    ) -> Result<u32, ChecksumValidationError> {
        self.validate_ordered(asks.iter(), bids.iter(), evidence)
    }

    /// Validates ordered retained book iterators without per-update canonical allocations.
    pub(crate) fn validate_ordered<'a, A, B>(
        &self,
        asks: A,
        bids: B,
        evidence: &ProviderChecksumEvidence,
    ) -> Result<u32, ChecksumValidationError>
    where
        A: IntoIterator<Item = &'a ProviderBookLevel>,
        B: IntoIterator<Item = &'a ProviderBookLevel>,
    {
        let ProviderChecksumEvidence::Provided { value, rule } = evidence else {
            return Err(ChecksumValidationError::EvidenceProfileMismatch);
        };
        if rule != &self.rule {
            return Err(ChecksumValidationError::EvidenceProfileMismatch);
        }
        let expected = value
            .as_str()
            .parse::<u32>()
            .map_err(|_| ChecksumValidationError::InvalidChecksumValue)?;
        let computed = kraken_v2_crc32_ordered(asks, bids, self.level_count)?;
        if expected == computed {
            Ok(computed)
        } else {
            Err(ChecksumValidationError::Mismatch { expected, computed })
        }
    }
}

/// Computes the official Kraken WebSocket v2 book CRC32 from exact provider lexemes.
///
/// Asks are processed low-to-high, then bids high-to-low. For each price and quantity, only the
/// decimal point and leading zeros are removed. Trailing zeros remain checksum-significant.
///
/// # Errors
///
/// Rejects invalid side ordering, nonpositive levels, and bounded canonical-buffer allocation
/// failure.
pub fn kraken_v2_crc32(
    asks: &[ProviderBookLevel],
    bids: &[ProviderBookLevel],
    level_count: NonZeroU16,
) -> Result<u32, ChecksumValidationError> {
    let retained = usize::from(level_count.get());
    let retained_asks = &asks[..asks.len().min(retained)];
    let retained_bids = &bids[..bids.len().min(retained)];
    kraken_v2_crc32_ordered(retained_asks.iter(), retained_bids.iter(), level_count)
}

fn kraken_v2_crc32_ordered<'a, A, B>(
    asks: A,
    bids: B,
    level_count: NonZeroU16,
) -> Result<u32, ChecksumValidationError>
where
    A: IntoIterator<Item = &'a ProviderBookLevel>,
    B: IntoIterator<Item = &'a ProviderBookLevel>,
{
    let retained = usize::from(level_count.get());
    let mut hasher = Hasher::new();
    hash_side(&mut hasher, asks.into_iter().take(retained), true)?;
    hash_side(&mut hasher, bids.into_iter().take(retained), false)?;
    Ok(hasher.finalize())
}

fn append_component(hasher: &mut Hasher, value: &str) {
    let first_significant = value
        .as_bytes()
        .iter()
        .position(|byte| *byte != b'0' && *byte != b'.')
        .unwrap_or(value.len());
    for component in value.as_bytes()[first_significant..].split(|byte| *byte == b'.') {
        hasher.update(component);
    }
}

fn hash_side<'a>(
    hasher: &mut Hasher,
    levels: impl Iterator<Item = &'a ProviderBookLevel>,
    ascending: bool,
) -> Result<(), ChecksumValidationError> {
    let mut previous = None;
    for level in levels {
        let price = level.price().value().decimal();
        let quantity = level.quantity().value().decimal();
        if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
            return Err(ChecksumValidationError::NonPositiveLevel);
        }
        if previous.is_some_and(|prior| {
            if ascending {
                prior >= price
            } else {
                prior <= price
            }
        }) {
            return Err(ChecksumValidationError::InvalidOrdering);
        }
        append_component(hasher, level.price().value().as_str());
        append_component(hasher, level.quantity().value().as_str());
        previous = Some(price);
    }
    Ok(())
}

/// Provider checksum resolution or validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ChecksumValidationError {
    /// Metadata names no closed implementation supported by this binary.
    #[error("checksum profile is unsupported")]
    UnsupportedProfile,
    /// Configured retained depth cannot support the checksum scope.
    #[error("configured depth {configured} is smaller than required checksum depth {required}")]
    InsufficientRetainedDepth {
        /// Configured level count.
        configured: usize,
        /// Required level count.
        required: usize,
    },
    /// Observation evidence does not match the resolved profile.
    #[error("checksum evidence does not match resolved profile")]
    EvidenceProfileMismatch,
    /// Provider checksum text is not an unsigned 32-bit decimal.
    #[error("provider checksum value is invalid")]
    InvalidChecksumValue,
    /// Candidate levels are not in the required strict canonical order.
    #[error("checksum levels are not in strict canonical side order")]
    InvalidOrdering,
    /// Candidate checksum input contains a zero or negative live level.
    #[error("checksum candidate contains a nonpositive live level")]
    NonPositiveLevel,
    /// Provider checksum differs from the candidate computation.
    #[error("checksum mismatch: expected {expected}, computed {computed}")]
    Mismatch {
        /// Provider checksum.
        expected: u32,
        /// Locally computed checksum.
        computed: u32,
    },
    /// Bounded canonical buffer allocation failed.
    #[error("checksum canonical buffer allocation failed")]
    Allocation,
}
