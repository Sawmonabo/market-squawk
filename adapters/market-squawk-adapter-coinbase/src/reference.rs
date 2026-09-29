//! Exact public Advanced Trade product reference, separate from live market frames.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Maximum bytes accepted for one public product object.
pub const MAX_COINBASE_PUBLIC_PRODUCT_BYTES: usize = 256 * 1024;
/// Official no-key Advanced Trade product resource.
pub const COINBASE_PUBLIC_PRODUCT_ENDPOINT: &str =
    "https://api.coinbase.com/api/v3/brokerage/market/products";

/// A source-authored spot product selected from one exact public response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbasePublicProductReference {
    product_id: String,
    base_currency: String,
    quote_currency: String,
    body_digest: EvidenceDigest,
}

impl CoinbasePublicProductReference {
    /// Decodes a bounded original response. The configured product is an expectation, not evidence.
    pub fn from_response(
        original_body: &[u8],
        requested_product: &str,
    ) -> Result<Self, CoinbasePublicProductReferenceError> {
        if original_body.is_empty() || original_body.len() > MAX_COINBASE_PUBLIC_PRODUCT_BYTES {
            return Err(CoinbasePublicProductReferenceError::InvalidSize);
        }
        let wire: ProductWire = serde_json::from_slice(original_body)
            .map_err(|_| CoinbasePublicProductReferenceError::InvalidResponse)?;
        if wire.product_id != requested_product
            || wire.product_type != "SPOT"
            || wire.is_disabled
            || wire.trading_disabled
            || wire.base_currency_id.is_empty()
            || wire.quote_currency_id.is_empty()
            || wire.product_id.len() > 64
            || wire.base_currency_id.len() > 16
            || wire.quote_currency_id.len() > 16
        {
            return Err(CoinbasePublicProductReferenceError::NotAdmittedSpotProduct);
        }
        let body_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(original_body).into(),
        );
        Ok(Self {
            product_id: wire.product_id,
            base_currency: wire.base_currency_id,
            quote_currency: wire.quote_currency_id,
            body_digest,
        })
    }

    /// Exact provider product ID carried in the original body.
    pub fn product_id(&self) -> &str {
        &self.product_id
    }
    /// Source-reported base currency ID.
    pub fn base_currency(&self) -> &str {
        &self.base_currency
    }
    /// Source-reported quote currency ID.
    pub fn quote_currency(&self) -> &str {
        &self.quote_currency
    }
    /// SHA-256 of the complete original response bytes.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }
}

#[derive(Deserialize)]
struct ProductWire {
    product_id: String,
    product_type: String,
    base_currency_id: String,
    quote_currency_id: String,
    is_disabled: bool,
    trading_disabled: bool,
}

/// Public reference is unavailable or no longer agrees with its requested spot product.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CoinbasePublicProductReferenceError {
    #[error("Coinbase public product response exceeds its bound or is empty")]
    InvalidSize,
    #[error("Coinbase public product response is malformed")]
    InvalidResponse,
    #[error("Coinbase public product is absent, disabled, or not the requested spot product")]
    NotAdmittedSpotProduct,
}
