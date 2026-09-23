//! Ordinary portfolio product identity and presentation helpers.

use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_domain::{AccountId, Timestamp};
use rust_decimal::Decimal;

use crate::application::opaque_product_text_token;

use super::{PortfolioApplicationServiceError, model::PortfolioReadImage};

const ACCOUNT_TOKEN_DOMAIN: &[u8] = b"market-squawk/product-portfolio-account/v1\0";
const ACCOUNT_TOKEN_PREFIX: &str = "portfolio_";
const MAXIMUM_PRODUCT_TOKEN_BYTES: usize = 512;

pub(super) struct ProductAccountBinding {
    account_id: AccountId,
    token: Box<str>,
    display_name: Box<str>,
}

impl ProductAccountBinding {
    pub(super) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(super) fn token(&self) -> &str {
        &self.token
    }

    pub(super) fn display_name(&self) -> &str {
        &self.display_name
    }
}

fn account_token(account_id: AccountId) -> Result<Box<str>, PortfolioApplicationServiceError> {
    let identity = account_id.to_string();
    opaque_product_text_token(
        ACCOUNT_TOKEN_PREFIX,
        ACCOUNT_TOKEN_DOMAIN,
        &[identity.as_bytes()],
        MAXIMUM_PRODUCT_TOKEN_BYTES,
    )
    .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
}

/// Prepares and collision-checks the complete bounded product account population.
pub(super) fn account_catalog(
    image: &PortfolioReadImage,
) -> Result<Vec<ProductAccountBinding>, PortfolioApplicationServiceError> {
    let mut catalog = Vec::new();
    catalog
        .try_reserve_exact(image.accounts.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for (index, account_id) in image.accounts.keys().copied().enumerate() {
        let ordinal = index
            .checked_add(1)
            .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
        catalog.push(ProductAccountBinding {
            account_id,
            token: account_token(account_id)?,
            display_name: format!("Portfolio {ordinal}").into_boxed_str(),
        });
    }
    catalog.sort_unstable_by(|left, right| left.token.cmp(&right.token));
    if catalog
        .windows(2)
        .any(|pair| pair[0].token == pair[1].token)
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    catalog.sort_unstable_by_key(|binding| binding.account_id());
    Ok(catalog)
}

/// Resolves an opaque product token only after validating the complete current population.
pub(super) fn resolve_account_token(
    catalog: &[ProductAccountBinding],
    token: &str,
) -> Result<AccountId, PortfolioApplicationServiceError> {
    if token.len() < 16 || token.len() > MAXIMUM_PRODUCT_TOKEN_BYTES {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let mut resolved = None;
    for binding in catalog {
        if binding.token() == token {
            if resolved.replace(binding.account_id()).is_some() {
                return Err(PortfolioApplicationServiceError::CorruptPublication);
            }
        }
    }
    resolved.ok_or(PortfolioApplicationServiceError::NotFound)
}

pub(super) fn account_display_name(
    image: &PortfolioReadImage,
    account_id: AccountId,
) -> Result<Box<str>, PortfolioApplicationServiceError> {
    let catalog = account_catalog(image)?;
    catalog
        .iter()
        .find(|binding| binding.account_id() == account_id)
        .map(|binding| binding.display_name.clone())
        .ok_or(PortfolioApplicationServiceError::NotFound)
}

pub(super) fn timestamp(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub(super) fn percentage(value: f64) -> Result<String, PortfolioApplicationServiceError> {
    let percent = value
        .is_finite()
        .then(|| value.to_string())
        .and_then(|value| Decimal::from_str_exact(&value).ok())
        .and_then(|value| value.checked_mul(Decimal::from(100_u32)))
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    Ok(format!("{}%", percent.normalize()))
}
