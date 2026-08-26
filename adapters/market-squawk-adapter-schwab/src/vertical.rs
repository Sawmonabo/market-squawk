//! Family-scoped observed capability evidence.
//!
//! A successful request from one Schwab family never authorizes another family. Raw market-data
//! capture is handed to the shared provider-capture authority; this module retains only the
//! minimum parsed User Preference evidence needed to establish a price-history capability.

use std::fmt;
use std::time::Duration;

use market_squawk_domain::Timestamp;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, ExecutedRestResponse, ReadOnlyRoute,
    SchwabOAuthAuthorityReceipt, SchwabRestPayload, SchwabUserPreferenceEvidence,
};

/// The sole family for which this adapter currently observes scoped capability evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabObservedCapabilityFamily {
    DailyPriceHistory,
}

/// Currentness of one exact family observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabCapabilityCurrentness {
    Current,
    Expired,
    TokenGenerationChanged,
    OAuthAuthorityChanged,
    PrincipalChanged,
    BootstrapChanged,
    ProbeChanged,
}

/// Fresh User Preference plus exact daily price-history response evidence.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SchwabPriceHistoryCapabilityObservation {
    observed_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    oauth_authority: SchwabOAuthAuthorityReceipt,
    market_data_principal_sha256: [u8; 32],
    user_preference_receipt_sha256: [u8; 32],
    price_history_receipt_sha256: [u8; 32],
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    returned_candles: u64,
    receipt_sha256: [u8; 32],
}

impl fmt::Debug for SchwabPriceHistoryCapabilityObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPriceHistoryCapabilityObservation")
            .field("family", &self.family())
            .field("observed_at_unix_seconds", &self.observed_at_unix_seconds)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("oauth_authority", &self.oauth_authority)
            .field("market_data_principal", &"[ONE-WAY BINDING]")
            .field("user_preference_receipt", &"[DIGEST]")
            .field("price_history_receipt", &"[DIGEST]")
            .field("returned_candles", &self.returned_candles)
            .field("receipt", &"[DIGEST]")
            .finish()
    }
}

impl SchwabPriceHistoryCapabilityObservation {
    /// Observes only an exact daily, frequency-one, explicit-range price-history probe.
    pub fn try_observe(
        oauth_authority: SchwabOAuthAuthorityReceipt,
        user_preference_probe: &SchwabUserPreferenceEvidence,
        price_history_probe: &ExecutedRestResponse,
        observed_at_unix_seconds: u64,
        valid_for: Duration,
    ) -> Result<Self, SchwabVerticalError> {
        let preference_receipt = user_preference_probe.receipt();
        let history_receipt = price_history_probe.capture().receipt();
        let bootstrap = user_preference_probe.bootstrap();
        let SchwabRestPayload::PriceHistory(history) = price_history_probe.payload() else {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        };
        let Some((_start, _end)) = exact_daily_range(history_receipt.request_url()) else {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        };
        let preference_accounting = user_preference_probe.accounting();
        let history_accounting = price_history_probe.accounting();
        let expires_at_unix_seconds = observed_at_unix_seconds
            .checked_add(valid_for.as_secs())
            .ok_or(SchwabVerticalError::Overflow)?;
        if valid_for.is_zero()
            || valid_for > Duration::from_secs(ACCESS_TOKEN_MAX_LIFETIME_SECONDS)
            || observed_at_unix_seconds < oauth_authority.access_issued_at_unix_seconds()
            || observed_at_unix_seconds >= oauth_authority.access_expires_at_unix_seconds()
            || expires_at_unix_seconds > oauth_authority.access_expires_at_unix_seconds()
            || oauth_authority.generation() != preference_receipt.token_generation()
            || oauth_authority.generation() != history_receipt.token_generation()
            || preference_receipt.route() != ReadOnlyRoute::UserPreference
            || history_receipt.route() != ReadOnlyRoute::PriceHistory
            || preference_receipt.token_generation() != history_receipt.token_generation()
            || preference_receipt.status() != 200
            || history_receipt.status() != 200
            || preference_receipt.body_sha256() != bootstrap.raw_sha256()
            || history_receipt.body_sha256() != history.raw_sha256()
            || bootstrap.value().market_data_principal_sha256() == [0; 32]
            || preference_accounting.requested != 1
            || preference_accounting.returned != 1
            || preference_accounting.missing != 0
            || preference_accounting.unexpected != 0
            || preference_accounting.provider_records != 1
            || history_accounting.requested != 1
            || history_accounting.returned != 1
            || history_accounting.missing != 0
            || history_accounting.unexpected != 0
            || history_accounting.provider_records == 0
            || history.value().empty
            || history.value().candles().is_empty()
            || u64::try_from(history.value().candles().len())
                .ok()
                .is_none_or(|count| count != history_accounting.provider_records)
            || preference_receipt.received_at_unix_millis()
                > history_receipt.received_at_unix_millis()
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        validate_observed_clock(
            preference_receipt.received_at_unix_millis(),
            observed_at_unix_seconds,
            valid_for,
        )?;
        validate_observed_clock(
            history_receipt.received_at_unix_millis(),
            observed_at_unix_seconds,
            valid_for,
        )?;
        if expires_at_unix_seconds <= observed_at_unix_seconds {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        let market_data_principal_sha256 = bootstrap.value().market_data_principal_sha256();
        let user_preference_receipt_sha256 = user_preference_receipt_digest(user_preference_probe);
        let price_history_receipt_sha256 = rest_receipt_digest(price_history_probe);
        let request_sha256 = history_receipt.request_sha256();
        let response_sha256 = history_receipt.body_sha256();
        let returned_candles = history_accounting.provider_records;
        let receipt_sha256 = capability_digest(
            observed_at_unix_seconds,
            expires_at_unix_seconds,
            oauth_authority,
            market_data_principal_sha256,
            user_preference_receipt_sha256,
            price_history_receipt_sha256,
            request_sha256,
            response_sha256,
            returned_candles,
        );
        Ok(Self {
            observed_at_unix_seconds,
            expires_at_unix_seconds,
            oauth_authority,
            market_data_principal_sha256,
            user_preference_receipt_sha256,
            price_history_receipt_sha256,
            request_sha256,
            response_sha256,
            returned_candles,
            receipt_sha256,
        })
    }

    pub const fn family(self) -> SchwabObservedCapabilityFamily {
        SchwabObservedCapabilityFamily::DailyPriceHistory
    }
    pub const fn receipt_sha256(self) -> [u8; 32] {
        self.receipt_sha256
    }
    pub const fn expires_at_unix_seconds(self) -> u64 {
        self.expires_at_unix_seconds
    }

    pub fn currentness(
        self,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        user_preference_probe: &SchwabUserPreferenceEvidence,
        price_history_probe: &ExecutedRestResponse,
        now_unix_seconds: u64,
    ) -> SchwabCapabilityCurrentness {
        if now_unix_seconds < self.observed_at_unix_seconds
            || now_unix_seconds >= self.expires_at_unix_seconds
            || now_unix_seconds < oauth_authority.access_issued_at_unix_seconds()
            || now_unix_seconds >= oauth_authority.access_expires_at_unix_seconds()
        {
            return SchwabCapabilityCurrentness::Expired;
        }
        let preference_receipt = user_preference_probe.receipt();
        let receipt = price_history_probe.capture().receipt();
        if preference_receipt.token_generation() != oauth_authority.generation()
            || receipt.token_generation() != oauth_authority.generation()
        {
            return SchwabCapabilityCurrentness::TokenGenerationChanged;
        }
        if oauth_authority != self.oauth_authority {
            return SchwabCapabilityCurrentness::OAuthAuthorityChanged;
        }
        let bootstrap = user_preference_probe.bootstrap();
        if bootstrap.value().market_data_principal_sha256() != self.market_data_principal_sha256 {
            return SchwabCapabilityCurrentness::PrincipalChanged;
        }
        let accounting = user_preference_probe.accounting();
        if preference_receipt.route() != ReadOnlyRoute::UserPreference
            || preference_receipt.status() != 200
            || preference_receipt.body_sha256() != bootstrap.raw_sha256()
            || accounting.requested != 1
            || accounting.returned != 1
            || accounting.missing != 0
            || accounting.unexpected != 0
            || accounting.provider_records != 1
            || user_preference_receipt_digest(user_preference_probe)
                != self.user_preference_receipt_sha256
            || preference_receipt.received_at_unix_millis() > receipt.received_at_unix_millis()
        {
            return SchwabCapabilityCurrentness::BootstrapChanged;
        }
        if receipt.route() != ReadOnlyRoute::PriceHistory
            || receipt.request_sha256() != self.request_sha256
            || receipt.body_sha256() != self.response_sha256
            || rest_receipt_digest(price_history_probe) != self.price_history_receipt_sha256
        {
            return SchwabCapabilityCurrentness::ProbeChanged;
        }
        SchwabCapabilityCurrentness::Current
    }
}

fn exact_daily_range(url: &str) -> Option<(Timestamp, Timestamp)> {
    let url = url::Url::parse(url).ok()?;
    let mut frequency_type = None;
    let mut frequency = None;
    let mut start = None;
    let mut end = None;
    let mut has_period_type = false;
    let mut has_period = false;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "frequencyType" => frequency_type = Some(value.into_owned()),
            "frequency" => frequency = Some(value.into_owned()),
            "startDate" => start = value.parse::<u64>().ok(),
            "endDate" => end = value.parse::<u64>().ok(),
            "periodType" => has_period_type = true,
            "period" => has_period = true,
            _ => {}
        }
    }
    if frequency_type.as_deref() != Some("daily")
        || frequency.as_deref() != Some("1")
        || has_period_type
        || has_period
    {
        return None;
    }
    let start = start?;
    let end = end?;
    if start >= end {
        return None;
    }
    Some((millis_timestamp(start)?, millis_timestamp(end)?))
}

pub(crate) fn admitted_daily_range(
    response: &ExecutedRestResponse,
) -> Result<(Timestamp, Timestamp), SchwabVerticalError> {
    if response.capture().receipt().route() != ReadOnlyRoute::PriceHistory {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    exact_daily_range(response.capture().receipt().request_url())
        .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)
}

fn millis_timestamp(value: u64) -> Option<Timestamp> {
    i64::try_from(value)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .map(Timestamp::from_unix_nanos)
}

fn validate_observed_clock(
    received_at_unix_millis: u64,
    observed_at_unix_seconds: u64,
    valid_for: Duration,
) -> Result<(), SchwabVerticalError> {
    let ceiling = observed_at_unix_seconds
        .checked_add(1)
        .and_then(|value| value.checked_mul(1_000))
        .and_then(|value| value.checked_sub(1))
        .ok_or(SchwabVerticalError::Overflow)?;
    let maximum_age =
        u64::try_from(valid_for.as_millis()).map_err(|_| SchwabVerticalError::Overflow)?;
    if received_at_unix_millis > ceiling || ceiling - received_at_unix_millis > maximum_age {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    Ok(())
}

pub(crate) fn rest_receipt_digest(response: &ExecutedRestResponse) -> [u8; 32] {
    receipt_digest(response.capture().receipt(), response.accounting())
}

pub(crate) fn user_preference_receipt_digest(response: &SchwabUserPreferenceEvidence) -> [u8; 32] {
    receipt_digest(response.receipt(), response.accounting())
}

fn receipt_digest(
    receipt: &crate::RawRestResponseReceipt,
    accounting: crate::RestItemAccounting,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-rest-observation/v1");
    hasher.update([route_tag(receipt.route())]);
    hasher.update(receipt.token_generation().get().to_be_bytes());
    hasher.update(receipt.request_sha256());
    hasher.update(receipt.status().to_be_bytes());
    hasher.update(receipt.received_at_unix_millis().to_be_bytes());
    hasher.update(receipt.body_bytes().to_be_bytes());
    hasher.update(receipt.body_sha256());
    for value in [
        accounting.requested,
        accounting.returned,
        accounting.missing,
        accounting.unexpected,
        accounting.provider_records,
    ] {
        hasher.update(value.to_be_bytes());
    }
    hasher.finalize().into()
}

#[allow(
    clippy::too_many_arguments,
    reason = "all observed evidence stays explicit"
)]
fn capability_digest(
    observed_at: u64,
    expires_at: u64,
    oauth_authority: SchwabOAuthAuthorityReceipt,
    principal: [u8; 32],
    preference_receipt: [u8; 32],
    history_receipt: [u8; 32],
    request: [u8; 32],
    response: [u8; 32],
    returned_candles: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-daily-price-history-capability/v1");
    hasher.update(observed_at.to_be_bytes());
    hasher.update(expires_at.to_be_bytes());
    hasher.update(oauth_authority.generation().get().to_be_bytes());
    hasher.update(
        oauth_authority
            .access_issued_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(
        oauth_authority
            .access_expires_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(
        oauth_authority
            .refresh_authorized_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(
        oauth_authority
            .refresh_expires_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(principal);
    hasher.update(preference_receipt);
    hasher.update(history_receipt);
    hasher.update(request);
    hasher.update(response);
    hasher.update(returned_candles.to_be_bytes());
    hasher.finalize().into()
}

const fn route_tag(route: ReadOnlyRoute) -> u8 {
    match route {
        ReadOnlyRoute::Quotes => 1,
        ReadOnlyRoute::SingleQuote => 2,
        ReadOnlyRoute::Chains => 3,
        ReadOnlyRoute::ExpirationChain => 4,
        ReadOnlyRoute::PriceHistory => 5,
        ReadOnlyRoute::Movers => 6,
        ReadOnlyRoute::Markets => 7,
        ReadOnlyRoute::SingleMarket => 8,
        ReadOnlyRoute::Instruments => 9,
        ReadOnlyRoute::InstrumentByCusip => 10,
        ReadOnlyRoute::UserPreference => 11,
    }
}

/// Secret-free provider-vertical failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabVerticalError {
    #[error("Schwab family capability evidence is incomplete or inconsistent")]
    InvalidCapabilityEvidence,
    #[error("Schwab provider evidence arithmetic overflowed")]
    Overflow,
}
