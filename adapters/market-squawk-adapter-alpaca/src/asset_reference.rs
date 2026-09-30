//! Authenticated Alpaca asset UUID evidence from one physically sealed original response.
//! Trading flags are deliberately discarded: this reference grants no order authority.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{
    AssetClass, CoverageDelay, Currency, DeliveryEvidence, DigestAlgorithm, EvidenceDigest,
    ExactPayloadEvidence, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
    VersionPinnedSourceLocator,
};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationMode, BudgetDispatchDecision, BudgetReservationDecision,
    EndpointPolicy, HttpRequestBounds, NetworkAccessPolicy, PathScope, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, ProviderWholeCaptureToken,
    SealedProviderCaptureMaterial, SealedProviderCaptureSetReceipt, SharedProviderBudget,
    SourceClass, SourceMetadata, SourceProtocolProfile, apply_http_retry_after,
};
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::historical_calendar::{
    authenticated_bounded_get, hardened_client, singleton_bounded_header,
};
use crate::{AlpacaCredentials, AlpacaError};

/// Read-only authenticated Paper asset reference route; a symbol or UUID is appended as one
/// percent-encoded path segment after independent request validation.
pub const ALPACA_ASSET_REFERENCE_ENDPOINT: &str = "https://paper-api.alpaca.markets/v2/assets";
const MAX_BODY_BYTES: usize = 64 * 1024;
const USER_AGENT: &str = "market-squawk/0.1 alpaca-asset-reference";

// Primary-source denomination contract checked 2026-09-30. It applies to the existing IEX
// startup snapshots request with no currency parameter; it is not an asset-response field.
const US_EQUITY_SNAPSHOT_CURRENCY_EXCERPT: &[u8] =
    b"GET https://data.alpaca.markets/v2/stocks/snapshots\ncurrency: The currency of all prices in ISO 4217 format. Default: USD.\n";
const US_EQUITY_SNAPSHOT_CURRENCY_SHA256: &str =
    "a3a45e964d08bdcfdbb2da373e8421d83d60bbc5d8e92ffc3a5df9d0559c1e1a";
const US_EQUITY_SNAPSHOT_CURRENCY_SOURCE: &str =
    "https://docs.alpaca.markets/us/reference/stocksnapshots-1";

/// Exact provider asset metadata, issued only after physical seal rejoin.
pub struct AlpacaOriginalAssetReference {
    id: Uuid,
    symbol: Box<str>,
    exchange: Box<str>,
    received_at: Timestamp,
    capture: SealedProviderCaptureSetReceipt,
}

impl AlpacaOriginalAssetReference {
    /// Physical rejoin exposes this value only after the exact response declares active us_equity.
    /// This classification does not establish common-share or execution eligibility.
    pub const fn is_active_us_equity(&self) -> bool {
        true
    }

    /// Quote denomination for the existing IEX snapshots request with currency omitted.
    /// The unit comes from the provider request contract, never from an issuer assumption.
    pub fn quote_currency(&self) -> Result<Currency, AlpacaError> {
        self.quote_currency_evidence()?;
        Currency::try_from("USD").map_err(|_| AlpacaError::Protocol)
    }

    /// Exact hash-pinned primary-source excerpt for the default snapshot denomination.
    pub fn quote_currency_evidence(&self) -> Result<ExactPayloadEvidence, AlpacaError> {
        let digest = hash(US_EQUITY_SNAPSHOT_CURRENCY_EXCERPT);
        let actual: String = digest
            .bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if actual != US_EQUITY_SNAPSHOT_CURRENCY_SHA256 {
            return Err(AlpacaError::Protocol);
        }
        Ok(ExactPayloadEvidence::with_version_pinned_locator(
            digest,
            VersionPinnedSourceLocator::new(
                SourceIdentifier::try_from(US_EQUITY_SNAPSHOT_CURRENCY_SOURCE)?,
                SourceIdentifier::try_from(format!("sha256:{actual}"))?,
            ),
        ))
    }

    pub const fn id(&self) -> Uuid {
        self.id
    }
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    /// Provider primary exchange code, independent of the IEX feed venue.
    pub fn exchange(&self) -> &str {
        &self.exchange
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.capture
    }
}

/// Source-owned one-symbol client. The caller must seal the pending page before decoding;
/// the catalog consumes its whole-capture token in the same transaction as identity update.
pub struct AlpacaAssetReferenceClient {
    metadata: SourceMetadata,
    bounds: HttpRequestBounds,
    client: reqwest::Client,
}

impl AlpacaAssetReferenceClient {
    pub fn try_new(
        metadata: SourceMetadata,
        bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        let [venue] = metadata.coverage().topology().venues() else {
            return Err(AlpacaError::InvalidCoverage);
        };
        if metadata.provider().as_str() != "alpaca-market-data"
            || metadata.source_class() != SourceClass::Broker
            || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.protocol_profile() != &SourceProtocolProfile::NotLive
            || !metadata
                .coverage()
                .asset_classes()
                .contains(&AssetClass::Equity)
            || metadata.coverage().delay() != CoverageDelay::NotApplicable
            || metadata.coverage().delivery() != DeliveryEvidence::AuthorizedBroker
            || !metadata.coverage().live_channels().is_empty()
            || venue.as_str() != "iex"
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        let network = NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_from_api_rules(
            vec![ApiEndpointRule::try_new(
                ALPACA_ASSET_REFERENCE_ENDPOINT,
                PathScope::Descendants,
                vec![],
                1,
                128,
            )?],
            bounds,
        )?);
        if metadata.network_policy() != &network {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(Self {
            metadata,
            bounds,
            client: hardened_client(bounds, USER_AGENT)?,
        })
    }

    pub async fn acquire<T, Retain, Retained>(
        &self,
        credentials: &AlpacaCredentials,
        budget: &SharedProviderBudget,
        symbol: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
        retain: Retain,
    ) -> Result<T, AlpacaError>
    where
        Retain: FnOnce(AlpacaPendingAssetReference) -> Retained,
        Retained: std::future::Future<Output = Result<T, AlpacaError>>,
    {
        ensure_active(deadline, cancellation)?;
        let url = asset_url(symbol)?;
        self.metadata.network_policy().authorize(url.as_str())?;
        let permit = acquire_permit(budget, deadline, cancellation).await?;
        let response = authenticated_bounded_get(
            &self.client,
            credentials,
            &url,
            self.bounds,
            MAX_BODY_BYTES,
            deadline,
            cancellation,
        )
        .await?;
        if matches!(response.status, 429 | 503) {
            let retry = singleton_bounded_header(&response.headers, RETRY_AFTER, 128)?;
            let _ = apply_http_retry_after(budget, retry.as_deref(), 1_000);
            permit.release();
            return Err(AlpacaError::Network);
        }
        if matches!(response.status, 401 | 403) {
            permit.release();
            return Err(AlpacaError::InvalidAuthorization);
        }
        if response.status != 200 {
            permit.release();
            return Err(AlpacaError::Protocol);
        }
        permit.release();
        // Physical sealing must finish before response semantics or cancellation can discard it.
        let pending = AlpacaPendingAssetReference::from_body(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            symbol.to_owned(),
            Bytes::from(response.body),
            response.received_at,
        )?;
        let retained = retain(pending).await?;
        let media = singleton_bounded_header(&response.headers, CONTENT_TYPE, 128)?;
        if !media.as_deref().is_some_and(|value| {
            value.eq_ignore_ascii_case(b"application/json")
                || value.eq_ignore_ascii_case(b"application/json; charset=utf-8")
        }) || !self.metadata.is_effective_at(response.received_at)
        {
            return Err(AlpacaError::Protocol);
        }
        budget.record_success().map_err(|_| AlpacaError::Network)?;
        ensure_active(deadline, cancellation)?;
        Ok(retained)
    }
}

/// Raw original before catalog custody and physical seal rejoin.
pub struct AlpacaPendingAssetReference {
    source: SourceId,
    revision: MetadataRevision,
    symbol: String,
    body: Bytes,
    received_at: Timestamp,
}

impl AlpacaPendingAssetReference {
    /// Reopens pending decoding from exact original catalog receipts and raw envelopes.
    /// As with live acquisition, physical seal rejoin is required before reference admission.
    pub fn restore_original(
        expected: &ProviderCaptureSetReceipt,
        records: &[RawCaptureRecord],
    ) -> Result<Self, AlpacaError> {
        let ([page], [record]) = (expected.pages(), records) else {
            return Err(AlpacaError::CaptureMaterial);
        };
        let symbol = expected
            .dataset()
            .as_str()
            .strip_prefix("alpaca:asset-reference:")
            .ok_or(AlpacaError::CaptureMaterial)?;
        let pending = Self::from_body(
            expected.source_id().clone(),
            expected.metadata_revision().clone(),
            symbol.to_owned(),
            Bytes::copy_from_slice(record.payload()),
            page.received_at(),
        )?;
        decode(&pending.body, symbol)?;
        let material = pending.material()?;
        if material.receipt() != expected || material.records() != records {
            return Err(AlpacaError::CaptureMaterial);
        }
        ProviderCaptureMaterial::try_new(expected.clone(), records.to_vec())
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        Ok(pending)
    }

    fn from_body(
        source: SourceId,
        revision: MetadataRevision,
        symbol: String,
        body: Bytes,
        received_at: Timestamp,
    ) -> Result<Self, AlpacaError> {
        asset_url(&symbol)?;
        if body.is_empty() || body.len() > MAX_BODY_BYTES {
            return Err(AlpacaError::BodyTooLarge);
        }
        Ok(Self {
            source,
            revision,
            symbol,
            body,
            received_at,
        })
    }

    pub fn into_seal_parts(
        self,
    ) -> Result<(AlpacaAssetReferenceRejoin, ProviderCaptureSealRequest), AlpacaError> {
        let (expectation, request) = self.material()?.into_whole_seal_parts();
        Ok((
            AlpacaAssetReferenceRejoin {
                page: self,
                expectation,
            },
            request,
        ))
    }

    fn material(&self) -> Result<ProviderCaptureMaterial, AlpacaError> {
        let url = asset_url(&self.symbol)?;
        let request_identity = hash(url.as_str().as_bytes());
        let receipt = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            200,
            u64::try_from(self.body.len()).map_err(|_| AlpacaError::BodyTooLarge)?,
            hash(&self.body),
            self.received_at,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        let capture = ProviderCaptureSetReceipt::try_new(
            self.source.clone(),
            self.revision.clone(),
            SourceIdentifier::try_from(format!("alpaca:asset-reference:{}", self.symbol))?,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![receipt],
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        let connection_id =
            Uuid::new_v5(&Uuid::NAMESPACE_URL, &capture.observation_digest().bytes());
        let event_id = Uuid::new_v5(&connection_id, &hash(&self.body).bytes());
        let record = RawCaptureRecord::try_new_live(
            event_id,
            Arc::from(self.source.as_str()),
            connection_id,
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(self.received_at.unix_nanos()),
            self.body.clone(),
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        ProviderCaptureMaterial::try_new(capture, vec![record])
            .map_err(|_| AlpacaError::CaptureMaterial)
    }
}

pub struct AlpacaAssetReferenceRejoin {
    page: AlpacaPendingAssetReference,
    expectation: ProviderCaptureSealExpectation,
}

impl AlpacaAssetReferenceRejoin {
    pub fn into_original_token(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<ProviderWholeCaptureToken, AlpacaError> {
        self.expectation
            .try_rejoin(sealed)
            .and_then(|value| value.try_into_whole())
            .map_err(|_| AlpacaError::CaptureMaterial)
    }

    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<(AlpacaOriginalAssetReference, ProviderWholeCaptureToken), AlpacaError> {
        let authority = self
            .expectation
            .try_rejoin(sealed)
            .and_then(|value| value.try_into_whole())
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let wire = decode(&self.page.body, &self.page.symbol)?;
        let asset = AlpacaOriginalAssetReference {
            id: wire.id,
            symbol: wire.symbol.into_boxed_str(),
            exchange: wire.exchange.into_boxed_str(),
            received_at: self.page.received_at,
            capture: authority.persisted_receipt().clone(),
        };
        Ok((asset, authority))
    }
}

#[derive(Deserialize)]
struct WireAsset {
    id: Uuid,
    symbol: String,
    exchange: String,
    #[serde(rename = "class")]
    class: String,
    status: String,
}

fn decode(body: &[u8], expected_symbol: &str) -> Result<WireAsset, AlpacaError> {
    let wire: WireAsset = serde_json::from_slice(body).map_err(|_| AlpacaError::Protocol)?;
    if wire.id.is_nil()
        || wire.symbol != expected_symbol
        || wire.class != "us_equity"
        || wire.status != "active"
        || !matches!(
            wire.exchange.as_str(),
            "AMEX" | "ARCA" | "BATS" | "NYSE" | "NASDAQ" | "NYSEARCA"
        )
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(wire)
}

fn asset_url(symbol: &str) -> Result<Url, AlpacaError> {
    crate::config::validate_equity_symbol(symbol)?;
    let mut url = Url::parse(ALPACA_ASSET_REFERENCE_ENDPOINT).map_err(|_| AlpacaError::Protocol)?;
    url.path_segments_mut()
        .map_err(|_| AlpacaError::Protocol)?
        .push(symbol);
    if url.query().is_some() {
        return Err(AlpacaError::Protocol);
    }
    Ok(url)
}

fn hash(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn ensure_active(deadline: Instant, cancellation: &CancellationToken) -> Result<(), AlpacaError> {
    if cancellation.is_cancelled() {
        Err(AlpacaError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(AlpacaError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

async fn acquire_permit(
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<market_squawk_sources::BudgetPermit, AlpacaError> {
    loop {
        ensure_active(deadline, cancellation)?;
        let wait_until = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => match reservation.commit_dispatch() {
                BudgetDispatchDecision::Ready(permit) => return Ok(permit),
                BudgetDispatchDecision::WaitUntil(time) => time,
                BudgetDispatchDecision::Unavailable(_) => return Err(AlpacaError::Network),
            },
            BudgetReservationDecision::WaitUntil(time) => time,
            BudgetReservationDecision::Unavailable(_) => return Err(AlpacaError::Network),
        };
        let wait = budget
            .remaining_wait(wait_until)
            .map_err(|_| AlpacaError::Network)?;
        if wait
            > deadline
                .checked_duration_since(Instant::now())
                .ok_or(AlpacaError::DeadlineExceeded)?
        {
            return Err(AlpacaError::DeadlineExceeded);
        }
        tokio::select! { biased;
            () = cancellation.cancelled() => return Err(AlpacaError::Cancelled),
            () = tokio::time::sleep(wait) => {},
        }
    }
}
