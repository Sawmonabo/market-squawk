//! Authenticated, read-only corporate-action acquisition with terminal native capture.
//!
//! Exhaustion proves the requested processing-date query completed. Alpaca explicitly makes no
//! guarantee about when an announced action appears, so exhaustion is not economic or historical
//! point-in-time completeness. All sixteen categories and early incomplete records are requested.

mod identity;
mod native;
mod publication;

pub use identity::AlpacaCorporateActionInstrument;
pub use native::{
    AlpacaCorporateActionCategory, AlpacaCorporateActionDate, AlpacaCorporateActionDates,
};
pub use publication::{
    AlpacaCorporateActionDisposition, AlpacaCorporateActionIdentity,
    AlpacaCorporateActionsCoverage, AlpacaPreparedCorporateActionsPublication,
};

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    BudgetDispatchDecision, BudgetPermit, BudgetReservationDecision, HttpRequestBounds,
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSealExpectation,
    ProviderCaptureSealRequest, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    SealedProviderCaptureMaterial, SharedProviderBudget, SourceMetadata, apply_http_retry_after,
};
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::historical_calendar::{
    authenticated_bounded_get, hardened_client, singleton_bounded_header,
};
use crate::{AlpacaCredentials, AlpacaError};
use native::NativeAction;

const ENDPOINT: &str = "https://data.alpaca.markets/v1/corporate-actions";
const MAX_PAGES: usize = 16;
const MAX_PAGE_BYTES: usize = 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 2_048;

/// Exact bounded all-category, all-quality US processing-date query.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AlpacaCorporateActionsRequest {
    symbols: Vec<String>,
    process_start: CalendarDate,
    process_end: CalendarDate,
}

impl AlpacaCorporateActionsRequest {
    /// Fixes the source query without inventing canonical identity or ex-date coverage.
    pub fn try_new(
        mut symbols: Vec<String>,
        process_start: CalendarDate,
        process_end: CalendarDate,
    ) -> Result<Self, AlpacaError> {
        if symbols.is_empty()
            || symbols.len() > 32
            || process_start > process_end
            || process_start.year() > 9_999
            || process_end.year() > 9_999
        {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        for symbol in &symbols {
            crate::config::validate_equity_symbol(symbol)?;
        }
        symbols.sort_unstable();
        if symbols.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(Self {
            symbols,
            process_start,
            process_end,
        })
    }

    /// Returns the requested source symbols; these are query coordinates, not instrument IDs.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }
    /// Returns the inclusive provider processing-date start.
    pub const fn process_start(&self) -> CalendarDate {
        self.process_start
    }
    /// Returns the inclusive provider processing-date end.
    pub const fn process_end(&self) -> CalendarDate {
        self.process_end
    }

    fn url(&self, token: Option<&str>) -> Result<Url, AlpacaError> {
        let mut url = Url::parse(ENDPOINT).map_err(|_| AlpacaError::Protocol)?;
        url.query_pairs_mut()
            .append_pair("symbols", &self.symbols.join(","))
            .append_pair("start", &self.process_start.to_string())
            .append_pair("end", &self.process_end.to_string())
            .append_pair("region", "us")
            .append_pair("data_quality", "all")
            .append_pair("limit", "1000")
            .append_pair("sort", "asc");
        // Omission is the provider-defined all-types selection, including newly introduced types.
        // A newly returned category fails the closed decoder until its contract is reviewed.
        if let Some(token) = token {
            validate_token(token)?;
            url.query_pairs_mut().append_pair("page_token", token);
        }
        Ok(url)
    }
}

/// Hardened request client; credentials and shared provider permits remain caller-owned.
pub struct AlpacaCorporateActionsClient {
    metadata: SourceMetadata,
    bounds: HttpRequestBounds,
    client: reqwest::Client,
}

impl AlpacaCorporateActionsClient {
    /// Admits only the existing Alpaca market-data extraction authority and exact route.
    pub fn try_new(
        metadata: SourceMetadata,
        bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        if metadata.source_id().as_str() != "alpaca-basic-iex-market-data"
            || metadata.provider().as_str() != crate::config::ALPACA_PROVIDER
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.quality_ceiling() != market_squawk_domain::DataQuality::Aggregated
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        metadata.network_policy().authorize(ENDPOINT)?;
        let client = hardened_client(bounds, "market-squawk/0.1 alpaca-corporate-actions")?;
        Ok(Self {
            metadata,
            bounds,
            client,
        })
    }

    /// Acquires and validates every page before issuing the one-use common physical seal request.
    /// A genuinely empty terminal response is retained; a limit hit or an empty continuing page
    /// fails the operation. Failed provider responses cannot mint terminal coverage.
    pub async fn acquire_complete(
        &self,
        credentials: &AlpacaCredentials,
        budget: &SharedProviderBudget,
        request: AlpacaCorporateActionsRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(AlpacaCorporateActionsSealRejoin, ProviderCaptureSealRequest), AlpacaError> {
        let request_identity = request_identity(&self.metadata, &request)?;
        let dataset = SourceIdentifier::try_from(format!(
            "alpaca:corporate-actions:v1:{}",
            lower_hex(request_identity.bytes())
        ))?;
        let mut pages = Vec::new();
        let mut token: Option<String> = None;
        let mut seen_tokens = BTreeSet::new();
        let mut seen_ids = BTreeSet::new();
        let mut seen_bodies = BTreeSet::new();
        let mut bytes = 0usize;
        let mut last_process_date = None;
        loop {
            if pages.len() == MAX_PAGES {
                return Err(AlpacaError::BodyTooLarge);
            }
            ensure_active(deadline, cancellation)?;
            let url = request.url(token.as_deref())?;
            self.metadata.network_policy().authorize(url.as_str())?;
            let permit = acquire_permit(budget, deadline, cancellation).await?;
            let dispatch_at = Utc::now()
                .timestamp_nanos_opt()
                .map(Timestamp::from_unix_nanos)
                .ok_or(AlpacaError::Protocol)?;
            if !self.metadata.is_effective_at(dispatch_at)
                || !self.metadata.authorization().is_effective_at(dispatch_at)
            {
                return Err(AlpacaError::InvalidAuthorization);
            }
            let response = authenticated_bounded_get(
                &self.client,
                credentials,
                &url,
                self.bounds,
                MAX_PAGE_BYTES,
                deadline,
                cancellation,
            )
            .await?;
            if matches!(response.status, 429 | 503) {
                let retry = singleton_bounded_header(&response.headers, RETRY_AFTER, 128)?;
                let decision = apply_http_retry_after(budget, retry.as_deref(), 1_000);
                if let market_squawk_sources::BudgetDecision::Ready(permit) = decision {
                    permit.release();
                }
                return Err(AlpacaError::Network);
            }
            if matches!(response.status, 401 | 403) {
                return Err(AlpacaError::InvalidAuthorization);
            }
            if response.status >= 500 {
                return Err(AlpacaError::Network);
            }
            if response.status != 200 || !is_json(&response.headers)? {
                return Err(AlpacaError::Protocol);
            }
            if !self.metadata.is_effective_at(response.received_at)
                || !self
                    .metadata
                    .authorization()
                    .is_effective_at(response.received_at)
            {
                return Err(AlpacaError::InvalidAuthorization);
            }
            budget.record_success().map_err(|_| AlpacaError::Network)?;
            permit.release();
            bytes = bytes
                .checked_add(response.body.len())
                .filter(|n| *n <= MAX_TOTAL_BYTES)
                .ok_or(AlpacaError::BodyTooLarge)?;
            let body = Bytes::from(response.body);
            let body_digest = sha256(&body);
            if !seen_bodies.insert(body_digest.bytes()) {
                return Err(AlpacaError::Protocol);
            }
            let (actions, next) = native::decode(&body)?;
            if actions.is_empty() && next.is_some() {
                return Err(AlpacaError::Protocol);
            }
            if let Some(next) = &next {
                validate_token(next)?;
                if !seen_tokens.insert(next.clone()) {
                    return Err(AlpacaError::Protocol);
                }
            }
            let mut page_max = last_process_date;
            for action in &actions {
                let process_date = action.fields.process_date.date();
                if process_date < request.process_start
                    || process_date > request.process_end
                    || last_process_date.is_some_and(|last| process_date < last)
                    || !seen_ids.insert(action.fields.id)
                    || (action.symbols().next().is_some()
                        && !action.symbols().any(|symbol| {
                            request.symbols.iter().any(|requested| requested == symbol)
                        }))
                {
                    return Err(AlpacaError::Protocol);
                }
                page_max = Some(page_max.map_or(process_date, |last| last.max(process_date)));
            }
            last_process_date = page_max;
            if pages
                .last()
                .is_some_and(|page: &Page| page.received_at > response.received_at)
            {
                return Err(AlpacaError::Protocol);
            }
            pages.try_reserve(1).map_err(|_| AlpacaError::Allocation)?;
            pages.push(Page {
                ordinal: u16::try_from(pages.len()).map_err(|_| AlpacaError::Protocol)?,
                request_url: url,
                request_token: token,
                next_token: next.clone(),
                body,
                body_digest,
                received_at: response.received_at,
                rate: RateEvidence::read(&response.headers)?,
                actions,
            });
            token = next;
            if token.is_none() {
                break;
            }
        }
        ensure_active(deadline, cancellation)?;
        let material = capture_material(&self.metadata, &dataset, request_identity, &pages)?;
        let (expectation, seal_request) = material.into_whole_seal_parts();
        Ok((
            AlpacaCorporateActionsSealRejoin {
                expectation,
                metadata: self.metadata.clone(),
                request,
                dataset,
                request_identity,
                pages,
            },
            seal_request,
        ))
    }
}

impl std::fmt::Debug for AlpacaCorporateActionsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaCorporateActionsClient")
            .field("source", self.metadata.source_id())
            .finish_non_exhaustive()
    }
}

/// Opaque continuation which rejoins only its own physically sealed, complete page graph.
pub struct AlpacaCorporateActionsSealRejoin {
    expectation: ProviderCaptureSealExpectation,
    metadata: SourceMetadata,
    request: AlpacaCorporateActionsRequest,
    dataset: SourceIdentifier,
    request_identity: EvidenceDigest,
    pages: Vec<Page>,
}

impl AlpacaCorporateActionsSealRejoin {
    /// Returns exact metadata for existing application source/right admission.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
    /// Returns the source-owned immutable request dataset.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }
    /// Returns source symbols requiring canonical catalog resolution, without minting identity.
    pub fn returned_symbols(&self) -> impl Iterator<Item = &str> {
        self.pages
            .iter()
            .flat_map(|p| p.actions.iter().flat_map(NativeAction::symbols))
    }
    /// Returns source action IDs, subject symbols, and exact ex/effective dates for resolution.
    pub fn returned_actions(
        &self,
    ) -> impl Iterator<Item = (Uuid, Option<&str>, Option<CalendarDate>)> {
        self.pages.iter().flat_map(|p| {
            p.actions
                .iter()
                .map(|a| (a.fields.id, a.subject_symbol(), a.effective_date()))
        })
    }

    /// Returns exact subject and successor/distribution symbols for independent source-qualified
    /// catalog resolution at the native ex/effective date. Source terms remain unchanged.
    pub fn returned_action_identities(
        &self,
    ) -> impl Iterator<Item = (Uuid, Option<&str>, Option<&str>, Option<CalendarDate>)> {
        self.pages.iter().flat_map(|page| {
            page.actions.iter().map(|action| {
                (
                    action.fields.id,
                    action.subject_symbol(),
                    action.related_symbol(),
                    action.effective_date(),
                )
            })
        })
    }

    /// Returns the source's independent date coordinates for canonical date/entitlement mapping.
    pub fn returned_action_dates(
        &self,
    ) -> impl Iterator<Item = (Uuid, AlpacaCorporateActionDates)> {
        self.pages.iter().flat_map(|page| {
            page.actions
                .iter()
                .map(|action| (action.fields.id, action.dates()))
        })
    }
    /// Rejoins physical capture before any canonical publication or coverage result is exposed.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<AlpacaPreparedCorporateActionsPublication, AlpacaError> {
        let authority = self
            .expectation
            .try_rejoin(sealed)
            .and_then(|value| value.try_into_whole())
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let capture = authority.persisted_receipt().capture();
        if capture.source_id() != self.metadata.source_id()
            || capture.metadata_revision() != self.metadata.revision()
            || capture.dataset() != &self.dataset
            || capture.request_set_identity() != self.request_identity
            || capture.pages().len() != self.pages.len()
        {
            return Err(AlpacaError::CaptureMaterial);
        }
        Ok(AlpacaPreparedCorporateActionsPublication::new(
            authority,
            self.metadata,
            self.request,
            self.dataset,
            self.pages,
        ))
    }
}

impl std::fmt::Debug for AlpacaCorporateActionsSealRejoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaCorporateActionsSealRejoin")
            .field("dataset", &self.dataset)
            .field("pages", &self.pages.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Page {
    ordinal: u16,
    request_url: Url,
    request_token: Option<String>,
    next_token: Option<String>,
    body: Bytes,
    body_digest: EvidenceDigest,
    received_at: Timestamp,
    rate: RateEvidence,
    actions: Vec<NativeAction>,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct RateEvidence {
    limit: Option<u32>,
    remaining: Option<u32>,
    reset_unix_seconds: Option<i64>,
}

impl RateEvidence {
    fn read(headers: &reqwest::header::HeaderMap) -> Result<Self, AlpacaError> {
        let result = Self {
            limit: rate_header(headers, "x-ratelimit-limit")?,
            remaining: rate_header(headers, "x-ratelimit-remaining")?,
            reset_unix_seconds: rate_header(headers, "x-ratelimit-reset")?,
        };
        if result.limit == Some(0)
            || matches!((result.limit, result.remaining), (Some(limit), Some(remaining)) if remaining > limit)
            || result.reset_unix_seconds.is_some_and(|value| value < 0)
        {
            return Err(AlpacaError::Protocol);
        }
        Ok(result)
    }
}

fn rate_header<T: std::str::FromStr>(
    headers: &reqwest::header::HeaderMap,
    name: &'static str,
) -> Result<Option<T>, AlpacaError> {
    singleton_bounded_header(headers, reqwest::header::HeaderName::from_static(name), 64)?
        .map(|value| {
            std::str::from_utf8(&value)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or(AlpacaError::Protocol)
        })
        .transpose()
}

fn capture_material(
    metadata: &SourceMetadata,
    dataset: &SourceIdentifier,
    request: EvidenceDigest,
    pages: &[Page],
) -> Result<ProviderCaptureMaterial, AlpacaError> {
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(pages.len())
        .map_err(|_| AlpacaError::Allocation)?;
    for page in pages {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/alpaca-corporate-actions/page/v1\0");
        hash.update(request.bytes());
        hash.update(page.ordinal.to_be_bytes());
        hash.update(page.request_url.as_str().as_bytes());
        receipts.push(
            ProviderCapturePageReceipt::try_new(
                page.ordinal,
                EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
                page.request_token
                    .as_deref()
                    .map(|value| sha256(value.as_bytes())),
                page.next_token
                    .as_deref()
                    .map(|value| sha256(value.as_bytes())),
                200,
                u64::try_from(page.body.len()).map_err(|_| AlpacaError::CaptureMaterial)?,
                page.body_digest,
                page.received_at,
            )
            .map_err(|_| AlpacaError::CaptureMaterial)?,
        );
    }
    let capture = ProviderCaptureSetReceipt::try_new(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        dataset.clone(),
        request,
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        receipts,
    )
    .map_err(|_| AlpacaError::CaptureMaterial)?;
    let connection = Uuid::new_v5(&Uuid::NAMESPACE_URL, &capture.observation_digest().bytes());
    let source: Arc<str> = Arc::from(metadata.source_id().as_str());
    let mut records = Vec::new();
    records
        .try_reserve_exact(pages.len())
        .map_err(|_| AlpacaError::Allocation)?;
    for page in pages {
        records.push(
            RawCaptureRecord::try_new_live(
                Uuid::new_v5(&connection, &page.ordinal.to_be_bytes()),
                Arc::clone(&source),
                connection,
                Some(u64::from(page.ordinal)),
                None,
                DateTime::<Utc>::from_timestamp_nanos(page.received_at.unix_nanos()),
                page.body.clone(),
            )
            .map_err(|_| AlpacaError::CaptureMaterial)?,
        );
    }
    ProviderCaptureMaterial::try_new(capture, records).map_err(|_| AlpacaError::CaptureMaterial)
}

fn request_identity(
    metadata: &SourceMetadata,
    request: &AlpacaCorporateActionsRequest,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/alpaca-corporate-actions/request/v1\0");
    let url = request.url(None)?;
    for value in [
        metadata.source_id().as_str(),
        metadata.revision().as_source_identifier().as_str(),
        url.as_str(),
    ] {
        hash.update(
            u32::try_from(value.len())
                .map_err(|_| AlpacaError::Protocol)?
                .to_be_bytes(),
        );
        hash.update(value.as_bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn validate_token(value: &str) -> Result<(), AlpacaError> {
    if value.is_empty()
        || value.len() > MAX_TOKEN_BYTES
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        Err(AlpacaError::Protocol)
    } else {
        Ok(())
    }
}

fn is_json(headers: &reqwest::header::HeaderMap) -> Result<bool, AlpacaError> {
    Ok(
        singleton_bounded_header(headers, CONTENT_TYPE, 128)?.is_some_and(|value| {
            value.eq_ignore_ascii_case(b"application/json")
                || value.eq_ignore_ascii_case(b"application/json; charset=utf-8")
        }),
    )
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
) -> Result<BudgetPermit, AlpacaError> {
    loop {
        ensure_active(deadline, cancellation)?;
        let wait_until = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => match reservation.commit_dispatch() {
                BudgetDispatchDecision::Ready(permit) => return Ok(permit),
                BudgetDispatchDecision::WaitUntil(at) => at,
                BudgetDispatchDecision::Unavailable(_) => return Err(AlpacaError::Network),
            },
            BudgetReservationDecision::WaitUntil(at) => at,
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
        tokio::select! { biased; () = cancellation.cancelled() => return Err(AlpacaError::Cancelled), () = tokio::time::sleep(wait) => {} }
    }
}

fn sha256(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 15)]));
    }
    value
}
