//! Original Alpaca Paper option-contract reference responses.
//!
//! The contract's `multiplier`, `size`, and deliverables are independent source fields. Neither
//! OSI syntax nor an exchange matching unit supplies economics. Every admitted term is decoded
//! from a bounded original response and bound to its physical capture. Application publication
//! additionally requires catalog original custody, exact canonical identity joins, current rights,
//! and atomic retention of these captures as dependencies of the option snapshot generation.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{
    CalendarDate, Currency, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, OccOptionIdentity, OptionComponentState, OptionExerciseStyle, OptionKind,
    SourceId, SourceIdentifier, Timestamp, VersionPinnedSourceLocator,
};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    BudgetDispatchDecision, BudgetReservationDecision, HttpRequestBounds, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderOptionContractReferenceDependency, ProviderOptionContractReferenceRow,
    ProviderOptionMarketBatch, ProviderWholeCaptureToken, SealedProviderCaptureMaterial,
    SealedProviderCaptureSetReceipt, SharedProviderBudget, SourceMetadata, apply_http_retry_after,
};
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::historical_calendar::{
    authenticated_bounded_get, hardened_client, singleton_bounded_header,
};
use crate::{AlpacaCredentials, AlpacaError};

/// Sole admitted reference route. It supplies public contract facts, never account or order data.
pub const ALPACA_OPTION_CONTRACT_REFERENCE_ENDPOINT: &str =
    "https://paper-api.alpaca.markets/v2/options/contracts";
/// Application resource bound, distinct from the provider's documented maximum of 10,000.
pub const ALPACA_OPTION_CONTRACT_REFERENCE_PAGE_ROWS: usize = 1_000;
/// Whole requested range must terminate inside this resource ceiling or no complete set is issued.
pub const ALPACA_OPTION_CONTRACT_REFERENCE_MAX_PAGES: usize = 32;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 32 * 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 2_048;
const MAX_DELIVERABLES: usize = 64;
const MAX_CONTEXT_BYTES: usize = 128 * 1024;
const USER_AGENT: &str = "market-squawk/0.1 alpaca-option-contract-reference";

// Exact short source excerpt verified 2026-09-23. This documents denomination only: the
// numerical multiplier in this example is NEVER used as a contract economic default.
const US_OPTION_PREMIUM_EXCERPT: &[u8] =
    b"($5.10 execution price) x (100 shares) x (1 contract) = $510.00 USD buying power.";
const US_OPTION_PREMIUM_EXCERPT_SHA256: &str =
    "88a16b7849b397ddb3880d532d3cc51aad186b01871be75f4277ed6bd7731936";
const US_OPTION_PREMIUM_SOURCE: &str =
    "https://docs.alpaca.markets/us/docs/options-orders#buy-a-call";

/// Explicit civil-date range; the API's implicit next-weekend cutoff is never used.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaOptionContractReferenceRequest {
    underlying_symbol: String,
    expiration_start: CalendarDate,
    expiration_end: CalendarDate,
}

impl AlpacaOptionContractReferenceRequest {
    /// Constructs an exact active-contract range for one underlying.
    pub fn try_new(
        underlying_symbol: String,
        expiration_start: CalendarDate,
        expiration_end: CalendarDate,
    ) -> Result<Self, AlpacaError> {
        crate::config::validate_equity_symbol(&underlying_symbol)?;
        if expiration_start > expiration_end || expiration_end.year() > 9_999 {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(Self {
            underlying_symbol,
            expiration_start,
            expiration_end,
        })
    }

    /// Returns the exact requested underlying alias, not a canonical identity.
    pub fn underlying_symbol(&self) -> &str {
        &self.underlying_symbol
    }
    /// Returns the requested inclusive start date.
    pub const fn expiration_start(&self) -> CalendarDate {
        self.expiration_start
    }
    /// Returns the requested inclusive end date.
    pub const fn expiration_end(&self) -> CalendarDate {
        self.expiration_end
    }

    fn url(&self, token: Option<&str>) -> Result<Url, AlpacaError> {
        Self::try_new(
            self.underlying_symbol.clone(),
            self.expiration_start,
            self.expiration_end,
        )?;
        let mut url = Url::parse(ALPACA_OPTION_CONTRACT_REFERENCE_ENDPOINT)
            .map_err(|_| AlpacaError::Protocol)?;
        url.query_pairs_mut()
            .append_pair("underlying_symbols", &self.underlying_symbol)
            .append_pair("expiration_date_gte", &self.expiration_start.to_string())
            .append_pair("expiration_date_lte", &self.expiration_end.to_string())
            .append_pair("status", "active")
            .append_pair("show_deliverables", "true")
            .append_pair(
                "limit",
                &ALPACA_OPTION_CONTRACT_REFERENCE_PAGE_ROWS.to_string(),
            );
        if let Some(token) = token {
            validate_token(token)?;
            url.query_pairs_mut().append_pair("page_token", token);
        }
        Ok(url)
    }
}

/// Native deliverable facts. An equity quantity is not the premium multiplier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaOptionDeliverable {
    kind: Box<str>,
    symbol: Box<str>,
    asset_id: Option<Uuid>,
    amount: Decimal,
    allocation_percentage: Decimal,
    settlement_type: Box<str>,
    settlement_method: Box<str>,
    delayed_settlement: bool,
}
impl AlpacaOptionDeliverable {
    /// Returns the source type (`equity` or `cash`).
    pub fn kind(&self) -> &str {
        &self.kind
    }
    /// Returns the exact deliverable identifier; no canonical identity is inferred.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    /// Returns an Alpaca asset identity when explicitly supplied.
    pub const fn asset_id(&self) -> Option<Uuid> {
        self.asset_id
    }
    /// Returns the exact original deliverable amount.
    pub const fn amount(&self) -> Decimal {
        self.amount
    }
    /// Returns the original allocation percentage independently of amount.
    pub const fn allocation_percentage(&self) -> Decimal {
        self.allocation_percentage
    }
    /// Returns the source settlement timing code.
    pub fn settlement_type(&self) -> &str {
        &self.settlement_type
    }
    /// Returns the source settlement-method code, without guessing contract settlement style.
    pub fn settlement_method(&self) -> &str {
        &self.settlement_method
    }
    /// Returns the source-authored delayed-settlement flag.
    pub const fn delayed_settlement(&self) -> bool {
        self.delayed_settlement
    }
}

/// Economic facts decoded only inside a physically sealed original reference page.
///
/// No public constructor or deserializer accepts caller-provided economic scalars. This proof
/// establishes facts at the original response observation, never a historical effective interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaOriginalOptionContract {
    id: Uuid,
    symbol: Box<str>,
    underlying_symbol: Box<str>,
    underlying_asset_id: Uuid,
    occ_identity: OccOptionIdentity,
    expiration: CalendarDate,
    strike: Decimal,
    kind: OptionKind,
    multiplier: Decimal,
    size: Decimal,
    exercise_style: OptionExerciseStyle,
    tradable: bool,
    deliverables: Box<[AlpacaOptionDeliverable]>,
    received_at: Timestamp,
    capture: Arc<SealedProviderCaptureSetReceipt>,
}
impl AlpacaOriginalOptionContract {
    /// Returns Alpaca's native contract UUID, independently of the compact OCC symbol.
    pub const fn id(&self) -> Uuid {
        self.id
    }
    /// Returns the original source contract symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    /// Returns the original underlying symbol; adjusted roots need not equal it.
    pub fn underlying_symbol(&self) -> &str {
        &self.underlying_symbol
    }
    /// Returns the provider's independent underlying UUID.
    pub const fn underlying_asset_id(&self) -> Uuid {
        self.underlying_asset_id
    }
    /// Returns the exact padded OCC identity cross-checked against explicit original terms.
    pub const fn occ_identity(&self) -> &OccOptionIdentity {
        &self.occ_identity
    }
    /// Returns the original four-digit civil expiration date.
    pub const fn expiration(&self) -> CalendarDate {
        self.expiration
    }
    /// Returns the original exact strike; currency comes from separately admitted definition.
    pub const fn strike(&self) -> Decimal {
        self.strike
    }
    /// Returns the route-owned USD premium convention, inferred from the documented US option
    /// premium examples. This is not a field of the contract response. Canonical admission must
    /// independently restrict the contract to an approved USD Equity/Fund underlying.
    pub fn quote_currency(&self) -> Result<Currency, AlpacaError> {
        self.quote_currency_evidence()?;
        Currency::try_from("USD").map_err(|_| AlpacaError::Protocol)
    }
    /// Returns exact frozen primary-source excerpt evidence for that narrow denomination rule.
    /// Its version identifies the excerpt bytes, not an invented provider document revision.
    pub fn quote_currency_evidence(&self) -> Result<ExactPayloadEvidence, AlpacaError> {
        let digest = hash(US_OPTION_PREMIUM_EXCERPT);
        let actual: String = digest
            .bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if actual != US_OPTION_PREMIUM_EXCERPT_SHA256 {
            return Err(AlpacaError::Protocol);
        }
        Ok(ExactPayloadEvidence::with_version_pinned_locator(
            digest,
            VersionPinnedSourceLocator::new(
                SourceIdentifier::try_from(US_OPTION_PREMIUM_SOURCE)?,
                SourceIdentifier::try_from(format!("sha256:{actual}"))?,
            ),
        ))
    }
    /// Returns the original call/put kind.
    pub const fn kind(&self) -> OptionKind {
        self.kind
    }
    /// Returns only the source `multiplier`; neither `size` nor deliverable amount substitutes.
    pub const fn multiplier(&self) -> Decimal {
        self.multiplier
    }
    /// Returns the separately reported source `size`.
    pub const fn size(&self) -> Decimal {
        self.size
    }
    /// Returns the original explicit exercise style.
    pub const fn exercise_style(&self) -> &OptionExerciseStyle {
        &self.exercise_style
    }
    /// Returns the original provider tradability flag; this grants no execution authority.
    pub const fn tradable(&self) -> bool {
        self.tradable
    }
    /// Returns all original deliverables, including adjusted combinations.
    pub fn deliverables(&self) -> &[AlpacaOptionDeliverable] {
        &self.deliverables
    }
    /// Returns first observation of these original response facts, not an effective-from clock.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns the exact sealed original dependency required at precommit and restart.
    pub fn capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.capture
    }
}

/// Hardened source-reference child of the currently activated Paper source and shared budget.
pub struct AlpacaOptionContractReferenceClient {
    metadata: SourceMetadata,
    bounds: HttpRequestBounds,
    client: reqwest::Client,
}
impl AlpacaOptionContractReferenceClient {
    /// Admits only an extraction-enabled Alpaca profile whose exact policy permits the Paper route.
    pub fn try_new(
        metadata: SourceMetadata,
        bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        if metadata.provider().as_str() != crate::config::ALPACA_PROVIDER
            || !metadata.capabilities().extraction()
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(Self {
            metadata,
            bounds,
            client: hardened_client(bounds, USER_AGENT)?,
        })
    }

    /// Acquires a complete bounded page graph. Any refusal, repeated token/contract, malformed
    /// economics or resource limit returns no complete graph. The owner-supplied sequential custody
    /// continuation must finish for each page before another request; it must use a separate bounded
    /// custody deadline so acquisition cancellation cannot discard a returned page.
    pub async fn acquire_complete<T, Retain, Retained>(
        &self,
        credentials: &AlpacaCredentials,
        budget: &SharedProviderBudget,
        request: AlpacaOptionContractReferenceRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
        mut retain: Retain,
    ) -> Result<Vec<T>, AlpacaError>
    where
        Retain: FnMut(AlpacaPendingOptionContractReferencePage) -> Retained,
        Retained: std::future::Future<Output = Result<T, AlpacaError>>,
    {
        let mut pages = Vec::new();
        let mut token = None;
        let mut tokens = BTreeSet::new();
        let mut symbols = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut total = 0_usize;
        loop {
            ensure_active(deadline, cancellation)?;
            if pages.len() == ALPACA_OPTION_CONTRACT_REFERENCE_MAX_PAGES {
                return Err(AlpacaError::BodyTooLarge);
            }
            let url = request.url(token.as_deref())?;
            self.metadata.network_policy().authorize(url.as_str())?;
            let permit = acquire_permit(budget, deadline, cancellation).await?;
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
                let _decision = apply_http_retry_after(budget, retry.as_deref(), 1_000);
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
            let body_bytes = response.body.len();
            let received_at = response.received_at;
            let page = AlpacaPendingOptionContractReferencePage::from_body(
                self.metadata.source_id().clone(),
                self.metadata.revision().clone(),
                request.clone(),
                token.take(),
                Bytes::from(response.body),
                response.received_at,
            )?;
            // Retain each completed page before the next request or currentness/deadline gate.
            // Failed later pages leave these physical originals in the existing store; only an
            // exhausted successful graph is returned for atomic original-session admission.
            let body = page.body.clone();
            let retained = retain(page).await?;
            let media = singleton_bounded_header(&response.headers, CONTENT_TYPE, 128)?;
            if !media.as_deref().is_some_and(|value| {
                value.eq_ignore_ascii_case(b"application/json")
                    || value.eq_ignore_ascii_case(b"application/json; charset=utf-8")
            }) {
                return Err(AlpacaError::Protocol);
            }
            // Economic decoding follows physical custody, including for malformed JSON or terms.
            // Cloning Bytes shares the same bounded response allocation during the continuation.
            let wire = AlpacaPendingOptionContractReferencePage::decode(&body, &request)?;
            let identities = wire.option_contracts.into_iter().map(|wire| (wire.id, wire.symbol));
            let next = wire.next_page_token;
            if !self.metadata.is_effective_at(received_at) {
                return Err(AlpacaError::InvalidAuthorization);
            }
            budget.record_success().map_err(|_| AlpacaError::Network)?;
            total = total.checked_add(body_bytes)
                .filter(|total| *total <= MAX_TOTAL_BYTES)
                .ok_or(AlpacaError::BodyTooLarge)?;
            for (id, symbol) in identities {
                if !symbols.insert(symbol) || !ids.insert(id) {
                    return Err(AlpacaError::Protocol);
                }
            }
            token = next;
            if let Some(next) = &token
                && !tokens.insert(next.clone())
            {
                return Err(AlpacaError::Protocol);
            }
            pages.try_reserve(1).map_err(|_| AlpacaError::Allocation)?;
            pages.push(retained);
            if token.is_none() {
                break;
            }
        }
        ensure_active(deadline, cancellation)?;
        Ok(pages)
    }
}

/// Source-owned raw page before original custody and physical seal rejoin.
pub struct AlpacaPendingOptionContractReferencePage {
    source: SourceId,
    revision: MetadataRevision,
    request: AlpacaOptionContractReferenceRequest,
    request_token: Option<String>,
    body: Bytes,
    received_at: Timestamp,
    reopened_original: bool,
}
impl AlpacaPendingOptionContractReferencePage {
    fn from_body(
        source: SourceId,
        revision: MetadataRevision,
        request: AlpacaOptionContractReferenceRequest,
        request_token: Option<String>,
        body: Bytes,
        received_at: Timestamp,
    ) -> Result<Self, AlpacaError> {
        request.url(request_token.as_deref())?;
        if body.is_empty() || body.len() > MAX_PAGE_BYTES {
            return Err(AlpacaError::BodyTooLarge);
        }
        Ok(Self {
            source,
            revision,
            request,
            request_token,
            body,
            received_at,
            reopened_original: false,
        })
    }

    fn decode(body: &[u8], request: &AlpacaOptionContractReferenceRequest) -> Result<WirePage, AlpacaError> {
        let wire: WirePage = serde_json::from_slice(body).map_err(|_| AlpacaError::Protocol)?;
        if wire.option_contracts.len() > ALPACA_OPTION_CONTRACT_REFERENCE_PAGE_ROWS
            || (wire.option_contracts.is_empty() && wire.next_page_token.is_some())
        {
            return Err(AlpacaError::Protocol);
        }
        if let Some(token) = &wire.next_page_token {
            validate_token(token)?;
        }
        let mut ids = BTreeSet::new();
        let mut symbols = BTreeSet::new();
        for contract in &wire.option_contracts {
            contract.validate(request)?;
            if !ids.insert(&contract.id) || !symbols.insert(&contract.symbol) {
                return Err(AlpacaError::Protocol);
            }
        }
        Ok(wire)
    }

    /// Returns exact context for the original source session. Retain the complete ordered contexts
    /// in ordinal zero of `provider_capture_originals`; no credentials are included.
    pub fn context(&self) -> Result<Vec<u8>, AlpacaError> {
        serde_json::to_vec(&PageContext {
            version: 1,
            source: self.source.clone(),
            revision: self.revision.clone(),
            request: self.request.clone(),
            request_token: self.request_token.clone(),
            received_at: self.received_at,
        })
        .map_err(|_| AlpacaError::Serialization)
    }

    /// Reconstructs only pending decoding from catalog-reopened original envelopes. The exact
    /// expected receipt must come from `reopen_provider_capture_original`, not caller JSON.
    /// Rejoining the new physical seal remains mandatory before any economic proof is exposed.
    pub fn restore_original(
        context: &[u8],
        expected: &ProviderCaptureSetReceipt,
        records: &[RawCaptureRecord],
    ) -> Result<Self, AlpacaError> {
        if context.len() > MAX_CONTEXT_BYTES || records.len() != 1 {
            return Err(AlpacaError::CaptureMaterial);
        }
        let context: PageContext =
            serde_json::from_slice(context).map_err(|_| AlpacaError::Protocol)?;
        if context.version != 1 {
            return Err(AlpacaError::Protocol);
        }
        let body = Bytes::copy_from_slice(records[0].payload());
        let mut page = Self::from_body(
            context.source,
            context.revision,
            context.request,
            context.request_token,
            body,
            context.received_at,
        )?;
        Self::decode(&page.body, &page.request)?;
        let material = page.material()?;
        if material.receipt() != expected || material.records() != records {
            return Err(AlpacaError::CaptureMaterial);
        }
        // Re-validate original envelope source, receipt time, sequence, UUID and payload through
        // the canonical material constructor; it binds every physical frame to the receipt.
        ProviderCaptureMaterial::try_new(expected.clone(), records.to_vec())
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        page.reopened_original = true;
        Ok(page)
    }

    /// Prepares one standalone physical capture for existing original-custody retention.
    pub fn into_seal_parts(
        self,
    ) -> Result<
        (
            AlpacaOptionContractReferenceRejoin,
            ProviderCaptureSealRequest,
        ),
        AlpacaError,
    > {
        let (expectation, request) = self.material()?.into_whole_seal_parts();
        Ok((
            AlpacaOptionContractReferenceRejoin {
                page: self,
                expectation,
            },
            request,
        ))
    }

    fn material(&self) -> Result<ProviderCaptureMaterial, AlpacaError> {
        let url = self.request.url(self.request_token.as_deref())?;
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
            SourceIdentifier::try_from(format!(
                "alpaca:option-contract-reference:{}:{}:{}",
                self.request.underlying_symbol,
                self.request.expiration_start,
                self.request.expiration_end
            ))?,
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

/// One-use closure joining strict original decoding to the matching physical sealer result.
pub struct AlpacaOptionContractReferenceRejoin {
    page: AlpacaPendingOptionContractReferencePage,
    expectation: ProviderCaptureSealExpectation,
}
impl AlpacaOptionContractReferenceRejoin {
    /// Returns a physical original token for durable custody. This intentionally exposes no terms.
    pub fn into_original_token(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<ProviderWholeCaptureToken, AlpacaError> {
        self.expectation
            .try_rejoin(sealed)
            .and_then(|value| value.try_into_whole())
            .map_err(|_| AlpacaError::CaptureMaterial)
    }

    /// Rejoins the exact original page after catalog/raw replay and yields immutable term facts.
    /// The application retains the catalog custody receipt and enforces it again at commit.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<AlpacaSealedOptionContractReferencePage, AlpacaError> {
        if !self.page.reopened_original {
            return Err(AlpacaError::CaptureMaterial);
        }
        let authority = self
            .expectation
            .try_rejoin(sealed)
            .and_then(|value| value.try_into_whole())
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let capture = Arc::new(authority.persisted_receipt().clone());
        let wire = AlpacaPendingOptionContractReferencePage::decode(&self.page.body, &self.page.request)?;
        let mut contracts = Vec::new();
        contracts
            .try_reserve_exact(wire.option_contracts.len())
            .map_err(|_| AlpacaError::Allocation)?;
        for wire in wire.option_contracts {
            contracts.push(wire.into_contract(
                &self.page.request,
                self.page.received_at,
                capture.clone(),
            )?);
        }
        Ok(AlpacaSealedOptionContractReferencePage {
            request: self.page.request,
            request_token: self.page.request_token,
            next_token: wire.next_page_token,
            capture: Arc::unwrap_or_clone(capture),
            contracts,
        })
    }
}

/// Constructor-private page proof; application custody accompanies this physical receipt.
pub struct AlpacaSealedOptionContractReferencePage {
    request: AlpacaOptionContractReferenceRequest,
    request_token: Option<String>,
    next_token: Option<String>,
    capture: SealedProviderCaptureSetReceipt,
    contracts: Vec<AlpacaOriginalOptionContract>,
}
impl AlpacaSealedOptionContractReferencePage {
    /// Returns the exact original physical dependency.
    pub const fn capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.capture
    }
}

/// Complete original reference graph, with exact pagination and duplicate rejection.
pub struct AlpacaOptionContractReferenceSet {
    pages: Vec<AlpacaSealedOptionContractReferencePage>,
}
impl AlpacaOptionContractReferenceSet {
    /// Requires the entire ordered exhausted graph before making any contract economically usable.
    pub fn try_from_pages(
        pages: Vec<AlpacaSealedOptionContractReferencePage>,
    ) -> Result<Self, AlpacaError> {
        if pages.is_empty() || pages.len() > ALPACA_OPTION_CONTRACT_REFERENCE_MAX_PAGES {
            return Err(AlpacaError::InvalidCoverage);
        }
        let first = &pages[0];
        let mut expected_token = None;
        let mut seen_tokens = BTreeSet::new();
        let mut seen_symbols = BTreeSet::new();
        let mut seen_ids = BTreeSet::new();
        let mut previous_time = None;
        let mut total = 0_u64;
        for (index, page) in pages.iter().enumerate() {
            let raw = page.capture.capture();
            let received = raw.pages()[0].received_at();
            if page.request != first.request
                || page.request_token.as_deref() != expected_token
                || raw.source_id() != first.capture.capture().source_id()
                || raw.metadata_revision() != first.capture.capture().metadata_revision()
                || previous_time.is_some_and(|previous| previous > received)
                || (page.next_token.is_none() != (index + 1 == pages.len()))
            {
                return Err(AlpacaError::InvalidCoverage);
            }
            previous_time = Some(received);
            total = total
                .checked_add(raw.pages()[0].body_bytes())
                .filter(|total| *total <= MAX_TOTAL_BYTES as u64)
                .ok_or(AlpacaError::BodyTooLarge)?;
            for contract in &page.contracts {
                if !seen_symbols.insert(contract.symbol()) || !seen_ids.insert(contract.id()) {
                    return Err(AlpacaError::Protocol);
                }
            }
            expected_token = page.next_token.as_deref();
            if let Some(token) = expected_token
                && !seen_tokens.insert(token)
            {
                return Err(AlpacaError::Protocol);
            }
        }
        Ok(Self { pages })
    }
    /// Returns the exact completed reference request for equality with the chain request.
    pub fn request(&self) -> &AlpacaOptionContractReferenceRequest {
        &self.pages[0].request
    }
    /// Iterates all exact source-authored contract facts without allocating canonical identities.
    pub fn contracts(&self) -> impl Iterator<Item = &AlpacaOriginalOptionContract> {
        self.pages.iter().flat_map(|page| &page.contracts)
    }
    /// Returns exact original economics for this returned compact symbol; missing stays missing.
    pub fn contract(&self, symbol: &str) -> Option<&AlpacaOriginalOptionContract> {
        self.pages
            .iter()
            .flat_map(|page| &page.contracts)
            .find(|value| value.symbol() == symbol)
    }
    /// Validates each canonical term against the original response and returns the complete
    /// reference graph, including pages whose contracts are not present in the requested chain.
    pub fn dependencies_for(
        &self,
        batch: &ProviderOptionMarketBatch,
    ) -> Result<Vec<ProviderOptionContractReferenceDependency>, AlpacaError> {
        let snapshots = batch.snapshots().ok_or(AlpacaError::InvalidCoverage)?;
        let request = self.request();
        let range = batch
            .scope()
            .filter()
            .expiration_range()
            .ok_or(AlpacaError::InvalidCoverage)?;
        if batch.scope().provider_instrument_id().as_str() != request.underlying_symbol()
            || range.start() != request.expiration_start()
            || range.end() != request.expiration_end()
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(self.pages.len())
            .map_err(|_| AlpacaError::Allocation)?;
        let mut seen = BTreeSet::new();
        for page in &self.pages {
            let mut rows = Vec::new();
            for (canonical, snapshot) in snapshots.iter().enumerate() {
                let terms = snapshot.terms();
                let Some((native, original)) =
                    page.contracts.iter().enumerate().find(|(_, original)| {
                        terms.occ_identity() == Some(original.occ_identity())
                    })
                else {
                    continue;
                };
                if terms.expiration() != original.expiration()
                    || terms.strike().amount() != original.strike()
                    || terms.strike().currency() != original.quote_currency()?
                    || terms.provider_instrument_id().as_str() != original.symbol()
                    || terms.exercise_style().source_at().is_some()
                    || terms.settlement().unavailable_reason()
                        != Some(OptionComponentState::ProviderAbsent)
                    || terms.settlement().source_at().is_some()
                    || terms.kind() != original.kind()
                    || terms.multiplier() != original.multiplier()
                    || terms.exercise_style().value() != Some(original.exercise_style())
                    || !seen.insert(canonical)
                {
                    return Err(AlpacaError::InvalidCoverage);
                }
                rows.try_reserve(1).map_err(|_| AlpacaError::Allocation)?;
                rows.push(
                    ProviderOptionContractReferenceRow::try_new(
                        u32::try_from(canonical).map_err(|_| AlpacaError::InvalidCoverage)?,
                        u32::try_from(native).map_err(|_| AlpacaError::InvalidCoverage)?,
                        terms,
                    )
                    .map_err(|_| AlpacaError::InvalidCoverage)?,
                );
            }
            dependencies.push(
                ProviderOptionContractReferenceDependency::try_new(page.capture.clone(), rows)
                    .map_err(|_| AlpacaError::CaptureMaterial)?,
            );
        }
        if seen.len() != snapshots.len() {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(dependencies)
    }
    /// Returns every raw dependency in original request order for atomic catalog publication.
    pub fn captures(&self) -> impl Iterator<Item = &SealedProviderCaptureSetReceipt> {
        self.pages.iter().map(|page| &page.capture)
    }
    /// Returns the last reference observation. Chain acquisition must begin after this clock.
    pub fn observed_at(&self) -> Timestamp {
        self.pages[self.pages.len() - 1].capture.capture().pages()[0].received_at()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageContext {
    version: u16,
    source: SourceId,
    revision: MetadataRevision,
    request: AlpacaOptionContractReferenceRequest,
    request_token: Option<String>,
    received_at: Timestamp,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePage {
    #[serde(deserialize_with = "bounded_contracts")]
    option_contracts: Vec<WireContract>,
    next_page_token: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireContract {
    id: String,
    symbol: String,
    name: String,
    status: String,
    tradable: bool,
    expiration_date: String,
    root_symbol: Option<String>,
    underlying_symbol: String,
    underlying_asset_id: String,
    #[serde(rename = "type")]
    kind: String,
    style: String,
    strike_price: String,
    multiplier: String,
    size: String,
    #[serde(deserialize_with = "bounded_deliverables")]
    deliverables: Vec<WireDeliverable>,
    // These separately dated observations stay raw here; they are not untimed current prices.
    #[serde(rename = "open_interest")]
    _open_interest: Option<String>,
    #[serde(rename = "open_interest_date")]
    _open_interest_date: Option<String>,
    #[serde(rename = "close_price")]
    _close_price: Option<String>,
    #[serde(rename = "close_price_date")]
    _close_price_date: Option<String>,
    #[serde(rename = "ppind")]
    _ppind: Option<bool>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDeliverable {
    #[serde(rename = "type")]
    kind: String,
    symbol: String,
    asset_id: Option<String>,
    amount: String,
    allocation_percentage: String,
    settlement_type: String,
    settlement_method: String,
    delayed_settlement: bool,
}
fn bounded_contracts<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<WireContract>, D::Error> {
    bounded_sequence::<D, WireContract, ALPACA_OPTION_CONTRACT_REFERENCE_PAGE_ROWS>(deserializer)
}
fn bounded_deliverables<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<WireDeliverable>, D::Error> {
    bounded_sequence::<D, WireDeliverable, MAX_DELIVERABLES>(deserializer)
}
fn bounded_sequence<'de, D, T, const LIMIT: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T, const LIMIT: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const LIMIT: usize> serde::de::Visitor<'de> for Bounded<T, LIMIT> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "at most {LIMIT} original source rows")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut rows = Vec::new();
            while let Some(value) = sequence.next_element::<T>()? {
                if rows.len() == LIMIT {
                    return Err(serde::de::Error::custom("source row bound exceeded"));
                }
                rows.try_reserve(1).map_err(serde::de::Error::custom)?;
                rows.push(value);
            }
            Ok(rows)
        }
    }
    deserializer.deserialize_seq(Bounded::<T, LIMIT>(std::marker::PhantomData))
}
impl WireContract {
    fn validate(&self, request: &AlpacaOptionContractReferenceRequest) -> Result<(), AlpacaError> {
        let identity = compact_occ(&self.symbol)?;
        let expiration = parse_date(&self.expiration_date)?;
        let strike = decimal(&self.strike_price, false)?;
        let kind = match self.kind.as_str() {
            "call" => OptionKind::Call,
            "put" => OptionKind::Put,
            _ => return Err(AlpacaError::Protocol),
        };
        let occ_strike = Decimal::new(
            i64::try_from(identity.strike_thousandths()).map_err(|_| AlpacaError::Protocol)?,
            3,
        );
        if self.status != "active"
            || self.underlying_symbol != request.underlying_symbol
            || self.name.len() > 512
            || self.name.chars().any(char::is_control)
            || expiration < request.expiration_start
            || expiration > request.expiration_end
            || identity.kind() != kind
            || identity.expiration_month() != expiration.month()
            || identity.expiration_day() != expiration.day()
            || u16::from(identity.expiration_yy()) != expiration.year() % 100
            || strike != occ_strike
            || self
                .root_symbol
                .as_deref()
                .is_some_and(|root| root != identity.root())
            || !matches!(self.style.as_str(), "american" | "european")
            || self.deliverables.is_empty()
            || self.deliverables.len() > MAX_DELIVERABLES
            || uuid(&self.id)? == uuid(&self.underlying_asset_id)?
        {
            return Err(AlpacaError::Protocol);
        }
        decimal(&self.multiplier, true)?;
        decimal(&self.size, true)?;
        for deliverable in &self.deliverables {
            deliverable.validate()?;
        }
        Ok(())
    }
    fn into_contract(
        self,
        request: &AlpacaOptionContractReferenceRequest,
        received_at: Timestamp,
        capture: Arc<SealedProviderCaptureSetReceipt>,
    ) -> Result<AlpacaOriginalOptionContract, AlpacaError> {
        self.validate(request)?;
        let mut deliverables = Vec::new();
        deliverables
            .try_reserve_exact(self.deliverables.len())
            .map_err(|_| AlpacaError::Allocation)?;
        for value in self.deliverables {
            deliverables.push(value.into_deliverable()?);
        }
        Ok(AlpacaOriginalOptionContract {
            id: uuid(&self.id)?,
            occ_identity: compact_occ(&self.symbol)?,
            expiration: parse_date(&self.expiration_date)?,
            underlying_asset_id: uuid(&self.underlying_asset_id)?,
            symbol: self.symbol.into_boxed_str(),
            underlying_symbol: self.underlying_symbol.into_boxed_str(),
            strike: decimal(&self.strike_price, false)?,
            kind: if self.kind == "call" {
                OptionKind::Call
            } else {
                OptionKind::Put
            },
            multiplier: decimal(&self.multiplier, true)?,
            size: decimal(&self.size, true)?,
            exercise_style: if self.style == "american" {
                OptionExerciseStyle::American
            } else {
                OptionExerciseStyle::European
            },
            tradable: self.tradable,
            deliverables: deliverables.into_boxed_slice(),
            received_at,
            capture,
        })
    }
}
impl WireDeliverable {
    fn validate(&self) -> Result<(), AlpacaError> {
        if !matches!(self.kind.as_str(), "equity" | "cash")
            || self.symbol.is_empty()
            || self.symbol.len() > 64
            || self.symbol.chars().any(char::is_control)
            || !matches!(
                self.settlement_type.as_str(),
                "T+0" | "T+1" | "T+2" | "T+3" | "T+4" | "T+5"
            )
            || !matches!(
                self.settlement_method.as_str(),
                "BTOB" | "CADF" | "CAFX" | "CCC"
            )
            || decimal(&self.allocation_percentage, false)? > Decimal::from(100)
        {
            return Err(AlpacaError::Protocol);
        }
        decimal(&self.amount, false)?;
        if let Some(id) = &self.asset_id {
            uuid(id)?;
        }
        Ok(())
    }
    fn into_deliverable(self) -> Result<AlpacaOptionDeliverable, AlpacaError> {
        self.validate()?;
        Ok(AlpacaOptionDeliverable {
            amount: decimal(&self.amount, false)?,
            allocation_percentage: decimal(&self.allocation_percentage, false)?,
            asset_id: self.asset_id.as_deref().map(uuid).transpose()?,
            kind: self.kind.into_boxed_str(),
            symbol: self.symbol.into_boxed_str(),
            settlement_type: self.settlement_type.into_boxed_str(),
            settlement_method: self.settlement_method.into_boxed_str(),
            delayed_settlement: self.delayed_settlement,
        })
    }
}
fn compact_occ(symbol: &str) -> Result<OccOptionIdentity, AlpacaError> {
    crate::config::validate_option_symbol(symbol)?;
    let split = symbol.len().checked_sub(15).ok_or(AlpacaError::Protocol)?;
    let (root, suffix) = symbol.split_at(split);
    OccOptionIdentity::try_from(format!("{root:<6}{suffix}")).map_err(|_| AlpacaError::Protocol)
}
fn parse_date(value: &str) -> Result<CalendarDate, AlpacaError> {
    if value.len() != 10
        || !value.is_ascii()
        || value.as_bytes()[4] != b'-'
        || value.as_bytes()[7] != b'-'
    {
        return Err(AlpacaError::Protocol);
    }
    let year = value[..4].parse().map_err(|_| AlpacaError::Protocol)?;
    let month = value[5..7].parse().map_err(|_| AlpacaError::Protocol)?;
    let day = value[8..].parse().map_err(|_| AlpacaError::Protocol)?;
    CalendarDate::new(year, month, day).map_err(|_| AlpacaError::Protocol)
}
fn decimal(value: &str, positive: bool) -> Result<Decimal, AlpacaError> {
    if value.is_empty()
        || value.len() > 64
        || value.trim() != value
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return Err(AlpacaError::Protocol);
    }
    let value = Decimal::from_str_exact(value)
        .map_err(|_| AlpacaError::Protocol)?
        .normalize();
    if value.is_sign_negative() || (positive && value.is_zero()) {
        return Err(AlpacaError::Protocol);
    }
    Ok(value)
}
fn uuid(value: &str) -> Result<Uuid, AlpacaError> {
    let id = Uuid::parse_str(value).map_err(|_| AlpacaError::Protocol)?;
    if id.is_nil() || id.hyphenated().to_string() != value {
        return Err(AlpacaError::Protocol);
    }
    Ok(id)
}
fn hash(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
fn validate_token(value: &str) -> Result<(), AlpacaError> {
    if value.is_empty()
        || value.len() > MAX_TOKEN_BYTES
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        Err(AlpacaError::Protocol)
    } else {
        Ok(())
    }
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
