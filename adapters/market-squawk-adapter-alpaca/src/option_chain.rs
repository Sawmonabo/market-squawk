//! Complete, bounded Alpaca indicative option-chain acquisition and publication preparation.
//!
//! Each returned page enters physical custody before decoding. The common capture store then
//! seals the complete terminal page graph independently before canonical publication.
//! Canonical option rows are minted only after that physical seal rejoins this provider-local
//! continuation, and only when every returned contract has exact provider/canonical identity.
//! The resulting binding carries the closed indicative-options native-lineage implementation.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{
    AssetClass, Currency, DigestAlgorithm, EvidenceDigest, MarketDataReference, Money,
    OptionComponent, OptionComponentState, OptionContractTerms, OptionContractTermsInput,
    OptionSnapshotObservation, OptionSnapshotObservationInput, OptionUnderlyingObservation,
    ProviderChannel, ProviderProduct, SourceIdentifier, Timestamp,
};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    BudgetDecision, BudgetDispatchDecision, BudgetReservation, BudgetReservationDecision,
    OptionMarketBatchDisposition, OptionMarketCompleteness, OptionMarketCompletenessInput,
    OptionMarketCursorState, OptionMarketRequestFilter, OptionMarketRequestScope,
    OptionMarketRequestScopeInput, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSealExpectation, ProviderCaptureSealRequest, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageImplementation,
    ProviderOptionMarketBatch, ProviderOptionMarketNativeLineageBatch,
    SealedProviderCaptureMaterial, SealedProviderOptionMarketBinding, SharedProviderBudget,
    apply_http_retry_after,
};
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};
use rust_decimal::Decimal;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::config::{
    ALPACA_BASIC_OPTION_CHAIN_PAGE_ROWS, ALPACA_OPTION_CHAIN_MAX_PAGES,
    ALPACA_OPTIONS_SNAPSHOTS_ENDPOINT, ALPACA_PROVIDER, INDICATIVE_OPTIONS_VENUE,
};
use crate::historical_calendar::{
    authenticated_bounded_get, hardened_client, singleton_bounded_header,
};
use crate::{
    AlpacaCredentials, AlpacaError, AlpacaInstrumentMapping, AlpacaOptionChainConfig,
    AlpacaOptionContractReferenceRequest, AlpacaOptionContractReferenceSet, AlpacaOptionMapping,
    AlpacaOriginalOptionContract,
};

const USER_AGENT: &str = "market-squawk/0.1 alpaca-indicative-option-chain";
const MAX_CHAIN_BYTES: usize = 64 * 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 2_048;
const MAX_PAGE_ATTEMPTS: u8 = 3;

/// Canonical option identity joined to constructor-private original provider economics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaOptionChainContractAuthority {
    mapping: AlpacaOptionMapping,
    reference: MarketDataReference,
    original: AlpacaOriginalOptionContract,
}
impl AlpacaOptionChainContractAuthority {
    /// Accepts economics only from a physically sealed original contract response.
    pub fn try_new(
        reference: MarketDataReference,
        original: &AlpacaOriginalOptionContract,
        at: Timestamp,
    ) -> Result<Self, AlpacaError> {
        reference
            .validate_at(at)
            .map_err(|_| AlpacaError::InvalidCoverage)?;
        if reference.asset_class() != AssetClass::Option
            || reference.source_symbol().as_str() != original.symbol()
            || original.received_at() > at
            || reference.currency() != original.quote_currency()?
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        // This is a genuine source symbol join; economic terms never come from a caller scalar.
        let mapping =
            AlpacaOptionMapping::try_new(original.symbol().to_owned(), reference.instrument_id())?;
        Ok(Self {
            mapping,
            reference,
            original: original.clone(),
        })
    }
    /// Exact source contract symbol.
    pub fn symbol(&self) -> &str {
        self.mapping.symbol()
    }
}

/// Exact catalog references and original contract dependencies applied after raw page sealing.
pub struct AlpacaOptionChainPublicationRequest {
    underlying: AlpacaInstrumentMapping,
    underlying_reference: MarketDataReference,
    contracts: Vec<AlpacaOptionChainContractAuthority>,
    original_contracts: AlpacaOptionContractReferenceSet,
    reference_origin: Option<market_squawk_sources::OptionReferenceOrigin>,
    currency: Currency,
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    ingested_at: Timestamp,
}
impl std::fmt::Debug for AlpacaOptionChainPublicationRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaOptionChainPublicationRequest")
            .field("underlying", &self.underlying)
            .field("contract_count", &self.contracts.len())
            .field("ingested_at", &self.ingested_at)
            .finish_non_exhaustive()
    }
}
impl AlpacaOptionChainPublicationRequest {
    /// Joins the immutable references and original native economics; no execution terms required.
    #[allow(
        clippy::too_many_arguments,
        reason = "original economics, rights and clocks stay explicit"
    )]
    pub fn try_new(
        underlying_reference: MarketDataReference,
        contracts: Vec<AlpacaOptionChainContractAuthority>,
        original_contracts: AlpacaOptionContractReferenceSet,
        entitlement_evidence: EvidenceDigest,
        capability_evidence: EvidenceDigest,
        ingested_at: Timestamp,
    ) -> Result<Self, AlpacaError> {
        require_evidence(entitlement_evidence)?;
        require_evidence(capability_evidence)?;
        underlying_reference
            .validate_at(ingested_at)
            .map_err(|_| AlpacaError::InvalidCoverage)?;
        let underlying = AlpacaInstrumentMapping::try_new(
            underlying_reference.source_symbol().as_str().to_owned(),
            underlying_reference.instrument_id(),
            underlying_reference.asset_class(),
        )?;
        if !matches!(
            underlying_reference.asset_class(),
            AssetClass::Equity | AssetClass::Fund
        ) || underlying_reference.currency().as_str() != "USD"
            || original_contracts.request().underlying_symbol() != underlying.symbol()
            || contracts.len()
                > usize::from(ALPACA_BASIC_OPTION_CHAIN_PAGE_ROWS)
                    * usize::from(ALPACA_OPTION_CHAIN_MAX_PAGES)
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        let mut symbols = BTreeSet::new();
        let mut instruments = BTreeSet::new();
        for contract in &contracts {
            if !symbols.insert(contract.symbol())
                || !instruments.insert(contract.mapping.instrument())
                || contract.original.underlying_symbol() != underlying.symbol()
                || contract.reference.currency() != underlying_reference.currency()
                || underlying_reference.currency() != contract.original.quote_currency()?
                || original_contracts.contract(contract.symbol()) != Some(&contract.original)
            {
                return Err(AlpacaError::InvalidCoverage);
            }
        }
        Ok(Self {
            currency: underlying_reference.currency(),
            underlying,
            underlying_reference,
            contracts,
            original_contracts,
            reference_origin: None,
            entitlement_evidence,
            capability_evidence,
            ingested_at,
        })
    }

    /// Preserves historical references under separately admitted current doctor authority.
    pub fn with_reference_origin(
        mut self,
        origin: market_squawk_sources::OptionReferenceOrigin,
    ) -> Self {
        self.reference_origin = Some(origin);
        self
    }
}

/// Hardened complete-chain client using the account's single shared provider budget.
pub struct AlpacaOptionChainClient {
    config: AlpacaOptionChainConfig,
    client: reqwest::Client,
}

impl AlpacaOptionChainClient {
    /// Constructs the REST child only from an entitlement-admitted extraction profile.
    pub fn try_new(config: AlpacaOptionChainConfig) -> Result<Self, AlpacaError> {
        if config.metadata().provider().as_str() != ALPACA_PROVIDER
            || config.metadata().quality_ceiling() != market_squawk_domain::DataQuality::Indicative
            || config.metadata().capabilities().live()
            || !config.metadata().capabilities().extraction()
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        let bounds = config.request_bounds();
        Ok(Self {
            config,
            client: hardened_client(bounds, USER_AGENT)?,
        })
    }

    /// Acquires all chain pages until the provider omits `next_page_token`.
    ///
    /// The returned continuation owns the only parsed copy and the one-use capture expectation.
    pub async fn acquire_complete_chain<Retain, Retained>(
        &self,
        credentials: &AlpacaCredentials,
        budget: &SharedProviderBudget,
        underlying: &AlpacaInstrumentMapping,
        reference_request: &AlpacaOptionContractReferenceRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
        mut retain: Retain,
    ) -> Result<(AlpacaOptionChainSealRejoin, ProviderCaptureSealRequest), AlpacaError>
    where
        Retain: FnMut(ProviderCaptureSealRequest) -> Retained,
        Retained: std::future::Future<Output = Result<(), AlpacaError>>,
    {
        if reference_request.underlying_symbol() != underlying.symbol() {
            return Err(AlpacaError::InvalidCoverage);
        }
        let metadata = self.config.metadata();
        let dataset = chain_dataset(metadata, underlying, reference_request)?;
        let request_set_identity = chain_request_set_identity(
            metadata.source_id(),
            metadata.revision(),
            &dataset,
            underlying,
        )?;
        let mut pages = Vec::new();
        let mut seen_tokens = BTreeSet::new();
        let mut seen_symbols: BTreeSet<Box<str>> = BTreeSet::new();
        let mut token: Option<Box<str>> = None;
        let mut total_bytes = 0_usize;
        loop {
            if pages.len() == usize::from(ALPACA_OPTION_CHAIN_MAX_PAGES) {
                return Err(AlpacaError::Protocol);
            }
            ensure_active(deadline, cancellation)?;
            let ordinal = u16::try_from(pages.len()).map_err(|_| AlpacaError::Protocol)?;
            let url = chain_url(underlying.symbol(), reference_request, token.as_deref())?;
            metadata.network_policy().authorize(url.as_str())?;
            let mut attempts = 0_u8;
            let response = loop {
                attempts = attempts.checked_add(1).ok_or(AlpacaError::Protocol)?;
                let reservation = acquire_budget(budget, deadline, cancellation).await?;
                let permit = commit_budget(reservation, budget, deadline, cancellation).await?;
                let response = authenticated_bounded_get(
                    &self.client,
                    credentials,
                    &url,
                    self.config.request_bounds(),
                    16 * 1024 * 1024,
                    deadline,
                    cancellation,
                )
                .await?;
                if matches!(response.status, 429 | 503) {
                    let retry_after =
                        singleton_bounded_header(&response.headers, RETRY_AFTER, 128)?;
                    let decision = apply_http_retry_after(budget, retry_after.as_deref(), 1_000);
                    permit.release();
                    if attempts == MAX_PAGE_ATTEMPTS {
                        return Err(AlpacaError::OptionChainHttpStatus(response.status));
                    }
                    wait_for_budget_decision(budget, decision, deadline, cancellation).await?;
                    continue;
                }
                if response.status != 200 {
                    permit.release();
                    return Err(AlpacaError::OptionChainHttpStatus(response.status));
                }
                permit.release();
                break response;
            };
            // Retain the actual completed response before decoding, graph validation or another
            // cancellation/currentness gate. A standalone page never claims a complete chain.
            let body = Bytes::from(response.body);
            let raw = standalone_response_material(metadata, dataset.clone(), &url, &body, response.received_at)?;
            let (_, pending) = raw.into_whole_seal_parts();
            retain(pending).await?;
            if !json_content_type(&response.headers)? {
                return Err(AlpacaError::Protocol);
            }
            budget.record_success().map_err(|_| AlpacaError::Network)?;
            if !metadata.is_effective_at(response.received_at) {
                return Err(AlpacaError::InvalidAuthorization);
            }
            let rate = option_rate_evidence(&response.headers)?;
            total_bytes = total_bytes
                .checked_add(body.len())
                .filter(|bytes| *bytes <= MAX_CHAIN_BYTES)
                .ok_or(AlpacaError::BodyTooLarge)?;
            let page = parse_page(
                ordinal,
                url,
                token.clone(),
                body,
                response.received_at,
                rate,
            )?;
            for snapshot in &page.snapshots {
                if !seen_symbols.insert(snapshot.symbol.clone()) {
                    return Err(AlpacaError::Protocol);
                }
            }
            token = page.next_page_token.clone();
            if let Some(next) = token.as_deref()
                && !seen_tokens.insert(next.to_owned())
            {
                return Err(AlpacaError::Protocol);
            }
            pages.push(page);
            if token.is_none() {
                break;
            }
        }
        let material = capture_material(metadata, dataset.clone(), request_set_identity, &pages)?;
        let (expectation, seal_request) = material.into_whole_seal_parts();
        Ok((
            AlpacaOptionChainSealRejoin {
                expectation,
                metadata: metadata.clone(),
                provider_product: self.config.provider_product().clone(),
                provider_channel: self.config.provider_channel().clone(),
                dataset,
                request_set_identity,
                underlying_mapping: underlying.clone(),
                reference_request: reference_request.clone(),
                pages,
            },
            seal_request,
        ))
    }
}

impl std::fmt::Debug for AlpacaOptionChainClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaOptionChainClient")
            .field("source_id", self.config.metadata().source_id())
            .field("credentials", &"[CALLER-OWNED; NOT RETAINED]")
            .finish_non_exhaustive()
    }
}

/// Opaque parsed/raw continuation that can rejoin only its own physical seal.
pub struct AlpacaOptionChainSealRejoin {
    expectation: ProviderCaptureSealExpectation,
    metadata: market_squawk_sources::SourceMetadata,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    dataset: SourceIdentifier,
    request_set_identity: EvidenceDigest,
    underlying_mapping: AlpacaInstrumentMapping,
    reference_request: AlpacaOptionContractReferenceRequest,
    pages: Vec<AlpacaOptionChainPage>,
}

impl AlpacaOptionChainSealRejoin {
    /// Returns the exact immutable source metadata captured before acquisition.
    pub const fn metadata(&self) -> &market_squawk_sources::SourceMetadata {
        &self.metadata
    }

    /// Returns the exact provider dataset bound to the terminal raw page graph.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns every provider-returned contract symbol from the complete page chain.
    ///
    /// These bounded native keys are discovery inputs only. The application must resolve each
    /// through its source-qualified reference catalog before creating publication authority.
    pub fn returned_contract_symbols(&self) -> impl Iterator<Item = &str> {
        self.pages.iter().flat_map(|page| {
            page.snapshots
                .iter()
                .map(|snapshot| snapshot.symbol.as_ref())
        })
    }

    /// Returns the last exact response clock of this complete, nonempty page chain.
    pub fn received_at(&self) -> Result<Timestamp, AlpacaError> {
        self.pages
            .last()
            .map(|page| page.received_at)
            .ok_or(AlpacaError::Protocol)
    }

    /// Rejoins exact sealed pages, rejects any unmapped returned contract, and prepares canonical
    /// option rows plus native semantics for the common immutable publication boundary.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
        request: AlpacaOptionChainPublicationRequest,
    ) -> Result<AlpacaPreparedOptionMarketPublication, AlpacaError> {
        if request.underlying != self.underlying_mapping
            || request.original_contracts.request() != &self.reference_request
            || request.ingested_at
                < self
                    .pages
                    .last()
                    .map(|page| page.received_at)
                    .ok_or(AlpacaError::Protocol)?
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        let authority = self
            .expectation
            .try_rejoin(sealed)
            .and_then(|rejoined| rejoined.try_into_whole())
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let capture = authority.persisted_receipt().capture();
        if capture.dataset() != &self.dataset
            || capture.request_set_identity() != self.request_set_identity
            || capture.pages().len() != self.pages.len()
        {
            return Err(AlpacaError::CaptureMaterial);
        }
        let mut inputs = BTreeMap::new();
        for contract in &request.contracts {
            if inputs.insert(contract.symbol(), contract).is_some() {
                return Err(AlpacaError::InvalidCoverage);
            }
        }
        let mut rows = Vec::new();
        let mut native_rows = Vec::new();
        let mut row_pages = Vec::new();
        let underlying = OptionUnderlyingObservation::try_new(
            OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
            capture.observation_digest(),
        )
        .map_err(|_| AlpacaError::Protocol)?;
        let first_response_at = self.pages.first().ok_or(AlpacaError::Protocol)?.received_at;
        if request.original_contracts.observed_at() > first_response_at {
            return Err(AlpacaError::InvalidCoverage);
        }
        for page in &self.pages {
            request
                .underlying_reference
                .validate_at(page.received_at)
                .map_err(|_| AlpacaError::InvalidCoverage)?;
            for snapshot in &page.snapshots {
                let contract = inputs
                    .get(snapshot.symbol.as_ref())
                    .ok_or(AlpacaError::InvalidCoverage)?;
                contract
                    .reference
                    .validate_at(page.received_at)
                    .map_err(|_| AlpacaError::InvalidCoverage)?;
                // Adjusted OCC roots need not equal the underlying ticker. The original Alpaca
                // economic-underlying field supplies this relationship, independently of root.
                if contract.original.underlying_symbol() != request.underlying.symbol() {
                    return Err(AlpacaError::InvalidCoverage);
                }
                rows.push(map_snapshot(
                    snapshot,
                    contract,
                    &request,
                    underlying.clone(),
                    page.received_at,
                )?);
                native_rows.push(encode_option_native_row(
                    rows.len() - 1,
                    page.ordinal,
                    snapshot,
                    contract,
                    &request,
                )?);
                row_pages.push(page.ordinal);
            }
        }
        // A complete exhausted empty chain is a header-only canonical generation. Actual raw
        // pages and exact reference-window dependencies remain mandatory even with zero rows.
        // The original contract universe may contain contracts without a returned snapshot.
        // Every returned row already requires a unique admitted original above; absence is not
        // permission to synthesize a row or to reject an otherwise exhausted response graph.
        let page_count =
            NonZeroU16::new(u16::try_from(self.pages.len()).map_err(|_| AlpacaError::Protocol)?)
                .ok_or(AlpacaError::Protocol)?;
        let received_at = self
            .pages
            .last()
            .map(|page| page.received_at)
            .ok_or(AlpacaError::Protocol)?;
        let scope = OptionMarketRequestScope::try_new(OptionMarketRequestScopeInput {
            source_id: self.metadata.source_id().clone(),
            metadata_revision: self.metadata.revision().clone(),
            dataset: self.dataset.clone(),
            provider_product: self.provider_product.clone(),
            provider_channel: self.provider_channel.clone(),
            venue_id: Some(
                market_squawk_domain::VenueId::try_from(INDICATIVE_OPTIONS_VENUE)
                    .map_err(|_| AlpacaError::Protocol)?,
            ),
            underlying_instrument_id: request.underlying.instrument(),
            underlying_definition_revision: request.underlying_reference.definition_digest(),
            provider_instrument_id: request.underlying_reference.source_symbol().clone(),
            request_identity: self.request_set_identity,
            observation_identity: capture.observation_digest(),
            entitlement_evidence: request.entitlement_evidence,
            capability_evidence: request.capability_evidence,
            available_at: received_at,
            received_at,
            ingested_at: request.ingested_at,
            filter: OptionMarketRequestFilter::try_new(
                Some(
                    market_squawk_sources::OptionExpirationRange::try_new(
                        self.reference_request.expiration_start(),
                        self.reference_request.expiration_end(),
                    )
                    .map_err(|_| AlpacaError::Protocol)?,
                ),
                None,
                None,
                Vec::new(),
            )
            .map_err(|_| AlpacaError::Protocol)?,
        })
        .map_err(|_| AlpacaError::Protocol)?;
        let completeness = OptionMarketCompleteness::try_new(OptionMarketCompletenessInput {
            expected_records: None,
            returned_records: u64::try_from(rows.len()).map_err(|_| AlpacaError::Protocol)?,
            missing_records: 0,
            unexpected_records: 0,
            provider_reported_records: None,
            page_count,
            cursor: OptionMarketCursorState::Exhausted,
            disposition: OptionMarketBatchDisposition::Complete,
        })
        .map_err(|_| AlpacaError::Protocol)?;
        let batch = ProviderOptionMarketBatch::try_snapshots(scope, completeness, rows)
            .map_err(|_| AlpacaError::Protocol)?;
        let native_sidecar = encode_option_sidecar(
            &self.metadata,
            &self.dataset,
            &self.pages,
            request.entitlement_evidence,
            request.capability_evidence,
            &self.provider_product,
            &self.provider_channel,
            batch.row_count(),
        )?;
        let mut reference_dependencies = request.original_contracts.dependencies_for(&batch)?;
        if let Some(origin) = request.reference_origin {
            let first = reference_dependencies
                .first_mut()
                .ok_or(AlpacaError::CaptureMaterial)?;
            *first = first.clone().with_origin(origin);
        }
        Ok(AlpacaPreparedOptionMarketPublication {
            parts: AlpacaOptionMarketPublicationParts {
                authority,
                batch,
                native_rows,
                native_sidecar,
                reference_dependencies,
                row_pages,
            },
        })
    }
}

impl std::fmt::Debug for AlpacaOptionChainSealRejoin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaOptionChainSealRejoin")
            .field("source_id", self.metadata.source_id())
            .field("dataset", &self.dataset)
            .field("page_count", &self.pages.len())
            .field("sealed_transition", &"PENDING")
            .finish_non_exhaustive()
    }
}

/// Complete provider-local option publication bound to its closed shared lineage tag.
#[derive(Debug)]
pub struct AlpacaPreparedOptionMarketPublication {
    parts: AlpacaOptionMarketPublicationParts,
}

impl AlpacaPreparedOptionMarketPublication {
    /// Consumes the one-use raw authority into the common immutable option-market binding.
    pub fn try_into_binding(self) -> Result<SealedProviderOptionMarketBinding, AlpacaError> {
        let AlpacaOptionMarketPublicationParts {
            authority,
            batch,
            native_rows,
            native_sidecar,
            row_pages,
            reference_dependencies,
        } = self.parts;
        let native = ProviderOptionMarketNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::AlpacaIndicativeOptionsV1,
            &batch,
            native_rows,
            native_sidecar,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        SealedProviderOptionMarketBinding::try_new(
            authority,
            batch,
            native,
            row_pages,
            reference_dependencies,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)
    }
}

#[derive(Debug)]
struct AlpacaOptionMarketPublicationParts {
    authority: market_squawk_sources::ProviderWholeCaptureToken,
    batch: ProviderOptionMarketBatch,
    native_rows: Vec<Bytes>,
    native_sidecar: Bytes,
    row_pages: Vec<u16>,
    reference_dependencies: Vec<market_squawk_sources::ProviderOptionContractReferenceDependency>,
}

#[derive(Debug)]
struct AlpacaOptionChainPage {
    ordinal: u16,
    request_url: Url,
    request_page_token: Option<Box<str>>,
    next_page_token: Option<Box<str>>,
    body: Bytes,
    body_digest: EvidenceDigest,
    received_at: Timestamp,
    rate: AlpacaOptionRateEvidenceV1,
    snapshots: Vec<AlpacaOptionSnapshotWire>,
}

#[derive(Debug)]
struct AlpacaOptionSnapshotWire {
    symbol: Box<str>,
    value: Value,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaOptionRateEvidenceV1 {
    limit: Option<u32>,
    remaining: Option<u32>,
    reset_unix_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlpacaOptionChainPageWire {
    snapshots: AlpacaOptionSnapshots,
    next_page_token: Option<String>,
}

struct AlpacaOptionSnapshots(BTreeMap<String, Value>);

impl<'de> Deserialize<'de> for AlpacaOptionSnapshots {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SnapshotsVisitor;
        impl<'de> Visitor<'de> for SnapshotsVisitor {
            type Value = AlpacaOptionSnapshots;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a bounded map of distinct option symbols")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut snapshots = BTreeMap::new();
                while let Some(symbol) = map.next_key::<String>()? {
                    if snapshots.len() == usize::from(ALPACA_BASIC_OPTION_CHAIN_PAGE_ROWS)
                        || snapshots.contains_key(&symbol)
                    {
                        return Err(serde::de::Error::custom(
                            "option page repeats a symbol or exceeds its bound",
                        ));
                    }
                    crate::config::validate_option_symbol(&symbol)
                        .map_err(|_| serde::de::Error::custom("invalid option symbol"))?;
                    snapshots.insert(symbol, map.next_value()?);
                }
                Ok(AlpacaOptionSnapshots(snapshots))
            }
        }
        deserializer.deserialize_map(SnapshotsVisitor)
    }
}

fn parse_page(
    ordinal: u16,
    request_url: Url,
    request_page_token: Option<Box<str>>,
    body: Bytes,
    received_at: Timestamp,
    rate: AlpacaOptionRateEvidenceV1,
) -> Result<AlpacaOptionChainPage, AlpacaError> {
    let parsed: AlpacaOptionChainPageWire =
        serde_json::from_slice(&body).map_err(|_| AlpacaError::Protocol)?;
    // Validate nested object keys before financial interpretation. serde Value retains exact
    // numbers but otherwise collapses repeated keys, which is not admissible price evidence.
    let checked: CheckedOptionPage =
        serde_json::from_slice(&body).map_err(|_| AlpacaError::Protocol)?;
    drop(checked);
    let next_page_token = parsed
        .next_page_token
        .map(|token| {
            validate_token(&token)?;
            Ok::<_, AlpacaError>(token.into_boxed_str())
        })
        .transpose()?;
    let mut snapshots = Vec::new();
    snapshots
        .try_reserve_exact(parsed.snapshots.0.len())
        .map_err(|_| AlpacaError::Allocation)?;
    for (symbol, value) in parsed.snapshots.0 {
        crate::config::validate_option_symbol(&symbol)?;
        validate_snapshot_shape(&value)?;
        snapshots.push(AlpacaOptionSnapshotWire {
            symbol: symbol.into_boxed_str(),
            value,
        });
    }
    Ok(AlpacaOptionChainPage {
        ordinal,
        request_url,
        request_page_token,
        next_page_token,
        body_digest: sha256(&body),
        body,
        received_at,
        rate,
        snapshots,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedOptionPage {
    #[serde(rename = "snapshots")]
    _snapshots: BTreeMap<String, CheckedSnapshot>,
    #[serde(rename = "next_page_token")]
    _next_page_token: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedSnapshot {
    #[serde(rename = "latestQuote")]
    _quote: Option<CheckedQuote>,
    #[serde(rename = "latestTrade")]
    _trade: Option<CheckedTrade>,
    #[serde(rename = "greeks")]
    _greeks: Option<CheckedGreeks>,
    #[serde(rename = "impliedVolatility")]
    _iv: Option<serde::de::IgnoredAny>,
    // Ancillary bars stay in the raw response and native snapshot evidence. They do not
    // supply quote/trade/Greeks values or replace the normalized historical-bar consumer.
    #[serde(rename = "minuteBar")]
    _minute_bar: Option<serde::de::IgnoredAny>,
    #[serde(rename = "dailyBar")]
    _daily_bar: Option<serde::de::IgnoredAny>,
    #[serde(rename = "prevDailyBar")]
    _prev_daily_bar: Option<serde::de::IgnoredAny>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedQuote {
    #[serde(rename = "ap")]
    _ap: Option<serde::de::IgnoredAny>,
    #[serde(rename = "as")]
    _ask_size: Option<serde::de::IgnoredAny>,
    #[serde(rename = "ax")]
    _ax: Option<serde::de::IgnoredAny>,
    #[serde(rename = "bp")]
    _bp: Option<serde::de::IgnoredAny>,
    #[serde(rename = "bs")]
    _bs: Option<serde::de::IgnoredAny>,
    #[serde(rename = "bx")]
    _bx: Option<serde::de::IgnoredAny>,
    #[serde(rename = "c")]
    _c: Option<serde::de::IgnoredAny>,
    #[serde(rename = "t")]
    _t: Option<serde::de::IgnoredAny>,
    #[serde(rename = "z")]
    _z: Option<serde::de::IgnoredAny>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedTrade {
    #[serde(rename = "c")]
    _c: Option<serde::de::IgnoredAny>,
    #[serde(rename = "p")]
    _p: Option<serde::de::IgnoredAny>,
    #[serde(rename = "s")]
    _s: Option<serde::de::IgnoredAny>,
    #[serde(rename = "t")]
    _t: Option<serde::de::IgnoredAny>,
    #[serde(rename = "x")]
    _x: Option<serde::de::IgnoredAny>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedGreeks {
    #[serde(rename = "delta")]
    _delta: Option<serde::de::IgnoredAny>,
    #[serde(rename = "gamma")]
    _gamma: Option<serde::de::IgnoredAny>,
    #[serde(rename = "rho")]
    _rho: Option<serde::de::IgnoredAny>,
    #[serde(rename = "theta")]
    _theta: Option<serde::de::IgnoredAny>,
    #[serde(rename = "vega")]
    _vega: Option<serde::de::IgnoredAny>,
}

fn validate_snapshot_shape(value: &Value) -> Result<(), AlpacaError> {
    let object = value.as_object().ok_or(AlpacaError::Protocol)?;
    require_keys(
        object,
        &[
            "latestQuote",
            "latestTrade",
            "greeks",
            "impliedVolatility",
            "minuteBar",
            "dailyBar",
            "prevDailyBar",
        ],
    )?;
    if let Some(value) = object.get("latestQuote")
        && !value.is_null()
    {
        require_keys(
            value.as_object().ok_or(AlpacaError::Protocol)?,
            &["ap", "as", "ax", "bp", "bs", "bx", "c", "t", "z"],
        )?;
    }
    if let Some(value) = object.get("latestTrade")
        && !value.is_null()
    {
        require_keys(
            value.as_object().ok_or(AlpacaError::Protocol)?,
            &["c", "p", "s", "t", "x"],
        )?;
    }
    if let Some(value) = object.get("greeks")
        && !value.is_null()
    {
        require_keys(
            value.as_object().ok_or(AlpacaError::Protocol)?,
            &["delta", "gamma", "rho", "theta", "vega"],
        )?;
    }
    Ok(())
}

fn require_keys(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), AlpacaError> {
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        Err(AlpacaError::Protocol)
    } else {
        Ok(())
    }
}

fn map_snapshot(
    snapshot: &AlpacaOptionSnapshotWire,
    contract: &AlpacaOptionChainContractAuthority,
    request: &AlpacaOptionChainPublicationRequest,
    underlying: OptionUnderlyingObservation,
    received_at: Timestamp,
) -> Result<OptionSnapshotObservation, AlpacaError> {
    let object = snapshot.value.as_object().ok_or(AlpacaError::Protocol)?;
    let quote = object.get("latestQuote").and_then(Value::as_object);
    let trade = object.get("latestTrade").and_then(Value::as_object);
    let greeks = object.get("greeks").and_then(Value::as_object);
    let quote_at = component_timestamp(quote, "t", received_at)?;
    let trade_at = component_timestamp(trade, "t", received_at)?;
    let terms = OptionContractTerms::try_new(OptionContractTermsInput {
        option_instrument_id: contract.mapping.instrument(),
        underlying_instrument_id: request.underlying.instrument(),
        option_definition_revision: contract.reference.definition_digest(),
        underlying_definition_revision: request.underlying_reference.definition_digest(),
        provider_instrument_id: contract.reference.source_symbol().clone(),
        occ_identity: Some(contract.original.occ_identity().clone()),
        expiration: contract.original.expiration(),
        strike: Money::new(contract.original.strike(), request.currency),
        kind: contract.original.kind(),
        multiplier: contract.original.multiplier(),
        exercise_style: OptionComponent::observed(contract.original.exercise_style().clone(), None),
        settlement: OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
    })
    .map_err(|_| AlpacaError::Protocol)?;
    OptionSnapshotObservation::try_new(OptionSnapshotObservationInput {
        terms,
        bid_price: money_component(quote, "bp", request.currency, quote_at),
        bid_size: quantity_component(quote, "bs", quote_at),
        ask_price: money_component(quote, "ap", request.currency, quote_at),
        ask_size: quantity_component(quote, "as", quote_at),
        last_price: money_component(trade, "p", request.currency, trade_at),
        last_size: quantity_component(trade, "s", trade_at),
        mark_price: OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
        trade_conditions: trade_conditions(trade, trade_at),
        volume: OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
        open_interest: OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
        implied_volatility: decimal_component(Some(object), "impliedVolatility", None, false),
        delta: decimal_component(greeks, "delta", None, true),
        gamma: decimal_component(greeks, "gamma", None, true),
        theta: decimal_component(greeks, "theta", None, true),
        vega: decimal_component(greeks, "vega", None, true),
        rho: decimal_component(greeks, "rho", None, true),
        underlying,
    })
    .map_err(|_| AlpacaError::Protocol)
}

fn decimal_component(
    object: Option<&Map<String, Value>>,
    key: &str,
    source_at: Option<Timestamp>,
    signed: bool,
) -> OptionComponent<Decimal> {
    let Some(value) = object.and_then(|object| object.get(key)) else {
        return OptionComponent::unavailable(OptionComponentState::ProviderAbsent, source_at);
    };
    if value.is_null() {
        return OptionComponent::unavailable(OptionComponentState::ProviderNull, source_at);
    }
    match value.as_number().and_then(parse_decimal) {
        Some(value) if signed || !value.is_sign_negative() => {
            OptionComponent::observed(value, source_at)
        }
        _ => OptionComponent::unavailable(OptionComponentState::Invalid, source_at),
    }
}

fn money_component(
    object: Option<&Map<String, Value>>,
    key: &str,
    currency: Currency,
    source_at: Option<Timestamp>,
) -> OptionComponent<Money> {
    match decimal_component(object, key, source_at, false) {
        OptionComponent::Observed { value, source_at } => {
            OptionComponent::observed(Money::new(value, currency), source_at)
        }
        OptionComponent::Unavailable { reason, source_at } => {
            OptionComponent::unavailable(reason, source_at)
        }
    }
}

fn quantity_component(
    object: Option<&Map<String, Value>>,
    key: &str,
    source_at: Option<Timestamp>,
) -> OptionComponent<u64> {
    let Some(value) = object.and_then(|object| object.get(key)) else {
        return OptionComponent::unavailable(OptionComponentState::ProviderAbsent, source_at);
    };
    if value.is_null() {
        return OptionComponent::unavailable(OptionComponentState::ProviderNull, source_at);
    }
    value.as_u64().map_or_else(
        || OptionComponent::unavailable(OptionComponentState::Invalid, source_at),
        |value| OptionComponent::observed(value, source_at),
    )
}

fn trade_conditions(
    trade: Option<&Map<String, Value>>,
    source_at: Option<Timestamp>,
) -> OptionComponent<Box<[SourceIdentifier]>> {
    let Some(value) = trade.and_then(|trade| trade.get("c")) else {
        return OptionComponent::unavailable(OptionComponentState::ProviderAbsent, source_at);
    };
    if value.is_null() {
        return OptionComponent::unavailable(OptionComponentState::ProviderNull, source_at);
    }
    let Some(values) = value.as_array().filter(|values| values.len() <= 32) else {
        return OptionComponent::unavailable(OptionComponentState::Invalid, source_at);
    };
    let conditions = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .and_then(|value| SourceIdentifier::try_from(value).ok())
        })
        .collect::<Option<Vec<_>>>();
    conditions.map_or_else(
        || OptionComponent::unavailable(OptionComponentState::Invalid, source_at),
        |values| OptionComponent::observed(values.into_boxed_slice(), source_at),
    )
}

fn component_timestamp(
    object: Option<&Map<String, Value>>,
    key: &str,
    received_at: Timestamp,
) -> Result<Option<Timestamp>, AlpacaError> {
    let Some(value) = object.and_then(|object| object.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or(AlpacaError::Protocol)?;
    let timestamp = parse_timestamp(value)?;
    if timestamp > received_at {
        return Err(AlpacaError::Protocol);
    }
    Ok(Some(timestamp))
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaOptionNativeRowV1<'a> {
    version: u16,
    canonical_row_ordinal: u32,
    capture_page_ordinal: u16,
    provider_symbol: &'a str,
    option_reference: &'a MarketDataReference,
    underlying_reference: &'a MarketDataReference,
    original_reference_capture: EvidenceDigest,
    native_contract_id: Uuid,
    native_underlying_asset_id: Uuid,
    original_multiplier: Decimal,
    original_size: Decimal,
    exact_provider_snapshot: &'a Value,
}

fn encode_option_native_row(
    ordinal: usize,
    page: u16,
    snapshot: &AlpacaOptionSnapshotWire,
    contract: &AlpacaOptionChainContractAuthority,
    request: &AlpacaOptionChainPublicationRequest,
) -> Result<Bytes, AlpacaError> {
    serde_json::to_vec(&AlpacaOptionNativeRowV1 {
        version: 1,
        canonical_row_ordinal: u32::try_from(ordinal).map_err(|_| AlpacaError::Protocol)?,
        capture_page_ordinal: page,
        provider_symbol: &snapshot.symbol,
        option_reference: &contract.reference,
        underlying_reference: &request.underlying_reference,
        original_reference_capture: contract.original.capture().receipt_digest(),
        native_contract_id: contract.original.id(),
        native_underlying_asset_id: contract.original.underlying_asset_id(),
        original_multiplier: contract.original.multiplier(),
        original_size: contract.original.size(),
        exact_provider_snapshot: &snapshot.value,
    })
    .map(Bytes::from)
    .map_err(|_| AlpacaError::Serialization)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaOptionSidecarV1<'a> {
    version: u16,
    family: &'static str,
    feed: &'static str,
    provider_product: &'a str,
    provider_channel: &'a str,
    quality: &'static str,
    opra: bool,
    delayed_trade_nanos: u64,
    dataset: &'a str,
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    page_count: usize,
    row_count: usize,
    cursor_exhausted: bool,
    pages: Vec<AlpacaOptionPageEvidenceV1<'a>>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaOptionPageEvidenceV1<'a> {
    ordinal: u16,
    request_url: &'a str,
    request_page_token: Option<&'a str>,
    response_next_page_token: Option<&'a str>,
    http_status: u16,
    body_digest: EvidenceDigest,
    body_bytes: usize,
    received_at: Timestamp,
    returned_contracts: usize,
    rate: AlpacaOptionRateEvidenceV1,
}

fn encode_option_sidecar(
    metadata: &market_squawk_sources::SourceMetadata,
    dataset: &SourceIdentifier,
    pages: &[AlpacaOptionChainPage],
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    provider_product: &ProviderProduct,
    provider_channel: &ProviderChannel,
    row_count: usize,
) -> Result<Bytes, AlpacaError> {
    if metadata.quality_ceiling() != market_squawk_domain::DataQuality::Indicative {
        return Err(AlpacaError::Protocol);
    }
    let pages: Vec<AlpacaOptionPageEvidenceV1<'_>> = pages
        .iter()
        .map(|page| AlpacaOptionPageEvidenceV1 {
            ordinal: page.ordinal,
            request_url: page.request_url.as_str(),
            request_page_token: page.request_page_token.as_deref(),
            response_next_page_token: page.next_page_token.as_deref(),
            http_status: 200,
            body_digest: page.body_digest,
            body_bytes: page.body.len(),
            received_at: page.received_at,
            returned_contracts: page.snapshots.len(),
            rate: page.rate,
        })
        .collect();
    serde_json::to_vec(&AlpacaOptionSidecarV1 {
        version: 1,
        family: "alpaca.option_chain_snapshots",
        feed: "indicative",
        provider_product: provider_product.as_source_identifier().as_str(),
        provider_channel: provider_channel.as_source_identifier().as_str(),
        quality: "indicative",
        opra: false,
        delayed_trade_nanos: 900_000_000_000,
        dataset: dataset.as_str(),
        entitlement_evidence,
        capability_evidence,
        page_count: pages.len(),
        row_count,
        cursor_exhausted: true,
        pages,
    })
    .map(Bytes::from)
    .map_err(|_| AlpacaError::Serialization)
}

fn standalone_response_material(
    metadata: &market_squawk_sources::SourceMetadata,
    dataset: SourceIdentifier,
    url: &Url,
    body: &Bytes,
    received_at: Timestamp,
) -> Result<ProviderCaptureMaterial, AlpacaError> {
    let request = sha256(url.as_str().as_bytes());
    let digest = sha256(body);
    let receipt = ProviderCaptureSetReceipt::try_new(
        metadata.source_id().clone(), metadata.revision().clone(), dataset, request,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![ProviderCapturePageReceipt::try_new(
            0, request, None, None, 200,
            u64::try_from(body.len()).map_err(|_| AlpacaError::CaptureMaterial)?,
            digest, received_at,
        ).map_err(|_| AlpacaError::CaptureMaterial)?],
    ).map_err(|_| AlpacaError::CaptureMaterial)?;
    let connection = Uuid::new_v5(&Uuid::NAMESPACE_URL, &receipt.observation_digest().bytes());
    let record = RawCaptureRecord::try_new_live(
        Uuid::new_v5(&connection, &digest.bytes()), Arc::from(metadata.source_id().as_str()),
        connection, Some(0), None,
        DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()), body.clone(),
    ).map_err(|_| AlpacaError::CaptureMaterial)?;
    ProviderCaptureMaterial::try_new(receipt, vec![record]).map_err(|_| AlpacaError::CaptureMaterial)
}

fn capture_material(
    metadata: &market_squawk_sources::SourceMetadata,
    dataset: SourceIdentifier,
    request_set_identity: EvidenceDigest,
    pages: &[AlpacaOptionChainPage],
) -> Result<ProviderCaptureMaterial, AlpacaError> {
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(pages.len())
        .map_err(|_| AlpacaError::Allocation)?;
    for page in pages {
        receipts.push(
            ProviderCapturePageReceipt::try_new(
                page.ordinal,
                page_request_identity(request_set_identity, page)?,
                page.request_page_token.as_deref().map(token_digest),
                page.next_page_token.as_deref().map(token_digest),
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
        dataset,
        request_set_identity,
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        receipts,
    )
    .map_err(|_| AlpacaError::CaptureMaterial)?;
    let connection_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, &capture.observation_digest().bytes());
    let source: Arc<str> = Arc::from(metadata.source_id().as_str());
    let mut records = Vec::new();
    records
        .try_reserve_exact(pages.len())
        .map_err(|_| AlpacaError::Allocation)?;
    for page in pages {
        let event_id = Uuid::new_v5(
            &connection_id,
            &[
                page.ordinal.to_be_bytes().as_slice(),
                page.body_digest.bytes().as_slice(),
            ]
            .concat(),
        );
        records.push(
            RawCaptureRecord::try_new_live(
                event_id,
                Arc::clone(&source),
                connection_id,
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

fn chain_url(
    symbol: &str,
    reference_request: &AlpacaOptionContractReferenceRequest,
    token: Option<&str>,
) -> Result<Url, AlpacaError> {
    crate::config::validate_equity_symbol(symbol)?;
    let mut url =
        Url::parse(ALPACA_OPTIONS_SNAPSHOTS_ENDPOINT).map_err(|_| AlpacaError::Protocol)?;
    url.path_segments_mut()
        .map_err(|_| AlpacaError::Protocol)?
        .push(symbol);
    url.query_pairs_mut()
        .append_pair("limit", &ALPACA_BASIC_OPTION_CHAIN_PAGE_ROWS.to_string())
        .append_pair("feed", "indicative")
        .append_pair(
            "expiration_date_gte",
            &reference_request.expiration_start().to_string(),
        )
        .append_pair(
            "expiration_date_lte",
            &reference_request.expiration_end().to_string(),
        );
    if let Some(token) = token {
        validate_token(token)?;
        url.query_pairs_mut().append_pair("page_token", token);
    }
    Ok(url)
}

fn chain_dataset(
    metadata: &market_squawk_sources::SourceMetadata,
    underlying: &AlpacaInstrumentMapping,
    reference_request: &AlpacaOptionContractReferenceRequest,
) -> Result<SourceIdentifier, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-indicative-option-chain-dataset/v1\0");
    hash_text(&mut digest, metadata.source_id().as_str())?;
    hash_text(
        &mut digest,
        metadata.revision().as_source_identifier().as_str(),
    )?;
    hash_text(&mut digest, underlying.symbol())?;
    hash_text(&mut digest, &underlying.instrument().to_string())?;
    hash_text(
        &mut digest,
        &reference_request.expiration_start().to_string(),
    )?;
    hash_text(&mut digest, &reference_request.expiration_end().to_string())?;
    SourceIdentifier::try_from(format!(
        "alpaca:indicative-option-chain:v1:{}",
        lower_hex(digest.finalize().into())
    ))
    .map_err(Into::into)
}

fn chain_request_set_identity(
    source: &market_squawk_domain::SourceId,
    revision: &market_squawk_domain::MetadataRevision,
    dataset: &SourceIdentifier,
    underlying: &AlpacaInstrumentMapping,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-indicative-option-chain-request/v1\0");
    hash_text(&mut digest, source.as_str())?;
    hash_text(&mut digest, revision.as_source_identifier().as_str())?;
    hash_text(&mut digest, dataset.as_str())?;
    hash_text(&mut digest, underlying.symbol())?;
    hash_text(&mut digest, &underlying.instrument().to_string())?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn page_request_identity(
    request_set: EvidenceDigest,
    page: &AlpacaOptionChainPage,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-indicative-option-chain-page/v1\0");
    digest.update(request_set.bytes());
    digest.update(page.ordinal.to_be_bytes());
    hash_text(&mut digest, page.request_url.as_str())?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

async fn acquire_budget(
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetReservation, AlpacaError> {
    loop {
        ensure_active(deadline, cancellation)?;
        match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => return Ok(reservation),
            BudgetReservationDecision::WaitUntil(wait_until) => {
                wait_for_budget(budget, wait_until, deadline, cancellation).await?;
            }
            BudgetReservationDecision::Unavailable(_) => return Err(AlpacaError::Network),
        }
    }
}

async fn commit_budget(
    mut reservation: BudgetReservation,
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<market_squawk_sources::BudgetPermit, AlpacaError> {
    loop {
        ensure_active(deadline, cancellation)?;
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(wait_until) => {
                wait_for_budget(budget, wait_until, deadline, cancellation).await?;
                reservation = acquire_budget(budget, deadline, cancellation).await?;
            }
            BudgetDispatchDecision::Unavailable(_) => return Err(AlpacaError::Network),
        }
    }
}

async fn wait_for_budget(
    budget: &SharedProviderBudget,
    wait_until: market_squawk_sources::MonotonicInstant,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaError> {
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
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(AlpacaError::Cancelled),
        () = tokio::time::sleep(wait) => Ok(()),
    }
}

async fn wait_for_budget_decision(
    budget: &SharedProviderBudget,
    decision: BudgetDecision,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaError> {
    match decision {
        BudgetDecision::WaitUntil(wait_until) => {
            wait_for_budget(budget, wait_until, deadline, cancellation).await
        }
        BudgetDecision::Ready(permit) => {
            permit.release();
            Err(AlpacaError::Protocol)
        }
        BudgetDecision::Unavailable(_) => Err(AlpacaError::Network),
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

fn json_content_type(headers: &reqwest::header::HeaderMap) -> Result<bool, AlpacaError> {
    singleton_bounded_header(headers, CONTENT_TYPE, 128).map(|value| {
        value.is_some_and(|value| {
            value.eq_ignore_ascii_case(b"application/json")
                || value.eq_ignore_ascii_case(b"application/json; charset=utf-8")
        })
    })
}

fn option_rate_evidence(
    headers: &reqwest::header::HeaderMap,
) -> Result<AlpacaOptionRateEvidenceV1, AlpacaError> {
    Ok(AlpacaOptionRateEvidenceV1 {
        limit: optional_rate_header(headers, "x-ratelimit-limit")?,
        remaining: optional_rate_header(headers, "x-ratelimit-remaining")?,
        reset_unix_seconds: optional_rate_header(headers, "x-ratelimit-reset")?,
    })
}

fn optional_rate_header<T>(
    headers: &reqwest::header::HeaderMap,
    name: &'static str,
) -> Result<Option<T>, AlpacaError>
where
    T: std::str::FromStr,
{
    singleton_bounded_header(headers, reqwest::header::HeaderName::from_static(name), 64)?
        .map(|value| {
            std::str::from_utf8(&value)
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or(AlpacaError::Protocol)
        })
        .transpose()
}

fn parse_timestamp(value: &str) -> Result<Timestamp, AlpacaError> {
    if value.len() > 64 || !(value.ends_with('Z') || value.ends_with("+00:00")) {
        return Err(AlpacaError::Protocol);
    }
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| AlpacaError::Protocol)?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err(AlpacaError::Protocol);
    }
    parsed
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaError::Protocol)
}

fn parse_decimal(value: &Number) -> Option<Decimal> {
    value
        .to_string()
        .parse::<Decimal>()
        .ok()
        .map(|value| value.normalize())
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

fn require_evidence(value: EvidenceDigest) -> Result<(), AlpacaError> {
    if value.algorithm() != DigestAlgorithm::Sha256 || value.bytes() == [0; 32] {
        Err(AlpacaError::InvalidCoverage)
    } else {
        Ok(())
    }
}

fn token_digest(value: &str) -> EvidenceDigest {
    sha256(value.as_bytes())
}

fn sha256(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), AlpacaError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| AlpacaError::Protocol)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_bars_preserve_raw_evidence_and_reject_duplicate_prices()
    -> Result<(), Box<dyn std::error::Error>> {
        let body = r#"{
            "snapshots": {"MSFT261016C00500000": {
                "latestQuote": {"bp": 1.25, "ap": 1.30, "bs": 2, "as": 3,
                    "t": "2026-10-08T15:00:00Z"},
                "latestTrade": {"p": 1.27, "s": 1, "t": "2026-10-08T14:45:00Z"},
                "greeks": {"delta": 0.42}, "impliedVolatility": 0.25,
                "minuteBar": {"o": 1.20, "h": 1.30, "l": 1.20, "c": 1.27,
                    "v": 12, "n": 4, "vw": 1.25, "t": "2026-10-08T14:45:00Z"},
                "dailyBar": {"o": 1.10, "h": 1.40, "l": 1.00, "c": 1.27,
                    "v": 120, "n": 40, "vw": 1.22, "t": "2026-10-08T04:00:00Z"},
                "prevDailyBar": {"o": 1.00, "h": 1.20, "l": 0.90, "c": 1.10,
                    "v": 100, "n": 30, "vw": 1.05, "t": "2026-10-07T04:00:00Z"}
            }},
            "next_page_token": null
        }"#;
        let url = Url::parse("https://data.alpaca.markets/v1beta1/options/snapshots/MSFT")?;
        let received_at = parse_timestamp("2026-10-08T15:00:01Z")?;
        let rate = AlpacaOptionRateEvidenceV1 {
            limit: None,
            remaining: None,
            reset_unix_seconds: None,
        };
        let page = parse_page(
            0,
            url.clone(),
            None,
            Bytes::copy_from_slice(body.as_bytes()),
            received_at,
            rate,
        )?;
        assert_eq!(page.body.as_ref(), body.as_bytes());
        assert_eq!(page.body_digest, sha256(body.as_bytes()));
        assert_eq!(page.snapshots.len(), 1);
        let snapshot = page.snapshots.first().ok_or("missing snapshot")?;
        let quote = snapshot.value.get("latestQuote").and_then(Value::as_object);
        let quote_at = component_timestamp(quote, "t", received_at)?;
        let currency = Currency::try_from("USD")?;
        assert_eq!(
            money_component(quote, "bp", currency, quote_at),
            OptionComponent::observed(Money::new(Decimal::new(125, 2), currency), quote_at),
        );
        let duplicate = body.replace("\"bp\": 1.25", "\"bp\": 1.25, \"bp\": 9.99");
        assert!(matches!(
            parse_page(0, url, None, Bytes::from(duplicate), received_at, rate),
            Err(AlpacaError::Protocol),
        ));
        Ok(())
    }
}
