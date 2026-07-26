//! Authenticated Coinbase Direct snapshot/bootstrap and live transport.
//!
//! This is intentionally not a [`market_squawk_sources::LiveMarketSource`]. Direct bootstrap owns
//! a REST product response, a segmented level-3 snapshot, a bounded replay queue, and one
//! instrument book across the atomic snapshot-to-live handoff. The generic raw-only source
//! contract cannot express that ownership without hiding synchronization state.

mod http;

use std::cmp::Ordering;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _, future::BoxFuture};
use market_squawk_domain::{
    ConnectionGeneration, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    InstrumentExecutionTerms, LiveEventClass, MarketDepth, PriceTicks, QuantityLots,
    SequenceNumber, SnapshotApplicability, SourceIdentifier, Timestamp,
};
use market_squawk_live::{
    DirectOrderBook, DirectOrderBookError, DirectPublishedBook, DirectPublishedLevel,
};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDecision, BudgetPermit, ChecksumValidationProfile,
    DecodedProviderBatch, DecoderEvidence, HttpCaptureMethod, LiveSourceGeneration,
    NetworkAccessPolicy, ProviderBookChange, ProviderBookLevel, ProviderBookSide,
    ProviderChecksumEvidence, ProviderDecimalLexeme, ProviderNormalizedObservation,
    ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
    ProviderSnapshotEvidence, ProviderTimestampEvidence, RawMarketSink, SegmentedHttpCaptureError,
    SegmentedHttpResponseCapture, SegmentedHttpResponseReceipt, SequenceValidationProfile,
    SharedProviderBudget, SinkError, SourceError, SourceMetadata, SourceMetadataProvider,
    SourceProtocolProfile, TlsProviderCapability, TransportFrameKind, apply_http_retry_after,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use self::http::{
    CoinbaseDirectHttpRequest, CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransport,
    CoinbaseDirectHttpTransportError, ReqwestCoinbaseDirectHttpTransport,
};
use crate::{
    CoinbaseConfigError, CoinbaseDirectConfig, CoinbaseDirectDecodeError,
    CoinbaseDirectDecodeOutcome, CoinbaseDirectDecoder, CoinbaseDirectNonBookEvent,
    CoinbaseDirectProductError, CoinbaseDirectProductEvidence, CoinbaseDirectSigningCapability,
    CoinbaseDirectSigningError, CoinbaseDirectSnapshotDecoder, CoinbaseDirectSnapshotError,
    CoinbaseSignedSubscription,
};

/// Borrowed, read-only healthy book evidence emitted by one Direct owner.
///
/// This update is unqualified provider evidence. It does not mint `DirectVerified`, canonical
/// market events, order authority, or execution eligibility.
#[derive(Clone, Copy, Debug)]
pub struct CoinbaseDirectBookUpdate<'a> {
    config: &'a CoinbaseDirectConfig,
    sequence: SequenceNumber,
    source_timestamp: Timestamp,
    decoder_evidence: &'a DecoderEvidence,
    subscription_evidence: &'a ExactPayloadEvidence,
    snapshot_receipt: &'a SegmentedHttpResponseReceipt,
    book: DirectPublishedBook<'a>,
    previous: Option<&'a PublishedBookState>,
    current: &'a PublishedBookState,
    publication: CoinbaseDirectPublicationKind,
}

impl<'a> CoinbaseDirectBookUpdate<'a> {
    /// Returns the exact healthy public product cursor.
    pub const fn sequence(self) -> SequenceNumber {
        self.sequence
    }

    /// Returns the provider event time associated with the healthy cursor.
    pub const fn source_timestamp(self) -> Timestamp {
        self.source_timestamp
    }

    /// Returns the complete generation-bound level-3 snapshot receipt.
    pub const fn snapshot_receipt(self) -> &'a SegmentedHttpResponseReceipt {
        self.snapshot_receipt
    }

    /// Returns the exact validated subscription acknowledgement evidence for this generation.
    pub const fn subscription_evidence(self) -> &'a ExactPayloadEvidence {
        self.subscription_evidence
    }

    /// Returns an allocation-free bounded-depth view of the session-owned book.
    pub const fn book(self) -> DirectPublishedBook<'a> {
        self.book
    }

    /// Returns the message-atomic canonical publication shape selected by the single writer.
    pub const fn publication_kind(self) -> CoinbaseDirectPublicationKind {
        self.publication
    }

    /// Converts this non-forgeable synchronized-book view into one ordinary captured batch.
    ///
    /// The returned value remains unqualified. The application must still bind the exact raw-frame
    /// capture receipt and pass the batch through the source registry, live processor, strategy,
    /// central risk service, and execution dispatcher.
    ///
    /// # Errors
    ///
    /// Rejects stale or transplanted snapshot/frame authority, a cursor-only empty book,
    /// profile-rule mismatch, or an exact financial conversion that cannot be represented.
    pub fn try_publication_batch(
        self,
    ) -> Result<DecodedProviderBatch, CoinbaseDirectPublicationError> {
        self.decoder_evidence
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseDirectPublicationError::StaleAuthority)?;
        self.snapshot_receipt
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseDirectPublicationError::StaleAuthority)?;
        if !self
            .decoder_evidence
            .binding()
            .shares_allocation_with(self.snapshot_receipt.binding())
            || !self
                .decoder_evidence
                .currentness_lease()
                .shares_authority_with(self.snapshot_receipt.currentness_lease())
            || self.decoder_evidence.binding().source_id() != self.config.metadata().source_id()
            || self.decoder_evidence.binding().metadata_revision()
                != self.config.metadata().revision()
            || self.snapshot_receipt.connection_generation()
                != self.decoder_evidence.binding().connection_generation()
        {
            return Err(CoinbaseDirectPublicationError::EvidenceMismatch);
        }
        let SourceProtocolProfile::Live(protocol) = self.config.metadata().protocol_profile()
        else {
            return Err(CoinbaseDirectPublicationError::ProfileMismatch);
        };
        let sequence_rule = match protocol.sequence() {
            SequenceValidationProfile::Provided { rule, .. } => rule.clone(),
            SequenceValidationProfile::Unsupported { .. } => {
                return Err(CoinbaseDirectPublicationError::ProfileMismatch);
            }
        };
        let checksum_rule = match protocol.checksum() {
            ChecksumValidationProfile::Unsupported { rule } => rule.clone(),
            ChecksumValidationProfile::Provided { .. } => {
                return Err(CoinbaseDirectPublicationError::ProfileMismatch);
            }
        };
        let (event_class, depth) = match self.publication {
            CoinbaseDirectPublicationKind::Snapshot => {
                (LiveEventClass::BookSnapshot, Some(MarketDepth::PriceLevel))
            }
            CoinbaseDirectPublicationKind::Delta => {
                (LiveEventClass::BookDelta, Some(MarketDepth::PriceLevel))
            }
            CoinbaseDirectPublicationKind::Quote => (LiveEventClass::Quote, None),
        };
        let coverage_rule = self
            .config
            .metadata()
            .coverage()
            .live()
            .and_then(|coverage| coverage.rule_for(event_class, depth))
            .ok_or(CoinbaseDirectPublicationError::ProfileMismatch)?;
        let snapshot = match (self.publication, coverage_rule.snapshot_applicability()) {
            (CoinbaseDirectPublicationKind::Snapshot, SnapshotApplicability::Required)
                if self.previous.is_none() =>
            {
                ProviderSnapshotEvidence::InitializingSnapshot {
                    provider_reference: None,
                }
            }
            (CoinbaseDirectPublicationKind::Delta, SnapshotApplicability::Required)
                if self.previous.is_some() =>
            {
                ProviderSnapshotEvidence::Delta {
                    provider_snapshot_reference: None,
                }
            }
            (
                CoinbaseDirectPublicationKind::Quote,
                SnapshotApplicability::NotApplicable { metadata_rule },
            ) if self.previous.is_some() => {
                ProviderSnapshotEvidence::NotApplicable(metadata_rule.clone())
            }
            _ => return Err(CoinbaseDirectPublicationError::ProfileMismatch),
        };
        let terms = self.config.execution_terms();
        match (self.publication, self.previous) {
            (CoinbaseDirectPublicationKind::Snapshot, None) => {}
            (CoinbaseDirectPublicationKind::Delta, Some(previous)) if previous != self.current => {}
            (CoinbaseDirectPublicationKind::Quote, Some(previous)) if previous == self.current => {}
            _ => return Err(CoinbaseDirectPublicationError::EvidenceMismatch),
        }
        let payload = match self.publication {
            CoinbaseDirectPublicationKind::Snapshot => ProviderObservationPayload::book_snapshot(
                MarketDepth::PriceLevel,
                provider_levels(&self.current.bids, terms)?,
                provider_levels(&self.current.asks, terms)?,
            ),
            CoinbaseDirectPublicationKind::Delta => {
                let previous = self
                    .previous
                    .ok_or(CoinbaseDirectPublicationError::ProfileMismatch)?;
                ProviderObservationPayload::book_delta(
                    MarketDepth::PriceLevel,
                    provider_changes(previous, self.current, terms)?,
                )
            }
            CoinbaseDirectPublicationKind::Quote => {
                let bid = self
                    .current
                    .bids
                    .first()
                    .copied()
                    .map(|level| provider_level(level, terms))
                    .transpose()?;
                let ask = self
                    .current
                    .asks
                    .first()
                    .copied()
                    .map(|level| provider_level(level, terms))
                    .transpose()?;
                ProviderObservationPayload::quote(bid, ask)
            }
        }
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)?;
        let source_identifier = direct_book_identifier(self.sequence, self.snapshot_receipt)?;
        let observation = ProviderNormalizedObservation::try_new(
            source_identifier,
            self.config.venue().clone(),
            self.config.instrument(),
            ProviderTimestampEvidence::Provided {
                value: self.source_timestamp,
                rule: protocol.timestamp_rule().clone(),
            },
            ProviderSequenceEvidence::Provided {
                value: self.sequence,
                rule: sequence_rule,
            },
            snapshot,
            ProviderChecksumEvidence::Unsupported {
                rule: checksum_rule,
            },
            payload,
        )
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)?;
        DecodedProviderBatch::try_new(self.decoder_evidence.clone(), vec![observation])
            .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)
    }
}

/// Exact bounded publication selected by the single-writer Direct owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoinbaseDirectPublicationKind {
    /// First complete bounded price-level image for the connection generation.
    Snapshot,
    /// Exact changed price levels relative to the preceding accepted publication.
    Delta,
    /// Cursor-only successor whose bounded price-level image did not change.
    Quote,
}

/// Failure to bind one synchronized Direct book to the ordinary captured live-ingress contract.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectPublicationError {
    /// Snapshot or frame currentness was revoked before publication.
    #[error("Coinbase Direct publication authority is stale")]
    StaleAuthority,
    /// Snapshot and last-frame evidence do not share one exact source generation.
    #[error("Coinbase Direct publication evidence is inconsistent")]
    EvidenceMismatch,
    /// Immutable metadata no longer exposes the Direct quote profile.
    #[error("Coinbase Direct publication profile is inconsistent")]
    ProfileMismatch,
    /// A synchronized best level cannot be converted exactly using the configured terms.
    #[error("Coinbase Direct publication numeric evidence is invalid")]
    InvalidNumeric,
    /// A bounded publication buffer could not be allocated.
    #[error("Coinbase Direct publication allocation failed")]
    Allocation,
    /// The bounded normalized observation could not be constructed.
    #[error("Coinbase Direct publication observation is invalid")]
    InvalidObservation,
}

#[derive(Debug, Eq, PartialEq)]
struct PublishedBookState {
    bids: Vec<DirectPublishedLevel>,
    asks: Vec<DirectPublishedLevel>,
}

impl PublishedBookState {
    fn try_new(depth: usize) -> Result<Self, CoinbaseDirectSessionError> {
        let mut bids = Vec::new();
        bids.try_reserve_exact(depth)
            .map_err(|_error| CoinbaseDirectSessionError::PublicationAllocation)?;
        let mut asks = Vec::new();
        asks.try_reserve_exact(depth)
            .map_err(|_error| CoinbaseDirectSessionError::PublicationAllocation)?;
        Ok(Self { bids, asks })
    }

    fn replace_from(&mut self, book: DirectPublishedBook<'_>) {
        self.bids.clear();
        self.bids.extend(book.bids());
        self.asks.clear();
        self.asks.extend(book.asks());
    }

    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

fn provider_level(
    level: DirectPublishedLevel,
    terms: InstrumentExecutionTerms,
) -> Result<ProviderBookLevel, CoinbaseDirectPublicationError> {
    provider_level_parts(level.price(), level.quantity(), terms)
}

fn provider_level_parts(
    price: PriceTicks,
    quantity: QuantityLots,
    terms: InstrumentExecutionTerms,
) -> Result<ProviderBookLevel, CoinbaseDirectPublicationError> {
    let mut price = price
        .checked_to_decimal(terms.price_tick())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    price.rescale(terms.price_tick().as_decimal().scale());
    let mut quantity = quantity
        .checked_to_decimal(terms.lot_size())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    quantity.rescale(terms.lot_size().as_decimal().scale());
    let price = ProviderDecimalLexeme::try_new(&price.to_string())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    let quantity = ProviderDecimalLexeme::try_new(&quantity.to_string())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    Ok(ProviderBookLevel::new(
        ProviderPrice::new(price),
        ProviderQuantity::new(quantity),
    ))
}

fn provider_levels(
    levels: &[DirectPublishedLevel],
    terms: InstrumentExecutionTerms,
) -> Result<Vec<ProviderBookLevel>, CoinbaseDirectPublicationError> {
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(levels.len())
        .map_err(|_error| CoinbaseDirectPublicationError::Allocation)?;
    for level in levels {
        normalized.push(provider_level(*level, terms)?);
    }
    Ok(normalized)
}

fn provider_changes(
    previous: &PublishedBookState,
    current: &PublishedBookState,
    terms: InstrumentExecutionTerms,
) -> Result<Vec<ProviderBookChange>, CoinbaseDirectPublicationError> {
    let capacity = previous
        .bids
        .len()
        .checked_add(previous.asks.len())
        .and_then(|value| value.checked_add(current.bids.len()))
        .and_then(|value| value.checked_add(current.asks.len()))
        .ok_or(CoinbaseDirectPublicationError::InvalidObservation)?;
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(capacity)
        .map_err(|_error| CoinbaseDirectPublicationError::Allocation)?;
    append_side_changes(
        &previous.bids,
        &current.bids,
        ProviderBookSide::Bid,
        terms,
        &mut changes,
    )?;
    append_side_changes(
        &previous.asks,
        &current.asks,
        ProviderBookSide::Ask,
        terms,
        &mut changes,
    )?;
    Ok(changes)
}

fn append_side_changes(
    previous: &[DirectPublishedLevel],
    current: &[DirectPublishedLevel],
    side: ProviderBookSide,
    terms: InstrumentExecutionTerms,
    changes: &mut Vec<ProviderBookChange>,
) -> Result<(), CoinbaseDirectPublicationError> {
    let mut previous_index = 0_usize;
    let mut current_index = 0_usize;
    while previous_index < previous.len() || current_index < current.len() {
        match (
            previous.get(previous_index).copied(),
            current.get(current_index).copied(),
        ) {
            (Some(old), Some(new)) if old.price() == new.price() => {
                if old.quantity() != new.quantity() {
                    changes.push(ProviderBookChange::new(side, provider_level(new, terms)?));
                }
                previous_index = previous_index.saturating_add(1);
                current_index = current_index.saturating_add(1);
            }
            (Some(old), Some(new)) if level_precedes(old, new, side) => {
                changes.push(ProviderBookChange::new(
                    side,
                    provider_level_parts(
                        old.price(),
                        QuantityLots::new(0)
                            .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?,
                        terms,
                    )?,
                ));
                previous_index = previous_index.saturating_add(1);
            }
            (Some(_old), Some(new)) => {
                changes.push(ProviderBookChange::new(side, provider_level(new, terms)?));
                current_index = current_index.saturating_add(1);
            }
            (Some(old), None) => {
                changes.push(ProviderBookChange::new(
                    side,
                    provider_level_parts(
                        old.price(),
                        QuantityLots::new(0)
                            .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?,
                        terms,
                    )?,
                ));
                previous_index = previous_index.saturating_add(1);
            }
            (None, Some(new)) => {
                changes.push(ProviderBookChange::new(side, provider_level(new, terms)?));
                current_index = current_index.saturating_add(1);
            }
            (None, None) => break,
        }
    }
    Ok(())
}

fn level_precedes(
    left: DirectPublishedLevel,
    right: DirectPublishedLevel,
    side: ProviderBookSide,
) -> bool {
    match side {
        ProviderBookSide::Bid => left.price().cmp(&right.price()) == Ordering::Greater,
        ProviderBookSide::Ask => left.price().cmp(&right.price()) == Ordering::Less,
    }
}

fn direct_book_identifier(
    sequence: SequenceNumber,
    snapshot: &SegmentedHttpResponseReceipt,
) -> Result<SourceIdentifier, CoinbaseDirectPublicationError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = snapshot.body_digest().bytes();
    let mut identifier = format!("coinbase-direct-book-{}-", sequence.get());
    identifier
        .try_reserve_exact(digest.len().saturating_mul(2))
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)?;
    for byte in digest {
        identifier.push(char::from(HEX[usize::from(byte >> 4)]));
        identifier.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SourceIdentifier::try_from(identifier)
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)
}

/// Nonblocking application boundary for one Coinbase Direct generation.
///
/// Every `try_*` callback is synchronous. Implementations must not hide an unbounded queue or
/// await downstream work. Raw frames are accepted through [`RawMarketSink`] before any decoded
/// outcome mutates the session.
pub trait CoinbaseDirectOutput: RawMarketSink {
    /// Accepts current provider product/status/tick/lot evidence without qualifying it.
    fn try_publish_product(
        &mut self,
        evidence: CoinbaseDirectProductEvidence,
    ) -> Result<(), SinkError>;

    /// Accepts one private lifecycle event that carries no public cursor or book authority.
    fn try_publish_non_book(&mut self, event: CoinbaseDirectNonBookEvent) -> Result<(), SinkError>;

    /// Retains capture admission for the exact sequenced frame that now backs the Direct book.
    ///
    /// Queueing may call this repeatedly before the first published book. Implementations must
    /// replace the prior retained sequenced-frame receipt rather than accumulating receipts.
    fn try_retain_sequenced_frame(&mut self, evidence: &DecoderEvidence) -> Result<(), SinkError>;

    /// Accepts one borrowed read-only view after healthy handoff or a contiguous live successor.
    fn try_publish_book(&mut self, update: CoinbaseDirectBookUpdate<'_>) -> Result<(), SinkError>;
}

/// Terminal construction, transport, synchronization, or output failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectSessionError {
    /// Immutable Coinbase configuration could not produce the pinned runtime profile.
    #[error("Coinbase Direct session configuration is invalid: {0}")]
    Configuration(#[from] CoinbaseConfigError),
    /// Registry generation, provider budget, network, cancellation, or raw sink failure.
    #[error("Coinbase Direct source authority or transport failed: {0}")]
    Source(#[from] SourceError),
    /// Application-owned signing capability or signed subscription construction failed.
    #[error("Coinbase Direct signing failed: {0}")]
    Signing(#[from] CoinbaseDirectSigningError),
    /// A captured order lifecycle frame failed the pinned decoder.
    #[error("Coinbase Direct frame decode failed: {0}")]
    Decode(#[from] CoinbaseDirectDecodeError),
    /// A complete captured product response failed typed evidence decoding.
    #[error("Coinbase Direct product evidence failed: {0}")]
    Product(#[from] CoinbaseDirectProductError),
    /// A complete captured level-3 response failed snapshot decoding.
    #[error("Coinbase Direct snapshot failed: {0}")]
    Snapshot(#[from] CoinbaseDirectSnapshotError),
    /// Same-owner queue, replay, or live mutation failed.
    #[error("Coinbase Direct book synchronization failed: {0}")]
    Book(#[from] DirectOrderBookError),
    /// Generation-bound segmented HTTP capture failed.
    #[error("Coinbase Direct HTTP capture failed: {0}")]
    Capture(#[from] SegmentedHttpCaptureError),
    /// The exact signed `full` subscription acknowledgement was absent, malformed, or duplicated.
    #[error("Coinbase Direct subscription acknowledgement is invalid")]
    Subscription,
    /// A WebSocket payload or internal protocol frame was not accepted by the pinned profile.
    #[error("Coinbase Direct WebSocket protocol is invalid")]
    WebSocketProtocol,
    /// HTTP response metadata, headers, media type, or final URL was not admitted.
    #[error("Coinbase Direct HTTP response is invalid")]
    HttpResponse,
    /// HTTP acquisition exceeded its configured total deadline.
    #[error("Coinbase Direct HTTP response deadline elapsed")]
    HttpDeadline,
    /// HTTP response bytes exceeded the configured complete-body ceiling.
    #[error("Coinbase Direct HTTP response exceeded its byte ceiling")]
    HttpBodyTooLarge,
    /// HTTP response segmentation exceeded the configured count ceiling.
    #[error("Coinbase Direct HTTP response exceeded its segment ceiling")]
    HttpSegmentLimit,
    /// The bounded product/status refresh deadline could not be represented.
    #[error("Coinbase Direct product refresh deadline is invalid")]
    ProductRefreshDeadline,
    /// A cancellation close handshake failed or exceeded its bound.
    #[error("Coinbase Direct WebSocket shutdown failed")]
    Shutdown,
    /// The bounded publication-state buffers could not be reserved before transport starts.
    #[error("Coinbase Direct publication state allocation failed")]
    PublicationAllocation,
}

#[derive(Debug)]
struct SequencedFrameEvidence {
    sequence: SequenceNumber,
    evidence: DecoderEvidence,
}

/// Production one-generation Coinbase Direct session.
#[derive(Debug)]
pub struct CoinbaseDirectSession {
    config: CoinbaseDirectConfig,
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    decoder: CoinbaseDirectDecoder,
    snapshot_decoder: CoinbaseDirectSnapshotDecoder,
    http: Arc<dyn CoinbaseDirectHttpTransport>,
    http_timeout: Duration,
    book: DirectOrderBook,
    acknowledgement_evidence: Option<ExactPayloadEvidence>,
    last_sequenced_frame: Option<SequencedFrameEvidence>,
    snapshot_receipt: Option<SegmentedHttpResponseReceipt>,
    published_state: PublishedBookState,
    next_published_state: PublishedBookState,
    has_published_book: bool,
    next_product_refresh: Option<Instant>,
    generation_started: bool,
}

impl CoinbaseDirectSession {
    /// Consumes one registry-minted generation and the project-installed TLS capability before
    /// creating the hardened production HTTP client.
    ///
    /// No credentials are read or retained. The signing capability is supplied only to
    /// [`Self::run`] at first use.
    pub fn try_new(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        tls_provider: TlsProviderCapability,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        let bounds = direct_http_bounds(&config)?;
        let http = Arc::new(
            ReqwestCoinbaseDirectHttpTransport::try_new(bounds, tls_provider)
                .map_err(|_error| CoinbaseDirectSessionError::HttpResponse)?,
        );
        Self::try_new_inner(config, generation, http)
    }

    #[cfg(test)]
    fn try_new_with_transport(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        http: Arc<dyn CoinbaseDirectHttpTransport>,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        Self::try_new_inner(config, generation, http)
    }

    fn try_new_inner(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        http: Arc<dyn CoinbaseDirectHttpTransport>,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        let decoder = CoinbaseDirectDecoder::try_new(&config)?;
        let snapshot_decoder = CoinbaseDirectSnapshotDecoder::try_new(&config)?;
        let book = DirectOrderBook::try_new(
            authority.generation(),
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        let bounds = direct_http_bounds(&config)?;
        let published_depth = config.limits().book().published_depth();
        Ok(Self {
            config,
            authority,
            budget,
            decoder,
            snapshot_decoder,
            http,
            http_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            book,
            acknowledgement_evidence: None,
            last_sequenced_frame: None,
            snapshot_receipt: None,
            published_state: PublishedBookState::try_new(published_depth)?,
            next_published_state: PublishedBookState::try_new(published_depth)?,
            has_published_book: false,
            next_product_refresh: None,
            generation_started: false,
        })
    }

    /// Returns immutable source metadata. Its quality is a ceiling declaration, not current
    /// qualification minted by this session.
    pub const fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }

    /// Runs one production connection until cancellation or a typed fail-closed terminal defect.
    pub async fn run(
        &mut self,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let outcome = self.run_production(signer, output, cancellation).await;
        self.finish_generation(outcome)
    }

    async fn run_production(
        &mut self,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError> {
        self.begin_generation()?;
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled.into());
        }
        self.validate_generation()?;
        self.authorize_endpoint(self.config.websocket_endpoint())?;
        let permit = self.acquire_budget()?;
        let limits = self.config.limits().websocket();
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(limits.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(32 * 1024)
            .max_message_size(Some(limits.max_frame_bytes()))
            .max_frame_size(Some(limits.max_frame_bytes()));
        let connect = connect_async_with_config(
            self.config.websocket_endpoint(),
            Some(websocket_config),
            true,
        );
        let (socket, _response) =
            await_websocket(&cancellation, limits.connect_timeout(), connect, |error| {
                map_connect_error(error, &self.budget)
            })
            .await?;
        self.run_connected(socket, permit, signer, None, output, cancellation)
            .await
    }

    #[cfg(test)]
    async fn run_with_socket_for_test<S>(
        &mut self,
        socket: WebSocketStream<S>,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
        unix_seconds: u64,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let outcome = async {
            self.begin_generation()?;
            if cancellation.is_cancelled() {
                return Err(SourceError::Cancelled.into());
            }
            self.validate_generation()?;
            let permit = self.acquire_budget()?;
            self.run_connected(
                socket,
                permit,
                signer,
                Some(unix_seconds),
                output,
                cancellation,
            )
            .await
        }
        .await;
        self.finish_generation(outcome)
    }

    async fn run_connected<S>(
        &mut self,
        socket: WebSocketStream<S>,
        connection_guard: BudgetPermit,
        signer: &dyn CoinbaseDirectSigningCapability,
        fixed_unix_seconds: Option<u64>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let limits = self.config.limits().websocket();
        let (outcome, shutdown) = {
            let mut socket = socket;
            let outcome = async {
                let unix_seconds = match fixed_unix_seconds {
                    Some(unix_seconds) => unix_seconds,
                    None => current_unix_seconds()?,
                };
                if cancellation.is_cancelled() {
                    return Err(SourceError::Cancelled.into());
                }
                self.validate_generation()?;
                let subscription = self.config.try_signed_subscription(unix_seconds, signer)?;
                self.validate_generation()?;
                if cancellation.is_cancelled() {
                    return Err(SourceError::Cancelled.into());
                }
                self.run_connected_inner(&mut socket, subscription, output, &cancellation)
                    .await
            };
            let outcome = outcome.await;
            let shutdown = if matches!(
                outcome,
                Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled))
            ) {
                self.shutdown_socket(&mut socket, output, limits.io_timeout())
                    .await
            } else {
                Ok(())
            };
            (outcome, shutdown)
        };
        // The socket has completed its bounded shutdown or terminal drop before this release.
        connection_guard.release();
        shutdown?;
        outcome
    }

    async fn run_connected_inner<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        subscription: CoinbaseSignedSubscription,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let limits = self.config.limits().websocket();
        self.validate_generation()?;
        send_with_deadline(
            socket,
            Message::Text(subscription.as_str().into()),
            cancellation,
            limits.io_timeout(),
        )
        .await?;
        drop(subscription);
        self.await_subscription(socket, output, cancellation)
            .await?;
        self.budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;

        self.bootstrap_and_run_live(socket, output, cancellation)
            .await
    }

    async fn await_subscription<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let io_timeout = self.config.limits().websocket().io_timeout();
        let acknowledgement = async {
            loop {
                let message = read_with_deadline(socket, output, cancellation, io_timeout).await?;
                if self
                    .handle_message(
                        socket,
                        message,
                        output,
                        cancellation,
                        InboundMode::AwaitingAck,
                    )
                    .await?
                    == MessageDisposition::Acknowledged
                {
                    return Ok(());
                }
            }
        };
        match tokio::time::timeout(io_timeout, acknowledgement).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(SourceError::ConnectionIdle.into()),
        }
    }

    async fn bootstrap_and_run_live<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let product_url = self.config.product_url().to_owned();
        let product_response = self
            .fetch_while_processing(
                socket,
                output,
                cancellation,
                &product_url,
                InboundMode::Queueing,
            )
            .await?;
        let product_capture = self.finish_http_capture(&product_url, product_response)?;
        let product_evidence = self.config.decode_product_evidence(&product_capture)?;
        output
            .try_publish_product(product_evidence)
            .map_err(SourceError::Sink)?;
        self.schedule_product_refresh()?;

        let snapshot_url = self.config.snapshot_url().to_owned();
        let snapshot_response = self
            .fetch_while_processing(
                socket,
                output,
                cancellation,
                &snapshot_url,
                InboundMode::Queueing,
            )
            .await?;
        let snapshot_capture = self.finish_http_capture(&snapshot_url, snapshot_response)?;
        let snapshot_receipt = snapshot_capture.receipt().clone();
        let frontier = handoff_frontier_payload(&snapshot_receipt, self.authority.generation());
        self.await_handoff_frontier(socket, output, cancellation, frontier)
            .await?;
        self.snapshot_decoder
            .decode_into(&snapshot_capture, &mut self.book)?;
        self.book.begin_replay()?;
        loop {
            if cancellation.is_cancelled() {
                return Err(SourceError::Cancelled.into());
            }
            if !self.book.replay_next()? {
                break;
            }
        }
        self.book.finish_replay()?;
        self.validate_generation()?;
        self.snapshot_receipt = Some(snapshot_receipt);
        self.publish_initial_book_if_exact(output)?;

        loop {
            let refresh_at = self
                .next_product_refresh
                .ok_or(CoinbaseDirectSessionError::ProductRefreshDeadline)?;
            match read_with_refresh_deadline(
                socket,
                output,
                cancellation,
                self.config.limits().websocket().io_timeout(),
                refresh_at,
            )
            .await?
            {
                LiveRead::Message(message) => {
                    let _disposition = self
                        .handle_message(socket, message, output, cancellation, InboundMode::Live)
                        .await?;
                }
                LiveRead::RefreshProduct => {
                    self.refresh_product_while_live(socket, output, cancellation)
                        .await?;
                }
            }
        }
    }

    async fn refresh_product_while_live<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let product_url = self.config.product_url().to_owned();
        let response = self
            .fetch_while_processing(
                socket,
                output,
                cancellation,
                &product_url,
                InboundMode::Live,
            )
            .await?;
        let capture = self.finish_http_capture(&product_url, response)?;
        let evidence = self.config.decode_product_evidence(&capture)?;
        output
            .try_publish_product(evidence)
            .map_err(SourceError::Sink)?;
        self.schedule_product_refresh()
    }

    async fn await_handoff_frontier<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        frontier: Bytes,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let io_timeout = self.config.limits().websocket().io_timeout();
        let operation = async {
            self.validate_generation()?;
            send_with_deadline(
                socket,
                Message::Ping(frontier.clone()),
                cancellation,
                io_timeout,
            )
            .await?;
            let mut matching_pong_seen = false;
            loop {
                let message = read_with_deadline(socket, output, cancellation, io_timeout).await?;
                if matches!(&message, Message::Pong(payload) if payload == &frontier) {
                    self.validate_generation()?;
                    matching_pong_seen = true;
                    continue;
                }
                let disposition = self
                    .handle_message(socket, message, output, cancellation, InboundMode::Queueing)
                    .await?;
                if matching_pong_seen && disposition == MessageDisposition::Sequenced {
                    self.validate_generation()?;
                    return Ok(());
                }
            }
        };
        match tokio::time::timeout(io_timeout, operation).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(SourceError::ConnectionIdle.into()),
        }
    }

    async fn fetch_while_processing<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        url: &str,
        mode: InboundMode,
    ) -> Result<CoinbaseDirectHttpResponse, CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut response = self.start_http_request(url, cancellation.clone())?;
        loop {
            let deadline =
                ReceiveDeadline::strictest(output, self.config.limits().websocket().io_timeout())?;
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(SourceError::Cancelled.into());
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
                    if deadline.sink_owned {
                        output
                            .poll_deadline(Instant::now())
                            .map_err(SourceError::Sink)?;
                        return Err(SourceError::InvalidProtocolState.into());
                    }
                    return Err(SourceError::ConnectionIdle.into());
                }
                completed = &mut response => {
                    return completed.map_err(map_http_transport_error);
                }
                message = socket.next() => {
                    let message = map_next_message(message)?;
                    let _disposition = self
                        .handle_message(
                            socket,
                            message,
                            output,
                            cancellation,
                            mode,
                        )
                        .await?;
                }
            }
        }
    }

    fn start_http_request(
        &self,
        url: &str,
        cancellation: CancellationToken,
    ) -> Result<
        BoxFuture<'static, Result<CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransportError>>,
        CoinbaseDirectSessionError,
    > {
        self.validate_generation()?;
        self.authorize_endpoint(url)?;
        let permit = self.acquire_budget()?;
        let limits = self.config.limits();
        let request = CoinbaseDirectHttpRequest::new(
            url,
            limits.max_snapshot_bytes(),
            limits.max_snapshot_segments(),
            self.http_timeout,
            cancellation,
        );
        let transport = Arc::clone(&self.http);
        Ok(Box::pin(async move {
            let outcome = transport.get(request).await;
            drop(permit);
            outcome
        }))
    }

    fn finish_http_capture(
        &mut self,
        expected_url: &str,
        response: CoinbaseDirectHttpResponse,
    ) -> Result<SegmentedHttpResponseCapture, CoinbaseDirectSessionError> {
        self.validate_generation()?;
        self.authorize_endpoint(response.final_url.as_ref())?;
        if response.final_url.as_ref() != expected_url {
            return Err(CoinbaseDirectSessionError::HttpResponse);
        }
        if matches!(response.status, 401 | 403) {
            return Err(SourceError::Unauthorized.into());
        }
        if response.status == 429 || (500..=599).contains(&response.status) {
            return Err(
                SourceError::from_applied_budget_refusal(apply_http_retry_after(
                    &self.budget,
                    response.retry_after.as_deref(),
                    1_000,
                ))
                .into(),
            );
        }
        if response.status != 200
            || !content_type_is_json(response.content_type.as_deref())
            || response
                .content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(CoinbaseDirectSessionError::HttpResponse);
        }
        let limits = self.config.limits();
        let mut builder = self.authority.frames_mut()?.try_http_response_builder(
            HttpCaptureMethod::Get,
            response.final_url.as_ref(),
            response.status,
            response.declared_body_length,
            limits.max_snapshot_bytes(),
            limits.max_snapshot_segments(),
        )?;
        for segment in response.segments {
            builder.try_push_segment(segment)?;
        }
        let capture = builder.finish()?;
        self.validate_generation()?;
        self.budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
        Ok(capture)
    }

    async fn handle_message<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        message: Message,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        mode: InboundMode,
    ) -> Result<MessageDisposition, CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match message {
            Message::Text(text) => self.capture_decode_commit(
                TransportFrameKind::Text,
                Bytes::copy_from_slice(text.as_bytes()),
                output,
                mode,
            ),
            Message::Binary(payload) => {
                self.capture_rejected_protocol_frame(TransportFrameKind::Binary, payload, output)?;
                Err(if mode == InboundMode::AwaitingAck {
                    CoinbaseDirectSessionError::Subscription
                } else {
                    CoinbaseDirectSessionError::WebSocketProtocol
                })
            }
            Message::Ping(payload) => {
                send_with_deadline(
                    socket,
                    Message::Pong(payload),
                    cancellation,
                    self.config.limits().websocket().io_timeout(),
                )
                .await?;
                Ok(MessageDisposition::Control)
            }
            Message::Pong(_) => Ok(MessageDisposition::Control),
            Message::Close(_frame) => {
                flush_with_deadline(
                    socket,
                    cancellation,
                    self.config.limits().websocket().io_timeout(),
                )
                .await?;
                Err(SourceError::ProviderUnavailable.into())
            }
            Message::Frame(_) => Err(CoinbaseDirectSessionError::WebSocketProtocol),
        }
    }

    fn capture_decode_commit(
        &mut self,
        transport: TransportFrameKind,
        payload: Bytes,
        output: &mut dyn CoinbaseDirectOutput,
        mode: InboundMode,
    ) -> Result<MessageDisposition, CoinbaseDirectSessionError> {
        ensure_frame_bound(
            payload.len(),
            self.config.limits().websocket().max_frame_bytes(),
        )?;
        let frame = self.authority.frames_mut()?.try_frame(transport, payload)?;
        let acknowledgement_evidence = if mode == InboundMode::AwaitingAck {
            let digest: [u8; 32] = Sha256::digest(frame.payload()).into();
            Some(ExactPayloadEvidence::from_content_digest(
                EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            ))
        } else {
            None
        };
        let decoded = {
            let validated = self.authority.validate_live_frame(&frame)?;
            if mode == InboundMode::AwaitingAck {
                validate_subscription_ack(validated.frame().payload(), self.config.product())
                    .map(|()| None)
            } else {
                match self.decoder.decode(&validated) {
                    Ok(outcome) => Ok(Some(outcome)),
                    Err(CoinbaseDirectDecodeError::UnsupportedMessage)
                        if validate_subscription_ack(
                            validated.frame().payload(),
                            self.config.product(),
                        )
                        .is_ok() =>
                    {
                        Err(CoinbaseDirectSessionError::Subscription)
                    }
                    Err(error) => Err(error.into()),
                }
            }
        };
        output.try_publish(frame).map_err(SourceError::Sink)?;
        let Some(decoded) = decoded? else {
            self.acknowledgement_evidence = acknowledgement_evidence;
            return Ok(MessageDisposition::Acknowledged);
        };
        match decoded {
            CoinbaseDirectDecodeOutcome::Sequenced(event) => {
                let sequence = event.sequence();
                let evidence = event.evidence().clone();
                if let Some(previous) = self.last_sequenced_frame.as_ref() {
                    validate_sequenced_progression(previous.sequence, sequence)?;
                }
                match mode {
                    InboundMode::Queueing => self.book.try_queue(event)?,
                    InboundMode::Live => {
                        let book_sequence = self
                            .book
                            .last_sequence()
                            .ok_or(DirectOrderBookError::WrongPhase)?;
                        if self.has_published_book || sequence > book_sequence {
                            self.book.try_apply_live(event)?;
                        }
                    }
                    InboundMode::AwaitingAck => {
                        return Err(CoinbaseDirectSessionError::Subscription);
                    }
                }
                output
                    .try_retain_sequenced_frame(&evidence)
                    .map_err(SourceError::Sink)?;
                self.last_sequenced_frame = Some(SequencedFrameEvidence { sequence, evidence });
                if mode == InboundMode::Live {
                    if self.has_published_book {
                        self.publish_book(output)?;
                    } else {
                        self.publish_initial_book_if_exact(output)?;
                    }
                }
                Ok(MessageDisposition::Sequenced)
            }
            CoinbaseDirectDecodeOutcome::NonBook(event) => {
                output
                    .try_publish_non_book(event)
                    .map_err(SourceError::Sink)?;
                Ok(MessageDisposition::NonBook)
            }
        }
    }

    fn capture_rejected_protocol_frame(
        &mut self,
        transport: TransportFrameKind,
        payload: Bytes,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        ensure_frame_bound(
            payload.len(),
            self.config.limits().websocket().max_frame_bytes(),
        )?;
        let frame = self.authority.frames_mut()?.try_frame(transport, payload)?;
        let _validated = self.authority.validate_live_frame(&frame)?;
        output.try_publish(frame).map_err(SourceError::Sink)?;
        Ok(())
    }

    fn publish_book(
        &mut self,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let sequence = self
            .book
            .last_sequence()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        let source_timestamp = self
            .book
            .source_timestamp()
            .ok_or(DirectOrderBookError::SnapshotTimestampRequired)?;
        let snapshot_receipt = self
            .snapshot_receipt
            .as_ref()
            .ok_or(DirectOrderBookError::SnapshotReceiptRequired)?;
        let sequenced_frame = self
            .last_sequenced_frame
            .as_ref()
            .ok_or(CoinbaseDirectSessionError::WebSocketProtocol)?;
        if sequenced_frame.sequence != sequence {
            return Err(CoinbaseDirectSessionError::WebSocketProtocol);
        }
        let subscription_evidence = self
            .acknowledgement_evidence
            .as_ref()
            .ok_or(CoinbaseDirectSessionError::Subscription)?;
        let book = self
            .book
            .published_book()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        self.next_published_state.replace_from(book);
        let publication = if !self.has_published_book {
            CoinbaseDirectPublicationKind::Snapshot
        } else if self.published_state == self.next_published_state {
            CoinbaseDirectPublicationKind::Quote
        } else {
            CoinbaseDirectPublicationKind::Delta
        };
        let previous = self.has_published_book.then_some(&self.published_state);
        output
            .try_publish_book(CoinbaseDirectBookUpdate {
                config: &self.config,
                sequence,
                source_timestamp,
                decoder_evidence: &sequenced_frame.evidence,
                subscription_evidence,
                snapshot_receipt,
                book,
                previous,
                current: &self.next_published_state,
                publication,
            })
            .map_err(SourceError::Sink)?;
        std::mem::swap(&mut self.published_state, &mut self.next_published_state);
        self.next_published_state.clear();
        self.has_published_book = true;
        Ok(())
    }

    fn publish_initial_book_if_exact(
        &mut self,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let sequence = self
            .book
            .last_sequence()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        let observed = self
            .last_sequenced_frame
            .as_ref()
            .ok_or(CoinbaseDirectSessionError::WebSocketProtocol)?
            .sequence;
        match observed.cmp(&sequence) {
            Ordering::Equal => self.publish_book(output),
            Ordering::Less => Ok(()),
            Ordering::Greater => Err(CoinbaseDirectSessionError::WebSocketProtocol),
        }
    }

    fn validate_generation(&self) -> Result<(), SourceError> {
        self.authority.validate_current()?;
        let issued = self
            .authority
            .budget()?
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        if !self.budget.shares_allocation_with(issued) {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        Ok(())
    }

    fn authorize_endpoint(&self, target: &str) -> Result<(), SourceError> {
        self.config
            .metadata()
            .network_policy()
            .authorize(target)
            .map_err(|_error| SourceError::InvalidProtocolState)
    }

    fn acquire_budget(&self) -> Result<BudgetPermit, SourceError> {
        self.validate_generation()?;
        match self.budget.try_acquire() {
            BudgetDecision::Ready(permit) => Ok(permit),
            BudgetDecision::WaitUntil(deadline) => Err(SourceError::BudgetWaitUntil { deadline }),
            BudgetDecision::Unavailable(reason) => Err(SourceError::BudgetUnavailable { reason }),
        }
    }

    fn schedule_product_refresh(&mut self) -> Result<(), CoinbaseDirectSessionError> {
        self.next_product_refresh = Some(
            Instant::now()
                .checked_add(self.config.limits().product_refresh_interval())
                .ok_or(CoinbaseDirectSessionError::ProductRefreshDeadline)?,
        );
        Ok(())
    }

    fn begin_generation(&mut self) -> Result<(), SourceError> {
        if self.generation_started {
            return Err(SourceError::InvalidProtocolState);
        }
        self.generation_started = true;
        Ok(())
    }

    fn finish_generation(
        &mut self,
        outcome: Result<(), CoinbaseDirectSessionError>,
    ) -> Result<(), CoinbaseDirectSessionError> {
        if outcome.is_err() {
            self.book.invalidate_generation();
            self.acknowledgement_evidence = None;
            self.last_sequenced_frame = None;
            self.snapshot_receipt = None;
            self.published_state.clear();
            self.next_published_state.clear();
            self.has_published_book = false;
            self.next_product_refresh = None;
        }
        outcome
    }

    async fn shutdown_socket<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        deadline: Duration,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let operation = async {
            socket
                .send(Message::Close(None))
                .await
                .map_err(|_error| CoinbaseDirectSessionError::Shutdown)?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        self.capture_rejected_protocol_frame(
                            TransportFrameKind::Text,
                            Bytes::copy_from_slice(text.as_bytes()),
                            output,
                        )?;
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                    Some(Ok(Message::Binary(payload))) => {
                        self.capture_rejected_protocol_frame(
                            TransportFrameKind::Binary,
                            payload,
                            output,
                        )?;
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                    Some(Ok(Message::Ping(_) | Message::Frame(_))) | Some(Err(_)) => {
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                }
            }
        };
        tokio::time::timeout(deadline, operation)
            .await
            .map_err(|_elapsed| CoinbaseDirectSessionError::Shutdown)?
    }
}

impl SourceMetadataProvider for CoinbaseDirectSession {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

fn direct_http_bounds(
    config: &CoinbaseDirectConfig,
) -> Result<market_squawk_sources::HttpRequestBounds, CoinbaseDirectSessionError> {
    match config.metadata().network_policy() {
        NetworkAccessPolicy::Allowlisted(policy) => Ok(policy.request_bounds()),
        NetworkAccessPolicy::Denied => Err(SourceError::InvalidProtocolState.into()),
    }
}

fn current_unix_seconds() -> Result<u64, CoinbaseDirectSigningError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .filter(|seconds| *seconds > 0)
        .ok_or(CoinbaseDirectSigningError::InvalidTimestamp)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboundMode {
    AwaitingAck,
    Queueing,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MessageDisposition {
    Acknowledged,
    Control,
    NonBook,
    Sequenced,
}

#[derive(Clone, Debug)]
enum LiveRead {
    Message(Message),
    RefreshProduct,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionAck {
    #[serde(rename = "type")]
    kind: String,
    channels: [SubscriptionAckChannel; 1],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionAckChannel {
    name: String,
    product_ids: [String; 1],
}

fn validate_subscription_ack(
    payload: &[u8],
    product: &market_squawk_domain::ProviderProduct,
) -> Result<(), CoinbaseDirectSessionError> {
    let ack: SubscriptionAck = serde_json::from_slice(payload)
        .map_err(|_error| CoinbaseDirectSessionError::Subscription)?;
    let channel = &ack.channels[0];
    if ack.kind != "subscriptions"
        || channel.name != "full"
        || channel.product_ids[0] != product.as_source_identifier().as_str()
    {
        return Err(CoinbaseDirectSessionError::Subscription);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct ReceiveDeadline {
    at: Instant,
    sink_owned: bool,
}

impl ReceiveDeadline {
    fn strictest(
        output: &dyn CoinbaseDirectOutput,
        transport_timeout: Duration,
    ) -> Result<Self, SourceError> {
        let transport = Instant::now()
            .checked_add(transport_timeout)
            .ok_or(SourceError::InvalidProtocolState)?;
        match output.next_deadline() {
            Some(sink_deadline) if sink_deadline <= transport => Ok(Self {
                at: sink_deadline,
                sink_owned: true,
            }),
            _ => Ok(Self {
                at: transport,
                sink_owned: false,
            }),
        }
    }
}

async fn read_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    output: &mut dyn CoinbaseDirectOutput,
    cancellation: &CancellationToken,
    transport_timeout: Duration,
) -> Result<Message, CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = ReceiveDeadline::strictest(output, transport_timeout)?;
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(SourceError::Cancelled.into()),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
            if deadline.sink_owned {
                output
                    .poll_deadline(Instant::now())
                    .map_err(SourceError::Sink)?;
                return Err(SourceError::InvalidProtocolState.into());
            }
            return Err(SourceError::ConnectionIdle.into());
        }
        result = socket.next() => result,
    };
    map_next_message(next)
}

async fn read_with_refresh_deadline<S>(
    socket: &mut WebSocketStream<S>,
    output: &mut dyn CoinbaseDirectOutput,
    cancellation: &CancellationToken,
    transport_timeout: Duration,
    refresh_at: Instant,
) -> Result<LiveRead, CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let receive_deadline = ReceiveDeadline::strictest(output, transport_timeout)?;
    if refresh_at >= receive_deadline.at {
        return read_with_deadline(socket, output, cancellation, transport_timeout)
            .await
            .map(LiveRead::Message);
    }

    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled.into()),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(refresh_at)) => {
            Ok(LiveRead::RefreshProduct)
        }
        message = socket.next() => map_next_message(message).map(LiveRead::Message),
    }
}

fn map_next_message(
    message: Option<Result<Message, WebSocketError>>,
) -> Result<Message, CoinbaseDirectSessionError> {
    match message {
        Some(Ok(message)) => Ok(message),
        Some(Err(_error)) => Err(SourceError::Network.into()),
        None => Err(SourceError::ProviderUnavailable.into()),
    }
}

async fn await_websocket<T, E, F>(
    cancellation: &CancellationToken,
    deadline: Duration,
    operation: impl Future<Output = Result<T, E>>,
    map_error: F,
) -> Result<T, CoinbaseDirectSessionError>
where
    F: FnOnce(E) -> SourceError,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled.into()),
        result = tokio::time::timeout(deadline, operation) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_error(error).into()),
            Err(_elapsed) => Err(SourceError::Network.into()),
        }
    }
}

async fn send_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_websocket(cancellation, deadline, socket.send(message), |_error| {
        SourceError::Network
    })
    .await
}

async fn flush_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_websocket(cancellation, deadline, socket.flush(), |_error| {
        SourceError::Network
    })
    .await
}

fn ensure_frame_bound(actual: usize, maximum: usize) -> Result<(), CoinbaseDirectSessionError> {
    if actual > maximum {
        Err(SourceError::FrameTooLarge { max: maximum }.into())
    } else {
        Ok(())
    }
}

fn validate_sequenced_progression(
    previous: SequenceNumber,
    observed: SequenceNumber,
) -> Result<(), CoinbaseDirectSessionError> {
    if observed == previous {
        return Err(DirectOrderBookError::DuplicateSequence.into());
    }
    if observed < previous {
        return Err(DirectOrderBookError::SequenceRegression.into());
    }
    let expected = previous
        .checked_next()
        .map_err(|_error| DirectOrderBookError::SequenceExhausted)?;
    if observed != expected {
        return Err(DirectOrderBookError::SequenceGap.into());
    }
    Ok(())
}

fn handoff_frontier_payload(
    receipt: &SegmentedHttpResponseReceipt,
    generation: ConnectionGeneration,
) -> Bytes {
    let mut payload = [0_u8; 56];
    payload[..8].copy_from_slice(b"MSQCBF01");
    payload[8..40].copy_from_slice(&receipt.body_digest().bytes());
    payload[40..48].copy_from_slice(&receipt.received_at().unix_nanos().to_be_bytes());
    payload[48..].copy_from_slice(&generation.get().to_be_bytes());
    Bytes::copy_from_slice(&payload)
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn map_http_transport_error(error: CoinbaseDirectHttpTransportError) -> CoinbaseDirectSessionError {
    match error {
        CoinbaseDirectHttpTransportError::Network => SourceError::Network.into(),
        CoinbaseDirectHttpTransportError::Deadline => CoinbaseDirectSessionError::HttpDeadline,
        CoinbaseDirectHttpTransportError::Cancelled => SourceError::Cancelled.into(),
        CoinbaseDirectHttpTransportError::BodyTooLarge => {
            CoinbaseDirectSessionError::HttpBodyTooLarge
        }
        CoinbaseDirectHttpTransportError::SegmentLimit => {
            CoinbaseDirectSessionError::HttpSegmentLimit
        }
        CoinbaseDirectHttpTransportError::Protocol
        | CoinbaseDirectHttpTransportError::Allocation => CoinbaseDirectSessionError::HttpResponse,
    }
}

fn map_connect_error(error: WebSocketError, budget: &SharedProviderBudget) -> SourceError {
    if let WebSocketError::Http(response) = &error {
        let status = response.status();
        if matches!(status.as_u16(), 401 | 403) {
            return SourceError::Unauthorized;
        }
        if status.as_u16() == 429 || status.is_server_error() {
            return SourceError::from_applied_budget_refusal(apply_http_retry_after(
                budget,
                response
                    .headers()
                    .get(tokio_tungstenite::tungstenite::http::header::RETRY_AFTER)
                    .map(|value| value.as_bytes()),
                1_000,
            ));
        }
    }
    SourceError::Network
}

#[cfg(test)]
#[path = "direct_transport/tests.rs"]
mod tests;
