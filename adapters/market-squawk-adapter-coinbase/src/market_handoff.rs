use market_squawk_domain::{
    CapturePayload, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    LiveEventClass, MarketDepth, PriceTicks, ProviderProduct, QuantityLots, SequenceNumber,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    DecodedProviderBatch, DecoderEvidence, ProviderBookSide, ProviderNativeInstrumentAttestation,
    ProviderOrderEvent, ProviderOrderEventKind, SegmentedHttpResponseCapture,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{COINBASE_DIRECT_VERIFY_ENDPOINT, CoinbaseDirectConfig, CoinbaseExchangeConfig};

/// Coinbase transport profile that produced one typed market-data handoff.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoinbaseMarketFeed {
    /// Public Advanced Trade market-data WebSocket.
    AdvancedTradePublic,
    /// Optional authenticated Exchange Direct `full` feed.
    ExchangeDirectFull,
}

/// Exact Coinbase channel represented by one handoff.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoinbaseMarketChannel {
    /// Advanced Trade price-level book channel.
    Level2,
    /// Advanced Trade public trade channel.
    MarketTrades,
    /// Exchange Direct authenticated `full` channel.
    Full,
}

/// Terminal provider continuity retained without overstating provider guarantees.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoinbaseMarketContinuity {
    /// Advanced Trade supplied an envelope cursor, but this profile does not prove contiguity.
    ProviderCursorUnverified { terminal: u64 },
    /// Direct replay proved every successor from the exact REST snapshot through `terminal`.
    SnapshotContiguous {
        snapshot: SequenceNumber,
        terminal: SequenceNumber,
    },
}

impl CoinbaseMarketContinuity {
    /// Returns the final provider cursor represented by this handoff.
    pub const fn terminal(self) -> u64 {
        match self {
            Self::ProviderCursorUnverified { terminal } => terminal,
            Self::SnapshotContiguous { terminal, .. } => terminal.get(),
        }
    }
}

/// Native Coinbase Direct match identity retained alongside its exact replay frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectTradeEvidence {
    trade_id: u64,
    maker_order_id: SourceIdentifier,
    taker_order_id: SourceIdentifier,
    maker_side: ProviderBookSide,
    price: PriceTicks,
    quantity: QuantityLots,
    sequence: SequenceNumber,
    provider_timestamp: Timestamp,
}

impl CoinbaseDirectTradeEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "exact provider match fields stay explicit"
    )]
    pub(crate) const fn new(
        trade_id: u64,
        maker_order_id: SourceIdentifier,
        taker_order_id: SourceIdentifier,
        maker_side: ProviderBookSide,
        price: PriceTicks,
        quantity: QuantityLots,
        sequence: SequenceNumber,
        provider_timestamp: Timestamp,
    ) -> Self {
        Self {
            trade_id,
            maker_order_id,
            taker_order_id,
            maker_side,
            price,
            quantity,
            sequence,
            provider_timestamp,
        }
    }

    /// Returns Coinbase's numeric trade identifier.
    pub const fn trade_id(&self) -> u64 {
        self.trade_id
    }

    /// Returns the maker order identifier mutated by the match.
    pub const fn maker_order_id(&self) -> &SourceIdentifier {
        &self.maker_order_id
    }

    /// Returns the provider-authored taker order identifier.
    pub const fn taker_order_id(&self) -> &SourceIdentifier {
        &self.taker_order_id
    }

    /// Returns the provider-reported maker side.
    pub const fn maker_side(&self) -> ProviderBookSide {
        self.maker_side
    }

    /// Returns the instrument-scaled match price.
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    /// Returns the instrument-scaled match quantity.
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns the Direct product cursor carried by this match.
    pub const fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Returns the venue event clock carried by this match.
    pub const fn provider_timestamp(&self) -> Timestamp {
        self.provider_timestamp
    }
}

/// One exact post-snapshot Direct frame with its typed order event and optional native match.
#[derive(Debug)]
pub struct CoinbaseDirectReplayFrame {
    event: ProviderOrderEvent,
    raw_payload: CapturePayload,
    native_trade: Option<CoinbaseDirectTradeEvidence>,
}

impl CoinbaseDirectReplayFrame {
    pub(crate) fn try_new(
        event: ProviderOrderEvent,
        raw_payload: CapturePayload,
        native_trade: Option<CoinbaseDirectTradeEvidence>,
    ) -> Result<Self, CoinbaseMarketHandoffError> {
        event
            .evidence()
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseMarketHandoffError::StaleAuthority)?;
        if event.wire_bytes() != raw_payload.as_bytes().len()
            || event.evidence().payload_digest() != exact_digest(raw_payload.as_bytes())
            || !native_trade_matches_event(native_trade.as_ref(), &event)
        {
            return Err(CoinbaseMarketHandoffError::EvidenceMismatch);
        }
        Ok(Self {
            event,
            raw_payload,
            native_trade,
        })
    }

    /// Returns the exact Direct product sequence.
    pub const fn sequence(&self) -> SequenceNumber {
        self.event.sequence()
    }

    /// Returns the already-decoded provider order event.
    pub const fn event(&self) -> &ProviderOrderEvent {
        &self.event
    }

    /// Returns decoder evidence bound to the exact frame bytes.
    pub const fn decoder_evidence(&self) -> &DecoderEvidence {
        self.event.evidence()
    }

    /// Returns the exact bounded WebSocket payload.
    pub const fn raw_payload(&self) -> &CapturePayload {
        &self.raw_payload
    }

    /// Returns complete native match identity when this exact frame is a `match`.
    pub const fn native_trade(&self) -> Option<&CoinbaseDirectTradeEvidence> {
        self.native_trade.as_ref()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ProviderOrderEvent,
        CapturePayload,
        Option<CoinbaseDirectTradeEvidence>,
    ) {
        (self.event, self.raw_payload, self.native_trade)
    }
}

/// Exact Direct initial-state material pending application-owned physical sealing.
#[derive(Debug)]
pub struct CoinbaseDirectInitialMarketLineage {
    snapshot: SegmentedHttpResponseCapture,
    replay: Vec<CoinbaseDirectReplayFrame>,
}

impl CoinbaseDirectInitialMarketLineage {
    pub(crate) fn try_new(
        snapshot: SegmentedHttpResponseCapture,
        replay: Vec<CoinbaseDirectReplayFrame>,
    ) -> Result<Self, CoinbaseMarketHandoffError> {
        snapshot
            .receipt()
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseMarketHandoffError::StaleAuthority)?;
        if replay.is_empty() {
            return Err(CoinbaseMarketHandoffError::EvidenceMismatch);
        }
        Ok(Self { snapshot, replay })
    }

    /// Returns the exact segmented response capture; no receipt-only surrogate is exposed.
    pub const fn snapshot(&self) -> &SegmentedHttpResponseCapture {
        &self.snapshot
    }

    /// Returns every admitted frame strictly after the snapshot cutoff in sequence order.
    pub fn replay(&self) -> &[CoinbaseDirectReplayFrame] {
        &self.replay
    }

    /// Consumes the pending lineage into exact snapshot-response and ordered replay material.
    ///
    /// No value returned by this split claims that bytes have been physically sealed. The
    /// publication handoff converts both parts into application-consumable raw material and later
    /// accepts only the exact one-use common seal tokens returned by that application boundary.
    pub fn into_sealing_split(
        self,
    ) -> (SegmentedHttpResponseCapture, Vec<CoinbaseDirectReplayFrame>) {
        (self.snapshot, self.replay)
    }
}

/// Closed raw lineage carried by one Coinbase market handoff.
#[derive(Debug)]
pub enum CoinbaseMarketRawLineage {
    /// One exact public Advanced Trade frame.
    AdvancedTrade(CapturePayload),
    /// Exact Direct level-3 snapshot plus all admitted post-cutoff replay frames.
    DirectInitial(CoinbaseDirectInitialMarketLineage),
}

impl CoinbaseMarketRawLineage {
    fn terminal_payload(&self) -> Option<&CapturePayload> {
        match self {
            Self::AdvancedTrade(payload) => Some(payload),
            Self::DirectInitial(lineage) => lineage
                .replay
                .last()
                .map(CoinbaseDirectReplayFrame::raw_payload),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CoinbaseMarketHandoffInput {
    pub(crate) feed: CoinbaseMarketFeed,
    pub(crate) channel: CoinbaseMarketChannel,
    pub(crate) native_input_depth: Option<MarketDepth>,
    pub(crate) product: ProviderProduct,
    pub(crate) instrument_attestation: Arc<ProviderNativeInstrumentAttestation>,
    pub(crate) request_set_digest: EvidenceDigest,
    pub(crate) subscription_digest: EvidenceDigest,
    pub(crate) subscription_acknowledgement: Option<ExactPayloadEvidence>,
    pub(crate) continuity: CoinbaseMarketContinuity,
    pub(crate) provider_published_at: Timestamp,
    pub(crate) snapshot_provider_at: Option<Timestamp>,
}

/// Relationally derived provider evidence consumed with exact raw lineage and typed observations.
#[derive(Debug)]
pub struct CoinbaseMarketHandoffEvidence {
    feed: CoinbaseMarketFeed,
    channel: CoinbaseMarketChannel,
    event_class: LiveEventClass,
    native_input_depth: Option<MarketDepth>,
    output_depth: Option<MarketDepth>,
    product: ProviderProduct,
    instrument_attestation: Arc<ProviderNativeInstrumentAttestation>,
    request_set_digest: EvidenceDigest,
    subscription_digest: EvidenceDigest,
    subscription_acknowledgement: Option<ExactPayloadEvidence>,
    continuity: CoinbaseMarketContinuity,
    provider_published_at: Timestamp,
    snapshot_provider_at: Option<Timestamp>,
}

impl CoinbaseMarketHandoffEvidence {
    /// Returns the exact Coinbase transport profile.
    pub const fn feed(&self) -> CoinbaseMarketFeed {
        self.feed
    }

    /// Returns the exact Coinbase subscription channel.
    pub const fn channel(&self) -> CoinbaseMarketChannel {
        self.channel
    }

    /// Returns the event class derived from every typed observation in the batch.
    pub const fn event_class(&self) -> LiveEventClass {
        self.event_class
    }

    /// Returns the provider-native input depth before projection.
    pub const fn native_input_depth(&self) -> Option<MarketDepth> {
        self.native_input_depth
    }

    /// Returns the output depth derived from every typed observation in the batch.
    pub const fn output_depth(&self) -> Option<MarketDepth> {
        self.output_depth
    }

    /// Returns Coinbase's native product identity.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the externally configured instrument binding; the adapter never mints it.
    pub fn configured_instrument(&self) -> InstrumentId {
        self.instrument_attestation.instrument_id()
    }

    /// Returns the exact venue identity.
    pub fn venue(&self) -> &VenueId {
        self.instrument_attestation.venue_mapping().venue_id()
    }

    /// Returns the exact durable provider/canonical identity selected before the session.
    pub fn instrument_attestation(&self) -> &ProviderNativeInstrumentAttestation {
        self.instrument_attestation.as_ref()
    }

    /// Returns SHA-256 over the selected secret-free request set.
    pub const fn request_set_digest(&self) -> EvidenceDigest {
        self.request_set_digest
    }

    /// Returns SHA-256 over exact outbound subscription bytes.
    pub const fn subscription_digest(&self) -> EvidenceDigest {
        self.subscription_digest
    }

    /// Returns exact inbound Direct subscription acknowledgement evidence, when applicable.
    pub const fn subscription_acknowledgement(&self) -> Option<&ExactPayloadEvidence> {
        self.subscription_acknowledgement.as_ref()
    }

    /// Returns the truthful provider continuity class.
    pub const fn continuity(&self) -> CoinbaseMarketContinuity {
        self.continuity
    }

    /// Returns the provider publication/event clock for the terminal frame.
    pub const fn provider_published_at(&self) -> Timestamp {
        self.provider_published_at
    }

    /// Returns the Direct REST snapshot provider clock, when applicable.
    pub const fn snapshot_provider_at(&self) -> Option<Timestamp> {
        self.snapshot_provider_at
    }
}

/// One non-serializable, consuming Coinbase provider boundary.
#[derive(Debug)]
pub struct CoinbaseMarketHandoff {
    evidence: CoinbaseMarketHandoffEvidence,
    raw_lineage: CoinbaseMarketRawLineage,
    typed_batch: DecodedProviderBatch,
}

impl CoinbaseMarketHandoff {
    pub(crate) fn try_new(
        input: CoinbaseMarketHandoffInput,
        raw_lineage: CoinbaseMarketRawLineage,
        typed_batch: DecodedProviderBatch,
    ) -> Result<Self, CoinbaseMarketHandoffError> {
        typed_batch
            .evidence()
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseMarketHandoffError::StaleAuthority)?;
        let (event_class, output_depth) = relational_batch_shape(&typed_batch)?;
        let terminal_payload = raw_lineage
            .terminal_payload()
            .ok_or(CoinbaseMarketHandoffError::EvidenceMismatch)?;
        let decoder = typed_batch.evidence();
        if terminal_payload.as_bytes().len() != decoder.frame_bytes()
            || exact_digest(terminal_payload.as_bytes()) != decoder.payload_digest()
            || input
                .instrument_attestation
                .validate_at(decoder.received_at())
                .is_err()
            || typed_batch.observations().iter().any(|observation| {
                observation.instrument_attestation() != input.instrument_attestation.as_ref()
                    || observation.venue()
                        != input.instrument_attestation.venue_mapping().venue_id()
                    || observation.instrument() != input.instrument_attestation.instrument_id()
            })
        {
            return Err(CoinbaseMarketHandoffError::EvidenceMismatch);
        }

        match (&raw_lineage, input.feed, input.channel, input.continuity) {
            (
                CoinbaseMarketRawLineage::AdvancedTrade(_),
                CoinbaseMarketFeed::AdvancedTradePublic,
                CoinbaseMarketChannel::Level2,
                CoinbaseMarketContinuity::ProviderCursorUnverified { .. },
            ) if input.native_input_depth == Some(MarketDepth::PriceLevel)
                && output_depth == Some(MarketDepth::PriceLevel)
                && matches!(
                    event_class,
                    LiveEventClass::BookSnapshot | LiveEventClass::BookDelta
                )
                && input.subscription_acknowledgement.is_none()
                && input.snapshot_provider_at.is_none() => {}
            (
                CoinbaseMarketRawLineage::AdvancedTrade(_),
                CoinbaseMarketFeed::AdvancedTradePublic,
                CoinbaseMarketChannel::MarketTrades,
                CoinbaseMarketContinuity::ProviderCursorUnverified { .. },
            ) if input.native_input_depth.is_none()
                && output_depth.is_none()
                && event_class == LiveEventClass::Trade
                && input.subscription_acknowledgement.is_none()
                && input.snapshot_provider_at.is_none() => {}
            (
                CoinbaseMarketRawLineage::DirectInitial(lineage),
                CoinbaseMarketFeed::ExchangeDirectFull,
                CoinbaseMarketChannel::Full,
                CoinbaseMarketContinuity::SnapshotContiguous { snapshot, terminal },
            ) if input.native_input_depth == Some(MarketDepth::OrderLevel)
                && output_depth == Some(MarketDepth::PriceLevel)
                && event_class == LiveEventClass::BookSnapshot
                && input.subscription_acknowledgement.is_some()
                && input.snapshot_provider_at.is_some() =>
            {
                validate_direct_initial(lineage, decoder, snapshot, terminal, &input)?;
            }
            _ => return Err(CoinbaseMarketHandoffError::ProfileMismatch),
        }

        Ok(Self {
            evidence: CoinbaseMarketHandoffEvidence {
                feed: input.feed,
                channel: input.channel,
                event_class,
                native_input_depth: input.native_input_depth,
                output_depth,
                product: input.product,
                instrument_attestation: input.instrument_attestation,
                request_set_digest: input.request_set_digest,
                subscription_digest: input.subscription_digest,
                subscription_acknowledgement: input.subscription_acknowledgement,
                continuity: input.continuity,
                provider_published_at: input.provider_published_at,
                snapshot_provider_at: input.snapshot_provider_at,
            },
            raw_lineage,
            typed_batch,
        })
    }

    /// Returns the validated provider evidence.
    pub const fn evidence(&self) -> &CoinbaseMarketHandoffEvidence {
        &self.evidence
    }

    /// Returns the exact closed raw lineage.
    pub const fn raw_lineage(&self) -> &CoinbaseMarketRawLineage {
        &self.raw_lineage
    }

    /// Returns the terminal exact provider payload.
    pub fn raw_payload(&self) -> &CapturePayload {
        self.raw_lineage
            .terminal_payload()
            .expect("validated Coinbase handoff always has a terminal payload")
    }

    /// Returns the already-decoded message-atomic provider batch.
    pub const fn typed_batch(&self) -> &DecodedProviderBatch {
        &self.typed_batch
    }

    /// Returns the terminal payload digest bound by decoder evidence.
    pub const fn raw_payload_digest(&self) -> EvidenceDigest {
        self.typed_batch.evidence().payload_digest()
    }

    /// Consumes the only handoff into evidence, exact raw lineage, and typed observations.
    pub fn into_parts(
        self,
    ) -> (
        CoinbaseMarketHandoffEvidence,
        CoinbaseMarketRawLineage,
        DecodedProviderBatch,
    ) {
        (self.evidence, self.raw_lineage, self.typed_batch)
    }
}

/// Public decoder result preserving typed non-market outcomes without a generic market bypass.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the bounded handoff stays allocation-free and is consumed immediately"
)]
pub enum CoinbaseMarketDecodeOutcome {
    /// A consuming public market-data handoff.
    Market(CoinbaseMarketHandoff),
    /// A typed control, ignore, recovery, or quarantine outcome.
    Other(market_squawk_sources::DecodeOutcome),
}

/// Failure to bind typed provider data to exact raw lineage and request evidence.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseMarketHandoffError {
    /// Registry-backed authority is no longer current.
    #[error("Coinbase market handoff authority is no longer current")]
    StaleAuthority,
    /// Raw, typed, replay, or snapshot evidence does not match.
    #[error("Coinbase market handoff raw and typed evidence do not match")]
    EvidenceMismatch,
    /// Feed, channel, class, native input depth, output depth, or continuity is inconsistent.
    #[error("Coinbase market handoff profile is inconsistent")]
    ProfileMismatch,
    /// A bounded replay or evidence container could not be allocated within its admitted slots.
    #[error("Coinbase market handoff bounded allocation failed")]
    Allocation,
}

fn relational_batch_shape(
    batch: &DecodedProviderBatch,
) -> Result<(LiveEventClass, Option<MarketDepth>), CoinbaseMarketHandoffError> {
    let first = batch
        .observations()
        .first()
        .ok_or(CoinbaseMarketHandoffError::EvidenceMismatch)?;
    let event_class = first.payload().event_class();
    let depth = first.payload().depth();
    if first.event_class() != event_class
        || first.depth() != depth
        || batch.observations().iter().any(|observation| {
            observation.payload().event_class() != event_class
                || observation.payload().depth() != depth
                || observation.event_class() != event_class
                || observation.depth() != depth
        })
    {
        return Err(CoinbaseMarketHandoffError::ProfileMismatch);
    }
    Ok((event_class, depth))
}

fn validate_direct_initial(
    lineage: &CoinbaseDirectInitialMarketLineage,
    terminal_decoder: &DecoderEvidence,
    snapshot: SequenceNumber,
    terminal: SequenceNumber,
    input: &CoinbaseMarketHandoffInput,
) -> Result<(), CoinbaseMarketHandoffError> {
    let receipt = lineage.snapshot.receipt();
    receipt
        .currentness_lease()
        .validate_current()
        .map_err(|_error| CoinbaseMarketHandoffError::StaleAuthority)?;
    if !terminal_decoder
        .binding()
        .shares_allocation_with(receipt.binding())
        || !terminal_decoder
            .currentness_lease()
            .shares_authority_with(receipt.currentness_lease())
        || input.snapshot_provider_at.is_none()
    {
        return Err(CoinbaseMarketHandoffError::EvidenceMismatch);
    }
    let expected_first = snapshot
        .checked_next()
        .map_err(|_error| CoinbaseMarketHandoffError::EvidenceMismatch)?;
    let mut previous = None;
    for frame in &lineage.replay {
        frame
            .decoder_evidence()
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseMarketHandoffError::StaleAuthority)?;
        let expected = previous
            .map_or(Ok(expected_first), SequenceNumber::checked_next)
            .map_err(|_error| CoinbaseMarketHandoffError::EvidenceMismatch)?;
        if frame.sequence() != expected {
            return Err(CoinbaseMarketHandoffError::EvidenceMismatch);
        }
        previous = Some(frame.sequence());
    }
    let last = lineage
        .replay
        .last()
        .ok_or(CoinbaseMarketHandoffError::EvidenceMismatch)?;
    if last.sequence() != terminal
        || last.decoder_evidence().frame_id() != terminal_decoder.frame_id()
        || last.decoder_evidence().payload_digest() != terminal_decoder.payload_digest()
        || last.event().timestamp() != input.provider_published_at
    {
        return Err(CoinbaseMarketHandoffError::EvidenceMismatch);
    }
    Ok(())
}

fn native_trade_matches_event(
    native: Option<&CoinbaseDirectTradeEvidence>,
    event: &ProviderOrderEvent,
) -> bool {
    match (native, event.kind()) {
        (
            Some(trade),
            ProviderOrderEventKind::Match {
                maker_order_id,
                maker_side,
                maker_price,
                quantity,
            },
        ) => {
            trade.maker_order_id == *maker_order_id
                && trade.maker_side == *maker_side
                && trade.price == *maker_price
                && trade.quantity == *quantity
                && trade.sequence == event.sequence()
                && trade.provider_timestamp == event.timestamp()
        }
        (None, ProviderOrderEventKind::Match { .. }) | (Some(_), _) => false,
        (None, _) => true,
    }
}

pub(crate) fn exact_digest(payload: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(payload).into())
}

pub(crate) fn public_request_digests(
    config: &CoinbaseExchangeConfig,
) -> (EvidenceDigest, EvidenceDigest) {
    let subscription = digest_parts(
        b"market-squawk/coinbase/advanced-trade/subscriptions/v1",
        config
            .subscriptions()
            .iter()
            .map(|payload| payload.as_bytes()),
    );
    let request = digest_parts(
        b"market-squawk/coinbase/advanced-trade/request-set/v1",
        std::iter::once(config.endpoint().as_bytes()).chain(
            config
                .subscriptions()
                .iter()
                .map(|payload| payload.as_bytes()),
        ),
    );
    (request, subscription)
}

pub(crate) fn direct_request_set_digest(config: &CoinbaseDirectConfig) -> EvidenceDigest {
    digest_parts(
        b"market-squawk/coinbase/exchange-direct/request-set/v1",
        [
            config.websocket_endpoint().as_bytes(),
            b"full".as_slice(),
            b"GET".as_slice(),
            COINBASE_DIRECT_VERIFY_ENDPOINT.as_bytes(),
            b"GET".as_slice(),
            config.product_url().as_bytes(),
            b"GET".as_slice(),
            config.snapshot_url().as_bytes(),
            config.product().as_source_identifier().as_str().as_bytes(),
        ],
    )
}

fn digest_parts<'a>(domain: &[u8], parts: impl IntoIterator<Item = &'a [u8]>) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(domain.len().to_be_bytes());
    hasher.update(domain);
    for part in parts {
        hasher.update(part.len().to_be_bytes());
        hasher.update(part);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}
use std::sync::Arc;
