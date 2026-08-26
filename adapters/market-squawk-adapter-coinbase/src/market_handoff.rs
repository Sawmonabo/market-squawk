use market_squawk_domain::{
    CapturePayload, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    LiveEventClass, MarketDepth, PriceTicks, ProviderProduct, QuantityLots, SequenceNumber,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    DecodedProviderBatch, DecoderEvidence, ProviderBookSide, ProviderOrderEvent,
    ProviderOrderEventKind, SegmentedHttpResponseCapture,
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
}

/// Opaque, one-shot expectation for the missing common snapshot logical-object publisher.
///
/// This is not a sealed-capture claim. It only binds the exact snapshot material and ordered
/// replay digests that a future common publisher must accept atomically. It is deliberately
/// non-cloneable and has no provider-local completion constructor.
#[derive(Debug)]
pub struct CoinbaseDirectSnapshotSealExpectation {
    snapshot_body_digest: EvidenceDigest,
    snapshot_body_length: u64,
    snapshot_received_at: Timestamp,
    replay_payload_digests: Vec<EvidenceDigest>,
}

impl CoinbaseDirectSnapshotSealExpectation {
    /// Returns the exact logical-object body digest that must be sealed.
    pub const fn snapshot_body_digest(&self) -> EvidenceDigest {
        self.snapshot_body_digest
    }

    /// Returns the exact logical-object body length that must be sealed.
    pub const fn snapshot_body_length(&self) -> u64 {
        self.snapshot_body_length
    }

    /// Returns the registry-trusted completion clock of the exact response body.
    pub const fn snapshot_received_at(&self) -> Timestamp {
        self.snapshot_received_at
    }

    /// Returns every admitted post-snapshot WebSocket payload digest in sequence order.
    pub fn replay_payload_digests(&self) -> &[EvidenceDigest] {
        &self.replay_payload_digests
    }
}

/// Exact Direct initial-state material pending one common immutable logical-object claim.
#[derive(Debug)]
pub struct CoinbaseDirectInitialMarketLineage {
    snapshot: SegmentedHttpResponseCapture,
    replay: Vec<CoinbaseDirectReplayFrame>,
    sealing_expectation: CoinbaseDirectSnapshotSealExpectation,
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
        let maximum_digest_slots = replay
            .len()
            .checked_next_power_of_two()
            .ok_or(CoinbaseMarketHandoffError::Allocation)?;
        let mut replay_payload_digests = Vec::new();
        replay_payload_digests
            .try_reserve_exact(replay.len())
            .map_err(|_error| CoinbaseMarketHandoffError::Allocation)?;
        if replay_payload_digests.capacity() > maximum_digest_slots {
            return Err(CoinbaseMarketHandoffError::Allocation);
        }
        replay_payload_digests.extend(
            replay
                .iter()
                .map(|frame| frame.decoder_evidence().payload_digest()),
        );
        let receipt = snapshot.receipt();
        let sealing_expectation = CoinbaseDirectSnapshotSealExpectation {
            snapshot_body_digest: receipt.body_digest(),
            snapshot_body_length: receipt.body_length(),
            snapshot_received_at: receipt.received_at(),
            replay_payload_digests,
        };
        Ok(Self {
            snapshot,
            replay,
            sealing_expectation,
        })
    }

    /// Returns the exact segmented response capture; no receipt-only surrogate is exposed.
    pub const fn snapshot(&self) -> &SegmentedHttpResponseCapture {
        &self.snapshot
    }

    /// Returns every admitted frame strictly after the snapshot cutoff in sequence order.
    pub fn replay(&self) -> &[CoinbaseDirectReplayFrame] {
        &self.replay
    }

    /// Returns the non-cloneable acceptance expectation for common product orchestration.
    pub const fn sealing_expectation(&self) -> &CoinbaseDirectSnapshotSealExpectation {
        &self.sealing_expectation
    }

    /// Consumes the pending lineage into the exact response, ordered replay, and opaque
    /// completion expectation required by the future common publisher.
    pub fn into_sealing_split(
        self,
    ) -> (
        SegmentedHttpResponseCapture,
        Vec<CoinbaseDirectReplayFrame>,
        CoinbaseDirectSnapshotSealExpectation,
    ) {
        (self.snapshot, self.replay, self.sealing_expectation)
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
    pub(crate) configured_instrument: InstrumentId,
    pub(crate) venue: VenueId,
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
    configured_instrument: InstrumentId,
    venue: VenueId,
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
    pub const fn configured_instrument(&self) -> InstrumentId {
        self.configured_instrument
    }

    /// Returns the exact venue identity.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
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
            || typed_batch.observations().iter().any(|observation| {
                observation.venue() != &input.venue
                    || observation.instrument() != input.configured_instrument
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
                configured_instrument: input.configured_instrument,
                venue: input.venue,
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
        || lineage.sealing_expectation.snapshot_body_digest != receipt.body_digest()
        || lineage.sealing_expectation.snapshot_body_length != receipt.body_length()
        || lineage.sealing_expectation.snapshot_received_at != receipt.received_at()
        || lineage.replay.len() != lineage.sealing_expectation.replay_payload_digests.len()
    {
        return Err(CoinbaseMarketHandoffError::EvidenceMismatch);
    }
    let expected_first = snapshot
        .checked_next()
        .map_err(|_error| CoinbaseMarketHandoffError::EvidenceMismatch)?;
    let mut previous = None;
    for (index, frame) in lineage.replay.iter().enumerate() {
        frame
            .decoder_evidence()
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseMarketHandoffError::StaleAuthority)?;
        let expected = previous
            .map_or(Ok(expected_first), SequenceNumber::checked_next)
            .map_err(|_error| CoinbaseMarketHandoffError::EvidenceMismatch)?;
        if frame.sequence() != expected
            || frame.decoder_evidence().payload_digest()
                != lineage.sealing_expectation.replay_payload_digests[index]
        {
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
