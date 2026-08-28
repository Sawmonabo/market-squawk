//! Direct-local canonical projection for authenticated Coinbase Exchange replay.

use std::time::{SystemTime, UNIX_EPOCH};

use market_squawk_adapter_coinbase::{
    CoinbaseDirectConfig, CoinbaseDirectReplayFrame, CoinbaseExchangeDirectSnapshotRow,
    CoinbaseMarketPublicationError,
};
use market_squawk_domain::{
    AggressorSide, BookChange, BookDeltaEvent, BookLevel, BookSnapshotEvent, BookStateBinding,
    CanonicalStateDigest, CanonicalizationRule, ConnectionGeneration, CoverageStatus, DataQuality,
    DecodedLiveProvenanceInput, DigestAlgorithm, EvidenceDigest, LiveEventClass,
    LiveEvidenceBinding, LiveProvenance, MarketDepth, MarketEvent, MarketSide, PayloadHash,
    PayloadReference, PriceTicks, QuantityLots, RuleVersion, SequenceNumber, SourceIdentifier,
    Timestamp, TradeEvent,
};
use market_squawk_live::{
    BookSide, DepthLimit, LevelUpdate, ScaledBook, normalize_delta_quantity,
    normalize_positive_quantity, normalize_price,
};
use market_squawk_sources::{
    DecoderEvidence, ProviderBookChange, ProviderBookLevel, ProviderBookSide,
    ProviderEventMicrobatchFrameReceipt, ProviderNormalizedObservation, ProviderObservationPayload,
    ProviderSequenceEvidence, ProviderTimestampEvidence, SegmentedHttpResponseReceipt,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const CANONICALIZATION_RULE: &str = "market-squawk-live-state-v2";
const CANONICALIZATION_VERSION: u32 = 2;

/// Bounded canonical price-level state for one Direct product owner.
///
/// Projection is transactional: a complete candidate book, provenance graph, and event vector are
/// built before any session, sequence, snapshot-origin, or price-level state is committed.
#[derive(Clone, Debug)]
pub(in crate::live_source) struct CoinbaseDirectCanonicalState {
    book: ScaledBook,
    session_id: Option<SourceIdentifier>,
    connection_generation: Option<ConnectionGeneration>,
    snapshot_origin: Option<SnapshotOrigin>,
    terminal_sequence: Option<SequenceNumber>,
}

impl CoinbaseDirectCanonicalState {
    /// Creates empty bounded state using the adapter's already-admitted publication depth.
    pub(in crate::live_source) fn try_new(
        config: &CoinbaseDirectConfig,
    ) -> Result<Self, CoinbaseDirectCanonicalError> {
        let depth = DepthLimit::new(config.limits().book().published_depth())
            .map_err(|_| CoinbaseDirectCanonicalError::State)?;
        Ok(Self {
            book: ScaledBook::new(depth),
            session_id: None,
            connection_generation: None,
            snapshot_origin: None,
            terminal_sequence: None,
        })
    }

    /// Projects exact native matches followed by the terminal price-level snapshot or delta.
    ///
    /// `replay` and `frames` must describe the same complete ordered raw microbatch. Cursor-only
    /// Direct order events remain in that raw/native lineage and do not become canonical quotes.
    pub(in crate::live_source) fn try_project_replay(
        &mut self,
        config: &CoinbaseDirectConfig,
        replay: &[CoinbaseDirectReplayFrame],
        terminal: &ProviderNormalizedObservation,
        frames: &[ProviderEventMicrobatchFrameReceipt],
    ) -> Result<Vec<MarketEvent>, CoinbaseDirectCanonicalError> {
        validate_profile(config, terminal)?;
        if replay.is_empty() || replay.len() != frames.len() {
            return Err(CoinbaseDirectCanonicalError::FrameEvidence);
        }

        let mut candidate = self.clone();
        let decoder = replay
            .last()
            .map(CoinbaseDirectReplayFrame::decoder_evidence)
            .ok_or(CoinbaseDirectCanonicalError::FrameEvidence)?;
        let terminal_sequence = provided_sequence(terminal)?;
        let terminal_timestamp = provided_timestamp(terminal)?;
        if terminal_sequence != replay[replay.len() - 1].sequence()
            || terminal_timestamp != replay[replay.len() - 1].event().timestamp()
        {
            return Err(CoinbaseDirectCanonicalError::FrameEvidence);
        }
        validate_replay_frames(replay, frames)?;

        let session_id = decoder
            .binding()
            .session_id()
            .as_source_identifier()
            .clone();
        let connection_generation = decoder.binding().connection_generation();
        let initial = matches!(
            terminal.payload(),
            ProviderObservationPayload::BookSnapshot(_)
        );
        if initial {
            candidate.session_id = Some(session_id.clone());
            candidate.connection_generation = Some(connection_generation);
            candidate.snapshot_origin = None;
            candidate.terminal_sequence = None;
        } else {
            candidate.validate_successor_generation(&session_id, connection_generation, replay)?;
        }

        let trade_count = replay
            .iter()
            .filter(|frame| frame.native_trade().is_some())
            .count();
        let mut events = Vec::new();
        events
            .try_reserve_exact(
                trade_count
                    .checked_add(1)
                    .ok_or(CoinbaseDirectCanonicalError::Allocation)?,
            )
            .map_err(|_| CoinbaseDirectCanonicalError::Allocation)?;
        for (replay_frame, frame) in replay.iter().zip(frames) {
            if let Some(trade) = replay_frame.native_trade() {
                events.push(build_trade(
                    config,
                    replay_frame.decoder_evidence(),
                    frame,
                    trade,
                )?);
            }
        }

        let terminal_frame = frames
            .last()
            .ok_or(CoinbaseDirectCanonicalError::FrameEvidence)?;
        let terminal_event = candidate.apply_terminal_book(
            config,
            terminal,
            decoder,
            terminal_frame,
            terminal_sequence,
        )?;
        events.push(terminal_event);
        candidate.terminal_sequence = Some(terminal_sequence);
        *self = candidate;
        Ok(events)
    }

    fn validate_successor_generation(
        &self,
        session_id: &SourceIdentifier,
        connection_generation: ConnectionGeneration,
        replay: &[CoinbaseDirectReplayFrame],
    ) -> Result<(), CoinbaseDirectCanonicalError> {
        if self.session_id.as_ref() != Some(session_id)
            || self.connection_generation != Some(connection_generation)
            || self.snapshot_origin.is_none()
        {
            return Err(CoinbaseDirectCanonicalError::SnapshotRequired);
        }
        let previous = self
            .terminal_sequence
            .ok_or(CoinbaseDirectCanonicalError::SnapshotRequired)?;
        let mut previous = previous;
        for frame in replay {
            let expected = previous
                .checked_next()
                .map_err(|_| CoinbaseDirectCanonicalError::Sequence)?;
            if frame.sequence() != expected {
                return Err(CoinbaseDirectCanonicalError::Sequence);
            }
            previous = frame.sequence();
        }
        Ok(())
    }

    fn apply_terminal_book(
        &mut self,
        config: &CoinbaseDirectConfig,
        provider: &ProviderNormalizedObservation,
        decoder: &DecoderEvidence,
        frame: &ProviderEventMicrobatchFrameReceipt,
        sequence: SequenceNumber,
    ) -> Result<MarketEvent, CoinbaseDirectCanonicalError> {
        match provider.payload() {
            ProviderObservationPayload::BookSnapshot(snapshot) => {
                let bids = snapshot_updates(snapshot.bids(), BookSide::Bid, config)?;
                let asks = snapshot_updates(snapshot.asks(), BookSide::Ask, config)?;
                self.book
                    .replace_snapshot(&bids, &asks)
                    .map_err(|_| CoinbaseDirectCanonicalError::State)?;
                let canonical = digest_book(&self.book)?;
                let origin = SnapshotOrigin {
                    state_id: provider.source_identifier().clone(),
                    digest: canonical.clone(),
                };
                let book_state = BookStateBinding::new(
                    MarketDepth::PriceLevel,
                    origin.state_id.clone(),
                    canonical.clone(),
                );
                let provenance = build_frame_provenance(
                    config,
                    decoder,
                    frame,
                    provider,
                    canonical,
                    Some(book_state),
                )?;
                let (book_bids, book_asks) = canonical_book_levels(&self.book)?;
                let event = BookSnapshotEvent::new(
                    provenance,
                    MarketDepth::PriceLevel,
                    book_bids,
                    book_asks,
                    Some(sequence),
                )
                .map_err(|_| CoinbaseDirectCanonicalError::MarketEvent)?;
                self.snapshot_origin = Some(origin);
                Ok(MarketEvent::BookSnapshot(event))
            }
            ProviderObservationPayload::BookDelta(delta) => {
                let origin = self
                    .snapshot_origin
                    .as_ref()
                    .ok_or(CoinbaseDirectCanonicalError::SnapshotRequired)?
                    .clone();
                let updates = delta_updates(delta.changes(), config)?;
                self.book
                    .apply_delta(&updates)
                    .map_err(|_| CoinbaseDirectCanonicalError::State)?;
                let canonical = digest_book(&self.book)?;
                let book_state = BookStateBinding::new_with_snapshot_origin(
                    MarketDepth::PriceLevel,
                    provider.source_identifier().clone(),
                    canonical.clone(),
                    origin.state_id,
                    origin.digest,
                );
                let provenance = build_frame_provenance(
                    config,
                    decoder,
                    frame,
                    provider,
                    canonical,
                    Some(book_state),
                )?;
                let changes = canonical_changes(&updates)?;
                Ok(MarketEvent::BookDelta(
                    BookDeltaEvent::new(
                        provenance,
                        MarketDepth::PriceLevel,
                        changes,
                        Some(sequence),
                    )
                    .map_err(|_| CoinbaseDirectCanonicalError::MarketEvent)?,
                ))
            }
            ProviderObservationPayload::Trade { .. }
            | ProviderObservationPayload::Quote { .. }
            | ProviderObservationPayload::Auction { .. }
            | ProviderObservationPayload::TradingHalt { .. }
            | ProviderObservationPayload::InstrumentStatus { .. }
            | ProviderObservationPayload::CorporateAction { .. } => {
                Err(CoinbaseDirectCanonicalError::Profile)
            }
        }
    }
}

/// Builds the separately sealed pre-replay REST snapshot while the adapter lends its exact row.
pub(super) fn try_build_initial_snapshot(
    config: &CoinbaseDirectConfig,
    row: CoinbaseExchangeDirectSnapshotRow<'_>,
) -> Result<MarketEvent, CoinbaseMarketPublicationError> {
    build_initial_snapshot(config, row).map_err(|_| CoinbaseMarketPublicationError::EventMismatch)
}

fn build_initial_snapshot(
    config: &CoinbaseDirectConfig,
    row: CoinbaseExchangeDirectSnapshotRow<'_>,
) -> Result<MarketEvent, CoinbaseDirectCanonicalError> {
    let provider = row.provider_observation();
    validate_profile(config, provider)?;
    let ProviderObservationPayload::BookSnapshot(snapshot) = provider.payload() else {
        return Err(CoinbaseDirectCanonicalError::Profile);
    };
    let sequence = provided_sequence(provider)?;
    let receipt = row.response_receipt();
    let depth = DepthLimit::new(config.limits().book().published_depth())
        .map_err(|_| CoinbaseDirectCanonicalError::State)?;
    let mut book = ScaledBook::new(depth);
    let bids = snapshot_updates(snapshot.bids(), BookSide::Bid, config)?;
    let asks = snapshot_updates(snapshot.asks(), BookSide::Ask, config)?;
    book.replace_snapshot(&bids, &asks)
        .map_err(|_| CoinbaseDirectCanonicalError::State)?;
    let canonical = digest_book(&book)?;
    let book_state = BookStateBinding::new(
        MarketDepth::PriceLevel,
        provider.source_identifier().clone(),
        canonical.clone(),
    );
    let provenance =
        build_response_provenance(config, receipt, provider, canonical, Some(book_state))?;
    let (book_bids, book_asks) = canonical_book_levels(&book)?;
    Ok(MarketEvent::BookSnapshot(
        BookSnapshotEvent::new(
            provenance,
            MarketDepth::PriceLevel,
            book_bids,
            book_asks,
            Some(sequence),
        )
        .map_err(|_| CoinbaseDirectCanonicalError::MarketEvent)?,
    ))
}

fn build_trade(
    config: &CoinbaseDirectConfig,
    decoder: &DecoderEvidence,
    frame: &ProviderEventMicrobatchFrameReceipt,
    trade: &market_squawk_adapter_coinbase::CoinbaseDirectTradeEvidence,
) -> Result<MarketEvent, CoinbaseDirectCanonicalError> {
    if frame.payload_digest() != decoder.payload_digest()
        || frame.received_at() != decoder.received_at()
        || frame.source_sequence() != Some(trade.sequence().get())
        || frame.exchange_at() != Some(trade.provider_timestamp())
    {
        return Err(CoinbaseDirectCanonicalError::FrameEvidence);
    }
    let aggressor = match trade.maker_side() {
        ProviderBookSide::Bid => AggressorSide::Sell,
        ProviderBookSide::Ask => AggressorSide::Buy,
    };
    let canonical = digest_trade(trade.price(), trade.quantity(), aggressor)?;
    let source_identifier = SourceIdentifier::try_from(trade.trade_id().to_string())
        .map_err(|_| CoinbaseDirectCanonicalError::Identity)?;
    let provenance = build_provenance(
        config,
        decoder
            .binding()
            .session_id()
            .as_source_identifier()
            .clone(),
        decoder.binding().connection_generation(),
        LiveEventClass::Trade,
        source_identifier,
        decoder.payload_digest(),
        canonical,
        None,
        Some(trade.provider_timestamp()),
        decoder.received_at(),
    )?;
    Ok(MarketEvent::Trade(
        TradeEvent::new(provenance, trade.price(), trade.quantity(), aggressor, None)
            .map_err(|_| CoinbaseDirectCanonicalError::MarketEvent)?,
    ))
}

fn build_frame_provenance(
    config: &CoinbaseDirectConfig,
    decoder: &DecoderEvidence,
    frame: &ProviderEventMicrobatchFrameReceipt,
    provider: &ProviderNormalizedObservation,
    canonical: CanonicalStateDigest,
    book_state: Option<BookStateBinding>,
) -> Result<LiveProvenance, CoinbaseDirectCanonicalError> {
    if frame.payload_digest() != decoder.payload_digest()
        || frame.received_at() != decoder.received_at()
    {
        return Err(CoinbaseDirectCanonicalError::FrameEvidence);
    }
    build_provenance(
        config,
        decoder
            .binding()
            .session_id()
            .as_source_identifier()
            .clone(),
        decoder.binding().connection_generation(),
        provider.event_class(),
        provider.source_identifier().clone(),
        frame.payload_digest(),
        canonical,
        book_state,
        Some(provided_timestamp(provider)?),
        frame.received_at(),
    )
}

fn build_response_provenance(
    config: &CoinbaseDirectConfig,
    receipt: &SegmentedHttpResponseReceipt,
    provider: &ProviderNormalizedObservation,
    canonical: CanonicalStateDigest,
    book_state: Option<BookStateBinding>,
) -> Result<LiveProvenance, CoinbaseDirectCanonicalError> {
    build_provenance(
        config,
        receipt.session_id().as_source_identifier().clone(),
        receipt.connection_generation(),
        provider.event_class(),
        provider.source_identifier().clone(),
        receipt.body_digest(),
        canonical,
        book_state,
        Some(provided_timestamp(provider)?),
        receipt.received_at(),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the complete anti-transplant provenance binding remains explicit"
)]
fn build_provenance(
    config: &CoinbaseDirectConfig,
    session_id: SourceIdentifier,
    connection_generation: ConnectionGeneration,
    event_class: LiveEventClass,
    source_identifier: SourceIdentifier,
    payload_digest: EvidenceDigest,
    canonical: CanonicalStateDigest,
    book_state: Option<BookStateBinding>,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
) -> Result<LiveProvenance, CoinbaseDirectCanonicalError> {
    let live = config
        .metadata()
        .coverage()
        .live()
        .ok_or(CoinbaseDirectCanonicalError::Profile)?;
    let binding = LiveEvidenceBinding::new(
        config.metadata().source_id().clone(),
        session_id,
        config.metadata().revision().clone(),
        config.metadata().authorization().basis().clone(),
        config.venue().clone(),
        config.instrument(),
        connection_generation,
        live.provider_product().clone(),
        live.provider_channel().clone(),
        event_class,
        source_identifier,
        payload_digest,
        canonical,
        book_state,
    )
    .map_err(|_| CoinbaseDirectCanonicalError::Binding)?;
    let canonicalized_at = timestamp_at_least(received_at)?;
    LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding,
        source_timestamp,
        received_at,
        canonicalized_at,
        canonicalized_at,
        DataQuality::DirectUnverified,
        CoverageStatus::Unknown,
        PayloadReference::ContentHash(PayloadHash::new(
            payload_digest.algorithm(),
            payload_digest.bytes(),
        )),
    ))
    .map_err(|_| CoinbaseDirectCanonicalError::Provenance)
}

fn validate_profile(
    config: &CoinbaseDirectConfig,
    provider: &ProviderNormalizedObservation,
) -> Result<(), CoinbaseDirectCanonicalError> {
    if provider.venue() != config.venue()
        || provider.instrument() != config.instrument()
        || provider.depth() != Some(MarketDepth::PriceLevel)
        || !matches!(
            provider.event_class(),
            LiveEventClass::BookSnapshot | LiveEventClass::BookDelta
        )
    {
        return Err(CoinbaseDirectCanonicalError::Profile);
    }
    Ok(())
}

fn validate_replay_frames(
    replay: &[CoinbaseDirectReplayFrame],
    frames: &[ProviderEventMicrobatchFrameReceipt],
) -> Result<(), CoinbaseDirectCanonicalError> {
    let connection_id = frames
        .first()
        .map(ProviderEventMicrobatchFrameReceipt::connection_id)
        .ok_or(CoinbaseDirectCanonicalError::FrameEvidence)?;
    let first_decoder = replay
        .first()
        .map(CoinbaseDirectReplayFrame::decoder_evidence)
        .ok_or(CoinbaseDirectCanonicalError::FrameEvidence)?;
    let mut previous: Option<SequenceNumber> = None;
    for (ordinal, (replay_frame, frame)) in replay.iter().zip(frames).enumerate() {
        let decoder = replay_frame.decoder_evidence();
        if usize::from(frame.ordinal()) != ordinal
            || frame.connection_id() != connection_id
            || frame.source_sequence() != Some(replay_frame.sequence().get())
            || frame.exchange_at() != Some(replay_frame.event().timestamp())
            || frame.received_at() != decoder.received_at()
            || frame.payload_bytes() != u64::try_from(decoder.frame_bytes()).unwrap_or(u64::MAX)
            || frame.payload_digest() != decoder.payload_digest()
            || !decoder
                .binding()
                .shares_allocation_with(first_decoder.binding())
        {
            return Err(CoinbaseDirectCanonicalError::FrameEvidence);
        }
        if let Some(previous) = previous {
            let expected = previous
                .checked_next()
                .map_err(|_| CoinbaseDirectCanonicalError::Sequence)?;
            if replay_frame.sequence() != expected {
                return Err(CoinbaseDirectCanonicalError::Sequence);
            }
        }
        previous = Some(replay_frame.sequence());
    }
    Ok(())
}

fn snapshot_updates(
    levels: &[ProviderBookLevel],
    side: BookSide,
    config: &CoinbaseDirectConfig,
) -> Result<Vec<LevelUpdate>, CoinbaseDirectCanonicalError> {
    let terms = config.execution_terms();
    let mut updates = Vec::new();
    updates
        .try_reserve_exact(levels.len())
        .map_err(|_| CoinbaseDirectCanonicalError::Allocation)?;
    for level in levels {
        updates.push(LevelUpdate::new(
            side,
            normalize_price(level.price(), terms.price_tick())
                .map_err(|_| CoinbaseDirectCanonicalError::Numeric)?,
            normalize_positive_quantity(level.quantity(), terms.lot_size())
                .map_err(|_| CoinbaseDirectCanonicalError::Numeric)?,
        ));
    }
    Ok(updates)
}

fn delta_updates(
    changes: &[ProviderBookChange],
    config: &CoinbaseDirectConfig,
) -> Result<Vec<LevelUpdate>, CoinbaseDirectCanonicalError> {
    let terms = config.execution_terms();
    let mut updates = Vec::new();
    updates
        .try_reserve_exact(changes.len())
        .map_err(|_| CoinbaseDirectCanonicalError::Allocation)?;
    for change in changes {
        let side = match change.side() {
            ProviderBookSide::Bid => BookSide::Bid,
            ProviderBookSide::Ask => BookSide::Ask,
        };
        updates.push(LevelUpdate::new(
            side,
            normalize_price(change.level().price(), terms.price_tick())
                .map_err(|_| CoinbaseDirectCanonicalError::Numeric)?,
            normalize_delta_quantity(change.level().quantity(), terms.lot_size())
                .map_err(|_| CoinbaseDirectCanonicalError::Numeric)?,
        ));
    }
    Ok(updates)
}

fn canonical_changes(
    updates: &[LevelUpdate],
) -> Result<Vec<BookChange>, CoinbaseDirectCanonicalError> {
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(updates.len())
        .map_err(|_| CoinbaseDirectCanonicalError::Allocation)?;
    for update in updates {
        canonical.push(BookChange::new(
            match update.side() {
                BookSide::Bid => MarketSide::Bid,
                BookSide::Ask => MarketSide::Ask,
            },
            update.price(),
            update.quantity(),
        ));
    }
    Ok(canonical)
}

fn canonical_book_levels(
    book: &ScaledBook,
) -> Result<(Vec<BookLevel>, Vec<BookLevel>), CoinbaseDirectCanonicalError> {
    let bids = book.bid_levels();
    let asks = book.ask_levels();
    let mut canonical_bids = Vec::new();
    canonical_bids
        .try_reserve_exact(bids.len())
        .map_err(|_| CoinbaseDirectCanonicalError::Allocation)?;
    for (price, quantity) in bids {
        canonical_bids.push(
            BookLevel::new(price, quantity)
                .map_err(|_| CoinbaseDirectCanonicalError::MarketEvent)?,
        );
    }
    let mut canonical_asks = Vec::new();
    canonical_asks
        .try_reserve_exact(asks.len())
        .map_err(|_| CoinbaseDirectCanonicalError::Allocation)?;
    for (price, quantity) in asks {
        canonical_asks.push(
            BookLevel::new(price, quantity)
                .map_err(|_| CoinbaseDirectCanonicalError::MarketEvent)?,
        );
    }
    Ok((canonical_bids, canonical_asks))
}

fn digest_book(book: &ScaledBook) -> Result<CanonicalStateDigest, CoinbaseDirectCanonicalError> {
    let bids = book.bid_levels();
    let asks = book.ask_levels();
    let mut hasher = Sha256::new();
    hasher.update(b"MSQKBOOK\x01");
    hash_side(&mut hasher, 1, &bids)?;
    hash_side(&mut hasher, 2, &asks)?;
    canonical_digest_from_sha256(hasher.finalize().into())
}

fn digest_trade(
    price: PriceTicks,
    quantity: QuantityLots,
    aggressor: AggressorSide,
) -> Result<CanonicalStateDigest, CoinbaseDirectCanonicalError> {
    let mut hasher = Sha256::new();
    hasher.update(b"MSQKEVENT\x02");
    hasher.update([1]);
    hasher.update(price.get().to_be_bytes());
    hasher.update(quantity.get().to_be_bytes());
    hasher.update([match aggressor {
        AggressorSide::Buy => 1,
        AggressorSide::Sell => 2,
        AggressorSide::Unknown => 3,
    }]);
    hasher.update([0]);
    canonical_digest_from_sha256(hasher.finalize().into())
}

fn hash_side(
    hasher: &mut Sha256,
    tag: u8,
    levels: &[(PriceTicks, QuantityLots)],
) -> Result<(), CoinbaseDirectCanonicalError> {
    hasher.update([tag]);
    hasher.update(
        u32::try_from(levels.len())
            .map_err(|_| CoinbaseDirectCanonicalError::Allocation)?
            .to_be_bytes(),
    );
    for (price, quantity) in levels {
        hasher.update(price.get().to_be_bytes());
        hasher.update(quantity.get().to_be_bytes());
    }
    Ok(())
}

fn canonical_digest_from_sha256(
    digest: [u8; 32],
) -> Result<CanonicalStateDigest, CoinbaseDirectCanonicalError> {
    Ok(CanonicalStateDigest::new(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
        CanonicalizationRule::new(
            SourceIdentifier::try_from(CANONICALIZATION_RULE)
                .map_err(|_| CoinbaseDirectCanonicalError::Identity)?,
            RuleVersion::new(CANONICALIZATION_VERSION)
                .map_err(|_| CoinbaseDirectCanonicalError::Identity)?,
        ),
    ))
}

fn provided_sequence(
    provider: &ProviderNormalizedObservation,
) -> Result<SequenceNumber, CoinbaseDirectCanonicalError> {
    match provider.sequence() {
        ProviderSequenceEvidence::Provided { value, .. } => Ok(*value),
        ProviderSequenceEvidence::Unsupported { .. } => Err(CoinbaseDirectCanonicalError::Profile),
    }
}

fn provided_timestamp(
    provider: &ProviderNormalizedObservation,
) -> Result<Timestamp, CoinbaseDirectCanonicalError> {
    match provider.timestamp() {
        ProviderTimestampEvidence::Provided { value, .. } => Ok(*value),
        ProviderTimestampEvidence::AuthoritativelyAbsent(_) => {
            Err(CoinbaseDirectCanonicalError::Profile)
        }
    }
}

fn timestamp_at_least(received_at: Timestamp) -> Result<Timestamp, CoinbaseDirectCanonicalError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CoinbaseDirectCanonicalError::Clock)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_| CoinbaseDirectCanonicalError::Clock)?;
    Ok(received_at.max(Timestamp::from_unix_nanos(nanos)))
}

#[derive(Clone, Debug)]
struct SnapshotOrigin {
    state_id: SourceIdentifier,
    digest: CanonicalStateDigest,
}

/// Fail-closed Direct-local canonical projection failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(in crate::live_source) enum CoinbaseDirectCanonicalError {
    #[error("Coinbase Direct canonical input profile is inconsistent")]
    Profile,
    #[error("Coinbase Direct canonical raw-frame evidence is inconsistent")]
    FrameEvidence,
    #[error("Coinbase Direct canonical sequence is not contiguous")]
    Sequence,
    #[error("Coinbase Direct canonical successor requires an initialized snapshot")]
    SnapshotRequired,
    #[error("Coinbase Direct canonical price-level state is invalid")]
    State,
    #[error("Coinbase Direct canonical numeric conversion is inexact")]
    Numeric,
    #[error("Coinbase Direct canonical allocation failed")]
    Allocation,
    #[error("Coinbase Direct canonical identity is invalid")]
    Identity,
    #[error("Coinbase Direct canonical evidence binding is invalid")]
    Binding,
    #[error("Coinbase Direct canonical provenance is invalid")]
    Provenance,
    #[error("Coinbase Direct canonical market event is invalid")]
    MarketEvent,
    #[error("Coinbase Direct canonical clock is outside the supported range")]
    Clock,
}
