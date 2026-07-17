//! Canonical typed event construction and deterministic state encodings.

use market_squawk_domain::{
    AuctionEvent, BookChange, BookDeltaEvent, BookSnapshotEvent, CorporateActionEvent,
    CorporateActionKind, InstrumentDefinition, InstrumentStatusEvent, LiveProvenance, MarketEvent,
    PriceTicks, QuantityLots, QuoteEvent, SourceIdentifier, Timestamp, TradeEvent,
    TradingHaltEvent, TradingStatus,
};
use market_squawk_sources::ProviderObservationPayload;
use sha2::{Digest, Sha256};

use super::LiveApplyError;
use crate::provider_book::{ProviderBook, ProviderBookCandidate};
use crate::qualification::canonical_digest_from_sha256;
use crate::{normalize_positive_quantity, normalize_price};

/// Maximum canonical snapshot-vector bytes while allocator spare capacity is normalized through
/// an owned boxed slice. Both final side vectors and one in-flight conversion allocation coexist.
pub(crate) fn snapshot_canonical_vector_peak_bytes(
    depth: usize,
    maximum_items: usize,
) -> Option<u64> {
    let retained_levels = (depth as u64).checked_mul(2)?.min(maximum_items as u64);
    let conversion_overlap = (depth as u64).min(maximum_items as u64);
    retained_levels
        .checked_add(conversion_overlap)?
        .checked_mul(std::mem::size_of::<market_squawk_domain::BookLevel>() as u64)
}

/// Maximum canonical delta-vector bytes while allocator spare capacity is normalized through an
/// owned boxed slice. The pre-normalization and final allocations may briefly coexist.
pub(crate) fn delta_canonical_vector_peak_bytes(maximum_items: usize) -> Option<u64> {
    (maximum_items as u64)
        .checked_mul(2)?
        .checked_mul(std::mem::size_of::<BookChange>() as u64)
}

#[derive(Clone, Debug)]
pub(super) enum PreparedEvent {
    Trade {
        price: PriceTicks,
        quantity: QuantityLots,
        aggressor: market_squawk_domain::AggressorSide,
    },
    Quote {
        bid: Option<market_squawk_domain::BookLevel>,
        ask: Option<market_squawk_domain::BookLevel>,
    },
    BookSnapshot {
        depth: market_squawk_domain::MarketDepth,
        bids: Vec<market_squawk_domain::BookLevel>,
        asks: Vec<market_squawk_domain::BookLevel>,
        sequence: Option<market_squawk_domain::SequenceNumber>,
    },
    BookDelta {
        depth: market_squawk_domain::MarketDepth,
        changes: Vec<BookChange>,
        sequence: Option<market_squawk_domain::SequenceNumber>,
    },
    Auction {
        phase: market_squawk_domain::AuctionPhase,
        price: Option<PriceTicks>,
        paired_quantity: QuantityLots,
    },
    TradingHalt {
        transition: market_squawk_domain::HaltTransition,
        reason: SourceIdentifier,
    },
    InstrumentStatus {
        status: TradingStatus,
    },
    CorporateAction {
        effective_at: Timestamp,
        kind: CorporateActionKind,
    },
}

impl PreparedEvent {
    pub(super) fn build(
        self,
        provenance: LiveProvenance,
    ) -> Result<MarketEvent, market_squawk_domain::MarketEventError> {
        match self {
            Self::Trade {
                price,
                quantity,
                aggressor,
            } => Ok(MarketEvent::Trade(TradeEvent::new(
                provenance, price, quantity, aggressor,
            )?)),
            Self::Quote { bid, ask } => {
                Ok(MarketEvent::Quote(QuoteEvent::new(provenance, bid, ask)?))
            }
            Self::BookSnapshot {
                depth,
                bids,
                asks,
                sequence,
            } => Ok(MarketEvent::BookSnapshot(BookSnapshotEvent::new(
                provenance, depth, bids, asks, sequence,
            )?)),
            Self::BookDelta {
                depth,
                changes,
                sequence,
            } => Ok(MarketEvent::BookDelta(BookDeltaEvent::new(
                provenance, depth, changes, sequence,
            )?)),
            Self::Auction {
                phase,
                price,
                paired_quantity,
            } => Ok(MarketEvent::Auction(AuctionEvent::new(
                provenance,
                phase,
                price,
                paired_quantity,
            )?)),
            Self::TradingHalt { transition, reason } => Ok(MarketEvent::TradingHalt(
                TradingHaltEvent::new(provenance, transition, reason)?,
            )),
            Self::InstrumentStatus { status } => Ok(MarketEvent::InstrumentStatus(
                InstrumentStatusEvent::new(provenance, status)?,
            )),
            Self::CorporateAction { effective_at, kind } => Ok(MarketEvent::CorporateAction(
                CorporateActionEvent::new(provenance, effective_at, kind)?,
            )),
        }
    }

    pub(super) fn canonical_bytes(&self) -> Result<Vec<u8>, LiveApplyError> {
        let mut output = Vec::new();
        output
            .try_reserve(512)
            .map_err(|_| LiveApplyError::Allocation)?;
        output.extend_from_slice(b"MSQKEVENT\x01");
        match self {
            Self::Trade {
                price,
                quantity,
                aggressor,
            } => {
                output.push(1);
                encode_i64(&mut output, price.get());
                encode_i64(&mut output, quantity.get());
                output.push(match aggressor {
                    market_squawk_domain::AggressorSide::Buy => 1,
                    market_squawk_domain::AggressorSide::Sell => 2,
                    market_squawk_domain::AggressorSide::Unknown => 3,
                });
            }
            Self::Quote { bid, ask } => {
                output.push(2);
                encode_level(&mut output, *bid);
                encode_level(&mut output, *ask);
            }
            Self::Auction {
                phase,
                price,
                paired_quantity,
            } => {
                output.push(5);
                output.push(match phase {
                    market_squawk_domain::AuctionPhase::Opening => 1,
                    market_squawk_domain::AuctionPhase::Closing => 2,
                    market_squawk_domain::AuctionPhase::Volatility => 3,
                    market_squawk_domain::AuctionPhase::Other => 4,
                });
                encode_price(&mut output, *price);
                encode_i64(&mut output, paired_quantity.get());
            }
            Self::TradingHalt { transition, reason } => {
                output.push(6);
                output.push(match transition {
                    market_squawk_domain::HaltTransition::Halted => 1,
                    market_squawk_domain::HaltTransition::Resumed => 2,
                });
                encode_bytes(&mut output, reason.as_str().as_bytes())?;
            }
            Self::InstrumentStatus { status } => {
                output.extend_from_slice(&[7, trading_status_tag(*status)]);
            }
            Self::CorporateAction { effective_at, kind } => {
                output.push(8);
                encode_i64(&mut output, effective_at.unix_nanos());
                encode_action(&mut output, kind)?;
            }
            Self::BookSnapshot { .. } | Self::BookDelta { .. } => {
                return Err(LiveApplyError::PayloadClassMismatch);
            }
        }
        Ok(output)
    }
}

pub(super) fn prepare_non_book(
    payload: &ProviderObservationPayload,
    definition: &InstrumentDefinition,
) -> Result<PreparedEvent, LiveApplyError> {
    match payload {
        ProviderObservationPayload::Trade {
            price,
            quantity,
            aggressor,
            ..
        } => Ok(PreparedEvent::Trade {
            price: normalize_price(price, definition.tick_size())?,
            quantity: normalize_positive_quantity(quantity, definition.lot_size())?,
            aggressor: aggressor.side(),
        }),
        ProviderObservationPayload::Quote { bid, ask } => Ok(PreparedEvent::Quote {
            bid: bid
                .as_ref()
                .map(|value| normalize_level(value, definition))
                .transpose()?,
            ask: ask
                .as_ref()
                .map(|value| normalize_level(value, definition))
                .transpose()?,
        }),
        ProviderObservationPayload::Auction {
            phase,
            price,
            paired_quantity,
            ..
        } => Ok(PreparedEvent::Auction {
            phase: *phase,
            price: price
                .as_ref()
                .map(|value| normalize_price(value, definition.tick_size()))
                .transpose()?,
            paired_quantity: normalize_positive_quantity(paired_quantity, definition.lot_size())?,
        }),
        ProviderObservationPayload::TradingHalt {
            transition, reason, ..
        } => Ok(PreparedEvent::TradingHalt {
            transition: *transition,
            reason: reason.clone(),
        }),
        ProviderObservationPayload::InstrumentStatus {
            trading_status: status,
            ..
        } => Ok(PreparedEvent::InstrumentStatus { status: *status }),
        ProviderObservationPayload::CorporateAction {
            effective_at, kind, ..
        } => Ok(PreparedEvent::CorporateAction {
            effective_at: *effective_at,
            kind: kind.clone(),
        }),
        ProviderObservationPayload::BookSnapshot(_) | ProviderObservationPayload::BookDelta(_) => {
            Err(LiveApplyError::PayloadClassMismatch)
        }
    }
}

pub(super) fn digest_book(
    book: &ProviderBook,
) -> Result<market_squawk_domain::CanonicalStateDigest, LiveApplyError> {
    let mut hasher = Sha256::new();
    hasher.update(b"MSQKBOOK\x01");
    hash_side(&mut hasher, 1, book.scaled_bid_iter())?;
    hash_side(&mut hasher, 2, book.scaled_ask_iter())?;
    Ok(canonical_digest_from_sha256(hasher.finalize().into())?)
}

pub(super) fn digest_candidate_book(
    book: &ProviderBookCandidate<'_>,
) -> Result<market_squawk_domain::CanonicalStateDigest, LiveApplyError> {
    let mut hasher = Sha256::new();
    hasher.update(b"MSQKBOOK\x01");
    hash_side(&mut hasher, 1, book.scaled_bid_iter())?;
    hash_side(&mut hasher, 2, book.scaled_ask_iter())?;
    Ok(canonical_digest_from_sha256(hasher.finalize().into())?)
}

fn normalize_level(
    level: &market_squawk_sources::ProviderBookLevel,
    definition: &InstrumentDefinition,
) -> Result<market_squawk_domain::BookLevel, LiveApplyError> {
    Ok(market_squawk_domain::BookLevel::new(
        normalize_price(level.price(), definition.tick_size())?,
        normalize_positive_quantity(level.quantity(), definition.lot_size())?,
    )?)
}

fn hash_side(
    hasher: &mut Sha256,
    tag: u8,
    levels: impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)>,
) -> Result<(), LiveApplyError> {
    hasher.update([tag]);
    hasher.update(
        u32::try_from(levels.len())
            .map_err(|_| LiveApplyError::Allocation)?
            .to_be_bytes(),
    );
    for (price, quantity) in levels {
        hasher.update(price.get().to_be_bytes());
        hasher.update(quantity.get().to_be_bytes());
    }
    Ok(())
}

fn encode_level(output: &mut Vec<u8>, level: Option<market_squawk_domain::BookLevel>) {
    match level {
        Some(value) => {
            output.push(1);
            encode_i64(output, value.price().get());
            encode_i64(output, value.quantity().get());
        }
        None => output.push(0),
    }
}
fn encode_price(output: &mut Vec<u8>, price: Option<PriceTicks>) {
    match price {
        Some(value) => {
            output.push(1);
            encode_i64(output, value.get());
        }
        None => output.push(0),
    }
}
fn encode_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_be_bytes());
}
fn encode_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}
fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), LiveApplyError> {
    encode_u32(
        output,
        u32::try_from(bytes.len()).map_err(|_| LiveApplyError::Allocation)?,
    );
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_action(output: &mut Vec<u8>, action: &CorporateActionKind) -> Result<(), LiveApplyError> {
    match action {
        CorporateActionKind::Split {
            numerator,
            denominator,
        } => {
            output.push(1);
            encode_u32(output, numerator.get());
            encode_u32(output, denominator.get());
        }
        CorporateActionKind::CashDividend { amount } => {
            output.push(2);
            encode_bytes(output, amount.amount().to_string().as_bytes())?;
            output.extend_from_slice(amount.currency().as_str().as_bytes());
        }
        CorporateActionKind::Merger { successor } => {
            output.push(3);
            output.extend_from_slice(successor.as_uuid().as_bytes());
        }
        CorporateActionKind::Delisting => output.push(4),
        CorporateActionKind::SymbolChange {
            venue_id,
            previous,
            current,
        } => {
            output.push(5);
            encode_bytes(output, venue_id.as_str().as_bytes())?;
            encode_bytes(output, previous.as_str().as_bytes())?;
            encode_bytes(output, current.as_str().as_bytes())?;
        }
    }
    Ok(())
}

const fn trading_status_tag(status: TradingStatus) -> u8 {
    match status {
        TradingStatus::Active => 1,
        TradingStatus::Halted => 2,
        TradingStatus::Inactive => 3,
        TradingStatus::Delisted => 4,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use market_squawk_domain::{CorporateActionKind, LotSize, TickSize, Timestamp};
    use market_squawk_sources::{
        ProviderBookLevel, ProviderDecimalLexeme, ProviderPrice, ProviderQuantity,
    };
    use rust_decimal::Decimal;
    use sha2::{Digest, Sha256};

    use super::{PreparedEvent, digest_book};
    use crate::{
        DepthLimit,
        provider_book::{BookProcessingScratch, ProviderBook},
    };

    fn level(price: &str, quantity: &str) -> Result<ProviderBookLevel, Box<dyn std::error::Error>> {
        Ok(ProviderBookLevel::new(
            ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
            ProviderQuantity::new(ProviderDecimalLexeme::try_new(quantity)?),
        ))
    }

    #[test]
    fn streaming_book_digest_matches_v1_canonical_bytes() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut book = ProviderBook::try_new(DepthLimit::new(2)?)?;
        let mut scratch = BookProcessingScratch::try_new(4)?;
        book.replace_snapshot(
            &[level("100", "2")?, level("99", "3")?],
            &[level("101", "4")?, level("102", "5")?],
            TickSize::try_from_decimal(Decimal::ONE)?,
            LotSize::try_from_decimal(Decimal::ONE)?,
            None,
            &mut scratch,
        )?;
        let mut legacy = Vec::from(&b"MSQKBOOK\x01"[..]);
        legacy.push(1);
        legacy.extend_from_slice(&2_u32.to_be_bytes());
        for (price, quantity) in [(100_i64, 2_i64), (99, 3)] {
            legacy.extend_from_slice(&price.to_be_bytes());
            legacy.extend_from_slice(&quantity.to_be_bytes());
        }
        legacy.push(2);
        legacy.extend_from_slice(&2_u32.to_be_bytes());
        for (price, quantity) in [(101_i64, 4_i64), (102, 5)] {
            legacy.extend_from_slice(&price.to_be_bytes());
            legacy.extend_from_slice(&quantity.to_be_bytes());
        }
        let expected: [u8; 32] = Sha256::digest(&legacy).into();
        assert_eq!(digest_book(&book)?.digest().bytes(), expected);
        Ok(())
    }

    #[test]
    fn corporate_action_canonical_bytes_bind_effective_time_and_typed_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let action = |at, numerator, denominator| -> Result<_, Box<dyn std::error::Error>> {
            Ok(PreparedEvent::CorporateAction {
                effective_at: Timestamp::from_unix_nanos(at),
                kind: CorporateActionKind::Split {
                    numerator: NonZeroU32::new(numerator).ok_or("zero numerator")?,
                    denominator: NonZeroU32::new(denominator).ok_or("zero denominator")?,
                },
            })
        };
        let baseline = action(10, 2, 1)?.canonical_bytes()?;
        assert_ne!(baseline, action(11, 2, 1)?.canonical_bytes()?);
        assert_ne!(baseline, action(10, 3, 1)?.canonical_bytes()?);
        assert_ne!(baseline, action(10, 2, 3)?.canonical_bytes()?);
        Ok(())
    }
}
