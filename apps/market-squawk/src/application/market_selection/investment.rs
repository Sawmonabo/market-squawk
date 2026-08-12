//! Typed, authority-free market evidence for one selected investment instrument.

use std::fmt;

use market_squawk_domain::{
    AggressorSide, CaptureIntegrityState, ConnectionGeneration, CoverageStatus, Currency,
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentDefinition, InstrumentExecutionTerms,
    InstrumentId, MarketDataInstrumentDefinition, MarketDepth, PriceTicks, QuantityLots,
    SourceIdentifier, Timestamp, TradingStatus,
};
use market_squawk_live::{
    LastTradeSnapshot, LiveFeatureSetSnapshot, LiveFeatureSnapshot, OrderLevelPhase,
    OrderLevelPriceProjection, PriceLevelProjection, SnapshotCompleteness, StreamPhaseSnapshot,
    StreamSnapshot,
};
use market_squawk_sources::MarketFreshness;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use super::{
    CandidateTimestamps, IntegrityState, MarketCoverage, MarketSelectionReceipt,
    SelectedMarketSource,
};
use crate::application::market_runtime::{
    MarketDisplaySnapshotLease, MarketKrakenPriceProjectionLease,
};
use crate::live_source::display_market::{
    DisplayEffectiveTimeBasis, DisplayMarketAvailability, DisplayMarketPayload,
    DisplayMarketReadObservation, DisplayQuoteSide, DisplayTrade,
};

const MARK_EVIDENCE_DIGEST_DOMAIN: &[u8] = b"market-squawk/market-investment-mark-evidence/v1";

/// A caller supplied evidence that does not exactly match the selected receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketInvestmentReadError {
    ExecutionOperationForbidden,
    SelectedSourceMismatch,
    InvalidFinancialTerms,
    AmbiguousFeatureEvidence,
    EvidenceIdentityEncoding,
}

impl fmt::Display for MarketInvestmentReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionOperationForbidden => {
                formatter.write_str("investment evidence cannot carry execution authority")
            }
            Self::SelectedSourceMismatch => formatter
                .write_str("market evidence does not match the exact selected source generation"),
            Self::InvalidFinancialTerms => {
                formatter.write_str("market mark cannot be represented by the admitted terms")
            }
            Self::AmbiguousFeatureEvidence => {
                formatter.write_str("more than one feature set matches the exact source generation")
            }
            Self::EvidenceIdentityEncoding => {
                formatter.write_str("market mark evidence cannot be represented canonically")
            }
        }
    }
}

impl std::error::Error for MarketInvestmentReadError {}

/// Truthful reason one selected source cannot currently produce an investment mark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketInvestmentUnavailableReason {
    NoEligibleSource,
    NoFreshLastTradeOrMidpoint,
}

/// Why feature evidence is absent without borrowing evidence from another source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketFeatureUnavailableReason {
    SourceDoesNotPublishLiveFeatures,
    IncompleteSnapshot,
    NoExactSourceGeneration,
    AvailableAfterSelection,
    IncompleteValueSet,
}

/// Exact live feature evidence, or a typed reason it is unavailable.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MarketFeatureEvidence<'source> {
    Available(&'source LiveFeatureSetSnapshot),
    Unavailable(MarketFeatureUnavailableReason),
}

/// Code-owned mark choice. Neither variant is an order, target, or recommendation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketInvestmentMarkBasis {
    FreshLastTrade,
    FreshBidAskMidpoint,
}

/// Borrowed exact evidence from which the selected mark was computed.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MarketInvestmentMarkEvidence<'source> {
    LiveTrade(&'source LastTradeSnapshot),
    LiveBook(&'source StreamSnapshot),
    DisplayTrade(&'source DisplayMarketReadObservation),
    DisplayQuote(&'source DisplayMarketReadObservation),
    KrakenPriceProjection(&'source OrderLevelPriceProjection),
}

/// Exact decimal mark and currency backed by one retained source observation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarketInvestmentMark<'source> {
    value: Decimal,
    currency: Currency,
    basis: MarketInvestmentMarkBasis,
    evidence_identity: EvidenceDigest,
    fresh_until: Option<Timestamp>,
    evidence: MarketInvestmentMarkEvidence<'source>,
}

impl<'source> MarketInvestmentMark<'source> {
    pub(crate) const fn value(self) -> Decimal {
        self.value
    }

    pub(crate) const fn currency(self) -> Currency {
        self.currency
    }

    pub(crate) const fn basis(self) -> MarketInvestmentMarkBasis {
        self.basis
    }

    /// Returns the versioned identity of the exact mark, source selection, and retained evidence.
    pub(crate) const fn evidence_identity(self) -> EvidenceDigest {
        self.evidence_identity
    }

    /// Returns the inclusive source-specific freshness deadline when one is retained.
    /// Deadline-requiring consumers must reject `None` rather than infer a value.
    pub(crate) const fn fresh_until(self) -> Option<Timestamp> {
        self.fresh_until
    }

    pub(crate) const fn evidence(self) -> MarketInvestmentMarkEvidence<'source> {
        self.evidence
    }
}

/// One native live stream plus the immutable publication facts needed to prevent transplantation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveMarketInvestmentSource<'source> {
    surface_id: &'source SourceIdentifier,
    provider: &'source SourceIdentifier,
    stream: &'source StreamSnapshot,
    features: &'source LiveFeatureSnapshot,
    definition: &'source InstrumentDefinition,
    published_at: Timestamp,
}

impl<'source> LiveMarketInvestmentSource<'source> {
    pub(crate) const fn new(
        surface_id: &'source SourceIdentifier,
        provider: &'source SourceIdentifier,
        stream: &'source StreamSnapshot,
        features: &'source LiveFeatureSnapshot,
        definition: &'source InstrumentDefinition,
        published_at: Timestamp,
    ) -> Self {
        Self {
            surface_id,
            provider,
            stream,
            features,
            definition,
            published_at,
        }
    }
}

/// The exact retained source selected by the existing unified resolver.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SelectedMarketInvestmentSource<'source> {
    Live(LiveMarketInvestmentSource<'source>),
    Display {
        snapshot: &'source MarketDisplaySnapshotLease,
        definition: &'source MarketDataInstrumentDefinition,
    },
    Kraken(&'source MarketKrakenPriceProjectionLease),
}

/// Typed non-executable observation for an analysis compositor.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarketInvestmentObservation<'receipt, 'source> {
    selected: SelectedMarketSource<'receipt>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    mark: MarketInvestmentMark<'source>,
    features: MarketFeatureEvidence<'source>,
}

impl<'receipt, 'source> MarketInvestmentObservation<'receipt, 'source> {
    pub(crate) const fn instrument_id(self) -> InstrumentId {
        self.selected.candidate().identity().instrument_id()
    }

    pub(crate) const fn selected_source(self) -> SelectedMarketSource<'receipt> {
        self.selected
    }

    pub(crate) const fn selection_digest(self) -> EvidenceDigest {
        self.selection_digest
    }

    pub(crate) const fn selected_at(self) -> Timestamp {
        self.selected_at
    }

    pub(crate) const fn freshness_age_nanos(self) -> u64 {
        self.selected.freshness_age_nanos()
    }

    pub(crate) const fn generation(self) -> Option<ConnectionGeneration> {
        self.selected
            .candidate()
            .admission()
            .integrity()
            .generation()
    }

    pub(crate) const fn mark(self) -> MarketInvestmentMark<'source> {
        self.mark
    }

    pub(crate) const fn timestamps(self) -> CandidateTimestamps {
        self.selected.candidate().timestamps()
    }

    pub(crate) const fn quality(self) -> DataQuality {
        self.selected.candidate().capabilities().quality()
    }

    pub(crate) const fn depth(self) -> Option<MarketDepth> {
        self.selected.candidate().capabilities().depth()
    }

    pub(crate) const fn coverage(self) -> MarketCoverage {
        self.selected.candidate().capabilities().coverage()
    }

    pub(crate) const fn integrity(self) -> IntegrityState {
        self.selected.candidate().admission().integrity().state()
    }

    pub(crate) const fn features(self) -> MarketFeatureEvidence<'source> {
        self.features
    }
}

/// Complete single-instrument result without partial or fabricated market evidence.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MarketInvestmentRead<'receipt, 'source> {
    Available(MarketInvestmentObservation<'receipt, 'source>),
    Unavailable(MarketInvestmentUnavailableReason),
}

/// Verifies that an immutable selected receipt names the exact retained connection generation.
pub(crate) fn selected_generation_matches(
    selected: SelectedMarketSource<'_>,
    actual: ConnectionGeneration,
) -> bool {
    selected.candidate().admission().integrity().generation() == Some(actual)
}

/// Builds a source-preserving observation without JSON, execution authority, sizing, or orders.
pub(crate) fn read_market_investment_observation<'receipt, 'source>(
    receipt: &'receipt MarketSelectionReceipt,
    source: Option<SelectedMarketInvestmentSource<'source>>,
) -> Result<MarketInvestmentRead<'receipt, 'source>, MarketInvestmentReadError> {
    if receipt.request().operation().requires_execution_quality() {
        return Err(MarketInvestmentReadError::ExecutionOperationForbidden);
    }
    let (selected, source) = match (receipt.selected(), source) {
        (None, None) => {
            return Ok(MarketInvestmentRead::Unavailable(
                MarketInvestmentUnavailableReason::NoEligibleSource,
            ));
        }
        (Some(selected), Some(source)) => (selected, source),
        (None, Some(_)) | (Some(_), None) => {
            return Err(MarketInvestmentReadError::SelectedSourceMismatch);
        }
    };
    let selected_at = receipt.selected_at();
    let selection_digest = receipt.selection_digest();
    let (mark, features) = match source {
        SelectedMarketInvestmentSource::Live(source) => {
            live_evidence(selected, selection_digest, source, selected_at)?
        }
        SelectedMarketInvestmentSource::Display {
            snapshot,
            definition,
        } => display_evidence(
            selected,
            selection_digest,
            snapshot,
            definition,
            selected_at,
        )?,
        SelectedMarketInvestmentSource::Kraken(snapshot) => {
            kraken_evidence(selected, selection_digest, snapshot, selected_at)?
        }
    };
    let mark = if selected.freshness_age_nanos()
        <= receipt.request().freshness().maximum_age_nanos()
        && !matches!(
            selected.candidate().capabilities().quality(),
            DataQuality::Stale | DataQuality::Quarantined
        ) {
        mark
    } else {
        None
    };
    let Some(mark) = mark else {
        return Ok(MarketInvestmentRead::Unavailable(
            MarketInvestmentUnavailableReason::NoFreshLastTradeOrMidpoint,
        ));
    };
    Ok(MarketInvestmentRead::Available(
        MarketInvestmentObservation {
            selected,
            selection_digest: receipt.selection_digest(),
            selected_at,
            mark,
            features,
        },
    ))
}

fn live_evidence<'source>(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    source: LiveMarketInvestmentSource<'source>,
    selected_at: Timestamp,
) -> Result<
    (
        Option<MarketInvestmentMark<'source>>,
        MarketFeatureEvidence<'source>,
    ),
    MarketInvestmentReadError,
> {
    let identity = selected.candidate().identity();
    let stream = source.stream;
    let timestamps = selected.candidate().timestamps();
    if source.provider != identity.provider()
        || source.surface_id != identity.observation_id()
        || stream.source() != identity.source_id()
        || Some(stream.venue()) != identity.venue_id()
        || stream.instrument() != identity.instrument_id()
        || stream.provider_product() != identity.product()
        || stream.provider_channel() != identity.feed()
        || !selected_generation_matches(selected, stream.connection_generation())
        || source.definition.instrument_id() != identity.instrument_id()
        || timestamps.source_timestamp() != stream.source_timestamp()
        || timestamps.effective_at() != stream.source_timestamp().unwrap_or(stream.received_at())
        || timestamps.received_at() != stream.received_at()
        || timestamps.available_at() != stream.evaluated_at()
        || timestamps.ingested_at() != source.published_at
    {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }

    let execution_terms = source.definition.execution_terms();
    let currency = execution_terms.quote_currency();
    let mark = if let Some(trade) = stream.last_trade().filter(|trade| {
        stream.generation_current()
            && stream.phase() == StreamPhaseSnapshot::Healthy
            && selected_at <= stream.source_valid_until()
            && trade.connection_generation() == stream.connection_generation()
            && trade
                .source_timestamp()
                .is_none_or(|source_timestamp| source_timestamp <= selected_at)
            && trade.received_at() <= trade.available_at()
            && trade.available_at() <= trade.ingested_at()
            && trade.ingested_at() <= selected_at
            && trade.qualification_evaluated_at() <= selected_at
            && selected_at <= trade.qualification_valid_until()
            && !matches!(
                trade.recorded_quality(),
                DataQuality::Stale | DataQuality::Quarantined
            )
    }) {
        let value = trade
            .price()
            .checked_to_decimal(execution_terms.price_tick())
            .map_err(|_error| MarketInvestmentReadError::InvalidFinancialTerms)?
            .normalize();
        let fresh_until = stream
            .source_valid_until()
            .min(trade.qualification_valid_until());
        Some(MarketInvestmentMark {
            value,
            currency,
            basis: MarketInvestmentMarkBasis::FreshLastTrade,
            evidence_identity: live_trade_mark_evidence_identity(
                selected,
                selection_digest,
                selected_at,
                value,
                currency,
                fresh_until,
                stream,
                trade,
                execution_terms,
            )?,
            fresh_until: Some(fresh_until),
            evidence: MarketInvestmentMarkEvidence::LiveTrade(trade),
        })
    } else if stream.generation_current()
        && stream.phase() == StreamPhaseSnapshot::Healthy
        && selected_at <= stream.source_valid_until()
    {
        match (stream.bids().first(), stream.asks().first()) {
            (Some(bid), Some(ask)) => {
                midpoint(Some(bid.price()), Some(ask.price()), execution_terms)?
                    .map(|value| {
                        let fresh_until = stream.source_valid_until();
                        Ok(MarketInvestmentMark {
                            value,
                            currency,
                            basis: MarketInvestmentMarkBasis::FreshBidAskMidpoint,
                            evidence_identity: live_book_mark_evidence_identity(
                                selected,
                                selection_digest,
                                selected_at,
                                value,
                                currency,
                                fresh_until,
                                stream,
                                bid.price(),
                                bid.quantity(),
                                ask.price(),
                                ask.quantity(),
                                execution_terms,
                            )?,
                            fresh_until: Some(fresh_until),
                            evidence: MarketInvestmentMarkEvidence::LiveBook(stream),
                        })
                    })
                    .transpose()?
            }
            _ => None,
        }
    } else {
        None
    };
    let features = exact_live_features(selected, stream, source.features, selected_at)?;
    Ok((mark, features))
}

fn display_evidence<'source>(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    snapshot: &'source MarketDisplaySnapshotLease,
    definition: &'source MarketDataInstrumentDefinition,
    selected_at: Timestamp,
) -> Result<
    (
        Option<MarketInvestmentMark<'source>>,
        MarketFeatureEvidence<'source>,
    ),
    MarketInvestmentReadError,
> {
    let identity = selected.candidate().identity();
    let actor = snapshot.lease();
    let key = actor.key();
    if snapshot.metadata().provider() != identity.provider()
        || snapshot.surface_id() != identity.observation_id()
        || key.source_id() != identity.source_id()
        || Some(key.venue_id()) != identity.venue_id()
        || key.instrument_id() != identity.instrument_id()
        || definition.instrument_id() != identity.instrument_id()
        || definition.effective_interval().starts_at() > selected_at
        || definition
            .effective_interval()
            .ends_at()
            .is_some_and(|ends_at| selected_at >= ends_at)
        || !selected_generation_matches(selected, key.generation())
    {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }

    let selected_observation = actor.selection_observation().filter(|observation| {
        matches!(
            observation.availability(),
            DisplayMarketAvailability::Fresh { .. }
        ) && display_provenance_matches(observation, selected, selected_at)
    });
    let mark = if let Some(observation) = selected_observation {
        let (stale_after, expires_after) = match observation.availability() {
            DisplayMarketAvailability::Fresh {
                stale_after,
                expires_after,
            } => (stale_after, expires_after),
            DisplayMarketAvailability::Stale { .. }
            | DisplayMarketAvailability::Expired { .. }
            | DisplayMarketAvailability::Quarantined { .. } => {
                return Err(MarketInvestmentReadError::SelectedSourceMismatch);
            }
        };
        if selected_at > stale_after || stale_after > expires_after {
            return Err(MarketInvestmentReadError::SelectedSourceMismatch);
        }
        match observation.observation().payload() {
            DisplayMarketPayload::Trade(trade) => {
                let value = trade.price().value().normalize();
                let currency = definition.quote_currency();
                Some(MarketInvestmentMark {
                    value,
                    currency,
                    basis: MarketInvestmentMarkBasis::FreshLastTrade,
                    evidence_identity: display_trade_mark_evidence_identity(
                        selected,
                        selection_digest,
                        selected_at,
                        value,
                        currency,
                        stale_after,
                        snapshot,
                        definition,
                        observation,
                        trade,
                    )?,
                    fresh_until: Some(stale_after),
                    evidence: MarketInvestmentMarkEvidence::DisplayTrade(observation),
                })
            }
            DisplayMarketPayload::Quote(quote) => match quote.bid().zip(quote.ask()) {
                Some((bid, ask)) => {
                    let value = checked_midpoint(bid.price().value(), ask.price().value())?;
                    let currency = definition.quote_currency();
                    Some(MarketInvestmentMark {
                        value,
                        currency,
                        basis: MarketInvestmentMarkBasis::FreshBidAskMidpoint,
                        evidence_identity: display_quote_mark_evidence_identity(
                            selected,
                            selection_digest,
                            selected_at,
                            value,
                            currency,
                            stale_after,
                            snapshot,
                            definition,
                            observation,
                            bid,
                            ask,
                        )?,
                        fresh_until: Some(stale_after),
                        evidence: MarketInvestmentMarkEvidence::DisplayQuote(observation),
                    })
                }
                None => None,
            },
            DisplayMarketPayload::Status(_) => None,
        }
    } else {
        None
    };
    Ok((
        mark,
        MarketFeatureEvidence::Unavailable(
            MarketFeatureUnavailableReason::SourceDoesNotPublishLiveFeatures,
        ),
    ))
}

fn kraken_evidence<'source>(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    snapshot: &'source MarketKrakenPriceProjectionLease,
    selected_at: Timestamp,
) -> Result<
    (
        Option<MarketInvestmentMark<'source>>,
        MarketFeatureEvidence<'source>,
    ),
    MarketInvestmentReadError,
> {
    let identity = selected.candidate().identity();
    let key = snapshot.key();
    let projection = snapshot.projection();
    let live = snapshot
        .metadata()
        .coverage()
        .live()
        .ok_or(MarketInvestmentReadError::SelectedSourceMismatch)?;
    let terms = snapshot.execution_terms();
    if snapshot.metadata().provider() != identity.provider()
        || snapshot.surface_id() != identity.observation_id()
        || live.provider_product() != identity.product()
        || live.provider_channel() != identity.feed()
        || key.source_id() != identity.source_id()
        || Some(key.venue_id()) != identity.venue_id()
        || key.instrument_id() != identity.instrument_id()
        || !selected_generation_matches(selected, key.generation())
        || projection.route().generation() != key.generation()
        || terms.instrument_id() != key.instrument_id()
        || projection.received_at() > projection.available_at()
        || projection.available_at() > selected_at
    {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }
    let mark = if projection.phase() == OrderLevelPhase::Healthy
        && matches!(projection.freshness(), MarketFreshness::Fresh { .. })
    {
        match (projection.bids().first(), projection.asks().first()) {
            (Some(bid), Some(ask)) => midpoint(Some(bid.price()), Some(ask.price()), terms)?
                .map(|value| {
                    let currency = terms.quote_currency();
                    Ok(MarketInvestmentMark {
                        value,
                        currency,
                        basis: MarketInvestmentMarkBasis::FreshBidAskMidpoint,
                        evidence_identity: kraken_mark_evidence_identity(
                            selected,
                            selection_digest,
                            selected_at,
                            value,
                            currency,
                            snapshot,
                            projection,
                            *bid,
                            *ask,
                            terms,
                        )?,
                        fresh_until: None,
                        evidence: MarketInvestmentMarkEvidence::KrakenPriceProjection(projection),
                    })
                })
                .transpose()?,
            _ => None,
        }
    } else {
        None
    };
    Ok((
        mark,
        MarketFeatureEvidence::Unavailable(
            MarketFeatureUnavailableReason::SourceDoesNotPublishLiveFeatures,
        ),
    ))
}

fn display_provenance_matches(
    observation: &DisplayMarketReadObservation,
    selected: SelectedMarketSource<'_>,
    selected_at: Timestamp,
) -> bool {
    let identity = selected.candidate().identity();
    let provenance = observation.observation().provenance();
    let timestamps = selected.candidate().timestamps();
    provenance.coverage().provider_product() == identity.product().as_source_identifier()
        && provenance.coverage().provider_channel() == identity.feed().as_source_identifier()
        && Some(provenance.generation())
            == selected.candidate().admission().integrity().generation()
        && provenance.received_at() <= provenance.available_at()
        && provenance.available_at() <= selected_at
        && provenance.effective_at() <= selected_at
        && timestamps.effective_at() == provenance.effective_at()
        && timestamps.source_timestamp() == provenance.source_at()
        && timestamps.received_at() == provenance.received_at()
        && timestamps.available_at() == provenance.available_at()
        && timestamps.ingested_at() == provenance.available_at()
}

fn exact_live_features<'source>(
    selected: SelectedMarketSource<'_>,
    stream: &StreamSnapshot,
    features: &'source LiveFeatureSnapshot,
    selected_at: Timestamp,
) -> Result<MarketFeatureEvidence<'source>, MarketInvestmentReadError> {
    if features.set_dimension().completeness() != SnapshotCompleteness::Complete {
        return Ok(MarketFeatureEvidence::Unavailable(
            MarketFeatureUnavailableReason::IncompleteSnapshot,
        ));
    }
    let mut matches = features.sets().iter().filter(|set| {
        set.source() == stream.source()
            && set.venue() == stream.venue()
            && set.instrument() == stream.instrument()
            && set.provider_product() == stream.provider_product()
            && set.provider_channel() == stream.provider_channel()
            && set.connection_generation() == stream.connection_generation()
            && selected_generation_matches(selected, set.connection_generation())
    });
    let Some(features) = matches.next() else {
        return Ok(MarketFeatureEvidence::Unavailable(
            MarketFeatureUnavailableReason::NoExactSourceGeneration,
        ));
    };
    if matches.next().is_some() {
        return Err(MarketInvestmentReadError::AmbiguousFeatureEvidence);
    }
    if features.available_at() > selected_at {
        return Ok(MarketFeatureEvidence::Unavailable(
            MarketFeatureUnavailableReason::AvailableAfterSelection,
        ));
    }
    if features.value_dimension().completeness() != SnapshotCompleteness::Complete {
        return Ok(MarketFeatureEvidence::Unavailable(
            MarketFeatureUnavailableReason::IncompleteValueSet,
        ));
    }
    Ok(MarketFeatureEvidence::Available(features))
}

fn midpoint(
    bid: Option<market_squawk_domain::PriceTicks>,
    ask: Option<market_squawk_domain::PriceTicks>,
    terms: InstrumentExecutionTerms,
) -> Result<Option<Decimal>, MarketInvestmentReadError> {
    bid.zip(ask)
        .map(|(bid, ask)| {
            let bid = bid
                .checked_to_decimal(terms.price_tick())
                .map_err(|_error| MarketInvestmentReadError::InvalidFinancialTerms)?;
            let ask = ask
                .checked_to_decimal(terms.price_tick())
                .map_err(|_error| MarketInvestmentReadError::InvalidFinancialTerms)?;
            checked_midpoint(bid, ask)
        })
        .transpose()
}

fn checked_midpoint(bid: Decimal, ask: Decimal) -> Result<Decimal, MarketInvestmentReadError> {
    if bid > ask {
        return Err(MarketInvestmentReadError::InvalidFinancialTerms);
    }
    bid.checked_add(ask)
        .and_then(|sum| sum.checked_div(Decimal::from(2_u8)))
        .map(|midpoint| midpoint.normalize())
        .ok_or(MarketInvestmentReadError::InvalidFinancialTerms)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds each independent mark fact"
)]
fn live_trade_mark_evidence_identity(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    value: Decimal,
    currency: Currency,
    fresh_until: Timestamp,
    stream: &StreamSnapshot,
    trade: &LastTradeSnapshot,
    terms: InstrumentExecutionTerms,
) -> Result<EvidenceDigest, MarketInvestmentReadError> {
    let mut digest = mark_evidence_prefix(
        selected,
        selection_digest,
        selected_at,
        value,
        currency,
        MarketInvestmentMarkBasis::FreshLastTrade,
        Some(fresh_until),
    )?;
    hash_tag(&mut digest, 1)?;
    hash_live_stream(&mut digest, stream, terms)?;
    hash_text(&mut digest, trade.source_identifier().as_str())?;
    hash_text(&mut digest, trade.stable_trade_id().as_str())?;
    hash_u64(&mut digest, trade.connection_generation().get())?;
    hash_i64(&mut digest, trade.price().get())?;
    hash_i64(&mut digest, trade.quantity().get())?;
    hash_tag(&mut digest, aggressor_side_tag(trade.aggressor_side()))?;
    hash_optional_timestamp(&mut digest, trade.source_timestamp())?;
    hash_timestamp(&mut digest, trade.received_at())?;
    hash_timestamp(&mut digest, trade.available_at())?;
    hash_timestamp(&mut digest, trade.ingested_at())?;
    hash_tag(&mut digest, data_quality_tag(trade.recorded_quality()))?;
    hash_tag(&mut digest, coverage_status_tag(trade.recorded_coverage()))?;
    hash_text(&mut digest, trade.assessment_id().as_str())?;
    hash_timestamp(&mut digest, trade.qualification_evaluated_at())?;
    hash_timestamp(&mut digest, trade.qualification_valid_until())?;
    hash_evidence_digest(&mut digest, trade.payload_digest())?;
    hash_bytes(&mut digest, &trade.binding_digest())?;
    hash_tag(&mut digest, trading_status_tag(trade.trading_status()))?;
    hash_u64(&mut digest, trade.committed_state_revision())?;
    Ok(finish_mark_evidence(digest))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds both exact midpoint sides"
)]
fn live_book_mark_evidence_identity(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    value: Decimal,
    currency: Currency,
    fresh_until: Timestamp,
    stream: &StreamSnapshot,
    bid_price: PriceTicks,
    bid_quantity: QuantityLots,
    ask_price: PriceTicks,
    ask_quantity: QuantityLots,
    terms: InstrumentExecutionTerms,
) -> Result<EvidenceDigest, MarketInvestmentReadError> {
    let mut digest = mark_evidence_prefix(
        selected,
        selection_digest,
        selected_at,
        value,
        currency,
        MarketInvestmentMarkBasis::FreshBidAskMidpoint,
        Some(fresh_until),
    )?;
    hash_tag(&mut digest, 2)?;
    hash_live_stream(&mut digest, stream, terms)?;
    hash_i64(&mut digest, bid_price.get())?;
    hash_i64(&mut digest, bid_quantity.get())?;
    hash_i64(&mut digest, ask_price.get())?;
    hash_i64(&mut digest, ask_quantity.get())?;
    hash_decimal(
        &mut digest,
        bid_price
            .checked_to_decimal(terms.price_tick())
            .map_err(|_error| MarketInvestmentReadError::InvalidFinancialTerms)?,
    )?;
    hash_decimal(
        &mut digest,
        ask_price
            .checked_to_decimal(terms.price_tick())
            .map_err(|_error| MarketInvestmentReadError::InvalidFinancialTerms)?,
    )?;
    Ok(finish_mark_evidence(digest))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds each independent mark fact"
)]
fn display_trade_mark_evidence_identity(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    value: Decimal,
    currency: Currency,
    fresh_until: Timestamp,
    snapshot: &MarketDisplaySnapshotLease,
    definition: &MarketDataInstrumentDefinition,
    observation: &DisplayMarketReadObservation,
    trade: &DisplayTrade,
) -> Result<EvidenceDigest, MarketInvestmentReadError> {
    let mut digest = mark_evidence_prefix(
        selected,
        selection_digest,
        selected_at,
        value,
        currency,
        MarketInvestmentMarkBasis::FreshLastTrade,
        Some(fresh_until),
    )?;
    hash_tag(&mut digest, 3)?;
    hash_display_common(&mut digest, snapshot, definition, observation)?;
    hash_display_decimal(&mut digest, trade.price())?;
    hash_display_decimal(&mut digest, trade.quantity())?;
    Ok(finish_mark_evidence(digest))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds both exact midpoint sides"
)]
fn display_quote_mark_evidence_identity(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    value: Decimal,
    currency: Currency,
    fresh_until: Timestamp,
    snapshot: &MarketDisplaySnapshotLease,
    definition: &MarketDataInstrumentDefinition,
    observation: &DisplayMarketReadObservation,
    bid: &DisplayQuoteSide,
    ask: &DisplayQuoteSide,
) -> Result<EvidenceDigest, MarketInvestmentReadError> {
    let mut digest = mark_evidence_prefix(
        selected,
        selection_digest,
        selected_at,
        value,
        currency,
        MarketInvestmentMarkBasis::FreshBidAskMidpoint,
        Some(fresh_until),
    )?;
    hash_tag(&mut digest, 4)?;
    hash_display_common(&mut digest, snapshot, definition, observation)?;
    hash_display_decimal(&mut digest, bid.price())?;
    hash_display_decimal(&mut digest, bid.quantity())?;
    hash_display_decimal(&mut digest, ask.price())?;
    hash_display_decimal(&mut digest, ask.quantity())?;
    Ok(finish_mark_evidence(digest))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds both exact midpoint sides"
)]
fn kraken_mark_evidence_identity(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    value: Decimal,
    currency: Currency,
    snapshot: &MarketKrakenPriceProjectionLease,
    projection: &OrderLevelPriceProjection,
    bid: PriceLevelProjection,
    ask: PriceLevelProjection,
    terms: InstrumentExecutionTerms,
) -> Result<EvidenceDigest, MarketInvestmentReadError> {
    let mut digest = mark_evidence_prefix(
        selected,
        selection_digest,
        selected_at,
        value,
        currency,
        MarketInvestmentMarkBasis::FreshBidAskMidpoint,
        None,
    )?;
    hash_tag(&mut digest, 5)?;
    hash_source_metadata(&mut digest, snapshot.metadata())?;
    hash_text(&mut digest, snapshot.provider_symbol().as_str())?;
    hash_text(&mut digest, snapshot.surface_id().as_str())?;
    let key = snapshot.key();
    hash_text(&mut digest, key.source_id().as_str())?;
    hash_text(&mut digest, key.venue_id().as_str())?;
    hash_bytes(&mut digest, key.instrument_id().as_uuid().as_bytes())?;
    hash_u64(&mut digest, key.generation().get())?;
    let route = projection.route();
    hash_text(&mut digest, route.provider_instrument().as_str())?;
    hash_text(&mut digest, projection.batch_identifier().as_str())?;
    hash_u64(&mut digest, projection.revision())?;
    hash_tag(&mut digest, data_quality_tag(projection.quality()))?;
    match projection.freshness() {
        MarketFreshness::Uninitialized => hash_tag(&mut digest, 0)?,
        MarketFreshness::Fresh { last_market_at } => {
            hash_tag(&mut digest, 1)?;
            hash_timestamp(&mut digest, last_market_at)?;
        }
        MarketFreshness::Stale { last_market_at } => {
            hash_tag(&mut digest, 2)?;
            hash_timestamp(&mut digest, last_market_at)?;
        }
    }
    hash_timestamp(&mut digest, projection.source_timestamp())?;
    hash_timestamp(&mut digest, projection.received_at())?;
    hash_timestamp(&mut digest, projection.available_at())?;
    hash_optional_u64(
        &mut digest,
        projection
            .provider_sequence()
            .map(|sequence| sequence.get()),
    )?;
    hash_optional_u64(&mut digest, projection.diagnostic_ordinal())?;
    hash_execution_terms(&mut digest, terms)?;
    hash_price_level(&mut digest, bid, terms)?;
    hash_price_level(&mut digest, ask, terms)?;
    Ok(finish_mark_evidence(digest))
}

fn mark_evidence_prefix(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    value: Decimal,
    currency: Currency,
    basis: MarketInvestmentMarkBasis,
    fresh_until: Option<Timestamp>,
) -> Result<Sha256, MarketInvestmentReadError> {
    let mut digest = Sha256::new();
    hash_bytes(&mut digest, MARK_EVIDENCE_DIGEST_DOMAIN)?;
    hash_evidence_digest(&mut digest, selection_digest)?;
    let identity = selected.candidate().identity();
    hash_text(&mut digest, identity.provider().as_str())?;
    hash_text(
        &mut digest,
        identity.product().as_source_identifier().as_str(),
    )?;
    hash_text(&mut digest, identity.feed().as_source_identifier().as_str())?;
    hash_text(&mut digest, identity.source_id().as_str())?;
    match identity.venue_id() {
        Some(venue) => {
            hash_tag(&mut digest, 1)?;
            hash_text(&mut digest, venue.as_str())?;
        }
        None => hash_tag(&mut digest, 0)?,
    }
    hash_bytes(&mut digest, identity.instrument_id().as_uuid().as_bytes())?;
    hash_text(&mut digest, identity.observation_id().as_str())?;
    let integrity = selected.candidate().admission().integrity();
    let generation = integrity
        .generation()
        .ok_or(MarketInvestmentReadError::SelectedSourceMismatch)?;
    hash_u64(&mut digest, generation.get())?;
    hash_tag(&mut digest, integrity_state_tag(integrity.state()))?;
    hash_timestamp(&mut digest, integrity.assessed_at())?;
    let timestamps = selected.candidate().timestamps();
    hash_timestamp(&mut digest, timestamps.effective_at())?;
    hash_optional_timestamp(&mut digest, timestamps.source_timestamp())?;
    hash_timestamp(&mut digest, timestamps.received_at())?;
    hash_timestamp(&mut digest, timestamps.available_at())?;
    hash_timestamp(&mut digest, timestamps.ingested_at())?;
    hash_tag(
        &mut digest,
        data_quality_tag(selected.candidate().capabilities().quality()),
    )?;
    hash_optional_depth(&mut digest, selected.candidate().capabilities().depth())?;
    hash_tag(
        &mut digest,
        market_coverage_tag(selected.candidate().capabilities().coverage()),
    )?;
    hash_decimal(&mut digest, value)?;
    hash_text(&mut digest, currency.as_str())?;
    hash_tag(&mut digest, mark_basis_tag(basis))?;
    hash_timestamp(&mut digest, selected_at)?;
    hash_optional_timestamp(&mut digest, fresh_until)?;
    Ok(digest)
}

fn hash_live_stream(
    digest: &mut Sha256,
    stream: &StreamSnapshot,
    terms: InstrumentExecutionTerms,
) -> Result<(), MarketInvestmentReadError> {
    hash_text(digest, stream.source().as_str())?;
    hash_text(digest, stream.venue().as_str())?;
    hash_bytes(digest, stream.instrument().as_uuid().as_bytes())?;
    hash_text(
        digest,
        stream.provider_product().as_source_identifier().as_str(),
    )?;
    hash_text(
        digest,
        stream.provider_channel().as_source_identifier().as_str(),
    )?;
    hash_u64(digest, stream.connection_generation().get())?;
    hash_u64(digest, stream.state_revision())?;
    hash_optional_u64(
        digest,
        stream.last_sequence().map(|sequence| sequence.get()),
    )?;
    hash_optional_u64(digest, stream.snapshot_origin_revision())?;
    hash_bool(digest, stream.snapshot_initialized())?;
    hash_bool(digest, stream.generation_current())?;
    hash_u64(digest, stream.health_epoch())?;
    hash_timestamp(digest, stream.source_valid_until())?;
    hash_optional_timestamp(digest, stream.source_timestamp())?;
    hash_timestamp(digest, stream.received_at())?;
    hash_timestamp(digest, stream.evaluated_at())?;
    hash_tag(digest, data_quality_tag(stream.quality()))?;
    hash_execution_terms(digest, terms)?;
    match stream.runtime_evidence() {
        Some(runtime) => {
            if !runtime.matches_stream(stream) {
                return Err(MarketInvestmentReadError::SelectedSourceMismatch);
            }
            hash_tag(digest, 1)?;
            hash_text(digest, runtime.session_id().as_str())?;
            hash_bytes(digest, runtime.instrument_id().as_uuid().as_bytes())?;
            hash_u64(digest, runtime.connection_generation().get())?;
            hash_u64(digest, runtime.health_epoch())?;
            hash_u64(digest, runtime.state_revision())?;
            hash_text(
                digest,
                runtime.assessment_id().as_source_identifier().as_str(),
            )?;
            hash_bytes(digest, &runtime.binding_digest())?;
            hash_tag(digest, capture_integrity_tag(runtime.capture_integrity()))?;
            hash_tag(digest, coverage_status_tag(runtime.coverage_status()))?;
            hash_tag(digest, data_quality_tag(runtime.quality()))?;
            hash_timestamp(digest, runtime.health_observed_at())?;
            hash_timestamp(digest, runtime.qualification_evaluated_at())?;
            hash_timestamp(digest, runtime.qualification_valid_until())?;
        }
        None => hash_tag(digest, 0)?,
    }
    hash_optional_trading_status(digest, stream.trading_status())?;
    hash_optional_u64(digest, stream.trading_status_revision())?;
    hash_u32(digest, stream.configured_depth())?;
    hash_count(digest, stream.state_bid_depth())?;
    hash_count(digest, stream.state_ask_depth())
}

fn hash_display_common(
    digest: &mut Sha256,
    snapshot: &MarketDisplaySnapshotLease,
    definition: &MarketDataInstrumentDefinition,
    observation: &DisplayMarketReadObservation,
) -> Result<(), MarketInvestmentReadError> {
    hash_source_metadata(digest, snapshot.metadata())?;
    hash_text(digest, snapshot.provider_symbol().as_str())?;
    hash_text(digest, snapshot.surface_id().as_str())?;
    let actor = snapshot.lease();
    let key = actor.key();
    hash_text(digest, key.source_id().as_str())?;
    hash_text(digest, key.venue_id().as_str())?;
    hash_bytes(digest, key.instrument_id().as_uuid().as_bytes())?;
    hash_u64(digest, key.generation().get())?;
    hash_u64(digest, actor.revision())?;
    hash_bytes(digest, definition.instrument_id().as_uuid().as_bytes())?;
    hash_text(
        digest,
        definition
            .reference_revision()
            .as_source_identifier()
            .as_str(),
    )?;
    hash_evidence_digest(
        digest,
        definition.reference_payload_evidence().content_digest(),
    )?;
    hash_evidence_digest(
        digest,
        definition.quote_currency_evidence().content_digest(),
    )?;

    let provenance = observation.observation().provenance();
    match observation.availability() {
        DisplayMarketAvailability::Fresh {
            stale_after,
            expires_after,
        } => {
            hash_tag(digest, 1)?;
            hash_timestamp(digest, stale_after)?;
            hash_timestamp(digest, expires_after)?;
        }
        DisplayMarketAvailability::Stale { .. }
        | DisplayMarketAvailability::Expired { .. }
        | DisplayMarketAvailability::Quarantined { .. } => {
            return Err(MarketInvestmentReadError::SelectedSourceMismatch);
        }
    }
    hash_text(digest, provenance.source_identifier().as_str())?;
    hash_optional_timestamp(digest, provenance.source_at())?;
    hash_timestamp(digest, provenance.effective_at())?;
    hash_tag(
        digest,
        display_time_basis_tag(provenance.effective_time_basis()),
    )?;
    hash_timestamp(digest, provenance.received_at())?;
    hash_timestamp(digest, provenance.available_at())?;
    hash_text(
        digest,
        provenance
            .metadata_revision()
            .as_source_identifier()
            .as_str(),
    )?;
    hash_tag(digest, data_quality_tag(provenance.quality()))?;
    hash_optional_depth(digest, provenance.display_depth())?;
    hash_u64(digest, provenance.generation().get())?;
    hash_text(digest, provenance.session_id().as_str())?;
    hash_u64(digest, provenance.frame_id().get())?;
    hash_evidence_digest(digest, provenance.payload_digest())?;
    hash_tag(
        digest,
        capture_integrity_tag(provenance.capture_integrity()),
    )?;
    hash_text(digest, provenance.decoder_rule().as_str())?;
    hash_u32(digest, provenance.decoder_rule_version().get())?;
    hash_text(digest, provenance.timestamp_rule().as_str())?;
    hash_u32(digest, provenance.timestamp_rule_version().get())?;
    let coverage = provenance.coverage();
    hash_text(digest, coverage.provider_product().as_str())?;
    hash_text(digest, coverage.provider_channel().as_str())?;
    hash_optional_depth(digest, coverage.declared_depth())?;
    hash_tag(digest, coverage_status_tag(coverage.status()))?;
    hash_evidence_digest(digest, coverage.static_evidence_digest())?;
    hash_optional_evidence_digest(digest, coverage.runtime_evidence_digest())?;
    hash_timestamp(digest, coverage.effective_from())?;
    hash_optional_timestamp(digest, coverage.effective_until())
}

fn hash_source_metadata(
    digest: &mut Sha256,
    metadata: &market_squawk_sources::SourceMetadata,
) -> Result<(), MarketInvestmentReadError> {
    hash_text(digest, metadata.source_id().as_str())?;
    hash_text(digest, metadata.provider().as_str())?;
    hash_text(digest, metadata.revision().as_source_identifier().as_str())?;
    hash_evidence_digest(
        digest,
        metadata
            .revision_evidence()
            .payload_evidence()
            .content_digest(),
    )
}

fn hash_execution_terms(
    digest: &mut Sha256,
    terms: InstrumentExecutionTerms,
) -> Result<(), MarketInvestmentReadError> {
    hash_bytes(digest, terms.instrument_id().as_uuid().as_bytes())?;
    hash_u64(digest, terms.definition_revision().get())?;
    hash_decimal(digest, terms.price_tick().as_decimal())?;
    hash_decimal(digest, terms.lot_size().as_decimal())?;
    hash_text(digest, terms.quote_currency().as_str())?;
    hash_decimal(digest, terms.contract_multiplier())
}

fn hash_price_level(
    digest: &mut Sha256,
    level: PriceLevelProjection,
    terms: InstrumentExecutionTerms,
) -> Result<(), MarketInvestmentReadError> {
    hash_i64(digest, level.price().get())?;
    hash_i64(digest, level.quantity().get())?;
    hash_u32(digest, level.order_count())?;
    hash_decimal(
        digest,
        level
            .price()
            .checked_to_decimal(terms.price_tick())
            .map_err(|_error| MarketInvestmentReadError::InvalidFinancialTerms)?,
    )
}

fn hash_display_decimal(
    digest: &mut Sha256,
    value: &crate::live_source::display_market::DisplayDecimal,
) -> Result<(), MarketInvestmentReadError> {
    hash_decimal(digest, value.value())?;
    hash_text(digest, value.provider_lexeme())
}

fn finish_mark_evidence(digest: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), MarketInvestmentReadError> {
    let length = u64::try_from(value.len())
        .map_err(|_error| MarketInvestmentReadError::EvidenceIdentityEncoding)?;
    digest.update(length.to_be_bytes());
    digest.update(value);
    Ok(())
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), MarketInvestmentReadError> {
    hash_bytes(digest, value.as_bytes())
}

fn hash_tag(digest: &mut Sha256, value: u8) -> Result<(), MarketInvestmentReadError> {
    hash_bytes(digest, &[value])
}

fn hash_bool(digest: &mut Sha256, value: bool) -> Result<(), MarketInvestmentReadError> {
    hash_tag(digest, u8::from(value))
}

fn hash_u32(digest: &mut Sha256, value: u32) -> Result<(), MarketInvestmentReadError> {
    hash_bytes(digest, &value.to_be_bytes())
}

fn hash_u64(digest: &mut Sha256, value: u64) -> Result<(), MarketInvestmentReadError> {
    hash_bytes(digest, &value.to_be_bytes())
}

fn hash_i64(digest: &mut Sha256, value: i64) -> Result<(), MarketInvestmentReadError> {
    hash_bytes(digest, &value.to_be_bytes())
}

fn hash_i128(digest: &mut Sha256, value: i128) -> Result<(), MarketInvestmentReadError> {
    hash_bytes(digest, &value.to_be_bytes())
}

fn hash_count(digest: &mut Sha256, value: usize) -> Result<(), MarketInvestmentReadError> {
    hash_u64(
        digest,
        u64::try_from(value)
            .map_err(|_error| MarketInvestmentReadError::EvidenceIdentityEncoding)?,
    )
}

fn hash_timestamp(digest: &mut Sha256, value: Timestamp) -> Result<(), MarketInvestmentReadError> {
    hash_i64(digest, value.unix_nanos())
}

fn hash_optional_timestamp(
    digest: &mut Sha256,
    value: Option<Timestamp>,
) -> Result<(), MarketInvestmentReadError> {
    match value {
        Some(value) => {
            hash_tag(digest, 1)?;
            hash_timestamp(digest, value)
        }
        None => hash_tag(digest, 0),
    }
}

fn hash_optional_u64(
    digest: &mut Sha256,
    value: Option<u64>,
) -> Result<(), MarketInvestmentReadError> {
    match value {
        Some(value) => {
            hash_tag(digest, 1)?;
            hash_u64(digest, value)
        }
        None => hash_tag(digest, 0),
    }
}

fn hash_evidence_digest(
    digest: &mut Sha256,
    value: EvidenceDigest,
) -> Result<(), MarketInvestmentReadError> {
    hash_tag(
        digest,
        match value.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        },
    )?;
    hash_bytes(digest, &value.bytes())
}

fn hash_optional_evidence_digest(
    digest: &mut Sha256,
    value: Option<EvidenceDigest>,
) -> Result<(), MarketInvestmentReadError> {
    match value {
        Some(value) => {
            hash_tag(digest, 1)?;
            hash_evidence_digest(digest, value)
        }
        None => hash_tag(digest, 0),
    }
}

fn hash_decimal(digest: &mut Sha256, value: Decimal) -> Result<(), MarketInvestmentReadError> {
    let value = value.normalize();
    hash_i128(digest, value.mantissa())?;
    hash_u32(digest, value.scale())
}

fn hash_optional_depth(
    digest: &mut Sha256,
    value: Option<MarketDepth>,
) -> Result<(), MarketInvestmentReadError> {
    hash_tag(
        digest,
        match value {
            None => 0,
            Some(MarketDepth::TopOfBook) => 1,
            Some(MarketDepth::PriceLevel) => 2,
            Some(MarketDepth::OrderLevel) => 3,
        },
    )
}

fn hash_optional_trading_status(
    digest: &mut Sha256,
    value: Option<TradingStatus>,
) -> Result<(), MarketInvestmentReadError> {
    match value {
        Some(value) => {
            hash_tag(digest, 1)?;
            hash_tag(digest, trading_status_tag(value))
        }
        None => hash_tag(digest, 0),
    }
}

const fn mark_basis_tag(value: MarketInvestmentMarkBasis) -> u8 {
    match value {
        MarketInvestmentMarkBasis::FreshLastTrade => 1,
        MarketInvestmentMarkBasis::FreshBidAskMidpoint => 2,
    }
}

const fn integrity_state_tag(value: IntegrityState) -> u8 {
    match value {
        IntegrityState::Verified => 1,
        IntegrityState::Unverified => 2,
        IntegrityState::NotApplicable => 3,
        IntegrityState::Failed => 4,
        IntegrityState::Quarantined => 5,
    }
}

const fn data_quality_tag(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

const fn market_coverage_tag(value: MarketCoverage) -> u8 {
    match value {
        MarketCoverage::Consolidated => 1,
        MarketCoverage::MultiVenuePartial => 2,
        MarketCoverage::SingleVenue => 3,
        MarketCoverage::Benchmark => 4,
        MarketCoverage::Reference => 5,
        MarketCoverage::UserOwned => 6,
    }
}

const fn coverage_status_tag(value: CoverageStatus) -> u8 {
    match value {
        CoverageStatus::Sufficient => 1,
        CoverageStatus::Insufficient => 2,
        CoverageStatus::Unknown => 3,
    }
}

const fn aggressor_side_tag(value: AggressorSide) -> u8 {
    match value {
        AggressorSide::Buy => 1,
        AggressorSide::Sell => 2,
        AggressorSide::Unknown => 3,
    }
}

const fn trading_status_tag(value: TradingStatus) -> u8 {
    match value {
        TradingStatus::Active => 1,
        TradingStatus::Halted => 2,
        TradingStatus::Inactive => 3,
        TradingStatus::Delisted => 4,
    }
}

const fn capture_integrity_tag(value: CaptureIntegrityState) -> u8 {
    match value {
        CaptureIntegrityState::Disabled => 1,
        CaptureIntegrityState::Healthy => 2,
        CaptureIntegrityState::Incomplete => 3,
    }
}

const fn display_time_basis_tag(value: DisplayEffectiveTimeBasis) -> u8 {
    match value {
        DisplayEffectiveTimeBasis::Provider => 1,
        DisplayEffectiveTimeBasis::Received => 2,
    }
}

#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "the proof exercises the production digest prefix"
)]
pub(super) fn synthetic_mark_evidence_identity(
    selected: SelectedMarketSource<'_>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    value: Decimal,
    currency: Currency,
    basis: MarketInvestmentMarkBasis,
    fresh_until: Option<Timestamp>,
    evidence_revision: u64,
) -> Result<EvidenceDigest, MarketInvestmentReadError> {
    let mut digest = mark_evidence_prefix(
        selected,
        selection_digest,
        selected_at,
        value,
        currency,
        basis,
        fresh_until,
    )?;
    hash_tag(&mut digest, u8::MAX)?;
    hash_u64(&mut digest, evidence_revision)?;
    Ok(finish_mark_evidence(digest))
}
