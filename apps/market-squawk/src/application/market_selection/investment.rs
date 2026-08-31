//! Typed, fail-closed market evidence for one selected investment instrument.

use std::fmt;

use market_squawk_domain::{
    ConnectionGeneration, Currency, DataQuality, EvidenceDigest, InstrumentDefinition,
    InstrumentId, MarketDataInstrumentDefinition, MarketDepth, SourceIdentifier, Timestamp,
};
use market_squawk_live::{
    LastTradeSnapshot, LiveFeatureSetSnapshot, LiveFeatureSnapshot, OrderLevelPriceProjection,
    SnapshotCompleteness, StreamSnapshot,
};
use rust_decimal::Decimal;

use super::{
    CandidateTimestamps, IntegrityState, MarketCoverage, MarketSelectionReceipt,
    SelectedMarketSource,
};
use crate::application::market_runtime::{
    MarketDisplaySnapshotLease, MarketKrakenPriceProjectionLease,
};
use crate::live_source::display_market::DisplayMarketReadObservation;

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
    DurablePitEvidenceNotEstablished,
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

/// Exact feature evidence, or a typed reason it is unavailable.
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

/// Exact decimal mark and currency backed by one retained durable source observation.
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

/// One native live stream plus immutable publication facts used only for mismatch rejection.
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

/// The exact retained hot source selected by the existing unified resolver.
///
/// Every current variant is display-only. A future available investment observation must add a
/// distinct variant carrying a proof-bearing durable point-in-time read authority; a caller flag
/// or one of these hot leases can never create that authority.
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
///
/// Every field is private and the current hot-source reader has no construction path. A future
/// constructor must consume a distinct proof-bearing durable point-in-time source variant.
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

/// Rejects mismatched hot evidence, then closes investment analysis until durable PIT exists.
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
    validate_hot_source_matches_selection(selected, source, receipt.selected_at())?;
    Ok(MarketInvestmentRead::Unavailable(
        MarketInvestmentUnavailableReason::DurablePitEvidenceNotEstablished,
    ))
}

fn validate_hot_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    source: SelectedMarketInvestmentSource<'_>,
    selected_at: Timestamp,
) -> Result<(), MarketInvestmentReadError> {
    match source {
        SelectedMarketInvestmentSource::Live(source) => {
            validate_live_source_matches_selection(selected, source)
        }
        SelectedMarketInvestmentSource::Display {
            snapshot,
            definition,
        } => validate_display_source_matches_selection(selected, snapshot, definition, selected_at),
        SelectedMarketInvestmentSource::Kraken(snapshot) => {
            validate_kraken_source_matches_selection(selected, snapshot, selected_at)
        }
    }
}

fn validate_live_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    source: LiveMarketInvestmentSource<'_>,
) -> Result<(), MarketInvestmentReadError> {
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
    if source.features.set_dimension().completeness() == SnapshotCompleteness::Complete {
        let mut matching_features = source.features.sets().iter().filter(|features| {
            features.source() == stream.source()
                && features.venue() == stream.venue()
                && features.instrument() == stream.instrument()
                && features.provider_product() == stream.provider_product()
                && features.provider_channel() == stream.provider_channel()
                && features.connection_generation() == stream.connection_generation()
                && selected_generation_matches(selected, features.connection_generation())
        });
        let _ = matching_features.next();
        if matching_features.next().is_some() {
            return Err(MarketInvestmentReadError::AmbiguousFeatureEvidence);
        }
    }
    Ok(())
}

fn validate_display_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    snapshot: &MarketDisplaySnapshotLease,
    definition: &MarketDataInstrumentDefinition,
    selected_at: Timestamp,
) -> Result<(), MarketInvestmentReadError> {
    let identity = selected.candidate().identity();
    let key = snapshot.lease().key();
    if snapshot.metadata().provider() != identity.provider()
        || snapshot.surface_id() != identity.observation_id()
        || key.source_id() != identity.source_id()
        || Some(key.venue_id()) != identity.venue_id()
        || key.instrument_id() != identity.instrument_id()
        || definition.instrument_id() != identity.instrument_id()
        || !snapshot.matches_definition(definition)
        || definition.effective_interval().starts_at() > selected_at
        || definition
            .effective_interval()
            .ends_at()
            .is_some_and(|ends_at| selected_at >= ends_at)
        || !selected_generation_matches(selected, key.generation())
    {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }
    Ok(())
}

fn validate_kraken_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    snapshot: &MarketKrakenPriceProjectionLease,
    selected_at: Timestamp,
) -> Result<(), MarketInvestmentReadError> {
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
    Ok(())
}
