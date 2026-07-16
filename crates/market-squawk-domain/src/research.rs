//! Canonical point-in-time research observation family.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{CorporateActionKind, InstrumentId, QuantityLots, ResearchContext, SourceIdentifier};

#[path = "research/observations.rs"]
mod observations;

pub use observations::{
    AlternativeDataObservation, CorporateActionObservation, FilingObservation,
    FundamentalObservation, MacroObservation, PositionObservation, TransactionObservation,
};

/// Direction of a nonzero portfolio position.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    /// Positive economic exposure.
    Long,
    /// Negative economic exposure represented with a positive absolute lot quantity.
    Short,
}

/// A canonical research observation, deliberately separate from [`crate::MarketEvent`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "observation", content = "payload", rename_all = "snake_case")]
pub enum ResearchObservation {
    /// Regulatory or issuer filing.
    Filing(FilingObservation),
    /// Company fundamental fact.
    Fundamental(FundamentalObservation),
    /// Macroeconomic series observation.
    Macro(MacroObservation),
    /// Account position as of an effective time.
    PortfolioPosition(PositionObservation),
    /// Source transaction record.
    Transaction(TransactionObservation),
    /// Corporate action obtained through research ingestion.
    CorporateAction(CorporateActionObservation),
    /// User-owned, licensed, or public alternative dataset observation.
    AlternativeData(AlternativeDataObservation),
}

/// A canonical research-payload invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResearchError {
    /// Instrument-scoped research data lacks stable instrument identity.
    MissingInstrument,
    /// Persisted positions must have a nonzero absolute quantity.
    ZeroPosition,
    /// A merger successor is the same stable instrument.
    SelfMerger,
    /// A symbol-change action does not change the symbol.
    UnchangedSymbol,
}

impl fmt::Display for ResearchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingInstrument => {
                formatter.write_str("research observation requires an instrument")
            }
            Self::ZeroPosition => formatter.write_str("portfolio position must be nonzero"),
            Self::SelfMerger => {
                formatter.write_str("merger successor must be a distinct instrument")
            }
            Self::UnchangedSymbol => formatter.write_str("symbol change requires distinct symbols"),
        }
    }
}

impl std::error::Error for ResearchError {}

pub(super) fn require_instrument(context: &ResearchContext) -> Result<InstrumentId, ResearchError> {
    context
        .provenance()
        .instrument_id()
        .ok_or(ResearchError::MissingInstrument)
}

pub(super) fn validate_corporate_action(
    context: &ResearchContext,
    action: &CorporateActionKind,
) -> Result<(), ResearchError> {
    let instrument_id = require_instrument(context)?;
    match action {
        CorporateActionKind::Merger { successor } if *successor == instrument_id => {
            Err(ResearchError::SelfMerger)
        }
        CorporateActionKind::SymbolChange { previous, current } if previous == current => {
            Err(ResearchError::UnchangedSymbol)
        }
        _ => Ok(()),
    }
}
