//! Canonical point-in-time research observation family.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::market::CorporateActionInvariantError;
use crate::{
    CorporateActionKind, InstrumentId, QuantityLots, ResearchContext, RevisionNumber,
    SourceIdentifier,
};

#[path = "research/observations.rs"]
mod observations;
#[path = "research/xbrl.rs"]
mod xbrl;

pub use observations::{
    AlternativeDataObservation, CorporateActionObservation, FilingObservation,
    FundamentalObservation, MacroMissingValue, MacroObservation, MacroValue, PositionObservation,
    TransactionObservation, UniverseMembershipObservation,
};
pub use xbrl::{
    MAX_XBRL_DIMENSIONS, MAX_XBRL_GRAPH_EVENTS, MAX_XBRL_RELATIONSHIP_REFS, MAX_XBRL_RELATIONSHIPS,
    MAX_XBRL_UNIT_MEASURES, XBRL_FACT_EVIDENCE_SCHEMA_VERSION, XbrlAccuracy, XbrlAccuracyValue,
    XbrlContextGraph, XbrlDimensionEvidence, XbrlDimensionLocation, XbrlDimensionMember,
    XbrlDuplicateClass, XbrlDuplicateEvidence, XbrlEntity, XbrlEvidenceError, XbrlFactEvidence,
    XbrlFactEvidenceInput, XbrlOccurrenceRelationships, XbrlPeriod, XbrlQualifiedName,
    XbrlRelationshipEvidence, XbrlSign, XbrlTaxonomySet, XbrlTaxonomyStatus, XbrlText,
    XbrlTypedMemberValidation, XbrlUnitExpression, XbrlXmlEvent,
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
#[serde(
    deny_unknown_fields,
    tag = "observation",
    content = "payload",
    rename_all = "snake_case"
)]
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
    /// Source-authored historical instrument-universe membership.
    UniverseMembership(UniverseMembershipObservation),
    /// User-owned, licensed, or public alternative dataset observation.
    AlternativeData(AlternativeDataObservation),
}

impl ResearchObservation {
    /// Rebinds one finalized observation to a durable revision without changing its payload,
    /// provenance, or source-authored temporal coordinates.
    ///
    /// # Errors
    ///
    /// Returns the original variant's invariant error if reconstruction detects corrupted retained
    /// state. Valid canonical observations remain valid because revision is not a payload invariant.
    pub fn with_revision(&self, revision: RevisionNumber) -> Result<Self, ResearchError> {
        match self {
            Self::Filing(value) => FilingObservation::new(
                value.context().with_revision(revision),
                value.form_type().clone(),
                value.accession().clone(),
            )
            .map(Self::Filing),
            Self::Fundamental(value) => match value.xbrl_evidence() {
                Some(evidence) => FundamentalObservation::new_with_xbrl_evidence(
                    value.context().with_revision(revision),
                    value.concept().clone(),
                    value.value(),
                    value.unit().clone(),
                    evidence.clone(),
                ),
                None => FundamentalObservation::new(
                    value.context().with_revision(revision),
                    value.concept().clone(),
                    value.value(),
                    value.unit().clone(),
                ),
            }
            .map(Self::Fundamental),
            Self::Macro(value) => {
                let context = value.context().with_revision(revision);
                if let Some(observed) = value.value().observed_value() {
                    Ok(Self::Macro(MacroObservation::new(
                        context,
                        value.series().clone(),
                        observed,
                        value.unit().clone(),
                    )))
                } else if let Some(missing) = value.value().missing_value() {
                    Ok(Self::Macro(MacroObservation::missing(
                        context,
                        value.series().clone(),
                        missing.clone(),
                        value.unit().clone(),
                    )))
                } else {
                    Err(ResearchError::InvalidMacroValueState)
                }
            }
            Self::PortfolioPosition(value) => PositionObservation::new(
                value.context().with_revision(revision),
                value.account_id().clone(),
                value.side(),
                value.absolute_quantity(),
            )
            .map(Self::PortfolioPosition),
            Self::Transaction(value) => Ok(Self::Transaction(TransactionObservation::new(
                value.context().with_revision(revision),
                value.account_id().clone(),
                value.transaction_type().clone(),
                value.source_record_id().clone(),
            ))),
            Self::CorporateAction(value) => CorporateActionObservation::new(
                value.context().with_revision(revision),
                value.action().clone(),
            )
            .map(Self::CorporateAction),
            Self::UniverseMembership(value) => UniverseMembershipObservation::new(
                value.context().with_revision(revision),
                value.universe().clone(),
                value.effective_interval(),
            )
            .map(Self::UniverseMembership),
            Self::AlternativeData(value) => {
                Ok(Self::AlternativeData(AlternativeDataObservation::new(
                    value.context().with_revision(revision),
                    value.dataset().clone(),
                    value.field().clone(),
                    value.value(),
                    value.unit().cloned(),
                )))
            }
        }
    }
}

/// A canonical research-payload invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResearchError {
    /// Instrument-scoped research data lacks stable instrument identity.
    MissingInstrument,
    /// A venue-scoped research observation lacks venue identity.
    MissingVenue,
    /// Persisted positions must have a nonzero absolute quantity.
    ZeroPosition,
    /// A macro observation encoded both or neither observed and missing value state.
    InvalidMacroValueState,
    /// A merger successor is the same stable instrument.
    SelfMerger,
    /// A spinoff distributes the same stable instrument.
    SelfSpinoff,
    /// A corporate-action monetary distribution or consideration is not strictly positive.
    NonPositiveCorporateActionAmount,
    /// A symbol-change action does not change the symbol.
    UnchangedSymbol,
    /// A symbol-change action's venue disagrees with research provenance.
    CorporateActionVenueMismatch,
    /// A universe membership interval does not start at the observation's exact effective time.
    UniverseIntervalStartMismatch,
    /// XBRL evidence failed validation or did not bind the canonical value.
    XbrlEvidence(XbrlEvidenceError),
}

impl fmt::Display for ResearchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingInstrument => {
                formatter.write_str("research observation requires an instrument")
            }
            Self::MissingVenue => {
                formatter.write_str("venue-scoped research observation requires a venue")
            }
            Self::ZeroPosition => formatter.write_str("portfolio position must be nonzero"),
            Self::InvalidMacroValueState => formatter
                .write_str("macro observation requires exactly one observed or missing value"),
            Self::SelfMerger => {
                formatter.write_str("merger successor must be a distinct instrument")
            }
            Self::SelfSpinoff => {
                formatter.write_str("spinoff distribution must be a distinct instrument")
            }
            Self::NonPositiveCorporateActionAmount => {
                formatter.write_str("corporate-action monetary amount must be positive")
            }
            Self::UnchangedSymbol => formatter.write_str("symbol change requires distinct symbols"),
            Self::CorporateActionVenueMismatch => {
                formatter.write_str("symbol-change venue must match research provenance")
            }
            Self::UniverseIntervalStartMismatch => {
                formatter.write_str("universe membership interval must start at its effective time")
            }
            Self::XbrlEvidence(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ResearchError {}

impl From<XbrlEvidenceError> for ResearchError {
    fn from(value: XbrlEvidenceError) -> Self {
        Self::XbrlEvidence(value)
    }
}

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
    action
        .validate_for_instrument(instrument_id)
        .map_err(|error| match error {
            CorporateActionInvariantError::SelfMerger => ResearchError::SelfMerger,
            CorporateActionInvariantError::SelfSpinoff => ResearchError::SelfSpinoff,
            CorporateActionInvariantError::NonPositiveMonetaryAmount => {
                ResearchError::NonPositiveCorporateActionAmount
            }
        })?;
    match action {
        CorporateActionKind::SymbolChange {
            venue_id,
            previous,
            current,
        } => {
            let provenance_venue = context
                .provenance()
                .venue_id()
                .ok_or(ResearchError::MissingVenue)?;
            if provenance_venue != venue_id {
                return Err(ResearchError::CorporateActionVenueMismatch);
            }
            if previous == current {
                return Err(ResearchError::UnchangedSymbol);
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
