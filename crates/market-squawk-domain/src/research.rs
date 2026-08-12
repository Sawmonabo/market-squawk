//! Canonical point-in-time research observation family.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::market::CorporateActionInvariantError;
use crate::{
    CorporateActionKind, InstrumentId, QuantityLots, ResearchContext, RevisionNumber,
    SourceIdentifier,
};

#[path = "research/fund_nav.rs"]
mod fund_nav;
#[path = "research/fundamental_context.rs"]
mod fundamental_context;
#[path = "research/observations.rs"]
mod observations;
#[path = "research/portfolio_transactions.rs"]
mod portfolio_transactions;
#[path = "research/xbrl.rs"]
mod xbrl;

pub use fund_nav::{
    FundNavCompleteness, FundNavCorrectionState, FundNavDisposition, FundNavEntitlementEvidence,
    FundNavFinality, FundNavLineage, FundNavMissingState, FundNavNativeSchema, FundNavObservation,
    FundNavObservationInput, FundNavRevisionEvidence, FundNavValuationBasis, FundNavValue,
};
pub use fundamental_context::{
    FundamentalAmendmentStatus, FundamentalCadence, FundamentalConsolidation,
    FundamentalContextError, FundamentalDimensionContext, FundamentalFactContext,
    FundamentalFactContextInput, FundamentalPeriod, FundamentalRestatementStatus,
    FundamentalRevisionOrder,
};
pub use observations::{
    AlternativeDataObservation, BarTimeSemantics, BarTimestampBasis, CorporateActionObservation,
    FilingObservation, FundamentalObservation, MacroMissingValue, MacroObservation, MacroValue,
    MarketBarAdjustment, MarketBarObservation, MarketBarSessionEvidence, MarketBarSessionKind,
    PositionObservation, TransactionObservation, UniverseMembershipObservation,
};
pub use portfolio_transactions::{
    NormalizedPortfolioLotMethod, NormalizedPortfolioTransactionClass,
    NormalizedPortfolioTransactionError, NormalizedPortfolioTransactionEvidence,
    NormalizedPortfolioTransactionEvidenceInput,
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
    /// Exact historical market bar with canonical instrument and venue identity.
    MarketBar(MarketBarObservation),
    /// Exact daily net asset value for one resolved fund/share class.
    FundNav(FundNavObservation),
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
                    value.fact_context().clone(),
                    evidence.clone(),
                ),
                None => FundamentalObservation::new(
                    value.context().with_revision(revision),
                    value.concept().clone(),
                    value.value(),
                    value.fact_context().clone(),
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
            Self::MarketBar(value) => MarketBarObservation::new(
                value.context().with_revision(revision),
                value.provider_instrument_id().clone(),
                value.feed().clone(),
                value.interval().clone(),
                value.time_semantics().clone(),
                value.adjustment(),
                value.open(),
                value.high(),
                value.low(),
                value.close(),
                value.volume(),
                value.trade_count(),
                value.vwap(),
            )
            .map(Self::MarketBar),
            Self::FundNav(value) => FundNavObservation::try_new(FundNavObservationInput {
                context: value.context().with_revision(revision),
                provider_instrument_id: value.provider_instrument_id().clone(),
                instrument_reference_revision: value.instrument_reference_revision().clone(),
                provider_product: value.provider_product().clone(),
                provider_channel: value.provider_channel().clone(),
                nav_date: value.nav_date(),
                valuation_basis: value.valuation_basis(),
                currency: value.currency(),
                value: value.value(),
                canonical_published_at: value.canonical_published_at(),
                lineage: value.lineage().clone(),
                revision_evidence: value.revision_evidence().clone(),
            })
            .map(Self::FundNav),
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
    /// Fundamental source context is internally inconsistent or disagrees with PIT evidence.
    FundamentalContext(FundamentalContextError),
    /// A market bar lacks an exact effective timestamp.
    MarketBarRequiresExactEffectiveTime,
    /// A market-bar aggregation period is empty or reversed.
    InvalidMarketBarTimeRange,
    /// Market-bar session evidence carries no usable exact identity.
    InvalidMarketBarSessionEvidence,
    /// Canonical effective/provenance time disagrees with the declared provider boundary.
    MarketBarProviderTimestampMismatch,
    /// Conservative point-in-time availability does not establish completed-bar knowledge.
    MarketBarUnavailableBeforeCompletion,
    /// A market bar price is zero or negative.
    NonPositiveMarketBarPrice,
    /// A market bar mixes price currencies.
    MarketBarCurrencyMismatch,
    /// A market bar violates its low/high envelope.
    InvalidMarketBarRange,
    /// A market bar volume is negative.
    NegativeMarketBarVolume,
    /// A fund NAV carried a venue and could be mistaken for a traded price.
    FundNavMustNotHaveVenue,
    /// The NAV date does not equal the exact calendar-date effective coordinate.
    FundNavDateMismatch,
    /// Source publication precision disagrees with the source timestamp.
    FundNavSourcePublicationMismatch,
    /// A NAV lacks conservative availability evidence.
    FundNavRequiresConservativeAvailability,
    /// Canonical publication precedes local receipt, availability, or ingestion.
    FundNavCanonicalPublicationTooEarly,
    /// Exact NAV value, currency, completeness, and disposition disagree.
    InvalidFundNavValueState,
    /// Fund NAV request/raw/schema lineage contains unusable exact evidence.
    InvalidFundNavLineage,
    /// Fund NAV correction predecessor/successor evidence is invalid.
    InvalidFundNavRevisionEvidence,
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
            Self::FundamentalContext(error) => error.fmt(formatter),
            Self::MarketBarRequiresExactEffectiveTime => {
                formatter.write_str("market bar requires an exact effective timestamp")
            }
            Self::InvalidMarketBarTimeRange => {
                formatter.write_str("market bar aggregation period must have a positive duration")
            }
            Self::InvalidMarketBarSessionEvidence => {
                formatter.write_str("market bar session evidence requires a nonzero identity")
            }
            Self::MarketBarProviderTimestampMismatch => formatter.write_str(
                "market bar effective and source timestamps must equal the provider boundary",
            ),
            Self::MarketBarUnavailableBeforeCompletion => formatter.write_str(
                "market bar availability must conservatively establish period completion",
            ),
            Self::NonPositiveMarketBarPrice => {
                formatter.write_str("market bar prices must be positive")
            }
            Self::MarketBarCurrencyMismatch => {
                formatter.write_str("market bar prices must use one currency")
            }
            Self::InvalidMarketBarRange => {
                formatter.write_str("market bar prices violate the low/high range")
            }
            Self::NegativeMarketBarVolume => {
                formatter.write_str("market bar volume must not be negative")
            }
            Self::FundNavMustNotHaveVenue => {
                formatter.write_str("fund NAV must not carry a trading venue")
            }
            Self::FundNavDateMismatch => formatter
                .write_str("fund NAV date must equal its calendar-date effective coordinate"),
            Self::FundNavSourcePublicationMismatch => formatter
                .write_str("fund NAV source publication precision must match its source timestamp"),
            Self::FundNavRequiresConservativeAvailability => formatter
                .write_str("fund NAV requires conservative point-in-time availability evidence"),
            Self::FundNavCanonicalPublicationTooEarly => formatter.write_str(
                "fund NAV canonical publication cannot precede receipt, availability, or ingestion",
            ),
            Self::InvalidFundNavValueState => formatter.write_str(
                "fund NAV value, currency, completeness, and disposition are inconsistent",
            ),
            Self::InvalidFundNavLineage => {
                formatter.write_str("fund NAV lineage requires nonzero exact evidence")
            }
            Self::InvalidFundNavRevisionEvidence => {
                formatter.write_str("fund NAV revision evidence is invalid")
            }
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

impl From<FundamentalContextError> for ResearchError {
    fn from(value: FundamentalContextError) -> Self {
        Self::FundamentalContext(value)
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
