//! Exact source context retained for one company fundamental fact.

use std::fmt;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{CalendarDate, ResearchContext, RevisionNumber, SchemaVersion, SchemaVersionError};

use super::{
    MAX_XBRL_DIMENSIONS, SourceIdentifier, XbrlDimensionEvidence, XbrlFactEvidence, XbrlPeriod,
};

/// Instant or duration semantics reported for one fundamental fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum FundamentalPeriod {
    /// The fact measures a value at one civil date.
    Instant {
        /// Exact source-reported instant date.
        instant: CalendarDate,
    },
    /// The fact measures a value over one inclusive source interval.
    Duration {
        /// Exact source-reported start date.
        start: CalendarDate,
        /// Exact source-reported end date.
        end: CalendarDate,
    },
}

impl FundamentalPeriod {
    /// Constructs instant-period context.
    pub const fn instant(instant: CalendarDate) -> Self {
        Self::Instant { instant }
    }

    /// Constructs duration-period context.
    ///
    /// # Errors
    ///
    /// Rejects a duration whose start is later than its end.
    pub fn duration(
        start: CalendarDate,
        end: CalendarDate,
    ) -> Result<Self, FundamentalContextError> {
        if start > end {
            Err(FundamentalContextError::InvertedPeriod)
        } else {
            Ok(Self::Duration { start, end })
        }
    }

    /// Returns the duration start, or `None` for an instant fact.
    pub const fn start(self) -> Option<CalendarDate> {
        match self {
            Self::Instant { .. } => None,
            Self::Duration { start, .. } => Some(start),
        }
    }

    /// Returns the instant date or duration end date.
    pub const fn end(self) -> CalendarDate {
        match self {
            Self::Instant { instant } => instant,
            Self::Duration { end, .. } => end,
        }
    }

    fn matches_xbrl(self, period: XbrlPeriod) -> bool {
        match (self, period) {
            (Self::Instant { instant: left }, XbrlPeriod::Instant { instant: right }) => {
                left == right
            }
            (
                Self::Duration {
                    start: left_start,
                    end: left_end,
                },
                XbrlPeriod::Duration {
                    start: right_start,
                    end: right_end,
                },
            ) => left_start == right_start && left_end == right_end,
            (Self::Instant { .. }, XbrlPeriod::Duration { .. })
            | (Self::Duration { .. }, XbrlPeriod::Instant { .. }) => false,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
enum FundamentalPeriodWire {
    Instant {
        instant: CalendarDate,
    },
    Duration {
        start: CalendarDate,
        end: CalendarDate,
    },
}

impl<'de> Deserialize<'de> for FundamentalPeriod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match FundamentalPeriodWire::deserialize(deserializer)? {
            FundamentalPeriodWire::Instant { instant } => Ok(Self::instant(instant)),
            FundamentalPeriodWire::Duration { start, end } => {
                Self::duration(start, end).map_err(serde::de::Error::custom)
            }
        }
    }
}

/// Filing-amendment semantics proven by the exact source form.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundamentalAmendmentStatus {
    /// The exact filing form does not carry an amendment suffix.
    Original,
    /// The exact filing form carries an amendment suffix.
    Amendment,
    /// The source did not supply a filing form from which amendment status can be proven.
    Unavailable,
}

/// Reporting cadence explicitly decoded from the source's fiscal-period contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundamentalCadence {
    /// Source fiscal-period semantics identify an annual fact.
    Annual,
    /// Source fiscal-period semantics identify a quarterly fact.
    Quarterly,
    /// The source supplied another exact fiscal-period class.
    Other,
    /// The source did not supply a fiscal-period class.
    Unavailable,
}

/// Whether a source explicitly characterized the fact as consolidated.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundamentalConsolidation {
    /// The source explicitly identified consolidated reporting.
    SourceReportedConsolidated,
    /// The source explicitly identified non-consolidated reporting.
    SourceReportedNonConsolidated,
    /// The source supplied no explicit consolidation assertion.
    Unavailable,
}

/// Source-reported restatement status, kept separate from amendment and revision order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "status")]
pub enum FundamentalRestatementStatus {
    /// The source supplied no explicit restatement assertion.
    Unavailable,
    /// The source supplied an exact restatement assertion.
    SourceReported {
        /// Whether the source identified this occurrence as restated.
        restated: bool,
        /// Exact source status or ruleset identity supporting the assertion.
        source_status: SourceIdentifier,
    },
}

/// Deterministic ordering of occurrences within one exact concept, unit, and period family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundamentalRevisionOrder {
    ordinal: RevisionNumber,
    ruleset: SourceIdentifier,
}

impl FundamentalRevisionOrder {
    /// Constructs one source-family ordinal under an exact ordering ruleset.
    pub const fn new(ordinal: RevisionNumber, ruleset: SourceIdentifier) -> Self {
        Self { ordinal, ruleset }
    }

    /// Returns the one-based occurrence ordinal within the exact fact family.
    pub const fn ordinal(&self) -> RevisionNumber {
        self.ordinal
    }

    /// Returns the exact ordering-ruleset identity.
    pub const fn ruleset(&self) -> &SourceIdentifier {
        &self.ruleset
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "status")]
enum FundamentalDimensionState {
    Unavailable,
    SourceReported {
        dimensions: Vec<XbrlDimensionEvidence>,
    },
}

/// Bounded dimensions retained only when the source supplies an XBRL context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FundamentalDimensionContext(FundamentalDimensionState);

impl FundamentalDimensionContext {
    /// Constructs explicit source-unavailable dimensional context.
    pub const fn unavailable() -> Self {
        Self(FundamentalDimensionState::Unavailable)
    }

    /// Retains a complete bounded source-reported dimension list, including a reported empty list.
    ///
    /// # Errors
    ///
    /// Rejects more than [`MAX_XBRL_DIMENSIONS`] values or a bounded allocation failure.
    pub fn try_source_reported(
        dimensions: &[XbrlDimensionEvidence],
    ) -> Result<Self, FundamentalContextError> {
        if dimensions.len() > MAX_XBRL_DIMENSIONS {
            return Err(FundamentalContextError::TooManyDimensions);
        }
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(dimensions.len())
            .map_err(|_| FundamentalContextError::AllocationFailed)?;
        retained.extend_from_slice(dimensions);
        Ok(Self(FundamentalDimensionState::SourceReported {
            dimensions: retained,
        }))
    }

    /// Returns source-reported dimensions, distinguishing unavailable from a reported empty list.
    pub fn dimensions(&self) -> Option<&[XbrlDimensionEvidence]> {
        match &self.0 {
            FundamentalDimensionState::Unavailable => None,
            FundamentalDimensionState::SourceReported { dimensions } => Some(dimensions),
        }
    }
}

struct BoundedDimensions(Vec<XbrlDimensionEvidence>);

struct BoundedDimensionsVisitor;

impl<'de> Visitor<'de> for BoundedDimensionsVisitor {
    type Value = BoundedDimensions;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded fundamental dimension list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let hint = sequence.size_hint().unwrap_or(0).min(MAX_XBRL_DIMENSIONS);
        let mut dimensions = Vec::new();
        dimensions
            .try_reserve_exact(hint)
            .map_err(|_| serde::de::Error::custom(FundamentalContextError::AllocationFailed))?;
        while dimensions.len() < MAX_XBRL_DIMENSIONS {
            let Some(dimension) = sequence.next_element()? else {
                return Ok(BoundedDimensions(dimensions));
            };
            if dimensions.len() == dimensions.capacity() {
                dimensions.try_reserve_exact(1).map_err(|_| {
                    serde::de::Error::custom(FundamentalContextError::AllocationFailed)
                })?;
            }
            dimensions.push(dimension);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                FundamentalContextError::TooManyDimensions,
            ))
        } else {
            Ok(BoundedDimensions(dimensions))
        }
    }
}

impl<'de> Deserialize<'de> for BoundedDimensions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedDimensionsVisitor)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "status")]
enum FundamentalDimensionStateWire {
    Unavailable,
    SourceReported { dimensions: BoundedDimensions },
}

impl<'de> Deserialize<'de> for FundamentalDimensionContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match FundamentalDimensionStateWire::deserialize(deserializer)? {
            FundamentalDimensionStateWire::Unavailable => Ok(Self::unavailable()),
            FundamentalDimensionStateWire::SourceReported { dimensions } => {
                Ok(Self(FundamentalDimensionState::SourceReported {
                    dimensions: dimensions.0,
                }))
            }
        }
    }
}

/// Complete current-schema construction input for one fundamental fact context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FundamentalFactContextInput {
    /// Current domain schema.
    pub schema_version: SchemaVersion,
    /// Exact instant or duration semantics.
    pub period: FundamentalPeriod,
    /// Exact source unit key.
    pub unit: SourceIdentifier,
    /// Filing accession carrying the fact.
    pub accession: SourceIdentifier,
    /// Exact filing form when supplied.
    pub filing_form: Option<SourceIdentifier>,
    /// Amendment status proven by the filing form, or unavailable.
    pub amendment_status: FundamentalAmendmentStatus,
    /// Exact filing date when supplied.
    pub filed_on: Option<CalendarDate>,
    /// Exact SEC frame identity when supplied.
    pub frame: Option<SourceIdentifier>,
    /// Source-reported fiscal year when supplied.
    pub fiscal_year: Option<u16>,
    /// Exact source fiscal-period code when supplied.
    pub fiscal_period: Option<SourceIdentifier>,
    /// Cadence decoded from the source fiscal-period contract.
    pub cadence: FundamentalCadence,
    /// Exact XBRL context identity when supplied.
    pub xbrl_context_id: Option<SourceIdentifier>,
    /// Complete bounded source-reported dimensions, or explicitly unavailable.
    pub dimensions: FundamentalDimensionContext,
    /// Explicit source consolidation assertion, or unavailable.
    pub consolidation: FundamentalConsolidation,
    /// Deterministic occurrence order within the exact fact family.
    pub revision_order: FundamentalRevisionOrder,
    /// Explicit source restatement assertion, or unavailable.
    pub restatement_status: FundamentalRestatementStatus,
}

/// Strict semantic context for one exact fundamental observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundamentalFactContext {
    schema_version: SchemaVersion,
    period: FundamentalPeriod,
    unit: SourceIdentifier,
    accession: SourceIdentifier,
    filing_form: Option<SourceIdentifier>,
    amendment_status: FundamentalAmendmentStatus,
    filed_on: Option<CalendarDate>,
    frame: Option<SourceIdentifier>,
    fiscal_year: Option<u16>,
    fiscal_period: Option<SourceIdentifier>,
    cadence: FundamentalCadence,
    xbrl_context_id: Option<SourceIdentifier>,
    dimensions: FundamentalDimensionContext,
    consolidation: FundamentalConsolidation,
    revision_order: FundamentalRevisionOrder,
    restatement_status: FundamentalRestatementStatus,
}

impl FundamentalFactContext {
    /// Constructs strict source context without inferring any unavailable field.
    ///
    /// # Errors
    ///
    /// Rejects unsupported schema, contradictory filing/amendment semantics, year zero, or a
    /// cadence that is not backed by a source fiscal-period code.
    pub fn try_new(input: FundamentalFactContextInput) -> Result<Self, FundamentalContextError> {
        input.schema_version.ensure_supported()?;
        let expected_amendment =
            input
                .filing_form
                .as_ref()
                .map_or(FundamentalAmendmentStatus::Unavailable, |form| {
                    if form.as_str().ends_with("/A") {
                        FundamentalAmendmentStatus::Amendment
                    } else {
                        FundamentalAmendmentStatus::Original
                    }
                });
        if input.amendment_status != expected_amendment {
            return Err(FundamentalContextError::AmendmentStatusMismatch);
        }
        if input.fiscal_year == Some(0) {
            return Err(FundamentalContextError::InvalidFiscalYear);
        }
        if input.fiscal_period.is_some() == (input.cadence == FundamentalCadence::Unavailable) {
            return Err(FundamentalContextError::FiscalCadenceMismatch);
        }
        Ok(Self {
            schema_version: input.schema_version,
            period: input.period,
            unit: input.unit,
            accession: input.accession,
            filing_form: input.filing_form,
            amendment_status: input.amendment_status,
            filed_on: input.filed_on,
            frame: input.frame,
            fiscal_year: input.fiscal_year,
            fiscal_period: input.fiscal_period,
            cadence: input.cadence,
            xbrl_context_id: input.xbrl_context_id,
            dimensions: input.dimensions,
            consolidation: input.consolidation,
            revision_order: input.revision_order,
            restatement_status: input.restatement_status,
        })
    }

    /// Returns the current domain schema.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns exact instant or duration semantics.
    pub const fn period(&self) -> FundamentalPeriod {
        self.period
    }

    /// Returns the exact source unit key.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }

    /// Returns the exact filing accession.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    /// Returns the exact filing form when supplied.
    pub const fn filing_form(&self) -> Option<&SourceIdentifier> {
        self.filing_form.as_ref()
    }

    /// Returns form-proven amendment status.
    pub const fn amendment_status(&self) -> FundamentalAmendmentStatus {
        self.amendment_status
    }

    /// Returns the exact filing date when supplied.
    pub const fn filed_on(&self) -> Option<CalendarDate> {
        self.filed_on
    }

    /// Returns the exact SEC frame identity when supplied.
    pub const fn frame(&self) -> Option<&SourceIdentifier> {
        self.frame.as_ref()
    }

    /// Returns the source-reported fiscal year when supplied.
    pub const fn fiscal_year(&self) -> Option<u16> {
        self.fiscal_year
    }

    /// Returns the exact source fiscal-period code when supplied.
    pub const fn fiscal_period(&self) -> Option<&SourceIdentifier> {
        self.fiscal_period.as_ref()
    }

    /// Returns source-contract cadence semantics.
    pub const fn cadence(&self) -> FundamentalCadence {
        self.cadence
    }

    /// Returns the exact XBRL context identity when supplied.
    pub const fn xbrl_context_id(&self) -> Option<&SourceIdentifier> {
        self.xbrl_context_id.as_ref()
    }

    /// Returns bounded source-reported dimensions or explicit unavailability.
    pub const fn dimensions(&self) -> &FundamentalDimensionContext {
        &self.dimensions
    }

    /// Returns explicit source consolidation status.
    pub const fn consolidation(&self) -> FundamentalConsolidation {
        self.consolidation
    }

    /// Returns deterministic source-family occurrence ordering.
    pub const fn revision_order(&self) -> &FundamentalRevisionOrder {
        &self.revision_order
    }

    /// Returns explicit source restatement status.
    pub const fn restatement_status(&self) -> &FundamentalRestatementStatus {
        &self.restatement_status
    }

    pub(super) fn validate_research_context(
        &self,
        context: &ResearchContext,
    ) -> Result<(), FundamentalContextError> {
        if context.time().effective().calendar_date_value() != Some(self.period.end()) {
            return Err(FundamentalContextError::EffectivePeriodMismatch);
        }
        if let Some(filed_on) = self.filed_on
            && context
                .time()
                .published()
                .and_then(|published| published.calendar_date_value())
                != Some(filed_on)
        {
            return Err(FundamentalContextError::FilingPublicationMismatch);
        }
        Ok(())
    }

    pub(super) fn validate_xbrl_evidence(
        &self,
        evidence: &XbrlFactEvidence,
    ) -> Result<(), FundamentalContextError> {
        if evidence.accession() != &self.accession
            || !self.period.matches_xbrl(evidence.period())
            || self.xbrl_context_id.as_ref() != Some(evidence.context_id())
            || self.dimensions.dimensions() != Some(evidence.dimensions())
        {
            return Err(FundamentalContextError::XbrlContextMismatch);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
struct RequiredOption<T>(Option<T>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundamentalFactContextWire {
    schema_version: SchemaVersion,
    period: FundamentalPeriod,
    unit: SourceIdentifier,
    accession: SourceIdentifier,
    filing_form: RequiredOption<SourceIdentifier>,
    amendment_status: FundamentalAmendmentStatus,
    filed_on: RequiredOption<CalendarDate>,
    frame: RequiredOption<SourceIdentifier>,
    fiscal_year: RequiredOption<u16>,
    fiscal_period: RequiredOption<SourceIdentifier>,
    cadence: FundamentalCadence,
    xbrl_context_id: RequiredOption<SourceIdentifier>,
    dimensions: FundamentalDimensionContext,
    consolidation: FundamentalConsolidation,
    revision_order: FundamentalRevisionOrder,
    restatement_status: FundamentalRestatementStatus,
}

impl<'de> Deserialize<'de> for FundamentalFactContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundamentalFactContextWire::deserialize(deserializer)?;
        Self::try_new(FundamentalFactContextInput {
            schema_version: wire.schema_version,
            period: wire.period,
            unit: wire.unit,
            accession: wire.accession,
            filing_form: wire.filing_form.0,
            amendment_status: wire.amendment_status,
            filed_on: wire.filed_on.0,
            frame: wire.frame.0,
            fiscal_year: wire.fiscal_year.0,
            fiscal_period: wire.fiscal_period.0,
            cadence: wire.cadence,
            xbrl_context_id: wire.xbrl_context_id.0,
            dimensions: wire.dimensions,
            consolidation: wire.consolidation,
            revision_order: wire.revision_order,
            restatement_status: wire.restatement_status,
        })
        .map_err(serde::de::Error::custom)
    }
}

/// Fundamental source-context construction or cross-evidence failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundamentalContextError {
    /// A duration start is after its end.
    InvertedPeriod,
    /// Filing form and amendment status disagree.
    AmendmentStatusMismatch,
    /// Fiscal year zero is not a valid source year.
    InvalidFiscalYear,
    /// Cadence is unavailable despite a period code, or asserted without one.
    FiscalCadenceMismatch,
    /// The source supplied more dimensions than the domain ceiling.
    TooManyDimensions,
    /// A bounded retained allocation failed.
    AllocationFailed,
    /// Canonical effective date and source fact period disagree.
    EffectivePeriodMismatch,
    /// Canonical publication date and source filing date disagree.
    FilingPublicationMismatch,
    /// Inline-XBRL accession, period, context, or dimensions disagree with canonical context.
    XbrlContextMismatch,
    /// The embedded domain schema is unsupported.
    Schema(SchemaVersionError),
}

impl fmt::Display for FundamentalContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvertedPeriod => {
                formatter.write_str("fundamental duration start must not follow its end")
            }
            Self::AmendmentStatusMismatch => {
                formatter.write_str("fundamental filing form and amendment status disagree")
            }
            Self::InvalidFiscalYear => {
                formatter.write_str("fundamental fiscal year must be nonzero")
            }
            Self::FiscalCadenceMismatch => formatter.write_str(
                "fundamental cadence must be unavailable exactly when fiscal period is unavailable",
            ),
            Self::TooManyDimensions => formatter.write_str("too many fundamental dimensions"),
            Self::AllocationFailed => {
                formatter.write_str("fundamental context bounded allocation failed")
            }
            Self::EffectivePeriodMismatch => {
                formatter.write_str("fundamental effective date and source period disagree")
            }
            Self::FilingPublicationMismatch => {
                formatter.write_str("fundamental publication and filing dates disagree")
            }
            Self::XbrlContextMismatch => formatter.write_str(
                "fundamental context does not match the retained Inline-XBRL occurrence",
            ),
            Self::Schema(error) => {
                write!(formatter, "fundamental context schema is invalid: {error}")
            }
        }
    }
}

impl std::error::Error for FundamentalContextError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SchemaVersionError> for FundamentalContextError {
    fn from(value: SchemaVersionError) -> Self {
        Self::Schema(value)
    }
}
