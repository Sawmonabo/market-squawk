//! Source-field projection for an already closed and calendar-reconciled EOD history.
//!
//! Official source contracts: https://www.tiingo.com/documentation/end-of-day and
//! https://www.tiingo.com/documentation/corporate-actions/splits. `divCash` is attached to ex-date,
//! not payment date. `splitFactor` is new units / old units. No separate corporate-action endpoint
//! entitlement, dividend currency, payment date, or global all-action coverage is inferred here.

use crate::{TiingoEodContractEvidence, TiingoEodInstrumentAuthority};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, CorporateActionKind, CorporateActionObservation, Currency,
    DataQuality, EvidenceDigest, InstrumentId, Money, PayloadHash, PayloadReference,
    ResearchContext, ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate,
    ResearchTime, RevisionBoundPayloadEvidence, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_sources::MarketHistoryCashUnitStatus;
use rust_decimal::Decimal;
use std::num::NonZeroU32;

/// Reconstruction evidence for the exact monetary unit of `divCash`, independently of the price
/// currency. The source owner retains whether the assertion was source-attested or a separately
/// reviewed interpretation. This value alone is not source/publication authority.
#[derive(Clone, Debug)]
pub struct TiingoEodCashUnitEvidence {
    status: MarketHistoryCashUnitStatus,
    instrument: InstrumentId,
    contract_identity: EvidenceDigest,
    currency: Currency,
    assertion: RevisionBoundPayloadEvidence,
    available_at: Timestamp,
}
impl TiingoEodCashUnitEvidence {
    pub fn try_new(
        instrument: InstrumentId,
        contract_identity: EvidenceDigest,
        currency: Currency,
        assertion: RevisionBoundPayloadEvidence,
        available_at: Timestamp,
    ) -> Result<Self, TiingoEodActionError> {
        Self::try_new_with_status(
            instrument,
            contract_identity,
            currency,
            assertion,
            available_at,
            MarketHistoryCashUnitStatus::SourceAttested,
        )
    }

    /// The caller must supply the original retained status on replay. A reviewed interpretation
    /// requires its own scoped assertion revision; it must never use the source-attested default.
    pub fn try_new_with_status(
        instrument: InstrumentId,
        contract_identity: EvidenceDigest,
        currency: Currency,
        assertion: RevisionBoundPayloadEvidence,
        available_at: Timestamp,
        status: MarketHistoryCashUnitStatus,
    ) -> Result<Self, TiingoEodActionError> {
        if contract_identity.bytes() == [0; 32]
            || assertion.payload_evidence().content_digest().bytes() == [0; 32]
        {
            return Err(TiingoEodActionError::InvalidEvidence);
        }
        Ok(Self {
            status,
            instrument,
            contract_identity,
            currency,
            assertion,
            available_at,
        })
    }
    pub const fn status(&self) -> MarketHistoryCashUnitStatus {
        self.status
    }
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
    pub const fn contract_identity(&self) -> EvidenceDigest {
        self.contract_identity
    }
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    pub const fn currency(&self) -> Currency {
        self.currency
    }
    pub const fn assertion(&self) -> &RevisionBoundPayloadEvidence {
        &self.assertion
    }
}

/// Closed per-field disposition. An explicit zero/one is different from a missing field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TiingoEodActionFieldDisposition {
    ExplicitNoEvent,
    MissingField,
    MissingMonetaryUnit,
    RatioOutsideCanonicalPrecision,
    Normalized { observation_index: usize },
}

/// One original provider row, retained even if its raw and adjusted bar surfaces are null.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TiingoEodDailyActionDisposition {
    pub history_page_index: usize,
    pub provider_row_index: u32,
    pub date: CalendarDate,
    pub row_digest: EvidenceDigest,
    pub cash_dividend: Option<Decimal>,
    pub split_factor: Option<Decimal>,
    pub cash: TiingoEodActionFieldDisposition,
    pub shares: TiingoEodActionFieldDisposition,
}

/// Canonical candidate with the exact existing history page and native row coordinates. The
/// shared history publisher must bind it to the same sealed graph as the original daily row.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TiingoEodNormalizedAction {
    pub history_page_index: usize,
    pub provider_row_index: u32,
    pub row_digest: EvidenceDigest,
    pub observation: CorporateActionObservation,
}

/// One bounded native page projection awaiting complete history reconciliation.
#[derive(Debug)]
pub(crate) struct TiingoEodPageActionProjection {
    rows: Box<[TiingoEodDailyActionDisposition]>,
    observations: Box<[TiingoEodNormalizedAction]>,
}
impl TiingoEodPageActionProjection {
    pub fn rows(&self) -> &[TiingoEodDailyActionDisposition] {
        &self.rows
    }
    pub fn observations(&self) -> &[TiingoEodNormalizedAction] {
        &self.observations
    }
    pub fn ordinary_fields_complete(&self) -> bool {
        self.rows.iter().all(|row| {
            [row.cash, row.shares].into_iter().all(|field| {
                matches!(
                    field,
                    TiingoEodActionFieldDisposition::ExplicitNoEvent
                        | TiingoEodActionFieldDisposition::Normalized { .. }
                )
            })
        })
    }
}

struct ProjectionContext<'a> {
    instrument: &'a TiingoEodInstrumentAuthority,
    contract: &'a TiingoEodContractEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
}
impl ProjectionContext<'_> {
    fn validate_unit(
        &self,
        unit: Option<&TiingoEodCashUnitEvidence>,
    ) -> Result<(), TiingoEodActionError> {
        if unit.is_some_and(|unit| {
            unit.instrument != self.instrument.instrument_id()
                || unit.contract_identity != self.contract.mapping_identity()
                || unit.available_at > self.ingested_at
        }) {
            return Err(TiingoEodActionError::InvalidEvidence);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ActionRowValues {
    provider_row_index: u32,
    date: CalendarDate,
    row_digest: EvidenceDigest,
    cash_dividend: Option<Decimal>,
    split_factor: Option<Decimal>,
}
struct ProjectionBuilder {
    count: usize,
    rows: Vec<TiingoEodDailyActionDisposition>,
    observations: Vec<TiingoEodNormalizedAction>,
}
impl ProjectionBuilder {
    fn try_new(count: usize) -> Result<Self, TiingoEodActionError> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(count)
            .map_err(|_| TiingoEodActionError::ResourceBound)?;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(
                count
                    .checked_mul(2)
                    .ok_or(TiingoEodActionError::ResourceBound)?,
            )
            .map_err(|_| TiingoEodActionError::ResourceBound)?;
        Ok(Self {
            count,
            rows,
            observations,
        })
    }
    fn push(
        &mut self,
        history_page_index: usize,
        context: &ProjectionContext<'_>,
        row: ActionRowValues,
        cash_unit: Option<&TiingoEodCashUnitEvidence>,
    ) -> Result<(), TiingoEodActionError> {
        if self.rows.len() >= self.count || row.row_digest.bytes() == [0; 32] {
            return Err(TiingoEodActionError::InvalidEvidence);
        }
        let shares = match row.split_factor {
            None => TiingoEodActionFieldDisposition::MissingField,
            Some(value) if value == Decimal::ONE => {
                TiingoEodActionFieldDisposition::ExplicitNoEvent
            }
            Some(value) if value <= Decimal::ZERO => {
                return Err(TiingoEodActionError::InvalidEvidence);
            }
            Some(value) => match exact_ratio(value) {
                Some((numerator, denominator)) => push_action(
                    &mut self.observations,
                    history_page_index,
                    context,
                    &row,
                    "split",
                    CorporateActionKind::Split {
                        numerator,
                        denominator,
                    },
                )?,
                None => TiingoEodActionFieldDisposition::RatioOutsideCanonicalPrecision,
            },
        };
        let cash = match row.cash_dividend {
            None => TiingoEodActionFieldDisposition::MissingField,
            Some(value) if value == Decimal::ZERO => {
                TiingoEodActionFieldDisposition::ExplicitNoEvent
            }
            Some(value) if value < Decimal::ZERO => {
                return Err(TiingoEodActionError::InvalidEvidence);
            }
            Some(value) => match cash_unit {
                Some(unit) => push_action(
                    &mut self.observations,
                    history_page_index,
                    context,
                    &row,
                    "dividend",
                    CorporateActionKind::CashDividend {
                        amount: Money::new(value, unit.currency),
                    },
                )?,
                None => TiingoEodActionFieldDisposition::MissingMonetaryUnit,
            },
        };
        self.rows.push(TiingoEodDailyActionDisposition {
            history_page_index,
            provider_row_index: row.provider_row_index,
            date: row.date,
            row_digest: row.row_digest,
            cash_dividend: row.cash_dividend,
            split_factor: row.split_factor,
            cash,
            shares,
        });
        Ok(())
    }
    fn finish(self) -> Result<TiingoEodPageActionProjection, TiingoEodActionError> {
        if self.rows.len() != self.count {
            return Err(TiingoEodActionError::InvalidEvidence);
        }
        Ok(TiingoEodPageActionProjection {
            rows: self.rows.into_boxed_slice(),
            observations: self.observations.into_boxed_slice(),
        })
    }
}

fn push_action(
    observations: &mut Vec<TiingoEodNormalizedAction>,
    history_page_index: usize,
    context: &ProjectionContext<'_>,
    row: &ActionRowValues,
    component: &str,
    kind: CorporateActionKind,
) -> Result<TiingoEodActionFieldDisposition, TiingoEodActionError> {
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: context.contract.source_id().clone(),
        instrument_id: Some(context.instrument.instrument_id()),
        venue_id: Some(context.instrument.venue_id().clone()),
        source_identifier: SourceIdentifier::try_from(format!(
            "tiingo-eod-action-{}-{}-{component}",
            context.instrument.ticker(),
            row.date
        ))
        .map_err(|_| TiingoEodActionError::InvalidEvidence)?,
        source_timestamp: None,
        received_at: context.received_at,
        ingested_at: context.ingested_at,
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            row.row_digest.algorithm(),
            row.row_digest.bytes(),
        )),
        availability: AvailabilityEvidence::local_first_observed(context.received_at),
    })
    .map_err(|_| TiingoEodActionError::InvalidEvidence)?;
    let time = ResearchTime::try_new_with_coordinates(
        ResearchTemporalCoordinate::calendar_date(row.date),
        None,
        RevisionNumber::new(1).map_err(|_| TiingoEodActionError::InvalidEvidence)?,
        None,
    )
    .map_err(|_| TiingoEodActionError::InvalidEvidence)?;
    let observation = CorporateActionObservation::new(
        ResearchContext::new(provenance, time)
            .map_err(|_| TiingoEodActionError::InvalidEvidence)?,
        kind,
    )
    .map_err(|_| TiingoEodActionError::InvalidEvidence)?;
    let observation_index = observations.len();
    observations.push(TiingoEodNormalizedAction {
        history_page_index,
        provider_row_index: row.provider_row_index,
        row_digest: row.row_digest,
        observation,
    });
    Ok(TiingoEodActionFieldDisposition::Normalized { observation_index })
}

fn exact_ratio(value: Decimal) -> Option<(NonZeroU32, NonZeroU32)> {
    let value = value.normalize();
    let numerator = u128::try_from(value.mantissa()).ok()?;
    let denominator = 10_u128.checked_pow(value.scale())?;
    let (mut left, mut right) = (numerator, denominator);
    while right != 0 {
        (left, right) = (right, left % right);
    }
    Some((
        NonZeroU32::new(u32::try_from(numerator / left).ok()?)?,
        NonZeroU32::new(u32::try_from(denominator / left).ok()?)?,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TiingoEodActionError {
    #[error("Tiingo history has unresolved expected financial dates")]
    IncompleteFinancialDates,
    #[error("Tiingo daily action fields or unit evidence are inconsistent")]
    InvalidEvidence,
    #[error("Tiingo daily action projection exceeds its bounded capacity")]
    ResourceBound,
}

/// Projects one bounded native page for private staging; only the complete validated history
/// owner may publish these candidates after full calendar reconciliation.
pub(crate) fn project_eod_page_actions(
    page: &crate::TiingoEodPageCandidate,
    history_page_index: usize,
    cash_unit: Option<&TiingoEodCashUnitEvidence>,
) -> Result<TiingoEodPageActionProjection, TiingoEodActionError> {
    let context = ProjectionContext {
        instrument: page.instrument(),
        contract: page.contract(),
        received_at: page.received_at(),
        ingested_at: page.ingested_at(),
    };
    context.validate_unit(cash_unit)?;
    if page.provider_actions().len() > page.request().max_rows() {
        return Err(TiingoEodActionError::ResourceBound);
    }
    let mut projection = ProjectionBuilder::try_new(page.provider_actions().len())?;
    for row in page.provider_actions() {
        projection.push(
            history_page_index,
            &context,
            ActionRowValues {
                provider_row_index: row.provider_row_index(),
                date: row.provider_date(),
                row_digest: row.row_digest(),
                cash_dividend: row.cash_dividend(),
                split_factor: row.split_factor(),
            },
            cash_unit,
        )?;
    }
    projection.finish()
}
