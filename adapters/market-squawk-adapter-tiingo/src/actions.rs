//! Source-field projection for an already closed and calendar-reconciled EOD history.
//!
//! Official source contracts: https://www.tiingo.com/documentation/end-of-day and
//! https://www.tiingo.com/documentation/corporate-actions/splits. `divCash` is attached to ex-date,
//! not payment date. `splitFactor` is new units / old units. No separate corporate-action endpoint
//! entitlement, dividend currency, payment date, or global all-action coverage is inferred here.

use crate::{
    TiingoEodContractEvidence, TiingoEodFinancialCoverageDisposition, TiingoEodInstrumentAuthority,
    TiingoEodReceipt, TiingoHistoryPlan, TiingoPendingEodHistoryPublication, TiingoRequestScope,
};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, CorporateActionKind, CorporateActionObservation, Currency,
    DataQuality, EvidenceDigest, InstrumentId, Money, PayloadHash, PayloadReference,
    ResearchContext, ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate,
    ResearchTime, RevisionBoundPayloadEvidence, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{CompleteMarketBarDateWindowsV1, MarketHistoryCashUnitStatus};
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
        Self::try_new_with_status(instrument, contract_identity, currency, assertion,
            available_at, MarketHistoryCashUnitStatus::SourceAttested)
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoEodActionFieldDisposition {
    ExplicitNoEvent,
    MissingField,
    MissingMonetaryUnit,
    RatioOutsideCanonicalPrecision,
    Normalized { observation_index: usize },
}

/// One original provider row, retained even if its raw and adjusted bar surfaces are null.
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
pub struct TiingoEodNormalizedAction {
    pub history_page_index: usize,
    pub provider_row_index: u32,
    pub row_digest: EvidenceDigest,
    pub observation: CorporateActionObservation,
}

/// Pure projection of source-known daily fields. This carries no seal token or canonical/PIT
/// authority. Its completion identity must be retained by the one existing history publisher.
#[derive(Debug)]
pub struct TiingoEodHistoryActionProjection {
    completion_identity: EvidenceDigest,
    request_set_identity: EvidenceDigest,
    rows: Box<[TiingoEodDailyActionDisposition]>,
    observations: Box<[TiingoEodNormalizedAction]>,
    cash_unit: Option<TiingoEodCashUnitEvidence>,
}
impl TiingoEodHistoryActionProjection {
    pub const fn completion_identity(&self) -> EvidenceDigest {
        self.completion_identity
    }
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }
    pub fn rows(&self) -> &[TiingoEodDailyActionDisposition] {
        &self.rows
    }
    pub fn observations(&self) -> &[TiingoEodNormalizedAction] {
        &self.observations
    }
    pub const fn cash_unit(&self) -> Option<&TiingoEodCashUnitEvidence> {
        self.cash_unit.as_ref()
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

/// Projects every source row only after the existing history producer has reconciled the exact
/// expected session set. `ordinary_fields_complete` describes these two fields, never all action
/// families. A nonzero dividend without independently evidenced currency remains unresolved.
pub fn normalize_eod_history_actions(
    history: &TiingoPendingEodHistoryPublication,
    cash_unit: Option<&TiingoEodCashUnitEvidence>,
) -> Result<TiingoEodHistoryActionProjection, TiingoEodActionError> {
    if history.financial_coverage() != TiingoEodFinancialCoverageDisposition::Complete
        || !history.missing_expected_sessions().is_empty()
    {
        return Err(TiingoEodActionError::IncompleteFinancialDates);
    }
    let count = usize::try_from(history.total_provider_actions())
        .map_err(|_| TiingoEodActionError::ResourceBound)?;
    let mut projection = ProjectionBuilder::try_new(count)?;
    for (history_page_index, page) in history.pages().iter().enumerate() {
        let context = ProjectionContext {
            instrument: page.instrument(),
            contract: page.contract(),
            received_at: page.received_at(),
            ingested_at: page.ingested_at(),
        };
        context.validate_unit(cash_unit)?;
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
    }
    projection.finish(
        history.completion_identity(),
        history.capture().plan().request_set_identity(),
        cash_unit,
    )
}

/// Bounded strict decoder output plus exact retained native membership and the original
/// normalization clock. The serving owner obtains these from the same immutable generation.
/// These values are not a publication seal or durable read capability.
#[derive(Clone, Copy, Debug)]
pub struct TiingoEodActionReplayPage<'a> {
    pub response: &'a TiingoEodReceipt,
    pub ingested_at: Timestamp,
    pub native_row_digests: &'a [EvidenceDigest],
}

/// Reprojects already verified original raw windows and checks every native date, count and row
/// digest against the same complete graph. This pure function does not mint financial coverage or
/// publication authority: the application must retain the genuine shared history read and native
/// binding, and compare these observations with that generation's canonical action rows.
///
/// `plan` is the source request plan, whose identity is distinct from the graph's admitted study
/// plan digest. No receipt clock, source unit, payable date or missing field is reconstructed from
/// the current wall clock or another price series.
pub fn rejoin_eod_history_actions(
    graph: &CompleteMarketBarDateWindowsV1,
    plan: &TiingoHistoryPlan,
    instrument: &TiingoEodInstrumentAuthority,
    contract: &TiingoEodContractEvidence,
    pages: &[TiingoEodActionReplayPage<'_>],
    cash_unit: Option<&TiingoEodCashUnitEvidence>,
) -> Result<TiingoEodHistoryActionProjection, TiingoEodActionError> {
    let normalization = graph.normalization();
    if normalization.contract_identity != contract.mapping_identity()
        || &normalization.native_schema_revision != contract.native_schema_revision()
        || &normalization.entitlement_generation != contract.entitlement_generation_identity()
        || &normalization.adjusted_surface_evidence != contract.adjusted_surface_evidence()
        || !match (&normalization.cash_unit, cash_unit) {
            (None, None) => true,
            (Some(retained), Some(unit)) => {
                retained.status == unit.status()
                    && retained.currency == unit.currency()
                    && &retained.assertion == unit.assertion()
                    && retained.available_at == unit.available_at()
            }
            _ => false,
        }
        || graph.instrument_id() != instrument.instrument_id()
        || graph.instrument_revision_digest()
            != instrument
                .instrument_definition()
                .payload_evidence()
                .content_digest()
        || graph.provider_instrument_id() != instrument.provider_instrument_id()
        || graph.venue_id() != instrument.venue_id()
        || graph.interval().as_str() != "tiingo-calendar-day"
        || graph.graph_purpose().as_str() != "tiingo-eod-complete-date-windows/v1"
        || graph.requested_dates() != plan.interval()
        || plan.ticker() != instrument.ticker()
        || pages.len() != plan.pages().len()
        || pages.len() != graph.windows().len()
        || pages.is_empty()
    {
        return Err(TiingoEodActionError::InvalidEvidence);
    }
    let mut projection = ProjectionBuilder::try_new(graph.sessions().len())?;
    let mut response_bytes = 0_u64;
    let mut session_ordinal = 0_usize;
    for (history_page_index, ((page, request), window)) in pages
        .iter()
        .zip(plan.pages())
        .zip(graph.windows())
        .enumerate()
    {
        let response = page.response;
        let evidence = response.evidence();
        let TiingoRequestScope::History {
            start_date,
            end_date,
            ..
        } = request.scope()
        else {
            return Err(TiingoEodActionError::InvalidEvidence);
        };
        if evidence.request() != request
            || window.request_identity != request.request_identity()
            || usize::from(window.component_ordinal) != history_page_index + 1
            || (window.start_date, window.end_date) != (*start_date, *end_date)
            || usize::try_from(window.first_session_ordinal).ok() != Some(session_ordinal)
            || usize::try_from(window.returned_session_count).ok() != Some(response.rows().len())
            || page.native_row_digests.len() != response.rows().len()
            || response.rows().len() > request.max_rows()
            || evidence.native_contract_revision() != contract.native_schema_revision()
            || evidence.entitlement_generation() != contract.entitlement_generation_identity()
            || !(200..300).contains(&evidence.status())
            || evidence.response_bytes() == 0
            || evidence.response_bytes() > request.max_response_bytes() as u64
            || evidence.body_digest().bytes() == [0; 32]
            || evidence.received_at() > evidence.decoded_at()
            || evidence.decoded_at() > page.ingested_at
            || evidence.decoded_at() != window.decoded_at
            || page.ingested_at != window.ingested_at
            || normalization.metadata_decoded_at > evidence.received_at()
            || instrument.resolved_at() > evidence.received_at()
            || response.disposition().response_bytes() != evidence.response_bytes()
            || usize::try_from(response.disposition().returned_rows()).ok()
                != Some(response.rows().len())
        {
            return Err(TiingoEodActionError::InvalidEvidence);
        }
        response_bytes = response_bytes
            .checked_add(evidence.response_bytes())
            .filter(|bytes| *bytes <= plan.maximum_response_bytes())
            .ok_or(TiingoEodActionError::ResourceBound)?;
        let context = ProjectionContext {
            instrument,
            contract,
            received_at: evidence.received_at(),
            ingested_at: page.ingested_at,
        };
        context.validate_unit(cash_unit)?;
        for (provider_row_index, (row, expected_digest)) in response
            .rows()
            .iter()
            .zip(page.native_row_digests)
            .enumerate()
        {
            let session = graph
                .sessions()
                .get(session_ordinal)
                .ok_or(TiingoEodActionError::InvalidEvidence)?;
            if row.date() != session.date
                || row.row_digest() != *expected_digest
                || row.date() < *start_date
                || row.date() > *end_date
                || session.time.nominal_daily_date().is_none_or(|date| date.date() != row.date())
                || session.row_digest != row.row_digest()
            {
                return Err(TiingoEodActionError::InvalidEvidence);
            }
            projection.push(
                history_page_index,
                &context,
                ActionRowValues {
                    provider_row_index: u32::try_from(provider_row_index)
                        .map_err(|_| TiingoEodActionError::ResourceBound)?,
                    date: row.date(),
                    row_digest: row.row_digest(),
                    cash_dividend: row.cash_dividend(),
                    split_factor: row.split_factor(),
                },
                cash_unit,
            )?;
            session_ordinal += 1;
        }
    }
    if session_ordinal != graph.sessions().len() {
        return Err(TiingoEodActionError::InvalidEvidence);
    }
    projection.finish(
        graph.completeness_evidence(),
        plan.request_set_identity(),
        cash_unit,
    )
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
        if count > 64_000 {
            return Err(TiingoEodActionError::ResourceBound);
        }
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
    fn finish(
        self,
        completion_identity: EvidenceDigest,
        request_set_identity: EvidenceDigest,
        cash_unit: Option<&TiingoEodCashUnitEvidence>,
    ) -> Result<TiingoEodHistoryActionProjection, TiingoEodActionError> {
        if self.rows.len() != self.count {
            return Err(TiingoEodActionError::InvalidEvidence);
        }
        Ok(TiingoEodHistoryActionProjection {
            completion_identity,
            request_set_identity,
            rows: self.rows.into_boxed_slice(),
            observations: self.observations.into_boxed_slice(),
            cash_unit: cash_unit.cloned(),
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
