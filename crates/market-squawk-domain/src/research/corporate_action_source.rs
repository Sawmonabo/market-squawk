//! Source-query evidence. These values become coverage authority only after sealed publication
//! and an exact manifest/native/raw read; they never assert absence outside the captured query.

use crate::{
    CalendarDate, Currency, EvidenceDigest, InstrumentId, ResearchContext, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use serde::{Deserialize, Serialize};

/// Reviewed source taxonomy, preserving distinctions that are not interchangeable economics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorporateActionSourceCategory {
    ReverseSplit,
    ForwardSplit,
    UnitSplit,
    CashDividend,
    StockDividend,
    SpinOff,
    CashMerger,
    StockMerger,
    StockAndCashMerger,
    Redemption,
    NameChange,
    WorthlessRemoval,
    RightsDistribution,
    PartialCall,
    Reorganization,
    CapitalGainsDistribution,
}
impl CorporateActionSourceCategory {
    /// Closed, stable count ordering used by the source summary.
    pub const ALL: [Self; 16] = [
        Self::ReverseSplit,
        Self::ForwardSplit,
        Self::UnitSplit,
        Self::CashDividend,
        Self::StockDividend,
        Self::SpinOff,
        Self::CashMerger,
        Self::StockMerger,
        Self::StockAndCashMerger,
        Self::Redemption,
        Self::NameChange,
        Self::WorthlessRemoval,
        Self::RightsDistribution,
        Self::PartialCall,
        Self::Reorganization,
        Self::CapitalGainsDistribution,
    ];
    /// Stable index into the corresponding sixteen-element count vector.
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Exact reason why source terms have or have not produced a canonical economic event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorporateActionSourceDisposition {
    /// Source explicitly cancelled the event; no economic effect is inferred.
    Cancelled,
    Normalized,
    MissingIdentity,
    MissingEffectiveDate,
    MissingCurrency,
    MissingOrInvalidTerms,
    UnsupportedCanonicalEconomics,
    DueBillEntitlementRequired,
}

/// Independently reported dates. Processing/payment never stand in for ex/effective dates.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionSourceDates {
    /// Native processing date used by the query filter.
    pub process_date: CalendarDate,
    /// Native ex-entitlement date, when supplied.
    pub ex_date: Option<CalendarDate>,
    /// Independent native economic effective date, when supplied.
    pub effective_date: Option<CalendarDate>,
    /// Native shareholder record date.
    pub record_date: Option<CalendarDate>,
    /// Native cash or security payment date.
    pub payable_date: Option<CalendarDate>,
    /// Native start of the due-bill entitlement interval.
    pub due_bill_on_date: Option<CalendarDate>,
    /// Native end of the due-bill entitlement interval.
    pub due_bill_off_date: Option<CalendarDate>,
    /// Native due-bill redemption date.
    pub due_bill_redemption_date: Option<CalendarDate>,
    /// Native rights expiration date.
    pub expiration_date: Option<CalendarDate>,
}
impl CorporateActionSourceDates {
    /// Source ex-date takes priority when the native category supplies it.
    pub const fn economic_date(self) -> Option<CalendarDate> {
        match self.ex_date {
            Some(date) => Some(date),
            None => self.effective_date,
        }
    }
}

/// Inert query-level reference coordinates retained for every selected symbol, including a
/// terminal response with zero returned actions. Only replay of the exact catalog selection can
/// turn these values back into authority. Current selection does not imply historical aliases.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionQueryInstrumentIdentity {
    pub symbol: SourceIdentifier,
    pub instrument_id: InstrumentId,
    pub knowledge_at: Timestamp,
    pub effective_at: Timestamp,
    pub definition_revision_digest: EvidenceDigest,
    pub definition_revision_sequence: u32,
    pub definition_published_at: Timestamp,
    pub definition_reference_revision: crate::MetadataRevision,
    pub definition_reference_payload_digest: EvidenceDigest,
    pub provider_identity_revision: crate::MetadataRevision,
    pub provider_identity_payload_digest: EvidenceDigest,
    pub provider_identity_validity: crate::EffectiveInterval,
    pub selection_digest: EvidenceDigest,
}
impl CorporateActionQueryInstrumentIdentity {
    /// Historical economic query aliases must contain the actual selected economic instant,
    /// not the later acquisition clock. Exact catalog replay remains independently mandatory.
    pub fn valid_for_economic_capture(&self, received_at: Timestamp) -> bool {
        self.effective_at <= self.knowledge_at
            && self.knowledge_at <= received_at
            && self.definition_published_at <= self.knowledge_at
            && self.definition_revision_sequence > 0
            && self.provider_identity_validity.starts_at() <= self.effective_at
            && self.provider_identity_validity.ends_at().is_none_or(|end| self.effective_at < end)
            && [self.definition_revision_digest, self.definition_reference_payload_digest,
                self.provider_identity_payload_digest, self.selection_digest]
                .into_iter().all(|digest| digest.algorithm() == crate::DigestAlgorithm::Sha256 && digest.bytes() != [0; 32])
    }
    /// Value validation only; the catalog reader must reproduce the original opaque selection.
    pub fn valid_for_capture(&self, received_at: Timestamp) -> bool {
        self.effective_at <= self.knowledge_at
            && self.knowledge_at <= received_at
            && self.definition_published_at <= self.knowledge_at
            && self.definition_revision_sequence > 0
            && self.provider_identity_validity.starts_at() <= self.effective_at
            && self
                .provider_identity_validity
                .ends_at()
                .is_none_or(|end| received_at < end)
            && [
                self.definition_revision_digest,
                self.definition_reference_payload_digest,
                self.provider_identity_payload_digest,
                self.selection_digest,
            ]
            .into_iter()
            .all(|digest| {
                digest.algorithm() == crate::DigestAlgorithm::Sha256 && digest.bytes() != [0; 32]
            })
    }
}

/// Original catalog coordinates for a source-date action instrument. These are inert retained
/// values. Only the catalog authority can replay the exact selection before publication/reuse.
/// Event identity is resolved after the source page arrives, so its knowledge clock is bounded
/// by ingestion rather than rewritten to the native page receipt. Historical aliases may expire
/// before acquisition; their validity must contain the economic coordinate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionEventInstrumentIdentity {
    pub source_id: crate::SourceId,
    pub provider_instrument_id: crate::ProviderInstrumentId,
    pub venue_id: crate::VenueId,
    pub venue_symbol: crate::VenueSymbol,
    pub selection: CorporateActionQueryInstrumentIdentity,
    pub resolution_receipt_digest: EvidenceDigest,
}
impl CorporateActionEventInstrumentIdentity {
    /// Checks representation and clocks only; this method cannot mint catalog authority.
    pub fn valid_for_event(&self, ingested_at: Timestamp) -> bool {
        let selection = &self.selection;
        selection.symbol.as_str() == self.provider_instrument_id.as_str()
            && self.venue_symbol.as_str() == self.provider_instrument_id.as_str()
            && selection.effective_at <= selection.knowledge_at
            && selection.knowledge_at <= ingested_at
            && selection.definition_published_at <= selection.knowledge_at
            && selection.definition_revision_sequence > 0
            && selection.provider_identity_validity.starts_at() <= selection.effective_at
            && selection
                .provider_identity_validity
                .ends_at()
                .is_none_or(|end| selection.effective_at < end)
            && [
                selection.definition_revision_digest,
                selection.definition_reference_payload_digest,
                selection.provider_identity_payload_digest,
                selection.selection_digest,
                self.resolution_receipt_digest,
            ]
            .into_iter()
            .all(|digest| {
                digest.algorithm() == crate::DigestAlgorithm::Sha256 && digest.bytes() != [0; 32]
            })
    }
}

/// All-query native selection coordinates. Symbols are source filters, not canonical identities.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionSourceScope {
    /// Exact provider query dataset identity.
    pub dataset: SourceIdentifier,
    /// Explicit inclusive processing-date query start.
    pub process_start: CalendarDate,
    /// Explicit inclusive processing-date query end.
    pub process_end: CalendarDate,
    /// Sorted exact provider symbol filters, without canonical identity inference.
    pub symbols: Vec<SourceIdentifier>,
    /// Exact original catalog selection for each filter, independently of returned events.
    pub query_instruments: Vec<CorporateActionQueryInstrumentIdentity>,
    /// Closed reviewed provider request contract.
    pub query_contract: CorporateActionSourceQueryContract,
    /// Exact received page graph identity.
    pub capture_observation_digest: EvidenceDigest,
    /// Exact physical capture-seal identity.
    pub sealed_capture_receipt_digest: EvidenceDigest,
}

/// Reviewed request semantics; there is no user-selected complete flag.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorporateActionSourceQueryContract {
    /// All sixteen reviewed types, all quality, US, inclusive processing dates, terminal paging.
    AlpacaAllTypesAllQualityUsProcessDatesV1,
}

/// Closed ordinary economic-date queries, distinct from processing-date announcements.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorporateActionEconomicQueryContract {
    /// Exact ticker and inclusive startExDate/endExDate, actual complete response.
    TiingoDistributionsTickerExDatesV1,
    /// All-symbol exact exDate batch; every returned native row remains retained.
    TiingoSplitsAllSymbolsExDateV1,
}
/// Exact source decimal terms retained independently of financial admission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "family", rename_all = "snake_case")]
pub enum CorporateActionEconomicTerms {
    Split { split_from: rust_decimal::Decimal, split_to: rust_decimal::Decimal, split_factor: rust_decimal::Decimal },
    Distribution { distribution: rust_decimal::Decimal },
}
impl CorporateActionEconomicTerms {
    /// Normalizes only exact supported source economics. Currency is provider-declared evidence;
    /// passing a listing/quote currency is not an admissible source-unit rejoin.
    pub fn normalize_kind(&self, currency: Option<Currency>, native_status: Option<&SourceIdentifier>) -> Result<crate::CorporateActionKind, CorporateActionSourceDisposition> {
        use CorporateActionSourceDisposition as D;
        let status = native_status.map(SourceIdentifier::as_str).ok_or(D::MissingOrInvalidTerms)?;
        if status == "c" { return Err(D::Cancelled); }
        match self {
            Self::Split { split_from, split_to, split_factor } => {
                if status != "a" || *split_from <= rust_decimal::Decimal::ZERO || *split_to <= rust_decimal::Decimal::ZERO || *split_factor <= rust_decimal::Decimal::ZERO
                    || split_from.checked_mul(*split_factor) != Some(*split_to)
                { return Err(D::MissingOrInvalidTerms); }
                let from = split_from.normalize(); let to = split_to.normalize();
                let mut numerator = u128::try_from(to.mantissa()).map_err(|_| D::MissingOrInvalidTerms)?;
                let mut denominator = u128::try_from(from.mantissa()).map_err(|_| D::MissingOrInvalidTerms)?;
                let common = gcd(numerator, denominator); numerator /= common; denominator /= common;
                let mut up = 10_u128.checked_pow(from.scale()).ok_or(D::MissingOrInvalidTerms)?;
                let mut down = 10_u128.checked_pow(to.scale()).ok_or(D::MissingOrInvalidTerms)?;
                let common = gcd(up, down); up /= common; down /= common;
                let common = gcd(numerator, down); numerator /= common; down /= common;
                let common = gcd(denominator, up); denominator /= common; up /= common;
                let numerator = numerator.checked_mul(up).and_then(|n| u32::try_from(n).ok()).and_then(std::num::NonZeroU32::new).ok_or(D::MissingOrInvalidTerms)?;
                let denominator = denominator.checked_mul(down).and_then(|n| u32::try_from(n).ok()).and_then(std::num::NonZeroU32::new).ok_or(D::MissingOrInvalidTerms)?;
                Ok(crate::CorporateActionKind::Split { numerator, denominator })
            }
            Self::Distribution { distribution } => {
                if !["w", "bm", "m", "tm", "q", "sa", "a", "ir", "f", "u"].contains(&status) || *distribution <= rust_decimal::Decimal::ZERO { return Err(D::MissingOrInvalidTerms); }
                let currency = currency.ok_or(D::MissingCurrency)?;
                Ok(crate::CorporateActionKind::CashDividend { amount: crate::Money::new(*distribution, currency) })
            }
        }
    }
}
fn gcd(mut a: u128, mut b: u128) -> u128 { while b != 0 { let remainder = a % b; a = b; b = remainder; } a }

/// Inert exact economic query/capture coordinates. Only original physical source reread can
/// certify terminal response completeness; these values alone confer no financial admission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionEconomicSourceScope {
    pub dataset: SourceIdentifier,
    pub ex_date_start: CalendarDate,
    pub ex_date_end: CalendarDate,
    pub query_contract: CorporateActionEconomicQueryContract,
    /// Actual canonical subject selections. Batch requests remain all-symbol queries even when
    /// the application selected one instrument; this field never alters the HTTP filter scope.
    pub query_instruments: Vec<CorporateActionQueryInstrumentIdentity>,
    pub capture_observation_digest: EvidenceDigest,
    pub sealed_capture_receipt_digest: EvidenceDigest,
}

/// One true summary row plus one disposition row for every returned native action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum CorporateActionSourcePayload {
    EconomicQuerySummary {
        scope: CorporateActionEconomicSourceScope,
        returned_count: u32,
        normalized_count: u32,
    },
    EconomicReturnedAction {
        action_id: SourceIdentifier,
        query_contract: CorporateActionEconomicQueryContract,
        native_symbol: SourceIdentifier,
        native_perma_ticker: Option<SourceIdentifier>,
        ex_date: CalendarDate,
        payable_date: Option<CalendarDate>,
        native_status: Option<SourceIdentifier>,
        native_terms: CorporateActionEconomicTerms,
        native_row_digest: EvidenceDigest,
        currency: Option<Currency>,
        disposition: CorporateActionSourceDisposition,
    },
    QuerySummary {
        scope: CorporateActionSourceScope,
        category_counts: [u32; 16],
        normalized_count: u32,
        page_count: u16,
    },
    ReturnedAction {
        action_id: SourceIdentifier,
        category: CorporateActionSourceCategory,
        subject_symbol: Option<SourceIdentifier>,
        dates: CorporateActionSourceDates,
        currency: Option<Currency>,
        disposition: CorporateActionSourceDisposition,
    },
}

/// Canonical source evidence independent of economic-event admission. Constructor validates
/// values only: the data publisher and retained reader alone mint complete-query receipts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "CorporateActionSourceObservationInput")]
pub struct CorporateActionSourceObservation {
    context: ResearchContext,
    payload: CorporateActionSourcePayload,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionSourceObservationInput {
    /// Actual source, local-clock, and raw-payload evidence.
    pub context: ResearchContext,
    /// Source query summary or individual disposition, independent of accounting admission.
    pub payload: CorporateActionSourcePayload,
}
impl CorporateActionSourceObservation {
    /// Checks the source coordinate and chronological invariants without granting coverage authority.
    pub fn try_new(
        input: CorporateActionSourceObservationInput,
    ) -> Result<Self, CorporateActionSourceError> {
        let p = input.context.provenance();
        if p.source_timestamp().is_some()
            || input.context.time().published().is_some()
            || p.availability().conservative_available_at() != Some(p.received_at())
            || p.ingested_at() < p.received_at()
        {
            return Err(CorporateActionSourceError);
        }
        let date = match &input.payload {
            CorporateActionSourcePayload::EconomicQuerySummary { scope, returned_count, normalized_count } => {
                if p.instrument_id().is_some() || scope.ex_date_start > scope.ex_date_end
                    || scope.query_instruments.is_empty() || scope.query_instruments.len() > 32
                    || scope.query_instruments.iter().any(|identity| !identity.valid_for_economic_capture(p.received_at()))
                    || scope.query_instruments.iter().enumerate().any(|(index, identity)| scope.query_instruments[..index].iter().any(|prior| prior.instrument_id == identity.instrument_id))
                    || (scope.query_contract == CorporateActionEconomicQueryContract::TiingoDistributionsTickerExDatesV1 && scope.query_instruments.len() != 1)
                    || (scope.query_contract == CorporateActionEconomicQueryContract::TiingoSplitsAllSymbolsExDateV1 && scope.ex_date_start != scope.ex_date_end)
                    || *returned_count > 16_000 || normalized_count > returned_count
                    || [scope.capture_observation_digest, scope.sealed_capture_receipt_digest].iter().any(|digest| digest.algorithm() != crate::DigestAlgorithm::Sha256 || digest.bytes() == [0;32])
                { return Err(CorporateActionSourceError); }
                scope.ex_date_start
            }
            CorporateActionSourcePayload::EconomicReturnedAction { action_id, query_contract, ex_date, payable_date, native_status, native_terms, native_row_digest, currency, disposition, .. } => {
                let family_matches = matches!((query_contract, native_terms),
                    (CorporateActionEconomicQueryContract::TiingoDistributionsTickerExDatesV1, CorporateActionEconomicTerms::Distribution { .. })
                    | (CorporateActionEconomicQueryContract::TiingoSplitsAllSymbolsExDateV1, CorporateActionEconomicTerms::Split { .. }));
                let normalized = native_terms.normalize_kind(*currency, native_status.as_ref());
                if action_id != p.source_identifier() || !family_matches
                    || native_row_digest.algorithm() != crate::DigestAlgorithm::Sha256 || native_row_digest.bytes() == [0;32]
                    || (*disposition == CorporateActionSourceDisposition::Normalized && (p.instrument_id().is_none() || normalized.is_err()))
                    || (*disposition == CorporateActionSourceDisposition::Cancelled && !matches!(normalized, Err(CorporateActionSourceDisposition::Cancelled)))
                    || payable_date.is_some_and(|payable| payable < *ex_date)
                { return Err(CorporateActionSourceError); }
                *ex_date
            }
            CorporateActionSourcePayload::QuerySummary {
                scope,
                category_counts,
                normalized_count,
                page_count,
            } => {
                let count = category_counts
                    .iter()
                    .try_fold(0u32, |n, v| n.checked_add(*v))
                    .ok_or(CorporateActionSourceError)?;
                if p.instrument_id().is_some()
                    || scope.process_start > scope.process_end
                    || scope.symbols.is_empty()
                    || scope.symbols.len() > 32
                    || scope
                        .symbols
                        .windows(2)
                        .any(|v| v[0].as_str() >= v[1].as_str())
                    || scope.query_instruments.len() != scope.symbols.len()
                    || scope.query_instruments.iter().zip(&scope.symbols).any(
                        |(identity, symbol)| {
                            &identity.symbol != symbol
                                || !identity.valid_for_capture(p.received_at())
                        },
                    )
                    || scope
                        .query_instruments
                        .iter()
                        .enumerate()
                        .any(|(index, identity)| {
                            scope.query_instruments[..index]
                                .iter()
                                .any(|prior| prior.instrument_id == identity.instrument_id)
                        })
                    || *page_count == 0
                    || *page_count > 16
                    || count > 16_000
                    || *normalized_count > count
                    || scope.capture_observation_digest.bytes() == [0; 32]
                    || scope.sealed_capture_receipt_digest.bytes() == [0; 32]
                {
                    return Err(CorporateActionSourceError);
                }
                scope.process_start
            }
            CorporateActionSourcePayload::ReturnedAction {
                action_id,
                dates,
                disposition,
                ..
            } => {
                if action_id != p.source_identifier()
                    || (*disposition == CorporateActionSourceDisposition::Normalized
                        && (p.instrument_id().is_none() || dates.economic_date().is_none()))
                {
                    return Err(CorporateActionSourceError);
                }
                dates.process_date
            }
        };
        if input.context.time().effective().calendar_date_value() != Some(date) {
            return Err(CorporateActionSourceError);
        }
        Ok(Self {
            context: input.context,
            payload: input.payload,
        })
    }
    /// Returns the unchanged source and actual local-clock context.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }
    /// Returns the exact summary or returned-action disposition.
    pub const fn payload(&self) -> &CorporateActionSourcePayload {
        &self.payload
    }
    /// Rebinds only the locally assigned canonical revision.
    pub fn with_revision(&self, revision: RevisionNumber) -> Self {
        Self {
            context: self.context.with_revision(revision),
            payload: self.payload.clone(),
        }
    }
}
impl TryFrom<CorporateActionSourceObservationInput> for CorporateActionSourceObservation {
    type Error = CorporateActionSourceError;
    fn try_from(value: CorporateActionSourceObservationInput) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorporateActionSourceError;
impl std::fmt::Display for CorporateActionSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("corporate-action source evidence is inconsistent")
    }
}
impl std::error::Error for CorporateActionSourceError {}
