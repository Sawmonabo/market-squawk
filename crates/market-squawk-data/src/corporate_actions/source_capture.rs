//! One closure check shared by canonical admission and exact retained capture reads.

use crate::PersistedProviderCaptureBindingEvidence;
use market_squawk_domain::{
    CalendarDate, CorporateActionEventInstrumentIdentity, CorporateActionQueryInstrumentIdentity,
    CorporateActionSourceCategory as Category, CorporateActionSourceDates,
    CorporateActionSourceDisposition as Disposition, CorporateActionSourcePayload as Payload,
    CorporateActionSourceScope, DigestAlgorithm, EvidenceDigest, ResearchObservation, Timestamp,
};
use market_squawk_sources::ProviderCaptureTerminalDisposition;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Shared source-closure failure, including wrong native version or incomplete row membership.
#[derive(Debug, thiserror::Error)]
#[error("corporate-action source capture does not reconcile with the canonical publication")]
pub struct CorporateActionSourceCaptureError;
type Error = CorporateActionSourceCaptureError;

/// No receipt is issued here. Admission calls this before the existing publisher commits;
/// retained reads call it after catalog/raw and every exact canonical row have been checked.
pub(crate) fn validate_corporate_action_source_capture(
    observations: &[ResearchObservation],
    binding: Option<&PersistedProviderCaptureBindingEvidence>,
) -> Result<(), Error> {
    if let Some(binding) = binding.filter(|binding| {
        binding.native_lineage().implementation() == "tiingo_corporate_actions_v1"
    }) {
        return super::current_ordinary::source::validate_current_ordinary_capture(
            observations,
            binding,
        );
    }
    let has_source = observations
        .iter()
        .any(|v| matches!(v, ResearchObservation::CorporateActionSource(_)));
    let is_native = binding
        .is_some_and(|b| b.native_lineage().implementation() == "alpaca_corporate_actions_v1");
    if !has_source && !is_native {
        return Ok(());
    }
    let binding = binding
        .filter(|_| is_native)
        .ok_or(CorporateActionSourceCaptureError)?;
    binding
        .verify_integrity()
        .map_err(|_| CorporateActionSourceCaptureError)?;
    if binding.scope() != "whole"
        || binding.layout() != "whole_single_segment"
        || binding.capture().terminal()
            != ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        || observations.len() != binding.record_count()
    {
        return Err(CorporateActionSourceCaptureError);
    }
    let sidecar: Sidecar = serde_json::from_slice(
        binding
            .native_lineage()
            .batch_sidecar_semantic_payload()
            .ok_or(CorporateActionSourceCaptureError)?,
    )
    .map_err(|_| CorporateActionSourceCaptureError)?;
    if sidecar.version != 1
        || sidecar.scope != "all_types_all_quality_us_processing_dates"
        || sidecar.announcement_time_supplied
        || sidecar.lifecycle_coverage_guaranteed
        || sidecar.pages.len() != binding.capture().pages().len()
        || sidecar.pages.len() > 16
    {
        return Err(CorporateActionSourceCaptureError);
    }
    let summaries: Vec<_> = observations
        .iter()
        .filter_map(|v| match v {
            ResearchObservation::CorporateActionSource(v) => match v.payload() {
                Payload::QuerySummary {
                    scope,
                    category_counts,
                    normalized_count,
                    page_count,
                } => Some((v, scope, category_counts, normalized_count, page_count)),
                _ => None,
            },
            _ => None,
        })
        .collect();
    let [(summary, scope, declared_counts, normalized_count, page_count)] = summaries.as_slice()
    else {
        return Err(CorporateActionSourceCaptureError);
    };
    if scope.dataset != *binding.capture().dataset()
        || scope.capture_observation_digest != binding.capture().observation_digest()
        || scope.sealed_capture_receipt_digest != binding.sealed_capture_receipt_digest()
        || usize::from(**page_count) != sidecar.pages.len()
        || sidecar.coverage.request.symbols
            != scope
                .symbols
                .iter()
                .map(|v| v.as_str().to_owned())
                .collect::<Vec<_>>()
        || sidecar.coverage.query_instruments != scope.query_instruments
        || scope.query_instruments.iter().any(|identity| {
            binding
                .capture()
                .pages()
                .first()
                .is_none_or(|page| !identity.valid_for_capture(page.received_at()))
        })
        || sidecar.coverage.request.process_start != scope.process_start
        || sidecar.coverage.request.process_end != scope.process_end
        || sidecar.coverage.capture_observation_digest != scope.capture_observation_digest
        || sidecar.coverage.sealed_capture_receipt_digest != scope.sealed_capture_receipt_digest
        || sidecar.coverage.category_counts != **declared_counts
        || sidecar.coverage.page_count != sidecar.pages.len()
        || sidecar.coverage.received_at != summary.context().provenance().received_at()
    {
        return Err(CorporateActionSourceCaptureError);
    }
    let base_url = request_url(scope, None)?;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/alpaca-corporate-actions/request/v1\0");
    for text in [
        binding.capture().source_id().as_str(),
        binding
            .capture()
            .metadata_revision()
            .as_source_identifier()
            .as_str(),
        base_url.as_str(),
    ] {
        hash.update(
            u32::try_from(text.len())
                .map_err(|_| CorporateActionSourceCaptureError)?
                .to_be_bytes(),
        );
        hash.update(text.as_bytes());
    }
    if digest(hash) != binding.capture().request_set_identity() {
        return Err(CorporateActionSourceCaptureError);
    }
    let mut native_actions = BTreeMap::new();
    let mut counts = [0u32; 16];
    for (page, receipt) in sidecar.pages.iter().zip(binding.capture().pages()) {
        let url = request_url(scope, page.request_page_token.as_deref())?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/alpaca-corporate-actions/page/v1\0");
        hash.update(binding.capture().request_set_identity().bytes());
        hash.update(page.ordinal.to_be_bytes());
        hash.update(url.as_str().as_bytes());
        if page.ordinal != receipt.ordinal()
            || page.request_url != url.as_str()
            || page.body_digest != receipt.body_digest()
            || page.body_bytes != receipt.body_bytes()
            || page.received_at != receipt.received_at()
            || digest(hash) != receipt.request_identity()
            || page.request_page_token.as_deref().map(sha256) != receipt.request_page_token_digest()
            || page.response_next_page_token.as_deref().map(sha256)
                != receipt.response_next_page_token_digest()
            || page.actions.len() > 1000
        {
            return Err(CorporateActionSourceCaptureError);
        }
        for action in &page.actions {
            if action.fields.process_date < scope.process_start
                || action.fields.process_date > scope.process_end
                || native_actions
                    .insert(action.fields.id, (page, action))
                    .is_some()
            {
                return Err(CorporateActionSourceCaptureError);
            }
            counts[action.category.index()] += 1;
        }
    }
    if counts != **declared_counts
        || native_actions.len() != sidecar.coverage.actions.len()
        || sidecar.pages.last().map(|p| p.received_at) != Some(sidecar.coverage.received_at)
    {
        return Err(CorporateActionSourceCaptureError);
    }
    let dispositions: BTreeMap<_, _> = sidecar
        .coverage
        .actions
        .iter()
        .map(|a| (a.action_id, a))
        .collect();
    if dispositions.len() != native_actions.len() {
        return Err(CorporateActionSourceCaptureError);
    }
    let mut source_ids = BTreeSet::new();
    let mut economic_ids = BTreeSet::new();
    let mut event_pairs = BTreeMap::new();
    for (observation, row) in observations.iter().zip(binding.rows()) {
        let (context, kind) = match observation {
            ResearchObservation::CorporateActionSource(v) => (v.context(), "source_disposition"),
            ResearchObservation::CorporateAction(v) => (v.context(), "economic"),
            _ => return Err(CorporateActionSourceCaptureError),
        };
        let provenance = context.provenance();
        if provenance.source_id() != binding.capture().source_id()
            || provenance.quality() != market_squawk_domain::DataQuality::Aggregated
            || provenance.payload_reference()
                != &market_squawk_domain::PayloadReference::ContentHash(
                    market_squawk_domain::PayloadHash::new(
                        row.page_body_digest().algorithm(),
                        row.page_body_digest().bytes(),
                    ),
                )
            || provenance.received_at() != row.received_at()
            || provenance.source_timestamp().is_some()
            || provenance.availability().conservative_available_at() != Some(row.received_at())
        {
            return Err(CorporateActionSourceCaptureError);
        }
        if let ResearchObservation::CorporateActionSource(v) = observation
            && matches!(v.payload(), Payload::QuerySummary { .. })
        {
            let native: NativeSummary = serde_json::from_slice(row.native_semantic_payload())
                .map_err(|_| CorporateActionSourceCaptureError)?;
            if native.row_kind != "query_summary"
                || native.capture_observation_digest != binding.capture().observation_digest()
                || row.capture_page_ordinal()
                    != u16::try_from(sidecar.pages.len() - 1)
                        .map_err(|_| CorporateActionSourceCaptureError)?
            {
                return Err(CorporateActionSourceCaptureError);
            }
            continue;
        }
        let native: NativeRow = serde_json::from_slice(row.native_semantic_payload())
            .map_err(|_| CorporateActionSourceCaptureError)?;
        let id = native.action.fields.id;
        let (page, action) = native_actions
            .get(&id)
            .ok_or(CorporateActionSourceCaptureError)?;
        let disposition = dispositions
            .get(&id)
            .ok_or(CorporateActionSourceCaptureError)?;
        if native.row_kind != kind
            || native.page_ordinal != page.ordinal
            || native.action != **action
            || native.subject != provenance.instrument_id()
            || native.subject
                != native
                    .subject_identity
                    .as_ref()
                    .map(|identity| identity.selection.instrument_id)
            || native.related
                != native
                    .related_identity
                    .as_ref()
                    .map(|identity| identity.selection.instrument_id)
            || native
                .subject_identity
                .iter()
                .chain(native.related_identity.iter())
                .any(|identity| {
                    identity.source_id != *binding.capture().source_id()
                        || !identity.valid_for_event(provenance.ingested_at())
                })
            || native.subject_identity.as_ref().is_some_and(|identity| {
                Some(identity.provider_instrument_id.as_str()) != action.subject_symbol()
            })
            || row.capture_page_ordinal() != page.ordinal
            || disposition.category != action.category
            || disposition.instrument_id != native.subject
            || disposition.effective_date != action.fields.dates().economic_date()
            || disposition.received_at != page.received_at
            || provenance.source_identifier().as_str() != format!("alpaca:corporate-action:{id}")
        {
            return Err(CorporateActionSourceCaptureError);
        }
        let event_pair = (
            native.subject_identity.as_ref(),
            native.related_identity.as_ref(),
        );
        let pair_bytes =
            serde_json::to_vec(&event_pair).map_err(|_| CorporateActionSourceCaptureError)?;
        let pair_digest: [u8; 32] = Sha256::digest(&pair_bytes).into();
        if event_pairs
            .insert(id, pair_digest)
            .is_some_and(|prior| prior != pair_digest)
        {
            return Err(CorporateActionSourceCaptureError);
        }
        if event_pair.1.is_some_and(|identity| {
            Some(identity.provider_instrument_id.as_str()) != action.related_symbol()
        }) || ((event_pair.0.is_some() || event_pair.1.is_some())
            && action.fields.dates().economic_date().is_none())
        {
            return Err(CorporateActionSourceCaptureError);
        }
        match observation {
            ResearchObservation::CorporateActionSource(v) => {
                let Payload::ReturnedAction {
                    category,
                    dates,
                    currency,
                    disposition: observed,
                    subject_symbol,
                    ..
                } = v.payload()
                else {
                    return Err(CorporateActionSourceCaptureError);
                };
                if !source_ids.insert(id)
                    || *category != action.category
                    || *dates != action.fields.dates()
                    || *currency
                        != action
                            .fields
                            .currency
                            .as_deref()
                            .and_then(|value| market_squawk_domain::Currency::try_from(value).ok())
                    || *observed != disposition.disposition
                    || subject_symbol.as_ref().map(|v| v.as_str()) != action.subject_symbol()
                {
                    return Err(CorporateActionSourceCaptureError);
                }
            }
            ResearchObservation::CorporateAction(v) => {
                if !economic_ids.insert(id)
                    || disposition.disposition != Disposition::Normalized
                    || v.context().time().effective().calendar_date_value()
                        != action.fields.dates().economic_date()
                {
                    return Err(CorporateActionSourceCaptureError);
                }
            }
            _ => unreachable!(),
        }
    }
    if source_ids.len() != native_actions.len()
        || economic_ids.len()
            != usize::try_from(**normalized_count).map_err(|_| CorporateActionSourceCaptureError)?
        || dispositions.values().any(|v| {
            (v.disposition == Disposition::Normalized) != economic_ids.contains(&v.action_id)
        })
    {
        return Err(CorporateActionSourceCaptureError);
    }
    Ok(())
}

fn request_url(scope: &CorporateActionSourceScope, token: Option<&str>) -> Result<url::Url, Error> {
    let mut url = url::Url::parse("https://data.alpaca.markets/v1/corporate-actions")
        .map_err(|_| CorporateActionSourceCaptureError)?;
    url.query_pairs_mut()
        .append_pair(
            "symbols",
            &scope
                .symbols
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(","),
        )
        .append_pair("start", &scope.process_start.to_string())
        .append_pair("end", &scope.process_end.to_string())
        .append_pair("region", "us")
        .append_pair("data_quality", "all")
        .append_pair("limit", "1000")
        .append_pair("sort", "asc");
    if let Some(token) = token {
        if token.is_empty() || token.len() > 2048 {
            return Err(CorporateActionSourceCaptureError);
        }
        url.query_pairs_mut().append_pair("page_token", token);
    }
    Ok(url)
}
fn digest(hash: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}
fn sha256(value: &str) -> EvidenceDigest {
    EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(value.as_bytes()).into(),
    )
}

// These are bounded, read-only projections of the registered native27 envelope. The original
// sealed native bytes remain the authority; unused financial fields stay retained unchanged.
#[derive(Deserialize)]
struct Sidecar {
    version: u16,
    scope: String,
    announcement_time_supplied: bool,
    lifecycle_coverage_guaranteed: bool,
    coverage: Coverage,
    pages: Vec<Page>,
}
#[derive(Deserialize)]
struct Coverage {
    request: Request,
    query_instruments: Vec<CorporateActionQueryInstrumentIdentity>,
    capture_observation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    category_counts: [u32; 16],
    actions: Vec<DispositionRow>,
    page_count: usize,
    received_at: Timestamp,
}
#[derive(Deserialize)]
struct Request {
    symbols: Vec<String>,
    process_start: CalendarDate,
    process_end: CalendarDate,
}
#[derive(Deserialize)]
struct DispositionRow {
    action_id: Uuid,
    category: Category,
    instrument_id: Option<market_squawk_domain::InstrumentId>,
    disposition: Disposition,
    effective_date: Option<CalendarDate>,
    received_at: Timestamp,
}
#[derive(Deserialize)]
struct Page {
    ordinal: u16,
    request_url: String,
    request_page_token: Option<String>,
    response_next_page_token: Option<String>,
    body_digest: EvidenceDigest,
    body_bytes: u64,
    received_at: Timestamp,
    actions: Vec<NativeAction>,
}
#[derive(Deserialize)]
struct NativeSummary {
    row_kind: String,
    capture_observation_digest: EvidenceDigest,
}
#[derive(Deserialize)]
struct NativeRow {
    row_kind: String,
    page_ordinal: u16,
    action: NativeAction,
    subject: Option<market_squawk_domain::InstrumentId>,
    related: Option<market_squawk_domain::InstrumentId>,
    subject_identity: Option<CorporateActionEventInstrumentIdentity>,
    related_identity: Option<CorporateActionEventInstrumentIdentity>,
}
#[derive(Deserialize, PartialEq)]
struct NativeAction {
    category: Category,
    fields: Fields,
}
#[derive(Deserialize, PartialEq)]
struct Fields {
    id: Uuid,
    process_date: CalendarDate,
    ex_date: Option<CalendarDate>,
    effective_date: Option<CalendarDate>,
    record_date: Option<CalendarDate>,
    payable_date: Option<CalendarDate>,
    due_bill_on_date: Option<CalendarDate>,
    due_bill_off_date: Option<CalendarDate>,
    due_bill_redemption_date: Option<CalendarDate>,
    expiration_date: Option<CalendarDate>,
    currency: Option<String>,
    symbol: Option<String>,
    old_symbol: Option<String>,
    source_symbol: Option<String>,
    acquiree_symbol: Option<String>,
    new_symbol: Option<String>,
    acquirer_symbol: Option<String>,
}
impl Fields {
    fn dates(&self) -> CorporateActionSourceDates {
        CorporateActionSourceDates {
            process_date: self.process_date,
            ex_date: self.ex_date,
            effective_date: self.effective_date,
            record_date: self.record_date,
            payable_date: self.payable_date,
            due_bill_on_date: self.due_bill_on_date,
            due_bill_off_date: self.due_bill_off_date,
            due_bill_redemption_date: self.due_bill_redemption_date,
            expiration_date: self.expiration_date,
        }
    }
}
impl NativeAction {
    fn related_symbol(&self) -> Option<&str> {
        match self.category {
            Category::CashMerger | Category::StockMerger | Category::StockAndCashMerger => {
                self.fields.acquirer_symbol.as_deref()
            }
            Category::SpinOff
            | Category::ReverseSplit
            | Category::UnitSplit
            | Category::NameChange
            | Category::RightsDistribution => self.fields.new_symbol.as_deref(),
            _ => None,
        }
        .filter(|symbol| !symbol.is_empty())
    }
    fn subject_symbol(&self) -> Option<&str> {
        match self.category {
            Category::UnitSplit | Category::NameChange => self.fields.old_symbol.as_deref(),
            Category::SpinOff | Category::RightsDistribution => {
                self.fields.source_symbol.as_deref()
            }
            Category::CashMerger | Category::StockMerger | Category::StockAndCashMerger => {
                self.fields.acquiree_symbol.as_deref()
            }
            _ => self.fields.symbol.as_deref(),
        }
    }
}

/// Inert original event selections retained in the registered native rows. The catalog query
/// reader must replay every value before source coverage can be served.
pub(crate) fn retained_corporate_action_event_identities<'a>(
    payloads: impl Iterator<Item = &'a [u8]>,
) -> Result<Box<[CorporateActionEventInstrumentIdentity]>, Error> {
    let mut identities = BTreeMap::new();
    for payload in payloads {
        let row: EventIdentityRow =
            serde_json::from_slice(payload).map_err(|_| CorporateActionSourceCaptureError)?;
        if !matches!(
            row.row_kind.as_str(),
            "query_summary" | "economic" | "source_disposition"
        ) {
            return Err(CorporateActionSourceCaptureError);
        }
        if row.row_kind == "query_summary"
            && (row.subject_identity.is_some() || row.related_identity.is_some())
        {
            return Err(CorporateActionSourceCaptureError);
        }
        for identity in row.subject_identity.into_iter().chain(row.related_identity) {
            let key = identity.selection.selection_digest.bytes();
            if let Some(prior) = identities.get(&key) {
                if prior != &identity {
                    return Err(CorporateActionSourceCaptureError);
                }
            } else {
                if identities.len() >= 32_000 {
                    return Err(CorporateActionSourceCaptureError);
                }
                identities.insert(key, identity);
            }
        }
    }
    Ok(identities.into_values().collect())
}

#[derive(Deserialize)]
struct EventIdentityRow {
    row_kind: String,
    subject_identity: Option<CorporateActionEventInstrumentIdentity>,
    related_identity: Option<CorporateActionEventInstrumentIdentity>,
}

/// Original civil-date/identity pairs from actual source-disposition rows, never reconstructed
/// from caller filters. The application joins each date back to its genuine calendar session.
pub(crate) fn retained_corporate_action_event_identity_dates<'a>(
    payloads: impl Iterator<Item = &'a [u8]>,
    identities: &[CorporateActionEventInstrumentIdentity],
) -> Result<Box<[(CalendarDate, usize)]>, Error> {
    let mut dated = Vec::new();
    for payload in payloads {
        let row: EventIdentityRow =
            serde_json::from_slice(payload).map_err(|_| CorporateActionSourceCaptureError)?;
        if row.row_kind != "source_disposition" {
            continue;
        }
        let native: NativeRow =
            serde_json::from_slice(payload).map_err(|_| CorporateActionSourceCaptureError)?;
        for identity in native
            .subject_identity
            .into_iter()
            .chain(native.related_identity)
        {
            let date = native
                .action
                .fields
                .dates()
                .economic_date()
                .ok_or(CorporateActionSourceCaptureError)?;
            if dated.len() >= 32_000 {
                return Err(CorporateActionSourceCaptureError);
            }
            dated
                .try_reserve(1)
                .map_err(|_| CorporateActionSourceCaptureError)?;
            let index = identities
                .binary_search_by_key(&identity.selection.selection_digest.bytes(), |value| {
                    value.selection.selection_digest.bytes()
                })
                .map_err(|_| CorporateActionSourceCaptureError)?;
            if identities[index] != identity {
                return Err(CorporateActionSourceCaptureError);
            }
            dated.push((date, index));
        }
    }
    Ok(dated.into_boxed_slice())
}
