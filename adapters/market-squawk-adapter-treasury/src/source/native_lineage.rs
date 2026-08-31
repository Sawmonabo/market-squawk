//! Provider-native Treasury semantics aligned to canonical extraction order.

use market_squawk_domain::{CalendarDate, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    ExtractionBatch, ProviderNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageImplementation,
};
use serde::Serialize;

use crate::{
    FiscalDataPage, FiscalDataRecord, TreasuryDailyRateFamily, TreasuryDailyRateObservation,
    TreasuryDailyRatePage, TreasuryDailyRatePeriod, TreasuryDailyRatePeriodKind,
    TreasuryDailyRatePoint, TreasuryFiscalQuery, TreasurySourceError,
};

use super::lineage::lower_hex;

#[derive(Debug)]
pub(super) enum TreasuryNativeLineagePlan {
    Fiscal {
        dataset: SourceIdentifier,
        source_identity: &'static str,
        first_record_date: CalendarDate,
        last_record_date: CalendarDate,
        page_size: u16,
        pages: Vec<FiscalDataPage>,
    },
    Daily {
        dataset: SourceIdentifier,
        page: TreasuryDailyRatePage,
    },
}

impl TreasuryNativeLineagePlan {
    pub(super) fn fiscal(dataset: SourceIdentifier, query: &TreasuryFiscalQuery) -> Self {
        Self::Fiscal {
            dataset,
            source_identity: query.source_identity(),
            first_record_date: query.first_record_date(),
            last_record_date: query.last_record_date(),
            page_size: query.page_size().get(),
            pages: Vec::new(),
        }
    }

    pub(super) fn try_push_fiscal_page(
        &mut self,
        page: FiscalDataPage,
    ) -> Result<(), TreasurySourceError> {
        let Self::Fiscal { pages, .. } = self else {
            return Err(TreasurySourceError::InvalidProtocol);
        };
        if page.page_number() != pages.len() + 1
            || pages.first().is_some_and(|first| {
                first.schema_digest() != page.schema_digest()
                    || first.total_count() != page.total_count()
                    || first.total_pages() != page.total_pages()
            })
        {
            return Err(TreasurySourceError::InvalidProtocol);
        }
        pages
            .try_reserve(1)
            .map_err(|_| TreasurySourceError::InvalidProtocol)?;
        pages.push(page);
        Ok(())
    }

    pub(super) fn try_daily(
        dataset: SourceIdentifier,
        page: TreasuryDailyRatePage,
    ) -> Result<Self, TreasurySourceError> {
        if page.dataset() != &dataset || page.observations().is_empty() {
            return Err(TreasurySourceError::InvalidProtocol);
        }
        Ok(Self::Daily { dataset, page })
    }

    pub(super) fn try_encode(
        self,
        batch: &ExtractionBatch,
    ) -> Result<(ProviderNativeLineageBatch, Vec<u16>), TreasurySourceError> {
        match self {
            Self::Fiscal {
                dataset,
                source_identity,
                first_record_date,
                last_record_date,
                page_size,
                pages,
            } => encode_fiscal(
                batch,
                &dataset,
                source_identity,
                first_record_date,
                last_record_date,
                page_size,
                &pages,
            ),
            Self::Daily { dataset, page } => encode_daily(batch, &dataset, &page),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "provider query and response semantics remain explicit"
)]
fn encode_fiscal(
    batch: &ExtractionBatch,
    dataset: &SourceIdentifier,
    source_identity: &'static str,
    first_record_date: CalendarDate,
    last_record_date: CalendarDate,
    page_size: u16,
    pages: &[FiscalDataPage],
) -> Result<(ProviderNativeLineageBatch, Vec<u16>), TreasurySourceError> {
    let first_page = pages.first().ok_or(TreasurySourceError::InvalidProtocol)?;
    let source_rows = pages.iter().try_fold(0_usize, |total, page| {
        total
            .checked_add(page.records().len())
            .ok_or(TreasurySourceError::InvalidProtocol)
    })?;
    if batch.request().object().dataset() != dataset
        || source_rows != batch.records().len()
        || pages.len() != first_page.total_pages()
        || source_rows != first_page.total_count()
    {
        return Err(TreasurySourceError::InvalidProtocol);
    }
    let mut page_semantics = Vec::new();
    page_semantics
        .try_reserve_exact(pages.len())
        .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    for page in pages {
        page_semantics.push(FiscalNativePageV1 {
            page_number: page.page_number(),
            total_count: page.total_count(),
            total_pages: page.total_pages(),
            returned: page.records().len(),
            next_page_token: page.next_page_token(),
        });
    }
    let mut builder = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::UsTreasuryMacroV1,
        batch,
    )
    .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    builder
        .try_set_batch_sidecar(&TreasuryFiscalNativeBatchV1 {
            version: 1,
            surface: "fiscal_data",
            dataset,
            profile: "average_interest_rates_v2",
            source_identity,
            first_record_date,
            last_record_date,
            page_size,
            schema: FiscalNativeSchemaV1 {
                labels: first_page.schema().labels(),
                data_types: first_page.schema().data_types(),
                data_formats: first_page.schema().data_formats(),
            },
            pages: &page_semantics,
        })
        .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let mut row_capture_page_ordinals = Vec::new();
    row_capture_page_ordinals
        .try_reserve_exact(source_rows)
        .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let mut canonical = batch.records().iter();
    for (page_ordinal, page) in pages.iter().enumerate() {
        let page_ordinal =
            u16::try_from(page_ordinal).map_err(|_| TreasurySourceError::InvalidProtocol)?;
        for record in page.records() {
            let canonical_record = canonical
                .next()
                .ok_or(TreasurySourceError::InvalidProtocol)?;
            validate_fiscal_alignment(canonical_record, record)?;
            let canonical_series = canonical_series(canonical_record)?;
            builder
                .try_push(&TreasuryFiscalNativeRowV1 {
                    row_identity: lower_hex(record.row_identity()),
                    fields: record.values(),
                    canonical_series: &canonical_series,
                })
                .map_err(|_| TreasurySourceError::InvalidProtocol)?;
            row_capture_page_ordinals.push(page_ordinal);
        }
    }
    if canonical.next().is_some() {
        return Err(TreasurySourceError::InvalidProtocol);
    }
    let lineage = builder
        .finish()
        .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    Ok((lineage, row_capture_page_ordinals))
}

fn validate_fiscal_alignment(
    canonical: &market_squawk_sources::ExtractionRecord,
    native: &FiscalDataRecord,
) -> Result<(), TreasurySourceError> {
    let record_date = native
        .get("record_date")
        .ok_or(TreasurySourceError::InvalidProtocol)
        .and_then(|value| {
            crate::rates::parse_date(value).map_err(|_| TreasurySourceError::InvalidProtocol)
        })?;
    let source_line = native
        .get("src_line_nbr")
        .ok_or(TreasurySourceError::InvalidProtocol)?;
    let expected_revision = format!(
        "treasury-fiscal-rate:{record_date}:{source_line}:{}",
        lower_hex(native.row_identity())
    );
    if canonical.effective_time().calendar_date_value() != Some(record_date)
        || canonical.published_time().is_some()
        || canonical.revision().as_str() != expected_revision
    {
        return Err(TreasurySourceError::InvalidProtocol);
    }
    Ok(())
}

fn encode_daily(
    batch: &ExtractionBatch,
    dataset: &SourceIdentifier,
    page: &TreasuryDailyRatePage,
) -> Result<(ProviderNativeLineageBatch, Vec<u16>), TreasurySourceError> {
    let native_points = page
        .observations()
        .iter()
        .try_fold(0_usize, |total, observation| {
            total
                .checked_add(observation.points().len())
                .ok_or(TreasurySourceError::InvalidProtocol)
        })?;
    if batch.request().object().dataset() != dataset
        || page.dataset() != dataset
        || native_points != batch.records().len()
    {
        return Err(TreasurySourceError::InvalidProtocol);
    }
    let mut builder = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::UsTreasuryMacroV1,
        batch,
    )
    .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    builder
        .try_set_batch_sidecar(&TreasuryDailyNativeBatchV1 {
            version: 1,
            surface: "daily_rates",
            dataset,
            family: page.family(),
            provider_key: page.family().provider_key(),
            feed_identity: page.family().feed_identity(),
            feed_title: page.family().feed_title(),
            schema_revision: page.family().schema_revision(),
            period: NativeDailyPeriodV1::from(page.period()),
            page_number: page.page_number(),
            feed_published_at: page.feed_published_at(),
            provider_rows: page.observations().len(),
            terminal_for_query: !page.period().is_all_history() || page.is_terminal(),
        })
        .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let mut row_capture_page_ordinals = Vec::new();
    row_capture_page_ordinals
        .try_reserve_exact(native_points)
        .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let mut canonical = batch.records().iter();
    for observation in page.observations() {
        for point in observation.points() {
            let canonical_record = canonical
                .next()
                .ok_or(TreasurySourceError::InvalidProtocol)?;
            validate_daily_alignment(canonical_record, observation, point)?;
            let canonical_series = canonical_series(canonical_record)?;
            builder
                .try_push(&TreasuryDailyNativeRowV1 {
                    family: observation.family(),
                    source_record_id: observation.source_record_id(),
                    record_date: observation.record_date(),
                    source_published_at: observation.source_published_at(),
                    market_unavailability_reason: observation.market_unavailability_reason(),
                    row_identity: lower_hex(observation.row_identity()),
                    point,
                    canonical_series: &canonical_series,
                })
                .map_err(|_| TreasurySourceError::InvalidProtocol)?;
            row_capture_page_ordinals.push(0);
        }
    }
    if canonical.next().is_some() {
        return Err(TreasurySourceError::InvalidProtocol);
    }
    let lineage = builder
        .finish()
        .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    Ok((lineage, row_capture_page_ordinals))
}

fn validate_daily_alignment(
    canonical: &market_squawk_sources::ExtractionRecord,
    observation: &TreasuryDailyRateObservation,
    point: &TreasuryDailyRatePoint,
) -> Result<(), TreasurySourceError> {
    let expected_revision = format!(
        "treasury-daily-rate:{}:{}:{}:{}:{}",
        observation.family().dataset_family_token(),
        observation.record_date(),
        point.metric().as_series_token(),
        observation.source_published_at().unix_nanos(),
        lower_hex(observation.row_identity()),
    );
    if canonical.effective_time().calendar_date_value() != Some(observation.record_date())
        || canonical
            .published_time()
            .and_then(|coordinate| coordinate.exact_timestamp())
            != Some(observation.source_published_at())
        || canonical.revision().as_str() != expected_revision
    {
        return Err(TreasurySourceError::InvalidProtocol);
    }
    Ok(())
}

fn canonical_series(
    record: &market_squawk_sources::ExtractionRecord,
) -> Result<SourceIdentifier, TreasurySourceError> {
    let market_squawk_domain::ResearchObservation::Macro(observation) =
        serde_json::from_slice(record.payload())
            .map_err(|_| TreasurySourceError::InvalidProtocol)?
    else {
        return Err(TreasurySourceError::InvalidProtocol);
    };
    Ok(observation.series().clone())
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryFiscalNativeBatchV1<'a> {
    version: u16,
    surface: &'static str,
    dataset: &'a SourceIdentifier,
    profile: &'static str,
    source_identity: &'static str,
    first_record_date: CalendarDate,
    last_record_date: CalendarDate,
    page_size: u16,
    schema: FiscalNativeSchemaV1<'a>,
    pages: &'a [FiscalNativePageV1<'a>],
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct FiscalNativeSchemaV1<'a> {
    labels: &'a [(String, String)],
    data_types: &'a [(String, String)],
    data_formats: &'a [(String, String)],
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct FiscalNativePageV1<'a> {
    page_number: usize,
    total_count: usize,
    total_pages: usize,
    returned: usize,
    next_page_token: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryFiscalNativeRowV1<'a> {
    row_identity: String,
    fields: &'a [(String, String)],
    canonical_series: &'a SourceIdentifier,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryDailyNativeBatchV1<'a> {
    version: u16,
    surface: &'static str,
    dataset: &'a SourceIdentifier,
    family: TreasuryDailyRateFamily,
    provider_key: &'static str,
    feed_identity: &'static str,
    feed_title: &'static str,
    schema_revision: &'static str,
    period: NativeDailyPeriodV1,
    page_number: usize,
    feed_published_at: Timestamp,
    provider_rows: usize,
    terminal_for_query: bool,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct NativeDailyPeriodV1 {
    kind: &'static str,
    year: Option<u16>,
    month: Option<u8>,
}

impl From<TreasuryDailyRatePeriod> for NativeDailyPeriodV1 {
    fn from(period: TreasuryDailyRatePeriod) -> Self {
        Self {
            kind: match period.kind() {
                TreasuryDailyRatePeriodKind::Year => "year",
                TreasuryDailyRatePeriodKind::Month => "month",
                TreasuryDailyRatePeriodKind::AllHistory => "all_history",
            },
            year: period.year_value(),
            month: period.month_value(),
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryDailyNativeRowV1<'a> {
    family: TreasuryDailyRateFamily,
    source_record_id: &'a str,
    record_date: CalendarDate,
    source_published_at: Timestamp,
    market_unavailability_reason: Option<&'a str>,
    row_identity: String,
    point: &'a TreasuryDailyRatePoint,
    canonical_series: &'a SourceIdentifier,
}
