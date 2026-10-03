//! Source-owned non-Level-One projection after the original Streamer frame is physically sealed.
use crate::streamer_publication::SchwabStreamerPublicationError as Error;
use crate::streamer_screener::SchwabStreamerScreenerField as ItemField;
use crate::{
    MarketDataService, NativeScalar, SchwabCanonicalStreamerRecord, SchwabMarketDataQualification,
    SchwabStreamerFieldDictionary, SchwabStreamerSemanticField as Field, StreamerNativeValue,
};
use market_squawk_domain::{
    AssetClass, LiveEventClass, LiveProvenance, MarketDataBookEvent, MarketDataBookInput,
    MarketDataBookLevel, MarketDataBookParticipant, MarketDataChartCompletion,
    MarketDataChartEvent, MarketDataChartInput, MarketDataChartTimestampBasis, MarketDataReference,
    MarketDataReported, MarketDataScreenerEvent, MarketDataScreenerInput, MarketDataScreenerItem,
    MarketDataScreenerSort, MarketDataSizeUnit, MarketEvent, Money, ProviderInstrumentId,
    SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

/// Exact source coordinate and independent identity/provenance authorities; no caller-authored prices.
#[derive(Debug)]
pub struct SchwabStreamerFamilyRecordRequest {
    pub(crate) frame_ordinal: u16,
    pub(crate) data_batch_ordinal: u16,
    pub(crate) content_ordinal: u16,
    pub(crate) dictionary: SchwabStreamerFieldDictionary,
    pub(crate) reference: Option<MarketDataReference>,
    pub(crate) item_references: BTreeMap<ProviderInstrumentId, MarketDataReference>,
    pub(crate) provenance: LiveProvenance,
    pub(crate) qualification: SchwabMarketDataQualification,
}
impl SchwabStreamerFamilyRecordRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "original coordinate, source evidence and independent identities remain explicit"
    )]
    pub fn try_new(
        frame_ordinal: u16,
        data_batch_ordinal: u16,
        content_ordinal: u16,
        dictionary: SchwabStreamerFieldDictionary,
        reference: Option<MarketDataReference>,
        item_references: Vec<MarketDataReference>,
        provenance: LiveProvenance,
        qualification: SchwabMarketDataQualification,
    ) -> Result<Self, Error> {
        let service = qualification
            .streamer_service()
            .ok_or(Error::InvalidEvidence)?;
        if dictionary.service() != service
            || event_class(service).is_none()
            || provenance.binding().event_class()
                != event_class(service).ok_or(Error::InvalidEvidence)?
            || (is_screener(service) != reference.is_none())
            || (!is_screener(service) && !item_references.is_empty())
        {
            return Err(Error::InvalidEvidence);
        }
        let mut references = BTreeMap::new();
        for reference in item_references {
            if references
                .insert(reference.source_symbol().clone(), reference)
                .is_some()
            {
                return Err(Error::InvalidEvidence);
            }
        }
        Ok(Self {
            frame_ordinal,
            data_batch_ordinal,
            content_ordinal,
            dictionary,
            reference,
            item_references: references,
            provenance,
            qualification,
        })
    }
}

pub(crate) const fn event_class(service: MarketDataService) -> Option<LiveEventClass> {
    match service {
        MarketDataService::NyseBook
        | MarketDataService::NasdaqBook
        | MarketDataService::OptionsBook => Some(LiveEventClass::BookSnapshot),
        MarketDataService::ChartEquity | MarketDataService::ChartFutures => {
            Some(LiveEventClass::Chart)
        }
        MarketDataService::ScreenerEquity | MarketDataService::ScreenerOption => {
            Some(LiveEventClass::Screener)
        }
        _ => None,
    }
}
fn is_screener(service: MarketDataService) -> bool {
    matches!(
        service,
        MarketDataService::ScreenerEquity | MarketDataService::ScreenerOption
    )
}

/// Returns only the original family's own observation timestamp, never the transport envelope clock.
pub fn streamer_family_source_timestamp(
    record: &SchwabCanonicalStreamerRecord,
) -> Result<Timestamp, Error> {
    let field = match record.service {
        MarketDataService::ChartEquity | MarketDataService::ChartFutures => Field::ChartTime,
        _ if event_class(record.service).is_some() => Field::SnapshotTime,
        _ => return Err(Error::MappingMismatch),
    };
    millis(integer(required(record, field)?)?)
}

pub(crate) fn canonical_event(
    record: &SchwabCanonicalStreamerRecord,
    input: &SchwabStreamerFamilyRecordRequest,
) -> Result<MarketEvent, Error> {
    if input.dictionary.service() != record.service
        || input.dictionary.version() != &record.dictionary_version
        || input.dictionary.evidence() != record.dictionary_evidence
        || input.provenance.source_identifier().as_str() != record.provider_identifier.as_str()
        || input.provenance.source_timestamp() != Some(streamer_family_source_timestamp(record)?)
    {
        return Err(Error::MappingMismatch);
    }
    if let Some(value) = optional(record, Field::Symbol) {
        if !matches!(value, StreamerNativeValue::Scalar(NativeScalar::Text(symbol)) if symbol.as_ref() == record.provider_identifier.as_str())
        {
            return Err(Error::MappingMismatch);
        }
    }
    if is_screener(record.service) {
        return screener(record, input);
    }
    let reference = input.reference.as_ref().ok_or(Error::InvalidEvidence)?;
    if reference.source_symbol().as_str() != record.provider_identifier.as_str() {
        return Err(Error::MappingMismatch);
    }
    let class_matches = match record.service {
        MarketDataService::NyseBook
        | MarketDataService::NasdaqBook
        | MarketDataService::ChartEquity => matches!(
            reference.asset_class(),
            AssetClass::Equity | AssetClass::Fund
        ),
        MarketDataService::OptionsBook => reference.asset_class() == AssetClass::Option,
        MarketDataService::ChartFutures => reference.asset_class() == AssetClass::Future,
        _ => false,
    };
    if !class_matches {
        return Err(Error::MappingMismatch);
    }
    if matches!(
        record.service,
        MarketDataService::ChartEquity | MarketDataService::ChartFutures
    ) {
        let money = |field| -> Result<Money, Error> {
            Ok(Money::new(
                decimal(required(record, field)?)?,
                reference.currency(),
            ))
        };
        return MarketDataChartEvent::try_new(MarketDataChartInput {
            provenance: input.provenance.clone(),
            reference: reference.clone(),
            interval_nanos: 60_000_000_000,
            timestamp_basis: MarketDataChartTimestampBasis::Unspecified,
            completion: MarketDataChartCompletion::Unknown,
            open: money(Field::OpenPrice)?,
            high: money(Field::HighPrice)?,
            low: money(Field::LowPrice)?,
            close: money(Field::ClosePrice)?,
            volume: decimal(required(record, Field::Volume)?)?,
            volume_unit: MarketDataSizeUnit::Unspecified,
            provider_sequence: reported(optional(record, Field::Sequence), integer)?,
            provider_day: reported(optional(record, Field::ChartDay), signed_integer)?,
        })
        .map(MarketEvent::MarketDataChart)
        .map_err(|_| Error::InvalidEvidence);
    }
    MarketDataBookEvent::try_new(MarketDataBookInput {
        provenance: input.provenance.clone(),
        reference: reference.clone(),
        size_unit: MarketDataSizeUnit::Unspecified,
        bids: book_levels(required(record, Field::BidBook)?, reference)?,
        asks: book_levels(required(record, Field::AskBook)?, reference)?,
    })
    .map(MarketEvent::MarketDataBook)
    .map_err(|_| Error::InvalidEvidence)
}

fn book_levels(
    value: &StreamerNativeValue,
    reference: &MarketDataReference,
) -> Result<Vec<MarketDataBookLevel>, Error> {
    let StreamerNativeValue::Sequence(levels) = value else {
        return Err(Error::MappingMismatch);
    };
    if levels.len() > market_squawk_domain::MAX_MARKET_DATA_BOOK_LEVELS {
        return Err(Error::InvalidEvidence);
    }
    levels
        .iter()
        .map(|level| {
            let StreamerNativeValue::Fields(fields) = level else {
                return Err(Error::MappingMismatch);
            };
            if fields.iter().any(|field| field.field_id > 3) {
                return Err(Error::MappingMismatch);
            }
            let get = |id| {
                fields
                    .iter()
                    .find(|field| field.field_id == id)
                    .map(|field| &field.value)
                    .ok_or(Error::MappingMismatch)
            };
            let StreamerNativeValue::Sequence(makers) = get(3)? else {
                return Err(Error::MappingMismatch);
            };
            if makers.len() > market_squawk_domain::MAX_MARKET_DATA_BOOK_PARTICIPANTS {
                return Err(Error::InvalidEvidence);
            }
            let participants = makers
                .iter()
                .map(|maker| {
                    let StreamerNativeValue::Fields(fields) = maker else {
                        return Err(Error::MappingMismatch);
                    };
                    if fields.iter().any(|field| field.field_id > 2) {
                        return Err(Error::MappingMismatch);
                    }
                    let get = |id| {
                        fields
                            .iter()
                            .find(|field| field.field_id == id)
                            .map(|field| &field.value)
                    };
                    Ok(MarketDataBookParticipant {
                        identifier: SourceIdentifier::try_from(text(
                            get(0).ok_or(Error::MappingMismatch)?,
                        )?)
                        .map_err(|_| Error::MappingMismatch)?,
                        size: decimal(get(1).ok_or(Error::MappingMismatch)?)?,
                        quote_time_millis: reported(get(2), integer)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(MarketDataBookLevel {
                price: Money::new(decimal(get(0)?)?, reference.currency()),
                aggregate_size: decimal(get(1)?)?,
                participant_count: integer(get(2)?)?,
                participants,
            })
        })
        .collect()
}

fn screener(
    record: &SchwabCanonicalStreamerRecord,
    input: &SchwabStreamerFamilyRecordRequest,
) -> Result<MarketEvent, Error> {
    let sort_text = text(required(record, Field::SortField)?)?;
    let sort = match sort_text {
        "VOLUME" => MarketDataScreenerSort::Volume,
        "TRADES" => MarketDataScreenerSort::Trades,
        "PERCENT_CHANGE_UP" => MarketDataScreenerSort::PercentChangeUp,
        "PERCENT_CHANGE_DOWN" => MarketDataScreenerSort::PercentChangeDown,
        "AVERAGE_PERCENT_VOLUME" => MarketDataScreenerSort::AveragePercentVolume,
        _ => return Err(Error::MappingMismatch),
    };
    let frequency = integer(required(record, Field::Frequency)?)?;
    if !matches!(frequency, 0 | 1 | 5 | 10 | 30 | 60) {
        return Err(Error::MappingMismatch);
    }
    let suffix = format!("_{sort_text}_{frequency}");
    let prefix = record
        .provider_identifier
        .as_str()
        .strip_suffix(&suffix)
        .ok_or(Error::MappingMismatch)?;
    let valid_prefix = match record.service {
        MarketDataService::ScreenerEquity => matches!(
            prefix,
            "$COMPX" | "$DJI" | "$SPX" | "INDEX_ALL" | "NYSE" | "NASDAQ" | "OTCBB" | "EQUITY_ALL"
        ),
        MarketDataService::ScreenerOption => {
            matches!(prefix, "OPTION_PUT" | "OPTION_CALL" | "OPTION_ALL")
        }
        _ => false,
    };
    if !valid_prefix {
        return Err(Error::MappingMismatch);
    }
    let StreamerNativeValue::ScreenerItems(original) = required(record, Field::Items)? else {
        return Err(Error::MappingMismatch);
    };
    if original.len() > market_squawk_domain::MAX_MARKET_DATA_SCREENER_ITEMS {
        return Err(Error::InvalidEvidence);
    }
    let mut used = std::collections::BTreeSet::new();
    let mut items = Vec::new();
    for item in original {
        let value = |name| {
            item.fields()
                .iter()
                .find(|field| *field.name() == name)
                .map(|field| field.value())
        };
        let Some(NativeScalar::Text(symbol)) = value(ItemField::Symbol) else {
            return Err(Error::MappingMismatch);
        };
        let symbol =
            ProviderInstrumentId::try_from(symbol.as_ref()).map_err(|_| Error::MappingMismatch)?;
        let reference = input.item_references.get(&symbol).cloned();
        if let Some(reference) = &reference {
            let matches = match record.service {
                MarketDataService::ScreenerOption => reference.asset_class() == AssetClass::Option,
                _ => matches!(
                    reference.asset_class(),
                    AssetClass::Equity | AssetClass::Fund
                ),
            };
            if !matches {
                return Err(Error::MappingMismatch);
            }
            used.insert(symbol.clone());
        }
        items.push(MarketDataScreenerItem {
            symbol,
            reference,
            description: scalar_reported(value(ItemField::Description), |v| match v {
                NativeScalar::Text(v) => Ok(v.to_string()),
                _ => Err(Error::MappingMismatch),
            })?,
            last_price: scalar_reported(value(ItemField::LastPrice), scalar_decimal)?,
            market_share_percent: scalar_reported(value(ItemField::MarketShare), scalar_decimal)?,
            net_change: scalar_reported(value(ItemField::NetChange), scalar_decimal)?,
            net_percent_change: scalar_reported(
                value(ItemField::NetPercentChange),
                scalar_decimal,
            )?,
            total_volume: scalar_reported(value(ItemField::TotalVolume), scalar_integer)?,
            trades: scalar_reported(value(ItemField::Trades), scalar_integer)?,
            volume: scalar_reported(value(ItemField::Volume), scalar_integer)?,
            volume_unit: MarketDataSizeUnit::Unspecified,
        });
    }
    if used.len() != input.item_references.len() {
        return Err(Error::MappingMismatch);
    }
    MarketDataScreenerEvent::try_new(MarketDataScreenerInput {
        provenance: input.provenance.clone(),
        cohort_key: SourceIdentifier::try_from(record.provider_identifier.as_str())
            .map_err(|_| Error::MappingMismatch)?,
        sort,
        frequency_minutes: if frequency == 0 {
            None
        } else {
            Some(u16::try_from(frequency).map_err(|_| Error::MappingMismatch)?)
        },
        items,
    })
    .map(MarketEvent::MarketDataScreener)
    .map_err(|_| Error::InvalidEvidence)
}
fn optional(record: &SchwabCanonicalStreamerRecord, name: Field) -> Option<&StreamerNativeValue> {
    record
        .fields
        .iter()
        .find(|field| field.meaning == name)
        .map(|field| &field.value)
}
fn required(
    record: &SchwabCanonicalStreamerRecord,
    name: Field,
) -> Result<&StreamerNativeValue, Error> {
    optional(record, name).ok_or(Error::MappingMismatch)
}
fn text(value: &StreamerNativeValue) -> Result<&str, Error> {
    match value {
        StreamerNativeValue::Scalar(NativeScalar::Text(value)) => Ok(value),
        _ => Err(Error::MappingMismatch),
    }
}
fn decimal(value: &StreamerNativeValue) -> Result<Decimal, Error> {
    match value {
        StreamerNativeValue::Scalar(value) => scalar_decimal(value),
        _ => Err(Error::MappingMismatch),
    }
}
fn scalar_decimal(value: &NativeScalar) -> Result<Decimal, Error> {
    match value {
        NativeScalar::Number(value) => {
            Decimal::from_str_exact(value.as_str()).map_err(|_| Error::MappingMismatch)
        }
        _ => Err(Error::MappingMismatch),
    }
}
fn scalar_integer(value: &NativeScalar) -> Result<u64, Error> {
    match value {
        NativeScalar::Number(value) => value.as_str().parse().map_err(|_| Error::MappingMismatch),
        _ => Err(Error::MappingMismatch),
    }
}
fn integer(value: &StreamerNativeValue) -> Result<u64, Error> {
    match value {
        StreamerNativeValue::Scalar(value) => scalar_integer(value),
        _ => Err(Error::MappingMismatch),
    }
}
fn signed_integer(value: &StreamerNativeValue) -> Result<i64, Error> {
    match value {
        StreamerNativeValue::Scalar(NativeScalar::Number(value)) => {
            value.as_str().parse().map_err(|_| Error::MappingMismatch)
        }
        _ => Err(Error::MappingMismatch),
    }
}
fn millis(value: u64) -> Result<Timestamp, Error> {
    i64::try_from(value)
        .ok()
        .and_then(|v| v.checked_mul(1_000_000))
        .map(Timestamp::from_unix_nanos)
        .ok_or(Error::MappingMismatch)
}
fn reported<T>(
    value: Option<&StreamerNativeValue>,
    convert: impl FnOnce(&StreamerNativeValue) -> Result<T, Error>,
) -> Result<MarketDataReported<T>, Error> {
    match value {
        None => Ok(MarketDataReported::Absent),
        Some(StreamerNativeValue::Scalar(NativeScalar::Null)) => Ok(MarketDataReported::Null),
        Some(value) => convert(value).map(MarketDataReported::Value),
    }
}
fn scalar_reported<T>(
    value: Option<&NativeScalar>,
    convert: impl FnOnce(&NativeScalar) -> Result<T, Error>,
) -> Result<MarketDataReported<T>, Error> {
    match value {
        None => Ok(MarketDataReported::Absent),
        Some(NativeScalar::Null) => Ok(MarketDataReported::Null),
        Some(value) => convert(value).map(MarketDataReported::Value),
    }
}
