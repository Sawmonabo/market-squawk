//! Closed, date-preserving decoding of Alpaca's all-category corporate-action response.
//!
//! Contract reviewed 2026-09-08:
//! https://docs.alpaca.markets/us/reference/corporateactions-1
//! https://github.com/alpacahq/cli/blob/main/api/specs/market-data-api.json
//! Request dates filter `process_date`; they are never ex-date or announcement guarantees.

use std::{collections::BTreeMap, fmt};

use market_squawk_domain::CalendarDate;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, SeqAccess, Visitor},
};
use serde_json::{Number, Value};
use uuid::Uuid;

use crate::AlpacaError;

pub(super) const PAGE_ROWS: usize = 1_000;

/// Every category in the reviewed all-types response; order fixes coverage-count encoding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaCorporateActionCategory {
    /// Reverse share split.
    ReverseSplit,
    /// Forward share split.
    ForwardSplit,
    /// Unit conversion with more than one security allocation.
    UnitSplit,
    /// Cash dividend, including explicitly classified return of capital or interest.
    CashDividend,
    /// Additional shares distributed to the same security.
    StockDividend,
    /// Shares distributed in another security.
    SpinOff,
    /// Acquisition for cash.
    CashMerger,
    /// Acquisition for stock.
    StockMerger,
    /// Acquisition for stock and cash.
    StockAndCashMerger,
    /// Redemption of the security.
    Redemption,
    /// Security name, ticker, or identifier change.
    NameChange,
    /// Removal of a worthless security.
    WorthlessRemoval,
    /// Distribution of expiring rights.
    RightsDistribution,
    /// Partial call with allocation/lottery evidence.
    PartialCall,
    /// Reorganization with cash and/or multiple stock allocations.
    Reorganization,
    /// Long-term and/or short-term capital-gain cash distribution.
    CapitalGainsDistribution,
}

impl AlpacaCorporateActionCategory {
    pub(super) const ALL: [Self; 16] = [
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

    pub(super) fn from_response_key(key: &str) -> Result<Self, AlpacaError> {
        Ok(match key {
            "reverse_splits" => Self::ReverseSplit,
            "forward_splits" => Self::ForwardSplit,
            "unit_splits" => Self::UnitSplit,
            "cash_dividends" => Self::CashDividend,
            "stock_dividends" => Self::StockDividend,
            "spin_offs" => Self::SpinOff,
            "cash_mergers" => Self::CashMerger,
            "stock_mergers" => Self::StockMerger,
            "stock_and_cash_mergers" => Self::StockAndCashMerger,
            "redemptions" => Self::Redemption,
            "name_changes" => Self::NameChange,
            "worthless_removals" => Self::WorthlessRemoval,
            "rights_distributions" => Self::RightsDistribution,
            "partial_calls" => Self::PartialCall,
            "reorganizations" => Self::Reorganization,
            "capital_gains_distributions" => Self::CapitalGainsDistribution,
            _ => return Err(AlpacaError::Protocol),
        })
    }

    pub(super) const fn index(self) -> usize {
        self as usize
    }

    fn admits(self, key: &str) -> bool {
        if matches!(key, "id" | "process_date" | "currency") {
            return true;
        }
        let fields: &[&str] = match self {
            Self::ReverseSplit => &[
                "symbol",
                "old_cusip",
                "new_cusip",
                "old_isin",
                "new_isin",
                "new_symbol",
                "new_rate",
                "old_rate",
                "ex_date",
                "record_date",
                "payable_date",
            ],
            Self::ForwardSplit => &[
                "symbol",
                "cusip",
                "isin",
                "new_rate",
                "old_rate",
                "ex_date",
                "record_date",
                "payable_date",
                "due_bill_redemption_date",
            ],
            Self::UnitSplit => &[
                "old_symbol",
                "old_cusip",
                "old_isin",
                "old_rate",
                "new_symbol",
                "new_cusip",
                "new_isin",
                "new_rate",
                "alternate_symbol",
                "alternate_cusip",
                "alternate_isin",
                "alternate_rate",
                "effective_date",
                "payable_date",
            ],
            Self::CashDividend => &[
                "symbol",
                "cusip",
                "isin",
                "rate",
                "special",
                "foreign",
                "ex_date",
                "record_date",
                "payable_date",
                "due_bill_on_date",
                "due_bill_off_date",
                "sub_type",
            ],
            Self::StockDividend => &[
                "symbol",
                "cusip",
                "isin",
                "rate",
                "ex_date",
                "record_date",
                "payable_date",
            ],
            Self::SpinOff => &[
                "source_symbol",
                "source_cusip",
                "source_isin",
                "source_rate",
                "new_symbol",
                "new_cusip",
                "new_isin",
                "new_rate",
                "ex_date",
                "record_date",
                "payable_date",
                "due_bill_redemption_date",
            ],
            Self::CashMerger => &[
                "acquiree_symbol",
                "acquiree_cusip",
                "acquiree_isin",
                "acquirer_symbol",
                "acquirer_cusip",
                "acquirer_isin",
                "rate",
                "effective_date",
                "payable_date",
            ],
            Self::StockMerger | Self::StockAndCashMerger => {
                if key == "cash_rate" {
                    return self == Self::StockAndCashMerger;
                }
                &[
                    "acquiree_symbol",
                    "acquiree_cusip",
                    "acquiree_isin",
                    "acquiree_rate",
                    "acquirer_symbol",
                    "acquirer_cusip",
                    "acquirer_isin",
                    "acquirer_rate",
                    "effective_date",
                    "payable_date",
                ]
            }
            Self::Redemption => &["symbol", "cusip", "isin", "rate", "payable_date"],
            Self::NameChange => &[
                "old_symbol",
                "old_cusip",
                "old_isin",
                "new_symbol",
                "new_cusip",
                "new_isin",
            ],
            Self::WorthlessRemoval => &["symbol", "cusip", "isin"],
            Self::RightsDistribution => &[
                "source_symbol",
                "source_cusip",
                "source_isin",
                "new_symbol",
                "new_cusip",
                "new_isin",
                "rate",
                "ex_date",
                "record_date",
                "payable_date",
                "expiration_date",
            ],
            Self::PartialCall => &[
                "symbol",
                "cusip",
                "isin",
                "price",
                "dividend_rate",
                "record_date",
                "payable_date",
                "lottery_type",
                "lottery_date",
                "results_publication_date",
            ],
            Self::Reorganization => &[
                "symbol",
                "cusip",
                "isin",
                "cash_rate",
                "stock_movements",
                "effective_date",
                "payable_date",
            ],
            Self::CapitalGainsDistribution => &[
                "symbol",
                "cusip",
                "isin",
                "long_term_rate",
                "short_term_rate",
                "ex_date",
                "record_date",
                "payable_date",
            ],
        };
        fields.contains(&key)
    }
}

/// Source date preserved as a calendar date, with no invented timezone or midnight timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct AlpacaCorporateActionDate(pub(super) CalendarDate);

impl AlpacaCorporateActionDate {
    /// Returns the exact civil date.
    pub const fn date(self) -> CalendarDate {
        self.0
    }
}

/// Independent source dates; absence is retained and no processing date substitutes for another.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct AlpacaCorporateActionDates {
    /// Alpaca processing date used by the API's range filter.
    pub process: CalendarDate,
    /// Ex-entitlement date when supplied.
    pub ex: Option<CalendarDate>,
    /// Explicit source effective date when supplied.
    pub effective: Option<CalendarDate>,
    /// Shareholder record date when supplied.
    pub record: Option<CalendarDate>,
    /// Payment or distribution date when supplied.
    pub payable: Option<CalendarDate>,
    /// Due-bill entitlement start when supplied.
    pub due_bill_on: Option<CalendarDate>,
    /// Due-bill entitlement end when supplied.
    pub due_bill_off: Option<CalendarDate>,
    /// Due-bill redemption date when supplied.
    pub due_bill_redemption: Option<CalendarDate>,
    /// Rights expiration date when supplied.
    pub expiration: Option<CalendarDate>,
}

impl<'de> Deserialize<'de> for AlpacaCorporateActionDate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_date(&text)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

pub(super) fn parse_date(value: &str) -> Result<CalendarDate, AlpacaError> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(i, b)| i != 4 && i != 7 && !b.is_ascii_digit())
    {
        return Err(AlpacaError::Protocol);
    }
    CalendarDate::new(
        value[0..4].parse().map_err(|_| AlpacaError::Protocol)?,
        value[5..7].parse().map_err(|_| AlpacaError::Protocol)?,
        value[8..10].parse().map_err(|_| AlpacaError::Protocol)?,
    )
    .map_err(|_| AlpacaError::Protocol)
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CashSubtype {
    Interest,
    ReturnOfCapital,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum LotteryType {
    Original,
    Supplemental,
}

/// All fields have a closed type; category-specific field admission precedes this decoder.
/// Optional fields also retain early/incomplete records from `data_quality=all`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fields {
    pub id: Uuid,
    pub process_date: AlpacaCorporateActionDate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cusip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_cusip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_isin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_cusip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_isin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_cusip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_isin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternate_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternate_cusip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternate_isin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquiree_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquiree_cusip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquiree_isin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquirer_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquirer_cusip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquirer_isin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternate_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquiree_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquirer_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cash_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub long_term_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_term_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dividend_rate: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub special: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_type: Option<CashSubtype>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ex_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payable_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_bill_on_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_bill_off_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_bill_redemption_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lottery_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results_publication_date: Option<AlpacaCorporateActionDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lottery_type: Option<LotteryType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stock_movements: Option<Rows<StockMovement>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StockMovement {
    symbol: String,
    cusip: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    isin: Option<String>,
    new_rate: Number,
    source_rate: Number,
}

#[derive(Debug, Serialize)]
pub(super) struct NativeAction {
    pub category: AlpacaCorporateActionCategory,
    pub fields: Fields,
}

impl NativeAction {
    pub(super) fn dates(&self) -> AlpacaCorporateActionDates {
        let f = &self.fields;
        AlpacaCorporateActionDates {
            process: f.process_date.date(),
            ex: f.ex_date.map(AlpacaCorporateActionDate::date),
            effective: f.effective_date.map(AlpacaCorporateActionDate::date),
            record: f.record_date.map(AlpacaCorporateActionDate::date),
            payable: f.payable_date.map(AlpacaCorporateActionDate::date),
            due_bill_on: f.due_bill_on_date.map(AlpacaCorporateActionDate::date),
            due_bill_off: f.due_bill_off_date.map(AlpacaCorporateActionDate::date),
            due_bill_redemption: f
                .due_bill_redemption_date
                .map(AlpacaCorporateActionDate::date),
            expiration: f.expiration_date.map(AlpacaCorporateActionDate::date),
        }
    }

    pub(super) fn subject_symbol(&self) -> Option<&str> {
        let f = &self.fields;
        match self.category {
            AlpacaCorporateActionCategory::UnitSplit
            | AlpacaCorporateActionCategory::NameChange => f.old_symbol.as_deref(),
            AlpacaCorporateActionCategory::SpinOff
            | AlpacaCorporateActionCategory::RightsDistribution => f.source_symbol.as_deref(),
            AlpacaCorporateActionCategory::CashMerger
            | AlpacaCorporateActionCategory::StockMerger
            | AlpacaCorporateActionCategory::StockAndCashMerger => f.acquiree_symbol.as_deref(),
            _ => f.symbol.as_deref(),
        }
    }

    pub(super) fn related_symbol(&self) -> Option<&str> {
        match self.category {
            AlpacaCorporateActionCategory::CashMerger
            | AlpacaCorporateActionCategory::StockMerger
            | AlpacaCorporateActionCategory::StockAndCashMerger => {
                self.fields.acquirer_symbol.as_deref()
            }
            AlpacaCorporateActionCategory::SpinOff
            | AlpacaCorporateActionCategory::ReverseSplit
            | AlpacaCorporateActionCategory::UnitSplit
            | AlpacaCorporateActionCategory::NameChange
            | AlpacaCorporateActionCategory::RightsDistribution => {
                self.fields.new_symbol.as_deref()
            }
            _ => None,
        }
        .filter(|symbol| !symbol.is_empty())
    }

    pub(super) fn effective_date(&self) -> Option<CalendarDate> {
        self.fields
            .ex_date
            .or(self.fields.effective_date)
            .map(|value| value.0)
    }

    pub(super) fn symbols(&self) -> impl Iterator<Item = &str> {
        let f = &self.fields;
        [
            f.symbol.as_deref(),
            f.old_symbol.as_deref(),
            f.new_symbol.as_deref(),
            f.source_symbol.as_deref(),
            f.alternate_symbol.as_deref(),
            f.acquiree_symbol.as_deref(),
            f.acquirer_symbol.as_deref(),
        ]
        .into_iter()
        .flatten()
        .chain(
            f.stock_movements
                .iter()
                .flat_map(|rows| rows.0.iter().map(|row| row.symbol.as_str())),
        )
        .filter(|symbol| !symbol.is_empty())
    }
}

/// Bounded arrays reject growth before deserializing the first excess element.
#[derive(Debug, Serialize)]
struct Rows<T>(Vec<T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Rows<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RowVisitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for RowVisitor<T> {
            type Value = Rows<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded corporate-action array")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut rows = Vec::new();
                while rows.len() < PAGE_ROWS {
                    let Some(row) = seq.next_element()? else {
                        return Ok(Rows(rows));
                    };
                    rows.try_reserve(1).map_err(serde::de::Error::custom)?;
                    rows.push(row);
                }
                if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "corporate-action array exceeds limit",
                    ));
                }
                Ok(Rows(rows))
            }
        }
        deserializer.deserialize_seq(RowVisitor(std::marker::PhantomData))
    }
}

/// Intermediate objects reject duplicate fields before conversion to a closed native type.
struct RawAction(BTreeMap<String, Value>);

impl<'de> Deserialize<'de> for RawAction {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = RawAction;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a corporate-action object with distinct fields")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = BTreeMap::new();
                while let Some(key) = map.next_key::<String>()? {
                    if key.len() > 32 || values.len() == 24 || values.contains_key(&key) {
                        return Err(serde::de::Error::custom("invalid corporate-action fields"));
                    }
                    let value = if key == "stock_movements" {
                        // Decode each nested object before Value conversion so duplicate
                        // movement terms cannot disappear under last-write-wins semantics.
                        match map.next_value::<Option<Rows<RawAction>>>()? {
                            None => Value::Null,
                            Some(rows) => Value::Array(
                                rows.0
                                    .into_iter()
                                    .map(|row| Value::Object(row.0.into_iter().collect()))
                                    .collect(),
                            ),
                        }
                    } else {
                        map.next_value()?
                    };
                    values.insert(key, value);
                }
                Ok(RawAction(values))
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

struct Categories(Vec<NativeAction>);

impl<'de> Deserialize<'de> for Categories {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CategoryVisitor;
        impl<'de> Visitor<'de> for CategoryVisitor {
            type Value = Categories;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("the reviewed corporate-action categories")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut seen = [false; 16];
                let mut actions = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    let category = AlpacaCorporateActionCategory::from_response_key(&key)
                        .map_err(serde::de::Error::custom)?;
                    if seen[category.index()] {
                        return Err(serde::de::Error::custom(
                            "repeated corporate-action category",
                        ));
                    }
                    seen[category.index()] = true;
                    let rows = map.next_value::<Rows<RawAction>>()?;
                    if actions.len() + rows.0.len() > PAGE_ROWS {
                        return Err(serde::de::Error::custom(
                            "corporate-action page exceeds total limit",
                        ));
                    }
                    actions
                        .try_reserve_exact(rows.0.len())
                        .map_err(serde::de::Error::custom)?;
                    for row in rows.0 {
                        if row.0.keys().any(|field| !category.admits(field)) {
                            return Err(serde::de::Error::custom(
                                "field is not valid for corporate-action category",
                            ));
                        }
                        if row.0.values().any(|value| {
                            value.as_str().is_some_and(|text| {
                                text.len() > 128 || text.chars().any(char::is_control)
                            })
                        }) {
                            return Err(serde::de::Error::custom(
                                "unbounded corporate-action field",
                            ));
                        }
                        let fields: Fields =
                            serde_json::from_value(Value::Object(row.0.into_iter().collect()))
                                .map_err(serde::de::Error::custom)?;
                        if fields.id.is_nil() {
                            return Err(serde::de::Error::custom(
                                "missing corporate-action identity",
                            ));
                        }
                        actions.push(NativeAction { category, fields });
                    }
                }
                Ok(Categories(actions))
            }
        }
        deserializer.deserialize_map(CategoryVisitor)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Page {
    corporate_actions: Categories,
    next_page_token: Option<String>,
}

pub(super) fn decode(body: &[u8]) -> Result<(Vec<NativeAction>, Option<String>), AlpacaError> {
    let page: Page = serde_json::from_slice(body).map_err(|_| AlpacaError::Protocol)?;
    Ok((page.corporate_actions.0, page.next_page_token))
}
