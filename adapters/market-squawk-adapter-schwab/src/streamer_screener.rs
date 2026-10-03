//! Closed provider-native screener item fields from the retained official Streamer table.
//!
//! Number lexemes, item order, and absent/null distinctions remain original. Unknown fields use
//! the existing bounded diagnostics; their values remain only in the original sealed frame.

use crate::SchwabAdapterError;
use crate::rest::{NativeFieldEntry, NativeScalar, ParseContext};
use serde_json::Value;

/// Official named fields inside SCREENER_EQUITY and SCREENER_OPTION field 4.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabStreamerScreenerField {
    Description,
    LastPrice,
    MarketShare,
    NetChange,
    NetPercentChange,
    Symbol,
    TotalVolume,
    Trades,
    Volume,
}

impl SchwabStreamerScreenerField {
    /// Exact provider field spelling, independent of display labels.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Description => "description",
            Self::LastPrice => "lastPrice",
            Self::MarketShare => "marketShare",
            Self::NetChange => "netChange",
            Self::NetPercentChange => "netPercentChange",
            Self::Symbol => "symbol",
            Self::TotalVolume => "totalVolume",
            Self::Trades => "trades",
            Self::Volume => "volume",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "description" => Self::Description,
            "lastPrice" => Self::LastPrice,
            "marketShare" => Self::MarketShare,
            "netChange" => Self::NetChange,
            "netPercentChange" => Self::NetPercentChange,
            "symbol" => Self::Symbol,
            "totalVolume" => Self::TotalVolume,
            "trades" => Self::Trades,
            "volume" => Self::Volume,
            _ => return None,
        })
    }
}

/// One original array item, before canonical identity or economic-unit admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabStreamerScreenerItem {
    fields: Box<[NativeFieldEntry<SchwabStreamerScreenerField>]>,
}

impl SchwabStreamerScreenerItem {
    /// Supplied fields only; an absent field is distinct from an explicit null value.
    pub fn fields(&self) -> &[NativeFieldEntry<SchwabStreamerScreenerField>] {
        &self.fields
    }
}

pub(crate) fn parse_items(
    value: Value,
    context: &mut ParseContext,
) -> Result<Box<[SchwabStreamerScreenerItem]>, SchwabAdapterError> {
    let Value::Array(items) = value else {
        return Err(SchwabAdapterError::SchemaViolation);
    };
    let mut output = Vec::new();
    for item in items {
        context.take_record()?;
        let Value::Object(fields) = item else {
            return Err(SchwabAdapterError::SchemaViolation);
        };
        let mut retained = Vec::new();
        for (name, value) in fields {
            let Some(field) = SchwabStreamerScreenerField::parse(&name) else {
                context.record_unknown("$.data[].content[].4[]", &name, &value)?;
                continue;
            };
            let scalar = NativeScalar::try_from_json(value)?;
            let valid_type = matches!(scalar, NativeScalar::Null)
                || match field {
                    SchwabStreamerScreenerField::Description
                    | SchwabStreamerScreenerField::Symbol => {
                        matches!(scalar, NativeScalar::Text(_))
                    }
                    _ => matches!(scalar, NativeScalar::Number(_)),
                };
            if !valid_type {
                return Err(SchwabAdapterError::SchemaViolation);
            }
            retained.push(NativeFieldEntry::new(field, scalar));
        }
        output.push(SchwabStreamerScreenerItem {
            fields: retained.into_boxed_slice(),
        });
    }
    Ok(output.into_boxed_slice())
}
