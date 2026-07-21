use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;

use crate::chunks::{BlsAccessTier, limits_for};

const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_OBSERVATIONS_PER_SERIES: usize = 2_000;

/// The historical-revision capability exposed by the BLS API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlsVintageCapability {
    /// The API supplies current data only; revisions require locally retained response versions.
    LocallyObservedVersionsOnly,
}

/// One provider footnote retained without interpretation loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsFootnote {
    code: Option<String>,
    text: Option<String>,
}

impl BlsFootnote {
    /// Returns the provider footnote code when present.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Returns the provider footnote text when present.
    pub fn text(&self) -> Option<&str> {
        self.text.as_deref()
    }
}

/// One exact BLS period observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsObservation {
    year: u16,
    period: String,
    period_name: String,
    raw_value: String,
    value: Option<Decimal>,
    latest: bool,
    preliminary: bool,
    footnotes: Vec<BlsFootnote>,
}

impl BlsObservation {
    /// Returns the exact decimal, or `None` for the provider's `-` marker.
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }

    /// Returns whether the observation carries the provider `P` footnote.
    pub const fn is_preliminary(&self) -> bool {
        self.preliminary
    }

    /// Returns the observation year.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns the provider period code.
    pub fn period(&self) -> &str {
        &self.period
    }

    /// Returns the provider period label.
    pub fn period_name(&self) -> &str {
        &self.period_name
    }

    /// Returns the exact provider lexical value.
    pub fn raw_value(&self) -> &str {
        &self.raw_value
    }

    /// Returns whether the provider marked the observation latest.
    pub const fn is_latest(&self) -> bool {
        self.latest
    }

    /// Returns retained provider footnotes.
    pub fn footnotes(&self) -> &[BlsFootnote] {
        &self.footnotes
    }
}

/// One BLS series result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsSeries {
    series_id: String,
    observations: Vec<BlsObservation>,
}

impl BlsSeries {
    /// Returns the provider series identifier.
    pub fn series_id(&self) -> &str {
        &self.series_id
    }

    /// Returns observations in provider order.
    pub fn observations(&self) -> &[BlsObservation] {
        &self.observations
    }
}

/// One validated BLS response, including partial-result evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsResponse {
    response_time_millis: u64,
    messages: Vec<String>,
    series: Vec<BlsSeries>,
    partial: bool,
}

impl BlsResponse {
    /// Parses the official JSON response for the selected access tier.
    pub fn parse(bytes: &[u8], tier: BlsAccessTier) -> Result<Self, BlsParseError> {
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(BlsParseError::BodyTooLarge);
        }
        let wire: ResponseWire = serde_json::from_slice(bytes)?;
        if wire.status != "REQUEST_SUCCEEDED" {
            return Err(BlsParseError::ProviderFailure(wire.status));
        }
        if wire.results.series.len() > limits_for(tier).series_per_query() {
            return Err(BlsParseError::LimitExceeded);
        }
        validate_messages(&wire.message)?;
        let mut series = Vec::with_capacity(wire.results.series.len());
        for series_wire in wire.results.series {
            validate_identifier(&series_wire.series_id)?;
            if series_wire.data.len() > MAX_OBSERVATIONS_PER_SERIES {
                return Err(BlsParseError::LimitExceeded);
            }
            let mut observations = Vec::with_capacity(series_wire.data.len());
            for observation in series_wire.data {
                let year = u16::from_str(&observation.year)
                    .map_err(|_| BlsParseError::InvalidField("year"))?;
                if year < 1900 {
                    return Err(BlsParseError::InvalidField("year"));
                }
                if observation.period.is_empty()
                    || observation.period.len() > 8
                    || observation.period_name.len() > 64
                {
                    return Err(BlsParseError::InvalidField("period"));
                }
                let latest = match observation.latest.as_deref() {
                    None | Some("false") => false,
                    Some("true") => true,
                    Some(_) => return Err(BlsParseError::InvalidField("latest")),
                };
                let value = if observation.value == "-" {
                    None
                } else {
                    Some(
                        Decimal::from_str_exact(&observation.value)
                            .map_err(|_| BlsParseError::InvalidField("value"))?,
                    )
                };
                let footnotes = observation
                    .footnotes
                    .into_iter()
                    .map(|footnote| BlsFootnote {
                        code: footnote.code,
                        text: footnote.text,
                    })
                    .collect::<Vec<_>>();
                let preliminary = footnotes
                    .iter()
                    .any(|footnote| footnote.code.as_deref() == Some("P"));
                observations.push(BlsObservation {
                    year,
                    period: observation.period,
                    period_name: observation.period_name,
                    raw_value: observation.value,
                    value,
                    latest,
                    preliminary,
                    footnotes,
                });
            }
            series.push(BlsSeries {
                series_id: series_wire.series_id,
                observations,
            });
        }
        let partial = series.is_empty()
            || !wire.message.is_empty()
            || series.iter().any(|item| item.observations.is_empty());
        Ok(Self {
            response_time_millis: wire.response_time,
            messages: wire.message,
            series,
            partial,
        })
    }

    /// Returns whether messages or empty series prove a partial response.
    pub const fn is_partial(&self) -> bool {
        self.partial
    }

    /// Returns provider response messages without silently discarding errors.
    pub fn messages(&self) -> &[String] {
        &self.messages
    }

    /// Returns all provider-returned series, including empty invalid-series rows.
    pub fn series(&self) -> &[BlsSeries] {
        &self.series
    }

    /// Returns the provider-reported processing time in milliseconds.
    pub const fn response_time_millis(&self) -> u64 {
        self.response_time_millis
    }

    /// Returns the explicit limitation on historical revision reconstruction.
    pub const fn vintage_capability(&self) -> BlsVintageCapability {
        BlsVintageCapability::LocallyObservedVersionsOnly
    }
}

/// A bounded BLS response parsing failure.
#[derive(Debug, Error)]
pub enum BlsParseError {
    /// The response exceeds the parser byte budget.
    #[error("BLS response exceeds its byte budget")]
    BodyTooLarge,
    /// JSON does not match the exact supported response schema.
    #[error("invalid BLS JSON response: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// The provider returned a failed status.
    #[error("BLS provider request failed with status {0}")]
    ProviderFailure(String),
    /// A provider field violates the supported contract.
    #[error("invalid BLS field: {0}")]
    InvalidField(&'static str),
    /// Response cardinality exceeds a provider or parser limit.
    #[error("BLS response exceeds a cardinality limit")]
    LimitExceeded,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseWire {
    status: String,
    #[serde(rename = "responseTime")]
    response_time: u64,
    message: Vec<String>,
    #[serde(rename = "Results")]
    results: ResultsWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultsWire {
    series: Vec<SeriesWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesWire {
    #[serde(rename = "seriesID")]
    series_id: String,
    data: Vec<ObservationWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationWire {
    year: String,
    period: String,
    #[serde(rename = "periodName")]
    period_name: String,
    #[serde(default)]
    latest: Option<String>,
    value: String,
    footnotes: Vec<FootnoteWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FootnoteWire {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

fn validate_identifier(value: &str) -> Result<(), BlsParseError> {
    if value.is_empty()
        || value.len() > 50
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(BlsParseError::InvalidField("series identifier"));
    }
    Ok(())
}

fn validate_messages(messages: &[String]) -> Result<(), BlsParseError> {
    if messages.len() > 100 || messages.iter().any(|message| message.len() > 8 * 1024) {
        return Err(BlsParseError::LimitExceeded);
    }
    Ok(())
}
