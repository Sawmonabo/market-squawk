use chrono::{DateTime, Utc};
use market_squawk_domain::{CalendarDate, SourceIdentifier, Timestamp};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::cmp::Ordering;
use url::Url;

use crate::series::{
    FredParseLimits, FredProtocolError, admit_body, parse_date, valid_exact_series_id,
    validate_strings,
};

/// Documented maximum observations returned by one FRED API v2 release request.
pub const MAX_FRED_V2_RELEASE_PAGE_OBSERVATIONS: usize = 500_000;

/// Exact opaque v2 continuation coordinate retained without provider-specific reinterpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredReleaseCursor {
    encoded: String,
    series_id: SourceIdentifier,
    observation_date: CalendarDate,
}

impl FredReleaseCursor {
    /// Parses the documented `<series_id>,<yyyy-mm-dd>` cursor grammar.
    pub fn try_new(value: String) -> Result<Self, FredProtocolError> {
        if value.is_empty() || value.len() > 256 {
            return Err(FredProtocolError::InvalidField("release cursor"));
        }
        let mut fields = value.split(',');
        let series_id = fields
            .next()
            .ok_or(FredProtocolError::InvalidField("release cursor"))?;
        let date = fields
            .next()
            .ok_or(FredProtocolError::InvalidField("release cursor"))?;
        if fields.next().is_some() || !valid_exact_series_id(series_id) {
            return Err(FredProtocolError::InvalidField("release cursor"));
        }
        let series_id = SourceIdentifier::try_from(series_id)
            .map_err(|_| FredProtocolError::InvalidField("release cursor"))?;
        Ok(Self {
            observation_date: parse_date(date)?,
            encoded: value,
            series_id,
        })
    }

    /// Returns the exact provider cursor for the next request.
    pub fn encoded(&self) -> &str {
        &self.encoded
    }

    /// Returns the series component retained for continuity validation.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the civil-date component retained for continuity validation.
    pub const fn observation_date(&self) -> CalendarDate {
        self.observation_date
    }

    fn compare_coordinate(&self, series_id: &SourceIdentifier, date: CalendarDate) -> Ordering {
        self.series_id
            .cmp(series_id)
            .then_with(|| self.observation_date.cmp(&date))
    }
}

/// One exact provider named in the v2 release response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredReleaseSource {
    name: String,
    url: String,
    notes: Option<String>,
}

impl FredReleaseSource {
    /// Returns the provider-published source name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the provider-published originating URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns optional provider-published source notes.
    pub fn notes(&self) -> Option<&str> {
        self.notes.as_deref()
    }
}

/// Release identity and exact publisher attribution returned on every v2 page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredReleaseMetadata {
    release_id: u32,
    name: String,
    url: String,
    sources: Vec<FredReleaseSource>,
}

impl FredReleaseMetadata {
    /// Returns the positive provider release identifier.
    pub const fn release_id(&self) -> u32 {
        self.release_id
    }

    /// Returns the release name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the release publisher's originating URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the exact release-source attributions.
    pub fn sources(&self) -> &[FredReleaseSource] {
        &self.sources
    }
}

/// One exact v2 observation value and civil date.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredReleaseObservation {
    observation_date: CalendarDate,
    raw_value: String,
    value: Option<Decimal>,
}

impl FredReleaseObservation {
    /// Returns the provider observation date without inventing a time zone.
    pub const fn observation_date(&self) -> CalendarDate {
        self.observation_date
    }

    /// Returns the exact provider lexical value.
    pub fn raw_value(&self) -> &str {
        &self.raw_value
    }

    /// Returns the exact decimal, or `None` for the provider `.` marker.
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }
}

/// One series segment in a v2 release page, with all attribution and update metadata retained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredReleaseSeries {
    series_id: SourceIdentifier,
    title: String,
    frequency: String,
    units: String,
    seasonal_adjustment: String,
    last_updated: Timestamp,
    copyright_id: String,
    notes: String,
    observations: Vec<FredReleaseObservation>,
}

impl FredReleaseSeries {
    /// Returns the exact FRED series identifier.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the series title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the provider frequency label.
    pub fn frequency(&self) -> &str {
        &self.frequency
    }

    /// Returns the provider units label.
    pub fn units(&self) -> &str {
        &self.units
    }

    /// Returns the provider seasonal-adjustment label.
    pub fn seasonal_adjustment(&self) -> &str {
        &self.seasonal_adjustment
    }

    /// Returns the provider's UTC series-update instant.
    pub const fn last_updated(&self) -> Timestamp {
        self.last_updated
    }

    /// Returns the exact provider copyright/attribution notice for this series.
    pub fn copyright_id(&self) -> &str {
        &self.copyright_id
    }

    /// Returns provider series notes without dropping attribution context.
    pub fn notes(&self) -> &str {
        &self.notes
    }

    /// Returns strictly ascending observations for this series segment.
    pub fn observations(&self) -> &[FredReleaseObservation] {
        &self.observations
    }
}

/// Strict, cursor-bearing FRED v2 release-observations page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredReleaseObservationPage {
    has_more: bool,
    next_cursor: Option<FredReleaseCursor>,
    release: FredReleaseMetadata,
    series: Vec<FredReleaseSeries>,
    observation_count: usize,
}

impl FredReleaseObservationPage {
    /// Parses one response and validates its release identity, cursor, bounds, and ordering.
    ///
    /// `requested_cursor` must be the exact cursor supplied to this request. On continuation
    /// pages the first returned coordinate must equal it. This detects accidental page skips or
    /// reuse of a cursor from a different chain.
    pub fn parse_for_request(
        bytes: &[u8],
        limits: FredParseLimits,
        expected_release_id: u32,
        requested_cursor: Option<&FredReleaseCursor>,
    ) -> Result<Self, FredProtocolError> {
        admit_body(bytes, limits)?;
        if expected_release_id == 0 || limits.max_records > MAX_FRED_V2_RELEASE_PAGE_OBSERVATIONS {
            return Err(FredProtocolError::InvalidLimit);
        }
        let wire: ReleasePageWire = serde_json::from_slice(bytes)?;
        if wire.release.release_id != expected_release_id || wire.release.release_id == 0 {
            return Err(FredProtocolError::InvalidField("release identity"));
        }
        validate_strings(
            [wire.release.name.as_str(), wire.release.url.as_str()],
            limits,
        )?;
        validate_origin_url(&wire.release.url)?;
        if wire.release.sources.is_empty() {
            return Err(FredProtocolError::InvalidField("release sources"));
        }
        let mut sources = Vec::new();
        sources
            .try_reserve_exact(wire.release.sources.len())
            .map_err(|_| FredProtocolError::InvalidField("release sources"))?;
        for source in wire.release.sources {
            validate_strings(
                [source.name.as_str(), source.url.as_str()]
                    .into_iter()
                    .chain(source.notes.as_deref()),
                limits,
            )?;
            if source.name.is_empty() {
                return Err(FredProtocolError::InvalidField("release source"));
            }
            validate_origin_url(&source.url)?;
            sources.push(FredReleaseSource {
                name: source.name,
                url: source.url,
                notes: source.notes,
            });
        }

        let next_cursor = wire
            .next_cursor
            .map(FredReleaseCursor::try_new)
            .transpose()?;
        if wire.has_more != next_cursor.is_some() {
            return Err(FredProtocolError::InvalidField("release cursor state"));
        }

        let mut series = Vec::new();
        series
            .try_reserve_exact(wire.series.len())
            .map_err(|_| FredProtocolError::InvalidField("release series"))?;
        let mut observation_count = 0_usize;
        let mut previous_series: Option<SourceIdentifier> = None;
        let mut first_coordinate = None;
        let mut last_coordinate = None;
        for item in wire.series {
            if !valid_exact_series_id(&item.series_id) {
                return Err(FredProtocolError::InvalidField("release series id"));
            }
            let series_id = SourceIdentifier::try_from(item.series_id)
                .map_err(|_| FredProtocolError::InvalidField("release series id"))?;
            if previous_series
                .as_ref()
                .is_some_and(|previous| previous >= &series_id)
            {
                return Err(FredProtocolError::InvalidField("release series ordering"));
            }
            previous_series = Some(series_id.clone());
            validate_strings(
                [
                    item.title.as_str(),
                    item.frequency.as_str(),
                    item.units.as_str(),
                    item.seasonal_adjustment.as_str(),
                    item.last_updated.as_str(),
                    item.copyright_id.as_str(),
                    item.notes.as_str(),
                ],
                limits,
            )?;
            if item.title.is_empty()
                || item.frequency.is_empty()
                || item.units.is_empty()
                || item.seasonal_adjustment.is_empty()
                || item.copyright_id.is_empty()
                || item.observations.is_empty()
            {
                return Err(FredProtocolError::InvalidField("release series metadata"));
            }
            let last_updated = parse_utc_timestamp(&item.last_updated)?;
            let mut observations = Vec::new();
            observations
                .try_reserve_exact(item.observations.len())
                .map_err(|_| FredProtocolError::InvalidField("release observations"))?;
            let mut previous_date = None;
            for observation in item.observations {
                validate_strings(
                    [observation.date.as_str(), observation.value.as_str()],
                    limits,
                )?;
                let observation_date = parse_date(&observation.date)?;
                if previous_date.is_some_and(|previous| previous >= observation_date) {
                    return Err(FredProtocolError::InvalidField(
                        "release observation ordering",
                    ));
                }
                previous_date = Some(observation_date);
                let value = if observation.value == "." {
                    None
                } else {
                    Some(
                        Decimal::from_str_exact(&observation.value)
                            .map_err(|_| FredProtocolError::InvalidValue)?,
                    )
                };
                let coordinate = (series_id.clone(), observation_date);
                first_coordinate.get_or_insert_with(|| coordinate.clone());
                last_coordinate = Some(coordinate);
                observations.push(FredReleaseObservation {
                    observation_date,
                    raw_value: observation.value,
                    value,
                });
            }
            observation_count = observation_count
                .checked_add(observations.len())
                .filter(|count| *count <= limits.max_records)
                .ok_or(FredProtocolError::InvalidField("release page size"))?;
            series.push(FredReleaseSeries {
                series_id,
                title: item.title,
                frequency: item.frequency,
                units: item.units,
                seasonal_adjustment: item.seasonal_adjustment,
                last_updated,
                copyright_id: item.copyright_id,
                notes: item.notes,
                observations,
            });
        }
        if wire.has_more && observation_count == 0 {
            return Err(FredProtocolError::InvalidField("release page size"));
        }
        match (requested_cursor, first_coordinate.as_ref()) {
            (Some(requested), Some((series_id, date)))
                if requested.compare_coordinate(series_id, *date) != Ordering::Equal =>
            {
                return Err(FredProtocolError::InvalidField(
                    "release continuation start",
                ));
            }
            (Some(_), None) => {
                return Err(FredProtocolError::InvalidField(
                    "release continuation start",
                ));
            }
            _ => {}
        }
        if let (Some(next), Some((series_id, date))) = (next_cursor.as_ref(), last_coordinate)
            && next.compare_coordinate(&series_id, date) != Ordering::Greater
        {
            return Err(FredProtocolError::InvalidField(
                "release continuation cursor",
            ));
        }

        Ok(Self {
            has_more: wire.has_more,
            next_cursor,
            release: FredReleaseMetadata {
                release_id: wire.release.release_id,
                name: wire.release.name,
                url: wire.release.url,
                sources,
            },
            series,
            observation_count,
        })
    }

    /// Returns whether another cursor page is required.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact next cursor, only on nonterminal pages.
    pub const fn next_cursor(&self) -> Option<&FredReleaseCursor> {
        self.next_cursor.as_ref()
    }

    /// Returns release identity and publisher attribution.
    pub const fn release(&self) -> &FredReleaseMetadata {
        &self.release
    }

    /// Returns strictly ordered series segments.
    pub fn series(&self) -> &[FredReleaseSeries] {
        &self.series
    }

    /// Returns the number of observations counted against the v2 page limit.
    pub const fn observation_count(&self) -> usize {
        self.observation_count
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleasePageWire {
    has_more: bool,
    #[serde(default)]
    next_cursor: Option<String>,
    release: ReleaseWire,
    series: Vec<ReleaseSeriesWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseWire {
    release_id: u32,
    name: String,
    url: String,
    sources: Vec<ReleaseSourceWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseSourceWire {
    name: String,
    url: String,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseSeriesWire {
    series_id: String,
    title: String,
    frequency: String,
    units: String,
    seasonal_adjustment: String,
    last_updated: String,
    copyright_id: String,
    notes: String,
    observations: Vec<ReleaseObservationWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseObservationWire {
    date: String,
    value: String,
}

fn validate_origin_url(value: &str) -> Result<(), FredProtocolError> {
    let url = Url::parse(value).map_err(|_| FredProtocolError::InvalidField("release URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(FredProtocolError::InvalidField("release URL"));
    }
    Ok(())
}

fn parse_utc_timestamp(value: &str) -> Result<Timestamp, FredProtocolError> {
    if !value.ends_with('Z') {
        return Err(FredProtocolError::InvalidField("series last updated"));
    }
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| FredProtocolError::InvalidField("series last updated"))?
        .with_timezone(&Utc);
    let nanos = parsed
        .timestamp_nanos_opt()
        .ok_or(FredProtocolError::InvalidField("series last updated"))?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
