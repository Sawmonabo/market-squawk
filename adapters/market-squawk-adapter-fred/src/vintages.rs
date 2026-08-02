use market_squawk_domain::CalendarDate;
use serde::Deserialize;

use crate::series::{
    FredParseLimits, FredProtocolError, admit_body, parse_date, validate_page, validate_strings,
};

/// One validated FRED/ALFRED vintage-date page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredVintagePage {
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    count: usize,
    offset: usize,
    limit: usize,
    next_offset: Option<usize>,
    vintage_dates: Vec<CalendarDate>,
}

impl FredVintagePage {
    /// Parses the documented ascending vintage-date representation.
    pub fn parse(bytes: &[u8], limits: FredParseLimits) -> Result<Self, FredProtocolError> {
        admit_body(bytes, limits)?;
        let wire: VintagePageWire = serde_json::from_slice(bytes)?;
        validate_strings(
            [
                wire.realtime_start.as_str(),
                wire.realtime_end.as_str(),
                wire.order_by.as_str(),
                wire.sort_order.as_str(),
            ],
            limits,
        )?;
        if wire.order_by != "vintage_date" || wire.sort_order != "asc" {
            return Err(FredProtocolError::InvalidField("vintage response mode"));
        }
        validate_page(
            wire.count,
            wire.offset,
            wire.limit,
            wire.vintage_dates.len(),
            limits,
        )?;
        validate_strings(wire.vintage_dates.iter().map(String::as_str), limits)?;
        let realtime_start = parse_date(&wire.realtime_start)?;
        let realtime_end = parse_date(&wire.realtime_end)?;
        if realtime_start > realtime_end {
            return Err(FredProtocolError::InvalidField("realtime interval"));
        }
        let vintage_dates = wire
            .vintage_dates
            .iter()
            .map(|value| parse_date(value))
            .collect::<Result<Vec<_>, _>>()?;
        if vintage_dates.windows(2).any(|dates| dates[0] >= dates[1]) {
            return Err(FredProtocolError::InvalidField("vintage ordering"));
        }
        let consumed = wire
            .offset
            .checked_add(vintage_dates.len())
            .ok_or(FredProtocolError::InvalidField("page cursor"))?;
        Ok(Self {
            realtime_start,
            realtime_end,
            count: wire.count,
            offset: wire.offset,
            limit: wire.limit,
            next_offset: (consumed < wire.count).then_some(consumed),
            vintage_dates,
        })
    }

    /// Returns the next required offset, or `None` for the terminal page.
    pub const fn next_offset(&self) -> Option<usize> {
        self.next_offset
    }

    /// Returns the strictly ascending vintage dates.
    pub fn vintage_dates(&self) -> &[CalendarDate] {
        &self.vintage_dates
    }

    /// Returns the provider's total matching vintage count.
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Returns this page's zero-based offset.
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the provider-declared limit.
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Returns the closed realtime interval start.
    pub const fn realtime_start(&self) -> CalendarDate {
        self.realtime_start
    }

    /// Returns the closed realtime interval end.
    pub const fn realtime_end(&self) -> CalendarDate {
        self.realtime_end
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VintagePageWire {
    realtime_start: String,
    realtime_end: String,
    order_by: String,
    sort_order: String,
    count: usize,
    offset: usize,
    limit: usize,
    vintage_dates: Vec<String>,
}
