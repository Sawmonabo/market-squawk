//! Selects a reviewed relationship while preserving the actual source calendar venue.

use market_squawk_domain::{CalendarDate, VenueId};
use market_squawk_sources::ReviewedMarketCalendarRelationship;

pub(crate) fn tiingo_calendar_source_venue(
    instrument_venue: &VenueId,
    requested: (CalendarDate, CalendarDate),
) -> Option<VenueId> {
    let native = match instrument_venue.as_str() {
        "XNYS" | "XNAS" => instrument_venue.clone(),
        "ARCX" => VenueId::try_from("XNYS").ok()?,
        _ => return None,
    };
    relationship_evidence(&native, instrument_venue, requested).map(|_| native)
}

pub(super) fn relationship_evidence(
    native_venue: &VenueId,
    instrument_venue: &VenueId,
    requested: (CalendarDate, CalendarDate),
) -> Option<ReviewedMarketCalendarRelationship> {
    ReviewedMarketCalendarRelationship::try_new(
        native_venue.clone(), instrument_venue.clone(), requested.0, requested.1,
    ).ok()
}
