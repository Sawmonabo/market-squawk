//! Allocation-free cross-venue divergence over complete bounded observations.

use std::cmp::Ordering;
use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_domain::{Timestamp, VenueId};
use thiserror::Error;

use crate::{ExactFeatureRatio, FeatureError, FeatureValidity, FeatureValue};

/// Maximum venues compared by one pure cross-venue calculation.
pub const MAX_CROSS_VENUE_OBSERVATIONS: usize = 64;

/// Borrowed validated identity set that defines complete cross-venue coverage.
#[derive(Clone, Copy, Debug)]
pub struct ExpectedVenueSet<'a> {
    venues: &'a [&'a VenueId],
}

impl<'a> ExpectedVenueSet<'a> {
    /// Validates a unique expected-venue set against caller and production bounds.
    ///
    /// # Errors
    ///
    /// Returns a typed error when fewer than two venues are supplied, a venue is duplicated, or a
    /// caller or production count bound is exceeded.
    pub fn try_new(
        venues: &'a [&'a VenueId],
        maximum_venues: NonZeroUsize,
    ) -> Result<Self, CrossVenueFeatureError> {
        if maximum_venues.get() > MAX_CROSS_VENUE_OBSERVATIONS {
            return Err(CrossVenueFeatureError::VenueBoundTooLarge);
        }
        if venues.len() < 2 {
            return Err(CrossVenueFeatureError::InsufficientExpectedVenues);
        }
        if venues.len() > maximum_venues.get() {
            return Err(CrossVenueFeatureError::VenueBoundExceeded);
        }
        for (index, venue) in venues.iter().enumerate() {
            if venues[index + 1..].contains(venue) {
                return Err(CrossVenueFeatureError::DuplicateExpectedVenue);
            }
        }
        Ok(Self { venues })
    }

    /// Returns expected venue identities without allocation.
    #[must_use]
    pub const fn venues(self) -> &'a [&'a VenueId] {
        self.venues
    }

    fn contains(self, venue_id: &VenueId) -> bool {
        self.venues.contains(&venue_id)
    }
}

/// Borrowed exact midpoint observation for one venue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VenueFeatureObservation<'a> {
    venue_id: &'a VenueId,
    midpoint: ExactFeatureRatio,
    observed_at: Timestamp,
}

impl<'a> VenueFeatureObservation<'a> {
    /// Constructs a borrowed authority-free venue observation.
    #[must_use]
    pub const fn new(
        venue_id: &'a VenueId,
        midpoint: ExactFeatureRatio,
        observed_at: Timestamp,
    ) -> Self {
        Self {
            venue_id,
            midpoint,
            observed_at,
        }
    }

    /// Returns the venue identity.
    #[must_use]
    pub const fn venue_id(self) -> &'a VenueId {
        self.venue_id
    }

    /// Returns the exact midpoint.
    #[must_use]
    pub const fn midpoint(self) -> ExactFeatureRatio {
        self.midpoint
    }

    /// Returns the midpoint observation timestamp.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }
}

/// Computes exact positive max-to-min divergence in basis points.
///
/// Every expected venue must be present exactly once and fresh. Missing, duplicate, or nonpositive
/// observations make the result unavailable; one stale venue makes the entire result stale. No
/// venue is silently omitted.
///
/// # Errors
///
/// Returns a typed error when the configured or supplied venue count exceeds production bounds, or
/// foundational invalid-state construction fails. Unrepresentable rational arithmetic is reported
/// as a value-free `Overflow` feature.
pub fn cross_venue_divergence(
    observations: &[VenueFeatureObservation<'_>],
    expected_venues: ExpectedVenueSet<'_>,
    maximum_age_nanos: NonZeroU64,
    evaluated_at: Timestamp,
) -> Result<FeatureValue<ExactFeatureRatio>, CrossVenueFeatureError> {
    if observations.len() > MAX_CROSS_VENUE_OBSERVATIONS {
        return Err(CrossVenueFeatureError::VenueBoundExceeded);
    }
    if observations.len() != expected_venues.venues.len() {
        return invalid(FeatureValidity::Unavailable, evaluated_at);
    }
    for (index, observation) in observations.iter().enumerate() {
        if !expected_venues.contains(observation.venue_id) {
            return invalid(FeatureValidity::Unavailable, evaluated_at);
        }
        if observations[index + 1..]
            .iter()
            .any(|other| other.venue_id == observation.venue_id)
        {
            return invalid(FeatureValidity::Unavailable, evaluated_at);
        }
        let age = i128::from(evaluated_at.unix_nanos())
            - i128::from(observation.observed_at.unix_nanos());
        if age < 0 {
            return invalid(FeatureValidity::TimestampRegression, evaluated_at);
        }
        if age > i128::from(maximum_age_nanos.get()) {
            return invalid(FeatureValidity::Stale, evaluated_at);
        }
        if observation.midpoint.numerator() <= 0 {
            return invalid(FeatureValidity::Unavailable, evaluated_at);
        }
    }

    let mut minimum = observations[0].midpoint;
    let mut maximum = observations[0].midpoint;
    for observation in &observations[1..] {
        match compare_ratio(observation.midpoint, minimum) {
            Some(Ordering::Less) => minimum = observation.midpoint,
            Some(_) => {}
            None => return invalid(FeatureValidity::Overflow, evaluated_at),
        }
        match compare_ratio(observation.midpoint, maximum) {
            Some(Ordering::Greater) => maximum = observation.midpoint,
            Some(_) => {}
            None => return invalid(FeatureValidity::Overflow, evaluated_at),
        }
    }

    let Some(maximum_denominator) = i128::try_from(maximum.denominator().get()).ok() else {
        return invalid(FeatureValidity::Overflow, evaluated_at);
    };
    let Some(minimum_denominator) = i128::try_from(minimum.denominator().get()).ok() else {
        return invalid(FeatureValidity::Overflow, evaluated_at);
    };
    let numerator = maximum
        .numerator()
        .checked_mul(minimum_denominator)
        .and_then(|maximum_scaled| {
            minimum
                .numerator()
                .checked_mul(maximum_denominator)
                .and_then(|minimum_scaled| maximum_scaled.checked_sub(minimum_scaled))
        })
        .and_then(|difference| difference.checked_mul(10_000));
    let denominator = maximum_denominator
        .checked_mul(minimum.numerator())
        .and_then(|value| u128::try_from(value).ok());
    match (numerator, denominator) {
        (Some(numerator), Some(denominator)) => Ok(FeatureValue::ready(
            ExactFeatureRatio::try_new(numerator, denominator)?,
            evaluated_at,
        )),
        _ => invalid(FeatureValidity::Overflow, evaluated_at),
    }
}

/// Cross-venue bounds or foundational-state failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CrossVenueFeatureError {
    /// The expected venue count exceeded the production maximum.
    #[error("cross-venue expected count exceeds its production maximum")]
    VenueBoundTooLarge,
    /// The supplied observation count exceeded the production maximum.
    #[error("cross-venue observation count exceeds its production maximum")]
    VenueBoundExceeded,
    /// Fewer than two expected venues were supplied.
    #[error("cross-venue comparison requires at least two expected venues")]
    InsufficientExpectedVenues,
    /// The expected venue set repeated an identity.
    #[error("cross-venue expected set contains a duplicate venue")]
    DuplicateExpectedVenue,
    /// Foundational feature-state construction failed.
    #[error(transparent)]
    FeatureState(#[from] FeatureError),
}

fn compare_ratio(left: ExactFeatureRatio, right: ExactFeatureRatio) -> Option<Ordering> {
    let left_denominator = i128::try_from(left.denominator().get()).ok()?;
    let right_denominator = i128::try_from(right.denominator().get()).ok()?;
    let left_scaled = left.numerator().checked_mul(right_denominator)?;
    let right_scaled = right.numerator().checked_mul(left_denominator)?;
    Some(left_scaled.cmp(&right_scaled))
}

fn invalid<T>(
    validity: FeatureValidity,
    observed_at: Timestamp,
) -> Result<FeatureValue<T>, CrossVenueFeatureError> {
    Ok(FeatureValue::invalid(validity, observed_at)?)
}
