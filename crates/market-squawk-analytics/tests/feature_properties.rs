use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    ExactFeatureRatio, ExpectedVenueSet, FeatureValidity, TopOfBookView, VenueFeatureObservation,
    cross_venue_divergence, top_of_book_features,
};
use market_squawk_domain::{PriceTicks, QuantityLots, Timestamp, VenueId};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn translation_and_positive_scaling_preserve_exact_feature_relations() -> TestResult {
    for translation in [-1_000_i64, 0, 1_000] {
        let top = TopOfBookView::try_new(
            PriceTicks::new(10_000 + translation),
            QuantityLots::new(3)?,
            PriceTicks::new(10_004 + translation),
            QuantityLots::new(2)?,
            Timestamp::from_unix_nanos(100),
        )?;
        assert_eq!(
            top_of_book_features(top)?.spread().ready_value(),
            Some(PriceTicks::new(4))
        );
    }

    let venue_a = VenueId::try_from("venue-a")?;
    let venue_b = VenueId::try_from("venue-b")?;
    let venue_c = VenueId::try_from("venue-c")?;
    let at = Timestamp::from_unix_nanos(1_000);
    let base = [
        VenueFeatureObservation::new(&venue_a, ExactFeatureRatio::try_new(100, 1)?, at),
        VenueFeatureObservation::new(&venue_b, ExactFeatureRatio::try_new(101, 1)?, at),
    ];
    let scaled = [
        VenueFeatureObservation::new(&venue_a, ExactFeatureRatio::try_new(1_000, 1)?, at),
        VenueFeatureObservation::new(&venue_b, ExactFeatureRatio::try_new(1_010, 1)?, at),
    ];
    let maximum_venues = NonZeroUsize::new(2).ok_or("venue count")?;
    let expected_ids = [&venue_a, &venue_b];
    let expected = ExpectedVenueSet::try_new(&expected_ids, maximum_venues)?;
    let age = NonZeroU64::new(10).ok_or("maximum age")?;
    let first = cross_venue_divergence(&base, expected, age, at)?
        .ready_value()
        .ok_or("base divergence")?;
    let second = cross_venue_divergence(&scaled, expected, age, at)?
        .ready_value()
        .ok_or("scaled divergence")?;
    assert_eq!(
        first.numerator() * i128::try_from(second.denominator().get())?,
        second.numerator() * i128::try_from(first.denominator().get())?
    );

    let stale = cross_venue_divergence(
        &base,
        expected,
        NonZeroU64::MIN,
        Timestamp::from_unix_nanos(1_002),
    )?;
    assert_eq!(stale.validity(), FeatureValidity::Stale);
    assert_eq!(stale.value(), None);

    let substituted = [
        base[0],
        VenueFeatureObservation::new(&venue_c, base[1].midpoint(), at),
    ];
    let unavailable = cross_venue_divergence(&substituted, expected, age, at)?;
    assert_eq!(unavailable.validity(), FeatureValidity::Unavailable);
    assert_eq!(unavailable.value(), None);
    Ok(())
}
