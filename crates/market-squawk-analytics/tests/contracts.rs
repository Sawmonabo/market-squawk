use market_squawk_analytics::{
    ExactFeatureRatio, FeatureScalar, FeatureValidity, FeatureValue, LiveFeatureView,
};
use market_squawk_domain::{PriceTicks, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn invalidation_removes_the_previous_ready_value() -> TestResult {
    let mut feature = FeatureValue::ready(
        FeatureScalar::PriceTicks(PriceTicks::new(10_001)),
        Timestamp::from_unix_nanos(100),
    );

    feature.invalidate(FeatureValidity::Stale, Timestamp::from_unix_nanos(101))?;

    assert_eq!(feature.validity(), FeatureValidity::Stale);
    assert_eq!(feature.value(), None);
    assert_eq!(feature.observed_at(), Timestamp::from_unix_nanos(101));
    Ok(())
}

#[test]
fn exact_ratios_have_one_canonical_integer_representation() -> TestResult {
    let cases = [
        (i128::MIN, i128::MIN.unsigned_abs(), -1, 1),
        (6, 8, 3, 4),
        (-6, 8, -3, 4),
        (0, u128::MAX, 0, 1),
    ];

    for (numerator, denominator, expected_numerator, expected_denominator) in cases {
        let ratio = ExactFeatureRatio::try_new(numerator, denominator)?;
        assert_eq!(ratio.numerator(), expected_numerator);
        assert_eq!(ratio.denominator().get(), expected_denominator);
    }

    Ok(())
}

#[allow(dead_code)]
fn live_view_is_object_safe(view: &dyn LiveFeatureView) {
    let _ = view;
}
