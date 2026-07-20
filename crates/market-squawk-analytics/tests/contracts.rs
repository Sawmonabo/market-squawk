use market_squawk_analytics::{
    ExactFeatureRatio, FeatureOutputType, FeatureScalar, FeatureValidity, FeatureValue,
    LiveFeatureView, StatisticalF64, TopOfBookView, top_of_book_features,
};
use market_squawk_domain::{BasisPoints, PriceTicks, QuantityLots, Timestamp};

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

#[test]
fn every_feature_scalar_has_one_closed_output_type() -> TestResult {
    let midpoint = top_of_book_features(TopOfBookView::try_new(
        PriceTicks::new(10_000),
        QuantityLots::new(1)?,
        PriceTicks::new(10_001),
        QuantityLots::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?)?
    .midpoint()
    .ready_value()
    .ok_or("missing midpoint")?;
    let cases = [
        (
            FeatureScalar::PriceTicks(PriceTicks::new(1)),
            FeatureOutputType::PriceTicks,
        ),
        (
            FeatureScalar::HalfTickPrice(midpoint),
            FeatureOutputType::HalfTickPrice,
        ),
        (
            FeatureScalar::QuantityLots(QuantityLots::new(1)?),
            FeatureOutputType::QuantityLots,
        ),
        (
            FeatureScalar::BasisPoints(BasisPoints::new(1)),
            FeatureOutputType::BasisPoints,
        ),
        (
            FeatureScalar::SignedInteger(1),
            FeatureOutputType::SignedInteger,
        ),
        (
            FeatureScalar::UnsignedInteger(1),
            FeatureOutputType::UnsignedInteger,
        ),
        (
            FeatureScalar::ExactRatio(ExactFeatureRatio::try_new(1, 2)?),
            FeatureOutputType::ExactRatio,
        ),
        (
            FeatureScalar::Statistical(StatisticalF64::try_new(0.5)?),
            FeatureOutputType::StatisticalF64,
        ),
    ];

    for (scalar, output_type) in cases {
        assert_eq!(scalar.output_type(), output_type);
    }
    Ok(())
}

#[allow(dead_code)]
fn live_view_is_object_safe(view: &dyn LiveFeatureView) {
    let _ = view;
}
