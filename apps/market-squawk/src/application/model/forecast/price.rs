//! Causal arithmetic-return to price projection. The stored model output remains a return.

use market_squawk_data::Sha256Digest;
use market_squawk_modeling::{ForecastMeasurement, ForecastValue, ModelMetadata};
use rust_decimal::{Decimal, RoundingStrategy};
use sha2::{Digest as _, Sha256};

use super::{
    ForecastApplicationError, ForecastServingEvidence, SelectedPriceForecastPoint,
    SelectedPriceInterval, SelectedPriceIntervals,
};

pub(super) fn project(
    metadata: &ModelMetadata,
    serving: &ForecastServingEvidence,
    points: &mut [SelectedPriceForecastPoint],
) -> Result<Sha256Digest, ForecastApplicationError> {
    let binding = metadata.output_binding();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-price-projection/v1\0");
    digest.update(binding.identity().bytes());
    digest.update(metadata.metadata_hash().bytes());
    digest.update(serving.feature_sha256().bytes());
    match binding.measurement() {
        ForecastMeasurement::Price { currency } => {
            digest.update([1]);
            digest.update(currency.as_str().as_bytes());
        }
        ForecastMeasurement::Return
            if binding.expected_arithmetic_return_horizon_nanos().is_some() =>
        {
            let origin = serving
                .origin_bar()
                .ok_or(ForecastApplicationError::Unavailable)?;
            if serving.current_price_input().is_none() {
                super::validate_price_origin(
                    origin,
                    serving.source_id(),
                    serving
                        .observed_through()
                        .ok_or(ForecastApplicationError::Unavailable)?,
                    serving.knowledge_cutoff(),
                )?;
            }
            digest.update([2]);
            digest.update(
                serde_json::to_vec(origin).map_err(|_| ForecastApplicationError::InvalidRecord)?,
            );
            let amount = if let Some(current) = serving.current_price_input() {
                digest.update(
                    serde_json::to_vec(current)
                        .map_err(|_| ForecastApplicationError::InvalidRecord)?,
                );
                if current.current_unit_price.currency() != origin.currency() {
                    return Err(ForecastApplicationError::InvalidRecord);
                }
                current.current_unit_price.amount()
            } else {
                origin.close().amount()
            };
            convert_points(amount, points)?;
        }
        _ => return Err(ForecastApplicationError::Unavailable),
    }
    for point in points {
        digest.update(point.target_at.unix_nanos().to_be_bytes());
        hash_value(&mut digest, point.central);
        if let Some(intervals) = point.intervals {
            digest.update([1]);
            for band in [
                intervals.interval_50,
                intervals.interval_80,
                intervals.interval_95,
            ] {
                hash_value(&mut digest, band.lower);
                hash_value(&mut digest, band.upper);
            }
        } else {
            digest.update([0]);
        }
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

pub(super) fn convert_points(
    price: Decimal,
    points: &mut [SelectedPriceForecastPoint],
) -> Result<(), ForecastApplicationError> {
    for point in points.iter_mut() {
        point.central =
            price_from_return(price, point.central, RoundingStrategy::MidpointNearestEven)?;
        point.intervals = point
            .intervals
            .map(|intervals| {
                Ok::<_, ForecastApplicationError>(SelectedPriceIntervals {
                    interval_50: interval(price, intervals.interval_50)?,
                    interval_80: interval(price, intervals.interval_80)?,
                    interval_95: interval(price, intervals.interval_95)?,
                })
            })
            .transpose()?;
    }
    Ok(())
}

fn interval(
    price: Decimal,
    band: SelectedPriceInterval,
) -> Result<SelectedPriceInterval, ForecastApplicationError> {
    Ok(SelectedPriceInterval {
        lower: price_from_return(price, band.lower, RoundingStrategy::ToNegativeInfinity)?,
        upper: price_from_return(price, band.upper, RoundingStrategy::ToPositiveInfinity)?,
    })
}

fn price_from_return(
    price: Decimal,
    value: ForecastValue,
    rounding: RoundingStrategy,
) -> Result<ForecastValue, ForecastApplicationError> {
    // Twelve places, outward interval rounding and half-even means are part of projection v1.
    let arithmetic_return =
        Decimal::try_from_i128_with_scale(value.mantissa(), u32::from(value.scale()))
            .map_err(|_| ForecastApplicationError::Unavailable)?;
    let mut projected = Decimal::ONE
        .checked_add(arithmetic_return)
        .and_then(|gross| price.checked_mul(gross))
        .ok_or(ForecastApplicationError::Unavailable)?
        .round_dp_with_strategy(12, rounding);
    if projected <= Decimal::ZERO {
        return Err(ForecastApplicationError::Unavailable);
    }
    projected.rescale(12);
    if projected.scale() != 12 {
        return Err(ForecastApplicationError::Unavailable);
    }
    ForecastValue::try_new(projected.mantissa(), 12)
        .map_err(|_| ForecastApplicationError::Unavailable)
}

fn hash_value(digest: &mut Sha256, value: ForecastValue) {
    digest.update(value.mantissa().to_be_bytes());
    digest.update([value.scale()]);
}
