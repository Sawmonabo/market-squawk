//! Monetary outcomes derived from the admitted direct estimator's empirical error distribution.

use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_data::Sha256Digest;
use market_squawk_domain::Timestamp;
use market_squawk_modeling::{
    AuthenticatedForecastServingBinding, ForecastTerminalDistribution, ForecastValue,
    ForecastVintage, ModelBundle,
};
use sha2::{Digest as _, Sha256};

use super::{
    ExactHorizonPriceForecastEvidence, ForecastApplicationError, ForecastPriceEvidence,
    ForecastSelectionReceipt, LatestValidForecast, SelectedPriceForecastPoint,
};

/// One positive monetary outcome with an explicit empirical probability mass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedForecastDistributionPoint {
    value: ForecastValue,
    probability_ppm: NonZeroU32,
}

impl SelectedForecastDistributionPoint {
    pub(crate) const fn value(self) -> ForecastValue {
        self.value
    }
    pub(crate) const fn probability_ppm(self) -> NonZeroU32 {
        self.probability_ppm
    }
}

/// A normalized discrete price distribution, distinct from prediction-interval endpoints.
///
/// Its exact vintage, model, residual member, causal price conversion, and selection are bound
/// into the identity. The underlying empirical-error stationarity assumption remains explicit.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectedForecastDistribution {
    identity: Sha256Digest,
    residual_distribution_identity: Sha256Digest,
    vintage_id: Sha256Digest,
    target_at: Option<Timestamp>,
    native_output: ForecastTerminalDistribution,
    serving_binding: AuthenticatedForecastServingBinding,
    points: Box<[SelectedForecastDistributionPoint]>,
}

impl SelectedForecastDistribution {
    pub(crate) const fn identity(&self) -> Sha256Digest {
        self.identity
    }
    pub(crate) const fn residual_distribution_identity(&self) -> Sha256Digest {
        self.residual_distribution_identity
    }
    pub(crate) const fn vintage_id(&self) -> Sha256Digest {
        self.vintage_id
    }
    pub(crate) const fn target_at(&self) -> Option<Timestamp> {
        self.target_at
    }
    pub(crate) fn points(&self) -> &[SelectedForecastDistributionPoint] {
        &self.points
    }
    pub(crate) const fn native_output(&self) -> &ForecastTerminalDistribution {
        &self.native_output
    }
    pub(crate) const fn serving_binding(&self) -> &AuthenticatedForecastServingBinding {
        &self.serving_binding
    }
}

impl LatestValidForecast {
    /// Returns only the normalized distribution for the exact live, calibrated price selection.
    pub(crate) fn exact_horizon_price_distribution(
        &self,
        horizon_nanos: NonZeroU64,
    ) -> Result<Option<&SelectedForecastDistribution>, ForecastApplicationError> {
        let ExactHorizonPriceForecastEvidence::Available(projection) =
            self.exact_horizon_price_projection(horizon_nanos)?
        else {
            return Ok(None);
        };
        let Some(distribution) = self.distribution.as_ref() else {
            return Ok(None);
        };
        if distribution.target_at != Some(projection.terminal.target_at())
            || distribution.vintage_id != projection.price.vintage_id()
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        Ok(Some(distribution))
    }
}

pub(super) fn project(
    price: &ForecastPriceEvidence,
    selection: &ForecastSelectionReceipt,
    vintage: &ForecastVintage,
    bundle: &ModelBundle,
    artifact_bytes: &[u8],
    financial: Option<(
        market_squawk_data::FeatureDatasetInputCoordinate<'_>,
        &market_squawk_data::PinnedQueryOutput,
    )>,
    current_price: Option<(
        market_squawk_data::FeatureDatasetInputCoordinate<'_>,
        &market_squawk_data::PinnedQueryOutput,
    )>,
) -> Result<Option<SelectedForecastDistribution>, ForecastApplicationError> {
    let Some(distribution) = bundle
        .terminal_distribution(vintage)
        .map_err(|_| ForecastApplicationError::CorruptIndex)?
    else {
        return Ok(None);
    };
    if let Some((coordinate, query)) = financial {
        let serving_binding = AuthenticatedForecastServingBinding::from_financial_artifact(
            artifact_bytes,
            &distribution,
            coordinate,
            query,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/selected-financial-distribution/v1\0");
        digest.update(distribution.identity().bytes());
        digest.update(selection.receipt_digest().bytes());
        let mut points = Vec::new();
        points
            .try_reserve_exact(distribution.points().len())
            .map_err(|_| ForecastApplicationError::Capacity)?;
        for point in distribution.points() {
            points.push(SelectedForecastDistributionPoint {
                value: point.value(),
                probability_ppm: point.probability_ppm(),
            });
        }
        return Ok(Some(SelectedForecastDistribution {
            identity: Sha256Digest::new(digest.finalize().into()),
            residual_distribution_identity: distribution.residual_distribution_identity(),
            vintage_id: Sha256Digest::new(vintage.id().bytes()),
            target_at: None,
            native_output: distribution,
            serving_binding,
            points: points.into_boxed_slice(),
        }));
    }
    let ForecastPriceEvidence::Available(price) = price else {
        return Ok(None);
    };
    let target_at = distribution
        .target_at()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    if !selection.is_exact_horizon_price_qualified(price.terminal_horizon_nanos()) {
        return Ok(None);
    }
    let serving_binding = match current_price {
        Some((coordinate, query)) => {
            AuthenticatedForecastServingBinding::from_current_price_artifact(
                artifact_bytes,
                &distribution,
                coordinate,
                query,
            )
        }
        None => AuthenticatedForecastServingBinding::from_artifact(artifact_bytes, &distribution),
    }
    .map_err(|_| ForecastApplicationError::CorruptIndex)?;
    if vintage.id().bytes() != price.vintage_id().bytes()
        || price.calibration().is_none_or(|calibration| {
            calibration.residuals_hash() != distribution.residual_distribution().residuals_hash()
        })
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let mut points: Vec<SelectedForecastDistributionPoint> = Vec::new();
    points
        .try_reserve_exact(distribution.points().len())
        .map_err(|_| ForecastApplicationError::Unavailable)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/selected-price-distribution/v1\0");
    digest.update(price.vintage_id().bytes());
    digest.update(selection.receipt_digest().bytes());
    digest.update(distribution.identity().bytes());
    digest.update(price.output_binding_identity().bytes());
    digest.update(target_at.unix_nanos().to_be_bytes());
    for point in distribution.points() {
        let mut projected = [SelectedPriceForecastPoint {
            target_at,
            central: point.value(),
            intervals: None,
        }];
        let conversion = match super::price::project(
            bundle.metadata(),
            price.serving_evidence(),
            &mut projected,
        ) {
            Ok(conversion) => conversion,
            Err(ForecastApplicationError::Unavailable) => return Ok(None),
            Err(error) => return Err(error),
        };
        let value = projected[0].central();
        if value.mantissa() <= 0 {
            // Empirical mass below zero is not clipped into a fictitious valid price distribution.
            return Ok(None);
        }
        digest.update(conversion.bytes());
        digest.update(point.probability_ppm().get().to_be_bytes());
        if let Some(previous) = points.last_mut()
            && previous.value == value
        {
            previous.probability_ppm =
                NonZeroU32::new(previous.probability_ppm.get() + point.probability_ppm().get())
                    .ok_or(ForecastApplicationError::CorruptIndex)?;
        } else {
            points.push(SelectedForecastDistributionPoint {
                value,
                probability_ppm: point.probability_ppm(),
            });
        }
    }
    if points.is_empty()
        || points.windows(2).any(|pair| pair[0].value >= pair[1].value)
        || points
            .iter()
            .map(|point| point.probability_ppm.get())
            .sum::<u32>()
            != 1_000_000
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    Ok(Some(SelectedForecastDistribution {
        identity: Sha256Digest::new(digest.finalize().into()),
        residual_distribution_identity: distribution.residual_distribution_identity(),
        vintage_id: price.vintage_id(),
        target_at: distribution.target_at(),
        native_output: distribution,
        serving_binding,
        points: points.into_boxed_slice(),
    }))
}
