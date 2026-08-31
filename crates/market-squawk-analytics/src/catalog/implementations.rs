//! Code-owned implementation identities and their exact permitted feature keys.

use std::num::NonZeroU32;

use market_squawk_domain::feature_dataset_macro_components_v1;

use super::{KnownFeatureImplementation, RequiredLiveFeature};
use crate::metadata::digest::implementation_digest_for_identity;
use crate::{FeatureImplementationDigest, FeatureKey, FeatureMetadata, FeatureMetadataError};

impl KnownFeatureImplementation {
    /// Every implementation identity compiled into this release.
    pub const ALL: [Self; 22] = [
        Self::LiveSpread,
        Self::LiveMidpoint,
        Self::LiveMicroprice,
        Self::LiveBookImbalance,
        Self::LiveOrderFlowImbalance,
        Self::LiveDepthWeightedPrice,
        Self::LiveAggressorImbalance,
        Self::LiveRollingVwap,
        Self::LiveVolumeVelocity,
        Self::LiveMomentum,
        Self::LiveRollingReturn,
        Self::LiveRollingVolatility,
        Self::LiveCrossVenueDivergence,
        Self::LiveAvailableLiquidity,
        Self::LiveSlippage,
        Self::BatchReturns,
        Self::BatchRisk,
        Self::BatchFactors,
        Self::BatchFundamentals,
        Self::BatchMacro,
        Self::BatchPortfolioScenarios,
        Self::BatchHarmonicPatterns,
    ];

    /// Returns the SHA-256 digest of this source-owned implementation identity.
    ///
    /// # Errors
    ///
    /// Fails closed if the cryptographic digest ever equals the reserved all-zero sentinel.
    pub fn implementation_digest(
        self,
    ) -> Result<FeatureImplementationDigest, FeatureMetadataError> {
        implementation_digest_for_identity(self.identity().as_bytes())
    }

    fn identity(self) -> &'static str {
        match self {
            Self::LiveSpread => "market-squawk-analytics::book::spread@v1",
            Self::LiveMidpoint => "market-squawk-analytics::book::midpoint@v1",
            Self::LiveMicroprice => "market-squawk-analytics::book::microprice@v1",
            Self::LiveBookImbalance => "market-squawk-analytics::book::imbalance@v1",
            Self::LiveOrderFlowImbalance => {
                "market-squawk-analytics::book::order-flow-imbalance@v1"
            }
            Self::LiveDepthWeightedPrice => {
                "market-squawk-analytics::book::depth-weighted-price@v1"
            }
            Self::LiveAggressorImbalance => {
                "market-squawk-analytics::trade::aggressor-imbalance@v1"
            }
            Self::LiveRollingVwap => "market-squawk-analytics::rolling::vwap@v1",
            Self::LiveVolumeVelocity => "market-squawk-analytics::rolling::volume-velocity@v1",
            Self::LiveMomentum => "market-squawk-analytics::rolling::momentum@v1",
            Self::LiveRollingReturn => "market-squawk-analytics::rolling::return@v1",
            Self::LiveRollingVolatility => "market-squawk-analytics::rolling::volatility@v1",
            Self::LiveCrossVenueDivergence => "market-squawk-analytics::cross-venue::divergence@v1",
            Self::LiveAvailableLiquidity => {
                "market-squawk-analytics::liquidity::available-quantity@v1"
            }
            Self::LiveSlippage => "market-squawk-analytics::liquidity::slippage@v1",
            Self::BatchReturns => "market-squawk-analytics::batch::returns@v1",
            Self::BatchRisk => "market-squawk-analytics::batch::risk@v1",
            Self::BatchFactors => "market-squawk-analytics::batch::factors@v1",
            Self::BatchFundamentals => "market-squawk-analytics::batch::fundamentals@v1",
            Self::BatchMacro => "market-squawk-analytics::batch::macro@v1",
            Self::BatchPortfolioScenarios => {
                "market-squawk-analytics::batch::portfolio-scenarios@v1"
            }
            Self::BatchHarmonicPatterns => crate::harmonics::HARMONIC_IMPLEMENTATION_IDENTITY,
        }
    }

    fn permits_key(self, key: &FeatureKey) -> bool {
        if key.version() != NonZeroU32::MIN {
            return false;
        }
        match self {
            Self::LiveSpread => key.name() == RequiredLiveFeature::Spread.name(),
            Self::LiveMidpoint => key.name() == RequiredLiveFeature::Midpoint.name(),
            Self::LiveMicroprice => key.name() == RequiredLiveFeature::Microprice.name(),
            Self::LiveBookImbalance => key.name() == RequiredLiveFeature::BookImbalance.name(),
            Self::LiveOrderFlowImbalance => {
                key.name() == RequiredLiveFeature::OrderFlowImbalance.name()
            }
            Self::LiveDepthWeightedPrice => {
                key.name() == RequiredLiveFeature::DepthWeightedPrice.name()
            }
            Self::LiveAggressorImbalance => {
                key.name() == RequiredLiveFeature::AggressorImbalance.name()
            }
            Self::LiveRollingVwap => key.name() == RequiredLiveFeature::RollingVwap.name(),
            Self::LiveVolumeVelocity => key.name() == RequiredLiveFeature::VolumeVelocity.name(),
            Self::LiveMomentum => key.name() == RequiredLiveFeature::Momentum.name(),
            Self::LiveRollingReturn => key.name() == RequiredLiveFeature::RollingReturn.name(),
            Self::LiveRollingVolatility => {
                key.name() == RequiredLiveFeature::RollingVolatility.name()
            }
            Self::LiveCrossVenueDivergence => {
                key.name() == RequiredLiveFeature::CrossVenueDivergence.name()
            }
            Self::LiveAvailableLiquidity => {
                key.name() == RequiredLiveFeature::AvailableLiquidity.name()
            }
            Self::LiveSlippage => key.name() == RequiredLiveFeature::Slippage.name(),
            Self::BatchReturns => matches!(
                key.name(),
                "research.price-return" | "research.total-return" | "research.cumulative-return"
            ),
            Self::BatchRisk => matches!(
                key.name(),
                "risk.volatility"
                    | "risk.maximum-drawdown"
                    | "risk.maximum-drawdown-peak-index"
                    | "risk.maximum-drawdown-trough-index"
                    | "risk.maximum-drawdown-recovery-index"
                    | "risk.correlation"
                    | "risk.alpha"
                    | "risk.beta"
                    | "risk.sharpe"
                    | "risk.sortino"
                    | "risk.tracking-error"
                    | "risk.information-ratio"
                    | "risk.historical-var"
                    | "risk.parametric-var"
                    | "risk.expected-shortfall"
            ),
            Self::BatchFactors => matches!(
                key.name(),
                "factors.intercept" | "factors.exposure" | "factors.r-squared"
            ),
            Self::BatchFundamentals => matches!(
                key.name(),
                "fundamentals.growth"
                    | "fundamentals.margin"
                    | "fundamentals.valuation-multiple"
                    | "fundamentals.free-cash-flow"
                    | "fundamentals.free-cash-flow-yield"
                    | "fundamentals.earnings-surprise"
            ),
            Self::BatchMacro => {
                matches!(
                    key.name(),
                    "macro.surprise"
                        | "macro.yield-curve-short-rate"
                        | "macro.yield-curve-middle-rate"
                        | "macro.yield-curve-long-rate"
                        | "macro.yield-curve-slope"
                        | "macro.yield-curve-curvature"
                        | "macro.rate-change-average-parallel-shift"
                        | "macro.rate-change-slope"
                        | "macro.rate-change-short"
                        | "macro.rate-change-long"
                ) || feature_dataset_macro_components_v1()
                    .iter()
                    .any(|descriptor| descriptor.component_name() == key.name())
            }
            Self::BatchPortfolioScenarios => matches!(
                key.name(),
                "portfolio.net-exposure"
                    | "portfolio.gross-exposure"
                    | "portfolio.attribution-contribution"
                    | "portfolio.attribution-total"
                    | "scenario.stress-contribution"
                    | "scenario.stress-total"
            ),
            Self::BatchHarmonicPatterns => {
                key.name() == crate::harmonics::HARMONIC_PATTERN_FEATURE_NAME
            }
        }
    }
}

pub(super) const fn required_live_implementation(
    feature: RequiredLiveFeature,
) -> KnownFeatureImplementation {
    match feature {
        RequiredLiveFeature::Spread => KnownFeatureImplementation::LiveSpread,
        RequiredLiveFeature::Midpoint => KnownFeatureImplementation::LiveMidpoint,
        RequiredLiveFeature::Microprice => KnownFeatureImplementation::LiveMicroprice,
        RequiredLiveFeature::BookImbalance => KnownFeatureImplementation::LiveBookImbalance,
        RequiredLiveFeature::OrderFlowImbalance => {
            KnownFeatureImplementation::LiveOrderFlowImbalance
        }
        RequiredLiveFeature::DepthWeightedPrice => {
            KnownFeatureImplementation::LiveDepthWeightedPrice
        }
        RequiredLiveFeature::AggressorImbalance => {
            KnownFeatureImplementation::LiveAggressorImbalance
        }
        RequiredLiveFeature::RollingVwap => KnownFeatureImplementation::LiveRollingVwap,
        RequiredLiveFeature::VolumeVelocity => KnownFeatureImplementation::LiveVolumeVelocity,
        RequiredLiveFeature::Momentum => KnownFeatureImplementation::LiveMomentum,
        RequiredLiveFeature::RollingReturn => KnownFeatureImplementation::LiveRollingReturn,
        RequiredLiveFeature::RollingVolatility => KnownFeatureImplementation::LiveRollingVolatility,
        RequiredLiveFeature::CrossVenueDivergence => {
            KnownFeatureImplementation::LiveCrossVenueDivergence
        }
        RequiredLiveFeature::AvailableLiquidity => {
            KnownFeatureImplementation::LiveAvailableLiquidity
        }
        RequiredLiveFeature::Slippage => KnownFeatureImplementation::LiveSlippage,
    }
}

pub(crate) fn is_known_local_implementation(metadata: &FeatureMetadata) -> bool {
    metadata.is_code_owned()
        && KnownFeatureImplementation::ALL
            .into_iter()
            .any(|implementation| {
                implementation.permits_key(metadata.key())
                    && implementation
                        .implementation_digest()
                        .is_ok_and(|digest| digest == metadata.implementation_digest())
            })
}
