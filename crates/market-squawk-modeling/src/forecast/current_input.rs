//! Original current StudyInputs price epoch retained by a forward forecast artifact.

use super::serving::hex;
use super::*;
use market_squawk_data::{
    DatasetBuildPurpose, FeatureDatasetInputCoordinate, FeatureDatasetProductContract,
    FixedHorizonOriginBasis, ForecastFeatureValue,
};
use market_squawk_domain::{HistoricalStudyBasis, MarketBarAdjustment, Money};
use serde::{Deserialize, Serialize};

/// Inert exact source reference. Only a genuine feature-only reread can authenticate it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForecastCurrentPriceServingRecord {
    pub example_id: String,
    pub production_identity_sha256: String,
    pub production_receipt_sha256: String,
    pub input_epoch_json: String,
    pub origin_basis: FixedHorizonOriginBasis,
    pub current_unit_price: Money,
    /// Opaque application calendar reference; source matching alone does not authenticate it.
    pub session_cohort_json: Option<String>,
}

impl ForecastCurrentPriceServingRecord {
    pub fn from_coordinate(
        coordinate: FeatureDatasetInputCoordinate<'_>,
    ) -> Result<Self, ForecastError> {
        let epoch = coordinate.epoch();
        if coordinate.dataset().product_contract()
            != FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
            || epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || epoch.basis() != HistoricalStudyBasis::HistoricalAsKnown
            || epoch.financial_period().is_some()
            || epoch
                .target_origin()
                .is_none_or(|origin| origin > epoch.source_selection_as_of())
            || epoch
                .target_at()
                .is_none_or(|target| target <= epoch.source_selection_as_of())
            || epoch
                .market_bar()
                .is_none_or(|bar| bar.adjustment() != MarketBarAdjustment::Raw)
        {
            return Err(ForecastError::InvalidRequest);
        }
        if epoch.market_bar().is_none_or(|bar| {
            matches!(
                bar.context().provenance().quality(),
                market_squawk_domain::DataQuality::Modeled
                    | market_squawk_domain::DataQuality::Stale
                    | market_squawk_domain::DataQuality::Quarantined
            )
        }) {
            return Err(ForecastError::InvalidRequest);
        }
        if !matches!(epoch.adjustment(), Some(market_squawk_data::ComponentAdjustmentEvidence::Applied { policy, .. })
            if policy.adjustment() == market_squawk_data::CorporateActionAdjustment::SplitAdjusted)
        {
            return Err(ForecastError::InvalidRequest);
        }
        let receipt = coordinate.dataset().production_receipt();
        let bytes = epoch
            .canonical_bytes()
            .map_err(|_| ForecastError::InvalidRequest)?;
        Ok(Self {
            example_id: epoch.example_id().to_owned(),
            production_identity_sha256: hex(receipt.production_identity().bytes()),
            production_receipt_sha256: hex(receipt.receipt_sha256().bytes()),
            input_epoch_json: String::from_utf8(bytes)
                .map_err(|_| ForecastError::InvalidRequest)?,
            origin_basis: epoch
                .fixed_horizon_origin_basis()
                .ok_or(ForecastError::InvalidRequest)?,
            current_unit_price: epoch
                .current_unit_price()
                .map_err(|_| ForecastError::InvalidRequest)?,
            session_cohort_json: None,
        })
    }
    pub fn matches_coordinate(&self, coordinate: FeatureDatasetInputCoordinate<'_>) -> bool {
        Self::from_coordinate(coordinate).is_ok_and(|mut actual| {
            actual
                .session_cohort_json
                .clone_from(&self.session_cohort_json);
            actual == *self
        })
    }
    pub(super) fn validate(&self) -> bool {
        self.session_cohort_json
            .as_ref()
            .is_none_or(|value| !value.is_empty() && value.len() <= 64 * 1024)
            && !self.example_id.is_empty()
            && self.example_id.len() <= 128
            && !self.input_epoch_json.is_empty()
            && self.input_epoch_json.len() <= 64 * 1024
            && self.current_unit_price.amount().mantissa() > 0
            && [
                self.production_identity_sha256.as_str(),
                self.production_receipt_sha256.as_str(),
            ]
            .iter()
            .all(|value| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && value.bytes().any(|byte| byte != b'0')
            })
            && matches!(
                self.origin_basis,
                FixedHorizonOriginBasis::CompletedBarClose
                    | FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar
            )
    }
    /// Commits the entire genuine epoch, ordered rows and row lineage without floating conversion.
    pub fn feature_identity(
        coordinate: FeatureDatasetInputCoordinate<'_>,
    ) -> Result<Sha256Digest, ForecastError> {
        let record = Self::from_coordinate(coordinate)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/current-price-forecast-input/v1\0");
        hash.update((record.input_epoch_json.len() as u64).to_be_bytes());
        hash.update(record.input_epoch_json.as_bytes());
        hash.update((coordinate.rows().len() as u64).to_be_bytes());
        for row in coordinate.rows() {
            hash.update((row.component_name().len() as u64).to_be_bytes());
            hash.update(row.component_name().as_bytes());
            hash.update(row.component_version().to_be_bytes());
            hash.update(row.lineage_sha256().bytes());
            match row.value() {
                ForecastFeatureValue::Float(value) if value.is_finite() => {
                    hash.update([1]);
                    hash.update(value.to_bits().to_be_bytes());
                }
                ForecastFeatureValue::Decimal { mantissa, scale } => {
                    hash.update([2]);
                    hash.update(mantissa.to_be_bytes());
                    hash.update([*scale]);
                }
                _ => return Err(ForecastError::InvalidRequest),
            }
        }
        Ok(Sha256Digest::new(hash.finalize().into()))
    }
}
