//! Completed native training and feature-input jobs enter the existing forecast authority.
use super::*;
use crate::application::{
    analytical_profile::ValidatedAnalyticalProfile, model::runtime::ModelAdmissionReceipt,
    fiscal_projection_targets,
};
use market_squawk_data::{
    FeatureDatasetProductContract, FeatureLabelMeasurement, PythonDatasetSelection,
};
use std::num::NonZeroU64;
impl InstalledForecastPreparation {
    pub(in crate::service) async fn prepare_financial_completed(
        &self,
        receipt: &ModelAdmissionReceipt,
        selection: &PythonDatasetSelection,
        output: &market_squawk_data::FeatureDatasetInputEpochOutput,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<PreparedForecastJobInput, ServiceError> {
        ensure_live(context)?;
        if selection.product_contract()
            != FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1
        {
            return Err(ServiceError::InvalidRequest);
        }
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        let [epoch] = output.epochs() else {
            return Err(ServiceError::InvalidResult);
        };
        let period = epoch
            .financial_period()
            .ok_or(ServiceError::InvalidResult)?;
        let FeatureLabelMeasurement::FinancialAmount {
            role,
            basis,
            share_convention,
            ..
        } = epoch
            .financial_measurement()
            .ok_or(ServiceError::InvalidResult)?
        else {
            return Err(ServiceError::InvalidResult);
        };
        let offset = period
            .target_ordinal()
            .checked_sub(period.observed_ordinal())
            .ok_or(ServiceError::InvalidResult)?;
        if !fiscal_projection_targets().iter().any(|target| {
            target.role == role
                && target.basis == basis
                && target.share_convention == share_convention
                && target.cadence == period.cadence()
                && u32::from(target.periods_ahead.get()) == offset
        }) || epoch.source_selection_as_of() != selection.as_of()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let identity = resolve_product_identity(
            self.instruments.as_ref().ok_or(ServiceError::Unavailable)?,
            epoch.instrument_id(),
            epoch.source_selection_as_of(),
            epoch.source_selection_as_of(),
            context,
        )?;
        let validity = u64::try_from(
            profile
                .recommendation_policy()
                .parameters()
                .financial_model_max_age_nanos,
        )
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(ServiceError::InvalidRequest)?;
        let digest =
            super::super::jobs::parse_sha256(&profile.resolution().configuration_digest)?.bytes();
        authority
            .prepare_financial_job(
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                receipt,
                selection.identity().manifest(),
                epoch.example_id(),
                identity,
                digest,
                validity,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await
            .map_err(map_preparation)
    }
}
