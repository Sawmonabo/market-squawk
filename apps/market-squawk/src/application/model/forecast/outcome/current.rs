//! Current-input measurement and replay under the existing immutable outcome publication lock.

use super::super::{ForecastServingEvidence, current_input, persistence::VintageRecord};
use super::*;
use crate::application::research::corporate_actions::{
    SourceAppliedCorporateActionPlanReference, SourceAppliedCorporateActionReadCapability,
    SourceForecastOutcomeEvidence, ApplicableActionPlanError,
};
use market_squawk_modeling::ForecastVintage;

#[allow(clippy::too_many_arguments)]
pub(super) async fn measure(
    service: &ModelDomainService,
    token: Uuid,
    record: &VintageRecord,
    vintage: &ForecastVintage,
    outcome_manifest: DatasetManifestRef,
    as_of: Timestamp,
    source_reference: &SourceAppliedCorporateActionPlanReference,
    analytical: &AnalyticalReadCapability,
    source_actions: &SourceAppliedCorporateActionReadCapability,
    context: &ForecastEvidenceReadContext,
) -> Result<ForecastOutcomeMeasurement, ForecastApplicationError> {
    let unavailable = || ForecastOutcomeMeasurement::Unavailable {
        forecast_token: token,
    };
    let serving = record.serving_evidence()?;
    let input = serving
        .current_price_input()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    let output = current_input::reopen_current_price_input(
        analytical,
        serving.manifest(),
        context.artifact.deadline(),
        context.artifact.cancellation().clone(),
    )
    .await
    .map_err(ForecastApplicationError::CurrentInputRead)?;
    let index = current_input::current_price_coordinate_index(&output, &input.example_id)
        .map_err(ForecastApplicationError::CurrentInputRead)?;
    let coordinate = output
        .coordinate(index)
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    let cohort = current_input::current_price_cohort_reference(input)
        .map_err(ForecastApplicationError::CurrentInputRead)?;
    current_input::current_price_session_origin(
        service.forecast_calendar.as_ref(),
        cohort.as_ref(),
        coordinate,
        context.artifact.deadline(),
        context.artifact.cancellation().clone(),
    )
    .await
    .map_err(ForecastApplicationError::CurrentInputRead)?;
    if ForecastServingEvidence::from_current_price_output(&output, index, cohort.as_ref())?
        != serving
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let [terminal] = vintage.path().points() else {
        return Ok(unavailable());
    };
    let target_at = terminal
        .target_at()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    if coordinate.epoch().target_at() != Some(target_at)
        || vintage.path().output_binding().measurement() != ForecastMeasurement::Return
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let forecasts = service
        .forecasts
        .as_ref()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let existing = {
        let index = forecasts.index.lock().await;
        index
            .outcomes
            .iter()
            .find(|outcome| outcome.vintage_id == record.vintage_id)
            .cloned()
    };
    if let Some(existing) = existing {
        if existing.available_at() > as_of {
            return Ok(unavailable());
        }
        existing.verify_native_identity(vintage)?;
        let artifact = forecasts
            .artifacts
            .read(
                ArtifactReadRequest::try_new(
                    existing.artifact_reference()?,
                    context.maximum_artifact_bytes,
                )?,
                context.artifact.clone(),
            )
            .await?;
        existing.verify_measurement_artifact(&artifact, vintage, MeasurementSourceKind::CurrentInputSourceActions)?;
        let proof: Value = serde_json::from_slice(artifact.content())
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let retained: SourceForecastOutcomeEvidence = serde_json::from_value(
            proof
                .get("sourceMeasurement")
                .cloned()
                .ok_or(ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let manifest = retained
            .outcome_manifest
            .typed()
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let Some(reopened) = source_actions
            .read_current_forecast_outcome(
                &retained.source_action_reference,
                coordinate,
                &manifest,
                retained.available_at(),
                analytical,
                context.artifact.deadline(),
                context.artifact.cancellation().clone(),
            )
            .await
            .map_err(|error| map_source_error(error, context))?
        else {
            return Ok(unavailable());
        };
        if reopened != retained {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        context.ensure_live()?;
        return Ok(ForecastOutcomeMeasurement::Recorded {
            forecast_token: token,
            outcome: record.product_outcome(&existing)?,
        });
    }
    let Some(source) = source_actions
        .read_current_forecast_outcome(
            source_reference,
            coordinate,
            &outcome_manifest,
            as_of,
            analytical,
            context.artifact.deadline(),
            context.artifact.cancellation().clone(),
        )
        .await
        .map_err(|error| map_source_error(error, context))?
    else {
        return Ok(unavailable());
    };
    let measured = rounded_measurement(&source, terminal.central().scale())?;
    let scale = terminal.central().scale();
    let recorded_at = wall_now()?;
    if recorded_at < source.available_at() {
        return Err(ForecastApplicationError::Unavailable);
    }
    let proof = json!({
        "schemaVersion":1,
        "measurementSourceKind":MeasurementSourceKind::CurrentInputSourceActions,
        "forecastVintageId":hex(vintage.id().bytes()),
        "forecastArtifactSha256":hex(vintage.artifact_hash().bytes()),
        "outputBindingSha256":hex(vintage.path().output_binding().identity().bytes()),
        "knowledgeCutoffUnixNanos":source.available_at().unix_nanos().to_string(),
        "recordedAtUnixNanos":recorded_at.unix_nanos().to_string(),
        "sourceMeasurement":source,
        "actualMantissa":measured.mantissa().to_string(),"decimalScale":scale,"rounding":"half_even"
    });
    let publication = ArtifactPublication::try_json(
        serde_json::to_vec(&proof).map_err(|_| ForecastApplicationError::InvalidRecord)?,
    )?;
    context.ensure_live()?;
    let published = forecasts
        .artifacts
        .publish(
            publication,
            ArtifactPublicationContext::new(
                context.artifact.cancellation().clone(),
                context.artifact.deadline(),
            ),
        )
        .await?;
    let outcome = ForecastOutcome::try_new(
        vintage,
        target_at,
        target_at,
        source.available_at(),
        ForecastValue::try_new(measured.mantissa(), scale)
            .map_err(|_| ForecastApplicationError::InvalidRecord)?,
        digest_from_hex(published.sha256())?,
        outcome_quality(&source.target, &source.reread_origin),
    )
    .map_err(|_| ForecastApplicationError::InvalidRecord)?;
    context.ensure_live()?;
    forecasts
        .append_outcome(&outcome, &published, recorded_at)
        .await?;
    let index = forecasts.index.lock().await;
    let retained = index
        .outcomes
        .iter()
        .find(|row| row.id() == hex(outcome.id().bytes()))
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    Ok(ForecastOutcomeMeasurement::Recorded {
        forecast_token: token,
        outcome: record.product_outcome(retained)?,
    })
}

pub(in crate::application::model::forecast) fn rounded_measurement(
    source: &SourceForecastOutcomeEvidence,
    scale: u8,
) -> Result<rust_decimal::Decimal, ForecastApplicationError> {
    let mut value = source
        .measured_return()
        .map_err(|_| ForecastApplicationError::CurrentInputRead(market_squawk_services::ServiceError::InvalidResult))?
        .round_dp_with_strategy(u32::from(scale), RoundingStrategy::MidpointNearestEven);
    value.rescale(u32::from(scale));
    if value.scale() != u32::from(scale) {
        return Err(ForecastApplicationError::Unavailable);
    }
    Ok(value)
}

fn map_source_error(
    error: ApplicableActionPlanError,
    context: &ForecastEvidenceReadContext,
) -> ForecastApplicationError {
    if let Err(control) = context.ensure_live() {
        return control.into();
    }
    match error {
        ApplicableActionPlanError::SourceRead(error) => ForecastApplicationError::CurrentInputRead(error),
        ApplicableActionPlanError::IncompleteOrdinaryCoverage
        | ApplicableActionPlanError::UnresolvedApplicableActions => ForecastApplicationError::Unavailable,
        ApplicableActionPlanError::InvalidEvidence => {
            ForecastApplicationError::CurrentInputRead(market_squawk_services::ServiceError::InvalidResult)
        }
        // Interrupted without an expired/cancelled request is a source control failure, not absence.
        ApplicableActionPlanError::Interrupted => {
            ForecastApplicationError::CurrentInputRead(market_squawk_services::ServiceError::Internal)
        }
    }
}
