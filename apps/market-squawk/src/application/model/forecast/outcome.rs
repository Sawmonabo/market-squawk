//! Exact completed-bar outcome measurement over immutable forecast and source artifacts.

mod current;
mod event;
pub(crate) use event::EventOutcomePreparation;
pub(in crate::application::model) use event::prepare as prepare_event;
pub(super) use event::{exact_probability, validate_for_read as validate_event_for_read};
mod preparation;
use crate::application::research::corporate_actions::{
    SourceAppliedCorporateActionPlanReference, SourceAppliedCorporateActionReadCapability,
};
pub(super) use current::rounded_measurement as rounded_current_measurement;
pub(crate) use preparation::ForecastOutcomePreparationOrigin;
pub(super) use preparation::read_origin as read_preparation_origin;

use market_squawk_data::{
    AnalyticalReadCapability, DatasetManifestRef, OutcomeMarketBarRequest,
    OutcomeMarketBarSelection, OutcomeMarketBarSeries,
};
use market_squawk_domain::{DataQuality, MarketBarObservation, Timestamp};
use market_squawk_modeling::{ForecastMeasurement, ForecastOutcome, ForecastValue};
use market_squawk_services::{
    ArtifactPublication, ArtifactPublicationContext, ArtifactReadRequest,
};
use rust_decimal::RoundingStrategy;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    ForecastApplicationError, ForecastEvidenceReadContext, ModelDomainService,
    persistence::{digest_from_hex, hex},
    product_vintage,
};

/// Closed source families for the single active measurement proof schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MeasurementSourceKind {
    TimestampedBars,
    CurrentInputSourceActions,
    ProbabilityEventDataset,
}

/// One immutable measured outcome or an honest absence of the required completed observations.
#[derive(Clone, Debug)]
pub(crate) enum ForecastOutcomeMeasurement {
    Recorded {
        forecast_token: Uuid,
        outcome: Value,
    },
    Unavailable {
        forecast_token: Uuid,
    },
}

impl ForecastOutcomeMeasurement {
    pub(crate) fn product_value(self) -> Value {
        match self {
            Self::Recorded {
                forecast_token,
                outcome,
            } => json!({
                "forecastToken": forecast_token, "state": "recorded", "outcome": outcome,
            }),
            Self::Unavailable { forecast_token } => json!({
                "forecastToken": forecast_token, "state": "unavailable", "outcome": null,
            }),
        }
    }
}

pub(super) async fn measure(
    service: &ModelDomainService,
    token: Uuid,
    outcome_manifest: DatasetManifestRef,
    as_of: Timestamp,
    source_action_reference: Option<&SourceAppliedCorporateActionPlanReference>,
    analytical: &AnalyticalReadCapability,
    source_actions: Option<&SourceAppliedCorporateActionReadCapability>,
    context: ForecastEvidenceReadContext,
) -> Result<ForecastOutcomeMeasurement, ForecastApplicationError> {
    context.ensure_live()?;
    let now = wall_now()?;
    if as_of > now {
        return Err(ForecastApplicationError::InvalidRecord);
    }
    let forecasts = service
        .forecasts
        .as_ref()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let _publication = tokio::select! {
        biased;
        _ = context.artifact.cancellation().cancelled() => {
            return Err(market_squawk_services::ArtifactError::Cancelled.into());
        }
        _ = tokio::time::sleep_until(context.artifact.deadline().into()) => {
            return Err(market_squawk_services::ArtifactError::DeadlineExceeded.into());
        }
        guard = forecasts.publication.lock() => guard,
    };
    context.ensure_live()?;
    let index = forecasts.selected_index(token, &context.artifact).await?;
    let record = product_vintage(&index, token)?.clone();
    drop(index);
    let image = service.read_image.load();
    let (model_id, bundle_id, bundle_version) = record.typed_model_coordinate()?;
    let bundle = image
        .registry
        .get(&bundle_id, bundle_version)
        .map_err(|_| ForecastApplicationError::Unavailable)?
        .ok_or(ForecastApplicationError::Unavailable)?;
    if bundle.metadata().model_id() != model_id {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let artifact = forecasts
        .artifacts
        .read(
            ArtifactReadRequest::try_new(
                record.artifact_reference()?,
                context.maximum_artifact_bytes,
            )?,
            context.artifact.clone(),
        )
        .await?;
    record.verify_artifact_read(&artifact)?;
    let retained_serving = record.serving_evidence()?;
    if retained_serving.financial_input().is_some() {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    }
    let vintage = record.revalidated_vintage(&bundle, None)?;
    let [terminal] = vintage.path().points() else {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    };
    let target_at = terminal
        .target_at()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    if target_at > as_of || vintage.created_at() > as_of {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    }
    if matches!(
        vintage.path().output_binding().target(),
        market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent { .. }
    ) {
        if source_action_reference.is_some() {
            return Err(ForecastApplicationError::InvalidRecord);
        }
        return event::measure(
            service,
            token,
            &record,
            &vintage,
            outcome_manifest,
            as_of,
            analytical,
            &context,
        )
        .await;
    }
    let source_action_reference =
        source_action_reference.ok_or(ForecastApplicationError::InvalidRecord)?;
    let source_actions = source_actions.ok_or(ForecastApplicationError::Unavailable)?;
    if retained_serving.current_price_input().is_some() {
        return current::measure(
            service,
            token,
            &record,
            &vintage,
            outcome_manifest,
            as_of,
            source_action_reference,
            analytical,
            source_actions,
            &context,
        )
        .await;
    }
    // The original outcome is immutable. Repeated measurement cannot count a revision as a new trial.
    if let Some(existing) = {
        let index = forecasts.index_for_vintage(record.clone())?;
        index
            .outcomes
            .iter()
            .find(|outcome| outcome.vintage_id == record.vintage_id)
            .cloned()
    } {
        if existing.available_at() > as_of {
            return Ok(ForecastOutcomeMeasurement::Unavailable {
                forecast_token: token,
            });
        }
        existing.verify_native_identity(&vintage)?;
        let evidence = forecasts
            .artifacts
            .read(
                ArtifactReadRequest::try_new(
                    existing.artifact_reference()?,
                    context.maximum_artifact_bytes,
                )?,
                context.artifact.clone(),
            )
            .await?;
        existing.verify_measurement_artifact(
            &evidence,
            &vintage,
            MeasurementSourceKind::TimestampedBars,
        )?;
        context.ensure_live()?;
        return Ok(ForecastOutcomeMeasurement::Recorded {
            forecast_token: token,
            outcome: record.product_outcome(&existing)?,
        });
    }
    let serving = record.serving_evidence()?;
    let Some(origin) = serving.origin_bar() else {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    };
    let Some(origin_at) = origin.completed_at() else {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    };
    // This retained measurement reader has an exact timestamp selector only. It does not
    // infer a timestamp from a nominal source date or manufacture a measured terminal bar.
    let Some(exact) = origin.time_semantics().timestamped_period() else {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    };
    let provenance = origin.context().provenance();
    let series = OutcomeMarketBarSeries::new(
        vintage.path().instrument_id(),
        provenance.source_id().clone(),
        provenance
            .venue_id()
            .cloned()
            .ok_or(ForecastApplicationError::InvalidRecord)?,
        origin.provider_instrument_id().clone(),
        origin.feed().clone(),
        origin.interval().clone(),
        origin.adjustment(),
        exact.timestamp_basis(),
        exact.session().kind(),
        exact.session().ruleset().clone(),
    );
    let target_request = OutcomeMarketBarRequest::try_new(
        outcome_manifest.clone(),
        series.clone(),
        as_of,
        target_at,
        target_at,
    )
    .map_err(map_read_error)?;
    let selected = analytical
        .select_outcome_market_bar(
            target_request,
            context.artifact.deadline(),
            context.artifact.cancellation().clone(),
        )
        .await
        .map_err(map_read_error)?;
    let OutcomeMarketBarSelection::Selected(target) = selected else {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    };
    if target.bar().completed_at() != Some(target_at) {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    }
    // Reread the origin in the same adjustment vintage as the outcome, so a later split does not
    // manufacture a loss. The original price used to forecast remains unchanged and is retained too.
    let origin_request = OutcomeMarketBarRequest::try_new(
        outcome_manifest.clone(),
        series,
        as_of,
        origin_at,
        origin_at,
    )
    .map_err(map_read_error)?;
    let selected = analytical
        .select_outcome_market_bar(
            origin_request,
            context.artifact.deadline(),
            context.artifact.cancellation().clone(),
        )
        .await
        .map_err(map_read_error)?;
    let OutcomeMarketBarSelection::Selected(rebased_origin) = selected else {
        return Ok(ForecastOutcomeMeasurement::Unavailable {
            forecast_token: token,
        });
    };
    if target.bar().currency() != origin.currency()
        || rebased_origin.bar().currency() != origin.currency()
        || rebased_origin.bar().completed_at() != Some(origin_at)
    {
        return Err(ForecastApplicationError::InvalidRecord);
    }
    let binding = vintage.path().output_binding();
    let measured = match binding.measurement() {
        ForecastMeasurement::Return
            if binding.expected_arithmetic_return_horizon_nanos()
                == vintage.path().horizon().step_nanos() =>
        {
            target
                .bar()
                .close()
                .amount()
                .checked_div(rebased_origin.bar().close().amount())
                .and_then(|ratio| ratio.checked_sub(rust_decimal::Decimal::ONE))
        }
        ForecastMeasurement::Price { currency }
            if currency == target.bar().currency()
                && rebased_origin.bar().close() == origin.close() =>
        {
            Some(target.bar().close().amount())
        }
        _ => None,
    }
    .ok_or(ForecastApplicationError::Unavailable)?;
    let scale = terminal.central().scale();
    let mut measured =
        measured.round_dp_with_strategy(u32::from(scale), RoundingStrategy::MidpointNearestEven);
    measured.rescale(u32::from(scale));
    if measured.scale() != u32::from(scale) {
        return Err(ForecastApplicationError::Unavailable);
    }
    let available_at = available(target.bar())?.max(available(rebased_origin.bar())?);
    if available_at > as_of || target.bar().completed_at() != Some(target_at) {
        return Err(ForecastApplicationError::InvalidRecord);
    }
    let recorded_at = wall_now()?;
    if recorded_at < now {
        return Err(ForecastApplicationError::Unavailable);
    }
    let proof = json!({
        "schemaVersion": 1,
        "measurementSourceKind": MeasurementSourceKind::TimestampedBars,
        "forecastVintageId": hex(vintage.id().bytes()),
        "forecastArtifactSha256": hex(vintage.artifact_hash().bytes()),
        "outputBindingSha256": hex(binding.identity().bytes()),
        "outcomeManifest": manifest_value(&outcome_manifest),
        "knowledgeCutoffUnixNanos": as_of.unix_nanos().to_string(),
        "recordedAtUnixNanos": recorded_at.unix_nanos().to_string(),
        "originalOrigin": origin,
        "adjustedOrigin": rebased_origin.bar(),
        "originReceiptSha256": hex(rebased_origin.receipt_digest().bytes()),
        "target": target.bar(),
        "targetReceiptSha256": hex(target.receipt_digest().bytes()),
        "actualMantissa": measured.mantissa().to_string(),
        "decimalScale": scale,
        "rounding": "half_even",
    });
    context.ensure_live()?;
    let proof = ArtifactPublication::try_json(
        serde_json::to_vec(&proof).map_err(|_| ForecastApplicationError::InvalidRecord)?,
    )?;
    let proof = forecasts
        .artifacts
        .publish(
            proof,
            ArtifactPublicationContext::new(
                context.artifact.cancellation().clone(),
                context.artifact.deadline(),
            ),
        )
        .await?;
    let outcome = ForecastOutcome::try_new(
        &vintage,
        target_at,
        target_at,
        available_at,
        ForecastValue::try_new(measured.mantissa(), scale)
            .map_err(|_| ForecastApplicationError::InvalidRecord)?,
        digest_from_hex(proof.sha256())?,
        outcome_quality(target.bar(), rebased_origin.bar()),
    )
    .map_err(|_| ForecastApplicationError::InvalidRecord)?;
    context.ensure_live()?;
    forecasts
        .append_outcome(&outcome, &record, &proof, recorded_at)
        .await?;
    let index = forecasts.index_for_vintage(record.clone())?;
    let retained = index
        .outcomes
        .iter()
        .find(|record| record.id() == hex(outcome.id().bytes()))
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    Ok(ForecastOutcomeMeasurement::Recorded {
        forecast_token: token,
        outcome: record.product_outcome(retained)?,
    })
}

fn wall_now() -> Result<Timestamp, ForecastApplicationError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(ForecastApplicationError::Unavailable)
}

fn available(bar: &MarketBarObservation) -> Result<Timestamp, ForecastApplicationError> {
    if matches!(
        bar.context().provenance().quality(),
        DataQuality::Modeled | DataQuality::Stale | DataQuality::Quarantined
    ) {
        return Err(ForecastApplicationError::Unavailable);
    }
    bar.context()
        .provenance()
        .availability()
        .conservative_available_at()
        .ok_or(ForecastApplicationError::InvalidRecord)
}

pub(super) fn outcome_quality(
    target: &MarketBarObservation,
    origin: &MarketBarObservation,
) -> DataQuality {
    let left = target.context().provenance().quality();
    let right = origin.context().provenance().quality();
    if left == right {
        return left;
    }
    if [left, right].contains(&DataQuality::Estimated) {
        return DataQuality::Estimated;
    }
    if [left, right].contains(&DataQuality::Indicative) {
        return DataQuality::Indicative;
    }
    DataQuality::Aggregated
}

fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({ "dataset": manifest.dataset_id().as_str(), "manifestVersion": manifest.manifest_version(),
        "schema": { "name": manifest.schema().name(), "version": manifest.schema_version().get(),
            "fingerprint": hex(manifest.schema().fingerprint()) },
        "contentHash": hex(manifest.content_hash().bytes()) })
}

fn map_read_error(error: market_squawk_data::AnalyticalReadError) -> ForecastApplicationError {
    ForecastApplicationError::CurrentInputRead(
        crate::application::research::corporate_actions::map_analytical_error(error),
    )
}
