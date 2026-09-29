//! Original forecast/source coordinates for later acquisition, never product-JSON authority.

use super::super::{ForecastServingEvidence, current_input};
use super::*;
use market_squawk_domain::{CalendarDate, InstrumentId, SourceId, VenueId};

/// Private construction follows exact saved artifact/input/calendar replay. This grants no source
/// acquisition identity and no claim that a later target observation exists.
pub(crate) struct ForecastOutcomePreparationOrigin {
    token: Uuid,
    bar: MarketBarObservation,
    origin_at: Timestamp,
    target_at: Timestamp,
    native_origin_date: Option<CalendarDate>,
    native_origin_open: Option<Timestamp>,
    venue: VenueId,
    instrument: InstrumentId,
    event: Option<market_squawk_data::ProbabilityEventTarget>,
    analysis_evidence: super::super::ForecastAnalysisEvidence,
    serving_manifest: DatasetManifestRef,
}
impl ForecastOutcomePreparationOrigin {
    pub(crate) const fn event_target(&self) -> Option<market_squawk_data::ProbabilityEventTarget> { self.event }
    pub(crate) const fn analysis_evidence(&self) -> &super::super::ForecastAnalysisEvidence { &self.analysis_evidence }
    pub(crate) const fn serving_manifest(&self) -> &DatasetManifestRef { &self.serving_manifest }
    pub(crate) const fn forecast_token(&self) -> Uuid {
        self.token
    }
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument
    }
    pub(crate) const fn origin_bar(&self) -> &MarketBarObservation {
        &self.bar
    }
    pub(crate) fn source_id(&self) -> &SourceId {
        self.bar.context().provenance().source_id()
    }
    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue
    }
    pub(crate) const fn origin_at(&self) -> Timestamp {
        self.origin_at
    }
    pub(crate) const fn target_at(&self) -> Timestamp {
        self.target_at
    }
    pub(crate) const fn native_origin_date(&self) -> Option<CalendarDate> {
        self.native_origin_date
    }
    pub(crate) const fn native_origin_open(&self) -> Option<Timestamp> {
        self.native_origin_open
    }
}

pub(in crate::application::model::forecast) async fn read_origin(
    service: &ModelDomainService,
    token: Uuid,
    analytical: &AnalyticalReadCapability,
    context: ForecastEvidenceReadContext,
) -> Result<Option<ForecastOutcomePreparationOrigin>, ForecastApplicationError> {
    context.ensure_live()?;
    let forecasts = service
        .forecasts
        .as_ref()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let record = {
        let index = forecasts.index.lock().await;
        product_vintage(&index, token)?.clone()
    };
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
    let serving = record.serving_evidence()?;
    let Some(current) = serving.current_price_input() else {
        return Ok(None);
    };
    let output = current_input::reopen_current_price_input(
        analytical,
        serving.manifest(),
        context.artifact.deadline(),
        context.artifact.cancellation().clone(),
    )
    .await
    .map_err(ForecastApplicationError::CurrentInputRead)?;
    let index = current_input::current_price_coordinate_index(&output, &current.example_id)
        .map_err(ForecastApplicationError::CurrentInputRead)?;
    let coordinate = output
        .coordinate(index)
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    let cohort = current_input::current_price_cohort_reference(current)
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
    current_input::current_price_feature_values(bundle.metadata(), coordinate)
        .map_err(ForecastApplicationError::CurrentInputRead)?;
    if ForecastServingEvidence::from_current_price_output(&output, index, cohort.as_ref())?
        != serving
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let vintage = record.revalidated_vintage(&bundle, None)?;
    let [terminal] = vintage.path().points() else {
        return Ok(None);
    };
    let epoch = coordinate.epoch();
    let (Some(origin_at), Some(target_at), Some(bar)) =
        (epoch.target_origin(), epoch.target_at(), epoch.market_bar())
    else {
        return Ok(None);
    };
    if terminal.target_at() != Some(target_at)
        || vintage.created_at() > wall_now()?
        || vintage.path().instrument_id() != epoch.instrument_id()
        || !matches!((vintage.path().output_binding().measurement(), vintage.path().output_binding().target()),
            (ForecastMeasurement::Return, market_squawk_modeling::ForecastTargetMeaning::FixedHorizonTerminal { .. })
            | (ForecastMeasurement::Probability, market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent { .. }))
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let venue = bar
        .context()
        .provenance()
        .venue_id()
        .cloned()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let origin = ForecastOutcomePreparationOrigin {
        token,
        bar: bar.clone(),
        origin_at,
        target_at,
        native_origin_date: epoch
            .named_session_origin()
            .map(|native| native.native_date()),
        native_origin_open: epoch.named_session_origin().map(|native| native.opens_at()),
        venue,
        instrument: epoch.instrument_id(),
        event: match vintage.path().output_binding().target() {
            market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent { event, .. } => Some(event),
            _ => None,
        },
        analysis_evidence: record.analysis_evidence()?,
        serving_manifest: serving.manifest().clone(),
    };
    context.ensure_live()?;
    Ok(Some(origin))
}
