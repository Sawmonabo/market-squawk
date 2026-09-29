//! Original event-label selection and immutable outcome publication; no probability-to-price path.
use super::super::{ForecastServingEvidence, current_input, persistence::VintageRecord};
use super::*;
use market_squawk_data::{
    AnalyticalReadLimit, DatasetId, FeatureDatasetProductContract, ForecastDatasetReadLimits,
    ForecastProbabilityOutcome, ProbabilityEventTarget,
};
use market_squawk_modeling::{ForecastTargetMeaning, ForecastVintage};

pub(crate) enum EventOutcomePreparation {
    NotEvent,
    NotYetMature,
    Unavailable,
    Prepared {
        manifest: DatasetManifestRef,
        as_of: Timestamp,
    },
}

fn contract(event: ProbabilityEventTarget) -> FeatureDatasetProductContract {
    use FeatureDatasetProductContract::*;
    match event {
        ProbabilityEventTarget::PriceHigher => {
            PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1
        }
        ProbabilityEventTarget::BenchmarkOutperformance { .. } => {
            PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1
        }
        ProbabilityEventTarget::ProfitAfterCosts { .. } => {
            PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1
        }
    }
}

async fn original_vintage(
    service: &ModelDomainService,
    token: Uuid,
    analytical: &AnalyticalReadCapability,
    context: &ForecastEvidenceReadContext,
) -> Result<
    Option<(
        VintageRecord,
        ForecastVintage,
        market_squawk_modeling::ModelMetadata,
    )>,
    ForecastApplicationError,
> {
    context.ensure_live()?;
    let forecasts = service
        .forecasts
        .as_ref()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let record = {
        let index = forecasts.selected_index(token, &context.artifact).await?;
        product_vintage(&index, token)?.clone()
    };
    let image = service.read_image.load();
    let (model_id, bundle_id, version) = record.typed_model_coordinate()?;
    let bundle = image
        .registry
        .get(&bundle_id, version)
        .map_err(|_| ForecastApplicationError::Unavailable)?
        .ok_or(ForecastApplicationError::Unavailable)?;
    if bundle.metadata().model_id() != model_id {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    if !matches!(
        bundle.metadata().output_binding().target(),
        ForecastTargetMeaning::FixedHorizonEvent { .. }
    ) {
        return Ok(None);
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
    let vintage = record.revalidated_vintage(&bundle, None)?;
    if matches!(
        vintage.path().output_binding().target(),
        ForecastTargetMeaning::FixedHorizonEvent { .. }
    ) {
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
        current_input::current_price_feature_values(bundle.metadata(), coordinate)
            .map_err(ForecastApplicationError::CurrentInputRead)?;
        if ForecastServingEvidence::from_current_price_output(&output, index, cohort.as_ref())?
            != serving
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
    }
    let ForecastTargetMeaning::FixedHorizonEvent { event, .. } =
        vintage.path().output_binding().target()
    else {
        return Err(ForecastApplicationError::CorruptIndex);
    };
    let original_analysis = record.analysis_evidence()?;
    let analysis = analytical
        .forecast_dataset_evidence(
            contract(event),
            original_analysis.manifest(),
            vintage.path().available_at(),
            ForecastDatasetReadLimits::try_new(100_000, 256 * 1024 * 1024)
                .map_err(map_read_error)?,
            context.artifact.deadline(),
            context.artifact.cancellation().clone(),
        )
        .await
        .map_err(map_read_error)?;
    if analysis.probability_event_target() != Some(event)
        || analysis
            .dataset()
            .production_receipt()
            .production_identity()
            != original_analysis.production_identity_sha256()
        || analysis.dataset().production_receipt().receipt_sha256()
            != original_analysis.production_receipt_sha256()
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    Ok(Some((record, vintage, bundle.metadata().clone())))
}

struct SelectedEventOutcome {
    source: ForecastProbabilityOutcome,
    benchmark_origin: Option<Value>,
}
impl std::ops::Deref for SelectedEventOutcome {
    type Target = ForecastProbabilityOutcome;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

/// A benchmark baseline comes from the exact Analysis parents, never a current lookup by symbol.
/// Complete source owners are reopened sequentially and discarded after retaining their receipts.
async fn benchmark_original(
    service: &ModelDomainService,
    analytical: &AnalyticalReadCapability,
    evidence: &market_squawk_data::ForecastDatasetEvidence,
    instrument: market_squawk_domain::InstrumentId,
    origin: Timestamp,
    cutoff: Timestamp,
    context: &ForecastEvidenceReadContext,
) -> Result<Option<(market_squawk_data::Sha256Digest, Value)>, ForecastApplicationError> {
    use crate::application::market_calendar::CompletedMarketSessionReference;
    use market_squawk_data::MarketHistorySelectionPolicy;
    let parents = evidence.dataset().generation().parents();
    if parents.len() > 1024 {
        return Err(ForecastApplicationError::Capacity);
    }
    let mut found = None;
    for parent in parents {
        context.ensure_live()?;
        let Some(request) = analytical
            .exact_canonical_market_bar_history_window(
                instrument,
                parent.manifest().content_hash(),
                MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1,
                cutoff,
                context.artifact.deadline(),
                context.artifact.cancellation(),
            )
            .map_err(map_read_error)?
        else {
            continue;
        };
        if request.exact_manifest() != Some(parent.manifest()) {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let Some(history) = analytical
            .read_canonical_market_bar_history(
                request,
                context.artifact.deadline(),
                context.artifact.cancellation().clone(),
            )
            .await
            .map_err(map_read_error)?
        else {
            continue;
        };
        let receipt = history.selection().receipt();
        let calendar = if let Some(graph) = receipt.date_windows() {
            let retained = graph.calendar();
            let reference = CompletedMarketSessionReference::try_from_retained_digests(
                retained.origin_content_digest,
                retained.capture_binding_digest,
            )
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
            let crate::application::market_calendar::ForecastSessionReadCapability::Current(reader) =
                service
                    .forecast_calendar
                    .as_ref()
                    .ok_or(ForecastApplicationError::Unavailable)?
            else {
                return Err(ForecastApplicationError::Unavailable);
            };
            Some(
                reader
                    .read_reference(
                        &reference,
                        cutoff,
                        context.artifact.deadline(),
                        context.artifact.cancellation().clone(),
                    )
                    .await
                    .map_err(event_calendar_error)?
                    .ok_or(ForecastApplicationError::Unavailable)?,
            )
        } else {
            None
        };
        for bar in history.bars() {
            context.ensure_live()?;
            let mut session_coordinate = None;
            let calendar_proof = if let Some(date) = bar.time_semantics().nominal_daily_date() {
                let calendar = calendar
                    .as_ref()
                    .ok_or(ForecastApplicationError::CorruptIndex)?;
                let Some(session) = calendar.date_session_on(date.date(), cutoff, cutoff) else {
                    continue;
                };
                if session.closes_at_exclusive() != origin {
                    continue;
                }
                session_coordinate = Some((
                    session.date(),
                    session.opens_at(),
                    session.closes_at_exclusive(),
                ));
                Some(
                    json!({"reference": session.reference(), "date": session.date(),
                    "opensAtUnixNanos": session.opens_at().unix_nanos().to_string(),
                    "closesAtUnixNanos": session.closes_at_exclusive().unix_nanos().to_string(),
                    "availableAtUnixNanos": session.available_at().unix_nanos().to_string(),
                    "evidenceSha256": hex(session.evidence_digest().bytes())}),
                )
            } else {
                if calendar.is_some() || bar.completed_at() != Some(origin) {
                    continue;
                }
                None
            };
            let observation = ForecastProbabilityOutcome::original_observation_identity(bar)
                .map_err(map_read_error)?;
            use sha2::{Digest, Sha256};
            let identity = market_squawk_data::Sha256Digest::new(
                Sha256::digest(
                    serde_json::to_vec(&(observation.bytes(), session_coordinate))
                        .map_err(|_| ForecastApplicationError::CorruptIndex)?,
                )
                .into(),
            );
            if found.is_some() {
                return Err(ForecastApplicationError::CorruptIndex);
            }
            found = Some((
                identity,
                json!({"manifest": manifest_value(parent.manifest()),
                "instrumentId": instrument, "originUnixNanos": origin.unix_nanos().to_string(),
                "knowledgeCutoffUnixNanos": cutoff.unix_nanos().to_string(),
                "observationSha256": hex(identity.bytes()),
                "selectionReceiptSha256": hex(receipt.receipt_digest().bytes()),
                "captureReceiptSha256": hex(receipt.capture_receipt_digest().bytes()),
                "readResultSha256": hex(history.read_receipt().result_digest().bytes()),
                "historyContentSha256": hex(history.read_receipt().history_content_digest().bytes()),
                "calendar": calendar_proof}),
            ));
        }
    }
    Ok(found)
}

fn event_calendar_error(
    error: crate::application::market_calendar::CompletedMarketSessionError,
) -> ForecastApplicationError {
    use crate::application::market_calendar::CompletedMarketSessionError as Error;
    use market_squawk_services::ServiceError;
    ForecastApplicationError::CurrentInputRead(match error {
        Error::Cancelled => ServiceError::Cancelled,
        Error::DeadlineExceeded => ServiceError::DeadlineExceeded,
        Error::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        Error::InvalidRequest => ServiceError::InvalidRequest,
        Error::InvalidEvidence => ServiceError::InvalidResult,
        Error::Unavailable => ServiceError::Unavailable,
    })
}

async fn select(
    service: &ModelDomainService,
    service_record: &VintageRecord,
    vintage: &ForecastVintage,
    manifest: &DatasetManifestRef,
    as_of: Timestamp,
    analytical: &AnalyticalReadCapability,
    context: &ForecastEvidenceReadContext,
) -> Result<Option<SelectedEventOutcome>, ForecastApplicationError> {
    let ForecastTargetMeaning::FixedHorizonEvent {
        event,
        horizon_nanos,
        origin_basis,
    } = vintage.path().output_binding().target()
    else {
        return Err(ForecastApplicationError::InvalidRecord);
    };
    let origin = vintage
        .path()
        .observed_cutoff()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    let target = origin
        .checked_add_nanos(
            i64::try_from(horizon_nanos.get())
                .map_err(|_| ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
    let limits =
        ForecastDatasetReadLimits::try_new(100_000, 256 * 1024 * 1024).map_err(map_read_error)?;
    let evidence = match analytical
        .forecast_dataset_evidence(
            contract(event),
            manifest,
            as_of,
            limits,
            context.artifact.deadline(),
            context.artifact.cancellation().clone(),
        )
        .await
    {
        Ok(value) => value,
        Err(market_squawk_data::AnalyticalReadError::ForecastDatasetUnavailable) => {
            return Ok(None);
        }
        Err(error) => return Err(map_read_error(error)),
    };
    let Some(source) = evidence
        .select_probability_outcome(event, vintage.path().instrument_id(), origin, target)
        .map_err(map_read_error)?
    else {
        return Ok(None);
    };
    let serving = service_record.serving_evidence()?;
    if source.origin_basis() != origin_basis
        || serving
            .origin_bar()
            .is_none_or(|bar| !source.matches_origin_observation(bar))
    {
        return Ok(None);
    }
    let benchmark_origin = if let ProbabilityEventTarget::BenchmarkOutperformance {
        benchmark_instrument_id,
        ..
    } = event
    {
        let original = service_record.analysis_evidence()?;
        let saved = analytical
            .forecast_dataset_evidence(
                contract(event),
                original.manifest(),
                vintage.path().available_at(),
                limits,
                context.artifact.deadline(),
                context.artifact.cancellation().clone(),
            )
            .await
            .map_err(map_read_error)?;
        if saved.probability_event_target() != Some(event)
            || saved.dataset().production_receipt().production_identity()
                != original.production_identity_sha256()
            || saved.dataset().production_receipt().receipt_sha256()
                != original.production_receipt_sha256()
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let Some((saved_identity, saved_proof)) = benchmark_original(
            service,
            analytical,
            &saved,
            benchmark_instrument_id,
            origin,
            serving.knowledge_cutoff(),
            context,
        )
        .await?
        else {
            return Ok(None);
        };
        let Some((later_identity, later_proof)) = benchmark_original(
            service,
            analytical,
            &evidence,
            benchmark_instrument_id,
            origin,
            as_of,
            context,
        )
        .await?
        else {
            return Ok(None);
        };
        if saved_identity != later_identity {
            return Ok(None);
        }
        Some(json!({"original": saved_proof, "outcome": later_proof}))
    } else {
        None
    };
    Ok(Some(SelectedEventOutcome {
        source,
        benchmark_origin,
    }))
}

/// A later genuine AnalysisV1 publication may settle the exact saved origin. Missing labels do
/// not become false; each event is searched independently under its LocalAnalysis rights.
pub(crate) async fn prepare(
    service: &ModelDomainService,
    token: Uuid,
    analytical: &AnalyticalReadCapability,
    context: &ForecastEvidenceReadContext,
) -> Result<EventOutcomePreparation, ForecastApplicationError> {
    let Some((record, vintage, _)) = original_vintage(service, token, analytical, context).await?
    else {
        return Ok(EventOutcomePreparation::NotEvent);
    };
    let ForecastTargetMeaning::FixedHorizonEvent { event, .. } =
        vintage.path().output_binding().target()
    else {
        return Ok(EventOutcomePreparation::NotEvent);
    };
    let [terminal] = vintage.path().points() else {
        return Err(ForecastApplicationError::CorruptIndex);
    };
    let target = terminal
        .target_at()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    let maturity = match event {
        ProbabilityEventTarget::ProfitAfterCosts { policy } => target
            .checked_add_nanos(policy.maximum_exit_lag_nanos)
            .map_err(|_| ForecastApplicationError::CorruptIndex)?,
        _ => target,
    };
    let as_of = wall_now()?;
    if as_of < maturity {
        return Ok(EventOutcomePreparation::NotYetMature);
    }
    let limit = AnalyticalReadLimit::try_new(64).map_err(map_read_error)?;
    let mut after: Option<DatasetId> = None;
    let mut examined = 0usize;
    let mut found: Option<SelectedEventOutcome> = None;
    loop {
        context.ensure_live()?;
        let page = analytical
            .feature_datasets(
                contract(event),
                after.as_ref(),
                limit,
                context.artifact.deadline(),
                context.artifact.cancellation(),
            )
            .map_err(map_read_error)?;
        if page.datasets().is_empty() {
            if page.has_more() {
                return Err(ForecastApplicationError::CorruptIndex);
            }
            break;
        }
        examined = examined
            .checked_add(page.datasets().len())
            .ok_or(ForecastApplicationError::Capacity)?;
        if examined > 4096 {
            return Err(ForecastApplicationError::Capacity);
        }
        for dataset in page.datasets() {
            if let Some(source) = select(
                service,
                &record,
                &vintage,
                dataset.generation().manifest(),
                as_of,
                analytical,
                context,
            )
            .await?
            {
                if found.as_ref().is_some_and(|original| {
                    original.value() != source.value()
                        || original.origin_observation_sha256()
                            != source.origin_observation_sha256()
                }) {
                    return Ok(EventOutcomePreparation::Unavailable);
                }
                // Stable catalog order chooses the first exact receipt; revisions never overwrite a saved outcome.
                if found.is_none() {
                    found = Some(source);
                }
            }
        }
        after = page
            .datasets()
            .last()
            .map(|dataset| dataset.generation().manifest().dataset_id().clone());
        if !page.has_more() {
            break;
        }
    }
    Ok(
        found.map_or(EventOutcomePreparation::Unavailable, |source| {
            EventOutcomePreparation::Prepared {
                manifest: source.fence().manifest().clone(),
                as_of,
            }
        }),
    )
}

fn source_value(source: &SelectedEventOutcome) -> Value {
    json!({
        "event": source.event(), "instrumentId": source.instrument_id(),
        "originUnixNanos": source.origin().unix_nanos().to_string(), "originBasis": source.origin_basis(),
        "targetAtUnixNanos": source.target_at().unix_nanos().to_string(),
        "labelMaturityUnixNanos": source.label_maturity().unix_nanos().to_string(),
        "availableAtUnixNanos": source.available_at().unix_nanos().to_string(),
        "value": source.value(), "quality": source.quality(),
        "lineageSha256": hex(source.lineage_sha256().bytes()),
        "originSeriesSha256": hex(source.origin_series_sha256().bytes()),
        "originObservationSha256": hex(source.origin_observation_sha256().bytes()),
        "benchmarkOrigin": source.benchmark_origin,
        "productionReceiptSha256": hex(source.production_receipt_sha256().bytes()),
        "catalogIdentitySha256": hex(source.fence().catalog_identity().bytes()),
        "manifest": manifest_value(source.fence().manifest()),
        "asOfUnixNanos": source.fence().as_of().unix_nanos().to_string(),
        "exportSha256": hex(source.fence().export_sha256().bytes()),
        "selectionSha256": hex(source.fence().selection_sha256().bytes()),
        "selectedRows": source.fence().selected_rows().get().to_string(),
    })
}

pub(super) async fn measure(
    service: &ModelDomainService,
    token: Uuid,
    record: &VintageRecord,
    vintage: &ForecastVintage,
    manifest: DatasetManifestRef,
    as_of: Timestamp,
    analytical: &AnalyticalReadCapability,
    context: &ForecastEvidenceReadContext,
) -> Result<ForecastOutcomeMeasurement, ForecastApplicationError> {
    let unavailable = || ForecastOutcomeMeasurement::Unavailable {
        forecast_token: token,
    };
    // Reopen original input and calendar, not just the stored display values.
    let (original, reopened, _) = original_vintage(service, token, analytical, context)
        .await?
        .ok_or(ForecastApplicationError::InvalidRecord)?;
    if original.vintage_id != record.vintage_id || reopened.id() != vintage.id() {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let forecasts = service
        .forecasts
        .as_ref()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let existing = {
        let index = forecasts.index_for_vintage(record.clone())?;
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
        existing.verify_measurement_artifact(
            &artifact,
            vintage,
            MeasurementSourceKind::ProbabilityEventDataset,
        )?;
        let proof: Value = serde_json::from_slice(artifact.content())
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let retained = proof
            .get("eventSource")
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let original_manifest =
            crate::application::model::outcome_measurement::parse_outcome_manifest(
                retained
                    .get("manifest")
                    .ok_or(ForecastApplicationError::CorruptIndex)?,
            )
            .map_err(ForecastApplicationError::CurrentInputRead)?;
        let original_as_of = retained
            .get("asOfUnixNanos")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
            .map(Timestamp::from_unix_nanos)
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let Some(source) = select(
            service,
            record,
            vintage,
            &original_manifest,
            original_as_of,
            analytical,
            context,
        )
        .await?
        else {
            return Ok(unavailable());
        };
        if source_value(&source) != *retained {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        existing.verify_probability_identity(vintage, &source)?;
        return Ok(ForecastOutcomeMeasurement::Recorded {
            forecast_token: token,
            outcome: record.product_outcome(&existing)?,
        });
    }
    let Some(source) = select(
        service, record, vintage, &manifest, as_of, analytical, context,
    )
    .await?
    else {
        return Ok(unavailable());
    };
    let [terminal] = vintage.path().points() else {
        return Err(ForecastApplicationError::CorruptIndex);
    };
    let scale = terminal.central().scale();
    let actual = if source.value() {
        10_i128
            .checked_pow(u32::from(scale))
            .ok_or(ForecastApplicationError::InvalidRecord)?
    } else {
        0
    };
    let recorded_at = wall_now()?;
    let proof = json!({"schemaVersion": 1, "measurementSourceKind": MeasurementSourceKind::ProbabilityEventDataset,
        "forecastVintageId": hex(vintage.id().bytes()), "forecastArtifactSha256": hex(vintage.artifact_hash().bytes()),
        "outputBindingSha256": hex(vintage.path().output_binding().identity().bytes()),
        "recordedAtUnixNanos": recorded_at.unix_nanos().to_string(),
        "actualMantissa": actual.to_string(), "decimalScale": scale, "rounding": "half_even", "eventSource": source_value(&source)});
    context.ensure_live()?;
    let artifact = forecasts
        .artifacts
        .publish(
            ArtifactPublication::try_json(
                serde_json::to_vec(&proof).map_err(|_| ForecastApplicationError::InvalidRecord)?,
            )?,
            ArtifactPublicationContext::new(
                context.artifact.cancellation().clone(),
                context.artifact.deadline(),
            ),
        )
        .await?;
    let outcome = ForecastOutcome::try_from_probability_observation(
        vintage,
        &source,
        digest_from_hex(artifact.sha256())?,
    )
    .map_err(|_| ForecastApplicationError::InvalidRecord)?;
    context.ensure_live()?;
    forecasts
        .append_outcome(&outcome, record, &artifact, recorded_at)
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

pub(in crate::application::model::forecast) async fn exact_probability(
    service: &ModelDomainService,
    token: Uuid,
    as_of: Timestamp,
    analytical: &AnalyticalReadCapability,
    context: &ForecastEvidenceReadContext,
) -> Result<super::super::ReopenedProbabilityForecast, ForecastApplicationError> {
    if as_of > wall_now()? {
        return Err(ForecastApplicationError::InvalidRecord);
    }
    let (record, vintage, metadata) = original_vintage(service, token, analytical, context)
        .await?
        .ok_or(ForecastApplicationError::InvalidRecord)?;
    let [terminal] = vintage.path().points() else {
        return Err(ForecastApplicationError::CorruptIndex);
    };
    let target = terminal
        .target_at()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    if vintage.created_at() > as_of
        || vintage.path().available_at() > as_of
        || vintage.expires_at() <= as_of
        || target <= as_of
    {
        return Err(ForecastApplicationError::NotFound);
    }
    let serving_evidence = record.serving_evidence()?;
    if serving_evidence.knowledge_cutoff() > as_of {
        return Err(ForecastApplicationError::Unavailable);
    }
    context.ensure_live()?;
    Ok(super::super::ReopenedProbabilityForecast {
        vintage,
        model_metadata: metadata,
        serving_evidence,
    })
}

/// Product reads reopen original event inputs and any retained outcome receipt after restart.
pub(in crate::application::model::forecast) async fn validate_for_read(
    service: &ModelDomainService,
    token: Uuid,
    request: &market_squawk_services::RequestContext,
) -> Result<(), ForecastApplicationError> {
    let forecasts = service
        .forecasts
        .as_ref()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let descriptor = forecasts
        .catalog
        .get(
            forecasts.catalog.head()?,
            market_squawk_data::ForecastInventoryLookup::Token(&token.to_string()),
        )?
        .ok_or(ForecastApplicationError::NotFound)?;
    let descriptor = super::super::persistence::StoredVintageRecord::decode(&descriptor)?;
    if descriptor.summary["target"]["valueKind"].as_str() != Some("probability") {
        return Ok(());
    }
    let (record, existing) = {
        let index = forecasts
            .selected_index(
                token,
                &market_squawk_services::ArtifactReadContext::new(
                    request.cancellation().clone(),
                    request.deadline(),
                ),
            )
            .await?;
        let record = product_vintage(&index, token)?.clone();
        let existing = index
            .outcomes
            .iter()
            .find(|outcome| outcome.vintage_id == record.vintage_id)
            .cloned();
        (record, existing)
    };
    if !record.is_probability_event() {
        return Ok(());
    }
    let analytical = service
        .forecast_analytical
        .as_ref()
        .ok_or(ForecastApplicationError::Unavailable)?;
    let context = ForecastEvidenceReadContext::new(
        market_squawk_services::ArtifactReadContext::new(
            request.cancellation().clone(),
            request.deadline(),
        ),
        std::num::NonZeroUsize::new(super::super::MAXIMUM_FORECAST_ARTIFACT_BYTES)
            .ok_or(ForecastApplicationError::Capacity)?,
    );
    let (_, vintage, _) = original_vintage(service, token, analytical, &context)
        .await?
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    if let Some(existing) = existing {
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
        existing.verify_measurement_artifact(
            &artifact,
            &vintage,
            MeasurementSourceKind::ProbabilityEventDataset,
        )?;
        let proof: Value = serde_json::from_slice(artifact.content())
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let retained = proof
            .get("eventSource")
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let manifest = crate::application::model::outcome_measurement::parse_outcome_manifest(
            retained
                .get("manifest")
                .ok_or(ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(ForecastApplicationError::CurrentInputRead)?;
        let as_of = retained
            .get("asOfUnixNanos")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
            .map(Timestamp::from_unix_nanos)
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let source = select(
            service, &record, &vintage, &manifest, as_of, analytical, &context,
        )
        .await?
        .ok_or(ForecastApplicationError::Unavailable)?;
        if source_value(&source) != *retained {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        existing.verify_probability_identity(&vintage, &source)?;
    }
    context.ensure_live()?;
    Ok(())
}
