//! Native-period labor and regional income over original atomic publication selectors.
use super::super::{
    BeaMacroApplicationClosure, BeaMacroApplicationError, BeaMacroCapabilityState,
    BeaProviderPeriodLatestKnownRequest, BlsMacroApplicationClosure, BlsMacroApplicationError,
    BlsMacroCapabilityState, BlsProviderPeriodLatestKnownRequest,
};
use super::*;
use market_squawk_adapter_bea::decode_bea_california_personal_income_coordinate as decode_income_coordinate;
use market_squawk_domain::ResearchPeriod;
use std::num::NonZeroU16;

const INCOME: MacroContextIndicatorDefinition = MacroContextIndicatorDefinition {
    indicator_id: "california-annual-personal-income",
    label: "California annual personal income",
    category: MacroContextCategory::Income,
    frequency: MacroContextFrequency::Annual,
    seasonal_adjustment: MacroContextSeasonalAdjustment::NotSupplied,
    unit: &INCOME_UNIT,
    source_slot: "california-annual-personal-income",
};
static INCOME_UNIT: MacroContextUnitDto = MacroContextUnitDto {
    code: Cow::Borrowed("native_income"),
    label: Cow::Borrowed("Native income unit"),
    symbol: None,
};

pub(super) struct PeriodRead {
    observation: MacroObservation,
    receipt: Arc<MacroContextSourceReceipt>,
    unit: Option<MacroContextUnitDto>,
    consulted: Vec<Arc<MacroContextSourceReceipt>>,
}

impl MacroContextReadCapability {
    async fn period_origins(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<(DatasetManifestRef, SourceId)>, ServiceError> {
        let Some(research) = self.energy_store.as_ref() else {
            return Ok(Vec::new());
        };
        let reader = self.reader.clone();
        research
            .run_owned_research_io(deadline, cancellation, move |cancel| {
                let mut cursor = None;
                let mut manifests = Vec::new();
                let mut datasets = std::collections::BTreeSet::new();
                for page_index in 0..16 {
                    let page = reader
                        .datasets(
                            cursor.as_ref(),
                            market_squawk_data::AnalyticalReadLimit::try_new(64)
                                .map_err(map_read_error)?,
                            deadline,
                            &cancel,
                        )
                        .map_err(map_read_error)?;
                    for generation in page.generations() {
                        let dataset = generation.manifest().dataset_id();
                        if !datasets.insert(dataset.as_str().to_owned()) {
                            return Err(ServiceError::InvalidResult);
                        }
                        let source = generation.source_id();
                        let selected = matches!(
                            source.as_str(),
                            "bls-bls.v1-unregistered" | "bls-bls.v2-registered"
                        ) && dataset.as_str().starts_with("bls.timeseries.")
                            || source.as_str() == "us-bea"
                                && dataset.as_str().starts_with("bea.data-v1.");
                        if selected {
                            // Each dataset binds the complete requested plan: BLS tier/series/year
                            // windows/metadata, or BEA dataset/all selectors/expected count. Another
                            // plan therefore occupies another catalog row, which this scan also visits.
                            // Atomic publications append within that dataset; the chosen manifest
                            // retains prior objects and their original capture-binding edges.
                            let (origins, _) = reader
                                .provider_capture_origin_candidates(
                                    dataset,
                                    cutoffs.knowledge_cutoff,
                                    None,
                                    market_squawk_data::AnalyticalReadLimit::try_new(1)
                                        .map_err(map_read_error)?,
                                    deadline,
                                    &cancel,
                                )
                                .map_err(map_read_error)?;
                            for origin in origins {
                                manifests.push((origin, source.clone()));
                            }
                            if manifests.len() > 64 {
                                return Err(ServiceError::ResourceExhausted);
                            }
                        }
                    }
                    if !page.has_more() {
                        return Ok(manifests);
                    }
                    if page_index == 15 {
                        return Err(ServiceError::ResourceExhausted);
                    }
                    cursor = Some(
                        page.generations()
                            .last()
                            .ok_or(ServiceError::InvalidResult)?
                            .manifest()
                            .dataset_id()
                            .clone(),
                    );
                }
                Err(ServiceError::ResourceExhausted)
            })
            .await
            .map_err(map_worker)?
    }

    pub(super) async fn read_provider_periods(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<(Option<PeriodRead>, Option<PeriodRead>), ServiceError> {
        let Some(research) = self.energy_store.as_ref() else {
            return Ok((None, None));
        };
        let Some((year, month, _)) = energy::completed_month(cutoffs.effective_date_cutoff)? else {
            return Ok((None, None));
        };
        let labor_cutoff = period("bls-monthly", year, month, format!("M{month:02}"))?;
        let income_year = if month == 12 {
            year
        } else {
            year.saturating_sub(1)
        };
        let mut labor = None;
        let mut income = None;
        let mut labor_receipts = Vec::new();
        let mut income_receipts = Vec::new();
        for (manifest, source) in self
            .period_origins(cutoffs, deadline, &cancellation)
            .await?
        {
            check_control(deadline, &cancellation)?;
            let analytical = research.analytical_service();
            let selected_manifest = manifest.clone();
            let selector = research
                .run_owned_research_io(deadline, &cancellation, move |_| {
                    analytical.recover_provider_macro_plan_selector(&selected_manifest)
                })
                .await
                .map_err(map_worker)?
                .map_err(map_ingest)?;
            if selector.source_id() != &source || selector.manifest() != &manifest {
                return Err(ServiceError::InvalidResult);
            }
            if source.as_str() != "us-bea" {
                let request = BlsProviderPeriodLatestKnownRequest::try_new(
                    selector,
                    allowlist(
                        SourceIdentifier::try_from("LNS14000000")
                            .map_err(|_| ServiceError::InvalidResult)?,
                    )?,
                    cutoffs.knowledge_cutoff,
                    labor_cutoff.clone(),
                )
                .map_err(map_bls)?;
                let limits = limits(request.required_query_rows(), deadline)?;
                let state = BlsMacroApplicationClosure::new(Arc::clone(research))
                    .read_provider_period_latest_known(
                        request,
                        limits,
                        deadline,
                        cancellation.child_token(),
                    )
                    .await
                    .map_err(map_bls)?;
                let read = match state {
                    BlsMacroCapabilityState::Available(read) => read,
                    BlsMacroCapabilityState::IncompleteAtCutoff { .. } => continue,
                    BlsMacroCapabilityState::Unavailable(_) => {
                        return Err(ServiceError::InvalidResult);
                    }
                };
                let rows = read.output().observations();
                if rows.len() != 1
                    || read.native_rows().len() != 1
                    || read.custody().rows().len() != 1
                {
                    return Err(ServiceError::InvalidResult);
                }
                let native = read.native_rows()[0].native().series();
                if native.series_id().as_str() != "LNS14000000"
                    || native.unit().as_str() != "percent"
                    || native.frequency().as_str() != "monthly"
                    || native.seasonal_adjustment().as_str() != "seasonally-adjusted"
                    || native.measure().as_str() != "unemployment-rate"
                {
                    return Err(ServiceError::InvalidResult);
                }
                let receipt = source_receipt(
                    read.output(),
                    read.custody().rows()[0].binding().binding_digest(),
                    MacroContextInternalSource::LaborMarket,
                )?;
                labor_receipts.push(Arc::clone(&receipt));
                select(
                    &mut labor,
                    PeriodRead {
                        observation: rows[0].clone(),
                        receipt,
                        unit: None,
                        consulted: Vec::new(),
                    },
                )?;
            } else if income_year > 0 {
                let coordinate = research
                    .read_provider_capture_generation(
                        manifest,
                        deadline,
                        &cancellation,
                        move |evidence, _, _, _, cancel| {
                            let mut coordinate = None;
                            let mut rows = 0_usize;
                            for object in evidence.objects() {
                                for input in object.inputs() {
                                    for row in input.binding().rows() {
                                        if cancel.is_cancelled() {
                                            return Err(crate::ResearchServiceError::Ingest(
                                                market_squawk_data::IngestError::Cancelled,
                                            ));
                                        }
                                        rows = rows.checked_add(1).ok_or(
                                            crate::ResearchServiceError::IngestAuthorityMismatch,
                                        )?;
                                        if rows > MAXIMUM_MACRO_CONTEXT_INPUTS {
                                            return Err(
                                                crate::ResearchServiceError::IngestAuthorityMismatch,
                                            );
                                        }
                                        let decoded = decode_income_coordinate(
                                            row.native_semantic_payload(),
                                        )
                                        .map_err(|_| {
                                            crate::ResearchServiceError::IngestAuthorityMismatch
                                        })?;
                                        if let Some(decoded) = decoded {
                                            if coordinate
                                                .as_ref()
                                                .is_some_and(|prior| prior != &decoded)
                                            {
                                                return Err(
                                                    crate::ResearchServiceError::IngestAuthorityMismatch,
                                                );
                                            }
                                            coordinate = Some(decoded);
                                        }
                                    }
                                }
                            }
                            Ok(coordinate)
                        },
                    )
                    .await
                    .map_err(map_worker)?;
                let Some(coordinate) = coordinate else {
                    continue;
                };
                let request = BeaProviderPeriodLatestKnownRequest::try_new(
                    selector,
                    allowlist(coordinate.canonical_series().clone())?,
                    cutoffs.knowledge_cutoff,
                    period("bea-annual", income_year, 1, format!("{income_year:04}"))?,
                )
                .map_err(map_bea)?;
                let query_limits = limits(request.required_query_rows(), deadline)?;
                let state = match BeaMacroApplicationClosure::new(Arc::clone(research))
                    .read_provider_period_latest_known(
                        request,
                        query_limits,
                        deadline,
                        cancellation.child_token(),
                    )
                    .await
                {
                    Ok(state) => state,
                    Err(BeaMacroApplicationError::AnalyticalRead(
                        market_squawk_data::AnalyticalReadError::MacroSnapshotIncomplete,
                    )) => continue,
                    Err(error) => return Err(map_bea(error)),
                };
                let BeaMacroCapabilityState::Available(read) = state else {
                    return Err(ServiceError::InvalidResult);
                };
                let output = read.output();
                if output.observations().len() != 1 {
                    return Err(ServiceError::InvalidResult);
                }
                let observation = &output.observations()[0];
                if observation.series() != coordinate.canonical_series()
                    || observation.unit() != coordinate.canonical_unit()
                {
                    return Err(ServiceError::InvalidResult);
                }
                let custody = research
                    .read_selected_provider_capture_evidence(
                        output.selected_provider_rows().map_err(map_read_error)?,
                        MACRO_CONTEXT_QUERY_BYTES as usize,
                        deadline,
                        &cancellation,
                    )
                    .await
                    .map_err(map_worker)?;
                let row = custody
                    .rows()
                    .first()
                    .filter(|_| custody.rows().len() == 1)
                    .ok_or(ServiceError::InvalidResult)?;
                let selected_coordinate =
                    market_squawk_adapter_bea::decode_bea_california_personal_income_coordinate(
                        row.row().native_semantic_payload(),
                    )
                    .map_err(|_| ServiceError::InvalidResult)?
                    .ok_or(ServiceError::InvalidResult)?;
                if selected_coordinate != coordinate {
                    return Err(ServiceError::InvalidResult);
                }
                let stored = serde_json::to_vec(&market_squawk_domain::ResearchObservation::Macro(
                    observation.clone(),
                ))
                .map_err(|_| ServiceError::InvalidResult)?;
                if EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(stored).into())
                    != row.stored_payload_digest()
                {
                    return Err(ServiceError::InvalidResult);
                }
                let receipt = source_receipt(
                    output,
                    row.binding().binding_digest(),
                    MacroContextInternalSource::RegionalIncome,
                )?;
                income_receipts.push(Arc::clone(&receipt));
                select(
                    &mut income,
                    PeriodRead {
                        observation: observation.clone(),
                        receipt,
                        unit: Some(MacroContextUnitDto {
                            code: Cow::Borrowed("native_income"),
                            label: Cow::Borrowed(
                                coordinate
                                    .canonical_unit_label()
                                    .ok_or(ServiceError::InvalidResult)?,
                            ),
                            symbol: None,
                        }),
                        consulted: Vec::new(),
                    },
                )?;
            }
        }
        if let Some(read) = labor.as_mut() {
            read.consulted = labor_receipts;
        }
        if let Some(read) = income.as_mut() {
            read.consulted = income_receipts;
        }
        Ok((labor, income))
    }
}

fn source_receipt(
    output: &market_squawk_data::AnalyticalMacroProviderPeriodLatestKnownOutput,
    binding: EvidenceDigest,
    source: MacroContextInternalSource,
) -> Result<Arc<MacroContextSourceReceipt>, ServiceError> {
    Ok(Arc::new(MacroContextSourceReceipt {
        source,
        source_id: output.source_id().clone(),
        manifest: output.output().manifest().clone(),
        object_graph_digest: require_sha256(output.output().object_graph_digest())?,
        query_identity: require_sha256(output.output().query_identity())?,
        result_digest: require_sha256(output.output().result_digest())?,
        selection_digest: require_sha256(output.selection_digest())?,
        native_binding_digest: Some(require_sha256(binding)?),
    }))
}
fn period(
    scheme: &str,
    year: u16,
    ordinal: u16,
    code: String,
) -> Result<ResearchPeriod, ServiceError> {
    ResearchPeriod::try_new(
        SourceIdentifier::try_from(scheme).map_err(|_| ServiceError::InvalidResult)?,
        year,
        NonZeroU16::new(ordinal).ok_or(ServiceError::InvalidResult)?,
        SourceIdentifier::try_from(code).map_err(|_| ServiceError::InvalidResult)?,
    )
    .map_err(|_| ServiceError::InvalidResult)
}
fn allowlist(series: SourceIdentifier) -> Result<AnalyticalMacroSeriesAllowlist, ServiceError> {
    AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![series])
        .map_err(map_read_error)
}
fn limits(rows: u64, deadline: std::time::Instant) -> Result<QueryLimits, ServiceError> {
    check_control(deadline, &CancellationToken::new())?;
    QueryLimits::try_new_with_inline_bytes(
        rows,
        MACRO_CONTEXT_QUERY_BYTES,
        MACRO_CONTEXT_QUERY_BYTES,
        MACRO_CONTEXT_QUERY_MEMORY_BYTES,
        4,
        2048,
        4096,
        deadline
            .saturating_duration_since(std::time::Instant::now())
            .min(Duration::from_secs(60)),
    )
    .map_err(map_query_error)
}
fn check_control(
    deadline: std::time::Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn map_worker(error: crate::ResearchServiceError) -> ServiceError {
    match error {
        crate::ResearchServiceError::Ingest(error) => map_ingest(error),
        crate::ResearchServiceError::IngestAuthorityMismatch => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}
fn map_ingest(error: market_squawk_data::IngestError) -> ServiceError {
    match error {
        market_squawk_data::IngestError::Cancelled => ServiceError::Cancelled,
        market_squawk_data::IngestError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        _ => ServiceError::InvalidResult,
    }
}
fn map_bls(error: BlsMacroApplicationError) -> ServiceError {
    match error {
        BlsMacroApplicationError::AnalyticalRead(error) => map_read_error(error),
        BlsMacroApplicationError::ResearchService(error) => map_worker(error),
        BlsMacroApplicationError::Ingest(error) => map_ingest(error),
        BlsMacroApplicationError::Extraction(
            market_squawk_sources::ExtractionSourceError::Cancelled,
        ) => ServiceError::Cancelled,
        _ => ServiceError::InvalidResult,
    }
}
fn map_bea(error: BeaMacroApplicationError) -> ServiceError {
    match error {
        BeaMacroApplicationError::AnalyticalRead(error) => map_read_error(error),
        BeaMacroApplicationError::ResearchService(error) => map_worker(error),
        _ => ServiceError::InvalidResult,
    }
}
fn key(observation: &MacroObservation) -> Result<(u16, u16, i64), ServiceError> {
    let p = observation
        .context()
        .time()
        .effective()
        .source_period_value()
        .ok_or(ServiceError::InvalidResult)?;
    let at = observation
        .context()
        .provenance()
        .availability()
        .conservative_available_at()
        .ok_or(ServiceError::InvalidResult)?;
    Ok((p.year(), p.ordinal().get(), at.unix_nanos()))
}
fn select(target: &mut Option<PeriodRead>, candidate: PeriodRead) -> Result<(), ServiceError> {
    let replace = if let Some(old) = target.as_ref() {
        let a = key(&candidate.observation)?;
        let b = key(&old.observation)?;
        if a == b && candidate.observation.value() != old.observation.value() {
            return Err(ServiceError::InvalidResult);
        }
        a > b
    } else {
        true
    };
    if replace {
        *target = Some(candidate)
    }
    Ok(())
}

pub(super) fn project_labor(
    read: Option<PeriodRead>,
    cutoffs: MacroContextCutoffs,
    target: &mut MacroContextObservationDto,
    selected: &mut MacroContextSelectedObservation,
    inputs: &mut Vec<MacroContextInputObservation>,
    receipts: &mut Vec<Arc<MacroContextSourceReceipt>>,
) -> Result<(), ServiceError> {
    let Some(read) = read else { return Ok(()) };
    let native = key(&read.observation)?;
    let replace = if let Some(old) = selected.observation.as_ref() {
        let date = effective_calendar_date(old, cutoffs)?;
        let parsed = chrono::NaiveDate::parse_from_str(&date.to_string(), "%Y-%m-%d")
            .map_err(|_| ServiceError::InvalidResult)?;
        let old_key = (
            u16::try_from(parsed.year()).map_err(|_| ServiceError::InvalidResult)?,
            u16::try_from(parsed.month()).map_err(|_| ServiceError::InvalidResult)?,
            old.context()
                .provenance()
                .availability()
                .conservative_available_at()
                .ok_or(ServiceError::InvalidResult)?
                .unix_nanos(),
        );
        if native == old_key && read.observation.value() != old.value() {
            return Err(ServiceError::InvalidResult);
        }
        native >= old_key
    } else {
        true
    };
    if replace {
        *target = UNEMPLOYMENT_INDICATOR.unavailable();
        project(
            &read,
            cutoffs,
            target,
            selected,
            MacroContextSelectionAuthority::OfficialLaborStatistics,
        )?;
    }
    inputs.push(MacroContextInputObservation {
        role: MacroContextInputRole::LaborMarket,
        observation: read.observation,
    });
    receipts.extend(read.consulted);
    Ok(())
}
pub(super) fn project_income(
    read: Option<PeriodRead>,
    cutoffs: MacroContextCutoffs,
    observations: &mut Vec<MacroContextObservationDto>,
    inputs: &mut Vec<MacroContextInputObservation>,
    receipts: &mut Vec<Arc<MacroContextSourceReceipt>>,
) -> Result<MacroContextSelectedObservation, ServiceError> {
    let mut selected = MacroContextSelectedObservation::unavailable(INCOME);
    let mut dto = INCOME.unavailable();
    if let Some(read) = read {
        project(
            &read,
            cutoffs,
            &mut dto,
            &mut selected,
            MacroContextSelectionAuthority::OfficialIncomeStatistics,
        )?;
        dto.unit = read.unit.ok_or(ServiceError::InvalidResult)?;
        inputs.push(MacroContextInputObservation {
            role: MacroContextInputRole::CaliforniaAnnualPersonalIncome,
            observation: read.observation,
        });
        receipts.extend(read.consulted);
    }
    observations.push(dto);
    Ok(selected)
}
fn project(
    read: &PeriodRead,
    cutoffs: MacroContextCutoffs,
    dto: &mut MacroContextObservationDto,
    selected: &mut MacroContextSelectedObservation,
    authority: MacroContextSelectionAuthority,
) -> Result<(), ServiceError> {
    let observation = &read.observation;
    let provenance = observation.context().provenance();
    let time = observation.context().time();
    let p = time
        .effective()
        .source_period_value()
        .ok_or(ServiceError::InvalidResult)?;
    let valid_period = match authority {
        MacroContextSelectionAuthority::OfficialLaborStatistics => {
            p.scheme().as_str() == "bls-monthly"
                && (1..=12).contains(&p.ordinal().get())
                && p.code().as_str() == format!("M{:02}", p.ordinal())
        }
        MacroContextSelectionAuthority::OfficialIncomeStatistics => {
            p.scheme().as_str() == "bea-annual"
                && p.ordinal().get() == 1
                && p.code().as_str() == format!("{:04}", p.year())
        }
        _ => false,
    };
    if !valid_period
        || provenance.source_id() != &read.receipt.source_id
        || provenance.instrument_id().is_some()
        || provenance.venue_id().is_some()
        || provenance.quality() != DataQuality::OfficialDelayed
    {
        return Err(ServiceError::InvalidResult);
    }
    let available = provenance
        .availability()
        .conservative_available_at()
        .filter(|at| *at <= cutoffs.knowledge_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    dto.recorded = match time.published() {
        Some(published) => MacroContextRecordedDateDto::Known {
            date: coordinate_calendar_date_at_knowledge(published, cutoffs.knowledge_cutoff)?
                .to_string(),
        },
        None => MacroContextRecordedDateDto::NotSupplied,
    };
    dto.superseded_after = time
        .superseded()
        .map(coordinate_calendar_date)
        .transpose()?
        .map(|date| date.to_string());
    dto.effective_period = Some(if p.scheme().as_str() == "bls-monthly" {
        format!("{:04}-{:02}", p.year(), p.ordinal())
    } else {
        p.code().as_str().to_owned()
    });
    dto.available_at = Some(timestamp_text(available)?);
    dto.revision = Some(time.revision().get());
    match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(value), None) => {
            dto.value = MacroContextValueDto::Observed {
                decimal: value.normalize().to_string(),
            };
            dto.availability = MacroContextObservationAvailability::Available;
            dto.confidence = MacroContextConfidenceDto::moderate();
        }
        (None, Some(_)) => {
            dto.value = MacroContextValueDto::Missing {
                reason: MacroContextMissingReason::NotReported,
                explanation: "No value was reported at this cutoff.",
            };
            dto.availability = MacroContextObservationAvailability::Missing;
            dto.confidence = MacroContextConfidenceDto::limited();
        }
        _ => return Err(ServiceError::InvalidResult),
    }
    selected.observation = Some(observation.clone());
    selected.authority = Some(authority);
    selected.source_receipt = Some(Arc::clone(&read.receipt));
    Ok(())
}
