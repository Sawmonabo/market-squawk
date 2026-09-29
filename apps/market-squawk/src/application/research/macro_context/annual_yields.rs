//! Source-owned, manifest-pinned annual government-yield sample for the premium model.

use super::super::equity_premium::AnnualEquityCashReturnRead;
use super::*;
use market_squawk_adapter_federal_reserve::BoardFullHistoryOriginal;
use market_squawk_data::ProviderLogicalPublicationOrigin;
use market_squawk_valuation::EQUITY_PREMIUM_SAMPLE_YEARS;
use market_squawk_domain::ResearchObservation;
use rust_decimal::Decimal;

const ENDPOINTS: usize = EQUITY_PREMIUM_SAMPLE_YEARS + 1;

/// Inert replay locator; only a fresh pinned read can turn it back into serving evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AnnualGovernmentYieldReference {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    original_digest: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    economic_origin: Timestamp,
    equity_sample_evidence: EvidenceDigest,
    equity_closing_dates: [CalendarDate; ENDPOINTS],
    selection_digests: [EvidenceDigest; ENDPOINTS],
    evidence_digest: EvidenceDigest,
}

impl AnnualGovernmentYieldReference {
    /// Reconstructs only inert retained coordinates; `read_annual_government_yield_reference`
    /// must authenticate the original generation and reproduce every selection before use.
    pub(crate) fn from_retained(
        manifest: DatasetManifestRef,
        binding_digest: EvidenceDigest,
        original_digest: EvidenceDigest,
        equity: &AnnualEquityCashReturnRead,
        economic_origin: Timestamp,
        selection_digests: [EvidenceDigest; ENDPOINTS],
        evidence_digest: EvidenceDigest,
    ) -> Result<Self, ServiceError> {
        if economic_origin != equity.economic_origin() {
            return Err(ServiceError::InvalidResult);
        }
        let knowledge_cutoff = equity.source().knowledge_cutoff();
        let equity_closing_dates = *equity.closing_dates();
        let equity_sample_evidence = require_sha256(equity.reference().evidence_digest())?;
        require_sha256(binding_digest)?;
        require_sha256(original_digest)?;
        for digest in selection_digests
            .iter()
            .chain(std::iter::once(&evidence_digest))
        {
            require_sha256(*digest)?;
        }
        Ok(Self {
            manifest,
            binding_digest,
            original_digest,
            knowledge_cutoff,
            economic_origin,
            equity_sample_evidence,
            equity_closing_dates,
            selection_digests,
            evidence_digest,
        })
    }
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub(crate) const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub(crate) const fn economic_origin(&self) -> Timestamp {
        self.economic_origin
    }
    pub(crate) const fn equity_sample_evidence(&self) -> EvidenceDigest {
        self.equity_sample_evidence
    }
    pub(crate) const fn equity_closing_dates(&self) -> &[CalendarDate; ENDPOINTS] {
        &self.equity_closing_dates
    }
    pub(crate) const fn selection_digests(&self) -> &[EvidenceDigest; ENDPOINTS] {
        &self.selection_digests
    }
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Eleven exact source selections, including the opening endpoint before the ten-year sample.
#[derive(Debug)]
pub(crate) struct AnnualGovernmentYieldRead {
    reference: AnnualGovernmentYieldReference,
    observations: [MacroObservation; ENDPOINTS],
    yields_percent: [Decimal; ENDPOINTS],
    max_source_available_at: Timestamp,
    validated_at: Timestamp,
}

impl AnnualGovernmentYieldRead {
    pub(crate) const fn reference(&self) -> &AnnualGovernmentYieldReference {
        &self.reference
    }
    pub(crate) const fn observations(&self) -> &[MacroObservation; ENDPOINTS] {
        &self.observations
    }
    pub(crate) const fn yields_percent(&self) -> &[Decimal; ENDPOINTS] {
        &self.yields_percent
    }
    pub(crate) const fn max_source_available_at(&self) -> Timestamp {
        self.max_source_available_at
    }
    pub(crate) const fn validated_at(&self) -> Timestamp {
        self.validated_at
    }
}

impl MacroContextReadCapability {
    /// Resolves only the source graph's actual original calendar publication at the same cutoff.
    /// This adds its separate real parent to final macro/premium graph authorization; a caller
    /// cannot add an unrelated calendar manifest or substitute a newer calendar generation.
    pub(crate) async fn read_equity_history_calendar_parent(
        &self,
        research: &crate::ResearchService,
        source: &super::super::TiingoCompletedEodActionRead,
        deadline: std::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetManifestRef, super::super::EquityPremiumReadError> {
        let graph = source
            .history()
            .selection()
            .receipt()
            .date_windows()
            .ok_or(ServiceError::InvalidResult)?;
        let calendar = graph.calendar();
        if calendar.calendar_available_at > source.knowledge_cutoff() {
            return Err(ServiceError::InvalidResult.into());
        }
        let reader = self.reader.clone();
        let binding = calendar.capture_binding_digest;
        let content = market_squawk_data::Sha256Digest::new(calendar.origin_content_digest.bytes());
        let cutoff = source.knowledge_cutoff();
        let result = research
            .run_owned_research_io(deadline, cancellation, move |worker_cancellation| {
                reader.provider_capture_origin(
                    binding,
                    content,
                    cutoff,
                    deadline,
                    &worker_cancellation,
                )
            })
            .await?;
        super::super::equity_premium::selection::check_selection_control(deadline, cancellation)?;
        result.map_err(map_read_error)?.ok_or(
            super::super::EquityPremiumUnavailable::OriginalCalendarPublicationMissing.into(),
        )
    }

    /// Uses the existing H.15 canonical generation and fixed series policy; no provider request
    /// or substitute maturity is introduced. Equity closing dates must come from the sealed
    /// complete history/action owner. Every yield must have that exact native effective date.
    pub(crate) async fn read_annual_government_yields(
        &self,
        research: &crate::ResearchService,
        equity: &AnnualEquityCashReturnRead,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<AnnualGovernmentYieldRead, ServiceError> {
        let knowledge_cutoff = equity.source().knowledge_cutoff();
        let profile = full_history_profile().map_err(|_| ServiceError::Unavailable)?;
        let dataset = DatasetId::try_from(profile.analytical_dataset().as_str())
            .map_err(|_| ServiceError::Unavailable)?;
        let data = research.analytical_service();
        let source_id =
            SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::InvalidResult)?;
        let selected = research
            .run_owned_research_io(deadline, &cancellation, move |worker| {
                data.provider_logical_origin_candidates(
                    &dataset,
                    &source_id,
                    BoardFullHistoryOriginal::native_partition_schema_digest(),
                    knowledge_cutoff,
                    None,
                    1,
                    deadline,
                    &worker,
                )
            })
            .await
            .map_err(map_annual_yield_worker_error)?;
        super::super::equity_premium::selection::check_selection_control(deadline, &cancellation)?;
        let (mut generations, _) = selected.map_err(map_annual_yield_ingest_error)?;
        let origin = generations.pop().ok_or(ServiceError::Unavailable)?;
        self.read_pinned_annual_government_yields(research, origin, equity, deadline, cancellation)
            .await
    }

    /// Reopens the original generation and original knowledge/date selectors. A newer source
    /// generation never repairs a stale locator or silently revises an existing estimate.
    pub(crate) async fn read_annual_government_yield_reference(
        &self,
        research: &crate::ResearchService,
        reference: &AnnualGovernmentYieldReference,
        equity: &AnnualEquityCashReturnRead,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<AnnualGovernmentYieldRead, ServiceError> {
        if reference.knowledge_cutoff != equity.source().knowledge_cutoff()
            || reference.economic_origin != equity.economic_origin()
            || reference.equity_closing_dates != *equity.closing_dates()
            || reference.equity_sample_evidence != equity.reference().evidence_digest()
        {
            return Err(ServiceError::InvalidResult);
        }
        let data = research.analytical_service();
        let dataset = reference.manifest.dataset_id().clone();
        let source_id =
            SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::InvalidResult)?;
        let binding = reference.binding_digest;
        let content = reference.manifest.content_hash();
        let cutoff = reference.knowledge_cutoff;
        let selected = research
            .run_owned_research_io(deadline, &cancellation, move |worker| {
                data.provider_logical_origin(
                    &dataset,
                    &source_id,
                    BoardFullHistoryOriginal::native_partition_schema_digest(),
                    binding,
                    content,
                    cutoff,
                    deadline,
                    &worker,
                )
            })
            .await
            .map_err(map_annual_yield_worker_error)?;
        super::super::equity_premium::selection::check_selection_control(deadline, &cancellation)?;
        let origin = selected
            .map_err(map_annual_yield_ingest_error)?
            .ok_or(ServiceError::Unavailable)?;
        if origin.manifest() != &reference.manifest {
            return Err(ServiceError::InvalidResult);
        }
        let read = self
            .read_pinned_annual_government_yields(research, origin, equity, deadline, cancellation)
            .await?;
        if read.reference != *reference {
            return Err(ServiceError::InvalidResult);
        }
        Ok(read)
    }

    async fn read_pinned_annual_government_yields(
        &self,
        research: &crate::ResearchService,
        origin: ProviderLogicalPublicationOrigin,
        equity: &AnnualEquityCashReturnRead,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<AnnualGovernmentYieldRead, ServiceError> {
        let knowledge_cutoff = equity.source().knowledge_cutoff();
        let economic_origin = equity.economic_origin();
        let equity_closing_dates = *equity.closing_dates();
        let equity_sample_evidence = require_sha256(equity.reference().evidence_digest())?;
        let manifest = origin.manifest().clone();
        let binding_digest = origin.publication().binding_digest();
        let original_digest = origin
            .publication()
            .terminal()
            .provider_terminal_evidence_digest();
        let evaluated_at = current_timestamp()?;
        let origin_date = timestamp_calendar_date(economic_origin)?;
        let opening_year = origin_date
            .year()
            .checked_sub(ENDPOINTS as u16)
            .ok_or(ServiceError::InvalidRequest)?;
        if economic_origin > knowledge_cutoff
            || knowledge_cutoff > evaluated_at
            || equity_closing_dates
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || equity_closing_dates.iter().enumerate().any(|(i, date)| {
                date.year() != opening_year + i as u16
                    || date.month() != 12
                    || date.day() < 24
                    || *date >= origin_date
            })
        {
            return Err(ServiceError::InvalidRequest);
        }
        let profile = full_history_profile().map_err(|_| ServiceError::Unavailable)?;
        if manifest.dataset_id().as_str() != profile.analytical_dataset().as_str() {
            return Err(ServiceError::InvalidResult);
        }
        let descriptor = h15_treasury_constant_maturities_dashboard_series()
            .iter()
            .find(|series| series.slot() == "10y")
            .ok_or(ServiceError::Unavailable)?;
        let series = descriptor
            .canonical_macro_series_identifier()
            .map_err(|_| ServiceError::Unavailable)?;
        let expected_unit = h15_treasury_constant_maturities_canonical_unit_identifier()
            .map_err(|_| ServiceError::Unavailable)?;
        let source_id =
            SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
        let mut observations = Vec::with_capacity(ENDPOINTS);
        let mut yields = Vec::with_capacity(ENDPOINTS);
        let mut selections = Vec::with_capacity(ENDPOINTS);
        let mut max_available = Timestamp::from_unix_nanos(i64::MIN);
        let mut digest = Sha256::new();
        hash_text(
            &mut digest,
            "market-squawk/annual-government-yield-sample/h15-economic-origin-exact-date/v2",
        );
        hash_text(&mut digest, manifest.dataset_id().as_str());
        digest.update(manifest.manifest_version().to_be_bytes());
        digest.update(manifest.schema().fingerprint());
        digest.update(manifest.content_hash().bytes());
        digest.update(binding_digest.bytes());
        digest.update(original_digest.bytes());
        digest.update(knowledge_cutoff.unix_nanos().to_be_bytes());
        digest.update(economic_origin.unix_nanos().to_be_bytes());
        digest.update(equity_sample_evidence.bytes());
        for date in equity_closing_dates {
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            let request = AnalyticalMacroLatestKnownRequest::try_new(
                manifest.clone(),
                source_id.clone(),
                knowledge_cutoff,
                date,
                AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![
                    series.clone(),
                ])
                .map_err(map_read_error)?,
            )
            .map_err(map_read_error)?;
            let limits = macro_context_query_limits(&request, deadline)?;
            let output = self
                .reader
                .read_macro_latest_known_snapshot(
                    request,
                    limits,
                    deadline,
                    cancellation.child_token(),
                )
                .await
                .map_err(map_read_error)?;
            if output.output().manifest() != &manifest || output.source_id() != &source_id {
                return Err(ServiceError::InvalidResult);
            }
            let [observation] = output.observations() else {
                return Err(ServiceError::Unavailable);
            };
            validate_canonical_input(
                observation,
                &source_id,
                MacroContextCutoffs {
                    knowledge_cutoff,
                    effective_date_cutoff: date,
                    evaluated_at,
                },
            )?;
            // This source preserves a native calendar date. Do not invent an intraday fixing or
            // borrow a preceding business day to fill a missing matched annual endpoint.
            if observation
                .context()
                .time()
                .effective()
                .calendar_date_value()
                != Some(date)
                || observation.series() != &series
                || observation.unit() != &expected_unit
            {
                return Err(ServiceError::Unavailable);
            }
            let value = observation
                .value()
                .observed_value()
                .filter(|value| *value >= Decimal::ZERO)
                .ok_or(ServiceError::Unavailable)?;
            let available = observation
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .filter(|available| *available <= knowledge_cutoff)
                .ok_or(ServiceError::InvalidResult)?;
            max_available = max_available.max(available);
            hash_text(&mut digest, &date.to_string());
            digest.update(output.selection_digest().bytes());
            hash_bytes(
                &mut digest,
                &serde_json::to_vec(observation).map_err(|_| ServiceError::InvalidResult)?,
            );
            selections.push(output.selection_digest());
            yields.push(value.normalize());
            observations.push(observation.clone());
        }
        // The canonical reader above has authorized the exact original manifest before any raw
        // source file is reopened. Verify the selected complete native/map partitions and all eleven rows.
        let data = research.analytical_service();
        let store = research.provider_capture_store();
        let reopened = research
            .run_owned_research_io(deadline, &cancellation, move |worker| {
                data.reopen_board_full_history_origin(
                    &origin,
                    equity_closing_dates,
                    &store,
                    deadline,
                    &worker,
                )
            })
            .await
            .map_err(map_annual_yield_worker_error)?;
        super::super::equity_premium::selection::check_selection_control(deadline, &cancellation)?;
        let reopened = reopened.map_err(map_annual_yield_ingest_error)?;
        if reopened.manifest() != &manifest
            || reopened.binding_digest() != binding_digest
            || reopened.original_digest() != original_digest
        {
            return Err(ServiceError::InvalidResult);
        }
        for (original, canonical) in reopened.observations().iter().zip(&observations) {
            let expected = ResearchObservation::Macro(original.clone())
                .with_revision(canonical.context().time().revision())
                .map_err(|_| ServiceError::InvalidResult)?;
            if expected != ResearchObservation::Macro(canonical.clone()) {
                return Err(ServiceError::InvalidResult);
            }
        }
        let evidence_digest = require_sha256(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))?;
        Ok(AnnualGovernmentYieldRead {
            reference: AnnualGovernmentYieldReference {
                manifest,
                binding_digest,
                original_digest,
                knowledge_cutoff,
                economic_origin,
                equity_sample_evidence,
                equity_closing_dates,
                selection_digests: selections
                    .try_into()
                    .map_err(|_| ServiceError::InvalidResult)?,
                evidence_digest,
            },
            observations: observations
                .try_into()
                .map_err(|_| ServiceError::InvalidResult)?,
            yields_percent: yields.try_into().map_err(|_| ServiceError::InvalidResult)?,
            max_source_available_at: max_available,
            validated_at: current_timestamp()?,
        })
    }
}

// One code-owned complete-file identity. The dashboard is never substituted for eleven endpoints.
fn full_history_profile()
-> Result<BoardDatasetProfile, market_squawk_adapter_federal_reserve::BoardSourceError> {
    BoardDatasetProfile::h15_treasury_constant_maturities_full_history()
}

fn map_annual_yield_worker_error(error: crate::ResearchServiceError) -> ServiceError {
    super::super::EquityPremiumReadError::History(error).into_service_error()
}
fn map_annual_yield_ingest_error(error: market_squawk_data::IngestError) -> ServiceError {
    map_annual_yield_worker_error(crate::ResearchServiceError::Ingest(error))
}
