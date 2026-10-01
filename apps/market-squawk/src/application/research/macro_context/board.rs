//! Bounded rolling and full-history H.15 reads below the neutral macro selector.

use super::*;
use market_squawk_adapter_federal_reserve::BoardFullHistoryOriginal;
use market_squawk_data::BoardFullHistoryMacroRead;

/// The full-history variant cannot enter neutral projection without native selected-row proof.
pub(super) enum BoardRead {
    Rolling(AnalyticalMacroLatestKnownOutput),
    FullHistory {
        output: AnalyticalMacroLatestKnownOutput,
        native: Box<BoardFullHistoryMacroRead>,
    },
}

impl BoardRead {
    pub(super) fn output(&self) -> &AnalyticalMacroLatestKnownOutput {
        match self {
            Self::Rolling(output) | Self::FullHistory { output, .. } => output,
        }
    }

    pub(super) fn source_receipt(&self) -> Result<MacroContextSourceReceipt, ServiceError> {
        let output = self.output();
        let mut receipt = MacroContextSourceReceipt::try_from_output(
            MacroContextInternalSource::InterestRates,
            output,
        )?;
        if let Self::FullHistory { native, .. } = self {
            if native.manifest() != output.output().manifest()
                || native.selection_digest() != output.selection_digest()
                || native.observations() != output.observations()
            {
                return Err(ServiceError::InvalidResult);
            }
            require_sha256(native.original_digest())?;
            // The complete logical binding commits the authenticated original raw/transport
            // identity and native/map partitions. Keep it with this exact canonical selection.
            receipt.native_binding_digest = Some(require_sha256(native.binding_digest())?);
        }
        Ok(receipt)
    }
}

impl MacroContextReadCapability {
    pub(super) async fn read_board(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<[Option<BoardRead>; 2], ServiceError> {
        let (rolling, full_history) = tokio::try_join!(
            self.read_board_rolling(cutoffs, deadline, cancellation.child_token()),
            self.read_board_full_history(cutoffs, deadline, cancellation.child_token()),
        )?;
        check_board_read(deadline, &cancellation)?;
        // Both datasets use the same economic ranking alongside Treasury. Preserve the existing
        // rolling result only on an exact rank tie; dataset scope is not a new economic rank.
        Ok([rolling.map(BoardRead::Rolling), full_history])
    }

    async fn read_board_full_history(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<BoardRead>, ServiceError> {
        let Some(research) = self.energy_store.as_ref() else {
            return Ok(None);
        };
        check_board_read(deadline, &cancellation)?;
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()
            .map_err(|_| ServiceError::Unavailable)?;
        let dataset = DatasetId::try_from(profile.analytical_dataset().as_str())
            .map_err(|_| ServiceError::Unavailable)?;
        let source =
            SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
        let data = research.analytical_service();
        let (mut origins, _has_older_origins) = research
            .run_owned_research_io(deadline, &cancellation, move |worker| {
                data.provider_logical_origin_candidates(
                    &dataset,
                    &source,
                    BoardFullHistoryOriginal::native_partition_schema_digest(),
                    cutoffs.knowledge_cutoff,
                    None,
                    1,
                    deadline,
                    &worker,
                )
            })
            .await
            .map_err(map_board_worker_error)?
            .map_err(map_board_ingest_error)?;
        check_board_read(deadline, &cancellation)?;
        let Some(origin) = origins.pop() else {
            return Ok(None);
        };
        // Discovery returns an inert cutoff-admitted original. The canonical reader must
        // authorize this exact creating manifest before its retained source objects are opened.
        let request = board_request(origin.manifest().clone(), cutoffs)?;
        let limits = macro_context_query_limits(&request, deadline)?;
        let output = self
            .reader
            .read_macro_latest_known_snapshot(request, limits, deadline, cancellation.child_token())
            .await
            .map_err(map_read_error)?;
        if output.output().manifest() != origin.manifest()
            || output.source_id().as_str() != BOARD_DDP_SOURCE_ID
        {
            return Err(ServiceError::InvalidResult);
        }
        if output.observations().is_empty() {
            return Ok(None);
        }
        let data = research.analytical_service();
        let store = research.provider_capture_store();
        let read = research
            .run_owned_research_io(deadline, &cancellation, move |worker| {
                let native = data.reopen_board_full_history_macro_selection(
                    &origin, &output, &store, deadline, &worker,
                )?;
                Ok::<_, market_squawk_data::IngestError>(BoardRead::FullHistory {
                    output,
                    native: Box::new(native),
                })
            })
            .await
            .map_err(map_board_worker_error)?
            .map_err(map_board_ingest_error)?;
        check_board_read(deadline, &cancellation)?;
        Ok(Some(read))
    }

    async fn read_board_rolling(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<AnalyticalMacroLatestKnownOutput>, ServiceError> {
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_rolling_dashboard()
            .map_err(|_| ServiceError::Unavailable)?;
        let contract = profile.contract();
        if contract.release() != BoardRelease::H15SelectedInterestRates
            || contract.family() != BoardDatasetFamily::H15TreasuryConstantMaturities
            || contract.frequency() != BoardFrequency::BusinessDaily
            || h15_treasury_constant_maturities_dashboard_series().len() != H15_INDICATOR_COUNT
        {
            return Err(ServiceError::Unavailable);
        }

        let dataset = DatasetId::try_from(profile.analytical_dataset().as_str())
            .map_err(|_| ServiceError::Unavailable)?;
        // Resolve the cutoff-admitted creating generation, whose append manifest retains earlier
        // captures, before selecting rows. A later refresh cannot change retained inputs.
        let reader = self.reader.clone();
        let select = move |worker: CancellationToken| {
            let (mut origins, _has_older_origins) = reader
                .provider_capture_origin_candidates(
                    &dataset,
                    cutoffs.knowledge_cutoff,
                    None,
                    market_squawk_data::AnalyticalReadLimit::try_new(1).map_err(map_read_error)?,
                    deadline,
                    &worker,
                )
                .map_err(map_read_error)?;
            let Some(manifest) = origins.pop() else {
                return Ok(None);
            };
            let generation = reader
                .exact(&manifest, deadline, &worker)
                .map_err(map_read_error)?;
            let source_id =
                SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
            if generation.source_id() != &source_id
                || generation.manifest().dataset_id() != &dataset
            {
                return Err(ServiceError::InvalidResult);
            }
            Ok(Some(generation.manifest().clone()))
        };
        let manifest = if let Some(research) = self.energy_store.as_ref() {
            research
                .run_owned_research_io(deadline, &cancellation, select)
                .await
                .map_err(map_board_worker_error)??
        } else {
            select(cancellation.clone())?
        };
        check_board_read(deadline, &cancellation)?;
        let Some(manifest) = manifest else {
            return Ok(None);
        };

        let request = board_request(manifest.clone(), cutoffs)?;
        let query_limits = macro_context_query_limits(&request, deadline)?;
        let output = self
            .reader
            .read_macro_latest_known_snapshot(request, query_limits, deadline, cancellation)
            .await
            .map_err(map_read_error)?;
        if output.output().manifest() != &manifest {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Some(output))
    }
}

fn board_request(
    manifest: DatasetManifestRef,
    cutoffs: MacroContextCutoffs,
) -> Result<AnalyticalMacroLatestKnownRequest, ServiceError> {
    let mut series = Vec::new();
    series
        .try_reserve_exact(H15_INDICATOR_COUNT)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for descriptor in h15_treasury_constant_maturities_dashboard_series() {
        series.push(
            descriptor
                .canonical_macro_series_identifier()
                .map_err(|_| ServiceError::Unavailable)?,
        );
    }
    AnalyticalMacroLatestKnownRequest::try_new(
        manifest,
        SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?,
        cutoffs.knowledge_cutoff,
        cutoffs.effective_date_cutoff,
        AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(series)
            .map_err(map_read_error)?,
    )
    .map_err(map_read_error)
}

fn check_board_read(
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

fn map_board_ingest_error(error: market_squawk_data::IngestError) -> ServiceError {
    use market_squawk_data::IngestError;
    match error {
        IngestError::Cancelled => ServiceError::Cancelled,
        IngestError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        IngestError::InvalidProviderMacroPlan
        | IngestError::ReplayConflict
        | IngestError::IncompleteSuccessfulRun => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}

fn map_board_worker_error(error: crate::ResearchServiceError) -> ServiceError {
    use market_squawk_platform::{ResearchObjectControlError, SealedResearchJournalStoreError};
    match error {
        crate::ResearchServiceError::Ingest(error) => map_board_ingest_error(error),
        crate::ResearchServiceError::ProviderCaptureStore(
            SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Cancelled),
        ) => ServiceError::Cancelled,
        crate::ResearchServiceError::ProviderCaptureStore(
            SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::DeadlineExceeded,
            ),
        ) => ServiceError::DeadlineExceeded,
        crate::ResearchServiceError::IngestAuthorityMismatch => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}
