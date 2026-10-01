//! Indexed logical market-event reads under the existing supervised I/O owner.

use super::*;
use crate::MarketEventCommitRef;
use crate::catalog::market_event_store::{
    load_market_event_active_rows, load_market_event_commit,
    load_market_event_commit_for_publication,
};

impl AnalyticalDataService {
    /// Tests exact publication membership at an immutable logical commit horizon.
    pub fn has_provider_market_event_publication(
        &self,
        commit: &MarketEventCommitRef,
        digest: EvidenceDigest,
        kind: ProviderMarketEventPublicationKind,
    ) -> Result<bool, IngestError> {
        Ok(self
            .manifests
            .has_market_event_publication(commit, digest, kind)?)
    }

    /// Pages publications through a fixed logical horizon in digest order.
    /// A short page proves exhaustion; continue with the last digest otherwise.
    pub fn provider_market_event_publications(
        &self,
        commit: &MarketEventCommitRef,
        after: Option<EvidenceDigest>,
        limit: usize,
    ) -> Result<Vec<ProviderMarketEventPublicationSelector>, IngestError> {
        self.manifests
            .market_event_publications(commit, after, limit)?
            .into_iter()
            .map(|(digest, kind)| {
                Ok(ProviderMarketEventPublicationSelector::new(
                    digest,
                    ProviderMarketEventPublicationKind::from_catalog(&kind)?,
                ))
            })
            .collect()
    }

    /// Reopens raw and native publication evidence at the supplied logical horizon.
    pub fn provider_market_event_publication_evidence(
        &self,
        commit: &MarketEventCommitRef,
        selector: ProviderMarketEventPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
    ) -> Result<crate::PersistedProviderPublicationEvidence, IngestError> {
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, &cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| {
                Self::market_event_publication_origin(snapshot, commit, selector)?;
                self.read_market_event_publication_evidence_snapshot(
                    selector,
                    store,
                    deadline,
                    &cancellation,
                    snapshot,
                )
            })
            .map_err(|error| match error {
                IngestError::Catalog(error) => map_market_recovery_catalog_error(error),
                error => error,
            })
    }

    fn market_event_publication_origin(
        snapshot: &crate::catalog::CatalogReadSnapshot,
        horizon: &MarketEventCommitRef,
        selector: ProviderMarketEventPublicationSelector,
    ) -> Result<MarketEventCommitRef, IngestError> {
        let retained = load_market_event_commit(
            snapshot.connection(),
            horizon.dataset_id(),
            horizon.sequence(),
        )?
        .ok_or(IngestError::ProviderCaptureRequired)?;
        if &retained != horizon {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let origin = load_market_event_commit_for_publication(
            snapshot.connection(),
            horizon.dataset_id(),
            selector.publication_digest,
        )?
        .ok_or(IngestError::ProviderCaptureRequired)?;
        if origin.sequence() > horizon.sequence()
            || origin.available_at() > horizon.available_at()
            || origin.schema() != horizon.schema()
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        Ok(origin)
    }

    fn read_market_event_publication_evidence_snapshot(
        &self,
        selector: ProviderMarketEventPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
        snapshot: &crate::catalog::CatalogReadSnapshot,
    ) -> Result<crate::PersistedProviderPublicationEvidence, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let evidence = snapshot
            .publication_evidence(selector.publication_digest)?
            .ok_or(IngestError::ProviderCaptureRequired)?;
        evidence.verify_integrity()?;
        for payload in evidence.identity_selections().flatten() {
            check_market_event_read(deadline, cancellation)?;
            let selection = serde_json::from_slice(payload)
                .map_err(|_| IngestError::ProviderCaptureRequired)?;
            snapshot
                .verify_identity_evidence(&selection)
                .map_err(map_native_identity_catalog_error)?;
        }
        Self::verify_provider_market_event_publication_raw_evidence(
            &evidence,
            selector,
            store,
            Some(&MarketEventReadControl {
                deadline,
                cancellation,
            }),
        )?;
        Ok(evidence)
    }

    fn read_market_event_publication_snapshot(
        &self,
        horizon: &MarketEventCommitRef,
        selector: ProviderMarketEventPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
        snapshot: &crate::catalog::CatalogReadSnapshot,
    ) -> Result<
        (
            Arc<crate::PersistedProviderPublicationEvidence>,
            ProviderMarketEventArrowBatch,
        ),
        IngestError,
    > {
        check_market_event_read(deadline, cancellation)?;
        let origin = Self::market_event_publication_origin(snapshot, horizon, selector)?;
        let evidence = self.read_market_event_publication_evidence_snapshot(
            selector,
            store,
            deadline,
            cancellation,
            snapshot,
        )?;
        let rows = load_market_event_active_rows(
            snapshot.connection(),
            &origin,
            self.catalog_read_limits,
            deadline,
            cancellation,
        )?;
        let batch =
            ProviderMarketEventArrowBatch::try_from_canonical_json_with_publication_evidence(
                rows,
                &evidence,
                MAX_EVENT_PUBLICATION_READ_BYTES,
            )?;
        check_market_event_read(deadline, cancellation)?;
        let lineage: Vec<u8> = snapshot.connection().query_row(
            "SELECT lineage_digest FROM market_event_commits WHERE dataset_id=?1 AND commit_sequence=?2",
            rusqlite::params![origin.dataset_id().as_str(), i64::try_from(origin.sequence()).map_err(|_| IngestError::ProviderCaptureRequired)?],
            |row| row.get(0),
        ).map_err(CatalogError::from)?;
        if batch.schema_ref() != origin.schema()
            || batch.events().len() as u64 != origin.row_count()
            || lineage != batch.lineage_digest()?.bytes()
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        snapshot.validate_event_metadata(batch.events(), &evidence)?;
        Ok((Arc::new(evidence), batch))
    }

    fn verify_provider_market_event_publication_raw_evidence(
        evidence: &crate::PersistedProviderPublicationEvidence,
        selector: ProviderMarketEventPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
        control: Option<&MarketEventReadControl<'_>>,
    ) -> Result<(), IngestError> {
        evidence.verify_integrity()?;
        if evidence.publication_digest() != selector.publication_digest
            || evidence.publication_kind() != selector.publication_kind.as_str()
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        if let Some(response) = evidence.response() {
            let verified = match control {
                Some(control) => {
                    store.open_verified_claim_with_control(response.physical_claim(), control)
                }
                None => store.open_verified_claim(response.physical_claim()),
            }
            .map_err(map_provider_recovery_store_error)?;
            if verified.receipt().claim() != response.physical_claim() {
                return Err(IngestError::ProviderCaptureRequired);
            }
        }
        if let Some(event) = evidence.event() {
            let verified = match control {
                Some(control) => {
                    store.open_verified_claim_with_control(event.physical_claim(), control)
                }
                None => store.open_verified_claim(event.physical_claim()),
            }
            .map_err(map_provider_recovery_store_error)?;
            if verified.receipt().claim() != event.physical_claim() {
                return Err(IngestError::ProviderCaptureRequired);
            }
        }
        Ok(())
    }

    /// Reopens one exact publication at a verified logical horizon, including raw/native custody.
    pub async fn read_provider_market_event_publication(
        &self,
        commit: &MarketEventCommitRef,
        selector: ProviderMarketEventPublicationSelector,
        store: Arc<market_squawk_platform::SealedResearchJournalStore>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ProviderMarketEventArrowBatch, IngestError> {
        check_market_event_read(deadline, &cancellation)?;
        let permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(IngestError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(IngestError::DeadlineExceeded),
            permit = self.objects.acquire_blocking_permit(&cancellation) => permit?,
        };
        let reader = Self {
            authority: Arc::clone(&self.authority),
            catalog_id: self.catalog_id,
            catalog_read_limits: self.catalog_read_limits,
            market_data_instrument_reader: self.market_data_instrument_reader.clone(),
            manifests: Arc::clone(&self.manifests),
            objects: Arc::clone(&self.objects),
            operation_gate: self.operation_gate.clone(),
        };
        let commit = commit.clone();
        let operation_cancellation = cancellation.child_token();
        let _cancel_on_drop = operation_cancellation.clone().drop_guard();
        let worker_cancellation = operation_cancellation.clone();
        let supervisor = BlockingIoSupervisor::new(operation_cancellation);
        let mut worker = supervisor
            .spawn_blocking(move || {
                let _permit = permit;
                let snapshot = reader
                    .manifests
                    .read_snapshot(reader.catalog_read_limits, deadline, &worker_cancellation)
                    .map_err(map_market_recovery_catalog_error)?;
                snapshot
                    .read(|snapshot| {
                        let (_, batch) = reader.read_market_event_publication_snapshot(
                            &commit,
                            selector,
                            &store,
                            deadline,
                            &worker_cancellation,
                            snapshot,
                        )?;
                        Ok(batch)
                    })
                    .map_err(|error| match error {
                        IngestError::Catalog(error) => map_market_recovery_catalog_error(error),
                        error => error,
                    })
            })
            .map_err(|error| match error {
                BlockingIoAdmissionError::Cancelled => IngestError::Cancelled,
                BlockingIoAdmissionError::Saturated => {
                    IngestError::Parquet(ParquetStoreError::BlockingTaskLimitExceeded)
                }
                BlockingIoAdmissionError::ReaperUnavailable => {
                    IngestError::Parquet(ParquetStoreError::BlockingTaskFailed)
                }
            })?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(IngestError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => Err(IngestError::DeadlineExceeded),
            result = &mut worker => {
                check_market_event_read(deadline, &cancellation)?;
                result.map_err(|_| IngestError::Parquet(ParquetStoreError::BlockingTaskFailed))?
            }
        }
    }

    /// Selects indexed candidates at both financial cutoffs and verifies selected publications.
    pub async fn read_provider_market_event_point_in_time(
        &self,
        request: &crate::ProviderMarketEventPointInTimeRequest,
        store: Arc<market_squawk_platform::SealedResearchJournalStore>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<crate::ProviderMarketEventPointInTimeSelection>, IngestError> {
        check_market_event_read(deadline, &cancellation)?;
        let permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(IngestError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => {
                return Err(IngestError::DeadlineExceeded);
            }
            permit = self.objects.acquire_blocking_permit(&cancellation) => permit?,
        };
        let reader = Self {
            authority: Arc::clone(&self.authority),
            catalog_id: self.catalog_id,
            catalog_read_limits: self.catalog_read_limits,
            market_data_instrument_reader: self.market_data_instrument_reader.clone(),
            manifests: Arc::clone(&self.manifests),
            objects: Arc::clone(&self.objects),
            operation_gate: self.operation_gate.clone(),
        };
        let request = request.clone();
        let operation_cancellation = cancellation.child_token();
        let _cancel_on_drop = operation_cancellation.clone().drop_guard();
        let worker_cancellation = operation_cancellation.clone();
        let supervisor = BlockingIoSupervisor::new(operation_cancellation);
        let mut worker = supervisor
            .spawn_blocking(move || {
                let _permit = permit;
                reader.read_provider_market_event_point_in_time_blocking(
                    &request,
                    &store,
                    deadline,
                    &worker_cancellation,
                )
            })
            .map_err(|error| match error {
                BlockingIoAdmissionError::Cancelled => IngestError::Cancelled,
                BlockingIoAdmissionError::Saturated => {
                    IngestError::Parquet(ParquetStoreError::BlockingTaskLimitExceeded)
                }
                BlockingIoAdmissionError::ReaperUnavailable => {
                    IngestError::Parquet(ParquetStoreError::BlockingTaskFailed)
                }
            })?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(IngestError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => Err(IngestError::DeadlineExceeded),
            result = &mut worker => {
                check_market_event_read(deadline, &cancellation)?;
                result.map_err(|_| IngestError::Parquet(ParquetStoreError::BlockingTaskFailed))?
            }
        }
    }

    fn read_provider_market_event_point_in_time_blocking(
        &self,
        request: &crate::ProviderMarketEventPointInTimeRequest,
        store: &market_squawk_platform::SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::ProviderMarketEventPointInTimeSelection>, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| {
                self.read_provider_market_event_point_in_time_snapshot(
                    request,
                    store,
                    deadline,
                    cancellation,
                    snapshot,
                )
            })
            .map_err(|error| match error {
                IngestError::Catalog(error) => map_market_recovery_catalog_error(error),
                error => error,
            })
    }

    fn read_provider_market_event_point_in_time_snapshot(
        &self,
        request: &crate::ProviderMarketEventPointInTimeRequest,
        store: &market_squawk_platform::SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
        snapshot: &crate::catalog::CatalogReadSnapshot,
    ) -> Result<Option<crate::ProviderMarketEventPointInTimeSelection>, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let Some(plan) = self
            .manifests
            .select_provider_market_event_candidates(request, snapshot)?
        else {
            return Ok(None);
        };
        check_market_event_read(deadline, cancellation)?;
        if plan.candidates.is_empty() {
            return crate::ProviderMarketEventPointInTimeSelection::try_from_reconstructed(
                request.clone(),
                plan,
                Vec::new(),
            )
            .map(Some)
            .map_err(Into::into);
        }
        let mut reopened: Vec<(
            ProviderMarketEventPublicationSelector,
            Arc<crate::PersistedProviderPublicationEvidence>,
            ProviderMarketEventArrowBatch,
        )> = Vec::new();
        reopened
            .try_reserve_exact(plan.candidates.len())
            .map_err(|_| crate::ProviderMarketEventSelectionError::Allocation)?;
        for planned in &plan.candidates {
            check_market_event_read(deadline, cancellation)?;
            let selector = ProviderMarketEventPublicationSelector {
                publication_digest: planned.publication.digest(),
                publication_kind: planned.publication.kind(),
            };
            if reopened
                .iter()
                .any(|(retained, _, _)| *retained == selector)
            {
                continue;
            }
            let (evidence, batch) = self.read_market_event_publication_snapshot(
                &plan.commit,
                selector,
                store,
                deadline,
                cancellation,
                snapshot,
            )?;
            reopened.push((selector, evidence, batch));
        }

        let mut reconstructed = Vec::new();
        reconstructed
            .try_reserve_exact(plan.candidates.len())
            .map_err(|_| crate::ProviderMarketEventSelectionError::Allocation)?;
        for planned in &plan.candidates {
            check_market_event_read(deadline, cancellation)?;
            let selector = ProviderMarketEventPublicationSelector {
                publication_digest: planned.publication.digest(),
                publication_kind: planned.publication.kind(),
            };
            let (_, evidence, batch) = reopened
                .iter()
                .find(|(retained, _, _)| *retained == selector)
                .ok_or(crate::ProviderMarketEventSelectionError::EvidenceMismatch)?;
            reconstructed.push(
                crate::ProviderMarketEventSelectedCandidate::try_from_reopened_publication(
                    request,
                    planned,
                    snapshot,
                    batch,
                    Arc::clone(evidence),
                )?,
            );
        }
        check_market_event_read(deadline, cancellation)?;
        crate::ProviderMarketEventPointInTimeSelection::try_from_reconstructed(
            request.clone(),
            plan,
            reconstructed,
        )
        .map(Some)
        .map_err(Into::into)
    }

    /// Reopens an exact logical horizon and requires the original complete selection receipt.
    pub async fn verify_provider_market_event_point_in_time_restart(
        &self,
        original: &crate::ProviderMarketEventPointInTimeSelection,
        store: Arc<market_squawk_platform::SealedResearchJournalStore>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<crate::ProviderMarketEventPointInTimeSelection, IngestError> {
        let request = original.exact_restart_request()?;
        let replay = self
            .read_provider_market_event_point_in_time(&request, store, deadline, cancellation)
            .await?
            .ok_or(crate::ProviderMarketEventSelectionError::RestartMismatch)?;
        original.verify_restart_replay(&replay)?;
        Ok(replay)
    }
}
