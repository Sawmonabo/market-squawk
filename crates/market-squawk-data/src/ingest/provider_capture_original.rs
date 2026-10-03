//! Original response custody using the sole analytical gate, raw store and catalog.
use super::*;
use crate::ProviderCaptureOriginalReceipt;
use market_squawk_platform::{SealedResearchJournalSegment, SealedResearchJournalStore};
use market_squawk_sources::{ProviderCaptureMaterial, SealedProviderCaptureSetReceipt};

/// Exclusive original-acquisition lease; also excludes backup and raw quarantine until retention.
/// Drop before ordinary final ingest, which reacquires the same existing operation gate.
pub struct ProviderCaptureOriginalLease {
    _lease: crate::analytical_backup::AnalyticalOperationLease,
}

/// Original physical records and an exact receipt, issued only by controlled catalog/raw replay.
/// Consuming it creates new process-local sealing material from those same original envelopes.
pub struct ProviderCaptureOriginalRead {
    original: ProviderCaptureOriginalReceipt,
    segment: SealedResearchJournalSegment,
}
impl ProviderCaptureOriginalRead {
    /// Returns the actual catalog-owned original reference.
    pub const fn original(&self) -> &ProviderCaptureOriginalReceipt {
        &self.original
    }
    /// Borrows original bounded bytes while the original verified segment remains owned.
    pub fn records(&self) -> &[market_squawk_platform::RawCaptureRecord] {
        self.segment.records()
    }
    /// Re-enters the existing physical sealer using every original envelope coordinate unchanged.
    pub fn into_material(
        self,
    ) -> Result<(ProviderCaptureOriginalReceipt, ProviderCaptureMaterial), IngestError> {
        let material = ProviderCaptureMaterial::try_new(
            self.original.capture().clone(),
            self.segment.into_records().into_vec(),
        )
        .map_err(|_| IngestError::ProviderCaptureRequired)?;
        Ok((self.original, material))
    }
}
impl AnalyticalDataService {
    /// Discovers the sole pending original session for exact restart recovery. The caller must
    /// validate its persisted request/account generation before using any original response.
    pub fn pending_provider_capture_original(
        &self,
        source: &SourceId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProviderCaptureOriginalReceipt>, IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| {
                snapshot
                    .provider_capture_original_pending_session(source)?
                    .map(|session| snapshot.provider_capture_original(session, 0))
                    .transpose()
                    .map(Option::flatten)
            })
            .map_err(map_market_recovery_catalog_error)
    }

    /// Rejects replacing another unpublished source graph before its original publication finishes.
    pub fn require_provider_capture_original_session(
        &self,
        source: &SourceId,
        session: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        let pending = snapshot
            .read(|snapshot| snapshot.provider_capture_original_pending_session(source))
            .map_err(map_market_recovery_catalog_error)?;
        if pending.is_some_and(|pending| pending != session) {
            return Err(IngestError::ReplayConflict);
        }
        Ok(())
    }

    /// Acquires the existing catalog mutation gate before source dispatch/raw sealing.
    pub async fn acquire_provider_capture_original_lease(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderCaptureOriginalLease, IngestError> {
        tokio::select! { biased;
            _=cancellation.cancelled()=>Err(IngestError::Cancelled),
            _=tokio::time::sleep_until(deadline.into())=>Err(IngestError::DeadlineExceeded),
            lease=self.operation_gate.acquire(cancellation)=>Ok(ProviderCaptureOriginalLease{_lease:lease.ok_or(IngestError::Cancelled)?}),
        }
    }
    /// Reads one exact response locator. This never selects a different source graph or page.
    pub fn provider_capture_original(
        &self,
        session: EvidenceDigest,
        ordinal: u16,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProviderCaptureOriginalReceipt>, IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| snapshot.provider_capture_original(session, ordinal))
            .map_err(map_market_recovery_catalog_error)
    }
    /// Consumes a new live token into durable original custody before a provider checkpoint moves.
    /// Caller holds the original lease and runs this synchronous method on its existing I/O worker.
    #[allow(clippy::too_many_arguments)]
    pub fn retain_provider_capture_original(
        &self,
        metadata: &SourceMetadata,
        session: EvidenceDigest,
        ordinal: u16,
        expected_count: u16,
        dataset: &DatasetId,
        context: &[u8],
        decoded_at: Timestamp,
        token: ProviderWholeCaptureToken,
        rights: &RightsDecisionInput,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderCaptureOriginalReceipt, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let receipt = token.persisted_receipt();
        if receipt.capture().source_id() != metadata.source_id()
            || receipt.capture().metadata_revision() != metadata.revision()
            || rights.source_id != *metadata.source_id()
            || rights.payload_digest != receipt.capture().observation_digest()
            || !rights
                .permitted_operations
                .contains(&SourceOperation::Persist)
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let control = MarketEventReadControl {
            deadline,
            cancellation,
        };
        let segment = store
            .open_verified_claim_with_control(receipt.segment().claim(), &control)
            .map_err(map_provider_recovery_store_error)?;
        let reopened = SealedProviderCaptureSetReceipt::try_bind(
            receipt.capture().clone(),
            segment.receipt().clone(),
        )
        .map_err(|_| IngestError::ProviderCaptureRequired)?;
        if reopened != *receipt {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let authority = self.market_recovery_authority(deadline, cancellation)?;
        if authority.source(metadata.source_id())?.as_ref() != Some(metadata) {
            authority.register_source(metadata, decoded_at)?;
        }
        let grant = authority.admit_source_rights(rights.clone())?;
        authority
            .catalog()
            .retain_provider_capture_original(
                session,
                ordinal,
                expected_count,
                dataset,
                context,
                decoded_at,
                token,
                &grant,
                deadline,
                cancellation,
            )
            .map_err(map_market_recovery_catalog_error)
    }
    /// Physically verifies every original before atomically retaining the complete custody graph.
    /// The original acquisition lease must remain held across sealing and this call.
    #[allow(clippy::too_many_arguments)]
    pub fn retain_option_contract_reference_originals(
        &self,
        metadata: &SourceMetadata,
        session: EvidenceDigest,
        dataset: &DatasetId,
        context: &[u8],
        pages: Vec<(Timestamp, ProviderWholeCaptureToken, RightsDecisionInput)>,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ProviderCaptureOriginalReceipt>, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        if pages.is_empty() || pages.len() > 32 || context.is_empty() || context.len() > 128 * 1024
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let control = MarketEventReadControl {
            deadline,
            cancellation,
        };
        let mut total = 0_u64;
        for (_, token, rights) in &pages {
            check_market_event_read(deadline, cancellation)?;
            let receipt = token.persisted_receipt();
            total = total
                .checked_add(receipt.capture().total_body_bytes())
                .filter(|total| *total <= 32 * 1024 * 1024)
                .ok_or(IngestError::ReservationPayloadMismatch)?;
            if receipt.capture().source_id() != metadata.source_id()
                || receipt.capture().metadata_revision() != metadata.revision()
                || rights.source_id != *metadata.source_id()
                || rights.payload_digest != receipt.capture().observation_digest()
                || !rights
                    .permitted_operations
                    .contains(&SourceOperation::Persist)
            {
                return Err(IngestError::ReservationPayloadMismatch);
            }
            let segment = store
                .open_verified_claim_with_control(receipt.segment().claim(), &control)
                .map_err(map_provider_recovery_store_error)?;
            let reopened = SealedProviderCaptureSetReceipt::try_bind(
                receipt.capture().clone(),
                segment.receipt().clone(),
            )
            .map_err(|_| IngestError::ProviderCaptureRequired)?;
            if reopened != *receipt {
                return Err(IngestError::ProviderCaptureRequired);
            }
        }
        let authority = self.market_recovery_authority(deadline, cancellation)?;
        if authority.source(metadata.source_id())?.as_ref() != Some(metadata) {
            authority.register_source(metadata, pages[0].0)?;
        }
        let mut admitted = Vec::with_capacity(pages.len());
        for (decoded_at, token, rights) in pages {
            check_market_event_read(deadline, cancellation)?;
            let grant = authority.admit_source_rights(rights)?;
            admitted.push((decoded_at, token, grant));
        }
        authority
            .catalog()
            .retain_option_contract_reference_originals(
                session,
                dataset,
                context,
                admitted,
                deadline,
                cancellation,
            )
            .map_err(map_market_recovery_catalog_error)
    }

    /// Reopens a published option-reference original only through its exact owning option
    /// generation. A published original cannot be reused as fresh acquisition or another dataset.
    pub fn reopen_option_contract_reference_original(
        &self,
        expected: &ProviderCaptureOriginalReceipt,
        owning_option_binding: EvidenceDigest,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderCaptureOriginalRead, IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        let actual = snapshot
            .read(|snapshot| -> Result<_, IngestError> {
                let actual = snapshot
                    .provider_capture_original(expected.session(), expected.ordinal())
                    .map_err(map_market_recovery_catalog_error)?
                    .ok_or(IngestError::ProviderCaptureRequired)?;
                if actual != *expected || actual.published_binding() != Some(owning_option_binding)
                {
                    return Err(IngestError::ReplayConflict);
                }
                let binding = snapshot
                    .option_market_binding_evidence(owning_option_binding)
                    .map_err(map_market_recovery_catalog_error)?
                    .ok_or(IngestError::ProviderCaptureRequired)?;
                let dependency = binding
                    .reference_dependencies()
                    .get(usize::from(actual.ordinal()))
                    .ok_or(IngestError::ReplayConflict)?;
                if dependency.capture() != actual.capture()
                    || dependency.physical() != actual.physical()
                    || actual.decoded_at() > binding.capture().pages()[0].received_at()
                {
                    return Err(IngestError::ReplayConflict);
                }
                Ok(actual)
            })
            .map_err(|error| match error {
                IngestError::Catalog(error) => map_market_recovery_catalog_error(error),
                other => other,
            })?;
        let control = MarketEventReadControl {
            deadline,
            cancellation,
        };
        let segment = store
            .open_verified_claim_with_control(actual.physical().claim(), &control)
            .map_err(map_provider_recovery_store_error)?;
        let receipt = SealedProviderCaptureSetReceipt::try_bind(
            actual.capture().clone(),
            segment.receipt().clone(),
        )
        .map_err(|_| IngestError::ProviderCaptureRequired)?;
        if receipt.receipt_digest() != actual.physical().sealed_capture_receipt_digest() {
            return Err(IngestError::ProviderCaptureRequired);
        }
        Ok(ProviderCaptureOriginalRead {
            original: actual,
            segment,
        })
    }

    /// Reopens the same current custody record and physically verifies every original raw frame.
    /// No token is recreated from a digest, decoded JSON or an unverified physical path.
    pub fn reopen_provider_capture_original(
        &self,
        expected: &ProviderCaptureOriginalReceipt,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderCaptureOriginalRead, IngestError> {
        let actual = self
            .provider_capture_original(
                expected.session(),
                expected.ordinal(),
                deadline,
                cancellation,
            )?
            .ok_or(IngestError::ProviderCaptureRequired)?;
        if actual != *expected || actual.published_binding().is_some() {
            return Err(IngestError::ReplayConflict);
        }
        let control = MarketEventReadControl {
            deadline,
            cancellation,
        };
        let segment = store
            .open_verified_claim_with_control(actual.physical().claim(), &control)
            .map_err(map_provider_recovery_store_error)?;
        let receipt = SealedProviderCaptureSetReceipt::try_bind(
            actual.capture().clone(),
            segment.receipt().clone(),
        )
        .map_err(|_| IngestError::ProviderCaptureRequired)?;
        if receipt.receipt_digest() != actual.physical().sealed_capture_receipt_digest() {
            return Err(IngestError::ProviderCaptureRequired);
        }
        Ok(ProviderCaptureOriginalRead {
            original: actual,
            segment,
        })
    }
}
