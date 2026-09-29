//! Original metadata/native replay and physical resealing; no provider reads occur here.
use super::*;
use crate::provider_activation::tiingo::{
    TiingoHistoryMetadataInput, TiingoHistoryOriginalContext,
};
use market_squawk_adapter_tiingo::{TiingoDecoder, TiingoEodReceipt, TiingoRequestSpec};
use market_squawk_data::ProviderCaptureOriginalReceipt;
use market_squawk_sources::{ProviderCaptureMaterial, ProviderWholeCaptureToken};
use std::io::Write as _;

impl ProductionResearchIngestCoordinator {
    pub(crate) async fn recover_tiingo_history_metadata(
        &self,
        session: EvidenceDigest,
        source: &market_squawk_domain::SourceId,
        plan: &TiingoHistoryPlan,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Option<(TiingoHistoryMetadataInput, TiingoHistoryOriginalContext)>,
        TiingoHistoryApplicationError,
    > {
        let lease = Arc::new(
            self.research
                .analytical()
                .acquire_provider_capture_original_lease(deadline, cancellation)
                .await?,
        );
        let data = self.research.analytical_service();
        let source = source.clone();
        self.research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                data.require_provider_capture_original_session(&source, session, deadline, &worker)
            })
            .await??;
        let Some(original) = self
            .tiingo_original(session, 0, deadline, cancellation)
            .await?
        else {
            return Ok(None);
        };
        if original.published_binding().is_some() {
            return Err(TiingoHistoryApplicationError::OriginalContinuationRequired);
        }
        let context: TiingoHistoryOriginalContext = serde_json::from_slice(original.context())
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        if context.session != session
            || usize::from(original.expected_count()) != plan.pages().len() + 1
        {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let request = TiingoRequestSpec::metadata(plan.pages()[0].ticker().clone())
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let data = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        let saved = original.clone();
        let native = context.native_contract_revision.clone();
        let entitlement = context.entitlement_generation.clone();
        let decoded = self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                let _lease = lease;
                let read =
                    data.reopen_provider_capture_original(&saved, &store, deadline, &worker)?;
                let page = saved
                    .capture()
                    .pages()
                    .first()
                    .ok_or(TiingoHistoryApplicationError::Admission)?;
                if saved.capture().request_set_identity() != request.request_identity() {
                    return Err(TiingoHistoryApplicationError::Admission);
                }
                let record = read
                    .records()
                    .first()
                    .ok_or(TiingoHistoryApplicationError::Admission)?;
                TiingoDecoder::new(native, entitlement)
                    .decode_metadata(
                        request,
                        page.http_status(),
                        record.payload(),
                        page.received_at(),
                        saved.decoded_at(),
                    )
                    .map_err(|_| TiingoHistoryApplicationError::Admission)
            })
            .await??;
        Ok(Some((
            TiingoHistoryMetadataInput::Original { original, decoded },
            context,
        )))
    }
    pub(super) async fn tiingo_original(
        &self,
        session: EvidenceDigest,
        ordinal: u16,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProviderCaptureOriginalReceipt>, TiingoHistoryApplicationError> {
        let data = self.research.analytical_service();
        Ok(self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                data.provider_capture_original(session, ordinal, deadline, &worker)
            })
            .await??)
    }
    /// Once an HTTP response completed, finish this bounded raw seal/custody unit on the existing
    /// worker even when the caller cancels. Network and later checkpoint advancement still cancel.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn retain_tiingo_original(
        &self,
        material: ProviderCaptureMaterial,
        context: &TiingoHistoryOriginalContext,
        ordinal: u16,
        expected_count: u16,
        dataset: &DatasetId,
        decoded_at: Timestamp,
        publication: &ResearchProviderPublicationOperation,
        lease: Arc<market_squawk_data::ProviderCaptureOriginalLease>,
        deadline: Instant,
    ) -> Result<ProviderCaptureOriginalReceipt, TiingoHistoryApplicationError> {
        publication
            .precommit_authority()
            .validate_precommit()
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let rights = publication
            .rights()
            .decision(material.receipt().observation_digest(), decoded_at)
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let metadata = publication.source().clone();
        let session = context.session;
        let dataset = dataset.clone();
        let bytes = if ordinal == 0 {
            serde_json::to_vec(context).map_err(|_| TiingoHistoryApplicationError::Admission)?
        } else {
            Vec::new()
        };
        let data = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        let finish = CancellationToken::new();
        self.research
            .run_owned_research_io(deadline, &finish, move |worker| {
                let _lease = lease;
                let (expectation, request) = material.into_whole_seal_parts();
                let token = expectation
                    .try_rejoin(
                        request
                            .seal(&store)
                            .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                    )?
                    .try_into_whole()?;
                Ok::<_, TiingoHistoryApplicationError>(data.retain_provider_capture_original(
                    &metadata,
                    session,
                    ordinal,
                    expected_count,
                    &dataset,
                    &bytes,
                    decoded_at,
                    token,
                    &rights,
                    &store,
                    deadline,
                    &worker,
                )?)
            })
            .await?
    }
    /// The original token was consumed into custody. A new private witness is issued only after
    /// original raw bytes are physically verified and the SAME sealer reproduces the exact receipt.
    pub(super) async fn reseal_tiingo_original(
        &self,
        original: ProviderCaptureOriginalReceipt,
        lease: Arc<market_squawk_data::ProviderCaptureOriginalLease>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            ProviderWholeCaptureToken,
            market_squawk_platform::VerifiedResearchObject,
        ),
        TiingoHistoryApplicationError,
    > {
        let data = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        self.research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                let _lease = lease;
                let read =
                    data.reopen_provider_capture_original(&original, &store, deadline, &worker)?;
                let body = seal_original_body(&read, &store, deadline, &worker)?;
                let (saved, material) = read.into_material()?;
                let (expectation, request) = material.into_whole_seal_parts();
                let token = expectation
                    .try_rejoin(
                        request
                            .seal(&store)
                            .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                    )?
                    .try_into_whole()?;
                if token.persisted_receipt().receipt_digest()
                    != saved.physical().sealed_capture_receipt_digest()
                    || token.persisted_receipt().segment().claim() != saved.physical().claim()
                    || token.persisted_receipt().capture() != saved.capture()
                {
                    return Err(TiingoHistoryApplicationError::Admission);
                }
                Ok((token, body))
            })
            .await?
    }
    /// Decodes and reseals one original page in the same supervised physical read. Only the
    /// bounded page and its live token leave the worker; durable custody remains the replay source.
    pub(super) async fn reopen_tiingo_original_page(
        &self,
        original: ProviderCaptureOriginalReceipt,
        request: TiingoRequestSpec,
        context: &TiingoHistoryOriginalContext,
        lease: Arc<market_squawk_data::ProviderCaptureOriginalLease>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            TiingoEodReceipt,
            ProviderWholeCaptureToken,
            market_squawk_platform::VerifiedResearchObject,
        ),
        TiingoHistoryApplicationError,
    > {
        let data = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        let native = context.native_contract_revision.clone();
        let entitlement = context.entitlement_generation.clone();
        self.research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                let _lease = lease;
                let read =
                    data.reopen_provider_capture_original(&original, &store, deadline, &worker)?;
                let page = original
                    .capture()
                    .pages()
                    .first()
                    .ok_or(TiingoHistoryApplicationError::Admission)?;
                if original.capture().request_set_identity() != request.request_identity() {
                    return Err(TiingoHistoryApplicationError::Admission);
                }
                let record = read
                    .records()
                    .first()
                    .ok_or(TiingoHistoryApplicationError::Admission)?;
                let decoded = TiingoDecoder::new(native, entitlement)
                    .decode_eod(
                        request,
                        page.http_status(),
                        record.payload(),
                        page.received_at(),
                        original.decoded_at(),
                    )
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                let body = seal_original_body(&read, &store, deadline, &worker)?;
                let (saved, material) = read.into_material()?;
                let (expectation, request) = material.into_whole_seal_parts();
                let token = expectation
                    .try_rejoin(
                        request
                            .seal(&store)
                            .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                    )?
                    .try_into_whole()?;
                if token.persisted_receipt().receipt_digest()
                    != saved.physical().sealed_capture_receipt_digest()
                    || token.persisted_receipt().segment().claim() != saved.physical().claim()
                    || token.persisted_receipt().capture() != saved.capture()
                {
                    return Err(TiingoHistoryApplicationError::Admission);
                }
                Ok((decoded, token, body))
            })
            .await?
    }
}

/// Makes one immutable logical payload from the same verified original frame used by decoding.
fn seal_original_body(
    read: &market_squawk_data::ProviderCaptureOriginalRead,
    store: &market_squawk_platform::SealedResearchJournalStore,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<market_squawk_platform::VerifiedResearchObject, TiingoHistoryApplicationError> {
    let [record] = read.records() else {
        return Err(TiingoHistoryApplicationError::Admission);
    };
    let admission = market_squawk_platform::ResearchObjectAdmission::try_new(
        (record.payload().len() as u64).max(1),
        4095,
    )
    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
    let control = HistoryStreamControl {
        deadline,
        cancellation: cancellation.clone(),
    };
    let mut pending = store
        .begin_logical_object(admission)
        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
    let written = (|| {
        for bytes in record.payload().chunks(64 * 1024) {
            market_squawk_platform::ResearchObjectControl::checkpoint(
                &control,
                market_squawk_platform::ResearchObjectControlPoint::BeforeVerification,
            )
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
            pending
                .write_all(bytes)
                .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        }
        Ok::<_, TiingoHistoryApplicationError>(())
    })();
    if let Err(error) = written {
        let _ = store.abort_logical_object(pending);
        return Err(error);
    }
    store
        .finish_logical_object(pending, &control)
        .map_err(|_| TiingoHistoryApplicationError::Admission)
}
