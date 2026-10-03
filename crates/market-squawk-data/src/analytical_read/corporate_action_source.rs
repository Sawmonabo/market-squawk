//! Exact reader for a source-owned corporate-action query, including true zero-event responses.

use super::AnalyticalReadCapability;
use crate::arrow_convert::ResearchLineageDigestAccumulator;
use crate::corporate_actions::source_capture::{
    retained_corporate_action_event_identities, retained_corporate_action_event_identity_dates,
    validate_corporate_action_source_capture,
};
use crate::{
    CorporateActionRecord, DatasetManifestRef, GenerationOwnedProviderCaptureEvidence,
    ResearchArrowBatch, Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, CorporateActionEventInstrumentIdentity, CorporateActionSourceObservation,
    CorporateActionSourcePayload, CorporateActionSourceScope, DigestAlgorithm, EvidenceDigest,
    ResearchObservation, Timestamp,
};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
};
use sha2::{Digest as _, Sha256};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

const MAX_ROWS: usize = 32_001;
const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;

/// Authentic known-source snapshot; no public constructor or Deserialize authority exists.
/// It proves the finite processing-date query retained by this one generation, including zero
/// returned actions. Economic-date applicability is evaluated separately from native dates.
#[derive(Clone, Debug)]
pub struct CorporateActionSourceSnapshot {
    manifest: DatasetManifestRef,
    source_capture_receipt_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    capture_page_received_at: Box<[Timestamp]>,
    event_identities: Box<[CorporateActionEventInstrumentIdentity]>,
    event_identity_dates: Box<[(CalendarDate, usize)]>,
    summary: CorporateActionSourceObservation,
    source_actions: Box<[CorporateActionSourceObservation]>,
    actions: Box<[CorporateActionRecord]>,
    receipt_digest: EvidenceDigest,
}
impl CorporateActionSourceSnapshot {
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub const fn source_capture_receipt_digest(&self) -> EvidenceDigest {
        self.source_capture_receipt_digest
    }
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    /// Every original received page clock, including pages with no returned economic rows.
    /// These values come only from the generation-owned, physically verified capture binding.
    pub fn capture_page_received_at(&self) -> &[Timestamp] {
        &self.capture_page_received_at
    }
    /// Original native economic dates and their subject/successor catalog coordinates. These
    /// inert values require the genuine source calendar session on every application reopen.
    pub fn event_identity_dates(
        &self,
    ) -> impl Iterator<Item = (CalendarDate, &CorporateActionEventInstrumentIdentity)> {
        self.event_identity_dates
            .iter()
            .map(|(date, index)| (*date, &self.event_identities[*index]))
    }
    pub(crate) fn event_identities(&self) -> &[CorporateActionEventInstrumentIdentity] {
        &self.event_identities
    }
    pub const fn summary(&self) -> &CorporateActionSourceObservation {
        &self.summary
    }
    pub fn scope(&self) -> &CorporateActionSourceScope {
        match self.summary.payload() {
            CorporateActionSourcePayload::QuerySummary { scope, .. } => scope,
            _ => unreachable!("private issuer retains summary only"),
        }
    }
    /// Every returned source action, including unresolved identity/date/currency/economics.
    pub fn source_actions(&self) -> &[CorporateActionSourceObservation] {
        &self.source_actions
    }
    /// Existing canonical economic records, with original dates and immutable lineage unchanged.
    pub fn actions(&self) -> &[CorporateActionRecord] {
        &self.actions
    }
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CorporateActionSourceReadError {
    #[error("corporate-action source snapshot does not match its exact source publication")]
    InvalidEvidence,
    #[error("corporate-action source snapshot exceeds its bounded read budget")]
    ResourceBound,
    #[error("corporate-action source snapshot is unavailable at the requested knowledge cutoff")]
    FutureEvidence,
    #[error("corporate-action source read was cancelled or reached its deadline")]
    Interrupted,
    #[error(transparent)]
    Manifest(#[from] crate::ManifestCatalogError),
    #[error(transparent)]
    Parquet(#[from] crate::ParquetStoreError),
    #[error(transparent)]
    Arrow(#[from] crate::ArrowConversionError),
}
use CorporateActionSourceReadError as Error;

impl AnalyticalReadCapability {
    /// Reopens the exact creating generation supplied by the common ingest/raw authority.
    /// No filtered event list, caller-crafted observations, or value-only coverage claims enter.
    pub async fn read_corporate_action_source_snapshot(
        &self,
        source: &GenerationOwnedProviderCaptureEvidence,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CorporateActionSourceSnapshot, Error> {
        if source.published_at() > knowledge_cutoff {
            return Err(Error::FutureEvidence);
        }
        let control = Control {
            deadline,
            cancellation: &cancellation,
        };
        control.check()?;
        let [owned_object] = source.objects() else {
            return Err(Error::InvalidEvidence);
        };
        let [owned_input] = owned_object.inputs() else {
            return Err(Error::InvalidEvidence);
        };
        let binding = owned_input.binding();
        if binding.native_lineage().implementation() != "alpaca_corporate_actions_v1"
            || binding.record_count() > MAX_ROWS
            || binding.record_count() == 0
        {
            return Err(Error::InvalidEvidence);
        }
        if binding
            .capture()
            .pages()
            .iter()
            .any(|page| page.received_at() > knowledge_cutoff)
        {
            return Err(Error::FutureEvidence);
        }
        // Rejoin the creating generation through the existing bounded catalog authority. This
        // also authenticates the source and publication clock against this reader's catalog.
        let owned = self.manifests.generation_owned_provider_captures_bounded(
            source.pinned().manifest(),
            deadline,
            &cancellation,
        )?;
        if owned.receipt_digest != source.receipt_digest()
            || owned.source_id != *source.source_id()
            || owned.published_at != source.published_at()
        {
            return Err(Error::InvalidEvidence);
        }
        let pinned = owned.pinned;
        let ordinal = owned_object.generation_object_ordinal();
        let object = pinned
            .objects()
            .get(ordinal)
            .ok_or(Error::InvalidEvidence)?;
        if &pinned != source.pinned()
            || object != owned_object.object()
            || object.object().row_count()
                != u64::try_from(binding.record_count()).map_err(|_| Error::ResourceBound)?
        {
            return Err(Error::InvalidEvidence);
        }
        let read_cancel = cancellation.child_token();
        let read = self.objects.read_pinned_object_bounded_async(
            &pinned,
            object.artifact_id(),
            ordinal,
            MAX_ROWS,
            MAX_OBJECT_BYTES,
            &read_cancel,
        );
        tokio::pin!(read);
        let timeout = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(timeout);
        let batches = tokio::select! {biased;
            _=cancellation.cancelled()=>{read_cancel.cancel();let _=read.as_mut().await;return Err(Error::Interrupted)},
            _=timeout.as_mut()=>{read_cancel.cancel();let _=read.as_mut().await;return Err(Error::Interrupted)},
            result=read.as_mut()=>result?,
        };
        let mut remaining_arrow = batches.iter().try_fold(0usize, |n, b| {
            n.checked_add(b.get_array_memory_size())
                .ok_or(Error::ResourceBound)
        })?;
        let mut retained = 0usize;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(binding.record_count())
            .map_err(|_| Error::ResourceBound)?;
        let mut lineage = ResearchLineageDigestAccumulator::new();
        for batch in batches {
            control.check()?;
            let arrow_bytes = batch.get_array_memory_size();
            let allowance = MAX_OBJECT_BYTES
                .checked_sub(remaining_arrow)
                .and_then(|n| n.checked_sub(retained))
                .ok_or(Error::ResourceBound)?;
            let decoded = ResearchArrowBatch::decode_provider_capture_record_batch_bounded(
                batch,
                allowance,
                &mut lineage,
                &control,
            )?;
            if &decoded.schema_ref != pinned.manifest().schema() {
                return Err(Error::InvalidEvidence);
            }
            retained = retained
                .checked_add(decoded.retained_bytes)
                .ok_or(Error::ResourceBound)?;
            for (observation, coordinate) in
                decoded.observations.into_iter().zip(decoded.coordinates)
            {
                let row = binding
                    .rows()
                    .get(observations.len())
                    .ok_or(Error::InvalidEvidence)?;
                if coordinate.binding_digest != binding.binding_digest()
                    || coordinate.capture_observation_digest
                        != binding.capture().observation_digest()
                    || coordinate.canonical_row_ordinal != row.canonical_row_ordinal()
                    || coordinate.canonical_row_digest != row.canonical_row_digest()
                    || coordinate.native_semantic_digest != row.native_semantic_digest()
                    || coordinate.capture_page_ordinal != row.capture_page_ordinal()
                    || coordinate.segment_ordinal != row.segment_ordinal()
                    || coordinate.physical_frame_ordinal != row.physical_frame_ordinal()
                    || coordinate.page_body_digest != row.page_body_digest()
                {
                    return Err(Error::InvalidEvidence);
                }
                let context = match &observation {
                    ResearchObservation::CorporateActionSource(v) => v.context(),
                    ResearchObservation::CorporateAction(v) => v.context(),
                    _ => return Err(Error::InvalidEvidence),
                };
                let p = context.provenance();
                if p.availability()
                    .conservative_available_at()
                    .is_none_or(|v| v > knowledge_cutoff)
                    || p.received_at() > knowledge_cutoff
                    || p.ingested_at() > knowledge_cutoff
                {
                    return Err(Error::FutureEvidence);
                }
                observations.push(observation);
            }
            remaining_arrow = remaining_arrow
                .checked_sub(arrow_bytes)
                .ok_or(Error::ResourceBound)?;
        }
        if observations.len() != binding.record_count()
            || Sha256Digest::new(lineage.finish().bytes()) != object.object().lineage_digest()
        {
            return Err(Error::InvalidEvidence);
        }
        validate_corporate_action_source_capture(&observations, Some(binding))
            .map_err(|_| Error::InvalidEvidence)?;
        let event_identities = retained_corporate_action_event_identities(
            binding
                .rows()
                .iter()
                .map(|row| row.native_semantic_payload()),
        )
        .map_err(|_| Error::InvalidEvidence)?;
        let event_identity_dates = retained_corporate_action_event_identity_dates(
            binding
                .rows()
                .iter()
                .map(|row| row.native_semantic_payload()),
            &event_identities,
        )
        .map_err(|_| Error::InvalidEvidence)?;
        let mut summary = None;
        let mut source_actions = Vec::new();
        let mut actions = Vec::new();
        source_actions
            .try_reserve_exact(observations.len())
            .map_err(|_| Error::ResourceBound)?;
        actions
            .try_reserve_exact(observations.len())
            .map_err(|_| Error::ResourceBound)?;
        for (observation, row) in observations.into_iter().zip(binding.rows()) {
            match observation {
                ResearchObservation::CorporateActionSource(v) => match v.payload() {
                    CorporateActionSourcePayload::QuerySummary { .. } => summary = Some(v),
                    CorporateActionSourcePayload::ReturnedAction { .. } => source_actions.push(v),
                    CorporateActionSourcePayload::EconomicQuerySummary { .. }
                    | CorporateActionSourcePayload::EconomicReturnedAction { .. } => {
                        return Err(Error::InvalidEvidence);
                    }
                },
                ResearchObservation::CorporateAction(v) => {
                    actions.push(CorporateActionRecord::new(
                        v,
                        pinned.manifest().clone(),
                        row.canonical_row_digest(),
                    ))
                }
                _ => return Err(Error::InvalidEvidence),
            }
        }
        let summary = summary.ok_or(Error::InvalidEvidence)?;
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/corporate-action-source-read/v1\0");
        digest.update(source.receipt_digest().bytes());
        digest.update(source.published_at().unix_nanos().to_be_bytes());
        digest.update(binding.binding_digest().bytes());
        digest.update(object.artifact_id().as_bytes());
        digest.update(
            u64::try_from(ordinal)
                .map_err(|_| Error::ResourceBound)?
                .to_be_bytes(),
        );
        digest.update(object.object().content_hash().bytes());
        digest.update(object.object().lineage_digest().bytes());
        digest.update(object.object().row_count().to_be_bytes());
        digest.update(object.object().size_bytes().to_be_bytes());
        digest.update(knowledge_cutoff.unix_nanos().to_be_bytes());
        control.check()?;
        Ok(CorporateActionSourceSnapshot {
            manifest: pinned.manifest().clone(),
            source_capture_receipt_digest: source.receipt_digest(),
            binding_digest: binding.binding_digest(),
            knowledge_cutoff,
            capture_page_received_at: binding
                .capture()
                .pages()
                .iter()
                .map(|page| page.received_at())
                .collect(),
            event_identities,
            event_identity_dates,
            summary,
            source_actions: source_actions.into_boxed_slice(),
            actions: actions.into_boxed_slice(),
            receipt_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        })
    }
}
struct Control<'a> {
    deadline: Instant,
    cancellation: &'a CancellationToken,
}
impl Control<'_> {
    fn check(&self) -> Result<(), Error> {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            Err(Error::Interrupted)
        } else {
            Ok(())
        }
    }
}
impl ResearchObjectControl for Control<'_> {
    fn checkpoint(&self, _: ResearchObjectControlPoint) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}
