//! Private bounded reread of the exact Tiingo economic-action creating generation.
use super::AnalyticalReadCapability;
use crate::arrow_convert::ResearchLineageDigestAccumulator;
use crate::{
    CorporateActionSourceReadError as Error, DatasetManifestRef,
    GenerationOwnedProviderCaptureEvidence, PersistedProviderCaptureBindingEvidence,
    ResearchArrowBatch, Sha256Digest,
};
use market_squawk_domain::{ResearchObservation, Timestamp};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
const MAX_ROWS: usize = 8_193;
const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;
/// Private product of original object/row/physical binding authentication, never caller values.
pub(crate) struct OriginalCurrentOrdinaryCapture {
    pub(crate) manifest: DatasetManifestRef,
    pub(crate) binding: PersistedProviderCaptureBindingEvidence,
    pub(crate) observations: Vec<ResearchObservation>,
}
impl AnalyticalReadCapability {
    pub(crate) async fn read_original_current_ordinary_capture(
        &self,
        source: &GenerationOwnedProviderCaptureEvidence,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<OriginalCurrentOrdinaryCapture, Error> {
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
        if binding.native_lineage().implementation() != "tiingo_corporate_actions_v1"
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
        crate::corporate_actions::source_capture::validate_corporate_action_source_capture(
            &observations,
            Some(binding),
        )
        .map_err(|_| Error::InvalidEvidence)?;
        control.check()?;
        Ok(OriginalCurrentOrdinaryCapture {
            manifest: pinned.manifest().clone(),
            binding: binding.clone(),
            observations,
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
