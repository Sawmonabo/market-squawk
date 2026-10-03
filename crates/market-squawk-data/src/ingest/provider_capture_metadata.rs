//! Physical verification of original macro metadata on the caller's existing I/O owner.
use super::*;
use crate::catalog::ProviderMetadataCaptureEvidence;
use market_squawk_platform::SealedResearchJournalStore;
use market_squawk_sources::SealedProviderCaptureSetReceipt;

/// Original metadata verified against the sole immutable raw store before publication.
/// This value is neither cloneable nor serializable and cannot be constructed from a digest.
#[derive(Debug)]
pub struct ProviderMacroMetadataCapture {
    pub(super) evidence: ProviderMetadataCaptureEvidence,
    pub(super) catalog_id: uuid::Uuid,
}

impl AnalyticalDataService {
    /// Synchronous owned-worker operation: validates the full original logical/physical receipt.
    pub fn verify_provider_macro_metadata_capture(
        &self,
        receipt: SealedProviderCaptureSetReceipt,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderMacroMetadataCapture, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let evidence = ProviderMetadataCaptureEvidence::from_receipt(&receipt)?;
        verify_metadata(
            &evidence,
            store,
            Some(&MarketEventReadControl {
                deadline,
                cancellation,
            }),
        )?;
        check_market_event_read(deadline, cancellation)?;
        Ok(ProviderMacroMetadataCapture {
            evidence,
            catalog_id: self.catalog_id,
        })
    }
}

pub(super) fn verify_metadata(
    metadata: &ProviderMetadataCaptureEvidence,
    store: &SealedResearchJournalStore,
    control: Option<&MarketEventReadControl<'_>>,
) -> Result<(), IngestError> {
    metadata.validate()?;
    if let Some(control) = control {
        check_market_event_read(control.deadline, control.cancellation)?;
    }
    let segment = match control {
        Some(control) => store.open_verified_claim_with_control(metadata.physical.claim(), control),
        None => store.open_verified_claim(metadata.physical.claim()),
    }
    .map_err(map_provider_recovery_store_error)?;
    let physical_receipt = segment.receipt().clone();
    // Reuse original envelope validation, including source and event/connection identity.
    // Move the verified records; do not copy bodies, seal again or mint a live publication token.
    drop(
        market_squawk_sources::ProviderCaptureMaterial::try_new(
            metadata.capture.clone(),
            segment.into_records().into_vec(),
        )
        .map_err(|_| IngestError::ProviderCaptureRequired)?,
    );
    let reopened =
        SealedProviderCaptureSetReceipt::try_bind(metadata.capture.clone(), physical_receipt)
            .map_err(|_| IngestError::ProviderCaptureRequired)?;
    if ProviderMetadataCaptureEvidence::from_receipt(&reopened)? != *metadata {
        return Err(IngestError::ProviderCaptureRequired);
    }
    if let Some(control) = control {
        check_market_event_read(control.deadline, control.cancellation)?;
    }
    Ok(())
}
