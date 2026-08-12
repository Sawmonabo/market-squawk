//! Fail-closed installed forecast-evidence boundary.
//!
//! Admitted model metadata retains its exact `TrainingV1` dataset provenance. There is currently
//! no sealed receipt that pairs that training identity with a separately admitted `AnalysisV1`
//! dataset, nor a reviewed terminal-price producer for the installed return model. Consequently
//! this reader advertises no materializable forecast dataset and never substitutes training
//! provenance for analysis evidence.

use std::{fmt, time::Instant};

use async_trait::async_trait;
use market_squawk_data::{AnalyticalReadCapability, Sha256Digest};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::model::forecast_preparation::{
    ForecastEvidenceCatalogRequest, ForecastEvidenceCatalogSnapshot,
    ForecastEvidenceMaterializationRequest, ForecastEvidenceReadError, ForecastEvidenceReader,
    ForecastEvidenceRevalidation, PreparedForecastEvidence,
};

/// Installed analytical boundary for model-owned forecast preparation.
#[derive(Clone)]
pub(crate) struct AnalyticalForecastEvidenceReader {
    analytical: AnalyticalReadCapability,
}

impl AnalyticalForecastEvidenceReader {
    pub(crate) const fn new(analytical: AnalyticalReadCapability) -> Self {
        Self { analytical }
    }
}

impl fmt::Debug for AnalyticalForecastEvidenceReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalForecastEvidenceReader")
            .field("analytical", &self.analytical)
            .field("analysis_pairing", &"[UNAVAILABLE]")
            .finish()
    }
}

#[async_trait]
impl ForecastEvidenceReader for AnalyticalForecastEvidenceReader {
    async fn catalog(
        &self,
        request: ForecastEvidenceCatalogRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ForecastEvidenceCatalogSnapshot, ForecastEvidenceReadError> {
        check_control(deadline, &cancellation)?;
        let mut authority = Sha256::new();
        authority.update(b"market-squawk/forecast-analysis-pairing-unavailable/v1\0");
        authority.update(request.runtime_generation_sha256().bytes());
        for model in request.models() {
            check_control(deadline, &cancellation)?;
            if model.runtime_generation_sha256() != request.runtime_generation_sha256() {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            authority.update(model.metadata().metadata_hash().bytes());
        }
        ForecastEvidenceCatalogSnapshot::try_new(
            Sha256Digest::new(authority.finalize().into()),
            Vec::new(),
        )
    }

    async fn prepare(
        &self,
        _request: ForecastEvidenceMaterializationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedForecastEvidence, ForecastEvidenceReadError> {
        check_control(deadline, &cancellation)?;
        Err(ForecastEvidenceReadError::Unavailable)
    }

    async fn revalidate(
        &self,
        _expected: &ForecastEvidenceRevalidation,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ForecastEvidenceReadError> {
        check_control(deadline, &cancellation)?;
        Err(ForecastEvidenceReadError::Unavailable)
    }
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ForecastEvidenceReadError> {
    if cancellation.is_cancelled() {
        Err(ForecastEvidenceReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ForecastEvidenceReadError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
