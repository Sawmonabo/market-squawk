//! FRED/ALFRED foreground restart composition.

use market_squawk_adapter_fred::{FredOperation, FredRightsDisposition, FredSource};
use market_squawk_domain::{SourceIdentifier, Timestamp};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    FRED_SURFACE, FredAdapterActivation, ProviderActivationOutcome, ProviderAdapterActivation,
    ProviderAdapterActivationError, require_surface,
};

/// Exact FRED dataset and policy retained for one foreground restart.
#[derive(Debug)]
pub struct FredRestartActivation {
    specification: FredAdapterActivation,
    provider_dataset: SourceIdentifier,
}

impl FredRestartActivation {
    /// Binds one exact FRED/ALFRED observations interval to its admitted source policy.
    ///
    /// # Errors
    ///
    /// Rejects an invalid provider dataset or a series that does not have effective persistence
    /// authority in the retained activation policy.
    pub fn try_new(
        specification: FredAdapterActivation,
        provider_dataset: SourceIdentifier,
        effective_at: Timestamp,
    ) -> Result<Self, ProviderAdapterActivationError> {
        let series = FredSource::rights_subject_identifier(&provider_dataset)?;
        let decision = specification
            .policy
            .assess(&series, &[FredOperation::Persist], effective_at)
            .map_err(|_| ProviderAdapterActivationError::InvalidRights)?;
        if decision.disposition() != FredRightsDisposition::Permitted {
            return Err(ProviderAdapterActivationError::InvalidRights);
        }
        Ok(Self {
            specification,
            provider_dataset,
        })
    }

    /// Returns the exact restart-stable provider dataset.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    fn into_specification(self) -> FredAdapterActivation {
        self.specification
    }
}

impl ProviderAdapterActivation {
    /// Restores one already-active FRED runtime through an explicit foreground credential read.
    ///
    /// The exact provider dataset is validated before the protected credential boundary is
    /// crossed. Restart therefore recreates the same source, series, policy, and runtime
    /// generation instead of selecting a provider dataset in process memory.
    pub async fn restore_active_fred_profile(
        &self,
        session_id: Uuid,
        activation: FredRestartActivation,
        cancellation: CancellationToken,
    ) -> Result<ProviderActivationOutcome, ProviderAdapterActivationError> {
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        FredSource::rights_subject_identifier(activation.provider_dataset())?;
        let lease = self.onboarding.activation_lease(session_id)?;
        require_surface(&lease, FRED_SURFACE)?;
        self.activate_fred(lease, activation.into_specification(), cancellation)
            .await
            .map(Into::into)
    }
}
