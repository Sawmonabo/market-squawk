//! Exact read-only Schwab OAuth and doctor activation for one account market runtime.

use std::sync::{Arc, Mutex};

use market_squawk_adapter_schwab::TransientAccessToken;
use market_squawk_sources::SchwabMarketDataDoctorReceiptV1;
use tokio_util::sync::CancellationToken;

use crate::provider_onboarding::{SchwabOAuthMarketAuthority, SchwabOAuthPublicationEpoch};
use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::ProviderAdapterActivation;
use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderAccountRuntimeAuthority,
    ProviderAccountRuntimeCurrentness, ProviderMarketAccount,
};

/// Non-clone owner of one callable Schwab read-only market-data epoch.
///
/// It retains the exact active onboarding lease, durable doctor receipt, protected OAuth market
/// authority, account-lifetime authority, and shared provider-rate authority. It exposes no
/// account, position, transaction, order, or money-movement operation.
pub struct SchwabMarketDataAccountActivation {
    authority: Arc<ProviderAccountRuntimeAuthority>,
    oauth: SchwabOAuthMarketAuthority,
    doctor: SchwabMarketDataDoctorReceiptV1,
    doctor_generation: Mutex<SchwabDoctorGenerationDisposition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchwabDoctorGenerationDisposition {
    Current(u64),
    RenewalRequired {
        doctor_generation: u64,
        observed_generation: u64,
    },
}

impl SchwabMarketDataAccountActivation {
    pub fn lease(&self) -> &ProviderActivationLease {
        self.authority.lease()
    }

    pub fn account_binding(&self) -> &ProviderAccountBinding {
        self.authority.binding()
    }

    pub(crate) fn oauth_authority(&self) -> SchwabOAuthMarketAuthority {
        self.oauth.clone()
    }

    pub(crate) const fn doctor_receipt(&self) -> &SchwabMarketDataDoctorReceiptV1 {
        &self.doctor
    }

    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.authority.currentness()
    }

    pub async fn require_current(&self) -> Result<(), SchwabMarketDataActivationError> {
        self.authority.require_current().await?;
        let current = self.oauth.current_receipt().await?;
        self.require_doctor_generation(current.generation().get())
    }

    /// Acquires one exact token/publication attempt behind the serialized OAuth barrier.
    ///
    /// A protected refresh may legitimately advance the token generation. That observation is
    /// latched as `DoctorRenewalRequired`; no request using the rotated token can proceed until
    /// onboarding publishes a fresh doctor receipt and constructs a successor activation.
    pub(crate) async fn acquire_publication_attempt(
        &self,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        self.authority.require_current().await?;
        let (token, epoch) = self.oauth.acquire_publication_attempt().await?;
        self.require_doctor_generation(epoch.receipt().generation().get())?;
        Ok((token, epoch))
    }

    fn require_doctor_generation(
        &self,
        observed_generation: u64,
    ) -> Result<(), SchwabMarketDataActivationError> {
        require_doctor_generation(&self.doctor_generation, observed_generation)
    }

    #[cfg(test)]
    pub(crate) async fn acquire_test_publication_attempt(
        oauth: &SchwabOAuthMarketAuthority,
        doctor_generation: u64,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        let (token, epoch) = oauth.acquire_publication_attempt().await?;
        let disposition = Mutex::new(SchwabDoctorGenerationDisposition::Current(
            doctor_generation,
        ));
        require_doctor_generation(&disposition, epoch.receipt().generation().get())?;
        Ok((token, epoch))
    }
}

fn require_doctor_generation(
    authority: &Mutex<SchwabDoctorGenerationDisposition>,
    observed_generation: u64,
) -> Result<(), SchwabMarketDataActivationError> {
    let mut disposition = authority
        .lock()
        .map_err(|_poisoned| SchwabMarketDataActivationError::AuthorityMismatch)?;
    match *disposition {
        SchwabDoctorGenerationDisposition::Current(doctor_generation)
            if doctor_generation == observed_generation =>
        {
            Ok(())
        }
        SchwabDoctorGenerationDisposition::Current(doctor_generation) => {
            *disposition = SchwabDoctorGenerationDisposition::RenewalRequired {
                doctor_generation,
                observed_generation,
            };
            Err(SchwabMarketDataActivationError::DoctorRenewalRequired)
        }
        SchwabDoctorGenerationDisposition::RenewalRequired { .. } => {
            Err(SchwabMarketDataActivationError::DoctorRenewalRequired)
        }
    }
}

impl std::fmt::Debug for SchwabMarketDataAccountActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabMarketDataAccountActivation")
            .field("authority", &self.authority)
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .field("doctor_receipt", &self.doctor.receipt_sha256())
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Activates the exact OAuth epoch proven by the retained durable Schwab doctor receipt.
    pub(crate) async fn activate_schwab_market_data_account(
        &self,
        lease: ProviderActivationLease,
        oauth: SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
    ) -> Result<SchwabMarketDataAccountActivation, SchwabMarketDataActivationError> {
        if cancellation.is_cancelled() {
            return Err(SchwabMarketDataActivationError::Cancelled);
        }
        let binding = ProviderAccountBinding::try_from_lease(
            ProviderMarketAccount::SchwabMarketData,
            &lease,
        )?;
        if oauth.session_id() != lease.session_id() {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let doctor = lease
            .runtime_verification_evidence()
            .schwab_market_data_receipt()
            .cloned()
            .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?;
        let current = oauth.current_receipt().await?;
        if cancellation.is_cancelled() {
            return Err(SchwabMarketDataActivationError::Cancelled);
        }
        if doctor.receipt_sha256() != binding.verification_evidence()
            || doctor.access_token_generation() != current.generation().get()
            || doctor.market_data_principal_sha256()
                != lease
                    .account_digest()
                    .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let authority = Arc::new(ProviderAccountRuntimeAuthority::try_acquire(
            ProviderMarketAccount::SchwabMarketData,
            lease,
            Arc::clone(&self.onboarding),
            &self.app_config,
            self.provider_rate.clone(),
        )?);
        let activation = SchwabMarketDataAccountActivation {
            authority,
            oauth,
            doctor,
            doctor_generation: Mutex::new(SchwabDoctorGenerationDisposition::Current(
                current.generation().get(),
            )),
        };
        activation.require_current().await?;
        Ok(activation)
    }
}

/// Schwab read-only market-data account activation failure.
#[derive(Debug, thiserror::Error)]
pub enum SchwabMarketDataActivationError {
    #[error("Schwab market-data activation was cancelled")]
    Cancelled,
    #[error("Schwab OAuth, doctor, account, or onboarding authority does not match")]
    AuthorityMismatch,
    #[error("Schwab OAuth token rotation requires a serialized doctor renewal")]
    DoctorRenewalRequired,
    #[error(transparent)]
    Account(#[from] ProviderAccountActivationError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    OAuth(#[from] crate::provider_onboarding::SchwabOAuthRuntimeError),
}
