//! Installed provider activation authority shared by native Settings and authorized CLI setup.

use std::time::Instant;

use async_trait::async_trait;
use market_squawk_domain::SourceIdentifier;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::contracts::{
    OnboardingSessionView, ProviderPortalActivationRequest, ProviderPortalActivationView,
    SchwabOAuthLifecycleAction, SchwabOAuthLifecycleView,
};

/// Application-owned authority that completes onboarding and registers one durable adapter.
#[async_trait]
pub trait ProviderPortalActivationAuthority: Send + Sync {
    /// Activates the exact session and provider-specific configuration.
    async fn activate(
        &self,
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, ProviderPortalActivationError>;

    /// Resumes publication from an unchanged admitted research recipe and runtime generation.
    /// The response may acknowledge pending data; it never replaces a completed-read receipt.
    async fn resume_research_publication(
        &self,
        _session_id: Uuid,
        _cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, ProviderPortalActivationError> {
        Err(ProviderPortalActivationError::Unavailable)
    }

    /// Revalidates a desired saved configuration through the same retained activation owner.
    async fn verify_saved_setup(
        &self,
        _session_id: Uuid,
        _cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, ProviderPortalActivationError> {
        Err(ProviderPortalActivationError::Unavailable)
    }

    /// Returns only the identity of the exact desired saved setup, without exporting its recipe.
    fn retained_setup_session(
        &self,
        _profile: &SourceIdentifier,
    ) -> Result<Option<Uuid>, ProviderPortalActivationError> {
        Ok(None)
    }

    /// Reports actual retained publication work for one selected saved setup. This read grants no activation authority.
    async fn setup_publication_pending(&self, _session_id: Uuid) -> bool {
        false
    }

    /// Returns the exact fixed discovery dataset carried by one callable provider runtime.
    fn provider_dataset_identifier(
        &self,
        _profile: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ProviderPortalActivationError> {
        Ok(None)
    }

    /// Revokes callable runtime authority before deterministic onboarding cleanup.
    async fn cancel(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<OnboardingSessionView, ProviderPortalActivationError>;

    /// Applies one exact application-owned Schwab OAuth lifecycle operation.
    ///
    /// The default remains unavailable until the application runtime supplies the sole callback
    /// session and protected token authority owner.
    async fn schwab_oauth(
        &self,
        _session_id: Uuid,
        _action: SchwabOAuthLifecycleAction,
        _cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, ProviderPortalActivationError> {
        Err(ProviderPortalActivationError::Unavailable)
    }

    /// Closes admission to application-owned activation work before installed service teardown.
    fn begin_shutdown(&self) {}

    /// Joins any retained activation reconciliation through the application shutdown deadline.
    async fn finish_shutdown(
        &self,
        _deadline: Instant,
    ) -> Result<(), ProviderPortalActivationError> {
        Ok(())
    }
}

/// Closed installed adapter activation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderPortalActivationError {
    /// The provider configuration or session/surface pairing is invalid.
    #[error("provider adapter request is invalid")]
    InvalidRequest,
    /// Onboarding or adapter activation is not currently admitted.
    #[error("provider adapter activation is unavailable")]
    Unavailable,
    /// Durable activation state could not be committed.
    #[error("provider adapter state is unavailable")]
    StateUnavailable,
    /// The caller or application lifecycle cancelled the operation.
    #[error("provider adapter activation was cancelled")]
    Cancelled,
}
