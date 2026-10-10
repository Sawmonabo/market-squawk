//! Maintained economic-date acquisition through the existing Tiingo activation and shared budget.
use super::*;
use market_squawk_adapter_tiingo::{
    TiingoCapturedPage, TiingoCorporateActionReceipt, TiingoRequestSpec,
};
use market_squawk_services::{RequestContext, ServiceError};
pub(crate) struct TiingoCurrentActionAcquisition {
    pub(crate) captured: TiingoCapturedPage<TiingoCorporateActionReceipt>,
    pub(crate) publication: ResearchProviderPublicationOperation,
}
impl TiingoCurrentActionAcquisition {
    pub(crate) fn receipt(&self) -> &TiingoCorporateActionReceipt {
        self.captured.decoded()
    }
}
impl ProviderAdapterActivation {
    pub(crate) async fn acquire_tiingo_current_actions(
        &self,
        request: TiingoRequestSpec,
        context: &RequestContext,
    ) -> Result<TiingoCurrentActionAcquisition, ServiceError> {
        check(context)?;
        let activation = self
            .tiingo
            .read()
            .map_err(|_| ServiceError::Internal)?
            .as_ref()
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        let onboarding = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding
            .require_active(&activation.lease)
            .map_err(|_| ServiceError::Unavailable)?;
        let publication = self
            .research
            .acquire_provider_publication_operation(
                activation.generation(),
                context.cancellation().clone(),
                context.deadline(),
            )
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        drop(onboarding);
        let now = crate::application::market_calendar::MarketCalendarClock::now(
            &crate::application::market_calendar::SystemMarketCalendarClock,
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let remaining = context
            .deadline()
            .checked_duration_since(Instant::now())
            .ok_or(ServiceError::DeadlineExceeded)?;
        let deadline = now
            .checked_add_nanos(
                i64::try_from(remaining.as_nanos()).map_err(|_| ServiceError::InvalidRequest)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
        publication
            .validate_precommit()
            .map_err(|_| controlled(context, ServiceError::Internal))?;
        let captured = activation
            .source
            .fetch_corporate_actions(request, deadline, publication.cancellation())
            .await
            .map_err(|error| super::history::source_error(&error, context))?;
        publication
            .validate_precommit()
            .map_err(|_| controlled(context, ServiceError::Internal))?;
        check(context)?;
        Ok(TiingoCurrentActionAcquisition {
            captured,
            publication,
        })
    }
}
fn check(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn controlled(context: &RequestContext, error: ServiceError) -> ServiceError {
    check(context).err().unwrap_or(error)
}
