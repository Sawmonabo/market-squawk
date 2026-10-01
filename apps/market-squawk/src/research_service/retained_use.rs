//! Current local policy for using retained evidence, independent of network credentials.

use super::{ResearchService, ResearchServiceError};
use market_squawk_data::{RetainedResearchUsePolicy, RightsBasis, SourceOperation};
use market_squawk_sources::{
    DataUseOperation, OperationAdmission, ProfileReleaseState, built_in_provider_profiles,
};
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

pub(super) fn current_policies() -> Result<Box<[RetainedResearchUsePolicy]>, ResearchServiceError> {
    let profiles = built_in_provider_profiles().map_err(crate::ProviderOnboardingError::from)?;
    let mut policies = Vec::new();
    for profile in profiles.iter() {
        if profile.release_state() != ProfileReleaseState::Available {
            continue;
        }
        let Some(evidence) = profile
            .persistence_evidence()
            .filter(|value| !value.refresh_required())
        else {
            continue;
        };
        let Some(digest) = evidence.content_digest() else {
            continue;
        };
        let operations = research_source_operations(|operation| {
            profile.rights().0.iter().any(|right| {
                right.operation() == operation && right.admission() == OperationAdmission::Admitted
            })
        });
        if !operations.contains(&SourceOperation::Persist) {
            continue;
        }
        policies.push(
            RetainedResearchUsePolicy::try_new(
                RightsBasis::reviewed_terms(evidence.official_url(), digest)?,
                profile.rights_decision_digest(),
                // These reviewed local-use profiles declare no retained-data expiry. Connection
                // verification and OAuth expiry continue to govern live acquisition independently.
                None,
                operations,
            )
            .map_err(|_| ResearchServiceError::IngestAuthorityMismatch)?,
        );
    }
    Ok(policies.into_boxed_slice())
}

pub(crate) fn research_source_operations(
    admits: impl Fn(DataUseOperation) -> bool,
) -> Vec<SourceOperation> {
    let mut operations = Vec::new();
    for (provider, research) in [
        (DataUseOperation::Retrieve, SourceOperation::Retrieve),
        (DataUseOperation::Display, SourceOperation::Display),
        (DataUseOperation::Persist, SourceOperation::Persist),
        (DataUseOperation::ModelTraining, SourceOperation::Train),
        (
            DataUseOperation::Redistribute,
            SourceOperation::Redistribute,
        ),
    ] {
        if admits(provider) {
            operations.push(research);
        }
    }
    if admits(DataUseOperation::Persist) {
        operations.push(SourceOperation::Cache);
    }
    operations
}

impl ResearchService {
    /// Admits exact lineage on the original retained synchronous I/O lane.
    /// The worker owns only the existing analytical service, never this worker's owner.
    pub(crate) async fn authorize_research_use(
        &self,
        request: market_squawk_data::ResearchUseRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Result<
            market_squawk_data::AuthorizedResearchUse,
            market_squawk_data::ResearchUseCatalogError,
        >,
        ResearchServiceError,
    > {
        let Some(traversal_deadline) =
            Instant::now().checked_add(request.limits().traversal_deadline())
        else {
            return Ok(Err(
                market_squawk_data::ResearchUseCatalogError::DeadlineExceeded,
            ));
        };
        let deadline = deadline.min(traversal_deadline);
        // Admit before occupying the I/O lane needed by original-capture gate holders.
        let admission = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(Err(market_squawk_data::ResearchUseCatalogError::Cancelled)),
            () = tokio::time::sleep_until(deadline.into()) => return Ok(Err(market_squawk_data::ResearchUseCatalogError::DeadlineExceeded)),
            admission = self.analytical.acquire_research_operation(cancellation) => match admission {
                Ok(admission) => admission,
                Err(market_squawk_data::IngestError::Cancelled) => return Ok(Err(market_squawk_data::ResearchUseCatalogError::Cancelled)),
                Err(error) => return Err(error.into()),
            },
        };
        let policies = Arc::clone(&self.retained_use_policies);
        let result = self
            .run_owned_research_io(deadline, cancellation, move |worker_cancellation| {
                admission.authorize_research_use_with_retained_policy(
                    request,
                    &policies,
                    deadline,
                    &worker_cancellation,
                )
            })
            .await?;
        if cancellation.is_cancelled() {
            return Err(market_squawk_data::IngestError::Cancelled.into());
        }
        if Instant::now() >= deadline {
            return Err(market_squawk_data::IngestError::DeadlineExceeded.into());
        }
        Ok(result)
    }

    /// Admits exact logical market observations on the same retained I/O lane.
    /// The worker owns only the existing analytical service, never this worker's owner.
    pub(crate) async fn authorize_market_event_use(
        &self,
        request: market_squawk_data::MarketEventUseRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Result<
            market_squawk_data::AuthorizedMarketEventUse,
            market_squawk_data::ResearchUseCatalogError,
        >,
        ResearchServiceError,
    > {
        let Some(traversal_deadline) =
            Instant::now().checked_add(request.limits().traversal_deadline())
        else {
            return Ok(Err(
                market_squawk_data::ResearchUseCatalogError::DeadlineExceeded,
            ));
        };
        let deadline = deadline.min(traversal_deadline);
        // Admit before occupying the I/O lane needed by original-capture gate holders.
        let admission = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(Err(market_squawk_data::ResearchUseCatalogError::Cancelled)),
            () = tokio::time::sleep_until(deadline.into()) => return Ok(Err(market_squawk_data::ResearchUseCatalogError::DeadlineExceeded)),
            admission = self.analytical.acquire_research_operation(cancellation) => match admission {
                Ok(admission) => admission,
                Err(market_squawk_data::IngestError::Cancelled) => return Ok(Err(market_squawk_data::ResearchUseCatalogError::Cancelled)),
                Err(error) => return Err(error.into()),
            },
        };
        let policies = Arc::clone(&self.retained_use_policies);
        let result = self
            .run_owned_research_io(deadline, cancellation, move |worker_cancellation| {
                admission.authorize_market_event_use_with_retained_policy(
                    request,
                    &policies,
                    deadline,
                    &worker_cancellation,
                )
            })
            .await?;
        if cancellation.is_cancelled() {
            return Err(market_squawk_data::IngestError::Cancelled.into());
        }
        if Instant::now() >= deadline {
            return Err(market_squawk_data::IngestError::DeadlineExceeded.into());
        }
        Ok(result)
    }
}
