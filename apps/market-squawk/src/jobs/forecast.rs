//! Durable runner adapter for bounded research forecast generation.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    JobCompletion, JobRecoveryDisposition, JobRunContext, JobRunError, JobRunner,
};
use market_squawk_services::{ArtifactRepository, ServiceDomain, ServiceLimits, TypedToolRequest};

use crate::application::{ApplicationDomainService, job::JobAdmission};

use super::research::{ApplicationOperationJobRunner, ResearchJobRunnerError};

/// Forecast runner failure uses the shared closed application-operation contract.
pub type ForecastJobRunnerError = ResearchJobRunnerError;

/// Executes the descriptor-admitted terminal forecast operation and publishes its artifact.
pub struct ForecastJobRunner {
    operation: ApplicationOperationJobRunner,
    artifacts: Arc<dyn ArtifactRepository>,
}

impl ForecastJobRunner {
    /// Creates the exact runner for terminal `Model.GenerateForecast` requests.
    pub fn try_new(
        model: Arc<dyn ApplicationDomainService>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ForecastJobRunnerError> {
        let operation = ApplicationOperationJobRunner::try_new(
            "model.forecast-generation.v1",
            "Model.GenerateForecast",
            "model.forecast-input.v1",
            "model.forecast-vintage.v1",
            ServiceDomain::Model,
            model,
            maximum_pending,
            run_timeout,
        )?;
        Ok(Self {
            operation,
            artifacts,
        })
    }

    /// Registers one immutable descriptor-admitted terminal forecast request.
    pub fn admit(
        &self,
        request: TypedToolRequest,
        limits: ServiceLimits,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ForecastJobRunnerError> {
        self.operation.admit(request, limits, captured_at)
    }

    /// Releases a pending request if durable job creation did not succeed.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ForecastJobRunnerError> {
        self.operation.revoke(admission)
    }
}

#[async_trait]
impl JobRunner for ForecastJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        self.operation.kind()
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        self.operation
            .run_read_operation(context, &self.artifacts)
            .await
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for ForecastJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForecastJobRunner")
            .field("operation", &"Model.GenerateForecast")
            .field("authority", &"[ADMITTED MODEL FORECAST DOMAIN]")
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .finish()
    }
}
