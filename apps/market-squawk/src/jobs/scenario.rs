//! Durable runner adapter for deterministic scenario batches.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    JobCompletion, JobRecoveryDisposition, JobRunContext, JobRunError, JobRunner,
};
use market_squawk_services::{ArtifactRepository, ServiceDomain, ServiceLimits, TypedToolRequest};

use crate::application::{ApplicationDomainService, job::JobAdmission};

use super::research::{ApplicationOperationJobRunner, ResearchJobRunnerError};

/// Scenario runner over the existing deterministic Analysis domain authority.
pub struct ScenarioJobRunner {
    operation: ApplicationOperationJobRunner,
    artifacts: Arc<dyn ArtifactRepository>,
}

impl ScenarioJobRunner {
    /// Creates the exact runner for bounded `Analysis.GetScenarios` batches.
    pub fn try_new(
        analysis: Arc<dyn ApplicationDomainService>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ResearchJobRunnerError> {
        let operation = ApplicationOperationJobRunner::try_new(
            "analysis.scenario-batch.v1",
            "Analysis.GetScenarios",
            "analysis.scenario-input.v1",
            "analysis.scenario-result.v1",
            ServiceDomain::Analysis,
            analysis,
            maximum_pending,
            run_timeout,
        )?;
        Ok(Self {
            operation,
            artifacts,
        })
    }

    /// Registers one descriptor-admitted immutable scenario request.
    pub fn admit(
        &self,
        request: TypedToolRequest,
        limits: ServiceLimits,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ResearchJobRunnerError> {
        self.operation.admit(request, limits, captured_at)
    }

    /// Releases one pending scenario batch when durable job creation did not succeed.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ResearchJobRunnerError> {
        self.operation.revoke(admission)
    }
}

#[async_trait]
impl JobRunner for ScenarioJobRunner {
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

impl fmt::Debug for ScenarioJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScenarioJobRunner")
            .field("operation", &"Analysis.GetScenarios")
            .field("authority", &"[DETERMINISTIC ANALYSIS DOMAIN]")
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .finish()
    }
}
