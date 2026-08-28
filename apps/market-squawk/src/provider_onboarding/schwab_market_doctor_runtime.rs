//! Activation-bound composition for the Schwab market-data doctor.
//!
//! This leaf owns no provider route, credential, rate budget, raw-capture store, or catalog
//! writer. It composes the exact authorities already owned by onboarding, OAuth, research, and the
//! provider-native probe executor, then proves that an observed receipt was durably reproduced by
//! the onboarding catalog before returning it.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use market_squawk_adapter_schwab::SchwabOAuthAuthorityReceipt;
use market_squawk_sources::RuntimeVerificationEvidence;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::contracts::SchwabOAuthBootstrapLease;
use super::schwab_market_doctor::{
    SchwabMarketDataDoctorError, SchwabMarketDataDoctorExecutor, SchwabMarketDataDoctorOutcome,
    SchwabMarketDataDoctorRun, SchwabMarketDoctorCaptureSealer, SchwabMarketDoctorProbeExecutor,
    SchwabMarketDoctorSetupRequiredEvidence,
};
use super::schwab_oauth_runtime::SchwabOAuthMarketAuthority;
use super::{ProviderOnboardingError, ProviderOnboardingService};
use crate::research_service::ResearchService;

/// Truthful activation-doctor terminal. Setup-required evidence is never recorded as an observed
/// provider capability; an observed run is returned only after its exact typed receipt is durable.
#[derive(Debug)]
pub(crate) enum SchwabMarketDoctorRuntimeTerminal {
    Observed(SchwabMarketDataDoctorRun),
    SetupRequired(SchwabMarketDoctorSetupRequiredEvidence),
}

impl SchwabMarketDoctorRuntimeTerminal {
    pub(crate) const fn observed(&self) -> Option<&SchwabMarketDataDoctorRun> {
        match self {
            Self::Observed(run) => Some(run),
            Self::SetupRequired(_) => None,
        }
    }

    pub(crate) const fn setup_required(&self) -> Option<&SchwabMarketDoctorSetupRequiredEvidence> {
        match self {
            Self::Observed(_) => None,
            Self::SetupRequired(evidence) => Some(evidence),
        }
    }
}

/// One installed-process composition of onboarding, analytical raw sealing, and exact probes.
pub(crate) struct SchwabMarketDoctorRuntimeCoordinator {
    onboarding: Arc<ProviderOnboardingService>,
    research: Arc<ResearchService>,
    probes: Arc<dyn SchwabMarketDoctorProbeExecutor>,
}

impl SchwabMarketDoctorRuntimeCoordinator {
    pub(crate) fn new(
        onboarding: Arc<ProviderOnboardingService>,
        research: Arc<ResearchService>,
        probes: Arc<dyn SchwabMarketDoctorProbeExecutor>,
    ) -> Self {
        Self {
            onboarding,
            research,
            probes,
        }
    }

    /// Runs one doctor against one exact current bootstrap lease and OAuth market authority.
    ///
    /// The catalog append is the sole irreversible step. Cancellation/deadline wins while waiting
    /// for its mutation lock, but once the synchronous append can commit, this method completes
    /// post-commit receipt and authority reconciliation before reporting a terminal.
    pub(crate) async fn run(
        &self,
        lease: SchwabOAuthBootstrapLease,
        authority: SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabMarketDoctorRuntimeTerminal, SchwabMarketDoctorRuntimeError> {
        ensure_active(&cancellation, deadline)?;
        if lease.session_id() != authority.session_id() {
            return Err(SchwabMarketDoctorRuntimeError::AuthorityMismatch);
        }

        // Both calls revalidate the complete lease against current durable onboarding state. The
        // returned factory is deliberately dropped unopened; no second OAuth authority is made.
        drop(self.onboarding.schwab_oauth_authority_factory(&lease)?);
        let opening_oauth = current_receipt(&authority, &cancellation, deadline).await?;
        let binding = self
            .onboarding
            .schwab_market_doctor_authority_binding(&lease)?;
        let rate = self
            .onboarding
            .schwab_market_doctor_rate_authority(&lease)?;
        let sealer: Arc<dyn SchwabMarketDoctorCaptureSealer> = self.research.clone();
        let doctor =
            SchwabMarketDataDoctorExecutor::try_new(rate, sealer, Arc::clone(&self.probes))?;
        let outcome = bounded(
            doctor.run(
                binding,
                authority.clone(),
                cancellation.child_token(),
                deadline,
            ),
            &cancellation,
            deadline,
        )
        .await??;

        let after_doctor = current_receipt(&authority, &cancellation, deadline).await?;
        if after_doctor != opening_oauth {
            return Err(SchwabMarketDoctorRuntimeError::AuthorityChanged);
        }
        drop(self.onboarding.schwab_oauth_authority_factory(&lease)?);

        match outcome {
            SchwabMarketDataDoctorOutcome::SetupRequired(evidence) => {
                ensure_active(&cancellation, deadline)?;
                Ok(SchwabMarketDoctorRuntimeTerminal::SetupRequired(evidence))
            }
            SchwabMarketDataDoctorOutcome::Observed(run) => {
                validate_run_authority(&lease, opening_oauth, &run)?;
                // Re-derive the service binding immediately before mutation. This is a fresh
                // exact-lease/current-lifecycle check, not a second authority implementation.
                let _current_binding = self
                    .onboarding
                    .schwab_market_doctor_authority_binding(&lease)?;
                let notifier = RecordCancellationNotifier::start(cancellation.clone(), deadline);
                let record_result = self
                    .onboarding
                    .record_schwab_market_data_doctor_observation(
                        &lease,
                        run.observation().clone(),
                        notifier.record_cancellation(),
                    )
                    .await;
                let cancellation_reason = notifier.finish().await;
                match record_result {
                    Ok(()) => {}
                    Err(ProviderOnboardingError::OperationCancelled) => {
                        return Err(match cancellation_reason {
                            RecordCancellationReason::Caller => {
                                SchwabMarketDoctorRuntimeError::Cancelled
                            }
                            RecordCancellationReason::Deadline => {
                                SchwabMarketDoctorRuntimeError::Deadline
                            }
                            RecordCancellationReason::None => {
                                SchwabMarketDoctorRuntimeError::CancellationSourceMismatch
                            }
                        });
                    }
                    Err(error) => return Err(error.into()),
                }

                // From here on the append is durable. Do not convert late cancellation into a
                // false no-effect result; reconcile the exact retained receipt and authorities.
                self.reconcile_recorded_run(&lease, opening_oauth, &authority, &run)
                    .await?;
                Ok(SchwabMarketDoctorRuntimeTerminal::Observed(run))
            }
        }
    }

    async fn reconcile_recorded_run(
        &self,
        lease: &SchwabOAuthBootstrapLease,
        opening_oauth: SchwabOAuthAuthorityReceipt,
        authority: &SchwabOAuthMarketAuthority,
        run: &SchwabMarketDataDoctorRun,
    ) -> Result<(), SchwabMarketDoctorRuntimeError> {
        let retained = self
            .onboarding
            .retained_runtime_verification_evidence(
                lease.session_id(),
                lease.surface_id(),
                run.receipt().public_configuration_digest(),
                lease.generation(),
            )
            .map_err(SchwabMarketDoctorRuntimeError::PostCommitOnboarding)?;
        let RuntimeVerificationEvidence::SchwabMarketDataDoctorReceiptV1(durable) =
            retained.evidence()
        else {
            return Err(SchwabMarketDoctorRuntimeError::DurableReceiptMismatch);
        };
        if durable.as_ref() != run.receipt() {
            return Err(SchwabMarketDoctorRuntimeError::DurableReceiptMismatch);
        }
        drop(
            self.onboarding
                .schwab_oauth_authority_factory(lease)
                .map_err(SchwabMarketDoctorRuntimeError::PostCommitOnboarding)?,
        );
        let current = authority
            .current_receipt()
            .await
            .map_err(|_| SchwabMarketDoctorRuntimeError::PostCommitAuthorityUnavailable)?;
        if current != opening_oauth {
            return Err(SchwabMarketDoctorRuntimeError::PostCommitAuthorityChanged);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RecordCancellationReason {
    None = 0,
    Caller = 1,
    Deadline = 2,
}

/// Scoped bridge from caller/deadline signals into the service-owned record cancellation token.
///
/// The service future is awaited directly. Dropping the enclosing coordinator future aborts this
/// notifier and cancels its child token, while normal completion cancels and joins it.
struct RecordCancellationNotifier {
    record_cancellation: CancellationToken,
    stop: CancellationToken,
    reason: Arc<AtomicU8>,
    task: Option<JoinHandle<()>>,
}

impl RecordCancellationNotifier {
    fn start(caller: CancellationToken, deadline: Instant) -> Self {
        let record_cancellation = CancellationToken::new();
        let stop = CancellationToken::new();
        let reason = Arc::new(AtomicU8::new(RecordCancellationReason::None as u8));
        let task_record_cancellation = record_cancellation.clone();
        let task_stop = stop.clone();
        let task_reason = Arc::clone(&reason);
        let task = tokio::spawn(async move {
            let selected = tokio::select! {
                biased;
                () = caller.cancelled() => RecordCancellationReason::Caller,
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    RecordCancellationReason::Deadline
                }
                () = task_stop.cancelled() => return,
            };
            task_reason.store(selected as u8, Ordering::Release);
            task_record_cancellation.cancel();
        });
        Self {
            record_cancellation,
            stop,
            reason,
            task: Some(task),
        }
    }

    fn record_cancellation(&self) -> CancellationToken {
        self.record_cancellation.clone()
    }

    async fn finish(mut self) -> RecordCancellationReason {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _joined = task.await;
        }
        self.reason()
    }

    fn reason(&self) -> RecordCancellationReason {
        match self.reason.load(Ordering::Acquire) {
            value if value == RecordCancellationReason::Caller as u8 => {
                RecordCancellationReason::Caller
            }
            value if value == RecordCancellationReason::Deadline as u8 => {
                RecordCancellationReason::Deadline
            }
            _ => RecordCancellationReason::None,
        }
    }
}

impl Drop for RecordCancellationNotifier {
    fn drop(&mut self) {
        self.record_cancellation.cancel();
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl fmt::Debug for SchwabMarketDoctorRuntimeCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabMarketDoctorRuntimeCoordinator")
            .field("onboarding", &"[SERVICE-OWNED CATALOG/RATE AUTHORITY]")
            .field("research", &"[APPLICATION RAW-SEAL AUTHORITY]")
            .field("probes", &self.probes)
            .finish()
    }
}

fn validate_run_authority(
    lease: &SchwabOAuthBootstrapLease,
    oauth: SchwabOAuthAuthorityReceipt,
    run: &SchwabMarketDataDoctorRun,
) -> Result<(), SchwabMarketDoctorRuntimeError> {
    let receipt = run.receipt();
    if receipt.surface_id() != lease.surface_id()
        || receipt.application_credential_generation() != lease.generation()
        || receipt.access_token_generation() != oauth.generation().get()
        || receipt.session_identifier().as_str() != lease.session_id().to_string()
        || receipt.observation() != run.observation()
    {
        return Err(SchwabMarketDoctorRuntimeError::AuthorityMismatch);
    }
    Ok(())
}

async fn current_receipt(
    authority: &SchwabOAuthMarketAuthority,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<SchwabOAuthAuthorityReceipt, SchwabMarketDoctorRuntimeError> {
    bounded(authority.current_receipt(), cancellation, deadline)
        .await?
        .map_err(|_| SchwabMarketDoctorRuntimeError::AuthorityUnavailable)
}

async fn bounded<T>(
    operation: impl Future<Output = T>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T, SchwabMarketDoctorRuntimeError> {
    ensure_active(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabMarketDoctorRuntimeError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(SchwabMarketDoctorRuntimeError::Deadline)
        }
        output = operation => Ok(output),
    }
}

fn ensure_active(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), SchwabMarketDoctorRuntimeError> {
    if cancellation.is_cancelled() {
        Err(SchwabMarketDoctorRuntimeError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SchwabMarketDoctorRuntimeError::Deadline)
    } else {
        Ok(())
    }
}

/// Secret-free application-composition failure. Post-commit variants explicitly disclose that
/// durable observation recording succeeded before reconciliation failed.
#[derive(Debug, Error)]
pub(crate) enum SchwabMarketDoctorRuntimeError {
    #[error("the Schwab doctor runtime authority does not match the exact bootstrap lease")]
    AuthorityMismatch,
    #[error("the Schwab OAuth market authority is unavailable")]
    AuthorityUnavailable,
    #[error("the Schwab OAuth market authority changed during the doctor run")]
    AuthorityChanged,
    #[error("the Schwab doctor operation was cancelled before durable recording")]
    Cancelled,
    #[error("the Schwab doctor deadline elapsed before durable recording")]
    Deadline,
    #[error("the Schwab doctor record cancellation source could not be reconciled")]
    CancellationSourceMismatch,
    #[error(
        "the Schwab doctor was recorded, but its durable receipt did not reproduce the observed receipt"
    )]
    DurableReceiptMismatch,
    #[error("the Schwab doctor was recorded, but its bootstrap lease could not be reconciled")]
    PostCommitOnboarding(#[source] ProviderOnboardingError),
    #[error("the Schwab doctor was recorded, but the OAuth authority became unavailable")]
    PostCommitAuthorityUnavailable,
    #[error("the Schwab doctor was recorded, but the OAuth authority changed")]
    PostCommitAuthorityChanged,
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    Doctor(#[from] SchwabMarketDataDoctorError),
}
