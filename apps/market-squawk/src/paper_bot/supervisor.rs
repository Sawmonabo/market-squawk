//! Out-of-hot-path paper portfolio publication and financial-state reconciliation ownership.
use super::portfolio::PaperPortfolioPublication;
use market_squawk_adapter_paper::{
    PaperExecutionAdapter, PaperFinancialChangeReadError, PaperFinancialChangeReader,
};
use market_squawk_execution::{
    AccountRiskReconciliationFence, ExecutionDispatchError, ExecutionDispatcher, ExecutionTask,
    ExecutionTaskReaper,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
const RECONCILIATION_RETRY_INTERVAL: Duration = Duration::from_millis(100);
#[derive(Debug)]
pub(super) struct PaperFinancialSupervisor {
    cancellation: CancellationToken,
    task: ExecutionTask<PaperFinancialSupervisorShutdown>,
}
impl PaperFinancialSupervisor {
    pub(super) fn try_start(
        reader: PaperFinancialChangeReader,
        dispatcher: Arc<ExecutionDispatcher>,
        fence: AccountRiskReconciliationFence,
        task_reaper: &ExecutionTaskReaper,
        adapter: Arc<PaperExecutionAdapter>,
        mut publication: Option<PaperPortfolioPublication>,
        timeout: Duration,
    ) -> Result<Self, super::ProductionPaperBotStartError> {
        if let Some(owner) = publication.as_mut() {
            owner
                .bind_financial_fence(fence.clone())
                .map_err(|_| super::ProductionPaperBotStartError::PortfolioPublication)?;
        }
        let cancellation = CancellationToken::new();
        let child = cancellation.child_token();
        let task = task_reaper
            .try_reserve()
            .map_err(super::ProductionPaperBotStartError::TaskOwnership)?
            .spawn(run_supervisor(
                reader,
                dispatcher,
                fence,
                adapter,
                publication,
                timeout,
                child,
            ))
            .map_err(super::ProductionPaperBotStartError::TaskOwnership)?;
        Ok(Self { cancellation, task })
    }
    pub(super) async fn shutdown(mut self) -> PaperFinancialSupervisorShutdown {
        self.cancellation.cancel();
        match self.task.join().await {
            Ok(outcome) => outcome,
            Err(_) => PaperFinancialSupervisorShutdown {
                complete: false,
                last_error: None,
                reader_closed: false,
                portfolio_current: false,
            },
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PaperFinancialSupervisorShutdown {
    complete: bool,
    last_error: Option<ExecutionDispatchError>,
    reader_closed: bool,
    portfolio_current: bool,
}
impl PaperFinancialSupervisorShutdown {
    pub(super) const fn is_complete(self) -> bool {
        self.complete && self.portfolio_current
    }
    pub(super) const fn last_error(self) -> Option<ExecutionDispatchError> {
        self.last_error
    }
    pub(super) const fn reader_closed(self) -> bool {
        self.reader_closed
    }
}
async fn run_supervisor(
    mut reader: PaperFinancialChangeReader,
    dispatcher: Arc<ExecutionDispatcher>,
    fence: AccountRiskReconciliationFence,
    adapter: Arc<PaperExecutionAdapter>,
    mut publication: Option<PaperPortfolioPublication>,
    timeout: Duration,
    cancellation: CancellationToken,
) -> PaperFinancialSupervisorShutdown {
    let mut last_error = None;
    let mut portfolio_current = publication.is_none();
    loop {
        if cancellation.is_cancelled() {
            return PaperFinancialSupervisorShutdown {
                complete: true,
                last_error,
                reader_closed: false,
                portfolio_current,
            };
        }
        // Publish the exact native worker cash/positions/claims before releasing its account fence.
        // An unavailable source or stale mark retains the reconciliation barrier and is retried.
        portfolio_current = match publication.as_mut() {
            Some(owner) => owner
                .reconcile(&adapter, cancellation.child_token(), timeout)
                .await
                .is_ok(),
            None => true,
        };
        if portfolio_current {
            while fence.applied_sequence() < fence.required_sequence() {
                let result = match dispatcher.reconcile().await {
                    Ok(state) if fence.is_current() => Ok(state),
                    Ok(_) | Err(ExecutionDispatchError::OrderNotTracked) => {
                        dispatcher.reconcile_accounts().await
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(_) => last_error = None,
                    Err(error) => {
                        last_error = Some(error);
                        break;
                    }
                }
            }
        }
        let retry = !portfolio_current || !fence.is_current();
        tokio::select! {biased;
            ()=cancellation.cancelled()=>return PaperFinancialSupervisorShutdown{complete:true,last_error,reader_closed:false,portfolio_current},
            changed=reader.changed()=>{if matches!(changed,Err(PaperFinancialChangeReadError::Closed)){return PaperFinancialSupervisorShutdown{complete:true,last_error,reader_closed:true,portfolio_current};}},
            ()=tokio::time::sleep(RECONCILIATION_RETRY_INTERVAL),if retry=>{}
        }
    }
}
