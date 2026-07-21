//! Out-of-hot-path paper financial-state reconciliation ownership.

use std::sync::Arc;
use std::time::Duration;

use market_squawk_adapter_paper::{PaperFinancialChangeReadError, PaperFinancialChangeReader};
use market_squawk_execution::{
    AccountRiskReconciliationFence, ExecutionDispatchError, ExecutionDispatcher, ExecutionTask,
    ExecutionTaskReaper, ExecutionTaskReaperError,
};
use tokio_util::sync::CancellationToken;

const RECONCILIATION_RETRY_INTERVAL: Duration = Duration::from_millis(10);

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
    ) -> Result<Self, ExecutionTaskReaperError> {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.child_token();
        let task = task_reaper.try_reserve()?.spawn(run_supervisor(
            reader,
            dispatcher,
            fence,
            task_cancellation,
        ))?;
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
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PaperFinancialSupervisorShutdown {
    complete: bool,
    last_error: Option<ExecutionDispatchError>,
    reader_closed: bool,
}

impl PaperFinancialSupervisorShutdown {
    pub(super) const fn is_complete(self) -> bool {
        self.complete
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
    cancellation: CancellationToken,
) -> PaperFinancialSupervisorShutdown {
    let mut last_error = None;
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return PaperFinancialSupervisorShutdown {
                    complete: true,
                    last_error,
                    reader_closed: false,
                };
            }
            changed = reader.changed() => {
                if matches!(changed, Err(PaperFinancialChangeReadError::Closed)) {
                    return PaperFinancialSupervisorShutdown {
                        complete: true,
                        last_error,
                        reader_closed: true,
                    };
                }
            }
        }
        while !fence.is_current() {
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
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => {
                            return PaperFinancialSupervisorShutdown {
                                complete: true,
                                last_error,
                                reader_closed: false,
                            };
                        }
                        () = tokio::time::sleep(RECONCILIATION_RETRY_INTERVAL) => {}
                    }
                }
            }
        }
    }
}
