//! Shared adapter-attempt isolation and outcome ownership.

use std::future::Future;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::{
    ExecutionAdapter, ExecutionAdapterError, ExecutionTaskPermit, ExecutionTaskReaperError,
};

pub(super) async fn attempt_adapter_call<T, F, Fut>(
    adapter: &Arc<dyn ExecutionAdapter>,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
    task_permit: Option<ExecutionTaskPermit>,
    call: F,
) -> (Result<T, ExecutionAdapterError>, bool)
where
    T: Send + 'static,
    F: FnOnce(Arc<dyn ExecutionAdapter>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, ExecutionAdapterError>> + Send + 'static,
{
    let future = call(Arc::clone(adapter));
    if adapter.is_cooperative() {
        return match tokio::time::timeout_at(deadline, future).await {
            Ok(result) => (result, false),
            Err(_) => {
                cancellation.cancel();
                (Err(ExecutionAdapterError::UncertainOutcome), true)
            }
        };
    }

    let Some(permit) = task_permit else {
        return (Err(ExecutionAdapterError::KnownFailure), false);
    };
    let mut task = match permit.spawn(future) {
        Ok(task) => task,
        Err(
            ExecutionTaskReaperError::Allocation
            | ExecutionTaskReaperError::ReaperUnavailable
            | ExecutionTaskReaperError::Saturated
            | ExecutionTaskReaperError::RuntimeUnavailable,
        ) => return (Err(ExecutionAdapterError::KnownFailure), false),
        Err(ExecutionTaskReaperError::OutcomeLost | ExecutionTaskReaperError::JoinFailed) => {
            return (Err(ExecutionAdapterError::UncertainOutcome), false);
        }
    };
    match tokio::time::timeout_at(deadline, task.join()).await {
        Ok(Ok(result)) => (result, false),
        Ok(Err(_)) => (Err(ExecutionAdapterError::UncertainOutcome), false),
        Err(_) => {
            cancellation.cancel();
            task.transfer();
            (Err(ExecutionAdapterError::UncertainOutcome), true)
        }
    }
}
