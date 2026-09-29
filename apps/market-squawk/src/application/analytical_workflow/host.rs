//! Installed-workspace custody for the existing bounded analytical controller.

use super::AnalyticalWorkflowController;
use market_squawk_platform::LocalPaths;
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, RequestOrigin, ResultEnvelopeProjection,
    ServiceError, ServiceLimits, ToolAuthorization, ToolServices,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    sync::{Arc, OnceLock, Weak},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowError {
    code: &'static str,
    message: String,
}
impl WorkflowError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub(crate) fn invalid_request(message: &'static str) -> Self {
        Self::new("invalid_request", message)
    }
    pub(crate) fn into_projection(self) -> Value {
        json!({"kind":"unavailable","code":self.code,"message":self.message})
    }
    pub(crate) fn internal() -> Self {
        Self::new(
            "internal",
            "Market Squawk could not complete this local request.",
        )
    }
}
impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for WorkflowError {}
impl From<ServiceError> for WorkflowError {
    fn from(error: ServiceError) -> Self {
        let code = match error {
            ServiceError::InvalidRequest => "invalid_request",
            ServiceError::NotFound => "not_found",
            ServiceError::Unauthorized => "unauthorized",
            ServiceError::ResourceExhausted => "resource_exhausted",
            ServiceError::Unavailable => "unavailable",
            ServiceError::InvalidResult => "invalid_result",
            ServiceError::Internal => "internal",
            ServiceError::Cancelled => "cancelled",
            ServiceError::DeadlineExceeded => "deadline_exceeded",
        };
        Self::new(code, error.to_string())
    }
}

/// Original authenticated origin retained with its bounded delegation; never accepted from input.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct WorkflowOrigin {
    workspace_id: Uuid,
    client_id: Uuid,
}
impl WorkflowOrigin {
    pub(super) fn new(origin: RequestOrigin) -> Self {
        Self {
            workspace_id: origin.workspace_id(),
            client_id: origin.client_id(),
        }
    }
    pub(super) fn admitted(self, workspace: Uuid) -> Result<RequestOrigin, WorkflowError> {
        if self.workspace_id != workspace {
            return Err(WorkflowError::internal());
        }
        RequestOrigin::try_new(self.workspace_id, self.client_id)
            .map_err(|_| WorkflowError::internal())
    }
}

pub(crate) struct WorkflowHost {
    controller: Arc<AnalyticalWorkflowController>,
    services: OnceLock<Weak<dyn ToolServices>>,
    work_available: tokio::sync::Notify,
    background_failure: std::sync::Mutex<Option<WorkflowError>>,
    cancellation: CancellationToken,
    fence: Arc<tokio::sync::Mutex<()>>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), WorkflowError>>>>,
}
impl WorkflowHost {
    pub(crate) fn open(paths: &LocalPaths, workspace: Uuid) -> Result<Arc<Self>, WorkflowError> {
        Ok(Arc::new(Self {
            controller: Arc::new(AnalyticalWorkflowController::try_open(paths, workspace)?),
            services: OnceLock::new(),
            work_available: tokio::sync::Notify::new(),
            background_failure: std::sync::Mutex::new(None),
            cancellation: CancellationToken::new(),
            fence: Arc::new(tokio::sync::Mutex::new(())),
            task: tokio::sync::Mutex::new(None),
        }))
    }
    pub(crate) fn bind(&self, services: Weak<dyn ToolServices>) -> Result<(), WorkflowError> {
        self.services
            .set(services)
            .map_err(|_| WorkflowError::internal())
    }
    pub(crate) fn generation(
        self: &Arc<Self>,
        origin: RequestOrigin,
    ) -> Result<Arc<WorkflowGeneration>, WorkflowError> {
        WorkflowOrigin::new(origin).admitted(self.controller.owner_workspace_id)?;
        if self.cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled.into());
        }
        Ok(Arc::new(WorkflowGeneration {
            host: Arc::clone(self),
            origin,
        }))
    }
    pub(crate) fn launch(self: &Arc<Self>, origin: RequestOrigin) -> Result<(), WorkflowError> {
        super::workflow_driver::launch(self.generation(origin)?);
        Ok(())
    }
    pub(crate) fn cancellation_on_drop(&self) -> tokio_util::sync::DropGuard {
        self.cancellation.clone().drop_guard()
    }
    pub(crate) async fn shutdown(&self) -> Result<(), WorkflowError> {
        {
            let _fence = self.fence.lock().await;
            self.cancellation.cancel();
        }
        let mut task = self.task.lock().await;
        WorkflowGeneration::join_analytical_driver(&mut task).await
    }
}

pub(crate) struct WorkflowState;
impl WorkflowState {
    pub(super) fn admit_current(
        &self,
        generation: &WorkflowGeneration,
    ) -> Result<(), WorkflowError> {
        if generation.host.cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled.into());
        }
        WorkflowOrigin::new(generation.origin)
            .admitted(generation.host.controller.owner_workspace_id)?;
        Ok(())
    }
}
pub(crate) struct WorkflowGeneration {
    host: Arc<WorkflowHost>,
    origin: RequestOrigin,
}
impl WorkflowGeneration {
    pub(super) fn work_available(&self) -> &tokio::sync::Notify {
        &self.host.work_available
    }
    pub(super) fn record_background_failure(
        &self,
        error: Option<WorkflowError>,
    ) -> Result<(), WorkflowError> {
        *self
            .host
            .background_failure
            .lock()
            .map_err(|_| WorkflowError::internal())? = error;
        Ok(())
    }
    pub(super) fn admit_background_health(&self) -> Result<(), WorkflowError> {
        match self
            .host
            .background_failure
            .lock()
            .map_err(|_| WorkflowError::internal())?
            .as_ref()
        {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
    pub(super) fn origin(&self) -> WorkflowOrigin {
        WorkflowOrigin::new(self.origin)
    }
    pub(super) fn for_origin(&self, origin: WorkflowOrigin) -> Result<Arc<Self>, WorkflowError> {
        self.host
            .generation(origin.admitted(self.host.controller.owner_workspace_id)?)
    }
    pub(super) fn for_workflow(&self, token: &str) -> Result<Arc<Self>, WorkflowError> {
        let document = self.analytical_controller().lock_document()?;
        let run = super::workflow_control::find_workflow(&document, token)?;
        self.for_origin(run.origin)
    }
    pub(super) fn analytical_controller(&self) -> &AnalyticalWorkflowController {
        &self.host.controller
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.host.cancellation.child_token()
    }
    pub(super) async fn analytical_retirement_fence(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.host.fence).lock_owned().await
    }
    pub(super) fn analytical_driver_task(
        &self,
    ) -> &tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), WorkflowError>>>> {
        &self.host.task
    }
    pub(super) async fn join_analytical_driver(
        task: &mut Option<tokio::task::JoinHandle<Result<(), WorkflowError>>>,
    ) -> Result<(), WorkflowError> {
        let Some(running) = task.as_mut() else {
            return Ok(());
        };
        let result = running.await;
        *task = None;
        result.map_err(|_| WorkflowError::new("analytical_driver_failed", "The background analysis stopped unexpectedly. Review the saved workflow before continuing."))?
    }
    fn services(&self) -> Result<Arc<dyn ToolServices>, WorkflowError> {
        self.host
            .services
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(WorkflowError::internal)
    }
    pub(super) fn has_operation(&self, name: &str) -> bool {
        self.services()
            .is_ok_and(|s| s.capabilities().find(name).is_some())
    }
}

#[derive(Clone, Copy)]
pub(super) enum InvocationAuthority {
    ReadOnly,
    ExactConfirmed(&'static str),
}
pub(super) fn desktop_result_limits() -> Value {
    json!({"maximumItems":1000,"maximumBytes":1048576})
}

pub(super) fn prepare_analytical_arguments(
    generation: &WorkflowGeneration,
    operation: &str,
    mut arguments: Map<String, Value>,
    authority: InvocationAuthority,
) -> Result<Map<String, Value>, WorkflowError> {
    let services = generation.services()?;
    let capabilities = services.capabilities();
    let descriptor = capabilities
        .find(operation)
        .ok_or_else(|| WorkflowError::from(ServiceError::NotFound))?;
    if matches!(authority, InvocationAuthority::ExactConfirmed(_)) {
        arguments.insert("confirm".into(), json!(true));
    }
    if descriptor
        .input_schema()
        .get("properties")
        .and_then(|v| v.get("resultLimits"))
        .is_some()
    {
        let limits = desktop_result_limits();
        if arguments
            .get("resultLimits")
            .is_some_and(|old| old != &limits)
        {
            return Err(WorkflowError::invalid_request(
                "The saved operation result limits differ from the workflow limits.",
            ));
        }
        arguments.insert("resultLimits".into(), limits);
    }
    Ok(arguments)
}

/// Private controller boundary. Its caller selects only the existing closed financial sequence;
/// public requests never supply operation names or authority modes to this function.
pub(super) async fn invoke_analytical_operation(
    generation: &Arc<WorkflowGeneration>,
    operation: &'static str,
    arguments: Map<String, Value>,
    authority: InvocationAuthority,
    request_id: RequestId,
    cancellation: CancellationToken,
) -> Result<Value, WorkflowError> {
    WorkflowState.admit_current(generation)?;
    let services = generation.services()?;
    let capabilities = services.capabilities();
    let descriptor = capabilities
        .find(operation)
        .ok_or_else(|| WorkflowError::from(ServiceError::NotFound))?;
    let authorized = match authority {
        InvocationAuthority::ReadOnly => {
            descriptor.effects().read_only()
                && descriptor.contract().authorization() == ToolAuthorization::ReadOnly
        }
        InvocationAuthority::ExactConfirmed(expected) => {
            expected == operation
                && !descriptor.effects().read_only()
                && descriptor.contract().authorization() == ToolAuthorization::LocalConfirmation
        }
    };
    if !authorized {
        return Err(ServiceError::Unauthorized.into());
    }
    let arguments = prepare_analytical_arguments(generation, operation, arguments, authority)?;
    let structure = JsonStructureLimits::try_new(64, 128 * 1024, 10_000, 2_000)
        .map_err(|_| WorkflowError::internal())?;
    market_squawk_services::validate_json_contract(
        &Value::Object(arguments.clone()),
        structure,
        256 * 1024,
    )
    .map_err(|_| {
        WorkflowError::invalid_request("The analysis request exceeds its retained limit.")
    })?;
    let request = descriptor
        .admit(arguments)
        .map_err(|_| WorkflowError::from(ServiceError::InvalidRequest))?;
    let limits = ServiceLimits::try_new(256 * 1024, 1000, 1024 * 1024, 1000, structure)
        .map_err(|_| WorkflowError::internal())?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let request_cancel = generation.cancellation();
    let _cancel_on_exit = request_cancel.clone().drop_guard();
    let context = RequestContext::new(request_id, request_cancel.clone(), deadline, limits)
        .with_origin(generation.origin);
    let result = tokio::select! { biased;
        () = request_cancel.cancelled() => return Err(ServiceError::Cancelled.into()),
        () = cancellation.cancelled() => return Err(ServiceError::Cancelled.into()),
        () = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded.into()),
        result = services.call(request, context) => result.map_err(WorkflowError::from)?,
    };
    result
        .validate_for(descriptor)
        .map_err(|_| WorkflowError::from(ServiceError::InvalidResult))?;
    Ok(result.into_envelope(ResultEnvelopeProjection::NativeEvidenceV1))
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn interrupted_driver_join_retains_task_custody_and_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::{future::Future, task::Poll, time::Duration};

        let (release, completion) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = completion.await;
            Err(super::WorkflowError::new(
                "retained_driver_failure",
                "The previous driver failed.",
            ))
        });
        let task_id = task.id();
        let slot = tokio::sync::Mutex::new(Some(task));
        {
            let mut owned = slot.lock().await;
            let mut interrupted = std::pin::pin!(
                super::WorkflowGeneration::join_analytical_driver(&mut owned)
            );
            // The task cannot complete until release is sent. Drop the pending join and
            // its slot guard exactly as cancellation drops the awaiting lifecycle future.
            assert!(
                std::future::poll_fn(|context| { Poll::Ready(interrupted.as_mut().poll(context)) })
                    .await
                    .is_pending()
            );
        }
        let mut retained = slot.lock().await;
        assert_eq!(retained.as_ref().map(|task| task.id()), Some(task_id));
        assert!(retained.as_ref().is_some_and(|task| !task.is_finished()));
        assert!(release.send(()).is_ok());
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            super::WorkflowGeneration::join_analytical_driver(&mut retained),
        )
        .await?;
        assert!(retained.is_none());
        // A successful Tokio join must preserve the failed predecessor's task result.
        let error = result.expect_err("the retained driver failure must reach its joining owner");
        assert_eq!(
            serde_json::to_value(error)?["code"],
            "retained_driver_failure"
        );
        Ok(())
    }
}
