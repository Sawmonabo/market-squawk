//! Lifecycle-owned transport-neutral portfolio imports, immutable reads, and analytics.

mod analytics;
mod import;
mod model;
mod read;

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Instant;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, TypedToolRequest, TypedToolResult,
};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::application::ApplicationDomainService;

use self::import::{ImportAuthority, ImportRequest};
use self::model::PortfolioReadImage;

const DEFAULT_MAX_ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_RETAINED_BYTES: usize = 128 * 1024 * 1024;

/// Caller-selected resource ceilings for the portfolio application authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioApplicationLimitInput {
    /// Maximum independently published accounts.
    pub max_accounts: NonZeroUsize,
    /// Maximum immutable revisions retained for one account.
    pub max_history_per_account: NonZeroUsize,
    /// Maximum rows admitted by one portfolio result.
    pub max_result_items: NonZeroUsize,
    /// Maximum bytes retained by portfolio state and one result.
    pub max_retained_bytes: NonZeroUsize,
    /// Maximum serialized extraction artifact accepted by one import.
    pub max_artifact_bytes: NonZeroUsize,
}

/// Validated bounded portfolio application limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioApplicationLimits {
    max_accounts: usize,
    max_history_per_account: usize,
    max_result_items: usize,
    max_retained_bytes: usize,
    max_artifact_bytes: usize,
}

impl PortfolioApplicationLimits {
    /// Conservative local defaults below portfolio and authority-store hard ceilings.
    #[must_use]
    pub fn standard() -> Self {
        Self {
            max_accounts: 256,
            max_history_per_account: 4_096,
            max_result_items: 100_000,
            max_retained_bytes: DEFAULT_MAX_RETAINED_BYTES,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
        }
    }

    /// Validates application ceilings against the durable authority-store boundary.
    pub fn try_new(
        input: PortfolioApplicationLimitInput,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if input.max_artifact_bytes.get() > LocalAuthorityStateStore::maximum_payload_bytes()
            || input.max_retained_bytes.get() < input.max_artifact_bytes.get()
        {
            return Err(PortfolioApplicationServiceError::InvalidLimits);
        }
        Ok(Self {
            max_accounts: input.max_accounts.get(),
            max_history_per_account: input.max_history_per_account.get(),
            max_result_items: input.max_result_items.get(),
            max_retained_bytes: input.max_retained_bytes.get(),
            max_artifact_bytes: input.max_artifact_bytes.get(),
        })
    }
}

/// Portfolio composition, import, recovery, or analytical publication failure.
#[derive(Debug, Error)]
pub enum PortfolioApplicationServiceError {
    /// Configured limits are internally inconsistent.
    #[error("portfolio application limits are invalid")]
    InvalidLimits,
    /// Controlled local paths are unavailable.
    #[error("portfolio controlled local paths are unavailable")]
    Path,
    /// Durable publication or adapter authority could not be opened.
    #[error("portfolio durable authority is unavailable")]
    Authority,
    /// Durable publication state is malformed or no longer matches retained evidence.
    #[error("portfolio publication state is corrupt")]
    CorruptPublication,
    /// An admitted artifact, account, or point-in-time scope is invalid.
    #[error("portfolio request is invalid")]
    InvalidRequest,
    /// Requested account or point-in-time revision does not exist.
    #[error("portfolio account or revision was not found")]
    NotFound,
    /// A bounded memory, row, byte, source, or history ceiling was exceeded.
    #[error("portfolio resource limit was exceeded")]
    ResourceExhausted,
    /// Request or service cancellation won the lifecycle race.
    #[error("portfolio request was cancelled")]
    Cancelled,
    /// Request deadline elapsed.
    #[error("portfolio request deadline elapsed")]
    DeadlineExceeded,
    /// Raw-preserving adapter validation or reconciliation rejected the import.
    #[error("portfolio import failed validation")]
    Import,
    /// Immutable revision construction or publication failed.
    #[error("portfolio revision publication failed")]
    Publication,
    /// A Task 12 analytical kernel rejected the available evidence.
    #[error("portfolio analytical calculation failed")]
    Analytics,
}

impl PortfolioApplicationServiceError {
    fn as_service_error(&self) -> ServiceError {
        match self {
            Self::InvalidLimits | Self::InvalidRequest | Self::Import => {
                ServiceError::InvalidRequest
            }
            Self::NotFound => ServiceError::NotFound,
            Self::ResourceExhausted => ServiceError::ResourceExhausted,
            Self::Cancelled => ServiceError::Cancelled,
            Self::DeadlineExceeded => ServiceError::DeadlineExceeded,
            Self::Path | Self::Authority => ServiceError::Unavailable,
            Self::CorruptPublication | Self::Publication | Self::Analytics => {
                ServiceError::Internal
            }
        }
    }
}

/// Sole portfolio-domain implementation shared by CLI and MCP.
#[derive(Clone)]
pub struct PortfolioApplicationService {
    runtime: Arc<Runtime>,
}

impl PortfolioApplicationService {
    /// Opens durable raw/publication authority and reconstructs the exact immutable read image.
    pub fn try_new(
        paths: &LocalPaths,
        limits: PortfolioApplicationLimits,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let artifacts = paths
            .artifacts()
            .map_err(|_| PortfolioApplicationServiceError::Path)?
            .clone();
        let control = paths
            .control_root()
            .map_err(|_| PortfolioApplicationServiceError::Path)?;
        control
            .try_clone_directory()
            .map_err(|_| PortfolioApplicationServiceError::Path)?
            .create_dir_all("portfolio")
            .map_err(|_| PortfolioApplicationServiceError::Path)?;
        let (authority, image) =
            ImportAuthority::restore(artifacts.clone(), control.root(), limits)?;
        Ok(Self {
            runtime: Arc::new(Runtime {
                artifacts,
                limits,
                authority: std::sync::Mutex::new(authority),
                image: ArcSwap::from(Arc::new(image)),
                accepting: AtomicBool::new(true),
                cancellation: CancellationToken::new(),
                active: AtomicUsize::new(0),
                idle: Notify::new(),
            }),
        })
    }
}

impl fmt::Debug for PortfolioApplicationService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioApplicationService")
            .field("accepting", &self.runtime.accepting.load(Ordering::Acquire))
            .field(
                "active_operations",
                &self.runtime.active.load(Ordering::Acquire),
            )
            .field("artifacts", &"[CONTROLLED ARTIFACT CAPABILITY]")
            .finish()
    }
}

struct Runtime {
    artifacts: market_squawk_platform::ArtifactRoot,
    limits: PortfolioApplicationLimits,
    authority: std::sync::Mutex<ImportAuthority>,
    image: ArcSwap<PortfolioReadImage>,
    accepting: AtomicBool,
    cancellation: CancellationToken,
    active: AtomicUsize,
    idle: Notify,
}

impl Runtime {
    fn admit(self: &Arc<Self>) -> Result<OperationGuard, PortfolioApplicationServiceError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(PortfolioApplicationServiceError::Cancelled);
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) {
            self.operation_complete();
            return Err(PortfolioApplicationServiceError::Cancelled);
        }
        Ok(OperationGuard {
            runtime: Arc::clone(self),
        })
    }

    fn operation_complete(&self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle.notify_waiters();
        }
    }

    fn import(
        &self,
        request: ImportRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
        ensure_live(self, context)?;
        let mut authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let publication = authority.import(&self.artifacts, request, context, self)?;
        self.image.store(Arc::new(publication.image));
        Ok(publication.result)
    }
}

struct OperationGuard {
    runtime: Arc<Runtime>,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.runtime.operation_complete();
    }
}

#[async_trait]
impl ApplicationDomainService for PortfolioApplicationService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Portfolio
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let guard = self
            .runtime
            .admit()
            .map_err(|error| error.as_service_error())?;
        ensure_live(&self.runtime, &context).map_err(|error| error.as_service_error())?;
        if request.contract().domain() != ServiceDomain::Portfolio {
            return Err(ServiceError::InvalidRequest);
        }
        if request.name() == "Portfolio.Import" {
            let import =
                ImportRequest::from_request(&request).map_err(|error| error.as_service_error())?;
            let runtime = Arc::clone(&self.runtime);
            let worker_context = context.clone();
            return tokio::task::spawn_blocking(move || {
                let _guard = guard;
                runtime.import(import, &worker_context)
            })
            .await
            .map_err(|_| ServiceError::Internal)?
            .map_err(|error| error.as_service_error());
        }
        let _guard = guard;
        read::call(
            &self.runtime.image.load(),
            &request,
            &context,
            self.runtime.limits,
        )
        .map_err(|error| error.as_service_error())
    }

    fn begin_shutdown(&self) {
        if self.runtime.accepting.swap(false, Ordering::AcqRel) {
            self.runtime.cancellation.cancel();
            self.runtime.idle.notify_waiters();
        }
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        loop {
            let notified = self.runtime.idle.notified();
            if self.runtime.active.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ServiceError::DeadlineExceeded);
            }
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), notified)
                .await
                .map_err(|_| ServiceError::DeadlineExceeded)?;
        }
    }
}

fn ensure_live(
    runtime: &Runtime,
    context: &RequestContext,
) -> Result<(), PortfolioApplicationServiceError> {
    if runtime.cancellation.is_cancelled() || context.cancellation().is_cancelled() {
        return Err(PortfolioApplicationServiceError::Cancelled);
    }
    if Instant::now() >= context.deadline() {
        return Err(PortfolioApplicationServiceError::DeadlineExceeded);
    }
    Ok(())
}

impl From<std::num::TryFromIntError> for PortfolioApplicationServiceError {
    fn from(_source: std::num::TryFromIntError) -> Self {
        Self::ResourceExhausted
    }
}
