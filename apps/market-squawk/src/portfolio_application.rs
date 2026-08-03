//! Lifecycle-owned transport-neutral portfolio imports, immutable reads, and analytics.

mod advanced;
mod analytics;
mod backup;
mod import;
mod model;
mod read;

pub(crate) use backup::{
    PORTFOLIO_BACKUP_PRODUCER, PORTFOLIO_BACKUP_SCHEMA, PortfolioBackupAuthority,
    PortfolioBackupComponent, RetainedPortfolioBackupSnapshot, TRANSACTION_BACKUP_SCHEMA,
};
pub(crate) use import::{
    GovernedImportCommitReceipt, PortfolioImportInterpretation, PortfolioImportPreview,
    ServerHeldPortfolioImportResolution,
};

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Instant;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use market_squawk_domain::AccountId;
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
use market_squawk_portfolio::PortfolioRevision;
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
    /// A consistent backup cannot be retained while a governed import is pending.
    #[error("portfolio backup snapshot is unavailable while an import is pending")]
    SnapshotUnavailable,
    /// Restore was directed at a workspace that already contains portfolio authority state.
    #[error("portfolio restore target is not fresh")]
    RestoreTargetNotFresh,
    /// A Task 12 analytical kernel rejected the available evidence.
    #[error("portfolio analytical calculation failed")]
    Analytics,
}

impl PortfolioApplicationServiceError {
    pub(crate) fn as_service_error(&self) -> ServiceError {
        match self {
            Self::InvalidLimits | Self::InvalidRequest | Self::Import => {
                ServiceError::InvalidRequest
            }
            Self::NotFound => ServiceError::NotFound,
            Self::ResourceExhausted => ServiceError::ResourceExhausted,
            Self::Cancelled => ServiceError::Cancelled,
            Self::DeadlineExceeded => ServiceError::DeadlineExceeded,
            Self::Path
            | Self::Authority
            | Self::SnapshotUnavailable
            | Self::RestoreTargetNotFresh => ServiceError::Unavailable,
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
        Ok(Self::from_restored(artifacts, limits, authority, image))
    }

    fn from_restored(
        artifacts: market_squawk_platform::ArtifactRoot,
        limits: PortfolioApplicationLimits,
        authority: ImportAuthority,
        image: PortfolioReadImage,
    ) -> Self {
        Self {
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
        }
    }

    /// Returns a paired backup capability without exposing import or publication authority.
    pub(crate) fn backup_authority(&self) -> PortfolioBackupAuthority {
        PortfolioBackupAuthority {
            runtime: Arc::clone(&self.runtime),
        }
    }

    /// Maximum verified bytes the staged-input boundary may transfer to this authority.
    pub(crate) fn maximum_staged_import_bytes(&self) -> usize {
        self.runtime.limits.max_artifact_bytes
    }

    fn restore_backup(
        paths: &LocalPaths,
        limits: PortfolioApplicationLimits,
        portfolios: &[u8],
        transactions: &[u8],
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
        let (authority, image) = ImportAuthority::restore_backup(
            artifacts.clone(),
            control.root(),
            limits,
            portfolios,
            transactions,
        )?;
        Ok(Self::from_restored(artifacts, limits, authority, image))
    }

    /// Returns read-only access to genuine immutable portfolio revisions.
    ///
    /// The capability cannot import, publish, revoke, or reconstruct a revision.
    pub fn fair_value_reader(&self) -> PortfolioFairValueReadCapability {
        PortfolioFairValueReadCapability {
            runtime: Arc::clone(&self.runtime),
        }
    }

    /// Prepares a non-mutating portfolio import from bytes already claimed by native input
    /// staging. The caller must pass the server-derived ticket ID only for audit binding; this
    /// method neither resolves a filesystem path nor accepts a client-provided artifact ID.
    ///
    /// The returned preview is canonical and server-held. Committing it is intentionally exposed
    /// through a separate governed path so interpretation and approval evidence cannot be
    /// smuggled into preview input.
    pub(crate) fn prepare_staged_import(
        &self,
        account_id: AccountId,
        input_ticket_id: String,
        bytes: &[u8],
        context: &RequestContext,
    ) -> Result<PortfolioImportPreview, PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, context)?;
        let mut authority = self
            .runtime
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let preview = authority.prepare_staged_import(
            &self.runtime.artifacts,
            account_id,
            input_ticket_id,
            bytes,
        )?;
        ensure_live(&self.runtime, context)?;
        Ok(preview)
    }

    /// Rebuilds the bounded projection for one exact server-held prepared import.
    pub(crate) fn prepared_import_preview(
        &self,
        preview_id: &str,
        context: &RequestContext,
    ) -> Result<PortfolioImportPreview, PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, context)?;
        let mut authority = self
            .runtime
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let preview = authority.prepared_import_preview(&self.runtime.artifacts, preview_id)?;
        ensure_live(&self.runtime, context)?;
        Ok(preview)
    }

    /// Commits a prepared import using only server-held resolution evidence. The shared native
    /// boundary must consume and validate the governance authorization handle before it can
    /// construct `resolution`; the desktop never supplies its actor/time/rule, selected lots, or
    /// corporate-action plan through this API.
    pub(crate) fn commit_prepared_import(
        &self,
        preview_id: &str,
        interpretations: &[PortfolioImportInterpretation],
        resolution: &ServerHeldPortfolioImportResolution,
        context: &RequestContext,
    ) -> Result<(), PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, context)?;
        let mut authority = self
            .runtime
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let image = authority.commit_prepared_import(
            &self.runtime.artifacts,
            preview_id,
            interpretations,
            resolution,
        )?;
        ensure_live(&self.runtime, context)?;
        self.runtime.image.store(Arc::new(image));
        Ok(())
    }

    /// Recovers a durable promotion that was interrupted after governance admission but before
    /// portfolio publication. The caller must rehydrate the same server-held resolution evidence
    /// from the governance authority; this method refuses newly supplied client interpretations.
    pub(crate) fn recover_promoting_import(
        &self,
        preview_id: &str,
        resolution: &ServerHeldPortfolioImportResolution,
        context: &RequestContext,
    ) -> Result<(), PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, context)?;
        let mut authority = self
            .runtime
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let image =
            authority.recover_promoting_import(&self.runtime.artifacts, preview_id, resolution)?;
        ensure_live(&self.runtime, context)?;
        self.runtime.image.store(Arc::new(image));
        Ok(())
    }

    /// Resumes one approved import after a process interruption. The exact server-held approval
    /// may be replayed only to finish the same durable transition; a completed publication is
    /// recognized through its immutable governed receipt.
    pub(crate) fn resume_approved_import(
        &self,
        preview_id: &str,
        interpretations: &[PortfolioImportInterpretation],
        resolution: &ServerHeldPortfolioImportResolution,
        context: &RequestContext,
    ) -> Result<(), PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, context)?;
        let mut authority = self
            .runtime
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let image = authority.resume_approved_import(
            &self.runtime.artifacts,
            preview_id,
            interpretations,
            resolution,
        )?;
        ensure_live(&self.runtime, context)?;
        self.runtime.image.store(Arc::new(image));
        Ok(())
    }

    /// Discards an unapproved prepared import. A promotion that has consumed approval cannot be
    /// discarded and must complete through recovery.
    pub(crate) fn discard_prepared_import(
        &self,
        preview_id: &str,
        context: &RequestContext,
    ) -> Result<(), PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, context)?;
        let mut authority = self
            .runtime
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        authority.discard_prepared_import(preview_id)?;
        ensure_live(&self.runtime, context)
    }
}

/// Cloneable least-authority exporter for genuine portfolio revisions.
#[derive(Clone)]
pub struct PortfolioFairValueReadCapability {
    runtime: Arc<Runtime>,
}

impl PortfolioFairValueReadCapability {
    /// Clones the exact current immutable revision for one account.
    ///
    /// The clone retains the producer-issued revision token and complete evidence. Missing
    /// accounts, cancellation, deadlines, and service shutdown fail closed.
    pub fn current_revision(
        &self,
        account_id: AccountId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PortfolioRevision, PortfolioApplicationServiceError> {
        if cancellation.is_cancelled() || self.runtime.cancellation.is_cancelled() {
            return Err(PortfolioApplicationServiceError::Cancelled);
        }
        if !self.runtime.accepting.load(Ordering::Acquire) {
            return Err(PortfolioApplicationServiceError::Authority);
        }
        if Instant::now() >= deadline {
            return Err(PortfolioApplicationServiceError::DeadlineExceeded);
        }
        let image = self.runtime.image.load();
        let revision = image
            .revisions
            .current_revisions()
            .find(|revision| revision.account_id() == account_id)
            .cloned()
            .ok_or(PortfolioApplicationServiceError::NotFound)?;
        if cancellation.is_cancelled() || self.runtime.cancellation.is_cancelled() {
            return Err(PortfolioApplicationServiceError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(PortfolioApplicationServiceError::DeadlineExceeded);
        }
        Ok(revision)
    }
}

impl fmt::Debug for PortfolioFairValueReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioFairValueReadCapability")
            .field("revision_source", &"[IMMUTABLE PORTFOLIO READ IMAGE]")
            .finish()
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
