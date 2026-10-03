//! Application-owned Schwab OAuth consent, callback, and protected-token lifecycle.
//!
//! This boundary owns the sole callback listener and protected OAuth authority for each exact
//! onboarding session. It exposes neither application credentials, authorization URLs, callback
//! codes, nor bearer tokens. Browser presentation and callback TLS custody remain installation
//! capabilities supplied by the native product. Market-runtime draining is also injected so local
//! unlink cannot race a still-callable REST, Streamer, or publication generation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Component, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_schwab::{
    AuthorizationRequest, CallbackOutcome, OAuthLoopbackBounds, OAuthLoopbackError,
    OAuthLoopbackReceiver, OAuthLoopbackTlsAcceptor, ProtectedSchwabOAuthAuthority,
    RequestAdmission, ReqwestSchwabOAuthWire, SchwabAccessTokenSource,
    SchwabApplicationCredentialReplacement, SchwabApplicationCredentialReplacementBinding,
    SchwabOAuthAuthorityError, SchwabOAuthAuthorityReceipt, SchwabOAuthAuthorityStatus,
    SchwabOAuthInteraction, SchwabOAuthWire, SchwabOAuthWireBounds, TokenAuthorityError,
    TransientAccessToken,
};
use market_squawk_domain::Timestamp;
use market_squawk_platform::SecretKey;
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard, Notify, OwnedMutexGuard};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::{Zeroize as _, Zeroizing};

use super::contracts::{
    SchwabOAuthBootstrapLease, SchwabOAuthLifecycleAction, SchwabOAuthLifecycleState,
    SchwabOAuthLifecycleView,
};
use super::service::{
    ProviderOnboardingError, ProviderOnboardingService, SchwabOAuthBootstrapAuthorityFactory,
};

const OAUTH_STATE_BYTES: usize = 32;
static ACTIVE_RUNTIME_ROOTS: LazyLock<StdMutex<BTreeSet<PathBuf>>> =
    LazyLock::new(|| StdMutex::new(BTreeSet::new()));

/// Owned browser-open future returned by the installation capability.
pub(crate) type SchwabOAuthBrowserFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), SchwabOAuthBrowserError>> + Send + 'a>>;

/// Installation capability that consumes a redacted Schwab authorization request.
///
/// Implementations may expose the contained URL only to the operating-system browser. They must
/// not log it, persist it, place it on a command line, or return it to an untrusted renderer. They
/// must honor `cancellation` and must not detach browser-open work beyond the returned future.
pub(crate) trait SchwabOAuthBrowser: fmt::Debug + Send + Sync {
    fn open(
        &self,
        request: AuthorizationRequest,
        cancellation: CancellationToken,
    ) -> SchwabOAuthBrowserFuture<'_>;
}

/// Secret-free browser capability failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SchwabOAuthBrowserError {
    #[error("the Schwab authorization page could not be opened")]
    Unavailable,
    #[error("opening the Schwab authorization page was cancelled")]
    Cancelled,
}

/// Owned market-authority drain future used before local OAuth unlink.
pub(crate) type SchwabOAuthMarketDrainFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), SchwabOAuthMarketDrainError>> + Send + 'a>>;

/// Explicit owner intent; absence of a current token receipt cannot identify process shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchwabOAuthMarketDrainPurpose {
    ProcessShutdown,
    CredentialLock,
    Unlink,
    CredentialReplacement,
}

/// Application capability that drains every callable Schwab market-data generation.
///
/// The implementation must stop REST admission, Streamer reconnect/subscription admission, and
/// publication leases for `session_id`, then join their work before returning. It must not call
/// back into this OAuth runtime while the drain is in progress or detach work beyond the returned
/// future.
pub(crate) trait SchwabOAuthMarketDrain: fmt::Debug + Send + Sync {
    fn drain(
        &self,
        session_id: Uuid,
        current: Option<SchwabOAuthAuthorityReceipt>,
        purpose: SchwabOAuthMarketDrainPurpose,
        cancellation: CancellationToken,
    ) -> SchwabOAuthMarketDrainFuture<'_>;
}

/// Secret-free market drain failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("the callable Schwab market-data runtime could not be drained")]
pub(crate) struct SchwabOAuthMarketDrainError;

/// Finite application controls for the Schwab OAuth runtime.
pub(crate) struct SchwabOAuthRuntimeConfiguration {
    state_root: PathBuf,
    wire_bounds: SchwabOAuthWireBounds,
    callback_bounds: OAuthLoopbackBounds,
    authorization_admission: RequestAdmission,
}

impl SchwabOAuthRuntimeConfiguration {
    pub(crate) fn try_new(
        state_root: PathBuf,
        wire_bounds: SchwabOAuthWireBounds,
        callback_bounds: OAuthLoopbackBounds,
        maximum_authorization_request_bytes: NonZeroUsize,
    ) -> Result<Self, SchwabOAuthRuntimeError> {
        if !state_root.is_absolute()
            || state_root
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(SchwabOAuthRuntimeError::InvalidConfiguration);
        }
        Ok(Self {
            state_root,
            wire_bounds,
            callback_bounds,
            authorization_admission: RequestAdmission::new(
                maximum_authorization_request_bytes,
                NonZeroUsize::MIN,
            ),
        })
    }
}

impl fmt::Debug for SchwabOAuthRuntimeConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthRuntimeConfiguration")
            .field("state_root", &"[APPLICATION-CONFINED]")
            .field("wire_bounds", &self.wire_bounds)
            .field("callback_bounds", &self.callback_bounds)
            .field("authorization_admission", &self.authorization_admission)
            .finish()
    }
}

/// Process-local single-writer reservation for the application-confined OAuth state root.
struct SchwabOAuthRuntimeRootLease {
    root: PathBuf,
}

impl SchwabOAuthRuntimeRootLease {
    fn try_claim(root: PathBuf) -> Result<Self, SchwabOAuthRuntimeError> {
        let mut active = ACTIVE_RUNTIME_ROOTS
            .lock()
            .map_err(|_poisoned| SchwabOAuthRuntimeError::RuntimeRootUnavailable)?;
        if !active.insert(root.clone()) {
            return Err(SchwabOAuthRuntimeError::RuntimeRootAlreadyOwned);
        }
        Ok(Self { root })
    }
}

impl Drop for SchwabOAuthRuntimeRootLease {
    fn drop(&mut self) {
        let mut active = match ACTIVE_RUNTIME_ROOTS.lock() {
            Ok(active) => active,
            Err(poisoned) => poisoned.into_inner(),
        };
        active.remove(&self.root);
    }
}

impl fmt::Debug for SchwabOAuthRuntimeRootLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SchwabOAuthRuntimeRootLease([APPLICATION-CONFINED])")
    }
}

/// Sole application owner for Schwab OAuth consent and protected token authority.
pub(crate) struct SchwabOAuthRuntime {
    onboarding: Arc<ProviderOnboardingService>,
    configuration: SchwabOAuthRuntimeConfiguration,
    wire: Arc<dyn SchwabOAuthWire>,
    tls: Arc<dyn OAuthLoopbackTlsAcceptor>,
    browser: Arc<dyn SchwabOAuthBrowser>,
    market_drain: Arc<dyn SchwabOAuthMarketDrain>,
    sessions: Mutex<BTreeMap<Uuid, SchwabOAuthSession>>,
    credential_replacement: Mutex<Option<PendingCredentialReplacement>>,
    accepting: AtomicBool,
    in_flight: std::sync::atomic::AtomicUsize,
    in_flight_changed: Notify,
    shutdown: CancellationToken,
    _root_lease: SchwabOAuthRuntimeRootLease,
}

impl SchwabOAuthRuntime {
    /// Constructs the sole production OAuth owner with the hardened Schwab token wire.
    pub(crate) fn try_new(
        onboarding: Arc<ProviderOnboardingService>,
        configuration: SchwabOAuthRuntimeConfiguration,
        tls: Arc<dyn OAuthLoopbackTlsAcceptor>,
        browser: Arc<dyn SchwabOAuthBrowser>,
        market_drain: Arc<dyn SchwabOAuthMarketDrain>,
    ) -> Result<Self, SchwabOAuthRuntimeError> {
        let root_lease = SchwabOAuthRuntimeRootLease::try_claim(configuration.state_root.clone())?;
        let wire = Arc::new(ReqwestSchwabOAuthWire::try_new(configuration.wire_bounds)?)
            as Arc<dyn SchwabOAuthWire>;
        Ok(Self {
            onboarding,
            configuration,
            wire,
            tls,
            browser,
            market_drain,
            sessions: Mutex::new(BTreeMap::new()),
            credential_replacement: Mutex::new(None),
            accepting: AtomicBool::new(true),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            in_flight_changed: Notify::new(),
            shutdown: CancellationToken::new(),
            _root_lease: root_lease,
        })
    }

    /// Restores one exact session from its durable protected OAuth authority state.
    ///
    /// Pending browser consent is intentionally not recoverable after a process restart. The
    /// protected authority recovers only a durable active or reauthorization-required state; an
    /// incomplete user interaction must begin again with fresh correlation state.
    pub(crate) async fn restore(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
        let _operation = self.admit_operation()?;
        let mut sessions = self.lock_sessions(&cancellation).await?;
        let session = self
            .ensure_session(&mut sessions, session_id, cancellation)
            .await?;
        let status = session.status().await?;
        lifecycle_view(session_id, SchwabOAuthLifecycleAction::Continue, status)
    }

    /// Applies one exact local OAuth lifecycle action.
    pub(crate) async fn apply(
        &self,
        session_id: Uuid,
        action: SchwabOAuthLifecycleAction,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
        let _operation = self.admit_operation()?;
        match action {
            SchwabOAuthLifecycleAction::Begin => self.begin(session_id, cancellation).await,
            SchwabOAuthLifecycleAction::Continue => {
                self.continue_pending(session_id, cancellation).await
            }
            SchwabOAuthLifecycleAction::Cancel => self.cancel(session_id, cancellation).await,
            SchwabOAuthLifecycleAction::Unlink => self.unlink(session_id, cancellation).await,
        }
    }

    /// Issues a bounded market-data token authority only from an active exact OAuth session.
    pub(crate) async fn market_authority(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthMarketAuthority, SchwabOAuthRuntimeError> {
        let _operation = self.admit_operation()?;
        let mut sessions = self.lock_sessions(&cancellation).await?;
        let session = self
            .ensure_session(&mut sessions, session_id, cancellation)
            .await?;
        if session.exchange.is_some() {
            return Err(SchwabOAuthRuntimeError::AuthorizationExchangeInFlight);
        }
        let authority = &session.authority;
        let status = session.status().await?;
        let SchwabOAuthAuthorityStatus::Active(receipt) = status else {
            return Err(SchwabOAuthRuntimeError::ReauthorizationRequired);
        };
        Ok(SchwabOAuthMarketAuthority {
            session_id,
            issued_receipt: receipt,
            authority: Arc::clone(authority),
            currentness: Arc::clone(&session.market_epoch),
        })
    }

    /// Stops new lifecycle admission and cancels every pending callback receiver.
    pub(crate) fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.shutdown.cancel();
    }

    /// Closes credential admission without terminating the installation's reusable OAuth owner.
    pub(crate) fn begin_credential_suspension(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// Releases protected in-memory state while preserving the saved OAuth session on disk.
    pub(crate) async fn suspend_credentials(
        &self,
        deadline: Instant,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        self.begin_credential_suspension();
        self.finish_drain(deadline, SchwabOAuthMarketDrainPurpose::CredentialLock)
            .await?;
        let mut sessions =
            tokio::time::timeout_at(TokioInstant::from_std(deadline), self.sessions.lock())
                .await
                .map_err(|_| SchwabOAuthRuntimeError::ShutdownStillDraining)?;
        if sessions
            .values()
            .any(|session| Arc::strong_count(&session.authority) != 1)
        {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRetained);
        }
        sessions.clear();
        Ok(())
    }

    /// Subsequent exact-session reads reopen the unchanged saved protected authority lazily.
    pub(crate) async fn resume_credentials(&self) -> Result<(), SchwabOAuthRuntimeError> {
        if self.shutdown.is_cancelled()
            || self.onboarding.credential_access_status()?.access
                != market_squawk_platform::SecretAccessState::Ready
        {
            return Err(SchwabOAuthRuntimeError::ShuttingDown);
        }
        let sessions = self.sessions.lock().await;
        if !self.accepting.load(Ordering::Acquire) && !sessions.is_empty() {
            return Err(SchwabOAuthRuntimeError::ShutdownStillDraining);
        }
        self.accepting.store(true, Ordering::Release);
        self.require_admission()
    }

    /// Drains all market generations and joins callback receivers by `deadline`.
    ///
    /// Only receiver-only work may be aborted at expiry. A token exchange or credential
    /// transition is an admitted lifecycle operation and must finish under sole runtime custody;
    /// if it is still running at the deadline, the caller receives `ShutdownStillDraining` and
    /// must retain this runtime and retry shutdown.
    pub(crate) async fn finish_shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        self.begin_shutdown();
        self.finish_drain(deadline, SchwabOAuthMarketDrainPurpose::ProcessShutdown)
            .await
    }

    async fn finish_drain(
        &self,
        deadline: Instant,
        purpose: SchwabOAuthMarketDrainPurpose,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        let deadline = TokioInstant::from_std(deadline);
        self.wait_for_operations(deadline).await?;
        let mut sessions = tokio::time::timeout_at(deadline, self.sessions.lock())
            .await
            .map_err(|_| SchwabOAuthRuntimeError::ShutdownDeadline)?;
        if let Err(error) = self
            .finish_credential_replacement(&mut sessions, Some(deadline))
            .await
        {
            // A joined replacement restores the exact resulting authority before reporting its
            // operation error. Credential locking only needs that custody back, including when
            // the already sealed vault caused the admitted replacement to fail.
            let replacement_drained = self
                .credential_replacement
                .try_lock()
                .is_ok_and(|retained| retained.is_none());
            if purpose != SchwabOAuthMarketDrainPurpose::CredentialLock || !replacement_drained {
                return Err(error);
            }
        }
        let mut first_error = None;
        let mut market_sessions = Vec::new();
        // Exchange and receiver tasks do not acquire the session-map lock.
        for (session_id, session) in sessions.iter_mut() {
            session.market_epoch.cancel();
            market_sessions.push(*session_id);
            if let Some(exchange) = session.exchange.as_mut() {
                match tokio::time::timeout_at(deadline, exchange.join()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        if purpose != SchwabOAuthMarketDrainPurpose::CredentialLock {
                            first_error.get_or_insert(error);
                        }
                    }
                    Err(_) => return Err(SchwabOAuthRuntimeError::ShutdownStillDraining),
                }
            }
            if let Some(pending) = session.pending.as_mut() {
                pending.cancellation.cancel();
                match tokio::time::timeout_at(deadline, pending.join()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        if purpose != SchwabOAuthMarketDrainPurpose::CredentialLock {
                            first_error.get_or_insert(error);
                        }
                    }
                    Err(_) => {
                        // Receiver-only abort is allowed, but the handle stays owned until joined.
                        pending.aborted = true;
                        pending.task.abort();
                        return Err(SchwabOAuthRuntimeError::ShutdownStillDraining);
                    }
                }
            }
        }
        drop(sessions);
        for session_id in market_sessions {
            let drain_cancellation = CancellationToken::new();
            let drain =
                self.market_drain
                    .drain(session_id, None, purpose, drain_cancellation.clone());
            match tokio::time::timeout_at(deadline, drain).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error.into());
                }
                Err(_) => {
                    drain_cancellation.cancel();
                    return Err(SchwabOAuthRuntimeError::ShutdownStillDraining);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn begin(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
        let mut sessions = self.lock_sessions(&cancellation).await?;
        if let Some((pending_session, _session)) = sessions
            .iter()
            .find(|(_session_id, session)| session.pending.is_some())
        {
            if *pending_session == session_id {
                return Ok(SchwabOAuthLifecycleView::new(
                    session_id,
                    SchwabOAuthLifecycleAction::Begin,
                    SchwabOAuthLifecycleState::AwaitingAuthorization,
                    None,
                    None,
                    None,
                ));
            }
            return Err(SchwabOAuthRuntimeError::CallbackAlreadyOwned);
        }

        let session = self
            .ensure_session(&mut sessions, session_id, cancellation.clone())
            .await?;
        if session.exchange.is_some() {
            return Ok(exchanging_view(
                session_id,
                SchwabOAuthLifecycleAction::Begin,
            ));
        }
        let status = session.status().await?;
        if let SchwabOAuthAuthorityStatus::Active(receipt) = status {
            return active_view(session_id, SchwabOAuthLifecycleAction::Begin, receipt);
        }

        let state = oauth_state()?;
        let request = session
            .authority
            .authorization_request(
                &state,
                self.configuration.authorization_admission,
                SchwabOAuthInteraction::Foreground,
            )
            .await?;
        let receiver = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SchwabOAuthRuntimeError::Cancelled),
            () = self.shutdown.cancelled() => return Err(SchwabOAuthRuntimeError::ShuttingDown),
            receiver = OAuthLoopbackReceiver::bind(
                Arc::clone(&self.tls),
                self.configuration.callback_bounds,
            ) => receiver?,
        };
        let callback_cancellation = self.shutdown.child_token();
        let task_cancellation = callback_cancellation.clone();
        let callback_task =
            tokio::spawn(async move { receiver.receive(state.as_str(), task_cancellation).await });
        session.pending = Some(PendingAuthorization {
            cancellation: callback_cancellation,
            task: callback_task,
            joined: None,
            aborted: false,
        });

        let browser_cancellation = self.shutdown.child_token();
        let opened = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(SchwabOAuthBrowserError::Cancelled),
            () = self.shutdown.cancelled() => Err(SchwabOAuthBrowserError::Cancelled),
            opened = self.browser.open(request, browser_cancellation.clone()) => opened,
        };
        browser_cancellation.cancel();
        if let Err(error) = opened {
            let pending = session
                .pending
                .as_mut()
                .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
            pending.cancellation.cancel();
            pending.join().await?;
            session.pending.take();
            return Err(error.into());
        }
        Ok(SchwabOAuthLifecycleView::new(
            session_id,
            SchwabOAuthLifecycleAction::Begin,
            SchwabOAuthLifecycleState::AwaitingAuthorization,
            None,
            None,
            None,
        ))
    }

    async fn continue_pending(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
        let mut sessions = self.lock_sessions(&cancellation).await?;
        let session = self
            .ensure_session(&mut sessions, session_id, cancellation.clone())
            .await?;
        if session
            .exchange
            .as_ref()
            .is_some_and(|exchange| !exchange.task.is_finished())
        {
            return Ok(exchanging_view(
                session_id,
                SchwabOAuthLifecycleAction::Continue,
            ));
        }
        if let Some(exchange) = session.exchange.as_mut() {
            exchange.join().await?;
            let mut exchange = session
                .exchange
                .take()
                .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
            let receipt = exchange
                .joined
                .take()
                .ok_or(SchwabOAuthRuntimeError::InvalidState)?
                .map_err(|_| SchwabOAuthRuntimeError::ExchangeTask)??;
            session
                .market_epoch
                .observe_status(SchwabOAuthAuthorityStatus::Active(receipt))?;
            return active_view(session_id, SchwabOAuthLifecycleAction::Continue, receipt);
        }
        let Some(pending) = session.pending.as_ref() else {
            let status = session.status().await?;
            return lifecycle_view(session_id, SchwabOAuthLifecycleAction::Continue, status);
        };
        if !pending.task.is_finished() {
            return Ok(SchwabOAuthLifecycleView::new(
                session_id,
                SchwabOAuthLifecycleAction::Continue,
                SchwabOAuthLifecycleState::AwaitingAuthorization,
                None,
                None,
                None,
            ));
        }
        session
            .pending
            .as_mut()
            .ok_or(SchwabOAuthRuntimeError::InvalidState)?
            .join()
            .await?;
        let mut pending = session
            .pending
            .take()
            .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
        let outcome = pending
            .joined
            .take()
            .ok_or(SchwabOAuthRuntimeError::InvalidState)?
            .map_err(|_| SchwabOAuthRuntimeError::CallbackTask)??;
        match outcome {
            CallbackOutcome::Authorized(callback) => {
                if cancellation.is_cancelled() || self.shutdown.is_cancelled() {
                    return Err(SchwabOAuthRuntimeError::Cancelled);
                }
                session.market_epoch.invalidate();
                // The provider exchange may legitimately outlive the portal's request timeout.
                // Supervise it under the application runtime so dropping the HTTP request cannot
                // discard the validated one-time callback or detach the protected transition.
                let authority = Arc::clone(&session.authority);
                let issued_at_unix_seconds = unix_seconds()?;
                session.exchange = Some(PendingTokenExchange {
                    joined: None,
                    task: tokio::spawn(async move {
                        authority
                            .complete_authorization(
                                &callback,
                                issued_at_unix_seconds,
                                SchwabOAuthInteraction::Foreground,
                            )
                            .await
                    }),
                });
                Ok(exchanging_view(
                    session_id,
                    SchwabOAuthLifecycleAction::Continue,
                ))
            }
            CallbackOutcome::Denied { .. } => Ok(SchwabOAuthLifecycleView::new(
                session_id,
                SchwabOAuthLifecycleAction::Continue,
                SchwabOAuthLifecycleState::Cancelled,
                None,
                None,
                None,
            )),
        }
    }

    async fn cancel(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
        let mut sessions = self.lock_sessions(&cancellation).await?;
        let session = self
            .ensure_session(&mut sessions, session_id, cancellation)
            .await?;
        if session.exchange.is_some() {
            return Ok(exchanging_view(
                session_id,
                SchwabOAuthLifecycleAction::Cancel,
            ));
        }
        if let Some(pending) = session.pending.as_mut() {
            pending.cancellation.cancel();
            pending.join().await?;
            session.pending.take();
            return Ok(SchwabOAuthLifecycleView::new(
                session_id,
                SchwabOAuthLifecycleAction::Cancel,
                SchwabOAuthLifecycleState::Cancelled,
                None,
                None,
                None,
            ));
        }
        let status = session.status().await?;
        if let SchwabOAuthAuthorityStatus::Active(receipt) = status {
            return active_view(session_id, SchwabOAuthLifecycleAction::Cancel, receipt);
        }
        Ok(SchwabOAuthLifecycleView::new(
            session_id,
            SchwabOAuthLifecycleAction::Cancel,
            SchwabOAuthLifecycleState::Cancelled,
            None,
            None,
            None,
        ))
    }

    async fn unlink(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
        let mut sessions = self.lock_sessions(&cancellation).await?;
        let session = self
            .ensure_session(&mut sessions, session_id, cancellation.clone())
            .await?;
        if session
            .exchange
            .as_ref()
            .is_some_and(|exchange| !exchange.task.is_finished())
        {
            return Ok(exchanging_view(
                session_id,
                SchwabOAuthLifecycleAction::Unlink,
            ));
        }
        if let Some(exchange) = session.exchange.as_mut() {
            exchange.join().await?;
        }
        if let Some(pending) = session.pending.as_mut() {
            pending.cancellation.cancel();
            pending.join().await?;
            session.pending.take();
        }
        let status = session.status().await?;
        let receipt = match status {
            SchwabOAuthAuthorityStatus::Active(receipt) => Some(receipt),
            SchwabOAuthAuthorityStatus::AwaitingAuthorization
            | SchwabOAuthAuthorityStatus::ReauthorizationRequired => None,
        };
        session.market_epoch.cancel();
        self.market_drain
            .drain(
                session_id,
                receipt,
                SchwabOAuthMarketDrainPurpose::Unlink,
                cancellation,
            )
            .await?;
        session
            .authority
            .revoke(SchwabOAuthInteraction::Foreground)
            .await?;
        sessions
            .remove(&session_id)
            .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
        Ok(SchwabOAuthLifecycleView::new(
            session_id,
            SchwabOAuthLifecycleAction::Unlink,
            SchwabOAuthLifecycleState::Unlinked,
            None,
            None,
            None,
        ))
    }

    async fn ensure_session<'a>(
        &self,
        sessions: &'a mut BTreeMap<Uuid, SchwabOAuthSession>,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<&'a mut SchwabOAuthSession, SchwabOAuthRuntimeError> {
        self.finish_credential_replacement(sessions, None).await?;
        if session_id.is_nil() {
            return Err(SchwabOAuthRuntimeError::InvalidSession);
        }
        if !sessions.is_empty() && !sessions.contains_key(&session_id) {
            return Err(SchwabOAuthRuntimeError::DifferentSessionOwned);
        }
        let lease = self
            .onboarding
            .prepare_schwab_oauth_bootstrap(session_id, cancellation.clone())
            .await?;
        let credential_changed = sessions
            .get(&session_id)
            .is_some_and(|session| !same_credential_authority(&session.bootstrap, &lease));
        if credential_changed {
            self.replace_session_credential(sessions, session_id, lease, cancellation)
                .await?;
            return sessions
                .get_mut(&session_id)
                .ok_or(SchwabOAuthRuntimeError::InvalidState);
        }
        if sessions.contains_key(&session_id) {
            let session = sessions
                .get_mut(&session_id)
                .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
            session.bootstrap = lease;
            return Ok(session);
        }

        let factory = self.onboarding.schwab_oauth_authority_factory(&lease)?;
        let authority = Arc::new(self.open_authority(session_id, &factory).await?);
        sessions.insert(
            session_id,
            SchwabOAuthSession {
                bootstrap: lease,
                authority,
                pending: None,
                exchange: None,
                market_epoch: Arc::new(SchwabOAuthMarketEpochAuthority::new(
                    session_id,
                    self.shutdown.child_token(),
                )),
            },
        );
        sessions
            .get_mut(&session_id)
            .ok_or(SchwabOAuthRuntimeError::InvalidState)
    }

    async fn replace_session_credential(
        &self,
        sessions: &mut BTreeMap<Uuid, SchwabOAuthSession>,
        session_id: Uuid,
        replacement_lease: SchwabOAuthBootstrapLease,
        cancellation: CancellationToken,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        let current = sessions
            .get(&session_id)
            .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
        if current
            .exchange
            .as_ref()
            .is_some_and(|exchange| !exchange.task.is_finished())
        {
            return Err(SchwabOAuthRuntimeError::AuthorizationExchangeInFlight);
        }
        let application_credential_series = SecretKey::try_new(
            "provider-onboarding",
            &format!(
                "{}.{}",
                current.bootstrap.surface_id().as_str(),
                current.bootstrap.session_id().simple()
            ),
        )
        .map_err(|_| SchwabOAuthRuntimeError::InvalidConfiguration)?;
        let replacement = SchwabApplicationCredentialReplacement::try_new(
            application_credential_series,
            current.bootstrap.application_secret_reference().clone(),
            replacement_lease.application_secret_reference().clone(),
        )?;
        let current = sessions
            .get_mut(&session_id)
            .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
        if let Some(exchange) = current.exchange.as_mut() {
            exchange.join().await?;
        }
        if let Some(pending) = current.pending.as_mut() {
            pending.cancellation.cancel();
            pending.join().await?;
        }
        let status = current.status().await?;
        let receipt = match status {
            SchwabOAuthAuthorityStatus::Active(receipt) => Some(receipt),
            SchwabOAuthAuthorityStatus::AwaitingAuthorization
            | SchwabOAuthAuthorityStatus::ReauthorizationRequired => None,
        };
        current.market_epoch.cancel();
        self.market_drain
            .drain(
                session_id,
                receipt,
                SchwabOAuthMarketDrainPurpose::CredentialReplacement,
                cancellation,
            )
            .await?;
        let mut retained = self.credential_replacement.lock().await;
        if retained.is_some() {
            return Err(SchwabOAuthRuntimeError::InvalidState);
        }
        let current = sessions
            .remove(&session_id)
            .ok_or(SchwabOAuthRuntimeError::InvalidState)?;
        let lifecycle = self.shutdown.clone();
        // Removal, spawning, and retaining the actual task have no intervening await.
        *retained = Some(PendingCredentialReplacement {
            task: tokio::spawn(async move {
                let mut current = current;
                let authority = match Arc::try_unwrap(current.authority) {
                    Ok(authority) => authority,
                    Err(authority) => {
                        current.authority = authority;
                        return (
                            session_id,
                            current,
                            Err(SchwabOAuthRuntimeError::MarketAuthorityRetained),
                        );
                    }
                };
                let outcome = authority
                    .replace_application_credential(replacement, SchwabOAuthInteraction::Foreground)
                    .await;
                let result = match outcome {
                    Ok(authority) => {
                        current.authority = Arc::new(authority);
                        current.bootstrap = replacement_lease;
                        current.pending = None;
                        current.exchange = None;
                        Ok(())
                    }
                    Err(failure) => {
                        let (authority, binding, error) = failure.into_parts();
                        current.authority = Arc::new(authority);
                        if binding == SchwabApplicationCredentialReplacementBinding::Replacement {
                            current.bootstrap = replacement_lease;
                        }
                        Err(error.into())
                    }
                };
                current.market_epoch = Arc::new(SchwabOAuthMarketEpochAuthority::new(
                    session_id,
                    lifecycle.child_token(),
                ));
                (session_id, current, result)
            }),
            failed: false,
        });
        drop(retained);
        self.finish_credential_replacement(sessions, None).await
    }

    async fn finish_credential_replacement(
        &self,
        sessions: &mut BTreeMap<Uuid, SchwabOAuthSession>,
        deadline: Option<TokioInstant>,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        let mut retained = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, self.credential_replacement.lock())
                .await
                .map_err(|_| SchwabOAuthRuntimeError::ShutdownStillDraining)?
        } else {
            self.credential_replacement.lock().await
        };
        let Some(pending) = retained.as_mut() else {
            return Ok(());
        };
        if pending.failed {
            return Err(SchwabOAuthRuntimeError::ExchangeTask);
        }
        let joined = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, &mut pending.task)
                .await
                .map_err(|_| SchwabOAuthRuntimeError::ShutdownStillDraining)?
        } else {
            (&mut pending.task).await
        };
        match joined {
            Ok((session_id, session, result)) => {
                // The actual returned authority is restored before the operation result escapes.
                sessions.insert(session_id, session);
                *retained = None;
                result
            }
            Err(_) => {
                pending.failed = true;
                Err(SchwabOAuthRuntimeError::ExchangeTask)
            }
        }
    }

    async fn open_authority(
        &self,
        session_id: Uuid,
        factory: &SchwabOAuthBootstrapAuthorityFactory,
    ) -> Result<ProtectedSchwabOAuthAuthority, SchwabOAuthRuntimeError> {
        let configuration = factory.configuration(Arc::clone(&self.wire))?;
        Ok(ProtectedSchwabOAuthAuthority::try_open(
            self.configuration
                .state_root
                .join(session_id.simple().to_string()),
            configuration,
        )
        .await?)
    }

    async fn lock_sessions(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<MutexGuard<'_, BTreeMap<Uuid, SchwabOAuthSession>>, SchwabOAuthRuntimeError> {
        self.require_admission()?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(SchwabOAuthRuntimeError::Cancelled),
            () = self.shutdown.cancelled() => Err(SchwabOAuthRuntimeError::ShuttingDown),
            sessions = self.sessions.lock() => {
                self.require_admission()?;
                Ok(sessions)
            }
        }
    }

    fn require_admission(&self) -> Result<(), SchwabOAuthRuntimeError> {
        if self.accepting.load(Ordering::Acquire)
            && !self.shutdown.is_cancelled()
            && self.onboarding.credential_access_status()?.access
                == market_squawk_platform::SecretAccessState::Ready
        {
            Ok(())
        } else {
            Err(SchwabOAuthRuntimeError::ShuttingDown)
        }
    }

    fn admit_operation(&self) -> Result<SchwabOAuthOperationGuard<'_>, SchwabOAuthRuntimeError> {
        self.require_admission()?;
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_current| SchwabOAuthRuntimeError::OperationCapacity)?;
        if !self.accepting.load(Ordering::Acquire) {
            self.finish_operation();
            return Err(SchwabOAuthRuntimeError::ShuttingDown);
        }
        Ok(SchwabOAuthOperationGuard { runtime: self })
    }

    fn finish_operation(&self) {
        if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.in_flight_changed.notify_waiters();
        }
    }

    async fn wait_for_operations(
        &self,
        deadline: TokioInstant,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        loop {
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            let changed = self.in_flight_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_elapsed| SchwabOAuthRuntimeError::ShutdownStillDraining)?;
        }
    }
}

struct SchwabOAuthOperationGuard<'a> {
    runtime: &'a SchwabOAuthRuntime,
}

impl Drop for SchwabOAuthOperationGuard<'_> {
    fn drop(&mut self) {
        self.runtime.finish_operation();
    }
}

impl fmt::Debug for SchwabOAuthRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthRuntime")
            .field("configuration", &self.configuration)
            .field("wire", &"[SCHWAB TOKEN ENDPOINT]")
            .field("tls", &"[INSTALLATION CALLBACK IDENTITY]")
            .field("browser", &"[INSTALLATION BROWSER]")
            .field("market_drain", &"[MARKET REVOCATION AUTHORITY]")
            .field("accepting", &self.accepting.load(Ordering::Acquire))
            .field("in_flight", &self.in_flight.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Drop for SchwabOAuthRuntime {
    fn drop(&mut self) {
        self.accepting.store(false, Ordering::Release);
        self.shutdown.cancel();
    }
}

#[derive(Debug)]
struct CurrentSchwabOAuthGeneration {
    receipt: SchwabOAuthAuthorityReceipt,
    currentness: CancellationToken,
}

/// Process-local, secret-free currentness authority for one OAuth market session.
///
/// The protected OAuth store is intentionally absent from this synchronous boundary. Async token
/// acquisition and status reconciliation update this state before a publication capability can be
/// minted; durable precommit performs only nonblocking in-memory checks.
struct SchwabOAuthMarketEpochAuthority {
    session_id: Uuid,
    lifecycle: CancellationToken,
    current: StdMutex<Option<CurrentSchwabOAuthGeneration>>,
    attempt_barrier: Arc<Mutex<()>>,
}

impl SchwabOAuthMarketEpochAuthority {
    fn new(session_id: Uuid, shutdown: CancellationToken) -> Self {
        Self {
            session_id,
            lifecycle: shutdown.child_token(),
            current: StdMutex::new(None),
            attempt_barrier: Arc::new(Mutex::new(())),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.lifecycle.is_cancelled()
    }

    fn cancel(&self) {
        self.lifecycle.cancel();
        self.invalidate();
    }

    fn invalidate(&self) {
        let mut current = match self.current.lock() {
            Ok(current) => current,
            Err(poisoned) => {
                self.lifecycle.cancel();
                poisoned.into_inner()
            }
        };
        if let Some(current) = current.take() {
            current.currentness.cancel();
        }
    }

    /// Observes a lifecycle status without minting publication authority.
    fn observe_status(
        &self,
        status: SchwabOAuthAuthorityStatus,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        if self.is_cancelled() {
            self.invalidate();
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        let mut current = self.current.lock().map_err(|_poisoned| {
            self.lifecycle.cancel();
            SchwabOAuthRuntimeError::MarketEpochUnavailable
        })?;
        match status {
            SchwabOAuthAuthorityStatus::Active(receipt) => {
                if current
                    .as_ref()
                    .is_some_and(|current| current.receipt != receipt)
                    && let Some(stale) = current.take()
                {
                    stale.currentness.cancel();
                }
            }
            SchwabOAuthAuthorityStatus::AwaitingAuthorization
            | SchwabOAuthAuthorityStatus::ReauthorizationRequired => {
                if let Some(stale) = current.take() {
                    stale.currentness.cancel();
                }
            }
        }
        Ok(())
    }

    /// Reconciles an async protected-authority status into one exact local publication epoch.
    fn reconcile_current(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<CancellationToken, SchwabOAuthRuntimeError> {
        validate_receipt_time(receipt)?;
        if self.is_cancelled() {
            self.invalidate();
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        let mut current = self.current.lock().map_err(|_poisoned| {
            self.lifecycle.cancel();
            SchwabOAuthRuntimeError::MarketEpochUnavailable
        })?;
        if let Some(current) = current.as_ref()
            && current.receipt == receipt
        {
            return Ok(current.currentness.clone());
        }
        if let Some(stale) = current.take() {
            stale.currentness.cancel();
        }
        let currentness = self.lifecycle.child_token();
        *current = Some(CurrentSchwabOAuthGeneration {
            receipt,
            currentness: currentness.clone(),
        });
        drop(current);
        if self.is_cancelled() {
            self.invalidate();
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        Ok(currentness)
    }

    async fn acquire_attempt_barrier(
        self: &Arc<Self>,
    ) -> Result<OwnedMutexGuard<()>, SchwabOAuthRuntimeError> {
        if self.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        let barrier = Arc::clone(&self.attempt_barrier).lock_owned().await;
        if self.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        Ok(barrier)
    }

    fn validate_current_receipt(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        validate_receipt_time(receipt)?;
        if self.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        let current = self.current.lock().map_err(|_poisoned| {
            self.lifecycle.cancel();
            SchwabOAuthRuntimeError::MarketEpochUnavailable
        })?;
        if current
            .as_ref()
            .is_none_or(|current| current.receipt != receipt || current.currentness.is_cancelled())
        {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        drop(current);
        if self.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        Ok(())
    }
}

impl fmt::Debug for SchwabOAuthMarketEpochAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthMarketEpochAuthority")
            .field("session_id", &self.session_id)
            .field("revoked", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

struct SchwabOAuthSession {
    bootstrap: SchwabOAuthBootstrapLease,
    authority: Arc<ProtectedSchwabOAuthAuthority>,
    pending: Option<PendingAuthorization>,
    exchange: Option<PendingTokenExchange>,
    market_epoch: Arc<SchwabOAuthMarketEpochAuthority>,
}

impl SchwabOAuthSession {
    async fn status(&self) -> Result<SchwabOAuthAuthorityStatus, SchwabOAuthRuntimeError> {
        let status = match self.authority.status().await {
            Ok(status) => status,
            Err(error) => {
                self.market_epoch.invalidate();
                return Err(error.into());
            }
        };
        self.market_epoch.observe_status(status)?;
        Ok(status)
    }
}

impl fmt::Debug for SchwabOAuthSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthSession")
            .field("bootstrap", &self.bootstrap)
            .field("authority", &"[PROTECTED OAUTH AUTHORITY]")
            .field("pending", &self.pending.is_some())
            .field("exchange", &self.exchange.is_some())
            .field("market_epoch_revoked", &self.market_epoch.is_cancelled())
            .finish()
    }
}

struct PendingCredentialReplacement {
    task: JoinHandle<(
        Uuid,
        SchwabOAuthSession,
        Result<(), SchwabOAuthRuntimeError>,
    )>,
    failed: bool,
}

struct PendingAuthorization {
    cancellation: CancellationToken,
    task: JoinHandle<Result<CallbackOutcome, OAuthLoopbackError>>,
    joined: Option<Result<Result<CallbackOutcome, OAuthLoopbackError>, tokio::task::JoinError>>,
    aborted: bool,
}

struct PendingTokenExchange {
    task: JoinHandle<Result<SchwabOAuthAuthorityReceipt, SchwabOAuthAuthorityError>>,
    joined: Option<
        Result<
            Result<SchwabOAuthAuthorityReceipt, SchwabOAuthAuthorityError>,
            tokio::task::JoinError,
        >,
    >,
}

impl PendingAuthorization {
    async fn join(&mut self) -> Result<(), SchwabOAuthRuntimeError> {
        if self.joined.is_none() {
            self.joined = Some((&mut self.task).await);
        }
        match self.joined.as_ref() {
            Some(Ok(_)) => Ok(()),
            Some(Err(error)) if self.aborted && error.is_cancelled() => Ok(()),
            _ => Err(SchwabOAuthRuntimeError::CallbackTask),
        }
    }
}

impl PendingTokenExchange {
    async fn join(&mut self) -> Result<(), SchwabOAuthRuntimeError> {
        if self.joined.is_none() {
            self.joined = Some((&mut self.task).await);
        }
        match self.joined.as_ref() {
            Some(Ok(Ok(_))) => Ok(()),
            _ => Err(SchwabOAuthRuntimeError::ExchangeTask),
        }
    }
}

impl fmt::Debug for PendingTokenExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingTokenExchange")
            .field("finished", &self.task.is_finished())
            .finish()
    }
}

impl fmt::Debug for PendingAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAuthorization")
            .field("state", &"[REDACTED]")
            .field("finished", &self.task.is_finished())
            .finish()
    }
}

/// Restricted market-data token authority bound to one active OAuth receipt at issuance.
///
/// Clones share the same protected authority and its internal serialization gate; they do not
/// create another token writer. Consumers must re-read `current_receipt` before binding a new
/// doctor, REST, Streamer, or publication generation because token acquisition may rotate it.
#[derive(Clone)]
pub(crate) struct SchwabOAuthMarketAuthority {
    session_id: Uuid,
    issued_receipt: SchwabOAuthAuthorityReceipt,
    authority: Arc<ProtectedSchwabOAuthAuthority>,
    currentness: Arc<SchwabOAuthMarketEpochAuthority>,
}

impl SchwabOAuthMarketAuthority {
    #[cfg(test)]
    pub(crate) fn from_test_authority(
        session_id: Uuid,
        issued_receipt: SchwabOAuthAuthorityReceipt,
        authority: Arc<ProtectedSchwabOAuthAuthority>,
    ) -> Self {
        Self {
            session_id,
            issued_receipt,
            authority,
            currentness: Arc::new(SchwabOAuthMarketEpochAuthority::new(
                session_id,
                CancellationToken::new(),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn revoke_test_authority(&self) {
        self.currentness.cancel();
    }

    pub(crate) const fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub(crate) const fn issued_receipt(&self) -> SchwabOAuthAuthorityReceipt {
        self.issued_receipt
    }

    /// Retains only secret-free validation of this session's reconciled OAuth epoch.
    pub(crate) fn receipt_currentness(&self) -> SchwabOAuthReceiptCurrentness {
        SchwabOAuthReceiptCurrentness {
            session_id: self.session_id,
            currentness: Arc::clone(&self.currentness),
        }
    }

    /// Returns the exact current receipt after durable authority reconciliation.
    pub(crate) async fn current_receipt(
        &self,
    ) -> Result<SchwabOAuthAuthorityReceipt, SchwabOAuthRuntimeError> {
        self.reconciled_receipt()
            .await
            .map(|(receipt, _currentness)| receipt)
    }

    /// Acquires one transient token and the exact non-cloneable publication epoch for that
    /// attempt.
    ///
    /// The returned epoch retains the serialized token/publication barrier until the caller has
    /// sealed the response and crossed durable precommit. A refresh therefore cannot rotate the
    /// protected generation underneath an admitted response. If this acquisition did rotate the
    /// token, the returned receipt exposes that exact generation so the account activation can
    /// require a fresh doctor disposition before the request is dispatched.
    pub(crate) async fn acquire_publication_attempt(
        &self,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabOAuthRuntimeError> {
        let barrier = self.currentness.acquire_attempt_barrier().await?;
        let token = match self.authority.acquire().await {
            Ok(token) => token,
            Err(error) => {
                self.currentness.invalidate();
                return Err(map_token_authority_error(error));
            }
        };
        let (receipt, generation_currentness) = self.reconciled_receipt().await?;
        if token.generation() != receipt.generation() {
            self.currentness.invalidate();
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        let epoch = SchwabOAuthPublicationEpoch {
            session_id: self.currentness.session_id,
            receipt,
            currentness: generation_currentness.child_token(),
            _attempt_barrier: barrier,
        };
        epoch.validate_current(receipt)?;
        Ok((token, epoch))
    }

    async fn reconciled_receipt(
        &self,
    ) -> Result<(SchwabOAuthAuthorityReceipt, CancellationToken), SchwabOAuthRuntimeError> {
        if self.currentness.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        let status = match self.authority.status().await {
            Ok(status) => status,
            Err(error) => {
                self.currentness.invalidate();
                return Err(error.into());
            }
        };
        if self.currentness.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        match status {
            SchwabOAuthAuthorityStatus::Active(receipt) => {
                let currentness = self.currentness.reconcile_current(receipt)?;
                if currentness.is_cancelled() || self.currentness.is_cancelled() {
                    return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
                }
                Ok((receipt, currentness))
            }
            SchwabOAuthAuthorityStatus::AwaitingAuthorization
            | SchwabOAuthAuthorityStatus::ReauthorizationRequired => {
                self.currentness.invalidate();
                Err(SchwabOAuthRuntimeError::ReauthorizationRequired)
            }
        }
    }
}

/// Secret-free validation of an already reconciled OAuth market session.
///
/// Publication holds this capability plus the response's non-cloneable attempt epoch. It has no
/// protected-store or token-source handle and cannot acquire, refresh, or clone a bearer token.
#[derive(Clone)]
pub(crate) struct SchwabOAuthReceiptCurrentness {
    session_id: Uuid,
    currentness: Arc<SchwabOAuthMarketEpochAuthority>,
}

impl SchwabOAuthReceiptCurrentness {
    pub(crate) const fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub(crate) fn validate_current_receipt(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        self.currentness.validate_current_receipt(receipt)
    }
}

impl fmt::Debug for SchwabOAuthReceiptCurrentness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthReceiptCurrentness")
            .field("session_id", &self.session_id)
            .field("revoked", &self.currentness.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl SchwabAccessTokenSource for SchwabOAuthMarketAuthority {
    fn acquire(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<TransientAccessToken, TokenAuthorityError>> + Send + '_>>
    {
        let currentness = Arc::clone(&self.currentness);
        let authority = Arc::clone(&self.authority);
        Box::pin(async move {
            let _barrier = currentness
                .acquire_attempt_barrier()
                .await
                .map_err(map_market_epoch_token_error)?;
            let token = match authority.acquire().await {
                Ok(token) => token,
                Err(error) => {
                    currentness.invalidate();
                    return Err(error);
                }
            };
            let status = authority.status().await.map_err(|error| {
                currentness.invalidate();
                match error {
                    SchwabOAuthAuthorityError::ReauthorizationRequired
                    | SchwabOAuthAuthorityError::MissingRefreshToken => {
                        TokenAuthorityError::ReauthorizationRequired
                    }
                    _ => TokenAuthorityError::Unavailable,
                }
            })?;
            let SchwabOAuthAuthorityStatus::Active(receipt) = status else {
                currentness.invalidate();
                return Err(TokenAuthorityError::ReauthorizationRequired);
            };
            let reconciled = currentness.reconcile_current(receipt).map_err(|error| {
                currentness.invalidate();
                map_market_epoch_token_error(error)
            })?;
            if reconciled.is_cancelled() || token.generation() != receipt.generation() {
                currentness.invalidate();
                return Err(TokenAuthorityError::ReauthorizationRequired);
            }
            Ok(token)
        })
    }
}

impl fmt::Debug for SchwabOAuthMarketAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthMarketAuthority")
            .field("session_id", &self.session_id)
            .field("issued_generation", &self.issued_receipt.generation().get())
            .field("authority", &"[PROTECTED TOKEN AUTHORITY]")
            .field("revoked", &self.currentness.is_cancelled())
            .finish()
    }
}

/// Secret-free synchronous currentness capability for one exact OAuth publication attempt.
///
/// This value owns no token or protected store handle. It can only compare the exact receipt and
/// process-local market epoch minted beside a transient token by
/// [`SchwabOAuthMarketAuthority::acquire_publication_attempt`]. Its owned barrier makes the value
/// deliberately non-cloneable and prevents another token acquisition or refresh until it drops.
pub(crate) struct SchwabOAuthPublicationEpoch {
    session_id: Uuid,
    receipt: SchwabOAuthAuthorityReceipt,
    currentness: CancellationToken,
    _attempt_barrier: OwnedMutexGuard<()>,
}

impl SchwabOAuthPublicationEpoch {
    pub(crate) const fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub(crate) const fn receipt(&self) -> SchwabOAuthAuthorityReceipt {
        self.receipt
    }

    /// Revalidates the exact receipt without blocking or reopening protected OAuth state.
    pub(crate) fn validate_current(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<(), SchwabOAuthRuntimeError> {
        if receipt != self.receipt || self.currentness.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        validate_receipt_time(receipt)?;
        if self.currentness.is_cancelled() {
            return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
        }
        Ok(())
    }
}

impl fmt::Debug for SchwabOAuthPublicationEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthPublicationEpoch")
            .field("session_id", &self.session_id)
            .field("oauth_generation", &self.receipt.generation().get())
            .field("revoked", &self.currentness.is_cancelled())
            .finish()
    }
}

fn same_credential_authority(
    current: &SchwabOAuthBootstrapLease,
    refreshed: &SchwabOAuthBootstrapLease,
) -> bool {
    current.session_id() == refreshed.session_id()
        && current.surface_id() == refreshed.surface_id()
        && current.capability_revision() == refreshed.capability_revision()
        && current.generation() == refreshed.generation()
        && current.application_secret_reference() == refreshed.application_secret_reference()
}

fn lifecycle_view(
    session_id: Uuid,
    action: SchwabOAuthLifecycleAction,
    status: SchwabOAuthAuthorityStatus,
) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
    match status {
        SchwabOAuthAuthorityStatus::AwaitingAuthorization => Ok(SchwabOAuthLifecycleView::new(
            session_id,
            action,
            SchwabOAuthLifecycleState::AwaitingAuthorization,
            None,
            None,
            None,
        )),
        SchwabOAuthAuthorityStatus::Active(receipt) => active_view(session_id, action, receipt),
        SchwabOAuthAuthorityStatus::ReauthorizationRequired => Ok(SchwabOAuthLifecycleView::new(
            session_id,
            action,
            SchwabOAuthLifecycleState::ReauthorizationRequired,
            None,
            None,
            None,
        )),
    }
}

fn exchanging_view(
    session_id: Uuid,
    action: SchwabOAuthLifecycleAction,
) -> SchwabOAuthLifecycleView {
    SchwabOAuthLifecycleView::new(
        session_id,
        action,
        SchwabOAuthLifecycleState::ExchangingAuthorization,
        None,
        None,
        None,
    )
}

fn active_view(
    session_id: Uuid,
    action: SchwabOAuthLifecycleAction,
    receipt: SchwabOAuthAuthorityReceipt,
) -> Result<SchwabOAuthLifecycleView, SchwabOAuthRuntimeError> {
    Ok(SchwabOAuthLifecycleView::new(
        session_id,
        action,
        SchwabOAuthLifecycleState::Active,
        Some(receipt.generation().get()),
        Some(timestamp_from_unix_seconds(
            receipt.access_expires_at_unix_seconds(),
        )?),
        Some(timestamp_from_unix_seconds(
            receipt.refresh_expires_at_unix_seconds(),
        )?),
    ))
}

fn oauth_state() -> Result<Zeroizing<String>, SchwabOAuthRuntimeError> {
    let mut bytes = [0_u8; OAUTH_STATE_BYTES];
    getrandom::fill(&mut bytes).map_err(|_error| SchwabOAuthRuntimeError::RandomUnavailable)?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = Zeroizing::new(String::new());
    encoded
        .try_reserve_exact(OAUTH_STATE_BYTES * 2)
        .map_err(|_error| SchwabOAuthRuntimeError::RandomUnavailable)?;
    for byte in &bytes {
        encoded.push(char::from(HEX[usize::from(*byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(*byte & 0x0f)]));
    }
    bytes.zeroize();
    Ok(encoded)
}

fn unix_seconds() -> Result<u64, SchwabOAuthRuntimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_error| SchwabOAuthRuntimeError::Clock)
}

fn timestamp_from_unix_seconds(seconds: u64) -> Result<Timestamp, SchwabOAuthRuntimeError> {
    let nanos = seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SchwabOAuthRuntimeError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn validate_receipt_time(
    receipt: SchwabOAuthAuthorityReceipt,
) -> Result<(), SchwabOAuthRuntimeError> {
    let now = unix_seconds()?;
    if now >= receipt.access_expires_at_unix_seconds()
        || now >= receipt.refresh_expires_at_unix_seconds()
    {
        return Err(SchwabOAuthRuntimeError::MarketAuthorityRevoked);
    }
    Ok(())
}

fn map_market_epoch_token_error(error: SchwabOAuthRuntimeError) -> TokenAuthorityError {
    match error {
        SchwabOAuthRuntimeError::MarketAuthorityRevoked
        | SchwabOAuthRuntimeError::ReauthorizationRequired
        | SchwabOAuthRuntimeError::ShuttingDown => TokenAuthorityError::ReauthorizationRequired,
        _ => TokenAuthorityError::Unavailable,
    }
}

fn map_token_authority_error(error: TokenAuthorityError) -> SchwabOAuthRuntimeError {
    match error {
        TokenAuthorityError::ReauthorizationRequired => {
            SchwabOAuthRuntimeError::ReauthorizationRequired
        }
        TokenAuthorityError::Unavailable => SchwabOAuthRuntimeError::MarketEpochUnavailable,
    }
}

/// Closed, secret-free application OAuth runtime failure.
#[derive(Debug, Error)]
pub(crate) enum SchwabOAuthRuntimeError {
    #[error("the Schwab OAuth runtime configuration is invalid")]
    InvalidConfiguration,
    #[error("the Schwab OAuth onboarding session is invalid")]
    InvalidSession,
    #[error("another exact Schwab OAuth session already owns the product token authority")]
    DifferentSessionOwned,
    #[error("the Schwab OAuth runtime state is invalid")]
    InvalidState,
    #[error("the Schwab OAuth runtime is shutting down")]
    ShuttingDown,
    #[error("the Schwab OAuth lifecycle operation was cancelled")]
    Cancelled,
    #[error("the Schwab OAuth lifecycle operation capacity is exhausted")]
    OperationCapacity,
    #[error("the fixed Schwab callback endpoint is already owned")]
    CallbackAlreadyOwned,
    #[error("the Schwab OAuth runtime root is unavailable")]
    RuntimeRootUnavailable,
    #[error("the Schwab OAuth runtime root already has a process owner")]
    RuntimeRootAlreadyOwned,
    #[error("Schwab owner reauthorization is required")]
    ReauthorizationRequired,
    #[error("the protected Schwab authorization exchange is still in progress")]
    AuthorizationExchangeInFlight,
    #[error("the issued Schwab market authority was revoked")]
    MarketAuthorityRevoked,
    #[error("the Schwab OAuth publication epoch authority is unavailable")]
    MarketEpochUnavailable,
    #[error("a Schwab market authority remained retained after its required drain")]
    MarketAuthorityRetained,
    #[error("secure OAuth correlation state is unavailable")]
    RandomUnavailable,
    #[error("the Schwab OAuth callback task failed")]
    CallbackTask,
    #[error("the Schwab OAuth token-exchange task failed")]
    ExchangeTask,
    #[error("the Schwab OAuth shutdown deadline elapsed")]
    ShutdownDeadline,
    #[error("the Schwab OAuth runtime is still draining an admitted lifecycle operation")]
    ShutdownStillDraining,
    #[error("the Schwab OAuth clock is unavailable")]
    Clock,
    #[error(transparent)]
    Browser(#[from] SchwabOAuthBrowserError),
    #[error(transparent)]
    MarketDrain(#[from] SchwabOAuthMarketDrainError),
    #[error(transparent)]
    Callback(#[from] OAuthLoopbackError),
    #[error(transparent)]
    Authority(#[from] SchwabOAuthAuthorityError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
}
