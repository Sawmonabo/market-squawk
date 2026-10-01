//! Optional connection protection, independent of local service and saved-data access.

use crate::application::source::{SourceLifecycleAuthority, SourceLifecycleError};
use crate::{ProviderOnboardingService, ProviderPortalActivationAuthority};
use market_squawk_platform::{
    SecretAccessPolicy, SecretAccessState, SecretAccessStatus, SecretValue,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

const ACCESS_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Eq, PartialEq)]
enum RuntimeTransition {
    Open,
    Suspending(SuspendStep),
    Suspended,
    Resuming(ResumeStep),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SuspendStep {
    Paper,
    Portal,
    Source,
    Onboarding,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ResumeStep {
    Source,
    Paper,
}

/// A committed vault operation must wake recovery even if its request future is dropped.
struct NotifyAccessChange<'a>(&'a Notify);

impl Drop for NotifyAccessChange<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

pub(crate) struct CredentialAccessCoordinator {
    onboarding: Arc<ProviderOnboardingService>,
    portal: Arc<dyn ProviderPortalActivationAuthority>,
    lifecycle: Arc<dyn SourceLifecycleAuthority>,
    paper: crate::application::PaperCredentialRuntimeControl,
    mutation: Mutex<RuntimeTransition>,
    changed: Notify,
    suspended: AtomicBool,
    vault_pending: AtomicBool,
}

impl CredentialAccessCoordinator {
    pub(crate) fn new(
        onboarding: Arc<ProviderOnboardingService>,
        portal: Arc<dyn ProviderPortalActivationAuthority>,
        lifecycle: Arc<dyn SourceLifecycleAuthority>,
        paper: crate::application::PaperCredentialRuntimeControl,
    ) -> Self {
        Self {
            onboarding,
            portal,
            lifecycle,
            paper,
            mutation: Mutex::new(RuntimeTransition::Open),
            changed: Notify::new(),
            suspended: AtomicBool::new(false),
            vault_pending: AtomicBool::new(false),
        }
    }

    pub(crate) fn status(&self) -> Result<SecretAccessStatus, SourceLifecycleError> {
        let mut status = self
            .onboarding
            .credential_access_status()
            .map_err(|_| SourceLifecycleError::Unavailable)?;
        if status.access == SecretAccessState::Locked && !self.suspended.load(Ordering::Acquire) {
            status.access = SecretAccessState::RecoveryRequired;
        }
        Ok(status)
    }

    pub(crate) async fn configure(
        &self,
        policy: SecretAccessPolicy,
        secret: Option<SecretValue>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecretAccessStatus, SourceLifecycleError> {
        let mut transition = self.mutation.lock().await;
        let _notify = NotifyAccessChange(&self.changed);
        self.finish_pending_suspension(&mut transition, deadline, &cancellation)
            .await?;
        self.vault_pending.store(true, Ordering::Release);
        let outcome = self
            .onboarding
            .configure_credential_access(policy, secret, cancellation.clone())
            .await;
        self.vault_pending.store(false, Ordering::Release);
        outcome.map_err(|_| SourceLifecycleError::Unavailable)?;
        self.reconcile(&mut transition, deadline, &cancellation)
            .await?;
        self.status()
    }

    pub(crate) async fn unlock(
        &self,
        secret: SecretValue,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecretAccessStatus, SourceLifecycleError> {
        let mut transition = self.mutation.lock().await;
        let _notify = NotifyAccessChange(&self.changed);
        self.finish_pending_suspension(&mut transition, deadline, &cancellation)
            .await?;
        self.vault_pending.store(true, Ordering::Release);
        let outcome = self
            .onboarding
            .unlock_credential_access(secret, cancellation.clone())
            .await;
        self.vault_pending.store(false, Ordering::Release);
        outcome.map_err(|_| SourceLifecycleError::Unauthorized)?;
        self.reconcile(&mut transition, deadline, &cancellation)
            .await?;
        self.status()
    }

    pub(crate) async fn lock(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecretAccessStatus, SourceLifecycleError> {
        let mut transition = self.mutation.lock().await;
        let _notify = NotifyAccessChange(&self.changed);
        // Seal new reads first; then join work that had already acquired credentials. The lock
        // receipt is returned only after all credential-bearing runtime owners acknowledge drain.
        self.vault_pending.store(true, Ordering::Release);
        let outcome = self
            .onboarding
            .lock_credential_access(cancellation.clone())
            .await;
        self.vault_pending.store(false, Ordering::Release);
        outcome.map_err(|_| SourceLifecycleError::Unavailable)?;
        self.suspend(&mut transition, deadline, &cancellation)
            .await?;
        self.status()
    }

    pub(crate) async fn forget(
        &self,
        cancellation: CancellationToken,
    ) -> Result<SecretAccessStatus, SourceLifecycleError> {
        let _mutation = self.mutation.lock().await;
        let _notify = NotifyAccessChange(&self.changed);
        self.vault_pending.store(true, Ordering::Release);
        let outcome = self.onboarding.forget_credential_access(cancellation).await;
        self.vault_pending.store(false, Ordering::Release);
        outcome.map_err(|_| SourceLifecycleError::Unavailable)?;
        self.status()
    }

    /// Finish interrupted drains before reopening the vault or its runtime owners. This also
    /// covers an initially locked installation whose monitor has not acquired the owner yet.
    async fn finish_pending_suspension(
        &self,
        transition: &mut RuntimeTransition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        self.finish_vault_operation(deadline, cancellation).await?;
        let status = self
            .onboarding
            .credential_access_status()
            .map_err(|_| SourceLifecycleError::Unavailable)?;
        if matches!(transition, RuntimeTransition::Suspending(_))
            || status.access != SecretAccessState::Ready
        {
            self.suspend(transition, deadline, cancellation).await?;
        }
        Ok(())
    }

    /// A dropped request can leave the existing secret worker under reaper custody. Join its
    /// permit before reading access state, so recovery cannot miss a later committed unlock.
    async fn finish_vault_operation(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        if self.vault_pending.load(Ordering::Acquire) {
            self.onboarding
                .drain_credential_operations(deadline, cancellation)
                .await
                .map_err(|_| SourceLifecycleError::Unavailable)?;
            self.vault_pending.store(false, Ordering::Release);
        }
        Ok(())
    }

    async fn reconcile(
        &self,
        transition: &mut RuntimeTransition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        let status = self
            .onboarding
            .credential_access_status()
            .map_err(|_| SourceLifecycleError::Unavailable)?;
        if status.access == SecretAccessState::Ready {
            self.resume(transition, deadline, cancellation).await
        } else {
            self.suspend(transition, deadline, cancellation).await
        }
    }

    async fn suspend(
        &self,
        transition: &mut RuntimeTransition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        if *transition == RuntimeTransition::Suspended {
            return Ok(());
        }
        if !matches!(transition, RuntimeTransition::Suspending(_)) {
            self.suspended.store(false, Ordering::Release);
            *transition = RuntimeTransition::Suspending(SuspendStep::Paper);
        }
        // Store each exact step before its first await. OAuth must drain before Source retains
        // the shared mutation gate; retrying later steps must not re-enter the OAuth drain.
        loop {
            match *transition {
                RuntimeTransition::Suspending(SuspendStep::Paper) => {
                    self.paper
                        .suspend(deadline, cancellation)
                        .await
                        .map_err(|_| SourceLifecycleError::Unavailable)?;
                    *transition = RuntimeTransition::Suspending(SuspendStep::Portal);
                }
                RuntimeTransition::Suspending(SuspendStep::Portal) => {
                    self.portal
                        .suspend_credential_access(deadline)
                        .await
                        .map_err(|_| SourceLifecycleError::Unavailable)?;
                    *transition = RuntimeTransition::Suspending(SuspendStep::Source);
                }
                RuntimeTransition::Suspending(SuspendStep::Source) => {
                    self.lifecycle
                        .suspend_credential_runtimes(deadline, cancellation)
                        .await?;
                    *transition = RuntimeTransition::Suspending(SuspendStep::Onboarding);
                }
                RuntimeTransition::Suspending(SuspendStep::Onboarding) => {
                    self.onboarding
                        .drain_credential_operations(deadline, cancellation)
                        .await
                        .map_err(|_| SourceLifecycleError::Unavailable)?;
                    *transition = RuntimeTransition::Suspended;
                    self.suspended.store(true, Ordering::Release);
                    return Ok(());
                }
                _ => return Err(SourceLifecycleError::Internal),
            }
        }
    }

    async fn resume(
        &self,
        transition: &mut RuntimeTransition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        if matches!(transition, RuntimeTransition::Suspending(_)) {
            self.suspend(transition, deadline, cancellation).await?;
        }
        if *transition == RuntimeTransition::Suspended {
            self.suspended.store(false, Ordering::Release);
            *transition = RuntimeTransition::Resuming(ResumeStep::Source);
        }
        loop {
            match *transition {
                RuntimeTransition::Open => return Ok(()),
                RuntimeTransition::Resuming(ResumeStep::Source) => {
                    // Source releases its held mutation gate before reopening the same portal,
                    // and retains restoration intent if this waiter fails or is dropped.
                    self.lifecycle
                        .resume_credential_runtimes(deadline, cancellation)
                        .await?;
                    *transition = RuntimeTransition::Resuming(ResumeStep::Paper);
                }
                RuntimeTransition::Resuming(ResumeStep::Paper) => {
                    self.paper
                        .resume(deadline, cancellation)
                        .await
                        .map_err(|_| SourceLifecycleError::Unavailable)?;
                    *transition = RuntimeTransition::Open;
                }
                _ => return Err(SourceLifecycleError::Internal),
            }
        }
    }

    /// Driven by the installed service lifetime; no detached task or additional runtime.
    pub(crate) async fn monitor(&self, cancellation: CancellationToken) {
        let _cancel_on_drop = cancellation.clone().drop_guard();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut retry = false;
            let deadline = {
                let mut transition = self.mutation.lock().await;
                let recovery_deadline = Instant::now() + ACCESS_DRAIN_TIMEOUT;
                let status = self
                    .finish_vault_operation(recovery_deadline, &cancellation)
                    .await
                    .and_then(|()| {
                        self.onboarding
                            .credential_access_status()
                            .map_err(|_| SourceLifecycleError::Unavailable)
                    });
                match status {
                    Ok(status) => {
                        let result = if status.access == SecretAccessState::Ready {
                            self.resume(&mut transition, recovery_deadline, &cancellation)
                                .await
                        } else {
                            self.suspend(&mut transition, recovery_deadline, &cancellation)
                                .await
                        };
                        if result.is_err() {
                            tracing::warn!(
                                "optional connection protection is awaiting runtime recovery"
                            );
                            retry = true;
                        }
                        if status.access == SecretAccessState::Ready {
                            status.reauthenticate_at_unix_seconds
                        } else {
                            None
                        }
                    }
                    Err(_) => {
                        retry = true;
                        None
                    }
                }
            };
            let delay = if retry {
                Some(Duration::from_secs(5))
            } else {
                deadline.map(|deadline| {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    // This schedules a chosen deadline, not an imposed authentication interval.
                    // Recheck wall-clock changes at most daily for distant chosen deadlines.
                    Duration::from_secs(deadline.saturating_sub(now).min(86_400))
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                () = &mut changed => {},
                () = async { match delay { Some(delay) => tokio::time::sleep(delay).await, None => std::future::pending::<()>().await } } => {},
            }
        }
    }
}
