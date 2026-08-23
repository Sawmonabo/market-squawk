//! Shared provider/account budgets for onboarding verification probes.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_domain::{EvidenceDigest, SourceIdentifier};
use market_squawk_sources::{
    BudgetDecision, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetReservationDecision, BudgetUnavailableReason, BudgetWindowSemantics, ProbeTransport,
    ProviderBudgetPolicy, ProviderBudgetWindow, ProviderOnboardingProfile, ProviderProfileRegistry,
    ProviderRateAuthority, ProviderRateDeclaration, RatePolicyDescriptor, SharedProviderBudget,
    apply_http_retry_after,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::ProviderOnboardingError;

const PROBE_OPERATION_DURATION: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProbeRateScopeKey {
    provider: SourceIdentifier,
    authorization_account: Option<SourceIdentifier>,
}

#[derive(Debug)]
pub(super) struct ProbeRateAuthority {
    policies: BTreeMap<SourceIdentifier, ProbeRateBinding>,
    provider_rate: Option<ProviderRateAuthority>,
}

#[derive(Debug)]
struct ProbeRateBinding {
    descriptor: RatePolicyDescriptor,
    scope: Arc<ProbeRateScope>,
}

#[derive(Debug)]
struct ProbeRateScope {
    policy: ProviderBudgetPolicy,
    scope_evidence_digest: EvidenceDigest,
    refresh_on_http_429: bool,
    concurrency: Arc<Semaphore>,
    state: AsyncMutex<ProbeRateState>,
}

#[derive(Debug)]
struct ProbeRateState {
    windows: Vec<ProbeRateWindowState>,
    cooldown_until: Option<Instant>,
}

#[derive(Debug)]
struct ProbeRateWindowState {
    descriptor: ProviderBudgetWindow,
    started_at: Instant,
    admitted: u32,
    sliding_releases: VecDeque<Instant>,
}

#[derive(Debug)]
pub(super) struct ProbeRatePermit {
    authority: ProbeRatePermitAuthority,
    pub(super) deadline: Instant,
}

#[derive(Debug)]
enum ProbeRatePermitAuthority {
    Legacy {
        scope: Arc<ProbeRateScope>,
        _concurrency: OwnedSemaphorePermit,
    },
    Aggregate {
        budget: SharedProviderBudget,
        reservation: Option<BudgetReservation>,
        permit: Option<BudgetPermit>,
    },
}

impl ProbeRateAuthority {
    pub(super) fn try_new(
        profiles: &ProviderProfileRegistry,
    ) -> Result<Self, ProviderOnboardingError> {
        let mut scopes: BTreeMap<ProbeRateScopeKey, Arc<ProbeRateScope>> = BTreeMap::new();
        let mut policies = BTreeMap::new();
        for profile in profiles.iter() {
            let descriptor = profile.capability().rate_policy();
            let policy = descriptor
                .enforcement_policy()
                .cloned()
                .ok_or(ProviderOnboardingError::InvalidProfile)?;
            let scope_evidence_digest = descriptor
                .scope_evidence_digest()
                .ok_or(ProviderOnboardingError::InvalidProfile)?;
            let refresh_on_http_429 = descriptor
                .refresh_on_http_429()
                .ok_or(ProviderOnboardingError::InvalidProfile)?;
            if descriptor.enforcement_revision().is_none()
                || descriptor.endpoint_class().is_none()
                || !descriptor.unknown_is_conservative()
                || (profile.probe().transport() != ProbeTransport::Local && !refresh_on_http_429)
            {
                return Err(ProviderOnboardingError::InvalidProfile);
            }
            let scope = policy.scope();
            let key = ProbeRateScopeKey {
                provider: scope.as_source_identifier().clone(),
                authorization_account: scope.authorization_account().cloned(),
            };
            let shared_scope = if let Some(existing) = scopes.get(&key) {
                if existing.policy != policy
                    || existing.scope_evidence_digest != scope_evidence_digest
                    || existing.refresh_on_http_429 != refresh_on_http_429
                {
                    return Err(ProviderOnboardingError::InvalidProfile);
                }
                Arc::clone(existing)
            } else {
                let scope = Arc::new(ProbeRateScope::try_new(
                    policy,
                    scope_evidence_digest,
                    refresh_on_http_429,
                )?);
                scopes.insert(key, Arc::clone(&scope));
                scope
            };
            let binding = ProbeRateBinding {
                descriptor: descriptor.clone(),
                scope: shared_scope,
            };
            if policies
                .insert(descriptor.policy_id().clone(), binding)
                .is_some()
            {
                return Err(ProviderOnboardingError::InvalidProfile);
            }
        }
        Ok(Self {
            policies,
            provider_rate: None,
        })
    }

    pub(super) fn try_new_with_provider_rate(
        profiles: &ProviderProfileRegistry,
        provider_rate: ProviderRateAuthority,
    ) -> Result<Self, ProviderOnboardingError> {
        let mut authority = Self::try_new(profiles)?;
        authority.provider_rate = Some(provider_rate);
        Ok(authority)
    }

    pub(super) async fn acquire(
        &self,
        profile: &ProviderOnboardingProfile,
        descriptor: &RatePolicyDescriptor,
        authorization_subject: Option<&SourceIdentifier>,
        cancellation: CancellationToken,
    ) -> Result<ProbeRatePermit, ProviderOnboardingError> {
        let binding = self
            .policies
            .get(descriptor.policy_id())
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        if binding.descriptor != *descriptor {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        if let Some(provider_rate) = &self.provider_rate {
            let policy = descriptor
                .enforcement_policy()
                .cloned()
                .ok_or(ProviderOnboardingError::InvalidProfile)?;
            let declaration = match policy.scope().authorization_account() {
                Some(_) => ProviderRateDeclaration::try_for_authorization_subject(
                    policy,
                    authorization_subject.ok_or(ProviderOnboardingError::InvalidProfile)?,
                ),
                None => ProviderRateDeclaration::try_for_endpoint(
                    policy,
                    profile
                        .probe()
                        .endpoint_policy()
                        .ok_or(ProviderOnboardingError::InvalidProfile)?,
                ),
            }
            .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
            let budget = provider_rate
                .register_budget(declaration)
                .map_err(|_| ProviderOnboardingError::ProbeRateLimited)?;
            return acquire_aggregate_budget(budget, cancellation).await;
        }
        binding.scope.acquire(cancellation).await
    }
}

impl ProbeRateScope {
    fn try_new(
        policy: ProviderBudgetPolicy,
        scope_evidence_digest: EvidenceDigest,
        refresh_on_http_429: bool,
    ) -> Result<Self, ProviderOnboardingError> {
        let started_at = Instant::now();
        let mut windows = Vec::new();
        windows
            .try_reserve_exact(policy.window_count())
            .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        for index in 0..policy.window_count() {
            let descriptor = policy
                .window(index)
                .ok_or(ProviderOnboardingError::InvalidProfile)?;
            let mut sliding_releases = VecDeque::new();
            if descriptor.semantics() == BudgetWindowSemantics::Sliding {
                let capacity = usize::try_from(descriptor.requests_per_window())
                    .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
                sliding_releases
                    .try_reserve_exact(capacity)
                    .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
            }
            windows.push(ProbeRateWindowState {
                descriptor,
                started_at,
                admitted: 0,
                sliding_releases,
            });
        }
        if windows.is_empty() || policy.max_concurrent() == 0 {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        Ok(Self {
            concurrency: Arc::new(Semaphore::new(usize::from(policy.max_concurrent()))),
            policy,
            scope_evidence_digest,
            refresh_on_http_429,
            state: AsyncMutex::new(ProbeRateState {
                windows,
                cooldown_until: None,
            }),
        })
    }

    async fn acquire(
        self: &Arc<Self>,
        cancellation: CancellationToken,
    ) -> Result<ProbeRatePermit, ProviderOnboardingError> {
        let deadline = Instant::now()
            .checked_add(PROBE_OPERATION_DURATION)
            .ok_or(ProviderOnboardingError::Clock)?;
        loop {
            let blocked_until = {
                let mut state = self.state.lock().await;
                state.blocked_until(Instant::now())?
            };
            if let Some(blocked_until) = blocked_until {
                wait_for_probe_rate(blocked_until, deadline, &cancellation).await?;
                continue;
            }
            let concurrency = Arc::clone(&self.concurrency);
            let permit = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(ProviderOnboardingError::OperationCancelled);
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    return Err(ProviderOnboardingError::ProbeRateLimited);
                }
                permit = concurrency.acquire_owned() => {
                    permit.map_err(|_| ProviderOnboardingError::ProbeRateLimited)?
                }
            };
            let now = Instant::now();
            let mut state = self.state.lock().await;
            if let Some(blocked_until) = state.blocked_until(now)? {
                drop(state);
                drop(permit);
                wait_for_probe_rate(blocked_until, deadline, &cancellation).await?;
                continue;
            }
            state.admit(now)?;
            return Ok(ProbeRatePermit {
                authority: ProbeRatePermitAuthority::Legacy {
                    scope: Arc::clone(self),
                    _concurrency: permit,
                },
                deadline,
            });
        }
    }
}

impl ProbeRateState {
    fn blocked_until(&mut self, now: Instant) -> Result<Option<Instant>, ProviderOnboardingError> {
        let mut blocker = self.cooldown_until.filter(|deadline| *deadline > now);
        if blocker.is_none() {
            self.cooldown_until = None;
        }
        for window in &mut self.windows {
            let duration = Duration::from_nanos(window.descriptor.window_nanos());
            match window.descriptor.semantics() {
                BudgetWindowSemantics::Tumbling => {
                    let ends_at = window
                        .started_at
                        .checked_add(duration)
                        .ok_or(ProviderOnboardingError::Clock)?;
                    if now >= ends_at {
                        window.started_at = now;
                        window.admitted = 0;
                    } else if window.admitted >= window.descriptor.requests_per_window() {
                        blocker = Some(blocker.map_or(ends_at, |current| current.max(ends_at)));
                    }
                }
                BudgetWindowSemantics::Sliding => {
                    while window
                        .sliding_releases
                        .front()
                        .is_some_and(|release| *release <= now)
                    {
                        let _released = window.sliding_releases.pop_front();
                    }
                    if window.sliding_releases.len()
                        >= usize::try_from(window.descriptor.requests_per_window())
                            .map_err(|_| ProviderOnboardingError::InvalidProfile)?
                    {
                        let release = window
                            .sliding_releases
                            .front()
                            .copied()
                            .ok_or(ProviderOnboardingError::InvalidProfile)?;
                        blocker = Some(blocker.map_or(release, |current| current.max(release)));
                    }
                }
            }
        }
        Ok(blocker)
    }

    fn admit(&mut self, now: Instant) -> Result<(), ProviderOnboardingError> {
        if self.blocked_until(now)?.is_some() {
            return Err(ProviderOnboardingError::ProbeRateLimited);
        }
        for window in &mut self.windows {
            match window.descriptor.semantics() {
                BudgetWindowSemantics::Tumbling => {
                    window.admitted = window
                        .admitted
                        .checked_add(1)
                        .ok_or(ProviderOnboardingError::InvalidProfile)?;
                }
                BudgetWindowSemantics::Sliding => {
                    let release = now
                        .checked_add(Duration::from_nanos(window.descriptor.window_nanos()))
                        .ok_or(ProviderOnboardingError::Clock)?;
                    window.sliding_releases.push_back(release);
                }
            }
        }
        Ok(())
    }
}

impl ProbeRatePermit {
    pub(super) async fn commit_dispatch(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProviderOnboardingError> {
        let ProbeRatePermitAuthority::Aggregate {
            budget,
            reservation,
            permit,
        } = &mut self.authority
        else {
            return Ok(());
        };
        if permit.is_some() {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        loop {
            let candidate = match reservation.take() {
                Some(candidate) => candidate,
                None => reserve_aggregate_budget(budget, self.deadline, cancellation).await?,
            };
            match candidate.commit_dispatch() {
                BudgetDispatchDecision::Ready(active) => {
                    *permit = Some(active);
                    return Ok(());
                }
                BudgetDispatchDecision::WaitUntil(blocked_until) => {
                    let wait = budget
                        .remaining_wait(blocked_until)
                        .map_err(|_| ProviderOnboardingError::ProbeRateLimited)?;
                    wait_for_aggregate_rate(wait, self.deadline, cancellation).await?;
                }
                BudgetDispatchDecision::Unavailable(
                    BudgetUnavailableReason::ConcurrencyExhausted,
                ) => {
                    wait_for_aggregate_rate(Duration::from_millis(25), self.deadline, cancellation)
                        .await?;
                }
                BudgetDispatchDecision::Unavailable(_) => {
                    return Err(ProviderOnboardingError::ProbeRateLimited);
                }
            }
        }
    }

    pub(super) async fn observe_http_429(
        &self,
        retry_after: Option<&[u8]>,
    ) -> Result<(), ProviderOnboardingError> {
        match &self.authority {
            ProbeRatePermitAuthority::Legacy { scope, .. } => {
                if !scope.refresh_on_http_429 {
                    return Err(ProviderOnboardingError::InvalidProfile);
                }
                let maximum = Duration::from_nanos(scope.policy.backoff().maximum_nanos());
                let delay = retry_after
                    .and_then(parse_retry_after_seconds)
                    .map(Duration::from_secs)
                    .filter(|delay| !delay.is_zero())
                    .map_or(maximum, |delay| delay.min(maximum));
                let cooldown = Instant::now()
                    .checked_add(delay)
                    .ok_or(ProviderOnboardingError::Clock)?;
                let mut state = scope.state.lock().await;
                state.cooldown_until = Some(
                    state
                        .cooldown_until
                        .map_or(cooldown, |current| current.max(cooldown)),
                );
                Ok(())
            }
            ProbeRatePermitAuthority::Aggregate {
                budget,
                reservation: None,
                permit: Some(_),
            } => match apply_http_retry_after(budget, retry_after, 0) {
                BudgetDecision::WaitUntil(_) => Ok(()),
                BudgetDecision::Unavailable(BudgetUnavailableReason::RetryAfterExceedsPolicy) => {
                    Ok(())
                }
                BudgetDecision::Unavailable(_) | BudgetDecision::Ready(_) => {
                    Err(ProviderOnboardingError::ProbeRateLimited)
                }
            },
            ProbeRatePermitAuthority::Aggregate { .. } => {
                Err(ProviderOnboardingError::InvalidSessionState)
            }
        }
    }

    pub(super) fn record_success(&self) -> Result<(), ProviderOnboardingError> {
        match &self.authority {
            ProbeRatePermitAuthority::Legacy { .. } => Ok(()),
            ProbeRatePermitAuthority::Aggregate {
                budget,
                reservation: None,
                permit: Some(_),
            } => budget
                .record_success()
                .map_err(|_| ProviderOnboardingError::ProbeRateLimited),
            ProbeRatePermitAuthority::Aggregate { .. } => {
                Err(ProviderOnboardingError::InvalidSessionState)
            }
        }
    }
}

async fn acquire_aggregate_budget(
    budget: SharedProviderBudget,
    cancellation: CancellationToken,
) -> Result<ProbeRatePermit, ProviderOnboardingError> {
    let deadline = Instant::now()
        .checked_add(PROBE_OPERATION_DURATION)
        .ok_or(ProviderOnboardingError::Clock)?;
    let reservation = reserve_aggregate_budget(&budget, deadline, &cancellation).await?;
    Ok(ProbeRatePermit {
        authority: ProbeRatePermitAuthority::Aggregate {
            budget,
            reservation: Some(reservation),
            permit: None,
        },
        deadline,
    })
}

async fn reserve_aggregate_budget(
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetReservation, ProviderOnboardingError> {
    const CONCURRENCY_RECHECK: Duration = Duration::from_millis(25);

    loop {
        match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => {
                return Ok(reservation);
            }
            BudgetReservationDecision::WaitUntil(blocked_until) => {
                let wait = budget
                    .remaining_wait(blocked_until)
                    .map_err(|_| ProviderOnboardingError::ProbeRateLimited)?;
                wait_for_aggregate_rate(wait, deadline, &cancellation).await?;
            }
            BudgetReservationDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted,
            ) => {
                wait_for_aggregate_rate(CONCURRENCY_RECHECK, deadline, &cancellation).await?;
            }
            BudgetReservationDecision::Unavailable(_) => {
                return Err(ProviderOnboardingError::ProbeRateLimited);
            }
        }
    }
}

async fn wait_for_aggregate_rate(
    wait: Duration,
    operation_deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProviderOnboardingError> {
    let now = Instant::now();
    let wake = now
        .checked_add(wait)
        .ok_or(ProviderOnboardingError::Clock)?;
    if now >= operation_deadline || wake >= operation_deadline {
        return Err(ProviderOnboardingError::ProbeRateLimited);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ProviderOnboardingError::OperationCancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => Ok(()),
    }
}

async fn wait_for_probe_rate(
    blocked_until: Instant,
    operation_deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProviderOnboardingError> {
    let now = Instant::now();
    if now >= operation_deadline || blocked_until >= operation_deadline {
        return Err(ProviderOnboardingError::ProbeRateLimited);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ProviderOnboardingError::OperationCancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(blocked_until)) => Ok(()),
    }
}

fn parse_retry_after_seconds(field: &[u8]) -> Option<u64> {
    if field.is_empty() || field.len() > 20 || !field.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(field).ok()?.parse().ok()
}
