//! Bounded equity source consumer for the existing production paper execution graph.
//! Risk/accounts/dispatcher/paper/checkpoint owners are supplied by `start_inner`; none is copied.
use market_squawk_execution::virtual_paper::ExecutionVirtualPaperHook;
use market_squawk_execution::{ExecutionTask, ExecutionTaskReaper};
use market_squawk_live::virtual_paper::{ConsumedVirtualPaperAuthority, VirtualPaperCurrentness};
use market_squawk_services::ServiceError;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct EquitySourceStatus {
    current: Mutex<Option<VirtualPaperCurrentness>>,
}

/// Capacity-one handoff from real active-source reads to the one route-owned strategy consumer.
/// Each value continues to own its original actor count/byte ticket until risk/dispatch drops it.
#[derive(Clone, Debug)]
pub(crate) struct EquityPaperQuoteIngress {
    sender: mpsc::Sender<ConsumedVirtualPaperAuthority>,
    cancellation: CancellationToken,
}
impl EquityPaperQuoteIngress {
    pub(crate) fn reserve(&self) -> Result<EquityPaperQuotePermit, ServiceError> {
        if self.cancellation.is_cancelled() {
            return Err(ServiceError::Unavailable);
        }
        self.sender
            .clone()
            .try_reserve_owned()
            .map(|permit| EquityPaperQuotePermit { permit })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ServiceError::ResourceExhausted,
                mpsc::error::TrySendError::Closed(_) => ServiceError::Unavailable,
            })
    }

    /// Exact descriptor removal, canonical revision change or source replacement closes this run.
    /// A later installation receives a new bounded ingress; the old handle cannot be revived.
    pub(crate) fn invalidate(&self) {
        self.cancellation.cancel();
    }
}

pub(crate) struct EquityPaperQuotePermit {
    permit: mpsc::OwnedPermit<ConsumedVirtualPaperAuthority>,
}
impl EquityPaperQuotePermit {
    pub(crate) fn publish(self, authority: ConsumedVirtualPaperAuthority) {
        let _sender = self.permit.send(authority);
    }
}

/// Source-lifecycle owner stored in the production paper runtime's virtual source variant.
#[derive(Debug)]
pub(crate) struct EquityPaperRuntime {
    worker: Option<ExecutionTask<()>>,
    cancellation: CancellationToken,
    status: Arc<EquitySourceStatus>,
    declared_hook_bytes: usize,
    shutdown_deadline: Duration,
}
impl EquityPaperRuntime {
    /// Called only after the common accounts, corporate-action reopen, risk and dispatcher graph
    /// is assembled. The caller retains the returned ingress in the installed equity route.
    pub(crate) fn try_start(
        mut hook: ExecutionVirtualPaperHook,
        task_reaper: &ExecutionTaskReaper,
        maximum_hook_bytes: usize,
        shutdown_deadline: Duration,
        cancellation: CancellationToken,
    ) -> Result<(EquityPaperQuoteIngress, Self), ServiceError> {
        let declared_hook_bytes = hook
            .retained_bytes()
            .map_err(|_| ServiceError::ResourceExhausted)?;
        if declared_hook_bytes > maximum_hook_bytes
            || shutdown_deadline.is_zero()
            || cancellation.is_cancelled()
        {
            return Err(ServiceError::ResourceExhausted);
        }
        let permit = task_reaper
            .try_reserve()
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let (sender, mut receiver) = mpsc::channel::<ConsumedVirtualPaperAuthority>(1);
        let status = Arc::new(EquitySourceStatus::default());
        let worker_status = Arc::clone(&status);
        let worker_cancellation = cancellation.clone();
        let worker = permit.spawn(async move {
            let _cancellation_guard = worker_cancellation.clone().drop_guard();
            loop {
                let authority = tokio::select! {
                    biased;
                    () = worker_cancellation.cancelled() => break,
                    authority = receiver.recv() => match authority { Some(authority) => authority, None => break },
                };
                if authority.validate_current().is_err() {
                    continue;
                }
                if hook.retained_bytes().ok() != Some(declared_hook_bytes) {
                    worker_cancellation.cancel();
                    break;
                }
                let currentness = authority.currentness();
                // The shared worker remains the final action-coverage and ledger-sequence gate.
                // A rejected order never changes the actual source observation's quality.
                let _disposition = hook.on_quote(authority);
                if let Ok(mut current) = worker_status.current.try_lock() {
                    *current = Some(currentness);
                } else {
                    worker_cancellation.cancel();
                    break;
                }
            }
            receiver.close();
            while receiver.try_recv().is_ok() {}
            if let Ok(mut current) = worker_status.current.try_lock() {
                *current = None;
            }
        }).map_err(|_| ServiceError::ResourceExhausted)?;
        Ok((
            EquityPaperQuoteIngress {
                sender,
                cancellation: cancellation.clone(),
            },
            Self {
                worker: Some(worker),
                cancellation,
                status,
                declared_hook_bytes,
                shutdown_deadline,
            },
        ))
    }
    pub(crate) fn source_is_current(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self.worker.is_some()
            && self.status.current.try_lock().ok().is_some_and(|current| {
                current
                    .as_ref()
                    .is_some_and(VirtualPaperCurrentness::is_current)
            })
    }
    pub(crate) const fn declared_hook_bytes(&self) -> usize {
        self.declared_hook_bytes
    }
    pub(crate) async fn shutdown(mut self) -> Result<(), ServiceError> {
        self.cancellation.cancel();
        let Some(mut worker) = self.worker.take() else {
            return Ok(());
        };
        let deadline = Instant::now()
            .checked_add(self.shutdown_deadline)
            .ok_or(ServiceError::Unavailable)?;
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), worker.join()).await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ServiceError::Unavailable),
            Err(_) => {
                worker.transfer();
                Err(ServiceError::Unavailable)
            }
        }
    }
}
impl Drop for EquityPaperRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
}

impl super::ProductionPaperBotRuntime {
    /// Applies exact original source economics and the same admitted quote as one financial
    /// mutation, then waits for the existing publisher/risk fence before allowing matching.
    pub(crate) async fn reconcile_equity_quotes(
        &self,
        source: crate::application::SourceAppliedCorporateActionPlan,
        expected_sequence: u64,
        authorities: &[ConsumedVirtualPaperAuthority],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if authorities.is_empty() || authorities.len() > 32 {
            return Err(ServiceError::InvalidRequest);
        }
        let marks = authorities
            .iter()
            .map(market_squawk_execution::virtual_paper::VirtualPaperValuationMark::from_authority)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ServiceError::Unavailable)?
            .into_boxed_slice();
        let reference = source.reference().map_err(|_| ServiceError::Unavailable)?;
        let bytes = serde_json::to_vec(&reference).map_err(|_| ServiceError::InvalidResult)?;
        if bytes.len() > 64 * 1024 {
            return Err(ServiceError::ResourceExhausted);
        }
        let plan = source
            .into_covered_accounting_plan()
            .map_err(|_| ServiceError::Unavailable)?;
        if Some(plan.valuation_cutoff())
            != authorities
                .iter()
                .map(ConsumedVirtualPaperAuthority::received_at)
                .max()
        {
            return Err(ServiceError::InvalidResult);
        }
        let bounded = super::bounded_paper_control_deadline(
            self.paper_control_timeout,
            deadline,
            cancellation,
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let control = market_squawk_adapter_paper::PaperControlContext::try_new_before(
            tokio::time::Instant::from_std(bounded),
            cancellation.child_token(),
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let applied = self
            .paper
            .adapter()
            .reconcile_corporate_actions_with_virtual_marks(
                plan,
                bytes,
                expected_sequence,
                marks,
                control,
            )
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        let applied_sequence = applied.sequence();
        let fence = self.accounts.reconciliation_fence();
        loop {
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            if Instant::now() >= bounded {
                return Err(ServiceError::Unavailable);
            }
            for authority in authorities {
                authority
                    .validate_current()
                    .map_err(|_| ServiceError::Unavailable)?;
            }
            let required = fence.required_sequence();
            let acknowledged = fence.applied_sequence();
            if required > applied_sequence || acknowledged > applied_sequence {
                return Err(ServiceError::Unavailable);
            }
            if required == applied_sequence
                && acknowledged == applied_sequence
                && fence.is_current()
            {
                return Ok(());
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(bounded)) => return Err(ServiceError::Unavailable),
                () = tokio::time::sleep(Duration::from_millis(5)) => {},
            }
        }
    }
}
