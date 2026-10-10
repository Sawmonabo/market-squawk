//! Finite generation-bound history work retained by the existing product startup task owner.

use super::ProductionSourceLifecycleAuthority;
use crate::application::AlpacaHistoricalRuntimeCapability;
use crate::application::{map_market_definition_read_error, map_source_research_error};
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use market_squawk_domain::Timestamp;
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
};
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Only the latest exact generation is retained; notifications never accumulate a work queue.
#[derive(Clone)]
pub(crate) struct StarterHistoryRequest {
    runtime: AlpacaHistoricalRuntimeCapability,
}

pub(crate) type StarterHistorySender = watch::Sender<Option<StarterHistoryRequest>>;
pub(crate) type StarterHistoryReceiver = watch::Receiver<Option<StarterHistoryRequest>>;

impl ProductionSourceLifecycleAuthority {
    pub(crate) fn starter_history_channel() -> (StarterHistorySender, StarterHistoryReceiver) {
        watch::channel(None)
    }

    /// Transfers finite preparation to product lifetime ownership after source activation.
    pub(super) fn admit_display_history(
        &self,
        runtime: AlpacaHistoricalRuntimeCapability,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        if runtime.is_revoked() {
            return Err(ServiceError::Unavailable);
        }
        self.display_history_requests
            .send(Some(StarterHistoryRequest { runtime }))
            .map_err(|_| ServiceError::Unavailable)
    }

    /// This entire future belongs to ProductStartupTasks. Cancellation signals the actual work
    /// and then awaits its cleanup; neither a replacement nor shutdown drops that work early.
    pub(crate) async fn run_display_history_worker(
        self: Arc<Self>,
        mut requests: StarterHistoryReceiver,
        shutdown: CancellationToken,
    ) {
        let mut pending = None;
        let mut attempted_generation = None;
        let mut next_day: Option<tokio::time::Instant> = None;
        let history_publications = self.research.history_publications();
        loop {
            let (request, retained_only) = if let Some(request) = pending.take() {
                (request, false)
            } else {
                let retained_only = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    changed = requests.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        false
                    }
                    () = history_publications.notified() => true,
                    () = async {
                        match next_day {
                            Some(at) => tokio::time::sleep_until(at).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => false,
                };
                // A publication wake must not consume an activation racing with it: that
                // activation still owns its one acquisition pass on the next iteration.
                let request = if retained_only {
                    requests.borrow().clone()
                } else {
                    requests.borrow_and_update().clone()
                };
                let Some(request) = request else {
                    next_day = None;
                    continue;
                };
                (request, retained_only)
            };
            if shutdown.is_cancelled() {
                return;
            }
            if request.runtime.is_revoked() {
                next_day = None;
                continue;
            }
            let (native_day, until_next_day) = match display_native_day_now() {
                Ok(day) => day,
                Err(error) => {
                    next_day = None;
                    record_history_outcome(Err(error));
                    continue;
                }
            };
            next_day = tokio::time::Instant::now().checked_add(until_next_day);
            let generation = (request.runtime.group_generation(), native_day);
            // A publication racing the midnight timer still owes this generation its new
            // native-day preparation; resetting the timer must not skip that acquisition.
            let retained_only = retained_only
                && !attempted_generation.is_some_and(|(previous_generation, previous_day)| {
                    previous_generation == generation.0 && previous_day != native_day
                });
            if !retained_only && attempted_generation == Some(generation) {
                continue;
            }
            if !retained_only {
                attempted_generation = Some(generation);
            }
            // Shared setup is bounded separately. Each instrument receives its own ordinary
            // recovery window when admitted; this generation is never retried by a duplicate.
            let Some(deadline) = Instant::now().checked_add(super::super::LOCAL_RECOVERY_TIMEOUT)
            else {
                record_history_outcome(Err(ServiceError::Internal));
                continue;
            };
            let cancellation = shutdown.child_token();
            let preparation = self.prepare_display_history(
                &request.runtime,
                retained_only,
                deadline,
                &cancellation,
            );
            tokio::pin!(preparation);
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        cancellation.cancel();
                        record_history_outcome(preparation.await);
                        return;
                    }
                    () = request.runtime.wait_until_revoked() => {
                        cancellation.cancel();
                        record_history_outcome(preparation.await);
                        break;
                    }
                    changed = requests.changed() => {
                        if changed.is_err() {
                            cancellation.cancel();
                            record_history_outcome(preparation.await);
                            return;
                        }
                        let next = requests.borrow_and_update().clone();
                        if next.as_ref().is_some_and(|next| {
                            next.runtime.group_generation() == request.runtime.group_generation()
                        }) {
                            // A duplicate cannot extend this admitted operation's deadline.
                            continue;
                        }
                        cancellation.cancel();
                        record_history_outcome(preparation.await);
                        // Replacement may itself have been superseded while cleanup drained.
                        pending = requests.borrow_and_update().clone();
                        break;
                    }
                    result = &mut preparation => {
                        record_history_outcome(result);
                        break;
                    }
                }
            }
        }
    }

    async fn prepare_display_history(
        &self,
        runtime: &AlpacaHistoricalRuntimeCapability,
        retained_only: bool,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        let instruments = self
            .live
            .equity_paper_instrument_ids(deadline, cancellation)
            .await
            .map_err(|error| {
                tracing::warn!(
                    ?error,
                    stage = "active-display-instruments",
                    "market display history preparation unavailable"
                );
                error
            })?;
        let reader = self.research.market_data_instruments();
        let records = self
            .research
            .run_owned_research_read(deadline, cancellation, move |operation_cancellation| {
                instruments
                    .into_iter()
                    .map(|instrument| {
                        reader
                            .latest(instrument, deadline, &operation_cancellation)
                            .map_err(map_market_definition_read_error)?
                            .ok_or(ServiceError::Unavailable)
                    })
                    .collect::<Result<Vec<_>, ServiceError>>()
            })
            .await
            .map_err(map_source_research_error)
            .and_then(std::convert::identity)
            .map_err(|error| {
                tracing::warn!(
                    ?error,
                    stage = "display-instrument-definitions",
                    "market display history preparation unavailable"
                );
                error
            })?;
        // These bounds govern the one publication receipt, not retained market history. The
        // provider coordinator retains its ordinary streaming, rate and acquisition policies.
        let structure = JsonStructureLimits::try_new(32, 1024 * 1024, 10_000, 1_000)
            .map_err(|_| ServiceError::Internal)?;
        let limits = ServiceLimits::try_new(256 * 1024, 1_000, 1024 * 1024, 1_000, structure)
            .map_err(|_| ServiceError::Internal)?;
        let mut unique = BTreeSet::new();
        for record in &records {
            if !unique.insert(record.definition().instrument_id()) {
                return Err(ServiceError::InvalidRequest);
            }
        }
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        let calendar = self
            .display_history
            .prepare_market_display_calendar(retained_only, deadline, cancellation)
            .await?;
        let mut first_failure = None;
        for record in &records {
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            if runtime.is_revoked() {
                return Err(ServiceError::Unavailable);
            }
            let deadline = Instant::now()
                .checked_add(super::super::LOCAL_RECOVERY_TIMEOUT)
                .ok_or(ServiceError::Internal)?;
            let context = RequestContext::new(
                RequestId::String(Arc::from("source.starter-market-history")),
                cancellation.child_token(),
                deadline,
                limits,
            );
            let result = self
                .display_history
                .prepare_market_display_history(
                    runtime,
                    record,
                    retained_only,
                    calendar.as_ref(),
                    &context,
                )
                .await;
            if cancellation.is_cancelled() || result == Err(ServiceError::Cancelled) {
                return Err(ServiceError::Cancelled);
            }
            if runtime.is_revoked() {
                return Err(ServiceError::Unavailable);
            }
            if let Err(error) = result {
                // An instrument's exhausted window does not consume the next one's budget.
                // The existing operation has returned and drained before another is admitted.
                first_failure.get_or_insert(error);
            }
        }
        // Only this owner publishes the locator, after actual current-coverage selection and
        // preparation. An unrelated historical calendar publication cannot replace it.
        if let Some(calendar) = calendar {
            self.research.set_market_display_calendar_origin(
                calendar.source_action_calendar().manifest().clone(),
            );
        }
        first_failure.map_or(Ok(()), Err)
    }
}

fn record_history_outcome(result: Result<(), ServiceError>) {
    if let Err(error) = result
        && error != ServiceError::Cancelled
    {
        tracing::warn!(?error, "starter market history preparation is unavailable");
    }
}

/// The Alpaca display/history owner uses the admitted provider's New York civil-day rule.
/// This timer only schedules finite preparation; it does not assign financial session dates.
fn display_native_day(at: Timestamp) -> Result<(chrono::NaiveDate, Duration), ServiceError> {
    let local = DateTime::<Utc>::from_timestamp_nanos(at.unix_nanos()).with_timezone(&New_York);
    let day = local.date_naive();
    let next = day
        .succ_opt()
        .and_then(|day| day.and_hms_opt(0, 0, 0))
        .and_then(|midnight| midnight.and_local_timezone(New_York).single())
        .and_then(|midnight| midnight.timestamp_nanos_opt())
        .ok_or(ServiceError::Internal)?;
    let remaining = next
        .checked_sub(at.unix_nanos())
        .and_then(|nanos| u64::try_from(nanos).ok())
        .filter(|nanos| *nanos > 0)
        .ok_or(ServiceError::Internal)?;
    Ok((day, Duration::from_nanos(remaining)))
}

fn display_native_day_now() -> Result<(chrono::NaiveDate, Duration), ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .ok_or(ServiceError::Internal)?;
    display_native_day(Timestamp::from_unix_nanos(nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_refresh_uses_native_midnight_and_dst_without_reconnect()
    -> Result<(), Box<dyn std::error::Error>> {
        let at = |text: &str| -> Result<Timestamp, Box<dyn std::error::Error>> {
            Ok(Timestamp::from_unix_nanos(
                DateTime::parse_from_rfc3339(text)?
                    .timestamp_nanos_opt()
                    .ok_or("timestamp")?,
            ))
        };
        let (friday, wait) = display_native_day(at("2026-10-03T00:00:00Z")?)?;
        assert_eq!(friday.to_string(), "2026-10-02");
        assert_eq!(wait, Duration::from_secs(4 * 3600));
        let (saturday, _) = display_native_day(at("2026-10-03T04:00:00Z")?)?;
        assert_ne!(friday, saturday); // The same runtime generation admits the new native day.
        assert_eq!(
            display_native_day(at("2026-03-08T05:00:00Z")?)?.1,
            Duration::from_secs(23 * 3600)
        );
        assert_eq!(
            display_native_day(at("2026-11-01T04:00:00Z")?)?.1,
            Duration::from_secs(25 * 3600)
        );
        Ok(())
    }
}
