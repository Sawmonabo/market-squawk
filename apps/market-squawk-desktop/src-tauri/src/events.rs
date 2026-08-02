//! Window-scoped, generation-aware service event forwarding.

use std::time::Duration;

use market_squawk_runtime::{ApplicationClient, EventCursor, EventPageLimit};
use serde_json::Value;
use tauri::{State, ipc::Channel};

use crate::{
    bridge::DesktopState,
    contracts::{DesktopCommandError, DesktopEvent},
};

const EVENT_PAGE_LIMIT: usize = 128;
const EMPTY_POLL_INTERVAL: Duration = Duration::from_millis(500);

#[tauri::command]
pub(crate) fn subscribe_service_events(
    on_event: Channel<DesktopEvent>,
    state: State<'_, DesktopState>,
) -> Result<(), DesktopCommandError> {
    let application = state.application();
    let runtime = state.runtime();
    let cancellation = state.cancellation();
    let operations = state.service_operation_index();
    tauri::async_runtime::spawn(async move {
        let limit = match EventPageLimit::try_new(EVENT_PAGE_LIMIT) {
            Ok(limit) => limit,
            Err(_error) => return,
        };
        let mut cursor: Option<EventCursor> = None;
        loop {
            let previous_sequence = cursor.as_ref().map_or(0, EventCursor::sequence);
            let page = application
                .read_events(cursor, limit, cancellation.child_token())
                .await;
            let (values, next) = match page {
                Ok(page) => page,
                Err(_error) => {
                    let _ = on_event.send(DesktopEvent::resync_required(
                        runtime,
                        previous_sequence,
                        "service_event_stream_changed",
                    ));
                    return;
                }
            };
            for (offset, value) in values.iter().enumerate() {
                let Ok(offset) = u64::try_from(offset) else {
                    return;
                };
                let Some(sequence) = previous_sequence
                    .checked_add(offset)
                    .and_then(|value| value.checked_add(1))
                else {
                    let _ = on_event.send(DesktopEvent::resync_required(
                        runtime,
                        previous_sequence,
                        "service_event_sequence_exhausted",
                    ));
                    return;
                };
                let Some(event) = authority_changed(runtime, sequence, value, &operations) else {
                    let _ = on_event.send(DesktopEvent::resync_required(
                        runtime,
                        previous_sequence,
                        "invalid_service_event",
                    ));
                    return;
                };
                if on_event.send(event).is_err() {
                    return;
                }
            }
            let empty = values.is_empty();
            cursor = Some(next);
            if empty {
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(EMPTY_POLL_INTERVAL) => {}
                }
            }
        }
    });
    Ok(())
}

fn authority_changed(
    runtime: market_squawk_runtime::RuntimeIdentity,
    sequence: u64,
    value: &Value,
    operations: &std::collections::BTreeMap<String, String>,
) -> Option<DesktopEvent> {
    let object = value.as_object()?;
    if object.len() != 3 || object.get("type")?.as_str()? != "application.changed" {
        return None;
    }
    let operation = object.get("operation")?.as_str()?;
    let domain = operations.get(operation)?.clone();
    let request_id = object.get("requestId")?.as_str()?;
    if request_id.is_empty() {
        return None;
    }
    Some(DesktopEvent::authority_changed(
        runtime,
        sequence,
        domain,
        operation.to_owned(),
        request_id.to_owned(),
    ))
}
