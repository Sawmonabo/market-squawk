//! Window-scoped, generation-aware service event forwarding.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use market_squawk_runtime::{
    ApplicationClient, ApplicationClientError, EventCursor, EventPageLimit,
    LoopbackApplicationClient, RuntimeIdentity,
};
use serde_json::Value;
use tauri::{State, Window, async_runtime::JoinHandle, ipc::Channel};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    bridge::DesktopState,
    contracts::{
        DesktopCommandError, DesktopEvent, DesktopEventSubscriptionReceipt,
        DesktopEventSubscriptionRequest, ProductSessionToken,
    },
};

const EVENT_PAGE_LIMIT: usize = 128;
const EMPTY_POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Default)]
pub(crate) struct DesktopEventSubscriptions {
    inner: tokio::sync::Mutex<SubscriptionRegistry>,
}

#[derive(Debug, Default)]
struct SubscriptionRegistry {
    active: Option<ActiveSubscription>,
    retained: Option<RetainedCursor>,
}

#[derive(Debug)]
struct ActiveSubscription {
    id: Uuid,
    runtime: RuntimeIdentity,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
    cursor: Arc<Mutex<Option<EventCursor>>>,
}

#[derive(Clone, Debug)]
struct RetainedCursor {
    runtime: RuntimeIdentity,
    cursor: EventCursor,
}

#[tauri::command]
pub(crate) async fn subscribe_service_events(
    request: DesktopEventSubscriptionRequest,
    on_event: Channel<DesktopEvent>,
    window: Window,
    state: State<'_, DesktopState>,
    subscriptions: State<'_, DesktopEventSubscriptions>,
) -> Result<DesktopEventSubscriptionReceipt, DesktopCommandError> {
    if window.label() != "main" {
        return Err(DesktopCommandError::new(
            "event_subscription_rejected",
            "The event subscription does not belong to this window and service generation.",
        ));
    }
    let limit = EventPageLimit::try_new(EVENT_PAGE_LIMIT)
        .map_err(|_error| DesktopCommandError::internal())?;
    let subscription_id = Uuid::new_v4();
    let generation = state.current_generation()?;
    if request.product_session_token() != generation.product_session_token() {
        return Err(DesktopCommandError::new(
            "event_subscription_rejected",
            "The event subscription does not belong to this window and service generation.",
        ));
    }
    let runtime = generation.runtime();
    let product_session_token = generation.product_session_token();
    let requested_sequence = request.after_sequence();
    let mut registry = subscriptions.inner.lock().await;
    state.admit_current(&generation)?;
    stop_active_subscription(&mut registry, None).await;

    let resumed_cursor = registry
        .retained
        .as_ref()
        .filter(|retained| {
            retained.runtime == runtime && retained.cursor.sequence() == requested_sequence
        })
        .map(|retained| retained.cursor.clone());
    let resumed = requested_sequence > 0 && resumed_cursor.is_some();
    if requested_sequence > 0 && !resumed {
        registry.retained = None;
        let _ = on_event.send(DesktopEvent::resync_required(
            product_session_token,
            requested_sequence,
            "service_event_cursor_unavailable",
        ));
        return Ok(DesktopEventSubscriptionReceipt::new(
            subscription_id,
            product_session_token,
            requested_sequence,
            false,
        ));
    }

    if requested_sequence == 0 {
        registry.retained = None;
    }
    let cursor = Arc::new(Mutex::new(resumed_cursor));
    let cancellation = generation.cancellation();
    let task = tauri::async_runtime::spawn(forward_service_events(
        generation.application(),
        runtime,
        product_session_token,
        generation.service_operation_index(),
        limit,
        Arc::clone(&cursor),
        cancellation.clone(),
        on_event,
    ));
    registry.active = Some(ActiveSubscription {
        id: subscription_id,
        runtime,
        cancellation,
        task,
        cursor,
    });
    if let Err(error) = state.admit_current(&generation) {
        stop_active_subscription(&mut registry, Some(subscription_id)).await;
        registry.retained = None;
        return Err(error);
    }
    let receipt = DesktopEventSubscriptionReceipt::new(
        subscription_id,
        product_session_token,
        requested_sequence,
        resumed,
    );
    if let Err(error) =
        state.acknowledge_webview_product_session(&generation, product_session_token)
    {
        stop_active_subscription(&mut registry, Some(subscription_id)).await;
        registry.retained = None;
        return Err(error);
    }
    Ok(receipt)
}

impl DesktopEventSubscriptions {
    pub(crate) async fn stop_clear_and_replace<T>(
        &self,
        replace: impl FnOnce() -> Result<T, DesktopCommandError>,
    ) -> Result<T, DesktopCommandError> {
        let mut registry = self.inner.lock().await;
        stop_active_subscription(&mut registry, None).await;
        registry.retained = None;
        replace()
    }
}

#[tauri::command]
pub(crate) async fn unsubscribe_service_events(
    subscription_id: Uuid,
    subscriptions: State<'_, DesktopEventSubscriptions>,
) -> Result<(), DesktopCommandError> {
    let mut registry = subscriptions.inner.lock().await;
    stop_active_subscription(&mut registry, Some(subscription_id)).await;
    Ok(())
}

async fn stop_active_subscription(registry: &mut SubscriptionRegistry, expected_id: Option<Uuid>) {
    let Some(active) = registry.active.as_ref() else {
        return;
    };
    if expected_id.is_some_and(|expected| expected != active.id) {
        return;
    }
    let Some(active) = registry.active.take() else {
        return;
    };
    active.cancellation.cancel();
    let _ = active.task.await;
    registry.retained = active
        .cursor
        .lock()
        .ok()
        .and_then(|cursor| cursor.clone())
        .map(|cursor| RetainedCursor {
            runtime: active.runtime,
            cursor,
        });
}

async fn forward_service_events(
    application: Arc<LoopbackApplicationClient>,
    runtime: RuntimeIdentity,
    product_session_token: ProductSessionToken,
    operations: BTreeMap<String, String>,
    limit: EventPageLimit,
    retained_cursor: Arc<Mutex<Option<EventCursor>>>,
    cancellation: CancellationToken,
    on_event: Channel<DesktopEvent>,
) {
    let mut cursor = match retained_cursor.lock() {
        Ok(cursor) => cursor.clone(),
        Err(_error) => return,
    };
    loop {
        let previous_sequence = cursor.as_ref().map_or(0, EventCursor::sequence);
        let page = application
            .read_events(cursor.clone(), limit, cancellation.child_token())
            .await;
        let (values, next) = match page {
            Ok(page) => page,
            Err(ApplicationClientError::Interrupted) if cancellation.is_cancelled() => return,
            Err(ApplicationClientError::Unavailable | ApplicationClientError::Interrupted) => {
                let _ = on_event.send(DesktopEvent::stream_disconnected(
                    product_session_token,
                    previous_sequence,
                    "service_event_stream_unavailable",
                ));
                return;
            }
            Err(ApplicationClientError::Rejected | ApplicationClientError::InvalidResponse) => {
                let _ = on_event.send(DesktopEvent::resync_required(
                    product_session_token,
                    previous_sequence,
                    "service_event_stream_changed",
                ));
                return;
            }
        };
        for (offset, value) in values.iter().enumerate() {
            if cancellation.is_cancelled() {
                return;
            }
            let Ok(offset) = u64::try_from(offset) else {
                return;
            };
            let Some(sequence) = previous_sequence
                .checked_add(offset)
                .and_then(|value| value.checked_add(1))
            else {
                let _ = on_event.send(DesktopEvent::resync_required(
                    product_session_token,
                    previous_sequence,
                    "service_event_sequence_exhausted",
                ));
                return;
            };
            let Some(event) =
                authority_changed(product_session_token, sequence, value, &operations)
            else {
                let _ = on_event.send(DesktopEvent::resync_required(
                    product_session_token,
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
        let cursor_retained = match retained_cursor.lock() {
            Ok(mut retained) => {
                *retained = cursor.clone();
                true
            }
            Err(_error) => false,
        };
        if !cursor_retained {
            let _ = on_event.send(DesktopEvent::resync_required(
                product_session_token,
                previous_sequence,
                "service_event_cursor_unavailable",
            ));
            return;
        }
        if empty {
            tokio::select! {
                () = cancellation.cancelled() => return,
                () = tokio::time::sleep(EMPTY_POLL_INTERVAL) => {}
            }
        }
    }
}

fn authority_changed(
    product_session_token: ProductSessionToken,
    sequence: u64,
    value: &Value,
    operations: &BTreeMap<String, String>,
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
        product_session_token,
        sequence,
        domain,
        operation.to_owned(),
        request_id.to_owned(),
    ))
}
