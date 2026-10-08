//! Authenticated, body-owned delivery from the existing shared event journal.

use super::*;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream;
use std::convert::Infallible;

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SubscriptionRequest {
    cursor: Option<EventCursor>,
    limit: usize,
}

struct Subscription {
    state: Arc<RouterState>,
    authentication: AuthenticatedClient,
    _admission: tokio::sync::OwnedSemaphorePermit,
    changes: tokio::sync::watch::Receiver<()>,
    cursor: Option<EventCursor>,
    limit: EventPageLimit,
    pending: Option<crate::EventPage>,
    finished: bool,
    drain: bool,
}

pub(super) async fn subscribe_events(
    State(state): State<Arc<RouterState>>,
    request: Request<Body>,
) -> Response {
    let authentication =
        match authenticate_transport(&state, &request, Method::POST, Some(JSON_MEDIA_TYPE)) {
            Ok(value) => value,
            Err(status) => return rejected(status),
        };
    let admission = match Arc::clone(&state.event_subscriptions).try_acquire_owned() {
        Ok(value) => value,
        Err(_) => return rejected(StatusCode::SERVICE_UNAVAILABLE),
    };
    let body = match to_bytes(request.into_body(), state.limits.event_request_bytes.get()).await {
        Ok(body) => body,
        Err(_) => return rejected(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let request: SubscriptionRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    let limit = match EventPageLimit::try_new(request.limit) {
        Ok(limit) => limit,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    // Subscribe before the initial snapshot. A publication racing with it remains observable.
    let changes = state.events.subscribe();
    let mut subscription = Subscription {
        state,
        authentication,
        _admission: admission,
        changes,
        cursor: request.cursor,
        limit,
        pending: None,
        finished: false,
        drain: false,
    };
    subscription.pending = match subscription.read_page() {
        Ok(page) => Some(page),
        Err(_) => return rejected(StatusCode::GONE),
    };
    let stream = stream::unfold(subscription, |mut subscription| async move {
        if subscription.finished {
            return None;
        }
        loop {
            if subscription.state.request_cancellation.is_cancelled()
                || subscription.authentication.lifetime.is_cancelled()
            {
                return None;
            }
            if let Some(page) = subscription.pending.take() {
                let encoded = match serde_json::to_string(&page) {
                    Ok(value)
                        if value.len() <= subscription.state.limits.response_body_bytes.get() =>
                    {
                        value
                    }
                    _ => {
                        subscription.finished = true;
                        return Some((Ok::<_, Infallible>(resync()), subscription));
                    }
                };
                subscription.cursor = Some(page.cursor().clone());
                subscription.drain =
                    page.snapshot_required() || page.events().len() == subscription.limit.get();
                return Some((
                    Ok(Event::default().event("application.events").data(encoded)),
                    subscription,
                ));
            }
            if subscription.drain {
                subscription.drain = false;
                subscription.pending = match subscription.read_page() {
                    Ok(page) => Some(page),
                    Err(_) => {
                        subscription.finished = true;
                        return Some((Ok(resync()), subscription));
                    }
                };
                continue;
            }
            // Refresh the cursor before expiry even without data; this also detects an idle
            // disconnected peer. It does not query providers or poll financial data.
            let renewal = subscription.state.limits.event_cursor_lifetime / 2;
            tokio::select! {
                biased;
                () = subscription.state.request_cancellation.cancelled() => return None,
                () = subscription.authentication.lifetime.cancelled() => return None,
                result = subscription.changes.changed() => {
                    if result.is_err() { return None; }
                }
                () = tokio::time::sleep(renewal.max(Duration::from_millis(1))) => {}
            }
            subscription.pending = match subscription.read_page() {
                Ok(page) => Some(page),
                Err(_) => {
                    subscription.finished = true;
                    return Some((Ok(resync()), subscription));
                }
            };
        }
    });
    // Standard SSE comments keep an otherwise idle local socket observable. The stream itself
    // owns authentication/activity and admission through response EOF/drop, not handler return.
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

impl Subscription {
    fn read_page(&self) -> Result<crate::EventPage, RouterError> {
        let now = wall_now()?;
        let expires_at = add_duration(now, self.state.limits.event_cursor_lifetime)?;
        match self.state.events.read_after(
            self.authentication.client_id,
            self.cursor.as_ref(),
            self.limit,
            now,
            expires_at,
        ) {
            Ok(page) => Ok(page),
            Err(
                crate::EventReadError::SequenceGap { .. }
                | crate::EventReadError::Cursor(crate::EventCursorError::Expired),
            ) => self
                .state
                .events
                .snapshot_page(self.authentication.client_id, expires_at)
                .map_err(|_| RouterError::Unavailable),
            Err(_) => Err(RouterError::Unavailable),
        }
    }
}

fn resync() -> Event {
    Event::default()
        .event("application.resync_required")
        .data("{}")
}
