//! Bounded SSE decoding with the same exact journal cursor checks as ordinary event reads.

use super::*;
use crate::EventPage;
use futures_util::stream::BoxStream;
use serde::Serialize;

/// One authenticated event response. Dropping it releases this subscriber's connection.
pub struct ApplicationEventSubscription {
    events: Option<BoxStream<'static, Result<sse_stream::Sse, ApplicationClientError>>>,
    scope: ApplicationRequestScope,
    cursor: Option<EventCursor>,
    limit: EventPageLimit,
    maximum_response_bytes: usize,
    response_structure: JsonStructureLimits,
    cancellation: CancellationToken,
}

impl fmt::Debug for ApplicationEventSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationEventSubscription")
            .field("cursor", &self.cursor)
            .field("closed", &self.events.is_none())
            .finish_non_exhaustive()
    }
}

impl ApplicationEventSubscription {
    pub(super) async fn open(
        client: &LoopbackApplicationClient,
        cursor: Option<EventCursor>,
        limit: EventPageLimit,
        cancellation: CancellationToken,
    ) -> Result<Self, ApplicationClientError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Request<'a> {
            cursor: &'a Option<EventCursor>,
            limit: usize,
        }
        let send = client
            .authenticated_request(Method::POST, "/app/v1/events")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&Request {
                cursor: &cursor,
                limit: limit.get(),
            })
            .send();
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ApplicationClientError::Interrupted),
            response = tokio::time::timeout(client.transport_timeout, send) => {
                response.map_err(|_| ApplicationClientError::Interrupted)?
                    .map_err(|_| ApplicationClientError::Unavailable)?
            }
        };
        if !response.status().is_success() {
            return Err(ApplicationClientError::Rejected);
        }
        if response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            != Some("text/event-stream")
        {
            return Err(ApplicationClientError::InvalidResponse);
        }
        // Bound bytes before the maintained decoder can accumulate an unfinished line/event.
        // 128 bytes covers our fixed SSE type/data framing in addition to the existing JSON bound.
        let ceiling = client
            .maximum_response_bytes
            .checked_add(128)
            .ok_or(ApplicationClientError::InvalidResponse)?;
        let mut frame = FrameBound::new(ceiling);
        let bytes = response.bytes_stream().map(move |chunk| {
            let bytes = chunk.map_err(|_| ApplicationClientError::Unavailable)?;
            frame.observe(&bytes)?;
            Ok::<_, ApplicationClientError>(bytes)
        });
        let events = sse_stream::SseStream::from_bytes_stream(bytes)
            .map(|event| {
                event.map_err(|error| match error {
                    sse_stream::Error::Body(cause) => cause
                        .downcast_ref::<ApplicationClientError>()
                        .cloned()
                        .unwrap_or(ApplicationClientError::Unavailable),
                    _ => ApplicationClientError::InvalidResponse,
                })
            })
            .boxed();
        Ok(Self {
            events: Some(events),
            scope: client.scope.clone(),
            cursor,
            limit,
            maximum_response_bytes: client.maximum_response_bytes,
            response_structure: client.response_structure,
            cancellation,
        })
    }

    /// Waits for the next committed page or connection-maintenance page; never polls providers.
    /// An explicit snapshot-required page establishes the baseline before reloading durable data.
    /// An error closes the response. Reconnect with the last successfully consumed cursor.
    pub async fn next_page(&mut self) -> Result<EventPage, ApplicationClientError> {
        let result = self.read_page().await;
        if result.is_err() {
            self.events = None;
        }
        result
    }

    async fn read_page(&mut self) -> Result<EventPage, ApplicationClientError> {
        let events = self
            .events
            .as_mut()
            .ok_or(ApplicationClientError::Unavailable)?;
        let event = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(ApplicationClientError::Interrupted),
            event = events.next() => event.ok_or(ApplicationClientError::Unavailable)??,
        };
        if event.event.as_deref() != Some("application.events")
            || event.id.is_some()
            || event.retry.is_some()
        {
            return Err(ApplicationClientError::InvalidResponse);
        }
        let data = event.data.ok_or(ApplicationClientError::InvalidResponse)?;
        if data.len() > self.maximum_response_bytes {
            return Err(ApplicationClientError::InvalidResponse);
        }
        let page: EventPage =
            serde_json::from_str(&data).map_err(|_| ApplicationClientError::InvalidResponse)?;
        let expected_generation = self.scope.runtime().service_generation();
        page.cursor()
            .ensure_current(
                self.scope.client_id(),
                expected_generation,
                client_wall_now()?,
            )
            .map_err(|_| ApplicationClientError::InvalidResponse)?;
        if page.events().len() > self.limit.get() {
            return Err(ApplicationClientError::InvalidResponse);
        }
        let mut sequence = self.cursor.as_ref().map_or(0, EventCursor::sequence);
        if page.snapshot_required() {
            if !page.events().is_empty() || page.cursor().sequence() < sequence {
                return Err(ApplicationClientError::InvalidResponse);
            }
            self.cursor = Some(page.cursor().clone());
            return Ok(page);
        }
        for event in page.events().iter() {
            sequence = sequence
                .checked_add(1)
                .ok_or(ApplicationClientError::InvalidResponse)?;
            if event.generation() != expected_generation || event.sequence() != sequence {
                return Err(ApplicationClientError::InvalidResponse);
            }
            validate_json_contract(
                event.payload(),
                self.response_structure,
                self.maximum_response_bytes,
            )
            .map_err(|_| ApplicationClientError::InvalidResponse)?;
        }
        if page.cursor().sequence() != sequence {
            return Err(ApplicationClientError::InvalidResponse);
        }
        self.cursor = Some(page.cursor().clone());
        Ok(page)
    }
}

/// Size accounting only; SSE syntax/UTF-8/field decoding remains owned by sse-stream.
struct FrameBound {
    maximum: usize,
    bytes: usize,
    line_bytes: usize,
    after_cr: bool,
}

impl FrameBound {
    fn new(maximum: usize) -> Self {
        Self {
            maximum,
            bytes: 0,
            line_bytes: 0,
            after_cr: false,
        }
    }

    fn observe(&mut self, chunk: &[u8]) -> Result<(), ApplicationClientError> {
        for byte in chunk {
            if self.after_cr && *byte == b'\n' {
                self.after_cr = false;
                continue;
            }
            self.after_cr = *byte == b'\r';
            self.bytes = self
                .bytes
                .checked_add(1)
                .ok_or(ApplicationClientError::InvalidResponse)?;
            if self.bytes > self.maximum {
                return Err(ApplicationClientError::InvalidResponse);
            }
            if *byte == b'\n' || *byte == b'\r' {
                if self.line_bytes == 0 {
                    self.bytes = 0;
                }
                self.line_bytes = 0;
            } else {
                self.line_bytes += 1;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unfinished_frames_are_bounded_across_chunks_and_line_endings() {
        for separator in [b"\n".as_slice(), b"\r".as_slice(), b"\r\n".as_slice()] {
            let mut bound = FrameBound::new(24);
            for _ in 0..100 {
                for chunk in [b"data: ok".as_slice(), separator, separator] {
                    for byte in chunk {
                        assert!(bound.observe(&[*byte]).is_ok());
                    }
                }
            }
            assert!(bound.observe(&[b'x'; 24]).is_ok());
            assert_eq!(
                bound.observe(b"x"),
                Err(ApplicationClientError::InvalidResponse)
            );
        }
        let mut bound = FrameBound::new(24);
        for _ in 0..3 {
            assert!(bound.observe(b"data: x\n").is_ok());
        }
        assert_eq!(
            bound.observe(b"data: x\n"),
            Err(ApplicationClientError::InvalidResponse)
        );
    }
}
