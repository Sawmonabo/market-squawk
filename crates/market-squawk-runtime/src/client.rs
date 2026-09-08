//! Authenticated loopback implementation of the native application client.

use std::{
    fmt, io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::{StreamExt as _, stream};
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_platform::SecretValue;
use market_squawk_services::{JsonStructureLimits, RequestId, validate_json_contract};
use reqwest::{Client, Method, Response, redirect::Policy};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{
    AppRequestEnvelope, AppResponseEnvelope, ApplicationClient, ApplicationClientError,
    ApplicationRequestScope, CLIENT_ID_HEADER, CREDENTIAL_GENERATION_HEADER, EventCursor,
    EventPage, EventPageLimit, INPUT_LENGTH_HEADER, INPUT_MEDIA_TYPE_HEADER, INPUT_SHA256_HEADER,
    INSTALLATION_ID_HEADER, InputAdmission, InputTicket, RendezvousRecord,
    SERVICE_GENERATION_HEADER, WORKSPACE_ID_HEADER,
};

const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Native client that resolves one authenticated, generation-bound loopback service.
pub struct LoopbackApplicationClient {
    http: Client,
    endpoint: String,
    host: String,
    scope: ApplicationRequestScope,
    credential: Arc<SecretValue>,
    origin: Option<Box<str>>,
    maximum_response_bytes: usize,
    response_structure: JsonStructureLimits,
    transport_timeout: Duration,
}

impl fmt::Debug for LoopbackApplicationClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoopbackApplicationClient")
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .field("credential", &"[REDACTED]")
            .field("origin", &self.origin)
            .field("maximum_response_bytes", &self.maximum_response_bytes)
            .field("response_structure", &self.response_structure)
            .field("transport_timeout", &self.transport_timeout)
            .finish()
    }
}

impl LoopbackApplicationClient {
    /// Creates a no-proxy, no-redirect client bound to one authenticated rendezvous generation.
    pub fn try_new(
        rendezvous: &RendezvousRecord,
        scope: ApplicationRequestScope,
        credential: SecretValue,
        origin: Option<String>,
        maximum_response_bytes: usize,
        response_structure: JsonStructureLimits,
        transport_timeout: Duration,
    ) -> Result<Self, ApplicationClientError> {
        if rendezvous.runtime() != scope.runtime()
            || !rendezvous
                .protocols()
                .contains(crate::ApplicationProtocolVersion::V1)
            || maximum_response_bytes == 0
            || transport_timeout.is_zero()
            || origin
                .as_ref()
                .is_some_and(|value| value.is_empty() || value == "*")
        {
            return Err(ApplicationClientError::Rejected);
        }
        let host = rendezvous.endpoint().to_string();
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(transport_timeout)
            .build()
            .map_err(|_| ApplicationClientError::Unavailable)?;
        Ok(Self {
            http,
            endpoint: format!("http://{host}"),
            host,
            scope,
            credential: Arc::new(credential),
            origin: origin.map(String::into_boxed_str),
            maximum_response_bytes,
            response_structure,
            transport_timeout,
        })
    }

    /// Bound request factory for advanced relay and CLI adapters.
    #[must_use]
    pub const fn request_scope(&self) -> &ApplicationRequestScope {
        &self.scope
    }

    /// Performs an authenticated application-route readiness probe for the exact runtime
    /// generation before its rendezvous record is published.
    pub async fn probe_ready(
        &self,
        cancellation: CancellationToken,
    ) -> Result<(), ApplicationClientError> {
        let exchange = async {
            let response = self
                .request(Method::GET, "/health")
                .send()
                .await
                .map_err(|_| ApplicationClientError::Unavailable)?;
            let bytes = self.response_bytes(response, &cancellation).await?;
            validate_json_contract(
                &serde_json::from_slice::<Value>(&bytes)
                    .map_err(|_| ApplicationClientError::InvalidResponse)?,
                self.response_structure,
                self.maximum_response_bytes,
            )
            .map_err(|_| ApplicationClientError::InvalidResponse)?;
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            let expected_runtime = serde_json::to_value(self.scope.runtime())
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            if value.get("status").and_then(Value::as_str) != Some("ready")
                || value.get("runtime") != Some(&expected_runtime)
            {
                return Err(ApplicationClientError::InvalidResponse);
            }
            Ok(())
        };
        tokio::select! {
            () = cancellation.cancelled() => Err(ApplicationClientError::Interrupted),
            result = tokio::time::timeout(self.transport_timeout, exchange) => {
                result.map_err(|_| ApplicationClientError::Interrupted)?
            }
        }
    }

    /// Reads the bounded, non-secret installed-product bootstrap snapshot.
    pub async fn bootstrap(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Value, ApplicationClientError> {
        let exchange = async {
            let response = self
                .request(Method::GET, "/app/v1/bootstrap")
                .send()
                .await
                .map_err(|_| ApplicationClientError::Unavailable)?;
            let bytes = self.response_bytes(response, &cancellation).await?;
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            validate_json_contract(&value, self.response_structure, self.maximum_response_bytes)
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            Ok(value)
        };
        tokio::select! {
            () = cancellation.cancelled() => Err(ApplicationClientError::Interrupted),
            result = tokio::time::timeout(self.transport_timeout, exchange) => {
                result.map_err(|_| ApplicationClientError::Interrupted)?
            }
        }
    }

    /// Builds and invokes one bounded operation without exposing domain/time construction to a
    /// native presentation client.
    pub async fn invoke_operation(
        &self,
        request_id: RequestId,
        operation: &str,
        arguments: Value,
        lifetime: Duration,
        cancellation: CancellationToken,
    ) -> Result<AppResponseEnvelope, ApplicationClientError> {
        if lifetime.is_zero() || lifetime > self.transport_timeout {
            return Err(ApplicationClientError::Rejected);
        }
        let now = client_wall_now()?;
        let nanos = i64::try_from(lifetime.as_nanos())
            .map_err(|_error| ApplicationClientError::Rejected)?;
        let deadline = now
            .checked_add_nanos(nanos)
            .map_err(|_error| ApplicationClientError::Rejected)?;
        let operation = SourceIdentifier::try_from(operation)
            .map_err(|_error| ApplicationClientError::Rejected)?;
        let request = self
            .scope
            .request(request_id, deadline, now, operation, arguments)
            .map_err(|_error| ApplicationClientError::Rejected)?;
        self.invoke(request, cancellation).await
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.endpoint))
            .header(reqwest::header::HOST, &self.host)
            .header(
                CLIENT_ID_HEADER,
                self.scope.client_id().as_uuid().to_string(),
            )
            .header(
                INSTALLATION_ID_HEADER,
                self.scope.runtime().installation_id().as_uuid().to_string(),
            )
            .header(
                WORKSPACE_ID_HEADER,
                self.scope.runtime().workspace_id().as_uuid().to_string(),
            )
            .header(
                SERVICE_GENERATION_HEADER,
                self.scope.runtime().service_generation().get(),
            )
            .header(
                CREDENTIAL_GENERATION_HEADER,
                self.scope.credential_generation().get(),
            )
            .bearer_auth(self.credential.expose_secret());
        if let Some(origin) = &self.origin {
            request = request.header(reqwest::header::ORIGIN, origin.as_ref());
        }
        request
    }

    async fn response_bytes(
        &self,
        response: Response,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, ApplicationClientError> {
        if !response.status().is_success() {
            return Err(ApplicationClientError::Rejected);
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = tokio::select! {
            _ = cancellation.cancelled() => return Err(ApplicationClientError::Interrupted),
            chunk = stream.next() => chunk,
        } {
            let chunk = chunk.map_err(|_| ApplicationClientError::Unavailable)?;
            let next = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(ApplicationClientError::InvalidResponse)?;
            if next > self.maximum_response_bytes {
                return Err(ApplicationClientError::InvalidResponse);
            }
            bytes
                .try_reserve(chunk.len())
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

#[async_trait]
impl ApplicationClient for LoopbackApplicationClient {
    async fn invoke(
        &self,
        request: AppRequestEnvelope,
        cancellation: CancellationToken,
    ) -> Result<AppResponseEnvelope, ApplicationClientError> {
        let expected_request = request.request_id().clone();
        let expected_generation = request.service_generation();
        let request_timeout = request
            .remaining_lifetime(client_wall_now()?)
            .map_err(|_| ApplicationClientError::Interrupted)?
            .min(self.transport_timeout);
        let exchange = async {
            let response = self
                .request(Method::POST, "/app/v1/invoke")
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .json(&request)
                .send()
                .await
                .map_err(|_| ApplicationClientError::Unavailable)?;
            let bytes = self.response_bytes(response, &cancellation).await?;
            AppResponseEnvelope::decode_expected(
                &bytes,
                &expected_request,
                expected_generation,
                self.response_structure,
                self.maximum_response_bytes,
            )
            .map_err(|_| ApplicationClientError::InvalidResponse)
        };
        tokio::select! {
            _ = cancellation.cancelled() => return Err(ApplicationClientError::Interrupted),
            result = tokio::time::timeout(request_timeout, exchange) => {
                result.map_err(|_| ApplicationClientError::Interrupted)?
            }
        }
    }

    async fn stage_input(
        &self,
        admission: InputAdmission,
        input: &mut (dyn AsyncRead + Send + Unpin),
        cancellation: CancellationToken,
    ) -> Result<InputTicket, ApplicationClientError> {
        let (sender, receiver) = mpsc::channel::<Result<Vec<u8>, io::Error>>(2);
        let body_stream = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|chunk| (chunk, receiver))
        });
        let digest = encode_hex(admission.expected_digest().bytes());
        let send = self
            .request(Method::POST, "/app/v1/inputs")
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .header(reqwest::header::CONTENT_LENGTH, admission.expected_bytes())
            .header(INPUT_MEDIA_TYPE_HEADER, admission.media_type().as_str())
            .header(INPUT_LENGTH_HEADER, admission.expected_bytes())
            .header(INPUT_SHA256_HEADER, digest)
            .body(reqwest::Body::wrap_stream(body_stream))
            .send();
        let pump = pump_input(input, admission.expected_bytes(), sender, &cancellation);
        let transfer = async {
            let (response, pumped) = tokio::join!(send, pump);
            pumped?;
            response.map_err(|_| ApplicationClientError::Unavailable)
        };
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(ApplicationClientError::Interrupted),
            result = tokio::time::timeout(self.transport_timeout, transfer) => {
                result.map_err(|_| ApplicationClientError::Interrupted)??
            }
        };
        let bytes = self.response_bytes(response, &cancellation).await?;
        let now = client_wall_now()?;
        InputTicket::decode_expected(
            &bytes,
            self.scope.runtime(),
            self.scope.client_id(),
            &admission,
            now,
            self.maximum_response_bytes,
        )
        .map_err(|_| ApplicationClientError::InvalidResponse)
    }

    async fn read_events(
        &self,
        cursor: Option<EventCursor>,
        limit: EventPageLimit,
        cancellation: CancellationToken,
    ) -> Result<(Arc<[Value]>, EventCursor), ApplicationClientError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Request<'a> {
            cursor: &'a Option<EventCursor>,
            limit: usize,
        }
        let send = self
            .request(Method::POST, "/app/v1/events")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&Request {
                cursor: &cursor,
                limit: limit.get(),
            })
            .send();
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(ApplicationClientError::Interrupted),
            response = send => response.map_err(|_| ApplicationClientError::Unavailable)?,
        };
        let bytes = self.response_bytes(response, &cancellation).await?;
        let page: EventPage =
            serde_json::from_slice(&bytes).map_err(|_| ApplicationClientError::InvalidResponse)?;
        let now = client_wall_now()?;
        let expected_generation = self.scope.runtime().service_generation();
        page.cursor()
            .ensure_current(self.scope.client_id(), expected_generation, now)
            .map_err(|_| ApplicationClientError::InvalidResponse)?;
        if page.events().len() > limit.get() {
            return Err(ApplicationClientError::InvalidResponse);
        }
        let mut expected_sequence = cursor.as_ref().map_or(0, EventCursor::sequence);
        let mut values = Vec::with_capacity(page.events().len());
        for event in page.events().iter() {
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(ApplicationClientError::InvalidResponse)?;
            if event.generation() != expected_generation || event.sequence() != expected_sequence {
                return Err(ApplicationClientError::InvalidResponse);
            }
            validate_json_contract(
                event.payload(),
                self.response_structure,
                self.maximum_response_bytes,
            )
            .map_err(|_| ApplicationClientError::InvalidResponse)?;
            values.push(event.payload().clone());
        }
        if page.cursor().sequence() != expected_sequence {
            return Err(ApplicationClientError::InvalidResponse);
        }
        let values: Arc<[Value]> = values.into();
        Ok((values, page.cursor().clone()))
    }
}

async fn pump_input(
    input: &mut (dyn AsyncRead + Send + Unpin),
    expected_bytes: u64,
    sender: mpsc::Sender<Result<Vec<u8>, io::Error>>,
    cancellation: &CancellationToken,
) -> Result<(), ApplicationClientError> {
    let mut sent = 0_u64;
    loop {
        let remaining = expected_bytes.saturating_sub(sent);
        if remaining == 0 {
            let mut probe = [0_u8; 1];
            let read = tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(ApplicationClientError::Interrupted);
                }
                read = input.read(&mut probe) => {
                    read.map_err(|_| ApplicationClientError::Rejected)?
                }
            };
            if read != 0 {
                return Err(ApplicationClientError::Rejected);
            }
            break;
        }
        let capacity = usize::try_from(remaining.min(STREAM_CHUNK_BYTES as u64))
            .map_err(|_| ApplicationClientError::Rejected)?;
        let mut chunk = vec![0_u8; capacity];
        let read = tokio::select! {
            _ = cancellation.cancelled() => return Err(ApplicationClientError::Interrupted),
            read = input.read(&mut chunk) => read.map_err(|_| ApplicationClientError::Rejected)?,
        };
        if read == 0 {
            return Err(ApplicationClientError::Rejected);
        }
        chunk.truncate(read);
        sent = sent
            .checked_add(u64::try_from(read).map_err(|_| ApplicationClientError::Rejected)?)
            .ok_or(ApplicationClientError::Rejected)?;
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(ApplicationClientError::Interrupted);
            }
            result = sender.send(Ok(chunk)) => {
                result.map_err(|_| ApplicationClientError::Unavailable)?;
            }
        }
    }
    Ok(())
}

fn client_wall_now() -> Result<Timestamp, ApplicationClientError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApplicationClientError::Unavailable)?;
    let nanos =
        i64::try_from(duration.as_nanos()).map_err(|_| ApplicationClientError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
