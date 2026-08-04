//! Service-lifetime, owner-authenticated admission for fresh installed native clients.

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

use std::{
    fs::{self, OpenOptions},
    io::{Read as _, Write as _},
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use getrandom::fill as fill_random;
use market_squawk_platform::{LocalPaths, SecretStore, SecretValue};
use market_squawk_runtime::{
    ApplicationProtocolVersion, ClientCredentialRegistration, ClientId, CredentialGeneration,
    NamedClient, ProcessIdentity, ProcessIdentityVerifier as _, RendezvousRecord, RuntimeIdentity,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::{Zeroize as _, Zeroizing};

use super::{
    InstalledServiceError, SystemProcessIdentityVerifier, mcp_control::InstalledMcpControl,
};

#[cfg(unix)]
use self::unix as platform;
#[cfg(windows)]
use self::windows as platform;

const ADMISSION_DIRECTORY: &str = "installed-service/admission";
const METADATA_FILE: &str = "state";
const METADATA_TEMP_FILE: &str = ".state.tmp";
const METADATA_MAGIC: &[u8; 4] = b"MSQA";
const PREFACE: &[u8; 8] = b"MSQA\0\x01\0\0";
const PROTOCOL_VERSION: u16 = 1;
const METADATA_BYTES: usize = 90;
const REQUEST_BYTES: usize = 107;
const RESPONSE_FIXED_BYTES: usize = 133;
const MAXIMUM_FRAME_BYTES: usize = 64 * 1024;
const MAXIMUM_CREDENTIAL_BYTES: usize = 4 * 1024;
const MAXIMUM_CONNECTIONS: usize = 16;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const TRAILING_DATA_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdmissionMetadata {
    runtime: RuntimeIdentity,
    process: ProcessIdentity,
    endpoint_key: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
struct AdmissionRequest {
    client: NamedClient,
    request_nonce: [u8; 16],
    deadline: Instant,
}

pub(super) struct AdmittedRuntimeClient {
    pub(super) record: RendezvousRecord,
    pub(super) client_id: ClientId,
    pub(super) generation: CredentialGeneration,
    pub(super) credential: SecretValue,
}

impl std::fmt::Debug for AdmittedRuntimeClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdmittedRuntimeClient")
            .field("record", &self.record)
            .field("client_id", &self.client_id)
            .field("generation", &self.generation)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

struct AdmissionAuthority {
    record: RendezvousRecord,
    desktop: ClientCredentialRegistration,
    cli: ClientCredentialRegistration,
    secret_store: Arc<dyn SecretStore>,
    mcp: Arc<InstalledMcpControl>,
}

impl AdmissionAuthority {
    fn admit(
        &self,
        client: NamedClient,
    ) -> Result<(ClientCredentialRegistration, SecretValue), InstalledServiceError> {
        match client {
            NamedClient::Desktop | NamedClient::Cli => {
                let registration = if client == NamedClient::Desktop {
                    self.desktop.clone()
                } else {
                    self.cli.clone()
                };
                let credential =
                    super::runtime::load_client_credential(&self.secret_store, &registration)?;
                Ok((registration, credential))
            }
            NamedClient::ClaudeCode | NamedClient::Codex => {
                self.mcp.admit_client(client).map_err(Into::into)
            }
        }
    }
}

/// Exact-generation admission listener owned for the complete ready service lifetime.
pub(super) struct ReadyAdmission {
    root: PathBuf,
    metadata: AdmissionMetadata,
    cancellation: CancellationToken,
    failed: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for ReadyAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadyAdmission")
            .field("runtime", &self.metadata.runtime)
            .field("process", &self.metadata.process)
            .finish_non_exhaustive()
    }
}

impl ReadyAdmission {
    pub(super) fn start(
        paths: &LocalPaths,
        record: RendezvousRecord,
        desktop: ClientCredentialRegistration,
        cli: ClientCredentialRegistration,
        secret_store: Arc<dyn SecretStore>,
        mcp: Arc<InstalledMcpControl>,
    ) -> Result<Self, InstalledServiceError> {
        let root = paths.control_root()?.root().join(ADMISSION_DIRECTORY);
        let mut endpoint_key = [0_u8; 32];
        fill_random(&mut endpoint_key)
            .map_err(|_error| InstalledServiceError::EntropyUnavailable)?;
        if endpoint_key.iter().all(|byte| *byte == 0) {
            return Err(InstalledServiceError::EntropyUnavailable);
        }
        let metadata = AdmissionMetadata {
            runtime: record.runtime(),
            process: record.process_identity(),
            endpoint_key,
        };
        let listener = platform::Listener::bind(&root, &endpoint_key)?;
        let authority = Arc::new(AdmissionAuthority {
            record,
            desktop,
            cli,
            secret_store,
            mcp,
        });
        let cancellation = CancellationToken::new();
        let failed = CancellationToken::new();
        let server_cancellation = cancellation.clone();
        let server_failed = failed.clone();
        let task = tokio::spawn(async move {
            run_server(listener, metadata, authority, server_cancellation).await;
            server_failed.cancel();
        });
        Ok(Self {
            root,
            metadata,
            cancellation,
            failed,
            task: Some(task),
        })
    }

    pub(super) async fn probe(&self) -> Result<(), InstalledServiceError> {
        let root = self.root.clone();
        let metadata = self.metadata;
        tokio::task::spawn_blocking(move || {
            request_with_metadata(&root, metadata, NamedClient::Desktop, CONNECTION_TIMEOUT)
                .map(drop)
        })
        .await
        .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?
    }

    pub(super) fn publish(&self) -> Result<(), InstalledServiceError> {
        if self.failed.is_cancelled() {
            return Err(InstalledServiceError::AdmissionUnavailable);
        }
        publish_metadata(&self.root, self.metadata)?;
        if self.failed.is_cancelled() {
            let _retired = remove_metadata_if_current(&self.root, self.metadata);
            return Err(InstalledServiceError::AdmissionUnavailable);
        }
        Ok(())
    }

    pub(super) async fn failed(&self) {
        self.failed.cancelled().await;
    }

    pub(super) async fn shutdown(&mut self) -> bool {
        let retired = remove_metadata_if_current(&self.root, self.metadata).unwrap_or(false);
        self.cancellation.cancel();
        let Some(mut task) = self.task.take() else {
            return false;
        };
        let joined = match tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(result) => result.is_ok(),
            Err(_) => {
                task.abort();
                task.await.is_ok()
            }
        };
        retired && joined
    }
}

impl Drop for ReadyAdmission {
    fn drop(&mut self) {
        let _retired = remove_metadata_if_current(&self.root, self.metadata);
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(super) fn request(
    paths: &LocalPaths,
    client: NamedClient,
    timeout: Duration,
) -> Result<AdmittedRuntimeClient, InstalledServiceError> {
    if timeout.is_zero() {
        return Err(InstalledServiceError::AdmissionDeadline);
    }
    let root = paths.control_root()?.root().join(ADMISSION_DIRECTORY);
    let metadata = load_metadata(&root)?;
    request_with_metadata(&root, metadata, client, timeout.min(CONNECTION_TIMEOUT))
}

async fn run_server(
    listener: platform::Listener,
    metadata: AdmissionMetadata,
    authority: Arc<AdmissionAuthority>,
    cancellation: CancellationToken,
) {
    let mut workers = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            joined = workers.join_next(), if !workers.is_empty() => {
                if joined.is_some_and(|result| result.is_err()) {
                    break;
                }
            }
            accepted = listener.accept(cancellation.clone()) => {
                let Ok(stream) = accepted else { break; };
                if workers.len() >= MAXIMUM_CONNECTIONS {
                    drop(stream);
                    continue;
                }
                let authority = Arc::clone(&authority);
                workers.spawn(async move {
                    let _served = tokio::time::timeout(
                        CONNECTION_TIMEOUT,
                        serve_connection(stream, metadata, authority),
                    ).await;
                });
            }
        }
    }
    workers.abort_all();
    while workers.join_next().await.is_some() {}
}

async fn serve_connection(
    mut stream: platform::Stream,
    metadata: AdmissionMetadata,
    authority: Arc<AdmissionAuthority>,
) -> Result<(), InstalledServiceError> {
    platform::authenticate_preface(&mut stream).await?;
    let mut frame = read_async_frame(&mut stream).await?;
    let request = decode_request(&frame, metadata)?;
    frame.zeroize();
    let mut trailing = [0_u8; 1];
    match tokio::time::timeout(TRAILING_DATA_TIMEOUT, stream.read(&mut trailing)).await {
        Ok(Ok(0)) | Err(_) => {}
        _ => return Err(InstalledServiceError::AdmissionProtocol),
    }
    if Instant::now() >= request.deadline {
        return Err(InstalledServiceError::AdmissionDeadline);
    }
    let (registration, credential) = authority.admit(request.client)?;
    let mut response = encode_response(
        metadata,
        request,
        &authority.record,
        &registration,
        &credential,
    )?;
    drop(credential);
    let length = u32::try_from(response.len())
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?
        .to_be_bytes();
    stream.write_all(&length).await?;
    let result = stream.write_all(&response).await;
    response.zeroize();
    result?;
    stream.shutdown().await?;
    Ok(())
}

async fn read_async_frame(
    stream: &mut platform::Stream,
) -> Result<Zeroizing<Vec<u8>>, InstalledServiceError> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    if length == 0 || length > MAXIMUM_FRAME_BYTES {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let mut frame = Zeroizing::new(vec![0_u8; length]);
    stream.read_exact(&mut frame).await?;
    Ok(frame)
}

fn request_with_metadata(
    root: &Path,
    metadata: AdmissionMetadata,
    client: NamedClient,
    timeout: Duration,
) -> Result<AdmittedRuntimeClient, InstalledServiceError> {
    let mut stream = platform::connect_blocking(root, &metadata.endpoint_key, timeout)?;
    let mut request_nonce = [0_u8; 16];
    fill_random(&mut request_nonce).map_err(|_error| InstalledServiceError::EntropyUnavailable)?;
    if request_nonce.iter().all(|byte| *byte == 0) {
        return Err(InstalledServiceError::EntropyUnavailable);
    }
    let deadline_millis = u32::try_from(timeout.as_millis())
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    let request = encode_request(metadata, client, request_nonce, deadline_millis)?;
    stream.write_all(PREFACE)?;
    stream.write_all(
        &u32::try_from(request.len())
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?
            .to_be_bytes(),
    )?;
    stream.write_all(&request)?;
    stream.flush()?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(InstalledServiceError::AdmissionDeadline)?;
    let mut frame = read_blocking_frame(&mut stream)?;
    if Instant::now() >= deadline {
        return Err(InstalledServiceError::AdmissionDeadline);
    }
    let admitted = decode_response(&frame, metadata, client, request_nonce)?;
    frame.zeroize();
    Ok(admitted)
}

fn read_blocking_frame(
    stream: &mut platform::BlockingStream,
) -> Result<Zeroizing<Vec<u8>>, InstalledServiceError> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    if length == 0 || length > MAXIMUM_FRAME_BYTES {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let mut frame = Zeroizing::new(vec![0_u8; length]);
    stream.read_exact(&mut frame)?;
    Ok(frame)
}

fn encode_request(
    metadata: AdmissionMetadata,
    client: NamedClient,
    request_nonce: [u8; 16],
    deadline_millis: u32,
) -> Result<[u8; REQUEST_BYTES], InstalledServiceError> {
    if deadline_millis == 0
        || deadline_millis > u32::try_from(CONNECTION_TIMEOUT.as_millis()).unwrap_or(u32::MAX)
    {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let mut encoded = [0_u8; REQUEST_BYTES];
    encoded[..2].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    encode_runtime(metadata.runtime, &mut encoded[2..42]);
    encode_process(metadata.process, &mut encoded[42..54]);
    encoded[54..86].copy_from_slice(&metadata.endpoint_key);
    encoded[86] = encode_client(client);
    encoded[87..91].copy_from_slice(&deadline_millis.to_be_bytes());
    encoded[91..].copy_from_slice(&request_nonce);
    Ok(encoded)
}

fn decode_request(
    encoded: &[u8],
    metadata: AdmissionMetadata,
) -> Result<AdmissionRequest, InstalledServiceError> {
    if encoded.len() != REQUEST_BYTES
        || u16::from_be_bytes([encoded[0], encoded[1]]) != PROTOCOL_VERSION
        || decode_runtime(&encoded[2..42])? != metadata.runtime
        || decode_process(&encoded[42..54])? != metadata.process
        || encoded[54..86] != metadata.endpoint_key
    {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let deadline_millis = u32::from_be_bytes(
        encoded[87..91]
            .try_into()
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
    );
    if deadline_millis == 0
        || deadline_millis > u32::try_from(CONNECTION_TIMEOUT.as_millis()).unwrap_or(u32::MAX)
    {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let request_nonce: [u8; 16] = encoded[91..]
        .try_into()
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    if request_nonce.iter().all(|byte| *byte == 0) {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    Ok(AdmissionRequest {
        client: decode_client(encoded[86])?,
        request_nonce,
        deadline: Instant::now()
            .checked_add(Duration::from_millis(u64::from(deadline_millis)))
            .ok_or(InstalledServiceError::AdmissionProtocol)?,
    })
}

fn encode_response(
    metadata: AdmissionMetadata,
    request: AdmissionRequest,
    record: &RendezvousRecord,
    registration: &ClientCredentialRegistration,
    credential: &SecretValue,
) -> Result<Zeroizing<Vec<u8>>, InstalledServiceError> {
    if registration.client() != request.client {
        return Err(InstalledServiceError::AdmissionRejected);
    }
    let record =
        serde_json::to_vec(record).map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    let credential = credential.expose_secret().as_bytes();
    if credential.is_empty() || credential.len() > MAXIMUM_CREDENTIAL_BYTES {
        return Err(InstalledServiceError::AdmissionRejected);
    }
    let mut encoded = Zeroizing::new(Vec::with_capacity(
        RESPONSE_FIXED_BYTES + record.len() + credential.len(),
    ));
    encoded.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    encode_runtime_extend(metadata.runtime, &mut encoded);
    encode_process_extend(metadata.process, &mut encoded);
    encoded.extend_from_slice(&metadata.endpoint_key);
    encoded.push(encode_client(request.client));
    encoded.extend_from_slice(registration.client_id().as_uuid().as_bytes());
    encoded.extend_from_slice(&registration.generation().get().to_be_bytes());
    encoded.extend_from_slice(&request.request_nonce);
    encoded.extend_from_slice(
        &u32::try_from(record.len())
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(
        &u16::try_from(credential.len())
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&record);
    encoded.extend_from_slice(credential);
    Ok(encoded)
}

fn decode_response(
    encoded: &[u8],
    metadata: AdmissionMetadata,
    client: NamedClient,
    request_nonce: [u8; 16],
) -> Result<AdmittedRuntimeClient, InstalledServiceError> {
    if encoded.len() < RESPONSE_FIXED_BYTES
        || u16::from_be_bytes([encoded[0], encoded[1]]) != PROTOCOL_VERSION
        || decode_runtime(&encoded[2..42])? != metadata.runtime
        || decode_process(&encoded[42..54])? != metadata.process
        || encoded[54..86] != metadata.endpoint_key
        || decode_client(encoded[86])? != client
        || encoded[111..127] != request_nonce
    {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let client_id = ClientId::try_from_uuid(
        Uuid::from_slice(&encoded[87..103])
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
    )?;
    let generation = CredentialGeneration::try_new(u64::from_be_bytes(
        encoded[103..111]
            .try_into()
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
    ))?;
    let record_len = usize::try_from(u32::from_be_bytes(
        encoded[127..131]
            .try_into()
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
    ))
    .map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    let credential_len = usize::from(u16::from_be_bytes([encoded[131], encoded[132]]));
    if credential_len == 0
        || credential_len > MAXIMUM_CREDENTIAL_BYTES
        || encoded.len() != RESPONSE_FIXED_BYTES + record_len + credential_len
    {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let record = serde_json::from_slice::<RendezvousRecord>(
        &encoded[RESPONSE_FIXED_BYTES..RESPONSE_FIXED_BYTES + record_len],
    )
    .map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    if record.runtime() != metadata.runtime
        || record.process_identity() != metadata.process
        || record.endpoint().ip() != Ipv4Addr::LOCALHOST
        || record.endpoint().port() == 0
        || !record.protocols().contains(ApplicationProtocolVersion::V1)
        || !SystemProcessIdentityVerifier
            .is_current(metadata.process)
            .map_err(InstalledServiceError::Rendezvous)?
    {
        return Err(InstalledServiceError::AdmissionRejected);
    }
    let credential_bytes = &encoded[RESPONSE_FIXED_BYTES + record_len..];
    if credential_bytes.len() != 64
        || credential_bytes
            .iter()
            .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(InstalledServiceError::AdmissionRejected);
    }
    let credential = SecretValue::from_utf8_bytes(credential_bytes.to_vec())
        .map_err(|_error| InstalledServiceError::AdmissionRejected)?;
    Ok(AdmittedRuntimeClient {
        record,
        client_id,
        generation,
        credential,
    })
}

fn publish_metadata(root: &Path, metadata: AdmissionMetadata) -> Result<(), InstalledServiceError> {
    let temporary = root.join(METADATA_TEMP_FILE);
    let final_path = root.join(METADATA_FILE);
    let _stale = fs::remove_file(&temporary);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(&encode_metadata(metadata))?;
    file.sync_all()?;
    #[cfg(windows)]
    match fs::remove_file(&final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::rename(temporary, final_path)?;
    Ok(())
}

fn load_metadata(root: &Path) -> Result<AdmissionMetadata, InstalledServiceError> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(root.join(METADATA_FILE))
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                InstalledServiceError::ServiceUnavailable
            } else {
                InstalledServiceError::Io(error)
            }
        })?;
    let file_metadata = file.metadata()?;
    if !file_metadata.file_type().is_file() || file_metadata.len() != METADATA_BYTES as u64 {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let mut encoded = [0_u8; METADATA_BYTES];
    file.read_exact(&mut encoded)?;
    decode_metadata(&encoded)
}

fn remove_metadata_if_current(
    root: &Path,
    metadata: AdmissionMetadata,
) -> Result<bool, InstalledServiceError> {
    match load_metadata(root) {
        Ok(current) if current == metadata => {
            fs::remove_file(root.join(METADATA_FILE))?;
            Ok(true)
        }
        Ok(_) => Ok(false),
        Err(InstalledServiceError::ServiceUnavailable) => Ok(true),
        Err(error) => Err(error),
    }
}

fn encode_metadata(metadata: AdmissionMetadata) -> [u8; METADATA_BYTES] {
    let mut encoded = [0_u8; METADATA_BYTES];
    encoded[..4].copy_from_slice(METADATA_MAGIC);
    encoded[4..6].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    encode_runtime(metadata.runtime, &mut encoded[6..46]);
    encode_process(metadata.process, &mut encoded[46..58]);
    encoded[58..].copy_from_slice(&metadata.endpoint_key);
    encoded
}

fn decode_metadata(
    encoded: &[u8; METADATA_BYTES],
) -> Result<AdmissionMetadata, InstalledServiceError> {
    if &encoded[..4] != METADATA_MAGIC
        || u16::from_be_bytes([encoded[4], encoded[5]]) != PROTOCOL_VERSION
    {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let endpoint_key: [u8; 32] = encoded[58..]
        .try_into()
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?;
    if endpoint_key.iter().all(|byte| *byte == 0) {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    Ok(AdmissionMetadata {
        runtime: decode_runtime(&encoded[6..46])?,
        process: decode_process(&encoded[46..58])?,
        endpoint_key,
    })
}

fn encode_runtime(runtime: RuntimeIdentity, target: &mut [u8]) {
    target[..16].copy_from_slice(runtime.installation_id().as_uuid().as_bytes());
    target[16..32].copy_from_slice(runtime.workspace_id().as_uuid().as_bytes());
    target[32..40].copy_from_slice(&runtime.service_generation().get().to_be_bytes());
}

fn encode_runtime_extend(runtime: RuntimeIdentity, target: &mut Vec<u8>) {
    let mut encoded = [0_u8; 40];
    encode_runtime(runtime, &mut encoded);
    target.extend_from_slice(&encoded);
}

fn decode_runtime(encoded: &[u8]) -> Result<RuntimeIdentity, InstalledServiceError> {
    if encoded.len() != 40 {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let installation = market_squawk_runtime::InstallationId::try_from_uuid(
        Uuid::from_slice(&encoded[..16])
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
    )?;
    let workspace = market_squawk_runtime::WorkspaceId::try_from_uuid(
        Uuid::from_slice(&encoded[16..32])
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
    )?;
    let generation = market_squawk_runtime::ServiceGeneration::try_new(u64::from_be_bytes(
        encoded[32..40]
            .try_into()
            .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
    ))?;
    RuntimeIdentity::try_new(installation, workspace, generation).map_err(Into::into)
}

fn encode_process(process: ProcessIdentity, target: &mut [u8]) {
    target[..4].copy_from_slice(&process.process_id().to_be_bytes());
    target[4..12].copy_from_slice(&process.start_identity().to_be_bytes());
}

fn encode_process_extend(process: ProcessIdentity, target: &mut Vec<u8>) {
    let mut encoded = [0_u8; 12];
    encode_process(process, &mut encoded);
    target.extend_from_slice(&encoded);
}

fn decode_process(encoded: &[u8]) -> Result<ProcessIdentity, InstalledServiceError> {
    if encoded.len() != 12 {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    ProcessIdentity::try_new(
        u32::from_be_bytes(
            encoded[..4]
                .try_into()
                .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
        ),
        u64::from_be_bytes(
            encoded[4..]
                .try_into()
                .map_err(|_error| InstalledServiceError::AdmissionProtocol)?,
        ),
    )
    .map_err(Into::into)
}

const fn encode_client(client: NamedClient) -> u8 {
    match client {
        NamedClient::Desktop => 1,
        NamedClient::Cli => 2,
        NamedClient::ClaudeCode => 3,
        NamedClient::Codex => 4,
    }
}

fn decode_client(encoded: u8) -> Result<NamedClient, InstalledServiceError> {
    match encoded {
        1 => Ok(NamedClient::Desktop),
        2 => Ok(NamedClient::Cli),
        3 => Ok(NamedClient::ClaudeCode),
        4 => Ok(NamedClient::Codex),
        _ => Err(InstalledServiceError::AdmissionProtocol),
    }
}
