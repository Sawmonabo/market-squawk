//! Short-lived owner-authenticated credential-bootstrap transport.

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

use std::{
    fs::{self, OpenOptions},
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{BufMut as _, Bytes};
use futures_util::{SinkExt as _, StreamExt as _};
use getrandom::fill as fill_random;
use market_squawk_platform::{
    EncryptedFileFallbackStatus, EncryptedFileUnlockCapability, LocalPaths, SecretCancellation,
    SecretInteractionPolicy, SecretOperationControl, SecretStore, SecretValue,
};
use market_squawk_runtime::InstallationId;
use serde::Serialize;
use tokio::io::AsyncWriteExt as _;
use tokio_util::codec::LengthDelimitedCodec;
use uuid::Uuid;
use zeroize::Zeroize as _;

use super::InstalledServiceError;

#[cfg(unix)]
use self::unix as platform;
#[cfg(windows)]
use self::windows as platform;

const BOOTSTRAP_DIRECTORY: &str = "boot";
const METADATA_FILE: &str = "state";
const METADATA_TEMP_FILE: &str = ".state.tmp";
const METADATA_MAGIC: &[u8; 4] = b"MSQM";
const PREFACE: &[u8; 8] = b"MSQB\0\x01\0\0";
const PROTOCOL_VERSION: u16 = 1;
const MAXIMUM_FRAME_BYTES: usize = 64 * 1024;
const MAXIMUM_UNLOCK_BYTES: usize = 4 * 1024;
const MAXIMUM_CONNECTIONS: usize = 16;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const TOTAL_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const TRAILING_DATA_TIMEOUT: Duration = Duration::from_millis(100);
const SECRET_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Non-secret credential condition that permits a bounded foreground retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapRequirement {
    /// The configured encrypted fallback needs explicit user-held unlock material.
    EncryptedFallbackLocked,
    /// The foreground process completed a platform keyring interaction and may request retry.
    ForegroundKeyringRetry,
}

/// Closed bootstrap-channel lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstalledServiceBootstrapState {
    /// The service is alive and waiting for one admitted foreground action.
    Required,
    /// The action was accepted and the unchanged runtime preparation is being retried.
    Retrying,
}

/// Typed, secret-free bootstrap status returned to native callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstalledServiceBootstrapStatus {
    state: InstalledServiceBootstrapState,
    requirement: Option<BootstrapRequirement>,
    installation_id: InstallationId,
    generation: u64,
}

impl InstalledServiceBootstrapStatus {
    /// Current short-lived channel state.
    #[must_use]
    pub const fn state(self) -> InstalledServiceBootstrapState {
        self.state
    }

    /// Exact typed foreground requirement, when the service is waiting.
    #[must_use]
    pub const fn requirement(self) -> Option<BootstrapRequirement> {
        self.requirement
    }

    /// Installation identity bound into every request.
    #[must_use]
    pub const fn installation_id(self) -> InstallationId {
        self.installation_id
    }

    /// Process-local anti-replay generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BootstrapAction {
    Retry,
    UnlockAccepted,
}

#[derive(Clone, Copy, Debug)]
struct BootstrapMetadata {
    installation_id: InstallationId,
    generation: u64,
    requirement: BootstrapRequirement,
}

#[derive(Debug)]
enum BootstrapCommand {
    Status,
    RetryAfterForegroundKeyring,
    UnlockEncryptedFallback(SecretValue),
}

#[derive(Debug)]
struct BootstrapRequest {
    deadline: Instant,
    command: BootstrapCommand,
}

#[derive(Clone, Copy, Debug)]
enum ResponseCode {
    Required = 1,
    Retrying = 2,
    Rejected = 3,
}

pub(super) async fn wait_for_action(
    paths: &LocalPaths,
    secret_store: Arc<dyn SecretStore>,
    installation_id: InstallationId,
    requirement: BootstrapRequirement,
) -> Result<BootstrapAction, InstalledServiceError> {
    let root = bootstrap_root(paths)?;
    let generation = random_generation()?;
    let metadata = BootstrapMetadata {
        installation_id,
        generation,
        requirement,
    };
    let listener = platform::Listener::bind(&root)?;
    publish_metadata(&root, metadata)?;
    let _metadata_cleanup = MetadataCleanup(root.join(METADATA_FILE));
    let total_deadline = Instant::now()
        .checked_add(TOTAL_BOOTSTRAP_TIMEOUT)
        .ok_or(InstalledServiceError::BootstrapUnavailable)?;

    for _ in 0..MAXIMUM_CONNECTIONS {
        let remaining = total_deadline
            .checked_duration_since(Instant::now())
            .ok_or(InstalledServiceError::BootstrapDeadline)?;
        let stream = tokio::time::timeout(remaining, listener.accept())
            .await
            .map_err(|_elapsed| InstalledServiceError::BootstrapDeadline)??;
        match tokio::time::timeout(
            CONNECTION_TIMEOUT,
            serve_connection(stream, metadata, Arc::clone(&secret_store)),
        )
        .await
        {
            Ok(Ok(Some(action))) => return Ok(action),
            Ok(Ok(None) | Err(_)) | Err(_) => continue,
        }
    }
    Err(InstalledServiceError::BootstrapUnavailable)
}

pub(super) async fn status(
    paths: &LocalPaths,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    request(paths, BootstrapCommand::Status).await
}

pub(super) async fn unlock(
    paths: &LocalPaths,
    unlock: SecretValue,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    request(paths, BootstrapCommand::UnlockEncryptedFallback(unlock)).await
}

pub(super) async fn retry_after_foreground_keyring(
    paths: &LocalPaths,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    request(paths, BootstrapCommand::RetryAfterForegroundKeyring).await
}

async fn request(
    paths: &LocalPaths,
    command: BootstrapCommand,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    let root = bootstrap_root(paths)?;
    let metadata = load_metadata(&root)?;
    let mut stream = platform::connect(&root).await?;
    stream.write_all(PREFACE).await?;
    let mut framed = codec().new_framed(stream);
    let frame = encode_request(metadata, command)?;
    framed
        .send(Bytes::from(frame))
        .await
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    framed
        .get_mut()
        .shutdown()
        .await
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    let response = tokio::time::timeout(CONNECTION_TIMEOUT, framed.next())
        .await
        .map_err(|_elapsed| InstalledServiceError::BootstrapDeadline)?
        .ok_or(InstalledServiceError::BootstrapUnavailable)?
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    decode_response(&response, metadata)
}

async fn serve_connection(
    mut stream: platform::Stream,
    metadata: BootstrapMetadata,
    secret_store: Arc<dyn SecretStore>,
) -> Result<Option<BootstrapAction>, InstalledServiceError> {
    platform::authenticate_preface(&mut stream).await?;
    let mut framed = codec().new_framed(stream);
    let mut frame = framed
        .next()
        .await
        .ok_or(InstalledServiceError::BootstrapProtocol)?
        .map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
    let request = decode_request(&frame, metadata)?;
    frame.as_mut().zeroize();
    // A quiet window is sufficient: this handler executes exactly one decoded action and closes,
    // so incomplete or delayed trailing bytes are discarded and can never become a second action.
    match tokio::time::timeout(TRAILING_DATA_TIMEOUT, framed.next()).await {
        Ok(None) | Err(_) => {}
        Ok(Some(_)) => return Err(InstalledServiceError::BootstrapProtocol),
    }
    if Instant::now() >= request.deadline {
        return Err(InstalledServiceError::BootstrapDeadline);
    }
    let (code, action) = match request.command {
        BootstrapCommand::Status => (ResponseCode::Required, None),
        BootstrapCommand::RetryAfterForegroundKeyring => {
            (ResponseCode::Retrying, Some(BootstrapAction::Retry))
        }
        BootstrapCommand::UnlockEncryptedFallback(unlock) => {
            let control = bootstrap_secret_control()?;
            let status = secret_store
                .unlock_encrypted_file_fallback(
                    EncryptedFileUnlockCapability::new(unlock),
                    &control,
                )
                .map_err(|_error| InstalledServiceError::BootstrapRejected)?;
            if status != EncryptedFileFallbackStatus::Ready {
                (ResponseCode::Rejected, None)
            } else {
                (
                    ResponseCode::Retrying,
                    Some(BootstrapAction::UnlockAccepted),
                )
            }
        }
    };
    framed
        .send(Bytes::from(encode_response(metadata, code)))
        .await
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    framed
        .close()
        .await
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    Ok(action)
}

fn codec() -> tokio_util::codec::length_delimited::Builder {
    let mut builder = LengthDelimitedCodec::builder();
    builder
        .big_endian()
        .length_field_length(4)
        .max_frame_length(MAXIMUM_FRAME_BYTES);
    builder
}

fn encode_request(
    metadata: BootstrapMetadata,
    command: BootstrapCommand,
) -> Result<Vec<u8>, InstalledServiceError> {
    let (command_code, mut payload) = match command {
        BootstrapCommand::Status => (1_u8, Vec::new()),
        BootstrapCommand::RetryAfterForegroundKeyring => (2, Vec::new()),
        BootstrapCommand::UnlockEncryptedFallback(unlock) => {
            let payload = unlock.expose_secret().as_bytes().to_vec();
            drop(unlock);
            (3, payload)
        }
    };
    if payload.len() > MAXIMUM_UNLOCK_BYTES {
        payload.zeroize();
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let payload_len =
        u16::try_from(payload.len()).map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
    let mut encoded = Vec::with_capacity(33 + payload.len());
    encoded.put_u16(PROTOCOL_VERSION);
    encoded.extend_from_slice(metadata.installation_id.as_uuid().as_bytes());
    encoded.put_u64(metadata.generation);
    encoded.put_u8(command_code);
    encoded.put_u32(
        u32::try_from(CONNECTION_TIMEOUT.as_millis())
            .map_err(|_error| InstalledServiceError::BootstrapProtocol)?,
    );
    encoded.put_u16(payload_len);
    encoded.extend_from_slice(&payload);
    payload.zeroize();
    Ok(encoded)
}

fn decode_request(
    encoded: &[u8],
    metadata: BootstrapMetadata,
) -> Result<BootstrapRequest, InstalledServiceError> {
    if encoded.len() < 33 {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let version = u16::from_be_bytes([encoded[0], encoded[1]]);
    let installation_id = InstallationId::try_from_uuid(
        Uuid::from_slice(&encoded[2..18])
            .map_err(|_error| InstalledServiceError::BootstrapProtocol)?,
    )?;
    let generation = u64::from_be_bytes(
        encoded[18..26]
            .try_into()
            .map_err(|_error| InstalledServiceError::BootstrapProtocol)?,
    );
    let command = encoded[26];
    let deadline_millis = u32::from_be_bytes(
        encoded[27..31]
            .try_into()
            .map_err(|_error| InstalledServiceError::BootstrapProtocol)?,
    );
    let payload_len = usize::from(u16::from_be_bytes([encoded[31], encoded[32]]));
    if version != PROTOCOL_VERSION
        || installation_id != metadata.installation_id
        || generation != metadata.generation
        || deadline_millis == 0
        || deadline_millis > u32::try_from(CONNECTION_TIMEOUT.as_millis()).unwrap_or(u32::MAX)
        || encoded.len() != 33 + payload_len
    {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(u64::from(deadline_millis)))
        .ok_or(InstalledServiceError::BootstrapProtocol)?;
    let command = match command {
        1 if payload_len == 0 => BootstrapCommand::Status,
        2 if payload_len == 0 => BootstrapCommand::RetryAfterForegroundKeyring,
        3 if payload_len <= MAXIMUM_UNLOCK_BYTES => BootstrapCommand::UnlockEncryptedFallback(
            SecretValue::from_utf8_bytes(encoded[33..].to_vec())
                .map_err(|_error| InstalledServiceError::BootstrapRejected)?,
        ),
        _ => return Err(InstalledServiceError::BootstrapProtocol),
    };
    Ok(BootstrapRequest { deadline, command })
}

fn encode_response(metadata: BootstrapMetadata, code: ResponseCode) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(28);
    encoded.put_u16(PROTOCOL_VERSION);
    encoded.extend_from_slice(metadata.installation_id.as_uuid().as_bytes());
    encoded.put_u64(metadata.generation);
    encoded.put_u8(code as u8);
    encoded.put_u8(match metadata.requirement {
        BootstrapRequirement::EncryptedFallbackLocked => 1,
        BootstrapRequirement::ForegroundKeyringRetry => 2,
    });
    encoded
}

fn decode_response(
    encoded: &[u8],
    metadata: BootstrapMetadata,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    if encoded.len() != 28
        || u16::from_be_bytes([encoded[0], encoded[1]]) != PROTOCOL_VERSION
        || encoded[2..18] != metadata.installation_id.as_uuid().as_bytes()[..]
        || u64::from_be_bytes(
            encoded[18..26]
                .try_into()
                .map_err(|_error| InstalledServiceError::BootstrapProtocol)?,
        ) != metadata.generation
    {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let state = match encoded[26] {
        1 => InstalledServiceBootstrapState::Required,
        2 => InstalledServiceBootstrapState::Retrying,
        _ => return Err(InstalledServiceError::BootstrapRejected),
    };
    let requirement = match encoded[27] {
        1 => BootstrapRequirement::EncryptedFallbackLocked,
        2 => BootstrapRequirement::ForegroundKeyringRetry,
        _ => return Err(InstalledServiceError::BootstrapProtocol),
    };
    Ok(InstalledServiceBootstrapStatus {
        state,
        requirement: (state == InstalledServiceBootstrapState::Required).then_some(requirement),
        installation_id: metadata.installation_id,
        generation: metadata.generation,
    })
}

fn bootstrap_root(paths: &LocalPaths) -> Result<PathBuf, InstalledServiceError> {
    Ok(paths.control_root()?.root().join(BOOTSTRAP_DIRECTORY))
}

fn publish_metadata(root: &Path, metadata: BootstrapMetadata) -> Result<(), InstalledServiceError> {
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
    fs::rename(&temporary, &final_path)?;
    Ok(())
}

fn load_metadata(root: &Path) -> Result<BootstrapMetadata, InstalledServiceError> {
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
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() != 29 {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let mut encoded = [0_u8; 29];
    file.read_exact(&mut encoded)?;
    decode_metadata(&encoded)
}

fn encode_metadata(metadata: BootstrapMetadata) -> [u8; 29] {
    let mut encoded = [0_u8; 29];
    encoded[..4].copy_from_slice(METADATA_MAGIC);
    encoded[4..20].copy_from_slice(metadata.installation_id.as_uuid().as_bytes());
    encoded[20..28].copy_from_slice(&metadata.generation.to_be_bytes());
    encoded[28] = match metadata.requirement {
        BootstrapRequirement::EncryptedFallbackLocked => 1,
        BootstrapRequirement::ForegroundKeyringRetry => 2,
    };
    encoded
}

fn decode_metadata(encoded: &[u8; 29]) -> Result<BootstrapMetadata, InstalledServiceError> {
    if &encoded[..4] != METADATA_MAGIC {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let installation_id = InstallationId::try_from_uuid(
        Uuid::from_slice(&encoded[4..20])
            .map_err(|_error| InstalledServiceError::BootstrapProtocol)?,
    )?;
    let generation = u64::from_be_bytes(
        encoded[20..28]
            .try_into()
            .map_err(|_error| InstalledServiceError::BootstrapProtocol)?,
    );
    if generation == 0 {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let requirement = match encoded[28] {
        1 => BootstrapRequirement::EncryptedFallbackLocked,
        2 => BootstrapRequirement::ForegroundKeyringRetry,
        _ => return Err(InstalledServiceError::BootstrapProtocol),
    };
    Ok(BootstrapMetadata {
        installation_id,
        generation,
        requirement,
    })
}

fn random_generation() -> Result<u64, InstalledServiceError> {
    let mut bytes = [0_u8; 8];
    fill_random(&mut bytes).map_err(|_error| InstalledServiceError::EntropyUnavailable)?;
    let generation = u64::from_be_bytes(bytes);
    if generation == 0 {
        return Err(InstalledServiceError::EntropyUnavailable);
    }
    Ok(generation)
}

fn bootstrap_secret_control() -> Result<SecretOperationControl, InstalledServiceError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_TIMEOUT)
        .ok_or(InstalledServiceError::BootstrapUnavailable)?;
    SecretOperationControl::try_new(
        "installed-service-bootstrap-unlock",
        deadline,
        1,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .map_err(|_error| InstalledServiceError::BootstrapUnavailable)
}

struct MetadataCleanup(PathBuf);

impl Drop for MetadataCleanup {
    fn drop(&mut self) {
        let _removed = fs::remove_file(&self.0);
    }
}
