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

use getrandom::fill as fill_random;
use market_squawk_platform::{
    EncryptedFileFallbackStatus, EncryptedFileUnlockCapability, LocalPaths, SecretCancellation,
    SecretInteractionPolicy, SecretOperationControl, SecretRef, SecretStore, SecretValue,
};
use market_squawk_runtime::InstallationId;
use serde::Serialize;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use uuid::Uuid;
use zeroize::{Zeroize as _, Zeroizing};

use super::{InstalledServiceError, runtime::ForegroundRuntimeCredential};

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
const MAXIMUM_CREDENTIAL_BYTES: usize = 4 * 1024;
const MAXIMUM_REFERENCE_BYTES: usize = 1024;
const MAXIMUM_CONNECTIONS: usize = 16;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const TOTAL_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const TRAILING_DATA_TIMEOUT: Duration = Duration::from_millis(100);
const SECRET_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Non-secret credential condition that permits one bounded foreground action.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapRequirement {
    /// The configured encrypted fallback needs explicit user-held unlock material.
    EncryptedFallbackLocked,
    /// The foreground process must supply the exact protected runtime credential.
    ForegroundKeyringCredential,
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

#[derive(Debug)]
pub(super) enum BootstrapAction {
    ForegroundCredential(ForegroundRuntimeCredential),
    UnlockAccepted,
}

#[derive(Debug)]
pub(super) enum BootstrapAdmission {
    EncryptedFallbackUnlock,
    ForegroundKeyringCredential { expected_reference: SecretRef },
}

impl BootstrapAdmission {
    const fn requirement(&self) -> BootstrapRequirement {
        match self {
            Self::EncryptedFallbackUnlock => BootstrapRequirement::EncryptedFallbackLocked,
            Self::ForegroundKeyringCredential { .. } => {
                BootstrapRequirement::ForegroundKeyringCredential
            }
        }
    }

    fn admits_foreground_reference(&self, reference: &SecretRef) -> bool {
        matches!(
            self,
            Self::ForegroundKeyringCredential { expected_reference }
                if expected_reference == reference
        )
    }

    const fn admits_fallback_unlock(&self) -> bool {
        matches!(self, Self::EncryptedFallbackUnlock)
    }
}

#[derive(Clone, Copy, Debug)]
struct BootstrapMetadata {
    installation_id: InstallationId,
    generation: u64,
    requirement: BootstrapRequirement,
}

enum BootstrapCommand {
    Status,
    ProvideForegroundCredential {
        reference: SecretRef,
        credential: SecretValue,
    },
    UnlockEncryptedFallback(SecretValue),
}

impl std::fmt::Debug for BootstrapCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status => formatter.write_str("Status"),
            Self::ProvideForegroundCredential { reference, .. } => formatter
                .debug_struct("ProvideForegroundCredential")
                .field("reference", reference)
                .field("credential", &"[REDACTED]")
                .finish(),
            Self::UnlockEncryptedFallback(_) => formatter
                .debug_tuple("UnlockEncryptedFallback")
                .field(&"[REDACTED]")
                .finish(),
        }
    }
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
    admission: BootstrapAdmission,
) -> Result<BootstrapAction, InstalledServiceError> {
    let root = bootstrap_root(paths)?;
    let generation = random_generation()?;
    let metadata = BootstrapMetadata {
        installation_id,
        generation,
        requirement: admission.requirement(),
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
            serve_connection(stream, metadata, &admission, Arc::clone(&secret_store)),
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
    let root = bootstrap_root(paths)?;
    let metadata = load_metadata(&root)?;
    request_exact_at_root(&root, metadata, BootstrapCommand::Status).await
}

pub(super) async fn unlock(
    paths: &LocalPaths,
    captured_status: InstalledServiceBootstrapStatus,
    unlock: SecretValue,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    let metadata = required_metadata(
        captured_status,
        BootstrapRequirement::EncryptedFallbackLocked,
    )?;
    request_exact(
        paths,
        metadata,
        BootstrapCommand::UnlockEncryptedFallback(unlock),
    )
    .await
}

pub(super) async fn provide_foreground_credential(
    paths: &LocalPaths,
    captured_status: InstalledServiceBootstrapStatus,
    foreground: ForegroundRuntimeCredential,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    let metadata = required_metadata(
        captured_status,
        BootstrapRequirement::ForegroundKeyringCredential,
    )?;
    let (installation_id, reference, credential) = foreground.into_parts();
    if installation_id != metadata.installation_id {
        return Err(InstalledServiceError::BootstrapRejected);
    }
    request_exact(
        paths,
        metadata,
        BootstrapCommand::ProvideForegroundCredential {
            reference,
            credential,
        },
    )
    .await
}

async fn request_exact(
    paths: &LocalPaths,
    metadata: BootstrapMetadata,
    command: BootstrapCommand,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    let root = bootstrap_root(paths)?;
    request_exact_at_root(&root, metadata, command).await
}

async fn request_exact_at_root(
    root: &Path,
    metadata: BootstrapMetadata,
    command: BootstrapCommand,
) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
    let deadline = Instant::now()
        .checked_add(CONNECTION_TIMEOUT)
        .ok_or(InstalledServiceError::BootstrapUnavailable)?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(InstalledServiceError::BootstrapDeadline)?;
    tokio::time::timeout(remaining, async move {
        let mut stream = platform::connect(root).await?;
        stream.write_all(PREFACE).await?;
        let frame = encode_request(metadata, command)?;
        write_frame(&mut stream, &frame).await?;
        drop(frame);
        stream
            .shutdown()
            .await
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
        let response = read_frame(&mut stream)
            .await
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
        let status = decode_response(&response, metadata)?;
        require_no_trailing_data(&mut stream).await?;
        Ok(status)
    })
    .await
    .map_err(|_elapsed| InstalledServiceError::BootstrapDeadline)?
}

async fn serve_connection(
    mut stream: platform::Stream,
    metadata: BootstrapMetadata,
    admission: &BootstrapAdmission,
    secret_store: Arc<dyn SecretStore>,
) -> Result<Option<BootstrapAction>, InstalledServiceError> {
    platform::authenticate_preface(&mut stream).await?;
    let frame = read_frame(&mut stream).await?;
    let request = decode_request(&frame, metadata)?;
    drop(frame);
    require_no_trailing_data(&mut stream).await?;
    if Instant::now() >= request.deadline {
        return Err(InstalledServiceError::BootstrapDeadline);
    }
    let (code, action) = match request.command {
        BootstrapCommand::Status => (ResponseCode::Required, None),
        BootstrapCommand::ProvideForegroundCredential {
            reference,
            credential,
        } if metadata.requirement == BootstrapRequirement::ForegroundKeyringCredential
            && admission.admits_foreground_reference(&reference) =>
        {
            let foreground = ForegroundRuntimeCredential::try_new(
                metadata.installation_id,
                reference,
                credential,
            )?;
            (
                ResponseCode::Retrying,
                Some(BootstrapAction::ForegroundCredential(foreground)),
            )
        }
        BootstrapCommand::UnlockEncryptedFallback(unlock)
            if metadata.requirement == BootstrapRequirement::EncryptedFallbackLocked
                && admission.admits_fallback_unlock() =>
        {
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
        BootstrapCommand::ProvideForegroundCredential { .. }
        | BootstrapCommand::UnlockEncryptedFallback(_) => (ResponseCode::Rejected, None),
    };
    let response = encode_response(metadata, code);
    write_frame(&mut stream, &response).await?;
    // Returning immediately after the acknowledged frame keeps credential handoff and connection
    // close in the same poll; there is no cancellation point where the client can observe
    // acceptance after the server has dropped the accepted credential.
    Ok(action)
}

fn required_metadata(
    status: InstalledServiceBootstrapStatus,
    requirement: BootstrapRequirement,
) -> Result<BootstrapMetadata, InstalledServiceError> {
    if status.state != InstalledServiceBootstrapState::Required
        || status.requirement != Some(requirement)
        || status.generation == 0
    {
        return Err(InstalledServiceError::BootstrapRejected);
    }
    Ok(BootstrapMetadata {
        installation_id: status.installation_id,
        generation: status.generation,
        requirement,
    })
}

async fn write_frame(
    stream: &mut platform::Stream,
    frame: &[u8],
) -> Result<(), InstalledServiceError> {
    if frame.is_empty() || frame.len() > MAXIMUM_FRAME_BYTES {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let length = u32::try_from(frame.len())
        .map_err(|_error| InstalledServiceError::BootstrapProtocol)?
        .to_be_bytes();
    stream
        .write_all(&length)
        .await
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    stream
        .write_all(frame)
        .await
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)
}

async fn read_frame(
    stream: &mut platform::Stream,
) -> Result<Zeroizing<Vec<u8>>, InstalledServiceError> {
    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .await
        .map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
    if length == 0 || length > MAXIMUM_FRAME_BYTES {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let mut frame = zeroizing_buffer(length)?;
    frame.resize(length, 0);
    stream
        .read_exact(&mut frame)
        .await
        .map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
    Ok(frame)
}

async fn require_no_trailing_data(
    stream: &mut platform::Stream,
) -> Result<(), InstalledServiceError> {
    let mut trailing = [0_u8; 1];
    let outcome = tokio::time::timeout(TRAILING_DATA_TIMEOUT, stream.read(&mut trailing)).await;
    let result = match outcome {
        Ok(Ok(0)) => Ok(()),
        Ok(Ok(_)) | Ok(Err(_)) => Err(InstalledServiceError::BootstrapProtocol),
        Err(_) => Err(InstalledServiceError::BootstrapDeadline),
    };
    trailing.zeroize();
    result
}

fn encode_request(
    metadata: BootstrapMetadata,
    command: BootstrapCommand,
) -> Result<Zeroizing<Vec<u8>>, InstalledServiceError> {
    let (command_code, payload) = match command {
        BootstrapCommand::Status => (1_u8, Zeroizing::new(Vec::new())),
        BootstrapCommand::ProvideForegroundCredential {
            reference,
            credential,
        } => {
            let reference = serde_json::to_vec(&reference)
                .map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
            let credential_bytes = credential.expose_secret().as_bytes();
            if reference.is_empty()
                || reference.len() > MAXIMUM_REFERENCE_BYTES
                || credential_bytes.is_empty()
                || credential_bytes.len() > MAXIMUM_CREDENTIAL_BYTES
            {
                return Err(InstalledServiceError::BootstrapProtocol);
            }
            let payload_length = 4_usize
                .checked_add(reference.len())
                .and_then(|length| length.checked_add(credential_bytes.len()))
                .ok_or(InstalledServiceError::BootstrapProtocol)?;
            let mut payload = zeroizing_buffer(payload_length)?;
            payload.extend_from_slice(
                &u16::try_from(reference.len())
                    .map_err(|_error| InstalledServiceError::BootstrapProtocol)?
                    .to_be_bytes(),
            );
            payload.extend_from_slice(
                &u16::try_from(credential_bytes.len())
                    .map_err(|_error| InstalledServiceError::BootstrapProtocol)?
                    .to_be_bytes(),
            );
            payload.extend_from_slice(&reference);
            payload.extend_from_slice(credential_bytes);
            drop(credential);
            (2, payload)
        }
        BootstrapCommand::UnlockEncryptedFallback(unlock) => {
            let unlock_bytes = unlock.expose_secret().as_bytes();
            if unlock_bytes.is_empty() || unlock_bytes.len() > MAXIMUM_UNLOCK_BYTES {
                return Err(InstalledServiceError::BootstrapProtocol);
            }
            let mut payload = zeroizing_buffer(unlock_bytes.len())?;
            payload.extend_from_slice(unlock_bytes);
            drop(unlock);
            (3, payload)
        }
    };
    let payload_len =
        u16::try_from(payload.len()).map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
    let frame_length = 33_usize
        .checked_add(payload.len())
        .filter(|length| *length <= MAXIMUM_FRAME_BYTES)
        .ok_or(InstalledServiceError::BootstrapProtocol)?;
    let mut encoded = zeroizing_buffer(frame_length)?;
    encoded.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    encoded.extend_from_slice(metadata.installation_id.as_uuid().as_bytes());
    encoded.extend_from_slice(&metadata.generation.to_be_bytes());
    encoded.push(command_code);
    encoded.extend_from_slice(
        &u32::try_from(CONNECTION_TIMEOUT.as_millis())
            .map_err(|_error| InstalledServiceError::BootstrapProtocol)?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&payload);
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
        2 => decode_foreground_credential(&encoded[33..])?,
        3 if payload_len <= MAXIMUM_UNLOCK_BYTES => BootstrapCommand::UnlockEncryptedFallback(
            decode_secret(&encoded[33..], MAXIMUM_UNLOCK_BYTES)?,
        ),
        _ => return Err(InstalledServiceError::BootstrapProtocol),
    };
    Ok(BootstrapRequest { deadline, command })
}

fn decode_foreground_credential(payload: &[u8]) -> Result<BootstrapCommand, InstalledServiceError> {
    if payload.len() < 4 {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let reference_len = usize::from(u16::from_be_bytes([payload[0], payload[1]]));
    let credential_len = usize::from(u16::from_be_bytes([payload[2], payload[3]]));
    if reference_len == 0
        || reference_len > MAXIMUM_REFERENCE_BYTES
        || credential_len == 0
        || credential_len > MAXIMUM_CREDENTIAL_BYTES
        || 4_usize
            .checked_add(reference_len)
            .and_then(|length| length.checked_add(credential_len))
            != Some(payload.len())
    {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let reference_end = 4 + reference_len;
    let encoded_reference = &payload[4..reference_end];
    let reference: SecretRef = serde_json::from_slice(encoded_reference)
        .map_err(|_error| InstalledServiceError::BootstrapRejected)?;
    let canonical_reference = serde_json::to_vec(&reference)
        .map_err(|_error| InstalledServiceError::BootstrapProtocol)?;
    if canonical_reference != encoded_reference {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let credential = decode_secret(&payload[reference_end..], MAXIMUM_CREDENTIAL_BYTES)?;
    Ok(BootstrapCommand::ProvideForegroundCredential {
        reference,
        credential,
    })
}

fn decode_secret(
    encoded: &[u8],
    maximum_bytes: usize,
) -> Result<SecretValue, InstalledServiceError> {
    if encoded.is_empty() || encoded.len() > maximum_bytes {
        return Err(InstalledServiceError::BootstrapRejected);
    }
    let mut secret = zeroizing_buffer(encoded.len())?;
    secret.extend_from_slice(encoded);
    SecretValue::from_utf8_bytes(std::mem::take(&mut *secret))
        .map_err(|_error| InstalledServiceError::BootstrapRejected)
}

fn zeroizing_buffer(capacity: usize) -> Result<Zeroizing<Vec<u8>>, InstalledServiceError> {
    let mut buffer = Zeroizing::new(Vec::new());
    buffer
        .try_reserve_exact(capacity)
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
    Ok(buffer)
}

fn encode_response(metadata: BootstrapMetadata, code: ResponseCode) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(28);
    encoded.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    encoded.extend_from_slice(metadata.installation_id.as_uuid().as_bytes());
    encoded.extend_from_slice(&metadata.generation.to_be_bytes());
    encoded.push(code as u8);
    encoded.push(match metadata.requirement {
        BootstrapRequirement::EncryptedFallbackLocked => 1,
        BootstrapRequirement::ForegroundKeyringCredential => 2,
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
        2 => BootstrapRequirement::ForegroundKeyringCredential,
        _ => return Err(InstalledServiceError::BootstrapProtocol),
    };
    if requirement != metadata.requirement {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
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
        BootstrapRequirement::ForegroundKeyringCredential => 2,
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
        2 => BootstrapRequirement::ForegroundKeyringCredential,
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
