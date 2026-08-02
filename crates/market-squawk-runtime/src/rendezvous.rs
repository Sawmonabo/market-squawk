//! Authenticated, crash-safe discovery for the single installed per-user service.

use std::{
    fmt,
    net::{Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
};

use hmac::{Hmac, Mac as _};
use market_squawk_domain::Timestamp;
use market_squawk_platform::{LocalAuthorityStateStore, SecretValue};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;

use crate::{ApplicationProtocolRange, RuntimeIdentity};

const RENDEZVOUS_FORMAT_VERSION: u16 = 1;
const RENDEZVOUS_MAC_DOMAIN: &[u8] = b"market-squawk-rendezvous-v1\0";
const MINIMUM_SIGNING_KEY_BYTES: usize = 32;

type HmacSha256 = Hmac<Sha256>;

/// Operating-system process identity including a start-time discriminator.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProcessIdentity {
    process_id: NonZeroU32,
    start_identity: NonZeroU64,
}

impl ProcessIdentity {
    /// Creates an identity that cannot alias a zero/sentinel process value.
    pub fn try_new(process_id: u32, start_identity: u64) -> Result<Self, RendezvousError> {
        Ok(Self {
            process_id: NonZeroU32::new(process_id).ok_or(RendezvousError::InvalidRecord)?,
            start_identity: NonZeroU64::new(start_identity)
                .ok_or(RendezvousError::InvalidRecord)?,
        })
    }

    /// Process identifier observed when the service published the record.
    #[must_use]
    pub const fn process_id(self) -> u32 {
        self.process_id.get()
    }

    /// Platform-derived process-start discriminator.
    #[must_use]
    pub const fn start_identity(self) -> u64 {
        self.start_identity.get()
    }
}

/// Credential-free discovery record for one exact running service.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RendezvousRecord {
    runtime: RuntimeIdentity,
    endpoint: SocketAddr,
    protocols: ApplicationProtocolRange,
    process: ProcessIdentity,
    published_at: Timestamp,
}

impl RendezvousRecord {
    /// Creates a loopback-only discovery record.
    pub fn try_new(
        runtime: RuntimeIdentity,
        endpoint: SocketAddr,
        protocols: ApplicationProtocolRange,
        process: ProcessIdentity,
        published_at: Timestamp,
    ) -> Result<Self, RendezvousError> {
        if endpoint.ip() != Ipv4Addr::LOCALHOST
            || endpoint.port() == 0
            || published_at.unix_nanos() <= 0
        {
            return Err(RendezvousError::InvalidRecord);
        }
        Ok(Self {
            runtime,
            endpoint,
            protocols,
            process,
            published_at,
        })
    }

    /// Exact runtime identity discovered by the client.
    #[must_use]
    pub const fn runtime(&self) -> RuntimeIdentity {
        self.runtime
    }

    /// Loopback endpoint selected during installation or repair.
    #[must_use]
    pub const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    /// Closed application protocol range.
    #[must_use]
    pub const fn protocols(&self) -> ApplicationProtocolRange {
        self.protocols
    }

    /// Exact process/start identity that must still own the endpoint.
    #[must_use]
    pub const fn process_identity(&self) -> ProcessIdentity {
        self.process
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SignedRendezvous {
    format_version: u16,
    record: RendezvousRecord,
    tag: String,
}

/// Platform verifier that rejects PID reuse and stale discovery records.
pub trait ProcessIdentityVerifier: fmt::Debug + Send + Sync {
    /// Returns true only when both PID and start discriminator still identify one process.
    fn is_current(&self, identity: ProcessIdentity) -> Result<bool, RendezvousError>;
}

/// Exclusive crash-safe rendezvous publisher and reader.
pub struct RendezvousAuthority {
    root: PathBuf,
    signing_key: SecretValue,
}

impl fmt::Debug for RendezvousAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RendezvousAuthority([OWNER-ONLY AUTHORITY])")
    }
}

impl RendezvousAuthority {
    /// Validates the owner-only authority-state root used by short atomic operations.
    pub fn try_open(
        root: impl AsRef<Path>,
        signing_key: SecretValue,
    ) -> Result<Self, RendezvousError> {
        if signing_key.expose_secret().len() < MINIMUM_SIGNING_KEY_BYTES {
            return Err(RendezvousError::InvalidSigningKey);
        }
        let root = root.as_ref().to_path_buf();
        let store =
            LocalAuthorityStateStore::try_open(&root).map_err(|_| RendezvousError::Storage)?;
        drop(store);
        Ok(Self { root, signing_key })
    }

    /// Atomically publishes an authenticated credential-free record.
    pub fn publish(&self, record: &RendezvousRecord) -> Result<(), RendezvousError> {
        let encoded = encode_record(record, &self.signing_key)?;
        let store =
            LocalAuthorityStateStore::try_open(&self.root).map_err(|_| RendezvousError::Storage)?;
        store.store(&encoded).map_err(|_| RendezvousError::Storage)
    }

    /// Loads, authenticates, and verifies the current process-start identity.
    pub fn load(
        &self,
        verifier: &dyn ProcessIdentityVerifier,
    ) -> Result<Option<RendezvousRecord>, RendezvousError> {
        self.encoded_current()?
            .map(|encoded| self.verify_encoded(&encoded, verifier))
            .transpose()
    }

    /// Returns the current signed bytes for transport to a native client.
    pub fn encoded_current(&self) -> Result<Option<Vec<u8>>, RendezvousError> {
        let store =
            LocalAuthorityStateStore::try_open(&self.root).map_err(|_| RendezvousError::Storage)?;
        store.load().map_err(|_| RendezvousError::Storage)
    }

    /// Authenticates supplied bytes before trusting any discovery field.
    pub fn verify_encoded(
        &self,
        encoded: &[u8],
        verifier: &dyn ProcessIdentityVerifier,
    ) -> Result<RendezvousRecord, RendezvousError> {
        let signed: SignedRendezvous =
            serde_json::from_slice(encoded).map_err(|_| RendezvousError::Malformed)?;
        if signed.format_version != RENDEZVOUS_FORMAT_VERSION {
            return Err(RendezvousError::UnsupportedVersion);
        }
        let payload = serde_json::to_vec(&signed.record).map_err(|_| RendezvousError::Malformed)?;
        let tag = decode_hex_tag(&signed.tag)?;
        let mut mac = rendezvous_mac(&self.signing_key)?;
        mac.update(RENDEZVOUS_MAC_DOMAIN);
        mac.update(&payload);
        mac.verify_slice(&tag)
            .map_err(|_| RendezvousError::AuthenticationFailed)?;
        if !verifier.is_current(signed.record.process)? {
            return Err(RendezvousError::StaleProcess);
        }
        Ok(signed.record)
    }
}

fn encode_record(
    record: &RendezvousRecord,
    signing_key: &SecretValue,
) -> Result<Vec<u8>, RendezvousError> {
    let payload = serde_json::to_vec(record).map_err(|_| RendezvousError::Malformed)?;
    let mut mac = rendezvous_mac(signing_key)?;
    mac.update(RENDEZVOUS_MAC_DOMAIN);
    mac.update(&payload);
    let tag = encode_hex(&mac.finalize().into_bytes());
    serde_json::to_vec(&SignedRendezvous {
        format_version: RENDEZVOUS_FORMAT_VERSION,
        record: record.clone(),
        tag,
    })
    .map_err(|_| RendezvousError::Malformed)
}

fn rendezvous_mac(key: &SecretValue) -> Result<HmacSha256, RendezvousError> {
    HmacSha256::new_from_slice(key.expose_secret().as_bytes())
        .map_err(|_| RendezvousError::InvalidSigningKey)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex_tag(value: &str) -> Result<[u8; 32], RendezvousError> {
    if value.len() != 64 {
        return Err(RendezvousError::AuthenticationFailed);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_nibble(pair[0]).ok_or(RendezvousError::AuthenticationFailed)?;
        let low = decode_nibble(pair[1]).ok_or(RendezvousError::AuthenticationFailed)?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

const fn decode_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

/// Rendezvous publication, authentication, or process-identity failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RendezvousError {
    /// Endpoint, process identity, or timestamp is invalid.
    #[error("rendezvous record is invalid")]
    InvalidRecord,
    /// Signing key is below the fixed entropy/length floor.
    #[error("rendezvous signing key is invalid")]
    InvalidSigningKey,
    /// Owner-only authority state cannot be opened, reconciled, read, or written.
    #[error("rendezvous storage is unavailable")]
    Storage,
    /// Signed bytes are not one closed record.
    #[error("rendezvous record is malformed")]
    Malformed,
    /// Signed record format is unsupported.
    #[error("rendezvous record version is unsupported")]
    UnsupportedVersion,
    /// The record's authentication tag does not verify.
    #[error("rendezvous authentication failed")]
    AuthenticationFailed,
    /// PID or process-start identity is no longer current.
    #[error("rendezvous process identity is stale")]
    StaleProcess,
}
