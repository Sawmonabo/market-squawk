//! Threshold-signed update-metadata admission and monotonic local trust state.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Component, Path, PathBuf};

use atomicwrites::{AllowOverwrite, AtomicFile};
use chrono::{DateTime, SecondsFormat, Utc};
use ed25519_dalek::{Signature, VerifyingKey};
use olpc_cjson::CanonicalFormatter;
use serde::de::{DeserializeOwned, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const SPEC_VERSION: &str = "1.0.35";
const STATE_SCHEMA_VERSION: u32 = 1;
const STATE_FILE: &str = "trusted-update-metadata.json";
const MAXIMUM_METADATA_BYTES: usize = 1024 * 1024;
const MAXIMUM_STATE_BYTES: usize = 2 * MAXIMUM_METADATA_BYTES;
const MAXIMUM_ROOT_CHAIN: usize = 32;
const MAXIMUM_KEYS: usize = 64;
const MAXIMUM_SIGNATURES: usize = 64;
const MAXIMUM_TARGETS: usize = 512;
const MAXIMUM_TARGET_PATH_BYTES: usize = 512;

/// A caller-supplied, bounded top-level metadata chain.
#[derive(Clone, Copy, Debug)]
pub struct SuppliedMetadata<'a> {
    /// Sequential root metadata beginning with the next version, or an exact current-root retry.
    pub root_chain: &'a [&'a [u8]],
    /// Unversioned `timestamp.json` bytes.
    pub timestamp: &'a [u8],
    /// Version-prefixed snapshot repository path.
    pub snapshot_path: &'a str,
    /// Snapshot metadata bytes.
    pub snapshot: &'a [u8],
    /// Version-prefixed targets repository path.
    pub targets_path: &'a str,
    /// Targets metadata bytes.
    pub targets: &'a [u8],
}

/// Exact bytes or a regular local file downloaded by the existing installer transport.
#[derive(Clone, Copy, Debug)]
pub enum TargetSource<'a> {
    /// Already bounded in-memory target bytes.
    Bytes(&'a [u8]),
    /// A downloaded regular file whose contents are streamed through SHA-256 verification.
    File(&'a Path),
}

/// One requested target and its consistent-snapshot download path.
#[derive(Clone, Copy, Debug)]
pub struct SuppliedTarget<'a> {
    /// Unprefixed target name stored in signed targets metadata.
    pub metadata_path: &'a str,
    /// SHA-256-prefixed repository path required by consistent snapshots.
    pub download_path: &'a str,
    /// Exact local bytes to verify.
    pub source: TargetSource<'a>,
}

/// Exact identity of a target admitted by signed metadata and verified local bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedTarget {
    metadata_path: Box<str>,
    download_path: Box<str>,
    length: u64,
    sha256: [u8; 32],
}

impl TrustedTarget {
    /// Returns the unprefixed signed metadata path.
    pub fn metadata_path(&self) -> &str {
        &self.metadata_path
    }

    /// Returns the SHA-256-prefixed download path.
    pub fn download_path(&self) -> &str {
        &self.download_path
    }

    /// Returns the signed byte length.
    pub const fn length(&self) -> u64 {
        self.length
    }

    /// Returns the signed and verified SHA-256 digest.
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }
}

/// Self-consistent root metadata distributed through a pinned installer trust anchor.
#[derive(Clone, Debug)]
pub struct TrustedRoot {
    envelope: Value,
    signed: RootSigned,
    digest: [u8; 32],
}

impl TrustedRoot {
    /// Admits one pinned root, including its own threshold.
    ///
    /// Expiry is checked after the sequential root update so an authentic expired root can still
    /// rotate to a current root, as required by the TUF client workflow.
    pub fn from_pinned(metadata: &[u8]) -> Result<Self, UpdateMetadataError> {
        let envelope = parse_envelope::<RootSigned>(metadata)?;
        validate_root(&envelope.signed)?;
        verify_role(&envelope, &envelope.signed, RoleName::Root)?;
        Ok(Self {
            digest: canonical_digest(&envelope.value)?,
            envelope: envelope.value,
            signed: envelope.signed,
        })
    }
}

/// Persisted monotonic update trust and the current rotatable root.
#[derive(Debug)]
pub struct TrustedUpdateStore {
    root: PathBuf,
    state: TrustedState,
    trusted_root: TrustedRoot,
    base_digest: Option<[u8; 32]>,
}

impl TrustedUpdateStore {
    /// Loads protected trust state or prepares an in-memory bootstrap from the pinned root.
    ///
    /// Bootstrap does not write anything. Trust advances only through
    /// [`PendingTrustedUpdate::persist`].
    pub fn open_or_bootstrap(
        program_root: &Path,
        pinned_root: TrustedRoot,
        trusted_time: DateTime<Utc>,
    ) -> Result<Self, UpdateMetadataError> {
        verify_private_root(program_root)?;
        let state_path = program_root.join(STATE_FILE);
        let Some((state, state_digest)) = read_state(&state_path)? else {
            return Ok(Self {
                root: program_root.to_path_buf(),
                state: TrustedState::bootstrap(&pinned_root, trusted_time),
                trusted_root: pinned_root,
                base_digest: None,
            });
        };
        state.validate()?;
        if trusted_time < state.trusted_time {
            return Err(UpdateMetadataError::TrustedTimeRollback);
        }
        let root_bytes = canonical_json(&state.root_metadata)?;
        let envelope = parse_envelope::<RootSigned>(&root_bytes)?;
        validate_root(&envelope.signed)?;
        verify_role(&envelope, &envelope.signed, RoleName::Root)?;
        let root_digest = canonical_digest(&envelope.value)?;
        if root_digest != state.root_sha256_bytes()?
            || envelope.signed.version != state.root_version
            || (state.root_version == pinned_root.signed.version
                && root_digest != pinned_root.digest)
        {
            return Err(UpdateMetadataError::CorruptState);
        }
        Ok(Self {
            root: program_root.to_path_buf(),
            state,
            trusted_root: TrustedRoot {
                envelope: envelope.value,
                signed: envelope.signed,
                digest: root_digest,
            },
            base_digest: Some(state_digest),
        })
    }

    /// Verifies a complete supplied metadata chain and exact requested target bytes.
    ///
    /// This method has no durable side effect. The caller must first admit the release manifest
    /// and archive, then explicitly persist the returned pending trust state.
    pub fn admit(
        &self,
        metadata: SuppliedMetadata<'_>,
        targets: &[SuppliedTarget<'_>],
        trusted_time: DateTime<Utc>,
    ) -> Result<PendingTrustedUpdate, UpdateMetadataError> {
        if trusted_time < self.state.trusted_time {
            return Err(UpdateMetadataError::TrustedTimeRollback);
        }
        if metadata.root_chain.len() > MAXIMUM_ROOT_CHAIN {
            return Err(UpdateMetadataError::LimitExceeded("root chain"));
        }

        let mut trusted_root = self.trusted_root.clone();
        for root_bytes in metadata.root_chain {
            let candidate = parse_envelope::<RootSigned>(root_bytes)?;
            validate_root(&candidate.signed)?;
            let candidate_digest = canonical_digest(&candidate.value)?;
            if candidate.signed.version == trusted_root.signed.version {
                if candidate_digest != trusted_root.digest {
                    return Err(UpdateMetadataError::ChangedMetadata(
                        RoleName::Root.as_str(),
                    ));
                }
                continue;
            }
            if candidate.signed.version != trusted_root.signed.version.saturating_add(1) {
                return Err(UpdateMetadataError::MetadataRollback(
                    RoleName::Root.as_str(),
                ));
            }
            verify_role(&candidate, &trusted_root.signed, RoleName::Root)?;
            verify_role(&candidate, &candidate.signed, RoleName::Root)?;
            trusted_root = TrustedRoot {
                envelope: candidate.value,
                signed: candidate.signed,
                digest: candidate_digest,
            };
        }
        verify_expiry(RoleName::Root, &trusted_root.signed.expires, trusted_time)?;

        let timestamp = parse_envelope::<TimestampSigned>(metadata.timestamp)?;
        validate_common(
            RoleName::Timestamp,
            &timestamp.signed.kind,
            &timestamp.signed.spec_version,
            timestamp.signed.version,
        )?;
        verify_expiry(RoleName::Timestamp, &timestamp.signed.expires, trusted_time)?;
        verify_role(&timestamp, &trusted_root.signed, RoleName::Timestamp)?;
        let timestamp_digest: [u8; 32] = Sha256::digest(metadata.timestamp).into();
        admit_monotonic(
            RoleName::Timestamp,
            self.state.timestamp_version,
            self.state.timestamp_sha256.as_deref(),
            timestamp.signed.version,
            timestamp_digest,
        )?;
        let snapshot_description = exact_metadata_description(
            &timestamp.signed.meta,
            "snapshot.json",
            RoleName::Timestamp,
        )?;
        verify_metadata_file(
            metadata.snapshot,
            metadata.snapshot_path,
            "snapshot.json",
            snapshot_description,
        )?;

        let snapshot = parse_envelope::<SnapshotSigned>(metadata.snapshot)?;
        validate_common(
            RoleName::Snapshot,
            &snapshot.signed.kind,
            &snapshot.signed.spec_version,
            snapshot.signed.version,
        )?;
        if snapshot.signed.version != snapshot_description.version {
            return Err(UpdateMetadataError::MixAndMatch(
                RoleName::Snapshot.as_str(),
            ));
        }
        verify_expiry(RoleName::Snapshot, &snapshot.signed.expires, trusted_time)?;
        verify_role(&snapshot, &trusted_root.signed, RoleName::Snapshot)?;
        let snapshot_digest: [u8; 32] = Sha256::digest(metadata.snapshot).into();
        admit_monotonic(
            RoleName::Snapshot,
            self.state.snapshot_version,
            self.state.snapshot_sha256.as_deref(),
            snapshot.signed.version,
            snapshot_digest,
        )?;
        let targets_description =
            exact_metadata_description(&snapshot.signed.meta, "targets.json", RoleName::Snapshot)?;
        verify_metadata_file(
            metadata.targets,
            metadata.targets_path,
            "targets.json",
            targets_description,
        )?;

        let targets_envelope = parse_envelope::<TargetsSigned>(metadata.targets)?;
        validate_common(
            RoleName::Targets,
            &targets_envelope.signed.kind,
            &targets_envelope.signed.spec_version,
            targets_envelope.signed.version,
        )?;
        if targets_envelope.signed.version != targets_description.version {
            return Err(UpdateMetadataError::MixAndMatch(RoleName::Targets.as_str()));
        }
        verify_expiry(
            RoleName::Targets,
            &targets_envelope.signed.expires,
            trusted_time,
        )?;
        verify_role(&targets_envelope, &trusted_root.signed, RoleName::Targets)?;
        if targets_envelope.signed.delegations.is_some() {
            return Err(UpdateMetadataError::InvalidMetadata);
        }
        let targets_digest: [u8; 32] = Sha256::digest(metadata.targets).into();
        admit_monotonic(
            RoleName::Targets,
            self.state.targets_version,
            self.state.targets_sha256.as_deref(),
            targets_envelope.signed.version,
            targets_digest,
        )?;
        let admitted_targets = verify_targets(&targets_envelope.signed.targets, targets)?;

        let state = TrustedState {
            schema_version: STATE_SCHEMA_VERSION,
            root_metadata: trusted_root.envelope.clone(),
            root_version: trusted_root.signed.version,
            root_sha256: hex(&trusted_root.digest).into(),
            timestamp_version: timestamp.signed.version,
            timestamp_sha256: Some(hex(&timestamp_digest).into()),
            snapshot_version: snapshot.signed.version,
            snapshot_sha256: Some(hex(&snapshot_digest).into()),
            targets_version: targets_envelope.signed.version,
            targets_sha256: Some(hex(&targets_digest).into()),
            trusted_time,
        };
        state.validate()?;
        Ok(PendingTrustedUpdate {
            root: self.root.clone(),
            state,
            base_digest: self.base_digest,
            targets: admitted_targets,
        })
    }
}

/// Fully verified update trust that remains in memory until explicit commit.
#[derive(Debug)]
pub struct PendingTrustedUpdate {
    root: PathBuf,
    state: TrustedState,
    base_digest: Option<[u8; 32]>,
    targets: BTreeMap<Box<str>, TrustedTarget>,
}

impl PendingTrustedUpdate {
    /// Returns one verified target identity by its signed metadata path.
    pub fn target(&self, metadata_path: &str) -> Option<&TrustedTarget> {
        self.targets.get(metadata_path)
    }

    /// Atomically advances trusted metadata. There is deliberately no rollback operation.
    pub fn persist(self) -> Result<TrustedUpdateReceipt, UpdateMetadataError> {
        let path = self.root.join(STATE_FILE);
        let current = read_state(&path)?.map(|(_, digest)| digest);
        if current != self.base_digest {
            return Err(UpdateMetadataError::ConcurrentStateChange);
        }
        write_state(&self.root, &path, &self.state)?;
        Ok(TrustedUpdateReceipt {
            root_version: self.state.root_version,
            timestamp_version: self.state.timestamp_version,
            snapshot_version: self.state.snapshot_version,
            targets_version: self.state.targets_version,
        })
    }
}

/// Versions durably committed by one trusted update admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustedUpdateReceipt {
    root_version: u64,
    timestamp_version: u64,
    snapshot_version: u64,
    targets_version: u64,
}

impl TrustedUpdateReceipt {
    /// Returns the committed root version.
    pub const fn root_version(self) -> u64 {
        self.root_version
    }

    /// Returns the committed timestamp version.
    pub const fn timestamp_version(self) -> u64 {
        self.timestamp_version
    }

    /// Returns the committed snapshot version.
    pub const fn snapshot_version(self) -> u64 {
        self.snapshot_version
    }

    /// Returns the committed targets version.
    pub const fn targets_version(self) -> u64 {
        self.targets_version
    }
}

/// Fail-closed trusted update metadata error.
#[derive(Debug, Error)]
pub enum UpdateMetadataError {
    #[error("update metadata is malformed or unsupported")]
    InvalidMetadata,
    #[error("update metadata exceeded the {0} bound")]
    LimitExceeded(&'static str),
    #[error("{0} metadata did not meet its trusted signature threshold")]
    SignatureThreshold(&'static str),
    #[error("{0} metadata is expired")]
    Expired(&'static str),
    #[error("{0} metadata attempted a version rollback")]
    MetadataRollback(&'static str),
    #[error("{0} reused a trusted version with changed bytes")]
    ChangedMetadata(&'static str),
    #[error("{0} metadata does not match its signed parent")]
    MixAndMatch(&'static str),
    #[error("trusted time moved backwards")]
    TrustedTimeRollback,
    #[error("trusted update state is corrupt")]
    CorruptState,
    #[error("trusted update state changed concurrently")]
    ConcurrentStateChange,
    #[error("update target path is invalid")]
    InvalidTargetPath,
    #[error("update target is absent from signed targets metadata")]
    UnknownTarget,
    #[error("update target length or SHA-256 does not match signed metadata")]
    TargetMismatch,
    #[error("trusted update state path is unsafe")]
    UnsafeStatePath,
    #[error("{operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Copy, Debug)]
enum RoleName {
    Root,
    Targets,
    Snapshot,
    Timestamp,
}

impl std::fmt::Display for RoleName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Root => "root",
            Self::Targets => "targets",
            Self::Snapshot => "snapshot",
            Self::Timestamp => "timestamp",
        })
    }
}

impl RoleName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Targets => "targets",
            Self::Snapshot => "snapshot",
            Self::Timestamp => "timestamp",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RootSigned {
    #[serde(rename = "_type")]
    kind: Box<str>,
    spec_version: Box<str>,
    version: u64,
    expires: Box<str>,
    consistent_snapshot: bool,
    keys: BTreeMap<Box<str>, RootKey>,
    roles: BTreeMap<Box<str>, RoleDefinition>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RootKey {
    keytype: Box<str>,
    scheme: Box<str>,
    keyval: KeyValue,
    #[serde(flatten)]
    extra: BTreeMap<Box<str>, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct KeyValue {
    public: Box<str>,
    #[serde(flatten)]
    extra: BTreeMap<Box<str>, Value>,
}

#[derive(Clone, Debug, Deserialize)]
struct RoleDefinition {
    keyids: Vec<Box<str>>,
    threshold: u64,
}

#[derive(Debug, Deserialize)]
struct TimestampSigned {
    #[serde(rename = "_type")]
    kind: Box<str>,
    spec_version: Box<str>,
    version: u64,
    expires: Box<str>,
    meta: BTreeMap<Box<str>, MetadataDescription>,
}

#[derive(Debug, Deserialize)]
struct SnapshotSigned {
    #[serde(rename = "_type")]
    kind: Box<str>,
    spec_version: Box<str>,
    version: u64,
    expires: Box<str>,
    meta: BTreeMap<Box<str>, MetadataDescription>,
}

#[derive(Debug, Deserialize)]
struct TargetsSigned {
    #[serde(rename = "_type")]
    kind: Box<str>,
    spec_version: Box<str>,
    version: u64,
    expires: Box<str>,
    targets: BTreeMap<Box<str>, TargetDescription>,
    #[serde(default)]
    delegations: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct MetadataDescription {
    version: u64,
    length: u64,
    hashes: BTreeMap<Box<str>, Box<str>>,
}

#[derive(Debug, Deserialize)]
struct TargetDescription {
    length: u64,
    hashes: BTreeMap<Box<str>, Box<str>>,
    #[serde(default)]
    custom: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignatureEntry {
    keyid: Box<str>,
    sig: Box<str>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeFields {
    signatures: Vec<SignatureEntry>,
    signed: Value,
}

#[derive(Debug)]
struct ParsedEnvelope<T> {
    value: Value,
    signed_value: Value,
    signed: T,
    signatures: Vec<SignatureEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TrustedState {
    schema_version: u32,
    root_metadata: Value,
    root_version: u64,
    root_sha256: Box<str>,
    timestamp_version: u64,
    timestamp_sha256: Option<Box<str>>,
    snapshot_version: u64,
    snapshot_sha256: Option<Box<str>>,
    targets_version: u64,
    targets_sha256: Option<Box<str>>,
    trusted_time: DateTime<Utc>,
}

impl TrustedState {
    fn bootstrap(root: &TrustedRoot, trusted_time: DateTime<Utc>) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            root_metadata: root.envelope.clone(),
            root_version: root.signed.version,
            root_sha256: hex(&root.digest).into(),
            timestamp_version: 0,
            timestamp_sha256: None,
            snapshot_version: 0,
            snapshot_sha256: None,
            targets_version: 0,
            targets_sha256: None,
            trusted_time,
        }
    }

    fn validate(&self) -> Result<(), UpdateMetadataError> {
        if self.schema_version != STATE_SCHEMA_VERSION
            || self.root_version == 0
            || decode_hex::<32>(&self.root_sha256).is_err()
            || !valid_version_digest(self.timestamp_version, self.timestamp_sha256.as_deref())
            || !valid_version_digest(self.snapshot_version, self.snapshot_sha256.as_deref())
            || !valid_version_digest(self.targets_version, self.targets_sha256.as_deref())
        {
            return Err(UpdateMetadataError::CorruptState);
        }
        Ok(())
    }

    fn root_sha256_bytes(&self) -> Result<[u8; 32], UpdateMetadataError> {
        decode_hex(&self.root_sha256).map_err(|_| UpdateMetadataError::CorruptState)
    }
}

fn valid_version_digest(version: u64, digest: Option<&str>) -> bool {
    match (version, digest) {
        (0, None) => true,
        (1.., Some(value)) => decode_hex::<32>(value).is_ok(),
        _ => false,
    }
}

fn parse_envelope<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<ParsedEnvelope<T>, UpdateMetadataError> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_METADATA_BYTES {
        return Err(UpdateMetadataError::LimitExceeded("metadata bytes"));
    }
    let value = unique_json(bytes)?;
    let fields: EnvelopeFields =
        serde_json::from_value(value.clone()).map_err(|_| UpdateMetadataError::InvalidMetadata)?;
    if fields.signatures.is_empty() || fields.signatures.len() > MAXIMUM_SIGNATURES {
        return Err(UpdateMetadataError::InvalidMetadata);
    }
    let signed: T = serde_json::from_value(fields.signed.clone())
        .map_err(|_| UpdateMetadataError::InvalidMetadata)?;
    Ok(ParsedEnvelope {
        value,
        signed_value: fields.signed,
        signed,
        signatures: fields.signatures,
    })
}

fn validate_root(root: &RootSigned) -> Result<(), UpdateMetadataError> {
    validate_common(RoleName::Root, &root.kind, &root.spec_version, root.version)?;
    if !root.consistent_snapshot
        || root.keys.is_empty()
        || root.keys.len() > MAXIMUM_KEYS
        || root.roles.len() < 4
        || root.roles.len() > MAXIMUM_KEYS
    {
        return Err(UpdateMetadataError::InvalidMetadata);
    }
    for (key_id, key) in &root.keys {
        if key.keytype.as_ref() != "ed25519" || key.scheme.as_ref() != "ed25519" {
            return Err(UpdateMetadataError::InvalidMetadata);
        }
        let public = decode_hex::<32>(&key.keyval.public)?;
        VerifyingKey::from_bytes(&public).map_err(|_| UpdateMetadataError::InvalidMetadata)?;
        let key_value =
            serde_json::to_value(key).map_err(|_| UpdateMetadataError::InvalidMetadata)?;
        if decode_hex::<32>(key_id)? != canonical_digest(&key_value)? {
            return Err(UpdateMetadataError::InvalidMetadata);
        }
    }
    for definition in root.roles.values() {
        let threshold = usize::try_from(definition.threshold)
            .map_err(|_| UpdateMetadataError::InvalidMetadata)?;
        let unique = definition.keyids.iter().collect::<BTreeSet<_>>();
        if threshold == 0
            || threshold > definition.keyids.len()
            || unique.len() != definition.keyids.len()
            || definition
                .keyids
                .iter()
                .any(|key| !root.keys.contains_key(key))
        {
            return Err(UpdateMetadataError::InvalidMetadata);
        }
    }
    for role in [
        RoleName::Root,
        RoleName::Targets,
        RoleName::Snapshot,
        RoleName::Timestamp,
    ] {
        root.roles
            .get(role.as_str())
            .ok_or(UpdateMetadataError::InvalidMetadata)?;
    }
    Ok(())
}

fn validate_common(
    expected: RoleName,
    kind: &str,
    spec_version: &str,
    version: u64,
) -> Result<(), UpdateMetadataError> {
    if kind != expected.as_str() || spec_version != SPEC_VERSION || version == 0 {
        return Err(UpdateMetadataError::InvalidMetadata);
    }
    Ok(())
}

fn verify_role<T>(
    envelope: &ParsedEnvelope<T>,
    root: &RootSigned,
    role: RoleName,
) -> Result<(), UpdateMetadataError> {
    let definition = root
        .roles
        .get(role.as_str())
        .ok_or(UpdateMetadataError::InvalidMetadata)?;
    let threshold =
        usize::try_from(definition.threshold).map_err(|_| UpdateMetadataError::InvalidMetadata)?;
    let canonical = canonical_json(&envelope.signed_value)?;
    let mut seen = BTreeSet::new();
    let mut valid = 0_usize;
    for signature_entry in &envelope.signatures {
        let signature_id = decode_hex::<32>(&signature_entry.keyid)?;
        if !seen.insert(signature_id) {
            return Err(UpdateMetadataError::InvalidMetadata);
        }
        let signature_bytes = decode_hex::<64>(&signature_entry.sig)?;
        let signature = Signature::from_bytes(&signature_bytes);
        if definition
            .keyids
            .iter()
            .any(|key| key.as_ref() == signature_entry.keyid.as_ref())
        {
            let key = root
                .keys
                .get(&signature_entry.keyid)
                .ok_or(UpdateMetadataError::InvalidMetadata)?;
            let public = decode_hex::<32>(&key.keyval.public)?;
            let verifying = VerifyingKey::from_bytes(&public)
                .map_err(|_| UpdateMetadataError::InvalidMetadata)?;
            if verifying.verify_strict(&canonical, &signature).is_ok() {
                valid = valid.saturating_add(1);
            }
        }
    }
    if valid < threshold {
        return Err(UpdateMetadataError::SignatureThreshold(role.as_str()));
    }
    Ok(())
}

fn verify_expiry(
    role: RoleName,
    expires: &str,
    trusted_time: DateTime<Utc>,
) -> Result<(), UpdateMetadataError> {
    let parsed = DateTime::parse_from_rfc3339(expires)
        .map_err(|_| UpdateMetadataError::InvalidMetadata)?
        .with_timezone(&Utc);
    if parsed.to_rfc3339_opts(SecondsFormat::Secs, true) != expires {
        return Err(UpdateMetadataError::InvalidMetadata);
    }
    if parsed <= trusted_time {
        return Err(UpdateMetadataError::Expired(role.as_str()));
    }
    Ok(())
}

fn admit_monotonic(
    role: RoleName,
    stored_version: u64,
    stored_digest: Option<&str>,
    candidate_version: u64,
    candidate_digest: [u8; 32],
) -> Result<(), UpdateMetadataError> {
    if candidate_version < stored_version {
        return Err(UpdateMetadataError::MetadataRollback(role.as_str()));
    }
    if candidate_version == stored_version && stored_version != 0 {
        let stored = stored_digest.ok_or(UpdateMetadataError::CorruptState)?;
        if decode_hex::<32>(stored)? != candidate_digest {
            return Err(UpdateMetadataError::ChangedMetadata(role.as_str()));
        }
    }
    Ok(())
}

fn exact_metadata_description<'a>(
    metadata: &'a BTreeMap<Box<str>, MetadataDescription>,
    name: &str,
    parent: RoleName,
) -> Result<&'a MetadataDescription, UpdateMetadataError> {
    if metadata.len() != 1 {
        return Err(UpdateMetadataError::MixAndMatch(parent.as_str()));
    }
    metadata
        .get(name)
        .ok_or(UpdateMetadataError::MixAndMatch(parent.as_str()))
}

fn verify_metadata_file(
    bytes: &[u8],
    supplied_path: &str,
    base_name: &str,
    description: &MetadataDescription,
) -> Result<(), UpdateMetadataError> {
    let expected_path = format!("{}.{}", description.version, base_name);
    let supplied_digest: [u8; 32] = Sha256::digest(bytes).into();
    if supplied_path != expected_path
        || description.length != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || exact_sha256(&description.hashes)? != supplied_digest
    {
        return Err(UpdateMetadataError::MixAndMatch(
            match base_name {
                "snapshot.json" => RoleName::Snapshot,
                _ => RoleName::Targets,
            }
            .as_str(),
        ));
    }
    Ok(())
}

fn verify_targets(
    descriptions: &BTreeMap<Box<str>, TargetDescription>,
    supplied: &[SuppliedTarget<'_>],
) -> Result<BTreeMap<Box<str>, TrustedTarget>, UpdateMetadataError> {
    if descriptions.is_empty()
        || descriptions.len() > MAXIMUM_TARGETS
        || supplied.is_empty()
        || supplied.len() > MAXIMUM_TARGETS
    {
        return Err(UpdateMetadataError::LimitExceeded("targets"));
    }
    for path in descriptions.keys() {
        validate_target_path(path)?;
    }
    let mut result = BTreeMap::new();
    for target in supplied {
        validate_target_path(target.metadata_path)?;
        let description = descriptions
            .get(target.metadata_path)
            .ok_or(UpdateMetadataError::UnknownTarget)?;
        let digest = exact_sha256(&description.hashes)?;
        let download_path = consistent_target_path(target.metadata_path, digest)?;
        if target.download_path != download_path
            || source_identity(target.source, description.length)? != (description.length, digest)
        {
            return Err(UpdateMetadataError::TargetMismatch);
        }
        let identity = TrustedTarget {
            metadata_path: target.metadata_path.into(),
            download_path: target.download_path.into(),
            length: description.length,
            sha256: digest,
        };
        if result
            .insert(identity.metadata_path.clone(), identity)
            .is_some()
        {
            return Err(UpdateMetadataError::InvalidMetadata);
        }
        let _ = &description.custom;
    }
    Ok(result)
}

fn source_identity(
    source: TargetSource<'_>,
    expected_length: u64,
) -> Result<(u64, [u8; 32]), UpdateMetadataError> {
    match source {
        TargetSource::Bytes(bytes) => Ok((
            u64::try_from(bytes.len()).map_err(|_| UpdateMetadataError::TargetMismatch)?,
            Sha256::digest(bytes).into(),
        )),
        TargetSource::File(path) => {
            let metadata =
                fs::symlink_metadata(path).map_err(|source| UpdateMetadataError::Io {
                    operation: "inspect downloaded update target",
                    source,
                })?;
            if !metadata.file_type().is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() != expected_length
            {
                return Err(UpdateMetadataError::TargetMismatch);
            }
            let file = File::open(path).map_err(|source| UpdateMetadataError::Io {
                operation: "open downloaded update target",
                source,
            })?;
            let mut reader = file.take(expected_length.saturating_add(1));
            let mut buffer = [0_u8; 64 * 1024];
            let mut length = 0_u64;
            let mut hasher = Sha256::new();
            loop {
                let read = reader
                    .read(&mut buffer)
                    .map_err(|source| UpdateMetadataError::Io {
                        operation: "hash downloaded update target",
                        source,
                    })?;
                if read == 0 {
                    break;
                }
                length = length
                    .checked_add(
                        u64::try_from(read).map_err(|_| UpdateMetadataError::TargetMismatch)?,
                    )
                    .ok_or(UpdateMetadataError::TargetMismatch)?;
                if length > expected_length {
                    return Err(UpdateMetadataError::TargetMismatch);
                }
                hasher.update(&buffer[..read]);
            }
            Ok((length, hasher.finalize().into()))
        }
    }
}

fn validate_target_path(path: &str) -> Result<(), UpdateMetadataError> {
    if path.is_empty()
        || path.len() > MAXIMUM_TARGET_PATH_BYTES
        || path.contains('\\')
        || path.bytes().any(|byte| byte.is_ascii_control())
        || Path::new(path).is_absolute()
        || Path::new(path).components().any(|component| {
            !matches!(component, Component::Normal(_)) || component.as_os_str().to_str().is_none()
        })
    {
        return Err(UpdateMetadataError::InvalidTargetPath);
    }
    Ok(())
}

fn consistent_target_path(path: &str, digest: [u8; 32]) -> Result<String, UpdateMetadataError> {
    validate_target_path(path)?;
    let (directory, file) = path.rsplit_once('/').unwrap_or(("", path));
    let prefix = hex(&digest);
    if directory.is_empty() {
        Ok(format!("{prefix}.{file}"))
    } else {
        Ok(format!("{directory}/{prefix}.{file}"))
    }
}

fn exact_sha256(hashes: &BTreeMap<Box<str>, Box<str>>) -> Result<[u8; 32], UpdateMetadataError> {
    if hashes.len() != 1 {
        return Err(UpdateMetadataError::InvalidMetadata);
    }
    decode_hex(
        hashes
            .get("sha256")
            .ok_or(UpdateMetadataError::InvalidMetadata)?,
    )
}

fn read_state(path: &Path) -> Result<Option<(TrustedState, [u8; 32])>, UpdateMetadataError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(UpdateMetadataError::Io {
                operation: "inspect trusted update state",
                source,
            });
        }
    };
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAXIMUM_STATE_BYTES as u64
    {
        return Err(UpdateMetadataError::UnsafeStatePath);
    }
    let file = File::open(path).map_err(|source| UpdateMetadataError::Io {
        operation: "open trusted update state",
        source,
    })?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len()).map_err(|_| UpdateMetadataError::CorruptState)?,
    );
    file.take(MAXIMUM_STATE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| UpdateMetadataError::Io {
            operation: "read trusted update state",
            source,
        })?;
    if bytes.len() > MAXIMUM_STATE_BYTES {
        return Err(UpdateMetadataError::CorruptState);
    }
    let value = unique_json(&bytes).map_err(|_| UpdateMetadataError::CorruptState)?;
    let state = serde_json::from_value(value).map_err(|_| UpdateMetadataError::CorruptState)?;
    Ok(Some((state, Sha256::digest(&bytes).into())))
}

fn write_state(root: &Path, path: &Path, state: &TrustedState) -> Result<(), UpdateMetadataError> {
    let mut bytes =
        serde_json::to_vec_pretty(state).map_err(|_| UpdateMetadataError::CorruptState)?;
    bytes.push(b'\n');
    if bytes.len() > MAXIMUM_STATE_BYTES {
        return Err(UpdateMetadataError::CorruptState);
    }
    let atomic = AtomicFile::new(path, AllowOverwrite);
    atomic
        .write_with_options(
            |file| {
                file.write_all(&bytes)?;
                file.sync_all()
            },
            private_open_options(),
        )
        .map_err(|error| {
            let source: std::io::Error = error.into();
            UpdateMetadataError::Io {
                operation: "publish trusted update state",
                source,
            }
        })?;
    #[cfg(unix)]
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| UpdateMetadataError::Io {
            operation: "synchronize trusted update state directory",
            source,
        })?;
    Ok(())
}

fn private_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options
}

fn verify_private_root(root: &Path) -> Result<(), UpdateMetadataError> {
    let metadata = fs::symlink_metadata(root).map_err(|source| UpdateMetadataError::Io {
        operation: "inspect trusted update root",
        source,
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(UpdateMetadataError::UnsafeStatePath);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(UpdateMetadataError::UnsafeStatePath);
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(UpdateMetadataError::UnsafeStatePath);
        }
    }
    Ok(())
}

fn unique_json(bytes: &[u8]) -> Result<Value, UpdateMetadataError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = UniqueValue::deserialize(&mut deserializer)
        .map_err(|_| UpdateMetadataError::InvalidMetadata)?
        .0;
    deserializer
        .end()
        .map_err(|_| UpdateMetadataError::InvalidMetadata)?;
    Ok(value)
}

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueVisitor)
    }
}

struct UniqueVisitor;

impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bounded canonical-compatible JSON")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(value)))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value.to_owned())))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value)))
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<UniqueValue>()? {
            values.push(value.0);
        }
        Ok(UniqueValue(Value::Array(values)))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = Map::new();
        while let Some((key, value)) = map.next_entry::<String, UniqueValue>()? {
            if values.insert(key, value.0).is_some() {
                return Err(A::Error::custom("duplicate JSON member"));
            }
        }
        Ok(UniqueValue(Value::Object(values)))
    }
}

fn canonical_json(value: &Value) -> Result<Vec<u8>, UpdateMetadataError> {
    let mut output = Vec::new();
    let mut serializer =
        serde_json::Serializer::with_formatter(&mut output, CanonicalFormatter::new());
    value
        .serialize(&mut serializer)
        .map_err(|_| UpdateMetadataError::InvalidMetadata)?;
    Ok(output)
}

fn canonical_digest(value: &Value) -> Result<[u8; 32], UpdateMetadataError> {
    Ok(Sha256::digest(canonical_json(value)?).into())
}

fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], UpdateMetadataError> {
    if value.len() != N.saturating_mul(2)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(UpdateMetadataError::InvalidMetadata);
    }
    let mut output = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8, UpdateMetadataError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(UpdateMetadataError::InvalidMetadata),
    }
}

fn hex(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(ALPHABET[usize::from(byte >> 4)]));
        output.push(char::from(ALPHABET[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;

    use chrono::{DateTime, Utc};
    use ed25519_dalek::{Signer as _, SigningKey};
    use serde_json::{Value, json};
    use sha2::{Digest as _, Sha256};
    use tempfile::TempDir;

    use super::{
        SuppliedMetadata, SuppliedTarget, TargetSource, TrustedRoot, TrustedUpdateStore,
        canonical_json,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    #[test]
    fn canonical_signed_json_uses_olpc_control_escapes_and_utf8() -> TestResult {
        let value = json!({"Z": 1, "A": [true, false, null, "\n €"]});
        assert_eq!(
            canonical_json(&value)?,
            b"{\"A\":[true,false,null,\"\n \xe2\x82\xac\"],\"Z\":1}"
        );
        Ok(())
    }

    #[test]
    fn admits_threshold_root_rotation_and_persists_an_idempotent_chain() -> TestResult {
        let fixture = Fixture::new()?;
        let temporary = private_directory()?;
        let root = TrustedRoot::from_pinned(&fixture.root_one)?;
        let store = TrustedUpdateStore::open_or_bootstrap(temporary.path(), root, fixture.now)?;
        let root_chain = [fixture.root_two.as_slice()];
        let pending = store.admit(
            fixture.metadata(&root_chain),
            &fixture.targets(),
            fixture.now,
        )?;

        assert_eq!(
            pending
                .target("release/manifest.json")
                .ok_or("manifest target is missing")?
                .sha256(),
            fixture.manifest_sha256
        );
        let receipt = pending.persist()?;
        assert_eq!(receipt.root_version(), 2);
        assert_eq!(receipt.timestamp_version(), 3);

        let pinned = TrustedRoot::from_pinned(&fixture.root_one)?;
        let reopened =
            TrustedUpdateStore::open_or_bootstrap(temporary.path(), pinned, fixture.now)?;
        let root_chain = [fixture.root_two.as_slice()];
        reopened
            .admit(
                fixture.metadata(&root_chain),
                &fixture.targets(),
                fixture.now,
            )?
            .persist()?;
        Ok(())
    }

    #[test]
    fn rejects_replay_expiry_mix_and_match_and_wrong_target_bytes() -> TestResult {
        let fixture = Fixture::new()?;
        let temporary = private_directory()?;
        let root = TrustedRoot::from_pinned(&fixture.root_one)?;
        let root_chain = [fixture.root_two.as_slice()];
        TrustedUpdateStore::open_or_bootstrap(temporary.path(), root, fixture.now)?
            .admit(
                fixture.metadata(&root_chain),
                &fixture.targets(),
                fixture.now,
            )?
            .persist()?;

        for defect in [
            Defect::OldTimestamp,
            Defect::ExpiredTimestamp,
            Defect::ChangedSameVersion,
            Defect::MixedSnapshot,
            Defect::WrongTarget,
        ] {
            let candidate = Fixture::new_with_defect(defect)?;
            let pinned = TrustedRoot::from_pinned(&candidate.root_one)?;
            let store =
                TrustedUpdateStore::open_or_bootstrap(temporary.path(), pinned, candidate.now)?;
            let root_chain = [candidate.root_two.as_slice()];
            assert!(
                store
                    .admit(
                        candidate.metadata(&root_chain),
                        &candidate.targets(),
                        candidate.now,
                    )
                    .is_err(),
                "defect {defect:?} must fail closed"
            );
        }
        Ok(())
    }

    #[derive(Clone, Copy, Debug)]
    enum Defect {
        OldTimestamp,
        ExpiredTimestamp,
        ChangedSameVersion,
        MixedSnapshot,
        WrongTarget,
    }

    struct Fixture {
        now: DateTime<Utc>,
        root_one: Vec<u8>,
        root_two: Vec<u8>,
        timestamp: Vec<u8>,
        snapshot: Vec<u8>,
        targets_metadata: Vec<u8>,
        manifest: Vec<u8>,
        archive: Vec<u8>,
        manifest_sha256: [u8; 32],
        manifest_download_path: String,
        archive_download_path: String,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn Error>> {
            Self::new_inner(None)
        }

        fn new_with_defect(defect: Defect) -> Result<Self, Box<dyn Error>> {
            Self::new_inner(Some(defect))
        }

        fn new_inner(defect: Option<Defect>) -> Result<Self, Box<dyn Error>> {
            let now = "2026-08-02T12:00:00Z".parse()?;
            let expiry = match defect {
                Some(Defect::ExpiredTimestamp) => "2026-08-01T00:00:00Z",
                Some(Defect::ChangedSameVersion) => "2026-09-02T00:00:00Z",
                _ => "2026-09-01T00:00:00Z",
            };
            let keys = [key(1), key(2), key(3)];
            let root_one_signed = root_signed(
                1,
                &[&keys[0], &keys[1]],
                &[
                    ("root", &[&keys[0], &keys[1]], 2),
                    ("targets", &[&keys[0]], 1),
                    ("snapshot", &[&keys[1]], 1),
                    ("timestamp", &[&keys[0]], 1),
                ],
            );
            let root_one = envelope(root_one_signed, &[&keys[0], &keys[1]])?;
            let root_two_signed = root_signed(
                2,
                &[&keys[1], &keys[2]],
                &[
                    ("root", &[&keys[1], &keys[2]], 2),
                    ("targets", &[&keys[1]], 1),
                    ("snapshot", &[&keys[2]], 1),
                    ("timestamp", &[&keys[1]], 1),
                ],
            );
            let root_two = envelope(root_two_signed, &[&keys[0], &keys[1], &keys[2]])?;

            let manifest = b"release manifest".to_vec();
            let archive = if matches!(defect, Some(Defect::WrongTarget)) {
                b"wrong archive".to_vec()
            } else {
                b"complete archive".to_vec()
            };
            let expected_archive = b"complete archive";
            let manifest_sha256: [u8; 32] = Sha256::digest(&manifest).into();
            let archive_sha256: [u8; 32] = Sha256::digest(expected_archive).into();
            let targets_signed = json!({
                "_type": "targets", "spec_version": "1.0.35", "version": 5,
                "expires": "2026-09-01T00:00:00Z",
                "targets": {
                    "release/archive.zip": target(expected_archive.len(), archive_sha256),
                    "release/manifest.json": target(manifest.len(), manifest_sha256)
                }
            });
            let targets_metadata = envelope(targets_signed, &[&keys[1]])?;
            let targets_description = metadata_description(5, &targets_metadata);

            let snapshot_signed = json!({
                "_type": "snapshot", "spec_version": "1.0.35", "version": 4,
                "expires": "2026-09-01T00:00:00Z",
                "meta": {"targets.json": targets_description}
            });
            let snapshot = envelope(snapshot_signed, &[&keys[2]])?;
            let timestamp_snapshot = if matches!(defect, Some(Defect::MixedSnapshot)) {
                envelope(
                    json!({
                        "_type": "snapshot", "spec_version": "1.0.35", "version": 4,
                        "expires": "2026-09-01T00:00:00Z", "meta": {
                            "targets.json": metadata_description(5, b"different targets")
                        }
                    }),
                    &[&keys[2]],
                )?
            } else {
                snapshot.clone()
            };
            let timestamp_version = match defect {
                Some(Defect::OldTimestamp) => 2,
                _ => 3,
            };
            let timestamp_signed = json!({
                "_type": "timestamp", "spec_version": "1.0.35",
                "version": timestamp_version, "expires": expiry,
                "meta": {"snapshot.json": metadata_description(4, &timestamp_snapshot)}
            });
            let timestamp = envelope(timestamp_signed, &[&keys[1]])?;

            Ok(Self {
                now,
                root_one,
                root_two,
                timestamp,
                snapshot,
                targets_metadata,
                manifest,
                archive,
                manifest_sha256,
                manifest_download_path: consistent_path("release/manifest.json", manifest_sha256),
                archive_download_path: consistent_path("release/archive.zip", archive_sha256),
            })
        }

        fn metadata<'a>(&'a self, root_chain: &'a [&'a [u8]]) -> SuppliedMetadata<'a> {
            SuppliedMetadata {
                root_chain,
                timestamp: &self.timestamp,
                snapshot_path: "4.snapshot.json",
                snapshot: &self.snapshot,
                targets_path: "5.targets.json",
                targets: &self.targets_metadata,
            }
        }

        fn targets(&self) -> [SuppliedTarget<'_>; 2] {
            [
                SuppliedTarget {
                    metadata_path: "release/manifest.json",
                    download_path: &self.manifest_download_path,
                    source: TargetSource::Bytes(&self.manifest),
                },
                SuppliedTarget {
                    metadata_path: "release/archive.zip",
                    download_path: &self.archive_download_path,
                    source: TargetSource::Bytes(&self.archive),
                },
            ]
        }
    }

    struct TestKey {
        signing: SigningKey,
        key_id: String,
        value: Value,
    }

    fn key(seed: u8) -> TestKey {
        let signing = SigningKey::from_bytes(&[seed; 32]);
        let value = json!({
            "keytype": "ed25519", "scheme": "ed25519",
            "keyval": {"public": hex(signing.verifying_key().as_bytes())}
        });
        let key_id = hex(&Sha256::digest(
            serde_json::to_vec(&value).unwrap_or_default(),
        ));
        TestKey {
            signing,
            key_id,
            value,
        }
    }

    fn root_signed(version: u64, keys: &[&TestKey], roles: &[(&str, &[&TestKey], u64)]) -> Value {
        let key_map = keys
            .iter()
            .map(|key| (key.key_id.clone(), key.value.clone()))
            .collect::<serde_json::Map<_, _>>();
        let role_map = roles
            .iter()
            .map(|(name, role_keys, threshold)| {
                (
                    (*name).to_owned(),
                    json!({
                        "keyids": role_keys.iter().map(|key| &key.key_id).collect::<Vec<_>>(),
                        "threshold": threshold
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        json!({
            "_type": "root", "spec_version": "1.0.35", "version": version,
            "expires": "2026-09-01T00:00:00Z", "consistent_snapshot": true,
            "keys": key_map, "roles": role_map
        })
    }

    fn envelope(signed: Value, keys: &[&TestKey]) -> Result<Vec<u8>, serde_json::Error> {
        let canonical = serde_json::to_vec(&signed)?;
        let signatures = keys
            .iter()
            .map(|key| {
                json!({
                    "keyid": key.key_id,
                    "sig": hex(&key.signing.sign(&canonical).to_bytes())
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&json!({"signatures": signatures, "signed": signed}))
    }

    fn metadata_description(version: u64, bytes: &[u8]) -> Value {
        json!({
            "version": version, "length": bytes.len(),
            "hashes": {"sha256": hex(&Sha256::digest(bytes))}
        })
    }

    fn target(length: usize, digest: [u8; 32]) -> Value {
        json!({"length": length, "hashes": {"sha256": hex(&digest)}})
    }

    fn consistent_path(path: &str, digest: [u8; 32]) -> String {
        let (directory, file) = path.rsplit_once('/').unwrap_or(("", path));
        if directory.is_empty() {
            format!("{}.{}", hex(&digest), file)
        } else {
            format!("{directory}/{}.{}", hex(&digest), file)
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn private_directory() -> Result<TempDir, Box<dyn Error>> {
        let temporary = TempDir::new()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))?;
        }
        Ok(temporary)
    }
}
