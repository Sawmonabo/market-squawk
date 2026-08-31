//! Immutable generation publication and restart validation.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::*;
#[cfg(windows)]
use crate::evidence_store::sync_new_link_metadata;
use crate::evidence_store::sync_publication_directory;
#[cfg(test)]
use crate::evidence_store::{PublicationCommitEvent, PublicationCommitTestProbe};

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Snapshot {
    pub(super) schema_version: u16,
    pub(super) generation: u64,
    pub(super) entries: Vec<SecRepresentation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotWire {
    schema_version: u16,
    generation: u64,
    entries: BoundedRepresentationWires,
}

struct BoundedRepresentationWires(Vec<SecRepresentationWire>);

struct RepresentationWireVisitor;

impl<'de> Visitor<'de> for RepresentationWireVisitor {
    type Value = BoundedRepresentationWires;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded SEC representation list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let initial = sequence.size_hint().unwrap_or(0).min(64);
        let mut values = Vec::new();
        values
            .try_reserve(initial)
            .map_err(|_| serde::de::Error::custom(SecRepresentationError::AllocationFailed))?;
        while values.len() < MAX_REPRESENTATIONS {
            let Some(value) = sequence.next_element()? else {
                return Ok(BoundedRepresentationWires(values));
            };
            if values.len() == values.capacity() {
                values.try_reserve(1).map_err(|_| {
                    serde::de::Error::custom(SecRepresentationError::AllocationFailed)
                })?;
            }
            values.push(value);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                SecRepresentationError::RepresentationLimitExceeded,
            ))
        } else {
            Ok(BoundedRepresentationWires(values))
        }
    }
}

impl<'de> Deserialize<'de> for BoundedRepresentationWires {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RepresentationWireVisitor)
    }
}

pub(super) fn load_latest(
    directory: &Dir,
    limits: SecRepresentationLimits,
) -> Result<RepresentationState, SecRepresentationError> {
    let mut committed = Vec::new();
    let mut staging = Vec::new();
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| SecRepresentationError::InvalidSnapshotName)?;
        if name.starts_with(SNAPSHOT_PREFIX) && name.ends_with(SNAPSHOT_SUFFIX) {
            committed.push(name);
            if committed.len() > MAX_COMMITTED_SNAPSHOTS {
                return Err(SecRepresentationError::SnapshotLimitExceeded);
            }
        } else if name.starts_with(STAGING_PREFIX) && name.ends_with(".tmp") {
            staging.push(name);
            if staging.len() > MAX_COMMITTED_SNAPSHOTS {
                return Err(SecRepresentationError::SnapshotLimitExceeded);
            }
        }
    }
    for name in staging {
        directory.remove_file(name)?;
    }
    let mut latest: Option<(u64, BTreeMap<(SourceId, String), SecRepresentation>)> = None;
    for name in committed {
        let (name_generation, expected_digest) = parse_snapshot_name(&name)?;
        let bytes = read_bounded_regular(directory, &name, limits.max_snapshot_bytes)?;
        let actual = snapshot_checksum(&bytes);
        if actual != expected_digest {
            return Err(SecRepresentationError::SnapshotDigestMismatch);
        }
        let wire: SnapshotWire = serde_json::from_slice(&bytes)?;
        if wire.schema_version != SNAPSHOT_SCHEMA_VERSION || wire.generation != name_generation {
            return Err(SecRepresentationError::InvalidSnapshot);
        }
        if wire.entries.0.len() > limits.max_representations {
            return Err(SecRepresentationError::RepresentationLimitExceeded);
        }
        let mut entries = BTreeMap::new();
        for representation in wire.entries.0 {
            let representation = representation.validate(limits)?;
            if entries
                .insert(
                    (
                        representation.source_id.clone(),
                        representation.locator.clone(),
                    ),
                    representation,
                )
                .is_some()
            {
                return Err(SecRepresentationError::DuplicateLocator);
            }
        }
        match &latest {
            Some((generation, _)) if *generation == wire.generation => {
                return Err(SecRepresentationError::DuplicateGeneration);
            }
            Some((generation, _)) if *generation > wire.generation => {}
            _ => latest = Some((wire.generation, entries)),
        }
    }
    Ok(match latest {
        Some((generation, entries)) => RepresentationState {
            generation,
            entries,
        },
        None => RepresentationState {
            generation: 0,
            entries: BTreeMap::new(),
        },
    })
}

pub(super) fn persist_snapshot(
    directory: &Dir,
    limits: SecRepresentationLimits,
    snapshot: &Snapshot,
    cancellation: &CancellationToken,
    #[cfg(test)] publication_probe: Option<&PublicationCommitTestProbe>,
) -> Result<(), SecRepresentationError> {
    check_cancelled(cancellation)?;
    let maximum = usize::try_from(limits.max_snapshot_bytes)
        .map_err(|_| SecRepresentationError::SnapshotTooLarge)?;
    let mut writer = BoundedSnapshotWriter::new(maximum);
    serde_json::to_writer(&mut writer, snapshot)?;
    let bytes = writer.into_inner();
    let digest = snapshot_checksum(&bytes);
    let final_name = snapshot_name(snapshot.generation, digest);
    match read_bounded_regular(directory, &final_name, limits.max_snapshot_bytes) {
        Ok(existing) if existing == bytes => {
            #[cfg(test)]
            if let Some(probe) = publication_probe {
                probe.record(PublicationCommitEvent::IdenticalFinalObserved);
            }
            sync_publication_directory(directory)?;
            #[cfg(test)]
            if let Some(probe) = publication_probe {
                probe.record(PublicationCommitEvent::ParentDirectorySynced);
            }
            return Ok(());
        }
        Ok(_) => return Err(SecRepresentationError::SnapshotConflict),
        Err(SecRepresentationError::Io(error)) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let staging_name = format!("{STAGING_PREFIX}{}.tmp", Uuid::new_v4());
    let cleanup = StagingCleanup {
        directory,
        name: &staging_name,
    };
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let mut staging = directory.open_with(&staging_name, &options)?.into_std();
    for chunk in bytes.chunks(64 * 1024) {
        check_cancelled(cancellation)?;
        staging.write_all(chunk)?;
    }
    check_cancelled(cancellation)?;
    staging.sync_all()?;
    staging.seek(SeekFrom::Start(0))?;
    let mut verified = Vec::new();
    verified
        .try_reserve(bytes.len())
        .map_err(|_| SecRepresentationError::AllocationFailed)?;
    read_bounded_chunks(
        &mut staging,
        &mut verified,
        limits.max_snapshot_bytes,
        cancellation,
    )?;
    if verified != bytes || snapshot_checksum(&verified) != digest {
        return Err(SecRepresentationError::SnapshotDigestMismatch);
    }
    check_cancelled(cancellation)?;
    match directory.hard_link(&staging_name, directory, &final_name) {
        Ok(()) => {
            #[cfg(test)]
            if let Some(probe) = publication_probe {
                probe.final_link_published(cancellation);
            }
            #[cfg(windows)]
            sync_new_link_metadata(&staging)?;
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            if read_bounded_regular(directory, &final_name, limits.max_snapshot_bytes)? != bytes {
                return Err(SecRepresentationError::SnapshotConflict);
            }
            #[cfg(test)]
            if let Some(probe) = publication_probe {
                probe.record(PublicationCommitEvent::IdenticalFinalObserved);
            }
        }
        Err(error) => return Err(error.into()),
    }
    sync_publication_directory(directory)?;
    #[cfg(test)]
    if let Some(probe) = publication_probe {
        probe.record(PublicationCommitEvent::ParentDirectorySynced);
    }
    drop(cleanup);
    Ok(())
}

pub(super) fn cleanup_old_snapshots(
    directory: &Dir,
    current_generation: u64,
) -> Result<(), SecRepresentationError> {
    let retain_from = current_generation.saturating_sub(RETAINED_SNAPSHOTS as u64 - 1);
    let mut removed = false;
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| SecRepresentationError::InvalidSnapshotName)?;
        if name.starts_with(SNAPSHOT_PREFIX) && name.ends_with(SNAPSHOT_SUFFIX) {
            let (generation, _) = parse_snapshot_name(&name)?;
            if generation < retain_from {
                directory.remove_file(name)?;
                removed = true;
            }
        }
    }
    if removed {
        sync_publication_directory(directory)?;
    }
    Ok(())
}

fn read_bounded_regular(
    directory: &Dir,
    name: &str,
    max_bytes: u64,
) -> Result<Vec<u8>, SecRepresentationError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    let file = directory.open_with(name, &options)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || !directory.symlink_metadata(name)?.is_file() {
        return Err(SecRepresentationError::NotRegularFile);
    }
    if metadata.len() > max_bytes {
        return Err(SecRepresentationError::SnapshotTooLarge);
    }
    let mut bytes = Vec::new();
    let cancellation = CancellationToken::new();
    read_bounded_chunks(&mut file.into_std(), &mut bytes, max_bytes, &cancellation)?;
    Ok(bytes)
}

fn read_bounded_chunks(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
    max_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<(), SecRepresentationError> {
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_cancelled(cancellation)?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        let new_len = bytes
            .len()
            .checked_add(read)
            .ok_or(SecRepresentationError::SnapshotTooLarge)?;
        if u64::try_from(new_len).map_or(true, |length| length > max_bytes) {
            return Err(SecRepresentationError::SnapshotTooLarge);
        }
        bytes
            .try_reserve(read)
            .map_err(|_| SecRepresentationError::AllocationFailed)?;
        bytes.extend_from_slice(&buffer[..read]);
    }
}

struct BoundedSnapshotWriter {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedSnapshotWriter {
    const fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedSnapshotWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let new_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("SEC representation snapshot is too large"))?;
        if new_len > self.maximum {
            return Err(std::io::Error::other(
                "SEC representation snapshot is too large",
            ));
        }
        self.bytes
            .try_reserve(buffer.len())
            .map_err(|_| std::io::Error::other("SEC representation allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn snapshot_name(generation: u64, digest: [u8; 32]) -> String {
    format!(
        "{SNAPSHOT_PREFIX}{generation:020}-{}{SNAPSHOT_SUFFIX}",
        encode_hex(digest)
    )
}

fn snapshot_checksum(bytes: &[u8]) -> [u8; 32] {
    let mut checksum = Sha256::new();
    checksum.update(SNAPSHOT_CHECKSUM_DOMAIN);
    checksum.update((bytes.len() as u64).to_be_bytes());
    checksum.update(bytes);
    checksum.finalize().into()
}

fn parse_snapshot_name(name: &str) -> Result<(u64, [u8; 32]), SecRepresentationError> {
    let body = name
        .strip_prefix(SNAPSHOT_PREFIX)
        .and_then(|value| value.strip_suffix(SNAPSHOT_SUFFIX))
        .ok_or(SecRepresentationError::InvalidSnapshotName)?;
    let (generation, digest) = body
        .split_once('-')
        .ok_or(SecRepresentationError::InvalidSnapshotName)?;
    if generation.len() != 20 || digest.len() != 64 {
        return Err(SecRepresentationError::InvalidSnapshotName);
    }
    let generation = generation
        .parse()
        .map_err(|_| SecRepresentationError::InvalidSnapshotName)?;
    let mut bytes = [0_u8; 32];
    for (index, pair) in digest.as_bytes().chunks_exact(2).enumerate() {
        let pair =
            std::str::from_utf8(pair).map_err(|_| SecRepresentationError::InvalidSnapshotName)?;
        bytes[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| SecRepresentationError::InvalidSnapshotName)?;
    }
    Ok((generation, bytes))
}

fn encode_hex(bytes: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        let _ignored = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

struct StagingCleanup<'a> {
    directory: &'a Dir,
    name: &'a str,
}

impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        let _ignored = self.directory.remove_file(self.name);
    }
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}
