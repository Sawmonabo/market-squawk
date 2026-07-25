//! Bounded hashing and no-clobber evidence publication.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

const HASH_BUFFER_BYTES: usize = 64 * 1024;
const MAXIMUM_REPORT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct EvidenceEnvelope<'a, T> {
    schema_version: u32,
    kind: &'a str,
    payload_sha256: String,
    payload: &'a T,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedEvidenceEnvelope {
    schema_version: u32,
    kind: String,
    payload_sha256: String,
    payload: Value,
}

pub(super) fn publish_report<T: Serialize>(
    output: &Path,
    kind: &'static str,
    payload: &T,
) -> Result<PublishedReport> {
    let bytes = report_bytes(kind, payload)?;
    write_report(output, &bytes)
}

pub(super) fn publish_report_with_identity_barrier<T, F>(
    output: &Path,
    kind: &'static str,
    payload: &T,
    mut revalidate: F,
) -> Result<PublishedReport>
where
    T: Serialize,
    F: FnMut() -> Result<()>,
{
    let bytes = report_bytes(kind, payload)?;
    revalidate().context("release-evidence pre-publication identity barrier failed")?;
    let published = write_report(output, &bytes)?;
    if let Err(revalidation_error) = revalidate() {
        if let Err(cleanup_error) = remove_created_report(&published) {
            return Err(anyhow::anyhow!(
                "release-evidence post-publication identity barrier failed: \
                 {revalidation_error:#}; just-created report cleanup also failed: \
                 {cleanup_error:#}"
            ));
        }
        return Err(revalidation_error).context(
            "release-evidence post-publication identity barrier failed; \
             the just-created report was removed",
        );
    }
    Ok(published)
}

fn report_bytes<T: Serialize>(kind: &'static str, payload: &T) -> Result<Vec<u8>> {
    let payload_value =
        serde_json::to_value(payload).context("failed to serialize release-evidence payload")?;
    let payload_bytes = serde_json::to_vec(&payload_value)
        .context("failed to canonicalize release-evidence payload")?;
    if payload_bytes.len() > MAXIMUM_REPORT_BYTES {
        bail!("release-evidence payload exceeds its fixed bound");
    }
    let envelope = EvidenceEnvelope {
        schema_version: 1,
        kind,
        payload_sha256: sha256_bytes(&payload_bytes),
        payload: &payload_value,
    };
    let mut bytes =
        serde_json::to_vec_pretty(&envelope).context("failed to serialize release evidence")?;
    bytes.push(b'\n');
    if bytes.len() > MAXIMUM_REPORT_BYTES {
        bail!("release-evidence report exceeds its fixed bound");
    }
    Ok(bytes)
}

fn write_report(output: &Path, bytes: &[u8]) -> Result<PublishedReport> {
    let path = normalized_new_file(output)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .context("release-evidence output could not be created")?;
    file.write_all(bytes)
        .context("release-evidence output write failed")?;
    file.sync_all()
        .context("release-evidence output synchronization failed")?;
    sync_parent(&path)?;
    Ok(PublishedReport {
        path,
        sha256: sha256_bytes(bytes),
        byte_count: bytes.len(),
    })
}

fn remove_created_report(published: &PublishedReport) -> Result<()> {
    let expected_bytes =
        u64::try_from(published.byte_count).context("release-evidence output size exceeds u64")?;
    let identity = hash_stable_file(&published.path, expected_bytes)
        .context("just-created release-evidence output could not be revalidated for cleanup")?;
    if identity.byte_count != expected_bytes || identity.sha256 != published.sha256 {
        bail!("refusing to remove a changed release-evidence output");
    }
    fs::remove_file(&published.path)
        .context("just-created release-evidence output could not be removed")?;
    sync_parent(&published.path)
        .context("release-evidence parent could not be synchronized after cleanup")
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = File::open(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("release-evidence output has no parent"))?,
    )
    .context("release-evidence parent could not be opened")?;
    parent
        .sync_all()
        .context("release-evidence parent synchronization failed")
}

pub(super) fn hash_stable_file(path: &Path, maximum_bytes: u64) -> Result<StableFileIdentity> {
    let named = fs::symlink_metadata(path).context("evidence input metadata is unavailable")?;
    if named.file_type().is_symlink() || !named.is_file() || named.len() > maximum_bytes {
        bail!("evidence input is not an admitted bounded regular file");
    }
    let canonical = path
        .canonicalize()
        .context("evidence input cannot be canonicalized")?;
    let mut file = File::open(&canonical).context("evidence input cannot be opened")?;
    let before = file
        .metadata()
        .context("evidence input metadata is unavailable")?;
    let first = hash_pass(&mut file, maximum_bytes)?;
    file.seek(SeekFrom::Start(0))
        .context("evidence input rewind failed")?;
    let second = hash_pass(&mut file, maximum_bytes)?;
    let after = file
        .metadata()
        .context("evidence input metadata is unavailable")?;
    if first.sha256 != second.sha256
        || first.byte_count != before.len()
        || second.byte_count != before.len()
        || after.len() != before.len()
        || after.modified().ok() != before.modified().ok()
    {
        bail!("evidence input changed while it was read");
    }
    Ok(StableFileIdentity {
        canonical_path: canonical,
        sha256: first.sha256,
        byte_count: first.byte_count,
    })
}

pub(super) fn read_stable_bytes(path: &Path, maximum_bytes: u64) -> Result<Vec<u8>> {
    let identity = hash_stable_file(path, maximum_bytes)?;
    let capacity = usize::try_from(identity.byte_count)
        .context("evidence input cannot fit in addressable memory")?;
    let mut file =
        File::open(&identity.canonical_path).context("evidence input cannot be reopened")?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .context("evidence input allocation failed")?;
    file.read_to_end(&mut bytes)
        .context("evidence input read failed")?;
    if bytes.len() != capacity || sha256_bytes(&bytes) != identity.sha256 {
        bail!("evidence input changed between identity and content reads");
    }
    Ok(bytes)
}

pub(super) fn read_report(
    path: &Path,
    maximum_bytes: u64,
    expected_kind: &str,
) -> Result<VerifiedReport> {
    let file = hash_stable_file(path, maximum_bytes)?;
    let bytes = read_stable_bytes(path, maximum_bytes)?;
    if file.byte_count != u64::try_from(bytes.len())? || file.sha256 != sha256_bytes(&bytes) {
        bail!("release-evidence report changed between identity and decode");
    }
    let envelope: OwnedEvidenceEnvelope =
        serde_json::from_slice(&bytes).context("release-evidence report is invalid JSON")?;
    if envelope.schema_version != 1 || envelope.kind != expected_kind {
        bail!("release-evidence report has an unexpected schema or kind");
    }
    let canonical = serde_json::to_vec(&envelope.payload)
        .context("release-evidence payload canonicalization failed")?;
    if sha256_bytes(&canonical) != envelope.payload_sha256 {
        bail!("release-evidence payload hash does not match its content");
    }
    Ok(VerifiedReport {
        file,
        payload_sha256: envelope.payload_sha256,
        payload: envelope.payload,
    })
}

fn hash_pass(file: &mut File, maximum_bytes: u64) -> Result<HashPass> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    let mut bytes = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .context("evidence input read failed")?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).context("evidence input size overflow")?)
            .ok_or_else(|| anyhow::anyhow!("evidence input size overflow"))?;
        if bytes > maximum_bytes {
            bail!("evidence input exceeded its fixed bound");
        }
        hasher.update(&buffer[..read]);
    }
    Ok(HashPass {
        sha256: hex_digest(hasher.finalize().into()),
        byte_count: bytes,
    })
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StableFileIdentity {
    #[serde(skip)]
    canonical_path: PathBuf,
    pub(super) sha256: String,
    pub(super) byte_count: u64,
}

struct HashPass {
    sha256: String,
    byte_count: u64,
}

#[derive(Debug)]
pub(super) struct PublishedReport {
    pub(super) path: PathBuf,
    pub(super) sha256: String,
    pub(super) byte_count: usize,
}

pub(super) struct VerifiedReport {
    pub(super) file: StableFileIdentity,
    pub(super) payload_sha256: String,
    pub(super) payload: Value,
}

fn normalized_new_file(path: &Path) -> Result<PathBuf> {
    let filename = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow::anyhow!("release-evidence output has no filename"))?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("release-evidence output has no parent"))?;
    if parent
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("release-evidence output contains parent traversal");
    }
    let requested_parent = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir()
            .context("current directory is unavailable")?
            .join(parent)
    };
    let canonical_parent = requested_parent
        .canonicalize()
        .context("release-evidence output parent is unavailable")?;
    if !canonical_parent.is_dir() {
        bail!("release-evidence output parent is not a directory");
    }
    let candidate = canonical_parent.join(filename);
    if fs::symlink_metadata(&candidate).is_ok() {
        bail!("release-evidence output already exists");
    }
    Ok(candidate)
}

pub(super) fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_digest(hasher.finalize().into())
}

pub(super) fn hex_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
