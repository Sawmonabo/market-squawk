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
    let mut pending = PendingReport::create(output, &bytes)?;
    if let Err(error) = pending.write_and_sync(&bytes) {
        return Err(pending.fail(error));
    }
    Ok(pending.commit())
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
    let mut pending = PendingReport::create(output, &bytes)?;
    if let Err(error) = pending.write_and_sync(&bytes) {
        return Err(pending.fail(error));
    }
    if let Err(error) =
        revalidate().context("release-evidence post-publication identity barrier failed")
    {
        return Err(pending.fail(error));
    }
    Ok(pending.commit())
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

#[derive(Debug)]
struct PendingReport {
    file: Option<File>,
    identity: Option<ReportFileIdentity>,
    path: PathBuf,
    expected_sha256: String,
    expected_size: usize,
    committed: bool,
    cleanup_complete: bool,
}

impl PendingReport {
    fn create(output: &Path, bytes: &[u8]) -> Result<Self> {
        ensure_transactional_report_supported()?;
        let path = normalized_new_file(output)?;
        let expected_sha256 = sha256_bytes(bytes);
        let expected_size = bytes.len();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        configure_private_report_creation(&mut options);
        let file = options
            .open(&path)
            .context("release-evidence output could not be created")?;
        let mut pending = Self {
            file: Some(file),
            identity: None,
            path,
            expected_sha256,
            expected_size,
            committed: false,
            cleanup_complete: false,
        };
        let identity = match pending.file.as_ref().map(report_file_identity) {
            Some(Ok(identity)) => identity,
            Some(Err(error)) => {
                let original = anyhow::Error::from(error)
                    .context("release-evidence opened-file identity could not be established");
                return Err(pending.fail(original));
            }
            None => {
                return Err(pending.fail(anyhow::anyhow!(
                    "release-evidence opened file is unavailable after creation"
                )));
            }
        };
        pending.identity = Some(identity);
        Ok(pending)
    }

    fn write_and_sync(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() != self.expected_size || sha256_bytes(bytes) != self.expected_sha256 {
            bail!("release-evidence pending bytes changed before publication");
        }
        let identity = self
            .identity
            .context("release-evidence pending file identity is unavailable")?;
        let expected_size = u64::try_from(self.expected_size)
            .context("release-evidence output size exceeds u64")?;
        let file = self
            .file
            .as_mut()
            .context("release-evidence pending file is unavailable")?;
        if report_file_identity(file)? != identity {
            bail!("release-evidence opened file identity changed before publication");
        }
        file.write_all(bytes)
            .context("release-evidence output write failed")?;
        if file
            .metadata()
            .context("release-evidence opened file metadata is unavailable")?
            .len()
            != expected_size
        {
            bail!("release-evidence output length does not match the encoded report");
        }
        file.sync_all()
            .context("release-evidence output synchronization failed")?;
        file.seek(SeekFrom::Start(0))
            .context("release-evidence output rewind failed")?;
        let written = hash_pass(file, expected_size)
            .context("release-evidence output verification failed")?;
        if written.byte_count != expected_size || written.sha256 != self.expected_sha256 {
            bail!("release-evidence output differs from the encoded report");
        }
        if report_file_identity(file)? != identity
            || !path_has_report_identity(&identity, &self.path)?
        {
            bail!("release-evidence output identity changed during publication");
        }
        sync_parent(&self.path)?;
        if !path_has_report_identity(&identity, &self.path)? {
            bail!("release-evidence output identity changed after parent synchronization");
        }
        Ok(())
    }

    fn commit(mut self) -> PublishedReport {
        let published = PublishedReport {
            path: self.path.clone(),
            sha256: self.expected_sha256.clone(),
            byte_count: self.expected_size,
        };
        drop(self.file.take());
        self.committed = true;
        published
    }

    fn fail(mut self, original: anyhow::Error) -> anyhow::Error {
        match self.cleanup() {
            Ok(()) => original.context(
                "just-created release-evidence output was invalidated, removed, and synchronized",
            ),
            Err(error) => {
                anyhow::anyhow!(
                    "release-evidence operation failed: {original:#}; transactional cleanup also \
                     failed: {error:#}"
                )
            }
        }
    }

    fn cleanup(&mut self) -> Result<()> {
        if self.committed || self.cleanup_complete {
            return Ok(());
        }
        let mut failures = Vec::new();
        if let Some(file) = self.file.as_mut() {
            if let Err(error) = file.set_len(0) {
                failures.push(format!("opened-file invalidation failed: {error}"));
            }
            if let Err(error) = file.sync_all() {
                failures.push(format!("opened-file invalidation sync failed: {error}"));
            }
        } else {
            failures.push("authoritative opened file is unavailable".to_owned());
        }
        drop(self.file.take());

        let Some(identity) = self.identity else {
            failures.push(
                "exact opened-file identity is unavailable; refusing to remove the named path"
                    .to_owned(),
            );
            if let Err(error) = sync_parent(&self.path) {
                failures.push(format!("parent fail-closed sync failed: {error:#}"));
            }
            bail!(
                "release-evidence transactional cleanup was incomplete: {}",
                failures.join("; ")
            );
        };

        match path_has_report_identity(&identity, &self.path) {
            Ok(true) => match fs::remove_file(&self.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => failures.push(format!("exact output removal failed: {error}")),
            },
            Ok(false) => {
                failures.push("refusing to remove a path with a different identity".to_owned());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => failures.push(format!("named-file identity check failed: {error}")),
        }
        if let Err(error) = sync_parent(&self.path) {
            failures.push(format!("parent cleanup sync failed: {error:#}"));
        }

        if failures.is_empty() {
            self.cleanup_complete = true;
            Ok(())
        } else {
            bail!(
                "release-evidence transactional cleanup was incomplete: {}",
                failures.join("; ")
            )
        }
    }
}

impl Drop for PendingReport {
    fn drop(&mut self) {
        if !self.committed && !self.cleanup_complete {
            let _ignored = self.cleanup();
        }
    }
}

#[cfg(any(unix, windows))]
fn ensure_transactional_report_supported() -> Result<()> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn ensure_transactional_report_supported() -> Result<()> {
    bail!("transactional release-evidence publication is unsupported on this platform")
}

#[cfg(unix)]
fn configure_private_report_creation(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(windows)]
fn configure_private_report_creation(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_private_report_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReportFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn report_file_identity(opened: &File) -> Result<ReportFileIdentity, std::io::Error> {
    use std::os::unix::fs::MetadataExt as _;

    let opened = opened.metadata()?;
    if !opened.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "release-evidence output is not a regular file",
        ));
    }
    Ok(ReportFileIdentity {
        device: opened.dev(),
        inode: opened.ino(),
    })
}

#[cfg(unix)]
fn path_has_report_identity(
    identity: &ReportFileIdentity,
    path: &Path,
) -> Result<bool, std::io::Error> {
    use std::os::unix::fs::MetadataExt as _;

    let named = fs::symlink_metadata(path)?;
    Ok(!named.file_type().is_symlink()
        && named.is_file()
        && identity.device == named.dev()
        && identity.inode == named.ino())
}

#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReportFileIdentity {
    volume: u32,
    index: u64,
}

#[cfg(windows)]
fn report_file_identity(opened: &File) -> Result<ReportFileIdentity, std::io::Error> {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let opened = opened.metadata()?;
    if !opened.is_file() || opened.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "release-evidence output is not a regular non-reparse file",
        ));
    }
    let volume = opened.volume_serial_number().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "release-evidence output volume identity is unavailable",
        )
    })?;
    let index = opened.file_index().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "release-evidence output file identity is unavailable",
        )
    })?;
    Ok(ReportFileIdentity { volume, index })
}

#[cfg(windows)]
fn path_has_report_identity(
    identity: &ReportFileIdentity,
    path: &Path,
) -> Result<bool, std::io::Error> {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let named = fs::symlink_metadata(path)?;
    Ok(named.is_file()
        && named.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
        && Some(identity.volume) == named.volume_serial_number()
        && Some(identity.index) == named.file_index())
}

#[cfg(not(any(unix, windows)))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReportFileIdentity;

#[cfg(not(any(unix, windows)))]
fn report_file_identity(_opened: &File) -> Result<ReportFileIdentity, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "release-evidence output identity is unsupported",
    ))
}

#[cfg(not(any(unix, windows)))]
fn path_has_report_identity(
    _identity: &ReportFileIdentity,
    _path: &Path,
) -> Result<bool, std::io::Error> {
    Ok(false)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let parent_path = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("release-evidence output has no parent"))?;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let parent = options
        .open(parent_path)
        .context("release-evidence parent could not be opened")?;
    parent
        .sync_all()
        .context("release-evidence parent synchronization failed")
}

#[cfg(windows)]
fn sync_parent(_path: &Path) -> Result<()> {
    // Rust does not expose a portable Windows directory-flush primitive. The authoritative report
    // handle is flushed before this boundary, and its exact non-reparse identity is checked again.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_parent(_path: &Path) -> Result<()> {
    bail!("release-evidence parent synchronization is unsupported on this platform")
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
