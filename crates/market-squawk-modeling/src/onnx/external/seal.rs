//! Descriptor-verified private generations for optional native ONNX runtimes.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};

use super::{
    ExternalOnnxRuntimeError, ExternalOnnxRuntimeReference, ExternalRuntimeEvidenceWire,
    ExternalRuntimePlatform, MAX_RUNTIME_EVIDENCE_BYTES, MAX_RUNTIME_LIBRARY_BYTES, decode_digest,
};

/// Process-composition-owned local root for optional native runtime artifacts.
#[derive(Clone, Debug)]
pub struct ControlledOnnxRuntimeRoot {
    canonical_source_root: PathBuf,
    canonical_seal_root: PathBuf,
}

impl ControlledOnnxRuntimeRoot {
    /// Opens one source root and one private generation root for native runtime artifacts.
    ///
    /// # Errors
    ///
    /// Returns a typed error when either root is absent, foreign-owned, public, or invalid.
    pub fn open_ambient(
        source_root: impl AsRef<Path>,
        seal_root: impl AsRef<Path>,
    ) -> Result<Self, ExternalOnnxRuntimeError> {
        let canonical_source_root = canonical_directory(source_root.as_ref())?;
        let canonical_seal_root = canonical_directory(seal_root.as_ref())?;
        require_private_directory(&canonical_seal_root)?;
        if canonical_source_root == canonical_seal_root {
            return Err(ExternalOnnxRuntimeError::Root);
        }
        Ok(Self {
            canonical_source_root,
            canonical_seal_root,
        })
    }

    /// Copies descriptor-verified evidence and native code into a private sealed generation.
    ///
    /// # Errors
    ///
    /// Rejects symlinks, root escape, oversized or changed files, invalid evidence, wrong hashes,
    /// versions, platforms, unsafe ownership or permissions, and malformed binary headers.
    pub fn admit(
        &self,
        reference: &ExternalOnnxRuntimeReference,
    ) -> Result<ExternalOnnxRuntimeAdmission, ExternalOnnxRuntimeError> {
        let evidence_path = self.resolve_no_follow(&reference.evidence_relative_path)?;
        let evidence_bytes = read_bounded_evidence(&evidence_path)?;
        if Sha256::digest(&evidence_bytes).as_slice() != reference.evidence_digest {
            return Err(ExternalOnnxRuntimeError::EvidenceDigest);
        }
        let evidence: ExternalRuntimeEvidenceWire = serde_json::from_slice(&evidence_bytes)
            .map_err(|_| ExternalOnnxRuntimeError::EvidenceSyntax)?;
        if evidence.schema_version != 1
            || evidence.library_relative_path != reference.library_relative_path.as_ref()
            || decode_digest(&evidence.library_sha256)? != reference.library_digest
            || decode_digest(&evidence.policy_sha256)? != reference.verifier_policy_digest
            || evidence.runtime_version != reference.runtime_version.as_ref()
            || evidence.platform != reference.platform.as_str()
        {
            return Err(ExternalOnnxRuntimeError::EvidenceMismatch);
        }
        let library_path = self.resolve_no_follow(&reference.library_relative_path)?;
        let mut library =
            File::open(&library_path).map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
        let metadata = library
            .metadata()
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
        if !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_RUNTIME_LIBRARY_BYTES
            || metadata.len() != evidence.library_size_bytes
        {
            return Err(ExternalOnnxRuntimeError::LibrarySize);
        }
        if hash_open_runtime(&mut library)? != reference.library_digest {
            return Err(ExternalOnnxRuntimeError::LibraryDigest);
        }
        verify_binary_header_open(&mut library, reference.platform)?;
        let sealed_library_path = self.seal_runtime(&mut library, &evidence_bytes, reference)?;
        Ok(ExternalOnnxRuntimeAdmission {
            library_path: sealed_library_path,
            library_digest: reference.library_digest,
            runtime_version: reference.runtime_version.clone(),
            platform: reference.platform,
            evidence_digest: reference.evidence_digest,
        })
    }

    fn resolve_no_follow(&self, relative: &str) -> Result<PathBuf, ExternalOnnxRuntimeError> {
        let mut candidate = self.canonical_source_root.clone();
        for component in Path::new(relative).components() {
            let Component::Normal(component) = component else {
                return Err(ExternalOnnxRuntimeError::InvalidReference);
            };
            candidate.push(component);
            let metadata = fs::symlink_metadata(&candidate)
                .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
            if metadata.file_type().is_symlink() {
                return Err(ExternalOnnxRuntimeError::Symlink);
            }
        }
        let canonical = fs::canonicalize(candidate)
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
        if !canonical.starts_with(&self.canonical_source_root) {
            return Err(ExternalOnnxRuntimeError::RootEscape);
        }
        Ok(canonical)
    }

    fn seal_runtime(
        &self,
        library: &mut File,
        evidence: &[u8],
        reference: &ExternalOnnxRuntimeReference,
    ) -> Result<PathBuf, ExternalOnnxRuntimeError> {
        let generation_root = self
            .canonical_seal_root
            .join(runtime_generation_id(reference));
        let file_name = Path::new(reference.library_relative_path.as_ref())
            .file_name()
            .ok_or(ExternalOnnxRuntimeError::InvalidReference)?;
        let sealed_library = generation_root.join(file_name);
        if generation_root.exists() {
            verify_existing_generation(&generation_root, &sealed_library, reference)?;
            return Ok(sealed_library);
        }

        let staging = tempfile::Builder::new()
            .prefix(".market-squawk-onnx-staging-")
            .tempdir_in(&self.canonical_seal_root)
            .map_err(|_| ExternalOnnxRuntimeError::Seal)?;
        require_private_directory(staging.path())?;
        let staging_library = staging.path().join(file_name);
        library
            .seek(SeekFrom::Start(0))
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging_library)
            .map_err(|_| ExternalOnnxRuntimeError::Seal)?;
        let copied = io::copy(
            &mut library.take(MAX_RUNTIME_LIBRARY_BYTES + 1),
            &mut output,
        )
        .map_err(|_| ExternalOnnxRuntimeError::Seal)?;
        if copied == 0 || copied > MAX_RUNTIME_LIBRARY_BYTES {
            return Err(ExternalOnnxRuntimeError::LibrarySize);
        }
        output
            .sync_all()
            .map_err(|_| ExternalOnnxRuntimeError::Seal)?;
        set_sealed_file_permissions(&staging_library)?;
        let evidence_path = staging.path().join("admission.json");
        let mut evidence_output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&evidence_path)
            .map_err(|_| ExternalOnnxRuntimeError::Seal)?;
        evidence_output
            .write_all(evidence)
            .and_then(|()| evidence_output.sync_all())
            .map_err(|_| ExternalOnnxRuntimeError::Seal)?;
        set_sealed_file_permissions(&evidence_path)?;
        let staging_path = staging.keep();
        if fs::rename(&staging_path, &generation_root).is_err() {
            if generation_root.is_dir() {
                fs::remove_dir_all(&staging_path).map_err(|_| ExternalOnnxRuntimeError::Seal)?;
            } else {
                return Err(ExternalOnnxRuntimeError::Seal);
            }
        }
        verify_existing_generation(&generation_root, &sealed_library, reference)?;
        Ok(sealed_library)
    }
}

/// Exact optional runtime identity after descriptor-bound local admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalOnnxRuntimeAdmission {
    pub(super) library_path: PathBuf,
    pub(super) library_digest: [u8; 32],
    pub(super) runtime_version: Box<str>,
    pub(super) platform: ExternalRuntimePlatform,
    evidence_digest: [u8; 32],
}

impl ExternalOnnxRuntimeAdmission {
    /// Returns the admitted native library digest.
    #[must_use]
    pub const fn library_digest(&self) -> [u8; 32] {
        self.library_digest
    }

    /// Returns the admitted verifier-evidence digest.
    #[must_use]
    pub const fn evidence_digest(&self) -> [u8; 32] {
        self.evidence_digest
    }

    /// Returns the exact admitted runtime version.
    #[must_use]
    pub const fn runtime_version(&self) -> &str {
        &self.runtime_version
    }

    /// Returns the admitted platform identity.
    #[must_use]
    pub const fn platform(&self) -> ExternalRuntimePlatform {
        self.platform
    }

    pub(super) fn revalidate(&self) -> Result<(), ExternalOnnxRuntimeError> {
        verify_sealed_runtime(
            &self.library_path,
            self.library_digest,
            self.platform.wire_id(),
        )
    }
}

fn canonical_directory(path: &Path) -> Result<PathBuf, ExternalOnnxRuntimeError> {
    if fs::symlink_metadata(path)
        .map_err(|_| ExternalOnnxRuntimeError::Root)?
        .file_type()
        .is_symlink()
    {
        return Err(ExternalOnnxRuntimeError::Symlink);
    }
    let canonical = fs::canonicalize(path).map_err(|_| ExternalOnnxRuntimeError::Root)?;
    fs::metadata(&canonical)
        .map_err(|_| ExternalOnnxRuntimeError::Root)?
        .is_dir()
        .then_some(canonical)
        .ok_or(ExternalOnnxRuntimeError::Root)
}

#[cfg(unix)]
fn require_private_directory(path: &Path) -> Result<(), ExternalOnnxRuntimeError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path).map_err(|_| ExternalOnnxRuntimeError::Root)?;
    (metadata.is_dir()
        && metadata.mode() & 0o077 == 0
        && metadata.uid() == rustix::process::geteuid().as_raw())
    .then_some(())
    .ok_or(ExternalOnnxRuntimeError::Root)
}

#[cfg(not(unix))]
fn require_private_directory(path: &Path) -> Result<(), ExternalOnnxRuntimeError> {
    fs::metadata(path)
        .map_err(|_| ExternalOnnxRuntimeError::Root)?
        .is_dir()
        .then_some(())
        .ok_or(ExternalOnnxRuntimeError::Root)
}

fn runtime_generation_id(reference: &ExternalOnnxRuntimeReference) -> String {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/onnx-runtime-generation/v1");
    digest.update(reference.library_digest);
    digest.update(reference.evidence_digest);
    digest.update(reference.verifier_policy_digest);
    digest.update(reference.runtime_version.as_bytes());
    digest.update([reference.platform.wire_id()]);
    encode_digest(digest.finalize().into())
}

fn verify_existing_generation(
    generation_root: &Path,
    library_path: &Path,
    reference: &ExternalOnnxRuntimeReference,
) -> Result<(), ExternalOnnxRuntimeError> {
    if fs::symlink_metadata(generation_root)
        .map_err(|_| ExternalOnnxRuntimeError::Seal)?
        .file_type()
        .is_symlink()
        || fs::symlink_metadata(library_path)
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?
            .file_type()
            .is_symlink()
    {
        return Err(ExternalOnnxRuntimeError::Symlink);
    }
    require_private_directory(generation_root).map_err(|_| ExternalOnnxRuntimeError::Seal)?;
    verify_sealed_runtime(
        library_path,
        reference.library_digest,
        reference.platform.wire_id(),
    )?;
    let evidence_digest = hash_bounded_file(
        &generation_root.join("admission.json"),
        MAX_RUNTIME_EVIDENCE_BYTES,
    )?;
    (evidence_digest == reference.evidence_digest)
        .then_some(())
        .ok_or(ExternalOnnxRuntimeError::EvidenceDigest)
}

#[cfg(unix)]
fn set_sealed_file_permissions(path: &Path) -> Result<(), ExternalOnnxRuntimeError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o400))
        .map_err(|_| ExternalOnnxRuntimeError::Seal)
}

#[cfg(not(unix))]
fn set_sealed_file_permissions(path: &Path) -> Result<(), ExternalOnnxRuntimeError> {
    let mut permissions = fs::metadata(path)
        .map_err(|_| ExternalOnnxRuntimeError::Seal)?
        .permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions).map_err(|_| ExternalOnnxRuntimeError::Seal)
}

#[cfg(target_os = "linux")]
pub(crate) fn verify_sealed_runtime(
    path: &Path,
    expected_digest: [u8; 32],
    platform_wire_id: u8,
) -> Result<(), ExternalOnnxRuntimeError> {
    open_verified_sealed_runtime(path, expected_digest, platform_wire_id).map(drop)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn verify_sealed_runtime(
    _path: &Path,
    _expected_digest: [u8; 32],
    _platform_wire_id: u8,
) -> Result<(), ExternalOnnxRuntimeError> {
    Err(ExternalOnnxRuntimeError::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
pub(crate) fn open_verified_sealed_runtime(
    path: &Path,
    expected_digest: [u8; 32],
    platform_wire_id: u8,
) -> Result<File, ExternalOnnxRuntimeError> {
    use std::os::unix::fs::MetadataExt;

    use rustix::fs::{Mode, OFlags, open};

    let platform = ExternalRuntimePlatform::from_wire_id(platform_wire_id)
        .ok_or(ExternalOnnxRuntimeError::Platform)?;
    if !platform.is_current() {
        return Err(ExternalOnnxRuntimeError::UnsupportedPlatform);
    }
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::LOOP {
            ExternalOnnxRuntimeError::Symlink
        } else {
            ExternalOnnxRuntimeError::LibraryUnavailable
        }
    })?;
    let mut library = File::from(descriptor);
    let metadata = library
        .metadata()
        .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    if !metadata.is_file()
        || metadata.mode() & 0o277 != 0
        || metadata.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(ExternalOnnxRuntimeError::Seal);
    }
    verify_open_runtime(&mut library, expected_digest, platform_wire_id)?;
    Ok(library)
}

#[cfg(target_os = "linux")]
pub(crate) fn verify_open_runtime(
    library: &mut File,
    expected_digest: [u8; 32],
    platform_wire_id: u8,
) -> Result<(), ExternalOnnxRuntimeError> {
    let platform = ExternalRuntimePlatform::from_wire_id(platform_wire_id)
        .ok_or(ExternalOnnxRuntimeError::Platform)?;
    if !platform.is_current() {
        return Err(ExternalOnnxRuntimeError::UnsupportedPlatform);
    }
    if hash_open_runtime(library)? != expected_digest {
        return Err(ExternalOnnxRuntimeError::LibraryDigest);
    }
    verify_binary_header_open(library, platform)
}

fn read_bounded_evidence(path: &Path) -> Result<Vec<u8>, ExternalOnnxRuntimeError> {
    let file = File::open(path).map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    let metadata = file
        .metadata()
        .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_RUNTIME_EVIDENCE_BYTES {
        return Err(ExternalOnnxRuntimeError::EvidenceSize);
    }
    let expected_len =
        usize::try_from(metadata.len()).map_err(|_| ExternalOnnxRuntimeError::EvidenceSize)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(expected_len)
        .map_err(|_| ExternalOnnxRuntimeError::EvidenceSize)?;
    let mut reader = file.take(MAX_RUNTIME_EVIDENCE_BYTES + 1);
    reader
        .read_to_end(&mut bytes)
        .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    let final_len = reader
        .get_ref()
        .metadata()
        .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?
        .len();
    if bytes.len() != expected_len || final_len != metadata.len() {
        return Err(ExternalOnnxRuntimeError::EvidenceSize);
    }
    Ok(bytes)
}

fn hash_bounded_file(path: &Path, limit: u64) -> Result<[u8; 32], ExternalOnnxRuntimeError> {
    let mut file = File::open(path).map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    hash_open_bounded(&mut file, limit)
}

fn hash_open_runtime(file: &mut File) -> Result<[u8; 32], ExternalOnnxRuntimeError> {
    hash_open_bounded(file, MAX_RUNTIME_LIBRARY_BYTES)
}

fn hash_open_bounded(file: &mut File, limit: u64) -> Result<[u8; 32], ExternalOnnxRuntimeError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    let metadata = file
        .metadata()
        .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > limit {
        return Err(ExternalOnnxRuntimeError::LibrarySize);
    }
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| ExternalOnnxRuntimeError::LibrarySize)?)
            .filter(|value| *value <= limit)
            .ok_or(ExternalOnnxRuntimeError::LibrarySize)?;
        digest.update(&buffer[..read]);
    }
    if total != metadata.len()
        || file
            .metadata()
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?
            .len()
            != metadata.len()
    {
        return Err(ExternalOnnxRuntimeError::LibrarySize);
    }
    Ok(digest.finalize().into())
}

fn verify_binary_header_open(
    file: &mut File,
    platform: ExternalRuntimePlatform,
) -> Result<(), ExternalOnnxRuntimeError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ExternalOnnxRuntimeError::Platform)?;
    let mut header = [0_u8; 64];
    file.read_exact(&mut header)
        .map_err(|_| ExternalOnnxRuntimeError::Platform)?;
    let valid = match platform {
        ExternalRuntimePlatform::MacosArm64MachO => {
            header[..4] == [0xcf, 0xfa, 0xed, 0xfe]
                && u32::from_le_bytes([header[4], header[5], header[6], header[7]]) == 0x0100_000c
                && u32::from_le_bytes([header[12], header[13], header[14], header[15]]) == 6
        }
        ExternalRuntimePlatform::MacosX8664MachO => {
            header[..4] == [0xcf, 0xfa, 0xed, 0xfe]
                && u32::from_le_bytes([header[4], header[5], header[6], header[7]]) == 0x0100_0007
                && u32::from_le_bytes([header[12], header[13], header[14], header[15]]) == 6
        }
        ExternalRuntimePlatform::LinuxArm64Elf => {
            header[..4] == [0x7f, b'E', b'L', b'F']
                && header[4] == 2
                && header[5] == 1
                && u16::from_le_bytes([header[16], header[17]]) == 3
                && u16::from_le_bytes([header[18], header[19]]) == 183
        }
        ExternalRuntimePlatform::LinuxX8664Elf => {
            header[..4] == [0x7f, b'E', b'L', b'F']
                && header[4] == 2
                && header[5] == 1
                && u16::from_le_bytes([header[16], header[17]]) == 3
                && u16::from_le_bytes([header[18], header[19]]) == 62
        }
        ExternalRuntimePlatform::WindowsArm64Pe => verify_pe_machine(file, &header, 0xaa64)?,
        ExternalRuntimePlatform::WindowsX8664Pe => verify_pe_machine(file, &header, 0x8664)?,
    };
    valid
        .then_some(())
        .ok_or(ExternalOnnxRuntimeError::Platform)
}

fn verify_pe_machine(
    file: &mut File,
    header: &[u8; 64],
    expected_machine: u16,
) -> Result<bool, ExternalOnnxRuntimeError> {
    if header[..2] != *b"MZ" {
        return Ok(false);
    }
    let pe_offset = u32::from_le_bytes([header[60], header[61], header[62], header[63]]);
    file.seek(SeekFrom::Start(u64::from(pe_offset)))
        .map_err(|_| ExternalOnnxRuntimeError::Platform)?;
    let mut pe_header = [0_u8; 24];
    file.read_exact(&mut pe_header)
        .map_err(|_| ExternalOnnxRuntimeError::Platform)?;
    Ok(pe_header[..4] == [b'P', b'E', 0, 0]
        && u16::from_le_bytes([pe_header[4], pe_header[5]]) == expected_machine
        && u16::from_le_bytes([pe_header[22], pe_header[23]]) & 0x2000 != 0)
}

fn encode_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
