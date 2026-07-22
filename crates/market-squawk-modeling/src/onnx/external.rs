//! Optional, explicitly admitted ONNX Runtime acceleration with mandatory tract fallback.

use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::native::decide;
use crate::{
    InferenceBackend, InferenceError, ModelInput, ModelMetadata, ModelOutput, ModelOutputIdentity,
};

use super::worker::OnnxWorker;
use super::{OnnxPolicyError, TractOnnxBackend, normalize_input};

const MAX_RUNTIME_LIBRARY_BYTES: u64 = 512 * 1024 * 1024;
const MAX_RUNTIME_EVIDENCE_BYTES: u64 = 32 * 1024;
const MAX_RUNTIME_PATH_BYTES: usize = 256;
const TRACKED_RUNTIME_POLICY: &[u8] =
    include_bytes!("../../../../docs/verification/onnx-runtime-policy.json");

/// Exact optional native runtime version admitted by the tracked verifier policy.
pub const OPTIONAL_ONNX_RUNTIME_VERSION: &str = "1.24.4";

/// Returns the SHA-256 identity of the verifier policy compiled into this backend.
#[must_use]
pub fn optional_onnx_runtime_policy_digest() -> [u8; 32] {
    Sha256::digest(TRACKED_RUNTIME_POLICY).into()
}

/// Closed host platform bound to one operator-supplied ONNX Runtime library.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExternalRuntimePlatform {
    MacosArm64MachO,
    MacosX8664MachO,
    LinuxArm64Elf,
    LinuxX8664Elf,
    WindowsArm64Pe,
    WindowsX8664Pe,
}

impl ExternalRuntimePlatform {
    /// Returns the verifier's canonical platform identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MacosArm64MachO => "macos-arm64-macho",
            Self::MacosX8664MachO => "macos-x86_64-macho",
            Self::LinuxArm64Elf => "linux-arm64-elf",
            Self::LinuxX8664Elf => "linux-x86_64-elf",
            Self::WindowsArm64Pe => "windows-arm64-pe",
            Self::WindowsX8664Pe => "windows-x86_64-pe",
        }
    }

    fn is_current(self) -> bool {
        cfg!(all(target_os = "macos", target_arch = "aarch64")) && self == Self::MacosArm64MachO
            || cfg!(all(target_os = "macos", target_arch = "x86_64"))
                && self == Self::MacosX8664MachO
            || cfg!(all(target_os = "linux", target_arch = "aarch64"))
                && self == Self::LinuxArm64Elf
            || cfg!(all(target_os = "linux", target_arch = "x86_64")) && self == Self::LinuxX8664Elf
            || cfg!(all(target_os = "windows", target_arch = "aarch64"))
                && self == Self::WindowsArm64Pe
            || cfg!(all(target_os = "windows", target_arch = "x86_64"))
                && self == Self::WindowsX8664Pe
    }
}

/// Exact operator-configured runtime and verifier-evidence identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalOnnxRuntimeReference {
    library_relative_path: Box<str>,
    evidence_relative_path: Box<str>,
    library_digest: [u8; 32],
    evidence_digest: [u8; 32],
    verifier_policy_digest: [u8; 32],
    runtime_version: Box<str>,
    platform: ExternalRuntimePlatform,
}

impl ExternalOnnxRuntimeReference {
    /// Constructs an exact local runtime reference without opening or loading native code.
    ///
    /// # Errors
    ///
    /// Rejects non-relative paths, reserved hashes, non-1.24 versions, or a foreign platform.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent native-library and verifier identities remain explicit"
    )]
    pub fn try_new(
        library_relative_path: impl AsRef<str>,
        evidence_relative_path: impl AsRef<str>,
        library_digest: [u8; 32],
        evidence_digest: [u8; 32],
        verifier_policy_digest: [u8; 32],
        runtime_version: impl AsRef<str>,
        platform: ExternalRuntimePlatform,
    ) -> Result<Self, ExternalOnnxRuntimeError> {
        let library_relative_path = library_relative_path.as_ref();
        let evidence_relative_path = evidence_relative_path.as_ref();
        let runtime_version = runtime_version.as_ref();
        if !controlled_relative_path(library_relative_path)
            || !controlled_relative_path(evidence_relative_path)
            || library_relative_path == evidence_relative_path
            || [library_digest, evidence_digest, verifier_policy_digest].contains(&[0; 32])
            || verifier_policy_digest != optional_onnx_runtime_policy_digest()
            || runtime_version != OPTIONAL_ONNX_RUNTIME_VERSION
            || !platform.is_current()
        {
            return Err(ExternalOnnxRuntimeError::InvalidReference);
        }
        Ok(Self {
            library_relative_path: library_relative_path.into(),
            evidence_relative_path: evidence_relative_path.into(),
            library_digest,
            evidence_digest,
            verifier_policy_digest,
            runtime_version: runtime_version.into(),
            platform,
        })
    }
}

/// Process-composition-owned local root for optional native runtime artifacts.
#[derive(Clone, Debug)]
pub struct ControlledOnnxRuntimeRoot {
    canonical_root: PathBuf,
}

impl ControlledOnnxRuntimeRoot {
    /// Opens and canonicalizes one operator-controlled local runtime root.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the root is absent, not a directory, or cannot be canonicalized.
    pub fn open_ambient(path: impl AsRef<Path>) -> Result<Self, ExternalOnnxRuntimeError> {
        let canonical_root = fs::canonicalize(path).map_err(|_| ExternalOnnxRuntimeError::Root)?;
        if !fs::metadata(&canonical_root)
            .map_err(|_| ExternalOnnxRuntimeError::Root)?
            .is_dir()
        {
            return Err(ExternalOnnxRuntimeError::Root);
        }
        Ok(Self { canonical_root })
    }

    /// Revalidates verifier evidence and the native library without loading native code.
    ///
    /// # Errors
    ///
    /// Rejects symlinks, root escape, oversized or changed files, invalid evidence, wrong hashes,
    /// versions, platforms, or binary headers.
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
        let metadata = fs::metadata(&library_path)
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
        if !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_RUNTIME_LIBRARY_BYTES
            || metadata.len() != evidence.library_size_bytes
        {
            return Err(ExternalOnnxRuntimeError::LibrarySize);
        }
        if hash_file(&library_path)? != reference.library_digest {
            return Err(ExternalOnnxRuntimeError::LibraryDigest);
        }
        verify_binary_header(&library_path, reference.platform)?;
        Ok(ExternalOnnxRuntimeAdmission {
            canonical_root: self.canonical_root.clone(),
            library_relative_path: reference.library_relative_path.clone(),
            library_path,
            library_digest: reference.library_digest,
            runtime_version: reference.runtime_version.clone(),
            platform: reference.platform,
            evidence_digest: reference.evidence_digest,
        })
    }

    fn resolve_no_follow(&self, relative: &str) -> Result<PathBuf, ExternalOnnxRuntimeError> {
        let mut candidate = self.canonical_root.clone();
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
        if !canonical.starts_with(&self.canonical_root) {
            return Err(ExternalOnnxRuntimeError::RootEscape);
        }
        Ok(canonical)
    }
}

/// Exact optional runtime identity after direct local admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalOnnxRuntimeAdmission {
    canonical_root: PathBuf,
    library_relative_path: Box<str>,
    library_path: PathBuf,
    library_digest: [u8; 32],
    runtime_version: Box<str>,
    platform: ExternalRuntimePlatform,
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

    fn revalidate(&self) -> Result<(), ExternalOnnxRuntimeError> {
        let root = ControlledOnnxRuntimeRoot {
            canonical_root: self.canonical_root.clone(),
        };
        let resolved = root.resolve_no_follow(&self.library_relative_path)?;
        if resolved != self.library_path || hash_file(&resolved)? != self.library_digest {
            return Err(ExternalOnnxRuntimeError::LibraryDigest);
        }
        verify_binary_header(&resolved, self.platform)
    }
}

/// Optional external ONNX Runtime backend with mandatory tract fallback.
#[derive(Debug)]
pub struct ExternalOnnxRuntimeBackend {
    fallback: Arc<TractOnnxBackend>,
    worker: OnnxWorker,
    output_identity: Arc<ModelOutputIdentity>,
    runtime: ExternalOnnxRuntimeAdmission,
}

impl ExternalOnnxRuntimeBackend {
    /// Loads an admitted local ONNX Runtime and warms the same exact bundle as the tract fallback.
    ///
    /// This borrows the required backend, so failed optional construction cannot consume it.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, environment, session, parity, or warm-up failure.
    pub fn try_from_tract(
        fallback: &Arc<TractOnnxBackend>,
        runtime: ExternalOnnxRuntimeAdmission,
    ) -> Result<Self, ExternalOnnxRuntimeError> {
        runtime.revalidate()?;
        initialize_external_runtime(&runtime)?;
        let model_bytes = fallback
            .bundle
            .onnx_artifact_bytes()
            .ok_or(ExternalOnnxRuntimeError::Session)?;
        let builder =
            ort::session::Session::builder().map_err(|_| ExternalOnnxRuntimeError::Session)?;
        let builder = builder
            .with_intra_threads(1)
            .map_err(|_| ExternalOnnxRuntimeError::Session)?;
        let builder = builder
            .with_inter_threads(1)
            .map_err(|_| ExternalOnnxRuntimeError::Session)?;
        let mut builder = builder
            .with_parallel_execution(false)
            .map_err(|_| ExternalOnnxRuntimeError::Session)?;
        let session = builder
            .commit_from_memory(model_bytes)
            .map_err(|_| ExternalOnnxRuntimeError::Session)?;
        let input_elements = fallback.policy.preflight(model_bytes)?.input_elements();
        let (worker, warm_up) = OnnxWorker::start_external(
            session,
            fallback.policy.input_shape(),
            input_elements,
            fallback.policy.inference_deadline(),
        )
        .map_err(|_| ExternalOnnxRuntimeError::WarmUp)?;
        let tract_warm_up = fallback.evidence.warm_up_score();
        let tolerance = 1.0e-5_f32 * tract_warm_up.abs().max(1.0);
        if !warm_up.is_finite() || (warm_up - tract_warm_up).abs() > tolerance {
            return Err(ExternalOnnxRuntimeError::Parity);
        }
        runtime.revalidate()?;
        let output_identity = Arc::new(ModelOutputIdentity::from_metadata(fallback.metadata()));
        Ok(Self {
            fallback: Arc::clone(fallback),
            worker,
            output_identity,
            runtime,
        })
    }

    /// Returns the exact admitted optional-runtime identity retained by this backend.
    #[must_use]
    pub const fn runtime_admission(&self) -> &ExternalOnnxRuntimeAdmission {
        &self.runtime
    }
}

impl InferenceBackend for ExternalOnnxRuntimeBackend {
    fn metadata(&self) -> &ModelMetadata {
        self.fallback.metadata()
    }

    fn infer(&self, input: &ModelInput<'_>) -> Result<ModelOutput, InferenceError> {
        let normalized = normalize_input(self.metadata(), input)?;
        let score = match self.worker.execute(normalized) {
            Ok(score) => f64::from(score),
            Err(_) => return self.fallback.infer(input),
        };
        let (decision, confidence) = match decide(score, self.metadata().decision_thresholds()) {
            Ok(result) => result,
            Err(_) => return self.fallback.infer(input),
        };
        Ok(ModelOutput::new(
            Arc::clone(&self.output_identity),
            score,
            confidence,
            decision,
        ))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalRuntimeEvidenceWire {
    schema_version: u32,
    library_relative_path: String,
    library_sha256: String,
    library_size_bytes: u64,
    runtime_version: String,
    platform: String,
    policy_sha256: String,
}

/// Optional external-runtime admission or initialization failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExternalOnnxRuntimeError {
    #[error("external ONNX Runtime reference is invalid")]
    InvalidReference,
    #[error("external ONNX Runtime root is unavailable")]
    Root,
    #[error("external ONNX Runtime artifact is unavailable")]
    LibraryUnavailable,
    #[error("external ONNX Runtime path contains a symlink")]
    Symlink,
    #[error("external ONNX Runtime path escapes its controlled root")]
    RootEscape,
    #[error("external ONNX Runtime evidence exceeds its size limit")]
    EvidenceSize,
    #[error("external ONNX Runtime evidence digest differs")]
    EvidenceDigest,
    #[error("external ONNX Runtime evidence syntax is invalid")]
    EvidenceSyntax,
    #[error("external ONNX Runtime evidence identity differs")]
    EvidenceMismatch,
    #[error("external ONNX Runtime library size is invalid")]
    LibrarySize,
    #[error("external ONNX Runtime library digest differs")]
    LibraryDigest,
    #[error("external ONNX Runtime binary platform differs")]
    Platform,
    #[error("external ONNX Runtime environment could not be initialized")]
    Environment,
    #[error("external ONNX Runtime session could not be constructed")]
    Session,
    #[error("external ONNX Runtime warm-up failed")]
    WarmUp,
    #[error("external ONNX Runtime warm-up differs from tract")]
    Parity,
    #[error("common ONNX graph policy failed: {0}")]
    Policy(#[from] OnnxPolicyError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InitializedRuntime {
    digest: [u8; 32],
    version: Box<str>,
    platform: ExternalRuntimePlatform,
}

static INITIALIZED_RUNTIME: LazyLock<Mutex<Option<InitializedRuntime>>> =
    LazyLock::new(|| Mutex::new(None));

fn initialize_external_runtime(
    runtime: &ExternalOnnxRuntimeAdmission,
) -> Result<(), ExternalOnnxRuntimeError> {
    let mut initialized = INITIALIZED_RUNTIME
        .lock()
        .map_err(|_| ExternalOnnxRuntimeError::Environment)?;
    let identity = InitializedRuntime {
        digest: runtime.library_digest,
        version: runtime.runtime_version.clone(),
        platform: runtime.platform,
    };
    if let Some(existing) = initialized.as_ref() {
        return (existing == &identity)
            .then_some(())
            .ok_or(ExternalOnnxRuntimeError::Environment);
    }
    let committed = ort::init_from(&runtime.library_path)
        .map_err(|_| ExternalOnnxRuntimeError::Environment)?
        .with_name("market-squawk-local-onnx-runtime")
        .with_telemetry(false)
        .commit();
    if !committed {
        return Err(ExternalOnnxRuntimeError::Environment);
    }
    *initialized = Some(identity);
    Ok(())
}

fn controlled_relative_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_RUNTIME_PATH_BYTES
        && !value.contains("//")
        && !value.contains('\\')
        && !value.contains(':')
        && !value.bytes().any(|byte| byte.is_ascii_control())
        && Path::new(value)
            .components()
            .all(|component| matches!(component, Component::Normal(segment) if !segment.is_empty()))
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

fn hash_file(path: &Path) -> Result<[u8; 32], ExternalOnnxRuntimeError> {
    let file = File::open(path).map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    let metadata = file
        .metadata()
        .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_RUNTIME_LIBRARY_BYTES {
        return Err(ExternalOnnxRuntimeError::LibrarySize);
    }
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| ExternalOnnxRuntimeError::LibrarySize)?)
            .filter(|value| *value <= MAX_RUNTIME_LIBRARY_BYTES)
            .ok_or(ExternalOnnxRuntimeError::LibrarySize)?;
        digest.update(&buffer[..read]);
    }
    if total != metadata.len() {
        return Err(ExternalOnnxRuntimeError::LibrarySize);
    }
    Ok(digest.finalize().into())
}

fn verify_binary_header(
    path: &Path,
    platform: ExternalRuntimePlatform,
) -> Result<(), ExternalOnnxRuntimeError> {
    let mut file = File::open(path).map_err(|_| ExternalOnnxRuntimeError::LibraryUnavailable)?;
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
        ExternalRuntimePlatform::WindowsArm64Pe => verify_pe_machine(&mut file, &header, 0xaa64)?,
        ExternalRuntimePlatform::WindowsX8664Pe => verify_pe_machine(&mut file, &header, 0x8664)?,
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

fn decode_digest(value: &str) -> Result<[u8; 32], ExternalOnnxRuntimeError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ExternalOnnxRuntimeError::EvidenceSyntax);
    }
    let mut digest = [0_u8; 32];
    for (target, pair) in digest.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let pair =
            std::str::from_utf8(pair).map_err(|_| ExternalOnnxRuntimeError::EvidenceSyntax)?;
        *target =
            u8::from_str_radix(pair, 16).map_err(|_| ExternalOnnxRuntimeError::EvidenceSyntax)?;
    }
    Ok(digest)
}
