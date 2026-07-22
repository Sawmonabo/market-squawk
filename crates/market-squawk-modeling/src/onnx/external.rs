//! Optional, explicitly admitted ONNX Runtime acceleration with mandatory tract fallback.

use std::path::{Component, Path};
use std::sync::Arc;
use std::time::Instant;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::native::decide;
use crate::{
    InferenceBackend, InferenceError, ModelInput, ModelMetadata, ModelOutput, ModelOutputIdentity,
};

use super::worker::OnnxWorker;
use super::{OnnxPolicyError, TractOnnxBackend, normalize_input};

mod seal;

pub use seal::{ControlledOnnxRuntimeRoot, ExternalOnnxRuntimeAdmission};
#[cfg(target_os = "linux")]
pub(crate) use seal::{open_verified_sealed_runtime, verify_open_runtime};

pub(crate) const MAX_RUNTIME_LIBRARY_BYTES: u64 = 512 * 1024 * 1024;
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
        cfg!(all(target_os = "linux", target_arch = "aarch64")) && self == Self::LinuxArm64Elf
            || cfg!(all(target_os = "linux", target_arch = "x86_64")) && self == Self::LinuxX8664Elf
    }

    pub(crate) const fn wire_id(self) -> u8 {
        match self {
            Self::MacosArm64MachO => 1,
            Self::MacosX8664MachO => 2,
            Self::LinuxArm64Elf => 3,
            Self::LinuxX8664Elf => 4,
            Self::WindowsArm64Pe => 5,
            Self::WindowsX8664Pe => 6,
        }
    }

    #[cfg(target_os = "linux")]
    fn from_wire_id(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::MacosArm64MachO),
            2 => Some(Self::MacosX8664MachO),
            3 => Some(Self::LinuxArm64Elf),
            4 => Some(Self::LinuxX8664Elf),
            5 => Some(Self::WindowsArm64Pe),
            6 => Some(Self::WindowsX8664Pe),
            _ => None,
        }
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
        if !platform.is_current() {
            return Err(ExternalOnnxRuntimeError::UnsupportedPlatform);
        }
        if !controlled_relative_path(library_relative_path)
            || !controlled_relative_path(evidence_relative_path)
            || library_relative_path == evidence_relative_path
            || [library_digest, evidence_digest, verifier_policy_digest].contains(&[0; 32])
            || verifier_policy_digest != optional_onnx_runtime_policy_digest()
            || runtime_version != OPTIONAL_ONNX_RUNTIME_VERSION
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

/// Optional external ONNX Runtime backend with mandatory tract fallback.
#[derive(Debug)]
pub struct ExternalOnnxRuntimeBackend {
    fallback: Arc<TractOnnxBackend>,
    worker: OnnxWorker,
    output_identity: Arc<ModelOutputIdentity>,
    runtime: ExternalOnnxRuntimeAdmission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeDeadlines {
    external: Instant,
    total: Instant,
}

impl RuntimeDeadlines {
    fn from_budget(started_at: Instant, total_budget: std::time::Duration) -> Option<Self> {
        Some(Self {
            external: started_at.checked_add(total_budget / 2)?,
            total: started_at.checked_add(total_budget)?,
        })
    }
}

impl ExternalOnnxRuntimeBackend {
    /// Loads an admitted local ONNX Runtime in a helper and validates tract parity.
    ///
    /// This borrows the required backend, so failed optional construction cannot consume it.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, session, parity, or warm-up failure.
    pub fn try_from_tract(
        fallback: &Arc<TractOnnxBackend>,
        runtime: ExternalOnnxRuntimeAdmission,
    ) -> Result<Self, ExternalOnnxRuntimeError> {
        runtime.revalidate()?;
        let model_bytes = fallback
            .bundle
            .onnx_artifact_bytes()
            .ok_or(ExternalOnnxRuntimeError::Session)?;
        let input_elements = fallback.policy.preflight(model_bytes)?.input_elements();
        let (worker, warm_up) = OnnxWorker::start_external(
            fallback.worker.program(),
            model_bytes,
            fallback.policy.input_shape(),
            input_elements,
            fallback.policy.inference_deadline(),
            &runtime.library_path,
            runtime.library_digest,
            &runtime.runtime_version,
            runtime.platform.wire_id(),
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
        let deadlines = RuntimeDeadlines::from_budget(Instant::now(), self.worker.deadline())
            .ok_or(InferenceError::OnnxDeadlineExceeded)?;
        let fallback_input = normalized.clone();
        let score = match self.worker.execute_until(normalized, deadlines.external) {
            Ok(score) => f64::from(score),
            Err(_) => {
                return self
                    .fallback
                    .infer_normalized_until(fallback_input, deadlines.total);
            }
        };
        let (decision, confidence) = match decide(score, self.metadata().decision_thresholds()) {
            Ok(result) => result,
            Err(_) => {
                return self
                    .fallback
                    .infer_normalized_until(fallback_input, deadlines.total);
            }
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
    #[error("external ONNX Runtime loading is unsupported on this platform")]
    UnsupportedPlatform,
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
    #[error("external ONNX Runtime generation could not be sealed")]
    Seal,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn external_runtime_reserves_half_of_the_total_deadline_for_tract() {
        let started_at = Instant::now();
        let deadlines = RuntimeDeadlines::from_budget(started_at, Duration::from_millis(200));

        assert_eq!(
            deadlines,
            Some(RuntimeDeadlines {
                external: started_at + Duration::from_millis(100),
                total: started_at + Duration::from_millis(200),
            })
        );
    }
}
