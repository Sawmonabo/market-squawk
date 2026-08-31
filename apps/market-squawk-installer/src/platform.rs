//! Supported release platforms and code-owned installed program identities.

use std::path::PathBuf;

use clap::ValueEnum;
use directories::{BaseDirs, ProjectDirs};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One platform for which a complete Market Squawk release is built and verified.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum SupportedTarget {
    /// Apple Silicon macOS.
    #[serde(rename = "aarch64-apple-darwin")]
    Aarch64AppleDarwin,
    /// Intel macOS.
    #[serde(rename = "x86_64-apple-darwin")]
    X86_64AppleDarwin,
    /// 64-bit Windows using the Microsoft toolchain.
    #[serde(rename = "x86_64-pc-windows-msvc")]
    X86_64PcWindowsMsvc,
    /// 64-bit glibc Linux.
    #[serde(rename = "x86_64-unknown-linux-gnu")]
    X86_64UnknownLinuxGnu,
}

/// Native publisher-trust evidence carried by one release target.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[value(rename_all = "kebab-case")]
pub enum NativeTrustMode {
    /// Integrity and build provenance are verified without a native publisher identity.
    ProvenanceOnly,
    /// Apple Developer ID signing, timestamping, notarization, and stapling were verified.
    DeveloperIdSignedAndNotarized,
    /// Authenticode publisher signing and timestamping were verified.
    AuthenticodeSigned,
}

impl NativeTrustMode {
    /// Returns whether this trust mode is meaningful for the release target.
    pub const fn supports(self, target: SupportedTarget) -> bool {
        match self {
            Self::ProvenanceOnly => true,
            Self::DeveloperIdSignedAndNotarized => matches!(
                target,
                SupportedTarget::Aarch64AppleDarwin | SupportedTarget::X86_64AppleDarwin
            ),
            Self::AuthenticodeSigned => {
                matches!(target, SupportedTarget::X86_64PcWindowsMsvc)
            }
        }
    }
}

impl SupportedTarget {
    /// Detects the current supported release target.
    ///
    /// # Errors
    ///
    /// Returns [`PlatformError::Unsupported`] on an operating-system and architecture pair that
    /// has no V1 release.
    pub fn current() -> Result<Self, PlatformError> {
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        {
            Ok(Self::Aarch64AppleDarwin)
        }
        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        {
            Ok(Self::X86_64AppleDarwin)
        }
        #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
        {
            Ok(Self::X86_64PcWindowsMsvc)
        }
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            Ok(Self::X86_64UnknownLinuxGnu)
        }
        #[cfg(not(any(
            all(target_os = "macos", target_arch = "aarch64"),
            all(target_os = "macos", target_arch = "x86_64"),
            all(target_os = "windows", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "x86_64"),
        )))]
        {
            Err(PlatformError::Unsupported {
                operating_system: std::env::consts::OS,
                architecture: std::env::consts::ARCH,
            })
        }
    }

    /// Returns the canonical Rust target triple serialized into release manifests.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aarch64AppleDarwin => "aarch64-apple-darwin",
            Self::X86_64AppleDarwin => "x86_64-apple-darwin",
            Self::X86_64PcWindowsMsvc => "x86_64-pc-windows-msvc",
            Self::X86_64UnknownLinuxGnu => "x86_64-unknown-linux-gnu",
        }
    }

    /// Returns the exact minimum operating-system contract for this release target.
    pub const fn minimum_system(self) -> &'static str {
        match self {
            Self::Aarch64AppleDarwin | Self::X86_64AppleDarwin => "macOS 12",
            Self::X86_64PcWindowsMsvc => "Windows 10 1809",
            Self::X86_64UnknownLinuxGnu => "Ubuntu 24.04-compatible",
        }
    }

    /// Returns the native executable suffix for this release target.
    pub const fn executable_suffix(self) -> &'static str {
        match self {
            Self::X86_64PcWindowsMsvc => ".exe",
            Self::Aarch64AppleDarwin | Self::X86_64AppleDarwin | Self::X86_64UnknownLinuxGnu => "",
        }
    }
}

/// A fixed installed program that the lifecycle launcher may execute.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ProgramName {
    /// The permanent desktop application.
    Desktop,
    /// The permanent installed application service.
    Service,
    /// The permanent local MCP relay shared by supported clients.
    McpRelay,
    /// The Market Squawk command-line interface and stdio MCP server.
    Cli,
    /// The isolated raw-capture helper.
    CaptureHelper,
    /// The isolated ONNX inference worker.
    OnnxWorker,
    /// The sealed model-bundle validator.
    ModelValidator,
    /// The versioned installer and maintenance command.
    Installer,
    /// The bundled uv executable.
    Uv,
    /// The bundled managed Python interpreter.
    Python,
}

impl ProgramName {
    pub(crate) fn relative_path(self, target: SupportedTarget) -> PathBuf {
        let suffix = target.executable_suffix();
        match self {
            Self::Desktop => PathBuf::from(format!("bin/market-squawk-desktop{suffix}")),
            Self::Service => PathBuf::from(format!("bin/market-squawk-service{suffix}")),
            Self::McpRelay => PathBuf::from(format!("bin/market-squawk-mcp-relay{suffix}")),
            Self::Cli => PathBuf::from(format!("bin/market-squawk{suffix}")),
            Self::CaptureHelper => {
                PathBuf::from(format!("bin/market-squawk-capture-helper{suffix}"))
            }
            Self::OnnxWorker => PathBuf::from(format!("bin/market-squawk-onnx-worker{suffix}")),
            Self::ModelValidator => {
                PathBuf::from(format!("bin/market-squawk-model-validator{suffix}"))
            }
            Self::Installer => PathBuf::from(format!("bin/market-squawk-installer{suffix}")),
            Self::Uv => PathBuf::from(format!("tools/uv{suffix}")),
            Self::Python => match target {
                SupportedTarget::X86_64PcWindowsMsvc => PathBuf::from("python.exe"),
                SupportedTarget::Aarch64AppleDarwin
                | SupportedTarget::X86_64AppleDarwin
                | SupportedTarget::X86_64UnknownLinuxGnu => PathBuf::from("bin/python"),
            },
        }
    }
}

/// Returns the platform-native per-user Market Squawk data root.
///
/// Mutable application data lives beneath this root. The separately returned program root is its
/// `program` child so an ordinary uninstall can preserve configuration, credentials, portfolios,
/// datasets, models, and logs.
///
/// # Errors
///
/// Returns [`PlatformError::StandardDirectoriesUnavailable`] when the operating system does not
/// expose a per-user application-data location.
pub fn default_installation_data_root() -> Result<PathBuf, PlatformError> {
    let directories = ProjectDirs::from("com", "MarketSquawk", "Market Squawk")
        .ok_or(PlatformError::StandardDirectoriesUnavailable)?;
    Ok(directories.data_local_dir().to_path_buf())
}

/// Returns the platform-native per-user program root.
///
/// # Errors
///
/// Returns [`PlatformError::StandardDirectoriesUnavailable`] when the operating system does not
/// expose a per-user application-data location.
pub fn default_install_root() -> Result<PathBuf, PlatformError> {
    Ok(default_installation_data_root()?.join("program"))
}

/// Returns the native desktop's default workspace-data root.
///
/// This intentionally mirrors Tauri's `app_local_data_dir`: the operating system's local data
/// directory joined with the configured desktop bundle identifier. Keeping the calculation in
/// the installer lets a background service receive an absolute path without depending on an
/// unspecified service-manager working directory.
pub(crate) fn default_workspace_data_root() -> Result<PathBuf, PlatformError> {
    const DESKTOP_IDENTIFIER: &str = "com.marketsquawk.desktop";

    let directories = BaseDirs::new().ok_or(PlatformError::StandardDirectoriesUnavailable)?;
    Ok(directories.data_local_dir().join(DESKTOP_IDENTIFIER))
}

/// Platform selection or per-user path failure.
#[derive(Debug, Error)]
pub enum PlatformError {
    /// This operating-system and architecture pair has no complete V1 release.
    #[error("unsupported release platform: {operating_system}/{architecture}")]
    Unsupported {
        /// Rust operating-system identifier.
        operating_system: &'static str,
        /// Rust architecture identifier.
        architecture: &'static str,
    },
    /// A standard per-user data location could not be determined.
    #[error("the operating system did not provide a per-user application-data directory")]
    StandardDirectoriesUnavailable,
}
