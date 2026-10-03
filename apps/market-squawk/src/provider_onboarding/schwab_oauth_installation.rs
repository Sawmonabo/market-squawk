//! Installed-product capabilities for Schwab OAuth browser consent and loopback TLS.
//!
//! The identity is generated once below the installation-global control root. Browser trust is a
//! separate explicit foreground action; changing workspaces never regenerates or broadens it.

use std::fmt;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use cap_fs_ext::{FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use chrono::Datelike as _;
use market_squawk_adapter_schwab::{
    AuthorizationRequest, OAuthLoopbackTlsAcceptError, OAuthLoopbackTlsAcceptFuture,
    OAuthLoopbackTlsAcceptor, OAuthLoopbackTlsStream,
};
use market_squawk_platform::ControlRoot;
use market_squawk_runtime::InstallationId;
#[cfg(target_os = "macos")]
use objc2::rc::autoreleasepool;
#[cfg(target_os = "macos")]
use objc2_app_kit::NSWorkspace;
#[cfg(target_os = "macos")]
use objc2_foundation::{NSString, NSURL};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::schwab_oauth_runtime::{
    SchwabOAuthBrowser, SchwabOAuthBrowserError, SchwabOAuthBrowserFuture,
};

const TLS_MAXIMUM_FRAGMENT_BYTES: usize = 16 * 1024;
const TLS_MINIMUM_FRAGMENT_BYTES: usize = 32;
const HTTP_1_1_ALPN: &[u8] = b"http/1.1";
const IDENTITY_PARENT_DIRECTORY: &str = "provider-onboarding";
const IDENTITY_DIRECTORY: &str = "schwab-oauth-loopback-v1";
const CERTIFICATE_FILE: &str = "certificate.der";
const PRIVATE_KEY_FILE: &str = "private-key.der";
const RECEIPT_FILE: &str = "identity.json";
const MAXIMUM_CERTIFICATE_BYTES: u64 = 64 * 1024;
const MAXIMUM_PRIVATE_KEY_BYTES: u64 = 64 * 1024;
const MAXIMUM_RECEIPT_BYTES: u64 = 16 * 1024;
const RECEIPT_SCHEMA_VERSION: u16 = 1;
const CALLBACK_HOST: &str = "127.0.0.1";
const SECURITY_TOOL: &str = "/usr/bin/security";
const CERTIFICATE_VALIDITY_YEARS: i32 = 5;
const TRUST_STATUS_TIMEOUT: Duration = Duration::from_secs(15);
const TRUST_MUTATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Secret-free failure while admitting an installation-owned callback identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabOAuthInstallationCapabilityError {
    #[error("the Schwab OAuth loopback TLS identity is not usable")]
    InvalidTlsIdentity,
    #[error("Schwab OAuth browser trust enrollment was not completed")]
    TrustEnrollment,
    #[error("Schwab OAuth browser trust enrollment was cancelled")]
    TrustCancelled,
    #[error("Schwab OAuth browser trust enrollment timed out")]
    TrustTimeout,
    #[error("Schwab OAuth browser trust is unsupported on this platform")]
    UnsupportedPlatform,
}

/// Secret-free current browser-trust disposition for the fixed Schwab callback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabOAuthInstallationTrustState {
    Trusted,
    SetupRequired,
    RepairRequired,
    Unsupported,
}

/// Explicit native action over the installation's exact Schwab callback trust.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabOAuthInstallationTrustAction {
    Status,
    Enroll,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct InstallationIdentityReceipt {
    schema_version: u16,
    installation_id: Uuid,
    host: String,
    certificate_sha256: String,
    created_at_unix_seconds: u64,
    not_before_year: i32,
    not_after_year: i32,
    trust_domain: String,
    trust_result: String,
}

/// One installation-global callback identity shared by every selected workspace.
#[derive(Clone)]
pub(crate) struct InstallationSchwabOAuthIdentity {
    control_root: ControlRoot,
    installation_id: InstallationId,
    certificate_sha256: [u8; 32],
}

impl fmt::Debug for InstallationSchwabOAuthIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstallationSchwabOAuthIdentity")
            .field("installation_id", &self.installation_id)
            .field("identity", &"[INSTALLATION-GLOBAL TLS IDENTITY]")
            .finish()
    }
}

impl InstallationSchwabOAuthIdentity {
    /// Loads the exact installation identity or publishes it once when absent.
    ///
    /// This operation never changes OS trust and therefore may run during service startup.
    pub(crate) fn try_prepare(
        control_root: &ControlRoot,
        installation_id: InstallationId,
    ) -> Result<Self, SchwabOAuthInstallationCapabilityError> {
        let control = control_root
            .try_clone_directory()
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
        control
            .create_dir_all(IDENTITY_PARENT_DIRECTORY)
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
        let parent = control
            .open_dir(IDENTITY_PARENT_DIRECTORY)
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
        require_private_or_tighten_directory(&parent)?;

        match parent.symlink_metadata(IDENTITY_DIRECTORY) {
            Ok(_) => Self::load(control_root.clone(), installation_id),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match publish_identity(&parent, installation_id) {
                    Ok(()) => Self::load(control_root.clone(), installation_id),
                    Err(publication_error) => Self::load(control_root.clone(), installation_id)
                        .map_err(|_load_error| publication_error),
                }
            }
            Err(_error) => Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity),
        }
    }

    fn load(
        control_root: ControlRoot,
        installation_id: InstallationId,
    ) -> Result<Self, SchwabOAuthInstallationCapabilityError> {
        let directory = open_identity_directory(&control_root)?;
        let certificate = read_identity_file(
            &directory,
            CERTIFICATE_FILE,
            MAXIMUM_CERTIFICATE_BYTES,
            false,
        )?;
        let private_key = read_identity_file(
            &directory,
            PRIVATE_KEY_FILE,
            MAXIMUM_PRIVATE_KEY_BYTES,
            true,
        )?;
        let receipt_bytes =
            read_identity_file(&directory, RECEIPT_FILE, MAXIMUM_RECEIPT_BYTES, false)?;
        let receipt: InstallationIdentityReceipt = serde_json::from_slice(&receipt_bytes)
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
        let certificate_sha256: [u8; 32] = Sha256::digest(&certificate).into();
        let current_year = chrono::Utc::now().year();
        if receipt.schema_version != RECEIPT_SCHEMA_VERSION
            || receipt.installation_id != installation_id.as_uuid()
            || receipt.host != CALLBACK_HOST
            || receipt.certificate_sha256 != lower_hex(&certificate_sha256)
            || receipt.created_at_unix_seconds == 0
            || receipt.not_before_year > current_year
            || receipt.not_after_year <= current_year
            || receipt.trust_domain != "user"
            || receipt.trust_result != "trust_as_root"
        {
            return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
        }
        // Rustls validates the exact private key/certificate match before this identity is
        // admitted. This creates no listener and retains no copy of caller-visible secret bytes.
        build_tls_acceptor(certificate, private_key)?;
        Ok(Self {
            control_root,
            installation_id,
            certificate_sha256,
        })
    }

    fn certificate_path(&self) -> PathBuf {
        self.control_root
            .root()
            .join(IDENTITY_PARENT_DIRECTORY)
            .join(IDENTITY_DIRECTORY)
            .join(CERTIFICATE_FILE)
    }

    /// Checks the actual user-domain SSL trust for the exact certificate and hostname.
    pub(crate) async fn trust_state(
        &self,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthInstallationTrustState, SchwabOAuthInstallationCapabilityError> {
        verify_security_tool_path()?;
        #[cfg(target_os = "macos")]
        {
            let mut command = Command::new(SECURITY_TOOL);
            command
                .args(["verify-cert", "-c"])
                .arg(self.certificate_path())
                .args(["-p", "ssl", "-n", CALLBACK_HOST, "-L", "-q"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let status = run_security_tool(command, TRUST_STATUS_TIMEOUT, cancellation).await?;
            return Ok(if status.success() {
                SchwabOAuthInstallationTrustState::Trusted
            } else {
                SchwabOAuthInstallationTrustState::SetupRequired
            });
        }
        #[cfg(not(target_os = "macos"))]
        Err(SchwabOAuthInstallationCapabilityError::UnsupportedPlatform)
    }

    /// Performs the explicit foreground per-user macOS trust transition and verifies it.
    pub(crate) async fn enroll_trust_foreground(
        &self,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthInstallationTrustState, SchwabOAuthInstallationCapabilityError> {
        if self.trust_state(cancellation.child_token()).await?
            == SchwabOAuthInstallationTrustState::Trusted
        {
            return Ok(SchwabOAuthInstallationTrustState::Trusted);
        }
        #[cfg(target_os = "macos")]
        {
            let mut command = Command::new(SECURITY_TOOL);
            command
                .args([
                    "add-trusted-cert",
                    "-r",
                    "trustAsRoot",
                    "-p",
                    "ssl",
                    "-s",
                    CALLBACK_HOST,
                ])
                .arg(self.certificate_path())
                .stdin(Stdio::null());
            let status = match run_security_tool(
                command,
                TRUST_MUTATION_TIMEOUT,
                cancellation.child_token(),
            )
            .await
            {
                Ok(status) => status,
                Err(
                    error @ (SchwabOAuthInstallationCapabilityError::TrustCancelled
                    | SchwabOAuthInstallationCapabilityError::TrustTimeout),
                ) => {
                    if self.trust_state(CancellationToken::new()).await?
                        == SchwabOAuthInstallationTrustState::Trusted
                    {
                        return Ok(SchwabOAuthInstallationTrustState::Trusted);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            if !status.success()
                || self.trust_state(CancellationToken::new()).await?
                    != SchwabOAuthInstallationTrustState::Trusted
            {
                return Err(SchwabOAuthInstallationCapabilityError::TrustEnrollment);
            }
            Ok(SchwabOAuthInstallationTrustState::Trusted)
        }
        #[cfg(not(target_os = "macos"))]
        Err(SchwabOAuthInstallationCapabilityError::UnsupportedPlatform)
    }

    fn load_tls_acceptor(
        &self,
    ) -> Result<InstallationSchwabOAuthTlsAcceptor, SchwabOAuthInstallationCapabilityError> {
        let directory = open_identity_directory(&self.control_root)?;
        let certificate = read_identity_file(
            &directory,
            CERTIFICATE_FILE,
            MAXIMUM_CERTIFICATE_BYTES,
            false,
        )?;
        if <[u8; 32]>::from(Sha256::digest(&certificate)) != self.certificate_sha256 {
            return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
        }
        let private_key = read_identity_file(
            &directory,
            PRIVATE_KEY_FILE,
            MAXIMUM_PRIVATE_KEY_BYTES,
            true,
        )?;
        build_tls_acceptor(certificate, private_key)
    }
}

pub(crate) async fn apply_installation_trust_action(
    control_root: &ControlRoot,
    installation_id: InstallationId,
    action: SchwabOAuthInstallationTrustAction,
    cancellation: CancellationToken,
) -> Result<SchwabOAuthInstallationTrustState, SchwabOAuthInstallationCapabilityError> {
    let current =
        inspect_installation_trust(control_root, installation_id, cancellation.child_token())
            .await?;
    match action {
        SchwabOAuthInstallationTrustAction::Status => Ok(current),
        SchwabOAuthInstallationTrustAction::Enroll => {
            if current != SchwabOAuthInstallationTrustState::SetupRequired {
                return Ok(current);
            }
            let identity =
                InstallationSchwabOAuthIdentity::try_prepare(control_root, installation_id)?;
            identity.enroll_trust_foreground(cancellation).await
        }
    }
}

async fn inspect_installation_trust(
    control_root: &ControlRoot,
    installation_id: InstallationId,
    cancellation: CancellationToken,
) -> Result<SchwabOAuthInstallationTrustState, SchwabOAuthInstallationCapabilityError> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (control_root, installation_id, cancellation);
        return Ok(SchwabOAuthInstallationTrustState::Unsupported);
    }
    #[cfg(target_os = "macos")]
    {
        if !identity_exists(control_root)? {
            return Ok(SchwabOAuthInstallationTrustState::SetupRequired);
        }
        match InstallationSchwabOAuthIdentity::load(control_root.clone(), installation_id) {
            Ok(identity) => identity.trust_state(cancellation).await,
            Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity) => {
                Ok(SchwabOAuthInstallationTrustState::RepairRequired)
            }
            Err(error) => Err(error),
        }
    }
}

fn identity_exists(
    control_root: &ControlRoot,
) -> Result<bool, SchwabOAuthInstallationCapabilityError> {
    let control = control_root
        .try_clone_directory()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let parent = match control.open_dir(IDENTITY_PARENT_DIRECTORY) {
        Ok(parent) => parent,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_error) => return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity),
    };
    match parent.symlink_metadata(IDENTITY_DIRECTORY) {
        Ok(_metadata) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_error) => Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity),
    }
}

/// Server-side loopback TLS backed by an installation-supplied certificate identity.
///
/// The constructor retains the supplied certificate resolver and signing identity while replacing
/// configuration that is unnecessary for a one-shot local callback. The adapter independently
/// owns the fixed listener address and bounded handshake lifetime.
pub(crate) struct InstallationSchwabOAuthTlsAcceptor {
    acceptor: TlsAcceptor,
}

impl InstallationSchwabOAuthTlsAcceptor {
    /// Loads the exact already-prepared installation identity into the TLS acceptor.
    pub(crate) fn try_from_identity(
        identity: &InstallationSchwabOAuthIdentity,
    ) -> Result<Self, SchwabOAuthInstallationCapabilityError> {
        identity.load_tls_acceptor()
    }

    /// Admits one complete rustls server configuration supplied by the installation owner.
    ///
    /// The caller must provision a certificate identity accepted by the user's browser for the
    /// code-owned loopback callback name. This function never generates, exports, or installs
    /// certificate or private-key material.
    pub(crate) fn try_new(
        mut server_configuration: rustls::ServerConfig,
    ) -> Result<Self, SchwabOAuthInstallationCapabilityError> {
        if server_configuration
            .max_fragment_size
            .is_some_and(|maximum| {
                !(TLS_MINIMUM_FRAGMENT_BYTES..=TLS_MAXIMUM_FRAGMENT_BYTES).contains(&maximum)
            })
        {
            return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
        }

        server_configuration.alpn_protocols = vec![HTTP_1_1_ALPN.to_vec()];
        server_configuration.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        server_configuration.ticketer = Arc::new(DisabledTlsTickets);
        server_configuration.key_log = Arc::new(rustls::NoKeyLog);
        server_configuration.enable_secret_extraction = false;
        server_configuration.max_early_data_size = 0;
        server_configuration.send_half_rtt_data = false;
        server_configuration.send_tls13_tickets = 0;
        server_configuration.max_tls13_tickets = 0;
        server_configuration.cert_compressors.clear();
        server_configuration.cert_decompressors.clear();

        let server_configuration = Arc::new(server_configuration);
        rustls::ServerConnection::new(Arc::clone(&server_configuration))
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
        Ok(Self {
            acceptor: TlsAcceptor::from(server_configuration),
        })
    }
}

fn open_identity_directory(
    control_root: &ControlRoot,
) -> Result<Dir, SchwabOAuthInstallationCapabilityError> {
    let control = control_root
        .try_clone_directory()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let parent = control
        .open_dir(IDENTITY_PARENT_DIRECTORY)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    require_private_directory(
        &parent
            .dir_metadata()
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?,
    )?;
    let named = parent
        .symlink_metadata(IDENTITY_DIRECTORY)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    if !named.is_dir() || named.file_type().is_symlink() {
        return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
    }
    let directory = parent
        .open_dir(IDENTITY_DIRECTORY)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let opened = directory
        .dir_metadata()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    if !opened.is_dir()
        || opened.file_type().is_symlink()
        || (named.dev(), named.ino()) != (opened.dev(), opened.ino())
    {
        return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
    }
    require_private_directory(&opened)?;
    Ok(directory)
}

fn publish_identity(
    parent: &Dir,
    installation_id: InstallationId,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    let staging_directory = format!("{IDENTITY_DIRECTORY}.pending-{}", Uuid::new_v4().simple());
    parent
        .create_dir(&staging_directory)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    set_private_directory_permissions(parent, &staging_directory)?;
    let staging = parent
        .open_dir(&staging_directory)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    require_private_directory(
        &staging
            .dir_metadata()
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?,
    )?;

    let current_year = chrono::Utc::now().year();
    let not_before_year = current_year
        .checked_sub(1)
        .ok_or(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let not_after_year = current_year
        .checked_add(CERTIFICATE_VALIDITY_YEARS)
        .ok_or(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;

    let issuer_key = KeyPair::generate()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let mut issuer_params = CertificateParams::default();
    issuer_params.not_before = rcgen::date_time_ymd(not_before_year, 1, 1);
    issuer_params.not_after = rcgen::date_time_ymd(not_after_year, 12, 31);
    issuer_params.distinguished_name = rcgen::DistinguishedName::new();
    issuer_params
        .distinguished_name
        .push(DnType::OrganizationName, "Market Squawk");
    issuer_params
        .distinguished_name
        .push(DnType::CommonName, "Market Squawk one-time local issuer");
    issuer_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    issuer_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let issuer = Issuer::new(issuer_params, issuer_key);

    let leaf_key = KeyPair::generate()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let mut leaf_params = CertificateParams::new(vec![CALLBACK_HOST.to_owned()])
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    leaf_params.not_before = rcgen::date_time_ymd(not_before_year, 1, 1);
    leaf_params.not_after = rcgen::date_time_ymd(not_after_year, 12, 31);
    leaf_params.distinguished_name = rcgen::DistinguishedName::new();
    leaf_params
        .distinguished_name
        .push(DnType::OrganizationName, "Market Squawk");
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "Market Squawk Schwab OAuth loopback");
    leaf_params.is_ca = IsCa::ExplicitNoCa;
    leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let certificate = leaf_params
        .signed_by(&leaf_key, &issuer)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let certificate = certificate.der().as_ref();
    let private_key = Zeroizing::new(leaf_key.serialize_der());
    let certificate_sha256: [u8; 32] = Sha256::digest(certificate).into();
    let created_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?
        .as_secs();
    let receipt = serde_json::to_vec(&InstallationIdentityReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        installation_id: installation_id.as_uuid(),
        host: CALLBACK_HOST.to_owned(),
        certificate_sha256: lower_hex(&certificate_sha256),
        created_at_unix_seconds,
        not_before_year,
        not_after_year,
        trust_domain: "user".to_owned(),
        trust_result: "trust_as_root".to_owned(),
    })
    .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let mut renamed = false;
    let publication = (|| {
        write_identity_file(&staging, CERTIFICATE_FILE, certificate)?;
        write_identity_file(&staging, PRIVATE_KEY_FILE, private_key.as_slice())?;
        write_identity_file(&staging, RECEIPT_FILE, &receipt)?;
        sync_directory(&staging)?;
        parent
            .rename(&staging_directory, parent, IDENTITY_DIRECTORY)
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
        renamed = true;
        sync_directory(parent)
    })();
    if publication.is_err() && !renamed {
        let _ = parent.remove_dir_all(&staging_directory);
    }
    publication
}

fn sync_directory(directory: &Dir) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    directory
        .try_clone()
        .and_then(|opened| opened.into_std_file().sync_all())
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)
}

fn write_identity_file(
    directory: &Dir,
    name: &str,
    bytes: &[u8],
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    if bytes.is_empty() {
        return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
    }
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    let mut file = directory
        .open_with(name, &options)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    set_private_file_permissions(&file)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let metadata = file
        .metadata()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.nlink() != 1
        || metadata.len() != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
    {
        return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
    }
    require_private_file(&metadata)
}

fn build_tls_acceptor(
    certificate: Vec<u8>,
    private_key: Vec<u8>,
) -> Result<InstallationSchwabOAuthTlsAcceptor, SchwabOAuthInstallationCapabilityError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let certificate = rustls::pki_types::CertificateDer::from(certificate);
    let private_key = rustls::pki_types::PrivateKeyDer::try_from(private_key)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let configuration = builder
        .with_no_client_auth()
        .with_single_cert(vec![certificate], private_key)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    InstallationSchwabOAuthTlsAcceptor::try_new(configuration)
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn verify_security_tool_path() -> Result<(), SchwabOAuthInstallationCapabilityError> {
    #[cfg(target_os = "macos")]
    {
        let metadata = std::fs::symlink_metadata(SECURITY_TOOL)
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::TrustEnrollment)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(SchwabOAuthInstallationCapabilityError::TrustEnrollment);
        }
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    Err(SchwabOAuthInstallationCapabilityError::UnsupportedPlatform)
}

async fn run_security_tool(
    mut command: Command,
    timeout: Duration,
    cancellation: CancellationToken,
) -> Result<std::process::ExitStatus, SchwabOAuthInstallationCapabilityError> {
    if timeout.is_zero() || cancellation.is_cancelled() {
        return Err(SchwabOAuthInstallationCapabilityError::TrustCancelled);
    }
    command.kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::TrustEnrollment)?;
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            if let Some(status) = child
                .try_wait()
                .map_err(|_error| SchwabOAuthInstallationCapabilityError::TrustEnrollment)?
            {
                return Ok(status);
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(SchwabOAuthInstallationCapabilityError::TrustCancelled)
        }
        () = &mut deadline => {
            if let Some(status) = child
                .try_wait()
                .map_err(|_error| SchwabOAuthInstallationCapabilityError::TrustEnrollment)?
            {
                return Ok(status);
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(SchwabOAuthInstallationCapabilityError::TrustTimeout)
        }
        status = child.wait() => status
            .map_err(|_error| SchwabOAuthInstallationCapabilityError::TrustEnrollment),
    }
}

#[cfg(target_os = "macos")]
fn open_authorization_in_browser(
    url: &str,
    cancellation: &CancellationToken,
) -> Result<(), SchwabOAuthBrowserError> {
    if cancellation.is_cancelled() {
        return Err(SchwabOAuthBrowserError::Cancelled);
    }
    autoreleasepool(|_| {
        let native_url = NSURL::URLWithString(&NSString::from_str(url))
            .ok_or(SchwabOAuthBrowserError::Unavailable)?;
        if NSWorkspace::sharedWorkspace().openURL(&native_url) {
            Ok(())
        } else {
            Err(SchwabOAuthBrowserError::Unavailable)
        }
    })
}

#[cfg(not(target_os = "macos"))]
fn open_authorization_in_browser(
    _url: &str,
    _cancellation: &CancellationToken,
) -> Result<(), SchwabOAuthBrowserError> {
    Err(SchwabOAuthBrowserError::Unavailable)
}

fn require_private_or_tighten_directory(
    directory: &Dir,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    set_private_directory_permissions(directory, ".")?;
    let metadata = directory
        .dir_metadata()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    require_private_directory(&metadata)
}

#[cfg(unix)]
fn set_private_directory_permissions(
    directory: &Dir,
    path: impl AsRef<Path>,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    use cap_std::fs::PermissionsExt as _;

    directory
        .set_permissions(path, cap_std::fs::Permissions::from_mode(0o700))
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)
}

#[cfg(not(unix))]
fn set_private_directory_permissions(
    _directory: &Dir,
    _path: impl AsRef<Path>,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(
    file: &cap_std::fs::File,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    use cap_std::fs::PermissionsExt as _;

    file.set_permissions(cap_std::fs::Permissions::from_mode(0o600))
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)
}

#[cfg(not(unix))]
fn set_private_file_permissions(
    _file: &cap_std::fs::File,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    Ok(())
}

fn read_identity_file(
    directory: &Dir,
    name: &str,
    maximum_bytes: u64,
    private: bool,
) -> Result<Vec<u8>, SchwabOAuthInstallationCapabilityError> {
    let named = directory
        .symlink_metadata(name)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    if !named.is_file()
        || named.file_type().is_symlink()
        || named.len() == 0
        || named.len() > maximum_bytes
        || (private && named.nlink() != 1)
    {
        return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
    }
    require_private_file(&named)?;

    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = directory
        .open_with(name, &options)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let opened = file
        .metadata()
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    if !opened.is_file()
        || opened.file_type().is_symlink()
        || opened.len() != named.len()
        || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
    {
        return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
    }
    require_private_file(&opened)?;
    let capacity = usize::try_from(opened.len())
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    let final_metadata = directory
        .symlink_metadata(name)
        .map_err(|_error| SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)?;
    if bytes.len() != capacity
        || final_metadata.len() != opened.len()
        || (final_metadata.dev(), final_metadata.ino()) != (opened.dev(), opened.ino())
    {
        return Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity);
    }
    Ok(bytes)
}

#[cfg(unix)]
fn require_private_directory(
    metadata: &cap_std::fs::Metadata,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    use cap_std::fs::PermissionsExt as _;

    if metadata.permissions().mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)
    }
}

#[cfg(not(unix))]
fn require_private_directory(
    _metadata: &cap_std::fs::Metadata,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    Ok(())
}

#[cfg(unix)]
fn require_private_file(
    metadata: &cap_std::fs::Metadata,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    use cap_std::fs::PermissionsExt as _;

    if metadata.permissions().mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity)
    }
}

#[cfg(not(unix))]
fn require_private_file(
    _metadata: &cap_std::fs::Metadata,
) -> Result<(), SchwabOAuthInstallationCapabilityError> {
    Ok(())
}

impl fmt::Debug for InstallationSchwabOAuthTlsAcceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstallationSchwabOAuthTlsAcceptor([PROTECTED TLS IDENTITY])")
    }
}

impl OAuthLoopbackTlsAcceptor for InstallationSchwabOAuthTlsAcceptor {
    fn accept(&self, stream: TcpStream) -> OAuthLoopbackTlsAcceptFuture<'_> {
        let acceptor = self.acceptor.clone();
        Box::pin(async move {
            let stream = acceptor
                .accept(stream)
                .await
                .map_err(|_error| OAuthLoopbackTlsAcceptError)?;
            let stream: Box<dyn OAuthLoopbackTlsStream> = Box::new(stream);
            Ok(stream)
        })
    }
}

#[derive(Debug)]
struct DisabledTlsTickets;

impl rustls::server::ProducesTickets for DisabledTlsTickets {
    fn enabled(&self) -> bool {
        false
    }

    fn lifetime(&self) -> u32 {
        0
    }

    fn encrypt(&self, _plain: &[u8]) -> Option<Vec<u8>> {
        None
    }

    fn decrypt(&self, _cipher: &[u8]) -> Option<Vec<u8>> {
        None
    }
}

/// Cancellation-aware dispatch to the operating system's default hardened browser.
#[derive(Clone)]
pub(crate) struct InstallationSchwabOAuthBrowser {
    identity: Arc<InstallationSchwabOAuthIdentity>,
}

impl InstallationSchwabOAuthBrowser {
    pub(crate) fn new(identity: Arc<InstallationSchwabOAuthIdentity>) -> Self {
        Self { identity }
    }
}

impl fmt::Debug for InstallationSchwabOAuthBrowser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstallationSchwabOAuthBrowser([VERIFIED CALLBACK TRUST])")
    }
}

impl SchwabOAuthBrowser for InstallationSchwabOAuthBrowser {
    fn open(
        &self,
        request: AuthorizationRequest,
        cancellation: CancellationToken,
    ) -> SchwabOAuthBrowserFuture<'_> {
        let identity = Arc::clone(&self.identity);
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(SchwabOAuthBrowserError::Cancelled);
            }
            let trust = match identity.trust_state(cancellation.child_token()).await {
                Ok(trust) => trust,
                Err(SchwabOAuthInstallationCapabilityError::TrustCancelled) => {
                    return Err(SchwabOAuthBrowserError::Cancelled);
                }
                Err(_error) => return Err(SchwabOAuthBrowserError::Unavailable),
            };
            if trust != SchwabOAuthInstallationTrustState::Trusted {
                return Err(SchwabOAuthBrowserError::Unavailable);
            }
            // The direct AppKit call is the browser-launch commit boundary. Apple documents
            // NSWorkspace URL opening as thread-safe. The URL enters neither a child-process
            // argument list nor the `log` facade, and the synchronous dispatch cannot outlive or
            // detach from this future.
            open_authorization_in_browser(request.expose_url(), &cancellation)
        })
    }
}
