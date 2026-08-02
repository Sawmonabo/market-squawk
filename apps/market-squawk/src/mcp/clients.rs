//! Owned Claude Code and Codex registration for the installed MCP relay.
//!
//! Client configuration remains owned by each client's official CLI. Market Squawk persists only
//! non-secret receipts inside its controlled authority store and never parses or rewrites a client
//! configuration file.

mod claude;
mod codex;
mod protocol;

use std::{
    collections::BTreeSet,
    env,
    ffi::{OsStr, OsString},
    fmt, fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::Mutex,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError,
};
use market_squawk_runtime::RuntimeIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

pub use protocol::{MCP_PROTOCOL_VERSION, McpProtocolVerification};

pub const SERVER_NAME: &str = "market-squawk";
const RECEIPT_DIRECTORY: &str = "mcp/client-registrations";
const RECEIPT_FORMAT_VERSION: u16 = 1;
const MAXIMUM_COMMAND_OUTPUT_BYTES: u64 = 512 * 1024;
const CLIENT_COMMAND_TIMEOUT: Duration = Duration::from_secs(15);

/// Supported installed MCP clients with independently scoped credentials and audit identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpClientKind {
    ClaudeCode,
    Codex,
}

impl McpClientKind {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::ClaudeCode => "Claude Code",
            Self::Codex => "Codex",
        }
    }

    const fn relay_argument(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude",
            Self::Codex => "codex",
        }
    }

    const fn executable_name(self) -> &'static str {
        match self {
            Self::ClaudeCode => claude::EXECUTABLE_NAME,
            Self::Codex => codex::EXECUTABLE_NAME,
        }
    }
}

/// Truthful lifecycle state derived from the official client CLI and an owned receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpClientState {
    Absent,
    Unsupported,
    Ready,
    Owned,
    RepairRequired,
    Conflict,
}

/// Non-secret service identities bound into an owned client-registration receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct McpRegistrationAuthority {
    runtime: RuntimeIdentity,
    endpoint_identity: String,
    credential_identity: String,
}

impl McpRegistrationAuthority {
    pub fn try_new(
        runtime: RuntimeIdentity,
        endpoint_identity: impl Into<String>,
        credential_identity: impl Into<String>,
    ) -> Result<Self, McpClientRegistrationError> {
        let endpoint_identity = endpoint_identity.into();
        let credential_identity = credential_identity.into();
        if !valid_identity(&endpoint_identity) || !valid_identity(&credential_identity) {
            return Err(McpClientRegistrationError::InvalidAuthorityIdentity);
        }
        Ok(Self {
            runtime,
            endpoint_identity,
            credential_identity,
        })
    }

    #[must_use]
    pub const fn runtime(&self) -> RuntimeIdentity {
        self.runtime
    }

    #[must_use]
    pub fn endpoint_identity(&self) -> &str {
        &self.endpoint_identity
    }

    #[must_use]
    pub fn credential_identity(&self) -> &str {
        &self.credential_identity
    }
}

/// Durable, non-secret proof that Market Squawk owns one exact client entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct McpClientRegistrationReceipt {
    receipt_version: u16,
    client: McpClientKind,
    server_name: String,
    client_version: String,
    command: String,
    arguments: Vec<String>,
    command_sha256: String,
    authority: McpRegistrationAuthority,
    observed_at_unix_seconds: u64,
    #[serde(default)]
    last_verification: Option<McpProtocolVerification>,
}

impl McpClientRegistrationReceipt {
    #[must_use]
    pub const fn client(&self) -> McpClientKind {
        self.client
    }

    #[must_use]
    pub fn command_sha256(&self) -> &str {
        &self.command_sha256
    }

    #[must_use]
    pub const fn authority(&self) -> &McpRegistrationAuthority {
        &self.authority
    }

    #[must_use]
    pub fn client_version(&self) -> &str {
        &self.client_version
    }

    #[must_use]
    pub fn observed_at_unix_seconds(&self) -> u64 {
        self.observed_at_unix_seconds
    }

    #[must_use]
    pub const fn last_verification(&self) -> Option<&McpProtocolVerification> {
        self.last_verification.as_ref()
    }
}

/// Secret-free status returned to setup and repair surfaces.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpClientStatus {
    client: McpClientKind,
    state: McpClientState,
    client_version: Option<String>,
    executable: Option<String>,
    owned_receipt: Option<McpClientRegistrationReceipt>,
    blocker: Option<&'static str>,
}

impl McpClientStatus {
    #[must_use]
    pub const fn client(&self) -> McpClientKind {
        self.client
    }

    #[must_use]
    pub const fn state(&self) -> McpClientState {
        self.state
    }

    #[must_use]
    pub const fn receipt(&self) -> Option<&McpClientRegistrationReceipt> {
        self.owned_receipt.as_ref()
    }

    #[must_use]
    pub fn client_version(&self) -> Option<&str> {
        self.client_version.as_deref()
    }

    #[must_use]
    pub fn executable(&self) -> Option<&str> {
        self.executable.as_deref()
    }

    #[must_use]
    pub const fn blocker(&self) -> Option<&'static str> {
        self.blocker
    }
}

/// Registration plan passed only to official client CLI commands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpClientRegistration {
    command: String,
    arguments: Vec<String>,
}

impl McpClientRegistration {
    fn try_new(
        relay_program: &Path,
        client: McpClientKind,
    ) -> Result<Self, McpClientRegistrationError> {
        let command = relay_program
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or(McpClientRegistrationError::InvalidRelayProgram)?
            .to_owned();
        Ok(Self {
            command,
            arguments: vec!["--client".to_owned(), client.relay_argument().to_owned()],
        })
    }

    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }

    #[must_use]
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    fn digest(&self) -> Result<String, McpClientRegistrationError> {
        let encoded = serde_json::to_vec(&(self.command(), self.arguments()))
            .map_err(|_error| McpClientRegistrationError::ReceiptEncoding)?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }

    fn matches(&self, observed: &ObservedRegistration) -> bool {
        matches!(
            observed,
            ObservedRegistration::Present {
                transport,
                command,
                arguments,
                has_environment: false,
            } if transport == "stdio" && command == &self.command && arguments == &self.arguments
        )
    }
}

/// Exclusive registration authority for both supported clients.
pub struct McpClientRegistrationManager {
    receipts: LocalAuthorityStateStore,
    relay_program: PathBuf,
    search_directories: Vec<PathBuf>,
    mutation_gate: Mutex<()>,
}

impl fmt::Debug for McpClientRegistrationManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpClientRegistrationManager")
            .field("receipts", &self.receipts)
            .field("relay_program", &"[VERIFIED INSTALLED PROGRAM]")
            .field("search_directories", &"[CONTROLLED SEARCH POLICY]")
            .finish()
    }
}

impl McpClientRegistrationManager {
    /// Opens the owner-only receipt store and verifies the relay is a safe installed executable.
    pub fn try_new(
        paths: &LocalPaths,
        relay_program: impl AsRef<Path>,
    ) -> Result<Self, McpClientRegistrationError> {
        let relay_program = verify_executable(relay_program.as_ref())?;
        let receipts = LocalAuthorityStateStore::try_open(
            paths.control_root()?.root().join(RECEIPT_DIRECTORY),
        )?;
        Ok(Self {
            receipts,
            relay_program,
            search_directories: discovery_directories(),
            mutation_gate: Mutex::new(()),
        })
    }

    /// Inspects one client without mutating its configuration.
    pub fn inspect(
        &self,
        client: McpClientKind,
    ) -> Result<McpClientStatus, McpClientRegistrationError> {
        let Some(program) = self.find_client_program(client)? else {
            return Ok(status(client, McpClientState::Absent, None, None, None));
        };
        let version_output = run_checked(&version_command(client, &program))?;
        let capability_output = run_checked(&capability_command(client, &program))?;
        let version = output_text(&version_output);
        let capability = output_text(&capability_output);
        if !version_output.status.success() || !capability_output.status.success() {
            return Ok(status(
                client,
                McpClientState::Unsupported,
                (!version.is_empty()).then_some(version),
                Some(program),
                Some("The installed client rejected its official MCP capability check."),
            ));
        }
        if !supports(client, &version, &capability) {
            return Ok(status(
                client,
                McpClientState::Unsupported,
                Some(version),
                Some(program),
                Some("The installed client does not expose the required official MCP commands."),
            ));
        }
        let registration = McpClientRegistration::try_new(&self.relay_program, client)?;
        let observed = observe(client, &program)?;
        let document = self.load_receipts()?;
        let receipt = document.receipt(client).cloned();
        let state = match (&observed, receipt.as_ref()) {
            (ObservedRegistration::Missing, _) => McpClientState::Ready,
            (observed, Some(receipt))
                if registration.matches(observed)
                    && receipt.command_sha256 == registration.digest()? =>
            {
                McpClientState::Owned
            }
            (ObservedRegistration::Present { .. }, _) => McpClientState::Conflict,
        };
        Ok(McpClientStatus {
            client,
            state,
            client_version: Some(version),
            executable: Some(program.to_string_lossy().into_owned()),
            owned_receipt: receipt,
            blocker: (state == McpClientState::Conflict).then_some(
                "A same-name client entry is not the exact Market Squawk-owned registration.",
            ),
        })
    }

    /// Inspects one client and proves its receipt belongs to the current service authority.
    pub fn inspect_with_authority(
        &self,
        client: McpClientKind,
        authority: &McpRegistrationAuthority,
    ) -> Result<McpClientStatus, McpClientRegistrationError> {
        let mut status = self.inspect(client)?;
        if status.state == McpClientState::Owned
            && status
                .owned_receipt
                .as_ref()
                .is_some_and(|receipt| &receipt.authority != authority)
        {
            status.state = McpClientState::RepairRequired;
            status.blocker = Some(
                "The owned client entry belongs to an earlier service or credential identity.",
            );
        }
        Ok(status)
    }

    /// Registers a missing entry or refreshes an exact already-owned entry.
    pub fn connect(
        &self,
        client: McpClientKind,
        authority: McpRegistrationAuthority,
    ) -> Result<McpClientStatus, McpClientRegistrationError> {
        let status = self.inspect(client)?;
        let program = status
            .executable
            .as_deref()
            .map(PathBuf::from)
            .ok_or(McpClientRegistrationError::ClientUnavailable { client })?;
        let registration = McpClientRegistration::try_new(&self.relay_program, client)?;
        match status.state {
            McpClientState::Ready => {
                run_success(&add_command(client, &program, &registration), client)?;
            }
            McpClientState::Owned => {}
            McpClientState::Conflict => {
                return Err(McpClientRegistrationError::UnownedConflict { client });
            }
            McpClientState::Absent
            | McpClientState::Unsupported
            | McpClientState::RepairRequired => {
                return Err(McpClientRegistrationError::ClientUnavailable { client });
            }
        }
        let observed = observe(client, &program)?;
        if !registration.matches(&observed) {
            return Err(McpClientRegistrationError::RegistrationVerification { client });
        }
        let version = status
            .client_version
            .ok_or(McpClientRegistrationError::InvalidClientOutput { client })?;
        let receipt = receipt(client, version, registration, authority)?;
        self.mutate_receipts(|document| document.upsert(receipt))?;
        self.inspect(client)
    }

    /// Repairs only an entry already owned by a durable Market Squawk receipt.
    pub fn repair(
        &self,
        client: McpClientKind,
        authority: McpRegistrationAuthority,
    ) -> Result<McpClientStatus, McpClientRegistrationError> {
        if self.load_receipts()?.receipt(client).is_none() {
            return Err(McpClientRegistrationError::OwnershipRequired { client });
        }
        let status = self.inspect(client)?;
        if status.state == McpClientState::Conflict {
            return Err(McpClientRegistrationError::UnownedConflict { client });
        }
        self.connect(client, authority)
    }

    /// Removes only an exact entry backed by Market Squawk's receipt.
    pub fn disconnect(
        &self,
        client: McpClientKind,
    ) -> Result<McpClientStatus, McpClientRegistrationError> {
        let status = self.inspect(client)?;
        if status.state != McpClientState::Owned {
            return Err(McpClientRegistrationError::OwnershipRequired { client });
        }
        let program = status
            .executable
            .as_deref()
            .map(PathBuf::from)
            .ok_or(McpClientRegistrationError::ClientUnavailable { client })?;
        run_success(&remove_command(client, &program), client)?;
        if !matches!(observe(client, &program)?, ObservedRegistration::Missing) {
            return Err(McpClientRegistrationError::RegistrationVerification { client });
        }
        self.mutate_receipts(|document| document.remove(client))?;
        self.inspect(client)
    }

    /// Runs a real initialized MCP session through the exact installed relay.
    pub fn verify_protocol(
        &self,
        client: McpClientKind,
    ) -> Result<McpProtocolVerification, McpClientRegistrationError> {
        let status = self.inspect(client)?;
        if status.state != McpClientState::Owned {
            return Err(McpClientRegistrationError::OwnershipRequired { client });
        }
        let verification = protocol::verify(&self.relay_program, client)?;
        self.mutate_receipts(|document| {
            if let Some(receipt) = document.receipt_mut(client) {
                receipt.last_verification = Some(verification.clone());
            }
        })?;
        Ok(verification)
    }

    /// Verifies both named clients concurrently against one shared service authority.
    pub fn verify_concurrent_clients(
        &self,
    ) -> Result<[McpProtocolVerification; 2], McpClientRegistrationError> {
        let document = self.load_receipts()?;
        let claude = document.receipt(McpClientKind::ClaudeCode).ok_or(
            McpClientRegistrationError::OwnershipRequired {
                client: McpClientKind::ClaudeCode,
            },
        )?;
        let codex = document.receipt(McpClientKind::Codex).ok_or(
            McpClientRegistrationError::OwnershipRequired {
                client: McpClientKind::Codex,
            },
        )?;
        if claude.authority.runtime != codex.authority.runtime
            || claude.authority.endpoint_identity != codex.authority.endpoint_identity
            || claude.authority.credential_identity == codex.authority.credential_identity
        {
            return Err(McpClientRegistrationError::InvalidAuthorityIdentity);
        }
        thread::scope(|scope| {
            let claude = scope.spawn(|| self.verify_protocol(McpClientKind::ClaudeCode));
            let codex = scope.spawn(|| self.verify_protocol(McpClientKind::Codex));
            let claude = claude
                .join()
                .map_err(|_| McpClientRegistrationError::Protocol)??;
            let codex = codex
                .join()
                .map_err(|_| McpClientRegistrationError::Protocol)??;
            Ok([claude, codex])
        })
    }

    fn find_client_program(
        &self,
        client: McpClientKind,
    ) -> Result<Option<PathBuf>, McpClientRegistrationError> {
        for directory in &self.search_directories {
            for name in executable_names(client.executable_name()) {
                let candidate = directory.join(name);
                if candidate.is_file() {
                    return verify_executable(&candidate).map(Some);
                }
            }
        }
        Ok(None)
    }

    fn load_receipts(&self) -> Result<ReceiptDocument, McpClientRegistrationError> {
        let document = self
            .receipts
            .load()?
            .map(|encoded| serde_json::from_slice::<ReceiptDocument>(&encoded))
            .transpose()
            .map_err(|_error| McpClientRegistrationError::InvalidReceipt)?
            .unwrap_or_default();
        document.validate()
    }

    fn mutate_receipts(
        &self,
        mutation: impl FnOnce(&mut ReceiptDocument),
    ) -> Result<(), McpClientRegistrationError> {
        let _guard = self
            .mutation_gate
            .lock()
            .map_err(|_| McpClientRegistrationError::ReceiptMutation)?;
        let mut document = self.load_receipts()?;
        mutation(&mut document);
        let document = document.validate()?;
        let encoded = serde_json::to_vec(&document)
            .map_err(|_error| McpClientRegistrationError::ReceiptEncoding)?;
        self.receipts.store(&encoded)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ReceiptDocument {
    format_version: u16,
    receipts: Vec<McpClientRegistrationReceipt>,
}

impl Default for ReceiptDocument {
    fn default() -> Self {
        Self {
            format_version: RECEIPT_FORMAT_VERSION,
            receipts: Vec::new(),
        }
    }
}

impl ReceiptDocument {
    fn validate(self) -> Result<Self, McpClientRegistrationError> {
        let unique = self
            .receipts
            .iter()
            .map(|receipt| receipt.client)
            .collect::<BTreeSet<_>>();
        if self.format_version != RECEIPT_FORMAT_VERSION
            || self.receipts.len() > 2
            || unique.len() != self.receipts.len()
        {
            return Err(McpClientRegistrationError::InvalidReceipt);
        }
        for receipt in &self.receipts {
            let registration = McpClientRegistration {
                command: receipt.command.clone(),
                arguments: receipt.arguments.clone(),
            };
            if receipt.receipt_version != RECEIPT_FORMAT_VERSION
                || receipt.server_name != SERVER_NAME
                || receipt.client_version.is_empty()
                || receipt.client_version.len() > 256
                || receipt.command_sha256 != registration.digest()?
                || receipt.arguments
                    != [
                        "--client".to_owned(),
                        receipt.client.relay_argument().to_owned(),
                    ]
                || !valid_identity(&receipt.authority.endpoint_identity)
                || !valid_identity(&receipt.authority.credential_identity)
                || receipt
                    .last_verification
                    .as_ref()
                    .is_some_and(|verification| {
                        verification.client() != receipt.client || !verification.is_valid()
                    })
            {
                return Err(McpClientRegistrationError::InvalidReceipt);
            }
        }
        Ok(self)
    }

    fn receipt(&self, client: McpClientKind) -> Option<&McpClientRegistrationReceipt> {
        self.receipts
            .iter()
            .find(|receipt| receipt.client == client)
    }

    fn receipt_mut(&mut self, client: McpClientKind) -> Option<&mut McpClientRegistrationReceipt> {
        self.receipts
            .iter_mut()
            .find(|receipt| receipt.client == client)
    }

    fn upsert(&mut self, receipt: McpClientRegistrationReceipt) {
        self.remove(receipt.client);
        self.receipts.push(receipt);
        self.receipts.sort_by_key(|receipt| receipt.client);
    }

    fn remove(&mut self, client: McpClientKind) {
        self.receipts.retain(|receipt| receipt.client != client);
    }
}

#[derive(Debug)]
pub(super) enum ObservedRegistration {
    Missing,
    Present {
        transport: String,
        command: String,
        arguments: Vec<String>,
        has_environment: bool,
    },
}

#[derive(Debug)]
pub(super) struct McpCommandSpec {
    program: PathBuf,
    arguments: Vec<OsString>,
}

impl McpCommandSpec {
    fn new<I, S>(program: &Path, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Self {
            program: program.to_path_buf(),
            arguments: arguments
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string())
                .collect(),
        }
    }

    fn push(&mut self, argument: impl AsRef<OsStr>) {
        self.arguments.push(argument.as_ref().to_os_string());
    }

    fn extend(&mut self, arguments: &[String]) {
        self.arguments.extend(arguments.iter().map(OsString::from));
    }
}

struct McpCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct CommandChildGuard {
    child: Child,
    armed: bool,
}

impl CommandChildGuard {
    const fn new(child: Child) -> Self {
        Self { child, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl std::ops::Deref for CommandChildGuard {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl std::ops::DerefMut for CommandChildGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for CommandChildGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn run_checked(command: &McpCommandSpec) -> Result<McpCommandOutput, McpClientRegistrationError> {
    let mut child = CommandChildGuard::new(
        Command::new(&command.program)
            .args(&command.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_error| McpClientRegistrationError::ClientLaunch)?,
    );
    let stdout = child
        .stdout
        .take()
        .ok_or(McpClientRegistrationError::ClientLaunch)?;
    let stderr = child
        .stderr
        .take()
        .ok_or(McpClientRegistrationError::ClientLaunch)?;
    let stdout = thread::spawn(move || read_bounded(stdout));
    let stderr = thread::spawn(move || read_bounded(stderr));
    let deadline = Instant::now()
        .checked_add(CLIENT_COMMAND_TIMEOUT)
        .ok_or(McpClientRegistrationError::ClientDeadline)?;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|_error| McpClientRegistrationError::ClientLaunch)?
        {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(McpClientRegistrationError::ClientDeadline);
        }
        thread::sleep(Duration::from_millis(20));
    };
    child.disarm();
    let stdout = stdout
        .join()
        .map_err(|_| McpClientRegistrationError::ClientOutput)??;
    let stderr = stderr
        .join()
        .map_err(|_| McpClientRegistrationError::ClientOutput)??;
    Ok(McpCommandOutput {
        status,
        stdout,
        stderr,
    })
}

fn run_success(
    command: &McpCommandSpec,
    client: McpClientKind,
) -> Result<McpCommandOutput, McpClientRegistrationError> {
    let output = run_checked(command)?;
    if !output.status.success() {
        return Err(McpClientRegistrationError::ClientCommand { client });
    }
    Ok(output)
}

fn read_bounded(reader: impl Read) -> Result<Vec<u8>, McpClientRegistrationError> {
    let mut bytes = Vec::new();
    reader
        .take(MAXIMUM_COMMAND_OUTPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_error| McpClientRegistrationError::ClientOutput)?;
    if u64::try_from(bytes.len()).map_or(true, |length| length > MAXIMUM_COMMAND_OUTPUT_BYTES) {
        return Err(McpClientRegistrationError::ClientOutput);
    }
    Ok(bytes)
}

fn observe(
    client: McpClientKind,
    program: &Path,
) -> Result<ObservedRegistration, McpClientRegistrationError> {
    let output = run_checked(&get_command(client, program))?;
    let stdout = String::from_utf8(output.stdout)
        .map_err(|_error| McpClientRegistrationError::ClientOutput)?;
    let stderr = String::from_utf8(output.stderr)
        .map_err(|_error| McpClientRegistrationError::ClientOutput)?;
    match client {
        McpClientKind::ClaudeCode => {
            claude::parse_registration(output.status.success(), &stdout, &stderr)
        }
        McpClientKind::Codex => {
            codex::parse_registration(output.status.success(), &stdout, &stderr)
        }
    }
}

fn version_command(client: McpClientKind, program: &Path) -> McpCommandSpec {
    match client {
        McpClientKind::ClaudeCode => claude::version_command(program),
        McpClientKind::Codex => codex::version_command(program),
    }
}

fn capability_command(client: McpClientKind, program: &Path) -> McpCommandSpec {
    match client {
        McpClientKind::ClaudeCode => claude::capability_command(program),
        McpClientKind::Codex => codex::capability_command(program),
    }
}

fn get_command(client: McpClientKind, program: &Path) -> McpCommandSpec {
    match client {
        McpClientKind::ClaudeCode => claude::get_command(program),
        McpClientKind::Codex => codex::get_command(program),
    }
}

fn add_command(
    client: McpClientKind,
    program: &Path,
    registration: &McpClientRegistration,
) -> McpCommandSpec {
    match client {
        McpClientKind::ClaudeCode => claude::add_command(program, registration),
        McpClientKind::Codex => codex::add_command(program, registration),
    }
}

fn remove_command(client: McpClientKind, program: &Path) -> McpCommandSpec {
    match client {
        McpClientKind::ClaudeCode => claude::remove_command(program),
        McpClientKind::Codex => codex::remove_command(program),
    }
}

fn supports(client: McpClientKind, version: &str, help: &str) -> bool {
    match client {
        McpClientKind::ClaudeCode => claude::supports(version, help),
        McpClientKind::Codex => codex::supports(version, help),
    }
}

fn output_text(output: &McpCommandOutput) -> String {
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.stderr.is_empty() {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    text.trim().to_owned()
}

fn receipt(
    client: McpClientKind,
    client_version: String,
    registration: McpClientRegistration,
    authority: McpRegistrationAuthority,
) -> Result<McpClientRegistrationReceipt, McpClientRegistrationError> {
    let command_sha256 = registration.digest()?;
    let observed_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| McpClientRegistrationError::Clock)?
        .as_secs();
    Ok(McpClientRegistrationReceipt {
        receipt_version: RECEIPT_FORMAT_VERSION,
        client,
        server_name: SERVER_NAME.to_owned(),
        client_version,
        command: registration.command,
        arguments: registration.arguments,
        command_sha256,
        authority,
        observed_at_unix_seconds,
        last_verification: None,
    })
}

fn status(
    client: McpClientKind,
    state: McpClientState,
    client_version: Option<String>,
    executable: Option<PathBuf>,
    blocker: Option<&'static str>,
) -> McpClientStatus {
    McpClientStatus {
        client,
        state,
        client_version,
        executable: executable.map(|path| path.to_string_lossy().into_owned()),
        owned_receipt: None,
        blocker,
    }
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
}

fn discovery_directories() -> Vec<PathBuf> {
    let mut directories = env::var_os("PATH")
        .map(|value| env::split_paths(&value).collect::<Vec<_>>())
        .unwrap_or_default();
    if let Some(home) = env::var_os("HOME") {
        directories.push(PathBuf::from(home).join(".local/bin"));
    }
    #[cfg(target_os = "macos")]
    {
        directories.push(PathBuf::from("/opt/homebrew/bin"));
        directories.push(PathBuf::from("/usr/local/bin"));
    }
    directories.retain(|directory| directory.is_absolute());
    let mut unique = BTreeSet::new();
    directories.retain(|directory| unique.insert(directory.clone()));
    directories
}

fn executable_names(name: &str) -> Vec<OsString> {
    #[cfg(target_os = "windows")]
    {
        let extensions = env::var_os("PATHEXT")
            .map(|value| {
                value
                    .to_string_lossy()
                    .split(';')
                    .filter(|extension| !extension.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![".EXE".to_owned(), ".CMD".to_owned(), ".BAT".to_owned()]);
        let mut names = vec![OsString::from(name)];
        names.extend(
            extensions
                .into_iter()
                .map(|extension| OsString::from(format!("{name}{extension}"))),
        );
        names
    }
    #[cfg(not(target_os = "windows"))]
    {
        vec![OsString::from(name)]
    }
}

fn verify_executable(path: &Path) -> Result<PathBuf, McpClientRegistrationError> {
    let canonical =
        fs::canonicalize(path).map_err(|_error| McpClientRegistrationError::UnsafeExecutable)?;
    let metadata =
        fs::metadata(&canonical).map_err(|_error| McpClientRegistrationError::UnsafeExecutable)?;
    if !metadata.is_file() {
        return Err(McpClientRegistrationError::UnsafeExecutable);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let mode = metadata.permissions().mode();
        let owner = metadata.uid();
        let current = rustix::process::geteuid().as_raw();
        if mode & 0o111 == 0
            || mode & 0o022 != 0
            || (owner != 0 && owner != current)
            || metadata.nlink() != 1
        {
            return Err(McpClientRegistrationError::UnsafeExecutable);
        }
    }
    Ok(canonical)
}

/// Client discovery, ownership, command, receipt, or verification failure.
#[derive(Debug, Error)]
pub enum McpClientRegistrationError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    ReceiptStore(#[from] LocalAuthorityStateStoreError),
    #[error("the installed MCP relay is absent or unsafe")]
    InvalidRelayProgram,
    #[error("an MCP client executable is unsafe")]
    UnsafeExecutable,
    #[error("MCP registration authority identity is invalid")]
    InvalidAuthorityIdentity,
    #[error("{client:?} is absent or does not support the required MCP commands")]
    ClientUnavailable { client: McpClientKind },
    #[error("{client:?} MCP registration inspection failed")]
    ClientInspection { client: McpClientKind },
    #[error("{client:?} rejected an official MCP management command")]
    ClientCommand { client: McpClientKind },
    #[error("{client:?} returned an invalid MCP registration contract")]
    InvalidClientOutput { client: McpClientKind },
    #[error("the MCP client process could not start")]
    ClientLaunch,
    #[error("the MCP client process exceeded its deadline")]
    ClientDeadline,
    #[error("the MCP client process returned invalid or excessive output")]
    ClientOutput,
    #[error("an unowned {client:?} registration already uses the market-squawk name")]
    UnownedConflict { client: McpClientKind },
    #[error("an exact owned receipt is required for {client:?}")]
    OwnershipRequired { client: McpClientKind },
    #[error("{client:?} did not reproduce the exact requested registration")]
    RegistrationVerification { client: McpClientKind },
    #[error("the MCP receipt is corrupt or incompatible")]
    InvalidReceipt,
    #[error("the MCP receipt could not be encoded")]
    ReceiptEncoding,
    #[error("the MCP receipt mutation boundary is unavailable")]
    ReceiptMutation,
    #[error("the system clock is before the Unix epoch")]
    Clock,
    #[error("the MCP protocol verification failed")]
    Protocol,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_client_observations_must_exactly_match_the_owned_relay()
    -> Result<(), Box<dyn std::error::Error>> {
        let relay = Path::new("/opt/market-squawk/bin/market-squawk-mcp-relay");
        let claude = claude::parse_registration(
            true,
            "market-squawk:\n  Scope: User config (available in all your projects)\n  Status: Connected\n  Type: stdio\n  Command: /opt/market-squawk/bin/market-squawk-mcp-relay\n  Args: --client claude\n  Environment:\n",
            "",
        )?;
        let codex = codex::parse_registration(
            true,
            r#"{"name":"market-squawk","enabled":true,"disabled_reason":null,"transport":{"type":"stdio","command":"/opt/market-squawk/bin/market-squawk-mcp-relay","args":["--client","codex"],"env":null,"env_vars":[],"cwd":null}}"#,
            "",
        )?;

        assert!(McpClientRegistration::try_new(relay, McpClientKind::ClaudeCode)?.matches(&claude));
        assert!(McpClientRegistration::try_new(relay, McpClientKind::Codex)?.matches(&codex));
        Ok(())
    }
}
