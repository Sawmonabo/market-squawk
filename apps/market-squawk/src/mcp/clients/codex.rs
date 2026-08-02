//! Codex's official global MCP command surface.

use std::path::Path;

use serde::Deserialize;

use super::{
    McpClientKind, McpClientRegistration, McpClientRegistrationError, McpCommandSpec,
    ObservedRegistration, SERVER_NAME,
};

pub(super) const EXECUTABLE_NAME: &str = "codex";

pub(super) fn version_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["--version"])
}

pub(super) fn capability_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["mcp", "add", "--help"])
}

pub(super) fn get_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["mcp", "get", SERVER_NAME, "--json"])
}

pub(super) fn add_command(program: &Path, registration: &McpClientRegistration) -> McpCommandSpec {
    let mut command = McpCommandSpec::new(program, ["mcp", "add", SERVER_NAME, "--"]);
    command.push(registration.command());
    command.extend(registration.arguments());
    command
}

pub(super) fn remove_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["mcp", "remove", SERVER_NAME])
}

pub(super) fn supports(version: &str, help: &str) -> bool {
    version.contains("codex-cli") && help.contains("-- <COMMAND>") && !help.contains("--scope")
}

pub(super) fn parse_registration(
    success: bool,
    stdout: &str,
    stderr: &str,
) -> Result<ObservedRegistration, McpClientRegistrationError> {
    if !success {
        if stdout.contains("No MCP server named") || stderr.contains("No MCP server named") {
            return Ok(ObservedRegistration::Missing);
        }
        return Err(McpClientRegistrationError::ClientInspection {
            client: McpClientKind::Codex,
        });
    }
    let document = serde_json::from_str::<CodexRegistration>(stdout).map_err(|_error| {
        McpClientRegistrationError::InvalidClientOutput {
            client: McpClientKind::Codex,
        }
    })?;
    if document.name != SERVER_NAME || !document.enabled {
        return Err(McpClientRegistrationError::InvalidClientOutput {
            client: McpClientKind::Codex,
        });
    }
    match document.transport {
        CodexTransport::Stdio {
            command,
            args,
            env,
            env_vars,
            cwd,
        } => Ok(ObservedRegistration::Present {
            transport: "stdio".to_owned(),
            command,
            arguments: args,
            has_environment: env.is_some_and(|values| !values.is_empty())
                || !env_vars.is_empty()
                || cwd.is_some(),
        }),
        CodexTransport::Other => Ok(ObservedRegistration::Present {
            transport: "other".to_owned(),
            command: String::new(),
            arguments: Vec::new(),
            has_environment: false,
        }),
    }
}

#[derive(Debug, Deserialize)]
struct CodexRegistration {
    name: String,
    enabled: bool,
    transport: CodexTransport,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexTransport {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        env: Option<std::collections::BTreeMap<String, String>>,
        #[serde(default)]
        env_vars: Vec<String>,
        cwd: Option<String>,
    },
    #[serde(other)]
    Other,
}
