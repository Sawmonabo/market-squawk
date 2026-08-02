//! Claude Code's official user-scoped MCP command surface.

use std::path::Path;

use super::{
    McpClientKind, McpClientRegistration, McpClientRegistrationError, McpCommandSpec,
    ObservedRegistration, SERVER_NAME,
};

pub(super) const EXECUTABLE_NAME: &str = "claude";

pub(super) fn version_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["--version"])
}

pub(super) fn capability_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["mcp", "add", "--help"])
}

pub(super) fn get_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["mcp", "get", SERVER_NAME])
}

pub(super) fn add_command(program: &Path, registration: &McpClientRegistration) -> McpCommandSpec {
    let mut command = McpCommandSpec::new(
        program,
        [
            "mcp",
            "add",
            "--transport",
            "stdio",
            "--scope",
            "user",
            SERVER_NAME,
            "--",
        ],
    );
    command.push(registration.command());
    command.extend(registration.arguments());
    command
}

pub(super) fn remove_command(program: &Path) -> McpCommandSpec {
    McpCommandSpec::new(program, ["mcp", "remove", "--scope", "user", SERVER_NAME])
}

pub(super) fn supports(version: &str, help: &str) -> bool {
    version.contains("Claude Code")
        && help.contains("--scope")
        && help.contains("--transport")
        && help.contains("stdio")
}

pub(super) fn parse_registration(
    success: bool,
    stdout: &str,
    stderr: &str,
) -> Result<ObservedRegistration, McpClientRegistrationError> {
    let combined = combined_output(stdout, stderr);
    if !success {
        if combined.contains("No MCP server named") {
            return Ok(ObservedRegistration::Missing);
        }
        return Err(McpClientRegistrationError::ClientInspection {
            client: McpClientKind::ClaudeCode,
        });
    }
    let kind =
        field(&combined, "Type:").ok_or(McpClientRegistrationError::InvalidClientOutput {
            client: McpClientKind::ClaudeCode,
        })?;
    let command =
        field(&combined, "Command:").ok_or(McpClientRegistrationError::InvalidClientOutput {
            client: McpClientKind::ClaudeCode,
        })?;
    let arguments = field(&combined, "Args:")
        .map(|value| value.split_whitespace().map(str::to_owned).collect())
        .unwrap_or_default();
    let environment = field(&combined, "Environment:").unwrap_or_default();
    Ok(ObservedRegistration::Present {
        transport: kind.to_owned(),
        command: command.to_owned(),
        arguments,
        has_environment: !environment.is_empty(),
    })
}

fn field<'a>(output: &'a str, label: &str) -> Option<&'a str> {
    output.lines().find_map(|line| {
        let line = line.trim();
        line.strip_prefix(label).map(str::trim)
    })
}

fn combined_output(stdout: &str, stderr: &str) -> String {
    let mut combined = String::with_capacity(stdout.len().saturating_add(stderr.len() + 1));
    combined.push_str(stdout);
    combined.push('\n');
    combined.push_str(stderr);
    combined
}
