use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use anyhow::{Context, Result, bail};
use market_squawk::{AppPaths, DiagnosticRawEnvelope};
use serde_json::Value;
use tempfile::tempdir;
use uuid::Uuid;

const SOURCE: &str = "coinbase-exchange";

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_market-squawk"))
}

fn command(data_dir: &Path) -> Command {
    let mut command = Command::new(binary());
    command.args([
        "--data-dir",
        &data_dir.display().to_string(),
        "--log",
        "error",
    ]);
    command
}

fn legacy_fixture(data_dir: &Path) -> Result<PathBuf> {
    let paths = AppPaths::prepare(data_dir)?;
    let current = paths.journal_write_file("fixture")?;
    let legacy = paths.journal_dir().join(format!("{SOURCE}.mej"));
    let mut writer = paths.open_journal_writer("fixture")?;
    writer.append(&DiagnosticRawEnvelope::try_from_compatibility_parts(
        Uuid::new_v4(),
        "fixture-source".to_owned(),
        Uuid::nil(),
        Some(1),
        None,
        chrono::Utc::now(),
        br#"{"fixture":true}"#.to_vec(),
    )?)?;
    writer.flush()?;
    drop(writer);

    let mut bytes = fs::read(&current)?;
    bytes
        .get_mut(..4)
        .context("fixture journal has no magic header")?
        .copy_from_slice(b"MEJ1");
    fs::write(&legacy, bytes)?;
    fs::remove_file(current)?;
    Ok(legacy)
}

fn current_fixture(data_dir: &Path) -> Result<PathBuf> {
    let paths = AppPaths::prepare(data_dir)?;
    let path = paths.journal_write_file(SOURCE)?;
    paths.open_journal_writer(SOURCE)?.flush()?;
    Ok(path)
}

fn assert_success(output: &Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    bail!(
        "command failed: status={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn init_does_not_shadow_a_legacy_journal_with_an_empty_current_journal() -> Result<()> {
    let directory = tempdir()?;
    let legacy = legacy_fixture(directory.path())?;

    let output = command(directory.path()).arg("init").output()?;

    assert_success(&output)?;
    assert!(legacy.is_file());
    assert!(
        !directory
            .path()
            .join("journal")
            .join(format!("{SOURCE}.msj"))
            .exists()
    );
    Ok(())
}

#[test]
fn replay_automatically_reads_the_sole_legacy_journal() -> Result<()> {
    let directory = tempdir()?;
    legacy_fixture(directory.path())?;

    let output = command(directory.path())
        .args(["replay", "--source", SOURCE])
        .output()?;

    assert_success(&output)?;
    let replay: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(replay["summary"]["records"], 1);
    assert!(
        !directory
            .path()
            .join("journal")
            .join(format!("{SOURCE}.msj"))
            .exists()
    );
    Ok(())
}

#[test]
fn replay_does_not_create_storage_while_reporting_a_missing_journal() -> Result<()> {
    let directory = tempdir()?;
    let data_dir = directory.path().join("not-created");

    let output = command(&data_dir)
        .args(["replay", "--source", SOURCE])
        .output()?;

    assert!(!output.status.success());
    assert!(!data_dir.exists());
    Ok(())
}

#[test]
fn replay_rejects_ambiguous_formats_until_the_user_chooses_one() -> Result<()> {
    let directory = tempdir()?;
    legacy_fixture(directory.path())?;
    current_fixture(directory.path())?;

    let ambiguous = command(directory.path())
        .args(["replay", "--source", SOURCE])
        .output()?;
    assert!(!ambiguous.status.success());
    let stderr = String::from_utf8_lossy(&ambiguous.stderr);
    assert!(stderr.contains("ambiguous"), "stderr={stderr}");
    assert!(stderr.contains("--journal-format"), "stderr={stderr}");

    let selected = command(directory.path())
        .args(["replay", "--source", SOURCE, "--journal-format", "legacy"])
        .output()?;
    assert_success(&selected)?;
    let replay: Value = serde_json::from_slice(&selected.stdout)?;
    assert_eq!(replay["summary"]["records"], 1);
    Ok(())
}

fn offline_mcp(data_dir: &Path, journal_format: Option<&str>) -> Result<Output> {
    let mut command = command(data_dir);
    command.args(["mcp", "--offline"]);
    if let Some(format) = journal_format {
        command.args(["--journal-format", format]);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let mut stdin = child.stdin.take().context("MCP stdin was not piped")?;
    stdin.write_all(
        br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"Journal.GetSummary","arguments":{}}}
"#,
    )?;
    drop(stdin);
    Ok(child.wait_with_output()?)
}

#[test]
fn offline_mcp_reads_the_sole_legacy_journal_without_creating_current_data() -> Result<()> {
    let directory = tempdir()?;
    legacy_fixture(directory.path())?;

    let output = offline_mcp(directory.path(), None)?;

    assert_success(&output)?;
    let response: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(response["result"]["structuredContent"]["records"], 1);
    assert!(
        !directory
            .path()
            .join("journal")
            .join(format!("{SOURCE}.msj"))
            .exists()
    );
    Ok(())
}

#[test]
fn offline_mcp_does_not_create_storage_when_no_journal_exists() -> Result<()> {
    let directory = tempdir()?;
    let data_dir = directory.path().join("not-created");

    let output = offline_mcp(&data_dir, None)?;

    assert_success(&output)?;
    assert!(!data_dir.exists());
    Ok(())
}

#[test]
fn offline_mcp_rejects_ambiguous_formats_before_serving_requests() -> Result<()> {
    let directory = tempdir()?;
    legacy_fixture(directory.path())?;
    current_fixture(directory.path())?;

    let output = offline_mcp(directory.path(), None)?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ambiguous"), "stderr={stderr}");
    assert!(stderr.contains("--journal-format"), "stderr={stderr}");
    Ok(())
}
