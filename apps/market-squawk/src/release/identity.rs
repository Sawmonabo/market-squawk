//! Clean repository identity retained across one evidence producer.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context as _, Result, anyhow, bail};
use serde::Serialize;

use crate::cli::ReleaseRepositoryArguments;

const MAXIMUM_GIT_OUTPUT_BYTES: usize = 1024 * 1024;

/// Immutable repository identity observed at producer admission.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryIdentity {
    pub(super) head: String,
    pub(super) tree: String,
    pub(super) clean: bool,
    #[serde(skip)]
    root: PathBuf,
}

impl RepositoryIdentity {
    pub(super) fn admit(arguments: &ReleaseRepositoryArguments) -> Result<Self> {
        if arguments.head.is_some() != arguments.tree.is_some() {
            bail!("release evidence requires --head and --tree together");
        }
        let root = repository_root()?;
        let head = git_line(&root, &["rev-parse", "--verify", "HEAD"])?;
        let tree = git_line(&root, &["rev-parse", "--verify", "HEAD^{tree}"])?;
        validate_object_id(&head)?;
        validate_object_id(&tree)?;
        let clean = git_bytes(
            &root,
            &["status", "--porcelain=v1", "--untracked-files=all"],
        )?
        .is_empty();
        if let (Some(expected_head), Some(expected_tree)) = (&arguments.head, &arguments.tree) {
            validate_object_id(expected_head)?;
            validate_object_id(expected_tree)?;
            if &head != expected_head || &tree != expected_tree {
                bail!("release evidence repository identity does not match --head/--tree");
            }
            if !clean {
                bail!("exact-head release evidence requires a clean repository");
            }
        }
        Ok(Self {
            head,
            tree,
            clean,
            root,
        })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn verify_unchanged(&self) -> Result<()> {
        let current_head = git_line(&self.root, &["rev-parse", "--verify", "HEAD"])?;
        let current_tree = git_line(&self.root, &["rev-parse", "--verify", "HEAD^{tree}"])?;
        let clean = git_bytes(
            &self.root,
            &["status", "--porcelain=v1", "--untracked-files=all"],
        )?
        .is_empty();
        if current_head != self.head || current_tree != self.tree || clean != self.clean {
            bail!("repository identity changed while release evidence was collected");
        }
        Ok(())
    }
}

fn repository_root() -> Result<PathBuf> {
    let current = std::env::current_dir().context("current directory is unavailable")?;
    let root = git_line(&current, &["rev-parse", "--show-toplevel"])?;
    let path = PathBuf::from(root);
    let canonical = path
        .canonicalize()
        .context("repository root is unavailable")?;
    if !canonical.is_dir() {
        bail!("repository root is not a directory");
    }
    Ok(canonical)
}

fn git_line(root: &Path, arguments: &[&str]) -> Result<String> {
    let output = git_bytes(root, arguments)?;
    if output.is_empty() || output.len() > 4096 || output.contains(&0) {
        bail!("Git returned an invalid repository identity");
    }
    let value = std::str::from_utf8(&output)
        .context("Git returned a non-UTF-8 repository identity")?
        .trim();
    if value.is_empty() || value.contains(['\r', '\n']) {
        bail!("Git returned an invalid single-line repository identity");
    }
    Ok(value.to_owned())
}

fn git_bytes(root: &Path, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .context("failed to execute Git")?;
    if !output.status.success() {
        let status = output
            .status
            .code()
            .map_or_else(|| "signal".to_owned(), |code| code.to_string());
        return Err(anyhow!("Git repository query failed with status {status}"));
    }
    if output.stdout.len() > MAXIMUM_GIT_OUTPUT_BYTES
        || output.stderr.len() > MAXIMUM_GIT_OUTPUT_BYTES
    {
        bail!("Git repository query exceeded its output bound");
    }
    Ok(output.stdout)
}

fn validate_object_id(value: &str) -> Result<()> {
    if !matches!(value.len(), 40 | 64)
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        bail!("release evidence contains an invalid Git object identity");
    }
    Ok(())
}
