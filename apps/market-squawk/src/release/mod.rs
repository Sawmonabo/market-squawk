//! Exact-head release evidence and deterministic local demonstration.

mod close;
mod fuzz;
mod identity;
mod io;
mod process;

use anyhow::Result;
use serde_json::Value;

use crate::cli::{ReleaseCommand, ReleaseEvidenceCommand};
use crate::AppConfig;

/// Executes one production release-evidence operation outside the live event path.
pub async fn execute_release_command(config: AppConfig, command: ReleaseCommand) -> Result<Value> {
    match command {
        ReleaseCommand::Evidence {
            command: ReleaseEvidenceCommand::Fuzz(arguments),
        } => fuzz::run(arguments),
        ReleaseCommand::Evidence {
            command: ReleaseEvidenceCommand::Close(arguments),
        } => close::run(arguments),
        ReleaseCommand::Evidence {
            command: ReleaseEvidenceCommand::Benchmark(_) | ReleaseEvidenceCommand::Providers(_),
        }
        | ReleaseCommand::Demonstrate(_) => {
            drop(config);
            anyhow::bail!("selected release operation is not implemented")
        }
    }
}
