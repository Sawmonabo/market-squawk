//! Exact-head release evidence and deterministic local demonstration.

#[cfg(feature = "release-evidence")]
mod benchmark;
mod close;
mod close_demonstration;
mod close_provider;
#[cfg(feature = "release-evidence")]
mod demonstrate;
mod fuzz;
mod identity;
mod io;
mod process;
mod providers;

use anyhow::Result;
use serde_json::Value;

use crate::AppConfig;
use crate::cli::{ReleaseCommand, ReleaseEvidenceCommand};

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
            command: ReleaseEvidenceCommand::Benchmark(arguments),
        } => {
            #[cfg(feature = "release-evidence")]
            {
                benchmark::run(config, arguments).await
            }
            #[cfg(not(feature = "release-evidence"))]
            {
                drop(config);
                drop(arguments);
                anyhow::bail!(
                    "release benchmark requires a build with the release-evidence feature"
                )
            }
        }
        ReleaseCommand::Evidence {
            command: ReleaseEvidenceCommand::BenchmarkWorker(arguments),
        } => {
            #[cfg(feature = "release-evidence")]
            {
                benchmark::run_worker(config, arguments).await
            }
            #[cfg(not(feature = "release-evidence"))]
            {
                drop(config);
                drop(arguments);
                anyhow::bail!(
                    "release benchmark worker requires a build with the release-evidence feature"
                )
            }
        }
        ReleaseCommand::Evidence {
            command: ReleaseEvidenceCommand::Providers(arguments),
        } => providers::run(config, arguments).await,
        ReleaseCommand::Demonstrate(arguments) => {
            #[cfg(feature = "release-evidence")]
            {
                demonstrate::run(config, arguments).await
            }
            #[cfg(not(feature = "release-evidence"))]
            {
                drop(config);
                drop(arguments);
                anyhow::bail!(
                    "release demonstration requires a build with the release-evidence feature"
                )
            }
        }
    }
}
