use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub products: Vec<String>,
    pub stale_after_ms: i64,
    pub journal_queue_capacity: usize,
    pub paper_bot_enabled: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(".market-engine"),
            products: vec!["BTC-USD".to_owned()],
            stale_after_ms: 5_000,
            journal_queue_capacity: 16_384,
            paper_bot_enabled: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppPaths {
    root: PathBuf,
}

impl AppPaths {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn journal_dir(&self) -> PathBuf {
        self.root.join("journal")
    }

    #[must_use]
    pub fn journal_file(&self, source: &str) -> PathBuf {
        self.journal_dir().join(format!("{source}.mej"))
    }

    #[must_use]
    pub fn state_file(&self) -> PathBuf {
        self.root.join("state.json")
    }

    pub fn initialize(&self) -> Result<()> {
        std::fs::create_dir_all(self.journal_dir()).with_context(|| {
            format!(
                "failed to create journal directory {}",
                self.journal_dir().display()
            )
        })?;
        Ok(())
    }
}
