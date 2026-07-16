use std::{
    error::Error,
    fmt,
    path::{Path, PathBuf},
};

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
            data_dir: PathBuf::from(".market-squawk"),
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum JournalFileFormat {
    Current,
    Legacy,
}

impl JournalFileFormat {
    const fn extension(self) -> &'static str {
        match self {
            Self::Current => "msj",
            Self::Legacy => "mej",
        }
    }
}

#[derive(Debug)]
pub enum JournalSelectionError {
    Ambiguous {
        current: PathBuf,
        legacy: PathBuf,
    },
    SelectedFormatNotFound {
        format: JournalFileFormat,
        path: PathBuf,
    },
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl fmt::Display for JournalSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ambiguous { current, legacy } => write!(
                formatter,
                "journal selection is ambiguous because both {} and {} exist; choose one with \
                 --journal-format current or --journal-format legacy",
                current.display(),
                legacy.display()
            ),
            Self::SelectedFormatNotFound { format, path } => write!(
                formatter,
                "selected journal format {format:?} does not exist at {}",
                path.display()
            ),
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "failed to inspect journal {}: {source}",
                    path.display()
                )
            }
        }
    }
}

impl Error for JournalSelectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Ambiguous { .. } | Self::SelectedFormatNotFound { .. } => None,
        }
    }
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
    pub fn journal_write_file(&self, source: &str) -> PathBuf {
        self.journal_path(source, JournalFileFormat::Current)
    }

    pub fn journal_initialization_file(
        &self,
        source: &str,
    ) -> std::result::Result<Option<PathBuf>, JournalSelectionError> {
        let legacy = self.journal_path(source, JournalFileFormat::Legacy);
        let current = self.journal_write_file(source);
        let legacy_exists = legacy
            .try_exists()
            .map_err(|source| JournalSelectionError::Io {
                path: legacy,
                source,
            })?;
        let current_exists = current
            .try_exists()
            .map_err(|source| JournalSelectionError::Io {
                path: current.clone(),
                source,
            })?;
        Ok((!legacy_exists || current_exists).then_some(current))
    }

    pub fn select_journal_for_read(
        &self,
        source: &str,
        requested: Option<JournalFileFormat>,
    ) -> std::result::Result<PathBuf, JournalSelectionError> {
        let current = self.journal_path(source, JournalFileFormat::Current);
        let legacy = self.journal_path(source, JournalFileFormat::Legacy);
        if let Some(format) = requested {
            let path = self.journal_path(source, format);
            return if path
                .try_exists()
                .map_err(|source| JournalSelectionError::Io {
                    path: path.clone(),
                    source,
                })? {
                Ok(path)
            } else {
                Err(JournalSelectionError::SelectedFormatNotFound { format, path })
            };
        }

        let current_exists = current
            .try_exists()
            .map_err(|source| JournalSelectionError::Io {
                path: current.clone(),
                source,
            })?;
        let legacy_exists = legacy
            .try_exists()
            .map_err(|source| JournalSelectionError::Io {
                path: legacy.clone(),
                source,
            })?;
        match (current_exists, legacy_exists) {
            (true, true) => Err(JournalSelectionError::Ambiguous { current, legacy }),
            (true, false) | (false, false) => Ok(current),
            (false, true) => Ok(legacy),
        }
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

    fn journal_path(&self, source: &str, format: JournalFileFormat) -> PathBuf {
        self.journal_dir()
            .join(format!("{source}.{}", format.extension()))
    }
}
