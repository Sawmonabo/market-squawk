//! Durable starter-investment visibility preferences shared by every product client.

use std::{fmt, path::Path, sync::Mutex};

use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Backend-owned starter choices; these symbols grant no canonical identity or price authority.
pub(crate) const STARTER_MARKET_SYMBOLS: [&str; 9] = [
    "SPY", "QQQ", "DIA", "IWM", "VTI", "AAPL", "MSFT", "NVDA", "TSLA",
];

const AUTHORITY_DIRECTORY: &str = "market-collection-authority";
const FORMAT_VERSION: u16 = 1;
const MAXIMUM_DOCUMENT_BYTES: usize = 4096;

/// One saved visibility choice in the backend's declared display order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MarketCollectionChoice {
    pub(crate) symbol: String,
    pub(crate) kept: bool,
}

/// Exact durable preference revision, including removed starter choices.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MarketCollectionSnapshot {
    pub(crate) revision: u64,
    pub(crate) choices: Vec<MarketCollectionChoice>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MarketCollectionDocument {
    format_version: u16,
    revision: u64,
    choices: [MarketCollectionChoice; STARTER_MARKET_SYMBOLS.len()],
}

impl MarketCollectionDocument {
    fn initial() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            revision: 1,
            choices: STARTER_MARKET_SYMBOLS.map(|symbol| MarketCollectionChoice {
                symbol: symbol.to_owned(),
                kept: true,
            }),
        }
    }

    fn validate(&self) -> Result<(), MarketCollectionError> {
        if self.format_version != FORMAT_VERSION
            || self.revision == 0
            || self
                .choices
                .iter()
                .zip(STARTER_MARKET_SYMBOLS)
                .any(|(choice, symbol)| choice.symbol != symbol)
        {
            return Err(MarketCollectionError::CorruptState);
        }
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<Self, MarketCollectionError> {
        if bytes.is_empty() || bytes.len() > MAXIMUM_DOCUMENT_BYTES {
            return Err(MarketCollectionError::CorruptState);
        }
        let document: Self =
            serde_json::from_slice(bytes).map_err(|_| MarketCollectionError::CorruptState)?;
        document.validate()?;
        Ok(document)
    }

    fn encode(&self) -> Result<Vec<u8>, MarketCollectionError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| MarketCollectionError::Encoding)?;
        if bytes.is_empty() || bytes.len() > MAXIMUM_DOCUMENT_BYTES {
            return Err(MarketCollectionError::Encoding);
        }
        Ok(bytes)
    }

    fn snapshot(&self) -> MarketCollectionSnapshot {
        MarketCollectionSnapshot {
            revision: self.revision,
            choices: self.choices.to_vec(),
        }
    }
}

struct MarketCollectionState {
    document: MarketCollectionDocument,
    recovery_required: bool,
}

/// Sole workspace owner of starter visibility; it owns no financial observations or identities.
pub(crate) struct MarketCollectionAuthority {
    store: LocalAuthorityStateStore,
    state: Mutex<MarketCollectionState>,
}

impl MarketCollectionAuthority {
    /// Initializes defaults only when no durable document exists.
    pub(crate) fn try_open(control_root: &Path) -> Result<Self, MarketCollectionError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        let document = match store.load()? {
            Some(bytes) => MarketCollectionDocument::decode(&bytes)?,
            None => {
                let document = MarketCollectionDocument::initial();
                store.store(&document.encode()?)?;
                document
            }
        };
        Ok(Self::from_document(store, document))
    }

    /// Reads the last acknowledged durable preference, including an entirely removed collection.
    pub(crate) fn snapshot(&self) -> Result<MarketCollectionSnapshot, MarketCollectionError> {
        let state = self
            .state
            .lock()
            .map_err(|_| MarketCollectionError::Unavailable)?;
        ensure_recovered(&state)?;
        Ok(state.document.snapshot())
    }

    /// Commits one revision-checked choice before publishing it in memory.
    pub(crate) fn set_choice(
        &self,
        expected_revision: u64,
        symbol: &str,
        kept: bool,
    ) -> Result<MarketCollectionSnapshot, MarketCollectionError> {
        let index = STARTER_MARKET_SYMBOLS
            .iter()
            .position(|candidate| *candidate == symbol)
            .ok_or(MarketCollectionError::UnknownSymbol)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| MarketCollectionError::Unavailable)?;
        ensure_recovered(&state)?;
        if state.document.revision != expected_revision {
            return Err(MarketCollectionError::StaleRevision);
        }
        if state.document.choices[index].kept == kept {
            return Ok(state.document.snapshot());
        }
        let mut document = state.document.clone();
        document.revision = document
            .revision
            .checked_add(1)
            .ok_or(MarketCollectionError::RevisionExhausted)?;
        document.choices[index].kept = kept;
        let bytes = document.encode()?;
        if let Err(error) = self.store.store(&bytes) {
            // A slot may already contain the successor. Reopen through platform recovery before
            // exposing any state or accepting another mutation; never assume the write rolled back.
            state.recovery_required = true;
            return Err(MarketCollectionError::Persistence(error));
        }
        state.document = document;
        Ok(state.document.snapshot())
    }

    /// Retains complete canonical preference bytes for the existing Configuration backup owner.
    pub(crate) fn retain_workspace_backup(
        &self,
    ) -> Result<RetainedMarketCollectionBackup, MarketCollectionError> {
        let state = self
            .state
            .lock()
            .map_err(|_| MarketCollectionError::Unavailable)?;
        ensure_recovered(&state)?;
        let canonical_bytes = state.document.encode()?;
        Ok(RetainedMarketCollectionBackup {
            authority_revision_sha256: Sha256::digest(&canonical_bytes).into(),
            canonical_bytes,
        })
    }

    /// Refuses a backup lease after any durable preference change.
    pub(crate) fn revalidate_workspace_backup(
        &self,
        retained: &RetainedMarketCollectionBackup,
    ) -> Result<(), MarketCollectionError> {
        if self.retain_workspace_backup()? != *retained {
            return Err(MarketCollectionError::StateChanged);
        }
        Ok(())
    }

    /// Verifies that restore would not overwrite an existing collection document.
    pub(crate) fn ensure_workspace_backup_target_absent(
        control_root: &Path,
    ) -> Result<(), MarketCollectionError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        ensure_target_absent(&store)
    }

    /// Validates the complete canonical backup before any aggregate restore writes.
    pub(crate) fn validate_workspace_backup(bytes: &[u8]) -> Result<(), MarketCollectionError> {
        if MarketCollectionDocument::decode(bytes)?.encode()? != bytes {
            return Err(MarketCollectionError::CorruptState);
        }
        Ok(())
    }

    /// Restores the exact validated preference into a fresh target without reseeding defaults.
    pub(crate) fn restore_workspace_backup_absent(
        control_root: &Path,
        canonical_bytes: &[u8],
    ) -> Result<Self, MarketCollectionError> {
        Self::validate_workspace_backup(canonical_bytes)?;
        let document = MarketCollectionDocument::decode(canonical_bytes)?;
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        ensure_target_absent(&store)?;
        store.store(canonical_bytes)?;
        Ok(Self::from_document(store, document))
    }

    fn from_document(store: LocalAuthorityStateStore, document: MarketCollectionDocument) -> Self {
        Self {
            store,
            state: Mutex::new(MarketCollectionState {
                document,
                recovery_required: false,
            }),
        }
    }
}

impl fmt::Debug for MarketCollectionAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MarketCollectionAuthority([STARTER VISIBILITY PREFERENCES])")
    }
}

/// Canonical Configuration-component payload with its complete revision identity.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RetainedMarketCollectionBackup {
    canonical_bytes: Vec<u8>,
    authority_revision_sha256: [u8; 32],
}

impl RetainedMarketCollectionBackup {
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub(crate) const fn authority_revision_sha256(&self) -> [u8; 32] {
        self.authority_revision_sha256
    }
}

fn ensure_recovered(state: &MarketCollectionState) -> Result<(), MarketCollectionError> {
    if state.recovery_required {
        Err(MarketCollectionError::RecoveryRequired)
    } else {
        Ok(())
    }
}

fn ensure_target_absent(store: &LocalAuthorityStateStore) -> Result<(), MarketCollectionError> {
    if store.load()?.is_some() {
        Err(MarketCollectionError::RestoreTargetOccupied)
    } else {
        Ok(())
    }
}

/// Closed preference-validation and durable-recovery failures.
#[derive(Debug, Error)]
pub(crate) enum MarketCollectionError {
    #[error("starter investment choice is unknown")]
    UnknownSymbol,
    #[error("market collection revision is stale")]
    StaleRevision,
    #[error("market collection revision space is exhausted")]
    RevisionExhausted,
    #[error("market collection durable state is invalid")]
    CorruptState,
    #[error("market collection authority is unavailable")]
    Unavailable,
    #[error("market collection persistence recovery is required")]
    RecoveryRequired,
    #[error("market collection encoding failed")]
    Encoding,
    #[error("market collection changed after backup retention")]
    StateChanged,
    #[error("market collection restore target is not empty")]
    RestoreTargetOccupied,
    #[error("market collection persistence failed")]
    Persistence(#[from] LocalAuthorityStateStoreError),
}
