//! Durable program-generation journal for installed trusted updates.

use std::{fmt, path::Path, sync::Mutex};

use market_squawk_platform::LocalAuthorityStateStore;
use serde::{Deserialize, Serialize};

use crate::application::lifecycle::{
    ProgramGeneration, UpdateError, UpdateJournal, UpdateTransitionRecord,
};

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "installed-update-journal";
const MAXIMUM_TRANSITIONS: usize = 2_048;

/// Exclusive persisted owner of the monotonic installed program generation.
pub(crate) struct DurableUpdateJournal {
    store: LocalAuthorityStateStore,
    document: Mutex<UpdateJournalDocument>,
}

impl DurableUpdateJournal {
    /// Opens or initializes the journal and revalidates the complete generation chain.
    pub(crate) fn try_open(control_root: &Path) -> Result<Self, UpdateError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))
            .map_err(|_| UpdateError::JournalUnavailable)?;
        let document = match store.load().map_err(|_| UpdateError::JournalUnavailable)? {
            Some(bytes) => serde_json::from_slice::<UpdateJournalDocument>(&bytes)
                .map_err(|_| UpdateError::JournalUnavailable)?
                .validate()?,
            None => {
                let document = UpdateJournalDocument::initial()?;
                store_document(&store, &document)?;
                document
            }
        };
        Ok(Self {
            store,
            document: Mutex::new(document),
        })
    }

    /// Returns the only active generation proven by the complete durable chain.
    pub(crate) fn current_generation(&self) -> Result<ProgramGeneration, UpdateError> {
        self.document
            .lock()
            .map(|document| document.current_generation)
            .map_err(|_| UpdateError::JournalUnavailable)
    }
}

impl UpdateJournal for DurableUpdateJournal {
    fn append(&self, record: &UpdateTransitionRecord) -> Result<(), UpdateError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| UpdateError::JournalUnavailable)?;
        if document.transitions.len() >= MAXIMUM_TRANSITIONS {
            return Err(UpdateError::JournalUnavailable);
        }
        let next = record.validate_after(document.current_generation)?;
        let mut candidate = document.clone();
        candidate.transitions.push(record.clone());
        candidate.current_generation = next;
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(())
    }
}

impl fmt::Debug for DurableUpdateJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurableUpdateJournal([MONOTONIC PROGRAM AUTHORITY])")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct UpdateJournalDocument {
    format_version: u16,
    initial_generation: ProgramGeneration,
    current_generation: ProgramGeneration,
    transitions: Vec<UpdateTransitionRecord>,
}

impl UpdateJournalDocument {
    fn initial() -> Result<Self, UpdateError> {
        let generation = ProgramGeneration::try_new(1)?;
        Ok(Self {
            format_version: FORMAT_VERSION,
            initial_generation: generation,
            current_generation: generation,
            transitions: Vec::new(),
        })
    }

    fn validate(self) -> Result<Self, UpdateError> {
        if self.format_version != FORMAT_VERSION || self.transitions.len() > MAXIMUM_TRANSITIONS {
            return Err(UpdateError::JournalUnavailable);
        }
        let mut current = self.initial_generation;
        for transition in &self.transitions {
            current = transition.validate_after(current)?;
        }
        if current != self.current_generation {
            return Err(UpdateError::JournalUnavailable);
        }
        Ok(self)
    }
}

fn store_document(
    store: &LocalAuthorityStateStore,
    document: &UpdateJournalDocument,
) -> Result<(), UpdateError> {
    let encoded = serde_json::to_vec(document).map_err(|_| UpdateError::JournalUnavailable)?;
    if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
        return Err(UpdateError::JournalUnavailable);
    }
    store
        .store(&encoded)
        .map_err(|_| UpdateError::JournalUnavailable)
}
