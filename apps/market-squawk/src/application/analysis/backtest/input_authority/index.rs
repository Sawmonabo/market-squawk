//! Canonical bounded persistence for immutable governed-backtest input recipes.

use std::fmt;

use market_squawk_domain::SourceIdentifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::application::domain_support::encode_hex;

const INPUT_INDEX_SCHEMA_VERSION: u16 = 1;
const INPUT_ID_PREFIX: &str = "backtest-input-";

#[derive(Clone, Copy, Debug)]
pub(super) struct InputIndexLimits {
    pub(super) maximum_inputs: usize,
    pub(super) maximum_index_bytes: usize,
}

impl InputIndexLimits {
    fn valid(self) -> bool {
        self.maximum_inputs > 0 && self.maximum_index_bytes > 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InputInsertDisposition {
    Added,
    Replay,
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct StoredInputRecipe {
    input_id: SourceIdentifier,
    recipe_digest: [u8; 32],
    recipe_json: String,
}

impl StoredInputRecipe {
    pub(super) fn try_new(
        recipe: Vec<u8>,
        limits: InputIndexLimits,
    ) -> Result<Self, InputIndexError> {
        if !limits.valid() || recipe.is_empty() || recipe.len() > limits.maximum_index_bytes {
            return Err(InputIndexError::ResourceExhausted);
        }
        let value: serde_json::Value =
            serde_json::from_slice(&recipe).map_err(|_| InputIndexError::Corrupt)?;
        if !value.is_object()
            || serde_json::to_vec(&value).map_err(|_| InputIndexError::Corrupt)? != recipe
        {
            return Err(InputIndexError::Corrupt);
        }
        let recipe_digest: [u8; 32] = Sha256::digest(&recipe).into();
        let input_id = input_id(recipe_digest)?;
        let recipe_json = String::from_utf8(recipe).map_err(|_| InputIndexError::Corrupt)?;
        Ok(Self {
            input_id,
            recipe_digest,
            recipe_json,
        })
    }

    fn from_wire(wire: InputEntryWire) -> Result<Self, InputIndexError> {
        let recipe_digest = decode_hex(&wire.recipe_digest)?;
        let expected = input_id(recipe_digest)?;
        if wire.input_id != expected
            || Sha256::digest(wire.recipe.as_bytes()).as_slice() != recipe_digest
        {
            return Err(InputIndexError::Corrupt);
        }
        let value: serde_json::Value =
            serde_json::from_str(&wire.recipe).map_err(|_| InputIndexError::Corrupt)?;
        if !value.is_object()
            || serde_json::to_string(&value).map_err(|_| InputIndexError::Corrupt)? != wire.recipe
        {
            return Err(InputIndexError::Corrupt);
        }
        Ok(Self {
            input_id: wire.input_id,
            recipe_digest,
            recipe_json: wire.recipe,
        })
    }

    pub(super) const fn input_id(&self) -> &SourceIdentifier {
        &self.input_id
    }

    pub(super) const fn recipe_digest(&self) -> [u8; 32] {
        self.recipe_digest
    }

    pub(super) fn recipe_bytes(&self) -> &[u8] {
        self.recipe_json.as_bytes()
    }
}

impl fmt::Debug for StoredInputRecipe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredInputRecipe")
            .field("input_id", &self.input_id)
            .field("recipe_digest", &"[SHA-256]")
            .field("recipe", &"[CANONICAL INPUT RECIPE]")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(super) struct InputIndex {
    entries: Vec<StoredInputRecipe>,
}

impl InputIndex {
    pub(super) const fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub(super) fn decode(bytes: &[u8], limits: InputIndexLimits) -> Result<Self, InputIndexError> {
        if !limits.valid() || bytes.len() > limits.maximum_index_bytes {
            return Err(InputIndexError::Corrupt);
        }
        let wire: InputIndexWire =
            serde_json::from_slice(bytes).map_err(|_| InputIndexError::Corrupt)?;
        if wire.schema_version != INPUT_INDEX_SCHEMA_VERSION
            || wire.entries.len() > limits.maximum_inputs
        {
            return Err(InputIndexError::Corrupt);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(wire.entries.len())
            .map_err(|_| InputIndexError::ResourceExhausted)?;
        for entry in wire.entries {
            entries.push(StoredInputRecipe::from_wire(entry)?);
        }
        if entries
            .windows(2)
            .any(|pair| pair[0].input_id >= pair[1].input_id)
        {
            return Err(InputIndexError::Corrupt);
        }
        let index = Self { entries };
        if index.encode(limits)? != bytes {
            return Err(InputIndexError::Corrupt);
        }
        Ok(index)
    }

    pub(super) fn encode(&self, limits: InputIndexLimits) -> Result<Vec<u8>, InputIndexError> {
        if !limits.valid() || self.entries.len() > limits.maximum_inputs {
            return Err(InputIndexError::ResourceExhausted);
        }
        let entries = self
            .entries
            .iter()
            .map(|entry| InputEntryView {
                input_id: entry.input_id(),
                recipe_digest: encode_hex(entry.recipe_digest()),
                recipe: &entry.recipe_json,
            })
            .collect::<Vec<_>>();
        let encoded = serde_json::to_vec(&InputIndexView {
            schema_version: INPUT_INDEX_SCHEMA_VERSION,
            entries,
        })
        .map_err(|_| InputIndexError::Corrupt)?;
        if encoded.len() > limits.maximum_index_bytes {
            return Err(InputIndexError::ResourceExhausted);
        }
        Ok(encoded)
    }

    pub(super) fn insert(
        &mut self,
        entry: StoredInputRecipe,
        limits: InputIndexLimits,
    ) -> Result<InputInsertDisposition, InputIndexError> {
        match self
            .entries
            .binary_search_by(|candidate| candidate.input_id.cmp(&entry.input_id))
        {
            Ok(position) => {
                return if self.entries.get(position) == Some(&entry) {
                    Ok(InputInsertDisposition::Replay)
                } else {
                    Err(InputIndexError::Conflict)
                };
            }
            Err(_) if self.entries.len() >= limits.maximum_inputs => {
                return Err(InputIndexError::ResourceExhausted);
            }
            Err(position) => {
                if self
                    .entries
                    .iter()
                    .any(|candidate| candidate.recipe_digest == entry.recipe_digest)
                {
                    return Err(InputIndexError::Conflict);
                }
                self.entries
                    .try_reserve_exact(1)
                    .map_err(|_| InputIndexError::ResourceExhausted)?;
                self.entries.insert(position, entry);
                if let Err(error) = self.encode(limits) {
                    self.entries.remove(position);
                    return Err(error);
                }
            }
        }
        Ok(InputInsertDisposition::Added)
    }

    pub(super) fn get(&self, input_id: &SourceIdentifier) -> Option<StoredInputRecipe> {
        self.entries
            .binary_search_by(|candidate| candidate.input_id.cmp(input_id))
            .ok()
            .and_then(|position| self.entries.get(position))
            .cloned()
    }

    pub(super) fn entries(&self) -> &[StoredInputRecipe] {
        &self.entries
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InputIndexView<'a> {
    schema_version: u16,
    entries: Vec<InputEntryView<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InputEntryView<'a> {
    input_id: &'a SourceIdentifier,
    recipe_digest: String,
    recipe: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InputIndexWire {
    schema_version: u16,
    entries: Vec<InputEntryWire>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InputEntryWire {
    input_id: SourceIdentifier,
    recipe_digest: String,
    recipe: String,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum InputIndexError {
    #[error("governed-backtest input index is corrupt")]
    Corrupt,
    #[error("governed-backtest input identity conflicts")]
    Conflict,
    #[error("governed-backtest input index exceeded its resource contract")]
    ResourceExhausted,
}

fn input_id(digest: [u8; 32]) -> Result<SourceIdentifier, InputIndexError> {
    SourceIdentifier::try_from(format!("{INPUT_ID_PREFIX}{}", encode_hex(digest)))
        .map_err(|_| InputIndexError::Corrupt)
}

fn decode_hex(value: &str) -> Result<[u8; 32], InputIndexError> {
    if value.len() != 64 {
        return Err(InputIndexError::Corrupt);
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = nibble(pair[0]).ok_or(InputIndexError::Corrupt)?;
        let low = nibble(pair[1]).ok_or(InputIndexError::Corrupt)?;
        decoded[index] = (high << 4) | low;
    }
    Ok(decoded)
}

fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{InputIndex, InputIndexLimits, InputInsertDisposition, StoredInputRecipe};

    #[test]
    fn input_index_restarts_canonically_replays_exactly_and_rejects_tampering()
    -> Result<(), Box<dyn Error>> {
        let limits = InputIndexLimits {
            maximum_inputs: 4,
            maximum_index_bytes: 4_096,
        };
        let recipe = br#"{"schemaVersion":1,"strategyId":"strategy-a"}"#.to_vec();
        let stored = StoredInputRecipe::try_new(recipe, limits)?;
        let mut index = InputIndex::empty();

        assert_eq!(
            index.insert(stored.clone(), limits)?,
            InputInsertDisposition::Added
        );
        assert_eq!(
            index.insert(stored, limits)?,
            InputInsertDisposition::Replay
        );

        let encoded = index.encode(limits)?;
        let restarted = InputIndex::decode(&encoded, limits)?;
        assert_eq!(restarted.encode(limits)?, encoded);

        let mut tampered = String::from_utf8(encoded)?;
        tampered = tampered.replace("strategy-a", "strategy-b");
        assert!(InputIndex::decode(tampered.as_bytes(), limits).is_err());
        Ok(())
    }
}
