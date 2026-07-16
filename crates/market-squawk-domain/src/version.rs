use std::fmt;
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

/// A validated version of a serialized Market Squawk schema.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SchemaVersion(NonZeroU16);

impl SchemaVersion {
    /// The schema version written by this release.
    pub const CURRENT: Self = Self(NonZeroU16::MIN);

    /// Creates a validated schema version.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaVersionError::Zero`] when `value` is zero.
    pub fn new(value: u16) -> Result<Self, SchemaVersionError> {
        NonZeroU16::new(value)
            .map(Self)
            .ok_or(SchemaVersionError::Zero)
    }

    /// Returns the version as a primitive integer.
    pub const fn get(self) -> u16 {
        self.0.get()
    }

    /// Confirms that this release can read the schema version.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaVersionError::UnsupportedFuture`] while preserving the version found in
    /// serialized input when it is newer than [`Self::CURRENT`].
    pub fn ensure_supported(self) -> Result<Self, SchemaVersionError> {
        if self == Self::CURRENT {
            Ok(self)
        } else {
            Err(SchemaVersionError::UnsupportedFuture {
                found: self,
                current: Self::CURRENT,
            })
        }
    }
}

/// A schema version invariant or compatibility failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaVersionError {
    /// Schema version zero is reserved and invalid.
    Zero,
    /// Serialized input uses a schema newer than this release supports.
    UnsupportedFuture {
        /// The version retained from serialized input.
        found: SchemaVersion,
        /// The newest version supported by this release.
        current: SchemaVersion,
    },
}

impl fmt::Display for SchemaVersionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("schema version must be nonzero"),
            Self::UnsupportedFuture { found, current } => write!(
                formatter,
                "schema version {} is newer than supported version {}",
                found.get(),
                current.get()
            ),
        }
    }
}

impl std::error::Error for SchemaVersionError {}
