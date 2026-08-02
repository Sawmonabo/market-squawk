use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// A failure to construct a validated internal identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityError {
    /// A required identifier was empty or contained only whitespace.
    Empty,
    /// An identifier exceeded its documented UTF-8 byte bound.
    TooLong {
        /// The maximum permitted UTF-8 byte length.
        max: usize,
    },
    /// An identifier contained whitespace or a control character.
    InvalidCharacter,
    /// The nil UUID does not identify an instrument.
    NilUuid,
    /// Text did not contain a UUID.
    InvalidUuid,
    /// A monotonically increasing counter cannot advance beyond `u64::MAX`.
    CounterOverflow,
    /// Connection generation zero is reserved for the absence of a connection.
    ZeroGeneration,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("identifier must not be empty"),
            Self::TooLong { max } => write!(
                formatter,
                "identifier exceeds the maximum length of {max} UTF-8 bytes"
            ),
            Self::InvalidCharacter => {
                formatter.write_str("identifier must not contain whitespace or control characters")
            }
            Self::NilUuid => formatter.write_str("instrument UUID must not be nil"),
            Self::InvalidUuid => formatter.write_str("instrument identifier is not a UUID"),
            Self::CounterOverflow => formatter.write_str("identity counter overflow"),
            Self::ZeroGeneration => formatter.write_str("connection generation must be nonzero"),
        }
    }
}

impl std::error::Error for IdentityError {}

/// A stable internal instrument identity independent of provider symbols.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstrumentId(Uuid);

impl InstrumentId {
    /// Returns the underlying non-nil UUID by value.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl TryFrom<Uuid> for InstrumentId {
    type Error = IdentityError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        if value.is_nil() {
            Err(IdentityError::NilUuid)
        } else {
            Ok(Self(value))
        }
    }
}

impl FromStr for InstrumentId {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value)
            .map_err(|_| IdentityError::InvalidUuid)
            .and_then(Self::try_from)
    }
}

impl fmt::Display for InstrumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for InstrumentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for InstrumentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

fn validate_bounded_identity(value: &str, max: usize) -> Result<(), IdentityError> {
    if value.is_empty() || value.chars().all(char::is_whitespace) {
        return Err(IdentityError::Empty);
    }
    if value.len() > max {
        return Err(IdentityError::TooLong { max });
    }
    if value
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(IdentityError::InvalidCharacter);
    }
    Ok(())
}

macro_rules! bounded_identity {
    ($(#[$metadata:meta])* $name:ident, $max:expr) => {
        $(#[$metadata])*
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// The maximum encoded length in UTF-8 bytes.
            pub const MAX_LENGTH: usize = $max;

            /// Returns the validated identifier as a borrowed string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Returns bytes retained by the identifier's owned string allocation.
            ///
            /// This can exceed [`Self::as_str`] length when construction retained spare capacity.
            pub fn retained_bytes(&self) -> usize {
                self.0.capacity()
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdentityError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                validate_bounded_identity(value, Self::MAX_LENGTH)?;
                Ok(Self(value.to_owned()))
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentityError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                validate_bounded_identity(&value, Self::MAX_LENGTH)?;
                Ok(Self(value))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::try_from(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

bounded_identity!(
    /// A bounded internal venue identifier.
    VenueId,
    64
);

bounded_identity!(
    /// A bounded internal data-source identifier.
    SourceId,
    128
);

bounded_identity!(
    /// An instrument identifier in a provider's namespace.
    ProviderInstrumentId,
    256
);

bounded_identity!(
    /// A bounded source-defined record or stream identifier retained for provenance.
    SourceIdentifier,
    512
);

/// A source sequence value. Zero remains valid because provider sequence domains differ.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// Constructs a sequence value without imposing a provider-specific starting offset.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the primitive sequence value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advances the sequence by one.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::CounterOverflow`] at `u64::MAX`.
    pub fn checked_next(self) -> Result<Self, IdentityError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(IdentityError::CounterOverflow)
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A nonzero generation distinguishing successive source connections.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionGeneration(u64);

impl ConnectionGeneration {
    /// Constructs a nonzero connection generation.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::ZeroGeneration`] for zero.
    pub const fn new(value: u64) -> Result<Self, IdentityError> {
        if value == 0 {
            Err(IdentityError::ZeroGeneration)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the primitive generation value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advances to the next connection generation.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::CounterOverflow`] at `u64::MAX`.
    pub fn checked_next(self) -> Result<Self, IdentityError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(IdentityError::CounterOverflow)
    }
}

impl fmt::Display for ConnectionGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for ConnectionGeneration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ConnectionGeneration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::SourceIdentifier;

    #[test]
    fn bounded_identity_reports_retained_capacity_not_only_encoded_length()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut value = String::with_capacity(SourceIdentifier::MAX_LENGTH);
        value.push_str("id");
        let identifier = SourceIdentifier::try_from(value)?;

        assert_eq!(identifier.as_str().len(), 2);
        assert!(identifier.retained_bytes() >= SourceIdentifier::MAX_LENGTH);
        Ok(())
    }
}
