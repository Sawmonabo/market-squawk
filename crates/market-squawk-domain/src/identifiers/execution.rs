//! Execution identities that cannot be interchanged with instrument or provider identifiers.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// A failure to construct an execution identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionIdentityError {
    /// A UUID identity was nil.
    NilUuid,
    /// Text was not a UUID.
    InvalidUuid,
    /// A textual identity was empty.
    Empty,
    /// A textual identity exceeded its UTF-8 byte ceiling.
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
    },
    /// A textual identity contained whitespace, a control byte, or unsupported punctuation.
    InvalidCharacter,
}

impl fmt::Display for ExecutionIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NilUuid => formatter.write_str("execution UUID must not be nil"),
            Self::InvalidUuid => formatter.write_str("execution identity is not a UUID"),
            Self::Empty => formatter.write_str("execution identity must not be empty"),
            Self::TooLong { max } => {
                write!(formatter, "execution identity exceeds {max} UTF-8 bytes")
            }
            Self::InvalidCharacter => formatter.write_str(
                "execution identity contains whitespace, control bytes, or unsupported punctuation",
            ),
        }
    }
}

impl std::error::Error for ExecutionIdentityError {}

macro_rules! execution_uuid_identity {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            /// Returns the underlying non-nil UUID.
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl TryFrom<Uuid> for $name {
            type Error = ExecutionIdentityError;

            fn try_from(value: Uuid) -> Result<Self, Self::Error> {
                if value.is_nil() {
                    Err(ExecutionIdentityError::NilUuid)
                } else {
                    Ok(Self(value))
                }
            }
        }

        impl FromStr for $name {
            type Err = ExecutionIdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value)
                    .map_err(|_| ExecutionIdentityError::InvalidUuid)
                    .and_then(Self::try_from)
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
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

execution_uuid_identity!(
    /// Stable account identity.
    AccountId
);
execution_uuid_identity!(
    /// Stable strategy identity.
    StrategyId
);
execution_uuid_identity!(
    /// Stable model identity.
    ModelId
);
execution_uuid_identity!(
    /// Stable order identity.
    OrderId
);
execution_uuid_identity!(
    /// Stable risk-approval identity.
    ApprovalId
);

/// Caller-selected idempotency identity for one logical order.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    /// Maximum encoded client-order identity length.
    pub const MAX_LENGTH: usize = 128;

    /// Returns the validated identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns bytes retained by the owned allocation.
    pub fn retained_bytes(&self) -> usize {
        self.0.capacity()
    }
}

fn validate_client_order_id(value: &str) -> Result<(), ExecutionIdentityError> {
    if value.is_empty() {
        return Err(ExecutionIdentityError::Empty);
    }
    if value.len() > ClientOrderId::MAX_LENGTH {
        return Err(ExecutionIdentityError::TooLong {
            max: ClientOrderId::MAX_LENGTH,
        });
    }
    if value.bytes().any(|byte| {
        !byte.is_ascii_alphanumeric() || byte.is_ascii_whitespace() || byte.is_ascii_control()
    }) {
        let all_supported = value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'));
        if !all_supported {
            return Err(ExecutionIdentityError::InvalidCharacter);
        }
    }
    Ok(())
}

impl TryFrom<&str> for ClientOrderId {
    type Error = ExecutionIdentityError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_client_order_id(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for ClientOrderId {
    type Error = ExecutionIdentityError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_client_order_id(&value)?;
        Ok(Self(value))
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for ClientOrderId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ClientOrderId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}
