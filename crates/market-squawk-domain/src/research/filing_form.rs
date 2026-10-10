//! Exact source-native regulatory filing form labels.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

/// A bounded filing form label, preserving meaningful internal spaces and amendment suffixes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct FilingForm(String);

impl FilingForm {
    /// Maximum UTF-8 bytes retained by one source filing form.
    pub const MAX_LENGTH: usize = 512;

    /// Returns the exact source form without whitespace rewriting.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns bytes retained by the owned string allocation, including spare capacity.
    pub fn retained_bytes(&self) -> usize {
        self.0.capacity()
    }

    fn validate(value: &str) -> Result<(), FilingFormError> {
        if value.is_empty() {
            return Err(FilingFormError::Empty);
        }
        if value.len() > Self::MAX_LENGTH {
            return Err(FilingFormError::TooLong);
        }
        if value.trim() != value || value.chars().any(char::is_control) {
            return Err(FilingFormError::InvalidText);
        }
        Ok(())
    }
}

impl TryFrom<&str> for FilingForm {
    type Error = FilingFormError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::validate(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for FilingForm {
    type Error = FilingFormError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::validate(&value)?;
        Ok(Self(value))
    }
}

impl fmt::Display for FilingForm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'de> Deserialize<'de> for FilingForm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FormVisitor;

        impl serde::de::Visitor<'_> for FormVisitor {
            type Value = FilingForm;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a nonempty trimmed filing form of at most 512 bytes without controls",
                )
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                FilingForm::try_from(value).map_err(E::custom)
            }

            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                FilingForm::try_from(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_string(FormVisitor)
    }
}

/// A filing form violates its exact-text contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilingFormError {
    /// No form label was supplied.
    Empty,
    /// The form exceeds its UTF-8 byte bound.
    TooLong,
    /// The form has boundary whitespace or a control character.
    InvalidText,
}

impl fmt::Display for FilingFormError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "filing form is empty",
            Self::TooLong => "filing form exceeds 512 bytes",
            Self::InvalidText => "filing form has boundary whitespace or control characters",
        })
    }
}

impl std::error::Error for FilingFormError {}
