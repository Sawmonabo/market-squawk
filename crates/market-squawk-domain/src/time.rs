use std::fmt;

use serde::{Deserialize, Serialize};

/// A checked timestamp conversion or arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeError {
    /// Signed Unix-nanosecond arithmetic exceeded `i64`.
    Overflow,
}

impl fmt::Display for TimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => formatter.write_str("timestamp arithmetic overflow"),
        }
    }
}

impl std::error::Error for TimeError {}

/// A UTC instant represented as signed nanoseconds from the Unix epoch.
///
/// This stable scalar contract contains no wall-clock access or I/O. Its inclusive representation
/// boundaries are `i64::MIN` through `i64::MAX` Unix nanoseconds; adapters must perform checked
/// conversion before constructing it. Task 4 builds ordering-rich provenance on this same type.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Constructs a timestamp from an already range-checked signed Unix-nanosecond value.
    pub const fn from_unix_nanos(value: i64) -> Self {
        Self(value)
    }

    /// Returns signed Unix nanoseconds.
    pub const fn unix_nanos(self) -> i64 {
        self.0
    }

    /// Adds a signed nanosecond offset using checked arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`TimeError::Overflow`] outside the signed `i64` representation.
    pub fn checked_add_nanos(self, offset: i64) -> Result<Self, TimeError> {
        self.0
            .checked_add(offset)
            .map(Self)
            .ok_or(TimeError::Overflow)
    }

    /// Subtracts a signed nanosecond offset using checked arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`TimeError::Overflow`] outside the signed `i64` representation.
    pub fn checked_sub_nanos(self, offset: i64) -> Result<Self, TimeError> {
        self.0
            .checked_sub(offset)
            .map(Self)
            .ok_or(TimeError::Overflow)
    }
}
