//! FIX `MonthYear` values used by derivative maturity fields.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::IdentifierError;

/// A source-preserving FIX `MonthYear` value.
///
/// FIX Latest distinguishes a month (`YYYYMM`), a specific day (`YYYYMMDD`), and a numbered week
/// (`YYYYMMwN`). Keeping those forms as distinct variants prevents weekly or daily contracts from
/// collapsing into a lossy month-only identity. This type is used for both
/// `MaturityMonthYear(200)` and `LegMaturityMonthYear(610)`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MaturityMonthYear {
    /// A maturity identified only to a calendar month.
    Month {
        /// Four-digit FIX year, including the FIX-defined `0000` value.
        year: u16,
        /// Calendar month from 1 through 12.
        month: u8,
    },
    /// A maturity identified by an appended day value.
    Day {
        /// Four-digit FIX year, including the FIX-defined `0000` value.
        year: u16,
        /// Calendar month from 1 through 12.
        month: u8,
        /// FIX `MonthYear` day from 1 through 31.
        day: u8,
    },
    /// A maturity identified by an appended week-of-month value.
    Week {
        /// Four-digit FIX year, including the FIX-defined `0000` value.
        year: u16,
        /// Calendar month from 1 through 12.
        month: u8,
        /// FIX week code from 1 through 5.
        week: u8,
    },
}

impl MaturityMonthYear {
    /// Constructs the `YYYYMM` form.
    ///
    /// # Errors
    ///
    /// Rejects years above 9999 and months outside 1 through 12.
    pub fn month(year: u16, month: u8) -> Result<Self, IdentifierError> {
        validate_year_month(year, month)?;
        Ok(Self::Month { year, month })
    }

    /// Constructs the `YYYYMMDD` form.
    ///
    /// # Errors
    ///
    /// Rejects an invalid year/month or a FIX `MonthYear` day outside 1 through 31.
    pub fn day(year: u16, month: u8, day: u8) -> Result<Self, IdentifierError> {
        validate_year_month(year, month)?;
        if day == 0 || day > 31 {
            return Err(IdentifierError::InvalidDate);
        }
        Ok(Self::Day { year, month, day })
    }

    /// Constructs the `YYYYMMwN` form.
    ///
    /// # Errors
    ///
    /// Rejects an invalid year/month or a week outside FIX's `w1` through `w5` values.
    pub fn week(year: u16, month: u8, week: u8) -> Result<Self, IdentifierError> {
        validate_year_month(year, month)?;
        if week == 0 || week > 5 {
            return Err(IdentifierError::InvalidDate);
        }
        Ok(Self::Week { year, month, week })
    }

    /// Returns the four-digit FIX year.
    pub const fn year(self) -> u16 {
        match self {
            Self::Month { year, .. } | Self::Day { year, .. } | Self::Week { year, .. } => year,
        }
    }

    /// Returns the calendar month from 1 through 12.
    pub const fn calendar_month(self) -> u8 {
        match self {
            Self::Month { month, .. } | Self::Day { month, .. } | Self::Week { month, .. } => month,
        }
    }

    /// Returns the appended day only for the `YYYYMMDD` form.
    pub const fn appended_day(self) -> Option<u8> {
        match self {
            Self::Day { day, .. } => Some(day),
            Self::Month { .. } | Self::Week { .. } => None,
        }
    }

    /// Returns the appended week only for the `YYYYMMwN` form.
    pub const fn appended_week(self) -> Option<u8> {
        match self {
            Self::Week { week, .. } => Some(week),
            Self::Month { .. } | Self::Day { .. } => None,
        }
    }
}

const fn validate_year_month(year: u16, month: u8) -> Result<(), IdentifierError> {
    if year > 9_999 || month == 0 || month > 12 {
        Err(IdentifierError::InvalidDate)
    } else {
        Ok(())
    }
}

impl FromStr for MaturityMonthYear {
    type Err = IdentifierError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !value.is_ascii() || !matches!(value.len(), 6 | 8) {
            return Err(IdentifierError::InvalidDate);
        }
        let bytes = value.as_bytes();
        if !bytes[..6].iter().all(u8::is_ascii_digit) {
            return Err(IdentifierError::InvalidDate);
        }
        let year = parse_digits(&bytes[..4])?;
        let month =
            u8::try_from(parse_digits(&bytes[4..6])?).map_err(|_| IdentifierError::InvalidDate)?;
        match bytes.len() {
            6 => Self::month(year, month),
            8 if bytes[6] == b'w' && bytes[7].is_ascii_digit() => {
                Self::week(year, month, bytes[7] - b'0')
            }
            8 if bytes[6..].iter().all(u8::is_ascii_digit) => {
                let day = u8::try_from(parse_digits(&bytes[6..])?)
                    .map_err(|_| IdentifierError::InvalidDate)?;
                Self::day(year, month, day)
            }
            8 | 0..=5 | 7 | 9.. => Err(IdentifierError::InvalidDate),
        }
    }
}

impl TryFrom<&str> for MaturityMonthYear {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

fn parse_digits(bytes: &[u8]) -> Result<u16, IdentifierError> {
    bytes.iter().try_fold(0_u16, |value, byte| {
        if byte.is_ascii_digit() {
            value
                .checked_mul(10)
                .and_then(|value| value.checked_add(u16::from(*byte - b'0')))
                .ok_or(IdentifierError::InvalidDate)
        } else {
            Err(IdentifierError::InvalidDate)
        }
    })
}

impl fmt::Display for MaturityMonthYear {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Month { year, month } => write!(formatter, "{year:04}{month:02}"),
            Self::Day { year, month, day } => write!(formatter, "{year:04}{month:02}{day:02}"),
            Self::Week { year, month, week } => write!(formatter, "{year:04}{month:02}w{week}"),
        }
    }
}

impl Serialize for MaturityMonthYear {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MaturityMonthYear {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}
