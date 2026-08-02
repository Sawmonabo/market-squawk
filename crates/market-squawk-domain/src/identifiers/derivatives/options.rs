//! OCC option identity parsing kept separate from futures reference-data contracts.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::IdentifierError;

/// OCC option type encoded in the fixed-width OSI identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionKind {
    /// Call option (`C`).
    Call,
    /// Put option (`P`).
    Put,
}

/// A syntactically validated OCC/OSI 21-character clearing identifier.
///
/// Fixed offsets and the example are specified by the [CAT Industry Member Technical
/// Specification](https://www.catnmsplan.com/sites/default/files/2026-03/03.06.26_CAT_Reporting_Technical_Specifications_for_Industry_Members_v4.1.0r15_CLEAN.pdf).
/// Syntax does not establish series existence, deliverables, economic underlying, or data rights.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OccOptionIdentity {
    raw: String,
    root_length: usize,
    expiration_yy: u8,
    expiration_month: u8,
    expiration_day: u8,
    kind: OptionKind,
    strike_thousandths: u64,
}

impl OccOptionIdentity {
    /// Returns the source-preserved 21-character identity including root padding.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Returns the root field with trailing fixed-width padding removed.
    pub fn root(&self) -> &str {
        self.raw.get(..self.root_length).unwrap_or_default()
    }

    /// Returns the unresolved two-digit expiration year.
    pub const fn expiration_yy(&self) -> u8 {
        self.expiration_yy
    }

    /// Returns the expiration month.
    pub const fn expiration_month(&self) -> u8 {
        self.expiration_month
    }

    /// Returns the expiration day.
    pub const fn expiration_day(&self) -> u8 {
        self.expiration_day
    }

    /// Returns call/put identity.
    pub const fn kind(&self) -> OptionKind {
        self.kind
    }

    /// Returns the eight-digit strike field as integer thousandths.
    pub const fn strike_thousandths(&self) -> u64 {
        self.strike_thousandths
    }
}

impl TryFrom<&str> for OccOptionIdentity {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 21 || !value.is_ascii() {
            return Err(IdentifierError::InvalidLength);
        }
        let bytes = value.as_bytes();
        let root = bytes.get(..6).ok_or(IdentifierError::InvalidLength)?;
        let root_length = root
            .iter()
            .position(|byte| *byte == b' ')
            .unwrap_or(root.len());
        if root_length == 0
            || !root.iter().take(root_length).all(u8::is_ascii_graphic)
            || !root.iter().skip(root_length).all(|byte| *byte == b' ')
        {
            return Err(IdentifierError::InvalidCharacter);
        }
        let yy = parse_two_digits(bytes, 6)?;
        let month = parse_two_digits(bytes, 8)?;
        let day = parse_two_digits(bytes, 10)?;
        if !valid_two_digit_date(yy, month, day) {
            return Err(IdentifierError::InvalidDate);
        }
        let kind = match bytes.get(12) {
            Some(b'C') => OptionKind::Call,
            Some(b'P') => OptionKind::Put,
            _ => return Err(IdentifierError::InvalidOptionKind),
        };
        let strike = bytes.get(13..21).ok_or(IdentifierError::InvalidLength)?;
        if !strike.iter().all(u8::is_ascii_digit) {
            return Err(IdentifierError::InvalidCharacter);
        }
        let strike_thousandths = strike
            .iter()
            .fold(0_u64, |amount, byte| amount * 10 + u64::from(*byte - b'0'));
        Ok(Self {
            raw: value.to_owned(),
            root_length,
            expiration_yy: yy,
            expiration_month: month,
            expiration_day: day,
            kind,
            strike_thousandths,
        })
    }
}

impl TryFrom<String> for OccOptionIdentity {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl fmt::Display for OccOptionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.fmt(formatter)
    }
}

impl Serialize for OccOptionIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for OccOptionIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

fn parse_two_digits(bytes: &[u8], offset: usize) -> Result<u8, IdentifierError> {
    let Some(tens) = bytes.get(offset).copied() else {
        return Err(IdentifierError::InvalidLength);
    };
    let Some(ones) = bytes.get(offset + 1).copied() else {
        return Err(IdentifierError::InvalidLength);
    };
    if !tens.is_ascii_digit() || !ones.is_ascii_digit() {
        return Err(IdentifierError::InvalidCharacter);
    }
    Ok((tens - b'0') * 10 + (ones - b'0'))
}

fn valid_two_digit_date(year: u8, month: u8, day: u8) -> bool {
    let leap = year.is_multiple_of(4);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    day > 0 && day <= days
}
