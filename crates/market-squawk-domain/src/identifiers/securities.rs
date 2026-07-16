use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{IdentifierError, decimal_digit_sum, identifier_value, uppercase_fixed};

/// A checksum-valid CUSIP syntax value, not evidence of CGS assignment or data rights.
///
/// The grammar follows the [CGS identifier description](https://www.cusip.com/identifiers.html?section=CUSIP)
/// and its published Modulus 10 Double-Add-Double rule. CUSIP data is licensed; this type bundles
/// no reference database and does not establish permission to store or redistribute CGS data.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Cusip(String);

impl Cusip {
    /// Returns the normalized uppercase, checksum-valid syntax value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Cusip {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let normalized = uppercase_fixed(value, 9)?;
        let bytes = normalized.as_bytes();
        let mut sum = 0_u32;
        for (index, byte) in bytes.iter().copied().take(8).enumerate() {
            let mapped = match byte {
                b'*' if index < 6 => 36,
                b'@' if index < 6 => 37,
                b'#' if index < 6 => 38,
                _ => identifier_value(byte).ok_or(IdentifierError::InvalidCharacter)?,
            };
            let product = mapped * if index % 2 == 0 { 1 } else { 2 };
            sum += decimal_digit_sum(product);
        }
        let Some(check_byte) = bytes.get(8).copied() else {
            return Err(IdentifierError::InvalidLength);
        };
        if !check_byte.is_ascii_digit() {
            return Err(IdentifierError::InvalidCharacter);
        }
        let expected = (10 - sum % 10) % 10;
        if u32::from(check_byte - b'0') != expected {
            return Err(IdentifierError::InvalidChecksum);
        }
        Ok(Self(normalized))
    }
}

impl TryFrom<String> for Cusip {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

checked_identifier_serde!(Cusip);

/// A checksum-valid ISO 6166 ISIN syntax value, not proof of NNA/DSB assignment.
///
/// ISO TC 68 publishes the [12-character structure and Modulus 10
/// algorithm](https://committee.iso.org/sites/tc68/home/articles/content-left-area/articles/what-is-isin.html).
/// Prefix registry policy and licensed reference data remain outside this syntax type.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Isin(String);

impl Isin {
    /// Returns the normalized uppercase, checksum-valid syntax value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Isin {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let normalized = uppercase_fixed(value, 12)?;
        let bytes = normalized.as_bytes();
        if !bytes
            .iter()
            .copied()
            .take(2)
            .all(|byte| byte.is_ascii_uppercase())
            || !bytes
                .iter()
                .copied()
                .skip(2)
                .take(9)
                .all(|byte| byte.is_ascii_alphanumeric())
            || !bytes.get(11).is_some_and(u8::is_ascii_digit)
        {
            return Err(IdentifierError::InvalidCharacter);
        }
        let mut digits = Vec::with_capacity(24);
        for byte in bytes {
            let mapped = identifier_value(*byte).ok_or(IdentifierError::InvalidCharacter)?;
            if mapped >= 10 {
                digits.push(mapped / 10);
            }
            digits.push(mapped % 10);
        }
        let sum = digits
            .iter()
            .rev()
            .enumerate()
            .fold(0_u32, |total, (index, digit)| {
                let weighted = if index % 2 == 1 { digit * 2 } else { *digit };
                total + decimal_digit_sum(weighted)
            });
        if !sum.is_multiple_of(10) {
            return Err(IdentifierError::InvalidChecksum);
        }
        Ok(Self(normalized))
    }
}

impl TryFrom<String> for Isin {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

checked_identifier_serde!(Isin);

/// A checksum-valid SEDOL syntax value, not proof of LSEG assignment or licensing.
///
/// Legacy numeric and post-March-2004 consonant formats follow the
/// [LSEG SEDOL Masterfile Service & Technical Guide v8.8](https://www.lseg.com/content/dam/lseg/en_us/documents/sedol/sedol-masterfile-service-and-technical-guide-v8.8.pdf).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sedol(String);

impl Sedol {
    /// Returns the normalized uppercase, checksum-valid syntax value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Sedol {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        const WEIGHTS: [u32; 7] = [1, 3, 1, 7, 3, 9, 1];
        const CONSONANTS: &[u8] = b"BCDFGHJKLMNPQRSTVWXYZ";
        let normalized = uppercase_fixed(value, 7)?;
        let bytes = normalized.as_bytes();
        let legacy = bytes.iter().all(u8::is_ascii_digit);
        let current = bytes.first().is_some_and(|byte| CONSONANTS.contains(byte))
            && bytes
                .iter()
                .copied()
                .skip(1)
                .take(5)
                .all(|byte| byte.is_ascii_digit() || CONSONANTS.contains(&byte))
            && bytes.get(6).is_some_and(u8::is_ascii_digit);
        if !legacy && !current {
            return Err(IdentifierError::InvalidCharacter);
        }
        let mut sum = 0_u32;
        for (byte, weight) in bytes.iter().zip(WEIGHTS) {
            sum += identifier_value(*byte).ok_or(IdentifierError::InvalidCharacter)? * weight;
        }
        if !sum.is_multiple_of(10) {
            return Err(IdentifierError::InvalidChecksum);
        }
        Ok(Self(normalized))
    }
}

impl TryFrom<String> for Sedol {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

checked_identifier_serde!(Sedol);

/// A checksum-valid ANSI X9.145 FIGI syntax value, not proof of OpenFIGI assignment.
///
/// Grammar, reserved prefixes, weights, and check-digit behavior follow the
/// [ANSI X9.145-2021 specification](https://x9.org/wp-content/uploads/2021/08/ANSI-X9.145-2021-Financial-Instrument-Global-Identifier-FIGI.pdf).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Figi(String);

impl Figi {
    /// Returns the normalized uppercase, checksum-valid syntax value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Figi {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        const CONSONANTS: &[u8] = b"BCDFGHJKLMNPQRSTVWXYZ";
        const RESERVED: [&[u8; 2]; 7] = [b"BS", b"BM", b"GG", b"GB", b"GH", b"KY", b"VG"];
        let normalized = uppercase_fixed(value, 12)?;
        let bytes = normalized.as_bytes();
        let Some(prefix) = bytes.get(0..2) else {
            return Err(IdentifierError::InvalidLength);
        };
        if RESERVED
            .iter()
            .any(|reserved| prefix == reserved.as_slice())
        {
            return Err(IdentifierError::ReservedPrefix);
        }
        if !bytes
            .iter()
            .copied()
            .take(2)
            .all(|byte| CONSONANTS.contains(&byte))
            || bytes.get(2) != Some(&b'G')
            || !bytes
                .iter()
                .copied()
                .skip(3)
                .take(8)
                .all(|byte| byte.is_ascii_digit() || CONSONANTS.contains(&byte))
            || !bytes.get(11).is_some_and(u8::is_ascii_digit)
        {
            return Err(IdentifierError::InvalidCharacter);
        }
        let mut sum = 0_u32;
        for (index, byte) in bytes.iter().copied().take(11).enumerate() {
            let mapped = identifier_value(byte).ok_or(IdentifierError::InvalidCharacter)?;
            let product = mapped * if index % 2 == 0 { 1 } else { 2 };
            sum += decimal_digit_sum(product);
        }
        let expected = (10 - sum % 10) % 10;
        let Some(check) = bytes.get(11).copied() else {
            return Err(IdentifierError::InvalidLength);
        };
        if u32::from(check - b'0') != expected {
            return Err(IdentifierError::InvalidChecksum);
        }
        Ok(Self(normalized))
    }
}

impl TryFrom<String> for Figi {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

checked_identifier_serde!(Figi);
