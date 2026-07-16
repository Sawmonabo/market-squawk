use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha3::{Digest, Keccak256};

use crate::{ProviderInstrumentId, SourceIdentifier, VenueId};

/// A syntax or check-digit failure for an external identifier.
///
/// Validation represented by this error is deliberately separate from registry assignment,
/// instrument existence, lifecycle status, and data-license entitlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierError {
    /// A required identifier was empty.
    Empty,
    /// Text did not have the identifier type's fixed length.
    InvalidLength,
    /// Text contained a character forbidden by this identifier type.
    InvalidCharacter,
    /// The identifier's type-specific check digit did not match.
    InvalidChecksum,
    /// A FIGI used an ANSI X9.145-reserved prefix.
    ReservedPrefix,
    /// A calendar or contract-month field was invalid.
    InvalidDate,
    /// An OCC option had neither `C` nor `P` in its option-type field.
    InvalidOptionKind,
    /// A crypto pair used the same source asset identity on both sides.
    IdenticalPairAssets,
    /// A chain identifier did not satisfy the CAIP-2 grammar.
    InvalidChainId,
    /// An address did not satisfy its explicitly selected protocol grammar.
    InvalidAddress,
    /// A mixed-case EVM address did not satisfy EIP-55.
    InvalidAddressChecksum,
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("identifier must not be empty"),
            Self::InvalidLength => formatter.write_str("identifier has an invalid length"),
            Self::InvalidCharacter => {
                formatter.write_str("identifier contains an invalid character")
            }
            Self::InvalidChecksum => formatter.write_str("identifier check digit is invalid"),
            Self::ReservedPrefix => formatter.write_str("identifier uses a reserved prefix"),
            Self::InvalidDate => formatter.write_str("identifier contains an invalid date"),
            Self::InvalidOptionKind => formatter.write_str("option kind must be C or P"),
            Self::IdenticalPairAssets => {
                formatter.write_str("crypto pair base and quote identities must differ")
            }
            Self::InvalidChainId => {
                formatter.write_str("chain identifier is not valid CAIP-2 syntax")
            }
            Self::InvalidAddress => formatter.write_str("address does not match its protocol rule"),
            Self::InvalidAddressChecksum => formatter.write_str("EVM address checksum is invalid"),
        }
    }
}

impl std::error::Error for IdentifierError {}

fn validate_source_symbol(value: &str, max: usize, ticker: bool) -> Result<(), IdentifierError> {
    if value.is_empty() {
        return Err(IdentifierError::Empty);
    }
    if value.len() > max {
        return Err(IdentifierError::InvalidLength);
    }
    let valid = value.bytes().all(|byte| {
        if ticker {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/')
        } else {
            byte.is_ascii_graphic()
        }
    });
    if valid {
        Ok(())
    } else {
        Err(IdentifierError::InvalidCharacter)
    }
}

macro_rules! source_symbol {
    ($(#[$metadata:meta])* $name:ident, $max:expr, $ticker:expr) => {
        $(#[$metadata])*
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Maximum source symbol length in ASCII bytes.
            pub const MAX_LENGTH: usize = $max;

            /// Returns the source-preserved symbol.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdentifierError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                validate_source_symbol(value, Self::MAX_LENGTH, $ticker)?;
                Ok(Self(value.to_owned()))
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                validate_source_symbol(&value, Self::MAX_LENGTH, $ticker)?;
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

source_symbol!(
    /// A bounded source-provided ticker alias. It is not an instrument identity or registry proof.
    Ticker,
    32,
    true
);

source_symbol!(
    /// A bounded venue-native symbol preserved without global pair/futures parsing.
    VenueSymbol,
    128,
    false
);

fn uppercase_fixed(value: &str, length: usize) -> Result<String, IdentifierError> {
    if value.len() != length {
        return Err(IdentifierError::InvalidLength);
    }
    if !value.is_ascii() {
        return Err(IdentifierError::InvalidCharacter);
    }
    Ok(value.to_ascii_uppercase())
}

fn identifier_value(byte: u8) -> Option<u32> {
    match byte {
        b'0'..=b'9' => Some(u32::from(byte - b'0')),
        b'A'..=b'Z' => Some(u32::from(byte - b'A') + 10),
        _ => None,
    }
}

fn decimal_digit_sum(value: u32) -> u32 {
    value / 10 + value % 10
}

macro_rules! checked_identifier_serde {
    ($name:ident) => {
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

/// A validated futures contract month kept separate from venue-native symbols.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct ContractMonth {
    year: u16,
    month: u8,
}

impl fmt::Display for ContractMonth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:04}-{:02}", self.year, self.month)
    }
}

#[derive(Deserialize)]
struct ContractMonthWire {
    year: u16,
    month: u8,
}

impl<'de> Deserialize<'de> for ContractMonth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ContractMonthWire::deserialize(deserializer)?;
        Self::new(wire.year, wire.month).map_err(serde::de::Error::custom)
    }
}

impl ContractMonth {
    /// Validates a year and month without parsing venue month codes.
    ///
    /// # Errors
    ///
    /// Rejects year zero and months outside 1 through 12.
    pub const fn new(year: u16, month: u8) -> Result<Self, IdentifierError> {
        if year == 0 || month == 0 || month > 12 {
            Err(IdentifierError::InvalidDate)
        } else {
            Ok(Self { year, month })
        }
    }

    /// Returns the full contract year.
    pub const fn year(self) -> u16 {
        self.year
    }

    /// Returns the contract month from 1 through 12.
    pub const fn month(self) -> u8 {
        self.month
    }
}

/// Venue reference-data security type for a futures identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FuturesSecurityType {
    /// Outright future.
    Future,
    /// Venue-defined spread or multileg instrument.
    SpreadOrMultileg,
    /// Venue-defined daily contract.
    Daily,
}

/// A structured futures identity; deliberately not a universal root/month-symbol parser.
///
/// CFTC large-trader layouts and CME security definitions keep exchange security identifiers,
/// identifier sources, product codes, native symbols, and expiry fields separate. Existence and
/// contract economics require licensed venue reference data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FuturesContractIdentity {
    venue_id: VenueId,
    security_id: ProviderInstrumentId,
    security_id_source: SourceIdentifier,
    product_code: ProviderInstrumentId,
    native_symbol: VenueSymbol,
    security_type: FuturesSecurityType,
    contract_month: ContractMonth,
}

impl FuturesContractIdentity {
    /// Constructs a futures identity from separately sourced venue metadata fields.
    pub fn new(
        venue_id: VenueId,
        security_id: ProviderInstrumentId,
        security_id_source: SourceIdentifier,
        product_code: ProviderInstrumentId,
        native_symbol: VenueSymbol,
        security_type: FuturesSecurityType,
        contract_month: ContractMonth,
    ) -> Self {
        Self {
            venue_id,
            security_id,
            security_id_source,
            product_code,
            native_symbol,
            security_type,
            contract_month,
        }
    }

    /// Returns the unmodified venue-native symbol.
    pub const fn native_symbol(&self) -> &VenueSymbol {
        &self.native_symbol
    }

    /// Returns the separately supplied contract month.
    pub const fn contract_month(&self) -> ContractMonth {
        self.contract_month
    }
}

/// Venue product family for a directional crypto pair.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CryptoProductType {
    /// Spot pair.
    Spot,
    /// Perpetual derivative pair.
    Perpetual,
    /// Dated future pair.
    Future,
    /// Option pair.
    Option,
}

/// A venue-qualified directional crypto pair from structured venue product metadata.
///
/// It never guesses delimiters, quote suffixes, or global BTC/XBT aliases. The raw product ID and
/// separate base/quote source identities are preserved; syntax does not prove product existence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CryptoPair {
    venue_id: VenueId,
    raw_product_id: ProviderInstrumentId,
    base_asset_id: ProviderInstrumentId,
    quote_asset_id: ProviderInstrumentId,
    product_type: CryptoProductType,
}

impl CryptoPair {
    /// Constructs a directional pair from venue reference fields.
    ///
    /// # Errors
    ///
    /// Rejects equal base and quote source identities.
    pub fn new(
        venue_id: VenueId,
        raw_product_id: ProviderInstrumentId,
        base_asset_id: ProviderInstrumentId,
        quote_asset_id: ProviderInstrumentId,
        product_type: CryptoProductType,
    ) -> Result<Self, IdentifierError> {
        if base_asset_id == quote_asset_id {
            return Err(IdentifierError::IdenticalPairAssets);
        }
        Ok(Self {
            venue_id,
            raw_product_id,
            base_asset_id,
            quote_asset_id,
            product_type,
        })
    }

    /// Returns the unmodified venue product ID.
    pub const fn raw_product_id(&self) -> &ProviderInstrumentId {
        &self.raw_product_id
    }

    /// Returns the source-aware base asset identity.
    pub const fn base_asset_id(&self) -> &ProviderInstrumentId {
        &self.base_asset_id
    }
}

impl fmt::Display for CryptoPair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw_product_id.fmt(formatter)
    }
}

#[derive(Deserialize)]
struct CryptoPairWire {
    venue_id: VenueId,
    raw_product_id: ProviderInstrumentId,
    base_asset_id: ProviderInstrumentId,
    quote_asset_id: ProviderInstrumentId,
    product_type: CryptoProductType,
}

impl<'de> Deserialize<'de> for CryptoPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CryptoPairWire::deserialize(deserializer)?;
        Self::new(
            wire.venue_id,
            wire.raw_product_id,
            wire.base_asset_id,
            wire.quote_asset_id,
            wire.product_type,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// A case-sensitive CAIP-2 chain identifier.
///
/// This validates only the [CAIP-2 grammar](https://standards.chainagnostic.org/CAIPs/caip-2), not
/// chain existence or canonical reference semantics.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChainId(String);

impl ChainId {
    /// Returns the source-preserved chain identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ChainId {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let Some((namespace, reference)) = value.split_once(':') else {
            return Err(IdentifierError::InvalidChainId);
        };
        let namespace_valid = (3..=8).contains(&namespace.len())
            && namespace
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        let reference_valid = (1..=32).contains(&reference.len())
            && reference
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if !namespace_valid || !reference_valid || reference.contains(':') {
            return Err(IdentifierError::InvalidChainId);
        }
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for ChainId {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for ChainId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ChainId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

/// Semantic role of a chain-qualified address.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainAddressRole {
    /// General account or wallet.
    Account,
    /// Recipient in a transfer context.
    Recipient,
    /// EVM token contract.
    TokenContract,
    /// Solana token mint account.
    Mint,
}

/// The explicit protocol rule used to validate a chain address.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainAddressRule {
    /// 20-byte EVM hex with EIP-55 enforcement for mixed case.
    EvmHex20Eip55,
    /// 32-byte case-sensitive Solana base58 public key.
    SolanaBase58PublicKey,
}

/// A chain-qualified, protocol-specifically validated address.
///
/// EVM validation follows [EIP-55](https://eips.ethereum.org/EIPS/eip-55); Solana validation uses
/// the 32-byte public-key contract documented by [Solana accounts](https://solana.com/docs/core/accounts).
/// The type exposes no universal address parser, does not infer chains, and does not prove on-chain
/// account/contract existence or token semantics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChainAddress {
    chain_id: ChainId,
    submitted: String,
    canonical: String,
    decoded_bytes: Vec<u8>,
    role: ChainAddressRole,
    rule: ChainAddressRule,
}

impl ChainAddress {
    /// Validates a 20-byte EVM address, enforcing EIP-55 when input case is mixed.
    ///
    /// # Errors
    ///
    /// Rejects wrong length/hex or an invalid mixed-case checksum.
    pub fn try_evm(
        chain_id: ChainId,
        submitted: &str,
        role: ChainAddressRole,
    ) -> Result<Self, IdentifierError> {
        if submitted.len() != 42 || !submitted.starts_with("0x") && !submitted.starts_with("0X") {
            return Err(IdentifierError::InvalidAddress);
        }
        let body = submitted.get(2..).ok_or(IdentifierError::InvalidAddress)?;
        if !body.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(IdentifierError::InvalidAddress);
        }
        let has_lower = body.bytes().any(|byte| matches!(byte, b'a'..=b'f'));
        let has_upper = body.bytes().any(|byte| matches!(byte, b'A'..=b'F'));
        if has_lower && has_upper && !valid_eip55(body) {
            return Err(IdentifierError::InvalidAddressChecksum);
        }
        let mut decoded = Vec::with_capacity(20);
        let bytes = body.as_bytes();
        for pair in bytes.chunks_exact(2) {
            let Some(high) = pair.first().and_then(|byte| hex_nibble(*byte)) else {
                return Err(IdentifierError::InvalidAddress);
            };
            let Some(low) = pair.get(1).and_then(|byte| hex_nibble(*byte)) else {
                return Err(IdentifierError::InvalidAddress);
            };
            decoded.push(high * 16 + low);
        }
        Ok(Self {
            chain_id,
            submitted: submitted.to_owned(),
            canonical: format!("0x{}", body.to_ascii_lowercase()),
            decoded_bytes: decoded,
            role,
            rule: ChainAddressRule::EvmHex20Eip55,
        })
    }

    /// Validates a case-sensitive Solana base58 value that decodes to exactly 32 bytes.
    ///
    /// # Errors
    ///
    /// Rejects invalid base58 or decoded lengths other than 32 bytes.
    pub fn try_solana(
        chain_id: ChainId,
        submitted: &str,
        role: ChainAddressRole,
    ) -> Result<Self, IdentifierError> {
        let decoded = bs58::decode(submitted)
            .into_vec()
            .map_err(|_| IdentifierError::InvalidAddress)?;
        if decoded.len() != 32 {
            return Err(IdentifierError::InvalidAddress);
        }
        Ok(Self {
            chain_id,
            submitted: submitted.to_owned(),
            canonical: submitted.to_owned(),
            decoded_bytes: decoded,
            role,
            rule: ChainAddressRule::SolanaBase58PublicKey,
        })
    }

    /// Returns the explicitly supplied chain identity.
    pub const fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }

    /// Returns the protocol-defined canonical display retained alongside submitted text.
    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    /// Returns the losslessly decoded identity bytes.
    pub fn decoded_bytes(&self) -> &[u8] {
        &self.decoded_bytes
    }
}

impl fmt::Display for ChainAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.canonical.fmt(formatter)
    }
}

fn valid_eip55(body: &str) -> bool {
    let lowercase = body.to_ascii_lowercase();
    let hash = Keccak256::digest(lowercase.as_bytes());
    for (index, byte) in body.bytes().enumerate() {
        if !byte.is_ascii_alphabetic() {
            continue;
        }
        let hash_byte = hash[index / 2];
        let hash_nibble = if index % 2 == 0 {
            hash_byte >> 4
        } else {
            hash_byte & 0x0f
        };
        if byte.is_ascii_uppercase() != (hash_nibble >= 8) {
            return false;
        }
    }
    true
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Deserialize)]
struct ChainAddressWire {
    chain_id: ChainId,
    submitted: String,
    role: ChainAddressRole,
    rule: ChainAddressRule,
}

impl<'de> Deserialize<'de> for ChainAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ChainAddressWire::deserialize(deserializer)?;
        match wire.rule {
            ChainAddressRule::EvmHex20Eip55 => {
                Self::try_evm(wire.chain_id, &wire.submitted, wire.role)
            }
            ChainAddressRule::SolanaBase58PublicKey => {
                Self::try_solana(wire.chain_id, &wire.submitted, wire.role)
            }
        }
        .map_err(serde::de::Error::custom)
    }
}
