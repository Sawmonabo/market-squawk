use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
    /// An address role is not meaningful for the selected protocol.
    InvalidAddressRole,
    /// A mixed-case EVM address did not satisfy EIP-55.
    InvalidAddressChecksum,
    /// A valid Bitcoin address was not valid for the explicitly requested network.
    InvalidAddressNetwork,
    /// First trade was later than last trade.
    FirstTradeAfterLastTrade,
    /// First trade was later than expiration.
    FirstTradeAfterExpiration,
    /// First trade was later than settlement.
    FirstTradeAfterSettlement,
    /// Last trade was later than expiration.
    LastTradeAfterExpiration,
    /// Last trade was later than settlement.
    LastTradeAfterSettlement,
    /// Expiration was later than settlement.
    ExpirationAfterSettlement,
    /// Maturity was later than settlement.
    MaturityAfterSettlement,
    /// First notice was later than last notice.
    FirstNoticeAfterLastNotice,
    /// First delivery was later than last delivery.
    FirstDeliveryAfterLastDelivery,
    /// A futures leg position or ratio was zero.
    ZeroLegRatio,
    /// A futures leg position was zero.
    ZeroLegPosition,
    /// Multileg futures legs were not ordered from one without gaps.
    InvalidLegOrdering,
    /// An outright contract had legs, or a multileg contract lacked at least two legs.
    InvalidLegStructure,
    /// A multileg contract repeated a source-qualified leg security identity.
    DuplicateLeg,
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
            Self::InvalidAddressRole => {
                formatter.write_str("address role is incompatible with its protocol")
            }
            Self::InvalidAddressChecksum => formatter.write_str("EVM address checksum is invalid"),
            Self::InvalidAddressNetwork => {
                formatter.write_str("Bitcoin address is not valid for the requested network")
            }
            Self::FirstTradeAfterLastTrade => {
                formatter.write_str("futures first trade date is after last trade date")
            }
            Self::FirstTradeAfterExpiration => {
                formatter.write_str("futures first trade date is after expiration date")
            }
            Self::FirstTradeAfterSettlement => {
                formatter.write_str("futures first trade date is after settlement date")
            }
            Self::LastTradeAfterExpiration => {
                formatter.write_str("futures last trade date is after expiration date")
            }
            Self::LastTradeAfterSettlement => {
                formatter.write_str("futures last trade date is after settlement date")
            }
            Self::ExpirationAfterSettlement => {
                formatter.write_str("futures expiration date is after settlement date")
            }
            Self::MaturityAfterSettlement => {
                formatter.write_str("futures maturity date is after settlement date")
            }
            Self::FirstNoticeAfterLastNotice => {
                formatter.write_str("futures first notice date is after last notice date")
            }
            Self::FirstDeliveryAfterLastDelivery => {
                formatter.write_str("futures first delivery date is after last delivery date")
            }
            Self::ZeroLegRatio => formatter.write_str("futures leg ratio must be nonzero"),
            Self::ZeroLegPosition => formatter.write_str("futures leg position must be nonzero"),
            Self::InvalidLegOrdering => {
                formatter.write_str("futures leg positions must be ordered from one without gaps")
            }
            Self::InvalidLegStructure => {
                formatter.write_str("futures legs do not match the security type")
            }
            Self::DuplicateLeg => {
                formatter.write_str("futures multileg identity contains a duplicate leg")
            }
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

mod derivatives;
mod digital_assets;
mod execution;
mod securities;

pub use derivatives::{
    FuturesContractIdentity, FuturesContractIdentityInput, FuturesLeg, FuturesLegInput,
    FuturesLegSide, FuturesLifecycleDateFields, FuturesLifecycleDates, FuturesSecurityType,
    MaturityMonthYear, OccOptionIdentity, OptionKind,
};
pub use digital_assets::{
    BitcoinAddressType, BitcoinNetwork, ChainAddress, ChainAddressRole, ChainAddressRule, ChainId,
    CryptoPair, CryptoProductType, EvmChainId, SolanaChainId, SolanaNetwork,
};
pub use execution::{
    AccountId, ApprovalId, ClientOrderId, ExecutionIdentityError, ModelId, OrderId, StrategyId,
};
pub use securities::{Cusip, Figi, Isin, Sedol};
