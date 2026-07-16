use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha3::{Digest, Keccak256};

use super::IdentifierError;
use crate::{ProviderInstrumentId, VenueId};

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
