use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use bitcoin::address::{Address, NetworkUnchecked};
use bitcoin::{AddressType, Network};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha3::{Digest, Keccak256};

use super::IdentifierError;
use crate::{ProviderInstrumentId, VenueId};

/// Venue product family for a directional crypto pair.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
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

    /// Returns the venue namespace that defines all source-side pair fields.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the unmodified venue product ID.
    pub const fn raw_product_id(&self) -> &ProviderInstrumentId {
        &self.raw_product_id
    }

    /// Returns the source-aware base asset identity.
    pub const fn base_asset_id(&self) -> &ProviderInstrumentId {
        &self.base_asset_id
    }

    /// Returns the source-aware quote asset identity.
    pub const fn quote_asset_id(&self) -> &ProviderInstrumentId {
        &self.quote_asset_id
    }

    /// Returns the venue product family.
    pub const fn product_type(&self) -> CryptoProductType {
        self.product_type
    }
}

impl fmt::Display for CryptoPair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw_product_id.fmt(formatter)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
pub struct ChainId {
    canonical: String,
    namespace: String,
    reference: String,
}

impl ChainId {
    /// Returns the source-preserved chain identifier.
    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    /// Returns the CAIP-2 namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the CAIP-2 reference.
    pub fn reference(&self) -> &str {
        &self.reference
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
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
        if !namespace_valid || !reference_valid || reference.contains(':') {
            return Err(IdentifierError::InvalidChainId);
        }
        Ok(Self {
            canonical: value.to_owned(),
            namespace: namespace.to_owned(),
            reference: reference.to_owned(),
        })
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
        self.canonical.fmt(formatter)
    }
}

impl Serialize for ChainId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.canonical)
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

/// Protocol-qualified EIP-155 chain identity with a canonical decimal reference.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EvmChainId(ChainId);

impl EvmChainId {
    /// Returns the validated generic CAIP-2 representation.
    pub const fn chain_id(&self) -> &ChainId {
        &self.0
    }

    /// Returns the canonical decimal EIP-155 chain-number text.
    pub fn numeric_reference(&self) -> &str {
        self.0.reference()
    }
}

impl TryFrom<ChainId> for EvmChainId {
    type Error = IdentifierError;

    fn try_from(value: ChainId) -> Result<Self, Self::Error> {
        let reference = value.reference();
        let canonical_numeric = !reference.is_empty()
            && reference.bytes().all(|byte| byte.is_ascii_digit())
            && (reference == "0" || !reference.starts_with('0'));
        if value.namespace() != "eip155" || !canonical_numeric {
            return Err(IdentifierError::InvalidChainId);
        }
        Ok(Self(value))
    }
}

impl TryFrom<&str> for EvmChainId {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ChainId::try_from(value).and_then(Self::try_from)
    }
}

impl TryFrom<String> for EvmChainId {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl<'de> Deserialize<'de> for EvmChainId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ChainId::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for EvmChainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Source-registry network associated with a recognized Solana genesis-hash reference.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SolanaNetwork {
    /// Solana mainnet-beta.
    Mainnet,
    /// Solana testnet.
    Testnet,
    /// Solana devnet.
    Devnet,
}

impl SolanaNetwork {
    const fn reference(self) -> &'static str {
        match self {
            Self::Mainnet => "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp",
            Self::Testnet => "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3z",
            Self::Devnet => "EtWTRABZaYq6iMfeYKouRu166VU2xqa1",
        }
    }
}

/// Protocol-qualified Solana chain identity recognized from a registry genesis-hash reference.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SolanaChainId {
    chain_id: ChainId,
    #[serde(skip)]
    network: SolanaNetwork,
}

impl SolanaChainId {
    /// Returns the official mainnet-beta chain identity.
    pub fn mainnet() -> Self {
        Self::from_network(SolanaNetwork::Mainnet)
    }

    /// Constructs a chain identity from a recognized source-registry network.
    pub fn from_network(network: SolanaNetwork) -> Self {
        let reference = network.reference();
        Self {
            chain_id: ChainId {
                canonical: format!("solana:{reference}"),
                namespace: "solana".to_owned(),
                reference: reference.to_owned(),
            },
            network,
        }
    }

    /// Returns the validated generic CAIP-2 representation.
    pub const fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }

    /// Returns the source-registry network matched by the genesis reference.
    pub const fn network(&self) -> SolanaNetwork {
        self.network
    }
}

impl TryFrom<ChainId> for SolanaChainId {
    type Error = IdentifierError;

    fn try_from(value: ChainId) -> Result<Self, Self::Error> {
        if value.namespace() != "solana" {
            return Err(IdentifierError::InvalidChainId);
        }
        let network = [
            SolanaNetwork::Mainnet,
            SolanaNetwork::Testnet,
            SolanaNetwork::Devnet,
        ]
        .into_iter()
        .find(|network| value.reference() == network.reference())
        .ok_or(IdentifierError::InvalidChainId)?;
        Ok(Self {
            chain_id: value,
            network,
        })
    }
}

impl TryFrom<&str> for SolanaChainId {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ChainId::try_from(value).and_then(Self::try_from)
    }
}

impl TryFrom<String> for SolanaChainId {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl<'de> Deserialize<'de> for SolanaChainId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ChainId::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for SolanaChainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.chain_id.fmt(formatter)
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
    /// Bitcoin Base58, Bech32, or Bech32m address validated by rust-bitcoin.
    BitcoinAddress,
}

/// Bitcoin network required during address parsing.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BitcoinNetwork {
    /// Bitcoin production network.
    Mainnet,
    /// Bitcoin testnet3 network.
    Testnet,
    /// Bitcoin testnet4 network.
    Testnet4,
    /// Bitcoin signet network.
    Signet,
    /// Local Bitcoin regression-test network.
    Regtest,
}

impl From<BitcoinNetwork> for Network {
    fn from(value: BitcoinNetwork) -> Self {
        match value {
            BitcoinNetwork::Mainnet => Self::Bitcoin,
            BitcoinNetwork::Testnet => Self::Testnet,
            BitcoinNetwork::Testnet4 => Self::Testnet4,
            BitcoinNetwork::Signet => Self::Signet,
            BitcoinNetwork::Regtest => Self::Regtest,
        }
    }
}

/// Supported Bitcoin address family after network validation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BitcoinAddressType {
    /// Legacy pay-to-public-key-hash Base58 address.
    P2pkh,
    /// Legacy pay-to-script-hash Base58 address.
    P2sh,
    /// Version-zero pay-to-witness-public-key-hash Bech32 address.
    P2wpkh,
    /// Version-zero pay-to-witness-script-hash Bech32 address.
    P2wsh,
    /// Version-one pay-to-Taproot Bech32m address.
    P2tr,
}

impl TryFrom<AddressType> for BitcoinAddressType {
    type Error = IdentifierError;

    fn try_from(value: AddressType) -> Result<Self, Self::Error> {
        match value {
            AddressType::P2pkh => Ok(Self::P2pkh),
            AddressType::P2sh => Ok(Self::P2sh),
            AddressType::P2wpkh => Ok(Self::P2wpkh),
            AddressType::P2wsh => Ok(Self::P2wsh),
            AddressType::P2tr => Ok(Self::P2tr),
            _ => Err(IdentifierError::InvalidAddress),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ChainAddressPayload {
    Evm([u8; 20]),
    Solana([u8; 32]),
    BitcoinScript(Vec<u8>),
}

/// A chain-qualified, protocol-specifically validated address.
///
/// EVM validation follows [EIP-55](https://eips.ethereum.org/EIPS/eip-55); Solana validation uses
/// the 32-byte public-key contract documented by [Solana accounts](https://solana.com/docs/core/accounts).
/// The type exposes no universal address parser, does not infer chains, and does not prove on-chain
/// account/contract existence or token semantics.
#[derive(Clone, Debug)]
pub struct ChainAddress {
    chain_id: ChainId,
    submitted: String,
    canonical: String,
    payload: ChainAddressPayload,
    role: ChainAddressRole,
    rule: ChainAddressRule,
    bitcoin_network: Option<BitcoinNetwork>,
    bitcoin_address_type: Option<BitcoinAddressType>,
}

impl ChainAddress {
    /// Validates a 20-byte EVM address, enforcing EIP-55 when input case is mixed.
    ///
    /// # Errors
    ///
    /// Rejects wrong length/hex or an invalid mixed-case checksum.
    pub fn try_evm(
        chain_id: EvmChainId,
        submitted: &str,
        role: ChainAddressRole,
    ) -> Result<Self, IdentifierError> {
        if matches!(role, ChainAddressRole::Mint) {
            return Err(IdentifierError::InvalidAddressRole);
        }
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
        let mut decoded = [0_u8; 20];
        let bytes = body.as_bytes();
        for (index, pair) in bytes.chunks_exact(2).enumerate() {
            let Some(high) = pair.first().and_then(|byte| hex_nibble(*byte)) else {
                return Err(IdentifierError::InvalidAddress);
            };
            let Some(low) = pair.get(1).and_then(|byte| hex_nibble(*byte)) else {
                return Err(IdentifierError::InvalidAddress);
            };
            decoded[index] = high * 16 + low;
        }
        Ok(Self {
            chain_id: chain_id.0,
            submitted: submitted.to_owned(),
            canonical: format!("0x{}", body.to_ascii_lowercase()),
            payload: ChainAddressPayload::Evm(decoded),
            role,
            rule: ChainAddressRule::EvmHex20Eip55,
            bitcoin_network: None,
            bitcoin_address_type: None,
        })
    }

    /// Validates a case-sensitive Solana base58 value that decodes to exactly 32 bytes.
    ///
    /// # Errors
    ///
    /// Rejects invalid base58 or decoded lengths other than 32 bytes.
    pub fn try_solana(
        chain_id: SolanaChainId,
        submitted: &str,
        role: ChainAddressRole,
    ) -> Result<Self, IdentifierError> {
        if matches!(role, ChainAddressRole::TokenContract) {
            return Err(IdentifierError::InvalidAddressRole);
        }
        if submitted.is_empty() || submitted.len() > 44 {
            return Err(IdentifierError::InvalidAddress);
        }
        let mut decoded = [0_u8; 32];
        let decoded_length = bs58::decode(submitted)
            .onto(&mut decoded[..])
            .map_err(|_| IdentifierError::InvalidAddress)?;
        if decoded_length != decoded.len() {
            return Err(IdentifierError::InvalidAddress);
        }
        Ok(Self {
            chain_id: chain_id.chain_id,
            submitted: submitted.to_owned(),
            canonical: bs58::encode(decoded).into_string(),
            payload: ChainAddressPayload::Solana(decoded),
            role,
            rule: ChainAddressRule::SolanaBase58PublicKey,
            bitcoin_network: None,
            bitcoin_address_type: None,
        })
    }

    /// Validates a bounded, network-qualified Bitcoin legacy, SegWit, or Taproot address.
    ///
    /// Parsing and network validation use rust-bitcoin 0.32, whose address implementation
    /// follows BIP 13/16, BIP 173, BIP 341, and BIP 350. In particular, witness version zero
    /// requires Bech32 while witness versions one through sixteen require Bech32m.
    ///
    /// # Errors
    ///
    /// Rejects input over the BIP 173 90-character bound, invalid encodings, unsupported address
    /// families, or addresses invalid for `network`.
    pub fn try_bitcoin(
        chain_id: ChainId,
        submitted: &str,
        role: ChainAddressRole,
        network: BitcoinNetwork,
    ) -> Result<Self, IdentifierError> {
        if matches!(
            role,
            ChainAddressRole::TokenContract | ChainAddressRole::Mint
        ) {
            return Err(IdentifierError::InvalidAddressRole);
        }
        if submitted.is_empty() || submitted.len() > 90 || !submitted.is_ascii() {
            return Err(IdentifierError::InvalidAddress);
        }
        let bitcoin_network: Network = network.into();
        let genesis_hash = bitcoin::blockdata::constants::genesis_block(bitcoin_network)
            .block_hash()
            .to_string();
        let Some(expected_reference) = genesis_hash.get(..32) else {
            return Err(IdentifierError::InvalidChainId);
        };
        if chain_id.namespace() != "bip122" || chain_id.reference() != expected_reference {
            return Err(IdentifierError::InvalidChainId);
        }
        let unchecked = Address::<NetworkUnchecked>::from_str(submitted)
            .map_err(|_| IdentifierError::InvalidAddress)?;
        let checked = unchecked
            .require_network(bitcoin_network)
            .map_err(|_| IdentifierError::InvalidAddressNetwork)?;
        let address_type = checked
            .address_type()
            .ok_or(IdentifierError::InvalidAddress)
            .and_then(BitcoinAddressType::try_from)?;
        Ok(Self {
            chain_id,
            submitted: submitted.to_owned(),
            canonical: checked.to_string(),
            payload: ChainAddressPayload::BitcoinScript(checked.script_pubkey().into_bytes()),
            role,
            rule: ChainAddressRule::BitcoinAddress,
            bitcoin_network: Some(network),
            bitcoin_address_type: Some(address_type),
        })
    }

    /// Returns the explicitly supplied chain identity.
    pub const fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }

    /// Returns the source-submitted text before canonical protocol rendering.
    pub fn submitted(&self) -> &str {
        &self.submitted
    }

    /// Returns the protocol-defined canonical display retained alongside submitted text.
    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    /// Returns the losslessly decoded identity bytes.
    pub fn decoded_bytes(&self) -> &[u8] {
        match &self.payload {
            ChainAddressPayload::Evm(bytes) => bytes,
            ChainAddressPayload::Solana(bytes) => bytes,
            ChainAddressPayload::BitcoinScript(bytes) => bytes,
        }
    }

    /// Returns the fixed-width EVM address bytes when this is an EVM address.
    pub const fn evm_address_bytes(&self) -> Option<&[u8; 20]> {
        match &self.payload {
            ChainAddressPayload::Evm(bytes) => Some(bytes),
            ChainAddressPayload::Solana(_) | ChainAddressPayload::BitcoinScript(_) => None,
        }
    }

    /// Returns the fixed-width Solana public key when this is a Solana address.
    pub const fn solana_public_key(&self) -> Option<&[u8; 32]> {
        match &self.payload {
            ChainAddressPayload::Solana(bytes) => Some(bytes),
            ChainAddressPayload::Evm(_) | ChainAddressPayload::BitcoinScript(_) => None,
        }
    }

    /// Returns the explicitly selected semantic role.
    pub const fn role(&self) -> ChainAddressRole {
        self.role
    }

    /// Returns the protocol rule that performed validation.
    pub const fn rule(&self) -> ChainAddressRule {
        self.rule
    }

    /// Returns the required Bitcoin network when this is a Bitcoin address.
    pub const fn bitcoin_network(&self) -> Option<BitcoinNetwork> {
        self.bitcoin_network
    }

    /// Returns the decoded Bitcoin address family when this is a Bitcoin address.
    pub const fn bitcoin_address_type(&self) -> Option<BitcoinAddressType> {
        self.bitcoin_address_type
    }
}

impl PartialEq for ChainAddress {
    fn eq(&self, other: &Self) -> bool {
        self.chain_id == other.chain_id
            && self.payload == other.payload
            && self.role == other.role
            && self.rule == other.rule
            && self.bitcoin_network == other.bitcoin_network
            && self.bitcoin_address_type == other.bitcoin_address_type
    }
}

impl Eq for ChainAddress {}

impl Hash for ChainAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.chain_id.hash(state);
        self.payload.hash(state);
        self.role.hash(state);
        self.rule.hash(state);
        self.bitcoin_network.hash(state);
        self.bitcoin_address_type.hash(state);
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

#[derive(Serialize)]
struct ChainAddressWireRef<'a> {
    chain_id: &'a ChainId,
    submitted: &'a str,
    role: ChainAddressRole,
    rule: ChainAddressRule,
    #[serde(skip_serializing_if = "Option::is_none")]
    bitcoin_network: Option<BitcoinNetwork>,
}

impl Serialize for ChainAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ChainAddressWireRef {
            chain_id: &self.chain_id,
            submitted: &self.canonical,
            role: self.role,
            rule: self.rule,
            bitcoin_network: self.bitcoin_network,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainAddressWire {
    chain_id: ChainId,
    submitted: String,
    role: ChainAddressRole,
    rule: ChainAddressRule,
    bitcoin_network: Option<BitcoinNetwork>,
}

impl<'de> Deserialize<'de> for ChainAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ChainAddressWire::deserialize(deserializer)?;
        match (wire.rule, wire.bitcoin_network) {
            (ChainAddressRule::EvmHex20Eip55, None) => EvmChainId::try_from(wire.chain_id)
                .and_then(|chain| Self::try_evm(chain, &wire.submitted, wire.role)),
            (ChainAddressRule::SolanaBase58PublicKey, None) => {
                SolanaChainId::try_from(wire.chain_id)
                    .and_then(|chain| Self::try_solana(chain, &wire.submitted, wire.role))
            }
            (ChainAddressRule::BitcoinAddress, Some(network)) => {
                Self::try_bitcoin(wire.chain_id, &wire.submitted, wire.role, network)
            }
            _ => Err(IdentifierError::InvalidAddress),
        }
        .map_err(serde::de::Error::custom)
    }
}
