//! Generic CAIP-2 and protocol-qualified chain identities.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::IdentifierError;

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

    pub(super) fn into_chain_id(self) -> ChainId {
        self.0
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

    pub(super) fn into_chain_id(self) -> ChainId {
        self.chain_id
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
