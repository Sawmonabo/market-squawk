use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use market_squawk_domain::{
    ChainAddress, ChainAddressRole, ChainId, CryptoPair, CryptoProductType, EvmChainId,
    IdentifierError, ProviderInstrumentId, SolanaChainId, SolanaNetwork, VenueId,
};

const SOLANA_MAINNET: &str = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp";

fn hash(value: &ChainAddress) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn protocol_chain_types_validate_canonical_references() -> Result<(), Box<dyn std::error::Error>> {
    let ethereum = EvmChainId::try_from("eip155:1")?;
    assert_eq!(ethereum.chain_id().as_str(), "eip155:1");
    assert_eq!(ethereum.numeric_reference(), "1");
    for invalid in ["eip155:01", "eip155:+1", "eip155:mainnet", SOLANA_MAINNET] {
        assert_eq!(
            EvmChainId::try_from(invalid),
            Err(IdentifierError::InvalidChainId)
        );
    }

    let mainnet = SolanaChainId::try_from(SOLANA_MAINNET)?;
    assert_eq!(mainnet.network(), SolanaNetwork::Mainnet);
    assert_eq!(mainnet.chain_id().as_str(), SOLANA_MAINNET);
    assert_eq!(SolanaChainId::mainnet(), mainnet);
    assert_eq!(
        serde_json::from_value::<SolanaChainId>(serde_json::to_value(&mainnet)?)?,
        mainnet
    );
    for invalid in [
        "solana:mainnet",
        "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdq",
        "eip155:1",
    ] {
        assert_eq!(
            SolanaChainId::try_from(invalid),
            Err(IdentifierError::InvalidChainId)
        );
    }

    // Generic CAIP-2 remains grammar-only and does not imply protocol qualification.
    assert_eq!(
        ChainId::try_from("solana:mainnet")?.as_str(),
        "solana:mainnet"
    );
    Ok(())
}

#[test]
fn evm_identity_uses_decoded_bytes_for_equality_hash_and_canonical_wire()
-> Result<(), Box<dyn std::error::Error>> {
    let chain = EvmChainId::try_from("eip155:1")?;
    let checksummed = ChainAddress::try_evm(
        chain.clone(),
        "0x52908400098527886E0F7030069857D2E4169EE7",
        ChainAddressRole::TokenContract,
    )?;
    let lowercase = ChainAddress::try_evm(
        chain,
        "0x52908400098527886e0f7030069857d2e4169ee7",
        ChainAddressRole::TokenContract,
    )?;
    assert_eq!(checksummed, lowercase);
    assert_eq!(hash(&checksummed), hash(&lowercase));
    assert_ne!(checksummed.submitted(), lowercase.submitted());
    let wire = serde_json::to_value(&checksummed)?;
    assert_eq!(
        wire["submitted"],
        "0x52908400098527886e0f7030069857d2e4169ee7"
    );
    assert_eq!(serde_json::from_value::<ChainAddress>(wire)?, lowercase);
    Ok(())
}

#[test]
fn protocol_role_matrix_rejects_cross_protocol_semantics() -> Result<(), Box<dyn std::error::Error>>
{
    let evm = EvmChainId::try_from("eip155:1")?;
    assert_eq!(
        ChainAddress::try_evm(
            evm,
            "0x52908400098527886e0f7030069857d2e4169ee7",
            ChainAddressRole::Mint,
        ),
        Err(IdentifierError::InvalidAddressRole)
    );
    let solana = SolanaChainId::mainnet();
    assert_eq!(
        ChainAddress::try_solana(
            solana.clone(),
            "11111111111111111111111111111111",
            ChainAddressRole::TokenContract,
        ),
        Err(IdentifierError::InvalidAddressRole)
    );
    assert!(
        ChainAddress::try_solana(
            solana,
            "11111111111111111111111111111111",
            ChainAddressRole::Mint,
        )
        .is_ok()
    );
    Ok(())
}

#[test]
fn chain_and_pair_wires_reject_unknown_or_cross_protocol_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let tampered = serde_json::json!({
        "chain_id": SOLANA_MAINNET,
        "submitted": "11111111111111111111111111111111",
        "role": "mint",
        "rule": "evm_hex20_eip55",
        "bitcoin_network": null
    });
    assert!(serde_json::from_value::<ChainAddress>(tampered).is_err());

    let pair = CryptoPair::new(
        VenueId::try_from("coinbase")?,
        ProviderInstrumentId::try_from("BTC-USD")?,
        ProviderInstrumentId::try_from("BTC")?,
        ProviderInstrumentId::try_from("USD")?,
        CryptoProductType::Spot,
    )?;
    let mut pair_wire = serde_json::to_value(pair)?;
    pair_wire["inferred_symbol"] = serde_json::json!("BTCUSD");
    assert!(serde_json::from_value::<CryptoPair>(pair_wire).is_err());
    Ok(())
}
