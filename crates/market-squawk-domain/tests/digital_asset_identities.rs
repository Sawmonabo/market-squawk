use market_squawk_domain::{
    BitcoinAddressType, BitcoinNetwork, ChainAddress, ChainAddressRole, ChainAddressRule, ChainId,
    CryptoPair, CryptoProductType, IdentifierError, ProviderInstrumentId, VenueId,
};

#[test]
fn caip_2_reference_uses_the_exact_final_grammar() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(ChainId::try_from("eip155:1")?.as_str(), "eip155:1");
    assert_eq!(ChainId::try_from("eip155:1")?.namespace(), "eip155");
    assert_eq!(ChainId::try_from("eip155:1")?.reference(), "1");
    assert_eq!(
        ChainId::try_from("bip122:000000000019d6689c085ae165831e93")?.as_str(),
        "bip122:000000000019d6689c085ae165831e93"
    );

    for invalid in [
        "eip155:chain.reference",
        "EIP155:1",
        "ab:1",
        "namespace9:1",
        "eip155:",
        "eip155:a/b",
        "eip155:reference:extra",
    ] {
        assert_eq!(
            ChainId::try_from(invalid),
            Err(IdentifierError::InvalidChainId)
        );
    }
    Ok(())
}

#[test]
fn crypto_pair_exposes_every_source_qualified_component() -> Result<(), Box<dyn std::error::Error>>
{
    let pair = CryptoPair::new(
        VenueId::try_from("coinbase")?,
        ProviderInstrumentId::try_from("BTC-USD")?,
        ProviderInstrumentId::try_from("BTC")?,
        ProviderInstrumentId::try_from("USD")?,
        CryptoProductType::Spot,
    )?;

    assert_eq!(pair.venue_id().as_str(), "coinbase");
    assert_eq!(pair.raw_product_id().as_str(), "BTC-USD");
    assert_eq!(pair.base_asset_id().as_str(), "BTC");
    assert_eq!(pair.quote_asset_id().as_str(), "USD");
    assert_eq!(pair.product_type(), CryptoProductType::Spot);
    assert_eq!(pair.to_string(), "BTC-USD");
    let mut set = std::collections::HashSet::new();
    assert!(set.insert(pair));
    Ok(())
}

#[test]
fn solana_decode_is_bounded_fixed_width_and_wire_authoritative()
-> Result<(), Box<dyn std::error::Error>> {
    let chain_id = ChainId::try_from("solana:mainnet")?;
    let address = ChainAddress::try_solana(
        chain_id.clone(),
        "11111111111111111111111111111111",
        ChainAddressRole::Mint,
    )?;

    assert_eq!(address.chain_id(), &chain_id);
    assert_eq!(address.submitted(), "11111111111111111111111111111111");
    assert_eq!(address.canonical(), address.submitted());
    assert_eq!(address.solana_public_key(), Some(&[0_u8; 32]));
    assert_eq!(address.role(), ChainAddressRole::Mint);
    assert_eq!(address.rule(), ChainAddressRule::SolanaBase58PublicKey);

    let encoded = serde_json::to_value(&address)?;
    assert!(encoded.get("canonical").is_none());
    assert!(encoded.get("decoded_bytes").is_none());
    assert_eq!(serde_json::from_value::<ChainAddress>(encoded)?, address);

    let oversized = "1".repeat(45);
    assert_eq!(
        ChainAddress::try_solana(chain_id, &oversized, ChainAddressRole::Account),
        Err(IdentifierError::InvalidAddress)
    );

    let tampered = serde_json::json!({
        "chain_id": "solana:mainnet",
        "submitted": "11111111111111111111111111111111",
        "role": "mint",
        "rule": "solana_base58_public_key",
        "decoded_bytes": [1, 2, 3]
    });
    assert!(serde_json::from_value::<ChainAddress>(tampered).is_err());
    Ok(())
}

#[test]
fn bitcoin_addresses_are_bounded_network_checked_and_encoding_aware()
-> Result<(), Box<dyn std::error::Error>> {
    let chain_id = ChainId::try_from("bip122:000000000019d6689c085ae165831e93")?;
    let legacy = ChainAddress::try_bitcoin(
        chain_id.clone(),
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
        ChainAddressRole::Recipient,
        BitcoinNetwork::Mainnet,
    )?;
    assert_eq!(legacy.bitcoin_network(), Some(BitcoinNetwork::Mainnet));
    assert_eq!(
        legacy.bitcoin_address_type(),
        Some(BitcoinAddressType::P2pkh)
    );
    assert!(!legacy.decoded_bytes().is_empty());

    let segwit = ChainAddress::try_bitcoin(
        chain_id.clone(),
        "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
        ChainAddressRole::Recipient,
        BitcoinNetwork::Mainnet,
    )?;
    assert_eq!(
        segwit.bitcoin_address_type(),
        Some(BitcoinAddressType::P2wpkh)
    );

    let taproot = ChainAddress::try_bitcoin(
        chain_id.clone(),
        "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0",
        ChainAddressRole::Recipient,
        BitcoinNetwork::Mainnet,
    )?;
    assert_eq!(
        taproot.bitcoin_address_type(),
        Some(BitcoinAddressType::P2tr)
    );

    assert_eq!(
        ChainAddress::try_bitcoin(
            ChainId::try_from("bip122:000000000933ea01ad0ee984209779ba")?,
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            ChainAddressRole::Recipient,
            BitcoinNetwork::Testnet,
        ),
        Err(IdentifierError::InvalidAddressNetwork)
    );
    assert_eq!(
        ChainAddress::try_bitcoin(
            ChainId::try_from("eip155:1")?,
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            ChainAddressRole::Recipient,
            BitcoinNetwork::Mainnet,
        ),
        Err(IdentifierError::InvalidChainId)
    );
    assert_eq!(
        ChainAddress::try_bitcoin(
            chain_id,
            "tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx",
            ChainAddressRole::Recipient,
            BitcoinNetwork::Testnet,
        ),
        Err(IdentifierError::InvalidChainId)
    );
    assert_eq!(
        ChainAddress::try_bitcoin(
            ChainId::try_from("bip122:000000000019d6689c085ae165831e93")?,
            &"b".repeat(91),
            ChainAddressRole::Recipient,
            BitcoinNetwork::Mainnet,
        ),
        Err(IdentifierError::InvalidAddress)
    );
    Ok(())
}
