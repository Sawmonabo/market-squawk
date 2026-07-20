use std::error::Error;
use std::num::NonZeroU16;

use market_squawk_domain::{IntegrityRule, MarketDepth, RuleVersion, SourceIdentifier};
use market_squawk_sources::{
    ChecksumAlgorithm, ChecksumBookScope, ChecksumValidationError, ChecksumValidationProfile,
    KRAKEN_V2_CANONICALIZATION_ID, KRAKEN_V2_SCOPE_ID, ProviderBookLevel, ProviderChecksumEvidence,
    ProviderDecimalLexeme, ProviderPrice, ProviderQuantity, ResolvedChecksumValidator,
    kraken_v2_crc32,
};

fn level(price: &str, quantity: &str) -> Result<ProviderBookLevel, Box<dyn Error>> {
    Ok(ProviderBookLevel::new(
        ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
        ProviderQuantity::new(ProviderDecimalLexeme::try_new(quantity)?),
    ))
}

fn rule() -> Result<IntegrityRule, Box<dyn Error>> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from("kraken-ws-v2-book-checksum")?,
        RuleVersion::new(1)?,
    ))
}

#[test]
fn official_kraken_v2_snapshot_matches_the_published_crc32() -> Result<(), Box<dyn Error>> {
    let bids = [
        level("45283.5", "0.10000000")?,
        level("45283.4", "1.54582015")?,
        level("45282.1", "0.10000000")?,
        level("45281.0", "0.10000000")?,
        level("45280.3", "1.54592586")?,
        level("45279.0", "0.07990000")?,
        level("45277.6", "0.03310103")?,
        level("45277.5", "0.30000000")?,
        level("45277.3", "1.54602737")?,
        level("45276.6", "0.15445238")?,
    ];
    let asks = [
        level("45285.2", "0.00100000")?,
        level("45286.4", "1.54571953")?,
        level("45286.6", "1.54571109")?,
        level("45289.6", "1.54560911")?,
        level("45290.2", "0.15890660")?,
        level("45291.8", "1.54553491")?,
        level("45294.7", "0.04454749")?,
        level("45296.1", "0.35380000")?,
        level("45297.5", "0.09945542")?,
        level("45299.5", "0.18772827")?,
    ];

    assert_eq!(
        kraken_v2_crc32(&asks, &bids, NonZeroU16::new(10).ok_or("zero")?)?,
        3_310_070_434
    );
    Ok(())
}

#[test]
fn exact_lexemes_preserve_trailing_zeros_and_remove_only_dot_and_leading_zeros()
-> Result<(), Box<dyn Error>> {
    let asks = [level("01.00", "0.0100")?];
    let bids = [level("0.90", "2.000")?];
    let with_trailing_zeros = kraken_v2_crc32(&asks, &bids, NonZeroU16::new(10).ok_or("zero")?)?;
    let normalized_decimals = kraken_v2_crc32(
        &[level("1", "0.01")?],
        &[level("0.9", "2")?],
        NonZeroU16::new(10).ok_or("zero")?,
    )?;

    assert_ne!(with_trailing_zeros, normalized_decimals);
    Ok(())
}

#[test]
fn closed_profile_dispatch_refuses_unknown_or_insufficient_scope() -> Result<(), Box<dyn Error>> {
    let supported = ChecksumValidationProfile::Provided {
        rule: rule()?,
        algorithm: ChecksumAlgorithm::Crc32IsoHdlc,
        canonicalization: SourceIdentifier::try_from(KRAKEN_V2_CANONICALIZATION_ID)?,
        scope: SourceIdentifier::try_from(KRAKEN_V2_SCOPE_ID)?,
        book_scope: Some(ChecksumBookScope::new(
            MarketDepth::PriceLevel,
            NonZeroU16::new(10),
        )),
    };
    assert!(ResolvedChecksumValidator::resolve(&supported, 10).is_ok());
    assert_eq!(
        ResolvedChecksumValidator::resolve(&supported, 9),
        Err(ChecksumValidationError::InsufficientRetainedDepth {
            configured: 9,
            required: 10,
        })
    );

    let unknown = ChecksumValidationProfile::Provided {
        rule: rule()?,
        algorithm: ChecksumAlgorithm::Crc32IsoHdlc,
        canonicalization: SourceIdentifier::try_from("unknown-canonicalization")?,
        scope: SourceIdentifier::try_from(KRAKEN_V2_SCOPE_ID)?,
        book_scope: Some(ChecksumBookScope::new(
            MarketDepth::PriceLevel,
            NonZeroU16::new(10),
        )),
    };
    assert_eq!(
        ResolvedChecksumValidator::resolve(&unknown, 10),
        Err(ChecksumValidationError::UnsupportedProfile)
    );
    Ok(())
}

#[test]
fn supplied_checksum_must_match_the_candidate_after_all_updates() -> Result<(), Box<dyn Error>> {
    let profile = ChecksumValidationProfile::Provided {
        rule: rule()?,
        algorithm: ChecksumAlgorithm::Crc32IsoHdlc,
        canonicalization: SourceIdentifier::try_from(KRAKEN_V2_CANONICALIZATION_ID)?,
        scope: SourceIdentifier::try_from(KRAKEN_V2_SCOPE_ID)?,
        book_scope: Some(ChecksumBookScope::new(
            MarketDepth::PriceLevel,
            NonZeroU16::new(10),
        )),
    };
    let validator = ResolvedChecksumValidator::resolve(&profile, 10)?;
    let asks = [level("101.00", "1.000")?];
    let bids = [level("100.00", "2.000")?];
    let expected = kraken_v2_crc32(&asks, &bids, NonZeroU16::new(10).ok_or("zero")?)?;

    validator.validate(
        &asks,
        &bids,
        &ProviderChecksumEvidence::Provided {
            value: SourceIdentifier::try_from(expected.to_string())?,
            rule: rule()?,
        },
    )?;
    assert!(matches!(
        validator.validate(
            &asks,
            &bids,
            &ProviderChecksumEvidence::Provided {
                value: SourceIdentifier::try_from("1")?,
                rule: rule()?,
            },
        ),
        Err(ChecksumValidationError::Mismatch { .. })
    ));
    Ok(())
}

#[test]
fn checksum_ignores_and_does_not_validate_levels_beyond_its_exact_scope()
-> Result<(), Box<dyn Error>> {
    let asks = (0..10)
        .map(|offset| level(&(101 + offset).to_string(), "1.0"))
        .collect::<Result<Vec<_>, _>>()?;
    let bids = (0..10)
        .map(|offset| level(&(100 - offset).to_string(), "2.0"))
        .collect::<Result<Vec<_>, _>>()?;
    let scoped = NonZeroU16::new(10).ok_or("zero")?;
    let expected = kraken_v2_crc32(&asks, &bids, scoped)?;
    let mut asks_with_irrelevant_tail = asks.clone();
    let mut bids_with_irrelevant_tail = bids.clone();
    // These tails deliberately violate canonical order. Kraken's fixed top-ten scope neither
    // scans nor incorporates them.
    asks_with_irrelevant_tail.extend(
        (0..20_000)
            .map(|_| level("1", "1"))
            .collect::<Result<Vec<_>, _>>()?,
    );
    bids_with_irrelevant_tail.extend(
        (0..20_000)
            .map(|_| level("999", "1"))
            .collect::<Result<Vec<_>, _>>()?,
    );

    assert_eq!(
        kraken_v2_crc32(
            &asks_with_irrelevant_tail,
            &bids_with_irrelevant_tail,
            scoped
        )?,
        expected
    );
    Ok(())
}
