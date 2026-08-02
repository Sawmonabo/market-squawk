use std::str::FromStr;

use market_squawk_domain::{InstrumentId, VenueId};

use super::{ShardKey, v1_hash};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const INSTRUMENT_ONE: &str = "018f0000-0000-7000-8000-000000000001";
const INSTRUMENT_TWO: &str = "018f0000-0000-7000-8000-000000000002";

fn key(venue: &str, instrument: &str) -> TestResult<ShardKey> {
    Ok(ShardKey::new(
        VenueId::try_from(venue)?,
        InstrumentId::from_str(instrument)?,
    ))
}

#[test]
fn v1_full_hash_locks_domain_version_length_and_uuid_bytes() -> TestResult {
    let vectors = [
        ("coinbase", INSTRUMENT_ONE, 0x28ed_ee9c_b185_2659),
        ("coinbase", INSTRUMENT_TWO, 0x28ed_eb9c_b185_2140),
        ("a", INSTRUMENT_ONE, 0xada8_d2fd_3b2a_185b),
        ("ab", INSTRUMENT_ONE, 0x5069_3d96_557d_7dd4),
        ("é", INSTRUMENT_ONE, 0xfbac_43d0_45b3_704b),
        ("e\u{301}", INSTRUMENT_ONE, 0xace9_2c88_cffa_4fbe),
    ];

    for (venue, instrument, expected) in vectors {
        assert_eq!(v1_hash(&key(venue, instrument)?), expected);
    }
    Ok(())
}

#[test]
fn v1_hashes_utf8_as_stored_without_normalization_or_delimiter_aliasing() -> TestResult {
    assert_ne!(
        v1_hash(&key("é", INSTRUMENT_ONE)?),
        v1_hash(&key("e\u{301}", INSTRUMENT_ONE)?)
    );
    assert_ne!(
        v1_hash(&key("a", INSTRUMENT_ONE)?),
        v1_hash(&key("ab", INSTRUMENT_ONE)?)
    );
    assert_ne!(
        v1_hash(&key("coinbase", INSTRUMENT_ONE)?),
        v1_hash(&key("coinbase", INSTRUMENT_TWO)?)
    );
    Ok(())
}
