use std::str::FromStr;

use market_squawk_domain::{InstrumentId, VenueId};
use market_squawk_live::{
    ShardCount, ShardId, ShardKey, ShardRouter, ShardRoutingError, ShardRoutingVersion,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const INSTRUMENT_ONE: &str = "018f0000-0000-7000-8000-000000000001";
const INSTRUMENT_TWO: &str = "018f0000-0000-7000-8000-000000000002";

fn instrument(value: &str) -> TestResult<InstrumentId> {
    Ok(InstrumentId::from_str(value)?)
}

fn key(venue: &str, instrument_id: &str) -> TestResult<ShardKey> {
    Ok(ShardKey::new(
        VenueId::try_from(venue)?,
        instrument(instrument_id)?,
    ))
}

#[test]
fn routing_v1_matches_frozen_cross_process_vector() -> TestResult {
    let route = key("coinbase", INSTRUMENT_ONE)?;
    let router = ShardRouter::v1(16)?;

    assert_eq!(router.version(), ShardRoutingVersion::V1);
    assert_eq!(router.count(), ShardCount::new(16)?);
    assert_eq!(router.route(&route), ShardId::new(9, 16)?);
    assert_eq!(route.venue().as_str(), "coinbase");
    assert_eq!(route.instrument(), instrument(INSTRUMENT_ONE)?);
    Ok(())
}

#[test]
fn routing_v1_matches_golden_indices_across_counts() -> TestResult {
    let route = key("coinbase", INSTRUMENT_ONE)?;
    let vectors = [
        (1_u16, 0_u16),
        (2, 1),
        (3, 1),
        (16, 9),
        (257, 122),
        (u16::MAX, 61_288),
    ];

    for (count, expected_index) in vectors {
        let router = ShardRouter::v1(count)?;
        assert_eq!(router.route(&route), ShardId::new(expected_index, count)?);
        assert!(router.route(&route).index() < count);
    }
    Ok(())
}

#[test]
fn routing_v1_uses_utf8_byte_length_without_unicode_normalization() -> TestResult {
    let composed = key("é", INSTRUMENT_ONE)?;
    let decomposed = key("e\u{301}", INSTRUMENT_ONE)?;
    let router = ShardRouter::v1(u16::MAX)?;

    assert_eq!(composed.venue().as_str().as_bytes(), &[0xc3, 0xa9]);
    assert_eq!(decomposed.venue().as_str().as_bytes(), &[0x65, 0xcc, 0x81]);
    assert_eq!(router.route(&composed), ShardId::new(62_843, u16::MAX)?);
    assert_eq!(router.route(&decomposed), ShardId::new(63_786, u16::MAX)?);
    assert_ne!(router.route(&composed), router.route(&decomposed));
    Ok(())
}

#[test]
fn routing_v1_length_prefix_separates_delimiter_ambiguous_venues() -> TestResult {
    let short = key("a", INSTRUMENT_ONE)?;
    let extended = key("ab", INSTRUMENT_ONE)?;
    let router = ShardRouter::v1(u16::MAX)?;

    assert_eq!(router.route(&short), ShardId::new(54_315, u16::MAX)?);
    assert_eq!(router.route(&extended), ShardId::new(24_913, u16::MAX)?);
    assert_ne!(router.route(&short), router.route(&extended));
    Ok(())
}

#[test]
fn routing_v1_uses_uuid_network_bytes_not_display_text() -> TestResult {
    let first = key("coinbase", INSTRUMENT_ONE)?;
    let second = key("coinbase", INSTRUMENT_TWO)?;
    let router = ShardRouter::v1(u16::MAX)?;

    assert_eq!(router.route(&first), ShardId::new(61_288, u16::MAX)?);
    assert_eq!(router.route(&second), ShardId::new(59_215, u16::MAX)?);
    assert_ne!(router.route(&first), router.route(&second));
    Ok(())
}

#[test]
fn routing_rejects_zero_and_preserves_maximum_cardinality() -> TestResult {
    assert!(matches!(
        ShardRouter::v1(0),
        Err(ShardRoutingError::ZeroShardCount)
    ));
    assert_eq!(ShardCount::new(0), Err(ShardRoutingError::ZeroShardCount));
    assert_eq!(ShardId::new(0, 0), Err(ShardRoutingError::ZeroShardCount));
    assert_eq!(ShardCount::new(u16::MAX)?.get(), u16::MAX);
    assert_eq!(ShardId::new(u16::MAX - 1, u16::MAX)?.index(), u16::MAX - 1);
    Ok(())
}

#[test]
fn shard_id_constructor_and_deserializer_preserve_index_bound() -> TestResult {
    assert_eq!(
        ShardId::new(16, 16),
        Err(ShardRoutingError::IndexOutOfRange {
            index: 16,
            count: 16,
        })
    );
    assert!(serde_json::from_str::<ShardId>(r#"{"index":16,"count":16}"#).is_err());
    assert!(serde_json::from_str::<ShardId>(r#"{"index":0,"count":0}"#).is_err());
    assert!(serde_json::from_str::<ShardId>(r#"{"index":0,"count":1,"extra":true}"#).is_err());

    let shard = ShardId::new(9, 16)?;
    let encoded = serde_json::to_string(&shard)?;
    assert_eq!(serde_json::from_str::<ShardId>(&encoded)?, shard);
    Ok(())
}

#[test]
fn routing_types_round_trip_without_relaxing_identity_invariants() -> TestResult {
    let route = key("coinbase", INSTRUMENT_ONE)?;
    let route_json = serde_json::to_string(&route)?;
    assert_eq!(serde_json::from_str::<ShardKey>(&route_json)?, route);
    assert!(
        serde_json::from_str::<ShardKey>(
            r#"{"venue":"coinbase","instrument":"00000000-0000-0000-0000-000000000000"}"#,
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<ShardKey>(
            r#"{"venue":"coin base","instrument":"018f0000-0000-7000-8000-000000000001"}"#,
        )
        .is_err()
    );
    assert!(serde_json::from_str::<ShardCount>("0").is_err());

    let version_json = serde_json::to_string(&ShardRoutingVersion::V1)?;
    assert_eq!(version_json, r#""v1""#);
    assert_eq!(
        serde_json::from_str::<ShardRoutingVersion>(&version_json)?,
        ShardRoutingVersion::V1
    );
    assert!(serde_json::from_str::<ShardRoutingVersion>(r#""v2""#).is_err());
    Ok(())
}

#[test]
fn routing_is_repeatable_for_every_valid_sampled_count() -> TestResult {
    let routes = [
        key("coinbase", INSTRUMENT_ONE)?,
        key("coinbase", INSTRUMENT_TWO)?,
        key("é", INSTRUMENT_ONE)?,
        key("e\u{301}", INSTRUMENT_ONE)?,
    ];
    let counts = [1_u16, 2, 3, 16, 257, 4_096, u16::MAX];

    for count in counts {
        let first = ShardRouter::v1(count)?;
        let second = ShardRouter::v1(count)?;
        for route in &routes {
            let expected = first.route(route);
            assert_eq!(second.route(route), expected);
            assert_eq!(expected.count(), ShardCount::new(count)?);
            assert!(expected.index() < count);
        }
    }
    Ok(())
}
