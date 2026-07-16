use std::str::FromStr;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, Currency, Denomination, InstrumentDefinition, InstrumentDefinitionInput,
    InstrumentId, LotSize, TickSize, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigError,
    LiveRuntimeConfigInput, ShardCount, ShardId, ShardKey, ShardRouter, ShardRoutingError,
    ShardRoutingVersion, SnapshotLimits,
};
use rust_decimal::Decimal;

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

fn definition(instrument_id: &str, venue: &str) -> TestResult<InstrumentDefinition> {
    Ok(InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument(instrument_id)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from(venue)?,
            VenueSymbol::try_from("BTC-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}

fn route_config(
    venue: &str,
    instrument_id: &str,
    depth: usize,
    nonce_capacity: usize,
    nonce_reclaim_budget: usize,
) -> TestResult<LiveRouteConfig> {
    Ok(LiveRouteConfig::try_new(LiveRouteConfigInput {
        route: key(venue, instrument_id)?,
        definition: definition(instrument_id, venue)?,
        depth: DepthLimit::new(depth)?,
        nonce_capacity,
        nonce_reclaim_budget,
        maximum_capability_lifetime: Duration::from_secs(1),
    })?)
}

fn runtime_input() -> TestResult<LiveRuntimeConfigInput> {
    Ok(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: 2,
        mailbox_count_per_shard: 64,
        mailbox_bytes_per_shard: 1_048_576,
        maximum_message_bytes: 262_144,
        maximum_routes_per_shard: 8,
        maximum_sources_per_route: 8,
        registration_control_capacity: 8,
        registration_deadline: Duration::from_secs(1),
        health_event_capacity: 64,
        snapshot_event_budget: 128,
        snapshot_interval: Duration::from_millis(100),
        snapshot_limits: SnapshotLimits::try_new(8, 8, 8, 100, 1_048_576)?,
        maximum_retained_snapshot_readers: 4,
        shutdown_deadline: Duration::from_secs(5),
        maximum_runtime_bytes: 256 * 1024 * 1024,
    })
}

fn memory_input(maximum_runtime_bytes: u64) -> TestResult<LiveRuntimeConfigInput> {
    Ok(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: 2,
        mailbox_count_per_shard: 4,
        mailbox_bytes_per_shard: 1_024,
        maximum_message_bytes: 512,
        maximum_routes_per_shard: 1,
        maximum_sources_per_route: 2,
        registration_control_capacity: 2,
        registration_deadline: Duration::from_secs(1),
        health_event_capacity: 4,
        snapshot_event_budget: 8,
        snapshot_interval: Duration::from_millis(10),
        snapshot_limits: SnapshotLimits::try_new(1, 2, 2, 4, 4_096)?,
        maximum_retained_snapshot_readers: 2,
        shutdown_deadline: Duration::from_secs(1),
        maximum_runtime_bytes,
    })
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

#[test]
fn runtime_config_rejects_every_zero_capacity_and_duration() -> TestResult {
    let mut input = runtime_input()?;
    input.shard_count = 0;
    assert!(matches!(
        LiveRuntimeConfig::try_new(input),
        Err(LiveRuntimeConfigError::Routing(
            ShardRoutingError::ZeroShardCount
        ))
    ));

    macro_rules! zero_capacity {
        ($field:ident) => {{
            let mut input = runtime_input()?;
            input.$field = 0;
            assert!(matches!(
                LiveRuntimeConfig::try_new(input),
                Err(LiveRuntimeConfigError::ZeroCapacity { field })
                    if field == stringify!($field)
            ));
        }};
    }
    zero_capacity!(mailbox_count_per_shard);
    zero_capacity!(mailbox_bytes_per_shard);
    zero_capacity!(maximum_message_bytes);
    zero_capacity!(maximum_routes_per_shard);
    zero_capacity!(maximum_sources_per_route);
    zero_capacity!(registration_control_capacity);
    zero_capacity!(health_event_capacity);
    zero_capacity!(snapshot_event_budget);
    zero_capacity!(maximum_retained_snapshot_readers);
    zero_capacity!(maximum_runtime_bytes);

    macro_rules! zero_duration {
        ($field:ident) => {{
            let mut input = runtime_input()?;
            input.$field = Duration::ZERO;
            assert!(matches!(
                LiveRuntimeConfig::try_new(input),
                Err(LiveRuntimeConfigError::ZeroDuration { field })
                    if field == stringify!($field)
            ));
        }};
    }
    zero_duration!(registration_deadline);
    zero_duration!(snapshot_interval);
    zero_duration!(shutdown_deadline);
    Ok(())
}

#[test]
fn runtime_config_enforces_exact_public_hard_limits() -> TestResult {
    macro_rules! hard_limit {
        ($field:ident, $maximum:expr) => {{
            let mut exact = runtime_input()?;
            exact.$field = $maximum;
            LiveRuntimeConfig::try_new(exact)?;

            let mut over = runtime_input()?;
            over.$field = $maximum + 1;
            assert!(matches!(
                LiveRuntimeConfig::try_new(over),
                Err(LiveRuntimeConfigError::CapacityExceedsHardLimit {
                    field,
                    value,
                    maximum,
                }) if field == stringify!($field)
                    && value == ($maximum + 1) as u64
                    && maximum == $maximum as u64
            ));
        }};
    }
    hard_limit!(shard_count, 64);
    hard_limit!(mailbox_count_per_shard, 1_000_000);
    hard_limit!(maximum_routes_per_shard, 64);
    hard_limit!(maximum_sources_per_route, 64);
    hard_limit!(registration_control_capacity, 65_536);
    hard_limit!(health_event_capacity, 65_536);
    hard_limit!(snapshot_event_budget, 1_000_000);

    let mut exact_permits = runtime_input()?;
    exact_permits.mailbox_bytes_per_shard = u32::MAX;
    exact_permits.maximum_message_bytes = u32::MAX;
    exact_permits.maximum_retained_snapshot_readers = u32::MAX;
    let exact_permits = LiveRuntimeConfig::try_new(exact_permits)?;
    assert_eq!(exact_permits.mailbox_bytes_per_shard().get(), u32::MAX);
    assert_eq!(exact_permits.maximum_message_bytes().get(), u32::MAX);
    assert_eq!(
        exact_permits.maximum_retained_snapshot_readers().get(),
        u32::MAX
    );
    Ok(())
}

#[test]
fn runtime_config_accepts_exact_byte_limit_and_rejects_one_over_mailbox() -> TestResult {
    let mut exact = runtime_input()?;
    exact.mailbox_bytes_per_shard = 100;
    exact.maximum_message_bytes = 100;
    let exact = LiveRuntimeConfig::try_new(exact)?;
    assert_eq!(exact.mailbox_bytes_per_shard().get(), 100);
    assert_eq!(exact.maximum_message_bytes().get(), 100);

    let mut over = runtime_input()?;
    over.mailbox_bytes_per_shard = 100;
    over.maximum_message_bytes = 101;
    assert!(matches!(
        LiveRuntimeConfig::try_new(over),
        Err(LiveRuntimeConfigError::MessageExceedsMailbox {
            message: 101,
            mailbox: 100,
        })
    ));
    Ok(())
}

#[test]
fn route_config_rejects_identity_venue_and_processor_boundaries() -> TestResult {
    let valid_definition = definition(INSTRUMENT_ONE, "coinbase")?;
    assert!(matches!(
        LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: key("coinbase", INSTRUMENT_TWO)?,
            definition: valid_definition.clone(),
            depth: DepthLimit::new(4)?,
            nonce_capacity: 1,
            nonce_reclaim_budget: 1,
            maximum_capability_lifetime: Duration::from_secs(1),
        }),
        Err(LiveRuntimeConfigError::RouteInstrumentMismatch)
    ));
    assert!(matches!(
        LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: key("kraken", INSTRUMENT_ONE)?,
            definition: valid_definition,
            depth: DepthLimit::new(4)?,
            nonce_capacity: 1,
            nonce_reclaim_budget: 1,
            maximum_capability_lifetime: Duration::from_secs(1),
        }),
        Err(LiveRuntimeConfigError::RouteVenueMismatch)
    ));

    for (nonce_capacity, nonce_reclaim_budget, expected_field) in
        [(0, 1, "nonce_capacity"), (1, 0, "nonce_reclaim_budget")]
    {
        assert!(matches!(
            LiveRouteConfig::try_new(LiveRouteConfigInput {
                route: key("coinbase", INSTRUMENT_ONE)?,
                definition: definition(INSTRUMENT_ONE, "coinbase")?,
                depth: DepthLimit::new(4)?,
                nonce_capacity,
                nonce_reclaim_budget,
                maximum_capability_lifetime: Duration::from_secs(1),
            }),
            Err(LiveRuntimeConfigError::ZeroCapacity { field }) if field == expected_field
        ));
    }

    let exact = route_config("coinbase", INSTRUMENT_ONE, 4, 1_000_000, 1_000_000)?;
    assert_eq!(exact.nonce_capacity().get(), 1_000_000);
    assert_eq!(exact.nonce_reclaim_budget().get(), 1_000_000);
    assert!(matches!(
        LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: key("coinbase", INSTRUMENT_ONE)?,
            definition: definition(INSTRUMENT_ONE, "coinbase")?,
            depth: DepthLimit::new(4)?,
            nonce_capacity: 1_000_001,
            nonce_reclaim_budget: 1,
            maximum_capability_lifetime: Duration::from_secs(1),
        }),
        Err(LiveRuntimeConfigError::CapacityExceedsHardLimit {
            field: "nonce_capacity",
            value: 1_000_001,
            maximum: 1_000_000,
        })
    ));
    assert!(matches!(
        LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: key("coinbase", INSTRUMENT_ONE)?,
            definition: definition(INSTRUMENT_ONE, "coinbase")?,
            depth: DepthLimit::new(4)?,
            nonce_capacity: 1,
            nonce_reclaim_budget: 1_000_001,
            maximum_capability_lifetime: Duration::from_secs(1),
        }),
        Err(LiveRuntimeConfigError::CapacityExceedsHardLimit {
            field: "nonce_reclaim_budget",
            value: 1_000_001,
            maximum: 1_000_000,
        })
    ));
    assert!(matches!(
        LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: key("coinbase", INSTRUMENT_ONE)?,
            definition: definition(INSTRUMENT_ONE, "coinbase")?,
            depth: DepthLimit::new(4)?,
            nonce_capacity: 1,
            nonce_reclaim_budget: 1,
            maximum_capability_lifetime: Duration::ZERO,
        }),
        Err(LiveRuntimeConfigError::ZeroDuration {
            field: "maximum_capability_lifetime",
        })
    ));
    Ok(())
}

#[test]
fn route_validation_rejects_duplicates_and_per_shard_overflow() -> TestResult {
    let route_one = route_config("coinbase", INSTRUMENT_ONE, 4, 8, 1)?;
    let route_two = route_config("coinbase", INSTRUMENT_TWO, 4, 8, 1)?;

    let mut duplicate_input = runtime_input()?;
    duplicate_input.shard_count = 1;
    duplicate_input.maximum_routes_per_shard = 2;
    let duplicate_config = LiveRuntimeConfig::try_new(duplicate_input)?;
    assert!(matches!(
        duplicate_config.validate_routes(&[route_one.clone(), route_one.clone()]),
        Err(LiveRuntimeConfigError::DuplicateRoute)
    ));

    let mut full_input = runtime_input()?;
    full_input.shard_count = 1;
    full_input.maximum_routes_per_shard = 1;
    let full_config = LiveRuntimeConfig::try_new(full_input)?;
    assert!(matches!(
        full_config.validate_routes(&[route_one, route_two]),
        Err(LiveRuntimeConfigError::TooManyRoutesForShard {
            shard: 0,
            count: 2,
            maximum: 1,
        })
    ));
    Ok(())
}

#[test]
fn estimated_peak_bytes_matches_golden_and_exact_ceiling_boundary() -> TestResult {
    const EXPECTED_PEAK_BYTES: u64 = 215_680;
    let routes = [route_config("coinbase", INSTRUMENT_ONE, 4, 8, 1)?];

    let config = LiveRuntimeConfig::try_new(memory_input(EXPECTED_PEAK_BYTES)?)?;
    assert_eq!(
        config.estimated_peak_bytes(&routes)?.get(),
        EXPECTED_PEAK_BYTES
    );

    let below = LiveRuntimeConfig::try_new(memory_input(EXPECTED_PEAK_BYTES - 1)?)?;
    assert!(matches!(
        below.estimated_peak_bytes(&routes),
        Err(LiveRuntimeConfigError::PeakMemoryExceedsCeiling {
            estimated: EXPECTED_PEAK_BYTES,
            ceiling,
        }) if ceiling == EXPECTED_PEAK_BYTES - 1
    ));
    Ok(())
}
