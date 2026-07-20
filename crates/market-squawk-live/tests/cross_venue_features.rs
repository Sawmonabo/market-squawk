use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{ExactFeatureRatio, FeatureValidity};
use market_squawk_domain::{ConnectionGeneration, InstrumentId, Timestamp, VenueId};
use market_squawk_live::{CrossVenueFeatureError, CrossVenueFeatureHub, CrossVenueUpdate};
use std::str::FromStr;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn coalesces_by_venue_and_publishes_a_complete_deterministic_snapshot() -> TestResult {
    let instrument = InstrumentId::from_str("00000000-0000-0000-0000-000000000007")?;
    let coinbase = VenueId::try_from("coinbase")?;
    let kraken = VenueId::try_from("kraken")?;
    let mut hub = create_hub(4, 2, 4, 4096)?;
    hub.try_register(instrument, &[kraken.clone(), coinbase.clone()])?;

    hub.try_publish(update(instrument, coinbase, 1, 200, 10)?)?;
    hub.try_publish(update(instrument, kraken.clone(), 1, 202, 10)?)?;
    hub.try_publish(update(instrument, kraken, 1, 204, 11)?)?;
    assert_eq!(hub.pending_command_count(), 2);
    assert_eq!(hub.drain()?, 2);

    let snapshot = hub.snapshot(instrument, Timestamp::from_unix_nanos(11))?;
    assert_eq!(snapshot.validity(), FeatureValidity::Ready);
    assert_eq!(snapshot.venues()[0].venue().as_str(), "coinbase");
    assert_eq!(snapshot.venues()[1].venue().as_str(), "kraken");
    assert_eq!(
        snapshot
            .divergence()
            .map(|value| (value.numerator(), value.denominator().get())),
        Some((200, 1))
    );
    assert_eq!(
        hub.snapshot(instrument, Timestamp::from_unix_nanos(1_012))?
            .validity(),
        FeatureValidity::Stale
    );
    Ok(())
}

#[test]
fn saturation_and_generation_replacement_fail_closed_without_partial_coverage() -> TestResult {
    let instrument = InstrumentId::from_str("00000000-0000-0000-0000-000000000008")?;
    let coinbase = VenueId::try_from("coinbase")?;
    let kraken = VenueId::try_from("kraken")?;
    let mut hub = create_hub(1, 2, 1, std::mem::size_of::<CrossVenueUpdate>())?;
    hub.try_register(instrument, &[coinbase.clone(), kraken.clone()])?;

    hub.try_publish(update(instrument, coinbase.clone(), 1, 200, 10)?)?;
    assert_eq!(
        hub.try_publish(update(instrument, kraken.clone(), 1, 202, 10)?),
        Err(CrossVenueFeatureError::CommandCapacityFull)
    );
    assert_eq!(
        hub.snapshot(instrument, Timestamp::from_unix_nanos(10))?
            .validity(),
        FeatureValidity::Unavailable
    );
    hub.drain()?;
    assert_eq!(
        hub.snapshot(instrument, Timestamp::from_unix_nanos(10))?
            .validity(),
        FeatureValidity::Unavailable
    );

    hub.try_publish(update(instrument, coinbase, 2, 204, 20)?)?;
    hub.drain()?;
    assert_eq!(
        hub.snapshot(instrument, Timestamp::from_unix_nanos(20))?
            .validity(),
        FeatureValidity::Unavailable
    );
    hub.try_publish(update(instrument, kraken, 2, 206, 20)?)?;
    hub.drain()?;
    assert_eq!(
        hub.snapshot(instrument, Timestamp::from_unix_nanos(20))?
            .validity(),
        FeatureValidity::Ready
    );

    hub.try_publish(update(
        instrument,
        VenueId::try_from("coinbase")?,
        2,
        208,
        30,
    )?)?;
    assert_eq!(
        hub.try_publish(update(
            instrument,
            VenueId::try_from("kraken")?,
            2,
            210,
            30,
        )?),
        Err(CrossVenueFeatureError::CommandCapacityFull)
    );
    hub.drain()?;
    assert_eq!(
        hub.snapshot(instrument, Timestamp::from_unix_nanos(30))?
            .validity(),
        FeatureValidity::Unavailable
    );
    hub.try_publish(update(
        instrument,
        VenueId::try_from("kraken")?,
        2,
        210,
        30,
    )?)?;
    hub.drain()?;
    assert_eq!(
        hub.snapshot(instrument, Timestamp::from_unix_nanos(30))?
            .validity(),
        FeatureValidity::Ready
    );

    let mut byte_limited = create_hub(1, 2, 2, 1)?;
    byte_limited.try_register(
        instrument,
        &[VenueId::try_from("coinbase")?, VenueId::try_from("kraken")?],
    )?;
    assert_eq!(
        byte_limited.try_publish(update(
            instrument,
            VenueId::try_from("coinbase")?,
            1,
            200,
            30,
        )?),
        Err(CrossVenueFeatureError::CommandCapacityFull)
    );
    Ok(())
}

fn create_hub(
    instruments: usize,
    venues: usize,
    commands: usize,
    command_bytes: usize,
) -> Result<CrossVenueFeatureHub, CrossVenueFeatureError> {
    CrossVenueFeatureHub::try_new(
        NonZeroUsize::new(instruments).ok_or(CrossVenueFeatureError::ZeroCapacity)?,
        NonZeroUsize::new(venues).ok_or(CrossVenueFeatureError::ZeroCapacity)?,
        NonZeroUsize::new(commands).ok_or(CrossVenueFeatureError::ZeroCapacity)?,
        NonZeroUsize::new(command_bytes).ok_or(CrossVenueFeatureError::ZeroCapacity)?,
        NonZeroU64::new(1_000).ok_or(CrossVenueFeatureError::ZeroCapacity)?,
    )
}

fn update(
    instrument: InstrumentId,
    venue: VenueId,
    generation: u64,
    midpoint_numerator: i128,
    observed_at: i64,
) -> TestResult<CrossVenueUpdate> {
    Ok(CrossVenueUpdate::new(
        instrument,
        venue,
        ConnectionGeneration::new(generation)?,
        ExactFeatureRatio::try_new(midpoint_numerator, 2)?,
        Timestamp::from_unix_nanos(observed_at),
    ))
}
