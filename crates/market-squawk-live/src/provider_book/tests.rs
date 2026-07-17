use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use market_squawk_domain::{LotSize, TickSize};
use market_squawk_sources::{
    ProviderBookChange, ProviderBookLevel, ProviderBookSide, ProviderDecimalLexeme, ProviderPrice,
    ProviderQuantity,
};
use proptest::prelude::*;
use rust_decimal::Decimal;

use super::{
    BookProcessingScratch, ProviderBook, exact_level_arc_allocation_bytes,
    maximum_book_items_for_message,
};
use crate::DepthLimit;

fn level(price: &str, quantity: &str) -> Result<ProviderBookLevel, Box<dyn std::error::Error>> {
    Ok(ProviderBookLevel::new(
        ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
        ProviderQuantity::new(ProviderDecimalLexeme::try_new(quantity)?),
    ))
}

#[test]
fn dropped_candidate_preserves_active_state_and_reuses_inactive_buffers()
-> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::ONE)?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;
    let mut scratch = BookProcessingScratch::try_new(8)?;
    let mut book = ProviderBook::try_new(DepthLimit::new(4)?)?;
    book.replace_snapshot(
        &[level("100", "2")?],
        &[level("101", "3")?],
        tick,
        lot,
        None,
        &mut scratch,
    )?;
    let before = book.scaled_bid_iter().collect::<Vec<_>>();
    {
        let transaction = book.begin_delta(
            &[ProviderBookChange::new(
                ProviderBookSide::Bid,
                level("100", "9")?,
            )],
            tick,
            lot,
            None,
            &mut scratch,
        )?;
        assert_ne!(
            transaction
                .candidate()
                .scaled_bid_iter()
                .collect::<Vec<_>>(),
            before
        );
    }
    assert_eq!(book.scaled_bid_iter().collect::<Vec<_>>(), before);
    Ok(())
}

#[test]
fn repeated_delta_price_uses_last_wire_value_before_linear_merge()
-> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::ONE)?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;
    let mut scratch = BookProcessingScratch::try_new(8)?;
    let mut book = ProviderBook::try_new(DepthLimit::new(4)?)?;
    book.replace_snapshot(
        &[level("100", "2")?],
        &[level("101", "3")?],
        tick,
        lot,
        None,
        &mut scratch,
    )?;
    book.begin_delta(
        &[
            ProviderBookChange::new(ProviderBookSide::Bid, level("100", "4")?),
            ProviderBookChange::new(ProviderBookSide::Bid, level("100", "7")?),
        ],
        tick,
        lot,
        None,
        &mut scratch,
    )?
    .commit();
    assert_eq!(
        book.scaled_bid_iter().next().ok_or("missing bid")?.1.get(),
        7
    );
    Ok(())
}

#[test]
fn canonical_delta_preserves_wire_order_and_has_no_spare_logical_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::ONE)?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;
    let mut scratch = BookProcessingScratch::try_new(8)?;
    let mut book = ProviderBook::try_new(DepthLimit::new(4)?)?;
    book.replace_snapshot(
        &[level("100", "2")?, level("99", "3")?],
        &[level("101", "4")?, level("102", "5")?],
        tick,
        lot,
        None,
        &mut scratch,
    )?;
    let transaction = book.begin_delta(
        &[
            ProviderBookChange::new(ProviderBookSide::Ask, level("102", "9")?),
            ProviderBookChange::new(ProviderBookSide::Bid, level("99", "7")?),
            ProviderBookChange::new(ProviderBookSide::Ask, level("102", "11")?),
        ],
        tick,
        lot,
        None,
        &mut scratch,
    )?;
    let changes = transaction.normalized_changes()?;
    assert_eq!(changes.capacity(), changes.len());
    assert_eq!(
        changes
            .iter()
            .map(|change| (change.side(), change.price().get(), change.quantity().get()))
            .collect::<Vec<_>>(),
        [
            (market_squawk_domain::MarketSide::Ask, 102, 9),
            (market_squawk_domain::MarketSide::Bid, 99, 7),
            (market_squawk_domain::MarketSide::Ask, 102, 11),
        ]
    );
    transaction.commit();
    assert_eq!(
        book.scaled_ask_iter()
            .find(|(price, _)| price.get() == 102)
            .ok_or("missing updated ask")?
            .1
            .get(),
        11
    );
    Ok(())
}

#[test]
fn dropped_snapshot_candidate_preserves_the_active_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::ONE)?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;
    let mut scratch = BookProcessingScratch::try_new(8)?;
    let mut book = ProviderBook::try_new(DepthLimit::new(4)?)?;
    book.replace_snapshot(
        &[level("100", "2")?],
        &[level("101", "3")?],
        tick,
        lot,
        None,
        &mut scratch,
    )?;
    let before_bids = book.scaled_bid_iter().collect::<Vec<_>>();
    let before_asks = book.scaled_ask_iter().collect::<Vec<_>>();
    {
        let transaction = book.begin_snapshot(
            &[level("90", "4")?],
            &[level("91", "5")?],
            tick,
            lot,
            None,
            &mut scratch,
        )?;
        let candidate_bids = transaction.candidate().bid_levels()?;
        assert_eq!(candidate_bids.capacity(), candidate_bids.len());
    }
    assert_eq!(book.scaled_bid_iter().collect::<Vec<_>>(), before_bids);
    assert_eq!(book.scaled_ask_iter().collect::<Vec<_>>(), before_asks);
    Ok(())
}

#[test]
fn all_shards_observe_maximum_delta_shape_below_the_structural_peak()
-> Result<(), Box<dyn std::error::Error>> {
    const SHARDS: usize = 4;
    const DEPTH: usize = 64;
    const MAXIMUM_MESSAGE_BYTES: u32 = 64 * 1024;

    let maximum_items = maximum_book_items_for_message(MAXIMUM_MESSAGE_BYTES);
    let bids = (0..DEPTH)
        .map(|offset| level(&(10_000 - offset).to_string(), "1"))
        .collect::<Result<Vec<_>, _>>()?;
    let asks = (0..DEPTH)
        .map(|offset| level(&(20_000 + offset).to_string(), "1"))
        .collect::<Result<Vec<_>, _>>()?;
    let changes = (0..maximum_items)
        .map(|ordinal| {
            let side_offset = ordinal % (DEPTH * 2);
            if side_offset < DEPTH {
                Ok(ProviderBookChange::new(
                    ProviderBookSide::Bid,
                    level(&(10_000 - side_offset).to_string(), "2")?,
                ))
            } else {
                Ok(ProviderBookChange::new(
                    ProviderBookSide::Ask,
                    level(&(20_000 + side_offset - DEPTH).to_string(), "2")?,
                ))
            }
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let bids = Arc::<[ProviderBookLevel]>::from(bids);
    let asks = Arc::<[ProviderBookLevel]>::from(asks);
    let changes = Arc::<[ProviderBookChange]>::from(changes);
    let barrier = Arc::new(Barrier::new(SHARDS));
    let tick = TickSize::try_from_decimal(Decimal::ONE)?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;
    let structural = crate::runtime::book_processing_peak(MAXIMUM_MESSAGE_BYTES, DEPTH)?;

    let observations = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..SHARDS {
            let bids = Arc::clone(&bids);
            let asks = Arc::clone(&asks);
            let changes = Arc::clone(&changes);
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                let mut scratch = BookProcessingScratch::try_new(maximum_items)?;
                let mut book = ProviderBook::try_new(DepthLimit::new(DEPTH)?)?;
                book.replace_snapshot(&bids, &asks, tick, lot, None, &mut scratch)?;
                let transaction = book.begin_delta(&changes, tick, lot, None, &mut scratch)?;
                let candidate_levels = transaction.candidate().level_count() as u64;
                let canonical = transaction.normalized_changes()?;
                let observed = transaction
                    .observed_scratch_backing_bytes()
                    .checked_add(
                        candidate_levels
                            .checked_mul(exact_level_arc_allocation_bytes())
                            .ok_or(super::ProviderBookError::Allocation)?,
                    )
                    .and_then(|bytes| {
                        bytes.checked_add(
                            (canonical.capacity() as u64)
                                * (std::mem::size_of::<market_squawk_domain::BookChange>() as u64),
                        )
                    })
                    .ok_or(super::ProviderBookError::Allocation)?;
                barrier.wait();
                std::hint::black_box((&transaction, &canonical));
                Ok::<u64, super::ProviderBookError>(observed)
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "maximum-shape worker panicked")?
                    .map_err(Into::into)
            })
            .collect::<Result<Vec<u64>, Box<dyn std::error::Error>>>()
    })?;
    let observed = observations
        .into_iter()
        .try_fold(0_u64, |total, value| total.checked_add(value))
        .ok_or("observed allocation total overflow")?;
    let ceiling = structural
        .additional_bytes
        .checked_mul(SHARDS as u64)
        .ok_or("structural allocation total overflow")?;
    assert!(
        observed <= ceiling,
        "observed={observed}, ceiling={ceiling}"
    );
    Ok(())
}

fn assert_linear_merge_matches_reference(
    generated: &[(bool, u8, u8)],
) -> Result<(), Box<dyn std::error::Error>> {
    const DEPTH: usize = 4;
    let tick = TickSize::try_from_decimal(Decimal::ONE)?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;
    let initial_bids = [
        level("100", "1")?,
        level("99", "1")?,
        level("98", "1")?,
        level("97", "1")?,
    ];
    let initial_asks = [
        level("200", "1")?,
        level("201", "1")?,
        level("202", "1")?,
        level("203", "1")?,
    ];
    let mut reference_bids = BTreeMap::from([(100_i64, 1_i64), (99, 1), (98, 1), (97, 1)]);
    let mut reference_asks = BTreeMap::from([(200_i64, 1_i64), (201, 1), (202, 1), (203, 1)]);
    let changes = generated
        .iter()
        .map(|(is_bid, offset, quantity)| {
            let (side, price, reference) = if *is_bid {
                (
                    ProviderBookSide::Bid,
                    100_i64 - i64::from(*offset),
                    &mut reference_bids,
                )
            } else {
                (
                    ProviderBookSide::Ask,
                    200_i64 + i64::from(*offset),
                    &mut reference_asks,
                )
            };
            if *quantity == 0 {
                reference.remove(&price);
            } else {
                reference.insert(price, i64::from(*quantity));
            }
            Ok(ProviderBookChange::new(
                side,
                level(&price.to_string(), &quantity.to_string())?,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let mut scratch = BookProcessingScratch::try_new(generated.len().max(DEPTH * 2))?;
    let mut book = ProviderBook::try_new(DepthLimit::new(DEPTH)?)?;
    book.replace_snapshot(&initial_bids, &initial_asks, tick, lot, None, &mut scratch)?;
    book.begin_delta(&changes, tick, lot, None, &mut scratch)?
        .commit();
    let actual_bids = book
        .scaled_bid_iter()
        .map(|(price, quantity)| (price.get(), quantity.get()))
        .collect::<Vec<_>>();
    let actual_asks = book
        .scaled_ask_iter()
        .map(|(price, quantity)| (price.get(), quantity.get()))
        .collect::<Vec<_>>();
    let expected_bids = reference_bids
        .iter()
        .rev()
        .take(DEPTH)
        .map(|(price, quantity)| (*price, *quantity))
        .collect::<Vec<_>>();
    let expected_asks = reference_asks
        .iter()
        .take(DEPTH)
        .map(|(price, quantity)| (*price, *quantity))
        .collect::<Vec<_>>();
    assert_eq!(actual_bids, expected_bids);
    assert_eq!(actual_asks, expected_asks);
    Ok(())
}

proptest! {
    #[test]
    fn linear_merge_matches_wire_order_reference_for_arbitrary_repeated_changes(
        generated in prop::collection::vec((any::<bool>(), 0_u8..8, 0_u8..20), 1..128)
    ) {
        let outcome = assert_linear_merge_matches_reference(&generated);
        prop_assert!(outcome.is_ok(), "linear merge mismatch: {:?}", outcome.err());
    }
}
