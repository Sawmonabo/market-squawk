use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{PriceTicks, QuantityLots};
use market_squawk_live::{BookError, BookSide, DepthLimit, LevelUpdate, ScaledBook};
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed, TestCaseError, TestCaseResult};

const PROPERTY_CASES: u32 = 256;
const PROPERTY_SEED: u64 = 0x4d41_524b_4554;

#[derive(Clone, Debug)]
struct SnapshotCase {
    depth: usize,
    center: i64,
    bid_offsets: BTreeSet<i64>,
    ask_offsets: BTreeSet<i64>,
}

#[derive(Clone, Debug)]
struct RawChange {
    side: BookSide,
    offset: i64,
    quantity: i64,
}

#[derive(Clone, Debug)]
struct DeltaCase {
    depth: usize,
    bid_offsets: BTreeSet<i64>,
    ask_offsets: BTreeSet<i64>,
    changes: Vec<RawChange>,
}

#[derive(Clone, Copy, Debug)]
enum InvalidSnapshotKind {
    DuplicateBid,
    DuplicateAsk,
    WrongBidOrdering,
    WrongAskOrdering,
    ZeroBidQuantity,
    ZeroAskQuantity,
    BidSideMismatch,
    AskSideMismatch,
    Crossed,
}

fn property_config() -> Config {
    Config {
        cases: PROPERTY_CASES,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(PROPERTY_SEED),
        ..Config::default()
    }
}

fn snapshot_case() -> impl Strategy<Value = SnapshotCase> {
    (
        1_usize..16,
        -10_000_i64..10_000,
        prop::collection::btree_set(0_i64..128, 0..32),
        prop::collection::btree_set(0_i64..128, 0..32),
    )
        .prop_map(|(depth, center, bid_offsets, ask_offsets)| SnapshotCase {
            depth,
            center,
            bid_offsets,
            ask_offsets,
        })
}

fn delta_case() -> impl Strategy<Value = DeltaCase> {
    let change =
        (any::<bool>(), 0_i64..256, 0_i64..100).prop_map(|(is_bid, offset, quantity)| RawChange {
            side: if is_bid { BookSide::Bid } else { BookSide::Ask },
            offset,
            quantity,
        });

    (
        1_usize..16,
        prop::collection::btree_set(0_i64..128, 1..24),
        prop::collection::btree_set(0_i64..128, 1..24),
        prop::collection::vec(change, 1..64),
    )
        .prop_map(|(depth, bid_offsets, ask_offsets, changes)| DeltaCase {
            depth,
            bid_offsets,
            ask_offsets,
            changes,
        })
}

fn depth_limit(depth: usize) -> Result<DepthLimit, TestCaseError> {
    DepthLimit::new(depth).map_err(|error| TestCaseError::fail(error.to_string()))
}

fn quantity_lots(quantity: i64) -> Result<QuantityLots, TestCaseError> {
    QuantityLots::new(quantity).map_err(|error| TestCaseError::fail(error.to_string()))
}

fn level(side: BookSide, price: i64, quantity: i64) -> Result<LevelUpdate, TestCaseError> {
    Ok(LevelUpdate::new(
        side,
        PriceTicks::new(price),
        quantity_lots(quantity)?,
    ))
}

fn levels_from_offsets(
    side: BookSide,
    center: i64,
    offsets: &BTreeSet<i64>,
) -> Result<Vec<LevelUpdate>, TestCaseError> {
    offsets
        .iter()
        .map(|offset| {
            let price = match side {
                BookSide::Bid => center - 1 - offset,
                BookSide::Ask => center + 1 + offset,
            };
            level(side, price, offset.rem_euclid(97) + 1)
        })
        .collect()
}

fn apply_ok(result: Result<(), BookError>) -> TestCaseResult {
    result.map_err(|error| TestCaseError::fail(error.to_string()))
}

fn assert_book_invariants(book: &ScaledBook, depth: usize) -> TestCaseResult {
    let bids = book.bid_levels();
    let asks = book.ask_levels();

    prop_assert!(bids.len() <= depth);
    prop_assert!(asks.len() <= depth);
    prop_assert_eq!(book.depth_limit().get(), depth);
    prop_assert!(bids.windows(2).all(|window| window[0].0 > window[1].0));
    prop_assert!(asks.windows(2).all(|window| window[0].0 < window[1].0));
    prop_assert!(bids.iter().all(|(_, quantity)| quantity.get() > 0));
    prop_assert!(asks.iter().all(|(_, quantity)| quantity.get() > 0));
    prop_assert_eq!(book.best_bid(), bids.first().copied());
    prop_assert_eq!(book.best_ask(), asks.first().copied());

    if let (Some((best_bid, _)), Some((best_ask, _))) = (book.best_bid(), book.best_ask()) {
        prop_assert!(best_bid < best_ask);
    }

    Ok(())
}

fn expected_levels(levels: &[LevelUpdate], depth: usize) -> Vec<(PriceTicks, QuantityLots)> {
    levels
        .iter()
        .take(depth)
        .map(|entry| (entry.price(), entry.quantity()))
        .collect()
}

fn price_for_change(change: &RawChange) -> i64 {
    match change.side {
        BookSide::Bid => -1 - change.offset,
        BookSide::Ask => 1 + change.offset,
    }
}

fn apply_model_change(
    bids: &mut BTreeMap<i64, i64>,
    asks: &mut BTreeMap<i64, i64>,
    change: &RawChange,
) {
    let price = price_for_change(change);
    let side = match change.side {
        BookSide::Bid => bids,
        BookSide::Ask => asks,
    };

    if change.quantity == 0 {
        side.remove(&price);
    } else {
        side.insert(price, change.quantity);
    }
}

fn truncate_model(bids: &mut BTreeMap<i64, i64>, asks: &mut BTreeMap<i64, i64>, depth: usize) {
    while bids.len() > depth {
        let worst_price = bids.keys().next().copied();
        if let Some(price) = worst_price {
            bids.remove(&price);
        } else {
            break;
        }
    }

    while asks.len() > depth {
        let worst_price = asks.keys().next_back().copied();
        if let Some(price) = worst_price {
            asks.remove(&price);
        } else {
            break;
        }
    }
}

fn model_levels(
    levels: &BTreeMap<i64, i64>,
    side: BookSide,
) -> Result<Vec<(PriceTicks, QuantityLots)>, TestCaseError> {
    match side {
        BookSide::Bid => levels.iter().rev().map(model_level).collect(),
        BookSide::Ask => levels.iter().map(model_level).collect(),
    }
}

fn model_level(
    (price, quantity): (&i64, &i64),
) -> Result<(PriceTicks, QuantityLots), TestCaseError> {
    Ok((PriceTicks::new(*price), quantity_lots(*quantity)?))
}

proptest! {
    #![proptest_config(property_config())]

    #[test]
    fn valid_snapshots_preserve_order_depth_extrema_and_uncrossed_state(case in snapshot_case()) {
        let bids = levels_from_offsets(BookSide::Bid, case.center, &case.bid_offsets)?;
        let asks = levels_from_offsets(BookSide::Ask, case.center, &case.ask_offsets)?;
        let mut book = ScaledBook::new(depth_limit(case.depth)?);

        apply_ok(book.replace_snapshot(&bids, &asks))?;

        let expected_bids = expected_levels(&bids, case.depth);
        let expected_asks = expected_levels(&asks, case.depth);
        prop_assert_eq!(book.bid_levels(), expected_bids);
        prop_assert_eq!(book.ask_levels(), expected_asks);
        assert_book_invariants(&book, case.depth)?;
    }

    #[test]
    fn safe_delta_messages_match_model_and_never_commit_a_crossed_state(case in delta_case()) {
        let initial_bids = levels_from_offsets(BookSide::Bid, 0, &case.bid_offsets)?;
        let initial_asks = levels_from_offsets(BookSide::Ask, 0, &case.ask_offsets)?;
        let mut book = ScaledBook::new(depth_limit(case.depth)?);
        apply_ok(book.replace_snapshot(&initial_bids, &initial_asks))?;

        let mut model_bids: BTreeMap<i64, i64> = book
            .bid_levels()
            .into_iter()
            .map(|(price, quantity)| (price.get(), quantity.get()))
            .collect();
        let mut model_asks: BTreeMap<i64, i64> = book
            .ask_levels()
            .into_iter()
            .map(|(price, quantity)| (price.get(), quantity.get()))
            .collect();

        let changes: Vec<LevelUpdate> = case
            .changes
            .iter()
            .map(|change| level(change.side, price_for_change(change), change.quantity))
            .collect::<Result<_, _>>()?;

        apply_ok(book.apply_delta(&changes))?;
        for change in &case.changes {
            apply_model_change(&mut model_bids, &mut model_asks, change);
        }
        truncate_model(&mut model_bids, &mut model_asks, case.depth);

        let expected_bids = model_levels(&model_bids, BookSide::Bid)?;
        let expected_asks = model_levels(&model_asks, BookSide::Ask)?;
        prop_assert_eq!(book.bid_levels(), expected_bids);
        prop_assert_eq!(book.ask_levels(), expected_asks);
        assert_book_invariants(&book, case.depth)?;
    }

    #[test]
    fn zero_quantity_deletes_existing_best_levels(
        depth in 2_usize..16,
        bid_offsets in prop::collection::btree_set(0_i64..128, 2..24),
        ask_offsets in prop::collection::btree_set(0_i64..128, 2..24),
    ) {
        let bids = levels_from_offsets(BookSide::Bid, 0, &bid_offsets)?;
        let asks = levels_from_offsets(BookSide::Ask, 0, &ask_offsets)?;
        let mut book = ScaledBook::new(depth_limit(depth)?);
        apply_ok(book.replace_snapshot(&bids, &asks))?;

        let prior_bids = book.bid_levels();
        let prior_asks = book.ask_levels();
        let best_bid = prior_bids
            .first()
            .copied()
            .ok_or_else(|| TestCaseError::fail("missing best bid"))?
            .0;
        let best_ask = prior_asks
            .first()
            .copied()
            .ok_or_else(|| TestCaseError::fail("missing best ask"))?
            .0;
        let changes = [
            level(BookSide::Bid, best_bid.get(), 0)?,
            level(BookSide::Ask, best_ask.get(), 0)?,
        ];

        apply_ok(book.apply_delta(&changes))?;

        prop_assert!(!book.bid_levels().iter().any(|(price, _)| *price == best_bid));
        prop_assert!(!book.ask_levels().iter().any(|(price, _)| *price == best_ask));
        prop_assert_eq!(book.best_bid(), prior_bids.get(1).copied());
        prop_assert_eq!(book.best_ask(), prior_asks.get(1).copied());
        assert_book_invariants(&book, depth)?;
    }

    #[test]
    fn invalid_snapshot_candidates_are_rejected_atomically(case in snapshot_case()) {
        let initial_bids = levels_from_offsets(BookSide::Bid, case.center, &case.bid_offsets)?;
        let initial_asks = levels_from_offsets(BookSide::Ask, case.center, &case.ask_offsets)?;
        let mut initial = ScaledBook::new(depth_limit(case.depth)?);
        apply_ok(initial.replace_snapshot(&initial_bids, &initial_asks))?;

        for kind in [
            InvalidSnapshotKind::DuplicateBid,
            InvalidSnapshotKind::DuplicateAsk,
            InvalidSnapshotKind::WrongBidOrdering,
            InvalidSnapshotKind::WrongAskOrdering,
            InvalidSnapshotKind::ZeroBidQuantity,
            InvalidSnapshotKind::ZeroAskQuantity,
            InvalidSnapshotKind::BidSideMismatch,
            InvalidSnapshotKind::AskSideMismatch,
            InvalidSnapshotKind::Crossed,
        ] {
            let (candidate_bids, candidate_asks, expected_error) = match kind {
                InvalidSnapshotKind::DuplicateBid => (
                    vec![
                        level(BookSide::Bid, case.center - 1, 10)?,
                        level(BookSide::Bid, case.center - 1, 20)?,
                    ],
                    vec![level(BookSide::Ask, case.center + 1, 10)?],
                    BookError::DuplicatePrice {
                        side: BookSide::Bid,
                        price: PriceTicks::new(case.center - 1),
                    },
                ),
                InvalidSnapshotKind::DuplicateAsk => (
                    vec![level(BookSide::Bid, case.center - 1, 10)?],
                    vec![
                        level(BookSide::Ask, case.center + 1, 10)?,
                        level(BookSide::Ask, case.center + 1, 20)?,
                    ],
                    BookError::DuplicatePrice {
                        side: BookSide::Ask,
                        price: PriceTicks::new(case.center + 1),
                    },
                ),
                InvalidSnapshotKind::WrongBidOrdering => (
                    vec![
                        level(BookSide::Bid, case.center - 2, 10)?,
                        level(BookSide::Bid, case.center - 1, 20)?,
                    ],
                    vec![level(BookSide::Ask, case.center + 1, 10)?],
                    BookError::InvalidOrdering {
                        side: BookSide::Bid,
                    },
                ),
                InvalidSnapshotKind::WrongAskOrdering => (
                    vec![level(BookSide::Bid, case.center - 1, 10)?],
                    vec![
                        level(BookSide::Ask, case.center + 2, 10)?,
                        level(BookSide::Ask, case.center + 1, 20)?,
                    ],
                    BookError::InvalidOrdering {
                        side: BookSide::Ask,
                    },
                ),
                InvalidSnapshotKind::ZeroBidQuantity => (
                    vec![level(BookSide::Bid, case.center - 1, 0)?],
                    vec![level(BookSide::Ask, case.center + 1, 10)?],
                    BookError::ZeroSnapshotQuantity,
                ),
                InvalidSnapshotKind::ZeroAskQuantity => (
                    vec![level(BookSide::Bid, case.center - 1, 10)?],
                    vec![level(BookSide::Ask, case.center + 1, 0)?],
                    BookError::ZeroSnapshotQuantity,
                ),
                InvalidSnapshotKind::BidSideMismatch => (
                    vec![level(BookSide::Ask, case.center - 1, 10)?],
                    vec![level(BookSide::Ask, case.center + 1, 10)?],
                    BookError::SideMismatch {
                        expected: BookSide::Bid,
                        found: BookSide::Ask,
                    },
                ),
                InvalidSnapshotKind::AskSideMismatch => (
                    vec![level(BookSide::Bid, case.center - 1, 10)?],
                    vec![level(BookSide::Bid, case.center + 1, 10)?],
                    BookError::SideMismatch {
                        expected: BookSide::Ask,
                        found: BookSide::Bid,
                    },
                ),
                InvalidSnapshotKind::Crossed => (
                    vec![level(BookSide::Bid, case.center + 1, 10)?],
                    vec![level(BookSide::Ask, case.center, 10)?],
                    BookError::Crossed,
                ),
            };
            let mut candidate = initial.clone();

            let result = candidate.replace_snapshot(&candidate_bids, &candidate_asks);

            prop_assert_eq!(result, Err(expected_error));
            prop_assert_eq!(&candidate, &initial);
            assert_book_invariants(&candidate, case.depth)?;
        }
    }

    #[test]
    fn multi_change_crossing_delta_rolls_back_every_prior_change(
        depth in 2_usize..16,
        base in -10_000_i64..10_000,
        replacement_quantity in 1_i64..1_000,
    ) {
        let depth_i64 = i64::try_from(depth)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let bid_offsets: BTreeSet<i64> = (0..depth_i64 + 2).collect();
        let ask_offsets: BTreeSet<i64> = (0..depth_i64 + 2).collect();
        let bids = levels_from_offsets(BookSide::Bid, base, &bid_offsets)?;
        let asks = levels_from_offsets(BookSide::Ask, base + 100, &ask_offsets)?;
        let mut book = ScaledBook::new(depth_limit(depth)?);
        apply_ok(book.replace_snapshot(&bids, &asks))?;
        let before = book.clone();

        let best_bid = before.best_bid().ok_or_else(|| TestCaseError::fail("missing best bid"))?.0;
        let best_ask = before.best_ask().ok_or_else(|| TestCaseError::fail("missing best ask"))?.0;
        let changes = [
            level(BookSide::Bid, best_bid.get(), replacement_quantity)?,
            level(BookSide::Ask, best_ask.get(), 0)?,
            level(BookSide::Bid, base - depth_i64 - 100, 37)?,
            level(BookSide::Ask, best_bid.get(), 41)?,
        ];

        let result = book.apply_delta(&changes);

        prop_assert_eq!(result, Err(BookError::Crossed));
        prop_assert_eq!(&book, &before);
        assert_book_invariants(&book, depth)?;
    }
}
