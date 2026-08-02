use std::error::Error;

use market_squawk_domain::{PriceTicks, QuantityLots};
use market_squawk_live::{
    BookError, BookSide, DepthLimit, LevelUpdate, MAX_BOOK_MESSAGE_ITEMS, ScaledBook,
};

fn quantity(value: i64) -> Result<QuantityLots, Box<dyn Error>> {
    Ok(QuantityLots::new(value)?)
}

fn level(side: BookSide, price: i64, quantity_value: i64) -> Result<LevelUpdate, Box<dyn Error>> {
    Ok(LevelUpdate::new(
        side,
        PriceTicks::new(price),
        quantity(quantity_value)?,
    ))
}

#[test]
fn snapshot_and_delta_preserve_order_depth_and_delete_zero() -> Result<(), Box<dyn Error>> {
    let mut book = ScaledBook::new(DepthLimit::new(2)?);
    book.replace_snapshot(
        &[
            level(BookSide::Bid, 100, 10)?,
            level(BookSide::Bid, 99, 11)?,
            level(BookSide::Bid, 98, 12)?,
        ],
        &[
            level(BookSide::Ask, 101, 8)?,
            level(BookSide::Ask, 102, 9)?,
            level(BookSide::Ask, 103, 10)?,
        ],
    )?;

    assert_eq!(book.best_bid(), Some((PriceTicks::new(100), quantity(10)?)));
    assert_eq!(book.best_ask(), Some((PriceTicks::new(101), quantity(8)?)));
    assert_eq!(book.bid_levels().len(), 2);
    assert_eq!(book.ask_levels().len(), 2);

    book.apply_delta(&[
        level(BookSide::Bid, 100, 0)?,
        level(BookSide::Bid, 101, 7)?,
        level(BookSide::Ask, 101, 0)?,
        level(BookSide::Ask, 103, 5)?,
    ])?;

    assert_eq!(book.best_bid(), Some((PriceTicks::new(101), quantity(7)?)));
    assert_eq!(book.best_ask(), Some((PriceTicks::new(102), quantity(9)?)));
    assert!(
        book.bid_levels()
            .windows(2)
            .all(|levels| levels[0].0 > levels[1].0)
    );
    assert!(
        book.ask_levels()
            .windows(2)
            .all(|levels| levels[0].0 < levels[1].0)
    );
    assert!(
        book.bid_levels()
            .iter()
            .chain(book.ask_levels().iter())
            .all(|(_, lots)| lots.get() > 0)
    );
    Ok(())
}

#[test]
fn failed_multi_change_delta_rolls_back_the_complete_message() -> Result<(), Box<dyn Error>> {
    let mut book = ScaledBook::new(DepthLimit::new(4)?);
    book.replace_snapshot(
        &[level(BookSide::Bid, 100, 10)?, level(BookSide::Bid, 99, 9)?],
        &[level(BookSide::Ask, 101, 8)?, level(BookSide::Ask, 102, 7)?],
    )?;
    let before_bids = book.bid_levels();
    let before_asks = book.ask_levels();

    let error = book.apply_delta(&[level(BookSide::Bid, 98, 6)?, level(BookSide::Ask, 97, 5)?]);

    assert!(matches!(error, Err(market_squawk_live::BookError::Crossed)));
    assert_eq!(book.bid_levels(), before_bids);
    assert_eq!(book.ask_levels(), before_asks);
    Ok(())
}

#[test]
fn invalid_snapshot_never_replaces_the_last_good_book() -> Result<(), Box<dyn Error>> {
    let mut book = ScaledBook::new(DepthLimit::new(4)?);
    book.replace_snapshot(
        &[level(BookSide::Bid, 100, 10)?],
        &[level(BookSide::Ask, 101, 8)?],
    )?;

    let result = book.replace_snapshot(
        &[level(BookSide::Bid, 102, 2)?],
        &[level(BookSide::Ask, 101, 3)?],
    );

    assert!(matches!(
        result,
        Err(market_squawk_live::BookError::Crossed)
    ));
    assert_eq!(book.best_bid(), Some((PriceTicks::new(100), quantity(10)?)));
    assert_eq!(book.best_ask(), Some((PriceTicks::new(101), quantity(8)?)));
    Ok(())
}

#[test]
fn snapshot_rejects_duplicate_prices_and_wrong_side_order() -> Result<(), Box<dyn Error>> {
    let mut book = ScaledBook::new(DepthLimit::new(4)?);
    assert!(matches!(
        book.replace_snapshot(
            &[level(BookSide::Bid, 100, 1)?, level(BookSide::Bid, 100, 2)?],
            &[level(BookSide::Ask, 101, 1)?],
        ),
        Err(market_squawk_live::BookError::DuplicatePrice { .. })
    ));
    assert!(matches!(
        book.replace_snapshot(
            &[level(BookSide::Bid, 99, 1)?, level(BookSide::Bid, 100, 2)?],
            &[level(BookSide::Ask, 101, 1)?],
        ),
        Err(market_squawk_live::BookError::InvalidOrdering { .. })
    ));
    Ok(())
}

#[test]
fn snapshot_accepts_the_exact_combined_message_ceiling() -> Result<(), Box<dyn Error>> {
    let mut book = ScaledBook::new(DepthLimit::new(MAX_BOOK_MESSAGE_ITEMS / 2)?);
    let bids = (0..MAX_BOOK_MESSAGE_ITEMS / 2)
        .map(|offset| level(BookSide::Bid, 30_000 - i64::try_from(offset)?, 1))
        .collect::<Result<Vec<_>, _>>()?;
    let asks = (0..MAX_BOOK_MESSAGE_ITEMS / 2)
        .map(|offset| level(BookSide::Ask, 40_000 + i64::try_from(offset)?, 1))
        .collect::<Result<Vec<_>, _>>()?;

    book.replace_snapshot(&bids, &asks)?;

    assert_eq!(
        book.bid_levels().len() + book.ask_levels().len(),
        MAX_BOOK_MESSAGE_ITEMS
    );
    Ok(())
}

#[test]
fn over_limit_messages_fail_before_content_validation_and_preserve_state()
-> Result<(), Box<dyn Error>> {
    let mut book = ScaledBook::new(DepthLimit::new(2)?);
    book.replace_snapshot(
        &[level(BookSide::Bid, 100, 1)?],
        &[level(BookSide::Ask, 101, 1)?],
    )?;
    let invalid = vec![level(BookSide::Ask, 100, 1)?; MAX_BOOK_MESSAGE_ITEMS + 1];

    assert_eq!(
        book.replace_snapshot(&invalid, &[]),
        Err(BookError::MessageTooLarge {
            observed: MAX_BOOK_MESSAGE_ITEMS + 1,
            maximum: MAX_BOOK_MESSAGE_ITEMS,
        })
    );
    assert_eq!(
        book.apply_delta(&invalid),
        Err(BookError::MessageTooLarge {
            observed: MAX_BOOK_MESSAGE_ITEMS + 1,
            maximum: MAX_BOOK_MESSAGE_ITEMS,
        })
    );
    assert_eq!(book.best_bid(), Some((PriceTicks::new(100), quantity(1)?)));
    assert_eq!(book.best_ask(), Some((PriceTicks::new(101), quantity(1)?)));
    Ok(())
}
