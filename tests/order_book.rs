use market_squawk::{
    domain::{BookChange, PriceLevel, Side},
    features::OnlineFeatures,
    order_book::OrderBook,
};
use rust_decimal::Decimal;

#[test]
fn snapshot_and_delta_update_top_of_book_and_features() {
    let mut book = OrderBook::default();
    book.apply_snapshot(
        &[
            PriceLevel {
                price: Decimal::from(100_u32),
                size: Decimal::from(2_u32),
            },
            PriceLevel {
                price: Decimal::from(99_u32),
                size: Decimal::from(4_u32),
            },
        ],
        &[
            PriceLevel {
                price: Decimal::from(101_u32),
                size: Decimal::from(3_u32),
            },
            PriceLevel {
                price: Decimal::from(102_u32),
                size: Decimal::from(5_u32),
            },
        ],
    );

    let initial = book.top().expect("top of book");
    assert_eq!(initial.bid, Decimal::from(100_u32));
    assert_eq!(initial.ask, Decimal::from(101_u32));

    book.apply_changes(&[
        BookChange {
            side: Side::Buy,
            price: Decimal::from(100_u32),
            size: Decimal::ZERO,
        },
        BookChange {
            side: Side::Buy,
            price: Decimal::new(1005, 1),
            size: Decimal::from(1_u32),
        },
    ]);

    let updated = book.top().expect("updated top of book");
    assert_eq!(updated.bid, Decimal::new(1005, 1));
    assert_eq!(updated.ask, Decimal::from(101_u32));

    let features = OnlineFeatures::from_top(&updated).expect("valid features");
    assert_eq!(features.mid_price, Decimal::new(10075, 2));
    assert_eq!(features.spread, Decimal::new(5, 1));
}

#[test]
fn crossed_book_is_detected() {
    let mut book = OrderBook::default();
    book.apply_snapshot(
        &[PriceLevel {
            price: Decimal::from(102_u32),
            size: Decimal::ONE,
        }],
        &[PriceLevel {
            price: Decimal::from(101_u32),
            size: Decimal::ONE,
        }],
    );
    assert!(book.is_crossed());
}
