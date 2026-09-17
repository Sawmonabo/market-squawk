use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{DataQuality, MarketDepth, SourceIdentifier};
use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::Decimal;

use crate::decoder::decode_wire_for_test;
use crate::{
    TradierInstrumentKind, TradierLogicalProfile, TradierQuoteRequest, TradierRateLimitEvidence,
};

#[test]
fn provider_contract_preserves_quality_and_exact_wire_semantics() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        TradierLogicalProfile::ConsolidatedSecurities.quality_ceiling(),
        DataQuality::Aggregated
    );
    assert_eq!(
        TradierLogicalProfile::ConsolidatedSecurities.maximum_depth(),
        Some(MarketDepth::TopOfBook)
    );
    assert!(TradierLogicalProfile::ConsolidatedSecurities.supports_streaming());
    assert_eq!(
        TradierLogicalProfile::DerivedIndexes.quality_ceiling(),
        DataQuality::Modeled
    );
    assert_eq!(TradierLogicalProfile::DerivedIndexes.maximum_depth(), None);
    assert!(!TradierLogicalProfile::DerivedIndexes.supports_streaming());

    let quote = br#"{"type":"quote","symbol":"AAPL","bid":"216.10","bidsz":"60","bidexch":"Q","biddate":1786208100124,"ask":"216.12","asksz":"6","askexch":"P","askdate":1786208100128}"#;
    let (at, bid_shares, ask_shares) = decode_wire_for_test(quote, TradierInstrumentKind::Equity)
        .map_err(|_| "quote normalization failed")?;
    assert_eq!(at.unix_nanos(), 1_786_208_100_124_000_000);
    assert_eq!(bid_shares, Decimal::from(6_000));
    assert_eq!(ask_shares, Decimal::from(600));

    let trade = br#"{"type":"tradex","symbol":"AAPL","exch":"Q","price":"216.11","size":"25","cvol":"1000","date":1786208100130,"last":"216.11"}"#;
    let (at, price, shares) = decode_wire_for_test(trade, TradierInstrumentKind::Equity)
        .map_err(|_| "trade normalization failed")?;
    assert_eq!(at.unix_nanos(), 1_786_208_100_130_000_000);
    assert_eq!(price, Decimal::from_str("216.11")?);
    assert_eq!(shares, Decimal::from(25));

    let mut headers = HeaderMap::new();
    headers.insert("x-ratelimit-allowed", HeaderValue::from_static("120"));
    headers.insert("x-ratelimit-used", HeaderValue::from_static("17"));
    headers.insert("x-ratelimit-available", HeaderValue::from_static("103"));
    headers.insert(
        "x-ratelimit-expiry",
        HeaderValue::from_static("1786208160000"),
    );
    let rate = TradierRateLimitEvidence::try_from_headers(&headers)?;
    assert_eq!(
        (rate.allowed(), rate.used(), rate.available()),
        (120, 17, 103)
    );

    let duplicate = SourceIdentifier::try_from("AAPL")?;
    assert!(TradierQuoteRequest::try_new(vec![duplicate.clone(), duplicate], false).is_err());
    Ok(())
}
