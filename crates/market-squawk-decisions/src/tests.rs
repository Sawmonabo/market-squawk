use market_squawk_domain::{
    Currency, DigestAlgorithm, EvidenceDigest, FinancialError, InstrumentId, Money, RevisionNumber,
    Timestamp,
};
use rust_decimal::Decimal;

use crate::{
    DecisionActorId, DecisionContentDigest, DecisionContractError, DossierId, InvestmentTargetSet,
    InvestmentTargetSetId, ReferenceMark, TargetPriceCases, TargetPriceRange, TargetReview,
    TargetReviewDisposition, TargetReviewId,
};

fn content_digest(byte: u8) -> Result<DecisionContentDigest, DecisionContractError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]))
}

fn money(amount: i64, currency: &str) -> Result<Money, FinancialError> {
    Ok(Money::new(
        Decimal::new(amount, 2),
        Currency::try_from(currency)?,
    ))
}

fn target(expires_at: Timestamp) -> Result<InvestmentTargetSet, Box<dyn std::error::Error>> {
    let observed_at = Timestamp::from_unix_nanos(10);
    let reference = ReferenceMark::try_new(money(10_000, "USD")?, observed_at, content_digest(1)?)?;
    Ok(InvestmentTargetSet::try_new(
        InvestmentTargetSetId::try_new("target.alpha")?,
        RevisionNumber::new(1)?,
        DossierId::try_new("dossier.alpha")?,
        "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01".parse::<InstrumentId>()?,
        reference,
        TargetPriceCases::try_new(
            money(8_000, "USD")?,
            money(12_000, "USD")?,
            money(16_000, "USD")?,
        )?,
        TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "USD")?)?,
        TargetPriceRange::try_new(money(13_000, "USD")?, money(14_000, "USD")?)?,
        TargetPriceRange::try_new(money(7_000, "USD")?, money(8_000, "USD")?)?,
        Timestamp::from_unix_nanos(20),
        Timestamp::from_unix_nanos(90),
        expires_at,
        content_digest(2)?,
    )?)
}

#[test]
fn target_set_rejects_mixed_currency() -> Result<(), Box<dyn std::error::Error>> {
    let result = TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "EUR")?);

    assert_eq!(result, Err(DecisionContractError::CurrencyMismatch));
    Ok(())
}

#[test]
fn activation_review_rejects_expired_target() -> Result<(), Box<dyn std::error::Error>> {
    let target = target(Timestamp::from_unix_nanos(100))?;

    let result = TargetReview::try_new(
        TargetReviewId::try_new("review.alpha")?,
        &target,
        DecisionActorId::try_new("reviewer.alpha")?,
        Timestamp::from_unix_nanos(100),
        TargetReviewDisposition::Activate,
        content_digest(3)?,
    );

    assert_eq!(result, Err(DecisionContractError::ExpiredActivation));
    Ok(())
}

#[test]
fn target_set_rejects_reversed_case_order() -> Result<(), Box<dyn std::error::Error>> {
    let result = TargetPriceCases::try_new(
        money(13_000, "USD")?,
        money(12_000, "USD")?,
        money(16_000, "USD")?,
    );

    assert_eq!(result, Err(DecisionContractError::InvalidPriceOrder));
    Ok(())
}
