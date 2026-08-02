use std::error::Error;
use std::mem::{size_of, size_of_val};
use std::num::NonZeroUsize;

use market_squawk_portfolio::{
    Lot, PortfolioQuery, PortfolioService, PortfolioServiceError, PortfolioServiceLimitInput,
    PortfolioServiceLimits, PortfolioSnapshot, Position,
};

use super::analytics::{account, analytics_revision};

type TestResult = Result<(), Box<dyn Error>>;

fn service_limits() -> Result<PortfolioServiceLimits, PortfolioServiceError> {
    PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
        max_accounts: NonZeroUsize::new(4).ok_or(PortfolioServiceError::InvalidLimits)?,
        max_history_per_account: NonZeroUsize::new(4)
            .ok_or(PortfolioServiceError::InvalidLimits)?,
        max_results: NonZeroUsize::new(8).ok_or(PortfolioServiceError::InvalidLimits)?,
        max_retained_bytes: NonZeroUsize::new(1024 * 1024)
            .ok_or(PortfolioServiceError::InvalidLimits)?,
    })
}

#[test]
fn service_requires_current_opaque_revision_and_returns_only_bounded_read_models() -> TestResult {
    let first = analytics_revision()?;
    let stale_token = first.token();
    let mut ledger = first.into_ledger()?;
    let second = ledger.try_apply(
        Vec::new(),
        None,
        super::valuation(9, 5, &[(1, 13), (2, 17)])?,
        super::revision_evidence(9, 5)?,
    )?;
    let current_token = second.token();
    let service = PortfolioService::try_new(vec![second.clone()], Vec::new(), service_limits()?)?;

    let head = service.head(account()?)?;
    assert_eq!(head, current_token);
    assert!(matches!(
        service.query(None),
        Err(PortfolioServiceError::MissingPrecondition)
    ));
    assert!(matches!(
        service.query(Some(PortfolioQuery::try_new(
            account()?,
            stale_token.clone(),
            NonZeroUsize::new(8).ok_or("result limit")?,
            NonZeroUsize::new(1024 * 1024).ok_or("byte limit")?,
        )?)),
        Err(PortfolioServiceError::StaleRevision)
    ));

    let result = service.query(Some(PortfolioQuery::try_new(
        account()?,
        current_token.clone(),
        NonZeroUsize::new(8).ok_or("result limit")?,
        NonZeroUsize::new(1024 * 1024).ok_or("byte limit")?,
    )?))?;
    assert_eq!(result.revision(), &current_token);
    assert_eq!(result.holdings().len(), 2);
    let expected_retained_bytes = size_of::<PortfolioSnapshot>()
        + size_of_val(second.positions())
        + second
            .positions()
            .iter()
            .flat_map(Position::lots)
            .map(|lot| size_of::<Lot>() + lot.id().retained_bytes())
            .sum::<usize>();
    assert_eq!(result.retained_bytes(), expected_retained_bytes);
    assert!(result.retained_bytes() <= 1024 * 1024);
    assert!(matches!(
        service.query(Some(PortfolioQuery::try_new(
            account()?,
            current_token.clone(),
            NonZeroUsize::new(8).ok_or("result limit")?,
            NonZeroUsize::new(expected_retained_bytes - 1).ok_or("byte limit")?,
        )?)),
        Err(PortfolioServiceError::RetainedBytesExceeded)
    ));

    let revoked =
        PortfolioService::try_new(vec![second], vec![stale_token.clone()], service_limits()?)?;
    assert!(matches!(
        revoked.query(Some(PortfolioQuery::try_new(
            account()?,
            stale_token,
            NonZeroUsize::new(8).ok_or("result limit")?,
            NonZeroUsize::new(1024 * 1024).ok_or("byte limit")?,
        )?)),
        Err(PortfolioServiceError::RevokedRevision)
    ));
    assert!(matches!(
        service.query(Some(PortfolioQuery::try_new(
            account()?,
            current_token,
            NonZeroUsize::new(1).ok_or("result limit")?,
            NonZeroUsize::new(1024 * 1024).ok_or("byte limit")?,
        )?)),
        Err(PortfolioServiceError::ResultLimitExceeded { .. })
    ));
    Ok(())
}
