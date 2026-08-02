use std::error::Error;
use std::time::{Duration, Instant};

use market_squawk_domain::{AccountId, InstrumentId, Timestamp};
use market_squawk_valuation::{ClassificationRuleset, InputSignificance};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::ProductionFairValueInputAuthority;
use crate::application::{
    FairValueInputAuthorityLimits, FairValueInputResolutionError, FairValueInputResolutionRequest,
    FairValueInputResolver, FairValueProducerKind,
};

#[tokio::test]
async fn restart_starts_without_receipt_authority() -> Result<(), Box<dyn Error>> {
    let authority =
        ProductionFairValueInputAuthority::try_new(FairValueInputAuthorityLimits::standard())?;
    let result = authority
        .resolver()
        .resolve(FairValueInputResolutionRequest {
            producer: FairValueProducerKind::Research,
            receipt_id: format!("research:{}", "0".repeat(64)).into_boxed_str(),
            significance: InputSignificance::Significant,
            account_id: AccountId::try_from(Uuid::from_u128(1))?,
            instrument_id: InstrumentId::try_from(Uuid::from_u128(2))?,
            measurement_at: Timestamp::from_unix_nanos(1_000),
            ruleset: ClassificationRuleset::current(1_000)?,
            market_access_assessment: None,
            cancellation: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(1),
        })
        .await;

    assert!(matches!(
        result,
        Err(FairValueInputResolutionError::NotFound)
    ));
    Ok(())
}
