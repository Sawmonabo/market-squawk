//! Deterministic market-activity derivation from bounded committed receipts.

use super::*;

pub(super) fn derive_market_activity(
    receipts: &[CommittedQualifiedMarketObservation],
    selected: &CommittedQualifiedMarketObservation,
    measurement_at: Timestamp,
    policy: MarketActivityPolicy,
) -> Result<(MarketActivity, [u8; 32]), FairValueError> {
    if receipts.iter().any(|receipt| {
        receipt.instrument_id() != selected.instrument_id()
            || receipt.venue_id() != selected.venue_id()
            || receipt.execution_terms().definition_revision()
                != selected.execution_terms().definition_revision()
    }) {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(receipts.len())
        .map_err(|_| FairValueError::Arithmetic)?;
    canonical.extend(receipts.iter().map(|receipt| {
        (
            receipt.binding_digest(),
            receipt.source_timestamp(),
            receipt.source_authorization(),
            receipt.coverage_status(),
            receipt.trading_status(),
            receipt.recorded_quality(),
            match receipt.price() {
                QualifiedMarketPrice::Trade { quantity, .. } => u64::try_from(quantity.get()).ok(),
                QualifiedMarketPrice::Quote { .. } => None,
            },
        )
    }));
    canonical.sort_unstable_by_key(|value| value.0);
    canonical.dedup_by_key(|value| value.0);
    let mut set_hash = CanonicalHasher::new(b"market-squawk/market-activity-set/v1");
    set_hash.fixed(policy.hash().bytes());
    set_hash.u64(u64::try_from(canonical.len()).map_err(|_| FairValueError::Arithmetic)?);
    let mut qualifying = 0_usize;
    let mut aggregate_quantity_lots = 0_u64;
    for (binding, source_timestamp, authorization, coverage, trading, quality, executed_quantity) in
        canonical
    {
        set_hash.fixed(binding);
        match source_timestamp {
            Some(value) => {
                set_hash.u8(1);
                set_hash.i64(value.unix_nanos());
            }
            None => set_hash.u8(0),
        }
        let within_window = source_timestamp.is_some_and(|value| {
            value <= measurement_at
                && i128::from(measurement_at.unix_nanos()) - i128::from(value.unix_nanos())
                    <= i128::from(policy.lookback_nanos())
        });
        match executed_quantity {
            Some(value) => {
                set_hash.u8(1);
                set_hash.u64(value);
            }
            None => set_hash.u8(0),
        }
        if within_window
            && authorization == SourceAuthorization::Authorized
            && coverage == CoverageStatus::Sufficient
            && trading == TradingStatus::Active
            && quality == DataQuality::DirectVerified
            && executed_quantity.is_some()
        {
            qualifying = qualifying
                .checked_add(1)
                .ok_or(FairValueError::Arithmetic)?;
            aggregate_quantity_lots = aggregate_quantity_lots
                .checked_add(executed_quantity.ok_or(FairValueError::InvalidProducerEvidence)?)
                .ok_or(FairValueError::Arithmetic)?;
        }
    }
    Ok((
        if qualifying >= policy.minimum_observations()
            && aggregate_quantity_lots >= policy.minimum_aggregate_quantity_lots()
        {
            MarketActivity::Active
        } else {
            MarketActivity::Inactive
        },
        set_hash.finish(),
    ))
}
