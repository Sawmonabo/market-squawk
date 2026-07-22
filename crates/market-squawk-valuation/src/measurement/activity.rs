//! Deterministic market-activity derivation from bounded committed receipts.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActivityTimeline {
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    qualification_evaluated_at: Timestamp,
    qualification_valid_until: Timestamp,
}

impl ActivityTimeline {
    fn is_eligible(self, measurement_at: Timestamp, lookback_nanos: u64) -> bool {
        self.source_timestamp.is_some_and(|source_timestamp| {
            source_timestamp <= measurement_at
                && i128::from(measurement_at.unix_nanos())
                    - i128::from(source_timestamp.unix_nanos())
                    <= i128::from(lookback_nanos)
                && self.received_at <= measurement_at
                && self.available_at <= measurement_at
                && self.qualification_evaluated_at <= measurement_at
                && measurement_at <= self.qualification_valid_until
        })
    }

    fn hash_into(self, hash: &mut CanonicalHasher) {
        match self.source_timestamp {
            Some(value) => {
                hash.u8(1);
                hash.i64(value.unix_nanos());
            }
            None => hash.u8(0),
        }
        hash.i64(self.received_at.unix_nanos());
        hash.i64(self.available_at.unix_nanos());
        hash.i64(self.qualification_evaluated_at.unix_nanos());
        hash.i64(self.qualification_valid_until.unix_nanos());
    }
}

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
            ActivityTimeline {
                source_timestamp: receipt.source_timestamp(),
                received_at: receipt.received_at(),
                available_at: receipt.available_at(),
                qualification_evaluated_at: receipt.qualification_evaluated_at(),
                qualification_valid_until: receipt.qualification_valid_until(),
            },
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
    if canonical
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0 && pair[0] != pair[1])
    {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    canonical.dedup_by_key(|value| value.0);
    let mut set_hash = CanonicalHasher::new(b"market-squawk/market-activity-set/v2");
    set_hash.fixed(policy.hash().bytes());
    set_hash.u64(u64::try_from(canonical.len()).map_err(|_| FairValueError::Arithmetic)?);
    let mut qualifying = 0_usize;
    let mut aggregate_quantity_lots = 0_u64;
    for (binding, timeline, authorization, coverage, trading, quality, executed_quantity) in
        canonical
    {
        set_hash.fixed(binding);
        timeline.hash_into(&mut set_hash);
        let within_window = timeline.is_eligible(measurement_at, policy.lookback_nanos());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_timeline_requires_point_in_time_eligibility() {
        let measurement_at = Timestamp::from_unix_nanos(100);
        let eligible = ActivityTimeline {
            source_timestamp: Some(Timestamp::from_unix_nanos(90)),
            received_at: Timestamp::from_unix_nanos(91),
            available_at: Timestamp::from_unix_nanos(92),
            qualification_evaluated_at: Timestamp::from_unix_nanos(93),
            qualification_valid_until: Timestamp::from_unix_nanos(110),
        };
        assert!(eligible.is_eligible(measurement_at, 20));

        for ineligible in [
            ActivityTimeline {
                received_at: Timestamp::from_unix_nanos(101),
                ..eligible
            },
            ActivityTimeline {
                available_at: Timestamp::from_unix_nanos(101),
                ..eligible
            },
            ActivityTimeline {
                qualification_evaluated_at: Timestamp::from_unix_nanos(101),
                ..eligible
            },
            ActivityTimeline {
                qualification_valid_until: Timestamp::from_unix_nanos(99),
                ..eligible
            },
        ] {
            assert!(!ineligible.is_eligible(measurement_at, 20));
        }
    }
}
