//! Deterministic market-activity derivation from bounded committed receipts.

use std::cmp::Ordering;

use market_squawk_domain::{PriceTicks, QuantityLots};

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActivityTimeline {
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    qualification_evaluated_at: Timestamp,
    qualification_valid_until: Timestamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActivityTradeFacts {
    price: PriceTicks,
    quantity: QuantityLots,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActivityDelivery<'a> {
    binding_digest: [u8; 32],
    stable_trade_id: Option<&'a SourceIdentifier>,
    timeline: ActivityTimeline,
    authorization: SourceAuthorization,
    coverage: CoverageStatus,
    trading: TradingStatus,
    quality: DataQuality,
    trade: Option<ActivityTradeFacts>,
}

impl ActivityDelivery<'_> {
    fn is_qualifying(self, measurement_at: Timestamp, policy: MarketActivityPolicy) -> bool {
        self.trade.is_some()
            && self
                .timeline
                .is_eligible(measurement_at, policy.lookback_nanos())
            && self.authorization == SourceAuthorization::Authorized
            && self.coverage == CoverageStatus::Sufficient
            && self.trading == TradingStatus::Active
            && self.quality == DataQuality::DirectVerified
    }

    fn hash_delivery_into(self, hash: &mut CanonicalHasher) {
        hash.fixed(self.binding_digest);
        self.timeline.hash_into(hash);
        hash.u8(authorization_tag(self.authorization));
        hash.u8(coverage_tag(self.coverage));
        hash.u8(trading_tag(self.trading));
        hash.u8(quality_tag(self.quality));
    }
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
        receipt.source_id() != selected.source_id()
            || receipt.instrument_id() != selected.instrument_id()
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
    for receipt in receipts {
        let trade = match receipt.price() {
            QualifiedMarketPrice::Trade { price, quantity } => {
                Some(ActivityTradeFacts { price, quantity })
            }
            QualifiedMarketPrice::Quote { .. } => None,
        };
        let stable_trade_id = receipt.stable_trade_id();
        if trade.is_some() != stable_trade_id.is_some() {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        canonical.push(ActivityDelivery {
            binding_digest: receipt.binding_digest(),
            stable_trade_id,
            timeline: ActivityTimeline {
                source_timestamp: receipt.source_timestamp(),
                received_at: receipt.received_at(),
                available_at: receipt.available_at(),
                qualification_evaluated_at: receipt.qualification_evaluated_at(),
                qualification_valid_until: receipt.qualification_valid_until(),
            },
            authorization: receipt.source_authorization(),
            coverage: receipt.coverage_status(),
            trading: receipt.trading_status(),
            quality: receipt.recorded_quality(),
            trade,
        });
    }
    derive_canonical_activity(&canonical, selected.source_id(), measurement_at, policy)
}

fn derive_canonical_activity(
    deliveries: &[ActivityDelivery<'_>],
    source_id: &SourceId,
    measurement_at: Timestamp,
    policy: MarketActivityPolicy,
) -> Result<(MarketActivity, [u8; 32]), FairValueError> {
    let mut canonical = deliveries.to_vec();
    canonical.sort_unstable_by_key(|value| value.binding_digest);
    if canonical
        .windows(2)
        .any(|pair| pair[0].binding_digest == pair[1].binding_digest && pair[0] != pair[1])
    {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    canonical.dedup();
    canonical.sort_unstable_by(activity_delivery_order);

    let mut set_hash = CanonicalHasher::new(b"market-squawk/market-activity-set/v3");
    set_hash.fixed(policy.hash().bytes());
    set_hash.bytes(source_id.as_str().as_bytes());
    set_hash.u64(u64::try_from(canonical.len()).map_err(|_| FairValueError::Arithmetic)?);
    let mut qualifying = 0_usize;
    let mut aggregate_quantity_lots = 0_u64;
    let mut index = 0_usize;
    while index < canonical.len() {
        let first = canonical[index];
        let Some(trade_id) = first.stable_trade_id else {
            if first.trade.is_some() {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            set_hash.u8(0);
            first.hash_delivery_into(&mut set_hash);
            index = index.checked_add(1).ok_or(FairValueError::Arithmetic)?;
            continue;
        };
        let facts = first.trade.ok_or(FairValueError::InvalidProducerEvidence)?;
        let source_timestamp = first.timeline.source_timestamp;
        let mut end = index.checked_add(1).ok_or(FairValueError::Arithmetic)?;
        while end < canonical.len() && canonical[end].stable_trade_id == Some(trade_id) {
            if canonical[end].trade != Some(facts)
                || canonical[end].timeline.source_timestamp != source_timestamp
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            end = end.checked_add(1).ok_or(FairValueError::Arithmetic)?;
        }

        set_hash.u8(1);
        set_hash.bytes(trade_id.as_str().as_bytes());
        match source_timestamp {
            Some(value) => {
                set_hash.u8(1);
                set_hash.i64(value.unix_nanos());
            }
            None => set_hash.u8(0),
        }
        set_hash.i64(facts.price.get());
        set_hash.i64(facts.quantity.get());
        set_hash.u64(u64::try_from(end - index).map_err(|_| FairValueError::Arithmetic)?);
        let mut group_qualifies = false;
        for delivery in &canonical[index..end] {
            delivery.hash_delivery_into(&mut set_hash);
            group_qualifies |= delivery.is_qualifying(measurement_at, policy);
        }
        if group_qualifies {
            qualifying = qualifying
                .checked_add(1)
                .ok_or(FairValueError::Arithmetic)?;
            aggregate_quantity_lots = aggregate_quantity_lots
                .checked_add(
                    u64::try_from(facts.quantity.get())
                        .map_err(|_| FairValueError::InvalidProducerEvidence)?,
                )
                .ok_or(FairValueError::Arithmetic)?;
        }
        index = end;
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

fn activity_delivery_order(left: &ActivityDelivery<'_>, right: &ActivityDelivery<'_>) -> Ordering {
    match (left.stable_trade_id, right.stable_trade_id) {
        (Some(left_id), Some(right_id)) => left_id
            .as_str()
            .cmp(right_id.as_str())
            .then_with(|| left.binding_digest.cmp(&right.binding_digest)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.binding_digest.cmp(&right.binding_digest),
    }
}

const fn authorization_tag(value: SourceAuthorization) -> u8 {
    match value {
        SourceAuthorization::Authorized => 1,
        SourceAuthorization::Unauthorized => 2,
    }
}

const fn coverage_tag(value: CoverageStatus) -> u8 {
    match value {
        CoverageStatus::Sufficient => 1,
        CoverageStatus::Insufficient => 2,
        CoverageStatus::Unknown => 3,
    }
}

const fn trading_tag(value: TradingStatus) -> u8 {
    match value {
        TradingStatus::Active => 1,
        TradingStatus::Halted => 2,
        TradingStatus::Inactive => 3,
        TradingStatus::Delisted => 4,
    }
}

const fn quality_tag(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

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

    #[test]
    fn reconnect_redelivery_counts_one_economic_trade() -> Result<(), Box<dyn Error>> {
        let source = SourceId::try_from("kraken")?;
        let trade_id = SourceIdentifier::try_from("trade-42")?;
        let timeline = ActivityTimeline {
            source_timestamp: Some(Timestamp::from_unix_nanos(90)),
            received_at: Timestamp::from_unix_nanos(91),
            available_at: Timestamp::from_unix_nanos(92),
            qualification_evaluated_at: Timestamp::from_unix_nanos(93),
            qualification_valid_until: Timestamp::from_unix_nanos(110),
        };
        let first = ActivityDelivery {
            binding_digest: [1; 32],
            stable_trade_id: Some(&trade_id),
            timeline,
            authorization: SourceAuthorization::Authorized,
            coverage: CoverageStatus::Sufficient,
            trading: TradingStatus::Active,
            quality: DataQuality::DirectVerified,
            trade: Some(ActivityTradeFacts {
                price: PriceTicks::new(10_000),
                quantity: QuantityLots::new(1)?,
            }),
        };
        let redelivered_after_reconnect = ActivityDelivery {
            binding_digest: [2; 32],
            timeline: ActivityTimeline {
                received_at: Timestamp::from_unix_nanos(94),
                available_at: Timestamp::from_unix_nanos(95),
                qualification_evaluated_at: Timestamp::from_unix_nanos(96),
                ..timeline
            },
            ..first
        };
        let policy = MarketActivityPolicy::try_new(2, 2, 2, 20)?;

        let (activity, _) = derive_canonical_activity(
            &[first, redelivered_after_reconnect],
            &source,
            Timestamp::from_unix_nanos(100),
            policy,
        )?;

        assert_eq!(activity, MarketActivity::Inactive);
        Ok(())
    }
}
