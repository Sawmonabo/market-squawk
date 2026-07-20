//! Bounded immutable feature snapshot construction outside the event-to-action path.

use std::mem::size_of;

use market_squawk_analytics::{FeatureScalar, REQUIRED_LIVE_FEATURE_COUNT, RequiredLiveFeature};

use super::{FeatureSetState, RouteFeatureError, RouteFeatureState};
use crate::snapshot::{
    LiveFeatureScalarSnapshot, LiveFeatureSetSnapshot, LiveFeatureSnapshot,
    LiveFeatureValueSnapshot, SnapshotDimension,
};

impl RouteFeatureState {
    pub(crate) fn build_snapshot(
        &self,
        maximum_bytes: usize,
    ) -> Result<LiveFeatureSnapshot, RouteFeatureError> {
        let base = size_of::<LiveFeatureSnapshot>();
        if maximum_bytes < base {
            return Err(RouteFeatureError::SnapshotConstruction);
        }
        let mut ordered = self.active_sets().collect::<Vec<_>>();
        ordered.sort_by(compare_sets);
        let available = ordered.len();
        let mut sets = Vec::new();
        sets.try_reserve_exact(available)
            .map_err(|_| RouteFeatureError::Allocation)?;
        let mut retained_bytes = base;
        for set in ordered {
            let snapshot = self.snapshot_set(set)?;
            let charge =
                retained_set_bytes(&snapshot).ok_or(RouteFeatureError::SnapshotConstruction)?;
            let candidate = retained_bytes
                .checked_add(charge)
                .ok_or(RouteFeatureError::SnapshotConstruction)?;
            if candidate > maximum_bytes {
                break;
            }
            retained_bytes = candidate;
            sets.push(snapshot);
        }
        let returned = sets.len();
        Ok(LiveFeatureSnapshot {
            sets: sets.into_boxed_slice(),
            set_dimension: SnapshotDimension::from_counts(available, returned, available)
                .map_err(|_| RouteFeatureError::SnapshotConstruction)?,
            retained_bytes: u64::try_from(retained_bytes)
                .map_err(|_| RouteFeatureError::SnapshotConstruction)?,
        })
    }

    fn snapshot_set(
        &self,
        set: &FeatureSetState,
    ) -> Result<LiveFeatureSetSnapshot, RouteFeatureError> {
        let identity = set
            .identity()
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        let generation = set
            .generation()
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(REQUIRED_LIVE_FEATURE_COUNT)
            .map_err(|_| RouteFeatureError::Allocation)?;
        for (feature, value) in RequiredLiveFeature::ALL.iter().zip(set.values()) {
            let metadata = self
                .registry()
                .entries()
                .find(|entry| entry.key().name() == feature.name())
                .ok_or(RouteFeatureError::InternalStateInvariant)?;
            values.push(LiveFeatureValueSnapshot {
                name: metadata.key().name().to_owned(),
                version: metadata.key().version(),
                observed_at: value.observed_at(),
                validity: value.validity().into(),
                scalar: value.value().copied().map(snapshot_scalar),
            });
        }
        Ok(LiveFeatureSetSnapshot {
            source: identity.source_id().clone(),
            venue: identity.venue().clone(),
            instrument: identity.instrument(),
            provider_product: identity.provider_product().clone(),
            provider_channel: identity.provider_channel().clone(),
            connection_generation: generation,
            values: values.into_boxed_slice(),
            value_dimension: SnapshotDimension::from_counts(
                REQUIRED_LIVE_FEATURE_COUNT,
                REQUIRED_LIVE_FEATURE_COUNT,
                REQUIRED_LIVE_FEATURE_COUNT,
            )
            .map_err(|_| RouteFeatureError::SnapshotConstruction)?,
        })
    }
}

fn compare_sets(left: &&FeatureSetState, right: &&FeatureSetState) -> std::cmp::Ordering {
    match (left.identity(), right.identity()) {
        (Some(left), Some(right)) => left
            .source_id()
            .as_str()
            .cmp(right.source_id().as_str())
            .then_with(|| left.venue().as_str().cmp(right.venue().as_str()))
            .then_with(|| left.instrument().cmp(&right.instrument()))
            .then_with(|| {
                left.provider_product()
                    .as_source_identifier()
                    .as_str()
                    .cmp(right.provider_product().as_source_identifier().as_str())
            })
            .then_with(|| {
                left.provider_channel()
                    .as_source_identifier()
                    .as_str()
                    .cmp(right.provider_channel().as_source_identifier().as_str())
            }),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn snapshot_scalar(value: FeatureScalar) -> LiveFeatureScalarSnapshot {
    match value {
        FeatureScalar::PriceTicks(value) => LiveFeatureScalarSnapshot::PriceTicks(value.get()),
        FeatureScalar::HalfTickPrice(value) => {
            LiveFeatureScalarSnapshot::HalfTickPrice(value.half_ticks())
        }
        FeatureScalar::QuantityLots(value) => LiveFeatureScalarSnapshot::QuantityLots(value.get()),
        FeatureScalar::BasisPoints(value) => LiveFeatureScalarSnapshot::BasisPoints(value.get()),
        FeatureScalar::SignedInteger(value) => LiveFeatureScalarSnapshot::SignedInteger(value),
        FeatureScalar::UnsignedInteger(value) => LiveFeatureScalarSnapshot::UnsignedInteger(value),
        FeatureScalar::ExactRatio(value) => LiveFeatureScalarSnapshot::ExactRatio {
            numerator: value.numerator(),
            denominator: value.denominator().get(),
        },
        FeatureScalar::Statistical(value) => {
            LiveFeatureScalarSnapshot::StatisticalBits(value.get().to_bits())
        }
    }
}

fn retained_set_bytes(snapshot: &LiveFeatureSetSnapshot) -> Option<usize> {
    snapshot
        .source
        .retained_bytes()
        .checked_add(snapshot.venue.retained_bytes())?
        .checked_add(
            snapshot
                .provider_product
                .as_source_identifier()
                .retained_bytes(),
        )?
        .checked_add(
            snapshot
                .provider_channel
                .as_source_identifier()
                .retained_bytes(),
        )?
        .checked_add(size_of::<LiveFeatureSetSnapshot>())?
        .checked_add(
            snapshot
                .values
                .len()
                .checked_mul(size_of::<LiveFeatureValueSnapshot>())?,
        )?
        .checked_add(snapshot.values.iter().try_fold(0_usize, |total, value| {
            total.checked_add(value.name.capacity())
        })?)
}
