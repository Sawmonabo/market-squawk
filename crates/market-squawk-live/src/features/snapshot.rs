//! Bounded immutable feature snapshot construction outside the event-to-action path.

use std::mem::size_of;

use market_squawk_analytics::{FeatureScalar, REQUIRED_LIVE_FEATURE_COUNT, RequiredLiveFeature};
use market_squawk_domain::{
    ConnectionGeneration, DigestAlgorithm, EvidenceDigest, InstrumentId, ProviderChannel,
    ProviderProduct, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use sha2::{Digest, Sha256};

use super::{FeatureSetState, RouteFeatureError, RouteFeatureState};
use crate::snapshot::{
    LiveFeatureScalarSnapshot, LiveFeatureSetSnapshot, LiveFeatureSnapshot,
    LiveFeatureValueSnapshot, SnapshotDimension,
};

const LIVE_FEATURE_SET_DIGEST_DOMAIN: &[u8] = b"MSQKLIVEFEATURESET\x01";

impl RouteFeatureState {
    /// Additional heap ownership while one route constructs its feature snapshot.
    ///
    /// Accepted sets belong to the enclosing shard publication's retained-byte budget. Outside
    /// that budget, construction may own a complete candidate before checking its fit, unused
    /// reserved set slots, sorting references, and Vec-to-box normalization overlap. An actor
    /// builds one route at a time; readers retain publications, never this workspace.
    pub(crate) fn snapshot_construction_scratch_bytes(maximum_sets: usize) -> Option<usize> {
        if maximum_sets == 0 {
            return Some(0);
        }
        let values =
            REQUIRED_LIVE_FEATURE_COUNT.checked_mul(size_of::<LiveFeatureValueSnapshot>())?;
        let names = RequiredLiveFeature::ALL
            .iter()
            .try_fold(0_usize, |bytes, feature| {
                bytes.checked_add(feature.name().len())
            })?;
        let identities = SourceId::MAX_LENGTH
            .checked_add(VenueId::MAX_LENGTH)?
            .checked_add(2_usize.checked_mul(SourceIdentifier::MAX_LENGTH)?)?;
        // Four identity strings and one owned name per value, with allocator/header slack.
        let text_slack = REQUIRED_LIVE_FEATURE_COUNT
            .checked_add(4)?
            .checked_mul(64)?;
        let candidate = size_of::<LiveFeatureSetSnapshot>()
            .checked_add(identities)?
            .checked_add(values)?
            .checked_add(names)?
            .checked_add(text_slack)?;
        let set_slots = maximum_sets.checked_mul(size_of::<LiveFeatureSetSnapshot>())?;
        // Collect growth and stable-sort workspace coexist with the original reference vector.
        let ordering =
            maximum_sets.checked_mul(4_usize.checked_mul(size_of::<&FeatureSetState>())?)?;
        candidate
            .checked_add(values)?
            .checked_add(2_usize.checked_mul(set_slots)?)?
            .checked_add(ordering)?
            .checked_add(3 * 64)
    }

    pub(crate) fn build_snapshot(
        &self,
        maximum_bytes: usize,
        available_at: Timestamp,
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
            let snapshot = self.snapshot_set(set, available_at)?;
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
        available_at: Timestamp,
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
                .find(|entry| {
                    entry.key().name() == feature.name()
                        && entry.key().version() == std::num::NonZeroU32::MIN
                })
                .ok_or(RouteFeatureError::InternalStateInvariant)?;
            if value.observed_at() > available_at
                || value.validity().is_ready() != value.value().is_some()
                || value
                    .value()
                    .is_some_and(|scalar| scalar.output_type() != metadata.output_type())
            {
                return Err(RouteFeatureError::SnapshotConstruction);
            }
            values.push(LiveFeatureValueSnapshot::new(
                metadata.key().name().to_owned(),
                metadata.key().version(),
                metadata.semantic_digest().as_bytes(),
                metadata.implementation_digest().as_bytes(),
                metadata.output_type(),
                metadata.unit(),
                value.observed_at(),
                value.validity(),
                value.value().copied().map(snapshot_scalar),
            ));
        }
        let source = identity.source_id().clone();
        let venue = identity.venue().clone();
        let instrument = identity.instrument();
        let provider_product = identity.provider_product().clone();
        let provider_channel = identity.provider_channel().clone();
        let content_digest = feature_set_content_digest(
            &source,
            &venue,
            instrument,
            &provider_product,
            &provider_channel,
            generation,
            available_at,
            &values,
        )?;
        Ok(LiveFeatureSetSnapshot {
            source,
            venue,
            instrument,
            provider_product,
            provider_channel,
            connection_generation: generation,
            available_at,
            content_digest,
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

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds each independent stream-identity and timing dimension"
)]
fn feature_set_content_digest(
    source: &SourceId,
    venue: &VenueId,
    instrument: InstrumentId,
    provider_product: &ProviderProduct,
    provider_channel: &ProviderChannel,
    generation: ConnectionGeneration,
    available_at: Timestamp,
    values: &[LiveFeatureValueSnapshot],
) -> Result<EvidenceDigest, RouteFeatureError> {
    let mut hasher = Sha256::new();
    hasher.update(LIVE_FEATURE_SET_DIGEST_DOMAIN);
    digest_component(&mut hasher, source.as_str().as_bytes())?;
    digest_component(&mut hasher, venue.as_str().as_bytes())?;
    hasher.update(instrument.as_uuid().as_bytes());
    digest_component(
        &mut hasher,
        provider_product.as_source_identifier().as_str().as_bytes(),
    )?;
    digest_component(
        &mut hasher,
        provider_channel.as_source_identifier().as_str().as_bytes(),
    )?;
    hasher.update(generation.get().to_be_bytes());
    hasher.update(available_at.unix_nanos().to_be_bytes());
    hasher.update(
        u32::try_from(values.len())
            .map_err(|_| RouteFeatureError::SnapshotConstruction)?
            .to_be_bytes(),
    );
    for value in values {
        digest_component(&mut hasher, value.name.as_bytes())?;
        hasher.update(value.version.get().to_be_bytes());
        hasher.update(value.semantic_digest);
        hasher.update(value.implementation_digest);
        hasher.update([value.output_type_digest_tag()]);
        hasher.update([value.unit_digest_tag()]);
        hasher.update(value.observed_at.unix_nanos().to_be_bytes());
        hasher.update([value.validity_digest_tag()]);
        digest_scalar(&mut hasher, value.scalar.as_ref());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn digest_component(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), RouteFeatureError> {
    hasher.update(
        u32::try_from(bytes.len())
            .map_err(|_| RouteFeatureError::SnapshotConstruction)?
            .to_be_bytes(),
    );
    hasher.update(bytes);
    Ok(())
}

fn digest_scalar(hasher: &mut Sha256, scalar: Option<&LiveFeatureScalarSnapshot>) {
    let Some(scalar) = scalar else {
        hasher.update([0]);
        return;
    };
    match scalar {
        LiveFeatureScalarSnapshot::PriceTicks(value) => {
            hasher.update([1]);
            hasher.update(value.to_be_bytes());
        }
        LiveFeatureScalarSnapshot::HalfTickPrice(value) => {
            hasher.update([2]);
            hasher.update(value.to_be_bytes());
        }
        LiveFeatureScalarSnapshot::QuantityLots(value) => {
            hasher.update([3]);
            hasher.update(value.to_be_bytes());
        }
        LiveFeatureScalarSnapshot::BasisPoints(value) => {
            hasher.update([4]);
            hasher.update(value.to_be_bytes());
        }
        LiveFeatureScalarSnapshot::SignedInteger(value) => {
            hasher.update([5]);
            hasher.update(value.to_be_bytes());
        }
        LiveFeatureScalarSnapshot::UnsignedInteger(value) => {
            hasher.update([6]);
            hasher.update(value.to_be_bytes());
        }
        LiveFeatureScalarSnapshot::ExactRatio {
            numerator,
            denominator,
        } => {
            hasher.update([7]);
            hasher.update(numerator.to_be_bytes());
            hasher.update(denominator.to_be_bytes());
        }
        LiveFeatureScalarSnapshot::StatisticalBits(value) => {
            hasher.update([8]);
            hasher.update(value.to_be_bytes());
        }
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
