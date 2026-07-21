//! Canonical feature-schema and semantic digest encoding.

use sha2::{Digest, Sha256};

use super::{
    FeatureDataType, FeatureImplementationDigest, FeatureInputSchema, FeatureInputSchemaDigest,
    FeatureKey, FeatureMetadataError, FeatureNullPolicy, FeatureOutputType, FeatureParameterValue,
    FeatureParameters, FeatureSemanticDigest, FeatureTimeSemantics, FeatureUnit, FeatureWarmUp,
};

pub(super) fn input_schema_digest(schema: &FeatureInputSchema) -> FeatureInputSchemaDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk.feature-input-schema.v1\0");
    update_input_schema(&mut hasher, schema);
    FeatureInputSchemaDigest(finalize_sha256(hasher))
}

pub(crate) fn implementation_digest_for_identity(
    identity: &[u8],
) -> Result<FeatureImplementationDigest, FeatureMetadataError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk.known-feature-implementation.v1\0");
    update_bytes(&mut hasher, identity);
    FeatureImplementationDigest::try_from_sha256(finalize_sha256(hasher))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn semantic_digest(
    key: &FeatureKey,
    input_schema: &FeatureInputSchema,
    parameters: &FeatureParameters,
    time_semantics: FeatureTimeSemantics,
    warm_up: FeatureWarmUp,
    null_policy: FeatureNullPolicy,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    live_compatible: bool,
    point_in_time_compatible: bool,
    implementation_revision: &str,
    implementation_digest: FeatureImplementationDigest,
) -> FeatureSemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk.feature-metadata.v1\0");
    update_bytes(&mut hasher, key.name.as_bytes());
    hasher.update(key.version.get().to_be_bytes());
    update_input_schema(&mut hasher, input_schema);
    update_parameters(&mut hasher, parameters);
    update_time_semantics(&mut hasher, time_semantics);
    update_warm_up(&mut hasher, warm_up);
    hasher.update([null_policy_tag(null_policy)]);
    hasher.update([output_type_tag(output_type)]);
    hasher.update([unit_tag(unit)]);
    hasher.update([
        u8::from(live_compatible),
        u8::from(point_in_time_compatible),
    ]);
    update_bytes(&mut hasher, implementation_revision.as_bytes());
    hasher.update(implementation_digest.as_bytes());
    FeatureSemanticDigest(finalize_sha256(hasher))
}

fn update_input_schema(hasher: &mut Sha256, schema: &FeatureInputSchema) {
    update_len(hasher, schema.0.len());
    for input in &schema.0 {
        update_bytes(hasher, input.name.as_bytes());
        hasher.update([data_type_tag(input.data_type)]);
        hasher.update([unit_tag(input.unit)]);
        hasher.update([u8::from(input.nullable)]);
    }
}

fn update_parameters(hasher: &mut Sha256, parameters: &FeatureParameters) {
    update_len(hasher, parameters.0.len());
    for parameter in &parameters.0 {
        update_bytes(hasher, parameter.name.as_bytes());
        match parameter.value {
            FeatureParameterValue::SignedInteger(value) => {
                hasher.update([0]);
                hasher.update(value.to_be_bytes());
            }
            FeatureParameterValue::UnsignedInteger(value) => {
                hasher.update([1]);
                hasher.update(value.to_be_bytes());
            }
            FeatureParameterValue::Boolean(value) => hasher.update([2, u8::from(value)]),
            FeatureParameterValue::DurationNanos(value) => {
                hasher.update([3]);
                hasher.update(value.get().to_be_bytes());
            }
        }
    }
}

fn update_time_semantics(hasher: &mut Sha256, semantics: FeatureTimeSemantics) {
    match semantics {
        FeatureTimeSemantics::EventTime => hasher.update([0]),
        FeatureTimeSemantics::TrailingWindow { duration_nanos } => {
            hasher.update([1]);
            hasher.update(duration_nanos.get().to_be_bytes());
        }
        FeatureTimeSemantics::CrossVenue { maximum_skew_nanos } => {
            hasher.update([2]);
            hasher.update(maximum_skew_nanos.get().to_be_bytes());
        }
    }
}

fn update_warm_up(hasher: &mut Sha256, warm_up: FeatureWarmUp) {
    match warm_up {
        FeatureWarmUp::None => hasher.update([0]),
        FeatureWarmUp::Observations(observations) => {
            hasher.update([1]);
            hasher.update(observations.get().to_be_bytes());
        }
        FeatureWarmUp::DurationNanos(duration) => {
            hasher.update([2]);
            hasher.update(duration.get().to_be_bytes());
        }
        FeatureWarmUp::ObservationsAndDuration {
            observations,
            duration_nanos,
        } => {
            hasher.update([3]);
            hasher.update(observations.get().to_be_bytes());
            hasher.update(duration_nanos.get().to_be_bytes());
        }
    }
}

fn update_len(hasher: &mut Sha256, len: usize) {
    // Every caller is bounded far below `u64::MAX`; the fallback keeps encoding total and
    // deterministic even on a hypothetical target whose `usize` is wider than 64 bits.
    let canonical_len = u64::try_from(len).unwrap_or(u64::MAX);
    hasher.update(canonical_len.to_be_bytes());
}

fn update_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    update_len(hasher, bytes.len());
    hasher.update(bytes);
}

fn finalize_sha256(hasher: Sha256) -> [u8; 32] {
    hasher.finalize().into()
}

const fn data_type_tag(data_type: FeatureDataType) -> u8 {
    match data_type {
        FeatureDataType::PriceTicks => 0,
        FeatureDataType::QuantityLots => 1,
        FeatureDataType::BasisPoints => 2,
        FeatureDataType::Timestamp => 3,
        FeatureDataType::AggressorSide => 4,
        FeatureDataType::OrderSide => 5,
        FeatureDataType::ExactRatio => 6,
        FeatureDataType::InstrumentId => 7,
        FeatureDataType::VenueId => 8,
        FeatureDataType::SignedInteger => 9,
        FeatureDataType::UnsignedInteger => 10,
        FeatureDataType::Boolean => 11,
        FeatureDataType::StatisticalF64 => 12,
        FeatureDataType::Decimal => 13,
        FeatureDataType::Money => 14,
    }
}

const fn unit_tag(unit: FeatureUnit) -> u8 {
    match unit {
        FeatureUnit::PriceTicks => 0,
        FeatureUnit::QuantityLots => 1,
        FeatureUnit::BasisPoints => 2,
        FeatureUnit::Ratio => 3,
        FeatureUnit::Return => 4,
        FeatureUnit::Volatility => 5,
        FeatureUnit::LotsPerSecond => 6,
        FeatureUnit::Count => 7,
        FeatureUnit::Nanoseconds => 8,
        FeatureUnit::Unitless => 9,
        FeatureUnit::Rate => 10,
        FeatureUnit::CurrencyAmount => 11,
    }
}

const fn null_policy_tag(policy: FeatureNullPolicy) -> u8 {
    match policy {
        FeatureNullPolicy::Unavailable => 0,
        FeatureNullPolicy::WarmingUp => 1,
        FeatureNullPolicy::IgnoreNullable => 2,
    }
}

const fn output_type_tag(output_type: FeatureOutputType) -> u8 {
    match output_type {
        FeatureOutputType::PriceTicks => 0,
        FeatureOutputType::HalfTickPrice => 1,
        FeatureOutputType::QuantityLots => 2,
        FeatureOutputType::BasisPoints => 3,
        FeatureOutputType::SignedInteger => 4,
        FeatureOutputType::UnsignedInteger => 5,
        FeatureOutputType::ExactRatio => 6,
        FeatureOutputType::StatisticalF64 => 7,
        FeatureOutputType::Decimal => 8,
        FeatureOutputType::Money => 9,
    }
}
