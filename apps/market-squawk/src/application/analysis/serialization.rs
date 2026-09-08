//! Stable JSON projections for immutable analytical and feature identities.

use market_squawk_analytics::{
    FeatureDataType, FeatureMetadata, FeatureNullPolicy, FeatureOutputType, FeatureParameterValue,
    FeatureTimeSemantics, FeatureUnit, FeatureWarmUp, MissingValuePolicy, ShockComposition,
    VarianceConvention, WeightPolicy,
};
use market_squawk_data::{AnalyticalFeatureDataset, DatasetManifestRef};
use serde_json::{Value, json};

use crate::application::domain_support::encode_hex;

pub(super) fn feature_metadata_value(metadata: &FeatureMetadata) -> Value {
    json!({
        "kind": "feature_contract",
        "name": metadata.key().name(),
        "version": metadata.key().version().get(),
        "inputs": metadata
            .input_schema()
            .fields()
            .iter()
            .map(|input| json!({
                "name": input.name(),
                "dataType": feature_data_type_name(input.data_type()),
                "unit": feature_unit_name(input.unit()),
                "nullable": input.is_nullable()
            }))
            .collect::<Vec<_>>(),
        "inputSchemaDigest": encode_hex(metadata.input_schema_digest().as_bytes()),
        "parameters": metadata
            .parameters()
            .entries()
            .iter()
            .map(|parameter| json!({
                "name": parameter.name(),
                "value": feature_parameter_value(parameter.value())
            }))
            .collect::<Vec<_>>(),
        "timeSemantics": feature_time_semantics(metadata.time_semantics()),
        "warmUp": feature_warm_up(metadata.warm_up()),
        "nullPolicy": feature_null_policy_name(metadata.null_policy()),
        "outputType": feature_output_type_name(metadata.output_type()),
        "outputUnit": feature_unit_name(metadata.unit()),
        "liveCompatible": metadata.is_live_compatible(),
        "pointInTimeCompatible": metadata.is_point_in_time_compatible(),
        "implementationRevision": metadata.implementation_revision(),
        "implementationDigest": encode_hex(metadata.implementation_digest().as_bytes()),
        "semanticDigest": encode_hex(metadata.semantic_digest().as_bytes())
    })
}

pub(super) fn published_feature_dataset_value(dataset: &AnalyticalFeatureDataset) -> Value {
    let split = dataset.split_counts();
    json!({
        "kind": "feature_dataset",
        "manifest": manifest_value(dataset.generation().manifest()),
        "buildSpecDigest": dataset
            .generation()
            .build_spec_digest()
            .map(|digest| encode_hex(digest.digest().bytes())),
        "policyDigest": encode_hex(dataset.policy_digest().bytes()),
        "universeId": dataset.universe_id().as_str(),
        "universeDigest": encode_hex(dataset.universe_digest().bytes()),
        "pythonExportSha256": encode_hex(dataset.python_export_sha256().bytes()),
        "splitCounts": {
            "train": split.train_examples(),
            "validation": split.validation_examples(),
            "test": split.test_examples()
        }
    })
}

pub(super) fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "dataset": manifest.dataset_id().as_str(),
        "manifestVersion": manifest.manifest_version(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema_version().get(),
            "fingerprint": encode_hex(manifest.schema().fingerprint())
        },
        "contentHash": encode_hex(manifest.content_hash().bytes())
    })
}

fn feature_parameter_value(value: FeatureParameterValue) -> Value {
    match value {
        FeatureParameterValue::SignedInteger(value) => {
            json!({"kind": "signed_integer", "value": value})
        }
        FeatureParameterValue::UnsignedInteger(value) => {
            json!({"kind": "unsigned_integer", "value": value})
        }
        FeatureParameterValue::Boolean(value) => json!({"kind": "boolean", "value": value}),
        FeatureParameterValue::DurationNanos(value) => {
            json!({"kind": "duration_nanos", "value": value.get()})
        }
        FeatureParameterValue::VarianceConvention(value) => json!({
            "kind": "variance_convention",
            "value": variance_convention_name(value)
        }),
        FeatureParameterValue::MissingValuePolicy(value) => json!({
            "kind": "missing_value_policy",
            "value": missing_value_policy_name(value)
        }),
        FeatureParameterValue::WeightPolicy(value) => json!({
            "kind": "weight_policy",
            "value": weight_policy_name(value)
        }),
        FeatureParameterValue::RoundingPolicy(value) => {
            json!({"kind": "rounding_policy", "value": value})
        }
        FeatureParameterValue::ShockComposition(value) => json!({
            "kind": "shock_composition",
            "value": shock_composition_name(value)
        }),
    }
}

fn feature_time_semantics(value: FeatureTimeSemantics) -> Value {
    match value {
        FeatureTimeSemantics::EventTime => json!({"kind": "event_time"}),
        FeatureTimeSemantics::TrailingWindow { duration_nanos } => {
            json!({"kind": "trailing_window", "durationNanos": duration_nanos.get()})
        }
        FeatureTimeSemantics::CrossVenue { maximum_skew_nanos } => json!({
            "kind": "cross_venue",
            "maximumSkewNanos": maximum_skew_nanos.get()
        }),
    }
}

fn feature_warm_up(value: FeatureWarmUp) -> Value {
    match value {
        FeatureWarmUp::None => json!({"kind": "none"}),
        FeatureWarmUp::Observations(observations) => {
            json!({"kind": "observations", "observations": observations.get()})
        }
        FeatureWarmUp::DurationNanos(duration_nanos) => {
            json!({"kind": "duration_nanos", "durationNanos": duration_nanos.get()})
        }
        FeatureWarmUp::ObservationsAndDuration {
            observations,
            duration_nanos,
        } => json!({
            "kind": "observations_and_duration",
            "observations": observations.get(),
            "durationNanos": duration_nanos.get()
        }),
    }
}

const fn feature_null_policy_name(value: FeatureNullPolicy) -> &'static str {
    match value {
        FeatureNullPolicy::Unavailable => "unavailable",
        FeatureNullPolicy::WarmingUp => "warming_up",
        FeatureNullPolicy::IgnoreNullable => "ignore_nullable",
    }
}

const fn variance_convention_name(value: VarianceConvention) -> &'static str {
    match value {
        VarianceConvention::Population => "population",
        VarianceConvention::Sample => "sample",
    }
}

const fn missing_value_policy_name(value: MissingValuePolicy) -> &'static str {
    match value {
        MissingValuePolicy::Reject => "reject",
        MissingValuePolicy::Drop => "drop",
    }
}

const fn weight_policy_name(value: WeightPolicy) -> &'static str {
    match value {
        WeightPolicy::Equal => "equal",
        WeightPolicy::PositiveNormalized => "positive_normalized",
    }
}

const fn shock_composition_name(value: ShockComposition) -> &'static str {
    match value {
        ShockComposition::Additive => "additive",
        ShockComposition::Compounded => "compounded",
    }
}

const fn feature_output_type_name(value: FeatureOutputType) -> &'static str {
    match value {
        FeatureOutputType::PriceTicks => "price_ticks",
        FeatureOutputType::HalfTickPrice => "half_tick_price",
        FeatureOutputType::QuantityLots => "quantity_lots",
        FeatureOutputType::BasisPoints => "basis_points",
        FeatureOutputType::SignedInteger => "signed_integer",
        FeatureOutputType::UnsignedInteger => "unsigned_integer",
        FeatureOutputType::ExactRatio => "exact_ratio",
        FeatureOutputType::StatisticalF64 => "statistical_f64",
        FeatureOutputType::Decimal => "decimal",
        FeatureOutputType::Money => "money",
    }
}

const fn feature_unit_name(value: FeatureUnit) -> &'static str {
    match value {
        FeatureUnit::PriceTicks => "price_ticks",
        FeatureUnit::QuantityLots => "quantity_lots",
        FeatureUnit::BasisPoints => "basis_points",
        FeatureUnit::Ratio => "ratio",
        FeatureUnit::Return => "return",
        FeatureUnit::Volatility => "volatility",
        FeatureUnit::LotsPerSecond => "lots_per_second",
        FeatureUnit::Count => "count",
        FeatureUnit::Nanoseconds => "nanoseconds",
        FeatureUnit::Unitless => "unitless",
        FeatureUnit::Rate => "rate",
        FeatureUnit::CurrencyAmount => "currency_amount",
    }
}

const fn feature_data_type_name(value: FeatureDataType) -> &'static str {
    match value {
        FeatureDataType::PriceTicks => "price_ticks",
        FeatureDataType::QuantityLots => "quantity_lots",
        FeatureDataType::BasisPoints => "basis_points",
        FeatureDataType::Timestamp => "timestamp",
        FeatureDataType::AggressorSide => "aggressor_side",
        FeatureDataType::OrderSide => "order_side",
        FeatureDataType::ExactRatio => "exact_ratio",
        FeatureDataType::InstrumentId => "instrument_id",
        FeatureDataType::VenueId => "venue_id",
        FeatureDataType::SignedInteger => "signed_integer",
        FeatureDataType::UnsignedInteger => "unsigned_integer",
        FeatureDataType::Boolean => "boolean",
        FeatureDataType::StatisticalF64 => "statistical_f64",
        FeatureDataType::Decimal => "decimal",
        FeatureDataType::Money => "money",
        FeatureDataType::CanonicalIdentifier => "canonical_identifier",
        FeatureDataType::ExactRate => "exact_rate",
        FeatureDataType::DecimalMeasurement => "decimal_measurement",
        FeatureDataType::MonetaryValue => "monetary_value",
        FeatureDataType::StatisticalLocation => "statistical_location",
        FeatureDataType::StatisticalDispersion => "statistical_dispersion",
    }
}
