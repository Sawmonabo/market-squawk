//! Immutable feature identity, schema, parameters, and compatibility metadata.

use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64};

use thiserror::Error;

pub(crate) mod digest;

/// Maximum UTF-8 bytes in a stable feature name.
pub const MAX_FEATURE_NAME_BYTES: usize = 96;
/// Maximum UTF-8 bytes in an input or parameter field name.
pub const MAX_FEATURE_FIELD_NAME_BYTES: usize = 64;
/// Maximum inputs retained by one feature metadata record.
pub const MAX_FEATURE_INPUTS: usize = 32;
/// Maximum parameters retained by one feature metadata record.
pub const MAX_FEATURE_PARAMETERS: usize = 32;
/// Maximum UTF-8 bytes in an implementation revision.
pub const MAX_IMPLEMENTATION_REVISION_BYTES: usize = 128;

/// Deterministic SHA-256 digest of one ordered feature input schema.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FeatureInputSchemaDigest([u8; 32]);

impl FeatureInputSchemaDigest {
    /// Returns the exact SHA-256 bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Nonzero SHA-256 identity of code known to the local build.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FeatureImplementationDigest([u8; 32]);

impl FeatureImplementationDigest {
    /// Constructs an implementation identity from exact SHA-256 bytes.
    ///
    /// This validates representation only. Registration separately requires the digest to appear
    /// in the code-owned local implementation catalog, so caller-provided metadata cannot grant
    /// itself implementation authority.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureMetadataError::ZeroImplementationDigest`] for the all-zero sentinel.
    pub fn try_from_sha256(bytes: [u8; 32]) -> Result<Self, FeatureMetadataError> {
        if bytes == [0; 32] {
            Err(FeatureMetadataError::ZeroImplementationDigest)
        } else {
            Ok(Self(bytes))
        }
    }

    /// Returns the exact SHA-256 bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// SHA-256 commitment to every execution-relevant feature metadata field.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FeatureSemanticDigest([u8; 32]);

impl FeatureSemanticDigest {
    /// Returns the exact SHA-256 bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Stable feature identity consisting of a canonical name and nonzero version.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FeatureKey {
    name: String,
    version: NonZeroU32,
}

impl FeatureKey {
    /// Constructs a bounded canonical feature key.
    ///
    /// Names begin with a lowercase ASCII letter and otherwise contain lowercase ASCII letters,
    /// digits, `.`, `_`, or `-` only. This makes persisted identities locale-independent.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureMetadataError::InvalidFeatureName`] for an invalid name.
    pub fn try_new(name: &str, version: NonZeroU32) -> Result<Self, FeatureMetadataError> {
        validate_identifier(name, MAX_FEATURE_NAME_BYTES)
            .map_err(|()| FeatureMetadataError::InvalidFeatureName)?;
        Ok(Self {
            name: name.to_owned(),
            version,
        })
    }

    /// Returns the canonical feature name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the nonzero metadata version.
    #[must_use]
    pub const fn version(&self) -> NonZeroU32 {
        self.version
    }

    fn dynamic_retained_bytes(&self) -> usize {
        self.name.capacity()
    }
}

/// Primitive or domain type consumed by a feature kernel.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FeatureDataType {
    /// Exact instrument price ticks.
    PriceTicks,
    /// Exact instrument quantity lots.
    QuantityLots,
    /// Exact signed basis points.
    BasisPoints,
    /// UTC Unix nanoseconds.
    Timestamp,
    /// Canonical aggressor-side classification.
    AggressorSide,
    /// Canonical order-side classification.
    OrderSide,
    /// Exact numerator and positive denominator.
    ExactRatio,
    /// Internal instrument identity.
    InstrumentId,
    /// Internal venue identity.
    VenueId,
    /// Signed integer input.
    SignedInteger,
    /// Unsigned integer input.
    UnsignedInteger,
    /// Boolean input.
    Boolean,
    /// Finite floating-point statistical input.
    StatisticalF64,
    /// Exact analytical decimal input.
    Decimal,
    /// Exact amount carrying its own currency.
    Money,
}

/// Unit attached to an input or output field.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FeatureUnit {
    /// Instrument price ticks.
    PriceTicks,
    /// Instrument quantity lots.
    QuantityLots,
    /// Basis points.
    BasisPoints,
    /// A dimensionless exact ratio.
    Ratio,
    /// A dimensionless statistical return.
    Return,
    /// A dimensionless statistical volatility.
    Volatility,
    /// Quantity lots per second.
    LotsPerSecond,
    /// Number of observations or events.
    Count,
    /// Nanoseconds.
    Nanoseconds,
    /// Dimensionless value or identity.
    Unitless,
    /// Exact or statistical interest, discount, or growth rate.
    Rate,
    /// Currency amount whose currency is carried by the Money value.
    CurrencyAmount,
}

/// One ordered field in a feature's exact input schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureInput {
    name: String,
    data_type: FeatureDataType,
    unit: FeatureUnit,
    nullable: bool,
}

impl FeatureInput {
    /// Constructs one bounded, type-and-unit-consistent input field.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an invalid name or incompatible type and unit.
    pub fn try_new(
        name: &str,
        data_type: FeatureDataType,
        unit: FeatureUnit,
        nullable: bool,
    ) -> Result<Self, FeatureMetadataError> {
        validate_identifier(name, MAX_FEATURE_FIELD_NAME_BYTES)
            .map_err(|()| FeatureMetadataError::InvalidFieldName)?;
        if !input_unit_is_compatible(data_type, unit) {
            return Err(FeatureMetadataError::IncompatibleInputUnit);
        }
        Ok(Self {
            name: name.to_owned(),
            data_type,
            unit,
            nullable,
        })
    }

    /// Returns the canonical input name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact input data type.
    #[must_use]
    pub const fn data_type(&self) -> FeatureDataType {
        self.data_type
    }

    /// Returns the input unit.
    #[must_use]
    pub const fn unit(&self) -> FeatureUnit {
        self.unit
    }

    /// Returns whether an absent input is representable by this schema.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }
}

/// Nonempty ordered input schema for one feature version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureInputSchema(Box<[FeatureInput]>);

impl FeatureInputSchema {
    /// Validates and retains an exact ordered input schema.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an empty, oversized, or duplicate-name schema.
    pub fn try_new(inputs: Vec<FeatureInput>) -> Result<Self, FeatureMetadataError> {
        if inputs.is_empty() || inputs.len() > MAX_FEATURE_INPUTS {
            return Err(FeatureMetadataError::InvalidInputCount);
        }
        if has_duplicate_names(inputs.iter().map(FeatureInput::name)) {
            return Err(FeatureMetadataError::DuplicateInputName);
        }
        Ok(Self(inputs.into_boxed_slice()))
    }

    /// Returns the ordered input fields without allocation.
    #[must_use]
    pub fn fields(&self) -> &[FeatureInput] {
        &self.0
    }

    /// Returns a deterministic SHA-256 commitment to field order, names, types, units, and
    /// nullability.
    #[must_use]
    pub fn digest(&self) -> FeatureInputSchemaDigest {
        digest::input_schema_digest(self)
    }

    fn checked_dynamic_retained_bytes(&self) -> Option<usize> {
        let mut total = allocation_bytes::<FeatureInput>(self.0.len())?;
        for input in &self.0 {
            total = total.checked_add(input.name.capacity())?;
        }
        Some(total)
    }
}

/// Typed parameter value retained in registry metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureParameterValue {
    /// Signed integer parameter.
    SignedInteger(i64),
    /// Unsigned integer parameter.
    UnsignedInteger(u64),
    /// Boolean parameter.
    Boolean(bool),
    /// Positive duration in nanoseconds.
    DurationNanos(NonZeroU64),
}

/// One bounded, named feature parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureParameter {
    name: String,
    value: FeatureParameterValue,
}

impl FeatureParameter {
    /// Constructs a canonical parameter entry.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureMetadataError::InvalidFieldName`] for a non-canonical name.
    pub fn try_new(name: &str, value: FeatureParameterValue) -> Result<Self, FeatureMetadataError> {
        validate_identifier(name, MAX_FEATURE_FIELD_NAME_BYTES)
            .map_err(|()| FeatureMetadataError::InvalidFieldName)?;
        Ok(Self {
            name: name.to_owned(),
            value,
        })
    }

    /// Returns the canonical parameter name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact typed parameter value.
    #[must_use]
    pub const fn value(&self) -> FeatureParameterValue {
        self.value
    }
}

/// Bounded ordered parameters for one feature version.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FeatureParameters(Box<[FeatureParameter]>);

impl FeatureParameters {
    /// Validates and retains ordered parameters. Empty parameter sets are valid.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an excessive count or duplicate names.
    pub fn try_new(parameters: Vec<FeatureParameter>) -> Result<Self, FeatureMetadataError> {
        if parameters.len() > MAX_FEATURE_PARAMETERS {
            return Err(FeatureMetadataError::InvalidParameterCount);
        }
        if has_duplicate_names(parameters.iter().map(FeatureParameter::name)) {
            return Err(FeatureMetadataError::DuplicateParameterName);
        }
        Ok(Self(parameters.into_boxed_slice()))
    }

    /// Returns the ordered parameter entries without allocation.
    #[must_use]
    pub fn entries(&self) -> &[FeatureParameter] {
        &self.0
    }

    fn checked_dynamic_retained_bytes(&self) -> Option<usize> {
        let mut total = allocation_bytes::<FeatureParameter>(self.0.len())?;
        for parameter in &self.0 {
            total = total.checked_add(parameter.name.capacity())?;
        }
        Some(total)
    }
}

/// Event-time interpretation used by one feature version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureTimeSemantics {
    /// The output represents the triggering event's timestamp.
    EventTime,
    /// The output represents a positive trailing event-time window.
    TrailingWindow {
        /// Inclusive trailing duration in nanoseconds.
        duration_nanos: NonZeroU64,
    },
    /// The output compares venues at a bounded event-time skew.
    CrossVenue {
        /// Maximum accepted skew between venue observations.
        maximum_skew_nanos: NonZeroU64,
    },
}

/// Warm-up requirement before a feature may become ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureWarmUp {
    /// No warm-up is required once inputs are present.
    None,
    /// A positive number of observations is required.
    Observations(NonZeroU32),
    /// A positive event-time duration is required.
    DurationNanos(NonZeroU64),
    /// Both positive observation and duration thresholds are required.
    ObservationsAndDuration {
        /// Minimum observation count.
        observations: NonZeroU32,
        /// Minimum event-time duration.
        duration_nanos: NonZeroU64,
    },
}

/// Closed missing-input behavior for one feature version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureNullPolicy {
    /// Any missing required input makes the output unavailable.
    Unavailable,
    /// A missing input keeps the feature in warm-up.
    WarmingUp,
    /// Missing nullable inputs are ignored; required inputs still make the output unavailable.
    IgnoreNullable,
}

/// Scalar representation produced by one feature version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureOutputType {
    /// Exact price ticks.
    PriceTicks,
    /// Exact price represented in half-tick units.
    HalfTickPrice,
    /// Exact quantity lots.
    QuantityLots,
    /// Exact signed basis points.
    BasisPoints,
    /// Signed `i128`.
    SignedInteger,
    /// Unsigned `u128`.
    UnsignedInteger,
    /// Exact numerator and positive denominator.
    ExactRatio,
    /// Finite floating point admitted only for statistical calculations.
    StatisticalF64,
    /// Exact analytical decimal.
    Decimal,
    /// Exact amount carrying its own currency.
    Money,
}

/// Complete immutable metadata for one feature name and version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureMetadata {
    key: FeatureKey,
    input_schema: FeatureInputSchema,
    input_schema_digest: FeatureInputSchemaDigest,
    parameters: FeatureParameters,
    time_semantics: FeatureTimeSemantics,
    warm_up: FeatureWarmUp,
    null_policy: FeatureNullPolicy,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    live_compatible: bool,
    point_in_time_compatible: bool,
    implementation_revision: String,
    implementation_digest: FeatureImplementationDigest,
    semantic_digest: FeatureSemanticDigest,
    code_owned: bool,
}

impl FeatureMetadata {
    /// Constructs complete, bounded metadata for one immutable feature version.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an invalid revision, output/unit contradiction, or metadata that
    /// is compatible with neither live nor point-in-time use.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        key: FeatureKey,
        input_schema: FeatureInputSchema,
        parameters: FeatureParameters,
        time_semantics: FeatureTimeSemantics,
        warm_up: FeatureWarmUp,
        null_policy: FeatureNullPolicy,
        output_type: FeatureOutputType,
        unit: FeatureUnit,
        live_compatible: bool,
        point_in_time_compatible: bool,
        implementation_revision: &str,
        implementation_digest: FeatureImplementationDigest,
    ) -> Result<Self, FeatureMetadataError> {
        Self::try_new_with_authority(
            key,
            input_schema,
            parameters,
            time_semantics,
            warm_up,
            null_policy,
            output_type,
            unit,
            live_compatible,
            point_in_time_compatible,
            implementation_revision,
            implementation_digest,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new_code_owned(
        key: FeatureKey,
        input_schema: FeatureInputSchema,
        parameters: FeatureParameters,
        time_semantics: FeatureTimeSemantics,
        warm_up: FeatureWarmUp,
        null_policy: FeatureNullPolicy,
        output_type: FeatureOutputType,
        unit: FeatureUnit,
        live_compatible: bool,
        point_in_time_compatible: bool,
        implementation_revision: &str,
        implementation_digest: FeatureImplementationDigest,
    ) -> Result<Self, FeatureMetadataError> {
        Self::try_new_with_authority(
            key,
            input_schema,
            parameters,
            time_semantics,
            warm_up,
            null_policy,
            output_type,
            unit,
            live_compatible,
            point_in_time_compatible,
            implementation_revision,
            implementation_digest,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_with_authority(
        key: FeatureKey,
        input_schema: FeatureInputSchema,
        parameters: FeatureParameters,
        time_semantics: FeatureTimeSemantics,
        warm_up: FeatureWarmUp,
        null_policy: FeatureNullPolicy,
        output_type: FeatureOutputType,
        unit: FeatureUnit,
        live_compatible: bool,
        point_in_time_compatible: bool,
        implementation_revision: &str,
        implementation_digest: FeatureImplementationDigest,
        code_owned: bool,
    ) -> Result<Self, FeatureMetadataError> {
        validate_revision(implementation_revision)?;
        if !output_unit_is_compatible(output_type, unit) {
            return Err(FeatureMetadataError::IncompatibleOutputUnit);
        }
        if !live_compatible && !point_in_time_compatible {
            return Err(FeatureMetadataError::NoCompatibleExecutionPlane);
        }
        let input_schema_digest = input_schema.digest();
        let semantic_digest = digest::semantic_digest(
            &key,
            &input_schema,
            &parameters,
            time_semantics,
            warm_up,
            null_policy,
            output_type,
            unit,
            live_compatible,
            point_in_time_compatible,
            implementation_revision,
            implementation_digest,
        );
        Ok(Self {
            key,
            input_schema,
            input_schema_digest,
            parameters,
            time_semantics,
            warm_up,
            null_policy,
            output_type,
            unit,
            live_compatible,
            point_in_time_compatible,
            implementation_revision: implementation_revision.to_owned(),
            implementation_digest,
            semantic_digest,
            code_owned,
        })
    }

    /// Returns the stable `(name, version)` key.
    #[must_use]
    pub const fn key(&self) -> &FeatureKey {
        &self.key
    }

    /// Returns the exact ordered input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &FeatureInputSchema {
        &self.input_schema
    }

    /// Returns the exact ordered input-schema digest bound to this metadata.
    #[must_use]
    pub const fn input_schema_digest(&self) -> FeatureInputSchemaDigest {
        self.input_schema_digest
    }

    /// Returns the exact ordered parameter set.
    #[must_use]
    pub const fn parameters(&self) -> &FeatureParameters {
        &self.parameters
    }

    /// Returns the event-time interpretation.
    #[must_use]
    pub const fn time_semantics(&self) -> FeatureTimeSemantics {
        self.time_semantics
    }

    /// Returns the warm-up requirement.
    #[must_use]
    pub const fn warm_up(&self) -> FeatureWarmUp {
        self.warm_up
    }

    /// Returns the missing-input behavior.
    #[must_use]
    pub const fn null_policy(&self) -> FeatureNullPolicy {
        self.null_policy
    }

    /// Returns the closed scalar representation.
    #[must_use]
    pub const fn output_type(&self) -> FeatureOutputType {
        self.output_type
    }

    /// Returns the output unit.
    #[must_use]
    pub const fn unit(&self) -> FeatureUnit {
        self.unit
    }

    /// Returns whether the implementation is safe for the bounded live plane.
    #[must_use]
    pub const fn is_live_compatible(&self) -> bool {
        self.live_compatible
    }

    /// Returns whether the implementation preserves point-in-time semantics.
    #[must_use]
    pub const fn is_point_in_time_compatible(&self) -> bool {
        self.point_in_time_compatible
    }

    /// Returns the bounded implementation revision.
    #[must_use]
    pub fn implementation_revision(&self) -> &str {
        &self.implementation_revision
    }

    /// Returns the code identity requested by this metadata.
    #[must_use]
    pub const fn implementation_digest(&self) -> FeatureImplementationDigest {
        self.implementation_digest
    }

    /// Returns the digest of all execution-relevant metadata semantics.
    #[must_use]
    pub const fn semantic_digest(&self) -> FeatureSemanticDigest {
        self.semantic_digest
    }

    pub(crate) const fn is_code_owned(&self) -> bool {
        self.code_owned
    }

    /// Returns the complete exact retained footprint of this owned metadata graph.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureMetadataError::RetainedSizeOverflow`] on checked arithmetic overflow.
    pub fn retained_bytes(&self) -> Result<usize, FeatureMetadataError> {
        size_of::<Self>()
            .checked_add(
                self.checked_dynamic_retained_bytes()
                    .ok_or(FeatureMetadataError::RetainedSizeOverflow)?,
            )
            .ok_or(FeatureMetadataError::RetainedSizeOverflow)
    }

    pub(crate) fn checked_dynamic_retained_bytes(&self) -> Option<usize> {
        self.key
            .dynamic_retained_bytes()
            .checked_add(self.input_schema.checked_dynamic_retained_bytes()?)?
            .checked_add(self.parameters.checked_dynamic_retained_bytes()?)?
            .checked_add(self.implementation_revision.capacity())
    }
}

/// Immutable feature metadata validation or retained-accounting failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FeatureMetadataError {
    /// The feature name was empty, oversized, or non-canonical.
    #[error("invalid canonical feature name")]
    InvalidFeatureName,
    /// An input or parameter field name was empty, oversized, or non-canonical.
    #[error("invalid canonical feature field name")]
    InvalidFieldName,
    /// The input schema was empty or exceeded its count bound.
    #[error("feature input schema count is outside its bounds")]
    InvalidInputCount,
    /// The input schema repeated a field name.
    #[error("feature input schema contains a duplicate name")]
    DuplicateInputName,
    /// The parameter set exceeded its count bound.
    #[error("feature parameter count exceeds its bound")]
    InvalidParameterCount,
    /// The parameter set repeated a name.
    #[error("feature parameters contain a duplicate name")]
    DuplicateParameterName,
    /// An input type and unit contradicted each other.
    #[error("feature input type and unit are incompatible")]
    IncompatibleInputUnit,
    /// An output type and unit contradicted each other.
    #[error("feature output type and unit are incompatible")]
    IncompatibleOutputUnit,
    /// The implementation revision was empty, oversized, or contained unsafe characters.
    #[error("invalid feature implementation revision")]
    InvalidImplementationRevision,
    /// The implementation digest used the reserved all-zero sentinel.
    #[error("feature implementation digest must be a nonzero SHA-256 value")]
    ZeroImplementationDigest,
    /// The metadata was compatible with neither the live nor point-in-time plane.
    #[error("feature metadata must be compatible with at least one execution plane")]
    NoCompatibleExecutionPlane,
    /// Checked retained-size arithmetic overflowed.
    #[error("feature metadata retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    /// A code-owned batch catalog policy is outside its closed bounds.
    #[error("batch feature catalog policy is invalid")]
    InvalidBatchCatalogPolicy,
}

fn validate_identifier(value: &str, maximum_bytes: usize) -> Result<(), ()> {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(());
    };
    if value.len() > maximum_bytes || !first.is_ascii_lowercase() {
        return Err(());
    }
    if bytes.all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    }) {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_revision(value: &str) -> Result<(), FeatureMetadataError> {
    if value.is_empty()
        || value.len() > MAX_IMPLEMENTATION_REVISION_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'\\' | b'\"' | b'\''))
    {
        Err(FeatureMetadataError::InvalidImplementationRevision)
    } else {
        Ok(())
    }
}

fn has_duplicate_names<'a>(names: impl Iterator<Item = &'a str> + Clone) -> bool {
    names
        .clone()
        .enumerate()
        .any(|(index, name)| names.clone().skip(index + 1).any(|other| name == other))
}

const fn input_unit_is_compatible(data_type: FeatureDataType, unit: FeatureUnit) -> bool {
    match data_type {
        FeatureDataType::PriceTicks => matches!(unit, FeatureUnit::PriceTicks),
        FeatureDataType::QuantityLots => matches!(unit, FeatureUnit::QuantityLots),
        FeatureDataType::BasisPoints => matches!(unit, FeatureUnit::BasisPoints),
        FeatureDataType::Timestamp => matches!(unit, FeatureUnit::Nanoseconds),
        FeatureDataType::AggressorSide
        | FeatureDataType::OrderSide
        | FeatureDataType::InstrumentId
        | FeatureDataType::VenueId
        | FeatureDataType::Boolean => matches!(unit, FeatureUnit::Unitless),
        FeatureDataType::SignedInteger | FeatureDataType::UnsignedInteger => {
            matches!(unit, FeatureUnit::Count | FeatureUnit::Nanoseconds)
        }
        FeatureDataType::ExactRatio => matches!(
            unit,
            FeatureUnit::PriceTicks
                | FeatureUnit::BasisPoints
                | FeatureUnit::Ratio
                | FeatureUnit::Return
                | FeatureUnit::Volatility
                | FeatureUnit::LotsPerSecond
                | FeatureUnit::Rate
                | FeatureUnit::Unitless
        ),
        FeatureDataType::StatisticalF64 => !matches!(
            unit,
            FeatureUnit::PriceTicks | FeatureUnit::QuantityLots | FeatureUnit::CurrencyAmount
        ),
        FeatureDataType::Decimal => matches!(
            unit,
            FeatureUnit::BasisPoints
                | FeatureUnit::Ratio
                | FeatureUnit::Return
                | FeatureUnit::Volatility
                | FeatureUnit::Rate
                | FeatureUnit::Unitless
        ),
        FeatureDataType::Money => matches!(unit, FeatureUnit::CurrencyAmount),
    }
}

const fn output_unit_is_compatible(output_type: FeatureOutputType, unit: FeatureUnit) -> bool {
    match output_type {
        FeatureOutputType::PriceTicks => matches!(unit, FeatureUnit::PriceTicks),
        FeatureOutputType::HalfTickPrice => matches!(unit, FeatureUnit::PriceTicks),
        FeatureOutputType::QuantityLots => matches!(unit, FeatureUnit::QuantityLots),
        FeatureOutputType::BasisPoints => matches!(unit, FeatureUnit::BasisPoints),
        FeatureOutputType::SignedInteger => matches!(
            unit,
            FeatureUnit::QuantityLots | FeatureUnit::Count | FeatureUnit::Nanoseconds
        ),
        FeatureOutputType::UnsignedInteger => {
            matches!(unit, FeatureUnit::Count | FeatureUnit::Nanoseconds)
        }
        FeatureOutputType::ExactRatio => matches!(
            unit,
            FeatureUnit::PriceTicks
                | FeatureUnit::BasisPoints
                | FeatureUnit::Ratio
                | FeatureUnit::Return
                | FeatureUnit::Volatility
                | FeatureUnit::LotsPerSecond
                | FeatureUnit::Rate
                | FeatureUnit::Unitless
        ),
        FeatureOutputType::StatisticalF64 => !matches!(
            unit,
            FeatureUnit::PriceTicks | FeatureUnit::QuantityLots | FeatureUnit::CurrencyAmount
        ),
        FeatureOutputType::Decimal => matches!(
            unit,
            FeatureUnit::BasisPoints
                | FeatureUnit::Ratio
                | FeatureUnit::Return
                | FeatureUnit::Volatility
                | FeatureUnit::Rate
                | FeatureUnit::Unitless
        ),
        FeatureOutputType::Money => matches!(unit, FeatureUnit::CurrencyAmount),
    }
}

fn allocation_bytes<T>(count: usize) -> Option<usize> {
    size_of::<T>().checked_mul(count)
}
