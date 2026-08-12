//! Validated dataset-build requests and immutable result receipts.

use std::cmp::Ordering;
use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use market_squawk_domain::{
    AvailabilityEvidence, Currency, DigestAlgorithm, EvidenceDigest, InstrumentId,
    ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;

use super::DatasetBuildError;
use crate::{
    CorporateActionLimits, CorporateActionPolicy, DatasetBuildSpecDigest, DatasetId,
    DatasetManifestRef, DerivedGenerationParents, ObservationFamilyKey, PinnedDataset,
    PointInTimeLimits, PointInTimePolicy, PointInTimeRevisionMode, ResearchUse, ResearchUseLimits,
    RightsBasis, RightsDecisionInput, Sha256Digest, SourceOperation, UniverseId, UniverseLimits,
    UniverseMembership,
};

const MAX_COMPONENT_SELECTORS: usize = 64;
const MAX_COMPONENT_NAME_BYTES: usize = 256;
const MAX_EXAMPLE_ID_BYTES: usize = 256;
const MAX_BUILD_DURATION: Duration = Duration::from_secs(300);
const MAX_BUILD_INPUT_ROWS: usize = 1_000_000;
const MAX_BUILD_EXAMPLES: usize = 1_000_000;
const MAX_COMPONENTS_PER_EXAMPLE: usize = 1_024;
const MAX_BUILD_OUTPUT_ROWS: usize = 10_000_000;
const MAX_BUILD_RETAINED_BYTES: usize = 1024 * 1024 * 1024;

/// Closed semantic role of one versioned output component.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ComponentKind {
    /// Value available at or before the example cutoff.
    Feature,
    /// Value admitted only from the open-closed interval after the feature cutoff.
    Label,
}

/// Closed ownership scope for one component's selected source evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ComponentScope {
    /// Evidence must include this example's instrument and may include global context.
    Instrument,
    /// Evidence must include an account-scoped record and may include global context.
    Account,
    /// Evidence must be independent of instrument and account identity.
    Global,
}

/// Whether corporate-action economics can change a component's meaning or value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CorporateActionSensitivity {
    /// The component is independent of instrument lifecycle and distribution economics.
    NotApplicable,
    /// Non-raw policies require exact producer evidence for the selected action plan.
    RequiresAdjustment,
}

/// Code-owned unit tag for a dimensionless return label.
pub const FEATURE_LABEL_RETURN_UNIT: &str = "market-squawk.return";
/// Code-owned unit tag for a probability label.
pub const FEATURE_LABEL_PROBABILITY_UNIT: &str = "market-squawk.probability";

/// Closed measurement derived from the admitted rows of one numeric label.
///
/// This contract is produced from the row-level unit and currency columns. It is not inferred from
/// a label name and cannot be supplied by a model-training caller.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FeatureLabelMeasurement {
    /// A monetary price in one exact quote currency.
    Price { currency: Currency },
    /// A dimensionless return using [`FEATURE_LABEL_RETURN_UNIT`].
    Return,
    /// A probability using [`FEATURE_LABEL_PROBABILITY_UNIT`].
    Probability,
    /// A numeric regression measurement that is explicitly none of the above.
    OtherRegression,
}

impl FeatureLabelMeasurement {
    pub(crate) fn try_from_parts(
        unit: Option<&str>,
        currency: Option<&str>,
    ) -> Result<Self, DatasetBuildError> {
        let currency = currency
            .map(Currency::try_from)
            .transpose()
            .map_err(|_| DatasetBuildError::InvalidRequest)?;
        match (unit, currency) {
            (None, Some(currency)) => Ok(Self::Price { currency }),
            (Some(FEATURE_LABEL_RETURN_UNIT), None) => Ok(Self::Return),
            (Some(FEATURE_LABEL_PROBABILITY_UNIT), None) => Ok(Self::Probability),
            (None | Some(_), None) => Ok(Self::OtherRegression),
            (Some(_), Some(_)) => Err(DatasetBuildError::InvalidRequest),
        }
    }

    pub(super) fn try_from_value(
        value: &ComponentValue,
    ) -> Result<Option<Self>, DatasetBuildError> {
        match value {
            ComponentValue::Float {
                value,
                unit,
                currency,
            } => {
                let measurement = Self::try_from_parts(
                    unit.as_ref().map(SourceIdentifier::as_str),
                    currency.as_ref().map(Currency::as_str),
                )?;
                if matches!(measurement, Self::Price { .. }) && *value <= 0.0 {
                    return Err(DatasetBuildError::InvalidRequest);
                }
                Ok(Some(measurement))
            }
            ComponentValue::Decimal {
                value,
                unit,
                currency,
            } => {
                let measurement = Self::try_from_parts(
                    unit.as_ref().map(SourceIdentifier::as_str),
                    currency.as_ref().map(Currency::as_str),
                )?;
                if matches!(measurement, Self::Price { .. }) && *value <= Decimal::ZERO {
                    return Err(DatasetBuildError::InvalidRequest);
                }
                Ok(Some(measurement))
            }
            ComponentValue::Missing { .. } => Ok(None),
        }
    }
}

/// One exact label contract and the measurement consistently derived from all retained rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureLabelMeasurementBinding {
    label: FeatureLabelComponentSpec,
    measurement: FeatureLabelMeasurement,
    fixed_horizon_nanos: Option<NonZeroU64>,
}

impl FeatureLabelMeasurementBinding {
    pub(crate) fn try_new(
        label: FeatureLabelComponentSpec,
        measurement: FeatureLabelMeasurement,
        fixed_horizon_nanos: Option<NonZeroU64>,
    ) -> Result<Self, DatasetBuildError> {
        if label.kind() != ComponentKind::Label {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            label,
            measurement,
            fixed_horizon_nanos,
        })
    }

    /// Returns the exact label component contract.
    #[must_use]
    pub const fn label(&self) -> &FeatureLabelComponentSpec {
        &self.label
    }

    /// Returns the closed row-derived measurement.
    #[must_use]
    pub const fn measurement(&self) -> FeatureLabelMeasurement {
        self.measurement
    }

    /// Returns the exact label-effective offset from the feature-effective coordinate only when
    /// every retained numeric label row proved the same positive nanosecond horizon.
    #[must_use]
    pub const fn fixed_horizon_nanos(&self) -> Option<NonZeroU64> {
        self.fixed_horizon_nanos
    }
}

impl CorporateActionSensitivity {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::NotApplicable => 1,
            Self::RequiresAdjustment => 2,
        }
    }
}

impl ComponentScope {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::Instrument => 1,
            Self::Account => 2,
            Self::Global => 3,
        }
    }
}

impl ComponentKind {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::Feature => 1,
            Self::Label => 2,
        }
    }
}

/// One stable, versioned feature or label contract supplied by Task 12 or another producer.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FeatureLabelComponentSpec {
    kind: ComponentKind,
    scope: ComponentScope,
    corporate_actions: CorporateActionSensitivity,
    name: Box<str>,
    version: NonZeroU32,
}

impl FeatureLabelComponentSpec {
    /// Constructs a component contract using the stable output-identifier grammar.
    pub fn try_new(
        kind: ComponentKind,
        scope: ComponentScope,
        corporate_actions: CorporateActionSensitivity,
        name: impl AsRef<str>,
        version: NonZeroU32,
    ) -> Result<Self, DatasetBuildError> {
        let name = name.as_ref();
        if !canonical_identifier(name, MAX_COMPONENT_NAME_BYTES) {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            kind,
            scope,
            corporate_actions,
            name: name.into(),
            version,
        })
    }

    /// Returns whether the component is a feature or label.
    pub const fn kind(&self) -> ComponentKind {
        self.kind
    }

    /// Returns the closed source-evidence ownership scope.
    pub const fn scope(&self) -> ComponentScope {
        self.scope
    }

    /// Returns whether this component requires corporate-action treatment evidence.
    pub const fn corporate_actions(&self) -> CorporateActionSensitivity {
        self.corporate_actions
    }

    /// Returns the stable component name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the nonzero semantic version.
    pub const fn version(&self) -> NonZeroU32 {
        self.version
    }
}

/// Exact producer attestation for the corporate-action treatment of one component value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentAdjustmentEvidence {
    /// The producer retained the value in raw terms.
    Raw,
    /// The component contract declares corporate-action economics inapplicable.
    NotApplicable,
    /// The producer transformed the value using the exact selected plan and implementation.
    Applied {
        /// Non-raw policy implemented by the producer.
        policy: CorporateActionPolicy,
        /// Canonical identity of admitted actions and typed adjustment steps.
        plan_content: Sha256Digest,
        /// Canonical identity of the complete plan admission/exclusion audit.
        plan_audit: Sha256Digest,
        /// Immutable identity of the producer implementation that performed the transformation.
        implementation_evidence: EvidenceDigest,
    },
}

impl ComponentAdjustmentEvidence {
    /// Constructs non-reserved evidence for a producer-applied non-raw action plan.
    pub fn try_applied(
        policy: CorporateActionPolicy,
        plan_content: Sha256Digest,
        plan_audit: Sha256Digest,
        implementation_evidence: EvidenceDigest,
    ) -> Result<Self, DatasetBuildError> {
        if policy.adjustment() == crate::CorporateActionAdjustment::Raw
            || plan_content.bytes() == [0; 32]
            || plan_audit.bytes() == [0; 32]
            || implementation_evidence.bytes() == [0; 32]
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self::Applied {
            policy,
            plan_content,
            plan_audit,
            implementation_evidence,
        })
    }
}

/// One requested natural observation family used to prove component lineage or absence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentSelector {
    family: ObservationFamilyKey,
    identity: Sha256Digest,
}

impl ComponentSelector {
    /// Binds a selector to its versioned canonical family identity.
    pub fn new(family: ObservationFamilyKey) -> Self {
        let identity = super::canonical::family_key_digest(&family);
        Self { family, identity }
    }

    /// Returns the exact natural-family key.
    pub const fn family(&self) -> &ObservationFamilyKey {
        &self.family
    }

    /// Returns the selector's canonical SHA-256 identity.
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }
}

/// Explicit typed component value at the analytics-to-dataset boundary.
#[derive(Clone, Debug)]
pub enum ComponentValue {
    /// Statistical floating-point output with an explicit conversion boundary.
    Float {
        value: f64,
        unit: Option<SourceIdentifier>,
        currency: Option<Currency>,
    },
    /// Exact decimal output preserving mantissa, scale, unit, and optional currency.
    Decimal {
        value: Decimal,
        unit: Option<SourceIdentifier>,
        currency: Option<Currency>,
    },
    /// Explicit missing value whose requested selector families must all be absent.
    Missing { reason: SourceIdentifier },
}

impl ComponentValue {
    /// Constructs a finite statistical value.
    pub fn float(
        value: f64,
        unit: Option<SourceIdentifier>,
        currency: Option<Currency>,
    ) -> Result<Self, DatasetBuildError> {
        if !value.is_finite() || !unit.as_ref().is_none_or(valid_unit) {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self::Float {
            value,
            unit,
            currency,
        })
    }

    /// Constructs an exact analytical decimal with an Arrow-compatible scale.
    pub fn decimal(
        value: Decimal,
        unit: Option<SourceIdentifier>,
        currency: Option<Currency>,
    ) -> Result<Self, DatasetBuildError> {
        if value.scale() > 28 || !unit.as_ref().is_none_or(valid_unit) {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self::Decimal {
            value,
            unit,
            currency,
        })
    }

    /// Constructs an explicit missing marker.
    pub const fn missing(reason: SourceIdentifier) -> Self {
        Self::Missing { reason }
    }

    pub(super) const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing { .. })
    }
}

impl PartialEq for ComponentValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Float {
                    value: left,
                    unit: left_unit,
                    currency: left_currency,
                },
                Self::Float {
                    value: right,
                    unit: right_unit,
                    currency: right_currency,
                },
            ) => {
                left.to_bits() == right.to_bits()
                    && left_unit == right_unit
                    && left_currency == right_currency
            }
            (
                Self::Decimal {
                    value: left,
                    unit: left_unit,
                    currency: left_currency,
                },
                Self::Decimal {
                    value: right,
                    unit: right_unit,
                    currency: right_currency,
                },
            ) => left == right && left_unit == right_unit && left_currency == right_currency,
            (Self::Missing { reason: left }, Self::Missing { reason: right }) => left == right,
            _ => false,
        }
    }
}

impl Eq for ComponentValue {}

/// One component value and the natural families that must prove its lineage or absence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureLabelComponentInput {
    spec: FeatureLabelComponentSpec,
    value: ComponentValue,
    selectors: Box<[ComponentSelector]>,
    adjustment: ComponentAdjustmentEvidence,
}

impl FeatureLabelComponentInput {
    /// Constructs one selector-bound component value.
    pub fn try_new(
        spec: FeatureLabelComponentSpec,
        value: ComponentValue,
        mut selectors: Vec<ComponentSelector>,
        adjustment: ComponentAdjustmentEvidence,
    ) -> Result<Self, DatasetBuildError> {
        if selectors.is_empty() || selectors.len() > MAX_COMPONENT_SELECTORS {
            return Err(DatasetBuildError::InvalidRequest);
        }
        selectors.sort_unstable_by_key(ComponentSelector::identity);
        if selectors
            .windows(2)
            .any(|pair| pair[0].identity == pair[1].identity)
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            spec,
            value,
            selectors: selectors.into_boxed_slice(),
            adjustment,
        })
    }

    /// Returns the exact component contract.
    pub const fn spec(&self) -> &FeatureLabelComponentSpec {
        &self.spec
    }

    /// Returns the typed value or explicit missing marker.
    pub const fn value(&self) -> &ComponentValue {
        &self.value
    }

    /// Returns canonical, duplicate-free natural-family selectors.
    pub fn selectors(&self) -> &[ComponentSelector] {
        &self.selectors
    }

    /// Returns exact producer evidence for this value's corporate-action treatment.
    pub const fn adjustment(&self) -> &ComponentAdjustmentEvidence {
        &self.adjustment
    }
}

/// One immutable example cutoff and its complete feature/label component set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetExample {
    example_id: Box<str>,
    instrument_id: InstrumentId,
    /// Exact knowledge-time boundary used for availability and split admission.
    cutoff_at: Timestamp,
    /// Exact knowledge-time boundary used for label availability and split admission.
    label_cutoff_at: Timestamp,
    /// Source-precision feature/effective boundary; never inferred from knowledge time.
    effective_cutoff: ResearchTemporalCoordinate,
    /// Source-precision label/effective boundary; never inferred from knowledge time.
    label_effective_cutoff: ResearchTemporalCoordinate,
    components: Box<[FeatureLabelComponentInput]>,
}

impl DatasetExample {
    /// Constructs one leakage-bounded example.
    pub fn try_new(
        example_id: impl AsRef<str>,
        instrument_id: InstrumentId,
        cutoff_at: Timestamp,
        label_cutoff_at: Timestamp,
        components: Vec<FeatureLabelComponentInput>,
    ) -> Result<Self, DatasetBuildError> {
        Self::try_new_with_temporal_cutoffs(
            example_id,
            instrument_id,
            cutoff_at,
            label_cutoff_at,
            ResearchTemporalCoordinate::exact(cutoff_at),
            ResearchTemporalCoordinate::exact(label_cutoff_at),
            components,
        )
    }

    /// Constructs one leakage-bounded example while preserving source temporal precision.
    #[allow(
        clippy::too_many_arguments,
        reason = "knowledge-time and source-effective boundaries are independent semantics"
    )]
    pub fn try_new_with_temporal_cutoffs(
        example_id: impl AsRef<str>,
        instrument_id: InstrumentId,
        cutoff_at: Timestamp,
        label_cutoff_at: Timestamp,
        effective_cutoff: ResearchTemporalCoordinate,
        label_effective_cutoff: ResearchTemporalCoordinate,
        mut components: Vec<FeatureLabelComponentInput>,
    ) -> Result<Self, DatasetBuildError> {
        let example_id = example_id.as_ref();
        if !canonical_identifier(example_id, MAX_EXAMPLE_ID_BYTES)
            || label_cutoff_at <= cutoff_at
            || !matches!(
                label_effective_cutoff.partial_cmp(&effective_cutoff),
                Some(Ordering::Greater)
            )
            || components.is_empty()
            || components.len() > MAX_COMPONENTS_PER_EXAMPLE
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        components.sort_unstable_by(|left, right| left.spec.cmp(&right.spec));
        if components
            .windows(2)
            .any(|pair| pair[0].spec == pair[1].spec)
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            example_id: example_id.into(),
            instrument_id,
            cutoff_at,
            label_cutoff_at,
            effective_cutoff,
            label_effective_cutoff,
            components: components.into_boxed_slice(),
        })
    }

    /// Returns the stable example identity.
    pub fn example_id(&self) -> &str {
        &self.example_id
    }

    /// Returns the stable instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact feature knowledge cutoff used for availability and split admission.
    pub const fn cutoff_at(&self) -> Timestamp {
        self.cutoff_at
    }

    /// Returns the exact label knowledge cutoff used for availability and split admission.
    pub const fn label_cutoff_at(&self) -> Timestamp {
        self.label_cutoff_at
    }

    /// Returns the feature-effective boundary with its source precision intact.
    pub const fn effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.effective_cutoff
    }

    /// Returns the inclusive label-effective boundary with its source precision intact.
    pub const fn label_effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.label_effective_cutoff
    }

    /// Returns components in canonical contract order.
    pub fn components(&self) -> &[FeatureLabelComponentInput] {
        &self.components
    }
}

/// Closed chronological output split.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DatasetSplit {
    Train,
    Validation,
    Test,
}

impl DatasetSplit {
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Train => "train",
            Self::Validation => "validation",
            Self::Test => "test",
        }
    }
}

/// Inclusive chronological split ends; later splits begin one instant after the prior end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChronologicalSplitPolicy {
    train_end: Timestamp,
    validation_end: Timestamp,
    test_end: Timestamp,
}

impl ChronologicalSplitPolicy {
    /// Constructs strictly increasing train, validation, and test boundaries.
    pub fn try_new(
        train_end: Timestamp,
        validation_end: Timestamp,
        test_end: Timestamp,
    ) -> Result<Self, DatasetBuildError> {
        if !(train_end < validation_end && validation_end < test_end) {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            train_end,
            validation_end,
            test_end,
        })
    }

    pub(super) fn split_for(self, cutoff: Timestamp) -> Option<DatasetSplit> {
        if cutoff <= self.train_end {
            Some(DatasetSplit::Train)
        } else if cutoff <= self.validation_end {
            Some(DatasetSplit::Validation)
        } else if cutoff <= self.test_end {
            Some(DatasetSplit::Test)
        } else {
            None
        }
    }

    pub(super) const fn split_end(self, split: DatasetSplit) -> Timestamp {
        match split {
            DatasetSplit::Train => self.train_end,
            DatasetSplit::Validation => self.validation_end,
            DatasetSplit::Test => self.test_end,
        }
    }

    pub(super) const fn boundaries(self) -> [Timestamp; 3] {
        [self.train_end, self.validation_end, self.test_end]
    }
}

/// Explicit handling for components whose requested point-in-time families are absent.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MissingValuePolicy {
    /// Reject the complete build.
    Reject,
    /// Publish the caller-supplied missing marker with absence lineage.
    Preserve,
    /// Exclude the entire example when any component is missing.
    DropExample,
}

impl MissingValuePolicy {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::Reject => 1,
            Self::Preserve => 2,
            Self::DropExample => 3,
        }
    }
}

/// Versioned temporal, split, missing-value, and corporate-action semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetBuildPolicy {
    split: ChronologicalSplitPolicy,
    point_in_time: PointInTimePolicy,
    corporate_actions: CorporateActionPolicy,
    missing_values: MissingValuePolicy,
    implementation_revision: SourceIdentifier,
}

impl DatasetBuildPolicy {
    /// Binds every semantic policy and the exact implementation revision.
    pub const fn new(
        split: ChronologicalSplitPolicy,
        point_in_time: PointInTimePolicy,
        corporate_actions: CorporateActionPolicy,
        missing_values: MissingValuePolicy,
        implementation_revision: SourceIdentifier,
    ) -> Self {
        Self {
            split,
            point_in_time,
            corporate_actions,
            missing_values,
            implementation_revision,
        }
    }

    pub(super) const fn split(&self) -> ChronologicalSplitPolicy {
        self.split
    }

    pub(super) const fn point_in_time(&self) -> PointInTimePolicy {
        self.point_in_time
    }

    pub(super) const fn corporate_actions(&self) -> CorporateActionPolicy {
        self.corporate_actions
    }

    pub(super) const fn missing_values(&self) -> MissingValuePolicy {
        self.missing_values
    }

    pub(super) const fn implementation_revision(&self) -> &SourceIdentifier {
        &self.implementation_revision
    }
}

/// Exact immutable input generations, historical universe evidence, and requested examples.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetBuildInputs {
    parents: DerivedGenerationParents,
    universe_id: UniverseId,
    universe_memberships: Box<[UniverseMembership]>,
    component_specs: Box<[FeatureLabelComponentSpec]>,
    examples: Box<[DatasetExample]>,
}

impl DatasetBuildInputs {
    /// Canonicalizes all exact parents, component contracts, and examples.
    pub fn try_new(
        parents: Vec<DatasetManifestRef>,
        universe_id: UniverseId,
        universe_memberships: Vec<UniverseMembership>,
        mut component_specs: Vec<FeatureLabelComponentSpec>,
        mut examples: Vec<DatasetExample>,
    ) -> Result<Self, DatasetBuildError> {
        let parents = DerivedGenerationParents::try_new(parents)?;
        if universe_memberships.is_empty() || component_specs.is_empty() || examples.is_empty() {
            return Err(DatasetBuildError::InvalidRequest);
        }
        component_specs.sort_unstable();
        if component_specs.windows(2).any(|pair| pair[0] == pair[1])
            || !component_specs
                .iter()
                .any(|spec| spec.kind == ComponentKind::Feature)
            || !component_specs
                .iter()
                .any(|spec| spec.kind == ComponentKind::Label)
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        for membership in &universe_memberships {
            if !parents.as_slice().contains(membership.source_manifest()) {
                return Err(DatasetBuildError::InvalidRequest);
            }
        }
        examples.sort_unstable_by(compare_examples);
        if examples
            .windows(2)
            .any(|pair| pair[0].example_id == pair[1].example_id)
            || examples.iter().any(|example| {
                example.components.len() != component_specs.len()
                    || example
                        .components
                        .iter()
                        .zip(&component_specs)
                        .any(|(component, spec)| component.spec != *spec)
                    || example
                        .components
                        .iter()
                        .any(|component| !component_scope_matches(component, example.instrument_id))
            })
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            parents,
            universe_id,
            universe_memberships: universe_memberships.into_boxed_slice(),
            component_specs: component_specs.into_boxed_slice(),
            examples: examples.into_boxed_slice(),
        })
    }

    /// Returns exact immutable input generations in canonical order.
    pub fn parents(&self) -> &[DatasetManifestRef] {
        self.parents.as_slice()
    }

    /// Returns the stable historical-universe identity.
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }

    /// Returns historical membership evidence bound to exact parents.
    pub fn universe_memberships(&self) -> &[UniverseMembership] {
        &self.universe_memberships
    }

    /// Returns the complete canonical component contract.
    pub fn component_specs(&self) -> &[FeatureLabelComponentSpec] {
        &self.component_specs
    }

    /// Returns examples in deterministic chronological order.
    pub fn examples(&self) -> &[DatasetExample] {
        &self.examples
    }
}

/// Caller-selected work, time, and retained-memory limits capped by process ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatasetBuildLimits {
    max_input_rows: usize,
    max_examples: usize,
    max_components_per_example: usize,
    max_output_rows: usize,
    max_retained_bytes: usize,
    max_duration: Duration,
    point_in_time: PointInTimeLimits,
    universe: UniverseLimits,
    corporate_actions: CorporateActionLimits,
}

impl DatasetBuildLimits {
    /// Constructs bounded limits for every build phase.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent work and memory ceilings remain explicit"
    )]
    pub fn try_new(
        max_input_rows: usize,
        max_examples: usize,
        max_components_per_example: usize,
        max_output_rows: usize,
        max_retained_bytes: usize,
        max_duration: Duration,
        point_in_time: PointInTimeLimits,
        universe: UniverseLimits,
        corporate_actions: CorporateActionLimits,
    ) -> Result<Self, DatasetBuildError> {
        if max_input_rows == 0
            || max_input_rows > MAX_BUILD_INPUT_ROWS
            || max_examples == 0
            || max_examples > MAX_BUILD_EXAMPLES
            || max_components_per_example == 0
            || max_components_per_example > MAX_COMPONENTS_PER_EXAMPLE
            || max_output_rows == 0
            || max_output_rows > MAX_BUILD_OUTPUT_ROWS
            || max_retained_bytes == 0
            || max_retained_bytes > MAX_BUILD_RETAINED_BYTES
            || max_duration.is_zero()
            || max_duration > MAX_BUILD_DURATION
        {
            return Err(DatasetBuildError::InvalidLimits);
        }
        Ok(Self {
            max_input_rows,
            max_examples,
            max_components_per_example,
            max_output_rows,
            max_retained_bytes,
            max_duration,
            point_in_time,
            universe,
            corporate_actions,
        })
    }

    pub(super) const fn max_input_rows(self) -> usize {
        self.max_input_rows
    }

    pub(super) const fn max_examples(self) -> usize {
        self.max_examples
    }

    pub(super) const fn max_components_per_example(self) -> usize {
        self.max_components_per_example
    }

    pub(super) const fn max_output_rows(self) -> usize {
        self.max_output_rows
    }

    pub(super) const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }

    pub(super) const fn max_duration(self) -> Duration {
        self.max_duration
    }

    pub(super) const fn point_in_time(self) -> PointInTimeLimits {
        self.point_in_time
    }

    pub(super) const fn universe(self) -> UniverseLimits {
        self.universe
    }

    pub(super) const fn corporate_actions(self) -> CorporateActionLimits {
        self.corporate_actions
    }
}

/// Explicit evidence used to admit the produced content under output-persistence rights.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetOutputAuthorization {
    source_id: SourceId,
    basis: RightsBasis,
    authorization_evidence: EvidenceDigest,
    authorization_expires_at: Option<Timestamp>,
}

impl DatasetOutputAuthorization {
    /// Constructs non-reserved output authorization evidence.
    pub fn try_new(
        source_id: SourceId,
        basis: RightsBasis,
        authorization_evidence: EvidenceDigest,
        authorization_expires_at: Option<Timestamp>,
    ) -> Result<Self, DatasetBuildError> {
        if authorization_evidence.bytes() == [0; 32] || basis.digest().bytes() == [0; 32] {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            source_id,
            basis,
            authorization_evidence,
            authorization_expires_at,
        })
    }

    pub(super) fn rights_decision(
        &self,
        content_hash: Sha256Digest,
        created_at: Timestamp,
    ) -> RightsDecisionInput {
        RightsDecisionInput {
            source_id: self.source_id.clone(),
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, content_hash.bytes()),
            retrieved_at: created_at,
            basis: self.basis.clone(),
            authorization_evidence: self.authorization_evidence,
            authorization_expires_at: self.authorization_expires_at,
            permitted_operations: vec![SourceOperation::Persist],
        }
    }

    pub(super) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(super) const fn basis(&self) -> &RightsBasis {
        &self.basis
    }

    pub(super) const fn authorization_evidence(&self) -> EvidenceDigest {
        self.authorization_evidence
    }

    pub(super) const fn authorization_expires_at(&self) -> Option<Timestamp> {
        self.authorization_expires_at
    }
}

/// Complete immutable and authority-bound request for one derived generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetBuildRequest {
    output_dataset: DatasetId,
    inputs: DatasetBuildInputs,
    policy: DatasetBuildPolicy,
    intended_use: ResearchUse,
    research_use_limits: ResearchUseLimits,
    output_authorization: DatasetOutputAuthorization,
    limits: DatasetBuildLimits,
    build_spec_digest: DatasetBuildSpecDigest,
    policy_digest: Sha256Digest,
    universe_digest: Sha256Digest,
    retained_bytes: usize,
}

impl DatasetBuildRequest {
    /// Validates all cross-field temporal, authority, and resource invariants.
    #[allow(
        clippy::too_many_arguments,
        reason = "semantic inputs, authority, and resource policy stay independently typed"
    )]
    pub fn try_new(
        output_dataset: DatasetId,
        inputs: DatasetBuildInputs,
        policy: DatasetBuildPolicy,
        intended_use: ResearchUse,
        research_use_limits: ResearchUseLimits,
        output_authorization: DatasetOutputAuthorization,
        limits: DatasetBuildLimits,
    ) -> Result<Self, DatasetBuildError> {
        if intended_use == ResearchUse::Display
            || policy.point_in_time.revision_mode() != PointInTimeRevisionMode::LatestKnown
            || limits.max_duration >= research_use_limits.permit_lifetime()
            || inputs.parents().len() > research_use_limits.max_roots()
            || inputs.examples.len() > limits.max_examples
            || inputs.component_specs.len() > limits.max_components_per_example
            || inputs.universe_memberships.len() > limits.universe.max_candidates()
            || inputs
                .parents()
                .iter()
                .any(|parent| parent.dataset_id() == &output_dataset)
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        let output_rows = inputs
            .examples
            .len()
            .checked_mul(inputs.component_specs.len())
            .ok_or(DatasetBuildError::LimitExceeded)?;
        if output_rows > limits.max_output_rows {
            return Err(DatasetBuildError::LimitExceeded);
        }
        for example in &inputs.examples {
            let split = policy
                .split
                .split_for(example.cutoff_at)
                .ok_or(DatasetBuildError::TemporalLeakage)?;
            if example.label_cutoff_at > policy.split.split_end(split) {
                return Err(DatasetBuildError::TemporalLeakage);
            }
        }
        let retained = request_retained_bytes(&inputs)?;
        if retained > limits.max_retained_bytes {
            return Err(DatasetBuildError::LimitExceeded);
        }
        let policy_digest = super::canonical::policy_digest(&policy);
        let universe_digest = super::canonical::universe_contract_digest(&inputs);
        let build_spec_digest = DatasetBuildSpecDigest::try_new(
            super::canonical::build_spec_digest(
                &output_dataset,
                &inputs,
                intended_use,
                research_use_limits,
                &output_authorization,
                limits,
                policy_digest,
                universe_digest,
            )
            .bytes(),
        )?;
        Ok(Self {
            output_dataset,
            inputs,
            policy,
            intended_use,
            research_use_limits,
            output_authorization,
            limits,
            build_spec_digest,
            policy_digest,
            universe_digest,
            retained_bytes: retained,
        })
    }

    /// Returns the content-addressed complete build specification.
    pub const fn build_spec_digest(&self) -> DatasetBuildSpecDigest {
        self.build_spec_digest
    }

    pub(super) const fn output_dataset(&self) -> &DatasetId {
        &self.output_dataset
    }

    pub(super) const fn inputs(&self) -> &DatasetBuildInputs {
        &self.inputs
    }

    pub(super) const fn policy(&self) -> &DatasetBuildPolicy {
        &self.policy
    }

    pub(super) const fn intended_use(&self) -> ResearchUse {
        self.intended_use
    }

    pub(super) const fn research_use_limits(&self) -> ResearchUseLimits {
        self.research_use_limits
    }

    pub(super) const fn output_authorization(&self) -> &DatasetOutputAuthorization {
        &self.output_authorization
    }

    pub(super) const fn limits(&self) -> DatasetBuildLimits {
        self.limits
    }

    pub(super) const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    pub(super) const fn universe_digest(&self) -> Sha256Digest {
        self.universe_digest
    }

    /// Returns the checked Rust-visible bytes retained by this complete request.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Counts of admitted examples by chronological split.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DatasetSplitCounts {
    train_examples: usize,
    validation_examples: usize,
    test_examples: usize,
}

impl DatasetSplitCounts {
    pub(crate) const fn from_parts(
        train_examples: usize,
        validation_examples: usize,
        test_examples: usize,
    ) -> Self {
        Self {
            train_examples,
            validation_examples,
            test_examples,
        }
    }

    pub(super) fn record(&mut self, split: DatasetSplit) {
        match split {
            DatasetSplit::Train => self.train_examples += 1,
            DatasetSplit::Validation => self.validation_examples += 1,
            DatasetSplit::Test => self.test_examples += 1,
        }
    }

    /// Returns admitted train examples.
    pub const fn train_examples(self) -> usize {
        self.train_examples
    }

    /// Returns admitted validation examples.
    pub const fn validation_examples(self) -> usize {
        self.validation_examples
    }

    /// Returns admitted test examples.
    pub const fn test_examples(self) -> usize {
        self.test_examples
    }
}

/// Complete immutable derived-generation receipt and reproducibility identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureLabelDataset {
    pub(super) pinned: PinnedDataset,
    pub(super) build_spec_digest: DatasetBuildSpecDigest,
    pub(super) policy_digest: Sha256Digest,
    pub(super) universe_digest: Sha256Digest,
    pub(super) split_counts: DatasetSplitCounts,
    pub(super) universe_id: UniverseId,
    pub(super) split_policy: ChronologicalSplitPolicy,
    pub(super) point_in_time_policy: PointInTimePolicy,
    pub(super) missing_value_policy: MissingValuePolicy,
    pub(super) component_specs: Box<[FeatureLabelComponentSpec]>,
    pub(super) label_measurements: Box<[FeatureLabelMeasurementBinding]>,
}

impl FeatureLabelDataset {
    /// Returns the exact immutable derived generation.
    pub const fn pinned(&self) -> &PinnedDataset {
        &self.pinned
    }

    /// Returns the exact immutable manifest reference.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        self.pinned.manifest()
    }

    /// Returns the complete canonical build-specification identity.
    pub const fn build_spec_digest(&self) -> DatasetBuildSpecDigest {
        self.build_spec_digest
    }

    /// Returns the exact policy identity retained in Arrow metadata and lineage.
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    /// Returns the exact historical-universe contract identity.
    pub const fn universe_digest(&self) -> Sha256Digest {
        self.universe_digest
    }

    /// Returns admitted examples by chronological split.
    pub const fn split_counts(&self) -> DatasetSplitCounts {
        self.split_counts
    }

    /// Returns the exact measurements derived from all retained numeric label rows.
    #[must_use]
    pub fn label_measurements(&self) -> &[FeatureLabelMeasurementBinding] {
        &self.label_measurements
    }

    /// Produces the bounded canonical phase-one descriptor.
    ///
    /// This export proves an immutable analytical generation only; it carries no product/model
    /// admission or training authority without a matching closed production receipt.
    pub fn python_export(
        &self,
    ) -> Result<super::export::FeatureLabelPythonExport, DatasetBuildError> {
        super::export::encode(self)
    }
}

fn compare_examples(left: &DatasetExample, right: &DatasetExample) -> Ordering {
    left.cutoff_at
        .cmp(&right.cutoff_at)
        .then_with(|| left.instrument_id.cmp(&right.instrument_id))
        .then_with(|| left.example_id.cmp(&right.example_id))
}

fn request_retained_bytes(inputs: &DatasetBuildInputs) -> Result<usize, DatasetBuildError> {
    let mut retained = size_of::<DatasetBuildRequest>();
    for additional in [
        retained_array_bytes::<DatasetManifestRef>(inputs.parents().len())?,
        retained_array_bytes::<UniverseMembership>(inputs.universe_memberships.len())?,
        retained_array_bytes::<FeatureLabelComponentSpec>(inputs.component_specs.len())?,
        retained_array_bytes::<DatasetExample>(inputs.examples.len())?,
    ] {
        retained = retained
            .checked_add(additional)
            .ok_or(DatasetBuildError::LimitExceeded)?;
    }
    retained = retained
        .checked_add(inputs.universe_id().as_str().len())
        .ok_or(DatasetBuildError::LimitExceeded)?;
    for parent in inputs.parents() {
        retained = retained
            .checked_add(manifest_dynamic_bytes(parent)?)
            .ok_or(DatasetBuildError::LimitExceeded)?;
    }
    for membership in &inputs.universe_memberships {
        retained = retained
            .checked_add(manifest_dynamic_bytes(membership.source_manifest())?)
            .and_then(|bytes| {
                bytes.checked_add(availability_dynamic_bytes(membership.availability()))
            })
            .ok_or(DatasetBuildError::LimitExceeded)?;
    }
    for spec in &inputs.component_specs {
        retained = retained
            .checked_add(spec.name.len())
            .ok_or(DatasetBuildError::LimitExceeded)?;
    }
    for example in &inputs.examples {
        retained = retained
            .checked_add(example.example_id.len())
            .and_then(|value| {
                coordinate_dynamic_bytes(&example.effective_cutoff)
                    .ok()
                    .and_then(|bytes| value.checked_add(bytes))
            })
            .and_then(|value| {
                coordinate_dynamic_bytes(&example.label_effective_cutoff)
                    .ok()
                    .and_then(|bytes| value.checked_add(bytes))
            })
            .and_then(|value| {
                retained_array_bytes::<FeatureLabelComponentInput>(example.components.len())
                    .ok()
                    .and_then(|bytes| value.checked_add(bytes))
            })
            .ok_or(DatasetBuildError::LimitExceeded)?;
        for component in &example.components {
            retained = retained
                .checked_add(component.spec.name.len())
                .and_then(|bytes| {
                    bytes.checked_add(component_value_dynamic_bytes(&component.value))
                })
                .and_then(|bytes| {
                    retained_array_bytes::<ComponentSelector>(component.selectors.len())
                        .ok()
                        .and_then(|selectors| bytes.checked_add(selectors))
                })
                .ok_or(DatasetBuildError::LimitExceeded)?;
            for selector in &component.selectors {
                retained = retained
                    .checked_add(family_dynamic_bytes(selector.family())?)
                    .ok_or(DatasetBuildError::LimitExceeded)?;
            }
        }
    }
    Ok(retained)
}

fn manifest_dynamic_bytes(manifest: &DatasetManifestRef) -> Result<usize, DatasetBuildError> {
    manifest
        .dataset_id()
        .as_str()
        .len()
        .checked_add(manifest.schema().name().len())
        .ok_or(DatasetBuildError::LimitExceeded)
}

fn availability_dynamic_bytes(availability: &AvailabilityEvidence) -> usize {
    match availability {
        AvailabilityEvidence::Evidenced { evidence, .. } => evidence.as_str().len(),
        AvailabilityEvidence::Inferred { method, .. } => method.as_str().len(),
        AvailabilityEvidence::LocalFirstObserved { .. } | AvailabilityEvidence::Unknown => 0,
    }
}

fn component_value_dynamic_bytes(value: &ComponentValue) -> usize {
    match value {
        ComponentValue::Float { unit, .. } | ComponentValue::Decimal { unit, .. } => {
            unit.as_ref().map_or(0, |value| value.as_str().len())
        }
        ComponentValue::Missing { reason } => reason.as_str().len(),
    }
}

fn family_dynamic_bytes(family: &ObservationFamilyKey) -> Result<usize, DatasetBuildError> {
    match family {
        ObservationFamilyKey::Filing {
            source_id,
            accession,
            ..
        } => checked_family_dynamic(source_id, &[accession.as_str()], None),
        ObservationFamilyKey::Fundamental {
            source_id,
            concept,
            unit,
            ..
        } => checked_family_dynamic(source_id, &[concept.as_str(), unit.as_str()], None),
        ObservationFamilyKey::Macro {
            source_id,
            series,
            effective,
        } => checked_family_dynamic(source_id, &[series.as_str()], Some(effective)),
        ObservationFamilyKey::MarketBar {
            source_id,
            venue_id,
            provider_instrument_id,
            feed,
            interval,
            session,
            effective,
            ..
        } => checked_family_dynamic(
            source_id,
            &[
                venue_id.as_str(),
                provider_instrument_id.as_str(),
                feed.as_str(),
                interval.as_str(),
                session.ruleset().as_str(),
            ],
            Some(effective),
        ),
        ObservationFamilyKey::FundNav {
            source_id,
            provider_product,
            provider_channel,
            provider_instrument_id,
            currency,
            ..
        } => checked_family_dynamic(
            source_id,
            &[
                provider_product.as_source_identifier().as_str(),
                provider_channel.as_source_identifier().as_str(),
                provider_instrument_id.as_str(),
                currency.as_str(),
            ],
            None,
        ),
        ObservationFamilyKey::PortfolioPosition {
            source_id,
            account_id,
            effective,
            ..
        } => checked_family_dynamic(source_id, &[account_id.as_str()], Some(effective)),
        ObservationFamilyKey::Transaction {
            source_id,
            account_id,
            source_record_id,
            ..
        } => checked_family_dynamic(
            source_id,
            &[account_id.as_str(), source_record_id.as_str()],
            None,
        ),
        ObservationFamilyKey::CorporateAction {
            source_id,
            source_record,
            ..
        } => checked_family_dynamic(source_id, &[source_record.as_str()], None),
        ObservationFamilyKey::UniverseMembership {
            source_id,
            source_record,
            universe,
            ..
        } => checked_family_dynamic(
            source_id,
            &[source_record.as_str(), universe.as_str()],
            None,
        ),
        ObservationFamilyKey::AlternativeData {
            source_id,
            source_record,
            dataset,
            field,
            effective,
            ..
        } => checked_family_dynamic(
            source_id,
            &[source_record.as_str(), dataset.as_str(), field.as_str()],
            Some(effective),
        ),
    }
}

fn checked_family_dynamic(
    source: &SourceId,
    fields: &[&str],
    coordinate: Option<&ResearchTemporalCoordinate>,
) -> Result<usize, DatasetBuildError> {
    let mut retained = source.as_str().len();
    for field in fields {
        retained = retained
            .checked_add(field.len())
            .ok_or(DatasetBuildError::LimitExceeded)?;
    }
    if let Some(coordinate) = coordinate {
        retained = retained
            .checked_add(coordinate_dynamic_bytes(coordinate)?)
            .ok_or(DatasetBuildError::LimitExceeded)?;
    }
    Ok(retained)
}

fn coordinate_dynamic_bytes(
    coordinate: &ResearchTemporalCoordinate,
) -> Result<usize, DatasetBuildError> {
    let Some(period) = coordinate.source_period_value() else {
        return Ok(0);
    };
    period
        .scheme()
        .as_str()
        .len()
        .checked_add(period.code().as_str().len())
        .ok_or(DatasetBuildError::LimitExceeded)
}

fn retained_array_bytes<T>(len: usize) -> Result<usize, DatasetBuildError> {
    len.checked_mul(size_of::<T>())
        .ok_or(DatasetBuildError::LimitExceeded)
}

const fn selector_instrument(family: &ObservationFamilyKey) -> Option<InstrumentId> {
    match family {
        ObservationFamilyKey::Filing { instrument_id, .. }
        | ObservationFamilyKey::Fundamental { instrument_id, .. }
        | ObservationFamilyKey::MarketBar { instrument_id, .. }
        | ObservationFamilyKey::FundNav { instrument_id, .. }
        | ObservationFamilyKey::PortfolioPosition { instrument_id, .. }
        | ObservationFamilyKey::CorporateAction { instrument_id, .. }
        | ObservationFamilyKey::UniverseMembership { instrument_id, .. } => Some(*instrument_id),
        ObservationFamilyKey::AlternativeData { instrument_id, .. }
        | ObservationFamilyKey::Transaction { instrument_id, .. } => *instrument_id,
        ObservationFamilyKey::Macro { .. } => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectorScope {
    Instrument(InstrumentId),
    Account,
    Global,
}

const fn selector_scope(family: &ObservationFamilyKey) -> SelectorScope {
    if let Some(instrument) = selector_instrument(family) {
        return SelectorScope::Instrument(instrument);
    }
    match family {
        ObservationFamilyKey::Transaction { .. } => SelectorScope::Account,
        ObservationFamilyKey::Macro { .. } | ObservationFamilyKey::AlternativeData { .. } => {
            SelectorScope::Global
        }
        ObservationFamilyKey::Filing { .. }
        | ObservationFamilyKey::Fundamental { .. }
        | ObservationFamilyKey::MarketBar { .. }
        | ObservationFamilyKey::FundNav { .. }
        | ObservationFamilyKey::PortfolioPosition { .. }
        | ObservationFamilyKey::CorporateAction { .. }
        | ObservationFamilyKey::UniverseMembership { .. } => SelectorScope::Global,
    }
}

fn component_scope_matches(
    component: &FeatureLabelComponentInput,
    example_instrument: InstrumentId,
) -> bool {
    let mut has_declared_scope = component.spec.scope == ComponentScope::Global;
    for selector in component.selectors() {
        match (component.spec.scope, selector_scope(selector.family())) {
            (ComponentScope::Instrument, SelectorScope::Instrument(instrument))
                if instrument == example_instrument =>
            {
                has_declared_scope = true;
            }
            (ComponentScope::Instrument, SelectorScope::Global)
            | (ComponentScope::Account, SelectorScope::Global)
            | (ComponentScope::Global, SelectorScope::Global) => {}
            (ComponentScope::Account, SelectorScope::Account) => has_declared_scope = true,
            _ => return false,
        }
    }
    has_declared_scope
}

fn canonical_identifier(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn valid_unit(unit: &SourceIdentifier) -> bool {
    let value = unit.as_str();
    !value.is_empty()
        && value.len() <= 32
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b'%')
        })
}
