use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_domain::{CalendarDate, Currency, FundamentalCadence, SchemaVersion, Timestamp};
use serde::Deserialize;
use uuid::Uuid;

use super::{PythonDatasetCatalogError, PythonDatasetIdentity};
use crate::{
    ChronologicalSplitPolicy, ComponentKind, ComponentScope, CorporateActionSensitivity,
    DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry,
    FeatureLabelComponentSpec, FeatureLabelMeasurement, Sha256Digest, UniverseId,
};

const MAX_OBJECTS: usize = 128;
const MAX_COMPONENTS: usize = 1_024;
const MAX_PARENTS: usize = crate::MAX_DERIVED_GENERATION_PARENTS;

fn deserialize_study<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<crate::dataset_builder::study::StudyWire>, D::Error> {
    Option::<crate::dataset_builder::study::StudyWire>::deserialize(deserializer)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    #[serde(deserialize_with = "deserialize_study")]
    pub(super) study: Option<crate::dataset_builder::study::StudyWire>,
    pub(super) components: Vec<Component>,
    pub(super) dataset: Dataset,
    pub(super) missing_value_policy: String,
    pub(super) objects: Vec<Object>,
    pub(super) parents: Vec<Parent>,
    pub(super) point_in_time: PointInTime,
    pub(super) schema_version: u32,
    pub(super) split_counts: SplitCounts,
    pub(super) split_policy: SplitPolicy,
}

fn deserialize_price_input_origin<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<crate::DatasetPriceInputOrigin>, D::Error> {
    Option::deserialize(deserializer)
}

fn deserialize_population_source_use<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<crate::DatasetPopulationSourceUse>, D::Error> {
    Option::<crate::DatasetPopulationSourceUse>::deserialize(deserializer)
}

fn deserialize_population_partition<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<crate::DatasetPopulationPartition>, D::Error> {
    Option::<crate::DatasetPopulationPartition>::deserialize(deserializer)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Dataset {
    pub(super) build_spec_sha256: String,
    pub(super) dataset_id: String,
    pub(super) manifest_sha256: String,
    pub(super) manifest_version: u64,
    pub(super) policy_sha256: String,
    pub(super) population_basis: crate::DatasetPopulationBasis,
    #[serde(deserialize_with = "deserialize_price_input_origin")]
    pub(super) price_input_origin: Option<crate::DatasetPriceInputOrigin>,
    pub(super) population_member_count: usize,
    pub(super) population_unavailable: Vec<crate::CurrentPopulationInputUnavailable>,
    #[serde(deserialize_with = "deserialize_population_partition")]
    pub(super) population_partition: Option<crate::DatasetPopulationPartition>,
    #[serde(deserialize_with = "deserialize_population_source_use")]
    pub(super) population_source_use: Option<crate::DatasetPopulationSourceUse>,
    pub(super) schema_name: String,
    pub(super) schema_sha256: String,
    pub(super) schema_version: u16,
    pub(super) universe_id: String,
    pub(super) universe_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Component {
    pub(super) corporate_action_sensitivity: String,
    pub(super) kind: String,
    measurement: NullableMeasurement,
    pub(super) name: String,
    pub(super) scope: String,
    target: Target,
    pub(super) version: u32,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct NullableMeasurement(Option<Measurement>);

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Measurement {
    #[serde(rename = "financial_amount")]
    FinancialAmount {
        role: crate::FinancialAmountRole,
        basis: crate::FinancialAmountBasis,
        currency: String,
        share_convention: Option<crate::FinancialShareConvention>,
    },
    #[serde(rename = "price")]
    Price { currency: String },
    #[serde(rename = "return")]
    Return,
    #[serde(rename = "probability")]
    Probability,
    #[serde(rename = "other_regression")]
    OtherRegression,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Target {
    #[serde(rename = "fixed_horizon_event")]
    FixedHorizonEvent {
        horizon_nanos: u64,
        origin_basis: crate::FixedHorizonOriginBasis,
        event: crate::ProbabilityEventTarget,
    },
    #[serde(rename = "financial_period")]
    FinancialPeriod {
        cadence: FundamentalCadence,
        periods_ahead: std::num::NonZeroU16,
    },
    #[serde(rename = "not_applicable")]
    NotApplicable,
    #[serde(rename = "fixed_horizon_terminal")]
    FixedHorizonTerminal {
        horizon_nanos: u64,
        origin_basis: crate::FixedHorizonOriginBasis,
    },
    #[serde(rename = "unsupported")]
    Unsupported,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Object {
    pub(super) artifact_id: String,
    pub(super) lineage_sha256: String,
    pub(super) path: String,
    pub(super) row_count: u64,
    pub(super) sha256: String,
    pub(super) size_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Parent {
    pub(super) manifest: ParentManifest,
    pub(super) relation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ParentManifest {
    pub(super) dataset_id: String,
    pub(super) manifest_sha256: String,
    pub(super) manifest_version: u64,
    pub(super) schema_name: String,
    pub(super) schema_sha256: String,
    pub(super) schema_version: u16,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PointInTime {
    pub(super) revision_mode: String,
    pub(super) version: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SplitCounts {
    pub(super) train: usize,
    pub(super) validation: usize,
    pub(super) test: usize,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum SplitPolicy {
    ExactTime {
        train_end_unix_nanos: i64,
        validation_end_unix_nanos: i64,
        test_end_unix_nanos: i64,
    },
    FiscalDates {
        train_end: CalendarDate,
        validation_end: CalendarDate,
        test_end: CalendarDate,
    },
}
impl SplitPolicy {
    pub(super) fn decode(&self) -> Result<ChronologicalSplitPolicy, PythonDatasetCatalogError> {
        match *self {
            Self::ExactTime {
                train_end_unix_nanos,
                validation_end_unix_nanos,
                test_end_unix_nanos,
            } => ChronologicalSplitPolicy::try_new(
                Timestamp::from_unix_nanos(train_end_unix_nanos),
                Timestamp::from_unix_nanos(validation_end_unix_nanos),
                Timestamp::from_unix_nanos(test_end_unix_nanos),
            ),
            Self::FiscalDates {
                train_end,
                validation_end,
                test_end,
            } => ChronologicalSplitPolicy::try_fiscal(train_end, validation_end, test_end),
        }
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)
    }
}

impl Descriptor {
    pub(super) fn parse(bytes: &[u8]) -> Result<Self, PythonDatasetCatalogError> {
        let descriptor: Self = serde_json::from_slice(bytes)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<(), PythonDatasetCatalogError> {
        if self.schema_version != 4
            || self.components.is_empty()
            || self.components.len() > MAX_COMPONENTS
            || self.objects.is_empty()
            || self.objects.len() > MAX_OBJECTS
            || self.parents.is_empty()
            || self.parents.len() > MAX_PARENTS
            || self.point_in_time.revision_mode != "latest_known"
            || self.point_in_time.version != 1
            || !matches!(
                self.missing_value_policy.as_str(),
                "reject" | "preserve" | "drop_example"
            )
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        self.dataset.validate()?;
        if let Some(study) = &self.study {
            study
                .decode()
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        }
        self.split_policy.decode()?;

        let examples = self
            .split_counts
            .train
            .checked_add(self.split_counts.validation)
            .and_then(|value| value.checked_add(self.split_counts.test))
            .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
        self.validate_population(examples)?;
        let expected_rows = examples
            .checked_mul(self.components.len())
            .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
        let object_rows = self.objects.iter().try_fold(0_usize, |total, object| {
            usize::try_from(object.row_count)
                .ok()
                .and_then(|rows| total.checked_add(rows))
                .ok_or(PythonDatasetCatalogError::LimitExceeded)
        })?;
        if examples == 0 || expected_rows != object_rows {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }

        let mut component_identities = std::collections::BTreeSet::new();
        let mut kinds = std::collections::BTreeSet::new();
        for component in &self.components {
            component.validate()?;
            let identity = (
                component.kind.as_str(),
                component.name.as_str(),
                component.version,
            );
            if !component_identities.insert(identity) {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
            kinds.insert(component.kind.as_str());
        }
        let expected_kinds = if self
            .study
            .as_ref()
            .is_some_and(|study| study.purpose == crate::DatasetBuildPurpose::StudyInputs)
        {
            std::collections::BTreeSet::from(["feature"])
        } else {
            std::collections::BTreeSet::from(["feature", "label"])
        };
        if kinds != expected_kinds {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }

        let mut paths = std::collections::BTreeSet::new();
        for object in &self.objects {
            object.validate()?;
            if !paths.insert(object.path.as_str()) {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
        }
        let mut parents = Vec::new();
        parents
            .try_reserve_exact(self.parents.len())
            .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
        for parent in &self.parents {
            parent.validate()?;
            parents.push(parent.manifest_ref()?);
        }
        if let Some(study) = &self.study {
            let (policy, expected) = study
                .decode()
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
            let canonical = crate::DerivedGenerationParents::try_new(parents.clone())
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
            if canonical.as_slice() != parents.as_slice()
                || crate::dataset_builder::study::source_snapshot_from_parents(
                    policy.snapshot_as_of(),
                    &parents,
                ) != expected
            {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
        }
        Ok(())
    }

    fn validate_population(&self, examples: usize) -> Result<(), PythonDatasetCatalogError> {
        let study = self
            .study
            .as_ref()
            .map(|study| study.decode())
            .transpose()
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        self.dataset
            .population_basis
            .validate_study_policy(study.as_ref().map(|value| &value.0))
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        match (
            self.dataset.population_basis,
            self.dataset.population_source_use.as_ref(),
        ) {
            (crate::DatasetPopulationBasis::PublishedHistoricalMembership, None) => {}
            (
                crate::DatasetPopulationBasis::CurrentListedSnapshot
                | crate::DatasetPopulationBasis::PresentDayFixedCohort,
                Some(source_use),
            ) => {
                let purpose = study
                    .as_ref()
                    .ok_or(PythonDatasetCatalogError::CorruptAdmission)?
                    .0
                    .purpose();
                let required = match purpose {
                    crate::DatasetBuildPurpose::Training => crate::ResearchUse::Train,
                    crate::DatasetBuildPurpose::StudyInputs => crate::ResearchUse::LocalAnalysis,
                };
                source_use
                    .validate(required)
                    .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
            }
            _ => return Err(PythonDatasetCatalogError::CorruptAdmission),
        }
        let count = self.dataset.population_member_count;
        let unavailable = &self.dataset.population_unavailable;
        let partition = self.dataset.population_partition.as_ref();
        if let Some(partition) = partition {
            partition
                .validate(count)
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
            if unavailable.iter().any(|value| {
                partition
                    .member_ids()
                    .binary_search(&value.instrument_id())
                    .is_err()
            }) {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
        }
        let selected_count = partition.map_or(count, |value| value.member_ids().len());
        if count == 0
            || unavailable.len() >= selected_count
            || unavailable
                .windows(2)
                .any(|pair| pair[0].instrument_id() >= pair[1].instrument_id())
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        match self.dataset.population_basis {
            crate::DatasetPopulationBasis::PublishedHistoricalMembership => {
                if !unavailable.is_empty() || partition.is_some() {
                    return Err(PythonDatasetCatalogError::CorruptAdmission);
                }
            }
            crate::DatasetPopulationBasis::CurrentListedSnapshot => {
                if partition.is_none()
                    || examples.checked_add(unavailable.len()) != Some(selected_count)
                    || self.split_counts.train != 0
                    || self.split_counts.validation != 0
                {
                    return Err(PythonDatasetCatalogError::CorruptAdmission);
                }
            }
            crate::DatasetPopulationBasis::PresentDayFixedCohort => {
                if partition.is_none() || !unavailable.is_empty() || examples < selected_count {
                    return Err(PythonDatasetCatalogError::CorruptAdmission);
                }
            }
        }
        Ok(())
    }

    pub(super) fn identity(&self) -> Result<PythonDatasetIdentity, PythonDatasetCatalogError> {
        let schema = DatasetSchemaRef::try_new(
            &self.dataset.schema_name,
            SchemaVersion::new(self.dataset.schema_version)
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            digest(&self.dataset.schema_sha256)?,
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset.dataset_id.as_str())
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            self.dataset.manifest_version,
            schema,
            Sha256Digest::new(digest(&self.dataset.manifest_sha256)?),
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        Ok(PythonDatasetIdentity {
            manifest,
            build_spec_digest: DatasetBuildSpecDigest::try_new(digest(
                &self.dataset.build_spec_sha256,
            )?)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            universe_digest: Sha256Digest::new(digest(&self.dataset.universe_sha256)?),
            policy_digest: Sha256Digest::new(digest(&self.dataset.policy_sha256)?),
            universe_id: UniverseId::try_from(self.dataset.universe_id.as_str())
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            study_policy: self
                .study
                .as_ref()
                .map(|study| study.decode().map(|value| value.0))
                .transpose()
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            source_snapshot_digest: self
                .study
                .as_ref()
                .map(|study| study.decode().map(|value| value.1))
                .transpose()
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            split_policy: self.split_policy.decode()?,
            population_basis: self.dataset.population_basis,
            population_member_count: self.dataset.population_member_count,
            population_unavailable: self
                .dataset
                .population_unavailable
                .clone()
                .into_boxed_slice(),
            population_partition: self.dataset.population_partition.clone(),
            population_source_use: self.dataset.population_source_use.clone(),
        })
    }
}

impl Dataset {
    fn validate(&self) -> Result<(), PythonDatasetCatalogError> {
        DatasetId::try_from(self.dataset_id.as_str())
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        UniverseId::try_from(self.universe_id.as_str())
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        if self.manifest_version == 0 {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        let schema = DatasetSchemaRef::try_new(
            &self.schema_name,
            SchemaVersion::new(self.schema_version)
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            digest(&self.schema_sha256)?,
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        for value in [
            &self.build_spec_sha256,
            &self.manifest_sha256,
            &self.policy_sha256,
            &self.universe_sha256,
        ] {
            digest(value)?;
        }
        Ok(())
    }
}

impl Component {
    fn validate(&self) -> Result<(), PythonDatasetCatalogError> {
        let _measurement = self.measurement()?;
        let _target = self.fixed_horizon_nanos()?;
        if let Some(event) = self.probability_event_target()? {
            if self.measurement()? != Some(FeatureLabelMeasurement::Probability)
                || self.name != event.label_component_name()
                || self.version != 1
                || self.corporate_action_sensitivity != "requires_adjustment"
                || !matches!(
                    self.fixed_horizon_origin_basis(),
                    Some(
                        crate::FixedHorizonOriginBasis::CompletedBarClose
                            | crate::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar
                    )
                )
            {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
        }
        self.spec().map(|_spec| ())
    }

    pub(super) fn target_horizon(
        &self,
    ) -> Result<Option<crate::DatasetTargetHorizon>, PythonDatasetCatalogError> {
        let value = match (self.kind.as_str(), &self.target) {
            ("feature", Target::NotApplicable) | ("label", Target::Unsupported) => None,
            (
                "label",
                Target::FixedHorizonTerminal { horizon_nanos, .. }
                | Target::FixedHorizonEvent { horizon_nanos, .. },
            ) => Some(crate::DatasetTargetHorizon::ExactElapsed(
                std::time::Duration::from_nanos(*horizon_nanos),
            )),
            (
                "label",
                Target::FinancialPeriod {
                    cadence,
                    periods_ahead,
                },
            ) => Some(crate::DatasetTargetHorizon::FiscalPeriods {
                cadence: *cadence,
                periods_ahead: *periods_ahead,
            }),
            _ => return Err(PythonDatasetCatalogError::CorruptAdmission),
        };
        if let Some(value) = value {
            value
                .validate()
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        }
        Ok(value)
    }
    pub(super) fn fixed_horizon_nanos(
        &self,
    ) -> Result<Option<NonZeroU64>, PythonDatasetCatalogError> {
        Ok(self
            .target_horizon()?
            .and_then(|v| v.exact_elapsed())
            .and_then(|v| NonZeroU64::new(v.as_nanos() as u64)))
    }

    pub(super) fn probability_event_target(
        &self,
    ) -> Result<Option<crate::ProbabilityEventTarget>, PythonDatasetCatalogError> {
        match self.target {
            Target::FixedHorizonEvent { event, .. } => {
                event
                    .validate()
                    .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                Ok(Some(event))
            }
            _ => Ok(None),
        }
    }

    pub(super) fn fixed_horizon_origin_basis(&self) -> Option<crate::FixedHorizonOriginBasis> {
        match self.target {
            Target::FixedHorizonTerminal { origin_basis, .. }
            | Target::FixedHorizonEvent { origin_basis, .. } => Some(origin_basis),
            Target::NotApplicable | Target::Unsupported | Target::FinancialPeriod { .. } => None,
        }
    }

    pub(super) fn measurement(
        &self,
    ) -> Result<Option<FeatureLabelMeasurement>, PythonDatasetCatalogError> {
        match (self.kind.as_str(), &self.measurement.0) {
            ("feature", None) | ("label", None) => Ok(None),
            ("label", Some(measurement)) => {
                let measurement = match measurement {
                    Measurement::Price { currency } => {
                        let parsed = Currency::try_from(currency.as_str())
                            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                        if parsed.as_str() != currency {
                            return Err(PythonDatasetCatalogError::CorruptAdmission);
                        }
                        FeatureLabelMeasurement::Price { currency: parsed }
                    }
                    Measurement::FinancialAmount {
                        role,
                        basis,
                        currency,
                        share_convention,
                    } => {
                        let parsed = Currency::try_from(currency.as_str())
                            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                        let intent = crate::FinancialAmountSelection {
                            role: *role,
                            basis: *basis,
                            share_convention: *share_convention,
                        };
                        intent
                            .mapping()
                            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
                        if parsed.as_str() != currency {
                            return Err(PythonDatasetCatalogError::CorruptAdmission);
                        }
                        FeatureLabelMeasurement::FinancialAmount {
                            role: *role,
                            basis: *basis,
                            currency: parsed,
                            share_convention: *share_convention,
                        }
                    }
                    Measurement::Return => FeatureLabelMeasurement::Return,
                    Measurement::Probability => FeatureLabelMeasurement::Probability,
                    Measurement::OtherRegression => FeatureLabelMeasurement::OtherRegression,
                };
                Ok(Some(measurement))
            }
            _ => Err(PythonDatasetCatalogError::CorruptAdmission),
        }
    }

    pub(super) fn spec(&self) -> Result<FeatureLabelComponentSpec, PythonDatasetCatalogError> {
        let kind = match self.kind.as_str() {
            "feature" => ComponentKind::Feature,
            "label" => ComponentKind::Label,
            _ => return Err(PythonDatasetCatalogError::CorruptAdmission),
        };
        let scope = match self.scope.as_str() {
            "instrument" => ComponentScope::Instrument,
            "account" => ComponentScope::Account,
            "global" => ComponentScope::Global,
            _ => return Err(PythonDatasetCatalogError::CorruptAdmission),
        };
        let actions = match self.corporate_action_sensitivity.as_str() {
            "not_applicable" => CorporateActionSensitivity::NotApplicable,
            "requires_adjustment" => CorporateActionSensitivity::RequiresAdjustment,
            _ => return Err(PythonDatasetCatalogError::CorruptAdmission),
        };
        FeatureLabelComponentSpec::try_new(
            kind,
            scope,
            actions,
            &self.name,
            NonZeroU32::new(self.version).ok_or(PythonDatasetCatalogError::CorruptAdmission)?,
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)
    }
}

impl Object {
    fn validate(&self) -> Result<(), PythonDatasetCatalogError> {
        let artifact = Uuid::parse_str(&self.artifact_id)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        if artifact.is_nil()
            || artifact.to_string() != self.artifact_id
            || self.row_count == 0
            || self.size_bytes == 0
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        digest(&self.sha256)?;
        digest(&self.lineage_sha256)?;
        Ok(())
    }
}

impl Parent {
    fn manifest_ref(&self) -> Result<DatasetManifestRef, PythonDatasetCatalogError> {
        let schema = DatasetSchemaRef::try_new(
            &self.manifest.schema_name,
            SchemaVersion::new(self.manifest.schema_version)
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            digest(&self.manifest.schema_sha256)?,
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.manifest.dataset_id.as_str())
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            self.manifest.manifest_version,
            schema,
            Sha256Digest::new(digest(&self.manifest.manifest_sha256)?),
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)
    }

    fn validate(&self) -> Result<(), PythonDatasetCatalogError> {
        if self.relation != "derived_input" || self.manifest.manifest_version == 0 {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        DatasetId::try_from(self.manifest.dataset_id.as_str())
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        let schema = DatasetSchemaRef::try_new(
            &self.manifest.schema_name,
            SchemaVersion::new(self.manifest.schema_version)
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
            digest(&self.manifest.schema_sha256)?,
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        digest(&self.manifest.manifest_sha256)?;
        Ok(())
    }
}

pub(super) fn digest(value: &str) -> Result<[u8; 32], PythonDatasetCatalogError> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let mut output = [0_u8; 32];
    for (target, pair) in output.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = nibble(pair[0]).ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
        let low = nibble(pair[1]).ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
        *target = (high << 4) | low;
    }
    if output == [0; 32] {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(output)
}

fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
