use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_domain::{Currency, SchemaVersion, Timestamp};
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
const MAX_PARENTS: usize = 64;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Dataset {
    pub(super) build_spec_sha256: String,
    pub(super) dataset_id: String,
    pub(super) manifest_sha256: String,
    pub(super) manifest_version: u64,
    pub(super) policy_sha256: String,
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
    #[serde(rename = "not_applicable")]
    NotApplicable,
    #[serde(rename = "fixed_horizon_terminal")]
    FixedHorizonTerminal { horizon_nanos: u64 },
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
#[serde(deny_unknown_fields)]
pub(super) struct SplitPolicy {
    pub(super) train_end_unix_nanos: i64,
    pub(super) validation_end_unix_nanos: i64,
    pub(super) test_end_unix_nanos: i64,
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
            || self.components.len() < 2
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
        ChronologicalSplitPolicy::try_new(
            Timestamp::from_unix_nanos(self.split_policy.train_end_unix_nanos),
            Timestamp::from_unix_nanos(self.split_policy.validation_end_unix_nanos),
            Timestamp::from_unix_nanos(self.split_policy.test_end_unix_nanos),
        )
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;

        let examples = self
            .split_counts
            .train
            .checked_add(self.split_counts.validation)
            .and_then(|value| value.checked_add(self.split_counts.test))
            .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
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
        if kinds != std::collections::BTreeSet::from(["feature", "label"]) {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }

        let mut paths = std::collections::BTreeSet::new();
        for object in &self.objects {
            object.validate()?;
            if !paths.insert(object.path.as_str()) {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
        }
        for parent in &self.parents {
            parent.validate()?;
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
        self.spec().map(|_spec| ())
    }

    pub(super) fn fixed_horizon_nanos(
        &self,
    ) -> Result<Option<NonZeroU64>, PythonDatasetCatalogError> {
        match (self.kind.as_str(), &self.target) {
            ("feature", Target::NotApplicable) | ("label", Target::Unsupported) => Ok(None),
            ("label", Target::FixedHorizonTerminal { horizon_nanos }) => {
                NonZeroU64::new(*horizon_nanos)
                    .map(Some)
                    .ok_or(PythonDatasetCatalogError::CorruptAdmission)
            }
            _ => Err(PythonDatasetCatalogError::CorruptAdmission),
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
