//! Bounded process boundary for admitting a Python-exported native bundle candidate.

use std::env;
use std::fs;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies, FeatureRegistry,
    MissingValuePolicy, ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_data::{
    ComponentKind, ComponentScope, CorporateActionSensitivity, DatasetBuildSpecDigest, DatasetId,
    DatasetManifestRef, DatasetSchemaRef, FeatureLabelComponentSpec, Sha256Digest, UniverseId,
};
use market_squawk_domain::{ModelId, RoundingPolicy, SchemaVersion, Timestamp};
use market_squawk_modeling::{
    BundleExpectations, BundleId, BundleMetadataRef, ControlledModelRoot, ModelBundle,
    TrainingDatasetIdentity, TrainingPeriod,
};
use serde::Deserialize;

const MAX_EXPECTATIONS_BYTES: u64 = 256 * 1024;
const FEATURE_IMPLEMENTATION_REVISION: &str = "task14-python-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectationsWire {
    schema_version: u32,
    model_id: String,
    bundle_id: String,
    bundle_version: u64,
    dataset: DatasetWire,
    universe_id: String,
    training_period: PeriodWire,
    label: LabelWire,
    training_code_revision: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetWire {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_sha256: String,
    manifest_sha256: String,
    build_spec_sha256: String,
    universe_sha256: String,
    policy_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeriodWire {
    start_unix_nanos: i64,
    end_unix_nanos: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LabelWire {
    kind: String,
    scope: String,
    corporate_action_sensitivity: String,
    name: String,
    version: u32,
}

fn main() {
    let result = run();
    match result {
        Ok(metadata_sha256) => {
            println!("{{\"metadata_sha256\":\"{metadata_sha256}\",\"status\":\"valid\"}}")
        }
        Err(()) => {
            eprintln!("model bundle candidate rejected");
            std::process::exit(2);
        }
    }
}

fn run() -> Result<String, ()> {
    let arguments = arguments()?;
    let expectation_path = controlled_path(&arguments.root, &arguments.expectations)?;
    let metadata = fs::symlink_metadata(&expectation_path).map_err(|_| ())?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_EXPECTATIONS_BYTES {
        return Err(());
    }
    let bytes = fs::read(expectation_path).map_err(|_| ())?;
    let wire: ExpectationsWire = serde_json::from_slice(&bytes).map_err(|_| ())?;
    let expectations = expectations(&wire)?;
    let registry = feature_registry()?;
    let root = ControlledModelRoot::open_ambient(&arguments.root).map_err(|_| ())?;
    let reference = BundleMetadataRef::try_new(
        &arguments.metadata,
        Sha256Digest::new(parse_hex(&arguments.metadata_sha256)?),
    )
    .map_err(|_| ())?;
    ModelBundle::load(&root, &reference, &expectations, &registry).map_err(|_| ())?;
    Ok(arguments.metadata_sha256)
}

struct Arguments {
    root: PathBuf,
    metadata: String,
    metadata_sha256: String,
    expectations: String,
}

fn arguments() -> Result<Arguments, ()> {
    let values = env::args().skip(1).collect::<Vec<_>>();
    if values.len() != 8
        || values[0] != "--root"
        || values[2] != "--metadata"
        || values[4] != "--metadata-sha256"
        || values[6] != "--expectations"
    {
        return Err(());
    }
    parse_hex(&values[5])?;
    Ok(Arguments {
        root: PathBuf::from(&values[1]),
        metadata: values[3].clone(),
        metadata_sha256: values[5].clone(),
        expectations: values[7].clone(),
    })
}

fn controlled_path(root: &Path, relative: &str) -> Result<PathBuf, ()> {
    if relative.is_empty()
        || relative.len() > 256
        || relative.contains(['\\', ':'])
        || Path::new(relative).is_absolute()
        || Path::new(relative)
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(());
    }
    Ok(root.join(relative))
}

fn expectations(wire: &ExpectationsWire) -> Result<BundleExpectations, ()> {
    if wire.schema_version != 1 || wire.label.kind != "label" {
        return Err(());
    }
    let schema_version = SchemaVersion::new(wire.dataset.schema_version).map_err(|_| ())?;
    let schema = DatasetSchemaRef::try_new(
        &wire.dataset.schema_name,
        schema_version,
        parse_hex(&wire.dataset.schema_sha256)?,
    )
    .map_err(|_| ())?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(wire.dataset.dataset_id.as_str()).map_err(|_| ())?,
        wire.dataset.manifest_version,
        schema,
        Sha256Digest::new(parse_hex(&wire.dataset.manifest_sha256)?),
    )
    .map_err(|_| ())?;
    let dataset = TrainingDatasetIdentity::try_new(
        manifest,
        DatasetBuildSpecDigest::try_new(parse_hex(&wire.dataset.build_spec_sha256)?)
            .map_err(|_| ())?,
        Sha256Digest::new(parse_hex(&wire.dataset.universe_sha256)?),
        Sha256Digest::new(parse_hex(&wire.dataset.policy_sha256)?),
    )
    .map_err(|_| ())?;
    let scope = match wire.label.scope.as_str() {
        "instrument" => ComponentScope::Instrument,
        "account" => ComponentScope::Account,
        "global" => ComponentScope::Global,
        _ => return Err(()),
    };
    let actions = match wire.label.corporate_action_sensitivity.as_str() {
        "not_applicable" => CorporateActionSensitivity::NotApplicable,
        "requires_adjustment" => CorporateActionSensitivity::RequiresAdjustment,
        _ => return Err(()),
    };
    let label = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        scope,
        actions,
        &wire.label.name,
        NonZeroU32::new(wire.label.version).ok_or(())?,
    )
    .map_err(|_| ())?;
    BundleExpectations::try_new(
        ModelId::from_str(&wire.model_id).map_err(|_| ())?,
        BundleId::try_new(&wire.bundle_id).map_err(|_| ())?,
        NonZeroU64::new(wire.bundle_version).ok_or(())?,
        dataset,
        UniverseId::try_from(wire.universe_id.as_str()).map_err(|_| ())?,
        TrainingPeriod::try_new(
            Timestamp::from_unix_nanos(wire.training_period.start_unix_nanos),
            Timestamp::from_unix_nanos(wire.training_period.end_unix_nanos),
        )
        .map_err(|_| ())?,
        label,
        &wire.training_code_revision,
    )
    .map_err(|_| ())
}

fn feature_registry() -> Result<FeatureRegistry, ()> {
    let config = BatchFeatureCatalogConfig::try_new(
        NonZeroU32::new(252).ok_or(())?,
        NonZeroU32::new(950_000).ok_or(())?,
        6,
        BatchFeaturePolicies::new(
            VarianceConvention::Sample,
            MissingValuePolicy::Reject,
            WeightPolicy::PositiveNormalized,
            RoundingPolicy::NearestEven,
            ShockComposition::Compounded,
        ),
    )
    .map_err(|_| ())?;
    let catalog =
        BatchFeatureCatalog::try_new(config, FEATURE_IMPLEMENTATION_REVISION).map_err(|_| ())?;
    let mut registry = FeatureRegistry::try_new(
        BatchFeatureCatalog::minimum_registry_capacity(),
        NonZeroUsize::new(4 * 1024 * 1024).ok_or(())?,
    )
    .map_err(|_| ())?;
    catalog.try_register(&mut registry).map_err(|_| ())?;
    Ok(registry)
}

fn parse_hex(value: &str) -> Result<[u8; 32], ()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(());
    }
    let mut bytes = [0_u8; 32];
    for (target, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = nibble(pair[0]).ok_or(())?;
        let low = nibble(pair[1]).ok_or(())?;
        *target = (high << 4) | low;
    }
    Ok(bytes)
}

fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
