//! Bounded process boundary for admitting a Python-exported native bundle candidate.

use std::env;
use std::fs;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies, FeatureRegistry,
    MissingValuePolicy, ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_data::{
    ComponentKind, ComponentScope, CorporateActionSensitivity, FeatureLabelComponentSpec,
    PythonDatasetSelection, PythonDatasetVerificationLimits, Sha256Digest, verify_python_dataset,
};
use market_squawk_domain::{ModelId, RoundingPolicy, Timestamp};
use market_squawk_modeling::{
    BundleExpectations, BundleId, BundleMetadataRef, ControlledModelRoot, ModelBundle,
    TrainingDatasetIdentity, TrainingPeriod, VerifiedTrainingEnvironment,
    verify_validator_training_environment,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

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
    training_environment_sha256: String,
    bundle_metadata_sha256: String,
    artifact_sha256: String,
    training_run_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetWire {
    catalog_identity_sha256: String,
    dataset_id: String,
    export_sha256: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_sha256: String,
    manifest_sha256: String,
    build_spec_sha256: String,
    selected_component_rows: u64,
    selection_as_of_unix_nanos: i64,
    selection_sha256: String,
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
    let validator = env::current_exe().map_err(|_| ())?;
    let release_root = validator.parent().and_then(Path::parent).ok_or(())?;
    let training_environment =
        verify_validator_training_environment(release_root, &validator).map_err(|_| ())?;
    let candidate_root = controlled_root(&arguments.root)?;
    let authority_root = controlled_root(&arguments.authority_root)?;
    if candidate_root.starts_with(&authority_root) || authority_root.starts_with(&candidate_root) {
        return Err(());
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(20))
        .ok_or(())?;
    let cancellation = CancellationToken::new();
    let selection = verify_python_dataset(
        &arguments.catalog_root,
        Sha256Digest::new(parse_hex(&arguments.dataset_export_sha256)?),
        Timestamp::from_unix_nanos(arguments.dataset_as_of_unix_nanos),
        PythonDatasetVerificationLimits::try_new(100_000, 256 * 1024 * 1024).map_err(|_| ())?,
        deadline,
        &cancellation,
    )
    .map_err(|_| ())?;
    if selection.selection_sha256().bytes() != parse_hex(&arguments.dataset_selection_sha256)?
        || selection.catalog_identity().bytes() != parse_hex(&arguments.catalog_identity_sha256)?
        || candidate_root.starts_with(selection.local_root())
        || selection.local_root().starts_with(&candidate_root)
        || authority_root.starts_with(selection.local_root())
        || selection.local_root().starts_with(&authority_root)
    {
        return Err(());
    }
    let authority_path = controlled_path(&authority_root, &arguments.authority)?;
    let canonical_authority = fs::canonicalize(&authority_path).map_err(|_| ())?;
    if !canonical_authority.starts_with(&authority_root)
        || canonical_authority.starts_with(&candidate_root)
    {
        return Err(());
    }
    let metadata = fs::symlink_metadata(&authority_path).map_err(|_| ())?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_EXPECTATIONS_BYTES {
        return Err(());
    }
    let bytes = fs::read(authority_path).map_err(|_| ())?;
    let observed_authority_hash: [u8; 32] = Sha256::digest(&bytes).into();
    if observed_authority_hash != parse_hex(&arguments.authority_sha256)? {
        return Err(());
    }
    let wire: ExpectationsWire = serde_json::from_slice(&bytes).map_err(|_| ())?;
    let expectations = expectations(&wire, &selection, &training_environment)?;
    let registry = feature_registry()?;
    let root = ControlledModelRoot::open_ambient(candidate_root).map_err(|_| ())?;
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
    authority_root: PathBuf,
    authority: String,
    authority_sha256: String,
    catalog_root: PathBuf,
    dataset_export_sha256: String,
    dataset_as_of_unix_nanos: i64,
    dataset_selection_sha256: String,
    catalog_identity_sha256: String,
}

fn arguments() -> Result<Arguments, ()> {
    let values = env::args().skip(1).collect::<Vec<_>>();
    if values.len() != 22
        || values[0] != "--root"
        || values[2] != "--metadata"
        || values[4] != "--metadata-sha256"
        || values[6] != "--authority-root"
        || values[8] != "--authority"
        || values[10] != "--authority-sha256"
        || values[12] != "--catalog-root"
        || values[14] != "--dataset-export-sha256"
        || values[16] != "--dataset-as-of-unix-nanos"
        || values[18] != "--dataset-selection-sha256"
        || values[20] != "--catalog-identity-sha256"
    {
        return Err(());
    }
    parse_hex(&values[5])?;
    parse_hex(&values[11])?;
    parse_hex(&values[15])?;
    parse_hex(&values[19])?;
    parse_hex(&values[21])?;
    Ok(Arguments {
        root: PathBuf::from(&values[1]),
        metadata: values[3].clone(),
        metadata_sha256: values[5].clone(),
        authority_root: PathBuf::from(&values[7]),
        authority: values[9].clone(),
        authority_sha256: values[11].clone(),
        catalog_root: PathBuf::from(&values[13]),
        dataset_export_sha256: values[15].clone(),
        dataset_as_of_unix_nanos: values[17].parse().map_err(|_| ())?,
        dataset_selection_sha256: values[19].clone(),
        catalog_identity_sha256: values[21].clone(),
    })
}

fn controlled_root(root: &Path) -> Result<PathBuf, ()> {
    let metadata = fs::symlink_metadata(root).map_err(|_| ())?;
    if !metadata.file_type().is_dir() {
        return Err(());
    }
    fs::canonicalize(root).map_err(|_| ())
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

fn expectations(
    wire: &ExpectationsWire,
    selection: &PythonDatasetSelection,
    training_environment: &VerifiedTrainingEnvironment,
) -> Result<BundleExpectations, ()> {
    if wire.schema_version != 5
        || wire.label.kind != "label"
        || !dataset_matches_selection(&wire.dataset, selection)?
        || wire.universe_id != selection.identity().universe_id().as_str()
        || wire.training_code_revision != training_environment.training_code_revision()
        || parse_hex(&wire.training_environment_sha256)? != training_environment.receipt_sha256()
    {
        return Err(());
    }
    let identity = selection.identity();
    let dataset = TrainingDatasetIdentity::try_new(
        identity.manifest().clone(),
        identity.build_spec_digest(),
        identity.universe_digest(),
        identity.policy_digest(),
        selection.catalog_identity(),
        selection.export_sha256(),
        selection.selection_sha256(),
        selection.as_of(),
        NonZeroU64::new(u64::try_from(selection.selected_rows()).map_err(|_| ())?).ok_or(())?,
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
        identity.universe_id().clone(),
        TrainingPeriod::try_new(
            Timestamp::from_unix_nanos(wire.training_period.start_unix_nanos),
            Timestamp::from_unix_nanos(wire.training_period.end_unix_nanos),
        )
        .map_err(|_| ())?,
        label,
        training_environment.training_code_revision(),
        Sha256Digest::new(training_environment.receipt_sha256()),
        Sha256Digest::new(parse_hex(&wire.bundle_metadata_sha256)?),
        Sha256Digest::new(parse_hex(&wire.artifact_sha256)?),
        Sha256Digest::new(parse_hex(&wire.training_run_sha256)?),
    )
    .map_err(|_| ())
}

fn dataset_matches_selection(
    wire: &DatasetWire,
    selection: &PythonDatasetSelection,
) -> Result<bool, ()> {
    let identity = selection.identity();
    let manifest = identity.manifest();
    Ok(wire.dataset_id == manifest.dataset_id().as_str()
        && wire.manifest_version == manifest.manifest_version()
        && wire.schema_name == manifest.schema().name()
        && wire.schema_version == manifest.schema().version().get()
        && parse_hex(&wire.schema_sha256)? == manifest.schema().fingerprint()
        && parse_hex(&wire.manifest_sha256)? == manifest.content_hash().bytes()
        && parse_hex(&wire.build_spec_sha256)? == identity.build_spec_digest().digest().bytes()
        && parse_hex(&wire.universe_sha256)? == identity.universe_digest().bytes()
        && parse_hex(&wire.policy_sha256)? == identity.policy_digest().bytes()
        && parse_hex(&wire.catalog_identity_sha256)? == selection.catalog_identity().bytes()
        && parse_hex(&wire.export_sha256)? == selection.export_sha256().bytes()
        && parse_hex(&wire.selection_sha256)? == selection.selection_sha256().bytes()
        && wire.selection_as_of_unix_nanos == selection.as_of().unix_nanos()
        && wire.selected_component_rows
            == u64::try_from(selection.selected_rows()).map_err(|_| ())?)
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
