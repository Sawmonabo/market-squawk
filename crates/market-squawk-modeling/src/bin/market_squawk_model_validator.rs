//! Bounded process boundary for admitting a Python-exported native bundle candidate.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use market_squawk_data::{CatalogEndpointIdentity, PythonDatasetVerificationLimits, Sha256Digest};
use market_squawk_domain::Timestamp;
use market_squawk_modeling::{
    BundleMetadataRef, ControlledModelRoot, ProductionFeatureRegistry,
    PythonDatasetAdmissionAuthority, verify_model_candidate, verify_validator_training_environment,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

const MAX_EXPECTATIONS_BYTES: u64 = 256 * 1024;

fn main() {
    match run() {
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
    let candidate_path = controlled_root(&arguments.root)?;
    let authority_root = controlled_root(&arguments.authority_root)?;
    if candidate_path.starts_with(&authority_root) || authority_root.starts_with(&candidate_path) {
        return Err(());
    }
    let authority_bytes = read_authority(
        &candidate_path,
        &authority_root,
        &arguments.authority,
        parse_hex(&arguments.authority_sha256)?,
    )?;
    let dataset_root = controlled_root(&arguments.catalog_root)?;
    if !candidate_root_is_admissible(&candidate_path, &dataset_root)
        || authority_root.starts_with(&dataset_root)
        || dataset_root.starts_with(&authority_root)
    {
        return Err(());
    }
    let catalog_identity =
        CatalogEndpointIdentity::try_from_bytes(parse_hex(&arguments.catalog_identity_sha256)?)
            .ok_or(())?;
    let dataset = PythonDatasetAdmissionAuthority::try_new(
        Sha256Digest::new(parse_hex(&arguments.dataset_export_sha256)?),
        Timestamp::from_unix_nanos(arguments.dataset_as_of_unix_nanos),
        Sha256Digest::new(parse_hex(&arguments.dataset_selection_sha256)?),
        catalog_identity,
    )
    .map_err(|_| ())?;
    let root = ControlledModelRoot::open_ambient(candidate_path).map_err(|_| ())?;
    let metadata_sha256 = Sha256Digest::new(parse_hex(&arguments.metadata_sha256)?);
    let reference =
        BundleMetadataRef::try_new(&arguments.metadata, metadata_sha256).map_err(|_| ())?;
    let registry = ProductionFeatureRegistry::try_new().map_err(|_| ())?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(20))
        .ok_or(())?;
    verify_model_candidate(
        &root,
        &reference,
        &authority_bytes,
        Sha256Digest::new(parse_hex(&arguments.authority_sha256)?),
        &dataset_root,
        dataset,
        &training_environment,
        &registry,
        PythonDatasetVerificationLimits::try_new(100_000, 256 * 1024 * 1024).map_err(|_| ())?,
        deadline,
        &CancellationToken::new(),
    )
    .map_err(|_| ())?;
    Ok(arguments.metadata_sha256)
}

fn candidate_root_is_admissible(candidate: &Path, dataset: &Path) -> bool {
    if dataset.starts_with(candidate) {
        return false;
    }
    if !candidate.starts_with(dataset) {
        return true;
    }
    let model_root = dataset.join("artifacts").join("models");
    let Ok(model_root) = controlled_root(&model_root) else {
        return false;
    };
    candidate != model_root && candidate.starts_with(model_root)
}

fn read_authority(
    candidate_root: &Path,
    authority_root: &Path,
    relative: &str,
    expected_sha256: [u8; 32],
) -> Result<Vec<u8>, ()> {
    let authority_path = controlled_path(authority_root, relative)?;
    let canonical_authority = fs::canonicalize(&authority_path).map_err(|_| ())?;
    if !canonical_authority.starts_with(authority_root)
        || canonical_authority.starts_with(candidate_root)
    {
        return Err(());
    }
    let metadata = fs::symlink_metadata(&authority_path).map_err(|_| ())?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_EXPECTATIONS_BYTES {
        return Err(());
    }
    let bytes = fs::read(authority_path).map_err(|_| ())?;
    if <[u8; 32]>::from(Sha256::digest(&bytes)) != expected_sha256 {
        return Err(());
    }
    Ok(bytes)
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
    for value in [
        &values[5],
        &values[11],
        &values[15],
        &values[19],
        &values[21],
    ] {
        parse_hex(value)?;
    }
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

const fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
