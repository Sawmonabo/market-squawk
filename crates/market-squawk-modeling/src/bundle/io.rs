//! Bounded capability-relative reads, exact hashing, and allocation preflight.

use std::io::Read;
use std::path::{Component, Path};

use cap_std::fs::Dir;
use market_squawk_data::Sha256Digest;
use sha2::{Digest, Sha256};

use super::{BundleError, MAX_CONTROLLED_MODEL_PATH_BYTES};

const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_CONTAINER_ENTRIES: usize = crate::MAX_MODEL_FEATURES;
const MAX_JSON_MEMBERS: usize = 32_768;
const MAX_JSON_STRING_BYTES: usize = 4_096;

pub(super) fn read_exact_bounded(
    root: &Dir,
    relative_path: &str,
    maximum: usize,
    too_large: BundleError,
) -> Result<Vec<u8>, BundleError> {
    let mut file = root
        .open(relative_path)
        .map_err(|_| BundleError::ReadFailure)?;
    let metadata = file.metadata().map_err(|_| BundleError::ReadFailure)?;
    if !metadata.file_type().is_file() {
        return Err(BundleError::ReadFailure);
    }
    let byte_count = usize::try_from(metadata.len()).map_err(|_| too_large)?;
    if byte_count > maximum {
        return Err(too_large);
    }
    let mut bytes = vec![0_u8; byte_count];
    file.read_exact(&mut bytes)
        .map_err(|_| BundleError::ReadFailure)?;
    let mut extra = [0_u8; 1];
    if file
        .read(&mut extra)
        .map_err(|_| BundleError::ReadFailure)?
        != 0
    {
        return Err(too_large);
    }
    Ok(bytes)
}

pub(super) fn sha256_digest(bytes: &[u8]) -> Sha256Digest {
    Sha256Digest::new(Sha256::digest(bytes).into())
}

pub(super) fn validate_json_structure(bytes: &[u8]) -> Result<(), ()> {
    let mut kinds = [0_u8; MAX_JSON_DEPTH + 1];
    let mut separators = [0_usize; MAX_JSON_DEPTH + 1];
    let mut has_content = [false; MAX_JSON_DEPTH + 1];
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut string_bytes = 0_usize;
    let mut members = 0_usize;

    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
                string_bytes = 0;
            } else {
                string_bytes = string_bytes.checked_add(1).ok_or(())?;
                if string_bytes > MAX_JSON_STRING_BYTES {
                    return Err(());
                }
            }
            continue;
        }
        match *byte {
            b'"' => {
                in_string = true;
                has_content[depth] = true;
            }
            b'{' | b'[' => {
                has_content[depth] = true;
                depth = depth.checked_add(1).ok_or(())?;
                if depth > MAX_JSON_DEPTH {
                    return Err(());
                }
                kinds[depth] = *byte;
                separators[depth] = 0;
                has_content[depth] = false;
            }
            b'}' | b']' => {
                let expected = if *byte == b'}' { b'{' } else { b'[' };
                if depth == 0 || kinds[depth] != expected {
                    return Err(());
                }
                let entries = if has_content[depth] {
                    separators[depth].checked_add(1).ok_or(())?
                } else {
                    0
                };
                if entries > MAX_JSON_CONTAINER_ENTRIES {
                    return Err(());
                }
                depth -= 1;
            }
            b',' => {
                separators[depth] = separators[depth].checked_add(1).ok_or(())?;
                members = members.checked_add(1).ok_or(())?;
                if members > MAX_JSON_MEMBERS {
                    return Err(());
                }
            }
            byte if byte.is_ascii_whitespace() => {}
            _ => has_content[depth] = true,
        }
    }
    if depth == 0 && !in_string && !escaped {
        Ok(())
    } else {
        Err(())
    }
}

pub(super) fn is_controlled_relative_path(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_CONTROLLED_MODEL_PATH_BYTES
        || value.contains(['\\', ':'])
        || value.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
        })
    {
        return false;
    }
    let path = Path::new(value);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(segment) if !segment.is_empty()))
}
