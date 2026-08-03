use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{ModelBackupError, ModelBackupLimits, RetainedArchiveMember, hex};

const MAGIC: &[u8; 16] = b"MSQMODELARCHIVE1";
const ARCHIVE_VERSION: u16 = 1;
const MAXIMUM_ARCHIVE_PATH_BYTES: usize = 1_024;
const MAXIMUM_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SnapshotManifest {
    pub(super) schema_version: u16,
    pub(super) semantic_authority_revision: String,
    pub(super) runtime_index_path: String,
    pub(super) forecast_index_path: String,
    pub(super) models: Vec<ModelManifestRecord>,
    pub(super) forecast_artifacts: Vec<ForecastArtifactManifestRecord>,
    pub(super) members: Vec<MemberManifestRecord>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ModelManifestRecord {
    pub(super) model_id: String,
    pub(super) bundle_id: String,
    pub(super) bundle_version: u64,
    pub(super) candidate_directory: String,
    pub(super) metadata_path: String,
    pub(super) members: Vec<ModelMemberManifestRecord>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ModelMemberManifestRecord {
    pub(super) role: String,
    pub(super) relative_path: String,
    pub(super) archive_path: String,
    pub(super) byte_length: u64,
    pub(super) sha256: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ForecastArtifactManifestRecord {
    pub(super) artifact_id: String,
    pub(super) archive_path: String,
    pub(super) byte_length: u64,
    pub(super) sha256: String,
    pub(super) media_type: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct MemberManifestRecord {
    pub(super) path: String,
    pub(super) byte_length: u64,
    pub(super) sha256: String,
}

pub(super) struct DecodedArchive {
    pub(super) manifest: SnapshotManifest,
    pub(super) members: BTreeMap<String, Box<[u8]>>,
}

impl std::fmt::Debug for DecodedArchive {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DecodedArchive")
            .field("manifest", &self.manifest)
            .field("member_count", &self.members.len())
            .field("members", &"[VERIFIED ARCHIVE BYTES]")
            .finish()
    }
}

pub(super) fn write_archive(
    writer: &mut (dyn Write + Send),
    manifest: &SnapshotManifest,
    members: &[RetainedArchiveMember],
    limits: ModelBackupLimits,
    cancellation: &CancellationToken,
) -> Result<(u64, [u8; 32]), ModelBackupError> {
    let manifest_bytes = serde_json::to_vec(manifest).map_err(|_| ModelBackupError::Archive)?;
    if manifest_bytes.len() > MAXIMUM_MANIFEST_BYTES
        || serde_json::from_slice::<SnapshotManifest>(&manifest_bytes)
            .map_err(|_| ModelBackupError::Archive)?
            != *manifest
    {
        return Err(ModelBackupError::Archive);
    }
    let member_count = members
        .len()
        .checked_add(1)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(ModelBackupError::Capacity)?;
    if manifest.members.len() != members.len()
        || !manifest
            .members
            .iter()
            .zip(members)
            .all(|(expected, member)| {
                expected.path == member.path
                    && usize::try_from(expected.byte_length) == Ok(member.bytes.len())
                    && expected.sha256 == hex(Sha256::digest(&member.bytes).into())
            })
        || encoded_length(&manifest_bytes, members)? > limits.maximum_archive_bytes()
    {
        return Err(ModelBackupError::Archive);
    }
    let mut output = DigestingWriter::new(writer, limits.maximum_archive_bytes());
    output.write_all(MAGIC)?;
    output.write_all(&ARCHIVE_VERSION.to_be_bytes())?;
    output.write_all(&member_count.to_be_bytes())?;
    write_member(&mut output, "manifest.json", &manifest_bytes, cancellation)?;
    for member in members {
        if cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        write_member(&mut output, &member.path, &member.bytes, cancellation)?;
    }
    output.finish()
}

pub(super) fn read_archive(
    reader: &mut (dyn Read + Send),
    limits: ModelBackupLimits,
    cancellation: &CancellationToken,
) -> Result<DecodedArchive, ModelBackupError> {
    let mut input = BoundedReader::new(reader, limits.maximum_archive_bytes(), cancellation);
    let mut magic = [0_u8; MAGIC.len()];
    input.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(ModelBackupError::Archive);
    }
    let version = read_u16(&mut input)?;
    let count = usize::try_from(read_u32(&mut input)?).map_err(|_| ModelBackupError::Capacity)?;
    if version != ARCHIVE_VERSION || count == 0 || count > limits.maximum_members().get() {
        return Err(ModelBackupError::Archive);
    }
    let (manifest_path, manifest_bytes) = read_member(&mut input, MAXIMUM_MANIFEST_BYTES)?;
    if manifest_path != "manifest.json" {
        return Err(ModelBackupError::Archive);
    }
    let manifest = serde_json::from_slice::<SnapshotManifest>(&manifest_bytes)
        .map_err(|_| ModelBackupError::Archive)?;
    if manifest.schema_version != ARCHIVE_VERSION
        || serde_json::to_vec(&manifest)
            .map_err(|_| ModelBackupError::Archive)?
            .as_slice()
            != manifest_bytes.as_ref()
        || manifest.members.len().checked_add(1) != Some(count)
    {
        return Err(ModelBackupError::Archive);
    }
    let expected = manifest
        .members
        .iter()
        .map(|member| member.path.as_str())
        .collect::<BTreeSet<_>>();
    if expected.len() != manifest.members.len()
        || !expected.iter().all(|path| valid_archive_path(path))
    {
        return Err(ModelBackupError::Archive);
    }
    let mut members = BTreeMap::new();
    for expected_member in &manifest.members {
        let (path, bytes) = read_member(&mut input, limits.maximum_member_bytes().get())?;
        if path != expected_member.path
            || !expected.contains(path.as_str())
            || members.insert(path, bytes).is_some()
        {
            return Err(ModelBackupError::Archive);
        }
    }
    let mut trailing = [0_u8; 1];
    if input.read(&mut trailing)? != 0 || members.len() != manifest.members.len() {
        return Err(ModelBackupError::Archive);
    }
    for expected in &manifest.members {
        let bytes = members
            .get(&expected.path)
            .ok_or(ModelBackupError::Archive)?;
        if u64::try_from(bytes.len()) != Ok(expected.byte_length)
            || hex(Sha256::digest(bytes).into()) != expected.sha256
        {
            return Err(ModelBackupError::Archive);
        }
    }
    Ok(DecodedArchive { manifest, members })
}

fn write_member(
    writer: &mut DigestingWriter<'_>,
    path: &str,
    bytes: &[u8],
    cancellation: &CancellationToken,
) -> Result<(), ModelBackupError> {
    if !valid_archive_path(path) {
        return Err(ModelBackupError::Archive);
    }
    let path_length = u16::try_from(path.len()).map_err(|_| ModelBackupError::Capacity)?;
    let byte_length = u64::try_from(bytes.len()).map_err(|_| ModelBackupError::Capacity)?;
    writer.write_all(&path_length.to_be_bytes())?;
    writer.write_all(&byte_length.to_be_bytes())?;
    writer.write_all(&Sha256::digest(bytes))?;
    writer.write_all(path.as_bytes())?;
    for chunk in bytes.chunks(64 * 1024) {
        if cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        writer.write_all(chunk)?;
    }
    Ok(())
}

fn read_member(
    reader: &mut BoundedReader<'_>,
    maximum_bytes: usize,
) -> Result<(String, Box<[u8]>), ModelBackupError> {
    let path_length = usize::from(read_u16(reader)?);
    let byte_length = usize::try_from(read_u64(reader)?).map_err(|_| ModelBackupError::Capacity)?;
    if path_length == 0
        || path_length > MAXIMUM_ARCHIVE_PATH_BYTES
        || byte_length == 0
        || byte_length > maximum_bytes
    {
        return Err(ModelBackupError::Archive);
    }
    let mut expected_sha256 = [0_u8; 32];
    reader.read_exact(&mut expected_sha256)?;
    let mut path = vec![0_u8; path_length];
    reader.read_exact(&mut path)?;
    let path = String::from_utf8(path).map_err(|_| ModelBackupError::Archive)?;
    if !valid_archive_path(&path) {
        return Err(ModelBackupError::Archive);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(byte_length)
        .map_err(|_| ModelBackupError::Capacity)?;
    bytes.resize(byte_length, 0);
    reader.read_exact(&mut bytes)?;
    if <[u8; 32]>::from(Sha256::digest(&bytes)) != expected_sha256 {
        return Err(ModelBackupError::Archive);
    }
    Ok((path, bytes.into_boxed_slice()))
}

fn valid_archive_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAXIMUM_ARCHIVE_PATH_BYTES
        && !value.contains(['\\', ':'])
        && value.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && component.len() <= 255
                && !component.ends_with('.')
                && component.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'-' | b'_' | b'.')
                })
        })
}

fn encoded_length(
    manifest: &[u8],
    members: &[RetainedArchiveMember],
) -> Result<u64, ModelBackupError> {
    let fixed_header =
        u64::try_from(MAGIC.len() + 2 + 4).map_err(|_| ModelBackupError::Capacity)?;
    std::iter::once(("manifest.json", manifest))
        .chain(
            members
                .iter()
                .map(|member| (member.path.as_str(), member.bytes.as_ref())),
        )
        .try_fold(fixed_header, |total, (path, bytes)| {
            let path = u64::try_from(path.len()).map_err(|_| ModelBackupError::Capacity)?;
            let bytes = u64::try_from(bytes.len()).map_err(|_| ModelBackupError::Capacity)?;
            total
                .checked_add(2 + 8 + 32)
                .and_then(|value| value.checked_add(path))
                .and_then(|value| value.checked_add(bytes))
                .ok_or(ModelBackupError::Capacity)
        })
}

fn read_u16(reader: &mut impl Read) -> Result<u16, ModelBackupError> {
    let mut bytes = [0_u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> Result<u32, ModelBackupError> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, ModelBackupError> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

struct DigestingWriter<'writer> {
    writer: &'writer mut (dyn Write + Send),
    maximum: u64,
    written: u64,
    digest: Sha256,
}

impl<'writer> DigestingWriter<'writer> {
    fn new(writer: &'writer mut (dyn Write + Send), maximum: u64) -> Self {
        Self {
            writer,
            maximum,
            written: 0,
            digest: Sha256::new(),
        }
    }

    fn finish(self) -> Result<(u64, [u8; 32]), ModelBackupError> {
        self.writer.flush()?;
        Ok((self.written, self.digest.finalize().into()))
    }
}

impl Write for DigestingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| std::io::Error::other("model backup write length overflow"))?;
        let proposed = self
            .written
            .checked_add(length)
            .ok_or_else(|| std::io::Error::other("model backup write length overflow"))?;
        if proposed > self.maximum {
            return Err(std::io::Error::other(
                "model backup archive byte ceiling exceeded",
            ));
        }
        self.writer.write_all(bytes)?;
        self.digest.update(bytes);
        self.written = proposed;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

struct BoundedReader<'reader> {
    reader: &'reader mut (dyn Read + Send),
    remaining: u64,
    cancellation: &'reader CancellationToken,
}

impl<'reader> BoundedReader<'reader> {
    const fn new(
        reader: &'reader mut (dyn Read + Send),
        maximum: u64,
        cancellation: &'reader CancellationToken,
    ) -> Self {
        Self {
            reader,
            remaining: maximum,
            cancellation,
        }
    }
}

impl Read for BoundedReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "model backup restore was cancelled",
            ));
        }
        if self.remaining == 0 {
            let mut trailing = [0_u8; 1];
            return match self.reader.read(&mut trailing)? {
                0 => Ok(0),
                _ => Err(std::io::Error::other(
                    "model backup archive byte ceiling exceeded",
                )),
            };
        }
        let permitted = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let read = self.reader.read(&mut bytes[..permitted])?;
        self.remaining = self
            .remaining
            .saturating_sub(u64::try_from(read).unwrap_or(u64::MAX));
        Ok(read)
    }
}
