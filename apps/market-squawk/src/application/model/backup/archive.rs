//! Sequential archive framing. Only one verified member is resident at a time.
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{ModelBackupError, ModelBackupLimits, SEMANTIC_REVISION_DOMAIN};

const MAGIC: &[u8; 16] = b"MSQMODELSTREAM1!";
const MAXIMUM_ARCHIVE_PATH_BYTES: usize = 1_024;
const FOOTER_PATH: &str = "authority.sha256";

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

pub(super) struct ArchiveWriter<'a> {
    output: DigestingWriter<'a>,
    semantic: Sha256,
    members: usize,
    limits: ModelBackupLimits,
    cancellation: &'a CancellationToken,
}

impl<'a> ArchiveWriter<'a> {
    pub(super) fn new(
        writer: &'a mut (dyn Write + Send),
        limits: ModelBackupLimits,
        cancellation: &'a CancellationToken,
    ) -> Result<Self, ModelBackupError> {
        let mut output = DigestingWriter::new(writer, limits.maximum_archive_bytes());
        output.write_all(MAGIC)?;
        Ok(Self {
            output,
            semantic: semantic_digest(),
            members: 0,
            limits,
            cancellation,
        })
    }

    pub(super) fn member(&mut self, path: &str, bytes: &[u8]) -> Result<(), ModelBackupError> {
        if path == FOOTER_PATH
            || bytes.is_empty()
            || bytes.len() > self.limits.maximum_member_bytes().get()
            || self.members >= self.limits.maximum_members().get().saturating_sub(1)
        {
            return Err(ModelBackupError::Capacity);
        }
        write_member(&mut self.output, path, bytes, self.cancellation)?;
        update_semantic(&mut self.semantic, path, bytes)?;
        self.members += 1;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<([u8; 32], u64, [u8; 32]), ModelBackupError> {
        let revision: [u8; 32] = self.semantic.finalize().into();
        write_member(&mut self.output, FOOTER_PATH, &revision, self.cancellation)?;
        let (length, digest) = self.output.finish()?;
        Ok((revision, length, digest))
    }
}

pub(super) struct ArchiveReader<'a> {
    input: BoundedReader<'a>,
    semantic: Sha256,
    members: usize,
    limits: ModelBackupLimits,
}

impl<'a> ArchiveReader<'a> {
    pub(super) fn new(
        reader: &'a mut (dyn Read + Send),
        limits: ModelBackupLimits,
        cancellation: &'a CancellationToken,
    ) -> Result<Self, ModelBackupError> {
        let mut input = BoundedReader::new(reader, limits.maximum_archive_bytes(), cancellation);
        let mut magic = [0_u8; MAGIC.len()];
        let result = input.read_exact(&mut magic);
        if cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        result?;
        if &magic != MAGIC {
            return Err(ModelBackupError::Archive);
        }
        Ok(Self {
            input,
            semantic: semantic_digest(),
            members: 0,
            limits,
        })
    }

    pub(super) fn member(&mut self, expected: &str) -> Result<Box<[u8]>, ModelBackupError> {
        self.member_bounded(expected, self.limits.maximum_member_bytes().get())
    }

    pub(super) fn member_bounded(
        &mut self,
        expected: &str,
        maximum_bytes: usize,
    ) -> Result<Box<[u8]>, ModelBackupError> {
        if expected == FOOTER_PATH
            || self.members >= self.limits.maximum_members().get().saturating_sub(1)
        {
            return Err(ModelBackupError::Capacity);
        }
        let result = read_member(
            &mut self.input,
            maximum_bytes.min(self.limits.maximum_member_bytes().get()),
        );
        if self.input.cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        let (path, bytes) = result?;
        if path != expected {
            return Err(ModelBackupError::Archive);
        }
        update_semantic(&mut self.semantic, &path, &bytes)?;
        self.members += 1;
        Ok(bytes)
    }

    pub(super) fn finish(mut self) -> Result<[u8; 32], ModelBackupError> {
        let result = read_member(&mut self.input, 32);
        if self.input.cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        let (path, bytes) = result?;
        let revision: [u8; 32] = self.semantic.finalize().into();
        let mut trailing = [0_u8; 1];
        let result = self.input.read(&mut trailing);
        if self.input.cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        if path != FOOTER_PATH || bytes.as_ref() != revision || result? != 0 {
            return Err(ModelBackupError::Archive);
        }
        Ok(revision)
    }
}

fn semantic_digest() -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(SEMANTIC_REVISION_DOMAIN);
    digest
}

fn update_semantic(digest: &mut Sha256, path: &str, bytes: &[u8]) -> Result<(), ModelBackupError> {
    digest.update(
        u64::try_from(path.len())
            .map_err(|_| ModelBackupError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(path.as_bytes());
    digest.update(
        u64::try_from(bytes.len())
            .map_err(|_| ModelBackupError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(Sha256::digest(bytes));
    Ok(())
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

fn read_u16(reader: &mut impl Read) -> Result<u16, ModelBackupError> {
    let mut bytes = [0_u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_be_bytes(bytes))
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
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::other("model backup restore was cancelled"));
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
            .min(bytes.len())
            .min(64 * 1024);
        let read = self.reader.read(&mut bytes[..permitted])?;
        self.remaining = self
            .remaining
            .saturating_sub(u64::try_from(read).unwrap_or(u64::MAX));
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use super::{ArchiveReader, ArchiveWriter, MAGIC};
    use crate::application::model::backup::{ModelBackupError, ModelBackupLimits};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn streamed_archive_requires_complete_integrity_and_cancellation_terminates_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = ModelBackupLimits::standard()?;
        let cancellation = CancellationToken::new();
        let mut encoded = Vec::new();
        let mut writer = ArchiveWriter::new(&mut encoded, limits, &cancellation)?;
        writer.member("first.json", b"first")?;
        writer.member("last.json", b"last")?;
        let (revision, length, _) = writer.finish()?;
        assert_eq!(length, encoded.len() as u64);
        let read_complete = |bytes: &[u8]| -> Result<[u8; 32], ModelBackupError> {
            let mut input = Cursor::new(bytes);
            let mut reader = ArchiveReader::new(&mut input, limits, &cancellation)?;
            assert_eq!(reader.member("first.json")?.as_ref(), b"first");
            assert_eq!(reader.member("last.json")?.as_ref(), b"last");
            reader.finish()
        };
        assert_eq!(read_complete(&encoded)?, revision);
        // The last authority bytes must be present and authentic before restore can succeed.
        assert!(read_complete(&encoded[..encoded.len() - 1]).is_err());
        let mut corrupt = encoded.clone();
        let last = corrupt.last_mut().ok_or("empty archive")?;
        *last ^= 1;
        assert!(read_complete(&corrupt).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(read_complete(&trailing).is_err());

        struct CancelDuringRead<'a> {
            bytes: Cursor<&'a [u8]>,
            cancellation: CancellationToken,
        }
        impl Read for CancelDuringRead<'_> {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                let read = self.bytes.read(bytes)?;
                if self.bytes.position() > MAGIC.len() as u64 {
                    self.cancellation.cancel();
                }
                Ok(read)
            }
        }
        let cancelled = CancellationToken::new();
        let mut input = CancelDuringRead {
            bytes: Cursor::new(encoded.as_slice()),
            cancellation: cancelled.clone(),
        };
        let mut reader = ArchiveReader::new(&mut input, limits, &cancelled)?;
        assert!(matches!(
            reader.member("first.json"),
            Err(ModelBackupError::Cancelled)
        ));
        Ok(())
    }
}
