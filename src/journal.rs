use std::{
    error::Error,
    fmt,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    num::TryFromIntError,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use crc32fast::Hasher;
use fs2::FileExt;
use tokio::sync::{mpsc, oneshot};

use crate::domain::RawEnvelope;

const CURRENT_MAGIC: &[u8; 4] = b"MSJ1";
const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalFormat {
    LegacyMej1,
    MarketSquawkMsj1,
}

impl TryFrom<[u8; 4]> for JournalFormat {
    type Error = JournalError;

    fn try_from(value: [u8; 4]) -> std::result::Result<Self, Self::Error> {
        match &value {
            b"MEJ1" => Ok(Self::LegacyMej1),
            b"MSJ1" => Ok(Self::MarketSquawkMsj1),
            _ => Err(JournalError::UnsupportedMagic(value)),
        }
    }
}

#[derive(Debug)]
pub enum JournalError {
    UnsupportedMagic([u8; 4]),
    Io {
        context: String,
        source: std::io::Error,
    },
    InvalidRecord(String),
    Json(serde_json::Error),
    LengthOverflow(TryFromIntError),
}

impl JournalError {
    fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

impl fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedMagic(magic) => {
                write!(formatter, "unsupported journal magic: {magic:?}")
            }
            Self::Io { context, source } => write!(formatter, "{context}: {source}"),
            Self::InvalidRecord(message) => formatter.write_str(message),
            Self::Json(source) => write!(formatter, "invalid journal payload: {source}"),
            Self::LengthOverflow(source) => {
                write!(formatter, "journal record length overflow: {source}")
            }
        }
    }
}

impl Error for JournalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Json(source) => Some(source),
            Self::LengthOverflow(source) => Some(source),
            Self::UnsupportedMagic(_) | Self::InvalidRecord(_) => None,
        }
    }
}

impl From<serde_json::Error> for JournalError {
    fn from(source: serde_json::Error) -> Self {
        Self::Json(source)
    }
}

impl From<TryFromIntError> for JournalError {
    fn from(source: TryFromIntError) -> Self {
        Self::LengthOverflow(source)
    }
}

pub struct JournalWriter {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl JournalWriter {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("failed to open journal {}", path.display()))?;
        FileExt::try_lock_exclusive(&file)
            .with_context(|| format!("journal {} already has an active writer", path.display()))?;

        let is_new = file.metadata()?.len() == 0;
        if !is_new {
            validate_existing_journal(&path)?;
        }

        let mut writer = BufWriter::new(file);
        if is_new {
            writer.write_all(CURRENT_MAGIC)?;
            writer.flush()?;
        }

        Ok(Self { path, writer })
    }

    pub fn append(&mut self, envelope: &RawEnvelope) -> Result<()> {
        let payload = serde_json::to_vec(envelope)?;
        if payload.len() > MAX_RECORD_BYTES {
            bail!("journal record exceeds {MAX_RECORD_BYTES} bytes");
        }

        let length = u32::try_from(payload.len()).context("journal record length overflow")?;
        let crc = crc32fast::hash(&payload);
        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone)]
pub struct JournalSink {
    sender: mpsc::Sender<JournalCommand>,
}

enum JournalCommand {
    Append(RawEnvelope, oneshot::Sender<Result<()>>),
    Flush(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<Result<()>>),
}

impl JournalSink {
    pub fn spawn(
        path: impl AsRef<Path>,
        capacity: usize,
    ) -> Result<(Self, tokio::task::JoinHandle<Result<()>>)> {
        if capacity == 0 {
            bail!("journal queue capacity must be greater than zero");
        }

        let mut writer = JournalWriter::open(path)?;
        let (sender, mut receiver) = mpsc::channel(capacity);
        let task = tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    JournalCommand::Append(envelope, response) => {
                        if let Err(error) = writer.append(&envelope) {
                            let message = format!("{error:#}");
                            let _ = response.send(Err(anyhow!(message.clone())));
                            return Err(anyhow!(message));
                        }
                        let _ = response.send(Ok(()));
                    }
                    JournalCommand::Flush(response) => {
                        let result = writer.flush();
                        let failed = result.is_err();
                        let message = result.as_ref().err().map(|error| format!("{error:#}"));
                        let _ = response.send(result);
                        if failed {
                            return Err(anyhow!(message.unwrap_or_else(|| {
                                "journal flush failed without an error message".to_owned()
                            })));
                        }
                    }
                    JournalCommand::Shutdown(response) => {
                        let result = writer.flush();
                        let failed = result.is_err();
                        let message = result.as_ref().err().map(|error| format!("{error:#}"));
                        let _ = response.send(result);
                        if failed {
                            return Err(anyhow!(message.unwrap_or_else(|| {
                                "journal shutdown flush failed without an error message".to_owned()
                            })));
                        }
                        break;
                    }
                }
            }
            Ok(())
        });
        Ok((Self { sender }, task))
    }

    /// Enqueue and acknowledge a raw record before the caller publishes its decoded event.
    pub async fn append(&self, envelope: RawEnvelope) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(JournalCommand::Append(envelope, sender))
            .await
            .context("journal writer stopped")?;
        receiver.await.context("journal append response dropped")?
    }

    pub async fn flush(&self) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(JournalCommand::Flush(sender))
            .await
            .context("journal writer stopped")?;
        receiver.await.context("journal flush response dropped")?
    }

    pub async fn shutdown(self) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(JournalCommand::Shutdown(sender))
            .await
            .context("journal writer stopped")?;
        receiver
            .await
            .context("journal shutdown response dropped")?
    }
}

pub struct JournalReader<R = File> {
    reader: BufReader<R>,
    offset: u64,
    format: Option<JournalFormat>,
}

impl JournalReader<File> {
    pub fn open(path: impl AsRef<Path>) -> std::result::Result<Self, JournalError> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|source| {
            JournalError::io(format!("failed to open journal {}", path.display()), source)
        })?;
        let mut reader = Self::new(file);
        reader.ensure_format()?;
        Ok(reader)
    }
}

impl<R: Read> JournalReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            offset: 4,
            format: None,
        }
    }

    fn ensure_format(&mut self) -> std::result::Result<JournalFormat, JournalError> {
        if let Some(format) = self.format {
            return Ok(format);
        }

        let mut magic = [0_u8; 4];
        self.reader
            .read_exact(&mut magic)
            .map_err(|source| JournalError::io("truncated journal header", source))?;
        let format = JournalFormat::try_from(magic)?;
        self.format = Some(format);
        Ok(format)
    }

    pub fn next_record(&mut self) -> std::result::Result<Option<RawEnvelope>, JournalError> {
        self.ensure_format()?;

        let mut length_bytes = [0_u8; 4];
        let first_byte_count = self
            .reader
            .read(&mut length_bytes[..1])
            .map_err(|source| JournalError::io("failed to read journal record length", source))?;
        if first_byte_count == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut length_bytes[1..])
            .map_err(|source| JournalError::io("truncated record length", source))?;

        let mut crc_bytes = [0_u8; 4];
        self.reader
            .read_exact(&mut crc_bytes)
            .map_err(|source| JournalError::io("truncated record checksum", source))?;
        let length = u32::from_le_bytes(length_bytes) as usize;
        if length > MAX_RECORD_BYTES {
            return Err(JournalError::InvalidRecord(format!(
                "journal record at offset {} is too large: {length}",
                self.offset
            )));
        }

        let expected_crc = u32::from_le_bytes(crc_bytes);
        let mut payload = vec![0_u8; length];
        self.reader
            .read_exact(&mut payload)
            .map_err(|source| JournalError::io("truncated record payload", source))?;

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            return Err(JournalError::InvalidRecord(format!(
                "journal checksum mismatch at offset {}: expected={expected_crc}, actual={actual_crc}",
                self.offset
            )));
        }

        self.offset += 8 + u64::try_from(length)?;
        Ok(Some(serde_json::from_slice(&payload)?))
    }

    pub fn read_all(mut self) -> std::result::Result<Vec<RawEnvelope>, JournalError> {
        let mut records = Vec::new();
        while let Some(record) = self.next_record()? {
            records.push(record);
        }
        Ok(records)
    }
}

fn validate_existing_journal(path: &Path) -> Result<()> {
    let mut reader = JournalReader::open(path)?;
    while reader.next_record()?.is_some() {}
    Ok(())
}
