use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use crc32fast::Hasher;
use fs2::FileExt;
use tokio::sync::{mpsc, oneshot};

use crate::domain::RawEnvelope;

const MAGIC: &[u8; 4] = b"MEJ1";
const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

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
            writer.write_all(MAGIC)?;
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

pub struct JournalReader {
    reader: BufReader<File>,
    offset: u64,
}

impl JournalReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)
            .with_context(|| format!("failed to open journal {}", path.display()))?;
        let mut reader = BufReader::new(file);
        let mut magic = [0_u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            bail!("invalid journal header in {}", path.display());
        }
        Ok(Self { reader, offset: 4 })
    }

    pub fn next_record(&mut self) -> Result<Option<RawEnvelope>> {
        let mut length_bytes = [0_u8; 4];
        let first_byte_count = self.reader.read(&mut length_bytes[..1])?;
        if first_byte_count == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut length_bytes[1..])
            .context("truncated record length")?;

        let mut crc_bytes = [0_u8; 4];
        self.reader
            .read_exact(&mut crc_bytes)
            .context("truncated record checksum")?;
        let length = u32::from_le_bytes(length_bytes) as usize;
        if length > MAX_RECORD_BYTES {
            bail!(
                "journal record at offset {} is too large: {length}",
                self.offset
            );
        }

        let expected_crc = u32::from_le_bytes(crc_bytes);
        let mut payload = vec![0_u8; length];
        self.reader
            .read_exact(&mut payload)
            .context("truncated record payload")?;

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            bail!(
                "journal checksum mismatch at offset {}: expected={expected_crc}, actual={actual_crc}",
                self.offset
            );
        }

        self.offset += 8 + u64::try_from(length)?;
        Ok(Some(serde_json::from_slice(&payload)?))
    }

    pub fn read_all(mut self) -> Result<Vec<RawEnvelope>> {
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
