//! Crash-safe exact-object representation authority for local extraction.

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;

use market_squawk_domain::{
    EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use market_squawk_sources::AvailabilityEvidence;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::FileAdapterError;

const STATE_SCHEMA_VERSION: u16 = 1;
const MAX_RETAINED_OBJECT_VERSIONS: usize = 8_192;

pub(crate) struct FileRepresentationAuthority {
    store: LocalAuthorityStateStore,
    state: Mutex<RepresentationState>,
}

impl fmt::Debug for FileRepresentationAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileRepresentationAuthority")
            .field("store", &"[EXCLUSIVE CAPABILITY]")
            .field("state", &"[BOUNDED EXACT-OBJECT STATE]")
            .finish()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RepresentationState {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    manifest_digest: EvidenceDigest,
    #[serde(deserialize_with = "deserialize_records")]
    records: Vec<ObjectRepresentation>,
    #[serde(skip)]
    poisoned: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectRepresentation {
    dataset: SourceIdentifier,
    object_id: SourceIdentifier,
    digest: EvidenceDigest,
    size_bytes: u64,
    observed_at: Timestamp,
    received_at: Option<Timestamp>,
    ingested_at: Option<Timestamp>,
}

struct BoundedStateWriter {
    bytes: Vec<u8>,
    maximum: usize,
    exhausted: bool,
}

impl FileRepresentationAuthority {
    pub(crate) fn try_open(
        state_root: impl AsRef<Path>,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
        manifest_digest: EvidenceDigest,
    ) -> Result<Self, FileAdapterError> {
        let store = LocalAuthorityStateStore::try_open(state_root).map_err(map_store_error)?;
        let state = match store.load().map_err(map_store_error)? {
            Some(payload) => {
                let state: RepresentationState = serde_json::from_slice(&payload)
                    .map_err(|_| FileAdapterError::RepresentationAuthorityInvalid)?;
                state.validate(source_id, metadata_revision, manifest_digest)?;
                state
            }
            None => {
                let state = RepresentationState {
                    schema_version: STATE_SCHEMA_VERSION,
                    source_id: source_id.clone(),
                    metadata_revision: metadata_revision.clone(),
                    manifest_digest,
                    records: Vec::new(),
                    poisoned: false,
                };
                persist(&store, &state)?;
                state
            }
        };
        Ok(Self {
            store,
            state: Mutex::new(state),
        })
    }

    pub(crate) fn bind_object(
        &self,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
        size_bytes: u64,
        sampled_at: Timestamp,
    ) -> Result<AvailabilityEvidence, FileAdapterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FileAdapterError::RepresentationAuthorityUnavailable)?;
        state.ensure_usable()?;
        if let Some(record) = state.find(dataset, object_id, digest) {
            if record.size_bytes != size_bytes {
                return Err(FileAdapterError::RepresentationAuthorityInvalid);
            }
            return Ok(AvailabilityEvidence::LocalFirstObserved {
                observed_at: record.observed_at,
            });
        }
        if state.records.len() >= MAX_RETAINED_OBJECT_VERSIONS {
            return Err(FileAdapterError::RepresentationAuthorityExhausted);
        }
        let observed_at = match state
            .records
            .iter()
            .filter(|record| record.dataset == *dataset && record.object_id == *object_id)
            .map(|record| record.observed_at)
            .max()
        {
            Some(previous) if sampled_at <= previous => previous
                .checked_add_nanos(1)
                .map_err(|_| FileAdapterError::ClockFailure)?,
            _ => sampled_at,
        };
        state
            .records
            .try_reserve_exact(1)
            .map_err(|_| FileAdapterError::RepresentationAuthorityExhausted)?;
        state.records.push(ObjectRepresentation {
            dataset: dataset.clone(),
            object_id: object_id.clone(),
            digest,
            size_bytes,
            observed_at,
            received_at: None,
            ingested_at: None,
        });
        if let Err(error) = persist(&self.store, &state) {
            state.poisoned = true;
            return Err(error);
        }
        Ok(AvailabilityEvidence::LocalFirstObserved { observed_at })
    }

    pub(crate) fn verify_object(
        &self,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
        size_bytes: u64,
        availability: &AvailabilityEvidence,
    ) -> Result<(), FileAdapterError> {
        let state = self
            .state
            .lock()
            .map_err(|_| FileAdapterError::RepresentationAuthorityUnavailable)?;
        state.ensure_usable()?;
        let Some(record) = state.find(dataset, object_id, digest) else {
            return Err(FileAdapterError::ObjectAvailabilityMismatch);
        };
        match availability {
            AvailabilityEvidence::LocalFirstObserved { observed_at }
                if *observed_at == record.observed_at && size_bytes == record.size_bytes =>
            {
                Ok(())
            }
            _ => Err(FileAdapterError::ObjectAvailabilityMismatch),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical operation timing remains bound to exact object evidence"
    )]
    pub(crate) fn operation_times(
        &self,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
        size_bytes: u64,
        availability: &AvailabilityEvidence,
        sampled_received_at: Timestamp,
        sampled_ingested_at: Timestamp,
    ) -> Result<(Timestamp, Timestamp), FileAdapterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FileAdapterError::RepresentationAuthorityUnavailable)?;
        state.ensure_usable()?;
        let record = state
            .find_mut(dataset, object_id, digest)
            .ok_or(FileAdapterError::ObjectAvailabilityMismatch)?;
        let AvailabilityEvidence::LocalFirstObserved { observed_at } = availability else {
            return Err(FileAdapterError::ObjectAvailabilityMismatch);
        };
        if record.size_bytes != size_bytes || record.observed_at != *observed_at {
            return Err(FileAdapterError::ObjectAvailabilityMismatch);
        }
        match (record.received_at, record.ingested_at) {
            (Some(received_at), Some(ingested_at)) => return Ok((received_at, ingested_at)),
            (None, None) => {}
            _ => return Err(FileAdapterError::RepresentationAuthorityInvalid),
        }
        if sampled_received_at < record.observed_at || sampled_ingested_at < sampled_received_at {
            return Err(FileAdapterError::ClockFailure);
        }
        record.received_at = Some(sampled_received_at);
        record.ingested_at = Some(sampled_ingested_at);
        if let Err(error) = persist(&self.store, &state) {
            state.poisoned = true;
            return Err(error);
        }
        Ok((sampled_received_at, sampled_ingested_at))
    }
}

impl RepresentationState {
    fn ensure_usable(&self) -> Result<(), FileAdapterError> {
        if self.poisoned {
            Err(FileAdapterError::RepresentationAuthorityUnavailable)
        } else {
            Ok(())
        }
    }

    fn validate(
        &self,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
        manifest_digest: EvidenceDigest,
    ) -> Result<(), FileAdapterError> {
        if self.schema_version != STATE_SCHEMA_VERSION
            || self.source_id != *source_id
            || self.metadata_revision != *metadata_revision
            || self.manifest_digest != manifest_digest
            || self.records.len() > MAX_RETAINED_OBJECT_VERSIONS
        {
            return Err(FileAdapterError::RepresentationAuthorityInvalid);
        }
        for (index, record) in self.records.iter().enumerate() {
            if matches!(
                (record.received_at, record.ingested_at),
                (Some(_), None) | (None, Some(_))
            ) || record
                .received_at
                .is_some_and(|received_at| received_at < record.observed_at)
                || record
                    .received_at
                    .zip(record.ingested_at)
                    .is_some_and(|(received_at, ingested_at)| ingested_at < received_at)
                || self.records[..index].iter().any(|previous| {
                    previous.dataset == record.dataset
                        && previous.object_id == record.object_id
                        && (previous.digest == record.digest
                            || previous.observed_at >= record.observed_at)
                })
            {
                return Err(FileAdapterError::RepresentationAuthorityInvalid);
            }
        }
        Ok(())
    }

    fn find(
        &self,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
    ) -> Option<&ObjectRepresentation> {
        self.records.iter().find(|record| {
            record.dataset == *dataset && record.object_id == *object_id && record.digest == digest
        })
    }

    fn find_mut(
        &mut self,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        digest: EvidenceDigest,
    ) -> Option<&mut ObjectRepresentation> {
        self.records.iter_mut().find(|record| {
            record.dataset == *dataset && record.object_id == *object_id && record.digest == digest
        })
    }
}

fn persist(
    store: &LocalAuthorityStateStore,
    state: &RepresentationState,
) -> Result<(), FileAdapterError> {
    let mut writer = BoundedStateWriter {
        bytes: Vec::new(),
        maximum: LocalAuthorityStateStore::maximum_payload_bytes(),
        exhausted: false,
    };
    if serde_json::to_writer(&mut writer, state).is_err() {
        return Err(if writer.exhausted {
            FileAdapterError::RepresentationAuthorityExhausted
        } else {
            FileAdapterError::RepresentationAuthorityInvalid
        });
    }
    store.store(&writer.bytes).map_err(map_store_error)
}

impl io::Write for BoundedStateWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(required) = self.bytes.len().checked_add(bytes.len()) else {
            self.exhausted = true;
            return Err(io::ErrorKind::OutOfMemory.into());
        };
        if required > self.maximum {
            self.exhausted = true;
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        if required > self.bytes.capacity() {
            let next_capacity = self
                .bytes
                .capacity()
                .max(1_024)
                .saturating_mul(2)
                .max(required)
                .min(self.maximum);
            let additional = next_capacity.saturating_sub(self.bytes.len());
            if self.bytes.try_reserve_exact(additional).is_err() {
                self.exhausted = true;
                return Err(io::ErrorKind::OutOfMemory.into());
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn map_store_error(error: LocalAuthorityStateStoreError) -> FileAdapterError {
    match error {
        LocalAuthorityStateStoreError::AlreadyLocked => {
            FileAdapterError::RepresentationAuthorityLocked
        }
        LocalAuthorityStateStoreError::PayloadTooLarge { .. }
        | LocalAuthorityStateStoreError::Allocation => {
            FileAdapterError::RepresentationAuthorityExhausted
        }
        LocalAuthorityStateStoreError::CorruptEnvelope
        | LocalAuthorityStateStoreError::EnvelopeTooLarge { .. }
        | LocalAuthorityStateStoreError::GenerationConflict
        | LocalAuthorityStateStoreError::GenerationExhausted
        | LocalAuthorityStateStoreError::StaleCommitContext => {
            FileAdapterError::RepresentationAuthorityInvalid
        }
        LocalAuthorityStateStoreError::UnsafeRoot
        | LocalAuthorityStateStoreError::UnsafeFileType
        | LocalAuthorityStateStoreError::RecoveryRequired
        | LocalAuthorityStateStoreError::FinalizationPending
        | LocalAuthorityStateStoreError::WriterUnavailable
        | LocalAuthorityStateStoreError::AtomicReplaceUnsupported
        | LocalAuthorityStateStoreError::SecureRootUnsupported
        | LocalAuthorityStateStoreError::VerificationFailed
        | LocalAuthorityStateStoreError::Io { .. } => {
            FileAdapterError::RepresentationAuthorityUnavailable
        }
    }
}

fn deserialize_records<'de, D>(deserializer: D) -> Result<Vec<ObjectRepresentation>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(RecordVisitor(PhantomData))
}

struct RecordVisitor(PhantomData<ObjectRepresentation>);

impl<'de> Visitor<'de> for RecordVisitor {
    type Value = Vec<ObjectRepresentation>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at most {MAX_RETAINED_OBJECT_VERSIONS} exact object representations"
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence
            .size_hint()
            .is_some_and(|hint| hint > MAX_RETAINED_OBJECT_VERSIONS)
        {
            return Err(serde::de::Error::custom(
                "representation record limit exceeded",
            ));
        }
        let mut records = Vec::new();
        while records.len() < MAX_RETAINED_OBJECT_VERSIONS {
            let Some(record) = sequence.next_element()? else {
                return Ok(records);
            };
            records
                .try_reserve_exact(1)
                .map_err(|_| serde::de::Error::custom("representation record allocation failed"))?;
            records.push(record);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                "representation record limit exceeded",
            ))
        } else {
            Ok(records)
        }
    }
}
