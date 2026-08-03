//! Capability-confined segment storage and query indices.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::{Read as _, Write as _},
    ops::Bound::{Included, Unbounded},
    sync::Mutex,
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _, OpenOptionsMaybeDirExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::Timestamp;
use market_squawk_platform::ControlRoot;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    DiagnosticArtifactAdmission, DiagnosticArtifactPublisher, DiagnosticArtifactReceipt,
    FORMAT_VERSION, LOG_DIRECTORY, LogDomain, LogSeverity, LogStoragePolicy, MAXIMUM_QUERY_LIMIT,
    MAXIMUM_RECORD_BYTES, MAXIMUM_SEGMENT_BYTES, StructuredLogError, StructuredLogEvent,
    StructuredLogPage, StructuredLogQuery, StructuredLogRecord,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PersistedLogRecord {
    format_version: u16,
    sequence: u64,
    event: StructuredLogEvent,
    record_sha256: [u8; 32],
}

impl PersistedLogRecord {
    fn try_new(sequence: u64, event: StructuredLogEvent) -> Result<Self, StructuredLogError> {
        let record_sha256 = record_digest(sequence, &event)?;
        Ok(Self {
            format_version: FORMAT_VERSION,
            sequence,
            event,
            record_sha256,
        })
    }

    fn verify(self) -> Result<StructuredLogRecord, StructuredLogError> {
        if self.format_version != FORMAT_VERSION
            || self.record_sha256 != record_digest(self.sequence, &self.event)?
        {
            return Err(StructuredLogError::CorruptStore);
        }
        Ok(StructuredLogRecord {
            sequence: self.sequence,
            event: self.event,
        })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum IndexKey {
    Severity(LogSeverity),
    Domain(LogDomain),
    Source(String),
    Job(String),
    Correlation(String),
}

#[derive(Debug)]
struct IndexedRecord {
    record: StructuredLogRecord,
    segment: u64,
}

#[derive(Debug, Default)]
struct LogIndex {
    records: BTreeMap<u64, IndexedRecord>,
    by_time: BTreeSet<(Timestamp, u64)>,
    dimensions: BTreeMap<IndexKey, BTreeSet<u64>>,
}

impl LogIndex {
    fn insert(
        &mut self,
        record: StructuredLogRecord,
        segment: u64,
    ) -> Result<(), StructuredLogError> {
        let sequence = record.sequence;
        if sequence == 0 || self.records.contains_key(&sequence) {
            return Err(StructuredLogError::CorruptStore);
        }
        self.by_time.insert((record.event.observed_at, sequence));
        for key in index_keys(&record.event) {
            self.dimensions.entry(key).or_default().insert(sequence);
        }
        self.records
            .insert(sequence, IndexedRecord { record, segment });
        Ok(())
    }

    fn remove_segment(&mut self, segment: u64) {
        let sequences = self
            .records
            .iter()
            .filter_map(|(sequence, indexed)| (indexed.segment == segment).then_some(*sequence))
            .collect::<Vec<_>>();
        for sequence in sequences {
            if let Some(indexed) = self.records.remove(&sequence) {
                self.by_time
                    .remove(&(indexed.record.event.observed_at, sequence));
                for key in index_keys(&indexed.record.event) {
                    let remove = self.dimensions.get_mut(&key).is_some_and(|values| {
                        values.remove(&sequence);
                        values.is_empty()
                    });
                    if remove {
                        self.dimensions.remove(&key);
                    }
                }
            }
        }
    }

    fn matches_dimensions(&self, sequence: u64, query: &StructuredLogQuery) -> bool {
        query
            .domain
            .is_none_or(|value| self.contains(&IndexKey::Domain(value), sequence))
            && query
                .source_id
                .as_ref()
                .is_none_or(|value| self.contains(&IndexKey::Source(value.clone()), sequence))
            && query
                .job_id
                .as_ref()
                .is_none_or(|value| self.contains(&IndexKey::Job(value.clone()), sequence))
            && query
                .correlation_id
                .as_ref()
                .is_none_or(|value| self.contains(&IndexKey::Correlation(value.clone()), sequence))
            && query.minimum_severity.is_none_or(|minimum| {
                [
                    LogSeverity::Trace,
                    LogSeverity::Debug,
                    LogSeverity::Info,
                    LogSeverity::Warn,
                    LogSeverity::Error,
                ]
                .into_iter()
                .filter(|severity| *severity >= minimum)
                .any(|severity| self.contains(&IndexKey::Severity(severity), sequence))
            })
    }

    fn contains(&self, key: &IndexKey, sequence: u64) -> bool {
        self.dimensions
            .get(key)
            .is_some_and(|sequences| sequences.contains(&sequence))
    }
}

fn index_keys(event: &StructuredLogEvent) -> Vec<IndexKey> {
    let mut keys = vec![
        IndexKey::Severity(event.severity),
        IndexKey::Domain(event.domain),
    ];
    keys.extend(event.source_id.iter().cloned().map(IndexKey::Source));
    keys.extend(event.job_id.iter().cloned().map(IndexKey::Job));
    keys.extend(
        event
            .correlation_id
            .iter()
            .cloned()
            .map(IndexKey::Correlation),
    );
    keys
}

#[derive(Debug)]
struct LogState {
    current_segment: u64,
    current_bytes: u64,
    next_sequence: u64,
    latest_observed_at: Option<Timestamp>,
    segments: BTreeSet<u64>,
    index: LogIndex,
}

/// Exclusive structured-log owner over one retained control-root capability.
pub struct StructuredLogStore {
    directory: Dir,
    policy: LogStoragePolicy,
    state: Mutex<LogState>,
}

impl StructuredLogStore {
    /// Opens the fixed no-follow log directory beneath the retained prepared control root.
    pub fn try_open(
        control_root: &ControlRoot,
        policy: LogStoragePolicy,
        observed_at: Timestamp,
    ) -> Result<Self, StructuredLogError> {
        let directory = prepare_log_directory(control_root)?;
        let mut state = load_state(&directory, policy)?;
        if state.current_bytes >= policy.segment_bytes {
            rotate(&directory, &mut state)?;
        }
        let cutoff = policy.retention_cutoff(observed_at)?;
        prune_expired_segments(&directory, &mut state, cutoff)?;
        enforce_segment_ceiling(&directory, &mut state, policy.maximum_segments)?;
        Ok(Self {
            directory,
            policy,
            state: Mutex::new(state),
        })
    }

    /// Durably appends after rotation and retention without reopening an ambient path.
    pub fn append(&self, event: StructuredLogEvent) -> Result<u64, StructuredLogError> {
        let cutoff = self.policy.retention_cutoff(event.observed_at)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| StructuredLogError::Unavailable)?;
        if state
            .latest_observed_at
            .is_some_and(|latest| event.observed_at < latest)
        {
            return Err(StructuredLogError::OutOfOrderTimestamp);
        }
        if current_segment_contains_expired(&state, cutoff) {
            rotate(&self.directory, &mut state)?;
        }
        prune_expired_segments(&self.directory, &mut state, cutoff)?;
        if state.index.records.len() >= self.policy.maximum_records {
            rotate(&self.directory, &mut state)?;
            remove_oldest_segment(&self.directory, &mut state)?;
        }
        let sequence = state.next_sequence;
        let persisted = PersistedLogRecord::try_new(sequence, event)?;
        let record = persisted.clone().verify()?;
        let mut encoded =
            serde_json::to_vec(&persisted).map_err(|_| StructuredLogError::Encoding)?;
        encoded.push(b'\n');
        if encoded.len() > MAXIMUM_RECORD_BYTES {
            return Err(StructuredLogError::RecordTooLarge);
        }
        let encoded_length =
            u64::try_from(encoded.len()).map_err(|_| StructuredLogError::Allocation)?;
        if state.current_bytes.saturating_add(encoded_length) > self.policy.segment_bytes {
            rotate(&self.directory, &mut state)?;
        }
        enforce_segment_ceiling(&self.directory, &mut state, self.policy.maximum_segments)?;
        append_segment(&self.directory, state.current_segment, &encoded)?;
        state.current_bytes = state
            .current_bytes
            .checked_add(encoded_length)
            .ok_or(StructuredLogError::Allocation)?;
        state.next_sequence = sequence
            .checked_add(1)
            .ok_or(StructuredLogError::SequenceExhausted)?;
        state.latest_observed_at = Some(record.event.observed_at);
        let current_segment = state.current_segment;
        state.index.insert(record, current_segment)?;
        Ok(sequence)
    }

    /// Executes a bounded query over time and exact structured indices.
    pub fn query(
        &self,
        query: &StructuredLogQuery,
    ) -> Result<StructuredLogPage, StructuredLogError> {
        query.validate()?;
        let state = self
            .state
            .lock()
            .map_err(|_| StructuredLogError::Unavailable)?;
        let start = query.from.map_or(Unbounded, |value| Included((value, 0)));
        let end = query
            .through
            .map_or(Unbounded, |value| Included((value, u64::MAX)));
        let mut records = Vec::new();
        records
            .try_reserve_exact(query.limit.saturating_add(1))
            .map_err(|_| StructuredLogError::Allocation)?;
        for (_, sequence) in state.index.by_time.range((start, end)) {
            if query.after_sequence.is_some_and(|after| *sequence <= after)
                || !state.index.matches_dimensions(*sequence, query)
            {
                continue;
            }
            let indexed = state
                .index
                .records
                .get(sequence)
                .ok_or(StructuredLogError::CorruptStore)?;
            if search_matches(&indexed.record, query.search.as_deref()) {
                records.push(indexed.record.clone());
            }
            if records.len() > query.limit {
                break;
            }
        }
        let has_more = records.len() > query.limit;
        records.truncate(query.limit);
        let next_after_sequence = has_more
            .then(|| records.last().map(StructuredLogRecord::sequence))
            .flatten();
        Ok(StructuredLogPage {
            records,
            next_after_sequence,
        })
    }

    /// Publishes bounded redacted NDJSON only through controlled artifact authority.
    pub async fn export(
        &self,
        mut query: StructuredLogQuery,
        publisher: &dyn DiagnosticArtifactPublisher,
        cancellation: CancellationToken,
        deadline: std::time::Instant,
    ) -> Result<DiagnosticArtifactReceipt, StructuredLogError> {
        query.limit = query.limit.min(MAXIMUM_QUERY_LIMIT);
        let mut cursor = query.after_sequence;
        let mut bytes = Vec::new();
        let mut record_count = 0usize;
        loop {
            ensure_export_live(&cancellation, deadline)?;
            query.after_sequence = cursor;
            let page = self.query(&query)?;
            for record in &page.records {
                ensure_export_live(&cancellation, deadline)?;
                serde_json::to_writer(&mut bytes, record)
                    .map_err(|_| StructuredLogError::Encoding)?;
                bytes.push(b'\n');
                record_count = record_count
                    .checked_add(1)
                    .ok_or(StructuredLogError::Allocation)?;
                if bytes.len() > self.policy.maximum_export_bytes {
                    return Err(StructuredLogError::ExportTooLarge);
                }
            }
            match page.next_after_sequence {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let sha256 = Sha256::digest(&bytes).into();
        let byte_length = u64::try_from(bytes.len()).map_err(|_| StructuredLogError::Allocation)?;
        ensure_export_live(&cancellation, deadline)?;
        let receipt = publisher
            .publish(
                DiagnosticArtifactAdmission {
                    media_type: "application/x-ndjson",
                    bytes,
                    sha256,
                    record_count,
                },
                cancellation,
                deadline,
            )
            .await?;
        if receipt.byte_length != byte_length || receipt.sha256 != sha256 {
            return Err(StructuredLogError::InvalidArtifactReceipt);
        }
        Ok(receipt)
    }
}

impl fmt::Debug for StructuredLogStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StructuredLogStore([RETAINED CONTROL CAPABILITY])")
    }
}

fn ensure_export_live(
    cancellation: &CancellationToken,
    deadline: std::time::Instant,
) -> Result<(), StructuredLogError> {
    if cancellation.is_cancelled() {
        return Err(StructuredLogError::Cancelled);
    }
    if std::time::Instant::now() >= deadline {
        return Err(StructuredLogError::DeadlineExceeded);
    }
    Ok(())
}

fn record_digest(
    sequence: u64,
    event: &StructuredLogEvent,
) -> Result<[u8; 32], StructuredLogError> {
    serde_json::to_vec(&("market-squawk-structured-log-v1", sequence, event))
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(|_| StructuredLogError::Encoding)
}

fn search_matches(record: &StructuredLogRecord, search: Option<&str>) -> bool {
    search.is_none_or(|search| {
        let search = search.to_ascii_lowercase();
        record.event.message.to_ascii_lowercase().contains(&search)
            || record.event.fields.iter().any(|(name, value)| {
                name.to_ascii_lowercase().contains(&search)
                    || value.to_ascii_lowercase().contains(&search)
            })
    })
}

fn prepare_log_directory(control_root: &ControlRoot) -> Result<Dir, StructuredLogError> {
    let control = control_root.try_clone_directory()?;
    match control.create_dir(LOG_DIRECTORY) {
        Ok(()) => sync_directory(&control)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(source) => return Err(StructuredLogError::Io { source }),
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .follow(FollowSymlinks::No)
        .maybe_dir(true);
    let opened = control
        .open_with(LOG_DIRECTORY, &options)
        .map_err(|source| StructuredLogError::Io { source })?;
    if !opened
        .metadata()
        .map_err(|source| StructuredLogError::Io { source })?
        .is_dir()
    {
        return Err(StructuredLogError::UnsafeFilesystemEntry);
    }
    let opened = opened.into_std();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        opened
            .set_permissions(std::fs::Permissions::from_mode(0o700))
            .map_err(|source| StructuredLogError::Io { source })?;
    }
    let directory = Dir::from_std_file(opened);
    if !directory
        .dir_metadata()
        .map_err(|source| StructuredLogError::Io { source })?
        .is_dir()
    {
        return Err(StructuredLogError::UnsafeFilesystemEntry);
    }
    Ok(directory)
}

fn load_state(directory: &Dir, policy: LogStoragePolicy) -> Result<LogState, StructuredLogError> {
    let mut segments = BTreeSet::new();
    for entry in directory
        .entries()
        .map_err(|source| StructuredLogError::Io { source })?
    {
        let entry = entry.map_err(|source| StructuredLogError::Io { source })?;
        if !entry
            .file_type()
            .map_err(|source| StructuredLogError::Io { source })?
            .is_file()
        {
            return Err(StructuredLogError::UnsafeFilesystemEntry);
        }
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or(StructuredLogError::UnsafeFilesystemEntry)?;
        segments.insert(parse_segment_name(name)?);
    }
    if segments.len() > super::MAXIMUM_SEGMENTS.saturating_mul(2) {
        return Err(StructuredLogError::CorruptStore);
    }
    if segments.is_empty() {
        create_segment(directory, 1)?;
        segments.insert(1);
    }
    let current_segment = *segments.last().ok_or(StructuredLogError::CorruptStore)?;
    let mut index = LogIndex::default();
    let mut greatest_sequence = 0u64;
    let mut latest_observed_at = None;
    for segment in segments.iter().copied() {
        let name = segment_name(segment);
        let file = open_regular(directory, &name, false)?;
        let length = file
            .metadata()
            .map_err(|source| StructuredLogError::Io { source })?
            .len();
        if length > MAXIMUM_SEGMENT_BYTES {
            return Err(StructuredLogError::CorruptStore);
        }
        let mut bytes = Vec::new();
        file.take(MAXIMUM_SEGMENT_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|source| StructuredLogError::Io { source })?;
        if u64::try_from(bytes.len()).map_or(true, |value| value > MAXIMUM_SEGMENT_BYTES) {
            return Err(StructuredLogError::CorruptStore);
        }
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            if line.len() > MAXIMUM_RECORD_BYTES {
                return Err(StructuredLogError::CorruptStore);
            }
            let record = serde_json::from_slice::<PersistedLogRecord>(line)
                .map_err(|_| StructuredLogError::CorruptStore)?
                .verify()?;
            if record.sequence <= greatest_sequence
                || latest_observed_at.is_some_and(|latest| record.event.observed_at < latest)
            {
                return Err(StructuredLogError::CorruptStore);
            }
            greatest_sequence = record.sequence;
            latest_observed_at = Some(record.event.observed_at);
            index.insert(record, segment)?;
            if index.records.len() > policy.maximum_records {
                return Err(StructuredLogError::CapacityExceeded);
            }
        }
    }
    let current_bytes = open_regular(directory, &segment_name(current_segment), false)?
        .metadata()
        .map_err(|source| StructuredLogError::Io { source })?
        .len();
    Ok(LogState {
        current_segment,
        current_bytes,
        next_sequence: greatest_sequence
            .checked_add(1)
            .ok_or(StructuredLogError::SequenceExhausted)?,
        latest_observed_at,
        segments,
        index,
    })
}

fn rotate(directory: &Dir, state: &mut LogState) -> Result<(), StructuredLogError> {
    let next = state
        .current_segment
        .checked_add(1)
        .ok_or(StructuredLogError::SequenceExhausted)?;
    create_segment(directory, next)?;
    state.current_segment = next;
    state.current_bytes = 0;
    state.segments.insert(next);
    Ok(())
}

fn current_segment_contains_expired(state: &LogState, cutoff: Timestamp) -> bool {
    state.index.records.values().any(|indexed| {
        indexed.segment == state.current_segment && indexed.record.event.observed_at < cutoff
    })
}

fn prune_expired_segments(
    directory: &Dir,
    state: &mut LogState,
    cutoff: Timestamp,
) -> Result<(), StructuredLogError> {
    let expired = state
        .segments
        .iter()
        .copied()
        .filter(|segment| *segment != state.current_segment)
        .filter(|segment| {
            state
                .index
                .records
                .values()
                .filter(|indexed| indexed.segment == *segment)
                .all(|indexed| indexed.record.event.observed_at < cutoff)
        })
        .collect::<Vec<_>>();
    for segment in expired {
        remove_segment(directory, state, segment)?;
    }
    Ok(())
}

fn enforce_segment_ceiling(
    directory: &Dir,
    state: &mut LogState,
    maximum_segments: usize,
) -> Result<(), StructuredLogError> {
    while state.segments.len() > maximum_segments {
        remove_oldest_segment(directory, state)?;
    }
    Ok(())
}

fn remove_oldest_segment(directory: &Dir, state: &mut LogState) -> Result<(), StructuredLogError> {
    let oldest = state
        .segments
        .iter()
        .copied()
        .find(|segment| *segment != state.current_segment)
        .ok_or(StructuredLogError::CapacityExceeded)?;
    remove_segment(directory, state, oldest)
}

fn remove_segment(
    directory: &Dir,
    state: &mut LogState,
    segment: u64,
) -> Result<(), StructuredLogError> {
    let name = segment_name(segment);
    validate_private_file(
        &directory
            .symlink_metadata(&name)
            .map_err(|source| StructuredLogError::Io { source })?,
    )?;
    directory
        .remove_file(&name)
        .map_err(|source| StructuredLogError::Io { source })?;
    sync_directory(directory)?;
    state.segments.remove(&segment);
    state.index.remove_segment(segment);
    Ok(())
}

fn create_segment(directory: &Dir, segment: u64) -> Result<(), StructuredLogError> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    configure_private_file(&mut options);
    let file = directory
        .open_with(segment_name(segment), &options)
        .map_err(|source| StructuredLogError::Io { source })?;
    validate_private_file(
        &file
            .metadata()
            .map_err(|source| StructuredLogError::Io { source })?,
    )?;
    file.sync_all()
        .map_err(|source| StructuredLogError::Io { source })?;
    sync_directory(directory)
}

fn append_segment(directory: &Dir, segment: u64, bytes: &[u8]) -> Result<(), StructuredLogError> {
    let mut options = OpenOptions::new();
    options.write(true).append(true).follow(FollowSymlinks::No);
    let mut file = directory
        .open_with(segment_name(segment), &options)
        .map_err(|source| StructuredLogError::Io { source })?;
    validate_private_file(
        &file
            .metadata()
            .map_err(|source| StructuredLogError::Io { source })?,
    )?;
    file.write_all(bytes)
        .and_then(|()| file.sync_data())
        .map_err(|source| StructuredLogError::Io { source })
}

fn open_regular(
    directory: &Dir,
    name: &str,
    write: bool,
) -> Result<cap_std::fs::File, StructuredLogError> {
    let mut options = OpenOptions::new();
    options.read(true).write(write).follow(FollowSymlinks::No);
    let file = directory
        .open_with(name, &options)
        .map_err(|source| StructuredLogError::Io { source })?;
    validate_private_file(
        &file
            .metadata()
            .map_err(|source| StructuredLogError::Io { source })?,
    )?;
    Ok(file)
}

#[cfg(unix)]
fn configure_private_file(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(windows)]
fn configure_private_file(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_private_file(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn validate_private_file(metadata: &cap_std::fs::Metadata) -> Result<(), StructuredLogError> {
    use cap_fs_ext::MetadataExt as _;
    use cap_std::fs::PermissionsExt as _;

    if !metadata.is_file() || metadata.nlink() != 1 || metadata.permissions().mode() & 0o077 != 0 {
        return Err(StructuredLogError::UnsafeFilesystemEntry);
    }
    Ok(())
}

#[cfg(windows)]
fn validate_private_file(metadata: &cap_std::fs::Metadata) -> Result<(), StructuredLogError> {
    use cap_fs_ext::MetadataExt as _;
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    {
        return Err(StructuredLogError::UnsafeFilesystemEntry);
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn validate_private_file(_metadata: &cap_std::fs::Metadata) -> Result<(), StructuredLogError> {
    Err(StructuredLogError::UnsafeFilesystemEntry)
}

fn segment_name(segment: u64) -> String {
    format!("segment-{segment:020}.jsonl")
}

fn parse_segment_name(name: &str) -> Result<u64, StructuredLogError> {
    name.strip_prefix("segment-")
        .and_then(|value| value.strip_suffix(".jsonl"))
        .and_then(|value| (value.len() == 20).then_some(value))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or(StructuredLogError::UnsafeFilesystemEntry)
}

#[cfg(unix)]
fn sync_directory(directory: &Dir) -> Result<(), StructuredLogError> {
    directory
        .try_clone()
        .map_err(|source| StructuredLogError::Io { source })?
        .into_std_file()
        .sync_all()
        .map_err(|source| StructuredLogError::Io { source })
}

#[cfg(windows)]
fn sync_directory(directory: &Dir) -> Result<(), StructuredLogError> {
    directory
        .dir_metadata()
        .map_err(|source| StructuredLogError::Io { source })?
        .is_dir()
        .then_some(())
        .ok_or(StructuredLogError::UnsafeFilesystemEntry)
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(_directory: &Dir) -> Result<(), StructuredLogError> {
    Err(StructuredLogError::UnsafeFilesystemEntry)
}
