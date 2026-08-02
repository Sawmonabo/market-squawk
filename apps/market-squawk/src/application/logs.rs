//! Bounded, redacted structured local diagnostic logs.

mod store;
mod tracing_layer;

use std::{collections::BTreeMap, fmt, time::Duration};

use async_trait::async_trait;
use market_squawk_domain::Timestamp;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub use store::StructuredLogStore;
pub use tracing_layer::{
    StructuredLogDrain, StructuredLogDrainEvidence, StructuredLogLayer, StructuredLogWorker,
};

pub(super) const FORMAT_VERSION: u16 = 1;
pub(super) const LOG_DIRECTORY: &str = "structured-logs";
pub(super) const MINIMUM_SEGMENT_BYTES: u64 = 64 * 1024;
pub(super) const MAXIMUM_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const MAXIMUM_SEGMENTS: usize = 64;
pub(super) const MAXIMUM_RECORD_BYTES: usize = 32 * 1024;
pub(super) const MAXIMUM_QUERY_LIMIT: usize = 1_000;
const MAXIMUM_FIELD_COUNT: usize = 32;
const MAXIMUM_FIELD_NAME_BYTES: usize = 64;
const MAXIMUM_FIELD_VALUE_BYTES: usize = 2 * 1024;
const MAXIMUM_MESSAGE_BYTES: usize = 4 * 1024;
const MAXIMUM_FILTER_BYTES: usize = 256;
const MAXIMUM_EXPORT_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_ARTIFACT_REFERENCE_BYTES: usize = 256;

/// Severity of one structured diagnostic fact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogSeverity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Stable product area responsible for a diagnostic fact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogDomain {
    Application,
    Source,
    Market,
    Research,
    Portfolio,
    Model,
    Backtest,
    Execution,
    Risk,
    FairValue,
    Mcp,
    Lifecycle,
}

/// Storage, retention, and result bounds owned by the installed service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogStoragePolicy {
    pub(super) segment_bytes: u64,
    pub(super) maximum_segments: usize,
    retention: Duration,
    pub(super) maximum_records: usize,
    pub(super) maximum_export_bytes: usize,
}

impl LogStoragePolicy {
    /// Creates a policy whose worst-case disk and memory ownership is finite.
    pub fn try_new(
        segment_bytes: u64,
        maximum_segments: usize,
        retention: Duration,
        maximum_records: usize,
        maximum_export_bytes: usize,
    ) -> Result<Self, StructuredLogError> {
        if !(MINIMUM_SEGMENT_BYTES..=MAXIMUM_SEGMENT_BYTES).contains(&segment_bytes)
            || !(2..=MAXIMUM_SEGMENTS).contains(&maximum_segments)
            || retention.is_zero()
            || retention.as_nanos() > i64::MAX as u128
            || maximum_records == 0
            || maximum_records > 1_000_000
            || maximum_export_bytes == 0
            || maximum_export_bytes > MAXIMUM_EXPORT_BYTES
        {
            return Err(StructuredLogError::InvalidPolicy);
        }
        Ok(Self {
            segment_bytes,
            maximum_segments,
            retention,
            maximum_records,
            maximum_export_bytes,
        })
    }

    pub(super) fn retention_cutoff(
        self,
        observed_at: Timestamp,
    ) -> Result<Timestamp, StructuredLogError> {
        let nanos = i64::try_from(self.retention.as_nanos())
            .map_err(|_| StructuredLogError::InvalidPolicy)?;
        observed_at
            .checked_sub_nanos(nanos)
            .map_err(|_| StructuredLogError::InvalidTimestamp)
    }
}

impl Default for LogStoragePolicy {
    fn default() -> Self {
        Self {
            segment_bytes: 4 * 1024 * 1024,
            maximum_segments: 16,
            retention: Duration::from_secs(30 * 24 * 60 * 60),
            maximum_records: 100_000,
            maximum_export_bytes: 8 * 1024 * 1024,
        }
    }
}

/// One secret-free, bounded diagnostic event accepted for local persistence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StructuredLogEvent {
    pub(super) observed_at: Timestamp,
    pub(super) severity: LogSeverity,
    pub(super) domain: LogDomain,
    pub(super) operation: Option<String>,
    pub(super) source_id: Option<String>,
    pub(super) job_id: Option<String>,
    pub(super) correlation_id: Option<String>,
    pub(super) message: String,
    pub(super) fields: BTreeMap<String, String>,
}

impl StructuredLogEvent {
    /// Admits a diagnostic fact, redacting sensitive fields and rejecting unstructured secrets.
    #[allow(
        clippy::too_many_arguments,
        reason = "each indexed diagnostic dimension is explicit"
    )]
    pub fn try_new(
        observed_at: Timestamp,
        severity: LogSeverity,
        domain: LogDomain,
        operation: Option<String>,
        source_id: Option<String>,
        job_id: Option<String>,
        correlation_id: Option<String>,
        message: impl Into<String>,
        fields: BTreeMap<String, String>,
    ) -> Result<Self, StructuredLogError> {
        let operation = validate_optional_label(operation)?;
        let source_id = validate_optional_label(source_id)?;
        let job_id = validate_optional_label(job_id)?;
        let correlation_id = validate_optional_label(correlation_id)?;
        let message = message.into();
        if message.is_empty()
            || message.len() > MAXIMUM_MESSAGE_BYTES
            || message.chars().any(|character| character == '\0')
            || contains_sensitive_message_pattern(&message)
            || fields.len() > MAXIMUM_FIELD_COUNT
        {
            return Err(StructuredLogError::UnsafeRecord);
        }
        let fields = fields
            .into_iter()
            .map(|(name, value)| {
                if name.is_empty()
                    || name.len() > MAXIMUM_FIELD_NAME_BYTES
                    || name.chars().any(|character| {
                        !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.'))
                    })
                    || value.len() > MAXIMUM_FIELD_VALUE_BYTES
                    || value.chars().any(|character| character == '\0')
                {
                    return Err(StructuredLogError::UnsafeRecord);
                }
                Ok((
                    name.clone(),
                    if is_sensitive_field_name(&name) || contains_sensitive_message_pattern(&value)
                    {
                        "[REDACTED]".to_owned()
                    } else {
                        value
                    },
                ))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            observed_at,
            severity,
            domain,
            operation,
            source_id,
            job_id,
            correlation_id,
            message,
            fields,
        })
    }
}

/// Durable event paired with its monotonic local sequence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StructuredLogRecord {
    pub(super) sequence: u64,
    pub(super) event: StructuredLogEvent,
}

impl StructuredLogRecord {
    /// Returns the monotonic record sequence used for pagination.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the already-redacted event.
    #[must_use]
    pub const fn event(&self) -> &StructuredLogEvent {
        &self.event
    }
}

/// Validated bounded query over indexed structured dimensions.
#[derive(Clone, Debug)]
pub struct StructuredLogQuery {
    pub from: Option<Timestamp>,
    pub through: Option<Timestamp>,
    pub minimum_severity: Option<LogSeverity>,
    pub domain: Option<LogDomain>,
    pub source_id: Option<String>,
    pub job_id: Option<String>,
    pub correlation_id: Option<String>,
    pub search: Option<String>,
    pub after_sequence: Option<u64>,
    pub limit: usize,
}

impl StructuredLogQuery {
    pub(super) fn validate(&self) -> Result<(), StructuredLogError> {
        if self.limit == 0
            || self.limit > MAXIMUM_QUERY_LIMIT
            || self
                .from
                .zip(self.through)
                .is_some_and(|(from, through)| from > through)
            || [
                &self.source_id,
                &self.job_id,
                &self.correlation_id,
                &self.search,
            ]
            .into_iter()
            .flatten()
            .any(|value| {
                value.is_empty()
                    || value.len() > MAXIMUM_FILTER_BYTES
                    || value.chars().any(char::is_control)
            })
        {
            return Err(StructuredLogError::InvalidQuery);
        }
        Ok(())
    }
}

impl Default for StructuredLogQuery {
    fn default() -> Self {
        Self {
            from: None,
            through: None,
            minimum_severity: None,
            domain: None,
            source_id: None,
            job_id: None,
            correlation_id: None,
            search: None,
            after_sequence: None,
            limit: 250,
        }
    }
}

/// One bounded page in stable sequence order.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StructuredLogPage {
    pub(super) records: Vec<StructuredLogRecord>,
    pub(super) next_after_sequence: Option<u64>,
}

impl StructuredLogPage {
    /// Returns the already-redacted records.
    #[must_use]
    pub fn records(&self) -> &[StructuredLogRecord] {
        &self.records
    }
}

/// Exact controlled diagnostic artifact admission supplied by the artifact owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticArtifactAdmission {
    pub media_type: &'static str,
    pub bytes: Vec<u8>,
    pub sha256: [u8; 32],
    pub record_count: usize,
}

/// Path-free receipt returned after controlled artifact publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticArtifactReceipt {
    artifact_reference: String,
    pub(super) byte_length: u64,
    pub(super) sha256: [u8; 32],
}

impl DiagnosticArtifactReceipt {
    /// Creates a receipt that the store verifies against the exact admitted bytes.
    pub fn try_new(
        artifact_reference: impl Into<String>,
        byte_length: u64,
        sha256: [u8; 32],
    ) -> Result<Self, StructuredLogError> {
        let artifact_reference = artifact_reference.into();
        if artifact_reference.is_empty()
            || artifact_reference.len() > MAXIMUM_ARTIFACT_REFERENCE_BYTES
            || artifact_reference.starts_with('/')
            || artifact_reference.contains("..")
            || artifact_reference.contains('\\')
            || artifact_reference.chars().any(char::is_control)
        {
            return Err(StructuredLogError::InvalidArtifactReceipt);
        }
        Ok(Self {
            artifact_reference,
            byte_length,
            sha256,
        })
    }
}

/// Controlled artifact boundary; clients never receive arbitrary file authority.
#[async_trait]
pub trait DiagnosticArtifactPublisher: fmt::Debug + Send + Sync {
    /// Publishes exact bounded redacted bytes under the application's artifact root.
    async fn publish(
        &self,
        admission: DiagnosticArtifactAdmission,
        cancellation: CancellationToken,
        deadline: std::time::Instant,
    ) -> Result<DiagnosticArtifactReceipt, StructuredLogError>;
}

fn validate_optional_label(value: Option<String>) -> Result<Option<String>, StructuredLogError> {
    if value.as_ref().is_some_and(|value| {
        value.is_empty()
            || value.len() > MAXIMUM_FILTER_BYTES
            || value.chars().any(char::is_control)
            || contains_sensitive_message_pattern(value)
    }) {
        return Err(StructuredLogError::UnsafeRecord);
    }
    Ok(value)
}

fn is_sensitive_field_name(name: &str) -> bool {
    let normalized = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    [
        "authorization",
        "cookie",
        "credential",
        "password",
        "privatekey",
        "secret",
        "session",
        "token",
        "apikey",
    ]
    .into_iter()
    .any(|sensitive| normalized == sensitive || normalized.ends_with(sensitive))
}

fn contains_sensitive_message_pattern(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    if lower.contains("bearer ") {
        return true;
    }
    let compact = lower
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, ':' | '='))
        .collect::<String>();
    [
        "authorization",
        "cookie",
        "credential",
        "password",
        "privatekey",
        "secret",
        "session",
        "token",
        "apikey",
    ]
    .into_iter()
    .any(|sensitive| {
        compact.contains(&format!("{sensitive}:")) || compact.contains(&format!("{sensitive}="))
    })
}

/// Typed local-log failure without secret values or filesystem paths.
#[derive(Debug, Error)]
pub enum StructuredLogError {
    #[error("structured log policy is invalid")]
    InvalidPolicy,
    #[error("structured log timestamp cannot satisfy retention arithmetic")]
    InvalidTimestamp,
    #[error("structured log timestamp precedes the latest accepted event")]
    OutOfOrderTimestamp,
    #[error("structured log record is unsafe or exceeds an ingress bound")]
    UnsafeRecord,
    #[error("structured log record exceeds the durable record bound")]
    RecordTooLarge,
    #[error("structured log query is invalid")]
    InvalidQuery,
    #[error("structured log export exceeds its byte ceiling")]
    ExportTooLarge,
    #[error("structured log operation was cancelled")]
    Cancelled,
    #[error("structured log operation deadline elapsed")]
    DeadlineExceeded,
    #[error("structured log capacity is exhausted")]
    CapacityExceeded,
    #[error("structured log sequence is exhausted")]
    SequenceExhausted,
    #[error("structured log allocation failed")]
    Allocation,
    #[error("structured log store is corrupt")]
    CorruptStore,
    #[error("structured log store is unavailable")]
    Unavailable,
    #[error("structured log filesystem entry is unsafe")]
    UnsafeFilesystemEntry,
    #[error("structured log encoding failed")]
    Encoding,
    #[error("controlled diagnostic artifact receipt is invalid")]
    InvalidArtifactReceipt,
    #[error("prepared control root is unavailable")]
    ControlRoot(#[from] market_squawk_platform::PathError),
    #[error("structured log queue capacity is invalid")]
    InvalidQueueCapacity,
    #[error("structured log queue is full or disconnected")]
    QueueUnavailable,
    #[error("structured log flush or shutdown deadline elapsed")]
    DrainDeadlineElapsed,
    #[error("structured log worker failed to start or join")]
    WorkerUnavailable,
    #[error("structured log I/O failed")]
    Io { source: std::io::Error },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_structured_secrets_and_rejects_unstructured_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fields = BTreeMap::new();
        fields.insert("provider".to_owned(), "coinbase".to_owned());
        fields.insert("api_key".to_owned(), "must-not-persist".to_owned());
        fields.insert("accessToken".to_owned(), "must-not-persist".to_owned());
        fields.insert("clientSecret".to_owned(), "must-not-persist".to_owned());
        fields.insert(
            "response".to_owned(),
            "authorization: Bearer must-not-persist".to_owned(),
        );
        let event = StructuredLogEvent::try_new(
            Timestamp::from_unix_nanos(1_000_000_000),
            LogSeverity::Info,
            LogDomain::Source,
            Some("source_connect".to_owned()),
            Some("coinbase".to_owned()),
            None,
            Some("correlation-1".to_owned()),
            "source connected",
            fields,
        )?;
        assert_eq!(
            event.fields.get("api_key").map(String::as_str),
            Some("[REDACTED]")
        );
        assert_eq!(
            event.fields.get("accessToken").map(String::as_str),
            Some("[REDACTED]")
        );
        assert_eq!(
            event.fields.get("clientSecret").map(String::as_str),
            Some("[REDACTED]")
        );
        assert_eq!(
            event.fields.get("response").map(String::as_str),
            Some("[REDACTED]")
        );
        assert!(matches!(
            StructuredLogEvent::try_new(
                Timestamp::from_unix_nanos(1_000_000_001),
                LogSeverity::Error,
                LogDomain::Source,
                None,
                None,
                None,
                None,
                "authorization: Bearer must-not-persist",
                BTreeMap::new(),
            ),
            Err(StructuredLogError::UnsafeRecord)
        ));
        assert!(matches!(
            StructuredLogEvent::try_new(
                Timestamp::from_unix_nanos(1_000_000_002),
                LogSeverity::Error,
                LogDomain::Source,
                None,
                None,
                None,
                None,
                r#"provider response: {"refreshToken":"must-not-persist"}"#,
                BTreeMap::new(),
            ),
            Err(StructuredLogError::UnsafeRecord)
        ));
        assert!(matches!(
            StructuredLogEvent::try_new(
                Timestamp::from_unix_nanos(1_000_000_003),
                LogSeverity::Error,
                LogDomain::Source,
                None,
                Some("access_token=must-not-persist".to_owned()),
                None,
                None,
                "provider request failed",
                BTreeMap::new(),
            ),
            Err(StructuredLogError::UnsafeRecord)
        ));
        Ok(())
    }
}
