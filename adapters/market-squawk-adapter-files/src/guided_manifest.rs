//! Validated schema-v5 manifests for guided controlled-file imports.

use std::fmt;
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::Instant;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, ResearchTemporalCoordinate, SourceIdentifier,
    Timestamp,
};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::clock::{ExtractionClock, RequestDeadline, SystemExtractionClock};
use crate::contracts::{ParseBudget, SourceRowLimit};
use crate::manifest::FileSourceManifest;
use crate::parse::parse_rows;
use crate::source::validate_mapped_rows;
use crate::{ExtractionLimits, FileAdapterError, FilePreviewFormat, ParserLimit};

const MAXIMUM_SOURCE_COLUMN_BYTES: usize = 256;
const MAXIMUM_OBJECT_PATH_DEPTH: usize = 64;
const MAXIMUM_OBJECT_PATH_COMPONENT_BYTES: usize = 255;

/// Exact discovery-object time coordinates for one guided manifest object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuidedObjectTime {
    /// Inclusive effective instant for discovery selection.
    pub effective_at: Timestamp,
    /// Optional exact publication instant.
    pub published_at: Option<Timestamp>,
    /// Optional exclusive successor instant.
    pub superseded_at: Option<Timestamp>,
}

/// Exact record-time coordinates used when a row does not supply an override field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuidedRecordTimeFallback {
    /// Required effective coordinate.
    pub effective: ResearchTemporalCoordinate,
    /// Optional publication coordinate.
    pub published: Option<ResearchTemporalCoordinate>,
    /// Optional supersession coordinate.
    pub superseded: Option<ResearchTemporalCoordinate>,
}

/// Explicit instrument scope for one guided object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuidedInstrumentBinding {
    /// The imported observations are deliberately not scoped to one internal instrument.
    Unscoped,
    /// Every mapped observation belongs to one exact internal instrument.
    InternalInstrument {
        /// Stable internal instrument identity.
        instrument_id: InstrumentId,
    },
}

/// Explicit source-authored universe choice for one guided object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuidedUniverseBinding {
    /// The object supplies no historical-universe membership assertion.
    None,
    /// The object asserts one exact instrument membership interval.
    Membership {
        /// Stable source-authored universe identity.
        universe: SourceIdentifier,
        /// Inclusive membership start.
        starts_at: Timestamp,
        /// Optional exclusive membership end.
        ends_at: Option<Timestamp>,
    },
}

/// One exact numeric value mapping from a source column to a canonical output field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuidedValueMapping {
    /// Exact bounded UTF-8 source column name.
    pub source_field: String,
    /// Canonical output field identity.
    pub output_field: SourceIdentifier,
    /// Required exact decimal scale.
    pub decimal_scale: u32,
    /// Optional canonical unit identity.
    pub unit: Option<SourceIdentifier>,
}

/// Optional per-row temporal and revision source-column mappings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuidedRowTimeMapping {
    /// Exact source column containing signed Unix nanoseconds for effective time.
    pub effective_field: Option<String>,
    /// Exact source column containing signed Unix nanoseconds for publication time.
    pub published_field: Option<String>,
    /// Exact source column containing signed Unix nanoseconds for availability time.
    pub available_field: Option<String>,
    /// Exact source column containing a canonical revision identity.
    pub revision_field: Option<String>,
    /// Exact source column containing a nonzero decimal revision number.
    pub revision_number_field: Option<String>,
    /// Exact source column containing signed Unix nanoseconds for supersession time.
    pub superseded_field: Option<String>,
}

/// Complete caller input for one object in a generated guided manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuidedManifestInput {
    /// Canonical source dataset identity.
    pub dataset: SourceIdentifier,
    /// Canonical object identity unique within the dataset.
    pub object_id: SourceIdentifier,
    /// Portable relative path beneath the committed controlled-import root.
    pub object_path: String,
    /// Exact guided parser format.
    pub format: FilePreviewFormat,
    /// Discovery-object effective, publication, and supersession times.
    pub object_time: GuidedObjectTime,
    /// Object-level revision identity used as the row fallback.
    pub revision: SourceIdentifier,
    /// Nonzero object-level revision number used as the row fallback.
    pub revision_number: u32,
    /// Exact record-time fallbacks independent from discovery-object time.
    pub record_time: GuidedRecordTimeFallback,
    /// Explicit instrument scope.
    pub instrument_binding: GuidedInstrumentBinding,
    /// Explicit universe-membership choice.
    pub universe_binding: GuidedUniverseBinding,
    /// Exact source column that uniquely identifies every row.
    pub identity_field: String,
    /// Nonempty numeric value mappings.
    pub value_mappings: Vec<GuidedValueMapping>,
    /// Optional exact per-row temporal and revision source-column mappings.
    pub row_time_mapping: Option<GuidedRowTimeMapping>,
}

/// One guided manifest object paired with the exact bytes it declares.
pub struct GuidedManifestObject<'a> {
    /// Complete manifest declaration for this object.
    pub input: GuidedManifestInput,
    /// Exact bounded staged source bytes validated through the production parser.
    pub source_bytes: &'a [u8],
}

impl fmt::Debug for GuidedManifestObject<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GuidedManifestObject")
            .field("input", &self.input)
            .field(
                "source_bytes",
                &format_args!("[{} BYTES]", self.source_bytes.len()),
            )
            .finish()
    }
}

/// Exact validated schema-v5 manifest bytes and their SHA-256 digest.
#[derive(Clone, Eq, PartialEq)]
pub struct GuidedManifest {
    bytes: Box<[u8]>,
    digest: EvidenceDigest,
}

impl GuidedManifest {
    /// Returns the exact generated manifest bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the SHA-256 digest of the exact generated manifest bytes.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Consumes the result into its exact generated manifest bytes.
    pub fn into_bytes(self) -> Box<[u8]> {
        self.bytes
    }
}

impl fmt::Debug for GuidedManifest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GuidedManifest")
            .field("bytes", &format_args!("[{} BYTES]", self.bytes.len()))
            .field("digest", &self.digest)
            .finish()
    }
}

/// Builds one exact schema-v5 guided manifest object through the production validation path.
///
/// # Errors
///
/// Rejects invalid manifest declarations, malformed or empty source content, missing mapped
/// columns, duplicate row identities, invalid numeric/time/revision values, resource-limit
/// breaches, cancellation, or deadline expiry.
pub fn build_guided_manifest(
    input: GuidedManifestInput,
    source_bytes: &[u8],
    extraction_limits: ExtractionLimits,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<GuidedManifest, FileAdapterError> {
    build_guided_manifest_collection(
        vec![GuidedManifestObject {
            input,
            source_bytes,
        }],
        extraction_limits,
        deadline,
        cancellation,
    )
}

/// Builds one aggregate schema-v5 manifest for every committed controlled import object.
///
/// Every object is reparsed from its exact associated byte slice through the same production
/// parser, identity/value mapping, and row temporal/revision logic used by extraction. Dataset and
/// object pairs and controlled relative paths must each be unique. The existing manifest object,
/// mapping, byte, nesting, and retained-memory ceilings remain authoritative.
///
/// # Errors
///
/// Rejects an empty or excessive collection, duplicate identities or paths, any invalid object or
/// exact source, resource-limit breaches, cancellation, or deadline expiry.
pub fn build_guided_manifest_collection(
    inputs: Vec<GuidedManifestObject<'_>>,
    extraction_limits: ExtractionLimits,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<GuidedManifest, FileAdapterError> {
    if inputs.is_empty() || inputs.len() > extraction_limits.input.max_manifest_objects {
        return Err(FileAdapterError::LimitExceeded(
            ParserLimit::ManifestObjects,
        ));
    }
    let total_mappings = inputs.iter().try_fold(0_usize, |total, object| {
        total.checked_add(object.input.value_mappings.len())
    });
    if total_mappings.is_none_or(|total| total > extraction_limits.input.max_manifest_mappings) {
        return Err(FileAdapterError::LimitExceeded(
            ParserLimit::ManifestMappings,
        ));
    }
    let admission_expiry = Instant::now()
        .checked_add(extraction_limits.input.max_elapsed)
        .ok_or(FileAdapterError::ClockFailure)?;
    let clock: Arc<dyn ExtractionClock> = Arc::new(SystemExtractionClock);
    let sealed = RequestDeadline::seal(clock.as_ref(), deadline, admission_expiry)?;
    let mut objects = Vec::new();
    objects
        .try_reserve_exact(inputs.len())
        .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::ManifestRetainedBytes))?;
    for (index, object) in inputs.iter().enumerate() {
        sealed.checkpoint(clock.as_ref())?;
        if inputs[..index].iter().any(|previous| {
            (previous.input.dataset == object.input.dataset
                && previous.input.object_id == object.input.object_id)
                || previous.input.object_path == object.input.object_path
        }) {
            return Err(FileAdapterError::DuplicateField);
        }
        validate_guided_input(&object.input)?;
        objects.push(guided_object_value(&object.input));
    }
    let bytes = serde_json::to_vec(&json!({
        "schema_version": 5,
        "objects": objects,
    }))
    .map_err(|_| FileAdapterError::InvalidManifest)?;
    let manifest = FileSourceManifest::parse(&bytes, extraction_limits)?;
    manifest.validate()?;
    if manifest.objects.len() != inputs.len() {
        return Err(FileAdapterError::InvalidManifest);
    }
    for (specification, object) in manifest.objects.iter().zip(&inputs) {
        sealed.checkpoint(clock.as_ref())?;
        let source_bytes = u64::try_from(object.source_bytes.len())
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::SourceBytes))?;
        if object.source_bytes.is_empty() || source_bytes > extraction_limits.source_bytes() {
            return Err(if source_bytes > extraction_limits.source_bytes() {
                FileAdapterError::LimitExceeded(ParserLimit::SourceBytes)
            } else {
                FileAdapterError::InvalidRecord
            });
        }
        let mut budget = ParseBudget::new(
            extraction_limits,
            cancellation,
            Arc::clone(&clock),
            sealed,
            SourceRowLimit::from_adapter_limit(extraction_limits.input.max_records),
        );
        let rows = parse_rows(&specification.format, object.source_bytes, &mut budget)?;
        if rows.is_empty() {
            return Err(FileAdapterError::InvalidRecord);
        }
        validate_mapped_rows(specification, &rows, &mut budget)?;
    }
    sealed.checkpoint(clock.as_ref())?;
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&bytes).into());
    Ok(GuidedManifest {
        bytes: bytes.into_boxed_slice(),
        digest,
    })
}

fn validate_guided_input(input: &GuidedManifestInput) -> Result<(), FileAdapterError> {
    if !valid_object_path(&input.object_path)
        || !valid_source_column(&input.identity_field)
        || input.value_mappings.is_empty()
        || input
            .value_mappings
            .iter()
            .any(|mapping| !valid_source_column(&mapping.source_field))
        || input.row_time_mapping.as_ref().is_some_and(|mapping| {
            let fields = [
                mapping.effective_field.as_deref(),
                mapping.published_field.as_deref(),
                mapping.available_field.as_deref(),
                mapping.revision_field.as_deref(),
                mapping.revision_number_field.as_deref(),
                mapping.superseded_field.as_deref(),
            ];
            fields.iter().all(Option::is_none)
                || fields
                    .into_iter()
                    .flatten()
                    .any(|field| !valid_source_column(field))
        })
    {
        return Err(FileAdapterError::InvalidManifest);
    }
    Ok(())
}

fn valid_source_column(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAXIMUM_SOURCE_COLUMN_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_object_path(value: &str) -> bool {
    if value.is_empty()
        || value.contains('\\')
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains("//")
    {
        return false;
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return false;
    }
    let mut depth = 0_usize;
    for component in path.components() {
        let Component::Normal(component) = component else {
            return false;
        };
        let bytes = component.as_encoded_bytes();
        if bytes.is_empty() || bytes.len() > MAXIMUM_OBJECT_PATH_COMPONENT_BYTES {
            return false;
        }
        depth = depth.saturating_add(1);
        if depth > MAXIMUM_OBJECT_PATH_DEPTH {
            return false;
        }
    }
    depth != 0
}

fn guided_object_value(input: &GuidedManifestInput) -> Value {
    let format = match input.format {
        FilePreviewFormat::Csv { delimiter } => json!({
            "kind": "csv",
            "delimiter": delimiter,
        }),
        FilePreviewFormat::Json => json!({"kind": "json"}),
        FilePreviewFormat::Ndjson => json!({"kind": "ndjson"}),
        FilePreviewFormat::Parquet => json!({"kind": "parquet"}),
    };
    let instrument_binding = match input.instrument_binding {
        GuidedInstrumentBinding::Unscoped => json!({"kind": "unscoped"}),
        GuidedInstrumentBinding::InternalInstrument { instrument_id } => json!({
            "kind": "internal_instrument",
            "instrument_id": instrument_id,
        }),
    };
    let value_mappings: Vec<_> = input
        .value_mappings
        .iter()
        .map(|mapping| {
            json!({
                "source": mapping.source_field,
                "field": mapping.output_field,
                "decimal_scale": mapping.decimal_scale,
                "unit": mapping.unit,
            })
        })
        .collect();
    let mut object = Map::new();
    object.insert("dataset".to_owned(), json!(input.dataset));
    object.insert("object_id".to_owned(), json!(input.object_id));
    object.insert("path".to_owned(), json!(input.object_path));
    object.insert("format".to_owned(), format);
    object.insert(
        "effective_at".to_owned(),
        json!(input.object_time.effective_at),
    );
    object.insert(
        "published_at".to_owned(),
        json!(input.object_time.published_at),
    );
    object.insert("revision".to_owned(), json!(input.revision));
    object.insert("revision_number".to_owned(), json!(input.revision_number));
    object.insert(
        "superseded_at".to_owned(),
        json!(input.object_time.superseded_at),
    );
    object.insert(
        "record_time".to_owned(),
        json!({
            "effective": input.record_time.effective,
            "published": input.record_time.published,
            "superseded": input.record_time.superseded,
        }),
    );
    if let Some(row_time) = &input.row_time_mapping {
        object.insert(
            "row_time".to_owned(),
            json!({
                "effective_field": row_time.effective_field,
                "published_field": row_time.published_field,
                "available_field": row_time.available_field,
                "revision_field": row_time.revision_field,
                "revision_number_field": row_time.revision_number_field,
                "superseded_field": row_time.superseded_field,
            }),
        );
    }
    object.insert("instrument_binding".to_owned(), instrument_binding);
    if let GuidedUniverseBinding::Membership {
        universe,
        starts_at,
        ends_at,
    } = &input.universe_binding
    {
        object.insert(
            "universe_membership".to_owned(),
            json!({
                "universe": universe,
                "starts_at": starts_at,
                "ends_at": ends_at,
            }),
        );
    }
    object.insert(
        "row_policy".to_owned(),
        json!({
            "identity_field": input.identity_field,
            "fields": value_mappings,
        }),
    );
    Value::Object(object)
}
