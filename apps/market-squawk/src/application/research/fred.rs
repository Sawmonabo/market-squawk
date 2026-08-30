//! Transport-neutral FRED/ALFRED point-in-time application operation.

use std::{
    fmt,
    sync::{Arc, RwLock},
    time::Instant,
};

use chrono::{DateTime, Datelike, Utc};
use market_squawk_data::{
    AnalyticalGeneration, AnalyticalMacroLatestKnownOutput, DatasetManifestRef,
};
use market_squawk_domain::{CalendarDate, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_sources::FRED_ALFRED_API_SURFACE_ID;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{encode_hex, map_query_error, map_read_error, parse_timestamp};
use crate::provider_activation::{
    FRED_ALFRED_READ_OPERATION, FredPointInTimeReadCapability, FredPointInTimeReadError,
};

const FRED_OPERATION_SCHEMA: &str = "market-squawk-fred-alfred-operation/v1";
const RESULT_LIMITS_FIELD: &str = "resultLimits";
const GENERATION_FIELD: &str = "generation";
const KNOWLEDGE_CUTOFF_FIELD: &str = "knowledgeCutoff";
const EFFECTIVE_DATE_CUTOFF_FIELD: &str = "effectiveDateCutoff";
const MAXIMUM_TIMESTAMP_BYTES: usize = 64;
const MAXIMUM_SCHEMA_NAME_BYTES: usize = 256;

/// Availability of the exact desired FRED/ALFRED read binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FredLatestKnownAvailability {
    /// No durable desired FRED/ALFRED activation exists for this workspace.
    SetupRequired,
    /// Desired activation exists, but no exact immutable analytical manifest is bound.
    Unavailable,
    /// Desired activation and one exact immutable analytical manifest are bound.
    Ready,
}

/// One code-bound `Macro.GetFredAlfredLatestKnown` application operation.
///
/// The ready state retains the immutable manifest selected by application composition. Calls may
/// echo that exact generation but cannot select a provider, source, series, dataset, schema, or
/// physical path. Invoking this value can query only the local analytical reader already held by
/// [`FredPointInTimeReadCapability`]; it owns no provider, credential, acquisition, or publication
/// authority.
pub(crate) struct FredLatestKnownOperation {
    state: Arc<RwLock<Arc<FredLatestKnownState>>>,
}

enum FredLatestKnownState {
    SetupRequired,
    Unavailable {
        capability: FredPointInTimeReadCapability,
    },
    Ready {
        capability: FredPointInTimeReadCapability,
        manifest: DatasetManifestRef,
    },
}

impl FredLatestKnownOperation {
    /// Creates the truthful state used when no durable desired activation exists.
    #[must_use]
    pub(crate) fn setup_required() -> Self {
        Self {
            state: Arc::new(RwLock::new(Arc::new(FredLatestKnownState::SetupRequired))),
        }
    }

    /// Binds a desired activation to its exact immutable analytical generation, when present.
    ///
    /// `generation` must come from application composition or restart restoration. Its complete
    /// retained source and dataset identity are validated before this operation freezes its exact
    /// manifest. It is never reconstructed from request arguments. `None` preserves an explicit
    /// unavailable state instead of silently selecting a later generation.
    ///
    /// # Errors
    ///
    /// Rejects a generation owned by another source or analytical dataset, or a reserved digest
    /// identity.
    pub(crate) fn try_from_desired_activation(
        capability: FredPointInTimeReadCapability,
        generation: Option<AnalyticalGeneration>,
    ) -> Result<Self, FredLatestKnownCompositionError> {
        let state = match generation {
            Some(generation) => {
                let manifest = capability
                    .try_pin_generation(&generation)
                    .map_err(|_| FredLatestKnownCompositionError::InvalidManifestBinding)?;
                if manifest.content_hash().bytes() == [0; 32]
                    || manifest.schema().fingerprint() == [0; 32]
                {
                    return Err(FredLatestKnownCompositionError::InvalidManifestBinding);
                }
                FredLatestKnownState::Ready {
                    capability,
                    manifest,
                }
            }
            None => FredLatestKnownState::Unavailable { capability },
        };
        Ok(Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
        })
    }

    /// Returns the application-owned availability state without provider or filesystem access.
    #[must_use]
    pub(crate) fn availability(&self) -> FredLatestKnownAvailability {
        let Ok(state) = self.state.read() else {
            return FredLatestKnownAvailability::Unavailable;
        };
        match state.as_ref() {
            FredLatestKnownState::SetupRequired => FredLatestKnownAvailability::SetupRequired,
            FredLatestKnownState::Unavailable { .. } => FredLatestKnownAvailability::Unavailable,
            FredLatestKnownState::Ready { .. } => FredLatestKnownAvailability::Ready,
        }
    }

    /// Executes one availability or exact-generation point-in-time request.
    ///
    /// Omitting all three read fields returns bounded availability. Supplying all three reads the
    /// exact manifest retained by this operation. Partial read arguments, a stale generation echo,
    /// future knowledge, or an effective cutoff after the knowledge date fail closed.
    pub(crate) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(request, context)?;
        let invocation = parse_invocation(request.arguments())?;
        let state = self
            .state
            .read()
            .map_err(|_| ServiceError::Unavailable)?
            .clone();
        match state.as_ref() {
            FredLatestKnownState::SetupRequired => setup_required_result(limits),
            FredLatestKnownState::Unavailable { capability } => {
                unavailable_result(capability, limits)
            }
            FredLatestKnownState::Ready {
                capability,
                manifest,
            } => match invocation {
                FredLatestKnownInvocation::Availability => {
                    ready_availability_result(capability, manifest, limits)
                }
                FredLatestKnownInvocation::Read(arguments) => {
                    read_result(capability, manifest, arguments, context, limits).await
                }
            },
        }
    }

    /// Returns the current exact FRED Macro selection for neutral application composition.
    ///
    /// Setup-required and configured-unavailable states return `None`. Ready reads remain pinned
    /// to the installed manifest and return the typed analytical output directly, without
    /// provider DTO serialization or JSON reparsing.
    pub(crate) async fn read_current_analytical_latest_known(
        &self,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<AnalyticalMacroLatestKnownOutput>, ServiceError> {
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        let evaluated_at = evaluated_at()?;
        if knowledge_cutoff > evaluated_at
            || effective_date_cutoff > timestamp_calendar_date(knowledge_cutoff)?
        {
            return Err(ServiceError::InvalidRequest);
        }
        let state = self
            .state
            .read()
            .map_err(|_| ServiceError::Unavailable)?
            .clone();
        let FredLatestKnownState::Ready {
            capability,
            manifest,
        } = state.as_ref()
        else {
            return Ok(None);
        };
        capability
            .read_analytical_latest_known(
                manifest.clone(),
                knowledge_cutoff,
                effective_date_cutoff,
                deadline,
                cancellation,
            )
            .await
            .map(Some)
            .map_err(map_fred_read_error)
    }
}

impl fmt::Debug for FredLatestKnownOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FredLatestKnownOperation")
            .field("operation", &FRED_ALFRED_READ_OPERATION)
            .field("availability", &self.availability())
            .finish_non_exhaustive()
    }
}

impl Clone for FredLatestKnownOperation {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

enum FredLatestKnownInvocation {
    Availability,
    Read(FredLatestKnownReadArguments),
}

struct FredLatestKnownReadArguments {
    generation: FredGenerationSelector,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
}

struct FredGenerationSelector {
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: [u8; 32],
    content_hash: [u8; 32],
}

fn ensure_request_live(
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<(), ServiceError> {
    if request.name() != FRED_ALFRED_READ_OPERATION {
        return Err(ServiceError::NotFound);
    }
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

fn parse_invocation(
    arguments: &Map<String, Value>,
) -> Result<FredLatestKnownInvocation, ServiceError> {
    if arguments.keys().any(|field| {
        !matches!(
            field.as_str(),
            GENERATION_FIELD
                | KNOWLEDGE_CUTOFF_FIELD
                | EFFECTIVE_DATE_CUTOFF_FIELD
                | RESULT_LIMITS_FIELD
        )
    }) {
        return Err(ServiceError::InvalidRequest);
    }
    let generation = arguments.get(GENERATION_FIELD);
    let knowledge_cutoff = arguments.get(KNOWLEDGE_CUTOFF_FIELD);
    let effective_date_cutoff = arguments.get(EFFECTIVE_DATE_CUTOFF_FIELD);
    match (generation, knowledge_cutoff, effective_date_cutoff) {
        (None, None, None) => Ok(FredLatestKnownInvocation::Availability),
        (Some(generation), Some(knowledge_cutoff), Some(effective_date_cutoff)) => {
            let knowledge_cutoff = knowledge_cutoff
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= MAXIMUM_TIMESTAMP_BYTES)
                .ok_or(ServiceError::InvalidRequest)
                .and_then(parse_timestamp)?;
            let effective_date_cutoff = effective_date_cutoff
                .as_str()
                .ok_or(ServiceError::InvalidRequest)
                .and_then(parse_calendar_date)?;
            if effective_date_cutoff > timestamp_calendar_date(knowledge_cutoff)? {
                return Err(ServiceError::InvalidRequest);
            }
            Ok(FredLatestKnownInvocation::Read(
                FredLatestKnownReadArguments {
                    generation: parse_generation_selector(generation)?,
                    knowledge_cutoff,
                    effective_date_cutoff,
                },
            ))
        }
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn parse_generation_selector(value: &Value) -> Result<FredGenerationSelector, ServiceError> {
    let fields = value.as_object().ok_or(ServiceError::InvalidRequest)?;
    if fields.len() != 3
        || !fields.contains_key("manifestVersion")
        || !fields.contains_key("schema")
        || !fields.contains_key("contentHash")
    {
        return Err(ServiceError::InvalidRequest);
    }
    let manifest_version = fields["manifestVersion"]
        .as_str()
        .filter(|value| valid_positive_integer_text(value))
        .ok_or(ServiceError::InvalidRequest)?
        .parse::<u64>()
        .map_err(|_| ServiceError::InvalidRequest)?;
    let schema = fields["schema"]
        .as_object()
        .ok_or(ServiceError::InvalidRequest)?;
    if schema.len() != 3
        || !schema.contains_key("name")
        || !schema.contains_key("version")
        || !schema.contains_key("fingerprint")
    {
        return Err(ServiceError::InvalidRequest);
    }
    let schema_name = schema["name"]
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= MAXIMUM_SCHEMA_NAME_BYTES)
        .ok_or(ServiceError::InvalidRequest)?
        .to_owned();
    let schema_version = schema["version"]
        .as_u64()
        .filter(|version| *version > 0)
        .and_then(|version| u16::try_from(version).ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let schema_fingerprint = schema["fingerprint"]
        .as_str()
        .ok_or(ServiceError::InvalidRequest)
        .and_then(parse_sha256)?;
    let content_hash = fields["contentHash"]
        .as_str()
        .ok_or(ServiceError::InvalidRequest)
        .and_then(parse_sha256)?;
    Ok(FredGenerationSelector {
        manifest_version,
        schema_name,
        schema_version,
        schema_fingerprint,
        content_hash,
    })
}

fn valid_positive_integer_text(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 20
        && !value.starts_with('0')
        && value.bytes().all(|byte| byte.is_ascii_digit())
}

async fn read_result(
    capability: &FredPointInTimeReadCapability,
    manifest: &DatasetManifestRef,
    arguments: FredLatestKnownReadArguments,
    context: &RequestContext,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    if !generation_matches(&arguments.generation, manifest) {
        return Err(ServiceError::NotFound);
    }
    let evaluated_at = evaluated_at()?;
    if arguments.knowledge_cutoff > evaluated_at {
        return Err(ServiceError::InvalidRequest);
    }
    let read = capability
        .read_latest_known(
            manifest.clone(),
            arguments.knowledge_cutoff,
            arguments.effective_date_cutoff,
            evaluated_at,
            context.deadline(),
            context.cancellation().clone(),
        )
        .await
        .map_err(map_fred_read_error)?;
    let read = serde_json::to_value(read).map_err(|_| ServiceError::InvalidResult)?;
    let (binding, selection) = point_in_time_evidence(&read)?;
    let generation = generation_selector_value(manifest);
    let content = json!({
        "schemaIdentity": FRED_OPERATION_SCHEMA,
        "operation": FRED_ALFRED_READ_OPERATION,
        "state": "ready",
        "generation": generation,
        "result": read,
    });
    let coverage = json!({
        "operation": FRED_ALFRED_READ_OPERATION,
        "binding": binding,
        "selection": selection,
    });
    let quality = json!({
        "classification": "official_delayed_point_in_time",
        "recordLevelProvenance": true,
        "manifestPinned": true,
        "selectionComplete": true,
        "executionEligible": false,
        "executionEligibility": "research_only_execution_ineligible",
    });
    typed_result(content, 1, coverage, quality, limits)
}

fn setup_required_result(limits: ServiceLimits) -> Result<TypedToolResult, ServiceError> {
    let content = json!({
        "schemaIdentity": FRED_OPERATION_SCHEMA,
        "operation": FRED_ALFRED_READ_OPERATION,
        "state": "setup_required",
        "reason": "desired_activation_absent",
    });
    let coverage = json!({
        "operation": FRED_ALFRED_READ_OPERATION,
        "surfaceId": FRED_ALFRED_API_SURFACE_ID,
        "state": "setup_required",
        "configured": false,
    });
    let quality = unavailable_quality("desired_activation_absent");
    typed_result(content, 0, coverage, quality, limits)
}

fn unavailable_result(
    capability: &FredPointInTimeReadCapability,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let binding = provider_binding(capability);
    let content = json!({
        "schemaIdentity": FRED_OPERATION_SCHEMA,
        "operation": FRED_ALFRED_READ_OPERATION,
        "state": "unavailable",
        "reason": "exact_manifest_absent",
        "binding": binding,
    });
    let coverage = json!({
        "operation": FRED_ALFRED_READ_OPERATION,
        "state": "unavailable",
        "binding": provider_binding(capability),
        "manifestState": "absent",
    });
    let quality = unavailable_quality("exact_manifest_absent");
    typed_result(content, 0, coverage, quality, limits)
}

fn ready_availability_result(
    capability: &FredPointInTimeReadCapability,
    manifest: &DatasetManifestRef,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let generation = generation_selector_value(manifest);
    let content = json!({
        "schemaIdentity": FRED_OPERATION_SCHEMA,
        "operation": FRED_ALFRED_READ_OPERATION,
        "state": "ready",
        "binding": provider_binding(capability),
        "generation": generation,
    });
    let coverage = json!({
        "operation": FRED_ALFRED_READ_OPERATION,
        "state": "ready",
        "binding": provider_binding(capability),
        "generation": generation_selector_value(manifest),
    });
    let quality = json!({
        "classification": "manifest_bound_not_read",
        "manifestPinned": true,
        "executionEligible": false,
        "executionEligibility": "research_only_execution_ineligible",
    });
    typed_result(content, 0, coverage, quality, limits)
}

fn typed_result(
    content: Value,
    items: usize,
    coverage: Value,
    quality: Value,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let metadata = ToolResultMetadata::try_complete(coverage, quality)
        .map_err(|_| ServiceError::InvalidResult)?;
    TypedToolResult::try_new(content, items, metadata, limits).map_err(Into::into)
}

fn unavailable_quality(reason: &'static str) -> Value {
    json!({
        "classification": "unavailable",
        "reason": reason,
        "manifestPinned": false,
        "executionEligible": false,
    })
}

fn provider_binding(capability: &FredPointInTimeReadCapability) -> Value {
    json!({
        "surfaceId": FRED_ALFRED_API_SURFACE_ID,
        "providerDatasetId": capability.provider_dataset().as_str(),
        "analyticalDatasetId": capability.analytical_dataset().as_str(),
    })
}

fn generation_selector_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "manifestVersion": manifest.manifest_version().to_string(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema().version().get(),
            "fingerprint": encode_hex(manifest.schema().fingerprint()),
        },
        "contentHash": encode_hex(manifest.content_hash().bytes()),
    })
}

fn generation_matches(selector: &FredGenerationSelector, manifest: &DatasetManifestRef) -> bool {
    selector.manifest_version == manifest.manifest_version()
        && selector.schema_name == manifest.schema().name()
        && selector.schema_version == manifest.schema().version().get()
        && selector.schema_fingerprint == manifest.schema().fingerprint()
        && selector.content_hash == manifest.content_hash().bytes()
}

fn point_in_time_evidence(read: &Value) -> Result<(Value, Value), ServiceError> {
    let read = read.as_object().ok_or(ServiceError::InvalidResult)?;
    let binding = read
        .get("binding")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or(ServiceError::InvalidResult)?;
    let selection = read
        .get("selection")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or(ServiceError::InvalidResult)?;
    Ok((binding, selection))
}

fn evaluated_at() -> Result<Timestamp, ServiceError> {
    Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Unavailable)
}

fn parse_calendar_date(value: &str) -> Result<CalendarDate, ServiceError> {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(ServiceError::InvalidRequest);
    }
    let year = parse_date_component(&bytes[0..4])?;
    let month = u8::try_from(parse_date_component(&bytes[5..7])?)
        .map_err(|_| ServiceError::InvalidRequest)?;
    let day = u8::try_from(parse_date_component(&bytes[8..10])?)
        .map_err(|_| ServiceError::InvalidRequest)?;
    CalendarDate::new(year, month, day).map_err(|_| ServiceError::InvalidRequest)
}

fn parse_date_component(bytes: &[u8]) -> Result<u16, ServiceError> {
    bytes.iter().try_fold(0_u16, |value, byte| {
        let digit = byte
            .checked_sub(b'0')
            .filter(|digit| *digit <= 9)
            .ok_or(ServiceError::InvalidRequest)?;
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u16::from(digit)))
            .ok_or(ServiceError::InvalidRequest)
    })
}

fn timestamp_calendar_date(timestamp: Timestamp) -> Result<CalendarDate, ServiceError> {
    let date = DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos()).date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| ServiceError::InvalidRequest)?,
        u8::try_from(date.month()).map_err(|_| ServiceError::InvalidRequest)?,
        u8::try_from(date.day()).map_err(|_| ServiceError::InvalidRequest)?,
    )
    .map_err(|_| ServiceError::InvalidRequest)
}

fn parse_sha256(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64 {
        return Err(ServiceError::InvalidRequest);
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        digest[index] = hex_nibble(pair[0])?
            .checked_mul(16)
            .and_then(|high| high.checked_add(hex_nibble(pair[1]).ok()?))
            .ok_or(ServiceError::InvalidRequest)?;
    }
    if digest == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(digest)
}

fn hex_nibble(value: u8) -> Result<u8, ServiceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn map_fred_read_error(error: FredPointInTimeReadError) -> ServiceError {
    match error {
        FredPointInTimeReadError::InvalidBinding => ServiceError::Unavailable,
        FredPointInTimeReadError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        FredPointInTimeReadError::Cancelled => ServiceError::Cancelled,
        FredPointInTimeReadError::Query(error) => map_query_error(error),
        FredPointInTimeReadError::Analytical(error) => map_read_error(error),
        FredPointInTimeReadError::InvalidReadResult => ServiceError::InvalidResult,
    }
}

/// Invalid application composition for the fixed FRED/ALFRED operation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum FredLatestKnownCompositionError {
    /// Desired provider state and the retained immutable analytical manifest disagree.
    #[error("FRED/ALFRED desired activation and analytical manifest do not match")]
    InvalidManifestBinding,
}
