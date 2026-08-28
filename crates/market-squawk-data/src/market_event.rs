//! Typed canonical market-event Arrow/Parquet publication and restart validation.

use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray, TimestampNanosecondArray,
    UInt16Array, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, LiveProvenance, MarketEvent};
use market_squawk_sources::{
    ProviderMarketEventBatch, SealedProviderEventMicrobatchBinding,
    SealedProviderPublicationBinding, SealedProviderResponseMarketEventBinding,
};
use sha2::{Digest as _, Sha256};

use crate::arrow_convert::{ArrowConversionError, DatasetArrowBatch};
use crate::catalog::{
    PersistedProviderEventBindingEvidence, PersistedProviderEventBindingRow,
    PersistedProviderPublicationEvidence, PersistedProviderResponseMarketEventBindingEvidence,
    PersistedProviderResponseMarketEventBindingRow,
};
use crate::schema::{
    DatasetSchemaRef, DatasetSchemaRegistry, MARKET_EVENT_SCHEMA_VERSION,
    PROVIDER_PUBLICATION_DIGEST_KEY, PROVIDER_PUBLICATION_KIND_KEY, decode_hex,
    market_event_schema,
};

const RESPONSE_KIND: &str = "response_market_event";
const EVENT_KIND: &str = "event_microbatch";
const COMPOSITE_KIND: &str = "composite_response_event";
const MAX_EVENT_RESTART_BYTES: usize = 128 * 1024 * 1024;

/// Registered immutable typed events ready for the common Parquet object store.
#[derive(Clone, Debug)]
pub struct ProviderMarketEventArrowBatch {
    dataset: DatasetArrowBatch,
    events: Box<[MarketEvent]>,
    publication_digest: EvidenceDigest,
    publication_kind: Box<str>,
}

impl ProviderMarketEventArrowBatch {
    /// Projects a closed typed event publication without converting it to research rows.
    pub(crate) fn try_from_publication(
        binding: &SealedProviderPublicationBinding,
    ) -> Result<Self, ArrowConversionError> {
        let (dataset, digest, kind, rows) = match binding {
            SealedProviderPublicationBinding::ResponseSet(_) => {
                return Err(ArrowConversionError::InvalidMarketEventRow);
            }
            SealedProviderPublicationBinding::ResponseMarketEvent(response) => {
                response
                    .validate()
                    .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?;
                (
                    response.batch().dataset(),
                    response.evidence_digest().evidence(),
                    RESPONSE_KIND,
                    response_rows(response)?,
                )
            }
            SealedProviderPublicationBinding::EventMicrobatch(event) => {
                event
                    .validate()
                    .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?;
                (
                    event.batch().dataset(),
                    event.evidence_digest().evidence(),
                    EVENT_KIND,
                    event_rows(event)?,
                )
            }
            SealedProviderPublicationBinding::CompositeResponseEvent(composite) => {
                composite
                    .response()
                    .validate()
                    .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?;
                composite
                    .event()
                    .validate()
                    .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?;
                let mut rows = response_rows(composite.response())?;
                rows.try_reserve_exact(composite.event().record_count())
                    .map_err(|_| ArrowConversionError::AllocationFailure)?;
                rows.extend(event_rows(composite.event())?);
                (
                    composite.response().batch().dataset(),
                    composite.evidence_digest().evidence(),
                    COMPOSITE_KIND,
                    rows,
                )
            }
        };
        build_event_batch(dataset, digest, kind, rows)
    }

    /// Reopens typed Parquet rows and verifies exact catalog publication evidence.
    pub fn try_from_record_batch_with_publication_evidence(
        batch: RecordBatch,
        evidence: &PersistedProviderPublicationEvidence,
        maximum_retained_bytes: usize,
    ) -> Result<Self, ArrowConversionError> {
        evidence
            .verify_integrity()
            .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?;
        if maximum_retained_bytes == 0 || maximum_retained_bytes > MAX_EVENT_RESTART_BYTES {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let dataset = DatasetArrowBatch::try_from_record_batch(batch)?;
        if dataset.schema_ref().name()
            != DatasetSchemaRegistry::local()
                .canonical_market_events()?
                .name()
        {
            return Err(ArrowConversionError::UnexpectedDatasetSchema);
        }
        let batch = dataset.record_batch();
        if batch.get_array_memory_size() > maximum_retained_bytes {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let metadata = batch.schema().metadata().clone();
        let publication_digest = metadata
            .get(PROVIDER_PUBLICATION_DIGEST_KEY)
            .and_then(|value| decode_hex(value))
            .map(|bytes| EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        let publication_kind = metadata
            .get(PROVIDER_PUBLICATION_KIND_KEY)
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        if publication_digest != evidence.publication_digest()
            || publication_kind != evidence.publication_kind()
            || batch.num_rows() != publication_row_count(evidence)?
        {
            return Err(ArrowConversionError::InvalidMarketEventRow);
        }
        let columns = EventColumns::try_from_batch(batch)?;
        let mut events = Vec::new();
        events
            .try_reserve_exact(batch.num_rows())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for ordinal in 0..batch.num_rows() {
            let row = expected_row(evidence, ordinal)
                .ok_or(ArrowConversionError::InvalidMarketEventRow)?;
            let payload = columns.event_json.value(ordinal);
            let event: MarketEvent = serde_json::from_slice(payload)?;
            validate_restarted_row(&columns, ordinal, &event, payload, row)?;
            events.push(event);
        }
        verify_rebuilt_content(evidence, &events)?;
        Ok(Self {
            dataset,
            events: events.into_boxed_slice(),
            publication_digest,
            publication_kind: publication_kind.clone().into_boxed_str(),
        })
    }

    /// Returns the generic registered-dataset publication view.
    pub const fn dataset_batch(&self) -> &DatasetArrowBatch {
        &self.dataset
    }

    /// Returns exact typed canonical events in provider order.
    pub fn events(&self) -> &[MarketEvent] {
        &self.events
    }

    /// Returns the tagged provider publication digest.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the closed provider publication kind.
    pub fn publication_kind(&self) -> &str {
        &self.publication_kind
    }

    /// Returns the exact registered event schema.
    pub const fn schema_ref(&self) -> &DatasetSchemaRef {
        self.dataset.schema_ref()
    }

    /// Hashes exact row order, canonical/native identities, and frame coordinates.
    pub fn lineage_digest(&self) -> Result<EvidenceDigest, ArrowConversionError> {
        let batch = self.dataset.record_batch();
        let columns = EventColumns::try_from_batch(batch)?;
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/market-event-row-lineage/v1");
        digest.update(self.publication_digest.bytes());
        digest.update(
            u64::try_from(batch.num_rows())
                .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
                .to_be_bytes(),
        );
        for row in 0..batch.num_rows() {
            digest.update(columns.canonical_ordinals.value(row).to_be_bytes());
            digest.update(columns.logical_unit_ordinals.value(row).to_be_bytes());
            digest.update(columns.physical_frame_ordinals.value(row).to_be_bytes());
            digest.update(columns.raw_payload_digests.value(row));
            digest.update(columns.native_digests.value(row));
            digest.update(columns.event_digests.value(row));
        }
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))
    }
}

struct EventProjection<'a> {
    event: &'a MarketEvent,
    event_digest: EvidenceDigest,
    native_payload: &'a [u8],
    native_digest: EvidenceDigest,
    component_kind: &'static str,
    logical_ordinal: u16,
    physical_ordinal: u32,
    event_id: Option<[u8; 16]>,
    connection_id: Option<[u8; 16]>,
    source_sequence: Option<u64>,
    raw_payload_digest: EvidenceDigest,
}

fn response_rows(
    binding: &SealedProviderResponseMarketEventBinding,
) -> Result<Vec<EventProjection<'_>>, ArrowConversionError> {
    let batch = binding.batch();
    let native = binding.native_lineage();
    if batch.events().is_empty()
        || batch.events().len() != native.rows().len()
        || batch.events().len() != binding.row_frames().len()
    {
        return Err(ArrowConversionError::InvalidMarketEventRow);
    }
    batch
        .events()
        .iter()
        .zip(native.rows())
        .zip(binding.row_frames())
        .enumerate()
        .map(|(ordinal, ((event, native_payload), coordinate))| {
            Ok(EventProjection {
                event,
                event_digest: batch
                    .canonical_event_digest(ordinal)
                    .ok_or(ArrowConversionError::InvalidMarketEventRow)?,
                native_payload,
                native_digest: native
                    .row_digest(ordinal)
                    .ok_or(ArrowConversionError::InvalidMarketEventRow)?,
                component_kind: RESPONSE_KIND,
                logical_ordinal: coordinate.capture_page_ordinal(),
                physical_ordinal: coordinate.physical_frame_ordinal(),
                event_id: None,
                connection_id: None,
                source_sequence: coordinate.source_sequence(),
                raw_payload_digest: coordinate.page_body_digest(),
            })
        })
        .collect()
}

fn event_rows(
    binding: &SealedProviderEventMicrobatchBinding,
) -> Result<Vec<EventProjection<'_>>, ArrowConversionError> {
    let batch = binding.batch();
    let native = binding.native_lineage();
    if batch.events().is_empty()
        || batch.events().len() != native.rows().len()
        || batch.events().len() != binding.row_frames().len()
    {
        return Err(ArrowConversionError::InvalidMarketEventRow);
    }
    batch
        .events()
        .iter()
        .zip(native.rows())
        .zip(binding.row_frames())
        .enumerate()
        .map(|(ordinal, ((event, native_payload), coordinate))| {
            Ok(EventProjection {
                event,
                event_digest: batch
                    .canonical_event_digest(ordinal)
                    .ok_or(ArrowConversionError::InvalidMarketEventRow)?,
                native_payload,
                native_digest: native
                    .row_digest(ordinal)
                    .ok_or(ArrowConversionError::InvalidMarketEventRow)?,
                component_kind: EVENT_KIND,
                logical_ordinal: coordinate.event_frame_ordinal(),
                physical_ordinal: coordinate.physical_frame_ordinal(),
                event_id: Some(coordinate.event_id()),
                connection_id: Some(coordinate.connection_id()),
                source_sequence: coordinate.source_sequence(),
                raw_payload_digest: coordinate.payload_digest(),
            })
        })
        .collect()
}

fn build_event_batch(
    provider_dataset: &market_squawk_domain::SourceIdentifier,
    publication_digest: EvidenceDigest,
    publication_kind: &'static str,
    rows: Vec<EventProjection<'_>>,
) -> Result<ProviderMarketEventArrowBatch, ArrowConversionError> {
    if rows.is_empty() {
        return Err(ArrowConversionError::InvalidMarketEventRow);
    }
    let schema = market_event_schema(provider_dataset, publication_digest, publication_kind)?;
    let count = rows.len();
    let mut ordinals = Vec::with_capacity(count);
    let mut source_ids = Vec::with_capacity(count);
    let mut kinds = Vec::with_capacity(count);
    let mut instruments = Vec::with_capacity(count);
    let mut venues = Vec::with_capacity(count);
    let mut identifiers = Vec::with_capacity(count);
    let mut source_times = Vec::with_capacity(count);
    let mut received = Vec::with_capacity(count);
    let mut available = Vec::with_capacity(count);
    let mut ingested = Vec::with_capacity(count);
    let mut component_kinds = Vec::with_capacity(count);
    let mut logical = Vec::with_capacity(count);
    let mut physical = Vec::with_capacity(count);
    let mut event_ids = Vec::with_capacity(count);
    let mut connection_ids = Vec::with_capacity(count);
    let mut sequences = Vec::with_capacity(count);
    let mut raw_digests = Vec::with_capacity(count);
    let mut native_digests = Vec::with_capacity(count);
    let mut event_digests = Vec::with_capacity(count);
    let mut json = Vec::with_capacity(count);
    let mut events = Vec::with_capacity(count);
    for (ordinal, row) in rows.into_iter().enumerate() {
        let provenance = market_event_provenance(row.event);
        let payload = serde_json::to_vec(row.event)?;
        if sha256_evidence(&payload) != row.event_digest
            || sha256_evidence(row.native_payload) != row.native_digest
            || provenance.binding().payload_digest() != row.raw_payload_digest
        {
            return Err(ArrowConversionError::InvalidMarketEventRow);
        }
        ordinals
            .push(u32::try_from(ordinal).map_err(|_| ArrowConversionError::InvalidMarketEventRow)?);
        source_ids.push(provenance.source_id().as_str());
        kinds.push(market_event_kind(row.event));
        instruments.push(
            provenance
                .instrument_id()
                .ok_or(ArrowConversionError::InvalidMarketEventRow)?
                .as_uuid()
                .as_bytes()
                .to_vec(),
        );
        venues.push(
            provenance
                .venue_id()
                .ok_or(ArrowConversionError::InvalidMarketEventRow)?
                .as_str(),
        );
        identifiers.push(provenance.source_identifier().as_str());
        source_times.push(provenance.source_timestamp().map(|time| time.unix_nanos()));
        received.push(provenance.received_at().unix_nanos());
        available.push(provenance.available_at().unix_nanos());
        ingested.push(provenance.ingested_at().unix_nanos());
        component_kinds.push(row.component_kind);
        logical.push(row.logical_ordinal);
        physical.push(row.physical_ordinal);
        event_ids.push(row.event_id.map(|value| value.to_vec()));
        connection_ids.push(row.connection_id.map(|value| value.to_vec()));
        sequences.push(
            row.source_sequence
                .map(|value| value.to_be_bytes().to_vec()),
        );
        raw_digests.push(row.raw_payload_digest.bytes().to_vec());
        native_digests.push(row.native_digest.bytes().to_vec());
        event_digests.push(row.event_digest.bytes().to_vec());
        json.push(payload);
        events.push(row.event.clone());
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt16Array::from_value(MARKET_EVENT_SCHEMA_VERSION, count)),
        Arc::new(UInt32Array::from(ordinals)),
        Arc::new(StringArray::from(source_ids)),
        Arc::new(StringArray::from(kinds)),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            instruments.iter().map(Vec::as_slice),
        )?),
        Arc::new(StringArray::from(venues)),
        Arc::new(StringArray::from(identifiers)),
        Arc::new(TimestampNanosecondArray::from(source_times).with_timezone_utc()),
        Arc::new(TimestampNanosecondArray::from(received).with_timezone_utc()),
        Arc::new(TimestampNanosecondArray::from(available).with_timezone_utc()),
        Arc::new(TimestampNanosecondArray::from(ingested).with_timezone_utc()),
        Arc::new(StringArray::from(component_kinds)),
        Arc::new(UInt16Array::from(logical)),
        Arc::new(UInt32Array::from(physical)),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            event_ids.iter().map(|value| value.as_deref()),
            16,
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            connection_ids.iter().map(|value| value.as_deref()),
            16,
        )?),
        Arc::new(BinaryArray::from_iter(
            sequences.iter().map(|value| value.as_deref()),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            raw_digests.iter().map(Vec::as_slice),
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            native_digests.iter().map(Vec::as_slice),
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            event_digests.iter().map(Vec::as_slice),
        )?),
        Arc::new(BinaryArray::from_iter_values(
            json.iter().map(Vec::as_slice),
        )),
    ];
    let batch = RecordBatch::try_new(schema, columns)?;
    let dataset = DatasetArrowBatch::try_new(
        DatasetSchemaRegistry::local().canonical_market_events()?,
        batch,
    )?;
    Ok(ProviderMarketEventArrowBatch {
        dataset,
        events: events.into_boxed_slice(),
        publication_digest,
        publication_kind: publication_kind.into(),
    })
}

enum ExpectedRow<'a> {
    Response(&'a PersistedProviderResponseMarketEventBindingRow),
    Event(&'a PersistedProviderEventBindingRow),
}

fn publication_row_count(
    evidence: &PersistedProviderPublicationEvidence,
) -> Result<usize, ArrowConversionError> {
    match evidence {
        PersistedProviderPublicationEvidence::ResponseMarketEvent(response) => {
            Ok(response.canonical_event_count())
        }
        PersistedProviderPublicationEvidence::EventMicrobatch(event) => {
            Ok(event.canonical_event_count())
        }
        PersistedProviderPublicationEvidence::CompositeResponseEvent {
            response, event, ..
        } => response
            .canonical_event_count()
            .checked_add(event.canonical_event_count())
            .ok_or(ArrowConversionError::RetainedSizeOverflow),
    }
}

fn expected_row(
    evidence: &PersistedProviderPublicationEvidence,
    ordinal: usize,
) -> Option<ExpectedRow<'_>> {
    match evidence {
        PersistedProviderPublicationEvidence::ResponseMarketEvent(response) => {
            response.rows().get(ordinal).map(ExpectedRow::Response)
        }
        PersistedProviderPublicationEvidence::EventMicrobatch(event) => {
            event.rows().get(ordinal).map(ExpectedRow::Event)
        }
        PersistedProviderPublicationEvidence::CompositeResponseEvent {
            response, event, ..
        } => response
            .rows()
            .get(ordinal)
            .map(ExpectedRow::Response)
            .or_else(|| {
                ordinal
                    .checked_sub(response.rows().len())
                    .and_then(|local| event.rows().get(local))
                    .map(ExpectedRow::Event)
            }),
    }
}

fn validate_restarted_row(
    columns: &EventColumns<'_>,
    ordinal: usize,
    event: &MarketEvent,
    payload: &[u8],
    expected: ExpectedRow<'_>,
) -> Result<(), ArrowConversionError> {
    let provenance = market_event_provenance(event);
    if columns.schema_versions.value(ordinal) != MARKET_EVENT_SCHEMA_VERSION
        || columns.canonical_ordinals.value(ordinal)
            != u32::try_from(ordinal).map_err(|_| ArrowConversionError::InvalidMarketEventRow)?
        || columns.source_ids.value(ordinal) != provenance.source_id().as_str()
        || columns.event_kinds.value(ordinal) != market_event_kind(event)
        || columns.instrument_ids.value(ordinal)
            != provenance
                .instrument_id()
                .ok_or(ArrowConversionError::InvalidMarketEventRow)?
                .as_uuid()
                .as_bytes()
        || columns.venue_ids.value(ordinal)
            != provenance
                .venue_id()
                .ok_or(ArrowConversionError::InvalidMarketEventRow)?
                .as_str()
        || columns.source_identifiers.value(ordinal) != provenance.source_identifier().as_str()
        || optional_timestamp(columns.source_timestamps, ordinal)
            != provenance.source_timestamp().map(|time| time.unix_nanos())
        || columns.received_at.value(ordinal) != provenance.received_at().unix_nanos()
        || columns.available_at.value(ordinal) != provenance.available_at().unix_nanos()
        || columns.ingested_at.value(ordinal) != provenance.ingested_at().unix_nanos()
        || serde_json::to_vec(event)? != payload
    {
        return Err(ArrowConversionError::InvalidMarketEventRow);
    }
    let (
        kind,
        logical,
        physical,
        event_id,
        connection_id,
        sequence,
        raw,
        native_digest,
        event_digest,
    ) = match expected {
        ExpectedRow::Response(row) => (
            RESPONSE_KIND,
            row.capture_page_ordinal(),
            row.physical_frame_ordinal(),
            None,
            None,
            row.source_sequence(),
            row.payload_digest(),
            row.native_semantic_digest(),
            row.canonical_event_digest(),
        ),
        ExpectedRow::Event(row) => (
            EVENT_KIND,
            row.event_frame_ordinal(),
            row.physical_frame_ordinal(),
            Some(row.event_id()),
            Some(row.connection_id()),
            row.source_sequence(),
            row.payload_digest(),
            row.native_semantic_digest(),
            row.canonical_event_digest(),
        ),
    };
    let actual_event_id =
        (!columns.event_ids.is_null(ordinal)).then(|| columns.event_ids.value(ordinal));
    let actual_connection_id =
        (!columns.connection_ids.is_null(ordinal)).then(|| columns.connection_ids.value(ordinal));
    if columns.logical_unit_kinds.value(ordinal) != kind
        || columns.logical_unit_ordinals.value(ordinal) != logical
        || columns.physical_frame_ordinals.value(ordinal) != physical
        || actual_event_id != event_id.as_ref().map(<[u8; 16]>::as_slice)
        || actual_connection_id != connection_id.as_ref().map(<[u8; 16]>::as_slice)
        || decode_optional_u64(columns.source_sequence, ordinal)? != sequence
        || columns.raw_payload_digests.value(ordinal) != raw.bytes()
        || columns.native_digests.value(ordinal) != native_digest.bytes()
        || columns.event_digests.value(ordinal) != event_digest.bytes()
        || sha256_evidence(payload) != event_digest
        || provenance.binding().payload_digest() != raw
    {
        return Err(ArrowConversionError::InvalidMarketEventRow);
    }
    Ok(())
}

fn verify_rebuilt_content(
    evidence: &PersistedProviderPublicationEvidence,
    events: &[MarketEvent],
) -> Result<(), ArrowConversionError> {
    match evidence {
        PersistedProviderPublicationEvidence::ResponseMarketEvent(response) => {
            verify_response_content(response, events)
        }
        PersistedProviderPublicationEvidence::EventMicrobatch(event) => {
            verify_event_content(event, events)
        }
        PersistedProviderPublicationEvidence::CompositeResponseEvent {
            response, event, ..
        } => {
            let (response_events, event_events) = events.split_at(response.canonical_event_count());
            verify_response_content(response, response_events)?;
            verify_event_content(event, event_events)
        }
    }
}

fn verify_response_content(
    evidence: &PersistedProviderResponseMarketEventBindingEvidence,
    events: &[MarketEvent],
) -> Result<(), ArrowConversionError> {
    verify_content_identity(
        ProviderMarketEventBatch::try_new(
            evidence.capture().source_id().clone(),
            evidence.capture().metadata_revision().clone(),
            evidence.capture().dataset().clone(),
            events.to_vec(),
        )
        .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?,
        evidence.canonical_schema_fingerprint(),
        evidence.canonical_content_digest(),
        evidence.canonical_event_count(),
    )
}

fn verify_event_content(
    evidence: &PersistedProviderEventBindingEvidence,
    events: &[MarketEvent],
) -> Result<(), ArrowConversionError> {
    verify_content_identity(
        ProviderMarketEventBatch::try_new(
            evidence.capture().source_id().clone(),
            evidence.capture().metadata_revision().clone(),
            evidence.capture().dataset().clone(),
            events.to_vec(),
        )
        .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?,
        evidence.canonical_schema_fingerprint(),
        evidence.canonical_content_digest(),
        evidence.canonical_event_count(),
    )
}

fn verify_content_identity(
    batch: ProviderMarketEventBatch,
    schema: EvidenceDigest,
    content: EvidenceDigest,
    count: usize,
) -> Result<(), ArrowConversionError> {
    let observed = batch.content_identity();
    if observed.schema_fingerprint() != schema
        || observed.content_digest() != content
        || observed.event_count() != count
    {
        return Err(ArrowConversionError::InvalidMarketEventRow);
    }
    Ok(())
}

struct EventColumns<'a> {
    schema_versions: &'a UInt16Array,
    canonical_ordinals: &'a UInt32Array,
    source_ids: &'a StringArray,
    event_kinds: &'a StringArray,
    instrument_ids: &'a FixedSizeBinaryArray,
    venue_ids: &'a StringArray,
    source_identifiers: &'a StringArray,
    source_timestamps: &'a TimestampNanosecondArray,
    received_at: &'a TimestampNanosecondArray,
    available_at: &'a TimestampNanosecondArray,
    ingested_at: &'a TimestampNanosecondArray,
    logical_unit_kinds: &'a StringArray,
    logical_unit_ordinals: &'a UInt16Array,
    physical_frame_ordinals: &'a UInt32Array,
    event_ids: &'a FixedSizeBinaryArray,
    connection_ids: &'a FixedSizeBinaryArray,
    source_sequence: &'a BinaryArray,
    raw_payload_digests: &'a FixedSizeBinaryArray,
    native_digests: &'a FixedSizeBinaryArray,
    event_digests: &'a FixedSizeBinaryArray,
    event_json: &'a BinaryArray,
}

impl<'a> EventColumns<'a> {
    fn try_from_batch(batch: &'a RecordBatch) -> Result<Self, ArrowConversionError> {
        macro_rules! column {
            ($name:literal, $kind:ty) => {
                batch
                    .column_by_name($name)
                    .and_then(|value| value.as_any().downcast_ref::<$kind>())
                    .ok_or(ArrowConversionError::InvalidMarketEventRow)?
            };
        }
        Ok(Self {
            schema_versions: column!("schema_version", UInt16Array),
            canonical_ordinals: column!("canonical_row_ordinal", UInt32Array),
            source_ids: column!("source_id", StringArray),
            event_kinds: column!("event_kind", StringArray),
            instrument_ids: column!("instrument_id", FixedSizeBinaryArray),
            venue_ids: column!("venue_id", StringArray),
            source_identifiers: column!("source_identifier", StringArray),
            source_timestamps: column!("source_timestamp", TimestampNanosecondArray),
            received_at: column!("received_at", TimestampNanosecondArray),
            available_at: column!("available_at", TimestampNanosecondArray),
            ingested_at: column!("ingested_at", TimestampNanosecondArray),
            logical_unit_kinds: column!("logical_unit_kind", StringArray),
            logical_unit_ordinals: column!("logical_unit_ordinal", UInt16Array),
            physical_frame_ordinals: column!("physical_frame_ordinal", UInt32Array),
            event_ids: column!("event_id", FixedSizeBinaryArray),
            connection_ids: column!("connection_id", FixedSizeBinaryArray),
            source_sequence: column!("source_sequence_be", BinaryArray),
            raw_payload_digests: column!("raw_payload_sha256", FixedSizeBinaryArray),
            native_digests: column!("native_semantic_sha256", FixedSizeBinaryArray),
            event_digests: column!("event_sha256", FixedSizeBinaryArray),
            event_json: column!("event_json", BinaryArray),
        })
    }
}

const fn market_event_provenance(event: &MarketEvent) -> &LiveProvenance {
    match event {
        MarketEvent::Trade(value) => value.provenance(),
        MarketEvent::Quote(value) => value.provenance(),
        MarketEvent::BookSnapshot(value) => value.provenance(),
        MarketEvent::BookDelta(value) => value.provenance(),
        MarketEvent::Auction(value) => value.provenance(),
        MarketEvent::TradingHalt(value) => value.provenance(),
        MarketEvent::InstrumentStatus(value) => value.provenance(),
        MarketEvent::CorporateAction(value) => value.provenance(),
    }
}

const fn market_event_kind(event: &MarketEvent) -> &'static str {
    match event {
        MarketEvent::Trade(_) => "trade",
        MarketEvent::Quote(_) => "quote",
        MarketEvent::BookSnapshot(_) => "book_snapshot",
        MarketEvent::BookDelta(_) => "book_delta",
        MarketEvent::Auction(_) => "auction",
        MarketEvent::TradingHalt(_) => "trading_halt",
        MarketEvent::InstrumentStatus(_) => "instrument_status",
        MarketEvent::CorporateAction(_) => "corporate_action",
    }
}

fn optional_timestamp(array: &TimestampNanosecondArray, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn decode_optional_u64(
    array: &BinaryArray,
    row: usize,
) -> Result<Option<u64>, ArrowConversionError> {
    if array.is_null(row) {
        return Ok(None);
    }
    let bytes: [u8; 8] = array
        .value(row)
        .try_into()
        .map_err(|_| ArrowConversionError::InvalidMarketEventRow)?;
    Ok(Some(u64::from_be_bytes(bytes)))
}

fn sha256_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
