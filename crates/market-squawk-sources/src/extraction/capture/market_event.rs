//! Durable typed live-market event publication bound to sealed provider captures.

use std::io::{self, Write};

use market_squawk_domain::{LiveProvenance, MarketEvent};

use crate::ProviderNativeInstrumentAttestation;

use super::super::native_lineage::{
    MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES, MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
    MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES, ProviderNativeLineageBatchSidecarEvidenceRef,
};
use super::*;

/// Code-owned canonical schema version for durable live [`MarketEvent`] batches.
pub const PROVIDER_MARKET_EVENT_SCHEMA_VERSION: u16 = 1;
/// Maximum canonical events retained by one durable live publication batch.
pub const MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS: usize = MAX_PROVIDER_EVENT_MICROBATCH_FRAMES;
/// Maximum aggregate canonical JSON bytes admitted to one durable live publication batch.
pub const MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES: usize = 64 * 1024 * 1024;

const PROVIDER_MARKET_EVENT_SCHEMA_DOMAIN: &[u8] = b"market-squawk/provider-market-event/schema/v1";
const PROVIDER_MARKET_EVENT_CONTENT_DOMAIN: &[u8] =
    b"market-squawk/provider-market-event/content/v1";
const PROVIDER_MARKET_EVENT_NATIVE_DOMAIN: &[u8] =
    b"market-squawk/provider-market-event/native-lineage/v1";
const PROVIDER_MARKET_EVENT_NATIVE_ROW_MAGIC: &[u8; 5] = b"MSNI1";
const PROVIDER_MARKET_EVENT_NATIVE_ROW_HEADER_BYTES: usize = 9;

/// Deterministic identity of one exact ordered canonical live-market event batch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderMarketEventContentIdentity {
    schema_fingerprint: EvidenceDigest,
    content_digest: EvidenceDigest,
    event_count: usize,
}

impl ProviderMarketEventContentIdentity {
    /// Returns the code-owned canonical schema fingerprint.
    pub const fn schema_fingerprint(self) -> EvidenceDigest {
        self.schema_fingerprint
    }

    /// Returns deterministic identity over source coordinates and ordered canonical events.
    pub const fn content_digest(self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the exact canonical event count.
    pub const fn event_count(self) -> usize {
        self.event_count
    }
}

/// Non-cloneable bounded canonical batch for decoded typed live-market events.
#[derive(Debug)]
pub struct ProviderMarketEventBatch {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    events: Box<[MarketEvent]>,
    event_digests: Box<[EvidenceDigest]>,
    content_identity: ProviderMarketEventContentIdentity,
}

impl ProviderMarketEventBatch {
    /// Admits exact typed canonical events without routing them through research observations.
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        events: Vec<MarketEvent>,
    ) -> Result<Self, ProviderCaptureError> {
        if events.is_empty() || events.len() > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let schema_fingerprint = provider_market_event_schema_fingerprint();
        let mut content = Sha256::new();
        hash_binding_field(&mut content, PROVIDER_MARKET_EVENT_CONTENT_DOMAIN)?;
        hash_digest(&mut content, schema_fingerprint);
        hash_binding_field(&mut content, source_id.as_str().as_bytes())?;
        hash_binding_field(
            &mut content,
            metadata_revision.as_source_identifier().as_str().as_bytes(),
        )?;
        hash_binding_field(&mut content, dataset.as_str().as_bytes())?;
        hash_binding_length(&mut content, events.len())?;
        let mut event_digests = Vec::new();
        event_digests
            .try_reserve_exact(events.len())
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        let mut retained_bytes = 0_usize;
        for event in &events {
            let provenance = market_event_provenance(event);
            if provenance.source_id() != &source_id
                || provenance.binding().metadata_revision() != &metadata_revision
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            let payload = serialize_provider_market_event(event)?;
            retained_bytes = retained_bytes.checked_add(payload.len()).ok_or(
                ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES as u64,
                },
            )?;
            if retained_bytes > MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES {
                return Err(ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES as u64,
                });
            }
            let event_digest =
                EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
            hash_binding_length(&mut content, payload.len())?;
            content.update(payload);
            event_digests.push(event_digest);
        }
        let event_count = events.len();
        Ok(Self {
            source_id,
            metadata_revision,
            dataset,
            events: events.into_boxed_slice(),
            event_digests: event_digests.into_boxed_slice(),
            content_identity: ProviderMarketEventContentIdentity {
                schema_fingerprint,
                content_digest: EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    content.finalize().into(),
                ),
                event_count,
            },
        })
    }

    /// Returns the exact source authority identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata interpretation revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the canonical provider stream dataset.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns typed canonical live-market events in provider wire order.
    pub const fn events(&self) -> &[MarketEvent] {
        &self.events
    }

    /// Returns deterministic canonical batch identity.
    pub const fn content_identity(&self) -> ProviderMarketEventContentIdentity {
        self.content_identity
    }

    /// Returns SHA-256 of one exact canonical event payload for durable row alignment.
    pub fn canonical_event_digest(&self, ordinal: usize) -> Option<EvidenceDigest> {
        self.event_digests.get(ordinal).copied()
    }

    fn validate(&self) -> Result<(), ProviderCaptureError> {
        let rebuilt = Self::try_new(
            self.source_id.clone(),
            self.metadata_revision.clone(),
            self.dataset.clone(),
            self.events.to_vec(),
        )?;
        if rebuilt.content_identity != self.content_identity
            || rebuilt.event_digests != self.event_digests
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

fn market_event_provenance(event: &MarketEvent) -> &LiveProvenance {
    match event {
        MarketEvent::Trade(event) => event.provenance(),
        MarketEvent::Quote(event) => event.provenance(),
        MarketEvent::BookSnapshot(event) => event.provenance(),
        MarketEvent::BookDelta(event) => event.provenance(),
        MarketEvent::Auction(event) => event.provenance(),
        MarketEvent::TradingHalt(event) => event.provenance(),
        MarketEvent::InstrumentStatus(event) => event.provenance(),
        MarketEvent::CorporateAction(event) => event.provenance(),
    }
}

fn serialize_provider_market_event(event: &MarketEvent) -> Result<Vec<u8>, ProviderCaptureError> {
    let mut writer = ProviderMarketEventWriter {
        bytes: Vec::new(),
        limit_exceeded: false,
        allocation_failed: false,
    };
    if serde_json::to_writer(&mut writer, event).is_err() {
        return Err(if writer.limit_exceeded {
            ProviderCaptureError::ByteLimitExceeded {
                max: MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES as u64,
            }
        } else if writer.allocation_failed {
            ProviderCaptureError::AllocationFailed
        } else {
            ProviderCaptureError::MaterialBindingMismatch
        });
    }
    Ok(writer.bytes)
}

struct ProviderMarketEventWriter {
    bytes: Vec<u8>,
    limit_exceeded: bool,
    allocation_failed: bool,
}

impl Write for ProviderMarketEventWriter {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        let Some(next_len) = self.bytes.len().checked_add(value.len()) else {
            self.limit_exceeded = true;
            return Err(io::Error::other("canonical market-event length overflow"));
        };
        if next_len > MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES {
            self.limit_exceeded = true;
            return Err(io::Error::other(
                "canonical market-event exceeds batch bound",
            ));
        }
        if self.bytes.try_reserve_exact(value.len()).is_err() {
            self.allocation_failed = true;
            return Err(io::Error::other(
                "canonical market-event bounded allocation failed",
            ));
        }
        self.bytes.extend_from_slice(value);
        Ok(value.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn provider_market_event_schema_fingerprint() -> EvidenceDigest {
    let mut digest = Sha256::new();
    hash_field(&mut digest, PROVIDER_MARKET_EVENT_SCHEMA_DOMAIN);
    digest.update(PROVIDER_MARKET_EVENT_SCHEMA_VERSION.to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

/// Exact provider-native semantics aligned one-for-one to a canonical market-event batch.
#[derive(Debug)]
pub struct ProviderMarketEventNativeLineageRow {
    instrument_attestation: ProviderNativeInstrumentAttestation,
    provider_native_semantic_payload: Bytes,
    encoded_payload: Bytes,
}

impl ProviderMarketEventNativeLineageRow {
    /// Binds exact provider-native row semantics to one canonical identity attestation.
    pub fn try_new(
        instrument_attestation: ProviderNativeInstrumentAttestation,
        provider_native_semantic_payload: Bytes,
    ) -> Result<Self, ProviderCaptureError> {
        if provider_native_semantic_payload.is_empty() {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let encoded_payload = encode_provider_market_event_native_row(
            &instrument_attestation,
            &provider_native_semantic_payload,
        )?;
        Ok(Self {
            instrument_attestation,
            provider_native_semantic_payload,
            encoded_payload,
        })
    }

    /// Returns the exact durable canonical/provider identity evidence.
    pub const fn instrument_attestation(&self) -> &ProviderNativeInstrumentAttestation {
        &self.instrument_attestation
    }

    /// Returns exact adapter-native semantics without the shared identity envelope.
    pub const fn provider_native_semantic_payload(&self) -> &Bytes {
        &self.provider_native_semantic_payload
    }

    fn try_from_encoded(encoded_payload: Bytes) -> Result<Self, ProviderCaptureError> {
        let (instrument_attestation, provider_native_semantic_payload) =
            decode_provider_market_event_native_row(&encoded_payload)?;
        Ok(Self {
            instrument_attestation,
            provider_native_semantic_payload: Bytes::copy_from_slice(
                provider_native_semantic_payload,
            ),
            encoded_payload,
        })
    }
}

/// Exact provider-native semantics aligned one-for-one to a canonical market-event batch.
#[derive(Debug)]
pub struct ProviderMarketEventNativeLineageBatch {
    implementation: ProviderNativeLineageImplementation,
    content_identity: ProviderMarketEventContentIdentity,
    rows: Box<[Bytes]>,
    row_digests: Box<[EvidenceDigest]>,
    batch_sidecar: Option<Bytes>,
    batch_sidecar_digest: Option<EvidenceDigest>,
    batch_digest: EvidenceDigest,
}

impl ProviderMarketEventNativeLineageBatch {
    /// Binds bounded exact identity-attested adapter-native rows and one optional batch sidecar.
    pub fn try_new(
        implementation: ProviderNativeLineageImplementation,
        batch: &ProviderMarketEventBatch,
        rows: Vec<ProviderMarketEventNativeLineageRow>,
        batch_sidecar: Option<Bytes>,
    ) -> Result<Self, ProviderCaptureError> {
        if rows.len() != batch.events().len() || rows.len() > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let mut retained = 0_usize;
        let mut row_digests = Vec::new();
        row_digests
            .try_reserve_exact(rows.len())
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        for (ordinal, row) in rows.iter().enumerate() {
            validate_native_row_attestation(batch, ordinal, &row.instrument_attestation)?;
            if row.encoded_payload.is_empty()
                || row.encoded_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            retained = retained
                .checked_add(row.encoded_payload.len())
                .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
            row_digests.push(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&row.encoded_payload).into(),
            ));
        }
        let batch_sidecar_digest = match batch_sidecar.as_ref() {
            Some(sidecar) => {
                if sidecar.is_empty() || sidecar.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES {
                    return Err(ProviderCaptureError::SealedBindingMismatch);
                }
                retained = retained
                    .checked_add(sidecar.len())
                    .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
                Some(EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(sidecar).into(),
                ))
            }
            None => None,
        };
        if retained > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let content_identity = batch.content_identity();
        let batch_digest = provider_market_event_native_lineage_batch_digest(
            PROVIDER_MARKET_EVENT_SCHEMA_VERSION,
            implementation,
            content_identity.content_digest(),
            rows.iter()
                .zip(&row_digests)
                .enumerate()
                .map(|(ordinal, (row, row_digest))| {
                    Ok(ProviderMarketEventNativeLineageDigestRow {
                        canonical_row_ordinal: u32::try_from(ordinal)
                            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
                        canonical_event_digest: batch
                            .canonical_event_digest(ordinal)
                            .ok_or(ProviderCaptureError::SealedBindingMismatch)?,
                        native_semantic_bytes: row.encoded_payload.len(),
                        native_semantic_digest: *row_digest,
                    })
                }),
            batch_sidecar
                .as_ref()
                .zip(batch_sidecar_digest)
                .map(
                    |(sidecar, digest)| ProviderMarketEventNativeLineageSidecarDigestEvidence {
                        native_semantic_bytes: sidecar.len(),
                        native_semantic_digest: digest,
                    },
                ),
        )?;
        Ok(Self {
            implementation,
            content_identity,
            rows: rows
                .into_iter()
                .map(|row| row.encoded_payload)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            row_digests: row_digests.into_boxed_slice(),
            batch_sidecar,
            batch_sidecar_digest,
            batch_digest,
        })
    }

    /// Returns the closed adapter-native encoder implementation.
    pub const fn implementation(&self) -> ProviderNativeLineageImplementation {
        self.implementation
    }

    /// Returns the exact canonical batch identity this evidence is bound to.
    pub const fn content_identity(&self) -> ProviderMarketEventContentIdentity {
        self.content_identity
    }

    /// Returns exact row-aligned provider-native semantic payloads.
    pub const fn rows(&self) -> &[Bytes] {
        &self.rows
    }

    /// Returns the SHA-256 identity of one exact provider-native row payload.
    pub fn row_digest(&self, ordinal: usize) -> Option<EvidenceDigest> {
        self.row_digests.get(ordinal).copied()
    }

    /// Returns optional exact batch-level provider-native semantics.
    pub const fn batch_sidecar(&self) -> Option<&Bytes> {
        self.batch_sidecar.as_ref()
    }

    /// Returns optional SHA-256 identity of the batch-level sidecar.
    pub const fn batch_sidecar_digest(&self) -> Option<EvidenceDigest> {
        self.batch_sidecar_digest
    }

    /// Returns deterministic identity over canonical alignment and all native evidence.
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }

    fn validate(&self, batch: &ProviderMarketEventBatch) -> Result<(), ProviderCaptureError> {
        let rebuilt = Self::try_new(
            self.implementation,
            batch,
            self.rows
                .iter()
                .cloned()
                .map(ProviderMarketEventNativeLineageRow::try_from_encoded)
                .collect::<Result<Vec<_>, _>>()?,
            self.batch_sidecar.clone(),
        )?;
        if rebuilt.content_identity != self.content_identity
            || rebuilt.row_digests != self.row_digests
            || rebuilt.batch_digest != self.batch_digest
            || rebuilt.batch_sidecar_digest != self.batch_sidecar_digest
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

/// Checked borrowed restart projection of one provider-native live-event row.
///
/// This value carries no live publication authority. It exists only to revalidate persisted
/// native bytes against the exact canonical event digest and the live batch hash contract.
#[derive(Debug)]
pub struct ProviderMarketEventNativeLineageRowEvidenceRef<'a> {
    canonical_row_ordinal: u32,
    canonical_event_digest: EvidenceDigest,
    native_semantic_payload: &'a [u8],
    native_semantic_digest: EvidenceDigest,
    instrument_attestation: ProviderNativeInstrumentAttestation,
    provider_native_semantic_payload: &'a [u8],
}

impl<'a> ProviderMarketEventNativeLineageRowEvidenceRef<'a> {
    /// Validates one bounded borrowed native row without copying its semantic payload.
    pub fn try_new(
        canonical_row_ordinal: u32,
        canonical_event_digest: EvidenceDigest,
        native_semantic_payload: &'a [u8],
        native_semantic_digest: EvidenceDigest,
    ) -> Result<Self, ProviderCaptureError> {
        if usize::try_from(canonical_row_ordinal)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            >= MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
            || native_semantic_payload.is_empty()
            || native_semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        require_sha256_identity(canonical_event_digest)?;
        require_sha256_identity(native_semantic_digest)?;
        if native_semantic_digest
            != EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(native_semantic_payload).into(),
            )
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let (instrument_attestation, provider_native_semantic_payload) =
            decode_provider_market_event_native_row(native_semantic_payload)?;
        Ok(Self {
            canonical_row_ordinal,
            canonical_event_digest,
            native_semantic_payload,
            native_semantic_digest,
            instrument_attestation,
            provider_native_semantic_payload,
        })
    }

    /// Returns the exact durable canonical/provider identity evidence recovered at restart.
    pub const fn instrument_attestation(&self) -> &ProviderNativeInstrumentAttestation {
        &self.instrument_attestation
    }

    /// Returns exact adapter-native semantics without the shared identity envelope.
    pub const fn provider_native_semantic_payload(&self) -> &'a [u8] {
        self.provider_native_semantic_payload
    }

    fn digest_projection(&self) -> ProviderMarketEventNativeLineageDigestRow {
        ProviderMarketEventNativeLineageDigestRow {
            canonical_row_ordinal: self.canonical_row_ordinal,
            canonical_event_digest: self.canonical_event_digest,
            native_semantic_bytes: self.native_semantic_payload.len(),
            native_semantic_digest: self.native_semantic_digest,
        }
    }
}

/// Verifies persisted provider-native live-event evidence without recreating live authority.
///
/// The caller supplies the stored expected digest and canonical content identity. Common code
/// independently validates the compiled schema, exact row count/order, canonical-event alignment,
/// native bytes/digests, sidecar, and aggregate byte bound before comparing the same private hash
/// used by [`ProviderMarketEventNativeLineageBatch::try_new`].
#[allow(
    clippy::too_many_arguments,
    reason = "persisted live-event native-lineage evidence remains explicit"
)]
pub fn verify_provider_market_event_native_lineage_batch_evidence(
    expected_batch_digest: EvidenceDigest,
    schema_version: u16,
    implementation: ProviderNativeLineageImplementation,
    canonical_schema_fingerprint: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    canonical_event_count: usize,
    rows: &[ProviderMarketEventNativeLineageRowEvidenceRef<'_>],
    batch_sidecar: Option<&ProviderNativeLineageBatchSidecarEvidenceRef<'_>>,
) -> Result<(), ProviderCaptureError> {
    require_sha256_identity(expected_batch_digest)?;
    require_sha256_identity(canonical_schema_fingerprint)?;
    require_sha256_identity(canonical_content_digest)?;
    if schema_version != PROVIDER_MARKET_EVENT_SCHEMA_VERSION
        || canonical_schema_fingerprint != provider_market_event_schema_fingerprint()
        || rows.is_empty()
        || rows.len() != canonical_event_count
        || rows.len() > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut retained_bytes = 0_usize;
    for (expected_ordinal, row) in rows.iter().enumerate() {
        retained_bytes = retained_bytes
            .checked_add(row.native_semantic_payload.len())
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if row.canonical_row_ordinal
            != u32::try_from(expected_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            || row.native_semantic_payload.is_empty()
            || row.native_semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
            || row.native_semantic_digest
                != EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(row.native_semantic_payload).into(),
                )
            || retained_bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        require_sha256_identity(row.canonical_event_digest)?;
        require_sha256_identity(row.native_semantic_digest)?;
        row.instrument_attestation
            .validate_at(row.instrument_attestation.selected_at())
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
    }
    let sidecar = match batch_sidecar {
        Some(sidecar) => {
            retained_bytes = retained_bytes
                .checked_add(sidecar.semantic_payload().len())
                .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
            if sidecar.semantic_payload().is_empty()
                || sidecar.semantic_payload().len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES
                || retained_bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            require_sha256_identity(sidecar.semantic_payload_digest())?;
            Some(ProviderMarketEventNativeLineageSidecarDigestEvidence {
                native_semantic_bytes: sidecar.semantic_payload().len(),
                native_semantic_digest: sidecar.semantic_payload_digest(),
            })
        }
        None => None,
    };
    let observed = provider_market_event_native_lineage_batch_digest(
        schema_version,
        implementation,
        canonical_content_digest,
        rows.iter().map(|row| Ok(row.digest_projection())),
        sidecar,
    )?;
    if observed != expected_batch_digest {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    Ok(())
}

fn validate_native_row_attestation(
    batch: &ProviderMarketEventBatch,
    ordinal: usize,
    attestation: &ProviderNativeInstrumentAttestation,
) -> Result<(), ProviderCaptureError> {
    let event = batch
        .events()
        .get(ordinal)
        .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
    let provenance = market_event_provenance(event);
    if attestation.provider_key().source_id() != batch.source_id()
        || provenance.instrument_id() != Some(attestation.instrument_id())
        || provenance.venue_id() != Some(attestation.venue_mapping().venue_id())
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    attestation
        .validate_at(provenance.received_at())
        .map_err(|_| ProviderCaptureError::SealedBindingMismatch)
}

fn encode_provider_market_event_native_row(
    attestation: &ProviderNativeInstrumentAttestation,
    provider_native_semantic_payload: &[u8],
) -> Result<Bytes, ProviderCaptureError> {
    let attestation = serde_json::to_vec(attestation)
        .map_err(|_| ProviderCaptureError::MaterialBindingMismatch)?;
    let attestation_len = u32::try_from(attestation.len())
        .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
    let total = PROVIDER_MARKET_EVENT_NATIVE_ROW_HEADER_BYTES
        .checked_add(attestation.len())
        .and_then(|size| size.checked_add(provider_native_semantic_payload.len()))
        .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
    if total > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(total)
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    encoded.extend_from_slice(PROVIDER_MARKET_EVENT_NATIVE_ROW_MAGIC);
    encoded.extend_from_slice(&attestation_len.to_be_bytes());
    encoded.extend_from_slice(&attestation);
    encoded.extend_from_slice(provider_native_semantic_payload);
    Ok(Bytes::from(encoded))
}

fn decode_provider_market_event_native_row(
    encoded: &[u8],
) -> Result<(ProviderNativeInstrumentAttestation, &[u8]), ProviderCaptureError> {
    if encoded.len() <= PROVIDER_MARKET_EVENT_NATIVE_ROW_HEADER_BYTES
        || encoded.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
        || encoded.get(..PROVIDER_MARKET_EVENT_NATIVE_ROW_MAGIC.len())
            != Some(PROVIDER_MARKET_EVENT_NATIVE_ROW_MAGIC.as_slice())
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let length_bytes: [u8; 4] = encoded[5..9]
        .try_into()
        .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
    let attestation_end = PROVIDER_MARKET_EVENT_NATIVE_ROW_HEADER_BYTES
        .checked_add(
            usize::try_from(u32::from_be_bytes(length_bytes))
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
        )
        .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
    if attestation_end >= encoded.len() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let attestation = serde_json::from_slice(&encoded[9..attestation_end])
        .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
    Ok((attestation, &encoded[attestation_end..]))
}

#[derive(Clone, Copy)]
struct ProviderMarketEventNativeLineageDigestRow {
    canonical_row_ordinal: u32,
    canonical_event_digest: EvidenceDigest,
    native_semantic_bytes: usize,
    native_semantic_digest: EvidenceDigest,
}

#[derive(Clone, Copy)]
struct ProviderMarketEventNativeLineageSidecarDigestEvidence {
    native_semantic_bytes: usize,
    native_semantic_digest: EvidenceDigest,
}

fn provider_market_event_native_lineage_batch_digest(
    schema_version: u16,
    implementation: ProviderNativeLineageImplementation,
    canonical_content_digest: EvidenceDigest,
    rows: impl ExactSizeIterator<
        Item = Result<ProviderMarketEventNativeLineageDigestRow, ProviderCaptureError>,
    >,
    batch_sidecar: Option<ProviderMarketEventNativeLineageSidecarDigestEvidence>,
) -> Result<EvidenceDigest, ProviderCaptureError> {
    let mut digest = Sha256::new();
    hash_binding_field(&mut digest, PROVIDER_MARKET_EVENT_NATIVE_DOMAIN)?;
    digest.update(schema_version.to_be_bytes());
    digest.update([implementation.tag()]);
    hash_digest(&mut digest, canonical_content_digest);
    hash_binding_length(&mut digest, rows.len())?;
    for row in rows {
        let row = row?;
        hash_binding_length(
            &mut digest,
            usize::try_from(row.canonical_row_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
        )?;
        hash_digest(&mut digest, row.canonical_event_digest);
        hash_binding_length(&mut digest, row.native_semantic_bytes)?;
        hash_digest(&mut digest, row.native_semantic_digest);
    }
    match batch_sidecar {
        Some(sidecar) => {
            digest.update([1]);
            hash_binding_length(&mut digest, sidecar.native_semantic_bytes)?;
            hash_digest(&mut digest, sidecar.native_semantic_digest);
        }
        None => digest.update([0]),
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

/// Exact canonical-row mapping to one logical live-event frame and immutable physical frame.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderEventMicrobatchRowFrame {
    canonical_row_ordinal: u32,
    event_frame_ordinal: u16,
    physical_frame_ordinal: u32,
    event_id: [u8; 16],
    connection_id: [u8; 16],
    payload_digest: EvidenceDigest,
    exchange_at: Option<Timestamp>,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl ProviderEventMicrobatchRowFrame {
    /// Returns the contiguous zero-based canonical row ordinal.
    pub const fn canonical_row_ordinal(&self) -> u32 {
        self.canonical_row_ordinal
    }

    /// Returns the exact logical frame ordinal inside the application-defined microbatch.
    pub const fn event_frame_ordinal(&self) -> u16 {
        self.event_frame_ordinal
    }

    /// Returns the exact immutable journal frame ordinal.
    pub const fn physical_frame_ordinal(&self) -> u32 {
        self.physical_frame_ordinal
    }

    /// Returns the exact locally assigned event UUID bytes.
    pub const fn event_id(&self) -> [u8; 16] {
        self.event_id
    }

    /// Returns the exact connection-generation UUID bytes.
    pub const fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    /// Returns SHA-256 of the exact provider frame bytes.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns source-authored event time when supplied by the stream.
    pub const fn exchange_at(&self) -> Option<Timestamp> {
        self.exchange_at
    }

    /// Returns the exact socket-boundary receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the provider/source sequence when supplied by the stream.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }
}

/// Checked value-only restart projection of one canonical-row to live-event-frame mapping.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderEventMicrobatchRowFrameEvidence {
    canonical_row_ordinal: u32,
    event_frame_ordinal: u16,
    physical_frame_ordinal: u32,
    event_id: [u8; 16],
    connection_id: [u8; 16],
    payload_digest: EvidenceDigest,
    exchange_at: Option<Timestamp>,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl ProviderEventMicrobatchRowFrameEvidence {
    /// Validates one bounded persisted live-event row/frame coordinate.
    #[allow(
        clippy::too_many_arguments,
        reason = "persisted live-event row/frame evidence remains explicit"
    )]
    pub fn try_new(
        canonical_row_ordinal: u32,
        event_frame_ordinal: u16,
        physical_frame_ordinal: u32,
        event_id: [u8; 16],
        connection_id: [u8; 16],
        payload_digest: EvidenceDigest,
        exchange_at: Option<Timestamp>,
        received_at: Timestamp,
        source_sequence: Option<u64>,
    ) -> Result<Self, ProviderCaptureError> {
        if usize::try_from(canonical_row_ordinal)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            >= MAX_EXTRACTION_RECORDS
            || usize::from(event_frame_ordinal) >= MAX_PROVIDER_EVENT_MICROBATCH_FRAMES
            || usize::try_from(physical_frame_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
                >= MAX_PROVIDER_EVENT_MICROBATCH_FRAMES
            || event_id == [0; 16]
            || connection_id == [0; 16]
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        require_sha256_identity(payload_digest)?;
        Ok(Self {
            canonical_row_ordinal,
            event_frame_ordinal,
            physical_frame_ordinal,
            event_id,
            connection_id,
            payload_digest,
            exchange_at,
            received_at,
            source_sequence,
        })
    }

    fn digest_projection(&self) -> ProviderEventMicrobatchRowFrameDigestProjection {
        ProviderEventMicrobatchRowFrameDigestProjection {
            canonical_row_ordinal: self.canonical_row_ordinal,
            event_frame_ordinal: self.event_frame_ordinal,
            physical_frame_ordinal: self.physical_frame_ordinal,
            event_id: self.event_id,
            connection_id: self.connection_id,
            payload_digest: self.payload_digest,
            exchange_at: self.exchange_at,
            received_at: self.received_at,
            source_sequence: self.source_sequence,
        }
    }
}

/// Copyable evidence identity of one complete sealed live-event publication binding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderEventMicrobatchBindingDigest(EvidenceDigest);

impl ProviderEventMicrobatchBindingDigest {
    /// Returns the algorithm-qualified digest for durable evidence persistence.
    pub const fn evidence(self) -> EvidenceDigest {
        self.0
    }

    /// Verifies persisted value evidence without reconstructing live publication authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "persisted live-event publication evidence remains explicit"
    )]
    pub fn verify_evidence(
        expected_digest: EvidenceDigest,
        capture: &ProviderEventMicrobatchReceipt,
        sealed_receipt_digest: EvidenceDigest,
        physical_claim: &SealedResearchJournalSegmentClaim,
        canonical_schema_fingerprint: EvidenceDigest,
        canonical_content_digest: EvidenceDigest,
        canonical_event_count: usize,
        native_implementation: ProviderNativeLineageImplementation,
        native_batch_digest: EvidenceDigest,
        native_row_count: usize,
        row_frames: &[ProviderEventMicrobatchRowFrameEvidence],
    ) -> Result<(), ProviderCaptureError> {
        require_sha256_identity(expected_digest)?;
        let header = ProviderEventMicrobatchBindingDigestHeader {
            sealed_receipt_digest,
            canonical_schema_fingerprint,
            canonical_content_digest,
            canonical_event_count,
            native_implementation,
            native_batch_digest,
            native_row_count,
        };
        validate_provider_event_microbatch_binding_evidence(
            capture,
            header,
            row_frames,
            physical_claim,
        )?;
        let observed = compute_provider_event_microbatch_binding_digest(
            capture,
            header,
            row_frames.len(),
            |ordinal| {
                row_frames
                    .get(ordinal)
                    .map(ProviderEventMicrobatchRowFrameEvidence::digest_projection)
            },
            physical_claim,
        )?;
        if observed.evidence() != expected_digest {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

/// Non-reusable publication authority for one canonical batch derived from a sealed event batch.
#[derive(Debug)]
pub struct SealedProviderEventMicrobatchBinding {
    authority: ProviderEventMicrobatchToken,
    batch: ProviderMarketEventBatch,
    native_lineage: ProviderMarketEventNativeLineageBatch,
    content_identity: ProviderMarketEventContentIdentity,
    row_frames: Box<[ProviderEventMicrobatchRowFrame]>,
    evidence_digest: ProviderEventMicrobatchBindingDigest,
}

impl SealedProviderEventMicrobatchBinding {
    /// Consumes one sealed event token into canonical/native publication authority.
    pub fn try_new(
        authority: ProviderEventMicrobatchToken,
        batch: ProviderMarketEventBatch,
        native_lineage: ProviderMarketEventNativeLineageBatch,
        row_event_frame_ordinals: Vec<u16>,
    ) -> Result<Self, ProviderCaptureError> {
        validate_event_microbatch_batch(&authority, &batch)?;
        native_lineage
            .validate(&batch)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
        let content_identity = batch.content_identity();
        let row_frames = event_microbatch_row_frames(
            authority.persisted_receipt(),
            &batch,
            &row_event_frame_ordinals,
        )?;
        let evidence_digest = provider_event_microbatch_binding_digest(
            &authority,
            content_identity,
            &native_lineage,
            &row_frames,
        )?;
        Ok(Self {
            authority,
            batch,
            native_lineage,
            content_identity,
            row_frames,
            evidence_digest,
        })
    }

    /// Revalidates canonical/native alignment and exact event/physical-frame coordinates.
    pub fn validate(&self) -> Result<(), ProviderCaptureError> {
        validate_event_microbatch_batch(&self.authority, &self.batch)?;
        if self.native_lineage.validate(&self.batch).is_err()
            || self.content_identity != self.batch.content_identity()
            || self.row_frames.len() != self.batch.events().len()
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let ordinals = self
            .row_frames
            .iter()
            .map(|frame| frame.event_frame_ordinal)
            .collect::<Vec<_>>();
        let expected = event_microbatch_row_frames(
            self.authority.persisted_receipt(),
            &self.batch,
            &ordinals,
        )?;
        if expected.as_ref() != self.row_frames.as_ref()
            || provider_event_microbatch_binding_digest(
                &self.authority,
                self.content_identity,
                &self.native_lineage,
                &self.row_frames,
            )? != self.evidence_digest
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }

    /// Returns the exact canonical batch retained inside this one-use authority.
    pub const fn batch(&self) -> &ProviderMarketEventBatch {
        &self.batch
    }

    /// Returns exact row-aligned provider-native evidence, including its optional sidecar.
    pub const fn native_lineage(&self) -> &ProviderMarketEventNativeLineageBatch {
        &self.native_lineage
    }

    /// Returns the recomputable semantic identity of the canonical batch.
    pub const fn content_identity(&self) -> ProviderMarketEventContentIdentity {
        self.content_identity
    }

    /// Returns the exact canonical record count.
    pub fn record_count(&self) -> usize {
        self.row_frames.len()
    }

    /// Returns exact canonical-row to logical-event/physical-frame coordinates.
    pub const fn row_frames(&self) -> &[ProviderEventMicrobatchRowFrame] {
        &self.row_frames
    }

    /// Returns the cached, recomputable complete event-binding digest.
    pub const fn evidence_digest(&self) -> ProviderEventMicrobatchBindingDigest {
        self.evidence_digest
    }

    /// Returns cloneable logical event evidence; it cannot remint live authority.
    pub const fn capture_evidence(&self) -> &ProviderEventMicrobatchReceipt {
        self.authority.persisted_receipt().capture()
    }

    /// Returns the digest joining logical event observations to their physical object.
    pub const fn sealed_receipt_digest(&self) -> EvidenceDigest {
        self.authority.persisted_receipt().receipt_digest()
    }

    /// Returns exact persisted logical and physical event evidence.
    pub const fn persisted_receipt(&self) -> &SealedProviderEventMicrobatchReceipt {
        self.authority.persisted_receipt()
    }
}

/// Checked value-only restart projection of one typed HTTP-response market-event row.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderResponseMarketEventRowFrameEvidence {
    canonical_row_ordinal: u32,
    capture_page_ordinal: u16,
    segment_ordinal: u16,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl ProviderResponseMarketEventRowFrameEvidence {
    /// Validates one exact canonical-event to HTTP-page/physical-frame coordinate.
    #[allow(
        clippy::too_many_arguments,
        reason = "persisted typed response-event row evidence remains explicit"
    )]
    pub fn try_new(
        canonical_row_ordinal: u32,
        capture_page_ordinal: u16,
        segment_ordinal: u16,
        physical_frame_ordinal: u32,
        page_body_digest: EvidenceDigest,
        received_at: Timestamp,
        source_sequence: Option<u64>,
    ) -> Result<Self, ProviderCaptureError> {
        if usize::try_from(canonical_row_ordinal)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            >= MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
            || usize::from(capture_page_ordinal) >= MAX_PROVIDER_CAPTURE_PAGES
            || segment_ordinal != 0
            || usize::try_from(physical_frame_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
                >= MAX_PROVIDER_CAPTURE_PAGES
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        require_sha256_identity(page_body_digest)?;
        Ok(Self {
            canonical_row_ordinal,
            capture_page_ordinal,
            segment_ordinal,
            physical_frame_ordinal,
            page_body_digest,
            received_at,
            source_sequence,
        })
    }

    fn digest_projection(&self) -> ProviderCaptureRowFrameDigestProjection {
        ProviderCaptureRowFrameDigestProjection {
            canonical_row_ordinal: self.canonical_row_ordinal,
            capture_page_ordinal: self.capture_page_ordinal,
            segment_ordinal: self.segment_ordinal,
            physical_frame_ordinal: self.physical_frame_ordinal,
            page_body_digest: self.page_body_digest,
            received_at: self.received_at,
            source_sequence: self.source_sequence,
        }
    }
}

/// Copyable evidence identity of one sealed typed HTTP-response market-event binding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderResponseMarketEventBindingDigest(EvidenceDigest);

impl ProviderResponseMarketEventBindingDigest {
    /// Returns the durable algorithm-qualified evidence digest.
    pub const fn evidence(self) -> EvidenceDigest {
        self.0
    }

    /// Verifies persisted value evidence without reconstructing live publication authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "persisted typed response-event evidence remains explicit"
    )]
    pub fn verify_evidence(
        expected: EvidenceDigest,
        capture: &ProviderCaptureSetReceipt,
        sealed_receipt_digest: EvidenceDigest,
        physical_claim: &SealedResearchJournalSegmentClaim,
        canonical_schema_fingerprint: EvidenceDigest,
        canonical_content_digest: EvidenceDigest,
        canonical_event_count: usize,
        native_implementation: ProviderNativeLineageImplementation,
        native_batch_digest: EvidenceDigest,
        native_row_count: usize,
        row_frames: &[ProviderResponseMarketEventRowFrameEvidence],
    ) -> Result<(), ProviderCaptureError> {
        require_sha256_identity(expected)?;
        let header = ProviderResponseMarketEventBindingDigestHeader {
            sealed_receipt_digest,
            canonical_schema_fingerprint,
            canonical_content_digest,
            canonical_event_count,
            native_implementation,
            native_batch_digest,
            native_row_count,
        };
        validate_provider_response_market_event_binding_evidence(
            capture,
            header,
            row_frames,
            physical_claim,
        )?;
        let observed = compute_provider_response_market_event_binding_digest(
            capture,
            header,
            row_frames.len(),
            |ordinal| {
                row_frames
                    .get(ordinal)
                    .map(ProviderResponseMarketEventRowFrameEvidence::digest_projection)
            },
            physical_claim,
        )?;
        if observed.evidence() != expected {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

/// Non-reusable typed market-event authority derived from one sealed HTTP response set.
#[derive(Debug)]
pub struct SealedProviderResponseMarketEventBinding {
    authority: ProviderWholeCaptureToken,
    batch: ProviderMarketEventBatch,
    native_lineage: ProviderMarketEventNativeLineageBatch,
    content_identity: ProviderMarketEventContentIdentity,
    row_frames: Box<[ProviderCaptureRowFrame]>,
    evidence_digest: ProviderResponseMarketEventBindingDigest,
}

impl SealedProviderResponseMarketEventBinding {
    /// Consumes a whole sealed HTTP response into typed canonical market-event authority.
    pub fn try_new(
        authority: ProviderWholeCaptureToken,
        batch: ProviderMarketEventBatch,
        native_lineage: ProviderMarketEventNativeLineageBatch,
        row_capture_page_ordinals: Vec<u16>,
    ) -> Result<Self, ProviderCaptureError> {
        validate_response_market_event_batch(&authority, &batch)?;
        native_lineage
            .validate(&batch)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
        let content_identity = batch.content_identity();
        let row_frames = response_market_event_row_frames(
            authority.persisted_receipt(),
            &batch,
            &row_capture_page_ordinals,
        )?;
        let evidence_digest = provider_response_market_event_binding_digest(
            &authority,
            content_identity,
            &native_lineage,
            &row_frames,
        )?;
        Ok(Self {
            authority,
            batch,
            native_lineage,
            content_identity,
            row_frames,
            evidence_digest,
        })
    }

    /// Revalidates canonical/native alignment and exact HTTP-page/physical-frame coordinates.
    pub fn validate(&self) -> Result<(), ProviderCaptureError> {
        validate_response_market_event_batch(&self.authority, &self.batch)?;
        if self.native_lineage.validate(&self.batch).is_err()
            || self.content_identity != self.batch.content_identity()
            || self.row_frames.len() != self.batch.events().len()
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let ordinals = self
            .row_frames
            .iter()
            .map(|row| row.capture_page_ordinal)
            .collect::<Vec<_>>();
        let expected = response_market_event_row_frames(
            self.authority.persisted_receipt(),
            &self.batch,
            &ordinals,
        )?;
        if expected.as_ref() != self.row_frames.as_ref()
            || provider_response_market_event_binding_digest(
                &self.authority,
                self.content_identity,
                &self.native_lineage,
                &self.row_frames,
            )? != self.evidence_digest
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }

    /// Returns the exact typed canonical market-event batch.
    pub const fn batch(&self) -> &ProviderMarketEventBatch {
        &self.batch
    }

    /// Returns exact row-aligned provider-native evidence.
    pub const fn native_lineage(&self) -> &ProviderMarketEventNativeLineageBatch {
        &self.native_lineage
    }

    /// Returns the recomputable canonical content identity.
    pub const fn content_identity(&self) -> ProviderMarketEventContentIdentity {
        self.content_identity
    }

    /// Returns the exact canonical event count.
    pub fn record_count(&self) -> usize {
        self.row_frames.len()
    }

    /// Returns canonical-row to logical HTTP-page/physical-frame coordinates.
    pub const fn row_frames(&self) -> &[ProviderCaptureRowFrame] {
        &self.row_frames
    }

    /// Returns the cached, recomputable complete binding digest.
    pub const fn evidence_digest(&self) -> ProviderResponseMarketEventBindingDigest {
        self.evidence_digest
    }

    /// Returns cloneable logical HTTP response evidence without reminting live authority.
    pub fn capture_evidence(&self) -> &ProviderCaptureSetReceipt {
        self.authority.persisted_receipt().capture()
    }

    /// Returns the digest joining the logical HTTP response to its immutable physical object.
    pub fn sealed_receipt_digest(&self) -> EvidenceDigest {
        self.authority.persisted_receipt().receipt_digest()
    }

    /// Returns exact persisted logical and physical HTTP response evidence.
    pub fn persisted_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        self.authority.persisted_receipt()
    }
}

const PROVIDER_RESPONSE_MARKET_EVENT_BINDING_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/sealed-provider-response-market-event-binding/evidence/v1";
const PROVIDER_RESPONSE_MARKET_EVENT_BINDING_DIGEST_VERSION: u16 = 1;

#[derive(Clone, Copy)]
struct ProviderResponseMarketEventBindingDigestHeader {
    sealed_receipt_digest: EvidenceDigest,
    canonical_schema_fingerprint: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    canonical_event_count: usize,
    native_implementation: ProviderNativeLineageImplementation,
    native_batch_digest: EvidenceDigest,
    native_row_count: usize,
}

fn validate_response_market_event_batch(
    authority: &ProviderWholeCaptureToken,
    batch: &ProviderMarketEventBatch,
) -> Result<(), ProviderCaptureError> {
    batch.validate()?;
    let persisted = authority.persisted_receipt();
    let capture = persisted.capture();
    if persisted.receipt_digest().bytes() == [0; 32]
        || batch.source_id() != capture.source_id()
        || batch.metadata_revision() != capture.metadata_revision()
        || batch.dataset() != capture.dataset()
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    Ok(())
}

fn response_market_event_row_frames(
    receipt: &SealedProviderCaptureSetReceipt,
    batch: &ProviderMarketEventBatch,
    row_capture_page_ordinals: &[u16],
) -> Result<Box<[ProviderCaptureRowFrame]>, ProviderCaptureError> {
    if row_capture_page_ordinals.len() != batch.events().len() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut mappings = Vec::new();
    mappings
        .try_reserve_exact(row_capture_page_ordinals.len())
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    for (row_ordinal, (event, capture_page_ordinal)) in batch
        .events()
        .iter()
        .zip(row_capture_page_ordinals.iter().copied())
        .enumerate()
    {
        let page_index = usize::from(capture_page_ordinal);
        let page = receipt
            .capture()
            .pages()
            .get(page_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let frame = receipt
            .segment()
            .frames()
            .get(page_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let provenance = market_event_provenance(event);
        if page.ordinal() != capture_page_ordinal
            || provenance.binding().payload_digest() != page.body_digest()
            || provenance.received_at() != page.received_at()
            || frame.ordinal() != u32::from(capture_page_ordinal)
            || frame.provider_payload_bytes() != page.body_bytes()
            || frame.provider_payload_digest() != page.body_digest()
            || frame.received_at() != page.received_at()
            || frame.source_sequence() != Some(u64::from(capture_page_ordinal))
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        mappings.push(ProviderCaptureRowFrame {
            canonical_row_ordinal: u32::try_from(row_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            capture_page_ordinal,
            segment_ordinal: 0,
            physical_frame_ordinal: frame.ordinal(),
            page_body_digest: page.body_digest(),
            received_at: page.received_at(),
            source_sequence: frame.source_sequence(),
        });
    }
    Ok(mappings.into_boxed_slice())
}

fn provider_response_market_event_binding_digest(
    authority: &ProviderWholeCaptureToken,
    content_identity: ProviderMarketEventContentIdentity,
    native_lineage: &ProviderMarketEventNativeLineageBatch,
    row_frames: &[ProviderCaptureRowFrame],
) -> Result<ProviderResponseMarketEventBindingDigest, ProviderCaptureError> {
    let persisted = authority.persisted_receipt();
    compute_provider_response_market_event_binding_digest(
        persisted.capture(),
        ProviderResponseMarketEventBindingDigestHeader {
            sealed_receipt_digest: persisted.receipt_digest(),
            canonical_schema_fingerprint: content_identity.schema_fingerprint(),
            canonical_content_digest: content_identity.content_digest(),
            canonical_event_count: content_identity.event_count(),
            native_implementation: native_lineage.implementation(),
            native_batch_digest: native_lineage.batch_digest(),
            native_row_count: native_lineage.rows().len(),
        },
        row_frames.len(),
        |ordinal| {
            row_frames
                .get(ordinal)
                .map(ProviderCaptureRowFrameDigestProjection::from)
        },
        persisted.segment().claim(),
    )
}

fn compute_provider_response_market_event_binding_digest<RowAt>(
    capture: &ProviderCaptureSetReceipt,
    header: ProviderResponseMarketEventBindingDigestHeader,
    row_frame_count: usize,
    mut row_frame_at: RowAt,
    physical_claim: &SealedResearchJournalSegmentClaim,
) -> Result<ProviderResponseMarketEventBindingDigest, ProviderCaptureError>
where
    RowAt: FnMut(usize) -> Option<ProviderCaptureRowFrameDigestProjection>,
{
    let mut digest = Sha256::new();
    hash_binding_field(
        &mut digest,
        PROVIDER_RESPONSE_MARKET_EVENT_BINDING_DIGEST_DOMAIN,
    )?;
    digest.update(PROVIDER_RESPONSE_MARKET_EVENT_BINDING_DIGEST_VERSION.to_be_bytes());
    hash_provider_capture_receipt_for_binding(&mut digest, capture)?;
    hash_digest(&mut digest, header.sealed_receipt_digest);
    hash_digest(&mut digest, header.canonical_schema_fingerprint);
    hash_digest(&mut digest, header.canonical_content_digest);
    hash_binding_length(&mut digest, header.canonical_event_count)?;
    digest.update([header.native_implementation.tag()]);
    hash_digest(&mut digest, header.native_batch_digest);
    hash_binding_length(&mut digest, header.native_row_count)?;
    hash_binding_length(&mut digest, row_frame_count)?;
    for ordinal in 0..row_frame_count {
        let row = row_frame_at(ordinal).ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        digest.update(row.canonical_row_ordinal.to_be_bytes());
        digest.update(row.capture_page_ordinal.to_be_bytes());
        digest.update(row.segment_ordinal.to_be_bytes());
        digest.update(row.physical_frame_ordinal.to_be_bytes());
        hash_digest(&mut digest, row.page_body_digest);
        digest.update(row.received_at.unix_nanos().to_be_bytes());
        hash_binding_optional_u64(&mut digest, row.source_sequence);
    }
    hash_provider_capture_physical_claim(
        &mut digest,
        capture.content_digest(),
        capture.observation_digest(),
        header.sealed_receipt_digest,
        physical_claim,
    )?;
    Ok(ProviderResponseMarketEventBindingDigest(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
    ))
}

fn validate_provider_response_market_event_binding_evidence(
    capture: &ProviderCaptureSetReceipt,
    header: ProviderResponseMarketEventBindingDigestHeader,
    row_frames: &[ProviderResponseMarketEventRowFrameEvidence],
    physical_claim: &SealedResearchJournalSegmentClaim,
) -> Result<(), ProviderCaptureError> {
    require_sha256_identity(header.sealed_receipt_digest)?;
    require_sha256_identity(header.canonical_schema_fingerprint)?;
    require_sha256_identity(header.canonical_content_digest)?;
    require_sha256_identity(header.native_batch_digest)?;
    require_sha256_identity(physical_claim.content_digest())?;
    require_sha256_identity(physical_claim.physical_receipt_digest())?;
    if header.canonical_schema_fingerprint != provider_market_event_schema_fingerprint()
        || header.canonical_event_count == 0
        || header.canonical_event_count != header.native_row_count
        || row_frames.len() != header.canonical_event_count
        || row_frames.len() > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
        || physical_claim.frames().len() != capture.pages().len()
        || physical_claim.frames().len() > MAX_PROVIDER_CAPTURE_PAGES
        || sealed_provider_capture_receipt_digest(
            capture.observation_digest(),
            physical_claim.physical_receipt_digest(),
        ) != header.sealed_receipt_digest
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    for (page, frame) in capture.pages().iter().zip(physical_claim.frames()) {
        if frame.ordinal() != u32::from(page.ordinal())
            || frame.provider_payload_bytes() != page.body_bytes()
            || frame.provider_payload_digest() != page.body_digest()
            || frame.received_at() != page.received_at()
            || frame.source_sequence() != Some(u64::from(page.ordinal()))
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    for (expected_ordinal, row) in row_frames.iter().enumerate() {
        let page = capture
            .pages()
            .get(usize::from(row.capture_page_ordinal))
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let frame = physical_claim
            .frames()
            .get(
                usize::try_from(row.physical_frame_ordinal)
                    .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            )
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if row.canonical_row_ordinal
            != u32::try_from(expected_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            || row.segment_ordinal != 0
            || row.physical_frame_ordinal != u32::from(row.capture_page_ordinal)
            || row.page_body_digest != page.body_digest()
            || row.received_at != page.received_at()
            || row.source_sequence != Some(u64::from(row.capture_page_ordinal))
            || frame.ordinal() != row.physical_frame_ordinal
            || frame.provider_payload_digest() != row.page_body_digest
            || frame.received_at() != row.received_at
            || frame.source_sequence() != row.source_sequence
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    Ok(())
}

const PROVIDER_EVENT_MICROBATCH_BINDING_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/sealed-provider-event-microbatch-binding/evidence/v1";
const PROVIDER_EVENT_MICROBATCH_BINDING_DIGEST_VERSION: u16 = 1;

#[derive(Clone, Copy)]
struct ProviderEventMicrobatchBindingDigestHeader {
    sealed_receipt_digest: EvidenceDigest,
    canonical_schema_fingerprint: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    canonical_event_count: usize,
    native_implementation: ProviderNativeLineageImplementation,
    native_batch_digest: EvidenceDigest,
    native_row_count: usize,
}

#[derive(Clone, Copy)]
struct ProviderEventMicrobatchRowFrameDigestProjection {
    canonical_row_ordinal: u32,
    event_frame_ordinal: u16,
    physical_frame_ordinal: u32,
    event_id: [u8; 16],
    connection_id: [u8; 16],
    payload_digest: EvidenceDigest,
    exchange_at: Option<Timestamp>,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl From<&ProviderEventMicrobatchRowFrame> for ProviderEventMicrobatchRowFrameDigestProjection {
    fn from(frame: &ProviderEventMicrobatchRowFrame) -> Self {
        Self {
            canonical_row_ordinal: frame.canonical_row_ordinal,
            event_frame_ordinal: frame.event_frame_ordinal,
            physical_frame_ordinal: frame.physical_frame_ordinal,
            event_id: frame.event_id,
            connection_id: frame.connection_id,
            payload_digest: frame.payload_digest,
            exchange_at: frame.exchange_at,
            received_at: frame.received_at,
            source_sequence: frame.source_sequence,
        }
    }
}

fn validate_event_microbatch_batch(
    token: &ProviderEventMicrobatchToken,
    batch: &ProviderMarketEventBatch,
) -> Result<(), ProviderCaptureError> {
    batch.validate()?;
    let receipt = token.persisted_receipt();
    let capture = receipt.capture();
    if receipt.receipt_digest().bytes() == [0; 32]
        || batch.source_id() != capture.source_id()
        || batch.metadata_revision() != capture.metadata_revision()
        || batch.dataset() != capture.dataset()
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    Ok(())
}

fn event_microbatch_row_frames(
    receipt: &SealedProviderEventMicrobatchReceipt,
    batch: &ProviderMarketEventBatch,
    row_event_frame_ordinals: &[u16],
) -> Result<Box<[ProviderEventMicrobatchRowFrame]>, ProviderCaptureError> {
    if row_event_frame_ordinals.len() != batch.events().len() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut mappings = Vec::new();
    mappings
        .try_reserve_exact(row_event_frame_ordinals.len())
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    for (row_ordinal, event_frame_ordinal) in row_event_frame_ordinals.iter().copied().enumerate() {
        let frame_index = usize::from(event_frame_ordinal);
        let event = receipt
            .capture()
            .frames()
            .get(frame_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let physical = receipt
            .segment()
            .frames()
            .get(frame_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if event.ordinal() != event_frame_ordinal
            || physical.ordinal() != u32::from(event_frame_ordinal)
            || physical.provider_payload_bytes() != event.payload_bytes()
            || physical.provider_payload_digest() != event.payload_digest()
            || physical.received_at() != event.received_at()
            || physical.source_sequence() != event.source_sequence()
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        mappings.push(ProviderEventMicrobatchRowFrame {
            canonical_row_ordinal: u32::try_from(row_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            event_frame_ordinal,
            physical_frame_ordinal: physical.ordinal(),
            event_id: event.event_id(),
            connection_id: event.connection_id(),
            payload_digest: event.payload_digest(),
            exchange_at: event.exchange_at(),
            received_at: event.received_at(),
            source_sequence: event.source_sequence(),
        });
    }
    Ok(mappings.into_boxed_slice())
}

fn provider_event_microbatch_binding_digest(
    authority: &ProviderEventMicrobatchToken,
    content_identity: ProviderMarketEventContentIdentity,
    native_lineage: &ProviderMarketEventNativeLineageBatch,
    row_frames: &[ProviderEventMicrobatchRowFrame],
) -> Result<ProviderEventMicrobatchBindingDigest, ProviderCaptureError> {
    let receipt = authority.persisted_receipt();
    compute_provider_event_microbatch_binding_digest(
        receipt.capture(),
        ProviderEventMicrobatchBindingDigestHeader {
            sealed_receipt_digest: receipt.receipt_digest(),
            canonical_schema_fingerprint: content_identity.schema_fingerprint(),
            canonical_content_digest: content_identity.content_digest(),
            canonical_event_count: content_identity.event_count(),
            native_implementation: native_lineage.implementation(),
            native_batch_digest: native_lineage.batch_digest(),
            native_row_count: native_lineage.rows().len(),
        },
        row_frames.len(),
        |ordinal| {
            row_frames
                .get(ordinal)
                .map(ProviderEventMicrobatchRowFrameDigestProjection::from)
        },
        receipt.segment().claim(),
    )
}

fn compute_provider_event_microbatch_binding_digest<RowAt>(
    capture: &ProviderEventMicrobatchReceipt,
    header: ProviderEventMicrobatchBindingDigestHeader,
    row_frame_count: usize,
    mut row_frame_at: RowAt,
    physical_claim: &SealedResearchJournalSegmentClaim,
) -> Result<ProviderEventMicrobatchBindingDigest, ProviderCaptureError>
where
    RowAt: FnMut(usize) -> Option<ProviderEventMicrobatchRowFrameDigestProjection>,
{
    let mut digest = Sha256::new();
    hash_binding_field(&mut digest, PROVIDER_EVENT_MICROBATCH_BINDING_DIGEST_DOMAIN)?;
    digest.update(PROVIDER_EVENT_MICROBATCH_BINDING_DIGEST_VERSION.to_be_bytes());
    hash_binding_field(&mut digest, capture.source_id().as_str().as_bytes())?;
    hash_binding_field(
        &mut digest,
        capture
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_binding_field(&mut digest, capture.dataset().as_str().as_bytes())?;
    hash_binding_field(&mut digest, capture.stream_identity().as_str().as_bytes())?;
    digest.update(capture.total_payload_bytes().to_be_bytes());
    hash_digest(&mut digest, capture.content_digest());
    hash_digest(&mut digest, capture.observation_digest());
    hash_binding_length(&mut digest, capture.frames().len())?;
    for frame in capture.frames() {
        digest.update(frame.ordinal().to_be_bytes());
        digest.update(frame.event_id());
        digest.update(frame.connection_id());
        hash_binding_optional_u64(&mut digest, frame.source_sequence());
        hash_binding_optional_timestamp(&mut digest, frame.exchange_at());
        digest.update(frame.received_at().unix_nanos().to_be_bytes());
        digest.update(frame.payload_bytes().to_be_bytes());
        hash_digest(&mut digest, frame.payload_digest());
    }
    hash_digest(&mut digest, header.sealed_receipt_digest);
    hash_digest(&mut digest, header.canonical_schema_fingerprint);
    hash_digest(&mut digest, header.canonical_content_digest);
    hash_binding_length(&mut digest, header.canonical_event_count)?;
    digest.update([header.native_implementation.tag()]);
    hash_digest(&mut digest, header.native_batch_digest);
    hash_binding_length(&mut digest, header.native_row_count)?;
    hash_binding_length(&mut digest, row_frame_count)?;
    for ordinal in 0..row_frame_count {
        let frame = row_frame_at(ordinal).ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        digest.update(frame.canonical_row_ordinal.to_be_bytes());
        digest.update(frame.event_frame_ordinal.to_be_bytes());
        digest.update(frame.physical_frame_ordinal.to_be_bytes());
        digest.update(frame.event_id);
        digest.update(frame.connection_id);
        hash_digest(&mut digest, frame.payload_digest);
        hash_binding_optional_timestamp(&mut digest, frame.exchange_at);
        digest.update(frame.received_at.unix_nanos().to_be_bytes());
        hash_binding_optional_u64(&mut digest, frame.source_sequence);
    }
    hash_binding_field(&mut digest, physical_claim.relative_reference().as_bytes())?;
    hash_digest(&mut digest, physical_claim.content_digest());
    digest.update(physical_claim.size_bytes().to_be_bytes());
    hash_digest(&mut digest, physical_claim.physical_receipt_digest());
    hash_binding_length(&mut digest, physical_claim.frames().len())?;
    for frame in physical_claim.frames() {
        digest.update(frame.ordinal().to_be_bytes());
        digest.update(frame.offset().to_be_bytes());
        digest.update(frame.framed_bytes().to_be_bytes());
        digest.update(frame.provider_payload_bytes().to_be_bytes());
        hash_digest(&mut digest, frame.provider_payload_digest());
        digest.update(frame.received_at().unix_nanos().to_be_bytes());
        hash_binding_optional_u64(&mut digest, frame.source_sequence());
    }
    Ok(ProviderEventMicrobatchBindingDigest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    )))
}

fn validate_provider_event_microbatch_binding_evidence(
    capture: &ProviderEventMicrobatchReceipt,
    header: ProviderEventMicrobatchBindingDigestHeader,
    row_frames: &[ProviderEventMicrobatchRowFrameEvidence],
    physical_claim: &SealedResearchJournalSegmentClaim,
) -> Result<(), ProviderCaptureError> {
    capture.validate()?;
    require_sha256_identity(header.sealed_receipt_digest)?;
    require_sha256_identity(header.canonical_schema_fingerprint)?;
    require_sha256_identity(header.canonical_content_digest)?;
    require_sha256_identity(header.native_batch_digest)?;
    require_sha256_identity(physical_claim.content_digest())?;
    require_sha256_identity(physical_claim.physical_receipt_digest())?;
    if header.canonical_schema_fingerprint != provider_market_event_schema_fingerprint()
        || header.canonical_event_count != header.native_row_count
        || row_frames.len() != header.canonical_event_count
        || row_frames.len() > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
        || physical_claim.frames().len() != capture.frames().len()
        || physical_claim.frames().len() > MAX_PROVIDER_EVENT_MICROBATCH_FRAMES
        || sealed_provider_event_microbatch_receipt_digest(
            capture.observation_digest(),
            physical_claim.physical_receipt_digest(),
        ) != header.sealed_receipt_digest
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    for (event, physical) in capture.frames().iter().zip(physical_claim.frames()) {
        if physical.ordinal() != u32::from(event.ordinal())
            || physical.provider_payload_bytes() != event.payload_bytes()
            || physical.provider_payload_digest() != event.payload_digest()
            || physical.received_at() != event.received_at()
            || physical.source_sequence() != event.source_sequence()
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    for (expected_row, row) in row_frames.iter().enumerate() {
        let event = capture
            .frames()
            .get(usize::from(row.event_frame_ordinal))
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let physical = physical_claim
            .frames()
            .get(
                usize::try_from(row.physical_frame_ordinal)
                    .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            )
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if row.canonical_row_ordinal
            != u32::try_from(expected_row)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            || row.physical_frame_ordinal != u32::from(row.event_frame_ordinal)
            || row.event_id != event.event_id()
            || row.connection_id != event.connection_id()
            || row.payload_digest != event.payload_digest()
            || row.exchange_at != event.exchange_at()
            || row.received_at != event.received_at()
            || row.source_sequence != event.source_sequence()
            || physical.ordinal() != row.physical_frame_ordinal
            || physical.provider_payload_digest() != row.payload_digest
            || physical.received_at() != row.received_at
            || physical.source_sequence() != row.source_sequence
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    Ok(())
}

fn hash_provider_capture_receipt_for_binding(
    digest: &mut Sha256,
    root_capture: &ProviderCaptureSetReceipt,
) -> Result<(), ProviderCaptureError> {
    hash_binding_field(digest, root_capture.source_id().as_str().as_bytes())?;
    hash_binding_field(
        digest,
        root_capture
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_binding_field(digest, root_capture.dataset().as_str().as_bytes())?;
    hash_digest(digest, root_capture.request_set_identity());
    hash_binding_field(digest, root_capture.terminal().tag())?;
    digest.update(root_capture.total_body_bytes().to_be_bytes());
    hash_digest(digest, root_capture.content_digest());
    hash_digest(digest, root_capture.observation_digest());

    hash_binding_length(digest, root_capture.pages().len())?;
    for page in root_capture.pages() {
        digest.update(page.ordinal().to_be_bytes());
        hash_digest(digest, page.request_identity());
        hash_optional_digest(digest, page.request_page_token_digest());
        hash_optional_digest(digest, page.response_next_page_token_digest());
        digest.update(page.http_status().to_be_bytes());
        digest.update(page.body_bytes().to_be_bytes());
        hash_digest(digest, page.body_digest());
        digest.update(page.received_at().unix_nanos().to_be_bytes());
    }

    hash_binding_length(digest, root_capture.request_graph_components().len())?;
    for component in root_capture.request_graph_components() {
        digest.update(component.ordinal().to_be_bytes());
        hash_binding_field(digest, component.dataset().as_str().as_bytes())?;
        hash_digest(digest, component.request_set_identity());
        hash_binding_field(digest, component.terminal().tag())?;
        digest.update(component.first_page_ordinal().to_be_bytes());
        digest.update(component.page_count().get().to_be_bytes());
        digest.update(component.total_body_bytes().to_be_bytes());
        hash_digest(digest, component.content_digest());
        hash_digest(digest, component.observation_digest());
    }
    Ok(())
}

fn hash_provider_capture_physical_claim(
    digest: &mut Sha256,
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    claim: &SealedResearchJournalSegmentClaim,
) -> Result<(), ProviderCaptureError> {
    hash_digest(digest, capture_content_digest);
    hash_digest(digest, capture_observation_digest);
    hash_digest(digest, sealed_capture_receipt_digest);
    hash_binding_field(digest, claim.relative_reference().as_bytes())?;
    hash_digest(digest, claim.content_digest());
    digest.update(claim.size_bytes().to_be_bytes());
    hash_digest(digest, claim.physical_receipt_digest());
    hash_binding_length(digest, claim.frames().len())?;
    for frame in claim.frames() {
        digest.update(frame.ordinal().to_be_bytes());
        digest.update(frame.offset().to_be_bytes());
        digest.update(frame.framed_bytes().to_be_bytes());
        digest.update(frame.provider_payload_bytes().to_be_bytes());
        hash_digest(digest, frame.provider_payload_digest());
        digest.update(frame.received_at().unix_nanos().to_be_bytes());
        hash_binding_optional_u64(digest, frame.source_sequence());
    }
    Ok(())
}

fn hash_binding_optional_timestamp(digest: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn sealed_provider_event_microbatch_receipt_digest(
    observation_digest: EvidenceDigest,
    physical_receipt_digest: EvidenceDigest,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sealed-provider-event-microbatch-receipt/v1");
    hash_digest(&mut hash, observation_digest);
    hash_digest(&mut hash, physical_receipt_digest);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

/// Closed durable publication kind; response captures never masquerade as live-event batches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderPublicationBindingKind {
    /// Completed response set or request graph with HTTP/page semantics.
    ResponseSet,
    /// Typed canonical market events derived from a sealed HTTP response set.
    ResponseMarketEvent,
    /// Application-bounded ordered WebSocket/source-event frames with no HTTP completeness claim.
    EventMicrobatch,
    /// One sealed REST response snapshot followed by ordered live-event frames.
    CompositeResponseEvent,
}

/// One exact canonical row coordinate in a snapshot-plus-event publication.
#[derive(Clone, Copy, Debug)]
pub enum ProviderCompositeResponseEventRowCoordinate<'a> {
    /// Canonical snapshot row mapped to an exact response component/page and physical frame.
    ResponseSnapshot(&'a ProviderCaptureRowFrame),
    /// Canonical live row mapped to an exact event frame and physical frame.
    EventMicrobatch(&'a ProviderEventMicrobatchRowFrame),
}

/// Copyable evidence joining one exact response binding to one exact event binding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderCompositeResponseEventBindingDigest(EvidenceDigest);

impl ProviderCompositeResponseEventBindingDigest {
    /// Returns the durable algorithm-qualified evidence digest.
    pub const fn evidence(self) -> EvidenceDigest {
        self.0
    }

    /// Verifies the kind-preserving composition digest from independently verified sub-bindings.
    pub fn verify_evidence(
        expected: EvidenceDigest,
        response: EvidenceDigest,
        event: EvidenceDigest,
        response_row_count: usize,
        event_row_count: usize,
    ) -> Result<(), ProviderCaptureError> {
        require_sha256_identity(expected)?;
        if composite_response_event_binding_digest(
            response,
            event,
            response_row_count,
            event_row_count,
        )?
        .evidence()
            != expected
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

/// Non-cloneable authority joining a sealed response snapshot to sealed ordered live events.
#[derive(Debug)]
pub struct SealedProviderCompositeResponseEventBinding {
    response: SealedProviderResponseMarketEventBinding,
    event: SealedProviderEventMicrobatchBinding,
    evidence_digest: ProviderCompositeResponseEventBindingDigest,
}

impl SealedProviderCompositeResponseEventBinding {
    /// Joins independently sealed and canonical-bound response and event authorities.
    pub fn try_new(
        response: SealedProviderResponseMarketEventBinding,
        event: SealedProviderEventMicrobatchBinding,
    ) -> Result<Self, ProviderCaptureError> {
        response.validate()?;
        event.validate()?;
        if response.capture_evidence().source_id() != event.capture_evidence().source_id()
            || response.capture_evidence().metadata_revision()
                != event.capture_evidence().metadata_revision()
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let evidence_digest = composite_response_event_binding_digest(
            response.evidence_digest().evidence(),
            event.evidence_digest().evidence(),
            response.record_count(),
            event.record_count(),
        )?;
        Ok(Self {
            response,
            event,
            evidence_digest,
        })
    }

    /// Returns the complete response-set snapshot authority.
    pub const fn response(&self) -> &SealedProviderResponseMarketEventBinding {
        &self.response
    }

    /// Returns the complete ordered event-microbatch authority.
    pub const fn event(&self) -> &SealedProviderEventMicrobatchBinding {
        &self.event
    }

    /// Returns the kind-preserving digest joining both independently complete authorities.
    pub const fn evidence_digest(&self) -> ProviderCompositeResponseEventBindingDigest {
        self.evidence_digest
    }

    /// Iterates exact component/page coordinates first, then exact event-frame coordinates.
    pub fn row_coordinates(
        &self,
    ) -> impl Iterator<Item = ProviderCompositeResponseEventRowCoordinate<'_>> {
        self.response
            .row_frames()
            .iter()
            .map(ProviderCompositeResponseEventRowCoordinate::ResponseSnapshot)
            .chain(
                self.event
                    .row_frames()
                    .iter()
                    .map(ProviderCompositeResponseEventRowCoordinate::EventMicrobatch),
            )
    }
}

fn composite_response_event_binding_digest(
    response: EvidenceDigest,
    event: EvidenceDigest,
    response_row_count: usize,
    event_row_count: usize,
) -> Result<ProviderCompositeResponseEventBindingDigest, ProviderCaptureError> {
    let mut digest = Sha256::new();
    hash_binding_field(
        &mut digest,
        b"market-squawk/sealed-provider-composite-response-event-binding/v1",
    )?;
    require_sha256_identity(response)?;
    require_sha256_identity(event)?;
    hash_digest(&mut digest, response);
    hash_binding_length(&mut digest, response_row_count)?;
    hash_digest(&mut digest, event);
    hash_binding_length(&mut digest, event_row_count)?;
    Ok(ProviderCompositeResponseEventBindingDigest(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
    ))
}

/// Closed evidence identity for either admitted provider publication authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderPublicationBindingDigest {
    /// Evidence minted from completed response-set authority.
    ResponseSet(ProviderCaptureBindingDigest),
    /// Evidence minted from typed canonical events derived from a sealed HTTP response.
    ResponseMarketEvent(ProviderResponseMarketEventBindingDigest),
    /// Evidence minted from sealed event-microbatch authority.
    EventMicrobatch(ProviderEventMicrobatchBindingDigest),
    /// Evidence joining one exact response snapshot and one exact event microbatch.
    CompositeResponseEvent(ProviderCompositeResponseEventBindingDigest),
}

impl ProviderPublicationBindingDigest {
    /// Returns the durable algorithm-qualified evidence digest.
    pub const fn evidence(self) -> EvidenceDigest {
        match self {
            Self::ResponseSet(digest) => digest.evidence(),
            Self::ResponseMarketEvent(digest) => digest.evidence(),
            Self::EventMicrobatch(digest) => digest.evidence(),
            Self::CompositeResponseEvent(digest) => digest.evidence(),
        }
    }
}

/// Closed non-cloneable provider publication authority consumed by durable storage.
#[derive(Debug)]
pub enum SealedProviderPublicationBinding {
    /// Completed response-set authority preserving existing capture constructors and evidence.
    ResponseSet(SealedProviderCaptureBinding),
    /// Typed HTTP-response market-event authority.
    ResponseMarketEvent(SealedProviderResponseMarketEventBinding),
    /// Sealed event-microbatch authority with exact stream-frame coordinates.
    EventMicrobatch(SealedProviderEventMicrobatchBinding),
    /// Coinbase Direct-style snapshot plus subsequent ordered event frames.
    CompositeResponseEvent(SealedProviderCompositeResponseEventBinding),
}

impl SealedProviderPublicationBinding {
    /// Returns the closed kind without exposing or converting the underlying authority.
    pub const fn kind(&self) -> ProviderPublicationBindingKind {
        match self {
            Self::ResponseSet(_) => ProviderPublicationBindingKind::ResponseSet,
            Self::ResponseMarketEvent(_) => ProviderPublicationBindingKind::ResponseMarketEvent,
            Self::EventMicrobatch(_) => ProviderPublicationBindingKind::EventMicrobatch,
            Self::CompositeResponseEvent(_) => {
                ProviderPublicationBindingKind::CompositeResponseEvent
            }
        }
    }

    /// Returns response-set authority only when this is a pure response publication.
    pub const fn response_set(&self) -> Option<&SealedProviderCaptureBinding> {
        match self {
            Self::ResponseSet(binding) => Some(binding),
            Self::ResponseMarketEvent(_)
            | Self::EventMicrobatch(_)
            | Self::CompositeResponseEvent(_) => None,
        }
    }

    /// Returns typed HTTP-response market-event authority only for that exact publication kind.
    pub const fn response_market_event(&self) -> Option<&SealedProviderResponseMarketEventBinding> {
        match self {
            Self::ResponseMarketEvent(binding) => Some(binding),
            Self::ResponseSet(_) | Self::EventMicrobatch(_) | Self::CompositeResponseEvent(_) => {
                None
            }
        }
    }

    /// Returns event authority only when this is a pure event publication.
    pub const fn event_microbatch(&self) -> Option<&SealedProviderEventMicrobatchBinding> {
        match self {
            Self::EventMicrobatch(binding) => Some(binding),
            Self::ResponseSet(_)
            | Self::ResponseMarketEvent(_)
            | Self::CompositeResponseEvent(_) => None,
        }
    }

    /// Returns composite authority only for a response-snapshot plus event publication.
    pub const fn composite_response_event(
        &self,
    ) -> Option<&SealedProviderCompositeResponseEventBinding> {
        match self {
            Self::CompositeResponseEvent(binding) => Some(binding),
            Self::ResponseSet(_) | Self::ResponseMarketEvent(_) | Self::EventMicrobatch(_) => None,
        }
    }

    /// Returns the kind-qualified binding digest so downstream code must preserve the distinction.
    pub const fn evidence_digest(&self) -> ProviderPublicationBindingDigest {
        match self {
            Self::ResponseSet(binding) => {
                ProviderPublicationBindingDigest::ResponseSet(binding.evidence_digest())
            }
            Self::ResponseMarketEvent(binding) => {
                ProviderPublicationBindingDigest::ResponseMarketEvent(binding.evidence_digest())
            }
            Self::EventMicrobatch(binding) => {
                ProviderPublicationBindingDigest::EventMicrobatch(binding.evidence_digest())
            }
            Self::CompositeResponseEvent(binding) => {
                ProviderPublicationBindingDigest::CompositeResponseEvent(binding.evidence_digest())
            }
        }
    }
}

impl From<SealedProviderCaptureBinding> for SealedProviderPublicationBinding {
    fn from(binding: SealedProviderCaptureBinding) -> Self {
        Self::ResponseSet(binding)
    }
}

impl From<SealedProviderEventMicrobatchBinding> for SealedProviderPublicationBinding {
    fn from(binding: SealedProviderEventMicrobatchBinding) -> Self {
        Self::EventMicrobatch(binding)
    }
}

impl From<SealedProviderResponseMarketEventBinding> for SealedProviderPublicationBinding {
    fn from(binding: SealedProviderResponseMarketEventBinding) -> Self {
        Self::ResponseMarketEvent(binding)
    }
}

impl From<SealedProviderCompositeResponseEventBinding> for SealedProviderPublicationBinding {
    fn from(binding: SealedProviderCompositeResponseEventBinding) -> Self {
        Self::CompositeResponseEvent(binding)
    }
}
