//! Original native reference custody in the canonical instrument transaction.

use market_squawk_platform::{ResearchObjectClaim, ResearchObjectReceipt, SealedResearchRawClaim};
use market_squawk_sources::{
    ProviderCaptureTerminalDisposition, SegmentedHttpResponseReceipt, ValidatedRawMarketFrame,
};
use serde::{Deserialize, Serialize};

use super::*;
use crate::catalog::provider_capture::raw_claim_digest;
use crate::catalog::provider_event::insert_journal_claim;
use crate::catalog::provider_logical::insert_logical_claim;

const MAX_COORDINATE_BYTES: usize = 32 * 1024;
const MAX_CLAIM_BYTES: usize = 2 * 1024 * 1024;

/// Inert original transport receipt, retained independently of a live session lease.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReferenceSourceCoordinate {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    session_id: Option<market_squawk_sources::SessionId>,
    connection_generation: Option<u64>,
    received_at: Timestamp,
    body_digest: EvidenceDigest,
    body_length: u64,
    transport: NativeReferenceTransport,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum NativeReferenceTransport {
    ExtractionHttp {
        capture: Box<market_squawk_sources::ProviderCaptureSetReceipt>,
    },
    HttpGet {
        event_id: uuid::Uuid,
        final_url: String,
        status: u16,
        declared_body_length: Option<u64>,
        segments: Vec<(u32, u64, EvidenceDigest)>,
        response_coordinate_digest: EvidenceDigest,
    },
    Frame {
        frame_id: u64,
        transport: market_squawk_sources::TransportFrameKind,
    },
}

impl NativeReferenceSourceCoordinate {
    /// Returns the actual capture source, distinct from the native identity namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns the original capture source metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }
    /// Returns the original source session identity.
    pub const fn session_id(&self) -> Option<&market_squawk_sources::SessionId> {
        self.session_id.as_ref()
    }
    /// Returns the original connection generation.
    pub const fn connection_generation(&self) -> Option<u64> {
        self.connection_generation
    }
    /// Returns the original trusted local receipt time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns the exact original payload digest.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }
    /// Returns the exact original payload length.
    pub const fn body_length(&self) -> u64 {
        self.body_length
    }
    /// Returns the original HTTP endpoint, when this was an HTTP response.
    pub fn final_url(&self) -> Option<&str> {
        match &self.transport {
            NativeReferenceTransport::HttpGet { final_url, .. } => Some(final_url),
            _ => None,
        }
    }
    /// Returns the original HTTP receipt coordinate digest, when applicable.
    pub const fn response_coordinate_digest(&self) -> Option<EvidenceDigest> {
        match &self.transport {
            NativeReferenceTransport::HttpGet {
                response_coordinate_digest,
                ..
            } => Some(*response_coordinate_digest),
            _ => None,
        }
    }
    /// Returns the original generation-local frame ordinal, when applicable.
    pub const fn frame_id(&self) -> Option<u64> {
        match &self.transport {
            NativeReferenceTransport::Frame { frame_id, .. } => Some(*frame_id),
            _ => None,
        }
    }

    /// Returns the original extraction request/page receipt, when applicable.
    pub fn extraction_capture(&self) -> Option<&market_squawk_sources::ProviderCaptureSetReceipt> {
        match &self.transport {
            NativeReferenceTransport::ExtractionHttp { capture } => Some(capture),
            _ => None,
        }
    }

    fn validate(
        &self,
        claim: &SealedResearchRawClaim,
    ) -> Result<(), MarketDataInstrumentCatalogError> {
        if self.received_at.unix_nanos() <= 0 || self.body_length == 0 {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        match (&self.transport, claim) {
            (
                NativeReferenceTransport::ExtractionHttp { capture },
                SealedResearchRawClaim::JournalSegment(claim),
            ) => {
                if self.session_id.is_some()
                    || self.connection_generation.is_some()
                    || capture.pages().len() != 1
                    || claim.frames().len() != 1
                    || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                    || capture.source_id() != &self.source_id
                    || capture.metadata_revision() != &self.metadata_revision
                {
                    return Err(MarketDataInstrumentCatalogError::InvalidInput);
                }
                let page = &capture.pages()[0];
                let frame = &claim.frames()[0];
                if page.ordinal() != 0
                    || frame.ordinal() != 0
                    || frame.source_sequence() != Some(0)
                    || page.body_digest() != self.body_digest
                    || frame.provider_payload_digest() != self.body_digest
                    || page.body_bytes() != self.body_length
                    || frame.provider_payload_bytes() != self.body_length
                    || page.received_at() != self.received_at
                    || frame.received_at() != self.received_at
                    || page.request_page_token_digest().is_some()
                    || page.response_next_page_token_digest().is_some()
                {
                    return Err(MarketDataInstrumentCatalogError::InvalidInput);
                }
            }
            (
                NativeReferenceTransport::HttpGet { .. } | NativeReferenceTransport::Frame { .. },
                SealedResearchRawClaim::LogicalObject(claim),
            ) => {
                if self.session_id.is_none()
                    || self
                        .connection_generation
                        .is_none_or(|generation| generation == 0)
                    || self.body_digest != claim.content_digest()
                    || self.body_length != claim.size_bytes()
                {
                    return Err(MarketDataInstrumentCatalogError::InvalidInput);
                }
            }
            _ => return Err(MarketDataInstrumentCatalogError::InvalidInput),
        }
        match &self.transport {
            NativeReferenceTransport::HttpGet {
                event_id,
                final_url,
                status,
                declared_body_length,
                segments,
                response_coordinate_digest,
            } => {
                if event_id.is_nil()
                    || final_url.is_empty()
                    || final_url.len() > 2048
                    || !(200..300).contains(status)
                    || segments.is_empty()
                    || segments.len() > 64
                    || declared_body_length.is_some_and(|length| length != self.body_length)
                {
                    return Err(MarketDataInstrumentCatalogError::InvalidInput);
                }
                // Replay the source-owned HTTP coordinate algorithm; retain the complete source
                // session separately because that algorithm intentionally excludes the session.
                let mut hash = Sha256::new();
                hash.update(b"market-squawk/current-http-response-coordinate/v1");
                hash.update([0]);
                hash_text(&mut hash, final_url);
                hash.update(status.to_be_bytes());
                match declared_body_length {
                    Some(length) => {
                        hash.update([1]);
                        hash.update(length.to_be_bytes());
                    }
                    None => hash.update([0]),
                }
                hash.update(self.body_length.to_be_bytes());
                hash.update(self.body_digest.bytes());
                hash.update(self.received_at.unix_nanos().to_be_bytes());
                hash.update((segments.len() as u64).to_be_bytes());
                let mut total = 0_u64;
                for (index, (ordinal, length, digest)) in segments.iter().enumerate() {
                    if usize::try_from(*ordinal).ok() != Some(index)
                        || *length == 0
                        || digest.algorithm() != DigestAlgorithm::Sha256
                        || digest.bytes() == [0; 32]
                    {
                        return Err(MarketDataInstrumentCatalogError::InvalidInput);
                    }
                    total = total
                        .checked_add(*length)
                        .ok_or(MarketDataInstrumentCatalogError::InvalidInput)?;
                    hash.update(ordinal.to_be_bytes());
                    hash.update(length.to_be_bytes());
                    hash.update(digest.bytes());
                }
                if total != self.body_length
                    || digest(hash.finalize().into()) != *response_coordinate_digest
                {
                    return Err(MarketDataInstrumentCatalogError::InvalidInput);
                }
            }
            NativeReferenceTransport::Frame { frame_id, .. } if *frame_id == 0 => {
                return Err(MarketDataInstrumentCatalogError::InvalidInput);
            }
            NativeReferenceTransport::Frame { .. }
            | NativeReferenceTransport::ExtractionHttp { .. } => {}
        }
        Ok(())
    }
}

/// Source-observed native reference paired with the non-forgeable physical seal receipt.
#[derive(Debug)]
pub struct AcceptedNativeReferenceCapture {
    instrument_id: InstrumentId,
    identity_source: SourceId,
    native_id: ProviderInstrumentId,
    coordinate: NativeReferenceSourceCoordinate,
    raw: SealedResearchRawClaim,
}

impl AcceptedNativeReferenceCapture {
    /// Retains an authentic pre-session extraction response using its existing sealed journal.
    /// The original one-use publication token remains owned by the source publication path.
    pub fn from_extraction_http(
        instrument_id: InstrumentId,
        identity_source: SourceId,
        native_id: ProviderInstrumentId,
        capture: &ProviderWholeCaptureToken,
    ) -> Result<Self, MarketDataInstrumentCatalogError> {
        let sealed = capture.persisted_receipt();
        let receipt = sealed.capture();
        if receipt.pages().len() != 1 {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        let page = &receipt.pages()[0];
        let coordinate = NativeReferenceSourceCoordinate {
            source_id: receipt.source_id().clone(),
            metadata_revision: receipt.metadata_revision().clone(),
            session_id: None,
            connection_generation: None,
            received_at: page.received_at(),
            body_digest: page.body_digest(),
            body_length: page.body_bytes(),
            transport: NativeReferenceTransport::ExtractionHttp {
                capture: Box::new(receipt.clone()),
            },
        };
        let raw = SealedResearchRawClaim::JournalSegment(sealed.segment().claim().clone());
        coordinate.validate(&raw)?;
        Ok(Self {
            instrument_id,
            identity_source,
            native_id,
            coordinate,
            raw,
        })
    }

    /// Binds the exact captured HTTP body to its physical seal before session closure.
    pub fn from_http(
        instrument_id: InstrumentId,
        identity_source: SourceId,
        native_id: ProviderInstrumentId,
        capture: &SegmentedHttpResponseReceipt,
        raw: ResearchObjectReceipt,
    ) -> Result<Self, MarketDataInstrumentCatalogError> {
        let coordinate = NativeReferenceSourceCoordinate {
            source_id: capture.source_id().clone(),
            metadata_revision: capture.metadata_revision().clone(),
            session_id: Some(capture.session_id().clone()),
            connection_generation: Some(capture.connection_generation().get()),
            received_at: capture.received_at(),
            body_digest: capture.body_digest(),
            body_length: capture.body_length(),
            transport: NativeReferenceTransport::HttpGet {
                event_id: capture.event_id(),
                final_url: capture.final_url().to_owned(),
                status: capture.status(),
                declared_body_length: capture.declared_body_length(),
                segments: capture
                    .segments()
                    .iter()
                    .map(|segment| {
                        (
                            segment.ordinal(),
                            segment.body_length(),
                            segment.body_digest(),
                        )
                    })
                    .collect(),
                response_coordinate_digest: capture.coordinate_digest(),
            },
        };
        let raw = SealedResearchRawClaim::LogicalObject(raw.claim().clone());
        coordinate.validate(&raw)?;
        Ok(Self {
            instrument_id,
            identity_source,
            native_id,
            coordinate,
            raw,
        })
    }

    /// Binds a registry-validated frame to the exact physically sealed original payload.
    pub fn from_frame(
        instrument_id: InstrumentId,
        identity_source: SourceId,
        native_id: ProviderInstrumentId,
        capture: &ValidatedRawMarketFrame<'_>,
        raw: ResearchObjectReceipt,
    ) -> Result<Self, MarketDataInstrumentCatalogError> {
        let frame = capture.frame();
        let coordinate = NativeReferenceSourceCoordinate {
            source_id: frame.source_id().clone(),
            metadata_revision: frame.metadata_revision().clone(),
            session_id: Some(frame.session_id().clone()),
            connection_generation: Some(frame.connection_generation().get()),
            received_at: frame.received_at(),
            body_digest: digest(sha256(frame.payload())),
            body_length: frame.payload().len() as u64,
            transport: NativeReferenceTransport::Frame {
                frame_id: frame.frame_id().get(),
                transport: frame.transport(),
            },
        };
        let raw = SealedResearchRawClaim::LogicalObject(raw.claim().clone());
        coordinate.validate(&raw)?;
        Ok(Self {
            instrument_id,
            identity_source,
            native_id,
            coordinate,
            raw,
        })
    }
    /// Returns the canonical identity supplied by the source-specific normalization owner.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the native identity namespace.
    pub const fn identity_source(&self) -> &SourceId {
        &self.identity_source
    }
    /// Returns the exact provider-native identity.
    pub const fn native_id(&self) -> &ProviderInstrumentId {
        &self.native_id
    }
    /// Returns the original receipt coordinate without reviving its session lease.
    pub const fn coordinate(&self) -> &NativeReferenceSourceCoordinate {
        &self.coordinate
    }
    /// Returns the physically verified original claim.
    pub const fn claim(&self) -> Option<&ResearchObjectClaim> {
        match &self.raw {
            SealedResearchRawClaim::LogicalObject(claim) => Some(claim),
            _ => None,
        }
    }
    /// Returns the original logical-object or journal-segment claim.
    pub const fn raw_claim(&self) -> &SealedResearchRawClaim {
        &self.raw
    }
}

/// Catalog-verified original reference; physical use still requires reopening its sealed claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedNativeReferenceCapture {
    coordinate: NativeReferenceSourceCoordinate,
    claim: SealedResearchRawClaim,
    retained_at: Timestamp,
}
impl RetainedNativeReferenceCapture {
    /// Returns the complete original source receipt coordinate.
    pub const fn coordinate(&self) -> &NativeReferenceSourceCoordinate {
        &self.coordinate
    }
    /// Returns the exact claim to reopen through the existing research journal store.
    pub const fn claim(&self) -> Option<&ResearchObjectClaim> {
        match &self.claim {
            SealedResearchRawClaim::LogicalObject(claim) => Some(claim),
            _ => None,
        }
    }
    /// Returns the original logical-object or journal-segment claim.
    pub const fn raw_claim(&self) -> &SealedResearchRawClaim {
        &self.claim
    }
    /// Returns the original source receipt time, never a new restart observation.
    pub const fn received_at(&self) -> Timestamp {
        self.coordinate.received_at
    }
    /// Returns the catalog custody admission time.
    pub const fn retained_at(&self) -> Timestamp {
        self.retained_at
    }
}

impl MarketDataInstrumentSynchronizationCapability {
    /// Atomically admits exact original raw custody and a complete native-identity CAS batch.
    /// New or changed identities require custody; unrelated unchanged identities retain their
    /// existing evidence. Selecting any native reference still requires its exact custody read.
    pub fn synchronize_native_references_if_current(
        &self,
        synchronization: MarketDataInstrumentSynchronization,
        mut expected_current: Vec<MarketDataInstrumentCurrentExpectation>,
        captures: Vec<AcceptedNativeReferenceCapture>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSynchronizationReceipt, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        if expected_current.len() != synchronization.definitions.len()
            || captures.len() > MAX_MARKET_DATA_INSTRUMENT_SYNC_ROWS
        {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        expected_current.sort_unstable_by_key(|expected| expected.instrument_id);
        self.authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?
            .synchronize_market_data_instruments(
                synchronization,
                Some(&expected_current),
                Some(&captures),
                deadline,
                cancellation,
            )
    }
}

impl MarketDataInstrumentReadCapability {
    /// Replays the selected definition and returns its exact original native-reference custody.
    /// Callers must physically reopen the returned claim before using its bytes. No live source
    /// request or fabricated new observation is needed to recover this value after restart.
    pub fn native_reference(
        &self,
        selection: &MarketDataProviderIdentitySelection,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<RetainedNativeReferenceCapture>, MarketDataInstrumentCatalogError> {
        check_operation(deadline, cancellation)?;
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?;
        let record =
            authority.read_selected_provider_definition(selection, deadline, cancellation)?;
        let exact = selection.exact_receipt()?;
        let identity = record
            .definition()
            .provider_identities()
            .iter()
            .find(|identity| {
                identity.source_id() == selection.query().source_id()
                    && identity.provider_instrument_id()
                        == selection.query().provider_instrument_id()
                    && identity.metadata_revision() == exact.provider_identity_revision()
                    && identity.evidence().content_digest()
                        == exact.provider_identity_payload_digest()
                    && identity.validity() == exact.provider_identity_validity()
            })
            .ok_or(MarketDataInstrumentCatalogError::CorruptCatalog)?;
        let connection = &authority.catalog().connection;
        install_progress_handler(connection, deadline, cancellation)?;
        let result = load_reference(
            connection,
            identity,
            authority.catalog().result_bytes.max_record_bytes(),
        )
        .and_then(|value| {
            if value
                .as_ref()
                .is_some_and(|value| value.retained_at > selection.query().knowledge_at())
            {
                return Ok(None);
            }
            Ok(value)
        });
        clear_progress_handler(connection)?;
        classify_operation(result, deadline, cancellation)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the exact CAS predecessor plans and transaction controls remain explicit"
)]
pub(super) fn retain_references(
    transaction: &Transaction<'_>,
    prepared: &[PreparedDefinition],
    plans: &[PublicationPlan],
    captures: &[AcceptedNativeReferenceCapture],
    now: Timestamp,
    max_record_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), MarketDataInstrumentCatalogError> {
    if prepared.len() != plans.len() {
        return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
    }
    let mut admitted = BTreeSet::new();
    for capture in captures {
        check_operation(deadline, cancellation)?;
        let position = prepared
            .binary_search_by_key(&capture.instrument_id, |value| {
                value.definition.instrument_id()
            })
            .map_err(|_| MarketDataInstrumentCatalogError::InvalidInput)?;
        let definition = &prepared[position];
        let mut matches = definition
            .definition
            .provider_identities()
            .iter()
            .filter(|identity| {
                identity.source_id() == &capture.identity_source
                    && identity.provider_instrument_id() == &capture.native_id
                    && identity.evidence().content_digest() == capture.coordinate.body_digest
                    && identity
                        .observation_timestamps()
                        .contains(&capture.coordinate.received_at)
            });
        let identity = matches
            .next()
            .ok_or(MarketDataInstrumentCatalogError::InvalidInput)?;
        if matches.next().is_some() {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        let identity_digest = identity_digest(identity)?;
        if !admitted.insert(identity_digest.bytes()) || capture.coordinate.received_at > now {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        capture.coordinate.validate(&capture.raw)?;
        let coordinate_json = serde_json::to_string(&capture.coordinate)?;
        let claim_json = serde_json::to_string(&capture.raw)?;
        require_record_bound(&coordinate_json, &claim_json, max_record_bytes)?;
        if let Some(existing) = load_reference(transaction, identity, max_record_bytes)? {
            if existing.coordinate != capture.coordinate || existing.claim != capture.raw {
                return Err(MarketDataInstrumentCatalogError::ReferencePositionConflict);
            }
            continue;
        }
        let claim_digest = raw_claim_digest(claim_json.as_bytes());
        match &capture.raw {
            SealedResearchRawClaim::LogicalObject(claim) => {
                insert_logical_claim(transaction, claim_digest, claim, now)?
            }
            SealedResearchRawClaim::JournalSegment(claim) => {
                insert_journal_claim(transaction, claim_digest, claim, now)?
            }
        }
        let custody = custody_digest(
            identity_digest,
            digest(definition.digest),
            &coordinate_json,
            claim_digest,
            now,
        );
        transaction.execute("INSERT INTO market_data_native_reference_captures
            (identity_digest, origin_revision_digest, coordinate_json, raw_claim_digest, physical_receipt_digest, custody_digest, retained_at_ns)
            VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![identity_digest.bytes(), definition.digest, coordinate_json, claim_digest.bytes(),
                physical_digest(&capture.raw).bytes(), custody.bytes(), now.unix_nanos()])?;
        append_audit(
            transaction,
            "market-data-native-reference.retained",
            capture.identity_source.as_str(),
            custody.bytes(),
            now,
        )?;
    }
    for (definition, plan) in prepared.iter().zip(plans) {
        check_operation(deadline, cancellation)?;
        if definition.definition.provider_identities().is_empty() {
            return Err(MarketDataInstrumentCatalogError::InvalidInput);
        }
        // The CAS plan records the exact predecessor before this transaction's insert. Looking
        // at current_ here would inspect the just-published candidate and miss new identities.
        let predecessor = match plan {
            PublicationPlan::Insert {
                previous: Some(previous),
                ..
            } => Some(
                load_record_by_digest(transaction, *previous)?
                    .ok_or(MarketDataInstrumentCatalogError::CorruptCatalog)?,
            ),
            PublicationPlan::Insert { previous: None, .. } | PublicationPlan::Replay => None,
        };
        for identity in definition.definition.provider_identities() {
            check_operation(deadline, cancellation)?;
            let unchanged = matches!(plan, PublicationPlan::Replay)
                || predecessor.as_ref().is_some_and(|previous| {
                    previous
                        .definition()
                        .provider_identities()
                        .contains(identity)
                });
            let retained = load_reference(transaction, identity, max_record_bytes)?;
            if !unchanged && retained.is_none() {
                return Err(MarketDataInstrumentCatalogError::InvalidInput);
            }
        }
    }
    Ok(())
}

fn identity_digest(
    identity: &ProviderIdentityRecord,
) -> Result<EvidenceDigest, MarketDataInstrumentCatalogError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/native-reference-identity/v1\0");
    hash.update(serde_json::to_vec(identity)?);
    Ok(digest(hash.finalize().into()))
}

fn custody_digest(
    identity: EvidenceDigest,
    origin: EvidenceDigest,
    coordinate: &str,
    claim: EvidenceDigest,
    retained_at: Timestamp,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/native-reference-custody/v1\0");
    hash_evidence(&mut hash, identity);
    hash_evidence(&mut hash, origin);
    hash_text(&mut hash, coordinate);
    hash_evidence(&mut hash, claim);
    hash.update(retained_at.unix_nanos().to_be_bytes());
    digest(hash.finalize().into())
}

fn require_record_bound(
    coordinate: &str,
    claim: &str,
    limit: usize,
) -> Result<(), MarketDataInstrumentCatalogError> {
    if coordinate.len() > MAX_COORDINATE_BYTES
        || claim.len() > MAX_CLAIM_BYTES
        || coordinate
            .len()
            .checked_add(claim.len())
            .and_then(|bytes| bytes.checked_mul(3))
            .and_then(|bytes| bytes.checked_add(4096))
            .is_none_or(|bytes| bytes > limit)
    {
        return Err(MarketDataInstrumentCatalogError::ResultByteLimitExceeded);
    }
    Ok(())
}

fn load_reference(
    connection: &rusqlite::Connection,
    identity: &ProviderIdentityRecord,
    max_record_bytes: usize,
) -> Result<Option<RetainedNativeReferenceCapture>, MarketDataInstrumentCatalogError> {
    let identity_digest = identity_digest(identity)?;
    let mut statement = connection.prepare(
        "SELECT capture.origin_revision_digest, capture.coordinate_json, capture.raw_claim_digest,
                capture.physical_receipt_digest, capture.custody_digest, capture.retained_at_ns, raw.raw_claim_json
         FROM market_data_native_reference_captures AS capture
         JOIN sealed_raw_objects AS raw ON raw.raw_claim_digest=capture.raw_claim_digest
          AND raw.physical_receipt_digest=capture.physical_receipt_digest
         WHERE capture.identity_digest=?1",
    )?;
    let mut rows = statement.query([identity_digest.bytes()])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    // Charge SQLite's borrowed bytes before constructing owned JSON or deserialized claims.
    let coordinate_bytes = row
        .get_ref(1)?
        .as_str()
        .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?;
    let claim_bytes = row
        .get_ref(6)?
        .as_str()
        .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?;
    require_record_bound(coordinate_bytes, claim_bytes, max_record_bytes)?;
    let origin: Vec<u8> = row.get(0)?;
    let coordinate_json: String = row.get(1)?;
    let claim_digest: Vec<u8> = row.get(2)?;
    let stored_physical_digest: Vec<u8> = row.get(3)?;
    let custody: Vec<u8> = row.get(4)?;
    let retained_at: i64 = row.get(5)?;
    let claim_json: String = row.get(6)?;
    require_record_bound(&coordinate_json, &claim_json, max_record_bytes)?;
    let coordinate: NativeReferenceSourceCoordinate = serde_json::from_str(&coordinate_json)?;
    let claim: SealedResearchRawClaim = serde_json::from_str(&claim_json)?;
    let origin: [u8; 32] = origin
        .try_into()
        .map_err(|_| MarketDataInstrumentCatalogError::CorruptCatalog)?;
    let retained_at = Timestamp::from_unix_nanos(retained_at);
    let expected_claim_digest = raw_claim_digest(claim_json.as_bytes());
    if coordinate.validate(&claim).is_err()
        || serde_json::to_string(&coordinate)? != coordinate_json
        || serde_json::to_string(&claim)? != claim_json
        || expected_claim_digest.bytes().as_slice() != claim_digest
        || physical_digest(&claim).bytes().as_slice() != stored_physical_digest
        || coordinate.body_digest != identity.evidence().content_digest()
        || !identity
            .observation_timestamps()
            .contains(&coordinate.received_at)
        || coordinate.received_at > retained_at
        || custody_digest(
            identity_digest,
            digest(origin),
            &coordinate_json,
            expected_claim_digest,
            retained_at,
        )
        .bytes()
        .as_slice()
            != custody
    {
        return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
    }
    let definition_bytes: i64 = connection.query_row(
        "SELECT length(CAST(definition_json AS BLOB)) FROM market_data_instrument_revisions WHERE revision_digest=?1",
        [origin], |row| row.get(0),
    )?;
    if usize::try_from(definition_bytes)
        .ok()
        .is_none_or(|bytes| bytes > max_record_bytes)
    {
        return Err(MarketDataInstrumentCatalogError::ResultByteLimitExceeded);
    }
    let origin = load_record_by_digest(connection, origin)?
        .ok_or(MarketDataInstrumentCatalogError::CorruptCatalog)?;
    if !origin.definition().provider_identities().contains(identity)
        || origin.published_at() > retained_at
    {
        return Err(MarketDataInstrumentCatalogError::CorruptCatalog);
    }
    Ok(Some(RetainedNativeReferenceCapture {
        coordinate,
        claim,
        retained_at,
    }))
}

fn physical_digest(claim: &SealedResearchRawClaim) -> EvidenceDigest {
    match claim {
        SealedResearchRawClaim::LogicalObject(claim) => claim.physical_receipt_digest(),
        SealedResearchRawClaim::JournalSegment(claim) => claim.physical_receipt_digest(),
    }
}
