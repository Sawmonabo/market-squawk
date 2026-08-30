//! One-use application binding for complete official OCC/Cboe reference captures.
//!
//! This leaf bridges the provider-owned completed request closure to the application-owned
//! catalog boundary. It never persists a raw claim: the shared research-object store remains the
//! sole physical authority, and phase-B catalog composition receives only non-forgeable store
//! receipts after the same objects have been reverified immediately before commit.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Instant;

use market_squawk_adapter_options_reference::{
    CboeListingEvidence, CboeSeriesStatus, CboeVenue, CompletedModifiedReferencePublicationCapture,
    HttpLastModifiedEvidence, OccDlpPresence, OccExchangeCode, OccExchangeListingEvidence,
    OccPositionLimit, OccProductType, OptionsReferenceAliasDisposition,
    OptionsReferenceIdentityDisposition, OptionsReferenceValidityDisposition, PageTerminalState,
    ReferenceAliasKey, ReferenceAliasResolution, ReferenceAliasResolutionState, ReferenceConflict,
    ReferenceConflictKind, ReferenceExportRecord, ReferenceModifiedObjectHandoff,
    ReferenceNativeSchemaIdentity, ReferenceObjectContext, ReferenceProvider, ReferenceSurface,
};
use market_squawk_data::{
    OfficialOptionsReferenceAliasAssertionSetEvidence, OfficialOptionsReferenceAliasKey,
    OfficialOptionsReferenceAliasResolutionInput, OfficialOptionsReferenceAliasResolutionState,
    OfficialOptionsReferenceCboeSeries, OfficialOptionsReferenceConflictInput,
    OfficialOptionsReferenceConflictKind, OfficialOptionsReferenceConflictSetEvidence,
    OfficialOptionsReferenceError, OfficialOptionsReferenceGenerationHeader,
    OfficialOptionsReferenceObjectInput, OfficialOptionsReferenceOccExchangeListingEvidence,
    OfficialOptionsReferenceOccPositionLimit, OfficialOptionsReferenceOccProduct,
    OfficialOptionsReferenceOccProductType, OfficialOptionsReferencePublicationCapability,
    OfficialOptionsReferencePublicationReceipt, OfficialOptionsReferenceRecordInput,
    OfficialOptionsReferenceRecordSetEvidence, OfficialOptionsReferenceRecordValue,
    OfficialOptionsReferenceResolutionSetEvidence, OfficialOptionsReferenceSurface,
};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, EvidenceDigest, ResearchTemporalCoordinate, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectReceipt, SealedResearchJournalStoreError,
    VerifiedResearchObject,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::ProductionResearchIngestCoordinator;

/// Exact complete-request evidence retained independently of any eventual catalog generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OfficialOptionsReferenceClosureCommitment {
    request_id: SourceIdentifier,
    requested_at: Timestamp,
    request_deadline: Timestamp,
    selected_surfaces: Box<[ReferenceSurface]>,
    completed_objects: u32,
    payload_bytes: u64,
    strict_row_count: u64,
    conflicts: usize,
    strict_row_set_digest: EvidenceDigest,
    alias_assertions: u64,
    alias_assertion_closure_digest: EvidenceDigest,
}

impl OfficialOptionsReferenceClosureCommitment {
    pub(crate) const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    pub(crate) const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    pub(crate) const fn request_deadline(&self) -> Timestamp {
        self.request_deadline
    }

    pub(crate) fn selected_surfaces(&self) -> &[ReferenceSurface] {
        &self.selected_surfaces
    }

    pub(crate) const fn completed_objects(&self) -> u32 {
        self.completed_objects
    }

    pub(crate) const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub(crate) const fn strict_row_count(&self) -> u64 {
        self.strict_row_count
    }

    pub(crate) const fn conflicts(&self) -> usize {
        self.conflicts
    }

    pub(crate) const fn strict_row_set_digest(&self) -> EvidenceDigest {
        self.strict_row_set_digest
    }

    pub(crate) const fn alias_assertions(&self) -> u64 {
        self.alias_assertions
    }

    pub(crate) const fn alias_assertion_closure_digest(&self) -> EvidenceDigest {
        self.alias_assertion_closure_digest
    }
}

/// Exact strict-parser disposition retained for one selected physical object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OfficialOptionsReferenceStrictObjectCommitment {
    Typed {
        page_ordinal: NonZeroU32,
        returned_records: u32,
        rejected_records: u32,
        strict_row_set_digest: EvidenceDigest,
        terminal_state: PageTerminalState,
    },
    UninterpretedMemo,
}

impl OfficialOptionsReferenceStrictObjectCommitment {
    pub(crate) const fn returned_records(&self) -> u64 {
        match self {
            Self::Typed {
                returned_records, ..
            } => *returned_records as u64,
            Self::UninterpretedMemo => 0,
        }
    }

    pub(crate) const fn strict_row_set_digest(&self) -> Option<EvidenceDigest> {
        match self {
            Self::Typed {
                strict_row_set_digest,
                ..
            } => Some(*strict_row_set_digest),
            Self::UninterpretedMemo => None,
        }
    }

    pub(crate) const fn page_ordinal(&self) -> Option<NonZeroU32> {
        match self {
            Self::Typed { page_ordinal, .. } => Some(*page_ordinal),
            Self::UninterpretedMemo => None,
        }
    }

    pub(crate) const fn rejected_records(&self) -> u32 {
        match self {
            Self::Typed {
                rejected_records, ..
            } => *rejected_records,
            Self::UninterpretedMemo => 0,
        }
    }

    pub(crate) const fn terminal_state(&self) -> Option<&PageTerminalState> {
        match self {
            Self::Typed { terminal_state, .. } => Some(terminal_state),
            Self::UninterpretedMemo => None,
        }
    }
}

/// Full source, transport, temporal, schema, and strict-row evidence for one request object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OfficialOptionsReferenceObjectCommitment {
    ordinal: u16,
    provider: ReferenceProvider,
    surface: ReferenceSurface,
    source_id: SourceIdentifier,
    provider_id: SourceIdentifier,
    source_contract_digest: EvidenceDigest,
    object_id: SourceIdentifier,
    payload_digest: EvidenceDigest,
    payload_bytes: u64,
    strict: OfficialOptionsReferenceStrictObjectCommitment,
    posted: Option<ResearchTemporalCoordinate>,
    effective: Option<ResearchTemporalCoordinate>,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    headers_received_at: Timestamp,
    transport_elapsed_nanos: u64,
    request_digest: EvidenceDigest,
    transport_receipt_digest: EvidenceDigest,
    configured_locator: SourceIdentifier,
    final_locator: SourceIdentifier,
    redirect_chain: Box<[SourceIdentifier]>,
    cache_last_modified: Option<String>,
    http_last_modified: Option<HttpLastModifiedEvidence>,
    etag: Option<String>,
    media_type: SourceIdentifier,
    observed_content_type: String,
    source_filename: Option<SourceIdentifier>,
    source_publication_date: Option<CalendarDate>,
    native_schema: ReferenceNativeSchemaIdentity,
}

impl OfficialOptionsReferenceObjectCommitment {
    pub(crate) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    pub(crate) const fn provider(&self) -> ReferenceProvider {
        self.provider
    }

    pub(crate) const fn surface(&self) -> &ReferenceSurface {
        &self.surface
    }

    pub(crate) const fn source_id(&self) -> &SourceIdentifier {
        &self.source_id
    }

    pub(crate) const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    pub(crate) const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    pub(crate) const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub(crate) const fn strict(&self) -> &OfficialOptionsReferenceStrictObjectCommitment {
        &self.strict
    }

    pub(crate) fn posted(&self) -> Option<&ResearchTemporalCoordinate> {
        self.posted.as_ref()
    }

    pub(crate) fn effective(&self) -> Option<&ResearchTemporalCoordinate> {
        self.effective.as_ref()
    }

    pub(crate) const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    pub(crate) const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub(crate) const fn headers_received_at(&self) -> Timestamp {
        self.headers_received_at
    }

    pub(crate) const fn transport_elapsed_nanos(&self) -> u64 {
        self.transport_elapsed_nanos
    }

    pub(crate) const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    pub(crate) const fn transport_receipt_digest(&self) -> EvidenceDigest {
        self.transport_receipt_digest
    }

    pub(crate) const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    pub(crate) const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }

    pub(crate) fn redirect_chain(&self) -> &[SourceIdentifier] {
        &self.redirect_chain
    }

    pub(crate) fn cache_last_modified(&self) -> Option<&str> {
        self.cache_last_modified.as_deref()
    }

    pub(crate) const fn http_last_modified(&self) -> Option<&HttpLastModifiedEvidence> {
        self.http_last_modified.as_ref()
    }

    pub(crate) fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    pub(crate) const fn media_type(&self) -> &SourceIdentifier {
        &self.media_type
    }

    pub(crate) fn observed_content_type(&self) -> &str {
        &self.observed_content_type
    }

    pub(crate) const fn source_filename(&self) -> Option<&SourceIdentifier> {
        self.source_filename.as_ref()
    }

    pub(crate) const fn source_publication_date(&self) -> Option<CalendarDate> {
        self.source_publication_date
    }

    pub(crate) const fn native_schema(&self) -> &ReferenceNativeSchemaIdentity {
        &self.native_schema
    }

    pub(crate) const fn provider_id(&self) -> &SourceIdentifier {
        &self.provider_id
    }

    pub(crate) const fn source_contract_digest(&self) -> EvidenceDigest {
        self.source_contract_digest
    }
}

/// Non-cloneable application authority retaining exact provider handoffs and physical objects.
#[derive(Debug)]
pub(crate) struct OfficialOptionsReferenceApplicationBinding {
    capture: CompletedModifiedReferencePublicationCapture,
    closure: OfficialOptionsReferenceClosureCommitment,
    objects: Box<[OfficialOptionsReferenceObjectCommitment]>,
    verified: Box<[VerifiedResearchObject]>,
}

impl OfficialOptionsReferenceApplicationBinding {
    /// Reopens every store-issued receipt and binds it to the complete provider closure.
    pub(crate) fn try_new(
        coordinator: &ProductionResearchIngestCoordinator,
        capture: CompletedModifiedReferencePublicationCapture,
        control: &dyn ResearchObjectControl,
    ) -> Result<Self, OfficialOptionsReferenceApplicationError> {
        let (closure, objects) = validate_capture(&capture)?;
        let store = coordinator.research.provider_capture_store();
        let mut verified = Vec::new();
        verified
            .try_reserve_exact(capture.objects().len())
            .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?;
        for handoff in capture.objects() {
            let object = store.open_verified_logical_object(handoff.raw_receipt(), control)?;
            if object.content_digest() != handoff.raw_receipt().content_digest()
                || object.size_bytes() != handoff.raw_receipt().size_bytes()
            {
                return Err(OfficialOptionsReferenceApplicationError::PhysicalMismatch);
            }
            verified.push(object);
        }
        Ok(Self {
            capture,
            closure,
            objects,
            verified: verified.into_boxed_slice(),
        })
    }

    /// Consumes the application binding after immediate physical re-verification for commit.
    pub(crate) fn try_into_catalog_commit_input(
        self,
        control: &dyn ResearchObjectControl,
    ) -> Result<OfficialOptionsReferenceCatalogCommitInput, OfficialOptionsReferenceApplicationError>
    {
        let Self {
            capture,
            closure,
            objects,
            verified,
        } = self;
        let (revalidated_closure, revalidated_objects) = validate_capture(&capture)?;
        if closure != revalidated_closure || objects != revalidated_objects {
            return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
        }
        if capture.objects().len() != verified.len() || objects.len() != verified.len() {
            return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
        }
        let mut receipts = Vec::new();
        receipts
            .try_reserve_exact(verified.len())
            .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?;
        for (descriptor, handoff) in verified.into_vec().into_iter().zip(capture.objects()) {
            let receipt = descriptor.reverify_for_commit(control)?;
            if &receipt != handoff.raw_receipt() {
                return Err(OfficialOptionsReferenceApplicationError::PhysicalMismatch);
            }
            receipts.push(receipt);
        }
        let object_lookup = build_object_lookup(&capture)?;
        Ok(OfficialOptionsReferenceCatalogCommitInput {
            capture,
            closure,
            objects,
            receipts: receipts.into_boxed_slice(),
            object_lookup,
        })
    }
}

#[derive(Debug)]
struct OfficialOptionsReferenceObjectLookup {
    provider: ReferenceProvider,
    surface: ReferenceSurface,
    object_id: SourceIdentifier,
    ordinal: u16,
}

/// Non-cloneable, commit-ready phase-B input with freshly reverified physical receipts.
#[derive(Debug)]
pub(crate) struct OfficialOptionsReferenceCatalogCommitInput {
    capture: CompletedModifiedReferencePublicationCapture,
    closure: OfficialOptionsReferenceClosureCommitment,
    objects: Box<[OfficialOptionsReferenceObjectCommitment]>,
    receipts: Box<[ResearchObjectReceipt]>,
    object_lookup: Box<[OfficialOptionsReferenceObjectLookup]>,
}

impl OfficialOptionsReferenceCatalogCommitInput {
    pub(crate) const fn capture(&self) -> &CompletedModifiedReferencePublicationCapture {
        &self.capture
    }

    pub(crate) const fn closure(&self) -> &OfficialOptionsReferenceClosureCommitment {
        &self.closure
    }

    pub(crate) fn objects(&self) -> &[OfficialOptionsReferenceObjectCommitment] {
        &self.objects
    }

    pub(crate) fn receipts(&self) -> &[ResearchObjectReceipt] {
        &self.receipts
    }

    /// Maps one strict provider row into the closed durable catalog value without resolving a
    /// provider alias into canonical identity.
    pub(crate) fn map_record(
        &self,
        record: &ReferenceExportRecord,
    ) -> Result<OfficialOptionsReferenceRecordInput, OfficialOptionsReferenceApplicationError> {
        if record.identity() != OptionsReferenceIdentityDisposition::ProviderNativeReferenceOnly
            || record.validity() != OptionsReferenceValidityDisposition::ExactSourceSnapshotOnly
            || record.alias() != OptionsReferenceAliasDisposition::ProviderAliasCandidateOnly
        {
            return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
        }
        let object_ordinal = self.object_ordinal(record.object_context())?;
        let (provider_row_number, record_id, value) = match record {
            ReferenceExportRecord::CboeSeries(series) => {
                if series.listing_evidence() != CboeListingEvidence::PresentInVenuePublication {
                    return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
                }
                let value = OfficialOptionsReferenceCboeSeries::try_new(
                    cboe_venue(series.venue())?,
                    series.cboe_symbol_id().as_str(),
                    series.contract().osi().clone(),
                    series.underlying().clone(),
                    series.unit(),
                    series.status() == CboeSeriesStatus::ClosingOnly,
                )?;
                (
                    series.provider_row_number(),
                    series.record_id().clone(),
                    OfficialOptionsReferenceRecordValue::CboeSeries(value),
                )
            }
            ReferenceExportRecord::OccProduct(product) => {
                if product.presence() != OccDlpPresence::PresentInDirectoryPublication {
                    return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
                }
                let mut exchange_codes = String::with_capacity(product.trading_exchanges().len());
                for code in product.trading_exchanges() {
                    exchange_codes.push(occ_exchange_code(*code));
                }
                let value = OfficialOptionsReferenceOccProduct::try_new(
                    product.options_symbol().clone(),
                    product.underlying_symbol().clone(),
                    product.symbol_name(),
                    exchange_codes,
                    map_occ_exchange_listing(product.exchange_listing_evidence()),
                    map_occ_position_limit(product.position_limit()),
                    map_occ_product_type(product.product_type()),
                )?;
                (
                    product.provider_row_number(),
                    product.record_id().clone(),
                    OfficialOptionsReferenceRecordValue::OccProduct(value),
                )
            }
        };
        Ok(OfficialOptionsReferenceRecordInput::try_new(
            object_ordinal,
            provider_row_number,
            record_id,
            value,
        )?)
    }

    /// Maps one terminal request-scoped alias resolution without selecting a winner.
    pub(crate) fn map_resolution(
        &self,
        resolution: &ReferenceAliasResolution,
    ) -> Result<
        OfficialOptionsReferenceAliasResolutionInput,
        OfficialOptionsReferenceApplicationError,
    > {
        if resolution.request_id() != self.closure.request_id() {
            return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
        }
        Ok(OfficialOptionsReferenceAliasResolutionInput::try_new(
            map_alias_key(resolution.key())?,
            match resolution.state() {
                ReferenceAliasResolutionState::Exact => {
                    OfficialOptionsReferenceAliasResolutionState::Exact
                }
                ReferenceAliasResolutionState::Ambiguous => {
                    OfficialOptionsReferenceAliasResolutionState::Ambiguous
                }
            },
            resolution.observations(),
            resolution.conflicts(),
        )?)
    }

    /// Maps one exact provider conflict while preserving both retained evidence identities.
    pub(crate) fn map_conflict(
        &self,
        conflict: &ReferenceConflict,
    ) -> Result<OfficialOptionsReferenceConflictInput, OfficialOptionsReferenceApplicationError>
    {
        if conflict.request_id() != self.closure.request_id() {
            return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
        }
        Ok(OfficialOptionsReferenceConflictInput::try_new(
            map_alias_key(conflict.key())?,
            match conflict.kind() {
                ReferenceConflictKind::CboeSymbolMapsMultipleOsi => {
                    OfficialOptionsReferenceConflictKind::CboeSymbolMapsMultipleOsi
                }
                ReferenceConflictKind::CboeOsiMapsMultipleSymbols => {
                    OfficialOptionsReferenceConflictKind::CboeOsiMapsMultipleSymbols
                }
                ReferenceConflictKind::CboeSymbolMapsMultipleUnderlying => {
                    OfficialOptionsReferenceConflictKind::CboeSymbolMapsMultipleUnderlying
                }
                ReferenceConflictKind::DuplicateProviderRecord => {
                    OfficialOptionsReferenceConflictKind::DuplicateProviderRecord
                }
            },
            conflict.first_evidence().clone(),
            conflict.second_evidence().clone(),
        )?)
    }

    /// Atomically publishes the complete externally staged and pre-digested request closure.
    ///
    /// The callbacks must replay the exact strictly ordered streams used to build the supplied
    /// set evidence. Catalog publication recomputes every digest and rejects partial, reordered,
    /// replaced, or extra values.
    #[allow(
        clippy::too_many_arguments,
        reason = "the three independently staged streams and exact closure stay explicit"
    )]
    pub(crate) fn publish<NR, NA, NC>(
        self,
        publication: &OfficialOptionsReferencePublicationCapability,
        expected_previous_generation: Option<EvidenceDigest>,
        alias_assertion_set: OfficialOptionsReferenceAliasAssertionSetEvidence,
        records: OfficialOptionsReferenceRecordSetEvidence,
        resolutions: OfficialOptionsReferenceResolutionSetEvidence,
        conflicts: OfficialOptionsReferenceConflictSetEvidence,
        mut next_record: NR,
        mut next_resolution: NA,
        mut next_conflict: NC,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferencePublicationReceipt, OfficialOptionsReferenceApplicationError>
    where
        NR: FnMut() -> Result<Option<ReferenceExportRecord>, OfficialOptionsReferenceError>,
        NA: FnMut() -> Result<Option<ReferenceAliasResolution>, OfficialOptionsReferenceError>,
        NC: FnMut() -> Result<Option<ReferenceConflict>, OfficialOptionsReferenceError>,
    {
        let header = self.generation_header(
            expected_previous_generation,
            alias_assertion_set,
            records,
            resolutions,
            conflicts,
        )?;
        Ok(publication.publish(
            header,
            || {
                next_record()?
                    .as_ref()
                    .map(|record| self.map_record(record))
                    .transpose()
                    .map_err(|_error| OfficialOptionsReferenceError::InvalidInput)
            },
            || {
                next_resolution()?
                    .as_ref()
                    .map(|resolution| self.map_resolution(resolution))
                    .transpose()
                    .map_err(|_error| OfficialOptionsReferenceError::InvalidInput)
            },
            || {
                next_conflict()?
                    .as_ref()
                    .map(|conflict| self.map_conflict(conflict))
                    .transpose()
                    .map_err(|_error| OfficialOptionsReferenceError::InvalidInput)
            },
            deadline,
            cancellation,
        )?)
    }

    fn object_ordinal(
        &self,
        context: &ReferenceObjectContext,
    ) -> Result<u16, OfficialOptionsReferenceApplicationError> {
        let key = (context.provider(), context.surface(), context.object_id());
        let position = self
            .object_lookup
            .binary_search_by(|entry| (entry.provider, &entry.surface, &entry.object_id).cmp(&key))
            .map_err(|_error| OfficialOptionsReferenceApplicationError::InvalidBinding)?;
        let entry = &self.object_lookup[position];
        if self.capture.objects()[usize::from(entry.ordinal)].context() != context {
            return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
        }
        Ok(entry.ordinal)
    }

    fn generation_header(
        &self,
        expected_previous_generation: Option<EvidenceDigest>,
        alias_assertion_set: OfficialOptionsReferenceAliasAssertionSetEvidence,
        records: OfficialOptionsReferenceRecordSetEvidence,
        resolutions: OfficialOptionsReferenceResolutionSetEvidence,
        conflicts: OfficialOptionsReferenceConflictSetEvidence,
    ) -> Result<OfficialOptionsReferenceGenerationHeader, OfficialOptionsReferenceApplicationError>
    {
        let mut durable_objects = Vec::new();
        durable_objects
            .try_reserve_exact(self.objects.len())
            .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?;
        for (object, receipt) in self.objects.iter().zip(&self.receipts) {
            let strict_row_set_digest = object
                .strict()
                .strict_row_set_digest()
                .unwrap_or_else(OfficialOptionsReferenceObjectInput::empty_strict_row_set_digest);
            let source_timestamp = object
                .effective()
                .and_then(ResearchTemporalCoordinate::exact_timestamp)
                .or_else(|| {
                    object
                        .posted()
                        .and_then(ResearchTemporalCoordinate::exact_timestamp)
                });
            let available_at = object
                .availability()
                .conservative_available_at()
                .unwrap_or_else(|| object.received_at());
            durable_objects.push(OfficialOptionsReferenceObjectInput::try_new(
                market_squawk_data::OfficialOptionsReferenceObjectInputFields {
                    object_ordinal: object.ordinal(),
                    source_id: SourceId::try_from(object.source_id().as_str()).map_err(
                        |_error| OfficialOptionsReferenceApplicationError::InvalidBinding,
                    )?,
                    surface: map_surface(object.surface())?,
                    object_id: object.object_id().clone(),
                    native_schema: OfficialOptionsReferenceObjectInput::try_native_schema_identity(
                        object.native_schema().name(),
                        object.native_schema().version(),
                        object.native_schema().fingerprint(),
                    )?,
                    raw_receipt: receipt.clone(),
                    payload_digest: object.payload_digest(),
                    source_timestamp,
                    available_at,
                    received_at: object.received_at(),
                    strict_row_set_digest,
                    strict_row_count: object.strict().returned_records(),
                },
            )?);
        }
        let header = OfficialOptionsReferenceGenerationHeader::try_new(
            expected_previous_generation,
            self.closure.request_id().clone(),
            self.closure.requested_at(),
            self.closure.request_deadline(),
            alias_assertion_set,
            records,
            resolutions,
            conflicts,
            durable_objects,
        )?;
        if header.strict_row_set_digest() != self.closure.strict_row_set_digest()
            || header.alias_assertions() != self.closure.alias_assertions()
            || header.alias_assertion_closure_digest()
                != self.closure.alias_assertion_closure_digest()
            || header.total_payload_bytes() != self.closure.payload_bytes()
            || header.strict_row_count() != self.closure.strict_row_count()
            || header.objects().len()
                != usize::try_from(self.closure.completed_objects())
                    .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?
        {
            return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
        }
        Ok(header)
    }

    /// Moves exact provider handoffs and non-forgeable receipts into the serialized catalog owner.
    pub(crate) fn into_parts(
        self,
    ) -> (
        CompletedModifiedReferencePublicationCapture,
        OfficialOptionsReferenceClosureCommitment,
        Box<[OfficialOptionsReferenceObjectCommitment]>,
        Box<[ResearchObjectReceipt]>,
    ) {
        (self.capture, self.closure, self.objects, self.receipts)
    }
}

fn validate_capture(
    capture: &CompletedModifiedReferencePublicationCapture,
) -> Result<
    (
        OfficialOptionsReferenceClosureCommitment,
        Box<[OfficialOptionsReferenceObjectCommitment]>,
    ),
    OfficialOptionsReferenceApplicationError,
> {
    let request = capture.request();
    let accounting = capture.accounting();
    let reconciliation = capture.reconciliation();
    let completed_objects = u32::try_from(capture.objects().len())
        .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?;
    if capture.objects().is_empty()
        || capture.objects().len() != request.surfaces().len()
        || completed_objects != accounting.completed_pages()
        || accounting.request_id() != request.request_id()
        || reconciliation.request_id() != request.request_id()
        || reconciliation.conflicts() != accounting.conflicts()
        || reconciliation.assertions() != accounting.alias_assertions()
    {
        return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
    }
    let mut providers = BTreeSet::new();
    let mut payload_bytes = 0_u64;
    let mut strict_rows = 0_u64;
    let mut objects = Vec::new();
    objects
        .try_reserve_exact(capture.objects().len())
        .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?;
    for (ordinal, (handoff, selected_surface)) in
        capture.objects().iter().zip(request.surfaces()).enumerate()
    {
        let commitment = object_commitment(
            u16::try_from(ordinal)
                .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?,
            request.request_id(),
            request.requested_at(),
            request.deadline(),
            handoff,
            selected_surface,
        )?;
        providers.insert(commitment.provider());
        payload_bytes = payload_bytes
            .checked_add(commitment.payload_bytes())
            .ok_or(OfficialOptionsReferenceApplicationError::Capacity)?;
        strict_rows = strict_rows
            .checked_add(commitment.strict().returned_records())
            .ok_or(OfficialOptionsReferenceApplicationError::Capacity)?;
        objects.push(commitment);
    }
    if providers != BTreeSet::from([ReferenceProvider::Occ, ReferenceProvider::Cboe])
        || payload_bytes != accounting.payload_bytes()
        || strict_rows != accounting.returned_records()
    {
        return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
    }
    Ok((
        OfficialOptionsReferenceClosureCommitment {
            request_id: request.request_id().clone(),
            requested_at: request.requested_at(),
            request_deadline: request.deadline(),
            selected_surfaces: request.surfaces().to_vec().into_boxed_slice(),
            completed_objects,
            payload_bytes,
            strict_row_count: strict_rows,
            conflicts: accounting.conflicts(),
            strict_row_set_digest: accounting.strict_row_set_digest(),
            alias_assertions: accounting.alias_assertions(),
            alias_assertion_closure_digest: accounting.alias_assertion_closure_digest(),
        },
        objects.into_boxed_slice(),
    ))
}

fn build_object_lookup(
    capture: &CompletedModifiedReferencePublicationCapture,
) -> Result<Box<[OfficialOptionsReferenceObjectLookup]>, OfficialOptionsReferenceApplicationError> {
    let mut lookup = Vec::new();
    lookup
        .try_reserve_exact(capture.objects().len())
        .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?;
    for (ordinal, handoff) in capture.objects().iter().enumerate() {
        let context = handoff.context();
        lookup.push(OfficialOptionsReferenceObjectLookup {
            provider: context.provider(),
            surface: context.surface().clone(),
            object_id: context.object_id().clone(),
            ordinal: u16::try_from(ordinal)
                .map_err(|_error| OfficialOptionsReferenceApplicationError::Capacity)?,
        });
    }
    lookup.sort_by(|left, right| {
        (&left.provider, &left.surface, &left.object_id).cmp(&(
            &right.provider,
            &right.surface,
            &right.object_id,
        ))
    });
    if lookup.windows(2).any(|pair| {
        (&pair[0].provider, &pair[0].surface, &pair[0].object_id)
            == (&pair[1].provider, &pair[1].surface, &pair[1].object_id)
    }) {
        return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
    }
    Ok(lookup.into_boxed_slice())
}

fn map_surface(
    surface: &ReferenceSurface,
) -> Result<OfficialOptionsReferenceSurface, OfficialOptionsReferenceApplicationError> {
    Ok(match surface {
        ReferenceSurface::CboeAllSeries { venue } => {
            OfficialOptionsReferenceSurface::CboeAllSeries {
                venue: cboe_venue(*venue)?,
            }
        }
        ReferenceSurface::OccDlpSelectedText => OfficialOptionsReferenceSurface::OccDlpSelectedText,
        ReferenceSurface::OccDlpDailyText => OfficialOptionsReferenceSurface::OccDlpDailyText,
        ReferenceSurface::OccDlpDailyXml => OfficialOptionsReferenceSurface::OccDlpDailyXml,
        ReferenceSurface::OccMemoIndexCsv => OfficialOptionsReferenceSurface::OccMemoIndexCsv,
        ReferenceSurface::OccMemoIndexJson => OfficialOptionsReferenceSurface::OccMemoIndexJson,
        ReferenceSurface::OccMemoDocument { memo_number } => {
            OfficialOptionsReferenceSurface::OccMemoDocument {
                memo_number: *memo_number,
            }
        }
        ReferenceSurface::OccMemoAttachment {
            memo_number,
            ordinal,
        } => OfficialOptionsReferenceSurface::OccMemoAttachment {
            memo_number: *memo_number,
            ordinal: ordinal.get(),
        },
    })
}

fn cboe_venue(venue: CboeVenue) -> Result<VenueId, OfficialOptionsReferenceApplicationError> {
    VenueId::try_from(match venue {
        CboeVenue::C1 => "c1",
        CboeVenue::Bzx => "bzx",
        CboeVenue::C2 => "c2",
        CboeVenue::Edgx => "edgx",
    })
    .map_err(|_error| OfficialOptionsReferenceApplicationError::InvalidBinding)
}

const fn map_occ_product_type(value: OccProductType) -> OfficialOptionsReferenceOccProductType {
    match value {
        OccProductType::EquityUnderlying => {
            OfficialOptionsReferenceOccProductType::EquityUnderlying
        }
        OccProductType::EquityBounds => OfficialOptionsReferenceOccProductType::EquityBounds,
        OccProductType::EquityLongTerm => OfficialOptionsReferenceOccProductType::EquityLongTerm,
        OccProductType::EquityFlex => OfficialOptionsReferenceOccProductType::EquityFlex,
        OccProductType::CurrencyUnderlying => {
            OfficialOptionsReferenceOccProductType::CurrencyUnderlying
        }
        OccProductType::CurrencyLongTerm => {
            OfficialOptionsReferenceOccProductType::CurrencyLongTerm
        }
        OccProductType::CurrencyMonthEnd => {
            OfficialOptionsReferenceOccProductType::CurrencyMonthEnd
        }
        OccProductType::CurrencyFlex => OfficialOptionsReferenceOccProductType::CurrencyFlex,
        OccProductType::IndexLongTerm => OfficialOptionsReferenceOccProductType::IndexLongTerm,
        OccProductType::IndexUnderlying => OfficialOptionsReferenceOccProductType::IndexUnderlying,
        OccProductType::IndexFlex => OfficialOptionsReferenceOccProductType::IndexFlex,
        OccProductType::InterestRateFutures => {
            OfficialOptionsReferenceOccProductType::InterestRateFutures
        }
        OccProductType::StockFutures => OfficialOptionsReferenceOccProductType::StockFutures,
        OccProductType::FuturesCashIndex => {
            OfficialOptionsReferenceOccProductType::FuturesCashIndex
        }
        OccProductType::FuturesPhysicalIndex => {
            OfficialOptionsReferenceOccProductType::FuturesPhysicalIndex
        }
        OccProductType::TreasuryUnderlying => {
            OfficialOptionsReferenceOccProductType::TreasuryUnderlying
        }
        OccProductType::TreasuryLongTerm => {
            OfficialOptionsReferenceOccProductType::TreasuryLongTerm
        }
    }
}

const fn map_occ_exchange_listing(
    value: OccExchangeListingEvidence,
) -> OfficialOptionsReferenceOccExchangeListingEvidence {
    match value {
        OccExchangeListingEvidence::Reported => {
            OfficialOptionsReferenceOccExchangeListingEvidence::Reported
        }
        OccExchangeListingEvidence::NotReportedInSelectedDirectory => {
            OfficialOptionsReferenceOccExchangeListingEvidence::NotReportedInSelectedDirectory
        }
    }
}

const fn map_occ_position_limit(
    value: OccPositionLimit,
) -> OfficialOptionsReferenceOccPositionLimit {
    match value {
        OccPositionLimit::EquityReported(value) => {
            OfficialOptionsReferenceOccPositionLimit::EquityReported(value)
        }
        OccPositionLimit::NonEquityUnavailableZero => {
            OfficialOptionsReferenceOccPositionLimit::NonEquityUnavailableZero
        }
        OccPositionLimit::NonEquityProviderValueOutsideDocumentedScope { raw_value } => {
            OfficialOptionsReferenceOccPositionLimit::NonEquityProviderValueOutsideDocumentedScope(
                raw_value,
            )
        }
    }
}

const fn occ_exchange_code(value: OccExchangeCode) -> char {
    match value {
        OccExchangeCode::Amex => 'A',
        OccExchangeCode::Box => 'B',
        OccExchangeCode::Cboe => 'C',
        OccExchangeCode::Emld => 'D',
        OccExchangeCode::Edgx => 'E',
        OccExchangeCode::Cfe => 'F',
        OccExchangeCode::Gem => 'H',
        OccExchangeCode::Ise => 'I',
        OccExchangeCode::Mcry => 'J',
        OccExchangeCode::Xmfe => 'K',
        OccExchangeCode::Sphr => 'L',
        OccExchangeCode::Miax => 'M',
        OccExchangeCode::Arca => 'P',
        OccExchangeCode::Nasdaq => 'Q',
        OccExchangeCode::Mprl => 'R',
        OccExchangeCode::Nobo => 'T',
        OccExchangeCode::Memx => 'U',
        OccExchangeCode::C2 => 'W',
        OccExchangeCode::Phlx => 'X',
        OccExchangeCode::Bats => 'Z',
    }
}

fn map_alias_key(
    key: &ReferenceAliasKey,
) -> Result<OfficialOptionsReferenceAliasKey, OfficialOptionsReferenceApplicationError> {
    Ok(match key {
        ReferenceAliasKey::CboeSymbol { symbol } => OfficialOptionsReferenceAliasKey::CboeSymbol {
            symbol: symbol.as_str().to_owned(),
        },
        ReferenceAliasKey::CboeOsi { osi } => {
            OfficialOptionsReferenceAliasKey::CboeOsi { osi: osi.clone() }
        }
        ReferenceAliasKey::CboeVenueSymbol { venue, symbol } => {
            OfficialOptionsReferenceAliasKey::CboeVenueSymbol {
                venue: cboe_venue(*venue)?,
                symbol: symbol.as_str().to_owned(),
            }
        }
        ReferenceAliasKey::OccProduct {
            options_symbol,
            product_type,
        } => OfficialOptionsReferenceAliasKey::OccProduct {
            options_symbol: options_symbol.clone(),
            product_type: map_occ_product_type(*product_type),
        },
    })
}

fn object_commitment(
    ordinal: u16,
    request_id: &SourceIdentifier,
    requested_at: Timestamp,
    request_deadline: Timestamp,
    handoff: &ReferenceModifiedObjectHandoff,
    selected_surface: &ReferenceSurface,
) -> Result<OfficialOptionsReferenceObjectCommitment, OfficialOptionsReferenceApplicationError> {
    let context = handoff.context();
    let raw = handoff.raw_receipt();
    let http = handoff.http_receipt();
    let transport = http.transport_evidence();
    let official_request = transport.request();
    if context.surface() != selected_surface
        || official_request.request_id() != request_id
        || official_request.wall_started_at() != requested_at
        || official_request.wall_deadline() != request_deadline
        || official_request.provider() != context.provider()
        || official_request.surface() != context.surface()
        || official_request.configured_locator() != context.configured_locator()
        || official_request.native_schema() != context.native_schema_identity()
        || http.status() != 200
        || !http.body_complete()
        || !transport.is_modified()
        || http.configured_locator() != context.configured_locator()
        || http.final_locator() != context.final_locator()
        || http.payload_digest() != context.payload_digest()
        || http.payload_bytes() != context.payload_bytes()
        || raw.content_digest() != context.payload_digest()
        || raw.size_bytes() != context.payload_bytes()
        || http.received_at() != context.clocks().received_at()
        || http.transport_elapsed_nanos() != context.clocks().transport_elapsed_nanos()
        || http.source_filename() != context.source_filename()
        || http.source_publication_date() != context.source_publication_date()
    {
        return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
    }
    let strict = match handoff {
        ReferenceModifiedObjectHandoff::Typed(value) => {
            let page = value.page_receipt();
            if page.context() != context
                || page.rejected_records() != 0
                || !matches!(page.terminal_state(), PageTerminalState::Terminal)
            {
                return Err(OfficialOptionsReferenceApplicationError::InvalidBinding);
            }
            OfficialOptionsReferenceStrictObjectCommitment::Typed {
                page_ordinal: page.page_ordinal(),
                returned_records: page.returned_records(),
                rejected_records: page.rejected_records(),
                strict_row_set_digest: page.strict_row_set_digest(),
                terminal_state: page.terminal_state().clone(),
            }
        }
        ReferenceModifiedObjectHandoff::UninterpretedMemo(_) => {
            OfficialOptionsReferenceStrictObjectCommitment::UninterpretedMemo
        }
    };
    Ok(OfficialOptionsReferenceObjectCommitment {
        ordinal,
        provider: context.provider(),
        surface: context.surface().clone(),
        source_id: official_request.source_id().clone(),
        provider_id: official_request.provider_id().clone(),
        source_contract_digest: official_request.source_contract_digest(),
        object_id: context.object_id().clone(),
        payload_digest: context.payload_digest(),
        payload_bytes: context.payload_bytes(),
        strict,
        posted: context.clocks().posted().cloned(),
        effective: context.clocks().effective().cloned(),
        availability: context.clocks().availability().clone(),
        received_at: context.clocks().received_at(),
        headers_received_at: http.headers_received_at(),
        transport_elapsed_nanos: http.transport_elapsed_nanos(),
        request_digest: http.request_digest(),
        transport_receipt_digest: http
            .evidence_digest()
            .map_err(|_error| OfficialOptionsReferenceApplicationError::InvalidBinding)?,
        configured_locator: context.configured_locator().clone(),
        final_locator: context.final_locator().clone(),
        redirect_chain: http.redirect_chain().to_vec().into_boxed_slice(),
        cache_last_modified: http
            .cache()
            .last_modified()
            .map(|value| value.as_str().to_owned()),
        http_last_modified: context.http_last_modified().cloned(),
        etag: http.cache().etag().map(|value| value.as_str().to_owned()),
        media_type: context.media_type().clone(),
        observed_content_type: http.observed_content_type().as_str().to_owned(),
        source_filename: context.source_filename().cloned(),
        source_publication_date: context.source_publication_date(),
        native_schema: context.native_schema_identity().clone(),
    })
}

/// Application binding, physical verification, or capacity failure.
#[derive(Debug, Error)]
pub(crate) enum OfficialOptionsReferenceApplicationError {
    #[error("official options-reference capture does not match its complete request closure")]
    InvalidBinding,
    #[error("official options-reference physical object does not match its store receipt")]
    PhysicalMismatch,
    #[error("official options-reference application capacity was exceeded")]
    Capacity,
    #[error(transparent)]
    Catalog(#[from] OfficialOptionsReferenceError),
    #[error(transparent)]
    PhysicalStore(#[from] SealedResearchJournalStoreError),
}
