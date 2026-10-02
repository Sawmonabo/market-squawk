//! Exact complete SEC research replay through the shared logical publication authority.
use super::filing_xbrl::SecNumericNativeEvidence;
use super::*;
use market_squawk_domain::CompanyIdentityObservation;
use market_squawk_sources::{
    LogicalObjectRole, LogicalPartitionFamily, ProviderCaptureSetReceipt,
    ProviderNativeSidecarDescriptor,
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchCompanion {
    version: u16,
    family: String,
    capture: ProviderCaptureSetReceipt,
    sealed_receipt_digest: EvidenceDigest,
    original_segment_claim: market_squawk_platform::SealedResearchJournalSegmentClaim,
    company_identity: CompanyIdentityObservation,
    native_descriptor: Vec<u8>,
    extraction_content_identity: EvidenceDigest,
    record_count: usize,
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RowMap {
    canonical_row_ordinal: u32,
    capture_page_ordinal: u16,
    segment_ordinal: u16,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
    canonical_record_digest: EvidenceDigest,
    native_semantic_digest: EvidenceDigest,
}

impl SecResearchReadCapability {
    #[allow(
        clippy::too_many_arguments,
        reason = "exact read authority and operation controls"
    )]
    pub(super) async fn select_logical(
        &self,
        request: SecResearchReadRequest,
        raw_store: &SealedResearchJournalStore,
        pinned: crate::PinnedDataset,
        source_id: SourceId,
        company_identity: CompanyIdentityExactRecord,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchSelection, SecResearchReadError> {
        let mismatch = || SecResearchReadError::ProviderBindingMismatch;
        let (companion_family, capture_page_ordinal) = match request.family() {
            SecResearchFamily::CompanyFacts => ("sec_company_facts_capture", 0),
            SecResearchFamily::FilingXbrl => ("sec_filing_capture", 1),
            SecResearchFamily::Submissions => return Err(mismatch()),
        };
        let control = SecResearchOperationControl {
            deadline,
            cancellation: &cancellation,
        };
        if !self.manifests.has_provider_publication(
            request.manifest(),
            request.provider_binding_digest(),
            "provider_logical",
        )? {
            return Err(mismatch());
        }
        let binding = self
            .identities
            .logical_publication_binding(
                request.provider_binding_digest(),
                deadline,
                &cancellation,
            )?
            .ok_or_else(mismatch)?;
        if binding.binding_digest() != request.provider_binding_digest()
            || binding.terminal().source_id() != &source_id
        {
            return Err(mismatch());
        }
        let components = binding
            .objects()
            .iter()
            .filter(|object| object.role() == LogicalObjectRole::ProviderComponent)
            .collect::<Vec<_>>();
        let [native_object, companion_object] = components.as_slice() else {
            return Err(mismatch());
        };
        if companion_object.claim().size_bytes() > request.maximum_object_bytes() as u64 {
            return Err(SecResearchReadError::ObjectBudgetExceeded);
        }
        let mut companion_reader = raw_store
            .open_verified_logical_object_claim(companion_object.claim(), &control)
            .map_err(map_raw_store_error)?;
        let companion: ResearchCompanion =
            serde_json::from_reader(&mut companion_reader).map_err(|_| mismatch())?;
        companion_reader
            .reverify_for_commit(&control)
            .map_err(map_raw_store_error)?;
        if companion.version != 1
            || companion.family != companion_family
            || &companion.company_identity != company_identity.observation()
            || companion.capture.source_id() != &source_id
            || companion.extraction_content_identity
                != binding.terminal().provider_terminal_evidence_digest()
            || Some(companion.sealed_receipt_digest)
                != binding.terminal().execution_attempt_digest()
            || companion.record_count as u64 != binding.terminal().total_canonical_rows()
            || companion.record_count > request.point_in_time_limits().max_candidates()
            || companion_object.semantic_identity() != companion_object.claim().content_digest()
        {
            return Err(mismatch());
        }
        market_squawk_sources::SealedProviderCaptureSetReceipt::verify_persisted_claim(
            &companion.capture,
            &companion.original_segment_claim,
            companion.sealed_receipt_digest,
        )
        .map_err(|_| mismatch())?;
        let payloads = binding
            .objects()
            .iter()
            .filter(|object| object.role() == LogicalObjectRole::ProviderPayload)
            .collect::<Vec<_>>();
        if payloads.len() != companion.capture.pages().len()
            || binding.objects().len() != payloads.len() + 2
        {
            return Err(mismatch());
        }
        for (object, page) in payloads.iter().zip(companion.capture.pages()) {
            check_operation(deadline, &cancellation)?;
            if object.ordinal() != u32::from(page.ordinal())
                || object.claim().content_digest() != page.body_digest()
                || object.claim().size_bytes() != page.body_bytes()
                || object.semantic_identity() != companion.sealed_receipt_digest
            {
                return Err(mismatch());
            }
            let verified = raw_store
                .open_verified_logical_object_claim(object.claim(), &control)
                .map_err(map_raw_store_error)?;
            verified
                .reverify_for_commit(&control)
                .map_err(map_raw_store_error)?;
        }
        let sidecar_digest = evidence_digest(Sha256::digest(&companion.native_descriptor).into());
        if sidecar_digest != native_object.semantic_identity() {
            return Err(mismatch());
        }
        let descriptor = match request.family() {
            SecResearchFamily::FilingXbrl => {
                let descriptor: ProviderNativeSidecarDescriptor =
                    serde_json::from_slice(&companion.native_descriptor).map_err(|_| mismatch())?;
                if descriptor.total_bytes() != native_object.claim().size_bytes()
                    || descriptor.content_digest() != native_object.claim().content_digest()
                {
                    return Err(mismatch());
                }
                Some(descriptor)
            }
            SecResearchFamily::CompanyFacts => {
                let sidecar: CompanyFactsSidecar =
                    serde_json::from_slice(&companion.native_descriptor).map_err(|_| mismatch())?;
                if native_object.claim().content_digest() != sidecar_digest
                    || native_object.claim().size_bytes() != companion.native_descriptor.len() as u64
                    || sidecar.version != 1
                    || sidecar.family != "company_facts"
                    || &sidecar.dataset != companion.capture.dataset()
                    || &sidecar.cik != company_identity.observation().provider_company_id()
                    || sidecar.entity_name != company_identity.observation().conformed_name()
                    || companion.capture.terminal()
                        != market_squawk_sources::ProviderCaptureTerminalDisposition::StandaloneResponse
                    || companion.capture.pages().len() != 1
                {
                    return Err(mismatch());
                }
                None
            }
            SecResearchFamily::Submissions => return Err(mismatch()),
        };
        let scratch = Arc::new(indexed::IndexScratch::new(
            self.objects.operation_scratch()?,
            request.maximum_spill_bytes(),
        ));
        let mut mappings = indexed::RowsBuilder::<RowMap>::new(Arc::clone(&scratch))?;
        let mut native_rows =
            indexed::RowsBuilder::<SecNumericNativeEvidence>::new(Arc::clone(&scratch))?;
        let map_schema =
            evidence_digest(Sha256::digest(b"market-squawk/sec-filing/logical-row-map/v1").into());
        let native_schema = market_squawk_sources::ProviderNativeLineageSchema::for_implementation(
            market_squawk_sources::ProviderNativeLineageImplementation::SecEdgarV1,
        )
        .fingerprint();
        let mut next_native = 0u64;
        let mut next_map = 0u64;
        let mut mapping_digest = Sha256::new();
        mapping_digest.update(b"market-squawk/sec-filing/logical-row-map-complete/v1");
        for partition in binding.partitions() {
            let next = match partition.family() {
                LogicalPartitionFamily::ProviderNative => &mut next_native,
                LogicalPartitionFamily::CanonicalRowMap => &mut next_map,
                _ => return Err(mismatch()),
            };
            if partition.item_range().first_ordinal() != *next {
                return Err(mismatch());
            }
            let expected = binding
                .canonical_partitions()
                .iter()
                .find(|expected| expected.partition_ordinal() == partition.partition_ordinal())
                .ok_or_else(mismatch)?;
            if expected.row_range() != partition.item_range()
                || (partition.family() == LogicalPartitionFamily::ProviderNative
                    && partition.schema_identity() != native_schema)
                || (partition.family() == LogicalPartitionFamily::CanonicalRowMap
                    && partition.schema_identity() != map_schema)
            {
                return Err(mismatch());
            }
            let mut reader = raw_store
                .open_verified_logical_object_claim(partition.claim(), &control)
                .map_err(map_raw_store_error)?;
            for _ in 0..partition.item_range().item_count().get() {
                check_operation(deadline, &cancellation)?;
                let (ordinal, payload) = read_frame(&mut reader)?;
                if ordinal != *next {
                    return Err(mismatch());
                }
                match partition.family() {
                    LogicalPartitionFamily::ProviderNative => {
                        native_rows.push(&SecNumericNativeEvidence {
                            semantic_payload: payload,
                            capture_page_ordinal,
                        })?
                    }
                    LogicalPartitionFamily::CanonicalRowMap => {
                        let row: RowMap =
                            serde_json::from_slice(&payload).map_err(|_| mismatch())?;
                        if u64::from(row.canonical_row_ordinal) != ordinal
                            || row.capture_page_ordinal != capture_page_ordinal
                            || row.segment_ordinal != 0
                            || row.physical_frame_ordinal != u32::from(row.capture_page_ordinal)
                        {
                            return Err(mismatch());
                        }
                        let page = companion
                            .capture
                            .pages()
                            .get(usize::from(row.capture_page_ordinal))
                            .ok_or_else(mismatch)?;
                        if row.page_body_digest != page.body_digest()
                            || row.received_at != page.received_at()
                            || row.source_sequence != Some(u64::from(page.ordinal()))
                        {
                            return Err(mismatch());
                        }
                        mapping_digest.update((payload.len() as u64).to_be_bytes());
                        mapping_digest.update(&payload);
                        mappings.push(&row)?;
                    }
                    _ => return Err(mismatch()),
                }
                *next = next.checked_add(1).ok_or_else(mismatch)?;
            }
            let mut tail = [0u8; 1];
            if reader.read(&mut tail).map_err(|_| mismatch())? != 0 {
                return Err(mismatch());
            }
            reader
                .reverify_for_commit(&control)
                .map_err(map_raw_store_error)?;
        }
        if next_native != companion.record_count as u64 || next_map != next_native {
            return Err(mismatch());
        }
        let mappings = mappings.finish()?;
        let native_rows = native_rows.finish()?;
        let object_ordinal = exact_origin_object_ordinal(&pinned, &company_identity)?;
        let object = pinned
            .objects()
            .get(object_ordinal)
            .ok_or(SecResearchReadError::OriginMismatch)?
            .object();
        if object.row_count() != next_native {
            return Err(mismatch());
        }
        let mut cursor = self.objects.pinned_object_batch_cursor(
            &pinned,
            company_identity.artifact_id(),
            object_ordinal,
            256,
            request.maximum_object_bytes(),
            &cancellation,
        )?;
        let mut observations =
            indexed::RowsBuilder::<ResearchObservation>::new(Arc::clone(&scratch))?;
        let mut coordinates = Vec::new();
        let coordinate_bytes = companion
            .record_count
            .checked_mul(size_of::<ProviderCaptureRowCoordinate>())
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        let base_bytes = coordinate_bytes
            .checked_add(3 * 1024 * 1024)
            .filter(|bytes| *bytes < request.maximum_object_bytes())
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        coordinates
            .try_reserve_exact(companion.record_count)
            .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?;
        let mut lineage = ResearchLineageDigestAccumulator::new();
        let mut row_ordinal = 0usize;
        loop {
            check_operation(deadline, &cancellation)?;
            let batch = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(SecResearchReadError::Cancelled),
                _ = tokio::time::sleep_until(deadline.into()) => return Err(SecResearchReadError::DeadlineExceeded),
                batch = cursor.next_batch() => batch?,
            };
            let Some(batch) = batch else {
                break;
            };
            let available = request
                .maximum_object_bytes()
                .checked_sub(base_bytes)
                .and_then(|bytes| bytes.checked_sub(batch.get_array_memory_size()))
                .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
            let decoded = crate::ResearchArrowBatch::decode_provider_logical_record_batch_bounded(
                batch,
                available,
                &mut lineage,
                &control,
            )
            .map_err(map_arrow_error)?;
            if &decoded.schema_ref != pinned.manifest().schema()
                || decoded.retained_bytes > available
            {
                return Err(mismatch());
            }
            for (observation, coordinate) in
                decoded.observations.into_iter().zip(decoded.coordinates)
            {
                let row = mappings.get(row_ordinal)?.ok_or_else(mismatch)?;
                let native = native_rows.get(row_ordinal)?.ok_or_else(mismatch)?;
                let expected = binding
                    .canonical_partitions()
                    .iter()
                    .find(|partition| partition.partition_ordinal() == coordinate.partition_ordinal)
                    .ok_or_else(mismatch)?;
                let end = expected
                    .row_range()
                    .end_exclusive()
                    .map_err(|_| mismatch())?;
                if coordinate.canonical_row_ordinal != row_ordinal as u64
                    || coordinate.binding_digest != binding.binding_digest()
                    || coordinate.canonical_row_digest != row.canonical_record_digest
                    || coordinate.native_semantic_digest != row.native_semantic_digest
                    || evidence_digest(Sha256::digest(&native.semantic_payload).into())
                        != row.native_semantic_digest
                    || coordinate.canonical_row_ordinal < expected.row_range().first_ordinal()
                    || coordinate.canonical_row_ordinal >= end
                    || !request.family().accepts(&observation)
                    || !observation_has_issuer(
                        &observation,
                        company_identity.observation().provider_company_id(),
                    )
                    || observation_context(&observation).provenance().source_id() != &source_id
                    || observation_context(&observation)
                        .provenance()
                        .instrument_id()
                        .is_some()
                {
                    return Err(mismatch());
                }
                if request.family() == SecResearchFamily::CompanyFacts {
                    validate_company_fact_native(&native.semantic_payload, &observation)?;
                }
                observations.push(&observation)?;
                coordinates.push(ProviderCaptureRowCoordinate {
                    binding_digest: binding.binding_digest(),
                    capture_observation_digest: companion.capture.observation_digest(),
                    canonical_row_ordinal: row.canonical_row_ordinal,
                    canonical_row_digest: coordinate.canonical_row_digest,
                    observation_digest: coordinate.observation_digest,
                    native_semantic_digest: row.native_semantic_digest,
                    capture_page_ordinal: row.capture_page_ordinal,
                    segment_ordinal: row.segment_ordinal,
                    physical_frame_ordinal: row.physical_frame_ordinal,
                    page_body_digest: row.page_body_digest,
                });
                row_ordinal += 1;
            }
        }
        if row_ordinal != companion.record_count
            || lineage.finish().bytes() != object.lineage_digest().bytes()
        {
            return Err(mismatch());
        }
        let observations = observations.finish()?;
        let mut native_reader = raw_store
            .open_verified_logical_object_claim(native_object.claim(), &control)
            .map_err(map_raw_store_error)?;
        let (filing, retained) = if let Some(descriptor) = descriptor {
            descriptor
                .verify_reader(&mut ControlledNativeReader {
                    reader: &mut native_reader,
                    deadline,
                    cancellation: &cancellation,
                })
                .map_err(|_| mismatch())?;
            native_reader.rewind().map_err(|_| mismatch())?;
            let (filing, retained) = filing_xbrl::read_verified_filing(
                &companion.capture,
                &mut native_reader,
                sidecar_digest,
                &native_rows,
                &observations,
                company_identity
                    .observation()
                    .provider_company_id()
                    .as_str(),
                request
                    .maximum_object_bytes()
                    .checked_sub(base_bytes)
                    .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
                deadline,
                &cancellation,
                &control,
                scratch,
            )?;
            (Some(filing), retained)
        } else {
            // The physically verified inline object has the exact digest/size of the already
            // parsed bounded sidecar. Facts have no filing graph to fabricate or retain.
            (None, companion.native_descriptor.len())
        };
        native_reader
            .reverify_for_commit(&control)
            .map_err(map_raw_store_error)?;
        drop(mappings);
        drop(native_rows);
        let mut origin = SecResearchOrigin {
            manifest: request.manifest().clone(),
            source_id,
            run_id: company_identity.run_id(),
            control_manifest_id: company_identity.manifest_id(),
            artifact_id: company_identity.artifact_id(),
            object_ordinal,
            relative_reference: company_identity
                .artifact_relative_reference()
                .to_owned()
                .into_boxed_str(),
            object_content_digest: object.content_hash().evidence(),
            object_lineage_digest: object.lineage_digest().evidence(),
            object_row_count: object.row_count(),
            object_size_bytes: object.size_bytes(),
            generation_completed_at: company_identity.completed_at(),
            origin_digest: evidence_digest([0; 32]),
        };
        origin.origin_digest = origin_digest(&origin);
        materialize_selection(
            request,
            origin,
            company_identity,
            companion.capture.observation_digest(),
            evidence_digest(mapping_digest.finalize().into()),
            observations,
            filing,
            coordinates,
            base_bytes
                .checked_add(retained)
                .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
            0,
            self.objects.operation_scratch()?,
            deadline,
            &cancellation,
        )
        .await
    }
}
fn read_frame(reader: &mut impl Read) -> Result<(u64, Vec<u8>), SecResearchReadError> {
    let mut ordinal = [0u8; 8];
    let mut length = [0u8; 8];
    reader
        .read_exact(&mut ordinal)
        .map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
    reader
        .read_exact(&mut length)
        .map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
    let length = usize::try_from(u64::from_le_bytes(length))
        .ok()
        .filter(|length| *length > 0 && *length <= 128 * 1024)
        .ok_or(SecResearchReadError::ProviderBindingMismatch)?;
    let mut bytes = vec![0; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
    Ok((u64::from_le_bytes(ordinal), bytes))
}

struct ControlledNativeReader<'a, R> {
    reader: &'a mut R,
    deadline: Instant,
    cancellation: &'a CancellationToken,
}
impl<R: Read> Read for ControlledNativeReader<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        check_operation(self.deadline, self.cancellation).map_err(std::io::Error::other)?;
        self.reader.read(bytes)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompanyFactsSidecar {
    version: u16,
    family: String,
    dataset: SourceIdentifier,
    cik: SourceIdentifier,
    entity_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompanyFactNative {
    family: String,
    occurrence: CompanyFactOccurrence,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompanyFactOccurrence {
    concept: SourceIdentifier,
    unit: SourceIdentifier,
    source_ordinal: u32,
    value: rust_decimal::Decimal,
    accession: SourceIdentifier,
    form: market_squawk_domain::FilingForm,
    filed_on: market_squawk_domain::CalendarDate,
    period: CompanyFactPeriod,
    frame: Option<SourceIdentifier>,
    fiscal_year: Option<u16>,
    fiscal_period: Option<SourceIdentifier>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompanyFactPeriod {
    start: Option<market_squawk_domain::CalendarDate>,
    end: market_squawk_domain::CalendarDate,
}

fn validate_company_fact_native(
    bytes: &[u8],
    observation: &ResearchObservation,
) -> Result<(), SecResearchReadError> {
    let mismatch = || SecResearchReadError::ProviderBindingMismatch;
    let native: CompanyFactNative = serde_json::from_slice(bytes).map_err(|_| mismatch())?;
    let ResearchObservation::Fundamental(fact) = observation else {
        return Err(mismatch());
    };
    let occurrence = native.occurrence;
    let context = fact.fact_context();
    // The source array ordinal is authenticated by its native digest; it is not the canonical
    // row ordinal, because SEC rows are sorted across concepts, units and amendments.
    let _source_ordinal = occurrence.source_ordinal;
    if native.family != "company_fact"
        || &occurrence.concept != fact.concept()
        || occurrence.value != fact.value()
        || &occurrence.unit != context.unit()
        || &occurrence.accession != context.accession()
        || Some(&occurrence.form) != context.filing_form()
        || Some(occurrence.filed_on) != context.filed_on()
        || occurrence.period.start != context.period().start()
        || occurrence.period.end != context.period().end()
        || occurrence.frame.as_ref() != context.frame()
        || occurrence.fiscal_year != context.fiscal_year()
        || occurrence.fiscal_period.as_ref() != context.fiscal_period()
        || fact.xbrl_evidence().is_some()
    {
        return Err(mismatch());
    }
    Ok(())
}
