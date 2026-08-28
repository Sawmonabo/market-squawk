//! SEC fund-evidence Arrow publication and bounded point-in-time selection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, Date32Array, FixedSizeBinaryArray, StringArray,
    TimestampNanosecondArray, UInt16Array, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, FUND_HOLDINGS_SCHEMA_NAME, FUND_HOLDINGS_SCHEMA_VERSION,
    FundAmendmentState, FundEvidenceRecord, FundFilingIdentity, FundReleaseCoverage,
    FundRevisionLink, FundRevisionStatus, FundSourceFamily, FundSourceLineage, InstrumentId,
    SourceIdentifier, Timestamp,
};
use sha2::{Digest as _, Sha256};

use crate::arrow_convert::{ArrowConversionError, DatasetArrowBatch};
use crate::manifest::{DatasetId, DatasetManifestRef};
use crate::schema::{
    DATASET_KEY, DatasetSchemaRef, FUND_HOLDINGS_LINEAGE_DIGEST_KEY,
    FUND_HOLDINGS_PUBLICATION_DIGEST_KEY, decode_hex, fund_holdings_schema,
};

/// Maximum canonical fund records admitted to one Arrow batch or PIT result.
pub const MAX_FUND_HOLDINGS_BATCH_RECORDS: usize = 250_000;
/// Maximum retained Arrow-plus-decoded-payload bytes admitted by this typed boundary.
pub const MAX_FUND_HOLDINGS_RETAINED_BYTES: usize = 512 * 1024 * 1024;

const FUND_CONTENT_DOMAIN: &[u8] = b"market-squawk/fund-holdings/content/v1";
const FUND_LINEAGE_DOMAIN: &[u8] = b"market-squawk/fund-holdings/lineage/v1";
const FUND_SELECTION_DOMAIN: &[u8] = b"market-squawk/fund-holdings/pit-selection/v1";

/// One nonempty registered Arrow batch of closed canonical fund-evidence records.
#[derive(Clone, Debug)]
pub struct FundHoldingsArrowBatch {
    dataset: DatasetArrowBatch,
    records: Box<[FundEvidenceRecord]>,
    publication_digest: EvidenceDigest,
    lineage_digest: EvidenceDigest,
}

impl FundHoldingsArrowBatch {
    /// Encodes exact canonical fund evidence without flattening missing, conflict, or lineage state.
    pub fn try_from_records(
        dataset: SourceIdentifier,
        mut records: Vec<FundEvidenceRecord>,
    ) -> Result<Self, ArrowConversionError> {
        if records.is_empty() || records.len() > MAX_FUND_HOLDINGS_BATCH_RECORDS {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        canonical_sort(&mut records);
        validate_unique_records(&records)?;

        let mut payloads = Vec::new();
        payloads
            .try_reserve_exact(records.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        let mut payload_digests = Vec::new();
        payload_digests
            .try_reserve_exact(records.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        let mut lineage_digests = Vec::new();
        lineage_digests
            .try_reserve_exact(records.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        let mut retained_bytes = 0usize;

        for record in &records {
            validate_domain_schema(record)?;
            let payload = serde_json::to_vec(record)?;
            retained_bytes = retained_bytes
                .checked_add(payload.len())
                .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
            if retained_bytes > MAX_FUND_HOLDINGS_RETAINED_BYTES {
                return Err(ArrowConversionError::RetainedLimitExceeded);
            }
            payload_digests.push(sha256_evidence(&payload));
            payloads.push(payload);
            lineage_digests.push(lineage_digest(record_lineage(record))?);
        }

        let publication_digest = publication_digest(&dataset, &payload_digests)?;
        let lineage_digest = lineage_set_digest(&dataset, &records, &lineage_digests)?;
        let schema = fund_holdings_schema(&dataset, publication_digest, lineage_digest)?;
        let count = records.len();
        let row_ordinals = (0..count)
            .map(u32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?;

        let record_kinds = records.iter().map(record_kind).collect::<Vec<_>>();
        let source_ids = records
            .iter()
            .map(|record| record_filing(record).source_id().as_str())
            .collect::<Vec<_>>();
        let source_families = records
            .iter()
            .map(|record| family_name(record_filing(record).family()))
            .collect::<Vec<_>>();
        let instrument_ids = records
            .iter()
            .map(|record| {
                *record_filing(record)
                    .fund()
                    .instrument_id()
                    .as_uuid()
                    .as_bytes()
            })
            .collect::<Vec<_>>();
        let provider_series_ids = records
            .iter()
            .map(|record| record_filing(record).fund().provider_series_id().as_str())
            .collect::<Vec<_>>();
        let accessions = records
            .iter()
            .map(|record| record_filing(record).accession().as_str())
            .collect::<Vec<_>>();
        let forms = records
            .iter()
            .map(|record| record_filing(record).form().as_str())
            .collect::<Vec<_>>();
        let report_period_ends = records
            .iter()
            .map(|record| {
                record_filing(record)
                    .chronology()
                    .report_period_end()
                    .reported()
                    .copied()
                    .map(|date| date.days_since_unix_epoch())
            })
            .collect::<Vec<_>>();
        let accepted_at = records
            .iter()
            .map(|record| {
                record_filing(record)
                    .chronology()
                    .accepted_at()
                    .reported()
                    .copied()
                    .map(Timestamp::unix_nanos)
            })
            .collect::<Vec<_>>();
        let available_at = records
            .iter()
            .map(|record| {
                record_filing(record)
                    .chronology()
                    .availability()
                    .conservative_available_at()
                    .map(Timestamp::unix_nanos)
            })
            .collect::<Vec<_>>();
        let received_at = records
            .iter()
            .map(|record| {
                record_filing(record)
                    .chronology()
                    .received_at()
                    .unix_nanos()
            })
            .collect::<Vec<_>>();
        let ingested_at = records
            .iter()
            .map(|record| {
                record_filing(record)
                    .chronology()
                    .ingested_at()
                    .unix_nanos()
            })
            .collect::<Vec<_>>();
        let amendment_states = records
            .iter()
            .map(|record| amendment_name(record_filing(record).revision().amendment()))
            .collect::<Vec<_>>();
        let revision_statuses = records
            .iter()
            .map(|record| revision_status_name(record_filing(record).revision().status()))
            .collect::<Vec<_>>();
        let coverages = records
            .iter()
            .map(|record| coverage_name(record_filing(record).coverage()))
            .collect::<Vec<_>>();
        let holding_ids = records
            .iter()
            .map(|record| match record {
                FundEvidenceRecord::PortfolioHolding(value) => Some(value.holding_id().as_str()),
                FundEvidenceRecord::Report(_) | FundEvidenceRecord::ShareClass(_) => None,
            })
            .collect::<Vec<_>>();
        let held_instrument_ids = records
            .iter()
            .map(|record| match record {
                FundEvidenceRecord::PortfolioHolding(value) => value
                    .held_security()
                    .instrument_id()
                    .map(|instrument| instrument.as_uuid().as_bytes().to_vec()),
                FundEvidenceRecord::Report(_) | FundEvidenceRecord::ShareClass(_) => None,
            })
            .collect::<Vec<_>>();
        let native_generations = records
            .iter()
            .map(|record| record_lineage(record).native_generation().bytes().to_vec())
            .collect::<Vec<_>>();
        let layouts = records
            .iter()
            .map(|record| record_lineage(record).layout_evidence().bytes().to_vec())
            .collect::<Vec<_>>();
        let terminal_handoffs = records
            .iter()
            .map(|record| {
                record_lineage(record)
                    .terminal_handoff_evidence()
                    .bytes()
                    .to_vec()
            })
            .collect::<Vec<_>>();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from_value(FUND_HOLDINGS_SCHEMA_VERSION, count)),
            Arc::new(StringArray::from(record_kinds)),
            Arc::new(UInt32Array::from(row_ordinals)),
            Arc::new(StringArray::from(source_ids)),
            Arc::new(StringArray::from(source_families)),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                instrument_ids.iter().map(|bytes| bytes.as_slice()),
            )?),
            Arc::new(StringArray::from(provider_series_ids)),
            Arc::new(StringArray::from(accessions)),
            Arc::new(StringArray::from(forms)),
            Arc::new(Date32Array::from(report_period_ends)),
            Arc::new(TimestampNanosecondArray::from(accepted_at).with_timezone_utc()),
            Arc::new(TimestampNanosecondArray::from(available_at).with_timezone_utc()),
            Arc::new(TimestampNanosecondArray::from(received_at).with_timezone_utc()),
            Arc::new(TimestampNanosecondArray::from(ingested_at).with_timezone_utc()),
            Arc::new(StringArray::from(amendment_states)),
            Arc::new(StringArray::from(revision_statuses)),
            Arc::new(StringArray::from(coverages)),
            Arc::new(StringArray::from(holding_ids)),
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                held_instrument_ids.iter().map(Option::as_deref),
                16,
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                native_generations.iter().map(Vec::as_slice),
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                layouts.iter().map(Vec::as_slice),
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                terminal_handoffs.iter().map(Vec::as_slice),
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                lineage_digests.iter().map(|digest| digest.bytes()),
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                payload_digests.iter().map(|digest| digest.bytes()),
            )?),
            Arc::new(BinaryArray::from_iter_values(
                payloads.iter().map(Vec::as_slice),
            )),
        ];
        let batch = RecordBatch::try_new(schema, arrays)?;
        if batch.get_array_memory_size() > MAX_FUND_HOLDINGS_RETAINED_BYTES {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let dataset = DatasetArrowBatch::try_from_record_batch(batch)?;
        let candidate = Self {
            dataset,
            records: records.into_boxed_slice(),
            publication_digest,
            lineage_digest,
        };
        validate_unique_records(&candidate.records)?;
        if candidate
            .records
            .windows(2)
            .any(|pair| record_compare(&pair[0], &pair[1]).is_gt())
        {
            return Err(ArrowConversionError::ProjectionMismatch);
        }
        candidate.validate_projection()?;
        Ok(candidate)
    }

    /// Reopens and fully validates one retained fund batch under an exact memory ceiling.
    pub fn try_from_record_batch(
        batch: RecordBatch,
        maximum_retained_bytes: usize,
    ) -> Result<Self, ArrowConversionError> {
        if maximum_retained_bytes == 0 || maximum_retained_bytes > MAX_FUND_HOLDINGS_RETAINED_BYTES
        {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let dataset = DatasetArrowBatch::try_from_record_batch(batch)?;
        if dataset.schema_ref().name() != FUND_HOLDINGS_SCHEMA_NAME
            || dataset.record_batch().num_rows() > MAX_FUND_HOLDINGS_BATCH_RECORDS
        {
            return Err(ArrowConversionError::UnexpectedDatasetSchema);
        }
        let columns = FundColumns::try_from_batch(dataset.record_batch())?;
        let mut additional_bytes = 0usize;
        for row in 0..columns.payload_json.len() {
            additional_bytes = additional_bytes
                .checked_add(columns.payload_json.value(row).len())
                .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        }
        let admitted_bytes = dataset
            .record_batch()
            .get_array_memory_size()
            .checked_add(additional_bytes)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        if admitted_bytes > maximum_retained_bytes {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }

        let mut records = Vec::new();
        records
            .try_reserve_exact(dataset.record_batch().num_rows())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for row in 0..dataset.record_batch().num_rows() {
            let payload = columns.payload_json.value(row);
            let record: FundEvidenceRecord = serde_json::from_slice(payload)?;
            if serde_json::to_vec(&record)? != payload {
                return Err(ArrowConversionError::ProjectionMismatch);
            }
            records.push(record);
        }
        let metadata = dataset.record_batch().schema().metadata().clone();
        let publication_digest = metadata_digest(&metadata, FUND_HOLDINGS_PUBLICATION_DIGEST_KEY)?;
        let lineage_digest = metadata_digest(&metadata, FUND_HOLDINGS_LINEAGE_DIGEST_KEY)?;
        let candidate = Self {
            dataset,
            records: records.into_boxed_slice(),
            publication_digest,
            lineage_digest,
        };
        validate_unique_records(&candidate.records)?;
        if candidate
            .records
            .windows(2)
            .any(|pair| record_compare(&pair[0], &pair[1]).is_gt())
        {
            return Err(ArrowConversionError::ProjectionMismatch);
        }
        candidate.validate_projection()?;
        Ok(candidate)
    }

    /// Returns the registered dataset batch for Parquet publication.
    pub const fn dataset_batch(&self) -> &DatasetArrowBatch {
        &self.dataset
    }

    /// Returns the exact registered schema identity.
    pub const fn schema_ref(&self) -> &DatasetSchemaRef {
        self.dataset.schema_ref()
    }

    /// Returns validated canonical records in retained row order.
    pub fn records(&self) -> &[FundEvidenceRecord] {
        &self.records
    }

    /// Returns the ordered canonical content identity retained in schema metadata.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the ordered exact source-lineage-set identity retained in schema metadata.
    pub const fn lineage_digest(&self) -> EvidenceDigest {
        self.lineage_digest
    }

    fn validate_projection(&self) -> Result<(), ArrowConversionError> {
        let batch = self.dataset.record_batch();
        if self.records.is_empty() || self.records.len() != batch.num_rows() {
            return Err(ArrowConversionError::InvalidSchema);
        }
        let columns = FundColumns::try_from_batch(batch)?;
        let metadata = batch.schema().metadata().clone();
        let dataset = metadata
            .get(DATASET_KEY)
            .and_then(|value| SourceIdentifier::try_from(value.as_str()).ok())
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        let mut payload_digests = Vec::with_capacity(self.records.len());
        let mut lineage_digests = Vec::with_capacity(self.records.len());

        for (row, record) in self.records.iter().enumerate() {
            validate_domain_schema(record)?;
            let filing = record_filing(record);
            let lineage = record_lineage(record);
            let payload = serde_json::to_vec(record)?;
            let payload_digest = sha256_evidence(&payload);
            let row_lineage_digest = lineage_digest(lineage)?;
            let held_instrument = match record {
                FundEvidenceRecord::PortfolioHolding(value) => {
                    value.held_security().instrument_id()
                }
                FundEvidenceRecord::Report(_) | FundEvidenceRecord::ShareClass(_) => None,
            };
            let held_instrument_bytes =
                held_instrument.map(|instrument| *instrument.as_uuid().as_bytes());
            let holding_id = match record {
                FundEvidenceRecord::PortfolioHolding(value) => Some(value.holding_id().as_str()),
                FundEvidenceRecord::Report(_) | FundEvidenceRecord::ShareClass(_) => None,
            };
            if columns.schema_versions.value(row) != FUND_HOLDINGS_SCHEMA_VERSION
                || usize::try_from(columns.row_ordinals.value(row)).ok() != Some(row)
                || columns.record_kinds.value(row) != record_kind(record)
                || columns.source_ids.value(row) != filing.source_id().as_str()
                || columns.source_families.value(row) != family_name(filing.family())
                || columns.fund_instrument_ids.value(row)
                    != filing.fund().instrument_id().as_uuid().as_bytes()
                || columns.provider_series_ids.value(row)
                    != filing.fund().provider_series_id().as_str()
                || columns.accessions.value(row) != filing.accession().as_str()
                || columns.forms.value(row) != filing.form().as_str()
                || date32_value(columns.report_period_ends, row)
                    != filing
                        .chronology()
                        .report_period_end()
                        .reported()
                        .map(|date| date.days_since_unix_epoch())
                || timestamp_value(columns.accepted_at, row)
                    != filing
                        .chronology()
                        .accepted_at()
                        .reported()
                        .map(|timestamp| timestamp.unix_nanos())
                || timestamp_value(columns.available_at, row)
                    != filing
                        .chronology()
                        .availability()
                        .conservative_available_at()
                        .map(Timestamp::unix_nanos)
                || columns.received_at.value(row) != filing.chronology().received_at().unix_nanos()
                || columns.ingested_at.value(row) != filing.chronology().ingested_at().unix_nanos()
                || columns.amendment_states.value(row)
                    != amendment_name(filing.revision().amendment())
                || columns.revision_statuses.value(row)
                    != revision_status_name(filing.revision().status())
                || columns.coverages.value(row) != coverage_name(filing.coverage())
                || string_value(columns.holding_ids, row) != holding_id
                || fixed_binary_value(columns.held_instrument_ids, row)
                    != held_instrument_bytes.as_ref().map(|bytes| bytes.as_slice())
                || columns.native_generations.value(row) != lineage.native_generation().bytes()
                || columns.layouts.value(row) != lineage.layout_evidence().bytes()
                || columns.terminal_handoffs.value(row)
                    != lineage.terminal_handoff_evidence().bytes()
                || columns.lineage_digests.value(row) != row_lineage_digest.bytes()
                || columns.payload_digests.value(row) != payload_digest.bytes()
                || columns.payload_json.value(row) != payload
            {
                return Err(ArrowConversionError::ProjectionMismatch);
            }
            payload_digests.push(payload_digest);
            lineage_digests.push(row_lineage_digest);
        }

        if publication_digest(&dataset, &payload_digests)? != self.publication_digest
            || lineage_set_digest(&dataset, &self.records, &lineage_digests)? != self.lineage_digest
            || metadata_digest(&metadata, FUND_HOLDINGS_PUBLICATION_DIGEST_KEY)?
                != self.publication_digest
            || metadata_digest(&metadata, FUND_HOLDINGS_LINEAGE_DIGEST_KEY)? != self.lineage_digest
        {
            return Err(ArrowConversionError::ProjectionMismatch);
        }
        Ok(())
    }
}

/// Closed fund-filing revision policy for a fixed PIT read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FundPointInTimeRevisionMode {
    /// Return records for one exact accession as it was filed.
    AsFiled(SourceIdentifier),
    /// Return every knowable accession without choosing a winner.
    AllKnown,
    /// Select a unique latest accession only when the complete amendment chain proves it.
    LatestKnown,
}

/// Fixed bounded fund-evidence point-in-time request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FundPointInTimeRequest {
    dataset: DatasetId,
    fund_instrument_id: InstrumentId,
    source_family: Option<FundSourceFamily>,
    revision_mode: FundPointInTimeRevisionMode,
    knowledge_cutoff: Timestamp,
    maximum_records: usize,
    exact_manifest: Option<DatasetManifestRef>,
}

impl FundPointInTimeRequest {
    /// Requests one exact filing accession without applying amendment-currentness semantics.
    pub fn try_as_filed(
        dataset: DatasetId,
        fund_instrument_id: InstrumentId,
        source_family: Option<FundSourceFamily>,
        accession: SourceIdentifier,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            dataset,
            fund_instrument_id,
            source_family,
            FundPointInTimeRevisionMode::AsFiled(accession),
            knowledge_cutoff,
            maximum_records,
            exact_manifest,
        )
    }

    /// Requests all accessions knowable at the cutoff and performs no winner selection.
    pub fn try_all_known(
        dataset: DatasetId,
        fund_instrument_id: InstrumentId,
        source_family: Option<FundSourceFamily>,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            dataset,
            fund_instrument_id,
            source_family,
            FundPointInTimeRevisionMode::AllKnown,
            knowledge_cutoff,
            maximum_records,
            exact_manifest,
        )
    }

    /// Requests one latest accession for an exact filing family, failing closed if not provable.
    pub fn try_latest_known(
        dataset: DatasetId,
        fund_instrument_id: InstrumentId,
        source_family: FundSourceFamily,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            dataset,
            fund_instrument_id,
            Some(source_family),
            FundPointInTimeRevisionMode::LatestKnown,
            knowledge_cutoff,
            maximum_records,
            exact_manifest,
        )
    }

    fn try_new(
        dataset: DatasetId,
        fund_instrument_id: InstrumentId,
        source_family: Option<FundSourceFamily>,
        revision_mode: FundPointInTimeRevisionMode,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ArrowConversionError> {
        if maximum_records == 0
            || maximum_records > MAX_FUND_HOLDINGS_BATCH_RECORDS
            || exact_manifest.as_ref().is_some_and(|manifest| {
                manifest.dataset_id() != &dataset
                    || manifest.schema().name() != FUND_HOLDINGS_SCHEMA_NAME
            })
        {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        Ok(Self {
            dataset,
            fund_instrument_id,
            source_family,
            revision_mode,
            knowledge_cutoff,
            maximum_records,
            exact_manifest,
        })
    }

    pub const fn dataset(&self) -> &DatasetId {
        &self.dataset
    }
    pub const fn fund_instrument_id(&self) -> InstrumentId {
        self.fund_instrument_id
    }
    pub const fn source_family(&self) -> Option<FundSourceFamily> {
        self.source_family
    }
    pub const fn revision_mode(&self) -> &FundPointInTimeRevisionMode {
        &self.revision_mode
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub const fn maximum_records(&self) -> usize {
        self.maximum_records
    }
    pub const fn exact_manifest(&self) -> Option<&DatasetManifestRef> {
        self.exact_manifest.as_ref()
    }
}

/// Closed reason a unique latest filing cannot be established at the requested cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundLatestUnavailableReason {
    NoKnownRecords,
    IncompleteReleaseCoverage,
    RevisionConflict,
    RevisionUnavailable,
    UnresolvedRevisionLink,
    BrokenRevisionChain,
    NoCurrentRevision,
    MultipleCurrentRevisions,
}

/// Truthful result of one bounded fixed-cutoff fund-evidence selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FundPointInTimeOutcome {
    AsFiled {
        accession: SourceIdentifier,
        records: Box<[FundEvidenceRecord]>,
    },
    AllKnown {
        records: Box<[FundEvidenceRecord]>,
    },
    LatestKnown {
        accession: SourceIdentifier,
        records: Box<[FundEvidenceRecord]>,
    },
    LatestUnavailable {
        reason: FundLatestUnavailableReason,
        all_known_records: Box<[FundEvidenceRecord]>,
    },
}

impl FundPointInTimeOutcome {
    pub fn records(&self) -> &[FundEvidenceRecord] {
        match self {
            Self::AsFiled { records, .. }
            | Self::AllKnown { records }
            | Self::LatestKnown { records, .. } => records,
            Self::LatestUnavailable {
                all_known_records, ..
            } => all_known_records,
        }
    }
}

/// One exact manifest and explicit revision outcome selected at a fixed knowledge cutoff.
#[derive(Clone, Debug)]
pub struct FundPointInTimeSelection {
    manifest: DatasetManifestRef,
    outcome: FundPointInTimeOutcome,
    selection_digest: EvidenceDigest,
}

impl FundPointInTimeSelection {
    pub(crate) fn try_new(
        request: &FundPointInTimeRequest,
        manifest: DatasetManifestRef,
        batch: &FundHoldingsArrowBatch,
    ) -> Result<Self, ArrowConversionError> {
        if manifest.dataset_id() != request.dataset()
            || manifest.schema() != batch.schema_ref()
            || request
                .exact_manifest()
                .is_some_and(|expected| expected != &manifest)
        {
            return Err(ArrowConversionError::ExtractionBindingMismatch);
        }

        let mut known = batch
            .records()
            .iter()
            .filter(|record| record_matches_request(record, request))
            .cloned()
            .collect::<Vec<_>>();
        if known.len() > request.maximum_records() {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        canonical_sort(&mut known);

        let outcome = match request.revision_mode() {
            FundPointInTimeRevisionMode::AsFiled(accession) => {
                let records = known
                    .into_iter()
                    .filter(|record| record_filing(record).accession() == accession)
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                FundPointInTimeOutcome::AsFiled {
                    accession: accession.clone(),
                    records,
                }
            }
            FundPointInTimeRevisionMode::AllKnown => FundPointInTimeOutcome::AllKnown {
                records: known.into_boxed_slice(),
            },
            FundPointInTimeRevisionMode::LatestKnown => latest_outcome(known)?,
        };
        let selection_digest = selection_digest(request, &manifest, batch, &outcome)?;
        Ok(Self {
            manifest,
            outcome,
            selection_digest,
        })
    }

    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub const fn outcome(&self) -> &FundPointInTimeOutcome {
        &self.outcome
    }
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }
}

struct FundColumns<'a> {
    schema_versions: &'a UInt16Array,
    record_kinds: &'a StringArray,
    row_ordinals: &'a UInt32Array,
    source_ids: &'a StringArray,
    source_families: &'a StringArray,
    fund_instrument_ids: &'a FixedSizeBinaryArray,
    provider_series_ids: &'a StringArray,
    accessions: &'a StringArray,
    forms: &'a StringArray,
    report_period_ends: &'a Date32Array,
    accepted_at: &'a TimestampNanosecondArray,
    available_at: &'a TimestampNanosecondArray,
    received_at: &'a TimestampNanosecondArray,
    ingested_at: &'a TimestampNanosecondArray,
    amendment_states: &'a StringArray,
    revision_statuses: &'a StringArray,
    coverages: &'a StringArray,
    holding_ids: &'a StringArray,
    held_instrument_ids: &'a FixedSizeBinaryArray,
    native_generations: &'a FixedSizeBinaryArray,
    layouts: &'a FixedSizeBinaryArray,
    terminal_handoffs: &'a FixedSizeBinaryArray,
    lineage_digests: &'a FixedSizeBinaryArray,
    payload_digests: &'a FixedSizeBinaryArray,
    payload_json: &'a BinaryArray,
}

impl<'a> FundColumns<'a> {
    fn try_from_batch(batch: &'a RecordBatch) -> Result<Self, ArrowConversionError> {
        macro_rules! column {
            ($name:literal, $kind:ty) => {
                batch
                    .column_by_name($name)
                    .and_then(|column| column.as_any().downcast_ref::<$kind>())
                    .ok_or(ArrowConversionError::InvalidSchema)?
            };
        }
        let payload_json = column!("payload_json", BinaryArray);
        Ok(Self {
            schema_versions: column!("schema_version", UInt16Array),
            record_kinds: column!("record_kind", StringArray),
            row_ordinals: column!("canonical_row_ordinal", UInt32Array),
            source_ids: column!("source_id", StringArray),
            source_families: column!("source_family", StringArray),
            fund_instrument_ids: column!("fund_instrument_id", FixedSizeBinaryArray),
            provider_series_ids: column!("provider_series_id", StringArray),
            accessions: column!("accession", StringArray),
            forms: column!("form", StringArray),
            report_period_ends: column!("report_period_end", Date32Array),
            accepted_at: column!("accepted_at", TimestampNanosecondArray),
            available_at: column!("available_at", TimestampNanosecondArray),
            received_at: column!("received_at", TimestampNanosecondArray),
            ingested_at: column!("ingested_at", TimestampNanosecondArray),
            amendment_states: column!("amendment_state", StringArray),
            revision_statuses: column!("revision_status", StringArray),
            coverages: column!("release_coverage", StringArray),
            holding_ids: column!("holding_id", StringArray),
            held_instrument_ids: column!("held_instrument_id", FixedSizeBinaryArray),
            native_generations: column!("native_generation_sha256", FixedSizeBinaryArray),
            layouts: column!("layout_evidence_sha256", FixedSizeBinaryArray),
            terminal_handoffs: column!("terminal_handoff_evidence_sha256", FixedSizeBinaryArray),
            lineage_digests: column!("source_lineage_sha256", FixedSizeBinaryArray),
            payload_digests: column!("payload_sha256", FixedSizeBinaryArray),
            payload_json,
        })
    }
}

fn latest_outcome(
    all_known: Vec<FundEvidenceRecord>,
) -> Result<FundPointInTimeOutcome, ArrowConversionError> {
    if all_known.is_empty() {
        return Ok(latest_unavailable(
            FundLatestUnavailableReason::NoKnownRecords,
            all_known,
        ));
    }
    let mut filings = BTreeMap::<SourceIdentifier, &FundFilingIdentity>::new();
    for record in &all_known {
        let filing = record_filing(record);
        match filings.get(filing.accession()) {
            Some(retained) if *retained != filing => {
                return Err(ArrowConversionError::ProjectionMismatch);
            }
            Some(_) => {}
            None => {
                filings.insert(filing.accession().clone(), filing);
            }
        }
    }
    if filings
        .values()
        .any(|filing| !filing.coverage().is_complete())
    {
        return Ok(latest_unavailable(
            FundLatestUnavailableReason::IncompleteReleaseCoverage,
            all_known,
        ));
    }
    if filings
        .values()
        .any(|filing| filing.revision().status() == FundRevisionStatus::Conflict)
    {
        return Ok(latest_unavailable(
            FundLatestUnavailableReason::RevisionConflict,
            all_known,
        ));
    }
    if filings
        .values()
        .any(|filing| filing.revision().status() == FundRevisionStatus::Unavailable)
    {
        return Ok(latest_unavailable(
            FundLatestUnavailableReason::RevisionUnavailable,
            all_known,
        ));
    }
    if filings.values().any(|filing| {
        matches!(
            filing.revision().predecessor(),
            FundRevisionLink::Unresolved | FundRevisionLink::Conflict
        ) || matches!(
            filing.revision().successor(),
            FundRevisionLink::Unresolved | FundRevisionLink::Conflict
        )
    }) {
        return Ok(latest_unavailable(
            FundLatestUnavailableReason::UnresolvedRevisionLink,
            all_known,
        ));
    }

    let current = filings
        .values()
        .filter(|filing| filing.revision().status() == FundRevisionStatus::Current)
        .copied()
        .collect::<Vec<_>>();
    let current = match current.as_slice() {
        [] => {
            return Ok(latest_unavailable(
                FundLatestUnavailableReason::NoCurrentRevision,
                all_known,
            ));
        }
        [current] => *current,
        _ => {
            return Ok(latest_unavailable(
                FundLatestUnavailableReason::MultipleCurrentRevisions,
                all_known,
            ));
        }
    };

    if !complete_revision_chain(current, &filings) {
        return Ok(latest_unavailable(
            FundLatestUnavailableReason::BrokenRevisionChain,
            all_known,
        ));
    }
    let accession = current.accession().clone();
    let records = all_known
        .into_iter()
        .filter(|record| record_filing(record).accession() == &accession)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(FundPointInTimeOutcome::LatestKnown { accession, records })
}

fn complete_revision_chain(
    current: &FundFilingIdentity,
    filings: &BTreeMap<SourceIdentifier, &FundFilingIdentity>,
) -> bool {
    if !matches!(
        current.revision().successor(),
        FundRevisionLink::NotObserved
    ) {
        return false;
    }
    let mut visited = BTreeSet::new();
    let mut cursor = current;
    loop {
        if !visited.insert(cursor.accession().clone()) {
            return false;
        }
        match cursor.revision().amendment() {
            FundAmendmentState::Original => {
                if !matches!(
                    cursor.revision().predecessor(),
                    FundRevisionLink::NotApplicable
                ) {
                    return false;
                }
                break;
            }
            FundAmendmentState::Amendment => {
                let FundRevisionLink::Exact {
                    accession: predecessor,
                    ..
                } = cursor.revision().predecessor()
                else {
                    return false;
                };
                let Some(prior) = filings.get(predecessor).copied() else {
                    return false;
                };
                if prior.revision().status() != FundRevisionStatus::Superseded
                    || !matches!(
                        prior.revision().successor(),
                        FundRevisionLink::Exact { accession, .. } if accession == cursor.accession()
                    )
                {
                    return false;
                }
                cursor = prior;
            }
        }
    }
    visited.len() == filings.len()
}

fn latest_unavailable(
    reason: FundLatestUnavailableReason,
    all_known_records: Vec<FundEvidenceRecord>,
) -> FundPointInTimeOutcome {
    FundPointInTimeOutcome::LatestUnavailable {
        reason,
        all_known_records: all_known_records.into_boxed_slice(),
    }
}

fn record_matches_request(record: &FundEvidenceRecord, request: &FundPointInTimeRequest) -> bool {
    let filing = record_filing(record);
    filing.fund().instrument_id() == request.fund_instrument_id()
        && request
            .source_family()
            .is_none_or(|family| family == filing.family())
        && filing
            .chronology()
            .availability()
            .conservative_available_at()
            .is_some_and(|available| available <= request.knowledge_cutoff())
        && filing.chronology().received_at() <= request.knowledge_cutoff()
        && filing.chronology().ingested_at() <= request.knowledge_cutoff()
        && filing.fund().available_at() <= request.knowledge_cutoff()
        && filing.fund().observed_at() <= request.knowledge_cutoff()
}

fn canonical_sort(records: &mut [FundEvidenceRecord]) {
    records.sort_by(record_compare);
}

fn record_compare(left: &FundEvidenceRecord, right: &FundEvidenceRecord) -> std::cmp::Ordering {
    let left_filing = record_filing(left);
    let right_filing = record_filing(right);
    family_rank(left_filing.family())
        .cmp(&family_rank(right_filing.family()))
        .then_with(|| {
            left_filing
                .fund()
                .instrument_id()
                .as_uuid()
                .as_bytes()
                .cmp(right_filing.fund().instrument_id().as_uuid().as_bytes())
        })
        .then_with(|| {
            left_filing
                .fund()
                .provider_series_id()
                .cmp(right_filing.fund().provider_series_id())
        })
        .then_with(|| left_filing.accession().cmp(right_filing.accession()))
        .then_with(|| record_kind(left).cmp(record_kind(right)))
        .then_with(|| holding_id(left).cmp(&holding_id(right)))
}

fn validate_unique_records(records: &[FundEvidenceRecord]) -> Result<(), ArrowConversionError> {
    if records
        .windows(2)
        .any(|pair| record_compare(&pair[0], &pair[1]).is_eq())
    {
        Err(ArrowConversionError::InvalidSchema)
    } else {
        Ok(())
    }
}

fn selection_digest(
    request: &FundPointInTimeRequest,
    manifest: &DatasetManifestRef,
    batch: &FundHoldingsArrowBatch,
    outcome: &FundPointInTimeOutcome,
) -> Result<EvidenceDigest, ArrowConversionError> {
    let mut digest = Sha256::new();
    digest.update(FUND_SELECTION_DOMAIN);
    digest.update(request.dataset().as_str().as_bytes());
    digest.update(request.fund_instrument_id().as_uuid().as_bytes());
    digest.update(request.knowledge_cutoff().unix_nanos().to_be_bytes());
    digest.update(
        u64::try_from(request.maximum_records())
            .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
            .to_be_bytes(),
    );
    digest.update(
        request
            .source_family()
            .map_or(b"any".as_slice(), |family| family_name(family).as_bytes()),
    );
    match request.revision_mode() {
        FundPointInTimeRevisionMode::AsFiled(accession) => {
            digest.update(b"as_filed");
            digest.update(accession.as_str().as_bytes());
        }
        FundPointInTimeRevisionMode::AllKnown => digest.update(b"all_known"),
        FundPointInTimeRevisionMode::LatestKnown => digest.update(b"latest_known"),
    }
    digest.update(manifest.manifest_version().to_be_bytes());
    digest.update(manifest.schema().name().as_bytes());
    digest.update(manifest.schema().version().get().to_be_bytes());
    digest.update(manifest.schema().fingerprint());
    digest.update(manifest.content_hash().bytes());
    digest.update(batch.publication_digest().bytes());
    digest.update(batch.lineage_digest().bytes());
    digest.update(
        u64::try_from(outcome.records().len())
            .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
            .to_be_bytes(),
    );
    for (ordinal, record) in outcome.records().iter().enumerate() {
        let payload = serde_json::to_vec(record)?;
        digest.update(
            u32::try_from(ordinal)
                .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
                .to_be_bytes(),
        );
        digest.update(
            u64::try_from(payload.len())
                .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
                .to_be_bytes(),
        );
        digest.update(payload);
    }
    digest.update(match outcome {
        FundPointInTimeOutcome::AsFiled { .. } => b"as_filed".as_slice(),
        FundPointInTimeOutcome::AllKnown { .. } => b"all_known".as_slice(),
        FundPointInTimeOutcome::LatestKnown { .. } => b"latest_known".as_slice(),
        FundPointInTimeOutcome::LatestUnavailable { reason, .. } => {
            latest_unavailable_name(*reason).as_bytes()
        }
    });
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn publication_digest(
    dataset: &SourceIdentifier,
    payload_digests: &[EvidenceDigest],
) -> Result<EvidenceDigest, ArrowConversionError> {
    let mut digest = Sha256::new();
    digest.update(FUND_CONTENT_DOMAIN);
    digest.update(dataset.as_str().as_bytes());
    digest.update(
        u64::try_from(payload_digests.len())
            .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
            .to_be_bytes(),
    );
    for (ordinal, payload) in payload_digests.iter().enumerate() {
        digest.update(
            u32::try_from(ordinal)
                .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
                .to_be_bytes(),
        );
        digest.update(payload.bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn lineage_set_digest(
    dataset: &SourceIdentifier,
    records: &[FundEvidenceRecord],
    lineage_digests: &[EvidenceDigest],
) -> Result<EvidenceDigest, ArrowConversionError> {
    if records.len() != lineage_digests.len() {
        return Err(ArrowConversionError::InvalidSchema);
    }
    let mut digest = Sha256::new();
    digest.update(FUND_LINEAGE_DOMAIN);
    digest.update(dataset.as_str().as_bytes());
    digest.update(
        u64::try_from(records.len())
            .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
            .to_be_bytes(),
    );
    for (ordinal, (record, lineage)) in records.iter().zip(lineage_digests).enumerate() {
        digest.update(
            u32::try_from(ordinal)
                .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
                .to_be_bytes(),
        );
        digest.update(record_lineage(record).native_generation().bytes());
        digest.update(record_lineage(record).layout_evidence().bytes());
        digest.update(record_lineage(record).terminal_handoff_evidence().bytes());
        digest.update(lineage.bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn lineage_digest(lineage: &FundSourceLineage) -> Result<EvidenceDigest, ArrowConversionError> {
    Ok(sha256_evidence(&serde_json::to_vec(lineage)?))
}

fn sha256_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn metadata_digest(
    metadata: &std::collections::HashMap<String, String>,
    key: &str,
) -> Result<EvidenceDigest, ArrowConversionError> {
    metadata
        .get(key)
        .and_then(|value| decode_hex(value))
        .map(|bytes| EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .ok_or(ArrowConversionError::InvalidSchemaMetadata)
}

fn validate_domain_schema(record: &FundEvidenceRecord) -> Result<(), ArrowConversionError> {
    if record_filing(record).schema_version().get() != FUND_HOLDINGS_SCHEMA_VERSION {
        Err(ArrowConversionError::UnsupportedSchemaVersion {
            found: record_filing(record).schema_version().get(),
        })
    } else {
        Ok(())
    }
}

fn record_filing(record: &FundEvidenceRecord) -> &FundFilingIdentity {
    match record {
        FundEvidenceRecord::Report(value) => value.filing(),
        FundEvidenceRecord::ShareClass(value) => value.filing(),
        FundEvidenceRecord::PortfolioHolding(value) => value.filing(),
    }
}

fn record_lineage(record: &FundEvidenceRecord) -> &FundSourceLineage {
    match record {
        FundEvidenceRecord::Report(value) => value.lineage(),
        FundEvidenceRecord::ShareClass(value) => value.lineage(),
        FundEvidenceRecord::PortfolioHolding(value) => value.lineage(),
    }
}

fn holding_id(record: &FundEvidenceRecord) -> Option<&str> {
    match record {
        FundEvidenceRecord::PortfolioHolding(value) => Some(value.holding_id().as_str()),
        FundEvidenceRecord::Report(_) | FundEvidenceRecord::ShareClass(_) => None,
    }
}

const fn record_kind(record: &FundEvidenceRecord) -> &'static str {
    match record {
        FundEvidenceRecord::Report(_) => "report",
        FundEvidenceRecord::ShareClass(_) => "share_class",
        FundEvidenceRecord::PortfolioHolding(_) => "portfolio_holding",
    }
}

const fn family_name(family: FundSourceFamily) -> &'static str {
    match family {
        FundSourceFamily::Nport => "nport",
        FundSourceFamily::Ncen => "ncen",
    }
}

const fn family_rank(family: FundSourceFamily) -> u8 {
    match family {
        FundSourceFamily::Nport => 1,
        FundSourceFamily::Ncen => 2,
    }
}

const fn amendment_name(amendment: FundAmendmentState) -> &'static str {
    match amendment {
        FundAmendmentState::Original => "original",
        FundAmendmentState::Amendment => "amendment",
    }
}

const fn revision_status_name(status: FundRevisionStatus) -> &'static str {
    match status {
        FundRevisionStatus::Current => "current",
        FundRevisionStatus::Superseded => "superseded",
        FundRevisionStatus::Conflict => "conflict",
        FundRevisionStatus::Unavailable => "unavailable",
    }
}

const fn coverage_name(coverage: &FundReleaseCoverage) -> &'static str {
    match coverage {
        FundReleaseCoverage::Complete => "complete",
        FundReleaseCoverage::AcceptedSchemaExclusion { .. } => "accepted_schema_exclusion",
        FundReleaseCoverage::Incomplete { .. } => "incomplete",
    }
}

const fn latest_unavailable_name(reason: FundLatestUnavailableReason) -> &'static str {
    match reason {
        FundLatestUnavailableReason::NoKnownRecords => "no_known_records",
        FundLatestUnavailableReason::IncompleteReleaseCoverage => "incomplete_release_coverage",
        FundLatestUnavailableReason::RevisionConflict => "revision_conflict",
        FundLatestUnavailableReason::RevisionUnavailable => "revision_unavailable",
        FundLatestUnavailableReason::UnresolvedRevisionLink => "unresolved_revision_link",
        FundLatestUnavailableReason::BrokenRevisionChain => "broken_revision_chain",
        FundLatestUnavailableReason::NoCurrentRevision => "no_current_revision",
        FundLatestUnavailableReason::MultipleCurrentRevisions => "multiple_current_revisions",
    }
}

fn date32_value(array: &Date32Array, row: usize) -> Option<i32> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn timestamp_value(array: &TimestampNanosecondArray, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn string_value(array: &StringArray, row: usize) -> Option<&str> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn fixed_binary_value(array: &FixedSizeBinaryArray, row: usize) -> Option<&[u8]> {
    (!array.is_null(row)).then(|| array.value(row))
}
