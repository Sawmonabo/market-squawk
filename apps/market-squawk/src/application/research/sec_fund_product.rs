//! Closed product boundary for one official SEC fund publication.
//!
//! Presentation callers select only an SEC form family, quarter, accession, and the N-CEN fund
//! identifier required by that family. This leaf freezes the exact audited SEC catalogue
//! selection and injects every dataset, parser, storage, partition, and deadline authority. It
//! does not expose provider plumbing, perform network work, schedule a job, or claim that an
//! accession-driven publication is a complete Funds research workflow.

use market_squawk_adapter_sec::{
    SecBulkCoverage, SecBulkFamily, SecBulkParseLimits, SecBulkSelection,
    SecFundPartitionAdmissions, SecFundPublicationScope, SecQuarter,
};
use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRegistry, FundPointInTimeOutcome,
    FundPointInTimeSelection, PinnedDataset,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, FundCurrencyAmount, FundEvidenceRecord, FundHoldingQuantity,
    FundReportedDecimal, FundReportedValue, InstrumentId, SourceIdentifier, Timestamp,
};
use market_squawk_jobs::{JobGeneration, JobId};
use market_squawk_platform::ResearchObjectAdmission;
use market_squawk_sources::LogicalPartitionSetAdmission;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{SecFundPublicationReceipt, SecLiveFundRequest};

pub(crate) const SEC_FUND_START_PUBLICATION_OPERATION: &str = "Research.StartSecFundPublication";
const SEC_FUND_ANALYTICAL_DATASET: &str = "sec.fund-holdings.v1";
const SEC_FUND_OPERATION_DEADLINE_NANOS: i64 = 60 * 60 * 1_000_000_000;

// The adapter admits at most 100,000 selected source rows and 512 MiB of selected native bytes.
// Sixteen 64-MiB logical objects retain bounded framing headroom without exposing those physical
// choices to callers. The platform independently enforces 16-MiB integrity chunks.
const SEC_FUND_LOGICAL_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const SEC_FUND_LOGICAL_OBJECT_CHUNKS: usize = 4;
const SEC_FUND_MAXIMUM_PARTITIONS: u32 = 16;
const SEC_FUND_MAXIMUM_ITEMS_PER_PARTITION: u32 = 10_000;
const SEC_FUND_MAXIMUM_FRAME_BYTES: u64 = 4 * 1024 * 1024;
const SEC_FUND_PARSE_POLICY: &str = "sec-bulk-parse-production-defaults.v1";
const SEC_FUND_ADMISSION_DIGEST_DOMAIN: &[u8] = b"market-squawk/sec-fund-product-admission/v1";

/// Only the SEC filing families admitted by the fund publication product boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SecFundProductFamily {
    /// Form N-PORT portfolio reports and holdings.
    Nport,
    /// Form N-CEN investment-company annual reports.
    Ncen,
}

impl SecFundProductFamily {
    const fn adapter_family(self) -> SecBulkFamily {
        match self {
            Self::Nport => SecBulkFamily::Nport,
            Self::Ncen => SecBulkFamily::Ncen,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Nport => "nport",
            Self::Ncen => "ncen",
        }
    }
}

/// Closed presentation request for one accession-scoped official SEC fund publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SecFundProductRequest {
    family: SecFundProductFamily,
    year: u16,
    quarter: u8,
    accession: SourceIdentifier,
    #[serde(default)]
    fund_id: Option<SourceIdentifier>,
}

impl SecFundProductRequest {
    /// Builds the same closed request for typed non-JSON transports.
    pub(crate) fn try_new(
        family: SecFundProductFamily,
        year: u16,
        quarter: u8,
        accession: String,
        fund_id: Option<String>,
    ) -> Result<Self, SecFundProductBoundaryError> {
        Ok(Self {
            family,
            year,
            quarter,
            accession: SourceIdentifier::try_from(accession)
                .map_err(|_error| SecFundProductBoundaryError::InvalidRequest)?,
            fund_id: fund_id
                .map(SourceIdentifier::try_from)
                .transpose()
                .map_err(|_error| SecFundProductBoundaryError::InvalidRequest)?,
        })
    }

    pub(crate) const fn family(&self) -> SecFundProductFamily {
        self.family
    }

    pub(crate) const fn year(&self) -> u16 {
        self.year
    }

    pub(crate) const fn quarter(&self) -> u8 {
        self.quarter
    }

    pub(crate) const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    pub(crate) const fn fund_id(&self) -> Option<&SourceIdentifier> {
        self.fund_id.as_ref()
    }
}

/// Validated user coordinate retained across availability, queue, and publication results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecFundProductCoordinate {
    family: SecFundProductFamily,
    year: u16,
    quarter: u8,
    accession: SourceIdentifier,
    fund_id: Option<SourceIdentifier>,
}

impl SecFundProductCoordinate {
    pub(crate) const fn family(&self) -> SecFundProductFamily {
        self.family
    }

    pub(crate) const fn year(&self) -> u16 {
        self.year
    }

    pub(crate) const fn quarter(&self) -> u8 {
        self.quarter
    }

    pub(crate) const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    pub(crate) const fn fund_id(&self) -> Option<&SourceIdentifier> {
        self.fund_id.as_ref()
    }
}

/// Application-owned constructor for the exact SEC selection and all physical authorities.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SecFundProductRequestFactory;

impl SecFundProductRequestFactory {
    /// Validates presentation input and freezes the exact audited catalogue and physical bounds.
    pub(crate) fn admit(
        &self,
        request: SecFundProductRequest,
    ) -> Result<SecAdmittedFundProductRequest, SecFundProductBoundaryError> {
        validate_family_scope(&request)?;
        let quarter = SecQuarter::try_new(request.year, request.quarter)
            .map_err(|_error| SecFundProductBoundaryError::InvalidRequest)?;
        let selection = SecBulkSelection::current(request.family.adapter_family(), quarter)
            .map_err(|_error| SecFundProductBoundaryError::InvalidRequest)?;
        let scope = match request.family {
            SecFundProductFamily::Nport => {
                SecFundPublicationScope::try_nport(request.accession.clone())
            }
            SecFundProductFamily::Ncen => SecFundPublicationScope::try_ncen(
                request.accession.clone(),
                request
                    .fund_id
                    .clone()
                    .ok_or(SecFundProductBoundaryError::InvalidRequest)?,
            ),
        }
        .map_err(|_error| SecFundProductBoundaryError::InvalidRequest)?;
        let analytical_dataset = DatasetId::try_from(SEC_FUND_ANALYTICAL_DATASET)
            .map_err(|_error| SecFundProductBoundaryError::InvalidConfiguration)?;
        let object_admission = ResearchObjectAdmission::try_new(
            SEC_FUND_LOGICAL_OBJECT_BYTES,
            SEC_FUND_LOGICAL_OBJECT_CHUNKS,
        )
        .map_err(|_error| SecFundProductBoundaryError::InvalidConfiguration)?;
        let partition_admission = LogicalPartitionSetAdmission::try_new(
            object_admission,
            SEC_FUND_MAXIMUM_PARTITIONS,
            SEC_FUND_MAXIMUM_ITEMS_PER_PARTITION,
            SEC_FUND_MAXIMUM_FRAME_BYTES,
        )
        .map_err(|_error| SecFundProductBoundaryError::InvalidConfiguration)?;
        let coordinate = SecFundProductCoordinate {
            family: request.family,
            year: request.year,
            quarter: request.quarter,
            accession: request.accession,
            fund_id: request.fund_id,
        };
        let admission_digest =
            admitted_request_digest(&coordinate, &selection, &scope, &analytical_dataset);
        Ok(SecAdmittedFundProductRequest {
            coordinate,
            admission_digest,
            selection,
            scope,
            analytical_dataset,
            parse_limits: SecBulkParseLimits::production_defaults(),
            partition_admissions: SecFundPartitionAdmissions::new(
                partition_admission,
                partition_admission,
            ),
        })
    }

    /// Adds the code-owned absolute deadline immediately before a queued operation starts.
    pub(crate) fn prepare_for_run(
        &self,
        admitted: SecAdmittedFundProductRequest,
        started_at: Timestamp,
    ) -> Result<SecPreparedFundProductOperation, SecFundProductBoundaryError> {
        let deadline = started_at
            .checked_add_nanos(SEC_FUND_OPERATION_DEADLINE_NANOS)
            .map_err(|_error| SecFundProductBoundaryError::DeadlineUnavailable)?;
        let live_request = SecLiveFundRequest::try_new(
            admitted.selection,
            admitted.scope,
            admitted.analytical_dataset,
            admitted.parse_limits,
            admitted.partition_admissions,
            deadline,
        )
        .map_err(|_error| SecFundProductBoundaryError::InvalidConfiguration)?;
        Ok(SecPreparedFundProductOperation {
            coordinate: admitted.coordinate,
            live_request,
        })
    }

    /// Rejoins one runner-supplied live request to its exact code-owned product admission.
    pub(crate) fn validate_live_execution(
        &self,
        request: &SecLiveFundRequest,
        admitted_request_digest: EvidenceDigest,
    ) -> Result<SecFundProductCoordinate, SecFundProductBoundaryError> {
        let quarter = request.selection().quarter();
        let product = match request.scope() {
            SecFundPublicationScope::Nport { accession } => SecFundProductRequest::try_new(
                SecFundProductFamily::Nport,
                quarter.year(),
                quarter.quarter(),
                accession.as_str().to_owned(),
                None,
            )?,
            SecFundPublicationScope::Ncen { accession, fund_id } => SecFundProductRequest::try_new(
                SecFundProductFamily::Ncen,
                quarter.year(),
                quarter.quarter(),
                accession.as_str().to_owned(),
                Some(fund_id.as_str().to_owned()),
            )?,
        };
        let admitted = self.admit(product)?;
        if admitted.admission_digest != admitted_request_digest
            || admitted.selection != *request.selection()
            || admitted.scope != *request.scope()
            || admitted.analytical_dataset != *request.analytical_dataset()
            || admitted.parse_limits != request.parse_limits()
            || admitted.partition_admissions != request.partition_admissions()
        {
            return Err(SecFundProductBoundaryError::PublicationMismatch);
        }
        Ok(admitted.coordinate)
    }

    /// Reconstructs one durable product coordinate only when its admission digest still agrees.
    pub(crate) fn recover_coordinate(
        &self,
        family: SecFundProductFamily,
        year: u16,
        quarter: u8,
        accession: &SourceIdentifier,
        fund_id: Option<&SourceIdentifier>,
        admitted_request_digest: EvidenceDigest,
    ) -> Result<SecFundProductCoordinate, SecFundProductBoundaryError> {
        let admitted = self.admit(SecFundProductRequest::try_new(
            family,
            year,
            quarter,
            accession.as_str().to_owned(),
            fund_id.map(|value| value.as_str().to_owned()),
        )?)?;
        if admitted.admission_digest != admitted_request_digest {
            return Err(SecFundProductBoundaryError::PublicationMismatch);
        }
        Ok(admitted.coordinate)
    }
}

/// Immutable admitted request retained by the future durable job runner.
#[derive(Debug)]
pub(crate) struct SecAdmittedFundProductRequest {
    coordinate: SecFundProductCoordinate,
    admission_digest: EvidenceDigest,
    selection: SecBulkSelection,
    scope: SecFundPublicationScope,
    analytical_dataset: DatasetId,
    parse_limits: SecBulkParseLimits,
    partition_admissions: SecFundPartitionAdmissions,
}

impl SecAdmittedFundProductRequest {
    pub(crate) const fn coordinate(&self) -> &SecFundProductCoordinate {
        &self.coordinate
    }

    /// Exact presentation, catalogue, dataset, and physical-policy identity retained by the job.
    pub(crate) const fn admission_digest(&self) -> EvidenceDigest {
        self.admission_digest
    }
}

/// Exact live request plus the validated presentation coordinate that authored it.
#[derive(Debug)]
pub(crate) struct SecPreparedFundProductOperation {
    coordinate: SecFundProductCoordinate,
    live_request: SecLiveFundRequest,
}

impl SecPreparedFundProductOperation {
    pub(crate) const fn coordinate(&self) -> &SecFundProductCoordinate {
        &self.coordinate
    }

    pub(crate) fn into_parts(self) -> (SecFundProductCoordinate, SecLiveFundRequest) {
        (self.coordinate, self.live_request)
    }
}

/// Closed result of attempting to start or complete the publication operation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SecFundProductProjection {
    SetupRequired(SecFundSetupRequiredProjection),
    Unavailable(SecFundUnavailableProjection),
    Queued(SecFundQueuedProjection),
    Published(SecFundPublicationProjection),
}

impl SecFundProductProjection {
    pub(crate) fn setup_required(coordinate: SecFundProductCoordinate) -> Self {
        Self::SetupRequired(SecFundSetupRequiredProjection {
            coordinate,
            reason: SecFundSetupRequiredReason::SecSourceActivation,
        })
    }

    pub(crate) fn unavailable(coordinate: SecFundProductCoordinate) -> Self {
        Self::Unavailable(SecFundUnavailableProjection {
            coordinate,
            reason: SecFundUnavailableReason::ActivatedRuntimeUnavailable,
        })
    }

    pub(crate) fn queued(
        coordinate: SecFundProductCoordinate,
        job_id: JobId,
        generation: JobGeneration,
    ) -> Self {
        Self::Queued(SecFundQueuedProjection {
            coordinate,
            job_id,
            generation,
        })
    }

    pub(crate) fn try_published(
        coordinate: SecFundProductCoordinate,
        receipt: SecFundPublicationReceipt,
    ) -> Result<Self, SecFundProductBoundaryError> {
        SecFundPublicationProjection::try_from_receipt(coordinate, receipt).map(Self::Published)
    }
}

/// User-actionable absence of the optional activated SEC source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecFundSetupRequiredProjection {
    coordinate: SecFundProductCoordinate,
    reason: SecFundSetupRequiredReason,
}

impl SecFundSetupRequiredProjection {
    pub(crate) const fn coordinate(&self) -> &SecFundProductCoordinate {
        &self.coordinate
    }

    pub(crate) const fn reason(&self) -> SecFundSetupRequiredReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecFundSetupRequiredReason {
    SecSourceActivation,
}

/// Bounded non-user-actionable failure of an already activated SEC runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecFundUnavailableProjection {
    coordinate: SecFundProductCoordinate,
    reason: SecFundUnavailableReason,
}

impl SecFundUnavailableProjection {
    pub(crate) const fn coordinate(&self) -> &SecFundProductCoordinate {
        &self.coordinate
    }

    pub(crate) const fn reason(&self) -> SecFundUnavailableReason {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecFundUnavailableReason {
    ActivatedRuntimeUnavailable,
}

/// Durable job coordinate returned without waiting for SEC acquisition or publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecFundQueuedProjection {
    coordinate: SecFundProductCoordinate,
    job_id: JobId,
    generation: JobGeneration,
}

impl SecFundQueuedProjection {
    pub(crate) const fn coordinate(&self) -> &SecFundProductCoordinate {
        &self.coordinate
    }

    pub(crate) const fn job_id(&self) -> JobId {
        self.job_id
    }

    pub(crate) const fn generation(&self) -> JobGeneration {
        self.generation
    }
}

/// Exact immutable publication evidence suitable for a later closed wire projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecFundPublicationProjection {
    coordinate: SecFundProductCoordinate,
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    preparation_digest: EvidenceDigest,
    fund_instrument_id: InstrumentId,
    generation_row_count: u64,
    generation_total_bytes: u64,
    generation_object_count: usize,
}

impl SecFundPublicationProjection {
    fn try_from_receipt(
        coordinate: SecFundProductCoordinate,
        receipt: SecFundPublicationReceipt,
    ) -> Result<Self, SecFundProductBoundaryError> {
        let (manifest, binding_digest, preparation_digest, fund_instrument_id) = match &receipt {
            SecFundPublicationReceipt::Nport(receipt) => {
                let selector = receipt.restart_selector();
                if coordinate.family != SecFundProductFamily::Nport
                    || coordinate.accession != *selector.accession()
                    || coordinate.fund_id.is_some()
                {
                    return Err(SecFundProductBoundaryError::PublicationMismatch);
                }
                (
                    selector.manifest().clone(),
                    selector.binding_digest(),
                    selector.preparation_digest(),
                    selector.fund_instrument_id(),
                )
            }
            SecFundPublicationReceipt::Ncen(receipt) => {
                let selector = receipt.restart_selector();
                if coordinate.family != SecFundProductFamily::Ncen
                    || coordinate.accession != *selector.accession()
                    || coordinate.fund_id.as_ref() != Some(selector.fund_id())
                {
                    return Err(SecFundProductBoundaryError::PublicationMismatch);
                }
                (
                    selector.manifest().clone(),
                    selector.binding_digest(),
                    selector.preparation_digest(),
                    selector.fund_instrument_id(),
                )
            }
        };
        Self::try_from_durable_evidence(
            coordinate,
            manifest,
            binding_digest,
            preparation_digest,
            fund_instrument_id,
            receipt_committed(&receipt).pinned(),
        )
    }

    /// Reconstructs only evidence re-opened from the exact existing catalog and manifest.
    ///
    /// This is the recovery seam for the durable SEC job runner. Callers must first resolve the
    /// job-bound manifest and logical-publication coordinates through the sole analytical catalog;
    /// no latest-generation substitution is accepted here.
    pub(crate) fn try_from_durable_evidence(
        coordinate: SecFundProductCoordinate,
        manifest: DatasetManifestRef,
        binding_digest: EvidenceDigest,
        preparation_digest: EvidenceDigest,
        fund_instrument_id: InstrumentId,
        pinned: &PinnedDataset,
    ) -> Result<Self, SecFundProductBoundaryError> {
        let plan = pinned.plan();
        let expected_dataset = DatasetId::try_from(SEC_FUND_ANALYTICAL_DATASET)
            .map_err(|_error| SecFundProductBoundaryError::InvalidConfiguration)?;
        let expected_schema = DatasetSchemaRegistry::local()
            .canonical_fund_holdings()
            .map_err(|_error| SecFundProductBoundaryError::InvalidConfiguration)?;
        if manifest.dataset_id() != &expected_dataset
            || manifest.schema() != &expected_schema
            || pinned.manifest() != &manifest
            || plan.dataset_id() != manifest.dataset_id()
            || plan.content_hash() != manifest.content_hash()
            || plan.row_count() == 0
            || plan.total_bytes() == 0
            || plan.objects().is_empty()
            || !valid_digest(binding_digest)
            || !valid_digest(preparation_digest)
        {
            return Err(SecFundProductBoundaryError::PublicationMismatch);
        }
        Ok(Self {
            coordinate,
            manifest,
            binding_digest,
            preparation_digest,
            fund_instrument_id,
            generation_row_count: plan.row_count(),
            generation_total_bytes: plan.total_bytes(),
            generation_object_count: plan.objects().len(),
        })
    }

    pub(crate) const fn coordinate(&self) -> &SecFundProductCoordinate {
        &self.coordinate
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    pub(crate) const fn preparation_digest(&self) -> EvidenceDigest {
        self.preparation_digest
    }

    pub(crate) const fn fund_instrument_id(&self) -> InstrumentId {
        self.fund_instrument_id
    }

    pub(crate) const fn generation_row_count(&self) -> u64 {
        self.generation_row_count
    }

    pub(crate) const fn generation_total_bytes(&self) -> u64 {
        self.generation_total_bytes
    }

    pub(crate) const fn generation_object_count(&self) -> usize {
        self.generation_object_count
    }
}

/// Provider-neutral fund research data projected only from a verified point-in-time selection.
///
/// Exact manifests, accessions, source families, raw receipts, and publication plumbing remain in
/// the internal selector. Ordinary product pages receive canonical instrument identities, explicit
/// value states, coverage, and freshness only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FundResearchData {
    fund_instrument_id: InstrumentId,
    as_of: Timestamp,
    availability: FundResearchAvailability,
    report_count: usize,
    share_class_count: usize,
    holdings: Box<[FundHoldingData]>,
    latest_known_at: Option<Timestamp>,
}

impl FundResearchData {
    pub(crate) fn try_from_point_in_time(
        selection: &FundPointInTimeSelection,
        fund_instrument_id: InstrumentId,
        as_of: Timestamp,
    ) -> Result<Self, SecFundProductBoundaryError> {
        let (availability, records) = match selection.outcome() {
            FundPointInTimeOutcome::AsFiled { records, .. }
            | FundPointInTimeOutcome::AllKnown { records }
            | FundPointInTimeOutcome::LatestKnown { records, .. } => {
                (FundResearchAvailability::Available, records.as_ref())
            }
            FundPointInTimeOutcome::LatestUnavailable { .. } => {
                (FundResearchAvailability::Unavailable, &[][..])
            }
        };
        let mut report_count = 0_usize;
        let mut share_class_count = 0_usize;
        let mut holdings = Vec::new();
        let mut latest_known_at = None;
        for record in records {
            let filing = fund_record_filing(record);
            if filing.fund().instrument_id() != fund_instrument_id {
                return Err(SecFundProductBoundaryError::PublicationMismatch);
            }
            if let Some(known_at) = filing
                .chronology()
                .availability()
                .conservative_available_at()
            {
                latest_known_at = Some(
                    latest_known_at.map_or(known_at, |current: Timestamp| current.max(known_at)),
                );
            }
            match record {
                FundEvidenceRecord::Report(_) => {
                    report_count = report_count
                        .checked_add(1)
                        .ok_or(SecFundProductBoundaryError::PublicationMismatch)?;
                }
                FundEvidenceRecord::ShareClass(_) => {
                    share_class_count = share_class_count
                        .checked_add(1)
                        .ok_or(SecFundProductBoundaryError::PublicationMismatch)?;
                }
                FundEvidenceRecord::PortfolioHolding(holding) => {
                    holdings.push(FundHoldingData {
                        instrument_id: holding.held_security().instrument_id(),
                        quantity: holding.attributes().quantity().clone(),
                        value: holding.attributes().value().clone(),
                        percentage_of_net_assets: holding
                            .attributes()
                            .percentage_of_net_assets()
                            .clone(),
                    });
                }
            }
        }
        Ok(Self {
            fund_instrument_id,
            as_of,
            availability,
            report_count,
            share_class_count,
            holdings: holdings.into_boxed_slice(),
            latest_known_at,
        })
    }

    pub(crate) const fn fund_instrument_id(&self) -> InstrumentId {
        self.fund_instrument_id
    }
    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
    pub(crate) const fn availability(&self) -> FundResearchAvailability {
        self.availability
    }
    pub(crate) const fn report_count(&self) -> usize {
        self.report_count
    }
    pub(crate) const fn share_class_count(&self) -> usize {
        self.share_class_count
    }
    pub(crate) fn holdings(&self) -> &[FundHoldingData] {
        &self.holdings
    }
    pub(crate) const fn latest_known_at(&self) -> Option<Timestamp> {
        self.latest_known_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FundResearchAvailability {
    Available,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FundHoldingData {
    instrument_id: Option<InstrumentId>,
    quantity: FundReportedValue<FundHoldingQuantity>,
    value: FundReportedValue<FundCurrencyAmount>,
    percentage_of_net_assets: FundReportedValue<FundReportedDecimal>,
}

impl FundHoldingData {
    pub(crate) const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }
    pub(crate) const fn quantity(&self) -> &FundReportedValue<FundHoldingQuantity> {
        &self.quantity
    }
    pub(crate) const fn value(&self) -> &FundReportedValue<FundCurrencyAmount> {
        &self.value
    }
    pub(crate) const fn percentage_of_net_assets(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.percentage_of_net_assets
    }
}

fn fund_record_filing(record: &FundEvidenceRecord) -> &market_squawk_domain::FundFilingIdentity {
    match record {
        FundEvidenceRecord::Report(value) => value.filing(),
        FundEvidenceRecord::ShareClass(value) => value.filing(),
        FundEvidenceRecord::PortfolioHolding(value) => value.filing(),
    }
}

/// Fail-closed product-boundary error without provider payload or secret disclosure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SecFundProductBoundaryError {
    #[error("SEC fund publication request is invalid")]
    InvalidRequest,
    #[error("SEC fund publication configuration is invalid")]
    InvalidConfiguration,
    #[error("SEC fund publication deadline is unavailable")]
    DeadlineUnavailable,
    #[error("SEC fund publication evidence does not match its admitted request")]
    PublicationMismatch,
}

fn validate_family_scope(
    request: &SecFundProductRequest,
) -> Result<(), SecFundProductBoundaryError> {
    match (request.family, request.fund_id.as_ref()) {
        (SecFundProductFamily::Nport, None) | (SecFundProductFamily::Ncen, Some(_)) => Ok(()),
        (SecFundProductFamily::Nport, Some(_)) | (SecFundProductFamily::Ncen, None) => {
            Err(SecFundProductBoundaryError::InvalidRequest)
        }
    }
}

fn valid_digest(digest: EvidenceDigest) -> bool {
    digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes().iter().any(|byte| *byte != 0)
}

fn receipt_committed(receipt: &SecFundPublicationReceipt) -> &market_squawk_data::CommittedDataset {
    match receipt {
        SecFundPublicationReceipt::Nport(receipt) => receipt.committed(),
        SecFundPublicationReceipt::Ncen(receipt) => receipt.committed(),
    }
}

fn admitted_request_digest(
    coordinate: &SecFundProductCoordinate,
    selection: &SecBulkSelection,
    scope: &SecFundPublicationScope,
    dataset: &DatasetId,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(SEC_FUND_ADMISSION_DIGEST_DOMAIN);
    hash_text(&mut digest, coordinate.family.as_str());
    digest.update(coordinate.year.to_be_bytes());
    digest.update(coordinate.quarter.to_be_bytes());
    hash_text(&mut digest, coordinate.accession.as_str());
    hash_optional_text(
        &mut digest,
        coordinate.fund_id.as_ref().map(SourceIdentifier::as_str),
    );
    hash_text(&mut digest, selection.archive_locator().as_str());
    hash_text(&mut digest, selection.readme_locator().as_str());
    let catalog = selection.catalog_snapshot();
    hash_text(&mut digest, &catalog.audited_at().to_string());
    digest.update(catalog.latest_published().year().to_be_bytes());
    digest.update(catalog.latest_published().quarter().to_be_bytes());
    let schema = selection.accepted_schema();
    hash_text(&mut digest, schema.version().as_str());
    hash_text(&mut digest, &schema.effective_date().to_string());
    hash_text(&mut digest, schema.technical_spec_locator().as_str());
    match selection.coverage() {
        SecBulkCoverage::DerivedAsFiledIncludingAmendments => digest.update([1]),
        SecBulkCoverage::AcceptedSchemaExcluded { schema } => {
            digest.update([2]);
            hash_text(&mut digest, schema.version().as_str());
            hash_text(&mut digest, &schema.effective_date().to_string());
            hash_text(&mut digest, schema.technical_spec_locator().as_str());
        }
    }
    hash_text(&mut digest, scope.accession().as_str());
    hash_optional_text(&mut digest, scope.fund_id().map(SourceIdentifier::as_str));
    hash_text(&mut digest, dataset.as_str());
    hash_text(&mut digest, SEC_FUND_PARSE_POLICY);
    digest.update(SEC_FUND_LOGICAL_OBJECT_BYTES.to_be_bytes());
    digest.update((SEC_FUND_LOGICAL_OBJECT_CHUNKS as u64).to_be_bytes());
    digest.update(SEC_FUND_MAXIMUM_PARTITIONS.to_be_bytes());
    digest.update(SEC_FUND_MAXIMUM_ITEMS_PER_PARTITION.to_be_bytes());
    digest.update(SEC_FUND_MAXIMUM_FRAME_BYTES.to_be_bytes());
    digest.update(SEC_FUND_OPERATION_DEADLINE_NANOS.to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn hash_optional_text(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_text(digest, value);
        }
        None => digest.update([0]),
    }
}

fn hash_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}
