use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::catalog::SelectedFileReceipt;
use crate::decode::{
    DecodeChannelRole, DecodeContract, DecodeLimits, DplcChannelDistributionContract,
};
use crate::model::{DateError, PcapObjectEncoding, Sha256Digest, TradeDate};

/// Code-owned name of the sole provider lane used for IEX HIST discovery and files.
pub const IEX_HIST_PROVIDER_LANE: &str = "iex-hist-cold";

/// Explicit scheduler lane for IEX HIST work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ScheduleLane {
    /// Background historical work that yields to interactive/current-data work.
    Cold,
}

/// Authority that explicitly requested one exact feed/date artifact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ColdJobTrigger {
    /// Direct operator request.
    Operator,
    /// Bounded research job with a preselected artifact.
    ResearchJob,
}

/// Resume behavior allowed by the core plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ResumePolicy {
    /// Whole-file restart or adoption of independently reverified complete durable evidence only.
    ReverifyCompleteEvidenceOrRestartWholeFile,
}

/// Independently accounted capacity categories for one complete selected-file pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistCapacityCategory {
    /// Exact bytes received from the provider.
    NetworkResponse,
    /// Immutable exact mutable-catalog JSON evidence.
    DurableCatalog,
    /// Incomplete compressed object owned by the transfer phase.
    TemporaryCompressed,
    /// Immutable compressed raw object admitted by shared storage.
    DurableCompressed,
    /// Incomplete expanded PCAP owned by the materialization phase.
    TemporaryPcap,
    /// Immutable expanded PCAP raw object admitted by shared storage.
    DurablePcap,
    /// Complete provider-native decoded event batch.
    DecodedEventBatch,
    /// Canonical Arrow validation/staging batch.
    CanonicalArrow,
    /// Immutable canonical Parquet generation.
    ImmutableParquet,
    /// Manifest, journal, directory-sync, and atomic-publication overhead.
    ManifestAndAtomicOverhead,
    /// Bytes the application must keep free throughout the reservation.
    SafetyFreeReserve,
}

/// Complete worst-case resource footprint pre-reserved before an operation starts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IexHistCapacityFootprint {
    network_response: u64,
    durable_catalog: u64,
    temporary_compressed: u64,
    durable_compressed: u64,
    temporary_pcap: u64,
    durable_pcap: u64,
    decoded_event_batch: u64,
    canonical_arrow: u64,
    immutable_parquet: u64,
    manifest_and_atomic_overhead: u64,
    safety_free_reserve: u64,
}

impl IexHistCapacityFootprint {
    pub(crate) fn catalog(
        max_catalog_bytes: u64,
        atomic_overhead_bytes: u64,
        safety_free_reserve: u64,
    ) -> Result<Self, PlanError> {
        if max_catalog_bytes == 0 || atomic_overhead_bytes == 0 || safety_free_reserve == 0 {
            return Err(PlanError::InvalidLimits);
        }
        Ok(Self {
            network_response: max_catalog_bytes,
            durable_catalog: max_catalog_bytes,
            temporary_compressed: 0,
            durable_compressed: 0,
            temporary_pcap: 0,
            durable_pcap: 0,
            decoded_event_batch: 0,
            canonical_arrow: 0,
            immutable_parquet: 0,
            manifest_and_atomic_overhead: atomic_overhead_bytes,
            safety_free_reserve,
        })
    }

    fn selected_file(
        selected_bytes: u64,
        object_encoding: PcapObjectEncoding,
        limits: ByteAdmissionLimits,
    ) -> Result<Self, PlanError> {
        let (temporary_compressed, durable_compressed) = match object_encoding {
            PcapObjectEncoding::Gzip => (selected_bytes, selected_bytes),
            PcapObjectEncoding::Identity => (0, 0),
        };
        let footprint = Self {
            network_response: selected_bytes,
            durable_catalog: 0,
            temporary_compressed,
            durable_compressed,
            temporary_pcap: limits.max_pcap_bytes,
            durable_pcap: limits.max_pcap_bytes,
            decoded_event_batch: limits.max_decoded_event_batch_bytes,
            canonical_arrow: limits.max_canonical_arrow_bytes,
            immutable_parquet: limits.max_parquet_bytes,
            manifest_and_atomic_overhead: limits.manifest_and_atomic_overhead_bytes,
            safety_free_reserve: limits.required_free_reserve_bytes,
        };
        footprint.total_reserved_bytes()?;
        Ok(footprint)
    }

    /// Returns the exact reservation for one typed category.
    #[must_use]
    pub const fn bytes(self, category: IexHistCapacityCategory) -> u64 {
        match category {
            IexHistCapacityCategory::NetworkResponse => self.network_response,
            IexHistCapacityCategory::DurableCatalog => self.durable_catalog,
            IexHistCapacityCategory::TemporaryCompressed => self.temporary_compressed,
            IexHistCapacityCategory::DurableCompressed => self.durable_compressed,
            IexHistCapacityCategory::TemporaryPcap => self.temporary_pcap,
            IexHistCapacityCategory::DurablePcap => self.durable_pcap,
            IexHistCapacityCategory::DecodedEventBatch => self.decoded_event_batch,
            IexHistCapacityCategory::CanonicalArrow => self.canonical_arrow,
            IexHistCapacityCategory::ImmutableParquet => self.immutable_parquet,
            IexHistCapacityCategory::ManifestAndAtomicOverhead => {
                self.manifest_and_atomic_overhead
            }
            IexHistCapacityCategory::SafetyFreeReserve => self.safety_free_reserve,
        }
    }

    /// Returns the complete network-plus-disk reservation, including temporary/durable overlap.
    pub fn total_reserved_bytes(self) -> Result<u64, PlanError> {
        CAPACITY_CATEGORIES.into_iter().try_fold(0_u64, |total, category| {
            total
                .checked_add(self.bytes(category))
                .ok_or(PlanError::CapacityArithmetic)
        })
    }

    /// Returns all durable-volume categories, including temp/durable overlap and free reserve.
    pub fn required_disk_bytes(self) -> Result<u64, PlanError> {
        CAPACITY_CATEGORIES
            .into_iter()
            .filter(|category| *category != IexHistCapacityCategory::NetworkResponse)
            .try_fold(0_u64, |total, category| {
                total
                    .checked_add(self.bytes(category))
                    .ok_or(PlanError::CapacityArithmetic)
            })
    }

    fn covers(self, required: Self) -> bool {
        CAPACITY_CATEGORIES
            .into_iter()
            .all(|category| self.bytes(category) >= required.bytes(category))
    }

    fn identity(self) -> Sha256Digest {
        crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-capacity-footprint/v1",
            &self.network_response.to_le_bytes(),
            &self.durable_catalog.to_le_bytes(),
            &self.temporary_compressed.to_le_bytes(),
            &self.durable_compressed.to_le_bytes(),
            &self.temporary_pcap.to_le_bytes(),
            &self.durable_pcap.to_le_bytes(),
            &self.decoded_event_batch.to_le_bytes(),
            &self.canonical_arrow.to_le_bytes(),
            &self.immutable_parquet.to_le_bytes(),
            &self.manifest_and_atomic_overhead.to_le_bytes(),
            &self.safety_free_reserve.to_le_bytes(),
        ])
    }
}

const CAPACITY_CATEGORIES: [IexHistCapacityCategory; 11] = [
    IexHistCapacityCategory::NetworkResponse,
    IexHistCapacityCategory::DurableCatalog,
    IexHistCapacityCategory::TemporaryCompressed,
    IexHistCapacityCategory::DurableCompressed,
    IexHistCapacityCategory::TemporaryPcap,
    IexHistCapacityCategory::DurablePcap,
    IexHistCapacityCategory::DecodedEventBatch,
    IexHistCapacityCategory::CanonicalArrow,
    IexHistCapacityCategory::ImmutableParquet,
    IexHistCapacityCategory::ManifestAndAtomicOverhead,
    IexHistCapacityCategory::SafetyFreeReserve,
];

/// Exact upper bounds used to construct the complete selected-file footprint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteAdmissionLimits {
    /// Maximum admitted provider-advertised compressed bytes.
    pub max_compressed_bytes: u64,
    /// Maximum PCAP bytes allowed after streaming gzip expansion.
    pub max_pcap_bytes: u64,
    /// Maximum serialized complete provider-native decoded batch.
    pub max_decoded_event_batch_bytes: u64,
    /// Maximum canonical Arrow validation/staging bytes.
    pub max_canonical_arrow_bytes: u64,
    /// Maximum immutable Parquet bytes.
    pub max_parquet_bytes: u64,
    /// Manifest, journal, sync, and atomic-promotion overhead.
    pub manifest_and_atomic_overhead_bytes: u64,
    /// Bytes that remain unavailable to jobs as the application safety reserve.
    pub required_free_reserve_bytes: u64,
    /// Maximum age of the selected catalog receipt when an execution attempt is admitted.
    pub max_catalog_age_nanos: u64,
    /// Maximum wall-clock duration classified as normal for a complete file response.
    pub max_download_duration_nanos: u64,
    /// Maximum tolerated backward wall-clock movement during a transfer.
    pub max_clock_regression_nanos: u64,
}

/// Exact admission and scheduling contract for one immutable IEX HIST file selection.
///
/// An execution deadline is intentionally absent. A later attempt keeps this identity and may
/// adopt complete reverified raw/decode evidence under a new authority-owned attempt deadline.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ColdJobPlan {
    pub(crate) plan_sha256: Sha256Digest,
    pub(crate) selected_file: SelectedFileReceipt,
    pub(crate) trigger: ColdJobTrigger,
    pub(crate) lane: ScheduleLane,
    pub(crate) automatic_archive_catch_up: bool,
    pub(crate) max_parallel_transfers: u8,
    pub(crate) resume_policy: ResumePolicy,
    pub(crate) earliest_available_on: TradeDate,
    pub(crate) rolling_window_start: TradeDate,
    pub(crate) advertised_compressed_bytes: u64,
    pub(crate) max_pcap_bytes: u64,
    pub(crate) object_encoding: PcapObjectEncoding,
    pub(crate) decode_contract: DecodeContract,
    pub(crate) capacity_footprint: IexHistCapacityFootprint,
    pub(crate) max_catalog_age_nanos: u64,
    pub(crate) max_download_duration_nanos: u64,
    pub(crate) max_clock_regression_nanos: u64,
}

impl ColdJobPlan {
    #[must_use]
    pub const fn plan_sha256(&self) -> Sha256Digest { self.plan_sha256 }
    #[must_use]
    pub const fn selected_file(&self) -> &SelectedFileReceipt { &self.selected_file }
    #[must_use]
    pub const fn trigger(&self) -> ColdJobTrigger { self.trigger }
    #[must_use]
    pub const fn lane(&self) -> ScheduleLane { self.lane }
    #[must_use]
    pub const fn automatic_archive_catch_up(&self) -> bool { self.automatic_archive_catch_up }
    #[must_use]
    pub const fn max_parallel_transfers(&self) -> u8 { self.max_parallel_transfers }
    #[must_use]
    pub const fn earliest_available_on(&self) -> TradeDate { self.earliest_available_on }
    #[must_use]
    pub const fn rolling_window_start(&self) -> TradeDate { self.rolling_window_start }
    #[must_use]
    pub const fn capacity_footprint(&self) -> IexHistCapacityFootprint { self.capacity_footprint }
    #[must_use]
    pub const fn object_encoding(&self) -> PcapObjectEncoding { self.object_encoding }
    #[must_use]
    pub const fn decode_contract(&self) -> DecodeContract { self.decode_contract }
    /// Returns the complete reserved footprint, including overlap and protected free space.
    pub fn required_disk_bytes(&self) -> Result<u64, PlanError> {
        self.capacity_footprint.required_disk_bytes()
    }

    /// Encodes the complete closed plan needed for independent same-root reconstruction.
    pub fn durable_envelope(&self) -> Result<Vec<u8>, PlanError> {
        let envelope = DurableColdJobPlanEnvelope {
            schema_version: 2,
            plan_sha256: self.plan_sha256,
            trigger: self.trigger,
            catalog_sha256: self.selected_file.catalog_sha256,
            catalog_bytes: self.selected_file.catalog_bytes,
            catalog_observation: DurableCatalogObservationEnvelope::from_receipt(
                &self.selected_file.catalog_observation,
            ),
            descriptor_sha256: self.selected_file.descriptor_sha256,
            trade_date: self.selected_file.trade_date.compact(),
            feed: self.selected_file.feed.catalog_name().to_owned(),
            feed_version: self.selected_file.feed_version.catalog_value().to_owned(),
            transport_version: self.selected_file.transport_version.catalog_value().to_owned(),
            object_encoding: self.selected_file.object_encoding.identity_value().to_owned(),
            file_name: self.selected_file.file_name.clone(),
            download_url: self.selected_file.download_url.clone(),
            advertised_compressed_bytes: self.selected_file.advertised_compressed_bytes,
            decode_contract: self.decode_contract,
            capacity_footprint: self.capacity_footprint,
            max_catalog_age_nanos: self.max_catalog_age_nanos,
            max_download_duration_nanos: self.max_download_duration_nanos,
            max_clock_regression_nanos: self.max_clock_regression_nanos,
        };
        let payload = serde_json::to_vec(&envelope).map_err(|_| PlanError::InvalidEnvelope)?;
        if payload.is_empty() || payload.len() > MAX_DURABLE_PLAN_BYTES {
            return Err(PlanError::InvalidEnvelope);
        }
        Ok(payload)
    }
}

const MAX_DURABLE_PLAN_BYTES: usize = 16 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableColdJobPlanEnvelope {
    schema_version: u16,
    plan_sha256: Sha256Digest,
    trigger: ColdJobTrigger,
    catalog_sha256: Sha256Digest,
    catalog_bytes: u64,
    catalog_observation: DurableCatalogObservationEnvelope,
    descriptor_sha256: Sha256Digest,
    trade_date: String,
    feed: String,
    feed_version: String,
    transport_version: String,
    object_encoding: String,
    file_name: String,
    download_url: String,
    advertised_compressed_bytes: u64,
    decode_contract: DecodeContract,
    capacity_footprint: IexHistCapacityFootprint,
    max_catalog_age_nanos: u64,
    max_download_duration_nanos: u64,
    max_clock_regression_nanos: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableCatalogObservationEnvelope {
    body_sha256: Sha256Digest,
    body_bytes: u64,
    request_sha256: Sha256Digest,
    reservation_sha256: Sha256Digest,
    authority_generation: u64,
    storage_root_sha256: Sha256Digest,
    admitted_at_unix_nanos: i64,
    admitted_utc_offset_seconds: i32,
    admitted_observed_date: String,
    deadline_unix_nanos: i64,
    attempt_sha256: Sha256Digest,
    retrieved_at_unix_nanos: i64,
    retrieved_utc_offset_seconds: i32,
    retrieved_observed_date: String,
    receipt_sha256: Sha256Digest,
}

impl DurableCatalogObservationEnvelope {
    fn from_receipt(receipt: &IexHistCatalogObservationReceipt) -> Self {
        let attempt = receipt.attempt;
        Self {
            body_sha256: receipt.body_sha256,
            body_bytes: receipt.body_bytes,
            request_sha256: attempt.request_sha256,
            reservation_sha256: attempt.reservation_sha256,
            authority_generation: attempt.authority_generation,
            storage_root_sha256: attempt.storage_root_sha256,
            admitted_at_unix_nanos: attempt.admitted_clock.unix_nanos(),
            admitted_utc_offset_seconds: attempt.admitted_clock.utc_offset_seconds(),
            admitted_observed_date: attempt.admitted_clock.observed_date().compact(),
            deadline_unix_nanos: attempt.deadline_unix_nanos,
            attempt_sha256: attempt.attempt_sha256,
            retrieved_at_unix_nanos: receipt.retrieved_clock.unix_nanos(),
            retrieved_utc_offset_seconds: receipt.retrieved_clock.utc_offset_seconds(),
            retrieved_observed_date: receipt.retrieved_clock.observed_date().compact(),
            receipt_sha256: receipt.receipt_sha256,
        }
    }

    fn into_receipt(self) -> Result<IexHistCatalogObservationReceipt, PlanError> {
        let admitted_date = TradeDate::parse(&self.admitted_observed_date)
            .map_err(PlanError::Date)?;
        let retrieved_date = TradeDate::parse(&self.retrieved_observed_date)
            .map_err(PlanError::Date)?;
        let admitted_clock = IexHistTrustedClockReading::try_new(
            self.admitted_at_unix_nanos,
            self.admitted_utc_offset_seconds,
            admitted_date,
        )
        .map_err(|_| PlanError::InvalidEnvelope)?;
        let retrieved_clock = IexHistTrustedClockReading::try_new(
            self.retrieved_at_unix_nanos,
            self.retrieved_utc_offset_seconds,
            retrieved_date,
        )
        .map_err(|_| PlanError::InvalidEnvelope)?;
        let attempt = IexHistExecutionAttempt {
            request_sha256: self.request_sha256,
            reservation_sha256: self.reservation_sha256,
            authority_generation: self.authority_generation,
            storage_root_sha256: self.storage_root_sha256,
            admitted_clock,
            deadline_unix_nanos: self.deadline_unix_nanos,
            attempt_sha256: self.attempt_sha256,
        };
        attempt.validate().map_err(|_| PlanError::InvalidEnvelope)?;
        let receipt = IexHistCatalogObservationReceipt {
            body_sha256: self.body_sha256,
            body_bytes: self.body_bytes,
            attempt,
            retrieved_clock,
            receipt_sha256: self.receipt_sha256,
        };
        receipt.validate().map_err(|_| PlanError::InvalidEnvelope)?;
        Ok(receipt)
    }
}

/// Pure exact-file planner; concrete capacity ownership remains in the shared application.
#[derive(Clone, Copy, Debug, Default)]
pub struct IexHistPlanner;

/// Application-owned date-effective DPLC channel-distribution registry.
///
/// The production implementation must return roles only from its registered provider evidence for
/// the exact requested trade date. The adapter binds the returned evidence identity and all 16
/// roles into the immutable decoder contract; callers cannot directly construct that contract.
pub trait IexHistDplcDistributionAuthority: Send + Sync {
    /// Returns exact channel roles and the immutable provider-evidence identity for `trade_date`.
    fn registered_distribution(
        &self,
        trade_date: TradeDate,
    ) -> Result<([DecodeChannelRole; 16], Sha256Digest), IexHistDplcDistributionError>;
}

/// Date-effective DPLC distribution-registry failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum IexHistDplcDistributionError {
    /// No exact registered distribution evidence exists for the requested date.
    #[error("IEX HIST DPLC channel distribution is unavailable for the selected date")]
    Unavailable,
    /// Registered evidence or its immutable identity is invalid.
    #[error("IEX HIST DPLC channel distribution evidence is invalid")]
    InvalidEvidence,
}

impl IexHistPlanner {
    /// Builds the only admitted decoder contract for one exact selected descriptor.
    ///
    /// DPLC necessarily consults the application-owned date-effective distribution registry;
    /// TOPS, DEEP, and DPLS necessarily reject an unexpected DPLC authority.
    pub fn decode_contract(
        selected_file: &SelectedFileReceipt,
        limits: DecodeLimits,
        dplc_authority: Option<&dyn IexHistDplcDistributionAuthority>,
    ) -> Result<DecodeContract, PlanError> {
        let distribution = match selected_file.feed {
            crate::model::FeedKind::DeepPlusDplc => {
                let authority = dplc_authority.ok_or(PlanError::DplcDistributionUnavailable)?;
                let (roles, provider_evidence_sha256) = authority
                    .registered_distribution(selected_file.trade_date)
                    .map_err(|error| match error {
                        IexHistDplcDistributionError::Unavailable => {
                            PlanError::DplcDistributionUnavailable
                        }
                        IexHistDplcDistributionError::InvalidEvidence => {
                            PlanError::InvalidDplcDistribution
                        }
                    })?;
                if !nonzero_identity(provider_evidence_sha256) {
                    return Err(PlanError::InvalidDplcDistribution);
                }
                Some(
                    DplcChannelDistributionContract::try_new(
                        selected_file.trade_date,
                        roles,
                        provider_evidence_sha256,
                    )
                    .map_err(|_| PlanError::InvalidDplcDistribution)?,
                )
            }
            crate::model::FeedKind::Tops
            | crate::model::FeedKind::Deep
            | crate::model::FeedKind::DeepPlusDpls => {
                if dplc_authority.is_some() {
                    return Err(PlanError::InvalidDplcDistribution);
                }
                None
            }
        };
        DecodeContract::for_selection(
            selected_file.feed_version,
            selected_file.transport_version,
            limits,
            distribution,
        )
        .map_err(|_| PlanError::InvalidDecoderContract)
    }

    /// Builds one immutable cold/on-demand selection and its only admitted decoder contract.
    pub fn plan(
        selected_file: SelectedFileReceipt,
        trigger: ColdJobTrigger,
        limits: ByteAdmissionLimits,
        decode_limits: DecodeLimits,
        dplc_authority: Option<&dyn IexHistDplcDistributionAuthority>,
    ) -> Result<ColdJobPlan, PlanError> {
        let decode_contract = Self::decode_contract(
            &selected_file,
            decode_limits,
            dplc_authority,
        )?;
        Self::plan_with_contract(selected_file, trigger, limits, decode_contract)
    }

    fn plan_with_contract(
        selected_file: SelectedFileReceipt,
        trigger: ColdJobTrigger,
        limits: ByteAdmissionLimits,
        decode_contract: DecodeContract,
    ) -> Result<ColdJobPlan, PlanError> {
        if limits.max_compressed_bytes == 0
            || limits.max_pcap_bytes == 0
            || limits.max_decoded_event_batch_bytes == 0
            || limits.max_canonical_arrow_bytes == 0
            || limits.max_parquet_bytes == 0
            || limits.manifest_and_atomic_overhead_bytes == 0
            || limits.required_free_reserve_bytes == 0
            || limits.max_catalog_age_nanos == 0
            || limits.max_download_duration_nanos == 0
        {
            return Err(PlanError::InvalidLimits);
        }
        decode_contract
            .validate_for(selected_file.trade_date)
            .map_err(|_| PlanError::InvalidDecoderContract)?;
        if decode_contract.feed() != selected_file.feed
            || decode_contract.feed_version() != selected_file.feed_version
            || decode_contract.transport_version() != selected_file.transport_version
            || decode_contract.limits().max_decoded_event_batch_bytes
                != limits.max_decoded_event_batch_bytes
        {
            return Err(PlanError::InvalidDecoderContract);
        }
        let earliest_available_on = selected_file.trade_date.next_day().map_err(PlanError::Date)?;
        if selected_file.catalog_observed_on() < earliest_available_on {
            return Err(PlanError::NotTPlusOne);
        }
        let rolling_window_start = selected_file
            .catalog_observed_on()
            .rolling_year_start()
            .map_err(PlanError::Date)?;
        if selected_file.trade_date < rolling_window_start {
            return Err(PlanError::OutsideRollingWindow);
        }
        if selected_file.advertised_compressed_bytes > limits.max_compressed_bytes {
            return Err(PlanError::CompressedBytesExceeded);
        }
        if selected_file.object_encoding == PcapObjectEncoding::Identity
            && selected_file.advertised_compressed_bytes > limits.max_pcap_bytes
        {
            return Err(PlanError::ExpandedBytesExceeded);
        }
        let capacity_footprint = IexHistCapacityFootprint::selected_file(
            selected_file.advertised_compressed_bytes,
            selected_file.object_encoding,
            limits,
        )?;
        let object_encoding = selected_file.object_encoding;
        let identity = selected_file.identity();
        let plan_sha256 = crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-cold-plan/v3",
            identity.as_bytes(),
            &[match trigger {
                ColdJobTrigger::Operator => 1,
                ColdJobTrigger::ResearchJob => 2,
            }],
            capacity_footprint.identity().as_bytes(),
            decode_contract.contract_sha256().as_bytes(),
            &limits.max_catalog_age_nanos.to_le_bytes(),
            &limits.max_download_duration_nanos.to_le_bytes(),
            &limits.max_clock_regression_nanos.to_le_bytes(),
        ]);
        Ok(ColdJobPlan {
            plan_sha256,
            selected_file,
            trigger,
            lane: ScheduleLane::Cold,
            automatic_archive_catch_up: false,
            max_parallel_transfers: 1,
            resume_policy: ResumePolicy::ReverifyCompleteEvidenceOrRestartWholeFile,
            earliest_available_on,
            rolling_window_start,
            advertised_compressed_bytes: capacity_footprint.network_response,
            max_pcap_bytes: limits.max_pcap_bytes,
            object_encoding,
            decode_contract,
            capacity_footprint,
            max_catalog_age_nanos: limits.max_catalog_age_nanos,
            max_download_duration_nanos: limits.max_download_duration_nanos,
            max_clock_regression_nanos: limits.max_clock_regression_nanos,
        })
    }

    /// Reconstructs and revalidates a complete immutable plan without an in-memory original.
    pub fn restore(payload: &[u8]) -> Result<ColdJobPlan, PlanError> {
        if payload.is_empty() || payload.len() > MAX_DURABLE_PLAN_BYTES {
            return Err(PlanError::InvalidEnvelope);
        }
        let envelope: DurableColdJobPlanEnvelope =
            serde_json::from_slice(payload).map_err(|_| PlanError::InvalidEnvelope)?;
        if envelope.schema_version != 2
            || envelope.catalog_bytes == 0
            || envelope.file_name.is_empty()
            || envelope.download_url.is_empty()
        {
            return Err(PlanError::InvalidEnvelope);
        }
        let catalog_observation = envelope.catalog_observation.into_receipt()?;
        if catalog_observation.body_sha256() != envelope.catalog_sha256
            || catalog_observation.body_bytes() != envelope.catalog_bytes
        {
            return Err(PlanError::InvalidEnvelope);
        }
        let trade_date = TradeDate::parse(&envelope.trade_date).map_err(PlanError::Date)?;
        let (feed, feed_version, object_encoding) = match (
            envelope.feed.as_str(),
            envelope.feed_version.as_str(),
            envelope.object_encoding.as_str(),
        ) {
            ("TOPS", "1.6", "gzip-pcap") => (
                crate::model::FeedKind::Tops,
                crate::model::FeedVersion::Tops1_6,
                PcapObjectEncoding::Gzip,
            ),
            ("DEEP", "1.0", "gzip-pcap") => (
                crate::model::FeedKind::Deep,
                crate::model::FeedVersion::Deep1_0,
                PcapObjectEncoding::Gzip,
            ),
            ("DPLS", "1.0", "gzip-pcap") => (
                crate::model::FeedKind::DeepPlusDpls,
                crate::model::FeedVersion::DeepPlusDpls1_0,
                PcapObjectEncoding::Gzip,
            ),
            ("DPLC", "1", "identity-pcap") => (
                crate::model::FeedKind::DeepPlusDplc,
                crate::model::FeedVersion::DeepPlusDplc1,
                PcapObjectEncoding::Identity,
            ),
            _ => return Err(PlanError::InvalidEnvelope),
        };
        if envelope.transport_version != "IEXTP1" {
            return Err(PlanError::InvalidEnvelope);
        }
        validate_restored_descriptor(
            trade_date,
            &envelope.feed,
            &envelope.feed_version,
            &envelope.file_name,
            &envelope.download_url,
            envelope.advertised_compressed_bytes,
            object_encoding,
            envelope.descriptor_sha256,
        )?;
        let selected_file = SelectedFileReceipt {
            catalog_sha256: envelope.catalog_sha256,
            catalog_bytes: envelope.catalog_bytes,
            catalog_observation,
            descriptor_sha256: envelope.descriptor_sha256,
            trade_date,
            feed,
            feed_version,
            transport_version: crate::model::TransportVersion::IexTp1,
            object_encoding,
            file_name: envelope.file_name,
            download_url: envelope.download_url,
            advertised_compressed_bytes: envelope.advertised_compressed_bytes,
        };
        let limits = ByteAdmissionLimits {
            max_compressed_bytes: envelope.capacity_footprint.network_response,
            max_pcap_bytes: envelope.capacity_footprint.temporary_pcap,
            max_decoded_event_batch_bytes: envelope.capacity_footprint.decoded_event_batch,
            max_canonical_arrow_bytes: envelope.capacity_footprint.canonical_arrow,
            max_parquet_bytes: envelope.capacity_footprint.immutable_parquet,
            manifest_and_atomic_overhead_bytes: envelope
                .capacity_footprint
                .manifest_and_atomic_overhead,
            required_free_reserve_bytes: envelope.capacity_footprint.safety_free_reserve,
            max_catalog_age_nanos: envelope.max_catalog_age_nanos,
            max_download_duration_nanos: envelope.max_download_duration_nanos,
            max_clock_regression_nanos: envelope.max_clock_regression_nanos,
        };
        let plan = Self::plan_with_contract(
            selected_file,
            envelope.trigger,
            limits,
            envelope.decode_contract,
        )?;
        if plan.plan_sha256 != envelope.plan_sha256
            || plan.capacity_footprint != envelope.capacity_footprint
        {
            return Err(PlanError::InvalidEnvelope);
        }
        Ok(plan)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "restoration revalidates every exact provider descriptor coordinate"
)]
fn validate_restored_descriptor(
    trade_date: TradeDate,
    feed: &str,
    feed_version: &str,
    file_name: &str,
    download_url: &str,
    advertised_compressed_bytes: u64,
    object_encoding: PcapObjectEncoding,
    descriptor_sha256: Sha256Digest,
) -> Result<(), PlanError> {
    let expected_file_name = match object_encoding {
        PcapObjectEncoding::Gzip => format!(
            "{}_IEXTP1_{}{}.pcap.gz",
            trade_date.compact(),
            feed,
            feed_version
        ),
        PcapObjectEncoding::Identity => {
            format!("{}_IEXTP1_DPLC1.0.pcap", trade_date.compact())
        }
    };
    if advertised_compressed_bytes == 0 || file_name != expected_file_name {
        return Err(PlanError::InvalidEnvelope);
    }
    let parsed = Url::parse(download_url).map_err(|_| PlanError::InvalidEnvelope)?;
    let expected_path = format!(
        "/download/storage/v1/b/iex/o/data%2Ffeeds%2F{}%2F{}",
        trade_date.compact(),
        file_name
    );
    let pairs = parsed.query_pairs().collect::<Vec<_>>();
    let generation = pairs
        .iter()
        .find_map(|(key, value)| (key == "generation").then_some(value.as_ref()));
    let alt = pairs
        .iter()
        .find_map(|(key, value)| (key == "alt").then_some(value.as_ref()));
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("www.googleapis.com")
        || parsed.port().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != expected_path
        || pairs.len() != 2
        || generation.is_none_or(|value| {
            value.is_empty()
                || value.len() > 20
                || value.starts_with('0')
                || !value.bytes().all(|byte| byte.is_ascii_digit())
        })
        || alt != Some("media")
    {
        return Err(PlanError::InvalidEnvelope);
    }
    let advertised = advertised_compressed_bytes.to_string();
    let expected_descriptor = crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-catalog-descriptor/v2",
        trade_date.compact().as_bytes(),
        feed.as_bytes(),
        feed_version.as_bytes(),
        b"IEXTP1",
        advertised.as_bytes(),
        download_url.as_bytes(),
        object_encoding.identity_value().as_bytes(),
    ]);
    if expected_descriptor != descriptor_sha256 {
        return Err(PlanError::InvalidEnvelope);
    }
    Ok(())
}

/// Operation protected by one application-owned provider/network/capacity lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum IexHistCapacityOperation {
    /// One bounded mutable catalog retrieval and provider-object evidence handoff.
    CatalogDiscovery,
    /// One selected-file acquisition, materialization, decode, and downstream handoff pipeline.
    SelectedFilePipeline,
}

/// Exact request presented to the shared durable capacity authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IexHistCapacityRequest {
    operation: IexHistCapacityOperation,
    plan_sha256: Option<Sha256Digest>,
    footprint: IexHistCapacityFootprint,
    deadline_unix_nanos: i64,
    request_sha256: Sha256Digest,
}

impl IexHistCapacityRequest {
    pub(crate) fn catalog(
        footprint: IexHistCapacityFootprint,
        deadline_unix_nanos: i64,
    ) -> Result<Self, IexHistCapacityError> {
        Self::new(IexHistCapacityOperation::CatalogDiscovery, None, footprint, deadline_unix_nanos)
    }

    pub(crate) fn selected_file(
        plan: &ColdJobPlan,
        deadline_unix_nanos: i64,
        authority_free_reserve_bytes: u64,
    ) -> Result<Self, IexHistCapacityError> {
        if authority_free_reserve_bytes == 0 {
            return Err(IexHistCapacityError::InvalidRequest);
        }
        let mut footprint = plan.capacity_footprint;
        footprint.safety_free_reserve = footprint
            .safety_free_reserve
            .max(authority_free_reserve_bytes);
        Self::new(
            IexHistCapacityOperation::SelectedFilePipeline,
            Some(plan.plan_sha256),
            footprint,
            deadline_unix_nanos,
        )
    }

    fn new(
        operation: IexHistCapacityOperation,
        plan_sha256: Option<Sha256Digest>,
        footprint: IexHistCapacityFootprint,
        deadline_unix_nanos: i64,
    ) -> Result<Self, IexHistCapacityError> {
        if deadline_unix_nanos < 0 || footprint.total_reserved_bytes().is_err() {
            return Err(IexHistCapacityError::InvalidRequest);
        }
        let plan_present = [u8::from(plan_sha256.is_some())];
        let plan_bytes = plan_sha256.unwrap_or_else(|| Sha256Digest::of(b"")).as_bytes().to_owned();
        let request_sha256 = crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-capacity-request/v1",
            IEX_HIST_PROVIDER_LANE.as_bytes(),
            &[match operation {
                IexHistCapacityOperation::CatalogDiscovery => 1,
                IexHistCapacityOperation::SelectedFilePipeline => 2,
            }],
            &plan_present,
            &plan_bytes,
            footprint.identity().as_bytes(),
            &deadline_unix_nanos.to_le_bytes(),
        ]);
        Ok(Self { operation, plan_sha256, footprint, deadline_unix_nanos, request_sha256 })
    }

    #[must_use]
    pub const fn operation(&self) -> IexHistCapacityOperation { self.operation }
    #[must_use]
    pub const fn plan_sha256(&self) -> Option<Sha256Digest> { self.plan_sha256 }
    #[must_use]
    pub const fn footprint(&self) -> IexHistCapacityFootprint { self.footprint }
    #[must_use]
    pub const fn deadline_unix_nanos(&self) -> i64 { self.deadline_unix_nanos }
    #[must_use]
    pub const fn request_sha256(&self) -> Sha256Digest { self.request_sha256 }
}

/// Raw wall-clock/calendar sample returned only through the application authority seam.
///
/// This is deliberately not trusted evidence. The adapter validates it and mints the opaque
/// [`IexHistTrustedClockReading`] only while an admitted lease is alive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IexHistAuthorityClockSample {
    /// Exact non-negative Unix nanoseconds sampled atomically with the calendar coordinates.
    pub unix_nanos: i64,
    /// UTC offset applied by the authority when deriving `observed_date`.
    pub utc_offset_seconds: i32,
    /// Calendar date derived by the authority from the same sample and offset.
    pub observed_date: TradeDate,
}

/// Opaque validated wall-clock/calendar reading minted under an active authority lease.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IexHistTrustedClockReading {
    unix_nanos: i64,
    utc_offset_seconds: i32,
    observed_date: TradeDate,
}

impl IexHistTrustedClockReading {
    /// Mints a receipt only when its date is exactly derived from timestamp plus offset.
    pub(crate) fn try_new(
        unix_nanos: i64,
        utc_offset_seconds: i32,
        observed_date: TradeDate,
    ) -> Result<Self, IexHistCapacityError> {
        if unix_nanos < 0 || !(-64_800..=64_800).contains(&utc_offset_seconds) {
            return Err(IexHistCapacityError::Clock);
        }
        let offset_nanos = i64::from(utc_offset_seconds)
            .checked_mul(1_000_000_000)
            .ok_or(IexHistCapacityError::Clock)?;
        let local_nanos = unix_nanos.checked_add(offset_nanos).ok_or(IexHistCapacityError::Clock)?;
        let start = observed_date.start_epoch_nanos().map_err(|_| IexHistCapacityError::Clock)?;
        let end = observed_date
            .next_day()
            .and_then(TradeDate::start_epoch_nanos)
            .map_err(|_| IexHistCapacityError::Clock)?;
        if local_nanos < start || local_nanos >= end {
            return Err(IexHistCapacityError::Clock);
        }
        Ok(Self { unix_nanos, utc_offset_seconds, observed_date })
    }

    #[must_use]
    pub const fn unix_nanos(self) -> i64 { self.unix_nanos }
    #[must_use]
    pub const fn utc_offset_seconds(self) -> i32 { self.utc_offset_seconds }
    #[must_use]
    pub const fn observed_date(self) -> TradeDate { self.observed_date }

    fn validate(self) -> Result<(), IexHistCapacityError> {
        Self::try_new(self.unix_nanos, self.utc_offset_seconds, self.observed_date).map(|_| ())
    }
}

impl TryFrom<IexHistAuthorityClockSample> for IexHistTrustedClockReading {
    type Error = IexHistCapacityError;

    fn try_from(sample: IexHistAuthorityClockSample) -> Result<Self, Self::Error> {
        Self::try_new(
            sample.unix_nanos,
            sample.utc_offset_seconds,
            sample.observed_date,
        )
    }
}

/// Authority-bound observation of one exact catalog body.
///
/// Callers cannot construct this receipt. It is minted by an active catalog permit after the
/// complete bounded response body exists, binding body content and observation clock to the exact
/// durable reservation, authority generation, storage root, and execution attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IexHistCatalogObservationReceipt {
    body_sha256: Sha256Digest,
    body_bytes: u64,
    attempt: IexHistExecutionAttempt,
    retrieved_clock: IexHistTrustedClockReading,
    receipt_sha256: Sha256Digest,
}

impl IexHistCatalogObservationReceipt {
    #[must_use]
    pub const fn body_sha256(&self) -> Sha256Digest { self.body_sha256 }
    #[must_use]
    pub const fn body_bytes(&self) -> u64 { self.body_bytes }
    #[must_use]
    pub const fn attempt(&self) -> IexHistExecutionAttempt { self.attempt }
    #[must_use]
    pub const fn retrieved_clock(&self) -> IexHistTrustedClockReading { self.retrieved_clock }
    #[must_use]
    pub const fn receipt_sha256(&self) -> Sha256Digest { self.receipt_sha256 }

    pub(crate) fn validate(&self) -> Result<(), IexHistCapacityError> {
        self.retrieved_clock.validate()?;
        if self.body_bytes == 0
            || !nonzero_identity(self.body_sha256)
            || self.retrieved_clock.unix_nanos() < self.attempt.admitted_clock.unix_nanos()
            || self.retrieved_clock.unix_nanos() >= self.attempt.deadline_unix_nanos
            || catalog_observation_identity(
                self.body_sha256,
                self.body_bytes,
                self.attempt,
                self.retrieved_clock,
            ) != self.receipt_sha256
        {
            return Err(IexHistCapacityError::InvalidCatalogObservation);
        }
        Ok(())
    }
}

fn catalog_observation_identity(
    body_sha256: Sha256Digest,
    body_bytes: u64,
    attempt: IexHistExecutionAttempt,
    retrieved_clock: IexHistTrustedClockReading,
) -> Sha256Digest {
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-catalog-observation/v1",
        body_sha256.as_bytes(),
        &body_bytes.to_le_bytes(),
        attempt.request_sha256.as_bytes(),
        attempt.reservation_sha256.as_bytes(),
        &attempt.authority_generation.to_le_bytes(),
        attempt.storage_root_sha256.as_bytes(),
        attempt.attempt_sha256.as_bytes(),
        &attempt.admitted_clock.unix_nanos().to_le_bytes(),
        &attempt.admitted_clock.utc_offset_seconds().to_le_bytes(),
        attempt.admitted_clock.observed_date().compact().as_bytes(),
        &attempt.deadline_unix_nanos.to_le_bytes(),
        &retrieved_clock.unix_nanos().to_le_bytes(),
        &retrieved_clock.utc_offset_seconds().to_le_bytes(),
        retrieved_clock.observed_date().compact().as_bytes(),
    ])
}

/// Actual usage accumulated under one reservation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct IexHistCapacityUsage {
    bytes: [u64; 11],
}

impl IexHistCapacityUsage {
    #[must_use]
    pub const fn bytes(self, category: IexHistCapacityCategory) -> u64 {
        self.bytes[category as usize]
    }

    fn record(
        &mut self,
        category: IexHistCapacityCategory,
        bytes: u64,
        reserved: IexHistCapacityFootprint,
    ) -> Result<(), IexHistCapacityError> {
        if bytes > reserved.bytes(category) {
            return Err(IexHistCapacityError::UsageExceeded);
        }
        self.bytes[category as usize] = bytes;
        Ok(())
    }
}

/// Terminal disposition durably settled by the application capacity authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum IexHistCapacityDisposition {
    /// Every phase covered by the reservation completed and durable actuals were recorded.
    Completed,
    /// A complete durable phase was checkpointed; the reservation remains adoptable on restart.
    Checkpointed,
    /// The attempt failed before complete publication.
    Failed,
    /// Complete evidence exists but an exact quality/clock/version invariant forbids publication.
    Quarantined(IexHistTerminalReason),
    /// Required evidence or authority is deterministically unavailable for this generation.
    Unavailable(IexHistTerminalReason),
    /// The permit was dropped or its future was cancelled before an explicit settlement.
    Interrupted,
}

/// Provider-local terminal reason for root-owned durable CAS state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum IexHistTerminalReason {
    /// Trusted capture or message chronology exceeded its tolerance.
    ClockAnomaly,
    /// Raw transport/gzip/PCAP evidence is corrupt or incomplete.
    CorruptRawEvidence,
    /// The exact transport/feed/schema version is unsupported.
    UnsupportedVersion,
    /// Packet/message continuity is not complete.
    ContinuityFault,
    /// Shared provider, storage, or clock authority is unavailable.
    AuthorityUnavailable,
    /// The immutable decoder contract or its exact limits cannot be executed by this binary.
    InvalidDecoderContract,
    /// Provider content exceeded an immutable, pre-admitted decoder resource ceiling.
    ResourceLimitExceeded,
    /// Transactional staging, serialization, or commit evidence violated its integrity contract.
    DownstreamIntegrityFault,
}

/// Exact settlement presented to the shared authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IexHistCapacitySettlement {
    request_sha256: Sha256Digest,
    reservation_sha256: Sha256Digest,
    attempt_sha256: Sha256Digest,
    disposition: IexHistCapacityDisposition,
    usage: IexHistCapacityUsage,
}

impl IexHistCapacitySettlement {
    #[must_use]
    pub const fn request_sha256(&self) -> Sha256Digest { self.request_sha256 }
    #[must_use]
    pub const fn reservation_sha256(&self) -> Sha256Digest { self.reservation_sha256 }
    #[must_use]
    pub const fn attempt_sha256(&self) -> Sha256Digest { self.attempt_sha256 }
    #[must_use]
    pub const fn disposition(&self) -> IexHistCapacityDisposition { self.disposition }
    #[must_use]
    pub const fn usage(&self) -> IexHistCapacityUsage { self.usage }
}

/// Application-owned durable provider/network/disk authority.
///
/// The production implementation belongs in the existing shared SQLite provider authority. It
/// must serialize this lane at one lease, persist the complete category reservation before return,
/// retain it across restart, and make `settle` atomic with release of the provider/network slot.
pub trait IexHistCapacityAuthority: Send + Sync {
    /// Returns the durable-volume free-space reserve protected from all provider jobs.
    fn required_free_reserve_bytes(&self) -> Result<u64, IexHistCapacityError>;
    /// Acquires or reopens one exact durable reservation.
    fn acquire(
        &self,
        request: &IexHistCapacityRequest,
    ) -> Result<Box<dyn IexHistCapacityLease>, IexHistCapacityError>;
}

/// Opaque lease minted only by the application-owned capacity authority.
///
/// Implementations must conservatively persist interruption and release the slot from `Drop` when
/// adapter validation rejects a newly returned lease before it can be wrapped by the permit guard.
pub trait IexHistCapacityLease: Send {
    fn request_sha256(&self) -> Sha256Digest;
    fn reservation_sha256(&self) -> Sha256Digest;
    fn authority_generation(&self) -> u64;
    /// Identity of the application-owned durable volume that owns reservation and staging.
    fn storage_root_sha256(&self) -> Sha256Digest;
    fn max_parallel_transfers(&self) -> u8;
    fn reserved_footprint(&self) -> IexHistCapacityFootprint;
    /// Returns one atomic raw sample; the adapter validates and binds it to this lease.
    fn admitted_clock_sample(&self) -> IexHistAuthorityClockSample;
    fn deadline_unix_nanos(&self) -> i64;
    fn staging_directory(&self) -> Option<&Path>;
    /// Returns one atomic raw sample from the same authority clock used at admission.
    fn trusted_clock_sample(&self) -> Result<IexHistAuthorityClockSample, IexHistCapacityError>;
    fn settle(
        self: Box<Self>,
        settlement: &IexHistCapacitySettlement,
    ) -> Result<(), IexHistCapacityError>;
}

/// One expiring execution attempt over an immutable plan or catalog request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IexHistExecutionAttempt {
    request_sha256: Sha256Digest,
    reservation_sha256: Sha256Digest,
    authority_generation: u64,
    storage_root_sha256: Sha256Digest,
    admitted_clock: IexHistTrustedClockReading,
    deadline_unix_nanos: i64,
    attempt_sha256: Sha256Digest,
}

impl IexHistExecutionAttempt {
    #[must_use]
    pub const fn request_sha256(self) -> Sha256Digest { self.request_sha256 }
    #[must_use]
    pub const fn reservation_sha256(self) -> Sha256Digest { self.reservation_sha256 }
    #[must_use]
    pub const fn authority_generation(self) -> u64 { self.authority_generation }
    #[must_use]
    pub const fn storage_root_sha256(self) -> Sha256Digest { self.storage_root_sha256 }
    #[must_use]
    pub const fn attempt_sha256(self) -> Sha256Digest { self.attempt_sha256 }
    #[must_use]
    pub const fn admitted_clock(self) -> IexHistTrustedClockReading { self.admitted_clock }
    #[must_use]
    pub const fn deadline_unix_nanos(self) -> i64 { self.deadline_unix_nanos }

    fn validate(self) -> Result<(), IexHistCapacityError> {
        self.admitted_clock.validate()?;
        if !nonzero_identity(self.request_sha256)
            || !nonzero_identity(self.reservation_sha256)
            || self.authority_generation == 0
            || !nonzero_identity(self.storage_root_sha256)
            || self.admitted_clock.unix_nanos() >= self.deadline_unix_nanos
            || execution_attempt_identity(
                self.request_sha256,
                self.reservation_sha256,
                self.authority_generation,
                self.storage_root_sha256,
                self.admitted_clock,
                self.deadline_unix_nanos,
            ) != self.attempt_sha256
        {
            return Err(IexHistCapacityError::InvalidLease);
        }
        Ok(())
    }
}

/// Opaque terminal decode authority evidence minted from an active selected-file permit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IexHistDecodeAttemptEvidence {
    plan_sha256: Sha256Digest,
    decode_contract_sha256: Sha256Digest,
    attempt: IexHistExecutionAttempt,
    evidence_sha256: Sha256Digest,
}

impl IexHistDecodeAttemptEvidence {
    #[must_use]
    pub const fn plan_sha256(self) -> Sha256Digest { self.plan_sha256 }
    #[must_use]
    pub const fn decode_contract_sha256(self) -> Sha256Digest { self.decode_contract_sha256 }
    #[must_use]
    pub const fn request_sha256(self) -> Sha256Digest { self.attempt.request_sha256 }
    #[must_use]
    pub const fn reservation_sha256(self) -> Sha256Digest { self.attempt.reservation_sha256 }
    #[must_use]
    pub const fn authority_generation(self) -> u64 { self.attempt.authority_generation }
    #[must_use]
    pub const fn storage_root_sha256(self) -> Sha256Digest { self.attempt.storage_root_sha256 }
    #[must_use]
    pub const fn admitted_clock(self) -> IexHistTrustedClockReading {
        self.attempt.admitted_clock
    }
    #[must_use]
    pub const fn deadline_unix_nanos(self) -> i64 { self.attempt.deadline_unix_nanos }
    #[must_use]
    pub const fn attempt_sha256(self) -> Sha256Digest { self.attempt.attempt_sha256 }
    #[must_use]
    pub const fn evidence_sha256(self) -> Sha256Digest { self.evidence_sha256 }

    /// Revalidates every authority and immutable-plan coordinate before terminal evidence use.
    pub fn validate_against(self, plan: &ColdJobPlan) -> Result<(), IexHistCapacityError> {
        self.attempt.validate()?;
        if self.plan_sha256 != plan.plan_sha256
            || self.decode_contract_sha256 != plan.decode_contract().contract_sha256()
            || decode_attempt_evidence_identity(
                self.plan_sha256,
                self.decode_contract_sha256,
                self.attempt,
            ) != self.evidence_sha256
        {
            return Err(IexHistCapacityError::InvalidDecodeEvidence);
        }
        Ok(())
    }
}

fn decode_attempt_evidence_identity(
    plan_sha256: Sha256Digest,
    decode_contract_sha256: Sha256Digest,
    attempt: IexHistExecutionAttempt,
) -> Sha256Digest {
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-decode-attempt-evidence/v1",
        plan_sha256.as_bytes(),
        decode_contract_sha256.as_bytes(),
        attempt.attempt_sha256.as_bytes(),
        attempt.request_sha256.as_bytes(),
        attempt.reservation_sha256.as_bytes(),
        &attempt.authority_generation.to_le_bytes(),
        attempt.storage_root_sha256.as_bytes(),
        &attempt.admitted_clock.unix_nanos().to_le_bytes(),
        &attempt.admitted_clock.utc_offset_seconds().to_le_bytes(),
        attempt.admitted_clock.observed_date().compact().as_bytes(),
        &attempt.deadline_unix_nanos.to_le_bytes(),
    ])
}

fn execution_attempt_identity(
    request_sha256: Sha256Digest,
    reservation_sha256: Sha256Digest,
    authority_generation: u64,
    storage_root_sha256: Sha256Digest,
    admitted_clock: IexHistTrustedClockReading,
    deadline_unix_nanos: i64,
) -> Sha256Digest {
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-execution-attempt/v1",
        request_sha256.as_bytes(),
        reservation_sha256.as_bytes(),
        &authority_generation.to_le_bytes(),
        storage_root_sha256.as_bytes(),
        &admitted_clock.unix_nanos().to_le_bytes(),
        &admitted_clock.utc_offset_seconds().to_le_bytes(),
        admitted_clock.observed_date().compact().as_bytes(),
        &deadline_unix_nanos.to_le_bytes(),
    ])
}

/// Mutable, single-owner permit that keeps capacity and max-parallel-one authority alive.
pub struct IexHistExecutionPermit {
    request: IexHistCapacityRequest,
    attempt: IexHistExecutionAttempt,
    usage: IexHistCapacityUsage,
    lease: Option<Box<dyn IexHistCapacityLease>>,
}

impl fmt::Debug for IexHistExecutionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IexHistExecutionPermit")
            .field("operation", &self.request.operation)
            .field("attempt_sha256", &self.attempt.attempt_sha256)
            .field("settled", &self.lease.is_none())
            .finish_non_exhaustive()
    }
}

impl IexHistExecutionPermit {
    pub(crate) fn acquire(
        authority: &dyn IexHistCapacityAuthority,
        request: IexHistCapacityRequest,
        plan: Option<&ColdJobPlan>,
    ) -> Result<Self, IexHistCapacityError> {
        let lease = authority.acquire(&request)?;
        let admitted_clock = IexHistTrustedClockReading::try_from(lease.admitted_clock_sample())?;
        if lease.request_sha256() != request.request_sha256
            || !nonzero_identity(lease.reservation_sha256())
            || lease.authority_generation() == 0
            || !nonzero_identity(lease.storage_root_sha256())
            || lease.max_parallel_transfers() != 1
            || lease.deadline_unix_nanos() != request.deadline_unix_nanos
            || !lease.reserved_footprint().covers(request.footprint)
            || admitted_clock.unix_nanos() >= request.deadline_unix_nanos
            || (request.operation == IexHistCapacityOperation::SelectedFilePipeline
                && lease.staging_directory().is_none())
        {
            return Err(IexHistCapacityError::InvalidLease);
        }
        if let Some(plan) = plan {
            let age = admitted_clock
                .unix_nanos()
                .checked_sub(plan.selected_file.catalog_retrieved_at_unix_nanos())
                .ok_or(IexHistCapacityError::Clock)?;
            if age < 0 || u64::try_from(age).map_or(true, |value| value > plan.max_catalog_age_nanos) {
                return Err(IexHistCapacityError::CatalogStale);
            }
        }
        let reservation_sha256 = lease.reservation_sha256();
        let authority_generation = lease.authority_generation();
        let storage_root_sha256 = lease.storage_root_sha256();
        let attempt_sha256 = execution_attempt_identity(
            request.request_sha256,
            reservation_sha256,
            authority_generation,
            storage_root_sha256,
            admitted_clock,
            request.deadline_unix_nanos,
        );
        Ok(Self {
            request,
            attempt: IexHistExecutionAttempt {
                request_sha256: lease.request_sha256(),
                reservation_sha256,
                authority_generation,
                storage_root_sha256,
                admitted_clock,
                deadline_unix_nanos: lease.deadline_unix_nanos(),
                attempt_sha256,
            },
            usage: IexHistCapacityUsage::default(),
            lease: Some(lease),
        })
    }

    #[must_use]
    pub const fn attempt(&self) -> IexHistExecutionAttempt { self.attempt }

    /// Mints decode evidence only from the active selected-file lease for this exact plan.
    pub(crate) fn decode_attempt_evidence(
        &self,
        plan: &ColdJobPlan,
    ) -> Result<IexHistDecodeAttemptEvidence, IexHistCapacityError> {
        if self.request.operation != IexHistCapacityOperation::SelectedFilePipeline
            || self.request.plan_sha256 != Some(plan.plan_sha256)
            || self.lease.is_none()
        {
            return Err(IexHistCapacityError::InvalidDecodeEvidence);
        }
        let decode_contract_sha256 = plan.decode_contract().contract_sha256();
        let evidence_sha256 = decode_attempt_evidence_identity(
            plan.plan_sha256,
            decode_contract_sha256,
            self.attempt,
        );
        let evidence = IexHistDecodeAttemptEvidence {
            plan_sha256: plan.plan_sha256,
            decode_contract_sha256,
            attempt: self.attempt,
            evidence_sha256,
        };
        evidence.validate_against(plan)?;
        Ok(evidence)
    }

    pub(crate) fn staging_directory(&self) -> Result<&Path, IexHistCapacityError> {
        self.lease
            .as_deref()
            .and_then(IexHistCapacityLease::staging_directory)
            .ok_or(IexHistCapacityError::InvalidLease)
    }

    pub(crate) fn trusted_clock(&self) -> Result<IexHistTrustedClockReading, IexHistCapacityError> {
        let sample = self
            .lease
            .as_deref()
            .ok_or(IexHistCapacityError::AlreadySettled)?
            .trusted_clock_sample()?;
        IexHistTrustedClockReading::try_from(sample)
    }

    /// Mints authority-bound evidence for one exact completed catalog body.
    pub(crate) fn observe_catalog_body(
        &self,
        body: &[u8],
    ) -> Result<IexHistCatalogObservationReceipt, IexHistCapacityError> {
        if self.request.operation != IexHistCapacityOperation::CatalogDiscovery || body.is_empty() {
            return Err(IexHistCapacityError::InvalidCatalogObservation);
        }
        let body_bytes = u64::try_from(body.len())
            .map_err(|_| IexHistCapacityError::InvalidCatalogObservation)?;
        if body_bytes > self.request.footprint.network_response {
            return Err(IexHistCapacityError::UsageExceeded);
        }
        let retrieved_clock = self.trusted_clock()?;
        let body_sha256 = Sha256Digest::of(body);
        let receipt_sha256 = catalog_observation_identity(
            body_sha256,
            body_bytes,
            self.attempt,
            retrieved_clock,
        );
        let receipt = IexHistCatalogObservationReceipt {
            body_sha256,
            body_bytes,
            attempt: self.attempt,
            retrieved_clock,
            receipt_sha256,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    /// Records exact actual bytes for one pre-reserved category.
    pub fn record_usage(
        &mut self,
        category: IexHistCapacityCategory,
        bytes: u64,
    ) -> Result<(), IexHistCapacityError> {
        self.usage.record(category, bytes, self.request.footprint)
    }

    /// Atomically settles actual category bytes and releases provider/network ownership.
    pub fn settle(mut self, disposition: IexHistCapacityDisposition) -> Result<(), IexHistCapacityError> {
        self.validate_settlement(disposition)?;
        self.settle_inner(disposition)
    }

    fn validate_settlement(
        &self,
        disposition: IexHistCapacityDisposition,
    ) -> Result<(), IexHistCapacityError> {
        if matches!(
            disposition,
            IexHistCapacityDisposition::Failed
                | IexHistCapacityDisposition::Interrupted
                | IexHistCapacityDisposition::Quarantined(_)
                | IexHistCapacityDisposition::Unavailable(_)
        ) {
            return Ok(());
        }
        let network = self.usage.bytes(IexHistCapacityCategory::NetworkResponse);
        let durable_catalog = self.usage.bytes(IexHistCapacityCategory::DurableCatalog);
        if self.request.operation == IexHistCapacityOperation::CatalogDiscovery {
            if disposition != IexHistCapacityDisposition::Completed
                || network == 0
                || durable_catalog != network
            {
                return Err(IexHistCapacityError::IncompleteSettlement);
            }
            return Ok(());
        }
        let durable_compressed = self.usage.bytes(IexHistCapacityCategory::DurableCompressed);
        let durable_pcap = self.usage.bytes(IexHistCapacityCategory::DurablePcap);
        if durable_compressed != self.request.footprint.durable_compressed || durable_pcap == 0 {
            return Err(IexHistCapacityError::IncompleteSettlement);
        }
        if disposition == IexHistCapacityDisposition::Completed
            && (self.usage.bytes(IexHistCapacityCategory::DecodedEventBatch) == 0
                || self.usage.bytes(IexHistCapacityCategory::CanonicalArrow) == 0
                || self.usage.bytes(IexHistCapacityCategory::ImmutableParquet) == 0
                || self
                    .usage
                    .bytes(IexHistCapacityCategory::ManifestAndAtomicOverhead)
                    == 0)
        {
            return Err(IexHistCapacityError::IncompleteSettlement);
        }
        Ok(())
    }

    fn settle_inner(&mut self, disposition: IexHistCapacityDisposition) -> Result<(), IexHistCapacityError> {
        let lease = self.lease.take().ok_or(IexHistCapacityError::AlreadySettled)?;
        let settlement = IexHistCapacitySettlement {
            request_sha256: self.attempt.request_sha256,
            reservation_sha256: self.attempt.reservation_sha256,
            attempt_sha256: self.attempt.attempt_sha256,
            disposition,
            usage: self.usage,
        };
        lease.settle(&settlement)
    }
}

fn nonzero_identity(identity: Sha256Digest) -> bool {
    identity.as_bytes().iter().any(|byte| *byte != 0)
}

impl Drop for IexHistExecutionPermit {
    fn drop(&mut self) {
        if self.lease.is_some() {
            let _ = self.settle_inner(IexHistCapacityDisposition::Interrupted);
        }
    }
}

/// Exact-file planning failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PlanError {
    #[error("IEX HIST plan date is invalid: {0}")]
    Date(DateError),
    #[error("IEX HIST planning limits are invalid")]
    InvalidLimits,
    #[error("IEX HIST immutable decoder contract is invalid or mismatched")]
    InvalidDecoderContract,
    #[error("IEX HIST exact date-effective DPLC distribution evidence is unavailable")]
    DplcDistributionUnavailable,
    #[error("IEX HIST exact date-effective DPLC distribution evidence is invalid")]
    InvalidDplcDistribution,
    #[error("IEX HIST selected file is not yet T+1 eligible")]
    NotTPlusOne,
    #[error("IEX HIST selected date is outside the rolling 12-month window")]
    OutsideRollingWindow,
    #[error("IEX HIST compressed-byte ceiling is exceeded")]
    CompressedBytesExceeded,
    #[error("IEX HIST identity PCAP exceeds its expanded-byte ceiling")]
    ExpandedBytesExceeded,
    #[error("IEX HIST capacity arithmetic overflowed")]
    CapacityArithmetic,
    #[error("IEX HIST durable plan envelope is invalid")]
    InvalidEnvelope,
}

/// Shared application capacity/clock authority failure reduced to provider-local meaning.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum IexHistCapacityError {
    #[error("IEX HIST capacity request is invalid")]
    InvalidRequest,
    #[error("IEX HIST application capacity authority is unavailable")]
    Unavailable,
    #[error("IEX HIST provider lane is already owned")]
    Busy,
    #[error("IEX HIST durable capacity is insufficient")]
    InsufficientCapacity,
    #[error("IEX HIST capacity lease is invalid")]
    InvalidLease,
    #[error("IEX HIST trusted clock is unavailable or inconsistent")]
    Clock,
    #[error("IEX HIST selected catalog receipt is stale")]
    CatalogStale,
    #[error("IEX HIST authority-bound catalog observation receipt is invalid")]
    InvalidCatalogObservation,
    #[error("IEX HIST authority-bound decode attempt evidence is invalid")]
    InvalidDecodeEvidence,
    #[error("IEX HIST actual usage exceeded its reservation")]
    UsageExceeded,
    #[error("IEX HIST durable settlement omitted a required phase actual")]
    IncompleteSettlement,
    #[error("IEX HIST capacity permit was already settled")]
    AlreadySettled,
    #[error("IEX HIST durable capacity settlement failed")]
    Settlement,
}
