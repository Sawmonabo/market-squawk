use serde::Serialize;
use thiserror::Error;

use crate::catalog::SelectedFileReceipt;
use crate::model::{DateError, Sha256Digest, TradeDate};

/// Explicit scheduler lane for IEX HIST work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ScheduleLane {
    /// Background historical work that yields to interactive/current-data work.
    Cold,
}

/// Authority that explicitly requested one exact feed/date artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ColdJobTrigger {
    /// Direct operator request.
    Operator,
    /// Bounded research job with a preselected artifact.
    ResearchJob,
}

/// Resume behavior allowed by the core plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ResumePolicy {
    /// Restart unless a caller has retained affirmative server range/resume evidence.
    RequireMeasuredServerSupport,
}

/// Independent byte, disk, and deadline limits applied before transfer begins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteAdmissionLimits {
    /// Maximum admitted provider-advertised compressed bytes.
    pub max_compressed_bytes: u64,
    /// Maximum PCAP bytes allowed after streaming gzip expansion.
    pub max_pcap_bytes: u64,
    /// Currently available bytes on the controlled data volume.
    pub available_disk_bytes: u64,
    /// Bytes that must remain free after worst-case materialization.
    pub required_free_reserve_bytes: u64,
    /// Current trusted local time.
    pub now_unix_nanos: i64,
    /// Terminal job deadline.
    pub deadline_unix_nanos: i64,
}

/// Exact admission and scheduling contract for one selected IEX HIST file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ColdJobPlan {
    /// Stable plan identity.
    pub(crate) plan_sha256: Sha256Digest,
    /// Parent exact file selection.
    pub(crate) selected_file: SelectedFileReceipt,
    /// Explicit caller authority.
    pub(crate) trigger: ColdJobTrigger,
    /// Always the cold lane.
    pub(crate) lane: ScheduleLane,
    /// Always false: enablement never creates automatic archive acquisition.
    pub(crate) automatic_archive_catch_up: bool,
    /// One shared transfer until retained measurements authorize a future change.
    pub(crate) max_parallel_transfers: u8,
    /// Server resume cannot be assumed from catalog metadata.
    pub(crate) resume_policy: ResumePolicy,
    /// Earliest calendar date satisfying the source's T+1 posture.
    pub(crate) earliest_available_on: TradeDate,
    /// Application interpretation of the advertised rolling 12-month window.
    pub(crate) rolling_window_start: TradeDate,
    /// Provider-advertised compressed bytes.
    pub(crate) advertised_compressed_bytes: u64,
    /// Hard expanded PCAP ceiling.
    pub(crate) max_pcap_bytes: u64,
    /// Worst-case disk consumption including the required free reserve.
    pub(crate) required_disk_bytes: u64,
    /// Terminal deadline.
    pub(crate) deadline_unix_nanos: i64,
}

impl ColdJobPlan {
    /// Returns the stable plan identity.
    #[must_use]
    pub const fn plan_sha256(&self) -> Sha256Digest {
        self.plan_sha256
    }

    /// Returns the exact selected file receipt.
    #[must_use]
    pub const fn selected_file(&self) -> &SelectedFileReceipt {
        &self.selected_file
    }

    /// Returns the explicit trigger.
    #[must_use]
    pub const fn trigger(&self) -> ColdJobTrigger {
        self.trigger
    }

    /// Returns the cold scheduler lane.
    #[must_use]
    pub const fn lane(&self) -> ScheduleLane {
        self.lane
    }

    /// Returns false: no automatic archive catch-up is admitted.
    #[must_use]
    pub const fn automatic_archive_catch_up(&self) -> bool {
        self.automatic_archive_catch_up
    }

    /// Returns the application transfer-concurrency ceiling.
    #[must_use]
    pub const fn max_parallel_transfers(&self) -> u8 {
        self.max_parallel_transfers
    }

    /// Returns the earliest T+1 calendar date.
    #[must_use]
    pub const fn earliest_available_on(&self) -> TradeDate {
        self.earliest_available_on
    }

    /// Returns the application rolling-window start.
    #[must_use]
    pub const fn rolling_window_start(&self) -> TradeDate {
        self.rolling_window_start
    }

    /// Returns the complete worst-case disk requirement.
    #[must_use]
    pub const fn required_disk_bytes(&self) -> u64 {
        self.required_disk_bytes
    }
}

/// Pure exact-file planner; network and scheduling remain owned by the shared runtime.
#[derive(Clone, Copy, Debug, Default)]
pub struct IexHistPlanner;

impl IexHistPlanner {
    /// Builds a cold/on-demand plan after T+1, rolling-window, byte, disk, and deadline admission.
    ///
    /// # Errors
    ///
    /// Rejects automatic or implicit breadth by accepting only an already selected exact file.
    /// The request fails before network activity when any capacity or time bound is unavailable.
    pub fn plan(
        selected_file: SelectedFileReceipt,
        trigger: ColdJobTrigger,
        limits: ByteAdmissionLimits,
    ) -> Result<ColdJobPlan, PlanError> {
        if limits.now_unix_nanos < 0
            || limits.deadline_unix_nanos <= limits.now_unix_nanos
            || limits.max_compressed_bytes == 0
            || limits.max_pcap_bytes == 0
        {
            return Err(PlanError::InvalidLimits);
        }
        let earliest_available_on = selected_file
            .trade_date
            .next_day()
            .map_err(PlanError::Date)?;
        if selected_file.catalog_observed_on < earliest_available_on {
            return Err(PlanError::NotTPlusOne);
        }
        let rolling_window_start = selected_file
            .catalog_observed_on
            .rolling_year_start()
            .map_err(PlanError::Date)?;
        if selected_file.trade_date < rolling_window_start {
            return Err(PlanError::OutsideRollingWindow);
        }
        if selected_file.advertised_compressed_bytes > limits.max_compressed_bytes {
            return Err(PlanError::CompressedBytesExceeded);
        }
        let required_disk_bytes = selected_file
            .advertised_compressed_bytes
            .checked_add(limits.max_pcap_bytes)
            .and_then(|bytes| bytes.checked_add(limits.required_free_reserve_bytes))
            .ok_or(PlanError::DiskArithmetic)?;
        if required_disk_bytes > limits.available_disk_bytes {
            return Err(PlanError::InsufficientDisk);
        }
        let identity = selected_file.identity();
        let advertised_compressed_bytes = selected_file.advertised_compressed_bytes;
        let plan_sha256 = crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-cold-plan/v1",
            identity.as_bytes(),
            &[match trigger {
                ColdJobTrigger::Operator => 1,
                ColdJobTrigger::ResearchJob => 2,
            }],
            &limits.max_pcap_bytes.to_le_bytes(),
            &required_disk_bytes.to_le_bytes(),
            &limits.deadline_unix_nanos.to_le_bytes(),
        ]);
        Ok(ColdJobPlan {
            plan_sha256,
            selected_file,
            trigger,
            lane: ScheduleLane::Cold,
            automatic_archive_catch_up: false,
            max_parallel_transfers: 1,
            resume_policy: ResumePolicy::RequireMeasuredServerSupport,
            earliest_available_on,
            rolling_window_start,
            advertised_compressed_bytes,
            max_pcap_bytes: limits.max_pcap_bytes,
            required_disk_bytes,
            deadline_unix_nanos: limits.deadline_unix_nanos,
        })
    }
}

/// Exact-file planning failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PlanError {
    /// A date calculation failed.
    #[error("IEX HIST plan date is invalid: {0}")]
    Date(DateError),
    /// Byte/deadline inputs were absent or internally inconsistent.
    #[error("IEX HIST planning limits are invalid")]
    InvalidLimits,
    /// The catalog observation predates T+1 availability.
    #[error("IEX HIST selected file is not yet T+1 eligible")]
    NotTPlusOne,
    /// The date falls outside the application's advertised rolling-12-month admission policy.
    #[error("IEX HIST selected date is outside the rolling 12-month window")]
    OutsideRollingWindow,
    /// Advertised compressed bytes exceed the operator/job ceiling.
    #[error("IEX HIST compressed-byte ceiling is exceeded")]
    CompressedBytesExceeded,
    /// Worst-case disk arithmetic overflowed.
    #[error("IEX HIST disk-admission arithmetic overflowed")]
    DiskArithmetic,
    /// Available disk cannot hold compressed bytes, bounded PCAP output, and reserve.
    #[error("IEX HIST disk reserve is insufficient")]
    InsufficientDisk,
}
