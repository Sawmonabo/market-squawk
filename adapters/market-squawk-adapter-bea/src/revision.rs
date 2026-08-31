//! Immutable release, revision, and correction-ledger inputs.

use market_squawk_domain::Timestamp;
use sha2::{Digest, Sha256};

use crate::{BeaDataPage, BeaError, BeaMetadataGeneration, BeaTimePeriod};

const MAX_NOTICE_ID_BYTES: usize = 256;

pub(crate) const BEA_REVISION_STATE: &str = "provider_version_observed_shared_assignment_pending";
pub(crate) const BEA_CORRECTION_STATE: &str = "no_separate_correction_notice_in_this_acquisition";
pub(crate) const BEA_SUPERSESSION_STATE: &str = "shared_observed_revision_authority";

/// One exact provider row version prepared for canonical revision admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaObservedVersion {
    series_digest: [u8; 32],
    observation_digest: [u8; 32],
    upstream_response_digest: [u8; 32],
    response_digest: [u8; 32],
    metadata_generation: BeaMetadataGeneration,
    period: BeaTimePeriod,
    production_time: Option<Timestamp>,
    observed_at: Timestamp,
    version_digest: [u8; 32],
}

impl BeaObservedVersion {
    /// Builds immutable version evidence for one parsed row.
    ///
    /// # Errors
    ///
    /// Rejects a missing row index or a provider production instant later than local receipt.
    pub fn try_from_page(
        page: &BeaDataPage,
        observation_index: usize,
        observed_at: Timestamp,
    ) -> Result<Self, BeaError> {
        let observation = page
            .observations()
            .get(observation_index)
            .ok_or(BeaError::InvalidRevision)?;
        let production_time = page
            .production_time()
            .map(|production| production.timestamp());
        if production_time.is_some_and(|production| production > observed_at) {
            return Err(BeaError::InvalidRevision);
        }
        let series_digest = observation.identity().digest();
        let observation_digest = observation.digest();
        let upstream_response_digest = page.receipt().upstream_response_digest();
        let response_digest = page.receipt().response_digest();
        if upstream_response_digest == response_digest {
            return Err(BeaError::InvalidRevision);
        }
        let metadata_generation = page.metadata_generation();
        let period = observation.period().clone();
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk-bea-observed-version-v2");
        hasher.update(series_digest);
        hasher.update(observation_digest);
        hasher.update(upstream_response_digest);
        hasher.update(response_digest);
        hasher.update(metadata_generation.digest());
        hash_text(&mut hasher, period.raw())?;
        match production_time {
            Some(production) => {
                hasher.update([1]);
                hasher.update(production.unix_nanos().to_be_bytes());
            }
            None => hasher.update([0]),
        }
        hasher.update(observed_at.unix_nanos().to_be_bytes());
        Ok(Self {
            series_digest,
            observation_digest,
            upstream_response_digest,
            response_digest,
            metadata_generation,
            period,
            production_time,
            observed_at,
            version_digest: hasher.finalize().into(),
        })
    }

    /// Returns the provider-native dataset/table/line/dimension family digest.
    pub const fn series_digest(&self) -> [u8; 32] {
        self.series_digest
    }

    /// Returns the complete provider-native row digest.
    pub const fn observation_digest(&self) -> [u8; 32] {
        self.observation_digest
    }

    /// Returns SHA-256 of the exact upstream body before validated echo redaction.
    pub const fn upstream_response_digest(&self) -> [u8; 32] {
        self.upstream_response_digest
    }

    /// Returns the retained secret-free response-object digest.
    pub const fn response_digest(&self) -> [u8; 32] {
        self.response_digest
    }

    /// Returns the exact discovery generation used by the request.
    pub const fn metadata_generation(&self) -> BeaMetadataGeneration {
        self.metadata_generation
    }

    /// Returns the precision-preserving effective period.
    pub const fn period(&self) -> &BeaTimePeriod {
        &self.period
    }

    /// Returns the source production instant when supplied.
    pub const fn production_time(&self) -> Option<Timestamp> {
        self.production_time
    }

    /// Returns when this installation first observed the response.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the immutable complete version commitment.
    pub const fn version_digest(&self) -> [u8; 32] {
        self.version_digest
    }
}

/// Explicit evidence for a separately observed BEA correction notice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaCorrectionNotice {
    notice_id: String,
    notice_payload_digest: [u8; 32],
    affected_series_digest: [u8; 32],
    published_at: Timestamp,
    observed_at: Timestamp,
    notice_digest: [u8; 32],
}

impl BeaCorrectionNotice {
    /// Builds a bounded correction notice without guessing a corrected value.
    ///
    /// # Errors
    ///
    /// Rejects an empty/oversized/control-bearing identifier or local observation before the
    /// notice's source publication instant.
    pub fn try_new(
        notice_id: impl Into<String>,
        notice_payload_digest: [u8; 32],
        affected_series_digest: [u8; 32],
        published_at: Timestamp,
        observed_at: Timestamp,
    ) -> Result<Self, BeaError> {
        let notice_id = notice_id.into();
        if notice_id.is_empty()
            || notice_id.len() > MAX_NOTICE_ID_BYTES
            || notice_id.chars().any(char::is_control)
            || published_at > observed_at
        {
            return Err(BeaError::InvalidRevision);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk-bea-correction-notice-v1");
        hash_text(&mut hasher, &notice_id)?;
        hasher.update(notice_payload_digest);
        hasher.update(affected_series_digest);
        hasher.update(published_at.unix_nanos().to_be_bytes());
        hasher.update(observed_at.unix_nanos().to_be_bytes());
        Ok(Self {
            notice_id,
            notice_payload_digest,
            affected_series_digest,
            published_at,
            observed_at,
            notice_digest: hasher.finalize().into(),
        })
    }

    /// Returns the source/installation-assigned notice identity.
    pub fn notice_id(&self) -> &str {
        &self.notice_id
    }

    /// Returns SHA-256 of the exact correction notice body.
    pub const fn notice_payload_digest(&self) -> [u8; 32] {
        self.notice_payload_digest
    }

    /// Returns the exact affected provider-native family.
    pub const fn affected_series_digest(&self) -> [u8; 32] {
        self.affected_series_digest
    }

    /// Returns the source publication instant.
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }

    /// Returns when this installation observed the notice.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the complete notice commitment.
    pub const fn notice_digest(&self) -> [u8; 32] {
        self.notice_digest
    }
}

/// Source change classification selected from explicit release/correction evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeaRevisionKind {
    /// A later regularly scheduled estimate/release supersedes the predecessor.
    ScheduledRevision,
    /// A separately evidenced BEA correction supplies a corrected successor.
    Correction,
}

/// Append-only event submitted to the future durable correction/revision ledger.
///
/// These values never update a predecessor. Durable composition assigns a contiguous canonical
/// revision only after atomically publishing the new evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeaCorrectionLedgerInput {
    /// The first locally observed version in one native family.
    InitialRelease {
        /// Exact initial row version.
        version: BeaObservedVersion,
    },
    /// A successor release or correction with an exact predecessor edge.
    Revision {
        /// Evidence-backed revision class.
        kind: BeaRevisionKind,
        /// Immutable prior version.
        predecessor: BeaObservedVersion,
        /// Immutable new version.
        successor: Box<BeaObservedVersion>,
        /// Required only for a separately published correction.
        notice: Option<BeaCorrectionNotice>,
    },
    /// A correction notice observed before corrected provider data exists.
    UnresolvedCorrection {
        /// Last known provider row version.
        predecessor: BeaObservedVersion,
        /// Exact correction notice awaiting a corrected payload.
        notice: BeaCorrectionNotice,
    },
}

impl BeaCorrectionLedgerInput {
    /// Builds the first append-only release event.
    pub const fn initial_release(version: BeaObservedVersion) -> Self {
        Self::InitialRelease { version }
    }

    /// Builds an append-only successor event.
    ///
    /// # Errors
    ///
    /// Rejects different native families, identical values, time rollback, a correction without a
    /// matching notice, or a scheduled revision carrying a correction notice.
    pub fn revision(
        predecessor: BeaObservedVersion,
        successor: BeaObservedVersion,
        kind: BeaRevisionKind,
        notice: Option<BeaCorrectionNotice>,
    ) -> Result<Self, BeaError> {
        validate_successor(&predecessor, &successor)?;
        match (kind, notice.as_ref()) {
            (BeaRevisionKind::ScheduledRevision, None) => {}
            (BeaRevisionKind::Correction, Some(notice))
                if notice.affected_series_digest() == predecessor.series_digest()
                    && notice.observed_at() <= successor.observed_at() => {}
            _ => return Err(BeaError::InvalidRevision),
        }
        Ok(Self::Revision {
            kind,
            predecessor,
            successor: Box::new(successor),
            notice,
        })
    }

    /// Records a correction notice without fabricating a replacement observation.
    pub fn unresolved_correction(
        predecessor: BeaObservedVersion,
        notice: BeaCorrectionNotice,
    ) -> Result<Self, BeaError> {
        if notice.affected_series_digest() != predecessor.series_digest()
            || notice.observed_at() < predecessor.observed_at()
        {
            return Err(BeaError::InvalidRevision);
        }
        Ok(Self::UnresolvedCorrection {
            predecessor,
            notice,
        })
    }
}

fn validate_successor(
    predecessor: &BeaObservedVersion,
    successor: &BeaObservedVersion,
) -> Result<(), BeaError> {
    if predecessor.series_digest() != successor.series_digest()
        || predecessor.observation_digest() == successor.observation_digest()
        || predecessor.version_digest() == successor.version_digest()
        || successor.observed_at() <= predecessor.observed_at()
    {
        return Err(BeaError::InvalidRevision);
    }
    Ok(())
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaError::InvalidRevision)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}
