//! Immutable locally observed BEA row-version evidence.

use market_squawk_domain::Timestamp;
use sha2::{Digest, Sha256};

use crate::{BeaDataPage, BeaError, BeaMetadataGeneration, BeaTimePeriod};

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

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaError::InvalidRevision)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}
