//! Exact forecast read and recommendation-admission audit; no new source authority.

use super::*;
use crate::FairValueError;

/// Existing source call's purpose, distinct from whether that read succeeded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutomaticValuationForecastPurpose {
    PriceDistribution,
    NativeFinancial,
}

/// One actual source-factory invocation over an already authenticated selected forecast.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationForecastReadAudit {
    purpose: AutomaticValuationForecastPurpose,
    vintage_identity: EvidenceDigest,
    artifact_identity: EvidenceDigest,
    selection_identity: EvidenceDigest,
    started_at: Timestamp,
    completed_at: Timestamp,
    outcome: Result<EvidenceDigest, FairValueError>,
}

impl AutomaticValuationForecastReadAudit {
    /// Restores an audit mirror. None of these IDs reconstructs a forecast or serving receipt.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact source reference and read clocks remain distinct"
    )]
    pub fn try_new(
        purpose: AutomaticValuationForecastPurpose,
        vintage_identity: EvidenceDigest,
        artifact_identity: EvidenceDigest,
        selection_identity: EvidenceDigest,
        started_at: Timestamp,
        completed_at: Timestamp,
        outcome: Result<EvidenceDigest, FairValueError>,
    ) -> Result<Self, AutomaticValuationError> {
        if [vintage_identity, artifact_identity, selection_identity]
            .into_iter()
            .any(|id| !valid_sha256(id))
            || completed_at < started_at
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        match &outcome {
            Ok(id) if valid_sha256(*id) => {}
            Ok(_) => return Err(AutomaticValuationError::InvalidContract),
            Err(error) => {
                source_failure_tag(error)?;
            }
        }
        Ok(Self {
            purpose,
            vintage_identity,
            artifact_identity,
            selection_identity,
            started_at,
            completed_at,
            outcome,
        })
    }
    /// Parses the exact hex references exposed by the existing selected-forecast owner.
    #[allow(
        clippy::too_many_arguments,
        reason = "source-owned reference fields remain distinct"
    )]
    pub fn try_from_reference(
        purpose: AutomaticValuationForecastPurpose,
        vintage: &str,
        artifact: &str,
        selection_identity: EvidenceDigest,
        started_at: Timestamp,
        completed_at: Timestamp,
        outcome: Result<EvidenceDigest, FairValueError>,
    ) -> Result<Self, AutomaticValuationError> {
        let parse = |value| {
            crate::parse_digest_id(value)
                .map(|bytes| EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
                .map_err(|_| AutomaticValuationError::InvalidContract)
        };
        Self::try_new(
            purpose,
            parse(vintage)?,
            parse(artifact)?,
            selection_identity,
            started_at,
            completed_at,
            outcome,
        )
    }
    pub const fn purpose(&self) -> AutomaticValuationForecastPurpose {
        self.purpose
    }
    pub const fn vintage_identity(&self) -> EvidenceDigest {
        self.vintage_identity
    }
    pub const fn artifact_identity(&self) -> EvidenceDigest {
        self.artifact_identity
    }
    pub const fn selection_identity(&self) -> EvidenceDigest {
        self.selection_identity
    }
    pub const fn started_at(&self) -> Timestamp {
        self.started_at
    }
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }
    pub const fn outcome(&self) -> &Result<EvidenceDigest, FairValueError> {
        &self.outcome
    }

    pub(super) fn hash_into(
        &self,
        hash: &mut CanonicalHasher,
    ) -> Result<(), AutomaticValuationError> {
        hash.u8(match self.purpose {
            AutomaticValuationForecastPurpose::PriceDistribution => 1,
            AutomaticValuationForecastPurpose::NativeFinancial => 2,
        });
        hash.fixed(self.vintage_identity.bytes());
        hash.fixed(self.artifact_identity.bytes());
        hash.fixed(self.selection_identity.bytes());
        hash.i64(self.started_at.unix_nanos());
        hash.i64(self.completed_at.unix_nanos());
        match &self.outcome {
            Ok(id) => {
                hash.u8(1);
                hash.fixed(id.bytes());
            }
            Err(error) => {
                hash.u8(2);
                hash.u8(source_failure_tag(error)?);
            }
        }
        Ok(())
    }
}

/// Exact current source factory/constructor error set. An unsupported kind is a contract error,
/// never silently mapped to another failure. These are the original valuation error variants.
fn source_failure_tag(error: &FairValueError) -> Result<u8, AutomaticValuationError> {
    match error {
        FairValueError::InvalidProducerEvidence => Ok(1),
        FairValueError::Arithmetic => Ok(2),
        FairValueError::Persistence => Ok(3),
        _ => Err(AutomaticValuationError::InvalidContract),
    }
}

/// Why an actual successful calculation was or was not used for the recommendation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutomaticValuationRecommendationOutcome {
    /// Exact source reopening and current admission succeeded, and this calculation was selected.
    Selected,
    /// An aggregate result cannot be relabeled as an instrument-unit value.
    NotPerInstrumentUnit,
    /// Reported EPS share restatements are not proven comparable with the quoted share units.
    ShareUnitBasisUnproven,
    /// A higher-priority calculation was selected; this result was not reopened for admission.
    NotCheckedAfterSelection,
    /// The actual source read or selection-time current-admission check failed with this service classification.
    AdmissionFailed(AutomaticValuationFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutomaticValuationRecommendationAudit {
    assessed_at: Timestamp,
    outcome: AutomaticValuationRecommendationOutcome,
}
impl AutomaticValuationRecommendationAudit {
    /// Constructs only an audit mirror; it cannot grant recommendation evidence authority.
    pub const fn new(
        assessed_at: Timestamp,
        outcome: AutomaticValuationRecommendationOutcome,
    ) -> Self {
        Self {
            assessed_at,
            outcome,
        }
    }
    pub const fn assessed_at(&self) -> Timestamp {
        self.assessed_at
    }
    pub const fn outcome(&self) -> AutomaticValuationRecommendationOutcome {
        self.outcome
    }
    pub(super) fn hash_into(&self, hash: &mut CanonicalHasher) {
        hash.i64(self.assessed_at.unix_nanos());
        match self.outcome {
            AutomaticValuationRecommendationOutcome::Selected => hash.u8(1),
            AutomaticValuationRecommendationOutcome::NotPerInstrumentUnit => hash.u8(2),
            AutomaticValuationRecommendationOutcome::NotCheckedAfterSelection => hash.u8(3),
            AutomaticValuationRecommendationOutcome::ShareUnitBasisUnproven => hash.u8(5),
            AutomaticValuationRecommendationOutcome::AdmissionFailed(error) => {
                hash.u8(4);
                hash.u8(failure_tag(error));
            }
        }
    }
}
