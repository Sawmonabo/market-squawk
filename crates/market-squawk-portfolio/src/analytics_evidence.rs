//! Revision-derived point-in-time authority for portfolio analytics.

use std::num::NonZeroU32;

use market_squawk_data::{DatasetManifestRef, Sha256Digest};
use market_squawk_domain::{SourceIdentifier, Timestamp};
use sha2::{Digest as _, Sha256};

use crate::{PortfolioError, PortfolioRevision, PortfolioRevisionId};

/// One explicit versioned policy identity used to produce portfolio analytics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsPolicyBinding {
    id: SourceIdentifier,
    version: NonZeroU32,
    semantic_digest: Sha256Digest,
}

impl AnalyticsPolicyBinding {
    /// Constructs a canonical policy binding.
    ///
    /// # Errors
    ///
    /// The result type preserves constructor symmetry for future policy admission rules.
    pub fn try_new(id: SourceIdentifier, version: NonZeroU32) -> Result<Self, PortfolioError> {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/portfolio-analytics-policy/v1");
        update_bytes(&mut hash, id.as_str().as_bytes());
        hash.update(version.get().to_be_bytes());
        Ok(Self {
            id,
            version,
            semantic_digest: Sha256Digest::new(hash.finalize().into()),
        })
    }

    /// Returns the canonical policy identity.
    pub const fn id(&self) -> &SourceIdentifier {
        &self.id
    }

    /// Returns the semantic policy version.
    pub const fn version(&self) -> NonZeroU32 {
        self.version
    }

    /// Returns the versioned policy identity digest.
    pub const fn semantic_digest(&self) -> Sha256Digest {
        self.semantic_digest
    }
}

/// Immutable point-in-time authority for analytics over one exact portfolio revision.
///
/// Fields are private and construction is revision-derived, so callers cannot manufacture or
/// deserialize authority independently of the admitted Task 11 evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioAnalyticsEvidence {
    revision_id: PortfolioRevisionId,
    effective_through: Timestamp,
    available_through: Timestamp,
    dataset: DatasetManifestRef,
    point_in_time_content: Sha256Digest,
    point_in_time_audit: Sha256Digest,
    sources: Vec<SourceIdentifier>,
    valuation_policy: AnalyticsPolicyBinding,
    fx_policy: AnalyticsPolicyBinding,
    as_of_policy: AnalyticsPolicyBinding,
    semantic_digest: Sha256Digest,
}

impl PortfolioAnalyticsEvidence {
    /// Derives immutable analytics authority from one admitted portfolio revision.
    ///
    /// # Errors
    ///
    /// Rejects an effective or availability horizon later than the revision evidence cutoff.
    #[allow(
        clippy::too_many_arguments,
        reason = "all point-in-time and policy authorities remain explicit"
    )]
    pub fn try_from_revision(
        revision: &PortfolioRevision,
        effective_through: Timestamp,
        available_through: Timestamp,
        valuation_policy: AnalyticsPolicyBinding,
        fx_policy: AnalyticsPolicyBinding,
        as_of_policy: AnalyticsPolicyBinding,
    ) -> Result<Self, PortfolioError> {
        let revision_evidence = revision.evidence();
        if effective_through > revision_evidence.as_of()
            || available_through > revision_evidence.as_of()
        {
            return Err(PortfolioError::EvidenceMismatch);
        }
        let dataset = revision_evidence.dataset().clone();
        let point_in_time_content = revision_evidence.point_in_time_content();
        let point_in_time_audit = revision_evidence.point_in_time_audit();
        let sources = revision_evidence.sources().to_vec();
        let semantic_digest = evidence_digest(
            revision.id(),
            effective_through,
            available_through,
            &dataset,
            point_in_time_content,
            point_in_time_audit,
            &sources,
            [&valuation_policy, &fx_policy, &as_of_policy],
        );
        Ok(Self {
            revision_id: revision.id(),
            effective_through,
            available_through,
            dataset,
            point_in_time_content,
            point_in_time_audit,
            sources,
            valuation_policy,
            fx_policy,
            as_of_policy,
            semantic_digest,
        })
    }

    /// Returns the exact immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns the latest effective input time admitted by this authority.
    pub const fn effective_through(&self) -> Timestamp {
        self.effective_through
    }

    /// Returns the latest availability time admitted by this authority.
    pub const fn available_through(&self) -> Timestamp {
        self.available_through
    }

    /// Returns the exact Task 11 dataset generation.
    pub const fn dataset(&self) -> &DatasetManifestRef {
        &self.dataset
    }

    /// Returns the Task 11 point-in-time content identity.
    pub const fn point_in_time_content(&self) -> Sha256Digest {
        self.point_in_time_content
    }

    /// Returns the Task 11 point-in-time audit identity.
    pub const fn point_in_time_audit(&self) -> Sha256Digest {
        self.point_in_time_audit
    }

    /// Returns the canonical Task 11 source identities.
    pub fn sources(&self) -> &[SourceIdentifier] {
        &self.sources
    }

    /// Returns the valuation policy binding.
    pub const fn valuation_policy(&self) -> &AnalyticsPolicyBinding {
        &self.valuation_policy
    }

    /// Returns the FX policy binding.
    pub const fn fx_policy(&self) -> &AnalyticsPolicyBinding {
        &self.fx_policy
    }

    /// Returns the point-in-time as-of policy binding.
    pub const fn as_of_policy(&self) -> &AnalyticsPolicyBinding {
        &self.as_of_policy
    }

    /// Returns the complete versioned semantic authority digest.
    pub const fn semantic_digest(&self) -> Sha256Digest {
        self.semantic_digest
    }

    pub(crate) fn validate_report(
        &self,
        revision: &PortfolioRevision,
        effective_through: Timestamp,
        available_through: Timestamp,
    ) -> Result<(), PortfolioError> {
        self.validate_revision(revision)?;
        self.validate_horizon(effective_through, available_through)
    }

    pub(crate) fn validate_revision(
        &self,
        revision: &PortfolioRevision,
    ) -> Result<(), PortfolioError> {
        if self.revision_id != revision.id() {
            return Err(PortfolioError::RevisionMismatch);
        }
        Ok(())
    }

    pub(crate) fn validate_horizon(
        &self,
        effective_through: Timestamp,
        available_through: Timestamp,
    ) -> Result<(), PortfolioError> {
        if effective_through > self.effective_through || available_through > self.available_through
        {
            return Err(PortfolioError::EvidenceMismatch);
        }
        Ok(())
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds every authority component explicitly"
)]
fn evidence_digest(
    revision_id: PortfolioRevisionId,
    effective_through: Timestamp,
    available_through: Timestamp,
    dataset: &DatasetManifestRef,
    point_in_time_content: Sha256Digest,
    point_in_time_audit: Sha256Digest,
    sources: &[SourceIdentifier],
    policies: [&AnalyticsPolicyBinding; 3],
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/portfolio-analytics-evidence/v1");
    hash.update(revision_id.0);
    hash.update(effective_through.unix_nanos().to_be_bytes());
    hash.update(available_through.unix_nanos().to_be_bytes());
    update_bytes(&mut hash, dataset.dataset_id().as_str().as_bytes());
    hash.update(dataset.manifest_version().to_be_bytes());
    update_bytes(&mut hash, dataset.schema().name().as_bytes());
    hash.update(dataset.schema_version().get().to_be_bytes());
    hash.update(dataset.schema().fingerprint());
    hash.update(dataset.content_hash().bytes());
    hash.update(point_in_time_content.bytes());
    hash.update(point_in_time_audit.bytes());
    hash.update((sources.len() as u128).to_be_bytes());
    for source in sources {
        update_bytes(&mut hash, source.as_str().as_bytes());
    }
    for policy in policies {
        update_bytes(&mut hash, policy.id.as_str().as_bytes());
        hash.update(policy.version.get().to_be_bytes());
        hash.update(policy.semantic_digest.bytes());
    }
    Sha256Digest::new(hash.finalize().into())
}

fn update_bytes(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u128).to_be_bytes());
    hash.update(bytes);
}
