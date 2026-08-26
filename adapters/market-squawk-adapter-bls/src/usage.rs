use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::BlsSourceError;

const POLICY_ID: &str = "bls.private-personal-research-no-distribution.v2";

/// Stable provider attribution root presentation must retain with BLS-backed values.
pub const BLS_SOURCE_ATTRIBUTION: &str = "U.S. Bureau of Labor Statistics";

/// Stable product wording for the provider-required post-retrieval change disclaimer.
pub const BLS_POST_RETRIEVAL_NOTICE: &str =
    "BLS is not responsible for changes made to the data after retrieval.";

/// Closed BLS attribution and presentation duties joined by Desktop, MCP, and derived research.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsPresentationObligation {
    obligation_id: SourceIdentifier,
    source_attribution: Box<str>,
    retrieval_date_required: bool,
    post_retrieval_notice: Box<str>,
    truthful_representation_required: bool,
    provider_limit_compliance_required: bool,
    obligation_digest: EvidenceDigest,
}

impl BlsPresentationObligation {
    /// Returns the stable presentation-policy identity.
    pub const fn obligation_id(&self) -> &SourceIdentifier {
        &self.obligation_id
    }

    /// Returns the provider attribution retained with BLS-backed product evidence.
    pub fn source_attribution(&self) -> &str {
        &self.source_attribution
    }

    /// Confirms presentation must show the date on which BLS data was retrieved.
    pub const fn retrieval_date_required(&self) -> bool {
        self.retrieval_date_required
    }

    /// Returns the post-retrieval change notice retained with displayed BLS data.
    pub fn post_retrieval_notice(&self) -> &str {
        &self.post_retrieval_notice
    }

    /// Confirms downstream presentation must represent BLS values truthfully.
    pub const fn truthful_representation_required(&self) -> bool {
        self.truthful_representation_required
    }

    /// Confirms every retrieval path must use the shared provider-limit authority.
    pub const fn provider_limit_compliance_required(&self) -> bool {
        self.provider_limit_compliance_required
    }

    /// Returns the complete code-owned obligation identity.
    pub const fn obligation_digest(&self) -> EvidenceDigest {
        self.obligation_digest
    }

    /// Reconstructs the closed obligation instead of trusting copied fields.
    pub fn validate(&self) -> Result<(), BlsSourceError> {
        if self == &bls_presentation_obligation()? {
            Ok(())
        } else {
            Err(BlsSourceError::InvalidUsagePolicy)
        }
    }
}

/// Closed set of BLS data uses evaluated at the provider boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlsUsageOperation {
    /// Retrieve exact provider responses.
    Retrieve,
    /// Display canonical observations in the owner's local application.
    Display,
    /// Persist raw and canonical observations in the owner's local stores.
    Persist,
    /// Derive local features and research datasets.
    Transform,
    /// Run point-in-time backtests.
    Backtest,
    /// Produce local forecasts.
    Forecast,
    /// Train models for the owner's private research.
    ModelTraining,
    /// Operate trained models for the owner's private research.
    ModelOperation,
    /// Export provider data or derived datasets outside the local research boundary.
    Export,
    /// Sell provider data or derived datasets.
    Sale,
    /// Redistribute provider data or derived datasets to another party.
    Redistribute,
}

/// Fixed provider-local private-use matrix with sale and redistribution permanently denied.
///
/// This value contains no owner evidence and cannot mint application rights. It is reconstructed
/// solely from code-owned operations and presentation duties. The separate
/// [`BlsRootRightsRejoin`] carries the non-authoritative coordinate root must revalidate against
/// its durable rights registry at activation, publication, and read time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlsUsagePolicy {
    policy_digest: EvidenceDigest,
    presentation_obligation_digest: EvidenceDigest,
}

impl BlsUsagePolicy {
    /// Reconstructs the one closed provider-local policy admitted by this adapter.
    pub fn private_personal_research_no_distribution() -> Result<Self, BlsSourceError> {
        let presentation_obligation_digest =
            bls_presentation_obligation()?.obligation_digest();
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/bls-private-use-policy/v2\0");
        hash_obligation_field(&mut digest, POLICY_ID.as_bytes())?;
        for operation in [
            (BlsUsageOperation::Retrieve, true),
            (BlsUsageOperation::Display, true),
            (BlsUsageOperation::Persist, true),
            (BlsUsageOperation::Transform, true),
            (BlsUsageOperation::Backtest, true),
            (BlsUsageOperation::Forecast, true),
            (BlsUsageOperation::ModelTraining, true),
            (BlsUsageOperation::ModelOperation, true),
            (BlsUsageOperation::Export, false),
            (BlsUsageOperation::Sale, false),
            (BlsUsageOperation::Redistribute, false),
        ] {
            hash_obligation_field(&mut digest, operation.0.evidence_name().as_bytes())?;
            digest.update([u8::from(operation.1)]);
        }
        hash_obligation_digest(&mut digest, presentation_obligation_digest);
        Ok(Self {
            policy_digest: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                digest.finalize().into(),
            ),
            presentation_obligation_digest,
        })
    }

    /// Returns the immutable provider-local policy revision.
    pub const fn policy_id() -> &'static str {
        POLICY_ID
    }

    /// Returns the code-owned digest of every admitted and prohibited use.
    pub const fn policy_digest(self) -> EvidenceDigest {
        self.policy_digest
    }

    /// Returns the exact BLS attribution, retrieval-date, and disclaimer contract.
    pub fn presentation_obligation(self) -> Result<BlsPresentationObligation, BlsSourceError> {
        let obligation = bls_presentation_obligation()?;
        if obligation.obligation_digest() != self.presentation_obligation_digest {
            return Err(BlsSourceError::InvalidUsagePolicy);
        }
        Ok(obligation)
    }

    /// Returns the presentation-obligation identity bound into every root rejoin.
    pub const fn presentation_obligation_digest(self) -> EvidenceDigest {
        self.presentation_obligation_digest
    }

    /// Reconstructs and verifies the complete fixed policy.
    pub fn validate(self) -> Result<(), BlsSourceError> {
        if self == Self::private_personal_research_no_distribution()? {
            Ok(())
        } else {
            Err(BlsSourceError::InvalidUsagePolicy)
        }
    }

    /// Returns whether the frozen private/personal authorization admits one exact operation.
    pub const fn admits(self, operation: BlsUsageOperation) -> bool {
        matches!(
            operation,
            BlsUsageOperation::Retrieve
                | BlsUsageOperation::Display
                | BlsUsageOperation::Persist
                | BlsUsageOperation::Transform
                | BlsUsageOperation::Backtest
                | BlsUsageOperation::Forecast
                | BlsUsageOperation::ModelTraining
                | BlsUsageOperation::ModelOperation
        )
    }
}

impl BlsUsageOperation {
    const fn evidence_name(self) -> &'static str {
        match self {
            Self::Retrieve => "retrieve",
            Self::Display => "private_display",
            Self::Persist => "private_persist",
            Self::Transform => "private_transform",
            Self::Backtest => "private_backtest",
            Self::Forecast => "private_forecast",
            Self::ModelTraining => "private_model_training",
            Self::ModelOperation => "private_model_operation",
            Self::Export => "export",
            Self::Sale => "sale",
            Self::Redistribute => "redistribute",
        }
    }
}

/// Non-authoritative coordinate back to the root-owned durable rights decision.
///
/// Construction proves only that root supplied a concrete SHA-256 coordinate and that the
/// adapter's fixed private-use matrix is attached. It does not prove the root record exists,
/// remains current, or grants any operation; root must perform those checks independently.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsRootRightsRejoin {
    root_decision_digest: EvidenceDigest,
    provider_policy_digest: EvidenceDigest,
}

impl BlsRootRightsRejoin {
    /// Binds a nonzero root rights-decision coordinate to the fixed BLS policy.
    pub fn try_new(root_decision_digest: EvidenceDigest) -> Result<Self, BlsSourceError> {
        if root_decision_digest.algorithm() != DigestAlgorithm::Sha256
            || root_decision_digest.bytes() == [0; 32]
        {
            return Err(BlsSourceError::InvalidUsagePolicy);
        }
        let policy = BlsUsagePolicy::private_personal_research_no_distribution()?;
        Ok(Self {
            root_decision_digest,
            provider_policy_digest: policy.policy_digest(),
        })
    }

    /// Returns the exact root-owned rights record identity that must remain current.
    pub const fn root_decision_digest(self) -> EvidenceDigest {
        self.root_decision_digest
    }

    /// Returns the fixed adapter policy joined by that root coordinate.
    pub const fn provider_policy_digest(self) -> EvidenceDigest {
        self.provider_policy_digest
    }

    /// Reopens only the structural rejoin; it does not validate root rights authority.
    pub fn validate(self) -> Result<(), BlsSourceError> {
        if self == Self::try_new(self.root_decision_digest)? {
            Ok(())
        } else {
            Err(BlsSourceError::InvalidUsagePolicy)
        }
    }
}

fn bls_presentation_obligation() -> Result<BlsPresentationObligation, BlsSourceError> {
    let obligation_id = SourceIdentifier::try_from("bls-presentation-obligation-v1")
        .map_err(|_| BlsSourceError::InvalidUsagePolicy)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-presentation-obligation/v1\0");
    hash_obligation_field(&mut digest, obligation_id.as_str().as_bytes())?;
    hash_obligation_field(&mut digest, BLS_SOURCE_ATTRIBUTION.as_bytes())?;
    hash_obligation_field(&mut digest, BLS_POST_RETRIEVAL_NOTICE.as_bytes())?;
    digest.update(b"retrieval-date=required\0");
    digest.update(b"truthful-representation=required\0");
    digest.update(b"provider-limit-compliance=required\0");
    Ok(BlsPresentationObligation {
        obligation_id,
        source_attribution: BLS_SOURCE_ATTRIBUTION.into(),
        retrieval_date_required: true,
        post_retrieval_notice: BLS_POST_RETRIEVAL_NOTICE.into(),
        truthful_representation_required: true,
        provider_limit_compliance_required: true,
        obligation_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ),
    })
}

fn hash_obligation_field(digest: &mut Sha256, value: &[u8]) -> Result<(), BlsSourceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| BlsSourceError::InvalidUsagePolicy)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn hash_obligation_digest(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update(match value.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(value.bytes());
}
