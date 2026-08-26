//! Code-owned BEA personal-research policy and root-rights rejoin coordinates.
//!
//! This module deliberately issues no provider-local use permit. The application rights lease is
//! the sole authorization to retrieve, persist, transform, model, or display BEA data. The
//! adapter retains a fixed policy commitment and a non-authoritative commitment to the root
//! decision only so doctor, capture, canonicalization, and publication can be rejoined to that
//! lease by application composition.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_sources::{DataUseOperation, DataUseRight, OperationAdmission};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const RIGHTS_SCHEMA: &[u8] = b"market-squawk/bea-personal-research-policy/v3";

const SHARED_RIGHTS: [DataUseRight; 6] = [
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Admitted),
    DataUseRight::new(
        DataUseOperation::ModelTraining,
        OperationAdmission::Admitted,
    ),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Blocked),
    DataUseRight::new(
        DataUseOperation::Redistribute,
        OperationAdmission::Blocked,
    ),
];

/// Complete provider-local operation vocabulary covered by the owner's private-use decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeaPersonalResearchOperation {
    /// Retrieve exact official responses.
    Retrieve,
    /// Display canonical data inside the owner's private console.
    PrivateDisplay,
    /// Persist raw, native, and canonical private-research evidence.
    Persist,
    /// Transform retained data into private features and labels.
    Transform,
    /// Run private point-in-time backtests.
    Backtest,
    /// Produce private forecasts.
    Forecast,
    /// Train private models.
    ModelTraining,
    /// Operate private models using retained or transformed BEA data.
    ModelOperation,
    /// Export provider data or derived datasets outside the private boundary.
    Export,
    /// Sell provider data or a product that redistributes it.
    Sale,
    /// Redistribute provider data outside the owner's private boundary.
    Redistribute,
}

impl BeaPersonalResearchOperation {
    const fn evidence_name(self) -> &'static str {
        match self {
            Self::Retrieve => "retrieve",
            Self::PrivateDisplay => "private_display",
            Self::Persist => "persist",
            Self::Transform => "transform",
            Self::Backtest => "backtest",
            Self::Forecast => "forecast",
            Self::ModelTraining => "model_training",
            Self::ModelOperation => "model_operation",
            Self::Export => "export",
            Self::Sale => "sale",
            Self::Redistribute => "redistribute",
        }
    }
}

/// One immutable row in the code-owned BEA private-use policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeaPersonalResearchRight {
    operation: BeaPersonalResearchOperation,
    admission: OperationAdmission,
}

impl BeaPersonalResearchRight {
    const fn new(
        operation: BeaPersonalResearchOperation,
        admission: OperationAdmission,
    ) -> Self {
        Self {
            operation,
            admission,
        }
    }

    /// Returns the exact provider-local operation.
    pub const fn operation(self) -> BeaPersonalResearchOperation {
        self.operation
    }

    /// Returns the fixed policy disposition. It is evidence, not a use permit.
    pub const fn admission(self) -> OperationAdmission {
        self.admission
    }
}

const RESEARCH_RIGHTS: [BeaPersonalResearchRight; 11] = [
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Retrieve,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::PrivateDisplay,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Persist,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Transform,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Backtest,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Forecast,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::ModelTraining,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::ModelOperation,
        OperationAdmission::Admitted,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Export,
        OperationAdmission::Blocked,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Sale,
        OperationAdmission::Blocked,
    ),
    BeaPersonalResearchRight::new(
        BeaPersonalResearchOperation::Redistribute,
        OperationAdmission::Blocked,
    ),
];

/// Invalid non-authoritative rights-decision rejoin evidence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeaRightsError {
    /// Root supplied no valid SHA-256 decision commitment.
    #[error("invalid root BEA rights-decision rejoin evidence")]
    InvalidDecisionRejoin,
}

/// Fixed provider-local policy. It cannot issue authorization or a use permit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BeaPersonalResearchPolicy;

impl BeaPersonalResearchPolicy {
    /// Returns the shared six-operation policy projection.
    pub const fn shared_rights(self) -> &'static [DataUseRight; 6] {
        &SHARED_RIGHTS
    }

    /// Returns the complete provider-local private-use/no-sale matrix.
    pub const fn research_rights(self) -> &'static [BeaPersonalResearchRight; 11] {
        &RESEARCH_RIGHTS
    }

    /// Returns the fixed disposition for one provider-local operation.
    pub const fn admission(
        self,
        operation: BeaPersonalResearchOperation,
    ) -> OperationAdmission {
        match operation {
            BeaPersonalResearchOperation::Retrieve
            | BeaPersonalResearchOperation::PrivateDisplay
            | BeaPersonalResearchOperation::Persist
            | BeaPersonalResearchOperation::Transform
            | BeaPersonalResearchOperation::Backtest
            | BeaPersonalResearchOperation::Forecast
            | BeaPersonalResearchOperation::ModelTraining
            | BeaPersonalResearchOperation::ModelOperation => OperationAdmission::Admitted,
            BeaPersonalResearchOperation::Export
            | BeaPersonalResearchOperation::Sale
            | BeaPersonalResearchOperation::Redistribute => OperationAdmission::Blocked,
        }
    }

    /// Returns the code-owned policy digest independent of any caller or lease.
    pub fn policy_digest(self) -> EvidenceDigest {
        let mut hasher = Sha256::new();
        hasher.update(RIGHTS_SCHEMA);
        for right in RESEARCH_RIGHTS {
            hash_text(&mut hasher, right.operation().evidence_name());
            hash_text(&mut hasher, right.admission().evidence_name());
        }
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
    }
}

/// Non-authoritative coordinate for rejoining adapter evidence to the application rights lease.
///
/// Constructing this value never authorizes an operation. Root must validate and retain the live
/// rights lease before every provider call and before shared publication. The adapter only binds
/// the decision digest so a candidate cannot be transplanted across decisions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaRightsDecisionRejoin {
    root_decision_digest: EvidenceDigest,
    policy_digest: EvidenceDigest,
    rejoin_digest: EvidenceDigest,
}

impl BeaRightsDecisionRejoin {
    /// Binds one root-owned rights decision to the fixed BEA policy.
    pub fn try_new(root_decision_digest: EvidenceDigest) -> Result<Self, BeaRightsError> {
        if root_decision_digest.algorithm() != DigestAlgorithm::Sha256
            || root_decision_digest.bytes() == [0; 32]
        {
            return Err(BeaRightsError::InvalidDecisionRejoin);
        }
        let policy_digest = BeaPersonalResearchPolicy.policy_digest();
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-root-rights-decision-rejoin/v1");
        hasher.update(policy_digest.bytes());
        hasher.update(root_decision_digest.bytes());
        let rejoin_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(Self {
            root_decision_digest,
            policy_digest,
            rejoin_digest,
        })
    }

    /// Returns the fixed provider-local policy commitment.
    pub const fn policy_digest(&self) -> EvidenceDigest {
        self.policy_digest
    }

    /// Returns the root decision coordinate. It is not a lease or authorization.
    pub const fn root_decision_digest(&self) -> EvidenceDigest {
        self.root_decision_digest
    }

    /// Returns the combined non-authoritative rejoin commitment.
    pub const fn rejoin_digest(&self) -> EvidenceDigest {
        self.rejoin_digest
    }
}

fn hash_text(hasher: &mut Sha256, value: &str) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value.as_bytes());
}
