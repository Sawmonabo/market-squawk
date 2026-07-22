//! Application-owned build evidence, factories, and opaque admitted strategy capabilities.

use std::collections::BTreeMap;
use std::sync::Arc;

use market_squawk_data::Sha256Digest;
use market_squawk_domain::SourceIdentifier;
use market_squawk_execution::{BoundedOrderIntents, StrategyError};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    BacktestContext, BacktestExecutableIdentity, BacktestModelStrategy, ExperimentError,
    TrialComponentBinding,
};

const MAX_BUILD_EVIDENCE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONFIGURATION_BYTES: usize = 4 * 1024 * 1024;

/// Research strategy implementation without authority to self-assert governance identity.
pub trait BacktestStrategy: Send + std::fmt::Debug {
    /// Evaluates only the current immutable point-in-time observation.
    fn on_observation(
        &mut self,
        context: &BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError>;
}

/// Closed implementation family admitted by one application-owned build receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BacktestStrategyClass {
    /// Deterministic strategy without a model inference backend.
    RuleBased,
    /// Strategy whose model binding derives from admitted backend metadata.
    ModelBacked,
}

/// Sealed receipt derived from exact source-closure, executable, and canonical configuration bytes.
#[derive(Clone, Debug)]
pub struct BacktestBuildReceipt {
    build_id: SourceIdentifier,
    class: BacktestStrategyClass,
    strategy: TrialComponentBinding,
    code: TrialComponentBinding,
    configuration_digest: Sha256Digest,
}

impl BacktestBuildReceipt {
    /// Derives all digests inside the trusted application-composition boundary.
    pub fn try_from_evidence(
        build_id: SourceIdentifier,
        class: BacktestStrategyClass,
        strategy_name: SourceIdentifier,
        source_closure: &[u8],
        executable: &[u8],
        canonical_configuration: &[u8],
    ) -> Result<Self, BacktestAdmissionError> {
        if source_closure.is_empty()
            || executable.is_empty()
            || canonical_configuration.is_empty()
            || source_closure.len() > MAX_BUILD_EVIDENCE_BYTES
            || executable.len() > MAX_BUILD_EVIDENCE_BYTES
            || canonical_configuration.len() > MAX_CONFIGURATION_BYTES
        {
            return Err(BacktestAdmissionError::InvalidBuildEvidence);
        }
        let strategy_digest = digest_parts(
            b"market-squawk/backtest-strategy-source-closure/v1",
            &[source_closure],
        );
        let code_digest = digest_parts(
            b"market-squawk/backtest-executable/v1",
            &[source_closure, executable],
        );
        let configuration_digest = digest_parts(
            b"market-squawk/backtest-canonical-configuration/v1",
            &[canonical_configuration],
        );
        let code_name = SourceIdentifier::try_from(format!("{}-executable", build_id.as_str()))
            .map_err(|_| BacktestAdmissionError::InvalidBuildEvidence)?;
        let receipt = Self {
            build_id,
            class,
            strategy: TrialComponentBinding::try_new(strategy_name, strategy_digest)?,
            code: TrialComponentBinding::try_new(code_name, code_digest)?,
            configuration_digest,
        };
        BacktestExecutableIdentity::try_new(
            None,
            receipt.strategy.clone(),
            receipt.code.clone(),
            receipt.configuration_digest,
        )?;
        Ok(receipt)
    }
}

/// Closed factory output; the registry validates it against the receipt's implementation family.
pub enum BacktestStrategyInstance {
    /// Rule-based implementation owned by its registered factory.
    RuleBased(Box<dyn BacktestStrategy>),
    /// Model-backed implementation retaining backend-derived model identity.
    ModelBacked(BacktestModelStrategy),
}

impl std::fmt::Debug for BacktestStrategyInstance {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuleBased(value) => formatter.debug_tuple("RuleBased").field(value).finish(),
            Self::ModelBacked(value) => formatter.debug_tuple("ModelBacked").field(value).finish(),
        }
    }
}

/// Trusted factory owned by the build registry; run-time callers never supply implementations.
pub trait BacktestStrategyFactory: Send + Sync + std::fmt::Debug {
    /// Constructs the exact configured implementation represented by its sealed build receipt.
    fn build(&self) -> Result<BacktestStrategyInstance, BacktestAdmissionError>;
}

/// Private-field registration coupling one sealed build receipt to its owned factory.
pub struct BacktestBuildRegistration {
    receipt: BacktestBuildReceipt,
    factory: Arc<dyn BacktestStrategyFactory>,
}

impl BacktestBuildRegistration {
    /// Couples trusted build evidence and the factory retained by the application registry.
    #[must_use]
    pub fn new(receipt: BacktestBuildReceipt, factory: Arc<dyn BacktestStrategyFactory>) -> Self {
        Self { receipt, factory }
    }
}

impl std::fmt::Debug for BacktestBuildRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BacktestBuildRegistration")
            .field("receipt", &self.receipt)
            .field("factory", &self.factory)
            .finish()
    }
}

#[derive(Clone)]
struct RegisteredBuild {
    receipt: BacktestBuildReceipt,
    factory: Arc<dyn BacktestStrategyFactory>,
}

impl std::fmt::Debug for RegisteredBuild {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegisteredBuild")
            .field("receipt", &self.receipt)
            .field("factory", &self.factory)
            .finish()
    }
}

/// Application-owned registry that alone constructs and mints executable strategy capabilities.
#[derive(Clone, Debug)]
pub struct BacktestStrategyRegistry {
    builds: BTreeMap<SourceIdentifier, RegisteredBuild>,
}

impl BacktestStrategyRegistry {
    /// Validates a duplicate-free startup build manifest before any run input is handled.
    pub fn try_new(
        registrations: Vec<BacktestBuildRegistration>,
    ) -> Result<Self, BacktestAdmissionError> {
        let mut builds = BTreeMap::new();
        for registration in registrations {
            let build_id = registration.receipt.build_id.clone();
            if builds
                .insert(
                    build_id,
                    RegisteredBuild {
                        receipt: registration.receipt,
                        factory: registration.factory,
                    },
                )
                .is_some()
            {
                return Err(BacktestAdmissionError::DuplicateBuild);
            }
        }
        Ok(Self { builds })
    }

    /// Builds and admits a registered implementation using only its stable build identity.
    pub fn admit(
        &self,
        build_id: &SourceIdentifier,
    ) -> Result<AdmittedBacktestStrategy, BacktestAdmissionError> {
        let build = self
            .builds
            .get(build_id)
            .ok_or(BacktestAdmissionError::UnknownBuild)?;
        let instance = build.factory.build()?;
        let (class, model, implementation): (
            BacktestStrategyClass,
            Option<TrialComponentBinding>,
            Box<dyn BacktestStrategy>,
        ) = match instance {
            BacktestStrategyInstance::RuleBased(implementation) => {
                (BacktestStrategyClass::RuleBased, None, implementation)
            }
            BacktestStrategyInstance::ModelBacked(implementation) => {
                let model = implementation
                    .model_binding()
                    .cloned()
                    .ok_or(BacktestAdmissionError::ModelUnavailable)?;
                (
                    BacktestStrategyClass::ModelBacked,
                    Some(model),
                    Box::new(implementation),
                )
            }
        };
        if class != build.receipt.class {
            return Err(BacktestAdmissionError::ClassMismatch);
        }
        Ok(AdmittedBacktestStrategy {
            identity: BacktestExecutableIdentity::try_new(
                model,
                build.receipt.strategy.clone(),
                build.receipt.code.clone(),
                build.receipt.configuration_digest,
            )?,
            implementation,
        })
    }
}

/// Opaque runtime capability owning both executable evidence and the registry-built implementation.
pub struct AdmittedBacktestStrategy {
    identity: BacktestExecutableIdentity,
    implementation: Box<dyn BacktestStrategy>,
}

impl AdmittedBacktestStrategy {
    pub(crate) const fn identity(&self) -> &BacktestExecutableIdentity {
        &self.identity
    }
}

impl std::fmt::Debug for AdmittedBacktestStrategy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdmittedBacktestStrategy")
            .field("identity", &self.identity)
            .field("implementation", &self.implementation)
            .finish()
    }
}

impl BacktestStrategy for AdmittedBacktestStrategy {
    fn on_observation(
        &mut self,
        context: &BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        self.implementation.on_observation(context)
    }
}

fn digest_parts(domain: &[u8], parts: &[&[u8]]) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(domain);
    for part in parts {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        hash.update(part);
    }
    Sha256Digest::new(hash.finalize().into())
}

/// Build-evidence, factory, registry, or admission failure.
#[derive(Debug, Error)]
pub enum BacktestAdmissionError {
    /// Source closure, executable, or configuration evidence was empty or excessive.
    #[error("backtest build evidence is invalid")]
    InvalidBuildEvidence,
    /// A build identity was registered more than once.
    #[error("backtest build identity is duplicated")]
    DuplicateBuild,
    /// A run referenced no application-owned build registration.
    #[error("backtest build identity is not registered")]
    UnknownBuild,
    /// The factory output did not match its sealed receipt's implementation family.
    #[error("backtest strategy class does not match its build receipt")]
    ClassMismatch,
    /// A model-backed factory had no successfully admitted backend metadata.
    #[error("backtest model-backed strategy has no admitted model identity")]
    ModelUnavailable,
    /// A startup build receipt contained invalid immutable evidence.
    #[error("backtest build registration is invalid: {0}")]
    Experiment(#[from] ExperimentError),
}
