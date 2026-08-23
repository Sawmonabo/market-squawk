//! Retained, retryable shutdown transaction for one published account-market group.

use std::{
    fmt,
    sync::{Arc, Mutex as SyncMutex, Weak},
    time::{Duration, Instant},
};

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_platform::SecretGeneration;
use market_squawk_services::ServiceError;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

use super::{
    AccountMarketRuntimeNeverApplicable, AccountMarketRuntimePublishedCleanupProof,
    AlpacaHistoricalNeverClaimed, AlpacaHistoricalPublishedCleanupProof,
    AlpacaHistoricalSourceMutationAuthority,
    configuration::{AccountMarketSurface, PreparedMarketProviderConfigurationRequest},
    generation::MarketRuntimeGroupGeneration,
    group::{AccountMarketRuntimeHistoryClaim, AccountMarketRuntimeStoppingOwner},
};

const ACCOUNT_STOP_TERMINAL_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/account-group-stop-terminal-receipt/v1\0";

/// Credential-free historical-authority coordinate carried by a durable account stop key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AccountGroupStopHistoryEvidence {
    AlpacaNeverClaimed,
    Alpaca {
        parent_group_generation: EvidenceDigest,
        parent_binding_digest: EvidenceDigest,
    },
    NeverApplicable,
}

/// Stable serializable evidence for one exact physical account shutdown transaction.
///
/// This value intentionally contains no credential material or runtime pointer. Its fields mirror
/// the strict durable lifecycle wire, while the private runtime key separately retains the typed
/// parent authority required to perform cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AccountGroupStopKeyEvidence {
    registry_incarnation: uuid::Uuid,
    surface: AccountMarketSurface,
    onboarding_session_id: uuid::Uuid,
    public_configuration_digest: EvidenceDigest,
    runtime_verification_receipt_digest: EvidenceDigest,
    credential_generation: SecretGeneration,
    group_generation: EvidenceDigest,
    history: AccountGroupStopHistoryEvidence,
}

impl AccountGroupStopKeyEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "every durable account-stop coordinate remains explicit"
    )]
    pub(crate) fn try_new(
        registry_incarnation: uuid::Uuid,
        surface: AccountMarketSurface,
        onboarding_session_id: uuid::Uuid,
        public_configuration_digest: EvidenceDigest,
        runtime_verification_receipt_digest: EvidenceDigest,
        credential_generation: SecretGeneration,
        group_generation: EvidenceDigest,
        history: AccountGroupStopHistoryEvidence,
    ) -> Result<Self, ServiceError> {
        let evidence = Self {
            registry_incarnation,
            surface,
            onboarding_session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest,
            credential_generation,
            group_generation,
            history,
        };
        hash_account_stop_key(&mut Sha256::new(), evidence)?;
        Ok(evidence)
    }

    pub(crate) const fn registry_incarnation(self) -> uuid::Uuid {
        self.registry_incarnation
    }

    pub(crate) const fn surface(self) -> AccountMarketSurface {
        self.surface
    }

    pub(crate) const fn onboarding_session_id(self) -> uuid::Uuid {
        self.onboarding_session_id
    }

    pub(crate) const fn public_configuration_digest(self) -> EvidenceDigest {
        self.public_configuration_digest
    }

    pub(crate) const fn runtime_verification_receipt_digest(self) -> EvidenceDigest {
        self.runtime_verification_receipt_digest
    }

    pub(crate) const fn credential_generation(self) -> SecretGeneration {
        self.credential_generation
    }

    pub(crate) const fn group_generation(self) -> EvidenceDigest {
        self.group_generation
    }

    pub(crate) const fn history(self) -> AccountGroupStopHistoryEvidence {
        self.history
    }

    pub(super) fn validate(self) -> Result<(), ServiceError> {
        hash_account_stop_key(&mut Sha256::new(), self)
    }
}

/// Exact compare-and-set key retained from Active through explicit terminal acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AccountShutdownKey {
    registry_incarnation: uuid::Uuid,
    surface: AccountMarketSurface,
    request: PreparedMarketProviderConfigurationRequest,
    group_generation: MarketRuntimeGroupGeneration,
    history_claim: AccountMarketRuntimeHistoryClaim,
}

impl AccountShutdownKey {
    pub(super) fn try_from_active(
        registry_incarnation: uuid::Uuid,
        request: PreparedMarketProviderConfigurationRequest,
        evidence: &super::group::MarketProviderGroupLifecycleEvidence,
        history_claim: AccountMarketRuntimeHistoryClaim,
    ) -> Result<Self, ServiceError> {
        if registry_incarnation.is_nil()
            || evidence.surface_id().as_str() != request.surface().surface_id()
            || evidence.onboarding_session_id() != request.onboarding_session_id()
            || evidence.public_configuration_digest()
                != request.expected_public_configuration_digest()
            || evidence.runtime_verification_receipt_digest()
                != request.expected_runtime_verification_receipt_digest()
            || evidence.credential_generation() != request.expected_credential_generation()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let key = Self {
            registry_incarnation,
            surface: request.surface(),
            request,
            group_generation: evidence.group_generation(),
            history_claim,
        };
        key.evidence()?;
        Ok(key)
    }

    pub(super) const fn registry_incarnation(&self) -> uuid::Uuid {
        self.registry_incarnation
    }

    pub(super) const fn request(&self) -> PreparedMarketProviderConfigurationRequest {
        self.request
    }

    pub(super) const fn group_generation(&self) -> MarketRuntimeGroupGeneration {
        self.group_generation
    }

    pub(super) const fn history_claim(&self) -> AccountMarketRuntimeHistoryClaim {
        self.history_claim
    }

    pub(super) fn evidence(&self) -> Result<AccountGroupStopKeyEvidence, ServiceError> {
        let group_generation = self.group_generation.digest();
        require_sha256(group_generation)?;
        let history = match self.history_claim {
            AccountMarketRuntimeHistoryClaim::Alpaca(None)
                if self.surface == AccountMarketSurface::AlpacaBasic =>
            {
                AccountGroupStopHistoryEvidence::AlpacaNeverClaimed
            }
            AccountMarketRuntimeHistoryClaim::Alpaca(Some(parent))
                if self.surface == AccountMarketSurface::AlpacaBasic
                    && parent.group_generation() == self.group_generation =>
            {
                require_sha256(parent.binding_digest())?;
                AccountGroupStopHistoryEvidence::Alpaca {
                    parent_group_generation: parent.group_generation().digest(),
                    parent_binding_digest: parent.binding_digest(),
                }
            }
            AccountMarketRuntimeHistoryClaim::NeverApplicable
                if self.surface == AccountMarketSurface::KrakenLevel3 =>
            {
                AccountGroupStopHistoryEvidence::NeverApplicable
            }
            AccountMarketRuntimeHistoryClaim::Alpaca(_)
            | AccountMarketRuntimeHistoryClaim::NeverApplicable => {
                return Err(ServiceError::InvalidRequest);
            }
        };
        Ok(AccountGroupStopKeyEvidence {
            registry_incarnation: self.registry_incarnation,
            surface: self.surface,
            onboarding_session_id: self.request.onboarding_session_id(),
            public_configuration_digest: self.request.expected_public_configuration_digest(),
            runtime_verification_receipt_digest: self
                .request
                .expected_runtime_verification_receipt_digest(),
            credential_generation: self.request.expected_credential_generation(),
            group_generation,
            history,
        })
    }

    pub(super) fn matches(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        expected_group_generation: Option<MarketRuntimeGroupGeneration>,
    ) -> bool {
        self.surface == request.surface()
            && self.request == request
            && expected_group_generation.is_none_or(|expected| expected == self.group_generation)
    }
}

/// Non-cloneable compare-and-set capability prepared without mutating registry state.
pub(crate) struct PreparedAccountGroupStop {
    key: AccountShutdownKey,
}

impl PreparedAccountGroupStop {
    pub(super) const fn new(key: AccountShutdownKey) -> Self {
        Self { key }
    }

    pub(super) const fn into_key(self) -> AccountShutdownKey {
        self.key
    }

    pub(crate) fn key_evidence(&self) -> Result<AccountGroupStopKeyEvidence, ServiceError> {
        self.key.evidence()
    }

    pub(super) fn validate(self) -> Result<(), ServiceError> {
        hash_account_stop_key(&mut Sha256::new(), self.key.evidence()?)
    }
}

impl fmt::Debug for PreparedAccountGroupStop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedAccountGroupStop")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

/// Opaque join capability for one exact coordinator-owned shutdown attempt.
pub(crate) struct AccountGroupStopTicket {
    retained: Arc<RetainedAccountGroupStop>,
    completion: Arc<AccountShutdownCompletion>,
}

impl AccountGroupStopTicket {
    pub(super) fn new(
        retained: Arc<RetainedAccountGroupStop>,
        completion: Arc<AccountShutdownCompletion>,
    ) -> Self {
        Self {
            retained,
            completion,
        }
    }

    pub(super) fn key(&self) -> AccountShutdownKey {
        self.retained.key
    }

    pub(crate) fn key_evidence(&self) -> Result<AccountGroupStopKeyEvidence, ServiceError> {
        self.retained.key.evidence()
    }

    pub(super) async fn wait(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopReceipt, ServiceError> {
        self.completion.wait(deadline, cancellation).await
    }
}

impl fmt::Debug for AccountGroupStopTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccountGroupStopTicket")
            .field("key", &self.retained.key)
            .finish_non_exhaustive()
    }
}

/// Cloneable internal terminal evidence retained until the registry explicitly acknowledges it.
#[derive(Clone)]
pub(crate) struct AccountGroupStopReceipt {
    key: AccountShutdownKey,
    terminal_receipt_digest: EvidenceDigest,
    retained: Weak<RetainedAccountGroupStop>,
}

/// Opaque coordinate proving that the caller durably persisted this exact terminal receipt.
///
/// Construction validates the durable digest shape and binds it to the runtime-minted key and
/// completed physical-phase digest. Registry acknowledgement accepts no raw digest substitute.
pub(crate) struct AccountGroupStopDurableProof {
    key: AccountShutdownKey,
    terminal_receipt_digest: EvidenceDigest,
    durable_terminal_proof_digest: EvidenceDigest,
}

/// Closed result of applying one exact registry acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AccountGroupStopAcknowledgementDisposition {
    Removed,
    AlreadyAcknowledged,
}

/// Runtime-minted authority to advance the exact durable tombstone checkpoint.
///
/// This capability is intentionally non-cloneable. It binds the private runtime key, terminal
/// receipt digest, durable proof, and whether this call removed the tombstone or joined an earlier
/// exact acknowledgement. Durable state values alone cannot construct it.
pub(crate) struct AccountGroupStopAcknowledgementReceipt {
    key: AccountShutdownKey,
    terminal_receipt_digest: EvidenceDigest,
    durable_terminal_proof_digest: EvidenceDigest,
    disposition: AccountGroupStopAcknowledgementDisposition,
}

impl AccountGroupStopAcknowledgementReceipt {
    pub(super) fn try_new(
        receipt: &AccountGroupStopReceipt,
        durable_proof: &AccountGroupStopDurableProof,
        disposition: AccountGroupStopAcknowledgementDisposition,
    ) -> Result<Self, ServiceError> {
        if !durable_proof.matches_receipt(receipt)
            || terminal_receipt_digest(receipt.key)? != receipt.terminal_receipt_digest
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            key: receipt.key,
            terminal_receipt_digest: receipt.terminal_receipt_digest,
            durable_terminal_proof_digest: durable_proof.durable_terminal_proof_digest,
            disposition,
        })
    }

    /// Consumes this runtime authority while checking the exact durable coordinates it advances.
    pub(crate) fn authorize_checkpoint(
        self,
        key_evidence: AccountGroupStopKeyEvidence,
        durable_terminal_proof_digest: EvidenceDigest,
    ) -> Result<AccountGroupStopAcknowledgementDisposition, ServiceError> {
        key_evidence.validate()?;
        require_sha256(durable_terminal_proof_digest)?;
        if self.key.evidence()? != key_evidence
            || terminal_receipt_digest(self.key)? != self.terminal_receipt_digest
            || self.durable_terminal_proof_digest != durable_terminal_proof_digest
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(self.disposition)
    }
}

impl fmt::Debug for AccountGroupStopAcknowledgementReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccountGroupStopAcknowledgementReceipt")
            .field("key", &self.key)
            .field("disposition", &self.disposition)
            .finish_non_exhaustive()
    }
}

/// Bounded registry-retained evidence for the most recently acknowledged stop on one surface.
///
/// Retaining the exact stopped allocation lets concurrent receipt holders observe idempotent
/// success after the tombstone is removed without allowing that old receipt to name or mutate a
/// successor allocation.
pub(super) struct AccountGroupStopAcknowledgement {
    retained: Arc<RetainedAccountGroupStop>,
    terminal_receipt_digest: EvidenceDigest,
    durable_terminal_proof_digest: EvidenceDigest,
}

impl AccountGroupStopAcknowledgement {
    pub(super) fn try_new(
        receipt: &AccountGroupStopReceipt,
        durable_proof: &AccountGroupStopDurableProof,
        retained: &Arc<RetainedAccountGroupStop>,
    ) -> Result<Self, ServiceError> {
        if !retained.is_complete()
            || !receipt.matches_retained(retained)
            || !durable_proof.matches_receipt(receipt)
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            retained: Arc::clone(retained),
            terminal_receipt_digest: receipt.terminal_receipt_digest,
            durable_terminal_proof_digest: durable_proof.durable_terminal_proof_digest,
        })
    }

    pub(super) fn matches_surface(&self, surface: AccountMarketSurface) -> bool {
        self.retained.key.surface == surface
    }

    pub(super) fn matches_receipt(
        &self,
        receipt: &AccountGroupStopReceipt,
        durable_proof: &AccountGroupStopDurableProof,
    ) -> bool {
        receipt.matches_retained(&self.retained)
            && self.terminal_receipt_digest == receipt.terminal_receipt_digest
            && self.durable_terminal_proof_digest == durable_proof.durable_terminal_proof_digest
            && durable_proof.matches_receipt(receipt)
    }

    pub(super) fn reacquire_receipt(
        &self,
        key_evidence: AccountGroupStopKeyEvidence,
        durable_terminal_proof_digest: EvidenceDigest,
    ) -> Result<Option<AccountGroupStopReceipt>, ServiceError> {
        require_sha256(durable_terminal_proof_digest)?;
        if !self.retained.is_complete()
            || self.retained.key.evidence()? != key_evidence
            || self.durable_terminal_proof_digest != durable_terminal_proof_digest
            || terminal_receipt_digest(self.retained.key)? != self.terminal_receipt_digest
        {
            return Ok(None);
        }
        let receipt = AccountGroupStopReceipt {
            key: self.retained.key,
            terminal_receipt_digest: self.terminal_receipt_digest,
            retained: Arc::downgrade(&self.retained),
        };
        if !receipt.matches_retained(&self.retained) {
            return Err(ServiceError::Unavailable);
        }
        Ok(Some(receipt))
    }
}

impl AccountGroupStopReceipt {
    pub(super) const fn key(&self) -> AccountShutdownKey {
        self.key
    }

    pub(super) fn matches_retained(&self, retained: &Arc<RetainedAccountGroupStop>) -> bool {
        self.key == retained.key
            && terminal_receipt_digest(self.key)
                .is_ok_and(|expected| expected == self.terminal_receipt_digest)
            && self
                .retained
                .upgrade()
                .is_some_and(|current| Arc::ptr_eq(&current, retained))
    }

    pub(crate) fn key_evidence(&self) -> Result<AccountGroupStopKeyEvidence, ServiceError> {
        self.key.evidence()
    }

    pub(crate) const fn terminal_receipt_digest(&self) -> EvidenceDigest {
        self.terminal_receipt_digest
    }

    pub(crate) fn bind_durable_proof(
        &self,
        durable_terminal_proof_digest: EvidenceDigest,
    ) -> Result<AccountGroupStopDurableProof, ServiceError> {
        require_sha256(durable_terminal_proof_digest)?;
        if terminal_receipt_digest(self.key)? != self.terminal_receipt_digest {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(AccountGroupStopDurableProof {
            key: self.key,
            terminal_receipt_digest: self.terminal_receipt_digest,
            durable_terminal_proof_digest,
        })
    }
}

impl AccountGroupStopDurableProof {
    fn matches_receipt(&self, receipt: &AccountGroupStopReceipt) -> bool {
        self.key == receipt.key
            && self.terminal_receipt_digest == receipt.terminal_receipt_digest
            && require_sha256(self.durable_terminal_proof_digest).is_ok()
    }
}

impl fmt::Debug for AccountGroupStopDurableProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccountGroupStopDurableProof")
            .field("key", &self.key)
            .field("terminal_receipt_digest", &self.terminal_receipt_digest)
            .field(
                "durable_terminal_proof_digest",
                &self.durable_terminal_proof_digest,
            )
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for AccountGroupStopReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccountGroupStopReceipt")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AccountShutdownPhase {
    Revoked,
    HistoryDraining,
    HistoryDrained,
    RuntimeDraining,
    ReconciliationRequired,
    Complete,
}

pub(super) struct RetainedAccountGroupStop {
    key: AccountShutdownKey,
    state: Mutex<AccountShutdownState>,
    control: SyncMutex<AccountShutdownControl>,
}

struct AccountShutdownState {
    phase: AccountShutdownPhase,
    owner: Option<AccountMarketRuntimeStoppingOwner>,
    history_proof: Option<AccountMarketRuntimePublishedCleanupProof>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccountShutdownControlState {
    Ready,
    Driving,
    ReconciliationRequired,
    Complete,
}

struct AccountShutdownControl {
    state: AccountShutdownControlState,
    completion: Option<Arc<AccountShutdownCompletion>>,
}

impl RetainedAccountGroupStop {
    pub(super) fn new(
        key: AccountShutdownKey,
        owner: AccountMarketRuntimeStoppingOwner,
    ) -> Arc<Self> {
        debug_assert_eq!(key.group_generation, owner.evidence().group_generation());
        debug_assert_eq!(key.history_claim, owner.historical_parent_claim());
        Arc::new(Self {
            key,
            state: Mutex::new(AccountShutdownState {
                phase: AccountShutdownPhase::Revoked,
                owner: Some(owner),
                history_proof: None,
            }),
            control: SyncMutex::new(AccountShutdownControl {
                state: AccountShutdownControlState::Ready,
                completion: None,
            }),
        })
    }

    pub(super) const fn key(&self) -> AccountShutdownKey {
        self.key
    }

    pub(super) fn prepare_drive(
        self: &Arc<Self>,
    ) -> Result<(Arc<AccountShutdownCompletion>, bool), ServiceError> {
        let mut control = self
            .control
            .lock()
            .map_err(|_poisoned| ServiceError::Unavailable)?;
        match control.state {
            AccountShutdownControlState::Driving | AccountShutdownControlState::Complete => {
                let completion = control
                    .completion
                    .as_ref()
                    .ok_or(ServiceError::Unavailable)?;
                Ok((Arc::clone(completion), false))
            }
            AccountShutdownControlState::Ready
            | AccountShutdownControlState::ReconciliationRequired => {
                let completion = Arc::new(AccountShutdownCompletion::pending());
                control.state = AccountShutdownControlState::Driving;
                control.completion = Some(Arc::clone(&completion));
                Ok((completion, true))
            }
        }
    }

    pub(super) async fn drive(
        &self,
        alpaca_historical_source: &AlpacaHistoricalSourceMutationAuthority,
        shutdown_budget: Duration,
    ) -> Result<(), ServiceError> {
        let deadline = Instant::now()
            .checked_add(shutdown_budget)
            .ok_or(ServiceError::Unavailable)?;
        let cancellation = CancellationToken::new();
        let mut state = self.state.lock().await;
        if state.phase == AccountShutdownPhase::Complete {
            return Ok(());
        }
        if state.history_proof.is_none() {
            state.phase = AccountShutdownPhase::HistoryDraining;
            let proof = match self.key.history_claim {
                AccountMarketRuntimeHistoryClaim::Alpaca(Some(parent)) => {
                    if parent.group_generation() != self.key.group_generation {
                        state.phase = AccountShutdownPhase::ReconciliationRequired;
                        return Err(ServiceError::Unavailable);
                    }
                    let receipt = alpaca_historical_source
                        .drain_exact(parent, deadline, &cancellation)
                        .await
                        .map_err(|error| {
                            tracing::error!(%error, "Alpaca historical shutdown barrier failed");
                            ServiceError::Unavailable
                        })?;
                    AccountMarketRuntimePublishedCleanupProof::Alpaca(
                        AlpacaHistoricalPublishedCleanupProof::ExactDrain(receipt),
                    )
                }
                AccountMarketRuntimeHistoryClaim::Alpaca(None) => {
                    AccountMarketRuntimePublishedCleanupProof::Alpaca(
                        AlpacaHistoricalPublishedCleanupProof::NeverClaimed(
                            AlpacaHistoricalNeverClaimed {
                                group_generation: self.key.group_generation,
                                _private: (),
                            },
                        ),
                    )
                }
                AccountMarketRuntimeHistoryClaim::NeverApplicable => {
                    AccountMarketRuntimePublishedCleanupProof::NeverApplicable(
                        AccountMarketRuntimeNeverApplicable {
                            group_generation: self.key.group_generation,
                            _private: (),
                        },
                    )
                }
            };
            state.history_proof = Some(proof);
            state.phase = AccountShutdownPhase::HistoryDrained;
        }
        state.phase = AccountShutdownPhase::RuntimeDraining;
        let AccountShutdownState {
            owner,
            history_proof,
            ..
        } = &mut *state;
        let result = owner
            .as_mut()
            .ok_or(ServiceError::Unavailable)?
            .finish_published_before(
                history_proof.as_ref().ok_or(ServiceError::Unavailable)?,
                deadline,
                &cancellation,
            )
            .await;
        match result {
            Ok(()) => {
                let _completed_owner = owner.take().ok_or(ServiceError::Unavailable)?;
                state.phase = AccountShutdownPhase::Complete;
                Ok(())
            }
            Err(error) => {
                state.phase = AccountShutdownPhase::ReconciliationRequired;
                Err(error)
            }
        }
    }

    pub(super) fn finish_attempt(
        self: &Arc<Self>,
        completion: &Arc<AccountShutdownCompletion>,
        result: Result<(), ServiceError>,
    ) {
        let outcome = result.and_then(|()| {
            Ok(AccountGroupStopReceipt {
                key: self.key,
                terminal_receipt_digest: terminal_receipt_digest(self.key)?,
                retained: Arc::downgrade(self),
            })
        });
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let exact = control.state == AccountShutdownControlState::Driving
            && control
                .completion
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, completion));
        if exact {
            control.state = if outcome.is_ok() {
                AccountShutdownControlState::Complete
            } else {
                AccountShutdownControlState::ReconciliationRequired
            };
        }
        drop(control);
        if exact {
            completion.complete(outcome);
        }
    }

    fn abandon_attempt(self: &Arc<Self>, completion: &Arc<AccountShutdownCompletion>) {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let exact = control.state == AccountShutdownControlState::Driving
            && control
                .completion
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, completion));
        if exact {
            control.state = AccountShutdownControlState::ReconciliationRequired;
        }
        drop(control);
        if exact {
            completion.complete(Err(ServiceError::Unavailable));
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.control
            .lock()
            .is_ok_and(|control| control.state == AccountShutdownControlState::Complete)
    }

    #[cfg(test)]
    pub(super) async fn phase_for_test(&self) -> AccountShutdownPhase {
        self.state.lock().await.phase
    }
}

fn terminal_receipt_digest(key: AccountShutdownKey) -> Result<EvidenceDigest, ServiceError> {
    let mut hasher = Sha256::new();
    hasher.update(ACCOUNT_STOP_TERMINAL_RECEIPT_DOMAIN);
    hash_account_stop_key(&mut hasher, key.evidence()?)?;
    // Closed physical phase tag: Complete.
    hasher.update([1]);
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(ServiceError::Unavailable);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

fn hash_account_stop_key(
    hasher: &mut Sha256,
    key: AccountGroupStopKeyEvidence,
) -> Result<(), ServiceError> {
    if key.registry_incarnation.is_nil() || key.onboarding_session_id.is_nil() {
        return Err(ServiceError::InvalidRequest);
    }
    hasher.update(key.registry_incarnation.as_bytes());
    hash_field(hasher, key.surface.surface_id().as_bytes())?;
    hasher.update(key.onboarding_session_id.as_bytes());
    hash_evidence(hasher, key.public_configuration_digest)?;
    hash_evidence(hasher, key.runtime_verification_receipt_digest)?;
    hasher.update(key.credential_generation.get().to_be_bytes());
    hash_evidence(hasher, key.group_generation)?;
    match (key.surface, key.history) {
        (
            AccountMarketSurface::AlpacaBasic,
            AccountGroupStopHistoryEvidence::AlpacaNeverClaimed,
        ) => hasher.update([1]),
        (
            AccountMarketSurface::AlpacaBasic,
            AccountGroupStopHistoryEvidence::Alpaca {
                parent_group_generation,
                parent_binding_digest,
            },
        ) => {
            if parent_group_generation != key.group_generation {
                return Err(ServiceError::InvalidRequest);
            }
            hasher.update([2]);
            hash_evidence(hasher, parent_group_generation)?;
            hash_evidence(hasher, parent_binding_digest)?;
        }
        (AccountMarketSurface::KrakenLevel3, AccountGroupStopHistoryEvidence::NeverApplicable) => {
            hasher.update([3])
        }
        (AccountMarketSurface::AlpacaBasic, AccountGroupStopHistoryEvidence::NeverApplicable)
        | (
            AccountMarketSurface::KrakenLevel3,
            AccountGroupStopHistoryEvidence::AlpacaNeverClaimed
            | AccountGroupStopHistoryEvidence::Alpaca { .. },
        ) => return Err(ServiceError::InvalidRequest),
    }
    Ok(())
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), ServiceError> {
    let length = u64::try_from(bytes.len()).map_err(|_error| ServiceError::ResourceExhausted)?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn hash_evidence(hasher: &mut Sha256, digest: EvidenceDigest) -> Result<(), ServiceError> {
    require_sha256(digest)?;
    hasher.update([1]);
    hasher.update(digest.bytes());
    Ok(())
}

fn require_sha256(digest: EvidenceDigest) -> Result<(), ServiceError> {
    if digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32] {
        Ok(())
    } else {
        Err(ServiceError::InvalidRequest)
    }
}

/// Armed completion guard owned by the coordinator task itself.
///
/// Tokio drops the task future on abort and Rust drops it during unwind. In either case this guard
/// resolves only its exact still-current attempt as reconciliation-required, so waiters are never
/// stranded behind `Driving` and an obsolete task cannot overwrite a newer attempt.
pub(super) struct AccountShutdownAttemptGuard {
    retained: Arc<RetainedAccountGroupStop>,
    completion: Arc<AccountShutdownCompletion>,
    armed: bool,
}

impl AccountShutdownAttemptGuard {
    pub(super) fn new(
        retained: Arc<RetainedAccountGroupStop>,
        completion: Arc<AccountShutdownCompletion>,
    ) -> Self {
        Self {
            retained,
            completion,
            armed: true,
        }
    }

    pub(super) fn retained(&self) -> &RetainedAccountGroupStop {
        &self.retained
    }

    pub(super) fn finish(mut self, result: Result<(), ServiceError>) {
        self.retained.finish_attempt(&self.completion, result);
        self.armed = false;
    }
}

impl Drop for AccountShutdownAttemptGuard {
    fn drop(&mut self) {
        if self.armed {
            self.retained.abandon_attempt(&self.completion);
        }
    }
}

impl fmt::Debug for RetainedAccountGroupStop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedAccountGroupStop")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

pub(super) struct AccountShutdownCompletion {
    sender: watch::Sender<Option<Result<AccountGroupStopReceipt, ServiceError>>>,
}

impl AccountShutdownCompletion {
    fn pending() -> Self {
        let (sender, _receiver) = watch::channel(None);
        Self { sender }
    }

    fn complete(&self, outcome: Result<AccountGroupStopReceipt, ServiceError>) {
        self.sender.send_replace(Some(outcome));
    }

    async fn wait(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AccountGroupStopReceipt, ServiceError> {
        let mut receiver = self.sender.subscribe();
        loop {
            if let Some(outcome) = receiver.borrow().clone() {
                return outcome;
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    return Err(ServiceError::DeadlineExceeded);
                }
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return Err(ServiceError::Unavailable);
                    }
                }
            }
        }
    }
}
