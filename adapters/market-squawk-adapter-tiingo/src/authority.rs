//! Tiingo-specific extension requirements for the product-wide durable provider/account queue.
//!
//! The adapter deliberately owns no quota database or second persistence root. Generic request
//! windows, concurrency, and refusal backoff use `SharedProviderBudget`; the installed extension
//! owns Tiingo monthly unique symbols, monthly response bytes, request-graph checkpoints, and the
//! native-schema circuit. Product composition must root both capabilities in the same stable
//! provider subject and serialized SQLite authority.

use std::fmt;
use std::num::NonZeroU64;

use market_squawk_domain::{
    EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{BudgetUnavailableReason, MonotonicInstant, ProviderRateDeclaration};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    TIINGO_APPLICATION_BYTES_PER_MONTH, TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH,
    TIINGO_PROVIDER_BYTES_PER_MONTH, TIINGO_PROVIDER_UNIQUE_SYMBOLS_PER_MONTH, TiingoHistoryPlan,
    TiingoQuotaAdmission, TiingoSchemaChange, TiingoSchemaCircuitState, TiingoSealedHistoryPage,
    TiingoTicker,
};

/// Exact installation contract the shared provider/account authority must prove at source open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoProviderAuthorityRequirements {
    provider_rate_declaration: ProviderRateDeclaration,
    provider_unique_symbols_per_month: u64,
    application_unique_symbols_per_month: u64,
    provider_bytes_per_month: u64,
    application_bytes_per_month: u64,
    source_id: SourceId,
    source_contract_revision: MetadataRevision,
    native_contract_revision: SourceIdentifier,
    entitlement_generation: SourceIdentifier,
}

impl TiingoProviderAuthorityRequirements {
    pub(crate) fn new(
        provider_rate_declaration: ProviderRateDeclaration,
        source_id: SourceId,
        source_contract_revision: MetadataRevision,
        native_contract_revision: SourceIdentifier,
        entitlement_generation: SourceIdentifier,
    ) -> Self {
        Self {
            provider_rate_declaration,
            provider_unique_symbols_per_month: TIINGO_PROVIDER_UNIQUE_SYMBOLS_PER_MONTH,
            application_unique_symbols_per_month: TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH,
            provider_bytes_per_month: TIINGO_PROVIDER_BYTES_PER_MONTH,
            application_bytes_per_month: TIINGO_APPLICATION_BYTES_PER_MONTH,
            source_id,
            source_contract_revision,
            native_contract_revision,
            entitlement_generation,
        }
    }

    /// Returns the exact registered hour/day/concurrency/backoff declaration.
    pub const fn provider_rate_declaration(&self) -> &ProviderRateDeclaration {
        &self.provider_rate_declaration
    }

    /// Returns Tiingo's documented monthly unique-symbol ceiling.
    pub const fn provider_unique_symbols_per_month(&self) -> u64 {
        self.provider_unique_symbols_per_month
    }

    /// Returns the lower application monthly unique-symbol budget.
    pub const fn application_unique_symbols_per_month(&self) -> u64 {
        self.application_unique_symbols_per_month
    }

    /// Returns Tiingo's documented monthly response-byte ceiling.
    pub const fn provider_bytes_per_month(&self) -> u64 {
        self.provider_bytes_per_month
    }

    /// Returns the lower application monthly response-byte budget.
    pub const fn application_bytes_per_month(&self) -> u64 {
        self.application_bytes_per_month
    }

    /// Returns the exact stable source identity governed by this installation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact activated source contract used by raw capture and publication evidence.
    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }

    /// Returns the exact native decoder contract governed by the durable schema circuit.
    pub const fn native_contract_revision(&self) -> &SourceIdentifier {
        &self.native_contract_revision
    }

    /// Returns the exact credential generation admitted under the stable provider subject.
    pub const fn entitlement_generation(&self) -> &SourceIdentifier {
        &self.entitlement_generation
    }

    fn identity(&self) -> EvidenceDigest {
        digest_fields(&[
            b"market-squawk/tiingo/provider-authority-requirements/v2",
            &self.provider_rate_declaration.declaration_digest().bytes(),
            &self.provider_rate_declaration.policy_digest().bytes(),
            &self.provider_unique_symbols_per_month.to_be_bytes(),
            &self.application_unique_symbols_per_month.to_be_bytes(),
            &self.provider_bytes_per_month.to_be_bytes(),
            &self.application_bytes_per_month.to_be_bytes(),
            self.source_id.as_str().as_bytes(),
            self.source_contract_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
            self.native_contract_revision.as_str().as_bytes(),
            self.entitlement_generation.as_str().as_bytes(),
        ])
    }
}

/// Exact admitted installation returned by the shared durable authority at source construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoProviderAuthorityInstallation {
    requirements_identity: EvidenceDigest,
    authority_generation: SourceIdentifier,
    durable_store_generation: SourceIdentifier,
    authority_evidence: EvidenceDigest,
    admitted_at: Timestamp,
    installation_identity: EvidenceDigest,
}

impl TiingoProviderAuthorityInstallation {
    /// Binds an exact requirements set to one durable authority/store generation.
    pub fn try_new(
        requirements: &TiingoProviderAuthorityRequirements,
        authority_generation: SourceIdentifier,
        durable_store_generation: SourceIdentifier,
        authority_evidence: EvidenceDigest,
        admitted_at: Timestamp,
    ) -> Result<Self, TiingoProviderAuthorityError> {
        if authority_evidence.bytes() == [0; 32] || admitted_at.unix_nanos() < 0 {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        let requirements_identity = requirements.identity();
        let installation_identity = digest_fields(&[
            b"market-squawk/tiingo/provider-authority-installation/v2",
            &requirements_identity.bytes(),
            authority_generation.as_str().as_bytes(),
            durable_store_generation.as_str().as_bytes(),
            &authority_evidence.bytes(),
            &admitted_at.unix_nanos().to_be_bytes(),
        ]);
        Ok(Self {
            requirements_identity,
            authority_generation,
            durable_store_generation,
            authority_evidence,
            admitted_at,
            installation_identity,
        })
    }

    /// Returns the exact durable authority generation that must mint every permit/checkpoint.
    pub const fn authority_generation(&self) -> &SourceIdentifier {
        &self.authority_generation
    }

    /// Returns the exact shared SQLite/store schema generation.
    pub const fn durable_store_generation(&self) -> &SourceIdentifier {
        &self.durable_store_generation
    }

    /// Returns authority-owned registration/store evidence.
    pub const fn authority_evidence(&self) -> EvidenceDigest {
        self.authority_evidence
    }

    /// Returns when exact construction admission completed.
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }

    /// Returns the complete installation identity.
    pub const fn installation_identity(&self) -> EvidenceDigest {
        self.installation_identity
    }

    pub(crate) fn validate_against(
        &self,
        requirements: &TiingoProviderAuthorityRequirements,
    ) -> Result<(), TiingoProviderAuthorityError> {
        let rebuilt = Self::try_new(
            requirements,
            self.authority_generation.clone(),
            self.durable_store_generation.clone(),
            self.authority_evidence,
            self.admitted_at,
        )?;
        if rebuilt == *self {
            Ok(())
        } else {
            Err(TiingoProviderAuthorityError::InvalidReceipt)
        }
    }
}

/// Durable request-graph checkpoint required before any Tiingo history page can dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoHistoryCheckpointReceipt {
    plan_identity: EvidenceDigest,
    maximum_response_bytes: u64,
    page_count: u32,
    next_page_index: u32,
    predecessor_page_identity: Option<EvidenceDigest>,
    authority_generation: SourceIdentifier,
    installation_identity: EvidenceDigest,
    authority_receipt: EvidenceDigest,
    checkpointed_at: Timestamp,
    receipt_identity: EvidenceDigest,
}

impl TiingoHistoryCheckpointReceipt {
    /// Binds one durable storage/checkpoint authority transition to the exact next plan page.
    #[allow(
        clippy::too_many_arguments,
        reason = "plan, predecessor, authority, and durable checkpoint coordinates remain explicit"
    )]
    pub fn try_new(
        plan: &TiingoHistoryPlan,
        next_page_index: u32,
        predecessor_page_identity: Option<EvidenceDigest>,
        authority_generation: SourceIdentifier,
        installation_identity: EvidenceDigest,
        authority_receipt: EvidenceDigest,
        checkpointed_at: Timestamp,
    ) -> Result<Self, TiingoProviderAuthorityError> {
        let page_count = u32::try_from(plan.pages().len())
            .map_err(|_| TiingoProviderAuthorityError::InvalidReceipt)?;
        if page_count == 0
            || next_page_index > page_count
            || (next_page_index == 0) != predecessor_page_identity.is_none()
            || predecessor_page_identity.is_some_and(|digest| digest.bytes() == [0; 32])
            || installation_identity.bytes() == [0; 32]
            || authority_receipt.bytes() == [0; 32]
            || checkpointed_at.unix_nanos() < 0
        {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        let plan_identity = plan.request_set_identity();
        let maximum_response_bytes = plan.maximum_response_bytes();
        let predecessor = predecessor_page_identity
            .map_or_else(|| b"initial".to_vec(), |digest| digest.bytes().to_vec());
        let receipt_identity = digest_fields(&[
            b"market-squawk/tiingo/history-checkpoint/v2",
            &plan_identity.bytes(),
            &maximum_response_bytes.to_be_bytes(),
            &page_count.to_be_bytes(),
            &next_page_index.to_be_bytes(),
            &predecessor,
            authority_generation.as_str().as_bytes(),
            &installation_identity.bytes(),
            &authority_receipt.bytes(),
            &checkpointed_at.unix_nanos().to_be_bytes(),
        ]);
        Ok(Self {
            plan_identity,
            maximum_response_bytes,
            page_count,
            next_page_index,
            predecessor_page_identity,
            authority_generation,
            installation_identity,
            authority_receipt,
            checkpointed_at,
            receipt_identity,
        })
    }

    /// Returns the exact complete plan identity.
    pub const fn plan_identity(&self) -> EvidenceDigest {
        self.plan_identity
    }

    /// Returns the aggregate storage/quota capacity admitted before page zero.
    pub const fn maximum_response_bytes(&self) -> u64 {
        self.maximum_response_bytes
    }

    /// Returns exact planned page cardinality.
    pub const fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Returns the only page index currently authorized to dispatch.
    pub const fn next_page_index(&self) -> u32 {
        self.next_page_index
    }

    /// Returns the immediately preceding sealed page identity, absent only before page zero.
    pub const fn predecessor_page_identity(&self) -> Option<EvidenceDigest> {
        self.predecessor_page_identity
    }

    /// Returns the durable authority generation that committed this checkpoint.
    pub const fn authority_generation(&self) -> &SourceIdentifier {
        &self.authority_generation
    }

    /// Returns the exact source/rate/store installation that authorized this graph transition.
    pub const fn installation_identity(&self) -> EvidenceDigest {
        self.installation_identity
    }

    /// Returns the authority-owned durable checkpoint receipt.
    pub const fn authority_receipt(&self) -> EvidenceDigest {
        self.authority_receipt
    }

    /// Returns when the durable checkpoint commit completed.
    pub const fn checkpointed_at(&self) -> Timestamp {
        self.checkpointed_at
    }

    /// Returns the complete provider-local checkpoint binding.
    pub const fn receipt_identity(&self) -> EvidenceDigest {
        self.receipt_identity
    }

    pub(crate) fn validate_for(
        &self,
        plan: &TiingoHistoryPlan,
        installation: &TiingoProviderAuthorityInstallation,
        expected_next_page_index: u32,
        expected_predecessor: Option<EvidenceDigest>,
    ) -> Result<(), TiingoProviderAuthorityError> {
        let rebuilt = Self::try_new(
            plan,
            self.next_page_index,
            self.predecessor_page_identity,
            self.authority_generation.clone(),
            self.installation_identity,
            self.authority_receipt,
            self.checkpointed_at,
        )?;
        if rebuilt != *self
            || self.next_page_index != expected_next_page_index
            || self.predecessor_page_identity != expected_predecessor
            || self.authority_generation != *installation.authority_generation()
            || self.installation_identity != installation.installation_identity()
            || self.checkpointed_at < installation.admitted_at()
        {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        Ok(())
    }
}

/// Exact secret-free dimensions that the single shared queue must admit before transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoProviderAdmissionRequest {
    ticker: TiingoTicker,
    request_identity: EvidenceDigest,
    maximum_response_bytes: NonZeroU64,
    history_checkpoint: Option<TiingoHistoryCheckpointReceipt>,
}

impl TiingoProviderAdmissionRequest {
    pub(crate) fn new(
        ticker: TiingoTicker,
        request_identity: EvidenceDigest,
        maximum_response_bytes: NonZeroU64,
        history_checkpoint: Option<TiingoHistoryCheckpointReceipt>,
    ) -> Self {
        Self {
            ticker,
            request_identity,
            maximum_response_bytes,
            history_checkpoint,
        }
    }

    /// Returns the exact Tiingo provider ticker charged to monthly uniqueness.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the credential-free exact request identity.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns the maximum response bytes reserved before dispatch.
    pub const fn maximum_response_bytes(&self) -> NonZeroU64 {
        self.maximum_response_bytes
    }

    /// Returns the exact current graph checkpoint for a history-page admission.
    pub const fn history_checkpoint(&self) -> Option<&TiingoHistoryCheckpointReceipt> {
        self.history_checkpoint.as_ref()
    }
}

/// Durable admission receipt minted by the one shared provider/account queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoProviderPermit {
    ticker: TiingoTicker,
    request_identity: EvidenceDigest,
    maximum_response_bytes: NonZeroU64,
    authority_generation: SourceIdentifier,
    installation_identity: EvidenceDigest,
    history_checkpoint_identity: Option<EvidenceDigest>,
    permit_identity: EvidenceDigest,
    admitted_at: Timestamp,
}

impl TiingoProviderPermit {
    /// Constructs the exact receipt returned after the shared transaction has durably reserved
    /// request, concurrency, unique-symbol, and maximum-bandwidth capacity.
    #[allow(
        clippy::too_many_arguments,
        reason = "request, capacity, authority, and durable receipt coordinates remain explicit"
    )]
    pub fn try_new(
        ticker: TiingoTicker,
        request_identity: EvidenceDigest,
        maximum_response_bytes: NonZeroU64,
        authority_generation: SourceIdentifier,
        installation_identity: EvidenceDigest,
        history_checkpoint_identity: Option<EvidenceDigest>,
        permit_identity: EvidenceDigest,
        admitted_at: Timestamp,
    ) -> Result<Self, TiingoProviderAuthorityError> {
        if request_identity.bytes() == [0; 32]
            || installation_identity.bytes() == [0; 32]
            || history_checkpoint_identity.is_some_and(|identity| identity.bytes() == [0; 32])
            || permit_identity.bytes() == [0; 32]
            || admitted_at.unix_nanos() < 0
        {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        Ok(Self {
            ticker,
            request_identity,
            maximum_response_bytes,
            authority_generation,
            installation_identity,
            history_checkpoint_identity,
            permit_identity,
            admitted_at,
        })
    }

    /// Returns the charged Tiingo ticker.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the exact admitted request identity.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns the maximum reserved response bytes.
    pub const fn maximum_response_bytes(&self) -> NonZeroU64 {
        self.maximum_response_bytes
    }

    /// Returns the shared rate-authority generation that minted this permit.
    pub const fn authority_generation(&self) -> &SourceIdentifier {
        &self.authority_generation
    }

    /// Returns the exact source/rate/store installation that admitted this request.
    pub const fn installation_identity(&self) -> EvidenceDigest {
        self.installation_identity
    }

    /// Returns the exact durable graph checkpoint atomically admitted with this permit.
    pub const fn history_checkpoint_identity(&self) -> Option<EvidenceDigest> {
        self.history_checkpoint_identity
    }

    /// Returns the opaque durable permit identity.
    pub const fn permit_identity(&self) -> EvidenceDigest {
        self.permit_identity
    }

    /// Returns the admission transaction clock.
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }

    pub(crate) fn matches(
        &self,
        request: &TiingoProviderAdmissionRequest,
        installation: &TiingoProviderAuthorityInstallation,
    ) -> bool {
        self.ticker == request.ticker
            && self.request_identity == request.request_identity
            && self.maximum_response_bytes == request.maximum_response_bytes
            && self.authority_generation == *installation.authority_generation()
            && self.installation_identity == installation.installation_identity()
            && self.history_checkpoint_identity
                == request
                    .history_checkpoint()
                    .map(TiingoHistoryCheckpointReceipt::receipt_identity)
    }
}

/// One closed outcome from the shared provider/account queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TiingoProviderAdmissionDecision {
    /// All request, concurrency, symbol, and byte dimensions were durably reserved.
    Ready(TiingoProviderPermit),
    /// The shared queue has a monotonic retry coordinate.
    WaitUntil(MonotonicInstant),
    /// One Tiingo-specific quota dimension denied dispatch.
    QuotaDenied(TiingoQuotaAdmission),
    /// The exact restart-durable native-schema circuit opened before atomic dispatch admission.
    SchemaCircuitOpen(TiingoSchemaChange),
    /// The shared provider/account queue is unavailable for a closed reason.
    Unavailable(BudgetUnavailableReason),
}

/// Terminal meaning of one complete retained HTTP response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TiingoCompletedResponseDisposition {
    /// Strict provider-native decoding completed successfully.
    DecodedSuccess,
    /// A bounded non-rate-limit provider refusal was retained.
    ProviderRefusal,
    /// HTTP 429/503 requires atomic shared cooldown before concurrency is released.
    ProviderRateLimited {
        /// Bounded provider `Retry-After` bytes, when supplied.
        retry_after: Option<Box<[u8]>>,
        /// Code-owned bounded jitter sample used only for fallback backoff.
        jitter_sample_basis_points: u16,
    },
    /// A complete response was unusable for a non-schema reason.
    Rejected,
    /// Strict decoding established schema drift; the shared circuit must open atomically.
    SchemaChanged {
        /// Exact reviewed decoder contract that observed the drift.
        contract_revision: SourceIdentifier,
        /// Exact bounded schema-change evidence retained with the open circuit.
        change: TiingoSchemaChange,
    },
}

/// Exact response settlement applied atomically before a permit can release concurrency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TiingoResponseSettlement {
    /// Admission completed, but the exact request never reached transport dispatch.
    NotDispatched,
    /// A complete bounded response body was received and its terminal meaning is known.
    Complete {
        response_bytes: u64,
        disposition: TiingoCompletedResponseDisposition,
    },
    /// Transport stopped before a complete body; the conservative reservation remains charged.
    Incomplete {
        observed_response_bytes: u64,
        charged_response_bytes: u64,
    },
}

impl TiingoResponseSettlement {
    /// Returns exact bytes observed at the socket boundary.
    pub const fn observed_response_bytes(&self) -> u64 {
        match self {
            Self::NotDispatched => 0,
            Self::Complete { response_bytes, .. } => *response_bytes,
            Self::Incomplete {
                observed_response_bytes,
                ..
            } => *observed_response_bytes,
        }
    }

    /// Returns bytes conservatively charged to the durable monthly ledger.
    pub const fn charged_response_bytes(&self) -> u64 {
        match self {
            Self::NotDispatched => 0,
            Self::Complete { response_bytes, .. } => *response_bytes,
            Self::Incomplete {
                charged_response_bytes,
                ..
            } => *charged_response_bytes,
        }
    }

    /// Returns the complete-response disposition, absent for an interrupted transfer.
    pub const fn complete_disposition(&self) -> Option<&TiingoCompletedResponseDisposition> {
        match self {
            Self::Complete { disposition, .. } => Some(disposition),
            Self::NotDispatched | Self::Incomplete { .. } => None,
        }
    }

    /// Returns whether the admitted request was cancelled before any transport dispatch.
    pub const fn was_not_dispatched(&self) -> bool {
        matches!(self, Self::NotDispatched)
    }
}

/// Result of applying one bounded provider refusal to the shared queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoRateLimitDisposition {
    /// Every worker sharing this credential must wait until the exact monotonic coordinate.
    WaitUntil(MonotonicInstant),
    /// The shared queue became unavailable for the exact closed reason.
    Unavailable(BudgetUnavailableReason),
}

/// Single capability required by the HTTP source; implementations must use the shared durable
/// provider/account queue and must not create an adapter-local sidecar store.
pub trait TiingoProviderAuthority: fmt::Debug + Send + Sync {
    /// Proves exact generic rate registration plus durable monthly and schema-circuit dimensions.
    ///
    /// This is a construction-time control-plane check. Returning success requires one shared
    /// provider/account SQLite authority, the exact declaration digests and stable subject, both
    /// lower monthly application budgets, the exact source/source-contract identity, and
    /// restart-durable circuit state for the requested native contract and entitlement generation.
    fn validate_requirements(
        &self,
        requirements: &TiingoProviderAuthorityRequirements,
    ) -> Result<TiingoProviderAuthorityInstallation, TiingoProviderAuthorityError>;

    /// Pre-admits aggregate storage and initializes or resumes the exact durable history graph.
    fn prepare_history_plan(
        &self,
        plan: &TiingoHistoryPlan,
    ) -> Result<TiingoHistoryCheckpointReceipt, TiingoProviderAuthorityError>;

    /// Commits one externally sealed page before the following page becomes admissible.
    fn checkpoint_history_page(
        &self,
        checkpoint: &TiingoHistoryCheckpointReceipt,
        page: &TiingoSealedHistoryPage,
    ) -> Result<TiingoHistoryCheckpointReceipt, TiingoProviderAuthorityError>;

    /// Atomically validates the native-schema circuit and any history checkpoint, admits every
    /// Tiingo-specific quota dimension, and returns only after that exact reservation is durable.
    /// Generic shared-rate reservation follows this transaction and remains uncharged until its
    /// dispatch commit. An open circuit must be returned with its exact evidence; checking it only
    /// before this transaction is not sufficient because another process may open it concurrently.
    fn try_acquire(
        &self,
        request: &TiingoProviderAdmissionRequest,
    ) -> Result<TiingoProviderAdmissionDecision, TiingoProviderAuthorityError>;

    /// Atomically settles dispatch cancellation or bytes and terminal response meaning, then
    /// releases concurrency last.
    ///
    /// A rate-limit disposition must retain the provider-specific response meaning while the
    /// caller still holds the generic shared-rate permit and commits its refusal transition. A
    /// schema-change disposition must commit the open circuit in this transaction. A decoded
    /// success must never clear a schema circuit.
    fn settle_response(
        &self,
        permit: &TiingoProviderPermit,
        settlement: &TiingoResponseSettlement,
    ) -> Result<Option<TiingoRateLimitDisposition>, TiingoProviderAuthorityError>;

    /// Reads restart-durable schema-circuit state for the exact reviewed contract revision.
    fn schema_circuit_state(
        &self,
        contract_revision: &SourceIdentifier,
    ) -> Result<TiingoSchemaCircuitState, TiingoProviderAuthorityError>;
}

/// Failure of the one shared Tiingo provider/account authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TiingoProviderAuthorityError {
    /// Durable state or its exclusive owner is unavailable.
    #[error("Tiingo shared provider authority is unavailable")]
    Unavailable,
    /// The expected durable predecessor or permit no longer matches.
    #[error("Tiingo shared provider authority changed concurrently")]
    Conflict,
    /// Durable state failed closed validation.
    #[error("Tiingo shared provider authority is corrupt")]
    Corrupt,
    /// The authority returned a receipt that did not bind the exact admitted request.
    #[error("Tiingo shared provider authority returned an invalid receipt")]
    InvalidReceipt,
}

fn digest_fields(fields: &[&[u8]]) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    for field in fields {
        hasher.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(field);
    }
    EvidenceDigest::new(
        market_squawk_domain::DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    )
}
