//! Crash-safe, secret-free persistence for provider activation and source lifecycle authority.

mod evidence;

use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, SecretGeneration,
};
use market_squawk_sources::{AuthoritativeSourceRegistry, RegistryError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

use crate::application::{
    AccountMarketSurface, ProductionResearchIngestCoordinator, ResearchIngestCompositionError,
};
use crate::provider_activation::ProviderMarketAccount;
use crate::provider_onboarding::{
    ProviderOnboardingError, ProviderOnboardingService, ProviderRuntimeStartupAdmissions,
};
pub(super) use evidence::ActivationEvidenceCandidate;

const RECIPE_SCHEMA_VERSION: u16 = 5;
const EMBEDDED_PREDECESSOR_RECIPE_SCHEMA_VERSION: u16 = 4;
const PREDECESSOR_RECIPE_SCHEMA_VERSION: u16 = 3;
const LEGACY_RECIPE_SCHEMA_VERSION: u16 = 2;
const QUARANTINE_SCHEMA_VERSION: u16 = 2;
const QUARANTINE_RECORD_KIND: &str = "provider_activation_quarantine";
const MAXIMUM_RECIPE_EVIDENCE_OBJECTS: usize = 1_024;
const ACTIVATION_STATE_DIRECTORY: &str = "sources/provider-activation-v1";
const SOURCE_LIFECYCLE_SCHEMA_VERSION: u16 = 3;
const PROVIDER_METADATA_BACKUP_SCHEMA_VERSION: u16 = 2;
pub(super) const PROVIDER_METADATA_BACKUP_SCHEMA: &str = "market-squawk-provider-metadata-v1";
pub(super) const PROVIDER_METADATA_BACKUP_PRODUCER: &str =
    "market-squawk.provider-metadata-authority";
const MAXIMUM_PROVIDER_METADATA_BACKUP_BYTES: usize = 160 * 1024 * 1024;
const MAXIMUM_BACKUP_EVIDENCE_OBJECT_BYTES: u64 = 1024 * 1024;
const RESTORED_REQUIREMENT_SCHEMA_VERSION: u16 = 1;

pub(super) const RESTORABLE_RESEARCH_SURFACES: [&str; 8] = [
    "sec.edgar-public",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
    "local.files",
    "federal-reserve-board.data-download-program",
];
pub(super) const SERIALIZED_RESEARCH_SURFACES: [&str; 9] = [
    "sec.edgar-public",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
    "local.files",
    "local.portfolio-imports",
    "federal-reserve-board.data-download-program",
];

const COINBASE_DIRECT_LIVE_SURFACE: &str = "coinbase.exchange-direct-market-data";

const SESSION_BACKED_LIVE_SURFACES: [&str; 3] = [
    COINBASE_DIRECT_LIVE_SURFACE,
    ProviderMarketAccount::AlpacaBasic.surface_id(),
    ProviderMarketAccount::KrakenLevel3.surface_id(),
];

const SERIALIZED_LIFECYCLE_SURFACES: [&str; 14] = [
    "coinbase.public-market-data",
    COINBASE_DIRECT_LIVE_SURFACE,
    "kraken.spot-public-market-data",
    "sec.edgar-public",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
    "local.files",
    "local.portfolio-imports",
    ProviderMarketAccount::AlpacaBasic.surface_id(),
    ProviderMarketAccount::KrakenLevel3.surface_id(),
    "federal-reserve-board.data-download-program",
];

/// Least-authority owner seam for the protected provider-metadata component.
#[derive(Clone)]
pub(super) struct ProviderMetadataBackupAuthority {
    activation: DurableProviderActivationState,
    onboarding: Arc<ProviderOnboardingService>,
    research: Arc<ProductionResearchIngestCoordinator>,
}

/// Immutable provider-metadata bytes captured while all three owner fences were held.
pub(super) struct RetainedProviderMetadataBackup {
    bytes: Arc<[u8]>,
    authority_revision_sha256: [u8; 32],
}

/// Explicit operator action required after restoring one provider recipe.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ProviderMetadataRestoreRequirementKind {
    /// Re-run provider onboarding so current credential and entitlement authority is reconstructed.
    Reactivation,
    /// Re-select a local input because ambient paths are intentionally absent from backup state.
    Reselection,
}

/// One secret-free, typed restore requirement retained in the protected component.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ProviderMetadataRestoreRequirement {
    surface_id: String,
    kind: ProviderMetadataRestoreRequirementKind,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProviderMetadataBackupWire {
    schema: String,
    schema_version: u16,
    lifecycle_records: Vec<ProviderMetadataStateRecordWire>,
    activation_recipes: Vec<ProviderMetadataStateRecordWire>,
    evidence_objects: Vec<ProviderMetadataEvidenceWire>,
    registry_clean_restart_base64: String,
    restore_requirements: Vec<ProviderMetadataRestoreRequirement>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProviderMetadataStateRecordWire {
    surface_id: String,
    encoded_state_base64: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProviderMetadataEvidenceWire {
    sha256: String,
    bytes_base64: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RestoredProviderRequirementWire {
    schema_version: u16,
    surface_id: String,
    restored_state_sha256: String,
    requirements: Vec<ProviderMetadataRestoreRequirementKind>,
}

struct ValidatedProviderMetadataRestore {
    lifecycle_records: Vec<(String, Vec<u8>)>,
    activation_recipes: Vec<(String, Vec<u8>)>,
    evidence_objects: Vec<(String, Vec<u8>)>,
    registry: Box<[u8]>,
    requirements: Vec<ProviderMetadataRestoreRequirement>,
}

impl ProviderMetadataBackupAuthority {
    pub(super) fn new(
        activation: DurableProviderActivationState,
        onboarding: Arc<ProviderOnboardingService>,
        research: Arc<ProductionResearchIngestCoordinator>,
    ) -> Self {
        Self {
            activation,
            onboarding,
            research,
        }
    }

    /// Acquires the fixed owner order and freezes one immutable provider-metadata revision.
    pub(super) async fn retain(
        &self,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<RetainedProviderMetadataBackup, ProviderMetadataBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProviderMetadataBackupError::Cancelled);
        }
        let activation_fence = self.activation.acquire_provider_metadata_backup().await;
        if cancellation.is_cancelled() {
            return Err(ProviderMetadataBackupError::Cancelled);
        }
        let onboarding_fence = self.onboarding.acquire_runtime_mutation_authority().await;
        if cancellation.is_cancelled() {
            return Err(ProviderMetadataBackupError::Cancelled);
        }
        let registry = self.research.retain_provider_metadata_registry_backup()?;
        let wire = self.activation.export_provider_metadata_wire(&registry)?;
        let bytes = serde_json::to_vec(&wire).map_err(|_| ProviderMetadataBackupError::Invalid)?;
        if bytes.is_empty() || bytes.len() > MAXIMUM_PROVIDER_METADATA_BACKUP_BYTES {
            return Err(ProviderMetadataBackupError::ResourceExhausted);
        }
        let authority_revision_sha256 = Sha256::digest(&bytes).into();
        drop(onboarding_fence);
        drop(activation_fence);
        if cancellation.is_cancelled() {
            return Err(ProviderMetadataBackupError::Cancelled);
        }
        Ok(RetainedProviderMetadataBackup {
            bytes: bytes.into(),
            authority_revision_sha256,
        })
    }

    /// Restores only into absent activation/evidence and registry authority stores.
    pub(super) fn restore_fresh(
        activation: &DurableProviderActivationState,
        registry_store: LocalAuthorityStateStore,
        bytes: &[u8],
    ) -> Result<Vec<ProviderMetadataRestoreRequirement>, ProviderMetadataBackupError> {
        let validated = validate_provider_metadata_backup(bytes)?;
        activation.restore_provider_metadata_fresh(validated, registry_store)
    }

    /// Restores the complete provider-metadata component into one prepared fresh workspace.
    pub(super) fn restore_fresh_workspace(
        paths: &market_squawk_platform::LocalPaths,
        bytes: &[u8],
    ) -> Result<Vec<ProviderMetadataRestoreRequirement>, ProviderMetadataBackupError> {
        let control_root = paths
            .control_root()
            .map_err(|_error| ProviderMetadataBackupError::RestoreTargetNotFresh)?;
        let activation = DurableProviderActivationState::new(control_root.root().to_path_buf());
        let registry_store = LocalAuthorityStateStore::try_open(
            control_root
                .root()
                .join(market_squawk_sources::RESEARCH_SOURCE_AUTHORITY_DIRECTORY),
        )?;
        Self::restore_fresh(&activation, registry_store, bytes)
    }
}

impl std::fmt::Debug for ProviderMetadataBackupAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProviderMetadataBackupAuthority([SEALED OWNER REFERENCES])")
    }
}

impl RetainedProviderMetadataBackup {
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) const fn authority_revision_sha256(&self) -> [u8; 32] {
        self.authority_revision_sha256
    }

    pub(super) fn revalidate_emitted(
        &self,
        authority_revision_sha256: [u8; 32],
        byte_length: u64,
        sha256: [u8; 32],
    ) -> Result<(), ProviderMetadataBackupError> {
        if authority_revision_sha256 != self.authority_revision_sha256
            || usize::try_from(byte_length).ok() != Some(self.bytes.len())
            || sha256 != self.authority_revision_sha256
        {
            return Err(ProviderMetadataBackupError::Invalid);
        }
        Ok(())
    }
}

impl std::fmt::Debug for RetainedProviderMetadataBackup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedProviderMetadataBackup")
            .field("byte_length", &self.bytes.len())
            .field("authority_revision_sha256", &"[SHA-256]")
            .finish()
    }
}

/// Exact activation recipe recovered from crash-safe application-owned state.
pub(super) struct DurableActivationRecipe {
    pub(super) session_id: Uuid,
    pub(super) request_bytes: Box<[u8]>,
    pub(super) evidence_digests: Vec<String>,
    pub(super) runtime_generation_digest: EvidenceDigest,
    pub(super) predecessor_runtime_generation_digest: Option<EvidenceDigest>,
    pub(super) state_digest: EvidenceDigest,
    encoded_state: Box<[u8]>,
    pub(super) staged_predecessor: Option<Box<DurableActivationRecipe>>,
}

/// Closed restart disposition for one durable provider activation surface.
pub(super) enum DurableActivationRecipeState {
    /// No activation has been published for this surface.
    Missing,
    /// One exact activation is durable and must be reconstructed on restart.
    Desired(DurableActivationRecipe),
    /// One exact adapter was prepared but durable onboarding/callable cutover did not complete.
    Staged(DurableActivationRecipe),
    /// Roll-forward authority is durable while exact candidate finalization remains incomplete.
    Cutover(DurableActivationRecipe),
    /// Prior state was disabled and requires a new onboarding activation.
    Quarantined(DurableActivationQuarantine),
}

/// Crash-visible state of one source lifecycle surface.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DurableSourceLifecyclePhase {
    /// A runtime mutation started but did not publish its final result.
    Applying,
    /// Source runtime authority is active.
    Active,
    /// Source configuration is retained but runtime authority is stopped.
    Stopped,
    /// Source configuration and runtime authority were removed.
    Removed,
    /// A prior mutation requires explicit reconciliation.
    ReconciliationRequired,
}

/// Closed original intent retained by one durable lifecycle transaction.
///
/// `Retry` is intentionally absent: retry is authority to resume the exact retained transaction,
/// never authority to replace its operation identity, command digest, or intent.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DurableSourceLifecycleIntent {
    Start,
    Stop,
    Remove,
    Resynchronize,
    Reconfigure,
    Verify,
    VerifyStop,
    UnhealthyRecovery,
    ProductShutdown,
}

/// Monotonic durable checkpoint for one retained lifecycle transaction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DurableSourceLifecycleCheckpoint {
    Planned,
    VerificationBound,
    ShutdownKeyPersisted,
    AccountStopping,
    RuntimeDrained,
    PortalCancelled,
    TerminalPublished,
    TombstoneAcknowledged,
    SuccessorStarted,
    SuccessorDurable,
    ReadsAdmitted,
}

/// Whether the retained transaction is being driven or requires exact reconciliation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DurableSourceLifecyclePendingPhase {
    Applying,
    ReconciliationRequired,
}

/// Closed result retained for the most recently completed lifecycle operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DurableSourceLifecycleCompletionOutcome {
    Applied,
    NoEffect,
}

/// Exact completed physical state hashed into runtime-drain evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DurableSourceLifecyclePhysicalPhase {
    AccountStopCompleted,
    NonAccountRuntimeDrained,
    RuntimeProvenAbsent,
}

/// Typed physical evidence required before a durable lifecycle can cross the drain barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DurableSourceLifecycleRuntimeDrainEvidence {
    AccountGroupCompleted {
        phase: DurableSourceLifecyclePhysicalPhase,
        proof_digest: EvidenceDigest,
    },
    NonAccountDrained {
        phase: DurableSourceLifecyclePhysicalPhase,
        proof_digest: EvidenceDigest,
    },
    RuntimeProvenAbsent {
        phase: DurableSourceLifecyclePhysicalPhase,
        proof_digest: EvidenceDigest,
    },
}

impl DurableSourceLifecycleRuntimeDrainEvidence {
    const fn phase(self) -> DurableSourceLifecyclePhysicalPhase {
        match self {
            Self::AccountGroupCompleted { phase, .. }
            | Self::NonAccountDrained { phase, .. }
            | Self::RuntimeProvenAbsent { phase, .. } => phase,
        }
    }

    const fn proof_digest(self) -> EvidenceDigest {
        match self {
            Self::AccountGroupCompleted { proof_digest, .. }
            | Self::NonAccountDrained { proof_digest, .. }
            | Self::RuntimeProvenAbsent { proof_digest, .. } => proof_digest,
        }
    }
}

/// Exact scalar, non-account digest, or account-group runtime generation retained durably.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DurableSourceRuntimeGeneration {
    Scalar(NonZeroU64),
    NonAccountDigest(EvidenceDigest),
    AccountGroup(EvidenceDigest),
}

impl DurableSourceRuntimeGeneration {
    pub(super) fn non_account_digest(
        digest: EvidenceDigest,
    ) -> Result<Self, DurableProviderActivationStateError> {
        require_sha256(digest)?;
        Ok(Self::NonAccountDigest(digest))
    }

    pub(super) fn account_group(
        digest: EvidenceDigest,
    ) -> Result<Self, DurableProviderActivationStateError> {
        require_sha256(digest)?;
        Ok(Self::AccountGroup(digest))
    }

    pub(super) const fn digest(self) -> Option<EvidenceDigest> {
        match self {
            Self::Scalar(_) => None,
            Self::NonAccountDigest(digest) | Self::AccountGroup(digest) => Some(digest),
        }
    }
}

/// Alpaca historical authority coordinates retained inside an exact account shutdown key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DurableAlpacaHistoricalParent {
    group_generation: EvidenceDigest,
    binding_digest: EvidenceDigest,
}

impl DurableAlpacaHistoricalParent {
    pub(super) fn try_new(
        group_generation: EvidenceDigest,
        binding_digest: EvidenceDigest,
    ) -> Result<Self, DurableProviderActivationStateError> {
        require_sha256(group_generation)?;
        require_sha256(binding_digest)?;
        Ok(Self {
            group_generation,
            binding_digest,
        })
    }

    pub(super) const fn group_generation(self) -> EvidenceDigest {
        self.group_generation
    }

    pub(super) const fn binding_digest(self) -> EvidenceDigest {
        self.binding_digest
    }
}

/// Closed historical-authority coordinate carried by one account shutdown key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DurableAccountHistoryClaim {
    AlpacaNeverClaimed,
    Alpaca(DurableAlpacaHistoricalParent),
    NeverApplicable,
}

/// Serializable, credential-free identity of one exact physical account shutdown transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DurableAccountShutdownKey {
    registry_incarnation: Uuid,
    surface_id: AccountMarketSurface,
    onboarding_session_id: Uuid,
    public_configuration_digest: EvidenceDigest,
    runtime_verification_receipt_digest: EvidenceDigest,
    credential_generation: SecretGeneration,
    group_generation: EvidenceDigest,
    history_claim: DurableAccountHistoryClaim,
}

impl DurableAccountShutdownKey {
    #[allow(
        clippy::too_many_arguments,
        reason = "every exact shutdown-key authority coordinate remains explicit"
    )]
    pub(super) fn try_new(
        registry_incarnation: Uuid,
        surface_id: AccountMarketSurface,
        onboarding_session_id: Uuid,
        public_configuration_digest: EvidenceDigest,
        runtime_verification_receipt_digest: EvidenceDigest,
        credential_generation: SecretGeneration,
        group_generation: EvidenceDigest,
        history_claim: DurableAccountHistoryClaim,
    ) -> Result<Self, DurableProviderActivationStateError> {
        let key = Self {
            registry_incarnation,
            surface_id,
            onboarding_session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest,
            credential_generation,
            group_generation,
            history_claim,
        };
        validate_account_shutdown_key(&key)?;
        Ok(key)
    }

    pub(super) const fn registry_incarnation(self) -> Uuid {
        self.registry_incarnation
    }

    pub(super) const fn surface_id(self) -> AccountMarketSurface {
        self.surface_id
    }

    pub(super) const fn onboarding_session_id(self) -> Uuid {
        self.onboarding_session_id
    }

    pub(super) const fn public_configuration_digest(self) -> EvidenceDigest {
        self.public_configuration_digest
    }

    pub(super) const fn runtime_verification_receipt_digest(self) -> EvidenceDigest {
        self.runtime_verification_receipt_digest
    }

    pub(super) const fn credential_generation(self) -> SecretGeneration {
        self.credential_generation
    }

    pub(super) const fn group_generation(self) -> EvidenceDigest {
        self.group_generation
    }

    pub(super) const fn history_claim(self) -> DurableAccountHistoryClaim {
        self.history_claim
    }
}

/// One settled predecessor/current or intended target lifecycle snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableSourceLifecycleSnapshot {
    phase: DurableSourceLifecyclePhase,
    session_id: Option<Uuid>,
    public_configuration_digest: Option<EvidenceDigest>,
    runtime_verification_receipt_digest: Option<EvidenceDigest>,
    credential_generation: Option<SecretGeneration>,
    runtime_generation: Option<DurableSourceRuntimeGeneration>,
}

/// Exact identity retained for the most recently completed operation.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableSourceLifecycleCompletedOperation {
    transition_revision: NonZeroU64,
    operation_id: SourceIdentifier,
    command_digest: EvidenceDigest,
    transition_digest: EvidenceDigest,
    intent: DurableSourceLifecycleIntent,
    predecessor: DurableSourceLifecycleSnapshot,
    initial_target: DurableSourceLifecycleSnapshot,
    target: DurableSourceLifecycleSnapshot,
    expected_runtime_generation: Option<DurableSourceRuntimeGeneration>,
    shutdown_key: Option<DurableAccountShutdownKey>,
    terminal_checkpoint: DurableSourceLifecycleCheckpoint,
    runtime_drain_evidence: Option<DurableSourceLifecycleRuntimeDrainEvidence>,
    portal_cancellation_proof_digest: Option<EvidenceDigest>,
    outcome: DurableSourceLifecycleCompletionOutcome,
}

/// One crash-visible physical transaction retained independently from settled lifecycle truth.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableSourceLifecyclePendingTransaction {
    transition_revision: NonZeroU64,
    operation_id: SourceIdentifier,
    command_digest: EvidenceDigest,
    transition_digest: EvidenceDigest,
    intent: DurableSourceLifecycleIntent,
    predecessor: DurableSourceLifecycleSnapshot,
    initial_target: DurableSourceLifecycleSnapshot,
    target: DurableSourceLifecycleSnapshot,
    expected_runtime_generation: Option<DurableSourceRuntimeGeneration>,
    shutdown_key: Option<DurableAccountShutdownKey>,
    phase: DurableSourceLifecyclePendingPhase,
    checkpoint: DurableSourceLifecycleCheckpoint,
    runtime_drain_evidence: Option<DurableSourceLifecycleRuntimeDrainEvidence>,
    portal_cancellation_proof_digest: Option<EvidenceDigest>,
}

/// Immutable, validated lifecycle snapshot coordinates exposed to recovery composition.
pub(super) struct DurableSourceLifecycleSnapshotView<'a> {
    snapshot: &'a DurableSourceLifecycleSnapshot,
}

impl DurableSourceLifecycleSnapshotView<'_> {
    pub(super) const fn phase(&self) -> DurableSourceLifecyclePhase {
        self.snapshot.phase
    }

    pub(super) const fn session_id(&self) -> Option<Uuid> {
        self.snapshot.session_id
    }

    pub(super) const fn public_configuration_digest(&self) -> Option<EvidenceDigest> {
        self.snapshot.public_configuration_digest
    }

    pub(super) const fn runtime_verification_receipt_digest(&self) -> Option<EvidenceDigest> {
        self.snapshot.runtime_verification_receipt_digest
    }

    pub(super) const fn credential_generation(&self) -> Option<SecretGeneration> {
        self.snapshot.credential_generation
    }

    pub(super) const fn runtime_generation(&self) -> Option<DurableSourceRuntimeGeneration> {
        self.snapshot.runtime_generation
    }
}

/// Immutable, validated crash-recovery view of one pending lifecycle transaction.
pub(super) struct DurableSourceLifecyclePendingView<'a> {
    pending: &'a DurableSourceLifecyclePendingTransaction,
}

impl DurableSourceLifecyclePendingView<'_> {
    pub(super) const fn transition_revision(&self) -> NonZeroU64 {
        self.pending.transition_revision
    }

    pub(super) const fn intent(&self) -> DurableSourceLifecycleIntent {
        self.pending.intent
    }

    pub(super) fn operation_id(&self) -> &SourceIdentifier {
        &self.pending.operation_id
    }

    pub(super) const fn command_digest(&self) -> EvidenceDigest {
        self.pending.command_digest
    }

    pub(super) const fn transition_digest(&self) -> EvidenceDigest {
        self.pending.transition_digest
    }

    pub(super) const fn phase(&self) -> DurableSourceLifecyclePhase {
        match self.pending.phase {
            DurableSourceLifecyclePendingPhase::Applying => DurableSourceLifecyclePhase::Applying,
            DurableSourceLifecyclePendingPhase::ReconciliationRequired => {
                DurableSourceLifecyclePhase::ReconciliationRequired
            }
        }
    }

    pub(super) const fn checkpoint(&self) -> DurableSourceLifecycleCheckpoint {
        self.pending.checkpoint
    }

    pub(super) const fn predecessor(&self) -> DurableSourceLifecycleSnapshotView<'_> {
        DurableSourceLifecycleSnapshotView {
            snapshot: &self.pending.predecessor,
        }
    }

    pub(super) const fn initial_target(&self) -> DurableSourceLifecycleSnapshotView<'_> {
        DurableSourceLifecycleSnapshotView {
            snapshot: &self.pending.initial_target,
        }
    }

    pub(super) const fn target(&self) -> DurableSourceLifecycleSnapshotView<'_> {
        DurableSourceLifecycleSnapshotView {
            snapshot: &self.pending.target,
        }
    }

    pub(super) const fn expected_runtime_generation(
        &self,
    ) -> Option<DurableSourceRuntimeGeneration> {
        self.pending.expected_runtime_generation
    }

    pub(super) const fn shutdown_key(&self) -> Option<DurableAccountShutdownKey> {
        self.pending.shutdown_key
    }

    pub(super) const fn runtime_drain_proof_digest(&self) -> Option<EvidenceDigest> {
        match self.pending.runtime_drain_evidence {
            Some(evidence) => Some(evidence.proof_digest()),
            None => None,
        }
    }

    pub(super) const fn portal_cancellation_proof_digest(&self) -> Option<EvidenceDigest> {
        self.pending.portal_cancellation_proof_digest
    }
}

/// Validated durable lifecycle record used for compare-and-apply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DurableSourceLifecycleRecord {
    revision: NonZeroU64,
    settled: DurableSourceLifecycleSnapshot,
    last_completed: Option<DurableSourceLifecycleCompletedOperation>,
    pending: Option<DurableSourceLifecyclePendingTransaction>,
}

impl DurableSourceLifecycleRecord {
    pub(super) const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    pub(super) const fn phase(&self) -> DurableSourceLifecyclePhase {
        match &self.pending {
            Some(pending)
                if matches!(pending.phase, DurableSourceLifecyclePendingPhase::Applying) =>
            {
                DurableSourceLifecyclePhase::Applying
            }
            Some(_) => DurableSourceLifecyclePhase::ReconciliationRequired,
            None => self.settled.phase,
        }
    }

    pub(super) const fn settled_view(&self) -> DurableSourceLifecycleSnapshotView<'_> {
        DurableSourceLifecycleSnapshotView {
            snapshot: &self.settled,
        }
    }

    pub(super) const fn pending_view(&self) -> Option<DurableSourceLifecyclePendingView<'_>> {
        match &self.pending {
            Some(pending) => Some(DurableSourceLifecyclePendingView { pending }),
            None => None,
        }
    }

    pub(super) fn operation_id(&self) -> Option<&SourceIdentifier> {
        self.pending
            .as_ref()
            .map(|pending| &pending.operation_id)
            .or_else(|| {
                self.last_completed
                    .as_ref()
                    .map(|completed| &completed.operation_id)
            })
    }

    pub(super) const fn session_id(&self) -> Option<Uuid> {
        match &self.pending {
            Some(pending) => pending.target.session_id,
            None => self.settled.session_id,
        }
    }

    pub(super) const fn public_configuration_digest(&self) -> Option<EvidenceDigest> {
        match &self.pending {
            Some(pending) => pending.target.public_configuration_digest,
            None => self.settled.public_configuration_digest,
        }
    }

    pub(super) const fn runtime_verification_receipt_digest(&self) -> Option<EvidenceDigest> {
        match &self.pending {
            Some(pending) => pending.target.runtime_verification_receipt_digest,
            None => self.settled.runtime_verification_receipt_digest,
        }
    }

    pub(super) const fn credential_generation(&self) -> Option<SecretGeneration> {
        match &self.pending {
            Some(pending) => pending.target.credential_generation,
            None => self.settled.credential_generation,
        }
    }

    pub(super) const fn runtime_generation(&self) -> Option<DurableSourceRuntimeGeneration> {
        match &self.pending {
            Some(pending) => pending.target.runtime_generation,
            None => self.settled.runtime_generation,
        }
    }

    pub(super) const fn pending_intent(&self) -> Option<DurableSourceLifecycleIntent> {
        match &self.pending {
            Some(pending) => Some(pending.intent),
            None => None,
        }
    }

    pub(super) const fn pending_checkpoint(&self) -> Option<DurableSourceLifecycleCheckpoint> {
        match &self.pending {
            Some(pending) => Some(pending.checkpoint),
            None => None,
        }
    }

    pub(super) const fn pending_transition_digest(&self) -> Option<EvidenceDigest> {
        match &self.pending {
            Some(pending) => Some(pending.transition_digest),
            None => None,
        }
    }

    pub(super) fn pending_operation_id(&self) -> Option<&SourceIdentifier> {
        self.pending.as_ref().map(|pending| &pending.operation_id)
    }

    pub(super) const fn pending_command_digest(&self) -> Option<EvidenceDigest> {
        match &self.pending {
            Some(pending) => Some(pending.command_digest),
            None => None,
        }
    }

    pub(super) const fn pending_expected_runtime_generation(
        &self,
    ) -> Option<DurableSourceRuntimeGeneration> {
        match &self.pending {
            Some(pending) => pending.expected_runtime_generation,
            None => None,
        }
    }

    pub(super) const fn pending_shutdown_key(&self) -> Option<DurableAccountShutdownKey> {
        match &self.pending {
            Some(pending) => pending.shutdown_key,
            None => None,
        }
    }

    pub(super) const fn pending_terminal_proof_digest(&self) -> Option<EvidenceDigest> {
        match &self.pending {
            Some(pending) => match pending.runtime_drain_evidence {
                Some(evidence) => Some(evidence.proof_digest()),
                None => None,
            },
            None => None,
        }
    }

    pub(super) const fn pending_portal_cancellation_proof_digest(&self) -> Option<EvidenceDigest> {
        match &self.pending {
            Some(pending) => pending.portal_cancellation_proof_digest,
            None => None,
        }
    }
}

/// Admission result for one exact source lifecycle command.
pub(super) enum DurableSourceLifecycleTransition {
    /// The caller owns the newly durable in-progress mutation.
    Apply(DurableSourceLifecycleRecord),
    /// The same exact command already reached a final durable result.
    Replay(DurableSourceLifecycleRecord),
}

impl DurableSourceLifecycleTransition {
    pub(super) fn transition_digest(
        &self,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        match self {
            Self::Apply(record) => record
                .pending
                .as_ref()
                .map(|pending| pending.transition_digest)
                .ok_or(DurableProviderActivationStateError::InvalidLifecycle),
            Self::Replay(record) => record
                .last_completed
                .as_ref()
                .map(|completed| completed.transition_digest)
                .ok_or(DurableProviderActivationStateError::InvalidLifecycle),
        }
    }

    pub(super) const fn record(&self) -> &DurableSourceLifecycleRecord {
        match self {
            Self::Apply(record) | Self::Replay(record) => record,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceLifecycleWire {
    schema_version: u16,
    surface_id: String,
    revision: u64,
    settled: SourceLifecycleSnapshotWire,
    last_completed: Option<SourceLifecycleCompletedOperationWire>,
    pending: Option<SourceLifecyclePendingTransactionWire>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceLifecycleSnapshotWire {
    phase: DurableSourceLifecyclePhase,
    session_id: Option<Uuid>,
    public_configuration_sha256: Option<String>,
    runtime_verification_receipt_sha256: Option<String>,
    credential_generation: Option<u64>,
    runtime_generation: Option<SourceRuntimeGenerationWire>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
enum SourceRuntimeGenerationWire {
    Scalar { generation: u64 },
    NonAccountDigest { sha256: String },
    AccountGroup { sha256: String },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceLifecycleCompletedOperationWire {
    transition_revision: u64,
    operation_id: String,
    command_sha256: String,
    transition_sha256: String,
    intent: DurableSourceLifecycleIntent,
    predecessor: SourceLifecycleSnapshotWire,
    initial_target: SourceLifecycleSnapshotWire,
    target: SourceLifecycleSnapshotWire,
    expected_runtime_generation: Option<SourceRuntimeGenerationWire>,
    shutdown_key: Option<AccountShutdownKeyWire>,
    terminal_checkpoint: DurableSourceLifecycleCheckpoint,
    runtime_drain_evidence: Option<SourceLifecycleRuntimeDrainEvidenceWire>,
    portal_cancellation_proof_sha256: Option<String>,
    outcome: DurableSourceLifecycleCompletionOutcome,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceLifecyclePendingTransactionWire {
    transition_revision: u64,
    operation_id: String,
    command_sha256: String,
    transition_sha256: String,
    intent: DurableSourceLifecycleIntent,
    predecessor: SourceLifecycleSnapshotWire,
    initial_target: SourceLifecycleSnapshotWire,
    target: SourceLifecycleSnapshotWire,
    expected_runtime_generation: Option<SourceRuntimeGenerationWire>,
    shutdown_key: Option<AccountShutdownKeyWire>,
    phase: DurableSourceLifecyclePendingPhase,
    checkpoint: DurableSourceLifecycleCheckpoint,
    runtime_drain_evidence: Option<SourceLifecycleRuntimeDrainEvidenceWire>,
    portal_cancellation_proof_sha256: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
enum SourceLifecycleRuntimeDrainEvidenceWire {
    AccountGroupCompleted {
        phase: DurableSourceLifecyclePhysicalPhase,
        proof_sha256: String,
    },
    NonAccountDrained {
        phase: DurableSourceLifecyclePhysicalPhase,
        proof_sha256: String,
    },
    RuntimeProvenAbsent {
        phase: DurableSourceLifecyclePhysicalPhase,
        proof_sha256: String,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccountShutdownKeyWire {
    registry_incarnation: Uuid,
    surface_id: String,
    onboarding_session_id: Uuid,
    public_configuration_sha256: String,
    runtime_verification_receipt_sha256: String,
    credential_generation: u64,
    group_generation_sha256: String,
    history_claim: AccountHistoryClaimWire,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
enum AccountHistoryClaimWire {
    AlpacaNeverClaimed,
    Alpaca {
        parent_group_generation_sha256: String,
        parent_binding_sha256: String,
    },
    NeverApplicable,
}

/// Secret-free evidence for one disabled activation recipe.
pub(super) struct DurableActivationQuarantine {
    pub(super) session_id: Option<Uuid>,
    pub(super) reason: DurableActivationQuarantineReason,
    pub(super) state_digest: EvidenceDigest,
    pub(super) evidence_digests: Vec<String>,
}

/// Code-owned reasons that disable one provider without blocking the rest of the product.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DurableActivationQuarantineReason {
    /// The durable envelope, recipe, or evidence binding was invalid.
    StateInvalid,
    /// The retained request schema or authority semantics were superseded.
    RequestSuperseded,
    /// The retained onboarding session no longer authorizes adapter construction.
    AuthorityInvalidated,
    /// Adapter reconstruction rejected the retained exact configuration.
    AdapterRejected,
    /// The owning application operation cancelled and withdrew this exact activation.
    Cancelled,
}

/// Controlled persistence for activation recipes and their digest-addressed evidence objects.
#[derive(Clone)]
pub(super) struct DurableProviderActivationState {
    root: PathBuf,
    activation_gate: Arc<AsyncMutex<()>>,
}

impl DurableProviderActivationState {
    pub(super) fn new(control_root: PathBuf) -> Self {
        Self {
            root: control_root.join(ACTIVATION_STATE_DIRECTORY),
            activation_gate: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Returns every exact session still retained by durable runtime lifecycle authority.
    ///
    /// Research recipes and session-backed market groups share onboarding startup reconciliation.
    /// Stopped and indeterminate live records retain restart/reconciliation authority; removed
    /// records do not retain credential authority.
    pub(super) fn startup_runtime_admissions(
        &self,
    ) -> Result<ProviderRuntimeStartupAdmissions, ProviderOnboardingError> {
        let mut entries = Vec::new();
        for surface_id in RESTORABLE_RESEARCH_SURFACES {
            let recovered = match self.restored_requirements(surface_id) {
                Ok(requirements) if !requirements.is_empty() => Vec::new(),
                Err(_) => Vec::new(),
                Ok(_) => match self.load_recipe(surface_id) {
                    Ok(DurableActivationRecipeState::Desired(recipe)) => {
                        vec![recipe.session_id]
                    }
                    Ok(
                        DurableActivationRecipeState::Staged(recipe)
                        | DurableActivationRecipeState::Cutover(recipe),
                    ) => {
                        let mut sessions = std::collections::BTreeSet::from([recipe.session_id]);
                        if let Some(predecessor) = recipe.staged_predecessor {
                            sessions.insert(predecessor.session_id);
                        }
                        sessions.into_iter().collect()
                    }
                    Ok(
                        DurableActivationRecipeState::Missing
                        | DurableActivationRecipeState::Quarantined(_),
                    )
                    | Err(_) => Vec::new(),
                },
            };
            let surface_id = SourceIdentifier::try_from(surface_id)?;
            entries.extend(
                recovered
                    .into_iter()
                    .map(|session_id| (surface_id.clone(), session_id)),
            );
        }
        for surface_id in SESSION_BACKED_LIVE_SURFACES {
            let record = self
                .source_lifecycle_record(surface_id)
                .map_err(|_error| ProviderOnboardingError::InvalidSessionState)?;
            let mut retained_sessions = std::collections::BTreeSet::new();
            if record.settled.phase != DurableSourceLifecyclePhase::Removed
                && let Some(session_id) = record.settled.session_id
            {
                retained_sessions.insert(session_id);
            }
            if let Some(pending) = &record.pending
                && pending.target.phase != DurableSourceLifecyclePhase::Removed
                && let Some(session_id) = pending.target.session_id
            {
                retained_sessions.insert(session_id);
            }
            let surface = SourceIdentifier::try_from(surface_id)?;
            for session_id in retained_sessions {
                entries.push((surface.clone(), session_id));
            }
        }
        ProviderRuntimeStartupAdmissions::try_new(entries)
    }

    /// Serializes every activation that can mutate the shared runtime or evidence index.
    pub(super) async fn acquire_activation(
        &self,
        surface_id: &str,
    ) -> Result<OwnedMutexGuard<()>, DurableProviderActivationStateError> {
        if !SERIALIZED_RESEARCH_SURFACES.contains(&surface_id) {
            return Err(DurableProviderActivationStateError::UnknownSurface);
        }
        Ok(Arc::clone(&self.activation_gate).lock_owned().await)
    }

    async fn acquire_provider_metadata_backup(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.activation_gate).lock_owned().await
    }

    /// Serializes lifecycle compare-and-apply for every code-owned source surface.
    pub(super) async fn acquire_source_lifecycle(
        &self,
        surface_id: &str,
    ) -> Result<OwnedMutexGuard<()>, DurableProviderActivationStateError> {
        if !SERIALIZED_LIFECYCLE_SURFACES.contains(&surface_id) {
            return Err(DurableProviderActivationStateError::UnknownSurface);
        }
        Ok(Arc::clone(&self.activation_gate).lock_owned().await)
    }

    /// Loads the durable compare-and-apply record for one code-owned source surface.
    pub(super) fn source_lifecycle_record(
        &self,
        surface_id: &str,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let key = lifecycle_surface_key(surface_id)?;
        let store = LocalAuthorityStateStore::try_open(self.lifecycle_root(key))?;
        let Some(encoded) = store.load()? else {
            return Ok(DurableSourceLifecycleRecord {
                revision: NonZeroU64::MIN,
                settled: DurableSourceLifecycleSnapshot {
                    phase: DurableSourceLifecyclePhase::Stopped,
                    session_id: None,
                    public_configuration_digest: None,
                    runtime_verification_receipt_digest: None,
                    credential_generation: None,
                    runtime_generation: None,
                },
                last_completed: None,
                pending: None,
            });
        };
        decode_source_lifecycle(surface_id, &encoded)
    }

    /// Durably claims one exact lifecycle transition before mutating runtime authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "original intent, target binding, and expected runtime identity remain explicit"
    )]
    pub(super) fn begin_source_lifecycle_transition(
        &self,
        surface_id: &str,
        expected_revision: NonZeroU64,
        operation_id: SourceIdentifier,
        command_digest: EvidenceDigest,
        intent: DurableSourceLifecycleIntent,
        target_session_id: Option<Uuid>,
        target_public_configuration_digest: Option<EvidenceDigest>,
        expected_runtime_generation: Option<DurableSourceRuntimeGeneration>,
    ) -> Result<DurableSourceLifecycleTransition, DurableProviderActivationStateError> {
        require_sha256(command_digest)?;
        let current = self.source_lifecycle_record(surface_id)?;
        if let Some(pending) = &current.pending {
            if pending.operation_id == operation_id
                && pending.command_digest == command_digest
                && pending.intent == intent
            {
                return Err(DurableProviderActivationStateError::LifecycleReconciliationRequired);
            }
            return Err(DurableProviderActivationStateError::LifecycleReconciliationRequired);
        }
        if let Some(completed) = &current.last_completed
            && completed.operation_id == operation_id
            && completed.command_digest == command_digest
            && completed.intent == intent
        {
            return match completed.outcome {
                DurableSourceLifecycleCompletionOutcome::Applied => {
                    Ok(DurableSourceLifecycleTransition::Replay(current))
                }
                DurableSourceLifecycleCompletionOutcome::NoEffect => {
                    Err(DurableProviderActivationStateError::StaleState)
                }
            };
        }
        if current.revision != expected_revision {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let revision = current
            .revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(DurableProviderActivationStateError::ResourceExhausted)?;
        let predecessor = current.settled.clone();
        let initial_target = source_lifecycle_target(
            surface_id,
            intent,
            &predecessor,
            target_session_id,
            target_public_configuration_digest,
        )?;
        validate_runtime_generation(surface_id, expected_runtime_generation)?;
        let transition_digest = source_lifecycle_transition_digest(
            surface_id,
            revision,
            &operation_id,
            command_digest,
            intent,
            &predecessor,
            &initial_target,
            expected_runtime_generation,
        )?;
        let applying = DurableSourceLifecycleRecord {
            revision,
            settled: predecessor.clone(),
            last_completed: current.last_completed,
            pending: Some(DurableSourceLifecyclePendingTransaction {
                transition_revision: revision,
                operation_id,
                command_digest,
                transition_digest,
                intent,
                predecessor,
                initial_target: initial_target.clone(),
                target: initial_target,
                expected_runtime_generation,
                shutdown_key: None,
                phase: DurableSourceLifecyclePendingPhase::Applying,
                checkpoint: DurableSourceLifecycleCheckpoint::Planned,
                runtime_drain_evidence: None,
                portal_cancellation_proof_digest: None,
            }),
        };
        self.store_source_lifecycle(surface_id, &applying)?;
        Ok(DurableSourceLifecycleTransition::Apply(applying))
    }

    /// Resumes only the exact retained original transaction; retry never mints new intent.
    pub(super) fn resume_source_lifecycle_transition(
        &self,
        surface_id: &str,
        expected_revision: NonZeroU64,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
    ) -> Result<DurableSourceLifecycleTransition, DurableProviderActivationStateError> {
        require_sha256(expected_transition)?;
        let mut current = self.source_lifecycle_record(surface_id)?;
        if current.revision != expected_revision {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let pending = current
            .pending
            .as_mut()
            .ok_or(DurableProviderActivationStateError::LifecycleReconciliationRequired)?;
        if pending.transition_digest != expected_transition || pending.intent != expected_intent {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        pending.phase = DurableSourceLifecyclePendingPhase::Applying;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(DurableSourceLifecycleTransition::Apply(current))
    }

    /// Binds exact runtime-verification evidence before any physical account shutdown key.
    pub(super) fn bind_source_lifecycle_verification(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        runtime_verification_receipt_digest: EvidenceDigest,
        credential_generation: SecretGeneration,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        require_sha256(runtime_verification_receipt_digest)?;
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        let evidence_matches = if pending.target.phase == DurableSourceLifecyclePhase::Removed {
            pending.predecessor.runtime_verification_receipt_digest
                == Some(runtime_verification_receipt_digest)
                && pending.predecessor.credential_generation == Some(credential_generation)
        } else {
            pending.target.runtime_verification_receipt_digest
                == Some(runtime_verification_receipt_digest)
                && pending.target.credential_generation == Some(credential_generation)
        };
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::VerificationBound,
        )? {
            return if evidence_matches {
                Ok(current)
            } else {
                Err(DurableProviderActivationStateError::StaleState)
            };
        }
        if pending.target.phase == DurableSourceLifecyclePhase::Removed && !evidence_matches {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        if pending.target.phase != DurableSourceLifecyclePhase::Removed {
            let may_replace_verification = matches!(
                expected_intent,
                DurableSourceLifecycleIntent::Start
                    | DurableSourceLifecycleIntent::Reconfigure
                    | DurableSourceLifecycleIntent::Verify
                    | DurableSourceLifecycleIntent::VerifyStop
            );
            if !evidence_matches && !may_replace_verification {
                return Err(DurableProviderActivationStateError::StaleState);
            }
            pending.target.runtime_verification_receipt_digest =
                Some(runtime_verification_receipt_digest);
            pending.target.credential_generation = Some(credential_generation);
        }
        pending.checkpoint = DurableSourceLifecycleCheckpoint::VerificationBound;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Persists the exact credential-free physical account key before runtime mutation starts.
    pub(super) fn bind_source_lifecycle_shutdown_key(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        shutdown_key: DurableAccountShutdownKey,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        validate_account_shutdown_key(&shutdown_key)?;
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        validate_shutdown_key_for_pending(surface_id, pending, shutdown_key)?;
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::ShutdownKeyPersisted,
        )? {
            return if pending.shutdown_key == Some(shutdown_key) {
                Ok(current)
            } else {
                Err(DurableProviderActivationStateError::StaleState)
            };
        }
        if pending.shutdown_key.is_some() {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        pending.shutdown_key = Some(shutdown_key);
        pending.checkpoint = DurableSourceLifecycleCheckpoint::ShutdownKeyPersisted;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Records that the runtime registry accepted the exact persisted key as AccountStopping.
    pub(super) fn record_source_lifecycle_account_stopping(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        shutdown_key: DurableAccountShutdownKey,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        require_exact_pending_shutdown_key(surface_id, pending, shutdown_key)?;
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::AccountStopping,
        )? {
            return Ok(current);
        }
        pending.checkpoint = DurableSourceLifecycleCheckpoint::AccountStopping;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Persists exact terminal physical-drain evidence while retaining the account tombstone.
    pub(super) fn record_source_lifecycle_runtime_drained(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        shutdown_key: DurableAccountShutdownKey,
        terminal_proof_digest: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let expected_proof =
            source_lifecycle_account_stop_proof_digest(expected_transition, shutdown_key)?;
        if terminal_proof_digest != expected_proof {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        require_exact_pending_shutdown_key(surface_id, pending, shutdown_key)?;
        let evidence = DurableSourceLifecycleRuntimeDrainEvidence::AccountGroupCompleted {
            phase: DurableSourceLifecyclePhysicalPhase::AccountStopCompleted,
            proof_digest: terminal_proof_digest,
        };
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::RuntimeDrained,
        )? {
            return if pending.runtime_drain_evidence == Some(evidence) {
                Ok(current)
            } else {
                Err(DurableProviderActivationStateError::StaleState)
            };
        }
        if pending.runtime_drain_evidence.is_some() {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        pending.runtime_drain_evidence = Some(evidence);
        pending.checkpoint = DurableSourceLifecycleCheckpoint::RuntimeDrained;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Persists exact scalar/non-account physical-drain evidence before terminal work.
    pub(super) fn record_source_lifecycle_non_account_runtime_drained(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        terminal_proof_digest: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let current = self.source_lifecycle_record(surface_id)?;
        let pending = current
            .pending
            .as_ref()
            .ok_or(DurableProviderActivationStateError::StaleState)?;
        let expected_proof = match pending.predecessor.runtime_generation {
            Some(DurableSourceRuntimeGeneration::Scalar(generation)) => {
                source_lifecycle_non_account_runtime_drain_proof_digest(
                    expected_transition,
                    generation,
                )?
            }
            Some(DurableSourceRuntimeGeneration::NonAccountDigest(digest)) => {
                source_lifecycle_non_account_digest_runtime_drain_proof_digest(
                    expected_transition,
                    digest,
                )?
            }
            _ => return Err(DurableProviderActivationStateError::InvalidLifecycle),
        };
        if terminal_proof_digest != expected_proof {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        self.record_source_lifecycle_runtime_drain_evidence(
            surface_id,
            expected_transition,
            expected_intent,
            DurableSourceLifecycleRuntimeDrainEvidence::NonAccountDrained {
                phase: DurableSourceLifecyclePhysicalPhase::NonAccountRuntimeDrained,
                proof_digest: terminal_proof_digest,
            },
        )
    }

    /// Persists typed proof that no runtime existed before Remove portal cancellation.
    pub(super) fn record_source_lifecycle_runtime_proven_absent(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        terminal_proof_digest: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        if terminal_proof_digest
            != source_lifecycle_runtime_absent_proof_digest(expected_transition)?
        {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        self.record_source_lifecycle_runtime_drain_evidence(
            surface_id,
            expected_transition,
            expected_intent,
            DurableSourceLifecycleRuntimeDrainEvidence::RuntimeProvenAbsent {
                phase: DurableSourceLifecyclePhysicalPhase::RuntimeProvenAbsent,
                proof_digest: terminal_proof_digest,
            },
        )
    }

    fn record_source_lifecycle_runtime_drain_evidence(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        evidence: DurableSourceLifecycleRuntimeDrainEvidence,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::RuntimeDrained,
        )? {
            return if pending.runtime_drain_evidence == Some(evidence) {
                Ok(current)
            } else {
                Err(DurableProviderActivationStateError::StaleState)
            };
        }
        if pending.shutdown_key.is_some() || pending.runtime_drain_evidence.is_some() {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        pending.runtime_drain_evidence = Some(evidence);
        pending.checkpoint = DurableSourceLifecycleCheckpoint::RuntimeDrained;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Persists exact onboarding-portal cancellation evidence for Remove only.
    pub(super) fn record_source_lifecycle_portal_cancelled(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        account_stop_evidence: Option<(DurableAccountShutdownKey, EvidenceDigest)>,
        portal_cancellation_proof_digest: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        if portal_cancellation_proof_digest
            != source_lifecycle_portal_cancellation_proof_digest(expected_transition)?
        {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending = exact_pending_source_lifecycle_mut(
            &mut current,
            expected_transition,
            DurableSourceLifecycleIntent::Remove,
        )?;
        match (pending.shutdown_key, account_stop_evidence) {
            (Some(retained_key), Some((supplied_key, supplied_proof))) => {
                require_exact_pending_shutdown_key(surface_id, pending, supplied_key)?;
                if retained_key != supplied_key
                    || pending.runtime_drain_evidence
                        != Some(
                            DurableSourceLifecycleRuntimeDrainEvidence::AccountGroupCompleted {
                                phase: DurableSourceLifecyclePhysicalPhase::AccountStopCompleted,
                                proof_digest: supplied_proof,
                            },
                        )
                    || supplied_proof
                        != source_lifecycle_account_stop_proof_digest(
                            expected_transition,
                            supplied_key,
                        )?
                {
                    return Err(DurableProviderActivationStateError::StaleState);
                }
            }
            (None, None) if pending.runtime_drain_evidence.is_some() => {}
            _ => return Err(DurableProviderActivationStateError::StaleState),
        }
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::PortalCancelled,
        )? {
            return if pending.portal_cancellation_proof_digest
                == Some(portal_cancellation_proof_digest)
            {
                Ok(current)
            } else {
                Err(DurableProviderActivationStateError::StaleState)
            };
        }
        if pending.portal_cancellation_proof_digest.is_some() {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        pending.portal_cancellation_proof_digest = Some(portal_cancellation_proof_digest);
        pending.checkpoint = DurableSourceLifecycleCheckpoint::PortalCancelled;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Publishes the intended terminal target while retaining the pending transaction.
    ///
    /// Target generation is bound only by `bind_source_lifecycle_target_generation`; terminal
    /// publication cannot mint or replace runtime identity.
    #[allow(
        clippy::too_many_arguments,
        reason = "the complete non-generation terminal binding remains explicit for exact CAS"
    )]
    pub(super) fn complete_source_lifecycle_transition(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        phase: DurableSourceLifecyclePhase,
        session_id: Option<Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        runtime_verification_receipt_digest: Option<EvidenceDigest>,
        credential_generation: Option<SecretGeneration>,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        let terminal = DurableSourceLifecycleSnapshot {
            phase,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest,
            credential_generation,
            runtime_generation: pending.target.runtime_generation,
        };
        validate_source_lifecycle_snapshot(surface_id, &terminal)?;
        validate_terminal_target(&pending.target, &terminal)?;
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::TerminalPublished,
        )? {
            return Ok(current);
        }
        pending.checkpoint = DurableSourceLifecycleCheckpoint::TerminalPublished;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Persists exact registry-tombstone acknowledgement after durable physical proof.
    pub(super) fn record_source_lifecycle_tombstone_acknowledged(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        shutdown_key: DurableAccountShutdownKey,
        terminal_proof_digest: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        require_exact_pending_shutdown_key(surface_id, pending, shutdown_key)?;
        if pending.runtime_drain_evidence
            != Some(
                DurableSourceLifecycleRuntimeDrainEvidence::AccountGroupCompleted {
                    phase: DurableSourceLifecyclePhysicalPhase::AccountStopCompleted,
                    proof_digest: terminal_proof_digest,
                },
            )
            || terminal_proof_digest
                != source_lifecycle_account_stop_proof_digest(expected_transition, shutdown_key)?
        {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::TombstoneAcknowledged,
        )? {
            return Ok(current);
        }
        pending.checkpoint = DurableSourceLifecycleCheckpoint::TombstoneAcknowledged;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Binds the exact successor generation while reads remain closed.
    pub(super) fn bind_source_lifecycle_target_generation(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        target_generation: DurableSourceRuntimeGeneration,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        validate_runtime_generation(surface_id, Some(target_generation))?;
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        if matches!(
            pending.intent,
            DurableSourceLifecycleIntent::Stop
                | DurableSourceLifecycleIntent::Remove
                | DurableSourceLifecycleIntent::VerifyStop
                | DurableSourceLifecycleIntent::ProductShutdown
        ) || pending.target.phase != DurableSourceLifecyclePhase::Active
        {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        if Some(target_generation) == pending.expected_runtime_generation {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::SuccessorStarted,
        )? {
            return if pending.target.runtime_generation == Some(target_generation) {
                Ok(current)
            } else {
                Err(DurableProviderActivationStateError::StaleState)
            };
        }
        if pending.target.runtime_generation.is_some() {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        pending.target.runtime_generation = Some(target_generation);
        pending.checkpoint = DurableSourceLifecycleCheckpoint::SuccessorStarted;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Persists successor durability for the exact still-closed generation.
    pub(super) fn record_source_lifecycle_successor_durable(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        target_generation: DurableSourceRuntimeGeneration,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        if pending.target.runtime_generation != Some(target_generation) {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::SuccessorDurable,
        )? {
            return Ok(current);
        }
        pending.checkpoint = DurableSourceLifecycleCheckpoint::SuccessorDurable;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Records that the exact successor generation is externally readable.
    pub(super) fn record_source_lifecycle_reads_admitted(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
        target_generation: DurableSourceRuntimeGeneration,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending =
            exact_pending_source_lifecycle_mut(&mut current, expected_transition, expected_intent)?;
        if pending.target.runtime_generation != Some(target_generation) {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        if !prepare_source_lifecycle_checkpoint_advance(
            surface_id,
            pending,
            DurableSourceLifecycleCheckpoint::ReadsAdmitted,
        )? {
            return Ok(current);
        }
        pending.checkpoint = DurableSourceLifecycleCheckpoint::ReadsAdmitted;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Clears one exact terminal transaction after all required durable checkpoints are present.
    pub(super) fn confirm_source_lifecycle_transition(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        expected_intent: DurableSourceLifecycleIntent,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending = current
            .pending
            .take()
            .ok_or(DurableProviderActivationStateError::StaleState)?;
        if pending.phase != DurableSourceLifecyclePendingPhase::Applying
            || pending.transition_digest != expected_transition
            || pending.intent != expected_intent
        {
            current.pending = Some(pending);
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let terminal_checkpoint = pending.checkpoint;
        current.settled = pending.target.clone();
        current.last_completed = Some(DurableSourceLifecycleCompletedOperation {
            transition_revision: pending.transition_revision,
            operation_id: pending.operation_id,
            command_digest: pending.command_digest,
            transition_digest: pending.transition_digest,
            intent: pending.intent,
            predecessor: pending.predecessor,
            initial_target: pending.initial_target,
            target: pending.target,
            expected_runtime_generation: pending.expected_runtime_generation,
            shutdown_key: pending.shutdown_key,
            terminal_checkpoint,
            runtime_drain_evidence: pending.runtime_drain_evidence,
            portal_cancellation_proof_digest: pending.portal_cancellation_proof_digest,
            outcome: DurableSourceLifecycleCompletionOutcome::Applied,
        });
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Completes a doctor attempt that had no onboarding mutation, restoring the exact prior
    /// lifecycle phase and bindings while retaining the attempt's revision/audit identity.
    pub(super) fn complete_source_lifecycle_no_effect(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        prior: &DurableSourceLifecycleRecord,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        if matches!(
            prior.phase(),
            DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired
                | DurableSourceLifecyclePhase::Removed
        ) {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending = current
            .pending
            .take()
            .ok_or(DurableProviderActivationStateError::StaleState)?;
        if pending.phase != DurableSourceLifecyclePendingPhase::Applying
            || pending.transition_digest != expected_transition
            || pending.predecessor != prior.settled
            || pending.checkpoint != DurableSourceLifecycleCheckpoint::Planned
            || pending.shutdown_key.is_some()
            || pending.runtime_drain_evidence.is_some()
            || pending.portal_cancellation_proof_digest.is_some()
        {
            current.pending = Some(pending);
            return Err(DurableProviderActivationStateError::StaleState);
        }
        current.settled = pending.predecessor.clone();
        current.last_completed = Some(DurableSourceLifecycleCompletedOperation {
            transition_revision: pending.transition_revision,
            operation_id: pending.operation_id,
            command_digest: pending.command_digest,
            transition_digest: pending.transition_digest,
            intent: pending.intent,
            predecessor: pending.predecessor.clone(),
            initial_target: pending.initial_target,
            target: pending.predecessor,
            expected_runtime_generation: pending.expected_runtime_generation,
            shutdown_key: None,
            terminal_checkpoint: DurableSourceLifecycleCheckpoint::Planned,
            runtime_drain_evidence: None,
            portal_cancellation_proof_digest: None,
            outcome: DurableSourceLifecycleCompletionOutcome::NoEffect,
        });
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Settles one retained account Stop whose validated predecessor was already runtime-absent.
    ///
    /// Unlike the doctor-attempt helper, recovery supplies no reconstructed prior record. The
    /// durable owner derives the rollback exclusively from the exact retained pending transaction,
    /// so either an in-process Stop or a reopened Retry can finish the same NoEffect operation.
    pub(super) fn complete_retained_account_stop_no_effect(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending = current
            .pending
            .take()
            .ok_or(DurableProviderActivationStateError::StaleState)?;
        let predecessor_kind =
            source_lifecycle_predecessor_runtime_kind(surface_id, &pending.predecessor)?;
        let exact_no_effect = AccountMarketSurface::parse(surface_id).is_some()
            && matches!(
                pending.phase,
                DurableSourceLifecyclePendingPhase::Applying
                    | DurableSourceLifecyclePendingPhase::ReconciliationRequired
            )
            && pending.transition_digest == expected_transition
            && pending.intent == DurableSourceLifecycleIntent::Stop
            && source_lifecycle_path(pending.intent, predecessor_kind)?
                == SourceLifecyclePath::NoEffectOnly
            && pending.checkpoint == DurableSourceLifecycleCheckpoint::Planned
            && pending.predecessor == pending.initial_target
            && pending.predecessor == pending.target
            && pending.expected_runtime_generation.is_none()
            && pending.shutdown_key.is_none()
            && pending.runtime_drain_evidence.is_none()
            && pending.portal_cancellation_proof_digest.is_none();
        if !exact_no_effect {
            current.pending = Some(pending);
            return Err(DurableProviderActivationStateError::StaleState);
        }
        current.settled = pending.predecessor.clone();
        current.last_completed = Some(DurableSourceLifecycleCompletedOperation {
            transition_revision: pending.transition_revision,
            operation_id: pending.operation_id,
            command_digest: pending.command_digest,
            transition_digest: pending.transition_digest,
            intent: pending.intent,
            predecessor: pending.predecessor.clone(),
            initial_target: pending.initial_target,
            target: pending.predecessor,
            expected_runtime_generation: None,
            shutdown_key: None,
            terminal_checkpoint: DurableSourceLifecycleCheckpoint::Planned,
            runtime_drain_evidence: None,
            portal_cancellation_proof_digest: None,
            outcome: DurableSourceLifecycleCompletionOutcome::NoEffect,
        });
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Converts an interrupted or indeterminate transition into an explicit recovery barrier.
    pub(super) fn require_source_lifecycle_reconciliation(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let mut current = self.source_lifecycle_record(surface_id)?;
        let pending = current
            .pending
            .as_mut()
            .ok_or(DurableProviderActivationStateError::StaleState)?;
        if pending.phase != DurableSourceLifecyclePendingPhase::Applying
            || pending.transition_digest != expected_transition
        {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        pending.phase = DurableSourceLifecyclePendingPhase::ReconciliationRequired;
        self.store_source_lifecycle(surface_id, &current)?;
        Ok(current)
    }

    /// Converts an exact just-completed lifecycle mutation into a recovery barrier when the
    /// corresponding runtime publication step fails after the durable final-state write.
    pub(super) fn require_completed_source_lifecycle_reconciliation(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        self.require_source_lifecycle_reconciliation(surface_id, expected_transition)
    }

    fn store_source_lifecycle(
        &self,
        surface_id: &str,
        record: &DurableSourceLifecycleRecord,
    ) -> Result<(), DurableProviderActivationStateError> {
        let key = lifecycle_surface_key(surface_id)?;
        let encoded = encode_source_lifecycle(surface_id, record)?;
        LocalAuthorityStateStore::try_open(self.lifecycle_root(key))?
            .store(&encoded)
            .map_err(Into::into)
    }

    pub(super) fn load_evidence(
        &self,
        sha256: &str,
        maximum_bytes: u64,
    ) -> Result<StoredActivationEvidence, DurableProviderActivationStateError> {
        let bytes = self.load_indexed_evidence(sha256, maximum_bytes)?;
        let length = u64::try_from(bytes.len())
            .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
        if length > maximum_bytes || sha256_bytes(&bytes) != sha256 {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&bytes).into());
        Ok(StoredActivationEvidence {
            bytes: bytes.into_boxed_slice(),
            digest,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "independent authority dimensions stay explicit for review and exact CAS"
    )]
    #[cfg(test)]
    pub(super) fn publish_recipe(
        &self,
        surface_id: &str,
        expected_state_digest: Option<EvidenceDigest>,
        session_id: Uuid,
        request_bytes: &[u8],
        evidence_digests: &[String],
        runtime_generation_digest: EvidenceDigest,
        predecessor_runtime_generation_digest: Option<EvidenceDigest>,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        self.publish_recipe_with_state(
            surface_id,
            expected_state_digest,
            session_id,
            request_bytes,
            evidence_digests,
            runtime_generation_digest,
            predecessor_runtime_generation_digest,
            RecipePublicationState::Desired,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "independent authority dimensions stay explicit for review and exact CAS"
    )]
    pub(super) fn publish_staged_recipe(
        &self,
        surface_id: &str,
        expected_state_digest: Option<EvidenceDigest>,
        session_id: Uuid,
        request_bytes: &[u8],
        evidence_digests: &[String],
        runtime_generation_digest: EvidenceDigest,
        predecessor_runtime_generation_digest: Option<EvidenceDigest>,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        self.publish_recipe_with_state(
            surface_id,
            expected_state_digest,
            session_id,
            request_bytes,
            evidence_digests,
            runtime_generation_digest,
            predecessor_runtime_generation_digest,
            RecipePublicationState::Staged,
        )
    }

    /// Stages a generation replacement while retaining the exact predecessor envelope.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent replacement authority dimensions stay explicit for exact CAS"
    )]
    pub(super) fn publish_staged_replacement(
        &self,
        surface_id: &str,
        predecessor: &DurableActivationRecipe,
        session_id: Uuid,
        request_bytes: &[u8],
        evidence_digests: &[String],
        runtime_generation_digest: EvidenceDigest,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        if predecessor.staged_predecessor.is_some()
            || digest_bytes(&predecessor.encoded_state) != predecessor.state_digest
        {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        let DurableActivationRecipeState::Desired(decoded_predecessor) =
            decode_recipe(surface_id, &predecessor.encoded_state, false)?
        else {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        };
        if decoded_predecessor.state_digest != predecessor.state_digest
            || decoded_predecessor.runtime_generation_digest
                != predecessor.runtime_generation_digest
        {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        let key = surface_key(surface_id)?;
        let (encoded, state_digest) = encode_recipe(
            surface_id,
            session_id,
            request_bytes,
            evidence_digests,
            runtime_generation_digest,
            Some(predecessor.runtime_generation_digest),
            RecipePublicationState::Staged,
            Some(&predecessor.encoded_state),
        )?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let current = store
            .load()?
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        if digest_bytes(&current) != predecessor.state_digest
            || current.as_slice() != predecessor.encoded_state.as_ref()
        {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        store.store(&encoded)?;
        Ok(state_digest)
    }

    /// Restores the exact predecessor retained by one unchanged staged replacement.
    pub(super) fn restore_staged_predecessor(
        &self,
        surface_id: &str,
        expected_staged_digest: EvidenceDigest,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        let DurableActivationRecipeState::Staged(staged) =
            self.load_recipe_for_lifecycle(surface_id)?
        else {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        };
        if staged.state_digest != expected_staged_digest {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let predecessor = staged
            .staged_predecessor
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        let key = surface_key(surface_id)?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let current = store
            .load()?
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        if digest_bytes(&current) != expected_staged_digest {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        store.store(&predecessor.encoded_state)?;
        Ok(predecessor.state_digest)
    }

    /// Makes exact-candidate roll-forward durable while retaining predecessor recovery evidence.
    pub(super) fn commit_staged_cutover(
        &self,
        surface_id: &str,
        expected_staged_digest: EvidenceDigest,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        let DurableActivationRecipeState::Staged(recipe) =
            self.load_recipe_for_lifecycle(surface_id)?
        else {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        };
        if recipe.state_digest != expected_staged_digest {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let predecessor = recipe
            .staged_predecessor
            .as_ref()
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        let key = surface_key(surface_id)?;
        let (encoded, cutover_digest) = encode_recipe(
            surface_id,
            recipe.session_id,
            &recipe.request_bytes,
            &recipe.evidence_digests,
            recipe.runtime_generation_digest,
            recipe.predecessor_runtime_generation_digest,
            RecipePublicationState::Cutover,
            Some(&predecessor.encoded_state),
        )?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let current = store
            .load()?
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        if digest_bytes(&current) != expected_staged_digest {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        store.store(&encoded)?;
        Ok(cutover_digest)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "independent authority dimensions stay explicit for review and exact CAS"
    )]
    fn publish_recipe_with_state(
        &self,
        surface_id: &str,
        expected_state_digest: Option<EvidenceDigest>,
        session_id: Uuid,
        request_bytes: &[u8],
        evidence_digests: &[String],
        runtime_generation_digest: EvidenceDigest,
        predecessor_runtime_generation_digest: Option<EvidenceDigest>,
        publication_state: RecipePublicationState,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        let (encoded, state_digest) = encode_recipe(
            surface_id,
            session_id,
            request_bytes,
            evidence_digests,
            runtime_generation_digest,
            predecessor_runtime_generation_digest,
            publication_state,
            None,
        )?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let current = store.load()?;
        if current.as_deref().map(digest_bytes) != expected_state_digest {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        store.store(&encoded)?;
        Ok(state_digest)
    }

    /// Promotes only the exact staged recipe to restart-desired authority.
    pub(super) fn promote_staged_recipe(
        &self,
        surface_id: &str,
        expected_staged_digest: EvidenceDigest,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        let DurableActivationRecipeState::Staged(recipe) =
            self.load_recipe_for_lifecycle(surface_id)?
        else {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        };
        if recipe.state_digest != expected_staged_digest || recipe.staged_predecessor.is_some() {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let key = surface_key(surface_id)?;
        let (encoded, desired_digest) = encode_recipe(
            surface_id,
            recipe.session_id,
            &recipe.request_bytes,
            &recipe.evidence_digests,
            recipe.runtime_generation_digest,
            recipe.predecessor_runtime_generation_digest,
            RecipePublicationState::Desired,
            None,
        )?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let current = store
            .load()?
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        if digest_bytes(&current) != expected_staged_digest {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        store.store(&encoded)?;
        Ok(desired_digest)
    }

    /// Completes only the exact durable cutover and drops no predecessor evidence before then.
    pub(super) fn complete_cutover_recipe(
        &self,
        surface_id: &str,
        expected_cutover_digest: EvidenceDigest,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        let DurableActivationRecipeState::Cutover(recipe) =
            self.load_recipe_for_lifecycle(surface_id)?
        else {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        };
        if recipe.state_digest != expected_cutover_digest || recipe.staged_predecessor.is_none() {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let key = surface_key(surface_id)?;
        let (encoded, desired_digest) = encode_recipe(
            surface_id,
            recipe.session_id,
            &recipe.request_bytes,
            &recipe.evidence_digests,
            recipe.runtime_generation_digest,
            recipe.predecessor_runtime_generation_digest,
            RecipePublicationState::Desired,
            None,
        )?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let current = store
            .load()?
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        if digest_bytes(&current) != expected_cutover_digest {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        store.store(&encoded)?;
        Ok(desired_digest)
    }

    /// Computes the exact candidate-state digest without publishing it.
    pub(super) fn recipe_digest(
        &self,
        surface_id: &str,
        session_id: Uuid,
        request_bytes: &[u8],
        evidence_digests: &[String],
        runtime_generation_digest: EvidenceDigest,
        predecessor_runtime_generation_digest: Option<EvidenceDigest>,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        surface_key(surface_id)?;
        encode_recipe(
            surface_id,
            session_id,
            request_bytes,
            evidence_digests,
            runtime_generation_digest,
            predecessor_runtime_generation_digest,
            RecipePublicationState::Desired,
            None,
        )
        .map(|(_encoded, digest)| digest)
    }

    /// Returns the digest of the exact state envelope currently occupying one surface slot.
    pub(super) fn current_state_digest(
        &self,
        surface_id: &str,
    ) -> Result<Option<EvidenceDigest>, DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        LocalAuthorityStateStore::try_open(self.recipe_root(key))?
            .load()
            .map(|encoded| encoded.as_deref().map(digest_bytes))
            .map_err(Into::into)
    }

    pub(super) fn load_recipe(
        &self,
        surface_id: &str,
    ) -> Result<DurableActivationRecipeState, DurableProviderActivationStateError> {
        if !self.restored_requirements(surface_id)?.is_empty()
            || matches!(
                self.source_lifecycle_record(surface_id)?.phase(),
                DurableSourceLifecyclePhase::Stopped
                    | DurableSourceLifecyclePhase::Removed
                    | DurableSourceLifecyclePhase::Applying
                    | DurableSourceLifecyclePhase::ReconciliationRequired
            )
        {
            return Ok(DurableActivationRecipeState::Missing);
        }
        self.load_recipe_for_lifecycle(surface_id)
    }

    /// Loads retained recipe authority even while lifecycle policy keeps it non-callable.
    pub(super) fn load_recipe_for_lifecycle(
        &self,
        surface_id: &str,
    ) -> Result<DurableActivationRecipeState, DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        let Some(encoded) = LocalAuthorityStateStore::try_open(self.recipe_root(key))?.load()?
        else {
            return Ok(DurableActivationRecipeState::Missing);
        };
        decode_activation_state(surface_id, &encoded)
    }

    /// Replaces unreadable or superseded activation state with an explicit disabled record.
    ///
    /// The original payload is retained only by digest. A new exact activation recipe supersedes
    /// this record, so recovery cannot accidentally reuse invalid authority.
    pub(super) fn quarantine_recipe(
        &self,
        surface_id: &str,
        reason: DurableActivationQuarantineReason,
    ) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let encoded = store
            .load()?
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        quarantine_encoded(&store, surface_id, reason, &encoded)
    }

    /// Quarantines only the exact candidate that produced an adapter rejection.
    ///
    /// Returns `false` without mutation when another activation has already published different
    /// state for the surface.
    pub(super) fn quarantine_recipe_if_current(
        &self,
        surface_id: &str,
        expected: EvidenceDigest,
        reason: DurableActivationQuarantineReason,
    ) -> Result<bool, DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        let store = LocalAuthorityStateStore::try_open(self.recipe_root(key))?;
        let encoded = store
            .load()?
            .ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        if digest_bytes(&encoded) != expected {
            return Ok(false);
        }
        quarantine_encoded(&store, surface_id, reason, &encoded)?;
        Ok(true)
    }

    fn recipe_root(&self, key: &str) -> PathBuf {
        self.root.join("recipes").join(key)
    }

    fn lifecycle_root(&self, key: &str) -> PathBuf {
        self.root.join("lifecycle").join(key)
    }

    fn restored_requirement_root(&self, key: &str) -> PathBuf {
        self.root.join("restored-requirements").join(key)
    }

    fn restored_requirements(
        &self,
        surface_id: &str,
    ) -> Result<Vec<ProviderMetadataRestoreRequirementKind>, DurableProviderActivationStateError>
    {
        let key = surface_key(surface_id)?;
        let Some(encoded) =
            LocalAuthorityStateStore::try_open(self.restored_requirement_root(key))?.load()?
        else {
            return Ok(Vec::new());
        };
        let wire: RestoredProviderRequirementWire = serde_json::from_slice(&encoded)
            .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
        let current_recipe = LocalAuthorityStateStore::try_open(self.recipe_root(key))?.load()?;
        if wire.schema_version != RESTORED_REQUIREMENT_SCHEMA_VERSION
            || wire.surface_id != surface_id
            || !valid_sha256(&wire.restored_state_sha256)
            || wire.requirements.is_empty()
            || wire.requirements.len() > 2
            || wire.requirements[0] != ProviderMetadataRestoreRequirementKind::Reactivation
            || wire
                .requirements
                .get(1)
                .is_some_and(|kind| *kind != ProviderMetadataRestoreRequirementKind::Reselection)
            || serde_json::to_vec(&wire)
                .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?
                != encoded
        {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        if current_recipe
            .as_deref()
            .is_none_or(|current| sha256_bytes(current) != wire.restored_state_sha256)
        {
            // A successful explicit replacement activation supersedes the restore barrier by
            // changing the exact recipe revision. No deletion race is needed.
            return Ok(Vec::new());
        }
        Ok(wire.requirements)
    }

    fn store_restored_requirements(
        &self,
        surface_id: &str,
        restored_state_sha256: String,
        requirements: Vec<ProviderMetadataRestoreRequirementKind>,
    ) -> Result<(), DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        let wire = RestoredProviderRequirementWire {
            schema_version: RESTORED_REQUIREMENT_SCHEMA_VERSION,
            surface_id: surface_id.to_owned(),
            restored_state_sha256,
            requirements,
        };
        if !valid_sha256(&wire.restored_state_sha256)
            || wire.requirements.is_empty()
            || wire.requirements.len() > 2
            || wire.requirements[0] != ProviderMetadataRestoreRequirementKind::Reactivation
            || wire
                .requirements
                .get(1)
                .is_some_and(|kind| *kind != ProviderMetadataRestoreRequirementKind::Reselection)
        {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        let encoded = serde_json::to_vec(&wire)
            .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
        LocalAuthorityStateStore::try_open(self.restored_requirement_root(key))?.store(&encoded)?;
        Ok(())
    }

    pub(super) fn referenced_evidence_digests(
        &self,
    ) -> Result<std::collections::BTreeSet<String>, DurableProviderActivationStateError> {
        let mut referenced = std::collections::BTreeSet::new();
        for surface_id in RESTORABLE_RESEARCH_SURFACES {
            match self.load_recipe_for_lifecycle(surface_id)? {
                DurableActivationRecipeState::Missing => {}
                DurableActivationRecipeState::Desired(recipe)
                | DurableActivationRecipeState::Staged(recipe)
                | DurableActivationRecipeState::Cutover(recipe) => {
                    referenced.extend(recipe.evidence_digests);
                    if let Some(predecessor) = recipe.staged_predecessor {
                        referenced.extend(predecessor.evidence_digests);
                    }
                }
                DurableActivationRecipeState::Quarantined(quarantine) => {
                    referenced.extend(quarantine.evidence_digests);
                }
            }
        }
        Ok(referenced)
    }

    fn export_provider_metadata_wire(
        &self,
        registry: &[u8],
    ) -> Result<ProviderMetadataBackupWire, ProviderMetadataBackupError> {
        let mut lifecycle_records = Vec::new();
        lifecycle_records
            .try_reserve_exact(SERIALIZED_LIFECYCLE_SURFACES.len())
            .map_err(|_| ProviderMetadataBackupError::ResourceExhausted)?;
        for surface_id in SERIALIZED_LIFECYCLE_SURFACES {
            let record = self.source_lifecycle_record(surface_id)?;
            let encoded = encode_source_lifecycle(surface_id, &record)?;
            lifecycle_records.push(ProviderMetadataStateRecordWire {
                surface_id: surface_id.to_owned(),
                encoded_state_base64: BASE64_STANDARD.encode(encoded),
            });
        }

        let mut activation_recipes = Vec::new();
        let mut restore_requirements = Vec::new();
        for surface_id in RESTORABLE_RESEARCH_SURFACES {
            let key = surface_key(surface_id)?;
            let Some(encoded) =
                LocalAuthorityStateStore::try_open(self.recipe_root(key))?.load()?
            else {
                continue;
            };
            let (encoded, requires_reselection) =
                backup_safe_activation_state(surface_id, &encoded)?;
            activation_recipes.push(ProviderMetadataStateRecordWire {
                surface_id: surface_id.to_owned(),
                encoded_state_base64: BASE64_STANDARD.encode(encoded),
            });
            restore_requirements.push(ProviderMetadataRestoreRequirement {
                surface_id: surface_id.to_owned(),
                kind: ProviderMetadataRestoreRequirementKind::Reactivation,
            });
            if requires_reselection {
                restore_requirements.push(ProviderMetadataRestoreRequirement {
                    surface_id: surface_id.to_owned(),
                    kind: ProviderMetadataRestoreRequirementKind::Reselection,
                });
            }
        }

        let referenced = self.referenced_evidence_digests()?;
        let mut evidence_objects = Vec::new();
        evidence_objects
            .try_reserve_exact(referenced.len())
            .map_err(|_| ProviderMetadataBackupError::ResourceExhausted)?;
        for sha256 in referenced {
            let evidence = self.load_evidence(&sha256, MAXIMUM_BACKUP_EVIDENCE_OBJECT_BYTES)?;
            evidence_objects.push(ProviderMetadataEvidenceWire {
                sha256,
                bytes_base64: BASE64_STANDARD.encode(evidence.as_bytes()),
            });
        }

        Ok(ProviderMetadataBackupWire {
            schema: PROVIDER_METADATA_BACKUP_SCHEMA.to_owned(),
            schema_version: PROVIDER_METADATA_BACKUP_SCHEMA_VERSION,
            lifecycle_records,
            activation_recipes,
            evidence_objects,
            registry_clean_restart_base64: BASE64_STANDARD.encode(registry),
            restore_requirements,
        })
    }

    fn restore_provider_metadata_fresh(
        &self,
        validated: ValidatedProviderMetadataRestore,
        registry_store: LocalAuthorityStateStore,
    ) -> Result<Vec<ProviderMetadataRestoreRequirement>, ProviderMetadataBackupError> {
        for surface_id in SERIALIZED_LIFECYCLE_SURFACES {
            let key = lifecycle_surface_key(surface_id)?;
            if LocalAuthorityStateStore::try_open(self.lifecycle_root(key))?
                .load()?
                .is_some()
            {
                return Err(ProviderMetadataBackupError::RestoreTargetNotFresh);
            }
        }
        for surface_id in RESTORABLE_RESEARCH_SURFACES {
            let key = surface_key(surface_id)?;
            if LocalAuthorityStateStore::try_open(self.recipe_root(key))?
                .load()?
                .is_some()
                || LocalAuthorityStateStore::try_open(self.restored_requirement_root(key))?
                    .load()?
                    .is_some()
            {
                return Err(ProviderMetadataBackupError::RestoreTargetNotFresh);
            }
        }
        if !self.evidence_backup_target_is_absent()? {
            return Err(ProviderMetadataBackupError::RestoreTargetNotFresh);
        }
        if registry_store.load()?.is_some() {
            return Err(ProviderMetadataBackupError::RestoreTargetNotFresh);
        }

        AuthoritativeSourceRegistry::restore_clean_restart_backup_fresh(
            registry_store,
            &validated.registry,
        )?;
        for (surface_id, encoded) in &validated.lifecycle_records {
            let key = lifecycle_surface_key(surface_id)?;
            LocalAuthorityStateStore::try_open(self.lifecycle_root(key))?.store(encoded)?;
        }
        for (surface_id, encoded) in &validated.activation_recipes {
            let key = surface_key(surface_id)?;
            LocalAuthorityStateStore::try_open(self.recipe_root(key))?.store(encoded)?;
            let requirements = validated
                .requirements
                .iter()
                .filter(|requirement| requirement.surface_id == *surface_id)
                .map(|requirement| requirement.kind)
                .collect();
            self.store_restored_requirements(surface_id, sha256_bytes(encoded), requirements)?;
        }
        let candidates = validated
            .evidence_objects
            .iter()
            .map(|(sha256, bytes)| ActivationEvidenceCandidate { sha256, bytes })
            .collect::<Vec<_>>();
        self.persist_evidence_bundle(&candidates)?;
        self.reconcile_evidence_objects()?;
        Ok(validated.requirements)
    }
}

fn validate_provider_metadata_backup(
    bytes: &[u8],
) -> Result<ValidatedProviderMetadataRestore, ProviderMetadataBackupError> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_PROVIDER_METADATA_BACKUP_BYTES {
        return Err(ProviderMetadataBackupError::ResourceExhausted);
    }
    let wire: ProviderMetadataBackupWire =
        serde_json::from_slice(bytes).map_err(|_| ProviderMetadataBackupError::Invalid)?;
    if wire.schema != PROVIDER_METADATA_BACKUP_SCHEMA
        || serde_json::to_vec(&wire).map_err(|_| ProviderMetadataBackupError::Invalid)? != bytes
    {
        return Err(ProviderMetadataBackupError::Invalid);
    }
    if wire.schema_version != PROVIDER_METADATA_BACKUP_SCHEMA_VERSION
        || wire.lifecycle_records.len() != SERIALIZED_LIFECYCLE_SURFACES.len()
    {
        return Err(ProviderMetadataBackupError::Invalid);
    }

    let mut lifecycle_records = Vec::new();
    lifecycle_records
        .try_reserve_exact(SERIALIZED_LIFECYCLE_SURFACES.len())
        .map_err(|_| ProviderMetadataBackupError::ResourceExhausted)?;
    for (encoded, expected_surface) in wire
        .lifecycle_records
        .into_iter()
        .zip(SERIALIZED_LIFECYCLE_SURFACES)
    {
        if encoded.surface_id != expected_surface {
            return Err(ProviderMetadataBackupError::Invalid);
        }
        let bytes = decode_canonical_base64(&encoded.encoded_state_base64)?;
        let decoded = decode_source_lifecycle(expected_surface, &bytes)?;
        if encode_source_lifecycle(expected_surface, &decoded)? != bytes {
            return Err(ProviderMetadataBackupError::Invalid);
        }
        lifecycle_records.push((encoded.surface_id, bytes));
    }
    let mut activation_recipes = Vec::new();
    let mut referenced = std::collections::BTreeSet::new();
    let mut expected_requirements = Vec::new();
    let mut previous_position = None;
    for encoded in wire.activation_recipes {
        let position = RESTORABLE_RESEARCH_SURFACES
            .iter()
            .position(|surface| *surface == encoded.surface_id)
            .ok_or(ProviderMetadataBackupError::Invalid)?;
        if previous_position.is_some_and(|previous| position <= previous) {
            return Err(ProviderMetadataBackupError::Invalid);
        }
        previous_position = Some(position);
        let bytes = decode_canonical_base64(&encoded.encoded_state_base64)?;
        let requires_reselection = activation_state_contains_path_field(&bytes)?;
        match decode_activation_state(&encoded.surface_id, &bytes)? {
            DurableActivationRecipeState::Missing => {
                return Err(ProviderMetadataBackupError::Invalid);
            }
            DurableActivationRecipeState::Desired(recipe)
            | DurableActivationRecipeState::Staged(recipe)
            | DurableActivationRecipeState::Cutover(recipe) => {
                referenced.extend(recipe.evidence_digests);
                if let Some(predecessor) = recipe.staged_predecessor {
                    referenced.extend(predecessor.evidence_digests);
                }
            }
            DurableActivationRecipeState::Quarantined(quarantine) => {
                referenced.extend(quarantine.evidence_digests);
            }
        }
        expected_requirements.push(ProviderMetadataRestoreRequirement {
            surface_id: encoded.surface_id.clone(),
            kind: ProviderMetadataRestoreRequirementKind::Reactivation,
        });
        if requires_reselection {
            expected_requirements.push(ProviderMetadataRestoreRequirement {
                surface_id: encoded.surface_id.clone(),
                kind: ProviderMetadataRestoreRequirementKind::Reselection,
            });
        }
        activation_recipes.push((encoded.surface_id, bytes));
    }

    if wire.restore_requirements != expected_requirements {
        return Err(ProviderMetadataBackupError::Invalid);
    }

    let mut evidence_objects = Vec::new();
    evidence_objects
        .try_reserve_exact(wire.evidence_objects.len())
        .map_err(|_| ProviderMetadataBackupError::ResourceExhausted)?;
    let mut observed = std::collections::BTreeSet::new();
    for evidence in wire.evidence_objects {
        validate_sha256(&evidence.sha256)?;
        if !observed.insert(evidence.sha256.clone()) {
            return Err(ProviderMetadataBackupError::Invalid);
        }
        let bytes = decode_canonical_base64(&evidence.bytes_base64)?;
        if bytes.is_empty()
            || u64::try_from(bytes.len())
                .map_or(true, |length| length > MAXIMUM_BACKUP_EVIDENCE_OBJECT_BYTES)
            || sha256_bytes(&bytes) != evidence.sha256
        {
            return Err(ProviderMetadataBackupError::Invalid);
        }
        evidence_objects.push((evidence.sha256, bytes));
    }
    if observed != referenced {
        return Err(ProviderMetadataBackupError::Invalid);
    }

    let registry_bytes = decode_canonical_base64(&wire.registry_clean_restart_base64)?;
    let registry =
        AuthoritativeSourceRegistry::validate_clean_restart_backup_bytes(&registry_bytes)?;
    Ok(ValidatedProviderMetadataRestore {
        lifecycle_records,
        activation_recipes,
        evidence_objects,
        registry,
        requirements: wire.restore_requirements,
    })
}

fn decode_canonical_base64(value: &str) -> Result<Vec<u8>, ProviderMetadataBackupError> {
    let bytes = BASE64_STANDARD
        .decode(value)
        .map_err(|_| ProviderMetadataBackupError::Invalid)?;
    if BASE64_STANDARD.encode(&bytes) != value {
        return Err(ProviderMetadataBackupError::Invalid);
    }
    Ok(bytes)
}

fn encode_source_lifecycle(
    surface_id: &str,
    record: &DurableSourceLifecycleRecord,
) -> Result<Vec<u8>, DurableProviderActivationStateError> {
    validate_source_lifecycle_record(surface_id, record)?;
    let wire = SourceLifecycleWire {
        schema_version: SOURCE_LIFECYCLE_SCHEMA_VERSION,
        surface_id: surface_id.to_owned(),
        revision: record.revision.get(),
        settled: encode_source_lifecycle_snapshot(&record.settled),
        last_completed: record
            .last_completed
            .as_ref()
            .map(encode_completed_source_lifecycle_operation),
        pending: record
            .pending
            .as_ref()
            .map(encode_pending_source_lifecycle_transaction),
    };
    serde_json::to_vec(&wire).map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)
}

fn decode_source_lifecycle(
    surface_id: &str,
    encoded: &[u8],
) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
    let wire: SourceLifecycleWire = serde_json::from_slice(encoded)
        .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?;
    if wire.schema_version != SOURCE_LIFECYCLE_SCHEMA_VERSION
        || wire.surface_id != surface_id
        || serde_json::to_vec(&wire)
            .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?
            != encoded
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    let revision = NonZeroU64::new(wire.revision)
        .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
    let record = DurableSourceLifecycleRecord {
        revision,
        settled: decode_source_lifecycle_snapshot(wire.settled)?,
        last_completed: wire
            .last_completed
            .map(decode_completed_source_lifecycle_operation)
            .transpose()?,
        pending: wire
            .pending
            .map(decode_pending_source_lifecycle_transaction)
            .transpose()?,
    };
    validate_source_lifecycle_record(surface_id, &record)?;
    Ok(record)
}

fn source_lifecycle_transition_digest(
    surface_id: &str,
    revision: NonZeroU64,
    operation_id: &SourceIdentifier,
    command_digest: EvidenceDigest,
    intent: DurableSourceLifecycleIntent,
    predecessor: &DurableSourceLifecycleSnapshot,
    target: &DurableSourceLifecycleSnapshot,
    expected_runtime_generation: Option<DurableSourceRuntimeGeneration>,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    require_sha256(command_digest)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-transition/v3\0");
    hash_field(&mut hasher, surface_id.as_bytes())?;
    hasher.update(revision.get().to_be_bytes());
    hash_field(&mut hasher, operation_id.as_str().as_bytes())?;
    hash_evidence(&mut hasher, command_digest)?;
    hasher.update([source_lifecycle_intent_tag(intent)]);
    hash_source_lifecycle_snapshot(&mut hasher, predecessor)?;
    hash_source_lifecycle_snapshot(&mut hasher, target)?;
    hash_optional_runtime_generation(&mut hasher, expected_runtime_generation)?;
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

fn encode_source_lifecycle_snapshot(
    snapshot: &DurableSourceLifecycleSnapshot,
) -> SourceLifecycleSnapshotWire {
    SourceLifecycleSnapshotWire {
        phase: snapshot.phase,
        session_id: snapshot.session_id,
        public_configuration_sha256: snapshot
            .public_configuration_digest
            .map(|digest| lower_hex(&digest.bytes())),
        runtime_verification_receipt_sha256: snapshot
            .runtime_verification_receipt_digest
            .map(|digest| lower_hex(&digest.bytes())),
        credential_generation: snapshot.credential_generation.map(SecretGeneration::get),
        runtime_generation: snapshot
            .runtime_generation
            .map(encode_source_runtime_generation),
    }
}

fn decode_source_lifecycle_snapshot(
    wire: SourceLifecycleSnapshotWire,
) -> Result<DurableSourceLifecycleSnapshot, DurableProviderActivationStateError> {
    Ok(DurableSourceLifecycleSnapshot {
        phase: wire.phase,
        session_id: wire.session_id,
        public_configuration_digest: wire
            .public_configuration_sha256
            .as_deref()
            .map(lifecycle_digest_from_lower_hex)
            .transpose()?,
        runtime_verification_receipt_digest: wire
            .runtime_verification_receipt_sha256
            .as_deref()
            .map(lifecycle_digest_from_lower_hex)
            .transpose()?,
        credential_generation: wire
            .credential_generation
            .map(SecretGeneration::new)
            .transpose()
            .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?,
        runtime_generation: wire
            .runtime_generation
            .map(decode_source_runtime_generation)
            .transpose()?,
    })
}

fn encode_source_runtime_generation(
    generation: DurableSourceRuntimeGeneration,
) -> SourceRuntimeGenerationWire {
    match generation {
        DurableSourceRuntimeGeneration::Scalar(generation) => SourceRuntimeGenerationWire::Scalar {
            generation: generation.get(),
        },
        DurableSourceRuntimeGeneration::NonAccountDigest(digest) => {
            SourceRuntimeGenerationWire::NonAccountDigest {
                sha256: lower_hex(&digest.bytes()),
            }
        }
        DurableSourceRuntimeGeneration::AccountGroup(digest) => {
            SourceRuntimeGenerationWire::AccountGroup {
                sha256: lower_hex(&digest.bytes()),
            }
        }
    }
}

fn decode_source_runtime_generation(
    wire: SourceRuntimeGenerationWire,
) -> Result<DurableSourceRuntimeGeneration, DurableProviderActivationStateError> {
    match wire {
        SourceRuntimeGenerationWire::Scalar { generation } => NonZeroU64::new(generation)
            .map(DurableSourceRuntimeGeneration::Scalar)
            .ok_or(DurableProviderActivationStateError::InvalidLifecycle),
        SourceRuntimeGenerationWire::NonAccountDigest { sha256 } => {
            lifecycle_digest_from_lower_hex(&sha256)
                .map(DurableSourceRuntimeGeneration::NonAccountDigest)
        }
        SourceRuntimeGenerationWire::AccountGroup { sha256 } => {
            lifecycle_digest_from_lower_hex(&sha256)
                .map(DurableSourceRuntimeGeneration::AccountGroup)
        }
    }
}

fn encode_completed_source_lifecycle_operation(
    completed: &DurableSourceLifecycleCompletedOperation,
) -> SourceLifecycleCompletedOperationWire {
    SourceLifecycleCompletedOperationWire {
        transition_revision: completed.transition_revision.get(),
        operation_id: completed.operation_id.as_str().to_owned(),
        command_sha256: lower_hex(&completed.command_digest.bytes()),
        transition_sha256: lower_hex(&completed.transition_digest.bytes()),
        intent: completed.intent,
        predecessor: encode_source_lifecycle_snapshot(&completed.predecessor),
        initial_target: encode_source_lifecycle_snapshot(&completed.initial_target),
        target: encode_source_lifecycle_snapshot(&completed.target),
        expected_runtime_generation: completed
            .expected_runtime_generation
            .map(encode_source_runtime_generation),
        shutdown_key: completed.shutdown_key.map(encode_account_shutdown_key),
        terminal_checkpoint: completed.terminal_checkpoint,
        runtime_drain_evidence: completed
            .runtime_drain_evidence
            .map(encode_source_lifecycle_runtime_drain_evidence),
        portal_cancellation_proof_sha256: completed
            .portal_cancellation_proof_digest
            .map(|digest| lower_hex(&digest.bytes())),
        outcome: completed.outcome,
    }
}

fn decode_completed_source_lifecycle_operation(
    wire: SourceLifecycleCompletedOperationWire,
) -> Result<DurableSourceLifecycleCompletedOperation, DurableProviderActivationStateError> {
    Ok(DurableSourceLifecycleCompletedOperation {
        transition_revision: NonZeroU64::new(wire.transition_revision)
            .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?,
        operation_id: SourceIdentifier::try_from(wire.operation_id)
            .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?,
        command_digest: lifecycle_digest_from_lower_hex(&wire.command_sha256)?,
        transition_digest: lifecycle_digest_from_lower_hex(&wire.transition_sha256)?,
        intent: wire.intent,
        predecessor: decode_source_lifecycle_snapshot(wire.predecessor)?,
        initial_target: decode_source_lifecycle_snapshot(wire.initial_target)?,
        target: decode_source_lifecycle_snapshot(wire.target)?,
        expected_runtime_generation: wire
            .expected_runtime_generation
            .map(decode_source_runtime_generation)
            .transpose()?,
        shutdown_key: wire
            .shutdown_key
            .map(decode_account_shutdown_key)
            .transpose()?,
        terminal_checkpoint: wire.terminal_checkpoint,
        runtime_drain_evidence: wire
            .runtime_drain_evidence
            .map(decode_source_lifecycle_runtime_drain_evidence)
            .transpose()?,
        portal_cancellation_proof_digest: wire
            .portal_cancellation_proof_sha256
            .as_deref()
            .map(lifecycle_digest_from_lower_hex)
            .transpose()?,
        outcome: wire.outcome,
    })
}

fn encode_pending_source_lifecycle_transaction(
    pending: &DurableSourceLifecyclePendingTransaction,
) -> SourceLifecyclePendingTransactionWire {
    SourceLifecyclePendingTransactionWire {
        transition_revision: pending.transition_revision.get(),
        operation_id: pending.operation_id.as_str().to_owned(),
        command_sha256: lower_hex(&pending.command_digest.bytes()),
        transition_sha256: lower_hex(&pending.transition_digest.bytes()),
        intent: pending.intent,
        predecessor: encode_source_lifecycle_snapshot(&pending.predecessor),
        initial_target: encode_source_lifecycle_snapshot(&pending.initial_target),
        target: encode_source_lifecycle_snapshot(&pending.target),
        expected_runtime_generation: pending
            .expected_runtime_generation
            .map(encode_source_runtime_generation),
        shutdown_key: pending.shutdown_key.map(encode_account_shutdown_key),
        phase: pending.phase,
        checkpoint: pending.checkpoint,
        runtime_drain_evidence: pending
            .runtime_drain_evidence
            .map(encode_source_lifecycle_runtime_drain_evidence),
        portal_cancellation_proof_sha256: pending
            .portal_cancellation_proof_digest
            .map(|digest| lower_hex(&digest.bytes())),
    }
}

fn decode_pending_source_lifecycle_transaction(
    wire: SourceLifecyclePendingTransactionWire,
) -> Result<DurableSourceLifecyclePendingTransaction, DurableProviderActivationStateError> {
    Ok(DurableSourceLifecyclePendingTransaction {
        transition_revision: NonZeroU64::new(wire.transition_revision)
            .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?,
        operation_id: SourceIdentifier::try_from(wire.operation_id)
            .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?,
        command_digest: lifecycle_digest_from_lower_hex(&wire.command_sha256)?,
        transition_digest: lifecycle_digest_from_lower_hex(&wire.transition_sha256)?,
        intent: wire.intent,
        predecessor: decode_source_lifecycle_snapshot(wire.predecessor)?,
        initial_target: decode_source_lifecycle_snapshot(wire.initial_target)?,
        target: decode_source_lifecycle_snapshot(wire.target)?,
        expected_runtime_generation: wire
            .expected_runtime_generation
            .map(decode_source_runtime_generation)
            .transpose()?,
        shutdown_key: wire
            .shutdown_key
            .map(decode_account_shutdown_key)
            .transpose()?,
        phase: wire.phase,
        checkpoint: wire.checkpoint,
        runtime_drain_evidence: wire
            .runtime_drain_evidence
            .map(decode_source_lifecycle_runtime_drain_evidence)
            .transpose()?,
        portal_cancellation_proof_digest: wire
            .portal_cancellation_proof_sha256
            .as_deref()
            .map(lifecycle_digest_from_lower_hex)
            .transpose()?,
    })
}

fn encode_source_lifecycle_runtime_drain_evidence(
    evidence: DurableSourceLifecycleRuntimeDrainEvidence,
) -> SourceLifecycleRuntimeDrainEvidenceWire {
    match evidence {
        DurableSourceLifecycleRuntimeDrainEvidence::AccountGroupCompleted {
            phase,
            proof_digest,
        } => SourceLifecycleRuntimeDrainEvidenceWire::AccountGroupCompleted {
            phase,
            proof_sha256: lower_hex(&proof_digest.bytes()),
        },
        DurableSourceLifecycleRuntimeDrainEvidence::NonAccountDrained {
            phase,
            proof_digest,
        } => SourceLifecycleRuntimeDrainEvidenceWire::NonAccountDrained {
            phase,
            proof_sha256: lower_hex(&proof_digest.bytes()),
        },
        DurableSourceLifecycleRuntimeDrainEvidence::RuntimeProvenAbsent {
            phase,
            proof_digest,
        } => SourceLifecycleRuntimeDrainEvidenceWire::RuntimeProvenAbsent {
            phase,
            proof_sha256: lower_hex(&proof_digest.bytes()),
        },
    }
}

fn decode_source_lifecycle_runtime_drain_evidence(
    wire: SourceLifecycleRuntimeDrainEvidenceWire,
) -> Result<DurableSourceLifecycleRuntimeDrainEvidence, DurableProviderActivationStateError> {
    match wire {
        SourceLifecycleRuntimeDrainEvidenceWire::AccountGroupCompleted {
            phase,
            proof_sha256,
        } => Ok(
            DurableSourceLifecycleRuntimeDrainEvidence::AccountGroupCompleted {
                phase,
                proof_digest: lifecycle_digest_from_lower_hex(&proof_sha256)?,
            },
        ),
        SourceLifecycleRuntimeDrainEvidenceWire::NonAccountDrained {
            phase,
            proof_sha256,
        } => Ok(
            DurableSourceLifecycleRuntimeDrainEvidence::NonAccountDrained {
                phase,
                proof_digest: lifecycle_digest_from_lower_hex(&proof_sha256)?,
            },
        ),
        SourceLifecycleRuntimeDrainEvidenceWire::RuntimeProvenAbsent {
            phase,
            proof_sha256,
        } => Ok(
            DurableSourceLifecycleRuntimeDrainEvidence::RuntimeProvenAbsent {
                phase,
                proof_digest: lifecycle_digest_from_lower_hex(&proof_sha256)?,
            },
        ),
    }
}

fn encode_account_shutdown_key(key: DurableAccountShutdownKey) -> AccountShutdownKeyWire {
    AccountShutdownKeyWire {
        registry_incarnation: key.registry_incarnation,
        surface_id: key.surface_id.surface_id().to_owned(),
        onboarding_session_id: key.onboarding_session_id,
        public_configuration_sha256: lower_hex(&key.public_configuration_digest.bytes()),
        runtime_verification_receipt_sha256: lower_hex(
            &key.runtime_verification_receipt_digest.bytes(),
        ),
        credential_generation: key.credential_generation.get(),
        group_generation_sha256: lower_hex(&key.group_generation.bytes()),
        history_claim: match key.history_claim {
            DurableAccountHistoryClaim::AlpacaNeverClaimed => {
                AccountHistoryClaimWire::AlpacaNeverClaimed
            }
            DurableAccountHistoryClaim::Alpaca(parent) => AccountHistoryClaimWire::Alpaca {
                parent_group_generation_sha256: lower_hex(&parent.group_generation.bytes()),
                parent_binding_sha256: lower_hex(&parent.binding_digest.bytes()),
            },
            DurableAccountHistoryClaim::NeverApplicable => AccountHistoryClaimWire::NeverApplicable,
        },
    }
}

fn decode_account_shutdown_key(
    wire: AccountShutdownKeyWire,
) -> Result<DurableAccountShutdownKey, DurableProviderActivationStateError> {
    let surface_id = AccountMarketSurface::parse(&wire.surface_id)
        .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
    let history_claim = match wire.history_claim {
        AccountHistoryClaimWire::AlpacaNeverClaimed => {
            DurableAccountHistoryClaim::AlpacaNeverClaimed
        }
        AccountHistoryClaimWire::Alpaca {
            parent_group_generation_sha256,
            parent_binding_sha256,
        } => DurableAccountHistoryClaim::Alpaca(DurableAlpacaHistoricalParent::try_new(
            lifecycle_digest_from_lower_hex(&parent_group_generation_sha256)?,
            lifecycle_digest_from_lower_hex(&parent_binding_sha256)?,
        )?),
        AccountHistoryClaimWire::NeverApplicable => DurableAccountHistoryClaim::NeverApplicable,
    };
    DurableAccountShutdownKey::try_new(
        wire.registry_incarnation,
        surface_id,
        wire.onboarding_session_id,
        lifecycle_digest_from_lower_hex(&wire.public_configuration_sha256)?,
        lifecycle_digest_from_lower_hex(&wire.runtime_verification_receipt_sha256)?,
        SecretGeneration::new(wire.credential_generation)
            .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?,
        lifecycle_digest_from_lower_hex(&wire.group_generation_sha256)?,
        history_claim,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceLifecyclePredecessorRuntimeKind {
    StoppedRuntimeAbsent,
    RemovedRuntimeAbsent,
    DesiredActiveRuntimeAbsent,
    NonAccount,
    AccountGroup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceLifecyclePath {
    NoEffectOnly,
    Verify,
    StoppedVerification,
    AbsentStart,
    DesiredActiveRestore,
    RuntimeSuccessor,
    AccountSuccessor,
    RuntimeTerminal,
    AccountTerminal,
    RuntimeRemove,
    AccountRemove,
}

struct SourceLifecycleTransactionAudit<'a> {
    transition_revision: NonZeroU64,
    operation_id: &'a SourceIdentifier,
    command_digest: EvidenceDigest,
    transition_digest: EvidenceDigest,
    intent: DurableSourceLifecycleIntent,
    predecessor: &'a DurableSourceLifecycleSnapshot,
    initial_target: &'a DurableSourceLifecycleSnapshot,
    target: &'a DurableSourceLifecycleSnapshot,
    expected_runtime_generation: Option<DurableSourceRuntimeGeneration>,
    shutdown_key: Option<DurableAccountShutdownKey>,
    checkpoint: DurableSourceLifecycleCheckpoint,
    runtime_drain_evidence: Option<DurableSourceLifecycleRuntimeDrainEvidence>,
    portal_cancellation_proof_digest: Option<EvidenceDigest>,
    outcome: Option<DurableSourceLifecycleCompletionOutcome>,
}

impl<'a> SourceLifecycleTransactionAudit<'a> {
    fn pending(pending: &'a DurableSourceLifecyclePendingTransaction) -> Self {
        Self {
            transition_revision: pending.transition_revision,
            operation_id: &pending.operation_id,
            command_digest: pending.command_digest,
            transition_digest: pending.transition_digest,
            intent: pending.intent,
            predecessor: &pending.predecessor,
            initial_target: &pending.initial_target,
            target: &pending.target,
            expected_runtime_generation: pending.expected_runtime_generation,
            shutdown_key: pending.shutdown_key,
            checkpoint: pending.checkpoint,
            runtime_drain_evidence: pending.runtime_drain_evidence,
            portal_cancellation_proof_digest: pending.portal_cancellation_proof_digest,
            outcome: None,
        }
    }

    fn completed(completed: &'a DurableSourceLifecycleCompletedOperation) -> Self {
        Self {
            transition_revision: completed.transition_revision,
            operation_id: &completed.operation_id,
            command_digest: completed.command_digest,
            transition_digest: completed.transition_digest,
            intent: completed.intent,
            predecessor: &completed.predecessor,
            initial_target: &completed.initial_target,
            target: &completed.target,
            expected_runtime_generation: completed.expected_runtime_generation,
            shutdown_key: completed.shutdown_key,
            checkpoint: completed.terminal_checkpoint,
            runtime_drain_evidence: completed.runtime_drain_evidence,
            portal_cancellation_proof_digest: completed.portal_cancellation_proof_digest,
            outcome: Some(completed.outcome),
        }
    }
}

fn validate_source_lifecycle_record(
    surface_id: &str,
    record: &DurableSourceLifecycleRecord,
) -> Result<(), DurableProviderActivationStateError> {
    lifecycle_surface_key(surface_id)?;
    validate_settled_source_lifecycle_snapshot(surface_id, &record.settled)?;
    if let Some(completed) = &record.last_completed {
        validate_source_lifecycle_transaction(
            surface_id,
            &SourceLifecycleTransactionAudit::completed(completed),
        )?;
        if completed.target != record.settled
            || completed.transition_revision > record.revision
            || record.pending.is_none() && completed.transition_revision != record.revision
        {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
    }
    if let Some(pending) = &record.pending {
        if pending.transition_revision != record.revision
            || pending.predecessor != record.settled
            || record.last_completed.as_ref().is_some_and(|completed| {
                completed.transition_revision >= pending.transition_revision
            })
        {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        validate_source_lifecycle_transaction(
            surface_id,
            &SourceLifecycleTransactionAudit::pending(pending),
        )?;
    }
    Ok(())
}

fn validate_source_lifecycle_snapshot(
    surface_id: &str,
    snapshot: &DurableSourceLifecycleSnapshot,
) -> Result<(), DurableProviderActivationStateError> {
    if matches!(
        snapshot.phase,
        DurableSourceLifecyclePhase::Applying | DurableSourceLifecyclePhase::ReconciliationRequired
    ) || snapshot
        .public_configuration_digest
        .is_some_and(|digest| require_sha256(digest).is_err())
        || snapshot
            .runtime_verification_receipt_digest
            .is_some_and(|digest| require_sha256(digest).is_err())
        || snapshot.public_configuration_digest.is_some() && snapshot.session_id.is_none()
        || snapshot.runtime_verification_receipt_digest.is_some()
            != snapshot.credential_generation.is_some()
        || snapshot.runtime_verification_receipt_digest.is_some()
            && (snapshot.session_id.is_none() || snapshot.public_configuration_digest.is_none())
        || snapshot.phase == DurableSourceLifecyclePhase::Removed
            && (snapshot.session_id.is_some()
                || snapshot.public_configuration_digest.is_some()
                || snapshot.runtime_verification_receipt_digest.is_some()
                || snapshot.credential_generation.is_some()
                || snapshot.runtime_generation.is_some())
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    validate_runtime_generation(surface_id, snapshot.runtime_generation)?;
    if snapshot.runtime_generation.is_some()
        && snapshot.phase != DurableSourceLifecyclePhase::Active
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    if matches!(
        snapshot.runtime_generation,
        Some(DurableSourceRuntimeGeneration::AccountGroup(_))
    ) && (snapshot.session_id.is_none()
        || snapshot.public_configuration_digest.is_none()
        || snapshot.runtime_verification_receipt_digest.is_none()
        || snapshot.credential_generation.is_none())
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(())
}

fn validate_settled_source_lifecycle_snapshot(
    surface_id: &str,
    snapshot: &DurableSourceLifecycleSnapshot,
) -> Result<(), DurableProviderActivationStateError> {
    validate_source_lifecycle_snapshot(surface_id, snapshot)?;
    if snapshot.phase == DurableSourceLifecyclePhase::Active
        && snapshot.runtime_generation.is_none()
        && (snapshot.session_id.is_none()
            || snapshot.public_configuration_digest.is_none()
            || AccountMarketSurface::parse(surface_id).is_some()
                && (snapshot.runtime_verification_receipt_digest.is_none()
                    || snapshot.credential_generation.is_none()))
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(())
}

fn validate_runtime_generation(
    surface_id: &str,
    generation: Option<DurableSourceRuntimeGeneration>,
) -> Result<(), DurableProviderActivationStateError> {
    match generation {
        Some(DurableSourceRuntimeGeneration::Scalar(generation)) => {
            if generation.get() == 0 || AccountMarketSurface::parse(surface_id).is_some() {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
        Some(DurableSourceRuntimeGeneration::NonAccountDigest(digest)) => {
            require_sha256(digest)?;
            if AccountMarketSurface::parse(surface_id).is_some() {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
        Some(DurableSourceRuntimeGeneration::AccountGroup(digest)) => {
            require_sha256(digest)?;
            if AccountMarketSurface::parse(surface_id).is_none() {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
        None => {}
    }
    Ok(())
}

fn validate_source_lifecycle_transaction(
    surface_id: &str,
    audit: &SourceLifecycleTransactionAudit<'_>,
) -> Result<(), DurableProviderActivationStateError> {
    require_sha256(audit.command_digest)?;
    require_sha256(audit.transition_digest)?;
    validate_settled_source_lifecycle_snapshot(surface_id, audit.predecessor)?;
    validate_source_lifecycle_snapshot(surface_id, audit.initial_target)?;
    validate_source_lifecycle_snapshot(surface_id, audit.target)?;
    validate_runtime_generation(surface_id, audit.expected_runtime_generation)?;
    if source_lifecycle_transition_digest(
        surface_id,
        audit.transition_revision,
        audit.operation_id,
        audit.command_digest,
        audit.intent,
        audit.predecessor,
        audit.initial_target,
        audit.expected_runtime_generation,
    )? != audit.transition_digest
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }

    let predecessor_kind = validate_source_lifecycle_intent_binding(surface_id, audit)?;
    let path = source_lifecycle_path(audit.intent, predecessor_kind)?;
    let stage = source_lifecycle_checkpoint_stage(
        path,
        audit.predecessor.session_id.is_some(),
        audit.checkpoint,
    )
    .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
    validate_source_lifecycle_target_progress(surface_id, audit, path, stage)?;
    validate_source_lifecycle_effect_evidence(surface_id, audit, path, stage)?;

    match audit.outcome {
        None => {}
        Some(DurableSourceLifecycleCompletionOutcome::Applied) => {
            let final_checkpoint = source_lifecycle_final_checkpoint(path)
                .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
            if audit.checkpoint != final_checkpoint {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
        Some(DurableSourceLifecycleCompletionOutcome::NoEffect) => {
            if audit.checkpoint != DurableSourceLifecycleCheckpoint::Planned
                || audit.target != audit.predecessor
                || audit.shutdown_key.is_some()
                || audit.runtime_drain_evidence.is_some()
                || audit.portal_cancellation_proof_digest.is_some()
            {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
    }
    Ok(())
}

fn validate_source_lifecycle_intent_binding(
    surface_id: &str,
    audit: &SourceLifecycleTransactionAudit<'_>,
) -> Result<SourceLifecyclePredecessorRuntimeKind, DurableProviderActivationStateError> {
    let predecessor_kind =
        source_lifecycle_predecessor_runtime_kind(surface_id, audit.predecessor)?;
    let exact_expected = audit.predecessor.runtime_generation;
    let stopped_target = DurableSourceLifecycleSnapshot {
        phase: DurableSourceLifecyclePhase::Stopped,
        session_id: audit.predecessor.session_id,
        public_configuration_digest: audit.predecessor.public_configuration_digest,
        runtime_verification_receipt_digest: audit.predecessor.runtime_verification_receipt_digest,
        credential_generation: audit.predecessor.credential_generation,
        runtime_generation: None,
    };
    let removed_target = DurableSourceLifecycleSnapshot {
        phase: DurableSourceLifecyclePhase::Removed,
        session_id: None,
        public_configuration_digest: None,
        runtime_verification_receipt_digest: None,
        credential_generation: None,
        runtime_generation: None,
    };
    let active_replacement_target = DurableSourceLifecycleSnapshot {
        phase: DurableSourceLifecyclePhase::Active,
        session_id: audit.predecessor.session_id,
        public_configuration_digest: audit.predecessor.public_configuration_digest,
        runtime_verification_receipt_digest: audit.predecessor.runtime_verification_receipt_digest,
        credential_generation: audit.predecessor.credential_generation,
        runtime_generation: None,
    };

    let valid = match audit.intent {
        DurableSourceLifecycleIntent::Start => {
            matches!(
                predecessor_kind,
                SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent
                    | SourceLifecyclePredecessorRuntimeKind::RemovedRuntimeAbsent
            ) && audit.expected_runtime_generation.is_none()
                && audit.initial_target.phase == DurableSourceLifecyclePhase::Active
                && (if SESSION_BACKED_LIVE_SURFACES.contains(&surface_id) {
                    audit.initial_target.session_id.is_some()
                        && audit.initial_target.public_configuration_digest.is_some()
                } else {
                    audit.initial_target.session_id.is_some()
                        == audit.initial_target.public_configuration_digest.is_some()
                })
                && audit.initial_target.runtime_generation.is_none()
        }
        DurableSourceLifecycleIntent::Stop => {
            matches!(
                predecessor_kind,
                SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent
                    | SourceLifecyclePredecessorRuntimeKind::DesiredActiveRuntimeAbsent
                    | SourceLifecyclePredecessorRuntimeKind::NonAccount
                    | SourceLifecyclePredecessorRuntimeKind::AccountGroup
            ) && audit.expected_runtime_generation == exact_expected
                && audit.initial_target == &stopped_target
        }
        DurableSourceLifecycleIntent::Remove => {
            audit.expected_runtime_generation == exact_expected
                && audit.initial_target == &removed_target
        }
        DurableSourceLifecycleIntent::Resynchronize => {
            audit.predecessor.phase == DurableSourceLifecyclePhase::Active
                && matches!(
                    predecessor_kind,
                    SourceLifecyclePredecessorRuntimeKind::NonAccount
                        | SourceLifecyclePredecessorRuntimeKind::AccountGroup
                )
                && audit.expected_runtime_generation == exact_expected
                && audit.initial_target == &active_replacement_target
        }
        DurableSourceLifecycleIntent::UnhealthyRecovery => {
            audit.predecessor.phase == DurableSourceLifecyclePhase::Active
                && (matches!(
                    predecessor_kind,
                    SourceLifecyclePredecessorRuntimeKind::NonAccount
                        | SourceLifecyclePredecessorRuntimeKind::AccountGroup
                ) || predecessor_kind
                    == SourceLifecyclePredecessorRuntimeKind::DesiredActiveRuntimeAbsent)
                && audit.expected_runtime_generation == exact_expected
                && audit.initial_target == &active_replacement_target
        }
        DurableSourceLifecycleIntent::Reconfigure => {
            audit.predecessor.phase == DurableSourceLifecyclePhase::Active
                && matches!(
                    predecessor_kind,
                    SourceLifecyclePredecessorRuntimeKind::DesiredActiveRuntimeAbsent
                        | SourceLifecyclePredecessorRuntimeKind::NonAccount
                        | SourceLifecyclePredecessorRuntimeKind::AccountGroup
                )
                && audit.expected_runtime_generation == exact_expected
                && audit.initial_target.phase == DurableSourceLifecyclePhase::Active
                && audit.initial_target.session_id.is_some()
                && audit.initial_target.public_configuration_digest.is_some()
                && (audit.initial_target.session_id != audit.predecessor.session_id
                    || audit.initial_target.public_configuration_digest
                        != audit.predecessor.public_configuration_digest)
                && audit
                    .initial_target
                    .runtime_verification_receipt_digest
                    .is_none()
                && audit.initial_target.credential_generation.is_none()
                && audit.initial_target.runtime_generation.is_none()
        }
        DurableSourceLifecycleIntent::Verify => {
            audit.predecessor.phase == DurableSourceLifecyclePhase::Active
                && matches!(
                    predecessor_kind,
                    SourceLifecyclePredecessorRuntimeKind::NonAccount
                        | SourceLifecyclePredecessorRuntimeKind::AccountGroup
                )
                && audit.expected_runtime_generation == exact_expected
                && audit.initial_target == audit.predecessor
        }
        DurableSourceLifecycleIntent::VerifyStop => {
            let retained_stopped_target = audit.initial_target == &stopped_target
                && source_lifecycle_snapshot_has_complete_binding(audit.predecessor);
            let explicit_fresh_stopped_target = predecessor_kind
                == SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent
                && admits_fresh_verify_stop_binding(surface_id, audit.predecessor)
                && audit.initial_target.phase == DurableSourceLifecyclePhase::Stopped
                && audit.initial_target.session_id.is_some()
                && audit.initial_target.public_configuration_digest.is_some()
                && audit
                    .initial_target
                    .runtime_verification_receipt_digest
                    .is_none()
                && audit.initial_target.credential_generation.is_none()
                && audit.initial_target.runtime_generation.is_none();
            matches!(
                predecessor_kind,
                SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent
                    | SourceLifecyclePredecessorRuntimeKind::NonAccount
                    | SourceLifecyclePredecessorRuntimeKind::AccountGroup
            ) && audit.expected_runtime_generation == exact_expected
                && (retained_stopped_target || explicit_fresh_stopped_target)
        }
        DurableSourceLifecycleIntent::ProductShutdown => {
            audit.predecessor.phase == DurableSourceLifecyclePhase::Active
                && predecessor_kind == SourceLifecyclePredecessorRuntimeKind::AccountGroup
                && audit.expected_runtime_generation == exact_expected
                && audit.initial_target == &active_replacement_target
        }
    };
    if !valid {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(predecessor_kind)
}

fn source_lifecycle_predecessor_runtime_kind(
    surface_id: &str,
    predecessor: &DurableSourceLifecycleSnapshot,
) -> Result<SourceLifecyclePredecessorRuntimeKind, DurableProviderActivationStateError> {
    match predecessor.runtime_generation {
        Some(DurableSourceRuntimeGeneration::AccountGroup(_))
            if AccountMarketSurface::parse(surface_id).is_some()
                && predecessor.phase == DurableSourceLifecyclePhase::Active =>
        {
            Ok(SourceLifecyclePredecessorRuntimeKind::AccountGroup)
        }
        Some(DurableSourceRuntimeGeneration::Scalar(_))
        | Some(DurableSourceRuntimeGeneration::NonAccountDigest(_))
            if AccountMarketSurface::parse(surface_id).is_none()
                && predecessor.phase == DurableSourceLifecyclePhase::Active =>
        {
            Ok(SourceLifecyclePredecessorRuntimeKind::NonAccount)
        }
        None if predecessor.phase == DurableSourceLifecyclePhase::Stopped => {
            Ok(SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent)
        }
        None if predecessor.phase == DurableSourceLifecyclePhase::Removed => {
            Ok(SourceLifecyclePredecessorRuntimeKind::RemovedRuntimeAbsent)
        }
        None if predecessor.phase == DurableSourceLifecyclePhase::Active => {
            Ok(SourceLifecyclePredecessorRuntimeKind::DesiredActiveRuntimeAbsent)
        }
        _ => Err(DurableProviderActivationStateError::InvalidLifecycle),
    }
}

fn source_lifecycle_path(
    intent: DurableSourceLifecycleIntent,
    predecessor_kind: SourceLifecyclePredecessorRuntimeKind,
) -> Result<SourceLifecyclePath, DurableProviderActivationStateError> {
    let path = match (intent, predecessor_kind) {
        (DurableSourceLifecycleIntent::Verify, _) => SourceLifecyclePath::Verify,
        (
            DurableSourceLifecycleIntent::VerifyStop,
            SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent,
        ) => SourceLifecyclePath::StoppedVerification,
        (
            DurableSourceLifecycleIntent::Start,
            SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent
            | SourceLifecyclePredecessorRuntimeKind::RemovedRuntimeAbsent,
        ) => SourceLifecyclePath::AbsentStart,
        (
            DurableSourceLifecycleIntent::Reconfigure
            | DurableSourceLifecycleIntent::UnhealthyRecovery,
            SourceLifecyclePredecessorRuntimeKind::DesiredActiveRuntimeAbsent,
        ) => SourceLifecyclePath::DesiredActiveRestore,
        (
            DurableSourceLifecycleIntent::Resynchronize
            | DurableSourceLifecycleIntent::Reconfigure
            | DurableSourceLifecycleIntent::UnhealthyRecovery,
            SourceLifecyclePredecessorRuntimeKind::AccountGroup,
        ) => SourceLifecyclePath::AccountSuccessor,
        (
            DurableSourceLifecycleIntent::Resynchronize
            | DurableSourceLifecycleIntent::Reconfigure
            | DurableSourceLifecycleIntent::UnhealthyRecovery,
            SourceLifecyclePredecessorRuntimeKind::NonAccount,
        ) => SourceLifecyclePath::RuntimeSuccessor,
        (
            DurableSourceLifecycleIntent::Stop
            | DurableSourceLifecycleIntent::VerifyStop
            | DurableSourceLifecycleIntent::ProductShutdown,
            SourceLifecyclePredecessorRuntimeKind::AccountGroup,
        ) => SourceLifecyclePath::AccountTerminal,
        (
            DurableSourceLifecycleIntent::Stop | DurableSourceLifecycleIntent::VerifyStop,
            SourceLifecyclePredecessorRuntimeKind::NonAccount,
        ) => SourceLifecyclePath::RuntimeTerminal,
        (
            DurableSourceLifecycleIntent::Stop,
            SourceLifecyclePredecessorRuntimeKind::DesiredActiveRuntimeAbsent,
        ) => SourceLifecyclePath::RuntimeTerminal,
        (
            DurableSourceLifecycleIntent::Remove,
            SourceLifecyclePredecessorRuntimeKind::AccountGroup,
        ) => SourceLifecyclePath::AccountRemove,
        (
            DurableSourceLifecycleIntent::Remove,
            SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent
            | SourceLifecyclePredecessorRuntimeKind::RemovedRuntimeAbsent
            | SourceLifecyclePredecessorRuntimeKind::DesiredActiveRuntimeAbsent
            | SourceLifecyclePredecessorRuntimeKind::NonAccount,
        ) => SourceLifecyclePath::RuntimeRemove,
        (
            DurableSourceLifecycleIntent::Stop,
            SourceLifecyclePredecessorRuntimeKind::StoppedRuntimeAbsent,
        ) => SourceLifecyclePath::NoEffectOnly,
        _ => return Err(DurableProviderActivationStateError::InvalidLifecycle),
    };
    Ok(path)
}

fn source_lifecycle_checkpoint_stage(
    path: SourceLifecyclePath,
    predecessor_has_session: bool,
    checkpoint: DurableSourceLifecycleCheckpoint,
) -> Option<u8> {
    use DurableSourceLifecycleCheckpoint as Checkpoint;
    match (path, checkpoint) {
        (SourceLifecyclePath::NoEffectOnly, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::Verify, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::Verify, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::Verify, Checkpoint::TerminalPublished) => Some(2),
        (SourceLifecyclePath::StoppedVerification, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::StoppedVerification, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::StoppedVerification, Checkpoint::TerminalPublished) => Some(2),
        (
            SourceLifecyclePath::AbsentStart | SourceLifecyclePath::DesiredActiveRestore,
            Checkpoint::Planned,
        ) => Some(0),
        (
            SourceLifecyclePath::AbsentStart | SourceLifecyclePath::DesiredActiveRestore,
            Checkpoint::VerificationBound,
        ) => Some(1),
        (
            SourceLifecyclePath::AbsentStart | SourceLifecyclePath::DesiredActiveRestore,
            Checkpoint::SuccessorStarted,
        ) => Some(2),
        (
            SourceLifecyclePath::AbsentStart | SourceLifecyclePath::DesiredActiveRestore,
            Checkpoint::SuccessorDurable,
        ) => Some(3),
        (
            SourceLifecyclePath::AbsentStart | SourceLifecyclePath::DesiredActiveRestore,
            Checkpoint::TerminalPublished,
        ) => Some(4),
        (
            SourceLifecyclePath::AbsentStart | SourceLifecyclePath::DesiredActiveRestore,
            Checkpoint::ReadsAdmitted,
        ) => Some(5),
        (SourceLifecyclePath::RuntimeSuccessor, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::RuntimeSuccessor, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::RuntimeSuccessor, Checkpoint::RuntimeDrained) => Some(2),
        (SourceLifecyclePath::RuntimeSuccessor, Checkpoint::SuccessorStarted) => Some(3),
        (SourceLifecyclePath::RuntimeSuccessor, Checkpoint::SuccessorDurable) => Some(4),
        (SourceLifecyclePath::RuntimeSuccessor, Checkpoint::TerminalPublished) => Some(5),
        (SourceLifecyclePath::RuntimeSuccessor, Checkpoint::ReadsAdmitted) => Some(6),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::ShutdownKeyPersisted) => Some(2),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::AccountStopping) => Some(3),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::RuntimeDrained) => Some(4),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::TombstoneAcknowledged) => Some(5),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::SuccessorStarted) => Some(6),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::SuccessorDurable) => Some(7),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::TerminalPublished) => Some(8),
        (SourceLifecyclePath::AccountSuccessor, Checkpoint::ReadsAdmitted) => Some(9),
        (SourceLifecyclePath::RuntimeTerminal, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::RuntimeTerminal, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::RuntimeTerminal, Checkpoint::RuntimeDrained) => Some(2),
        (SourceLifecyclePath::RuntimeTerminal, Checkpoint::TerminalPublished) => Some(3),
        (SourceLifecyclePath::AccountTerminal, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::AccountTerminal, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::AccountTerminal, Checkpoint::ShutdownKeyPersisted) => Some(2),
        (SourceLifecyclePath::AccountTerminal, Checkpoint::AccountStopping) => Some(3),
        (SourceLifecyclePath::AccountTerminal, Checkpoint::RuntimeDrained) => Some(4),
        (SourceLifecyclePath::AccountTerminal, Checkpoint::TerminalPublished) => Some(5),
        (SourceLifecyclePath::AccountTerminal, Checkpoint::TombstoneAcknowledged) => Some(6),
        (SourceLifecyclePath::RuntimeRemove, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::RuntimeRemove, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::RuntimeRemove, Checkpoint::RuntimeDrained) => Some(2),
        (SourceLifecyclePath::RuntimeRemove, Checkpoint::PortalCancelled)
            if predecessor_has_session =>
        {
            Some(3)
        }
        (SourceLifecyclePath::RuntimeRemove, Checkpoint::TerminalPublished) => {
            Some(if predecessor_has_session { 4 } else { 3 })
        }
        (SourceLifecyclePath::AccountRemove, Checkpoint::Planned) => Some(0),
        (SourceLifecyclePath::AccountRemove, Checkpoint::VerificationBound) => Some(1),
        (SourceLifecyclePath::AccountRemove, Checkpoint::ShutdownKeyPersisted) => Some(2),
        (SourceLifecyclePath::AccountRemove, Checkpoint::AccountStopping) => Some(3),
        (SourceLifecyclePath::AccountRemove, Checkpoint::RuntimeDrained) => Some(4),
        (SourceLifecyclePath::AccountRemove, Checkpoint::PortalCancelled) => Some(5),
        (SourceLifecyclePath::AccountRemove, Checkpoint::TerminalPublished) => Some(6),
        (SourceLifecyclePath::AccountRemove, Checkpoint::TombstoneAcknowledged) => Some(7),
        _ => None,
    }
}

const fn source_lifecycle_final_checkpoint(
    path: SourceLifecyclePath,
) -> Option<DurableSourceLifecycleCheckpoint> {
    match path {
        SourceLifecyclePath::NoEffectOnly => None,
        SourceLifecyclePath::Verify
        | SourceLifecyclePath::StoppedVerification
        | SourceLifecyclePath::RuntimeTerminal
        | SourceLifecyclePath::RuntimeRemove => {
            Some(DurableSourceLifecycleCheckpoint::TerminalPublished)
        }
        SourceLifecyclePath::AbsentStart
        | SourceLifecyclePath::DesiredActiveRestore
        | SourceLifecyclePath::RuntimeSuccessor
        | SourceLifecyclePath::AccountSuccessor => {
            Some(DurableSourceLifecycleCheckpoint::ReadsAdmitted)
        }
        SourceLifecyclePath::AccountTerminal | SourceLifecyclePath::AccountRemove => {
            Some(DurableSourceLifecycleCheckpoint::TombstoneAcknowledged)
        }
    }
}

fn validate_source_lifecycle_target_progress(
    surface_id: &str,
    audit: &SourceLifecycleTransactionAudit<'_>,
    path: SourceLifecyclePath,
    stage: u8,
) -> Result<(), DurableProviderActivationStateError> {
    let exact_no_effect_rollback = audit.outcome
        == Some(DurableSourceLifecycleCompletionOutcome::NoEffect)
        && audit.checkpoint == DurableSourceLifecycleCheckpoint::Planned
        && audit.target == audit.predecessor;
    if exact_no_effect_rollback {
        return Ok(());
    }
    if audit.target.phase != audit.initial_target.phase
        || audit.target.session_id != audit.initial_target.session_id
        || audit.target.public_configuration_digest
            != audit.initial_target.public_configuration_digest
        || audit.initial_target.phase == DurableSourceLifecyclePhase::Removed
            && audit.target != audit.initial_target
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    let verification_was_bound = audit.target.runtime_verification_receipt_digest
        != audit.initial_target.runtime_verification_receipt_digest
        || audit.target.credential_generation != audit.initial_target.credential_generation;
    if verification_was_bound
        && (audit.target.runtime_verification_receipt_digest.is_none()
            || audit.target.credential_generation.is_none()
            || audit.checkpoint == DurableSourceLifecycleCheckpoint::Planned
            || !matches!(
                audit.intent,
                DurableSourceLifecycleIntent::Start
                    | DurableSourceLifecycleIntent::Reconfigure
                    | DurableSourceLifecycleIntent::Verify
                    | DurableSourceLifecycleIntent::VerifyStop
            ))
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }

    let successor_started_stage = source_lifecycle_checkpoint_stage(
        path,
        audit.predecessor.session_id.is_some(),
        DurableSourceLifecycleCheckpoint::SuccessorStarted,
    );
    match successor_started_stage {
        Some(successor_stage) if stage >= successor_stage => {
            let generation = audit
                .target
                .runtime_generation
                .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
            validate_runtime_generation(surface_id, Some(generation))?;
            if Some(generation) == audit.expected_runtime_generation {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
        Some(_) => {
            if audit.target.runtime_generation.is_some() {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
        None => {
            if audit.target.runtime_generation != audit.initial_target.runtime_generation {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
    }
    Ok(())
}

fn validate_source_lifecycle_effect_evidence(
    surface_id: &str,
    audit: &SourceLifecycleTransactionAudit<'_>,
    path: SourceLifecyclePath,
    stage: u8,
) -> Result<(), DurableProviderActivationStateError> {
    let predecessor_has_session = audit.predecessor.session_id.is_some();
    let shutdown_stage = source_lifecycle_checkpoint_stage(
        path,
        predecessor_has_session,
        DurableSourceLifecycleCheckpoint::ShutdownKeyPersisted,
    );
    match shutdown_stage {
        Some(required) if stage >= required => {
            let key = audit
                .shutdown_key
                .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
            validate_account_shutdown_key(&key)?;
            validate_shutdown_key_for_transaction(
                surface_id,
                audit.predecessor,
                audit.expected_runtime_generation,
                key,
            )?;
        }
        _ if audit.shutdown_key.is_some() => {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        _ => {}
    }

    let drain_stage = source_lifecycle_checkpoint_stage(
        path,
        predecessor_has_session,
        DurableSourceLifecycleCheckpoint::RuntimeDrained,
    );
    match drain_stage {
        Some(required) if stage >= required => {
            let evidence = audit
                .runtime_drain_evidence
                .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
            validate_runtime_drain_evidence(surface_id, audit, evidence)?;
        }
        _ if audit.runtime_drain_evidence.is_some() => {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        _ => {}
    }

    let portal_stage = source_lifecycle_checkpoint_stage(
        path,
        predecessor_has_session,
        DurableSourceLifecycleCheckpoint::PortalCancelled,
    );
    match portal_stage {
        Some(required) if stage >= required => {
            if audit.portal_cancellation_proof_digest
                != Some(source_lifecycle_portal_cancellation_proof_digest(
                    audit.transition_digest,
                )?)
            {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
        }
        _ if audit.portal_cancellation_proof_digest.is_some() => {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        _ => {}
    }
    Ok(())
}

fn validate_runtime_drain_evidence(
    _surface_id: &str,
    audit: &SourceLifecycleTransactionAudit<'_>,
    evidence: DurableSourceLifecycleRuntimeDrainEvidence,
) -> Result<(), DurableProviderActivationStateError> {
    require_sha256(evidence.proof_digest())?;
    let expected = match audit.predecessor.runtime_generation {
        Some(DurableSourceRuntimeGeneration::AccountGroup(_)) => {
            let key = audit
                .shutdown_key
                .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
            DurableSourceLifecycleRuntimeDrainEvidence::AccountGroupCompleted {
                phase: DurableSourceLifecyclePhysicalPhase::AccountStopCompleted,
                proof_digest: source_lifecycle_account_stop_proof_digest(
                    audit.transition_digest,
                    key,
                )?,
            }
        }
        Some(DurableSourceRuntimeGeneration::Scalar(generation)) => {
            DurableSourceLifecycleRuntimeDrainEvidence::NonAccountDrained {
                phase: DurableSourceLifecyclePhysicalPhase::NonAccountRuntimeDrained,
                proof_digest: source_lifecycle_non_account_runtime_drain_proof_digest(
                    audit.transition_digest,
                    generation,
                )?,
            }
        }
        Some(DurableSourceRuntimeGeneration::NonAccountDigest(digest)) => {
            DurableSourceLifecycleRuntimeDrainEvidence::NonAccountDrained {
                phase: DurableSourceLifecyclePhysicalPhase::NonAccountRuntimeDrained,
                proof_digest: source_lifecycle_non_account_digest_runtime_drain_proof_digest(
                    audit.transition_digest,
                    digest,
                )?,
            }
        }
        None => DurableSourceLifecycleRuntimeDrainEvidence::RuntimeProvenAbsent {
            phase: DurableSourceLifecyclePhysicalPhase::RuntimeProvenAbsent,
            proof_digest: source_lifecycle_runtime_absent_proof_digest(audit.transition_digest)?,
        },
    };
    if evidence != expected || evidence.phase() != expected.phase() {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(())
}

fn exact_pending_source_lifecycle_mut<'a>(
    record: &'a mut DurableSourceLifecycleRecord,
    expected_transition: EvidenceDigest,
    expected_intent: DurableSourceLifecycleIntent,
) -> Result<&'a mut DurableSourceLifecyclePendingTransaction, DurableProviderActivationStateError> {
    require_sha256(expected_transition)?;
    let pending = record
        .pending
        .as_mut()
        .ok_or(DurableProviderActivationStateError::StaleState)?;
    if pending.phase != DurableSourceLifecyclePendingPhase::Applying
        || pending.transition_digest != expected_transition
        || pending.intent != expected_intent
    {
        return Err(DurableProviderActivationStateError::StaleState);
    }
    Ok(pending)
}

fn prepare_source_lifecycle_checkpoint_advance(
    surface_id: &str,
    pending: &DurableSourceLifecyclePendingTransaction,
    target: DurableSourceLifecycleCheckpoint,
) -> Result<bool, DurableProviderActivationStateError> {
    let kind = source_lifecycle_predecessor_runtime_kind(surface_id, &pending.predecessor)?;
    let path = source_lifecycle_path(pending.intent, kind)?;
    let has_session = pending.predecessor.session_id.is_some();
    let current_stage = source_lifecycle_checkpoint_stage(path, has_session, pending.checkpoint)
        .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
    let target_stage = source_lifecycle_checkpoint_stage(path, has_session, target)
        .ok_or(DurableProviderActivationStateError::StaleState)?;
    if current_stage >= target_stage {
        return Ok(false);
    }
    let has_session_backed_binding = SESSION_BACKED_LIVE_SURFACES.contains(&surface_id)
        && (pending.predecessor.session_id.is_some() || pending.target.session_id.is_some());
    let may_skip_optional_verification =
        current_stage == 0 && target_stage == 2 && !has_session_backed_binding;
    if target_stage != current_stage.saturating_add(1) && !may_skip_optional_verification {
        return Err(DurableProviderActivationStateError::StaleState);
    }
    Ok(true)
}

fn require_exact_pending_shutdown_key(
    surface_id: &str,
    pending: &DurableSourceLifecyclePendingTransaction,
    shutdown_key: DurableAccountShutdownKey,
) -> Result<(), DurableProviderActivationStateError> {
    validate_account_shutdown_key(&shutdown_key)?;
    validate_shutdown_key_for_pending(surface_id, pending, shutdown_key)?;
    if pending.shutdown_key != Some(shutdown_key) {
        return Err(DurableProviderActivationStateError::StaleState);
    }
    Ok(())
}

fn validate_terminal_target(
    planned: &DurableSourceLifecycleSnapshot,
    terminal: &DurableSourceLifecycleSnapshot,
) -> Result<(), DurableProviderActivationStateError> {
    if planned != terminal {
        return Err(DurableProviderActivationStateError::StaleState);
    }
    Ok(())
}

fn source_lifecycle_target(
    surface_id: &str,
    intent: DurableSourceLifecycleIntent,
    predecessor: &DurableSourceLifecycleSnapshot,
    target_session_id: Option<Uuid>,
    target_public_configuration_digest: Option<EvidenceDigest>,
) -> Result<DurableSourceLifecycleSnapshot, DurableProviderActivationStateError> {
    let phase = source_lifecycle_terminal_phase(intent);
    if phase == DurableSourceLifecyclePhase::Removed {
        let target = DurableSourceLifecycleSnapshot {
            phase,
            session_id: None,
            public_configuration_digest: None,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            runtime_generation: None,
        };
        validate_source_lifecycle_snapshot(surface_id, &target)?;
        return Ok(target);
    }
    let (session_id, public_configuration_digest) = match intent {
        DurableSourceLifecycleIntent::Start => {
            if target_session_id.is_some() != target_public_configuration_digest.is_some()
                || SESSION_BACKED_LIVE_SURFACES.contains(&surface_id) && target_session_id.is_none()
            {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
            (target_session_id, target_public_configuration_digest)
        }
        DurableSourceLifecycleIntent::Reconfigure => (
            Some(target_session_id.ok_or(DurableProviderActivationStateError::InvalidLifecycle)?),
            Some(
                target_public_configuration_digest
                    .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?,
            ),
        ),
        DurableSourceLifecycleIntent::VerifyStop
            if admits_fresh_verify_stop_binding(surface_id, predecessor) =>
        {
            (
                Some(
                    target_session_id
                        .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?,
                ),
                Some(
                    target_public_configuration_digest
                        .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?,
                ),
            )
        }
        _ => {
            if target_session_id.is_some_and(|value| Some(value) != predecessor.session_id)
                || target_public_configuration_digest
                    .is_some_and(|value| Some(value) != predecessor.public_configuration_digest)
            {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
            (
                predecessor.session_id,
                predecessor.public_configuration_digest,
            )
        }
    };
    let same_binding = session_id == predecessor.session_id
        && public_configuration_digest == predecessor.public_configuration_digest;
    if intent == DurableSourceLifecycleIntent::Reconfigure && same_binding {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    let preserve_runtime = intent == DurableSourceLifecycleIntent::Verify && same_binding;
    let target = DurableSourceLifecycleSnapshot {
        phase,
        session_id,
        public_configuration_digest,
        runtime_verification_receipt_digest: same_binding
            .then_some(predecessor.runtime_verification_receipt_digest)
            .flatten(),
        credential_generation: same_binding
            .then_some(predecessor.credential_generation)
            .flatten(),
        runtime_generation: preserve_runtime
            .then_some(predecessor.runtime_generation)
            .flatten(),
    };
    validate_source_lifecycle_snapshot(surface_id, &target)?;
    Ok(target)
}

fn admits_fresh_verify_stop_binding(
    surface_id: &str,
    predecessor: &DurableSourceLifecycleSnapshot,
) -> bool {
    surface_id == ProviderMarketAccount::AlpacaBasic.surface_id()
        && predecessor.phase == DurableSourceLifecyclePhase::Stopped
        && predecessor.session_id.is_none()
        && predecessor.public_configuration_digest.is_none()
        && predecessor.runtime_verification_receipt_digest.is_none()
        && predecessor.credential_generation.is_none()
        && predecessor.runtime_generation.is_none()
}

fn source_lifecycle_snapshot_has_complete_binding(
    snapshot: &DurableSourceLifecycleSnapshot,
) -> bool {
    snapshot.session_id.is_some()
        && snapshot.public_configuration_digest.is_some()
        && snapshot.runtime_verification_receipt_digest.is_some()
        && snapshot.credential_generation.is_some()
}

const fn source_lifecycle_terminal_phase(
    intent: DurableSourceLifecycleIntent,
) -> DurableSourceLifecyclePhase {
    match intent {
        DurableSourceLifecycleIntent::Stop | DurableSourceLifecycleIntent::VerifyStop => {
            DurableSourceLifecyclePhase::Stopped
        }
        DurableSourceLifecycleIntent::Remove => DurableSourceLifecyclePhase::Removed,
        DurableSourceLifecycleIntent::Start
        | DurableSourceLifecycleIntent::Resynchronize
        | DurableSourceLifecycleIntent::Reconfigure
        | DurableSourceLifecycleIntent::Verify
        | DurableSourceLifecycleIntent::UnhealthyRecovery
        | DurableSourceLifecycleIntent::ProductShutdown => DurableSourceLifecyclePhase::Active,
    }
}

fn validate_account_shutdown_key(
    key: &DurableAccountShutdownKey,
) -> Result<(), DurableProviderActivationStateError> {
    if key.registry_incarnation.is_nil() || key.onboarding_session_id.is_nil() {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    require_sha256(key.public_configuration_digest)?;
    require_sha256(key.runtime_verification_receipt_digest)?;
    require_sha256(key.group_generation)?;
    match (key.surface_id, key.history_claim) {
        (AccountMarketSurface::AlpacaBasic, DurableAccountHistoryClaim::AlpacaNeverClaimed) => {}
        (AccountMarketSurface::AlpacaBasic, DurableAccountHistoryClaim::Alpaca(parent)) => {
            if parent.group_generation != key.group_generation {
                return Err(DurableProviderActivationStateError::InvalidLifecycle);
            }
            require_sha256(parent.binding_digest)?;
        }
        (AccountMarketSurface::KrakenLevel3, DurableAccountHistoryClaim::NeverApplicable) => {}
        _ => return Err(DurableProviderActivationStateError::InvalidLifecycle),
    }
    Ok(())
}

fn validate_shutdown_key_for_pending(
    surface_id: &str,
    pending: &DurableSourceLifecyclePendingTransaction,
    key: DurableAccountShutdownKey,
) -> Result<(), DurableProviderActivationStateError> {
    validate_shutdown_key_for_transaction(
        surface_id,
        &pending.predecessor,
        pending.expected_runtime_generation,
        key,
    )
}

fn validate_shutdown_key_for_transaction(
    surface_id: &str,
    predecessor: &DurableSourceLifecycleSnapshot,
    expected_runtime_generation: Option<DurableSourceRuntimeGeneration>,
    key: DurableAccountShutdownKey,
) -> Result<(), DurableProviderActivationStateError> {
    if key.surface_id.surface_id() != surface_id
        || predecessor.session_id != Some(key.onboarding_session_id)
        || predecessor.public_configuration_digest != Some(key.public_configuration_digest)
        || predecessor.runtime_verification_receipt_digest
            != Some(key.runtime_verification_receipt_digest)
        || predecessor.credential_generation != Some(key.credential_generation)
        || predecessor.runtime_generation
            != Some(DurableSourceRuntimeGeneration::AccountGroup(
                key.group_generation,
            ))
        || expected_runtime_generation
            != Some(DurableSourceRuntimeGeneration::AccountGroup(
                key.group_generation,
            ))
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(())
}

pub(super) fn source_lifecycle_account_stop_proof_digest(
    transition_digest: EvidenceDigest,
    key: DurableAccountShutdownKey,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    require_sha256(transition_digest)?;
    validate_account_shutdown_key(&key)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-account-stop-proof/v1\0");
    hash_evidence(&mut hasher, transition_digest)?;
    hasher.update([source_lifecycle_physical_phase_tag(
        DurableSourceLifecyclePhysicalPhase::AccountStopCompleted,
    )]);
    hash_account_shutdown_key(&mut hasher, key)?;
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

pub(super) fn source_lifecycle_non_account_runtime_drain_proof_digest(
    transition_digest: EvidenceDigest,
    predecessor_generation: NonZeroU64,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    require_sha256(transition_digest)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-runtime-drain-proof/v1\0");
    hash_evidence(&mut hasher, transition_digest)?;
    hasher.update([source_lifecycle_physical_phase_tag(
        DurableSourceLifecyclePhysicalPhase::NonAccountRuntimeDrained,
    )]);
    hasher.update(predecessor_generation.get().to_be_bytes());
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

pub(super) fn source_lifecycle_non_account_digest_runtime_drain_proof_digest(
    transition_digest: EvidenceDigest,
    predecessor_generation: EvidenceDigest,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    require_sha256(transition_digest)?;
    require_sha256(predecessor_generation)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-non-account-digest-drain-proof/v1\0");
    hash_evidence(&mut hasher, transition_digest)?;
    hasher.update([source_lifecycle_physical_phase_tag(
        DurableSourceLifecyclePhysicalPhase::NonAccountRuntimeDrained,
    )]);
    hash_evidence(&mut hasher, predecessor_generation)?;
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

pub(super) fn source_lifecycle_runtime_absent_proof_digest(
    transition_digest: EvidenceDigest,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    require_sha256(transition_digest)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-runtime-drain-proof/v1\0");
    hash_evidence(&mut hasher, transition_digest)?;
    hasher.update([source_lifecycle_physical_phase_tag(
        DurableSourceLifecyclePhysicalPhase::RuntimeProvenAbsent,
    )]);
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

pub(super) fn source_lifecycle_portal_cancellation_proof_digest(
    transition_digest: EvidenceDigest,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    require_sha256(transition_digest)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-portal-cancellation-proof/v1\0");
    hash_evidence(&mut hasher, transition_digest)?;
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

fn hash_account_shutdown_key(
    hasher: &mut Sha256,
    key: DurableAccountShutdownKey,
) -> Result<(), DurableProviderActivationStateError> {
    validate_account_shutdown_key(&key)?;
    hasher.update(key.registry_incarnation.as_bytes());
    hash_field(hasher, key.surface_id.surface_id().as_bytes())?;
    hasher.update(key.onboarding_session_id.as_bytes());
    hash_evidence(hasher, key.public_configuration_digest)?;
    hash_evidence(hasher, key.runtime_verification_receipt_digest)?;
    hasher.update(key.credential_generation.get().to_be_bytes());
    hash_evidence(hasher, key.group_generation)?;
    match key.history_claim {
        DurableAccountHistoryClaim::AlpacaNeverClaimed => hasher.update([1]),
        DurableAccountHistoryClaim::Alpaca(parent) => {
            hasher.update([2]);
            hash_evidence(hasher, parent.group_generation)?;
            hash_evidence(hasher, parent.binding_digest)?;
        }
        DurableAccountHistoryClaim::NeverApplicable => hasher.update([3]),
    }
    Ok(())
}

fn hash_source_lifecycle_snapshot(
    hasher: &mut Sha256,
    snapshot: &DurableSourceLifecycleSnapshot,
) -> Result<(), DurableProviderActivationStateError> {
    hasher.update([source_lifecycle_phase_tag(snapshot.phase)?]);
    hash_optional_uuid(hasher, snapshot.session_id);
    hash_optional_evidence(hasher, snapshot.public_configuration_digest)?;
    hash_optional_evidence(hasher, snapshot.runtime_verification_receipt_digest)?;
    match snapshot.credential_generation {
        Some(generation) => {
            hasher.update([1]);
            hasher.update(generation.get().to_be_bytes());
        }
        None => hasher.update([0]),
    }
    hash_optional_runtime_generation(hasher, snapshot.runtime_generation)
}

fn hash_optional_runtime_generation(
    hasher: &mut Sha256,
    generation: Option<DurableSourceRuntimeGeneration>,
) -> Result<(), DurableProviderActivationStateError> {
    match generation {
        Some(DurableSourceRuntimeGeneration::Scalar(generation)) => {
            hasher.update([1]);
            hasher.update(generation.get().to_be_bytes());
        }
        Some(DurableSourceRuntimeGeneration::NonAccountDigest(digest)) => {
            hasher.update([3]);
            hash_evidence(hasher, digest)?;
        }
        Some(DurableSourceRuntimeGeneration::AccountGroup(digest)) => {
            hasher.update([2]);
            hash_evidence(hasher, digest)?;
        }
        None => hasher.update([0]),
    }
    Ok(())
}

fn hash_optional_uuid(hasher: &mut Sha256, value: Option<Uuid>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.as_bytes());
        }
        None => hasher.update([0]),
    }
}

fn hash_optional_evidence(
    hasher: &mut Sha256,
    value: Option<EvidenceDigest>,
) -> Result<(), DurableProviderActivationStateError> {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_evidence(hasher, value)?;
        }
        None => hasher.update([0]),
    }
    Ok(())
}

fn hash_evidence(
    hasher: &mut Sha256,
    digest: EvidenceDigest,
) -> Result<(), DurableProviderActivationStateError> {
    require_sha256(digest)?;
    hasher.update([1]);
    hasher.update(digest.bytes());
    Ok(())
}

const fn source_lifecycle_intent_tag(intent: DurableSourceLifecycleIntent) -> u8 {
    match intent {
        DurableSourceLifecycleIntent::Start => 1,
        DurableSourceLifecycleIntent::Stop => 2,
        DurableSourceLifecycleIntent::Remove => 3,
        DurableSourceLifecycleIntent::Resynchronize => 4,
        DurableSourceLifecycleIntent::Reconfigure => 5,
        DurableSourceLifecycleIntent::Verify => 6,
        DurableSourceLifecycleIntent::VerifyStop => 7,
        DurableSourceLifecycleIntent::UnhealthyRecovery => 8,
        DurableSourceLifecycleIntent::ProductShutdown => 9,
    }
}

const fn source_lifecycle_physical_phase_tag(phase: DurableSourceLifecyclePhysicalPhase) -> u8 {
    match phase {
        DurableSourceLifecyclePhysicalPhase::AccountStopCompleted => 1,
        DurableSourceLifecyclePhysicalPhase::NonAccountRuntimeDrained => 2,
        DurableSourceLifecyclePhysicalPhase::RuntimeProvenAbsent => 3,
    }
}

fn source_lifecycle_phase_tag(
    phase: DurableSourceLifecyclePhase,
) -> Result<u8, DurableProviderActivationStateError> {
    match phase {
        DurableSourceLifecyclePhase::Active => Ok(1),
        DurableSourceLifecyclePhase::Stopped => Ok(2),
        DurableSourceLifecyclePhase::Removed => Ok(3),
        DurableSourceLifecyclePhase::Applying
        | DurableSourceLifecyclePhase::ReconciliationRequired => {
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        }
    }
}

fn require_sha256(digest: EvidenceDigest) -> Result<(), DurableProviderActivationStateError> {
    if digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32] {
        Ok(())
    } else {
        Err(DurableProviderActivationStateError::InvalidLifecycle)
    }
}

fn lifecycle_digest_from_lower_hex(
    value: &str,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    if !valid_sha256(value) {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high =
            hex_nibble(pair[0]).ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
        let low =
            hex_nibble(pair[1]).ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
        bytes[index] = (high << 4) | low;
    }
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, bytes);
    require_sha256(digest)?;
    Ok(digest)
}

fn quarantine_encoded(
    store: &LocalAuthorityStateStore,
    surface_id: &str,
    reason: DurableActivationQuarantineReason,
    encoded: &[u8],
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    if let Ok(existing) = serde_json::from_slice::<QuarantineWire>(encoded)
        && existing.schema_version == QUARANTINE_SCHEMA_VERSION
        && existing.record_kind == QUARANTINE_RECORD_KIND
        && existing.surface_id == surface_id
        && valid_sha256(&existing.state_sha256)
        && existing.evidence_digests.len() <= MAXIMUM_RECIPE_EVIDENCE_OBJECTS
        && strictly_ordered(&existing.evidence_digests)
        && existing
            .evidence_digests
            .iter()
            .all(|digest| valid_sha256(digest))
    {
        return digest_from_lower_hex(&existing.state_sha256);
    }
    let recipe = serde_json::from_slice::<RecipeWire>(encoded).ok();
    let session_id = recipe.as_ref().map(|recipe| recipe.session_id);
    let mut evidence_digests = recipe
        .map(|recipe| recipe.evidence_digests)
        .unwrap_or_default();
    evidence_digests.sort_unstable();
    evidence_digests.dedup();
    if evidence_digests.len() > MAXIMUM_RECIPE_EVIDENCE_OBJECTS
        || evidence_digests.iter().any(|digest| !valid_sha256(digest))
    {
        evidence_digests.clear();
    }
    let state_sha256 = sha256_bytes(encoded);
    let state_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(encoded).into());
    let quarantine = QuarantineWire {
        schema_version: QUARANTINE_SCHEMA_VERSION,
        record_kind: QUARANTINE_RECORD_KIND.to_owned(),
        surface_id: surface_id.to_owned(),
        session_id,
        state_sha256,
        reason,
        evidence_digests,
    };
    let encoded = serde_json::to_vec(&quarantine)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    store.store(&encoded)?;
    Ok(state_digest)
}

impl std::fmt::Debug for DurableProviderActivationState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableProviderActivationState")
            .field("root", &"[CONTROLLED LOCAL STATE]")
            .finish()
    }
}

pub(super) struct StoredActivationEvidence {
    bytes: Box<[u8]>,
    digest: EvidenceDigest,
}

impl StoredActivationEvidence {
    pub(super) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecipeWire {
    schema_version: u16,
    surface_id: String,
    session_id: Uuid,
    request_sha256: String,
    evidence_digests: Vec<String>,
    runtime_generation_sha256: String,
    predecessor_runtime_generation_sha256: Option<String>,
    bundle_sha256: String,
    request_json: String,
    #[serde(default)]
    publication_state: RecipePublicationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predecessor_recipe_json: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecipePublicationState {
    Staged,
    Cutover,
    #[default]
    Desired,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QuarantineWire {
    schema_version: u16,
    record_kind: String,
    surface_id: String,
    session_id: Option<Uuid>,
    state_sha256: String,
    reason: DurableActivationQuarantineReason,
    evidence_digests: Vec<String>,
}

fn surface_key(surface_id: &str) -> Result<&'static str, DurableProviderActivationStateError> {
    match surface_id {
        "sec.edgar-public" => Ok("sec"),
        "bls.v1-unregistered" => Ok("bls-public"),
        "bls.v2-registered" => Ok("bls-registered"),
        "treasury.daily-rates-xml" => Ok("treasury-daily-rates"),
        "treasury.fiscal-data" => Ok("treasury-fiscal"),
        "fred-alfred.api-v1-v2" => Ok("fred-alfred"),
        "local.files" => Ok("local-files"),
        "federal-reserve-board.data-download-program" => Ok("federal-reserve-board-h15"),
        _ => Err(DurableProviderActivationStateError::UnknownSurface),
    }
}

fn lifecycle_surface_key(
    surface_id: &str,
) -> Result<&'static str, DurableProviderActivationStateError> {
    if let Some(account) = ProviderMarketAccount::from_surface_id(surface_id) {
        return Ok(match account {
            ProviderMarketAccount::AlpacaBasic => "alpaca-basic-market-data",
            ProviderMarketAccount::KrakenLevel3 => "kraken-authenticated-level3-market-data",
        });
    }
    match surface_id {
        "coinbase.public-market-data" => Ok("coinbase-public"),
        COINBASE_DIRECT_LIVE_SURFACE => Ok("coinbase-direct"),
        "kraken.spot-public-market-data" => Ok("kraken-public"),
        "local.files" => Ok("local-files"),
        "local.portfolio-imports" => Ok("local-portfolio-imports"),
        _ => surface_key(surface_id),
    }
}

fn decode_activation_state(
    surface_id: &str,
    encoded: &[u8],
) -> Result<DurableActivationRecipeState, DurableProviderActivationStateError> {
    if let Ok(quarantine) = serde_json::from_slice::<QuarantineWire>(encoded) {
        if quarantine.schema_version != QUARANTINE_SCHEMA_VERSION
            || quarantine.record_kind != QUARANTINE_RECORD_KIND
            || quarantine.surface_id != surface_id
            || !valid_sha256(&quarantine.state_sha256)
            || quarantine.evidence_digests.len() > MAXIMUM_RECIPE_EVIDENCE_OBJECTS
            || !strictly_ordered(&quarantine.evidence_digests)
        {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        for digest in &quarantine.evidence_digests {
            validate_sha256(digest)?;
        }
        let state_digest = digest_from_lower_hex(&quarantine.state_sha256)?;
        return Ok(DurableActivationRecipeState::Quarantined(
            DurableActivationQuarantine {
                session_id: quarantine.session_id,
                reason: quarantine.reason,
                state_digest,
                evidence_digests: quarantine.evidence_digests,
            },
        ));
    }
    decode_recipe(surface_id, encoded, true)
}

fn backup_safe_activation_state(
    surface_id: &str,
    encoded: &[u8],
) -> Result<(Vec<u8>, bool), DurableProviderActivationStateError> {
    let (recipe, publication_state) = match decode_activation_state(surface_id, encoded)? {
        DurableActivationRecipeState::Missing => {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        DurableActivationRecipeState::Desired(recipe) => (recipe, RecipePublicationState::Desired),
        DurableActivationRecipeState::Staged(recipe) => (recipe, RecipePublicationState::Staged),
        DurableActivationRecipeState::Cutover(recipe) => (recipe, RecipePublicationState::Cutover),
        DurableActivationRecipeState::Quarantined(_quarantine) => {
            return Ok((encoded.to_vec(), false));
        }
    };
    let (request_bytes, request_reselection) = redact_ambient_paths(&recipe.request_bytes)?;
    let (predecessor_bytes, predecessor_reselection) = match recipe.staged_predecessor {
        Some(predecessor) => {
            let (encoded, reselection) =
                backup_safe_activation_state(surface_id, &predecessor.encoded_state)?;
            (Some(encoded), reselection)
        }
        None => (None, false),
    };
    let (encoded, _state_digest) = encode_recipe(
        surface_id,
        recipe.session_id,
        &request_bytes,
        &recipe.evidence_digests,
        recipe.runtime_generation_digest,
        recipe.predecessor_runtime_generation_digest,
        publication_state,
        predecessor_bytes.as_deref(),
    )?;
    Ok((encoded, request_reselection || predecessor_reselection))
}

fn activation_state_contains_path_field(
    encoded: &[u8],
) -> Result<bool, DurableProviderActivationStateError> {
    if serde_json::from_slice::<QuarantineWire>(encoded).is_ok() {
        return Ok(false);
    }
    let recipe: RecipeWire = serde_json::from_slice(encoded)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    let mut request: serde_json::Value = serde_json::from_str(&recipe.request_json)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    let found = redact_path_fields(&mut request)?;
    let predecessor = recipe
        .predecessor_recipe_json
        .as_deref()
        .map(|predecessor| activation_state_contains_path_field(predecessor.as_bytes()))
        .transpose()?
        .unwrap_or(false);
    Ok(found || predecessor)
}

fn redact_ambient_paths(
    request_bytes: &[u8],
) -> Result<(Vec<u8>, bool), DurableProviderActivationStateError> {
    let mut request: serde_json::Value = serde_json::from_slice(request_bytes)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    let found = redact_path_fields(&mut request)?;
    if !found {
        return Ok((request_bytes.to_vec(), false));
    }
    let encoded = serde_json::to_vec(&request)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    Ok((encoded, true))
}

fn redact_path_fields(
    value: &mut serde_json::Value,
) -> Result<bool, DurableProviderActivationStateError> {
    match value {
        serde_json::Value::Array(values) => {
            let mut found = false;
            for value in values {
                found |= redact_path_fields(value)?;
            }
            Ok(found)
        }
        serde_json::Value::Object(fields) => {
            let mut found = false;
            for (name, value) in fields {
                if name == "path" {
                    if !value.is_string() {
                        return Err(DurableProviderActivationStateError::InvalidRecipe);
                    }
                    *value = serde_json::Value::String(String::new());
                    found = true;
                } else {
                    found |= redact_path_fields(value)?;
                }
            }
            Ok(found)
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => Ok(false),
    }
}

fn decode_recipe(
    surface_id: &str,
    encoded: &[u8],
    allow_embedded_predecessor: bool,
) -> Result<DurableActivationRecipeState, DurableProviderActivationStateError> {
    let recipe: RecipeWire = serde_json::from_slice(encoded)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    if !matches!(
        recipe.schema_version,
        RECIPE_SCHEMA_VERSION
            | EMBEDDED_PREDECESSOR_RECIPE_SCHEMA_VERSION
            | PREDECESSOR_RECIPE_SCHEMA_VERSION
            | LEGACY_RECIPE_SCHEMA_VERSION
    ) || recipe.surface_id != surface_id
        || recipe.request_json.is_empty()
        || recipe.evidence_digests.len() > MAXIMUM_RECIPE_EVIDENCE_OBJECTS
        || !strictly_ordered(&recipe.evidence_digests)
        || recipe.schema_version == LEGACY_RECIPE_SCHEMA_VERSION
            && recipe.publication_state != RecipePublicationState::Desired
        || recipe.schema_version != RECIPE_SCHEMA_VERSION
            && recipe.publication_state == RecipePublicationState::Cutover
        || recipe.schema_version < EMBEDDED_PREDECESSOR_RECIPE_SCHEMA_VERSION
            && recipe.predecessor_recipe_json.is_some()
        || recipe.publication_state == RecipePublicationState::Desired
            && recipe.predecessor_recipe_json.is_some()
        || recipe.publication_state == RecipePublicationState::Cutover
            && recipe.predecessor_recipe_json.is_none()
        || !allow_embedded_predecessor && recipe.predecessor_recipe_json.is_some()
    {
        return Err(DurableProviderActivationStateError::InvalidRecipe);
    }
    for digest in &recipe.evidence_digests {
        validate_sha256(digest)?;
    }
    let runtime_generation_digest = digest_from_lower_hex(&recipe.runtime_generation_sha256)?;
    let predecessor_runtime_generation_digest = recipe
        .predecessor_runtime_generation_sha256
        .as_deref()
        .map(digest_from_lower_hex)
        .transpose()?;
    if predecessor_runtime_generation_digest == Some(runtime_generation_digest) {
        return Err(DurableProviderActivationStateError::InvalidRecipe);
    }
    let predecessor_bytes = recipe.predecessor_recipe_json.as_deref().map(str::as_bytes);
    let request_bytes = recipe.request_json.as_bytes();
    if sha256_bytes(request_bytes) != recipe.request_sha256
        || bundle_digest(
            recipe.schema_version,
            recipe.publication_state,
            surface_id,
            recipe.session_id,
            request_bytes,
            &recipe.evidence_digests,
            runtime_generation_digest,
            predecessor_runtime_generation_digest,
            predecessor_bytes,
        )? != recipe.bundle_sha256
    {
        return Err(DurableProviderActivationStateError::Integrity);
    }
    let staged_predecessor = match recipe.predecessor_recipe_json {
        Some(predecessor_json) => {
            let DurableActivationRecipeState::Desired(predecessor) =
                decode_recipe(surface_id, predecessor_json.as_bytes(), false)?
            else {
                return Err(DurableProviderActivationStateError::InvalidRecipe);
            };
            if Some(predecessor.runtime_generation_digest) != predecessor_runtime_generation_digest
            {
                return Err(DurableProviderActivationStateError::InvalidRecipe);
            }
            Some(Box::new(predecessor))
        }
        None => None,
    };
    let publication_state = recipe.publication_state;
    let recipe = DurableActivationRecipe {
        session_id: recipe.session_id,
        request_bytes: recipe.request_json.into_bytes().into_boxed_slice(),
        evidence_digests: recipe.evidence_digests,
        runtime_generation_digest,
        predecessor_runtime_generation_digest,
        state_digest: digest_bytes(encoded),
        encoded_state: encoded.to_vec().into_boxed_slice(),
        staged_predecessor,
    };
    Ok(match publication_state {
        RecipePublicationState::Desired => DurableActivationRecipeState::Desired(recipe),
        RecipePublicationState::Staged => DurableActivationRecipeState::Staged(recipe),
        RecipePublicationState::Cutover => DurableActivationRecipeState::Cutover(recipe),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "independent recipe schema and integrity inputs stay explicit"
)]
fn encode_recipe(
    surface_id: &str,
    session_id: Uuid,
    request_bytes: &[u8],
    evidence_digests: &[String],
    runtime_generation_digest: EvidenceDigest,
    predecessor_runtime_generation_digest: Option<EvidenceDigest>,
    publication_state: RecipePublicationState,
    predecessor_recipe: Option<&[u8]>,
) -> Result<(Vec<u8>, EvidenceDigest), DurableProviderActivationStateError> {
    if request_bytes.is_empty()
        || runtime_generation_digest.bytes() == [0; 32]
        || predecessor_runtime_generation_digest == Some(runtime_generation_digest)
        || predecessor_recipe.is_some()
            && (!matches!(
                publication_state,
                RecipePublicationState::Staged | RecipePublicationState::Cutover
            ) || predecessor_runtime_generation_digest.is_none())
    {
        return Err(DurableProviderActivationStateError::InvalidRecipe);
    }
    let request_json = std::str::from_utf8(request_bytes)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?
        .to_owned();
    let mut evidence_digests = evidence_digests.to_vec();
    evidence_digests.sort_unstable();
    evidence_digests.dedup();
    if evidence_digests.len() > MAXIMUM_RECIPE_EVIDENCE_OBJECTS {
        return Err(DurableProviderActivationStateError::ResourceExhausted);
    }
    for digest in &evidence_digests {
        validate_sha256(digest)?;
    }
    let predecessor_recipe_json = predecessor_recipe
        .map(|encoded| {
            std::str::from_utf8(encoded)
                .map(str::to_owned)
                .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)
        })
        .transpose()?;
    let recipe = RecipeWire {
        schema_version: RECIPE_SCHEMA_VERSION,
        surface_id: surface_id.to_owned(),
        session_id,
        request_sha256: sha256_bytes(request_bytes),
        runtime_generation_sha256: lower_hex(&runtime_generation_digest.bytes()),
        predecessor_runtime_generation_sha256: predecessor_runtime_generation_digest
            .map(|digest| lower_hex(&digest.bytes())),
        bundle_sha256: bundle_digest(
            RECIPE_SCHEMA_VERSION,
            publication_state,
            surface_id,
            session_id,
            request_bytes,
            &evidence_digests,
            runtime_generation_digest,
            predecessor_runtime_generation_digest,
            predecessor_recipe,
        )?,
        evidence_digests,
        request_json,
        publication_state,
        predecessor_recipe_json,
    };
    let encoded = serde_json::to_vec(&recipe)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    let digest = digest_bytes(&encoded);
    Ok((encoded, digest))
}

fn digest_bytes(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "independent digest schema and integrity inputs stay explicit"
)]
fn bundle_digest(
    schema_version: u16,
    publication_state: RecipePublicationState,
    surface_id: &str,
    session_id: Uuid,
    request_bytes: &[u8],
    evidence_digests: &[String],
    runtime_generation_digest: EvidenceDigest,
    predecessor_runtime_generation_digest: Option<EvidenceDigest>,
    predecessor_recipe: Option<&[u8]>,
) -> Result<String, DurableProviderActivationStateError> {
    let mut hasher = Sha256::new();
    match schema_version {
        LEGACY_RECIPE_SCHEMA_VERSION => {
            hasher.update(b"market-squawk:durable-provider-activation:v2");
        }
        PREDECESSOR_RECIPE_SCHEMA_VERSION => {
            hasher.update(b"market-squawk:durable-provider-activation:v3");
            hasher.update([match publication_state {
                RecipePublicationState::Staged => 0,
                RecipePublicationState::Cutover => {
                    return Err(DurableProviderActivationStateError::InvalidRecipe);
                }
                RecipePublicationState::Desired => 1,
            }]);
        }
        EMBEDDED_PREDECESSOR_RECIPE_SCHEMA_VERSION => {
            hasher.update(b"market-squawk:durable-provider-activation:v4");
            hasher.update([match publication_state {
                RecipePublicationState::Staged => 0,
                RecipePublicationState::Cutover => {
                    return Err(DurableProviderActivationStateError::InvalidRecipe);
                }
                RecipePublicationState::Desired => 1,
            }]);
        }
        RECIPE_SCHEMA_VERSION => {
            hasher.update(b"market-squawk:durable-provider-activation:v5");
            hasher.update([match publication_state {
                RecipePublicationState::Staged => 0,
                RecipePublicationState::Cutover => 1,
                RecipePublicationState::Desired => 2,
            }]);
        }
        _ => return Err(DurableProviderActivationStateError::InvalidRecipe),
    }
    hash_field(&mut hasher, surface_id.as_bytes())?;
    hasher.update(session_id.as_bytes());
    hash_field(&mut hasher, request_bytes)?;
    hasher.update(runtime_generation_digest.bytes());
    match predecessor_runtime_generation_digest {
        Some(digest) => {
            hasher.update([1]);
            hasher.update(digest.bytes());
        }
        None => hasher.update([0]),
    }
    let count = u16::try_from(evidence_digests.len())
        .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
    hasher.update(count.to_be_bytes());
    for digest in evidence_digests {
        hash_field(&mut hasher, digest.as_bytes())?;
    }
    if matches!(
        schema_version,
        EMBEDDED_PREDECESSOR_RECIPE_SCHEMA_VERSION | RECIPE_SCHEMA_VERSION
    ) {
        match predecessor_recipe {
            Some(encoded) => {
                hasher.update([1]);
                hash_field(&mut hasher, encoded)?;
            }
            None => hasher.update([0]),
        }
    } else if predecessor_recipe.is_some() {
        return Err(DurableProviderActivationStateError::InvalidRecipe);
    }
    Ok(lower_hex(&hasher.finalize()))
}

fn hash_field(
    hasher: &mut Sha256,
    bytes: &[u8],
) -> Result<(), DurableProviderActivationStateError> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn strictly_ordered(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn validate_sha256(value: &str) -> Result<(), DurableProviderActivationStateError> {
    if valid_sha256(value) {
        Ok(())
    } else {
        Err(DurableProviderActivationStateError::InvalidRecipe)
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest_from_lower_hex(
    value: &str,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    validate_sha256(value)?;
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        let low = hex_nibble(pair[1]).ok_or(DurableProviderActivationStateError::InvalidRecipe)?;
        bytes[index] = (high << 4) | low;
    }
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidRecipe);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

/// Provider-metadata owner snapshot or fresh-restore failure.
#[derive(Debug, Error)]
pub(super) enum ProviderMetadataBackupError {
    #[error("provider metadata backup was cancelled")]
    Cancelled,
    #[error("provider metadata backup is invalid")]
    Invalid,
    #[error("provider metadata backup exceeded its resource contract")]
    ResourceExhausted,
    #[error("provider metadata restore target is not fresh")]
    RestoreTargetNotFresh,
    #[error(transparent)]
    Activation(#[from] DurableProviderActivationStateError),
    #[error(transparent)]
    Research(#[from] ResearchIngestCompositionError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Store(#[from] LocalAuthorityStateStoreError),
}

/// Durable activation recipe or evidence failure.
#[derive(Debug, Error)]
pub(super) enum DurableProviderActivationStateError {
    #[error("provider activation surface is not persistable")]
    UnknownSurface,
    #[error("provider activation recipe is invalid")]
    InvalidRecipe,
    #[error("provider activation evidence is missing")]
    MissingEvidence,
    #[error("provider activation state failed integrity verification")]
    Integrity,
    #[error("provider activation state exceeded its resource contract")]
    ResourceExhausted,
    #[error("provider activation state changed before exact publication")]
    StaleState,
    #[error("provider source lifecycle state is invalid")]
    InvalidLifecycle,
    #[error("provider source lifecycle requires reconciliation")]
    LifecycleReconciliationRequired,
    #[error("provider activation evidence reclamation failed")]
    EvidenceReclamation(#[source] std::io::Error),
    #[error(transparent)]
    Store(#[from] LocalAuthorityStateStoreError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::num::NonZeroU64;
    use std::time::{Duration, Instant};

    use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn generation_digest(value: u8) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, [value; 32])
    }

    #[test]
    fn quarantined_recipe_is_disabled_until_an_exact_replacement_is_persisted() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let state = DurableProviderActivationState::new(temporary.path().to_path_buf());
        let surface_id = "treasury.fiscal-data";
        let session_id = Uuid::new_v4();
        let legacy_request = br#"{"schema_version":1}"#;
        state.publish_recipe(
            surface_id,
            None,
            session_id,
            legacy_request,
            &[],
            generation_digest(1),
            None,
        )?;

        let evidence = state.quarantine_recipe(
            surface_id,
            DurableActivationQuarantineReason::RequestSuperseded,
        )?;
        assert_ne!(evidence.bytes(), [0; 32]);
        assert!(matches!(
            state.load_recipe(surface_id)?,
            DurableActivationRecipeState::Quarantined(quarantine)
                if quarantine.session_id == Some(session_id)
                    && quarantine.reason
                        == DurableActivationQuarantineReason::RequestSuperseded
        ));

        let current_request = br#"{"schema_version":2}"#;
        let quarantined_state = state.current_state_digest(surface_id)?;
        state.publish_recipe(
            surface_id,
            quarantined_state,
            session_id,
            current_request,
            &[],
            generation_digest(2),
            Some(generation_digest(1)),
        )?;
        assert!(matches!(
            state.load_recipe(surface_id)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == session_id
                    && recipe.request_bytes.as_ref() == current_request
                    && recipe.runtime_generation_digest == generation_digest(2)
                    && recipe.predecessor_runtime_generation_digest
                        == Some(generation_digest(1))
        ));
        Ok(())
    }

    #[test]
    fn stale_activation_candidate_cannot_quarantine_a_newer_recipe() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let state = DurableProviderActivationState::new(temporary.path().to_path_buf());
        let surface_id = "treasury.fiscal-data";
        let first = state.publish_recipe(
            surface_id,
            None,
            Uuid::new_v4(),
            br#"{"schema_version":2,"candidate":1}"#,
            &[],
            generation_digest(1),
            None,
        )?;
        let second_session = Uuid::new_v4();
        let second_request = br#"{"schema_version":2,"candidate":2}"#;
        let second = state.publish_recipe(
            surface_id,
            Some(first),
            second_session,
            second_request,
            &[],
            generation_digest(2),
            Some(generation_digest(1)),
        )?;

        assert!(matches!(
            state.publish_recipe(
                surface_id,
                Some(first),
                Uuid::new_v4(),
                br#"{"schema_version":2,"candidate":3}"#,
                &[],
                generation_digest(3),
                Some(generation_digest(1)),
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));

        assert!(!state.quarantine_recipe_if_current(
            surface_id,
            first,
            DurableActivationQuarantineReason::AdapterRejected,
        )?);
        assert!(matches!(
            state.load_recipe(surface_id)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == second_session
                    && recipe.request_bytes.as_ref() == second_request
                    && recipe.state_digest == second
        ));
        assert!(state.quarantine_recipe_if_current(
            surface_id,
            second,
            DurableActivationQuarantineReason::AdapterRejected,
        )?);
        Ok(())
    }

    #[test]
    fn source_lifecycle_transition_is_exactly_once_and_crash_visible() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let state = DurableProviderActivationState::new(temporary.path().to_path_buf());

        let recipe_only_surface = "treasury.daily-rates-xml";
        state.publish_recipe(
            recipe_only_surface,
            None,
            Uuid::new_v4(),
            br#"{"schema_version":2,"candidate":"recipe-only"}"#,
            &[],
            generation_digest(10),
            None,
        )?;
        let fresh = state.source_lifecycle_record(recipe_only_surface)?;
        assert_eq!(fresh.phase(), DurableSourceLifecyclePhase::Stopped);
        assert_eq!(fresh.session_id(), None);

        let account_surface = ProviderMarketAccount::AlpacaBasic.surface_id();
        let account_session = Uuid::new_v4();
        let account_configuration = generation_digest(21);
        let account_verification = generation_digest(22);
        let account_credential = SecretGeneration::new(1)?;
        let account_generation =
            DurableSourceRuntimeGeneration::account_group(generation_digest(23))?;

        let fresh_verify_temporary = tempfile::tempdir()?;
        let fresh_verify_state =
            DurableProviderActivationState::new(fresh_verify_temporary.path().to_path_buf());
        let fresh_verify = fresh_verify_state.begin_source_lifecycle_transition(
            account_surface,
            NonZeroU64::MIN,
            SourceIdentifier::try_from("fresh-account-stopped-verification")?,
            generation_digest(19),
            DurableSourceLifecycleIntent::VerifyStop,
            Some(account_session),
            Some(account_configuration),
            None,
        )?;
        let fresh_verify_digest = fresh_verify.transition_digest()?;
        fresh_verify_state.bind_source_lifecycle_verification(
            account_surface,
            fresh_verify_digest,
            DurableSourceLifecycleIntent::VerifyStop,
            account_verification,
            account_credential,
        )?;
        fresh_verify_state.complete_source_lifecycle_transition(
            account_surface,
            fresh_verify_digest,
            DurableSourceLifecycleIntent::VerifyStop,
            DurableSourceLifecyclePhase::Stopped,
            Some(account_session),
            Some(account_configuration),
            Some(account_verification),
            Some(account_credential),
        )?;
        let fresh_verified = fresh_verify_state.confirm_source_lifecycle_transition(
            account_surface,
            fresh_verify_digest,
            DurableSourceLifecycleIntent::VerifyStop,
        )?;
        assert_eq!(fresh_verified.phase(), DurableSourceLifecyclePhase::Stopped);
        assert_eq!(fresh_verified.session_id(), Some(account_session));
        assert_eq!(
            fresh_verified.runtime_verification_receipt_digest(),
            Some(account_verification)
        );

        let fresh_no_effect_temporary = tempfile::tempdir()?;
        let fresh_no_effect_state =
            DurableProviderActivationState::new(fresh_no_effect_temporary.path().to_path_buf());
        let fresh_no_effect_prior =
            fresh_no_effect_state.source_lifecycle_record(account_surface)?;
        let fresh_no_effect_operation =
            SourceIdentifier::try_from("fresh-account-verification-no-effect")?;
        let fresh_no_effect_command = generation_digest(18);
        let fresh_no_effect = fresh_no_effect_state.begin_source_lifecycle_transition(
            account_surface,
            fresh_no_effect_prior.revision(),
            fresh_no_effect_operation.clone(),
            fresh_no_effect_command,
            DurableSourceLifecycleIntent::VerifyStop,
            Some(account_session),
            Some(account_configuration),
            None,
        )?;
        let fresh_no_effect_digest = fresh_no_effect.transition_digest()?;
        let fresh_no_effect_completed = fresh_no_effect_state.complete_source_lifecycle_no_effect(
            account_surface,
            fresh_no_effect_digest,
            &fresh_no_effect_prior,
        )?;
        assert_eq!(
            fresh_no_effect_completed.phase(),
            DurableSourceLifecyclePhase::Stopped
        );
        assert_eq!(fresh_no_effect_completed.session_id(), None);
        assert_eq!(
            fresh_no_effect_completed.public_configuration_digest(),
            None
        );
        assert_eq!(
            fresh_no_effect_completed.operation_id(),
            Some(&fresh_no_effect_operation)
        );
        assert!(fresh_no_effect_completed.pending_view().is_none());
        assert!(matches!(
            fresh_no_effect_state.begin_source_lifecycle_transition(
                account_surface,
                fresh_no_effect_completed.revision(),
                fresh_no_effect_operation,
                fresh_no_effect_command,
                DurableSourceLifecycleIntent::VerifyStop,
                Some(account_session),
                Some(account_configuration),
                None,
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));

        let forged_verify_temporary = tempfile::tempdir()?;
        let forged_verify_state =
            DurableProviderActivationState::new(forged_verify_temporary.path().to_path_buf());
        let forged_surface = COINBASE_DIRECT_LIVE_SURFACE;
        let forged_predecessor = forged_verify_state
            .source_lifecycle_record(forged_surface)?
            .settled;
        let forged_revision = NonZeroU64::new(2).ok_or("forged lifecycle revision")?;
        let forged_operation = SourceIdentifier::try_from("forged-fresh-stopped-verification")?;
        let forged_command = generation_digest(17);
        let forged_target = DurableSourceLifecycleSnapshot {
            phase: DurableSourceLifecyclePhase::Stopped,
            session_id: Some(Uuid::new_v4()),
            public_configuration_digest: Some(generation_digest(16)),
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            runtime_generation: None,
        };
        let forged_transition = source_lifecycle_transition_digest(
            forged_surface,
            forged_revision,
            &forged_operation,
            forged_command,
            DurableSourceLifecycleIntent::VerifyStop,
            &forged_predecessor,
            &forged_target,
            None,
        )?;
        let forged_record = DurableSourceLifecycleRecord {
            revision: forged_revision,
            settled: forged_predecessor.clone(),
            last_completed: None,
            pending: Some(DurableSourceLifecyclePendingTransaction {
                transition_revision: forged_revision,
                operation_id: forged_operation,
                command_digest: forged_command,
                transition_digest: forged_transition,
                intent: DurableSourceLifecycleIntent::VerifyStop,
                predecessor: forged_predecessor,
                initial_target: forged_target.clone(),
                target: forged_target,
                expected_runtime_generation: None,
                shutdown_key: None,
                phase: DurableSourceLifecyclePendingPhase::Applying,
                checkpoint: DurableSourceLifecycleCheckpoint::Planned,
                runtime_drain_evidence: None,
                portal_cancellation_proof_digest: None,
            }),
        };
        assert!(matches!(
            forged_verify_state.store_source_lifecycle(forged_surface, &forged_record),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));

        let account_transition = state.begin_source_lifecycle_transition(
            account_surface,
            NonZeroU64::MIN,
            SourceIdentifier::try_from("account-start-operation")?,
            generation_digest(20),
            DurableSourceLifecycleIntent::Start,
            Some(account_session),
            Some(account_configuration),
            None,
        )?;
        let account_transition_digest = account_transition.transition_digest()?;
        state.bind_source_lifecycle_verification(
            account_surface,
            account_transition_digest,
            DurableSourceLifecycleIntent::Start,
            account_verification,
            account_credential,
        )?;
        let pending = state.source_lifecycle_record(account_surface)?;
        let pending_view = pending.pending_view().ok_or("pending account start")?;
        assert_eq!(pending_view.transition_revision(), pending.revision());
        assert_eq!(pending_view.intent(), DurableSourceLifecycleIntent::Start);
        assert_eq!(
            pending_view.operation_id().as_str(),
            "account-start-operation"
        );
        assert_eq!(pending_view.command_digest(), generation_digest(20));
        assert_eq!(pending_view.transition_digest(), account_transition_digest);
        assert_eq!(pending_view.phase(), DurableSourceLifecyclePhase::Applying);
        assert_eq!(
            pending_view.predecessor().phase(),
            DurableSourceLifecyclePhase::Stopped
        );
        assert_eq!(
            pending_view.initial_target().session_id(),
            Some(account_session)
        );
        assert_eq!(pending_view.target().session_id(), Some(account_session));
        assert_eq!(
            pending_view.target().public_configuration_digest(),
            Some(account_configuration)
        );
        assert_eq!(
            pending_view.target().runtime_verification_receipt_digest(),
            Some(account_verification)
        );
        assert_eq!(
            pending_view.target().credential_generation(),
            Some(account_credential)
        );
        assert_eq!(pending_view.expected_runtime_generation(), None);
        assert_eq!(pending_view.shutdown_key(), None);
        assert_eq!(pending_view.runtime_drain_proof_digest(), None);
        assert_eq!(pending_view.portal_cancellation_proof_digest(), None);
        assert!(matches!(
            state.complete_source_lifecycle_transition(
                account_surface,
                account_transition_digest,
                DurableSourceLifecycleIntent::Start,
                DurableSourceLifecyclePhase::Active,
                Some(account_session),
                Some(account_configuration),
                Some(account_verification),
                Some(account_credential),
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));
        state.bind_source_lifecycle_target_generation(
            account_surface,
            account_transition_digest,
            DurableSourceLifecycleIntent::Start,
            account_generation,
        )?;
        state.record_source_lifecycle_successor_durable(
            account_surface,
            account_transition_digest,
            DurableSourceLifecycleIntent::Start,
            account_generation,
        )?;
        state.complete_source_lifecycle_transition(
            account_surface,
            account_transition_digest,
            DurableSourceLifecycleIntent::Start,
            DurableSourceLifecyclePhase::Active,
            Some(account_session),
            Some(account_configuration),
            Some(account_verification),
            Some(account_credential),
        )?;
        state.record_source_lifecycle_reads_admitted(
            account_surface,
            account_transition_digest,
            DurableSourceLifecycleIntent::Start,
            account_generation,
        )?;
        let account_active = state.confirm_source_lifecycle_transition(
            account_surface,
            account_transition_digest,
            DurableSourceLifecycleIntent::Start,
        );
        let account_active = account_active?;
        assert_eq!(account_active.phase(), DurableSourceLifecyclePhase::Active);
        assert_eq!(
            account_active.runtime_generation(),
            Some(account_generation)
        );

        let active_no_effect_temporary = tempfile::tempdir()?;
        let active_no_effect_state =
            DurableProviderActivationState::new(active_no_effect_temporary.path().to_path_buf());
        active_no_effect_state.store_source_lifecycle(account_surface, &account_active)?;
        let active_no_effect_operation =
            SourceIdentifier::try_from("active-account-verification-no-effect")?;
        let active_no_effect_command = generation_digest(55);
        let active_no_effect = active_no_effect_state.begin_source_lifecycle_transition(
            account_surface,
            account_active.revision(),
            active_no_effect_operation.clone(),
            active_no_effect_command,
            DurableSourceLifecycleIntent::VerifyStop,
            Some(account_session),
            Some(account_configuration),
            Some(account_generation),
        )?;
        let active_no_effect_digest = active_no_effect.transition_digest()?;
        let active_no_effect_completed = active_no_effect_state
            .complete_source_lifecycle_no_effect(
                account_surface,
                active_no_effect_digest,
                &account_active,
            )?;
        assert_eq!(
            active_no_effect_completed.phase(),
            DurableSourceLifecyclePhase::Active
        );
        assert_eq!(
            active_no_effect_completed.runtime_generation(),
            Some(account_generation)
        );
        assert_eq!(
            active_no_effect_completed.runtime_verification_receipt_digest(),
            Some(account_verification)
        );
        assert_eq!(
            active_no_effect_completed.credential_generation(),
            Some(account_credential)
        );
        assert_eq!(
            active_no_effect_completed.operation_id(),
            Some(&active_no_effect_operation)
        );
        assert!(active_no_effect_completed.pending_view().is_none());
        assert!(matches!(
            active_no_effect_state.begin_source_lifecycle_transition(
                account_surface,
                active_no_effect_completed.revision(),
                active_no_effect_operation,
                active_no_effect_command,
                DurableSourceLifecycleIntent::VerifyStop,
                Some(account_session),
                Some(account_configuration),
                Some(account_generation),
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));

        let product_shutdown = state.begin_source_lifecycle_transition(
            account_surface,
            account_active.revision(),
            SourceIdentifier::try_from("account-product-shutdown")?,
            generation_digest(40),
            DurableSourceLifecycleIntent::ProductShutdown,
            Some(account_session),
            Some(account_configuration),
            Some(account_generation),
        )?;
        let product_shutdown_digest = product_shutdown.transition_digest()?;
        assert_eq!(
            product_shutdown.record().runtime_generation(),
            None,
            "ProductShutdown must not retain the generation it will destroy"
        );
        state.bind_source_lifecycle_verification(
            account_surface,
            product_shutdown_digest,
            DurableSourceLifecycleIntent::ProductShutdown,
            account_verification,
            account_credential,
        )?;
        let product_shutdown_key = DurableAccountShutdownKey::try_new(
            Uuid::new_v4(),
            AccountMarketSurface::AlpacaBasic,
            account_session,
            account_configuration,
            account_verification,
            account_credential,
            generation_digest(23),
            DurableAccountHistoryClaim::AlpacaNeverClaimed,
        )?;
        state.bind_source_lifecycle_shutdown_key(
            account_surface,
            product_shutdown_digest,
            DurableSourceLifecycleIntent::ProductShutdown,
            product_shutdown_key,
        )?;
        state.record_source_lifecycle_account_stopping(
            account_surface,
            product_shutdown_digest,
            DurableSourceLifecycleIntent::ProductShutdown,
            product_shutdown_key,
        )?;
        let product_shutdown_proof = source_lifecycle_account_stop_proof_digest(
            product_shutdown_digest,
            product_shutdown_key,
        )?;
        state.record_source_lifecycle_runtime_drained(
            account_surface,
            product_shutdown_digest,
            DurableSourceLifecycleIntent::ProductShutdown,
            product_shutdown_key,
            product_shutdown_proof,
        )?;
        state.complete_source_lifecycle_transition(
            account_surface,
            product_shutdown_digest,
            DurableSourceLifecycleIntent::ProductShutdown,
            DurableSourceLifecyclePhase::Active,
            Some(account_session),
            Some(account_configuration),
            Some(account_verification),
            Some(account_credential),
        )?;
        state.record_source_lifecycle_tombstone_acknowledged(
            account_surface,
            product_shutdown_digest,
            DurableSourceLifecycleIntent::ProductShutdown,
            product_shutdown_key,
            product_shutdown_proof,
        )?;
        let desired_active = state.confirm_source_lifecycle_transition(
            account_surface,
            product_shutdown_digest,
            DurableSourceLifecycleIntent::ProductShutdown,
        )?;
        assert_eq!(desired_active.phase(), DurableSourceLifecyclePhase::Active);
        assert_eq!(desired_active.runtime_generation(), None);
        assert_eq!(desired_active.session_id(), Some(account_session));
        assert_eq!(
            desired_active.public_configuration_digest(),
            Some(account_configuration)
        );

        let malformed_temporary = tempfile::tempdir()?;
        let malformed_state =
            DurableProviderActivationState::new(malformed_temporary.path().to_path_buf());
        let malformed_desired_active = DurableSourceLifecycleRecord {
            revision: NonZeroU64::MIN,
            settled: DurableSourceLifecycleSnapshot {
                phase: DurableSourceLifecyclePhase::Active,
                session_id: None,
                public_configuration_digest: None,
                runtime_verification_receipt_digest: None,
                credential_generation: None,
                runtime_generation: None,
            },
            last_completed: None,
            pending: None,
        };
        assert!(matches!(
            malformed_state.store_source_lifecycle(account_surface, &malformed_desired_active),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));

        let declined_stop_temporary = tempfile::tempdir()?;
        let declined_stop_state =
            DurableProviderActivationState::new(declined_stop_temporary.path().to_path_buf());
        declined_stop_state.store_source_lifecycle(account_surface, &desired_active)?;
        let declined_stop = declined_stop_state.begin_source_lifecycle_transition(
            account_surface,
            desired_active.revision(),
            SourceIdentifier::try_from("desired-active-declined-stop")?,
            generation_digest(48),
            DurableSourceLifecycleIntent::Stop,
            Some(account_session),
            Some(account_configuration),
            None,
        )?;
        let declined_stop_digest = declined_stop.transition_digest()?;
        declined_stop_state.bind_source_lifecycle_verification(
            account_surface,
            declined_stop_digest,
            DurableSourceLifecycleIntent::Stop,
            account_verification,
            account_credential,
        )?;
        let declined_stop_absence =
            source_lifecycle_runtime_absent_proof_digest(declined_stop_digest)?;
        declined_stop_state.record_source_lifecycle_runtime_proven_absent(
            account_surface,
            declined_stop_digest,
            DurableSourceLifecycleIntent::Stop,
            declined_stop_absence,
        )?;
        declined_stop_state.complete_source_lifecycle_transition(
            account_surface,
            declined_stop_digest,
            DurableSourceLifecycleIntent::Stop,
            DurableSourceLifecyclePhase::Stopped,
            Some(account_session),
            Some(account_configuration),
            Some(account_verification),
            Some(account_credential),
        )?;
        let declined_stopped = declined_stop_state.confirm_source_lifecycle_transition(
            account_surface,
            declined_stop_digest,
            DurableSourceLifecycleIntent::Stop,
        )?;
        assert_eq!(
            declined_stopped.phase(),
            DurableSourceLifecyclePhase::Stopped
        );
        assert_eq!(declined_stopped.pending_shutdown_key(), None);

        let malformed_account_binding = serde_json::to_vec(&serde_json::json!({
            "schema_version": SOURCE_LIFECYCLE_SCHEMA_VERSION,
            "surface_id": account_surface,
            "revision": 1,
            "settled": {
                "phase": "active",
                "session_id": account_session,
                "public_configuration_sha256": lower_hex(&account_configuration.bytes()),
                "runtime_verification_receipt_sha256": null,
                "credential_generation": null,
                "runtime_generation": null,
            },
            "last_completed": null,
            "pending": null,
        }))?;
        assert!(matches!(
            decode_source_lifecycle(account_surface, &malformed_account_binding),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));

        let declined_remove_temporary = tempfile::tempdir()?;
        let declined_remove_state =
            DurableProviderActivationState::new(declined_remove_temporary.path().to_path_buf());
        declined_remove_state.store_source_lifecycle(account_surface, &desired_active)?;
        let declined_remove = declined_remove_state.begin_source_lifecycle_transition(
            account_surface,
            desired_active.revision(),
            SourceIdentifier::try_from("desired-active-declined-remove")?,
            generation_digest(49),
            DurableSourceLifecycleIntent::Remove,
            None,
            None,
            None,
        )?;
        let declined_remove_digest = declined_remove.transition_digest()?;
        declined_remove_state.bind_source_lifecycle_verification(
            account_surface,
            declined_remove_digest,
            DurableSourceLifecycleIntent::Remove,
            account_verification,
            account_credential,
        )?;
        let declined_remove_portal =
            source_lifecycle_portal_cancellation_proof_digest(declined_remove_digest)?;
        assert!(matches!(
            declined_remove_state.record_source_lifecycle_portal_cancelled(
                account_surface,
                declined_remove_digest,
                None,
                declined_remove_portal,
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));
        let declined_remove_absence =
            source_lifecycle_runtime_absent_proof_digest(declined_remove_digest)?;
        declined_remove_state.record_source_lifecycle_runtime_proven_absent(
            account_surface,
            declined_remove_digest,
            DurableSourceLifecycleIntent::Remove,
            declined_remove_absence,
        )?;
        declined_remove_state.record_source_lifecycle_portal_cancelled(
            account_surface,
            declined_remove_digest,
            None,
            declined_remove_portal,
        )?;
        declined_remove_state.complete_source_lifecycle_transition(
            account_surface,
            declined_remove_digest,
            DurableSourceLifecycleIntent::Remove,
            DurableSourceLifecyclePhase::Removed,
            None,
            None,
            None,
            None,
        )?;
        let declined_removed = declined_remove_state.confirm_source_lifecycle_transition(
            account_surface,
            declined_remove_digest,
            DurableSourceLifecycleIntent::Remove,
        )?;
        assert_eq!(
            declined_removed.phase(),
            DurableSourceLifecyclePhase::Removed
        );

        let replacement_temporary = tempfile::tempdir()?;
        let replacement_state =
            DurableProviderActivationState::new(replacement_temporary.path().to_path_buf());
        replacement_state.store_source_lifecycle(account_surface, &desired_active)?;
        let replacement_session = Uuid::new_v4();
        let replacement_configuration = generation_digest(50);
        let replacement_verification = generation_digest(51);
        let replacement_credential = SecretGeneration::new(3)?;
        let replacement_generation =
            DurableSourceRuntimeGeneration::account_group(generation_digest(52))?;
        let replacement = replacement_state.begin_source_lifecycle_transition(
            account_surface,
            desired_active.revision(),
            SourceIdentifier::try_from("desired-active-reconfigure")?,
            generation_digest(53),
            DurableSourceLifecycleIntent::Reconfigure,
            Some(replacement_session),
            Some(replacement_configuration),
            None,
        )?;
        let replacement_digest = replacement.transition_digest()?;
        replacement_state.bind_source_lifecycle_verification(
            account_surface,
            replacement_digest,
            DurableSourceLifecycleIntent::Reconfigure,
            replacement_verification,
            replacement_credential,
        )?;
        assert!(matches!(
            replacement_state.complete_source_lifecycle_transition(
                account_surface,
                replacement_digest,
                DurableSourceLifecycleIntent::Reconfigure,
                DurableSourceLifecyclePhase::Active,
                Some(replacement_session),
                Some(replacement_configuration),
                Some(replacement_verification),
                Some(replacement_credential),
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));
        replacement_state.bind_source_lifecycle_target_generation(
            account_surface,
            replacement_digest,
            DurableSourceLifecycleIntent::Reconfigure,
            replacement_generation,
        )?;
        replacement_state.record_source_lifecycle_successor_durable(
            account_surface,
            replacement_digest,
            DurableSourceLifecycleIntent::Reconfigure,
            replacement_generation,
        )?;
        replacement_state.complete_source_lifecycle_transition(
            account_surface,
            replacement_digest,
            DurableSourceLifecycleIntent::Reconfigure,
            DurableSourceLifecyclePhase::Active,
            Some(replacement_session),
            Some(replacement_configuration),
            Some(replacement_verification),
            Some(replacement_credential),
        )?;
        replacement_state.record_source_lifecycle_reads_admitted(
            account_surface,
            replacement_digest,
            DurableSourceLifecycleIntent::Reconfigure,
            replacement_generation,
        )?;
        let replacement_active = replacement_state.confirm_source_lifecycle_transition(
            account_surface,
            replacement_digest,
            DurableSourceLifecycleIntent::Reconfigure,
        )?;
        assert_eq!(replacement_active.session_id(), Some(replacement_session));
        assert_eq!(
            replacement_active.public_configuration_digest(),
            Some(replacement_configuration)
        );
        assert_eq!(
            replacement_active.runtime_generation(),
            Some(replacement_generation)
        );

        let restored_generation =
            DurableSourceRuntimeGeneration::account_group(generation_digest(27))?;
        let restore = state.begin_source_lifecycle_transition(
            account_surface,
            desired_active.revision(),
            SourceIdentifier::try_from("account-startup-restore")?,
            generation_digest(41),
            DurableSourceLifecycleIntent::UnhealthyRecovery,
            Some(account_session),
            Some(account_configuration),
            None,
        )?;
        let restore_digest = restore.transition_digest()?;
        state.bind_source_lifecycle_verification(
            account_surface,
            restore_digest,
            DurableSourceLifecycleIntent::UnhealthyRecovery,
            account_verification,
            account_credential,
        )?;
        state.bind_source_lifecycle_target_generation(
            account_surface,
            restore_digest,
            DurableSourceLifecycleIntent::UnhealthyRecovery,
            restored_generation,
        )?;
        state.record_source_lifecycle_successor_durable(
            account_surface,
            restore_digest,
            DurableSourceLifecycleIntent::UnhealthyRecovery,
            restored_generation,
        )?;
        state.complete_source_lifecycle_transition(
            account_surface,
            restore_digest,
            DurableSourceLifecycleIntent::UnhealthyRecovery,
            DurableSourceLifecyclePhase::Active,
            Some(account_session),
            Some(account_configuration),
            Some(account_verification),
            Some(account_credential),
        )?;
        state.record_source_lifecycle_reads_admitted(
            account_surface,
            restore_digest,
            DurableSourceLifecycleIntent::UnhealthyRecovery,
            restored_generation,
        )?;
        let account_active = state.confirm_source_lifecycle_transition(
            account_surface,
            restore_digest,
            DurableSourceLifecycleIntent::UnhealthyRecovery,
        )?;
        assert_ne!(restored_generation, account_generation);
        assert_eq!(
            account_active.runtime_generation(),
            Some(restored_generation)
        );

        let stop_operation = SourceIdentifier::try_from("account-stop-operation")?;
        let stop_command = generation_digest(24);
        let stop = state.begin_source_lifecycle_transition(
            account_surface,
            account_active.revision(),
            stop_operation.clone(),
            stop_command,
            DurableSourceLifecycleIntent::Stop,
            Some(account_session),
            Some(account_configuration),
            Some(restored_generation),
        )?;
        let stop_digest = stop.transition_digest()?;
        state.bind_source_lifecycle_verification(
            account_surface,
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
            account_verification,
            account_credential,
        )?;
        assert!(matches!(
            state.complete_source_lifecycle_transition(
                account_surface,
                stop_digest,
                DurableSourceLifecycleIntent::Stop,
                DurableSourceLifecyclePhase::Stopped,
                Some(account_session),
                Some(account_configuration),
                Some(account_verification),
                Some(account_credential),
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));
        let shutdown_key = DurableAccountShutdownKey::try_new(
            Uuid::new_v4(),
            AccountMarketSurface::AlpacaBasic,
            account_session,
            account_configuration,
            account_verification,
            account_credential,
            generation_digest(27),
            DurableAccountHistoryClaim::AlpacaNeverClaimed,
        )?;
        state.bind_source_lifecycle_shutdown_key(
            account_surface,
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
            shutdown_key,
        )?;
        state.record_source_lifecycle_account_stopping(
            account_surface,
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
            shutdown_key,
        )?;
        let stop_proof = source_lifecycle_account_stop_proof_digest(stop_digest, shutdown_key)?;
        state.record_source_lifecycle_runtime_drained(
            account_surface,
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
            shutdown_key,
            stop_proof,
        )?;
        state.complete_source_lifecycle_transition(
            account_surface,
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
            DurableSourceLifecyclePhase::Stopped,
            Some(account_session),
            Some(account_configuration),
            Some(account_verification),
            Some(account_credential),
        )?;
        state.record_source_lifecycle_tombstone_acknowledged(
            account_surface,
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
            shutdown_key,
            stop_proof,
        )?;
        let account_stopped = state.confirm_source_lifecycle_transition(
            account_surface,
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
        )?;
        assert_eq!(
            account_stopped.phase(),
            DurableSourceLifecyclePhase::Stopped
        );
        assert!(matches!(
            state.begin_source_lifecycle_transition(
                account_surface,
                NonZeroU64::MIN,
                stop_operation,
                stop_command,
                DurableSourceLifecycleIntent::Stop,
                Some(account_session),
                Some(account_configuration),
                Some(restored_generation),
            )?,
            DurableSourceLifecycleTransition::Replay(_)
        ));
        let account_key = lifecycle_surface_key(account_surface)?;
        let account_encoded =
            LocalAuthorityStateStore::try_open(state.lifecycle_root(account_key))?
                .load()?
                .ok_or("account lifecycle bytes")?;
        let account_wire: serde_json::Value = serde_json::from_slice(&account_encoded)?;
        assert_eq!(
            account_wire["last_completed"]["shutdown_key"]["surface_id"],
            account_surface
        );
        assert_eq!(
            account_wire["last_completed"]["runtime_drain_evidence"]["kind"],
            "account_group_completed"
        );
        assert_eq!(
            account_wire["last_completed"]["runtime_drain_evidence"]["phase"],
            "account_stop_completed"
        );
        let invalid_completed_phase = String::from_utf8(account_encoded)?.replacen(
            "\"phase\":\"account_stop_completed\"",
            "\"phase\":\"runtime_proven_absent\"",
            1,
        );
        assert!(matches!(
            decode_source_lifecycle(account_surface, invalid_completed_phase.as_bytes()),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));

        let stopped_verification = generation_digest(43);
        let stopped_credential = SecretGeneration::new(2)?;
        let verify_stop_operation = SourceIdentifier::try_from("account-stopped-verification")?;
        let verify_stop_command = generation_digest(42);
        let verify_stop = state.begin_source_lifecycle_transition(
            account_surface,
            account_stopped.revision(),
            verify_stop_operation.clone(),
            verify_stop_command,
            DurableSourceLifecycleIntent::VerifyStop,
            Some(account_session),
            Some(account_configuration),
            None,
        )?;
        let verify_stop_digest = verify_stop.transition_digest()?;
        state.bind_source_lifecycle_verification(
            account_surface,
            verify_stop_digest,
            DurableSourceLifecycleIntent::VerifyStop,
            stopped_verification,
            stopped_credential,
        )?;
        state.complete_source_lifecycle_transition(
            account_surface,
            verify_stop_digest,
            DurableSourceLifecycleIntent::VerifyStop,
            DurableSourceLifecyclePhase::Stopped,
            Some(account_session),
            Some(account_configuration),
            Some(stopped_verification),
            Some(stopped_credential),
        )?;
        let account_stopped = state.confirm_source_lifecycle_transition(
            account_surface,
            verify_stop_digest,
            DurableSourceLifecycleIntent::VerifyStop,
        )?;
        assert_eq!(
            account_stopped.phase(),
            DurableSourceLifecyclePhase::Stopped
        );
        assert_eq!(
            account_stopped.runtime_verification_receipt_digest(),
            Some(stopped_verification)
        );
        assert_eq!(
            account_stopped.credential_generation(),
            Some(stopped_credential)
        );
        assert!(matches!(
            state.begin_source_lifecycle_transition(
                account_surface,
                account_stopped.revision(),
                verify_stop_operation,
                verify_stop_command,
                DurableSourceLifecycleIntent::VerifyStop,
                Some(account_session),
                Some(account_configuration),
                None,
            )?,
            DurableSourceLifecycleTransition::Replay(_)
        ));

        let no_effect_prior = account_stopped;
        let no_effect_operation = SourceIdentifier::try_from("account-stop-no-effect")?;
        let no_effect_command = generation_digest(25);
        let no_effect = state.begin_source_lifecycle_transition(
            account_surface,
            no_effect_prior.revision(),
            no_effect_operation.clone(),
            no_effect_command,
            DurableSourceLifecycleIntent::Stop,
            Some(account_session),
            Some(account_configuration),
            None,
        )?;
        let no_effect_digest = no_effect.transition_digest()?;
        let account_stopped = state.complete_source_lifecycle_no_effect(
            account_surface,
            no_effect_digest,
            &no_effect_prior,
        )?;
        let no_effect_encoded =
            LocalAuthorityStateStore::try_open(state.lifecycle_root(account_key))?
                .load()?
                .ok_or("no-effect lifecycle bytes")?;
        let no_effect_wire: serde_json::Value = serde_json::from_slice(&no_effect_encoded)?;
        assert_eq!(no_effect_wire["last_completed"]["outcome"], "no_effect");
        assert_eq!(
            no_effect_wire["last_completed"]["operation_id"],
            "account-stop-no-effect"
        );
        assert!(matches!(
            state.begin_source_lifecycle_transition(
                account_surface,
                no_effect_prior.revision(),
                no_effect_operation,
                no_effect_command,
                DurableSourceLifecycleIntent::Stop,
                Some(account_session),
                Some(account_configuration),
                None,
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));

        let remove = state.begin_source_lifecycle_transition(
            account_surface,
            account_stopped.revision(),
            SourceIdentifier::try_from("account-remove-operation")?,
            generation_digest(26),
            DurableSourceLifecycleIntent::Remove,
            None,
            None,
            None,
        )?;
        let remove_digest = remove.transition_digest()?;
        state.bind_source_lifecycle_verification(
            account_surface,
            remove_digest,
            DurableSourceLifecycleIntent::Remove,
            stopped_verification,
            stopped_credential,
        )?;
        let portal_proof = source_lifecycle_portal_cancellation_proof_digest(remove_digest)?;
        assert!(matches!(
            state.record_source_lifecycle_portal_cancelled(
                account_surface,
                remove_digest,
                None,
                portal_proof,
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));
        let absent_proof = source_lifecycle_runtime_absent_proof_digest(remove_digest)?;
        state.record_source_lifecycle_runtime_proven_absent(
            account_surface,
            remove_digest,
            DurableSourceLifecycleIntent::Remove,
            absent_proof,
        )?;
        state.record_source_lifecycle_portal_cancelled(
            account_surface,
            remove_digest,
            None,
            portal_proof,
        )?;
        state.complete_source_lifecycle_transition(
            account_surface,
            remove_digest,
            DurableSourceLifecycleIntent::Remove,
            DurableSourceLifecyclePhase::Removed,
            None,
            None,
            None,
            None,
        )?;
        state.confirm_source_lifecycle_transition(
            account_surface,
            remove_digest,
            DurableSourceLifecycleIntent::Remove,
        )?;

        let digest_surface = "coinbase.public-market-data";
        let digest_generation =
            DurableSourceRuntimeGeneration::non_account_digest(generation_digest(44))?;
        let digest_start = state.begin_source_lifecycle_transition(
            digest_surface,
            NonZeroU64::MIN,
            SourceIdentifier::try_from("digest-start-operation")?,
            generation_digest(45),
            DurableSourceLifecycleIntent::Start,
            None,
            None,
            None,
        )?;
        let digest_start_digest = digest_start.transition_digest()?;
        state.bind_source_lifecycle_target_generation(
            digest_surface,
            digest_start_digest,
            DurableSourceLifecycleIntent::Start,
            digest_generation,
        )?;
        state.record_source_lifecycle_successor_durable(
            digest_surface,
            digest_start_digest,
            DurableSourceLifecycleIntent::Start,
            digest_generation,
        )?;
        state.complete_source_lifecycle_transition(
            digest_surface,
            digest_start_digest,
            DurableSourceLifecycleIntent::Start,
            DurableSourceLifecyclePhase::Active,
            None,
            None,
            None,
            None,
        )?;
        state.record_source_lifecycle_reads_admitted(
            digest_surface,
            digest_start_digest,
            DurableSourceLifecycleIntent::Start,
            digest_generation,
        )?;
        let digest_active = state.confirm_source_lifecycle_transition(
            digest_surface,
            digest_start_digest,
            DurableSourceLifecycleIntent::Start,
        )?;
        assert_eq!(digest_active.runtime_generation(), Some(digest_generation));
        assert!(matches!(
            state.begin_source_lifecycle_transition(
                digest_surface,
                digest_active.revision(),
                SourceIdentifier::try_from("non-account-product-shutdown")?,
                generation_digest(54),
                DurableSourceLifecycleIntent::ProductShutdown,
                None,
                None,
                Some(digest_generation),
            ),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));
        assert_eq!(
            state.source_lifecycle_record(digest_surface)?,
            digest_active
        );
        let digest_stop = state.begin_source_lifecycle_transition(
            digest_surface,
            digest_active.revision(),
            SourceIdentifier::try_from("digest-stop-operation")?,
            generation_digest(46),
            DurableSourceLifecycleIntent::Stop,
            None,
            None,
            Some(digest_generation),
        )?;
        let digest_stop_digest = digest_stop.transition_digest()?;
        assert_eq!(
            digest_stop
                .record()
                .pending_view()
                .ok_or("pending digest stop")?
                .expected_runtime_generation(),
            Some(digest_generation)
        );
        let digest_drain_proof = source_lifecycle_non_account_digest_runtime_drain_proof_digest(
            digest_stop_digest,
            generation_digest(44),
        )?;
        state.record_source_lifecycle_non_account_runtime_drained(
            digest_surface,
            digest_stop_digest,
            DurableSourceLifecycleIntent::Stop,
            digest_drain_proof,
        )?;
        state.complete_source_lifecycle_transition(
            digest_surface,
            digest_stop_digest,
            DurableSourceLifecycleIntent::Stop,
            DurableSourceLifecyclePhase::Stopped,
            None,
            None,
            None,
            None,
        )?;
        state.confirm_source_lifecycle_transition(
            digest_surface,
            digest_stop_digest,
            DurableSourceLifecycleIntent::Stop,
        )?;
        let digest_key = lifecycle_surface_key(digest_surface)?;
        let digest_encoded = LocalAuthorityStateStore::try_open(state.lifecycle_root(digest_key))?
            .load()?
            .ok_or("digest lifecycle bytes")?;
        let digest_wire: serde_json::Value = serde_json::from_slice(&digest_encoded)?;
        assert_eq!(
            digest_wire["last_completed"]["expected_runtime_generation"]["kind"],
            "non_account_digest"
        );
        assert_eq!(
            digest_wire["last_completed"]["runtime_drain_evidence"]["proof_sha256"],
            lower_hex(&digest_drain_proof.bytes())
        );

        let surface_id = "treasury.fiscal-data";
        let scalar_session = Uuid::new_v4();
        let scalar_configuration = generation_digest(30);
        let scalar_generation =
            DurableSourceRuntimeGeneration::Scalar(NonZeroU64::new(31).ok_or("scalar generation")?);
        let scalar_start = state.begin_source_lifecycle_transition(
            surface_id,
            NonZeroU64::MIN,
            SourceIdentifier::try_from("scalar-start-operation")?,
            generation_digest(31),
            DurableSourceLifecycleIntent::Start,
            Some(scalar_session),
            Some(scalar_configuration),
            None,
        )?;
        let scalar_start_digest = scalar_start.transition_digest()?;
        state.bind_source_lifecycle_target_generation(
            surface_id,
            scalar_start_digest,
            DurableSourceLifecycleIntent::Start,
            scalar_generation,
        )?;
        state.record_source_lifecycle_successor_durable(
            surface_id,
            scalar_start_digest,
            DurableSourceLifecycleIntent::Start,
            scalar_generation,
        )?;
        state.complete_source_lifecycle_transition(
            surface_id,
            scalar_start_digest,
            DurableSourceLifecycleIntent::Start,
            DurableSourceLifecyclePhase::Active,
            Some(scalar_session),
            Some(scalar_configuration),
            None,
            None,
        )?;
        state.record_source_lifecycle_reads_admitted(
            surface_id,
            scalar_start_digest,
            DurableSourceLifecycleIntent::Start,
            scalar_generation,
        )?;
        let scalar_active = state.confirm_source_lifecycle_transition(
            surface_id,
            scalar_start_digest,
            DurableSourceLifecycleIntent::Start,
        )?;
        let scalar_remove = state.begin_source_lifecycle_transition(
            surface_id,
            scalar_active.revision(),
            SourceIdentifier::try_from("scalar-remove-operation")?,
            generation_digest(32),
            DurableSourceLifecycleIntent::Remove,
            None,
            None,
            Some(scalar_generation),
        )?;
        let scalar_remove_digest = scalar_remove.transition_digest()?;
        let scalar_portal_proof =
            source_lifecycle_portal_cancellation_proof_digest(scalar_remove_digest)?;
        assert!(matches!(
            state.record_source_lifecycle_portal_cancelled(
                surface_id,
                scalar_remove_digest,
                None,
                scalar_portal_proof,
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));
        let scalar_drain_proof = source_lifecycle_non_account_runtime_drain_proof_digest(
            scalar_remove_digest,
            NonZeroU64::new(31).ok_or("scalar generation")?,
        )?;
        state.record_source_lifecycle_non_account_runtime_drained(
            surface_id,
            scalar_remove_digest,
            DurableSourceLifecycleIntent::Remove,
            scalar_drain_proof,
        )?;
        state.record_source_lifecycle_portal_cancelled(
            surface_id,
            scalar_remove_digest,
            None,
            scalar_portal_proof,
        )?;
        state.complete_source_lifecycle_transition(
            surface_id,
            scalar_remove_digest,
            DurableSourceLifecycleIntent::Remove,
            DurableSourceLifecyclePhase::Removed,
            None,
            None,
            None,
            None,
        )?;
        let scalar_removed = state.confirm_source_lifecycle_transition(
            surface_id,
            scalar_remove_digest,
            DurableSourceLifecycleIntent::Remove,
        )?;
        let removed_revision = scalar_removed.revision();
        let removed_operation = scalar_removed
            .operation_id()
            .ok_or("completed scalar remove operation")?
            .clone();
        assert!(matches!(
            state.begin_source_lifecycle_transition(
                surface_id,
                removed_revision,
                SourceIdentifier::try_from("removed-stop-operation")?,
                generation_digest(47),
                DurableSourceLifecycleIntent::Stop,
                None,
                None,
                None,
            ),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));
        let scalar_removed_after_rejection = state.source_lifecycle_record(surface_id)?;
        assert_eq!(scalar_removed_after_rejection.revision(), removed_revision);
        assert_eq!(
            scalar_removed_after_rejection
                .operation_id()
                .ok_or("retained scalar remove operation")?,
            &removed_operation
        );
        assert_eq!(
            scalar_removed_after_rejection.phase(),
            DurableSourceLifecyclePhase::Removed
        );
        assert!(scalar_removed_after_rejection.pending_view().is_none());

        let interrupted = state.begin_source_lifecycle_transition(
            surface_id,
            scalar_removed.revision(),
            SourceIdentifier::try_from("interrupted-operation")?,
            generation_digest(9),
            DurableSourceLifecycleIntent::Start,
            None,
            None,
            None,
        )?;
        let interrupted_digest = interrupted.transition_digest()?;
        let blocked =
            state.require_source_lifecycle_reconciliation(surface_id, interrupted_digest)?;
        let lifecycle_key = lifecycle_surface_key(surface_id)?;
        let encoded = LocalAuthorityStateStore::try_open(state.lifecycle_root(lifecycle_key))?
            .load()?
            .ok_or("lifecycle bytes")?;
        let wire: serde_json::Value = serde_json::from_slice(&encoded)?;
        assert_eq!(wire["schema_version"], SOURCE_LIFECYCLE_SCHEMA_VERSION);
        assert_eq!(wire["settled"]["phase"], "removed");
        assert_eq!(wire["pending"]["intent"], "start");
        assert_eq!(wire["pending"]["checkpoint"], "planned");
        assert_eq!(
            wire["pending"]["target"]["session_id"],
            serde_json::Value::Null
        );

        let invalid_retry = String::from_utf8(encoded.clone())?.replacen(
            "\"intent\":\"start\"",
            "\"intent\":\"retry\"",
            1,
        );
        assert!(matches!(
            decode_source_lifecycle(surface_id, invalid_retry.as_bytes()),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));
        let invalid_zero_digest = String::from_utf8(encoded)?.replacen(
            &lower_hex(&generation_digest(9).bytes()),
            &"0".repeat(64),
            1,
        );
        assert!(matches!(
            decode_source_lifecycle(surface_id, invalid_zero_digest.as_bytes()),
            Err(DurableProviderActivationStateError::InvalidLifecycle)
        ));

        let recovery = state.resume_source_lifecycle_transition(
            surface_id,
            blocked.revision(),
            interrupted_digest,
            DurableSourceLifecycleIntent::Start,
        )?;
        assert_eq!(recovery.record().revision(), blocked.revision());
        assert_eq!(
            recovery.record().operation_id(),
            Some(&SourceIdentifier::try_from("interrupted-operation")?)
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_authority_recipe_is_quarantined_during_product_startup() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let environment = BTreeMap::<OsString, OsString>::new();
        let config = AppConfig::load(ConfigSources::new(
            None,
            &environment,
            ConfigOverrides {
                data_dir: Some(temporary.path().join("data")),
                ..ConfigOverrides::default()
            },
        ))?;
        let initial = crate::LocalProduct::try_new(config.clone())?;
        let state = initial.provider_activation_state().clone();
        assert!(
            initial
                .application
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        drop(initial);
        state.publish_recipe(
            "treasury.fiscal-data",
            None,
            Uuid::new_v4(),
            br#"{"schema_version":1}"#,
            &[],
            generation_digest(1),
            None,
        )?;

        drop(crate::LocalProduct::try_new(config)?);
        assert!(matches!(
            state.load_recipe("treasury.fiscal-data")?,
            DurableActivationRecipeState::Quarantined(quarantine)
                if quarantine.reason
                    == DurableActivationQuarantineReason::AuthorityInvalidated
        ));
        Ok(())
    }
}
