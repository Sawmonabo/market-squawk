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
use market_squawk_sources::{AuthoritativeSourceRegistry, RegistryError, SEC_EDGAR_PROFILE_ID};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

use crate::application::{ProductionResearchIngestCoordinator, ResearchIngestCompositionError};
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
const SOURCE_LIFECYCLE_SCHEMA_VERSION: u16 = 2;
const LEGACY_PROVIDER_METADATA_BACKUP_SCHEMA_VERSION: u16 = 1;
const PROVIDER_METADATA_BACKUP_SCHEMA_VERSION: u16 = 2;
const LEGACY_PROVIDER_METADATA_LIFECYCLE_SURFACE_COUNT: usize = 11;
pub(super) const PROVIDER_METADATA_BACKUP_SCHEMA: &str = "market-squawk-provider-metadata-v1";
pub(super) const PROVIDER_METADATA_BACKUP_PRODUCER: &str =
    "market-squawk.provider-metadata-authority";
const MAXIMUM_PROVIDER_METADATA_BACKUP_BYTES: usize = 160 * 1024 * 1024;
const MAXIMUM_BACKUP_EVIDENCE_OBJECT_BYTES: u64 = 1024 * 1024;
const RESTORED_REQUIREMENT_SCHEMA_VERSION: u16 = 1;

pub(super) const RESTORABLE_RESEARCH_SURFACES: [&str; 10] = [
    SEC_EDGAR_PROFILE_ID,
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
    "local.files",
    "federal-reserve-board.data-download-program",
    "yahoo-finance.experimental-enrichment",
    "tiingo.starter-eod-nav",
];
pub(super) const SERIALIZED_RESEARCH_SURFACES: [&str; 11] = [
    SEC_EDGAR_PROFILE_ID,
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
    "local.files",
    "local.portfolio-imports",
    "federal-reserve-board.data-download-program",
    "yahoo-finance.experimental-enrichment",
    "tiingo.starter-eod-nav",
];

const COINBASE_DIRECT_LIVE_SURFACE: &str = "coinbase.exchange-direct-market-data";

const SESSION_BACKED_LIVE_SURFACES: [&str; 4] = [
    COINBASE_DIRECT_LIVE_SURFACE,
    ProviderMarketAccount::AlpacaBasic.surface_id(),
    ProviderMarketAccount::KrakenLevel3.surface_id(),
    ProviderMarketAccount::SchwabMarketData.surface_id(),
];

// New lifecycle surfaces are appended so schema-v1 backups remain an exact prefix.
const SERIALIZED_LIFECYCLE_SURFACES: [&str; 17] = [
    "coinbase.public-market-data",
    COINBASE_DIRECT_LIVE_SURFACE,
    "kraken.spot-public-market-data",
    SEC_EDGAR_PROFILE_ID,
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
    ProviderMarketAccount::SchwabMarketData.surface_id(),
    "yahoo-finance.experimental-enrichment",
    "tiingo.starter-eod-nav",
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

/// Validated durable lifecycle record used for compare-and-apply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DurableSourceLifecycleRecord {
    revision: NonZeroU64,
    phase: DurableSourceLifecyclePhase,
    operation_id: Option<SourceIdentifier>,
    command_digest: Option<EvidenceDigest>,
    transition_digest: Option<EvidenceDigest>,
    session_id: Option<Uuid>,
    public_configuration_digest: Option<EvidenceDigest>,
    runtime_verification_receipt_digest: Option<EvidenceDigest>,
    credential_generation: Option<SecretGeneration>,
}

impl DurableSourceLifecycleRecord {
    pub(super) const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    pub(super) const fn phase(&self) -> DurableSourceLifecyclePhase {
        self.phase
    }

    pub(super) const fn operation_id(&self) -> Option<&SourceIdentifier> {
        self.operation_id.as_ref()
    }

    pub(super) const fn session_id(&self) -> Option<Uuid> {
        self.session_id
    }

    pub(super) const fn public_configuration_digest(&self) -> Option<EvidenceDigest> {
        self.public_configuration_digest
    }

    pub(super) const fn runtime_verification_receipt_digest(&self) -> Option<EvidenceDigest> {
        self.runtime_verification_receipt_digest
    }

    pub(super) const fn credential_generation(&self) -> Option<SecretGeneration> {
        self.credential_generation
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
    pub(super) fn transition_digest(&self) -> EvidenceDigest {
        match self {
            Self::Apply(record) | Self::Replay(record) => match record.transition_digest {
                Some(digest) => digest,
                None => EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            },
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
    phase: DurableSourceLifecyclePhase,
    operation_id: Option<String>,
    command_sha256: Option<String>,
    transition_sha256: Option<String>,
    session_id: Option<Uuid>,
    public_configuration_sha256: Option<String>,
    runtime_verification_receipt_sha256: Option<String>,
    credential_generation: Option<u64>,
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
            let recovered = match self.load_recipe_for_startup_admission(surface_id) {
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
            let retained_session = match record.phase() {
                DurableSourceLifecyclePhase::Active
                | DurableSourceLifecyclePhase::Stopped
                | DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired => record.session_id(),
                DurableSourceLifecyclePhase::Removed => None,
            };
            if let Some(session_id) = retained_session {
                entries.push((SourceIdentifier::try_from(surface_id)?, session_id));
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
        let Some(record) = self.stored_source_lifecycle_record(surface_id)? else {
            let recipe = surface_key(surface_id)
                .ok()
                .map(|recipe_key| {
                    LocalAuthorityStateStore::try_open(self.recipe_root(recipe_key))?.load()
                })
                .transpose()?
                .flatten()
                .map(|encoded| decode_activation_state(surface_id, &encoded))
                .transpose()?;
            let (phase, session_id) = match recipe {
                Some(DurableActivationRecipeState::Desired(recipe)) => {
                    (DurableSourceLifecyclePhase::Active, Some(recipe.session_id))
                }
                Some(
                    DurableActivationRecipeState::Staged(recipe)
                    | DurableActivationRecipeState::Cutover(recipe),
                ) => (
                    DurableSourceLifecyclePhase::ReconciliationRequired,
                    Some(recipe.session_id),
                ),
                Some(DurableActivationRecipeState::Quarantined(quarantine)) => (
                    DurableSourceLifecyclePhase::ReconciliationRequired,
                    quarantine.session_id,
                ),
                Some(DurableActivationRecipeState::Missing) | None => {
                    (DurableSourceLifecyclePhase::Stopped, None)
                }
            };
            return Ok(DurableSourceLifecycleRecord {
                revision: NonZeroU64::MIN,
                phase,
                operation_id: None,
                command_digest: None,
                transition_digest: None,
                session_id,
                public_configuration_digest: None,
                runtime_verification_receipt_digest: None,
                credential_generation: None,
            });
        };
        Ok(record)
    }

    fn stored_source_lifecycle_record(
        &self,
        surface_id: &str,
    ) -> Result<Option<DurableSourceLifecycleRecord>, DurableProviderActivationStateError> {
        let key = lifecycle_surface_key(surface_id)?;
        LocalAuthorityStateStore::try_open(self.lifecycle_root(key))?
            .load()?
            .map(|encoded| decode_source_lifecycle(surface_id, &encoded))
            .transpose()
    }

    /// Durably claims one exact lifecycle transition before mutating runtime authority.
    pub(super) fn begin_source_lifecycle_transition(
        &self,
        surface_id: &str,
        expected_revision: NonZeroU64,
        operation_id: SourceIdentifier,
        command_digest: EvidenceDigest,
        allow_reconciliation: bool,
        target_session_id: Option<Uuid>,
        target_public_configuration_digest: Option<EvidenceDigest>,
    ) -> Result<DurableSourceLifecycleTransition, DurableProviderActivationStateError> {
        if command_digest.bytes() == [0; 32] {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        let current = self.source_lifecycle_record(surface_id)?;
        if current.operation_id.as_ref() == Some(&operation_id)
            && current.command_digest == Some(command_digest)
        {
            return if matches!(
                current.phase,
                DurableSourceLifecyclePhase::Applying
                    | DurableSourceLifecyclePhase::ReconciliationRequired
            ) {
                Err(DurableProviderActivationStateError::LifecycleReconciliationRequired)
            } else {
                Ok(DurableSourceLifecycleTransition::Replay(current))
            };
        }
        if current.revision != expected_revision {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        if matches!(
            current.phase,
            DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired
        ) && !allow_reconciliation
        {
            return Err(DurableProviderActivationStateError::LifecycleReconciliationRequired);
        }
        let revision = current
            .revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(DurableProviderActivationStateError::ResourceExhausted)?;
        let transition_digest = source_lifecycle_transition_digest(
            surface_id,
            revision,
            &operation_id,
            command_digest,
        )?;
        let applying = DurableSourceLifecycleRecord {
            revision,
            phase: DurableSourceLifecyclePhase::Applying,
            operation_id: Some(operation_id),
            command_digest: Some(command_digest),
            transition_digest: Some(transition_digest),
            session_id: target_session_id.or(current.session_id),
            public_configuration_digest: target_public_configuration_digest
                .or(current.public_configuration_digest),
            runtime_verification_receipt_digest: current.runtime_verification_receipt_digest,
            credential_generation: current.credential_generation,
        };
        self.store_source_lifecycle(surface_id, &applying)?;
        Ok(DurableSourceLifecycleTransition::Apply(applying))
    }

    /// Publishes the final result only for the exact in-progress transition.
    pub(super) fn complete_source_lifecycle_transition(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
        phase: DurableSourceLifecyclePhase,
        session_id: Option<Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        runtime_verification_receipt_digest: Option<EvidenceDigest>,
        credential_generation: Option<SecretGeneration>,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        if matches!(
            phase,
            DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired
        ) {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        let current = self.source_lifecycle_record(surface_id)?;
        if current.phase != DurableSourceLifecyclePhase::Applying
            || current.transition_digest != Some(expected_transition)
        {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let completed = DurableSourceLifecycleRecord {
            phase,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest,
            credential_generation,
            ..current
        };
        self.store_source_lifecycle(surface_id, &completed)?;
        Ok(completed)
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
            prior.phase,
            DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired
                | DurableSourceLifecyclePhase::Removed
        ) {
            return Err(DurableProviderActivationStateError::InvalidLifecycle);
        }
        let current = self.source_lifecycle_record(surface_id)?;
        if current.phase != DurableSourceLifecyclePhase::Applying
            || current.transition_digest != Some(expected_transition)
        {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let completed = DurableSourceLifecycleRecord {
            phase: prior.phase,
            session_id: prior.session_id,
            public_configuration_digest: prior.public_configuration_digest,
            runtime_verification_receipt_digest: prior.runtime_verification_receipt_digest,
            credential_generation: prior.credential_generation,
            ..current
        };
        self.store_source_lifecycle(surface_id, &completed)?;
        Ok(completed)
    }

    /// Converts an interrupted or indeterminate transition into an explicit recovery barrier.
    pub(super) fn require_source_lifecycle_reconciliation(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let current = self.source_lifecycle_record(surface_id)?;
        if current.phase != DurableSourceLifecyclePhase::Applying
            || current.transition_digest != Some(expected_transition)
        {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let blocked = DurableSourceLifecycleRecord {
            phase: DurableSourceLifecyclePhase::ReconciliationRequired,
            ..current
        };
        self.store_source_lifecycle(surface_id, &blocked)?;
        Ok(blocked)
    }

    /// Converts an exact just-completed lifecycle mutation into a recovery barrier when the
    /// corresponding runtime publication step fails after the durable final-state write.
    pub(super) fn require_completed_source_lifecycle_reconciliation(
        &self,
        surface_id: &str,
        expected_transition: EvidenceDigest,
    ) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
        let current = self.source_lifecycle_record(surface_id)?;
        if matches!(
            current.phase,
            DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired
                | DurableSourceLifecyclePhase::Removed
        ) || current.transition_digest != Some(expected_transition)
        {
            return Err(DurableProviderActivationStateError::StaleState);
        }
        let blocked = DurableSourceLifecycleRecord {
            phase: DurableSourceLifecyclePhase::ReconciliationRequired,
            ..current
        };
        self.store_source_lifecycle(surface_id, &blocked)?;
        Ok(blocked)
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

    /// Loads retained recipe state only when startup may repair or reconstruct its runtime.
    ///
    /// Recipe staging predates the source-lifecycle record for a newly activated research source,
    /// so an absent lifecycle record must still permit exact staged/cutover recovery. Once a
    /// lifecycle record exists, only `Active` remains startup-callable: stopped, removed,
    /// interrupted, and reconciliation-blocked sources retain evidence without regaining runtime
    /// authority. Backup-restored recipes likewise remain inert until their explicit requirements
    /// are completed.
    pub(super) fn load_recipe_for_startup_recovery(
        &self,
        surface_id: &str,
    ) -> Result<DurableActivationRecipeState, DurableProviderActivationStateError> {
        if !self.restored_requirements(surface_id)?.is_empty() {
            return Ok(DurableActivationRecipeState::Missing);
        }
        if let Some(lifecycle) = self.stored_source_lifecycle_record(surface_id)? {
            if lifecycle.phase() != DurableSourceLifecyclePhase::Active {
                return Ok(DurableActivationRecipeState::Missing);
            }
        }
        self.load_recipe_for_lifecycle(surface_id)
    }

    /// Loads retained session authority needed for onboarding startup reconciliation.
    ///
    /// A stopped or indeterminate source remains non-callable, but its admitted credential session
    /// must survive restart so an explicit start, retry, or reconciliation action can use the
    /// retained recipe. Removed sources and backup-restored recipes do not retain that admission.
    pub(super) fn load_recipe_for_startup_admission(
        &self,
        surface_id: &str,
    ) -> Result<DurableActivationRecipeState, DurableProviderActivationStateError> {
        if !self.restored_requirements(surface_id)?.is_empty()
            || self
                .stored_source_lifecycle_record(surface_id)?
                .is_some_and(|record| record.phase() == DurableSourceLifecyclePhase::Removed)
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
    let backup_schema_version = wire.schema_version;
    let expected_lifecycle_surfaces: &[&str] = match backup_schema_version {
        PROVIDER_METADATA_BACKUP_SCHEMA_VERSION => &SERIALIZED_LIFECYCLE_SURFACES,
        LEGACY_PROVIDER_METADATA_BACKUP_SCHEMA_VERSION => {
            &SERIALIZED_LIFECYCLE_SURFACES[..LEGACY_PROVIDER_METADATA_LIFECYCLE_SURFACE_COUNT]
        }
        _ => return Err(ProviderMetadataBackupError::Invalid),
    };
    if wire.lifecycle_records.len() != expected_lifecycle_surfaces.len() {
        return Err(ProviderMetadataBackupError::Invalid);
    }

    let mut lifecycle_records = Vec::new();
    lifecycle_records
        .try_reserve_exact(SERIALIZED_LIFECYCLE_SURFACES.len())
        .map_err(|_| ProviderMetadataBackupError::ResourceExhausted)?;
    for (encoded, expected_surface) in wire
        .lifecycle_records
        .into_iter()
        .zip(expected_lifecycle_surfaces.iter().copied())
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
    if backup_schema_version == LEGACY_PROVIDER_METADATA_BACKUP_SCHEMA_VERSION {
        for surface_id in
            &SERIALIZED_LIFECYCLE_SURFACES[LEGACY_PROVIDER_METADATA_LIFECYCLE_SURFACE_COUNT..]
        {
            let record = DurableSourceLifecycleRecord {
                revision: NonZeroU64::MIN,
                phase: DurableSourceLifecyclePhase::Stopped,
                operation_id: None,
                command_digest: None,
                transition_digest: None,
                session_id: None,
                public_configuration_digest: None,
                runtime_verification_receipt_digest: None,
                credential_generation: None,
            };
            lifecycle_records.push((
                (*surface_id).to_owned(),
                encode_source_lifecycle(surface_id, &record)?,
            ));
        }
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
    let wire = SourceLifecycleWire {
        schema_version: SOURCE_LIFECYCLE_SCHEMA_VERSION,
        surface_id: surface_id.to_owned(),
        revision: record.revision.get(),
        phase: record.phase,
        operation_id: record
            .operation_id
            .as_ref()
            .map(|value| value.as_str().to_owned()),
        command_sha256: record.command_digest.map(|value| lower_hex(&value.bytes())),
        transition_sha256: record
            .transition_digest
            .map(|value| lower_hex(&value.bytes())),
        session_id: record.session_id,
        public_configuration_sha256: record
            .public_configuration_digest
            .map(|value| lower_hex(&value.bytes())),
        runtime_verification_receipt_sha256: record
            .runtime_verification_receipt_digest
            .map(|value| lower_hex(&value.bytes())),
        credential_generation: record.credential_generation.map(SecretGeneration::get),
    };
    serde_json::to_vec(&wire).map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)
}

fn decode_source_lifecycle(
    surface_id: &str,
    encoded: &[u8],
) -> Result<DurableSourceLifecycleRecord, DurableProviderActivationStateError> {
    let wire: SourceLifecycleWire = serde_json::from_slice(encoded)
        .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?;
    if wire.schema_version != SOURCE_LIFECYCLE_SCHEMA_VERSION || wire.surface_id != surface_id {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    let revision = NonZeroU64::new(wire.revision)
        .ok_or(DurableProviderActivationStateError::InvalidLifecycle)?;
    let operation_id = wire
        .operation_id
        .map(SourceIdentifier::try_from)
        .transpose()
        .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?;
    let command_digest = wire
        .command_sha256
        .as_deref()
        .map(digest_from_lower_hex)
        .transpose()?;
    let transition_digest = wire
        .transition_sha256
        .as_deref()
        .map(digest_from_lower_hex)
        .transpose()?;
    let public_configuration_digest = wire
        .public_configuration_sha256
        .as_deref()
        .map(digest_from_lower_hex)
        .transpose()?;
    let runtime_verification_receipt_digest = wire
        .runtime_verification_receipt_sha256
        .as_deref()
        .map(digest_from_lower_hex)
        .transpose()?;
    let credential_generation = wire
        .credential_generation
        .map(SecretGeneration::new)
        .transpose()
        .map_err(|_| DurableProviderActivationStateError::InvalidLifecycle)?;
    let command_identity_valid = operation_id.is_some() == command_digest.is_some()
        && command_digest.is_some() == transition_digest.is_some();
    let runtime_binding_valid = runtime_verification_receipt_digest.is_some()
        == credential_generation.is_some()
        && (runtime_verification_receipt_digest.is_none()
            || (wire.session_id.is_some() && public_configuration_digest.is_some()));
    if !command_identity_valid
        || (wire.phase == DurableSourceLifecyclePhase::Applying && transition_digest.is_none())
        || !runtime_binding_valid
        || (wire.phase == DurableSourceLifecyclePhase::Removed
            && (wire.session_id.is_some()
                || public_configuration_digest.is_some()
                || runtime_verification_receipt_digest.is_some()
                || credential_generation.is_some()))
    {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(DurableSourceLifecycleRecord {
        revision,
        phase: wire.phase,
        operation_id,
        command_digest,
        transition_digest,
        session_id: wire.session_id,
        public_configuration_digest,
        runtime_verification_receipt_digest,
        credential_generation,
    })
}

fn source_lifecycle_transition_digest(
    surface_id: &str,
    revision: NonZeroU64,
    operation_id: &SourceIdentifier,
    command_digest: EvidenceDigest,
) -> Result<EvidenceDigest, DurableProviderActivationStateError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-transition/v1\0");
    hash_field(&mut hasher, surface_id.as_bytes())?;
    hasher.update(revision.get().to_be_bytes());
    hash_field(&mut hasher, operation_id.as_str().as_bytes())?;
    hasher.update(command_digest.bytes());
    let bytes: [u8; 32] = hasher.finalize().into();
    if bytes == [0; 32] {
        return Err(DurableProviderActivationStateError::InvalidLifecycle);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
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
        SEC_EDGAR_PROFILE_ID => Ok("sec"),
        "bls.v1-unregistered" => Ok("bls-public"),
        "bls.v2-registered" => Ok("bls-registered"),
        "treasury.daily-rates-xml" => Ok("treasury-daily-rates"),
        "treasury.fiscal-data" => Ok("treasury-fiscal"),
        "fred-alfred.api-v1-v2" => Ok("fred-alfred"),
        "local.files" => Ok("local-files"),
        "federal-reserve-board.data-download-program" => Ok("federal-reserve-board-h15"),
        "yahoo-finance.experimental-enrichment" => Ok("yahoo-enrichment"),
        "tiingo.starter-eod-nav" => Ok("tiingo-starter-eod-nav"),
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
            ProviderMarketAccount::SchwabMarketData => "schwab-trader-api-market-data",
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
        let surface_id = "treasury.fiscal-data";
        let retained_session = Uuid::new_v4();
        state.publish_recipe(
            surface_id,
            None,
            retained_session,
            br#"{"schema_version":2,"candidate":"retained"}"#,
            &[],
            generation_digest(6),
            None,
        )?;
        assert!(matches!(
            state.load_recipe_for_startup_recovery(surface_id)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == retained_session
        ));
        let operation_id = SourceIdentifier::try_from("source-stop-operation")?;
        let command_digest = generation_digest(7);

        let transition = state.begin_source_lifecycle_transition(
            surface_id,
            NonZeroU64::MIN,
            operation_id.clone(),
            command_digest,
            false,
            None,
            None,
        )?;
        assert_eq!(
            transition.record().revision(),
            NonZeroU64::new(2).ok_or("revision")?
        );
        assert!(matches!(
            state.source_lifecycle_record(surface_id)?.phase(),
            DurableSourceLifecyclePhase::Applying
        ));

        let completed = state.complete_source_lifecycle_transition(
            surface_id,
            transition.transition_digest(),
            DurableSourceLifecyclePhase::Stopped,
            None,
            None,
            None,
            None,
        )?;
        assert_eq!(completed.revision(), NonZeroU64::new(2).ok_or("revision")?);
        assert_eq!(completed.phase(), DurableSourceLifecyclePhase::Stopped);
        assert!(matches!(
            state.load_recipe_for_lifecycle(surface_id)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == retained_session
        ));
        assert!(matches!(
            state.load_recipe_for_startup_recovery(surface_id)?,
            DurableActivationRecipeState::Missing
        ));
        assert!(matches!(
            state.load_recipe_for_startup_admission(surface_id)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == retained_session
        ));
        assert!(matches!(
            state.begin_source_lifecycle_transition(
                surface_id,
                NonZeroU64::MIN,
                operation_id,
                command_digest,
                false,
                None,
                None,
            )?,
            DurableSourceLifecycleTransition::Replay(_)
        ));
        assert!(matches!(
            state.begin_source_lifecycle_transition(
                surface_id,
                NonZeroU64::MIN,
                SourceIdentifier::try_from("different-stop-operation")?,
                generation_digest(8),
                false,
                None,
                None,
            ),
            Err(DurableProviderActivationStateError::StaleState)
        ));

        let interrupted = state.begin_source_lifecycle_transition(
            surface_id,
            completed.revision(),
            SourceIdentifier::try_from("interrupted-operation")?,
            generation_digest(9),
            false,
            None,
            None,
        )?;
        let blocked = state
            .require_source_lifecycle_reconciliation(surface_id, interrupted.transition_digest())?;
        let recovery = state.begin_source_lifecycle_transition(
            surface_id,
            blocked.revision(),
            SourceIdentifier::try_from("recovery-operation")?,
            generation_digest(10),
            true,
            None,
            None,
        )?;
        assert_eq!(
            recovery.record().revision().get(),
            blocked.revision().get() + 1
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

    #[tokio::test]
    async fn legacy_provider_metadata_restores_with_stopped_account_lifecycles() -> TestResult {
        let source = tempfile::tempdir()?;
        let environment = BTreeMap::<OsString, OsString>::new();
        let config = AppConfig::load(ConfigSources::new(
            None,
            &environment,
            ConfigOverrides {
                data_dir: Some(source.path().join("source")),
                ..ConfigOverrides::default()
            },
        ))?;
        let product = crate::LocalProduct::try_new(config)?;
        let state = product.provider_activation_state().clone();
        let surface_id = "treasury.fiscal-data";
        let retained_session = Uuid::new_v4();
        let retained_request = br#"{"schema_version":2,"candidate":"retained"}"#;
        let retained_digest = state.publish_recipe(
            surface_id,
            None,
            retained_session,
            retained_request,
            &[],
            generation_digest(21),
            None,
        )?;
        let authority = ProviderMetadataBackupAuthority::new(
            state.clone(),
            product.provider_onboarding(),
            product.research_ingest(),
        );
        let retained = authority
            .retain(&tokio_util::sync::CancellationToken::new())
            .await?;
        let mut legacy_wire: ProviderMetadataBackupWire = serde_json::from_slice(retained.bytes())?;
        legacy_wire.schema_version = LEGACY_PROVIDER_METADATA_BACKUP_SCHEMA_VERSION;
        legacy_wire
            .lifecycle_records
            .truncate(LEGACY_PROVIDER_METADATA_LIFECYCLE_SURFACE_COUNT);
        let legacy_backup = serde_json::to_vec(&legacy_wire)?;

        state.publish_recipe(
            surface_id,
            Some(retained_digest),
            Uuid::new_v4(),
            br#"{"schema_version":2,"candidate":"later"}"#,
            &[],
            generation_digest(22),
            Some(generation_digest(21)),
        )?;

        let destination = tempfile::tempdir()?;
        let restored_state =
            DurableProviderActivationState::new(destination.path().join("control"));
        let registry_store = LocalAuthorityStateStore::try_open(
            destination.path().join("control/sources/research-runtime"),
        )?;
        let requirements = ProviderMetadataBackupAuthority::restore_fresh(
            &restored_state,
            registry_store,
            &legacy_backup,
        )?;
        assert!(requirements.iter().any(|requirement| {
            requirement.surface_id == surface_id
                && requirement.kind == ProviderMetadataRestoreRequirementKind::Reactivation
        }));
        assert!(matches!(
            restored_state.load_recipe(surface_id)?,
            DurableActivationRecipeState::Missing
        ));
        assert!(matches!(
            restored_state.load_recipe_for_startup_recovery(surface_id)?,
            DurableActivationRecipeState::Missing
        ));
        assert!(matches!(
            restored_state.load_recipe_for_startup_admission(surface_id)?,
            DurableActivationRecipeState::Missing
        ));
        assert!(matches!(
            restored_state.load_recipe_for_lifecycle(surface_id)?,
            DurableActivationRecipeState::Desired(recipe)
                if recipe.session_id == retained_session
                    && recipe.request_bytes.as_ref() == retained_request
                    && recipe.state_digest == retained_digest
        ));
        for account in ProviderMarketAccount::ALL {
            let account_surface = account.surface_id();
            let lifecycle_key = lifecycle_surface_key(account_surface)?;
            assert!(
                LocalAuthorityStateStore::try_open(restored_state.lifecycle_root(lifecycle_key))?
                    .load()?
                    .is_some()
            );
            let record = restored_state.source_lifecycle_record(account_surface)?;
            assert_eq!(record.revision(), NonZeroU64::MIN);
            assert_eq!(record.phase(), DurableSourceLifecyclePhase::Stopped);
            assert_eq!(record.session_id(), None);
        }
        market_squawk_sources::AuthoritativeSourceRegistry::try_new_durable(
            LocalAuthorityStateStore::try_open(
                destination.path().join("control/sources/research-runtime"),
            )?,
        )?
        .shutdown()?;
        Ok(())
    }
}
