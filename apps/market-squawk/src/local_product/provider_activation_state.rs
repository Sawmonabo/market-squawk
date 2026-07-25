//! Crash-safe, secret-free persistence for reconstructible research-provider activation.

mod evidence;

use std::path::PathBuf;
use std::sync::Arc;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

use crate::provider_onboarding::{ProviderOnboardingError, ProviderRuntimeStartupAdmissions};
pub(super) use evidence::ActivationEvidenceCandidate;

const RECIPE_SCHEMA_VERSION: u16 = 4;
const PREDECESSOR_RECIPE_SCHEMA_VERSION: u16 = 3;
const LEGACY_RECIPE_SCHEMA_VERSION: u16 = 2;
const QUARANTINE_SCHEMA_VERSION: u16 = 2;
const QUARANTINE_RECORD_KIND: &str = "provider_activation_quarantine";
const MAXIMUM_RECIPE_EVIDENCE_OBJECTS: usize = 1_024;
const ACTIVATION_STATE_DIRECTORY: &str = "sources/provider-activation-v1";

pub(super) const RESTORABLE_RESEARCH_SURFACES: [&str; 6] = [
    "sec.edgar-public",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
];
pub(super) const SERIALIZED_RESEARCH_SURFACES: [&str; 8] = [
    "sec.edgar-public",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
    "local.files",
    "local.portfolio-imports",
];

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
    /// Prior state was disabled and requires a new onboarding activation.
    Quarantined(DurableActivationQuarantine),
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

    /// Returns the one exact desired runtime session for every durable research surface.
    pub(super) fn startup_runtime_admissions(
        &self,
    ) -> Result<ProviderRuntimeStartupAdmissions, ProviderOnboardingError> {
        let mut entries = Vec::new();
        for surface_id in RESTORABLE_RESEARCH_SURFACES {
            let recovered = match self.load_recipe(surface_id) {
                Ok(DurableActivationRecipeState::Desired(recipe)) => {
                    vec![recipe.session_id]
                }
                Ok(DurableActivationRecipeState::Staged(recipe)) => {
                    let mut sessions = vec![recipe.session_id];
                    if let Some(predecessor) = recipe.staged_predecessor
                        && predecessor.session_id != recipe.session_id
                    {
                        sessions.push(predecessor.session_id);
                    }
                    sessions
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

    /// Stages a cross-session replacement while retaining the exact predecessor envelope.
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
        if predecessor.session_id == session_id
            || predecessor.staged_predecessor.is_some()
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
        let DurableActivationRecipeState::Staged(staged) = self.load_recipe(surface_id)? else {
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
        let DurableActivationRecipeState::Staged(recipe) = self.load_recipe(surface_id)? else {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        };
        if recipe.state_digest != expected_staged_digest {
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
        let key = surface_key(surface_id)?;
        let Some(encoded) = LocalAuthorityStateStore::try_open(self.recipe_root(key))?.load()?
        else {
            return Ok(DurableActivationRecipeState::Missing);
        };
        if let Ok(quarantine) = serde_json::from_slice::<QuarantineWire>(&encoded) {
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
        decode_recipe(surface_id, &encoded, true)
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

    pub(super) fn referenced_evidence_digests(
        &self,
    ) -> Result<std::collections::BTreeSet<String>, DurableProviderActivationStateError> {
        let mut referenced = std::collections::BTreeSet::new();
        for surface_id in RESTORABLE_RESEARCH_SURFACES {
            match self.load_recipe(surface_id)? {
                DurableActivationRecipeState::Missing => {}
                DurableActivationRecipeState::Desired(recipe)
                | DurableActivationRecipeState::Staged(recipe) => {
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
        _ => Err(DurableProviderActivationStateError::UnknownSurface),
    }
}

fn decode_recipe(
    surface_id: &str,
    encoded: &[u8],
    allow_staged_predecessor: bool,
) -> Result<DurableActivationRecipeState, DurableProviderActivationStateError> {
    let recipe: RecipeWire = serde_json::from_slice(encoded)
        .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
    if !matches!(
        recipe.schema_version,
        RECIPE_SCHEMA_VERSION | PREDECESSOR_RECIPE_SCHEMA_VERSION | LEGACY_RECIPE_SCHEMA_VERSION
    ) || recipe.surface_id != surface_id
        || recipe.request_json.is_empty()
        || recipe.evidence_digests.len() > MAXIMUM_RECIPE_EVIDENCE_OBJECTS
        || !strictly_ordered(&recipe.evidence_digests)
        || recipe.schema_version == LEGACY_RECIPE_SCHEMA_VERSION
            && recipe.publication_state != RecipePublicationState::Desired
        || recipe.schema_version != RECIPE_SCHEMA_VERSION
            && recipe.predecessor_recipe_json.is_some()
        || recipe.publication_state == RecipePublicationState::Desired
            && recipe.predecessor_recipe_json.is_some()
        || !allow_staged_predecessor && recipe.predecessor_recipe_json.is_some()
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
            if predecessor.session_id == recipe.session_id
                || Some(predecessor.runtime_generation_digest)
                    != predecessor_runtime_generation_digest
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
            && (publication_state != RecipePublicationState::Staged
                || predecessor_runtime_generation_digest.is_none())
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
                RecipePublicationState::Desired => 1,
            }]);
        }
        RECIPE_SCHEMA_VERSION => {
            hasher.update(b"market-squawk:durable-provider-activation:v4");
            hasher.update([match publication_state {
                RecipePublicationState::Staged => 0,
                RecipePublicationState::Desired => 1,
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
    if schema_version == RECIPE_SCHEMA_VERSION {
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
    #[error("provider activation evidence reclamation failed")]
    EvidenceReclamation(#[source] std::io::Error),
    #[error(transparent)]
    Store(#[from] LocalAuthorityStateStoreError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
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

    #[tokio::test]
    async fn superseded_recipe_quarantines_only_its_provider_during_product_startup() -> TestResult
    {
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
                    == DurableActivationQuarantineReason::RequestSuperseded
        ));
        Ok(())
    }
}
