//! Crash-safe, secret-free persistence for reconstructible research-provider activation.

use std::path::PathBuf;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

const RECIPE_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_EVIDENCE_OBJECTS: usize = 1_024;
const ACTIVATION_STATE_DIRECTORY: &str = "sources/provider-activation-v1";

pub(super) const RESTORABLE_RESEARCH_SURFACES: [&str; 6] = [
    "sec.edgar-public",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
    "fred-alfred.api-v1-v2",
];

/// Exact activation recipe recovered from crash-safe application-owned state.
pub(super) struct DurableActivationRecipe {
    pub(super) session_id: Uuid,
    pub(super) request_bytes: Box<[u8]>,
    pub(super) evidence_digests: Vec<String>,
}

/// Controlled persistence for activation recipes and their digest-addressed evidence objects.
#[derive(Clone)]
pub(super) struct DurableProviderActivationState {
    root: PathBuf,
}

impl DurableProviderActivationState {
    pub(super) fn new(control_root: PathBuf) -> Self {
        Self {
            root: control_root.join(ACTIVATION_STATE_DIRECTORY),
        }
    }

    pub(super) fn persist_evidence(
        &self,
        sha256: &str,
        bytes: &[u8],
    ) -> Result<(), DurableProviderActivationStateError> {
        validate_sha256(sha256)?;
        if sha256_bytes(bytes) != sha256 {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        let store = LocalAuthorityStateStore::try_open(self.evidence_root(sha256))?;
        match store.load()? {
            Some(existing) if existing == bytes => Ok(()),
            Some(_) => Err(DurableProviderActivationStateError::Integrity),
            None => store.store(bytes).map_err(Into::into),
        }
    }

    pub(super) fn load_evidence(
        &self,
        sha256: &str,
        maximum_bytes: u64,
    ) -> Result<StoredActivationEvidence, DurableProviderActivationStateError> {
        validate_sha256(sha256)?;
        let store = LocalAuthorityStateStore::try_open(self.evidence_root(sha256))?;
        let bytes = store
            .load()?
            .ok_or(DurableProviderActivationStateError::MissingEvidence)?;
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

    pub(super) fn persist_recipe(
        &self,
        surface_id: &str,
        session_id: Uuid,
        request_bytes: &[u8],
        evidence_digests: &[String],
    ) -> Result<(), DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        if request_bytes.is_empty() {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        let request_json = std::str::from_utf8(request_bytes)
            .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?
            .to_owned();
        let mut evidence_digests = evidence_digests.to_vec();
        evidence_digests.sort_unstable();
        evidence_digests.dedup();
        if evidence_digests.len() > MAXIMUM_EVIDENCE_OBJECTS {
            return Err(DurableProviderActivationStateError::ResourceExhausted);
        }
        for digest in &evidence_digests {
            validate_sha256(digest)?;
        }
        let request_sha256 = sha256_bytes(request_bytes);
        let bundle_sha256 =
            bundle_digest(surface_id, session_id, request_bytes, &evidence_digests)?;
        let recipe = RecipeWire {
            schema_version: RECIPE_SCHEMA_VERSION,
            surface_id: surface_id.to_owned(),
            session_id,
            request_sha256,
            evidence_digests,
            bundle_sha256,
            request_json,
        };
        let encoded = serde_json::to_vec(&recipe)
            .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
        LocalAuthorityStateStore::try_open(self.recipe_root(key))?
            .store(&encoded)
            .map_err(Into::into)
    }

    pub(super) fn load_recipe(
        &self,
        surface_id: &str,
    ) -> Result<Option<DurableActivationRecipe>, DurableProviderActivationStateError> {
        let key = surface_key(surface_id)?;
        let Some(encoded) = LocalAuthorityStateStore::try_open(self.recipe_root(key))?.load()?
        else {
            return Ok(None);
        };
        let recipe: RecipeWire = serde_json::from_slice(&encoded)
            .map_err(|_| DurableProviderActivationStateError::InvalidRecipe)?;
        if recipe.schema_version != RECIPE_SCHEMA_VERSION
            || recipe.surface_id != surface_id
            || recipe.request_json.is_empty()
            || recipe.evidence_digests.len() > MAXIMUM_EVIDENCE_OBJECTS
            || !strictly_ordered(&recipe.evidence_digests)
        {
            return Err(DurableProviderActivationStateError::InvalidRecipe);
        }
        for digest in &recipe.evidence_digests {
            validate_sha256(digest)?;
        }
        let request_bytes = recipe.request_json.into_bytes();
        if sha256_bytes(&request_bytes) != recipe.request_sha256
            || bundle_digest(
                surface_id,
                recipe.session_id,
                &request_bytes,
                &recipe.evidence_digests,
            )? != recipe.bundle_sha256
        {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        Ok(Some(DurableActivationRecipe {
            session_id: recipe.session_id,
            request_bytes: request_bytes.into_boxed_slice(),
            evidence_digests: recipe.evidence_digests,
        }))
    }

    fn recipe_root(&self, key: &str) -> PathBuf {
        self.root.join("recipes").join(key)
    }

    fn evidence_root(&self, sha256: &str) -> PathBuf {
        self.root.join("evidence").join(sha256)
    }
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
    bundle_sha256: String,
    request_json: String,
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

fn bundle_digest(
    surface_id: &str,
    session_id: Uuid,
    request_bytes: &[u8],
    evidence_digests: &[String],
) -> Result<String, DurableProviderActivationStateError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk:durable-provider-activation:v1");
    hash_field(&mut hasher, surface_id.as_bytes())?;
    hasher.update(session_id.as_bytes());
    hash_field(&mut hasher, request_bytes)?;
    let count = u16::try_from(evidence_digests.len())
        .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
    hasher.update(count.to_be_bytes());
    for digest in evidence_digests {
        hash_field(&mut hasher, digest.as_bytes())?;
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
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(DurableProviderActivationStateError::InvalidRecipe)
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
    #[error(transparent)]
    Store(#[from] LocalAuthorityStateStoreError),
}
