//! Bounded, indexed ownership of durable provider-activation evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::io;

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_platform::LocalAuthorityStateStore;
use serde::{Deserialize, Serialize};

use super::{
    DurableProviderActivationState, DurableProviderActivationStateError, sha256_bytes,
    validate_sha256,
};

const EVIDENCE_INDEX_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_EVIDENCE_OBJECTS: usize = 6 * 1_024;
const MAXIMUM_EVIDENCE_OBJECT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_EVIDENCE_AGGREGATE_BYTES: u64 = 128 * 1024 * 1024;

/// One caller-loaded evidence object whose digest was already bound into a staged recipe.
#[derive(Clone, Copy, Debug)]
pub(in crate::local_product) struct ActivationEvidenceCandidate<'a> {
    pub(in crate::local_product) sha256: &'a str,
    pub(in crate::local_product) bytes: &'a [u8],
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum EvidenceObjectState {
    Pending,
    Ready,
    Deleting,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EvidenceIndex {
    schema_version: u16,
    objects: Vec<EvidenceIndexEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EvidenceIndexEntry {
    sha256: String,
    bytes: u64,
    state: EvidenceObjectState,
}

impl DurableProviderActivationState {
    pub(super) fn evidence_backup_target_is_absent(
        &self,
    ) -> Result<bool, DurableProviderActivationStateError> {
        let index_absent = LocalAuthorityStateStore::try_open(self.evidence_index_root())?
            .load()?
            .is_none();
        Ok(index_absent && self.inventory_evidence_objects()?.is_empty())
    }

    /// Persists one complete evidence bundle under aggregate count and byte ceilings.
    ///
    /// Every candidate must already be named by a staged, desired, or quarantined recipe. This
    /// ordering makes crash recovery deterministic: a partially written object is either still
    /// referenced and completed on retry, or is reclaimed as unreferenced debt.
    pub(in crate::local_product) fn persist_evidence_bundle(
        &self,
        candidates: &[ActivationEvidenceCandidate<'_>],
    ) -> Result<(), DurableProviderActivationStateError> {
        let referenced = self.referenced_evidence_digests()?;
        let candidates = canonical_candidates(candidates, &referenced)?;
        let incoming = candidates.keys().cloned().collect::<BTreeSet<_>>();
        let store = LocalAuthorityStateStore::try_open(self.evidence_index_root())?;
        let mut index = load_index(&store)?;
        self.reconcile_index(&store, &mut index, &referenced, &incoming)?;

        for (sha256, bytes) in candidates {
            if let Some(existing) = index.objects.get(&sha256) {
                let length = u64::try_from(bytes.len())
                    .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
                match existing.state {
                    EvidenceObjectState::Ready if existing.bytes == length => {
                        let retained = self.read_evidence_object(&sha256, existing.bytes)?;
                        if retained != bytes {
                            return Err(DurableProviderActivationStateError::Integrity);
                        }
                        continue;
                    }
                    EvidenceObjectState::Pending
                        if existing.bytes == 0 || existing.bytes == length =>
                    {
                        enforce_replacement_budget(&index.objects, &sha256, length)?;
                        index
                            .objects
                            .get_mut(&sha256)
                            .ok_or(DurableProviderActivationStateError::Integrity)?
                            .bytes = length;
                        store_index(&store, &index)?;
                    }
                    _ => return Err(DurableProviderActivationStateError::Integrity),
                }
            } else {
                return Err(DurableProviderActivationStateError::Integrity);
            }

            LocalAuthorityStateStore::try_open(self.evidence_object_root(&sha256))?.store(bytes)?;
            let entry = index
                .objects
                .get_mut(&sha256)
                .ok_or(DurableProviderActivationStateError::Integrity)?;
            entry.state = EvidenceObjectState::Ready;
            store_index(&store, &index)?;
        }
        Ok(())
    }

    /// Reclaims every indexed evidence object no longer retained by durable activation state.
    pub(in crate::local_product) fn reconcile_evidence_objects(
        &self,
    ) -> Result<(), DurableProviderActivationStateError> {
        let referenced = self.referenced_evidence_digests()?;
        let store = LocalAuthorityStateStore::try_open(self.evidence_index_root())?;
        let mut index = load_index(&store)?;
        self.reconcile_index(&store, &mut index, &referenced, &BTreeSet::new())
    }

    pub(super) fn load_indexed_evidence(
        &self,
        sha256: &str,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, DurableProviderActivationStateError> {
        validate_sha256(sha256)?;
        let store = LocalAuthorityStateStore::try_open(self.evidence_index_root())?;
        let index = load_index(&store)?;
        let entry = index
            .objects
            .get(sha256)
            .ok_or(DurableProviderActivationStateError::MissingEvidence)?;
        if entry.state != EvidenceObjectState::Ready
            || entry.bytes > maximum_bytes
            || entry.bytes > MAXIMUM_EVIDENCE_OBJECT_BYTES
        {
            return Err(DurableProviderActivationStateError::MissingEvidence);
        }
        self.read_evidence_object(sha256, entry.bytes)
    }

    fn reconcile_index(
        &self,
        store: &LocalAuthorityStateStore,
        index: &mut CanonicalEvidenceIndex,
        referenced: &BTreeSet<String>,
        incoming: &BTreeSet<String>,
    ) -> Result<(), DurableProviderActivationStateError> {
        let mut missing_referenced_evidence = false;
        let physical_objects = self.inventory_evidence_objects()?;
        let digests = index.objects.keys().cloned().collect::<Vec<_>>();
        for sha256 in digests {
            let entry = index
                .objects
                .get(&sha256)
                .cloned()
                .ok_or(DurableProviderActivationStateError::Integrity)?;
            match entry.state {
                EvidenceObjectState::Ready => {
                    if referenced.contains(&sha256) {
                        match self.read_evidence_object(&sha256, entry.bytes) {
                            Ok(_bytes) => {}
                            Err(DurableProviderActivationStateError::MissingEvidence) => {
                                index
                                    .objects
                                    .get_mut(&sha256)
                                    .ok_or(DurableProviderActivationStateError::Integrity)?
                                    .state = EvidenceObjectState::Pending;
                                store_index(store, index)?;
                                if !incoming.contains(&sha256) {
                                    missing_referenced_evidence = true;
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    } else {
                        self.mark_and_delete(store, index, &sha256)?;
                    }
                }
                EvidenceObjectState::Pending => {
                    if entry.bytes == 0 {
                        if referenced.contains(&sha256) {
                            if !incoming.contains(&sha256) {
                                missing_referenced_evidence = true;
                            }
                        } else {
                            self.mark_and_delete(store, index, &sha256)?;
                        }
                        continue;
                    }
                    match self.read_evidence_object(&sha256, entry.bytes) {
                        Ok(_bytes) if referenced.contains(&sha256) => {
                            index
                                .objects
                                .get_mut(&sha256)
                                .ok_or(DurableProviderActivationStateError::Integrity)?
                                .state = EvidenceObjectState::Ready;
                            store_index(store, index)?;
                        }
                        Ok(_bytes) => self.mark_and_delete(store, index, &sha256)?,
                        Err(DurableProviderActivationStateError::MissingEvidence) => {
                            if referenced.contains(&sha256) {
                                if !incoming.contains(&sha256) {
                                    missing_referenced_evidence = true;
                                }
                            } else {
                                self.mark_and_delete(store, index, &sha256)?;
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
                EvidenceObjectState::Deleting => {
                    if referenced.contains(&sha256) {
                        return Err(DurableProviderActivationStateError::Integrity);
                    }
                    self.delete_evidence_object(&sha256)?;
                    index.objects.remove(&sha256);
                    store_index(store, index)?;
                }
            }
        }

        for sha256 in physical_objects {
            if !referenced.contains(&sha256) && !index.objects.contains_key(&sha256) {
                self.delete_evidence_object(&sha256)?;
            }
        }

        let mut index_changed = false;
        for sha256 in referenced {
            if index.objects.contains_key(sha256) {
                continue;
            }
            match self.read_unindexed_evidence_object(sha256)? {
                Some(bytes) => {
                    let length = u64::try_from(bytes.len())
                        .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
                    enforce_addition_budget(&index.objects, length)?;
                    index.objects.insert(
                        sha256.clone(),
                        EvidenceIndexEntry {
                            sha256: sha256.clone(),
                            bytes: length,
                            state: EvidenceObjectState::Ready,
                        },
                    );
                    index_changed = true;
                }
                None => {
                    enforce_addition_budget(&index.objects, 0)?;
                    index.objects.insert(
                        sha256.clone(),
                        EvidenceIndexEntry {
                            sha256: sha256.clone(),
                            bytes: 0,
                            state: EvidenceObjectState::Pending,
                        },
                    );
                    index_changed = true;
                    if !incoming.contains(sha256) {
                        missing_referenced_evidence = true;
                    }
                }
            }
        }
        if index_changed {
            store_index(store, index)?;
        }
        if missing_referenced_evidence {
            return Err(DurableProviderActivationStateError::MissingEvidence);
        }
        Ok(())
    }

    fn mark_and_delete(
        &self,
        store: &LocalAuthorityStateStore,
        index: &mut CanonicalEvidenceIndex,
        sha256: &str,
    ) -> Result<(), DurableProviderActivationStateError> {
        index
            .objects
            .get_mut(sha256)
            .ok_or(DurableProviderActivationStateError::Integrity)?
            .state = EvidenceObjectState::Deleting;
        store_index(store, index)?;
        self.delete_evidence_object(sha256)?;
        index.objects.remove(sha256);
        store_index(store, index)
    }

    fn read_evidence_object(
        &self,
        sha256: &str,
        expected_bytes: u64,
    ) -> Result<Vec<u8>, DurableProviderActivationStateError> {
        validate_sha256(sha256)?;
        if expected_bytes == 0 || expected_bytes > MAXIMUM_EVIDENCE_OBJECT_BYTES {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        let Some(bytes) =
            LocalAuthorityStateStore::try_open(self.evidence_object_root(sha256))?.load()?
        else {
            return Err(DurableProviderActivationStateError::MissingEvidence);
        };
        let length = u64::try_from(bytes.len())
            .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
        if length != expected_bytes || sha256_bytes(&bytes) != sha256 {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        Ok(bytes)
    }

    fn read_unindexed_evidence_object(
        &self,
        sha256: &str,
    ) -> Result<Option<Vec<u8>>, DurableProviderActivationStateError> {
        validate_sha256(sha256)?;
        let Some(bytes) =
            LocalAuthorityStateStore::try_open(self.evidence_object_root(sha256))?.load()?
        else {
            return Ok(None);
        };
        let length = u64::try_from(bytes.len())
            .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
        if length == 0 || length > MAXIMUM_EVIDENCE_OBJECT_BYTES || sha256_bytes(&bytes) != sha256 {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        Ok(Some(bytes))
    }

    fn inventory_evidence_objects(
        &self,
    ) -> Result<BTreeSet<String>, DurableProviderActivationStateError> {
        let root = self.root.join("evidence");
        let directory = match Dir::open_ambient_dir(&root, ambient_authority()) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(BTreeSet::new());
            }
            Err(error) => {
                return Err(DurableProviderActivationStateError::EvidenceReclamation(
                    error,
                ));
            }
        };
        let mut objects = BTreeSet::new();
        for entry in directory
            .entries()
            .map_err(DurableProviderActivationStateError::EvidenceReclamation)?
        {
            let entry = entry.map_err(DurableProviderActivationStateError::EvidenceReclamation)?;
            if objects.len() >= MAXIMUM_EVIDENCE_OBJECTS {
                return Err(DurableProviderActivationStateError::ResourceExhausted);
            }
            let sha256 = entry
                .file_name()
                .into_string()
                .map_err(|_| DurableProviderActivationStateError::Integrity)?;
            validate_sha256(&sha256)?;
            let file_type = entry
                .file_type()
                .map_err(DurableProviderActivationStateError::EvidenceReclamation)?;
            if !file_type.is_dir() || file_type.is_symlink() || !objects.insert(sha256) {
                return Err(DurableProviderActivationStateError::Integrity);
            }
        }
        Ok(objects)
    }

    fn delete_evidence_object(
        &self,
        sha256: &str,
    ) -> Result<(), DurableProviderActivationStateError> {
        validate_sha256(sha256)?;
        let root = self.root.join("evidence");
        let directory = match Dir::open_ambient_dir(&root, ambient_authority()) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(DurableProviderActivationStateError::EvidenceReclamation(
                    error,
                ));
            }
        };
        match directory.remove_dir_all(sha256) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(DurableProviderActivationStateError::EvidenceReclamation(
                error,
            )),
        }
    }

    fn evidence_index_root(&self) -> std::path::PathBuf {
        self.root.join("evidence-index")
    }

    fn evidence_object_root(&self, sha256: &str) -> std::path::PathBuf {
        self.root.join("evidence").join(sha256)
    }
}

struct CanonicalEvidenceIndex {
    objects: BTreeMap<String, EvidenceIndexEntry>,
}

fn canonical_candidates<'a>(
    candidates: &'a [ActivationEvidenceCandidate<'a>],
    referenced: &BTreeSet<String>,
) -> Result<BTreeMap<String, &'a [u8]>, DurableProviderActivationStateError> {
    if candidates.len() > MAXIMUM_EVIDENCE_OBJECTS {
        return Err(DurableProviderActivationStateError::ResourceExhausted);
    }
    let mut canonical = BTreeMap::new();
    let mut aggregate = 0_u64;
    for candidate in candidates {
        validate_sha256(candidate.sha256)?;
        let length = u64::try_from(candidate.bytes.len())
            .map_err(|_| DurableProviderActivationStateError::ResourceExhausted)?;
        if length == 0
            || length > MAXIMUM_EVIDENCE_OBJECT_BYTES
            || sha256_bytes(candidate.bytes) != candidate.sha256
            || !referenced.contains(candidate.sha256)
        {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        aggregate = aggregate
            .checked_add(length)
            .ok_or(DurableProviderActivationStateError::ResourceExhausted)?;
        if aggregate > MAXIMUM_EVIDENCE_AGGREGATE_BYTES {
            return Err(DurableProviderActivationStateError::ResourceExhausted);
        }
        match canonical.get(candidate.sha256) {
            Some(existing) if *existing != candidate.bytes => {
                return Err(DurableProviderActivationStateError::Integrity);
            }
            Some(_) => {}
            None => {
                canonical.insert(candidate.sha256.to_owned(), candidate.bytes);
            }
        }
    }
    Ok(canonical)
}

fn load_index(
    store: &LocalAuthorityStateStore,
) -> Result<CanonicalEvidenceIndex, DurableProviderActivationStateError> {
    let Some(encoded) = store.load()? else {
        return Ok(CanonicalEvidenceIndex {
            objects: BTreeMap::new(),
        });
    };
    let wire = serde_json::from_slice::<EvidenceIndex>(&encoded)
        .map_err(|_| DurableProviderActivationStateError::Integrity)?;
    if wire.schema_version != EVIDENCE_INDEX_SCHEMA_VERSION
        || wire.objects.len() > MAXIMUM_EVIDENCE_OBJECTS
    {
        return Err(DurableProviderActivationStateError::Integrity);
    }
    let mut objects = BTreeMap::new();
    for entry in wire.objects {
        validate_sha256(&entry.sha256)?;
        if (entry.bytes == 0 && entry.state != EvidenceObjectState::Pending)
            || entry.bytes > MAXIMUM_EVIDENCE_OBJECT_BYTES
            || objects.insert(entry.sha256.clone(), entry).is_some()
        {
            return Err(DurableProviderActivationStateError::Integrity);
        }
    }
    let index = CanonicalEvidenceIndex { objects };
    validate_index(&index)?;
    Ok(index)
}

fn store_index(
    store: &LocalAuthorityStateStore,
    index: &CanonicalEvidenceIndex,
) -> Result<(), DurableProviderActivationStateError> {
    validate_index(index)?;
    let encoded = serde_json::to_vec(&EvidenceIndex {
        schema_version: EVIDENCE_INDEX_SCHEMA_VERSION,
        objects: index.objects.values().cloned().collect(),
    })
    .map_err(|_| DurableProviderActivationStateError::Integrity)?;
    store.store(&encoded)?;
    Ok(())
}

fn validate_index(
    index: &CanonicalEvidenceIndex,
) -> Result<(), DurableProviderActivationStateError> {
    if index.objects.len() > MAXIMUM_EVIDENCE_OBJECTS {
        return Err(DurableProviderActivationStateError::ResourceExhausted);
    }
    let total = index.objects.values().try_fold(0_u64, |total, entry| {
        if entry.sha256.is_empty()
            || (entry.bytes == 0 && entry.state != EvidenceObjectState::Pending)
            || entry.bytes > MAXIMUM_EVIDENCE_OBJECT_BYTES
        {
            return Err(DurableProviderActivationStateError::Integrity);
        }
        total
            .checked_add(entry.bytes)
            .ok_or(DurableProviderActivationStateError::ResourceExhausted)
    })?;
    if total > MAXIMUM_EVIDENCE_AGGREGATE_BYTES {
        return Err(DurableProviderActivationStateError::ResourceExhausted);
    }
    Ok(())
}

fn enforce_addition_budget(
    objects: &BTreeMap<String, EvidenceIndexEntry>,
    additional_bytes: u64,
) -> Result<(), DurableProviderActivationStateError> {
    if objects.len() >= MAXIMUM_EVIDENCE_OBJECTS {
        return Err(DurableProviderActivationStateError::ResourceExhausted);
    }
    let total = objects
        .values()
        .try_fold(additional_bytes, |total, entry| {
            total
                .checked_add(entry.bytes)
                .ok_or(DurableProviderActivationStateError::ResourceExhausted)
        })?;
    if total > MAXIMUM_EVIDENCE_AGGREGATE_BYTES {
        return Err(DurableProviderActivationStateError::ResourceExhausted);
    }
    Ok(())
}

fn enforce_replacement_budget(
    objects: &BTreeMap<String, EvidenceIndexEntry>,
    sha256: &str,
    replacement_bytes: u64,
) -> Result<(), DurableProviderActivationStateError> {
    let total = objects
        .iter()
        .try_fold(replacement_bytes, |total, (digest, entry)| {
            if digest == sha256 {
                return Ok(total);
            }
            total
                .checked_add(entry.bytes)
                .ok_or(DurableProviderActivationStateError::ResourceExhausted)
        })?;
    if total > MAXIMUM_EVIDENCE_AGGREGATE_BYTES {
        return Err(DurableProviderActivationStateError::ResourceExhausted);
    }
    Ok(())
}
