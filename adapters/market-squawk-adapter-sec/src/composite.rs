//! Complete SEC submissions convergence and deterministic composite evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::time::Duration;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_sources::ExtractionAuthority;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    RawEvidenceStore, RetrievedSecBytes, RetrievedSubmissions, SecClientError, SecEdgarSource,
    SecObjectLocator, SecParserError, SecParserLimits, SubmissionsArchive, SubmissionsDocument,
    reconcile_submissions, reconcile_submissions_with_cancellation,
};

const MAX_COMPANION_OBJECTS: u16 = 64;
const MAX_COMPOSITE_DECODED_BYTES: u64 = 512 * 1024 * 1024;
const MAX_COMPOSITE_MANIFEST_BYTES: usize = 512 * 1024;

/// Hard ceilings for one complete current-plus-historical submissions snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecCompositeBounds {
    max_companion_objects: u16,
    max_total_decoded_bytes: u64,
}

impl SecCompositeBounds {
    /// Conservative production bounds that still cover normal SEC companion histories.
    pub const fn production_defaults() -> Self {
        Self {
            max_companion_objects: MAX_COMPANION_OBJECTS,
            max_total_decoded_bytes: 128 * 1024 * 1024,
        }
    }

    /// Constructs explicit nonzero bounds within hard adapter limits.
    pub const fn try_new(
        max_companion_objects: u16,
        max_total_decoded_bytes: u64,
    ) -> Result<Self, SecClientError> {
        if max_companion_objects == 0
            || max_companion_objects > MAX_COMPANION_OBJECTS
            || max_total_decoded_bytes == 0
            || max_total_decoded_bytes > MAX_COMPOSITE_DECODED_BYTES
        {
            return Err(SecClientError::InvalidCompositeBounds);
        }
        Ok(Self {
            max_companion_objects,
            max_total_decoded_bytes,
        })
    }

    /// Returns the maximum number of provider-declared companion objects.
    pub const fn max_companion_objects(self) -> u16 {
        self.max_companion_objects
    }

    /// Returns the aggregate decoded response-byte ceiling.
    pub const fn max_total_decoded_bytes(self) -> u64 {
        self.max_total_decoded_bytes
    }
}

impl RetrievedSubmissions {
    /// Imports an exact current object and its complete declared companion set as Unknown history.
    pub fn import_exact_bytes(
        recent_bytes: &[u8],
        archive_bytes: &[&[u8]],
        raw_store: &RawEvidenceStore,
        parser_limits: SecParserLimits,
    ) -> Result<Self, SecClientError> {
        let bounds = SecCompositeBounds::production_defaults();
        let recent = SubmissionsDocument::parse(recent_bytes, parser_limits)?;
        if recent.companion_files().len() != archive_bytes.len()
            || archive_bytes.len() > usize::from(bounds.max_companion_objects)
        {
            return Err(SecClientError::InvalidCompanionSet);
        }
        let expected_prefix = format!("CIK{}-submissions-", recent.cik());
        let mut unique_names = BTreeSet::new();
        for name in recent.companion_files() {
            if !name.as_str().starts_with(&expected_prefix)
                || !unique_names.insert(name.as_str().to_owned())
            {
                return Err(SecClientError::InvalidCompanionSet);
            }
        }
        let mut total_bytes = u64::try_from(recent_bytes.len())
            .map_err(|_| SecClientError::CompositeByteLimitExceeded)?;
        ensure_total_bytes(total_bytes, bounds)?;
        let mut archives = Vec::new();
        archives
            .try_reserve(archive_bytes.len())
            .map_err(|_| SecClientError::AllocationFailed)?;
        let mut evidence_entries = Vec::new();
        evidence_entries
            .try_reserve(archive_bytes.len().saturating_add(1))
            .map_err(|_| SecClientError::AllocationFailed)?;
        let current_evidence = raw_store.persist(recent_bytes)?;
        evidence_entries.push(OfflineCompositeRepresentation {
            source_name: "current".to_owned(),
            evidence: current_evidence,
            size_bytes: u64::try_from(recent_bytes.len())
                .map_err(|_| SecClientError::CompositeByteLimitExceeded)?,
        });
        for (name, bytes) in recent.companion_files().iter().zip(archive_bytes) {
            total_bytes = total_bytes
                .checked_add(
                    u64::try_from(bytes.len())
                        .map_err(|_| SecClientError::CompositeByteLimitExceeded)?,
                )
                .ok_or(SecClientError::CompositeByteLimitExceeded)?;
            ensure_total_bytes(total_bytes, bounds)?;
            archives.push(SubmissionsDocument::parse_archive(bytes, parser_limits)?);
            evidence_entries.push(OfflineCompositeRepresentation {
                source_name: name.as_str().to_owned(),
                evidence: raw_store.persist(bytes)?,
                size_bytes: u64::try_from(bytes.len())
                    .map_err(|_| SecClientError::CompositeByteLimitExceeded)?,
            });
        }
        let reconciled = reconcile_submissions(&recent, &archives, parser_limits)?;
        let manifest = OfflineSubmissionsCompositeManifest {
            schema_version: "market-squawk-sec-offline-submissions-composite-v1",
            cik: reconciled.cik().as_str().to_owned(),
            representations: evidence_entries,
        };
        let cancellation = CancellationToken::new();
        let mut writer = CompositeManifestWriter::new(&cancellation);
        serde_json::to_writer(&mut writer, &manifest)
            .map_err(|_| SecClientError::CompositeSerialization)?;
        let manifest_bytes = writer.into_inner();
        let manifest_evidence = raw_store.persist(&manifest_bytes)?;
        let received_at = crate::client::system_timestamp()?;
        let mut components = Vec::new();
        components
            .try_reserve(archive_bytes.len().saturating_add(1))
            .map_err(|_| SecClientError::AllocationFailed)?;
        components.push(RetrievedSecBytes::offline_import(
            recent_bytes,
            current_evidence,
            received_at,
        ));
        for (bytes, entry) in archive_bytes
            .iter()
            .zip(manifest.representations.iter().skip(1))
        {
            components.push(RetrievedSecBytes::offline_import(
                bytes,
                entry.evidence,
                received_at,
            ));
        }
        Ok(Self::new(
            reconciled,
            RetrievedSecBytes::offline_import(&manifest_bytes, manifest_evidence, received_at),
            components,
        ))
    }
}

impl SecEdgarSource {
    /// Retrieves every provider-declared submissions companion and persists one composite manifest.
    pub async fn fetch_complete_submissions(
        &self,
        authority: &ExtractionAuthority,
        cik: &str,
        bounds: SecCompositeBounds,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<RetrievedSubmissions, SecClientError> {
        self.validate_authority(authority)?;
        let recent_cancellation = cancellation.child_token();
        let remaining = deadline_remaining(deadline)?;
        let recent = tokio::select! {
            result = self.fetch_submissions(authority, cik, recent_cancellation.clone()) => result?,
            () = tokio::time::sleep(remaining) => {
                recent_cancellation.cancel();
                return Err(SecClientError::DeadlineExceeded);
            }
            () = cancellation.cancelled() => {
                recent_cancellation.cancel();
                return Err(SecClientError::Cancelled);
            }
        };
        let expected_prefix = format!("CIK{}-submissions-", recent.document().cik());
        let companion_names = recent.document().companion_files();
        if companion_names.len() > usize::from(bounds.max_companion_objects) {
            return Err(SecClientError::CompanionObjectLimitExceeded);
        }
        let mut unique_names = BTreeSet::new();
        for name in companion_names {
            if !name.as_str().starts_with(&expected_prefix)
                || !unique_names.insert(name.as_str().to_owned())
            {
                return Err(SecClientError::InvalidCompanionSet);
            }
        }

        let recent_document = recent.document().clone();
        let mut total_bytes = response_size(recent.raw())?;
        ensure_total_bytes(total_bytes, bounds)?;
        let component_count = companion_names
            .len()
            .checked_add(1)
            .ok_or(SecClientError::CompanionObjectLimitExceeded)?;
        let mut components = Vec::new();
        components
            .try_reserve(component_count)
            .map_err(|_| SecClientError::AllocationFailed)?;
        components.push(recent.raw().clone());
        let mut archives = Vec::<SubmissionsArchive>::new();
        archives
            .try_reserve(companion_names.len())
            .map_err(|_| SecClientError::AllocationFailed)?;

        for name in companion_names {
            let attempt_cancellation = cancellation.child_token();
            let remaining = deadline_remaining(deadline)?;
            let (archive, raw) = tokio::select! {
                result = self.fetch_submissions_archive(
                    authority,
                    name.as_str(),
                    attempt_cancellation.clone(),
                ) => result?,
                () = tokio::time::sleep(remaining) => {
                    attempt_cancellation.cancel();
                    return Err(SecClientError::DeadlineExceeded);
                }
                () = cancellation.cancelled() => {
                    attempt_cancellation.cancel();
                    return Err(SecClientError::Cancelled);
                }
            };
            total_bytes = total_bytes
                .checked_add(response_size(&raw)?)
                .ok_or(SecClientError::CompositeByteLimitExceeded)?;
            ensure_total_bytes(total_bytes, bounds)?;
            archives.push(archive);
            components.push(raw);
        }

        let limits = self.parser_limits();
        let reconciled = self
            .run_validation_blocking(&cancellation, move |worker_cancellation| {
                reconcile_submissions_with_cancellation(
                    &recent_document,
                    &archives,
                    limits,
                    worker_cancellation,
                )
                .map_err(Into::into)
            })
            .await?;
        self.validate_authority(authority)?;
        let manifest_raw = persist_manifest(
            self,
            reconciled.cik().as_str().to_owned(),
            components.clone(),
            &cancellation,
        )
        .await?;
        self.validate_authority(authority)?;
        Ok(RetrievedSubmissions::new(
            reconciled,
            manifest_raw,
            components,
        ))
    }
}

pub(crate) fn restore_online_submissions(
    raw_store: &RawEvidenceStore,
    manifest_bytes: &[u8],
    manifest_evidence: EvidenceDigest,
    bounds: SecCompositeBounds,
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<RetrievedSubmissions, SecClientError> {
    if manifest_bytes.len() > MAX_COMPOSITE_MANIFEST_BYTES
        || manifest_evidence.algorithm() != DigestAlgorithm::Sha256
        || <[u8; 32]>::from(Sha256::digest(manifest_bytes)) != manifest_evidence.bytes()
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let manifest_limits = SecParserLimits::try_new(
        MAX_COMPOSITE_MANIFEST_BYTES,
        usize::from(MAX_COMPANION_OBJECTS) + 1,
        16,
        4 * 1024,
        256 * 1024,
        1024 * 1024,
    )?;
    let value = crate::json::parse_bounded_json_with_cancellation(
        manifest_bytes,
        manifest_limits,
        cancellation,
    )?;
    let manifest: OnlineSubmissionsCompositeManifestWire =
        serde_json::from_value(value).map_err(SecParserError::from)?;
    if manifest.schema_version != "market-squawk-sec-submissions-composite-v1"
        || manifest.representations.is_empty()
        || manifest.representations.len() > usize::from(bounds.max_companion_objects) + 1
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let current_locator = SecObjectLocator::submissions(&manifest.cik)?;
    let mut by_locator = BTreeMap::new();
    for representation in manifest.representations {
        validate_online_representation(&representation, bounds)?;
        if by_locator
            .insert(representation.locator.clone(), representation)
            .is_some()
        {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
    }
    let current = by_locator
        .remove(current_locator.url())
        .ok_or(SecClientError::InvalidCompositeRepresentation)?;
    let mut total_bytes = current.size_bytes;
    ensure_total_bytes(total_bytes, bounds)?;
    let current_bytes = read_component(raw_store, &current, cancellation)?;
    let current_document =
        SubmissionsDocument::parse_with_cancellation(&current_bytes, parser_limits, cancellation)?;
    if current_document.cik().as_str() != manifest.cik {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    if current_document.companion_files().len() > usize::from(bounds.max_companion_objects) {
        return Err(SecClientError::CompanionObjectLimitExceeded);
    }
    let mut components = Vec::new();
    components
        .try_reserve(current_document.companion_files().len().saturating_add(1))
        .map_err(|_| SecClientError::AllocationFailed)?;
    let mut available_at = current.first_observed_at;
    components.push(restored_component(current_bytes, current));
    let mut archives = Vec::new();
    archives
        .try_reserve(current_document.companion_files().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    for name in current_document.companion_files() {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        let locator = SecObjectLocator::companion(name.as_str())?;
        let representation = by_locator
            .remove(locator.url())
            .ok_or(SecClientError::InvalidCompositeRepresentation)?;
        total_bytes = total_bytes
            .checked_add(representation.size_bytes)
            .ok_or(SecClientError::CompositeByteLimitExceeded)?;
        ensure_total_bytes(total_bytes, bounds)?;
        available_at = available_at.max(representation.first_observed_at);
        let bytes = read_component(raw_store, &representation, cancellation)?;
        archives.push(SubmissionsDocument::parse_archive_with_cancellation(
            &bytes,
            parser_limits,
            cancellation,
        )?);
        components.push(restored_component(bytes, representation));
    }
    if !by_locator.is_empty() {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let reconciled = reconcile_submissions_with_cancellation(
        &current_document,
        &archives,
        parser_limits,
        cancellation,
    )?;
    let mut retained_manifest = Vec::new();
    retained_manifest
        .try_reserve(manifest_bytes.len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    retained_manifest.extend_from_slice(manifest_bytes);
    Ok(RetrievedSubmissions::new(
        reconciled,
        RetrievedSecBytes::local_composite(retained_manifest, manifest_evidence, available_at),
        components,
    ))
}

fn validate_online_representation(
    representation: &OnlineCompositeRepresentationWire,
    bounds: SecCompositeBounds,
) -> Result<(), SecClientError> {
    if representation.size_bytes == 0
        || representation.size_bytes > bounds.max_total_decoded_bytes
        || representation.evidence.algorithm() != DigestAlgorithm::Sha256
        || representation.retrieval_revision == 0
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let parsed = url::Url::parse(&representation.locator)
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("data.sec.gov")
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.as_str() != representation.locator
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    Ok(())
}

fn read_component(
    raw_store: &RawEvidenceStore,
    representation: &OnlineCompositeRepresentationWire,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, SecClientError> {
    let bytes = raw_store.read_verified_bounded_cancellable(
        &representation.evidence,
        representation.size_bytes,
        cancellation,
    )?;
    if u64::try_from(bytes.len()).ok() != Some(representation.size_bytes) {
        return Err(SecClientError::RawEvidenceMismatch);
    }
    Ok(bytes)
}

fn restored_component(
    bytes: Vec<u8>,
    representation: OnlineCompositeRepresentationWire,
) -> RetrievedSecBytes {
    RetrievedSecBytes::restored_online(
        bytes,
        representation.evidence,
        representation.first_observed_at,
        representation.locator,
        representation.retrieval_revision,
    )
}

async fn persist_manifest(
    source: &SecEdgarSource,
    cik: String,
    components: Vec<RetrievedSecBytes>,
    cancellation: &CancellationToken,
) -> Result<RetrievedSecBytes, SecClientError> {
    let raw_store = source.raw_store();
    source
        .run_blocking(cancellation, move |worker_cancellation| {
            let mut entries = Vec::new();
            entries
                .try_reserve(components.len())
                .map_err(|_| SecClientError::AllocationFailed)?;
            let mut available_at: Option<Timestamp> = None;
            for component in components {
                if worker_cancellation.is_cancelled() {
                    return Err(SecClientError::Cancelled);
                }
                let locator = component
                    .locator()
                    .ok_or(SecClientError::InvalidCompositeRepresentation)?
                    .to_owned();
                let retrieval_revision = component
                    .retrieval_revision()
                    .ok_or(SecClientError::InvalidCompositeRepresentation)?;
                let observed_at = component.received_at();
                available_at =
                    Some(available_at.map_or(observed_at, |current| current.max(observed_at)));
                entries.push(CompositeRepresentation {
                    locator,
                    evidence: component.evidence(),
                    size_bytes: response_size(&component)?,
                    first_observed_at: observed_at,
                    retrieval_revision,
                });
            }
            entries.sort_by(|left, right| left.locator.cmp(&right.locator));
            let available_at =
                available_at.ok_or(SecClientError::InvalidCompositeRepresentation)?;
            let manifest = SubmissionsCompositeManifest {
                schema_version: "market-squawk-sec-submissions-composite-v1",
                cik,
                representations: entries,
            };
            let mut writer = CompositeManifestWriter::new(worker_cancellation);
            if serde_json::to_writer(&mut writer, &manifest).is_err() {
                return if worker_cancellation.is_cancelled() {
                    Err(SecClientError::Cancelled)
                } else {
                    Err(SecClientError::CompositeSerialization)
                };
            }
            let bytes = writer.into_inner();
            let evidence = raw_store.persist_cancellable(&bytes, worker_cancellation)?;
            Ok(RetrievedSecBytes::local_composite(
                bytes,
                evidence,
                available_at,
            ))
        })
        .await
}

fn response_size(raw: &RetrievedSecBytes) -> Result<u64, SecClientError> {
    u64::try_from(raw.bytes().len()).map_err(|_| SecClientError::CompositeByteLimitExceeded)
}

fn ensure_total_bytes(total: u64, bounds: SecCompositeBounds) -> Result<(), SecClientError> {
    if total > bounds.max_total_decoded_bytes {
        Err(SecClientError::CompositeByteLimitExceeded)
    } else {
        Ok(())
    }
}

fn deadline_remaining(deadline: Timestamp) -> Result<Duration, SecClientError> {
    let remaining = deadline
        .unix_nanos()
        .saturating_sub(crate::client::system_timestamp()?.unix_nanos());
    if remaining <= 0 {
        Err(SecClientError::DeadlineExceeded)
    } else {
        u64::try_from(remaining)
            .map(Duration::from_nanos)
            .map_err(|_| SecClientError::DeadlineExceeded)
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SubmissionsCompositeManifest {
    schema_version: &'static str,
    cik: String,
    representations: Vec<CompositeRepresentation>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct CompositeRepresentation {
    locator: String,
    evidence: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    retrieval_revision: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OnlineSubmissionsCompositeManifestWire {
    schema_version: String,
    cik: String,
    representations: Vec<OnlineCompositeRepresentationWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OnlineCompositeRepresentationWire {
    locator: String,
    evidence: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    retrieval_revision: u64,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct OfflineSubmissionsCompositeManifest {
    schema_version: &'static str,
    cik: String,
    representations: Vec<OfflineCompositeRepresentation>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct OfflineCompositeRepresentation {
    source_name: String,
    evidence: EvidenceDigest,
    size_bytes: u64,
}

struct CompositeManifestWriter<'a> {
    bytes: Vec<u8>,
    cancellation: &'a CancellationToken,
}

impl<'a> CompositeManifestWriter<'a> {
    const fn new(cancellation: &'a CancellationToken) -> Self {
        Self {
            bytes: Vec::new(),
            cancellation,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for CompositeManifestWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "SEC composite manifest cancelled",
            ));
        }
        let new_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("SEC composite manifest is too large"))?;
        if new_len > MAX_COMPOSITE_MANIFEST_BYTES {
            return Err(std::io::Error::other("SEC composite manifest is too large"));
        }
        self.bytes
            .try_reserve(buffer.len())
            .map_err(|_| std::io::Error::other("SEC composite allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use cap_std::{ambient_authority, fs::Dir};

    use super::*;

    #[test]
    fn online_composite_restores_every_exact_declared_component()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let store = RawEvidenceStore::new(Dir::open_ambient_dir(
            temporary.path(),
            ambient_authority(),
        )?);
        let recent = include_bytes!("../fixtures/submissions-recent.json");
        let archive = include_bytes!("../fixtures/submissions-archive.json");
        let observed_at = Timestamp::from_unix_nanos(100);
        let manifest = SubmissionsCompositeManifest {
            schema_version: "market-squawk-sec-submissions-composite-v1",
            cik: "0000320193".to_owned(),
            representations: vec![
                CompositeRepresentation {
                    locator: SecObjectLocator::submissions("320193")?.url().to_owned(),
                    evidence: store.persist(recent)?,
                    size_bytes: u64::try_from(recent.len())?,
                    first_observed_at: observed_at,
                    retrieval_revision: 1,
                },
                CompositeRepresentation {
                    locator: SecObjectLocator::companion("CIK0000320193-submissions-001.json")?
                        .url()
                        .to_owned(),
                    evidence: store.persist(archive)?,
                    size_bytes: u64::try_from(archive.len())?,
                    first_observed_at: observed_at,
                    retrieval_revision: 1,
                },
            ],
        };
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        let manifest_evidence = store.persist(&manifest_bytes)?;
        let restored = restore_online_submissions(
            &store,
            &manifest_bytes,
            manifest_evidence,
            SecCompositeBounds::production_defaults(),
            SecParserLimits::production_defaults(),
            &CancellationToken::new(),
        )?;
        assert_eq!(restored.document().filings().len(), 3);
        assert_eq!(restored.components().len(), 2);
        Ok(())
    }
}
