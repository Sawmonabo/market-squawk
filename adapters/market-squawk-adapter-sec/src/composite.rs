//! Complete SEC submissions convergence and deterministic composite evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    ExtractionAuthority, MAX_PROVIDER_CAPTURE_BYTES, MAX_PROVIDER_CAPTURE_PAGES,
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    RawEvidenceStore, RetrievedSecBytes, RetrievedSubmissions, SecClientError, SecEdgarSource,
    SecObjectLocator, SecParserError, SecParserLimits, SubmissionsArchive, SubmissionsDocument,
    reconcile_submissions, reconcile_submissions_with_cancellation,
};

const MAX_COMPANION_OBJECTS: u16 = MAX_PROVIDER_CAPTURE_PAGES as u16 - 1;
const MAX_COMPOSITE_DECODED_BYTES: u64 = MAX_PROVIDER_CAPTURE_BYTES;
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
            max_total_decoded_bytes: MAX_PROVIDER_CAPTURE_BYTES,
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

    /// Returns exact body-only capture material for current submissions plus every declared
    /// companion in provider order.
    ///
    /// A direct current-only fetch is a standalone capture. A complete fetch is terminal only
    /// after every provider-declared companion is present. Offline imports return `None` because
    /// they have no HTTP receipt and must not be represented as provider responses.
    pub fn capture_material(&self) -> Result<Option<ProviderCaptureMaterial>, SecClientError> {
        if self.components().is_empty() {
            return self.raw().capture_material();
        }
        complete_submissions_capture_material(self)
    }
}

fn complete_submissions_capture_material(
    retrieved: &RetrievedSubmissions,
) -> Result<Option<ProviderCaptureMaterial>, SecClientError> {
    let expected_count = retrieved
        .document()
        .companion_files()
        .len()
        .checked_add(1)
        .ok_or(SecClientError::CompanionObjectLimitExceeded)?;
    if expected_count != retrieved.components().len() || expected_count > MAX_PROVIDER_CAPTURE_PAGES
    {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let current_locator = SecObjectLocator::submissions(retrieved.document().cik().as_str())?;
    let mut expected_locators = Vec::new();
    expected_locators
        .try_reserve_exact(expected_count)
        .map_err(|_| SecClientError::AllocationFailed)?;
    expected_locators.push(current_locator.url().to_owned());
    for name in retrieved.document().companion_files() {
        expected_locators.push(SecObjectLocator::companion(name.as_str())?.url().to_owned());
    }

    let all_offline = retrieved
        .components()
        .iter()
        .all(|component| component.capture_receipt().is_none());
    if all_offline {
        return Ok(None);
    }
    if retrieved
        .components()
        .iter()
        .any(|component| component.capture_receipt().is_none())
    {
        return Err(SecClientError::InvalidCaptureMaterial);
    }

    let first = retrieved
        .components()
        .first()
        .and_then(RetrievedSecBytes::capture_receipt)
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    let source_id = first.source_id().clone();
    let metadata_revision = first.metadata_revision().clone();
    let dataset = SourceIdentifier::try_from(format!(
        "sec.submissions.cik.{}",
        retrieved.document().cik()
    ))?;
    let mut request_set_hash = Sha256::new();
    request_set_hash.update(b"market-squawk/sec-complete-submissions-request-set/v1");
    hash_capture_field(&mut request_set_hash, source_id.as_str().as_bytes());
    hash_capture_field(
        &mut request_set_hash,
        metadata_revision.as_source_identifier().as_str().as_bytes(),
    );
    hash_capture_field(&mut request_set_hash, dataset.as_str().as_bytes());
    request_set_hash.update((expected_count as u64).to_be_bytes());

    let mut pages = Vec::new();
    pages
        .try_reserve_exact(expected_count)
        .map_err(|_| SecClientError::AllocationFailed)?;
    for (ordinal, (component, expected_locator)) in retrieved
        .components()
        .iter()
        .zip(&expected_locators)
        .enumerate()
    {
        if component.locator() != Some(expected_locator.as_str()) {
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        let capture = component
            .capture_receipt()
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let original = capture
            .pages()
            .first()
            .filter(|original| {
                capture.pages().len() == 1
                    && capture.source_id() == &source_id
                    && capture.metadata_revision() == &metadata_revision
                    && capture.dataset().as_str() == expected_locator
                    && capture.request_set_identity() == original.request_identity()
                    && capture.terminal() == ProviderCaptureTerminalDisposition::StandaloneResponse
            })
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        if original.body_digest() != component.evidence()
            || original.body_bytes()
                != u64::try_from(component.bytes().len())
                    .map_err(|_| SecClientError::CompositeByteLimitExceeded)?
        {
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        let request_token = ordinal.checked_sub(1).map(|index| {
            companion_token_digest(
                retrieved.document().companion_files()[index]
                    .as_str()
                    .as_bytes(),
            )
        });
        let response_token = retrieved
            .document()
            .companion_files()
            .get(ordinal)
            .map(|name| companion_token_digest(name.as_str().as_bytes()));
        let ordinal =
            u16::try_from(ordinal).map_err(|_| SecClientError::CompanionObjectLimitExceeded)?;
        request_set_hash.update(ordinal.to_be_bytes());
        request_set_hash.update(original.request_identity().bytes());
        if let Some(token) = response_token {
            request_set_hash.update(token.bytes());
        }
        pages.push(ProviderCapturePageReceipt::try_new(
            ordinal,
            original.request_identity(),
            request_token,
            response_token,
            original.http_status(),
            original.body_bytes(),
            original.body_digest(),
            original.received_at(),
        )?);
    }
    let request_set_identity =
        EvidenceDigest::new(DigestAlgorithm::Sha256, request_set_hash.finalize().into());
    let receipt = ProviderCaptureSetReceipt::try_new(
        source_id,
        metadata_revision,
        dataset,
        request_set_identity,
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        pages,
    )?;
    let connection_id = crate::client::deterministic_capture_uuid(b"connection", &receipt, 0);
    let mut records = Vec::new();
    records
        .try_reserve_exact(expected_count)
        .map_err(|_| SecClientError::AllocationFailed)?;
    for (ordinal, (page, component)) in receipt
        .pages()
        .iter()
        .zip(retrieved.components())
        .enumerate()
    {
        let ordinal =
            u16::try_from(ordinal).map_err(|_| SecClientError::CompanionObjectLimitExceeded)?;
        records.push(RawCaptureRecord::try_new_live(
            crate::client::deterministic_capture_uuid(b"event", &receipt, ordinal),
            Arc::from(receipt.source_id().as_str()),
            connection_id,
            Some(u64::from(ordinal)),
            None,
            DateTime::<Utc>::from_timestamp_nanos(page.received_at().unix_nanos()),
            Bytes::clone(component.bytes()),
        )?);
    }
    ProviderCaptureMaterial::try_new(receipt, records)
        .map(Some)
        .map_err(Into::into)
}

fn companion_token_digest(name: &[u8]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sec-submissions-companion-token/v1");
    hash_capture_field(&mut hash, name);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_capture_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
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
    use market_squawk_domain::{MetadataRevision, SourceId};
    use market_squawk_sources::SourceObjectCaptureIdentity;

    use super::*;

    fn captured_component(
        locator: String,
        bytes: &[u8],
        received_at: Timestamp,
    ) -> Result<RetrievedSecBytes, Box<dyn std::error::Error>> {
        let body_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into());
        let mut request_hash = Sha256::new();
        request_hash.update(b"sec-test-request");
        request_hash.update(locator.as_bytes());
        let request_identity =
            EvidenceDigest::new(DigestAlgorithm::Sha256, request_hash.finalize().into());
        let source_id = SourceId::try_from("sec-test")?;
        let metadata_revision = MetadataRevision::new(SourceIdentifier::try_from("sec-test-v1")?);
        let page = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            200,
            u64::try_from(bytes.len())?,
            body_digest,
            received_at,
        )?;
        let receipt = ProviderCaptureSetReceipt::try_new(
            source_id,
            metadata_revision,
            SourceIdentifier::try_from(locator.as_str())?,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )?;
        Ok(RetrievedSecBytes::captured_online(
            bytes.to_vec(),
            body_digest,
            received_at,
            locator,
            1,
            receipt,
        ))
    }

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

    #[test]
    fn complete_capture_closes_declared_companion_chain_and_rejects_object_conflict()
    -> Result<(), Box<dyn std::error::Error>> {
        let recent_bytes = include_bytes!("../fixtures/submissions-recent.json");
        let archive_bytes = include_bytes!("../fixtures/submissions-archive.json");
        let limits = SecParserLimits::production_defaults();
        let recent = SubmissionsDocument::parse(recent_bytes, limits)?;
        let archive = SubmissionsDocument::parse_archive(archive_bytes, limits)?;
        let reconciled = reconcile_submissions(&recent, &[archive], limits)?;
        let current_locator = SecObjectLocator::submissions(reconciled.cik().as_str())?
            .url()
            .to_owned();
        let companion_locator = SecObjectLocator::companion("CIK0000320193-submissions-001.json")?
            .url()
            .to_owned();
        let current = captured_component(
            current_locator,
            recent_bytes,
            Timestamp::from_unix_nanos(100),
        )?;
        let companion = captured_component(
            companion_locator,
            archive_bytes,
            Timestamp::from_unix_nanos(200),
        )?;
        let standalone = current
            .capture_material()?
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        assert!(matches!(
            SourceObjectCaptureIdentity::try_from_capture(standalone.receipt())?,
            SourceObjectCaptureIdentity::Paged {
                page_count,
                terminal: ProviderCaptureTerminalDisposition::StandaloneResponse,
                ..
            } if page_count.get() == 1
        ));
        let manifest_bytes = b"local-composite-manifest".to_vec();
        let manifest_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&manifest_bytes).into(),
        );
        let manifest = RetrievedSecBytes::local_composite(
            manifest_bytes,
            manifest_digest,
            Timestamp::from_unix_nanos(200),
        );
        let retrieved = RetrievedSubmissions::new(
            reconciled.clone(),
            manifest.clone(),
            vec![current.clone(), companion],
        );
        let capture = retrieved
            .capture_material()?
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        assert_eq!(
            capture.receipt().terminal(),
            ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        );
        assert_eq!(capture.receipt().pages().len(), 2);
        assert_eq!(capture.records()[0].payload(), recent_bytes);
        assert_eq!(capture.records()[1].payload(), archive_bytes);
        assert!(matches!(
            SourceObjectCaptureIdentity::try_from_capture(capture.receipt())?,
            SourceObjectCaptureIdentity::Paged {
                page_count,
                terminal: ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
                ..
            } if page_count.get() == 2
        ));
        assert_eq!(
            capture.receipt().pages()[0].response_next_page_token_digest(),
            capture.receipt().pages()[1].request_page_token_digest()
        );

        let conflicting = RetrievedSubmissions::new(
            reconciled,
            manifest,
            vec![
                current,
                captured_component(
                    SecObjectLocator::companion("CIK0000320193-submissions-002.json")?
                        .url()
                        .to_owned(),
                    archive_bytes,
                    Timestamp::from_unix_nanos(200),
                )?,
            ],
        );
        assert!(matches!(
            conflicting.capture_material(),
            Err(SecClientError::InvalidCaptureMaterial)
        ));
        Ok(())
    }
}
