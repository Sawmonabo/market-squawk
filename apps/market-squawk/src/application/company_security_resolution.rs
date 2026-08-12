//! Desktop-consumer operator workflow for evidence-authorized company/security relationships.
//!
//! This module composes pure catalog capabilities without teaching the catalog, service, or MCP
//! layer a Desktop workflow. Ticker, name, and exchange associations are absent from every
//! authority-bearing input. A preview is process-local, bounded, immutable, and one-use; final
//! publication is still revalidated atomically by the catalog authority.

mod digest;
mod model;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_data::{
    CompanyIdentityReadCapability, CompanySecurityIdentityCatalogError,
    CompanySecurityIdentityDisposition, CompanySecurityIdentityQuery,
    CompanySecurityIdentityReadCapability, CompanySecurityIdentityRecord,
    CompanySecurityLinkPublicationCapability, CompanySecurityLinkPublicationReceipt,
    MarketDataInstrumentCatalogError, MarketDataInstrumentReadCapability,
    MarketDataInstrumentRecord,
};
use market_squawk_domain::{
    CompanySecurityIdentityError, CompanySecurityIdentityLink, CompanySecurityIdentityLinkInput,
    CompanySecurityLinkTransition, CompanySecurityResolutionBasis, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId, SchemaVersion,
    SourceIdentifier, Timestamp,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use self::digest::preview_digest;
pub use self::model::{
    COMPANY_SECURITY_RESOLUTION_FORMAT_VERSION, CompanyIdentityParentSnapshot,
    CompanySecurityCandidateSnapshot, CompanySecurityEffectiveStartPolicy,
    CompanySecurityResolutionAction, CompanySecurityResolutionConfirmation,
    CompanySecurityResolutionPreview, CompanySecurityResolutionPreviewId,
    CompanySecurityResolutionPreviewRequest, ExpectedCompanySecurityRelationshipState,
    MAXIMUM_COMPANY_SECURITY_PREVIEW_BYTES, MAXIMUM_REVIEWED_SECURITY_CANDIDATES,
    OperatorEvidenceRightsDisposition, ReviewedRelationshipEvidence, ReviewedSecurityCandidate,
    ReviewedSecurityCandidateDisposition,
};

const MAXIMUM_PENDING_PREVIEWS: usize = 64;
const PREVIEW_LIFETIME: Duration = Duration::from_secs(15 * 60);
const PREVIEW_LIFETIME_NANOS: i64 = 15 * 60 * 1_000_000_000;
const PREVIEW_ID_ATTEMPTS: usize = 16;

/// Application-owned result of one operator-authorized immutable publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedCompanySecurityResolution {
    preview: CompanySecurityResolutionPreview,
    authorization_receipt_id: SourceIdentifier,
    authorized_at: Timestamp,
    publication: CompanySecurityLinkPublicationReceipt,
}

impl AuthorizedCompanySecurityResolution {
    /// Returns every reviewed fact whose SHA-256 identity is retained by the link receipt ID.
    pub const fn preview(&self) -> &CompanySecurityResolutionPreview {
        &self.preview
    }

    /// Returns the immutable local receipt identity embedded in the published link.
    pub const fn authorization_receipt_id(&self) -> &SourceIdentifier {
        &self.authorization_receipt_id
    }

    /// Returns the exact operator authorization time used as V1 effective start.
    pub const fn authorized_at(&self) -> Timestamp {
        self.authorized_at
    }

    /// Returns the inserted or exact-replay durable catalog event.
    pub const fn publication(&self) -> &CompanySecurityLinkPublicationReceipt {
        &self.publication
    }
}

#[derive(Clone)]
struct StoredPreview {
    preview: CompanySecurityResolutionPreview,
    expires_at: Instant,
}

#[derive(Default)]
struct PreviewRegistry {
    last_revision: u64,
    previews: BTreeMap<CompanySecurityResolutionPreviewId, StoredPreview>,
}

#[derive(Clone)]
struct CurrentRelationshipState {
    record: Option<CompanySecurityIdentityRecord>,
    selection_receipt_digest: EvidenceDigest,
    selection_knowledge_at: Timestamp,
}

/// Least-authority application coordinator for explicit operator resolution.
pub struct CompanySecurityResolutionAuthority {
    company_identities: CompanyIdentityReadCapability,
    market_instruments: MarketDataInstrumentReadCapability,
    relationships: CompanySecurityIdentityReadCapability,
    publisher: CompanySecurityLinkPublicationCapability,
    previews: Mutex<PreviewRegistry>,
}

impl CompanySecurityResolutionAuthority {
    /// Composes only exact reads and the narrow company/security link publisher.
    pub(crate) fn new(
        company_identities: CompanyIdentityReadCapability,
        market_instruments: MarketDataInstrumentReadCapability,
        publisher: CompanySecurityLinkPublicationCapability,
    ) -> Self {
        let relationships = company_identities.security_relationships();
        Self {
            company_identities,
            market_instruments,
            relationships,
            publisher,
            previews: Mutex::new(PreviewRegistry::default()),
        }
    }

    /// Resolves and retains one immutable preview without granting publication authority.
    pub fn preview(
        &self,
        request: CompanySecurityResolutionPreviewRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanySecurityResolutionPreview, CompanySecurityResolutionError> {
        check_operation(deadline, cancellation)?;
        let (company, company_digest, company_completed_at) = self
            .company_identities
            .exact_current(
                request.company_source_id(),
                request.provider_company_id(),
                request.company_surface(),
                deadline,
                cancellation,
            )
            .map_err(map_company_catalog_error)?
            .ok_or(CompanySecurityResolutionError::ParentUnavailable)?;
        let company_available_at = company
            .availability()
            .conservative_available_at()
            .ok_or(CompanySecurityResolutionError::ParentUnavailable)?;
        let (candidate_snapshots, selected_candidate_ordinal) =
            self.resolve_candidates(request.reviewed_candidates(), deadline, cancellation)?;
        let selected = candidate_snapshots
            .get(usize::from(selected_candidate_ordinal))
            .ok_or(CompanySecurityResolutionError::AmbiguousCandidates)?;
        let current_relationship = self.read_relationship_state(
            request.company_source_id(),
            request.provider_company_id(),
            request.company_surface(),
            selected.instrument_id(),
            deadline,
            cancellation,
        )?;
        ensure_expected_relationship(request.expected_relationship_state(), &current_relationship)?;
        ensure_action_is_admissible(
            request.action(),
            request.security_kind(),
            request.relationship_kind(),
            request.common_equity_suitability(),
            selected,
            current_relationship.record.as_ref(),
        )?;

        let issued_at = system_timestamp()?;
        check_operation(deadline, cancellation)?;
        request.reviewed_evidence().ensure_current(issued_at)?;
        ensure_company_parent_times(
            company.received_at(),
            company_available_at,
            company.ingested_at(),
            company_completed_at,
            issued_at,
        )?;
        if candidate_snapshots
            .iter()
            .any(|candidate| candidate.market_published_at() > issued_at)
        {
            return Err(CompanySecurityResolutionError::CorruptParent);
        }
        ensure_interval_covers_authorization(
            selected.market_effective_interval(),
            issued_at,
            request.effective_end(),
        )?;
        let expires_at = issued_at
            .checked_add_nanos(PREVIEW_LIFETIME_NANOS)
            .map_err(|_| CompanySecurityResolutionError::TimeUnavailable)?;
        let monotonic_expires_at = Instant::now()
            .checked_add(PREVIEW_LIFETIME)
            .ok_or(CompanySecurityResolutionError::TimeUnavailable)?;

        let company_parent = CompanyIdentityParentSnapshot::new(
            company.source_id().clone(),
            company.provider_company_id().clone(),
            company.surface(),
            company_digest,
            company.conformed_name().to_owned(),
            company.received_at(),
            company_available_at,
            company.ingested_at(),
            company_completed_at,
        );
        let mut registry = self
            .previews
            .try_lock()
            .map_err(|_| CompanySecurityResolutionError::AuthorityUnavailable)?;
        prune_expired(&mut registry, Instant::now());
        if registry.previews.len() >= MAXIMUM_PENDING_PREVIEWS {
            return Err(CompanySecurityResolutionError::CapacityExceeded);
        }
        let preview_revision = registry
            .last_revision
            .checked_add(1)
            .ok_or(CompanySecurityResolutionError::RevisionExhausted)?;
        let preview_id = next_preview_id(&registry.previews)?;
        let mut preview = CompanySecurityResolutionPreview::new(
            preview_id,
            preview_revision,
            company_parent,
            candidate_snapshots.into_boxed_slice(),
            selected_candidate_ordinal,
            request.operator_id().clone(),
            request.security_kind(),
            request.relationship_kind(),
            request.common_equity_suitability(),
            request.reviewed_evidence().clone(),
            request.action().clone(),
            request.expected_relationship_state(),
            current_relationship.selection_receipt_digest,
            current_relationship.selection_knowledge_at,
            request.effective_end(),
            issued_at,
            expires_at,
        );
        let digest = preview_digest(&preview)?;
        preview.install_digest(digest);
        ensure_preview_size(&preview)?;
        registry.last_revision = preview_revision;
        registry.previews.insert(
            preview_id,
            StoredPreview {
                preview: preview.clone(),
                expires_at: monotonic_expires_at,
            },
        );
        Ok(preview)
    }

    /// Consumes one explicit confirmation, rechecks every parent, and publishes one A1 event.
    pub fn confirm(
        &self,
        confirmation: CompanySecurityResolutionConfirmation,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AuthorizedCompanySecurityResolution, CompanySecurityResolutionError> {
        check_operation(deadline, cancellation)?;
        let preview = {
            let mut registry = self
                .previews
                .try_lock()
                .map_err(|_| CompanySecurityResolutionError::AuthorityUnavailable)?;
            let monotonic_now = Instant::now();
            let stored = registry
                .previews
                .get(&confirmation.preview_id())
                .ok_or(CompanySecurityResolutionError::PreviewUnavailable)?;
            if stored.expires_at <= monotonic_now {
                registry.previews.remove(&confirmation.preview_id());
                return Err(CompanySecurityResolutionError::PreviewExpired);
            }
            if stored.preview.preview_revision() != confirmation.preview_revision()
                || stored.preview.preview_digest() != confirmation.preview_digest()
                || preview_digest(&stored.preview)? != confirmation.preview_digest()
            {
                return Err(CompanySecurityResolutionError::InvalidConfirmation);
            }
            registry
                .previews
                .remove(&confirmation.preview_id())
                .ok_or(CompanySecurityResolutionError::PreviewUnavailable)?
                .preview
        };

        self.recheck_company_parent(&preview, deadline, cancellation)?;
        self.recheck_candidate_parents(&preview, deadline, cancellation)?;
        let selected = preview.selected_candidate();
        let current_relationship = self.read_relationship_state(
            preview.company_parent().company_source_id(),
            preview.company_parent().provider_company_id(),
            preview.company_parent().company_surface(),
            selected.instrument_id(),
            deadline,
            cancellation,
        )?;
        ensure_expected_relationship(preview.expected_relationship_state(), &current_relationship)?;
        ensure_action_is_admissible(
            preview.action(),
            preview.security_kind(),
            preview.relationship_kind(),
            preview.common_equity_suitability(),
            selected,
            current_relationship.record.as_ref(),
        )?;

        let authorized_at = system_timestamp()?;
        check_operation(deadline, cancellation)?;
        if preview.expires_at() <= authorized_at {
            return Err(CompanySecurityResolutionError::PreviewExpired);
        }
        preview.reviewed_evidence().ensure_current(authorized_at)?;
        ensure_interval_covers_authorization(
            selected.market_effective_interval(),
            authorized_at,
            preview.effective_end(),
        )?;
        let transition = transition_for_preview(&preview)?;
        let effective_interval = EffectiveInterval::new(authorized_at, preview.effective_end())
            .map_err(|_| CompanySecurityResolutionError::InvalidRequest)?;
        let authorization_receipt_id = authorization_receipt_id(preview.preview_digest())?;
        let link = CompanySecurityIdentityLink::try_new(CompanySecurityIdentityLinkInput {
            schema_version: SchemaVersion::CURRENT,
            company_source_id: preview.company_parent().company_source_id().clone(),
            provider_company_id: preview.company_parent().provider_company_id().clone(),
            company_surface: preview.company_parent().company_surface(),
            company_observation_digest: preview.company_parent().company_observation_digest(),
            instrument_id: selected.instrument_id(),
            permanent_figi: selected.permanent_figi().clone(),
            market_instrument_revision_digest: selected.market_revision_digest(),
            security_kind: preview.security_kind(),
            relationship_kind: preview.relationship_kind(),
            common_equity_suitability: preview.common_equity_suitability(),
            resolution_basis: CompanySecurityResolutionBasis::OperatorAuthorizedResolution {
                receipt_id: authorization_receipt_id.clone(),
                operator_id: preview.operator_id().clone(),
                evidence: ExactPayloadEvidence::from_content_digest(preview.preview_digest()),
                authorized_at,
            },
            relationship_evidence_rights: preview.reviewed_evidence().rights().clone(),
            effective_interval,
            available_at: authorized_at,
            ingested_at: authorized_at,
            transition,
        })
        .map_err(CompanySecurityResolutionError::Domain)?;
        check_operation(deadline, cancellation)?;
        let publication = self
            .publisher
            .publish(link, deadline, cancellation)
            .map_err(map_company_catalog_error)?;
        Ok(AuthorizedCompanySecurityResolution {
            preview,
            authorization_receipt_id,
            authorized_at,
            publication,
        })
    }

    fn resolve_candidates(
        &self,
        reviewed: &[ReviewedSecurityCandidate],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<CompanySecurityCandidateSnapshot>, u16), CompanySecurityResolutionError> {
        if reviewed.is_empty() || reviewed.len() > MAXIMUM_REVIEWED_SECURITY_CANDIDATES {
            return Err(CompanySecurityResolutionError::AmbiguousCandidates);
        }
        let mut snapshots = Vec::new();
        snapshots
            .try_reserve_exact(reviewed.len())
            .map_err(|_| CompanySecurityResolutionError::CapacityExceeded)?;
        let mut instrument_ids = BTreeSet::new();
        let mut selected = None;
        for (index, candidate) in reviewed.iter().enumerate() {
            check_operation(deadline, cancellation)?;
            let record = self
                .market_instruments
                .latest_by_figi(candidate.permanent_figi(), deadline, cancellation)
                .map_err(map_market_catalog_error)?
                .ok_or(CompanySecurityResolutionError::ParentUnavailable)?;
            validate_market_record(candidate.permanent_figi(), &record)?;
            if !instrument_ids.insert(record.definition().instrument_id()) {
                return Err(CompanySecurityResolutionError::AmbiguousCandidates);
            }
            let ordinal = u16::try_from(index)
                .map_err(|_| CompanySecurityResolutionError::CapacityExceeded)?;
            if candidate.disposition().is_selected() {
                if selected.replace(ordinal).is_some() {
                    return Err(CompanySecurityResolutionError::AmbiguousCandidates);
                }
            }
            snapshots.push(CompanySecurityCandidateSnapshot::new(
                ordinal,
                record.definition().permanent_figi().clone(),
                record.definition().instrument_id(),
                record.revision_digest(),
                record.revision_sequence(),
                record.published_at(),
                record.definition().effective_interval(),
                candidate.disposition().clone(),
            ));
        }
        let selected = selected.ok_or(CompanySecurityResolutionError::AmbiguousCandidates)?;
        Ok((snapshots, selected))
    }

    fn read_relationship_state(
        &self,
        company_source_id: &market_squawk_domain::SourceId,
        provider_company_id: &SourceIdentifier,
        company_surface: market_squawk_domain::CompanyIdentitySurface,
        instrument_id: InstrumentId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CurrentRelationshipState, CompanySecurityResolutionError> {
        check_operation(deadline, cancellation)?;
        let query = CompanySecurityIdentityQuery::new(
            company_source_id.clone(),
            provider_company_id.clone(),
            company_surface,
            Some(instrument_id),
            false,
        );
        let selection = self
            .relationships
            .current(&query, deadline, cancellation)
            .map_err(map_company_catalog_error)?;
        if selection.disposition() == CompanySecurityIdentityDisposition::Conflict
            || selection.candidates().len() + selection.exclusions().len() > 1
        {
            return Err(CompanySecurityResolutionError::AmbiguousCurrentRelationship);
        }
        let record = selection.candidates().first().cloned().or_else(|| {
            selection
                .exclusions()
                .first()
                .map(|value| value.record().clone())
        });
        validate_sha256_receipt(selection.receipt().receipt_digest())?;
        Ok(CurrentRelationshipState {
            record,
            selection_receipt_digest: selection.receipt().receipt_digest(),
            selection_knowledge_at: selection.receipt().knowledge_at(),
        })
    }

    fn recheck_company_parent(
        &self,
        preview: &CompanySecurityResolutionPreview,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), CompanySecurityResolutionError> {
        check_operation(deadline, cancellation)?;
        let expected = preview.company_parent();
        let (observation, digest, completed_at) = self
            .company_identities
            .exact_current(
                expected.company_source_id(),
                expected.provider_company_id(),
                expected.company_surface(),
                deadline,
                cancellation,
            )
            .map_err(map_company_catalog_error)?
            .ok_or(CompanySecurityResolutionError::ParentDrift)?;
        if digest != expected.company_observation_digest()
            || observation.received_at() != expected.received_at()
            || observation.availability().conservative_available_at()
                != Some(expected.available_at())
            || observation.ingested_at() != expected.ingested_at()
            || completed_at != expected.completed_at()
            || observation.conformed_name() != expected.conformed_name_for_display()
        {
            return Err(CompanySecurityResolutionError::ParentDrift);
        }
        Ok(())
    }

    fn recheck_candidate_parents(
        &self,
        preview: &CompanySecurityResolutionPreview,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), CompanySecurityResolutionError> {
        for expected in preview.candidates() {
            check_operation(deadline, cancellation)?;
            let record = self
                .market_instruments
                .latest_by_figi(expected.permanent_figi(), deadline, cancellation)
                .map_err(map_market_catalog_error)?
                .ok_or(CompanySecurityResolutionError::ParentDrift)?;
            if record.definition().instrument_id() != expected.instrument_id()
                || record.definition().permanent_figi() != expected.permanent_figi()
                || record.revision_digest() != expected.market_revision_digest()
                || record.revision_sequence() != expected.market_revision_sequence()
                || record.published_at() != expected.market_published_at()
                || record.definition().effective_interval() != expected.market_effective_interval()
            {
                return Err(CompanySecurityResolutionError::ParentDrift);
            }
        }
        Ok(())
    }
}

impl fmt::Debug for CompanySecurityResolutionAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("CompanySecurityResolutionAuthority([SEALED OPERATOR RESOLUTION AUTHORITY])")
    }
}

fn ensure_expected_relationship(
    expected: ExpectedCompanySecurityRelationshipState,
    actual: &CurrentRelationshipState,
) -> Result<(), CompanySecurityResolutionError> {
    match (expected, actual.record.as_ref()) {
        (ExpectedCompanySecurityRelationshipState::Absent, None) => Ok(()),
        (ExpectedCompanySecurityRelationshipState::Current { link_digest }, Some(record))
            if link_digest == record.link_digest() =>
        {
            Ok(())
        }
        _ => Err(CompanySecurityResolutionError::RelationshipStateConflict),
    }
}

fn ensure_action_is_admissible(
    action: &CompanySecurityResolutionAction,
    security_kind: market_squawk_domain::CompanySecurityKind,
    relationship_kind: market_squawk_domain::CompanySecurityRelationshipKind,
    suitability: market_squawk_domain::CommonEquitySuitability,
    selected: &CompanySecurityCandidateSnapshot,
    current: Option<&CompanySecurityIdentityRecord>,
) -> Result<(), CompanySecurityResolutionError> {
    match action {
        CompanySecurityResolutionAction::Authorize => Ok(()),
        CompanySecurityResolutionAction::Revoke { .. } => {
            let current =
                current.ok_or(CompanySecurityResolutionError::RelationshipStateConflict)?;
            if current.link().transition().is_revocation() {
                return Err(CompanySecurityResolutionError::RelationshipAlreadyRevoked);
            }
            if current.link().instrument_id() != selected.instrument_id()
                || current.link().permanent_figi() != selected.permanent_figi()
                || current.link().security_kind() != security_kind
                || current.link().relationship_kind() != relationship_kind
                || current.link().common_equity_suitability() != suitability
            {
                return Err(CompanySecurityResolutionError::RelationshipStateConflict);
            }
            Ok(())
        }
    }
}

fn transition_for_preview(
    preview: &CompanySecurityResolutionPreview,
) -> Result<CompanySecurityLinkTransition, CompanySecurityResolutionError> {
    match (preview.action(), preview.expected_relationship_state()) {
        (
            CompanySecurityResolutionAction::Authorize,
            ExpectedCompanySecurityRelationshipState::Absent,
        ) => Ok(CompanySecurityLinkTransition::Initial),
        (
            CompanySecurityResolutionAction::Authorize,
            ExpectedCompanySecurityRelationshipState::Current { link_digest },
        ) => Ok(CompanySecurityLinkTransition::Supersedes {
            previous_link_digest: link_digest,
        }),
        (
            CompanySecurityResolutionAction::Revoke { reason },
            ExpectedCompanySecurityRelationshipState::Current { link_digest },
        ) => Ok(CompanySecurityLinkTransition::Revokes {
            previous_link_digest: link_digest,
            reason: reason.clone(),
        }),
        _ => Err(CompanySecurityResolutionError::InvalidRequest),
    }
}

fn validate_market_record(
    requested_figi: &market_squawk_domain::Figi,
    record: &MarketDataInstrumentRecord,
) -> Result<(), CompanySecurityResolutionError> {
    if record.definition().permanent_figi() != requested_figi
        || record.revision_digest().algorithm() != DigestAlgorithm::Sha256
        || record.revision_digest().bytes() == [0; 32]
        || record.revision_sequence() == 0
    {
        return Err(CompanySecurityResolutionError::CorruptParent);
    }
    Ok(())
}

fn ensure_company_parent_times(
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    completed_at: Timestamp,
    now: Timestamp,
) -> Result<(), CompanySecurityResolutionError> {
    if received_at > ingested_at
        || available_at > ingested_at
        || ingested_at > completed_at
        || completed_at > now
    {
        Err(CompanySecurityResolutionError::CorruptParent)
    } else {
        Ok(())
    }
}

fn ensure_interval_covers_authorization(
    market: EffectiveInterval,
    authorized_at: Timestamp,
    requested_end: Option<Timestamp>,
) -> Result<(), CompanySecurityResolutionError> {
    if market.starts_at() > authorized_at
        || market.ends_at().is_some_and(|end| end <= authorized_at)
        || requested_end.is_some_and(|end| end <= authorized_at)
        || match (market.ends_at(), requested_end) {
            (Some(_), None) => true,
            (Some(market_end), Some(requested_end)) => market_end < requested_end,
            _ => false,
        }
    {
        Err(CompanySecurityResolutionError::ParentUnavailable)
    } else {
        Ok(())
    }
}

fn next_preview_id(
    previews: &BTreeMap<CompanySecurityResolutionPreviewId, StoredPreview>,
) -> Result<CompanySecurityResolutionPreviewId, CompanySecurityResolutionError> {
    for _ in 0..PREVIEW_ID_ATTEMPTS {
        let candidate = CompanySecurityResolutionPreviewId::new(Uuid::new_v4())?;
        if !previews.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    Err(CompanySecurityResolutionError::CapacityExceeded)
}

fn prune_expired(registry: &mut PreviewRegistry, now: Instant) {
    registry
        .previews
        .retain(|_, stored| stored.expires_at > now);
}

fn ensure_preview_size(
    preview: &CompanySecurityResolutionPreview,
) -> Result<(), CompanySecurityResolutionError> {
    let encoded =
        serde_json::to_vec(preview).map_err(|_| CompanySecurityResolutionError::Encoding)?;
    if encoded.len() > MAXIMUM_COMPANY_SECURITY_PREVIEW_BYTES {
        Err(CompanySecurityResolutionError::CapacityExceeded)
    } else {
        Ok(())
    }
}

fn authorization_receipt_id(
    preview_digest: EvidenceDigest,
) -> Result<SourceIdentifier, CompanySecurityResolutionError> {
    validate_sha256_receipt(preview_digest)?;
    let mut value = String::with_capacity(102);
    value.push_str("company-security-resolution-v1:");
    for byte in preview_digest.bytes() {
        write!(&mut value, "{byte:02x}").map_err(|_| CompanySecurityResolutionError::Encoding)?;
    }
    SourceIdentifier::try_from(value).map_err(|_| CompanySecurityResolutionError::Encoding)
}

fn validate_sha256_receipt(digest: EvidenceDigest) -> Result<(), CompanySecurityResolutionError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(CompanySecurityResolutionError::CorruptParent)
    } else {
        Ok(())
    }
}

fn system_timestamp() -> Result<Timestamp, CompanySecurityResolutionError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CompanySecurityResolutionError::TimeUnavailable)?
        .as_nanos();
    let nanos =
        i64::try_from(nanos).map_err(|_| CompanySecurityResolutionError::TimeUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompanySecurityResolutionError> {
    if cancellation.is_cancelled() {
        Err(CompanySecurityResolutionError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(CompanySecurityResolutionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_company_catalog_error(
    error: CompanySecurityIdentityCatalogError,
) -> CompanySecurityResolutionError {
    match error {
        CompanySecurityIdentityCatalogError::Cancelled => CompanySecurityResolutionError::Cancelled,
        CompanySecurityIdentityCatalogError::DeadlineExceeded => {
            CompanySecurityResolutionError::DeadlineExceeded
        }
        CompanySecurityIdentityCatalogError::AuthorityUnavailable => {
            CompanySecurityResolutionError::AuthorityUnavailable
        }
        CompanySecurityIdentityCatalogError::ParentUnavailable => {
            CompanySecurityResolutionError::ParentDrift
        }
        CompanySecurityIdentityCatalogError::AmbiguousParent => {
            CompanySecurityResolutionError::AmbiguousParent
        }
        error => CompanySecurityResolutionError::CompanyCatalog(error),
    }
}

fn map_market_catalog_error(
    error: MarketDataInstrumentCatalogError,
) -> CompanySecurityResolutionError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => CompanySecurityResolutionError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => {
            CompanySecurityResolutionError::DeadlineExceeded
        }
        MarketDataInstrumentCatalogError::AuthorityUnavailable => {
            CompanySecurityResolutionError::AuthorityUnavailable
        }
        error => CompanySecurityResolutionError::MarketCatalog(error),
    }
}

/// Closed fail-safe outcome for preview, confirmation, parent recheck, or publication.
#[derive(Debug, Error)]
pub enum CompanySecurityResolutionError {
    #[error("company/security operator-resolution request is invalid")]
    InvalidRequest,
    #[error("company/security candidate decisions are absent, duplicated, or ambiguous")]
    AmbiguousCandidates,
    #[error("company/security current relationship state is ambiguous")]
    AmbiguousCurrentRelationship,
    #[error("company/security exact parent is unavailable")]
    ParentUnavailable,
    #[error("company/security exact current parent is ambiguous")]
    AmbiguousParent,
    #[error("company/security exact parent changed after preview")]
    ParentDrift,
    #[error("company/security exact parent content is corrupt")]
    CorruptParent,
    #[error("company/security current relationship no longer matches the preview")]
    RelationshipStateConflict,
    #[error("company/security relationship is already revoked")]
    RelationshipAlreadyRevoked,
    #[error("company/security relationship evidence rights expired before authorization")]
    StaleRights,
    #[error("company/security relationship evidence rights were revoked")]
    RevokedRights,
    #[error("company/security preview is unavailable, consumed, or process-invalidated")]
    PreviewUnavailable,
    #[error("company/security preview expired")]
    PreviewExpired,
    #[error("company/security preview confirmation does not match its exact revision and digest")]
    InvalidConfirmation,
    #[error("company/security preview or candidate capacity was exceeded")]
    CapacityExceeded,
    #[error("company/security preview revision space is exhausted")]
    RevisionExhausted,
    #[error("company/security resolution authority is busy or poisoned")]
    AuthorityUnavailable,
    #[error("company/security resolution was cancelled")]
    Cancelled,
    #[error("company/security resolution deadline elapsed")]
    DeadlineExceeded,
    #[error("company/security wall-clock time is unavailable")]
    TimeUnavailable,
    #[error("company/security preview encoding failed")]
    Encoding,
    #[error("company/security immutable link is invalid")]
    Domain(#[source] CompanySecurityIdentityError),
    #[error("company/security catalog operation failed")]
    CompanyCatalog(#[source] CompanySecurityIdentityCatalogError),
    #[error("company/security market-parent operation failed")]
    MarketCatalog(#[source] MarketDataInstrumentCatalogError),
}
