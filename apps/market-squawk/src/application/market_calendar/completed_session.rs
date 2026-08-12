//! Pure selection of one latest, evidence-backed completed market session.

use std::{fmt, sync::Arc};

use market_squawk_domain::{
    BarTimeSemantics, BarTimestampBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    MarketBarSessionEvidence, MarketBarSessionKind, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_sources::{ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Hard ceiling on retained candidate periods for one exact evidence-series snapshot.
const MAXIMUM_COMPLETED_SESSION_CANDIDATES: usize = 16_384;

/// One exact source-neutral coordinate and its explicit temporal cutoffs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedMarketSessionRequest {
    venue_id: VenueId,
    timeframe: SourceIdentifier,
    evidence_series: SourceIdentifier,
    completion_cutoff: Timestamp,
    knowledge_cutoff: Timestamp,
    evaluated_at: Timestamp,
    digest: EvidenceDigest,
}

impl CompletedMarketSessionRequest {
    /// Constructs a request without consulting a wall clock or widening any cutoff.
    pub(crate) fn try_new(
        venue_id: VenueId,
        timeframe: SourceIdentifier,
        evidence_series: SourceIdentifier,
        completion_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Result<Self, CompletedMarketSessionError> {
        if completion_cutoff > knowledge_cutoff || knowledge_cutoff > evaluated_at {
            return Err(CompletedMarketSessionError::InvalidRequest);
        }
        let digest = request_digest(
            &venue_id,
            &timeframe,
            &evidence_series,
            completion_cutoff,
            knowledge_cutoff,
            evaluated_at,
        );
        Ok(Self {
            venue_id,
            timeframe,
            evidence_series,
            completion_cutoff,
            knowledge_cutoff,
            evaluated_at,
            digest,
        })
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn timeframe(&self) -> &SourceIdentifier {
        &self.timeframe
    }

    /// Returns the exact provider/calendar evidence series being queried.
    pub(crate) const fn evidence_series(&self) -> &SourceIdentifier {
        &self.evidence_series
    }

    /// Returns the inclusive latest permissible period-completion boundary.
    pub(crate) const fn completion_cutoff(&self) -> Timestamp {
        self.completion_cutoff
    }

    /// Returns the inclusive point-in-time knowledge cutoff.
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the explicit trusted instant used only for lifecycle validation.
    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Stable key the injected evidence owner must revalidate before and after selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedMarketSessionCurrentnessIdentity {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    venue_id: VenueId,
    timeframe: SourceIdentifier,
    evidence_series: SourceIdentifier,
    calendar_id: SourceIdentifier,
    calendar_ruleset: SourceIdentifier,
    calendar_evidence: EvidenceDigest,
    source_generation: EvidenceDigest,
    revocation_identity: EvidenceDigest,
}

impl CompletedMarketSessionCurrentnessIdentity {
    #[allow(
        clippy::too_many_arguments,
        reason = "source, calendar, generation, and revocation coordinates are independent"
    )]
    pub(crate) fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        venue_id: VenueId,
        timeframe: SourceIdentifier,
        evidence_series: SourceIdentifier,
        calendar_id: SourceIdentifier,
        calendar_ruleset: SourceIdentifier,
        calendar_evidence: EvidenceDigest,
        source_generation: EvidenceDigest,
        revocation_identity: EvidenceDigest,
    ) -> Result<Self, CompletedMarketSessionError> {
        for evidence in [calendar_evidence, source_generation, revocation_identity] {
            require_nonzero(evidence)?;
        }
        Ok(Self {
            source_id,
            metadata_revision,
            venue_id,
            timeframe,
            evidence_series,
            calendar_id,
            calendar_ruleset,
            calendar_evidence,
            source_generation,
            revocation_identity,
        })
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn timeframe(&self) -> &SourceIdentifier {
        &self.timeframe
    }

    pub(crate) const fn evidence_series(&self) -> &SourceIdentifier {
        &self.evidence_series
    }

    pub(crate) const fn calendar_id(&self) -> &SourceIdentifier {
        &self.calendar_id
    }

    pub(crate) const fn calendar_ruleset(&self) -> &SourceIdentifier {
        &self.calendar_ruleset
    }

    pub(crate) const fn calendar_evidence(&self) -> EvidenceDigest {
        self.calendar_evidence
    }

    pub(crate) const fn source_generation(&self) -> EvidenceDigest {
        self.source_generation
    }

    pub(crate) const fn revocation_identity(&self) -> EvidenceDigest {
        self.revocation_identity
    }
}

/// Fresh retained-state proof returned by the injected currentness authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedMarketSessionCurrentnessReceipt {
    identity: CompletedMarketSessionCurrentnessIdentity,
    checked_at: Timestamp,
    expires_at: Timestamp,
    evidence: EvidenceDigest,
}

impl CompletedMarketSessionCurrentnessReceipt {
    /// Constructs a positive proof. Expiry is exclusive and must follow the check instant.
    pub(crate) fn try_new(
        identity: CompletedMarketSessionCurrentnessIdentity,
        checked_at: Timestamp,
        expires_at: Timestamp,
        evidence: EvidenceDigest,
    ) -> Result<Self, CompletedMarketSessionError> {
        require_nonzero(evidence)?;
        if expires_at <= checked_at {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        Ok(Self {
            identity,
            checked_at,
            expires_at,
            evidence,
        })
    }

    pub(crate) const fn identity(&self) -> &CompletedMarketSessionCurrentnessIdentity {
        &self.identity
    }

    pub(crate) const fn checked_at(&self) -> Timestamp {
        self.checked_at
    }

    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }
}

/// Result of one non-mutating retained-state currentness/revocation check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CompletedMarketSessionCurrentnessResolution {
    Current(CompletedMarketSessionCurrentnessReceipt),
    CurrentnessUnproven,
    Stale,
    Revoked,
    Conflict,
}

/// One exact completed period and the evidence needed to qualify it independently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedMarketSessionCandidate {
    period: BarTimeSemantics,
    calendar_effective: EffectiveInterval,
    calendar_available_at: Timestamp,
    knowledge_available_at: Timestamp,
    expires_at: Timestamp,
    capture_receipt_digest: EvidenceDigest,
}

impl CompletedMarketSessionCandidate {
    /// Constructs a candidate whose knowledge time is the exact conservative maximum of period
    /// completion, calendar availability, and all sealed response receive times.
    pub(crate) fn try_new(
        period: BarTimeSemantics,
        calendar_effective: EffectiveInterval,
        calendar_available_at: Timestamp,
        knowledge_available_at: Timestamp,
        expires_at: Timestamp,
        capture: &SealedProviderCaptureSetReceipt,
    ) -> Result<Self, CompletedMarketSessionError> {
        validate_capture(&capture)?;
        let latest_received_at = capture
            .capture()
            .pages()
            .iter()
            .map(|page| page.received_at())
            .max()
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        let expected_knowledge_at = period
            .period_end_exclusive()
            .max(calendar_available_at)
            .max(latest_received_at);
        if knowledge_available_at != expected_knowledge_at
            || expires_at <= knowledge_available_at
            || expires_at <= calendar_effective.starts_at()
            || calendar_effective
                .ends_at()
                .is_some_and(|ends_at| expires_at > ends_at)
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        Ok(Self {
            period,
            calendar_effective,
            calendar_available_at,
            knowledge_available_at,
            expires_at,
            capture_receipt_digest: capture.receipt_digest(),
        })
    }

    pub(crate) const fn period(&self) -> &BarTimeSemantics {
        &self.period
    }

    pub(crate) const fn calendar_effective(&self) -> EffectiveInterval {
        self.calendar_effective
    }

    pub(crate) const fn calendar_available_at(&self) -> Timestamp {
        self.calendar_available_at
    }

    pub(crate) const fn knowledge_available_at(&self) -> Timestamp {
        self.knowledge_available_at
    }

    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn capture_receipt_digest(&self) -> EvidenceDigest {
        self.capture_receipt_digest
    }
}

/// Complete bounded candidate enumeration for exactly one request and currentness generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedMarketSessionCandidateSnapshot {
    request_digest: EvidenceDigest,
    currentness: CompletedMarketSessionCurrentnessIdentity,
    complete_through: Timestamp,
    completeness_evidence: EvidenceDigest,
    capture: SealedProviderCaptureSetReceipt,
    candidates: Box<[CompletedMarketSessionCandidate]>,
}

impl CompletedMarketSessionCandidateSnapshot {
    /// Retains provider-produced candidates without sorting, truncation, or inferred completeness.
    pub(crate) fn try_new(
        request: &CompletedMarketSessionRequest,
        currentness: CompletedMarketSessionCurrentnessIdentity,
        complete_through: Timestamp,
        completeness_evidence: EvidenceDigest,
        capture: SealedProviderCaptureSetReceipt,
        candidates: Vec<CompletedMarketSessionCandidate>,
    ) -> Result<Self, CompletedMarketSessionError> {
        require_nonzero(completeness_evidence)?;
        if complete_through != request.completion_cutoff
            || currentness.venue_id != request.venue_id
            || currentness.timeframe != request.timeframe
            || currentness.evidence_series != request.evidence_series
            || candidates.len() > MAXIMUM_COMPLETED_SESSION_CANDIDATES
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        validate_candidate_sequence(request, &currentness, &capture, &candidates)?;
        Ok(Self {
            request_digest: request.digest,
            currentness,
            complete_through,
            completeness_evidence,
            capture,
            candidates: candidates.into_boxed_slice(),
        })
    }

    pub(crate) const fn currentness(&self) -> &CompletedMarketSessionCurrentnessIdentity {
        &self.currentness
    }

    pub(crate) const fn complete_through(&self) -> Timestamp {
        self.complete_through
    }

    pub(crate) const fn completeness_evidence(&self) -> EvidenceDigest {
        self.completeness_evidence
    }

    pub(crate) const fn capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.capture
    }

    pub(crate) fn candidates(&self) -> &[CompletedMarketSessionCandidate] {
        &self.candidates
    }
}

/// Read failure from an injected retained-evidence capability.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CompletedMarketSessionEvidenceAccessError {
    #[error("completed-session evidence is unavailable")]
    Unavailable,
    #[error("completed-session evidence authorities conflict")]
    Conflict,
}

/// Provider-composed, read-only evidence/currentness capability used by the pure selector.
///
/// Implementations must read already-retained bounded evidence. They must not refresh providers,
/// mutate a registry, acquire execution authority, or substitute a process wall clock.
pub(crate) trait CompletedMarketSessionEvidenceAuthority:
    fmt::Debug + Send + Sync + 'static
{
    fn candidate_snapshot(
        &self,
        request: &CompletedMarketSessionRequest,
    ) -> Result<CompletedMarketSessionCandidateSnapshot, CompletedMarketSessionEvidenceAccessError>;

    fn validate_currentness(
        &self,
        identity: &CompletedMarketSessionCurrentnessIdentity,
        evaluated_at: Timestamp,
    ) -> CompletedMarketSessionCurrentnessResolution;
}

/// Generic one-coordinate completed-session selector.
#[derive(Clone)]
pub(crate) struct CompletedMarketSessionAuthority {
    evidence: Arc<dyn CompletedMarketSessionEvidenceAuthority>,
}

impl fmt::Debug for CompletedMarketSessionAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletedMarketSessionAuthority")
            .field("evidence", &self.evidence)
            .finish()
    }
}

impl CompletedMarketSessionAuthority {
    pub(crate) const fn new(evidence: Arc<dyn CompletedMarketSessionEvidenceAuthority>) -> Self {
        Self { evidence }
    }

    /// Selects the unique latest eligible period after identical positive pre/post currentness
    /// checks. It never falls back to an older candidate when the latest evidence is stale.
    pub(crate) fn resolve(
        &self,
        request: CompletedMarketSessionRequest,
    ) -> Result<CompletedMarketSessionResolution, CompletedMarketSessionError> {
        let snapshot = match self.evidence.candidate_snapshot(&request) {
            Ok(snapshot) => snapshot,
            Err(CompletedMarketSessionEvidenceAccessError::Unavailable) => {
                return Ok(CompletedMarketSessionResolution::Unavailable(
                    CompletedMarketSessionUnavailable::CurrentnessUnproven,
                ));
            }
            Err(CompletedMarketSessionEvidenceAccessError::Conflict) => {
                return Ok(CompletedMarketSessionResolution::Unavailable(
                    CompletedMarketSessionUnavailable::Conflict,
                ));
            }
        };
        validate_snapshot(&request, &snapshot)?;
        let precheck = match self.currentness(&request, snapshot.currentness())? {
            CurrentnessCheck::Current(receipt) => receipt,
            CurrentnessCheck::Unavailable(reason) => {
                return Ok(CompletedMarketSessionResolution::Unavailable(reason));
            }
        };
        let selection = select_latest(&request, snapshot.candidates())?;
        let postcheck = match self.currentness(&request, snapshot.currentness())? {
            CurrentnessCheck::Current(receipt) => receipt,
            CurrentnessCheck::Unavailable(reason) => {
                return Ok(CompletedMarketSessionResolution::Unavailable(reason));
            }
        };
        if precheck != postcheck {
            return Ok(CompletedMarketSessionResolution::Unavailable(
                CompletedMarketSessionUnavailable::Conflict,
            ));
        }
        let selected_ordinal = match selection {
            CandidateSelection::None => {
                return Ok(CompletedMarketSessionResolution::Unavailable(
                    CompletedMarketSessionUnavailable::NoCompletedPeriod,
                ));
            }
            CandidateSelection::Conflict => {
                return Ok(CompletedMarketSessionResolution::Unavailable(
                    CompletedMarketSessionUnavailable::Conflict,
                ));
            }
            CandidateSelection::Unique(index) => index,
        };
        let candidate = snapshot
            .candidates
            .get(selected_ordinal)
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?
            .clone();
        let Some(expires_at) = qualify_selected_candidate(&request, &candidate, &precheck)? else {
            return Ok(CompletedMarketSessionResolution::Unavailable(
                CompletedMarketSessionUnavailable::Stale,
            ));
        };
        let receipt = CompletedMarketSessionReceipt::mint(
            request,
            snapshot.currentness,
            snapshot.complete_through,
            snapshot.completeness_evidence,
            snapshot.capture,
            u32::try_from(selected_ordinal)
                .map_err(|_| CompletedMarketSessionError::ResourceBoundExceeded)?,
            u32::try_from(snapshot.candidates.len())
                .map_err(|_| CompletedMarketSessionError::ResourceBoundExceeded)?,
            candidate,
            precheck,
            expires_at,
        );
        Ok(CompletedMarketSessionResolution::Available(receipt))
    }

    fn currentness(
        &self,
        request: &CompletedMarketSessionRequest,
        identity: &CompletedMarketSessionCurrentnessIdentity,
    ) -> Result<CurrentnessCheck, CompletedMarketSessionError> {
        let resolution = self
            .evidence
            .validate_currentness(identity, request.evaluated_at);
        Ok(match resolution {
            CompletedMarketSessionCurrentnessResolution::Current(receipt) => {
                if receipt.identity != *identity || receipt.checked_at != request.evaluated_at {
                    return Err(CompletedMarketSessionError::InvalidEvidence);
                }
                CurrentnessCheck::Current(receipt)
            }
            CompletedMarketSessionCurrentnessResolution::CurrentnessUnproven => {
                CurrentnessCheck::Unavailable(
                    CompletedMarketSessionUnavailable::CurrentnessUnproven,
                )
            }
            CompletedMarketSessionCurrentnessResolution::Stale => {
                CurrentnessCheck::Unavailable(CompletedMarketSessionUnavailable::Stale)
            }
            CompletedMarketSessionCurrentnessResolution::Revoked => {
                CurrentnessCheck::Unavailable(CompletedMarketSessionUnavailable::Revoked)
            }
            CompletedMarketSessionCurrentnessResolution::Conflict => {
                CurrentnessCheck::Unavailable(CompletedMarketSessionUnavailable::Conflict)
            }
        })
    }
}

enum CurrentnessCheck {
    Current(CompletedMarketSessionCurrentnessReceipt),
    Unavailable(CompletedMarketSessionUnavailable),
}

enum CandidateSelection {
    None,
    Unique(usize),
    Conflict,
}

/// Fail-closed, expected absence from a completed-session query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletedMarketSessionUnavailable {
    NoCompletedPeriod,
    CurrentnessUnproven,
    Stale,
    Revoked,
    Conflict,
}

/// Available exact receipt or a typed fail-closed disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CompletedMarketSessionResolution {
    Available(CompletedMarketSessionReceipt),
    Unavailable(CompletedMarketSessionUnavailable),
}

/// Non-forgeable exact result of one completed-session selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedMarketSessionReceipt {
    request: CompletedMarketSessionRequest,
    currentness: CompletedMarketSessionCurrentnessIdentity,
    complete_through: Timestamp,
    completeness_evidence: EvidenceDigest,
    capture: SealedProviderCaptureSetReceipt,
    selected_ordinal: u32,
    candidate_count: u32,
    candidate: CompletedMarketSessionCandidate,
    currentness_receipt: CompletedMarketSessionCurrentnessReceipt,
    expires_at: Timestamp,
    digest: EvidenceDigest,
}

impl CompletedMarketSessionReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "the private mint binds every independently governed selection coordinate"
    )]
    fn mint(
        request: CompletedMarketSessionRequest,
        currentness: CompletedMarketSessionCurrentnessIdentity,
        complete_through: Timestamp,
        completeness_evidence: EvidenceDigest,
        capture: SealedProviderCaptureSetReceipt,
        selected_ordinal: u32,
        candidate_count: u32,
        candidate: CompletedMarketSessionCandidate,
        currentness_receipt: CompletedMarketSessionCurrentnessReceipt,
        expires_at: Timestamp,
    ) -> Self {
        let digest = completed_session_receipt_digest(
            &request,
            &currentness,
            complete_through,
            completeness_evidence,
            &capture,
            selected_ordinal,
            candidate_count,
            &candidate,
            &currentness_receipt,
            expires_at,
        );
        Self {
            request,
            currentness,
            complete_through,
            completeness_evidence,
            capture,
            selected_ordinal,
            candidate_count,
            candidate,
            currentness_receipt,
            expires_at,
            digest,
        }
    }

    pub(crate) const fn request(&self) -> &CompletedMarketSessionRequest {
        &self.request
    }

    pub(crate) const fn period(&self) -> &BarTimeSemantics {
        &self.candidate.period
    }

    pub(crate) const fn session(&self) -> &MarketBarSessionEvidence {
        self.candidate.period.session()
    }

    pub(crate) const fn calendar_id(&self) -> &SourceIdentifier {
        &self.currentness.calendar_id
    }

    pub(crate) const fn calendar_effective(&self) -> EffectiveInterval {
        self.candidate.calendar_effective
    }

    pub(crate) const fn calendar_available_at(&self) -> Timestamp {
        self.candidate.calendar_available_at
    }

    pub(crate) const fn knowledge_available_at(&self) -> Timestamp {
        self.candidate.knowledge_available_at
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.currentness.source_id
    }

    pub(crate) const fn metadata_revision(&self) -> &MetadataRevision {
        &self.currentness.metadata_revision
    }

    pub(crate) const fn capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.capture
    }

    pub(crate) const fn complete_through(&self) -> Timestamp {
        self.complete_through
    }

    pub(crate) const fn completeness_evidence(&self) -> EvidenceDigest {
        self.completeness_evidence
    }

    pub(crate) const fn selected_ordinal(&self) -> u32 {
        self.selected_ordinal
    }

    pub(crate) const fn candidate_count(&self) -> u32 {
        self.candidate_count
    }

    pub(crate) const fn currentness_receipt(&self) -> &CompletedMarketSessionCurrentnessReceipt {
        &self.currentness_receipt
    }

    /// Returns the exclusive expiry bounded by calendar, capture, and currentness evidence.
    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Structural construction or injected-evidence contract failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CompletedMarketSessionError {
    #[error("completed-session cutoffs are invalid")]
    InvalidRequest,
    #[error("completed-session evidence is structurally invalid")]
    InvalidEvidence,
    #[error("completed-session resource bound was exceeded")]
    ResourceBoundExceeded,
}

fn validate_snapshot(
    request: &CompletedMarketSessionRequest,
    snapshot: &CompletedMarketSessionCandidateSnapshot,
) -> Result<(), CompletedMarketSessionError> {
    if snapshot.request_digest != request.digest
        || snapshot.complete_through != request.completion_cutoff
        || snapshot.currentness.venue_id != request.venue_id
        || snapshot.currentness.timeframe != request.timeframe
        || snapshot.currentness.evidence_series != request.evidence_series
        || snapshot.candidates.len() > MAXIMUM_COMPLETED_SESSION_CANDIDATES
    {
        return Err(CompletedMarketSessionError::InvalidEvidence);
    }
    require_nonzero(snapshot.completeness_evidence)?;
    validate_candidate_sequence(
        request,
        &snapshot.currentness,
        &snapshot.capture,
        &snapshot.candidates,
    )
}

fn validate_candidate_sequence(
    request: &CompletedMarketSessionRequest,
    currentness: &CompletedMarketSessionCurrentnessIdentity,
    sealed_capture: &SealedProviderCaptureSetReceipt,
    candidates: &[CompletedMarketSessionCandidate],
) -> Result<(), CompletedMarketSessionError> {
    validate_capture(sealed_capture)?;
    let capture = sealed_capture.capture();
    let mut previous_end = None;
    for candidate in candidates {
        if currentness.venue_id != request.venue_id
            || currentness.timeframe != request.timeframe
            || currentness.evidence_series != request.evidence_series
            || capture.source_id() != &currentness.source_id
            || capture.metadata_revision() != &currentness.metadata_revision
            || capture.dataset() != &request.evidence_series
            || candidate.period.session().ruleset() != &currentness.calendar_ruleset
            || candidate.period.session().evidence() != currentness.calendar_evidence
            || candidate.capture_receipt_digest != sealed_capture.receipt_digest()
            || previous_end.is_some_and(|end| end > candidate.period.period_end_exclusive())
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        previous_end = Some(candidate.period.period_end_exclusive());
    }
    Ok(())
}

fn select_latest(
    request: &CompletedMarketSessionRequest,
    candidates: &[CompletedMarketSessionCandidate],
) -> Result<CandidateSelection, CompletedMarketSessionError> {
    let mut selected = None;
    let mut selected_end = None;
    let mut conflict = false;
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.period.period_end_exclusive() > request.completion_cutoff
            || candidate.knowledge_available_at > request.knowledge_cutoff
        {
            continue;
        }
        match selected_end {
            None => {
                selected = Some(index);
                selected_end = Some(candidate.period.period_end_exclusive());
                conflict = false;
            }
            Some(end) if candidate.period.period_end_exclusive() > end => {
                selected = Some(index);
                selected_end = Some(candidate.period.period_end_exclusive());
                conflict = false;
            }
            Some(end) if candidate.period.period_end_exclusive() == end => {
                conflict = true;
            }
            Some(_) => return Err(CompletedMarketSessionError::InvalidEvidence),
        }
    }
    Ok(if conflict {
        CandidateSelection::Conflict
    } else if let Some(index) = selected {
        CandidateSelection::Unique(index)
    } else {
        CandidateSelection::None
    })
}

fn qualify_selected_candidate(
    request: &CompletedMarketSessionRequest,
    candidate: &CompletedMarketSessionCandidate,
    currentness: &CompletedMarketSessionCurrentnessReceipt,
) -> Result<Option<Timestamp>, CompletedMarketSessionError> {
    let evaluated_at = request.evaluated_at;
    let effective = candidate.calendar_effective;
    if evaluated_at < candidate.calendar_available_at
        || evaluated_at < effective.starts_at()
        || effective
            .ends_at()
            .is_some_and(|ends_at| evaluated_at >= ends_at)
        || evaluated_at >= candidate.expires_at
        || evaluated_at >= currentness.expires_at
    {
        return Ok(None);
    }
    let mut expires_at = candidate.expires_at.min(currentness.expires_at);
    if let Some(effective_end) = effective.ends_at() {
        expires_at = expires_at.min(effective_end);
    }
    if expires_at <= evaluated_at {
        return Err(CompletedMarketSessionError::InvalidEvidence);
    }
    Ok(Some(expires_at))
}

fn validate_capture(
    sealed: &SealedProviderCaptureSetReceipt,
) -> Result<(), CompletedMarketSessionError> {
    for evidence in [
        sealed.capture().request_set_identity(),
        sealed.capture().content_digest(),
        sealed.capture().observation_digest(),
        sealed.segment().content_digest(),
        sealed.segment().physical_receipt_digest(),
        sealed.receipt_digest(),
    ] {
        require_nonzero(evidence)?;
    }
    if sealed.capture().pages().is_empty()
        || sealed.capture().pages().len() != sealed.segment().frames().len()
    {
        return Err(CompletedMarketSessionError::InvalidEvidence);
    }
    Ok(())
}

fn require_nonzero(evidence: EvidenceDigest) -> Result<(), CompletedMarketSessionError> {
    if evidence.bytes() == [0; 32] {
        Err(CompletedMarketSessionError::InvalidEvidence)
    } else {
        Ok(())
    }
}

fn request_digest(
    venue_id: &VenueId,
    timeframe: &SourceIdentifier,
    evidence_series: &SourceIdentifier,
    completion_cutoff: Timestamp,
    knowledge_cutoff: Timestamp,
    evaluated_at: Timestamp,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/completed-market-session-request/v1\0");
    hash_text(&mut digest, venue_id.as_str());
    hash_text(&mut digest, timeframe.as_str());
    hash_text(&mut digest, evidence_series.as_str());
    hash_timestamp(&mut digest, completion_cutoff);
    hash_timestamp(&mut digest, knowledge_cutoff);
    hash_timestamp(&mut digest, evaluated_at);
    sha256_evidence(digest)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds every independently governed selection coordinate"
)]
fn completed_session_receipt_digest(
    request: &CompletedMarketSessionRequest,
    currentness: &CompletedMarketSessionCurrentnessIdentity,
    complete_through: Timestamp,
    completeness_evidence: EvidenceDigest,
    sealed_capture: &SealedProviderCaptureSetReceipt,
    selected_ordinal: u32,
    candidate_count: u32,
    candidate: &CompletedMarketSessionCandidate,
    currentness_receipt: &CompletedMarketSessionCurrentnessReceipt,
    expires_at: Timestamp,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/completed-market-session-receipt/v1\0");
    hash_evidence(&mut digest, request.digest);
    hash_text(&mut digest, currentness.source_id.as_str());
    hash_text(
        &mut digest,
        currentness
            .metadata_revision
            .as_source_identifier()
            .as_str(),
    );
    hash_text(&mut digest, currentness.venue_id.as_str());
    hash_text(&mut digest, currentness.timeframe.as_str());
    hash_text(&mut digest, currentness.evidence_series.as_str());
    hash_text(&mut digest, currentness.calendar_id.as_str());
    hash_text(&mut digest, currentness.calendar_ruleset.as_str());
    hash_evidence(&mut digest, currentness.calendar_evidence);
    hash_evidence(&mut digest, currentness.source_generation);
    hash_evidence(&mut digest, currentness.revocation_identity);
    hash_timestamp(&mut digest, complete_through);
    hash_evidence(&mut digest, completeness_evidence);
    digest.update(selected_ordinal.to_be_bytes());
    digest.update(candidate_count.to_be_bytes());
    let period = &candidate.period;
    hash_timestamp(&mut digest, period.period_start());
    hash_timestamp(&mut digest, period.period_end_exclusive());
    digest.update([timestamp_basis_tag(period.timestamp_basis())]);
    digest.update([session_kind_tag(period.session().kind())]);
    hash_text(&mut digest, period.session().ruleset().as_str());
    hash_evidence(&mut digest, period.session().evidence());
    hash_timestamp(&mut digest, candidate.calendar_effective.starts_at());
    hash_optional_timestamp(&mut digest, candidate.calendar_effective.ends_at());
    hash_timestamp(&mut digest, candidate.calendar_available_at);
    hash_timestamp(&mut digest, candidate.knowledge_available_at);
    hash_timestamp(&mut digest, candidate.expires_at);
    let capture = sealed_capture.capture();
    hash_text(&mut digest, capture.source_id().as_str());
    hash_text(
        &mut digest,
        capture.metadata_revision().as_source_identifier().as_str(),
    );
    hash_text(&mut digest, capture.dataset().as_str());
    hash_evidence(&mut digest, capture.request_set_identity());
    digest.update([capture_terminal_tag(capture.terminal())]);
    digest.update(capture.total_body_bytes().to_be_bytes());
    digest.update((capture.pages().len() as u64).to_be_bytes());
    hash_evidence(&mut digest, capture.content_digest());
    hash_evidence(&mut digest, capture.observation_digest());
    hash_evidence(&mut digest, sealed_capture.segment().content_digest());
    hash_evidence(
        &mut digest,
        sealed_capture.segment().physical_receipt_digest(),
    );
    hash_evidence(&mut digest, sealed_capture.receipt_digest());
    hash_evidence(&mut digest, candidate.capture_receipt_digest);
    hash_timestamp(&mut digest, currentness_receipt.checked_at);
    hash_timestamp(&mut digest, currentness_receipt.expires_at);
    hash_evidence(&mut digest, currentness_receipt.evidence);
    hash_timestamp(&mut digest, expires_at);
    sha256_evidence(digest)
}

fn hash_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn hash_timestamp(digest: &mut Sha256, timestamp: Timestamp) {
    digest.update(timestamp.unix_nanos().to_be_bytes());
}

fn hash_optional_timestamp(digest: &mut Sha256, timestamp: Option<Timestamp>) {
    match timestamp {
        Some(timestamp) => {
            digest.update([1]);
            hash_timestamp(digest, timestamp);
        }
        None => digest.update([0]),
    }
}

fn hash_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn sha256_evidence(digest: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

const fn timestamp_basis_tag(basis: BarTimestampBasis) -> u8 {
    match basis {
        BarTimestampBasis::PeriodStart => 1,
        BarTimestampBasis::PeriodEnd => 2,
    }
}

const fn session_kind_tag(kind: MarketBarSessionKind) -> u8 {
    match kind {
        MarketBarSessionKind::Regular => 1,
        MarketBarSessionKind::Extended => 2,
        MarketBarSessionKind::Continuous => 3,
        MarketBarSessionKind::ProviderDefined => 4,
    }
}

const fn capture_terminal_tag(terminal: ProviderCaptureTerminalDisposition) -> u8 {
    match terminal {
        ProviderCaptureTerminalDisposition::StandaloneResponse => 1,
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage => 2,
        ProviderCaptureTerminalDisposition::CompleteRequestGraph => 3,
    }
}
