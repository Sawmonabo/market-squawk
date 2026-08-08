//! Application-owned candidate-dossier preparation and one-use confirmation receipts.

use std::{
    fmt,
    time::{Duration, Instant},
};

use market_squawk_decisions::{
    AppendOutcome, CandidateId, DecisionAuthority, DecisionContentDigest, DecisionDossier, Dossier,
    DossierEvidence, DossierId, DossierReference, DossierSection, ScreenRunId,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, Timestamp};
use market_squawk_runtime::{ServiceGeneration, WorkspaceId};
use market_squawk_services::RequestOrigin;
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use uuid::Uuid;

use super::{DecisionApplicationError, DecisionState, persist_outcome};

const MAXIMUM_PREPARED_DOSSIERS: usize = 256;
const RECEIPT_LIFETIME: Duration = Duration::from_secs(300);
const RECEIPT_LIFETIME_NANOS: i64 = 300_000_000_000;
const ID_ALLOCATION_ATTEMPTS: usize = 16;
const DOSSIER_DIGEST_DOMAIN: &[u8] = b"market-squawk/decision-dossier/v1\0";

/// Dossier preparation or confirmation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DossierPreparationError {
    /// The request contained an invalid or duplicate evidence selection.
    InvalidRequest,
    /// The selected candidate or evidence option is not retained.
    NotFound,
    /// Candidate evidence changed or a stable identity was already consumed differently.
    Conflict,
    /// The one-use receipt elapsed before confirmation.
    Expired,
    /// The receipt belongs to another client, workspace, or service generation.
    FenceMismatch,
    /// A fixed retained preparation bound was reached.
    Capacity,
    /// The durable decision application rejected the operation.
    Application(DecisionApplicationError),
}

impl fmt::Display for DossierPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest => formatter.write_str("dossier preparation request is invalid"),
            Self::NotFound => formatter.write_str("candidate dossier evidence was not found"),
            Self::Conflict => formatter.write_str("candidate dossier evidence changed"),
            Self::Expired => formatter.write_str("dossier preparation receipt expired"),
            Self::FenceMismatch => formatter.write_str("dossier preparation fence mismatch"),
            Self::Capacity => formatter.write_str("dossier preparation capacity is exhausted"),
            Self::Application(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for DossierPreparationError {}

impl From<DecisionApplicationError> for DossierPreparationError {
    fn from(error: DecisionApplicationError) -> Self {
        Self::Application(error)
    }
}

/// Exact installed request boundary to which a dossier receipt is confined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DossierPreparationFence {
    origin: RequestOrigin,
    workspace_id: WorkspaceId,
    service_generation: ServiceGeneration,
}

impl DossierPreparationFence {
    /// Constructs a fence only for the active installed workspace.
    pub fn try_new(
        origin: RequestOrigin,
        workspace_id: WorkspaceId,
        service_generation: ServiceGeneration,
    ) -> Result<Self, DossierPreparationError> {
        if origin.workspace_id() != workspace_id.as_uuid() {
            return Err(DossierPreparationError::FenceMismatch);
        }
        Ok(Self {
            origin,
            workspace_id,
            service_generation,
        })
    }
}

/// Closed evidence options derived from one retained candidate and its exact run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DossierEvidenceSelection {
    /// Candidate row/lineage evidence retained by the screen execution.
    Candidate,
    /// Exact point-in-time dataset generation consumed by the parent run.
    Dataset,
    /// Exact historical universe consumed by the parent run.
    Universe,
    /// Immutable portfolio-impact revision already retained by the candidate.
    PortfolioImpact,
}

/// Authority-free presentation request for dossier assembly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DossierPreparationDraft {
    pub candidate_id: CandidateId,
    pub evidence: Vec<DossierEvidenceSelection>,
}

/// Selectable evidence availability for one retained candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DossierEvidenceInventory {
    pub candidate_id: CandidateId,
    pub screen_run_id: ScreenRunId,
    pub instrument_id: InstrumentId,
    pub selected_at: Timestamp,
    pub portfolio_impact_available: bool,
}

/// Opaque one-use confirmation receipt.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct DossierPreparationReceipt(Uuid);

impl DossierPreparationReceipt {
    /// Parses a receipt returned by the same installed application authority.
    pub fn parse(value: &str) -> Result<Self, DossierPreparationError> {
        let value =
            Uuid::parse_str(value).map_err(|_error| DossierPreparationError::InvalidRequest)?;
        if value.is_nil() {
            return Err(DossierPreparationError::InvalidRequest);
        }
        Ok(Self(value))
    }
}

impl fmt::Display for DossierPreparationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl fmt::Debug for DossierPreparationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DossierPreparationReceipt([OPAQUE])")
    }
}

/// Human-safe preview of the exact dossier retained behind a one-use receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedDossierPreview {
    pub receipt: DossierPreparationReceipt,
    pub dossier_id: DossierId,
    pub candidate_id: CandidateId,
    pub screen_run_id: ScreenRunId,
    pub instrument_id: InstrumentId,
    pub evidence: Vec<DossierEvidenceSelection>,
    pub assembled_at: Timestamp,
    pub receipt_expires_at: Timestamp,
}

#[derive(Clone, Debug)]
struct PreparedDossier {
    receipt: DossierPreparationReceipt,
    fence: DossierPreparationFence,
    expires_at: Timestamp,
    deadline: Instant,
    binding_digest: [u8; 32],
    run_id: ScreenRunId,
    candidate_identity: DecisionContentDigest,
    dossier: DecisionDossier,
}

#[derive(Debug, Default)]
pub(super) struct DossierPreparationAuthority {
    prepared: Vec<PreparedDossier>,
}

impl DossierPreparationAuthority {
    pub(super) fn inventory(
        &self,
        authority: &DecisionAuthority,
        candidate_id: &CandidateId,
    ) -> Result<DossierEvidenceInventory, DossierPreparationError> {
        let (run, candidate) = authority
            .get_candidate(candidate_id)
            .map_err(|_error| DossierPreparationError::NotFound)?;
        Ok(DossierEvidenceInventory {
            candidate_id: candidate_id.clone(),
            screen_run_id: run.id().clone(),
            instrument_id: candidate.record().instrument_id(),
            selected_at: candidate.record().selected_at(),
            portfolio_impact_available: candidate.portfolio_impact().is_some(),
        })
    }

    pub(super) fn prepare(
        &mut self,
        authority: &DecisionAuthority,
        fence: DossierPreparationFence,
        mut draft: DossierPreparationDraft,
        now: Timestamp,
    ) -> Result<PreparedDossierPreview, DossierPreparationError> {
        let monotonic_now = Instant::now();
        self.prepared
            .retain(|entry| entry.expires_at > now && entry.deadline > monotonic_now);
        if self.prepared.len() >= MAXIMUM_PREPARED_DOSSIERS {
            return Err(DossierPreparationError::Capacity);
        }
        if draft.evidence.is_empty()
            || draft.evidence.len() > 4
            || draft
                .evidence
                .iter()
                .enumerate()
                .any(|(index, item)| draft.evidence[index + 1..].contains(item))
            || !draft
                .evidence
                .contains(&DossierEvidenceSelection::Candidate)
            || !draft.evidence.contains(&DossierEvidenceSelection::Dataset)
            || !draft.evidence.contains(&DossierEvidenceSelection::Universe)
        {
            return Err(DossierPreparationError::InvalidRequest);
        }
        draft
            .evidence
            .sort_unstable_by_key(|selection| selection_ordinal(*selection));
        let (run, candidate) = authority
            .get_candidate(&draft.candidate_id)
            .map_err(|_error| DossierPreparationError::NotFound)?;
        if candidate.record().selected_at() > now {
            return Err(DossierPreparationError::InvalidRequest);
        }
        let portfolio = if draft
            .evidence
            .contains(&DossierEvidenceSelection::PortfolioImpact)
        {
            Some(
                candidate
                    .portfolio_impact()
                    .cloned()
                    .ok_or(DossierPreparationError::NotFound)?,
            )
        } else {
            None
        };
        let references = draft
            .evidence
            .iter()
            .map(|selection| match selection {
                DossierEvidenceSelection::Candidate => Ok(DossierReference::new(
                    DossierSection::DecisionContext,
                    candidate.evidence_identity(),
                )),
                DossierEvidenceSelection::Dataset => Ok(DossierReference::new(
                    DossierSection::Data,
                    run.dataset_identity(),
                )),
                DossierEvidenceSelection::Universe => Ok(DossierReference::new(
                    DossierSection::Data,
                    run.universe_identity(),
                )),
                DossierEvidenceSelection::PortfolioImpact => {
                    let token = portfolio
                        .as_ref()
                        .ok_or(DossierPreparationError::NotFound)?;
                    Ok(DossierReference::new(
                        DossierSection::PortfolioImpact,
                        DecisionContentDigest::try_new(EvidenceDigest::new(
                            DigestAlgorithm::Sha256,
                            token.bytes(),
                        ))
                        .map_err(|_error| DossierPreparationError::InvalidRequest)?,
                    ))
                }
            })
            .collect::<Result<Vec<_>, DossierPreparationError>>()?;
        let dossier_id = allocate_dossier_id(authority, &self.prepared)?;
        let content_identity = dossier_identity(
            &dossier_id,
            candidate.record().id(),
            candidate.record().instrument_id(),
            now,
            &references,
            portfolio.as_ref().map(|token| token.bytes()),
        )?;
        let evidence = DossierEvidence::new(None, portfolio, None, content_identity);
        let dossier = Dossier::try_new(dossier_id.clone(), candidate.record(), now, evidence)
            .map_err(|_error| DossierPreparationError::InvalidRequest)?;
        let dossier = DecisionDossier::try_new(dossier, references)
            .map_err(|_error| DossierPreparationError::InvalidRequest)?;
        let receipt = allocate_receipt(&self.prepared)?;
        let expires_at = now
            .checked_add_nanos(RECEIPT_LIFETIME_NANOS)
            .map_err(|_error| DossierPreparationError::InvalidRequest)?;
        let deadline = monotonic_now
            .checked_add(RECEIPT_LIFETIME)
            .ok_or(DossierPreparationError::Capacity)?;
        let binding_digest = receipt_digest(receipt, fence, expires_at, content_identity);
        self.prepared.push(PreparedDossier {
            receipt,
            fence,
            expires_at,
            deadline,
            binding_digest,
            run_id: run.id().clone(),
            candidate_identity: candidate.evidence_identity(),
            dossier,
        });
        Ok(PreparedDossierPreview {
            receipt,
            dossier_id,
            candidate_id: draft.candidate_id,
            screen_run_id: run.id().clone(),
            instrument_id: candidate.record().instrument_id(),
            evidence: draft.evidence,
            assembled_at: now,
            receipt_expires_at: expires_at,
        })
    }

    fn take(
        &mut self,
        receipt: DossierPreparationReceipt,
        fence: DossierPreparationFence,
        now: Timestamp,
    ) -> Result<PreparedDossier, DossierPreparationError> {
        let index = self
            .prepared
            .iter()
            .position(|entry| entry.receipt == receipt)
            .ok_or(DossierPreparationError::NotFound)?;
        if self.prepared[index].fence != fence {
            return Err(DossierPreparationError::FenceMismatch);
        }
        let prepared = self.prepared.remove(index);
        if prepared.expires_at <= now || Instant::now() >= prepared.deadline {
            return Err(DossierPreparationError::Expired);
        }
        let expected = receipt_digest(
            prepared.receipt,
            prepared.fence,
            prepared.expires_at,
            prepared.dossier.dossier().evidence().content_identity(),
        );
        if !bool::from(prepared.binding_digest.ct_eq(&expected)) {
            return Err(DossierPreparationError::Conflict);
        }
        Ok(prepared)
    }
}

pub(super) fn consume_prepared(
    state: &mut DecisionState,
    receipt: DossierPreparationReceipt,
    fence: DossierPreparationFence,
    now: Timestamp,
) -> Result<AppendOutcome, DossierPreparationError> {
    let prepared = state.dossier_preparation.take(receipt, fence, now)?;
    let (run, candidate) = state
        .authority
        .get_candidate(prepared.dossier.dossier().candidate_id())
        .map_err(|_error| DossierPreparationError::Conflict)?;
    if run.id() != &prepared.run_id
        || candidate.record().instrument_id() != prepared.dossier.dossier().instrument_id()
        || candidate.evidence_identity() != prepared.candidate_identity
    {
        return Err(DossierPreparationError::Conflict);
    }
    let encoded = super::codec::dossier(&prepared.dossier)?;
    let outcome = state
        .authority
        .append_dossier(prepared.dossier)
        .map_err(DecisionApplicationError::from)?;
    persist_outcome(state, &encoded, outcome).map_err(Into::into)
}

fn allocate_dossier_id(
    authority: &DecisionAuthority,
    prepared: &[PreparedDossier],
) -> Result<DossierId, DossierPreparationError> {
    (0..ID_ALLOCATION_ATTEMPTS)
        .map(|_attempt| DossierId::try_new(format!("dossier.{}", Uuid::new_v4().simple())))
        .find_map(|candidate| match candidate {
            Ok(candidate)
                if authority.get_dossier(&candidate).is_err()
                    && prepared
                        .iter()
                        .all(|entry| entry.dossier.dossier().id() != &candidate) =>
            {
                Some(Ok(candidate))
            }
            Ok(_) => None,
            Err(_error) => Some(Err(DossierPreparationError::InvalidRequest)),
        })
        .transpose()?
        .ok_or(DossierPreparationError::Capacity)
}

fn allocate_receipt(
    prepared: &[PreparedDossier],
) -> Result<DossierPreparationReceipt, DossierPreparationError> {
    (0..ID_ALLOCATION_ATTEMPTS)
        .map(|_attempt| DossierPreparationReceipt(Uuid::new_v4()))
        .find(|candidate| prepared.iter().all(|entry| entry.receipt != *candidate))
        .ok_or(DossierPreparationError::Capacity)
}

fn dossier_identity(
    dossier_id: &DossierId,
    candidate_id: &CandidateId,
    instrument_id: InstrumentId,
    assembled_at: Timestamp,
    references: &[DossierReference],
    portfolio: Option<[u8; 32]>,
) -> Result<DecisionContentDigest, DossierPreparationError> {
    let mut hash = Sha256::new();
    hash.update(DOSSIER_DIGEST_DOMAIN);
    hash_string(&mut hash, dossier_id.as_str())?;
    hash_string(&mut hash, candidate_id.as_str())?;
    hash.update(instrument_id.as_uuid().as_bytes());
    hash.update(assembled_at.unix_nanos().to_be_bytes());
    hash.update(
        u64::try_from(references.len())
            .map_err(|_error| DossierPreparationError::Capacity)?
            .to_be_bytes(),
    );
    for reference in references {
        hash.update([section_ordinal(reference.section())]);
        hash.update(reference.content_identity().evidence_digest().bytes());
    }
    match portfolio {
        Some(portfolio) => {
            hash.update([1]);
            hash.update(portfolio);
        }
        None => hash.update([0]),
    }
    DecisionContentDigest::try_new(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
    .map_err(|_error| DossierPreparationError::InvalidRequest)
}

fn receipt_digest(
    receipt: DossierPreparationReceipt,
    fence: DossierPreparationFence,
    expires_at: Timestamp,
    content_identity: DecisionContentDigest,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/dossier-preparation-receipt/v1\0");
    hash.update(receipt.0.as_bytes());
    hash.update(fence.origin.client_id().as_bytes());
    hash.update(fence.origin.workspace_id().as_bytes());
    hash.update(fence.workspace_id.as_uuid().as_bytes());
    hash.update(fence.service_generation.get().to_be_bytes());
    hash.update(expires_at.unix_nanos().to_be_bytes());
    hash.update(content_identity.evidence_digest().bytes());
    hash.finalize().into()
}

fn hash_string(hash: &mut Sha256, value: &str) -> Result<(), DossierPreparationError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_error| DossierPreparationError::Capacity)?
            .to_be_bytes(),
    );
    hash.update(value.as_bytes());
    Ok(())
}

const fn selection_ordinal(selection: DossierEvidenceSelection) -> u8 {
    match selection {
        DossierEvidenceSelection::Candidate => 1,
        DossierEvidenceSelection::Dataset => 2,
        DossierEvidenceSelection::Universe => 3,
        DossierEvidenceSelection::PortfolioImpact => 4,
    }
}

const fn section_ordinal(section: DossierSection) -> u8 {
    match section {
        DossierSection::Data => 1,
        DossierSection::CorporateActions => 2,
        DossierSection::Fundamentals => 3,
        DossierSection::Forecast => 4,
        DossierSection::PortfolioImpact => 5,
        DossierSection::FairValue => 6,
        DossierSection::DecisionContext => 7,
    }
}
