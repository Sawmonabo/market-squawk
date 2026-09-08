//! Canonical-instrument company and fund research reads.
//!
//! Presentation callers supply only a canonical instrument, point-in-time cutoffs, and a closed
//! revision policy. Immutable generations, source identities, raw-object receipts, provider
//! bindings, filing coordinates, and content digests are selected by the data authority and remain
//! private in this leaf.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use market_squawk_data::{
    ArrowConversionError, FundLatestUnavailableReason, FundPointInTimeOutcome, IngestError,
    PointInTimeLimits, PointInTimeRevisionMode, PointInTimeRevisionState, SecFundJobFamily,
    SecFundPointInTimeReadOutcome, SecFundPointInTimeReadRequest, SecResearchDisposition,
    SecResearchFamily, SecResearchIdentityOutcome, SecResearchIdentityReadRequest,
    SecResearchIdentitySelection, SecResearchReadError,
};
use market_squawk_domain::{
    CalendarDate, EvidenceDigest, FundSourceFamily, FundamentalAmendmentStatus, FundamentalCadence,
    FundamentalConsolidation, FundamentalPeriod, FundamentalRestatementStatus, InstrumentId,
    ResearchContext, ResearchObservation, ResearchTemporalCoordinate, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::instrument_context::{
    InstrumentContextOutcome, InstrumentContextRead, InstrumentContextReadCapability,
    InstrumentContextReadError, InstrumentContextRequest,
};
use super::sec_fund_product::{FundResearchData, SecFundProductBoundaryError};
use super::{
    company_product::{
        CompanyProductProjectionError, CompanyProductResult, ResearchProductIdentity,
        project_company_product,
    },
    fund_product::{
        FundProductProjectionError, FundProductReadSet, FundProductResult, project_fund_product,
    },
};
use crate::ResearchService;

const MAX_COMPANY_RESEARCH_CANDIDATES: usize = 65_536;
const MAX_COMPANY_RESEARCH_FAMILIES: usize = 65_536;
const MAX_COMPANY_RESEARCH_CONFLICTS: usize = 1_024;
const MAX_COMPANY_RESEARCH_RESULT_ROWS: usize = 65_536;
const MAX_COMPANY_RESEARCH_RETAINED_BYTES: usize = 128 * 1024 * 1024;
const MAX_COMPANY_RESEARCH_OBJECT_BYTES: usize = 256 * 1024 * 1024;
const MAX_FUND_RESEARCH_RECORDS: usize = 65_536;
const COMPANY_RESEARCH_FAMILY_COUNT: usize = 3;

/// Product-level point-in-time revision policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResearchRevisionPolicy {
    LatestKnown,
    AllKnown,
}

impl ResearchRevisionPolicy {
    const fn data_policy(self) -> PointInTimeRevisionMode {
        match self {
            Self::LatestKnown => PointInTimeRevisionMode::LatestKnown,
            Self::AllKnown => PointInTimeRevisionMode::AllKnown,
        }
    }
}

/// Canonical company-research request with no provider or storage coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompanyResearchRequest {
    instrument_id: InstrumentId,
    knowledge_at: Timestamp,
    fact_effective_cutoff: ResearchTemporalCoordinate,
    revision_policy: ResearchRevisionPolicy,
}

impl CompanyResearchRequest {
    pub(crate) fn try_new(
        instrument_id: InstrumentId,
        knowledge_at: Timestamp,
        fact_effective_cutoff: ResearchTemporalCoordinate,
        revision_policy: ResearchRevisionPolicy,
    ) -> Result<Self, CanonicalResearchReadError> {
        if fact_effective_cutoff
            .exact_timestamp()
            .is_some_and(|cutoff| cutoff > knowledge_at)
        {
            return Err(CanonicalResearchReadError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            knowledge_at,
            fact_effective_cutoff,
            revision_policy,
        })
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
    pub(crate) const fn fact_effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.fact_effective_cutoff
    }
    pub(crate) const fn revision_policy(&self) -> ResearchRevisionPolicy {
        self.revision_policy
    }
}

/// Product meaning for the two admitted fund-report families.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FundResearchFamily {
    PortfolioHoldings,
    AnnualFundReport,
}

impl FundResearchFamily {
    const fn data_family(self) -> FundSourceFamily {
        match self {
            Self::PortfolioHoldings => FundSourceFamily::Nport,
            Self::AnnualFundReport => FundSourceFamily::Ncen,
        }
    }
}

/// Fund revision policy that cannot carry a source-native filing identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FundResearchRevisionPolicy {
    LatestKnown,
    AllKnown,
}

/// Canonical fund-research request with code-owned row and memory bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FundResearchRequest {
    fund_instrument_id: InstrumentId,
    family: FundResearchFamily,
    knowledge_at: Timestamp,
    revision_policy: FundResearchRevisionPolicy,
}

impl FundResearchRequest {
    pub(crate) const fn new(
        fund_instrument_id: InstrumentId,
        family: FundResearchFamily,
        knowledge_at: Timestamp,
        revision_policy: FundResearchRevisionPolicy,
    ) -> Self {
        Self {
            fund_instrument_id,
            family,
            knowledge_at,
            revision_policy,
        }
    }

    pub(crate) const fn fund_instrument_id(self) -> InstrumentId {
        self.fund_instrument_id
    }
    pub(crate) const fn family(self) -> FundResearchFamily {
        self.family
    }
    pub(crate) const fn knowledge_at(self) -> Timestamp {
        self.knowledge_at
    }
    pub(crate) const fn revision_policy(self) -> FundResearchRevisionPolicy {
        self.revision_policy
    }
}

/// Read-only canonical research composition over the sole local research authority.
#[derive(Clone)]
pub(crate) struct CompanyResearchReadCapability {
    research: Arc<ResearchService>,
}

impl fmt::Debug for CompanyResearchReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanyResearchReadCapability")
            .field("research", &"[LOCAL RESEARCH READ AUTHORITY]")
            .finish()
    }
}

impl CompanyResearchReadCapability {
    pub(crate) const fn new(research: Arc<ResearchService>) -> Self {
        Self { research }
    }

    /// Reads company facts and filings through the canonical-instrument selector.
    pub(crate) async fn read_company(
        &self,
        request: CompanyResearchRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompanyResearchRead, CanonicalResearchReadError> {
        check_operation(deadline, &cancellation)?;
        let point_in_time_limits = company_point_in_time_limits()?;
        let raw_store = self.research.provider_capture_store();
        let reader = self.research.analytical().sec_research_reader();
        let mut selections = Vec::new();
        selections
            .try_reserve_exact(COMPANY_RESEARCH_FAMILY_COUNT)
            .map_err(|_| CanonicalResearchReadError::ResourceExhausted)?;
        for family in [
            SecResearchFamily::CompanyFacts,
            SecResearchFamily::Submissions,
            SecResearchFamily::FilingXbrl,
        ] {
            check_operation(deadline, &cancellation)?;
            let data_request = SecResearchIdentityReadRequest::try_new(
                request.instrument_id,
                family,
                request.knowledge_at,
                request.fact_effective_cutoff.clone(),
                request.revision_policy.data_policy(),
                point_in_time_limits,
                MAX_COMPANY_RESEARCH_OBJECT_BYTES,
            )
            .map_err(map_company_data_error)?;
            let selection = reader
                .select_by_identity(
                    data_request,
                    raw_store.as_ref(),
                    deadline,
                    cancellation.child_token(),
                )
                .await
                .map_err(map_company_data_error)?;
            selections.push(selection);
        }
        check_operation(deadline, &cancellation)?;
        let outcome = project_company_research(&request, &selections)?;
        Ok(CompanyResearchRead {
            request,
            outcome,
            evidence: CompanyResearchEvidence {
                selections: selections.into_boxed_slice(),
            },
        })
    }

    /// Reopens every exact selector receipt and requires the same private and product result.
    pub(crate) async fn verify_company_restart(
        &self,
        expected: &CompanyResearchRead,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompanyResearchRead, CanonicalResearchReadError> {
        check_operation(deadline, &cancellation)?;
        let raw_store = self.research.provider_capture_store();
        let reader = self.research.analytical().sec_research_reader();
        let mut replayed = Vec::new();
        replayed
            .try_reserve_exact(expected.evidence.selections.len())
            .map_err(|_| CanonicalResearchReadError::ResourceExhausted)?;
        for selection in expected.evidence.selections.as_ref() {
            let replay = reader
                .verify_identity_restart(
                    selection,
                    raw_store.as_ref(),
                    deadline,
                    cancellation.child_token(),
                )
                .await
                .map_err(map_company_data_error)?;
            replayed.push(replay);
        }
        if replayed.as_slice() != expected.evidence.selections.as_ref() {
            return Err(CanonicalResearchReadError::RestartConflict);
        }
        let outcome = project_company_research(&expected.request, &replayed)?;
        if outcome != expected.outcome {
            return Err(CanonicalResearchReadError::RestartConflict);
        }
        Ok(CompanyResearchRead {
            request: expected.request.clone(),
            outcome,
            evidence: CompanyResearchEvidence {
                selections: replayed.into_boxed_slice(),
            },
        })
    }

    /// Reads fund holdings through canonical identity and an explicit report/revision policy.
    pub(crate) async fn read_fund(
        &self,
        request: FundResearchRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FundResearchRead, CanonicalResearchReadError> {
        check_operation(deadline, &cancellation)?;
        let data_request = fund_data_request(request)?;
        let raw_store = self.research.provider_capture_store();
        let evidence = self
            .research
            .analytical()
            .select_sec_fund_point_in_time(
                &data_request,
                raw_store.as_ref(),
                deadline,
                cancellation,
            )
            .await
            .map_err(map_fund_data_error)?;
        let outcome = project_fund_research(request, &evidence)?;
        Ok(FundResearchRead {
            request,
            outcome,
            evidence,
        })
    }

    /// Reopens the exact fund coordinate and requires identical revision and product meaning.
    pub(crate) async fn verify_fund_restart(
        &self,
        expected: &FundResearchRead,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FundResearchRead, CanonicalResearchReadError> {
        check_operation(deadline, &cancellation)?;
        let data_request = fund_data_request(expected.request)?;
        let raw_store = self.research.provider_capture_store();
        let evidence = self
            .research
            .analytical()
            .verify_sec_fund_identity_restart(
                &data_request,
                &expected.evidence,
                raw_store.as_ref(),
                deadline,
                cancellation,
            )
            .await
            .map_err(map_fund_data_error)?;
        let outcome = project_fund_research(expected.request, &evidence)?;
        if outcome != expected.outcome {
            return Err(CanonicalResearchReadError::RestartConflict);
        }
        Ok(FundResearchRead {
            request: expected.request,
            outcome,
            evidence,
        })
    }
}

/// Fixed product projection over canonical company and fund point-in-time reads.
///
/// Private restart evidence never crosses this capability. Daily NAV remains unavailable until
/// startup composition supplies an immutable canonical NAV generation selector; market price is
/// never substituted.
#[derive(Clone, Debug)]
pub(crate) struct ResearchProductReadCapability {
    canonical: CompanyResearchReadCapability,
    identity: Option<Arc<InstrumentContextReadCapability>>,
}

impl ResearchProductReadCapability {
    pub(crate) const fn new(
        canonical: CompanyResearchReadCapability,
        identity: Option<Arc<InstrumentContextReadCapability>>,
    ) -> Self {
        Self {
            canonical,
            identity,
        }
    }

    pub(crate) async fn read_company_product(
        &self,
        request: CompanyResearchRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompanyProductRead, ResearchProductReadError> {
        let canonical = self
            .canonical
            .read_company(request, deadline, cancellation.child_token())
            .await
            .map_err(ResearchProductReadError::Canonical)?;
        let identity = self.read_identity(
            canonical.request().instrument_id(),
            canonical.request().knowledge_at(),
            deadline,
            &cancellation,
        )?;
        let product = project_company_product(&canonical, product_identity(&identity)?)
            .map_err(ResearchProductReadError::CompanyProjection)?;
        Ok(CompanyProductRead {
            canonical,
            identity,
            product,
        })
    }

    pub(crate) async fn verify_company_product_restart(
        &self,
        expected: &CompanyProductRead,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompanyProductRead, ResearchProductReadError> {
        let canonical = self
            .canonical
            .verify_company_restart(&expected.canonical, deadline, cancellation.child_token())
            .await
            .map_err(ResearchProductReadError::Canonical)?;
        let identity = self.verify_identity_restart(&expected.identity, deadline, &cancellation)?;
        let product = project_company_product(&canonical, product_identity(&identity)?)
            .map_err(ResearchProductReadError::CompanyProjection)?;
        if product != expected.product {
            return Err(ResearchProductReadError::RestartConflict);
        }
        Ok(CompanyProductRead {
            canonical,
            identity,
            product,
        })
    }

    pub(crate) async fn read_fund_product(
        &self,
        fund_instrument_id: InstrumentId,
        knowledge_at: Timestamp,
        revision_policy: FundResearchRevisionPolicy,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FundProductRead, ResearchProductReadError> {
        let portfolio_request = FundResearchRequest::new(
            fund_instrument_id,
            FundResearchFamily::PortfolioHoldings,
            knowledge_at,
            revision_policy,
        );
        let annual_request = FundResearchRequest::new(
            fund_instrument_id,
            FundResearchFamily::AnnualFundReport,
            knowledge_at,
            revision_policy,
        );
        let portfolio = self
            .canonical
            .read_fund(portfolio_request, deadline, cancellation.child_token())
            .await
            .map_err(ResearchProductReadError::Canonical)?;
        let annual = self
            .canonical
            .read_fund(annual_request, deadline, cancellation.child_token())
            .await
            .map_err(ResearchProductReadError::Canonical)?;
        let identity =
            self.read_identity(fund_instrument_id, knowledge_at, deadline, &cancellation)?;
        let (holding_identities, holding_evidence) =
            self.read_holding_identities(&portfolio, knowledge_at, deadline, &cancellation)?;
        let product = project_fund_product(
            FundProductReadSet::new(&portfolio, &annual),
            None,
            product_identity(&identity)?,
            holding_identities,
        )
        .map_err(ResearchProductReadError::FundProjection)?;
        Ok(FundProductRead {
            portfolio,
            annual,
            identity,
            holding_evidence,
            product,
        })
    }

    pub(crate) async fn verify_fund_product_restart(
        &self,
        expected: &FundProductRead,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FundProductRead, ResearchProductReadError> {
        let portfolio = self
            .canonical
            .verify_fund_restart(&expected.portfolio, deadline, cancellation.child_token())
            .await
            .map_err(ResearchProductReadError::Canonical)?;
        let annual = self
            .canonical
            .verify_fund_restart(&expected.annual, deadline, cancellation.child_token())
            .await
            .map_err(ResearchProductReadError::Canonical)?;
        let identity = self.verify_identity_restart(&expected.identity, deadline, &cancellation)?;
        let (holding_identities, holding_evidence) =
            self.verify_holding_identities(&expected.holding_evidence, deadline, &cancellation)?;
        let product = project_fund_product(
            FundProductReadSet::new(&portfolio, &annual),
            None,
            product_identity(&identity)?,
            holding_identities,
        )
        .map_err(ResearchProductReadError::FundProjection)?;
        if product != expected.product {
            return Err(ResearchProductReadError::RestartConflict);
        }
        Ok(FundProductRead {
            portfolio,
            annual,
            identity,
            holding_evidence,
            product,
        })
    }

    fn read_identity(
        &self,
        instrument_id: InstrumentId,
        knowledge_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentContextRead, ResearchProductReadError> {
        let capability = self
            .identity
            .as_ref()
            .ok_or(ResearchProductReadError::IdentityUnavailable)?;
        let request = InstrumentContextRequest::try_new(instrument_id, knowledge_at, knowledge_at)
            .map_err(ResearchProductReadError::Identity)?;
        capability
            .read(request, deadline, cancellation)
            .map_err(ResearchProductReadError::Identity)
    }

    fn verify_identity_restart(
        &self,
        expected: &InstrumentContextRead,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentContextRead, ResearchProductReadError> {
        self.identity
            .as_ref()
            .ok_or(ResearchProductReadError::IdentityUnavailable)?
            .verify_restart(expected, deadline, cancellation)
            .map_err(ResearchProductReadError::Identity)
    }

    fn read_holding_identities(
        &self,
        portfolio: &FundResearchRead,
        knowledge_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<FundHoldingIdentityReads, ResearchProductReadError> {
        let holdings = match portfolio.outcome() {
            FundResearchOutcome::Available(snapshot) => snapshot.holdings().holdings(),
            FundResearchOutcome::Missing
            | FundResearchOutcome::Ambiguous
            | FundResearchOutcome::Unavailable(_) => &[],
        };
        let mut identities = Vec::new();
        let mut evidence = Vec::new();
        identities
            .try_reserve_exact(holdings.len())
            .map_err(|_| ResearchProductReadError::ResourceExhausted)?;
        evidence
            .try_reserve_exact(holdings.len())
            .map_err(|_| ResearchProductReadError::ResourceExhausted)?;
        for holding in holdings {
            let Some(instrument_id) = holding.instrument_id() else {
                identities.push(None);
                evidence.push(None);
                continue;
            };
            let read = self.read_identity(instrument_id, knowledge_at, deadline, cancellation)?;
            identities.push(product_identity_optional(&read)?);
            evidence.push(Some(read));
        }
        Ok((identities, evidence.into_boxed_slice()))
    }

    fn verify_holding_identities(
        &self,
        expected: &[Option<InstrumentContextRead>],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<FundHoldingIdentityReads, ResearchProductReadError> {
        let mut identities = Vec::new();
        let mut evidence = Vec::new();
        identities
            .try_reserve_exact(expected.len())
            .map_err(|_| ResearchProductReadError::ResourceExhausted)?;
        evidence
            .try_reserve_exact(expected.len())
            .map_err(|_| ResearchProductReadError::ResourceExhausted)?;
        for expected_read in expected {
            let Some(expected_read) = expected_read else {
                identities.push(None);
                evidence.push(None);
                continue;
            };
            let read = self.verify_identity_restart(expected_read, deadline, cancellation)?;
            identities.push(product_identity_optional(&read)?);
            evidence.push(Some(read));
        }
        Ok((identities, evidence.into_boxed_slice()))
    }
}

type FundHoldingIdentityReads = (
    Vec<Option<ResearchProductIdentity>>,
    Box<[Option<InstrumentContextRead>]>,
);

fn product_identity(
    read: &InstrumentContextRead,
) -> Result<ResearchProductIdentity, ResearchProductReadError> {
    product_identity_optional(read)?.ok_or(ResearchProductReadError::IdentityUnavailable)
}

fn product_identity_optional(
    read: &InstrumentContextRead,
) -> Result<Option<ResearchProductIdentity>, ResearchProductReadError> {
    match read.outcome() {
        InstrumentContextOutcome::Exact(identity) => {
            ResearchProductIdentity::try_new(identity.display_name(), identity.listed_symbol())
                .map(Some)
                .map_err(ResearchProductReadError::CompanyProjection)
        }
        InstrumentContextOutcome::Missing(_)
        | InstrumentContextOutcome::Ambiguous
        | InstrumentContextOutcome::Unavailable(_) => Ok(None),
    }
}

pub(crate) struct CompanyProductRead {
    canonical: CompanyResearchRead,
    identity: InstrumentContextRead,
    product: CompanyProductResult,
}

impl CompanyProductRead {
    pub(crate) const fn product(&self) -> &CompanyProductResult {
        &self.product
    }
}

impl fmt::Debug for CompanyProductRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanyProductRead")
            .field("product", &self.product)
            .field("canonical", &"[PRIVATE CANONICAL RESTART EVIDENCE]")
            .finish()
    }
}

pub(crate) struct FundProductRead {
    portfolio: FundResearchRead,
    annual: FundResearchRead,
    identity: InstrumentContextRead,
    holding_evidence: Box<[Option<InstrumentContextRead>]>,
    product: FundProductResult,
}

impl FundProductRead {
    pub(crate) const fn product(&self) -> &FundProductResult {
        &self.product
    }
}

impl fmt::Debug for FundProductRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FundProductRead")
            .field("product", &self.product)
            .field("canonical", &"[PRIVATE CANONICAL RESTART EVIDENCE]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ResearchProductReadError {
    #[error("canonical research read failed")]
    Canonical(CanonicalResearchReadError),
    #[error("company product projection failed")]
    CompanyProjection(CompanyProductProjectionError),
    #[error("fund product projection failed")]
    FundProjection(FundProductProjectionError),
    #[error("canonical display identity read failed")]
    Identity(InstrumentContextReadError),
    #[error("canonical display identity is unavailable or ambiguous")]
    IdentityUnavailable,
    #[error("research product identity projection exceeded its fixed resource bound")]
    ResourceExhausted,
    #[error("research product restart did not reproduce the same result")]
    RestartConflict,
}

/// Product company result plus inaccessible selector receipts.
pub(crate) struct CompanyResearchRead {
    request: CompanyResearchRequest,
    outcome: CompanyResearchOutcome,
    evidence: CompanyResearchEvidence,
}

impl CompanyResearchRead {
    pub(crate) const fn request(&self) -> &CompanyResearchRequest {
        &self.request
    }
    pub(crate) const fn outcome(&self) -> &CompanyResearchOutcome {
        &self.outcome
    }
}

impl fmt::Debug for CompanyResearchRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanyResearchRead")
            .field("request", &self.request)
            .field("outcome", &self.outcome)
            .field("evidence", &"[PRIVATE RESTART RECEIPTS]")
            .finish()
    }
}

struct CompanyResearchEvidence {
    selections: Box<[SecResearchIdentitySelection]>,
}

/// Honest company-research availability.
#[derive(Clone, Eq, PartialEq)]
pub(crate) enum CompanyResearchOutcome {
    Available(CompanyResearchSnapshot),
    Partial(CompanyResearchSnapshot),
    Missing,
    Ambiguous,
    Unavailable(CompanyResearchUnavailableReason),
}

impl fmt::Debug for CompanyResearchOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Available(snapshot) => {
                formatter.debug_tuple("Available").field(snapshot).finish()
            }
            Self::Partial(snapshot) => formatter.debug_tuple("Partial").field(snapshot).finish(),
            Self::Missing => formatter.write_str("Missing"),
            Self::Ambiguous => formatter.write_str("Ambiguous"),
            Self::Unavailable(reason) => {
                formatter.debug_tuple("Unavailable").field(reason).finish()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyResearchUnavailableReason {
    StaleIdentity,
    RevokedIdentity,
    ConflictingIdentityState,
    ConflictingRevisionEvidence,
}

/// Facts, filings, freshness, and ratio availability without source/storage plumbing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompanyResearchSnapshot {
    instrument_id: InstrumentId,
    as_of: Timestamp,
    company_facts: CompanyResearchSurfaceAvailability,
    filings: CompanyResearchSurfaceAvailability,
    filing_details: CompanyResearchSurfaceAvailability,
    facts: Box<[CompanyResearchFact]>,
    filing_events: Box<[CompanyResearchFiling]>,
    latest_known_at: Option<Timestamp>,
    ratios: CompanyRatioAvailability,
}

impl CompanyResearchSnapshot {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
    pub(crate) const fn company_facts(&self) -> CompanyResearchSurfaceAvailability {
        self.company_facts
    }
    pub(crate) const fn filings(&self) -> CompanyResearchSurfaceAvailability {
        self.filings
    }
    pub(crate) const fn filing_details(&self) -> CompanyResearchSurfaceAvailability {
        self.filing_details
    }
    pub(crate) fn facts(&self) -> &[CompanyResearchFact] {
        &self.facts
    }
    pub(crate) fn filing_events(&self) -> &[CompanyResearchFiling] {
        &self.filing_events
    }
    pub(crate) const fn latest_known_at(&self) -> Option<Timestamp> {
        self.latest_known_at
    }
    pub(crate) const fn ratios(&self) -> CompanyRatioAvailability {
        self.ratios
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyResearchSurfaceAvailability {
    Available,
    Missing,
}

/// Whether a fact is company-wide or bound to one detailed filing taxonomy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyFactScope {
    CompanyWide,
    FilingDetail,
}

/// Fiscal-period meaning retained without the source's lexical period coordinate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyResearchFiscalPeriod {
    FiscalYear,
    CalendarYear,
    FirstQuarter,
    SecondQuarter,
    ThirdQuarter,
    FourthQuarter,
    Unavailable,
    Unsupported,
}

/// Whether the exact source context reported no dimensions, some dimensions, or no assertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyResearchDimensionState {
    Unavailable,
    NoDimensions,
    Dimensions { count: usize },
}

/// Source-reported restatement meaning without its source-native status identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyResearchRestatementState {
    Unavailable,
    ReportedNotRestated,
    ReportedRestated,
}

/// One exact point-in-time fact stripped of provider and raw-object coordinates.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CompanyResearchFact {
    lineage: CompanyResearchFactLineage,
    scope: CompanyFactScope,
    revision: CompanyResearchRevisionState,
    metric: Box<str>,
    value: Decimal,
    unit: Box<str>,
    period: FundamentalPeriod,
    fiscal_year: Option<u16>,
    fiscal_period: CompanyResearchFiscalPeriod,
    cadence: FundamentalCadence,
    dimension_state: CompanyResearchDimensionState,
    consolidation: FundamentalConsolidation,
    amendment_status: FundamentalAmendmentStatus,
    restatement_state: CompanyResearchRestatementState,
    occurrence: RevisionNumber,
    filed_on: Option<CalendarDate>,
    effective: ResearchTemporalCoordinate,
    known_at: Timestamp,
}

impl fmt::Debug for CompanyResearchFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanyResearchFact")
            .field("scope", &self.scope)
            .field("revision", &self.revision)
            .field("metric", &self.metric)
            .field("value", &self.value)
            .field("unit", &self.unit)
            .field("period", &self.period)
            .field("fiscal_year", &self.fiscal_year)
            .field("fiscal_period", &self.fiscal_period)
            .field("cadence", &self.cadence)
            .field("dimension_state", &self.dimension_state)
            .field("consolidation", &self.consolidation)
            .field("amendment_status", &self.amendment_status)
            .field("restatement_state", &self.restatement_state)
            .field("occurrence", &self.occurrence)
            .field("filed_on", &self.filed_on)
            .field("effective", &self.effective)
            .field("known_at", &self.known_at)
            .finish()
    }
}

impl CompanyResearchFact {
    pub(crate) const fn lineage(&self) -> &CompanyResearchFactLineage {
        &self.lineage
    }
    pub(crate) const fn scope(&self) -> CompanyFactScope {
        self.scope
    }
    pub(crate) const fn revision(&self) -> CompanyResearchRevisionState {
        self.revision
    }
    pub(crate) fn metric(&self) -> &str {
        &self.metric
    }
    pub(crate) const fn value(&self) -> Decimal {
        self.value
    }
    pub(crate) fn unit(&self) -> &str {
        &self.unit
    }
    pub(crate) const fn period(&self) -> FundamentalPeriod {
        self.period
    }
    pub(crate) const fn fiscal_year(&self) -> Option<u16> {
        self.fiscal_year
    }
    pub(crate) const fn fiscal_period(&self) -> CompanyResearchFiscalPeriod {
        self.fiscal_period
    }
    pub(crate) const fn cadence(&self) -> FundamentalCadence {
        self.cadence
    }
    pub(crate) const fn dimension_state(&self) -> CompanyResearchDimensionState {
        self.dimension_state
    }
    pub(crate) const fn consolidation(&self) -> FundamentalConsolidation {
        self.consolidation
    }
    pub(crate) const fn amendment_status(&self) -> FundamentalAmendmentStatus {
        self.amendment_status
    }
    pub(crate) const fn restatement_state(&self) -> CompanyResearchRestatementState {
        self.restatement_state
    }
    pub(crate) const fn occurrence(&self) -> RevisionNumber {
        self.occurrence
    }
    pub(crate) const fn filed_on(&self) -> Option<CalendarDate> {
        self.filed_on
    }
    pub(crate) const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }
    pub(crate) const fn known_at(&self) -> Timestamp {
        self.known_at
    }
}

/// Exact filing and immutable publication identity used only for safe calculation grouping.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CompanyResearchFactLineage {
    filing_identity: Box<str>,
    publication_identity: EvidenceDigest,
}

impl fmt::Debug for CompanyResearchFactLineage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[PRIVATE FACT LINEAGE]")
    }
}

impl CompanyResearchFactLineage {
    pub(crate) fn filing_identity(&self) -> &str {
        &self.filing_identity
    }

    pub(crate) const fn publication_identity(&self) -> EvidenceDigest {
        self.publication_identity
    }
}

/// One filing event without provider identifiers or source-native filing coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompanyResearchFiling {
    revision: CompanyResearchRevisionState,
    form: Box<str>,
    effective: ResearchTemporalCoordinate,
    published: Option<ResearchTemporalCoordinate>,
    known_at: Timestamp,
}

impl CompanyResearchFiling {
    pub(crate) const fn revision(&self) -> CompanyResearchRevisionState {
        self.revision
    }
    pub(crate) fn form(&self) -> &str {
        &self.form
    }
    pub(crate) const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }
    pub(crate) const fn published(&self) -> Option<&ResearchTemporalCoordinate> {
        self.published.as_ref()
    }
    pub(crate) const fn known_at(&self) -> Timestamp {
        self.known_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyResearchRevisionState {
    Current,
    Superseded,
    IncomparableHistory,
}

/// Ratios stay unavailable until a typed calculation/publication receipt exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompanyRatioAvailability {
    UnavailableNoCanonicalCalculation,
}

/// Product fund result plus inaccessible exact publication/selection evidence.
pub(crate) struct FundResearchRead {
    request: FundResearchRequest,
    outcome: FundResearchOutcome,
    evidence: SecFundPointInTimeReadOutcome,
}

impl FundResearchRead {
    pub(crate) const fn request(&self) -> FundResearchRequest {
        self.request
    }
    pub(crate) const fn outcome(&self) -> &FundResearchOutcome {
        &self.outcome
    }
}

impl fmt::Debug for FundResearchRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FundResearchRead")
            .field("request", &self.request)
            .field("outcome", &self.outcome)
            .field("evidence", &"[PRIVATE RESTART RECEIPT]")
            .finish()
    }
}

/// Honest fund-research availability without provider or filing-coordinate leakage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FundResearchOutcome {
    Available(FundResearchSnapshot),
    Missing,
    Ambiguous,
    Unavailable(FundResearchUnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FundResearchUnavailableReason {
    IncompleteReportCoverage,
    RevisionConflict,
    RevisionUnavailable,
    UnresolvedRevisionLink,
    BrokenRevisionChain,
    NoCurrentRevision,
    MultipleCurrentRevisions,
    MultipleReportVersions,
}

/// Canonical holdings plus explicit exposure-coverage meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FundResearchSnapshot {
    holdings: FundResearchData,
    exposure: FundExposureCoverage,
}

impl FundResearchSnapshot {
    pub(crate) const fn holdings(&self) -> &FundResearchData {
        &self.holdings
    }
    pub(crate) const fn exposure(&self) -> FundExposureCoverage {
        self.exposure
    }
}

/// Coverage of instrument mapping and reported portfolio weights without invented totals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FundExposureCoverage {
    holdings: usize,
    identified_holdings: usize,
    reported_weights: usize,
    missing_weights: usize,
    conflicting_weights: usize,
}

impl FundExposureCoverage {
    pub(crate) const fn holdings(self) -> usize {
        self.holdings
    }
    pub(crate) const fn identified_holdings(self) -> usize {
        self.identified_holdings
    }
    pub(crate) const fn reported_weights(self) -> usize {
        self.reported_weights
    }
    pub(crate) const fn missing_weights(self) -> usize {
        self.missing_weights
    }
    pub(crate) const fn conflicting_weights(self) -> usize {
        self.conflicting_weights
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CanonicalResearchReadError {
    #[error("research read request is invalid")]
    InvalidRequest,
    #[error("research read was cancelled")]
    Cancelled,
    #[error("research read deadline elapsed")]
    DeadlineExceeded,
    #[error("research read authority is unavailable")]
    AuthorityUnavailable,
    #[error("research evidence is inconsistent")]
    EvidenceConflict,
    #[error("research read exceeded its fixed resource bound")]
    ResourceExhausted,
    #[error("research restart did not reproduce the same result")]
    RestartConflict,
}

fn project_company_research(
    request: &CompanyResearchRequest,
    selections: &[SecResearchIdentitySelection],
) -> Result<CompanyResearchOutcome, CanonicalResearchReadError> {
    if selections.len() != COMPANY_RESEARCH_FAMILY_COUNT {
        return Err(CanonicalResearchReadError::EvidenceConflict);
    }
    let mut facts = Vec::new();
    let mut filings = Vec::new();
    let mut available_families = 0_usize;
    let mut missing_families = 0_usize;
    let mut stale = false;
    let mut revoked = false;
    let mut identity_ambiguous = false;
    let mut conflicting_identity_state = false;
    let mut conflicting_revision_evidence = false;
    let mut company_identity: Option<SourceIdentifier> = None;
    let mut latest_known_at = None;
    let mut company_facts = CompanyResearchSurfaceAvailability::Missing;
    let mut filing_events = CompanyResearchSurfaceAvailability::Missing;
    let mut filing_details = CompanyResearchSurfaceAvailability::Missing;

    for selected in selections {
        if selected.request().instrument_id() != request.instrument_id
            || selected.request().knowledge_at() != request.knowledge_at
            || selected.request().effective_cutoff() != &request.fact_effective_cutoff
            || selected.request().revision_mode() != request.revision_policy.data_policy()
        {
            return Err(CanonicalResearchReadError::EvidenceConflict);
        }
        match selected.outcome() {
            SecResearchIdentityOutcome::Missing => {
                missing_families = checked_increment(missing_families)?;
            }
            SecResearchIdentityOutcome::Ambiguous => identity_ambiguous = true,
            SecResearchIdentityOutcome::Stale => stale = true,
            SecResearchIdentityOutcome::Revoked => revoked = true,
            SecResearchIdentityOutcome::Exact(selection) => {
                let [relationship] = selected.identity().candidates() else {
                    return Err(CanonicalResearchReadError::EvidenceConflict);
                };
                if relationship.link().instrument_id() != request.instrument_id {
                    return Err(CanonicalResearchReadError::EvidenceConflict);
                }
                let candidate_company = relationship.link().provider_company_id();
                if company_identity
                    .as_ref()
                    .is_some_and(|expected| expected != candidate_company)
                {
                    conflicting_identity_state = true;
                } else if company_identity.is_none() {
                    company_identity = Some(candidate_company.clone());
                }
                match selection.disposition() {
                    SecResearchDisposition::Conflict => conflicting_revision_evidence = true,
                    SecResearchDisposition::Unavailable => {
                        missing_families = checked_increment(missing_families)?;
                    }
                    SecResearchDisposition::Selected => {
                        available_families = checked_increment(available_families)?;
                        *surface_availability_mut(
                            selected.request().family(),
                            &mut company_facts,
                            &mut filing_events,
                            &mut filing_details,
                        ) = CompanyResearchSurfaceAvailability::Available;
                        append_company_rows(
                            request,
                            selected.request().family(),
                            selection,
                            &mut facts,
                            &mut filings,
                            &mut latest_known_at,
                        )?;
                    }
                }
            }
        }
    }

    if identity_ambiguous {
        return Ok(CompanyResearchOutcome::Ambiguous);
    }
    if conflicting_revision_evidence {
        return Ok(CompanyResearchOutcome::Unavailable(
            CompanyResearchUnavailableReason::ConflictingRevisionEvidence,
        ));
    }
    if conflicting_identity_state || (stale && revoked) {
        return Ok(CompanyResearchOutcome::Unavailable(
            CompanyResearchUnavailableReason::ConflictingIdentityState,
        ));
    }
    if stale {
        return Ok(CompanyResearchOutcome::Unavailable(
            CompanyResearchUnavailableReason::StaleIdentity,
        ));
    }
    if revoked {
        return Ok(CompanyResearchOutcome::Unavailable(
            CompanyResearchUnavailableReason::RevokedIdentity,
        ));
    }
    if available_families == 0 {
        if missing_families == COMPANY_RESEARCH_FAMILY_COUNT {
            return Ok(CompanyResearchOutcome::Missing);
        }
        return Err(CanonicalResearchReadError::EvidenceConflict);
    }
    let snapshot = CompanyResearchSnapshot {
        instrument_id: request.instrument_id,
        as_of: request.knowledge_at,
        company_facts,
        filings: filing_events,
        filing_details,
        facts: facts.into_boxed_slice(),
        filing_events: filings.into_boxed_slice(),
        latest_known_at,
        ratios: CompanyRatioAvailability::UnavailableNoCanonicalCalculation,
    };
    if available_families == COMPANY_RESEARCH_FAMILY_COUNT {
        Ok(CompanyResearchOutcome::Available(snapshot))
    } else {
        Ok(CompanyResearchOutcome::Partial(snapshot))
    }
}

fn surface_availability_mut<'availability>(
    family: SecResearchFamily,
    company_facts: &'availability mut CompanyResearchSurfaceAvailability,
    filing_events: &'availability mut CompanyResearchSurfaceAvailability,
    filing_details: &'availability mut CompanyResearchSurfaceAvailability,
) -> &'availability mut CompanyResearchSurfaceAvailability {
    match family {
        SecResearchFamily::CompanyFacts => company_facts,
        SecResearchFamily::Submissions => filing_events,
        SecResearchFamily::FilingXbrl => filing_details,
    }
}

fn append_company_rows(
    request: &CompanyResearchRequest,
    family: SecResearchFamily,
    selection: &market_squawk_data::SecResearchSelection,
    facts: &mut Vec<CompanyResearchFact>,
    filings: &mut Vec<CompanyResearchFiling>,
    latest_known_at: &mut Option<Timestamp>,
) -> Result<(), CanonicalResearchReadError> {
    if selection.selected().is_empty() {
        return Err(CanonicalResearchReadError::EvidenceConflict);
    }
    for selected in selection.selected() {
        let ordinal = usize::try_from(selected.row().row_ordinal())
            .map_err(|_| CanonicalResearchReadError::EvidenceConflict)?;
        let observation = selection
            .decoded_rows()
            .get(ordinal)
            .ok_or(CanonicalResearchReadError::EvidenceConflict)?;
        let context =
            observation_context(observation).ok_or(CanonicalResearchReadError::EvidenceConflict)?;
        if context.provenance().instrument_id() != Some(request.instrument_id) {
            return Err(CanonicalResearchReadError::EvidenceConflict);
        }
        let known_at = context
            .provenance()
            .availability()
            .conservative_available_at()
            .ok_or(CanonicalResearchReadError::EvidenceConflict)?;
        if known_at > request.knowledge_at {
            return Err(CanonicalResearchReadError::EvidenceConflict);
        }
        *latest_known_at = Some(latest_known_at.map_or(known_at, |current| current.max(known_at)));
        let retained_rows = facts
            .len()
            .checked_add(filings.len())
            .ok_or(CanonicalResearchReadError::ResourceExhausted)?;
        if retained_rows >= MAX_COMPANY_RESEARCH_RESULT_ROWS {
            return Err(CanonicalResearchReadError::ResourceExhausted);
        }
        match (family, observation) {
            (SecResearchFamily::CompanyFacts, ResearchObservation::Fundamental(fundamental))
            | (SecResearchFamily::FilingXbrl, ResearchObservation::Fundamental(fundamental)) => {
                let fact_context = fundamental.fact_context();
                facts
                    .try_reserve(1)
                    .map_err(|_| CanonicalResearchReadError::ResourceExhausted)?;
                facts.push(CompanyResearchFact {
                    lineage: CompanyResearchFactLineage {
                        filing_identity: try_boxed_text(fact_context.accession().as_str())?,
                        publication_identity: selection.origin().origin_digest(),
                    },
                    scope: match family {
                        SecResearchFamily::CompanyFacts => CompanyFactScope::CompanyWide,
                        SecResearchFamily::FilingXbrl => CompanyFactScope::FilingDetail,
                        SecResearchFamily::Submissions => {
                            return Err(CanonicalResearchReadError::EvidenceConflict);
                        }
                    },
                    revision: product_revision_state(selected.point_in_time().revision_state()),
                    metric: try_boxed_text(fundamental.concept().as_str())?,
                    value: fundamental.value(),
                    unit: try_boxed_text(fundamental.unit().as_str())?,
                    period: fact_context.period(),
                    fiscal_year: fact_context.fiscal_year(),
                    fiscal_period: company_fiscal_period(fact_context.fiscal_period()),
                    cadence: fact_context.cadence(),
                    dimension_state: company_dimension_state(
                        fact_context.dimensions().dimensions(),
                    ),
                    consolidation: fact_context.consolidation(),
                    amendment_status: fact_context.amendment_status(),
                    restatement_state: company_restatement_state(fact_context.restatement_status()),
                    occurrence: fact_context.revision_order().ordinal(),
                    filed_on: fact_context.filed_on(),
                    effective: fundamental.context().time().effective().clone(),
                    known_at,
                });
            }
            (SecResearchFamily::Submissions, ResearchObservation::Filing(filing)) => {
                filings
                    .try_reserve(1)
                    .map_err(|_| CanonicalResearchReadError::ResourceExhausted)?;
                filings.push(CompanyResearchFiling {
                    revision: product_revision_state(selected.point_in_time().revision_state()),
                    form: try_boxed_text(filing.form_type().as_str())?,
                    effective: filing.context().time().effective().clone(),
                    published: filing.context().time().published().cloned(),
                    known_at,
                });
            }
            _ => return Err(CanonicalResearchReadError::EvidenceConflict),
        }
    }
    Ok(())
}

fn company_fiscal_period(fiscal_period: Option<&SourceIdentifier>) -> CompanyResearchFiscalPeriod {
    match fiscal_period.map(SourceIdentifier::as_str) {
        Some("FY") => CompanyResearchFiscalPeriod::FiscalYear,
        Some("CY") => CompanyResearchFiscalPeriod::CalendarYear,
        Some("Q1") => CompanyResearchFiscalPeriod::FirstQuarter,
        Some("Q2") => CompanyResearchFiscalPeriod::SecondQuarter,
        Some("Q3") => CompanyResearchFiscalPeriod::ThirdQuarter,
        Some("Q4") => CompanyResearchFiscalPeriod::FourthQuarter,
        None => CompanyResearchFiscalPeriod::Unavailable,
        Some(_) => CompanyResearchFiscalPeriod::Unsupported,
    }
}

fn company_dimension_state(
    dimensions: Option<&[market_squawk_domain::XbrlDimensionEvidence]>,
) -> CompanyResearchDimensionState {
    match dimensions {
        None => CompanyResearchDimensionState::Unavailable,
        Some([]) => CompanyResearchDimensionState::NoDimensions,
        Some(dimensions) => CompanyResearchDimensionState::Dimensions {
            count: dimensions.len(),
        },
    }
}

fn company_restatement_state(
    state: &FundamentalRestatementStatus,
) -> CompanyResearchRestatementState {
    match state {
        FundamentalRestatementStatus::Unavailable => CompanyResearchRestatementState::Unavailable,
        FundamentalRestatementStatus::SourceReported {
            restated: false, ..
        } => CompanyResearchRestatementState::ReportedNotRestated,
        FundamentalRestatementStatus::SourceReported { restated: true, .. } => {
            CompanyResearchRestatementState::ReportedRestated
        }
    }
}

fn fund_data_request(
    request: FundResearchRequest,
) -> Result<SecFundPointInTimeReadRequest, CanonicalResearchReadError> {
    match request.revision_policy {
        FundResearchRevisionPolicy::LatestKnown => SecFundPointInTimeReadRequest::try_latest_known(
            request.fund_instrument_id,
            request.family.data_family(),
            request.knowledge_at,
            MAX_FUND_RESEARCH_RECORDS,
        ),
        FundResearchRevisionPolicy::AllKnown => SecFundPointInTimeReadRequest::try_all_known(
            request.fund_instrument_id,
            request.family.data_family(),
            request.knowledge_at,
            MAX_FUND_RESEARCH_RECORDS,
        ),
    }
    .map_err(|_| CanonicalResearchReadError::InvalidRequest)
}

fn project_fund_research(
    request: FundResearchRequest,
    evidence: &SecFundPointInTimeReadOutcome,
) -> Result<FundResearchOutcome, CanonicalResearchReadError> {
    match evidence {
        SecFundPointInTimeReadOutcome::Missing => Ok(FundResearchOutcome::Missing),
        SecFundPointInTimeReadOutcome::Ambiguous { .. } => Ok(FundResearchOutcome::Ambiguous),
        SecFundPointInTimeReadOutcome::Conflict { .. } => Ok(FundResearchOutcome::Unavailable(
            FundResearchUnavailableReason::RevisionConflict,
        )),
        SecFundPointInTimeReadOutcome::RevisionSet { .. } => Ok(FundResearchOutcome::Unavailable(
            FundResearchUnavailableReason::MultipleReportVersions,
        )),
        SecFundPointInTimeReadOutcome::Exact {
            publication,
            selection,
        } => {
            if publication.fund_instrument_id() != request.fund_instrument_id
                || !fund_family_matches(publication.family(), request.family)
                || publication.committed_at() > request.knowledge_at
            {
                return Err(CanonicalResearchReadError::EvidenceConflict);
            }
            if let FundPointInTimeOutcome::LatestUnavailable { reason, .. } = selection.outcome() {
                if *reason == FundLatestUnavailableReason::NoKnownRecords {
                    return Ok(FundResearchOutcome::Missing);
                }
                return Ok(FundResearchOutcome::Unavailable(
                    map_latest_fund_unavailable(*reason),
                ));
            }
            if !matches!(
                (request.revision_policy, selection.outcome()),
                (
                    FundResearchRevisionPolicy::LatestKnown,
                    FundPointInTimeOutcome::LatestKnown { .. }
                ) | (
                    FundResearchRevisionPolicy::AllKnown,
                    FundPointInTimeOutcome::AllKnown { .. }
                )
            ) {
                return Err(CanonicalResearchReadError::EvidenceConflict);
            }
            if let FundPointInTimeOutcome::AllKnown { records } = selection.outcome() {
                let mut accession = None;
                for record in records.as_ref() {
                    let candidate = fund_record_accession(record);
                    if accession.is_some_and(|expected| expected != candidate) {
                        return Ok(FundResearchOutcome::Unavailable(
                            FundResearchUnavailableReason::MultipleReportVersions,
                        ));
                    }
                    accession = Some(candidate);
                }
            }
            let holdings = FundResearchData::try_from_point_in_time(
                selection,
                request.fund_instrument_id,
                request.knowledge_at,
            )
            .map_err(map_fund_projection_error)?;
            let exposure = fund_exposure_coverage(&holdings)?;
            Ok(FundResearchOutcome::Available(FundResearchSnapshot {
                holdings,
                exposure,
            }))
        }
    }
}

fn product_revision_state(state: PointInTimeRevisionState) -> CompanyResearchRevisionState {
    match state {
        PointInTimeRevisionState::Current => CompanyResearchRevisionState::Current,
        PointInTimeRevisionState::Superseded => CompanyResearchRevisionState::Superseded,
        PointInTimeRevisionState::SupersessionIncomparable => {
            CompanyResearchRevisionState::IncomparableHistory
        }
    }
}

fn fund_record_accession(record: &market_squawk_domain::FundEvidenceRecord) -> &SourceIdentifier {
    match record {
        market_squawk_domain::FundEvidenceRecord::Report(value) => value.filing().accession(),
        market_squawk_domain::FundEvidenceRecord::ShareClass(value) => value.filing().accession(),
        market_squawk_domain::FundEvidenceRecord::PortfolioHolding(value) => {
            value.filing().accession()
        }
    }
}

fn fund_exposure_coverage(
    data: &FundResearchData,
) -> Result<FundExposureCoverage, CanonicalResearchReadError> {
    let mut identified_holdings = 0_usize;
    let mut reported_weights = 0_usize;
    let mut missing_weights = 0_usize;
    let mut conflicting_weights = 0_usize;
    for holding in data.holdings() {
        if holding.instrument_id().is_some() {
            identified_holdings = checked_increment(identified_holdings)?;
        }
        let weight = holding.percentage_of_net_assets();
        if weight.reported().is_some() {
            reported_weights = checked_increment(reported_weights)?;
        } else if weight.missing().is_some() {
            missing_weights = checked_increment(missing_weights)?;
        } else if weight.conflict().is_some() {
            conflicting_weights = checked_increment(conflicting_weights)?;
        } else {
            return Err(CanonicalResearchReadError::EvidenceConflict);
        }
    }
    Ok(FundExposureCoverage {
        holdings: data.holdings().len(),
        identified_holdings,
        reported_weights,
        missing_weights,
        conflicting_weights,
    })
}

fn fund_family_matches(data: SecFundJobFamily, product: FundResearchFamily) -> bool {
    matches!(
        (data, product),
        (
            SecFundJobFamily::Nport,
            FundResearchFamily::PortfolioHoldings
        ) | (SecFundJobFamily::Ncen, FundResearchFamily::AnnualFundReport)
    )
}

fn map_latest_fund_unavailable(
    reason: FundLatestUnavailableReason,
) -> FundResearchUnavailableReason {
    match reason {
        FundLatestUnavailableReason::NoKnownRecords => {
            FundResearchUnavailableReason::IncompleteReportCoverage
        }
        FundLatestUnavailableReason::IncompleteReleaseCoverage => {
            FundResearchUnavailableReason::IncompleteReportCoverage
        }
        FundLatestUnavailableReason::RevisionConflict => {
            FundResearchUnavailableReason::RevisionConflict
        }
        FundLatestUnavailableReason::RevisionUnavailable => {
            FundResearchUnavailableReason::RevisionUnavailable
        }
        FundLatestUnavailableReason::UnresolvedRevisionLink => {
            FundResearchUnavailableReason::UnresolvedRevisionLink
        }
        FundLatestUnavailableReason::BrokenRevisionChain => {
            FundResearchUnavailableReason::BrokenRevisionChain
        }
        FundLatestUnavailableReason::NoCurrentRevision => {
            FundResearchUnavailableReason::NoCurrentRevision
        }
        FundLatestUnavailableReason::MultipleCurrentRevisions => {
            FundResearchUnavailableReason::MultipleCurrentRevisions
        }
    }
}

fn company_point_in_time_limits() -> Result<PointInTimeLimits, CanonicalResearchReadError> {
    PointInTimeLimits::try_new(
        MAX_COMPANY_RESEARCH_CANDIDATES,
        MAX_COMPANY_RESEARCH_FAMILIES,
        MAX_COMPANY_RESEARCH_CONFLICTS,
        MAX_COMPANY_RESEARCH_RESULT_ROWS,
        MAX_COMPANY_RESEARCH_RETAINED_BYTES,
    )
    .map_err(|_| CanonicalResearchReadError::ResourceExhausted)
}

fn observation_context(observation: &ResearchObservation) -> Option<&ResearchContext> {
    match observation {
        ResearchObservation::Filing(value) => Some(value.context()),
        ResearchObservation::Fundamental(value) => Some(value.context()),
        _ => None,
    }
}

fn checked_increment(value: usize) -> Result<usize, CanonicalResearchReadError> {
    value
        .checked_add(1)
        .ok_or(CanonicalResearchReadError::ResourceExhausted)
}

fn try_boxed_text(value: &str) -> Result<Box<str>, CanonicalResearchReadError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| CanonicalResearchReadError::ResourceExhausted)?;
    owned.push_str(value);
    Ok(owned.into_boxed_str())
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CanonicalResearchReadError> {
    if cancellation.is_cancelled() {
        Err(CanonicalResearchReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(CanonicalResearchReadError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_company_data_error(error: SecResearchReadError) -> CanonicalResearchReadError {
    match error {
        SecResearchReadError::InvalidRequest => CanonicalResearchReadError::InvalidRequest,
        SecResearchReadError::Cancelled => CanonicalResearchReadError::Cancelled,
        SecResearchReadError::DeadlineExceeded => CanonicalResearchReadError::DeadlineExceeded,
        SecResearchReadError::AuthorityUnavailable => {
            CanonicalResearchReadError::AuthorityUnavailable
        }
        SecResearchReadError::ObjectBudgetExceeded => CanonicalResearchReadError::ResourceExhausted,
        SecResearchReadError::RestartMismatch => CanonicalResearchReadError::RestartConflict,
        _ => CanonicalResearchReadError::EvidenceConflict,
    }
}

fn map_fund_data_error(error: IngestError) -> CanonicalResearchReadError {
    match error {
        IngestError::Cancelled => CanonicalResearchReadError::Cancelled,
        IngestError::DeadlineExceeded => CanonicalResearchReadError::DeadlineExceeded,
        IngestError::AuthorityLockPoisoned => CanonicalResearchReadError::AuthorityUnavailable,
        IngestError::Arrow(
            ArrowConversionError::RetainedLimitExceeded { .. }
            | ArrowConversionError::RecordLimitExceeded { .. },
        ) => CanonicalResearchReadError::ResourceExhausted,
        IngestError::ReplayConflict => CanonicalResearchReadError::RestartConflict,
        _ => CanonicalResearchReadError::EvidenceConflict,
    }
}

fn map_fund_projection_error(error: SecFundProductBoundaryError) -> CanonicalResearchReadError {
    match error {
        SecFundProductBoundaryError::InvalidRequest => CanonicalResearchReadError::InvalidRequest,
        SecFundProductBoundaryError::InvalidConfiguration => {
            CanonicalResearchReadError::AuthorityUnavailable
        }
        SecFundProductBoundaryError::DeadlineUnavailable => {
            CanonicalResearchReadError::DeadlineExceeded
        }
        SecFundProductBoundaryError::PublicationMismatch => {
            CanonicalResearchReadError::EvidenceConflict
        }
        SecFundProductBoundaryError::ResourceExhausted => {
            CanonicalResearchReadError::ResourceExhausted
        }
    }
}
