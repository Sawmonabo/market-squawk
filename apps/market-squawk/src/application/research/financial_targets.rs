//! Authenticated SEC monetary inputs with their original fiscal periods and claim meanings.
//!
//! This is the source producer for native fiscal dataset admission. It does not publish a dataset,
//! forecast, valuation or accounting approval. The complete selected source remains available to
//! the existing dataset authority; a normalized scalar alone is never its admission evidence.

use std::{
    io,
    mem::size_of,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_data::{
    AuthorizedResearchUse, CompanySecurityIdentityDisposition, DatasetBuildError,
    FinancialAmountSelection, FinancialDatasetSeries, FinancialSeriesLimits, PointInTimeLimits,
    PointInTimeRevisionMode, ResearchUse, ResearchUseCatalogError, ResearchUseDecisionDigest,
    ResearchUseGraphDigest, ResearchUseLimits, ResearchUseRequest, SecResearchDisposition,
    SecResearchFamily, SecResearchIdentityOutcome, SecResearchIdentityReadRequest,
    SecResearchIdentitySelection, SecResearchReadError, SecResearchSelection,
};
use market_squawk_domain::{
    CalendarDate, Currency, DataQuality, DigestAlgorithm, EvidenceDigest, FundamentalCadence,
    FundamentalObservation, FundamentalPeriod, InstrumentId, Money, ResearchObservation,
    ResearchTemporalCoordinate, Timestamp,
};
use market_squawk_services::{RequestContext, ServiceError};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::company_product::{CompanyFinancialMetric, product_metric};
use crate::ResearchService;

const MAXIMUM_SOURCE_ROWS: usize = 65_536;
const MAXIMUM_SOURCE_BYTES: usize = 32 * 1024 * 1024;
const MAXIMUM_FACTS: usize = 4_096;
const MAXIMUM_FRAMES: usize = 1_024;
const MAXIMUM_CANONICAL_BYTES: usize = 16 * 1024 * 1024;
const POLICY: &[u8] = b"market-squawk/sec-native-fiscal-total-inputs/v1\0";

/// A reporting-entity total is not automatically attributable to common stockholders.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinancialTargetAmountBasis {
    ReportingEntityTotal,
    TotalCommonEquity,
}

/// Exact reported monetary meanings; ingredients retain their narrower scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinancialTargetRole {
    CommonIncome,
    ParentIncome,
    ParentEquity,
    PreferredIncomeAdjustments,
    OperatingCashFlow,
    PropertyPlantAndEquipmentPurchases,
    LongTermBorrowingProceeds,
    LongTermDebtRepayments,
    PreferredDividendsPaid,
    /// Issued stock value is not the complete preferred claim or common book value.
    PreferredStockIssuedValue,
}

impl FinancialTargetRole {
    fn from_metric(metric: CompanyFinancialMetric) -> Option<Self> {
        Some(match metric {
            CompanyFinancialMetric::CommonNetIncome => Self::CommonIncome,
            CompanyFinancialMetric::NetIncome => Self::ParentIncome,
            CompanyFinancialMetric::ShareholdersEquity => Self::ParentEquity,
            CompanyFinancialMetric::PreferredDividendsAndAdjustments => {
                Self::PreferredIncomeAdjustments
            }
            CompanyFinancialMetric::OperatingCashFlow => Self::OperatingCashFlow,
            CompanyFinancialMetric::PropertyPlantAndEquipmentPurchases => {
                Self::PropertyPlantAndEquipmentPurchases
            }
            CompanyFinancialMetric::LongTermBorrowingProceeds => Self::LongTermBorrowingProceeds,
            CompanyFinancialMetric::LongTermDebtRepayments => Self::LongTermDebtRepayments,
            CompanyFinancialMetric::PreferredDividendsPaid => Self::PreferredDividendsPaid,
            CompanyFinancialMetric::PreferredStockIssuedValue => Self::PreferredStockIssuedValue,
            _ => return None,
        })
    }

    const fn basis(self) -> FinancialTargetAmountBasis {
        match self {
            Self::CommonIncome => FinancialTargetAmountBasis::TotalCommonEquity,
            _ => FinancialTargetAmountBasis::ReportingEntityTotal,
        }
    }

    const fn period_matches(self, period: FundamentalPeriod) -> bool {
        matches!(
            (self, period),
            (
                Self::ParentEquity | Self::PreferredStockIssuedValue,
                FundamentalPeriod::Instant { .. }
            ) | (
                Self::CommonIncome
                    | Self::ParentIncome
                    | Self::PreferredIncomeAdjustments
                    | Self::OperatingCashFlow
                    | Self::PropertyPlantAndEquipmentPurchases
                    | Self::LongTermBorrowingProceeds
                    | Self::LongTermDebtRepayments
                    | Self::PreferredDividendsPaid,
                FundamentalPeriod::Duration { .. }
            )
        )
    }
}

/// One monetary fact bound to an actually selected canonical row, never a caller-authored amount.
#[derive(Debug)]
pub(crate) struct FinancialTargetFact {
    row_ordinal: u32,
    role: FinancialTargetRole,
    amount: Money,
    period: FundamentalPeriod,
    envelope_identity: EvidenceDigest,
    evidence_identity: EvidenceDigest,
}

impl FinancialTargetFact {
    pub(crate) const fn row_ordinal(&self) -> u32 {
        self.row_ordinal
    }
    pub(crate) const fn role(&self) -> FinancialTargetRole {
        self.role
    }
    pub(crate) const fn amount(&self) -> Money {
        self.amount
    }
    pub(crate) const fn basis(&self) -> FinancialTargetAmountBasis {
        self.role.basis()
    }
    pub(crate) const fn period(&self) -> FundamentalPeriod {
        self.period
    }
    pub(crate) const fn envelope_identity(&self) -> EvidenceDigest {
        self.envelope_identity
    }
    pub(crate) const fn evidence_identity(&self) -> EvidenceDigest {
        self.evidence_identity
    }
}

/// Same filing, native period, currency and reporting context; no cross-filing arithmetic.
#[derive(Debug)]
pub(crate) struct FinancialTargetFrame {
    identity: EvidenceDigest,
    period: FundamentalPeriod,
    currency: Currency,
    fact_indices: Vec<usize>,
}

impl FinancialTargetFrame {
    pub(crate) const fn identity(&self) -> EvidenceDigest {
        self.identity
    }
    pub(crate) const fn period(&self) -> FundamentalPeriod {
        self.period
    }
    pub(crate) const fn currency(&self) -> Currency {
        self.currency
    }
    pub(crate) fn fact_indices(&self) -> &[usize] {
        &self.fact_indices
    }
}

/// Complete immutable input and its bounded semantic projection, without renewed-use authority.
#[derive(Debug)]
pub(crate) struct FinancialTargetHistory {
    source: SecResearchIdentitySelection,
    facts: Vec<FinancialTargetFact>,
    frames: Vec<FinancialTargetFrame>,
    identity: EvidenceDigest,
    normalized_at: Timestamp,
    authorization_expires_at: Timestamp,
    authorization_decision: ResearchUseDecisionDigest,
    authorization_graph: ResearchUseGraphDigest,
    projection_retained_bytes: usize,
}

impl FinancialTargetHistory {
    /// The same-publisher fiscal admission must consume this genuine selection and exact rows.
    pub(crate) const fn source_selection(&self) -> &SecResearchIdentitySelection {
        &self.source
    }
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        self.source.request().instrument_id()
    }
    pub(crate) fn source_selection_as_of(&self) -> Timestamp {
        self.source.request().knowledge_at()
    }
    pub(crate) fn facts(&self) -> &[FinancialTargetFact] {
        &self.facts
    }
    pub(crate) fn frames(&self) -> &[FinancialTargetFrame] {
        &self.frames
    }
    pub(crate) const fn identity(&self) -> EvidenceDigest {
        self.identity
    }
    pub(crate) const fn normalized_at(&self) -> Timestamp {
        self.normalized_at
    }
    pub(crate) const fn authorization_expires_at(&self) -> Timestamp {
        self.authorization_expires_at
    }
    pub(crate) const fn authorization_decision(&self) -> ResearchUseDecisionDigest {
        self.authorization_decision
    }
    pub(crate) const fn authorization_graph(&self) -> ResearchUseGraphDigest {
        self.authorization_graph
    }
    /// Additional retained slots beyond the original data-authority selection.
    /// The source reader independently enforces its 32-MiB work/decoded budget and bounded
    /// company/security selector; this is not a claimed heap measurement of those authorities.
    pub(crate) const fn projection_retained_bytes(&self) -> usize {
        self.projection_retained_bytes
    }

    /// Reopens the original observation within this retained selection; no projected DTO is proof.
    pub(crate) fn observation(&self, index: usize) -> Option<&FundamentalObservation> {
        let fact = self.facts.get(index)?;
        let SecResearchIdentityOutcome::Exact(source) = self.source.outcome() else {
            return None;
        };
        match source
            .decoded_rows()
            .get(usize::try_from(fact.row_ordinal).ok()?)?
        {
            ResearchObservation::Fundamental(observation) => Some(observation),
            _ => None,
        }
    }

    /// Conflicting same-envelope occurrences remain conflicts, even if their numbers agree.
    pub(crate) fn unique_fact(
        &self,
        frame_index: usize,
        role: FinancialTargetRole,
    ) -> Result<Option<&FinancialTargetFact>, ServiceError> {
        let frame = self
            .frames
            .get(frame_index)
            .ok_or(ServiceError::InvalidRequest)?;
        let mut matches = frame
            .fact_indices
            .iter()
            .filter_map(|index| self.facts.get(*index))
            .filter(|fact| fact.role == role);
        let first = matches.next();
        if matches.next().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        Ok(first)
    }
}

/// Reads one exact company-wide SEC generation and normalizes its actual fiscal monetary rows.
///
/// Native period durations, negative income, revisions and missing claims are retained. This does
/// not annualize quarters, divide by shares, treat preferred issued value as all preferred claims,
/// equate long-term borrowing with all borrowing, or label CFO minus capital purchases as FCFE.
pub(crate) async fn read_financial_target_history(
    research: &ResearchService,
    instrument_id: InstrumentId,
    source_selection_as_of: Timestamp,
    effective_date: CalendarDate,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<FinancialTargetHistory, ServiceError> {
    let (source, authorization) = read_authorized_financial_source(
        research,
        instrument_id,
        source_selection_as_of,
        effective_date,
        deadline,
        cancellation,
    )
    .await?;
    let SecResearchIdentityOutcome::Exact(selected) = source.outcome() else {
        return Err(ServiceError::InvalidResult);
    };
    let authorization_expires_at = authorization.expires_at();
    let authorization_decision = authorization.decision_digest();
    let authorization_graph = authorization.graph().digest();
    let (facts, frames, source_identity, projection_retained_bytes) = normalize_selection(
        selected,
        instrument_id,
        source_selection_as_of,
        effective_date,
        deadline,
        cancellation,
    )?;
    check_control(deadline, cancellation)?;
    let normalized_at = current_timestamp()?;
    if normalized_at >= authorization_expires_at {
        return Err(ServiceError::Unavailable);
    }
    let mut identity = Sha256::new();
    identity.update(POLICY);
    identity.update(source_identity.bytes());
    identity.update(source.identity().receipt().receipt_digest().bytes());
    identity.update(authorization_decision.bytes());
    identity.update(authorization_graph.bytes());
    identity.update(normalized_at.unix_nanos().to_be_bytes());
    identity.update(authorization_expires_at.unix_nanos().to_be_bytes());
    let identity = finish_digest(identity)?;
    let _consumed_normalization_permit = authorization.into_permit();
    Ok(FinancialTargetHistory {
        source,
        facts,
        frames,
        identity,
        normalized_at,
        authorization_expires_at,
        authorization_decision,
        authorization_graph,
        projection_retained_bytes,
    })
}

/// Admits the exact selected SEC rows into the existing native fiscal dataset builder.
///
/// Selection intent never supplies an amount or period. The data owner proves the requested
/// role, monetary basis, share convention and cadence from the retained source. The resulting
/// series remains an input to the existing publisher, which obtains fresh Train/LocalAnalysis
/// authority for its actual generation; this function creates no alternate publication.
pub(crate) async fn read_native_financial_series(
    research: &ResearchService,
    instrument_id: InstrumentId,
    source_selection_as_of: Timestamp,
    effective_date: CalendarDate,
    selection: FinancialAmountSelection,
    cadence: FundamentalCadence,
    context: &RequestContext,
) -> Result<FinancialDatasetSeries, ServiceError> {
    let (source, authorization) = read_authorized_financial_source(
        research,
        instrument_id,
        source_selection_as_of,
        effective_date,
        context.deadline(),
        context.cancellation(),
    )
    .await?;
    check_control(context.deadline(), context.cancellation())?;
    let series = research
        .analytical()
        .dataset_builder()
        .financial_series(
            source,
            selection,
            cadence,
            FinancialSeriesLimits::try_new(MAXIMUM_FACTS, MAXIMUM_FRAMES, MAXIMUM_SOURCE_BYTES)
                .map_err(|_| ServiceError::Internal)?,
            context.deadline(),
            context.cancellation(),
        )
        .map_err(map_financial_series_error)?;
    check_control(context.deadline(), context.cancellation())?;
    if current_timestamp()? >= authorization.expires_at() {
        return Err(ServiceError::Unavailable);
    }
    let _consumed_source_admission_permit = authorization.into_permit();
    Ok(series)
}

/// Shared exact selector and current-use authority for total projections and native series.
async fn read_authorized_financial_source(
    research: &ResearchService,
    instrument_id: InstrumentId,
    source_selection_as_of: Timestamp,
    effective_date: CalendarDate,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(SecResearchIdentitySelection, AuthorizedResearchUse), ServiceError> {
    check_control(deadline, cancellation)?;
    if source_selection_as_of > current_timestamp()?
        || i64::from(effective_date.days_since_unix_epoch())
            > source_selection_as_of
                .unix_nanos()
                .div_euclid(86_400_000_000_000)
    {
        return Err(ServiceError::InvalidRequest);
    }
    let limits = PointInTimeLimits::try_new(
        MAXIMUM_SOURCE_ROWS,
        MAXIMUM_SOURCE_ROWS,
        1_024,
        MAXIMUM_SOURCE_ROWS,
        MAXIMUM_SOURCE_BYTES,
    )
    .map_err(|_| ServiceError::Internal)?;
    let request = SecResearchIdentityReadRequest::try_new(
        instrument_id,
        SecResearchFamily::CompanyFacts,
        source_selection_as_of,
        ResearchTemporalCoordinate::calendar_date(effective_date),
        PointInTimeRevisionMode::LatestKnown,
        limits,
        MAXIMUM_SOURCE_BYTES,
    )
    .map_err(map_source_error)?;
    let source = research
        .analytical()
        .sec_research_reader()
        .select_by_identity(
            request,
            &research.provider_capture_store(),
            deadline,
            cancellation.child_token(),
        )
        .await
        .map_err(map_source_error)?;
    check_control(deadline, cancellation)?;
    let SecResearchIdentityOutcome::Exact(selected) = source.outcome() else {
        return Err(ServiceError::Unavailable);
    };
    if source.identity().disposition() != CompanySecurityIdentityDisposition::Complete
        || selected.disposition() != SecResearchDisposition::Selected
        || !selected.conflicts().is_empty()
        || selected.selected().len() > MAXIMUM_SOURCE_ROWS
    {
        return Err(ServiceError::InvalidResult);
    }
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(ServiceError::DeadlineExceeded)?;
    let rights_limits = ResearchUseLimits::try_new(
        1,
        4_096,
        8_192,
        4_096,
        4 * 1024 * 1024,
        remaining.min(Duration::from_secs(5)),
        Duration::from_secs(300),
    )
    .map_err(|_| ServiceError::InvalidRequest)?;
    let authorization = research
        .analytical()
        .authorize_research_use(
            ResearchUseRequest::try_new(
                vec![selected.origin().manifest().clone()],
                ResearchUse::LocalAnalysis,
                rights_limits,
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            cancellation,
        )
        .map_err(map_rights_error)?;
    if authorization.research_use() != ResearchUse::LocalAnalysis
        || !authorization
            .graph()
            .nodes()
            .iter()
            .any(|node| node.manifest() == selected.origin().manifest())
    {
        return Err(ServiceError::InvalidResult);
    }
    check_control(deadline, cancellation)?;
    if current_timestamp()? >= authorization.expires_at() {
        return Err(ServiceError::Unavailable);
    }
    Ok((source, authorization))
}

fn map_financial_series_error(error: DatasetBuildError) -> ServiceError {
    match error {
        DatasetBuildError::Cancelled => ServiceError::Cancelled,
        DatasetBuildError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        DatasetBuildError::LimitExceeded => ServiceError::ResourceExhausted,
        DatasetBuildError::InvalidRequest | DatasetBuildError::InvalidLimits => {
            ServiceError::InvalidRequest
        }
        DatasetBuildError::ComponentEvidenceMismatch
        | DatasetBuildError::ComponentAdjustmentMismatch
        | DatasetBuildError::InvalidInputGeneration
        | DatasetBuildError::MissingValueRejected
        | DatasetBuildError::EmptyDataset
        | DatasetBuildError::TemporalLeakage => ServiceError::Unavailable,
        DatasetBuildError::ResearchUse(error) => map_rights_error(error),
        _ => ServiceError::InvalidResult,
    }
}

fn normalize_selection(
    source: &SecResearchSelection,
    instrument_id: InstrumentId,
    source_selection_as_of: Timestamp,
    effective_date: CalendarDate,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<
    (
        Vec<FinancialTargetFact>,
        Vec<FinancialTargetFrame>,
        EvidenceDigest,
        usize,
    ),
    ServiceError,
> {
    let mut facts = Vec::new();
    facts
        .try_reserve_exact(MAXIMUM_FACTS)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    let mut frames: Vec<FinancialTargetFrame> = Vec::new();
    frames
        .try_reserve_exact(MAXIMUM_FRAMES)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    let mut source_digest = Sha256::new();
    source_digest.update(POLICY);
    source_digest.update(source.receipt().result_digest().bytes());
    source_digest.update(source.receipt().selection_digest().bytes());
    let mut canonical_bytes = 0_usize;
    for (index, row) in source.selected().iter().enumerate() {
        if index % 32 == 0 {
            check_control(deadline, cancellation)?;
        }
        let ordinal = row.row().row_ordinal();
        let Some(ResearchObservation::Fundamental(observation)) = source
            .decoded_rows()
            .get(usize::try_from(ordinal).map_err(|_| ServiceError::InvalidResult)?)
        else {
            return Err(ServiceError::InvalidResult);
        };
        let Some(role) = product_metric(observation.concept().as_str())
            .and_then(FinancialTargetRole::from_metric)
        else {
            continue;
        };
        let context = observation.context();
        let fact = observation.fact_context();
        let period = fact.period();
        let available = context
            .provenance()
            .availability()
            .conservative_available_at()
            .ok_or(ServiceError::InvalidResult)?;
        if context.provenance().instrument_id() != Some(instrument_id)
            || context.provenance().source_id() != source.origin().source_id()
            || available > source_selection_as_of
            || period.end() > effective_date
            || !role.period_matches(period)
            || fact
                .dimensions()
                .dimensions()
                .is_some_and(|dimensions| !dimensions.is_empty())
            || matches!(
                context.provenance().quality(),
                DataQuality::Modeled | DataQuality::Stale | DataQuality::Quarantined
            )
        {
            return Err(ServiceError::InvalidResult);
        }
        let currency = Currency::try_from(
            observation
                .unit()
                .as_str()
                .strip_prefix("iso4217:")
                .unwrap_or(observation.unit().as_str()),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        if facts.len() == MAXIMUM_FACTS {
            return Err(ServiceError::ResourceExhausted);
        }
        let envelope_identity = canonical_digest(
            &(
                source.receipt().origin_digest(),
                currency,
                period,
                fact.accession(),
                fact.filing_form(),
                fact.filed_on(),
                fact.fiscal_year(),
                fact.fiscal_period(),
                fact.frame(),
                fact.cadence(),
                fact.xbrl_context_id(),
                fact.dimensions(),
                fact.consolidation(),
                fact.amendment_status(),
                fact.restatement_status(),
            ),
            &mut canonical_bytes,
        )?;
        let mut digest = Sha256::new();
        digest.update(POLICY);
        digest.update(envelope_identity.bytes());
        digest.update(row.row().canonical_row_digest().bytes());
        digest.update(row.row().observation_digest().bytes());
        digest.update(row.point_in_time().evidence_identity().bytes());
        digest.update(ordinal.to_be_bytes());
        digest.update(
            canonical_digest(&(role, role.basis(), observation), &mut canonical_bytes)?.bytes(),
        );
        let evidence_identity = finish_digest(digest)?;
        let fact_index = facts.len();
        let frame_index = match frames
            .iter()
            .position(|frame| frame.identity == envelope_identity)
        {
            Some(index) => index,
            None => {
                if frames.len() == MAXIMUM_FRAMES {
                    return Err(ServiceError::ResourceExhausted);
                }
                frames.push(FinancialTargetFrame {
                    identity: envelope_identity,
                    period,
                    currency,
                    fact_indices: Vec::new(),
                });
                frames.len() - 1
            }
        };
        frames[frame_index]
            .fact_indices
            .try_reserve_exact(1)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        frames[frame_index].fact_indices.push(fact_index);
        source_digest.update(evidence_identity.bytes());
        facts.push(FinancialTargetFact {
            row_ordinal: ordinal,
            role,
            amount: Money::new(observation.value(), currency),
            period,
            envelope_identity,
            evidence_identity,
        });
    }
    if facts.is_empty() {
        return Err(ServiceError::Unavailable);
    }
    // The original SEC selection keeps its own data-authority bounds. Count all additional
    // projection slots here; canonical hashing streams without allocating an encoding buffer.
    let retained = frames.iter().try_fold(
        size_of::<FinancialTargetHistory>()
            .checked_add(
                facts
                    .capacity()
                    .checked_mul(size_of::<FinancialTargetFact>())
                    .ok_or(ServiceError::ResourceExhausted)?,
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    frames
                        .capacity()
                        .checked_mul(size_of::<FinancialTargetFrame>())?,
                )
            })
            .ok_or(ServiceError::ResourceExhausted)?,
        |bytes, frame| {
            bytes
                .checked_add(
                    frame
                        .fact_indices
                        .capacity()
                        .checked_mul(size_of::<usize>())
                        .ok_or(ServiceError::ResourceExhausted)?,
                )
                .ok_or(ServiceError::ResourceExhausted)
        },
    )?;
    Ok((facts, frames, finish_digest(source_digest)?, retained))
}

struct DigestWriter<'budget> {
    hash: Sha256,
    bytes: &'budget mut usize,
}
impl io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        *self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|value| *value <= MAXIMUM_CANONICAL_BYTES)
            .ok_or_else(|| io::Error::other("fiscal input canonical byte budget exceeded"))?;
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn canonical_digest(
    value: &impl Serialize,
    bytes: &mut usize,
) -> Result<EvidenceDigest, ServiceError> {
    let mut writer = DigestWriter {
        hash: Sha256::new(),
        bytes,
    };
    writer.hash.update(POLICY);
    serde_json::to_writer(&mut writer, value).map_err(|error| {
        if error.is_io() {
            ServiceError::ResourceExhausted
        } else {
            ServiceError::InvalidResult
        }
    })?;
    finish_digest(writer.hash)
}
fn finish_digest(hash: Sha256) -> Result<EvidenceDigest, ServiceError> {
    let bytes = hash.finalize().into();
    if bytes == [0; 32] {
        return Err(ServiceError::InvalidResult);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}
fn check_control(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn current_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Internal)?;
    i64::try_from(elapsed.as_nanos())
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| ServiceError::Internal)
}
fn map_source_error(error: SecResearchReadError) -> ServiceError {
    match error {
        SecResearchReadError::Cancelled => ServiceError::Cancelled,
        SecResearchReadError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        SecResearchReadError::ObjectBudgetExceeded => ServiceError::ResourceExhausted,
        SecResearchReadError::InvalidRequest => ServiceError::InvalidRequest,
        SecResearchReadError::AuthorityUnavailable => ServiceError::Unavailable,
        _ => ServiceError::InvalidResult,
    }
}

fn map_rights_error(error: ResearchUseCatalogError) -> ServiceError {
    match error {
        ResearchUseCatalogError::Cancelled => ServiceError::Cancelled,
        ResearchUseCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ResearchUseCatalogError::LimitExceeded => ServiceError::ResourceExhausted,
        ResearchUseCatalogError::Denied { .. }
        | ResearchUseCatalogError::Expired
        | ResearchUseCatalogError::Revoked
        | ResearchUseCatalogError::UnknownGeneration => ServiceError::Unavailable,
        _ => ServiceError::InvalidResult,
    }
}
