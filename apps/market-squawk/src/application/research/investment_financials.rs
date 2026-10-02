//! Selected financial pages over operation-owned immutable source indexes.
//!
//! The bounded handle cache owns idle read leases, not a second financial history. A page holds
//! its own Arc, so eviction or closing a screen cannot interrupt another admitted read or ingest.

use super::{
    company_product::{
        CompanyProductProjectionError, fact_envelope_bytes, project_fact, project_filing,
        project_financial_envelope,
    },
    company_research::{
        CanonicalResearchReadError, CompanyResearchReadCapability, CompanyResearchRequest,
        ResearchRevisionPolicy, selected_company_row,
    },
    corporate_actions::map_research_error,
};
use crate::{
    ResearchService, application::market_selection::product::MarketProductSelectionReadCapability,
};
use chrono::{DateTime, Datelike as _, SecondsFormat, Utc};
use market_squawk_data::{
    DatasetManifestRef, MAX_RESEARCH_USE_EDGES, MAX_RESEARCH_USE_GRAPH_NODES,
    MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS, MAX_RESEARCH_USE_RETAINED_BYTES,
    MAX_RESEARCH_USE_SOURCES, MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS, OperationScratchDirectory,
    ResearchUse, ResearchUseCatalogError, ResearchUseLimits, ResearchUseRequest,
    SecResearchDisposition, SecResearchFamily, SecResearchIdentityOutcome,
    SecResearchIdentitySelection,
};
use market_squawk_domain::{ResearchTemporalCoordinate, Timestamp};
use market_squawk_services::{ServiceError, ServiceLimits, ToolResultMetadata, TypedToolResult};
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod page;
mod snapshot;
use page::page;
use snapshot::build_snapshot;

const IDLE_HANDLES: usize = 16;
const IDLE_LEASE: Duration = Duration::from_secs(600);
const MAX_PAGE_ITEMS: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InvestmentFinancialSection {
    Facts,
    Statements,
    Ratios,
    Filings,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InvestmentFinancialState {
    Reported,
    Missing,
    Conflict,
    Unavailable,
    Expired,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FamilyAvailability {
    family: &'static str,
    state: InvestmentFinancialState,
    reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InvestmentFinancialResult {
    selection_token: String,
    section: InvestmentFinancialSection,
    knowledge_at: Option<String>,
    effective_on: Option<String>,
    revision_policy: &'static str,
    state: InvestmentFinancialState,
    families: Vec<FamilyAvailability>,
    items: Vec<Value>,
    current_cursor: Option<String>,
    next_cursor: Option<String>,
    read_token: Option<String>,
    omitted_items: usize,
    limitations: Vec<&'static str>,
}
impl InvestmentFinancialResult {
    pub(crate) fn item_count(&self) -> usize {
        self.items.len()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Cursor {
    read_token: Uuid,
    unit: u64,
    item: usize,
}

struct Snapshot {
    request: CompanyResearchRequest,
    selection_token: String,
    section: InvestmentFinancialSection,
    effective_on: String,
    families: Vec<FamilyAvailability>,
    selections: Vec<SecResearchIdentitySelection>,
    index: std::path::PathBuf,
    _scratch: OperationScratchDirectory,
    omitted_facts: usize,
}
struct IdleRead {
    snapshot: Arc<Snapshot>,
    touched: Instant,
}
#[derive(Default)]
struct ReadCache {
    entries: Mutex<BTreeMap<Uuid, IdleRead>>,
}
impl ReadCache {
    fn prune(&self) {
        let removed = if let Ok(mut entries) = self.entries.lock() {
            let expired: Vec<_> = entries
                .iter()
                .filter(|(_, read)| read.touched.elapsed() >= IDLE_LEASE)
                .map(|(id, _)| *id)
                .collect();
            expired
                .into_iter()
                .filter_map(|id| entries.remove(&id))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        // Closing disk indexes can perform I/O; never do so under the shared cache lock.
        drop(removed);
    }
}

#[derive(Clone)]
pub(crate) struct InvestmentFinancialReadCapability {
    research: Arc<ResearchService>,
    selections: MarketProductSelectionReadCapability,
    cache: Arc<ReadCache>,
}
impl std::fmt::Debug for InvestmentFinancialReadCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InvestmentFinancialReadCapability")
    }
}
impl InvestmentFinancialReadCapability {
    pub(crate) fn new(
        research: Arc<ResearchService>,
        selections: MarketProductSelectionReadCapability,
    ) -> Self {
        let cache = Arc::new(ReadCache::default());
        Self {
            research,
            selections,
            cache,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one selected section read with shared transport bounds and operation control"
    )]
    pub(crate) async fn read(
        &self,
        selection_token: &str,
        section: InvestmentFinancialSection,
        cursor: Option<&str>,
        limit: usize,
        limits: ServiceLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InvestmentFinancialResult, ServiceError> {
        check(deadline, cancellation)?;
        if !(1..=MAX_PAGE_ITEMS).contains(&limit) || limit > limits.maximum_result_items() {
            return Err(ServiceError::InvalidRequest);
        }
        self.cache.prune();
        let (id, snapshot, position, fresh) = if let Some(cursor) = cursor {
            if cursor.len() > 1024 {
                return Err(ServiceError::InvalidRequest);
            }
            let position: Cursor =
                serde_json::from_str(cursor).map_err(|_| ServiceError::InvalidRequest)?;
            let snapshot = {
                let mut entries = self
                    .cache
                    .entries
                    .lock()
                    .map_err(|_| ServiceError::Unavailable)?;
                entries.get_mut(&position.read_token).map(|read| {
                    read.touched = Instant::now();
                    Arc::clone(&read.snapshot)
                })
            };
            let Some(snapshot) = snapshot else {
                return Ok(expired(selection_token, section));
            };
            if snapshot.selection_token != selection_token || snapshot.section != section {
                return Err(ServiceError::InvalidRequest);
            }
            (position.read_token, snapshot, position, false)
        } else {
            let now = Utc::now();
            let cutoff = now
                .timestamp_nanos_opt()
                .filter(|n| *n > 0)
                .map(Timestamp::from_unix_nanos)
                .ok_or(ServiceError::Unavailable)?;
            let instrument = self
                .selections
                .resolve(selection_token, cutoff, deadline, cancellation)
                .await?;
            let date = cutoff
                .utc_calendar_date()
                .map_err(|_| ServiceError::Unavailable)?;
            let request = CompanyResearchRequest::try_new(
                instrument,
                cutoff,
                ResearchTemporalCoordinate::calendar_date(date),
                ResearchRevisionPolicy::LatestKnown,
            )
            .map_err(canonical_error)?;
            let reader = CompanyResearchReadCapability::new(Arc::clone(&self.research));
            let mut selections = Vec::new();
            let mut families = Vec::new();
            let requested: &[SecResearchFamily] = if section == InvestmentFinancialSection::Filings
            {
                &[SecResearchFamily::Submissions]
            } else {
                &[
                    SecResearchFamily::CompanyFacts,
                    SecResearchFamily::FilingXbrl,
                ]
            };
            for family in requested {
                check(deadline, cancellation)?;
                match reader
                    .select_company_family(&request, *family, deadline, cancellation.child_token())
                    .await
                {
                    Ok(selected) => {
                        families.push(availability(*family, selected.outcome()));
                        selections.push(selected);
                    }
                    Err(CanonicalResearchReadError::Cancelled) => {
                        return Err(ServiceError::Cancelled);
                    }
                    Err(CanonicalResearchReadError::DeadlineExceeded) => {
                        return Err(ServiceError::DeadlineExceeded);
                    }
                    Err(_) => families.push(FamilyAvailability {
                        family: family_name(*family),
                        state: InvestmentFinancialState::Unavailable,
                        reason: Some("evidence_unavailable"),
                    }),
                }
            }
            // Authorize the original manifests before deriving even the grouping index.
            let authorized = self
                .authorize(&selections, section, deadline, cancellation)
                .await?;
            for (selected, allowed) in selections.iter().zip(&authorized) {
                if !allowed {
                    if let Some(family) = families
                        .iter_mut()
                        .find(|family| family.family == family_name(selected.request().family()))
                    {
                        family.state = InvestmentFinancialState::Unavailable;
                        family.reason = Some("rights_unavailable");
                    }
                }
            }
            let scratch = self
                .research
                .analytical()
                .operation_scratch()
                .map_err(|_| ServiceError::Unavailable)?;
            let selection_token = selection_token.to_owned();
            let effective_on = format!("{:04}-{:02}-{:02}", now.year(), now.month(), now.day());
            let snapshot = self
                .research
                .run_owned_research_generation_read(deadline, cancellation, move |control| {
                    build_snapshot(
                        request,
                        selection_token,
                        section,
                        effective_on,
                        families,
                        selections,
                        authorized,
                        scratch,
                        deadline,
                        &control,
                    )
                })
                .await
                .map_err(map_research_error)??;
            let id = Uuid::new_v4();
            (
                id,
                Arc::new(snapshot),
                Cursor {
                    read_token: id,
                    unit: 0,
                    item: 0,
                },
                true,
            )
        };
        // Each continuation re-authorizes every contributing immutable manifest. Retained source
        // row/relationship receipts are the original verified objects, not caller cursor fields.
        let authorized = self
            .authorize(&snapshot.selections, section, deadline, cancellation)
            .await?;
        let admitted_before_page = authorized.clone();
        let owned = Arc::clone(&snapshot);
        let result = self
            .research
            .run_owned_research_generation_read(deadline, cancellation, move |control| {
                page(
                    &owned,
                    &position,
                    limit,
                    limits,
                    &authorized,
                    deadline,
                    &control,
                )
            })
            .await
            .map_err(map_research_error)??;
        check(deadline, cancellation)?;
        // Queueing and projection may outlive a permit. Obtain fresh original-manifest
        // authorization after all blocking work before exposing any derived payload.
        let admitted_at_return = self
            .authorize(&snapshot.selections, section, deadline, cancellation)
            .await?;
        if admitted_before_page
            .iter()
            .zip(&admitted_at_return)
            .any(|(before, after)| *before && !after)
        {
            return Err(ServiceError::Unavailable);
        }
        check(deadline, cancellation)?;
        if fresh {
            let removed = {
                let mut entries = self
                    .cache
                    .entries
                    .lock()
                    .map_err(|_| ServiceError::Unavailable)?;
                let oldest = if entries.len() >= IDLE_HANDLES {
                    entries
                        .iter()
                        .min_by_key(|(_, read)| read.touched)
                        .map(|(id, _)| *id)
                } else {
                    None
                };
                let removed = oldest.and_then(|oldest| entries.remove(&oldest));
                entries.insert(
                    id,
                    IdleRead {
                        snapshot,
                        touched: Instant::now(),
                    },
                );
                removed
            };
            drop(removed);
        }
        Ok(result)
    }

    pub(crate) fn close(
        &self,
        selection_token: &str,
        read_token: &str,
    ) -> Result<bool, ServiceError> {
        self.cache.prune();
        let id = Uuid::parse_str(read_token).map_err(|_| ServiceError::InvalidRequest)?;
        let removed = {
            let mut entries = self
                .cache
                .entries
                .lock()
                .map_err(|_| ServiceError::Unavailable)?;
            if entries
                .get(&id)
                .is_some_and(|read| read.snapshot.selection_token != selection_token)
            {
                return Err(ServiceError::InvalidRequest);
            }
            entries.remove(&id)
        };
        Ok(removed.is_some())
    }
    pub(crate) fn clear(&self) {
        let removed = self
            .cache
            .entries
            .lock()
            .ok()
            .map(|mut entries| std::mem::take(&mut *entries));
        drop(removed);
    }

    async fn authorize(
        &self,
        selections: &[SecResearchIdentitySelection],
        section: InvestmentFinancialSection,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<bool>, ServiceError> {
        let mut allowed = Vec::new();
        for selected in selections {
            let SecResearchIdentityOutcome::Exact(exact) = selected.outcome() else {
                allowed.push(true);
                continue;
            };
            if exact.disposition() != SecResearchDisposition::Selected {
                allowed.push(true);
                continue;
            }
            let roots: Vec<DatasetManifestRef> = vec![exact.origin().manifest().clone()];
            let uses: &[ResearchUse] = if section == InvestmentFinancialSection::Ratios {
                &[ResearchUse::Display, ResearchUse::LocalAnalysis]
            } else {
                &[ResearchUse::Display]
            };
            let mut admitted = true;
            for use_kind in uses {
                check(deadline, cancellation)?;
                let duration =
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_secs(
                            MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS,
                        ));
                let request = ResearchUseRequest::try_new(
                    roots.clone(),
                    *use_kind,
                    ResearchUseLimits::try_new(
                        1,
                        MAX_RESEARCH_USE_GRAPH_NODES,
                        MAX_RESEARCH_USE_EDGES,
                        MAX_RESEARCH_USE_SOURCES,
                        MAX_RESEARCH_USE_RETAINED_BYTES,
                        duration,
                        Duration::from_secs(MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS),
                    )
                    .map_err(|_| ServiceError::InvalidResult)?,
                )
                .map_err(|_| ServiceError::InvalidResult)?;
                let authorization = self
                    .research
                    .authorize_research_use(request, deadline, cancellation)
                    .await
                    .map_err(map_research_error)?;
                let authorization = match authorization {
                    Ok(authorization) => authorization,
                    Err(ResearchUseCatalogError::Cancelled) => return Err(ServiceError::Cancelled),
                    Err(ResearchUseCatalogError::DeadlineExceeded) => {
                        return Err(ServiceError::DeadlineExceeded);
                    }
                    Err(_) => {
                        admitted = false;
                        break;
                    }
                };
                if authorization.research_use() != *use_kind
                    || authorization.graph().roots() != roots.as_slice()
                    || Utc::now()
                        .timestamp_nanos_opt()
                        .is_none_or(|now| now >= authorization.expires_at().unix_nanos())
                {
                    return Err(ServiceError::InvalidResult);
                }
            }
            allowed.push(admitted);
        }
        Ok(allowed)
    }
}

fn availability(
    family: SecResearchFamily,
    outcome: &SecResearchIdentityOutcome,
) -> FamilyAvailability {
    use InvestmentFinancialState as S;
    let (state, reason) = match outcome {
        SecResearchIdentityOutcome::Missing => (S::Missing, Some("identity_missing")),
        SecResearchIdentityOutcome::Ambiguous => (S::Conflict, Some("identity_ambiguous")),
        SecResearchIdentityOutcome::Stale => (S::Unavailable, Some("identity_stale")),
        SecResearchIdentityOutcome::Revoked => (S::Unavailable, Some("identity_revoked")),
        SecResearchIdentityOutcome::Exact(exact) => match exact.disposition() {
            SecResearchDisposition::Selected => (S::Reported, None),
            SecResearchDisposition::Conflict => (S::Conflict, Some("revision_conflict")),
            SecResearchDisposition::Unavailable => (S::Missing, Some("no_records")),
        },
    };
    FamilyAvailability {
        family: family_name(family),
        state,
        reason,
    }
}
fn family_name(family: SecResearchFamily) -> &'static str {
    match family {
        SecResearchFamily::CompanyFacts => "company_facts",
        SecResearchFamily::Submissions => "filings",
        SecResearchFamily::FilingXbrl => "filing_details",
    }
}

fn empty_state(families: &[FamilyAvailability]) -> InvestmentFinancialState {
    if families
        .iter()
        .any(|family| family.state == InvestmentFinancialState::Conflict)
    {
        InvestmentFinancialState::Conflict
    } else if families
        .iter()
        .any(|family| family.state == InvestmentFinancialState::Unavailable)
    {
        InvestmentFinancialState::Unavailable
    } else {
        InvestmentFinancialState::Missing
    }
}
fn expired(
    selection_token: &str,
    section: InvestmentFinancialSection,
) -> InvestmentFinancialResult {
    InvestmentFinancialResult {
        selection_token: selection_token.to_owned(),
        section,
        knowledge_at: None,
        effective_on: None,
        revision_policy: "latestKnown",
        state: InvestmentFinancialState::Expired,
        families: Vec::new(),
        items: Vec::new(),
        current_cursor: None,
        next_cursor: None,
        read_token: None,
        omitted_items: 0,
        limitations: vec!["read_expired"],
    }
}
fn cursor(read_token: Uuid, unit: u64, item: usize) -> Result<String, ServiceError> {
    serde_json::to_string(&Cursor {
        read_token,
        unit,
        item,
    })
    .map_err(|_| ServiceError::InvalidResult)
}
fn timestamp_text(value: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(value.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}
fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn sql_error(_: rusqlite::Error) -> ServiceError {
    ServiceError::Unavailable
}
fn canonical_error(error: CanonicalResearchReadError) -> ServiceError {
    match error {
        CanonicalResearchReadError::Cancelled => ServiceError::Cancelled,
        CanonicalResearchReadError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        CanonicalResearchReadError::InvalidRequest => ServiceError::InvalidRequest,
        CanonicalResearchReadError::AuthorityUnavailable => ServiceError::Unavailable,
        CanonicalResearchReadError::ResourceExhausted => ServiceError::ResourceExhausted,
        _ => ServiceError::InvalidResult,
    }
}
fn projection_error(error: CompanyProductProjectionError) -> ServiceError {
    match error {
        CompanyProductProjectionError::ResourceExhausted => ServiceError::ResourceExhausted,
        CompanyProductProjectionError::InvalidEvidence => ServiceError::InvalidResult,
    }
}
