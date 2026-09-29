//! Exact saved split-price comparisons over the existing complete-history source authority.
//! References are inert restart coordinates. Only a fresh source read constructs observations.

use super::{MarketHistoryReadCapability, MarketHistoryUnavailableReason as Error};
use crate::{ResearchService, application::market_calendar::CompletedMarketSessionReadCapability};
use market_squawk_data::ResearchUsePermit;
use market_squawk_domain::{CalendarDate, Currency, DataQuality, InstrumentId, Timestamp};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

mod projection;
mod source;

const MAX_POINTS: usize = 4096;
const MAX_MEMBERS: usize = 3;
const VERSION: u16 = 1;

/// Source-owned bounded restart recipe. A missing member stays missing on later reads.
/// Member order is subject, selected comparison, optional accompanying comparison.
/// The decision owner must bind these bytes and the independently saved canonical selection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BenchmarkHistoryReference {
    version: u16,
    currency: Currency,
    source_cutoff: Timestamp,
    observed_through: Timestamp,
    members: Vec<BenchmarkMemberReference>,
    projection_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkMemberReference {
    instrument_id: InstrumentId,
    source: Option<BenchmarkSourceReference>,
}

/// Exact existing read identities; no caller-authored adjustment or calendar recipe.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkSourceReference {
    selected_manifest: [u8; 32],
    origin_manifest: [u8; 32],
    selection_sha256: [u8; 32],
    publication_sha256: [u8; 32],
    capture_sha256: [u8; 32],
    history_sha256: [u8; 32],
    result_sha256: [u8; 32],
    native_sessions_sha256: [u8; 32],
}

impl BenchmarkHistoryReference {
    pub(crate) const fn currency(&self) -> Currency {
        self.currency
    }
    pub(crate) const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
    pub(crate) const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }
    pub(crate) fn instruments(&self) -> impl Iterator<Item = InstrumentId> + '_ {
        self.members.iter().map(|member| member.instrument_id)
    }
    pub(crate) const fn projection_sha256(&self) -> [u8; 32] {
        self.projection_sha256
    }
}

/// A genuine named regular session. Its close is not a provider's daily aggregation timestamp.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct BenchmarkHistoryCoordinate {
    pub(crate) session_close: Timestamp,
    pub(crate) date: CalendarDate,
}

/// Exact original price and Rust-owned comparable value, excluding cash distributions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct BenchmarkHistoryObservation {
    pub(crate) close: Decimal,
    /// Base 100 at the same original session for every displayed member; eight decimal places,
    /// midpoint-to-even. This is a price index, not a total return or an event probability.
    pub(crate) price_index: Decimal,
    pub(crate) available_at: Timestamp,
    pub(crate) provider_completed_at: Option<Timestamp>,
    pub(crate) quality: DataQuality,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct BenchmarkHistoryPoint {
    pub(crate) coordinate: BenchmarkHistoryCoordinate,
    /// None is a gap and must break the rendered line. Never interpolate or forward-fill.
    pub(crate) observations: Vec<Option<BenchmarkHistoryObservation>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum BenchmarkHistoryDisposition {
    Available,
    MissingSubject,
    MissingSelectedComparison,
    NoCommonObservation,
}

/// Original economic reference and newly authorized, bounded observations.
pub(crate) struct BenchmarkHistoryEvaluation {
    reference: BenchmarkHistoryReference,
    disposition: BenchmarkHistoryDisposition,
    baseline: Option<BenchmarkHistoryCoordinate>,
    points: Vec<BenchmarkHistoryPoint>,
    /// Fresh rights receipts are intentionally outside the immutable economic identity.
    rights: Vec<BenchmarkHistoryRights>,
}

#[derive(Clone, Debug)]
pub(crate) struct BenchmarkHistoryRights {
    pub(crate) instrument_id: InstrumentId,
    pub(crate) decision_sha256: [u8; 32],
    pub(crate) graph_sha256: [u8; 32],
    pub(crate) expires_at: Timestamp,
}

impl BenchmarkHistoryEvaluation {
    pub(crate) const fn reference(&self) -> &BenchmarkHistoryReference {
        &self.reference
    }
    pub(crate) const fn disposition(&self) -> BenchmarkHistoryDisposition {
        self.disposition
    }
    pub(crate) const fn baseline(&self) -> Option<BenchmarkHistoryCoordinate> {
        self.baseline
    }
    pub(crate) fn points(&self) -> &[BenchmarkHistoryPoint] {
        &self.points
    }
    pub(crate) fn rights(&self) -> &[BenchmarkHistoryRights] {
        &self.rights
    }
}

struct SourceSeries {
    reference: BenchmarkSourceReference,
    // Includes genuine expected native sessions with missing bars.
    points: Vec<(
        BenchmarkHistoryCoordinate,
        Option<BenchmarkHistoryObservation>,
    )>,
    permit: ResearchUsePermit,
}

impl MarketHistoryReadCapability {
    /// Original publication-time selection only. An absent native Split generation is unavailable;
    /// neither raw nor All-adjusted history is a substitute. No provider acquisition is performed.
    #[allow(
        clippy::too_many_arguments,
        reason = "original identities, clocks and lifecycle stay explicit"
    )]
    pub(crate) async fn read_benchmark_history(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        subject_and_selected: [InstrumentId; 2],
        accompanying: Option<InstrumentId>,
        currency: Currency,
        source_cutoff: Timestamp,
        observed_through: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BenchmarkHistoryEvaluation, Error> {
        check(deadline, &cancellation)?;
        if observed_through > source_cutoff || source_cutoff > wall_now()? {
            return Err(Error::IntegrityUnproven);
        }
        let mut members = reserved(MAX_MEMBERS)?;
        for instrument in subject_and_selected
            .into_iter()
            .chain(accompanying.filter(|value| !subject_and_selected.contains(value)))
        {
            members.push(BenchmarkMemberReference {
                instrument_id: instrument,
                source: None,
            });
        }
        self.benchmark_history_bound(
            research,
            calendars,
            BenchmarkHistoryReference {
                version: VERSION,
                currency,
                source_cutoff,
                observed_through,
                members,
                projection_sha256: [0; 32],
            },
            false,
            deadline,
            cancellation,
        )
        .await
    }

    /// Reopens every present source by its exact content hash at its original cutoff. Missing
    /// sources are never looked up; expiry today does not alter original economics or select latest.
    pub(crate) async fn read_saved_benchmark_history(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        saved: &BenchmarkHistoryReference,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BenchmarkHistoryEvaluation, Error> {
        check(deadline, &cancellation)?;
        validate_reference(saved)?;
        self.benchmark_history_bound(
            research,
            calendars,
            saved.clone(),
            true,
            deadline,
            cancellation,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "fresh versus exact source reads share one authority path"
    )]
    async fn benchmark_history_bound(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        mut reference: BenchmarkHistoryReference,
        saved: bool,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BenchmarkHistoryEvaluation, Error> {
        let mut sources = reserved(reference.members.len())?;
        for member in &mut reference.members {
            check(deadline, &cancellation)?;
            let source = if saved && member.source.is_none() {
                None
            } else {
                self.benchmark_source(
                    research,
                    calendars,
                    member.instrument_id,
                    reference.currency,
                    reference.source_cutoff,
                    reference.observed_through,
                    member.source.as_ref(),
                    deadline,
                    &cancellation,
                )
                .await?
            };
            if saved && source.as_ref().map(|value| &value.reference) != member.source.as_ref() {
                // Missing exact storage is unavailable; changed retained identity is integrity failure.
                return Err(if source.is_none() {
                    Error::StorageUnavailable
                } else {
                    Error::IntegrityUnproven
                });
            }
            member.source = source.as_ref().map(|value| value.reference.clone());
            sources.push(source);
        }
        let (disposition, baseline, points) = projection::align(&sources, deadline, &cancellation)?;
        let projection = projection::digest(disposition, baseline, &points)?;
        if saved && projection != reference.projection_sha256 {
            return Err(Error::IntegrityUnproven);
        }
        reference.projection_sha256 = projection;
        let mut rights = reserved(sources.len())?;
        let now = wall_now()?;
        for (member, source) in reference.members.iter().zip(sources) {
            if let Some(source) = source {
                if source.permit.expires_at() <= now {
                    return Err(Error::IntegrityUnproven);
                }
                rights.push(BenchmarkHistoryRights {
                    instrument_id: member.instrument_id,
                    decision_sha256: source.permit.decision_digest().bytes(),
                    graph_sha256: source.permit.graph_digest().bytes(),
                    expires_at: source.permit.expires_at(),
                });
            }
        }
        check(deadline, &cancellation)?;
        Ok(BenchmarkHistoryEvaluation {
            reference,
            disposition,
            baseline,
            points,
            rights,
        })
    }
}

fn validate_reference(value: &BenchmarkHistoryReference) -> Result<(), Error> {
    if value.version != VERSION
        || !(2..=MAX_MEMBERS).contains(&value.members.len())
        || value.observed_through > value.source_cutoff
        || value.source_cutoff > wall_now()?
        || value.projection_sha256 == [0; 32]
        || value.members.get(2).is_some_and(|accompanying| {
            value.members[..2]
                .iter()
                .any(|member| member.instrument_id == accompanying.instrument_id)
        })
    {
        return Err(Error::IntegrityUnproven);
    }
    for member in &value.members {
        if let Some(source) = &member.source {
            if [
                source.selected_manifest,
                source.origin_manifest,
                source.selection_sha256,
                source.publication_sha256,
                source.capture_sha256,
                source.history_sha256,
                source.result_sha256,
                source.native_sessions_sha256,
            ]
            .contains(&[0; 32])
            {
                return Err(Error::IntegrityUnproven);
            }
        }
    }
    Ok(())
}

fn reserved<T>(capacity: usize) -> Result<Vec<T>, Error> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(capacity)
        .map_err(|_| Error::CapacityExceeded)?;
    Ok(result)
}
fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), Error> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else if Instant::now() >= deadline {
        Err(Error::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn wall_now() -> Result<Timestamp, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(Error::IntegrityUnproven)
}
