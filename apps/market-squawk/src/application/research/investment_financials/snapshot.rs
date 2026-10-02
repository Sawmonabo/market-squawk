//! Immutable selected-row and complete reporting-envelope coordinate index.
use super::*;

#[allow(
    clippy::too_many_arguments,
    reason = "operation-owned selected evidence and its frozen request"
)]
pub(super) fn build_snapshot(
    request: CompanyResearchRequest,
    selection_token: String,
    section: InvestmentFinancialSection,
    effective_on: String,
    families: Vec<FamilyAvailability>,
    selections: Vec<SecResearchIdentitySelection>,
    authorized: Vec<bool>,
    scratch: OperationScratchDirectory,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Snapshot, ServiceError> {
    let index = scratch
        .path()
        .join(format!("investment-financials-{}.sqlite", Uuid::new_v4()));
    let connection = Connection::open(&index).map_err(sql_error)?;
    connection.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; CREATE TABLE coordinates(ordinal INTEGER PRIMARY KEY,family INTEGER NOT NULL,position INTEGER NOT NULL,envelope BLOB); CREATE INDEX envelopes ON coordinates(envelope,ordinal); BEGIN").map_err(sql_error)?;
    let mut ordinal = 0_i64;
    let mut omitted_facts = 0;
    let mut issuer = None;
    for (family, selected) in selections.iter().enumerate() {
        check(deadline, cancellation)?;
        if !authorized[family] {
            continue;
        }
        let SecResearchIdentityOutcome::Exact(exact) = selected.outcome() else {
            continue;
        };
        if exact.disposition() != SecResearchDisposition::Selected {
            continue;
        }
        let [relationship] = selected.identity().candidates() else {
            return Err(ServiceError::InvalidResult);
        };
        if selected.request().instrument_id() != request.instrument_id()
            || selected.request().knowledge_at() != request.knowledge_at()
            || selected.request().effective_cutoff() != request.fact_effective_cutoff()
            || selected.request().revision_mode()
                != market_squawk_data::PointInTimeRevisionMode::LatestKnown
        {
            return Err(ServiceError::InvalidResult);
        }
        let link = relationship.link();
        let company = exact.company_identity().observation();
        if link.instrument_id() != request.instrument_id()
            || link.company_observation_digest() != exact.receipt().company_observation_digest()
            || link.company_source_id() != company.source_id()
            || link.provider_company_id() != company.provider_company_id()
            || link.company_surface() != company.surface()
        {
            return Err(ServiceError::InvalidResult);
        }
        if issuer
            .as_ref()
            .is_some_and(|previous| previous != link.provider_company_id())
        {
            return Err(ServiceError::InvalidResult);
        }
        issuer = Some(link.provider_company_id().clone());
        for position in 0..exact.selected().len() {
            check(deadline, cancellation)?;
            let (fact, filing) =
                selected_company_row(&request, exact, position).map_err(canonical_error)?;
            let envelope = if let Some(fact) = fact {
                let Some(fact) =
                    project_fact(&fact, request.knowledge_at()).map_err(projection_error)?
                else {
                    omitted_facts += 1;
                    continue;
                };
                Some(fact_envelope_bytes(&fact).map_err(projection_error)?)
            } else if filing.is_some() {
                None
            } else {
                return Err(ServiceError::InvalidResult);
            };
            connection
                .execute(
                    "INSERT INTO coordinates(ordinal,family,position,envelope) VALUES(?1,?2,?3,?4)",
                    params![ordinal, family as i64, position as i64, envelope],
                )
                .map_err(sql_error)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
        }
    }
    connection.execute_batch("CREATE TABLE reporting_envelopes(ordinal INTEGER PRIMARY KEY,envelope BLOB NOT NULL UNIQUE); INSERT INTO reporting_envelopes(envelope) SELECT envelope FROM coordinates WHERE envelope IS NOT NULL GROUP BY envelope ORDER BY MIN(ordinal); COMMIT").map_err(sql_error)?;
    Ok(Snapshot {
        request,
        selection_token,
        section,
        effective_on,
        families,
        selections,
        index,
        _scratch: scratch,
        omitted_facts,
    })
}
