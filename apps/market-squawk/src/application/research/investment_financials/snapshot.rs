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
    begin_coordinates(&connection)?;
    let mut ordinal = 0_i64;
    let mut omitted_facts = 0_usize;
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
        for coordinate in exact
            .selected_display_coordinates()
            .map_err(map_company_data_error)
            .map_err(canonical_error)?
        {
            check(deadline, cancellation)?;
            let (position, coordinate) = coordinate
                .map_err(map_company_data_error)
                .map_err(canonical_error)?;
            let Some(coordinate) = coordinate else {
                omitted_facts = omitted_facts
                    .checked_add(1)
                    .ok_or(ServiceError::ResourceExhausted)?;
                continue;
            };
            // The data iterator yields selected positions, not original source ordinals.
            // Page reads still decode and validate only the requested original evidence.
            connection
                .execute(
                    "INSERT INTO source_coordinates(ordinal,family,position,envelope,effective_day,effective_time,published_day,published_time) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![ordinal, family as i64, position as i64, coordinate.envelope(),
                        coordinate.effective_day(), coordinate.effective_time(),
                        coordinate.published_day(), coordinate.published_time()],
                )
                .map_err(sql_error)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
        }
    }
    check(deadline, cancellation)?;
    finish_coordinates(&connection)?;
    check(deadline, cancellation)?;
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

pub(super) fn begin_coordinates(connection: &Connection) -> Result<(), ServiceError> {
    connection
        .execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE;
         PRAGMA mmap_size=0; PRAGMA cache_size=-1024;
         CREATE TABLE source_coordinates(
             ordinal INTEGER PRIMARY KEY,family INTEGER NOT NULL,position INTEGER NOT NULL,
             envelope BLOB,effective_day INTEGER NOT NULL,effective_time INTEGER,
             published_day INTEGER,published_time INTEGER);
         BEGIN",
        )
        .map_err(sql_error)
}

pub(super) fn finish_coordinates(connection: &Connection) -> Result<(), ServiceError> {
    // The immutable display ordinals are independent of the original selected row positions.
    // Equal dates retain original source order; no page sorts or rereads the complete corpus.
    connection.execute_batch(
        "CREATE TABLE coordinates(ordinal INTEGER PRIMARY KEY,family INTEGER NOT NULL,
             position INTEGER NOT NULL,envelope BLOB);
         INSERT INTO coordinates(ordinal,family,position,envelope)
             SELECT ROW_NUMBER() OVER (
                 ORDER BY effective_day DESC,effective_time DESC,published_day DESC,
                     published_time DESC,ordinal ASC)-1,family,position,envelope
             FROM source_coordinates;
         DROP TABLE source_coordinates;
         CREATE INDEX envelopes ON coordinates(envelope,ordinal);
         CREATE TABLE reporting_envelopes(ordinal INTEGER PRIMARY KEY,envelope BLOB NOT NULL UNIQUE);
         INSERT INTO reporting_envelopes(envelope)
             SELECT envelope FROM coordinates WHERE envelope IS NOT NULL
             GROUP BY envelope ORDER BY MIN(ordinal);
         COMMIT",
    ).map_err(sql_error)
}
