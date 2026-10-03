//! Bounded demand pages over original selected evidence.
use super::*;

pub(super) fn page(
    snapshot: &Snapshot,
    position: &Cursor,
    limit: usize,
    limits: ServiceLimits,
    authorized: &[bool],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<InvestmentFinancialResult, ServiceError> {
    check(deadline, cancellation)?;
    let connection = Connection::open_with_flags(&snapshot.index, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sql_error)?;
    connection
        .execute_batch("PRAGMA mmap_size=0; PRAGMA cache_size=-1024")
        .map_err(sql_error)?;
    let grouped = matches!(
        snapshot.section,
        InvestmentFinancialSection::Statements | InvestmentFinancialSection::Ratios
    );
    let mut unit = position.unit;
    let mut offset = position.item;
    let mut result = InvestmentFinancialResult {
        selection_token: snapshot.selection_token.clone(),
        section: snapshot.section,
        knowledge_at: Some(timestamp_text(snapshot.request.knowledge_at())),
        effective_on: Some(snapshot.effective_on.clone()),
        revision_policy: "latestKnown",
        state: empty_state(&snapshot.families),
        families: snapshot.families.clone(),
        items: Vec::new(),
        current_cursor: Some(cursor(position.read_token, unit, offset)?),
        next_cursor: None,
        read_token: Some(position.read_token.to_string()),
        omitted_items: 0,
        limitations: Vec::new(),
    };
    for (selected, allowed) in snapshot.selections.iter().zip(authorized) {
        if !allowed {
            if let Some(family) = result
                .families
                .iter_mut()
                .find(|family| family.family == family_name(selected.request().family()))
            {
                family.state = InvestmentFinancialState::Unavailable;
                family.reason = Some("rights_unavailable");
            }
        }
    }
    result.state = empty_state(&result.families);
    if snapshot.omitted_facts != 0 {
        result.limitations.push("some_reported_facts_not_supported")
    }
    'units: loop {
        check(deadline, cancellation)?;
        let items = if grouped {
            envelope_items(
                snapshot,
                &connection,
                unit,
                authorized,
                deadline,
                cancellation,
            )?
        } else {
            row_items(snapshot, &connection, unit, authorized)?
        };
        let Some(items) = items else {
            if offset != 0 {
                return Err(ServiceError::InvalidRequest);
            }
            break;
        };
        if !items.is_empty() && offset > items.len() {
            return Err(ServiceError::InvalidRequest);
        }
        for item in items.into_iter().skip(offset) {
            check(deadline, cancellation)?;
            if result.items.len() == limit {
                result.next_cursor = Some(cursor(position.read_token, unit, offset)?);
                break 'units;
            }
            result.items.push(item);
            // Admit the complete shared transport envelope, including its metadata, JSON
            // structure and a worst-width continuation. No corpus-sized response is built.
            result.next_cursor = Some(cursor(position.read_token, u64::MAX, usize::MAX)?);
            let previous_state = result.state;
            result.state = InvestmentFinancialState::Unavailable;
            let fits = TypedToolResult::try_new(
                serde_json::to_value(&result).map_err(|_| ServiceError::InvalidResult)?,
                result.items.len(),
                ToolResultMetadata::complete_not_applicable(),
                limits,
            )
            .is_ok();
            result.state = previous_state;
            if !fits {
                result.items.pop();
                if !result.items.is_empty() {
                    result.next_cursor = Some(cursor(position.read_token, unit, offset)?);
                    break 'units;
                }
                result.omitted_items += 1;
                if !result.limitations.contains(&"item_exceeds_response_limit") {
                    result.limitations.push("item_exceeds_response_limit")
                }
            }
            offset += 1;
            result.next_cursor = None;
        }
        unit = unit.checked_add(1).ok_or(ServiceError::ResourceExhausted)?;
        offset = 0;
    }
    if !result.items.is_empty() {
        result.state = if snapshot.section == InvestmentFinancialSection::Ratios
            && !result
                .items
                .iter()
                .any(|item| item.get("state").and_then(Value::as_str) == Some("reported"))
        {
            InvestmentFinancialState::Unavailable
        } else {
            InvestmentFinancialState::Reported
        };
    } else if result.omitted_items != 0
        || result
            .families
            .iter()
            .any(|family| family.state == InvestmentFinancialState::Reported)
    {
        result.state = InvestmentFinancialState::Unavailable;
    }
    check(deadline, cancellation)?;
    TypedToolResult::try_new(
        serde_json::to_value(&result).map_err(|_| ServiceError::InvalidResult)?,
        result.items.len(),
        ToolResultMetadata::complete_not_applicable(),
        limits,
    )
    .map_err(ServiceError::from)?;
    Ok(result)
}

fn row_coordinate(
    connection: &Connection,
    ordinal: u64,
) -> Result<Option<(i64, i64)>, ServiceError> {
    connection
        .query_row(
            "SELECT family,position FROM coordinates WHERE ordinal=?1",
            [i64::try_from(ordinal).map_err(|_| ServiceError::InvalidRequest)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)
}

fn row_items(
    snapshot: &Snapshot,
    connection: &Connection,
    ordinal: u64,
    authorized: &[bool],
) -> Result<Option<Vec<Value>>, ServiceError> {
    row_coordinate(connection, ordinal)?
        .map(|(family, position)| {
            let family = checked_coordinate(family)?;
            let position = checked_coordinate(position)?;
            if !authorized
                .get(family)
                .copied()
                .ok_or(ServiceError::InvalidResult)?
            {
                return Ok(Vec::new());
            }
            let exact = exact_family(snapshot, family)?;
            let (fact, filing) = selected_company_row(&snapshot.request, exact, position)
                .map_err(canonical_error)?;
            let item = if let Some(fact) = fact {
                serde_json::to_value(
                    project_fact(&fact, snapshot.request.knowledge_at())
                        .map_err(projection_error)?
                        .ok_or(ServiceError::InvalidResult)?,
                )
            } else {
                serde_json::to_value(
                    project_filing(
                        &filing.ok_or(ServiceError::InvalidResult)?,
                        snapshot.request.knowledge_at(),
                    )
                    .map_err(projection_error)?,
                )
            }
            .map_err(|_| ServiceError::InvalidResult)?;
            Ok(vec![item])
        })
        .transpose()
}
fn envelope_items(
    snapshot: &Snapshot,
    connection: &Connection,
    ordinal: u64,
    authorized: &[bool],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<Vec<Value>>, ServiceError> {
    let envelope: Option<Vec<u8>> = connection
        .query_row(
            "SELECT envelope FROM reporting_envelopes WHERE ordinal=?1",
            [i64::try_from(ordinal)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or(ServiceError::InvalidRequest)?],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(envelope) = envelope else {
        return Ok(None);
    };
    let mut statement = connection
        .prepare("SELECT family,position FROM coordinates WHERE envelope=?1 ORDER BY ordinal")
        .map_err(sql_error)?;
    let mut rows = statement.query([&envelope]).map_err(sql_error)?;
    let mut facts = Vec::new();
    while let Some(row) = rows.next().map_err(sql_error)? {
        check(deadline, cancellation)?;
        let family = checked_coordinate(row.get::<_, i64>(0).map_err(sql_error)?)?;
        if !authorized
            .get(family)
            .copied()
            .ok_or(ServiceError::InvalidResult)?
        {
            return Ok(Some(Vec::new()));
        }
        let exact = exact_family(snapshot, family)?;
        let position = checked_coordinate(row.get::<_, i64>(1).map_err(sql_error)?)?;
        let (fact, _) =
            selected_company_row(&snapshot.request, exact, position).map_err(canonical_error)?;
        let mut fact = fact.ok_or(ServiceError::InvalidResult)?;
        if snapshot.section == InvestmentFinancialSection::Ratios {
            if let Some((context, _)) = fact.lineage().xbrl_identity() {
                let inputs: Option<u16> = connection
                    .query_row(
                        "SELECT inputs FROM nonnumeric_inputs WHERE family=?1 AND context=?2",
                        params![family as i64, context.as_str()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(sql_error)?;
                fact.set_nonnumeric_inputs(inputs.unwrap_or_default());
            }
        }
        let fact = project_fact(&fact, snapshot.request.knowledge_at())
            .map_err(projection_error)?
            .ok_or(ServiceError::InvalidResult)?;
        if fact_envelope_bytes(&fact).map_err(projection_error)? != envelope {
            return Err(ServiceError::InvalidResult);
        }
        facts
            .try_reserve(1)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        facts.push(fact);
    }
    project_financial_envelope(
        &facts,
        snapshot.section == InvestmentFinancialSection::Ratios,
    )
    .map(Some)
    .map_err(projection_error)
}
fn checked_coordinate(value: i64) -> Result<usize, ServiceError> {
    usize::try_from(value).map_err(|_| ServiceError::InvalidResult)
}

fn exact_family(
    snapshot: &Snapshot,
    family: usize,
) -> Result<&market_squawk_data::SecResearchSelection, ServiceError> {
    match snapshot
        .selections
        .get(family)
        .map(SecResearchIdentitySelection::outcome)
    {
        Some(SecResearchIdentityOutcome::Exact(exact))
            if exact.disposition() == SecResearchDisposition::Selected =>
        {
            Ok(exact)
        }
        _ => Err(ServiceError::InvalidResult),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_data::{CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig};
    use market_squawk_domain::{CalendarDate, InstrumentId};
    use market_squawk_platform::LocalPaths;
    use market_squawk_services::JsonStructureLimits;

    /// The missing-evidence path is a real frozen snapshot too: changing section/selection must
    /// not reuse its handle, and closing it must not silently open a different current read.
    #[tokio::test]
    async fn selected_page_binds_cursor_and_close_to_original_snapshot() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("financial-pages"))?;
        let catalog = CatalogConfig::try_new(
            paths.catalog()?.clone(),
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?;
        let objects = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
        let research = Arc::new(ResearchService::initialize(&paths, catalog, 8, objects)?);
        let capability = InvestmentFinancialReadCapability::new(
            Arc::clone(&research),
            MarketProductSelectionReadCapability::new(
                Arc::clone(&research),
                research.market_data_instruments(),
            ),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        let cancellation = CancellationToken::new();
        let cutoff = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
        let request = CompanyResearchRequest::try_new(
            InstrumentId::try_from(Uuid::new_v4())?,
            cutoff,
            ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 10, 2)?),
            ResearchRevisionPolicy::LatestKnown,
        )?;
        let id = Uuid::new_v4();
        let snapshot = Arc::new(build_snapshot(
            request,
            "selected-security".into(),
            InvestmentFinancialSection::Facts,
            "2026-10-02".into(),
            vec![FamilyAvailability {
                family: "company_facts",
                state: InvestmentFinancialState::Missing,
                reason: Some("identity_missing"),
            }],
            Vec::new(),
            Vec::new(),
            research.analytical().operation_scratch()?,
            deadline,
            &cancellation,
        )?);
        capability
            .cache
            .entries
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?
            .insert(
                id,
                IdleRead {
                    snapshot: Arc::clone(&snapshot),
                    touched: Instant::now(),
                },
            );
        let cursor = cursor(id, 0, 0)?;
        let limits = ServiceLimits::try_new(
            16 * 1024,
            10,
            1024 * 1024,
            100,
            JsonStructureLimits::try_new(32, 128 * 1024, 4096, 4096)?,
        )?;
        let page = capability
            .read(
                "selected-security",
                InvestmentFinancialSection::Facts,
                Some(&cursor),
                1,
                limits,
                deadline,
                &cancellation,
            )
            .await?;
        assert_eq!(page.state, InvestmentFinancialState::Missing);
        assert_eq!(
            page.knowledge_at.as_deref(),
            Some(timestamp_text(cutoff).as_str())
        );
        assert_eq!(page.current_cursor.as_deref(), Some(cursor.as_str()));
        assert!(page.items.is_empty() && page.next_cursor.is_none());
        assert!(matches!(
            capability
                .read(
                    "other-security",
                    InvestmentFinancialSection::Facts,
                    Some(&cursor),
                    1,
                    limits,
                    deadline,
                    &cancellation
                )
                .await,
            Err(ServiceError::InvalidRequest)
        ));
        assert!(matches!(
            capability
                .read(
                    "selected-security",
                    InvestmentFinancialSection::Ratios,
                    Some(&cursor),
                    1,
                    limits,
                    deadline,
                    &cancellation
                )
                .await,
            Err(ServiceError::InvalidRequest)
        ));
        assert!(capability.close("selected-security", &id.to_string())?);
        let expired = capability
            .read(
                "selected-security",
                InvestmentFinancialSection::Facts,
                Some(&cursor),
                1,
                limits,
                deadline,
                &cancellation,
            )
            .await?;
        assert_eq!(expired.state, InvestmentFinancialState::Expired);
        assert!(expired.knowledge_at.is_none() && expired.read_token.is_none());
        // The admitted page's original ownership remains usable after the cache closes it.
        let owned = super::page(
            &snapshot,
            &Cursor {
                read_token: id,
                unit: 0,
                item: 0,
            },
            1,
            limits,
            &[],
            deadline,
            &cancellation,
        )?;
        assert_eq!(owned.knowledge_at, page.knowledge_at);
        // The same disk snapshot assigns recent-first display ordinals while retaining
        // every original family/position and every complete envelope across page boundaries.
        let ordered = Connection::open(directory.path().join("display-order.sqlite"))?;
        super::super::snapshot::begin_coordinates(&ordered)?;
        let old = i64::from(CalendarDate::new(1994, 12, 31)?.days_since_unix_epoch());
        let recent = i64::from(CalendarDate::new(2026, 6, 30)?.days_since_unix_epoch());
        for (ordinal, family, position, envelope, day, published) in [
            (0, 0, 12, Some(b"older".as_slice()), old, Some(old)),
            (1, 1, 22, None, recent, None),
            (
                2,
                0,
                34,
                Some(b"recent".as_slice()),
                recent,
                Some(recent + 1),
            ),
            (
                3,
                0,
                35,
                Some(b"recent".as_slice()),
                recent,
                Some(recent + 1),
            ),
            (4, 1, 45, None, recent, Some(recent + 2)),
        ] {
            ordered.execute(
                "INSERT INTO source_coordinates(ordinal,family,position,envelope,effective_day,published_day) VALUES(?1,?2,?3,?4,?5,?6)",
                params![ordinal, family, position, envelope, day, published],
            )?;
        }
        super::super::snapshot::finish_coordinates(&ordered)?;
        let expected = [(1, 45), (0, 34), (0, 35), (1, 22), (0, 12)];
        for (unit, expected) in expected.into_iter().enumerate() {
            let encoded = super::cursor(id, unit as u64, 0)?;
            let position: Cursor = serde_json::from_str(&encoded)?;
            assert_eq!(position.read_token, id);
            assert_eq!(row_coordinate(&ordered, position.unit)?, Some(expected));
        }
        assert_eq!(row_coordinate(&ordered, 5)?, None);
        // Previous visits the identical coordinate, including tied report dates.
        assert_eq!(row_coordinate(&ordered, 1)?, Some((0, 34)));
        let envelopes: Vec<Vec<u8>> = ordered
            .prepare("SELECT envelope FROM reporting_envelopes ORDER BY ordinal")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        assert_eq!(envelopes, vec![b"recent".to_vec(), b"older".to_vec()]);
        let recent_inputs: i64 = ordered.query_row(
            "SELECT COUNT(*) FROM coordinates WHERE envelope=?1",
            [b"recent".as_slice()],
            |row| row.get(0),
        )?;
        assert_eq!(recent_inputs, 2);
        // A coordinate cannot manufacture rows without its original source selection receipt.
        let connection = Connection::open(&snapshot.index)?;
        connection.execute(
            "INSERT INTO coordinates(ordinal,family,position,envelope) VALUES(0,0,0,NULL)",
            [],
        )?;
        assert!(matches!(
            super::page(
                &snapshot,
                &Cursor {
                    read_token: id,
                    unit: 0,
                    item: 0
                },
                1,
                limits,
                &[true],
                deadline,
                &cancellation
            ),
            Err(ServiceError::InvalidResult)
        ));
        Ok(())
    }
}
