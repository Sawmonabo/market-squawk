//! Guided source choices over a complete disk index. Listing never prepares financial outputs.
//!
//! The catalog commitment binds immutable inputs, full-cohort recipe coordinates and currently
//! admitted source uses. Macro completeness, exact PIT selection and publication authority belong
//! to the selected preview and its build-bound one-use receipt.

use super::*;
use arrow::array::{Array as _, StringArray, TimestampNanosecondArray};
use market_squawk_data::{OperationScratchDirectory, ResearchUseRequest};
use rusqlite::{Connection, OptionalExtension as _, params};
use std::collections::BTreeSet;

pub(super) struct CatalogOption {
    pub(super) summary: DatasetPreparationOption,
    series: Sha256Digest,
    generation: usize,
    annual: bool,
}

pub(super) struct Catalog {
    pub(super) options: Vec<CatalogOption>,
    pub(super) digest: Sha256Digest,
    index: Index,
}

struct Index {
    connection: Mutex<Connection>,
    // SQLite closes before its private, restart-reclaimable operation directory disappears.
    _directory: OperationScratchDirectory,
    generations: Vec<AnalyticalGeneration>,
    snapshot_as_of: Option<Timestamp>,
}

struct Recipe {
    coordinates: Vec<[usize; 3]>,
    split_counts: [usize; 3],
    identity: Sha256Digest,
    label: &'static str,
    observed_points: usize,
    from: Timestamp,
    through: Timestamp,
}

fn sql_index(value: usize) -> Result<i64, DatasetPreparationError> {
    i64::try_from(value).map_err(|_| DatasetPreparationError::Capacity)
}

fn read_index(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<usize> {
    let value: i64 = row.get(column)?;
    usize::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn index_error(_: rusqlite::Error) -> DatasetPreparationError {
    DatasetPreparationError::Unavailable
}

pub(super) async fn read(
    authority: &DatasetPreparationAuthority,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<Catalog, DatasetPreparationError> {
    check_control(deadline, &cancellation)?;
    let reader = authority.reader.clone();
    let analytical = authority.research.analytical_service();
    let directory = analytical
        .operation_scratch()
        .map_err(|_| DatasetPreparationError::Unavailable)?;
    let runtime =
        tokio::runtime::Handle::try_current().map_err(|_| DatasetPreparationError::Unavailable)?;
    authority
        .research
        .run_owned_research_io(deadline, &cancellation, move |worker_cancellation| {
            let index = runtime.block_on(Index::read(
                reader,
                directory,
                deadline,
                &worker_cancellation,
            ))?;
            index.catalog(&analytical, deadline, &worker_cancellation)
        })
        .await
        .map_err(|error| preparation_worker_error("catalog_worker", error))?
}

impl Index {
    async fn read(
        reader: AnalyticalReadCapability,
        directory: OperationScratchDirectory,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, DatasetPreparationError> {
        let connection =
            Connection::open(directory.path().join("preparation.sqlite")).map_err(index_error)?;
        let sql_cancellation = cancellation.clone();
        connection
            .progress_handler(
                1024,
                Some(move || sql_cancellation.is_cancelled() || Instant::now() >= deadline),
            )
            .map_err(index_error)?;
        connection.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE;
             PRAGMA cache_size=-2048; PRAGMA mmap_size=0;
             CREATE TABLE bars(series BLOB NOT NULL, generation INTEGER NOT NULL,
                effective INTEGER NOT NULL, available INTEGER NOT NULL, revision INTEGER NOT NULL,
                identifier TEXT NOT NULL, digest BLOB NOT NULL, payload BLOB NOT NULL,
                PRIMARY KEY(series,effective)) WITHOUT ROWID;
             CREATE TABLE support(generation INTEGER NOT NULL, kind INTEGER NOT NULL,
                instrument TEXT NOT NULL, source TEXT NOT NULL, identifier TEXT NOT NULL,
                revision INTEGER NOT NULL, digest BLOB NOT NULL, payload BLOB NOT NULL);
             CREATE INDEX support_order ON support(instrument,kind,generation,source,identifier,revision,digest);"
        ).map_err(index_error)?;
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        let limit = AnalyticalReadLimit::try_new(MAXIMUM_GENERATIONS)
            .map_err(|_| DatasetPreparationError::Capacity)?;
        let mut generations = Vec::new();
        let mut snapshot_as_of: Option<Timestamp> = None;
        let mut after = None;
        connection.execute_batch("BEGIN").map_err(index_error)?;
        loop {
            check_control(deadline, cancellation)?;
            let page = reader
                .datasets(after.as_ref(), limit, deadline, cancellation)
                .map_err(|error| preparation_read_error("catalog_read", error))?;
            for generation in page.generations() {
                if generation.manifest().schema() != &canonical {
                    continue;
                }
                let generation_index = generations.len();
                generations.push(generation.clone());
                // Canonical summary columns were validated before immutable publication. Read
                // their compact projection to account for the complete snapshot without decoding
                // unrelated financial payloads and their extraction lineages.
                let relevant = summarize_generation(
                    &reader,
                    generation.manifest(),
                    &mut snapshot_as_of,
                    deadline,
                    cancellation,
                )
                .await?;
                if !relevant {
                    continue;
                }
                // One row is the decode working unit. A 128-row Arrow slice retains all original
                // array buffers and therefore cannot make the decoder's admission row-bounded.
                let mut cursor = reader
                    .observation_batch_cursor(
                        generation.manifest(),
                        0,
                        0,
                        None,
                        1,
                        MAXIMUM_QUERY_BYTES * 2,
                        deadline,
                        cancellation,
                    )
                    .map_err(|error| preparation_read_error("catalog_cursor", error))?;
                loop {
                    check_control(deadline, cancellation)?;
                    let batch = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        cursor.next_batch(),
                    )
                    .await
                    .map_err(|_| DatasetPreparationError::Cancelled)?
                    .map_err(|error| preparation_read_error("catalog_batch", error.into()))?;
                    let Some(batch) = batch else {
                        break;
                    };
                    if !relevant_row(&batch, 0)? {
                        continue;
                    }
                    let (observations, _) = ResearchArrowBatch::decode_query_projection_bounded(
                        batch.clone(),
                        MAXIMUM_QUERY_BYTES,
                    )
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
                    let digests = batch
                        .column_by_name("payload_sha256")
                        .and_then(|column| {
                            column.as_any().downcast_ref::<arrow::array::BinaryArray>()
                        })
                        .ok_or(DatasetPreparationError::InvalidEvidence)?;
                    for (row, observation) in observations.into_iter().enumerate() {
                        let digest: [u8; 32] = digests
                            .value(row)
                            .try_into()
                            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
                        let provenance = observation_context(&observation).provenance();
                        let retained_at =
                            provenance.ingested_at().max(provenance.received_at()).max(
                                provenance
                                    .availability()
                                    .conservative_available_at()
                                    .unwrap_or(provenance.ingested_at()),
                            );
                        snapshot_as_of =
                            Some(snapshot_as_of.map_or(retained_at, |at| at.max(retained_at)));
                        match &observation {
                            ResearchObservation::MarketBar(bar) => {
                                if let Some(key) = series_key(bar)? {
                                    let identity =
                                        market_series_identity(generation.manifest(), &key);
                                    let payload = serde_json::to_vec(bar)
                                        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
                                    connection.execute(
                                        "INSERT INTO bars VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
                                         ON CONFLICT(series,effective) DO UPDATE SET
                                         available=excluded.available, revision=excluded.revision,
                                         identifier=excluded.identifier,digest=excluded.digest,payload=excluded.payload
                                         WHERE (excluded.available,excluded.revision,excluded.identifier,excluded.digest)
                                             >(bars.available,bars.revision,bars.identifier,bars.digest)",
                                        params![identity.bytes().as_slice(), sql_index(generation_index)?,
                                            bar.completed_at().ok_or(DatasetPreparationError::InvalidEvidence)?.unix_nanos(),
                                            provenance.availability().conservative_available_at()
                                                .ok_or(DatasetPreparationError::InvalidEvidence)?.unix_nanos(),
                                            bar.context().time().revision().get(),
                                            provenance.source_identifier().as_str(),digest.as_slice(),payload],
                                    ).map_err(index_error)?;
                                }
                            }
                            ResearchObservation::UniverseMembership(_)
                            | ResearchObservation::CorporateAction(_) => {
                                if let Some(instrument) = provenance.instrument_id() {
                                    let kind = i64::from(matches!(
                                        observation,
                                        ResearchObservation::CorporateAction(_)
                                    ));
                                    let payload = serde_json::to_vec(&observation)
                                        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
                                    connection
                                        .execute(
                                            "INSERT INTO support VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                                            params![
                                                sql_index(generation_index)?,
                                                kind,
                                                instrument.to_string(),
                                                provenance.source_id().as_str(),
                                                provenance.source_identifier().as_str(),
                                                observation_context(&observation)
                                                    .time()
                                                    .revision()
                                                    .get(),
                                                digest.as_slice(),
                                                payload
                                            ],
                                        )
                                        .map_err(index_error)?;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            if !page.has_more() {
                break;
            }
            let next = page
                .generations()
                .last()
                .ok_or(DatasetPreparationError::InvalidEvidence)?
                .manifest()
                .dataset_id()
                .clone();
            if after
                .as_ref()
                .is_some_and(|prior| prior.as_str() >= next.as_str())
            {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
            after = Some(next);
        }
        check_control(deadline, cancellation)?;
        connection
            .execute_batch(
                "COMMIT;
                 CREATE TABLE points AS SELECT series,generation,
                ROW_NUMBER() OVER(PARTITION BY series ORDER BY effective)-1 AS ordinal,
                effective,available,payload FROM bars;
             CREATE UNIQUE INDEX point_ordinal ON points(series,ordinal);
             CREATE UNIQUE INDEX point_effective ON points(series,effective);
             DROP TABLE bars;
             CREATE TABLE coordinates(ordinal INTEGER PRIMARY KEY,prior INTEGER NOT NULL,
                current INTEGER NOT NULL,terminal INTEGER NOT NULL,
                origin INTEGER NOT NULL,label INTEGER NOT NULL,partition INTEGER);",
            )
            .map_err(index_error)?;
        check_control(deadline, cancellation)?;
        Ok(Self {
            connection: Mutex::new(connection),
            _directory: directory,
            generations,
            snapshot_as_of,
        })
    }

    fn catalog(
        self,
        analytical: &market_squawk_data::AnalyticalDataService,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Catalog, DatasetPreparationError> {
        let mut options = Vec::new();
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/guided-source-recipe-catalog/v1\0");
        digest.update((self.generations.len() as u64).to_be_bytes());
        for generation in &self.generations {
            hash_manifest(&mut digest, generation.manifest());
        }
        let mut after: Vec<u8> = Vec::new();
        loop {
            check_control(deadline, cancellation)?;
            let connection = self
                .connection
                .lock()
                .map_err(|_| DatasetPreparationError::Unavailable)?;
            let next: Option<(Vec<u8>, usize, Vec<u8>)> = connection.query_row(
                "SELECT series,generation,payload FROM points WHERE series>?1 ORDER BY series,ordinal LIMIT 1",
                [&after], |row| Ok((row.get(0)?,read_index(row, 1)?,row.get(2)?)),
            ).optional().map_err(index_error)?;
            let Some((series, generation, payload)) = next else {
                break;
            };
            after = series.clone();
            let series = Sha256Digest::new(
                series
                    .try_into()
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
            );
            let bar: MarketBarObservation = serde_json::from_slice(&payload)
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            let key = series_key(&bar)?.ok_or(DatasetPreparationError::InvalidEvidence)?;
            // Revisions are already canonicalized. Preserve the original monotonic availability
            // requirement over the entire series before choosing any recipe coordinates.
            let invalid: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM points p JOIN points q
                   ON q.series=p.series AND q.ordinal=p.ordinal+1
                   WHERE p.series=?1 AND q.available<=p.available)",
                    [series.bytes().as_slice()],
                    |row| row.get(0),
                )
                .map_err(index_error)?;
            drop(connection);
            if invalid {
                continue;
            }
            for annual in [false, true] {
                check_control(deadline, cancellation)?;
                let Some(recipe) = self.recipe(series, annual, deadline, cancellation)? else {
                    continue;
                };
                let Some(membership) =
                    self.membership(key.instrument_id, recipe.from, recipe.through, cancellation)?
                else {
                    continue;
                };
                let mut parents = self.action_parents(key.instrument_id)?;
                push_parent(&mut parents, self.generations[generation].manifest())?;
                push_parent(&mut parents, &membership.manifest)?;
                let limits = ResearchUseLimits::try_new(
                    parents.len(),
                    16_384,
                    65_536,
                    4_096,
                    64 * 1024 * 1024,
                    Duration::from_secs(30),
                    Duration::from_secs(5 * 60),
                )
                .map_err(|_| DatasetPreparationError::Capacity)?;
                let mut available_uses = Vec::new();
                let mut admitted_graphs = Vec::new();
                for use_case in [
                    DatasetPreparationUse::LocalAnalysis,
                    DatasetPreparationUse::Train,
                ] {
                    check_control(deadline, cancellation)?;
                    let request =
                        ResearchUseRequest::try_new(parents.clone(), use_case.domain(), limits)
                            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
                    match analytical
                        .dataset_builder()
                        .preflight_research_use(request, cancellation)
                    {
                        Ok(receipt) => {
                            available_uses.push(use_case);
                            admitted_graphs.push(receipt.graph_digest().bytes());
                        }
                        Err(market_squawk_data::DatasetBuildError::ResearchUse(
                            market_squawk_data::ResearchUseCatalogError::Denied { .. },
                        )) => {}
                        Err(_) => {
                            return Err(check_control(deadline, cancellation)
                                .err()
                                .unwrap_or(DatasetPreparationError::Unavailable));
                        }
                    }
                    check_control(deadline, cancellation)?;
                }
                if available_uses.is_empty() {
                    continue;
                }
                let summary = DatasetPreparationOption {
                    id: format!("market-research-{}", short_hex(recipe.identity)),
                    label: recipe.label.to_owned(),
                    source_dataset: "Canonical market history".to_owned(),
                    immutable_generation: self.generations[generation]
                        .manifest()
                        .manifest_version(),
                    instrument_id: key.instrument_id,
                    observed_points: recipe.observed_points,
                    examples: recipe.coordinates.len(),
                    observed_from: recipe.from,
                    observed_through: recipe.through,
                    available_uses,
                };
                update_text(&mut digest, &summary.id);
                hash_manifest(&mut digest, &membership.manifest);
                digest.update(membership.content.bytes());
                digest.update((recipe.coordinates.len() as u64).to_be_bytes());
                for coordinate in &recipe.coordinates {
                    for index in coordinate {
                        digest.update((*index as u64).to_be_bytes());
                    }
                }
                for count in recipe.split_counts {
                    digest.update((count as u64).to_be_bytes());
                }
                for (use_case, graph) in summary.available_uses.iter().zip(admitted_graphs) {
                    digest.update([use_case.tag()]);
                    digest.update(graph);
                }
                options.push(CatalogOption {
                    summary,
                    series,
                    generation,
                    annual,
                });
            }
        }
        options.sort_unstable_by(|left, right| left.summary.id.cmp(&right.summary.id));
        if options
            .windows(2)
            .any(|pair| pair[0].summary.id == pair[1].summary.id)
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        check_control(deadline, cancellation)?;
        digest.update((options.len() as u64).to_be_bytes());
        Ok(Catalog {
            options,
            digest: Sha256Digest::new(digest.finalize().into()),
            index: self,
        })
    }

    fn recipe(
        &self,
        series: Sha256Digest,
        annual: bool,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Recipe>, DatasetPreparationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        recipe_from_index(&connection, series, annual, deadline, cancellation)
    }

    fn membership(
        &self,
        instrument: InstrumentId,
        from: Timestamp,
        through: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Option<MembershipSelection>, DatasetPreparationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        let mut statement=connection.prepare("SELECT generation,payload FROM support WHERE instrument=?1 AND kind=0 ORDER BY generation,source,identifier,revision,digest").map_err(index_error)?;
        let mut rows = statement
            .query([instrument.to_string()])
            .map_err(index_error)?;
        while let Some(row) = rows.next().map_err(index_error)? {
            if cancellation.is_cancelled() {
                return Err(DatasetPreparationError::Cancelled);
            }
            let generation = read_index(row, 0).map_err(index_error)?;
            let payload: Vec<u8> = row.get(1).map_err(index_error)?;
            let ResearchObservation::UniverseMembership(observation) =
                serde_json::from_slice(&payload)
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?
            else {
                return Err(DatasetPreparationError::InvalidEvidence);
            };
            let retained = CanonicalMembership {
                observation,
                manifest: self.generations[generation].manifest().clone(),
            };
            if let Some(selected) =
                membership_evidence(&[retained], instrument, from, through, cancellation)?
            {
                return Ok(Some(selected));
            }
        }
        Ok(None)
    }

    fn action_parents(
        &self,
        instrument: InstrumentId,
    ) -> Result<Vec<DatasetManifestRef>, DatasetPreparationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        let mut statement=connection.prepare("SELECT DISTINCT generation FROM support WHERE instrument=?1 AND kind=1 ORDER BY generation").map_err(index_error)?;
        let generations = statement
            .query_map([instrument.to_string()], |row| read_index(row, 0))
            .map_err(index_error)?;
        generations
            .map(|generation| {
                generation
                    .map(|generation| self.generations[generation].manifest().clone())
                    .map_err(index_error)
            })
            .collect()
    }

    fn support(
        &self,
        instrument: InstrumentId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CanonicalSupport, DatasetPreparationError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        let mut statement=connection.prepare("SELECT generation,payload FROM support WHERE instrument=?1 ORDER BY generation,source,identifier,revision,digest").map_err(index_error)?;
        let mut rows = statement
            .query([instrument.to_string()])
            .map_err(index_error)?;
        let mut memberships = Vec::new();
        let mut actions = Vec::new();
        while let Some(row) = rows.next().map_err(index_error)? {
            check_control(deadline, cancellation)?;
            let generation = read_index(row, 0).map_err(index_error)?;
            let payload: Vec<u8> = row.get(1).map_err(index_error)?;
            let observation: ResearchObservation = serde_json::from_slice(&payload)
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            let manifest = self.generations[generation].manifest().clone();
            match observation {
                ResearchObservation::UniverseMembership(observation) => {
                    memberships.push(CanonicalMembership {
                        observation,
                        manifest,
                    })
                }
                ResearchObservation::CorporateAction(_) => {
                    actions.push(PointInTimeCandidate::new(observation, manifest))
                }
                _ => return Err(DatasetPreparationError::InvalidEvidence),
            }
        }
        Ok(CanonicalSupport {
            snapshot_as_of: self.snapshot_as_of,
            memberships: memberships.into_boxed_slice(),
            actions: actions.into_boxed_slice(),
        })
    }
}

impl Catalog {
    pub(super) async fn prepare(
        &self,
        authority: &DatasetPreparationAuthority,
        id: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedOption, DatasetPreparationError> {
        let selected = self
            .options
            .iter()
            .find(|option| option.summary.id == id)
            .ok_or(DatasetPreparationError::InvalidSelection)?;
        let recipe = self
            .index
            .recipe(selected.series, selected.annual, deadline, cancellation)?
            .ok_or(DatasetPreparationError::StaleCatalog)?;
        let selected_ordinals = recipe
            .coordinates
            .iter()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut points = Vec::with_capacity(selected_ordinals.len());
        let mut remap = BTreeMap::new();
        {
            let connection = self
                .index
                .connection
                .lock()
                .map_err(|_| DatasetPreparationError::Unavailable)?;
            let mut statement = connection
                .prepare("SELECT payload FROM points WHERE series=?1 AND ordinal=?2")
                .map_err(index_error)?;
            for ordinal in selected_ordinals {
                check_control(deadline, cancellation)?;
                let payload: Vec<u8> = statement
                    .query_row(
                        params![selected.series.bytes().as_slice(), sql_index(ordinal)?],
                        |row| row.get(0),
                    )
                    .map_err(index_error)?;
                let observation: MarketBarObservation = serde_json::from_slice(&payload)
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
                remap.insert(ordinal, points.len());
                points.push(MarketSeriesPoint {
                    effective: observation
                        .completed_at()
                        .ok_or(DatasetPreparationError::InvalidEvidence)?,
                    available_at: observation
                        .context()
                        .provenance()
                        .availability()
                        .conservative_available_at()
                        .ok_or(DatasetPreparationError::InvalidEvidence)?,
                    manifest: self.index.generations[selected.generation]
                        .manifest()
                        .clone(),
                    session_evidence: observation
                        .time_semantics()
                        .session()
                        .ok_or(DatasetPreparationError::InvalidEvidence)?
                        .evidence(),
                    observation,
                });
            }
        }
        let key = series_key(
            &points
                .first()
                .ok_or(DatasetPreparationError::InvalidEvidence)?
                .observation,
        )?
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let coordinates = recipe
            .coordinates
            .iter()
            .map(|coordinate| {
                let mut mapped = [0; 3];
                for (index, original) in coordinate.iter().enumerate() {
                    mapped[index] = *remap
                        .get(original)
                        .ok_or(DatasetPreparationError::InvalidEvidence)?;
                }
                Ok(mapped)
            })
            .collect::<Result<Vec<_>, DatasetPreparationError>>()?;
        let recipe = DatasetRecipeCoordinates {
            identity: recipe.identity,
            label: recipe.label,
            coordinates,
            split_counts: recipe.split_counts,
            observed_points: recipe.observed_points,
        };
        let support = self
            .index
            .support(key.instrument_id, deadline, cancellation)?;
        build_option(
            authority,
            &self.index.generations[selected.generation],
            &support,
            &mut BTreeMap::new(),
            key,
            recipe,
            &points,
            deadline,
            cancellation,
        )
        .await?
        .ok_or(DatasetPreparationError::InvalidSelection)
    }
}

fn series_key(
    value: &MarketBarObservation,
) -> Result<Option<MarketSeriesKey>, DatasetPreparationError> {
    if value.adjustment() != MarketBarAdjustment::Raw {
        return Ok(None);
    }
    let provenance = value.context().provenance();
    let (Some(instrument_id), Some(venue_id), Some(effective), Some(available_at)) = (
        provenance.instrument_id(),
        provenance.venue_id(),
        value.completed_at(),
        provenance.availability().conservative_available_at(),
    ) else {
        return Ok(None);
    };
    if available_at < effective {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    Ok(Some(MarketSeriesKey {
        instrument_id,
        venue_id: venue_id.clone(),
        source_id: provenance.source_id().clone(),
        provider_instrument_id: value.provider_instrument_id().clone(),
        feed: value.feed().clone(),
        interval: value.interval().clone(),
        timestamp_basis: value
            .time_semantics()
            .timestamp_basis()
            .ok_or(DatasetPreparationError::InvalidEvidence)?,
        session: value
            .time_semantics()
            .session()
            .cloned()
            .ok_or(DatasetPreparationError::InvalidEvidence)?,
        currency: value.currency(),
    }))
}

fn recipe_from_index(
    connection: &Connection,
    series: Sha256Digest,
    annual: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<Recipe>, DatasetPreparationError> {
    check_control(deadline, cancellation)?;
    let sql_cancellation = cancellation.clone();
    connection
        .progress_handler(
            1024,
            Some(move || sql_cancellation.is_cancelled() || Instant::now() >= deadline),
        )
        .map_err(index_error)?;
    connection
        .execute("DELETE FROM coordinates", [])
        .map_err(index_error)?;
    if annual {
        let horizon = market_squawk_backtesting::RECOMMENDATION_TARGET_HORIZON_NANOS_V1;
        connection.execute(
                "INSERT INTO coordinates SELECT ROW_NUMBER() OVER(ORDER BY c.ordinal)-1,
                    c.ordinal-1,c.ordinal,t.ordinal,c.available,t.available,NULL
                 FROM points c JOIN points t ON t.series=c.series AND t.effective=c.effective+?2
                 WHERE c.series=?1 AND c.ordinal>0 AND c.effective<=?3
                    AND t.ordinal>c.ordinal AND c.available<t.effective AND c.available<t.available",
                params![series.bytes().as_slice(),horizon,i64::MAX-horizon],
            ).map_err(index_error)?;
    } else {
        let horizon: Option<i64> = connection
            .query_row(
                "SELECT t.effective-c.effective AS horizon FROM points c JOIN points t
                    ON t.series=c.series AND t.ordinal=c.ordinal+1
                 WHERE c.series=?1 AND c.ordinal%3=1
                 GROUP BY horizon ORDER BY COUNT(*) DESC,horizon LIMIT 1",
                [series.bytes().as_slice()],
                |row| row.get(0),
            )
            .optional()
            .map_err(index_error)?;
        let Some(horizon) = horizon else {
            return Ok(None);
        };
        if horizon <= 0 {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        connection
            .execute(
                "INSERT INTO coordinates SELECT ROW_NUMBER() OVER(ORDER BY c.ordinal)-1,
                    c.ordinal-1,c.ordinal,t.ordinal,c.available,t.available,NULL
                 FROM points c JOIN points t ON t.series=c.series AND t.ordinal=c.ordinal+1
                 WHERE c.series=?1 AND c.ordinal%3=1 AND t.effective-c.effective=?2",
                params![series.bytes().as_slice(), horizon],
            )
            .map_err(index_error)?;
    }
    let count: usize = connection
        .query_row("SELECT COUNT(*) FROM coordinates", [], |row| {
            read_index(row, 0)
        })
        .map_err(index_error)?;
    if count < 3 {
        return Ok(None);
    }
    if annual {
        let first: i64 = connection
            .query_row(
                "SELECT origin FROM coordinates ORDER BY ordinal LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(index_error)?;
        let last: i64 = connection
            .query_row(
                "SELECT label FROM coordinates ORDER BY ordinal DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(index_error)?;
        let span = i128::from(last) - i128::from(first);
        if span <= 0 {
            return Ok(None);
        }
        let train = i64::try_from(i128::from(first) + span / 3)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let validation = i64::try_from(i128::from(first) + 2 * span / 3)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        connection.execute(
                "UPDATE coordinates SET partition=CASE WHEN label<=?1 THEN 0
                 WHEN origin>?1 AND label<=?2 THEN 1 WHEN origin>?2 AND label<=?3 THEN 2 ELSE NULL END",
                params![train,validation,last],
            ).map_err(index_error)?;
    } else {
        connection.execute("UPDATE coordinates SET partition=CASE WHEN ordinal<?1 THEN 0 WHEN ordinal<?2 THEN 1 ELSE 2 END",
                params![sql_index(count/3)?,sql_index(2*(count/3))?]).map_err(index_error)?;
    }
    connection
        .execute_batch(
            "DROP TABLE IF EXISTS eligible;
             CREATE TABLE eligible AS SELECT prior,current,terminal,origin,label,partition,
                ROW_NUMBER() OVER(PARTITION BY partition ORDER BY ordinal)-1 AS position
             FROM coordinates WHERE partition IS NOT NULL;
             CREATE UNIQUE INDEX eligible_position ON eligible(partition,position);",
        )
        .map_err(index_error)?;
    let mut coordinates = Vec::new();
    let mut split_counts = [0; 3];
    let mut from = None;
    let mut through = None;
    for (partition, split_count) in split_counts.iter_mut().enumerate() {
        check_control(deadline, cancellation)?;
        let count: usize = connection
            .query_row(
                "SELECT COUNT(*) FROM eligible WHERE partition=?1",
                [sql_index(partition)?],
                |row| read_index(row, 0),
            )
            .map_err(index_error)?;
        if count == 0 {
            return Ok(None);
        }
        let maximum = if annual {
            MAXIMUM_EXAMPLES / 3 + usize::from(partition < MAXIMUM_EXAMPLES % 3)
        } else {
            MAXIMUM_EXAMPLES / 3
                + if partition == 2 {
                    MAXIMUM_EXAMPLES % 3
                } else {
                    0
                }
        };
        let retained = count.min(maximum);
        *split_count = retained;
        for index in 0..retained {
            // This is the existing annual recipe's value-independent time sampling, applied
            // after examining the complete intended cohort, never before horizon matching.
            let selected = if retained == 1 {
                0
            } else {
                usize::try_from((index as u128) * ((count - 1) as u128) / ((retained - 1) as u128))
                    .map_err(|_| DatasetPreparationError::Capacity)?
            };
            let (coordinate,origin,label):([usize;3],i64,i64)=connection.query_row(
                    "SELECT prior,current,terminal,origin,label FROM eligible WHERE partition=?1 AND position=?2",
                    params![sql_index(partition)?,sql_index(selected)?],|row|Ok(([read_index(row,0)?,read_index(row,1)?,read_index(row,2)?],row.get(3)?,row.get(4)?)),
                ).map_err(index_error)?;
            from.get_or_insert(Timestamp::from_unix_nanos(origin));
            through = Some(Timestamp::from_unix_nanos(label));
            coordinates.push(coordinate);
        }
    }
    let observed_points = coordinates
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .len();
    let mut identity = Sha256::new();
    identity.update(b"market-squawk/guided-full-history-sampled-recipe/v1\0");
    identity.update(series.bytes());
    identity.update([u8::from(annual)]);
    identity.update((MAXIMUM_EXAMPLES as u64).to_be_bytes());
    if annual {
        identity.update(
            market_squawk_backtesting::RECOMMENDATION_TARGET_HORIZON_NANOS_V1.to_be_bytes(),
        );
    }
    Ok(Some(Recipe {
        coordinates,
        split_counts,
        identity: Sha256Digest::new(identity.finalize().into()),
        label: if annual {
            "365-day price returns with economic context"
        } else {
            "Price returns with economic context"
        },
        observed_points,
        from: from.ok_or(DatasetPreparationError::InvalidEvidence)?,
        through: through.ok_or(DatasetPreparationError::InvalidEvidence)?,
    }))
}

const SUMMARY_COLUMNS: &[&str] = &[
    "observation_kind",
    "instrument_id",
    "effective_at",
    "received_at",
    "available_at",
    "ingested_at",
];

fn relevant_row(
    batch: &arrow::record_batch::RecordBatch,
    row: usize,
) -> Result<bool, DatasetPreparationError> {
    let strings = |name| {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or(DatasetPreparationError::InvalidEvidence)
    };
    let kinds = strings("observation_kind")?;
    let instruments = strings("instrument_id")?;
    if kinds.is_null(row) {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    if instruments.is_null(row) {
        return Ok(false);
    }
    Ok(match kinds.value(row) {
        "universe_membership" | "corporate_action" => true,
        "market_bar" => !batch
            .column_by_name("effective_at")
            .ok_or(DatasetPreparationError::InvalidEvidence)?
            .is_null(row),
        _ => false,
    })
}

async fn summarize_generation(
    reader: &AnalyticalReadCapability,
    manifest: &DatasetManifestRef,
    snapshot_as_of: &mut Option<Timestamp>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<bool, DatasetPreparationError> {
    let mut cursor = reader
        .observation_batch_cursor(
            manifest,
            0,
            0,
            Some(SUMMARY_COLUMNS),
            128,
            MAXIMUM_QUERY_BYTES * 2,
            deadline,
            cancellation,
        )
        .map_err(|error| preparation_read_error("catalog_summary", error))?;
    let mut relevant = false;
    loop {
        check_control(deadline, cancellation)?;
        let batch = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            cursor.next_batch(),
        )
        .await
        .map_err(|_| DatasetPreparationError::Cancelled)?
        .map_err(|error| preparation_read_error("catalog_summary_batch", error.into()))?;
        let Some(batch) = batch else {
            break;
        };
        let times = |name| {
            batch
                .column_by_name(name)
                .and_then(|column| column.as_any().downcast_ref::<TimestampNanosecondArray>())
                .ok_or(DatasetPreparationError::InvalidEvidence)
        };
        let received = times("received_at")?;
        let ingested = times("ingested_at")?;
        let available = times("available_at")?;
        for row in 0..batch.num_rows() {
            check_control(deadline, cancellation)?;
            if received.is_null(row) || ingested.is_null(row) {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
            let retained = received.value(row).max(ingested.value(row));
            let retained = if available.is_null(row) {
                retained
            } else {
                retained.max(available.value(row))
            };
            let retained = Timestamp::from_unix_nanos(retained);
            *snapshot_as_of = Some(snapshot_as_of.map_or(retained, |prior| prior.max(retained)));
            relevant |= relevant_row(&batch, row)?;
        }
    }
    Ok(relevant)
}

#[cfg(test)]
mod tests {
    use super::*;

    // This regression covers full-span selection instead of first-6144-point truncation.
    // It protects annual time-partition purging; decoding is exercised by service tests.
    #[test]
    fn guided_catalog_samples_full_retained_span() -> Result<(), Box<dyn std::error::Error>> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE points(series BLOB,ordinal INTEGER,effective INTEGER,available INTEGER);
             CREATE UNIQUE INDEX point_effective ON points(series,effective);
             CREATE UNIQUE INDEX point_ordinal ON points(series,ordinal);
             CREATE TABLE coordinates(ordinal INTEGER PRIMARY KEY,prior INTEGER,current INTEGER,
                terminal INTEGER,origin INTEGER,label INTEGER,partition INTEGER); BEGIN;",
        )?;
        let series = Sha256Digest::new([7; 32]);
        let day = 86_400_000_000_000_i64;
        for ordinal in 0..9000_i64 {
            connection.execute(
                "INSERT INTO points VALUES(?1,?2,?3,?4)",
                params![
                    series.bytes().as_slice(),
                    ordinal,
                    ordinal * day,
                    ordinal * day + 1
                ],
            )?;
        }
        connection.execute_batch("COMMIT")?;
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        for annual in [false, true] {
            let recipe = recipe_from_index(&connection, series, annual, deadline, &cancellation)?
                .ok_or("expected complete recipe")?;
            assert!(recipe.coordinates.len() <= MAXIMUM_EXAMPLES);
            assert_eq!(
                recipe.coordinates.len(),
                recipe.split_counts.iter().sum::<usize>()
            );
            assert!(recipe.coordinates.last().ok_or("last coordinate")?[2] > 6144);
            assert_eq!(recipe.from, Timestamp::from_unix_nanos(day + 1));
            assert_eq!(recipe.through, Timestamp::from_unix_nanos(8999 * day + 1));
            if annual {
                let first = i128::from(day + 1);
                let end = i128::from(8999 * day + 1);
                let train = first + (end - first) / 3;
                let validation = first + 2 * (end - first) / 3;
                for (index, coordinate) in recipe.coordinates.iter().enumerate() {
                    assert_eq!(coordinate[2] - coordinate[1], 365);
                    let origin = i128::from(coordinate[1] as i64 * day + 1);
                    let label = i128::from(coordinate[2] as i64 * day + 1);
                    if index < recipe.split_counts[0] {
                        assert!(label <= train);
                    } else if index < recipe.split_counts[0] + recipe.split_counts[1] {
                        assert!(origin > train && label <= validation);
                    } else {
                        assert!(origin > validation && label <= end);
                    }
                }
            }
        }
        cancellation.cancel();
        assert!(matches!(
            recipe_from_index(&connection, series, true, deadline, &cancellation),
            Err(DatasetPreparationError::Cancelled)
        ));
        Ok(())
    }
}
