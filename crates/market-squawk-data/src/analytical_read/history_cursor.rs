//! Complete-input verification with disk-ordered original observations and bounded decode state.
use super::*;
use rusqlite::params;

/// A fully verified immutable source selection. Only one original observation is decoded at once.
/// Its operation-owned staging files disappear when the cursor is dropped.
pub struct CompleteMarketBarHistoryCursor {
    pub(super) connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
    _directory: Arc<crate::OperationScratchDirectory>,
    pub(super) native_sessions: Option<RetainedHistoryNativeSessions>,
    counts: [usize; 3],
    pub(super) selection: CompleteMarketBarHistorySelection,
    pub(super) read_receipt: CompleteMarketBarHistoryReadReceipt,
    pub(super) deadline: Instant,
    pub(super) cancellation: CancellationToken,
}
impl std::fmt::Debug for CompleteMarketBarHistoryCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteMarketBarHistoryCursor")
            .field("selection", &self.selection)
            .finish_non_exhaustive()
    }
}
impl CompleteMarketBarHistoryCursor {
    /// Shares this immutable source operation's scratch authority for dependent streaming work.
    pub fn operation_scratch(&self) -> Arc<crate::OperationScratchDirectory> {
        Arc::clone(&self._directory)
    }
    /// Original exact manifest selection.
    pub const fn selection(&self) -> &CompleteMarketBarHistorySelection {
        &self.selection
    }
    /// Same canonical source-content and read identities as a complete materialized read.
    pub const fn read_receipt(&self) -> &CompleteMarketBarHistoryReadReceipt {
        &self.read_receipt
    }
    /// Repeatable ordered originals, without collecting the complete history.
    pub fn bars(
        &self,
    ) -> impl Iterator<Item = Result<MarketBarObservation, AnalyticalReadError>> + '_ {
        self.typed_rows(0)
    }
    fn typed_rows<T: serde::de::DeserializeOwned>(
        &self,
        kind: i64,
    ) -> impl Iterator<Item = Result<T, AnalyticalReadError>> + '_ {
        let mut prior: Option<(i64, i64)> = None;
        let mut done = false;
        std::iter::from_fn(move || {
            if done {
                return None;
            }
            if self.cancellation.is_cancelled() {
                done = true;
                return Some(Err(AnalyticalReadError::Query(QueryError::Cancelled)));
            }
            if Instant::now() >= self.deadline {
                done = true;
                return Some(Err(AnalyticalReadError::Query(
                    QueryError::DeadlineExceeded,
                )));
            }
            let result = (|| {
                use rusqlite::OptionalExtension as _;
                let connection = self
                    .connection
                    .lock()
                    .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?;
                let read_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<(i64, i64, Vec<u8>)> {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                };
                let row:Option<(i64,i64,Vec<u8>)>=if let Some((ordering,ordinal))=prior {
                    connection.prepare("SELECT ordering,ordinal,payload FROM history WHERE kind=?1 AND (ordering,ordinal)>(?2,?3) ORDER BY ordering,ordinal LIMIT 1")
                        .map_err(|_|AnalyticalReadError::InvalidMarketBarResult)?.query_row(params![kind,ordering,ordinal],read_row).optional()
                }else{
                    connection.prepare("SELECT ordering,ordinal,payload FROM history WHERE kind=?1 ORDER BY ordering,ordinal LIMIT 1")
                        .map_err(|_|AnalyticalReadError::InvalidMarketBarResult)?.query_row([kind],read_row).optional()
                }.map_err(|_|AnalyticalReadError::InvalidMarketBarResult)?;
                row.map(|(order, ordinal, bytes)| {
                    prior = Some((order, ordinal));
                    serde_json::from_slice(&bytes)
                        .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)
                })
                .transpose()
            })();
            match result {
                Ok(Some(row)) => Some(Ok(row)),
                Ok(None) => {
                    done = true;
                    None
                }
                Err(error) => {
                    done = true;
                    Some(Err(error))
                }
            }
        })
    }
    /// Looks up one exact original effective coordinate without scanning or aliasing precision.
    /// Duplicate coordinates and decoded/index mismatches fail closed.
    pub(crate) fn bar_at_coordinate(
        &self,
        coordinate: &market_squawk_domain::ResearchTemporalCoordinate,
    ) -> Result<Option<MarketBarObservation>, AnalyticalReadError> {
        if self.cancellation.is_cancelled() {
            return Err(QueryError::Cancelled.into());
        }
        if Instant::now() >= self.deadline {
            return Err(QueryError::DeadlineExceeded.into());
        }
        let invalid = || AnalyticalReadError::InvalidMarketBarResult;
        let key = serde_json::to_vec(coordinate).map_err(|_| invalid())?;
        let connection = self.connection.lock().map_err(|_| invalid())?;
        let mut statement = connection
            .prepare("SELECT payload FROM history WHERE kind=0 AND coordinate=?1 LIMIT 2")
            .map_err(|_| invalid())?;
        let mut rows = statement.query([key]).map_err(|_| invalid())?;
        let Some(row) = rows.next().map_err(|_| invalid())? else {
            return Ok(None);
        };
        let bytes: Vec<u8> = row.get(0).map_err(|_| invalid())?;
        let bar: MarketBarObservation = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if bar.context().time().effective() != coordinate
            || rows.next().map_err(|_| invalid())?.is_some()
        {
            return Err(invalid());
        }
        Ok(Some(bar))
    }
    /// Number of complete selected original bars.
    pub const fn bar_count(&self) -> usize {
        self.counts[0]
    }
    /// Number of original corporate action records.
    pub const fn source_action_count(&self) -> usize {
        self.counts[2]
    }
    /// Reopens companion source observations without retaining the complete series.
    pub fn companion_bars(
        &self,
    ) -> impl Iterator<Item = Result<MarketBarObservation, AnalyticalReadError>> + '_ {
        self.typed_rows(1)
    }
    /// Reopens source-owned corporate actions in their original order.
    pub fn source_actions(
        &self,
    ) -> impl Iterator<Item = Result<CorporateActionObservation, AnalyticalReadError>> + '_ {
        self.typed_rows(2)
    }

    pub(crate) fn source_action_at(
        &self,
        index: usize,
    ) -> Result<Option<CorporateActionObservation>, AnalyticalReadError> {
        if index >= self.source_action_count() {
            return Ok(None);
        }
        if self.cancellation.is_cancelled() {
            return Err(QueryError::Cancelled.into());
        }
        if Instant::now() >= self.deadline {
            return Err(QueryError::DeadlineExceeded.into());
        }
        let invalid = || AnalyticalReadError::InvalidMarketBarResult;
        use rusqlite::OptionalExtension as _;
        let connection = self.connection.lock().map_err(|_| invalid())?;
        let index = i64::try_from(index).map_err(|_| invalid())?;
        let bytes: Option<Vec<u8>> = connection.query_row(
            "SELECT payload FROM history WHERE kind=2 AND ordinal=(SELECT MIN(ordinal) FROM history WHERE kind=2)+?1",
            [index], |row| row.get(0),
        ).optional().map_err(|_| invalid())?;
        bytes
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|_| invalid()))
            .transpose()
    }

    /// Original source-calendar mapping when joined before spooling.
    pub const fn native_sessions(&self) -> Option<&RetainedHistoryNativeSessions> {
        self.native_sessions.as_ref()
    }
    /// Explicit financial convenience; ordinary chart reads consume the cursor directly.
    pub fn materialize(&self) -> Result<CompleteMarketBarHistoryOutput, AnalyticalReadError> {
        let bars = self
            .bars()
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        let companion_bars = self
            .typed_rows(1)
            .collect::<Result<Vec<MarketBarObservation>, _>>()?
            .into_boxed_slice();
        let source_actions = self
            .typed_rows(2)
            .collect::<Result<Vec<CorporateActionObservation>, _>>()?
            .into_boxed_slice();
        Ok(CompleteMarketBarHistoryOutput {
            companion_bars,
            source_actions,
            native_sessions: self.native_sessions.clone(),
            selection: self.selection.clone(),
            read_receipt: self.read_receipt.clone(),
            bars,
        })
    }
}

impl AnalyticalReadCapability {
    /// Reopens an exact or latest complete source window with bounded verified disk iteration.
    pub async fn read_complete_market_bar_history_cursor(
        &self,
        request: CompleteMarketBarHistoryRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompleteMarketBarHistoryCursor>, AnalyticalReadError> {
        let cutoff = request.knowledge_cutoff();
        let Some(selection) = self.manifests.select_complete_market_bar_history(
            &request,
            self.catalog_read_limits,
            deadline,
            &cancellation,
        )?
        else {
            return Ok(None);
        };
        self.selected_history_cursor(selection, cutoff, deadline, cancellation)
            .await
            .map(Some)
    }
    /// Reopens a canonical complete window using bounded decoding and disk-ordered verification.
    pub async fn read_canonical_market_bar_history_cursor(
        &self,
        request: CanonicalMarketBarHistoryRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompleteMarketBarHistoryCursor>, AnalyticalReadError> {
        let cutoff = request.knowledge_cutoff();
        let Some(selection) = self.manifests.select_canonical_market_bar_history(
            &request,
            self.catalog_read_limits,
            deadline,
            &cancellation,
        )?
        else {
            return Ok(None);
        };
        self.selected_history_cursor(selection, cutoff, deadline, cancellation)
            .await
            .map(Some)
    }
    pub(super) async fn selected_history_cursor(
        &self,
        selection: CompleteMarketBarHistorySelection,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompleteMarketBarHistoryCursor, AnalyticalReadError> {
        history_read_checkpoint(deadline, &cancellation).await?;
        let invalid = || AnalyticalReadError::InvalidMarketBarResult;
        let receipt = selection.receipt();
        let (origin, origin_source, _) = self.manifests.read_exact_snapshot(
            receipt.origin_manifest(),
            self.catalog_read_limits,
            deadline,
            &cancellation,
        )?;
        let ordinal = usize::from(receipt.origin_object_ordinal());
        let object = origin
            .objects()
            .get(ordinal)
            .filter(|object| object.artifact_id() == receipt.origin_artifact_id())
            .ok_or_else(invalid)?;
        if origin_source != *receipt.source_id()
            || object.object().row_count() != u64::from(receipt.origin_record_count())
        {
            return Err(invalid());
        }
        let origin_artifact_id = object.artifact_id();
        let object_content_hash = object.object().content_hash();
        let object_lineage_digest = object.object().lineage_digest();
        let object_row_count = object.object().row_count();
        let object_size_bytes = object.object().size_bytes();
        let directory = Arc::new(self.objects.operation_scratch()?);
        let mut connection = rusqlite::Connection::open(directory.path().join("history.sqlite"))
            .map_err(|_| invalid())?;
        connection.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA cache_size=-2048; CREATE TABLE history(kind INTEGER NOT NULL,ordering INTEGER NOT NULL,ordinal INTEGER NOT NULL,payload BLOB NOT NULL,coordinate BLOB,PRIMARY KEY(kind,ordering,ordinal)) WITHOUT ROWID; CREATE INDEX history_coordinates ON history(kind,coordinate)").map_err(|_|invalid())?;
        let mut cursor = self.objects.pinned_object_batch_cursor(
            &origin,
            origin_artifact_id,
            ordinal,
            256,
            COMPLETE_MARKET_BAR_HISTORY_OBJECT_MEMORY_BYTES,
            &cancellation,
        )?;
        let mut count = 0_i64;
        let mut selected_count = 0_usize;
        let mut companion_count = 0_usize;
        let mut action_suffix = false;
        loop {
            let batch = tokio::select! {
                biased;
                _=cancellation.cancelled()=>return Err(AnalyticalReadError::Query(QueryError::Cancelled)),
                _=tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))=>return Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded)),
                batch=cursor.next_batch()=>batch?,
            };
            let Some(batch) = batch else { break };
            let (observations, _) = ResearchArrowBatch::decode_record_batch_bounded(
                batch,
                COMPLETE_MARKET_BAR_HISTORY_OBJECT_MEMORY_BYTES,
            )
            .map_err(|_| invalid())?;
            // Keep every SQLite borrow inside a synchronous lexical scope. The owned
            // connection may cross the async checkpoint; its transaction and statements may not.
            {
                let transaction = connection.transaction().map_err(|_| invalid())?;
                {
                    let mut insert=transaction.prepare("INSERT INTO history(kind,ordering,ordinal,payload,coordinate) VALUES(?1,?2,?3,?4,?5)").map_err(|_|invalid())?;
                    for observation in observations {
                        if cancellation.is_cancelled() {
                            return Err(AnalyticalReadError::Query(QueryError::Cancelled));
                        }
                        if Instant::now() >= deadline {
                            return Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded));
                        }
                        let provenance = match &observation {
                            ResearchObservation::MarketBar(bar) => bar.context().provenance(),
                            ResearchObservation::CorporateAction(action) => {
                                action.context().provenance()
                            }
                            _ => return Err(invalid()),
                        };
                        if provenance
                            .availability()
                            .conservative_available_at()
                            .is_none_or(|value| value > knowledge_cutoff)
                            || provenance.received_at() > knowledge_cutoff
                            || provenance.ingested_at() > knowledge_cutoff
                            || provenance.source_id() != receipt.source_id()
                            || provenance.instrument_id() != Some(receipt.instrument_id())
                        {
                            return Err(invalid());
                        }
                        let (kind, ordering, bytes, coordinate) = match observation {
                            ResearchObservation::MarketBar(bar) if !action_suffix => {
                                let kind = if bar.adjustment() == receipt.adjustment() {
                                    selected_count += 1;
                                    0
                                } else if receipt.date_windows().is_some()
                                    && matches!(
                                        bar.adjustment(),
                                        MarketBarAdjustment::Raw | MarketBarAdjustment::All
                                    )
                                {
                                    companion_count += 1;
                                    1
                                } else {
                                    return Err(invalid());
                                };
                                let ordering = if receipt.date_windows().is_some() {
                                    count
                                } else {
                                    bar.time_semantics()
                                        .provider_timestamp()
                                        .ok_or_else(invalid)?
                                        .unix_nanos()
                                };
                                (
                                    kind,
                                    ordering,
                                    serde_json::to_vec(&bar).map_err(|_| invalid())?,
                                    Some(
                                        serde_json::to_vec(bar.context().time().effective())
                                            .map_err(|_| invalid())?,
                                    ),
                                )
                            }
                            ResearchObservation::CorporateAction(action)
                                if receipt.date_windows().is_some() =>
                            {
                                action_suffix = true;
                                (
                                    2,
                                    count,
                                    serde_json::to_vec(&action).map_err(|_| invalid())?,
                                    None,
                                )
                            }
                            _ => return Err(invalid()),
                        };
                        insert
                            .execute(params![kind, ordering, count, bytes, coordinate])
                            .map_err(|_| invalid())?;
                        count = count.checked_add(1).ok_or_else(invalid)?;
                    }
                }
                transaction.commit().map_err(|_| invalid())?;
            }
            history_read_checkpoint(deadline, &cancellation).await?;
        }
        if u64::try_from(count).ok() != Some(object_row_count) {
            return Err(invalid());
        }
        let placeholder = CompleteMarketBarHistoryReadReceipt {
            knowledge_cutoff,
            source_result_digest: selection.selection_digest(),
            selection_digest: selection.selection_digest(),
            publication_receipt_digest: receipt.receipt_digest(),
            origin_manifest: receipt.origin_manifest().clone(),
            origin_artifact_id,
            origin_object_ordinal: receipt.origin_object_ordinal(),
            object_content_hash,
            object_lineage_digest,
            object_row_count,
            object_size_bytes,
            history_content_digest: selection.selection_digest(),
            result_digest: selection.selection_digest(),
        };
        let mut output = CompleteMarketBarHistoryCursor {
            connection: Arc::new(std::sync::Mutex::new(connection)),
            _directory: directory,
            native_sessions: None,
            counts: [
                selected_count,
                companion_count,
                usize::try_from(count)
                    .map_err(|_| invalid())?
                    .checked_sub(selected_count + companion_count)
                    .ok_or_else(invalid)?,
            ],
            selection,
            read_receipt: placeholder,
            deadline,
            cancellation,
        };
        let receipt = output.selection.receipt();
        receipt.validate_selected_bar_iter(
            output
                .bars()
                .map(|bar| bar.map_err(|_| ManifestCatalogError::MarketBarHistoryMismatch)),
            selected_count,
        )?;
        if output.selection.surface_requirement()
            == crate::MarketHistoryPriceSurfaceRequirement::RawWithAll
        {
            receipt.validate_companion_bar_iter(
                output
                    .typed_rows(1)
                    .map(|bar| bar.map_err(|_| ManifestCatalogError::MarketBarHistoryMismatch)),
                companion_count,
            )?;
        }
        history_read_checkpoint(deadline, &output.cancellation).await?;
        let mut content = Sha256::new();
        content.update(COMPLETE_MARKET_BAR_HISTORY_CONTENT_DOMAIN);
        let mut result = Sha256::new();
        result.update(COMPLETE_MARKET_BAR_HISTORY_READ_DOMAIN);
        result.update(output.selection.selection_digest().bytes());
        for hash in [&mut content, &mut result] {
            let manifest = receipt.origin_manifest();
            hash.update(receipt.receipt_digest().bytes());
            hash_str(hash, manifest.dataset_id().as_str());
            hash.update(manifest.manifest_version().to_be_bytes());
            hash_str(hash, manifest.schema().name());
            hash.update(manifest.schema_version().get().to_be_bytes());
            hash.update(manifest.schema().fingerprint());
            hash.update(manifest.content_hash().bytes());
            hash.update(origin_artifact_id.as_bytes());
            hash.update(receipt.origin_object_ordinal().to_be_bytes());
            hash.update(object_content_hash.bytes());
            hash.update(object_lineage_digest.bytes());
            hash.update(object_row_count.to_be_bytes());
            hash.update(object_size_bytes.to_be_bytes());
            hash.update(receipt.bar_set_digest().bytes());
            hash.update((selected_count as u64).to_be_bytes());
        }
        for bar in output.bars() {
            let bar = bar?;
            let payload = CanonicalObservationPayload::try_from_observation(
                &ResearchObservation::MarketBar(bar.clone()),
            )
            .map_err(|_| invalid())?;
            for hash in [&mut content, &mut result] {
                if let Some(timestamp) = bar.time_semantics().provider_timestamp() {
                    hash.update([1]);
                    hash.update(timestamp.unix_nanos().to_be_bytes());
                } else if let Some(date) = bar.time_semantics().nominal_daily_date() {
                    hash.update([2]);
                    hash.update(date.date().year().to_be_bytes());
                    hash.update([date.date().month(), date.date().day()]);
                } else {
                    return Err(invalid());
                };
                hash_evidence(hash, payload.identity());
            }
        }
        output.read_receipt.history_content_digest = Sha256Digest::new(content.finalize().into());
        output.read_receipt.result_digest = Sha256Digest::new(result.finalize().into());
        output.read_receipt.source_result_digest = output.read_receipt.result_digest;
        history_read_checkpoint(deadline, &output.cancellation).await?;
        Ok(output)
    }
}

impl CompleteMarketBarHistoryOutput {
    /// Releases a materialized financial input into the same sealed disk cursor while preserving
    /// its joined native calendar and original receipts. No financial value is reconstructed.
    pub fn into_cursor(
        self,
        scratch: Arc<crate::OperationScratchDirectory>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompleteMarketBarHistoryCursor, AnalyticalReadError> {
        let invalid = || AnalyticalReadError::InvalidMarketBarResult;
        let mut connection = rusqlite::Connection::open(
            scratch
                .path()
                .join(format!("history-{}.sqlite", uuid::Uuid::new_v4())),
        )
        .map_err(|_| invalid())?;
        connection.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA cache_size=-2048; CREATE TABLE history(kind INTEGER NOT NULL,ordering INTEGER NOT NULL,ordinal INTEGER NOT NULL,payload BLOB NOT NULL,coordinate BLOB,PRIMARY KEY(kind,ordering,ordinal)) WITHOUT ROWID; CREATE INDEX history_coordinates ON history(kind,coordinate)").map_err(|_|invalid())?;
        let counts = [
            self.bars.len(),
            self.companion_bars.len(),
            self.source_actions.len(),
        ];
        let transaction = connection.transaction().map_err(|_| invalid())?;
        {
            let mut insert = transaction
                .prepare("INSERT INTO history(kind,ordering,ordinal,payload,coordinate) VALUES(?1,?2,?2,?3,?4)")
                .map_err(|_| invalid())?;
            let mut put = |kind: i64,
                           ordinal: usize,
                           bytes: Vec<u8>,
                           coordinate: Option<Vec<u8>>|
             -> Result<(), AnalyticalReadError> {
                if cancellation.is_cancelled() {
                    return Err(AnalyticalReadError::Query(QueryError::Cancelled));
                };
                if Instant::now() >= deadline {
                    return Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded));
                };
                insert
                    .execute(params![
                        kind,
                        i64::try_from(ordinal).map_err(|_| invalid())?,
                        bytes,
                        coordinate
                    ])
                    .map_err(|_| invalid())?;
                Ok(())
            };
            for (ordinal, bar) in self.bars.into_vec().into_iter().enumerate() {
                put(
                    0,
                    ordinal,
                    serde_json::to_vec(&bar).map_err(|_| invalid())?,
                    Some(
                        serde_json::to_vec(bar.context().time().effective())
                            .map_err(|_| invalid())?,
                    ),
                )?;
            }
            for (ordinal, bar) in self.companion_bars.into_vec().into_iter().enumerate() {
                put(
                    1,
                    ordinal,
                    serde_json::to_vec(&bar).map_err(|_| invalid())?,
                    Some(
                        serde_json::to_vec(bar.context().time().effective())
                            .map_err(|_| invalid())?,
                    ),
                )?;
            }
            for (ordinal, action) in self.source_actions.into_vec().into_iter().enumerate() {
                put(
                    2,
                    ordinal,
                    serde_json::to_vec(&action).map_err(|_| invalid())?,
                    None,
                )?;
            }
        }
        transaction.commit().map_err(|_| invalid())?;
        let connection = Arc::new(std::sync::Mutex::new(connection));
        let native_sessions = self
            .native_sessions
            .map(|native| {
                native.into_disk(
                    Arc::clone(&connection),
                    Arc::clone(&scratch),
                    deadline,
                    cancellation.clone(),
                )
            })
            .transpose()?;

        Ok(CompleteMarketBarHistoryCursor {
            connection,
            _directory: scratch,
            native_sessions,
            counts,
            selection: self.selection,
            read_receipt: self.read_receipt,
            deadline,
            cancellation,
        })
    }
}
