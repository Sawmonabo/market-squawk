//! Bounded selected Macro history over one immutable source-qualified generation.

use super::*;

const HISTORY_BYTES: usize = 32 * 1024 * 1024;

/// Inclusive effective range. Provider periods keep their native ordering namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnalyticalMacroHistoryRange {
    /// Inclusive calendar dates, without timestamp coercion.
    Calendar {
        start: CalendarDate,
        end: CalendarDate,
    },
    /// Inclusive periods in one provider/frequency scheme.
    ProviderPeriod {
        start: ResearchPeriod,
        end: ResearchPeriod,
    },
}

impl AnalyticalMacroHistoryRange {
    fn validate(&self) -> Result<(), AnalyticalReadError> {
        let valid = match self {
            Self::Calendar { start, end } => start <= end,
            Self::ProviderPeriod { start, end } => matches!(
                start.partial_cmp(end),
                Some(Ordering::Less | Ordering::Equal)
            ),
        };
        if valid {
            Ok(())
        } else {
            Err(AnalyticalReadError::InvalidMacroHistoryRequest)
        }
    }

    fn end(&self) -> ResearchTemporalCoordinate {
        match self {
            Self::Calendar { end, .. } => ResearchTemporalCoordinate::calendar_date(*end),
            Self::ProviderPeriod { end, .. } => {
                ResearchTemporalCoordinate::source_period(end.clone())
            }
        }
    }

    fn contains(&self, value: &ResearchTemporalCoordinate) -> bool {
        match self {
            Self::Calendar { start, end } => value
                .calendar_date_value()
                .is_some_and(|v| start <= &v && &v <= end),
            Self::ProviderPeriod { start, end } => value.source_period_value().is_some_and(|v| {
                matches!(
                    v.partial_cmp(start),
                    Some(Ordering::Greater | Ordering::Equal)
                ) && matches!(v.partial_cmp(end), Some(Ordering::Less | Ordering::Equal))
            }),
        }
    }

    fn columns(&self) -> &'static str {
        match self {
            Self::Calendar { .. } => "macro_series, effective_date",
            Self::ProviderPeriod { .. } => {
                "macro_series, effective_period_year, effective_period_ordinal, effective_period_code"
            }
        }
    }

    fn predicate(&self) -> String {
        match self {
            Self::Calendar { start, end } => format!(
                "effective_precision = 'calendar_date' AND effective_date >= DATE '{start}' AND effective_date <= DATE '{end}'"
            ),
            Self::ProviderPeriod { start, end } => format!(
                "effective_precision = 'source_period' AND effective_period_scheme = {} AND {} AND {}",
                sql_string_literal(start.scheme().as_str()),
                period_bound(start, true),
                period_bound(end, false)
            ),
        }
    }
}

// ResearchPeriod orders year/ordinal, but unequal codes at the same ordinal are
// incomparable. Inclusive endpoints therefore require code equality, never code >= or <=.
fn period_bound(period: &ResearchPeriod, lower: bool) -> String {
    let comparison = if lower { ">" } else { "<" };
    let year = period.year();
    let ordinal = period.ordinal().get();
    let code = sql_string_literal(period.code().as_str());
    format!(
        "(effective_period_year {comparison} {year} OR (effective_period_year = {year} AND effective_period_ordinal {comparison} {ordinal}) OR (effective_period_year = {year} AND effective_period_ordinal = {ordinal} AND effective_period_code = {code}))"
    )
}

/// Opaque continuation bound to the complete immutable history request.
/// Serialization supports restart; request construction revalidates the range and request fence.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AnalyticalMacroHistoryCursor {
    request_digest: [u8; 32],
    series: SourceIdentifier,
    effective: ResearchTemporalCoordinate,
}

/// Fixed knowledge cutoff and manifest for every page of selected history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalMacroHistoryRequest {
    manifest: DatasetManifestRef,
    source_series: AnalyticalMacroSourceQualifiedSeries,
    knowledge_cutoff: Timestamp,
    range: AnalyticalMacroHistoryRange,
    limit: AnalyticalReadLimit,
    cursor: Option<AnalyticalMacroHistoryCursor>,
}

impl AnalyticalMacroHistoryRequest {
    /// Creates a page of at most 32 rows, matching the selected-original verifier ceiling.
    /// A continuation cannot change source, cutoff, range or size.
    pub fn try_new(
        manifest: DatasetManifestRef,
        source_series: AnalyticalMacroSourceQualifiedSeries,
        knowledge_cutoff: Timestamp,
        range: AnalyticalMacroHistoryRange,
        limit: AnalyticalReadLimit,
        cursor: Option<AnalyticalMacroHistoryCursor>,
    ) -> Result<Self, AnalyticalReadError> {
        range.validate()?;
        // The existing selected-original verifier accepts at most 32 aligned rows.
        if limit.get() > MAX_MACRO_SNAPSHOT_SERIES {
            return Err(AnalyticalReadError::InvalidMacroHistoryRequest);
        }
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| AnalyticalReadError::InvalidObservationSchema)?;
        if manifest.schema() != &canonical {
            return Err(AnalyticalReadError::InvalidObservationSchema);
        }
        let request = Self {
            manifest,
            source_series,
            knowledge_cutoff,
            range,
            limit,
            cursor,
        };
        if request.cursor.as_ref().is_some_and(|cursor| {
            cursor.request_digest != request.fence()
                || !request
                    .source_series
                    .series_allowlist
                    .contains(&cursor.series)
                || !request.range.contains(&cursor.effective)
        }) {
            return Err(AnalyticalReadError::InvalidMacroHistoryRequest);
        }
        Ok(request)
    }

    /// Starts after an explicit source-qualified effective key on this manifest.
    /// This is a selection filter, not evidence or a continuation from another generation.
    pub fn after_coordinate(
        mut self,
        series: SourceIdentifier,
        effective: ResearchTemporalCoordinate,
    ) -> Result<Self, AnalyticalReadError> {
        if !self.source_series.series_allowlist.contains(&series)
            || !self.range.contains(&effective)
        {
            return Err(AnalyticalReadError::InvalidMacroHistoryRequest);
        }
        self.cursor = Some(AnalyticalMacroHistoryCursor {
            request_digest: self.fence(),
            series,
            effective,
        });
        Ok(self)
    }

    /// Exact generation shared by every page.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    /// Exact source and selected series.
    pub const fn source_series(&self) -> &AnalyticalMacroSourceQualifiedSeries {
        &self.source_series
    }
    /// Conservative inclusive knowledge cutoff, independent of effective range.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    /// Effective coordinate range without inferred dates.
    pub const fn range(&self) -> &AnalyticalMacroHistoryRange {
        &self.range
    }
    /// Complete row envelope, including all ties and saturation/lookahead sentinels.
    pub fn required_query_rows(&self) -> u64 {
        self.candidate_limit() as u64 + 1
    }

    fn candidate_limit(&self) -> usize {
        (self.limit.get() + 1) * MAX_MACRO_SNAPSHOT_TIED_CANDIDATES_PER_SERIES
    }

    fn fence(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/selected-macro-history-request/v1");
        hash_manifest(&mut hash, &self.manifest);
        hash_str(&mut hash, self.source_series.source_id.as_str());
        hash_timestamp(&mut hash, self.knowledge_cutoff);
        hash_str(&mut hash, &self.range.predicate());
        hash.update((self.limit.get() as u64).to_be_bytes());
        for series in self.source_series.series_allowlist.series() {
            hash_str(&mut hash, series.as_str());
        }
        hash.finalize().into()
    }

    fn after(&self) -> String {
        let Some(cursor) = &self.cursor else {
            return "TRUE".to_owned();
        };
        let series = sql_string_literal(cursor.series.as_str());
        let effective = match &self.range {
            AnalyticalMacroHistoryRange::Calendar { .. } => cursor.effective.calendar_date_value()
                .map(|date| format!("effective_date > DATE '{date}'")),
            AnalyticalMacroHistoryRange::ProviderPeriod { .. } => cursor.effective.source_period_value().map(|p| {
                format!("(effective_period_year > {} OR (effective_period_year = {} AND effective_period_ordinal > {}) OR (effective_period_year = {} AND effective_period_ordinal = {} AND effective_period_code > {}))", p.year(), p.year(), p.ordinal().get(), p.year(), p.ordinal().get(), sql_string_literal(p.code().as_str()))
            }),
        }.unwrap_or_else(|| "FALSE".to_owned());
        format!("(macro_series > {series} OR (macro_series = {series} AND {effective}))")
    }

    fn sql(&self) -> String {
        let columns = self.range.columns();
        let join = columns
            .split(", ")
            .map(|column| format!("eligible.{column} = page.{column}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let order = columns
            .split(", ")
            .map(|column| format!("eligible.{column}"))
            .collect::<Vec<_>>()
            .join(", ");
        let series = self
            .source_series
            .series_allowlist
            .series()
            .iter()
            .map(|s| sql_string_literal(s.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        let cutoff = self.knowledge_cutoff.unix_nanos();
        // Calendar publication eligibility is applied BEFORE MAX(revision), so an ineligible
        // later revision cannot hide an earlier revision that was known at the cutoff.
        let publication_date = self.knowledge_cutoff.utc_calendar_date().map(|date|
            format!(" AND (published_precision <> 'calendar_date' OR published_precision IS NULL OR published_date <= DATE '{date}')"))
            .unwrap_or_else(|_| " AND FALSE".to_owned());
        format!(
            "WITH eligible AS (SELECT * FROM {OBSERVATION_TABLE} WHERE observation_kind = 'macro' \
             AND source_id = {source} AND macro_series IN ({series}) AND available_at IS NOT NULL \
             AND CAST(available_at AS BIGINT) <= {cutoff} AND CAST(received_at AS BIGINT) <= {cutoff} \
             AND CAST(ingested_at AS BIGINT) <= {cutoff} \
             AND (published_precision IS NULL OR published_precision <> 'exact_timestamp' OR CAST(published_at AS BIGINT) <= {cutoff}) \
             {publication_date} AND {range} AND {after}), \
             families AS (SELECT {columns}, MAX(revision) AS latest_revision FROM eligible GROUP BY {columns}), \
             page AS (SELECT * FROM families ORDER BY {columns} LIMIT {keys}) \
             SELECT eligible.*, COUNT(*) OVER (PARTITION BY {order}) AS tie_count \
             FROM eligible JOIN page ON {join} AND eligible.revision = page.latest_revision \
             ORDER BY {order}, eligible.payload_sha256, eligible.source_identifier, eligible.request_sha256, eligible.extraction_lineage_json LIMIT {rows}",
            source = sql_string_literal(self.source_series.source_id.as_str()),
            range = self.range.predicate(),
            after = self.after(),
            keys = self.limit.get() + 1,
            rows = self.required_query_rows(),
        )
    }
}

/// One complete page of latest-known revisions, with aligned original capture authority.
#[derive(Debug)]
pub struct AnalyticalMacroHistoryPage {
    request: AnalyticalMacroHistoryRequest,
    output: PinnedQueryOutput,
    observations: Box<[MacroObservation]>,
    selected_rows: SelectedProviderCaptureRows,
    next_cursor: Option<AnalyticalMacroHistoryCursor>,
}

impl AnalyticalMacroHistoryPage {
    /// Full request needed to reproduce this page.
    pub const fn request(&self) -> &AnalyticalMacroHistoryRequest {
        &self.request
    }
    /// Canonical observations ordered by series and native effective coordinate.
    pub fn observations(&self) -> &[MacroObservation] {
        &self.observations
    }
    /// Exact query, object graph and result evidence.
    pub const fn output(&self) -> &PinnedQueryOutput {
        &self.output
    }
    /// Final ordered selection digest, including original capture coordinates.
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selected_rows.selection_digest
    }
    /// Row-for-row aligned capture authority for the existing bounded physical verifier.
    pub fn selected_provider_rows(&self) -> SelectedProviderCaptureRows {
        self.selected_rows.clone()
    }
    /// Opaque continuation; absence proves exhaustion of this request's eligible families.
    pub const fn next_cursor(&self) -> Option<&AnalyticalMacroHistoryCursor> {
        self.next_cursor.as_ref()
    }
    /// Continues without changing the manifest, cutoff, source, series, range or page size.
    pub fn next_request(&self) -> Option<AnalyticalMacroHistoryRequest> {
        self.next_cursor.clone().map(|cursor| {
            let mut request = self.request.clone();
            request.cursor = Some(cursor);
            request
        })
    }
}

impl AnalyticalReadCapability {
    /// Selects a complete bounded page of saved history at one fixed knowledge cutoff.
    /// Missing source capture or divergent same-revision semantics fail closed.
    pub async fn read_macro_selected_history(
        &self,
        request: AnalyticalMacroHistoryRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<AnalyticalMacroHistoryPage, AnalyticalReadError> {
        history_read_checkpoint(deadline, &cancellation).await?;
        let (pinned, source_id, _) =
            self.manifests
                .read_exact(request.manifest(), deadline, &cancellation)?;
        if &source_id != request.source_series.source_id() {
            return Err(AnalyticalReadError::MacroSnapshotSourceOwnerMismatch);
        }
        let query = QueryRequest::try_new(pinned.manifest().clone(), request.sql())?;
        let engine = ResearchQueryEngine::from_pinned_dataset(
            pinned,
            OBSERVATION_TABLE,
            Arc::clone(&self.objects),
            cancellation.clone(),
        )
        .await?;
        let operation = cancellation.child_token();
        let execution = engine.query_pinned(query, limits, operation.clone());
        tokio::pin!(execution);
        let output = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                operation.cancel(); let _ignored = execution.as_mut().await;
                return Err(AnalyticalReadError::Query(QueryError::Cancelled));
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                operation.cancel(); let _ignored = execution.as_mut().await;
                return Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded));
            }
            result = execution.as_mut() => result?,
        };
        decode_page(request, output, deadline, &cancellation).await
    }
}

// Validate the complete SQL envelope before decoding any canonical payload. COUNT is computed
// over whole selected families before LIMIT, so even a truncated oversize family is rejected.
fn validate_candidate_envelope(
    request: &AnalyticalMacroHistoryRequest,
    batches: &[RecordBatch],
) -> Result<usize, AnalyticalReadError> {
    let count = batches.iter().try_fold(0usize, |sum, batch| {
        sum.checked_add(batch.num_rows())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)
    })?;
    if count > request.candidate_limit() {
        return Err(AnalyticalReadError::MacroSnapshotCandidateSetSaturated);
    }
    for batch in batches {
        let ties = required_macro_column::<Int64Array>(batch, "tie_count")?;
        for row in 0..batch.num_rows() {
            if ties.is_null(row) {
                return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
            }
            if !(1..=MAX_MACRO_SNAPSHOT_TIED_CANDIDATES_PER_SERIES as i64)
                .contains(&ties.value(row))
            {
                return Err(AnalyticalReadError::MacroSnapshotCandidateSetSaturated);
            }
        }
    }
    Ok(count)
}

async fn decode_page(
    request: AnalyticalMacroHistoryRequest,
    output: PinnedQueryOutput,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<AnalyticalMacroHistoryPage, AnalyticalReadError> {
    let invalid = || AnalyticalReadError::InvalidMacroSnapshotResult;
    if output.manifest() != request.manifest() {
        return Err(invalid());
    }
    let crate::QueryResult::Inline { batches, .. } = output.result() else {
        return Err(AnalyticalReadError::MacroSnapshotResultRequiresInline);
    };
    let count = validate_candidate_envelope(&request, batches)?;
    let mut candidates = Vec::with_capacity(count);
    let mut captures = Vec::with_capacity(count);
    let mut groups = BTreeMap::new();
    let mut retained = count
        .checked_mul(
            std::mem::size_of::<crate::arrow_convert::ProviderCaptureRowCoordinate>()
                + std::mem::size_of::<PointInTimeCandidate>(),
        )
        .ok_or_else(invalid)?;
    for batch in batches {
        let ties = required_macro_column::<Int64Array>(batch, "tie_count")?;
        let requests = required_macro_column::<BinaryArray>(batch, "request_sha256")?;
        let lineages = required_macro_column::<BinaryArray>(batch, "extraction_lineage_json")?;
        let payloads = required_macro_column::<BinaryArray>(batch, "payload_json")?;
        let canonical = batch
            .project(&(0..batch.num_columns() - 1).collect::<Vec<_>>())
            .map_err(|_| invalid())?;
        for row in 0..batch.num_rows() {
            history_read_checkpoint(deadline, cancellation).await?;
            if ties.is_null(row)
                || requests.is_null(row)
                || lineages.is_null(row)
                || payloads.is_null(row)
            {
                return Err(invalid());
            }
            let declared = usize::try_from(ties.value(row)).map_err(|_| invalid())?;
            let (mut decoded, bytes) = ResearchArrowBatch::decode_query_projection_bounded(
                canonical.slice(row, 1),
                HISTORY_BYTES.checked_sub(retained).ok_or_else(invalid)?,
            )
            .map_err(|_| invalid())?;
            retained = retained.checked_add(bytes).ok_or_else(invalid)?;
            let observation = decoded.pop().ok_or_else(invalid)?;
            let ResearchObservation::Macro(value) = &observation else {
                return Err(invalid());
            };
            validate_observation(&request, value)?;
            let key = history_key(value)?;
            let group = groups.entry(key).or_insert((declared, 0usize));
            if group.0 != declared {
                return Err(invalid());
            }
            group.1 += 1;
            captures.push(
                ResearchArrowBatch::selected_provider_capture_coordinate(
                    lineages.value(row),
                    requests.value(row),
                    &observation,
                    payloads.value(row),
                )
                .map_err(|_| invalid())?
                .ok_or_else(invalid)?,
            );
            candidates.push(PointInTimeCandidate::new(
                observation,
                request.manifest.clone(),
            ));
        }
    }
    if groups
        .values()
        .any(|(declared, observed)| declared != observed)
        || groups.len() > request.limit.get() + 1
    {
        return Err(invalid());
    }
    let policy = PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)
        .map_err(|_| invalid())?;
    let pit_limits = PointInTimeLimits::try_new(
        request.candidate_limit(),
        request.limit.get() + 1,
        request.candidate_limit(),
        request.limit.get() + 1,
        HISTORY_BYTES,
    )
    .map_err(|_| invalid())?;
    // Calendar publication clocks were checked independently before SQL revision selection.
    // A period publication coordinate is never coerced into a knowledge timestamp.
    let pit = PointInTimeRequest::try_new(
        policy,
        request.knowledge_cutoff,
        None,
        request.range.end(),
        None,
        pit_limits,
    )
    .map_err(|_| invalid())?;
    let selection = PointInTimeService::new()
        .select(&pit, &candidates, cancellation, deadline)
        .await
        .map_err(|error| match error {
            crate::PointInTimeError::RevisionConflicts { .. } => {
                AnalyticalReadError::MacroSnapshotRevisionConflict
            }
            crate::PointInTimeError::Cancelled => AnalyticalReadError::Query(QueryError::Cancelled),
            crate::PointInTimeError::DeadlineExceeded => {
                AnalyticalReadError::Query(QueryError::DeadlineExceeded)
            }
            _ => invalid(),
        })?;
    if selection.records().len() != groups.len() {
        return Err(invalid());
    }
    let mut selected = selection
        .records()
        .iter()
        .map(|record| {
            let ResearchObservation::Macro(value) = record.candidate().observation() else {
                return Err(invalid());
            };
            let index = candidates
                .iter()
                .position(|c| std::ptr::eq(c, record.candidate()))
                .ok_or_else(invalid)?;
            Ok((history_key(value)?, value.clone(), captures[index], record))
        })
        .collect::<Result<Vec<_>, AnalyticalReadError>>()?;
    selected.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let has_more = selected.len() > request.limit.get();
    selected.truncate(request.limit.get());
    let next_cursor = if has_more {
        let value = &selected.last().ok_or_else(invalid)?.1;
        Some(AnalyticalMacroHistoryCursor {
            request_digest: request.fence(),
            series: value.series().clone(),
            effective: value.context().time().effective().clone(),
        })
    } else {
        None
    };
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/selected-macro-history-page/v1");
    hash.update(request.fence());
    hash_evidence(&mut hash, output.object_graph_digest());
    hash_evidence(&mut hash, output.query_identity());
    hash_evidence(&mut hash, output.result_digest());
    hash.update([u8::from(has_more)]);
    hash.update((selected.len() as u64).to_be_bytes());
    for (_, _, capture, record) in &selected {
        hash.update(record.evidence_identity().bytes());
        hash_capture(&mut hash, capture);
    }
    let selection_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
    let selected_rows = SelectedProviderCaptureRows {
        manifest: request.manifest.clone(),
        source_id: request.source_series.source_id.clone(),
        selection_digest,
        rows: selected
            .iter()
            .map(|r| r.2)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let observations = selected
        .into_iter()
        .map(|r| r.1)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    history_read_checkpoint(deadline, cancellation).await?;
    Ok(AnalyticalMacroHistoryPage {
        request,
        output,
        observations,
        selected_rows,
        next_cursor,
    })
}

fn validate_observation(
    request: &AnalyticalMacroHistoryRequest,
    value: &MacroObservation,
) -> Result<(), AnalyticalReadError> {
    let provenance = value.context().provenance();
    let effective = value.context().time().effective();
    let key_after = request
        .cursor
        .as_ref()
        .map(|cursor| {
            value.series() > &cursor.series
                || (value.series() == &cursor.series
                    && coordinate_key(effective) > coordinate_key(&cursor.effective))
        })
        .unwrap_or(true);
    if provenance.source_id() != request.source_series.source_id()
        || !request
            .source_series
            .series_allowlist
            .contains(value.series())
        || !request.range.contains(effective)
        || !key_after
        || provenance
            .availability()
            .conservative_available_at()
            .is_none_or(|t| t > request.knowledge_cutoff)
        || provenance.received_at() > request.knowledge_cutoff
        || provenance.ingested_at() > request.knowledge_cutoff
        || value.context().time().published().is_some_and(|p| {
            p.exact_timestamp()
                .is_some_and(|t| t > request.knowledge_cutoff)
                || p.calendar_date_value().is_some_and(|date| {
                    request
                        .knowledge_cutoff
                        .utc_calendar_date()
                        .map(|cutoff| date > cutoff)
                        .unwrap_or(true)
                })
        })
    {
        return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
    }
    Ok(())
}

// Only homogeneous validated ranges reach these keys. The source code distinguishes equal
// provider ordinals whose native labels differ; no synthetic calendar date is created.
fn coordinate_key(value: &ResearchTemporalCoordinate) -> (i32, u16, String) {
    if let Some(date) = value.calendar_date_value() {
        (date.days_since_unix_epoch(), 0, String::new())
    } else if let Some(period) = value.source_period_value() {
        (
            i32::from(period.year()),
            period.ordinal().get(),
            period.code().as_str().to_owned(),
        )
    } else {
        (i32::MIN, 0, String::new())
    }
}

fn history_key(
    value: &MacroObservation,
) -> Result<(SourceIdentifier, i32, u16, String), AnalyticalReadError> {
    let effective = value.context().time().effective();
    if effective.calendar_date_value().is_none() && effective.source_period_value().is_none() {
        return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
    }
    let (year, ordinal, code) = coordinate_key(effective);
    Ok((value.series().clone(), year, ordinal, code))
}

fn hash_capture(hash: &mut Sha256, capture: &crate::arrow_convert::ProviderCaptureRowCoordinate) {
    for digest in [
        capture.binding_digest,
        capture.capture_observation_digest,
        capture.canonical_row_digest,
        capture.observation_digest,
        capture.native_semantic_digest,
        capture.page_body_digest,
    ] {
        hash_evidence(hash, digest);
    }
    hash.update(capture.canonical_row_ordinal.to_be_bytes());
    hash.update(capture.capture_page_ordinal.to_be_bytes());
    hash.update(capture.segment_ordinal.to_be_bytes());
    hash.update(capture.physical_frame_ordinal.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};

    /// The regression combines the critical page boundary and PIT/restart failure: a future
    /// revision must not hide an admitted earlier revision, and ties cannot be split by paging.
    #[tokio::test]
    async fn history_pages_preserve_revision_ties_and_restart_cutoff()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from("macro-history")?,
            1,
            DatasetSchemaRegistry::local().canonical_research_observations()?,
            Sha256Digest::new([1; 32]),
        )?;
        let first = CalendarDate::new(1970, 1, 1)?;
        let second = CalendarDate::new(1970, 1, 2)?;
        let source = AnalyticalMacroSourceQualifiedSeries::new(
            SourceId::try_from("census")?,
            AnalyticalMacroSeriesAllowlist::try_from_code_owned(&["population"])?,
        );
        let request = AnalyticalMacroHistoryRequest::try_new(
            manifest.clone(),
            source.clone(),
            Timestamp::from_unix_nanos(100),
            AnalyticalMacroHistoryRange::Calendar {
                start: first,
                end: second,
            },
            AnalyticalReadLimit::try_new(1)?,
            None,
        )?;
        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "observation_kind",
                Arc::new(StringArray::from(vec!["macro"; 4])),
            ),
            ("source_id", Arc::new(StringArray::from(vec!["census"; 4]))),
            (
                "macro_series",
                Arc::new(StringArray::from(vec!["population"; 4])),
            ),
            (
                "effective_precision",
                Arc::new(StringArray::from(vec!["calendar_date"; 4])),
            ),
            (
                "effective_date",
                Arc::new(Date32Array::from(vec![0, 0, 1, 1])),
            ),
            ("available_at", Arc::new(Int64Array::from(vec![1; 4]))),
            ("received_at", Arc::new(Int64Array::from(vec![1; 4]))),
            ("ingested_at", Arc::new(Int64Array::from(vec![1; 4]))),
            (
                "published_precision",
                Arc::new(StringArray::from(vec!["calendar_date"; 4])),
            ),
            (
                "published_date",
                Arc::new(Date32Array::from(vec![0, 1, 0, 0])),
            ),
            ("published_at", Arc::new(Int64Array::from(vec![None; 4]))),
            ("revision", Arc::new(UInt32Array::from(vec![1, 2, 1, 1]))),
            (
                "payload_sha256",
                Arc::new(BinaryArray::from(vec![b"a".as_slice(), b"b", b"c", b"d"])),
            ),
            (
                "source_identifier",
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ),
            (
                "request_sha256",
                Arc::new(BinaryArray::from(vec![b"request".as_slice(); 4])),
            ),
            (
                "extraction_lineage_json",
                Arc::new(BinaryArray::from(vec![b"lineage".as_slice(); 4])),
            ),
        ];
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, column)| Field::new(*name, column.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            columns.into_iter().map(|(_, column)| column).collect(),
        )?;
        let limits = QueryLimits::try_new(
            request.required_query_rows(),
            1024 * 1024,
            32 * 1024 * 1024,
            1,
            4096,
            4096,
            Duration::from_secs(10),
        )?;
        let engine = ResearchQueryEngine::from_pinned_batches(
            manifest.clone(),
            OBSERVATION_TABLE,
            vec![batch.clone()],
        )?;
        let result = engine
            .query(
                QueryRequest::try_new(manifest.clone(), request.sql())?,
                limits,
                CancellationToken::new(),
            )
            .await?;
        let crate::QueryResult::Inline { batches, .. } = result else {
            return Err("expected inline history".into());
        };
        let revisions = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("revision")
                    .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
                    .into_iter()
                    .flat_map(|a| a.values().iter().copied())
            })
            .collect::<Vec<_>>();
        assert_eq!(revisions, vec![1, 1, 1]);
        let after = request.clone().after_coordinate(
            SourceIdentifier::try_from("population")?,
            ResearchTemporalCoordinate::calendar_date(first),
        )?;
        let serialized = serde_json::to_vec(after.cursor.as_ref().ok_or("missing cursor")?)?;
        let cursor: AnalyticalMacroHistoryCursor = serde_json::from_slice(&serialized)?;
        let restarted = AnalyticalMacroHistoryRequest::try_new(
            manifest.clone(),
            source.clone(),
            request.knowledge_cutoff,
            request.range.clone(),
            request.limit,
            Some(cursor.clone()),
        )?;
        let reopened = ResearchQueryEngine::from_pinned_batches(
            manifest.clone(),
            OBSERVATION_TABLE,
            vec![batch.clone()],
        )?;
        let result = reopened
            .query(
                QueryRequest::try_new(manifest.clone(), restarted.sql())?,
                limits,
                CancellationToken::new(),
            )
            .await?;
        let crate::QueryResult::Inline { batches, .. } = result else {
            return Err("expected inline history".into());
        };
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(batches.iter().all(|batch| {
            required_macro_column::<Int64Array>(batch, "tie_count")
                .is_ok_and(|ties| ties.values().iter().all(|count| *count == 2))
        }));
        // Both oversize paths fail closed: nine ties fit the total envelope but violate
        // the family ceiling; eighteen ties hit the seventeen-row saturation sentinel.
        for tie_count in [9usize, 18] {
            let tied = arrow::compute::concat_batches(
                &batch.schema(),
                &vec![batch.slice(0, 1); tie_count],
            )?;
            let tied_engine = ResearchQueryEngine::from_pinned_batches(
                manifest.clone(),
                OBSERVATION_TABLE,
                vec![tied],
            )?;
            let result = tied_engine
                .query(
                    QueryRequest::try_new(manifest.clone(), request.sql())?,
                    limits,
                    CancellationToken::new(),
                )
                .await?;
            let crate::QueryResult::Inline { batches, .. } = result else {
                return Err("expected inline tie envelope".into());
            };
            assert!(matches!(
                validate_candidate_envelope(&request, &batches),
                Err(AnalyticalReadError::MacroSnapshotCandidateSetSaturated)
            ));
        }

        // Distinct labels at one ordinal are incomparable, including at range endpoints.
        let period = |ordinal, code| -> Result<ResearchPeriod, Box<dyn std::error::Error>> {
            Ok(ResearchPeriod::try_new(
                SourceIdentifier::try_from("census-quarter")?,
                1970,
                std::num::NonZeroU16::new(ordinal).ok_or("zero period")?,
                SourceIdentifier::try_from(code)?,
            )?)
        };
        let start = period(1, "Q1")?;
        let end = period(2, "Q2")?;
        assert_eq!(start.partial_cmp(&period(1, "Q1B")?), None);
        let period_request = AnalyticalMacroHistoryRequest::try_new(
            manifest.clone(),
            source.clone(),
            request.knowledge_cutoff,
            AnalyticalMacroHistoryRange::ProviderPeriod { start, end },
            request.limit,
            None,
        )?;
        let mut period_columns = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, array)| {
                let array: ArrayRef = match field.name().as_str() {
                    "effective_precision" => Arc::new(StringArray::from(vec!["source_period"; 4])),
                    "published_date" => Arc::new(Date32Array::from(vec![0; 4])),
                    _ => Arc::clone(array),
                };
                (field.name().clone(), array)
            })
            .collect::<Vec<_>>();
        period_columns.extend([
            (
                "effective_period_scheme".to_owned(),
                Arc::new(StringArray::from(vec!["census-quarter"; 4])) as ArrayRef,
            ),
            (
                "effective_period_year".to_owned(),
                Arc::new(UInt16Array::from(vec![1970; 4])) as ArrayRef,
            ),
            (
                "effective_period_ordinal".to_owned(),
                Arc::new(UInt16Array::from(vec![1, 1, 2, 2])) as ArrayRef,
            ),
            (
                "effective_period_code".to_owned(),
                Arc::new(StringArray::from(vec!["Q1", "Q1B", "Q2", "Q2B"])) as ArrayRef,
            ),
        ]);
        let period_engine = ResearchQueryEngine::from_pinned_batches(
            manifest.clone(),
            OBSERVATION_TABLE,
            vec![RecordBatch::try_from_iter(period_columns)?],
        )?;
        let result = period_engine
            .query(
                QueryRequest::try_new(manifest.clone(), period_request.sql())?,
                limits,
                CancellationToken::new(),
            )
            .await?;
        let crate::QueryResult::Inline { batches, .. } = result else {
            return Err("expected inline period envelope".into());
        };
        let codes = batches
            .iter()
            .map(|batch| required_macro_column::<StringArray>(batch, "effective_period_code"))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            codes
                .iter()
                .flat_map(|array| array.iter())
                .collect::<Vec<_>>(),
            vec![Some("Q1"), Some("Q2")]
        );
        assert!(
            AnalyticalMacroHistoryRequest::try_new(
                manifest,
                source,
                Timestamp::from_unix_nanos(101),
                request.range,
                request.limit,
                Some(cursor)
            )
            .is_err()
        );
        Ok(())
    }
}
