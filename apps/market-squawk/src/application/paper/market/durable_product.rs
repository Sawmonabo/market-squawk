//! Catalog-backed product discovery, durable marks and qualified SnapshotDisplay fallback.

use super::*;
use crate::application::market_selection::product::MarketProductSelectionReadCapability;
use crate::application::research::map_catalog_error;
use market_squawk_services::ServiceLimits;
use serde_json::json;

/// Drop also records a read whose caller cancels or discards its future before an error returns.
struct ProductReadProgress<'a> {
    operation: &'a str,
    context: &'a RequestContext,
    started: Instant,
    stage_started: Instant,
    stage: &'static str,
    remaining_at_stage_entry_ms: u128,
    instrument_id: Option<InstrumentId>,
    current_completed: usize,
    fallback_completed: usize,
    completed: bool,
}

impl<'a> ProductReadProgress<'a> {
    fn new(operation: &'a str, context: &'a RequestContext) -> Self {
        let now = Instant::now();
        Self {
            operation,
            context,
            started: now,
            stage_started: now,
            stage: "collection_snapshot",
            remaining_at_stage_entry_ms: context
                .deadline()
                .saturating_duration_since(now)
                .as_millis(),
            instrument_id: None,
            current_completed: 0,
            fallback_completed: 0,
            completed: false,
        }
    }

    fn enter(&mut self, stage: &'static str, instrument_id: Option<InstrumentId>) {
        self.trace_completed_stage();
        self.stage = stage;
        self.instrument_id = instrument_id;
        self.stage_started = Instant::now();
        self.remaining_at_stage_entry_ms = self
            .context
            .deadline()
            .saturating_duration_since(self.stage_started)
            .as_millis();
    }

    fn finish<T>(&mut self, result: Result<T, ServiceError>) -> Result<T, ServiceError> {
        self.completed = result.is_ok();
        if self.completed {
            self.trace_completed_stage();
        }
        result
    }

    fn trace_completed_stage(&self) {
        if !matches!(
            self.stage,
            "population"
                | "display_read"
                | "retained_display_read"
                | "previous_close"
                | "retained_routes_and_events"
                | "retained_execution_terms"
                | "retained_projection"
        ) {
            return;
        }
        let now = Instant::now();
        tracing::debug!(
            request_id = ?self.context.request_id(),
            operation = self.operation,
            stage = self.stage,
            elapsed_ms = %now.duration_since(self.started).as_millis(),
            stage_elapsed_ms = %now.duration_since(self.stage_started).as_millis(),
            remaining_at_stage_entry_ms = %self.remaining_at_stage_entry_ms,
            instrument_id = ?self.instrument_id,
            "product market read stage completed"
        );
    }
}

impl Drop for ProductReadProgress<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let now = Instant::now();
            tracing::warn!(
                request_id = ?self.context.request_id(),
                operation = self.operation,
                stage = self.stage,
                elapsed_ms = %now.duration_since(self.started).as_millis(),
                stage_elapsed_ms = %now.duration_since(self.stage_started).as_millis(),
                remaining_at_stage_entry_ms = %self.remaining_at_stage_entry_ms,
                instrument_id = ?self.instrument_id,
                current_completed = self.current_completed,
                fallback_completed = self.fallback_completed,
                cancelled = self.context.cancellation().is_cancelled(),
                deadline_elapsed = now >= self.context.deadline(),
                "product market read failed or interrupted"
            );
        }
    }
}

impl MarketDomainService {
    pub(super) async fn call_product(
        &self,
        request: &TypedToolRequest,
        reference_at: Timestamp,
        limits: ServiceLimits,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let mut progress = ProductReadProgress::new(request.name(), context);
        let collection = if request.name() == "Market.GetCollection" {
            Some(
                self.market_collection
                    .snapshot()
                    .map_err(|_| ServiceError::Unavailable)?,
            )
        } else {
            None
        };
        if request.name() == "Market.GetCollection"
            && !request
                .arguments()
                .get("includeMarket")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        {
            let collection = collection.ok_or(ServiceError::InvalidResult)?;
            let entries: Vec<_> = collection
                .choices
                .iter()
                .map(|choice| {
                    json!({"symbol": choice.symbol, "kept": choice.kept, "market": Value::Null})
                })
                .collect();
            ensure_live(context)?;
            return progress.finish(
                TypedToolResult::try_new(
                    json!({"revision": collection.revision.to_string(), "entries": entries}),
                    entries.len(),
                    ToolResultMetadata::complete_not_applicable(),
                    limits,
                )
                .map_err(|_| ServiceError::ResourceExhausted),
            );
        }
        let selections = MarketProductSelectionReadCapability::new(
            Arc::clone(&self.product_research),
            self.market_data_instruments.clone(),
        );
        progress.enter("population", None);
        let records = selections
            .population(reference_at, context.deadline(), context.cancellation())
            .await?;
        progress.enter("identity_selection", None);
        let argument = |name: &str| request.arguments().get(name).and_then(Value::as_str);
        let mut identities =
            product::product_market_identities(&records, reference_at, argument("query"))?;
        if let Some(collection) = &collection {
            identities.retain(|identity| {
                let choice = matches!(identity.asset_class(), "equity" | "fund")
                    .then(|| {
                        collection
                            .choices
                            .iter()
                            .find(|choice| Some(choice.symbol.as_str()) == identity.symbol())
                    })
                    .flatten();
                choice.is_some()
            });
        }
        let maximum_rows = limits
            .maximum_result_items()
            .min(product::MAXIMUM_PRODUCT_MARKET_ROWS);
        if request.name() == MARKET_SEARCH_UNIVERSE {
            progress.enter("search_projection", None);
            let (content, available, has_more) = product::product_search_page(
                &identities,
                argument("query").ok_or(ServiceError::InvalidRequest)?,
                maximum_rows,
                argument("pageToken"),
            )?;
            ensure_live(context)?;
            return progress.finish(product::product_result(
                content, available, has_more, limits,
            ));
        }
        if request.name() == MARKET_GET_HISTORY {
            let token = argument("historyToken").ok_or(ServiceError::InvalidRequest)?;
            let instrument_id = product::resolve_history_token(&identities, token)?;
            progress.enter("history_read", Some(instrument_id));
            let result = history::build_product_market_history_result(
                &self.market_history,
                &self.product_research,
                instrument_id,
                token,
                request,
                limits,
                context,
            )
            .await;
            return progress.finish(result);
        }
        progress.enter("page_selection", None);
        let page = if request.name() == MARKET_GET_INSTRUMENT {
            let instrument_id = product::resolve_selection_token(
                &identities,
                argument("selectionToken").ok_or(ServiceError::InvalidRequest)?,
            )?;
            let identity = identities
                .iter()
                .find(|identity| identity.instrument_id() == instrument_id)
                .ok_or(ServiceError::InvalidResult)?;
            product::select_product_page(std::slice::from_ref(identity), None, 1, None)?
        } else {
            product::select_product_page(
                &identities,
                argument("query"),
                maximum_rows,
                argument("pageToken"),
            )?
        };
        let mut instrument_ids = page.instrument_ids().to_vec();
        instrument_ids.sort_unstable();
        progress.enter("display_read", None);
        let mut rows = self
            .product_display_rows(&records, &instrument_ids, reference_at, limits, context)
            .await?;
        let missing = instrument_ids
            .iter()
            .copied()
            .filter(|id| {
                !rows
                    .iter()
                    .any(|row| row_instrument(row) == Some(*id) && has_current_price(row))
            })
            .collect::<Vec<_>>();
        progress.current_completed = instrument_ids.len() - missing.len();
        if !missing.is_empty() {
            progress.enter("retained_display_read", None);
            let retained = self
                .product_retained_rows(&records, &missing, reference_at, limits, context)
                .await?;
            for instrument_id in missing {
                let retained_row = retained
                    .iter()
                    .find(|row| row_instrument(row) == Some(instrument_id));
                let row_index = rows
                    .iter()
                    .position(|row| row_instrument(row) == Some(instrument_id));
                let mut row =
                    if let Some(retained) = retained_row.filter(|row| has_current_price(row)) {
                        retained.clone()
                    } else {
                        row_index
                            .map(|index| rows[index].clone())
                            .or_else(|| retained_row.cloned())
                            .unwrap_or_else(|| {
                                json!({"instrumentId": instrument_id.to_string(),
                            "currentPrice": Value::Null, "availability": "unavailable"})
                            })
                    };
                if !has_current_price(&row) {
                    // Keep the original quote, depth and market clocks when a completed close
                    // supplies the compact card's price. A close never becomes a live quote.
                    if let Some(retained) = retained_row {
                        if row.get("quote").is_none_or(|quote| quote.is_null())
                            || row.get("availability").and_then(Value::as_str)
                                == Some("unavailable")
                        {
                            row = retained.clone();
                        }
                    }
                    let record = records
                        .binary_search_by_key(&instrument_id, |record| {
                            record.definition().instrument_id()
                        })
                        .ok()
                        .and_then(|index| records.get(index))
                        .ok_or(ServiceError::InvalidResult)?;
                    progress.enter("previous_close", Some(instrument_id));
                    let close = match self
                        .previous_close_product_row(record, reference_at, context)
                        .await
                    {
                        Ok(close) => close,
                        Err(ServiceError::Unavailable | ServiceError::Unauthorized) => None,
                        Err(error) => return Err(error),
                    };
                    if let Some(close) = close {
                        row["currentPrice"] = close["currentPrice"].clone();
                        row["availability"] = close["availability"].clone();
                    }
                }
                if let Some(index) = row_index {
                    rows[index] = row;
                } else {
                    rows.push(row);
                }
                progress.fallback_completed += 1;
            }
        }
        // Other instruments may have required retained reads after the actor snapshot.
        // Keep its observation details, but never return an expired observation as current.
        let projected_at = system_timestamp()?;
        for row in &mut rows {
            if row.get("availability").and_then(Value::as_str) != Some("end_of_day")
                && row["currentPrice"]["currentThrough"]
                    .as_str()
                    .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                    .and_then(|value| value.timestamp_nanos_opt())
                    .is_some_and(|until| projected_at.unix_nanos() > until)
            {
                row["currentPrice"] = Value::Null;
                row["availability"] = json!("stale");
            }
        }
        progress.enter("page_projection", None);
        let available = page.available();
        let has_more = page.has_more();
        let content = product::project_product_page(&identities, page, &rows)?;
        ensure_live(context)?;
        if request.name() == "Market.GetCollection" {
            progress.enter("collection_projection", None);
            let collection = collection.ok_or(ServiceError::InvalidResult)?;
            let projected = content
                .get("data")
                .and_then(Value::as_array)
                .ok_or(ServiceError::InvalidResult)?;
            let entries: Vec<_> = collection
                .choices
                .iter()
                .map(|choice| {
                    // Check the complete selected population, not only its displayed page.
                    let unambiguous = identities
                        .iter()
                        .filter(|identity| identity.symbol() == Some(choice.symbol.as_str()))
                        .count()
                        == 1;
                    let market = unambiguous
                        .then(|| {
                            projected.iter().find(|row| {
                                row["identity"]["symbol"].as_str() == Some(choice.symbol.as_str())
                            })
                        })
                        .flatten();
                    json!({"symbol": choice.symbol, "kept": choice.kept, "market": market})
                })
                .collect();
            return progress.finish(
                TypedToolResult::try_new(
                    json!({"revision": collection.revision.to_string(), "entries": entries}),
                    entries.len(),
                    ToolResultMetadata::complete_not_applicable(),
                    limits,
                )
                .map_err(|_| ServiceError::ResourceExhausted),
            );
        }
        progress.finish(product::product_result(
            content, available, has_more, limits,
        ))
    }

    /// Completed daily closes are presentation evidence, never current mark authority.
    async fn previous_close_product_row(
        &self,
        record: &MarketDataInstrumentRecord,
        reference_at: Timestamp,
        context: &RequestContext,
    ) -> Result<Option<Value>, ServiceError> {
        ensure_live(context)?;
        let instrument_id = record.definition().instrument_id();
        let Some(close) = self
            .market_history
            .read_latest_previous_close(
                &self.product_research,
                instrument_id,
                reference_at,
                context,
            )
            .await?
        else {
            return Ok(None);
        };
        ensure_live(context)?;
        if close.instrument_id() != instrument_id
            || close.currency() != record.definition().quote_currency()
            || close.session_close() > reference_at
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Some(json!({
            "instrumentId": instrument_id.to_string(), "availability": "end_of_day",
            "currentPrice": {"value": close.close().amount().normalize().to_string(),
                "currency": close.currency().as_str(),
                "observedAt": timestamp_value(close.session_close()),
                "currentThrough": timestamp_value(close.session_close())},
        })))
    }

    async fn product_display_rows(
        &self,
        records: &[MarketDataInstrumentRecord],
        instrument_ids: &[InstrumentId],
        _reference_at: Timestamp,
        limits: ServiceLimits,
        context: &RequestContext,
    ) -> Result<Vec<Value>, ServiceError> {
        let mut progress = ProductReadProgress::new("market_display_rows", context);
        progress.enter("display_filters", None);
        let filters = MarketFilters {
            instruments: instrument_ids.to_vec(),
            sources: Vec::new(),
            time_range: None,
        };
        progress.enter("registry_snapshots", None);
        let snapshots = self
            .registry
            .snapshots(context.deadline(), context.cancellation())
            .await?;
        progress.enter("stream_collection", None);
        let mut streams = collect_streams(&snapshots, &filters, context)?;
        let durable = DurableMarketEvidenceSet::default();
        progress.enter("display_instrument_ids", None);
        let display_ids =
            load_display_instrument_ids(self.registry.as_ref(), &filters, context).await?;
        progress.enter("kraken_price_projections", None);
        let mut kraken = load_kraken_price_projections(
            self.registry.as_ref(),
            instrument_ids,
            &filters,
            context,
        )
        .await?;
        progress.enter("instrument_definitions", None);
        let mut tick_ids = streams
            .iter()
            .map(|view| view.route.route().instrument())
            .chain(kraken.iter().map(|view| view.key().instrument_id()))
            .collect::<Vec<_>>();
        tick_ids.sort_unstable();
        tick_ids.dedup();
        let definitions = if tick_ids.is_empty() {
            Vec::new()
        } else {
            self.instrument_definitions
                .latest(
                    &tick_ids,
                    product::MAXIMUM_PRODUCT_MARKET_ROWS,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(map_catalog_error)?
        };
        let executable = |id| {
            definitions
                .iter()
                .any(|definition| definition.instrument_id() == id)
        };
        streams.retain(|view| executable(view.route.route().instrument()));
        kraken.retain(|view| executable(view.key().instrument_id()));
        let kraken_refs = kraken_projection_refs(&kraken)?;
        progress.enter("order_level_snapshots", None);
        let order_level =
            load_order_level_snapshots(self.registry.as_ref(), &streams, &kraken, context).await?;
        // Current presentation is selected when the actor handles the read, after slower
        // catalog preparation. Historical queries above retain the original request cutoff.
        progress.enter("display_snapshots", None);
        let mut display_batches = Vec::new();
        let maximum_sources = NonZeroUsize::new(MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT)
            .ok_or(ServiceError::Internal)?;
        for instrument in &display_ids {
            match self
                .registry
                .display_snapshots_for_instrument(
                    *instrument,
                    maximum_sources,
                    DisplayMarketReadTime::LatestDisplay,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
            {
                Ok(batch) => display_batches.push(batch),
                Err(ServiceError::Unavailable | ServiceError::Unauthorized) => {}
                Err(error) => return Err(error),
            }
        }
        progress.enter("display_projection", None);
        let mut display = display_snapshot_refs(&display_batches, &filters)?;
        display.retain(|snapshot| {
            records
                .binary_search_by_key(&snapshot.lease().key().instrument_id(), |record| {
                    record.definition().instrument_id()
                })
                .ok()
                .and_then(|index| records.get(index))
                .is_some_and(|record| snapshot.matches_definition_record(record))
        });
        let display_selected_at = system_timestamp()?;
        let policies = build_surface_policies(
            &snapshots,
            &display,
            &kraken_refs,
            &durable,
            display_selected_at,
            presentation_surface_operations()?,
        )?;
        let page_records = records
            .iter()
            .filter(|record| {
                instrument_ids
                    .binary_search(&record.definition().instrument_id())
                    .is_ok()
            })
            .cloned()
            .collect::<Vec<_>>();
        let result = build_market_overview_result(
            &streams,
            &filters,
            &definitions,
            &page_records,
            &display,
            &kraken_refs,
            &policies,
            &order_level,
            &durable,
            display_selected_at,
            snapshots.failures().is_empty() && durable.complete_for(&streams),
            limits,
            context,
        )?;
        let rows = result
            .structured_content()
            .as_array()
            .cloned()
            .ok_or(ServiceError::InvalidResult);
        progress.finish(rows)
    }

    async fn product_retained_rows(
        &self,
        records: &[MarketDataInstrumentRecord],
        instruments: &[InstrumentId],
        reference_at: Timestamp,
        limits: ServiceLimits,
        context: &RequestContext,
    ) -> Result<Vec<Value>, ServiceError> {
        let mut progress = ProductReadProgress::new("retained_market_display", context);
        progress.enter("retained_routes_and_events", None);
        let durable = load_retained_display_evidence(
            &self.product_research,
            records,
            instruments,
            reference_at,
            context,
        )
        .await?;
        let mut tick_ids = durable
            .routes
            .iter()
            .filter(|route| {
                route
                    .selections
                    .iter()
                    .flat_map(|receipt| receipt.selection().sources())
                    .flat_map(|source| source.tied_candidates())
                    .any(|candidate| {
                        matches!(
                            candidate.event(),
                            MarketEvent::Quote(_)
                                | MarketEvent::Trade(_)
                                | MarketEvent::BookSnapshot(_)
                                | MarketEvent::BookDelta(_)
                        )
                    })
            })
            .map(|route| route.instrument_id)
            .collect::<Vec<_>>();
        tick_ids.sort_unstable();
        tick_ids.dedup();
        progress.enter("retained_execution_terms", None);
        let definitions = if tick_ids.is_empty() {
            Vec::new()
        } else {
            self.instrument_definitions
                .latest(
                    &tick_ids,
                    product::MAXIMUM_PRODUCT_MARKET_ROWS,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(map_catalog_error)?
        };
        let mut policies = Vec::new();
        let selected_at = system_timestamp()?;
        let operations = presentation_surface_operations()?;
        for route in &durable.routes {
            for asset_class in route.metadata.coverage().asset_classes() {
                push_surface_policy(
                    &mut policies,
                    &route.surface_id,
                    &route.metadata,
                    *asset_class,
                    operations,
                    surface_rights(&route.metadata, operations, selected_at)?,
                )?;
            }
            if route
                .display_authorizations
                .iter()
                .any(|authorization| selected_at >= authorization.expires_at())
            {
                return Err(ServiceError::Unauthorized);
            }
        }
        let page_records = records
            .iter()
            .filter(|record| {
                instruments
                    .binary_search(&record.definition().instrument_id())
                    .is_ok()
            })
            .cloned()
            .collect::<Vec<_>>();
        progress.enter("retained_projection", None);
        let result = build_market_overview_result(
            &[],
            &MarketFilters {
                instruments: instruments.to_vec(),
                sources: Vec::new(),
                time_range: None,
            },
            &definitions,
            &page_records,
            &[],
            &[],
            &policies,
            &[],
            &durable,
            selected_at,
            durable.complete_for(&[]),
            limits,
            context,
        )?;
        let now = system_timestamp()?;
        if durable
            .routes
            .iter()
            .flat_map(|route| &route.display_authorizations)
            .any(|authorization| now >= authorization.expires_at())
        {
            return Err(ServiceError::Unauthorized);
        }
        progress.finish(
            result
                .structured_content()
                .as_array()
                .cloned()
                .ok_or(ServiceError::InvalidResult),
        )
    }
}

fn row_instrument(row: &Value) -> Option<InstrumentId> {
    row.get("instrumentId")
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
}
fn has_current_price(row: &Value) -> bool {
    row.get("currentPrice")
        .is_some_and(|price| !price.is_null())
        && !matches!(
            row.get("availability").and_then(Value::as_str),
            Some("stale" | "unavailable")
        )
}
