//! Catalog-backed product discovery, durable marks and qualified SnapshotDisplay fallback.

use super::*;
use crate::application::market_selection::{
    MarketInvestmentReadReceipt, product::MarketProductSelectionReadCapability,
};
use crate::application::research::{map_catalog_error, map_durable_market_ingest_error};
use market_squawk_services::ServiceLimits;
use serde_json::json;

impl MarketDomainService {
    pub(super) async fn call_product(
        &self,
        request: &TypedToolRequest,
        reference_at: Timestamp,
        limits: ServiceLimits,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let collection = if matches!(request.name(), MARKET_GET_OVERVIEW | "Market.GetCollection") {
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
            return TypedToolResult::try_new(
                json!({"revision": collection.revision.to_string(), "entries": entries}),
                entries.len(),
                ToolResultMetadata::complete_not_applicable(),
                limits,
            )
            .map_err(|_| ServiceError::ResourceExhausted);
        }
        let selections = MarketProductSelectionReadCapability::new(
            Arc::clone(&self.product_research),
            self.market_data_instruments.clone(),
        );
        let records = selections
            .population(reference_at, context.deadline(), context.cancellation())
            .await?;
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
                if request.name() == "Market.GetCollection" {
                    choice.is_some()
                } else {
                    choice.is_none_or(|choice| choice.kept)
                }
            });
        }
        let maximum_rows = limits
            .maximum_result_items()
            .min(product::MAXIMUM_PRODUCT_MARKET_ROWS);
        if request.name() == MARKET_SEARCH_UNIVERSE {
            let (content, available, has_more) = product::product_search_page(
                &identities,
                argument("query").ok_or(ServiceError::InvalidRequest)?,
                maximum_rows,
                argument("pageToken"),
            )?;
            ensure_live(context)?;
            return product::product_result(content, available, has_more, limits);
        }
        if request.name() == MARKET_GET_HISTORY {
            let token = argument("historyToken").ok_or(ServiceError::InvalidRequest)?;
            let instrument_id = product::resolve_history_token(&identities, token)?;
            return history::build_product_market_history_result(
                &self.market_history,
                &self.product_research,
                instrument_id,
                token,
                request,
                limits,
                context,
            )
            .await;
        }
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
        let mut rows = Vec::new();
        let mut missing = Vec::new();
        rows.try_reserve_exact(page.instrument_ids().len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        missing
            .try_reserve_exact(page.instrument_ids().len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for instrument_id in page.instrument_ids() {
            ensure_live(context)?;
            let record = records
                .binary_search_by_key(instrument_id, |record| record.definition().instrument_id())
                .ok()
                .and_then(|index| records.get(index))
                .ok_or(ServiceError::InvalidResult)?;
            let receipt = match self
                .product_markets
                .read(
                    *instrument_id,
                    reference_at,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
            {
                Ok(receipt) => receipt,
                Err(ServiceError::Unavailable | ServiceError::Unauthorized) => None,
                Err(error) => return Err(error),
            };
            let row = receipt
                .as_ref()
                .map(|receipt| self.durable_product_row(receipt, record, reference_at, context))
                .transpose()?
                .flatten();
            if let Some(row) = row {
                rows.push(row);
            } else {
                missing.push(*instrument_id);
            }
        }
        // SnapshotDisplay never issues an analytical receipt or sizing authority.
        if !missing.is_empty() {
            missing.sort_unstable();
            let display = match self
                .product_display_rows(&records, &missing, reference_at, limits, context)
                .await
            {
                Ok(rows) => rows,
                Err(ServiceError::Unavailable | ServiceError::Unauthorized) => Vec::new(),
                Err(error) => return Err(error),
            };
            for instrument_id in missing {
                let selected = display.iter().find(|row| {
                    row.get("instrumentId")
                        .and_then(Value::as_str)
                        .and_then(|id| id.parse::<InstrumentId>().ok())
                        == Some(instrument_id)
                });
                let selected = selected.filter(|row| {
                    row.get("currentPrice")
                        .is_some_and(|price| !price.is_null())
                        && !matches!(
                            row.get("availability").and_then(Value::as_str),
                            Some("stale" | "unavailable")
                        )
                });
                let row = if let Some(selected) = selected {
                    Some(selected.clone())
                } else {
                    let record = records
                        .binary_search_by_key(&instrument_id, |record| {
                            record.definition().instrument_id()
                        })
                        .ok()
                        .and_then(|index| records.get(index))
                        .ok_or(ServiceError::InvalidResult)?;
                    match self
                        .previous_close_product_row(record, reference_at, context)
                        .await
                    {
                        Ok(row) => row,
                        Err(ServiceError::Unavailable | ServiceError::Unauthorized) => None,
                        Err(error) => return Err(error),
                    }
                };
                rows.push(row.unwrap_or_else(|| json!({
                    "instrumentId": instrument_id.to_string(), "currentPrice": Value::Null, "availability": "unavailable",
                })));
            }
        }
        let available = page.available();
        let has_more = page.has_more();
        let content = product::project_product_page(&identities, page, &rows)?;
        ensure_live(context)?;
        if request.name() == "Market.GetCollection" {
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
            return TypedToolResult::try_new(
                json!({"revision": collection.revision.to_string(), "entries": entries}),
                entries.len(),
                ToolResultMetadata::complete_not_applicable(),
                limits,
            )
            .map_err(|_| ServiceError::ResourceExhausted);
        }
        product::product_result(content, available, has_more, limits)
    }

    fn durable_product_row(
        &self,
        receipt: &MarketInvestmentReadReceipt,
        record: &MarketDataInstrumentRecord,
        reference_at: Timestamp,
        context: &RequestContext,
    ) -> Result<Option<Value>, ServiceError> {
        let [selected_definition] = receipt.market_definitions().records() else {
            return Err(ServiceError::InvalidResult);
        };
        if selected_definition.revision_digest() != record.revision_digest()
            || receipt.instrument_id() != record.definition().instrument_id()
            || receipt.currency() != record.definition().quote_currency()
        {
            return Err(ServiceError::InvalidResult);
        }
        let observation = receipt
            .observation()
            .map_err(|_| ServiceError::InvalidResult)?;
        let mark = observation.mark();
        let now = system_timestamp()?;
        if mark.fresh_until().is_none_or(|until| until < now) {
            return Ok(None);
        }
        let event = receipt.event().map_err(|_| ServiceError::InvalidResult)?;
        let binding = market_event_provenance(event).binding();
        let metadata = self
            .product_research
            .analytical()
            .retained_source_metadata(
                binding.source_id(),
                binding.metadata_revision(),
                reference_at,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_durable_market_ingest_error)?
            .ok_or(ServiceError::InvalidResult)?;
        if metadata.source_id() != binding.source_id()
            || metadata.revision() != binding.metadata_revision()
            || !metadata.is_effective_at(reference_at)
        {
            return Err(ServiceError::InvalidResult);
        }
        let availability = match metadata.coverage().delay() {
            CoverageDelay::RealTime => "live",
            CoverageDelay::Delayed(_) => "delayed",
            CoverageDelay::NotApplicable => "stored",
            CoverageDelay::Unknown => "unavailable",
        };
        Ok(Some(json!({
            "instrumentId": receipt.instrument_id().to_string(), "availability": availability,
            "currentPrice": {"value": mark.value().normalize().to_string(), "currency": mark.currency().as_str(),
                "observedAt": timestamp_value(observation.timestamps().effective_at())},
        })))
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
                "currency": close.currency().as_str(), "currentThrough": timestamp_value(close.session_close())},
        })))
    }

    async fn product_display_rows(
        &self,
        records: &[MarketDataInstrumentRecord],
        instrument_ids: &[InstrumentId],
        reference_at: Timestamp,
        limits: ServiceLimits,
        context: &RequestContext,
    ) -> Result<Vec<Value>, ServiceError> {
        let filters = MarketFilters {
            instruments: instrument_ids.to_vec(),
            sources: Vec::new(),
            time_range: None,
        };
        let snapshots = self
            .registry
            .snapshots(context.deadline(), context.cancellation())
            .await?;
        let mut streams = collect_streams(&snapshots, &filters, context)?;
        let mut durable =
            load_durable_market_evidence(self.registry.as_ref(), &filters, reference_at, context)
                .await?;
        let display_ids =
            load_display_instrument_ids(self.registry.as_ref(), &filters, context).await?;
        let mut kraken = load_kraken_price_projections(
            self.registry.as_ref(),
            instrument_ids,
            &filters,
            context,
        )
        .await?;
        let definitions = self
            .instrument_definitions
            .latest(
                instrument_ids,
                product::MAXIMUM_PRODUCT_MARKET_ROWS,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_catalog_error)?;
        let executable = |id| {
            definitions
                .iter()
                .any(|definition| definition.instrument_id() == id)
        };
        streams.retain(|view| executable(view.route.route().instrument()));
        kraken.retain(|view| executable(view.key().instrument_id()));
        // Native Money is handled by its exact neutral receipt above. Only legacy tick events
        // enter the original presentation conversion with their genuine execution definition.
        durable.routes.retain(|route| {
            executable(route.instrument_id)
                && route.selections.iter().all(|receipt| {
                    receipt
                        .selection()
                        .sources()
                        .iter()
                        .flat_map(|source| source.tied_candidates())
                        .all(|candidate| {
                            !matches!(
                                candidate.event(),
                                MarketEvent::MarketDataQuote(_) | MarketEvent::MarketDataTrade(_)
                            )
                        })
                })
        });
        let kraken_refs = kraken_projection_refs(&kraken)?;
        let order_level =
            load_order_level_snapshots(self.registry.as_ref(), &streams, &kraken, context).await?;
        // Current presentation is selected when the actor handles the read, after slower
        // catalog preparation. Historical queries above retain the original request cutoff.
        let display_batches = load_display_snapshots(
            self.registry.as_ref(),
            &display_ids,
            DisplayMarketReadTime::LatestDisplay,
            context,
        )
        .await?;
        let display = display_snapshot_refs(&display_batches, &filters)?;
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
        result
            .structured_content()
            .as_array()
            .cloned()
            .ok_or(ServiceError::InvalidResult)
    }
}
