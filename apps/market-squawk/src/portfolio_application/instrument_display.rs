//! Canonical investment display at an immutable portfolio revision's original clocks.

use std::collections::BTreeMap;

use market_squawk_data::{
    MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS, MarketDataInstrumentCatalogError,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentReadCapability,
};
use market_squawk_domain::{InstrumentId, Timestamp};
use market_squawk_services::RequestContext;
use serde_json::{Value, json};

use super::{PortfolioApplicationServiceError, read::check_context};

/// Resolves only emitted page identities; absent historical evidence stays absent for the
/// caller to render as null. The service invokes this catalog read in its blocking worker.
pub(super) fn resolve(
    instruments: Option<&MarketDataInstrumentReadCapability>,
    ids: &[InstrumentId],
    effective_at: Timestamp,
    available_at: Option<Timestamp>,
    context: &RequestContext,
) -> Result<BTreeMap<InstrumentId, Value>, PortfolioApplicationServiceError> {
    check_context(context)?;
    let mut display = BTreeMap::new();
    let (Some(instruments), Some(knowledge_at)) = (instruments, available_at) else {
        return Ok(display);
    };
    for chunk in ids.chunks(MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS) {
        check_context(context)?;
        let mut members = Vec::new();
        members
            .try_reserve_exact(chunk.len())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        members.extend_from_slice(chunk);
        let query =
            MarketDataInstrumentPopulationQuery::try_new(members, knowledge_at, effective_at)
                .map_err(map_catalog_error)?;
        let selection = instruments
            .pin_population_as_of(query, context.deadline(), context.cancellation())
            .map_err(map_catalog_error)?;
        for record in selection.records() {
            check_context(context)?;
            let definition = record.definition();
            let mappings = definition.venue_mappings();
            let symbol = mappings.first().and_then(|first| {
                mappings
                    .iter()
                    .all(|mapping| mapping.venue_symbol() == first.venue_symbol())
                    .then(|| first.venue_symbol().as_str())
            });
            display.insert(
                definition.instrument_id(),
                json!({
                    "name": definition.display_name().map(|name| name.as_str()),
                    "symbol": symbol,
                }),
            );
        }
    }
    check_context(context)?;
    Ok(display)
}

fn map_catalog_error(error: MarketDataInstrumentCatalogError) -> PortfolioApplicationServiceError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => PortfolioApplicationServiceError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => {
            PortfolioApplicationServiceError::DeadlineExceeded
        }
        MarketDataInstrumentCatalogError::ResultByteLimitExceeded => {
            PortfolioApplicationServiceError::ResourceExhausted
        }
        MarketDataInstrumentCatalogError::InvalidInput
        | MarketDataInstrumentCatalogError::InvalidPopulationQuery
        | MarketDataInstrumentCatalogError::InvalidLimit => {
            PortfolioApplicationServiceError::InvalidRequest
        }
        MarketDataInstrumentCatalogError::CorruptCatalog => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        _ => PortfolioApplicationServiceError::Authority,
    }
}
