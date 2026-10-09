//! Shared canonical identities for the existing Markets product tokens.

use std::{fmt::Write as _, sync::Arc, time::Instant};

use market_squawk_data::{
    MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentReadCapability,
    MarketDataInstrumentRecord,
};
use market_squawk_domain::{AssetClass, DigestAlgorithm, EvidenceDigest, InstrumentId, Timestamp};
use market_squawk_services::ServiceError;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::domain_support::{opaque_product_text_token, try_boxed_product_text};
use crate::application::research::map_market_definition_read_error;

/// Resolves genuine revision-bound product tokens across the admitted canonical catalog.
///
/// This capability owns no token registry or hot snapshot. Display subscriptions do not constrain
/// discovery; retained investment references reopen independently at their original cutoff.
#[derive(Clone, Debug)]
pub(crate) struct MarketProductSelectionReadCapability {
    research: Arc<crate::ResearchService>,
    market_definitions: MarketDataInstrumentReadCapability,
}

impl MarketProductSelectionReadCapability {
    pub(crate) const fn new(
        research: Arc<crate::ResearchService>,
        market_definitions: MarketDataInstrumentReadCapability,
    ) -> Self {
        Self {
            research,
            market_definitions,
        }
    }

    pub(crate) async fn resolve(
        &self,
        selection_token: &str,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentId, ServiceError> {
        self.selection_record(selection_token, as_of, deadline, cancellation)
            .await
            .map(|record| record.definition().instrument_id())
    }

    /// Reopens only the selected investment, retaining the original cutoff checks.
    pub(crate) async fn selection_record(
        &self,
        selection_token: &str,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, ServiceError> {
        if selection_token.len() > MAXIMUM_TOKEN_BYTES {
            return Err(ServiceError::InvalidRequest);
        }
        let token = try_boxed_product_text(selection_token, MAXIMUM_TOKEN_BYTES)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let market_definitions = self.market_definitions.clone();
        self.research
            .run_owned_research_read(deadline, cancellation, move |operation_cancellation| {
                Self::selection_record_owned(
                    &market_definitions,
                    &token,
                    as_of,
                    deadline,
                    &operation_cancellation,
                )
            })
            .await
            .map_err(map_owned_read_error)?
    }

    /// History locators bind the same immutable definition as the investment selection.
    pub(crate) async fn history_record(
        &self,
        history_token: &str,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, ServiceError> {
        if history_token.len() > MAXIMUM_TOKEN_BYTES {
            return Err(ServiceError::InvalidRequest);
        }
        let token = try_boxed_product_text(history_token, MAXIMUM_TOKEN_BYTES)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let market_definitions = self.market_definitions.clone();
        self.research
            .run_owned_research_read(deadline, cancellation, move |operation_cancellation| {
                Self::history_record_owned(
                    &market_definitions,
                    &token,
                    as_of,
                    deadline,
                    &operation_cancellation,
                )
            })
            .await
            .map_err(map_owned_read_error)?
    }

    /// Creates a fresh product locator from a canonical identity, without interpreting an
    /// earlier display token. Historical analysis receipts keep their original locator.
    pub(crate) async fn token_for_instrument(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Box<str>, ServiceError> {
        let reader = self.market_definitions.clone();
        self.research
            .run_owned_research_read(deadline, cancellation, move |operation_cancellation| {
                let record = selected_record(
                    &reader,
                    instrument_id,
                    as_of,
                    deadline,
                    &operation_cancellation,
                )?;
                individual_selection_token(&record)
            })
            .await
            .map_err(map_owned_read_error)?
    }

    /// Collection symbols are display preferences, never a substitute for unique catalog identity.
    pub(crate) async fn collection_records(
        &self,
        symbols: Vec<String>,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<MarketDataInstrumentRecord>, ServiceError> {
        if as_of.unix_nanos() <= 0
            || symbols.len() > crate::application::market_collection::STARTER_MARKET_SYMBOLS.len()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let reader = self.market_definitions.clone();
        self.research
            .run_owned_research_read(deadline, cancellation, move |operation_cancellation| {
                let mut records = Vec::new();
                records
                    .try_reserve_exact(symbols.len())
                    .map_err(|_| ServiceError::ResourceExhausted)?;
                let mut retained = 0usize;
                for symbol in symbols {
                    if let Some(record) = reader
                        .unique_display_symbol_as_of(
                            &symbol,
                            as_of,
                            as_of,
                            deadline,
                            &operation_cancellation,
                        )
                        .map_err(map_market_definition_read_error)?
                    {
                        retained = retained
                            .checked_add(
                                record
                                    .retained_bytes()
                                    .map_err(map_market_definition_read_error)?
                                    .checked_add(1024)
                                    .ok_or(ServiceError::ResourceExhausted)?,
                            )
                            .filter(|bytes| *bytes <= MAXIMUM_PRODUCT_POPULATION_BYTES)
                            .ok_or(ServiceError::ResourceExhausted)?;
                        records.push(record);
                    }
                }
                records.sort_unstable_by_key(|record| record.definition().instrument_id());
                records.dedup_by_key(|record| record.definition().instrument_id());
                Ok(records)
            })
            .await
            .map_err(map_owned_read_error)?
    }

    /// Reads the admitted canonical population at one cutoff independently of active feeds.
    pub(crate) async fn population(
        &self,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<MarketDataInstrumentRecord>, ServiceError> {
        let reader = self.market_definitions.clone();
        self.research
            .run_owned_research_read(deadline, cancellation, move |operation_cancellation| {
                if as_of.unix_nanos() <= 0 {
                    return Err(ServiceError::InvalidRequest);
                }
                let mut records = Vec::new();
                let mut cursor = None;
                let mut scanned = 0usize;
                let mut retained = 0usize;
                loop {
                    let page = reader
                        .enumerate_as_of(
                            as_of,
                            as_of,
                            cursor.as_ref(),
                            MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS,
                            deadline,
                            &operation_cancellation,
                        )
                        .map_err(map_market_definition_read_error)?;
                    scanned = scanned
                        .checked_add(page.instrument_ids().len())
                        .filter(|count| *count <= MAXIMUM_PRODUCT_MARKET_POPULATION)
                        .ok_or(ServiceError::ResourceExhausted)?;
                    records
                        .try_reserve_exact(page.records().len())
                        .map_err(|_| ServiceError::ResourceExhausted)?;
                    for record in page.records() {
                        retained = retained
                            .checked_add(
                                record
                                    .retained_bytes()
                                    .map_err(map_market_definition_read_error)?
                                    .checked_add(1024)
                                    .ok_or(ServiceError::ResourceExhausted)?,
                            )
                            .filter(|bytes| *bytes <= MAXIMUM_PRODUCT_POPULATION_BYTES)
                            .ok_or(ServiceError::ResourceExhausted)?;
                        records.push(record.clone());
                    }
                    if page.complete() {
                        break;
                    }
                    if scanned == MAXIMUM_PRODUCT_MARKET_POPULATION {
                        return Err(ServiceError::ResourceExhausted);
                    }
                    cursor = page.next_cursor().cloned();
                }
                if records.windows(2).any(|pair| {
                    pair[0].definition().instrument_id() >= pair[1].definition().instrument_id()
                }) {
                    return Err(ServiceError::InvalidResult);
                }
                Ok(records)
            })
            .await
            .map_err(map_owned_read_error)?
    }

    pub(crate) fn selection_record_owned(
        market_definitions: &MarketDataInstrumentReadCapability,
        selection_token: &str,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, ServiceError> {
        record_for_token_owned(
            market_definitions,
            selection_token,
            MARKET_TOKEN_PREFIX,
            as_of,
            deadline,
            cancellation,
        )
    }

    pub(crate) fn history_record_owned(
        market_definitions: &MarketDataInstrumentReadCapability,
        history_token: &str,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, ServiceError> {
        record_for_token_owned(
            market_definitions,
            history_token,
            HISTORY_TOKEN_PREFIX,
            as_of,
            deadline,
            cancellation,
        )
    }
}

fn record_for_token_owned(
    market_definitions: &MarketDataInstrumentReadCapability,
    token: &str,
    prefix: &str,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<MarketDataInstrumentRecord, ServiceError> {
    if token.len() > MAXIMUM_TOKEN_BYTES || as_of.unix_nanos() <= 0 {
        return Err(ServiceError::InvalidRequest);
    }
    let revision = token_revision(token, prefix)?;
    let record = market_definitions
        .read_revision(revision, deadline, cancellation)
        .map_err(map_market_definition_read_error)?
        .ok_or(ServiceError::Unavailable)?;
    // The digest is only a locator. Its original revision must still be the unique
    // knowable and effective selection for this instrument at the caller's cutoff.
    let current = selected_record(
        market_definitions,
        record.definition().instrument_id(),
        as_of,
        deadline,
        cancellation,
    )?;
    if current != record {
        return Err(ServiceError::Unavailable);
    }
    Ok(record)
}

fn selected_record(
    reader: &MarketDataInstrumentReadCapability,
    instrument_id: InstrumentId,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<MarketDataInstrumentRecord, ServiceError> {
    if as_of.unix_nanos() <= 0 {
        return Err(ServiceError::InvalidRequest);
    }
    let population = reader
        .pin_population_as_of(
            MarketDataInstrumentPopulationQuery::try_new(vec![instrument_id], as_of, as_of)
                .map_err(map_market_definition_read_error)?,
            deadline,
            cancellation,
        )
        .map_err(map_market_definition_read_error)?;
    if population.disposition() != MarketDataInstrumentPopulationDisposition::Complete
        || !population.exclusions().is_empty()
    {
        return Err(ServiceError::Unavailable);
    }
    let [record] = population.records() else {
        return Err(ServiceError::InvalidResult);
    };
    Ok(record.clone())
}

const MARKET_TOKEN_PREFIX: &str = "market_";
const HISTORY_TOKEN_PREFIX: &str = "history_";
const MARKET_REVISION_HEX_BYTES: usize = 64;
const MAXIMUM_TOKEN_BYTES: usize = 96;
const MAXIMUM_PRODUCT_POPULATION_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAXIMUM_PRODUCT_MARKET_POPULATION: usize =
    market_squawk_data::MAX_CURRENT_LISTED_POPULATION_MEMBERS;

#[derive(Debug)]
pub(crate) struct ProductMarketIdentity {
    instrument_id: InstrumentId,
    selection_token: Box<str>,
    history_token: Box<str>,
    name: Box<str>,
    symbol: Option<Box<str>>,
    matches_search_query: bool,
    asset_class: &'static str,
    population_binding: [u8; 32],
}

impl ProductMarketIdentity {
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) fn selection_token(&self) -> &str {
        &self.selection_token
    }

    pub(crate) fn history_token(&self) -> &str {
        &self.history_token
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }

    pub(crate) const fn matches_search_query(&self) -> bool {
        self.matches_search_query
    }

    pub(crate) const fn asset_class(&self) -> &'static str {
        self.asset_class
    }

    pub(crate) const fn population_binding(&self) -> &[u8; 32] {
        &self.population_binding
    }
}

/// Builds the complete bounded token population and rejects every ambiguity or collision.
pub(crate) fn product_market_identities(
    market_data: &[MarketDataInstrumentRecord],
    effective_at: Timestamp,
    query: Option<&str>,
) -> Result<Vec<ProductMarketIdentity>, ServiceError> {
    if market_data.len() > MAXIMUM_PRODUCT_MARKET_POPULATION
        || market_data.windows(2).any(|pair| {
            pair[0].definition().instrument_id() >= pair[1].definition().instrument_id()
        })
    {
        return Err(ServiceError::Unavailable);
    }
    let population_binding = population_binding(market_data)?;
    let mut identities = Vec::new();
    identities
        .try_reserve_exact(market_data.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let mut selection_tokens = std::collections::BTreeSet::new();
    let mut history_tokens = std::collections::BTreeSet::new();
    for record in market_data {
        let definition = record.definition();
        let symbol = record.display_symbol_at(effective_at);
        let selection_token = individual_selection_token(record)?;
        let history_token = revision_token(HISTORY_TOKEN_PREFIX, record)?;
        if !selection_tokens.insert(selection_token.clone())
            || !history_tokens.insert(history_token.clone())
        {
            return Err(ServiceError::InvalidResult);
        }
        identities.push(ProductMarketIdentity {
            instrument_id: definition.instrument_id(),
            selection_token,
            history_token,
            symbol: symbol
                .map(|symbol| try_boxed_product_text(symbol, 256))
                .transpose()
                .map_err(|_| ServiceError::ResourceExhausted)?,
            matches_search_query: match query.map(str::trim).filter(|query| !query.is_empty()) {
                Some(query) => record
                    .matches_search_query_at(query, effective_at)
                    .map_err(map_market_definition_read_error)?,
                None => true,
            },
            // Display names are optional reference enrichment. Their absence must not
            // reject this identity or unrelated investments in the same population.
            name: try_boxed_product_text(
                definition
                    .display_name()
                    .map(|name| name.as_str())
                    .or(symbol)
                    .unwrap_or("Investment name unavailable"),
                256,
            )
            .map_err(|_| ServiceError::ResourceExhausted)?,
            asset_class: product_asset_class(definition.asset_class()),
            population_binding,
        });
    }
    identities.sort_unstable_by(|left, right| left.selection_token.cmp(&right.selection_token));
    Ok(identities)
}

/// The ordinary product token locates its original immutable canonical definition and evidence.
/// It grants no currentness or financial authority; readers validate the exact selected revision.
pub(crate) fn individual_selection_token(
    record: &MarketDataInstrumentRecord,
) -> Result<Box<str>, ServiceError> {
    revision_token(MARKET_TOKEN_PREFIX, record)
}

fn revision_token(
    prefix: &str,
    record: &MarketDataInstrumentRecord,
) -> Result<Box<str>, ServiceError> {
    let mut token = String::new();
    token
        .try_reserve_exact(prefix.len() + MARKET_REVISION_HEX_BYTES)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    token.push_str(prefix);
    for byte in record.revision_digest().bytes() {
        write!(token, "{byte:02x}").map_err(|_| ServiceError::ResourceExhausted)?;
    }
    Ok(token.into_boxed_str())
}

fn token_revision(selection_token: &str, prefix: &str) -> Result<EvidenceDigest, ServiceError> {
    let encoded = selection_token
        .strip_prefix(prefix)
        .filter(|encoded| {
            encoded.len() == MARKET_REVISION_HEX_BYTES
                && encoded
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or(ServiceError::InvalidRequest)?;
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
            .map_err(|_| ServiceError::InvalidRequest)?;
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

pub(crate) fn token(
    prefix: &str,
    domain: &[u8],
    components: &[&[u8]],
) -> Result<Box<str>, ServiceError> {
    opaque_product_text_token(prefix, domain, components, MAXIMUM_TOKEN_BYTES)
        .map_err(|_error| ServiceError::ResourceExhausted)
}

fn population_binding(
    market_data: &[MarketDataInstrumentRecord],
) -> Result<[u8; 32], ServiceError> {
    let mut digest = Sha256::new();
    for record in market_data {
        digest.update(record.definition().instrument_id().as_uuid().as_bytes());
        digest.update(record.revision_digest().bytes());
    }
    Ok(digest.finalize().into())
}

fn product_asset_class(asset_class: AssetClass) -> &'static str {
    match asset_class {
        AssetClass::Equity => "equity",
        AssetClass::FixedIncome => "fixed_income",
        AssetClass::Option => "option",
        AssetClass::Future => "future",
        AssetClass::ForeignExchange => "foreign_exchange",
        AssetClass::Crypto => "crypto",
        AssetClass::Commodity => "commodity",
        AssetClass::Fund => "fund",
        AssetClass::Index => "index",
        AssetClass::Cash => "cash",
    }
}

fn map_owned_read_error(error: crate::ResearchServiceError) -> ServiceError {
    match error {
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::Cancelled) => {
            ServiceError::Cancelled
        }
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::DeadlineExceeded) => {
            ServiceError::DeadlineExceeded
        }
        // Domain results are the worker's nested output; this outer failure means the
        // owned lane could not be admitted/joined or failed to return its result.
        _ => ServiceError::Internal,
    }
}
