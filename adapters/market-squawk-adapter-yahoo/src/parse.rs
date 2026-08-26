use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use rust_decimal::Decimal;
use serde_json::{Map, Value};

use crate::{
    AdapterBounds, EvidenceAuthority, PINNED_YFINANCE_COMMIT, PINNED_YFINANCE_VERSION,
    ParseContext, ProviderField, QualityIssue, YAHOO_FINANCE_EXPERIMENTAL, YahooAdapterError,
    YahooBar, YahooChart, YahooChartEvent, YahooChartEventKind, YahooEnrichment,
    YahooEnrichmentState, YahooFundData, YahooFundHolding, YahooHttpRequest, YahooLookupHint,
    YahooOptionChain, YahooOptionContract, YahooOptionSide, YahooProvenance, YahooQuote,
    YahooReference, YahooRequestFamily, YahooReturnedDisposition, YahooSymbol,
};

pub fn parse_quote_response(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    bytes: &[u8],
) -> Result<YahooReturnedDisposition<YahooQuote>, YahooAdapterError> {
    require_family(request, YahooRequestFamily::Quote)?;
    let root = parse_root(bytes, bounds)?;
    let response = object_path(&root, &["quoteResponse"])?;
    if let Some(error) = response.get("error").filter(|value| !value.is_null()) {
        return quote_provider_error(request, context, bounds, error);
    }
    let results = array_member(response, "result", "quoteResponse.result")?;
    enforce_count(
        "quoteResponse.result",
        results.len(),
        bounds.max_records_per_response,
    )?;

    let requested = request
        .requested_targets
        .iter()
        .map(|target| target.symbol.clone())
        .collect::<Vec<_>>();
    let requested_set = requested.iter().cloned().collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut returned = Vec::new();
    let mut rejected = Vec::new();
    let mut observations = Vec::new();
    reserve(&mut returned, results.len(), "quote returned symbols")?;
    reserve(&mut observations, results.len(), "quote observations")?;

    for (index, value) in results.iter().enumerate() {
        let object = as_object(value, &format!("quoteResponse.result[{index}]"))?;
        let symbol = required_symbol(
            object,
            "symbol",
            bounds,
            &format!("quoteResponse.result[{index}].symbol"),
        )?;
        if !requested_set.contains(&symbol) {
            return Err(YahooAdapterError::UnexpectedSymbol(
                symbol.as_str().to_owned(),
            ));
        }
        if !seen.insert(symbol.clone()) {
            return Err(YahooAdapterError::DuplicateReturnedIdentity(
                symbol.as_str().to_owned(),
            ));
        }
        returned.push(symbol.clone());
        let (quote, provenance, mut issues) = parse_quote_object(
            object,
            symbol.clone(),
            request,
            context,
            bounds,
            "quoteResponse.result",
        )?;
        if quote_type_is_crypto(&quote.quote_type) {
            issues.push(QualityIssue::UnsupportedAsset {
                quote_type: "CRYPTOCURRENCY".to_owned(),
            });
            rejected.push(symbol);
            observations.push(enrichment(None, provenance, issues));
        } else {
            observations.push(enrichment(Some(quote), provenance, issues));
        }
    }

    let mut missing = Vec::new();
    for symbol in &requested {
        if !seen.contains(symbol) {
            missing.push(symbol.clone());
            observations.push(YahooEnrichment {
                state: YahooEnrichmentState::Unavailable,
                authority: EvidenceAuthority::ExperimentalSupplementOnly,
                provenance: empty_provenance(
                    request,
                    context,
                    ProviderField::Value(symbol.clone()),
                ),
                issues: vec![QualityIssue::MissingRequestedSymbol {
                    symbol: symbol.clone(),
                }],
                data: None,
            });
        }
    }
    let valid_observations = observations
        .iter()
        .filter_map(|observation| observation.data.as_ref())
        .filter(|quote| quote_has_market_component(quote))
        .count();
    Ok(YahooReturnedDisposition {
        requested_symbols: requested,
        provider_returned_symbols: returned,
        valid_observations,
        missing_symbols: missing,
        rejected_symbols: rejected,
        observations,
    })
}

pub fn parse_chart_response(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    bytes: &[u8],
) -> Result<YahooEnrichment<YahooChart>, YahooAdapterError> {
    require_family(request, YahooRequestFamily::ChartHistory)?;
    let expected = exactly_one_request_symbol(request)?;
    let root = parse_root(bytes, bounds)?;
    let chart = object_path(&root, &["chart"])?;
    if let Some(error) = chart.get("error").filter(|value| !value.is_null()) {
        return Ok(provider_error_enrichment(
            request,
            context,
            ProviderField::Value(expected),
            bounds,
            error,
        ));
    }
    let results = array_member(chart, "result", "chart.result")?;
    if results.is_empty() {
        return Ok(unavailable(
            empty_provenance(request, context, ProviderField::Value(expected)),
            QualityIssue::EmptyResult,
        ));
    }
    if results.len() != 1 {
        return Err(YahooAdapterError::InvalidSchema {
            path: "chart.result".to_owned(),
            reason: "one symbol-specific chart request must return exactly one result".to_owned(),
        });
    }
    let result = as_object(&results[0], "chart.result[0]")?;
    let meta = member_object(result, "meta", "chart.result[0].meta")?;
    let mut issues = Vec::new();
    let provider_symbol = string_field(meta, "symbol", "chart.meta.symbol", bounds, &mut issues)?
        .map_value(|value| YahooSymbol::parse(value, bounds.max_string_bytes));
    let provider_symbol = transpose_symbol_field(provider_symbol, "chart.meta.symbol")?;
    if let ProviderField::Value(symbol) = &provider_symbol
        && symbol != &expected
    {
        return Err(YahooAdapterError::UnexpectedSymbol(
            symbol.as_str().to_owned(),
        ));
    }
    let instrument_type = string_field(
        meta,
        "instrumentType",
        "chart.meta.instrumentType",
        bounds,
        &mut issues,
    )?;
    if field_string_is_crypto(&instrument_type) {
        issues.push(QualityIssue::UnsupportedAsset {
            quote_type: "CRYPTOCURRENCY".to_owned(),
        });
        return Ok(enrichment(
            None,
            provenance_from_meta(request, context, meta, provider_symbol, bounds, &mut issues)?,
            issues,
        ));
    }

    let timestamps =
        optional_array_member(result, "timestamp", "chart.result[0].timestamp")?.unwrap_or(&[]);
    enforce_count(
        "chart.result[0].timestamp",
        timestamps.len(),
        bounds.max_records_per_response,
    )?;
    let indicators = nested_object_path(result, &["indicators"])?;
    let quote_arrays =
        optional_array_member(indicators, "quote", "chart.result[0].indicators.quote")?
            .unwrap_or(&[]);
    let quote_object = quote_arrays
        .first()
        .map(|value| as_object(value, "chart.result[0].indicators.quote[0]"))
        .transpose()?
        .unwrap_or(&EMPTY_OBJECT);
    let adjclose_object = optional_nested_first_object(
        result,
        &["indicators", "adjclose"],
        "chart.result[0].indicators.adjclose[0]",
    )?
    .unwrap_or(&EMPTY_OBJECT);
    for field in ["open", "high", "low", "close", "volume"] {
        note_array_length(quote_object, field, timestamps.len(), &mut issues)?;
    }
    note_array_length(adjclose_object, "adjclose", timestamps.len(), &mut issues)?;

    let mut bars = Vec::new();
    reserve(&mut bars, timestamps.len(), "chart bars")?;
    let mut seen_timestamps = BTreeSet::new();
    for (index, timestamp) in timestamps.iter().enumerate() {
        let timestamp =
            parse_i64_value(timestamp).ok_or_else(|| YahooAdapterError::InvalidNumber {
                path: format!("chart.result[0].timestamp[{index}]"),
            })?;
        if !seen_timestamps.insert(timestamp) {
            return Err(YahooAdapterError::DuplicateReturnedIdentity(format!(
                "{}:{timestamp}",
                expected.as_str()
            )));
        }
        bars.push(YahooBar {
            timestamp_unix_seconds: timestamp,
            open: indexed_decimal_field(quote_object, "open", index, &mut issues),
            high: indexed_decimal_field(quote_object, "high", index, &mut issues),
            low: indexed_decimal_field(quote_object, "low", index, &mut issues),
            close: indexed_decimal_field(quote_object, "close", index, &mut issues),
            adjusted_close: indexed_decimal_field(adjclose_object, "adjclose", index, &mut issues),
            volume: indexed_u64_field(quote_object, "volume", index, &mut issues),
        });
    }
    let events = parse_chart_events(result, bounds, &mut issues)?;
    if bars.is_empty() && events.is_empty() {
        issues.push(QualityIssue::EmptyResult);
    }
    let valid_ranges = parse_string_array(
        meta.get("validRanges"),
        "chart.meta.validRanges",
        bounds,
        &mut issues,
    )?;
    let regular_market_time_unix_seconds = i64_field(
        meta,
        "regularMarketTime",
        "chart.meta.regularMarketTime",
        &mut issues,
    );
    if !regular_market_time_unix_seconds.is_value() {
        issues.push(QualityIssue::MissingProviderTimestamp);
    }
    let provenance =
        provenance_from_meta(request, context, meta, provider_symbol, bounds, &mut issues)?;
    let valid_bar_count = bars
        .iter()
        .filter(|bar| bar_has_market_component(bar))
        .count();
    if !bars.is_empty() && valid_bar_count == 0 {
        issues.push(QualityIssue::PartialProviderResult);
    }
    let data = YahooChart {
        symbol: expected,
        instrument_type,
        currency: string_field(meta, "currency", "chart.meta.currency", bounds, &mut issues)?,
        data_granularity: string_field(
            meta,
            "dataGranularity",
            "chart.meta.dataGranularity",
            bounds,
            &mut issues,
        )?,
        range: string_field(meta, "range", "chart.meta.range", bounds, &mut issues)?,
        first_trade_time_unix_seconds: i64_field(
            meta,
            "firstTradeDate",
            "chart.meta.firstTradeDate",
            &mut issues,
        ),
        regular_market_time_unix_seconds,
        previous_close: decimal_field(
            meta,
            "previousClose",
            "chart.meta.previousClose",
            &mut issues,
        ),
        chart_previous_close: decimal_field(
            meta,
            "chartPreviousClose",
            "chart.meta.chartPreviousClose",
            &mut issues,
        ),
        valid_ranges,
        valid_bar_count,
        bars,
        events,
    };
    let has_data = data.valid_bar_count > 0 || !data.events.is_empty();
    Ok(enrichment(has_data.then_some(data), provenance, issues))
}

// A shared immutable empty map avoids manufacturing temporary references while optional modules
// are absent. It contains no provider data and is never exposed.
static EMPTY_OBJECT: LazyLock<Map<String, Value>> = LazyLock::new(Map::new);

trait MapProviderField<T> {
    fn map_value<U, F>(self, map: F) -> ProviderField<U>
    where
        F: FnOnce(T) -> U;
}

pub fn parse_reference_response(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    bytes: &[u8],
) -> Result<YahooEnrichment<YahooReference>, YahooAdapterError> {
    require_family(request, YahooRequestFamily::ReferenceSummary)?;
    let expected = exactly_one_request_symbol(request)?;
    let root = parse_root(bytes, bounds)?;
    let summary = object_path(&root, &["quoteSummary"])?;
    if let Some(error) = summary.get("error").filter(|value| !value.is_null()) {
        return Ok(provider_error_enrichment(
            request,
            context,
            ProviderField::Value(expected),
            bounds,
            error,
        ));
    }
    let result = first_summary_result(summary)?;
    let quote_type =
        optional_member_object(result, "quoteType", "quoteSummary.result[0].quoteType")?
            .unwrap_or(&EMPTY_OBJECT);
    let price = optional_member_object(result, "price", "quoteSummary.result[0].price")?
        .unwrap_or(&EMPTY_OBJECT);
    let detail = optional_member_object(
        result,
        "summaryDetail",
        "quoteSummary.result[0].summaryDetail",
    )?
    .unwrap_or(&EMPTY_OBJECT);
    let profile = optional_member_object(
        result,
        "summaryProfile",
        "quoteSummary.result[0].summaryProfile",
    )?
    .unwrap_or(&EMPTY_OBJECT);

    let mut issues = Vec::new();
    let provider_symbol = select_string_field(
        [
            (quote_type, "symbol", "quoteType.symbol"),
            (price, "symbol", "price.symbol"),
        ],
        bounds,
        &mut issues,
    )?
    .map_value(|value| YahooSymbol::parse(value, bounds.max_string_bytes));
    let provider_symbol = transpose_symbol_field(provider_symbol, "quoteSummary.symbol")?;
    if let ProviderField::Value(symbol) = &provider_symbol
        && symbol != &expected
    {
        return Err(YahooAdapterError::UnexpectedSymbol(
            symbol.as_str().to_owned(),
        ));
    }
    let quote_type_field = string_field(
        quote_type,
        "quoteType",
        "quoteType.quoteType",
        bounds,
        &mut issues,
    )?;
    let provenance = provenance_from_summary(
        request,
        context,
        provider_symbol,
        quote_type,
        price,
        profile,
        bounds,
        &mut issues,
    )?;
    if field_string_is_crypto(&quote_type_field) {
        issues.push(QualityIssue::UnsupportedAsset {
            quote_type: "CRYPTOCURRENCY".to_owned(),
        });
        return Ok(enrichment(None, provenance, issues));
    }
    let regular_market_time = select_i64_field(
        [
            (price, "regularMarketTime", "price.regularMarketTime"),
            (
                detail,
                "regularMarketTime",
                "summaryDetail.regularMarketTime",
            ),
        ],
        &mut issues,
    );
    if !regular_market_time.is_value() {
        issues.push(QualityIssue::MissingProviderTimestamp);
    }
    let summary_has_data =
        !quote_type.is_empty() || !price.is_empty() || !detail.is_empty() || !profile.is_empty();
    if !summary_has_data {
        issues.push(QualityIssue::EmptyResult);
    } else if quote_type.is_empty() || price.is_empty() {
        issues.push(QualityIssue::PartialProviderResult);
    }
    let data = YahooReference {
        symbol: expected,
        quote_type: quote_type_field,
        short_name: select_string_field(
            [
                (price, "shortName", "price.shortName"),
                (quote_type, "shortName", "quoteType.shortName"),
            ],
            bounds,
            &mut issues,
        )?,
        long_name: select_string_field(
            [
                (price, "longName", "price.longName"),
                (quote_type, "longName", "quoteType.longName"),
            ],
            bounds,
            &mut issues,
        )?,
        underlying_symbol: string_field(
            quote_type,
            "underlyingSymbol",
            "quoteType.underlyingSymbol",
            bounds,
            &mut issues,
        )?,
        currency: select_string_field(
            [
                (price, "currency", "price.currency"),
                (detail, "currency", "summaryDetail.currency"),
            ],
            bounds,
            &mut issues,
        )?,
        market_state: string_field(
            price,
            "marketState",
            "price.marketState",
            bounds,
            &mut issues,
        )?,
        regular_market_time_unix_seconds: regular_market_time,
        regular_market_price: select_decimal_field(
            [
                (price, "regularMarketPrice", "price.regularMarketPrice"),
                (
                    detail,
                    "regularMarketPrice",
                    "summaryDetail.regularMarketPrice",
                ),
            ],
            &mut issues,
        ),
        nav_price: decimal_field(detail, "navPrice", "summaryDetail.navPrice", &mut issues),
        total_assets: decimal_field(
            detail,
            "totalAssets",
            "summaryDetail.totalAssets",
            &mut issues,
        ),
        category: string_field(
            detail,
            "category",
            "summaryDetail.category",
            bounds,
            &mut issues,
        )?,
        fund_family: string_field(
            detail,
            "fundFamily",
            "summaryDetail.fundFamily",
            bounds,
            &mut issues,
        )?,
        sector: string_field(
            profile,
            "sector",
            "summaryProfile.sector",
            bounds,
            &mut issues,
        )?,
        industry: string_field(
            profile,
            "industry",
            "summaryProfile.industry",
            bounds,
            &mut issues,
        )?,
        website: string_field(
            profile,
            "website",
            "summaryProfile.website",
            bounds,
            &mut issues,
        )?,
        business_summary: string_field(
            profile,
            "longBusinessSummary",
            "summaryProfile.longBusinessSummary",
            bounds,
            &mut issues,
        )?,
    };
    Ok(enrichment(
        summary_has_data.then_some(data),
        provenance,
        issues,
    ))
}

pub fn parse_fund_response(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    bytes: &[u8],
) -> Result<YahooEnrichment<YahooFundData>, YahooAdapterError> {
    require_family(request, YahooRequestFamily::FundSummary)?;
    let expected = exactly_one_request_symbol(request)?;
    let root = parse_root(bytes, bounds)?;
    let summary = object_path(&root, &["quoteSummary"])?;
    if let Some(error) = summary.get("error").filter(|value| !value.is_null()) {
        return Ok(provider_error_enrichment(
            request,
            context,
            ProviderField::Value(expected),
            bounds,
            error,
        ));
    }
    let result = first_summary_result(summary)?;
    let quote_type =
        optional_member_object(result, "quoteType", "quoteSummary.result[0].quoteType")?
            .unwrap_or(&EMPTY_OBJECT);
    let profile = optional_member_object(
        result,
        "summaryProfile",
        "quoteSummary.result[0].summaryProfile",
    )?
    .unwrap_or(&EMPTY_OBJECT);
    let holdings =
        optional_member_object(result, "topHoldings", "quoteSummary.result[0].topHoldings")?
            .unwrap_or(&EMPTY_OBJECT);
    let fund_profile =
        optional_member_object(result, "fundProfile", "quoteSummary.result[0].fundProfile")?
            .unwrap_or(&EMPTY_OBJECT);
    let mut issues = Vec::new();
    let provider_symbol = string_field(
        quote_type,
        "symbol",
        "quoteType.symbol",
        bounds,
        &mut issues,
    )?
    .map_value(|value| YahooSymbol::parse(value, bounds.max_string_bytes));
    let provider_symbol = transpose_symbol_field(provider_symbol, "quoteType.symbol")?;
    if let ProviderField::Value(symbol) = &provider_symbol
        && symbol != &expected
    {
        return Err(YahooAdapterError::UnexpectedSymbol(
            symbol.as_str().to_owned(),
        ));
    }
    let quote_type_field = string_field(
        quote_type,
        "quoteType",
        "quoteType.quoteType",
        bounds,
        &mut issues,
    )?;
    let provenance = provenance_from_summary(
        request,
        context,
        provider_symbol,
        quote_type,
        &EMPTY_OBJECT,
        profile,
        bounds,
        &mut issues,
    )?;
    if field_string_is_crypto(&quote_type_field) {
        issues.push(QualityIssue::UnsupportedAsset {
            quote_type: "CRYPTOCURRENCY".to_owned(),
        });
        return Ok(enrichment(None, provenance, issues));
    }

    let holding_values =
        optional_array_member(holdings, "holdings", "topHoldings.holdings")?.unwrap_or(&[]);
    enforce_count(
        "topHoldings.holdings",
        holding_values.len(),
        bounds.max_fund_holdings,
    )?;
    let mut top_holdings = Vec::new();
    reserve(&mut top_holdings, holding_values.len(), "fund holdings")?;
    for (index, value) in holding_values.iter().enumerate() {
        let holding = as_object(value, &format!("topHoldings.holdings[{index}]"))?;
        let symbol = string_field(
            holding,
            "symbol",
            &format!("topHoldings.holdings[{index}].symbol"),
            bounds,
            &mut issues,
        )?
        .map_value(|value| YahooSymbol::parse(value, bounds.max_string_bytes));
        top_holdings.push(YahooFundHolding {
            symbol: transpose_symbol_field(
                symbol,
                &format!("topHoldings.holdings[{index}].symbol"),
            )?,
            name: string_field(
                holding,
                "holdingName",
                &format!("topHoldings.holdings[{index}].holdingName"),
                bounds,
                &mut issues,
            )?,
            holding_percent: decimal_field(
                holding,
                "holdingPercent",
                &format!("topHoldings.holdings[{index}].holdingPercent"),
                &mut issues,
            ),
        });
    }

    let fees = optional_member_object(
        fund_profile,
        "feesExpensesInvestment",
        "fundProfile.feesExpensesInvestment",
    )?
    .unwrap_or(&EMPTY_OBJECT);
    let fund_has_data = !quote_type.is_empty()
        || !profile.is_empty()
        || !holdings.is_empty()
        || !fund_profile.is_empty();
    if !fund_has_data {
        issues.push(QualityIssue::EmptyResult);
    } else if quote_type.is_empty() || (holdings.is_empty() && fund_profile.is_empty()) {
        issues.push(QualityIssue::PartialProviderResult);
    }
    let data = YahooFundData {
        symbol: expected,
        quote_type: quote_type_field,
        description: string_field(
            profile,
            "longBusinessSummary",
            "summaryProfile.longBusinessSummary",
            bounds,
            &mut issues,
        )?,
        category_name: string_field(
            fund_profile,
            "categoryName",
            "fundProfile.categoryName",
            bounds,
            &mut issues,
        )?,
        family: string_field(
            fund_profile,
            "family",
            "fundProfile.family",
            bounds,
            &mut issues,
        )?,
        legal_type: string_field(
            fund_profile,
            "legalType",
            "fundProfile.legalType",
            bounds,
            &mut issues,
        )?,
        annual_report_expense_ratio: decimal_field(
            fees,
            "annualReportExpenseRatio",
            "fundProfile.feesExpensesInvestment.annualReportExpenseRatio",
            &mut issues,
        ),
        annual_holdings_turnover: decimal_field(
            fees,
            "annualHoldingsTurnover",
            "fundProfile.feesExpensesInvestment.annualHoldingsTurnover",
            &mut issues,
        ),
        total_net_assets: decimal_field(
            fees,
            "totalNetAssets",
            "fundProfile.feesExpensesInvestment.totalNetAssets",
            &mut issues,
        ),
        asset_classes: named_decimal_fields(
            holdings,
            &[
                "cashPosition",
                "stockPosition",
                "bondPosition",
                "preferredPosition",
                "convertiblePosition",
                "otherPosition",
            ],
            "topHoldings",
            &mut issues,
        ),
        equity_metrics: decimal_object_fields(
            holdings,
            "equityHoldings",
            "topHoldings.equityHoldings",
            &mut issues,
        )?,
        bond_metrics: decimal_object_fields(
            holdings,
            "bondHoldings",
            "topHoldings.bondHoldings",
            &mut issues,
        )?,
        bond_ratings: decimal_list_map(
            holdings,
            "bondRatings",
            "topHoldings.bondRatings",
            bounds,
            &mut issues,
        )?,
        sector_weightings: decimal_list_map(
            holdings,
            "sectorWeightings",
            "topHoldings.sectorWeightings",
            bounds,
            &mut issues,
        )?,
        top_holdings,
    };
    Ok(enrichment(
        fund_has_data.then_some(data),
        provenance,
        issues,
    ))
}

pub fn parse_option_response(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    bytes: &[u8],
) -> Result<YahooEnrichment<YahooOptionChain>, YahooAdapterError> {
    require_family(request, YahooRequestFamily::OptionChain)?;
    let expected = exactly_one_request_symbol(request)?;
    let root = parse_root(bytes, bounds)?;
    let option_chain = object_path(&root, &["optionChain"])?;
    if let Some(error) = option_chain.get("error").filter(|value| !value.is_null()) {
        return Ok(provider_error_enrichment(
            request,
            context,
            ProviderField::Value(expected),
            bounds,
            error,
        ));
    }
    let results = array_member(option_chain, "result", "optionChain.result")?;
    if results.is_empty() {
        return Ok(unavailable(
            empty_provenance(request, context, ProviderField::Value(expected)),
            QualityIssue::EmptyResult,
        ));
    }
    if results.len() != 1 {
        return Err(YahooAdapterError::InvalidSchema {
            path: "optionChain.result".to_owned(),
            reason: "one underlying request must return exactly one option-chain result".to_owned(),
        });
    }
    let result = as_object(&results[0], "optionChain.result[0]")?;
    let mut issues = Vec::new();
    let quote = optional_member_object(result, "quote", "optionChain.result[0].quote")?
        .unwrap_or(&EMPTY_OBJECT);
    let provider_symbol = string_field(
        quote,
        "symbol",
        "optionChain.result[0].quote.symbol",
        bounds,
        &mut issues,
    )?
    .map_value(|value| YahooSymbol::parse(value, bounds.max_string_bytes));
    let provider_symbol = transpose_symbol_field(provider_symbol, "optionChain.quote.symbol")?;
    if let ProviderField::Value(symbol) = &provider_symbol
        && symbol != &expected
    {
        return Err(YahooAdapterError::UnexpectedSymbol(
            symbol.as_str().to_owned(),
        ));
    }
    let quote_type = string_field(
        quote,
        "quoteType",
        "optionChain.quote.quoteType",
        bounds,
        &mut issues,
    )?;
    let provenance = provenance_from_quote_map(
        request,
        context,
        quote,
        provider_symbol,
        bounds,
        &mut issues,
    )?;
    if field_string_is_crypto(&quote_type) {
        issues.push(QualityIssue::UnsupportedAsset {
            quote_type: "CRYPTOCURRENCY".to_owned(),
        });
        return Ok(enrichment(None, provenance, issues));
    }

    let expirations = parse_i64_array(
        result.get("expirationDates"),
        "optionChain.result[0].expirationDates",
        bounds.max_option_expirations,
    )?;
    let strikes = parse_decimal_array(
        result.get("strikes"),
        "optionChain.result[0].strikes",
        bounds.max_records_per_response,
    )?;
    let option_groups =
        optional_array_member(result, "options", "optionChain.result[0].options")?.unwrap_or(&[]);
    enforce_count(
        "optionChain.result[0].options",
        option_groups.len(),
        bounds.max_option_expirations,
    )?;
    let requested_expiration = request
        .effective_arguments
        .get("requested_expiration_unix_seconds")
        .and_then(|value| value.parse::<i64>().ok());
    let mut returned_expiration = ProviderField::Missing;
    let mut has_mini_options = ProviderField::Missing;
    let mut contracts = Vec::new();
    let mut seen_contracts = BTreeSet::new();

    for (group_index, value) in option_groups.iter().enumerate() {
        let group = as_object(
            value,
            &format!("optionChain.result[0].options[{group_index}]"),
        )?;
        let group_expiration = i64_field(
            group,
            "expirationDate",
            &format!("optionChain.options[{group_index}].expirationDate"),
            &mut issues,
        );
        if matches!(returned_expiration, ProviderField::Missing) {
            returned_expiration = group_expiration.clone();
        } else if returned_expiration != group_expiration {
            issues.push(QualityIssue::ExpirationMismatch);
        }
        if let (Some(requested), ProviderField::Value(returned)) =
            (requested_expiration, &group_expiration)
            && requested != *returned
        {
            issues.push(QualityIssue::ExpirationMismatch);
        }
        let group_mini = bool_field(
            group,
            "hasMiniOptions",
            &format!("optionChain.options[{group_index}].hasMiniOptions"),
            &mut issues,
        );
        if matches!(has_mini_options, ProviderField::Missing) {
            has_mini_options = group_mini;
        }
        for (side, key) in [
            (YahooOptionSide::Call, "calls"),
            (YahooOptionSide::Put, "puts"),
        ] {
            let values = optional_array_member(
                group,
                key,
                &format!("optionChain.options[{group_index}].{key}"),
            )?
            .unwrap_or(&[]);
            let next_count = contracts.len().checked_add(values.len()).ok_or(
                YahooAdapterError::ApplicationBoundExceeded {
                    name: "option_contracts",
                    actual: usize::MAX,
                    maximum: bounds.max_option_contracts,
                },
            )?;
            enforce_count("option contracts", next_count, bounds.max_option_contracts)?;
            reserve(&mut contracts, values.len(), "option contracts")?;
            for (contract_index, value) in values.iter().enumerate() {
                let path = format!("optionChain.options[{group_index}].{key}[{contract_index}]");
                let object = as_object(value, &path)?;
                let symbol = required_symbol(
                    object,
                    "contractSymbol",
                    bounds,
                    &format!("{path}.contractSymbol"),
                )?;
                if !seen_contracts.insert(symbol.clone()) {
                    return Err(YahooAdapterError::DuplicateReturnedIdentity(
                        symbol.as_str().to_owned(),
                    ));
                }
                let bid = decimal_field(object, "bid", &format!("{path}.bid"), &mut issues);
                let ask = decimal_field(object, "ask", &format!("{path}.ask"), &mut issues);
                quote_side_quality(&bid, &ask, &mut issues);
                contracts.push(YahooOptionContract {
                    side,
                    contract_symbol: symbol,
                    last_trade_time_unix_seconds: i64_field(
                        object,
                        "lastTradeDate",
                        &format!("{path}.lastTradeDate"),
                        &mut issues,
                    ),
                    strike: decimal_field(object, "strike", &format!("{path}.strike"), &mut issues),
                    last_price: decimal_field(
                        object,
                        "lastPrice",
                        &format!("{path}.lastPrice"),
                        &mut issues,
                    ),
                    bid,
                    ask,
                    change: decimal_field(object, "change", &format!("{path}.change"), &mut issues),
                    percent_change: decimal_field(
                        object,
                        "percentChange",
                        &format!("{path}.percentChange"),
                        &mut issues,
                    ),
                    volume: u64_field(object, "volume", &format!("{path}.volume"), &mut issues),
                    open_interest: u64_field(
                        object,
                        "openInterest",
                        &format!("{path}.openInterest"),
                        &mut issues,
                    ),
                    implied_volatility: decimal_field(
                        object,
                        "impliedVolatility",
                        &format!("{path}.impliedVolatility"),
                        &mut issues,
                    ),
                    in_the_money: bool_field(
                        object,
                        "inTheMoney",
                        &format!("{path}.inTheMoney"),
                        &mut issues,
                    ),
                    contract_size: string_field(
                        object,
                        "contractSize",
                        &format!("{path}.contractSize"),
                        bounds,
                        &mut issues,
                    )?,
                    currency: string_field(
                        object,
                        "currency",
                        &format!("{path}.currency"),
                        bounds,
                        &mut issues,
                    )?,
                });
            }
        }
    }
    let underlying_quote = if quote.is_empty() {
        ProviderField::Missing
    } else {
        let (quote, _, quote_issues) = parse_quote_object(
            quote,
            expected.clone(),
            request,
            context,
            bounds,
            "optionChain.quote",
        )?;
        issues.extend(quote_issues);
        ProviderField::Value(quote)
    };
    if contracts.is_empty() {
        issues.push(QualityIssue::EmptyResult);
    }
    let valid_contract_count = contracts
        .iter()
        .filter(|contract| option_contract_has_market_component(contract))
        .count();
    if !contracts.is_empty() && valid_contract_count == 0 {
        issues.push(QualityIssue::PartialProviderResult);
    }
    let data = YahooOptionChain {
        underlying_symbol: expected,
        requested_expiration_unix_seconds: requested_expiration,
        returned_expiration_unix_seconds: returned_expiration,
        expiration_dates_unix_seconds: expirations,
        strikes,
        has_mini_options,
        underlying_quote,
        valid_contract_count,
        contracts,
    };
    let has_data = !data.contracts.is_empty() || !data.expiration_dates_unix_seconds.is_empty();
    Ok(enrichment(has_data.then_some(data), provenance, issues))
}

pub fn parse_lookup_response(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    bytes: &[u8],
) -> Result<YahooReturnedDisposition<YahooLookupHint>, YahooAdapterError> {
    if request.family != YahooRequestFamily::Search && request.family != YahooRequestFamily::Lookup
    {
        return Err(YahooAdapterError::WrongRequestFamily);
    }
    let root = parse_root(bytes, bounds)?;
    let values = match request.family {
        YahooRequestFamily::Search => root
            .as_object()
            .ok_or(YahooAdapterError::MissingEnvelope("search root"))?
            .get("quotes")
            .and_then(Value::as_array)
            .ok_or(YahooAdapterError::MissingEnvelope("quotes"))?,
        YahooRequestFamily::Lookup => {
            let finance = object_path(&root, &["finance"])?;
            if let Some(error) = finance.get("error").filter(|value| !value.is_null()) {
                let issue = provider_error_issue(error, bounds);
                return Ok(YahooReturnedDisposition {
                    requested_symbols: Vec::new(),
                    provider_returned_symbols: Vec::new(),
                    valid_observations: 0,
                    missing_symbols: Vec::new(),
                    rejected_symbols: Vec::new(),
                    observations: vec![YahooEnrichment {
                        state: YahooEnrichmentState::Unavailable,
                        authority: EvidenceAuthority::ExperimentalSupplementOnly,
                        provenance: empty_provenance(request, context, ProviderField::Missing),
                        issues: vec![issue],
                        data: None,
                    }],
                });
            }
            let result = array_member(finance, "result", "finance.result")?;
            let first = result
                .first()
                .ok_or(YahooAdapterError::MissingEnvelope("finance.result[0]"))?;
            let first = as_object(first, "finance.result[0]")?;
            array_member(first, "documents", "finance.result[0].documents")?
        }
        _ => return Err(YahooAdapterError::WrongRequestFamily),
    };
    enforce_count(
        "lookup results",
        values.len(),
        bounds.max_records_per_response,
    )?;
    let mut returned = Vec::new();
    let mut rejected = Vec::new();
    let mut observations = Vec::new();
    let mut seen = BTreeSet::new();
    reserve(&mut returned, values.len(), "lookup returned")?;
    reserve(&mut observations, values.len(), "lookup observations")?;
    for (index, value) in values.iter().enumerate() {
        let object = as_object(value, &format!("lookup.results[{index}]"))?;
        let symbol = required_symbol(
            object,
            "symbol",
            bounds,
            &format!("lookup.results[{index}].symbol"),
        )?;
        if !seen.insert(symbol.clone()) {
            return Err(YahooAdapterError::DuplicateReturnedIdentity(
                symbol.as_str().to_owned(),
            ));
        }
        returned.push(symbol.clone());
        let mut issues = Vec::new();
        let quote_type = string_field(
            object,
            "quoteType",
            &format!("lookup.results[{index}].quoteType"),
            bounds,
            &mut issues,
        )?;
        let provenance = provenance_from_quote_map(
            request,
            context,
            object,
            ProviderField::Value(symbol.clone()),
            bounds,
            &mut issues,
        )?;
        if field_string_is_crypto(&quote_type) {
            issues.push(QualityIssue::UnsupportedAsset {
                quote_type: "CRYPTOCURRENCY".to_owned(),
            });
            rejected.push(symbol);
            observations.push(enrichment(None, provenance, issues));
            continue;
        }
        let hint = YahooLookupHint {
            symbol,
            quote_type,
            exchange: select_string_field(
                [
                    (object, "exchange", "lookup.exchange"),
                    (object, "exchDisp", "lookup.exchDisp"),
                ],
                bounds,
                &mut issues,
            )?,
            short_name: select_string_field(
                [
                    (object, "shortname", "lookup.shortname"),
                    (object, "shortName", "lookup.shortName"),
                ],
                bounds,
                &mut issues,
            )?,
            long_name: select_string_field(
                [
                    (object, "longname", "lookup.longname"),
                    (object, "longName", "lookup.longName"),
                ],
                bounds,
                &mut issues,
            )?,
            sector: string_field(object, "sector", "lookup.sector", bounds, &mut issues)?,
            industry: string_field(object, "industry", "lookup.industry", bounds, &mut issues)?,
            score: select_decimal_field(
                [
                    (object, "score", "lookup.score"),
                    (object, "navScore", "lookup.navScore"),
                ],
                &mut issues,
            ),
        };
        observations.push(enrichment(Some(hint), provenance, issues));
    }
    let valid_observations = observations
        .iter()
        .filter(|observation| observation.data.is_some())
        .count();
    Ok(YahooReturnedDisposition {
        requested_symbols: Vec::new(),
        provider_returned_symbols: returned,
        valid_observations,
        missing_symbols: Vec::new(),
        rejected_symbols: rejected,
        observations,
    })
}

fn parse_root(bytes: &[u8], bounds: AdapterBounds) -> Result<Value, YahooAdapterError> {
    bounds.validate()?;
    if bytes.len() > bounds.max_response_bytes {
        return Err(YahooAdapterError::ResponseTooLarge {
            actual: bytes.len(),
            maximum: bounds.max_response_bytes,
        });
    }
    serde_json::from_slice(bytes).map_err(|error| YahooAdapterError::InvalidJson(error.to_string()))
}

fn require_family(
    request: &YahooHttpRequest,
    expected: YahooRequestFamily,
) -> Result<(), YahooAdapterError> {
    if request.family != expected {
        return Err(YahooAdapterError::WrongRequestFamily);
    }
    Ok(())
}

fn exactly_one_request_symbol(
    request: &YahooHttpRequest,
) -> Result<YahooSymbol, YahooAdapterError> {
    if request.requested_targets.len() != 1 {
        return Err(YahooAdapterError::InvalidSchema {
            path: "request.requested_targets".to_owned(),
            reason: "this provider surface requires exactly one symbol".to_owned(),
        });
    }
    Ok(request.requested_targets[0].symbol.clone())
}

fn object_path<'a>(
    root: &'a Value,
    path: &[&str],
) -> Result<&'a Map<String, Value>, YahooAdapterError> {
    let mut cursor = root;
    for segment in path {
        cursor = cursor
            .as_object()
            .and_then(|object| object.get(*segment))
            .ok_or(YahooAdapterError::MissingEnvelope("nested path member"))?;
    }
    cursor
        .as_object()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: path.join("."),
            reason: "expected object".to_owned(),
        })
}

fn nested_object_path<'a>(
    root: &'a Map<String, Value>,
    path: &[&str],
) -> Result<&'a Map<String, Value>, YahooAdapterError> {
    let mut cursor = root;
    for (index, segment) in path.iter().enumerate() {
        let value = cursor
            .get(*segment)
            .ok_or(YahooAdapterError::MissingEnvelope("nested path member"))?;
        cursor = value
            .as_object()
            .ok_or_else(|| YahooAdapterError::InvalidSchema {
                path: path[..=index].join("."),
                reason: "expected object".to_owned(),
            })?;
    }
    Ok(cursor)
}

fn as_object<'a>(
    value: &'a Value,
    path: &str,
) -> Result<&'a Map<String, Value>, YahooAdapterError> {
    value
        .as_object()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "expected object".to_owned(),
        })
}

fn member_object<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<&'a Map<String, Value>, YahooAdapterError> {
    object
        .get(key)
        .ok_or(YahooAdapterError::MissingEnvelope("object member"))?
        .as_object()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "expected object".to_owned(),
        })
}

fn optional_member_object<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<Option<&'a Map<String, Value>>, YahooAdapterError> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            value
                .as_object()
                .map(Some)
                .ok_or_else(|| YahooAdapterError::InvalidSchema {
                    path: path.to_owned(),
                    reason: "expected object".to_owned(),
                })
        }
    }
}

fn array_member<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<&'a [Value], YahooAdapterError> {
    object
        .get(key)
        .ok_or(YahooAdapterError::MissingEnvelope("array member"))?
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "expected array".to_owned(),
        })
}

fn optional_array_member<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<Option<&'a [Value]>, YahooAdapterError> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_array()
            .map(|values| Some(values.as_slice()))
            .ok_or_else(|| YahooAdapterError::InvalidSchema {
                path: path.to_owned(),
                reason: "expected array".to_owned(),
            }),
    }
}

fn optional_nested_first_object<'a>(
    root: &'a Map<String, Value>,
    path: &[&str],
    display_path: &str,
) -> Result<Option<&'a Map<String, Value>>, YahooAdapterError> {
    let Some((last, parents)) = path.split_last() else {
        return Ok(None);
    };
    let parent = match nested_object_path(root, parents) {
        Ok(parent) => parent,
        Err(YahooAdapterError::MissingEnvelope(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(value) = parent.get(*last) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let array = value
        .as_array()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: display_path.to_owned(),
            reason: "expected array".to_owned(),
        })?;
    array
        .first()
        .map(|value| as_object(value, display_path))
        .transpose()
}

fn first_summary_result(
    summary: &Map<String, Value>,
) -> Result<&Map<String, Value>, YahooAdapterError> {
    let results = array_member(summary, "result", "quoteSummary.result")?;
    if results.len() != 1 {
        return Err(YahooAdapterError::InvalidSchema {
            path: "quoteSummary.result".to_owned(),
            reason: "one symbol-specific request must return exactly one result".to_owned(),
        });
    }
    as_object(&results[0], "quoteSummary.result[0]")
}

fn enforce_count(
    name: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), YahooAdapterError> {
    if actual > maximum {
        return Err(YahooAdapterError::ApplicationBoundExceeded {
            name,
            actual,
            maximum,
        });
    }
    Ok(())
}

fn reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    name: &'static str,
) -> Result<(), YahooAdapterError> {
    values
        .try_reserve(additional)
        .map_err(|_| YahooAdapterError::ApplicationBoundExceeded {
            name,
            actual: additional,
            maximum: additional,
        })
}

fn raw_value(value: &Value) -> &Value {
    value
        .as_object()
        .and_then(|object| object.get("raw"))
        .unwrap_or(value)
}

fn decimal_from_value(value: &Value) -> Option<Decimal> {
    let value = raw_value(value);
    let text = match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => return None,
    };
    Decimal::from_str_exact(&text)
        .or_else(|_| Decimal::from_scientific(&text))
        .ok()
}

fn parse_i64_value(value: &Value) -> Option<i64> {
    let value = raw_value(value);
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn parse_u64_value(value: &Value) -> Option<u64> {
    let value = raw_value(value);
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn parse_bool_value(value: &Value) -> Option<bool> {
    match raw_value(value) {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

fn string_field(
    object: &Map<String, Value>,
    key: &str,
    path: &str,
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<ProviderField<String>, YahooAdapterError> {
    let Some(value) = object.get(key) else {
        return Ok(ProviderField::Missing);
    };
    if value.is_null() {
        return Ok(ProviderField::Null);
    }
    let value = raw_value(value);
    let Some(value) = value.as_str() else {
        issues.push(QualityIssue::InvalidField {
            field: path.to_owned(),
        });
        return Ok(ProviderField::Invalid);
    };
    if value.len() > bounds.max_string_bytes {
        return Err(YahooAdapterError::StringTooLong {
            path: path.to_owned(),
        });
    }
    Ok(ProviderField::Value(value.to_owned()))
}

fn decimal_field(
    object: &Map<String, Value>,
    key: &str,
    path: &str,
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<Decimal> {
    let Some(value) = object.get(key) else {
        return ProviderField::Missing;
    };
    if value.is_null() || raw_value(value).is_null() {
        return ProviderField::Null;
    }
    match decimal_from_value(value) {
        Some(value) => ProviderField::Value(value),
        None => {
            issues.push(QualityIssue::InvalidField {
                field: path.to_owned(),
            });
            ProviderField::Invalid
        }
    }
}

fn i64_field(
    object: &Map<String, Value>,
    key: &str,
    path: &str,
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<i64> {
    let Some(value) = object.get(key) else {
        return ProviderField::Missing;
    };
    if value.is_null() || raw_value(value).is_null() {
        return ProviderField::Null;
    }
    match parse_i64_value(value) {
        Some(value) => ProviderField::Value(value),
        None => {
            issues.push(QualityIssue::InvalidField {
                field: path.to_owned(),
            });
            ProviderField::Invalid
        }
    }
}

fn u64_field(
    object: &Map<String, Value>,
    key: &str,
    path: &str,
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<u64> {
    let Some(value) = object.get(key) else {
        return ProviderField::Missing;
    };
    if value.is_null() || raw_value(value).is_null() {
        return ProviderField::Null;
    }
    match parse_u64_value(value) {
        Some(value) => ProviderField::Value(value),
        None => {
            issues.push(QualityIssue::InvalidField {
                field: path.to_owned(),
            });
            ProviderField::Invalid
        }
    }
}

fn bool_field(
    object: &Map<String, Value>,
    key: &str,
    path: &str,
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<bool> {
    let Some(value) = object.get(key) else {
        return ProviderField::Missing;
    };
    if value.is_null() || raw_value(value).is_null() {
        return ProviderField::Null;
    }
    match parse_bool_value(value) {
        Some(value) => ProviderField::Value(value),
        None => {
            issues.push(QualityIssue::InvalidField {
                field: path.to_owned(),
            });
            ProviderField::Invalid
        }
    }
}

fn required_symbol(
    object: &Map<String, Value>,
    key: &str,
    bounds: AdapterBounds,
    path: &str,
) -> Result<YahooSymbol, YahooAdapterError> {
    let mut issues = Vec::new();
    match string_field(object, key, path, bounds, &mut issues)? {
        ProviderField::Value(value) => YahooSymbol::parse(value, bounds.max_string_bytes),
        _ => Err(YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "required provider symbol is absent or malformed".to_owned(),
        }),
    }
}

fn transpose_symbol_field(
    field: ProviderField<Result<YahooSymbol, YahooAdapterError>>,
    path: &str,
) -> Result<ProviderField<YahooSymbol>, YahooAdapterError> {
    match field {
        ProviderField::Missing => Ok(ProviderField::Missing),
        ProviderField::Null => Ok(ProviderField::Null),
        ProviderField::Value(Ok(value)) => Ok(ProviderField::Value(value)),
        ProviderField::Value(Err(_)) => Err(YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "provider symbol is unsafe".to_owned(),
        }),
        ProviderField::Invalid => Ok(ProviderField::Invalid),
    }
}

fn select_string_field<const N: usize>(
    candidates: [(&Map<String, Value>, &str, &str); N],
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<ProviderField<String>, YahooAdapterError> {
    let mut fallback = ProviderField::Missing;
    for (object, key, path) in candidates {
        let value = string_field(object, key, path, bounds, issues)?;
        match value {
            ProviderField::Value(_) | ProviderField::Invalid => return Ok(value),
            ProviderField::Null => fallback = ProviderField::Null,
            ProviderField::Missing => {}
        }
    }
    Ok(fallback)
}

fn select_decimal_field<const N: usize>(
    candidates: [(&Map<String, Value>, &str, &str); N],
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<Decimal> {
    let mut fallback = ProviderField::Missing;
    for (object, key, path) in candidates {
        let value = decimal_field(object, key, path, issues);
        match value {
            ProviderField::Value(_) | ProviderField::Invalid => return value,
            ProviderField::Null => fallback = ProviderField::Null,
            ProviderField::Missing => {}
        }
    }
    fallback
}

fn select_i64_field<const N: usize>(
    candidates: [(&Map<String, Value>, &str, &str); N],
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<i64> {
    let mut fallback = ProviderField::Missing;
    for (object, key, path) in candidates {
        let value = i64_field(object, key, path, issues);
        match value {
            ProviderField::Value(_) | ProviderField::Invalid => return value,
            ProviderField::Null => fallback = ProviderField::Null,
            ProviderField::Missing => {}
        }
    }
    fallback
}

fn indexed_decimal_field(
    object: &Map<String, Value>,
    key: &str,
    index: usize,
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<Decimal> {
    let Some(values) = object.get(key).and_then(Value::as_array) else {
        return if object.contains_key(key) {
            issues.push(QualityIssue::InvalidField {
                field: format!("chart.{key}"),
            });
            ProviderField::Invalid
        } else {
            ProviderField::Missing
        };
    };
    let Some(value) = values.get(index) else {
        return ProviderField::Missing;
    };
    if value.is_null() {
        return ProviderField::Null;
    }
    match decimal_from_value(value) {
        Some(value) => ProviderField::Value(value),
        None => {
            issues.push(QualityIssue::InvalidField {
                field: format!("chart.{key}[{index}]"),
            });
            ProviderField::Invalid
        }
    }
}

fn indexed_u64_field(
    object: &Map<String, Value>,
    key: &str,
    index: usize,
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<u64> {
    let Some(values) = object.get(key).and_then(Value::as_array) else {
        return if object.contains_key(key) {
            issues.push(QualityIssue::InvalidField {
                field: format!("chart.{key}"),
            });
            ProviderField::Invalid
        } else {
            ProviderField::Missing
        };
    };
    let Some(value) = values.get(index) else {
        return ProviderField::Missing;
    };
    if value.is_null() {
        return ProviderField::Null;
    }
    match parse_u64_value(value) {
        Some(value) => ProviderField::Value(value),
        None => {
            issues.push(QualityIssue::InvalidField {
                field: format!("chart.{key}[{index}]"),
            });
            ProviderField::Invalid
        }
    }
}

fn note_array_length(
    object: &Map<String, Value>,
    key: &str,
    expected: usize,
    issues: &mut Vec<QualityIssue>,
) -> Result<(), YahooAdapterError> {
    let Some(value) = object.get(key) else {
        return Ok(());
    };
    let values = value
        .as_array()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: format!("chart.indicators.{key}"),
            reason: "expected array".to_owned(),
        })?;
    if values.len() != expected {
        issues.push(QualityIssue::ArrayLengthMismatch {
            field: format!("chart.indicators.{key}"),
        });
    }
    Ok(())
}

fn parse_string_array(
    value: Option<&Value>,
    path: &str,
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<Vec<String>, YahooAdapterError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let values = value
        .as_array()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "expected array".to_owned(),
        })?;
    enforce_count(
        "string array",
        values.len(),
        bounds.max_records_per_response,
    )?;
    let mut parsed = Vec::new();
    reserve(&mut parsed, values.len(), "string array")?;
    for (index, value) in values.iter().enumerate() {
        let Some(value) = value.as_str() else {
            issues.push(QualityIssue::InvalidField {
                field: format!("{path}[{index}]"),
            });
            continue;
        };
        if value.len() > bounds.max_string_bytes {
            return Err(YahooAdapterError::StringTooLong {
                path: format!("{path}[{index}]"),
            });
        }
        parsed.push(value.to_owned());
    }
    Ok(parsed)
}

fn parse_i64_array(
    value: Option<&Value>,
    path: &str,
    maximum: usize,
) -> Result<Vec<i64>, YahooAdapterError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let values = value
        .as_array()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "expected array".to_owned(),
        })?;
    enforce_count("integer array", values.len(), maximum)?;
    let mut parsed = Vec::new();
    reserve(&mut parsed, values.len(), "integer array")?;
    for (index, value) in values.iter().enumerate() {
        parsed.push(
            parse_i64_value(value).ok_or_else(|| YahooAdapterError::InvalidNumber {
                path: format!("{path}[{index}]"),
            })?,
        );
    }
    Ok(parsed)
}

fn parse_decimal_array(
    value: Option<&Value>,
    path: &str,
    maximum: usize,
) -> Result<Vec<Decimal>, YahooAdapterError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let values = value
        .as_array()
        .ok_or_else(|| YahooAdapterError::InvalidSchema {
            path: path.to_owned(),
            reason: "expected array".to_owned(),
        })?;
    enforce_count("decimal array", values.len(), maximum)?;
    let mut parsed = Vec::new();
    reserve(&mut parsed, values.len(), "decimal array")?;
    for (index, value) in values.iter().enumerate() {
        parsed.push(
            decimal_from_value(value).ok_or_else(|| YahooAdapterError::InvalidNumber {
                path: format!("{path}[{index}]"),
            })?,
        );
    }
    Ok(parsed)
}

fn parse_chart_events(
    result: &Map<String, Value>,
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<Vec<YahooChartEvent>, YahooAdapterError> {
    let Some(events) = result.get("events") else {
        return Ok(Vec::new());
    };
    if events.is_null() {
        return Ok(Vec::new());
    }
    let events = as_object(events, "chart.result[0].events")?;
    let mut parsed = Vec::new();
    for (key, kind) in [
        ("dividends", YahooChartEventKind::Dividend),
        ("splits", YahooChartEventKind::Split),
        ("capitalGains", YahooChartEventKind::CapitalGain),
    ] {
        let Some(values) = events.get(key) else {
            continue;
        };
        if values.is_null() {
            continue;
        }
        let values = as_object(values, &format!("chart.events.{key}"))?;
        let next_count = parsed.len().checked_add(values.len()).ok_or(
            YahooAdapterError::ApplicationBoundExceeded {
                name: "chart events",
                actual: usize::MAX,
                maximum: bounds.max_records_per_response,
            },
        )?;
        enforce_count("chart events", next_count, bounds.max_records_per_response)?;
        reserve(&mut parsed, values.len(), "chart events")?;
        for (identity, value) in values {
            let object = as_object(value, &format!("chart.events.{key}.{identity}"))?;
            let timestamp = object
                .get("date")
                .and_then(parse_i64_value)
                .or_else(|| identity.parse::<i64>().ok())
                .ok_or_else(|| YahooAdapterError::InvalidNumber {
                    path: format!("chart.events.{key}.{identity}.date"),
                })?;
            parsed.push(YahooChartEvent {
                kind,
                timestamp_unix_seconds: timestamp,
                amount: decimal_field(
                    object,
                    "amount",
                    &format!("chart.events.{key}.{identity}.amount"),
                    issues,
                ),
                currency: string_field(
                    object,
                    "currency",
                    &format!("chart.events.{key}.{identity}.currency"),
                    bounds,
                    issues,
                )?,
                numerator: decimal_field(
                    object,
                    "numerator",
                    &format!("chart.events.{key}.{identity}.numerator"),
                    issues,
                ),
                denominator: decimal_field(
                    object,
                    "denominator",
                    &format!("chart.events.{key}.{identity}.denominator"),
                    issues,
                ),
                split_ratio: string_field(
                    object,
                    "splitRatio",
                    &format!("chart.events.{key}.{identity}.splitRatio"),
                    bounds,
                    issues,
                )?,
            });
        }
    }
    parsed.sort_by_key(|event| (event.timestamp_unix_seconds, event_kind_order(event.kind)));
    Ok(parsed)
}

const fn event_kind_order(kind: YahooChartEventKind) -> u8 {
    match kind {
        YahooChartEventKind::Dividend => 0,
        YahooChartEventKind::Split => 1,
        YahooChartEventKind::CapitalGain => 2,
    }
}

fn named_decimal_fields(
    object: &Map<String, Value>,
    keys: &[&str],
    prefix: &str,
    issues: &mut Vec<QualityIssue>,
) -> BTreeMap<String, ProviderField<Decimal>> {
    keys.iter()
        .map(|key| {
            (
                (*key).to_owned(),
                decimal_field(object, key, &format!("{prefix}.{key}"), issues),
            )
        })
        .collect()
}

fn decimal_object_fields(
    parent: &Map<String, Value>,
    key: &str,
    path: &str,
    issues: &mut Vec<QualityIssue>,
) -> Result<BTreeMap<String, ProviderField<Decimal>>, YahooAdapterError> {
    let Some(object) = optional_member_object(parent, key, path)? else {
        return Ok(BTreeMap::new());
    };
    let mut parsed = BTreeMap::new();
    for field in object.keys() {
        parsed.insert(
            field.clone(),
            decimal_field(object, field, &format!("{path}.{field}"), issues),
        );
    }
    Ok(parsed)
}

fn decimal_list_map(
    parent: &Map<String, Value>,
    key: &str,
    path: &str,
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<BTreeMap<String, ProviderField<Decimal>>, YahooAdapterError> {
    let Some(values) = optional_array_member(parent, key, path)? else {
        return Ok(BTreeMap::new());
    };
    enforce_count(
        "fund map entries",
        values.len(),
        bounds.max_records_per_response,
    )?;
    let mut parsed = BTreeMap::new();
    for (index, value) in values.iter().enumerate() {
        let object = as_object(value, &format!("{path}[{index}]"))?;
        for field in object.keys() {
            if field.len() > bounds.max_string_bytes {
                return Err(YahooAdapterError::StringTooLong {
                    path: format!("{path}[{index}] key"),
                });
            }
            if parsed.contains_key(field) {
                return Err(YahooAdapterError::DuplicateReturnedIdentity(format!(
                    "{path}:{field}"
                )));
            }
            parsed.insert(
                field.clone(),
                decimal_field(object, field, &format!("{path}[{index}].{field}"), issues),
            );
        }
    }
    Ok(parsed)
}

fn parse_quote_object(
    object: &Map<String, Value>,
    symbol: YahooSymbol,
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    path: &str,
) -> Result<(YahooQuote, YahooProvenance, Vec<QualityIssue>), YahooAdapterError> {
    let mut issues = Vec::new();
    let bid = decimal_field(object, "bid", &format!("{path}.bid"), &mut issues);
    let ask = decimal_field(object, "ask", &format!("{path}.ask"), &mut issues);
    quote_side_quality(&bid, &ask, &mut issues);
    let regular_market_time = i64_field(
        object,
        "regularMarketTime",
        &format!("{path}.regularMarketTime"),
        &mut issues,
    );
    if !regular_market_time.is_value() {
        issues.push(QualityIssue::MissingProviderTimestamp);
    }
    let provider_symbol = match object.get("symbol") {
        Some(_) => ProviderField::Value(symbol.clone()),
        None => ProviderField::Missing,
    };
    let provenance = provenance_from_quote_map(
        request,
        context,
        object,
        provider_symbol,
        bounds,
        &mut issues,
    )?;
    let quote = YahooQuote {
        symbol,
        quote_type: string_field(
            object,
            "quoteType",
            &format!("{path}.quoteType"),
            bounds,
            &mut issues,
        )?,
        currency: string_field(
            object,
            "currency",
            &format!("{path}.currency"),
            bounds,
            &mut issues,
        )?,
        market_state: string_field(
            object,
            "marketState",
            &format!("{path}.marketState"),
            bounds,
            &mut issues,
        )?,
        regular_market_time_unix_seconds: regular_market_time,
        regular_market_price: decimal_field(
            object,
            "regularMarketPrice",
            &format!("{path}.regularMarketPrice"),
            &mut issues,
        ),
        bid,
        bid_size: u64_field(object, "bidSize", &format!("{path}.bidSize"), &mut issues),
        ask,
        ask_size: u64_field(object, "askSize", &format!("{path}.askSize"), &mut issues),
        open: decimal_field(
            object,
            "regularMarketOpen",
            &format!("{path}.regularMarketOpen"),
            &mut issues,
        ),
        day_low: decimal_field(
            object,
            "regularMarketDayLow",
            &format!("{path}.regularMarketDayLow"),
            &mut issues,
        ),
        day_high: decimal_field(
            object,
            "regularMarketDayHigh",
            &format!("{path}.regularMarketDayHigh"),
            &mut issues,
        ),
        previous_close: decimal_field(
            object,
            "regularMarketPreviousClose",
            &format!("{path}.regularMarketPreviousClose"),
            &mut issues,
        ),
        volume: u64_field(
            object,
            "regularMarketVolume",
            &format!("{path}.regularMarketVolume"),
            &mut issues,
        ),
        pre_market_price: decimal_field(
            object,
            "preMarketPrice",
            &format!("{path}.preMarketPrice"),
            &mut issues,
        ),
        pre_market_time_unix_seconds: i64_field(
            object,
            "preMarketTime",
            &format!("{path}.preMarketTime"),
            &mut issues,
        ),
        post_market_price: decimal_field(
            object,
            "postMarketPrice",
            &format!("{path}.postMarketPrice"),
            &mut issues,
        ),
        post_market_time_unix_seconds: i64_field(
            object,
            "postMarketTime",
            &format!("{path}.postMarketTime"),
            &mut issues,
        ),
        short_name: select_string_field(
            [
                (object, "shortName", "quote.shortName"),
                (object, "shortname", "quote.shortname"),
            ],
            bounds,
            &mut issues,
        )?,
        long_name: select_string_field(
            [
                (object, "longName", "quote.longName"),
                (object, "longname", "quote.longname"),
            ],
            bounds,
            &mut issues,
        )?,
    };
    Ok((quote, provenance, issues))
}

fn quote_side_quality(
    bid: &ProviderField<Decimal>,
    ask: &ProviderField<Decimal>,
    issues: &mut Vec<QualityIssue>,
) {
    match (bid, ask) {
        (ProviderField::Value(bid), ProviderField::Value(ask)) => {
            if *bid <= Decimal::ZERO || *ask <= Decimal::ZERO {
                issues.push(QualityIssue::NonPositiveQuoteSide);
            } else if bid > ask {
                issues.push(QualityIssue::CrossedQuote);
            }
        }
        (ProviderField::Value(_), _) | (_, ProviderField::Value(_)) => {
            issues.push(QualityIssue::OneSidedQuote);
        }
        _ => {}
    }
}

fn quote_has_market_component(quote: &YahooQuote) -> bool {
    quote.regular_market_price.is_value()
        || quote.bid.is_value()
        || quote.ask.is_value()
        || quote.volume.is_value()
}

fn bar_has_market_component(bar: &YahooBar) -> bool {
    bar.open.is_value()
        || bar.high.is_value()
        || bar.low.is_value()
        || bar.close.is_value()
        || bar.adjusted_close.is_value()
        || bar.volume.is_value()
}

fn option_contract_has_market_component(contract: &YahooOptionContract) -> bool {
    contract.last_price.is_value()
        || contract.bid.is_value()
        || contract.ask.is_value()
        || contract.volume.is_value()
        || contract.open_interest.is_value()
        || contract.implied_volatility.is_value()
}

fn quote_type_is_crypto(field: &ProviderField<String>) -> bool {
    field_string_is_crypto(field)
}

fn field_string_is_crypto(field: &ProviderField<String>) -> bool {
    matches!(field, ProviderField::Value(value) if value.eq_ignore_ascii_case("CRYPTOCURRENCY") || value.eq_ignore_ascii_case("CRYPTO"))
}

fn provenance_from_quote_map(
    request: &YahooHttpRequest,
    context: &ParseContext,
    object: &Map<String, Value>,
    provider_symbol: ProviderField<YahooSymbol>,
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<YahooProvenance, YahooAdapterError> {
    let delay = u64_field(
        object,
        "exchangeDataDelayedBy",
        "quote.exchangeDataDelayedBy",
        issues,
    );
    let delay = u64_to_u32_field(delay, "quote.exchangeDataDelayedBy", issues);
    if let ProviderField::Value(seconds) = delay
        && seconds > 0
    {
        issues.push(QualityIssue::Delayed { seconds });
    }
    Ok(YahooProvenance {
        provider: YAHOO_FINANCE_EXPERIMENTAL.to_owned(),
        pinned_client_version: PINNED_YFINANCE_VERSION.to_owned(),
        pinned_client_commit: PINNED_YFINANCE_COMMIT.to_owned(),
        request_family: request_family_name(request.family).to_owned(),
        request_target: request.target.clone(),
        provider_symbol,
        exchange: string_field(object, "exchange", "quote.exchange", bounds, issues)?,
        full_exchange_name: string_field(
            object,
            "fullExchangeName",
            "quote.fullExchangeName",
            bounds,
            issues,
        )?,
        market: string_field(object, "market", "quote.market", bounds, issues)?,
        country: select_string_field(
            [
                (object, "country", "quote.country"),
                (object, "region", "quote.region"),
            ],
            bounds,
            issues,
        )?,
        exchange_timezone_name: string_field(
            object,
            "exchangeTimezoneName",
            "quote.exchangeTimezoneName",
            bounds,
            issues,
        )?,
        exchange_delay_seconds: delay,
        provider_event_time_unix_seconds: i64_field(
            object,
            "regularMarketTime",
            "quote.regularMarketTime",
            issues,
        ),
        received_at_unix_ms: context.received_at_unix_ms,
        available_at_unix_ms: context.available_at_unix_ms,
    })
}

fn provenance_from_meta(
    request: &YahooHttpRequest,
    context: &ParseContext,
    meta: &Map<String, Value>,
    provider_symbol: ProviderField<YahooSymbol>,
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<YahooProvenance, YahooAdapterError> {
    let delay = u64_field(
        meta,
        "exchangeDataDelayedBy",
        "chart.meta.exchangeDataDelayedBy",
        issues,
    );
    let delay = u64_to_u32_field(delay, "chart.meta.exchangeDataDelayedBy", issues);
    if let ProviderField::Value(seconds) = delay
        && seconds > 0
    {
        issues.push(QualityIssue::Delayed { seconds });
    }
    Ok(YahooProvenance {
        provider: YAHOO_FINANCE_EXPERIMENTAL.to_owned(),
        pinned_client_version: PINNED_YFINANCE_VERSION.to_owned(),
        pinned_client_commit: PINNED_YFINANCE_COMMIT.to_owned(),
        request_family: request_family_name(request.family).to_owned(),
        request_target: request.target.clone(),
        provider_symbol,
        exchange: select_string_field(
            [
                (meta, "exchangeName", "chart.meta.exchangeName"),
                (meta, "exchange", "chart.meta.exchange"),
            ],
            bounds,
            issues,
        )?,
        full_exchange_name: string_field(
            meta,
            "fullExchangeName",
            "chart.meta.fullExchangeName",
            bounds,
            issues,
        )?,
        market: string_field(meta, "market", "chart.meta.market", bounds, issues)?,
        country: select_string_field(
            [
                (meta, "country", "chart.meta.country"),
                (meta, "region", "chart.meta.region"),
            ],
            bounds,
            issues,
        )?,
        exchange_timezone_name: string_field(
            meta,
            "exchangeTimezoneName",
            "chart.meta.exchangeTimezoneName",
            bounds,
            issues,
        )?,
        exchange_delay_seconds: delay,
        provider_event_time_unix_seconds: i64_field(
            meta,
            "regularMarketTime",
            "chart.meta.regularMarketTime",
            issues,
        ),
        received_at_unix_ms: context.received_at_unix_ms,
        available_at_unix_ms: context.available_at_unix_ms,
    })
}

#[allow(clippy::too_many_arguments)]
fn provenance_from_summary(
    request: &YahooHttpRequest,
    context: &ParseContext,
    provider_symbol: ProviderField<YahooSymbol>,
    quote_type: &Map<String, Value>,
    price: &Map<String, Value>,
    profile: &Map<String, Value>,
    bounds: AdapterBounds,
    issues: &mut Vec<QualityIssue>,
) -> Result<YahooProvenance, YahooAdapterError> {
    let delay = select_u64_field(
        [
            (
                price,
                "exchangeDataDelayedBy",
                "price.exchangeDataDelayedBy",
            ),
            (
                quote_type,
                "exchangeDataDelayedBy",
                "quoteType.exchangeDataDelayedBy",
            ),
        ],
        issues,
    );
    let delay = u64_to_u32_field(delay, "quoteSummary.exchangeDataDelayedBy", issues);
    if let ProviderField::Value(seconds) = &delay
        && *seconds > 0
    {
        issues.push(QualityIssue::Delayed { seconds: *seconds });
    }
    Ok(YahooProvenance {
        provider: YAHOO_FINANCE_EXPERIMENTAL.to_owned(),
        pinned_client_version: PINNED_YFINANCE_VERSION.to_owned(),
        pinned_client_commit: PINNED_YFINANCE_COMMIT.to_owned(),
        request_family: request_family_name(request.family).to_owned(),
        request_target: request.target.clone(),
        provider_symbol,
        exchange: select_string_field(
            [
                (price, "exchange", "price.exchange"),
                (quote_type, "exchange", "quoteType.exchange"),
            ],
            bounds,
            issues,
        )?,
        full_exchange_name: select_string_field(
            [
                (price, "exchangeName", "price.exchangeName"),
                (price, "fullExchangeName", "price.fullExchangeName"),
            ],
            bounds,
            issues,
        )?,
        market: select_string_field(
            [
                (price, "market", "price.market"),
                (quote_type, "market", "quoteType.market"),
            ],
            bounds,
            issues,
        )?,
        country: select_string_field(
            [
                (profile, "country", "summaryProfile.country"),
                (price, "region", "price.region"),
            ],
            bounds,
            issues,
        )?,
        exchange_timezone_name: select_string_field(
            [
                (price, "exchangeTimezoneName", "price.exchangeTimezoneName"),
                (quote_type, "timeZoneFullName", "quoteType.timeZoneFullName"),
            ],
            bounds,
            issues,
        )?,
        exchange_delay_seconds: delay,
        provider_event_time_unix_seconds: i64_field(
            price,
            "regularMarketTime",
            "price.regularMarketTime",
            issues,
        ),
        received_at_unix_ms: context.received_at_unix_ms,
        available_at_unix_ms: context.available_at_unix_ms,
    })
}

fn select_u64_field<const N: usize>(
    candidates: [(&Map<String, Value>, &str, &str); N],
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<u64> {
    let mut fallback = ProviderField::Missing;
    for (object, key, path) in candidates {
        let value = u64_field(object, key, path, issues);
        match value {
            ProviderField::Value(_) | ProviderField::Invalid => return value,
            ProviderField::Null => fallback = ProviderField::Null,
            ProviderField::Missing => {}
        }
    }
    fallback
}

fn u64_to_u32_field(
    field: ProviderField<u64>,
    path: &str,
    issues: &mut Vec<QualityIssue>,
) -> ProviderField<u32> {
    match field {
        ProviderField::Missing => ProviderField::Missing,
        ProviderField::Null => ProviderField::Null,
        ProviderField::Value(value) => match u32::try_from(value) {
            Ok(value) => ProviderField::Value(value),
            Err(_) => {
                issues.push(QualityIssue::InvalidField {
                    field: path.to_owned(),
                });
                ProviderField::Invalid
            }
        },
        ProviderField::Invalid => ProviderField::Invalid,
    }
}

fn request_family_name(family: YahooRequestFamily) -> &'static str {
    match family {
        YahooRequestFamily::Quote => "quote",
        YahooRequestFamily::ChartHistory => "chart-history",
        YahooRequestFamily::ReferenceSummary => "reference-summary",
        YahooRequestFamily::FundSummary => "fund-summary",
        YahooRequestFamily::OptionChain => "option-chain",
        YahooRequestFamily::Search => "search",
        YahooRequestFamily::Lookup => "lookup",
    }
}

fn empty_provenance(
    request: &YahooHttpRequest,
    context: &ParseContext,
    provider_symbol: ProviderField<YahooSymbol>,
) -> YahooProvenance {
    YahooProvenance {
        provider: YAHOO_FINANCE_EXPERIMENTAL.to_owned(),
        pinned_client_version: PINNED_YFINANCE_VERSION.to_owned(),
        pinned_client_commit: PINNED_YFINANCE_COMMIT.to_owned(),
        request_family: request_family_name(request.family).to_owned(),
        request_target: request.target.clone(),
        provider_symbol,
        exchange: ProviderField::Missing,
        full_exchange_name: ProviderField::Missing,
        market: ProviderField::Missing,
        country: ProviderField::Missing,
        exchange_timezone_name: ProviderField::Missing,
        exchange_delay_seconds: ProviderField::Missing,
        provider_event_time_unix_seconds: ProviderField::Missing,
        received_at_unix_ms: context.received_at_unix_ms,
        available_at_unix_ms: context.available_at_unix_ms,
    }
}

fn enrichment<T>(
    data: Option<T>,
    provenance: YahooProvenance,
    issues: Vec<QualityIssue>,
) -> YahooEnrichment<T> {
    let state = if data.is_none() {
        YahooEnrichmentState::Unavailable
    } else if issues.is_empty() {
        YahooEnrichmentState::Experimental
    } else {
        YahooEnrichmentState::Degraded
    };
    YahooEnrichment {
        state,
        authority: EvidenceAuthority::ExperimentalSupplementOnly,
        provenance,
        issues,
        data,
    }
}

fn unavailable<T>(provenance: YahooProvenance, issue: QualityIssue) -> YahooEnrichment<T> {
    YahooEnrichment {
        state: YahooEnrichmentState::Unavailable,
        authority: EvidenceAuthority::ExperimentalSupplementOnly,
        provenance,
        issues: vec![issue],
        data: None,
    }
}

fn provider_error_enrichment<T>(
    request: &YahooHttpRequest,
    context: &ParseContext,
    provider_symbol: ProviderField<YahooSymbol>,
    bounds: AdapterBounds,
    error: &Value,
) -> YahooEnrichment<T> {
    unavailable(
        empty_provenance(request, context, provider_symbol),
        provider_error_issue(error, bounds),
    )
}

fn provider_error_issue(error: &Value, bounds: AdapterBounds) -> QualityIssue {
    let object = error.as_object();
    let code = object
        .and_then(|object| object.get("code"))
        .and_then(raw_value_string)
        .unwrap_or("unknown-provider-error");
    let description = object
        .and_then(|object| object.get("description"))
        .and_then(raw_value_string)
        .unwrap_or("Yahoo returned an error without a description");
    QualityIssue::ProviderError {
        code: truncate_utf8(code, bounds.max_string_bytes),
        description: truncate_utf8(description, bounds.max_string_bytes),
    }
}

fn raw_value_string(value: &Value) -> Option<&str> {
    raw_value(value).as_str()
}

fn truncate_utf8(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.to_owned();
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value[..end].to_owned()
}

fn quote_provider_error(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    error: &Value,
) -> Result<YahooReturnedDisposition<YahooQuote>, YahooAdapterError> {
    let requested = request
        .requested_targets
        .iter()
        .map(|target| target.symbol.clone())
        .collect::<Vec<_>>();
    let issue = provider_error_issue(error, bounds);
    let observations = requested
        .iter()
        .map(|symbol| YahooEnrichment {
            state: YahooEnrichmentState::Unavailable,
            authority: EvidenceAuthority::ExperimentalSupplementOnly,
            provenance: empty_provenance(request, context, ProviderField::Value(symbol.clone())),
            issues: vec![issue.clone()],
            data: None,
        })
        .collect();
    Ok(YahooReturnedDisposition {
        requested_symbols: requested.clone(),
        provider_returned_symbols: Vec::new(),
        valid_observations: 0,
        missing_symbols: requested,
        rejected_symbols: Vec::new(),
        observations,
    })
}

impl<T> MapProviderField<T> for ProviderField<T> {
    fn map_value<U, F>(self, map: F) -> ProviderField<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            ProviderField::Missing => ProviderField::Missing,
            ProviderField::Null => ProviderField::Null,
            ProviderField::Value(value) => ProviderField::Value(map(value)),
            ProviderField::Invalid => ProviderField::Invalid,
        }
    }
}
