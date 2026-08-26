use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    ExplicitDemand, PINNED_YFINANCE_COMMIT, PINNED_YFINANCE_VERSION, YahooAdapterError,
    YahooAssetClass, YahooSymbol, YahooTarget,
};

const QUERY1_BASE: &str = "https://query1.finance.yahoo.com";
const QUERY2_BASE: &str = "https://query2.finance.yahoo.com";

/// Application-owned safety bounds. None of these values is a Yahoo provider quota.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdapterBounds {
    pub max_symbols_per_operation: usize,
    pub max_response_bytes: usize,
    pub max_records_per_response: usize,
    pub max_option_contracts: usize,
    pub max_option_expirations: usize,
    pub max_fund_holdings: usize,
    pub max_string_bytes: usize,
}

impl AdapterBounds {
    pub fn validate(self) -> Result<Self, YahooAdapterError> {
        for (name, value) in [
            ("max_symbols_per_operation", self.max_symbols_per_operation),
            ("max_response_bytes", self.max_response_bytes),
            ("max_records_per_response", self.max_records_per_response),
            ("max_option_contracts", self.max_option_contracts),
            ("max_option_expirations", self.max_option_expirations),
            ("max_fund_holdings", self.max_fund_holdings),
            ("max_string_bytes", self.max_string_bytes),
        ] {
            if value == 0 {
                return Err(YahooAdapterError::ZeroApplicationBound { name });
            }
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooLocale {
    language: String,
    region: String,
}

impl YahooLocale {
    pub fn new(
        language: impl Into<String>,
        region: impl Into<String>,
        max_string_bytes: usize,
    ) -> Result<Self, YahooAdapterError> {
        let language = language.into();
        let region = region.into();
        for value in [&language, &region] {
            if value.is_empty()
                || value.len() > max_string_bytes
                || value
                    .chars()
                    .any(|character| !(character.is_ascii_alphanumeric() || character == '-'))
            {
                return Err(YahooAdapterError::InvalidLocale);
            }
        }
        Ok(Self { language, region })
    }

    pub fn language(&self) -> &str {
        &self.language
    }

    pub fn region(&self) -> &str {
        &self.region
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum YahooHttpMethod {
    Get,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum YahooRequestFamily {
    Quote,
    ChartHistory,
    ReferenceSummary,
    FundSummary,
    OptionChain,
    Search,
    Lookup,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChartInterval {
    OneMinute,
    TwoMinutes,
    FiveMinutes,
    FifteenMinutes,
    ThirtyMinutes,
    SixtyMinutes,
    NinetyMinutes,
    OneHour,
    OneDay,
    FiveDays,
    OneWeek,
    OneMonth,
    ThreeMonths,
}

impl ChartInterval {
    pub(crate) const fn provider_value(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::TwoMinutes => "2m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::ThirtyMinutes => "30m",
            Self::SixtyMinutes => "60m",
            Self::NinetyMinutes => "90m",
            Self::OneHour => "1h",
            Self::OneDay => "1d",
            Self::FiveDays => "5d",
            Self::OneWeek => "1wk",
            Self::OneMonth => "1mo",
            Self::ThreeMonths => "3mo",
        }
    }

    pub(crate) fn from_provider_value(value: &str) -> Option<Self> {
        match value {
            "1m" => Some(Self::OneMinute),
            "2m" => Some(Self::TwoMinutes),
            "5m" => Some(Self::FiveMinutes),
            "15m" => Some(Self::FifteenMinutes),
            "30m" => Some(Self::ThirtyMinutes),
            "60m" => Some(Self::SixtyMinutes),
            "90m" => Some(Self::NinetyMinutes),
            "1h" => Some(Self::OneHour),
            "1d" => Some(Self::OneDay),
            "5d" => Some(Self::FiveDays),
            "1wk" => Some(Self::OneWeek),
            "1mo" => Some(Self::OneMonth),
            "3mo" => Some(Self::ThreeMonths),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ChartWindow {
    OneDay,
    FiveDays,
    OneMonth,
    ThreeMonths,
    SixMonths,
    OneYear,
    TwoYears,
    FiveYears,
    TenYears,
    YearToDate,
    Maximum,
    UnixRange {
        start_unix_seconds: i64,
        end_exclusive_unix_seconds: i64,
    },
}

impl ChartWindow {
    pub(crate) const fn provider_range_value(self) -> Option<&'static str> {
        match self {
            Self::OneDay => Some("1d"),
            Self::FiveDays => Some("5d"),
            Self::OneMonth => Some("1mo"),
            Self::ThreeMonths => Some("3mo"),
            Self::SixMonths => Some("6mo"),
            Self::OneYear => Some("1y"),
            Self::TwoYears => Some("2y"),
            Self::FiveYears => Some("5y"),
            Self::TenYears => Some("10y"),
            Self::YearToDate => Some("ytd"),
            Self::Maximum => Some("max"),
            Self::UnixRange { .. } => None,
        }
    }

    pub(crate) fn from_provider_range(value: &str) -> Option<Self> {
        match value {
            "1d" => Some(Self::OneDay),
            "5d" => Some(Self::FiveDays),
            "1mo" => Some(Self::OneMonth),
            "3mo" => Some(Self::ThreeMonths),
            "6mo" => Some(Self::SixMonths),
            "1y" => Some(Self::OneYear),
            "2y" => Some(Self::TwoYears),
            "5y" => Some(Self::FiveYears),
            "10y" => Some(Self::TenYears),
            "ytd" => Some(Self::YearToDate),
            "max" => Some(Self::Maximum),
            _ => None,
        }
    }

    fn append(self, url: &mut Url) -> Result<(), YahooAdapterError> {
        let mut query = url.query_pairs_mut();
        match self {
            Self::UnixRange {
                start_unix_seconds,
                end_exclusive_unix_seconds,
            } => {
                if start_unix_seconds >= end_exclusive_unix_seconds {
                    return Err(YahooAdapterError::InvalidHistoryWindow);
                }
                query.append_pair("period1", &start_unix_seconds.to_string());
                query.append_pair("period2", &end_exclusive_unix_seconds.to_string());
            }
            window => {
                let range = window
                    .provider_range_value()
                    .ok_or(YahooAdapterError::InvalidHistoryWindow)?;
                query.append_pair("range", range);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LookupKind {
    Equity,
    MutualFund,
    Etf,
    Index,
}

impl LookupKind {
    const fn provider_value(self) -> &'static str {
        match self {
            Self::Equity => "equity",
            Self::MutualFund => "mutualfund",
            Self::Etf => "etf",
            Self::Index => "index",
        }
    }
}

/// Exact effective provider request. Cookies, crumbs, fallback, and retries belong to transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct YahooHttpRequest {
    pub(crate) method: YahooHttpMethod,
    pub(crate) family: YahooRequestFamily,
    pub(crate) target: String,
    pub(crate) request_key: String,
    pub(crate) demand: ExplicitDemand,
    pub(crate) requested_targets: Vec<YahooTarget>,
    pub(crate) effective_arguments: BTreeMap<String, String>,
    pub(crate) requires_cookie_crumb_session: bool,
}

impl YahooHttpRequest {
    pub const fn method(&self) -> YahooHttpMethod {
        self.method
    }

    pub const fn family(&self) -> YahooRequestFamily {
        self.family
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn demand(&self) -> &ExplicitDemand {
        &self.demand
    }

    pub fn requested_targets(&self) -> &[YahooTarget] {
        &self.requested_targets
    }

    pub fn effective_arguments(&self) -> &BTreeMap<String, String> {
        &self.effective_arguments
    }

    pub fn requested_symbol_count(&self) -> usize {
        self.requested_targets.len()
    }
}

/// A plan is explicit about the number of symbol-specific upstream request units.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooRequestPlan {
    pub requests: Vec<YahooHttpRequest>,
    pub history_fans_out_per_ticker: bool,
}

impl YahooRequestPlan {
    pub fn actual_primary_attempt_units(&self) -> usize {
        self.requests.len()
    }
}

#[derive(Clone, Debug)]
pub struct YahooRequestPlanner {
    bounds: AdapterBounds,
    locale: YahooLocale,
}

impl YahooRequestPlanner {
    pub fn new(bounds: AdapterBounds, locale: YahooLocale) -> Result<Self, YahooAdapterError> {
        Ok(Self {
            bounds: bounds.validate()?,
            locale,
        })
    }

    pub const fn bounds(&self) -> AdapterBounds {
        self.bounds
    }

    pub fn quote(
        &self,
        demand: ExplicitDemand,
        targets: Vec<YahooTarget>,
    ) -> Result<YahooRequestPlan, YahooAdapterError> {
        self.validate_targets(&targets)?;
        let mut url = Url::parse(&format!("{QUERY1_BASE}/v7/finance/quote"))
            .map_err(|error| YahooAdapterError::InvalidUrl(error.to_string()))?;
        let symbols = targets
            .iter()
            .map(|target| target.symbol.as_str())
            .collect::<Vec<_>>()
            .join(",");
        url.query_pairs_mut()
            .append_pair("symbols", &symbols)
            .append_pair("formatted", "false")
            .append_pair("lang", self.locale.language())
            .append_pair("region", self.locale.region());
        Ok(YahooRequestPlan {
            requests: vec![self.request(demand, YahooRequestFamily::Quote, url, targets, [])],
            history_fans_out_per_ticker: false,
        })
    }

    pub fn chart_history(
        &self,
        demand: ExplicitDemand,
        targets: Vec<YahooTarget>,
        window: ChartWindow,
        interval: ChartInterval,
        include_pre_post: bool,
    ) -> Result<YahooRequestPlan, YahooAdapterError> {
        self.validate_targets(&targets)?;
        let mut requests = Vec::new();
        requests.try_reserve(targets.len()).map_err(|_| {
            YahooAdapterError::ApplicationBoundExceeded {
                name: "history_request_plan_allocation",
                actual: targets.len(),
                maximum: self.bounds.max_symbols_per_operation,
            }
        })?;
        for target in targets {
            let mut url = self.symbol_path(QUERY2_BASE, "/v8/finance/chart", &target.symbol)?;
            window.append(&mut url)?;
            url.query_pairs_mut()
                .append_pair("interval", interval.provider_value())
                .append_pair(
                    "includePrePost",
                    if include_pre_post { "true" } else { "false" },
                )
                .append_pair("events", "div,splits,capitalGains")
                .append_pair("includeAdjustedClose", "true");
            let arguments = [
                ("auto_adjust", "false"),
                ("repair", "false"),
                ("transient_retries", "0"),
            ];
            requests.push(self.request(
                demand.clone(),
                YahooRequestFamily::ChartHistory,
                url,
                vec![target],
                arguments,
            ));
        }
        Ok(YahooRequestPlan {
            requests,
            history_fans_out_per_ticker: true,
        })
    }

    pub fn reference(
        &self,
        demand: ExplicitDemand,
        target: YahooTarget,
    ) -> Result<YahooRequestPlan, YahooAdapterError> {
        self.validate_targets(std::slice::from_ref(&target))?;
        let mut url = self.symbol_path(QUERY2_BASE, "/v10/finance/quoteSummary", &target.symbol)?;
        url.query_pairs_mut()
            .append_pair("modules", "quoteType,price,summaryDetail,summaryProfile")
            .append_pair("corsDomain", "finance.yahoo.com")
            .append_pair("formatted", "false")
            .append_pair("symbol", target.symbol.as_str())
            .append_pair("lang", self.locale.language())
            .append_pair("region", self.locale.region());
        Ok(YahooRequestPlan {
            requests: vec![self.request(
                demand,
                YahooRequestFamily::ReferenceSummary,
                url,
                vec![target],
                [],
            )],
            history_fans_out_per_ticker: false,
        })
    }

    pub fn fund(
        &self,
        demand: ExplicitDemand,
        target: YahooTarget,
    ) -> Result<YahooRequestPlan, YahooAdapterError> {
        if target.asset_class != YahooAssetClass::Etf
            && target.asset_class != YahooAssetClass::MutualFund
        {
            return Err(YahooAdapterError::InvalidSchema {
                path: "request.target.asset_class".to_owned(),
                reason: "fund request requires ETF or mutual-fund target".to_owned(),
            });
        }
        self.validate_targets(std::slice::from_ref(&target))?;
        let mut url = self.symbol_path(QUERY2_BASE, "/v10/finance/quoteSummary", &target.symbol)?;
        url.query_pairs_mut()
            .append_pair(
                "modules",
                "quoteType,summaryProfile,topHoldings,fundProfile",
            )
            .append_pair("corsDomain", "finance.yahoo.com")
            .append_pair("formatted", "false")
            .append_pair("symbol", target.symbol.as_str())
            .append_pair("lang", self.locale.language())
            .append_pair("region", self.locale.region());
        Ok(YahooRequestPlan {
            requests: vec![self.request(
                demand,
                YahooRequestFamily::FundSummary,
                url,
                vec![target],
                [],
            )],
            history_fans_out_per_ticker: false,
        })
    }

    pub fn option_chain(
        &self,
        demand: ExplicitDemand,
        target: YahooTarget,
        expiration_unix_seconds: Option<i64>,
    ) -> Result<YahooRequestPlan, YahooAdapterError> {
        if target.asset_class != YahooAssetClass::Equity
            && target.asset_class != YahooAssetClass::Etf
            && target.asset_class != YahooAssetClass::Index
            && target.asset_class != YahooAssetClass::OptionUnderlying
        {
            return Err(YahooAdapterError::InvalidSchema {
                path: "request.target.asset_class".to_owned(),
                reason: "option request requires an admitted underlying".to_owned(),
            });
        }
        self.validate_targets(std::slice::from_ref(&target))?;
        let mut url = self.symbol_path(QUERY2_BASE, "/v7/finance/options", &target.symbol)?;
        if let Some(expiration) = expiration_unix_seconds {
            if expiration <= 0 {
                return Err(YahooAdapterError::InvalidOptionExpiration);
            }
            url.query_pairs_mut()
                .append_pair("date", &expiration.to_string());
        }
        let arguments = expiration_unix_seconds
            .map(|value| [("requested_expiration_unix_seconds", value.to_string())]);
        let mut effective_arguments = BTreeMap::new();
        if let Some(arguments) = arguments {
            effective_arguments.extend(arguments.map(|(key, value)| (key.to_owned(), value)));
        }
        Ok(YahooRequestPlan {
            requests: vec![self.request_with_arguments(
                demand,
                YahooRequestFamily::OptionChain,
                url,
                vec![target],
                effective_arguments,
            )],
            history_fans_out_per_ticker: false,
        })
    }

    pub fn search(
        &self,
        demand: ExplicitDemand,
        text: impl Into<String>,
        requested_results: usize,
    ) -> Result<YahooRequestPlan, YahooAdapterError> {
        let text = self.validate_search(text.into(), requested_results)?;
        let mut url = Url::parse(&format!("{QUERY2_BASE}/v1/finance/search"))
            .map_err(|error| YahooAdapterError::InvalidUrl(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("q", &text)
            .append_pair("quotesCount", &requested_results.to_string())
            .append_pair("enableFuzzyQuery", "false")
            .append_pair("newsCount", "0")
            .append_pair("listsCount", "0")
            .append_pair("enableCb", "false")
            .append_pair("enableNavLinks", "false")
            .append_pair("enableResearchReports", "false")
            .append_pair("enableCulturalAssets", "false")
            .append_pair("recommendedCount", "0");
        let arguments = [("requested_result_count", requested_results.to_string())]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect();
        Ok(YahooRequestPlan {
            requests: vec![self.request_with_arguments(
                demand,
                YahooRequestFamily::Search,
                url,
                Vec::new(),
                arguments,
            )],
            history_fans_out_per_ticker: false,
        })
    }

    pub fn lookup(
        &self,
        demand: ExplicitDemand,
        text: impl Into<String>,
        kind: LookupKind,
        requested_results: usize,
    ) -> Result<YahooRequestPlan, YahooAdapterError> {
        let text = self.validate_search(text.into(), requested_results)?;
        let mut url = Url::parse(&format!("{QUERY1_BASE}/v1/finance/lookup"))
            .map_err(|error| YahooAdapterError::InvalidUrl(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("query", &text)
            .append_pair("type", kind.provider_value())
            .append_pair("start", "0")
            .append_pair("count", &requested_results.to_string())
            .append_pair("formatted", "false")
            .append_pair("fetchPricingData", "true")
            .append_pair("lang", self.locale.language())
            .append_pair("region", self.locale.region());
        let arguments = [("requested_result_count", requested_results.to_string())]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect();
        Ok(YahooRequestPlan {
            requests: vec![self.request_with_arguments(
                demand,
                YahooRequestFamily::Lookup,
                url,
                Vec::new(),
                arguments,
            )],
            history_fans_out_per_ticker: false,
        })
    }

    fn validate_search(
        &self,
        text: String,
        requested_results: usize,
    ) -> Result<String, YahooAdapterError> {
        if text.trim().is_empty() {
            return Err(YahooAdapterError::EmptySearchText);
        }
        if text.len() > self.bounds.max_string_bytes {
            return Err(YahooAdapterError::StringTooLong {
                path: "request.search_text".to_owned(),
            });
        }
        if requested_results == 0 {
            return Err(YahooAdapterError::ZeroApplicationBound {
                name: "requested_results",
            });
        }
        if requested_results > self.bounds.max_records_per_response {
            return Err(YahooAdapterError::ApplicationBoundExceeded {
                name: "requested_results",
                actual: requested_results,
                maximum: self.bounds.max_records_per_response,
            });
        }
        Ok(text)
    }

    fn validate_targets(&self, targets: &[YahooTarget]) -> Result<(), YahooAdapterError> {
        if targets.is_empty() {
            return Err(YahooAdapterError::EmptySymbolSet);
        }
        if targets.len() > self.bounds.max_symbols_per_operation {
            return Err(YahooAdapterError::ApplicationBoundExceeded {
                name: "max_symbols_per_operation",
                actual: targets.len(),
                maximum: self.bounds.max_symbols_per_operation,
            });
        }
        let mut symbols = BTreeSet::new();
        for target in targets {
            if !symbols.insert(target.symbol.clone()) {
                return Err(YahooAdapterError::DuplicateSymbol(
                    target.symbol.as_str().to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn symbol_path(
        &self,
        base: &str,
        prefix: &str,
        symbol: &YahooSymbol,
    ) -> Result<Url, YahooAdapterError> {
        let mut url =
            Url::parse(base).map_err(|error| YahooAdapterError::InvalidUrl(error.to_string()))?;
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                YahooAdapterError::InvalidUrl(
                    "Yahoo base URL cannot accept path segments".to_owned(),
                )
            })?;
            segments.pop_if_empty();
            for segment in prefix.trim_start_matches('/').split('/') {
                segments.push(segment);
            }
            segments.push(symbol.as_str());
        }
        Ok(url)
    }

    fn request<const N: usize>(
        &self,
        demand: ExplicitDemand,
        family: YahooRequestFamily,
        url: Url,
        requested_targets: Vec<YahooTarget>,
        arguments: [(&str, &str); N],
    ) -> YahooHttpRequest {
        let effective_arguments = arguments
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        self.request_with_arguments(demand, family, url, requested_targets, effective_arguments)
    }

    fn request_with_arguments(
        &self,
        demand: ExplicitDemand,
        family: YahooRequestFamily,
        url: Url,
        requested_targets: Vec<YahooTarget>,
        mut effective_arguments: BTreeMap<String, String>,
    ) -> YahooHttpRequest {
        effective_arguments.insert(
            "pinned_yfinance_version".to_owned(),
            PINNED_YFINANCE_VERSION.to_owned(),
        );
        effective_arguments.insert(
            "pinned_yfinance_commit".to_owned(),
            PINNED_YFINANCE_COMMIT.to_owned(),
        );
        let target: String = url.into();
        YahooHttpRequest {
            method: YahooHttpMethod::Get,
            family,
            request_key: target.clone(),
            target,
            demand,
            requested_targets,
            effective_arguments,
            requires_cookie_crumb_session: true,
        }
    }
}
