use chrono::{Datelike as _, Days, NaiveDate};
use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};
use url::Url;

use crate::{TiingoAdapterError, TiingoApplicationPage, TiingoTicker};

const TIINGO_API_BASE: &str = "https://api.tiingo.com";
const METADATA_MAX_RESPONSE_BYTES: usize = 256 * 1024;
const LATEST_MAX_RESPONSE_BYTES: usize = 256 * 1024;
const HISTORY_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const LATEST_MAX_ROWS: usize = 1;
const HISTORY_MAX_ROWS: usize = 400;

/// Application date-window size used because the reviewed endpoint publishes no cursor contract.
pub const MAX_HISTORY_CALENDAR_DAYS_PER_PAGE: u64 = 366;
/// Maximum application-created date windows in one bounded history plan.
pub const MAX_HISTORY_PAGES: usize = 256;

/// Closed Tiingo response family and provider-native schema-circuit scope.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TiingoEndpointFamily {
    /// Exact per-ticker metadata lookup.
    Metadata,
    /// Latest daily EOD/NAV array.
    LatestDailyPrices,
    /// One application-bounded historical date window.
    HistoricalDailyPrices,
}

/// Exact financial/date scope of one request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TiingoRequestScope {
    /// Per-ticker metadata and coverage lookup.
    Metadata,
    /// Most recent daily row.
    Latest,
    /// One inclusive application-created history window.
    History {
        /// Inclusive first requested civil date.
        start_date: CalendarDate,
        /// Inclusive last requested civil date.
        end_date: CalendarDate,
        /// Ordinal and total count of the application-created page plan.
        page: TiingoApplicationPage,
    },
}

/// One credential-free, code-owned Tiingo request description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoRequestSpec {
    ticker: TiingoTicker,
    endpoint: TiingoEndpointFamily,
    scope: TiingoRequestScope,
    url: Url,
    max_response_bytes: usize,
    max_rows: usize,
}

impl TiingoRequestSpec {
    /// Creates an exact per-ticker metadata request.
    pub fn metadata(ticker: TiingoTicker) -> Result<Self, TiingoAdapterError> {
        Self::build(ticker, TiingoRequestScope::Metadata)
    }

    /// Creates a request for the most recent daily EOD/NAV row.
    pub fn latest(ticker: TiingoTicker) -> Result<Self, TiingoAdapterError> {
        Self::build(ticker, TiingoRequestScope::Latest)
    }

    fn history(
        ticker: TiingoTicker,
        start_date: CalendarDate,
        end_date: CalendarDate,
        page: TiingoApplicationPage,
    ) -> Result<Self, TiingoAdapterError> {
        Self::build(
            ticker,
            TiingoRequestScope::History {
                start_date,
                end_date,
                page,
            },
        )
    }

    fn build(ticker: TiingoTicker, scope: TiingoRequestScope) -> Result<Self, TiingoAdapterError> {
        let endpoint = match scope {
            TiingoRequestScope::Metadata => TiingoEndpointFamily::Metadata,
            TiingoRequestScope::Latest => TiingoEndpointFamily::LatestDailyPrices,
            TiingoRequestScope::History { .. } => TiingoEndpointFamily::HistoricalDailyPrices,
        };
        let mut url = Url::parse(TIINGO_API_BASE).map_err(|_| TiingoAdapterError::RequestBuild)?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| TiingoAdapterError::RequestBuild)?;
            segments.push("tiingo");
            segments.push("daily");
            segments.push(ticker.as_str());
            if !matches!(scope, TiingoRequestScope::Metadata) {
                segments.push("prices");
            }
        }
        if let TiingoRequestScope::History {
            start_date,
            end_date,
            ..
        } = &scope
        {
            url.query_pairs_mut()
                .append_pair("startDate", &start_date.to_string())
                .append_pair("endDate", &end_date.to_string());
        }

        let (max_response_bytes, max_rows) = match endpoint {
            TiingoEndpointFamily::Metadata => (METADATA_MAX_RESPONSE_BYTES, 1),
            TiingoEndpointFamily::LatestDailyPrices => (LATEST_MAX_RESPONSE_BYTES, LATEST_MAX_ROWS),
            TiingoEndpointFamily::HistoricalDailyPrices => {
                (HISTORY_MAX_RESPONSE_BYTES, HISTORY_MAX_ROWS)
            }
        };
        Ok(Self {
            ticker,
            endpoint,
            scope,
            url,
            max_response_bytes,
            max_rows,
        })
    }

    /// Returns the exact requested provider ticker.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the closed endpoint family.
    pub const fn endpoint(&self) -> TiingoEndpointFamily {
        self.endpoint
    }

    /// Returns the exact metadata/latest/date-window request scope.
    pub const fn scope(&self) -> &TiingoRequestScope {
        &self.scope
    }

    /// Returns the credential-free code-owned URL.
    pub const fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the maximum response bytes admitted for this request family.
    pub const fn max_response_bytes(&self) -> usize {
        self.max_response_bytes
    }

    /// Returns the maximum provider rows admitted from this response.
    pub const fn max_rows(&self) -> usize {
        self.max_rows
    }

    /// Returns a credential-free digest of the exact method, URL, endpoint, and page semantics.
    pub fn request_identity(&self) -> EvidenceDigest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/tiingo/request/v1\0");
        hash.update(b"GET\0");
        hash.update(self.url.as_str().as_bytes());
        hash.update([match self.endpoint {
            TiingoEndpointFamily::Metadata => 0,
            TiingoEndpointFamily::LatestDailyPrices => 1,
            TiingoEndpointFamily::HistoricalDailyPrices => 2,
        }]);
        match &self.scope {
            TiingoRequestScope::Metadata => hash.update([0]),
            TiingoRequestScope::Latest => hash.update([1]),
            TiingoRequestScope::History {
                start_date,
                end_date,
                page,
            } => {
                hash.update([2]);
                hash.update(start_date.to_string().as_bytes());
                hash.update([0]);
                hash.update(end_date.to_string().as_bytes());
                hash.update(page.ordinal().to_be_bytes());
                hash.update(page.count().to_be_bytes());
            }
        }
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
    }
}

/// A complete bounded history plan split into inclusive application date windows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoHistoryPlan {
    ticker: TiingoTicker,
    start_date: CalendarDate,
    end_date: CalendarDate,
    pages: Box<[TiingoRequestSpec]>,
    maximum_response_bytes: u64,
}

impl TiingoHistoryPlan {
    /// Splits an inclusive date range without inventing provider cursor pagination.
    ///
    /// # Errors
    ///
    /// Rejects inverted, unrepresentable, or more-than-256-page requests.
    pub fn try_new(
        ticker: TiingoTicker,
        start_date: CalendarDate,
        end_date: CalendarDate,
    ) -> Result<Self, TiingoAdapterError> {
        let start = to_naive_date(start_date)?;
        let end = to_naive_date(end_date)?;
        if start > end {
            return Err(TiingoAdapterError::InvalidDateRange);
        }

        let mut windows = Vec::new();
        let mut cursor = start;
        while cursor <= end {
            if windows.len() == MAX_HISTORY_PAGES {
                return Err(TiingoAdapterError::HistoryTooLarge);
            }
            let candidate_end = cursor
                .checked_add_days(Days::new(MAX_HISTORY_CALENDAR_DAYS_PER_PAGE - 1))
                .ok_or(TiingoAdapterError::InvalidDateRange)?;
            let page_end = candidate_end.min(end);
            windows.push((from_naive_date(cursor)?, from_naive_date(page_end)?));
            cursor = page_end
                .checked_add_days(Days::new(1))
                .ok_or(TiingoAdapterError::InvalidDateRange)?;
        }

        let page_count =
            u16::try_from(windows.len()).map_err(|_| TiingoAdapterError::HistoryTooLarge)?;
        let mut pages = Vec::with_capacity(windows.len());
        for (index, (page_start, page_end)) in windows.into_iter().enumerate() {
            let ordinal =
                u16::try_from(index + 1).map_err(|_| TiingoAdapterError::HistoryTooLarge)?;
            pages.push(TiingoRequestSpec::history(
                ticker.clone(),
                page_start,
                page_end,
                TiingoApplicationPage::try_new(ordinal, page_count)?,
            )?);
        }

        let maximum_response_bytes = pages.iter().try_fold(0_u64, |total, page| {
            let page_bytes = u64::try_from(page.max_response_bytes())
                .map_err(|_| TiingoAdapterError::HistoryTooLarge)?;
            total
                .checked_add(page_bytes)
                .ok_or(TiingoAdapterError::HistoryTooLarge)
        })?;
        Ok(Self {
            ticker,
            start_date,
            end_date,
            pages: pages.into_boxed_slice(),
            maximum_response_bytes,
        })
    }

    /// Returns the exact provider ticker for every page.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the complete inclusive request interval.
    pub const fn interval(&self) -> (CalendarDate, CalendarDate) {
        (self.start_date, self.end_date)
    }

    /// Returns every contiguous application-created date-window request.
    pub fn pages(&self) -> &[TiingoRequestSpec] {
        &self.pages
    }

    /// Returns the complete plan's worst-case response bytes for pre-dispatch storage/quota
    /// admission. A scheduler must admit this aggregate capacity before requesting page one.
    pub const fn maximum_response_bytes(&self) -> u64 {
        self.maximum_response_bytes
    }

    /// Returns the credential-free identity of the complete ordered application date-window plan.
    pub fn request_set_identity(&self) -> EvidenceDigest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/tiingo/history-request-set/v2\0");
        hash.update(self.ticker.as_str().as_bytes());
        hash.update(self.start_date.to_string().as_bytes());
        hash.update([0]);
        hash.update(self.end_date.to_string().as_bytes());
        hash.update(
            u64::try_from(self.pages.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hash.update(self.maximum_response_bytes.to_be_bytes());
        for page in &self.pages {
            hash.update(page.request_identity().bytes());
        }
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
    }
}

fn to_naive_date(date: CalendarDate) -> Result<NaiveDate, TiingoAdapterError> {
    NaiveDate::from_ymd_opt(
        i32::from(date.year()),
        u32::from(date.month()),
        u32::from(date.day()),
    )
    .ok_or(TiingoAdapterError::InvalidDateRange)
}

fn from_naive_date(date: NaiveDate) -> Result<CalendarDate, TiingoAdapterError> {
    let year = u16::try_from(date.year()).map_err(|_| TiingoAdapterError::InvalidDateRange)?;
    let month = u8::try_from(date.month()).map_err(|_| TiingoAdapterError::InvalidDateRange)?;
    let day = u8::try_from(date.day()).map_err(|_| TiingoAdapterError::InvalidDateRange)?;
    CalendarDate::new(year, month, day).map_err(|_| TiingoAdapterError::InvalidDateRange)
}
