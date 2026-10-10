//! Maintained economic-date queries; no invented provider cursor or split upper bound.
use super::*;
impl TiingoRequestSpec {
    /// Requests one ticker's distributions for one exact inclusive ex-date.
    pub fn corporate_action_distributions(
        ticker: TiingoTicker,
        date: CalendarDate,
    ) -> Result<Self, TiingoAdapterError> {
        Self::current_action(ticker, date, false)
    }
    /// Requests the complete split batch for one ex-date. The target ticker does not narrow the
    /// provider response; all returned symbols must remain in original decode/capture evidence.
    pub fn corporate_action_splits(
        ticker: TiingoTicker,
        date: CalendarDate,
    ) -> Result<Self, TiingoAdapterError> {
        Self::current_action(ticker, date, true)
    }
    fn current_action(
        ticker: TiingoTicker,
        date: CalendarDate,
        splits: bool,
    ) -> Result<Self, TiingoAdapterError> {
        let mut url = Url::parse(TIINGO_API_BASE).map_err(|_| TiingoAdapterError::RequestBuild)?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| TiingoAdapterError::RequestBuild)?;
            path.push("tiingo").push("corporate-actions");
            if !splits {
                path.push(ticker.as_str());
            }
            path.push(if splits { "splits" } else { "distributions" });
        }
        if splits {
            url.query_pairs_mut()
                .append_pair("exDate", &date.to_string());
        } else {
            url.query_pairs_mut()
                .append_pair("startExDate", &date.to_string())
                .append_pair("endExDate", &date.to_string());
        }
        Ok(Self {
            ticker,
            endpoint: if splits {
                TiingoEndpointFamily::CorporateActionSplits
            } else {
                TiingoEndpointFamily::CorporateActionDistributions
            },
            scope: if splits {
                TiingoRequestScope::Splits { date }
            } else {
                TiingoRequestScope::Distributions { date }
            },
            url,
            max_response_bytes: 2 * 1024 * 1024,
            max_rows: 4096,
        })
    }
}
