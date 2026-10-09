//! Reviewed numeric field tables with documented corrections to the published wire contract.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};

use super::{
    SchwabCanonicalError, SchwabStreamerFieldDictionary, SchwabStreamerSemanticField as F,
};
use crate::MarketDataService;

// Reviewed source snapshot: resources/market-data-documentation-20260916.json.
// The static table and digest are source provenance; constructing a dictionary does not rescan
// the immutable documentation blob in ordinary editable-source development.
const OFFICIAL_EVIDENCE_SHA256: [u8; 32] = [
    9, 251, 176, 9, 121, 254, 226, 80, 182, 132, 189, 185, 167, 228, 178, 99, 196, 42, 70, 89, 7,
    52, 67, 119, 55, 58, 53, 133, 90, 216, 102, 13,
];

impl SchwabStreamerFieldDictionary {
    /// Reviewed field meanings, retaining the official source and any named wire correction.
    pub fn official(service: MarketDataService) -> Result<Self, SchwabCanonicalError> {
        let fields: &[(u16, F)] = match service {
            MarketDataService::LevelOneEquities => &[
                (0, F::Symbol),
                (1, F::BidPrice),
                (2, F::AskPrice),
                (4, F::BidSize),
                (5, F::AskSize),
                (34, F::QuoteTime),
            ],
            MarketDataService::LevelOneOptions => &[
                (0, F::Symbol),
                (2, F::BidPrice),
                (3, F::AskPrice),
                (16, F::BidSize),
                (17, F::AskSize),
                (38, F::QuoteTime),
            ],
            MarketDataService::LevelOneFutures | MarketDataService::LevelOneFuturesOptions => &[
                (0, F::Symbol),
                (1, F::BidPrice),
                (2, F::AskPrice),
                (4, F::BidSize),
                (5, F::AskSize),
                (10, F::QuoteTime),
            ],
            MarketDataService::LevelOneForex => &[
                (0, F::Symbol),
                (1, F::BidPrice),
                (2, F::AskPrice),
                (4, F::BidSize),
                (5, F::AskSize),
                (8, F::QuoteTime),
            ],
            MarketDataService::NyseBook
            | MarketDataService::NasdaqBook
            | MarketDataService::OptionsBook => &[
                (0, F::Symbol),
                (1, F::SnapshotTime),
                (2, F::BidBook),
                (3, F::AskBook),
            ],
            MarketDataService::ChartEquity => &[
                (0, F::Symbol),
                (1, F::Sequence),
                (2, F::OpenPrice),
                (3, F::HighPrice),
                (4, F::LowPrice),
                (5, F::ClosePrice),
                (6, F::Volume),
                (7, F::ChartTime),
                (8, F::ChartDay),
            ],
            MarketDataService::ChartFutures => &[
                (0, F::Symbol),
                (1, F::ChartTime),
                (2, F::OpenPrice),
                (3, F::HighPrice),
                (4, F::LowPrice),
                (5, F::ClosePrice),
                (6, F::Volume),
            ],
            MarketDataService::ScreenerEquity | MarketDataService::ScreenerOption => &[
                (0, F::Symbol),
                (1, F::SnapshotTime),
                (2, F::SortField),
                (3, F::Frequency),
                (4, F::Items),
            ],
        };
        // The retained official CHART_EQUITY table transposes fields 1–6. The fixed wire
        // order matches native captures and ChartEquityFields in schwab-py, independently
        // documented at https://schwaby.readthedocs.io/en/stable/streaming.html#equity-charts
        // and https://github.com/alexgolec/schwab-py/blob/main/schwab/streaming.py .
        // Keep the original document digest; the version names this reviewed correction.
        let version = if service == MarketDataService::ChartEquity {
            "schwab-streamer-chart-equity-wire-20261009"
        } else {
            "schwab-streamer-official-20240627"
        };
        Self::try_new(
            service,
            SourceIdentifier::try_from(version)
                .map_err(|_| SchwabCanonicalError::DictionaryInvalid)?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, OFFICIAL_EVIDENCE_SHA256),
            fields.to_vec(),
        )
    }

    /// Numeric subscription fields in ascending order, matching this dictionary's meanings.
    pub fn field_ids(&self) -> impl Iterator<Item = u16> + '_ {
        self.fields.keys().copied()
    }
}
