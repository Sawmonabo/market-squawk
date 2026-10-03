//! Reviewed official numeric field tables; no fixture-derived field identities.
use super::*;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};
const ORIGINAL: &[u8] =
    include_bytes!("../../../../resources/schwab/market-data-documentation-20260916.json");
const DIGEST: [u8; 32] = [
    9, 251, 176, 9, 121, 254, 226, 80, 182, 132, 189, 185, 167, 228, 178, 99, 196, 42, 70, 89, 7,
    52, 67, 119, 55, 58, 53, 133, 90, 216, 102, 13,
];
/// Symbol, bid, ask, bid size, ask size, original quote clock from each service's own table.
pub(super) fn fields(service: MarketDataService) -> Result<Vec<u16>, ServiceError> {
    match service {
        MarketDataService::LevelOneEquities => Ok(vec![0, 1, 2, 4, 5, 34]),
        MarketDataService::LevelOneOptions => Ok(vec![0, 2, 3, 16, 17, 38]),
        MarketDataService::LevelOneFutures | MarketDataService::LevelOneFuturesOptions => {
            Ok(vec![0, 1, 2, 4, 5, 10])
        }
        MarketDataService::LevelOneForex => Ok(vec![0, 1, 2, 4, 5, 8]),
        MarketDataService::NyseBook
        | MarketDataService::NasdaqBook
        | MarketDataService::OptionsBook => Ok(vec![0, 1, 2, 3]),
        MarketDataService::ChartEquity => Ok((0..=8).collect()),
        MarketDataService::ChartFutures => Ok((0..=6).collect()),
        MarketDataService::ScreenerEquity | MarketDataService::ScreenerOption => {
            Ok((0..=4).collect())
        }
    }
}
pub(super) fn dictionary(
    service: MarketDataService,
) -> Result<SchwabStreamerFieldDictionary, ServiceError> {
    if <[u8; 32]>::from(Sha256::digest(ORIGINAL)) != DIGEST {
        return Err(ServiceError::InvalidResult);
    }
    let ids = fields(service)?;
    use SchwabStreamerSemanticField as F;
    let names = match service {
        MarketDataService::NyseBook
        | MarketDataService::NasdaqBook
        | MarketDataService::OptionsBook => {
            vec![F::Symbol, F::SnapshotTime, F::BidBook, F::AskBook]
        }
        MarketDataService::ChartEquity => vec![
            F::Symbol,
            F::OpenPrice,
            F::HighPrice,
            F::LowPrice,
            F::ClosePrice,
            F::Volume,
            F::Sequence,
            F::ChartTime,
            F::ChartDay,
        ],
        MarketDataService::ChartFutures => vec![
            F::Symbol,
            F::ChartTime,
            F::OpenPrice,
            F::HighPrice,
            F::LowPrice,
            F::ClosePrice,
            F::Volume,
        ],
        MarketDataService::ScreenerEquity | MarketDataService::ScreenerOption => vec![
            F::Symbol,
            F::SnapshotTime,
            F::SortField,
            F::Frequency,
            F::Items,
        ],
        _ => vec![
            F::Symbol,
            F::BidPrice,
            F::AskPrice,
            F::BidSize,
            F::AskSize,
            F::QuoteTime,
        ],
    };
    SchwabStreamerFieldDictionary::try_new(
        service,
        SourceIdentifier::try_from("schwab-streamer-official-20240627")
            .map_err(|_| ServiceError::Internal)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, DIGEST),
        ids.into_iter().zip(names).collect(),
    )
    .map_err(|_| ServiceError::InvalidResult)
}
