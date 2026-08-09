//! Official Nasdaq Trader Symbol Directory extraction for the U.S.-listed reference universe.
//!
//! This crate preserves current listing-reference fields and exact source-file lineage. It does
//! not provide quotes, trades, market depth, trading status, or execution-quality evidence.

mod client;
mod model;
mod parser;
mod source;

pub use model::{
    NasdaqDirectoryKind, NasdaqDirectoryPresence, NasdaqFileCreationTime, NasdaqFinancialStatus,
    NasdaqListingRecord, NasdaqMarketCategory, NasdaqModelError, NasdaqOtherExchange,
    NasdaqProviderFields,
};
pub use parser::{MAX_DIRECTORY_RECORDS, MAX_SOURCE_BYTES, NasdaqParseError};
pub use source::{
    NASDAQ_LISTED_URL, NASDAQ_SYMBOL_DIRECTORY_DATASET, NASDAQ_SYMBOL_DIRECTORY_PROVIDER,
    NASDAQ_SYMBOL_DIRECTORY_VENUES, NasdaqDirectoryHealth, NasdaqSymbolDirectoryConfig,
    NasdaqSymbolDirectorySource, NasdaqSymbolDirectorySourceError, OTHER_LISTED_URL,
};
