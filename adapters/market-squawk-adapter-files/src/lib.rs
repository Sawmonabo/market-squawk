//! Bounded extraction from explicitly user-authorized local financial files.
//!
//! The adapter never accepts an ambient file path after construction. Discovery and extraction
//! each obtain a fresh one-shot capability below
//! [`UserAuthorizedInputRoot`](market_squawk_platform::UserAuthorizedInputRoot), bind exact source
//! bytes to SHA-256 evidence, and enforce format-independent resource and time ceilings.

mod clock;
mod contracts;
mod csv;
mod database;
mod excel;
mod json;
mod manifest;
mod ofx;
mod parquet;
mod source;
mod xml;

pub use clock::{
    ExtractionClock, ExtractionClockError, ExtractionClockReading, SystemExtractionClock,
};
pub use contracts::{ExtractionLimits, ExtractionLimitsInput, FileAdapterError, ParserLimit};
pub use source::FileExtractionSource;

pub(crate) use contracts::{CellValue, ParseBudget, ParsedRow};
pub(crate) use manifest::FormulaPolicy;
