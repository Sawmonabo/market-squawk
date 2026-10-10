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
#[cfg(feature = "fuzzing")]
mod fuzzing;
mod guided_manifest;
mod json;
mod manifest;
mod manifest_bounds;
mod ofx;
mod parquet;
mod parse;
mod preview;
mod representation;
mod source;
mod xml;

pub use clock::{
    ExtractionClock, ExtractionClockError, ExtractionClockReading, SystemExtractionClock,
};
pub use contracts::{ExtractionLimits, ExtractionLimitsInput, FileAdapterError, ParserLimit};
#[cfg(feature = "fuzzing")]
pub use fuzzing::{FuzzFileFormat, fuzz_parse_bytes};
pub use guided_manifest::{
    GuidedInstrumentBinding, GuidedManifest, GuidedManifestInput, GuidedManifestObject,
    GuidedObjectTime, GuidedRecordTimeFallback, GuidedRowTimeMapping, GuidedUniverseBinding,
    GuidedValueMapping, build_guided_manifest, build_guided_manifest_collection,
};
pub use preview::{
    FilePreview, FilePreviewCell, FilePreviewColumn, FilePreviewColumnKind, FilePreviewFormat,
    FilePreviewLimits, FilePreviewRow, preview_bytes,
};
pub use source::FileExtractionSource;

pub(crate) use contracts::{CellValue, ParseBudget, ParsedRow};
pub(crate) use manifest::FormulaPolicy;
