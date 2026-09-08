//! Bounded format dispatch and parser limits.

mod csv;
mod sdmx;

use serde::Serialize;

use crate::{BoardAdapterError, BoardDatasetContract, BoardFileFormat, ParsedBoardDataset};

pub use csv::parse_csv;
pub use sdmx::{parse_sdmx_xml, parse_sdmx_zip};

/// Independent source, archive, and structural ceilings for one parse.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BoardParseLimits {
    max_source_bytes: usize,
    max_archive_entries: usize,
    max_decompressed_bytes: u64,
    max_entry_bytes: u64,
    max_compression_ratio: u64,
    max_series: usize,
    max_observations: usize,
    max_attributes: usize,
    max_xml_depth: usize,
    max_text_bytes: usize,
}

impl Default for BoardParseLimits {
    fn default() -> Self {
        Self {
            max_source_bytes: 128 * 1024 * 1024,
            max_archive_entries: 256,
            max_decompressed_bytes: 512 * 1024 * 1024,
            max_entry_bytes: 256 * 1024 * 1024,
            max_compression_ratio: 200,
            max_series: 20_000,
            max_observations: 5_000_000,
            max_attributes: 128,
            max_xml_depth: 64,
            max_text_bytes: 64 * 1024,
        }
    }
}

impl BoardParseLimits {
    /// Returns the closed parser budget for the rolling 100-date H.15 dashboard response.
    ///
    /// The observation ceiling is part of the production contract, so a server-side regression
    /// that ignores `lastobs=100` is rejected while the CSV is still being decoded rather than
    /// materializing an unbounded full-history batch.
    pub fn h15_treasury_constant_maturities_rolling_dashboard() -> Self {
        Self {
            max_source_bytes: 1024 * 1024,
            max_series: 11,
            max_observations: 1_100,
            ..Self::default()
        }
    }

    /// Builds explicit nonzero parser limits.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        max_source_bytes: usize,
        max_archive_entries: usize,
        max_decompressed_bytes: u64,
        max_entry_bytes: u64,
        max_compression_ratio: u64,
        max_series: usize,
        max_observations: usize,
        max_attributes: usize,
        max_xml_depth: usize,
        max_text_bytes: usize,
    ) -> Result<Self, BoardAdapterError> {
        let value = Self {
            max_source_bytes,
            max_archive_entries,
            max_decompressed_bytes,
            max_entry_bytes,
            max_compression_ratio,
            max_series,
            max_observations,
            max_attributes,
            max_xml_depth,
            max_text_bytes,
        };
        if max_source_bytes == 0
            || max_archive_entries == 0
            || max_decompressed_bytes == 0
            || max_entry_bytes == 0
            || max_compression_ratio == 0
            || max_series == 0
            || max_observations == 0
            || max_attributes == 0
            || max_xml_depth == 0
            || max_text_bytes == 0
        {
            Err(BoardAdapterError::InvalidContract)
        } else {
            Ok(value)
        }
    }

    pub(crate) const fn max_source_bytes(self) -> usize {
        self.max_source_bytes
    }
    pub(crate) const fn max_archive_entries(self) -> usize {
        self.max_archive_entries
    }
    pub(crate) const fn max_decompressed_bytes(self) -> u64 {
        self.max_decompressed_bytes
    }
    pub(crate) const fn max_entry_bytes(self) -> u64 {
        self.max_entry_bytes
    }
    pub(crate) const fn max_compression_ratio(self) -> u64 {
        self.max_compression_ratio
    }
    pub(crate) const fn max_series(self) -> usize {
        self.max_series
    }
    pub(crate) const fn max_observations(self) -> usize {
        self.max_observations
    }
    pub(crate) const fn max_attributes(self) -> usize {
        self.max_attributes
    }
    pub(crate) const fn max_xml_depth(self) -> usize {
        self.max_xml_depth
    }
    pub(crate) const fn max_text_bytes(self) -> usize {
        self.max_text_bytes
    }
}

/// Parses a self-contained CSV or ZIP response according to its frozen contract.
///
/// Uncompressed XML needs separately supplied structural artifacts and is therefore admitted by
/// [`parse_sdmx_xml`] rather than this convenience dispatcher.
pub fn parse_board_file(
    contract: &BoardDatasetContract,
    bytes: &[u8],
    limits: BoardParseLimits,
) -> Result<ParsedBoardDataset, BoardAdapterError> {
    match contract.format() {
        BoardFileFormat::DdpCsvSeriesColumnV1 => parse_csv(contract, bytes, limits),
        BoardFileFormat::SdmxCompactZipV1 => parse_sdmx_zip(contract, bytes, limits),
        BoardFileFormat::SdmxCompactXmlV1 => Err(BoardAdapterError::StructuralArtifactMismatch),
    }
}
