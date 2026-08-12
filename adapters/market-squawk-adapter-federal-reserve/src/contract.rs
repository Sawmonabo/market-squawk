//! Closed release, package, series, and request contracts.

use std::collections::BTreeSet;

use market_squawk_domain::CalendarDate;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use url::Url;

use crate::BoardAdapterError;
use crate::digest::{finish, update_bool, update_bytes, update_tag, update_u64};

/// Stable source identifier expected by the shared extraction registry.
pub const BOARD_DDP_SOURCE_ID: &str = "federal-reserve-board-ddp";
/// Native adapter contract version bound into requests, parsed batches, and receipts.
pub const BOARD_NATIVE_CONTRACT_VERSION: u16 = 1;

const MAX_URL_BYTES: usize = 4 * 1024;
const MAX_SERIES_PER_CONTRACT: usize = 20_000;
const MAX_SERIES_TEXT_BYTES: usize = 8 * 1024;
const MAX_ARTIFACTS_PER_PACKAGE: usize = 256;
const MAX_ARTIFACT_NAME_BYTES: usize = 512;
const SDMX_COMPACT_MESSAGE_NAMESPACE: &str =
    "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message";

/// Selected Board statistical release.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardRelease {
    /// H.15 Selected Interest Rates.
    H15SelectedInterestRates,
    /// G.17 Industrial Production and Capacity Utilization.
    G17IndustrialProduction,
}

impl BoardRelease {
    /// Returns the exact DDP release selector.
    pub const fn code(self) -> &'static str {
        match self {
            Self::H15SelectedInterestRates => "H15",
            Self::G17IndustrialProduction => "G17",
        }
    }

    /// Returns the source-authored release title.
    pub const fn title(self) -> &'static str {
        match self {
            Self::H15SelectedInterestRates => "H.15 Selected Interest Rates",
            Self::G17IndustrialProduction => "G.17 Industrial Production and Capacity Utilization",
        }
    }

    /// Returns the currently documented DDP-route transition for selected Board releases.
    ///
    /// The Board announcement is a lifecycle fact, not permission to substitute a FRED series for
    /// Board provenance. The application must validate either the frozen preformatted DDP URL or
    /// a Board-hosted release XML contract independently.
    pub fn documented_route_lifecycle(self) -> Result<BoardRouteLifecycle, BoardAdapterError> {
        Ok(BoardRouteLifecycle::DdpTransitionAnnounced {
            announced_on: CalendarDate::new(2026, 7, 16)
                .map_err(|_| BoardAdapterError::InvalidContract)?,
            build_your_package_removal_week: CalendarDate::new(2026, 11, 9)
                .map_err(|_| BoardAdapterError::InvalidContract)?,
            board_release_xml_remains_candidate: true,
            fred_is_separate_provenance: true,
        })
    }
}

/// Code-owned dataset family inside a Board release.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardDatasetFamily {
    /// The H.15 preformatted daily Treasury constant-maturity package.
    H15TreasuryConstantMaturities,
    /// A structure-bound complete H.15 SDMX release file.
    H15CompleteRelease,
    /// A frozen G.17 package or structure-bound complete G.17 release file.
    G17IndustrialProductionAndCapacity,
    /// Historical G.17 electric-power-use data discontinued after October 2005.
    G17DiscontinuedElectricPower,
}

impl BoardDatasetFamily {
    /// Returns the owning statistical release.
    pub const fn release(self) -> BoardRelease {
        match self {
            Self::H15TreasuryConstantMaturities | Self::H15CompleteRelease => {
                BoardRelease::H15SelectedInterestRates
            }
            Self::G17IndustrialProductionAndCapacity | Self::G17DiscontinuedElectricPower => {
                BoardRelease::G17IndustrialProduction
            }
        }
    }

    /// Returns the stable adapter-local family label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::H15TreasuryConstantMaturities => "h15-treasury-constant-maturities",
            Self::H15CompleteRelease => "h15-complete-release",
            Self::G17IndustrialProductionAndCapacity => "g17-industrial-production-and-capacity",
            Self::G17DiscontinuedElectricPower => "g17-discontinued-electric-power",
        }
    }
}

/// Exact file shape accepted by the adapter core.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardFileFormat {
    /// DDP CSV with labels included and series in columns.
    DdpCsvSeriesColumnV1,
    /// One uncompressed SDMX compact XML document plus separately supplied structures.
    SdmxCompactXmlV1,
    /// One closed ZIP containing SDMX compact data and every bound structural artifact.
    SdmxCompactZipV1,
}

impl BoardFileFormat {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DdpCsvSeriesColumnV1 => "ddp-csv-series-column-v1",
            Self::SdmxCompactXmlV1 => "sdmx-compact-xml-v1",
            Self::SdmxCompactZipV1 => "sdmx-compact-zip-v1",
        }
    }

    /// Returns the exact response media type requested by the scheduler.
    pub const fn accept(self) -> &'static str {
        match self {
            Self::DdpCsvSeriesColumnV1 => "text/csv",
            Self::SdmxCompactXmlV1 => "application/xml",
            Self::SdmxCompactZipV1 => "application/zip",
        }
    }
}

/// Source-authored frequency. One parsed file has exactly one value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardFrequency {
    /// Business-day observations, represented by DDP series suffix `B`.
    BusinessDaily,
    /// Calendar-day observations, represented by SDMX/series code `D`.
    Daily,
    /// Weekly observations.
    Weekly,
    /// Monthly observations.
    Monthly,
    /// Quarterly observations.
    Quarterly,
    /// Annual observations.
    Annual,
}

impl BoardFrequency {
    /// Returns the compact SDMX frequency code.
    pub const fn sdmx_code(self) -> &'static str {
        match self {
            Self::BusinessDaily | Self::Daily => "D",
            Self::Weekly => "W",
            Self::Monthly => "M",
            Self::Quarterly => "Q",
            Self::Annual => "A",
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BusinessDaily => "business_daily",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
            Self::Quarterly => "quarterly",
            Self::Annual => "annual",
        }
    }

    pub(crate) fn matches_ddp_series_name(self, value: &str) -> bool {
        match self {
            Self::BusinessDaily => value.ends_with(".B"),
            Self::Daily => value.ends_with(".D"),
            Self::Weekly => {
                value.ends_with(".W") || value.ends_with(".WW") || value.ends_with(".WF")
            }
            Self::Monthly => value.ends_with(".M"),
            Self::Quarterly => value.ends_with(".Q"),
            Self::Annual => value.ends_with(".A"),
        }
    }
}

/// Lifecycle of the selected Board acquisition route.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BoardRouteLifecycle {
    /// DDP custom package retirement is announced; Board XML and FRED remain distinct candidates.
    DdpTransitionAnnounced {
        /// Board announcement date.
        announced_on: CalendarDate,
        /// Beginning of the announced week for removal of Build Your Package.
        build_your_package_removal_week: CalendarDate,
        /// Whether a Board-hosted release XML route is documented as a direct candidate.
        board_release_xml_remains_candidate: bool,
        /// Whether FRED must retain its own provenance rather than impersonating Board DDP.
        fred_is_separate_provenance: bool,
    },
    /// A frozen direct route remains active with no selected replacement notice.
    Active,
    /// A route ended and is retained only for historical evidence.
    Discontinued {
        /// Last source-authored observation period retained by the route.
        last_observation_period: Box<str>,
        /// Whether the Board states that historical files remain available.
        historical_files_remain: bool,
    },
    /// A route was replaced by a separately identified route.
    Replaced {
        /// Stable replacement route identifier; it does not rewrite source provenance.
        replacement_route: Box<str>,
        /// Effective date of the replacement when source evidenced.
        effective_on: CalendarDate,
    },
}

/// Lifecycle of one provider series coordinate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BoardSeriesLifecycle {
    /// The series is active in the selected contract.
    Active,
    /// A source-evidenced successor is announced or effective.
    Replaced {
        /// Exact successor unique identifier.
        successor_unique_id: Box<str>,
        /// Exact source-authored effective-period token.
        effective_period: Box<str>,
        /// Digest of the Board replacement/revision evidence bytes.
        evidence_digest: [u8; 32],
    },
    /// The series no longer receives observations but its history may remain.
    Discontinued {
        /// Exact final source period.
        last_observation_period: Box<str>,
        /// Whether historical observations remain admitted.
        historical_observations_remain: bool,
        /// Digest of the Board discontinuation evidence bytes.
        evidence_digest: [u8; 32],
    },
}

impl BoardSeriesLifecycle {
    fn validate(&self) -> Result<(), BoardAdapterError> {
        match self {
            Self::Active => Ok(()),
            Self::Replaced {
                successor_unique_id,
                effective_period,
                ..
            } => {
                validate_identifier(successor_unique_id, 512)?;
                validate_text(effective_period, 64)
            }
            Self::Discontinued {
                last_observation_period,
                ..
            } => validate_text(last_observation_period, 64),
        }
    }

    fn update_digest(&self, digest: &mut Sha256) {
        match self {
            Self::Active => update_tag(digest, "active"),
            Self::Replaced {
                successor_unique_id,
                effective_period,
                evidence_digest,
            } => {
                update_tag(digest, "replaced");
                update_tag(digest, successor_unique_id);
                update_tag(digest, effective_period);
                update_bytes(digest, evidence_digest);
            }
            Self::Discontinued {
                last_observation_period,
                historical_observations_remain,
                evidence_digest,
            } => {
                update_tag(digest, "discontinued");
                update_tag(digest, last_observation_period);
                update_bool(digest, *historical_observations_remain);
                update_bytes(digest, evidence_digest);
            }
        }
    }
}

/// Exact metadata expected for one selected DDP/SDMX series.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardSeriesContract {
    unique_id: Box<str>,
    series_name: Box<str>,
    expected_description: Option<Box<str>>,
    unit: Box<str>,
    multiplier: Decimal,
    currency: Box<str>,
    frequency: BoardFrequency,
    lifecycle: BoardSeriesLifecycle,
}

impl BoardSeriesContract {
    /// Builds one exact series contract without converting values through binary floating point.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        unique_id: impl Into<Box<str>>,
        series_name: impl Into<Box<str>>,
        expected_description: Option<Box<str>>,
        unit: impl Into<Box<str>>,
        multiplier: Decimal,
        currency: impl Into<Box<str>>,
        frequency: BoardFrequency,
        lifecycle: BoardSeriesLifecycle,
    ) -> Result<Self, BoardAdapterError> {
        let value = Self {
            unique_id: unique_id.into(),
            series_name: series_name.into(),
            expected_description,
            unit: unit.into(),
            multiplier: multiplier.normalize(),
            currency: currency.into(),
            frequency,
            lifecycle,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), BoardAdapterError> {
        validate_identifier(&self.unique_id, 512)?;
        validate_identifier(&self.series_name, 256)?;
        if !self.frequency.matches_ddp_series_name(&self.series_name) {
            return Err(BoardAdapterError::InvalidContract);
        }
        if let Some(description) = &self.expected_description {
            validate_text(description, MAX_SERIES_TEXT_BYTES)?;
        }
        validate_text(&self.unit, 256)?;
        validate_identifier(&self.currency, 32)?;
        if self.multiplier <= Decimal::ZERO {
            return Err(BoardAdapterError::InvalidContract);
        }
        self.lifecycle.validate()
    }

    /// Returns the full release-qualified provider identifier.
    pub fn unique_id(&self) -> &str {
        &self.unique_id
    }

    /// Returns the DDP/SDMX series name.
    pub fn series_name(&self) -> &str {
        &self.series_name
    }

    /// Returns an exact expected description when the code-owned package freezes one.
    pub fn expected_description(&self) -> Option<&str> {
        self.expected_description.as_deref()
    }

    /// Returns the provider unit label.
    pub fn unit(&self) -> &str {
        &self.unit
    }

    /// Returns the exact provider unit multiplier.
    pub const fn multiplier(&self) -> Decimal {
        self.multiplier
    }

    /// Returns the source currency code or source-native `NA` marker.
    pub fn currency(&self) -> &str {
        &self.currency
    }

    /// Returns the source frequency.
    pub const fn frequency(&self) -> BoardFrequency {
        self.frequency
    }

    /// Returns replacement/discontinuation evidence for this coordinate.
    pub const fn lifecycle(&self) -> &BoardSeriesLifecycle {
        &self.lifecycle
    }

    fn update_digest(&self, digest: &mut Sha256) {
        update_tag(digest, &self.unique_id);
        update_tag(digest, &self.series_name);
        match &self.expected_description {
            Some(value) => {
                update_bool(digest, true);
                update_tag(digest, value);
            }
            None => update_bool(digest, false),
        }
        update_tag(digest, &self.unit);
        update_tag(digest, &self.multiplier.to_string());
        update_tag(digest, &self.currency);
        update_tag(digest, self.frequency.as_str());
        self.lifecycle.update_digest(digest);
    }
}

/// Whether the parser expects a closed series set or a structurally validated complete release.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BoardSeriesScope {
    /// Every series and its source metadata are frozen in the request contract.
    Exact { series: Vec<BoardSeriesContract> },
    /// A complete SDMX dataset may evolve only under the bound structure/schema digests.
    StructureBoundCompleteRelease {
        /// Whole-file series ceiling independent of provider claims.
        max_series: usize,
    },
}

impl BoardSeriesScope {
    fn validate(
        &self,
        release: BoardRelease,
        frequency: BoardFrequency,
        format: BoardFileFormat,
    ) -> Result<(), BoardAdapterError> {
        match self {
            Self::Exact { series } => {
                if series.is_empty() || series.len() > MAX_SERIES_PER_CONTRACT {
                    return Err(BoardAdapterError::InvalidContract);
                }
                let prefix = format!("{0}/{0}/", release.code());
                let mut ids = BTreeSet::new();
                let mut names = BTreeSet::new();
                for item in series {
                    item.validate()?;
                    if item.frequency != frequency
                        || !item.unique_id.starts_with(&prefix)
                        || !ids.insert(item.unique_id.as_ref())
                        || !names.insert(item.series_name.as_ref())
                    {
                        return Err(BoardAdapterError::InvalidContract);
                    }
                }
                Ok(())
            }
            Self::StructureBoundCompleteRelease { max_series } => {
                if !matches!(
                    format,
                    BoardFileFormat::SdmxCompactXmlV1 | BoardFileFormat::SdmxCompactZipV1
                ) || *max_series == 0
                    || *max_series > MAX_SERIES_PER_CONTRACT
                {
                    return Err(BoardAdapterError::InvalidContract);
                }
                Ok(())
            }
        }
    }

    /// Returns exact series metadata when the selected package freezes a series set.
    pub fn exact_series(&self) -> Option<&[BoardSeriesContract]> {
        match self {
            Self::Exact { series } => Some(series),
            Self::StructureBoundCompleteRelease { .. } => None,
        }
    }

    /// Returns the whole-file series ceiling.
    pub fn max_series(&self) -> usize {
        match self {
            Self::Exact { series } => series.len(),
            Self::StructureBoundCompleteRelease { max_series } => *max_series,
        }
    }

    fn update_digest(&self, digest: &mut Sha256) {
        match self {
            Self::Exact { series } => {
                update_tag(digest, "exact");
                update_u64(digest, series.len() as u64);
                for item in series {
                    item.update_digest(digest);
                }
            }
            Self::StructureBoundCompleteRelease { max_series } => {
                update_tag(digest, "structure-bound-complete-release");
                update_u64(digest, *max_series as u64);
            }
        }
    }
}

/// Role of one exact SDMX ZIP/XML artifact.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardArtifactKind {
    /// DDP CSV data document.
    DataCsv,
    /// Compact SDMX data document.
    DataXml,
    /// Single FRB common schema.
    FrbCommonSchema,
    /// Release-specific structure document.
    ReleaseStructure,
    /// Dataset-specific schema; one or more are required.
    DatasetSchema,
}

impl BoardArtifactKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DataCsv => "data_csv",
            Self::DataXml => "data_xml",
            Self::FrbCommonSchema => "frb_common_schema",
            Self::ReleaseStructure => "release_structure",
            Self::DatasetSchema => "dataset_schema",
        }
    }
}

/// Exact expected member and structural digest for one SDMX package artifact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardArtifactContract {
    name: Box<str>,
    kind: BoardArtifactKind,
    expected_sha256: Option<[u8; 32]>,
}

impl BoardArtifactContract {
    /// Builds an exact artifact contract. Data bytes vary by release and therefore carry no
    /// expected digest; every structural artifact must carry one.
    pub fn try_new(
        name: impl Into<Box<str>>,
        kind: BoardArtifactKind,
        expected_sha256: Option<[u8; 32]>,
    ) -> Result<Self, BoardAdapterError> {
        let value = Self {
            name: name.into(),
            kind,
            expected_sha256,
        };
        validate_artifact_name(&value.name)?;
        match (kind, expected_sha256) {
            (BoardArtifactKind::DataCsv | BoardArtifactKind::DataXml, None)
            | (
                BoardArtifactKind::FrbCommonSchema
                | BoardArtifactKind::ReleaseStructure
                | BoardArtifactKind::DatasetSchema,
                Some(_),
            ) => Ok(value),
            _ => Err(BoardAdapterError::InvalidContract),
        }
    }

    /// Returns the exact relative ZIP/member name or external artifact label.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns its structural role.
    pub const fn kind(&self) -> BoardArtifactKind {
        self.kind
    }

    /// Returns the required structural digest; data XML is digested on receipt.
    pub const fn expected_sha256(&self) -> Option<[u8; 32]> {
        self.expected_sha256
    }
}

/// Exact SDMX compact namespace, header, and artifact set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SdmxPackageContract {
    message_namespace: Box<str>,
    dataset_namespace: Box<str>,
    header_id_prefix: Box<str>,
    artifacts: Vec<BoardArtifactContract>,
}

impl SdmxPackageContract {
    /// Builds a structure-bound SDMX contract.
    ///
    /// The artifact set must contain exactly one data document, one FRB common schema, one
    /// release structure, and one or more dataset schemas. Unknown or extra ZIP members fail.
    pub fn try_new(
        dataset_namespace: impl Into<Box<str>>,
        header_id_prefix: impl Into<Box<str>>,
        artifacts: Vec<BoardArtifactContract>,
    ) -> Result<Self, BoardAdapterError> {
        let value = Self {
            message_namespace: SDMX_COMPACT_MESSAGE_NAMESPACE.into(),
            dataset_namespace: dataset_namespace.into(),
            header_id_prefix: header_id_prefix.into(),
            artifacts,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), BoardAdapterError> {
        validate_text(&self.message_namespace, 256)?;
        validate_text(&self.dataset_namespace, 512)?;
        validate_identifier(&self.header_id_prefix, 128)?;
        if self.artifacts.len() < 4 || self.artifacts.len() > MAX_ARTIFACTS_PER_PACKAGE {
            return Err(BoardAdapterError::InvalidContract);
        }
        let mut names = BTreeSet::new();
        let mut kinds = [0_usize; 4];
        for artifact in &self.artifacts {
            validate_artifact_name(&artifact.name)?;
            if !names.insert(artifact.name.to_ascii_lowercase()) {
                return Err(BoardAdapterError::InvalidContract);
            }
            let index = match artifact.kind {
                BoardArtifactKind::DataCsv => return Err(BoardAdapterError::InvalidContract),
                BoardArtifactKind::DataXml => 0,
                BoardArtifactKind::FrbCommonSchema => 1,
                BoardArtifactKind::ReleaseStructure => 2,
                BoardArtifactKind::DatasetSchema => 3,
            };
            kinds[index] = kinds[index]
                .checked_add(1)
                .ok_or(BoardAdapterError::CountOverflow)?;
        }
        if kinds[0] != 1 || kinds[1] != 1 || kinds[2] != 1 || kinds[3] == 0 {
            return Err(BoardAdapterError::InvalidContract);
        }
        Ok(())
    }

    /// Returns the exact compact-message namespace.
    pub fn message_namespace(&self) -> &str {
        &self.message_namespace
    }

    /// Returns the release/dataset namespace required on the SDMX `DataSet` and `Series` nodes.
    pub fn dataset_namespace(&self) -> &str {
        &self.dataset_namespace
    }

    /// Returns the required prefix of the nonempty SDMX header identifier.
    pub fn header_id_prefix(&self) -> &str {
        &self.header_id_prefix
    }

    /// Returns the closed artifact set.
    pub fn artifacts(&self) -> &[BoardArtifactContract] {
        &self.artifacts
    }

    pub(crate) fn data_artifact(&self) -> Result<&BoardArtifactContract, BoardAdapterError> {
        self.artifacts
            .iter()
            .find(|item| item.kind == BoardArtifactKind::DataXml)
            .ok_or(BoardAdapterError::InvalidContract)
    }

    fn update_digest(&self, digest: &mut Sha256) {
        update_tag(digest, &self.message_namespace);
        update_tag(digest, &self.dataset_namespace);
        update_tag(digest, &self.header_id_prefix);
        update_u64(digest, self.artifacts.len() as u64);
        for artifact in &self.artifacts {
            update_tag(digest, &artifact.name);
            update_tag(digest, artifact.kind.as_str());
            match artifact.expected_sha256 {
                Some(value) => {
                    update_bool(digest, true);
                    update_bytes(digest, &value);
                }
                None => update_bool(digest, false),
            }
        }
    }
}

/// Immutable exact dataset acquisition and parser contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardDatasetContract {
    release: BoardRelease,
    family: BoardDatasetFamily,
    format: BoardFileFormat,
    url: Box<str>,
    frequency: BoardFrequency,
    series_scope: BoardSeriesScope,
    route_lifecycle: BoardRouteLifecycle,
    sdmx: Option<SdmxPackageContract>,
    contract_digest: [u8; 32],
}

impl BoardDatasetContract {
    /// Builds the selected H.15 daily Treasury constant-maturity CSV contract over the exact
    /// generated automated-download URL supplied by the application registry.
    pub fn h15_treasury_constant_maturities_csv(
        generated_url: impl Into<Box<str>>,
    ) -> Result<Self, BoardAdapterError> {
        let series = [
            ("RIFLGFCM01_N.B", "1-month"),
            ("RIFLGFCM03_N.B", "3-month"),
            ("RIFLGFCM06_N.B", "6-month"),
            ("RIFLGFCY01_N.B", "1-year"),
            ("RIFLGFCY02_N.B", "2-year"),
            ("RIFLGFCY03_N.B", "3-year"),
            ("RIFLGFCY05_N.B", "5-year"),
            ("RIFLGFCY07_N.B", "7-year"),
            ("RIFLGFCY10_N.B", "10-year"),
            ("RIFLGFCY20_N.B", "20-year"),
            ("RIFLGFCY30_N.B", "30-year"),
        ]
        .into_iter()
        .map(|(series_name, _maturity)| {
            BoardSeriesContract::try_new(
                format!("H15/H15/{series_name}"),
                series_name,
                None,
                "Percent:_Per_Year",
                Decimal::ONE,
                "NA",
                BoardFrequency::BusinessDaily,
                BoardSeriesLifecycle::Active,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
        Self::try_csv_series_column(
            BoardDatasetFamily::H15TreasuryConstantMaturities,
            generated_url,
            BoardFrequency::BusinessDaily,
            series,
        )
    }

    /// Builds an exact-label, series-in-columns DDP CSV contract for a selected Board family.
    pub fn try_csv_series_column(
        family: BoardDatasetFamily,
        generated_url: impl Into<Box<str>>,
        frequency: BoardFrequency,
        series: Vec<BoardSeriesContract>,
    ) -> Result<Self, BoardAdapterError> {
        Self::try_new(
            family,
            BoardFileFormat::DdpCsvSeriesColumnV1,
            generated_url.into(),
            frequency,
            BoardSeriesScope::Exact { series },
            None,
        )
    }

    /// Builds an exact structure-bound SDMX XML or ZIP contract.
    pub fn try_sdmx(
        family: BoardDatasetFamily,
        format: BoardFileFormat,
        official_url: impl Into<Box<str>>,
        frequency: BoardFrequency,
        series_scope: BoardSeriesScope,
        sdmx: SdmxPackageContract,
    ) -> Result<Self, BoardAdapterError> {
        if !matches!(
            format,
            BoardFileFormat::SdmxCompactXmlV1 | BoardFileFormat::SdmxCompactZipV1
        ) {
            return Err(BoardAdapterError::InvalidContract);
        }
        Self::try_new(
            family,
            format,
            official_url.into(),
            frequency,
            series_scope,
            Some(sdmx),
        )
    }

    fn try_new(
        family: BoardDatasetFamily,
        format: BoardFileFormat,
        url: Box<str>,
        frequency: BoardFrequency,
        series_scope: BoardSeriesScope,
        sdmx: Option<SdmxPackageContract>,
    ) -> Result<Self, BoardAdapterError> {
        let release = family.release();
        validate_official_url(&url, release, format)?;
        series_scope.validate(release, frequency, format)?;
        match (format, sdmx.as_ref()) {
            (BoardFileFormat::DdpCsvSeriesColumnV1, None) => {}
            (
                BoardFileFormat::SdmxCompactXmlV1 | BoardFileFormat::SdmxCompactZipV1,
                Some(contract),
            ) => contract.validate()?,
            _ => return Err(BoardAdapterError::InvalidContract),
        }
        let route_lifecycle = match family {
            BoardDatasetFamily::G17DiscontinuedElectricPower => BoardRouteLifecycle::Discontinued {
                last_observation_period: "2005-10".into(),
                historical_files_remain: true,
            },
            _ => release.documented_route_lifecycle()?,
        };
        let contract_digest = contract_digest(
            release,
            family,
            format,
            &url,
            frequency,
            &series_scope,
            &route_lifecycle,
            sdmx.as_ref(),
        );
        Ok(Self {
            release,
            family,
            format,
            url,
            frequency,
            series_scope,
            route_lifecycle,
            sdmx,
            contract_digest,
        })
    }

    /// Returns the exact statistical release.
    pub const fn release(&self) -> BoardRelease {
        self.release
    }

    /// Returns the code-owned dataset family.
    pub const fn family(&self) -> BoardDatasetFamily {
        self.family
    }

    /// Returns the selected file format.
    pub const fn format(&self) -> BoardFileFormat {
        self.format
    }

    /// Returns the frozen official HTTPS URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the one frequency represented by this file contract.
    pub const fn frequency(&self) -> BoardFrequency {
        self.frequency
    }

    /// Returns the exact or structure-bound series scope.
    pub const fn series_scope(&self) -> &BoardSeriesScope {
        &self.series_scope
    }

    /// Returns the route lifecycle, including the announced DDP transition.
    pub const fn route_lifecycle(&self) -> &BoardRouteLifecycle {
        &self.route_lifecycle
    }

    /// Returns the SDMX contract for XML/ZIP formats.
    pub const fn sdmx(&self) -> Option<&SdmxPackageContract> {
        self.sdmx.as_ref()
    }

    /// Returns the canonical SHA-256 identity of every request/parser-relevant field.
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }

    /// Produces the immutable GET request identity; it performs no I/O.
    pub fn request(&self) -> BoardFileRequest {
        let mut digest = Sha256::new();
        update_tag(&mut digest, "market-squawk-federal-reserve-request-v1");
        update_bytes(&mut digest, &self.contract_digest);
        update_tag(&mut digest, "GET");
        update_tag(&mut digest, &self.url);
        update_tag(&mut digest, self.format.accept());
        BoardFileRequest {
            method: "GET",
            url: self.url.clone(),
            accept: self.format.accept(),
            contract_digest: self.contract_digest,
            request_digest: finish(digest),
        }
    }
}

/// Transport-free immutable request ready for shared scheduler admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardFileRequest {
    method: &'static str,
    url: Box<str>,
    accept: &'static str,
    contract_digest: [u8; 32],
    request_digest: [u8; 32],
}

impl BoardFileRequest {
    /// Returns `GET`.
    pub const fn method(&self) -> &'static str {
        self.method
    }

    /// Returns the exact official URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the exact expected media type.
    pub const fn accept(&self) -> &'static str {
        self.accept
    }

    /// Returns the bound dataset contract digest.
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }

    /// Returns the canonical request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
}

#[allow(clippy::too_many_arguments)]
fn contract_digest(
    release: BoardRelease,
    family: BoardDatasetFamily,
    format: BoardFileFormat,
    url: &str,
    frequency: BoardFrequency,
    series_scope: &BoardSeriesScope,
    lifecycle: &BoardRouteLifecycle,
    sdmx: Option<&SdmxPackageContract>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_tag(
        &mut digest,
        "market-squawk-federal-reserve-dataset-contract-v1",
    );
    digest.update(BOARD_NATIVE_CONTRACT_VERSION.to_be_bytes());
    update_tag(&mut digest, BOARD_DDP_SOURCE_ID);
    update_tag(&mut digest, release.code());
    update_tag(&mut digest, family.as_str());
    update_tag(&mut digest, format.as_str());
    update_tag(&mut digest, url);
    update_tag(&mut digest, frequency.as_str());
    series_scope.update_digest(&mut digest);
    update_route_lifecycle(&mut digest, lifecycle);
    match sdmx {
        Some(value) => {
            update_bool(&mut digest, true);
            value.update_digest(&mut digest);
        }
        None => update_bool(&mut digest, false),
    }
    finish(digest)
}

fn update_route_lifecycle(digest: &mut Sha256, lifecycle: &BoardRouteLifecycle) {
    match lifecycle {
        BoardRouteLifecycle::DdpTransitionAnnounced {
            announced_on,
            build_your_package_removal_week,
            board_release_xml_remains_candidate,
            fred_is_separate_provenance,
        } => {
            update_tag(digest, "ddp-transition-announced");
            update_tag(digest, &announced_on.to_string());
            update_tag(digest, &build_your_package_removal_week.to_string());
            update_bool(digest, *board_release_xml_remains_candidate);
            update_bool(digest, *fred_is_separate_provenance);
        }
        BoardRouteLifecycle::Active => update_tag(digest, "active"),
        BoardRouteLifecycle::Discontinued {
            last_observation_period,
            historical_files_remain,
        } => {
            update_tag(digest, "discontinued");
            update_tag(digest, last_observation_period);
            update_bool(digest, *historical_files_remain);
        }
        BoardRouteLifecycle::Replaced {
            replacement_route,
            effective_on,
        } => {
            update_tag(digest, "replaced");
            update_tag(digest, replacement_route);
            update_tag(digest, &effective_on.to_string());
        }
    }
}

fn validate_official_url(
    value: &str,
    release: BoardRelease,
    format: BoardFileFormat,
) -> Result<(), BoardAdapterError> {
    if value.is_empty() || value.len() > MAX_URL_BYTES {
        return Err(BoardAdapterError::RequestUrlRejected);
    }
    let parsed = Url::parse(value).map_err(|_| BoardAdapterError::RequestUrlRejected)?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("www.federalreserve.gov")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.fragment().is_some()
    {
        return Err(BoardAdapterError::RequestUrlRejected);
    }
    let path = parsed.path().to_ascii_lowercase();
    if path == "/datadownload/output.aspx" {
        validate_ddp_query(&parsed, release, format)
    } else {
        let release_prefix = format!("/releases/{}/", release.code().to_ascii_lowercase());
        if matches!(
            format,
            BoardFileFormat::SdmxCompactXmlV1 | BoardFileFormat::SdmxCompactZipV1
        ) && path.starts_with(&release_prefix)
            && ((format == BoardFileFormat::SdmxCompactXmlV1 && path.ends_with(".xml"))
                || (format == BoardFileFormat::SdmxCompactZipV1 && path.ends_with(".zip")))
            && parsed.query().is_none()
        {
            Ok(())
        } else {
            Err(BoardAdapterError::RequestUrlRejected)
        }
    }
}

fn validate_ddp_query(
    parsed: &Url,
    release: BoardRelease,
    format: BoardFileFormat,
) -> Result<(), BoardAdapterError> {
    let mut values = std::collections::BTreeMap::<String, String>::new();
    for (key, value) in parsed.query_pairs() {
        let key = key.to_ascii_lowercase();
        if !matches!(
            key.as_str(),
            "rel" | "series" | "lastobs" | "from" | "to" | "filetype" | "label" | "layout" | "type"
        ) || values.insert(key, value.into_owned()).is_some()
        {
            return Err(BoardAdapterError::RequestUrlRejected);
        }
    }
    if !values
        .get("rel")
        .is_some_and(|value| value.eq_ignore_ascii_case(release.code()))
    {
        return Err(BoardAdapterError::RequestUrlRejected);
    }
    match format {
        BoardFileFormat::DdpCsvSeriesColumnV1 => {
            let series = values
                .get("series")
                .ok_or(BoardAdapterError::RequestUrlRejected)?;
            if values.get("filetype").map(String::as_str) != Some("csv")
                || values.get("label").map(String::as_str) != Some("include")
                || values.get("layout").map(String::as_str) != Some("seriescolumn")
                || series.len() != 32
                || !series.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(BoardAdapterError::RequestUrlRejected);
            }
        }
        BoardFileFormat::SdmxCompactXmlV1 | BoardFileFormat::SdmxCompactZipV1 => {
            let expected = match format {
                BoardFileFormat::SdmxCompactXmlV1 => ["xml", "sdmx"].as_slice(),
                BoardFileFormat::SdmxCompactZipV1 => ["zip", "sdmx"].as_slice(),
                BoardFileFormat::DdpCsvSeriesColumnV1 => &[],
            };
            if !values
                .get("filetype")
                .is_some_and(|value| expected.contains(&value.as_str()))
            {
                return Err(BoardAdapterError::RequestUrlRejected);
            }
        }
    }
    if values.get("type").is_some_and(|value| value != "package") {
        return Err(BoardAdapterError::RequestUrlRejected);
    }
    Ok(())
}

fn validate_identifier(value: &str, maximum: usize) -> Result<(), BoardAdapterError> {
    if value.is_empty()
        || value.len() > maximum
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        Err(BoardAdapterError::InvalidContract)
    } else {
        Ok(())
    }
}

fn validate_text(value: &str, maximum: usize) -> Result<(), BoardAdapterError> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        Err(BoardAdapterError::InvalidContract)
    } else {
        Ok(())
    }
}

fn validate_artifact_name(value: &str) -> Result<(), BoardAdapterError> {
    if value.is_empty()
        || value.len() > MAX_ARTIFACT_NAME_BYTES
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains(['\\', ':', '\0'])
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        Err(BoardAdapterError::InvalidContract)
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_h15_one_series_contract(
    url: &str,
) -> Result<BoardDatasetContract, BoardAdapterError> {
    BoardDatasetContract::try_csv_series_column(
        BoardDatasetFamily::H15TreasuryConstantMaturities,
        url,
        BoardFrequency::BusinessDaily,
        vec![BoardSeriesContract::try_new(
            "H15/H15/RIFLGFCM01_N.B",
            "RIFLGFCM01_N.B",
            Some(
                "Market yield on U.S. Treasury securities at 1-month constant maturity, quoted on investment basis"
                    .into(),
            ),
            "Percent:_Per_Year",
            Decimal::ONE,
            "NA",
            BoardFrequency::BusinessDaily,
            BoardSeriesLifecycle::Active,
        )?],
    )
}
