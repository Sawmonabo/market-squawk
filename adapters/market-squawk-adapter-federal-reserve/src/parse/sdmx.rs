use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read as _};

use quick_xml::XmlVersion;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use zip::{CompressionMethod, ZipArchive};

use crate::contract::{
    BoardArtifactContract, BoardArtifactKind, BoardFileFormat, BoardSeriesContract,
    BoardSeriesLifecycle, BoardSeriesScope, SdmxPackageContract,
};
use crate::digest::{finish, sha256, update_bytes, update_tag, update_u64};
use crate::model::{
    BoardArtifactReceipt, BoardObservation, BoardPeriod, BoardSdmxHeader, BoardSeries, BoardValue,
};
use crate::{BoardAdapterError, BoardDatasetContract, BoardParseLimits, ParsedBoardDataset};

const DECOMPRESSION_CHUNK_BYTES: usize = 64 * 1024;

/// Parses uncompressed compact SDMX data with the exact separately retained structural artifacts.
pub fn parse_sdmx_xml(
    contract: &BoardDatasetContract,
    data_bytes: &[u8],
    structural_artifacts: &[(&str, &[u8])],
    limits: BoardParseLimits,
) -> Result<ParsedBoardDataset, BoardAdapterError> {
    if contract.format() != BoardFileFormat::SdmxCompactXmlV1 {
        return Err(BoardAdapterError::FormatMismatch);
    }
    if data_bytes.is_empty() || data_bytes.len() > limits.max_source_bytes() {
        return Err(BoardAdapterError::ByteLimitExceeded);
    }
    let package = contract.sdmx().ok_or(BoardAdapterError::InvalidContract)?;
    let artifacts = admit_external_artifacts(package, data_bytes, structural_artifacts, limits)?;
    parse_sdmx_document(contract, data_bytes, sha256(data_bytes), artifacts, limits)
}

/// Safely expands and parses one closed compact SDMX ZIP package.
pub fn parse_sdmx_zip(
    contract: &BoardDatasetContract,
    bytes: &[u8],
    limits: BoardParseLimits,
) -> Result<ParsedBoardDataset, BoardAdapterError> {
    if contract.format() != BoardFileFormat::SdmxCompactZipV1 {
        return Err(BoardAdapterError::FormatMismatch);
    }
    if bytes.is_empty() || bytes.len() > limits.max_source_bytes() {
        return Err(BoardAdapterError::ByteLimitExceeded);
    }
    let package = contract.sdmx().ok_or(BoardAdapterError::InvalidContract)?;
    let members = read_closed_archive(bytes, package, limits)?;
    let data_contract = package.data_artifact()?;
    let data = members
        .get(data_contract.name())
        .ok_or(BoardAdapterError::StructuralArtifactMismatch)?;
    let artifacts = artifact_receipts(package, &members)?;
    parse_sdmx_document(contract, data, sha256(bytes), artifacts, limits)
}

fn admit_external_artifacts(
    package: &SdmxPackageContract,
    data: &[u8],
    supplied: &[(&str, &[u8])],
    limits: BoardParseLimits,
) -> Result<Vec<BoardArtifactReceipt>, BoardAdapterError> {
    if supplied.len().saturating_add(1) != package.artifacts().len()
        || supplied.len().saturating_add(1) > limits.max_archive_entries()
    {
        return Err(BoardAdapterError::StructuralArtifactMismatch);
    }
    let mut by_name = BTreeMap::new();
    let mut total = u64::try_from(data.len()).map_err(|_| BoardAdapterError::CountOverflow)?;
    for (name, bytes) in supplied {
        if by_name.insert(*name, *bytes).is_some() {
            return Err(BoardAdapterError::DuplicateIdentity);
        }
        let size = u64::try_from(bytes.len()).map_err(|_| BoardAdapterError::CountOverflow)?;
        if size > limits.max_entry_bytes() {
            return Err(BoardAdapterError::ByteLimitExceeded);
        }
        total = total
            .checked_add(size)
            .ok_or(BoardAdapterError::CountOverflow)?;
        if total > limits.max_decompressed_bytes() {
            return Err(BoardAdapterError::ByteLimitExceeded);
        }
    }
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(package.artifacts().len())
        .map_err(|_| BoardAdapterError::AllocationFailed)?;
    for artifact in package.artifacts() {
        let bytes = if artifact.kind() == BoardArtifactKind::DataXml {
            if artifact.name() != package.data_artifact()?.name() {
                return Err(BoardAdapterError::InvalidContract);
            }
            data
        } else {
            by_name
                .get(artifact.name())
                .copied()
                .ok_or(BoardAdapterError::StructuralArtifactMismatch)?
        };
        validate_artifact_digest(artifact, bytes)?;
        receipts.push(BoardArtifactReceipt::new(
            artifact.name(),
            artifact.kind(),
            bytes.len(),
            sha256(bytes),
        )?);
    }
    Ok(receipts)
}

fn read_closed_archive(
    bytes: &[u8],
    package: &SdmxPackageContract,
    limits: BoardParseLimits,
) -> Result<BTreeMap<String, Vec<u8>>, BoardAdapterError> {
    let mut archive =
        ZipArchive::new(Cursor::new(bytes)).map_err(|_| BoardAdapterError::UnsafeArchive)?;
    if archive.offset() != 0
        || archive.len() != package.artifacts().len()
        || archive.len() > limits.max_archive_entries()
        || archive
            .has_overlapping_files()
            .map_err(|_| BoardAdapterError::UnsafeArchive)?
    {
        return Err(BoardAdapterError::UnsafeArchive);
    }
    let expected = package
        .artifacts()
        .iter()
        .map(|item| item.name().to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut names = BTreeSet::new();
    let mut declared_total = 0_u64;
    for index in 0..archive.len() {
        let file = archive
            .by_index(index)
            .map_err(|_| BoardAdapterError::UnsafeArchive)?;
        validate_zip_member(&file)?;
        if file.size() > limits.max_entry_bytes() {
            return Err(BoardAdapterError::ByteLimitExceeded);
        }
        validate_ratio(
            file.size(),
            file.compressed_size(),
            limits.max_compression_ratio(),
        )?;
        declared_total = declared_total
            .checked_add(file.size())
            .ok_or(BoardAdapterError::CountOverflow)?;
        if declared_total > limits.max_decompressed_bytes() {
            return Err(BoardAdapterError::ByteLimitExceeded);
        }
        let normalized = file.name().to_ascii_lowercase();
        if !expected.contains(&normalized) || !names.insert(normalized) {
            return Err(BoardAdapterError::UnsafeArchive);
        }
    }
    if names != expected {
        return Err(BoardAdapterError::StructuralArtifactMismatch);
    }

    let mut members = BTreeMap::new();
    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|_| BoardAdapterError::UnsafeArchive)?;
        let name = file.name().to_owned();
        let declared = file.size();
        let capacity = usize::try_from(
            declared
                .checked_add(1)
                .ok_or(BoardAdapterError::CountOverflow)?,
        )
        .map_err(|_| BoardAdapterError::AllocationFailed)?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(capacity)
            .map_err(|_| BoardAdapterError::AllocationFailed)?;
        let mut reader = file.by_ref().take(
            declared
                .checked_add(1)
                .ok_or(BoardAdapterError::CountOverflow)?,
        );
        let mut chunk = [0_u8; DECOMPRESSION_CHUNK_BYTES];
        loop {
            let count = reader
                .read(&mut chunk)
                .map_err(|_| BoardAdapterError::UnsafeArchive)?;
            if count == 0 {
                break;
            }
            payload.extend_from_slice(&chunk[..count]);
        }
        if u64::try_from(payload.len()).map_err(|_| BoardAdapterError::CountOverflow)? != declared {
            return Err(BoardAdapterError::UnsafeArchive);
        }
        if members.insert(name, payload).is_some() {
            return Err(BoardAdapterError::DuplicateIdentity);
        }
    }
    for artifact in package.artifacts() {
        let member = members
            .get(artifact.name())
            .ok_or(BoardAdapterError::StructuralArtifactMismatch)?;
        validate_artifact_digest(artifact, member)?;
    }
    Ok(members)
}

fn validate_zip_member<R: std::io::Read>(
    file: &zip::read::ZipFile<'_, R>,
) -> Result<(), BoardAdapterError> {
    let path = file
        .enclosed_name()
        .ok_or(BoardAdapterError::UnsafeArchive)?;
    if path.to_str() != Some(file.name())
        || file.name().contains(['\\', ':', '\0'])
        || file.encrypted()
        || file.is_symlink()
        || file.is_dir()
        || !matches!(
            file.compression(),
            CompressionMethod::Stored | CompressionMethod::Deflated
        )
    {
        Err(BoardAdapterError::UnsafeArchive)
    } else {
        Ok(())
    }
}

fn validate_ratio(
    uncompressed: u64,
    compressed: u64,
    maximum: u64,
) -> Result<(), BoardAdapterError> {
    if uncompressed == 0 {
        return Ok(());
    }
    let admitted = compressed
        .checked_mul(maximum)
        .ok_or(BoardAdapterError::CompressionRatioExceeded)?;
    if compressed == 0 || uncompressed > admitted {
        Err(BoardAdapterError::CompressionRatioExceeded)
    } else {
        Ok(())
    }
}

fn validate_artifact_digest(
    contract: &BoardArtifactContract,
    bytes: &[u8],
) -> Result<(), BoardAdapterError> {
    if bytes.is_empty()
        || contract
            .expected_sha256()
            .is_some_and(|expected| sha256(bytes) != expected)
    {
        Err(BoardAdapterError::StructuralArtifactMismatch)
    } else {
        Ok(())
    }
}

fn artifact_receipts(
    package: &SdmxPackageContract,
    members: &BTreeMap<String, Vec<u8>>,
) -> Result<Vec<BoardArtifactReceipt>, BoardAdapterError> {
    package
        .artifacts()
        .iter()
        .map(|artifact| {
            let bytes = members
                .get(artifact.name())
                .ok_or(BoardAdapterError::StructuralArtifactMismatch)?;
            BoardArtifactReceipt::new(artifact.name(), artifact.kind(), bytes.len(), sha256(bytes))
        })
        .collect()
}

fn parse_sdmx_document(
    contract: &BoardDatasetContract,
    data: &[u8],
    source_payload_digest: [u8; 32],
    artifacts: Vec<BoardArtifactReceipt>,
    limits: BoardParseLimits,
) -> Result<ParsedBoardDataset, BoardAdapterError> {
    let package = contract.sdmx().ok_or(BoardAdapterError::InvalidContract)?;
    let mut reader = NsReader::from_reader(data);
    reader.config_mut().trim_text(true);
    let mut state = XmlState::default();
    let mut xml_version = XmlVersion::Implicit1_0;
    loop {
        match reader.read_resolved_event() {
            Ok((resolved, Event::Start(start))) => {
                let namespace = owned_namespace(&resolved)?;
                drop(resolved);
                state.depth = state
                    .depth
                    .checked_add(1)
                    .ok_or(BoardAdapterError::StructuralLimitExceeded)?;
                if state.depth > limits.max_xml_depth() {
                    return Err(BoardAdapterError::StructuralLimitExceeded);
                }
                let local = local_name(&start)?;
                state.start(
                    &reader,
                    namespace.as_deref(),
                    &start,
                    &local,
                    package,
                    contract,
                    limits,
                    xml_version,
                )?;
            }
            Ok((resolved, Event::Empty(start))) => {
                let namespace = owned_namespace(&resolved)?;
                drop(resolved);
                let local = local_name(&start)?;
                state.empty(
                    &reader,
                    namespace.as_deref(),
                    &start,
                    &local,
                    package,
                    contract,
                    limits,
                    xml_version,
                )?;
            }
            Ok((_, Event::Text(text))) => {
                let decoded = text
                    .decode()
                    .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?;
                let unescaped = quick_xml::escape::unescape(&decoded)
                    .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?;
                state.text(unescaped.as_ref(), limits.max_text_bytes())?;
            }
            Ok((resolved, Event::End(end))) => {
                let namespace = owned_namespace(&resolved)?;
                let local = std::str::from_utf8(end.local_name().as_ref())
                    .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?
                    .to_owned();
                state.end(namespace.as_deref(), &local, package, contract, limits)?;
                state.depth = state
                    .depth
                    .checked_sub(1)
                    .ok_or(BoardAdapterError::SdmxSchemaDrift)?;
            }
            Ok((_, Event::Decl(declaration))) => {
                if state.saw_declaration || state.depth != 0 || state.saw_root {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                xml_version = declaration
                    .xml_version()
                    .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?;
                if xml_version != XmlVersion::Explicit1_0 {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                state.saw_declaration = true;
            }
            Ok((_, Event::DocType(_) | Event::CData(_) | Event::PI(_) | Event::GeneralRef(_))) => {
                return Err(BoardAdapterError::SdmxSchemaDrift);
            }
            Ok((_, Event::Eof)) => break,
            Ok((_, Event::Comment(_))) => {}
            Err(error) => return Err(BoardAdapterError::InvalidXml(error.to_string())),
        }
    }
    let (header, series) = state.finish(contract, limits)?;
    let schema_digest = sdmx_schema_digest(package, &header, &artifacts);
    ParsedBoardDataset::try_new(
        contract,
        &contract.request(),
        source_payload_digest,
        schema_digest,
        Some(header),
        artifacts,
        series,
    )
}

#[derive(Default)]
struct XmlState {
    depth: usize,
    saw_declaration: bool,
    saw_root: bool,
    closed_root: bool,
    saw_header: bool,
    closed_header: bool,
    saw_dataset: bool,
    closed_dataset: bool,
    target: HeaderTarget,
    header_id: Option<String>,
    header_test: Option<String>,
    header_prepared: Option<String>,
    header_sender: Option<String>,
    current_series: Option<SeriesBuilder>,
    series: Vec<BoardSeries>,
    observation_count: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum HeaderTarget {
    #[default]
    None,
    Id,
    Test,
    Prepared,
}

impl XmlState {
    #[allow(clippy::too_many_arguments)]
    fn start(
        &mut self,
        reader: &NsReader<&[u8]>,
        namespace: Option<&[u8]>,
        start: &BytesStart<'_>,
        local: &str,
        package: &SdmxPackageContract,
        contract: &BoardDatasetContract,
        limits: BoardParseLimits,
        xml_version: XmlVersion,
    ) -> Result<(), BoardAdapterError> {
        match (self.depth, local) {
            (1, "CompactData") => {
                require_namespace(namespace, package.message_namespace().as_bytes())?;
                if self.saw_root || !self.saw_declaration {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                self.saw_root = true;
            }
            (2, "Header") => {
                require_namespace(namespace, package.message_namespace().as_bytes())?;
                if !self.saw_root || self.saw_header || self.saw_dataset {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                self.saw_header = true;
            }
            (3, "ID" | "Test" | "Prepared") if self.saw_header && !self.closed_header => {
                require_namespace(namespace, package.message_namespace().as_bytes())?;
                let target = match local {
                    "ID" => HeaderTarget::Id,
                    "Test" => HeaderTarget::Test,
                    "Prepared" => HeaderTarget::Prepared,
                    _ => return Err(BoardAdapterError::SdmxSchemaDrift),
                };
                self.begin_header_text(target)?;
            }
            (2, "DataSet") => {
                require_namespace(namespace, package.dataset_namespace().as_bytes())?;
                if !self.closed_header || self.saw_dataset {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                self.saw_dataset = true;
            }
            (3, "Series") if self.saw_dataset && !self.closed_dataset => {
                require_namespace(namespace, package.dataset_namespace().as_bytes())?;
                if self.current_series.is_some()
                    || self.series.len()
                        >= contract
                            .series_scope()
                            .max_series()
                            .min(limits.max_series())
                {
                    return Err(BoardAdapterError::StructuralLimitExceeded);
                }
                self.current_series = Some(SeriesBuilder::from_start(
                    reader,
                    start,
                    limits,
                    xml_version,
                )?);
            }
            _ => return Err(BoardAdapterError::SdmxSchemaDrift),
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn empty(
        &mut self,
        reader: &NsReader<&[u8]>,
        namespace: Option<&[u8]>,
        start: &BytesStart<'_>,
        local: &str,
        package: &SdmxPackageContract,
        contract: &BoardDatasetContract,
        limits: BoardParseLimits,
        xml_version: XmlVersion,
    ) -> Result<(), BoardAdapterError> {
        match (self.depth, local) {
            (2, "Sender") if self.saw_header && !self.closed_header => {
                require_namespace(namespace, package.message_namespace().as_bytes())?;
                if self.header_sender.is_some() {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                let attributes = attributes(reader, start, limits, xml_version)?;
                if attributes.len() != 1 {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                self.header_sender = attributes
                    .get("id")
                    .cloned()
                    .or_else(|| attributes.get("ID").cloned());
                if self.header_sender.is_none() {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
            }
            (3, "Obs") if self.current_series.is_some() => {
                require_namespace(namespace, package.dataset_namespace().as_bytes())?;
                self.observation_count = self
                    .observation_count
                    .checked_add(1)
                    .ok_or(BoardAdapterError::CountOverflow)?;
                if self.observation_count > limits.max_observations() {
                    return Err(BoardAdapterError::StructuralLimitExceeded);
                }
                let observation = parse_observation(reader, start, contract, limits, xml_version)?;
                self.current_series
                    .as_mut()
                    .ok_or(BoardAdapterError::SdmxSchemaDrift)?
                    .push(observation)?;
            }
            _ => return Err(BoardAdapterError::SdmxSchemaDrift),
        }
        Ok(())
    }

    fn end(
        &mut self,
        namespace: Option<&[u8]>,
        local: &str,
        package: &SdmxPackageContract,
        contract: &BoardDatasetContract,
        limits: BoardParseLimits,
    ) -> Result<(), BoardAdapterError> {
        match (self.depth, local) {
            (3, "ID" | "Test" | "Prepared") => self.target = HeaderTarget::None,
            (3, "Series") => {
                require_namespace(namespace, package.dataset_namespace().as_bytes())?;
                let builder = self
                    .current_series
                    .take()
                    .ok_or(BoardAdapterError::SdmxSchemaDrift)?;
                self.series.push(builder.finish(contract)?);
                if self.series.len() > limits.max_series() {
                    return Err(BoardAdapterError::StructuralLimitExceeded);
                }
            }
            (2, "Header") => {
                require_namespace(namespace, package.message_namespace().as_bytes())?;
                if self.current_series.is_some() {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                self.closed_header = true;
            }
            (2, "DataSet") => {
                require_namespace(namespace, package.dataset_namespace().as_bytes())?;
                if self.current_series.is_some() {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                self.closed_dataset = true;
            }
            (1, "CompactData") => {
                require_namespace(namespace, package.message_namespace().as_bytes())?;
                if !self.closed_dataset {
                    return Err(BoardAdapterError::SdmxSchemaDrift);
                }
                self.closed_root = true;
            }
            _ => return Err(BoardAdapterError::SdmxSchemaDrift),
        }
        Ok(())
    }

    fn begin_header_text(&mut self, target: HeaderTarget) -> Result<(), BoardAdapterError> {
        let destination = match target {
            HeaderTarget::Id => &self.header_id,
            HeaderTarget::Test => &self.header_test,
            HeaderTarget::Prepared => &self.header_prepared,
            HeaderTarget::None => return Err(BoardAdapterError::SdmxSchemaDrift),
        };
        if destination.is_some() || self.target != HeaderTarget::None {
            return Err(BoardAdapterError::SdmxSchemaDrift);
        }
        match target {
            HeaderTarget::Id => self.header_id = Some(String::new()),
            HeaderTarget::Test => self.header_test = Some(String::new()),
            HeaderTarget::Prepared => self.header_prepared = Some(String::new()),
            HeaderTarget::None => {}
        }
        self.target = target;
        Ok(())
    }

    fn text(&mut self, text: &str, maximum: usize) -> Result<(), BoardAdapterError> {
        let target = match self.target {
            HeaderTarget::Id => self.header_id.as_mut(),
            HeaderTarget::Test => self.header_test.as_mut(),
            HeaderTarget::Prepared => self.header_prepared.as_mut(),
            HeaderTarget::None => {
                return if text.trim().is_empty() {
                    Ok(())
                } else {
                    Err(BoardAdapterError::SdmxSchemaDrift)
                };
            }
        };
        let target = target.ok_or(BoardAdapterError::SdmxSchemaDrift)?;
        if target.len().saturating_add(text.len()) > maximum {
            return Err(BoardAdapterError::StructuralLimitExceeded);
        }
        target.push_str(text);
        Ok(())
    }

    fn finish(
        self,
        contract: &BoardDatasetContract,
        limits: BoardParseLimits,
    ) -> Result<(BoardSdmxHeader, Vec<BoardSeries>), BoardAdapterError> {
        if self.depth != 0
            || !self.saw_declaration
            || !self.saw_root
            || !self.closed_root
            || !self.closed_header
            || !self.closed_dataset
            || self.current_series.is_some()
            || self.series.is_empty()
            || self.series.len() > limits.max_series()
        {
            return Err(BoardAdapterError::SdmxSchemaDrift);
        }
        let id = self.header_id.ok_or(BoardAdapterError::SdmxSchemaDrift)?;
        if !id.starts_with(
            contract
                .sdmx()
                .ok_or(BoardAdapterError::InvalidContract)?
                .header_id_prefix(),
        ) || self.header_test.as_deref() != Some("false")
        {
            return Err(BoardAdapterError::SdmxSchemaDrift);
        }
        let header = BoardSdmxHeader::try_new(
            &id,
            &self
                .header_prepared
                .ok_or(BoardAdapterError::SdmxSchemaDrift)?,
            &self
                .header_sender
                .ok_or(BoardAdapterError::SdmxSchemaDrift)?,
        )?;
        validate_series_scope(contract.series_scope(), &self.series)?;
        Ok((header, self.series))
    }
}

struct SeriesBuilder {
    attributes: BTreeMap<Box<str>, Box<str>>,
    observations: Vec<BoardObservation>,
}

impl SeriesBuilder {
    fn from_start(
        reader: &NsReader<&[u8]>,
        start: &BytesStart<'_>,
        limits: BoardParseLimits,
        xml_version: XmlVersion,
    ) -> Result<Self, BoardAdapterError> {
        let values = attributes(reader, start, limits, xml_version)?;
        Ok(Self {
            attributes: values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            observations: Vec::new(),
        })
    }

    fn push(&mut self, observation: BoardObservation) -> Result<(), BoardAdapterError> {
        if self
            .observations
            .last()
            .is_some_and(|previous| previous.period() >= observation.period())
        {
            return Err(BoardAdapterError::DuplicateIdentity);
        }
        self.observations.push(observation);
        Ok(())
    }

    fn finish(mut self, contract: &BoardDatasetContract) -> Result<BoardSeries, BoardAdapterError> {
        let series_name = take_required(&mut self.attributes, "SERIES_NAME")?;
        let frequency = take_required(&mut self.attributes, "FREQ")?;
        if frequency.as_ref() != contract.frequency().sdmx_code() {
            return Err(BoardAdapterError::SeriesMismatch);
        }
        let unit = take_required(&mut self.attributes, "UNIT")?;
        let multiplier_text = take_required(&mut self.attributes, "UNIT_MULT")?;
        let multiplier = Decimal::from_str_exact(&multiplier_text)
            .map_err(|_| BoardAdapterError::SeriesMismatch)?;
        let currency = take_required(&mut self.attributes, "CURRENCY")?;
        let description = self
            .attributes
            .remove("SERIES_DESCRIPTION")
            .or_else(|| self.attributes.remove("DESCRIPTION"))
            .ok_or(BoardAdapterError::SeriesMismatch)?;
        let unique_id = self
            .attributes
            .remove("UNIQUE_IDENTIFIER")
            .unwrap_or_else(|| format!("{0}/{0}/{series_name}", contract.release().code()).into());
        let expected = exact_contract(contract.series_scope(), &series_name)?;
        let lifecycle = if let Some(expected) = expected {
            if expected.unique_id() != unique_id.as_ref()
                || expected.unit() != unit.as_ref()
                || expected.multiplier() != multiplier.normalize()
                || expected.currency() != currency.as_ref()
                || expected
                    .expected_description()
                    .is_some_and(|value| value != description.as_ref())
            {
                return Err(BoardAdapterError::SeriesMismatch);
            }
            expected.lifecycle().clone()
        } else {
            BoardSeriesLifecycle::Active
        };
        BoardSeries::try_new(
            unique_id,
            series_name,
            description,
            unit,
            multiplier,
            currency,
            contract.frequency(),
            lifecycle,
            self.attributes,
            self.observations,
        )
    }
}

fn parse_observation(
    reader: &NsReader<&[u8]>,
    start: &BytesStart<'_>,
    contract: &BoardDatasetContract,
    limits: BoardParseLimits,
    xml_version: XmlVersion,
) -> Result<BoardObservation, BoardAdapterError> {
    let mut values = attributes(reader, start, limits, xml_version)?;
    let period = BoardPeriod::parse(
        &values
            .remove("TIME_PERIOD")
            .ok_or(BoardAdapterError::SdmxSchemaDrift)?,
        contract.frequency(),
    )?;
    let raw = values.remove("OBS_VALUE");
    let status = values
        .remove("OBS_STATUS")
        .ok_or(BoardAdapterError::SdmxSchemaDrift)?;
    let dimensions = values
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
    BoardObservation::try_new(
        period,
        BoardValue::parse(raw.as_deref(), &status)?,
        dimensions,
    )
}

fn attributes(
    reader: &NsReader<&[u8]>,
    start: &BytesStart<'_>,
    limits: BoardParseLimits,
    xml_version: XmlVersion,
) -> Result<BTreeMap<String, String>, BoardAdapterError> {
    let mut values = BTreeMap::new();
    for attribute in start.attributes() {
        let attribute =
            attribute.map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?;
        let raw_key = std::str::from_utf8(attribute.key.as_ref())
            .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?;
        if raw_key == "xmlns" || raw_key.starts_with("xmlns:") {
            continue;
        }
        let (namespace, local) = reader.resolver().resolve_attribute(attribute.key);
        if !matches!(namespace, ResolveResult::Unbound) {
            return Err(BoardAdapterError::SdmxSchemaDrift);
        }
        let key = std::str::from_utf8(local.as_ref())
            .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?
            .to_owned();
        let value = attribute
            .decoded_and_normalized_value(xml_version, reader.decoder())
            .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))?
            .into_owned();
        if key.is_empty()
            || key.len() > 128
            || value.len() > limits.max_text_bytes()
            || values.len() == limits.max_attributes()
            || values.insert(key, value).is_some()
        {
            return Err(BoardAdapterError::StructuralLimitExceeded);
        }
    }
    Ok(values)
}

fn local_name(start: &BytesStart<'_>) -> Result<String, BoardAdapterError> {
    std::str::from_utf8(start.local_name().as_ref())
        .map(str::to_owned)
        .map_err(|error| BoardAdapterError::InvalidXml(error.to_string()))
}

fn owned_namespace(actual: &ResolveResult<'_>) -> Result<Option<Vec<u8>>, BoardAdapterError> {
    match actual {
        ResolveResult::Bound(namespace) => Ok(Some(namespace.as_ref().to_vec())),
        ResolveResult::Unbound => Ok(None),
        ResolveResult::Unknown(_) => Err(BoardAdapterError::SdmxSchemaDrift),
    }
}

fn require_namespace(actual: Option<&[u8]>, expected: &[u8]) -> Result<(), BoardAdapterError> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(BoardAdapterError::SdmxSchemaDrift)
    }
}

fn take_required(
    values: &mut BTreeMap<Box<str>, Box<str>>,
    key: &str,
) -> Result<Box<str>, BoardAdapterError> {
    values
        .remove(key)
        .filter(|value| !value.is_empty())
        .ok_or(BoardAdapterError::SeriesMismatch)
}

fn exact_contract<'a>(
    scope: &'a BoardSeriesScope,
    name: &str,
) -> Result<Option<&'a BoardSeriesContract>, BoardAdapterError> {
    match scope {
        BoardSeriesScope::Exact { series } => series
            .iter()
            .find(|item| item.series_name() == name)
            .map(Some)
            .ok_or(BoardAdapterError::SeriesMismatch),
        BoardSeriesScope::StructureBoundCompleteRelease { .. } => Ok(None),
    }
}

fn validate_series_scope(
    scope: &BoardSeriesScope,
    parsed: &[BoardSeries],
) -> Result<(), BoardAdapterError> {
    if let BoardSeriesScope::Exact { series } = scope {
        let expected = series
            .iter()
            .map(BoardSeriesContract::series_name)
            .collect::<BTreeSet<_>>();
        let actual = parsed
            .iter()
            .map(BoardSeries::series_name)
            .collect::<BTreeSet<_>>();
        if expected != actual || parsed.len() != series.len() {
            return Err(BoardAdapterError::SeriesMismatch);
        }
    }
    Ok(())
}

fn sdmx_schema_digest(
    package: &SdmxPackageContract,
    header: &BoardSdmxHeader,
    artifacts: &[BoardArtifactReceipt],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_tag(
        &mut digest,
        "market-squawk-federal-reserve-sdmx-compact-schema-v1",
    );
    update_tag(&mut digest, package.message_namespace());
    update_tag(&mut digest, package.dataset_namespace());
    update_tag(&mut digest, header.id());
    update_tag(&mut digest, header.prepared());
    update_tag(&mut digest, header.sender_id());
    update_u64(&mut digest, artifacts.len() as u64);
    for artifact in artifacts {
        update_tag(&mut digest, artifact.name());
        update_tag(&mut digest, artifact.kind().as_str());
        update_bytes(&mut digest, &artifact.sha256());
    }
    finish(digest)
}
