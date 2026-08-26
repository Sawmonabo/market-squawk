use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::model::{
    DateError, FeedKind, FeedVersion, PcapObjectEncoding, Sha256Digest, TradeDate,
    TransportVersion,
};
use crate::planning::IexHistCatalogObservationReceipt;

/// Maximum exact catalog body accepted by this adapter core.
pub(crate) const MAX_CATALOG_BYTES: usize = 4 * 1024 * 1024;
/// Maximum date keys accepted in one catalog generation.
pub(crate) const MAX_CATALOG_DATES: usize = 5_000;
/// Maximum file descriptors accepted in one catalog generation.
pub(crate) const MAX_CATALOG_FILES: usize = 10_000;
const MAX_FILES_PER_DATE: usize = 8;
const MAX_LINK_BYTES: usize = 2_048;
const MAX_ETAG_BYTES: usize = 256;

/// Composition-bound HTTP metadata paired with one authority-observed exact catalog body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogTransportMetadata {
    /// HTTP status, required to be `200`.
    pub(crate) status: u16,
    /// Exact response content type.
    pub(crate) content_type: String,
    /// Exact HTTP response body length.
    pub(crate) content_length: u64,
    /// Optional bounded provider entity tag.
    pub(crate) etag: Option<String>,
    /// Opaque body/clock/attempt evidence minted while the catalog lease was active.
    pub(crate) observation: IexHistCatalogObservationReceipt,
}

/// Content-addressed catalog-generation receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CatalogReceipt {
    /// SHA-256 of the exact JSON body.
    pub body_sha256: Sha256Digest,
    /// Exact body bytes.
    pub body_bytes: u64,
    /// Bounded number of date keys.
    pub date_count: u32,
    /// Bounded number of descriptors.
    pub file_count: u32,
    /// Sum of provider-advertised compressed bytes.
    pub advertised_compressed_bytes: u64,
    /// Earliest observed descriptor date.
    pub earliest_date: TradeDate,
    /// Latest observed descriptor date.
    pub latest_date: TradeDate,
    /// Provider entity tag, when supplied.
    pub etag: Option<String>,
    /// Opaque authority-bound body, clock, reservation, storage-root, and attempt provenance.
    pub observation: IexHistCatalogObservationReceipt,
}

/// One validated descriptor from the mutable IEX HIST catalog.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CatalogFile {
    /// Provider trade date.
    pub trade_date: TradeDate,
    /// Raw provider feed label, including catalog families not decoded by this crate.
    pub feed: String,
    /// Raw provider feed version.
    pub feed_version: String,
    /// Raw provider transport version.
    pub transport_version: String,
    /// Provider-advertised compressed bytes.
    pub advertised_compressed_bytes: u64,
    /// Exact provider-returned HTTPS object URL.
    pub download_url: String,
    /// Filename bound into the exact object path.
    pub file_name: String,
    /// Exact provider object representation selected by the descriptor family.
    pub object_encoding: PcapObjectEncoding,
    /// Stable digest of the complete validated descriptor fields.
    pub descriptor_sha256: Sha256Digest,
}

/// Exact selection coordinates; no nearest-date or version fallback is permitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactFileRequest {
    /// Exact trade date.
    pub trade_date: TradeDate,
    /// Exact selected feed.
    pub feed: FeedKind,
    /// Exact feed-version family.
    pub feed_version: FeedVersion,
    /// Exact transport family.
    pub transport_version: TransportVersion,
    /// Exact provider object representation expected by the caller.
    pub object_encoding: PcapObjectEncoding,
    /// Exact catalog filename expected by the caller.
    pub file_name: String,
}

/// Catalog-parented exact file-selection receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SelectedFileReceipt {
    /// Parent catalog content digest.
    pub(crate) catalog_sha256: Sha256Digest,
    /// Exact parent catalog body length.
    pub(crate) catalog_bytes: u64,
    /// Trusted application-authority provenance for the exact parent catalog body.
    pub(crate) catalog_observation: IexHistCatalogObservationReceipt,
    /// Descriptor content digest.
    pub(crate) descriptor_sha256: Sha256Digest,
    /// Exact trade date.
    pub(crate) trade_date: TradeDate,
    /// Exact selected feed.
    pub(crate) feed: FeedKind,
    /// Exact selected feed version.
    pub(crate) feed_version: FeedVersion,
    /// Exact transport version.
    pub(crate) transport_version: TransportVersion,
    /// Exact provider object representation.
    pub(crate) object_encoding: PcapObjectEncoding,
    /// Exact filename.
    pub(crate) file_name: String,
    /// Exact provider-returned URL.
    pub(crate) download_url: String,
    /// Provider-advertised compressed bytes.
    pub(crate) advertised_compressed_bytes: u64,
}

impl SelectedFileReceipt {
    /// Returns the exact parent catalog-body identity.
    #[must_use]
    pub const fn catalog_sha256(&self) -> Sha256Digest {
        self.catalog_sha256
    }

    /// Returns the exact parent catalog body length.
    #[must_use]
    pub const fn catalog_bytes(&self) -> u64 {
        self.catalog_bytes
    }

    /// Returns the trusted local completion time for the parent catalog response.
    #[must_use]
    pub const fn catalog_retrieved_at_unix_nanos(&self) -> i64 {
        self.catalog_observation.retrieved_clock().unix_nanos()
    }

    /// Returns the exact authority-bound parent catalog observation.
    #[must_use]
    pub const fn catalog_observation(&self) -> &IexHistCatalogObservationReceipt {
        &self.catalog_observation
    }

    /// Returns the exact validated descriptor identity.
    #[must_use]
    pub const fn descriptor_sha256(&self) -> Sha256Digest {
        self.descriptor_sha256
    }

    /// Returns the exact trade date.
    #[must_use]
    pub const fn trade_date(&self) -> TradeDate {
        self.trade_date
    }

    /// Returns the exact selected feed.
    #[must_use]
    pub const fn feed(&self) -> FeedKind {
        self.feed
    }

    /// Returns the exact selected feed version.
    #[must_use]
    pub const fn feed_version(&self) -> FeedVersion {
        self.feed_version
    }

    /// Returns the exact selected transport version.
    #[must_use]
    pub const fn transport_version(&self) -> TransportVersion {
        self.transport_version
    }

    /// Returns the exact provider object representation.
    #[must_use]
    pub const fn object_encoding(&self) -> PcapObjectEncoding {
        self.object_encoding
    }

    /// Returns the exact catalog filename.
    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Returns the exact provider-returned download URL.
    #[must_use]
    pub fn download_url(&self) -> &str {
        &self.download_url
    }

    /// Returns provider-advertised compressed bytes.
    #[must_use]
    pub const fn advertised_compressed_bytes(&self) -> u64 {
        self.advertised_compressed_bytes
    }

    /// Returns the parent catalog observation date.
    #[must_use]
    pub const fn catalog_observed_on(&self) -> TradeDate {
        self.catalog_observation.retrieved_clock().observed_date()
    }

    /// Produces a stable parent identity for scheduling and downstream capture receipts.
    #[must_use]
    pub fn identity(&self) -> Sha256Digest {
        digest_fields(&[
            b"market-squawk/iex-hist-selected-file/v4",
            self.catalog_sha256.as_bytes(),
            &self.catalog_bytes.to_le_bytes(),
            self.catalog_observation.receipt_sha256().as_bytes(),
            self.descriptor_sha256.as_bytes(),
            self.trade_date.compact().as_bytes(),
            self.feed.catalog_name().as_bytes(),
            self.feed_version.catalog_value().as_bytes(),
            self.transport_version.catalog_value().as_bytes(),
            self.object_encoding.identity_value().as_bytes(),
            self.file_name.as_bytes(),
            self.download_url.as_bytes(),
            &self.advertised_compressed_bytes.to_le_bytes(),
        ])
    }
}

/// Parsed, bounded catalog generation.
#[derive(Clone, Debug)]
pub struct Catalog {
    receipt: CatalogReceipt,
    files: Vec<CatalogFile>,
}

impl Catalog {
    /// Parses and validates one exact catalog body and retains its content receipt.
    ///
    /// # Errors
    ///
    /// Rejects transport mismatch, body/schema drift, invalid descriptors, duplicates, and every
    /// configured count or byte overflow.
    pub(crate) fn parse(
        body: &[u8],
        metadata: CatalogTransportMetadata,
    ) -> Result<Self, CatalogError> {
        validate_transport(body, &metadata)?;
        let RawCatalog(raw_dates) =
            serde_json::from_slice(body).map_err(|_| CatalogError::InvalidJson)?;
        if raw_dates.is_empty() || raw_dates.len() > MAX_CATALOG_DATES {
            return Err(CatalogError::DateCount);
        }

        let mut files = Vec::new();
        files
            .try_reserve(raw_dates.len().saturating_mul(3).min(MAX_CATALOG_FILES))
            .map_err(|_| CatalogError::Capacity)?;
        let mut descriptors = BTreeSet::new();
        let mut advertised_total = 0_u64;
        let mut earliest = None;
        let mut latest = None;

        for (date_key, descriptors_for_date) in raw_dates {
            let trade_date = TradeDate::parse(&date_key).map_err(CatalogError::Date)?;
            if descriptors_for_date.is_empty() || descriptors_for_date.len() > MAX_FILES_PER_DATE {
                return Err(CatalogError::FilesPerDate);
            }
            earliest = Some(earliest.map_or(trade_date, |date: TradeDate| date.min(trade_date)));
            latest = Some(latest.map_or(trade_date, |date: TradeDate| date.max(trade_date)));

            for descriptor in descriptors_for_date {
                if files.len() >= MAX_CATALOG_FILES {
                    return Err(CatalogError::FileCount);
                }
                let file = validate_descriptor(trade_date, descriptor)?;
                if !descriptors.insert(file.descriptor_sha256) {
                    return Err(CatalogError::DuplicateDescriptor);
                }
                advertised_total = advertised_total
                    .checked_add(file.advertised_compressed_bytes)
                    .ok_or(CatalogError::AdvertisedBytesOverflow)?;
                files.push(file);
            }
        }

        files.sort_by(|left, right| {
            (
                left.trade_date,
                left.feed.as_str(),
                left.feed_version.as_str(),
                left.file_name.as_str(),
            )
                .cmp(&(
                    right.trade_date,
                    right.feed.as_str(),
                    right.feed_version.as_str(),
                    right.file_name.as_str(),
                ))
        });
        let body_bytes = u64::try_from(body.len()).map_err(|_| CatalogError::BodyTooLarge)?;
        let date_count = u32::try_from(
            files
                .iter()
                .map(|file| file.trade_date)
                .collect::<BTreeSet<_>>()
                .len(),
        )
        .map_err(|_| CatalogError::DateCount)?;
        let file_count = u32::try_from(files.len()).map_err(|_| CatalogError::FileCount)?;
        let receipt = CatalogReceipt {
            body_sha256: Sha256Digest::of(body),
            body_bytes,
            date_count,
            file_count,
            advertised_compressed_bytes: advertised_total,
            earliest_date: earliest.ok_or(CatalogError::DateCount)?,
            latest_date: latest.ok_or(CatalogError::DateCount)?,
            etag: metadata.etag,
            observation: metadata.observation,
        };
        Ok(Self { receipt, files })
    }

    /// Returns the exact catalog-generation receipt.
    #[must_use]
    pub const fn receipt(&self) -> &CatalogReceipt {
        &self.receipt
    }

    /// Returns all validated descriptors, including catalog families outside this decoder's scope.
    #[must_use]
    pub fn files(&self) -> &[CatalogFile] {
        &self.files
    }

    /// Selects exactly one supported descriptor without date/version/file fallback.
    ///
    /// # Errors
    ///
    /// Rejects incompatible feed/version pairs, malformed expected filenames, missing matches, or
    /// an ambiguous catalog.
    pub fn select(&self, request: &ExactFileRequest) -> Result<SelectedFileReceipt, CatalogError> {
        if request.feed_version.feed() != request.feed {
            return Err(CatalogError::UnsupportedVersion);
        }
        let expected_encoding = object_encoding(
            request.feed.catalog_name(),
            request.feed_version.catalog_value(),
            request.transport_version.catalog_value(),
        )?;
        if request.object_encoding != expected_encoding {
            return Err(CatalogError::ObjectEncodingMismatch);
        }
        let expected_name = expected_file_name(
            request.trade_date,
            request.feed.catalog_name(),
            request.feed_version.catalog_value(),
            request.transport_version.catalog_value(),
        );
        if request.file_name != expected_name {
            return Err(CatalogError::FilenameMismatch);
        }
        let mut matching = self.files.iter().filter(|file| {
            file.trade_date == request.trade_date
                && file.feed == request.feed.catalog_name()
                && file.feed_version == request.feed_version.catalog_value()
                && file.transport_version == request.transport_version.catalog_value()
                && file.object_encoding == request.object_encoding
                && file.file_name == request.file_name
        });
        let file = matching.next().ok_or(CatalogError::SelectionNotFound)?;
        if matching.next().is_some() {
            return Err(CatalogError::AmbiguousSelection);
        }
        Ok(SelectedFileReceipt {
            catalog_sha256: self.receipt.body_sha256,
            catalog_bytes: self.receipt.body_bytes,
            catalog_observation: self.receipt.observation.clone(),
            descriptor_sha256: file.descriptor_sha256,
            trade_date: file.trade_date,
            feed: request.feed,
            feed_version: request.feed_version,
            transport_version: request.transport_version,
            object_encoding: file.object_encoding,
            file_name: file.file_name.clone(),
            download_url: file.download_url.clone(),
            advertised_compressed_bytes: file.advertised_compressed_bytes,
        })
    }
}

fn validate_transport(
    body: &[u8],
    metadata: &CatalogTransportMetadata,
) -> Result<(), CatalogError> {
    if body.is_empty() || body.len() > MAX_CATALOG_BYTES {
        return Err(CatalogError::BodyTooLarge);
    }
    if metadata.status != 200
        || metadata.content_length != u64::try_from(body.len()).unwrap_or(u64::MAX)
        || !metadata
            .content_type
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return Err(CatalogError::InvalidTransportMetadata);
    }
    metadata
        .observation
        .validate()
        .map_err(|_| CatalogError::InvalidTransportMetadata)?;
    if metadata.observation.body_bytes() != u64::try_from(body.len()).unwrap_or(u64::MAX)
        || metadata.observation.body_sha256() != Sha256Digest::of(body)
    {
        return Err(CatalogError::InvalidTransportMetadata);
    }
    if metadata.etag.as_ref().is_some_and(|etag| {
        etag.is_empty()
            || etag.len() > MAX_ETAG_BYTES
            || !etag
                .bytes()
                .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    }) {
        return Err(CatalogError::InvalidTransportMetadata);
    }
    Ok(())
}

fn validate_descriptor(
    trade_date: TradeDate,
    descriptor: DescriptorWire,
) -> Result<CatalogFile, CatalogError> {
    if descriptor.date != trade_date.compact() {
        return Err(CatalogError::DescriptorDateMismatch);
    }
    let feed = match descriptor.feed.as_str() {
        "TOPS" | "DEEP" | "DPLS" | "DPLC" => descriptor.feed,
        _ => return Err(CatalogError::CatalogSchemaDrift),
    };
    if descriptor.protocol != "IEXTP1" {
        return Err(CatalogError::CatalogSchemaDrift);
    }
    validate_catalog_version(&feed, &descriptor.version)?;
    let object_encoding = object_encoding(&feed, &descriptor.version, &descriptor.protocol)?;
    let advertised_compressed_bytes = parse_advertised_bytes(&descriptor.size)?;
    let file_name =
        expected_file_name(trade_date, &feed, &descriptor.version, &descriptor.protocol);
    validate_download_url(&descriptor.link, trade_date, &file_name)?;
    let descriptor_sha256 = digest_fields(&[
        b"market-squawk/iex-hist-catalog-descriptor/v2",
        descriptor.date.as_bytes(),
        feed.as_bytes(),
        descriptor.version.as_bytes(),
        descriptor.protocol.as_bytes(),
        descriptor.size.as_bytes(),
        descriptor.link.as_bytes(),
        object_encoding.identity_value().as_bytes(),
    ]);
    Ok(CatalogFile {
        trade_date,
        feed,
        feed_version: descriptor.version,
        transport_version: descriptor.protocol,
        advertised_compressed_bytes,
        download_url: descriptor.link,
        file_name,
        object_encoding,
        descriptor_sha256,
    })
}

fn validate_catalog_version(feed: &str, version: &str) -> Result<(), CatalogError> {
    let valid = matches!(
        (feed, version),
        ("TOPS", "1.5" | "1.6") | ("DEEP" | "DPLS", "1.0") | ("DPLC", "1")
    );
    if valid {
        Ok(())
    } else {
        Err(CatalogError::CatalogSchemaDrift)
    }
}

fn object_encoding(
    feed: &str,
    version: &str,
    protocol: &str,
) -> Result<PcapObjectEncoding, CatalogError> {
    match (feed, version, protocol) {
        ("DPLC", "1", "IEXTP1") => Ok(PcapObjectEncoding::Identity),
        ("TOPS", "1.5" | "1.6", "IEXTP1")
        | ("DEEP" | "DPLS", "1.0", "IEXTP1") => Ok(PcapObjectEncoding::Gzip),
        _ => Err(CatalogError::CatalogSchemaDrift),
    }
}

fn parse_advertised_bytes(value: &str) -> Result<u64, CatalogError> {
    if value.is_empty()
        || value.len() > 20
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(CatalogError::InvalidAdvertisedBytes);
    }
    let bytes = value
        .parse::<u64>()
        .map_err(|_| CatalogError::InvalidAdvertisedBytes)?;
    if bytes == 0 {
        Err(CatalogError::InvalidAdvertisedBytes)
    } else {
        Ok(bytes)
    }
}

fn expected_file_name(trade_date: TradeDate, feed: &str, version: &str, protocol: &str) -> String {
    if (feed, version, protocol) == ("DPLC", "1", "IEXTP1") {
        // The exact admitted DPLC catalog family publishes version `1` in the descriptor while
        // retaining `1.0` and an uncompressed `.pcap` suffix in its provider-returned object name.
        format!("{}_IEXTP1_DPLC1.0.pcap", trade_date.compact())
    } else {
        format!(
            "{}_{}_{}{}.pcap.gz",
            trade_date.compact(),
            protocol,
            feed,
            version
        )
    }
}

fn validate_download_url(
    value: &str,
    trade_date: TradeDate,
    file_name: &str,
) -> Result<(), CatalogError> {
    if value.is_empty() || value.len() > MAX_LINK_BYTES {
        return Err(CatalogError::InvalidDownloadUrl);
    }
    let parsed = Url::parse(value).map_err(|_| CatalogError::InvalidDownloadUrl)?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("www.googleapis.com")
        || parsed.port().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(CatalogError::InvalidDownloadUrl);
    }
    let expected_path = format!(
        "/download/storage/v1/b/iex/o/data%2Ffeeds%2F{}%2F{}",
        trade_date.compact(),
        file_name
    );
    if parsed.path() != expected_path {
        return Err(CatalogError::InvalidDownloadUrl);
    }
    let pairs = parsed.query_pairs().collect::<Vec<_>>();
    if pairs.len() != 2 {
        return Err(CatalogError::InvalidDownloadUrl);
    }
    let generation = pairs
        .iter()
        .find_map(|(key, value)| (key == "generation").then_some(value.as_ref()));
    let alt = pairs
        .iter()
        .find_map(|(key, value)| (key == "alt").then_some(value.as_ref()));
    if generation.is_none_or(|value| {
        value.is_empty()
            || value.len() > 20
            || value.starts_with('0')
            || !value.bytes().all(|byte| byte.is_ascii_digit())
    }) || alt != Some("media")
    {
        return Err(CatalogError::InvalidDownloadUrl);
    }
    Ok(())
}

pub(crate) fn digest_fields(fields: &[&[u8]]) -> Sha256Digest {
    let mut hasher = Sha256::new();
    for field in fields {
        hasher.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(field);
    }
    Sha256Digest::from_bytes(hasher.finalize().into())
}

struct RawCatalog(BTreeMap<String, Vec<DescriptorWire>>);

impl<'de> Deserialize<'de> for RawCatalog {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RawCatalogVisitor)
    }
}

struct RawCatalogVisitor;

impl<'de> Visitor<'de> for RawCatalogVisitor {
    type Value = RawCatalog;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object mapping unique YYYYMMDD keys to descriptor arrays")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = BTreeMap::new();
        while let Some((key, value)) = map.next_entry::<String, Vec<DescriptorWire>>()? {
            if values.len() >= MAX_CATALOG_DATES || values.insert(key, value).is_some() {
                return Err(serde::de::Error::custom(
                    "duplicate or excessive catalog date key",
                ));
            }
        }
        Ok(RawCatalog(values))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DescriptorWire {
    link: String,
    date: String,
    feed: String,
    version: String,
    protocol: String,
    size: String,
}

/// Catalog receipt, schema, or exact-selection failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CatalogError {
    /// Body was empty or exceeded the bounded catalog ceiling.
    #[error("IEX HIST catalog body is empty or too large")]
    BodyTooLarge,
    /// HTTP metadata did not match the exact body.
    #[error("IEX HIST catalog transport metadata is invalid")]
    InvalidTransportMetadata,
    /// JSON was malformed, duplicated a date key, or did not match the closed schema.
    #[error("IEX HIST catalog JSON is invalid")]
    InvalidJson,
    /// A date failed canonical validation.
    #[error("IEX HIST catalog date is invalid: {0}")]
    Date(DateError),
    /// Date-key count was empty or excessive.
    #[error("IEX HIST catalog date count is outside bounds")]
    DateCount,
    /// A date carried an empty or excessive descriptor list.
    #[error("IEX HIST descriptor count for one date is outside bounds")]
    FilesPerDate,
    /// Total descriptor count exceeded the catalog ceiling.
    #[error("IEX HIST catalog file count exceeds its bound")]
    FileCount,
    /// Fallible allocation failed.
    #[error("IEX HIST catalog capacity is unavailable")]
    Capacity,
    /// Descriptor date did not match its parent key.
    #[error("IEX HIST descriptor date does not match its catalog key")]
    DescriptorDateMismatch,
    /// A field or version drifted outside the measured catalog schema.
    #[error("IEX HIST catalog schema or enumerated value drifted")]
    CatalogSchemaDrift,
    /// Provider-advertised compressed bytes were invalid.
    #[error("IEX HIST advertised compressed bytes are invalid")]
    InvalidAdvertisedBytes,
    /// Advertised catalog byte sum overflowed.
    #[error("IEX HIST advertised compressed-byte total overflowed")]
    AdvertisedBytesOverflow,
    /// Provider-returned object URL escaped the exact IEX bucket/file contract.
    #[error("IEX HIST download URL is invalid")]
    InvalidDownloadUrl,
    /// Two exact descriptors had the same validated identity.
    #[error("IEX HIST catalog contains a duplicate descriptor")]
    DuplicateDescriptor,
    /// Feed and decoder version are incompatible or unsupported.
    #[error("IEX HIST feed version is unsupported")]
    UnsupportedVersion,
    /// Caller filename did not match the exact versioned descriptor filename.
    #[error("IEX HIST expected filename is inconsistent")]
    FilenameMismatch,
    /// Caller object representation did not match the exact descriptor family.
    #[error("IEX HIST expected PCAP object encoding is inconsistent")]
    ObjectEncodingMismatch,
    /// No descriptor matched every exact selection coordinate.
    #[error("IEX HIST exact file selection was not found")]
    SelectionNotFound,
    /// More than one descriptor matched the exact request.
    #[error("IEX HIST exact file selection is ambiguous")]
    AmbiguousSelection,
}
