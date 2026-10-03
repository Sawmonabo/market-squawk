//! Exact SEC quarterly-bulk identities, clocks, coverage, and publication handoffs.

use std::collections::BTreeSet;

use chrono::NaiveDate;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, InstrumentId, ProviderIdentityRecord,
    ProviderIdentityRegistry, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{SecHttpValidators, SecObjectLocator, SecRepresentation};

use super::SecBulkError;

/// Current SEC Form N-PORT XML technical specification.
pub const SEC_NPORT_SCHEMA_VERSION: &str = "1.13";

/// Effective publication date of Form N-PORT XML technical specification 1.13.
pub const SEC_NPORT_SCHEMA_EFFECTIVE_DATE: &str = "2025-03-17";

/// Current SEC Form N-CEN XML technical specification.
pub const SEC_NCEN_SCHEMA_VERSION: &str = "3.1";

/// Effective publication date of Form N-CEN XML technical specification 3.1.
pub const SEC_NCEN_SCHEMA_EFFECTIVE_DATE: &str = "2025-06-16";

/// Frozen first-party catalogue audit date used to admit exact published archive locators.
pub const SEC_BULK_CATALOG_SNAPSHOT_DATE: &str = "2026-08-14";

/// One official SEC quarterly derived-data family.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum SecBulkFamily {
    /// Form N-PORT fund portfolio reports and holdings.
    Nport,
    /// Form N-CEN investment-company annual reports.
    Ncen,
}

impl SecBulkFamily {
    pub(crate) const fn tag(self) -> &'static str {
        match self {
            Self::Nport => "nport",
            Self::Ncen => "ncen",
        }
    }

    /// Returns the required metadata member name.
    pub const fn metadata_member(self) -> &'static str {
        match self {
            Self::Nport => "nport_metadata.json",
            Self::Ncen => "ncen_metadata.json",
        }
    }

    /// Returns the required archive-local readme member name.
    pub const fn archive_readme_member(self) -> &'static str {
        match self {
            Self::Nport => "nport_readme.htm",
            Self::Ncen => "ncen_readme.htm",
        }
    }
}

/// Exact calendar quarter encoded by an SEC derived-bulk archive name.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SecQuarter {
    year: u16,
    quarter: u8,
}

impl SecQuarter {
    /// Constructs a valid EDGAR quarterly coordinate.
    pub const fn try_new(year: u16, quarter: u8) -> Result<Self, SecBulkError> {
        if year < 2018 || quarter == 0 || quarter > 4 {
            Err(SecBulkError::InvalidQuarter)
        } else {
            Ok(Self { year, quarter })
        }
    }

    /// Returns the four-digit year.
    pub const fn year(self) -> u16 {
        self.year
    }

    /// Returns the quarter number in `1..=4`.
    pub const fn quarter(self) -> u8 {
        self.quarter
    }

    pub(crate) const fn is_catalogued(
        self,
        family: SecBulkFamily,
        snapshot: SecBulkCatalogSnapshot,
    ) -> bool {
        let ordinal = self.year as u32 * 4 + self.quarter as u32;
        let minimum = match family {
            SecBulkFamily::Nport => 2019 * 4 + 4,
            SecBulkFamily::Ncen => 2018 * 4 + 3,
        };
        let maximum =
            snapshot.latest_published.year as u32 * 4 + snapshot.latest_published.quarter as u32;
        ordinal >= minimum && ordinal <= maximum
    }
}

/// Exact audited boundary of the official SEC quarterly-download catalogues.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecBulkCatalogSnapshot {
    audited_at: NaiveDate,
    latest_published: SecQuarter,
}

impl SecBulkCatalogSnapshot {
    /// Returns the first-party catalogue state verified on 2026-08-14.
    pub fn official_2026_08_14() -> Result<Self, SecBulkError> {
        Ok(Self {
            audited_at: NaiveDate::parse_from_str(SEC_BULK_CATALOG_SNAPSHOT_DATE, "%Y-%m-%d")
                .map_err(|_| SecBulkError::InvalidQuarter)?,
            latest_published: SecQuarter::try_new(2026, 2)?,
        })
    }

    /// Returns the evidence audit date for the exact catalogue boundary.
    pub const fn audited_at(self) -> NaiveDate {
        self.audited_at
    }

    /// Returns the latest observed published quarter for both current SEC catalogue pages.
    pub const fn latest_published(self) -> SecQuarter {
        self.latest_published
    }
}

/// Exact accepted filing-schema authority for a bulk family.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecBulkSchemaIdentity {
    family: SecBulkFamily,
    version: SourceIdentifier,
    effective_date: NaiveDate,
    technical_spec_locator: SourceIdentifier,
}

impl SecBulkSchemaIdentity {
    /// Returns the currently accepted schema identity for one family.
    pub fn current(family: SecBulkFamily) -> Result<Self, SecBulkError> {
        let (version, date, locator) = match family {
            SecBulkFamily::Nport => (
                SEC_NPORT_SCHEMA_VERSION,
                SEC_NPORT_SCHEMA_EFFECTIVE_DATE,
                "https://www.sec.gov/file/form-n-port-xml-tech-specs-113",
            ),
            SecBulkFamily::Ncen => (
                SEC_NCEN_SCHEMA_VERSION,
                SEC_NCEN_SCHEMA_EFFECTIVE_DATE,
                "https://www.sec.gov/file/form-n-cen-xml-tech-specs-31",
            ),
        };
        Ok(Self {
            family,
            version: SourceIdentifier::try_from(version)?,
            effective_date: NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .map_err(|_| SecBulkError::InvalidSchemaIdentity)?,
            technical_spec_locator: SourceIdentifier::try_from(locator)?,
        })
    }

    /// Returns the filing family governed by this schema.
    pub const fn family(&self) -> SecBulkFamily {
        self.family
    }

    /// Returns the exact SEC version string.
    pub const fn version(&self) -> &SourceIdentifier {
        &self.version
    }

    /// Returns the SEC publication/effective date of the technical specification.
    pub const fn effective_date(&self) -> NaiveDate {
        self.effective_date
    }

    /// Returns the exact official technical-specification locator.
    pub const fn technical_spec_locator(&self) -> &SourceIdentifier {
        &self.technical_spec_locator
    }
}

/// Explicit relationship between an accepted filing schema and one derived quarterly archive.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum SecBulkCoverage {
    /// The archive is a derived representation, includes amendments, and has no declared schema
    /// exclusion for the selected accepted schema.
    DerivedAsFiledIncludingAmendments,
    /// The SEC-derived archive explicitly excludes filings accepted under this schema.
    AcceptedSchemaExcluded {
        /// Accepted schema not represented by the derived archive.
        schema: SecBulkSchemaIdentity,
    },
}

impl SecBulkCoverage {
    /// Returns the current official declared coverage for one family.
    pub fn current(family: SecBulkFamily, quarter: SecQuarter) -> Result<Self, SecBulkError> {
        match family {
            SecBulkFamily::Nport => Ok(Self::DerivedAsFiledIncludingAmendments),
            SecBulkFamily::Ncen => {
                let first_schema_31_quarter = SecQuarter::try_new(2025, 2)?;
                if quarter < first_schema_31_quarter {
                    // Schema 3.1 had not yet been accepted, so it cannot be an exclusion from an
                    // older quarter. Exact historical filing-schema identity remains a full-
                    // filing coordinate; the derived archive is bound by metadata/readme digests.
                    Ok(Self::DerivedAsFiledIncludingAmendments)
                } else {
                    Ok(Self::AcceptedSchemaExcluded {
                        schema: SecBulkSchemaIdentity::current(SecBulkFamily::Ncen)?,
                    })
                }
            }
        }
    }

    /// Returns whether a missing archive row could ever prove that no filing exists.
    pub const fn missing_row_proves_no_filing(&self) -> bool {
        false
    }
}

/// Per-filing result of reconciling full EDGAR filings against a derived bulk release.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecBulkRepresentationState {
    /// The exact accession is represented in the selected bulk archive.
    Represented,
    /// A full EDGAR filing exists but the selected derived archive does not represent it.
    BulkNotRepresented {
        /// Closed reason for the derived-coverage gap.
        reason: SecBulkNotRepresentedReason,
    },
    /// Full-filing reconciliation has not yet established whether a row should be present.
    FullFilingReconciliationRequired,
}

/// Why an existing filing is absent from a derived bulk archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecBulkNotRepresentedReason {
    /// N-CEN schema 3.1 filings are excluded from the current derived data sets.
    AcceptedSchemaExcluded,
    /// The filing missed the quarter's 17:30 ET last-business-day inclusion cutoff.
    AfterQuarterCutoff,
    /// The SEC-derived data set omitted source filing metadata or a derived row.
    DerivedExtractionGap,
}

/// Exact family/quarter/schema coordinate for one bulk acquisition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkSelection {
    family: SecBulkFamily,
    quarter: SecQuarter,
    accepted_schema: SecBulkSchemaIdentity,
    coverage: SecBulkCoverage,
    archive_locator: SourceIdentifier,
    readme_locator: SourceIdentifier,
    catalog_snapshot: SecBulkCatalogSnapshot,
}

impl SecBulkSelection {
    /// Builds current official quarterly coordinates.
    pub fn current(family: SecBulkFamily, quarter: SecQuarter) -> Result<Self, SecBulkError> {
        Self::from_catalog(
            family,
            quarter,
            SecBulkCatalogSnapshot::official_2026_08_14()?,
        )
    }

    /// Builds coordinates only when an explicit audited catalogue proves the quarter exists.
    pub fn from_catalog(
        family: SecBulkFamily,
        quarter: SecQuarter,
        catalog_snapshot: SecBulkCatalogSnapshot,
    ) -> Result<Self, SecBulkError> {
        if !quarter.is_catalogued(family, catalog_snapshot) {
            return Err(SecBulkError::QuarterNotPublished);
        }
        let archive = SecObjectLocator::quarterly_bulk_archive(family, quarter)?;
        let readme = SecObjectLocator::quarterly_bulk_readme(family)?;
        Ok(Self {
            family,
            quarter,
            accepted_schema: SecBulkSchemaIdentity::current(family)?,
            coverage: SecBulkCoverage::current(family, quarter)?,
            archive_locator: SourceIdentifier::try_from(archive.url())?,
            readme_locator: SourceIdentifier::try_from(readme.url())?,
            catalog_snapshot,
        })
    }

    /// Returns the selected derived-data family.
    pub const fn family(&self) -> SecBulkFamily {
        self.family
    }

    /// Returns the exact quarterly release coordinate.
    pub const fn quarter(&self) -> SecQuarter {
        self.quarter
    }

    /// Returns the accepted full-filing schema authority.
    pub const fn accepted_schema(&self) -> &SecBulkSchemaIdentity {
        &self.accepted_schema
    }

    /// Returns declared derived-bulk coverage.
    pub const fn coverage(&self) -> &SecBulkCoverage {
        &self.coverage
    }

    /// Returns the exact SEC archive URL.
    pub const fn archive_locator(&self) -> &SourceIdentifier {
        &self.archive_locator
    }

    /// Returns the exact SEC readme URL.
    pub const fn readme_locator(&self) -> &SourceIdentifier {
        &self.readme_locator
    }

    /// Returns the exact first-party catalogue boundary that admitted this locator.
    pub const fn catalog_snapshot(&self) -> SecBulkCatalogSnapshot {
        self.catalog_snapshot
    }
}

/// Closed response-body family for one streamed SEC bulk artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecBulkMediaKind {
    /// ZIP archive bytes.
    Zip,
    /// Official PDF readme bytes.
    Pdf,
}

/// Exact successful HTTP response metadata for a streamed bulk artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkTransportEvidence {
    http_status: u16,
    media_kind: SecBulkMediaKind,
    media_type: Option<String>,
    validators: SecHttpValidators,
    last_modified_at: Option<Timestamp>,
    body_received_at: Timestamp,
}

impl SecBulkTransportEvidence {
    /// Preserves exact successful HTTP response metadata without request headers or contact data.
    pub fn try_new(
        http_status: u16,
        media_kind: SecBulkMediaKind,
        media_type: Option<&str>,
        validators: SecHttpValidators,
        body_received_at: Timestamp,
    ) -> Result<Self, SecBulkError> {
        let last_modified_at = validators
            .last_modified()
            .map(parse_http_timestamp)
            .transpose()?;
        if http_status != 200
            || media_type.is_none_or(|value| {
                value.is_empty()
                    || value.len() > 256
                    || !value.is_ascii()
                    || value.bytes().any(|byte| byte.is_ascii_control())
                    || !admitted_media_type(media_kind, value)
            })
            || last_modified_at.is_some_and(|modified| modified > body_received_at)
        {
            return Err(SecBulkError::InvalidCapture);
        }
        Ok(Self {
            http_status,
            media_kind,
            media_type: media_type.map(str::to_owned),
            validators,
            last_modified_at,
            body_received_at,
        })
    }

    /// Returns the exact successful HTTP status.
    pub const fn http_status(&self) -> u16 {
        self.http_status
    }

    /// Returns the closed expected body family.
    pub const fn media_kind(&self) -> SecBulkMediaKind {
        self.media_kind
    }

    /// Returns the exact provider media-type header when supplied.
    pub fn media_type(&self) -> Option<&str> {
        self.media_type.as_deref()
    }

    /// Returns exact ETag/Last-Modified validators when supplied.
    pub const fn validators(&self) -> &SecHttpValidators {
        &self.validators
    }

    /// Returns parsed HTTP Last-Modified time when supplied and valid.
    pub const fn last_modified_at(&self) -> Option<Timestamp> {
        self.last_modified_at
    }

    /// Returns the trusted clock after the complete body reached the client.
    pub const fn body_received_at(&self) -> Timestamp {
        self.body_received_at
    }
}

/// Streamed, sealed exact SEC archive or readme representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkCapture {
    selection: SecBulkSelection,
    locator: SourceIdentifier,
    evidence: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    retrieval_revision: u64,
    transport: SecBulkTransportEvidence,
}

impl SecBulkCapture {
    pub(crate) fn try_new(
        selection: SecBulkSelection,
        locator: SourceIdentifier,
        evidence: EvidenceDigest,
        size_bytes: u64,
        first_observed_at: Timestamp,
        retrieval_revision: u64,
        transport: SecBulkTransportEvidence,
    ) -> Result<Self, SecBulkError> {
        let expected_media_kind = if locator == *selection.archive_locator() {
            SecBulkMediaKind::Zip
        } else if locator == *selection.readme_locator() {
            SecBulkMediaKind::Pdf
        } else {
            return Err(SecBulkError::InvalidCapture);
        };
        if evidence.algorithm() != DigestAlgorithm::Sha256
            || size_bytes == 0
            || retrieval_revision == 0
            || transport.media_kind != expected_media_kind
            || transport.body_received_at > first_observed_at
            || transport
                .last_modified_at
                .is_some_and(|modified| modified > first_observed_at)
        {
            return Err(SecBulkError::InvalidCapture);
        }
        Ok(Self {
            selection,
            locator,
            evidence,
            size_bytes,
            first_observed_at,
            retrieval_revision,
            transport,
        })
    }

    pub(crate) fn try_from_sealed_representation(
        selection: SecBulkSelection,
        locator: SourceIdentifier,
        evidence: EvidenceDigest,
        size_bytes: u64,
        first_observed_at: Timestamp,
        retrieval_revision: u64,
        transport: SecBulkTransportEvidence,
    ) -> Result<Self, SecBulkError> {
        Self::try_new(
            selection,
            locator,
            evidence,
            size_bytes,
            first_observed_at,
            retrieval_revision,
            transport,
        )
    }

    /// Admits a capture only from a durable representation-registry decision.
    ///
    /// The registry-issued value exclusively supplies the locator, exact-byte identity, size,
    /// first-observed clock, and monotonic retrieval revision. Archive inspection still reopens
    /// and hashes the corresponding raw object before admitting a layout.
    pub fn try_from_registry_representation(
        selection: SecBulkSelection,
        representation: SecRepresentation,
        transport: SecBulkTransportEvidence,
    ) -> Result<Self, SecBulkError> {
        if representation.validators() != transport.validators() {
            return Err(SecBulkError::InvalidCapture);
        }
        let locator = SourceIdentifier::try_from(representation.locator())?;
        Self::try_from_sealed_representation(
            selection,
            locator,
            representation.evidence(),
            representation.size_bytes(),
            representation.first_observed_at(),
            representation.retrieval_revision(),
            transport,
        )
    }

    /// Returns exact family, quarter, schema, and coverage coordinates.
    pub const fn selection(&self) -> &SecBulkSelection {
        &self.selection
    }

    /// Returns the exact provider locator.
    pub const fn locator(&self) -> &SourceIdentifier {
        &self.locator
    }

    /// Returns the sealed response-body digest.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }

    /// Returns the exact compressed response-body length.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the first local observation of these exact bytes.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns the durable monotonic representation revision for this URL.
    pub const fn retrieval_revision(&self) -> u64 {
        self.retrieval_revision
    }

    /// Returns exact status/media/validator/body-receipt transport evidence.
    pub const fn transport(&self) -> &SecBulkTransportEvidence {
        &self.transport
    }
}

/// One exact, validated W3C-tabular member of an SEC bulk archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkColumnContract {
    name: SourceIdentifier,
    datatype_base: String,
    max_length: Option<u64>,
    data_precision: Option<SecBulkNumericAttribute>,
    data_scale: Option<SecBulkNumericAttribute>,
    required: bool,
}

/// Exact JSON scalar used by SEC CSVW `dataPrecision` and `dataScale`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SecBulkNumericAttribute {
    /// A numeric precision or scale.
    Value(u64),
    /// The provider's literal JSON string `"NULL"`, meaning no declared numeric ceiling.
    ProviderNull,
}

impl SecBulkColumnContract {
    pub(crate) fn try_new(
        name: SourceIdentifier,
        datatype_base: String,
        max_length: Option<u64>,
        data_precision: Option<SecBulkNumericAttribute>,
        data_scale: Option<SecBulkNumericAttribute>,
        required: bool,
    ) -> Result<Self, SecBulkError> {
        let datatype_is_valid = match datatype_base.as_str() {
            "string" => max_length.is_some() && data_precision.is_none() && data_scale.is_none(),
            "date (DD-MON-YYYY)" => {
                max_length.is_none() && data_precision.is_none() && data_scale.is_none()
            }
            "NUMBER" => match (data_precision, data_scale) {
                (
                    Some(SecBulkNumericAttribute::Value(precision)),
                    Some(SecBulkNumericAttribute::Value(scale)),
                ) => precision > 0 && precision <= 38 && scale <= precision,
                (
                    Some(SecBulkNumericAttribute::ProviderNull),
                    Some(SecBulkNumericAttribute::ProviderNull),
                ) => true,
                _ => false,
            },
            _ => false,
        };
        if !datatype_is_valid || max_length == Some(0) {
            return Err(SecBulkError::InvalidMetadata);
        }
        Ok(Self {
            name,
            datatype_base,
            max_length,
            data_precision,
            data_scale,
            required,
        })
    }

    /// Returns the exact ordered SEC column name.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the exact CSVW datatype base declared by the SEC metadata.
    pub fn datatype_base(&self) -> &str {
        &self.datatype_base
    }

    /// Returns the exact CSVW maximum length when supplied.
    pub const fn max_length(&self) -> Option<u64> {
        self.max_length
    }

    /// Returns the exact CSVW numeric precision scalar when supplied.
    pub const fn data_precision(&self) -> Option<SecBulkNumericAttribute> {
        self.data_precision
    }

    /// Returns the exact CSVW numeric scale scalar when supplied.
    pub const fn data_scale(&self) -> Option<SecBulkNumericAttribute> {
        self.data_scale
    }

    /// Returns whether the SEC metadata marks the field required.
    pub const fn required(&self) -> bool {
        self.required
    }
}

/// Exact metadata-declared schema for one official table, whether or not that quarter populated it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkDeclaredTableContract {
    name: SourceIdentifier,
    primary_key: Vec<SourceIdentifier>,
    columns: Vec<SecBulkColumnContract>,
}

impl SecBulkDeclaredTableContract {
    pub(crate) fn try_new(
        name: SourceIdentifier,
        primary_key: Vec<SourceIdentifier>,
        columns: Vec<SecBulkColumnContract>,
    ) -> Result<Self, SecBulkError> {
        if columns.is_empty()
            || primary_key
                .iter()
                .any(|key| !columns.iter().any(|column| column.name() == key))
        {
            return Err(SecBulkError::InvalidMetadata);
        }
        Ok(Self {
            name,
            primary_key,
            columns,
        })
    }

    /// Returns the exact metadata member name.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns exact ordered metadata primary-key fields, including an intentional empty key.
    pub fn primary_key(&self) -> &[SourceIdentifier] {
        &self.primary_key
    }

    /// Returns exact ordered metadata datatype and requiredness contracts.
    pub fn columns(&self) -> &[SecBulkColumnContract] {
        &self.columns
    }
}

/// One exact, validated W3C-tabular member of an SEC bulk archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkTableReceipt {
    name: SourceIdentifier,
    evidence: EvidenceDigest,
    decoded_bytes: u64,
    row_count: u64,
    primary_key: Vec<SourceIdentifier>,
    columns: Vec<SecBulkColumnContract>,
}

impl SecBulkTableReceipt {
    pub(crate) fn try_new(
        name: SourceIdentifier,
        evidence: EvidenceDigest,
        decoded_bytes: u64,
        row_count: u64,
        primary_key: Vec<SourceIdentifier>,
        columns: Vec<SecBulkColumnContract>,
    ) -> Result<Self, SecBulkError> {
        if evidence.algorithm() != DigestAlgorithm::Sha256
            || decoded_bytes == 0
            || columns.is_empty()
            || primary_key
                .iter()
                .any(|key| !columns.iter().any(|column| column.name() == key))
        {
            return Err(SecBulkError::InvalidLayout);
        }
        Ok(Self {
            name,
            evidence,
            decoded_bytes,
            row_count,
            primary_key,
            columns,
        })
    }

    /// Returns the exact archive member name.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the exact decoded member digest.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }

    /// Returns the exact decoded byte length.
    pub const fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }

    /// Returns the exact number of physical data rows after the one-row header.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns exact ordered primary-key columns from SEC metadata.
    pub fn primary_key(&self) -> &[SourceIdentifier] {
        &self.primary_key
    }

    /// Returns exact ordered header and datatype contracts from SEC metadata.
    pub fn columns(&self) -> &[SecBulkColumnContract] {
        &self.columns
    }
}

/// Closed, content-addressed archive layout admitted for native row publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkLayoutManifest {
    capture: SecBulkCapture,
    official_readme_capture: SecBulkCapture,
    metadata_evidence: EvidenceDigest,
    readme_evidence: EvidenceDigest,
    declared_tables: Vec<SecBulkDeclaredTableContract>,
    tables: Vec<SecBulkTableReceipt>,
    absent_declared_tables: Vec<SourceIdentifier>,
    expanded_bytes: u64,
    evidence: EvidenceDigest,
}

impl SecBulkLayoutManifest {
    pub(crate) fn try_new(
        capture: SecBulkCapture,
        official_readme_capture: SecBulkCapture,
        metadata_evidence: EvidenceDigest,
        readme_evidence: EvidenceDigest,
        declared_tables: Vec<SecBulkDeclaredTableContract>,
        tables: Vec<SecBulkTableReceipt>,
        absent_declared_tables: Vec<SourceIdentifier>,
        expanded_bytes: u64,
    ) -> Result<Self, SecBulkError> {
        let present_names = tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<BTreeSet<_>>();
        let absent_names = absent_declared_tables
            .iter()
            .map(SourceIdentifier::as_str)
            .collect::<BTreeSet<_>>();
        let declared_names = declared_tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<BTreeSet<_>>();
        let expected_tables = match capture.selection.family {
            SecBulkFamily::Nport => 30,
            SecBulkFamily::Ncen => 53,
        };
        let mut previous_kind = None;
        let declared_order_is_exact = declared_tables.iter().all(|table| {
            let Ok(kind) =
                SecBulkTableKind::from_member(capture.selection.family, table.name().as_str())
            else {
                return false;
            };
            if previous_kind.is_some_and(|previous| previous >= kind) {
                return false;
            }
            previous_kind = Some(kind);
            true
        });
        if capture.selection != official_readme_capture.selection
            || capture.locator != capture.selection.archive_locator
            || official_readme_capture.locator != capture.selection.readme_locator
            || metadata_evidence.algorithm() != DigestAlgorithm::Sha256
            || readme_evidence.algorithm() != DigestAlgorithm::Sha256
            || tables.is_empty()
            || declared_tables.len() != expected_tables
            || declared_names.len() != declared_tables.len()
            || !declared_order_is_exact
            || expanded_bytes == 0
            || present_names.len() != tables.len()
            || absent_names.len() != absent_declared_tables.len()
            || present_names.iter().any(|name| absent_names.contains(name))
            || present_names
                .union(&absent_names)
                .copied()
                .collect::<BTreeSet<_>>()
                != declared_names
            || tables.iter().any(|receipt| {
                declared_tables
                    .iter()
                    .find(|contract| contract.name == receipt.name)
                    .is_none_or(|contract| {
                        contract.primary_key != receipt.primary_key
                            || contract.columns != receipt.columns
                    })
            })
        {
            return Err(SecBulkError::InvalidLayout);
        }
        let evidence = layout_digest(
            &capture,
            &official_readme_capture,
            metadata_evidence,
            readme_evidence,
            &declared_tables,
            &tables,
            &absent_declared_tables,
            expanded_bytes,
        );
        Ok(Self {
            capture,
            official_readme_capture,
            metadata_evidence,
            readme_evidence,
            declared_tables,
            tables,
            absent_declared_tables,
            expanded_bytes,
            evidence,
        })
    }

    /// Returns the exact streamed archive receipt.
    pub const fn capture(&self) -> &SecBulkCapture {
        &self.capture
    }

    /// Returns exact official PDF readme capture and representation revision.
    pub const fn official_readme_capture(&self) -> &SecBulkCapture {
        &self.official_readme_capture
    }

    /// Returns exact W3C metadata bytes identity.
    pub const fn metadata_evidence(&self) -> EvidenceDigest {
        self.metadata_evidence
    }

    /// Returns exact archive-local readme bytes identity.
    pub const fn readme_evidence(&self) -> EvidenceDigest {
        self.readme_evidence
    }

    /// Returns exact metadata contracts for every official table, including declared-absent ones.
    pub fn declared_table_contracts(&self) -> &[SecBulkDeclaredTableContract] {
        &self.declared_tables
    }

    /// Resolves one exact metadata table contract without accepting aliases.
    pub fn declared_table(&self, name: &str) -> Option<&SecBulkDeclaredTableContract> {
        self.declared_tables
            .iter()
            .find(|table| table.name.as_str() == name)
    }

    /// Returns every archive-present, metadata-declared TSV member in metadata order.
    pub fn tables(&self) -> &[SecBulkTableReceipt] {
        &self.tables
    }

    /// Returns metadata-declared tables omitted because that quarter has no populated rows.
    ///
    /// An absent derived table is never evidence that no underlying filing or fact exists.
    pub fn absent_declared_tables(&self) -> &[SourceIdentifier] {
        &self.absent_declared_tables
    }

    /// Returns total declared decoded bytes across all admitted archive members.
    pub const fn expanded_bytes(&self) -> u64 {
        self.expanded_bytes
    }

    /// Returns the deterministic layout-manifest content identity.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }

    /// Resolves an exact table receipt without accepting case or path aliases.
    pub fn table(&self, name: &str) -> Option<&SecBulkTableReceipt> {
        self.tables.iter().find(|table| table.name.as_str() == name)
    }
}

/// Exact provider NUMBER lexical value without a false 96-bit decimal ceiling.
///
/// SEC N-PORT declares up to 38 digits, which is wider than `rust_decimal::Decimal`. The exact
/// string is therefore authoritative; callers may request a Decimal only when it is representable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecExactNumber {
    lexical: String,
}

impl SecExactNumber {
    pub(crate) fn from_validated_lexical(lexical: &str) -> Self {
        Self {
            lexical: lexical.to_owned(),
        }
    }

    /// Returns the exact SEC fixed-point lexical value.
    pub fn as_str(&self) -> &str {
        &self.lexical
    }

    /// Converts only values representable by `rust_decimal`; exact source text remains available.
    pub fn to_decimal(&self) -> Option<Decimal> {
        Decimal::from_str_exact(&self.lexical).ok()
    }
}

impl<'de> Deserialize<'de> for SecExactNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            lexical: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        let unsigned = wire.lexical.strip_prefix('-').unwrap_or(&wire.lexical);
        let mut parts = unsigned.split('.');
        let integer = parts.next().unwrap_or_default();
        let fraction = parts.next();
        let valid = !wire.lexical.is_empty()
            && wire.lexical.len() <= 128
            && !wire.lexical.starts_with('+')
            && parts.next().is_none()
            && !(integer.is_empty() && fraction.is_none_or(str::is_empty))
            && integer.bytes().all(|byte| byte.is_ascii_digit())
            && fraction.is_none_or(|digits| {
                !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
            });
        if !valid {
            return Err(serde::de::Error::custom("invalid exact SEC NUMBER lexical"));
        }
        Ok(Self {
            lexical: wire.lexical,
        })
    }
}

/// Provider-native N-PORT holding row projected from `FUND_REPORTED_HOLDING.tsv`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNportHoldingRow {
    /// Exact EDGAR accession, including dashes.
    pub accession: SourceIdentifier,
    /// Provider-native `HOLDING_ID`, scoped to the source archive/filing.
    pub holding_id: SourceIdentifier,
    /// Source-reported issuer name; never identity evidence by itself.
    pub issuer_name: Option<String>,
    /// Source-reported issuer LEI.
    pub issuer_lei: Option<SourceIdentifier>,
    /// Source-reported issue title/description.
    pub issuer_title: Option<String>,
    /// Source-reported CUSIP.
    pub cusip: Option<SourceIdentifier>,
    /// Exact reported quantity/principal balance.
    pub balance: Option<SecExactNumber>,
    /// Source-reported balance unit.
    pub unit: Option<SourceIdentifier>,
    /// Description when the unit is `other`.
    pub other_unit_description: Option<String>,
    /// Source-reported ISO currency code.
    pub currency: Option<SourceIdentifier>,
    /// Exact source-reported currency value.
    pub value: Option<SecExactNumber>,
    /// Exact source-reported exchange rate.
    pub exchange_rate: Option<SecExactNumber>,
    /// Exact source-reported percentage of fund net assets.
    pub percentage: Option<SecExactNumber>,
    /// Source-reported long/short/not-applicable payoff profile.
    pub payoff_profile: Option<SourceIdentifier>,
    /// Source-reported asset category.
    pub asset_category: Option<SourceIdentifier>,
    /// Source description for an `other` asset category.
    pub other_asset: Option<String>,
    /// Source-reported issuer category.
    pub issuer_type: Option<SourceIdentifier>,
    /// Source description for an `other` issuer category.
    pub other_issuer: Option<String>,
    /// Source-reported investment-country code.
    pub investment_country: Option<SourceIdentifier>,
    /// Source-reported restricted-security state.
    pub restricted_security: Option<bool>,
    /// Source-reported fair-value hierarchy level.
    pub fair_value_level: Option<SourceIdentifier>,
    /// Source-reported derivative category.
    pub derivative_category: Option<SourceIdentifier>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity; exact raw lineage remains the table/archive digest.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-PORT filing row keyed by accession.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNportSubmissionRow {
    /// Exact EDGAR accession.
    pub accession: SourceIdentifier,
    /// Civil filing date from the derived archive.
    pub filing_date: Option<NaiveDate>,
    /// Exact form (`NPORT-P`, `NPORT-P/A`, or notice form).
    pub form: SourceIdentifier,
    /// Fiscal year-end reported by the filer.
    pub report_ending_period: Option<NaiveDate>,
    /// Date as of which portfolio information is reported.
    pub report_date: Option<NaiveDate>,
    /// Source-reported final-filing state.
    pub is_last_filing: Option<bool>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-PORT registrant row keyed by accession.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNportRegistrantRow {
    /// Exact EDGAR accession.
    pub accession: SourceIdentifier,
    /// Exact zero-padded ten-digit registrant CIK.
    pub cik: SourceIdentifier,
    /// Source name retained as evidence, never used as a security identity bridge.
    pub registrant_name: Option<String>,
    /// Source-reported registrant LEI.
    pub lei: Option<SourceIdentifier>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-PORT fund/series row keyed by accession.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNportFundRow {
    /// Exact EDGAR accession.
    pub accession: SourceIdentifier,
    /// Source-reported series name, not an identity bridge.
    pub series_name: Option<String>,
    /// Exact EDGAR series identifier.
    pub series_id: SourceIdentifier,
    /// Source-reported series LEI.
    pub series_lei: Option<SourceIdentifier>,
    /// Exact source-reported total assets.
    pub total_assets: Option<SecExactNumber>,
    /// Exact source-reported total liabilities.
    pub total_liabilities: Option<SecExactNumber>,
    /// Exact source-reported net assets.
    pub net_assets: Option<SecExactNumber>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-PORT identifier evidence keyed by `HOLDING_ID`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNportIdentifierRow {
    /// Exact provider-native holding identifier.
    pub holding_id: SourceIdentifier,
    /// Exact provider-native identifier-row key.
    pub identifiers_id: SourceIdentifier,
    /// Source-reported ISIN.
    pub isin: Option<SourceIdentifier>,
    /// Source-reported ticker retained only as unresolved evidence.
    pub ticker: Option<SourceIdentifier>,
    /// Source-reported other identifier.
    pub other_identifier: Option<String>,
    /// Source-reported type/description of the other identifier.
    pub other_identifier_description: Option<String>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-CEN filing row keyed by accession.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNcenSubmissionRow {
    /// Exact EDGAR accession.
    pub accession: SourceIdentifier,
    /// Exact form, including amendment or notice form.
    pub form: SourceIdentifier,
    /// Exact zero-padded ten-digit registrant CIK.
    pub cik: SourceIdentifier,
    /// Civil filing date from the derived archive.
    pub filing_date: Option<NaiveDate>,
    /// Annual report-ending date.
    pub report_ending_period: Option<NaiveDate>,
    /// Source-reported shorter-than-twelve-month state.
    pub report_period_less_than_twelve_months: Option<bool>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-CEN registrant operational/reference row keyed by accession.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNcenRegistrantRow {
    /// Exact EDGAR accession.
    pub accession: SourceIdentifier,
    /// Exact zero-padded ten-digit registrant CIK.
    pub cik: SourceIdentifier,
    /// Source name retained as evidence, never as a security identity bridge.
    pub registrant_name: Option<String>,
    /// Investment Company Act file number.
    pub file_number: Option<SourceIdentifier>,
    /// Source-reported LEI.
    pub lei: Option<SourceIdentifier>,
    /// Source-reported investment-company type.
    pub investment_company_type: Option<SourceIdentifier>,
    /// Source-reported total series count.
    pub total_series: Option<u64>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-CEN fund/series operational row keyed by accession and `FUND_ID`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNcenFundRow {
    /// Exact compound source key.
    pub fund_id: SourceIdentifier,
    /// Exact EDGAR accession.
    pub accession: SourceIdentifier,
    /// Source-reported fund name retained as evidence.
    pub fund_name: Option<String>,
    /// Exact EDGAR series identifier.
    pub series_id: Option<SourceIdentifier>,
    /// Source-reported series LEI.
    pub lei: Option<SourceIdentifier>,
    /// Source-reported ETF state.
    pub is_etf: Option<bool>,
    /// Source-reported index-fund state.
    pub is_index: Option<bool>,
    /// Source-reported monthly average net assets.
    pub monthly_average_net_assets: Option<SecExactNumber>,
    /// Source-reported daily average net assets.
    pub daily_average_net_assets: Option<SecExactNumber>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-CEN ETF mechanics keyed by exact `FUND_ID` and series.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNcenEtfRow {
    /// Exact compound source key joining to `FUND_REPORTED_INFO`.
    pub fund_id: SourceIdentifier,
    /// Source-reported fund name retained as evidence.
    pub fund_name: Option<String>,
    /// Exact EDGAR series identifier.
    pub series_id: Option<SourceIdentifier>,
    /// Source-reported collateral requirement.
    pub collateral_required: Option<bool>,
    /// Exact shares per creation unit.
    pub shares_per_creation_unit: Option<SecExactNumber>,
    /// Exact shares per redemption unit when reported.
    pub redeemed_shares_per_creation_unit: Option<SecExactNumber>,
    /// Whether the fund reports itself as an in-kind ETF.
    pub is_in_kind_etf: Option<bool>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

/// Provider-native N-CEN exchange/ticker association keyed by exact `FUND_ID`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecNcenSecurityExchangeRow {
    /// Exact compound source key joining to `FUND_REPORTED_INFO`.
    pub fund_id: SourceIdentifier,
    /// Source-reported exchange code.
    pub exchange: Option<SourceIdentifier>,
    /// Source-reported ticker retained only as association evidence.
    pub ticker: Option<SourceIdentifier>,
    /// One-based physical TSV data-row number.
    pub row_number: u64,
    /// Deterministic decoded row identity.
    pub row_evidence: EvidenceDigest,
}

macro_rules! sec_bulk_table_kinds {
    ($($variant:ident => ($family:ident, $member:literal)),+ $(,)?) => {
        /// Closed official SEC quarterly-derived table family.
        #[repr(u16)]
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        pub enum SecBulkTableKind {
            $(
                #[doc = concat!("Official `", $member, "` table.")]
                $variant,
            )+
        }

        impl SecBulkTableKind {
            /// Resolves only an exact current official member under its exact form family.
            pub fn from_member(family: SecBulkFamily, member: &str) -> Result<Self, SecBulkError> {
                match (family, member) {
                    $((SecBulkFamily::$family, $member) => Ok(Self::$variant),)+
                    _ => Err(SecBulkError::InvalidLayout),
                }
            }

            /// Returns the exact official ZIP member name.
            pub const fn member_name(self) -> &'static str {
                match self {
                    $(Self::$variant => $member,)+
                }
            }

            /// Returns the exact form family.
            pub const fn family(self) -> SecBulkFamily {
                match self {
                    $(Self::$variant => SecBulkFamily::$family,)+
                }
            }

            /// Returns the stable closed-table ordinal used by durable native indexes.
            pub const fn ordinal(self) -> u16 {
                self as u16
            }
        }
    };
}

sec_bulk_table_kinds! {
    NportSubmission => (Nport, "SUBMISSION.tsv"),
    NportRegistrant => (Nport, "REGISTRANT.tsv"),
    NportFundReportedInfo => (Nport, "FUND_REPORTED_INFO.tsv"),
    NportInterestRateRisk => (Nport, "INTEREST_RATE_RISK.tsv"),
    NportBorrower => (Nport, "BORROWER.tsv"),
    NportBorrowAggregate => (Nport, "BORROW_AGGREGATE.tsv"),
    NportMonthlyTotalReturn => (Nport, "MONTHLY_TOTAL_RETURN.tsv"),
    NportMonthlyReturnCategoryInstrument => (Nport, "MONTHLY_RETURN_CAT_INSTRUMENT.tsv"),
    NportFundVarInfo => (Nport, "FUND_VAR_INFO.tsv"),
    NportFundReportedHolding => (Nport, "FUND_REPORTED_HOLDING.tsv"),
    NportIdentifiers => (Nport, "IDENTIFIERS.tsv"),
    NportDebtSecurity => (Nport, "DEBT_SECURITY.tsv"),
    NportDebtSecurityReferenceInstrument => (Nport, "DEBT_SECURITY_REF_INSTRUMENT.tsv"),
    NportConvertibleSecurityCurrency => (Nport, "CONVERTIBLE_SECURITY_CURRENCY.tsv"),
    NportRepurchaseAgreement => (Nport, "REPURCHASE_AGREEMENT.tsv"),
    NportRepurchaseCounterparty => (Nport, "REPURCHASE_COUNTERPARTY.tsv"),
    NportRepurchaseCollateral => (Nport, "REPURCHASE_COLLATERAL.tsv"),
    NportDerivativeCounterparty => (Nport, "DERIVATIVE_COUNTERPARTY.tsv"),
    NportSwaptionOptionWarrantDerivative => (Nport, "SWAPTION_OPTION_WARNT_DERIV.tsv"),
    NportDescriptionReferenceIndexBasket => (Nport, "DESC_REF_INDEX_BASKET.tsv"),
    NportDescriptionReferenceIndexComponent => (Nport, "DESC_REF_INDEX_COMPONENT.tsv"),
    NportDescriptionReferenceOther => (Nport, "DESC_REF_OTHER.tsv"),
    NportFutureForwardNonforeignCurrencyContract => (Nport, "FUT_FWD_NONFOREIGNCUR_CONTRACT.tsv"),
    NportForwardForeignCurrencyContractSwap => (Nport, "FWD_FOREIGNCUR_CONTRACT_SWAP.tsv"),
    NportNonforeignExchangeSwap => (Nport, "NONFOREIGN_EXCHANGE_SWAP.tsv"),
    NportFloatingRateResetTenor => (Nport, "FLOATING_RATE_RESET_TENOR.tsv"),
    NportOtherDerivative => (Nport, "OTHER_DERIV.tsv"),
    NportOtherDerivativeNotionalAmount => (Nport, "OTHER_DERIV_NOTIONAL_AMOUNT.tsv"),
    NportSecuritiesLending => (Nport, "SECURITIES_LENDING.tsv"),
    NportExplanatoryNote => (Nport, "EXPLANATORY_NOTE.tsv"),
    NcenSubmission => (Ncen, "SUBMISSION.tsv"),
    NcenRegistrant => (Ncen, "REGISTRANT.tsv"),
    NcenRegistrantWebsite => (Ncen, "REGISTRANT_WEBSITE.tsv"),
    NcenLocationBooksRecord => (Ncen, "LOCATION_BOOKS_RECORD.tsv"),
    NcenTerminatedOrganization => (Ncen, "TERMINATED_ORGANIZATION.tsv"),
    NcenDirector => (Ncen, "DIRECTOR.tsv"),
    NcenDirectorFileNumber => (Ncen, "DIRECTOR_FILE_NUMBER.tsv"),
    NcenChiefComplianceOfficer => (Ncen, "CHIEF_COMPLIANCE_OFFICER.tsv"),
    NcenCcoEmployer => (Ncen, "CCO_EMPLOYER.tsv"),
    NcenRegistrantReportingSeries => (Ncen, "REGISTRANT_REPORTING_SERIES.tsv"),
    NcenReleaseNumber => (Ncen, "RELEASE_NUMBER.tsv"),
    NcenPrincipalUnderwriter => (Ncen, "PRINCIPAL_UNDERWRITER.tsv"),
    NcenPublicAccountant => (Ncen, "PUBLIC_ACCOUNTANT.tsv"),
    NcenValuationMethodChange => (Ncen, "VALUATION_METHOD_CHANGE.tsv"),
    NcenValuationMethodChangeSeries => (Ncen, "VALUATION_METHOD_CHANGE_SERIES.tsv"),
    NcenFundReportedInfo => (Ncen, "FUND_REPORTED_INFO.tsv"),
    NcenSharesOutstanding => (Ncen, "SHARES_OUTSTANDING.tsv"),
    NcenFeederFunds => (Ncen, "FEEDER_FUNDS.tsv"),
    NcenMasterFunds => (Ncen, "MASTER_FUNDS.tsv"),
    NcenForeignInvestment => (Ncen, "FOREIGN_INVESTMENT.tsv"),
    NcenSecurityLending => (Ncen, "SECURITY_LENDING.tsv"),
    NcenSecurityLendingIndemnityProvider => (Ncen, "SEC_LENDING_IDEMNITY_PROVIDER.tsv"),
    NcenCollateralManager => (Ncen, "COLLATERAL_MANAGER.tsv"),
    NcenAdviser => (Ncen, "ADVISER.tsv"),
    NcenTransferAgent => (Ncen, "TRANSFER_AGENT.tsv"),
    NcenPricingService => (Ncen, "PRICING_SERVICE.tsv"),
    NcenCustodian => (Ncen, "CUSTODIAN.tsv"),
    NcenShareholderServicingAgent => (Ncen, "SHAREHOLDER_SERVICING_AGENT.tsv"),
    NcenAdmin => (Ncen, "ADMIN.tsv"),
    NcenBrokerDealer => (Ncen, "BROKER_DEALER.tsv"),
    NcenBroker => (Ncen, "BROKER.tsv"),
    NcenPrincipalTransaction => (Ncen, "PRINCIPAL_TRANSACTION.tsv"),
    NcenLineOfCreditDetail => (Ncen, "LINE_OF_CREDIT_DETAIL.tsv"),
    NcenLineOfCreditInstitution => (Ncen, "LINE_OF_CREDIT_INSTITUTION.tsv"),
    NcenCreditUser => (Ncen, "CREDIT_USER.tsv"),
    NcenInterFundLendingDetail => (Ncen, "INTER_FUND_LENDING_DETAIL.tsv"),
    NcenInterFundBorrowingDetail => (Ncen, "INTER_FUND_BORROWING_DETAIL.tsv"),
    NcenSecurityRelatedItem => (Ncen, "SECURITY_RELATED_ITEM.tsv"),
    NcenRightsOfferingFund => (Ncen, "RIGHTS_OFFERING_FUND.tsv"),
    NcenLongtermDebtDefault => (Ncen, "LONGTERM_DEBT_DEFAULT.tsv"),
    NcenDividendsInArrear => (Ncen, "DIVIDENDS_IN_ARREAR.tsv"),
    NcenSecurityExchange => (Ncen, "SECURITY_EXCHANGE.tsv"),
    NcenAuthorizedParticipant => (Ncen, "AUTHORIZED_PARTICIPANT.tsv"),
    NcenEtf => (Ncen, "ETF.tsv"),
    NcenDepositor => (Ncen, "DEPOSITOR.tsv"),
    NcenUitAdmin => (Ncen, "UIT_ADMIN.tsv"),
    NcenUit => (Ncen, "UIT.tsv"),
    NcenSeriesCik => (Ncen, "SERIES_CIK.tsv"),
    NcenSponsor => (Ncen, "SPONSOR.tsv"),
    NcenTrustee => (Ncen, "TRUSTEE.tsv"),
    NcenContractSecurity => (Ncen, "CONTRACT_SECURITY.tsv"),
    NcenDivestment => (Ncen, "DIVESTMENT.tsv"),
    NcenRegistrantHeldSecurity => (Ncen, "REGISTRANT_HELDS_SECURITY.tsv"),
}

/// Exact archive presence state for one metadata-declared table.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum SecBulkTablePresence {
    /// The table member exists and contains one or more data rows.
    PresentRows {
        /// Exact decoded member evidence.
        evidence: EvidenceDigest,
        /// Exact physical data-row count.
        row_count: u64,
    },
    /// The table member exists with an exact header but no data rows.
    PresentEmpty {
        /// Exact decoded header-only member evidence.
        evidence: EvidenceDigest,
    },
    /// Metadata declares the table, but the SEC omitted the unpopulated member from this archive.
    DeclaredAbsent,
}

impl SecBulkLayoutManifest {
    /// Returns present-with-rows, present-empty, or declared-absent without treating absence as
    /// evidence that no filing/fact exists.
    pub fn table_presence(
        &self,
        table: SecBulkTableKind,
    ) -> Result<SecBulkTablePresence, SecBulkError> {
        if table.family() != self.capture.selection.family {
            return Err(SecBulkError::InvalidLayout);
        }
        if let Some(receipt) = self.table(table.member_name()) {
            return if receipt.row_count == 0 {
                Ok(SecBulkTablePresence::PresentEmpty {
                    evidence: receipt.evidence,
                })
            } else {
                Ok(SecBulkTablePresence::PresentRows {
                    evidence: receipt.evidence,
                    row_count: receipt.row_count,
                })
            };
        }
        if self
            .absent_declared_tables
            .iter()
            .any(|name| name.as_str() == table.member_name())
        {
            Ok(SecBulkTablePresence::DeclaredAbsent)
        } else {
            Err(SecBulkError::InvalidLayout)
        }
    }
}

/// Exact metadata-declared cell value; source strings are never guessed into richer semantics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SecBulkTypedValue {
    /// Metadata-declared nullable value is absent in this row.
    Missing,
    /// Exact SEC string value, including source-coded boolean/enumeration lexicals.
    Text(String),
    /// Validated exact `DD-MON-YYYY` calendar value.
    Date(NaiveDate),
    /// Exact fixed-point/integer SEC NUMBER lexical value.
    Number(SecExactNumber),
}

/// One lossless metadata-ordered typed field.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecBulkTypedField {
    pub(crate) name: SourceIdentifier,
    pub(crate) value: SecBulkTypedValue,
}

impl SecBulkTypedField {
    /// Returns exact SEC column name.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the exact metadata-governed value.
    pub const fn value(&self) -> &SecBulkTypedValue {
        &self.value
    }
}

/// Exact table primary-key component.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecBulkKeyField {
    pub(crate) name: SourceIdentifier,
    pub(crate) value: String,
}

impl SecBulkKeyField {
    /// Constructs one exact metadata primary-key filter component.
    ///
    /// Empty lexical components remain admissible when the exact metadata marks that compound-key
    /// column nullable; the generation-bound table contract decides validity.
    pub fn try_new(name: &str, value: &str) -> Result<Self, SecBulkError> {
        Ok(Self {
            name: SourceIdentifier::try_from(name)?,
            value: value.to_owned(),
        })
    }

    /// Returns exact key-column name.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns exact source lexical key value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Closed cross-table join domain retained from exact provider fields.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum SecBulkJoinDomain {
    /// EDGAR accession joins submission/registrant/fund/holding families.
    Accession,
    /// N-PORT holding identifier joins every C.9-C.12 supplement.
    Holding,
    /// N-CEN compound fund identifier joins fund/ETF/operational families.
    Fund,
    /// Exact EDGAR series identifier.
    Series,
    /// Exact zero-padded registrant CIK.
    RegistrantCik,
    /// Exact fund/share-class identifier retained for return and share-outstanding joins.
    ShareClass,
    /// N-CEN director sequence scoped by accession.
    NcenDirectorSequence,
    /// N-CEN chief-compliance-officer sequence scoped by accession.
    NcenComplianceOfficerSequence,
    /// N-CEN valuation-method-change sequence scoped by accession.
    NcenValuationChangeSequence,
    /// N-CEN securities-lending relationship sequence scoped by fund.
    NcenSecurityLendingSequence,
    /// N-CEN line-of-credit sequence scoped by fund.
    NcenLineOfCreditSequence,
}

/// One exact metadata-declared join coordinate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecBulkJoinCoordinate {
    pub(crate) domain: SecBulkJoinDomain,
    pub(crate) column: SourceIdentifier,
    pub(crate) value: String,
}

impl SecBulkJoinCoordinate {
    /// Returns the closed join domain.
    pub const fn domain(&self) -> SecBulkJoinDomain {
        self.domain
    }

    /// Returns exact provider column.
    pub const fn column(&self) -> &SourceIdentifier {
        &self.column
    }

    /// Returns exact source lexical join key.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Rich provider-native projection available for currently materialized SEC table shapes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SecBulkProviderProjection {
    /// N-PORT filing metadata.
    NportSubmission(Box<SecNportSubmissionRow>),
    /// N-PORT registrant metadata.
    NportRegistrant(Box<SecNportRegistrantRow>),
    /// N-PORT fund metadata.
    NportFund(Box<SecNportFundRow>),
    /// N-PORT holding core.
    NportHolding(Box<SecNportHoldingRow>),
    /// N-PORT identifier row.
    NportIdentifier(Box<SecNportIdentifierRow>),
    /// N-CEN filing metadata.
    NcenSubmission(Box<SecNcenSubmissionRow>),
    /// N-CEN registrant metadata.
    NcenRegistrant(Box<SecNcenRegistrantRow>),
    /// N-CEN fund metadata.
    NcenFund(Box<SecNcenFundRow>),
    /// N-CEN ETF terms.
    NcenEtf(Box<SecNcenEtfRow>),
    /// N-CEN exchange association.
    NcenSecurityExchange(Box<SecNcenSecurityExchangeRow>),
}

/// Closed reason why one lossless provider row does or does not have a richer projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SecBulkProjectionDisposition {
    /// The source row was projected into a richer provider-native SEC table shape.
    Projected(SecBulkProviderProjection),
    /// This official table has no richer projection contract.
    NotApplicable,
    /// A source field required by the richer projection is absent.
    SourceMissing,
    /// A present source field is malformed for the provider-native projection contract.
    InvalidSource,
    /// Source evidence exists, but its identity cannot be resolved under the admitted contract.
    UnresolvedIdentity,
}

/// One lossless provider-native row from any current official N-PORT/N-CEN table.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecBulkNativeRow {
    pub(crate) table: SecBulkTableKind,
    pub(crate) primary_key: Vec<SecBulkKeyField>,
    pub(crate) joins: Vec<SecBulkJoinCoordinate>,
    pub(crate) fields: Vec<SecBulkTypedField>,
    pub(crate) projection_disposition: SecBulkProjectionDisposition,
    pub(crate) row_number: u64,
    pub(crate) row_evidence: EvidenceDigest,
}

impl SecBulkNativeRow {
    /// Returns the closed official table identity.
    pub const fn table(&self) -> SecBulkTableKind {
        self.table
    }

    /// Returns exact metadata-declared primary-key fields in declared order.
    pub fn primary_key(&self) -> &[SecBulkKeyField] {
        &self.primary_key
    }

    /// Returns exact recognized cross-table coordinates.
    pub fn joins(&self) -> &[SecBulkJoinCoordinate] {
        &self.joins
    }

    /// Returns every exact field in metadata/header order.
    pub fn fields(&self) -> &[SecBulkTypedField] {
        &self.fields
    }

    /// Returns the closed provider-native projection outcome without discarding source evidence.
    pub const fn projection_disposition(&self) -> &SecBulkProjectionDisposition {
        &self.projection_disposition
    }

    /// Returns one-based physical data-row number.
    pub const fn row_number(&self) -> u64 {
        self.row_number
    }

    /// Returns deterministic decoded row evidence.
    pub const fn row_evidence(&self) -> EvidenceDigest {
        self.row_evidence
    }
}

/// Provider fields admitted as direct authoritative identity inputs.
///
/// Ticker, issuer/fund name, exchange, and arbitrary `OTHER_IDENTIFIER_DESC` namespaces are
/// deliberately absent and therefore cannot be promoted into canonical identity authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub enum SecAuthoritativeIdentifierNamespace {
    /// Exact EDGAR fund series identifier (`SERIES_ID`).
    SecSeriesId,
    /// Exact ISIN from `IDENTIFIERS.tsv`.
    Isin,
    /// Exact CUSIP from the N-PORT holding row.
    Cusip,
}

/// Closed authoritative identifier receipt consumed by SEC identity resolution.
///
/// Ticker, issuer name, and display text have no construction fields. The receipt binds one exact
/// provider identifier to a separately versioned authoritative crosswalk and conservative clocks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecGovernedIdentityReceipt {
    authority_source_id: SourceId,
    authority_revision: SourceIdentifier,
    identifier_namespace: SecAuthoritativeIdentifierNamespace,
    authoritative_identifier: SourceIdentifier,
    instrument_id: InstrumentId,
    evidence: ExactPayloadEvidence,
    available_at: Timestamp,
    observed_at: Timestamp,
}

impl SecGovernedIdentityReceipt {
    /// Resolves one exact non-name, non-ticker identifier through the governed registry at a
    /// historical cutoff. Conflicted, unavailable, or hindsight-only mappings abstain.
    pub fn resolve_exact(
        registry: &ProviderIdentityRegistry,
        authority_source_id: &SourceId,
        identifier_namespace: SecAuthoritativeIdentifierNamespace,
        authoritative_identifier: SourceIdentifier,
        cutoff: Timestamp,
    ) -> Result<Self, SecBulkError> {
        let provider_instrument_id =
            ProviderInstrumentId::try_from(authoritative_identifier.as_str())?;
        let record = registry
            .provider_identity_at(authority_source_id, &provider_instrument_id, cutoff)
            .ok_or(SecBulkError::UnresolvedIdentity)?;
        let receipt =
            Self::from_registry_record(identifier_namespace, authoritative_identifier, record)?;
        if receipt.available_at > cutoff || receipt.observed_at > cutoff {
            return Err(SecBulkError::UnresolvedIdentity);
        }
        Ok(receipt)
    }

    /// Mints a receipt only from a conflict-free mapping selected by the checked identity registry.
    pub(crate) fn from_registry_record(
        identifier_namespace: SecAuthoritativeIdentifierNamespace,
        authoritative_identifier: SourceIdentifier,
        record: &ProviderIdentityRecord,
    ) -> Result<Self, SecBulkError> {
        let identifier_is_exact = match identifier_namespace {
            SecAuthoritativeIdentifierNamespace::SecSeriesId => {
                authoritative_identifier.as_str().len() == 10
                    && authoritative_identifier.as_str().starts_with('S')
                    && authoritative_identifier.as_str()[1..]
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
            }
            SecAuthoritativeIdentifierNamespace::Isin => {
                authoritative_identifier.as_str().len() == 12
                    && authoritative_identifier
                        .as_str()
                        .bytes()
                        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
            }
            SecAuthoritativeIdentifierNamespace::Cusip => {
                authoritative_identifier.as_str().len() == 9
                    && authoritative_identifier.as_str().bytes().all(|byte| {
                        byte.is_ascii_uppercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'*' | b'@' | b'#')
                    })
            }
        };
        if !identifier_is_exact
            || record.provider_instrument_id().as_str() != authoritative_identifier.as_str()
        {
            return Err(SecBulkError::UnresolvedIdentity);
        }
        let evidence =
            ExactPayloadEvidence::from_content_digest(record.evidence().content_digest());
        let digest = evidence.content_digest();
        let observed_at = record.observed_at();
        let available_at = record.source_timestamp().unwrap_or(observed_at);
        if digest.algorithm() != DigestAlgorithm::Sha256
            || digest.bytes().iter().all(|byte| *byte == 0)
            || available_at > observed_at
        {
            return Err(SecBulkError::UnresolvedIdentity);
        }
        Ok(Self {
            authority_source_id: record.source_id().clone(),
            authority_revision: record.metadata_revision().as_source_identifier().clone(),
            identifier_namespace,
            authoritative_identifier,
            instrument_id: record.instrument_id(),
            evidence,
            available_at,
            observed_at,
        })
    }

    /// Returns the authority namespace responsible for the crosswalk.
    pub const fn authority_source_id(&self) -> &SourceId {
        &self.authority_source_id
    }

    /// Returns the exact immutable authority revision.
    pub const fn authority_revision(&self) -> &SourceIdentifier {
        &self.authority_revision
    }

    /// Returns the closed authoritative identifier namespace.
    pub const fn identifier_namespace(&self) -> SecAuthoritativeIdentifierNamespace {
        self.identifier_namespace
    }

    /// Returns the exact provider identifier resolved by the authority.
    pub const fn authoritative_identifier(&self) -> &SourceIdentifier {
        &self.authoritative_identifier
    }

    /// Returns the exact internal identity asserted by the selected registry record.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns exact crosswalk payload evidence.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns proven historical availability of the authority revision.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the first local observation of the selected registry assertion.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}

/// Provider-local readiness state for one exact SEC bulk workflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecBulkDoctorState {
    /// Locator, request bounds, capture, layout, and coverage checks passed.
    Ready,
    /// Application metadata does not authorize the required archive size/path.
    RequestBoundsInsufficient,
    /// Exact raw evidence exists, but layout inspection or recovery is invalid.
    InvalidEvidence,
    /// Full-filing reconciliation is mandatory before completeness can be stated.
    ReadyWithDeclaredCoverageGap,
}

/// Secret-free doctor evidence for activation of one exact family/quarter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkDoctorReport {
    selection: SecBulkSelection,
    state: SecBulkDoctorState,
    observed_at: Timestamp,
    manifest_evidence: Option<EvidenceDigest>,
    archive_capture: Option<SecBulkCapture>,
    official_readme_capture: Option<SecBulkCapture>,
}

impl SecBulkDoctorReport {
    /// Constructs a bounded secret-free report from a fresh provider-local recovery result.
    pub(crate) fn new(
        selection: SecBulkSelection,
        state: SecBulkDoctorState,
        observed_at: Timestamp,
        recovered_manifest: Option<&SecBulkLayoutManifest>,
    ) -> Self {
        Self {
            selection,
            state,
            observed_at,
            manifest_evidence: recovered_manifest.map(SecBulkLayoutManifest::evidence),
            archive_capture: recovered_manifest.map(|manifest| manifest.capture().clone()),
            official_readme_capture: recovered_manifest
                .map(|manifest| manifest.official_readme_capture().clone()),
        }
    }

    /// Returns the exact checked selection.
    pub const fn selection(&self) -> &SecBulkSelection {
        &self.selection
    }

    /// Returns readiness without erasing an N-CEN coverage gap.
    pub const fn state(&self) -> SecBulkDoctorState {
        self.state
    }

    /// Returns the trusted doctor clock.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns exact inspected layout evidence when available.
    pub const fn manifest_evidence(&self) -> Option<EvidenceDigest> {
        self.manifest_evidence
    }

    /// Returns the freshly reverified archive transport/revision receipt when recovery succeeded.
    pub const fn archive_capture(&self) -> Option<&SecBulkCapture> {
        self.archive_capture.as_ref()
    }

    /// Returns the freshly reverified official-readme transport/revision receipt.
    pub const fn official_readme_capture(&self) -> Option<&SecBulkCapture> {
        self.official_readme_capture.as_ref()
    }
}

/// Provider-local evidence eligible for root-owned activation publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecBulkActivationEvidence {
    report: SecBulkDoctorReport,
}

impl SecBulkActivationEvidence {
    /// Admits only technically ready doctor reports with exact capture evidence.
    pub fn try_new(report: SecBulkDoctorReport) -> Result<Self, SecBulkError> {
        let ready_state_matches_coverage = matches!(
            (&report.state, report.selection.coverage()),
            (
                SecBulkDoctorState::Ready,
                SecBulkCoverage::DerivedAsFiledIncludingAmendments
            ) | (
                SecBulkDoctorState::ReadyWithDeclaredCoverageGap,
                SecBulkCoverage::AcceptedSchemaExcluded { .. }
            )
        );
        if !ready_state_matches_coverage
            || report.manifest_evidence.is_none_or(|evidence| {
                evidence.algorithm() != DigestAlgorithm::Sha256
                    || evidence.bytes().iter().all(|byte| *byte == 0)
            })
            || report.archive_capture.as_ref().is_none_or(|capture| {
                capture.selection() != &report.selection
                    || capture.transport().media_kind() != SecBulkMediaKind::Zip
                    || capture.first_observed_at() > report.observed_at
            })
            || report
                .official_readme_capture
                .as_ref()
                .is_none_or(|capture| {
                    capture.selection() != &report.selection
                        || capture.transport().media_kind() != SecBulkMediaKind::Pdf
                        || capture.first_observed_at() > report.observed_at
                })
        {
            return Err(SecBulkError::ActivationNotReady);
        }
        Ok(Self { report })
    }

    /// Returns the exact doctor evidence.
    pub const fn report(&self) -> &SecBulkDoctorReport {
        &self.report
    }
}

fn layout_digest(
    capture: &SecBulkCapture,
    official_readme_capture: &SecBulkCapture,
    metadata: EvidenceDigest,
    readme: EvidenceDigest,
    declared_tables: &[SecBulkDeclaredTableContract],
    tables: &[SecBulkTableReceipt],
    absent_declared_tables: &[SourceIdentifier],
    expanded_bytes: u64,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-layout/v3");
    hash_field(&mut digest, capture.selection.family.tag().as_bytes());
    hash_field(&mut digest, &capture.selection.quarter.year.to_be_bytes());
    hash_field(&mut digest, &[capture.selection.quarter.quarter]);
    hash_field(
        &mut digest,
        capture
            .selection
            .accepted_schema
            .version
            .as_str()
            .as_bytes(),
    );
    hash_field(
        &mut digest,
        capture
            .selection
            .accepted_schema
            .effective_date
            .to_string()
            .as_bytes(),
    );
    hash_field(
        &mut digest,
        capture
            .selection
            .accepted_schema
            .technical_spec_locator
            .as_str()
            .as_bytes(),
    );
    hash_field(
        &mut digest,
        match &capture.selection.coverage {
            SecBulkCoverage::DerivedAsFiledIncludingAmendments => b"derived-including-amendments",
            SecBulkCoverage::AcceptedSchemaExcluded { .. } => b"accepted-schema-excluded",
        },
    );
    hash_field(
        &mut digest,
        capture
            .selection
            .catalog_snapshot
            .audited_at
            .to_string()
            .as_bytes(),
    );
    hash_field(&mut digest, &capture.evidence.bytes());
    hash_field(&mut digest, capture.locator.as_str().as_bytes());
    hash_field(&mut digest, &capture.size_bytes.to_be_bytes());
    hash_field(
        &mut digest,
        &capture.first_observed_at.unix_nanos().to_be_bytes(),
    );
    hash_field(&mut digest, &capture.retrieval_revision.to_be_bytes());
    hash_transport(&mut digest, &capture.transport);
    hash_field(
        &mut digest,
        official_readme_capture.locator.as_str().as_bytes(),
    );
    hash_field(&mut digest, &official_readme_capture.evidence.bytes());
    hash_field(
        &mut digest,
        &official_readme_capture.size_bytes.to_be_bytes(),
    );
    hash_field(
        &mut digest,
        &official_readme_capture
            .first_observed_at
            .unix_nanos()
            .to_be_bytes(),
    );
    hash_field(
        &mut digest,
        &official_readme_capture.retrieval_revision.to_be_bytes(),
    );
    hash_transport(&mut digest, &official_readme_capture.transport);
    hash_field(&mut digest, &metadata.bytes());
    hash_field(&mut digest, &readme.bytes());
    hash_field(&mut digest, &expanded_bytes.to_be_bytes());
    for table in declared_tables {
        hash_field(&mut digest, b"metadata-declared-table");
        hash_field(&mut digest, table.name.as_str().as_bytes());
        for key in &table.primary_key {
            hash_field(&mut digest, key.as_str().as_bytes());
        }
        for column in &table.columns {
            hash_column_contract(&mut digest, column);
        }
    }
    for table in tables {
        hash_field(&mut digest, table.name.as_str().as_bytes());
        hash_field(&mut digest, &table.evidence.bytes());
        hash_field(&mut digest, &table.decoded_bytes.to_be_bytes());
        hash_field(&mut digest, &table.row_count.to_be_bytes());
        for key in &table.primary_key {
            hash_field(&mut digest, key.as_str().as_bytes());
        }
        for column in &table.columns {
            hash_column_contract(&mut digest, column);
        }
    }
    for table in absent_declared_tables {
        hash_field(&mut digest, b"absent-declared-table");
        hash_field(&mut digest, table.as_str().as_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn hash_column_contract(digest: &mut Sha256, column: &SecBulkColumnContract) {
    hash_field(digest, column.name.as_str().as_bytes());
    hash_field(digest, column.datatype_base.as_bytes());
    hash_field(digest, &column.max_length.unwrap_or_default().to_be_bytes());
    hash_numeric_attribute(digest, column.data_precision);
    hash_numeric_attribute(digest, column.data_scale);
    hash_field(digest, &[u8::from(column.required)]);
}

fn hash_transport(digest: &mut Sha256, transport: &SecBulkTransportEvidence) {
    hash_field(digest, &transport.http_status.to_be_bytes());
    hash_field(
        digest,
        match transport.media_kind {
            SecBulkMediaKind::Zip => b"zip",
            SecBulkMediaKind::Pdf => b"pdf",
        },
    );
    hash_field(
        digest,
        transport.media_type.as_deref().unwrap_or("").as_bytes(),
    );
    hash_field(digest, transport.validators.etag().unwrap_or("").as_bytes());
    hash_field(
        digest,
        transport
            .validators
            .last_modified()
            .unwrap_or("")
            .as_bytes(),
    );
    hash_field(
        digest,
        &transport.body_received_at.unix_nanos().to_be_bytes(),
    );
}

fn admitted_media_type(kind: SecBulkMediaKind, value: &str) -> bool {
    let essence = value.split_once(';').map_or(value, |(essence, _)| essence);
    match kind {
        SecBulkMediaKind::Zip => matches!(
            essence,
            "application/zip" | "application/x-zip-compressed" | "application/octet-stream"
        ),
        SecBulkMediaKind::Pdf => {
            matches!(essence, "application/pdf" | "application/octet-stream")
        }
    }
}

fn parse_http_timestamp(value: &str) -> Result<Timestamp, SecBulkError> {
    let time = httpdate::parse_http_date(value).map_err(|_| SecBulkError::InvalidCapture)?;
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| SecBulkError::InvalidCapture)?;
    let nanos = i64::try_from(duration.as_nanos()).map_err(|_| SecBulkError::InvalidCapture)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn hash_numeric_attribute(digest: &mut Sha256, attribute: Option<SecBulkNumericAttribute>) {
    match attribute {
        None => hash_field(digest, b"none"),
        Some(SecBulkNumericAttribute::ProviderNull) => hash_field(digest, b"provider-null"),
        Some(SecBulkNumericAttribute::Value(value)) => {
            hash_field(digest, b"value");
            hash_field(digest, &value.to_be_bytes());
        }
    }
}
