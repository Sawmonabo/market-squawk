//! Evidence-bound company identity that remains separate from tradable instruments.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    AvailabilityEvidence, DataQuality, ExactPayloadEvidence, SchemaVersion, SchemaVersionError,
    SourceId, SourceIdentifier, Timestamp,
};

/// Maximum former-name observations retained from one provider representation.
pub const MAX_COMPANY_FORMER_NAMES: usize = 64;
/// Maximum provider-reported ticker/exchange associations retained per observation.
pub const MAX_COMPANY_SECURITY_ASSOCIATIONS: usize = 64;

const MAX_COMPANY_NAME_BYTES: usize = 512;
const MAX_ENTITY_TYPE_BYTES: usize = 128;
const MAX_SIC_BYTES: usize = 32;
const MAX_SIC_DESCRIPTION_BYTES: usize = 2_048;
const MAX_TICKER_BYTES: usize = 128;
const MAX_EXCHANGE_BYTES: usize = 128;

/// Provider surface that authored one company identity observation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompanyIdentitySurface {
    /// SEC submissions company metadata.
    SecSubmissions,
    /// SEC Company Facts entity metadata.
    SecCompanyFacts,
}

impl CompanyIdentitySurface {
    /// Returns the stable catalog representation.
    pub const fn database_name(self) -> &'static str {
        match self {
            Self::SecSubmissions => "sec_submissions",
            Self::SecCompanyFacts => "sec_company_facts",
        }
    }
}

/// One source-reported former company name and its source time interval.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FormerCompanyName {
    name: String,
    valid_from: Timestamp,
    valid_until: Timestamp,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FormerCompanyNameWire {
    name: String,
    valid_from: Timestamp,
    valid_until: Timestamp,
}

impl<'de> Deserialize<'de> for FormerCompanyName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FormerCompanyNameWire::deserialize(deserializer)?;
        Self::try_new(wire.name, wire.valid_from, wire.valid_until)
            .map_err(serde::de::Error::custom)
    }
}

impl FormerCompanyName {
    /// Constructs a bounded former-name interval.
    pub fn try_new(
        name: impl Into<String>,
        valid_from: Timestamp,
        valid_until: Timestamp,
    ) -> Result<Self, CompanyIdentityError> {
        let name = name.into();
        validate_text(&name, MAX_COMPANY_NAME_BYTES)?;
        if valid_until < valid_from {
            return Err(CompanyIdentityError::InvalidTime);
        }
        Ok(Self {
            name,
            valid_from,
            valid_until,
        })
    }

    /// Returns the exact provider-reported former name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the beginning of the provider-reported interval.
    pub const fn valid_from(&self) -> Timestamp {
        self.valid_from
    }

    /// Returns the end of the provider-reported interval.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }
}

/// One provider-reported ticker/exchange association.
///
/// This is research reference metadata. It is not a verified security assignment, venue mapping,
/// execution-quality assertion, or authority to construct a tradable instrument.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReportedSecurityAssociation {
    ticker: String,
    exchange: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderReportedSecurityAssociationWire {
    ticker: String,
    exchange: String,
}

impl<'de> Deserialize<'de> for ProviderReportedSecurityAssociation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderReportedSecurityAssociationWire::deserialize(deserializer)?;
        Self::try_new(wire.ticker, wire.exchange).map_err(serde::de::Error::custom)
    }
}

impl ProviderReportedSecurityAssociation {
    /// Constructs a bounded provider assertion without promoting it to canonical market identity.
    pub fn try_new(
        ticker: impl Into<String>,
        exchange: impl Into<String>,
    ) -> Result<Self, CompanyIdentityError> {
        let ticker = ticker.into();
        let exchange = exchange.into();
        validate_text(&ticker, MAX_TICKER_BYTES)?;
        validate_text(&exchange, MAX_EXCHANGE_BYTES)?;
        Ok(Self { ticker, exchange })
    }

    /// Returns the exact provider-reported ticker.
    pub fn ticker(&self) -> &str {
        &self.ticker
    }

    /// Returns the exact provider-reported exchange label.
    pub fn exchange(&self) -> &str {
        &self.exchange
    }
}

/// Complete input for one invariant-preserving company identity observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompanyIdentityObservationInput {
    /// Canonical domain schema version.
    pub schema_version: SchemaVersion,
    /// Source that supplied the exact representation.
    pub source_id: SourceId,
    /// Provider company identity, such as an SEC CIK.
    pub provider_company_id: SourceIdentifier,
    /// Provider surface that authored this observation.
    pub surface: CompanyIdentitySurface,
    /// Current provider-conformed company name.
    pub conformed_name: String,
    /// Bounded source-reported former-name intervals.
    #[serde(default)]
    pub former_names: Vec<FormerCompanyName>,
    /// Optional provider entity classification.
    #[serde(default)]
    pub entity_type: Option<String>,
    /// Optional provider industry code.
    #[serde(default)]
    pub sic: Option<String>,
    /// Optional provider industry description.
    #[serde(default)]
    pub sic_description: Option<String>,
    /// Provider-reported ticker/exchange associations without execution authority.
    #[serde(default)]
    pub associations: Vec<ProviderReportedSecurityAssociation>,
    /// Exact evidence owning the associated research ingest.
    pub parent_ingest_payload_evidence: ExactPayloadEvidence,
    /// Exact representation from which identity fields were parsed.
    pub identity_payload_evidence: ExactPayloadEvidence,
    /// Local receipt time for the identity representation.
    pub received_at: Timestamp,
    /// Explicit source-availability evidence.
    pub availability: AvailabilityEvidence,
    /// Local normalization time.
    pub ingested_at: Timestamp,
    /// Research data-quality classification.
    pub quality: DataQuality,
}

/// Immutable, evidence-bound company identity distinct from canonical instrument identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompanyIdentityObservation {
    schema_version: SchemaVersion,
    source_id: SourceId,
    provider_company_id: SourceIdentifier,
    surface: CompanyIdentitySurface,
    conformed_name: String,
    former_names: Box<[FormerCompanyName]>,
    entity_type: Option<String>,
    sic: Option<String>,
    sic_description: Option<String>,
    associations: Box<[ProviderReportedSecurityAssociation]>,
    parent_ingest_payload_evidence: ExactPayloadEvidence,
    identity_payload_evidence: ExactPayloadEvidence,
    received_at: Timestamp,
    availability: AvailabilityEvidence,
    ingested_at: Timestamp,
    quality: DataQuality,
}

impl CompanyIdentityObservation {
    /// Validates, canonicalizes, and binds one source-authored company identity observation.
    pub fn try_new(
        mut input: CompanyIdentityObservationInput,
    ) -> Result<Self, CompanyIdentityError> {
        input.schema_version.ensure_supported()?;
        validate_text(&input.conformed_name, MAX_COMPANY_NAME_BYTES)?;
        validate_optional_text(&input.entity_type, MAX_ENTITY_TYPE_BYTES)?;
        validate_optional_text(&input.sic, MAX_SIC_BYTES)?;
        validate_optional_text(&input.sic_description, MAX_SIC_DESCRIPTION_BYTES)?;
        if input.former_names.len() > MAX_COMPANY_FORMER_NAMES
            || input.associations.len() > MAX_COMPANY_SECURITY_ASSOCIATIONS
        {
            return Err(CompanyIdentityError::CollectionLimitExceeded);
        }
        input.former_names.sort_by(|left, right| {
            left.valid_from
                .cmp(&right.valid_from)
                .then_with(|| left.valid_until.cmp(&right.valid_until))
                .then_with(|| left.name.cmp(&right.name))
        });
        input.associations.sort_by(|left, right| {
            left.ticker
                .cmp(&right.ticker)
                .then_with(|| left.exchange.cmp(&right.exchange))
        });
        if input.former_names.windows(2).any(|pair| pair[0] == pair[1])
            || input.associations.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(CompanyIdentityError::DuplicateEntry);
        }
        if input.received_at > input.ingested_at
            || input
                .availability
                .reported_at()
                .is_some_and(|available_at| available_at > input.ingested_at)
            || input.quality == DataQuality::DirectVerified
        {
            return Err(if input.quality == DataQuality::DirectVerified {
                CompanyIdentityError::ExecutionQualityForbidden
            } else {
                CompanyIdentityError::InvalidTime
            });
        }
        Ok(Self {
            schema_version: input.schema_version,
            source_id: input.source_id,
            provider_company_id: input.provider_company_id,
            surface: input.surface,
            conformed_name: input.conformed_name,
            former_names: input.former_names.into_boxed_slice(),
            entity_type: input.entity_type,
            sic: input.sic,
            sic_description: input.sic_description,
            associations: input.associations.into_boxed_slice(),
            parent_ingest_payload_evidence: input.parent_ingest_payload_evidence,
            identity_payload_evidence: input.identity_payload_evidence,
            received_at: input.received_at,
            availability: input.availability,
            ingested_at: input.ingested_at,
            quality: input.quality,
        })
    }

    /// Returns whether two observations describe the same exact source representation.
    ///
    /// The local ingestion attempt time is deliberately excluded so an exact restart retry can
    /// retain the first durable observation rather than fabricating a conflict or rewriting it.
    pub fn same_source_representation(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.source_id == other.source_id
            && self.provider_company_id == other.provider_company_id
            && self.surface == other.surface
            && self.conformed_name == other.conformed_name
            && self.former_names == other.former_names
            && self.entity_type == other.entity_type
            && self.sic == other.sic
            && self.sic_description == other.sic_description
            && self.associations == other.associations
            && self.parent_ingest_payload_evidence == other.parent_ingest_payload_evidence
            && self.identity_payload_evidence == other.identity_payload_evidence
            && self.received_at == other.received_at
            && self.availability == other.availability
            && self.quality == other.quality
    }

    /// Returns the canonical schema version.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }
    /// Returns the source that supplied the observation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns the provider's company identity.
    pub const fn provider_company_id(&self) -> &SourceIdentifier {
        &self.provider_company_id
    }
    /// Returns the provider surface that authored the observation.
    pub const fn surface(&self) -> CompanyIdentitySurface {
        self.surface
    }
    /// Returns the current provider-conformed name.
    pub fn conformed_name(&self) -> &str {
        &self.conformed_name
    }
    /// Returns bounded source-reported former names.
    pub fn former_names(&self) -> &[FormerCompanyName] {
        &self.former_names
    }
    /// Returns the optional provider entity classification.
    pub fn entity_type(&self) -> Option<&str> {
        self.entity_type.as_deref()
    }
    /// Returns the optional provider industry code.
    pub fn sic(&self) -> Option<&str> {
        self.sic.as_deref()
    }
    /// Returns the optional provider industry description.
    pub fn sic_description(&self) -> Option<&str> {
        self.sic_description.as_deref()
    }
    /// Returns provider-reported, unverified security associations.
    pub fn associations(&self) -> &[ProviderReportedSecurityAssociation] {
        &self.associations
    }
    /// Returns the exact evidence owning the corresponding research ingest.
    pub const fn parent_ingest_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.parent_ingest_payload_evidence
    }
    /// Returns the exact representation from which identity fields were parsed.
    pub const fn identity_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.identity_payload_evidence
    }
    /// Returns the local receipt time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns explicit provider-availability evidence.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }
    /// Returns the local normalization time.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
    /// Returns the research data-quality classification.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }
}

impl<'de> Deserialize<'de> for CompanyIdentityObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CompanyIdentityObservationInput::deserialize(deserializer)
            .and_then(|input| Self::try_new(input).map_err(serde::de::Error::custom))
    }
}

/// Company identity construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanyIdentityError {
    /// A string was empty, untrimmed, oversized, or contained a control character.
    InvalidText,
    /// A bounded collection exceeded its hard ceiling.
    CollectionLimitExceeded,
    /// A source representation repeated an exact collection entry.
    DuplicateEntry,
    /// Source, availability, receipt, or ingestion times were inconsistent.
    InvalidTime,
    /// Research company metadata attempted to claim direct execution quality.
    ExecutionQualityForbidden,
    /// The canonical schema version is unsupported.
    Schema(SchemaVersionError),
}

impl fmt::Display for CompanyIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidText => formatter.write_str(
                "company identity text is empty, untrimmed, oversized, or contains a control character",
            ),
            Self::CollectionLimitExceeded => {
                formatter.write_str("company identity collection exceeds its hard bound")
            }
            Self::DuplicateEntry => formatter
                .write_str("company identity collection contains a duplicate entry"),
            Self::InvalidTime => {
                formatter.write_str("company identity time coordinates are inconsistent")
            }
            Self::ExecutionQualityForbidden => formatter
                .write_str("company identity cannot claim direct-verified execution quality"),
            Self::Schema(error) => write!(
                formatter,
                "company identity schema version is unsupported: {error}"
            ),
        }
    }
}

impl std::error::Error for CompanyIdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SchemaVersionError> for CompanyIdentityError {
    fn from(value: SchemaVersionError) -> Self {
        Self::Schema(value)
    }
}

fn validate_optional_text(value: &Option<String>, max: usize) -> Result<(), CompanyIdentityError> {
    value
        .as_deref()
        .map(|value| validate_text(value, max))
        .transpose()
        .map(|_| ())
}

fn validate_text(value: &str, max: usize) -> Result<(), CompanyIdentityError> {
    if value.is_empty()
        || value.len() > max
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        Err(CompanyIdentityError::InvalidText)
    } else {
        Ok(())
    }
}
