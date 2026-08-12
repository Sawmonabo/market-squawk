//! Closed normalization seam into the existing canonical macro observation family.

use std::num::NonZeroU16;

use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, MacroMissingValue, MacroObservation,
    PayloadHash, PayloadReference, ResearchContext, ResearchPeriod, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber, SourceId,
    SourceIdentifier, Timestamp,
};

use crate::types::digest_bytes;
use crate::{EiaDigest, EiaError, EiaNativeValue, EiaObservation, EiaPeriodKind};

/// Durable-authority coordinates supplied only after capture/ingest admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaCanonicalContext {
    source_id: SourceId,
    ingested_at: Timestamp,
    revision: RevisionNumber,
    superseded_at: Option<Timestamp>,
}

impl EiaCanonicalContext {
    /// Constructs canonicalization context. The adapter cannot allocate a durable revision itself;
    /// the shared revision authority supplies it here.
    pub const fn new(
        source_id: SourceId,
        ingested_at: Timestamp,
        revision: RevisionNumber,
        superseded_at: Option<Timestamp>,
    ) -> Self {
        Self {
            source_id,
            ingested_at,
            revision,
            superseded_at,
        }
    }
}

/// Canonical macro observation plus native evidence identities that downstream publication must
/// retain in its raw/native lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaCanonicalObservation {
    observation: MacroObservation,
    native_row_digest: EiaDigest,
    native_schema_digest: EiaDigest,
    series_digest: EiaDigest,
    raw_page_digest: EiaDigest,
}

impl EiaCanonicalObservation {
    /// Normalizes exact decimal or explicit missing evidence. Provider string-valued data stays in
    /// the native contract and fails closed rather than entering a numeric macro series.
    pub fn try_from_native(
        native: &EiaObservation,
        context: EiaCanonicalContext,
    ) -> Result<Self, EiaError> {
        let clocks = native.clocks();
        if clocks.received_at() > context.ingested_at
            || clocks
                .available_at()
                .is_some_and(|available| available > context.ingested_at)
        {
            return Err(EiaError::InvalidClock);
        }
        let series = source_identifier_from_digest("eia-series", native.series().digest())?;
        let unit_digest = digest_bytes(native.series().unit().as_bytes());
        let unit = source_identifier_from_digest("eia-unit", unit_digest)?;
        let source_identifier = SourceIdentifier::try_from(format!(
            "eia-row:{}:{}",
            lower_hex(native.series().digest().bytes()),
            lower_hex(native.row_digest().bytes())
        ))
        .map_err(|_| EiaError::Canonicalization)?;
        let availability = match clocks.available_at() {
            Some(available_at) => AvailabilityEvidence::evidenced(
                available_at,
                source_identifier_from_digest("eia-provider-availability", native.row_digest())?,
            ),
            None => AvailabilityEvidence::local_first_observed(clocks.received_at()),
        };
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: context.source_id,
            instrument_id: None,
            venue_id: None,
            source_identifier,
            source_timestamp: clocks.updated_at().or(clocks.released_at()),
            received_at: clocks.received_at(),
            ingested_at: context.ingested_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                DigestAlgorithm::Sha256,
                native.page_payload_digest().bytes(),
            )),
            availability,
        })
        .map_err(|_| EiaError::Canonicalization)?;
        let effective = canonical_effective(native)?;
        let published = clocks.released_at().map(ResearchTemporalCoordinate::exact);
        let superseded = context.superseded_at.map(ResearchTemporalCoordinate::exact);
        let time = ResearchTime::try_new_with_coordinates(
            effective,
            published,
            context.revision,
            superseded,
        )
        .map_err(|_| EiaError::Canonicalization)?;
        let research_context =
            ResearchContext::new(provenance, time).map_err(|_| EiaError::Canonicalization)?;
        let observation = match native.value() {
            EiaNativeValue::Decimal { value, .. } => {
                MacroObservation::new(research_context, series, *value, unit)
            }
            EiaNativeValue::Missing(missing) => MacroObservation::missing(
                research_context,
                series,
                MacroMissingValue::new(
                    SourceIdentifier::try_from(missing.lexical().unwrap_or("json-null"))
                        .map_err(|_| EiaError::Canonicalization)?,
                    None,
                ),
                unit,
            ),
            EiaNativeValue::String(_) => return Err(EiaError::Canonicalization),
        };
        Ok(Self {
            observation,
            native_row_digest: native.row_digest(),
            native_schema_digest: native.row_schema_digest(),
            series_digest: native.series().digest(),
            raw_page_digest: native.page_payload_digest(),
        })
    }

    /// Returns the canonical macro observation.
    pub const fn observation(&self) -> &MacroObservation {
        &self.observation
    }

    /// Returns native row content identity.
    pub const fn native_row_digest(&self) -> EiaDigest {
        self.native_row_digest
    }

    /// Returns native row schema identity.
    pub const fn native_schema_digest(&self) -> EiaDigest {
        self.native_schema_digest
    }

    /// Returns stable provider series identity.
    pub const fn series_digest(&self) -> EiaDigest {
        self.series_digest
    }

    /// Returns raw page content identity.
    pub const fn raw_page_digest(&self) -> EiaDigest {
        self.raw_page_digest
    }
}

fn canonical_effective(native: &EiaObservation) -> Result<ResearchTemporalCoordinate, EiaError> {
    match native.period().kind() {
        EiaPeriodKind::CalendarDate(date) => Ok(ResearchTemporalCoordinate::calendar_date(*date)),
        EiaPeriodKind::Year(year) => period_coordinate(native, *year, 1),
        EiaPeriodKind::Month { year, month } => period_coordinate(native, *year, u16::from(*month)),
        EiaPeriodKind::Quarter { year, quarter } => {
            period_coordinate(native, *year, u16::from(*quarter))
        }
        // Provider-native periods do not carry a trustworthy calendar year/ordinal. Retain them
        // in native evidence until a route-specific period scheme is explicitly modeled.
        EiaPeriodKind::Provider(_) => Err(EiaError::Canonicalization),
    }
}

fn period_coordinate(
    native: &EiaObservation,
    year: u16,
    ordinal: u16,
) -> Result<ResearchTemporalCoordinate, EiaError> {
    let scheme = source_identifier_from_digest("eia-period-scheme", native.series().digest())?;
    let code = SourceIdentifier::try_from(native.period().raw())
        .map_err(|_| EiaError::Canonicalization)?;
    let ordinal = NonZeroU16::new(ordinal).ok_or(EiaError::Canonicalization)?;
    let period = ResearchPeriod::try_new(scheme, year, ordinal, code)
        .map_err(|_| EiaError::Canonicalization)?;
    Ok(ResearchTemporalCoordinate::source_period(period))
}

fn source_identifier_from_digest(
    prefix: &str,
    digest: EiaDigest,
) -> Result<SourceIdentifier, EiaError> {
    SourceIdentifier::try_from(format!("{prefix}:{}", lower_hex(digest.bytes())))
        .map_err(|_| EiaError::Canonicalization)
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
