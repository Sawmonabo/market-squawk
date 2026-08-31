//! Non-authoritative BLS publication handoff bound to physically sealed provider evidence.

use std::sync::Arc;

use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, MetadataRevision, PayloadReference,
    ResearchObservation, ResearchTemporalCoordinate, SchemaVersion, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryRequestId, ExtractionBatch, ExtractionContentIdentity,
    ExtractionRevisionPlan, MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
    MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES, PROVIDER_NATIVE_LINEAGE_SCHEMA_VERSION,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    ProviderNativeLineageSchema, SealedProviderCaptureBinding, SourceMetadata,
    SourceObjectCaptureIdentity,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::contract::BlsRuntimeInstanceCapability;
use crate::source::BlsExtractionOutput;
use crate::{
    BlsActivationCandidate, BlsCredentialRejoin, BlsProviderRateDeclaration, BlsResponse,
    BlsSource, BlsSourceConfig, BlsSourceError,
};

const PUBLICATION_CANDIDATE_SCHEMA_VERSION: u16 = 1;
const COMPLETE_PUBLICATION_PLAN_HANDOFF_SCHEMA_VERSION: u16 = 1;
const BLS_PROVIDER_SEMANTICS_SCHEMA: &str = "market-squawk-bls-provider-semantics-v1";
/// Durable shared-catalog name of the exact BLS native-row implementation decoded here.
pub const BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION: &str = "bls_timeseries_v1";

/// Explicit root schema-extension rejoin required to preserve BLS-native semantics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsRootSchemaExtensionRequirement {
    base_schema: SourceIdentifier,
    base_schema_version: SchemaVersion,
    provider_semantics_schema: SourceIdentifier,
    co_persistence_required: bool,
    point_in_time_join_required: bool,
    typed_desktop_mcp_join_required: bool,
    requirement_digest: EvidenceDigest,
}

impl BlsRootSchemaExtensionRequirement {
    fn new() -> Result<Self, BlsSourceError> {
        let base_schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let base_schema_version = SchemaVersion::CURRENT;
        let provider_semantics_schema = SourceIdentifier::try_from(BLS_PROVIDER_SEMANTICS_SCHEMA)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let mut requirement = Self {
            base_schema,
            base_schema_version,
            provider_semantics_schema,
            co_persistence_required: true,
            point_in_time_join_required: true,
            typed_desktop_mcp_join_required: true,
            requirement_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        requirement.requirement_digest = digest_serialized(
            b"market-squawk/bls-root-schema-extension/v1",
            &(
                &requirement.base_schema,
                requirement.base_schema_version,
                &requirement.provider_semantics_schema,
                requirement.co_persistence_required,
                requirement.point_in_time_join_required,
                requirement.typed_desktop_mcp_join_required,
            ),
        )?;
        Ok(requirement)
    }

    /// Returns the shared canonical record schema carrying the MacroObservation payload.
    pub const fn base_schema(&self) -> &SourceIdentifier {
        &self.base_schema
    }

    /// Returns the exact companion provider-semantics schema root must persist and query.
    pub const fn provider_semantics_schema(&self) -> &SourceIdentifier {
        &self.provider_semantics_schema
    }

    /// Returns the complete root integration requirement identity.
    pub const fn requirement_digest(&self) -> EvidenceDigest {
        self.requirement_digest
    }

    /// Reconstructs the exact schema join instead of trusting serialized booleans.
    pub fn validate(&self) -> Result<(), BlsSourceError> {
        if self == &Self::new()? {
            Ok(())
        } else {
            Err(BlsSourceError::InvalidPublication)
        }
    }
}

/// Exact BLS footnote semantics retained beside the shared canonical macro row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalFootnote {
    code: Option<Box<str>>,
    text: Option<Box<str>>,
}

impl BlsCanonicalFootnote {
    /// Returns the exact provider code, including `P` for preliminary values.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Returns exact provider explanatory text when supplied.
    pub fn text(&self) -> Option<&str> {
        self.text.as_deref()
    }
}

/// Full configured semantics for one BLS series in a publication chunk.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalSeriesManifest {
    series_id: SourceIdentifier,
    title: Box<str>,
    unit: SourceIdentifier,
    frequency: SourceIdentifier,
    seasonal_adjustment: SourceIdentifier,
    measure: SourceIdentifier,
    metadata_content_digest: EvidenceDigest,
    authorization_reference: SourceIdentifier,
}

impl BlsCanonicalSeriesManifest {
    /// Returns the exact BLS series identifier.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the user-verified provider title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the exact verified frequency.
    pub const fn frequency(&self) -> &SourceIdentifier {
        &self.frequency
    }

    /// Returns the exact verified unit.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }

    /// Returns the exact verified seasonal-adjustment semantic.
    pub const fn seasonal_adjustment(&self) -> &SourceIdentifier {
        &self.seasonal_adjustment
    }

    /// Returns the exact verified measure semantic.
    pub const fn measure(&self) -> &SourceIdentifier {
        &self.measure
    }
}

/// BLS-native semantics and exact clocks aligned one-for-one with a canonical extraction row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalObservationSemantics {
    record_ordinal: u32,
    series_id: SourceIdentifier,
    year: u16,
    period: SourceIdentifier,
    period_label: Box<str>,
    raw_value: Box<str>,
    value: Option<Decimal>,
    latest: bool,
    preliminary: bool,
    footnotes: Box<[BlsCanonicalFootnote]>,
    missing_explanations: Box<[Box<str>]>,
    effective_time: ResearchTemporalCoordinate,
    first_observed_at: Timestamp,
    response_received_at: Timestamp,
    locally_available_at: Timestamp,
    canonical_ingested_at: Timestamp,
    canonical_revision: SourceIdentifier,
    canonical_payload_digest: EvidenceDigest,
}

impl BlsCanonicalObservationSemantics {
    /// Returns the exact zero-based canonical extraction-row coordinate.
    pub const fn record_ordinal(&self) -> u32 {
        self.record_ordinal
    }

    /// Returns the exact configured series identifier.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the provider-authored year.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns the provider-authored period code.
    pub const fn period(&self) -> &SourceIdentifier {
        &self.period
    }

    /// Returns the exact provider period label without reducing it to a month number.
    pub fn period_label(&self) -> &str {
        &self.period_label
    }

    /// Returns the exact lexical provider value, including the missing `-` marker.
    pub fn raw_value(&self) -> &str {
        &self.raw_value
    }

    /// Returns the checked provider decimal, or `None` for an explicit missing marker.
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }

    /// Returns whether BLS marked this row latest.
    pub const fn is_latest(&self) -> bool {
        self.latest
    }

    /// Returns whether exact footnote code `P` marked this row preliminary.
    pub const fn is_preliminary(&self) -> bool {
        self.preliminary
    }

    /// Returns every exact provider footnote.
    pub fn footnotes(&self) -> &[BlsCanonicalFootnote] {
        &self.footnotes
    }

    /// Returns exact provider footnote text explaining a missing marker, without interpretation.
    pub fn missing_explanations(&self) -> &[Box<str>] {
        &self.missing_explanations
    }

    /// Returns the provider-period coordinate retained without inventing a calendar date.
    pub const fn effective_time(&self) -> &ResearchTemporalCoordinate {
        &self.effective_time
    }

    /// Returns when response headers first became available to the transport.
    pub const fn response_received_at(&self) -> Timestamp {
        self.response_received_at
    }

    /// Returns when the complete bounded response became locally usable.
    pub const fn locally_available_at(&self) -> Timestamp {
        self.locally_available_at
    }

    /// Returns when canonical normalization completed.
    pub const fn canonical_ingested_at(&self) -> Timestamp {
        self.canonical_ingested_at
    }

    /// Returns the adapter-authored local-content revision identity.
    pub const fn canonical_revision(&self) -> &SourceIdentifier {
        &self.canonical_revision
    }

    /// Returns the exact extraction-row payload digest retained beside native evidence.
    pub const fn canonical_payload_digest(&self) -> EvidenceDigest {
        self.canonical_payload_digest
    }
}

/// Typed companion payload retaining BLS information the shared MacroObservation cannot express.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalProviderSemantics {
    schema_requirement: BlsRootSchemaExtensionRequirement,
    series: Box<[BlsCanonicalSeriesManifest]>,
    observations: Box<[BlsCanonicalObservationSemantics]>,
    semantics_digest: EvidenceDigest,
}

impl BlsCanonicalProviderSemantics {
    pub(crate) fn try_new(
        config: &BlsSourceConfig,
        response: &BlsResponse,
        batch: &ExtractionBatch,
        response_received_at: Timestamp,
        locally_available_at: Timestamp,
        canonical_ingested_at: Timestamp,
    ) -> Result<Self, BlsSourceError> {
        let mut series_manifests = Vec::new();
        series_manifests
            .try_reserve_exact(response.series().len())
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(batch.records().len())
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let mut record_ordinal = 0_usize;
        for series in response.series() {
            let metadata = config
                .series_metadata(series.series_id())
                .ok_or(BlsSourceError::InvalidSeriesMetadata)?;
            series_manifests.push(BlsCanonicalSeriesManifest {
                series_id: SourceIdentifier::try_from(series.series_id())
                    .map_err(|_| BlsSourceError::InvalidPublication)?,
                title: metadata.title().into(),
                unit: metadata.unit().clone(),
                frequency: metadata.frequency().clone(),
                seasonal_adjustment: metadata.seasonal_adjustment().clone(),
                measure: metadata.measure().clone(),
                metadata_content_digest: metadata.evidence().content_digest(),
                authorization_reference: metadata.authorization_reference().clone(),
            });
            for observation in series.observations() {
                let record = batch
                    .records()
                    .get(record_ordinal)
                    .ok_or(BlsSourceError::InvalidPublication)?;
                let ResearchObservation::Macro(canonical): ResearchObservation =
                    serde_json::from_slice(record.payload())
                        .map_err(|_| BlsSourceError::InvalidPublication)?
                else {
                    return Err(BlsSourceError::InvalidPublication);
                };
                let footnotes = observation
                    .footnotes()
                    .iter()
                    .map(|footnote| BlsCanonicalFootnote {
                        code: footnote.code().map(Into::into),
                        text: footnote.text().map(Into::into),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let missing_explanations = if observation.value().is_none() {
                    observation
                        .footnotes()
                        .iter()
                        .filter_map(|footnote| footnote.text().map(Into::into))
                        .collect::<Vec<Box<str>>>()
                        .into_boxed_slice()
                } else {
                    Box::default()
                };
                if response_received_at >= locally_available_at
                    || locally_available_at >= canonical_ingested_at
                    || record.schema().as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
                    || record.available_at() != Some(locally_available_at)
                    || record.effective_time() != canonical.context().time().effective()
                    || record.revision() != canonical.context().provenance().source_identifier()
                    || canonical.series().as_str() != series.series_id()
                    || canonical.unit() != metadata.unit()
                    || canonical.context().provenance().received_at() != locally_available_at
                    || canonical.context().provenance().ingested_at() != canonical_ingested_at
                    || canonical.value().observed_value() != observation.value()
                {
                    return Err(BlsSourceError::InvalidPublication);
                }
                observations.push(BlsCanonicalObservationSemantics {
                    record_ordinal: u32::try_from(record_ordinal)
                        .map_err(|_| BlsSourceError::InvalidPublication)?,
                    series_id: SourceIdentifier::try_from(series.series_id())
                        .map_err(|_| BlsSourceError::InvalidPublication)?,
                    year: observation.year(),
                    period: SourceIdentifier::try_from(observation.period())
                        .map_err(|_| BlsSourceError::InvalidPublication)?,
                    period_label: observation.period_name().into(),
                    raw_value: observation.raw_value().into(),
                    value: observation.value(),
                    latest: observation.is_latest(),
                    preliminary: observation.is_preliminary(),
                    footnotes,
                    missing_explanations,
                    effective_time: record.effective_time().clone(),
                    first_observed_at: locally_available_at,
                    response_received_at,
                    locally_available_at,
                    canonical_ingested_at,
                    canonical_revision: record.revision().clone(),
                    canonical_payload_digest: record.evidence().content_digest(),
                });
                record_ordinal = record_ordinal
                    .checked_add(1)
                    .ok_or(BlsSourceError::InvalidPublication)?;
            }
        }
        if record_ordinal != batch.records().len() {
            return Err(BlsSourceError::InvalidPublication);
        }
        let schema_requirement = BlsRootSchemaExtensionRequirement::new()?;
        let mut semantics = Self {
            schema_requirement,
            series: series_manifests.into_boxed_slice(),
            observations: observations.into_boxed_slice(),
            semantics_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        semantics.semantics_digest = semantics.compute_digest()?;
        semantics.validate(batch)?;
        Ok(semantics)
    }

    /// Returns configured provider series semantics in response order.
    pub fn series(&self) -> &[BlsCanonicalSeriesManifest] {
        &self.series
    }

    /// Returns provider observations aligned one-for-one with canonical rows.
    pub fn observations(&self) -> &[BlsCanonicalObservationSemantics] {
        &self.observations
    }

    /// Returns the explicit root storage/query/presentation schema join.
    pub const fn schema_requirement(&self) -> &BlsRootSchemaExtensionRequirement {
        &self.schema_requirement
    }

    /// Returns the exact companion semantic payload identity.
    pub const fn semantics_digest(&self) -> EvidenceDigest {
        self.semantics_digest
    }

    /// Decodes and independently validates one persisted BLS native-lineage sidecar.
    pub fn try_decode_persisted_native_sidecar(
        semantic_payload: &[u8],
    ) -> Result<Self, BlsSourceError> {
        if semantic_payload.is_empty()
            || semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        let semantics: Self = serde_json::from_slice(semantic_payload)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        semantics.validate_persisted_structure()?;
        Ok(semantics)
    }

    /// Rejoins one persisted companion observation to its exact value-only native row.
    pub fn validate_persisted_native_row(
        &self,
        record_ordinal: u32,
        native: &BlsTimeseriesNativeLineageRowV1,
    ) -> Result<&BlsCanonicalObservationSemantics, BlsSourceError> {
        let observation = self
            .observations
            .get(usize::try_from(record_ordinal).map_err(|_| BlsSourceError::InvalidPublication)?)
            .filter(|observation| observation.record_ordinal == record_ordinal)
            .ok_or(BlsSourceError::InvalidPublication)?;
        let mut matching_series = self
            .series
            .iter()
            .filter(|series| series.series_id == observation.series_id);
        let series = matching_series
            .next()
            .ok_or(BlsSourceError::InvalidPublication)?;
        let native_series = native.series();
        let native_observation = native.observation();
        if matching_series.next().is_some()
            || native_series.series_id() != &series.series_id
            || native_series.title() != series.title.as_ref()
            || native_series.unit() != &series.unit
            || native_series.frequency() != &series.frequency
            || native_series.seasonal_adjustment() != &series.seasonal_adjustment
            || native_series.measure() != &series.measure
            || native_observation.series_id() != &observation.series_id
            || native_observation.year() != observation.year
            || native_observation.period() != &observation.period
            || native_observation.period_label() != observation.period_label.as_ref()
            || native_observation.raw_value() != observation.raw_value.as_ref()
            || native_observation.value() != observation.value
            || native_observation.is_preliminary() != observation.preliminary
            || native_observation.footnotes() != observation.footnotes.as_ref()
            || native_observation.missing_explanations()
                != observation.missing_explanations.as_ref()
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(observation)
    }

    pub(crate) fn validate(&self, batch: &ExtractionBatch) -> Result<(), BlsSourceError> {
        self.validate_persisted_structure()?;
        self.schema_requirement.validate()?;
        if self.series.is_empty()
            || self.observations.len() != batch.records().len()
            || self
                .observations
                .iter()
                .enumerate()
                .any(|(index, observation)| {
                    usize::try_from(observation.record_ordinal).ok() != Some(index)
                        || batch.records().get(index).is_none_or(|record| {
                            record.revision() != &observation.canonical_revision
                                || record.effective_time() != &observation.effective_time
                                || record.evidence().content_digest()
                                    != observation.canonical_payload_digest
                                || record.available_at() != Some(observation.first_observed_at)
                                || observation.first_observed_at != observation.locally_available_at
                                || observation.response_received_at
                                    > observation.locally_available_at
                                || observation.locally_available_at
                                    > observation.canonical_ingested_at
                        })
                })
            || self.semantics_digest != self.compute_digest()?
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    fn validate_persisted_structure(&self) -> Result<(), BlsSourceError> {
        self.schema_requirement.validate()?;
        if self.series.is_empty()
            || self.observations.is_empty()
            || self.semantics_digest != self.compute_digest()?
            || self.series.iter().enumerate().any(|(index, series)| {
                series.title.is_empty()
                    || self.series[..index]
                        .iter()
                        .any(|prior| prior.series_id == series.series_id)
            })
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        for (index, observation) in self.observations.iter().enumerate() {
            let mut matching_series = self
                .series
                .iter()
                .filter(|series| series.series_id == observation.series_id);
            let series = matching_series
                .next()
                .ok_or(BlsSourceError::InvalidPublication)?;
            let (scheme, ordinal, frequency) =
                crate::observations::period_parts(observation.period.as_str())
                    .ok_or(BlsSourceError::InvalidPublication)?;
            let effective = observation
                .effective_time
                .source_period_value()
                .ok_or(BlsSourceError::InvalidPublication)?;
            let expected_value = if observation.raw_value.as_ref() == "-" {
                None
            } else {
                Some(
                    Decimal::from_str_exact(&observation.raw_value)
                        .map_err(|_| BlsSourceError::InvalidPublication)?,
                )
            };
            let expected_preliminary = observation
                .footnotes
                .iter()
                .any(|footnote| footnote.code() == Some("P"));
            let expected_missing_explanations = if expected_value.is_none() {
                observation
                    .footnotes
                    .iter()
                    .filter_map(BlsCanonicalFootnote::text)
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if usize::try_from(observation.record_ordinal).ok() != Some(index)
                || matching_series.next().is_some()
                || series.frequency.as_str() != frequency
                || effective.scheme().as_str() != scheme
                || effective.year() != observation.year
                || effective.ordinal().get() != ordinal
                || effective.code() != &observation.period
                || observation.value != expected_value
                || observation.preliminary != expected_preliminary
                || observation
                    .missing_explanations
                    .iter()
                    .map(AsRef::as_ref)
                    .ne(expected_missing_explanations)
                || observation.first_observed_at != observation.locally_available_at
                || observation.response_received_at >= observation.locally_available_at
                || observation.locally_available_at >= observation.canonical_ingested_at
                || observation.canonical_payload_digest.algorithm() != DigestAlgorithm::Sha256
                || observation.canonical_payload_digest.bytes() == [0; 32]
            {
                return Err(BlsSourceError::InvalidPublication);
            }
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<EvidenceDigest, BlsSourceError> {
        digest_serialized(
            b"market-squawk/bls-canonical-provider-semantics/v1",
            &(&self.schema_requirement, &self.series, &self.observations),
        )
    }

    fn try_native_lineage(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ProviderNativeLineageBatch, BlsSourceError> {
        self.validate(batch)?;
        let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::BlsTimeseriesV1,
            batch,
        )
        .map_err(|_| BlsSourceError::InvalidPublication)?;
        native_lineage
            .try_set_batch_sidecar(self)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        for observation in &self.observations {
            let mut matching_series = self
                .series
                .iter()
                .filter(|series| series.series_id == observation.series_id);
            let series = matching_series
                .next()
                .ok_or(BlsSourceError::InvalidPublication)?;
            if matching_series.next().is_some() {
                return Err(BlsSourceError::InvalidPublication);
            }
            native_lineage
                .try_push(&BlsNativeLineageRowV1 {
                    series: BlsNativeLineageSeriesV1 {
                        series_id: &series.series_id,
                        title: &series.title,
                        unit: &series.unit,
                        frequency: &series.frequency,
                        seasonal_adjustment: &series.seasonal_adjustment,
                        measure: &series.measure,
                    },
                    observation: BlsNativeLineageObservationV1 {
                        series_id: &observation.series_id,
                        year: observation.year,
                        period: &observation.period,
                        period_label: &observation.period_label,
                        raw_value: &observation.raw_value,
                        value: &observation.value,
                        preliminary: observation.preliminary,
                        footnotes: &observation.footnotes,
                        missing_explanations: &observation.missing_explanations,
                    },
                })
                .map_err(|_| BlsSourceError::InvalidPublication)?;
        }
        native_lineage
            .finish()
            .map_err(|_| BlsSourceError::InvalidPublication)
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct BlsNativeLineageRowV1<'a> {
    series: BlsNativeLineageSeriesV1<'a>,
    observation: BlsNativeLineageObservationV1<'a>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct BlsNativeLineageSeriesV1<'a> {
    series_id: &'a SourceIdentifier,
    title: &'a str,
    unit: &'a SourceIdentifier,
    frequency: &'a SourceIdentifier,
    seasonal_adjustment: &'a SourceIdentifier,
    measure: &'a SourceIdentifier,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct BlsNativeLineageObservationV1<'a> {
    series_id: &'a SourceIdentifier,
    year: u16,
    period: &'a SourceIdentifier,
    period_label: &'a str,
    raw_value: &'a str,
    value: &'a Option<Decimal>,
    preliminary: bool,
    footnotes: &'a [BlsCanonicalFootnote],
    missing_explanations: &'a [Box<str>],
}

/// Checked typed view of one persisted `BlsTimeseriesV1` provider-native row.
///
/// The shared store owns row identity and publication authority. This value only decodes the
/// bounded adapter payload after the caller supplies the persisted native-lineage schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BlsTimeseriesNativeLineageRowV1 {
    series: BlsTimeseriesNativeLineageSeriesV1,
    observation: BlsTimeseriesNativeLineageObservationV1,
}

impl BlsTimeseriesNativeLineageRowV1 {
    /// Decodes one exact persisted native row under the only supported BLS implementation/version.
    ///
    /// # Errors
    ///
    /// Rejects an incompatible shared schema, an empty or oversized payload, malformed JSON,
    /// unknown fields, or semantics that could not have been emitted by the BLS v1 encoder.
    pub fn try_decode(
        schema: ProviderNativeLineageSchema,
        semantic_payload: &[u8],
    ) -> Result<Self, BlsSourceError> {
        if schema.version() != PROVIDER_NATIVE_LINEAGE_SCHEMA_VERSION
            || schema.implementation() != ProviderNativeLineageImplementation::BlsTimeseriesV1
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Self::decode_payload(semantic_payload)
    }

    /// Decodes the value-only schema projection exposed by a durable shared-catalog read.
    ///
    /// The caller must first validate the complete persisted binding, including its schema
    /// fingerprint, row digest, and raw-capture join. This adapter check then rejects any catalog
    /// row that is not explicitly labeled as the current BLS timeseries implementation.
    pub fn try_decode_persisted(
        schema_version: u16,
        implementation: &str,
        semantic_payload: &[u8],
    ) -> Result<Self, BlsSourceError> {
        if schema_version != PROVIDER_NATIVE_LINEAGE_SCHEMA_VERSION
            || implementation != BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Self::decode_payload(semantic_payload)
    }

    fn decode_payload(semantic_payload: &[u8]) -> Result<Self, BlsSourceError> {
        if semantic_payload.is_empty()
            || semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        let row: Self = serde_json::from_slice(semantic_payload)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        row.validate()?;
        Ok(row)
    }

    /// Returns the exact configured series semantics retained for this observation.
    pub const fn series(&self) -> &BlsTimeseriesNativeLineageSeriesV1 {
        &self.series
    }

    /// Returns the exact provider observation semantics retained beside the canonical row.
    pub const fn observation(&self) -> &BlsTimeseriesNativeLineageObservationV1 {
        &self.observation
    }

    fn validate(&self) -> Result<(), BlsSourceError> {
        let expected_value = if self.observation.raw_value.as_ref() == "-" {
            None
        } else {
            Some(
                Decimal::from_str_exact(&self.observation.raw_value)
                    .map_err(|_| BlsSourceError::InvalidPublication)?,
            )
        };
        let expected_preliminary = self
            .observation
            .footnotes
            .iter()
            .any(|footnote| footnote.code() == Some("P"));
        let expected_missing_explanations = if expected_value.is_none() {
            self.observation
                .footnotes
                .iter()
                .filter_map(BlsCanonicalFootnote::text)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if self.series.series_id != self.observation.series_id
            || self.series.title.is_empty()
            || self.series.title.len() > 512
            || self.series.title.trim() != self.series.title.as_ref()
            || self.series.title.chars().any(char::is_control)
            || self.observation.year < 1900
            || crate::observations::period_parts(self.observation.period.as_str()).is_none()
            || self.observation.period_label.len() > 64
            || self.observation.value != expected_value
            || self.observation.preliminary != expected_preliminary
            || self
                .observation
                .missing_explanations
                .iter()
                .map(AsRef::as_ref)
                .ne(expected_missing_explanations)
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }
}

/// Typed configured-series projection decoded from one persisted BLS native row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BlsTimeseriesNativeLineageSeriesV1 {
    series_id: SourceIdentifier,
    title: Box<str>,
    unit: SourceIdentifier,
    frequency: SourceIdentifier,
    seasonal_adjustment: SourceIdentifier,
    measure: SourceIdentifier,
}

impl BlsTimeseriesNativeLineageSeriesV1 {
    /// Returns the exact BLS series identifier.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the user-verified series title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the explicit unit retained from verified series metadata.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }

    /// Returns the exact provider frequency semantic.
    pub const fn frequency(&self) -> &SourceIdentifier {
        &self.frequency
    }

    /// Returns the exact provider seasonal-adjustment semantic.
    pub const fn seasonal_adjustment(&self) -> &SourceIdentifier {
        &self.seasonal_adjustment
    }

    /// Returns the exact configured measure semantic.
    pub const fn measure(&self) -> &SourceIdentifier {
        &self.measure
    }
}

/// Typed observation projection decoded from one persisted BLS native row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BlsTimeseriesNativeLineageObservationV1 {
    series_id: SourceIdentifier,
    year: u16,
    period: SourceIdentifier,
    period_label: Box<str>,
    raw_value: Box<str>,
    value: Option<Decimal>,
    preliminary: bool,
    footnotes: Box<[BlsCanonicalFootnote]>,
    missing_explanations: Box<[Box<str>]>,
}

impl BlsTimeseriesNativeLineageObservationV1 {
    /// Returns the exact BLS series identifier.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the observation year.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns the exact BLS period code.
    pub const fn period(&self) -> &SourceIdentifier {
        &self.period
    }

    /// Returns the provider period label.
    pub fn period_label(&self) -> &str {
        &self.period_label
    }

    /// Returns the exact lexical provider value, including the missing `-` marker.
    pub fn raw_value(&self) -> &str {
        &self.raw_value
    }

    /// Returns the parsed exact decimal, or `None` for the provider missing marker.
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }

    /// Returns whether the provider marked this value preliminary.
    pub const fn is_preliminary(&self) -> bool {
        self.preliminary
    }

    /// Returns all exact provider footnotes.
    pub fn footnotes(&self) -> &[BlsCanonicalFootnote] {
        &self.footnotes
    }

    /// Returns explicit missing-value explanations derived from provider footnotes.
    pub fn missing_explanations(&self) -> &[Box<str>] {
        &self.missing_explanations
    }
}

/// One-use, provider-local proof that every deterministic BLS request chunk is present.
///
/// This value does not publish data or mint a shared generation. It keeps all non-cloneable
/// candidates together so the application can reserve and commit the request plan as one logical
/// unit without losing the exact request-set, source-generation, or chunk-closure evidence.
#[derive(Debug)]
pub struct BlsCompletePublicationPlanHandoff {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    discovery_request_id: DiscoveryRequestId,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    request_set_identity: EvidenceDigest,
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    total_chunks: u16,
    canonical_record_count: u64,
    candidates: Box<[BlsPublicationCandidate]>,
    completion_digest: EvidenceDigest,
}

impl BlsCompletePublicationPlanHandoff {
    /// Consumes an exact, ordered set of BLS candidates and proves complete plan closure.
    ///
    /// # Errors
    ///
    /// Rejects empty, missing, duplicate, reordered, cross-request, cross-capture,
    /// cross-generation, cross-activation, or otherwise internally inconsistent candidates.
    pub fn try_new(candidates: Vec<BlsPublicationCandidate>) -> Result<Self, BlsSourceError> {
        let first = candidates
            .first()
            .ok_or(BlsSourceError::InvalidPublication)?;
        let total_chunks = first.total_chunks;
        if total_chunks == 0 || candidates.len() != usize::from(total_chunks) {
            return Err(BlsSourceError::InvalidPublication);
        }
        first.validate_complete_plan_projection()?;
        let source_id = first.source_id.clone();
        let metadata_revision = first.metadata_revision.clone();
        let discovery_request_id = first.discovery_request_id;
        let provider_dataset = first.provider_dataset.clone();
        let analytical_dataset = first.analytical_dataset.clone();
        let request_set_identity = first.discovery_request_set_identity;
        let capture_content_digest = first.discovery_capture_content_digest;
        let capture_observation_digest = first.discovery_capture_observation_digest;
        let source_generation_digest = first.source_generation_digest;
        let sealed_capture_receipt_digest =
            first.sealed_capture_binding.sealed_capture_receipt_digest();
        let credential_rejoin = first.credential_rejoin;
        let provider_rate_declaration_digest = first.provider_rate_declaration_digest;
        let doctor_report_digest = first.doctor_report_digest;
        let sealed_doctor_capture_receipt_digest = first.sealed_doctor_capture_receipt_digest;
        let activation_expires_at = first.activation_expires_at;
        let activation_candidate_digest = first.activation_candidate_digest;
        let runtime_instance = Arc::clone(&first.runtime_instance);

        let mut canonical_record_count = 0_u64;
        for (expected_index, candidate) in candidates.iter().enumerate() {
            candidate.validate_complete_plan_projection()?;
            let expected_index =
                u16::try_from(expected_index).map_err(|_| BlsSourceError::InvalidPublication)?;
            if candidate.chunk_index != expected_index
                || candidate.total_chunks != total_chunks
                || candidate.source_id != source_id
                || candidate.metadata_revision != metadata_revision
                || candidate.discovery_request_id != discovery_request_id
                || candidate.provider_dataset != provider_dataset
                || candidate.analytical_dataset != analytical_dataset
                || candidate.discovery_request_set_identity != request_set_identity
                || candidate.discovery_capture_content_digest != capture_content_digest
                || candidate.discovery_capture_observation_digest != capture_observation_digest
                || candidate.source_generation_digest != source_generation_digest
                || candidate
                    .sealed_capture_binding
                    .sealed_capture_receipt_digest()
                    != sealed_capture_receipt_digest
                || candidate.credential_rejoin != credential_rejoin
                || candidate.provider_rate_declaration_digest != provider_rate_declaration_digest
                || candidate.doctor_report_digest != doctor_report_digest
                || candidate.sealed_doctor_capture_receipt_digest
                    != sealed_doctor_capture_receipt_digest
                || candidate.activation_expires_at != activation_expires_at
                || candidate.activation_candidate_digest != activation_candidate_digest
                || !Arc::ptr_eq(&candidate.runtime_instance, &runtime_instance)
            {
                return Err(BlsSourceError::InvalidPublication);
            }
            canonical_record_count = canonical_record_count
                .checked_add(u64::from(candidate.canonical_record_count))
                .ok_or(BlsSourceError::InvalidPublication)?;
        }
        if canonical_record_count == 0 {
            return Err(BlsSourceError::InvalidPublication);
        }

        let mut handoff = Self {
            schema_version: COMPLETE_PUBLICATION_PLAN_HANDOFF_SCHEMA_VERSION,
            source_id,
            metadata_revision,
            discovery_request_id,
            provider_dataset,
            analytical_dataset,
            request_set_identity,
            capture_content_digest,
            capture_observation_digest,
            source_generation_digest,
            sealed_capture_receipt_digest,
            total_chunks,
            canonical_record_count,
            candidates: candidates.into_boxed_slice(),
            completion_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        handoff.completion_digest = complete_publication_plan_digest(&handoff)?;
        Ok(handoff)
    }

    /// Returns the exact provider source root shared publication must rejoin.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact provider metadata revision shared publication must rejoin.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the discovery operation shared by every completed chunk.
    pub const fn discovery_request_id(&self) -> DiscoveryRequestId {
        self.discovery_request_id
    }

    /// Returns the exact provider request-plan dataset.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the shared analytical dataset root to reserve once for the complete plan.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns the deterministic identity of the complete provider request graph.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }

    /// Returns the stable content identity of the complete provider request graph.
    pub const fn capture_content_digest(&self) -> EvidenceDigest {
        self.capture_content_digest
    }

    /// Returns the receipt-time-bound identity of the complete provider request graph.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.capture_observation_digest
    }

    /// Returns the stable source/configuration/rights/credential generation for every chunk.
    pub const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }

    /// Returns the one physical seal shared by every completed request-plan chunk.
    pub const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_digest
    }

    /// Returns the exact number of contiguous deterministic chunks retained by this handoff.
    pub const fn total_chunks(&self) -> u16 {
        self.total_chunks
    }

    /// Returns the checked total canonical row count across all completed chunks.
    pub const fn canonical_record_count(&self) -> u64 {
        self.canonical_record_count
    }

    /// Returns candidates in exact deterministic chunk order without consuming authority.
    pub fn candidates(&self) -> &[BlsPublicationCandidate] {
        &self.candidates
    }

    /// Returns the non-authoritative identity of the complete provider-local plan handoff.
    pub const fn completion_digest(&self) -> EvidenceDigest {
        self.completion_digest
    }

    /// Consumes the closure proof into its ordered, still-one-use publication candidates.
    ///
    /// Callers must keep this returned slice as one logical publication group. Splitting it does
    /// not create partial-plan publication authority.
    pub fn into_candidates(self) -> Box<[BlsPublicationCandidate]> {
        self.candidates
    }
}

/// Canonical BLS input whose exact provider response has already been physically sealed.
///
/// This value is deliberately only a root-ingest handoff. It carries no manifest, generation,
/// committed timestamp, checkpoint, restore admission, query pin, or point-in-time receipt. Only
/// the shared application/data authority may assign locally observed revisions, commit an
/// immutable dataset, advance a job checkpoint, and mint a typed point-in-time read.
#[derive(Debug)]
pub struct BlsPublicationCandidate {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    discovery_request_id: DiscoveryRequestId,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    object_id: SourceIdentifier,
    canonical_schema: SourceIdentifier,
    canonical_schema_version: SchemaVersion,
    schema_extension_requirement_digest: EvidenceDigest,
    chunk_index: u16,
    total_chunks: u16,
    canonical_record_count: u32,
    first_observed_at: Timestamp,
    response_received_at: Timestamp,
    locally_available_at: Timestamp,
    canonical_ingested_at: Timestamp,
    discovery_request_set_identity: EvidenceDigest,
    discovery_capture_content_digest: EvidenceDigest,
    discovery_capture_observation_digest: EvidenceDigest,
    component_request_identity: EvidenceDigest,
    component_content_digest: EvidenceDigest,
    component_observation_digest: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
    sealed_capture_binding: SealedProviderCaptureBinding,
    extraction_content_digest: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    provider_semantics_digest: EvidenceDigest,
    native_lineage_digest: EvidenceDigest,
    credential_rejoin: BlsCredentialRejoin,
    provider_rate_declaration_digest: EvidenceDigest,
    doctor_report_digest: EvidenceDigest,
    sealed_doctor_capture_receipt_digest: EvidenceDigest,
    activation_expires_at: Timestamp,
    activation_candidate_digest: EvidenceDigest,
    provider_semantics: BlsCanonicalProviderSemantics,
    candidate_digest: EvidenceDigest,
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
}

impl BlsPublicationCandidate {
    pub(crate) fn try_new(
        metadata: &SourceMetadata,
        config: &BlsSourceConfig,
        rate: &BlsProviderRateDeclaration,
        output: BlsExtractionOutput,
        activation: &BlsActivationCandidate,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
    ) -> Result<Self, BlsSourceError> {
        let (batch, provider_semantics, discovery_admission) = output.into_parts();
        discovery_admission.validate_for_extraction(
            batch.request(),
            expected_runtime_instance,
            activation,
        )?;
        let object = batch.request().object();
        let chunk_index = discovery_admission.chunk_index();
        let sealed_discovery_capture = discovery_admission.sealed_discovery_capture();
        let capture = sealed_discovery_capture.capture();
        let page = capture
            .pages()
            .get(chunk_index)
            .ok_or(BlsSourceError::InvalidPublication)?;
        let chunk_index =
            u16::try_from(chunk_index).map_err(|_| BlsSourceError::InvalidPublication)?;
        let total_chunks =
            u16::try_from(config.chunk_count()).map_err(|_| BlsSourceError::InvalidPublication)?;
        let canonical_record_count =
            u32::try_from(batch.records().len()).map_err(|_| BlsSourceError::InvalidPublication)?;
        let first_observed_at = object.effective_interval().starts_at();
        let response_received_at = discovery_admission.response_received_at();
        let locally_available_at = page.received_at();
        let canonical_schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let canonical_schema_version = SchemaVersion::CURRENT;
        let canonical_ingested_at = validate_canonical_batch(
            metadata,
            config,
            &batch,
            page.body_digest(),
            first_observed_at,
            locally_available_at,
            &canonical_schema,
        )?;
        provider_semantics.validate(&batch)?;
        let native_lineage = provider_semantics.try_native_lineage(&batch)?;
        let schema_extension_requirement_digest =
            provider_semantics.schema_requirement().requirement_digest();
        let extraction_content = ExtractionContentIdentity::try_from_batch(&batch)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let discovery_request_id = object.discovery_request_id();
        let object_id = object.object_id().clone();
        let component_request_identity = discovery_admission.component_request_identity();
        let component_content_digest = discovery_admission.component_content_digest();
        let component_observation_digest = discovery_admission.component_observation_digest();
        let source_generation_digest = discovery_admission.source_generation_digest();

        if object.source_id() != metadata.source_id()
            || object.metadata_revision() != metadata.revision()
            || object.dataset() != config.dataset()
            || capture.source_id() != metadata.source_id()
            || capture.metadata_revision() != metadata.revision()
            || capture.dataset() != config.dataset()
            || !validate_discovery_component(
                capture,
                usize::from(chunk_index),
                object.capture_identity(),
                component_request_identity,
                component_content_digest,
                component_observation_digest,
            )
            || object.evidence().content_digest() != page.body_digest()
            || object.expected_bytes() != Some(page.body_bytes())
            || batch.request().deadline() >= activation.expires_at()
            || response_received_at >= locally_available_at
            || first_observed_at != locally_available_at
            || locally_available_at >= canonical_ingested_at
            || provider_semantics.observations.iter().any(|observation| {
                observation.response_received_at != response_received_at
                    || observation.locally_available_at != locally_available_at
                    || observation.canonical_ingested_at != canonical_ingested_at
            })
            || usize::from(chunk_index) >= config.chunk_count()
            || canonical_record_count == 0
            || extraction_content.record_count() != batch.records().len()
            || sealed_discovery_capture.receipt_digest().bytes() == [0; 32]
            || activation.candidate_digest().bytes() == [0; 32]
            || discovery_admission.activation_candidate_digest() != activation.candidate_digest()
            || !Arc::ptr_eq(
                discovery_admission.runtime_instance(),
                expected_runtime_instance,
            )
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        let discovery_request_set_identity = capture.request_set_identity();
        let discovery_capture_content_digest = capture.content_digest();
        let discovery_capture_observation_digest = capture.observation_digest();
        if !matches!(
            capture.terminal(),
            ProviderCaptureTerminalDisposition::StandaloneResponse
                | ProviderCaptureTerminalDisposition::CompleteRequestGraph
        ) {
            return Err(BlsSourceError::InvalidPublication);
        }
        let extraction_content_digest = extraction_content.digest();
        let canonical_content_digest = canonical_content_digest(&batch)?;
        let provider_semantics_digest = provider_semantics.semantics_digest();
        let native_lineage_digest = native_lineage.batch_digest();
        let mut row_capture_page_ordinals = Vec::new();
        row_capture_page_ordinals
            .try_reserve_exact(batch.records().len())
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        row_capture_page_ordinals
            .extend(std::iter::repeat(chunk_index).take(batch.records().len()));
        let capture_token = discovery_admission.into_capture_token();
        let sealed_capture_binding =
            capture_token.try_bind(batch, native_lineage, row_capture_page_ordinals)?;

        let mut candidate = Self {
            schema_version: PUBLICATION_CANDIDATE_SCHEMA_VERSION,
            source_id: metadata.source_id().clone(),
            metadata_revision: metadata.revision().clone(),
            discovery_request_id,
            provider_dataset: config.dataset().clone(),
            analytical_dataset: BlsSource::analytical_dataset_identifier(config.dataset())?,
            object_id,
            canonical_schema,
            canonical_schema_version,
            schema_extension_requirement_digest,
            chunk_index,
            total_chunks,
            canonical_record_count,
            first_observed_at,
            response_received_at,
            locally_available_at,
            canonical_ingested_at,
            discovery_request_set_identity,
            discovery_capture_content_digest,
            discovery_capture_observation_digest,
            component_request_identity,
            component_content_digest,
            component_observation_digest,
            source_generation_digest,
            sealed_capture_binding,
            extraction_content_digest,
            canonical_content_digest,
            provider_semantics_digest,
            native_lineage_digest,
            credential_rejoin: config.credential_rejoin(),
            provider_rate_declaration_digest: rate.declaration_digest(),
            doctor_report_digest: activation.doctor_report().report_digest(),
            sealed_doctor_capture_receipt_digest: activation
                .sealed_doctor_capture()
                .receipt_digest(),
            activation_expires_at: activation.expires_at(),
            activation_candidate_digest: activation.candidate_digest(),
            provider_semantics,
            candidate_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            runtime_instance: Arc::clone(expected_runtime_instance),
        };
        candidate.candidate_digest = candidate_digest(&candidate)?;
        Ok(candidate)
    }

    /// Reopens every provider-local invariant against the exact batch and physical receipt.
    ///
    /// Root should call this immediately before it reserves the shared ingest run. Successful
    /// validation does not mean that any dataset has been committed.
    pub(crate) fn validate(
        &self,
        metadata: &SourceMetadata,
        config: &BlsSourceConfig,
        rate: &BlsProviderRateDeclaration,
        activation: &BlsActivationCandidate,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
    ) -> Result<(), BlsSourceError> {
        activation.validate(
            &crate::BlsActivationPlan::try_new(
                metadata.source_id().clone(),
                metadata.revision().clone(),
                config.dataset().clone(),
                BlsSource::analytical_dataset_identifier(config.dataset())?,
                config.credential_rejoin(),
                rate.clone(),
            )?,
            crate::client::system_timestamp()?,
            expected_runtime_instance,
        )?;
        self.sealed_capture_binding
            .validate()
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let batch = self.sealed_capture_binding.batch();
        let native_lineage = self.sealed_capture_binding.native_lineage();
        let object = batch.request().object();
        let capture = self.sealed_capture_binding.capture_evidence();
        let page = capture
            .pages()
            .get(usize::from(self.chunk_index))
            .ok_or(BlsSourceError::InvalidPublication)?;
        let extraction = ExtractionContentIdentity::try_from_batch(batch)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let canonical_ingested_at = validate_canonical_batch(
            metadata,
            config,
            batch,
            page.body_digest(),
            self.first_observed_at,
            self.locally_available_at,
            &self.canonical_schema,
        )?;
        self.provider_semantics.validate(batch)?;
        native_lineage
            .validate(batch)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        if self.schema_version != PUBLICATION_CANDIDATE_SCHEMA_VERSION
            || self.source_id != *metadata.source_id()
            || self.metadata_revision != *metadata.revision()
            || self.provider_dataset != *config.dataset()
            || self.analytical_dataset
                != BlsSource::analytical_dataset_identifier(config.dataset())?
            || self.discovery_request_id != object.discovery_request_id()
            || self.object_id != *object.object_id()
            || self.first_observed_at != object.effective_interval().starts_at()
            || self.response_received_at >= self.locally_available_at
            || self.locally_available_at != page.received_at()
            || self.first_observed_at != self.locally_available_at
            || self.locally_available_at >= self.canonical_ingested_at
            || batch.request().deadline() >= activation.expires_at()
            || self.canonical_schema.as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
            || self.canonical_schema_version != SchemaVersion::CURRENT
            || self.schema_extension_requirement_digest
                != self
                    .provider_semantics
                    .schema_requirement()
                    .requirement_digest()
            || usize::from(self.total_chunks) != config.chunk_count()
            || usize::try_from(self.canonical_record_count).ok() != Some(batch.records().len())
            || self.canonical_record_count == 0
            || self.canonical_ingested_at != canonical_ingested_at
            || self.discovery_request_set_identity != capture.request_set_identity()
            || self.discovery_capture_content_digest != capture.content_digest()
            || self.discovery_capture_observation_digest != capture.observation_digest()
            || self
                .sealed_capture_binding
                .sealed_capture_receipt_digest()
                .bytes()
                == [0; 32]
            || self.sealed_capture_binding.component_ordinal()
                != (self.total_chunks > 1).then_some(self.chunk_index)
            || self.source_generation_digest != activation.plan().plan_digest()
            || !validate_discovery_component(
                capture,
                usize::from(self.chunk_index),
                object.capture_identity(),
                self.component_request_identity,
                self.component_content_digest,
                self.component_observation_digest,
            )
            || extraction.digest() != self.extraction_content_digest
            || canonical_content_digest(batch)? != self.canonical_content_digest
            || self.provider_semantics.semantics_digest() != self.provider_semantics_digest
            || self
                .provider_semantics
                .observations
                .iter()
                .any(|observation| {
                    observation.response_received_at != self.response_received_at
                        || observation.locally_available_at != self.locally_available_at
                        || observation.canonical_ingested_at != self.canonical_ingested_at
                })
            || native_lineage.batch_digest() != self.native_lineage_digest
            || self.credential_rejoin != config.credential_rejoin()
            || self.provider_rate_declaration_digest != rate.declaration_digest()
            || self.doctor_report_digest != activation.doctor_report().report_digest()
            || self.sealed_doctor_capture_receipt_digest
                != activation.sealed_doctor_capture().receipt_digest()
            || self.activation_expires_at != activation.expires_at()
            || self.activation_candidate_digest != activation.candidate_digest()
            || !Arc::ptr_eq(&self.runtime_instance, expected_runtime_instance)
            || self.candidate_digest != candidate_digest(self)?
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    fn validate_complete_plan_projection(&self) -> Result<(), BlsSourceError> {
        self.sealed_capture_binding
            .validate()
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let batch = self.sealed_capture_binding.batch();
        let native_lineage = self.sealed_capture_binding.native_lineage();
        let object = batch.request().object();
        let capture = self.sealed_capture_binding.capture_evidence();
        let chunk_index = usize::from(self.chunk_index);
        let total_chunks = usize::from(self.total_chunks);
        let page = capture
            .pages()
            .get(chunk_index)
            .ok_or(BlsSourceError::InvalidPublication)?;
        self.provider_semantics.validate(batch)?;
        native_lineage
            .validate(batch)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        for row in native_lineage.rows() {
            BlsTimeseriesNativeLineageRowV1::try_decode(
                native_lineage.schema(),
                row.semantic_payload(),
            )?;
        }
        let complete_capture = if total_chunks == 1 {
            capture.terminal() == ProviderCaptureTerminalDisposition::StandaloneResponse
                && capture.request_graph_components().is_empty()
                && self.sealed_capture_binding.component_ordinal().is_none()
        } else {
            capture.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
                && capture.request_graph_components().len() == total_chunks
                && self.sealed_capture_binding.component_ordinal() == Some(self.chunk_index)
        };
        if self.schema_version != PUBLICATION_CANDIDATE_SCHEMA_VERSION
            || total_chunks == 0
            || chunk_index >= total_chunks
            || capture.pages().len() != total_chunks
            || !complete_capture
            || self.source_id != *object.source_id()
            || self.metadata_revision != *object.metadata_revision()
            || self.discovery_request_id != object.discovery_request_id()
            || self.provider_dataset != *object.dataset()
            || self.object_id != *object.object_id()
            || self.first_observed_at != object.effective_interval().starts_at()
            || self.response_received_at >= self.locally_available_at
            || self.locally_available_at != page.received_at()
            || self.first_observed_at != self.locally_available_at
            || self.locally_available_at >= self.canonical_ingested_at
            || self.discovery_request_set_identity != capture.request_set_identity()
            || self.discovery_capture_content_digest != capture.content_digest()
            || self.discovery_capture_observation_digest != capture.observation_digest()
            || self.canonical_record_count == 0
            || usize::try_from(self.canonical_record_count).ok() != Some(batch.records().len())
            || self.schema_extension_requirement_digest
                != self
                    .provider_semantics
                    .schema_requirement()
                    .requirement_digest()
            || self.provider_semantics_digest != self.provider_semantics.semantics_digest()
            || self
                .provider_semantics
                .observations
                .iter()
                .any(|observation| {
                    observation.response_received_at != self.response_received_at
                        || observation.locally_available_at != self.locally_available_at
                        || observation.canonical_ingested_at != self.canonical_ingested_at
                })
            || self.native_lineage_digest != native_lineage.batch_digest()
            || !validate_discovery_component(
                capture,
                chunk_index,
                object.capture_identity(),
                self.component_request_identity,
                self.component_content_digest,
                self.component_observation_digest,
            )
            || self.candidate_digest != candidate_digest(self)?
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    /// Returns the exact source authority root ingest must rejoin.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision root ingest must rejoin.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the discovery operation that produced the exact source object.
    pub const fn discovery_request_id(&self) -> DiscoveryRequestId {
        self.discovery_request_id
    }

    /// Returns the provider request-plan dataset retained as source provenance.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the analytical dataset root publication must reserve.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns the exact discovered source object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the canonical extraction-record schema identity.
    pub const fn canonical_schema(&self) -> &SourceIdentifier {
        &self.canonical_schema
    }

    /// Returns the current shared canonical record schema version.
    pub const fn canonical_schema_version(&self) -> SchemaVersion {
        self.canonical_schema_version
    }

    /// Returns this request's zero-based deterministic chunk position.
    pub const fn chunk_index(&self) -> u16 {
        self.chunk_index
    }

    /// Returns the exact number of deterministic chunks in the request plan.
    pub const fn total_chunks(&self) -> u16 {
        self.total_chunks
    }

    /// Returns the exact number of canonical macro records root must ingest.
    pub const fn canonical_record_count(&self) -> u32 {
        self.canonical_record_count
    }

    /// Returns the first time this process observed the exact provider content.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns when provider response headers first became available to the transport.
    pub const fn response_received_at(&self) -> Timestamp {
        self.response_received_at
    }

    /// Returns when the complete bounded response became available for point-in-time use.
    pub const fn locally_available_at(&self) -> Timestamp {
        self.locally_available_at
    }

    /// Returns when canonical normalization completed locally.
    pub const fn canonical_ingested_at(&self) -> Timestamp {
        self.canonical_ingested_at
    }

    /// Returns the exact provider-request identity used for the sealed response.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.discovery_request_set_identity
    }

    /// Returns the stable provider-content identity excluding local receipt time.
    pub const fn capture_content_digest(&self) -> EvidenceDigest {
        self.discovery_capture_content_digest
    }

    /// Returns the receive-time-bound provider-capture identity.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.discovery_capture_observation_digest
    }

    /// Returns the stable source/configuration/rights/credential generation of this receipt.
    pub const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }

    /// Returns the validated scope binding between this batch and its immutable raw evidence.
    pub const fn sealed_capture_binding(&self) -> &SealedProviderCaptureBinding {
        &self.sealed_capture_binding
    }

    /// Returns the physical receipt identity of the owned immutable raw-journal seal.
    pub fn sealed_discovery_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_binding.sealed_capture_receipt_digest()
    }

    /// Returns the capture-bound semantic extraction identity root must recompute.
    pub const fn extraction_content_digest(&self) -> EvidenceDigest {
        self.extraction_content_digest
    }

    /// Returns the ordered canonical payload identity for this chunk.
    pub const fn canonical_content_digest(&self) -> EvidenceDigest {
        self.canonical_content_digest
    }

    /// Returns the explicit public marker or registered credential-generation coordinate.
    pub const fn credential_rejoin(&self) -> BlsCredentialRejoin {
        self.credential_rejoin
    }

    /// Returns the exact shared provider-rate declaration identity.
    pub const fn provider_rate_declaration_digest(&self) -> EvidenceDigest {
        self.provider_rate_declaration_digest
    }

    /// Returns the redacted successful doctor evidence bound into activation.
    pub const fn doctor_report_digest(&self) -> EvidenceDigest {
        self.doctor_report_digest
    }

    /// Returns the actual physical doctor-seal identity bound into activation.
    pub const fn sealed_doctor_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_doctor_capture_receipt_digest
    }

    /// Returns the exclusive end of the doctor-backed admission used for this candidate.
    pub const fn activation_expires_at(&self) -> Timestamp {
        self.activation_expires_at
    }

    /// Returns the current sealed-doctor admission root scheduling must rejoin.
    pub const fn activation_candidate_digest(&self) -> EvidenceDigest {
        self.activation_candidate_digest
    }

    /// Returns the non-authoritative identity of this sealed ingest handoff.
    pub const fn candidate_digest(&self) -> EvidenceDigest {
        self.candidate_digest
    }

    /// Returns the exact canonical extraction batch root ingest must publish.
    pub const fn batch(&self) -> &ExtractionBatch {
        self.sealed_capture_binding.batch()
    }

    /// Returns BLS-native semantics root must co-persist and join on typed reads.
    pub const fn provider_semantics(&self) -> &BlsCanonicalProviderSemantics {
        &self.provider_semantics
    }

    /// Returns bounded BLS-native semantics aligned exactly to canonical row ordinals.
    pub const fn native_lineage(&self) -> &ProviderNativeLineageBatch {
        self.sealed_capture_binding.native_lineage()
    }

    /// Consumes the candidate into provider provenance and the sole batch/native/physical authority.
    ///
    /// Response-wide metadata identity and BLS's clock-relative `latest` marker remain provenance;
    /// neither is encoded into row-local native revision semantics.
    pub fn into_root_publication_parts(
        self,
    ) -> (BlsCanonicalProviderSemantics, SealedProviderCaptureBinding) {
        (self.provider_semantics, self.sealed_capture_binding)
    }

    /// Returns the exact input-aligned local-content revision plan root must durably assign.
    ///
    /// This plan contains evidence only; it cannot allocate revision numbers or publish data.
    pub fn revision_plan(&self) -> Result<ExtractionRevisionPlan, BlsSourceError> {
        let batch = self.sealed_capture_binding.batch();
        self.provider_semantics.validate(batch)?;
        self.sealed_capture_binding
            .native_lineage()
            .validate(batch)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
            .map_err(Into::into)
    }
}

fn validate_discovery_component(
    capture: &market_squawk_sources::ProviderCaptureSetReceipt,
    chunk_index: usize,
    object_capture_identity: SourceObjectCaptureIdentity,
    component_request_identity: EvidenceDigest,
    component_content_digest: EvidenceDigest,
    component_observation_digest: EvidenceDigest,
) -> bool {
    let Some(page) = capture.pages().get(chunk_index) else {
        return false;
    };
    let (
        expected_request_identity,
        expected_content_digest,
        expected_observation_digest,
        expected_terminal,
    ) = match capture.terminal() {
        ProviderCaptureTerminalDisposition::StandaloneResponse
            if capture.pages().len() == 1
                && chunk_index == 0
                && capture.request_graph_components().is_empty() =>
        {
            (
                capture.request_set_identity(),
                capture.content_digest(),
                capture.observation_digest(),
                capture.terminal(),
            )
        }
        ProviderCaptureTerminalDisposition::CompleteRequestGraph => {
            let Some(component) = capture.request_graph_components().get(chunk_index) else {
                return false;
            };
            if usize::from(component.ordinal()) != chunk_index
                || usize::from(component.first_page_ordinal()) != chunk_index
                || component.page_count().get() != 1
                || component.total_body_bytes() != page.body_bytes()
                || component.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            {
                return false;
            }
            (
                component.request_set_identity(),
                component.content_digest(),
                component.observation_digest(),
                component.terminal(),
            )
        }
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        | ProviderCaptureTerminalDisposition::StandaloneResponse => return false,
    };
    let Some(page_count) = std::num::NonZeroU16::new(1) else {
        return false;
    };
    object_capture_identity
        == (SourceObjectCaptureIdentity::Paged {
            content_digest: expected_content_digest,
            page_count,
            terminal: expected_terminal,
        })
        && component_request_identity == expected_request_identity
        && component_content_digest == expected_content_digest
        && component_observation_digest == expected_observation_digest
        && page.request_identity() == expected_request_identity
}

fn validate_canonical_batch(
    metadata: &SourceMetadata,
    config: &BlsSourceConfig,
    batch: &ExtractionBatch,
    raw_body_digest: EvidenceDigest,
    first_observed_at: Timestamp,
    locally_available_at: Timestamp,
    canonical_schema: &SourceIdentifier,
) -> Result<Timestamp, BlsSourceError> {
    let mut canonical_ingested_at = None;
    for record in batch.records() {
        let ResearchObservation::Macro(observation) = serde_json::from_slice(record.payload())
            .map_err(|_| BlsSourceError::InvalidPublication)?
        else {
            return Err(BlsSourceError::InvalidPublication);
        };
        let context = observation.context();
        let provenance = context.provenance();
        let ingested_at = provenance.ingested_at();
        let raw_reference_matches = matches!(
            provenance.payload_reference(),
            PayloadReference::ContentHash(value)
                if value.algorithm() == raw_body_digest.algorithm()
                    && value.digest() == raw_body_digest.bytes()
        );
        let metadata_matches = config
            .series_metadata(observation.series().as_str())
            .is_some_and(|series| series.unit() == observation.unit());
        if record.schema() != canonical_schema
            || record.source_id() != metadata.source_id()
            || record.metadata_revision() != metadata.revision()
            || record.dataset() != config.dataset()
            || record.available_at() != Some(first_observed_at)
            || record.published_time().is_some()
            || record.superseded_time().is_some()
            || provenance.source_id() != metadata.source_id()
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.source_identifier() != record.revision()
            || provenance.source_timestamp().is_some()
            || provenance.received_at() != locally_available_at
            || ingested_at < locally_available_at
            || provenance.quality() != DataQuality::OfficialDelayed
            || provenance.availability().conservative_available_at() != Some(first_observed_at)
            || !raw_reference_matches
            || context.time().effective() != record.effective_time()
            || context.time().published().is_some()
            || context.time().revision().get() != 1
            || context.time().superseded().is_some()
            || !metadata_matches
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        match canonical_ingested_at {
            None => canonical_ingested_at = Some(ingested_at),
            Some(expected) if expected == ingested_at => {}
            Some(_) => return Err(BlsSourceError::InvalidPublication),
        }
    }
    canonical_ingested_at.ok_or(BlsSourceError::InvalidPublication)
}

fn canonical_content_digest(batch: &ExtractionBatch) -> Result<EvidenceDigest, BlsSourceError> {
    let records = batch.records();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-canonical-publication-content/v2\0");
    digest.update(
        u32::try_from(records.len())
            .map_err(|_| BlsSourceError::InvalidPublication)?
            .to_be_bytes(),
    );
    for record in records {
        hash_field(&mut digest, record.schema().as_str().as_bytes())?;
        hash_field(&mut digest, record.revision().as_str().as_bytes())?;
        hash_evidence_digest(&mut digest, record.evidence().content_digest());
        hash_field(&mut digest, record.payload())?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn candidate_digest(candidate: &BlsPublicationCandidate) -> Result<EvidenceDigest, BlsSourceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-publication-candidate/v4\0");
    digest.update(candidate.schema_version.to_be_bytes());
    hash_field(&mut digest, candidate.source_id.as_str().as_bytes())?;
    hash_field(
        &mut digest,
        candidate
            .metadata_revision
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    let discovery_request_id = serde_json::to_vec(&candidate.discovery_request_id)
        .map_err(|_| BlsSourceError::InvalidPublication)?;
    hash_field(&mut digest, &discovery_request_id)?;
    for value in [
        &candidate.provider_dataset,
        &candidate.analytical_dataset,
        &candidate.object_id,
        &candidate.canonical_schema,
    ] {
        hash_field(&mut digest, value.as_str().as_bytes())?;
    }
    let canonical_schema_version = serde_json::to_vec(&candidate.canonical_schema_version)
        .map_err(|_| BlsSourceError::InvalidPublication)?;
    hash_field(&mut digest, &canonical_schema_version)?;
    digest.update(candidate.chunk_index.to_be_bytes());
    digest.update(candidate.total_chunks.to_be_bytes());
    digest.update(candidate.canonical_record_count.to_be_bytes());
    digest.update(candidate.first_observed_at.unix_nanos().to_be_bytes());
    digest.update(candidate.response_received_at.unix_nanos().to_be_bytes());
    digest.update(candidate.locally_available_at.unix_nanos().to_be_bytes());
    digest.update(candidate.canonical_ingested_at.unix_nanos().to_be_bytes());
    for value in [
        candidate.schema_extension_requirement_digest,
        candidate.discovery_request_set_identity,
        candidate.discovery_capture_content_digest,
        candidate.discovery_capture_observation_digest,
        candidate.component_request_identity,
        candidate.component_content_digest,
        candidate.component_observation_digest,
        candidate.source_generation_digest,
        candidate
            .sealed_capture_binding
            .sealed_capture_receipt_digest(),
        candidate.extraction_content_digest,
        candidate.canonical_content_digest,
        candidate.provider_semantics_digest,
        candidate.native_lineage_digest,
        candidate.provider_rate_declaration_digest,
        candidate.doctor_report_digest,
        candidate.sealed_doctor_capture_receipt_digest,
        candidate.activation_candidate_digest,
    ] {
        hash_evidence_digest(&mut digest, value);
    }
    hash_credential_rejoin(&mut digest, candidate.credential_rejoin);
    digest.update(candidate.activation_expires_at.unix_nanos().to_be_bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn complete_publication_plan_digest(
    handoff: &BlsCompletePublicationPlanHandoff,
) -> Result<EvidenceDigest, BlsSourceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-complete-publication-plan-handoff/v1\0");
    digest.update(handoff.schema_version.to_be_bytes());
    hash_field(&mut digest, handoff.source_id.as_str().as_bytes())?;
    hash_field(
        &mut digest,
        handoff
            .metadata_revision
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_field(
        &mut digest,
        &serde_json::to_vec(&handoff.discovery_request_id)
            .map_err(|_| BlsSourceError::InvalidPublication)?,
    )?;
    hash_field(&mut digest, handoff.provider_dataset.as_str().as_bytes())?;
    hash_field(&mut digest, handoff.analytical_dataset.as_str().as_bytes())?;
    for value in [
        handoff.request_set_identity,
        handoff.capture_content_digest,
        handoff.capture_observation_digest,
        handoff.source_generation_digest,
        handoff.sealed_capture_receipt_digest,
    ] {
        hash_evidence_digest(&mut digest, value);
    }
    digest.update(handoff.total_chunks.to_be_bytes());
    digest.update(handoff.canonical_record_count.to_be_bytes());
    for candidate in &handoff.candidates {
        digest.update(candidate.chunk_index.to_be_bytes());
        hash_field(&mut digest, candidate.object_id.as_str().as_bytes())?;
        for value in [
            candidate.component_request_identity,
            candidate.component_content_digest,
            candidate.component_observation_digest,
            candidate.candidate_digest,
        ] {
            hash_evidence_digest(&mut digest, value);
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_credential_rejoin(digest: &mut Sha256, value: BlsCredentialRejoin) {
    match value {
        BlsCredentialRejoin::PublicNoCredential => digest.update(b"public-no-credential"),
        BlsCredentialRejoin::RegisteredGeneration(generation) => {
            digest.update(b"registered-generation");
            hash_evidence_digest(digest, generation);
        }
    }
}

fn digest_serialized<T: Serialize + ?Sized>(
    domain: &[u8],
    value: &T,
) -> Result<EvidenceDigest, BlsSourceError> {
    let wire = serde_json::to_vec(value).map_err(|_| BlsSourceError::InvalidPublication)?;
    let mut digest = Sha256::new();
    hash_field(&mut digest, domain)?;
    hash_field(&mut digest, &wire)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_evidence_digest(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update(match value.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(value.bytes());
}

fn hash_field(digest: &mut Sha256, value: &[u8]) -> Result<(), BlsSourceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| BlsSourceError::InvalidPublication)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}
