//! Shared serving-artifact codec and content-bound forecast source admission.

use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry,
    FeatureDatasetInputCoordinate, FeatureDatasetInputEpoch, PinnedQueryOutput,
};
use market_squawk_domain::{MarketBarAdjustment, MarketBarObservation, SchemaVersion, SourceId};
use serde::{Deserialize, Serialize};

use super::*;

/// Complete original derived parents plus the actual serving input generation.
pub const MAX_FORECAST_SERVING_PARENTS: usize =
    market_squawk_data::MAX_DERIVED_GENERATION_PARENTS + 1;

/// Current serving artifact's exact schema-reference wire fields; this carries no authority alone.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForecastArtifactSchemaRecord {
    /// Registered schema name.
    pub name: String,
    /// Registered schema version.
    pub version: u16,
    /// Exact lowercase hexadecimal fingerprint.
    pub fingerprint: String,
}

/// Shared existing artifact manifest encoding; a record alone grants no source authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForecastArtifactManifestRecord {
    /// Dataset identity.
    pub dataset: String,
    /// Exact immutable manifest version.
    pub manifest_version: u64,
    /// Exact registered schema.
    pub schema: ForecastArtifactSchemaRecord,
    /// Exact lowercase hexadecimal content identity.
    pub content_hash: String,
}

impl ForecastArtifactManifestRecord {
    /// Encodes an actual retained source generation under the shared artifact codec.
    #[must_use]
    pub fn from_manifest(manifest: &DatasetManifestRef) -> Self {
        Self {
            dataset: manifest.dataset_id().as_str().to_owned(),
            manifest_version: manifest.manifest_version(),
            schema: ForecastArtifactSchemaRecord {
                name: manifest.schema().name().to_owned(),
                version: manifest.schema_version().get(),
                fingerprint: hex(manifest.schema().fingerprint()),
            },
            content_hash: hex(manifest.content_hash().bytes()),
        }
    }

    /// Strictly decodes the current registered manifest representation without minting authority.
    pub fn typed(&self) -> Result<DatasetManifestRef, ForecastError> {
        let fingerprint = parse_hash(&self.schema.fingerprint)?;
        let content_hash = parse_hash(&self.content_hash)?;
        let schema = DatasetSchemaRef::try_new(
            &self.schema.name,
            SchemaVersion::new(self.schema.version).map_err(|_| ForecastError::InvalidVintage)?,
            fingerprint.bytes(),
        )
        .map_err(|_| ForecastError::InvalidVintage)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| ForecastError::InvalidVintage)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset.as_str())
                .map_err(|_| ForecastError::InvalidVintage)?,
            self.manifest_version,
            schema,
            content_hash,
        )
        .map_err(|_| ForecastError::InvalidVintage)
    }
}

/// Current complete serving field shared by forecast publication, recovery, and valuation.
///
/// Public wire fields support the existing publisher. Only [`AuthenticatedForecastServingBinding`]
/// can attest that these fields came from the exact artifact bound to a sealed model output.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForecastServingArtifactRecord {
    /// Primary actual market source generation.
    pub manifest: ForecastArtifactManifestRecord,
    /// Complete ordered market and macro source generations.
    pub parent_manifests: Vec<ForecastArtifactManifestRecord>,
    /// Exact source namespace of the selected origin bar.
    pub source_id: String,
    /// Source object graph identity.
    pub object_graph_sha256: String,
    /// Source selection identity.
    pub selection_sha256: String,
    /// Exact selected source result identity.
    pub result_sha256: String,
    /// Original source knowledge cutoff.
    pub knowledge_cutoff_unix_nanos: i64,
    /// Earlier observed input coordinate.
    pub prior_observed_at_unix_nanos: Option<i64>,
    /// Last observed input coordinate.
    pub observed_through_unix_nanos: Option<i64>,
    /// Exact derived feature and complete macro evidence identity.
    pub feature_sha256: String,
    /// Actual selected price origin when the model target is an arithmetic return.
    pub origin_bar: Option<MarketBarObservation>,
    /// Exact original data-owned fiscal input; this inert record grants no source authority.
    pub financial_input: Option<ForecastFinancialServingRecord>,
    /// Exact current feature-only source epoch; an inert record grants no authority.
    pub current_price_input: Option<ForecastCurrentPriceServingRecord>,
}

/// Durable reference and exact canonical epoch bytes for a native monetary input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForecastFinancialServingRecord {
    pub example_id: String,
    pub production_identity_sha256: String,
    pub production_receipt_sha256: String,
    pub input_epoch_json: String,
}
impl ForecastFinancialServingRecord {
    /// Encodes an actual source-authenticated financial coordinate.
    pub fn from_coordinate(
        coordinate: FeatureDatasetInputCoordinate<'_>,
    ) -> Result<Self, ForecastError> {
        if coordinate.epoch().financial_period().is_none() {
            return Err(ForecastError::InvalidRequest);
        }
        let bytes = coordinate
            .epoch()
            .canonical_bytes()
            .map_err(|_| ForecastError::InvalidRequest)?;
        let receipt = coordinate.dataset().production_receipt();
        Ok(Self {
            example_id: coordinate.epoch().example_id().to_owned(),
            production_identity_sha256: hex(receipt.production_identity().bytes()),
            production_receipt_sha256: hex(receipt.receipt_sha256().bytes()),
            input_epoch_json: String::from_utf8(bytes)
                .map_err(|_| ForecastError::InvalidRequest)?,
        })
    }
    fn validate(&self) -> bool {
        !self.example_id.is_empty()
            && self.example_id.len() <= 128
            && !self.input_epoch_json.is_empty()
            && self.input_epoch_json.len() <= 64 * 1024
            && parse_hash(&self.production_identity_sha256).is_ok()
            && parse_hash(&self.production_receipt_sha256).is_ok()
    }
    /// Rechecks exact canonical bytes against the genuine current coordinate.
    pub fn matches_coordinate(&self, coordinate: FeatureDatasetInputCoordinate<'_>) -> bool {
        self.validate() && Self::from_coordinate(coordinate).is_ok_and(|actual| actual == *self)
    }

    /// Exact input epoch, ordered feature values, and retained row lineage identity.
    pub fn feature_identity(
        coordinate: FeatureDatasetInputCoordinate<'_>,
    ) -> Result<Sha256Digest, ForecastError> {
        let bytes = coordinate
            .epoch()
            .canonical_bytes()
            .map_err(|_| ForecastError::InvalidRequest)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/native-financial-forecast-input/v1\0");
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
        hash.update((coordinate.rows().len() as u64).to_be_bytes());
        for row in coordinate.rows() {
            hash.update((row.component_name().len() as u64).to_be_bytes());
            hash.update(row.component_name().as_bytes());
            hash.update(row.component_version().to_be_bytes());
            hash.update(row.lineage_sha256().bytes());
            match row.value() {
                market_squawk_data::ForecastFeatureValue::Float(value) if value.is_finite() => {
                    hash.update([1]);
                    hash.update(value.to_bits().to_be_bytes());
                }
                market_squawk_data::ForecastFeatureValue::Decimal { mantissa, scale } => {
                    hash.update([2]);
                    hash.update(mantissa.to_be_bytes());
                    hash.update([*scale]);
                }
                _ => return Err(ForecastError::InvalidRequest),
            }
        }
        Ok(Sha256Digest::new(hash.finalize().into()))
    }
}

impl ForecastServingArtifactRecord {
    /// Checks the closed field contract; successful decoding alone is not artifact authority.
    #[must_use]
    pub fn validate(&self) -> bool {
        self.decode().is_ok()
    }

    fn decode(&self) -> Result<DecodedServingBinding, ForecastError> {
        let manifest = self.manifest.typed()?;
        if self.parent_manifests.is_empty()
            || self.parent_manifests.len() > MAX_FORECAST_SERVING_PARENTS
        {
            return Err(ForecastError::InvalidVintage);
        }
        let parents = self
            .parent_manifests
            .iter()
            .map(ForecastArtifactManifestRecord::typed)
            .collect::<Result<Vec<_>, _>>()?;
        if parents.iter().filter(|parent| **parent == manifest).count() != 1
            || parents.iter().enumerate().any(|(index, parent)| {
                parents[..index].iter().any(|prior| {
                    prior.dataset_id() == parent.dataset_id()
                        && prior.manifest_version() == parent.manifest_version()
                })
            })
            || match (
                &self.financial_input,
                &self.current_price_input,
                self.prior_observed_at_unix_nanos,
                self.observed_through_unix_nanos,
            ) {
                (None, None, Some(prior), Some(observed)) => {
                    prior >= observed || observed > self.knowledge_cutoff_unix_nanos
                }
                (Some(financial), None, None, None) => {
                    !financial.validate() || self.origin_bar.is_some()
                }
                (None, Some(current), None, Some(observed)) => {
                    !current.validate()
                        || observed > self.knowledge_cutoff_unix_nanos
                        || self.origin_bar.is_none()
                }
                _ => true,
            }
        {
            return Err(ForecastError::InvalidVintage);
        }
        let source_id = SourceId::try_from(self.source_id.as_str())
            .map_err(|_| ForecastError::InvalidVintage)?;
        let object_graph_sha256 = parse_hash(&self.object_graph_sha256)?;
        let selection_sha256 = parse_hash(&self.selection_sha256)?;
        let result_sha256 = parse_hash(&self.result_sha256)?;
        let feature_sha256 = parse_hash(&self.feature_sha256)?;
        if let Some(bar) = &self.origin_bar
            && self.current_price_input.is_none()
        {
            validate_forecast_price_origin(
                bar,
                &source_id,
                Timestamp::from_unix_nanos(
                    self.observed_through_unix_nanos
                        .ok_or(ForecastError::InvalidVintage)?,
                ),
                Timestamp::from_unix_nanos(self.knowledge_cutoff_unix_nanos),
            )?;
        }
        Ok(DecodedServingBinding {
            manifest,
            parents: parents.into_boxed_slice(),
            source_id,
            object_graph_sha256,
            selection_sha256,
            result_sha256,
            feature_sha256,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecodedServingBinding {
    manifest: DatasetManifestRef,
    parents: Box<[DatasetManifestRef]>,
    source_id: SourceId,
    object_graph_sha256: Sha256Digest,
    selection_sha256: Sha256Digest,
    result_sha256: Sha256Digest,
    feature_sha256: Sha256Digest,
}

/// Complete original serving source binding authenticated by the actual forecast artifact bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedForecastServingBinding {
    output_identity: Sha256Digest,
    forecast_artifact_hash: Sha256Digest,
    record: ForecastServingArtifactRecord,
    decoded: DecodedServingBinding,
    financial_epoch: Option<Box<FeatureDatasetInputEpoch>>,
    current_price_epoch: Option<Box<FeatureDatasetInputEpoch>>,
}

impl AuthenticatedForecastServingBinding {
    /// Hashes the complete controlled payload before extracting the shared serving field.
    ///
    /// This cannot bind caller-selected prices or parent lists to a different forecast artifact.
    pub fn from_artifact(
        bytes: &[u8],
        output: &ForecastTerminalDistribution,
    ) -> Result<Self, ForecastError> {
        if bytes.is_empty()
            || bytes.len() > 16 * 1024 * 1024
            || <[u8; 32]>::from(Sha256::digest(bytes)) != output.forecast_artifact_hash().bytes()
        {
            return Err(ForecastError::InvalidVintage);
        }
        #[derive(Deserialize)]
        struct Payload {
            #[serde(rename = "servingEvidence")]
            serving: ForecastServingArtifactRecord,
        }
        let payload: Payload =
            serde_json::from_slice(bytes).map_err(|_| ForecastError::InvalidVintage)?;
        let decoded = payload.serving.decode()?;
        if payload.serving.current_price_input.is_some()
            || payload.serving.financial_input.is_some()
            || output.financial_target().is_some()
            || payload.serving.observed_through_unix_nanos
                != output.observed_through().map(|time| time.unix_nanos())
            || payload.serving.knowledge_cutoff_unix_nanos < output.available_at().unix_nanos()
            || payload.serving.knowledge_cutoff_unix_nanos > output.published_at().unix_nanos()
            || payload.serving.origin_bar.as_ref().is_some_and(|bar| {
                bar.context().provenance().instrument_id() != Some(output.instrument_id())
            })
            || (output.output_binding().measurement() == ForecastMeasurement::Return
                && payload.serving.origin_bar.is_none())
        {
            return Err(ForecastError::InvalidVintage);
        }
        Ok(Self {
            output_identity: output.identity(),
            forecast_artifact_hash: output.forecast_artifact_hash(),
            record: payload.serving,
            decoded,
            financial_epoch: None,
            current_price_epoch: None,
        })
    }

    /// Authenticates a fiscal serving source using the exact artifact and a genuine data reread.
    pub fn from_financial_artifact(
        bytes: &[u8],
        output: &ForecastTerminalDistribution,
        coordinate: FeatureDatasetInputCoordinate<'_>,
        query: &PinnedQueryOutput,
    ) -> Result<Self, ForecastError> {
        if bytes.is_empty()
            || bytes.len() > 16 * 1024 * 1024
            || <[u8; 32]>::from(Sha256::digest(bytes)) != output.forecast_artifact_hash().bytes()
        {
            return Err(ForecastError::InvalidVintage);
        }
        #[derive(Deserialize)]
        struct Payload {
            #[serde(rename = "servingEvidence")]
            serving: ForecastServingArtifactRecord,
        }
        let payload: Payload =
            serde_json::from_slice(bytes).map_err(|_| ForecastError::InvalidVintage)?;
        let decoded = payload.serving.decode()?;
        let epoch = coordinate.epoch();
        if payload
            .serving
            .financial_input
            .as_ref()
            .is_none_or(|value| !value.matches_coordinate(coordinate))
            || decoded.manifest != *coordinate.dataset().generation().manifest()
            || query.manifest() != &decoded.manifest
            || decoded.object_graph_sha256.bytes() != query.object_graph_digest().bytes()
            || decoded.selection_sha256.bytes() != query.query_identity().bytes()
            || decoded.result_sha256.bytes() != query.result_digest().bytes()
            || decoded.feature_sha256
                != ForecastFinancialServingRecord::feature_identity(coordinate)?
            || &decoded.source_id != coordinate.dataset().generation().source_id()
            || output.financial_target() != epoch.financial_period()
            || output.financial_target().is_none()
            || output.instrument_id() != epoch.instrument_id()
            || output.available_at() != epoch.source_selection_as_of()
            || payload.serving.knowledge_cutoff_unix_nanos
                != epoch.source_selection_as_of().unix_nanos()
            || output.published_at() < output.available_at()
        {
            return Err(ForecastError::InvalidVintage);
        }
        let measurement = match epoch.financial_measurement() {
            Some(market_squawk_data::FeatureLabelMeasurement::FinancialAmount {
                currency,
                role,
                basis,
                share_convention,
            }) => ForecastMeasurement::FinancialAmount {
                currency,
                role,
                basis,
                share_convention,
            },
            _ => return Err(ForecastError::InvalidVintage),
        };
        if output.output_binding().measurement() != measurement {
            return Err(ForecastError::InvalidVintage);
        }
        let expected_parents: Vec<_> =
            std::iter::once(coordinate.dataset().generation().manifest())
                .chain(
                    coordinate
                        .dataset()
                        .generation()
                        .parents()
                        .iter()
                        .map(|parent| parent.manifest()),
                )
                .collect();
        if decoded.parents.iter().ne(expected_parents.into_iter()) {
            return Err(ForecastError::InvalidVintage);
        }
        Ok(Self {
            output_identity: output.identity(),
            forecast_artifact_hash: output.forecast_artifact_hash(),
            record: payload.serving,
            decoded,
            financial_epoch: Some(Box::new(epoch.clone())),
            current_price_epoch: None,
        })
    }

    /// Authenticates the exact current label-free input against its immutable native reread.
    pub fn from_current_price_artifact(
        bytes: &[u8],
        output: &ForecastTerminalDistribution,
        coordinate: FeatureDatasetInputCoordinate<'_>,
        query: &PinnedQueryOutput,
    ) -> Result<Self, ForecastError> {
        if bytes.is_empty()
            || bytes.len() > 16 * 1024 * 1024
            || <[u8; 32]>::from(Sha256::digest(bytes)) != output.forecast_artifact_hash().bytes()
        {
            return Err(ForecastError::InvalidVintage);
        }
        #[derive(Deserialize)]
        struct Payload {
            #[serde(rename = "servingEvidence")]
            serving: ForecastServingArtifactRecord,
        }
        let payload: Payload =
            serde_json::from_slice(bytes).map_err(|_| ForecastError::InvalidVintage)?;
        let decoded = payload.serving.decode()?;
        let epoch = coordinate.epoch();
        let expected_parents: Vec<_> =
            std::iter::once(coordinate.dataset().generation().manifest())
                .chain(
                    coordinate
                        .dataset()
                        .generation()
                        .parents()
                        .iter()
                        .map(|parent| parent.manifest()),
                )
                .collect();
        let basis_matches = matches!(output.output_binding().target(),
            ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos, origin_basis }
                if Some(origin_basis) == epoch.fixed_horizon_origin_basis()
                && epoch.target_at().zip(epoch.target_origin()).is_some_and(|(target, origin)|
                    target.unix_nanos().checked_sub(origin.unix_nanos()) == i64::try_from(horizon_nanos.get()).ok()));
        if payload
            .serving
            .current_price_input
            .as_ref()
            .is_none_or(|value| !value.matches_coordinate(coordinate))
            || payload.serving.financial_input.is_some()
            || decoded.manifest != *coordinate.dataset().generation().manifest()
            || query.manifest() != &decoded.manifest
            || decoded.object_graph_sha256.bytes() != query.object_graph_digest().bytes()
            || decoded.selection_sha256.bytes() != query.query_identity().bytes()
            || decoded.result_sha256.bytes() != query.result_digest().bytes()
            || decoded.feature_sha256
                != ForecastCurrentPriceServingRecord::feature_identity(coordinate)?
            || &decoded.source_id != coordinate.dataset().generation().source_id()
            || decoded.parents.iter().ne(expected_parents.into_iter())
            || output.instrument_id() != epoch.instrument_id()
            || output.observed_through() != epoch.target_origin()
            || payload.serving.observed_through_unix_nanos
                != epoch.target_origin().map(Timestamp::unix_nanos)
            || output.target_at() != epoch.target_at()
            || output.available_at() != epoch.source_selection_as_of()
            || payload.serving.knowledge_cutoff_unix_nanos
                != epoch.source_selection_as_of().unix_nanos()
            || payload.serving.origin_bar.as_ref() != epoch.market_bar()
            || output.published_at() < output.available_at()
            || output.output_binding().measurement() != ForecastMeasurement::Return
            || !basis_matches
        {
            return Err(ForecastError::InvalidVintage);
        }
        Ok(Self {
            output_identity: output.identity(),
            forecast_artifact_hash: output.forecast_artifact_hash(),
            record: payload.serving,
            decoded,
            financial_epoch: None,
            current_price_epoch: Some(Box::new(epoch.clone())),
        })
    }

    /// Original current price epoch, present only after the exact artifact and source reread agree.
    pub fn current_price_epoch(&self) -> Option<&FeatureDatasetInputEpoch> {
        self.current_price_epoch.as_deref()
    }

    /// Original source-authenticated financial epoch, absent for a market-price source.
    pub fn financial_epoch(&self) -> Option<&FeatureDatasetInputEpoch> {
        self.financial_epoch.as_deref()
    }

    /// Exact sealed terminal output bound by this source proof.
    #[must_use]
    pub const fn output_identity(&self) -> Sha256Digest {
        self.output_identity
    }
    /// Exact complete controlled forecast payload.
    #[must_use]
    pub const fn forecast_artifact_hash(&self) -> Sha256Digest {
        self.forecast_artifact_hash
    }
    /// Actual primary source generation.
    #[must_use]
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.decoded.manifest
    }
    /// Every actual serving source generation, including macro parents.
    #[must_use]
    pub fn parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.decoded.parents
    }
    /// Actual market source namespace.
    #[must_use]
    pub const fn source_id(&self) -> &SourceId {
        &self.decoded.source_id
    }
    /// Original source object graph identity.
    #[must_use]
    pub const fn object_graph_sha256(&self) -> Sha256Digest {
        self.decoded.object_graph_sha256
    }
    /// Original source selection identity.
    #[must_use]
    pub const fn selection_sha256(&self) -> Sha256Digest {
        self.decoded.selection_sha256
    }
    /// Original selected source result identity.
    #[must_use]
    pub const fn result_sha256(&self) -> Sha256Digest {
        self.decoded.result_sha256
    }
    /// Original derived feature identity.
    #[must_use]
    pub const fn feature_sha256(&self) -> Sha256Digest {
        self.decoded.feature_sha256
    }
    /// Original source knowledge cutoff.
    #[must_use]
    pub fn knowledge_cutoff(&self) -> Timestamp {
        Timestamp::from_unix_nanos(self.record.knowledge_cutoff_unix_nanos)
    }
    /// Original prior completed bar close coordinate.
    #[must_use]
    pub fn prior_observed_at(&self) -> Option<Timestamp> {
        self.record
            .prior_observed_at_unix_nanos
            .map(Timestamp::from_unix_nanos)
    }
    /// Original terminal completed bar close coordinate.
    #[must_use]
    pub fn observed_through(&self) -> Option<Timestamp> {
        self.record
            .observed_through_unix_nanos
            .map(Timestamp::from_unix_nanos)
    }
    /// Exact original selected causal price, never a caller-selected replacement.
    #[must_use]
    pub const fn origin_bar(&self) -> Option<&MarketBarObservation> {
        self.record.origin_bar.as_ref()
    }
}

/// Shared original-price chronology and adjustment checks for the artifact and its publisher.
pub fn validate_forecast_price_origin(
    bar: &MarketBarObservation,
    source_id: &SourceId,
    observed_through: Timestamp,
    knowledge_cutoff: Timestamp,
) -> Result<(), ForecastError> {
    if bar.context().provenance().source_id() != source_id
        || bar.completed_at() != Some(observed_through)
        || bar
            .completed_at()
            .is_none_or(|time| time > knowledge_cutoff)
        || bar.adjustment() != MarketBarAdjustment::Split
        || matches!(
            bar.context().provenance().quality(),
            DataQuality::Modeled | DataQuality::Stale | DataQuality::Quarantined
        )
        || bar.close().amount().scale() > u32::from(MAX_FORECAST_DECIMAL_SCALE)
        || bar
            .context()
            .provenance()
            .availability()
            .conservative_available_at()
            .is_none_or(|available| available > knowledge_cutoff)
    {
        return Err(ForecastError::InvalidVintage);
    }
    Ok(())
}

fn parse_hash(value: &str) -> Result<Sha256Digest, ForecastError> {
    if value.len() != 64 {
        return Err(ForecastError::InvalidVintage);
    }
    let mut bytes = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let digit = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(ForecastError::InvalidVintage),
        };
        bytes[index] = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    if bytes == [0; 32] {
        return Err(ForecastError::InvalidVintage);
    }
    Ok(Sha256Digest::new(bytes))
}

pub(super) fn hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(64);
    for byte in bytes {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    result
}
