//! Canonical wire identities for immutable analytical manifests.

use std::cmp::Ordering;

use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{SchemaVersion, SourceId};
use serde::{Deserialize, Serialize};

use super::{RecipeError, decode_digest, encode_digest};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::application::analysis::backtest::input_authority) struct ManifestWire {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: String,
    content_hash: String,
}

impl ManifestWire {
    pub(super) fn from_manifest(manifest: &DatasetManifestRef) -> Self {
        Self {
            dataset_id: manifest.dataset_id().as_str().to_owned(),
            manifest_version: manifest.manifest_version(),
            schema_name: manifest.schema().name().to_owned(),
            schema_version: manifest.schema_version().get(),
            schema_fingerprint: encode_digest(manifest.schema().fingerprint()),
            content_hash: encode_digest(manifest.content_hash().bytes()),
        }
    }

    pub(super) fn to_manifest(&self) -> Result<DatasetManifestRef, RecipeError> {
        let schema = DatasetSchemaRef::try_new(
            &self.schema_name,
            SchemaVersion::new(self.schema_version).map_err(|_| RecipeError::Invalid)?,
            decode_digest(&self.schema_fingerprint)?,
        )
        .map_err(|_| RecipeError::Invalid)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| RecipeError::Invalid)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset_id.as_str()).map_err(|_| RecipeError::Invalid)?,
            self.manifest_version,
            schema,
            Sha256Digest::new(decode_digest(&self.content_hash)?),
        )
        .map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn coordinate_cmp(&self, other: &Self) -> Ordering {
        self.dataset_id
            .cmp(&other.dataset_id)
            .then_with(|| self.manifest_version.cmp(&other.manifest_version))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::application::analysis::backtest::input_authority) struct ManifestAuthorityWire {
    pub(in crate::application::analysis::backtest::input_authority) manifest: ManifestWire,
    pub(in crate::application::analysis::backtest::input_authority) source_id: SourceId,
}

impl ManifestAuthorityWire {
    pub(in crate::application::analysis::backtest::input_authority) fn new(
        manifest: &DatasetManifestRef,
        source_id: SourceId,
    ) -> Self {
        Self {
            manifest: ManifestWire::from_manifest(manifest),
            source_id,
        }
    }
}

pub(in crate::application::analysis::backtest::input_authority) fn validate_manifest_authorities(
    authorities: &[ManifestAuthorityWire],
) -> Result<(), RecipeError> {
    if authorities.is_empty() {
        return Err(RecipeError::Invalid);
    }
    for authority in authorities {
        authority.manifest.to_manifest()?;
    }
    if authorities
        .windows(2)
        .any(|pair| pair[0].manifest.coordinate_cmp(&pair[1].manifest) != Ordering::Less)
    {
        return Err(RecipeError::Invalid);
    }
    Ok(())
}

pub(in crate::application::analysis::backtest::input_authority) fn sort_manifest_authorities(
    authorities: &mut [ManifestAuthorityWire],
) {
    authorities.sort_unstable_by(|left, right| {
        left.manifest
            .coordinate_cmp(&right.manifest)
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
}
