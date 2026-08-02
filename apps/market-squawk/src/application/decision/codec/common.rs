use std::num::NonZeroU32;

use market_squawk_analytics::{FeatureCompatibility, FeatureKey, FeatureRegistry, StatisticalF64};
use market_squawk_decisions::{DecisionContentDigest, DecisionText, ScreenFeatureBinding};
use market_squawk_domain::{EvidenceDigest, RevisionNumber};
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FeatureBindingWire {
    name: String,
    version: u32,
    semantic_digest: [u8; 32],
}

impl From<&ScreenFeatureBinding> for FeatureBindingWire {
    fn from(value: &ScreenFeatureBinding) -> Self {
        Self {
            name: value.key().name().to_owned(),
            version: value.key().version().get(),
            semantic_digest: value.semantic_digest().as_bytes(),
        }
    }
}

impl FeatureBindingWire {
    pub(super) fn decode(
        &self,
        registry: &FeatureRegistry,
    ) -> Result<ScreenFeatureBinding, DecisionApplicationError> {
        let version = NonZeroU32::new(self.version)
            .ok_or(DecisionApplicationError::InvalidPersistentState)?;
        let key = FeatureKey::try_new(&self.name, version)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        let metadata = registry
            .try_resolve(&key, FeatureCompatibility::PointInTime)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        if metadata.semantic_digest().as_bytes() != self.semantic_digest {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(ScreenFeatureBinding::new(key, metadata.semantic_digest()))
    }
}

pub(super) fn statistical(bits: u64) -> Result<StatisticalF64, DecisionApplicationError> {
    StatisticalF64::try_new(f64::from_bits(bits))
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
}

pub(super) fn revision(value: u32) -> Result<RevisionNumber, DecisionApplicationError> {
    RevisionNumber::new(value).map_err(|_error| DecisionApplicationError::InvalidPersistentState)
}

pub(super) fn revision_key(id: &str, revision: RevisionNumber) -> String {
    format!("{id}:{}", revision.get())
}

pub(super) fn content_digest(
    value: EvidenceDigest,
) -> Result<DecisionContentDigest, DecisionApplicationError> {
    DecisionContentDigest::try_new(value)
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
}

pub(super) fn decision_text(value: &str) -> Result<DecisionText, DecisionApplicationError> {
    DecisionText::try_new(value).map_err(|_error| DecisionApplicationError::InvalidPersistentState)
}
