//! Canonical restart-safe corporate-action recipe encoding.

use std::num::{NonZeroU32, NonZeroUsize};

use market_squawk_data::{
    CorporateActionAdjustment, CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy,
    CorporateActionRecord,
};
use market_squawk_domain::{CorporateActionObservation, EvidenceDigest, Timestamp};
use serde::{Deserialize, Serialize};

use super::{RecipeError, manifest::ManifestWire};

#[derive(Clone, Debug)]
pub struct GovernedBacktestCorporateActionsInput {
    pub policy: CorporateActionPolicy,
    pub knowledge_cutoff: Timestamp,
    pub valuation_cutoff: Timestamp,
    pub actions: Vec<CorporateActionRecord>,
    pub limits: CorporateActionLimits,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CorporateActionsWire {
    adjustment: CorporateActionAdjustmentWire,
    policy_version: u32,
    knowledge_cutoff_unix_nanos: i64,
    valuation_cutoff_unix_nanos: i64,
    actions: Vec<CorporateActionRecordWire>,
    maximum_actions: usize,
    maximum_retained_bytes: usize,
}

impl CorporateActionsWire {
    pub(super) fn try_from_input(
        input: GovernedBacktestCorporateActionsInput,
    ) -> Result<Self, RecipeError> {
        let plan = CorporateActionPlan::try_build(
            input.policy,
            input.knowledge_cutoff,
            input.valuation_cutoff,
            input.actions.clone(),
            input.limits,
        )
        .map_err(|_| RecipeError::Invalid)?;
        drop(plan);
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(input.actions.len())
            .map_err(|_| RecipeError::ResourceExhausted)?;
        for record in input.actions {
            let wire = CorporateActionRecordWire::from_record(&record);
            let key = serde_json::to_vec(&wire).map_err(|_| RecipeError::Invalid)?;
            canonical.push((key, wire));
        }
        canonical.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let actions = canonical.into_iter().map(|(_, wire)| wire).collect();
        Ok(Self {
            adjustment: input.policy.adjustment().into(),
            policy_version: input.policy.version().get(),
            knowledge_cutoff_unix_nanos: input.knowledge_cutoff.unix_nanos(),
            valuation_cutoff_unix_nanos: input.valuation_cutoff.unix_nanos(),
            actions,
            maximum_actions: input.limits.max_actions().get(),
            maximum_retained_bytes: input.limits.max_retained_bytes().get(),
        })
    }

    pub(super) fn build(&self) -> Result<CorporateActionPlan, RecipeError> {
        let policy = CorporateActionPolicy::new(
            self.adjustment.into(),
            NonZeroU32::new(self.policy_version).ok_or(RecipeError::Invalid)?,
        );
        let limits = CorporateActionLimits::try_new(
            NonZeroUsize::new(self.maximum_actions).ok_or(RecipeError::Invalid)?,
            NonZeroUsize::new(self.maximum_retained_bytes).ok_or(RecipeError::Invalid)?,
        )
        .map_err(|_| RecipeError::Invalid)?;
        let mut actions = Vec::new();
        actions
            .try_reserve_exact(self.actions.len())
            .map_err(|_| RecipeError::ResourceExhausted)?;
        let mut previous = None;
        for wire in &self.actions {
            let encoded = serde_json::to_vec(wire).map_err(|_| RecipeError::Invalid)?;
            if previous.as_ref().is_some_and(|prior| prior > &encoded) {
                return Err(RecipeError::Invalid);
            }
            previous = Some(encoded);
            actions.push(wire.to_record()?);
        }
        CorporateActionPlan::try_build(
            policy,
            Timestamp::from_unix_nanos(self.knowledge_cutoff_unix_nanos),
            Timestamp::from_unix_nanos(self.valuation_cutoff_unix_nanos),
            actions,
            limits,
        )
        .map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn source_manifests(
        &self,
    ) -> Result<Vec<market_squawk_data::DatasetManifestRef>, RecipeError> {
        self.actions
            .iter()
            .map(|record| record.source_manifest.to_manifest())
            .collect()
    }

    pub(super) fn into_input(self) -> Result<GovernedBacktestCorporateActionsInput, RecipeError> {
        let policy = CorporateActionPolicy::new(
            self.adjustment.into(),
            NonZeroU32::new(self.policy_version).ok_or(RecipeError::Invalid)?,
        );
        let limits = CorporateActionLimits::try_new(
            NonZeroUsize::new(self.maximum_actions).ok_or(RecipeError::Invalid)?,
            NonZeroUsize::new(self.maximum_retained_bytes).ok_or(RecipeError::Invalid)?,
        )
        .map_err(|_| RecipeError::Invalid)?;
        let mut actions = Vec::new();
        actions
            .try_reserve_exact(self.actions.len())
            .map_err(|_| RecipeError::ResourceExhausted)?;
        for action in self.actions {
            actions.push(action.into_record()?);
        }
        Ok(GovernedBacktestCorporateActionsInput {
            policy,
            knowledge_cutoff: Timestamp::from_unix_nanos(self.knowledge_cutoff_unix_nanos),
            valuation_cutoff: Timestamp::from_unix_nanos(self.valuation_cutoff_unix_nanos),
            actions,
            limits,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CorporateActionAdjustmentWire {
    Raw,
    SplitAdjusted,
    TotalReturn,
}

impl From<CorporateActionAdjustment> for CorporateActionAdjustmentWire {
    fn from(value: CorporateActionAdjustment) -> Self {
        match value {
            CorporateActionAdjustment::Raw => Self::Raw,
            CorporateActionAdjustment::SplitAdjusted => Self::SplitAdjusted,
            CorporateActionAdjustment::TotalReturn => Self::TotalReturn,
        }
    }
}

impl From<CorporateActionAdjustmentWire> for CorporateActionAdjustment {
    fn from(value: CorporateActionAdjustmentWire) -> Self {
        match value {
            CorporateActionAdjustmentWire::Raw => Self::Raw,
            CorporateActionAdjustmentWire::SplitAdjusted => Self::SplitAdjusted,
            CorporateActionAdjustmentWire::TotalReturn => Self::TotalReturn,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CorporateActionRecordWire {
    observation: CorporateActionObservation,
    source_manifest: ManifestWire,
    evidence_digest: EvidenceDigest,
}

impl CorporateActionRecordWire {
    fn from_record(record: &CorporateActionRecord) -> Self {
        Self {
            observation: record.observation().clone(),
            source_manifest: ManifestWire::from_manifest(record.source_manifest()),
            evidence_digest: record.evidence_digest(),
        }
    }

    fn to_record(&self) -> Result<CorporateActionRecord, RecipeError> {
        Ok(CorporateActionRecord::new(
            self.observation.clone(),
            self.source_manifest.to_manifest()?,
            self.evidence_digest,
        ))
    }

    fn into_record(self) -> Result<CorporateActionRecord, RecipeError> {
        Ok(CorporateActionRecord::new(
            self.observation,
            self.source_manifest.to_manifest()?,
            self.evidence_digest,
        ))
    }
}
