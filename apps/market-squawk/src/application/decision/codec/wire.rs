use serde::{Deserialize, Serialize};

use super::super::screen_workflow::ScreenJobPlanWire;
use super::candidate::ExecutionWire;
use super::dossier::DossierWire;
use super::screen::ScreenWire;
use super::target::{InvalidationWire, ReviewWire, TargetWire};

pub(super) const WIRE_VERSION: u32 = 1;
pub(super) const KIND_SCREEN: i64 = 1;
pub(super) const KIND_EXECUTION: i64 = 2;
pub(super) const KIND_DOSSIER: i64 = 3;
pub(super) const KIND_TARGET: i64 = 4;
pub(super) const KIND_REVIEW: i64 = 5;
pub(super) const KIND_INVALIDATION: i64 = 6;
pub(super) const KIND_SCREEN_JOB_INPUT: i64 = 7;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireEnvelope {
    pub(super) version: u32,
    pub(super) record: WireRecord,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(super) enum WireRecord {
    Screen(ScreenWire),
    Execution(ExecutionWire),
    Dossier(DossierWire),
    Target(Box<TargetWire>),
    Review(ReviewWire),
    Invalidation(InvalidationWire),
    ScreenJobInput(Box<ScreenJobPlanWire>),
}

impl WireRecord {
    pub(super) const fn kind(&self) -> i64 {
        match self {
            Self::Screen(_) => KIND_SCREEN,
            Self::Execution(_) => KIND_EXECUTION,
            Self::Dossier(_) => KIND_DOSSIER,
            Self::Target(_) => KIND_TARGET,
            Self::Review(_) => KIND_REVIEW,
            Self::Invalidation(_) => KIND_INVALIDATION,
            Self::ScreenJobInput(_) => KIND_SCREEN_JOB_INPUT,
        }
    }
}
