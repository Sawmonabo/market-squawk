//! Strict, versioned wire records for the append-only decision journal.

mod candidate;
mod common;
mod dossier;
mod recovery;
mod screen;
mod target;
mod wire;

use market_squawk_decisions::{
    DecisionDossier, GovernedTargetSet, SavedScreen, ScreenExecution, TargetInvalidation,
    TargetReview,
};
use market_squawk_domain::Timestamp;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use self::candidate::ExecutionWire;
use self::common::revision_key;
use self::dossier::DossierWire;
pub(super) use self::recovery::RecoveryContext;
use self::screen::ScreenWire;
use self::target::{InvalidationWire, ReviewWire, TargetWire};
use self::wire::{
    KIND_DOSSIER, KIND_EXECUTION, KIND_INVALIDATION, KIND_REVIEW, KIND_SCREEN, KIND_TARGET,
    WIRE_VERSION, WireEnvelope, WireRecord,
};
use super::DecisionApplicationError;

#[derive(Debug)]
pub(super) struct EncodedRecord {
    pub(super) kind: i64,
    pub(super) key: String,
    pub(super) payload: Vec<u8>,
    pub(super) digest: [u8; 32],
}

impl EncodedRecord {
    fn try_new(
        kind: i64,
        key: String,
        record: WireRecord,
    ) -> Result<Self, DecisionApplicationError> {
        let payload = encode(&WireEnvelope {
            version: WIRE_VERSION,
            record,
        })?;
        Ok(Self {
            kind,
            key,
            digest: Sha256::digest(&payload).into(),
            payload,
        })
    }
}

pub(super) fn screen(screen: &SavedScreen) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_SCREEN,
        revision_key(
            screen.revision().id().as_str(),
            screen.revision().revision(),
        ),
        WireRecord::Screen(ScreenWire::from(screen)),
    )
}

pub(super) fn execution(
    execution: &ScreenExecution,
    selected_at: Timestamp,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_EXECUTION,
        execution.run().id().as_str().to_owned(),
        WireRecord::Execution(ExecutionWire::from_execution(execution, selected_at)?),
    )
}

pub(super) fn dossier(
    dossier: &DecisionDossier,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_DOSSIER,
        dossier.dossier().id().as_str().to_owned(),
        WireRecord::Dossier(DossierWire::from(dossier)),
    )
}

pub(super) fn target(
    target: &GovernedTargetSet,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_TARGET,
        revision_key(target.target().id().as_str(), target.target().revision()),
        WireRecord::Target(Box::new(TargetWire::from(target))),
    )
}

pub(super) fn review(review: &TargetReview) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_REVIEW,
        review.id().as_str().to_owned(),
        WireRecord::Review(ReviewWire::from(review)),
    )
}

pub(super) fn invalidation(
    invalidation: &TargetInvalidation,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_INVALIDATION,
        invalidation.id().as_str().to_owned(),
        WireRecord::Invalidation(InvalidationWire::from(invalidation)),
    )
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, DecisionApplicationError> {
    serde_json::to_vec(value).map_err(|_error| DecisionApplicationError::InvalidPersistentState)
}
