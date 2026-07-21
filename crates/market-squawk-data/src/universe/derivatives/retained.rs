//! Checked retained-memory accounting for derivative-universe results.

use std::mem::size_of;

use market_squawk_domain::{AvailabilityEvidence, InstrumentId};

use super::{
    ContractRollEvidence, DerivativeCivilDate, DerivativeDecisionRecord, DerivativeLifecycle,
    DerivativeLifecycleEvidence,
};
use crate::{DatasetManifestRef, UniverseError, UniverseSnapshot};

pub(super) struct RetainedCapacities {
    pub(super) lifecycle: usize,
    pub(super) civil_dates: usize,
    pub(super) rolls: usize,
    pub(super) decisions: usize,
    pub(super) active: usize,
}

pub(super) fn retained_bytes(
    base: &UniverseSnapshot,
    lifecycle: &[DerivativeLifecycleEvidence],
    civil_dates: &[DerivativeCivilDate],
    rolls: &[ContractRollEvidence],
    capacities: RetainedCapacities,
) -> Result<usize, UniverseError> {
    let mut retained = base.retained_bytes();
    for bytes in [
        capacities
            .lifecycle
            .checked_mul(size_of::<DerivativeLifecycleEvidence>()),
        capacities
            .civil_dates
            .checked_mul(size_of::<DerivativeCivilDate>()),
        capacities
            .rolls
            .checked_mul(size_of::<ContractRollEvidence>()),
        capacities
            .decisions
            .checked_mul(size_of::<DerivativeDecisionRecord>()),
        capacities.active.checked_mul(size_of::<InstrumentId>()),
    ] {
        retained = retained
            .checked_add(bytes.ok_or(UniverseError::RetainedSizeOverflow)?)
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    for evidence in lifecycle {
        if let DerivativeLifecycle::Option { identity, .. } = &evidence.lifecycle {
            retained = retained
                .checked_add(identity.as_str().len())
                .ok_or(UniverseError::RetainedSizeOverflow)?;
        }
        retained = retained
            .checked_add(dynamic_manifest_bytes(&evidence.source_manifest)?)
            .and_then(|value| value.checked_add(dynamic_availability_bytes(&evidence.availability)))
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    for civil_date in civil_dates {
        retained = retained
            .checked_add(civil_date.calendar_rule.retained_bytes())
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    for roll in rolls {
        retained = retained
            .checked_add(dynamic_manifest_bytes(&roll.source_manifest)?)
            .and_then(|value| value.checked_add(dynamic_availability_bytes(&roll.availability)))
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    Ok(retained)
}

fn dynamic_manifest_bytes(manifest: &DatasetManifestRef) -> Result<usize, UniverseError> {
    manifest
        .dataset_id()
        .as_str()
        .len()
        .checked_add(manifest.schema().name().len())
        .ok_or(UniverseError::RetainedSizeOverflow)
}

fn dynamic_availability_bytes(value: &AvailabilityEvidence) -> usize {
    match value {
        AvailabilityEvidence::Evidenced { evidence, .. } => evidence.retained_bytes(),
        AvailabilityEvidence::Inferred { method, .. } => method.retained_bytes(),
        AvailabilityEvidence::LocalFirstObserved { .. } | AvailabilityEvidence::Unknown => 0,
    }
}
