//! Fixed-slot classification, successor-chain validation, and deterministic peer repair.

use zeroize::Zeroizing;

use super::LocalAuthorityStateStoreError;
use super::envelope::{Envelope, next_context};
use super::filesystem::{Slot, StateFiles};

pub(super) struct Head {
    pub(super) slot: Slot,
    pub(super) envelope: Envelope,
}

enum SlotRead {
    Absent,
    Valid(Envelope),
    Invalid(LocalAuthorityStateStoreError),
}

pub(super) fn reconcile(files: &StateFiles) -> Result<Option<Head>, LocalAuthorityStateStoreError> {
    reconcile_publication_residue(files)?;
    let a = read_slot(files, Slot::A)?;
    let b = read_slot(files, Slot::B)?;
    match (a, b) {
        (SlotRead::Absent, SlotRead::Absent) => Ok(None),
        (SlotRead::Valid(valid), SlotRead::Absent | SlotRead::Invalid(_)) => {
            repair_peer(files, Slot::B, valid).map(Some)
        }
        (SlotRead::Absent | SlotRead::Invalid(_), SlotRead::Valid(valid)) => {
            repair_peer(files, Slot::A, valid).map(Some)
        }
        (SlotRead::Valid(a), SlotRead::Valid(b)) => reconcile_valid_pair(files, a, b).map(Some),
        (SlotRead::Invalid(error), SlotRead::Absent)
        | (SlotRead::Absent, SlotRead::Invalid(error)) => Err(error),
        (SlotRead::Invalid(_), SlotRead::Invalid(_)) => {
            Err(LocalAuthorityStateStoreError::CorruptEnvelope)
        }
    }
}

fn reconcile_publication_residue(files: &StateFiles) -> Result<(), LocalAuthorityStateStoreError> {
    let Some(residue) = files.publication_residue()? else {
        return Ok(());
    };
    let candidate = Envelope::decode(&residue.bytes)
        .map_err(|_| LocalAuthorityStateStoreError::UnsafeFileType)?;
    let a = read_slot(files, Slot::A)?;
    let b = read_slot(files, Slot::B)?;
    if !is_proven_publication_residue(residue.slot, &candidate, &a, &b)? {
        return Err(LocalAuthorityStateStoreError::UnsafeFileType);
    }
    files.discard_publication_residue(&residue)
}

fn is_proven_publication_residue(
    candidate_slot: Slot,
    candidate: &Envelope,
    a: &SlotRead,
    b: &SlotRead,
) -> Result<bool, LocalAuthorityStateStoreError> {
    match (a, b) {
        (SlotRead::Absent, SlotRead::Absent) => {
            let initial = next_context(None)?;
            Ok(candidate_slot == Slot::A
                && candidate.is_first_copy()
                && candidate.context == initial)
        }
        (SlotRead::Valid(a), SlotRead::Valid(b)) => {
            is_proven_against_pair(candidate_slot, candidate, a, b)
        }
        (SlotRead::Valid(authority), SlotRead::Absent | SlotRead::Invalid(_)) => Ok(
            is_exact_peer_copy(candidate_slot, candidate, Slot::A, authority),
        ),
        (SlotRead::Absent | SlotRead::Invalid(_), SlotRead::Valid(authority)) => Ok(
            is_exact_peer_copy(candidate_slot, candidate, Slot::B, authority),
        ),
        (SlotRead::Invalid(_), SlotRead::Absent)
        | (SlotRead::Absent, SlotRead::Invalid(_))
        | (SlotRead::Invalid(_), SlotRead::Invalid(_)) => Ok(false),
    }
}

fn is_proven_against_pair(
    candidate_slot: Slot,
    candidate: &Envelope,
    a: &Envelope,
    b: &Envelope,
) -> Result<bool, LocalAuthorityStateStoreError> {
    let (high_slot, high, low_slot, low) = if a.generation > b.generation {
        (Slot::A, a, Slot::B, b)
    } else if b.generation > a.generation {
        (Slot::B, b, Slot::A, a)
    } else {
        return Ok(false);
    };
    if high.generation != low.generation.saturating_add(1)
        || high.predecessor != low.envelope_digest
    {
        return Ok(false);
    }
    if high.same_logical_payload(low) {
        if !low.is_first_copy() || !high.is_second_copy() {
            return Ok(false);
        }
        let expected = next_context(Some(high))?;
        Ok(candidate_slot == high_slot.other()
            && candidate.is_first_copy()
            && candidate.context == expected)
    } else if low.is_second_copy() && high.is_first_copy() {
        Ok(
            is_exact_peer_copy(candidate_slot, candidate, high_slot, high)
                && candidate_slot == low_slot,
        )
    } else {
        Ok(false)
    }
}

fn is_exact_peer_copy(
    candidate_slot: Slot,
    candidate: &Envelope,
    authority_slot: Slot,
    authority: &Envelope,
) -> bool {
    if candidate_slot != authority_slot.other() || !candidate.same_logical_payload(authority) {
        return false;
    }
    if authority.is_first_copy() {
        candidate.is_second_copy()
            && candidate.predecessor == authority.envelope_digest
            && authority
                .generation
                .checked_add(1)
                .is_some_and(|generation| candidate.generation == generation)
    } else if authority.is_second_copy() {
        candidate.is_first_copy()
            && candidate.envelope_digest == authority.predecessor
            && candidate
                .generation
                .checked_add(1)
                .is_some_and(|generation| authority.generation == generation)
    } else {
        false
    }
}

pub(super) fn publish_envelope(
    files: &StateFiles,
    slot: Slot,
    envelope: &Envelope,
) -> Result<(), LocalAuthorityStateStoreError> {
    let bytes = Zeroizing::new(envelope.encode()?);
    files.publish(slot, &bytes)?;
    let installed = match read_slot(files, slot)? {
        SlotRead::Valid(installed) => installed,
        SlotRead::Absent | SlotRead::Invalid(_) => {
            return Err(LocalAuthorityStateStoreError::VerificationFailed);
        }
    };
    if installed.envelope_digest != envelope.envelope_digest {
        return Err(LocalAuthorityStateStoreError::VerificationFailed);
    }
    Ok(())
}

fn reconcile_valid_pair(
    files: &StateFiles,
    a: Envelope,
    b: Envelope,
) -> Result<Head, LocalAuthorityStateStoreError> {
    let (high_slot, high, low_slot, low) = if a.generation > b.generation {
        (Slot::A, a, Slot::B, b)
    } else if b.generation > a.generation {
        (Slot::B, b, Slot::A, a)
    } else {
        return Err(LocalAuthorityStateStoreError::GenerationConflict);
    };
    if high.generation != low.generation.saturating_add(1)
        || high.predecessor != low.envelope_digest
    {
        return Err(LocalAuthorityStateStoreError::GenerationConflict);
    }
    if high.same_logical_payload(&low) {
        if !low.is_first_copy() || !high.is_second_copy() {
            return Err(LocalAuthorityStateStoreError::GenerationConflict);
        }
        Ok(Head {
            slot: high_slot,
            envelope: high,
        })
    } else if low.is_second_copy() && high.is_first_copy() {
        repair_peer(files, low_slot, high)
    } else {
        Err(LocalAuthorityStateStoreError::GenerationConflict)
    }
}

fn repair_peer(
    files: &StateFiles,
    target: Slot,
    authority: Envelope,
) -> Result<Head, LocalAuthorityStateStoreError> {
    if authority.is_first_copy() {
        let generation = authority
            .context
            .generation
            .checked_add(1)
            .ok_or(LocalAuthorityStateStoreError::GenerationExhausted)?;
        let repaired = Envelope::new(
            generation,
            authority.envelope_digest,
            &authority.context,
            authority.payload.to_vec(),
        )?;
        publish_envelope(files, target, &repaired)
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        Ok(Head {
            slot: target,
            envelope: repaired,
        })
    } else if authority.is_second_copy() {
        let repaired = Envelope::new(
            authority.context.generation,
            authority.context.predecessor,
            &authority.context,
            authority.payload.to_vec(),
        )?;
        if authority.predecessor != repaired.envelope_digest {
            return Err(LocalAuthorityStateStoreError::GenerationConflict);
        }
        publish_envelope(files, target, &repaired)
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        Ok(Head {
            slot: target.other(),
            envelope: authority,
        })
    } else {
        Err(LocalAuthorityStateStoreError::GenerationConflict)
    }
}

fn read_slot(files: &StateFiles, slot: Slot) -> Result<SlotRead, LocalAuthorityStateStoreError> {
    let bytes = match files.read_slot(slot) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Ok(SlotRead::Absent),
        Err(error @ LocalAuthorityStateStoreError::EnvelopeTooLarge { .. }) => {
            return Ok(SlotRead::Invalid(error));
        }
        Err(error) => return Err(error),
    };
    match Envelope::decode(&bytes) {
        Ok(envelope) => Ok(SlotRead::Valid(envelope)),
        Err(error) => Ok(SlotRead::Invalid(error)),
    }
}
