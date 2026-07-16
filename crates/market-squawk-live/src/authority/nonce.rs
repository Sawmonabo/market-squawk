//! Fixed-capacity nonce registration and bounded reclamation.

use thiserror::Error;

const MAX_NONCE_SLOTS: usize = 65_536;

/// Exact fixed-slot nonce identity retained by a capability.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct NonceTicket {
    slot: usize,
    epoch: u64,
    nonce: u64,
}

#[derive(Clone, Copy, Debug)]
enum SlotState {
    Vacant,
    Issued {
        nonce: u64,
        binding_digest: [u8; 32],
        deadline_mono_nanos: u64,
    },
    Retired,
}

#[derive(Clone, Copy, Debug)]
struct Slot {
    epoch: u64,
    state: SlotState,
}

/// Startup-sized O(1) nonce registry with bounded incremental reclamation.
#[derive(Debug)]
pub(super) struct NonceRegistry {
    slots: Box<[Slot]>,
    free: Vec<usize>,
    next_nonce: u64,
    reclaim_cursor: usize,
    last_reclaim_scan_count: usize,
}

impl NonceRegistry {
    pub(super) fn new(capacity: usize) -> Result<Self, NonceError> {
        Self::from_parts(capacity, 0, 0)
    }

    #[cfg(test)]
    pub(super) fn new_for_test(
        capacity: usize,
        next_nonce: u64,
        slot_epoch: u64,
    ) -> Result<Self, NonceError> {
        Self::from_parts(capacity, next_nonce, slot_epoch)
    }

    fn from_parts(capacity: usize, next_nonce: u64, slot_epoch: u64) -> Result<Self, NonceError> {
        if capacity == 0 || capacity > MAX_NONCE_SLOTS {
            return Err(NonceError::InvalidCapacity {
                requested: capacity,
                maximum: MAX_NONCE_SLOTS,
            });
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| NonceError::Allocation)?;
        slots.resize(
            capacity,
            Slot {
                epoch: slot_epoch,
                state: SlotState::Vacant,
            },
        );
        let mut free = Vec::new();
        free.try_reserve_exact(capacity)
            .map_err(|_| NonceError::Allocation)?;
        free.extend((0..capacity).rev());
        Ok(Self {
            slots: slots.into_boxed_slice(),
            free,
            next_nonce,
            reclaim_cursor: 0,
            last_reclaim_scan_count: 0,
        })
    }

    pub(super) fn register(
        &mut self,
        binding_digest: [u8; 32],
        deadline_mono_nanos: u64,
    ) -> Result<NonceTicket, NonceError> {
        let nonce = self
            .next_nonce
            .checked_add(1)
            .ok_or(NonceError::NonceExhausted)?;
        let slot_index = self.free.pop().ok_or(NonceError::CapacityExhausted)?;
        let slot = self
            .slots
            .get_mut(slot_index)
            .ok_or(NonceError::RegistryInvariant)?;
        let Some(epoch) = slot.epoch.checked_add(1) else {
            self.free.push(slot_index);
            return Err(NonceError::SlotEpochExhausted);
        };
        if !matches!(slot.state, SlotState::Vacant) {
            self.free.push(slot_index);
            return Err(NonceError::RegistryInvariant);
        }
        slot.epoch = epoch;
        slot.state = SlotState::Issued {
            nonce,
            binding_digest,
            deadline_mono_nanos,
        };
        self.next_nonce = nonce;
        Ok(NonceTicket {
            slot: slot_index,
            epoch,
            nonce,
        })
    }

    pub(super) fn consume(
        &mut self,
        ticket: &NonceTicket,
        binding_digest: [u8; 32],
        now_mono_nanos: u64,
    ) -> Result<(), NonceError> {
        let slot = self
            .slots
            .get_mut(ticket.slot)
            .ok_or(NonceError::StaleTicket)?;
        if slot.epoch != ticket.epoch {
            return Err(NonceError::StaleTicket);
        }
        match slot.state {
            SlotState::Issued {
                nonce,
                binding_digest: expected,
                deadline_mono_nanos,
            } if nonce == ticket.nonce => {
                if expected != binding_digest {
                    return Err(NonceError::BindingMismatch);
                }
                if now_mono_nanos > deadline_mono_nanos {
                    slot.state = SlotState::Retired;
                    return Err(NonceError::Expired);
                }
                slot.state = SlotState::Retired;
                Ok(())
            }
            SlotState::Issued { .. } => Err(NonceError::StaleTicket),
            SlotState::Retired => Err(NonceError::AlreadyConsumed),
            SlotState::Vacant => Err(NonceError::StaleTicket),
        }
    }

    pub(super) fn retire(&mut self, ticket: &NonceTicket) -> Result<(), NonceError> {
        let slot = self
            .slots
            .get_mut(ticket.slot)
            .ok_or(NonceError::StaleTicket)?;
        if slot.epoch != ticket.epoch {
            return Err(NonceError::StaleTicket);
        }
        match slot.state {
            SlotState::Issued { nonce, .. } if nonce == ticket.nonce => {
                slot.state = SlotState::Retired;
                Ok(())
            }
            SlotState::Retired => Err(NonceError::AlreadyConsumed),
            SlotState::Issued { .. } | SlotState::Vacant => Err(NonceError::StaleTicket),
        }
    }

    pub(super) fn reclaim(&mut self, now_mono_nanos: u64, scan_budget: usize) -> usize {
        let mut reclaimed = 0;
        let scan_count = scan_budget.min(self.slots.len());
        for _ in 0..scan_count {
            let index = self.reclaim_cursor;
            self.reclaim_cursor = (self.reclaim_cursor + 1) % self.slots.len();
            let Some(slot) = self.slots.get_mut(index) else {
                continue;
            };
            let reclaimable = match slot.state {
                SlotState::Retired => true,
                SlotState::Issued {
                    deadline_mono_nanos,
                    ..
                } => now_mono_nanos > deadline_mono_nanos,
                SlotState::Vacant => false,
            };
            if reclaimable {
                slot.state = SlotState::Vacant;
                self.free.push(index);
                reclaimed += 1;
            }
        }
        self.last_reclaim_scan_count = scan_count;
        reclaimed
    }

    #[cfg(test)]
    pub(super) const fn last_reclaim_scan_count(&self) -> usize {
        self.last_reclaim_scan_count
    }
}

/// Fixed-capacity nonce state failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum NonceError {
    #[error("invalid nonce capacity {requested}; maximum is {maximum}")]
    InvalidCapacity { requested: usize, maximum: usize },
    #[error("nonce registry allocation failed")]
    Allocation,
    #[error("nonce registry capacity is exhausted")]
    CapacityExhausted,
    #[error("global nonce counter is exhausted")]
    NonceExhausted,
    #[error("nonce slot epoch is exhausted")]
    SlotEpochExhausted,
    #[error("nonce registry invariant failed")]
    RegistryInvariant,
    #[error("nonce ticket is stale")]
    StaleTicket,
    #[error("nonce binding digest does not match")]
    BindingMismatch,
    #[error("nonce was already consumed")]
    AlreadyConsumed,
    #[error("nonce expired before consumption")]
    Expired,
}
