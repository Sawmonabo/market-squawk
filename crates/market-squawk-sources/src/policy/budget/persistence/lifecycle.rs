//! Packed, lock-free durability-session lifecycle and operation admission.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

const PHASE_BITS: u32 = 3;
const PHASE_MASK: u64 = (1_u64 << PHASE_BITS) - 1;
const ADMISSION_INCREMENT: u64 = 1_u64 << PHASE_BITS;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum AuthorityLifecyclePhase {
    Active = 0,
    Closing = 1,
    TerminalLatched = 2,
    TerminalWriting = 3,
    TerminalPersisted = 4,
    TerminalFailed = 5,
    Closed = 6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionTransition {
    Admitted,
    TerminalWriter,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TerminalWriterClaim {
    Owner,
    Persisted,
    Failed,
    Unavailable,
}

pub(super) trait LifecycleAtomic {
    fn load(&self, ordering: Ordering) -> u64;

    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64>;

    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64>;
}

impl LifecycleAtomic for AtomicU64 {
    fn load(&self, ordering: Ordering) -> u64 {
        self.load(ordering)
    }

    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.compare_exchange(current, new, success, failure)
    }

    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.compare_exchange_weak(current, new, success, failure)
    }
}

#[cfg(all(test, loom))]
impl LifecycleAtomic for loom::sync::atomic::AtomicU64 {
    fn load(&self, ordering: Ordering) -> u64 {
        self.load(ordering)
    }

    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.compare_exchange(current, new, success, failure)
    }

    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.compare_exchange_weak(current, new, success, failure)
    }
}

/// The lifecycle kernel shared by production atomics and concurrency model tests.
#[derive(Debug)]
pub(super) struct LifecycleWord<A> {
    atomic: A,
}

pub(super) type AuthorityLifecycleWord = LifecycleWord<AtomicU64>;

impl LifecycleWord<AtomicU64> {
    pub(super) fn new(initial: u64) -> Self {
        Self {
            atomic: AtomicU64::new(initial),
        }
    }

    #[cfg(test)]
    pub(super) fn store_raw(&self, word: u64) {
        self.atomic.store(word, Ordering::Release);
    }
}

impl<A: LifecycleAtomic> LifecycleWord<A> {
    fn try_admit(&self) -> AdmissionTransition {
        loop {
            let current = self.atomic.load(Ordering::Acquire);
            if lifecycle_phase(current) != AuthorityLifecyclePhase::Active {
                return AdmissionTransition::Unavailable;
            }
            let Some(next) = current.checked_add(ADMISSION_INCREMENT) else {
                let terminal = lifecycle_word(
                    AuthorityLifecyclePhase::TerminalLatched,
                    admitted_count(current),
                );
                if self
                    .atomic
                    .compare_exchange(current, terminal, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return AdmissionTransition::TerminalWriter;
                }
                continue;
            };
            if self
                .atomic
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return AdmissionTransition::Admitted;
            }
        }
    }

    fn release(&self, panicking: bool) {
        if panicking {
            self.latch_terminal();
        }
        loop {
            let current = self.atomic.load(Ordering::Acquire);
            let count = admitted_count(current);
            let next = if let Some(count) = count.checked_sub(1) {
                lifecycle_word(lifecycle_phase(current), count)
            } else if lifecycle_phase(current) == AuthorityLifecyclePhase::Active {
                lifecycle_word(AuthorityLifecyclePhase::TerminalFailed, 0)
            } else {
                return;
            };
            if self
                .atomic
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn phase(&self) -> AuthorityLifecyclePhase {
        lifecycle_phase(self.atomic.load(Ordering::Acquire))
    }

    fn begin_clean_close(&self) -> bool {
        self.atomic
            .compare_exchange(
                lifecycle_word(AuthorityLifecyclePhase::Active, 0),
                lifecycle_word(AuthorityLifecyclePhase::Closing, 0),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn finish_clean_close(&self, succeeded: bool) {
        let destination = if succeeded {
            AuthorityLifecyclePhase::Closed
        } else {
            AuthorityLifecyclePhase::TerminalFailed
        };
        if self
            .atomic
            .compare_exchange(
                lifecycle_word(AuthorityLifecyclePhase::Closing, 0),
                lifecycle_word(destination, 0),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.fail_active();
        }
    }

    fn claim_terminal_writer(&self) -> TerminalWriterClaim {
        loop {
            let current = self.atomic.load(Ordering::Acquire);
            match lifecycle_phase(current) {
                AuthorityLifecyclePhase::TerminalLatched => {
                    let next = lifecycle_word(
                        AuthorityLifecyclePhase::TerminalWriting,
                        admitted_count(current),
                    );
                    if self
                        .atomic
                        .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return TerminalWriterClaim::Owner;
                    }
                }
                AuthorityLifecyclePhase::TerminalPersisted => {
                    return TerminalWriterClaim::Persisted;
                }
                AuthorityLifecyclePhase::TerminalFailed => {
                    return TerminalWriterClaim::Failed;
                }
                AuthorityLifecyclePhase::Active
                | AuthorityLifecyclePhase::Closing
                | AuthorityLifecyclePhase::TerminalWriting
                | AuthorityLifecyclePhase::Closed => {
                    return TerminalWriterClaim::Unavailable;
                }
            }
        }
    }

    fn finish_terminal_write(&self, succeeded: bool) {
        let destination = if succeeded {
            AuthorityLifecyclePhase::TerminalPersisted
        } else {
            AuthorityLifecyclePhase::TerminalFailed
        };
        loop {
            let current = self.atomic.load(Ordering::Acquire);
            if lifecycle_phase(current) != AuthorityLifecyclePhase::TerminalWriting {
                self.fail_active();
                return;
            }
            let next = lifecycle_word(destination, admitted_count(current));
            if self
                .atomic
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn fail_active(&self) -> bool {
        loop {
            let current = self.atomic.load(Ordering::Acquire);
            if lifecycle_phase(current) != AuthorityLifecyclePhase::Active {
                return false;
            }
            let next = lifecycle_word(
                AuthorityLifecyclePhase::TerminalFailed,
                admitted_count(current),
            );
            if self
                .atomic
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn latch_terminal(&self) -> bool {
        loop {
            let current = self.atomic.load(Ordering::Acquire);
            if lifecycle_phase(current) != AuthorityLifecyclePhase::Active {
                return false;
            }
            let next = lifecycle_word(
                AuthorityLifecyclePhase::TerminalLatched,
                admitted_count(current),
            );
            if self
                .atomic
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    #[cfg(test)]
    pub(super) fn load_raw(&self) -> u64 {
        self.atomic.load(Ordering::Acquire)
    }
}

/// Owned proof that one terminal-capable operation entered while the session was active.
#[derive(Debug)]
pub(in crate::policy) struct AuthorityOperationAdmission {
    session: Arc<AuthorityDurabilitySession>,
    released: bool,
}

impl AuthorityOperationAdmission {
    pub(super) fn is_active_for(&self, session: &AuthorityDurabilitySession) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.session), session)
            && session.lifecycle_phase() == AuthorityLifecyclePhase::Active
    }

    pub(in crate::policy) fn belongs_to(&self, session: &AuthorityDurabilitySession) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.session), session)
    }

    pub(in crate::policy) fn latch_terminal(&self) {
        self.session.latch_terminal_from_admitted_operation(self);
    }

    pub(in crate::policy) fn invalidate_session(&self) {
        self.session.invalidate();
    }
}

impl Drop for AuthorityOperationAdmission {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.session.lifecycle.release(std::thread::panicking());
        self.released = true;
    }
}

impl AuthorityDurabilitySession {
    pub(super) fn initial_lifecycle_word() -> u64 {
        lifecycle_word(AuthorityLifecyclePhase::Active, 0)
    }

    pub(in crate::policy) fn admit_operation(
        self: &Arc<Self>,
    ) -> Result<AuthorityOperationAdmission, AuthorityPersistenceError> {
        if self.recovered_unclean || self.envelope.is_poisoned() || self.store.is_poisoned() {
            self.invalidate();
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        match self.lifecycle.try_admit() {
            AdmissionTransition::Admitted => {
                let admission = AuthorityOperationAdmission {
                    session: Arc::clone(self),
                    released: false,
                };
                if admission.is_active_for(self) {
                    Ok(admission)
                } else {
                    Err(AuthorityPersistenceError::SessionUnavailable)
                }
            }
            AdmissionTransition::TerminalWriter => {
                let _terminal = self.persist_terminal_and_detach();
                Err(AuthorityPersistenceError::SessionUnavailable)
            }
            AdmissionTransition::Unavailable => Err(AuthorityPersistenceError::SessionUnavailable),
        }
    }

    pub(super) fn lifecycle_is_active(&self) -> bool {
        self.lifecycle_phase() == AuthorityLifecyclePhase::Active
    }

    pub(super) fn lifecycle_is_closed(&self) -> bool {
        self.lifecycle_phase() == AuthorityLifecyclePhase::Closed
    }

    pub(super) fn begin_clean_close(&self) -> Result<(), AuthorityPersistenceError> {
        self.lifecycle
            .begin_clean_close()
            .then_some(())
            .ok_or(AuthorityPersistenceError::SessionUnavailable)
    }

    pub(super) fn finish_clean_close(&self, succeeded: bool) {
        self.lifecycle.finish_clean_close(succeeded);
    }

    pub(super) fn claim_terminal_writer(&self) -> TerminalWriterClaim {
        self.lifecycle.claim_terminal_writer()
    }

    pub(super) fn finish_terminal_write(&self, succeeded: bool) {
        self.lifecycle.finish_terminal_write(succeeded);
    }

    pub(super) fn fail_active_session_without_terminal_write(&self) -> bool {
        self.lifecycle.fail_active()
    }

    pub(crate) fn latch_terminal_for_time_discontinuity(&self) {
        self.lifecycle.latch_terminal();
    }

    fn latch_terminal_from_admitted_operation(&self, admission: &AuthorityOperationAdmission) {
        if admission.belongs_to(self) {
            self.lifecycle.latch_terminal();
        }
    }

    fn lifecycle_phase(&self) -> AuthorityLifecyclePhase {
        self.lifecycle.phase()
    }
}

const fn lifecycle_word(phase: AuthorityLifecyclePhase, admitted: u64) -> u64 {
    (admitted << PHASE_BITS) | phase as u64
}

const fn admitted_count(word: u64) -> u64 {
    word >> PHASE_BITS
}

const fn lifecycle_phase(word: u64) -> AuthorityLifecyclePhase {
    match word & PHASE_MASK {
        0 => AuthorityLifecyclePhase::Active,
        1 => AuthorityLifecyclePhase::Closing,
        2 => AuthorityLifecyclePhase::TerminalLatched,
        3 => AuthorityLifecyclePhase::TerminalWriting,
        4 => AuthorityLifecyclePhase::TerminalPersisted,
        5 => AuthorityLifecyclePhase::TerminalFailed,
        6 => AuthorityLifecyclePhase::Closed,
        _ => AuthorityLifecyclePhase::TerminalFailed,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn arbitrary_sequential_actions_never_reopen_or_lose_owned_admissions(
            actions in prop::collection::vec(any::<u8>(), 0..256),
        ) {
            let lifecycle = LifecycleWord::new(lifecycle_word(AuthorityLifecyclePhase::Active, 0));
            let mut owned_admissions = 0_u64;
            let mut left_active = false;

            for action in actions {
                match action % 8 {
                    0 => {
                        if lifecycle.try_admit() == AdmissionTransition::Admitted {
                            owned_admissions += 1;
                        }
                    }
                    1 if owned_admissions > 0 => {
                        lifecycle.release(false);
                        owned_admissions -= 1;
                    }
                    2 if owned_admissions > 0 => {
                        lifecycle.release(true);
                        owned_admissions -= 1;
                    }
                    3 => {
                        let _closed = lifecycle.begin_clean_close();
                    }
                    4 => {
                        let _latched = lifecycle.latch_terminal();
                    }
                    5 => {
                        if lifecycle.claim_terminal_writer() == TerminalWriterClaim::Owner {
                            lifecycle.finish_terminal_write(action & 8 == 0);
                        }
                    }
                    6 => lifecycle.finish_clean_close(action & 8 == 0),
                    7 => lifecycle.finish_terminal_write(action & 8 == 0),
                    _ => {}
                }

                let word = lifecycle.load_raw();
                let phase = lifecycle_phase(word);
                prop_assert_eq!(admitted_count(word), owned_admissions);
                if left_active {
                    prop_assert_ne!(phase, AuthorityLifecyclePhase::Active);
                }
                if matches!(
                    phase,
                    AuthorityLifecyclePhase::Closing | AuthorityLifecyclePhase::Closed
                ) {
                    prop_assert_eq!(owned_admissions, 0);
                }
                left_active |= phase != AuthorityLifecyclePhase::Active;
            }
        }
    }

    #[cfg(loom)]
    mod loom_model {
        use std::time::Duration;

        use loom::sync::Arc as LoomArc;
        use loom::sync::atomic::{AtomicU64 as LoomAtomicU64, AtomicUsize};
        use loom::thread;

        use super::*;

        #[test]
        fn admission_terminalization_and_clean_close_races() {
            let mut model = loom::model::Builder::new();
            model.max_threads = 4;
            model.max_branches = 1_000;
            model.max_permutations = Some(50_000);
            model.max_duration = Some(Duration::from_secs(30));
            model.preemption_bound = Some(2);
            model.checkpoint_file = None;
            model.checkpoint_interval = 20_000;
            model.expect_explicit_explore = false;
            model.location = false;
            model.log = false;
            model.check(|| {
                let lifecycle = LoomArc::new(LifecycleWord {
                    atomic: LoomAtomicU64::new(lifecycle_word(AuthorityLifecyclePhase::Active, 0)),
                });
                let terminal_writers = LoomArc::new(AtomicUsize::new(0));

                let admitted_lifecycle = LoomArc::clone(&lifecycle);
                let admitted_writers = LoomArc::clone(&terminal_writers);
                let admitted = thread::spawn(move || {
                    if admitted_lifecycle.try_admit() == AdmissionTransition::Admitted {
                        let _latched = admitted_lifecycle.latch_terminal();
                        if admitted_lifecycle.claim_terminal_writer() == TerminalWriterClaim::Owner
                        {
                            admitted_writers.fetch_add(1, Ordering::AcqRel);
                            admitted_lifecycle.finish_terminal_write(true);
                        }
                        admitted_lifecycle.release(false);
                    }
                });

                let fault_lifecycle = LoomArc::clone(&lifecycle);
                let fault_writers = LoomArc::clone(&terminal_writers);
                let fault = thread::spawn(move || {
                    let _latched = fault_lifecycle.latch_terminal();
                    if fault_lifecycle.claim_terminal_writer() == TerminalWriterClaim::Owner {
                        fault_writers.fetch_add(1, Ordering::AcqRel);
                        fault_lifecycle.finish_terminal_write(true);
                    }
                });

                let close_lifecycle = LoomArc::clone(&lifecycle);
                let close = thread::spawn(move || {
                    if close_lifecycle.begin_clean_close() {
                        close_lifecycle.finish_clean_close(true);
                    }
                });

                assert!(admitted.join().is_ok());
                assert!(fault.join().is_ok());
                assert!(close.join().is_ok());

                let word = lifecycle.atomic.load(Ordering::Acquire);
                let phase = lifecycle_phase(word);
                assert_eq!(admitted_count(word), 0);
                assert!(matches!(
                    phase,
                    AuthorityLifecyclePhase::TerminalPersisted | AuthorityLifecyclePhase::Closed
                ));
                let writers = terminal_writers.load(Ordering::Acquire);
                assert!(writers <= 1);
                assert_eq!(
                    writers,
                    usize::from(phase == AuthorityLifecyclePhase::TerminalPersisted)
                );
            });
        }
    }
}
