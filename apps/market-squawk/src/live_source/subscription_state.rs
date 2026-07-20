//! Bounded acknowledgement and control state for one source generation.

use std::{
    collections::VecDeque,
    mem::size_of,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

#[cfg(test)]
use market_squawk_domain::SourceIdentifier;
use market_squawk_domain::{ConnectionGeneration, IdentityError, MetadataRevision, SourceId};
use market_squawk_sources::{ControlFrameKind, IgnoredFrameReason, SessionId};
use thiserror::Error;

const MAX_PRODUCTS: usize = 100;
const MAX_PRODUCT_BYTES: usize = 64;
const MAX_CONTROL_MESSAGES: usize = 4_096;
const MAX_CONTROL_BYTES: usize = 4 * 1024 * 1024;
#[cfg(test)]
const REQUIRED_CHANNELS: [&str; 3] = ["heartbeat", "level2", "matches"];

/// Value identity of one already registry-validated source generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GenerationIdentity {
    source_id: SourceId,
    revision: MetadataRevision,
    session_id: SessionId,
    generation: ConnectionGeneration,
}

impl GenerationIdentity {
    pub(super) fn from_session(session: &market_squawk_sources::CurrentSourceSession) -> Self {
        Self {
            source_id: session.source_id().clone(),
            revision: session.revision().clone(),
            session_id: session.session_id().clone(),
            generation: session.generation(),
        }
    }

    #[cfg(test)]
    pub(super) fn try_new(
        source_id: &str,
        revision: &str,
        session_id: &str,
        generation: u64,
    ) -> Result<Self, SubscriptionConstructionError> {
        Ok(Self {
            source_id: SourceId::try_from(source_id)?,
            revision: MetadataRevision::new(SourceIdentifier::try_from(revision)?),
            session_id: SessionId::new(SourceIdentifier::try_from(session_id)?),
            generation: ConnectionGeneration::new(generation)?,
        })
    }
}

/// Explicit count, retained-byte, and transition ceilings for one generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SubscriptionLimits {
    control_messages: NonZeroUsize,
    control_bytes: NonZeroUsize,
}

impl SubscriptionLimits {
    pub(super) fn try_new(
        control_messages: usize,
        control_bytes: usize,
    ) -> Result<Self, SubscriptionConstructionError> {
        let control_messages = NonZeroUsize::new(control_messages)
            .filter(|value| value.get() <= MAX_CONTROL_MESSAGES)
            .ok_or(SubscriptionConstructionError::InvalidLimits)?;
        let control_bytes = NonZeroUsize::new(control_bytes)
            .filter(|value| {
                value.get() >= size_of::<ControlAuditRecord>() && value.get() <= MAX_CONTROL_BYTES
            })
            .ok_or(SubscriptionConstructionError::InvalidLimits)?;
        Ok(Self {
            control_messages,
            control_bytes,
        })
    }

    #[cfg(test)]
    pub(super) const fn minimum_control_bytes() -> usize {
        size_of::<ControlAuditRecord>()
    }
}

/// Closed lifecycle of a single connection generation's subscription authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SubscriptionPhase {
    AwaitingAcknowledgement,
    Active,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuditRecordKind {
    Control(ControlFrameKind),
    Ignored(IgnoredFrameReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ControlAuditRecord {
    kind: AuditRecordKind,
    transition: u64,
}

/// Bounded one-generation subscription and control owner.
#[derive(Debug)]
pub(super) struct SubscriptionStateMachine {
    generation: GenerationIdentity,
    expected_products: Vec<String>,
    acknowledgement_deadline: Instant,
    phase: SubscriptionPhase,
    transition: u64,
    rejected_pre_acknowledgement_data: u64,
    last_market_data_at: Option<Instant>,
    control_audit: VecDeque<ControlAuditRecord>,
    control_audit_bytes: usize,
    limits: SubscriptionLimits,
    estimated_peak_bytes: NonZeroUsize,
}

impl SubscriptionStateMachine {
    pub(super) fn try_new<'a>(
        generation: GenerationIdentity,
        products: impl IntoIterator<Item = &'a str>,
        acknowledgement_timeout: Duration,
        started_at: Instant,
        limits: SubscriptionLimits,
    ) -> Result<Self, SubscriptionConstructionError> {
        if acknowledgement_timeout.is_zero() {
            return Err(SubscriptionConstructionError::InvalidAcknowledgementTimeout);
        }
        let acknowledgement_deadline = started_at
            .checked_add(acknowledgement_timeout)
            .ok_or(SubscriptionConstructionError::InvalidAcknowledgementTimeout)?;
        let mut expected_products = Vec::new();
        for product in products {
            validate_product(product)?;
            if expected_products.len() == MAX_PRODUCTS {
                return Err(SubscriptionConstructionError::InvalidProductCount);
            }
            expected_products
                .try_reserve(1)
                .map_err(|_error| SubscriptionConstructionError::AllocationFailed)?;
            let mut owned_product = String::new();
            owned_product
                .try_reserve_exact(product.len())
                .map_err(|_error| SubscriptionConstructionError::AllocationFailed)?;
            owned_product.push_str(product);
            expected_products.push(owned_product);
        }
        if expected_products.is_empty() {
            return Err(SubscriptionConstructionError::InvalidProductCount);
        }
        expected_products.sort_unstable();
        if expected_products.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(SubscriptionConstructionError::DuplicateProduct);
        }
        let retained_control_capacity = limits
            .control_messages
            .get()
            .min(limits.control_bytes.get() / size_of::<ControlAuditRecord>());
        let mut control_audit = VecDeque::new();
        control_audit
            .try_reserve_exact(retained_control_capacity)
            .map_err(|_error| SubscriptionConstructionError::AllocationFailed)?;
        let estimated_peak_bytes = estimate_peak_bytes(
            &generation,
            &expected_products,
            expected_products.capacity(),
            control_audit.capacity(),
        )?;
        Ok(Self {
            generation,
            expected_products,
            acknowledgement_deadline,
            phase: SubscriptionPhase::AwaitingAcknowledgement,
            transition: 0,
            rejected_pre_acknowledgement_data: 0,
            last_market_data_at: None,
            control_audit,
            control_audit_bytes: 0,
            limits,
            estimated_peak_bytes,
        })
    }

    #[cfg(test)]
    pub(super) const fn phase(&self) -> SubscriptionPhase {
        self.phase
    }

    #[cfg(test)]
    pub(super) fn acknowledged_products(&self) -> impl Iterator<Item = &str> {
        (self.phase == SubscriptionPhase::Active)
            .then_some(self.expected_products.as_slice())
            .into_iter()
            .flatten()
            .map(String::as_str)
    }

    pub(super) const fn estimated_peak_bytes(&self) -> NonZeroUsize {
        self.estimated_peak_bytes
    }

    #[cfg(test)]
    pub(super) const fn last_market_data_at(&self) -> Option<Instant> {
        self.last_market_data_at
    }

    #[cfg(test)]
    pub(super) const fn rejected_pre_acknowledgement_data(&self) -> u64 {
        self.rejected_pre_acknowledgement_data
    }

    #[cfg(test)]
    pub(super) fn audit_usage(&self) -> (usize, usize) {
        (self.control_audit.len(), self.control_audit_bytes)
    }

    pub(super) fn observe_heartbeat(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        self.record_control(generation, now, ControlFrameKind::Heartbeat)?;
        Ok(self.phase)
    }

    pub(super) fn observe_control(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
        kind: ControlFrameKind,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        self.record_control(generation, now, kind)?;
        Ok(self.phase)
    }

    pub(super) fn observe_ignored(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
        reason: IgnoredFrameReason,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        self.record_audit(generation, now, AuditRecordKind::Ignored(reason))?;
        Ok(self.phase)
    }

    #[cfg(test)]
    pub(super) fn observe_acknowledgement(
        &mut self,
        generation: &GenerationIdentity,
        products: &[&str],
        channels: &[&str],
        now: Instant,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        self.validate_observation(generation, now)?;
        if self.phase == SubscriptionPhase::Active {
            return self.invalidate(SubscriptionFailure::DuplicateAcknowledgement);
        }
        if !is_exact_set(
            products,
            self.expected_products.len(),
            MAX_PRODUCT_BYTES,
            |product| {
                self.expected_products
                    .binary_search_by(|expected| expected.as_str().cmp(product))
                    .is_ok()
            },
        ) || !is_exact_set(
            channels,
            REQUIRED_CHANNELS.len(),
            MAX_PRODUCT_BYTES,
            |channel| REQUIRED_CHANNELS.contains(&channel),
        ) {
            return self.invalidate(SubscriptionFailure::AcknowledgementMismatch);
        }
        self.observe_validated_acknowledgement(generation, now)
    }

    /// Records an acknowledgement whose exact product/channel sets were already proven by the
    /// configured production decoder and bound to this session's capture receipt.
    pub(super) fn observe_validated_acknowledgement(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        self.validate_observation(generation, now)?;
        if self.expected_products.is_empty() {
            return self.invalidate(SubscriptionFailure::AcknowledgementMismatch);
        }
        if self.phase == SubscriptionPhase::Active {
            return self.invalidate(SubscriptionFailure::DuplicateAcknowledgement);
        }
        self.record_control(
            generation,
            now,
            ControlFrameKind::SubscriptionAcknowledgement,
        )?;
        self.phase = SubscriptionPhase::Active;
        Ok(self.phase)
    }

    pub(super) fn observe_data(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        self.validate_observation(generation, now)?;
        if self.phase != SubscriptionPhase::Active {
            let Some(rejected) = self.rejected_pre_acknowledgement_data.checked_add(1) else {
                return self.invalidate(SubscriptionFailure::RejectedDataCounterOverflow);
            };
            self.rejected_pre_acknowledgement_data = rejected;
            return self.invalidate(SubscriptionFailure::DataBeforeAcknowledgement);
        }
        self.last_market_data_at = Some(now);
        Ok(self.phase)
    }

    #[cfg(test)]
    pub(super) fn poll_deadline(
        &mut self,
        now: Instant,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        self.validate_deadline(now)?;
        Ok(self.phase)
    }

    fn record_control(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
        kind: ControlFrameKind,
    ) -> Result<(), SubscriptionFailure> {
        self.record_audit(generation, now, AuditRecordKind::Control(kind))
    }

    fn record_audit(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
        kind: AuditRecordKind,
    ) -> Result<(), SubscriptionFailure> {
        self.validate_observation(generation, now)?;
        let transition = match next_transition(self.transition) {
            Ok(transition) => transition,
            Err(error) => return self.invalidate(error),
        };
        let record_bytes = size_of::<ControlAuditRecord>();
        while self.control_audit.len() == self.limits.control_messages.get()
            || self
                .control_audit_bytes
                .checked_add(record_bytes)
                .is_none_or(|bytes| bytes > self.limits.control_bytes.get())
        {
            let Some(_evicted) = self.control_audit.pop_front() else {
                return self.invalidate(SubscriptionFailure::AuditAccountingInvariant);
            };
            let Some(remaining) = self.control_audit_bytes.checked_sub(record_bytes) else {
                return self.invalidate(SubscriptionFailure::AuditAccountingInvariant);
            };
            self.control_audit_bytes = remaining;
        }
        let Some(control_audit_bytes) = self.control_audit_bytes.checked_add(record_bytes) else {
            return self.invalidate(SubscriptionFailure::AuditAccountingInvariant);
        };
        self.control_audit
            .push_back(ControlAuditRecord { kind, transition });
        self.control_audit_bytes = control_audit_bytes;
        self.transition = transition;
        Ok(())
    }

    fn validate_observation(
        &mut self,
        generation: &GenerationIdentity,
        now: Instant,
    ) -> Result<(), SubscriptionFailure> {
        if self.phase == SubscriptionPhase::Invalid {
            return Err(SubscriptionFailure::GenerationInvalid);
        }
        if generation != &self.generation {
            return self.invalidate(SubscriptionFailure::StaleGeneration);
        }
        self.validate_deadline(now).map(|_phase| ())
    }

    fn validate_deadline(
        &mut self,
        now: Instant,
    ) -> Result<SubscriptionPhase, SubscriptionFailure> {
        if self.phase == SubscriptionPhase::Invalid {
            return Err(SubscriptionFailure::GenerationInvalid);
        }
        if self.phase == SubscriptionPhase::AwaitingAcknowledgement
            && now >= self.acknowledgement_deadline
        {
            return self.invalidate(SubscriptionFailure::AcknowledgementDeadlineExceeded);
        }
        Ok(self.phase)
    }

    fn invalidate<T>(&mut self, error: SubscriptionFailure) -> Result<T, SubscriptionFailure> {
        self.phase = SubscriptionPhase::Invalid;
        Err(error)
    }
}

pub(super) fn next_transition(current: u64) -> Result<u64, SubscriptionFailure> {
    current
        .checked_add(1)
        .ok_or(SubscriptionFailure::TransitionSequenceExhausted)
}

#[cfg(test)]
fn is_exact_set(
    values: &[&str],
    expected_count: usize,
    max_bytes: usize,
    is_expected: impl Fn(&str) -> bool,
) -> bool {
    values.len() == expected_count
        && values.iter().enumerate().all(|(index, value)| {
            !value.is_empty()
                && value.len() <= max_bytes
                && is_expected(value)
                && !values[..index].contains(value)
        })
}

fn estimate_peak_bytes(
    generation: &GenerationIdentity,
    products: &[String],
    product_capacity: usize,
    control_capacity: usize,
) -> Result<NonZeroUsize, SubscriptionConstructionError> {
    let allocation_overhead = size_of::<usize>()
        .checked_mul(2)
        .ok_or(SubscriptionConstructionError::RetainedSizeOverflow)?;
    let generation_bytes = generation
        .source_id
        .retained_bytes()
        .checked_add(generation.revision.as_source_identifier().retained_bytes())
        .and_then(|bytes| {
            bytes.checked_add(
                generation
                    .session_id
                    .as_source_identifier()
                    .retained_bytes(),
            )
        })
        .ok_or(SubscriptionConstructionError::RetainedSizeOverflow)?;
    let product_bytes = products
        .iter()
        .try_fold(
            product_capacity
                .checked_mul(size_of::<String>())
                .and_then(|bytes| bytes.checked_add(allocation_overhead))
                .ok_or(SubscriptionConstructionError::RetainedSizeOverflow)?,
            |bytes, product| {
                bytes
                    .checked_add(product.capacity())
                    .and_then(|value| value.checked_add(allocation_overhead))
            },
        )
        .ok_or(SubscriptionConstructionError::RetainedSizeOverflow)?;
    let control_bytes = control_capacity
        .checked_mul(size_of::<ControlAuditRecord>())
        .and_then(|bytes| bytes.checked_add(allocation_overhead))
        .ok_or(SubscriptionConstructionError::RetainedSizeOverflow)?;
    let peak = size_of::<SubscriptionStateMachine>()
        .checked_add(generation_bytes)
        .and_then(|bytes| bytes.checked_add(product_bytes))
        .and_then(|bytes| bytes.checked_add(control_bytes))
        .ok_or(SubscriptionConstructionError::RetainedSizeOverflow)?;
    NonZeroUsize::new(peak).ok_or(SubscriptionConstructionError::RetainedSizeOverflow)
}

fn validate_product(product: &str) -> Result<(), SubscriptionConstructionError> {
    if product.is_empty()
        || product.len() > MAX_PRODUCT_BYTES
        || !product
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(SubscriptionConstructionError::InvalidProduct);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SubscriptionConstructionError {
    #[error("subscription identity is invalid")]
    Identity(#[from] IdentityError),
    #[error("subscription product count is invalid")]
    InvalidProductCount,
    #[error("subscription product identifier is invalid")]
    InvalidProduct,
    #[error("subscription product identifier is duplicated")]
    DuplicateProduct,
    #[error("subscription acknowledgement timeout is invalid")]
    InvalidAcknowledgementTimeout,
    #[error("subscription state limits are invalid")]
    InvalidLimits,
    #[error("subscription state allocation failed")]
    AllocationFailed,
    #[error("subscription retained-size accounting overflowed")]
    RetainedSizeOverflow,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SubscriptionFailure {
    #[error("subscription generation is already invalid")]
    GenerationInvalid,
    #[error("subscription control belongs to a stale generation")]
    StaleGeneration,
    #[error("subscription acknowledgement did not exactly match configuration")]
    AcknowledgementMismatch,
    #[error("subscription acknowledgement was duplicated")]
    DuplicateAcknowledgement,
    #[error("subscription acknowledgement deadline expired")]
    AcknowledgementDeadlineExceeded,
    #[error("market data arrived before subscription acknowledgement")]
    DataBeforeAcknowledgement,
    #[error("rejected pre-acknowledgement data counter overflowed")]
    RejectedDataCounterOverflow,
    #[error("subscription transition sequence is exhausted")]
    TransitionSequenceExhausted,
    #[error("subscription audit retained-byte accounting is inconsistent")]
    AuditAccountingInvariant,
}
