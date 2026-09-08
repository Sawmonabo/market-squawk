//! Shared lifecycle and result-bound mechanics for application-owned domain services.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use market_squawk_services::{RequestContext, ServiceError, ServiceLimits, TypedToolRequest};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const SHUTDOWN_BIT: usize = 1_usize << (usize::BITS - 1);
const ACTIVE_MASK: usize = SHUTDOWN_BIT - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProductTextCopyError {
    BoundExceeded,
    AllocationFailed,
}

/// Copies product text only within a caller-owned byte ceiling and with fallible allocation.
pub(crate) fn try_boxed_product_text(
    value: &str,
    maximum_bytes: usize,
) -> Result<Box<str>, ProductTextCopyError> {
    if value.len() > maximum_bytes {
        return Err(ProductTextCopyError::BoundExceeded);
    }
    let mut output = String::new();
    output
        .try_reserve_exact(value.len())
        .map_err(|_| ProductTextCopyError::AllocationFailed)?;
    output.push_str(value);
    Ok(output.into_boxed_str())
}

/// Produces a deterministic opaque product token without exposing its native coordinates.
pub(crate) fn opaque_product_token(domain: &[u8], components: &[&[u8]]) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(domain);
    for component in components {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component);
    }
    let digest: [u8; 32] = digest.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Produces one bounded, prefixed product token without exposing UUID-shaped text.
pub(crate) fn opaque_product_text_token(
    prefix: &str,
    domain: &[u8],
    components: &[&[u8]],
    maximum_bytes: usize,
) -> Result<Box<str>, ProductTextCopyError> {
    if prefix.is_empty()
        || !prefix.ends_with('_')
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ProductTextCopyError::BoundExceeded);
    }
    let required = prefix
        .len()
        .checked_add(32)
        .ok_or(ProductTextCopyError::BoundExceeded)?;
    if required > maximum_bytes {
        return Err(ProductTextCopyError::BoundExceeded);
    }
    let mut token = String::new();
    token
        .try_reserve_exact(required)
        .map_err(|_| ProductTextCopyError::AllocationFailed)?;
    token.push_str(prefix);
    use fmt::Write as _;
    write!(
        token,
        "{}",
        opaque_product_token(domain, components).simple()
    )
    .map_err(|_| ProductTextCopyError::AllocationFailed)?;
    Ok(token.into_boxed_str())
}

/// Race-free request admission and bounded drain state for one domain service.
pub(super) struct DomainLifecycle {
    state: AtomicUsize,
    shutdown: CancellationToken,
    drained: Notify,
}

impl DomainLifecycle {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicUsize::new(0),
            shutdown: CancellationToken::new(),
            drained: Notify::new(),
        })
    }

    pub(super) fn enter(
        lifecycle: &Arc<Self>,
        context: &RequestContext,
    ) -> Result<DomainCall, ServiceError> {
        ensure_request_live(context, lifecycle)?;
        let mut current = lifecycle.state.load(Ordering::Acquire);
        loop {
            if current & SHUTDOWN_BIT != 0 {
                return Err(ServiceError::Unavailable);
            }
            if current & ACTIVE_MASK == ACTIVE_MASK {
                return Err(ServiceError::ResourceExhausted);
            }
            match lifecycle.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(DomainCall {
                        lifecycle: Arc::clone(lifecycle),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn begin_shutdown(&self) {
        let previous = self.state.fetch_or(SHUTDOWN_BIT, Ordering::AcqRel);
        self.shutdown.cancel();
        if previous & ACTIVE_MASK == 0 {
            self.drained.notify_waiters();
        }
    }

    pub(super) async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let deadline = tokio::time::Instant::from_std(deadline);
        loop {
            if self.state.load(Ordering::Acquire) & ACTIVE_MASK == 0 {
                return Ok(());
            }
            let notified = self.drained.notified();
            if self.state.load(Ordering::Acquire) & ACTIVE_MASK == 0 {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|_| ServiceError::DeadlineExceeded)?;
        }
    }

    pub(super) fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown
    }

    fn leave(&self) {
        let previous = self.state.fetch_sub(1, Ordering::AcqRel);
        if previous & ACTIVE_MASK == 1 {
            self.drained.notify_waiters();
        }
    }
}

impl fmt::Debug for DomainLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::Acquire);
        formatter
            .debug_struct("DomainLifecycle")
            .field("accepting_requests", &(state & SHUTDOWN_BIT == 0))
            .field("active_calls", &(state & ACTIVE_MASK))
            .finish()
    }
}

pub(super) struct DomainCall {
    lifecycle: Arc<DomainLifecycle>,
}

impl Drop for DomainCall {
    fn drop(&mut self) {
        self.lifecycle.leave();
    }
}

pub(super) fn ensure_request_live(
    context: &RequestContext,
    lifecycle: &DomainLifecycle,
) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    if lifecycle.shutdown_token().is_cancelled() {
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

pub(super) fn admitted_result_limits(
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<ServiceLimits, ServiceError> {
    let requested = request
        .arguments()
        .get("resultLimits")
        .and_then(serde_json::Value::as_object)
        .ok_or(ServiceError::InvalidRequest)?;
    let requested_items = requested
        .get("maximumItems")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let requested_bytes = requested
        .get("maximumBytes")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let caller = context.limits();
    let maximum_items = caller.maximum_result_items().min(requested_items);
    let maximum_bytes = caller.maximum_result_bytes().min(requested_bytes);
    ServiceLimits::try_new(
        caller.maximum_inline_bytes().min(maximum_bytes),
        caller.maximum_inline_items().min(maximum_items),
        maximum_bytes,
        maximum_items,
        caller.result_structure(),
    )
    .map_err(|_| ServiceError::InvalidRequest)
}

pub(super) fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
