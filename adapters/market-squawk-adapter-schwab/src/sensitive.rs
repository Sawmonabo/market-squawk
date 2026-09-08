//! Shared zeroizing ownership for secret-bearing network handoffs.

use std::fmt;

use bytes::Bytes;
use zeroize::Zeroize as _;

/// One non-cloneable byte allocation whose final shared owner clears it before release.
pub(crate) struct SensitiveBytesOwner {
    bytes: Vec<u8>,
    #[cfg(test)]
    drop_audit: Option<SensitiveDropAudit>,
}

impl SensitiveBytesOwner {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            #[cfg(test)]
            drop_audit: None,
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Transfers this allocation into shareable transport storage without copying its bytes.
    pub(crate) fn into_shared(self) -> Bytes {
        Bytes::from_owner(self)
    }

    #[cfg(test)]
    pub(crate) fn arm_drop_audit(&mut self, audit: SensitiveDropAudit) {
        self.drop_audit = Some(audit);
    }
}

impl AsRef<[u8]> for SensitiveBytesOwner {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Drop for SensitiveBytesOwner {
    fn drop(&mut self) {
        self.bytes.zeroize();
        #[cfg(test)]
        if let Some(audit) = &self.drop_audit {
            audit.record(self.bytes.iter().all(|byte| *byte == 0));
        }
    }
}

impl fmt::Debug for SensitiveBytesOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SensitiveBytesOwner([REDACTED])")
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct SensitiveDropAudit {
    state: std::sync::Arc<SensitiveDropAuditState>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct SensitiveDropAuditState {
    cleared_drops: std::sync::atomic::AtomicUsize,
    uncleared_drops: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl SensitiveDropAudit {
    fn record(&self, cleared: bool) {
        let counter = if cleared {
            &self.state.cleared_drops
        } else {
            &self.state.uncleared_drops
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn cleared_drops(&self) -> usize {
        self.state
            .cleared_drops
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn uncleared_drops(&self) -> usize {
        self.state
            .uncleared_drops
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
