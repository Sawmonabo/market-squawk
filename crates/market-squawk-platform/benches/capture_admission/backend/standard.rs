//! Standard-reference capture backend selected at compile time.

use std::num::NonZeroUsize;

use market_squawk_platform::capture_benchmark_support::{
    BenchmarkAttempt, BenchmarkCase, BenchmarkCaseReconciliation, BenchmarkOfferedLoadCase,
    BenchmarkOfferedLoadOutcome, BenchmarkOfferedLoadReconciliation, BenchmarkOperation,
    BenchmarkPreparedOperation, BenchmarkProducer,
};

use super::super::endpoints::Endpoint;
use super::super::schema::ForcedLockResult;

pub(crate) const EVIDENCE_BACKEND: &str = "standard";
pub(crate) const QUEUE_TRANSPORT: &str = "standard_sync_channel";
pub(crate) const QUEUE_PRIVATE_STORAGE_ACCOUNTING: &str = "not_measured";
pub(crate) const FIXTURES: &[&str] = &["matrix", "comparable_full", "sustained_rss"];
pub(crate) const EXPECTED_FIXTURES: &str = "matrix,comparable_full,sustained_rss";
pub(crate) const REQUIRES_BASELINE: bool = false;

#[derive(Debug)]
pub(crate) struct PreparedCase {
    inner: BenchmarkCase,
}

#[derive(Debug)]
pub(crate) struct Producer {
    inner: BenchmarkProducer,
}

#[derive(Debug)]
pub(crate) struct PreparedOperation {
    inner: BenchmarkPreparedOperation,
}

#[derive(Debug)]
pub(crate) struct OfferedLoadCase {
    inner: BenchmarkOfferedLoadCase,
}

#[derive(Debug)]
pub(crate) struct OfferedLoadProducer {
    inner: market_squawk_platform::capture_benchmark_support::BenchmarkOfferedLoadProducer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OfferedLoadOutcome {
    Accepted,
    QueueFull,
}

impl PreparedCase {
    pub(crate) fn try_new(
        endpoint: Endpoint,
        payload_bytes: usize,
        queue_depth: NonZeroUsize,
        maximum_samples: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: BenchmarkCase::try_new(
                operation(endpoint),
                payload_bytes,
                queue_depth,
                maximum_samples,
            )?,
        })
    }

    pub(crate) fn configured_queue_depth(&self) -> NonZeroUsize {
        self.inner.configured_queue_depth()
    }

    pub(crate) fn effective_capacity(&self) -> NonZeroUsize {
        self.inner.effective_capacity()
    }

    pub(crate) fn try_producer(&self) -> Result<Producer, Box<dyn std::error::Error>> {
        Ok(Producer {
            inner: self.inner.try_producer()?,
        })
    }

    pub(crate) fn finish(&self) -> Result<BenchmarkCaseReconciliation, Box<dyn std::error::Error>> {
        Ok(self.inner.finish()?)
    }
}

impl Producer {
    pub(crate) fn try_prepare_operation(
        &self,
    ) -> Result<PreparedOperation, Box<dyn std::error::Error>> {
        Ok(PreparedOperation {
            inner: self.inner.try_prepare_operation()?,
        })
    }
}

impl PreparedOperation {
    pub(crate) fn execute(self) -> Result<BenchmarkAttempt, Box<dyn std::error::Error>> {
        Ok(self.inner.execute()?)
    }
}

impl OfferedLoadCase {
    pub(crate) fn try_new(
        payload_bytes: usize,
        queue_depth: NonZeroUsize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: BenchmarkOfferedLoadCase::try_new(payload_bytes, queue_depth)?,
        })
    }

    pub(crate) fn try_producer(&self) -> Result<OfferedLoadProducer, Box<dyn std::error::Error>> {
        Ok(OfferedLoadProducer {
            inner: self.inner.try_producer()?,
        })
    }

    pub(crate) fn finish(
        &self,
    ) -> Result<BenchmarkOfferedLoadReconciliation, Box<dyn std::error::Error>> {
        let reconciliation = self.inner.finish()?;
        if reconciliation.accepted() != reconciliation.consumed() {
            return Err("offered-load capture queue did not reconcile".into());
        }
        Ok(reconciliation)
    }
}

impl OfferedLoadProducer {
    pub(crate) fn try_offer(&self) -> Result<OfferedLoadOutcome, Box<dyn std::error::Error>> {
        Ok(match self.inner.try_offer()? {
            BenchmarkOfferedLoadOutcome::Accepted => OfferedLoadOutcome::Accepted,
            BenchmarkOfferedLoadOutcome::QueueFull => OfferedLoadOutcome::QueueFull,
        })
    }
}

pub(crate) fn verify_comparable_full() -> Result<(), Box<dyn std::error::Error>> {
    Ok(market_squawk_platform::capture_benchmark_support::verify_comparable_full()?)
}

pub(crate) fn validate_compiled_transport() -> Result<(), Box<dyn std::error::Error>> {
    if market_squawk_platform::capture_benchmark_support::benchmark_transport_identity()
        != QUEUE_TRANSPORT
        || market_squawk_platform::capture_benchmark_support::benchmark_private_storage_accounting()
            != QUEUE_PRIVATE_STORAGE_ACCOUNTING
    {
        return Err("standard backend was not monomorphized over sync_channel".into());
    }
    Ok(())
}

pub(crate) fn run_forced_lock() -> Result<Option<ForcedLockResult>, Box<dyn std::error::Error>> {
    Ok(None)
}

const fn operation(endpoint: Endpoint) -> BenchmarkOperation {
    match endpoint {
        Endpoint::QueuePush => BenchmarkOperation::QueuePush,
        Endpoint::QueuePop => BenchmarkOperation::QueuePop,
        Endpoint::CaptureAdmission => BenchmarkOperation::CaptureAdmission,
        Endpoint::WriterAppend => BenchmarkOperation::WriterAppend,
        Endpoint::FlushInclusiveWriter => BenchmarkOperation::FlushInclusiveWriter,
    }
}
