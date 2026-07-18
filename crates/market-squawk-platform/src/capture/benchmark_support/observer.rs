//! Immutable bounded latency observer shared by standard and candidate benchmark seams.

use std::sync::Mutex;
use std::time::Instant;

use super::types::{BenchmarkSupportError, elapsed_nanos};

/// Opaque cross-callback interval issued only by the immutable observer.
#[derive(Debug)]
pub(super) struct LatencySpan(Instant);

/// Runs one exact operation inside an immutable clock boundary.
pub(super) fn measure_operation<T>(
    operation: impl FnOnce() -> T,
) -> Result<(T, u64), BenchmarkSupportError> {
    let started = Instant::now();
    let result = operation();
    Ok((result, elapsed_nanos(started)?))
}

/// Bounded single-observation log whose samples can only originate from an owned clock interval.
#[derive(Debug)]
pub(super) struct LatencyObserver {
    maximum_samples: usize,
    samples: Mutex<Vec<u64>>,
}

impl LatencyObserver {
    pub(super) fn try_new(maximum_samples: usize) -> Result<Self, BenchmarkSupportError> {
        let mut samples = Vec::new();
        samples
            .try_reserve_exact(maximum_samples)
            .map_err(|_error| BenchmarkSupportError::InvalidFixture)?;
        Ok(Self {
            maximum_samples,
            samples: Mutex::new(samples),
        })
    }

    pub(super) fn observe<T>(
        &self,
        operation: impl FnOnce() -> T,
    ) -> Result<T, BenchmarkSupportError> {
        let (result, latency) = measure_operation(operation)?;
        self.record_nanos(latency)?;
        Ok(result)
    }

    pub(super) fn begin_span(&self) -> LatencySpan {
        LatencySpan(Instant::now())
    }

    pub(super) fn complete_span(&self, span: LatencySpan) -> Result<(), BenchmarkSupportError> {
        self.record_nanos(elapsed_nanos(span.0)?)
    }

    fn record_nanos(&self, latency: u64) -> Result<(), BenchmarkSupportError> {
        let mut samples = self
            .samples
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        if samples.len() >= self.maximum_samples {
            return Err(BenchmarkSupportError::ObservationInvariant);
        }
        samples.push(latency);
        Ok(())
    }

    pub(super) fn take_exact(
        &self,
        expected_samples: usize,
    ) -> Result<Vec<u64>, BenchmarkSupportError> {
        let mut samples = self
            .samples
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        if samples.len() != expected_samples || samples.capacity() < self.maximum_samples {
            return Err(BenchmarkSupportError::ObservationInvariant);
        }
        Ok(std::mem::take(&mut *samples))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{LatencyObserver, measure_operation};

    #[test]
    fn immutable_clock_starts_before_the_operation_and_cannot_report_a_late_start_zero()
    -> Result<(), Box<dyn std::error::Error>> {
        let minimum = Duration::from_millis(10);
        let (_result, latency) = measure_operation(|| std::thread::sleep(minimum))?;
        assert!(latency >= u64::try_from(minimum.as_nanos())?);

        let observer = LatencyObserver::try_new(1)?;
        observer.observe(|| std::thread::sleep(minimum))?;
        let samples = observer.take_exact(1)?;
        assert_eq!(samples.len(), 1);
        assert!(samples[0] >= u64::try_from(minimum.as_nanos())?);
        Ok(())
    }
}
