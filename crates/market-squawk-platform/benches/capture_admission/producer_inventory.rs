//! Checked production capture-producer inventory.

use std::num::NonZeroUsize;

/// One sequential `MarketSource::run_session` task owns capture publication.
const SUPERVISED_SOURCE_TASKS: usize = 1;
/// Event analysis owns no raw-capture publisher.
const EVENT_ANALYSIS_TASKS: usize = 0;
/// The capture writer owns no producer handle.
const CAPTURE_WRITER_TASKS: usize = 0;
/// Coinbase has no child capture-producing task.
const COINBASE_CHILD_CAPTURE_TASKS: usize = 0;

/// Returns the frozen checked representative fan-in.
pub(crate) fn representative_producers() -> Result<NonZeroUsize, &'static str> {
    SUPERVISED_SOURCE_TASKS
        .checked_add(EVENT_ANALYSIS_TASKS)
        .and_then(|count| count.checked_add(CAPTURE_WRITER_TASKS))
        .and_then(|count| count.checked_add(COINBASE_CHILD_CAPTURE_TASKS))
        .and_then(NonZeroUsize::new)
        .ok_or("production capture-producer inventory must be nonzero and representable")
}
