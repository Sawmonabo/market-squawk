//! Fail-closed operating-system resource containment for helper processes.

use super::OnnxWorkerProcessError;

#[cfg(any(windows, all(unix, not(target_vendor = "apple"))))]
const WORKER_ADDRESS_SPACE_BYTES: u64 = 3 * 1024 * 1024 * 1024;
#[cfg(unix)]
const WORKER_FILE_DESCRIPTOR_LIMIT: u64 = 64;

#[cfg(unix)]
#[derive(Debug)]
pub(super) struct ResourceGuard;

#[cfg(unix)]
pub(super) fn apply_resource_limits() -> Result<ResourceGuard, OnnxWorkerProcessError> {
    use rustix::process::{Resource, Rlimit, setrlimit};

    let exact = |value| Rlimit {
        current: Some(value),
        maximum: Some(value),
    };
    #[cfg(not(target_vendor = "apple"))]
    setrlimit(Resource::As, exact(WORKER_ADDRESS_SPACE_BYTES))
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    // Darwin does not implement a usable RLIMIT_AS. Static compute admission and the parent-owned
    // per-request wall deadline contain work without imposing a cumulative lifetime CPU ceiling.
    setrlimit(Resource::Nofile, exact(WORKER_FILE_DESCRIPTOR_LIMIT))
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    setrlimit(Resource::Fsize, exact(0)).map_err(|_| OnnxWorkerProcessError::Resource)?;
    #[cfg(not(target_os = "haiku"))]
    setrlimit(Resource::Core, exact(0)).map_err(|_| OnnxWorkerProcessError::Resource)?;
    Ok(ResourceGuard)
}

#[cfg(windows)]
#[derive(Debug)]
pub(super) struct ResourceGuard(win32job::Job);

#[cfg(windows)]
pub(super) fn apply_resource_limits() -> Result<ResourceGuard, OnnxWorkerProcessError> {
    let maximum = usize::try_from(WORKER_ADDRESS_SPACE_BYTES)
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    let mut limits = win32job::ExtendedLimitInfo::new();
    limits
        .limit_working_memory(0, maximum)
        .limit_kill_on_job_close();
    let job = win32job::Job::create_with_limit_info(&limits)
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    job.assign_current_process()
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    Ok(ResourceGuard(job))
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug)]
pub(super) struct ResourceGuard;

#[cfg(not(any(unix, windows)))]
pub(super) fn apply_resource_limits() -> Result<ResourceGuard, OnnxWorkerProcessError> {
    Err(OnnxWorkerProcessError::Resource)
}
