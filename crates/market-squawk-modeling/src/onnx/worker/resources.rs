//! Fail-closed operating-system resource containment for helper processes.

use sha2::{Digest, Sha256};

use super::OnnxWorkerProcessError;

#[cfg(any(windows, all(unix, not(target_vendor = "apple"))))]
const WORKER_MEMORY_LIMIT_BYTES: u64 = 3 * 1024 * 1024 * 1024;
#[cfg(not(any(windows, all(unix, not(target_vendor = "apple")))))]
const WORKER_MEMORY_LIMIT_BYTES: u64 = 0;
#[cfg(unix)]
const WORKER_FILE_DESCRIPTOR_LIMIT: u64 = 64;
#[cfg(not(unix))]
const WORKER_FILE_DESCRIPTOR_LIMIT: u64 = 0;

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
    setrlimit(Resource::As, exact(WORKER_MEMORY_LIMIT_BYTES))
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    // Darwin does not implement a usable RLIMIT_AS. Static compute admission and the parent-owned
    // per-request wall deadline contain work without imposing a cumulative lifetime CPU ceiling.
    setrlimit(Resource::Nofile, exact(WORKER_FILE_DESCRIPTOR_LIMIT))
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    #[cfg(not(target_os = "haiku"))]
    setrlimit(Resource::Core, exact(0)).map_err(|_| OnnxWorkerProcessError::Resource)?;
    Ok(ResourceGuard)
}

#[cfg(unix)]
pub(super) fn deny_file_growth() -> Result<(), OnnxWorkerProcessError> {
    use rustix::process::{Resource, Rlimit, setrlimit};

    let denied = Rlimit {
        current: Some(0),
        maximum: Some(0),
    };
    setrlimit(Resource::Fsize, denied).map_err(|_| OnnxWorkerProcessError::Resource)
}

#[cfg(windows)]
#[derive(Debug)]
pub(super) struct ResourceGuard {
    _job: win32job::Job,
}

#[cfg(windows)]
pub(super) fn apply_resource_limits() -> Result<ResourceGuard, OnnxWorkerProcessError> {
    let maximum =
        usize::try_from(WORKER_MEMORY_LIMIT_BYTES).map_err(|_| OnnxWorkerProcessError::Resource)?;
    let mut limits = win32job::ExtendedLimitInfo::new();
    limits
        .limit_process_memory(maximum)
        .limit_job_memory(maximum)
        .limit_kill_on_job_close();
    let job = win32job::Job::create_with_limit_info(&limits)
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    job.assign_current_process()
        .map_err(|_| OnnxWorkerProcessError::Resource)?;
    Ok(ResourceGuard { _job: job })
}

#[cfg(windows)]
pub(super) fn deny_file_growth() -> Result<(), OnnxWorkerProcessError> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug)]
pub(super) struct ResourceGuard;

#[cfg(not(any(unix, windows)))]
pub(super) fn apply_resource_limits() -> Result<ResourceGuard, OnnxWorkerProcessError> {
    Err(OnnxWorkerProcessError::Resource)
}

#[cfg(not(any(unix, windows)))]
pub(super) fn deny_file_growth() -> Result<(), OnnxWorkerProcessError> {
    Err(OnnxWorkerProcessError::Resource)
}

pub(super) fn semantics_digest() -> [u8; 32] {
    let mut digest = Sha256::new();
    bind_bytes(
        &mut digest,
        b"namespace",
        b"market-squawk/onnx-worker-resource-profile/v2",
    );
    bind_bytes(&mut digest, b"target-os", std::env::consts::OS.as_bytes());
    bind_bytes(
        &mut digest,
        b"target-arch",
        std::env::consts::ARCH.as_bytes(),
    );
    bind_bytes(&mut digest, b"profile", resource_profile());
    bind_u128(
        &mut digest,
        b"memory-limit-bytes",
        u128::from(WORKER_MEMORY_LIMIT_BYTES),
    );
    bind_u128(
        &mut digest,
        b"file-descriptor-limit",
        u128::from(WORKER_FILE_DESCRIPTOR_LIMIT),
    );
    for (name, enabled) in [
        (
            b"address-space-limit".as_slice(),
            cfg!(all(unix, not(target_vendor = "apple"))),
        ),
        (b"process-committed-memory-limit".as_slice(), cfg!(windows)),
        (b"job-committed-memory-limit".as_slice(), cfg!(windows)),
        (
            b"core-dump-denial".as_slice(),
            cfg!(all(unix, not(target_os = "haiku"))),
        ),
        (b"file-growth-denial".as_slice(), cfg!(unix)),
        (b"kill-on-job-close".as_slice(), cfg!(windows)),
        (
            b"fail-closed-unsupported-target".as_slice(),
            !cfg!(any(unix, windows)),
        ),
    ] {
        bind_u128(&mut digest, name, u128::from(u8::from(enabled)));
    }
    digest.finalize().into()
}

const fn resource_profile() -> &'static [u8] {
    if cfg!(all(unix, target_vendor = "apple")) {
        b"darwin-rlimit-nofile-core-fsize/v1"
    } else if cfg!(unix) {
        b"unix-rlimit-as-nofile-core-fsize/v1"
    } else if cfg!(windows) {
        b"windows-job-process-and-job-commit-kill-on-close/v2"
    } else {
        b"unsupported-fail-closed/v1"
    }
}

fn bind_u128(digest: &mut Sha256, name: &[u8], value: u128) {
    bind_bytes(digest, b"field", name);
    digest.update(value.to_be_bytes());
}

fn bind_bytes(digest: &mut Sha256, name: &[u8], value: &[u8]) {
    digest.update((name.len() as u128).to_be_bytes());
    digest.update(name);
    digest.update((value.len() as u128).to_be_bytes());
    digest.update(value);
}
