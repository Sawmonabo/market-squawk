//! Capability-confined immutable artifact publication shared by local application surfaces.

use std::{
    future::Future,
    io::{Read as _, Write as _},
    num::NonZeroUsize,
    path::Path,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        mpsc::{SyncSender, sync_channel},
    },
    task::{Context, Poll},
    thread::JoinHandle as ThreadJoinHandle,
    time::Duration,
};

use async_trait::async_trait;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_platform::ArtifactRoot;
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRead,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactRepository,
    PARQUET_ARTIFACT_MEDIA_TYPE,
};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const ARTIFACT_NAMESPACE: &str = "mcp/v1";
const READ_CHECKPOINT_BYTES: usize = 64 * 1024;
const MAXIMUM_CONCURRENT_ARTIFACT_READS: usize = 8;
const MAXIMUM_PENDING_ARTIFACT_REAPS: usize = 64;
const ARTIFACT_REAPER_POLL_INTERVAL: Duration = Duration::from_millis(1);
const _: () = assert!(MAXIMUM_CONCURRENT_ARTIFACT_READS <= Semaphore::MAX_PERMITS);
const _: () = assert!(MAXIMUM_PENDING_ARTIFACT_REAPS <= Semaphore::MAX_PERMITS);

static ARTIFACT_READ_REAPER: LazyLock<ArtifactReadReaper> =
    LazyLock::new(ArtifactReadReaper::start);

#[derive(Clone, Debug)]
struct ArtifactReadSupervisor {
    admission: Arc<Semaphore>,
}

impl ArtifactReadSupervisor {
    fn try_new(capacity: NonZeroUsize) -> Result<Self, ArtifactError> {
        ARTIFACT_READ_REAPER.ensure_available()?;
        Ok(Self {
            admission: Arc::new(Semaphore::new(capacity.get())),
        })
    }

    async fn run<T, F>(
        &self,
        context: ArtifactReadContext,
        operation: F,
    ) -> Result<T, ArtifactError>
    where
        T: Send + 'static,
        F: FnOnce(CancellationToken) -> Result<T, ArtifactError> + Send + 'static,
    {
        context.ensure_live()?;
        let cancellation = context.cancellation().clone();
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| ArtifactError::Unavailable)?;
        let mut task = ArtifactReadTask::spawn(permit, operation)?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                task.cancel();
                Err(ArtifactError::Cancelled)
            }
            () = tokio::time::sleep_until(deadline) => {
                task.cancel();
                Err(ArtifactError::DeadlineExceeded)
            }
            result = &mut task => result,
        }
    }
}

struct ArtifactReadTask<T: Send + 'static> {
    cancellation: CancellationToken,
    command: Option<ArtifactReapCommand>,
    result: oneshot::Receiver<Result<T, ArtifactError>>,
}

impl<T: Send + 'static> ArtifactReadTask<T> {
    fn spawn<F>(operation_permit: OwnedSemaphorePermit, operation: F) -> Result<Self, ArtifactError>
    where
        F: FnOnce(CancellationToken) -> Result<T, ArtifactError> + Send + 'static,
    {
        let reaper_capacity = ARTIFACT_READ_REAPER.try_reserve()?;
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let (result_sender, result) = oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _operation_permit = operation_permit;
            let outcome = operation(worker_cancellation);
            let _ignored = result_sender.send(outcome);
        });
        Ok(Self {
            cancellation,
            command: Some(ArtifactReapCommand {
                worker,
                _capacity: reaper_capacity,
            }),
            result,
        })
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn handoff_pending_worker(&mut self) {
        let Some(command) = self.command.take() else {
            return;
        };
        if command.worker.is_finished() {
            return;
        }
        ARTIFACT_READ_REAPER.reap(command);
    }
}

impl<T: Send + 'static> Future for ArtifactReadTask<T> {
    type Output = Result<T, ArtifactError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.result).poll(context) {
            Poll::Ready(Ok(result)) => {
                self.handoff_pending_worker();
                Poll::Ready(result)
            }
            Poll::Ready(Err(_closed)) => {
                let Some(command) = self.command.as_mut() else {
                    return Poll::Ready(Err(ArtifactError::Unavailable));
                };
                match Pin::new(&mut command.worker).poll(context) {
                    Poll::Ready(result) => {
                        self.command = None;
                        if result.is_err() {
                            tracing::error!("blocking artifact reader failed before returning");
                        }
                        Poll::Ready(Err(ArtifactError::Unavailable))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Send + 'static> Drop for ArtifactReadTask<T> {
    fn drop(&mut self) {
        self.cancel();
        self.handoff_pending_worker();
    }
}

struct ArtifactReapCommand {
    worker: JoinHandle<()>,
    _capacity: OwnedSemaphorePermit,
}

struct ArtifactReadReaper {
    sender: Option<SyncSender<ArtifactReapCommand>>,
    capacity: Arc<Semaphore>,
    _thread: Option<ThreadJoinHandle<()>>,
}

impl ArtifactReadReaper {
    fn start() -> Self {
        let capacity = Arc::new(Semaphore::new(MAXIMUM_PENDING_ARTIFACT_REAPS));
        let (sender, receiver) =
            sync_channel::<ArtifactReapCommand>(MAXIMUM_PENDING_ARTIFACT_REAPS);
        let thread = std::thread::Builder::new()
            .name("market-squawk-artifact-reaper".to_owned())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    Self::reap_worker(command);
                }
            });
        match thread {
            Ok(thread) => Self {
                sender: Some(sender),
                capacity,
                _thread: Some(thread),
            },
            Err(_error) => Self {
                sender: None,
                capacity,
                _thread: None,
            },
        }
    }

    fn ensure_available(&self) -> Result<(), ArtifactError> {
        if self.sender.is_some() {
            Ok(())
        } else {
            Err(ArtifactError::Unavailable)
        }
    }

    fn try_reserve(&self) -> Result<OwnedSemaphorePermit, ArtifactError> {
        self.ensure_available()?;
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| ArtifactError::Unavailable)
    }

    fn reap(&self, command: ArtifactReapCommand) {
        let Some(sender) = self.sender.as_ref() else {
            self.reap_without_worker_thread(command);
            return;
        };
        // The semaphore and channel have equal capacity, and each queued command retains one
        // permit. An admitted command therefore already owns a channel slot.
        if let Err(error) = sender.send(command) {
            self.reap_without_worker_thread(error.0);
        }
    }

    fn reap_without_worker_thread(&self, command: ArtifactReapCommand) {
        Self::reap_worker(command);
    }

    fn reap_worker(mut command: ArtifactReapCommand) {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        loop {
            match Pin::new(&mut command.worker).poll(&mut context) {
                Poll::Ready(result) => {
                    if result.is_err() {
                        tracing::error!("blocking artifact reader failed while being reaped");
                    }
                    drop(command._capacity);
                    return;
                }
                Poll::Pending => std::thread::sleep(ARTIFACT_REAPER_POLL_INTERVAL),
            }
        }
    }
}

pub(crate) fn controlled_artifact_repository(
    root: ArtifactRoot,
    maximum_bytes: NonZeroUsize,
) -> Result<Arc<dyn ArtifactRepository>, ArtifactError> {
    ControlledArtifactRepository::try_new(root, maximum_bytes)
        .map(|repository| Arc::new(repository) as Arc<dyn ArtifactRepository>)
}

/// Bounded content-addressed repository under the configured artifact capability.
#[derive(Debug)]
pub(crate) struct ControlledArtifactRepository {
    root: ArtifactRoot,
    maximum_bytes: NonZeroUsize,
    reads: ArtifactReadSupervisor,
}

impl ControlledArtifactRepository {
    pub(crate) fn try_new(
        root: ArtifactRoot,
        maximum_bytes: NonZeroUsize,
    ) -> Result<Self, ArtifactError> {
        let directory = root
            .try_clone_directory()
            .map_err(|_| ArtifactError::Unavailable)?;
        directory
            .create_dir_all(ARTIFACT_NAMESPACE)
            .map_err(|_| ArtifactError::Unavailable)?;
        let read_capacity = NonZeroUsize::new(MAXIMUM_CONCURRENT_ARTIFACT_READS)
            .ok_or(ArtifactError::Unavailable)?;
        Ok(Self {
            root,
            maximum_bytes,
            reads: ArtifactReadSupervisor::try_new(read_capacity)?,
        })
    }

    fn publish_bounded(
        &self,
        publication: &ArtifactPublication,
        context: &ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        context.ensure_live()?;
        if publication.byte_count() == 0 || publication.byte_count() > self.maximum_bytes.get() {
            return Err(ArtifactError::InvalidPublication);
        }
        let digest = publication.sha256_hex();
        let coordinate = artifact_coordinate(publication.media_type(), digest)?;
        let parent = coordinate
            .path
            .parent()
            .ok_or(ArtifactError::InvalidPublication)?;
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| ArtifactError::Unavailable)?;
        let staging_reference = parent.join(format!("stage-{}.tmp", hex_bytes(&nonce)));
        let artifact_path = coordinate.path.as_path();
        let staging_path = Path::new(&staging_reference);
        drop(
            self.root
                .resolve(artifact_path)
                .map_err(|_| ArtifactError::Unavailable)?,
        );
        drop(
            self.root
                .resolve(staging_path)
                .map_err(|_| ArtifactError::Unavailable)?,
        );

        let directory = self
            .root
            .try_clone_directory()
            .map_err(|_| ArtifactError::Unavailable)?;
        directory
            .create_dir_all(parent)
            .map_err(|_| ArtifactError::Unavailable)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut staging = directory
            .open_with(staging_path, &options)
            .map_err(|_| ArtifactError::Unavailable)?;
        let mut guard = StagingGuard::new(&directory, staging_path);
        staging
            .write_all(publication.content())
            .map_err(|_| ArtifactError::Unavailable)?;
        staging.sync_all().map_err(|_| ArtifactError::Unavailable)?;
        drop(staging);
        context.ensure_live()?;
        #[cfg(windows)]
        eprintln!("artifact publication diagnostic: staging synchronized");

        publish_staged_artifact(
            &self.root,
            &directory,
            staging_path,
            artifact_path,
            &mut guard,
        )?;
        #[cfg(windows)]
        eprintln!("artifact publication diagnostic: staged artifact published");
        synchronize_publication_directories(&directory, artifact_path)?;
        #[cfg(windows)]
        eprintln!("artifact publication diagnostic: publication synchronized");
        context.ensure_live()?;
        let persisted = read_bounded_regular(&directory, artifact_path, self.maximum_bytes.get())?;
        #[cfg(windows)]
        eprintln!("artifact publication diagnostic: publication reopened");
        if persisted.as_slice() != publication.content()
            || format!("{:x}", Sha256::digest(&persisted)) != publication.sha256_hex()
        {
            return Err(ArtifactError::Unavailable);
        }
        ArtifactReference::try_new(
            coordinate.id,
            digest,
            publication.byte_count(),
            publication.media_type(),
        )
    }
}

#[async_trait]
impl ArtifactRepository for ControlledArtifactRepository {
    async fn publish(
        &self,
        publication: ArtifactPublication,
        context: ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        self.publish_bounded(&publication, &context)
    }

    async fn read(
        &self,
        request: ArtifactReadRequest,
        context: ArtifactReadContext,
    ) -> Result<ArtifactRead, ArtifactError> {
        let root = self.root.clone();
        let maximum_bytes = self.maximum_bytes;
        let worker_context = context.clone();
        self.reads
            .run(context, move |worker_cancellation| {
                read_verified_artifact(
                    &root,
                    maximum_bytes,
                    request,
                    &worker_context,
                    &worker_cancellation,
                )
            })
            .await
    }
}

#[derive(Debug)]
struct StagingGuard<'directory> {
    directory: &'directory Dir,
    path: &'directory Path,
    armed: bool,
}

impl<'directory> StagingGuard<'directory> {
    const fn new(directory: &'directory Dir, path: &'directory Path) -> Self {
        Self {
            directory,
            path,
            armed: true,
        }
    }

    fn remove(&mut self) -> Result<(), ArtifactError> {
        self.directory
            .remove_file(self.path)
            .map_err(|_| ArtifactError::Unavailable)?;
        self.armed = false;
        Ok(())
    }

    #[cfg(windows)]
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ignored = self.directory.remove_file(self.path);
            self.armed = false;
        }
    }
}

fn read_bounded_regular(
    directory: &Dir,
    path: &Path,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ArtifactError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_nonblocking_read(&mut options);
    let mut file = directory
        .open_with(path, &options)
        .map_err(|_| ArtifactError::Unavailable)?;
    let metadata = file.metadata().map_err(|_| ArtifactError::Unavailable)?;
    if !metadata.is_file() {
        return Err(ArtifactError::Unavailable);
    }
    let size = usize::try_from(metadata.len()).map_err(|_| ArtifactError::Unavailable)?;
    if size == 0 || size > maximum_bytes {
        return Err(ArtifactError::Unavailable);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| ArtifactError::Unavailable)?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)
        .map_err(|_| ArtifactError::Unavailable)?;
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| ArtifactError::Unavailable)?
        != 0
    {
        return Err(ArtifactError::Unavailable);
    }
    Ok(bytes)
}

fn read_verified_artifact(
    root: &ArtifactRoot,
    repository_maximum: NonZeroUsize,
    request: ArtifactReadRequest,
    context: &ArtifactReadContext,
    worker_cancellation: &CancellationToken,
) -> Result<ArtifactRead, ArtifactError> {
    ensure_artifact_read_live(context, worker_cancellation)?;
    let reference = request.reference();
    let coordinate = artifact_coordinate(reference.media_type(), reference.sha256())?;
    if reference.id() != coordinate.id {
        return Err(ArtifactError::InvalidReference);
    }
    if reference.byte_count() > repository_maximum.get()
        || reference.byte_count() > request.maximum_bytes().get()
    {
        return Err(ArtifactError::ReadLimitExceeded);
    }
    let artifact_path = coordinate.path.as_path();
    drop(
        root.resolve(artifact_path)
            .map_err(|_| ArtifactError::InvalidReference)?,
    );
    let directory = root
        .try_clone_directory()
        .map_err(|_| ArtifactError::Unavailable)?;
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_nonblocking_read(&mut options);
    let mut file = directory
        .open_with(artifact_path, &options)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ArtifactError::NotFound
            } else {
                ArtifactError::Unavailable
            }
        })?;
    let metadata = file.metadata().map_err(|_| ArtifactError::Unavailable)?;
    let size = usize::try_from(metadata.len()).map_err(|_| ArtifactError::Unavailable)?;
    if !metadata.is_file()
        || size != reference.byte_count()
        || size > repository_maximum.get()
        || size > request.maximum_bytes().get()
    {
        return Err(ArtifactError::Unavailable);
    }
    let mut content = Vec::new();
    content
        .try_reserve_exact(size)
        .map_err(|_| ArtifactError::ReadLimitExceeded)?;
    content.resize(size, 0);
    let mut offset = 0_usize;
    let mut digest = Sha256::new();
    while offset < size {
        ensure_artifact_read_live(context, worker_cancellation)?;
        let end = offset
            .checked_add(READ_CHECKPOINT_BYTES)
            .map(|end| end.min(size))
            .ok_or(ArtifactError::ReadLimitExceeded)?;
        let read = file
            .read(&mut content[offset..end])
            .map_err(|_| ArtifactError::Unavailable)?;
        if read == 0 {
            return Err(ArtifactError::Unavailable);
        }
        let read_end = offset
            .checked_add(read)
            .ok_or(ArtifactError::ReadLimitExceeded)?;
        digest.update(&content[offset..read_end]);
        offset = read_end;
    }
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| ArtifactError::Unavailable)?
        != 0
        || format!("{:x}", digest.finalize()) != reference.sha256()
    {
        return Err(ArtifactError::Unavailable);
    }
    ensure_artifact_read_live(context, worker_cancellation)?;
    ArtifactRead::try_new(request.into_reference(), content)
}

#[derive(Debug)]
struct ArtifactCoordinate {
    id: String,
    path: std::path::PathBuf,
}

fn artifact_coordinate(
    media_type: &str,
    digest: &str,
) -> Result<ArtifactCoordinate, ArtifactError> {
    let prefix = digest.get(..2).ok_or(ArtifactError::InvalidReference)?;
    match media_type {
        "application/json" => Ok(ArtifactCoordinate {
            id: format!("mcp-{digest}"),
            path: format!("{ARTIFACT_NAMESPACE}/{prefix}/{digest}.json").into(),
        }),
        PARQUET_ARTIFACT_MEDIA_TYPE => Ok(ArtifactCoordinate {
            id: format!("mcp-parquet-{digest}"),
            path: format!("{ARTIFACT_NAMESPACE}/parquet/{prefix}/{digest}.parquet").into(),
        }),
        _ => Err(ArtifactError::InvalidReference),
    }
}

fn ensure_artifact_read_live(
    context: &ArtifactReadContext,
    worker_cancellation: &CancellationToken,
) -> Result<(), ArtifactError> {
    context.ensure_live()?;
    if worker_cancellation.is_cancelled() {
        Err(ArtifactError::Cancelled)
    } else {
        Ok(())
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_nonblocking_read(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_nonblocking_read(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn publish_staged_artifact(
    _root: &ArtifactRoot,
    directory: &Dir,
    staging_path: &Path,
    artifact_path: &Path,
    guard: &mut StagingGuard<'_>,
) -> Result<(), ArtifactError> {
    match directory.hard_link(staging_path, directory, artifact_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_error) => return Err(ArtifactError::Unavailable),
    }
    guard.remove()
}

#[cfg(windows)]
fn publish_staged_artifact(
    root: &ArtifactRoot,
    directory: &Dir,
    staging_path: &Path,
    artifact_path: &Path,
    guard: &mut StagingGuard<'_>,
) -> Result<(), ArtifactError> {
    if staging_path.parent() != artifact_path.parent() {
        return Err(ArtifactError::Unavailable);
    }
    drop(
        root.resolve(staging_path)
            .map_err(|_| ArtifactError::Unavailable)?,
    );
    drop(
        root.resolve(artifact_path)
            .map_err(|_| ArtifactError::Unavailable)?,
    );
    let parent = staging_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or(ArtifactError::Unavailable)?;
    let _pinned_parent = directory
        .open_dir(parent)
        .map_err(|_| ArtifactError::Unavailable)?;
    root.try_clone_directory()
        .map_err(|_| ArtifactError::Unavailable)?;

    let source = root.root().join(staging_path);
    let destination = root.root().join(artifact_path);
    let publication = atomicwrites::move_atomic(&source, &destination);

    root.try_clone_directory()
        .map_err(|_| ArtifactError::Unavailable)?;
    let source_exists = windows_regular_entry_exists(directory, staging_path)?;
    let destination_exists = windows_regular_entry_exists(directory, artifact_path)?;
    eprintln!(
        "artifact publication diagnostic: move={:?}, raw_os_error={:?}, source_exists={}, destination_exists={}",
        publication.as_ref().err().map(std::io::Error::kind),
        publication
            .as_ref()
            .err()
            .and_then(std::io::Error::raw_os_error),
        source_exists,
        destination_exists,
    );
    match publication {
        Ok(()) if !source_exists && destination_exists => {
            guard.disarm();
            Ok(())
        }
        Err(_error) if source_exists && destination_exists => guard.remove(),
        Err(_) | Ok(()) => Err(ArtifactError::Unavailable),
    }
}

#[cfg(not(any(unix, windows)))]
fn publish_staged_artifact(
    _root: &ArtifactRoot,
    _directory: &Dir,
    _staging_path: &Path,
    _artifact_path: &Path,
    _guard: &mut StagingGuard<'_>,
) -> Result<(), ArtifactError> {
    Err(ArtifactError::Unavailable)
}

#[cfg(unix)]
fn synchronize_publication_directories(
    directory: &Dir,
    artifact_path: &Path,
) -> Result<(), ArtifactError> {
    use cap_std::fs::OpenOptionsExt as _;

    let parent = artifact_path.parent().ok_or(ArtifactError::Unavailable)?;
    let namespace = parent.parent().ok_or(ArtifactError::Unavailable)?;
    for path in [
        parent,
        namespace,
        Path::new(ARTIFACT_NAMESPACE),
        Path::new("mcp"),
        Path::new("."),
    ] {
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
        directory
            .open_with(path, &options)
            .map(cap_std::fs::File::into_std)
            .and_then(|opened| opened.sync_all())
            .map_err(|_| ArtifactError::Unavailable)?;
    }
    Ok(())
}

#[cfg(windows)]
fn synchronize_publication_directories(
    _directory: &Dir,
    _artifact_path: &Path,
) -> Result<(), ArtifactError> {
    // The staged file is synchronized before capability-relative, no-clobber hard-link
    // publication. Windows does not expose the Unix directory-fsync contract; publication is
    // reopened and verified before its opaque reference is returned.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn synchronize_publication_directories(
    _directory: &Dir,
    _artifact_path: &Path,
) -> Result<(), ArtifactError> {
    Err(ArtifactError::Unavailable)
}

#[cfg(windows)]
fn windows_regular_entry_exists(directory: &Dir, path: &Path) -> Result<bool, ArtifactError> {
    match directory.symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(ArtifactError::Unavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_error) => Err(ArtifactError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc, LazyLock,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    use market_squawk_services::{ArtifactError, ArtifactReadContext};
    use tokio_util::sync::CancellationToken;

    use super::ArtifactReadSupervisor;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    static ARTIFACT_READ_TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    #[tokio::test]
    async fn dropped_artifact_waiter_is_reaped_and_capacity_recovers_after_worker_exit()
    -> TestResult {
        let _serial = ARTIFACT_READ_TEST_SERIAL.lock().await;
        let supervisor = ArtifactReadSupervisor::try_new(NonZeroUsize::MIN)?;
        let (started_sender, mut started) = tokio::sync::mpsc::unbounded_channel();
        let (cancelled_sender, mut cancelled) = tokio::sync::mpsc::unbounded_channel();
        let (release, released) = mpsc::sync_channel(1);
        let first_supervisor = supervisor.clone();
        let first = tokio::spawn(async move {
            first_supervisor
                .run(
                    ArtifactReadContext::new(
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(5),
                    ),
                    move |worker_cancellation| {
                        started_sender
                            .send(())
                            .map_err(|_| ArtifactError::Unavailable)?;
                        released.recv().map_err(|_| ArtifactError::Unavailable)?;
                        cancelled_sender
                            .send(worker_cancellation.is_cancelled())
                            .map_err(|_| ArtifactError::Unavailable)?;
                        Ok(())
                    },
                )
                .await
        });
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), started.recv()).await,
            Ok(Some(()))
        ));
        first.abort();
        assert!(first.await.is_err());

        let saturated_worker_started = Arc::new(AtomicBool::new(false));
        let saturated_worker_observation = Arc::clone(&saturated_worker_started);
        let saturated = supervisor
            .run(
                ArtifactReadContext::new(
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5),
                ),
                move |_worker_cancellation| {
                    saturated_worker_observation.store(true, Ordering::Release);
                    Ok(())
                },
            )
            .await;
        assert_eq!(saturated, Err(ArtifactError::Unavailable));
        assert!(!saturated_worker_started.load(Ordering::Acquire));

        release.send(())?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), cancelled.recv()).await?,
            Some(true)
        );

        let recovered_starts = Arc::new(AtomicUsize::new(0));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let recovered_observation = Arc::clone(&recovered_starts);
                match supervisor
                    .run(
                        ArtifactReadContext::new(
                            CancellationToken::new(),
                            Instant::now() + Duration::from_secs(5),
                        ),
                        move |_worker_cancellation| {
                            recovered_observation.fetch_add(1, Ordering::AcqRel);
                            Ok(())
                        },
                    )
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(ArtifactError::Unavailable) => tokio::task::yield_now().await,
                    Err(error) => return Err(error),
                }
            }
        })
        .await??;
        assert_eq!(recovered_starts.load(Ordering::Acquire), 1);
        Ok(())
    }
}
