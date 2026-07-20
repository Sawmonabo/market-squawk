use super::*;

#[derive(Debug)]
struct ScriptedClock {
    readings: Mutex<Vec<ExtractionClockReading>>,
}

#[derive(Debug)]
struct FixedClock(ExtractionClockReading);

#[derive(Debug)]
struct ThreadProbeClock {
    sampled: AtomicBool,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
    sampling_thread: Arc<Mutex<Option<thread::ThreadId>>>,
}

#[derive(Debug)]
struct FailAtClock {
    origin: Instant,
    calls: AtomicUsize,
    fail_at: usize,
}

#[derive(Debug)]
struct SamplingSlotClock {
    calls: AtomicUsize,
    blocked_entered: Arc<Barrier>,
    blocked_release: Arc<Barrier>,
    reused_together: Arc<Barrier>,
}

#[derive(Debug)]
struct BlockedClockRelease {
    barrier: Option<Arc<Barrier>>,
}

#[derive(Debug)]
struct PanickingClock;

impl BlockedClockRelease {
    fn new(barrier: Arc<Barrier>) -> Self {
        Self {
            barrier: Some(barrier),
        }
    }

    fn release(&mut self) {
        if let Some(barrier) = self.barrier.take() {
            barrier.wait();
        }
    }
}

impl Drop for BlockedClockRelease {
    fn drop(&mut self) {
        self.release();
    }
}

impl ExtractionClock for FixedClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        Ok(self.0)
    }
}

impl ExtractionClock for ThreadProbeClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        if !self.sampled.swap(true, Ordering::SeqCst) {
            *self
                .sampling_thread
                .lock()
                .map_err(|_| ExtractionClockError::Unavailable)? = Some(thread::current().id());
            self.entered.wait();
            self.release.wait();
        }
        Ok(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(0),
            Instant::now(),
        ))
    }
}

impl ExtractionClock for FailAtClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.fail_at {
            return Err(ExtractionClockError::Unavailable);
        }
        let offset = u64::try_from(call).map_err(|_| ExtractionClockError::Range)?;
        let wall = i64::try_from(call)
            .ok()
            .and_then(|call| 300_i64.checked_add(call))
            .ok_or(ExtractionClockError::Range)?;
        let monotonic = self
            .origin
            .checked_add(Duration::from_nanos(offset))
            .ok_or(ExtractionClockError::Range)?;
        Ok(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(wall),
            monotonic,
        ))
    }
}

impl ExtractionClock for SamplingSlotClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0..=3 => {
                self.blocked_entered.wait();
                self.blocked_release.wait();
            }
            4..=7 => {
                self.reused_together.wait();
            }
            _ => {}
        }
        Ok(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(0),
            Instant::now(),
        ))
    }
}

impl ExtractionClock for PanickingClock {
    #[allow(
        clippy::panic,
        reason = "this hostile test proves a blocking worker panic becomes a typed adapter error"
    )]
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        panic!("hostile clock panic")
    }
}

pub(super) fn fixed_clock() -> Arc<dyn ExtractionClock> {
    Arc::new(FixedClock(ExtractionClockReading::new(
        Timestamp::from_unix_nanos(300),
        Instant::now(),
    )))
}

fn verify_deadline_sampling_saturation(
    source: FileExtractionSource,
    request: DiscoveryRequest,
    blocked_entered: Arc<Barrier>,
    blocked_release: Arc<Barrier>,
    reused_together: Arc<Barrier>,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .map_err(|error| format!("could not build isolated deadline runtime: {error}"))?;
    let result = runtime.block_on(async move {
        let mut owner_operations = Vec::new();
        for _ in 0..4 {
            let source = source.clone();
            let request = request.clone();
            let cancellation = CancellationToken::new();
            owner_operations.push(tokio::spawn(async move {
                source.discover_files(&request, &cancellation).await
            }));
        }
        let entered = Arc::clone(&blocked_entered);
        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .map_err(|error| format!("sampling-entry barrier task failed: {error}"))?;
        let mut release = BlockedClockRelease::new(blocked_release);

        let fifth_cancellation = CancellationToken::new();
        let fifth_operation = source.discover_files(&request, &fifth_cancellation);
        tokio::pin!(fifth_operation);
        match std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(fifth_operation.as_mut(), context))
        })
        .await
        {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(result) => {
                return Err(format!(
                    "queued sampling operation completed before expiry: {result:?}"
                ));
            }
        }

        tokio::time::advance(Duration::from_secs(1) + Duration::from_nanos(1)).await;
        let fifth_result = fifth_operation.await;
        let mut owner_results = Vec::new();
        for operation in owner_operations {
            owner_results.push(
                operation
                    .await
                    .map_err(|error| format!("sampling owner task failed: {error}"))?,
            );
        }
        release.release();

        if !matches!(fifth_result, Err(FileAdapterError::DeadlineExceeded)) {
            return Err(format!(
                "queued sampling operation did not expire: {fifth_result:?}"
            ));
        }
        if !owner_results
            .iter()
            .all(|result| matches!(result, Err(FileAdapterError::DeadlineExceeded)))
        {
            return Err(format!(
                "sampling owners did not all expire: {owner_results:?}"
            ));
        }

        tokio::time::resume();
        let mut reuse_operations = Vec::new();
        for _ in 0..4 {
            let source = source.clone();
            let request = request.clone();
            reuse_operations.push(tokio::spawn(async move {
                source
                    .discover_files(&request, &CancellationToken::new())
                    .await
            }));
        }
        tokio::task::spawn_blocking(move || reused_together.wait())
            .await
            .map_err(|error| format!("sampling-reuse barrier task failed: {error}"))?;
        for operation in reuse_operations {
            operation
                .await
                .map_err(|error| format!("sampling reuse task failed: {error}"))?
                .map_err(|error| format!("sampling permit was not reusable: {error}"))?;
        }
        Ok(())
    });
    drop(runtime);
    result
}

impl ExtractionClock for ScriptedClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        self.readings
            .lock()
            .map_err(|_| ExtractionClockError::Unavailable)?
            .pop()
            .ok_or(ExtractionClockError::Unavailable)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn discovery_control_path_fails_closed_without_blocking_the_runtime()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"id,value\none,1.00\n")?;
    let manifest = manifest("source.csv", "csv");
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let origin = Instant::now();
    let clock = Arc::new(ScriptedClock {
        readings: Mutex::new(vec![
            ExtractionClockReading::new(
                Timestamp::from_unix_nanos(10),
                origin + Duration::from_nanos(10),
            ),
            ExtractionClockReading::new(Timestamp::from_unix_nanos(0), origin),
        ]),
    });
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let sampling_thread = Arc::new(Mutex::new(None));
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        clock,
    )?;
    let request = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10),
    )?;
    let result = source
        .discover_files(&request, &CancellationToken::new())
        .await;
    assert!(matches!(result, Err(FileAdapterError::DeadlineExceeded)));

    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(ThreadProbeClock {
            sampled: AtomicBool::new(false),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            sampling_thread: Arc::clone(&sampling_thread),
        }),
    )?;
    let responsive_request = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(1_000_000_000),
    )?;
    let operation = tokio::spawn(async move {
        source
            .discover_files(&responsive_request, &CancellationToken::new())
            .await
    });
    tokio::task::spawn_blocking(move || entered.wait()).await?;
    let sampled_off_runtime = sampling_thread
        .lock()
        .map_err(|_| "sampling thread probe lock failed")?
        .is_some_and(|sampling| sampling != thread::current().id());
    tokio::task::spawn_blocking(move || release.wait()).await?;
    assert!(sampled_off_runtime);
    let _ = operation.await??;

    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(PanickingClock),
    )?;
    let panic_request = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(1_000_000_000),
    )?;
    let error = source
        .discover_files(&panic_request, &CancellationToken::new())
        .await
        .err()
        .ok_or("panicking blocking operation unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::BlockingTaskFailed);

    for fail_at in [3, 15] {
        let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
        let manifest_input = root
            .resolve("manifest.json")?
            .open_bounded(16 * 1024)?
            .read_bounded()?;
        let source = FileExtractionSource::try_new_with_clock(
            local_metadata(&manifest)?,
            root,
            manifest_input,
            ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
            Arc::new(FailAtClock {
                origin: Instant::now(),
                calls: AtomicUsize::new(0),
                fail_at,
            }),
        )?;
        let request = DiscoveryRequest::try_new(
            SourceIdentifier::try_from("alternative-prices")?,
            None,
            NonZeroU16::new(1).ok_or("nonzero")?,
            Timestamp::from_unix_nanos(1_000_000_000),
        )?;
        let error = source
            .discover_files(&request, &CancellationToken::new())
            .await
            .err()
            .ok_or("clock failure injection unexpectedly succeeded")?;
        assert_eq!(error, FileAdapterError::ClockFailure, "call {fail_at}");
    }
    for fail_at in [33, 37] {
        let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
        let manifest_input = root
            .resolve("manifest.json")?
            .open_bounded(16 * 1024)?
            .read_bounded()?;
        let source = FileExtractionSource::try_new_with_clock(
            local_metadata(&manifest)?,
            root,
            manifest_input,
            ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
            Arc::new(FailAtClock {
                origin: Instant::now(),
                calls: AtomicUsize::new(0),
                fail_at,
            }),
        )?;
        let discovery = DiscoveryRequest::try_new(
            SourceIdentifier::try_from("alternative-prices")?,
            None,
            NonZeroU16::new(1).ok_or("nonzero")?,
            Timestamp::from_unix_nanos(1_000_000_000),
        )?;
        let object = source
            .discover_files(&discovery, &CancellationToken::new())
            .await?
            .objects()
            .first()
            .cloned()
            .ok_or("control fixture was not discovered")?;
        let request = ExtractionRequest::try_new(
            object,
            NonZeroU32::new(1).ok_or("nonzero")?,
            NonZeroU64::new(16 * 1024).ok_or("nonzero")?,
            Timestamp::from_unix_nanos(1_000_000_000),
        )?;
        let error = source
            .extract_file(&request, &CancellationToken::new())
            .await
            .err()
            .ok_or("clock failure injection unexpectedly succeeded")?;
        assert_eq!(error, FileAdapterError::ClockFailure, "call {fail_at}");
    }

    let blocked_entered = Arc::new(Barrier::new(5));
    let blocked_release = Arc::new(Barrier::new(5));
    let reused_together = Arc::new(Barrier::new(5));
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let mut bounded_limits = ExtractionLimitsInput::standard();
    bounded_limits.max_elapsed = Duration::from_secs(1);
    let bounded_source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        manifest_input,
        ExtractionLimits::try_new(bounded_limits)?,
        Arc::new(SamplingSlotClock {
            calls: AtomicUsize::new(0),
            blocked_entered: Arc::clone(&blocked_entered),
            blocked_release: Arc::clone(&blocked_release),
            reused_together: Arc::clone(&reused_together),
        }),
    )?;
    let bounded_request = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    tokio::task::spawn_blocking(move || {
        verify_deadline_sampling_saturation(
            bounded_source,
            bounded_request,
            blocked_entered,
            blocked_release,
            reused_together,
        )
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn discovery_uses_half_open_manifest_intervals_before_file_reads()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"id,value\none,1.00\n")?;
    let manifest = manifest_with_superseded("source.csv", "csv", Some(500));
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        fixed_clock(),
    )?;
    let before = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        Some(Timestamp::from_unix_nanos(499)),
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    assert_eq!(
        source
            .discover_files(&before, &CancellationToken::new())
            .await?
            .objects()
            .len(),
        1
    );

    fs::remove_file(directory.path().join("source.csv"))?;
    let at_supersession = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        Some(Timestamp::from_unix_nanos(500)),
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    assert!(
        source
            .discover_files(&at_supersession, &CancellationToken::new())
            .await?
            .objects()
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn extraction_rejects_a_discovered_object_transplanted_from_another_source()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"id,value\none,1.00\n")?;
    let manifest = manifest("source.csv", "csv");
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        fixed_clock(),
    )?;
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let discovered = source
        .discover_files(&discovery, &CancellationToken::new())
        .await?;
    let object = &discovered.objects()[0];
    let transplanted = SourceObject::try_new(
        SourceId::try_from("unrelated-local-source")?,
        object.metadata_revision().clone(),
        &discovery,
        object.object_id().clone(),
        object.media_type().clone(),
        object.evidence().clone(),
        object.effective_interval(),
        object.published_at(),
        object.expected_bytes(),
    )?;
    fs::remove_file(directory.path().join("source.csv"))?;
    let request = ExtractionRequest::try_new(
        transplanted,
        NonZeroU32::new(16).ok_or("nonzero")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let error = source
        .extract_file(&request, &CancellationToken::new())
        .await
        .err()
        .ok_or("transplanted source object unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::ObjectLineageMismatch);
    Ok(())
}
