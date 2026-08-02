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
struct BlockingWorkerClock {
    calls: AtomicUsize,
    sealing_together: Arc<Barrier>,
    workers_entered: Arc<Barrier>,
    workers_release: Arc<Barrier>,
}

#[derive(Debug)]
struct RevocationReadClock {
    calls: AtomicUsize,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
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

impl ExtractionClock for BlockingWorkerClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0..=3 => {
                self.sealing_together.wait();
            }
            4..=7 => {
                self.workers_entered.wait();
                self.workers_release.wait();
            }
            _ => {}
        }
        Ok(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(300),
            Instant::now(),
        ))
    }
}

impl ExtractionClock for RevocationReadClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 3 {
            self.entered.wait();
            self.release.wait();
        }
        Ok(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(300),
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

#[tokio::test]
async fn revocation_during_blocking_read_prevents_discovery_publication()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let representation_state = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"id,value\nrow,1.00\n")?;
    let manifest = manifest("source.csv", "csv");
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let metadata = local_metadata(&manifest)?;
    let source = FileExtractionSource::try_new_with_clock(
        metadata.clone(),
        root,
        representation_state_root(&representation_state, &manifest),
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(RevocationReadClock {
            calls: AtomicUsize::new(0),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }),
    )?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata, Timestamp::from_unix_nanos(300))?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let request = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let operation = tokio::spawn(async move {
        source
            .discover_files(&authority, &request, &CancellationToken::new())
            .await
    });

    tokio::task::spawn_blocking(move || entered.wait()).await?;
    registry.revoke(&registered, Timestamp::from_unix_nanos(300))?;
    tokio::task::spawn_blocking(move || release.wait()).await?;

    assert!(matches!(
        operation.await?,
        Err(FileAdapterError::Authority(
            market_squawk_sources::ExtractionAuthorityError::NotCurrent
        ))
    ));
    Ok(())
}

fn verify_deadline_sampling_saturation(
    source: AuthorizedFileSource,
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

fn verify_blocking_worker_cancellation(
    source: AuthorizedFileSource,
    request: DiscoveryRequest,
    sealing_together: Arc<Barrier>,
    workers_entered: Arc<Barrier>,
    workers_release: Arc<Barrier>,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .map_err(|error| format!("could not build isolated blocking-worker runtime: {error}"))?;
    let result = runtime.block_on(async move {
        let mut cancellations = Vec::new();
        let mut operations = Vec::new();
        for _ in 0..4 {
            let source = source.clone();
            let request = request.clone();
            let cancellation = CancellationToken::new();
            cancellations.push(cancellation.clone());
            operations.push(tokio::spawn(async move {
                source.discover_files(&request, &cancellation).await
            }));
        }
        tokio::task::spawn_blocking(move || sealing_together.wait())
            .await
            .map_err(|error| format!("sealing barrier task failed: {error}"))?;
        tokio::task::spawn_blocking({
            let workers_entered = Arc::clone(&workers_entered);
            move || workers_entered.wait()
        })
        .await
        .map_err(|error| format!("worker-entry barrier task failed: {error}"))?;
        let mut release = BlockedClockRelease::new(workers_release);
        for cancellation in &cancellations {
            cancellation.cancel();
        }

        let mut completed = 0;
        for operation in &mut operations {
            let result = tokio::select! {
                biased;
                result = operation => Some(result),
                () = tokio::time::sleep(Duration::from_secs(1)) => None,
            };
            let Some(result) = result else {
                break;
            };
            require_cancelled(result)?;
            completed += 1;
        }
        let callers_released = completed == operations.len();

        let queued_cancellation = CancellationToken::new();
        let mut queued_operation = tokio::spawn({
            let source = source.clone();
            let request = request.clone();
            let queued_cancellation = queued_cancellation.clone();
            async move { source.discover_files(&request, &queued_cancellation).await }
        });
        queued_cancellation.cancel();
        let queued_result = tokio::select! {
            biased;
            result = &mut queued_operation => Some(result),
            () = tokio::time::sleep(Duration::from_secs(1)) => None,
        };
        let queued_released = queued_result.is_some();
        if let Some(result) = queued_result {
            require_cancelled(result)?;
        }

        release.release();
        for operation in operations.into_iter().skip(completed) {
            require_cancelled(operation.await)?;
        }
        if !queued_released {
            require_cancelled(queued_operation.await)?;
        }
        if !callers_released {
            return Err("cancelled callers remained joined to four blocking workers".to_owned());
        }
        if !queued_released {
            return Err(
                "a fifth caller did not fail promptly while detached workers held permits"
                    .to_owned(),
            );
        }
        Ok(())
    });
    drop(runtime);
    result
}

fn require_cancelled<T>(
    result: Result<Result<T, FileAdapterError>, tokio::task::JoinError>,
) -> Result<(), String> {
    match result {
        Ok(Err(FileAdapterError::Cancelled)) => Ok(()),
        Ok(Err(error)) => Err(format!("caller returned the wrong typed error: {error}")),
        Ok(Ok(_)) => Err("cancelled caller unexpectedly succeeded".to_owned()),
        Err(error) => Err(format!("cancelled caller task failed: {error}")),
    }
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
    let representation_state = tempfile::tempdir()?;
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
    let source = authorize_source(FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root_for(&representation_state, &manifest, "scripted"),
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        clock,
    )?)?;
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
    let source = authorize_source(FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root_for(&representation_state, &manifest, "thread-probe"),
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(ThreadProbeClock {
            sampled: AtomicBool::new(false),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            sampling_thread: Arc::clone(&sampling_thread),
        }),
    )?)?;
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
    let source = authorize_source(FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root_for(&representation_state, &manifest, "panic"),
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(PanickingClock),
    )?)?;
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

    let clock_fault_deadline = Timestamp::from_unix_nanos(60_000_000_000);
    for fail_at in [3, 15] {
        let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
        let manifest_input = root
            .resolve("manifest.json")?
            .open_bounded(16 * 1024)?
            .read_bounded()?;
        let source = authorize_source(FileExtractionSource::try_new_with_clock(
            local_metadata(&manifest)?,
            root,
            representation_state_root_for(
                &representation_state,
                &manifest,
                &format!("discovery-fail-{fail_at}"),
            ),
            manifest_input,
            ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
            Arc::new(FailAtClock {
                origin: Instant::now(),
                calls: AtomicUsize::new(0),
                fail_at,
            }),
        )?)?;
        let request = DiscoveryRequest::try_new(
            SourceIdentifier::try_from("alternative-prices")?,
            None,
            NonZeroU16::new(1).ok_or("nonzero")?,
            clock_fault_deadline,
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
        let source = authorize_source(FileExtractionSource::try_new_with_clock(
            local_metadata(&manifest)?,
            root,
            representation_state_root_for(
                &representation_state,
                &manifest,
                &format!("extraction-fail-{fail_at}"),
            ),
            manifest_input,
            ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
            Arc::new(FailAtClock {
                origin: Instant::now(),
                calls: AtomicUsize::new(0),
                fail_at,
            }),
        )?)?;
        let discovery = DiscoveryRequest::try_new(
            SourceIdentifier::try_from("alternative-prices")?,
            None,
            NonZeroU16::new(1).ok_or("nonzero")?,
            clock_fault_deadline,
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
            clock_fault_deadline,
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
    let bounded_source = authorize_source(FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root_for(&representation_state, &manifest, "deadline-sampling"),
        manifest_input,
        ExtractionLimits::try_new(bounded_limits)?,
        Arc::new(SamplingSlotClock {
            calls: AtomicUsize::new(0),
            blocked_entered: Arc::clone(&blocked_entered),
            blocked_release: Arc::clone(&blocked_release),
            reused_together: Arc::clone(&reused_together),
        }),
    )?)?;
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

    let sealing_together = Arc::new(Barrier::new(5));
    let workers_entered = Arc::new(Barrier::new(5));
    let workers_release = Arc::new(Barrier::new(5));
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = authorize_source(FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root_for(&representation_state, &manifest, "blocking-workers"),
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        Arc::new(BlockingWorkerClock {
            calls: AtomicUsize::new(0),
            sealing_together: Arc::clone(&sealing_together),
            workers_entered: Arc::clone(&workers_entered),
            workers_release: Arc::clone(&workers_release),
        }),
    )?)?;
    let request = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    tokio::task::spawn_blocking(move || {
        verify_blocking_worker_cancellation(
            source,
            request,
            sealing_together,
            workers_entered,
            workers_release,
        )
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn discovery_uses_half_open_manifest_intervals_before_file_reads()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let representation_state = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"id,value\none,1.00\n")?;
    let manifest = manifest_with_superseded("source.csv", "csv", Some(500));
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = authorize_source(FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root_for(&representation_state, &manifest, "half-open"),
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        fixed_clock(),
    )?)?;
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
    let representation_state = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"id,value\none,1.00\n")?;
    let manifest = manifest("source.csv", "csv");
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = authorize_source(FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        representation_state_root_for(&representation_state, &manifest, "transplant"),
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        fixed_clock(),
    )?)?;
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
