mod common;

use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use market_squawk_domain::ConnectionGeneration;
use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorityStateStore, AuthorityStateStoreError, BudgetDecision,
    BudgetUnavailableReason, CaptureGenerationHealth, RegistryError, RetryAfter, SessionId,
    SourceError, TransportFrameKind,
};

use common::{TestResult, direct_metadata, now_timestamp, source_identifier};

const CHILD_PHASE: &str = "MARKET_SQUAWK_AUTHORITY_CHILD_PHASE";
const CHILD_ROOT: &str = "MARKET_SQUAWK_AUTHORITY_CHILD_ROOT";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Default)]
struct FailingMemoryStore {
    payload: Mutex<Option<Vec<u8>>>,
    reject_stores: AtomicBool,
}

impl FailingMemoryStore {
    fn reject_stores(&self) {
        self.reject_stores.store(true, Ordering::Release);
    }
}

impl AuthorityStateStore for FailingMemoryStore {
    fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
        self.payload
            .lock()
            .map(|payload| payload.clone())
            .map_err(|_| AuthorityStateStoreError::Unavailable)
    }

    fn store(&self, payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
        if self.reject_stores.load(Ordering::Acquire) {
            return Err(AuthorityStateStoreError::Unavailable);
        }
        self.payload
            .lock()
            .map_err(|_| AuthorityStateStoreError::Unavailable)?
            .replace(payload.to_vec());
        Ok(())
    }
}

#[derive(Debug)]
struct TemporaryAuthorityRoot(PathBuf);

impl TemporaryAuthorityRoot {
    fn try_new(label: &str) -> TestResult<Self> {
        let sequence = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!(
            "market-squawk-authority-{label}-{}-{nanos}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&root)?;
        Ok(Self(root))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryAuthorityRoot {
    fn drop(&mut self) {
        let _result = fs::remove_dir_all(&self.0);
    }
}

fn run_child(root: &Path, phase: &str) -> TestResult {
    let output = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("durable_authority_child")
        .arg("--nocapture")
        .env(CHILD_PHASE, phase)
        .env(CHILD_ROOT, root)
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "authority child phase {phase} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .into())
}

fn open_registry(root: &Path) -> TestResult<AuthoritativeSourceRegistry> {
    let store: Arc<dyn AuthorityStateStore> = Arc::new(LocalAuthorityStateStore::try_open(root)?);
    Ok(AuthoritativeSourceRegistry::try_new_durable(store)?)
}

fn register_revision(
    registry: &mut AuthoritativeSourceRegistry,
    source: &str,
    revision: &str,
) -> TestResult<market_squawk_sources::RegisteredSource> {
    Ok(registry.register(
        direct_metadata(source, revision, 0, None)?,
        now_timestamp()?,
    )?)
}

fn metadata_for_provider(
    source: &str,
    revision: &str,
    provider: &str,
) -> TestResult<market_squawk_sources::SourceMetadata> {
    let mut wire = serde_json::to_value(direct_metadata(source, revision, 0, None)?)?;
    let metadata = wire
        .as_object_mut()
        .ok_or("source metadata did not serialize as an object")?;
    metadata.insert("provider".to_owned(), serde_json::json!(provider));
    let budget = metadata
        .get_mut("budget")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source budget missing")?;
    let scope = budget
        .get_mut("scope")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source budget scope missing")?;
    scope.insert("provider".to_owned(), serde_json::json!(provider));
    let allowlist = metadata
        .get_mut("network")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|network| network.get_mut("allowlisted"))
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source allowlist missing")?;
    allowlist.insert(
        "endpoints".to_owned(),
        serde_json::json!([format!("wss://{provider}.example.test/feed")]),
    );
    Ok(serde_json::from_value(wire)?)
}

fn open_memory_registry(
    store: Arc<FailingMemoryStore>,
) -> Result<AuthoritativeSourceRegistry, RegistryError> {
    let store: Arc<dyn AuthorityStateStore> = store;
    AuthoritativeSourceRegistry::try_new_durable(store)
}

#[test]
fn clean_child_restarts_preserve_capacity_cooldown_and_disable() -> TestResult {
    let request_root = TemporaryAuthorityRoot::try_new("requests")?;
    run_child(request_root.path(), "request-write")?;
    run_child(request_root.path(), "request-read")?;

    let cooldown_root = TemporaryAuthorityRoot::try_new("cooldown")?;
    run_child(cooldown_root.path(), "cooldown-write")?;
    run_child(cooldown_root.path(), "cooldown-read")?;

    let disabled_root = TemporaryAuthorityRoot::try_new("disabled")?;
    run_child(disabled_root.path(), "disabled-write")?;
    run_child(disabled_root.path(), "disabled-read")?;
    Ok(())
}

#[test]
fn unclean_child_restart_terminally_bars_live_authority() -> TestResult {
    let root = TemporaryAuthorityRoot::try_new("unclean")?;
    run_child(root.path(), "unclean-write")?;
    run_child(root.path(), "unclean-read")?;
    Ok(())
}

#[test]
fn write_failures_publish_no_new_authority_and_terminalize_restrictive_transitions() -> TestResult {
    let registration_store = Arc::new(FailingMemoryStore::default());
    let mut registration_registry = open_memory_registry(registration_store.clone())?;
    registration_store.reject_stores();
    assert!(matches!(
        registration_registry.register(
            metadata_for_provider(
                "failed-registration",
                "revision-1",
                "failed-registration-provider"
            )?,
            now_timestamp()?
        ),
        Err(RegistryError::AuthorityPersistence)
    ));

    let restrictive_store = Arc::new(FailingMemoryStore::default());
    let mut restrictive_registry = open_memory_registry(restrictive_store.clone())?;
    let registered = restrictive_registry.register(
        metadata_for_provider(
            "failed-restriction",
            "revision-1",
            "failed-restriction-provider",
        )?,
        now_timestamp()?,
    )?;
    let budget = registered
        .budget()
        .ok_or("restrictive budget missing")?
        .clone();
    restrictive_store.reject_stores();
    assert!(matches!(
        budget.disable(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(matches!(
        restrictive_registry.revoke(&registered, now_timestamp()?),
        Err(RegistryError::AuthorityPersistence)
    ));
    assert!(matches!(
        restrictive_registry.validate_registered(&registered, now_timestamp()?),
        Err(RegistryError::SourceRevoked)
    ));
    Ok(())
}

#[test]
fn clean_shutdown_requires_reconciled_sessions_and_provider_permits() -> TestResult {
    let active_store = Arc::new(FailingMemoryStore::default());
    let mut active_registry = open_memory_registry(active_store)?;
    let active = active_registry.register(
        metadata_for_provider("active-shutdown", "revision-1", "active-shutdown-provider")?,
        now_timestamp()?,
    )?;
    let _session = active_registry.begin_session(
        &active,
        SessionId::new(source_identifier("active-shutdown-session")?),
        ConnectionGeneration::new(1)?,
        now_timestamp()?,
    )?;
    assert!(matches!(
        active_registry.shutdown(),
        Err(RegistryError::ActiveAuthorityAtShutdown)
    ));

    let permit_store = Arc::new(FailingMemoryStore::default());
    let mut permit_registry = open_memory_registry(permit_store)?;
    let registered = permit_registry.register(
        metadata_for_provider("permit-shutdown", "revision-1", "permit-shutdown-provider")?,
        now_timestamp()?,
    )?;
    let budget = registered.budget().ok_or("permit budget missing")?.clone();
    let permit = match budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("unexpected permit decision: {other:?}").into()),
    };
    assert!(matches!(
        permit_registry.shutdown(),
        Err(RegistryError::AuthorityPersistence)
    ));
    permit.release();
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    Ok(())
}

#[test]
fn one_canonical_allocation_rejects_conflicting_durability_stores() -> TestResult {
    let mut first = open_memory_registry(Arc::new(FailingMemoryStore::default()))?;
    let mut second = open_memory_registry(Arc::new(FailingMemoryStore::default()))?;
    let _registered = first.register(
        metadata_for_provider("store-owner", "revision-1", "shared-store-provider")?,
        now_timestamp()?,
    )?;
    assert!(matches!(
        second.register(
            metadata_for_provider("store-contender", "revision-1", "shared-store-provider")?,
            now_timestamp()?
        ),
        Err(RegistryError::BudgetCoordinator)
    ));
    first.shutdown()?;
    second.shutdown()?;
    Ok(())
}

#[test]
fn failed_restore_rolls_back_the_unpublished_run_to_clean() -> TestResult {
    let root = TemporaryAuthorityRoot::try_new("restore-rollback")?;
    run_child(root.path(), "constructor-conflict-write")?;

    let mut existing = open_memory_registry(Arc::new(FailingMemoryStore::default()))?;
    let _registered = existing.register(
        direct_metadata("constructor-conflict-existing", "revision-1", 0, None)?,
        now_timestamp()?,
    )?;
    let result = AuthoritativeSourceRegistry::try_new_durable(Arc::new(
        LocalAuthorityStateStore::try_open(root.path())?,
    ));
    assert!(matches!(result, Err(RegistryError::BudgetCoordinator)));

    let store = LocalAuthorityStateStore::try_open(root.path())?;
    let payload = store.load()?.ok_or("rolled-back payload missing")?;
    let wire: serde_json::Value = serde_json::from_slice(&payload)?;
    assert_eq!(wire.get("run_state"), Some(&serde_json::json!("clean")));
    existing.shutdown()?;
    Ok(())
}

#[test]
fn nonclean_registry_drop_revokes_retained_request_capture_and_live_capabilities() -> TestResult {
    let mut registry = open_memory_registry(Arc::new(FailingMemoryStore::default()))?;
    let registered = registry.register(
        metadata_for_provider(
            "retained-unclean",
            "revision-1",
            "retained-unclean-provider",
        )?,
        now_timestamp()?,
    )?;
    let budget = registered
        .budget()
        .ok_or("retained budget missing")?
        .clone();
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("retained-unclean-session")?),
        ConnectionGeneration::new(1)?,
        now_timestamp()?,
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let capture_lease = capabilities.lease().clone();
    let mut raw_frames = registry.take_raw_frame_factory(&session)?;

    drop(registry);

    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(matches!(
        session.validate_current_lease(),
        Err(RegistryError::SessionNotCurrent)
    ));
    assert_eq!(capture_lease.health(), CaptureGenerationHealth::Incomplete);
    assert!(matches!(
        raw_frames.try_frame(
            now_timestamp()?,
            TransportFrameKind::Text,
            Bytes::from_static(b"{}")
        ),
        Err(SourceError::SessionNotCurrent)
    ));
    Ok(())
}

#[test]
fn durable_authority_child() -> TestResult {
    let Ok(phase) = std::env::var(CHILD_PHASE) else {
        return Ok(());
    };
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).ok_or("child root missing")?);
    match phase.as_str() {
        "request-write" => {
            let mut registry = open_registry(&root)?;
            let registered = register_revision(&mut registry, "durable-request", "revision-1")?;
            let budget = registered.budget().ok_or("registered budget missing")?;
            for _ in 0..9 {
                let BudgetDecision::Ready(permit) = budget.try_acquire() else {
                    return Err("request capacity was exhausted before nine reservations".into());
                };
                permit.release();
            }
            registry.shutdown()?;
        }
        "request-read" => {
            let mut registry = open_registry(&root)?;
            let registered = register_revision(&mut registry, "durable-request", "revision-2")?;
            let budget = registered.budget().ok_or("restored budget missing")?;
            let BudgetDecision::Ready(permit) = budget.try_acquire() else {
                return Err("restored request capacity did not preserve the final slot".into());
            };
            permit.release();
            assert!(matches!(budget.try_acquire(), BudgetDecision::WaitUntil(_)));
            registry.shutdown()?;
        }
        "cooldown-write" => {
            let mut registry = open_registry(&root)?;
            let registered = register_revision(&mut registry, "durable-cooldown", "revision-1")?;
            let budget = registered.budget().ok_or("registered budget missing")?;
            assert!(matches!(
                budget.apply_retry_after(RetryAfter::Delay(
                    NonZeroU64::new(30_000_000_000).ok_or("retry delay must be nonzero")?
                )),
                BudgetDecision::WaitUntil(_)
            ));
            registry.shutdown()?;
        }
        "cooldown-read" => {
            let mut registry = open_registry(&root)?;
            let registered = register_revision(&mut registry, "durable-cooldown", "revision-2")?;
            assert!(matches!(
                registered
                    .budget()
                    .ok_or("restored budget missing")?
                    .try_acquire(),
                BudgetDecision::WaitUntil(_)
            ));
            registry.shutdown()?;
        }
        "disabled-write" => {
            let mut registry = open_registry(&root)?;
            let registered = register_revision(&mut registry, "durable-disabled", "revision-1")?;
            assert!(matches!(
                registered
                    .budget()
                    .ok_or("registered budget missing")?
                    .disable(),
                BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
            ));
            registry.shutdown()?;
        }
        "disabled-read" => {
            let mut registry = open_registry(&root)?;
            let registered = register_revision(&mut registry, "durable-disabled", "revision-2")?;
            assert!(matches!(
                registered
                    .budget()
                    .ok_or("restored budget missing")?
                    .try_acquire(),
                BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
            ));
            registry.shutdown()?;
        }
        "unclean-write" => {
            let mut registry = open_registry(&root)?;
            let registered = register_revision(&mut registry, "durable-unclean", "revision-1")?;
            let permit = match registered
                .budget()
                .ok_or("registered budget missing")?
                .try_acquire()
            {
                BudgetDecision::Ready(permit) => permit,
                other => return Err(format!("unexpected unclean acquire: {other:?}").into()),
            };
            std::mem::forget(permit);
            drop(registry);
        }
        "constructor-conflict-write" => {
            let mut registry = open_registry(&root)?;
            let _registered =
                register_revision(&mut registry, "constructor-conflict", "revision-1")?;
            registry.shutdown()?;
        }
        "unclean-read" => {
            let store: Arc<dyn AuthorityStateStore> =
                Arc::new(LocalAuthorityStateStore::try_open(&root)?);
            assert!(matches!(
                AuthoritativeSourceRegistry::try_new_durable(store),
                Err(RegistryError::UncleanAuthorityPredecessor)
            ));
        }
        other => return Err(format!("unknown authority child phase: {other}").into()),
    }
    Ok(())
}
