use std::sync::Arc;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{LocalPaths, SecretValue};
use market_squawk_services::{JsonStructureLimits, RequestId};
use serde_json::json;
use sha2::Digest as _;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn service_restart_rejects_prior_in_memory_credentials() -> TestResult {
    let clients = [
        (client_id(11)?, NamedClient::Desktop),
        (client_id(12)?, NamedClient::Cli),
    ];
    let (first_registry, first) = CredentialRegistry::provision_set(clients)?;
    let prior = first_registry.credential(&first[0])?;
    let (restarted_registry, restarted) = CredentialRegistry::provision_set(clients)?;
    assert_eq!(first, restarted);
    assert!(matches!(
        restarted_registry.authenticate(
            restarted[0].client_id(),
            restarted[0].generation(),
            prior.expose_secret().as_bytes(),
        ),
        Err(CredentialError::AuthenticationFailed)
    ));
    Ok(())
}

#[test]
fn request_identity_mismatches_fail_closed_before_dispatch() -> TestResult {
    let expected = runtime_identity(1, 2, 3)?;
    let structure = JsonStructureLimits::try_new(8, 1_024, 64, 64)?;
    let request = request_for(runtime_identity(4, 2, 3)?, structure)?;
    assert_eq!(
        request.arguments()["secret"],
        "native-credential-redaction-sentinel"
    );
    assert!(!format!("{request:?}").contains("native-credential-redaction-sentinel"));
    assert_eq!(
        expected.admit(&request),
        Err(RuntimeAdmissionError::InstallationMismatch)
    );

    let request = request_for(runtime_identity(1, 4, 3)?, structure)?;
    assert_eq!(
        expected.admit(&request),
        Err(RuntimeAdmissionError::WorkspaceMismatch)
    );

    let request = request_for(runtime_identity(1, 2, 4)?, structure)?;
    assert_eq!(
        expected.admit(&request),
        Err(RuntimeAdmissionError::GenerationMismatch)
    );
    Ok(())
}

#[test]
fn mutation_replay_requires_the_original_digest_and_reuses_terminal_response() -> TestResult {
    let guard = MutationReplayGuard::try_new(ReplayLimits::try_new(8)?)?;
    let key = ReplayKey::new(client_id(7)?, RequestId::Integer(42));
    let first_digest = digest(1);
    let changed_digest = digest(2);
    let generation = ServiceGeneration::try_new(1)?;

    let ReplayAdmission::Execute(permit) = guard.begin(key.clone(), first_digest)? else {
        return Err("new mutation unexpectedly completed".into());
    };
    let response = AppResponseEnvelope::try_success(
        RequestId::Integer(42),
        generation,
        json!({"status": "accepted"}),
        JsonStructureLimits::try_new(8, 1_024, 64, 64)?,
        1_024,
    )?;
    permit.complete(response.clone())?;

    assert_eq!(
        guard.begin(key.clone(), first_digest)?,
        ReplayAdmission::Completed(response)
    );
    assert_eq!(
        guard.begin(key, changed_digest),
        Err(ReplayError::DigestConflict)
    );
    Ok(())
}

#[tokio::test]
async fn read_abort_reaches_service_workers_and_never_cancels_mutations() -> TestResult {
    use market_squawk_services::{RequestContext, ServiceLimits};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;

    #[derive(Debug)]
    struct Dispatcher {
        started: tokio::sync::Notify,
        stopped: Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl ApplicationDispatcher for Dispatcher {
        fn bootstrap(&self) -> Result<serde_json::Value, DispatchError> {
            Ok(json!({}))
        }
        fn effect(&self, operation: &SourceIdentifier) -> Result<OperationEffect, DispatchError> {
            Ok(if operation.as_str() == "Test.Mutate" {
                OperationEffect::Mutation
            } else {
                OperationEffect::Read
            })
        }
        async fn dispatch(
            &self,
            request: &AppRequestEnvelope,
            context: RequestContext,
        ) -> Result<serde_json::Value, DispatchError> {
            if request.operation().as_str() == "Test.Quick" {
                return Ok(json!({"done": true}));
            }
            let cancellation = context.cancellation().clone();
            let stopped = Arc::clone(&self.stopped);
            // This detached worker outlives the HTTP future; only service-context cancellation
            // can stop it. The cancellation route must work with the sole normal slot occupied.
            tokio::spawn(async move {
                cancellation.cancelled().await;
                stopped.notify_one();
            });
            self.started.notify_one();
            std::future::pending().await
        }
        fn mutation_response_committed(&self) -> Result<(), DispatchError> {
            Ok(())
        }
    }

    let directory = TempDir::new()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let runtime = runtime_identity(1, 2, 3)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?;
    let structure = JsonStructureLimits::try_new(16, 4_096, 64, 64)?;
    let (credentials, registrations) =
        CredentialRegistry::provision_set([(client_id(5)?, NamedClient::Desktop)])?;
    let secret = credentials.credential(&registrations[0])?;
    let dispatcher = Arc::new(Dispatcher {
        started: tokio::sync::Notify::new(),
        stopped: Arc::new(tokio::sync::Notify::new()),
    });
    let server = RuntimeRouter::try_new(
        runtime,
        endpoint,
        ApplicationProtocolRange::single(ApplicationProtocolVersion::V1),
        OriginPolicy::try_new([])?,
        RuntimeRouterLimits::try_new(
            4_096,
            4_096,
            2_048,
            1,
            Duration::from_secs(5),
            Duration::from_secs(5),
            structure,
            ServiceLimits::try_new(4_096, 64, 4_096, 64, structure)?,
        )?,
        Arc::new(credentials),
        dispatcher.clone(),
        Arc::new(MutationReplayGuard::try_new(ReplayLimits::try_new(2)?)?),
        Arc::new(EventHub::try_new(
            runtime.service_generation(),
            EventHubLimits::try_new(2, 4_096)?,
            ApplicationChanges::default(),
        )?),
        Arc::new(InputStager::new(
            paths.artifacts()?.clone(),
            runtime,
            InputStagingLimits::try_new(2, 4_096)?,
        )),
    )?
    .start(listener, None)?;
    let now = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let scope = ApplicationRequestScope::try_new(
        runtime,
        client_id(5)?,
        registrations[0].generation(),
        CorrelationId::try_from_uuid(Uuid::from_u128(6))?,
        structure,
        4_096,
    )?;
    let rendezvous = RendezvousRecord::try_new(
        runtime,
        endpoint,
        ApplicationProtocolRange::single(ApplicationProtocolVersion::V1),
        ProcessIdentity::try_new(7, 9)?,
        now,
    )?;
    let client = Arc::new(LoopbackApplicationClient::try_new(
        &rendezvous,
        scope.clone(),
        SecretValue::new(secret.expose_secret().to_owned())?,
        None,
        4_096,
        structure,
        Duration::from_secs(5),
    )?);

    let cancellation = CancellationToken::new();
    let read_client = Arc::clone(&client);
    let read_cancel = cancellation.clone();
    let read = tokio::spawn(async move {
        read_client
            .invoke_read_operation(
                RequestId::Integer(10),
                "Test.Slow",
                json!({}),
                Duration::from_secs(5),
                read_cancel,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), dispatcher.started.notified()).await?;
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), read).await??,
        Err(ApplicationClientError::Interrupted)
    ));
    tokio::time::timeout(Duration::from_secs(2), dispatcher.stopped.notified()).await?;
    // Capacity one is reusable after cancellation and after successful completion.
    for id in [11, 12] {
        client
            .invoke_read_operation(
                RequestId::Integer(id),
                "Test.Quick",
                json!({}),
                Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await?;
    }
    assert!(matches!(
        client
            .invoke_read_operation(
                RequestId::Integer(13),
                "Test.Mutate",
                json!({}),
                Duration::from_secs(5),
                CancellationToken::new()
            )
            .await,
        Err(ApplicationClientError::Rejected)
    ));

    // Abort after registration and before execution leaves no executable request or tombstone.
    let now = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let early = scope.request(
        RequestId::Integer(14),
        now.checked_add_nanos(5_000_000_000)?,
        now,
        SourceIdentifier::try_from("Test.Slow")?,
        json!({}),
    )?;
    let http = reqwest::Client::builder().no_proxy().build()?;
    let post = |path: &str| {
        http.post(format!("http://{endpoint}{path}"))
            .header(
                CLIENT_ID_HEADER,
                client_id(5).expect("fixed client").as_uuid().to_string(),
            )
            .header(
                INSTALLATION_ID_HEADER,
                runtime.installation_id().as_uuid().to_string(),
            )
            .header(
                WORKSPACE_ID_HEADER,
                runtime.workspace_id().as_uuid().to_string(),
            )
            .header(
                SERVICE_GENERATION_HEADER,
                runtime.service_generation().get(),
            )
            .header(
                CREDENTIAL_GENERATION_HEADER,
                registrations[0].generation().get(),
            )
            .bearer_auth(secret.expose_secret())
    };
    assert_eq!(
        post("/app/v1/register-read")
            .json(&early)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NO_CONTENT
    );
    let sha256 = format!("{:x}", sha2::Sha256::digest(serde_json::to_vec(&early)?));
    let cancellation = json!({"requestId": early.request_id(), "requestSha256": sha256});
    assert_eq!(
        post("/app/v1/cancel-read")
            .json(&cancellation)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(
        post("/app/v1/invoke-read")
            .json(&early)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::GONE
    );
    assert_eq!(
        post("/app/v1/cancel-read")
            .json(&cancellation)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NO_CONTENT
    );
    client
        .invoke_read_operation(
            RequestId::Integer(15),
            "Test.Quick",
            json!({}),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    drop(server);
    Ok(())
}

#[test]
fn event_overflow_requires_snapshot_resynchronization() -> TestResult {
    let generation = ServiceGeneration::try_new(9)?;
    let client = client_id(5)?;
    let changes = ApplicationChanges::default();
    let hub = EventHub::try_new(
        generation,
        EventHubLimits::try_new(2, 256)?,
        changes.clone(),
    )?;
    let initial = EventCursor::try_new(client, generation, 0, Timestamp::from_unix_nanos(1_000))?;
    hub.publish(json!({"sequence": 1}))?;
    hub.publish(json!({"sequence": 2}))?;
    hub.publish(json!({"sequence": 3}))?;

    assert_eq!(
        hub.read_after(
            client,
            Some(&initial),
            EventPageLimit::try_new(4)?,
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
        ),
        Err(EventReadError::SequenceGap {
            oldest_available: 2
        })
    );
    // Background producers coalesce without consuming one journal slot per observation.
    let last_command =
        EventCursor::try_new(client, generation, 3, Timestamp::from_unix_nanos(1_000))?;
    for _ in 0..1_000 {
        changes.record(market_squawk_services::ServiceDomain::Market);
    }
    changes.record(market_squawk_services::ServiceDomain::Macro);
    let page = hub.read_after(
        client,
        Some(&last_command),
        EventPageLimit::try_new(4)?,
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(1_000),
    )?;
    assert_eq!(page.events().len(), 1);
    assert_eq!(page.events()[0].sequence(), 4);
    assert_eq!(
        page.events()[0].payload(),
        &json!({
            "type": "application.domains_changed", "domains": ["market", "macro"]
        })
    );
    // Another subscriber sees the same journaled event; draining the signal does not consume it.
    assert_eq!(
        hub.read_after(
            client,
            Some(&last_command),
            EventPageLimit::try_new(4)?,
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
        )?,
        page
    );
    assert!(
        hub.read_after(
            client,
            Some(page.cursor()),
            EventPageLimit::try_new(4)?,
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
        )?
        .events()
        .is_empty()
    );

    // Failed append must not consume a committed change.
    let undersized =
        EventHub::try_new(generation, EventHubLimits::try_new(2, 1)?, changes.clone())?;
    changes.record(market_squawk_services::ServiceDomain::Research);
    assert_eq!(
        undersized.read_after(
            client,
            None,
            EventPageLimit::try_new(4)?,
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(1_000),
        ),
        Err(EventReadError::InvalidEvent)
    );
    let recovered = hub.read_after(
        client,
        Some(page.cursor()),
        EventPageLimit::try_new(4)?,
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(1_000),
    )?;
    assert_eq!(recovered.events()[0].sequence(), 5);
    assert_eq!(
        recovered.events()[0].payload()["domains"],
        json!(["research"])
    );
    Ok(())
}

#[tokio::test]
async fn staged_input_claim_is_owner_bound_and_one_shot() -> TestResult {
    let directory = TempDir::new()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let runtime = runtime_identity(1, 2, 3)?;
    let stager = InputStager::new(
        paths.artifacts()?.clone(),
        runtime,
        InputStagingLimits::try_new(2, 1_024)?,
    );
    let bytes = b"exact staged input";
    let media_type = SourceIdentifier::try_from("market-squawk.training-config.v1")?;
    let admission = InputAdmission::try_new(
        media_type.clone(),
        u64::try_from(bytes.len())?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, sha2::Sha256::digest(bytes).into()),
    )?;
    let owner = client_id(5)?;
    let mut stage = stager.begin(
        owner,
        admission,
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(100),
    )?;
    stage.write_chunk(bytes).await?;
    let ticket = stage.finish(Timestamp::from_unix_nanos(200)).await?;

    assert!(matches!(
        stager.claim(
            ticket.id(),
            client_id(6)?,
            &media_type,
            Timestamp::from_unix_nanos(300),
        ),
        Err(InputStagingError::TicketRejected)
    ));
    let claimed = stager.claim(
        ticket.id(),
        owner,
        &media_type,
        Timestamp::from_unix_nanos(300),
    )?;
    assert_eq!(claimed.read_verified(1_024)?.as_ref(), bytes);
    assert!(matches!(
        stager.claim(
            ticket.id(),
            owner,
            &media_type,
            Timestamp::from_unix_nanos(300),
        ),
        Err(InputStagingError::TicketRejected)
    ));
    Ok(())
}

#[test]
fn rendezvous_is_secret_free_authenticated_and_bound_to_process_start() -> TestResult {
    let directory = TempDir::new()?;
    let key = SecretValue::new("0123456789abcdef0123456789abcdef".to_owned())?;
    let record = RendezvousRecord::try_new(
        runtime_identity(1, 2, 3)?,
        "127.0.0.1:48621".parse()?,
        ApplicationProtocolRange::single(ApplicationProtocolVersion::V1),
        ProcessIdentity::try_new(700, 900)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let authority = RendezvousAuthority::try_open(directory.path(), key)?;
    authority.publish(&record)?;

    let encoded = authority.encoded_current()?.ok_or("missing rendezvous")?;
    let text = std::str::from_utf8(&encoded)?;
    assert!(!text.contains("credential"));
    assert!(!text.contains("0123456789abcdef0123456789abcdef"));
    assert_eq!(
        authority.load(&FixedProcessVerifier::new(record.process_identity()))?,
        Some(record.clone())
    );

    let mut tampered = encoded;
    let marker = b"\"tag\":\"";
    let index = tampered
        .windows(marker.len())
        .position(|window| window == marker)
        .and_then(|offset| offset.checked_add(marker.len()))
        .ok_or("missing rendezvous tag")?;
    tampered[index] = if tampered[index] == b'0' { b'1' } else { b'0' };
    assert_eq!(
        authority.verify_encoded(
            &tampered,
            &FixedProcessVerifier::new(record.process_identity())
        ),
        Err(RendezvousError::AuthenticationFailed)
    );
    assert_eq!(
        authority.load(&FixedProcessVerifier::new(ProcessIdentity::try_new(
            700, 901
        )?)),
        Err(RendezvousError::StaleProcess)
    );
    assert!(!authority.remove_if_current(runtime_identity(1, 2, 4)?)?);
    assert!(authority.remove_if_current(record.runtime())?);
    assert_eq!(authority.encoded_current()?, None);
    assert_eq!(
        authority.load(&FixedProcessVerifier::new(record.process_identity()))?,
        None
    );
    assert!(!authority.remove_if_current(record.runtime())?);
    Ok(())
}

fn runtime_identity(
    installation: u128,
    workspace: u128,
    generation: u64,
) -> Result<RuntimeIdentity, RuntimeContractError> {
    RuntimeIdentity::try_new(
        InstallationId::try_from_uuid(Uuid::from_u128(installation))?,
        WorkspaceId::try_from_uuid(Uuid::from_u128(workspace))?,
        ServiceGeneration::try_new(generation)?,
    )
}

fn request_for(
    identity: RuntimeIdentity,
    structure: JsonStructureLimits,
) -> Result<AppRequestEnvelope, RuntimeContractError> {
    AppRequestEnvelope::try_new(
        RequestId::Integer(42),
        identity.installation_id(),
        identity.workspace_id(),
        identity.service_generation(),
        client_id(5)?,
        CredentialGeneration::try_new(1).map_err(|_| RuntimeContractError::InvalidPayload)?,
        CorrelationId::try_from_uuid(Uuid::from_u128(6))?,
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(100),
        SourceIdentifier::try_from("Market.Snapshot")
            .map_err(|_| RuntimeContractError::InvalidPayload)?,
        json!({"secret": "native-credential-redaction-sentinel"}),
        structure,
        1_024,
    )
}

fn client_id(value: u128) -> Result<ClientId, RuntimeContractError> {
    ClientId::try_from_uuid(Uuid::from_u128(value))
}

const fn digest(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}

#[derive(Debug)]
struct FixedProcessVerifier {
    expected: ProcessIdentity,
}

impl FixedProcessVerifier {
    const fn new(expected: ProcessIdentity) -> Self {
        Self { expected }
    }
}

impl ProcessIdentityVerifier for FixedProcessVerifier {
    fn is_current(&self, identity: ProcessIdentity) -> Result<bool, RendezvousError> {
        Ok(identity == self.expected)
    }
}
