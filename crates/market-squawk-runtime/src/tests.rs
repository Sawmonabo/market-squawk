use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::SecretValue;
use market_squawk_services::{JsonStructureLimits, RequestId};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn request_identity_mismatches_fail_closed_before_dispatch() -> TestResult {
    let expected = runtime_identity(1, 2, 3)?;
    let structure = JsonStructureLimits::try_new(8, 1_024, 64, 64)?;
    let request = request_for(runtime_identity(4, 2, 3)?, structure)?;
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

#[test]
fn event_overflow_requires_snapshot_resynchronization() -> TestResult {
    let generation = ServiceGeneration::try_new(9)?;
    let client = client_id(5)?;
    let hub = EventHub::try_new(generation, EventHubLimits::try_new(2, 64)?)?;
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
        json!({}),
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
