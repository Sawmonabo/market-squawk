use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::{
    AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, RevisionBoundPayloadEvidence, SchemaVersion, SourceId, SourceIdentifier,
    Timestamp,
};

use super::{
    BoundedVec, PersistedProviderBudgetPolicy, PersistedSourceAuthority, RegistryAuthorityState,
};
use crate::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, EndpointPolicy,
    ProviderBudgetPolicy,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn revision(value: &str) -> TestResult<MetadataRevision> {
    Ok(MetadataRevision::new(SourceIdentifier::try_from(value)?))
}

fn revision_evidence(value: &str, byte: u8) -> TestResult<RevisionBoundPayloadEvidence> {
    Ok(RevisionBoundPayloadEvidence::new(
        revision(value)?,
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [byte; 32],
        )),
    ))
}

fn source(index: u8) -> TestResult<PersistedSourceAuthority> {
    let mut revisions = Vec::new();
    revisions.try_reserve(6)?;
    for revision_index in 1_u8..=6 {
        revisions.push(revision(&format!(
            "revision-{index:02}-{revision_index:02}"
        ))?);
    }
    Ok(PersistedSourceAuthority {
        source_id: SourceId::try_from(format!("source-{index:02}"))?,
        used_revisions: BoundedVec::try_new(revisions)?,
        latest_revision_evidence: None,
        revoked: false,
        last_epoch: u64::from(index),
        generation_high_water: None,
    })
}

fn policy(index: u8) -> TestResult<PersistedProviderBudgetPolicy> {
    let provider = SourceIdentifier::try_from(format!("provider-{index:02}"))?;
    let policy = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider),
        NonZeroU32::new(10).ok_or("request limit must be nonzero")?,
        NonZeroU64::new(60_000_000_000).ok_or("window must be nonzero")?,
        NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("backoff must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("backoff cap must be nonzero")?,
            0,
        )?,
    )?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(SourceIdentifier::try_from("public-terms")?),
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [index; 32],
        )),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
    );
    Ok(PersistedProviderBudgetPolicy::try_new(
        policy,
        EndpointPolicy::try_new([format!("https://provider-{index:02}.example.test")])?,
        authorization,
        None,
    )?)
}

fn raw_state(
    sources: Vec<PersistedSourceAuthority>,
    budget_policies: Vec<PersistedProviderBudgetPolicy>,
) -> TestResult<RegistryAuthorityState> {
    Ok(RegistryAuthorityState {
        schema_version: SchemaVersion::CURRENT,
        sources: BoundedVec::try_new(sources)?,
        budget_policies: BoundedVec::try_new(budget_policies)?,
    })
}

fn deterministic_shuffle<T>(values: &mut [T], seed: &mut u64) -> TestResult {
    for index in (1..values.len()).rev() {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let bound = u64::try_from(index.checked_add(1).ok_or("shuffle bound overflow")?)?;
        let selected = usize::try_from(*seed % bound)?;
        values.swap(index, selected);
    }
    Ok(())
}

#[test]
fn canonicalization_preserves_semantic_revision_order_and_rejects_a_false_latest_revision()
-> TestResult {
    let state = RegistryAuthorityState::try_new(
        vec![PersistedSourceAuthority {
            source_id: SourceId::try_from("source-a")?,
            used_revisions: BoundedVec::try_new(vec![
                revision("revision-z")?,
                revision("revision-a")?,
            ])?,
            latest_revision_evidence: Some(revision_evidence("revision-a", 7)?),
            revoked: false,
            last_epoch: 2,
            generation_high_water: None,
        }],
        Vec::new(),
    )?;
    let reversed_wire = serde_json::to_vec(&state)?;
    let mut decoded: RegistryAuthorityState = serde_json::from_slice(&reversed_wire)?;

    decoded.canonicalize()?;

    let canonical_wire = serde_json::to_vec(&decoded)?;
    assert_eq!(canonical_wire, reversed_wire);
    let revisions = decoded
        .sources
        .as_slice()
        .first()
        .ok_or("canonical source missing")?
        .used_revisions
        .as_slice()
        .iter()
        .map(|value| value.as_source_identifier().as_str())
        .collect::<Vec<_>>();
    assert_eq!(revisions, ["revision-z", "revision-a"]);
    assert!(matches!(
        RegistryAuthorityState::try_new(
            vec![PersistedSourceAuthority {
                source_id: SourceId::try_from("source-a")?,
                used_revisions: BoundedVec::try_new(vec![
                    revision("revision-z")?,
                    revision("revision-a")?,
                ])?,
                latest_revision_evidence: Some(revision_evidence("revision-z", 8)?),
                revoked: false,
                last_epoch: 2,
                generation_high_water: None,
            }],
            Vec::new(),
        ),
        Err(crate::RegistryError::InvalidAuthorityState)
    ));
    Ok(())
}

#[test]
fn canonicalization_rejects_duplicate_nested_revisions() -> TestResult {
    let duplicate = revision("revision-a")?;
    let mut state = raw_state(
        vec![PersistedSourceAuthority {
            source_id: SourceId::try_from("source-a")?,
            used_revisions: BoundedVec::try_new(vec![duplicate.clone(), duplicate])?,
            latest_revision_evidence: None,
            revoked: false,
            last_epoch: 2,
            generation_high_water: None,
        }],
        Vec::new(),
    )?;

    assert!(matches!(
        state.canonicalize(),
        Err(crate::policy::AuthorityPersistenceError::InvalidState)
    ));
    Ok(())
}

#[test]
fn canonicalization_rejects_duplicate_top_level_sources() -> TestResult {
    let source = source(1)?;
    let mut state = raw_state(vec![source.clone(), source], Vec::new())?;

    assert!(matches!(
        state.canonicalize(),
        Err(crate::policy::AuthorityPersistenceError::InvalidState)
    ));
    Ok(())
}

#[test]
fn canonicalization_rejects_duplicate_budget_policies() -> TestResult {
    let policy = policy(1)?;
    let mut state = raw_state(Vec::new(), vec![policy.clone(), policy])?;

    assert!(matches!(
        state.canonicalize(),
        Err(crate::policy::AuthorityPersistenceError::InvalidState)
    ));
    Ok(())
}

#[test]
fn canonicalization_is_identical_across_one_hundred_twenty_registry_permutations() -> TestResult {
    let mut sources = Vec::new();
    let mut policies = Vec::new();
    sources.try_reserve(6)?;
    policies.try_reserve(6)?;
    for index in 1_u8..=6 {
        sources.push(source(index)?);
        policies.push(policy(index)?);
    }

    let mut expected = RegistryAuthorityState::try_new(sources.clone(), policies.clone())?;
    expected.canonicalize()?;
    let expected_wire = serde_json::to_vec(&expected)?;

    let mut seed = 0x6a09_e667_f3bc_c909_u64;
    for _ in 0..120 {
        let mut permuted_sources = sources.clone();
        deterministic_shuffle(&mut permuted_sources, &mut seed)?;
        let mut permuted_policies = policies.clone();
        deterministic_shuffle(&mut permuted_policies, &mut seed)?;

        let mut candidate = RegistryAuthorityState::try_new(permuted_sources, permuted_policies)?;
        candidate.canonicalize()?;
        assert_eq!(serde_json::to_vec(&candidate)?, expected_wire);
    }
    Ok(())
}
