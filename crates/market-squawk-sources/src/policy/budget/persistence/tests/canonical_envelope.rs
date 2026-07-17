use market_squawk_domain::{
    ConnectionGeneration, MetadataRevision, SchemaVersion, SourceId, SourceIdentifier, Timestamp,
};
use serde::Serialize;

use super::*;

const PERMUTATION_COUNT: usize = 128;
const SOURCE_COUNT: u8 = 4;
const REVISIONS_PER_SOURCE: u8 = 4;
const POLICY_COUNT: u8 = 6;

#[derive(Clone, Debug, Serialize)]
struct SourceWire {
    source_id: SourceId,
    used_revisions: Vec<MetadataRevision>,
    last_epoch: u64,
    generation_high_water: Option<ConnectionGeneration>,
}

#[derive(Clone, Debug, Serialize)]
struct RegistryWire {
    schema_version: SchemaVersion,
    sources: Vec<SourceWire>,
    budget_policies: Vec<PersistedProviderBudgetPolicy>,
}

#[derive(Serialize)]
struct EnvelopeWire<'a> {
    format_version: u16,
    run_generation: u64,
    run_state: DurableRunState,
    saved_at_wall: Timestamp,
    wall_high_water: Timestamp,
    registry: &'a RegistryWire,
    budgets: &'a [DurableBudgetGroup],
}

#[derive(Clone, Debug)]
struct GroupFixture {
    declarations: Vec<PersistedProviderBudgetPolicy>,
    checkpoint: BudgetCheckpointState,
}

#[derive(Clone, Debug)]
struct RichFixture {
    sources: Vec<SourceWire>,
    policies: Vec<PersistedProviderBudgetPolicy>,
    groups: Vec<GroupFixture>,
}

fn revision(source_index: u8, revision_index: u8) -> TestResult<MetadataRevision> {
    Ok(MetadataRevision::new(SourceIdentifier::try_from(format!(
        "revision-{source_index:02}-{revision_index:02}"
    ))?))
}

fn source_wire(source_index: u8, revision_indices: &[u8]) -> TestResult<SourceWire> {
    let mut revisions = Vec::new();
    revisions.try_reserve(revision_indices.len())?;
    for revision_index in revision_indices {
        revisions.push(revision(source_index, *revision_index)?);
    }
    Ok(SourceWire {
        source_id: SourceId::try_from(format!("source-{source_index:02}"))?,
        used_revisions: revisions,
        last_epoch: u64::from(source_index),
        generation_high_water: None,
    })
}

fn registry_wire(
    sources: Vec<SourceWire>,
    budget_policies: Vec<PersistedProviderBudgetPolicy>,
) -> RegistryWire {
    RegistryWire {
        schema_version: SchemaVersion::CURRENT,
        sources,
        budget_policies,
    }
}

fn registry_with_single_source(revision_indices: &[u8]) -> TestResult<RegistryWire> {
    let mut sources = Vec::new();
    sources.try_reserve(1)?;
    sources.push(source_wire(1, revision_indices)?);
    Ok(registry_wire(sources, Vec::new()))
}

fn registry_state(wire: &RegistryWire) -> TestResult<crate::RegistryAuthorityState> {
    Ok(serde_json::from_slice(&serde_json::to_vec(wire)?)?)
}

fn raw_envelope_bytes(registry: &RegistryWire) -> TestResult<Vec<u8>> {
    Ok(serde_json::to_vec(&EnvelopeWire {
        format_version: DURABLE_AUTHORITY_FORMAT_VERSION,
        run_generation: 1,
        run_state: DurableRunState::InUse,
        saved_at_wall: Timestamp::from_unix_nanos(100),
        wall_high_water: Timestamp::from_unix_nanos(100),
        registry,
        budgets: &[],
    })?)
}

fn rich_fixture() -> TestResult<RichFixture> {
    let mut sources = Vec::new();
    sources.try_reserve(usize::from(SOURCE_COUNT))?;
    let mut revision_indices = Vec::new();
    revision_indices.try_reserve(usize::from(REVISIONS_PER_SOURCE))?;
    for revision_index in 1..=REVISIONS_PER_SOURCE {
        revision_indices.push(revision_index);
    }
    for source_index in 1..=SOURCE_COUNT {
        sources.push(source_wire(source_index, &revision_indices)?);
    }

    let mut policies = Vec::new();
    policies.try_reserve(usize::from(POLICY_COUNT))?;
    for policy_index in 1..=POLICY_COUNT {
        policies.push(declaration(policy_index)?);
    }

    let declaration_pairs = [(0_usize, 1_usize), (2, 3), (4, 5)];
    let mut groups = Vec::new();
    groups.try_reserve(declaration_pairs.len())?;
    for (group_index, (first, second)) in declaration_pairs.into_iter().enumerate() {
        let mut declarations = Vec::new();
        declarations.try_reserve(2)?;
        declarations.push(
            policies
                .get(first)
                .ok_or("first group declaration missing")?
                .clone(),
        );
        declarations.push(
            policies
                .get(second)
                .ok_or("second group declaration missing")?
                .clone(),
        );
        let checkpoint_index = u8::try_from(
            group_index
                .checked_add(1)
                .ok_or("checkpoint index overflow")?,
        )?;
        groups.push(GroupFixture {
            declarations,
            checkpoint: checkpoint(checkpoint_index),
        });
    }

    Ok(RichFixture {
        sources,
        policies,
        groups,
    })
}

fn durable_group(fixture: GroupFixture) -> TestResult<DurableBudgetGroup> {
    let mut declarations = fixture.declarations.into_iter();
    let first = declarations
        .next()
        .ok_or("durable group requires a declaration")?;
    let mut group = DurableBudgetGroup::try_new(first, fixture.checkpoint)?;
    for declaration in declarations {
        group.add_declaration(declaration)?;
    }
    if group.declarations().len() < 2 {
        return Err("durable group requires at least two declarations".into());
    }
    Ok(group)
}

fn envelope(fixture: RichFixture) -> TestResult<DurableAuthorityEnvelope> {
    let registry = registry_state(&registry_wire(fixture.sources, fixture.policies))?;
    let mut groups = Vec::new();
    groups.try_reserve(fixture.groups.len())?;
    for group in fixture.groups {
        groups.push(durable_group(group)?);
    }
    Ok(DurableAuthorityEnvelope {
        format_version: DURABLE_AUTHORITY_FORMAT_VERSION,
        run_generation: 7,
        run_state: DurableRunState::InUse,
        saved_at_wall: Timestamp::from_unix_nanos(100),
        wall_high_water: Timestamp::from_unix_nanos(100),
        registry,
        budgets: BoundedVec::try_new(groups)?,
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
fn reversed_nested_revisions_are_rejected_at_the_authenticated_envelope_boundary() -> TestResult {
    let canonical = registry_with_single_source(&[1, 2, 3])?;
    let canonical_payload = raw_envelope_bytes(&canonical)?;
    assert!(deserialize_canonical_envelope(&canonical_payload).is_ok());

    let reversed = registry_with_single_source(&[3, 2, 1])?;
    let reversed_payload = raw_envelope_bytes(&reversed)?;
    assert!(matches!(
        deserialize_canonical_envelope(&reversed_payload),
        Err(AuthorityPersistenceError::InvalidState)
    ));
    Ok(())
}

#[test]
fn duplicate_nested_revisions_are_rejected_at_the_authenticated_envelope_boundary() -> TestResult {
    let canonical = registry_with_single_source(&[1, 2, 3])?;
    let canonical_payload = raw_envelope_bytes(&canonical)?;
    assert!(deserialize_canonical_envelope(&canonical_payload).is_ok());

    let duplicate = registry_with_single_source(&[1, 1, 3])?;
    let duplicate_payload = raw_envelope_bytes(&duplicate)?;
    assert!(matches!(
        deserialize_canonical_envelope(&duplicate_payload),
        Err(AuthorityPersistenceError::InvalidState)
    ));
    Ok(())
}

#[test]
fn canonical_envelope_roundtrip_preserves_identical_authenticated_bytes() -> TestResult {
    let payload = serialize_canonical_envelope(&envelope(rich_fixture()?)?)?;
    let decoded = deserialize_canonical_envelope(&payload)?;
    assert_eq!(serialize_canonical_envelope(&decoded)?, payload);
    Ok(())
}

#[test]
fn canonical_envelope_is_identical_across_all_nested_permutations() -> TestResult {
    let fixture = rich_fixture()?;
    const { assert!(PERMUTATION_COUNT >= 100) };
    assert!(fixture.sources.len() >= 2);
    assert!(
        fixture
            .sources
            .iter()
            .all(|source| source.used_revisions.len() >= 3)
    );
    assert!(fixture.policies.len() >= 2);
    assert!(fixture.groups.len() >= 2);
    assert!(
        fixture
            .groups
            .iter()
            .all(|group| group.declarations.len() >= 2)
    );
    let expected = serialize_canonical_envelope(&envelope(fixture.clone())?)?;
    let mut seed = 0x6a09_e667_f3bc_c909_u64;

    for _ in 0..PERMUTATION_COUNT {
        let mut candidate = fixture.clone();
        deterministic_shuffle(&mut candidate.sources, &mut seed)?;
        for source in &mut candidate.sources {
            deterministic_shuffle(&mut source.used_revisions, &mut seed)?;
        }
        deterministic_shuffle(&mut candidate.policies, &mut seed)?;
        deterministic_shuffle(&mut candidate.groups, &mut seed)?;
        for group in &mut candidate.groups {
            deterministic_shuffle(&mut group.declarations, &mut seed)?;
        }

        let payload = serialize_canonical_envelope(&envelope(candidate)?)?;
        assert_eq!(payload, expected);
        let decoded = deserialize_canonical_envelope(&payload)?;
        assert_eq!(serialize_canonical_envelope(&decoded)?, payload);
    }
    Ok(())
}
