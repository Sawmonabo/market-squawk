use std::error::Error;
use std::time::Duration;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceId, Timestamp};
use uuid::Uuid;

use super::{
    DerivedPublicationInput, DerivedPublicationObject, DerivedRetentionOperation, ResearchUse,
    ResearchUseAuthorityEvidence, ResearchUseDecisionInput, ResearchUseDecisionOutcome,
    ResearchUseDenialReason, ResearchUseError, ResearchUseGeneration, ResearchUseGraph,
    ResearchUseGraphEdge, ResearchUseLimits, ResearchUseSet, ResearchUseSourceInput, issue_permit,
};
use crate::{
    DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DatasetSchemaRegistry, GenerationKind,
    GenerationParentRelation, ManifestObject, ManifestPlan, Sha256Digest,
};

#[test]
fn contracts_canonicalize_bounded_authority_and_seal_permits() -> Result<(), Box<dyn Error>> {
    let uses = ResearchUseSet::try_new(vec![
        ResearchUse::Train,
        ResearchUse::Display,
        ResearchUse::LocalAnalysis,
    ])?;
    assert_eq!(uses.len(), 3);
    assert!(uses.contains(ResearchUse::Display));
    assert!(uses.contains(ResearchUse::LocalAnalysis));
    assert!(uses.contains(ResearchUse::Train));
    assert_eq!(
        ResearchUseSet::try_new(vec![ResearchUse::Display, ResearchUse::Display]),
        Err(ResearchUseError::DuplicateUse)
    );

    let limits = ResearchUseLimits::try_new(
        8,
        32,
        64,
        16,
        1_048_576,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;
    assert_eq!(limits.max_roots(), 8);
    assert_eq!(
        ResearchUseLimits::try_new(
            8,
            32,
            64,
            16,
            1_048_576,
            Duration::from_secs(31),
            Duration::from_secs(30),
        ),
        Err(ResearchUseError::InvalidLimits)
    );
    assert_eq!(
        ResearchUseLimits::try_new(
            8,
            32,
            64,
            16,
            1_048_576,
            Duration::from_secs(2),
            Duration::from_secs(301),
        ),
        Err(ResearchUseError::InvalidLimits)
    );

    let parent_a = manifest("raw.a", 1, 1)?;
    let parent_b = manifest("raw.b", 1, 2)?;
    let output = manifest("derived.features", 1, 3)?;
    let build = DatasetBuildSpecDigest::try_new([4; 32])?;
    let node_a =
        ResearchUseGeneration::try_new(11, parent_a.clone(), GenerationKind::Ingest, None, 0)?;
    let node_b =
        ResearchUseGeneration::try_new(12, parent_b.clone(), GenerationKind::Ingest, None, 0)?;
    let node_output = ResearchUseGeneration::try_new(
        13,
        output.clone(),
        GenerationKind::Derived,
        Some(build),
        2,
    )?;
    let edge_a = ResearchUseGraphEdge::try_new(13, 11, GenerationParentRelation::DerivedInput)?;
    let edge_b = ResearchUseGraphEdge::try_new(13, 12, GenerationParentRelation::DerivedInput)?;
    let source_a = ResearchUseSourceInput::try_new(
        11,
        uuid(1),
        SourceId::try_from("files.user-owned")?,
        [5; 32],
    )?;
    let source_b =
        ResearchUseSourceInput::try_new(12, uuid(2), SourceId::try_from("sec.edgar")?, [6; 32])?;

    let graph = ResearchUseGraph::try_new(
        vec![output.clone()],
        vec![node_output.clone(), node_b.clone(), node_a.clone()],
        vec![edge_b, edge_a],
        vec![source_b.clone(), source_a.clone()],
        limits,
    )?;
    let permuted_graph = ResearchUseGraph::try_new(
        vec![output.clone()],
        vec![node_a, node_output, node_b],
        vec![edge_a, edge_b],
        vec![source_a.clone(), source_b.clone()],
        limits,
    )?;
    assert_eq!(graph.digest(), permuted_graph.digest());
    assert_eq!(
        graph.digest().bytes(),
        [
            0xae, 0x0b, 0x15, 0xab, 0x89, 0x0a, 0xf4, 0x73, 0xad, 0x96, 0x38, 0xf8, 0x97, 0xa8,
            0x24, 0xc7, 0x31, 0xbc, 0xaa, 0x40, 0x0b, 0x10, 0x93, 0xa2, 0xbc, 0xd6, 0xb8, 0x29,
            0x05, 0x61, 0x4b, 0x1f,
        ]
    );
    assert_eq!(
        ResearchUseGraph::try_new(vec![output.clone()], vec![], vec![], vec![], limits,),
        Err(ResearchUseError::InvalidGraph)
    );
    let low_byte_limits = ResearchUseLimits::try_new(
        8,
        32,
        64,
        16,
        1,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;
    assert_eq!(
        ResearchUseGraph::try_new(
            graph.roots().to_vec(),
            graph.nodes().to_vec(),
            graph.edges().to_vec(),
            graph.sources().to_vec(),
            low_byte_limits,
        ),
        Err(ResearchUseError::InvalidGraph)
    );
    let truncated_ingest = manifest("raw.a", 2, 44)?;
    assert_eq!(
        ResearchUseGraph::try_new(
            vec![truncated_ingest.clone()],
            vec![ResearchUseGeneration::try_new(
                14,
                truncated_ingest,
                GenerationKind::Ingest,
                None,
                1,
            )?],
            vec![],
            vec![ResearchUseSourceInput::try_new(
                14,
                uuid(14),
                source_a.source_id().clone(),
                source_a.rights_id(),
            )?],
            limits,
        ),
        Err(ResearchUseError::InvalidGraph)
    );

    assert_eq!(
        ResearchUseAuthorityEvidence::try_new(
            source_a.clone(),
            [99; 32],
            [7; 32],
            EvidenceDigest::new(DigestAlgorithm::Sha256, [5; 32]),
            None,
            [8; 32],
            EvidenceDigest::new(DigestAlgorithm::Sha256, [8; 32]),
            Some(Timestamp::from_unix_nanos(3_000)),
            4,
        ),
        Err(ResearchUseError::InvalidAuthorityEvidence)
    );
    let authority_a = authority(&source_a, 8)?;
    let authority_b = authority(&source_b, 10)?;
    assert_eq!(
        ResearchUseDecisionInput::try_new(
            &graph,
            ResearchUse::LocalAnalysis,
            1,
            Timestamp::from_unix_nanos(1_000),
            Some(Timestamp::from_unix_nanos(2_000)),
            ResearchUseDecisionOutcome::Allowed,
            vec![authority_a.clone()],
        ),
        Err(ResearchUseError::InvalidDecision)
    );
    let foreign_source = ResearchUseSourceInput::try_new(
        99,
        uuid(99),
        SourceId::try_from("foreign.source")?,
        [99; 32],
    )?;
    assert_eq!(
        ResearchUseDecisionInput::try_new(
            &graph,
            ResearchUse::LocalAnalysis,
            1,
            Timestamp::from_unix_nanos(1_000),
            None,
            ResearchUseDecisionOutcome::Denied(ResearchUseDenialReason::MissingGrant),
            vec![authority(&foreign_source, 100)?],
        ),
        Err(ResearchUseError::InvalidDecision)
    );
    let short_lived_limits = ResearchUseLimits::try_new(
        8,
        32,
        64,
        16,
        1_048_576,
        Duration::from_secs(2),
        Duration::from_nanos(500),
    )?;
    let short_lived_graph = ResearchUseGraph::try_new(
        graph.roots().to_vec(),
        graph.nodes().to_vec(),
        graph.edges().to_vec(),
        graph.sources().to_vec(),
        short_lived_limits,
    )?;
    assert_eq!(short_lived_graph.digest(), graph.digest());
    assert_eq!(
        ResearchUseDecisionInput::try_new(
            &short_lived_graph,
            ResearchUse::LocalAnalysis,
            1,
            Timestamp::from_unix_nanos(1_000),
            Some(Timestamp::from_unix_nanos(2_000)),
            ResearchUseDecisionOutcome::Allowed,
            vec![authority_b.clone(), authority_a.clone()],
        ),
        Err(ResearchUseError::InvalidDecision)
    );
    let decision = ResearchUseDecisionInput::try_new(
        &graph,
        ResearchUse::LocalAnalysis,
        1,
        Timestamp::from_unix_nanos(1_000),
        Some(Timestamp::from_unix_nanos(2_000)),
        ResearchUseDecisionOutcome::Allowed,
        vec![authority_b.clone(), authority_a.clone()],
    )?;
    let permuted_decision = ResearchUseDecisionInput::try_new(
        &permuted_graph,
        ResearchUse::LocalAnalysis,
        1,
        Timestamp::from_unix_nanos(1_000),
        Some(Timestamp::from_unix_nanos(2_000)),
        ResearchUseDecisionOutcome::Allowed,
        vec![authority_a.clone(), authority_b.clone()],
    )?;
    assert_eq!(
        ResearchUseDecisionInput::try_new(
            &graph,
            ResearchUse::LocalAnalysis,
            2,
            Timestamp::from_unix_nanos(1_000),
            Some(Timestamp::from_unix_nanos(2_000)),
            ResearchUseDecisionOutcome::Allowed,
            vec![authority_a.clone(), authority_b.clone()],
        ),
        Err(ResearchUseError::InvalidDecision)
    );
    assert_eq!(decision.digest(), permuted_decision.digest());
    assert_eq!(
        decision.digest().bytes(),
        [
            0x06, 0xb3, 0xbe, 0x02, 0x7b, 0x7e, 0x7b, 0x0f, 0xc1, 0x65, 0x1a, 0x53, 0xc9, 0x82,
            0x07, 0x8b, 0x10, 0x81, 0x5d, 0xf5, 0xe5, 0x99, 0x58, 0x8e, 0x37, 0x25, 0x93, 0x3d,
            0x84, 0xc8, 0x2b, 0x36,
        ]
    );
    let display_decision = ResearchUseDecisionInput::try_new(
        &graph,
        ResearchUse::Display,
        1,
        Timestamp::from_unix_nanos(1_000),
        Some(Timestamp::from_unix_nanos(2_000)),
        ResearchUseDecisionOutcome::Allowed,
        vec![authority_b, authority_a],
    )?;

    let object_a = DerivedPublicationObject::try_new(
        uuid(21),
        [22; 32],
        DerivedRetentionOperation::Cache,
        [5; 32],
        uuid(23),
        Sha256Digest::new([24; 32]),
        10,
        1_000,
        Sha256Digest::new([25; 32]),
    )?;
    let object_b = DerivedPublicationObject::try_new(
        uuid(31),
        [32; 32],
        DerivedRetentionOperation::Cache,
        [6; 32],
        uuid(33),
        Sha256Digest::new([34; 32]),
        20,
        2_000,
        Sha256Digest::new([35; 32]),
    )?;
    let plan = ManifestPlan::derive(
        DatasetId::try_from("derived.features")?,
        vec![
            ManifestObject::try_new(
                Sha256Digest::new([34; 32]),
                20,
                2_000,
                Sha256Digest::new([35; 32]),
            )?,
            ManifestObject::try_new(
                Sha256Digest::new([24; 32]),
                10,
                1_000,
                Sha256Digest::new([25; 32]),
            )?,
        ],
        1_024,
    )?;
    let display_permit = issue_permit(uuid(50), &display_decision)?;
    assert!(matches!(
        DerivedPublicationInput::try_new(
            display_permit,
            &graph,
            build,
            output.schema().clone(),
            plan.clone(),
            vec![object_b.clone(), object_a.clone()],
            uuid(23),
        ),
        Err(ResearchUseError::InvalidPublication)
    ));
    let publication_permit = issue_permit(uuid(51), &decision)?;
    let publication = DerivedPublicationInput::try_new(
        publication_permit,
        &graph,
        build,
        output.schema().clone(),
        plan.clone(),
        vec![object_b.clone(), object_a.clone()],
        uuid(23),
    )?;
    let permuted_publication_permit = issue_permit(uuid(52), &permuted_decision)?;
    let permuted_publication = DerivedPublicationInput::try_new(
        permuted_publication_permit,
        &permuted_graph,
        build,
        output.schema().clone(),
        plan,
        vec![object_a, object_b],
        uuid(23),
    )?;
    assert_eq!(publication.digest(), permuted_publication.digest());
    assert_eq!(
        publication.digest().bytes(),
        [
            0xda, 0x1d, 0xac, 0xff, 0x39, 0x59, 0x67, 0x7c, 0x43, 0xf3, 0xfa, 0xb3, 0x52, 0x4b,
            0x22, 0xfc, 0x76, 0x94, 0xd2, 0xe5, 0xa3, 0x63, 0x87, 0x3b, 0x48, 0x3e, 0x50, 0x21,
            0x19, 0x08, 0xba, 0x03,
        ]
    );

    let permit = issue_permit(uuid(53), &decision)?;
    assert_eq!(permit.decision_digest(), decision.digest());
    assert_eq!(permit.graph_digest(), graph.digest());
    assert_eq!(permit.research_use(), ResearchUse::LocalAnalysis);
    assert_eq!(permit.expires_at(), Timestamp::from_unix_nanos(2_000));
    assert_eq!(
        format!("{permit:?}"),
        "ResearchUsePermit([SEALED AUTHORITY])"
    );
    Ok(())
}

fn manifest(
    dataset: &str,
    manifest_version: u64,
    digest_byte: u8,
) -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(dataset)?,
        manifest_version,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([digest_byte; 32]),
    )?)
}

fn authority(
    source: &ResearchUseSourceInput,
    grant_byte: u8,
) -> Result<ResearchUseAuthorityEvidence, ResearchUseError> {
    ResearchUseAuthorityEvidence::try_new(
        source.clone(),
        source.rights_id(),
        [grant_byte.wrapping_add(1); 32],
        EvidenceDigest::new(DigestAlgorithm::Sha256, source.rights_id()),
        None,
        [grant_byte; 32],
        EvidenceDigest::new(DigestAlgorithm::Sha256, [grant_byte; 32]),
        Some(Timestamp::from_unix_nanos(3_000)),
        4,
    )
}

fn uuid(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}
