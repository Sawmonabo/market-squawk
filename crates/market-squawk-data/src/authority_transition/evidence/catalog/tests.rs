use super::{EvidenceError, GenerationEvidenceHeader, GenerationObjectEvidenceRow};
use crate::authority_transition::evidence::GenerationPlanEvidence;
use crate::{
    DatasetId, DatasetSchemaRegistry, GenerationKind, ManifestObject, ManifestPlan, Sha256Digest,
};
use uuid::Uuid;

#[test]
fn streamed_generation_replay_matches_manifest_plan_and_rejects_changed_lineage()
-> Result<(), Box<dyn std::error::Error>> {
    let dataset = DatasetId::try_from("prices.daily")?;
    let objects = vec![
        ManifestObject::try_new(
            Sha256Digest::new([3; 32]),
            7,
            512,
            Sha256Digest::new([5; 32]),
        )?,
        ManifestObject::try_new(
            Sha256Digest::new([7; 32]),
            11,
            768,
            Sha256Digest::new([9; 32]),
        )?,
    ];
    for object_count in 1..=objects.len() {
        let selected = &objects[..object_count];
        let plan = ManifestPlan::append(dataset.clone(), None, selected.to_vec(), 8)?;
        let mut header = GenerationEvidenceHeader {
            generation_sequence: 1,
            dataset_id: dataset.clone(),
            manifest_version: 1,
            content_hash: plan.content_hash(),
            lineage_hash: plan.lineage_digest(),
            row_count: plan.row_count(),
            total_bytes: plan.total_bytes(),
            schema: DatasetSchemaRegistry::local().canonical_research_observations()?,
            anchor_manifest_id: Uuid::new_v4(),
            kind: GenerationKind::Ingest,
            build_spec_digest: None,
        };
        for corrupt in [false, true] {
            if corrupt {
                header.lineage_hash = Sha256Digest::new([99; 32]);
            }
            let mut replay = GenerationPlanEvidence::new(&header)?;
            for object in selected {
                replay.object(&GenerationObjectEvidenceRow::try_new(
                    Uuid::new_v4(),
                    object.content_hash(),
                    object.row_count(),
                    object.size_bytes(),
                    object.lineage_digest(),
                )?)?;
            }
            let result = replay.finish(&header, object_count as u64);
            if corrupt {
                assert!(matches!(
                    result,
                    Err(EvidenceError::GenerationSemanticMismatch)
                ));
            } else {
                result?;
            }
        }
    }
    Ok(())
}
