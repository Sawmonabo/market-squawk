//! Recovery of the exact original dataset publication, never a latest-generation inference.

use super::*;
use market_squawk_data::{
    DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DatasetSplitCounts, DatasetStudyPolicy,
    FeatureDatasetProductContract, Sha256Digest,
};
use market_squawk_jobs::JobSnapshot;
use tokio_util::sync::CancellationToken;

pub(super) fn input_identity(
    dataset: &DatasetId,
    contract: Option<FeatureDatasetProductContract>,
) -> Result<SourceIdentifier, ResearchJobRunnerError> {
    // DatasetId excludes ':'; the complete coordinate fits SourceIdentifier's existing 512-byte bound.
    identifier(format!(
        "phase-one-build-v1:{}:{}",
        contract.map_or("unprepared", FeatureDatasetProductContract::identity),
        dataset.as_str()
    ))
}

pub(super) struct DatasetResultView<'a> {
    pub(super) contract: Option<FeatureDatasetProductContract>,
    pub(super) manifest: &'a DatasetManifestRef,
    pub(super) build_spec: DatasetBuildSpecDigest,
    pub(super) policy: Sha256Digest,
    pub(super) universe: Sha256Digest,
    pub(super) export: Sha256Digest,
    pub(super) study: Option<DatasetStudyPolicy>,
    pub(super) splits: DatasetSplitCounts,
}

pub(super) fn encode_result(
    snapshot: &JobSnapshot,
    view: DatasetResultView<'_>,
) -> Result<Vec<u8>, JobRunError> {
    let manifest = view.manifest;
    serde_json::to_vec(&serde_json::json!({
        "jobBinding":{"jobId":snapshot.id().as_uuid(),"generation":snapshot.generation().get(),
            "inputSha256":encode_hex(snapshot.spec().input().digest().bytes())},
        "publicationStage":"phase_one_derived_generation",
        "productAdmission":if view.contract.is_some() {"admitted_by_product_recipe_at_completion"} else {"not_admitted_by_phase_one_operation_at_completion"},
        "productContract":view.contract.map(FeatureDatasetProductContract::identity),
        "selectionAsOfUnixNanos":view.study.map(|study|study.snapshot_as_of().unix_nanos()),
        "manifest":{"dataset":manifest.dataset_id().as_str(),"version":manifest.manifest_version(),
            "schema":manifest.schema().name(),"schemaVersion":manifest.schema_version().get(),
            "schemaFingerprintSha256":encode_hex(manifest.schema().fingerprint()),"contentSha256":encode_hex(manifest.content_hash().bytes())},
        "buildSpecSha256":encode_hex(view.build_spec.digest().bytes()),"policySha256":encode_hex(view.policy.bytes()),
        "universeSha256":encode_hex(view.universe.bytes()),"phaseOneDescriptorSha256":encode_hex(view.export.bytes()),
        "splitExamples":{"train":view.splits.train_examples(),"validation":view.splits.validation_examples(),"test":view.splits.test_examples()},
    })).map_err(|_|failed("phase-one-derived-generation-result-invalid",false))
}

pub(super) async fn published_result(
    runner: &PhaseOneDerivedGenerationJobRunner,
    snapshot: &JobSnapshot,
) -> Result<Option<JobResultReference>, JobRunError> {
    let spec = snapshot.spec();
    if spec.kind() != &runner.kind
        || spec.input().authority() != &runner.input_authority
        || spec.authority().authority() != &runner.result_authority
        || spec.authority().digest() != runner.authority_digest
    {
        return Err(JobRunError::Recovery);
    }
    let rest = spec
        .input()
        .identity()
        .as_str()
        .strip_prefix("phase-one-build-v1:")
        .ok_or(JobRunError::Recovery)?;
    let (contract, dataset) = rest.rsplit_once(':').ok_or(JobRunError::Recovery)?;
    if contract == "unprepared" {
        return Ok(None);
    }
    let contract =
        FeatureDatasetProductContract::from_identity(contract).ok_or(JobRunError::Recovery)?;
    let dataset = DatasetId::try_from(dataset).map_err(|_| JobRunError::Recovery)?;
    if input_identity(&dataset, Some(contract)).map_err(|_| JobRunError::Recovery)?
        != *spec.input().identity()
    {
        return Err(JobRunError::Recovery);
    }
    let build_spec = DatasetBuildSpecDigest::try_new(spec.input().digest().bytes())
        .map_err(|_| JobRunError::Recovery)?;
    let deadline = std::time::Instant::now()
        .checked_add(runner.run_timeout.min(Duration::from_secs(30)))
        .ok_or(JobRunError::Recovery)?;
    let cancellation = CancellationToken::new();
    // The existing catalog transaction resolves the unique complete build identity and verifies
    // its actual production receipt. No source/output authority is reconstructed after restart.
    let Some(dataset) = runner
        .research
        .analytical_reader()
        .feature_dataset_for_build(contract, &dataset, build_spec, deadline, &cancellation)
        .map_err(|_| JobRunError::Recovery)?
    else {
        return Ok(None);
    };
    let manifest = dataset.generation().manifest();
    let bytes = encode_result(
        snapshot,
        DatasetResultView {
            contract: Some(contract),
            manifest,
            build_spec,
            policy: dataset.policy_digest(),
            universe: dataset.universe_digest(),
            export: dataset.python_export_sha256(),
            study: dataset.study_policy().copied(),
            splits: dataset.split_counts(),
        },
    )?;
    let artifact = runner
        .artifacts
        .publish(
            ArtifactPublication::try_json(bytes).map_err(map_artifact_error)?,
            ArtifactPublicationContext::new(cancellation, deadline),
        )
        .await
        .map_err(map_artifact_error)?;
    JobResultReference::try_new(
        runner.result_authority.clone(),
        identifier(format!(
            "phase-one-derived-generation-{}",
            encode_hex(manifest.content_hash().bytes())
        ))
        .map_err(|_| JobRunError::Recovery)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, manifest.content_hash().bytes()),
        vec![artifact],
    )
    .map(Some)
    .map_err(|_| JobRunError::Recovery)
}
