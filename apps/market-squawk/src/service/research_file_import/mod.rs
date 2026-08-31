//! Durable guided import of user-selected research files into the installed research pipeline.

mod state;

use std::{
    collections::BTreeSet,
    fmt,
    io::{Read as _, Write as _},
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

use chrono::DateTime;
use market_squawk_adapter_files::{
    ExtractionLimits, ExtractionLimitsInput, FilePreview, FilePreviewCell, FilePreviewColumnKind,
    FilePreviewFormat, FilePreviewLimits, GuidedInstrumentBinding, GuidedManifestInput,
    GuidedManifestObject, GuidedObjectTime, GuidedRecordTimeFallback, GuidedRowTimeMapping,
    GuidedUniverseBinding, GuidedValueMapping, build_guided_manifest_collection, preview_bytes,
};
use market_squawk_domain::{InstrumentId, ResearchTemporalCoordinate, SourceIdentifier, Timestamp};
use market_squawk_platform::{ArtifactRoot, LocalAuthorityStateStore, LocalPaths};
use market_squawk_runtime::{ClientId, InputStager, InputTicketId, RuntimeIdentity};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

use crate::{
    application::{Application, ResearchSourceDiscovery, ResearchSourceDiscoveryCoordinator},
    local_product::{ControlledLocalFileRequest, ProviderResearchActivationService},
};

use self::state::{
    ActivationRecord, ImportAuthority, ImportEntry, ImportPhase, MAXIMUM_IMPORTS,
    ResearchFileMapping, StoredFileFormat, StoredJobReceipt, StoredJobStart,
};

const PREVIEW_FILE: &str = "Research.PreviewStagedFile";
const COMMIT_FILE: &str = "Research.CommitStagedFile";
const DISCARD_FILE: &str = "Research.DiscardStagedFile";
const START_INGEST: &str = "Research.StartIngestSource";
const LOCAL_FILES_PROFILE: &str = "local.files";
const IMPORT_ROOT_REFERENCE: &str = "research-imports-v1";
const AUTHORITY_DIRECTORY: &str = "research/file-imports-v1";
const INPUT_MEDIA_TYPE: &str = "market-squawk.research-source-file.v1";
pub(super) const MAXIMUM_RESEARCH_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAXIMUM_RECOVERED_OBJECT_FILES: usize = MAXIMUM_IMPORTS * 2;

/// Installed, path-free service boundary for guided research-file imports.
#[derive(Clone)]
pub(super) struct InstalledResearchFileImportOperations {
    application: Arc<Application>,
    discovery: Arc<crate::application::ProductionResearchIngestCoordinator>,
    activation: Arc<ProviderResearchActivationService>,
    artifacts: ArtifactRoot,
    inputs: Arc<InputStager>,
    runtime: RuntimeIdentity,
    authority: Arc<Mutex<ImportAuthority>>,
    artifact_gate: Arc<Mutex<()>>,
    promotion_gate: Arc<AsyncMutex<()>>,
}

impl InstalledResearchFileImportOperations {
    pub(super) fn try_new(
        paths: &LocalPaths,
        application: Arc<Application>,
        discovery: Arc<crate::application::ProductionResearchIngestCoordinator>,
        activation: Arc<ProviderResearchActivationService>,
        inputs: Arc<InputStager>,
        runtime: RuntimeIdentity,
    ) -> Result<Self, ServiceError> {
        let control = paths
            .control_root()
            .map_err(|_error| ServiceError::Unavailable)?;
        let store = LocalAuthorityStateStore::try_open(control.root().join(AUTHORITY_DIRECTORY))
            .map_err(|_error| ServiceError::Unavailable)?;
        let artifacts = paths
            .artifacts()
            .map_err(|_error| ServiceError::Unavailable)?
            .clone();
        Ok(Self {
            application,
            discovery,
            activation,
            artifacts,
            inputs,
            runtime,
            authority: Arc::new(Mutex::new(ImportAuthority::open(store)?)),
            artifact_gate: Arc::new(Mutex::new(())),
            promotion_gate: Arc::new(AsyncMutex::new(())),
        })
    }

    pub(super) fn owns_direct(operation: &str) -> bool {
        matches!(operation, PREVIEW_FILE | DISCARD_FILE)
    }

    pub(super) fn owns_commit(operation: &str) -> bool {
        operation == COMMIT_FILE
    }

    pub(super) async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.call_blocking(&request, &context))
            .await
            .map_err(|_error| ServiceError::Internal)?
    }

    fn call_blocking(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.authorize(context)?;
        ensure_live(context)?;
        let _artifact_guard = self
            .artifact_gate
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        self.reap_expired_pending(current_timestamp()?.unix_nanos())?;
        self.reconcile_unreferenced_objects()?;
        let data = match request.name() {
            PREVIEW_FILE => self.preview(request, context)?,
            DISCARD_FILE => self.discard(request, context)?,
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            data,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    fn preview(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: PreviewRequest = decode(request.arguments())?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let client = ClientId::try_from_uuid(origin.client_id())
            .map_err(|_error| ServiceError::Unauthorized)?;
        let ticket_id = InputTicketId::try_from_uuid(input.input_ticket_id)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let media_type = SourceIdentifier::try_from(INPUT_MEDIA_TYPE)
            .map_err(|_error| ServiceError::Unavailable)?;
        let admitted_at = current_timestamp()?;
        let claimed = self
            .inputs
            .claim(ticket_id, client, &media_type, admitted_at)
            .map_err(|_error| ServiceError::Unauthorized)?;
        let bytes = claimed
            .read_verified(MAXIMUM_RESEARCH_FILE_BYTES)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let format = StoredFileFormat::from_request(input.format);
        let source_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        if source_sha256 != claimed.ticket().digest().bytes() {
            return Err(ServiceError::InvalidRequest);
        }
        let preview = preview_bytes(
            format.adapter_format(),
            &bytes,
            extraction_limits()?,
            FilePreviewLimits::standard(),
            wall_deadline(context)?,
            context.cancellation(),
        )
        .map_err(map_adapter_error)?;
        let source_sha256 = encode_hex(source_sha256);
        let object_reference = format!("objects/{source_sha256}.{}", format.extension());
        persist_immutable(
            &self.artifacts,
            &artifact_reference(&object_reference),
            &bytes,
        )?;
        let preview_id = preview_digest(
            claimed.ticket().id().as_uuid(),
            origin.workspace_id(),
            origin.client_id(),
            &source_sha256,
            format,
        );
        let entry = ImportEntry {
            preview_id: preview_id.clone(),
            ticket_id: claimed.ticket().id().as_uuid().hyphenated().to_string(),
            workspace_id: origin.workspace_id().hyphenated().to_string(),
            client_id: origin.client_id().hyphenated().to_string(),
            source_sha256: source_sha256.clone(),
            source_bytes: u64::try_from(bytes.len()).map_err(|_error| ServiceError::Internal)?,
            object_reference,
            format,
            admitted_at_unix_nanos: admitted_at.unix_nanos(),
            phase: ImportPhase::Pending,
            mapping: None,
            activation: None,
            job_start: None,
            job_receipt: None,
        };
        let replaced = match self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .append_pending(entry.clone(), current_timestamp()?.unix_nanos())
        {
            Ok(replaced) => replaced,
            Err(error) => {
                self.remove_unreferenced_object(&entry)?;
                return Err(error);
            }
        };
        self.remove_unreferenced_objects(&replaced)?;
        preview_value(&preview_id, &source_sha256, &preview)
    }

    fn discard(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: PreviewIdentityRequest = decode(request.arguments())?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let removed = {
            let mut authority = self
                .authority
                .lock()
                .map_err(|_error| ServiceError::Unavailable)?;
            let entry = authority
                .entry(&input.preview_id)
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            ensure_entry_owner(&entry, origin.workspace_id(), origin.client_id())?;
            if entry.phase != ImportPhase::Pending {
                return Err(ServiceError::InvalidRequest);
            }
            authority.remove(&input.preview_id)?
        };
        self.remove_unreferenced_object(&removed)?;
        Ok(json!({"previewId": input.preview_id, "status": "discarded"}))
    }

    pub(super) async fn prepare_commit(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedResearchFileCommit, ServiceError> {
        self.authorize(context)?;
        ensure_live(context)?;
        let input: CommitRequest = decode(request.arguments())?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let gate = Arc::clone(&self.promotion_gate).lock_owned().await;
        let artifact_guard = self
            .artifact_gate
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        self.reap_expired_pending(current_timestamp()?.unix_nanos())?;
        self.reconcile_unreferenced_objects()?;
        let entry = {
            let authority = self
                .authority
                .lock()
                .map_err(|_error| ServiceError::Unavailable)?;
            authority
                .entry(&input.preview_id)
                .cloned()
                .ok_or(ServiceError::NotFound)?
        };
        ensure_entry_owner(&entry, origin.workspace_id(), origin.client_id())?;
        if entry.phase == ImportPhase::Committed {
            let data = entry.job_receipt.clone().ok_or(ServiceError::Internal)?;
            return Ok(PreparedResearchFileCommit::Existing(job_result(
                data, &entry, context,
            )?));
        }
        if entry.phase == ImportPhase::Pending {
            let existing = {
                let authority = self
                    .authority
                    .lock()
                    .map_err(|_error| ServiceError::Unavailable)?;
                authority
                    .entries()
                    .iter()
                    .find(|candidate| {
                        candidate.phase == ImportPhase::Committed
                            && candidate.same_owner(&entry)
                            && candidate.source_sha256 == entry.source_sha256
                            && candidate.format == entry.format
                    })
                    .cloned()
            };
            if let Some(existing) = existing {
                if existing.mapping.as_ref() != Some(&input.mapping) {
                    return Err(ServiceError::InvalidRequest);
                }
                let data = existing.job_receipt.clone().ok_or(ServiceError::Internal)?;
                self.authority
                    .lock()
                    .map_err(|_error| ServiceError::Unavailable)?
                    .remove(&entry.preview_id)?;
                return Ok(PreparedResearchFileCommit::Existing(job_result(
                    data, &existing, context,
                )?));
            }
        }
        let entry = if entry.phase == ImportPhase::Pending {
            self.prepare_promotion(entry, input.mapping, context)?
        } else {
            if entry.mapping.as_ref() != Some(&input.mapping) {
                return Err(ServiceError::InvalidRequest);
            }
            entry
        };
        drop(artifact_guard);
        self.activate(&entry, context).await?;
        let (request, discovery) = self.discover_start_request(&entry, context).await?;
        let job_start = entry.job_start.clone().ok_or(ServiceError::Internal)?;
        let queued_result = job_result(
            StoredJobReceipt::queued(job_start.job_id())?,
            &entry,
            context,
        )?;
        Ok(PreparedResearchFileCommit::Ready(Box::new(PreparedStart {
            preview_id: entry.preview_id,
            workspace_id: entry.workspace_id,
            client_id: entry.client_id,
            job_start,
            queued_result,
            request,
            discovery: Some(discovery),
            revoke_discovery_on_drop: true,
            coordinator: Arc::clone(&self.discovery),
            _gate: gate,
        })))
    }

    pub(super) async fn prepare_recovery(
        &self,
        preview_id: &str,
        context: &RequestContext,
    ) -> Result<PreparedStart, ServiceError> {
        self.authorize(context)?;
        let gate = Arc::clone(&self.promotion_gate).lock_owned().await;
        let entry = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .entry(preview_id)
            .filter(|entry| entry.phase == ImportPhase::Promoting)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        self.activate(&entry, context).await?;
        let (request, discovery) = self.discover_start_request(&entry, context).await?;
        let job_start = entry.job_start.clone().ok_or(ServiceError::Internal)?;
        let queued_result = job_result(
            StoredJobReceipt::queued(job_start.job_id())?,
            &entry,
            context,
        )?;
        Ok(PreparedStart {
            preview_id: entry.preview_id,
            workspace_id: entry.workspace_id,
            client_id: entry.client_id,
            job_start,
            queued_result,
            request,
            discovery: Some(discovery),
            revoke_discovery_on_drop: true,
            coordinator: Arc::clone(&self.discovery),
            _gate: gate,
        })
    }

    pub(super) fn recovery_ids(&self) -> Result<Vec<String>, ServiceError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        Ok(authority
            .entries()
            .iter()
            .filter(|entry| entry.phase == ImportPhase::Promoting)
            .map(|entry| entry.preview_id.clone())
            .collect())
    }

    pub(super) fn committed_jobs(&self) -> Result<Vec<(String, StoredJobReceipt)>, ServiceError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        authority
            .entries()
            .iter()
            .filter(|entry| entry.phase == ImportPhase::Committed)
            .map(|entry| {
                Ok((
                    entry.preview_id.clone(),
                    entry.job_receipt.clone().ok_or(ServiceError::Internal)?,
                ))
            })
            .collect()
    }

    pub(super) fn reopen_committed_job(
        &self,
        preview_id: &str,
        expected_job_id: &str,
    ) -> Result<bool, ServiceError> {
        let mut authority = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        let mut entry = authority
            .entry(preview_id)
            .filter(|entry| {
                entry.phase == ImportPhase::Committed
                    && entry
                        .job_receipt
                        .as_ref()
                        .is_some_and(|receipt| receipt.job_id() == expected_job_id)
            })
            .cloned()
            .ok_or(ServiceError::InvalidRequest)?;
        let admitted_at_unix_nanos = entry
            .activation
            .as_ref()
            .ok_or(ServiceError::Internal)?
            .admitted_at_unix_nanos;
        let Some(next) = entry
            .job_start
            .as_ref()
            .ok_or(ServiceError::Internal)?
            .next(&entry, admitted_at_unix_nanos)?
        else {
            return Ok(false);
        };
        entry.phase = ImportPhase::Promoting;
        entry.job_start = Some(next);
        entry.job_receipt = None;
        authority.replace(entry)?;
        Ok(true)
    }

    pub(super) fn discard_pending_after_restart(&self) -> Result<(), ServiceError> {
        let _artifact_guard = self
            .artifact_gate
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        let removed = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .remove_all_pending()?;
        self.remove_unreferenced_objects(&removed)?;
        self.reconcile_unreferenced_objects()
    }

    pub(super) fn complete_commit(
        &self,
        preview_id: &str,
        result: &TypedToolResult,
    ) -> Result<(), ServiceError> {
        let mut authority = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        let mut entry = authority
            .entry(preview_id)
            .filter(|entry| entry.phase == ImportPhase::Promoting)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        entry.phase = ImportPhase::Committed;
        let receipt = StoredJobReceipt::decode(result.structured_content().clone())?;
        if entry
            .job_start
            .as_ref()
            .is_none_or(|start| start.job_id() != receipt.job_id())
        {
            return Err(ServiceError::InvalidResult);
        }
        entry.job_receipt = Some(receipt);
        authority.replace(entry)
    }

    fn prepare_promotion(
        &self,
        mut entry: ImportEntry,
        mapping: ResearchFileMapping,
        context: &RequestContext,
    ) -> Result<ImportEntry, ServiceError> {
        let committed = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .entries()
            .iter()
            .filter(|candidate| {
                candidate.phase == ImportPhase::Committed && candidate.same_owner(&entry)
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut aggregate = committed;
        entry.mapping = Some(mapping);
        aggregate.push(entry.clone());
        aggregate.sort_by(|left, right| left.object_reference.cmp(&right.object_reference));
        let source_bytes = aggregate
            .iter()
            .map(|candidate| self.read_import_object(candidate))
            .collect::<Result<Vec<_>, _>>()?;
        let guided_inputs = aggregate
            .iter()
            .zip(&source_bytes)
            .map(|(candidate, bytes)| {
                Ok(GuidedManifestObject {
                    input: guided_input(candidate)?,
                    source_bytes: bytes,
                })
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        let manifest = build_guided_manifest_collection(
            guided_inputs,
            extraction_limits()?,
            wall_deadline(context)?,
            context.cancellation(),
        )
        .map_err(map_adapter_error)?;
        let manifest_sha256 = encode_hex(manifest.digest().bytes());
        let manifest_reference = format!("manifests/{manifest_sha256}.json");
        persist_immutable(
            &self.artifacts,
            &artifact_reference(&manifest_reference),
            manifest.as_bytes(),
        )?;
        let activation = activation_record(
            &aggregate,
            &manifest_sha256,
            &manifest_reference,
            self.runtime,
            current_timestamp()?,
        )?;
        entry.phase = ImportPhase::Promoting;
        entry.activation = Some(activation);
        entry.job_start = Some(StoredJobStart::first(
            &entry,
            entry
                .activation
                .as_ref()
                .ok_or(ServiceError::Internal)?
                .admitted_at_unix_nanos,
        )?);
        if let Err(error) = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .replace(entry.clone())
        {
            self.remove_unreferenced_manifest(&manifest_reference)?;
            return Err(error);
        }
        Ok(entry)
    }

    async fn activate(
        &self,
        entry: &ImportEntry,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        let activation = entry.activation.as_ref().ok_or(ServiceError::Internal)?;
        self.activation
            .activate_controlled_local_files(
                ControlledLocalFileRequest {
                    root_reference: PathBuf::from(&activation.root_reference),
                    manifest_reference: PathBuf::from(&activation.manifest_reference),
                    manifest_sha256: activation.manifest_sha256.clone(),
                    admitted_input_set_sha256: activation.admitted_input_set_sha256.clone(),
                    local_admission_evidence_sha256: activation
                        .local_admission_evidence_sha256
                        .clone(),
                    workspace_receipt_evidence_sha256: activation
                        .workspace_receipt_evidence_sha256
                        .clone(),
                    import_receipt_evidence_sha256: activation
                        .import_receipt_evidence_sha256
                        .clone(),
                    admitted_at_unix_nanos: activation.admitted_at_unix_nanos,
                },
                context.cancellation().child_token(),
            )
            .await
            .map_err(|_error| ServiceError::Unavailable)
    }

    async fn discover_start_request(
        &self,
        entry: &ImportEntry,
        context: &RequestContext,
    ) -> Result<(TypedToolRequest, ResearchSourceDiscovery), ServiceError> {
        let mapping = entry.mapping.as_ref().ok_or(ServiceError::Internal)?;
        let profile = identifier(LOCAL_FILES_PROFILE)?;
        let dataset = identifier(&mapping.dataset)?;
        let maximum_objects = u16::try_from(MAXIMUM_IMPORTS)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(ServiceError::Internal)?;
        let discovery = self
            .discovery
            .discover_registered_objects(&profile, &dataset, None, maximum_objects, context)
            .await?;
        if discovery.profile() != &profile
            || discovery.request().dataset() != &dataset
            || discovery.request().effective_at().is_some()
            || discovery.request().max_results() != maximum_objects.get()
            || discovery.objects().len() > MAXIMUM_IMPORTS
            || discovery.receipts_survive_restart()
            || !discovery.rights().persistence_operation_admitted()
            || discovery.objects().iter().any(|object| {
                object.source_object().dataset() != &dataset
                    || object.source_object().source_id() != discovery.metadata().source_id()
                    || object.source_object().metadata_revision() != discovery.metadata().revision()
                    || object.discovery_receipt().is_empty()
            })
        {
            return Err(ServiceError::InvalidResult);
        }
        let expected_object = object_identifier(&entry.source_sha256)?;
        let mut matching = discovery.objects().iter().filter(|object| {
            object.source_object().dataset().as_str() == mapping.dataset
                && object.source_object().object_id() == &expected_object
        });
        let object = matching.next().ok_or(ServiceError::InvalidResult)?;
        if matching.next().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        let mut start = Map::new();
        start.insert("provider".to_owned(), json!(LOCAL_FILES_PROFILE));
        start.insert(
            "object".to_owned(),
            json!(object.source_object().object_id()),
        );
        start.insert("dataset".to_owned(), json!(mapping.dataset));
        start.insert(
            "discoveryReceipt".to_owned(),
            json!(object.discovery_receipt()),
        );
        start.insert("confirm".to_owned(), Value::Bool(true));
        start.insert("sourceCoverage".to_owned(), json!([LOCAL_FILES_PROFILE]));
        start.insert("resultLimits".to_owned(), result_limits(context));
        let request = self.application.admit(START_INGEST, start)?;
        Ok((request, discovery))
    }

    fn read_import_object(&self, entry: &ImportEntry) -> Result<Vec<u8>, ServiceError> {
        let bytes = read_immutable(
            &self.artifacts,
            &artifact_reference(&entry.object_reference),
            MAXIMUM_RESEARCH_FILE_BYTES,
        )?;
        if u64::try_from(bytes.len()).ok() != Some(entry.source_bytes)
            || encode_hex(Sha256::digest(&bytes).into()) != entry.source_sha256
        {
            return Err(ServiceError::Internal);
        }
        Ok(bytes)
    }

    fn reap_expired_pending(&self, now_unix_nanos: i64) -> Result<(), ServiceError> {
        let removed = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .remove_expired_pending(now_unix_nanos)?;
        self.remove_unreferenced_objects(&removed)
    }

    fn remove_unreferenced_objects(&self, removed: &[ImportEntry]) -> Result<(), ServiceError> {
        for entry in removed {
            self.remove_unreferenced_object(entry)?;
        }
        Ok(())
    }

    fn remove_unreferenced_object(&self, removed: &ImportEntry) -> Result<(), ServiceError> {
        let retained = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .entries()
            .iter()
            .any(|entry| entry.object_reference == removed.object_reference);
        if retained {
            return Ok(());
        }
        let relative = artifact_reference(&removed.object_reference);
        let directory = self
            .artifacts
            .try_clone_directory()
            .map_err(|_error| ServiceError::Unavailable)?;
        match directory.remove_file(relative) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_error) => Err(ServiceError::Unavailable),
        }
    }

    fn reconcile_unreferenced_objects(&self) -> Result<(), ServiceError> {
        let retained = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .entries()
            .iter()
            .map(|entry| entry.object_reference.clone())
            .collect::<BTreeSet<_>>();
        let root = self
            .artifacts
            .try_clone_directory()
            .map_err(|_error| ServiceError::Unavailable)?;
        let object_directory = match root.open_dir(artifact_reference("objects")) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_error) => return Err(ServiceError::Unavailable),
        };
        let mut inspected = 0_usize;
        for candidate in object_directory
            .entries()
            .map_err(|_error| ServiceError::Unavailable)?
        {
            inspected = inspected
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            if inspected > MAXIMUM_RECOVERED_OBJECT_FILES {
                return Err(ServiceError::ResourceExhausted);
            }
            let candidate = candidate.map_err(|_error| ServiceError::Unavailable)?;
            if !candidate
                .file_type()
                .map_err(|_error| ServiceError::Unavailable)?
                .is_file()
            {
                return Err(ServiceError::Internal);
            }
            let name = candidate
                .file_name()
                .into_string()
                .map_err(|_error| ServiceError::Internal)?;
            let reference = format!("objects/{name}");
            if !valid_object_reference(&reference) {
                return Err(ServiceError::Internal);
            }
            if !retained.contains(&reference) {
                root.remove_file(artifact_reference(&reference))
                    .map_err(|_error| ServiceError::Unavailable)?;
            }
        }
        Ok(())
    }

    fn remove_unreferenced_manifest(&self, manifest_reference: &str) -> Result<(), ServiceError> {
        let retained = self
            .authority
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .entries()
            .iter()
            .filter_map(|entry| entry.activation.as_ref())
            .any(|activation| activation.manifest_reference == manifest_reference);
        if retained {
            return Ok(());
        }
        let relative = artifact_reference(manifest_reference);
        let directory = self
            .artifacts
            .try_clone_directory()
            .map_err(|_error| ServiceError::Unavailable)?;
        match directory.remove_file(relative) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_error) => Err(ServiceError::Unavailable),
        }
    }

    fn authorize(&self, context: &RequestContext) -> Result<(), ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        if origin.workspace_id() != self.runtime.workspace_id().as_uuid() {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }
}

impl fmt::Debug for InstalledResearchFileImportOperations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledResearchFileImportOperations")
            .field("application", &"[APPLICATION AUTHORITY]")
            .field("discovery", &"[RESEARCH DISCOVERY AUTHORITY]")
            .field("activation", &"[CONTROLLED FILE ACTIVATION]")
            .field("artifacts", &self.artifacts)
            .field("inputs", &"[ONE-SHOT INPUT STAGER]")
            .field("runtime", &self.runtime)
            .field("authority", &"[DURABLE IMPORT AUTHORITY]")
            .field("artifact_gate", &"[SERIALIZED IMPORT ARTIFACT AUTHORITY]")
            .finish()
    }
}

pub(super) enum PreparedResearchFileCommit {
    Existing(TypedToolResult),
    Ready(Box<PreparedStart>),
}

pub(super) struct PreparedStart {
    preview_id: String,
    workspace_id: String,
    client_id: String,
    job_start: StoredJobStart,
    queued_result: TypedToolResult,
    request: TypedToolRequest,
    discovery: Option<ResearchSourceDiscovery>,
    revoke_discovery_on_drop: bool,
    coordinator: Arc<crate::application::ProductionResearchIngestCoordinator>,
    _gate: OwnedMutexGuard<()>,
}

impl PreparedStart {
    pub(super) const fn request(&self) -> &TypedToolRequest {
        &self.request
    }

    pub(super) fn preview_id(&self) -> &str {
        &self.preview_id
    }

    pub(super) fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    pub(super) fn client_id(&self) -> &str {
        &self.client_id
    }

    pub(super) const fn job_start(&self) -> &StoredJobStart {
        &self.job_start
    }

    pub(super) fn queued_result(&self) -> TypedToolResult {
        self.queued_result.clone()
    }

    pub(super) const fn mark_job_admission_may_exist(&mut self) {
        self.revoke_discovery_on_drop = false;
    }

    pub(super) const fn mark_job_not_admitted(&mut self) {
        self.revoke_discovery_on_drop = true;
    }
}

impl Drop for PreparedStart {
    fn drop(&mut self) {
        if self.revoke_discovery_on_drop
            && let Some(discovery) = self.discovery.as_ref()
        {
            let _rollback = self.coordinator.revoke_discovery_receipts(discovery);
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PreviewRequest {
    input_ticket_id: Uuid,
    format: RequestedFileFormat,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RequestedFileFormat {
    Csv,
    Json,
    Ndjson,
    Parquet,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CommitRequest {
    preview_id: String,
    mapping: ResearchFileMapping,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PreviewIdentityRequest {
    preview_id: String,
}

impl StoredFileFormat {
    const fn from_request(format: RequestedFileFormat) -> Self {
        match format {
            RequestedFileFormat::Csv => Self::Csv,
            RequestedFileFormat::Json => Self::Json,
            RequestedFileFormat::Ndjson => Self::Ndjson,
            RequestedFileFormat::Parquet => Self::Parquet,
        }
    }

    const fn adapter_format(self) -> FilePreviewFormat {
        match self {
            Self::Csv => FilePreviewFormat::Csv { delimiter: b',' },
            Self::Json => FilePreviewFormat::Json,
            Self::Ndjson => FilePreviewFormat::Ndjson,
            Self::Parquet => FilePreviewFormat::Parquet,
        }
    }
}

fn guided_input(entry: &ImportEntry) -> Result<GuidedManifestInput, ServiceError> {
    let mapping = entry.mapping.as_ref().ok_or(ServiceError::Internal)?;
    let effective_at = parse_timestamp(&mapping.effective_at)?;
    let published_at = mapping
        .published_at
        .as_deref()
        .map(parse_timestamp)
        .transpose()?;
    let instrument_binding =
        mapping
            .instrument_id
            .map_or(Ok(GuidedInstrumentBinding::Unscoped), |value| {
                InstrumentId::try_from(value)
                    .map(
                        |instrument_id| GuidedInstrumentBinding::InternalInstrument {
                            instrument_id,
                        },
                    )
                    .map_err(|_error| ServiceError::InvalidRequest)
            })?;
    let universe_binding = match (&mapping.universe, mapping.instrument_id) {
        (None, _) => GuidedUniverseBinding::None,
        (Some(universe), Some(_)) => GuidedUniverseBinding::Membership {
            universe: identifier(universe)?,
            starts_at: effective_at,
            ends_at: None,
        },
        (Some(_), None) => return Err(ServiceError::InvalidRequest),
    };
    let value_mappings = mapping
        .fields
        .iter()
        .map(|field| {
            Ok(GuidedValueMapping {
                source_field: field.source.clone(),
                output_field: identifier(&field.field)?,
                decimal_scale: field.decimal_scale,
                unit: field.unit.as_deref().map(identifier).transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let row_time = GuidedRowTimeMapping {
        effective_field: mapping.effective_field.clone(),
        published_field: mapping.published_field.clone(),
        available_field: mapping.available_field.clone(),
        revision_field: mapping.revision_field.clone(),
        revision_number_field: mapping.revision_number_field.clone(),
        superseded_field: mapping.superseded_field.clone(),
    };
    let has_row_time = row_time.effective_field.is_some()
        || row_time.published_field.is_some()
        || row_time.available_field.is_some()
        || row_time.revision_field.is_some()
        || row_time.revision_number_field.is_some()
        || row_time.superseded_field.is_some();
    let row_time_mapping = has_row_time.then_some(row_time);
    Ok(GuidedManifestInput {
        dataset: identifier(&mapping.dataset)?,
        object_id: object_identifier(&entry.source_sha256)?,
        object_path: entry.object_reference.clone(),
        format: entry.format.adapter_format(),
        object_time: GuidedObjectTime {
            effective_at,
            published_at,
            superseded_at: None,
        },
        revision: identifier(format!("sha256-{}", entry.source_sha256))?,
        revision_number: 1,
        record_time: GuidedRecordTimeFallback {
            effective: ResearchTemporalCoordinate::exact(effective_at),
            published: published_at.map(ResearchTemporalCoordinate::exact),
            superseded: None,
        },
        instrument_binding,
        universe_binding,
        identity_field: mapping.identity_field.clone(),
        value_mappings,
        row_time_mapping,
    })
}

fn activation_record(
    aggregate: &[ImportEntry],
    manifest_sha256: &str,
    manifest_reference: &str,
    runtime: RuntimeIdentity,
    admitted_at: Timestamp,
) -> Result<ActivationRecord, ServiceError> {
    let owner = aggregate.first().ok_or(ServiceError::Internal)?;
    if aggregate.iter().any(|entry| !entry.same_owner(owner)) {
        return Err(ServiceError::Internal);
    }
    let workspace_id =
        Uuid::parse_str(&owner.workspace_id).map_err(|_error| ServiceError::Internal)?;
    let client_id = Uuid::parse_str(&owner.client_id).map_err(|_error| ServiceError::Internal)?;
    if runtime.workspace_id().as_uuid() != workspace_id {
        return Err(ServiceError::Unauthorized);
    }
    let admitted_input_set_sha256 = aggregate_digest(
        b"market-squawk/research-import/raw-input-set/v1\0",
        aggregate.iter().map(|entry| entry.source_sha256.as_bytes()),
    );
    let mut local_admission = Sha256::new();
    local_admission.update(b"market-squawk/research-import/local-admission/v1\0");
    for entry in aggregate {
        update_framed(&mut local_admission, entry.preview_id.as_bytes());
        update_framed(&mut local_admission, entry.source_sha256.as_bytes());
        update_framed(
            &mut local_admission,
            &entry.admitted_at_unix_nanos.to_be_bytes(),
        );
    }
    let local_admission_evidence_sha256 = encode_hex(local_admission.finalize().into());
    let mut workspace_receipt = Sha256::new();
    workspace_receipt.update(b"market-squawk/research-import/workspace-client/v1\0");
    update_framed(&mut workspace_receipt, workspace_id.as_bytes());
    update_framed(&mut workspace_receipt, client_id.as_bytes());
    update_framed(
        &mut workspace_receipt,
        &runtime.service_generation().get().to_be_bytes(),
    );
    let workspace_receipt_evidence_sha256 = encode_hex(workspace_receipt.finalize().into());
    let mut mapping_digests = Vec::new();
    for entry in aggregate {
        let mapping = entry.mapping.as_ref().ok_or(ServiceError::Internal)?;
        let bytes = serde_json::to_vec(mapping).map_err(|_error| ServiceError::Internal)?;
        mapping_digests.push(Sha256::digest(bytes).to_vec());
    }
    let import_receipt_evidence_sha256 = aggregate_digest(
        b"market-squawk/research-import/receipt/v1\0",
        mapping_digests.iter().map(Vec::as_slice),
    );
    Ok(ActivationRecord {
        root_reference: IMPORT_ROOT_REFERENCE.to_owned(),
        manifest_reference: manifest_reference.to_owned(),
        manifest_sha256: manifest_sha256.to_owned(),
        admitted_input_set_sha256,
        local_admission_evidence_sha256,
        workspace_receipt_evidence_sha256,
        import_receipt_evidence_sha256,
        admitted_at_unix_nanos: admitted_at.unix_nanos(),
    })
}

fn preview_value(
    preview_id: &str,
    source_sha256: &str,
    preview: &FilePreview,
) -> Result<Value, ServiceError> {
    let columns = preview
        .columns()
        .iter()
        .map(|column| {
            json!({
                "name": column.name(),
                "kind": match column.kind() {
                    FilePreviewColumnKind::ExactDecimal => "exact_decimal",
                    FilePreviewColumnKind::Text => "text",
                    FilePreviewColumnKind::Mixed => "mixed",
                    FilePreviewColumnKind::Unsupported => "unsupported",
                    FilePreviewColumnKind::Null => "null",
                },
                "nullable": column.nullable(),
            })
        })
        .collect::<Vec<_>>();
    let sample_rows = preview
        .sample_rows()
        .iter()
        .map(|row| {
            row.cells()
                .iter()
                .map(|cell| match cell {
                    FilePreviewCell::Text { value, truncated } => json!({
                        "kind": "text",
                        "value": value,
                        "truncated": truncated,
                    }),
                    FilePreviewCell::Null => {
                        json!({"kind": "null", "value": null, "truncated": false})
                    }
                    FilePreviewCell::Unsupported => json!({
                        "kind": "unsupported",
                        "value": null,
                        "truncated": false,
                    }),
                    FilePreviewCell::Missing => {
                        json!({"kind": "missing", "value": null, "truncated": false})
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "previewId": preview_id,
        "sha256": source_sha256,
        "format": match preview.format() {
            FilePreviewFormat::Csv { .. } => "csv",
            FilePreviewFormat::Json => "json",
            FilePreviewFormat::Ndjson => "ndjson",
            FilePreviewFormat::Parquet => "parquet",
        },
        "rowCount": preview.row_count(),
        "columns": columns,
        "sampleRows": sample_rows,
    }))
}

fn extraction_limits() -> Result<ExtractionLimits, ServiceError> {
    let mut input = ExtractionLimitsInput::standard();
    input.max_source_bytes = MAXIMUM_RESEARCH_FILE_BYTES;
    ExtractionLimits::try_new(input).map_err(map_adapter_error)
}

fn wall_deadline(context: &RequestContext) -> Result<Timestamp, ServiceError> {
    let remaining = context
        .deadline()
        .checked_duration_since(Instant::now())
        .ok_or(ServiceError::DeadlineExceeded)?;
    let nanos = i64::try_from(remaining.as_nanos()).map_err(|_error| ServiceError::Internal)?;
    current_timestamp()?
        .checked_add_nanos(nanos)
        .map_err(|_error| ServiceError::Internal)
}

fn persist_immutable(
    root: &ArtifactRoot,
    relative: &Path,
    bytes: &[u8],
) -> Result<(), ServiceError> {
    if let Ok(existing) = read_immutable(
        root,
        relative,
        MAXIMUM_RESEARCH_FILE_BYTES.max(MAXIMUM_MANIFEST_BYTES),
    ) {
        return if existing == bytes {
            Ok(())
        } else {
            Err(ServiceError::Internal)
        };
    }
    let resolved = root
        .resolve(relative)
        .map_err(|_error| ServiceError::Unavailable)?;
    let mut file = match resolved.create_new() {
        Ok(file) => file,
        Err(_error) => {
            let existing = read_immutable(
                root,
                relative,
                MAXIMUM_RESEARCH_FILE_BYTES.max(MAXIMUM_MANIFEST_BYTES),
            )?;
            return if existing == bytes {
                Ok(())
            } else {
                Err(ServiceError::Internal)
            };
        }
    };
    if file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .is_err()
    {
        if let Ok(directory) = root.try_clone_directory() {
            let _remove = directory.remove_file(relative);
        }
        return Err(ServiceError::Unavailable);
    }
    let verified = read_immutable(
        root,
        relative,
        MAXIMUM_RESEARCH_FILE_BYTES.max(MAXIMUM_MANIFEST_BYTES),
    )?;
    if verified == bytes {
        Ok(())
    } else {
        Err(ServiceError::Internal)
    }
}

fn read_immutable(
    root: &ArtifactRoot,
    relative: &Path,
    maximum_bytes: u64,
) -> Result<Vec<u8>, ServiceError> {
    let file = root
        .resolve(relative)
        .map_err(|_error| ServiceError::Unavailable)?
        .open_read()
        .map_err(|_error| ServiceError::Unavailable)?;
    let length = file
        .metadata()
        .map_err(|_error| ServiceError::Unavailable)?
        .len();
    if length == 0 || length > maximum_bytes {
        return Err(ServiceError::Internal);
    }
    let capacity = usize::try_from(length).map_err(|_error| ServiceError::ResourceExhausted)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_error| ServiceError::Unavailable)?;
    if bytes.len() != capacity {
        return Err(ServiceError::Internal);
    }
    Ok(bytes)
}

fn preview_digest(
    ticket_id: Uuid,
    workspace_id: Uuid,
    client_id: Uuid,
    source_sha256: &str,
    format: StoredFileFormat,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/research-file-preview/v1\0");
    digest.update(ticket_id.as_bytes());
    digest.update(workspace_id.as_bytes());
    digest.update(client_id.as_bytes());
    digest.update(source_sha256.as_bytes());
    digest.update(format.name().as_bytes());
    encode_hex(digest.finalize().into())
}

fn aggregate_digest<'a>(domain: &[u8], values: impl IntoIterator<Item = &'a [u8]>) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    for value in values {
        digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(value);
    }
    encode_hex(digest.finalize().into())
}

fn update_framed(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn parse_timestamp(value: &str) -> Result<Timestamp, ServiceError> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_error| ServiceError::InvalidRequest)?
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidRequest)
}

fn object_identifier(source_sha256: &str) -> Result<SourceIdentifier, ServiceError> {
    identifier(format!("object-{source_sha256}"))
}

fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier, ServiceError> {
    SourceIdentifier::try_from(value.as_ref()).map_err(|_error| ServiceError::InvalidRequest)
}

fn artifact_reference(relative: &str) -> PathBuf {
    Path::new(IMPORT_ROOT_REFERENCE).join(relative)
}

fn valid_object_reference(reference: &str) -> bool {
    let Some(name) = reference.strip_prefix("objects/") else {
        return false;
    };
    let Some((digest, extension)) = name.rsplit_once('.') else {
        return false;
    };
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        && matches!(extension, "csv" | "json" | "ndjson" | "parquet")
}

fn result_limits(context: &RequestContext) -> Value {
    let maximum_bytes = context
        .limits()
        .maximum_result_bytes()
        .min(usize::try_from(MAXIMUM_MANIFEST_BYTES).unwrap_or(usize::MAX));
    json!({
        "maximumItems": 1,
        "maximumBytes": maximum_bytes,
    })
}

fn job_result(
    data: StoredJobReceipt,
    entry: &ImportEntry,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let mapping = entry.mapping.as_ref().ok_or(ServiceError::Internal)?;
    TypedToolResult::try_new(
        data.encode()?,
        1,
        ToolResultMetadata::try_complete(
            json!({
                "provider": LOCAL_FILES_PROFILE,
                "dataset": mapping.dataset,
            }),
            json!({
                "sourceObject": object_identifier(&entry.source_sha256)?,
                "discoveryReceiptBound": true,
                "executionEligible": false,
            }),
        )?,
        context.limits(),
    )
    .map_err(ServiceError::from)
}

fn ensure_entry_owner(
    entry: &ImportEntry,
    workspace_id: Uuid,
    client_id: Uuid,
) -> Result<(), ServiceError> {
    if entry.workspace_id == workspace_id.hyphenated().to_string()
        && entry.client_id == client_id.hyphenated().to_string()
    {
        Ok(())
    } else {
        Err(ServiceError::Unauthorized)
    }
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn current_timestamp() -> Result<Timestamp, ServiceError> {
    super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)
}

fn map_adapter_error(error: market_squawk_adapter_files::FileAdapterError) -> ServiceError {
    use market_squawk_adapter_files::FileAdapterError;

    match error {
        FileAdapterError::Cancelled => ServiceError::Cancelled,
        FileAdapterError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        FileAdapterError::LimitExceeded(_) | FileAdapterError::RepresentationAuthorityExhausted => {
            ServiceError::ResourceExhausted
        }
        FileAdapterError::ClockFailure
        | FileAdapterError::BlockingTaskFailed
        | FileAdapterError::RepresentationAuthorityLocked
        | FileAdapterError::RepresentationAuthorityUnavailable => ServiceError::Unavailable,
        FileAdapterError::RepresentationAuthorityInvalid
        | FileAdapterError::ExtractionContract(_)
        | FileAdapterError::Contract => ServiceError::Internal,
        _ => ServiceError::InvalidRequest,
    }
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(super::business_arguments(arguments)))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(TABLE[usize::from(byte >> 4)]));
        output.push(char::from(TABLE[usize::from(byte & 0x0f)]));
    }
    output
}
