//! One authenticated immutable source generation, reused across cutoffs and process restarts.
//! This is derived acceleration: relationship selection and rights remain caller-owned and fresh.
use super::*;
use market_squawk_platform::{ArtifactRoot, ResolvedArtifactPath};
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, Write};
use std::sync::Mutex;

const FORMAT: &[u8] = b"market-squawk/sec-prepared-generation/v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecResearchResolvedOutcome {
    Exact(SecResearchReadRequest),
    Missing,
    Ambiguous,
    Stale,
    Revoked,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchIdentityResolution {
    pub(super) request: SecResearchIdentityReadRequest,
    pub(super) identity: CompanySecurityIdentitySelection,
    pub(super) outcome: SecResearchResolvedOutcome,
}
impl SecResearchIdentityResolution {
    pub const fn request(&self) -> &SecResearchIdentityReadRequest {
        &self.request
    }
    pub const fn identity(&self) -> &CompanySecurityIdentitySelection {
        &self.identity
    }
    pub const fn outcome(&self) -> &SecResearchResolvedOutcome {
        &self.outcome
    }
    pub const fn exact_request(&self) -> Option<&SecResearchReadRequest> {
        match &self.outcome {
            SecResearchResolvedOutcome::Exact(value) => Some(value),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecPreparedGenerationReceipt {
    generation_key: EvidenceDigest,
    artifact: crate::ArtifactRecord,
    row_count: usize,
}
impl SecPreparedGenerationReceipt {
    pub const fn generation_key(&self) -> EvidenceDigest {
        self.generation_key
    }
    pub const fn artifact(&self) -> &crate::ArtifactRecord {
        &self.artifact
    }
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecResearchPreparationOutcome {
    Prepared(SecPreparedGenerationReceipt),
    Missing,
    Ambiguous,
    Stale,
    Revoked,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchIdentityPreparation {
    request: SecResearchIdentityReadRequest,
    identity: CompanySecurityIdentitySelection,
    outcome: SecResearchPreparationOutcome,
}
impl SecResearchIdentityPreparation {
    pub const fn request(&self) -> &SecResearchIdentityReadRequest {
        &self.request
    }
    pub const fn identity(&self) -> &CompanySecurityIdentitySelection {
        &self.identity
    }
    pub const fn outcome(&self) -> &SecResearchPreparationOutcome {
        &self.outcome
    }
}

/// Source ordering metadata only; financial eligibility/envelopes remain application projections.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SecResearchSourceCoordinate {
    row: SecResearchRowIdentity,
    available_at: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    effective: ResearchTemporalCoordinate,
    published: Option<ResearchTemporalCoordinate>,
    concept: Option<SourceIdentifier>,
}
impl SecResearchSourceCoordinate {
    pub const fn row(&self) -> SecResearchRowIdentity {
        self.row
    }
    pub const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }
    pub const fn published(&self) -> Option<&ResearchTemporalCoordinate> {
        self.published.as_ref()
    }
    pub const fn concept(&self) -> Option<&SourceIdentifier> {
        self.concept.as_ref()
    }
}

pub(super) struct AuthenticatedGeneration {
    pub(super) origin: SecResearchOrigin,
    pub(super) company_identity: CompanyIdentityExactRecord,
    pub(super) capture_observation_digest: EvidenceDigest,
    pub(super) row_mapping_digest: EvidenceDigest,
    pub(super) observations: SecResearchRows<ResearchObservation>,
    pub(super) filing_xbrl: Option<SecVerifiedFilingXbrl>,
    pub(super) coordinates: Vec<ProviderCaptureRowCoordinate>,
}
#[derive(Serialize, Deserialize)]
struct Header {
    origin: EvidenceDigest,
    binding: EvidenceDigest,
    company: EvidenceDigest,
    capture: EvidenceDigest,
    row_mapping: EvidenceDigest,
    family: u8,
}

/// Retains the exact opened endpoint for every derived row handle and PIT selector.
#[derive(Debug)]
pub(super) struct PreparedArtifact {
    root: ArtifactRoot,
    path: ResolvedArtifactPath,
    file: std::fs::File,
    record: crate::ArtifactRecord,
}
impl PreparedArtifact {
    fn open(
        root: ArtifactRoot,
        record: crate::ArtifactRecord,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>, SecResearchReadError> {
        let path = root
            .resolve(record.relative_reference())
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        let file = path
            .open_read()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        let value = Arc::new(Self {
            root,
            path,
            file,
            record,
        });
        value.verify(deadline, cancellation)?;
        Ok(value)
    }
    fn verify(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SecResearchReadError> {
        check_operation(deadline, cancellation)?;
        self.validate_endpoint()?;
        let mut file = self
            .file
            .try_clone()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        file.rewind()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        let mut hash = Sha256::new();
        let mut total = 0u64;
        let mut bytes = [0u8; 64 * 1024];
        loop {
            check_operation(deadline, cancellation)?;
            let count = file
                .read(&mut bytes)
                .map_err(|_| SecResearchReadError::PreparedIo)?;
            if count == 0 {
                break;
            }
            hash.update(&bytes[..count]);
            total = total
                .checked_add(count as u64)
                .ok_or(SecResearchReadError::PreparedIntegrity)?;
        }
        if total != self.record.size_bytes()
            || evidence_digest(hash.finalize().into()) != self.record.content_digest()
        {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        self.validate_endpoint()?;
        check_operation(deadline, cancellation)
    }
    fn validate_endpoint(&self) -> Result<(), SecResearchReadError> {
        use cap_fs_ext::MetadataExt as _;
        let named = self
            .path
            .open_read()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        let held = cap_std::fs::File::from_std(
            self.file
                .try_clone()
                .map_err(|_| SecResearchReadError::PreparedIo)?,
        )
        .metadata()
        .map_err(|_| SecResearchReadError::PreparedIo)?;
        let named = cap_std::fs::File::from_std(named)
            .metadata()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        if (held.dev(), held.ino(), held.len()) != (named.dev(), named.ino(), named.len())
            || !held.is_file()
        {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        Ok(())
    }
    fn connection(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Connection, SecResearchReadError> {
        check_operation(deadline, cancellation)?;
        self.validate_endpoint()?;
        let connection = Connection::open_with_flags(
            self.root.root().join(self.record.relative_reference()),
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; PRAGMA temp_store=FILE")?;
        let token = cancellation.clone();
        connection.progress_handler(
            1024,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        self.validate_endpoint()?;
        Ok(connection)
    }
}

impl SecResearchReadCapability {
    pub async fn select_by_identity(
        &self,
        request: SecResearchIdentityReadRequest,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchIdentitySelection, SecResearchReadError> {
        let resolution = self.resolve_by_identity(request, deadline, &cancellation)?;
        let outcome = match &resolution.outcome {
            SecResearchResolvedOutcome::Exact(exact) => SecResearchIdentityOutcome::Exact(
                self.select(exact.clone(), raw_store, deadline, cancellation)
                    .await?,
            ),
            other => closed_selection(other),
        };
        Ok(SecResearchIdentitySelection {
            request: resolution.request,
            identity: resolution.identity,
            outcome,
        })
    }
    /// Fast retained-only path. A missing derived generation never starts preparation or network I/O.
    pub async fn select_prepared_by_identity(
        &self,
        request: SecResearchIdentityReadRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchIdentitySelection, SecResearchReadError> {
        let resolution = self.resolve_by_identity(request, deadline, &cancellation)?;
        let outcome = match &resolution.outcome {
            SecResearchResolvedOutcome::Exact(exact) => {
                match self.open_generation(exact, deadline, &cancellation)? {
                    Some(generation) => SecResearchIdentityOutcome::Exact(self.select_generation(
                        exact.clone(),
                        generation,
                        deadline,
                        &cancellation,
                    )?),
                    None => SecResearchIdentityOutcome::PreparationRequired,
                }
            }
            other => closed_selection(other),
        };
        Ok(SecResearchIdentitySelection {
            request: resolution.request,
            identity: resolution.identity,
            outcome,
        })
    }
    pub async fn prepare_by_identity(
        &self,
        request: SecResearchIdentityReadRequest,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchIdentityPreparation, SecResearchReadError> {
        let resolution = self.resolve_by_identity(request, deadline, &cancellation)?;
        let outcome = match &resolution.outcome {
            SecResearchResolvedOutcome::Exact(exact) => SecResearchPreparationOutcome::Prepared(
                self.ensure_generation(exact, raw_store, deadline, &cancellation)
                    .await?
                    .receipt,
            ),
            SecResearchResolvedOutcome::Missing => SecResearchPreparationOutcome::Missing,
            SecResearchResolvedOutcome::Ambiguous => SecResearchPreparationOutcome::Ambiguous,
            SecResearchResolvedOutcome::Stale => SecResearchPreparationOutcome::Stale,
            SecResearchResolvedOutcome::Revoked => SecResearchPreparationOutcome::Revoked,
        };
        Ok(SecResearchIdentityPreparation {
            request: resolution.request,
            identity: resolution.identity,
            outcome,
        })
    }
    /// Analytical callers prepare missing sources through the same index used by UI reads.
    pub async fn select(
        &self,
        request: SecResearchReadRequest,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchSelection, SecResearchReadError> {
        let generation = match self.open_generation(&request, deadline, &cancellation)? {
            Some(generation) => {
                // Analytical replay retains its stronger original-physical-evidence contract.
                // This authenticates exact claims, never rebuilds decoded/native/PIT indexes.
                self.verify_retained_capture(&request, raw_store, deadline, &cancellation)?;
                generation
            }
            None => {
                self.ensure_generation(&request, raw_store, deadline, &cancellation)
                    .await?
            }
        };
        self.select_generation(request, generation, deadline, &cancellation)
    }
    fn verify_retained_capture(
        &self,
        request: &SecResearchReadRequest,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SecResearchReadError> {
        let control = SecResearchOperationControl {
            deadline,
            cancellation,
        };
        match request.family() {
            SecResearchFamily::Submissions => {
                let binding = self
                    .identities
                    .capture_binding_evidence(
                        request.provider_binding_digest(),
                        deadline,
                        cancellation,
                    )?
                    .ok_or(SecResearchReadError::ProviderBindingMismatch)?;
                binding.verify_integrity()?;
                for physical in binding.physical_claims() {
                    check_operation(deadline, cancellation)?;
                    raw_store
                        .verify_claim_with_control(physical.claim(), &control)
                        .map_err(map_raw_store_error)?;
                }
            }
            SecResearchFamily::CompanyFacts | SecResearchFamily::FilingXbrl => {
                let binding = self
                    .identities
                    .logical_publication_binding(
                        request.provider_binding_digest(),
                        deadline,
                        cancellation,
                    )?
                    .ok_or(SecResearchReadError::ProviderBindingMismatch)?;
                for claim in binding.objects().iter().map(|object| object.claim()).chain(
                    binding
                        .partitions()
                        .iter()
                        .map(|partition| partition.claim()),
                ) {
                    check_operation(deadline, cancellation)?;
                    raw_store
                        .open_verified_logical_object_claim(claim, &control)
                        .map_err(map_raw_store_error)?
                        .reverify_for_commit(&control)
                        .map_err(map_raw_store_error)?;
                }
            }
        }
        check_operation(deadline, cancellation)
    }
    fn exact_source(
        &self,
        request: &SecResearchReadRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            SecResearchOrigin,
            CompanyIdentityExactRecord,
            EvidenceDigest,
        ),
        SecResearchReadError,
    > {
        let (pinned, source_id, python) =
            self.manifests
                .read_exact(request.manifest(), deadline, cancellation)?;
        let company = self
            .identities
            .exact_company_identity_by_digest(
                request.company_observation_digest(),
                deadline,
                cancellation,
            )?
            .ok_or(SecResearchReadError::OriginMismatch)?;
        if python.is_some()
            || source_id.as_str() != SEC_SOURCE_ID
            || pinned.manifest() != request.manifest()
            || company.observation().source_id() != &source_id
            || company.observation().surface() != request.family().company_surface()
            || company.manifest_content_digest() != pinned.plan().content_hash().evidence()
            || (match request.family() {
                SecResearchFamily::Submissions => company.provider_binding_digest(),
                _ => company.provider_logical_binding_digest(),
            }) != Some(request.provider_binding_digest())
        {
            return Err(SecResearchReadError::OriginMismatch);
        }
        let ordinal = exact_origin_object_ordinal(&pinned, &company)?;
        let object = pinned
            .objects()
            .get(ordinal)
            .ok_or(SecResearchReadError::OriginMismatch)?
            .object();
        let mut origin = SecResearchOrigin {
            manifest: request.manifest().clone(),
            source_id,
            run_id: company.run_id(),
            control_manifest_id: company.manifest_id(),
            artifact_id: company.artifact_id(),
            object_ordinal: ordinal,
            relative_reference: company
                .artifact_relative_reference()
                .to_owned()
                .into_boxed_str(),
            object_content_digest: object.content_hash().evidence(),
            object_lineage_digest: object.lineage_digest().evidence(),
            object_row_count: object.row_count(),
            object_size_bytes: object.size_bytes(),
            generation_completed_at: company.completed_at(),
            origin_digest: evidence_digest([0; 32]),
        };
        origin.origin_digest = origin_digest(&origin);
        let key = generation_key(request, &origin);
        Ok((origin, company, key))
    }
    fn open_generation(
        &self,
        request: &SecResearchReadRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<OpenedGeneration>, SecResearchReadError> {
        let (origin, company, key) = self.exact_source(request, deadline, cancellation)?;
        let Some(record) = self
            .prepared
            .get(key, origin.artifact_id(), deadline, cancellation)?
        else {
            return Ok(None);
        };
        let artifact = PreparedArtifact::open(
            self.objects.try_clone_artifact_root()?,
            record.clone(),
            deadline,
            cancellation,
        )?;
        let connection = artifact.connection(deadline, cancellation)?;
        let header: Header = decode_metadata(&connection, "source")?;
        if header.origin != origin.origin_digest()
            || header.binding != request.provider_binding_digest()
            || header.company != company.observation_digest()
            || header.family != request.family().tag()
        {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        let check: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if check != "ok" {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        let row_count = usize::try_from(origin.object_row_count())
            .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?;
        if row_count > request.point_in_time_limits().max_candidates() {
            return Err(SecResearchReadError::ObjectBudgetExceeded);
        }
        // Read receipts can outlive this operation (cursor pages reauthorize separately).
        // Only bounded indexed row gets use this shared connection; PIT receives a fresh one.
        connection.progress_handler(0, None::<fn() -> bool>)?;
        let connection = Arc::new(Mutex::new(connection));
        let observations = SecResearchRows::reopen(
            Arc::clone(&artifact),
            Arc::clone(&connection),
            "canonical_rows",
        )?;
        if observations.len() != row_count {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        let filing = SecVerifiedFilingXbrl::reopen(Arc::clone(&artifact), Arc::clone(&connection))?;
        if filing.is_some() != (request.family() == SecResearchFamily::FilingXbrl) {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        check_operation(deadline, cancellation)?;
        Ok(Some(OpenedGeneration {
            receipt: SecPreparedGenerationReceipt {
                generation_key: key,
                artifact: record,
                row_count,
            },
            artifact,
            connection,
            header,
            origin,
            company,
            observations,
            filing,
        }))
    }
    async fn ensure_generation(
        &self,
        request: &SecResearchReadRequest,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OpenedGeneration, SecResearchReadError> {
        if let Some(value) = self.open_generation(request, deadline, cancellation)? {
            return Ok(value);
        }
        let source = self
            .authenticate_generation(request.clone(), raw_store, deadline, cancellation.clone())
            .await?;
        let key = generation_key(request, &source.origin);
        let scratch = self.objects.operation_scratch()?;
        let path = scratch.path().join("prepared.sqlite3");
        let connection = Connection::open(&path)?;
        configure_build(
            &connection,
            request.maximum_spill_bytes(),
            deadline,
            cancellation,
        )?;
        connection.execute_batch("CREATE TABLE metadata(name TEXT PRIMARY KEY,payload BLOB NOT NULL); CREATE TABLE row_indexes(name TEXT PRIMARY KEY,row_count INTEGER NOT NULL,digest BLOB NOT NULL); CREATE TABLE coordinates(ordinal INTEGER PRIMARY KEY,canonical_digest BLOB NOT NULL,observation_digest BLOB NOT NULL,available_at INTEGER,received_at INTEGER NOT NULL,ingested_at INTEGER NOT NULL,source BLOB NOT NULL); BEGIN")?;
        let header = Header {
            origin: source.origin.origin_digest(),
            binding: request.provider_binding_digest(),
            company: source.company_identity.observation_digest(),
            capture: source.capture_observation_digest,
            row_mapping: source.row_mapping_digest,
            family: request.family().tag(),
        };
        connection.execute(
            "INSERT INTO metadata(name,payload) VALUES('source',?1)",
            [serde_json::to_vec(&header).map_err(|_| SecResearchReadError::DigestEncoding)?],
        )?;
        let mut candidates = crate::pit::disk::CandidateStore::new(
            self.objects.operation_scratch()?,
            request.maximum_object_bytes(),
            source.observations.remaining_spill_bytes()?,
            cancellation,
            deadline,
        )
        .map_err(map_disk_pit_error)?;
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        for (ordinal, (observation, coordinate)) in source
            .observations
            .iter()
            .zip(&source.coordinates)
            .enumerate()
        {
            check_operation(deadline, cancellation)?;
            let observation = observation?;
            let bytes = serde_json::to_vec(&observation)
                .map_err(|_| SecResearchReadError::DigestEncoding)?
                .len()
                .checked_mul(2)
                .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
            if bytes > request.maximum_object_bytes() / 4 {
                return Err(SecResearchReadError::ObjectBudgetExceeded);
            }
            if !batch.is_empty()
                && (batch.len() == 256 || batch_bytes + bytes > request.maximum_object_bytes() / 4)
            {
                candidates
                    .append(std::mem::take(&mut batch), request.manifest())
                    .map_err(map_disk_pit_error)?;
                batch_bytes = 0;
            }
            let provenance = observation_context(&observation).provenance();
            let row = SecResearchSourceCoordinate {
                row: SecResearchRowIdentity {
                    row_ordinal: coordinate.canonical_row_ordinal,
                    canonical_row_digest: coordinate.canonical_row_digest,
                    observation_digest: coordinate.observation_digest,
                },
                available_at: provenance.availability().conservative_available_at(),
                received_at: provenance.received_at(),
                ingested_at: provenance.ingested_at(),
                effective: observation_context(&observation).time().effective().clone(),
                published: observation_context(&observation)
                    .time()
                    .published()
                    .cloned(),
                concept: match &observation {
                    ResearchObservation::Fundamental(fact) => Some(fact.concept().clone()),
                    _ => None,
                },
            };
            connection.execute(
                "INSERT INTO coordinates VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    ordinal as i64,
                    coordinate.canonical_row_digest.bytes().as_slice(),
                    coordinate.observation_digest.bytes().as_slice(),
                    row.available_at.map(Timestamp::unix_nanos),
                    row.received_at.unix_nanos(),
                    row.ingested_at.unix_nanos(),
                    serde_json::to_vec(&row).map_err(|_| SecResearchReadError::DigestEncoding)?
                ],
            )?;
            batch.push(observation);
            batch_bytes += bytes;
        }
        if !batch.is_empty() || candidates.len() == 0 {
            candidates
                .append(batch, request.manifest())
                .map_err(map_disk_pit_error)?;
        }
        if candidates.len() != source.observations.len() {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        candidates
            .export_prepared(&connection)
            .map_err(map_disk_pit_error)?;
        // One payload copy is sufficient: the retained row API reads the PIT candidate payload.
        source
            .observations
            .persist_descriptor(&connection, "canonical_rows")?;
        if let Some(filing) = source.filing_xbrl {
            filing.persist_into(&connection, deadline, cancellation)?;
        }
        connection.execute_batch("COMMIT; PRAGMA optimize")?;
        drop(connection);
        drop(candidates);
        let record = self.publish_index(
            &path,
            key,
            source.origin.artifact_id(),
            deadline,
            cancellation,
        )?;
        let _ = record;
        self.open_generation(request, deadline, cancellation)?
            .ok_or(SecResearchReadError::PreparedIntegrity)
    }
    fn publish_index(
        &self,
        path: &std::path::Path,
        key: EvidenceDigest,
        source: Uuid,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<crate::ArtifactRecord, SecResearchReadError> {
        let root = self.objects.try_clone_artifact_root()?;
        let reference = format!("sec-prepared/{}.sqlite3", Uuid::new_v4());
        let resolved = root
            .resolve(&reference)
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        let mut publication = UnregisteredIndex {
            root: root.clone(),
            reference: reference.clone(),
            retain: true,
        };
        let mut output = resolved
            .create_new()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        publication.retain = false;
        #[cfg(unix)]
        {
            use cap_std::fs::PermissionsExt as _;
            output
                .set_permissions(cap_std::fs::Permissions::from_mode(0o600))
                .map_err(|_| SecResearchReadError::PreparedIo)?;
        }
        let mut input = std::fs::File::open(path).map_err(|_| SecResearchReadError::PreparedIo)?;
        let mut buffer = [0u8; 64 * 1024];
        let mut hash = Sha256::new();
        let mut size = 0u64;
        loop {
            check_operation(deadline, cancellation)?;
            let count = input
                .read(&mut buffer)
                .map_err(|_| SecResearchReadError::PreparedIo)?;
            if count == 0 {
                break;
            }
            output
                .write_all(&buffer[..count])
                .map_err(|_| SecResearchReadError::PreparedIo)?;
            hash.update(&buffer[..count]);
            size += count as u64;
        }
        output
            .sync_all()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        root.try_clone_directory()
            .map_err(|_| SecResearchReadError::PreparedIo)?
            .open_dir("sec-prepared")
            .map_err(|_| SecResearchReadError::PreparedIo)?
            .into_std_file()
            .sync_all()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        root.try_clone_directory()
            .map_err(|_| SecResearchReadError::PreparedIo)?
            .into_std_file()
            .sync_all()
            .map_err(|_| SecResearchReadError::PreparedIo)?;
        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|time| i64::try_from(time.as_nanos()).ok())
            .map(Timestamp::from_unix_nanos)
            .ok_or(SecResearchReadError::PreparedIo)?;
        let record = crate::ArtifactRecord::try_new(
            reference,
            evidence_digest(hash.finalize().into()),
            size,
            created,
        )?;
        let verified = PreparedArtifact::open(root, record.clone(), deadline, cancellation)?;
        check_operation(deadline, cancellation)?;
        // A database commit error may be indeterminate. Keep that exact file until catalog
        // reconciliation; cancellation before registration can safely discard our private copy.
        publication.retain = true;
        let bound = self
            .prepared
            .register(key, source, &record, deadline, cancellation)?;
        publication.retain = bound.artifact_id() == record.artifact_id();
        drop(verified);
        Ok(bound)
    }
}

struct UnregisteredIndex {
    root: ArtifactRoot,
    reference: String,
    retain: bool,
}
impl Drop for UnregisteredIndex {
    fn drop(&mut self) {
        if !self.retain {
            if let Ok(directory) = self.root.try_clone_directory() {
                let _ = directory.remove_file(&self.reference);
            }
        }
    }
}
fn closed_selection(value: &SecResearchResolvedOutcome) -> SecResearchIdentityOutcome {
    match value {
        SecResearchResolvedOutcome::Missing => SecResearchIdentityOutcome::Missing,
        SecResearchResolvedOutcome::Ambiguous => SecResearchIdentityOutcome::Ambiguous,
        SecResearchResolvedOutcome::Stale => SecResearchIdentityOutcome::Stale,
        SecResearchResolvedOutcome::Revoked => SecResearchIdentityOutcome::Revoked,
        SecResearchResolvedOutcome::Exact(_) => unreachable!("exact handled before closed outcome"),
    }
}
fn generation_key(request: &SecResearchReadRequest, origin: &SecResearchOrigin) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(FORMAT);
    hash.update(crate::pit::disk::PREPARED_CANDIDATE_VERSION.to_be_bytes());
    hash_evidence(&mut hash, origin.origin_digest());
    hash_evidence(&mut hash, request.provider_binding_digest());
    hash_evidence(&mut hash, request.company_observation_digest());
    hash.update([request.family().tag()]);
    hash_text(&mut hash, request.manifest().schema().name());
    hash.update(request.manifest().schema().version().get().to_be_bytes());
    hash.update(request.manifest().schema().fingerprint());
    evidence_digest(hash.finalize().into())
}
fn configure_build(
    connection: &Connection,
    maximum: u64,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), SecResearchReadError> {
    connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; PRAGMA trusted_schema=OFF")?;
    connection.pragma_update(
        None,
        "max_page_count",
        i64::try_from(maximum / 4096).map_err(|_| SecResearchReadError::SpillBudgetExceeded)?,
    )?;
    let token = cancellation.clone();
    connection.progress_handler(
        1024,
        Some(move || token.is_cancelled() || Instant::now() >= deadline),
    )?;
    Ok(())
}
fn decode_metadata<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    name: &str,
) -> Result<T, SecResearchReadError> {
    let bytes: Vec<u8> = connection.query_row(
        "SELECT payload FROM metadata WHERE name=?1",
        [name],
        |row| row.get(0),
    )?;
    serde_json::from_slice(&bytes).map_err(|_| SecResearchReadError::PreparedIntegrity)
}
struct OpenedGeneration {
    receipt: SecPreparedGenerationReceipt,
    artifact: Arc<PreparedArtifact>,
    connection: Arc<Mutex<Connection>>,
    header: Header,
    origin: SecResearchOrigin,
    company: CompanyIdentityExactRecord,
    observations: SecResearchRows<ResearchObservation>,
    filing: Option<SecVerifiedFilingXbrl>,
}

impl SecResearchReadCapability {
    fn select_generation(
        &self,
        request: SecResearchReadRequest,
        generation: OpenedGeneration,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SecResearchSelection, SecResearchReadError> {
        let OpenedGeneration {
            artifact,
            connection,
            header,
            origin,
            company: company_identity,
            observations,
            filing: filing_xbrl,
            ..
        } = generation;
        let capture_observation_digest = header.capture;
        let row_mapping_digest = header.row_mapping;
        let company_knowable = company_identity
            .observation()
            .availability()
            .conservative_available_at()
            .is_some_and(|at| at <= request.knowledge_at())
            && company_identity.observation().received_at() <= request.knowledge_at()
            && company_identity.observation().ingested_at() <= request.knowledge_at()
            && company_identity.completed_at() <= request.knowledge_at();
        let mut aggregate =
            SecResearchAggregateBudget::new(request.maximum_object_bytes(), 1024 * 1024)?;
        let mut row_identities =
            aggregate.reserve_exact::<SecResearchRowIdentity>(observations.len())?;
        let mut eligible = aggregate.reserve_exact::<bool>(observations.len())?;
        let mut exclusions =
            aggregate.reserve_exact::<SecResearchExcludedRow>(observations.len())?;
        {
            let connection = connection
                .lock()
                .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
            let mut statement=connection.prepare("SELECT ordinal,canonical_digest,observation_digest,available_at,received_at,ingested_at FROM coordinates ORDER BY ordinal")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                check_operation(deadline, cancellation)?;
                let ordinal: i64 = row.get(0)?;
                if usize::try_from(ordinal).ok() != Some(row_identities.len()) {
                    return Err(SecResearchReadError::PreparedIntegrity);
                }
                let identity = SecResearchRowIdentity {
                    row_ordinal: u32::try_from(ordinal)
                        .map_err(|_| SecResearchReadError::PreparedIntegrity)?,
                    canonical_row_digest: evidence_digest(
                        row.get::<_, Vec<u8>>(1)?
                            .try_into()
                            .map_err(|_| SecResearchReadError::PreparedIntegrity)?,
                    ),
                    observation_digest: evidence_digest(
                        row.get::<_, Vec<u8>>(2)?
                            .try_into()
                            .map_err(|_| SecResearchReadError::PreparedIntegrity)?,
                    ),
                };
                let available: Option<i64> = row.get(3)?;
                let knowledge = SecResearchKnowledgeExclusions {
                    available_after_cutoff: available
                        .is_some_and(|at| at > request.knowledge_at().unix_nanos()),
                    received_after_cutoff: row.get::<_, i64>(4)?
                        > request.knowledge_at().unix_nanos(),
                    ingested_after_cutoff: row.get::<_, i64>(5)?
                        > request.knowledge_at().unix_nanos(),
                    generation_completed_after_cutoff: origin.generation_completed_at()
                        > request.knowledge_at(),
                    company_identity_not_knowable: !company_knowable,
                };
                row_identities.push(identity);
                eligible.push(knowledge.is_empty());
                if !knowledge.is_empty() {
                    exclusions.push(SecResearchExcludedRow {
                        row: identity,
                        knowledge,
                        point_in_time_reasons: None,
                        point_in_time: None,
                    });
                }
            }
        }
        if row_identities.len() != observations.len() {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        let mut candidates = crate::pit::disk::CandidateStore::open_prepared(
            artifact.connection(deadline, cancellation)?,
            vec![request.manifest().clone()],
            observations.len(),
            request.maximum_object_bytes(),
            request.maximum_spill_bytes(),
            cancellation,
            deadline,
        )
        .map_err(map_disk_pit_error)?;
        let mut selected = aggregate.reserve_exact::<SecResearchSelectedRow>(
            request.point_in_time_limits().max_result_rows(),
        )?;
        let mut conflicts = aggregate
            .reserve_exact::<SecResearchConflict>(request.point_in_time_limits().max_conflicts())?;
        aggregate.reserve_work(
            observations
                .len()
                .checked_mul(size_of::<(
                    SecResearchRowIdentity,
                    SecResearchPointInTimeIdentities,
                )>())
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
        )?;

        let policy = PointInTimePolicy::try_new(
            NonZeroU32::new(1).ok_or(SecResearchReadError::InvalidRequest)?,
            request.revision_mode(),
        )
        .map_err(|_| SecResearchReadError::PointInTimeSelection)?;
        let pit_request = PointInTimeRequest::try_new(
            policy,
            request.knowledge_at(),
            None,
            request.effective_cutoff().clone(),
            None,
            bounded_point_in_time_limits(&request, &aggregate)?,
        )
        .map_err(|_| SecResearchReadError::PointInTimeSelection)?;
        let outcome = candidates.select_decisions_filtered(&pit_request, |ordinal| {
            eligible
                .get(ordinal)
                .copied()
                .ok_or(PointInTimeError::CanonicalEncoding)
        });
        let (disposition, point_in_time_content_identity, point_in_time_audit_identity) =
            match outcome {
                Ok(selection) => (
                    SecResearchDisposition::Selected,
                    Some(selection.content_identity()),
                    selection.audit_identity(),
                ),
                Err(PointInTimeError::DiskRevisionConflicts { audit_identity, .. }) => {
                    (SecResearchDisposition::Conflict, None, audit_identity)
                }
                Err(error) => return Err(map_disk_pit_error(error)),
            };
        let mut conflict_groups = std::collections::BTreeMap::<
            ([u8; 32], u32),
            (
                Sha256Digest,
                market_squawk_domain::RevisionNumber,
                Vec<(SecResearchRowIdentity, SecResearchPointInTimeIdentities)>,
            ),
        >::new();
        candidates
            .visit_decisions(|ordinal, record, decision| {
                let row = *row_identities
                    .get(ordinal)
                    .ok_or(PointInTimeError::CanonicalEncoding)?;
                let identities = point_in_time_identities(&record);
                match decision {
                    crate::pit::disk::DecisionDisposition::Selected => {
                        if disposition == SecResearchDisposition::Selected {
                            selected.push(SecResearchSelectedRow {
                                row,
                                point_in_time: identities,
                            });
                        }
                    }
                    crate::pit::disk::DecisionDisposition::Excluded(reasons) => {
                        exclusions.push(SecResearchExcludedRow {
                            row,
                            knowledge: SecResearchKnowledgeExclusions::default(),
                            point_in_time_reasons: Some(reasons),
                            point_in_time: Some(identities),
                        })
                    }
                    crate::pit::disk::DecisionDisposition::Conflict => {
                        let revision = record.revision;
                        conflict_groups
                            .entry((record.family_identity.bytes(), revision.get()))
                            .or_insert_with(|| (record.family_identity, revision, Vec::new()))
                            .2
                            .push((row, identities));
                    }
                }
                Ok(())
            })
            .map_err(map_disk_pit_error)?;
        for (_, (family_identity, revision, rows)) in conflict_groups {
            conflicts.push(SecResearchConflict {
                family_identity,
                revision,
                rows: rows.into_boxed_slice(),
            });
        }
        let disposition = if disposition == SecResearchDisposition::Selected && selected.is_empty()
        {
            SecResearchDisposition::Unavailable
        } else {
            disposition
        };
        selected.sort_by_key(|row| row.row.row_ordinal);
        exclusions.sort_by_key(|row| row.row.row_ordinal);
        conflicts.sort_by(|left, right| {
            left.family_identity
                .bytes()
                .cmp(&right.family_identity.bytes())
                .then_with(|| left.revision.get().cmp(&right.revision.get()))
        });
        let selection_digest = selection_digest(
            disposition,
            point_in_time_content_identity,
            point_in_time_audit_identity,
            &selected,
            &exclusions,
            &conflicts,
            deadline,
            cancellation,
        )?;
        aggregate.reserve_work(
            selected
                .len()
                .checked_mul(size_of::<SecResearchSelectedRow>())
                .and_then(|bytes| {
                    bytes.checked_add(
                        exclusions
                            .len()
                            .checked_mul(size_of::<SecResearchExcludedRow>())?,
                    )
                })
                .and_then(|bytes| {
                    bytes.checked_add(
                        conflicts
                            .len()
                            .checked_mul(size_of::<SecResearchConflict>())?,
                    )
                })
                .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
        )?;
        let result_digest = result_digest(
            request.request_digest(),
            origin.origin_digest(),
            request.provider_binding_digest(),
            capture_observation_digest,
            row_mapping_digest,
            company_identity.observation_digest(),
            selection_digest,
        );
        let receipt = SecResearchSelectionReceipt {
            request_digest: request.request_digest(),
            origin_digest: origin.origin_digest(),
            provider_binding_digest: request.provider_binding_digest(),
            capture_observation_digest,
            row_mapping_digest,
            company_observation_digest: company_identity.observation_digest(),
            point_in_time_content_identity,
            point_in_time_audit_identity,
            selection_digest,
            result_digest,
        };
        Ok(SecResearchSelection {
            request,
            origin,
            company_identity,
            decoded_rows: observations,
            filing_xbrl,
            disposition,
            selected: selected.into_boxed_slice(),
            exclusions: exclusions.into_boxed_slice(),
            conflicts: conflicts.into_boxed_slice(),
            receipt,
            display: None,
        })
    }
}

/// Opaque application-owned display grouping; this type assigns no financial meaning.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SecResearchDisplayCoordinate {
    envelope: Option<Vec<u8>>,
    effective_day: i64,
    effective_time: Option<i64>,
    published_day: Option<i64>,
    published_time: Option<i64>,
}
impl SecResearchDisplayCoordinate {
    pub fn new(
        envelope: Option<Vec<u8>>,
        effective_day: i64,
        effective_time: Option<i64>,
        published_day: Option<i64>,
        published_time: Option<i64>,
    ) -> Self {
        Self {
            envelope,
            effective_day,
            effective_time,
            published_day,
            published_time,
        }
    }
    pub fn envelope(&self) -> Option<&[u8]> {
        self.envelope.as_deref()
    }
    pub const fn effective_day(&self) -> i64 {
        self.effective_day
    }
    pub const fn effective_time(&self) -> Option<i64> {
        self.effective_time
    }
    pub const fn published_day(&self) -> Option<i64> {
        self.published_day
    }
    pub const fn published_time(&self) -> Option<i64> {
        self.published_time
    }
}
/// An already authenticated original row. Preparation never assigns a request-time PIT decision.
pub struct SecResearchSourceRow<'row> {
    observation: &'row ResearchObservation,
    family: SecResearchFamily,
    origin: &'row SecResearchOrigin,
    company: &'row CompanyIdentityExactRecord,
}
impl SecResearchSourceRow<'_> {
    pub const fn observation(&self) -> &ResearchObservation {
        self.observation
    }
    pub const fn family(&self) -> SecResearchFamily {
        self.family
    }
    pub const fn origin(&self) -> &SecResearchOrigin {
        self.origin
    }
    pub const fn company_identity(&self) -> &CompanyIdentityExactRecord {
        self.company
    }
}
/// The application owns projection semantics and advances identity whenever they change.
pub trait SecResearchDisplayProjector: Send + Sync {
    fn identity(&self) -> EvidenceDigest;
    fn project(
        &self,
        row: &SecResearchSourceRow<'_>,
        state: PointInTimeRevisionState,
    ) -> Result<Option<SecResearchDisplayCoordinate>, SecResearchReadError>;
}
#[derive(Clone, Debug)]
pub(super) struct SecResearchDisplayRows {
    artifact: Arc<PreparedArtifact>,
    connection: Arc<Mutex<Connection>>,
    projector: EvidenceDigest,
}
impl PartialEq for SecResearchDisplayRows {
    fn eq(&self, other: &Self) -> bool {
        self.artifact.record == other.artifact.record && self.projector == other.projector
    }
}
impl Eq for SecResearchDisplayRows {}
impl SecResearchSelection {
    /// Positions refer to selected(), while each lookup retains the original source ordinal.
    /// Omitted display rows remain fully present in the source and selection evidence.
    pub fn selected_display_coordinates(
        &self,
    ) -> Result<
        impl Iterator<
            Item = Result<(usize, Option<SecResearchDisplayCoordinate>), SecResearchReadError>,
        > + '_,
        SecResearchReadError,
    > {
        let display = self
            .display
            .as_ref()
            .ok_or(SecResearchReadError::PreparationRequired)?;
        display.artifact.validate_endpoint()?;
        Ok(self
            .selected
            .iter()
            .enumerate()
            .map(move |(position, selected)| {
                let connection = display
                    .connection
                    .lock()
                    .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
                let payload: Option<Vec<u8>> = connection.query_row(
                    "SELECT payload FROM display_rows WHERE ordinal=?1 AND state=?2",
                    params![
                        selected.row.row_ordinal,
                        state_tag(selected.point_in_time.revision_state)
                    ],
                    |row| row.get(0),
                )?;
                let coordinate = payload
                    .map(|bytes| {
                        serde_json::from_slice(&bytes)
                            .map_err(|_| SecResearchReadError::PreparedIntegrity)
                    })
                    .transpose()?;
                Ok((position, coordinate))
            }))
    }
}
impl SecResearchReadCapability {
    pub async fn prepare_display_by_identity(
        &self,
        request: SecResearchIdentityReadRequest,
        raw_store: &SealedResearchJournalStore,
        projector: &dyn SecResearchDisplayProjector,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchIdentityPreparation, SecResearchReadError> {
        let resolution = self.resolve_by_identity(request, deadline, &cancellation)?;
        let outcome = match &resolution.outcome {
            SecResearchResolvedOutcome::Exact(exact) => {
                if !valid_sha256(projector.identity()) {
                    return Err(SecResearchReadError::InvalidRequest);
                }
                let generation = self
                    .ensure_generation(exact, raw_store, deadline, &cancellation)
                    .await?;
                let key = display_key(generation.receipt.generation_key, projector.identity());
                if self
                    .open_display(&generation, projector.identity(), deadline, &cancellation)?
                    .is_none()
                {
                    let scratch = self.objects.operation_scratch()?;
                    let path = scratch.path().join("display.sqlite3");
                    let connection = Connection::open(&path)?;
                    configure_build(
                        &connection,
                        exact.maximum_spill_bytes(),
                        deadline,
                        &cancellation,
                    )?;
                    connection.execute_batch("CREATE TABLE metadata(name TEXT PRIMARY KEY,payload BLOB NOT NULL); CREATE TABLE display_rows(ordinal INTEGER NOT NULL,state INTEGER NOT NULL,payload BLOB,PRIMARY KEY(ordinal,state)) WITHOUT ROWID; BEGIN")?;
                    connection.execute(
                        "INSERT INTO metadata(name,payload) VALUES('display',?1)",
                        [serde_json::to_vec(&(
                            generation.receipt.generation_key,
                            projector.identity(),
                            generation.receipt.row_count,
                        ))
                        .map_err(|_| SecResearchReadError::DigestEncoding)?],
                    )?;
                    for (ordinal, observation) in generation.observations.iter().enumerate() {
                        check_operation(deadline, &cancellation)?;
                        let observation = observation?;
                        let row = SecResearchSourceRow {
                            observation: &observation,
                            family: exact.family(),
                            origin: &generation.origin,
                            company: &generation.company,
                        };
                        for state in [
                            PointInTimeRevisionState::Current,
                            PointInTimeRevisionState::Superseded,
                            PointInTimeRevisionState::SupersessionIncomparable,
                        ] {
                            let projected = projector.project(&row, state)?;
                            let bytes = projected
                                .map(|value| {
                                    serde_json::to_vec(&value)
                                        .map_err(|_| SecResearchReadError::DigestEncoding)
                                })
                                .transpose()?;
                            if bytes
                                .as_ref()
                                .is_some_and(|value| value.len() > exact.maximum_object_bytes() / 4)
                            {
                                return Err(SecResearchReadError::ObjectBudgetExceeded);
                            }
                            connection.execute(
                                "INSERT INTO display_rows(ordinal,state,payload) VALUES(?1,?2,?3)",
                                params![ordinal as i64, state_tag(state), bytes],
                            )?;
                        }
                    }
                    connection.execute_batch("COMMIT")?;
                    drop(connection);
                    self.publish_index(
                        &path,
                        key,
                        generation.origin.artifact_id(),
                        deadline,
                        &cancellation,
                    )?;
                }
                let display = self
                    .open_display(&generation, projector.identity(), deadline, &cancellation)?
                    .ok_or(SecResearchReadError::PreparedIntegrity)?;
                SecResearchPreparationOutcome::Prepared(SecPreparedGenerationReceipt {
                    generation_key: key,
                    artifact: display.artifact.record.clone(),
                    row_count: generation.receipt.row_count,
                })
            }
            SecResearchResolvedOutcome::Missing => SecResearchPreparationOutcome::Missing,
            SecResearchResolvedOutcome::Ambiguous => SecResearchPreparationOutcome::Ambiguous,
            SecResearchResolvedOutcome::Stale => SecResearchPreparationOutcome::Stale,
            SecResearchResolvedOutcome::Revoked => SecResearchPreparationOutcome::Revoked,
        };
        Ok(SecResearchIdentityPreparation {
            request: resolution.request,
            identity: resolution.identity,
            outcome,
        })
    }
    pub async fn select_prepared_display_by_identity(
        &self,
        request: SecResearchIdentityReadRequest,
        projector_identity: EvidenceDigest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchIdentitySelection, SecResearchReadError> {
        let resolution = self.resolve_by_identity(request, deadline, &cancellation)?;
        let outcome = match &resolution.outcome {
            SecResearchResolvedOutcome::Exact(exact) => {
                if let Some(generation) = self.open_generation(exact, deadline, &cancellation)? {
                    if let Some(display) =
                        self.open_display(&generation, projector_identity, deadline, &cancellation)?
                    {
                        let mut selection = self.select_generation(
                            exact.clone(),
                            generation,
                            deadline,
                            &cancellation,
                        )?;
                        selection.display = Some(display);
                        SecResearchIdentityOutcome::Exact(selection)
                    } else {
                        SecResearchIdentityOutcome::PreparationRequired
                    }
                } else {
                    SecResearchIdentityOutcome::PreparationRequired
                }
            }
            other => closed_selection(other),
        };
        check_operation(deadline, &cancellation)?;
        Ok(SecResearchIdentitySelection {
            request: resolution.request,
            identity: resolution.identity,
            outcome,
        })
    }
    fn open_display(
        &self,
        generation: &OpenedGeneration,
        projector: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<SecResearchDisplayRows>, SecResearchReadError> {
        if !valid_sha256(projector) {
            return Err(SecResearchReadError::InvalidRequest);
        }
        let key = display_key(generation.receipt.generation_key, projector);
        let Some(record) =
            self.prepared
                .get(key, generation.origin.artifact_id(), deadline, cancellation)?
        else {
            return Ok(None);
        };
        let artifact = PreparedArtifact::open(
            self.objects.try_clone_artifact_root()?,
            record,
            deadline,
            cancellation,
        )?;
        let connection = artifact.connection(deadline, cancellation)?;
        let header: (EvidenceDigest, EvidenceDigest, usize) =
            decode_metadata(&connection, "display")?;
        if header
            != (
                generation.receipt.generation_key,
                projector,
                generation.receipt.row_count,
            )
        {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        let count: i64 =
            connection.query_row("SELECT COUNT(*) FROM display_rows", [], |row| row.get(0))?;
        if integrity != "ok"
            || usize::try_from(count).ok() != generation.receipt.row_count.checked_mul(3)
        {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        check_operation(deadline, cancellation)?;
        connection.progress_handler(0, None::<fn() -> bool>)?;
        Ok(Some(SecResearchDisplayRows {
            artifact,
            connection: Arc::new(Mutex::new(connection)),
            projector,
        }))
    }
}
fn display_key(generation: EvidenceDigest, projector: EvidenceDigest) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sec-prepared-display/v1\0");
    hash_evidence(&mut hash, generation);
    hash_evidence(&mut hash, projector);
    evidence_digest(hash.finalize().into())
}
const fn state_tag(state: PointInTimeRevisionState) -> u8 {
    match state {
        PointInTimeRevisionState::Current => 1,
        PointInTimeRevisionState::Superseded => 2,
        PointInTimeRevisionState::SupersessionIncomparable => 3,
    }
}
