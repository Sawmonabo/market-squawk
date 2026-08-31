//! Receipt-required final admission of exact feature datasets for product and model use.

use std::cmp::Ordering;
use std::io;
use std::num::NonZeroU32;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use rusqlite::{OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::export::encode;
use super::production::FeatureDatasetProductContract;
use super::{
    DatasetBuildError, DatasetBuildRequest, DatasetBuilderService, DatasetOutputAuthorization,
    FeatureLabelDataset, build,
};
use crate::{
    CatalogEndpointIdentity, DatasetBuildSpecDigest, DatasetManifestRef, GenerationKind,
    PythonDatasetCatalogError, ResearchUse, ResearchUseDecisionDigest, ResearchUseGraphDigest,
    ResearchUseRequest, Sha256Digest, SourceOperation,
};

/// Exact schema identity for the authoritative catalog-retained production receipt.
pub const FEATURE_DATASET_PRODUCTION_RECEIPT_SCHEMA: &str =
    "market-squawk.feature-dataset-production-receipt/v1";
/// Maximum number of exact producer-evidence bindings in one production receipt.
const MAX_FEATURE_DATASET_PRODUCTION_EVIDENCE_BINDINGS: usize = 64;
/// Maximum canonical bytes retained for one production receipt.
pub const MAX_FEATURE_DATASET_PRODUCTION_RECEIPT_BYTES: usize = 1024 * 1024;

const PRODUCTION_EVIDENCE_DOMAIN: &[u8] = b"market-squawk/feature-dataset-production-evidence/v1\0";
const PRODUCTION_IDENTITY_DOMAIN: &[u8] = b"market-squawk/feature-dataset-production-identity/v1\0";
const PRODUCTION_EVIDENCE_SCHEMA: &str = "market-squawk.feature-dataset-production-evidence/v1";
const APPLICATION_PRODUCER_ATTESTATION: &str = "application-producer-attested-authority-digests";
const MAX_RECEIPT_SCHEMA_BYTES: usize = 128;
const MAX_PRODUCT_CONTRACT_BYTES: usize = 128;
const MAX_DATASET_ID_BYTES: usize = 256;
const MAX_DATASET_SCHEMA_NAME_BYTES: usize = 128;
const MAX_RESEARCH_USE_NAME_BYTES: usize = 32;

/// One closed, versioned, digest-bound authority or derivation fact supplied by a producer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FeatureDatasetProductionEvidenceBinding {
    kind: SourceIdentifier,
    schema_version: NonZeroU32,
    evidence: EvidenceDigest,
}

impl FeatureDatasetProductionEvidenceBinding {
    /// Constructs one non-reserved exact producer-evidence coordinate.
    pub(super) fn try_new(
        kind: SourceIdentifier,
        schema_version: NonZeroU32,
        evidence: EvidenceDigest,
    ) -> Result<Self, PythonDatasetCatalogError> {
        if evidence.bytes() == [0; 32] {
            return Err(PythonDatasetCatalogError::InvalidProductionEvidence);
        }
        Ok(Self {
            kind,
            schema_version,
            evidence,
        })
    }
}

/// Canonical, bounded producer evidence included in the final data-owned receipt envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FeatureDatasetProductionEvidenceV1 {
    producer: SourceIdentifier,
    implementation_revision: SourceIdentifier,
    attested_at: Timestamp,
    currentness_expires_at: Timestamp,
    bindings: Box<[FeatureDatasetProductionEvidenceBinding]>,
    identity: Sha256Digest,
}

impl FeatureDatasetProductionEvidenceV1 {
    /// Constructs canonical producer evidence, rejecting empty, repeated, or excessive bindings.
    pub(super) fn try_new(
        producer: SourceIdentifier,
        implementation_revision: SourceIdentifier,
        attested_at: Timestamp,
        currentness_expires_at: Timestamp,
        mut bindings: Vec<FeatureDatasetProductionEvidenceBinding>,
    ) -> Result<Self, PythonDatasetCatalogError> {
        if bindings.is_empty()
            || bindings.len() > MAX_FEATURE_DATASET_PRODUCTION_EVIDENCE_BINDINGS
            || attested_at >= currentness_expires_at
        {
            return Err(PythonDatasetCatalogError::InvalidProductionEvidence);
        }
        bindings.sort_unstable_by(compare_evidence_bindings);
        if bindings.windows(2).any(|pair| pair[0].kind == pair[1].kind) {
            return Err(PythonDatasetCatalogError::InvalidProductionEvidence);
        }
        let identity = production_evidence_identity(
            &producer,
            &implementation_revision,
            attested_at,
            currentness_expires_at,
            &bindings,
        );
        Ok(Self {
            producer,
            implementation_revision,
            attested_at,
            currentness_expires_at,
            bindings: bindings.into_boxed_slice(),
            identity,
        })
    }
}

/// Canonical immutable receipt required by feature/model product reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureDatasetProductionReceiptV1 {
    production_identity: Sha256Digest,
    receipt_sha256: Sha256Digest,
    canonical_json: Box<[u8]>,
    product_contract: FeatureDatasetProductContract,
    producer_evidence: FeatureDatasetProductionEvidenceV1,
    output_group_id: [u8; 32],
    final_output_rights_id: [u8; 32],
    research_decision: ResearchUseDecisionDigest,
    research_graph: ResearchUseGraphDigest,
    research_use: ResearchUse,
    research_use_expires_at: Timestamp,
    admitted_at: Timestamp,
}

impl FeatureDatasetProductionReceiptV1 {
    /// Returns the stable semantic product identity; fresh retries retain this value.
    pub const fn production_identity(&self) -> Sha256Digest {
        self.production_identity
    }

    /// Returns the SHA-256 identity of the exact canonical retained receipt bytes.
    pub const fn receipt_sha256(&self) -> Sha256Digest {
        self.receipt_sha256
    }

    /// Returns exact authoritative canonical JSON retained in the analytical catalog.
    pub fn canonical_json(&self) -> &[u8] {
        &self.canonical_json
    }

    /// Returns the exact closed recipe and independently authorized consumer use.
    pub const fn product_contract(&self) -> FeatureDatasetProductContract {
        self.product_contract
    }

    /// Returns the immutable phase-one derived-publication identity.
    pub const fn output_group_id(&self) -> [u8; 32] {
        self.output_group_id
    }

    /// Returns the exact fresh output-persistence rights decision bound at first admission.
    pub const fn final_output_rights_id(&self) -> [u8; 32] {
        self.final_output_rights_id
    }

    /// Returns the fresh ResearchUse decision bound at final admission.
    pub const fn research_decision(&self) -> ResearchUseDecisionDigest {
        self.research_decision
    }

    /// Returns the exact transitive parent graph authorized at final admission.
    pub const fn research_graph(&self) -> ResearchUseGraphDigest {
        self.research_graph
    }

    /// Returns the independently authorized downstream use.
    pub const fn research_use(&self) -> ResearchUse {
        self.research_use
    }

    /// Returns the exclusive expiry that bounded the admission-time capability.
    ///
    /// This is immutable decision evidence, not a freshness deadline for the admitted dataset.
    pub const fn research_use_expires_at(&self) -> Timestamp {
        self.research_use_expires_at
    }

    /// Returns the trusted catalog transaction time of final admission.
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }

    pub(crate) fn decode_and_validate(
        canonical_json: &[u8],
        expectation: &FeatureDatasetProductionReceiptExpectation<'_>,
    ) -> Result<Self, PythonDatasetCatalogError> {
        if canonical_json.is_empty()
            || canonical_json.len() > MAX_FEATURE_DATASET_PRODUCTION_RECEIPT_BYTES
            || Sha256Digest::new(Sha256::digest(canonical_json).into())
                != expectation.receipt_sha256
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        let wire: ReceiptWire = serde_json::from_slice(canonical_json)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        let canonical = canonical_receipt_bytes(&wire)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        if canonical.as_slice() != canonical_json {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        let producer_evidence = producer_evidence_from_wire(&wire.producer_evidence)?;
        let expected_production_identity = production_identity(expectation, &producer_evidence);
        if wire.schema != FEATURE_DATASET_PRODUCTION_RECEIPT_SCHEMA
            || FeatureDatasetProductContract::from_identity(&wire.product_contract)
                != Some(expectation.product_contract)
            || parse_sha256(&wire.production_identity_sha256)? != expectation.production_identity
            || expectation.production_identity != expected_production_identity
            || parse_sha256_bytes(&wire.catalog_identity_sha256)?
                != expectation.catalog_identity.bytes()
            || !dataset_wire_matches(&wire.dataset, expectation)
            || parse_sha256_bytes(&wire.output_group_sha256)? != expectation.output_group_id
            || parse_sha256_bytes(&wire.output_authorization.final_output_rights_sha256)?
                != expectation.final_output_rights_id
            || parse_sha256(&wire.python_export_sha256)? != expectation.export_sha256
            || parse_research_decision(&wire.research_authorization.decision_sha256)?
                != expectation.research_decision
            || parse_research_graph(&wire.research_authorization.graph_sha256)?
                != expectation.research_graph
            || parse_research_use(&wire.research_authorization.research_use)?
                != expectation.research_use
            || wire.research_authorization.expires_at_unix_nanos
                != expectation.research_use_expires_at.unix_nanos()
            || wire.admitted_at_unix_nanos != expectation.admitted_at.unix_nanos()
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        Ok(Self {
            production_identity: expectation.production_identity,
            receipt_sha256: expectation.receipt_sha256,
            canonical_json: canonical.into_boxed_slice(),
            product_contract: expectation.product_contract,
            producer_evidence,
            output_group_id: expectation.output_group_id,
            final_output_rights_id: expectation.final_output_rights_id,
            research_decision: expectation.research_decision,
            research_graph: expectation.research_graph,
            research_use: expectation.research_use,
            research_use_expires_at: expectation.research_use_expires_at,
            admitted_at: expectation.admitted_at,
        })
    }
}

/// Whether final admission published a new atomic pair or replayed its exact retained result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FeatureDatasetProductionAdmissionDisposition {
    /// The descriptor and canonical receipt were atomically admitted by this call.
    Published,
    /// The exact semantic product was already retained and revalidated.
    Replay,
}

/// Complete final product admission returned only after atomic descriptor/receipt publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FeatureDatasetProductionAdmission {
    receipt: FeatureDatasetProductionReceiptV1,
    disposition: FeatureDatasetProductionAdmissionDisposition,
}

impl FeatureDatasetProductionAdmission {
    pub(super) const fn disposition(&self) -> FeatureDatasetProductionAdmissionDisposition {
        self.disposition
    }

    pub(super) fn into_receipt(self) -> FeatureDatasetProductionReceiptV1 {
        self.receipt
    }
}

pub(crate) struct FeatureDatasetProductionReceiptExpectation<'a> {
    pub(crate) production_identity: Sha256Digest,
    pub(crate) receipt_sha256: Sha256Digest,
    pub(crate) catalog_identity: CatalogEndpointIdentity,
    pub(crate) product_contract: FeatureDatasetProductContract,
    pub(crate) manifest: &'a DatasetManifestRef,
    pub(crate) build_spec_digest: DatasetBuildSpecDigest,
    pub(crate) policy_digest: Sha256Digest,
    pub(crate) universe_digest: Sha256Digest,
    pub(crate) universe_id: &'a str,
    pub(crate) output_group_id: [u8; 32],
    pub(crate) final_output_rights_id: [u8; 32],
    pub(crate) export_sha256: Sha256Digest,
    pub(crate) research_decision: ResearchUseDecisionDigest,
    pub(crate) research_graph: ResearchUseGraphDigest,
    pub(crate) research_use: ResearchUse,
    pub(crate) research_use_expires_at: Timestamp,
    pub(crate) admitted_at: Timestamp,
}

pub(super) fn register(
    builder: &DatasetBuilderService<'_>,
    publisher_session: Uuid,
    product_contract: FeatureDatasetProductContract,
    request: &DatasetBuildRequest,
    dataset: &FeatureLabelDataset,
    producer_evidence: FeatureDatasetProductionEvidenceV1,
    cancellation: &CancellationToken,
) -> Result<FeatureDatasetProductionAdmission, DatasetBuildError> {
    if cancellation.is_cancelled() {
        return Err(DatasetBuildError::Cancelled);
    }
    if request.intended_use() != product_contract.required_use()
        || builder.service.catalog_session_id() != publisher_session
    {
        return Err(DatasetBuildError::InvalidRequest);
    }
    validate_request_dataset(builder, request, dataset)?;
    let final_output_rights =
        build::authorize_existing_output(builder, request, dataset.pinned(), cancellation)?;
    let export = encode(dataset)?;
    let authority = builder
        .authority
        .lock()
        .map_err(|_| DatasetBuildError::AuthorityLockPoisoned)?;
    let research_request = ResearchUseRequest::try_new(
        request.inputs().parents().to_vec(),
        request.intended_use(),
        request.research_use_limits(),
    )?;
    let authorization = authority.authorize_research_use(research_request, cancellation)?;
    if authorization.research_use() != request.intended_use()
        || authorization.graph().roots() != request.inputs().parents()
    {
        return Err(DatasetBuildError::InvalidRequest);
    }
    let research_decision = authorization.decision_digest();
    let research_graph = authorization.graph().digest();
    let research_use = authorization.research_use();
    let research_use_expires_at = authorization.expires_at();
    drop(authorization);
    if cancellation.is_cancelled() {
        return Err(DatasetBuildError::Cancelled);
    }
    let catalog_identity = authority.catalog_endpoint_identity()?;
    let manifest = dataset.manifest();
    let manifest_version_sql = i64::try_from(manifest.manifest_version())
        .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
    let export_sha256 = export.content_hash();
    let export_bytes = export.bytes();
    let audit_subject = admission_audit_subject(manifest)?;
    let (receipt, disposition) = authority
        .with_feature_dataset_production_transaction(|transaction, catalog_session, now| {
            if cancellation.is_cancelled() {
                return Err(PythonDatasetCatalogError::Cancelled);
            }
            if catalog_session != publisher_session {
                return Err(PythonDatasetCatalogError::InvalidProductionEvidence);
            }
            if now >= research_use_expires_at {
                return Err(PythonDatasetCatalogError::ResearchAuthorizationExpired);
            }
            validate_fresh_research_authorization(
                transaction,
                research_decision,
                research_graph,
                research_use,
                research_use_expires_at,
                request.inputs().parents(),
                now,
            )?;
            let generation = load_generation_authority(
                transaction,
                manifest.dataset_id().as_str(),
                manifest_version_sql,
            )?
            .ok_or(PythonDatasetCatalogError::UnknownAdmission)?;
            validate_generation(dataset, &generation)?;
            validate_objects(transaction, dataset, manifest_version_sql)?;
            let [output_object] = dataset.pinned().objects() else {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            };
            let expected_output_payload = EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                output_object.object().content_hash().bytes(),
            );
            if final_output_rights.payload_digest() != expected_output_payload {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
            validate_final_output_rights(
                transaction,
                final_output_rights.rights_id(),
                request.output_authorization(),
                expected_output_payload,
                now,
            )?;
            if generation.requested_use != research_use.database_name()
                || generation.graph_digest.as_slice() != research_graph.bytes()
            {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
            let output_group_id = array_32(&generation.output_group_id)?;
            let stable_expectation = FeatureDatasetProductionReceiptExpectation {
                production_identity: Sha256Digest::new([0; 32]),
                receipt_sha256: Sha256Digest::new([0; 32]),
                catalog_identity,
                product_contract,
                manifest,
                build_spec_digest: dataset.build_spec_digest(),
                policy_digest: dataset.policy_digest(),
                universe_digest: dataset.universe_digest(),
                universe_id: request.inputs().universe_id().as_str(),
                output_group_id,
                final_output_rights_id: final_output_rights.rights_id(),
                export_sha256,
                research_decision,
                research_graph,
                research_use,
                research_use_expires_at,
                admitted_at: now,
            };
            let production_identity = production_identity(&stable_expectation, &producer_evidence);
            if let Some(retained) = load_retained_admission(
                transaction,
                manifest.dataset_id().as_str(),
                manifest_version_sql,
            )? {
                let retained_receipt = validate_retained_admission(
                    retained,
                    &stable_expectation,
                    production_identity,
                    &producer_evidence,
                    export_bytes,
                )?;
                if cancellation.is_cancelled() {
                    return Err(PythonDatasetCatalogError::Cancelled);
                }
                return Ok((
                    retained_receipt,
                    FeatureDatasetProductionAdmissionDisposition::Replay,
                ));
            }
            if producer_evidence.attested_at > now
                || now >= producer_evidence.currentness_expires_at
            {
                return Err(PythonDatasetCatalogError::InvalidProductionEvidence);
            }
            let receipt =
                encode_receipt(&stable_expectation, production_identity, &producer_evidence)?;
            if has_cross_admission_unique_collision(
                transaction,
                production_identity,
                receipt.receipt_sha256(),
                export_sha256,
                output_group_id,
                research_decision,
            )? {
                return Err(PythonDatasetCatalogError::ConflictingProductionAdmission);
            }
            if cancellation.is_cancelled() {
                return Err(PythonDatasetCatalogError::Cancelled);
            }
            transaction.execute(
                "INSERT INTO feature_dataset_production_admissions
                 (production_identity_sha256, receipt_schema, receipt_sha256, receipt_json,
                  product_contract, export_sha256, catalog_identity, dataset_id, manifest_version,
                  descriptor_json,
                  selection_digest_version, output_group_id, final_output_rights_id,
                  research_decision_id, research_graph_digest, research_use,
                  research_use_expires_at_ns, admitted_at_ns)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 2, ?11, ?12, ?13,
                         ?14, ?15, ?16, ?17)",
                params![
                    production_identity.bytes(),
                    FEATURE_DATASET_PRODUCTION_RECEIPT_SCHEMA,
                    receipt.receipt_sha256().bytes(),
                    receipt.canonical_json(),
                    product_contract.identity(),
                    export_sha256.bytes(),
                    catalog_identity.bytes(),
                    manifest.dataset_id().as_str(),
                    manifest_version_sql,
                    export_bytes,
                    output_group_id,
                    final_output_rights.rights_id(),
                    research_decision.bytes(),
                    research_graph.bytes(),
                    research_use.database_name(),
                    research_use_expires_at.unix_nanos(),
                    now.unix_nanos(),
                ],
            )?;
            let retained = load_retained_admission(
                transaction,
                manifest.dataset_id().as_str(),
                manifest_version_sql,
            )?
            .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
            let retained_receipt = validate_retained_admission(
                retained,
                &stable_expectation,
                production_identity,
                &producer_evidence,
                export_bytes,
            )?;
            // Cancellation observed through this point rolls back both the admission and audit
            // rows. The remaining audit insert plus catalog commit is the explicit commit-wins
            // boundary: there is no further fallible application work or await after it begins.
            if cancellation.is_cancelled() {
                return Err(PythonDatasetCatalogError::Cancelled);
            }
            transaction.execute(
                "INSERT INTO audit_events
                     (event_type, subject_id, details_digest, occurred_at_ns)
                     VALUES ('feature-dataset.production-admitted', ?1, ?2, ?3)",
                params![
                    audit_subject,
                    retained_receipt.receipt_sha256().bytes(),
                    now.unix_nanos(),
                ],
            )?;
            Ok((
                retained_receipt,
                FeatureDatasetProductionAdmissionDisposition::Published,
            ))
        })
        .map_err(|error| match error {
            PythonDatasetCatalogError::Cancelled => DatasetBuildError::Cancelled,
            other => DatasetBuildError::PythonDataset(other),
        })?;
    Ok(FeatureDatasetProductionAdmission {
        receipt,
        disposition,
    })
}

fn admission_audit_subject(
    manifest: &DatasetManifestRef,
) -> Result<String, PythonDatasetCatalogError> {
    let version = manifest.manifest_version().to_string();
    let capacity = manifest
        .dataset_id()
        .as_str()
        .len()
        .checked_add(1)
        .and_then(|value| value.checked_add(version.len()))
        .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
    let mut subject = String::new();
    subject
        .try_reserve_exact(capacity)
        .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
    subject.push_str(manifest.dataset_id().as_str());
    subject.push(':');
    subject.push_str(&version);
    Ok(subject)
}

fn validate_request_dataset(
    builder: &DatasetBuilderService<'_>,
    request: &DatasetBuildRequest,
    dataset: &FeatureLabelDataset,
) -> Result<(), DatasetBuildError> {
    if builder.service.pinned(dataset.manifest())? != dataset.pinned
        || request.output_dataset() != dataset.manifest().dataset_id()
        || request.build_spec_digest() != dataset.build_spec_digest()
        || request.policy_digest() != dataset.policy_digest()
        || request.universe_digest() != dataset.universe_digest()
        || request.inputs().universe_id() != &dataset.universe_id
        || request.policy().split() != dataset.split_policy
        || request.policy().point_in_time() != dataset.point_in_time_policy
        || request.policy().missing_values() != dataset.missing_value_policy
        || request.inputs().component_specs() != dataset.component_specs.as_ref()
        || dataset.pinned.generation_kind() != GenerationKind::Derived
        || dataset.pinned.build_spec_digest() != Some(request.build_spec_digest())
        || dataset.pinned.parents().len() != request.inputs().parents().len()
        || !dataset
            .pinned
            .parents()
            .iter()
            .zip(request.inputs().parents())
            .all(|(retained, requested)| retained.manifest() == requested)
    {
        return Err(DatasetBuildError::InvalidInputGeneration);
    }
    Ok(())
}

struct RetainedResearchDecision {
    graph_digest: Vec<u8>,
    requested_use: String,
    decided_at_ns: i64,
    expires_at_ns: Option<i64>,
    outcome: String,
    root_count: i64,
}

#[allow(
    clippy::too_many_arguments,
    reason = "final admission revalidates every independent fresh authority coordinate"
)]
fn validate_fresh_research_authorization(
    transaction: &rusqlite::Transaction<'_>,
    decision: ResearchUseDecisionDigest,
    graph: ResearchUseGraphDigest,
    research_use: ResearchUse,
    expires_at: Timestamp,
    expected_roots: &[DatasetManifestRef],
    admitted_at: Timestamp,
) -> Result<(), PythonDatasetCatalogError> {
    let retained = transaction
        .query_row(
            "SELECT graph_digest, requested_use, decided_at_ns, expires_at_ns, outcome, root_count
             FROM research_use_decisions WHERE decision_id=?1",
            params![decision.bytes()],
            |row| {
                Ok(RetainedResearchDecision {
                    graph_digest: row.get(0)?,
                    requested_use: row.get(1)?,
                    decided_at_ns: row.get(2)?,
                    expires_at_ns: row.get(3)?,
                    outcome: row.get(4)?,
                    root_count: row.get(5)?,
                })
            },
        )
        .optional()?
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    if retained.graph_digest.as_slice() != graph.bytes()
        || retained.requested_use != research_use.database_name()
        || retained.outcome != "allowed"
        || retained.expires_at_ns != Some(expires_at.unix_nanos())
        || retained.decided_at_ns > admitted_at.unix_nanos()
        || admitted_at >= expires_at
        || usize::try_from(retained.root_count).ok() != Some(expected_roots.len())
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }

    let read_limit = expected_roots
        .len()
        .checked_add(1)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
    let mut statement = transaction.prepare(
        "SELECT root.ordinal, root.dataset_id, root.manifest_version, root.schema_name,
                root.schema_version, root.schema_fingerprint, root.content_hash,
                generation.dataset_id, generation.manifest_version, generation.schema_name,
                generation.schema_version, generation.schema_fingerprint,
                generation.content_hash
         FROM research_use_decision_roots AS root
         JOIN analytical_generations AS generation
           ON generation.generation_sequence = root.generation_sequence
         WHERE root.decision_id=?1
         ORDER BY root.ordinal
         LIMIT ?2",
    )?;
    let mut rows = statement.query(params![decision.bytes(), read_limit])?;
    for (ordinal, expected) in expected_roots.iter().enumerate() {
        let row = rows
            .next()?
            .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
        let ordinal =
            i64::try_from(ordinal).map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
        let expected_version = i64::try_from(expected.manifest_version())
            .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
        let expected_schema_version = i64::from(expected.schema().version().get());
        let root_matches = row.get::<_, i64>(0)? == ordinal
            && row.get::<_, String>(1)? == expected.dataset_id().as_str()
            && row.get::<_, i64>(2)? == expected_version
            && row.get::<_, String>(3)? == expected.schema().name()
            && row.get::<_, i64>(4)? == expected_schema_version
            && row.get::<_, Vec<u8>>(5)?.as_slice() == expected.schema().fingerprint()
            && row.get::<_, Vec<u8>>(6)?.as_slice() == expected.content_hash().bytes();
        let generation_matches = row.get::<_, String>(7)? == expected.dataset_id().as_str()
            && row.get::<_, i64>(8)? == expected_version
            && row.get::<_, String>(9)? == expected.schema().name()
            && row.get::<_, i64>(10)? == expected_schema_version
            && row.get::<_, Vec<u8>>(11)?.as_slice() == expected.schema().fingerprint()
            && row.get::<_, Vec<u8>>(12)?.as_slice() == expected.content_hash().bytes();
        if !root_matches || !generation_matches {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
    }
    if rows.next()?.is_some() {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(())
}

fn validate_final_output_rights(
    transaction: &rusqlite::Transaction<'_>,
    rights_id: [u8; 32],
    authorization: &DatasetOutputAuthorization,
    payload: EvidenceDigest,
    admitted_at: Timestamp,
) -> Result<(), PythonDatasetCatalogError> {
    if payload.algorithm() != DigestAlgorithm::Sha256 || payload.bytes() == [0; 32] {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let basis = authorization.basis();
    let (basis_algorithm, basis_digest) = digest_columns(basis.digest());
    let (root_algorithm, root_digest) = basis
        .root_identity_digest()
        .map(digest_columns)
        .map_or((None, None), |(algorithm, digest)| {
            (Some(algorithm), Some(digest))
        });
    let (authorization_algorithm, authorization_digest) =
        digest_columns(authorization.authorization_evidence());
    let exact: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM source_rights
            WHERE rights_id=?1 AND source_id=?2
              AND payload_algorithm=1 AND payload_digest=?3
              AND retrieved_at_ns <= admitted_at_ns AND admitted_at_ns <= ?4
              AND basis_reference=?5 AND basis_algorithm=?6 AND basis_digest=?7
              AND authorization_algorithm=?8 AND authorization_digest=?9
              AND authorization_expires_at_ns IS ?10
              AND operation_mask=?11 AND basis_kind=?12
              AND basis_root_algorithm IS ?13 AND basis_root_digest IS ?14
              AND fingerprint_version=2
              AND (authorization_expires_at_ns IS NULL OR ?4 < authorization_expires_at_ns)
        )",
        params![
            rights_id,
            authorization.source_id().as_str(),
            payload.bytes(),
            admitted_at.unix_nanos(),
            basis.reference(),
            basis_algorithm,
            basis_digest,
            authorization_algorithm,
            authorization_digest,
            authorization
                .authorization_expires_at()
                .map(Timestamp::unix_nanos),
            i64::from(SourceOperation::Persist.mask()),
            basis.kind().database_name(),
            root_algorithm,
            root_digest,
        ],
        |row| row.get(0),
    )?;
    if !exact {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }

    let imported_exists: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM imported_user_input_rights WHERE rights_id=?1)",
        [rights_id],
        |row| row.get(0),
    )?;
    let Some(imported) = basis.imported_user_input_evidence() else {
        return if imported_exists {
            Err(PythonDatasetCatalogError::CorruptAdmission)
        } else {
            Ok(())
        };
    };
    if !imported_exists {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let (input_algorithm, input_digest) = digest_columns(imported.admitted_input_set_digest());
    let (manifest_algorithm, manifest_digest) =
        digest_columns(imported.generated_manifest_digest());
    let (local_algorithm, local_digest) = digest_columns(imported.local_admission_evidence());
    let (workspace_algorithm, workspace_digest) =
        digest_columns(imported.workspace_receipt_evidence());
    let (import_algorithm, import_digest) = digest_columns(imported.import_receipt_evidence());
    let (binding_algorithm, binding_digest) = digest_columns(imported.binding_digest());
    let imported_exact: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM imported_user_input_rights
            WHERE rights_id=?1 AND binding_schema_version=1
              AND admitted_input_set_algorithm=?2 AND admitted_input_set_digest=?3
              AND generated_manifest_algorithm=?4 AND generated_manifest_digest=?5
              AND local_admission_algorithm=?6 AND local_admission_digest=?7
              AND workspace_receipt_algorithm=?8 AND workspace_receipt_digest=?9
              AND import_receipt_algorithm=?10 AND import_receipt_digest=?11
              AND binding_algorithm=?12 AND binding_digest=?13
        )",
        params![
            rights_id,
            input_algorithm,
            input_digest,
            manifest_algorithm,
            manifest_digest,
            local_algorithm,
            local_digest,
            workspace_algorithm,
            workspace_digest,
            import_algorithm,
            import_digest,
            binding_algorithm,
            binding_digest,
        ],
        |row| row.get(0),
    )?;
    if imported_exact {
        Ok(())
    } else {
        Err(PythonDatasetCatalogError::CorruptAdmission)
    }
}

fn digest_columns(digest: EvidenceDigest) -> (i64, [u8; 32]) {
    (
        match digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        },
        digest.bytes(),
    )
}

fn has_cross_admission_unique_collision(
    transaction: &rusqlite::Transaction<'_>,
    production_identity: Sha256Digest,
    receipt_sha256: Sha256Digest,
    export_sha256: Sha256Digest,
    output_group_id: [u8; 32],
    research_decision: ResearchUseDecisionDigest,
) -> Result<bool, PythonDatasetCatalogError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM feature_dataset_production_admissions
                WHERE production_identity_sha256=?1 OR receipt_sha256=?2 OR export_sha256=?3
                   OR output_group_id=?4 OR research_decision_id=?5
            )",
            params![
                production_identity.bytes(),
                receipt_sha256.bytes(),
                export_sha256.bytes(),
                output_group_id,
                research_decision.bytes(),
            ],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

struct RetainedGenerationAuthority {
    content_hash: Vec<u8>,
    schema_name: String,
    schema_version: i64,
    schema_fingerprint: Vec<u8>,
    generation_kind: String,
    parent_count: i64,
    build_spec_digest: Option<Vec<u8>>,
    output_group_id: Vec<u8>,
    requested_use: String,
    graph_digest: Vec<u8>,
}

fn load_generation_authority(
    transaction: &rusqlite::Transaction<'_>,
    dataset_id: &str,
    manifest_version: i64,
) -> Result<Option<RetainedGenerationAuthority>, PythonDatasetCatalogError> {
    transaction
        .query_row(
            "SELECT generation.content_hash, generation.schema_name, generation.schema_version,
                    generation.schema_fingerprint, generation.generation_kind,
                    generation.parent_count, generation.build_spec_digest,
                    authorization.output_group_id, authorization.requested_use,
                    authorization.graph_digest
             FROM analytical_generations AS generation
             JOIN derived_generation_authorizations AS authorization
               ON authorization.generation_sequence = generation.generation_sequence
             WHERE generation.dataset_id=?1 AND generation.manifest_version=?2",
            params![dataset_id, manifest_version],
            |row| {
                Ok(RetainedGenerationAuthority {
                    content_hash: row.get(0)?,
                    schema_name: row.get(1)?,
                    schema_version: row.get(2)?,
                    schema_fingerprint: row.get(3)?,
                    generation_kind: row.get(4)?,
                    parent_count: row.get(5)?,
                    build_spec_digest: row.get(6)?,
                    output_group_id: row.get(7)?,
                    requested_use: row.get(8)?,
                    graph_digest: row.get(9)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn validate_generation(
    dataset: &FeatureLabelDataset,
    retained: &RetainedGenerationAuthority,
) -> Result<(), PythonDatasetCatalogError> {
    let manifest = dataset.manifest();
    let expected_parent_count = i64::try_from(dataset.pinned.parents().len())
        .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
    if retained.content_hash.as_slice() != manifest.content_hash().bytes()
        || retained.schema_name != manifest.schema().name()
        || u16::try_from(retained.schema_version).ok() != Some(manifest.schema().version().get())
        || retained.schema_fingerprint.as_slice() != manifest.schema().fingerprint()
        || retained.generation_kind != "derived"
        || retained.parent_count != expected_parent_count
        || retained.build_spec_digest.as_deref()
            != Some(dataset.build_spec_digest().digest().bytes().as_slice())
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(())
}

fn validate_objects(
    transaction: &rusqlite::Transaction<'_>,
    dataset: &FeatureLabelDataset,
    manifest_version: i64,
) -> Result<(), PythonDatasetCatalogError> {
    let dataset_id = dataset.manifest().dataset_id().as_str();
    let object_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM analytical_generation_objects
         WHERE dataset_id=?1 AND manifest_version=?2",
        params![dataset_id, manifest_version],
        |row| row.get(0),
    )?;
    if usize::try_from(object_count).ok() != Some(dataset.pinned.objects().len()) {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    for (ordinal, object) in dataset.pinned.objects().iter().enumerate() {
        let ordinal =
            i64::try_from(ordinal).map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
        let matched: bool = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM analytical_generation_objects AS object
                JOIN artifacts AS artifact ON artifact.artifact_id=object.artifact_id
                WHERE object.dataset_id=?1 AND object.manifest_version=?2
                  AND object.ordinal=?3 AND object.artifact_id=?4
                  AND object.content_hash=?5 AND object.row_count=?6
                  AND object.size_bytes=?7 AND object.lineage_hash=?8
                  AND artifact.relative_reference=?9
            )",
            params![
                dataset_id,
                manifest_version,
                ordinal,
                object.artifact_id().to_string(),
                object.object().content_hash().bytes(),
                i64::try_from(object.object().row_count())
                    .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?,
                i64::try_from(object.object().size_bytes())
                    .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?,
                object.object().lineage_digest().bytes(),
                object.relative_reference(),
            ],
            |row| row.get(0),
        )?;
        if !matched {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
    }
    Ok(())
}

struct RetainedAdmission {
    production_identity: Vec<u8>,
    receipt_schema: String,
    receipt_sha256: Vec<u8>,
    receipt_json: Vec<u8>,
    product_contract: String,
    export_sha256: Vec<u8>,
    catalog_identity: Vec<u8>,
    descriptor: Vec<u8>,
    selection_digest_version: i64,
    output_group_id: Vec<u8>,
    final_output_rights_id: Vec<u8>,
    research_decision: Vec<u8>,
    research_graph: Vec<u8>,
    research_use: String,
    research_use_expires_at_ns: i64,
    admitted_at_ns: i64,
}

fn load_retained_admission(
    transaction: &rusqlite::Transaction<'_>,
    dataset_id: &str,
    manifest_version: i64,
) -> Result<Option<RetainedAdmission>, PythonDatasetCatalogError> {
    transaction
        .query_row(
            "SELECT production_identity_sha256, receipt_schema, receipt_sha256, receipt_json,
                    product_contract, export_sha256, catalog_identity, descriptor_json,
                    selection_digest_version,
                    output_group_id, final_output_rights_id, research_decision_id,
                    research_graph_digest, research_use, research_use_expires_at_ns,
                    admitted_at_ns
             FROM feature_dataset_production_admissions
             WHERE dataset_id=?1 AND manifest_version=?2",
            params![dataset_id, manifest_version],
            |row| {
                Ok(RetainedAdmission {
                    production_identity: row.get(0)?,
                    receipt_schema: row.get(1)?,
                    receipt_sha256: row.get(2)?,
                    receipt_json: row.get(3)?,
                    product_contract: row.get(4)?,
                    export_sha256: row.get(5)?,
                    catalog_identity: row.get(6)?,
                    descriptor: row.get(7)?,
                    selection_digest_version: row.get(8)?,
                    output_group_id: row.get(9)?,
                    final_output_rights_id: row.get(10)?,
                    research_decision: row.get(11)?,
                    research_graph: row.get(12)?,
                    research_use: row.get(13)?,
                    research_use_expires_at_ns: row.get(14)?,
                    admitted_at_ns: row.get(15)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn validate_retained_admission(
    retained: RetainedAdmission,
    stable: &FeatureDatasetProductionReceiptExpectation<'_>,
    production_identity: Sha256Digest,
    producer_evidence: &FeatureDatasetProductionEvidenceV1,
    export_bytes: &[u8],
) -> Result<FeatureDatasetProductionReceiptV1, PythonDatasetCatalogError> {
    if retained.production_identity.as_slice() != production_identity.bytes() {
        return Err(PythonDatasetCatalogError::ConflictingProductionAdmission);
    }
    if retained.receipt_schema != FEATURE_DATASET_PRODUCTION_RECEIPT_SCHEMA
        || FeatureDatasetProductContract::from_identity(&retained.product_contract)
            != Some(stable.product_contract)
        || retained.export_sha256.as_slice() != stable.export_sha256.bytes()
        || retained.catalog_identity.as_slice() != stable.catalog_identity.bytes()
        || retained.descriptor.as_slice() != export_bytes
        || retained.selection_digest_version != 2
        || retained.output_group_id.as_slice() != stable.output_group_id
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let retained_expectation = FeatureDatasetProductionReceiptExpectation {
        production_identity,
        receipt_sha256: Sha256Digest::new(array_32(&retained.receipt_sha256)?),
        catalog_identity: stable.catalog_identity,
        product_contract: stable.product_contract,
        manifest: stable.manifest,
        build_spec_digest: stable.build_spec_digest,
        policy_digest: stable.policy_digest,
        universe_digest: stable.universe_digest,
        universe_id: stable.universe_id,
        output_group_id: stable.output_group_id,
        final_output_rights_id: array_32(&retained.final_output_rights_id)?,
        export_sha256: stable.export_sha256,
        research_decision: ResearchUseDecisionDigest::try_from_bytes(array_32(
            &retained.research_decision,
        )?)
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
        research_graph: ResearchUseGraphDigest::try_from_bytes(array_32(&retained.research_graph)?)
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
        research_use: parse_research_use(&retained.research_use)?,
        research_use_expires_at: Timestamp::from_unix_nanos(retained.research_use_expires_at_ns),
        admitted_at: Timestamp::from_unix_nanos(retained.admitted_at_ns),
    };
    let receipt = FeatureDatasetProductionReceiptV1::decode_and_validate(
        &retained.receipt_json,
        &retained_expectation,
    )?;
    if &receipt.producer_evidence != producer_evidence {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(receipt)
}

fn encode_receipt(
    stable: &FeatureDatasetProductionReceiptExpectation<'_>,
    production_identity: Sha256Digest,
    producer_evidence: &FeatureDatasetProductionEvidenceV1,
) -> Result<FeatureDatasetProductionReceiptV1, PythonDatasetCatalogError> {
    let wire = ReceiptWire {
        schema: fallible_owned(
            FEATURE_DATASET_PRODUCTION_RECEIPT_SCHEMA,
            MAX_RECEIPT_SCHEMA_BYTES,
        )?,
        product_contract: fallible_owned(
            stable.product_contract.identity(),
            MAX_PRODUCT_CONTRACT_BYTES,
        )?,
        production_identity_sha256: hex(production_identity.bytes())?,
        catalog_identity_sha256: hex(stable.catalog_identity.bytes())?,
        dataset: DatasetWire {
            dataset_id: fallible_owned(
                stable.manifest.dataset_id().as_str(),
                MAX_DATASET_ID_BYTES,
            )?,
            manifest_version: stable.manifest.manifest_version(),
            manifest_sha256: hex(stable.manifest.content_hash().bytes())?,
            schema_name: fallible_owned(
                stable.manifest.schema().name(),
                MAX_DATASET_SCHEMA_NAME_BYTES,
            )?,
            schema_version: stable.manifest.schema().version().get(),
            schema_sha256: hex(stable.manifest.schema().fingerprint())?,
            build_spec_sha256: hex(stable.build_spec_digest.digest().bytes())?,
            policy_sha256: hex(stable.policy_digest.bytes())?,
            universe_id: fallible_owned(stable.universe_id, crate::UniverseId::MAX_LENGTH)?,
            universe_sha256: hex(stable.universe_digest.bytes())?,
        },
        output_group_sha256: hex(stable.output_group_id)?,
        output_authorization: OutputAuthorizationWire {
            final_output_rights_sha256: hex(stable.final_output_rights_id)?,
        },
        python_export_sha256: hex(stable.export_sha256.bytes())?,
        producer_evidence: producer_evidence_wire(producer_evidence)?,
        research_authorization: ResearchAuthorizationWire {
            decision_sha256: hex(stable.research_decision.bytes())?,
            graph_sha256: hex(stable.research_graph.bytes())?,
            research_use: fallible_owned(
                stable.research_use.database_name(),
                MAX_RESEARCH_USE_NAME_BYTES,
            )?,
            expires_at_unix_nanos: stable.research_use_expires_at.unix_nanos(),
        },
        admitted_at_unix_nanos: stable.admitted_at.unix_nanos(),
    };
    let bytes = canonical_receipt_bytes(&wire)
        .map_err(|_| PythonDatasetCatalogError::ProductionReceiptEncoding)?;
    let receipt_sha256 = Sha256Digest::new(Sha256::digest(&bytes).into());
    let expectation = FeatureDatasetProductionReceiptExpectation {
        production_identity,
        receipt_sha256,
        catalog_identity: stable.catalog_identity,
        product_contract: stable.product_contract,
        manifest: stable.manifest,
        build_spec_digest: stable.build_spec_digest,
        policy_digest: stable.policy_digest,
        universe_digest: stable.universe_digest,
        universe_id: stable.universe_id,
        output_group_id: stable.output_group_id,
        final_output_rights_id: stable.final_output_rights_id,
        export_sha256: stable.export_sha256,
        research_decision: stable.research_decision,
        research_graph: stable.research_graph,
        research_use: stable.research_use,
        research_use_expires_at: stable.research_use_expires_at,
        admitted_at: stable.admitted_at,
    };
    FeatureDatasetProductionReceiptV1::decode_and_validate(&bytes, &expectation)
}

fn production_identity(
    expectation: &FeatureDatasetProductionReceiptExpectation<'_>,
    producer_evidence: &FeatureDatasetProductionEvidenceV1,
) -> Sha256Digest {
    // This identity covers only stable product semantics. The exact request is transitively bound
    // by build_spec_digest and the immutable output group. Fresh ResearchUse and output-rights
    // decisions are admission-time evidence retained in the canonical receipt, but their decision
    // timestamps may change on an otherwise exact replay and therefore cannot define a new product.
    // The producer evidence, including its immutable attestation window, is stable product
    // semantics and deliberately remains part of this identity.
    let mut hash = Sha256::new();
    hash.update(PRODUCTION_IDENTITY_DOMAIN);
    hash.update(expectation.catalog_identity.bytes());
    put_str(&mut hash, expectation.product_contract.identity());
    put_str(&mut hash, expectation.manifest.dataset_id().as_str());
    hash.update(expectation.manifest.manifest_version().to_be_bytes());
    hash.update(expectation.manifest.content_hash().bytes());
    put_str(&mut hash, expectation.manifest.schema().name());
    hash.update(expectation.manifest.schema().version().get().to_be_bytes());
    hash.update(expectation.manifest.schema().fingerprint());
    hash.update(expectation.build_spec_digest.digest().bytes());
    hash.update(expectation.policy_digest.bytes());
    put_str(&mut hash, expectation.universe_id);
    hash.update(expectation.universe_digest.bytes());
    hash.update(expectation.output_group_id);
    hash.update(expectation.export_sha256.bytes());
    hash.update(producer_evidence.identity.bytes());
    Sha256Digest::new(hash.finalize().into())
}

fn production_evidence_identity(
    producer: &SourceIdentifier,
    implementation_revision: &SourceIdentifier,
    attested_at: Timestamp,
    currentness_expires_at: Timestamp,
    bindings: &[FeatureDatasetProductionEvidenceBinding],
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(PRODUCTION_EVIDENCE_DOMAIN);
    put_str(&mut hash, producer.as_str());
    put_str(&mut hash, implementation_revision.as_str());
    hash.update(attested_at.unix_nanos().to_be_bytes());
    hash.update(currentness_expires_at.unix_nanos().to_be_bytes());
    put_len(&mut hash, bindings.len());
    for binding in bindings {
        put_str(&mut hash, binding.kind.as_str());
        hash.update(binding.schema_version.get().to_be_bytes());
        hash.update([digest_algorithm_tag(binding.evidence.algorithm())]);
        hash.update(binding.evidence.bytes());
    }
    Sha256Digest::new(hash.finalize().into())
}

fn compare_evidence_bindings(
    left: &FeatureDatasetProductionEvidenceBinding,
    right: &FeatureDatasetProductionEvidenceBinding,
) -> Ordering {
    left.kind
        .as_str()
        .cmp(right.kind.as_str())
        .then_with(|| left.schema_version.cmp(&right.schema_version))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReceiptWire {
    schema: String,
    product_contract: String,
    production_identity_sha256: String,
    catalog_identity_sha256: String,
    dataset: DatasetWire,
    output_group_sha256: String,
    output_authorization: OutputAuthorizationWire,
    python_export_sha256: String,
    producer_evidence: ProducerEvidenceWire,
    research_authorization: ResearchAuthorizationWire,
    admitted_at_unix_nanos: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutputAuthorizationWire {
    final_output_rights_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetWire {
    dataset_id: String,
    manifest_version: u64,
    manifest_sha256: String,
    schema_name: String,
    schema_version: u16,
    schema_sha256: String,
    build_spec_sha256: String,
    policy_sha256: String,
    universe_id: String,
    universe_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProducerEvidenceWire {
    schema: String,
    attestation: String,
    producer: String,
    implementation_revision: String,
    attested_at_unix_nanos: i64,
    currentness_expires_at_unix_nanos: i64,
    identity_sha256: String,
    bindings: Vec<EvidenceBindingWire>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EvidenceBindingWire {
    kind: String,
    schema_version: u32,
    algorithm: String,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResearchAuthorizationWire {
    decision_sha256: String,
    graph_sha256: String,
    research_use: String,
    expires_at_unix_nanos: i64,
}

fn producer_evidence_wire(
    evidence: &FeatureDatasetProductionEvidenceV1,
) -> Result<ProducerEvidenceWire, PythonDatasetCatalogError> {
    let mut bindings = Vec::new();
    bindings
        .try_reserve_exact(evidence.bindings.len())
        .map_err(|_| PythonDatasetCatalogError::ProductionReceiptEncoding)?;
    for binding in &evidence.bindings {
        bindings.push(EvidenceBindingWire {
            kind: fallible_owned(binding.kind.as_str(), SourceIdentifier::MAX_LENGTH)?,
            schema_version: binding.schema_version.get(),
            algorithm: fallible_owned(digest_algorithm_name(binding.evidence.algorithm()), 16)?,
            digest: hex(binding.evidence.bytes())?,
        });
    }
    Ok(ProducerEvidenceWire {
        schema: fallible_owned(PRODUCTION_EVIDENCE_SCHEMA, MAX_RECEIPT_SCHEMA_BYTES)?,
        attestation: fallible_owned(APPLICATION_PRODUCER_ATTESTATION, MAX_RECEIPT_SCHEMA_BYTES)?,
        producer: fallible_owned(evidence.producer.as_str(), SourceIdentifier::MAX_LENGTH)?,
        implementation_revision: fallible_owned(
            evidence.implementation_revision.as_str(),
            SourceIdentifier::MAX_LENGTH,
        )?,
        attested_at_unix_nanos: evidence.attested_at.unix_nanos(),
        currentness_expires_at_unix_nanos: evidence.currentness_expires_at.unix_nanos(),
        identity_sha256: hex(evidence.identity.bytes())?,
        bindings,
    })
}

fn producer_evidence_from_wire(
    wire: &ProducerEvidenceWire,
) -> Result<FeatureDatasetProductionEvidenceV1, PythonDatasetCatalogError> {
    if wire.schema != PRODUCTION_EVIDENCE_SCHEMA
        || wire.attestation != APPLICATION_PRODUCER_ATTESTATION
        || wire.attested_at_unix_nanos >= wire.currentness_expires_at_unix_nanos
        || wire.bindings.is_empty()
        || wire.bindings.len() > MAX_FEATURE_DATASET_PRODUCTION_EVIDENCE_BINDINGS
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let producer = SourceIdentifier::try_from(wire.producer.as_str())
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let implementation_revision = SourceIdentifier::try_from(wire.implementation_revision.as_str())
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let mut bindings = Vec::new();
    bindings
        .try_reserve_exact(wire.bindings.len())
        .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
    for binding in &wire.bindings {
        let kind = SourceIdentifier::try_from(binding.kind.as_str())
            .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
        let schema_version = NonZeroU32::new(binding.schema_version)
            .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
        let algorithm = parse_digest_algorithm(&binding.algorithm)?;
        let evidence = EvidenceDigest::new(algorithm, parse_sha256_bytes(&binding.digest)?);
        bindings.push(
            FeatureDatasetProductionEvidenceBinding::try_new(kind, schema_version, evidence)
                .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?,
        );
    }
    let evidence = FeatureDatasetProductionEvidenceV1::try_new(
        producer,
        implementation_revision,
        Timestamp::from_unix_nanos(wire.attested_at_unix_nanos),
        Timestamp::from_unix_nanos(wire.currentness_expires_at_unix_nanos),
        bindings,
    )
    .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    if parse_sha256(&wire.identity_sha256)? != evidence.identity {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(evidence)
}

fn dataset_wire_matches(
    wire: &DatasetWire,
    expectation: &FeatureDatasetProductionReceiptExpectation<'_>,
) -> bool {
    wire.dataset_id == expectation.manifest.dataset_id().as_str()
        && wire.manifest_version == expectation.manifest.manifest_version()
        && parse_sha256(&wire.manifest_sha256).ok() == Some(expectation.manifest.content_hash())
        && wire.schema_name == expectation.manifest.schema().name()
        && wire.schema_version == expectation.manifest.schema().version().get()
        && parse_sha256_bytes(&wire.schema_sha256).ok()
            == Some(expectation.manifest.schema().fingerprint())
        && parse_sha256(&wire.build_spec_sha256).ok()
            == Some(expectation.build_spec_digest.digest())
        && parse_sha256(&wire.policy_sha256).ok() == Some(expectation.policy_digest)
        && wire.universe_id == expectation.universe_id
        && parse_sha256(&wire.universe_sha256).ok() == Some(expectation.universe_digest)
}

fn parse_research_use(value: &str) -> Result<ResearchUse, PythonDatasetCatalogError> {
    match value {
        "local_analysis" => Ok(ResearchUse::LocalAnalysis),
        "train" => Ok(ResearchUse::Train),
        _ => Err(PythonDatasetCatalogError::CorruptAdmission),
    }
}

fn parse_research_decision(
    value: &str,
) -> Result<ResearchUseDecisionDigest, PythonDatasetCatalogError> {
    ResearchUseDecisionDigest::try_from_bytes(parse_sha256_bytes(value)?)
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)
}

fn parse_research_graph(value: &str) -> Result<ResearchUseGraphDigest, PythonDatasetCatalogError> {
    ResearchUseGraphDigest::try_from_bytes(parse_sha256_bytes(value)?)
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)
}

fn digest_algorithm_name(algorithm: DigestAlgorithm) -> &'static str {
    match algorithm {
        DigestAlgorithm::Sha256 => "sha256",
        DigestAlgorithm::Blake3 => "blake3",
    }
}

fn digest_algorithm_tag(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

fn parse_digest_algorithm(value: &str) -> Result<DigestAlgorithm, PythonDatasetCatalogError> {
    match value {
        "sha256" => Ok(DigestAlgorithm::Sha256),
        "blake3" => Ok(DigestAlgorithm::Blake3),
        _ => Err(PythonDatasetCatalogError::CorruptAdmission),
    }
}

fn parse_sha256(value: &str) -> Result<Sha256Digest, PythonDatasetCatalogError> {
    Ok(Sha256Digest::new(parse_sha256_bytes(value)?))
}

fn parse_sha256_bytes(value: &str) -> Result<[u8; 32], PythonDatasetCatalogError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    if bytes == [0; 32] {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(bytes)
}

fn array_32(value: &[u8]) -> Result<[u8; 32], PythonDatasetCatalogError> {
    let value: [u8; 32] = value
        .try_into()
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    if value == [0; 32] {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(value)
}

fn hex_nibble(value: u8) -> Result<u8, PythonDatasetCatalogError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(PythonDatasetCatalogError::CorruptAdmission),
    }
}

fn hex(bytes: [u8; 32]) -> Result<String, PythonDatasetCatalogError> {
    let mut encoded = String::new();
    encoded
        .try_reserve_exact(64)
        .map_err(|_| PythonDatasetCatalogError::ProductionReceiptEncoding)?;
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| PythonDatasetCatalogError::ProductionReceiptEncoding)?;
    }
    Ok(encoded)
}

fn fallible_owned(value: &str, maximum_bytes: usize) -> Result<String, PythonDatasetCatalogError> {
    if value.is_empty() || value.len() > maximum_bytes {
        return Err(PythonDatasetCatalogError::ProductionReceiptEncoding);
    }
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| PythonDatasetCatalogError::ProductionReceiptEncoding)?;
    owned.push_str(value);
    Ok(owned)
}

fn canonical_receipt_bytes(wire: &ReceiptWire) -> Result<Vec<u8>, ()> {
    let mut writer = BoundedReceiptWriter::default();
    serde_json::to_writer(&mut writer, wire).map_err(|_| ())?;
    let bytes = writer.into_inner();
    if bytes.is_empty() || bytes.len() > MAX_FEATURE_DATASET_PRODUCTION_RECEIPT_BYTES {
        return Err(());
    }
    Ok(bytes)
}

#[derive(Default)]
struct BoundedReceiptWriter {
    bytes: Vec<u8>,
}

impl BoundedReceiptWriter {
    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedReceiptWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("production receipt length overflow"))?;
        if next_len > MAX_FEATURE_DATASET_PRODUCTION_RECEIPT_BYTES {
            return Err(io::Error::other("production receipt exceeds bound"));
        }
        self.bytes
            .try_reserve(buffer.len())
            .map_err(|_| io::Error::other("production receipt allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn put_str(hash: &mut Sha256, value: &str) {
    put_len(hash, value.len());
    hash.update(value.as_bytes());
}

fn put_len(hash: &mut Sha256, value: usize) {
    hash.update((value as u64).to_be_bytes());
}
