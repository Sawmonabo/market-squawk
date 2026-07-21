//! Versioned, unambiguous SHA-256 framing for research-use authority evidence.

use std::time::Duration;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest as _, Sha256};

use super::decision::{
    ResearchUseAuthorityEvidence, ResearchUseDecisionInput, ResearchUseDecisionOutcome,
};
use super::graph::{ResearchUseGeneration, ResearchUseGraph, ResearchUseSourceInput};
use super::model::{
    ResearchUseDecisionDigest, ResearchUseError, ResearchUseGraphDigest, ResearchUseLimits,
};
use super::publication::{DerivedPublicationDigest, DerivedPublicationInput};
use crate::{DatasetManifestRef, DatasetSchemaRef, GenerationKind, GenerationParentRelation};

const GRAPH_DOMAIN: &[u8] = b"market-squawk/research-use-graph/v1";
const DECISION_DOMAIN: &[u8] = b"market-squawk/research-use-decision/v1";
const PUBLICATION_DOMAIN: &[u8] = b"market-squawk/derived-publication/v1";

pub(super) fn graph_digest(
    graph: &ResearchUseGraph,
) -> Result<ResearchUseGraphDigest, ResearchUseError> {
    let mut encoder = CanonicalHasher::new(GRAPH_DOMAIN)?;
    encoder.usize(graph.retained_bytes())?;
    encoder.len(graph.roots().len())?;
    for root in graph.roots() {
        encoder.manifest(root)?;
    }
    encoder.len(graph.nodes().len())?;
    for node in graph.nodes() {
        encoder.generation(node)?;
    }
    encoder.len(graph.edges().len())?;
    for edge in graph.edges() {
        encoder.u64(edge.child_sequence());
        encoder.u64(edge.parent_sequence());
        encoder.u8(relation_tag(edge.relation()));
    }
    encoder.len(graph.sources().len())?;
    for source in graph.sources() {
        encoder.source(source)?;
    }
    Ok(ResearchUseGraphDigest::from_canonical(encoder.finish()))
}

pub(super) fn decision_digest(
    decision: &ResearchUseDecisionInput,
) -> Result<ResearchUseDecisionDigest, ResearchUseError> {
    let mut encoder = CanonicalHasher::new(DECISION_DOMAIN)?;
    encoder.fixed(&decision.graph_digest.bytes());
    encoder.u8(decision.requested_use.tag());
    encoder.u32(decision.policy_version);
    encoder.timestamp(decision.evaluated_at);
    encoder.optional_timestamp(decision.expires_at);
    match decision.outcome {
        ResearchUseDecisionOutcome::Allowed => encoder.u8(1),
        ResearchUseDecisionOutcome::Denied(reason) => {
            encoder.u8(2);
            encoder.u8(reason.tag());
        }
    }
    encoder.limits(decision.limits)?;
    encoder.len(decision.authorities.len())?;
    for authority in &decision.authorities {
        encoder.authority(authority)?;
    }
    Ok(ResearchUseDecisionDigest::from_canonical(encoder.finish()))
}

pub(super) fn publication_digest(
    publication: &DerivedPublicationInput,
) -> Result<DerivedPublicationDigest, ResearchUseError> {
    let mut encoder = CanonicalHasher::new(PUBLICATION_DOMAIN)?;
    encoder.fixed(&publication.decision_digest().bytes());
    encoder.fixed(&publication.graph_digest().bytes());
    encoder.u8(publication.requested_use().tag());
    encoder.len(publication.parents.len())?;
    for parent in &publication.parents {
        encoder.manifest(parent)?;
    }
    encoder.fixed(&publication.build_spec_digest.digest().bytes());
    encoder.schema(&publication.schema)?;
    encoder.string(publication.plan.dataset_id().as_str())?;
    encoder.fixed(&publication.plan.content_hash().bytes());
    encoder.fixed(&publication.plan.lineage_digest().bytes());
    encoder.u64(publication.plan.row_count());
    encoder.u64(publication.plan.total_bytes());
    encoder.len(publication.objects.len())?;
    for object in &publication.objects {
        encoder.fixed(object.run_id.as_bytes());
        encoder.fixed(&object.reservation_digest);
        encoder.u8(object.operation.tag());
        encoder.fixed(&object.rights_id);
        encoder.fixed(object.artifact_id.as_bytes());
        encoder.fixed(&object.content_hash.bytes());
        encoder.u64(object.row_count);
        encoder.u64(object.size_bytes);
        encoder.fixed(&object.lineage_digest.bytes());
    }
    encoder.fixed(publication.anchor_artifact_id.as_bytes());
    Ok(DerivedPublicationDigest::from_canonical(encoder.finish()))
}

struct CanonicalHasher(Sha256);

impl CanonicalHasher {
    fn new(domain: &[u8]) -> Result<Self, ResearchUseError> {
        let mut value = Self(Sha256::new());
        value.bytes(domain)?;
        Ok(value)
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }

    fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u16(&mut self, value: u16) {
        self.0.update(value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.update(value.to_be_bytes());
    }

    fn usize(&mut self, value: usize) -> Result<(), ResearchUseError> {
        self.u64(u64::try_from(value).map_err(|_| ResearchUseError::CanonicalEncodingOverflow)?);
        Ok(())
    }

    fn len(&mut self, value: usize) -> Result<(), ResearchUseError> {
        self.usize(value)
    }

    fn fixed(&mut self, value: &[u8]) {
        self.0.update(value);
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), ResearchUseError> {
        self.len(value.len())?;
        self.fixed(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), ResearchUseError> {
        self.bytes(value.as_bytes())
    }

    fn timestamp(&mut self, value: Timestamp) {
        self.i64(value.unix_nanos());
    }

    fn optional_timestamp(&mut self, value: Option<Timestamp>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.timestamp(value);
            }
            None => self.u8(0),
        }
    }

    fn duration(&mut self, value: Duration) -> Result<(), ResearchUseError> {
        let nanos = u64::try_from(value.as_nanos())
            .map_err(|_| ResearchUseError::CanonicalEncodingOverflow)?;
        self.u64(nanos);
        Ok(())
    }

    fn manifest(&mut self, value: &DatasetManifestRef) -> Result<(), ResearchUseError> {
        self.string(value.dataset_id().as_str())?;
        self.u64(value.manifest_version());
        self.schema(value.schema())?;
        self.fixed(&value.content_hash().bytes());
        Ok(())
    }

    fn schema(&mut self, value: &DatasetSchemaRef) -> Result<(), ResearchUseError> {
        self.string(value.name())?;
        self.u16(value.version().get());
        self.fixed(&value.fingerprint());
        Ok(())
    }

    fn generation(&mut self, value: &ResearchUseGeneration) -> Result<(), ResearchUseError> {
        self.u64(value.sequence());
        self.manifest(value.manifest())?;
        self.u8(generation_kind_tag(value.kind()));
        match value.build_spec_digest() {
            Some(value) => {
                self.u8(1);
                self.fixed(&value.digest().bytes());
            }
            None => self.u8(0),
        }
        self.usize(value.parent_count())?;
        Ok(())
    }

    fn source(&mut self, value: &ResearchUseSourceInput) -> Result<(), ResearchUseError> {
        self.u64(value.generation_sequence());
        self.fixed(value.ingest_run_id().as_bytes());
        self.string(value.source_id().as_str())?;
        self.fixed(&value.rights_id());
        Ok(())
    }

    fn evidence(&mut self, value: EvidenceDigest) {
        self.u8(match value.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        });
        self.fixed(&value.bytes());
    }

    fn authority(&mut self, value: &ResearchUseAuthorityEvidence) -> Result<(), ResearchUseError> {
        self.source(&value.source)?;
        self.fixed(&value.rights_fingerprint);
        self.fixed(&value.rights_basis_digest);
        self.evidence(value.authorization_evidence);
        self.optional_timestamp(value.rights_expires_at);
        self.fixed(&value.research_grant_id);
        self.evidence(value.grant_evidence);
        self.optional_timestamp(value.grant_expires_at);
        self.u64(value.revocation_frontier);
        Ok(())
    }

    fn limits(&mut self, value: ResearchUseLimits) -> Result<(), ResearchUseError> {
        self.usize(value.max_roots())?;
        self.usize(value.max_nodes())?;
        self.usize(value.max_edges())?;
        self.usize(value.max_sources())?;
        self.usize(value.max_retained_bytes())?;
        self.duration(value.traversal_deadline())?;
        self.duration(value.permit_lifetime())
    }
}

fn generation_kind_tag(value: GenerationKind) -> u8 {
    match value {
        GenerationKind::Ingest => 1,
        GenerationKind::Compaction => 2,
        GenerationKind::Derived => 3,
    }
}

fn relation_tag(value: GenerationParentRelation) -> u8 {
    match value {
        GenerationParentRelation::AppendPredecessor => 1,
        GenerationParentRelation::CompactionPredecessor => 2,
        GenerationParentRelation::DerivedInput => 3,
    }
}
