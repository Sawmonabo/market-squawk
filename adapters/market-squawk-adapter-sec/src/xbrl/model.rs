//! Public bounded XBRL extraction result types.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use chrono::NaiveDate;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision, SourceId,
    SourceIdentifier, Timestamp, XbrlFactEvidence, XbrlOccurrenceRelationships, XbrlQualifiedName,
    XbrlTaxonomySet, XbrlText,
};
use market_squawk_sources::{
    FASB_XBRL_TAXONOMY_AUTHORITY, FilingTaxonomyLocator, FilingTaxonomySourceAuthority,
    ProviderCaptureMaterial, SEC_EDGAR_AUTHORITY, W3C_XML_SCHEMA_STANDARDS_AUTHORITY,
    XBRL_INTERNATIONAL_STANDARDS_AUTHORITY, XBRL_US_LEGACY_TAXONOMY_AUTHORITY,
    resolve_filing_taxonomy_authority,
};
use quick_xml::NsReader;
use quick_xml::events::Event;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;

use super::SecXbrlError;
use super::wire::{ResolvedAttributes, attributes, is_element, resolve_element_name};
use crate::{RawEvidenceStore, RetrievedSecBytes, SecParserLimits};

const TAXONOMY_REGISTRY_RULESET: &str = "sec-xbrl-taxonomy-registry-v1";
const TAXONOMY_LOCATOR_MAPPING_RULESET: &str = "sec-xbrl-logical-http-to-https-v1";
const TAXONOMY_CATALOG_RELEASE: &str = "sec-xbrl-taxonomy-catalog-2026-08-30";
pub(crate) const MAX_TAXONOMY_ARTIFACTS: usize = 64;
pub(crate) const MAX_TAXONOMY_REFERENCES: usize = 256;
pub(crate) const MAX_TAXONOMY_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_TAXONOMY_SET_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_TAXONOMY_GRAPH_SCAN_BYTES: u64 = 128 * 1024 * 1024;
const XML_SCHEMA_NAMESPACE: &str = "http://www.w3.org/2001/XMLSchema";
const XBRL_LINK_NAMESPACE: &str = "http://www.xbrl.org/2003/linkbase";
const XLINK_NAMESPACE: &str = "http://www.w3.org/1999/xlink";
const XML_NAMESPACE: &str = "http://www.w3.org/XML/1998/namespace";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum SecXbrlTaxonomyArtifactKind {
    Schema,
    Linkbase,
}

impl SecXbrlTaxonomyArtifactKind {
    const fn ordinal(self) -> u8 {
        match self {
            Self::Schema => 1,
            Self::Linkbase => 2,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Linkbase => "linkbase",
        }
    }
}

/// The independently governed origin of one captured taxonomy artifact.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum SecXbrlTaxonomyOrigin {
    SecFiling,
    SecTaxonomy,
    XbrlUsLegacyTaxonomy,
    FasbTaxonomy,
    XbrlStandard,
    W3cStandard,
}

impl SecXbrlTaxonomyOrigin {
    const fn ordinal(self) -> u8 {
        match self {
            Self::SecFiling => 1,
            Self::SecTaxonomy => 2,
            Self::XbrlUsLegacyTaxonomy => 3,
            Self::FasbTaxonomy => 4,
            Self::XbrlStandard => 5,
            Self::W3cStandard => 6,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::SecFiling => "sec_filing",
            Self::SecTaxonomy => "sec_taxonomy",
            Self::XbrlUsLegacyTaxonomy => "xbrl_us_legacy_taxonomy",
            Self::FasbTaxonomy => "fasb_taxonomy",
            Self::XbrlStandard => "xbrl_standard",
            Self::W3cStandard => "w3c_standard",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SecXbrlTaxonomyReferenceRole {
    FilingSchema,
    SchemaImport,
    SchemaInclude,
    SchemaRedefine,
    CalculationLinkbase,
    DefinitionLinkbase,
    LabelLinkbase,
    PresentationLinkbase,
    ReferenceLinkbase,
    RoleDefinition,
    ArcroleDefinition,
}

impl SecXbrlTaxonomyReferenceRole {
    const fn target_kind(self) -> SecXbrlTaxonomyArtifactKind {
        match self {
            Self::FilingSchema
            | Self::SchemaImport
            | Self::SchemaInclude
            | Self::SchemaRedefine
            | Self::RoleDefinition
            | Self::ArcroleDefinition => SecXbrlTaxonomyArtifactKind::Schema,
            Self::CalculationLinkbase
            | Self::DefinitionLinkbase
            | Self::LabelLinkbase
            | Self::PresentationLinkbase
            | Self::ReferenceLinkbase => SecXbrlTaxonomyArtifactKind::Linkbase,
        }
    }

    const fn ordinal(self) -> u8 {
        match self {
            Self::FilingSchema => 1,
            Self::SchemaImport => 2,
            Self::SchemaInclude => 3,
            Self::SchemaRedefine => 4,
            Self::CalculationLinkbase => 5,
            Self::DefinitionLinkbase => 6,
            Self::LabelLinkbase => 7,
            Self::PresentationLinkbase => 8,
            Self::ReferenceLinkbase => 9,
            Self::RoleDefinition => 10,
            Self::ArcroleDefinition => 11,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FilingSchema => "filing_schema",
            Self::SchemaImport => "schema_import",
            Self::SchemaInclude => "schema_include",
            Self::SchemaRedefine => "schema_redefine",
            Self::CalculationLinkbase => "calculation_linkbase",
            Self::DefinitionLinkbase => "definition_linkbase",
            Self::LabelLinkbase => "label_linkbase",
            Self::PresentationLinkbase => "presentation_linkbase",
            Self::ReferenceLinkbase => "reference_linkbase",
            Self::RoleDefinition => "role_definition",
            Self::ArcroleDefinition => "arcrole_definition",
        }
    }
}

/// One exact dependency edge, including the source-authored logical locator and the physical HTTPS
/// locator selected by the code-owned transport mapping.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SecXbrlTaxonomyReference {
    parent_logical_locator: SourceIdentifier,
    target_logical_locator: SourceIdentifier,
    target_physical_locator: SourceIdentifier,
    fragment: Option<SourceIdentifier>,
    declared_namespace: Option<SourceIdentifier>,
    referenced_uri: Option<SourceIdentifier>,
    role: SecXbrlTaxonomyReferenceRole,
    origin: SecXbrlTaxonomyOrigin,
}

impl SecXbrlTaxonomyReference {
    pub(crate) const fn parent_logical_locator(&self) -> &SourceIdentifier {
        &self.parent_logical_locator
    }

    pub(crate) const fn target_logical_locator(&self) -> &SourceIdentifier {
        &self.target_logical_locator
    }

    pub(crate) const fn target_physical_locator(&self) -> &SourceIdentifier {
        &self.target_physical_locator
    }

    pub(crate) const fn fragment(&self) -> Option<&SourceIdentifier> {
        self.fragment.as_ref()
    }

    pub(crate) const fn declared_namespace(&self) -> Option<&SourceIdentifier> {
        self.declared_namespace.as_ref()
    }

    pub(crate) const fn referenced_uri(&self) -> Option<&SourceIdentifier> {
        self.referenced_uri.as_ref()
    }

    pub(crate) const fn role(&self) -> &'static str {
        self.role.as_str()
    }

    pub(crate) const fn origin(&self) -> SecXbrlTaxonomyOrigin {
        self.origin
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecXbrlTaxonomyArtifactRequest {
    logical_locator: SourceIdentifier,
    physical_locator: SourceIdentifier,
    kind: SecXbrlTaxonomyArtifactKind,
    pinned_release: SourceIdentifier,
    origin: SecXbrlTaxonomyOrigin,
}

impl SecXbrlTaxonomyArtifactRequest {
    pub(crate) const fn logical_locator(&self) -> &SourceIdentifier {
        &self.logical_locator
    }

    pub(crate) const fn physical_locator(&self) -> &SourceIdentifier {
        &self.physical_locator
    }

    pub(crate) fn authority(&self) -> Result<FilingTaxonomySourceAuthority, SecXbrlError> {
        resolve_filing_taxonomy_authority(FilingTaxonomyLocator::new(
            self.logical_locator.as_str(),
            self.physical_locator.as_str(),
        ))
        .map(|resolved| resolved.authority())
        .map_err(|_| SecXbrlError::InvalidTaxonomySet)
    }

    pub(crate) fn same_physical_contract(&self, other: &Self) -> bool {
        self.physical_locator == other.physical_locator
            && self.kind == other.kind
            && self.pinned_release == other.pinned_release
            && self.origin == other.origin
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SecXbrlTaxonomyGraph {
    mapping_ruleset: SourceIdentifier,
    catalog_release: SourceIdentifier,
    evidence: EvidenceDigest,
    physical_bytes: u64,
    scanned_bytes: u64,
    references: Box<[SecXbrlTaxonomyReference]>,
}

struct BuiltTaxonomyGraph {
    retained: SecXbrlTaxonomyGraph,
    requests_by_physical: BTreeMap<String, Vec<SecXbrlTaxonomyArtifactRequest>>,
    target_namespaces: BTreeMap<String, Option<SourceIdentifier>>,
}

/// Code-owned validator for a bounded set of exact, captured official taxonomy artifacts.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SecXbrlTaxonomyRegistry {
    _private: (),
}

impl SecXbrlTaxonomyRegistry {
    /// Returns the sole code-owned taxonomy-set validation ruleset.
    pub(crate) const fn code_owned() -> Self {
        Self { _private: () }
    }

    /// Reopens exact captured artifacts and produces a non-cloneable pending admission.
    pub(crate) fn try_admit_captured(
        &self,
        raw_store: Arc<RawEvidenceStore>,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
        filing_document: &RetrievedSecBytes,
        mut artifacts: Vec<RetrievedSecBytes>,
        parser_limits: SecParserLimits,
        cancellation: &CancellationToken,
    ) -> Result<SecPendingValidatedXbrlTaxonomySet, SecXbrlError> {
        if artifacts.is_empty() || artifacts.len() > MAX_TAXONOMY_ARTIFACTS {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        artifacts.sort_unstable_by(|left, right| {
            left.locator()
                .cmp(&right.locator())
                .then_with(|| left.evidence().bytes().cmp(&right.evidence().bytes()))
        });
        if artifacts
            .windows(2)
            .any(|pair| pair[0].locator() == pair[1].locator())
        {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let physical_bytes =
            validate_taxonomy_capture_bodies(&raw_store, &artifacts, cancellation)?;
        let graph = build_taxonomy_graph(
            filing_document,
            &artifacts,
            physical_bytes,
            parser_limits,
            cancellation,
        )?;
        let source_revisions = validate_taxonomy_capture_authorities(
            source_id,
            metadata_revision,
            &artifacts,
            &graph,
        )?;
        let descriptors = taxonomy_artifact_descriptors(&artifacts, &graph, cancellation)?;
        let mut artifact_set = Sha256::new();
        hash_taxonomy_field(&mut artifact_set, b"sec-xbrl-exact-artifact-set-v1");
        hash_taxonomy_field(&mut artifact_set, TAXONOMY_REGISTRY_RULESET.as_bytes());
        hash_taxonomy_field(
            &mut artifact_set,
            TAXONOMY_LOCATOR_MAPPING_RULESET.as_bytes(),
        );
        hash_taxonomy_field(&mut artifact_set, TAXONOMY_CATALOG_RELEASE.as_bytes());
        artifact_set.update(graph.retained.evidence.bytes());
        artifact_set.update(
            u64::try_from(descriptors.len())
                .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
                .to_be_bytes(),
        );
        for artifact in &descriptors {
            hash_taxonomy_field(
                &mut artifact_set,
                artifact.physical_locator.as_str().as_bytes(),
            );
            artifact_set.update([artifact.kind.ordinal(), artifact.origin.ordinal()]);
            hash_taxonomy_field(&mut artifact_set, artifact.source_id.as_str().as_bytes());
            hash_taxonomy_field(
                &mut artifact_set,
                artifact
                    .metadata_revision
                    .as_source_identifier()
                    .as_str()
                    .as_bytes(),
            );
            hash_taxonomy_field(
                &mut artifact_set,
                artifact.pinned_release.as_str().as_bytes(),
            );
            artifact_set.update(
                u64::try_from(artifact.logical_locators.len())
                    .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
                    .to_be_bytes(),
            );
            for logical_locator in &artifact.logical_locators {
                hash_taxonomy_field(&mut artifact_set, logical_locator.as_str().as_bytes());
            }
            match &artifact.target_namespace {
                Some(namespace) => {
                    artifact_set.update([1]);
                    hash_taxonomy_field(&mut artifact_set, namespace.as_str().as_bytes());
                }
                None => artifact_set.update([0]),
            }
            artifact_set.update(artifact.evidence.bytes());
            artifact_set.update(artifact.size_bytes.to_be_bytes());
        }
        let artifact_set =
            EvidenceDigest::new(DigestAlgorithm::Sha256, artifact_set.finalize().into());
        let fingerprint = taxonomy_registry_fingerprint(artifact_set, graph.retained.evidence);
        let version = SourceIdentifier::try_from(format!(
            "sec-xbrl-taxonomy-set.{}",
            digest_prefix(fingerprint, 16)
        ))?;
        Ok(SecPendingValidatedXbrlTaxonomySet {
            validated: SecValidatedXbrlTaxonomySet {
                version,
                artifact_set,
                fingerprint,
                graph: graph.retained,
                artifacts: descriptors.into_boxed_slice(),
            },
            raw_store,
            source_id: source_id.clone(),
            metadata_revision: metadata_revision.clone(),
            source_revisions,
            filing_document: filing_document.clone(),
            artifacts,
            parser_limits,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecXbrlTaxonomyArtifact {
    physical_locator: SourceIdentifier,
    logical_locators: Box<[SourceIdentifier]>,
    kind: SecXbrlTaxonomyArtifactKind,
    pinned_release: SourceIdentifier,
    origin: SecXbrlTaxonomyOrigin,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    target_namespace: Option<SourceIdentifier>,
    evidence: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    retrieval_revision: u64,
}

impl SecXbrlTaxonomyArtifact {
    pub(crate) const fn physical_locator(&self) -> &SourceIdentifier {
        &self.physical_locator
    }

    pub(crate) fn logical_locators(&self) -> &[SourceIdentifier] {
        &self.logical_locators
    }

    pub(crate) const fn kind(&self) -> SecXbrlTaxonomyArtifactKind {
        self.kind
    }

    pub(crate) const fn pinned_release(&self) -> &SourceIdentifier {
        &self.pinned_release
    }

    pub(crate) const fn origin(&self) -> SecXbrlTaxonomyOrigin {
        self.origin
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub(crate) const fn target_namespace(&self) -> Option<&SourceIdentifier> {
        self.target_namespace.as_ref()
    }

    pub(crate) const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }

    pub(crate) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(crate) const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    pub(crate) const fn retrieval_revision(&self) -> u64 {
        self.retrieval_revision
    }
}

/// Opaque exact taxonomy-set identity minted only from captured official artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecValidatedXbrlTaxonomySet {
    version: SourceIdentifier,
    artifact_set: EvidenceDigest,
    fingerprint: EvidenceDigest,
    graph: SecXbrlTaxonomyGraph,
    artifacts: Box<[SecXbrlTaxonomyArtifact]>,
}

/// Non-cloneable captured taxonomy evidence awaiting the common physical-seal transition.
pub(crate) struct SecPendingValidatedXbrlTaxonomySet {
    validated: SecValidatedXbrlTaxonomySet,
    raw_store: Arc<RawEvidenceStore>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    source_revisions: BTreeMap<SourceId, MetadataRevision>,
    filing_document: RetrievedSecBytes,
    artifacts: Vec<RetrievedSecBytes>,
    parser_limits: SecParserLimits,
}

impl SecPendingValidatedXbrlTaxonomySet {
    pub(crate) const fn validated(&self) -> &SecValidatedXbrlTaxonomySet {
        &self.validated
    }

    pub(crate) fn revalidate(&self, cancellation: &CancellationToken) -> Result<(), SecXbrlError> {
        let physical_bytes =
            validate_taxonomy_capture_bodies(&self.raw_store, &self.artifacts, cancellation)?;
        let graph = build_taxonomy_graph(
            &self.filing_document,
            &self.artifacts,
            physical_bytes,
            self.parser_limits,
            cancellation,
        )?;
        let source_revisions = validate_taxonomy_capture_authorities(
            &self.source_id,
            &self.metadata_revision,
            &self.artifacts,
            &graph,
        )?;
        let descriptors = taxonomy_artifact_descriptors(&self.artifacts, &graph, cancellation)?;
        if source_revisions != self.source_revisions
            || descriptors.as_slice() != self.validated.artifacts.as_ref()
            || graph.retained != self.validated.graph
        {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        Ok(())
    }

    pub(crate) fn into_sealing_parts(
        self,
        cancellation: &CancellationToken,
    ) -> Result<(Self, Vec<ProviderCaptureMaterial>), SecXbrlError> {
        self.revalidate(cancellation)?;
        let mut materials = Vec::new();
        materials
            .try_reserve_exact(self.artifacts.len())
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        for artifact in &self.artifacts {
            if cancellation.is_cancelled() {
                return Err(SecXbrlError::Cancelled);
            }
            materials.push(
                artifact
                    .capture_material()
                    .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
            );
        }
        Ok((self, materials))
    }
}

impl SecValidatedXbrlTaxonomySet {
    /// Returns the exact set-derived version identity.
    pub(crate) const fn version(&self) -> &SourceIdentifier {
        &self.version
    }

    /// Returns the canonical digest of every exact captured artifact in the accepted set.
    pub(crate) const fn artifact_set(&self) -> EvidenceDigest {
        self.artifact_set
    }

    /// Returns the code-owned ruleset fingerprint of the accepted artifact set.
    pub(crate) const fn fingerprint(&self) -> EvidenceDigest {
        self.fingerprint
    }

    pub(crate) const fn mapping_ruleset(&self) -> &SourceIdentifier {
        &self.graph.mapping_ruleset
    }

    pub(crate) const fn catalog_release(&self) -> &SourceIdentifier {
        &self.graph.catalog_release
    }

    pub(crate) const fn graph_evidence(&self) -> EvidenceDigest {
        self.graph.evidence
    }

    pub(crate) const fn physical_bytes(&self) -> u64 {
        self.graph.physical_bytes
    }

    pub(crate) const fn scanned_bytes(&self) -> u64 {
        self.graph.scanned_bytes
    }

    pub(crate) fn references(&self) -> &[SecXbrlTaxonomyReference] {
        &self.graph.references
    }

    pub(crate) fn artifacts(&self) -> &[SecXbrlTaxonomyArtifact] {
        &self.artifacts
    }

    pub(crate) fn checked_dynamic_retained_bytes(&self) -> Option<usize> {
        let artifact_bytes = self.artifacts.iter().try_fold(0usize, |total, artifact| {
            let logical_bytes = artifact.logical_locators.iter().try_fold(
                artifact
                    .logical_locators
                    .len()
                    .checked_mul(std::mem::size_of::<SourceIdentifier>())?,
                |logical_total, locator| logical_total.checked_add(locator.retained_bytes()),
            )?;
            let dynamic = artifact
                .physical_locator
                .retained_bytes()
                .checked_add(logical_bytes)?
                .checked_add(artifact.pinned_release.retained_bytes())?
                .checked_add(artifact.source_id.retained_bytes())?
                .checked_add(
                    artifact
                        .metadata_revision
                        .as_source_identifier()
                        .retained_bytes(),
                )?
                .checked_add(
                    artifact
                        .target_namespace
                        .as_ref()
                        .map_or(0, SourceIdentifier::retained_bytes),
                )?;
            total.checked_add(dynamic)
        })?;
        let reference_bytes = self.graph.references.iter().try_fold(
            self.graph
                .references
                .len()
                .checked_mul(std::mem::size_of::<SecXbrlTaxonomyReference>())?,
            |total, reference| {
                let dynamic = reference
                    .parent_logical_locator
                    .retained_bytes()
                    .checked_add(reference.target_logical_locator.retained_bytes())?
                    .checked_add(reference.target_physical_locator.retained_bytes())?
                    .checked_add(
                        reference
                            .fragment
                            .as_ref()
                            .map_or(0, SourceIdentifier::retained_bytes),
                    )?
                    .checked_add(
                        reference
                            .declared_namespace
                            .as_ref()
                            .map_or(0, SourceIdentifier::retained_bytes),
                    )?
                    .checked_add(
                        reference
                            .referenced_uri
                            .as_ref()
                            .map_or(0, SourceIdentifier::retained_bytes),
                    )?;
                total.checked_add(dynamic)
            },
        )?;
        self.artifacts
            .len()
            .checked_mul(std::mem::size_of::<SecXbrlTaxonomyArtifact>())?
            .checked_add(self.version.retained_bytes())?
            .checked_add(self.graph.mapping_ruleset.retained_bytes())?
            .checked_add(self.graph.catalog_release.retained_bytes())?
            .checked_add(artifact_bytes)?
            .checked_add(reference_bytes)
    }

    pub(crate) fn domain_set(&self) -> XbrlTaxonomySet {
        XbrlTaxonomySet::declared(self.artifact_set, self.version.clone())
    }
}

fn taxonomy_registry_fingerprint(
    artifact_set: EvidenceDigest,
    graph_evidence: EvidenceDigest,
) -> EvidenceDigest {
    let mut fingerprint = Sha256::new();
    hash_taxonomy_field(&mut fingerprint, TAXONOMY_REGISTRY_RULESET.as_bytes());
    hash_taxonomy_field(
        &mut fingerprint,
        TAXONOMY_LOCATOR_MAPPING_RULESET.as_bytes(),
    );
    hash_taxonomy_field(&mut fingerprint, TAXONOMY_CATALOG_RELEASE.as_bytes());
    fingerprint.update(artifact_set.bytes());
    fingerprint.update(graph_evidence.bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, fingerprint.finalize().into())
}

fn validate_taxonomy_capture_bodies(
    raw_store: &RawEvidenceStore,
    artifacts: &[RetrievedSecBytes],
    cancellation: &CancellationToken,
) -> Result<u64, SecXbrlError> {
    let mut total_bytes = 0_u64;
    for artifact in artifacts {
        if cancellation.is_cancelled() {
            return Err(SecXbrlError::Cancelled);
        }
        let locator = artifact.locator().ok_or(SecXbrlError::InvalidTaxonomySet)?;
        validate_physical_taxonomy_locator(locator)?;
        artifact
            .capture_receipt()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        artifact
            .capture_material()
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let size_bytes =
            u64::try_from(artifact.bytes().len()).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
        if size_bytes == 0 || size_bytes > MAX_TAXONOMY_ARTIFACT_BYTES {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        total_bytes = total_bytes
            .checked_add(size_bytes)
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if total_bytes > MAX_TAXONOMY_SET_BYTES {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let reopened = raw_store
            .read_verified_bounded_cancellable(&artifact.evidence(), size_bytes, cancellation)
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
        if reopened.as_slice() != artifact.bytes().as_ref() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        artifact
            .retrieval_revision()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    }
    Ok(total_bytes)
}

fn validate_taxonomy_capture_authorities(
    root_source_id: &SourceId,
    root_metadata_revision: &MetadataRevision,
    artifacts: &[RetrievedSecBytes],
    graph: &BuiltTaxonomyGraph,
) -> Result<BTreeMap<SourceId, MetadataRevision>, SecXbrlError> {
    if root_source_id.as_str() != SEC_EDGAR_AUTHORITY.source_id() {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    let mut revisions = BTreeMap::new();
    for artifact in artifacts {
        let physical_locator = artifact.locator().ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let requests = graph
            .requests_by_physical
            .get(physical_locator)
            .filter(|requests| !requests.is_empty())
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let mut resolved_authority = None;
        for request in requests {
            let authority = resolve_filing_taxonomy_authority(FilingTaxonomyLocator::new(
                request.logical_locator.as_str(),
                request.physical_locator.as_str(),
            ))
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
            .authority();
            if !taxonomy_origin_matches_authority(request.origin, authority)
                || resolved_authority.is_some_and(|resolved| resolved != authority)
            {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            resolved_authority = Some(authority);
        }
        let authority = resolved_authority.ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let expected_source_id = authority
            .canonical_source_id()
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
        let expected_revision = if authority == SEC_EDGAR_AUTHORITY {
            root_metadata_revision.clone()
        } else {
            authority
                .metadata_revision()
                .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
        };
        let receipt = artifact
            .capture_receipt()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if receipt.source_id() != &expected_source_id
            || receipt.metadata_revision() != &expected_revision
        {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        if revisions
            .insert(expected_source_id, expected_revision.clone())
            .is_some_and(|existing| existing != expected_revision)
        {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
    }
    if revisions.is_empty() {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(revisions)
}

fn taxonomy_origin_matches_authority(
    origin: SecXbrlTaxonomyOrigin,
    authority: FilingTaxonomySourceAuthority,
) -> bool {
    match origin {
        SecXbrlTaxonomyOrigin::SecFiling | SecXbrlTaxonomyOrigin::SecTaxonomy => {
            authority == SEC_EDGAR_AUTHORITY
        }
        SecXbrlTaxonomyOrigin::XbrlUsLegacyTaxonomy => {
            authority == XBRL_US_LEGACY_TAXONOMY_AUTHORITY
        }
        SecXbrlTaxonomyOrigin::FasbTaxonomy => authority == FASB_XBRL_TAXONOMY_AUTHORITY,
        SecXbrlTaxonomyOrigin::XbrlStandard => authority == XBRL_INTERNATIONAL_STANDARDS_AUTHORITY,
        SecXbrlTaxonomyOrigin::W3cStandard => authority == W3C_XML_SCHEMA_STANDARDS_AUTHORITY,
    }
}

fn validate_physical_taxonomy_locator(locator: &str) -> Result<(), SecXbrlError> {
    let parsed = Url::parse(locator).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    let official_host = matches!(
        parsed.host_str(),
        Some(
            "www.sec.gov"
                | "xbrl.sec.gov"
                | "taxonomies.xbrl.us"
                | "xbrl.fasb.org"
                | "www.xbrl.org"
                | "www.w3.org"
        )
    );
    if parsed.scheme() != "https"
        || !official_host
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path().contains('%')
        || !(parsed.path().ends_with(".xsd") || parsed.path().ends_with(".xml"))
        || parsed.as_str() != locator
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(())
}

fn taxonomy_artifact_descriptors(
    artifacts: &[RetrievedSecBytes],
    graph: &BuiltTaxonomyGraph,
    cancellation: &CancellationToken,
) -> Result<Vec<SecXbrlTaxonomyArtifact>, SecXbrlError> {
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(artifacts.len())
        .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
    for artifact in artifacts {
        check_taxonomy_cancelled(cancellation)?;
        let physical_locator = artifact.locator().ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let requests = graph
            .requests_by_physical
            .get(physical_locator)
            .filter(|requests| !requests.is_empty())
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let first = requests.first().ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if requests.iter().any(|request| {
            request.physical_locator != first.physical_locator
                || request.kind != first.kind
                || request.pinned_release != first.pinned_release
                || request.origin != first.origin
        }) {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let mut logical_locators = Vec::new();
        logical_locators
            .try_reserve_exact(requests.len())
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        for request in requests {
            logical_locators.push(request.logical_locator.clone());
        }
        logical_locators.sort_unstable();
        logical_locators.dedup();
        if logical_locators.len() != requests.len() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        descriptors.push(SecXbrlTaxonomyArtifact {
            physical_locator: first.physical_locator.clone(),
            logical_locators: logical_locators.into_boxed_slice(),
            kind: first.kind,
            pinned_release: first.pinned_release.clone(),
            origin: first.origin,
            source_id: artifact
                .capture_receipt()
                .ok_or(SecXbrlError::InvalidTaxonomySet)?
                .source_id()
                .clone(),
            metadata_revision: artifact
                .capture_receipt()
                .ok_or(SecXbrlError::InvalidTaxonomySet)?
                .metadata_revision()
                .clone(),
            target_namespace: graph
                .target_namespaces
                .get(physical_locator)
                .cloned()
                .ok_or(SecXbrlError::InvalidTaxonomySet)?,
            evidence: artifact.evidence(),
            size_bytes: u64::try_from(artifact.bytes().len())
                .map_err(|_| SecXbrlError::InvalidTaxonomySet)?,
            first_observed_at: artifact.received_at(),
            retrieval_revision: artifact
                .retrieval_revision()
                .ok_or(SecXbrlError::InvalidTaxonomySet)?,
        });
    }
    Ok(descriptors)
}

fn build_taxonomy_graph(
    filing_document: &RetrievedSecBytes,
    artifacts: &[RetrievedSecBytes],
    physical_bytes: u64,
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<BuiltTaxonomyGraph, SecXbrlError> {
    check_taxonomy_cancelled(cancellation)?;
    let filing_locator = filing_document
        .locator()
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    validate_filing_document_locator(filing_locator)?;
    let filing_bytes = u64::try_from(filing_document.bytes().len())
        .map_err(|_| SecXbrlError::ByteLimitExceeded)?;
    if filing_bytes == 0
        || filing_document.bytes().len() > parser_limits.decoded_bytes()
        || filing_bytes > MAX_TAXONOMY_GRAPH_SCAN_BYTES
    {
        return Err(SecXbrlError::ByteLimitExceeded);
    }
    let mut artifacts_by_physical = BTreeMap::new();
    for artifact in artifacts {
        let locator = artifact.locator().ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if artifacts_by_physical.insert(locator, artifact).is_some() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
    }
    let filing_scan = scan_taxonomy_references(
        filing_document.bytes(),
        TaxonomyXmlExpectation::Filing,
        filing_locator,
        filing_locator,
        parser_limits,
        cancellation,
        None,
    )?;
    let mut queue = VecDeque::new();
    if filing_scan.references.len() > MAX_TAXONOMY_REFERENCES {
        return Err(SecXbrlError::RecordLimitExceeded);
    }
    queue
        .try_reserve(filing_scan.references.len())
        .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
    queue.extend(filing_scan.references);
    let mut references = BTreeSet::new();
    let mut requests_by_logical = BTreeMap::<String, SecXbrlTaxonomyArtifactRequest>::new();
    let mut requests_by_physical = BTreeMap::<String, Vec<SecXbrlTaxonomyArtifactRequest>>::new();
    let mut target_namespaces = BTreeMap::<String, Option<SourceIdentifier>>::new();
    let mut scanned_logical_locators = BTreeSet::new();
    let mut scanned_bytes = filing_bytes;
    while let Some(reference) = queue.pop_front() {
        check_taxonomy_cancelled(cancellation)?;
        if !references.insert(reference.clone()) {
            continue;
        }
        if references.len() > MAX_TAXONOMY_REFERENCES || references.len() > parser_limits.records()
        {
            return Err(SecXbrlError::RecordLimitExceeded);
        }
        let request = taxonomy_artifact_request(filing_locator, &reference)?;
        let logical_key = request.logical_locator.as_str().to_owned();
        if let Some(existing) = requests_by_logical.get(&logical_key) {
            if existing != &request {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
        } else {
            requests_by_logical.insert(logical_key.clone(), request.clone());
            let physical_key = request.physical_locator.as_str().to_owned();
            let requests = requests_by_physical.entry(physical_key).or_default();
            if requests.iter().any(|existing| {
                existing.kind != request.kind
                    || existing.pinned_release != request.pinned_release
                    || existing.origin != request.origin
            }) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            requests
                .try_reserve(1)
                .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
            requests.push(request.clone());
        }
        if !scanned_logical_locators.insert(logical_key) {
            continue;
        }
        let artifact = artifacts_by_physical
            .get(request.physical_locator.as_str())
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let artifact_bytes =
            u64::try_from(artifact.bytes().len()).map_err(|_| SecXbrlError::ByteLimitExceeded)?;
        scanned_bytes = scanned_bytes
            .checked_add(artifact_bytes)
            .ok_or(SecXbrlError::ByteLimitExceeded)?;
        if scanned_bytes > MAX_TAXONOMY_GRAPH_SCAN_BYTES {
            return Err(SecXbrlError::ByteLimitExceeded);
        }
        let scanned = scan_taxonomy_references(
            artifact.bytes(),
            match request.kind {
                SecXbrlTaxonomyArtifactKind::Schema => TaxonomyXmlExpectation::Schema,
                SecXbrlTaxonomyArtifactKind::Linkbase => TaxonomyXmlExpectation::Linkbase,
            },
            request.logical_locator.as_str(),
            filing_locator,
            parser_limits,
            cancellation,
            None,
        )?;
        validate_schema_namespace(&request, scanned.target_namespace.as_ref())?;
        match target_namespaces.get(request.physical_locator.as_str()) {
            Some(existing) if existing != &scanned.target_namespace => {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            Some(_) => {}
            None => {
                target_namespaces.insert(
                    request.physical_locator.as_str().to_owned(),
                    scanned.target_namespace,
                );
            }
        }
        let admitted_and_pending = references
            .len()
            .checked_add(queue.len())
            .and_then(|count| count.checked_add(scanned.references.len()))
            .ok_or(SecXbrlError::RecordLimitExceeded)?;
        if admitted_and_pending > MAX_TAXONOMY_REFERENCES
            || admitted_and_pending > parser_limits.records()
        {
            return Err(SecXbrlError::RecordLimitExceeded);
        }
        queue
            .try_reserve(scanned.references.len())
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        queue.extend(scanned.references);
    }
    if references.is_empty()
        || requests_by_physical.len() != artifacts_by_physical.len()
        || artifacts_by_physical
            .keys()
            .any(|locator| !requests_by_physical.contains_key(*locator))
        || requests_by_physical
            .keys()
            .any(|locator| !artifacts_by_physical.contains_key(locator.as_str()))
        || target_namespaces.len() != artifacts_by_physical.len()
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    validate_taxonomy_reference_bindings(
        filing_locator,
        &references,
        &requests_by_logical,
        &target_namespaces,
        &artifacts_by_physical,
        parser_limits,
        cancellation,
        &mut scanned_bytes,
    )?;
    for requests in requests_by_physical.values_mut() {
        requests.sort_unstable_by(|left, right| {
            left.logical_locator
                .cmp(&right.logical_locator)
                .then_with(|| left.kind.cmp(&right.kind))
        });
    }
    let references = references.into_iter().collect::<Vec<_>>();
    let mut graph_digest = Sha256::new();
    hash_taxonomy_field(&mut graph_digest, b"sec-xbrl-taxonomy-request-graph-v1");
    hash_taxonomy_field(
        &mut graph_digest,
        TAXONOMY_LOCATOR_MAPPING_RULESET.as_bytes(),
    );
    hash_taxonomy_field(&mut graph_digest, TAXONOMY_CATALOG_RELEASE.as_bytes());
    hash_taxonomy_field(&mut graph_digest, filing_locator.as_bytes());
    graph_digest.update(filing_document.evidence().bytes());
    graph_digest.update(physical_bytes.to_be_bytes());
    graph_digest.update(scanned_bytes.to_be_bytes());
    graph_digest.update(
        u64::try_from(artifacts.len())
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
            .to_be_bytes(),
    );
    for artifact in artifacts {
        let locator = artifact.locator().ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let receipt = artifact
            .capture_receipt()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        hash_taxonomy_field(&mut graph_digest, locator.as_bytes());
        hash_taxonomy_field(&mut graph_digest, receipt.source_id().as_str().as_bytes());
        hash_taxonomy_field(
            &mut graph_digest,
            receipt
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        graph_digest.update(artifact.evidence().bytes());
        graph_digest.update(
            u64::try_from(artifact.bytes().len())
                .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
                .to_be_bytes(),
        );
    }
    graph_digest.update(
        u64::try_from(references.len())
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
            .to_be_bytes(),
    );
    for reference in &references {
        hash_taxonomy_field(
            &mut graph_digest,
            reference.parent_logical_locator.as_str().as_bytes(),
        );
        hash_taxonomy_field(
            &mut graph_digest,
            reference.target_logical_locator.as_str().as_bytes(),
        );
        hash_taxonomy_field(
            &mut graph_digest,
            reference.target_physical_locator.as_str().as_bytes(),
        );
        match &reference.fragment {
            Some(fragment) => {
                graph_digest.update([1]);
                hash_taxonomy_field(&mut graph_digest, fragment.as_str().as_bytes());
            }
            None => graph_digest.update([0]),
        }
        for value in [&reference.declared_namespace, &reference.referenced_uri] {
            match value {
                Some(value) => {
                    graph_digest.update([1]);
                    hash_taxonomy_field(&mut graph_digest, value.as_str().as_bytes());
                }
                None => graph_digest.update([0]),
            }
        }
        graph_digest.update([reference.role.ordinal(), reference.origin.ordinal()]);
    }
    Ok(BuiltTaxonomyGraph {
        retained: SecXbrlTaxonomyGraph {
            mapping_ruleset: SourceIdentifier::try_from(TAXONOMY_LOCATOR_MAPPING_RULESET)?,
            catalog_release: SourceIdentifier::try_from(TAXONOMY_CATALOG_RELEASE)?,
            evidence: EvidenceDigest::new(DigestAlgorithm::Sha256, graph_digest.finalize().into()),
            physical_bytes,
            scanned_bytes,
            references: references.into_boxed_slice(),
        },
        requests_by_physical,
        target_namespaces,
    })
}

fn validate_taxonomy_reference_bindings(
    filing_locator: &str,
    references: &BTreeSet<SecXbrlTaxonomyReference>,
    requests: &BTreeMap<String, SecXbrlTaxonomyArtifactRequest>,
    namespaces: &BTreeMap<String, Option<SourceIdentifier>>,
    artifacts: &BTreeMap<&str, &RetrievedSecBytes>,
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
    scanned_bytes: &mut u64,
) -> Result<(), SecXbrlError> {
    let mut fragments_by_target = BTreeMap::<&str, BTreeSet<SourceIdentifier>>::new();
    for reference in references {
        check_taxonomy_cancelled(cancellation)?;
        let child = namespaces
            .get(reference.target_physical_locator.as_str())
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        match reference.role {
            SecXbrlTaxonomyReferenceRole::SchemaImport
            | SecXbrlTaxonomyReferenceRole::SchemaInclude
            | SecXbrlTaxonomyReferenceRole::SchemaRedefine => {
                let parent = requests
                    .get(reference.parent_logical_locator.as_str())
                    .and_then(|request| namespaces.get(request.physical_locator.as_str()))
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?;
                let valid = if reference.role == SecXbrlTaxonomyReferenceRole::SchemaImport {
                    &reference.declared_namespace == child && parent != child
                } else {
                    // XML Schema inclusion/redefinition permits a namespace-less child.
                    child.is_none() || child == parent
                };
                if !valid {
                    return Err(SecXbrlError::InvalidTaxonomySet);
                }
            }
            SecXbrlTaxonomyReferenceRole::RoleDefinition
            | SecXbrlTaxonomyReferenceRole::ArcroleDefinition => {
                let fragment = reference
                    .fragment
                    .as_ref()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?;
                if reference.referenced_uri.is_none() {
                    return Err(SecXbrlError::InvalidTaxonomySet);
                }
                fragments_by_target
                    .entry(reference.target_logical_locator.as_str())
                    .or_default()
                    .insert(fragment.clone());
            }
            _ => {}
        }
    }
    // Only IDs actually referenced by the bounded graph are indexed, not every concept in a
    // large base taxonomy. Original bodies remain the authority for ID/type/URI associations.
    for (logical, fragments) in fragments_by_target {
        check_taxonomy_cancelled(cancellation)?;
        let request = requests
            .get(logical)
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let artifact = artifacts
            .get(request.physical_locator.as_str())
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        *scanned_bytes = scanned_bytes
            .checked_add(
                u64::try_from(artifact.bytes().len())
                    .map_err(|_| SecXbrlError::ByteLimitExceeded)?,
            )
            .filter(|bytes| *bytes <= MAX_TAXONOMY_GRAPH_SCAN_BYTES)
            .ok_or(SecXbrlError::ByteLimitExceeded)?;
        let scanned = scan_taxonomy_references(
            artifact.bytes(),
            TaxonomyXmlExpectation::Schema,
            logical,
            filing_locator,
            parser_limits,
            cancellation,
            Some(&fragments),
        )?;
        for reference in references.iter().filter(|reference| {
            reference.target_logical_locator.as_str() == logical
                && reference.referenced_uri.is_some()
        }) {
            let definition = reference
                .fragment
                .as_ref()
                .and_then(|fragment| scanned.fragments.get(fragment))
                .and_then(Option::as_ref)
                .ok_or(SecXbrlError::InvalidTaxonomySet)?;
            if definition.role != reference.role
                || Some(&definition.uri) != reference.referenced_uri.as_ref()
            {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
        }
    }
    Ok(())
}

fn taxonomy_artifact_request(
    filing_locator: &str,
    reference: &SecXbrlTaxonomyReference,
) -> Result<SecXbrlTaxonomyArtifactRequest, SecXbrlError> {
    let (physical_locator, origin) = map_taxonomy_locator(
        filing_locator,
        reference.target_logical_locator.as_str(),
        reference.role.target_kind(),
    )?;
    if physical_locator != reference.target_physical_locator || origin != reference.origin {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(SecXbrlTaxonomyArtifactRequest {
        logical_locator: reference.target_logical_locator.clone(),
        physical_locator,
        kind: reference.role.target_kind(),
        pinned_release: pinned_taxonomy_release(
            filing_locator,
            reference.target_logical_locator.as_str(),
            origin,
        )?,
        origin,
    })
}

pub(crate) fn filing_taxonomy_seed_requests(
    filing_document: &RetrievedSecBytes,
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<Vec<SecXbrlTaxonomyArtifactRequest>, SecXbrlError> {
    let filing_locator = filing_document
        .locator()
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    validate_filing_document_locator(filing_locator)?;
    let scan = scan_taxonomy_references(
        filing_document.bytes(),
        TaxonomyXmlExpectation::Filing,
        filing_locator,
        filing_locator,
        parser_limits,
        cancellation,
        None,
    )?;
    requests_from_references(filing_locator, scan.references)
}

pub(crate) fn taxonomy_request_dependencies(
    filing_locator: &str,
    request: &SecXbrlTaxonomyArtifactRequest,
    bytes: &[u8],
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<Vec<SecXbrlTaxonomyArtifactRequest>, SecXbrlError> {
    let scan = scan_taxonomy_references(
        bytes,
        match request.kind {
            SecXbrlTaxonomyArtifactKind::Schema => TaxonomyXmlExpectation::Schema,
            SecXbrlTaxonomyArtifactKind::Linkbase => TaxonomyXmlExpectation::Linkbase,
        },
        request.logical_locator.as_str(),
        filing_locator,
        parser_limits,
        cancellation,
        None,
    )?;
    validate_schema_namespace(request, scan.target_namespace.as_ref())?;
    requests_from_references(filing_locator, scan.references)
}

fn requests_from_references(
    filing_locator: &str,
    references: Vec<SecXbrlTaxonomyReference>,
) -> Result<Vec<SecXbrlTaxonomyArtifactRequest>, SecXbrlError> {
    let mut requests = Vec::new();
    requests
        .try_reserve_exact(references.len())
        .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
    for reference in &references {
        requests.push(taxonomy_artifact_request(filing_locator, reference)?);
    }
    Ok(requests)
}

#[derive(Clone, Copy)]
enum TaxonomyXmlExpectation {
    Filing,
    Schema,
    Linkbase,
}

struct ScannedTaxonomyXml {
    target_namespace: Option<SourceIdentifier>,
    references: Vec<SecXbrlTaxonomyReference>,
    fragments: BTreeMap<SourceIdentifier, Option<TaxonomyFragmentDefinition>>,
}

struct TaxonomyFragmentDefinition {
    role: SecXbrlTaxonomyReferenceRole,
    uri: SourceIdentifier,
}

fn scan_taxonomy_references(
    bytes: &[u8],
    expectation: TaxonomyXmlExpectation,
    parent_logical_locator: &str,
    filing_locator: &str,
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
    requested_fragments: Option<&BTreeSet<SourceIdentifier>>,
) -> Result<ScannedTaxonomyXml, SecXbrlError> {
    if bytes.is_empty() || bytes.len() > parser_limits.decoded_bytes() {
        return Err(SecXbrlError::ByteLimitExceeded);
    }
    let mut reader = NsReader::from_reader(bytes);
    reader.config_mut().trim_text(false);
    reader.config_mut().expand_empty_elements = true;
    let mut depth = 0usize;
    let mut root_seen = false;
    let mut root_closed = false;
    let mut declaration_seen = false;
    let mut target_namespace = None;
    let mut references = Vec::new();
    let mut fragments = BTreeMap::new();
    loop {
        check_taxonomy_cancelled(cancellation)?;
        let (resolution, event) = reader.read_resolved_event()?;
        match event {
            Event::Start(start) => {
                if root_closed || root_seen && depth == 0 {
                    return Err(SecXbrlError::InvalidTaxonomySet);
                }
                depth = depth
                    .checked_add(1)
                    .ok_or(SecXbrlError::DepthLimitExceeded)?;
                if depth > parser_limits.depth() {
                    return Err(SecXbrlError::DepthLimitExceeded);
                }
                let name = resolve_element_name(resolution, start.name(), parser_limits)?;
                let values = attributes(&reader, &start, parser_limits)?;
                if values.namespaced(XML_NAMESPACE, "base").is_some() {
                    return Err(SecXbrlError::InvalidTaxonomySet);
                }
                if !root_seen {
                    let valid_root = match expectation {
                        TaxonomyXmlExpectation::Filing => {
                            is_element(&name, "http://www.w3.org/1999/xhtml", "html")
                                || is_element(&name, "http://www.xbrl.org/2003/instance", "xbrl")
                        }
                        TaxonomyXmlExpectation::Schema => {
                            is_element(&name, XML_SCHEMA_NAMESPACE, "schema")
                        }
                        TaxonomyXmlExpectation::Linkbase => {
                            is_element(&name, XBRL_LINK_NAMESPACE, "linkbase")
                        }
                    };
                    if !valid_root {
                        return Err(SecXbrlError::InvalidTaxonomySet);
                    }
                    if matches!(expectation, TaxonomyXmlExpectation::Schema) {
                        target_namespace = values
                            .unqualified("targetNamespace")
                            .map(SourceIdentifier::try_from)
                            .transpose()?;
                    }
                    root_seen = true;
                }
                if let Some(id) = values.unqualified("id")
                    && requested_fragments.is_some_and(|requested| {
                        requested.iter().any(|fragment| fragment.as_str() == id)
                    })
                {
                    let role = if is_element(&name, XBRL_LINK_NAMESPACE, "roleType") {
                        Some((SecXbrlTaxonomyReferenceRole::RoleDefinition, "roleURI"))
                    } else if is_element(&name, XBRL_LINK_NAMESPACE, "arcroleType") {
                        Some((
                            SecXbrlTaxonomyReferenceRole::ArcroleDefinition,
                            "arcroleURI",
                        ))
                    } else {
                        None
                    };
                    let definition = role
                        .map(|(role, attribute)| {
                            Ok::<_, SecXbrlError>(TaxonomyFragmentDefinition {
                                role,
                                uri: SourceIdentifier::try_from(
                                    values
                                        .unqualified(attribute)
                                        .ok_or(SecXbrlError::InvalidTaxonomySet)?,
                                )?,
                            })
                        })
                        .transpose()?;
                    if fragments
                        .insert(SourceIdentifier::try_from(id)?, definition)
                        .is_some()
                    {
                        return Err(SecXbrlError::InvalidTaxonomySet);
                    }
                }
                let reference = taxonomy_reference_for_element(
                    expectation,
                    &name,
                    &values,
                    parent_logical_locator,
                    filing_locator,
                )?;
                if let Some(reference) = reference {
                    if references.len() >= MAX_TAXONOMY_REFERENCES
                        || references.len() >= parser_limits.records()
                    {
                        return Err(SecXbrlError::RecordLimitExceeded);
                    }
                    references
                        .try_reserve(1)
                        .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
                    references.push(reference);
                }
            }
            Event::End(_) => {
                depth = depth
                    .checked_sub(1)
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?;
                if depth == 0 {
                    root_closed = true;
                }
            }
            Event::DocType(_) => return Err(SecXbrlError::DoctypeForbidden),
            Event::GeneralRef(reference) => {
                if depth == 0 || !is_safe_xml_general_reference(&reference.decode()?) {
                    return Err(SecXbrlError::InvalidTaxonomySet);
                }
            }
            Event::Eof => break,
            Event::Decl(_) => {
                if declaration_seen || root_seen {
                    return Err(SecXbrlError::InvalidTaxonomySet);
                }
                declaration_seen = true;
            }
            Event::Text(text) => {
                if depth == 0 && !text.xml10_content()?.trim().is_empty() {
                    return Err(SecXbrlError::InvalidTaxonomySet);
                }
            }
            Event::CData(_) if depth == 0 => return Err(SecXbrlError::InvalidTaxonomySet),
            Event::PI(_) | Event::Comment(_) | Event::CData(_) => {}
            Event::Empty(_) => return Err(SecXbrlError::ParserInvariant),
        }
    }
    if !root_seen
        || !root_closed
        || depth != 0
        || matches!(expectation, TaxonomyXmlExpectation::Filing) && references.is_empty()
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    references.sort_unstable();
    references.dedup();
    Ok(ScannedTaxonomyXml {
        target_namespace,
        references,
        fragments,
    })
}

fn is_safe_xml_general_reference(reference: &str) -> bool {
    if matches!(reference, "amp" | "lt" | "gt" | "apos" | "quot") {
        return true;
    }
    let scalar = if let Some(hex) = reference
        .strip_prefix("#x")
        .or_else(|| reference.strip_prefix("#X"))
    {
        u32::from_str_radix(hex, 16).ok()
    } else {
        reference
            .strip_prefix('#')
            .and_then(|decimal| decimal.parse::<u32>().ok())
    };
    scalar.is_some_and(|scalar| {
        char::from_u32(scalar).is_some()
            && matches!(
                scalar,
                0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF
            )
    })
}

fn taxonomy_reference_for_element(
    expectation: TaxonomyXmlExpectation,
    name: &XbrlQualifiedName,
    attributes: &ResolvedAttributes,
    parent_logical_locator: &str,
    filing_locator: &str,
) -> Result<Option<SecXbrlTaxonomyReference>, SecXbrlError> {
    if matches!(expectation, TaxonomyXmlExpectation::Filing)
        && is_element(name, XBRL_LINK_NAMESPACE, "schemaRef")
    {
        return reference_from_xlink(
            parent_logical_locator,
            filing_locator,
            SecXbrlTaxonomyReferenceRole::FilingSchema,
            attributes,
        )
        .map(Some);
    }
    if matches!(expectation, TaxonomyXmlExpectation::Schema)
        && is_element(name, XML_SCHEMA_NAMESPACE, "import")
    {
        return reference_from_schema_location(
            parent_logical_locator,
            filing_locator,
            SecXbrlTaxonomyReferenceRole::SchemaImport,
            attributes,
        )
        .map(Some);
    }
    if matches!(expectation, TaxonomyXmlExpectation::Schema)
        && is_element(name, XML_SCHEMA_NAMESPACE, "include")
    {
        return reference_from_schema_location(
            parent_logical_locator,
            filing_locator,
            SecXbrlTaxonomyReferenceRole::SchemaInclude,
            attributes,
        )
        .map(Some);
    }
    if matches!(expectation, TaxonomyXmlExpectation::Schema)
        && is_element(name, XML_SCHEMA_NAMESPACE, "redefine")
    {
        return reference_from_schema_location(
            parent_logical_locator,
            filing_locator,
            SecXbrlTaxonomyReferenceRole::SchemaRedefine,
            attributes,
        )
        .map(Some);
    }
    if matches!(expectation, TaxonomyXmlExpectation::Schema)
        && is_element(name, XBRL_LINK_NAMESPACE, "linkbaseRef")
    {
        let role = linkbase_reference_role(
            attributes
                .namespaced(XLINK_NAMESPACE, "role")
                .ok_or(SecXbrlError::InvalidTaxonomySet)?,
        )?;
        return reference_from_xlink(parent_logical_locator, filing_locator, role, attributes)
            .map(Some);
    }
    if is_element(name, XBRL_LINK_NAMESPACE, "roleRef") {
        return reference_from_xlink(
            parent_logical_locator,
            filing_locator,
            SecXbrlTaxonomyReferenceRole::RoleDefinition,
            attributes,
        )
        .map(Some);
    }
    if is_element(name, XBRL_LINK_NAMESPACE, "arcroleRef") {
        return reference_from_xlink(
            parent_logical_locator,
            filing_locator,
            SecXbrlTaxonomyReferenceRole::ArcroleDefinition,
            attributes,
        )
        .map(Some);
    }
    Ok(None)
}

fn reference_from_schema_location(
    parent_logical_locator: &str,
    filing_locator: &str,
    role: SecXbrlTaxonomyReferenceRole,
    attributes: &ResolvedAttributes,
) -> Result<SecXbrlTaxonomyReference, SecXbrlError> {
    let href = attributes
        .unqualified("schemaLocation")
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    let mut reference =
        resolve_taxonomy_reference(parent_logical_locator, filing_locator, role, href, false)?;
    if role == SecXbrlTaxonomyReferenceRole::SchemaImport {
        reference.declared_namespace = attributes
            .unqualified("namespace")
            .map(SourceIdentifier::try_from)
            .transpose()?;
    }
    Ok(reference)
}

fn reference_from_xlink(
    parent_logical_locator: &str,
    filing_locator: &str,
    role: SecXbrlTaxonomyReferenceRole,
    attributes: &ResolvedAttributes,
) -> Result<SecXbrlTaxonomyReference, SecXbrlError> {
    if attributes.namespaced(XLINK_NAMESPACE, "type") != Some("simple") {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    let href = attributes
        .namespaced(XLINK_NAMESPACE, "href")
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    let mut reference = resolve_taxonomy_reference(
        parent_logical_locator,
        filing_locator,
        role,
        href,
        matches!(
            role,
            SecXbrlTaxonomyReferenceRole::RoleDefinition
                | SecXbrlTaxonomyReferenceRole::ArcroleDefinition
        ),
    )?;
    let uri_attribute = match role {
        SecXbrlTaxonomyReferenceRole::RoleDefinition => Some("roleURI"),
        SecXbrlTaxonomyReferenceRole::ArcroleDefinition => Some("arcroleURI"),
        _ => None,
    };
    reference.referenced_uri = uri_attribute
        .map(|attribute| {
            SourceIdentifier::try_from(
                attributes
                    .unqualified(attribute)
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
            )
            .map_err(SecXbrlError::from)
        })
        .transpose()?;
    Ok(reference)
}

fn resolve_taxonomy_reference(
    parent_logical_locator: &str,
    filing_locator: &str,
    role: SecXbrlTaxonomyReferenceRole,
    href: &str,
    fragment_required: bool,
) -> Result<SecXbrlTaxonomyReference, SecXbrlError> {
    let base = Url::parse(parent_logical_locator).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    let mut logical = base
        .join(href)
        .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    let fragment = logical
        .fragment()
        .map(SourceIdentifier::try_from)
        .transpose()?;
    if fragment_required != fragment.is_some() {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    logical.set_fragment(None);
    let target_logical_locator = SourceIdentifier::try_from(logical.as_str())?;
    let (target_physical_locator, origin) = map_taxonomy_locator(
        filing_locator,
        target_logical_locator.as_str(),
        role.target_kind(),
    )?;
    Ok(SecXbrlTaxonomyReference {
        parent_logical_locator: SourceIdentifier::try_from(parent_logical_locator)?,
        target_logical_locator,
        target_physical_locator,
        fragment,
        declared_namespace: None,
        referenced_uri: None,
        role,
        origin,
    })
}

fn linkbase_reference_role(value: &str) -> Result<SecXbrlTaxonomyReferenceRole, SecXbrlError> {
    match value {
        "http://www.xbrl.org/2003/role/calculationLinkbase" => {
            Ok(SecXbrlTaxonomyReferenceRole::CalculationLinkbase)
        }
        "http://www.xbrl.org/2003/role/definitionLinkbase" => {
            Ok(SecXbrlTaxonomyReferenceRole::DefinitionLinkbase)
        }
        "http://www.xbrl.org/2003/role/labelLinkbase" => {
            Ok(SecXbrlTaxonomyReferenceRole::LabelLinkbase)
        }
        "http://www.xbrl.org/2003/role/presentationLinkbase" => {
            Ok(SecXbrlTaxonomyReferenceRole::PresentationLinkbase)
        }
        "http://www.xbrl.org/2003/role/referenceLinkbase" => {
            Ok(SecXbrlTaxonomyReferenceRole::ReferenceLinkbase)
        }
        _ => Err(SecXbrlError::InvalidTaxonomySet),
    }
}

fn validate_filing_document_locator(locator: &str) -> Result<Url, SecXbrlError> {
    let parsed = Url::parse(locator).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("www.sec.gov")
        || !parsed.path().starts_with("/Archives/edgar/data/")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path().contains('%')
        || !(parsed.path().ends_with(".htm")
            || parsed.path().ends_with(".html")
            || parsed.path().ends_with(".xml"))
        || parsed.as_str() != locator
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(parsed)
}

fn map_taxonomy_locator(
    filing_locator: &str,
    logical_locator: &str,
    kind: SecXbrlTaxonomyArtifactKind,
) -> Result<(SourceIdentifier, SecXbrlTaxonomyOrigin), SecXbrlError> {
    let filing = validate_filing_document_locator(filing_locator)?;
    let logical = Url::parse(logical_locator).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    if !matches!(logical.scheme(), "http" | "https")
        || !logical.username().is_empty()
        || logical.password().is_some()
        || logical.port().is_some()
        || logical.query().is_some()
        || logical.fragment().is_some()
        || logical.path().contains('%')
        || match kind {
            SecXbrlTaxonomyArtifactKind::Schema => !logical.path().ends_with(".xsd"),
            SecXbrlTaxonomyArtifactKind::Linkbase => !logical.path().ends_with(".xml"),
        }
        || logical.as_str() != logical_locator
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    let filing_directory = filing_directory(&filing)?;
    let same_filing_directory = logical.host_str() == Some("www.sec.gov")
        && logical.path().starts_with(&filing_directory)
        && !logical.path()[filing_directory.len()..].contains('/');
    let origin = if same_filing_directory {
        SecXbrlTaxonomyOrigin::SecFiling
    } else {
        match logical.host_str() {
            Some("xbrl.sec.gov") => SecXbrlTaxonomyOrigin::SecTaxonomy,
            Some("taxonomies.xbrl.us") => SecXbrlTaxonomyOrigin::XbrlUsLegacyTaxonomy,
            Some("xbrl.fasb.org") => SecXbrlTaxonomyOrigin::FasbTaxonomy,
            Some("www.xbrl.org") => SecXbrlTaxonomyOrigin::XbrlStandard,
            Some("www.w3.org") => SecXbrlTaxonomyOrigin::W3cStandard,
            _ => return Err(SecXbrlError::InvalidTaxonomySet),
        }
    };
    if logical.as_str() == filing.as_str() {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    let mut physical = logical;
    physical
        .set_scheme("https")
        .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    validate_physical_taxonomy_locator(physical.as_str())?;
    pinned_taxonomy_release(filing_locator, logical_locator, origin)?;
    Ok((SourceIdentifier::try_from(physical.as_str())?, origin))
}

fn pinned_taxonomy_release(
    filing_locator: &str,
    logical_locator: &str,
    origin: SecXbrlTaxonomyOrigin,
) -> Result<SourceIdentifier, SecXbrlError> {
    let filing = validate_filing_document_locator(filing_locator)?;
    let logical = Url::parse(logical_locator).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    let segments = logical
        .path_segments()
        .ok_or(SecXbrlError::InvalidTaxonomySet)?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let release = match origin {
        SecXbrlTaxonomyOrigin::SecFiling => {
            let directory = filing_directory(&filing)?;
            let mut digest = Sha256::new();
            hash_taxonomy_field(&mut digest, directory.as_bytes());
            let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into());
            format!("sec-filing-extension.{}", digest_prefix(digest, 8))
        }
        SecXbrlTaxonomyOrigin::SecTaxonomy => {
            if segments.len() < 3 || !is_taxonomy_family(segments[0]) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            let directory_release = admitted_taxonomy_release(segments[1])?;
            let release = taxonomy_artifact_release(
                segments
                    .last()
                    .copied()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
                directory_release,
            )?;
            format!("sec-{}-{release}", segments[0])
        }
        SecXbrlTaxonomyOrigin::XbrlUsLegacyTaxonomy => {
            if !matches!(segments.as_slice(), ["us-gaap", _, ..]) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            let directory_release = admitted_taxonomy_release(segments[1])?;
            let release = taxonomy_artifact_release(
                segments
                    .last()
                    .copied()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
                directory_release,
            )?;
            format!("xbrl-us-us-gaap-{release}")
        }
        SecXbrlTaxonomyOrigin::FasbTaxonomy => {
            if segments.len() < 3 || !is_taxonomy_family(segments[0]) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            let directory_release = if segments[1] == "2019_with_2019_dei" {
                segments[1]
            } else {
                admitted_taxonomy_release(segments[1])?
            };
            let release = taxonomy_artifact_release(
                segments
                    .last()
                    .copied()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
                directory_release,
            )?;
            format!("fasb-{}-{release}", segments[0])
        }
        SecXbrlTaxonomyOrigin::XbrlStandard => xbrl_standard_release(&segments)?,
        SecXbrlTaxonomyOrigin::W3cStandard => {
            let year = segments
                .first()
                .copied()
                .filter(|year| matches!(*year, "1999" | "2001"))
                .ok_or(SecXbrlError::InvalidTaxonomySet)?;
            if segments.len() < 2 {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            format!("w3c-standard-{year}")
        }
    };
    SourceIdentifier::try_from(release).map_err(Into::into)
}

// Namespace identifiers describe schema components, not the authority that supplied bytes.
// Exact import/include and role bindings are verified on the closed captured graph below.
fn validate_schema_namespace(
    request: &SecXbrlTaxonomyArtifactRequest,
    target_namespace: Option<&SourceIdentifier>,
) -> Result<(), SecXbrlError> {
    if request.kind == SecXbrlTaxonomyArtifactKind::Linkbase && target_namespace.is_some()
        || target_namespace.is_some_and(|namespace| namespace.as_str().is_empty())
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(())
}

fn filing_directory(filing: &Url) -> Result<String, SecXbrlError> {
    filing
        .path()
        .rsplit_once('/')
        .map(|(directory, _)| format!("{directory}/"))
        .ok_or(SecXbrlError::InvalidTaxonomySet)
}

fn admitted_taxonomy_release(value: &str) -> Result<&str, SecXbrlError> {
    let year_text = value.get(..4).ok_or(SecXbrlError::InvalidTaxonomySet)?;
    let quarter = value
        .get(4..)
        .is_some_and(|suffix| matches!(suffix, "q1" | "q2" | "q3" | "q4"));
    if !year_text.bytes().all(|byte| byte.is_ascii_digit())
        || !(value.len() == 4
            || quarter
            || value.len() == 10 && NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok())
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(value)
}

fn taxonomy_artifact_release<'a>(
    file: &'a str,
    directory_release: &'a str,
) -> Result<&'a str, SecXbrlError> {
    let stem = file
        .strip_suffix(".xsd")
        .or_else(|| file.strip_suffix(".xml"))
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    let candidate = [10usize, 6, 4]
        .into_iter()
        .find_map(|length| {
            let release = stem
                .len()
                .checked_sub(length)
                .and_then(|start| stem.get(start..))?;
            admitted_taxonomy_release(release).ok().map(|_| release)
        })
        .unwrap_or(directory_release);
    if taxonomy_releases_compatible(candidate, directory_release) {
        Ok(candidate)
    } else {
        Err(SecXbrlError::InvalidTaxonomySet)
    }
}

fn taxonomy_releases_compatible(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let Some(left_year) = left.get(..4) else {
        return false;
    };
    let Some(right_year) = right.get(..4) else {
        return false;
    };
    left_year == right_year && (left.len() == 4 || right.len() == 4)
}

fn xbrl_standard_release(segments: &[&str]) -> Result<String, SecXbrlError> {
    match segments {
        [release, ..]
            if release.len() == 4 && release.as_bytes().iter().all(u8::is_ascii_digit) =>
        {
            admitted_taxonomy_release(release)?;
            Ok(format!("xbrl-standard-{release}"))
        }
        ["lrr", "role" | "arcrole", file] => {
            Ok(format!("xbrl-lrr-{}", dated_taxonomy_schema_release(file)?))
        }
        ["dtr", "type", release, ..] => {
            let release = release
                .strip_prefix("CR-")
                .map_or(*release, |release| release);
            if NaiveDate::parse_from_str(release, "%Y-%m-%d").is_ok() {
                Ok(format!("xbrl-dtr-{release}"))
            } else {
                Ok(format!(
                    "xbrl-dtr-{}",
                    dated_taxonomy_schema_release(release)?
                ))
            }
        }
        ["dtr", "dtr.xsd"] => Ok("xbrl-dtr-catalog".to_owned()),
        ["lrr", "lrr.xsd"] => Ok("xbrl-lrr-catalog".to_owned()),
        _ => Err(SecXbrlError::InvalidTaxonomySet),
    }
}

fn dated_taxonomy_schema_release(file: &str) -> Result<&str, SecXbrlError> {
    let stem = file
        .strip_suffix(".xsd")
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    let release = stem
        .len()
        .checked_sub(10)
        .and_then(|offset| stem.get(offset..))
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    NaiveDate::parse_from_str(release, "%Y-%m-%d").map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    Ok(release)
}

fn is_taxonomy_family(value: &str) -> bool {
    value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn check_taxonomy_cancelled(cancellation: &CancellationToken) -> Result<(), SecXbrlError> {
    if cancellation.is_cancelled() {
        Err(SecXbrlError::Cancelled)
    } else {
        Ok(())
    }
}

fn digest_prefix(digest: EvidenceDigest, bytes: usize) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.saturating_mul(2));
    for byte in digest.bytes().into_iter().take(bytes) {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn hash_taxonomy_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(
        u64::try_from(value.len())
            .map_or(u64::MAX, |length| length)
            .to_be_bytes(),
    );
    digest.update(value);
}

/// Immutable document-level evidence shared by every parsed occurrence.
#[derive(Clone, Debug)]
pub struct XbrlDocumentContext {
    pub(super) accession: SourceIdentifier,
    pub(super) expected_cik: Option<SourceIdentifier>,
    pub(super) taxonomy_set: XbrlTaxonomySet,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) evaluated_at: Timestamp,
}

impl XbrlDocumentContext {
    // The opaque prepared handoff owns the same validated set used to construct this context.
    pub(crate) fn checked_dynamic_retained_bytes(
        &self,
        taxonomy: &SecValidatedXbrlTaxonomySet,
    ) -> Option<usize> {
        self.accession
            .retained_bytes()
            .checked_add(
                self.expected_cik
                    .as_ref()
                    .map_or(0, SourceIdentifier::retained_bytes),
            )?
            .checked_add(taxonomy.version().retained_bytes())?
            .checked_add(self.source_payload.dynamic_retained_bytes()?)
    }

    /// Binds parser output to accession, taxonomy set, exact payload, and evaluation time.
    pub const fn new(
        accession: SourceIdentifier,
        taxonomy_set: XbrlTaxonomySet,
        source_payload: ExactPayloadEvidence,
        evaluated_at: Timestamp,
    ) -> Self {
        Self {
            accession,
            expected_cik: None,
            taxonomy_set,
            source_payload,
            evaluated_at,
        }
    }

    /// Binds parser output to a freshly revalidated captured taxonomy admission.
    pub(crate) fn from_validated_taxonomy(
        accession: SourceIdentifier,
        expected_cik: SourceIdentifier,
        taxonomy_set: &SecPendingValidatedXbrlTaxonomySet,
        source_payload: ExactPayloadEvidence,
        evaluated_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecXbrlError> {
        taxonomy_set.revalidate(cancellation)?;
        Ok(Self {
            accession,
            expected_cik: Some(expected_cik),
            taxonomy_set: taxonomy_set.validated().domain_set(),
            source_payload,
            evaluated_at,
        })
    }
}

/// One exact normalized numeric fact plus its full occurrence evidence.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct XbrlNumericFact {
    pub(super) concept: SourceIdentifier,
    pub(super) unit: SourceIdentifier,
    pub(super) value: Decimal,
    pub(super) evidence: XbrlFactEvidence,
}

impl XbrlNumericFact {
    /// Returns the qualified concept identity.
    pub const fn concept(&self) -> &SourceIdentifier {
        &self.concept
    }
    /// Returns the normalized unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
    /// Returns the exact normalized decimal.
    pub const fn value(&self) -> Decimal {
        self.value
    }
    /// Returns occurrence-level audit evidence.
    pub const fn evidence(&self) -> &XbrlFactEvidence {
        &self.evidence
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SourceIdentifier,
        SourceIdentifier,
        Decimal,
        XbrlFactEvidence,
    ) {
        (self.concept, self.unit, self.value, self.evidence)
    }
}

/// Nil or nonnumeric occurrence retained without fabricating a Decimal.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct XbrlNonnumericOccurrence {
    pub(super) occurrence_id: SourceIdentifier,
    pub(super) accession: SourceIdentifier,
    pub(super) concept: XbrlQualifiedName,
    pub(super) context_id: SourceIdentifier,
    #[serde(with = "occurrence_context_serde")]
    pub(super) context: Arc<XbrlOccurrenceContext>,
    pub(super) lexical_value: XbrlText,
    pub(super) nil: bool,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) occurrence_relationships: XbrlOccurrenceRelationships,
}

/// One parsed source context shared by every nonnumeric occurrence referencing it.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct XbrlOccurrenceContext {
    pub(super) entity: market_squawk_domain::XbrlEntity,
    pub(super) period: market_squawk_domain::XbrlPeriod,
    pub(super) dimensions: Vec<market_squawk_domain::XbrlDimensionEvidence>,
    pub(super) context_graph: market_squawk_domain::XbrlContextGraph,
}

impl XbrlNonnumericOccurrence {
    /// Returns the source or deterministic occurrence identity.
    pub const fn occurrence_id(&self) -> &SourceIdentifier {
        &self.occurrence_id
    }

    /// Returns the exact filing accession carrying this occurrence.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    /// Returns the exact XBRL context identity referenced by this occurrence.
    pub const fn context_id(&self) -> &SourceIdentifier {
        &self.context_id
    }

    /// Returns the source entity referenced by this occurrence.
    pub fn entity(&self) -> &market_squawk_domain::XbrlEntity {
        &self.context.entity
    }

    /// Returns the exact source instant or duration.
    pub fn period(&self) -> market_squawk_domain::XbrlPeriod {
        self.context.period
    }

    /// Returns all source-reported dimensions, including segment/scenario placement.
    pub fn dimensions(&self) -> &[market_squawk_domain::XbrlDimensionEvidence] {
        &self.context.dimensions
    }

    /// Returns the retained source-only XML context graph.
    pub fn context_graph(&self) -> &market_squawk_domain::XbrlContextGraph {
        &self.context.context_graph
    }

    /// Returns the source lexical and resolved concept QName.
    pub const fn concept(&self) -> &XbrlQualifiedName {
        &self.concept
    }

    /// Returns the exact bounded text, empty only for an explicit nil occurrence.
    pub const fn lexical_value(&self) -> &XbrlText {
        &self.lexical_value
    }
    /// Returns whether the occurrence carried explicit nil semantics.
    pub const fn is_nil(&self) -> bool {
        self.nil
    }

    /// Returns nesting, continuation, and explanatory relationship evidence.
    pub const fn occurrence_relationships(&self) -> &XbrlOccurrenceRelationships {
        &self.occurrence_relationships
    }

    /// Returns the exact filing payload evidence carrying this occurrence.
    pub const fn source_payload(&self) -> &ExactPayloadEvidence {
        &self.source_payload
    }
}

/// One source footnote and its exact explanatory text, independent of numeric fact contexts.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct XbrlFootnoteOccurrence {
    pub(super) occurrence_id: SourceIdentifier,
    pub(super) accession: SourceIdentifier,
    pub(super) language: XbrlText,
    pub(super) role: SourceIdentifier,
    pub(super) title: Option<XbrlText>,
    pub(super) lexical_value: XbrlText,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) occurrence_relationships: XbrlOccurrenceRelationships,
}

impl XbrlFootnoteOccurrence {
    pub const fn occurrence_id(&self) -> &SourceIdentifier {
        &self.occurrence_id
    }
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }
    pub const fn language(&self) -> &XbrlText {
        &self.language
    }
    pub const fn role(&self) -> &SourceIdentifier {
        &self.role
    }
    pub const fn title(&self) -> Option<&XbrlText> {
        self.title.as_ref()
    }
    pub const fn lexical_value(&self) -> &XbrlText {
        &self.lexical_value
    }
    pub const fn source_payload(&self) -> &ExactPayloadEvidence {
        &self.source_payload
    }
    pub const fn occurrence_relationships(&self) -> &XbrlOccurrenceRelationships {
        &self.occurrence_relationships
    }
}

/// Parsed XBRL output preserving numeric and nonnumeric occurrence families separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedXbrlDocument {
    pub(super) accession: SourceIdentifier,
    pub(super) expected_cik: Option<SourceIdentifier>,
    pub(super) taxonomy_set: XbrlTaxonomySet,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) evaluated_at: Timestamp,
    pub(super) numeric_facts: Vec<XbrlNumericFact>,
    pub(super) nonnumeric_occurrences: Vec<XbrlNonnumericOccurrence>,
    pub(super) footnotes: Vec<XbrlFootnoteOccurrence>,
}

impl ParsedXbrlDocument {
    /// Returns complete explanatory footnotes carried by the source filing.
    pub fn footnotes(&self) -> &[XbrlFootnoteOccurrence] {
        &self.footnotes
    }

    /// Returns normalized numeric facts.
    pub fn numeric_facts(&self) -> &[XbrlNumericFact] {
        &self.numeric_facts
    }
    /// Returns nil and nonnumeric occurrences.
    pub fn nonnumeric_occurrences(&self) -> &[XbrlNonnumericOccurrence] {
        &self.nonnumeric_occurrences
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use cap_std::{ambient_authority, fs::Dir};
    use market_squawk_domain::ExactPayloadEvidence;
    use market_squawk_platform::LocalPaths;
    use market_squawk_sources::{
        ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    };

    use super::*;
    use crate::xbrl::SecTaxonomyClosure;

    fn captured_artifact(
        store: &RawEvidenceStore,
        locator: &str,
        bytes: &[u8],
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        received_at: Timestamp,
    ) -> Result<RetrievedSecBytes, Box<dyn std::error::Error>> {
        let evidence = store.persist(bytes)?;
        let mut request = Sha256::new();
        request.update(b"market-squawk/sec-taxonomy-test-request/v1");
        hash_taxonomy_field(&mut request, locator.as_bytes());
        let request = EvidenceDigest::new(DigestAlgorithm::Sha256, request.finalize().into());
        let page = ProviderCapturePageReceipt::try_new(
            0,
            request,
            None,
            None,
            200,
            u64::try_from(bytes.len())?,
            evidence,
            received_at,
        )?;
        let receipt = ProviderCaptureSetReceipt::try_new(
            source_id,
            metadata_revision,
            SourceIdentifier::try_from(locator)?,
            request,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )?;
        Ok(RetrievedSecBytes::captured_online(
            bytes.to_vec(),
            evidence,
            received_at,
            locator.to_owned(),
            1,
            receipt,
        ))
    }

    // Reuses this module's captured artifacts and the real closed adapter admission. All
    // physical custody, native binding, publication and reopening use the public owning APIs.
    async fn exercise_normalized_filing_physical_restart(
        root: &std::path::Path,
        store: Arc<RawEvidenceStore>,
        source_id: SourceId,
        revision: MetadataRevision,
        filing: RetrievedSecBytes,
        artifacts: Vec<RetrievedSecBytes>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use market_squawk_data::{
            AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
            CatalogLimit, CatalogResultLimits, DatasetId, IngestIdentity, ObjectStoreConfig,
            PointInTimeLimits, PointInTimeRevisionMode, RightsBasis, RightsDecisionInput,
            SecResearchDisposition, SecResearchFamily, SecResearchReadRequest, SourceOperation,
        };
        use market_squawk_domain::{
            AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
            EffectiveInterval, InstrumentId, ProviderIdentityEvidence, ProviderIdentityRecord,
            ProviderIdentityRecordInput, ProviderInstrumentId, ResearchTemporalCoordinate,
            RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability,
        };
        use market_squawk_sources::{
            AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, CoverageDomain,
            EndpointPolicy, FreshnessPolicy, HistoricalCapability, NetworkAccessPolicy,
            SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
            SourceMetadataProvider, SourceProtocolProfile,
        };
        use std::num::{NonZeroU32, NonZeroU64};
        use std::time::Instant;
        let cancellation = CancellationToken::new();
        let registry_path = root.join("normalized-filing-representations");
        std::fs::create_dir(&registry_path)?;
        let representations = Arc::new(crate::SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&registry_path, ambient_authority())?,
            crate::SecRepresentationLimits::production_defaults(),
        )?);
        let representation = representations.record_source_success_cancellable(
            &source_id,
            filing.locator().ok_or("captured filing locator")?,
            filing.evidence(),
            u64::try_from(filing.bytes().len())?,
            crate::SecHttpValidators::default(),
            &cancellation,
        )?;
        let at = representation.first_observed_at();
        let filing = captured_artifact(
            &store,
            representation.locator(),
            filing.bytes(),
            source_id.clone(),
            revision.clone(),
            at,
        )?;
        let artifacts = artifacts
            .into_iter()
            .map(|artifact| {
                let receipt = artifact
                    .capture_receipt()
                    .ok_or("captured taxonomy receipt")?;
                captured_artifact(
                    &store,
                    artifact.locator().ok_or("captured taxonomy locator")?,
                    artifact.bytes(),
                    receipt.source_id().clone(),
                    receipt.metadata_revision().clone(),
                    at,
                )
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let instrument: InstrumentId = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1".parse()?;
        let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            source_id.clone(),
            RevisionBoundPayloadEvidence::new(
                revision.clone(),
                ExactPayloadEvidence::from_content_digest(filing.evidence()),
            ),
            SourceClass::RegulatoryFiling,
            SourceIdentifier::try_from(crate::SEC_PROVIDER_RATE_SCOPE)?,
            AuthorizationGrant::new(
                AuthorizationMode::PublicInterface,
                AuthorizationBasis::new(SourceIdentifier::try_from("sec-public-edgar")?),
                ExactPayloadEvidence::from_content_digest(filing.evidence()),
                validity,
            ),
            SourceCoverage::try_non_instrument(
                ExactPayloadEvidence::from_content_digest(filing.evidence()),
                validity,
                CoverageDomain::RegulatoryFilings,
                CoverageDelay::Delayed(1),
                DeliveryEvidence::Unknown,
            )?,
            DataQuality::OfficialDelayed,
            NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([
                "https://data.sec.gov/submissions",
                "https://www.sec.gov/Archives/edgar/data",
            ])?),
            FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
            Some(crate::sec_application_budget_policy()?),
            SourceCapabilities::new(
                false,
                true,
                SequenceCapability::Unsupported,
                ChecksumCapability::Unsupported,
                HistoricalCapability::RevisionPreserving,
                false,
            ),
            SourceProtocolProfile::NotLive,
        ))?;
        struct MetadataOwner(SourceMetadata);
        impl SourceMetadataProvider for MetadataOwner {
            fn metadata(&self) -> &SourceMetadata {
                &self.0
            }
        }
        let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
        let registered = registry.register(metadata.clone(), at)?;
        let extraction_authority =
            registry.extraction_authority(&registered, &MetadataOwner(metadata.clone()))?;
        // CompanyFacts uses the same complete logical stream, preserving revisions and native
        // source ordinals across the existing 256-row work window. Repeat one official fixture
        // occurrence as distinct source-array occurrences; no rows are deduplicated or capped.
        {
            use market_squawk_sources::{
                DiscoveryRequest, ExtractionContentAccumulator, ExtractionRequest,
                ProviderCaptureMaterial, ProviderCaptureSetReceipt, SourceObject,
                SourceObjectCaptureIdentity,
            };
            let mut json: serde_json::Value =
                serde_json::from_slice(include_bytes!("../../fixtures/company-facts.json"))?;
            let fact = json["facts"]["us-gaap"]["Assets"]["units"]["USD"][0].clone();
            json["facts"] = serde_json::json!({ "us-gaap": { "Assets": { "units": { "USD": vec![fact; 257] } } } });
            let facts_bytes = serde_json::to_vec(&json)?;
            let selection = crate::SecResearchDataset::company_facts("0000320193")?;
            // Use the actual HTTP-to-representation boundary. A later successful response
            // preserves the first content-observation clock but has a fresh physical receipt.
            let body_received_at = crate::client::system_timestamp()?;
            let body_digest = store.persist(&facts_bytes)?;
            let representation = representations.record_source_success_cancellable(
                &source_id,
                selection.initial_provider_locator().as_str(),
                body_digest,
                facts_bytes.len() as u64,
                crate::SecHttpValidators::default(),
                &cancellation,
            )?;
            let first_capture = crate::client::retrieved_from_representation(
                facts_bytes.clone(),
                representation.clone(),
                &source_id,
                &revision,
                body_digest,
                200,
                body_received_at,
            )?;
            let later_received_at = crate::client::system_timestamp()?;
            let captured = crate::client::retrieved_from_representation(
                facts_bytes.clone(),
                representation,
                &source_id,
                &revision,
                body_digest,
                200,
                later_received_at,
            )?;
            let at = captured.received_at();
            assert!(body_received_at < at);
            assert!(at < later_received_at);
            assert_eq!(first_capture.received_at(), at);
            assert_eq!(
                captured.capture_receipt().ok_or("facts capture")?.pages()[0].received_at(),
                later_received_at
            );
            let transport = captured.capture_material()?.ok_or("facts transport")?;
            let receipt = transport.receipt();
            let material = ProviderCaptureMaterial::try_new(
                ProviderCaptureSetReceipt::try_new(
                    receipt.source_id().clone(),
                    receipt.metadata_revision().clone(),
                    selection.dataset().clone(),
                    receipt.request_set_identity(),
                    receipt.terminal(),
                    receipt.pages().to_vec(),
                )?,
                transport.records().to_vec(),
            )?;
            let deadline = crate::client::system_timestamp()?.checked_add_nanos(60_000_000_000)?;
            let discovery = DiscoveryRequest::try_new(
                selection.dataset().clone(),
                None,
                std::num::NonZeroU16::MIN,
                deadline,
            )?;
            let request_for = |object_id: SourceIdentifier| -> Result<ExtractionRequest, Box<dyn std::error::Error>> {
            let object = SourceObject::try_new_with_capture_identity(
                source_id.clone(),
                revision.clone(),
                &discovery,
                object_id,
                SourceIdentifier::try_from("application/json")?,
                ExactPayloadEvidence::with_version_pinned_locator(
                    captured.evidence(),
                    market_squawk_domain::VersionPinnedSourceLocator::new(
                        SourceIdentifier::try_from(captured.locator().ok_or("facts locator")?)?,
                        SourceIdentifier::try_from(captured.retrieval_revision().ok_or("facts revision")?.to_string())?,
                    ),
                ),
                SourceObjectCaptureIdentity::try_from_capture(material.receipt())?,
                EffectiveInterval::new(at, None)?,
                None,
                market_squawk_sources::AvailabilityEvidence::LocalFirstObserved { observed_at: at },
                Some(facts_bytes.len() as u64),
            )?;
            Ok(ExtractionRequest::try_new(
                object,
                NonZeroU32::new(100_000).ok_or("facts rows")?,
                NonZeroU64::new(64 * 1024 * 1024).ok_or("facts bytes")?,
                deadline,
            )?)
            };
            let request = request_for(selection.source_object_id().clone())?;
            let wrong_locator = crate::SecObjectLocator::company_facts("0000789019")?;
            assert!(matches!(
                crate::extraction::company_facts_stream_blocking(
                    request_for(SourceIdentifier::try_from(wrong_locator.url())?)?,
                    Arc::clone(&store),
                    source_id.clone(),
                    extraction_authority.clone(),
                    &material,
                    &cancellation,
                ),
                Err(crate::SecClientError::InvalidCaptureMaterial)
            ));
            // The first response's physical clock is earlier than the same representation
            // clock; accept it without rewriting either retained timestamp as well.
            let original = first_capture
                .capture_material()?
                .ok_or("first facts transport")?;
            let first_material = ProviderCaptureMaterial::try_new(
                ProviderCaptureSetReceipt::try_new(
                    original.receipt().source_id().clone(),
                    original.receipt().metadata_revision().clone(),
                    selection.dataset().clone(),
                    original.receipt().request_set_identity(),
                    original.receipt().terminal(),
                    original.receipt().pages().to_vec(),
                )?,
                original.records().to_vec(),
            )?;
            let first_stream = crate::extraction::company_facts_stream_blocking(
                request_for(selection.source_object_id().clone())?,
                Arc::clone(&store),
                source_id.clone(),
                extraction_authority.clone(),
                &first_material,
                &cancellation,
            )?;
            assert_eq!(first_stream.company_identity().received_at(), at);
            drop(first_stream);
            let mut facts = crate::extraction::company_facts_stream_blocking(
                request,
                Arc::clone(&store),
                source_id.clone(),
                extraction_authority.clone(),
                &material,
                &cancellation,
            )?;
            assert_eq!(facts.family(), crate::SecResearchDatasetKind::CompanyFacts);
            assert_eq!(facts.total_records(), 257);
            assert!(
                facts
                    .request()
                    .object()
                    .evidence()
                    .version_pinned_locator()
                    .is_some()
            );
            assert_eq!(
                facts.company_identity().parent_ingest_payload_evidence(),
                facts.request().object().evidence(),
                "logical publication must retain the complete admitted parent evidence",
            );
            let retrieved = crate::RetrievedCompanyFacts::restored(
                facts_bytes,
                captured.evidence(),
                at,
                market_squawk_domain::AvailabilityEvidence::LocalFirstObserved { observed_at: at },
                SecParserLimits::production_defaults(),
                &cancellation,
            )?;
            let expected = crate::normalize_company_facts(
                &source_id,
                &retrieved,
                facts.company_identity().ingested_at(),
            )?;
            let mut whole = ExtractionContentAccumulator::try_new(facts.request(), 257)?;
            let mut seen = 0;
            let mut chunks = 0;
            while let Some(chunk) = facts.next_chunk(&cancellation)? {
                chunks += 1;
                chunk.native_lineage().validate(chunk.batch())?;
                assert_eq!(
                    chunk.row_capture_page_ordinals(),
                    vec![0; chunk.batch().records().len()]
                );
                assert!(
                    chunk
                        .native_lineage()
                        .batch_sidecar()
                        .ok_or("facts sidecar")?
                        .chunks()
                        .is_none()
                );
                for (record, native) in chunk
                    .batch()
                    .records()
                    .iter()
                    .zip(chunk.native_lineage().rows())
                {
                    let decoded: market_squawk_domain::ResearchObservation =
                        serde_json::from_slice(record.payload())?;
                    assert_eq!(decoded, expected[seen]);
                    let expected_native = serde_json::to_vec(&serde_json::json!({
                        "family": "company_fact", "occurrence": &retrieved.document().occurrences()[seen],
                    }))?;
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(native.semantic_payload())?,
                        serde_json::from_slice::<serde_json::Value>(&expected_native)?
                    );
                    whole.push(record)?;
                    seen += 1;
                }
                if chunks == 1 {
                    assert_eq!(seen, 256);
                    let cancelled = CancellationToken::new();
                    cancelled.cancel();
                    assert!(matches!(
                        facts.next_chunk(&cancelled),
                        Err(crate::SecClientError::Cancelled)
                    ));
                    assert_eq!(facts.emitted_records(), 256);
                    // Incomplete output cannot become a complete family content identity.
                    let mut partial = ExtractionContentAccumulator::try_new(facts.request(), 257)?;
                    for record in chunk.batch().records() {
                        partial.push(record)?;
                    }
                    assert!(partial.finish().is_err());
                }
            }
            assert_eq!((chunks, seen, facts.emitted_records()), (2, 257, 257));
            assert_eq!(whole.finish()?.record_count(), 257);
        }
        let paths = LocalPaths::prepare(root.join("normalized-filing-restart"))?;
        let raw_store = paths.sealed_research_journal_store()?;
        let submissions_bytes = include_bytes!("../../fixtures/submissions-recent.json");
        let submissions_raw = captured_artifact(
            &store,
            crate::SecObjectLocator::submissions("0000320193")?.url(),
            submissions_bytes,
            source_id.clone(),
            revision.clone(),
            at,
        )?;
        let submissions = crate::RetrievedSubmissions::new(
            crate::SubmissionsDocument::parse(
                submissions_bytes,
                SecParserLimits::production_defaults(),
            )?,
            submissions_raw,
            Vec::new(),
        );
        let root_material = filing
            .capture_material()?
            .ok_or("captured filing material")?;
        let (expectation, request) = root_material.into_whole_seal_parts();
        let root_token = expectation
            .try_rejoin(request.seal(&raw_store)?)?
            .try_into_whole()?;
        let admitted = crate::extraction::admit_filing_xbrl_root_from_sealed_capture(
            root_token,
            Arc::clone(&store),
            representations,
            source_id.clone(),
            revision.clone(),
            SecParserLimits::production_defaults(),
            &submissions,
            "0000320193-25-000079",
            &filing,
            &cancellation,
        )?;
        let handoff = crate::extraction::prepare_filing_xbrl_capture_from_admitted_root(
            store,
            source_id.clone(),
            revision,
            SecParserLimits::production_defaults(),
            submissions,
            admitted,
            artifacts,
            &cancellation,
        )?;
        let dataset =
            DatasetId::try_from(handoff.dataset().analytical_dataset_identifier()?.as_str())?;
        let deadline = crate::client::system_timestamp()?.checked_add_nanos(60_000_000_000)?;
        let (mut stream, material) = handoff.extract(
            extraction_authority,
            NonZeroU32::new(1).ok_or("record ceiling")?,
            NonZeroU64::new(64 * 1024 * 1024).ok_or("byte ceiling")?,
            deadline,
            cancellation.clone(),
            root,
        )?;
        assert_eq!(stream.total_records(), 2);
        let company = stream.company_identity().clone();
        assert_eq!(
            company.surface(),
            market_squawk_domain::CompanyIdentitySurface::SecFilingXbrl
        );
        let company_json = serde_json::to_vec(&company)?;
        let company_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(company_json).into());
        let location = paths.catalog()?.clone();
        let catalog_config = CatalogConfig::try_new(
            location.clone(),
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?;
        let authority = CatalogAuthority::open(catalog_config.clone())?;
        authority.register_source(&metadata, at)?;
        // The complete graph retains distinct publisher source/revision authority. Register
        // its actual code-owned descriptors; SEC metadata cannot stand in for these parents.
        for publisher in [
            FASB_XBRL_TAXONOMY_AUTHORITY,
            XBRL_INTERNATIONAL_STANDARDS_AUTHORITY,
            W3C_XML_SCHEMA_STANDARDS_AUTHORITY,
        ] {
            let dependency = publisher.dependency_source_metadata()?;
            authority.register_source(&dependency, at)?;
        }
        let store_config =
            ObjectStoreConfig::try_new(64 * 1024 * 1024, 64, Duration::from_secs(60))?;
        let service = AnalyticalDataService::initialize(
            authority,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            store_config,
        )?;
        use market_squawk_sources::{
            ExtractionContentAccumulator, LogicalObjectRole, LogicalPartitionFamily,
            LogicalPartitionSetAdmission, PendingLogicalPartitionSet, ProviderLogicalTerminalInput,
            SealedLogicalObjectInput, SealedProviderLogicalPublicationBinding,
        };
        use std::io::{Read, Write};
        #[derive(Debug)]
        struct FixtureControl(CancellationToken);
        impl market_squawk_platform::ResearchObjectControl for FixtureControl {
            fn checkpoint(
                &self,
                _: market_squawk_platform::ResearchObjectControlPoint,
            ) -> Result<(), market_squawk_platform::ResearchObjectControlError> {
                if self.0.is_cancelled() {
                    return Err(market_squawk_platform::ResearchObjectControlError::Cancelled);
                }
                Ok(())
            }
        }
        impl market_squawk_data::IngestPrecommitAuthority for FixtureControl {
            fn validate_precommit(&self) -> Result<(), market_squawk_data::IngestError> {
                if self.0.is_cancelled() {
                    return Err(market_squawk_data::IngestError::Cancelled);
                }
                Ok(())
            }
        }
        fn object_admission(
            bytes: u64,
        ) -> Result<
            market_squawk_platform::ResearchObjectAdmission,
            market_squawk_platform::SealedResearchJournalStoreError,
        > {
            market_squawk_platform::ResearchObjectAdmission::try_new(bytes.max(1), 4095)
        }
        fn digest(bytes: &[u8]) -> EvidenceDigest {
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
        }
        let control = FixtureControl(cancellation.clone());
        let mut raw_objects = Vec::new();
        for record in material.records() {
            let mut pending =
                raw_store.begin_logical_object(object_admission(record.payload().len() as u64)?)?;
            pending.write_all(record.payload())?;
            raw_objects.push(raw_store.finish_logical_object(pending, &control)?);
        }
        let (expectation, seal_request) = material.into_whole_seal_parts();
        let token = expectation
            .try_rejoin(seal_request.seal(&raw_store)?)?
            .try_into_whole()?;
        let (mut objects, receipt) =
            SealedLogicalObjectInput::try_from_whole_capture(token, raw_objects, &control)?;
        let mut staging = service.begin_provider_logical_stream(
            dataset.clone(),
            source_id.clone(),
            &cancellation,
        )?;
        let partition_admission = LogicalPartitionSetAdmission::try_new(
            object_admission(32 * 1024 * 1024)?,
            4096,
            256,
            128 * 1024,
        )?;
        let mut row_partitions = PendingLogicalPartitionSet::begin(
            LogicalPartitionFamily::CanonicalRowMap,
            digest(b"market-squawk/sec-filing/logical-row-map/v1"),
            partition_admission,
            0,
        )?;
        let mut native_partitions = None;
        let mut content = None;
        let mut expectations = Vec::new();
        let mut native_descriptor = None;
        // The fixture forces two chunks while retaining complete shared filing evidence once.
        while let Some(chunk) = stream.next_chunk(&cancellation)? {
            assert_eq!(chunk.batch().records().len(), 1);
            assert_eq!(chunk.row_capture_page_ordinals(), &[1]);
            let observation: market_squawk_domain::ResearchObservation =
                serde_json::from_slice(chunk.batch().records()[0].payload())?;
            let market_squawk_domain::ResearchObservation::Fundamental(fact) = observation else {
                return Err("unexpected filing canonical observation".into());
            };
            assert_eq!(fact.context().provenance().instrument_id(), None);
            assert_eq!(fact.context().provenance().source_id(), company.source_id());
            assert_eq!(
                fact.subject().issuer_id(),
                Some(company.provider_company_id())
            );
            let start = stream.emitted_records() - chunk.batch().records().len();
            let (batch, _, native, page_ordinals) = chunk.into_parts();
            let batch = batch.try_bind_provider_capture(receipt.capture())?;
            if content.is_none() {
                content = Some(ExtractionContentAccumulator::try_new(
                    batch.request(),
                    stream.total_records(),
                )?);
            }
            native.validate(&batch)?;
            if native_partitions.is_none() {
                native_partitions = Some(PendingLogicalPartitionSet::begin(
                    LogicalPartitionFamily::ProviderNative,
                    native.schema().fingerprint(),
                    partition_admission,
                    0,
                )?);
                let sidecar = native.batch_sidecar().ok_or("complete source sidecar")?;
                let chunks = sidecar.chunks().ok_or("chunked source evidence")?;
                let mut reader = chunks.reader()?;
                let mut pending =
                    raw_store.begin_logical_object(object_admission(reader.metadata()?.len())?)?;
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    let read = reader.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    pending.write_all(&buffer[..read])?;
                }
                objects.push(SealedLogicalObjectInput::try_from_verified(
                    LogicalObjectRole::ProviderComponent,
                    objects.len() as u32,
                    sidecar.semantic_payload_digest(),
                    raw_store.finish_logical_object(pending, &control)?,
                    &control,
                )?);
                native_descriptor = Some(sidecar.semantic_payload().to_vec());
            }
            let native_set = native_partitions.as_mut().ok_or("native partitions")?;
            for (local, ((record, native_row), page)) in batch
                .records()
                .iter()
                .zip(native.rows())
                .zip(&page_ordinals)
                .enumerate()
            {
                let ordinal = u64::try_from(start + local)?;
                content.as_mut().ok_or("whole content")?.push(record)?;
                native_set.stage_frame(
                    &raw_store,
                    &control,
                    ordinal,
                    native_row.semantic_payload(),
                    native_row.semantic_payload_digest(),
                )?;
                let frame = receipt.row_frame(u32::try_from(ordinal)?, *page)?;
                let mapping = serde_json::to_vec(&serde_json::json!({
                    "canonical_row_ordinal": frame.canonical_row_ordinal(), "capture_page_ordinal": frame.capture_page_ordinal(),
                    "segment_ordinal": frame.segment_ordinal(), "physical_frame_ordinal": frame.physical_frame_ordinal(),
                    "page_body_digest": frame.page_body_digest(), "received_at": frame.received_at(), "source_sequence": frame.source_sequence(),
                    "canonical_record_digest": record.evidence().content_digest(), "native_semantic_digest": native_row.semantic_payload_digest(),
                }))?;
                row_partitions.stage_frame(
                    &raw_store,
                    &control,
                    ordinal,
                    &mapping,
                    digest(&mapping),
                )?;
            }
            native_set.seal_current_partition(&raw_store, &control)?;
            row_partitions.seal_current_partition(&raw_store, &control)?;
            let revisions = market_squawk_sources::ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())?;
            expectations.push(
                service
                    .stage_provider_logical_stream_chunk(
                        &mut staging,
                        batch,
                        native,
                        revisions,
                        &receipt,
                        &page_ordinals,
                        &cancellation,
                    )
                    .await?,
            );
        }
        assert_eq!(stream.emitted_records(), 2);
        assert_eq!(expectations.len(), 2);
        let whole_content = content.ok_or("whole content")?.finish()?;
        let companion = serde_json::to_vec(&serde_json::json!({
            "version": 1, "family": "sec_filing_capture", "capture": receipt.capture(),
            "sealed_receipt_digest": receipt.receipt_digest(), "original_segment_claim": receipt.segment().claim(),
            "company_identity": &company, "native_descriptor": native_descriptor,
            "extraction_content_identity": whole_content.digest(), "record_count": whole_content.record_count(),
        }))?;
        let mut pending =
            raw_store.begin_logical_object(object_admission(companion.len() as u64)?)?;
        pending.write_all(&companion)?;
        objects.push(SealedLogicalObjectInput::try_from_verified(
            LogicalObjectRole::ProviderComponent,
            objects.len() as u32,
            digest(&companion),
            raw_store.finish_logical_object(pending, &control)?,
            &control,
        )?);
        let mut partitions = native_partitions
            .ok_or("native partitions")?
            .finish(&raw_store, &control)?
            .into_partitions()
            .into_vec();
        partitions.extend(
            row_partitions
                .finish(&raw_store, &control)?
                .into_partitions()
                .into_vec(),
        );
        let total_logical_object_bytes = objects
            .iter()
            .map(|object| object.object().size_bytes())
            .sum();
        let binding = SealedProviderLogicalPublicationBinding::try_new(
            ProviderLogicalTerminalInput {
                source_id: source_id.clone(),
                source_revision_digest: metadata
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest(),
                execution_attempt_digest: Some(receipt.receipt_digest()),
                provider_terminal_evidence_digest: whole_content.digest(),
                total_decoded_events: 0,
                total_canonical_rows: 2,
                total_logical_object_bytes,
            },
            &[
                LogicalPartitionFamily::ProviderNative,
                LogicalPartitionFamily::CanonicalRowMap,
            ],
            objects,
            partitions,
            expectations,
        )?;
        let payload_digest = binding.binding_digest();
        let identity = IngestIdentity::try_new(
            source_id.clone(),
            payload_digest,
            SourceOperation::Persist,
            "sec:normalized-filing:restart:v1",
        )?;
        let reservation = service
            .reserve_source_ingest(
                &metadata,
                at,
                RightsDecisionInput {
                    source_id,
                    payload_digest,
                    retrieved_at: at,
                    basis: RightsBasis::reviewed_terms(
                        "https://www.sec.gov/os/accessing-edgar-data",
                        filing.evidence(),
                    )?,
                    authorization_evidence: filing.evidence(),
                    authorization_expires_at: None,
                    permitted_operations: vec![SourceOperation::Persist],
                },
                &identity,
                &cancellation,
            )
            .await?;
        let (committed, binding_digest) = service
            .finish_provider_logical_stream(
                staging,
                reservation,
                binding,
                company.clone(),
                Arc::new(control),
                cancellation.clone(),
            )
            .await
            .map_err(|error| {
                std::io::Error::other(format!("normalized SEC filing publication: {error}"))
            })?;
        assert_eq!(binding_digest, payload_digest);
        drop(stream);
        // The operator explicitly resolves this fixture instrument against the captured filing.
        // A matching ticker alone is deliberately not relationship authority.
        use market_squawk_domain::{
            AssetClass, CommonEquitySuitability, CompanySecurityIdentityLink,
            CompanySecurityIdentityLinkInput, CompanySecurityKind, CompanySecurityLinkTransition,
            CompanySecurityRelationshipKind, CompanySecurityResolutionBasis, Currency,
            IdentifierEntitlement, IdentifierRightsPolicyReference, MarketDataInstrumentDefinition,
            MarketDataInstrumentDefinitionInput, VenueId, VenueMapping, VenueSymbol,
        };
        let reviewed = ExactPayloadEvidence::from_content_digest(filing.evidence());
        let definition =
            MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
                instrument_id: instrument,
                reference_evidence: metadata.revision_evidence().clone(),
                effective_interval: validity,
                asset_class: AssetClass::Equity,
                display_name: None,
                quote_currency: Currency::try_from("USD")?,
                quote_currency_evidence: reviewed.clone(),
                venue_mappings: vec![VenueMapping::new(
                    VenueId::try_from("XNAS")?,
                    VenueSymbol::try_from("AAPL")?,
                )],
                provider_identities: vec![ProviderIdentityRecord::new(
                    ProviderIdentityRecordInput {
                        instrument_id: instrument,
                        source_id: metadata.source_id().clone(),
                        provider_instrument_id: ProviderInstrumentId::try_from("0000320193")?,
                        evidence: ProviderIdentityEvidence::from_content_digest(filing.evidence()),
                        source_timestamp: None,
                        observed_at: at,
                        metadata_revision: metadata.revision().clone(),
                        validity,
                        supersedes: None,
                    },
                )],
                identifiers: Vec::new(),
            })?;
        service
            .market_data_instrument_synchronization()
            .synchronize(
                market_squawk_data::MarketDataInstrumentSynchronization::try_new(
                    vec![definition],
                    1,
                )?,
                Instant::now() + Duration::from_secs(30),
                &cancellation,
            )?;
        let market_definition = service
            .market_data_instruments()
            .latest(
                instrument,
                Instant::now() + Duration::from_secs(30),
                &cancellation,
            )?
            .ok_or("fixture market definition")?;
        let authorized_at = crate::client::system_timestamp()?;
        service.company_security_link_publication().publish(
            CompanySecurityIdentityLink::try_new(CompanySecurityIdentityLinkInput {
                schema_version: SchemaVersion::CURRENT,
                company_source_id: company.source_id().clone(),
                provider_company_id: company.provider_company_id().clone(),
                company_surface: company.surface(),
                company_observation_digest: company_digest,
                instrument_id: instrument,
                market_instrument_revision_digest: market_definition.revision_digest(),
                security_kind: CompanySecurityKind::CommonEquity,
                relationship_kind: CompanySecurityRelationshipKind::Issuer,
                common_equity_suitability: CommonEquitySuitability::SuitableIssuerCommonEquity,
                resolution_basis: CompanySecurityResolutionBasis::OperatorAuthorizedResolution {
                    receipt_id: SourceIdentifier::try_from("fixture-common-equity-resolution")?,
                    operator_id: SourceIdentifier::try_from("fixture-operator")?,
                    evidence: reviewed,
                    authorized_at,
                },
                relationship_evidence_rights: IdentifierRightsPolicyReference::new(
                    SourceIdentifier::try_from("fixture-operator-local-use")?,
                    IdentifierEntitlement::UserOwned,
                    SourceIdentifier::try_from("fixture-operator-resolution")?,
                ),
                effective_interval: validity,
                available_at: authorized_at,
                ingested_at: authorized_at,
                transition: CompanySecurityLinkTransition::Initial,
            })?,
            Instant::now() + Duration::from_secs(30),
            &cancellation,
        )?;

        let knowledge_at = crate::client::system_timestamp()?.checked_add_nanos(1_000_000_000)?;
        let request = SecResearchReadRequest::try_new(
            committed.manifest().clone(),
            SecResearchFamily::FilingXbrl,
            binding_digest,
            company_digest,
            knowledge_at,
            ResearchTemporalCoordinate::calendar_date(market_squawk_domain::CalendarDate::new(
                2025, 7, 24,
            )?),
            PointInTimeRevisionMode::LatestKnown,
            PointInTimeLimits::try_new(8, 8, 8, 8, 8 * 1024 * 1024)?,
            64 * 1024 * 1024,
        )?;
        let selected = service
            .sec_research_reader()
            .select(
                request,
                &raw_store,
                Instant::now() + Duration::from_secs(30),
                cancellation.clone(),
            )
            .await?;
        assert_eq!(selected.disposition(), SecResearchDisposition::Selected);
        assert_eq!(selected.decoded_rows().len(), 2);
        let filing_source = selected
            .filing_xbrl()
            .ok_or("verified full filing source")?;
        assert_eq!(filing_source.numeric_fact_count(), 2);
        assert_eq!(filing_source.nonnumeric_occurrences().len(), 3);
        assert_eq!(filing_source.contexts().len(), 1);
        assert_eq!(filing_source.footnotes().len(), 1);
        let footnote = filing_source
            .footnotes()
            .get(0)?
            .ok_or("retained footnote")?;
        assert_eq!(footnote.occurrence_id().as_str(), "shares-footnote");
        assert_eq!(footnote.language().as_str(), "en-US");
        assert_eq!(
            footnote.role().as_str(),
            "http://www.xbrl.org/2003/role/footnote"
        );
        assert_eq!(
            footnote.title().map(|title| title.as_str()),
            Some("Reported count")
        );
        assert_eq!(
            footnote.lexical_value().as_str(),
            "As reported in this filing; no share forecast."
        );
        assert_eq!(
            footnote.occurrence_relationships().continuation_chain()[0].as_str(),
            "shares-note-cont"
        );
        assert_eq!(footnote.occurrence_relationships().relationships().len(), 1);
        let context = filing_source.contexts().get(0)?.ok_or("retained context")?;
        assert_eq!(context.entity().value().as_str(), "0000320193");
        assert_eq!(context.dimensions().len(), 1);
        assert!(!context.context_graph().events().is_empty());
        let mut found_nil = false;
        for occurrence in filing_source.nonnumeric_occurrences().iter() {
            let occurrence = occurrence?;
            found_nil |= occurrence.is_nil();
            assert_eq!(
                filing_source.context(occurrence.context_id())?.as_ref(),
                Some(&context)
            );
        }
        assert!(found_nil);
        let identity_selected = service
            .sec_research_reader()
            .select_by_identity(
                market_squawk_data::SecResearchIdentityReadRequest::try_new(
                    instrument,
                    SecResearchFamily::FilingXbrl,
                    knowledge_at,
                    selected.request().effective_cutoff().clone(),
                    PointInTimeRevisionMode::LatestKnown,
                    selected.request().point_in_time_limits(),
                    selected.request().maximum_object_bytes(),
                )?,
                &raw_store,
                Instant::now() + Duration::from_secs(30),
                cancellation.clone(),
            )
            .await?;
        let common_shares = market_squawk_valuation::CommonShareFilingEvidence::try_from_filing(
            &identity_selected,
        )?;
        assert_eq!(common_shares.instrument_id(), instrument);
        assert_eq!(
            common_shares.shares(),
            rust_decimal::Decimal::from(15_000_000_000_u64)
        );
        assert_eq!(
            common_shares.reported_on(),
            market_squawk_domain::CalendarDate::new(2025, 7, 24)?
        );
        assert_eq!(common_shares.knowledge_at(), knowledge_at);
        let market_squawk_data::SecResearchIdentityOutcome::Exact(identity_facts) =
            identity_selected.outcome()
        else {
            return Err("selected filing identity missing".into());
        };
        let mut eps_row = None;
        for row in identity_facts.selected() {
            if let Some(market_squawk_domain::ResearchObservation::Fundamental(fact)) =
                identity_facts
                    .decoded_rows()
                    .get(usize::try_from(row.row().row_ordinal())?)?
                && fact.xbrl_evidence().is_some_and(|xbrl| {
                    xbrl.concept().local_name().as_str() == "EarningsPerShareDiluted"
                })
            {
                eps_row = Some(row.row().row_ordinal());
            }
        }
        let eps_row = eps_row.ok_or("selected filing EPS missing")?;
        let eps = market_squawk_valuation::ValuationInput::from_selected_fundamental(
            &identity_selected,
            eps_row,
            market_squawk_valuation::InputSignificance::Significant,
        )?;
        assert_eq!(eps.subject_instrument_id(), instrument);
        assert_eq!(
            eps.amount().money().amount(),
            rust_decimal::Decimal::new(650, 2)
        );
        assert_eq!(
            eps.amount().basis(),
            market_squawk_valuation::ValuationAmountBasis::PerInstrumentUnit
        );
        assert!(
            market_squawk_valuation::ValuationInput::from_selected_fundamental(
                &identity_selected,
                u32::MAX,
                market_squawk_valuation::InputSignificance::Significant,
            )
            .is_err()
        );
        assert!(
            identity_selected
                .identity()
                .receipt()
                .validate_selected_company(
                    "11111111-1111-4111-8111-111111111111".parse()?,
                    &company,
                    company_digest,
                    knowledge_at,
                )
                .is_err()
        );
        assert!(
            identity_selected
                .identity()
                .receipt()
                .validate_selected_company(
                    instrument,
                    &company,
                    EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
                    knowledge_at,
                )
                .is_err()
        );
        let valuation_limits = market_squawk_valuation::FairValueLimits::try_new(
            market_squawk_valuation::FairValueLimitInput {
                max_measurements: 2,
                max_inputs_per_measurement: 2,
                max_records_per_family: 4,
                max_query_results: 4,
                max_retained_bytes: 2 * 1024 * 1024,
            },
        )?;
        // Exercise retained financial evidence, without treating the reported EPS as a quote.
        let measurement = market_squawk_valuation::ValuationMeasurement::try_new(
            market_squawk_valuation::ValuationMeasurementSpec {
                account_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse()?,
                instrument_id: instrument,
                amount: eps.amount(),
                measurement_at: knowledge_at,
                prepared_at: knowledge_at,
                prepared_by: market_squawk_valuation::ActorId::try_from(
                    "filing-evidence-preparer",
                )?,
                method: market_squawk_valuation::ValuationMethod::MarketApproach,
                inputs: vec![eps.clone()],
            },
        )?;
        let mut valuation = market_squawk_valuation::FairValueService::open(
            service.fair_value_catalog(),
            valuation_limits,
        )?;
        let decision = valuation.classify(
            measurement.clone(),
            market_squawk_valuation::ClassificationRuleset::current(1_000_000_000)?,
        )?;
        assert_ne!(
            decision.hierarchy(),
            market_squawk_domain::FairValueHierarchy::Level1
        );
        drop(valuation);
        let expected_request = selected.request().clone();
        let expected_receipt = selected.receipt();
        let identity_request = identity_selected.request().clone();
        drop(identity_selected);
        drop(selected);
        drop(committed);
        drop(service);
        // The physical capture receipt pins the original journal owner through its segment.
        // Its persisted evidence is already bound into the publication and expected read receipt.
        drop(receipt);
        drop(raw_store);
        let reopened = AnalyticalDataService::open(
            CatalogAuthority::open(catalog_config)?,
            AnalyticalManifestCatalog::open(&location, 8)?,
            paths.artifacts()?.clone(),
            store_config,
        )?;
        let reopened_raw = paths.sealed_research_journal_store()?;
        let valuation_replay = market_squawk_valuation::FairValueService::open(
            reopened.fair_value_catalog(),
            valuation_limits,
        )?;
        assert_eq!(
            valuation_replay.measurement(measurement.id()).as_deref(),
            Some(&measurement),
        );
        assert_eq!(
            valuation_replay.decision(decision.id()).as_deref(),
            Some(decision.as_ref())
        );
        drop(valuation_replay);
        let replay = reopened
            .sec_research_reader()
            .select(
                expected_request,
                &reopened_raw,
                Instant::now() + Duration::from_secs(30),
                cancellation.clone(),
            )
            .await?;
        assert_eq!(replay.receipt(), expected_receipt);
        for observation in replay.decoded_rows().iter() {
            let market_squawk_domain::ResearchObservation::Fundamental(fact) = observation? else {
                return Err("unexpected reopened filing observation".into());
            };
            assert_eq!(fact.context().provenance().instrument_id(), None);
            assert_eq!(
                fact.subject().issuer_id(),
                Some(company.provider_company_id())
            );
        }
        assert_eq!(
            replay
                .filing_xbrl()
                .ok_or("replayed filing")?
                .numeric_fact_count(),
            2
        );
        assert_eq!(
            replay
                .filing_xbrl()
                .ok_or("replayed filing")?
                .nonnumeric_occurrences()
                .len(),
            3
        );
        let identity_replay = reopened
            .sec_research_reader()
            .select_by_identity(
                identity_request,
                &reopened_raw,
                Instant::now() + Duration::from_secs(30),
                cancellation,
            )
            .await?;
        assert_eq!(
            market_squawk_valuation::CommonShareFilingEvidence::try_from_filing(&identity_replay)?,
            common_shares
        );
        assert_eq!(
            market_squawk_valuation::ValuationInput::from_selected_fundamental(
                &identity_replay,
                eps_row,
                market_squawk_valuation::InputSignificance::Significant,
            )?,
            eps
        );
        eprintln!("physical filing and financial identity/restart assertions passed");
        // Optional original-filing regression; this exercises the production indexed parser,
        // retaining one decoded occurrence at a time rather than building a diagnostic sidecar.
        if let Ok(path) = std::env::var("SEC_SOURCE_BUDGET_FILING") {
            let bytes = std::fs::read(path)?;
            assert_eq!(
                super::super::hex_prefix(&Sha256::digest(&bytes), 32),
                "6db62640d2908a510d91c9e13f5d6bdab6e373137bdec161a52510be08c49b41",
                "SEC_SOURCE_BUDGET_FILING must identify the retained MSFT FY2026 body",
            );
            let parser_cancellation = CancellationToken::new();
            let parsed = crate::XbrlDocumentParser::parse_indexed_in_with_cancellation(
                &bytes,
                crate::SecParserLimits::production_defaults(),
                crate::XbrlDocumentContext::new(
                    SourceIdentifier::try_from("0001193125-26-323660")?,
                    market_squawk_domain::XbrlTaxonomySet::declared(
                        EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
                        SourceIdentifier::try_from("original-msft-filing-regression")?,
                    ),
                    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        Sha256::digest(&bytes).into(),
                    )),
                    Timestamp::from_unix_nanos(1),
                ),
                &parser_cancellation,
                root,
            )?;
            assert!(parsed.numeric_count > 0);
            assert!(parsed.nonnumeric_count > 0);
            assert!(parsed.context_count()? > 0);
            let mut capitalized_zero = 0usize;
            let mut reportable_segments = 0usize;
            for ordinal in 0..parsed.numeric_count {
                let fact = parsed
                    .numeric_at(ordinal)?
                    .ok_or("missing indexed MSFT fact")?;
                match fact.evidence().lexical_value().as_str() {
                    "No" => {
                        assert_eq!(fact.value(), Decimal::ZERO);
                        capitalized_zero += 1;
                    }
                    "three" => {
                        assert_eq!(fact.value(), Decimal::from(3));
                        assert_eq!(
                            fact.concept().as_str(),
                            "us-gaap:NumberOfReportableSegments"
                        );
                        assert_eq!(
                            fact.evidence().occurrence_id().as_str(),
                            "F_7d649a3c-20aa-439f-96c6-6da56efffa7c"
                        );
                        assert_eq!(
                            fact.evidence().context_id().as_str(),
                            "C_29985a27-1d12-4b7e-9a06-156523f6e71e"
                        );
                        assert_eq!(
                            fact.evidence().unit().source_identifier()?.as_str(),
                            "msft:Segment"
                        );
                        assert_eq!(
                            serde_json::to_value(fact.evidence())?["transformed_lexeme"],
                            "3"
                        );
                        reportable_segments += 1;
                    }
                    _ => {}
                }
            }
            assert_eq!(capitalized_zero, 3);
            assert_eq!(reportable_segments, 1);
            parser_cancellation.cancel();
            assert!(matches!(
                parsed.numeric_at(0),
                Err(crate::SecXbrlError::Cancelled)
            ));
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn captured_taxonomy_closes_one_mixed_source_graph_and_honors_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let store = Arc::new(RawEvidenceStore::new(Dir::open_ambient_dir(
            temporary.path(),
            ambient_authority(),
        )?));
        let observed_at = crate::client::system_timestamp()?;
        let sec_source = SEC_EDGAR_AUTHORITY.canonical_source_id()?;
        let sec_revision = MetadataRevision::new(SourceIdentifier::try_from("sec-test-v1")?);
        let filing_locator =
            "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl-20250628.htm";
        let filing = captured_artifact(
            &store,
            filing_locator,
            br#"<html xmlns="http://www.w3.org/1999/xhtml"
                xmlns:link="http://www.xbrl.org/2003/linkbase"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
                xmlns:xbrli="http://www.xbrl.org/2003/instance"
                xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:dei="http://xbrl.sec.gov/dei/2025"
                xmlns:us-gaap="http://fasb.org/us-gaap/2025"
                xmlns:iso4217="http://www.xbrl.org/2003/iso4217"><head>
                <link:schemaRef xlink:type="simple" xlink:href="company-20251231.xsd"/>
                </head><body>
                <xbrli:context id="shares"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000320193</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2025-07-24</xbrli:instant></xbrli:period></xbrli:context>
                <xbrli:context id="annual"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000320193</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2024-06-29</xbrli:startDate><xbrli:endDate>2025-06-28</xbrli:endDate></xbrli:period></xbrli:context>
                <xbrli:context id="listing"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000320193</xbrli:identifier><xbrli:segment><xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonStockMember</xbrldi:explicitMember></xbrli:segment></xbrli:entity><xbrli:period><xbrli:startDate>2024-06-29</xbrli:startDate><xbrli:endDate>2025-06-28</xbrli:endDate></xbrli:period></xbrli:context>
                <xbrli:unit id="shares-unit"><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unit>
                <xbrli:unit id="eps-unit"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator></xbrli:divide></xbrli:unit>
                <ix:nonFraction id="shares-fact" name="dei:EntityCommonStockSharesOutstanding" contextRef="shares" unitRef="shares-unit" decimals="0">15000000000</ix:nonFraction>
                <ix:nonFraction id="eps-fact" name="us-gaap:EarningsPerShareDiluted" contextRef="annual" unitRef="eps-unit" decimals="2">6.50</ix:nonFraction>
                <ix:nonNumeric id="listing-symbol" name="dei:TradingSymbol" contextRef="listing">AAPL</ix:nonNumeric>
                <ix:nonNumeric id="listing-title" name="dei:Security12bTitle" contextRef="listing">Common Stock</ix:nonNumeric>
                <ix:nonNumeric id="nil-file-number" name="dei:EntityFileNumber" contextRef="listing" xsi:nil="true"/>
                <ix:footnote id="shares-footnote" xml:lang="en-US" title="Reported count" continuedAt="shares-note-cont">As reported in this filing;</ix:footnote>
                <ix:continuation id="shares-note-cont"> no share forecast.</ix:continuation>
                <ix:relationship arcrole="http://www.xbrl.org/2003/arcrole/fact-footnote" fromRefs="shares-fact" toRefs="shares-footnote"/>
                </body></html>"#,
            sec_source.clone(),
            sec_revision.clone(),
            observed_at,
        )?;
        let mut artifacts = vec![
            captured_artifact(
                &store,
                "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/company-20251231.xsd",
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    xmlns:link="http://www.xbrl.org/2003/linkbase"
                    xmlns:xlink="http://www.w3.org/1999/xlink"
                    targetNamespace="https://example.test/company/2025">
                    <xs:import namespace="http://fasb.org/us-gaap/2025"
                      schemaLocation="http://xbrl.fasb.org/us-gaap/2025/us-gaap-2025.xsd"/>
                    <xs:import namespace="http://xbrl.org/2020/extensible-enumerations-2.0"
                      schemaLocation="https://www.xbrl.org/2020/extensible-enumerations-2.0.xsd"/>
                    <xs:import namespace="http://fasb.org/srt/2025"
                      schemaLocation="https://xbrl.fasb.org/srt/2025/elts/srt-2025.xsd"/>
                    <xs:import namespace="http://xbrl.sec.gov/cyd/2025"
                      schemaLocation="https://xbrl.sec.gov/cyd/2025/cyd-2025.xsd"/>
                    <xs:import namespace="http://xbrl.sec.gov/ecd-sub/2025"
                      schemaLocation="https://xbrl.sec.gov/ecd/2025/ecd-sub-2025.xsd"/>
                    <link:linkbaseRef xlink:type="simple"
                      xlink:role="http://www.xbrl.org/2003/role/presentationLinkbase"
                      xlink:href="company-20251231_pre.xml"/>
                    <link:roleRef xlink:type="simple" roleURI="http://www.xbrl.org/2003/role/custom"
                      xlink:href="http://www.xbrl.org/2003/role/role-2003-12-31.xsd#custom"/>
                    <xs:annotation><xs:appinfo><link:linkbase>
                      <link:arcroleRef xlink:type="simple"
                        arcroleURI="http://www.esma.europa.eu/xbrl/esef/arcrole/wider-narrower"
                        xlink:href="http://www.xbrl.org/lrr/arcrole/esma-arcrole-2018-11-21.xsd#wider-narrower"/>
                    </link:linkbase></xs:appinfo></xs:annotation>
                    </xs:schema>"#,
                sec_source.clone(),
                sec_revision.clone(),
                observed_at,
            )?,
            captured_artifact(
                &store,
                "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/company-20251231_pre.xml",
                br#"<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"/>"#,
                sec_source.clone(),
                sec_revision.clone(),
                observed_at,
            )?,
            captured_artifact(
                &store,
                "https://xbrl.fasb.org/us-gaap/2025/us-gaap-2025.xsd",
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    targetNamespace="http://fasb.org/us-gaap/2025">
                    <xs:include schemaLocation="us-types-2025.xsd"/>
                    <xs:redefine schemaLocation="us-common-2025.xsd"/>
                    <xs:import schemaLocation="us-common-2025.xsd"/>
                    <xs:import namespace="http://www.w3.org/XML/1998/namespace"
                      schemaLocation="http://www.w3.org/2001/xml.xsd"/>
                    <xs:import namespace="http://www.w3.org/1999/xlink"
                      schemaLocation="http://www.xbrl.org/2003/xlink-2003-12-31.xsd"/>
                    </xs:schema>"#,
                FASB_XBRL_TAXONOMY_AUTHORITY.canonical_source_id()?,
                FASB_XBRL_TAXONOMY_AUTHORITY.metadata_revision()?,
                observed_at,
            )?,
            captured_artifact(
                &store,
                "https://xbrl.fasb.org/us-gaap/2025/us-types-2025.xsd",
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    targetNamespace="http://fasb.org/us-gaap/2025"/>"#,
                FASB_XBRL_TAXONOMY_AUTHORITY.canonical_source_id()?,
                FASB_XBRL_TAXONOMY_AUTHORITY.metadata_revision()?,
                observed_at,
            )?,
            captured_artifact(
                &store,
                "https://www.xbrl.org/2003/role/role-2003-12-31.xsd",
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    xmlns:link="http://www.xbrl.org/2003/linkbase"
                    targetNamespace="http://www.xbrl.org/2003/role">
                    <xs:annotation><xs:appinfo>
                      <link:roleType id="custom" roleURI="http://www.xbrl.org/2003/role/custom">
                        <link:usedOn>link:presentationLink</link:usedOn>
                      </link:roleType>
                    </xs:appinfo></xs:annotation></xs:schema>"#,
                XBRL_INTERNATIONAL_STANDARDS_AUTHORITY.canonical_source_id()?,
                XBRL_INTERNATIONAL_STANDARDS_AUTHORITY.metadata_revision()?,
                observed_at,
            )?,
            captured_artifact(
                &store,
                "https://www.w3.org/2001/xml.xsd",
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    targetNamespace="http://www.w3.org/XML/1998/namespace"/>"#,
                W3C_XML_SCHEMA_STANDARDS_AUTHORITY.canonical_source_id()?,
                W3C_XML_SCHEMA_STANDARDS_AUTHORITY.metadata_revision()?,
                observed_at,
            )?,
        ];
        // Exact declarations reduced from retained MSFT 0001193125-26-323660. These
        // official components add graph evidence without adding financial facts or meanings.
        for (locator, namespace, publisher) in [
            (
                "https://www.xbrl.org/2020/extensible-enumerations-2.0.xsd",
                "http://xbrl.org/2020/extensible-enumerations-2.0",
                XBRL_INTERNATIONAL_STANDARDS_AUTHORITY,
            ),
            (
                "https://xbrl.fasb.org/srt/2025/elts/srt-2025.xsd",
                "http://fasb.org/srt/2025",
                FASB_XBRL_TAXONOMY_AUTHORITY,
            ),
            (
                "https://xbrl.sec.gov/cyd/2025/cyd-2025.xsd",
                "http://xbrl.sec.gov/cyd/2025",
                SEC_EDGAR_AUTHORITY,
            ),
            (
                "https://xbrl.sec.gov/ecd/2025/ecd-sub-2025.xsd",
                "http://xbrl.sec.gov/ecd-sub/2025",
                SEC_EDGAR_AUTHORITY,
            ),
        ] {
            let body = format!(
                r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="{namespace}"/>"#
            );
            artifacts.push(captured_artifact(
                &store,
                locator,
                body.as_bytes(),
                publisher.canonical_source_id()?,
                if publisher == SEC_EDGAR_AUTHORITY {
                    sec_revision.clone()
                } else {
                    publisher.metadata_revision()?
                },
                observed_at,
            )?);
            let (physical_locator, origin) =
                map_taxonomy_locator(filing_locator, locator, SecXbrlTaxonomyArtifactKind::Schema)?;
            let request = SecXbrlTaxonomyArtifactRequest {
                logical_locator: SourceIdentifier::try_from(locator)?,
                physical_locator,
                kind: SecXbrlTaxonomyArtifactKind::Schema,
                pinned_release: pinned_taxonomy_release(filing_locator, locator, origin)?,
                origin,
            };
            assert_eq!(request.authority()?, publisher);
        }
        // Exact registered ESMA arcrole used by the retained MSFT extension, hosted by XBRL.
        let arcrole_locator = "https://www.xbrl.org/lrr/arcrole/esma-arcrole-2018-11-21.xsd";
        artifacts.push(captured_artifact(
            &store,
            arcrole_locator,
            br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:link="http://www.xbrl.org/2003/linkbase"
                targetNamespace="http://www.esma.europa.eu/xbrl/esef/arcrole/wider-narrower">
                <xs:annotation><xs:appinfo>
                  <link:arcroleType id="wider-narrower" cyclesAllowed="undirected"
                    arcroleURI="http://www.esma.europa.eu/xbrl/esef/arcrole/wider-narrower">
                    <link:usedOn>link:definitionArc</link:usedOn>
                  </link:arcroleType>
                </xs:appinfo></xs:annotation>
                </xs:schema>"#,
            XBRL_INTERNATIONAL_STANDARDS_AUTHORITY.canonical_source_id()?,
            XBRL_INTERNATIONAL_STANDARDS_AUTHORITY.metadata_revision()?,
            observed_at,
        )?);
        let arcrole_request = SecXbrlTaxonomyArtifactRequest {
            logical_locator: SourceIdentifier::try_from(arcrole_locator)?,
            physical_locator: SourceIdentifier::try_from(arcrole_locator)?,
            kind: SecXbrlTaxonomyArtifactKind::Schema,
            pinned_release: pinned_taxonomy_release(
                filing_locator,
                arcrole_locator,
                SecXbrlTaxonomyOrigin::XbrlStandard,
            )?,
            origin: SecXbrlTaxonomyOrigin::XbrlStandard,
        };
        assert_eq!(
            arcrole_request.authority()?,
            XBRL_INTERNATIONAL_STANDARDS_AUTHORITY
        );
        assert!(
            map_taxonomy_locator(
                filing_locator,
                "https://www.esma.europa.eu/lrr/arcrole/esma-arcrole-2018-11-21.xsd",
                SecXbrlTaxonomyArtifactKind::Schema,
            )
            .is_err()
        );
        for (locator, body, authority) in [
            (
                "https://www.xbrl.org/2003/xlink-2003-12-31.xsd",
                r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="http://www.w3.org/1999/xlink"/>"#,
                XBRL_INTERNATIONAL_STANDARDS_AUTHORITY,
            ),
            (
                "https://xbrl.fasb.org/us-gaap/2025/us-common-2025.xsd",
                r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"/>"#,
                FASB_XBRL_TAXONOMY_AUTHORITY,
            ),
        ] {
            artifacts.push(captured_artifact(
                &store,
                locator,
                body.as_bytes(),
                authority.canonical_source_id()?,
                authority.metadata_revision()?,
                observed_at,
            )?);
        }
        // The closed graph must validate authored edges, not infer namespace ownership from
        // the download host. Each hostile body gets genuine capture custody before admission.
        let reject_changed =
            |locator: &str, old: &str, new: &str| -> Result<(), Box<dyn std::error::Error>> {
                let mut hostile = artifacts.clone();
                let index = hostile
                    .iter()
                    .position(|artifact| artifact.locator() == Some(locator))
                    .ok_or("fixture artifact")?;
                let original = &hostile[index];
                let body = std::str::from_utf8(original.bytes())?.replace(old, new);
                assert_ne!(body.as_bytes(), original.bytes().as_ref());
                let receipt = original.capture_receipt().ok_or("fixture receipt")?;
                hostile[index] = captured_artifact(
                    &store,
                    locator,
                    body.as_bytes(),
                    receipt.source_id().clone(),
                    receipt.metadata_revision().clone(),
                    observed_at,
                )?;
                assert!(matches!(
                    SecXbrlTaxonomyRegistry::code_owned().try_admit_captured(
                        Arc::clone(&store),
                        &sec_source,
                        &sec_revision,
                        &filing,
                        hostile,
                        SecParserLimits::production_defaults(),
                        &CancellationToken::new()
                    ),
                    Err(SecXbrlError::InvalidTaxonomySet)
                ));
                Ok(())
            };
        let extension = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/company-20251231.xsd";
        // Mismatched import, absent import namespace, include and redefine namespace mismatch.
        reject_changed(
            "https://www.xbrl.org/2003/xlink-2003-12-31.xsd",
            "http://www.w3.org/1999/xlink",
            "http://unrelated.test/xlink",
        )?;
        reject_changed(extension, "namespace=\"http://fasb.org/srt/2025\"", "")?;
        reject_changed(
            "https://xbrl.fasb.org/us-gaap/2025/us-types-2025.xsd",
            "http://fasb.org/us-gaap/2025",
            "http://fasb.org/srt/2025",
        )?;
        reject_changed(
            "https://xbrl.fasb.org/us-gaap/2025/us-common-2025.xsd",
            "<xs:schema ",
            "<xs:schema targetNamespace=\"http://unrelated.test/namespace\" ",
        )?;
        // Correct host/namespace cannot compensate for a missing, wrong-type or wrong-URI ID.
        reject_changed(arcrole_locator, "id=\"wider-narrower\"", "id=\"other\"")?;
        reject_changed(arcrole_locator, "link:arcroleType", "link:roleType")?;
        reject_changed(
            arcrole_locator,
            "<xs:annotation>",
            "<xs:annotation id=\"wider-narrower\">",
        )?;
        reject_changed(
            arcrole_locator,
            "arcroleURI=\"http://www.esma.europa.eu/xbrl/esef/arcrole/wider-narrower\"",
            "arcroleURI=\"http://unrelated.test/arcrole\"",
        )?;
        reject_changed(
            extension,
            "roleURI=\"http://www.xbrl.org/2003/role/custom\"",
            "",
        )?;
        reject_changed(extension, "#custom", "#absent")?;
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            SecTaxonomyClosure::try_start(
                &filing,
                sec_source.clone(),
                sec_revision.clone(),
                SecParserLimits::production_defaults(),
                &cancelled,
            ),
            Err(SecXbrlError::Cancelled)
        ));

        let mut captured_by_locator = artifacts
            .iter()
            .cloned()
            .map(|artifact| {
                Ok((
                    artifact
                        .locator()
                        .ok_or(SecXbrlError::InvalidTaxonomySet)?
                        .to_owned(),
                    artifact,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, SecXbrlError>>()?;
        let mut closure = SecTaxonomyClosure::try_start(
            &filing,
            sec_source.clone(),
            sec_revision.clone(),
            SecParserLimits::production_defaults(),
            &CancellationToken::new(),
        )?;
        let acquisition_cancellation = CancellationToken::new();
        let mut retained_taxonomy_bytes = 0_u64;
        while let Some(request) = closure.next_request(&acquisition_cancellation)? {
            assert_eq!(
                request.maximum_response_bytes(),
                MAX_TAXONOMY_SET_BYTES - retained_taxonomy_bytes
            );
            let artifact = captured_by_locator
                .remove(request.physical_locator())
                .ok_or(SecXbrlError::InvalidTaxonomySet)?;
            retained_taxonomy_bytes += u64::try_from(artifact.bytes().len())?;
            assert_eq!(
                artifact
                    .capture_receipt()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?
                    .source_id(),
                &request.authority()?.canonical_source_id()?
            );
            closure.accept_captured(request, artifact, &acquisition_cancellation)?;
        }
        assert!(captured_by_locator.is_empty());
        let closed_artifacts = closure.finish(&acquisition_cancellation)?;

        let admitted = SecXbrlTaxonomyRegistry::code_owned().try_admit_captured(
            Arc::clone(&store),
            &sec_source,
            &sec_revision,
            &filing,
            closed_artifacts,
            SecParserLimits::production_defaults(),
            &CancellationToken::new(),
        )?;
        assert_eq!(admitted.validated().artifacts().len(), 13);
        let parser_context = || {
            XbrlDocumentContext::new(
                SourceIdentifier::try_from("0001").expect("static accession"),
                admitted.validated().domain_set(),
                ExactPayloadEvidence::from_content_digest(filing.evidence()),
                observed_at,
            )
        };
        let source_limits = SecParserLimits::production_defaults();
        let caller_limits = SecParserLimits::try_new(filing.bytes().len(), 1, 128, 256, 4096, 1)?;
        assert_eq!(source_limits.intersect(caller_limits)?.records(), 1);
        assert!(matches!(
            super::super::XbrlDocumentParser::parse_with_cancellation(
                filing.bytes(),
                source_limits.intersect(caller_limits)?,
                parser_context(),
                &CancellationToken::new(),
            ),
            Err(SecXbrlError::RetainedOutputLimitExceeded)
        ));
        let parsed = super::super::XbrlDocumentParser::parse_with_cancellation(
            filing.bytes(),
            source_limits,
            parser_context(),
            &CancellationToken::new(),
        )?;
        assert_eq!(parsed.numeric_facts().len(), 2);
        assert_eq!(parsed.nonnumeric_occurrences().len(), 3);
        assert_eq!(parsed.footnotes().len(), 1);
        super::super::exercise_nested_continuations(parser_context())?;
        super::super::exercise_number_word_transforms(parser_context())?;
        exercise_normalized_filing_physical_restart(
            temporary.path(),
            Arc::clone(&store),
            sec_source.clone(),
            sec_revision.clone(),
            filing.clone(),
            artifacts,
        )
        .await?;

        let roles = admitted
            .validated()
            .references()
            .iter()
            .map(SecXbrlTaxonomyReference::role)
            .collect::<BTreeSet<_>>();
        assert!(
            [
                "filing_schema",
                "schema_import",
                "schema_include",
                "schema_redefine",
                "presentation_linkbase",
                "role_definition",
                "arcrole_definition",
            ]
            .into_iter()
            .all(|role| roles.contains(role))
        );
        admitted.revalidate(&CancellationToken::new())?;
        let source_revisions = admitted
            .validated()
            .artifacts()
            .iter()
            .map(|artifact| {
                (
                    artifact.source_id().as_str(),
                    artifact.metadata_revision().as_source_identifier().as_str(),
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(source_revisions.len(), 4);

        // Root authority is admitted before taxonomy parsing. This filing is intentionally not
        // XML and has no retained source-qualified representation; the representation failure must
        // win before any closure capability can exist or emit a request.
        let submissions_bytes = include_bytes!("../../fixtures/submissions-recent.json");
        let submissions_locator = crate::SecObjectLocator::submissions("0000320193")?
            .url()
            .to_owned();
        let submissions_raw = captured_artifact(
            &store,
            &submissions_locator,
            submissions_bytes,
            sec_source.clone(),
            sec_revision.clone(),
            observed_at,
        )?;
        let submissions = crate::RetrievedSubmissions::new(
            crate::SubmissionsDocument::parse(
                submissions_bytes,
                SecParserLimits::production_defaults(),
            )?,
            submissions_raw,
            Vec::new(),
        );
        let root_locator = crate::SecObjectLocator::filing_document(
            "0000320193",
            "0000320193-25-000079",
            "aapl-20250628.htm",
        )?
        .url()
        .to_owned();
        let unauthorized_root = captured_artifact(
            &store,
            &root_locator,
            b"not XML; root admission must fail before parsing",
            sec_source.clone(),
            sec_revision.clone(),
            observed_at,
        )?;
        let representations_path = temporary.path().join("root-representations");
        std::fs::create_dir(&representations_path)?;
        let empty_registry = Arc::new(crate::SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&representations_path, ambient_authority())?,
            crate::SecRepresentationLimits::production_defaults(),
        )?);
        let root_material = unauthorized_root
            .capture_material()?
            .ok_or(crate::SecClientError::InvalidCaptureMaterial)?;
        let paths = LocalPaths::prepare(temporary.path().join("sealed-root"))?;
        let sealed_store = paths.sealed_research_journal_store()?;
        let (root_expectation, root_seal_request) = root_material.into_whole_seal_parts();
        let root_token = root_expectation
            .try_rejoin(root_seal_request.seal(&sealed_store)?)?
            .try_into_whole()?;
        assert!(matches!(
            crate::extraction::admit_filing_xbrl_root_from_sealed_capture(
                root_token,
                Arc::clone(&store),
                empty_registry,
                sec_source.clone(),
                sec_revision.clone(),
                SecParserLimits::production_defaults(),
                &submissions,
                "0000320193-25-000079",
                &unauthorized_root,
                &CancellationToken::new(),
            ),
            Err(crate::SecClientError::InvalidCompositeRepresentation)
        ));

        // A provider graph cannot emit request 65: the known artifact ceiling is admitted before
        // transport, so the over-limit artifact cannot be fetched or published.
        let mut references = String::from(
            r#"<html xmlns="http://www.w3.org/1999/xhtml"
                xmlns:link="http://www.xbrl.org/2003/linkbase"
                xmlns:xlink="http://www.w3.org/1999/xlink"><head>"#,
        );
        for ordinal in 0..=MAX_TAXONOMY_ARTIFACTS {
            references.push_str(&format!(
                r#"<link:schemaRef xlink:type="simple" xlink:href="artifact-{ordinal}.xsd"/>"#
            ));
        }
        references.push_str("</head></html>");
        let wide_filing = captured_artifact(
            &store,
            filing_locator,
            references.as_bytes(),
            sec_source.clone(),
            sec_revision.clone(),
            observed_at,
        )?;
        let mut bounded = SecTaxonomyClosure::try_start(
            &wide_filing,
            sec_source.clone(),
            sec_revision.clone(),
            SecParserLimits::production_defaults(),
            &CancellationToken::new(),
        )?;
        for ordinal in 0..MAX_TAXONOMY_ARTIFACTS {
            let request = bounded
                .next_request(&acquisition_cancellation)?
                .ok_or(SecXbrlError::InvalidTaxonomySet)?;
            let artifact = captured_artifact(
                &store,
                request.physical_locator(),
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    targetNamespace="https://example.test/wide-fixture"/>"#,
                sec_source.clone(),
                sec_revision.clone(),
                Timestamp::from_unix_nanos(200 + i64::try_from(ordinal)?),
            )?;
            bounded.accept_captured(request, artifact, &acquisition_cancellation)?;
        }
        assert!(matches!(
            bounded.next_request(&acquisition_cancellation),
            Err(SecXbrlError::RecordLimitExceeded)
        ));

        // Returning cancellation before the blocking owner exits can expose a late raw or
        // representation mutation. The shared production worker boundary must retain and join
        // that owner; taxonomy persistence and final validation use the same boundary.
        let operation_cancellation = CancellationToken::new();
        let worker_cancellation = operation_cancellation.clone();
        let worker_exited = Arc::new(AtomicBool::new(false));
        let worker_exit_observation = Arc::clone(&worker_exited);
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let mut operation = tokio::spawn(async move {
            crate::client::run_joined_blocking(
                Arc::new(tokio::sync::Semaphore::new(1)),
                &worker_cancellation,
                None,
                move |worker_token| {
                    started_sender
                        .send(())
                        .map_err(|_| crate::SecClientError::BlockingWorkerFailed)?;
                    while !worker_token.is_cancelled() {
                        std::thread::yield_now();
                    }
                    cancelled_sender
                        .send(())
                        .map_err(|_| crate::SecClientError::BlockingWorkerFailed)?;
                    release_receiver
                        .recv()
                        .map_err(|_| crate::SecClientError::BlockingWorkerFailed)?;
                    worker_exit_observation.store(true, Ordering::Release);
                    Ok(())
                },
            )
            .await
        });
        started_receiver.await?;
        operation_cancellation.cancel();
        cancelled_receiver.await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut operation)
                .await
                .is_err(),
            "the cancelled operation returned before its blocking owner exited"
        );
        release_sender.send(())?;
        assert!(matches!(
            operation.await?,
            Err(crate::SecClientError::Cancelled)
        ));
        assert!(worker_exited.load(Ordering::Acquire));

        // The unreleased V1 snapshot contract is source-qualified at its schema, filename, and
        // checksum boundaries. Its production writer must be reopenable by the same current
        // reader without admitting any predecessor format.
        let restart_path = temporary.path().join("source-qualified-restart");
        std::fs::create_dir(&restart_path)?;
        let restart_registry = crate::SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&restart_path, ambient_authority())?,
            crate::SecRepresentationLimits::production_defaults(),
        )?;
        let restart_locator = "https://xbrl.fasb.org/us-gaap/2025/us-gaap-2025.xsd";
        let restart_bytes = b"source-qualified-representation";
        let restart_evidence = store.persist(restart_bytes)?;
        let restart_source = FASB_XBRL_TAXONOMY_AUTHORITY.canonical_source_id()?;
        let written = restart_registry.record_source_success_cancellable(
            &restart_source,
            restart_locator,
            restart_evidence,
            u64::try_from(restart_bytes.len())?,
            crate::SecHttpValidators::default(),
            &CancellationToken::new(),
        )?;
        drop(restart_registry);
        let snapshot_name = std::fs::read_dir(&restart_path)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find_map(|name| {
                name.into_string()
                    .ok()
                    .filter(|name| name.ends_with(".json"))
            })
            .ok_or("current representation snapshot")?;
        let snapshot_bytes = std::fs::read(restart_path.join(&snapshot_name))?;
        let mut snapshot_checksum = Sha256::new();
        snapshot_checksum.update(b"market-squawk/sec-source-qualified-representation-snapshot/v1");
        snapshot_checksum.update(u64::try_from(snapshot_bytes.len())?.to_be_bytes());
        snapshot_checksum.update(&snapshot_bytes);
        assert_eq!(
            snapshot_name,
            format!(
                "sec-source-qualified-representations-v1-{generation:020}-{digest:x}.json",
                generation = 1_u64,
                digest = snapshot_checksum.finalize(),
            )
        );
        let snapshot: serde_json::Value = serde_json::from_slice(&snapshot_bytes)?;
        assert_eq!(snapshot["schema_version"], 1);
        assert_eq!(snapshot["entries"][0]["source_id"], restart_source.as_str());
        let reopened = crate::SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&restart_path, ambient_authority())?,
            crate::SecRepresentationLimits::production_defaults(),
        )?;
        assert_eq!(
            reopened.representation_for_source(&restart_source, restart_locator)?,
            Some(written)
        );
        Ok(())
    }
}

mod occurrence_context_serde {
    use super::XbrlOccurrenceContext;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;

    pub(super) fn serialize<S: Serializer>(
        value: &Arc<XbrlOccurrenceContext>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.as_ref().serialize(serializer)
    }
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Arc<XbrlOccurrenceContext>, D::Error> {
        XbrlOccurrenceContext::deserialize(deserializer).map(Arc::new)
    }
}
