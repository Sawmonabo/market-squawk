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
const EARLIEST_SEC_TAXONOMY_YEAR: u16 = 2005;
const EARLIEST_FASB_TAXONOMY_YEAR: u16 = 2011;
const EARLIEST_LEGACY_XBRL_US_GAAP_YEAR: u16 = 2009;
const LATEST_LEGACY_XBRL_US_GAAP_YEAR: u16 = 2010;
const LATEST_CATALOGUED_TAXONOMY_YEAR: u16 = 2026;
const XBRL_STANDARD_RELEASE_YEARS: &[u16] =
    &[2003, 2005, 2006, 2008, 2013, 2014, 2016, 2017, 2021, 2023];
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
                .checked_add(artifact.source_id.as_str().len())?
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
        )?;
        validate_pinned_namespace(&request, scanned.target_namespace.as_ref())?;
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
    )?;
    validate_pinned_namespace(request, scan.target_namespace.as_ref())?;
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
}

fn scan_taxonomy_references(
    bytes: &[u8],
    expectation: TaxonomyXmlExpectation,
    parent_logical_locator: &str,
    filing_locator: &str,
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
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
    resolve_taxonomy_reference(parent_logical_locator, filing_locator, role, href, false)
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
    resolve_taxonomy_reference(
        parent_logical_locator,
        filing_locator,
        role,
        href,
        matches!(
            role,
            SecXbrlTaxonomyReferenceRole::RoleDefinition
                | SecXbrlTaxonomyReferenceRole::ArcroleDefinition
        ),
    )
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
            if segments.len() < 3 || !is_sec_taxonomy_family(segments[0]) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            let directory_release = admitted_taxonomy_release(
                segments[1],
                EARLIEST_SEC_TAXONOMY_YEAR,
                LATEST_CATALOGUED_TAXONOMY_YEAR,
            )?;
            let release = taxonomy_artifact_release(
                segments
                    .last()
                    .copied()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
                directory_release,
                EARLIEST_SEC_TAXONOMY_YEAR,
                LATEST_CATALOGUED_TAXONOMY_YEAR,
            )?;
            format!("sec-{}-{release}", segments[0])
        }
        SecXbrlTaxonomyOrigin::XbrlUsLegacyTaxonomy => {
            if !matches!(segments.as_slice(), ["us-gaap", _, ..]) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            let directory_release = admitted_taxonomy_release(
                segments[1],
                EARLIEST_LEGACY_XBRL_US_GAAP_YEAR,
                LATEST_LEGACY_XBRL_US_GAAP_YEAR,
            )?;
            let release = taxonomy_artifact_release(
                segments
                    .last()
                    .copied()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
                directory_release,
                EARLIEST_LEGACY_XBRL_US_GAAP_YEAR,
                LATEST_LEGACY_XBRL_US_GAAP_YEAR,
            )?;
            format!("xbrl-us-us-gaap-{release}")
        }
        SecXbrlTaxonomyOrigin::FasbTaxonomy => {
            if !matches!(segments.as_slice(), ["us-gaap", _, ..]) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            let directory_release = if segments[1] == "2019_with_2019_dei" {
                segments[1]
            } else {
                admitted_taxonomy_release(
                    segments[1],
                    EARLIEST_FASB_TAXONOMY_YEAR,
                    LATEST_CATALOGUED_TAXONOMY_YEAR,
                )?
            };
            let release = taxonomy_artifact_release(
                segments
                    .last()
                    .copied()
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
                directory_release,
                EARLIEST_FASB_TAXONOMY_YEAR,
                LATEST_CATALOGUED_TAXONOMY_YEAR,
            )?;
            format!("fasb-us-gaap-{release}")
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

fn validate_pinned_namespace(
    request: &SecXbrlTaxonomyArtifactRequest,
    target_namespace: Option<&SourceIdentifier>,
) -> Result<(), SecXbrlError> {
    if request.kind == SecXbrlTaxonomyArtifactKind::Linkbase {
        return if target_namespace.is_none() {
            Ok(())
        } else {
            Err(SecXbrlError::InvalidTaxonomySet)
        };
    }
    let namespace = target_namespace.ok_or(SecXbrlError::InvalidTaxonomySet)?;
    if namespace.as_str().is_empty() {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    if request.origin == SecXbrlTaxonomyOrigin::SecFiling {
        return Ok(());
    }
    let parsed = Url::parse(namespace.as_str()).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    let namespace_segments = parsed
        .path_segments()
        .ok_or(SecXbrlError::InvalidTaxonomySet)?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let release = request.pinned_release.as_str();
    let matches_release = match request.origin {
        SecXbrlTaxonomyOrigin::SecFiling => true,
        SecXbrlTaxonomyOrigin::SecTaxonomy => {
            matches!(parsed.host_str(), Some("xbrl.sec.gov" | "xbrl.us"))
                && namespace_segments.len() >= 2
                && release
                    .strip_prefix(&format!("sec-{}-", namespace_segments[0]))
                    .is_some_and(|request_release| {
                        taxonomy_releases_compatible(request_release, namespace_segments[1])
                    })
        }
        SecXbrlTaxonomyOrigin::XbrlUsLegacyTaxonomy => {
            matches!(parsed.host_str(), Some("xbrl.us" | "taxonomies.xbrl.us"))
                && namespace_segments.len() >= 2
                && namespace_segments[0] == "us-gaap"
                && release
                    .strip_prefix("xbrl-us-us-gaap-")
                    .is_some_and(|request_release| {
                        taxonomy_releases_compatible(request_release, namespace_segments[1])
                    })
        }
        SecXbrlTaxonomyOrigin::FasbTaxonomy => {
            matches!(parsed.host_str(), Some("fasb.org" | "xbrl.fasb.org"))
                && namespace_segments.len() >= 2
                && namespace_segments[0] == "us-gaap"
                && release
                    .strip_prefix("fasb-us-gaap-")
                    .is_some_and(|request_release| {
                        taxonomy_releases_compatible(request_release, namespace_segments[1])
                    })
        }
        SecXbrlTaxonomyOrigin::XbrlStandard => {
            if !matches!(parsed.host_str(), Some("www.xbrl.org" | "xbrl.org")) {
                false
            } else if let Some(core_release) = release.strip_prefix("xbrl-standard-") {
                namespace_segments.first().copied() == Some(core_release)
            } else if release.starts_with("xbrl-lrr-") {
                namespace_segments
                    .iter()
                    .any(|segment| matches!(*segment, "lrr" | "role"))
            } else if release.starts_with("xbrl-dtr-") {
                namespace_segments
                    .windows(2)
                    .any(|segments| matches!(segments, ["dtr", "type"]))
                    || matches!(namespace_segments.as_slice(), ["2009", "dtr", ..])
            } else {
                false
            }
        }
        SecXbrlTaxonomyOrigin::W3cStandard => {
            parsed.host_str() == Some("www.w3.org")
                && namespace_segments
                    .first()
                    .is_some_and(|year| release == format!("w3c-standard-{year}"))
        }
    };
    if matches_release {
        Ok(())
    } else {
        Err(SecXbrlError::InvalidTaxonomySet)
    }
}

fn filing_directory(filing: &Url) -> Result<String, SecXbrlError> {
    filing
        .path()
        .rsplit_once('/')
        .map(|(directory, _)| format!("{directory}/"))
        .ok_or(SecXbrlError::InvalidTaxonomySet)
}

fn admitted_taxonomy_release<'a>(
    value: &'a str,
    minimum: u16,
    maximum: u16,
) -> Result<&'a str, SecXbrlError> {
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
    let year = year_text
        .parse::<u16>()
        .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    if !(minimum..=maximum).contains(&year) {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(value)
}

fn taxonomy_artifact_release<'a>(
    file: &'a str,
    directory_release: &'a str,
    minimum: u16,
    maximum: u16,
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
            admitted_taxonomy_release(release, minimum, maximum)
                .ok()
                .map(|_| release)
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
            let year = release
                .parse::<u16>()
                .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
            if !XBRL_STANDARD_RELEASE_YEARS.contains(&year) {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            Ok(format!("xbrl-standard-{year}"))
        }
        ["lrr", "role", file] => Ok(format!(
            "xbrl-lrr-{}",
            dated_taxonomy_schema_release(file, 2005)?
        )),
        ["dtr", "type", release, ..] => {
            let release = release
                .strip_prefix("CR-")
                .map_or(*release, |release| release);
            if NaiveDate::parse_from_str(release, "%Y-%m-%d").is_ok() {
                Ok(format!("xbrl-dtr-{release}"))
            } else {
                Ok(format!(
                    "xbrl-dtr-{}",
                    dated_taxonomy_schema_release(release, 2009)?
                ))
            }
        }
        ["dtr", "dtr.xsd"] => Ok("xbrl-dtr-catalog".to_owned()),
        ["lrr", "lrr.xsd"] => Ok("xbrl-lrr-catalog".to_owned()),
        _ => Err(SecXbrlError::InvalidTaxonomySet),
    }
}

fn dated_taxonomy_schema_release(file: &str, minimum_year: u16) -> Result<&str, SecXbrlError> {
    let stem = file
        .strip_suffix(".xsd")
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    let release = stem
        .len()
        .checked_sub(10)
        .and_then(|offset| stem.get(offset..))
        .ok_or(SecXbrlError::InvalidTaxonomySet)?;
    NaiveDate::parse_from_str(release, "%Y-%m-%d").map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    let year = release
        .get(..4)
        .ok_or(SecXbrlError::InvalidTaxonomySet)?
        .parse::<u16>()
        .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    if !(minimum_year..=LATEST_CATALOGUED_TAXONOMY_YEAR).contains(&year) {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(release)
}

fn is_sec_taxonomy_family(value: &str) -> bool {
    matches!(
        value,
        "cef"
            | "country"
            | "currency"
            | "dei"
            | "ecd"
            | "exch"
            | "invest"
            | "naics"
            | "oef"
            | "rr"
            | "rxp"
            | "sic"
            | "spac"
            | "srt"
            | "stpr"
            | "vip"
    )
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XbrlNonnumericOccurrence {
    pub(super) occurrence_id: SourceIdentifier,
    pub(super) accession: SourceIdentifier,
    pub(super) concept: XbrlQualifiedName,
    pub(super) context_id: SourceIdentifier,
    pub(super) lexical_value: XbrlText,
    pub(super) nil: bool,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) occurrence_relationships: XbrlOccurrenceRelationships,
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

/// Parsed XBRL output preserving numeric and nonnumeric occurrence families separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedXbrlDocument {
    pub(super) accession: SourceIdentifier,
    pub(super) expected_cik: Option<SourceIdentifier>,
    pub(super) taxonomy_set: XbrlTaxonomySet,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) evaluated_at: Timestamp,
    pub(super) retained_output_upper_bound: usize,
    pub(super) numeric_facts: Vec<XbrlNumericFact>,
    pub(super) nonnumeric_occurrences: Vec<XbrlNonnumericOccurrence>,
}

impl ParsedXbrlDocument {
    pub(crate) fn matches_document_context(
        &self,
        accession: &SourceIdentifier,
        expected_cik: &SourceIdentifier,
        taxonomy_set: &SecValidatedXbrlTaxonomySet,
        source_payload: EvidenceDigest,
    ) -> bool {
        &self.accession == accession
            && self.expected_cik.as_ref() == Some(expected_cik)
            && self.taxonomy_set == taxonomy_set.domain_set()
            && self.source_payload.content_digest() == source_payload
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) fn into_families(
        self,
    ) -> (Vec<XbrlNumericFact>, Vec<XbrlNonnumericOccurrence>, usize) {
        (
            self.numeric_facts,
            self.nonnumeric_occurrences,
            self.retained_output_upper_bound,
        )
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
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use cap_std::{ambient_authority, fs::Dir};
    use market_squawk_domain::{EffectiveInterval, ExactPayloadEvidence};
    use market_squawk_platform::LocalPaths;
    use market_squawk_sources::{
        DiscoveryRequest, ExtractionBatch, ExtractionRecord, ExtractionRequest,
        ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
        ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
        SealedProviderCaptureBinding, SourceObject, SourceObjectCaptureIdentity,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn captured_taxonomy_closes_one_mixed_source_graph_and_honors_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let store = Arc::new(RawEvidenceStore::new(Dir::open_ambient_dir(
            temporary.path(),
            ambient_authority(),
        )?));
        let observed_at = Timestamp::from_unix_nanos(100);
        let sec_source = SEC_EDGAR_AUTHORITY.canonical_source_id()?;
        let sec_revision = MetadataRevision::new(SourceIdentifier::try_from("sec-test-v1")?);
        let filing_locator = "https://www.sec.gov/Archives/edgar/data/1/0001/company-20251231.htm";
        let filing = captured_artifact(
            &store,
            filing_locator,
            br#"<html xmlns="http://www.w3.org/1999/xhtml"
                xmlns:link="http://www.xbrl.org/2003/linkbase"
                xmlns:xlink="http://www.w3.org/1999/xlink"><head>
                <link:schemaRef xlink:type="simple" xlink:href="company-20251231.xsd"/>
                </head></html>"#,
            sec_source.clone(),
            sec_revision.clone(),
            observed_at,
        )?;
        let artifacts = vec![
            captured_artifact(
                &store,
                "https://www.sec.gov/Archives/edgar/data/1/0001/company-20251231.xsd",
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    xmlns:link="http://www.xbrl.org/2003/linkbase"
                    xmlns:xlink="http://www.w3.org/1999/xlink"
                    targetNamespace="https://example.test/company/2025">
                    <xs:import namespace="http://fasb.org/us-gaap/2025"
                      schemaLocation="http://xbrl.fasb.org/us-gaap/2025/us-gaap-2025.xsd"/>
                    <link:linkbaseRef xlink:type="simple"
                      xlink:role="http://www.xbrl.org/2003/role/presentationLinkbase"
                      xlink:href="company-20251231_pre.xml"/>
                    <link:roleRef xlink:type="simple"
                      xlink:href="http://www.xbrl.org/2003/role/role-2003-12-31.xsd#custom"/>
                    </xs:schema>"#,
                sec_source.clone(),
                sec_revision.clone(),
                observed_at,
            )?,
            captured_artifact(
                &store,
                "https://www.sec.gov/Archives/edgar/data/1/0001/company-20251231_pre.xml",
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
                    <xs:import namespace="http://www.w3.org/2001/XMLSchema"
                      schemaLocation="http://www.w3.org/2001/xml.xsd"/>
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
                    targetNamespace="http://www.xbrl.org/2003/role"/>"#,
                XBRL_INTERNATIONAL_STANDARDS_AUTHORITY.canonical_source_id()?,
                XBRL_INTERNATIONAL_STANDARDS_AUTHORITY.metadata_revision()?,
                observed_at,
            )?,
            captured_artifact(
                &store,
                "https://www.w3.org/2001/xml.xsd",
                br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    targetNamespace="http://www.w3.org/2001/XMLSchema"/>"#,
                W3C_XML_SCHEMA_STANDARDS_AUTHORITY.canonical_source_id()?,
                W3C_XML_SCHEMA_STANDARDS_AUTHORITY.metadata_revision()?,
                observed_at,
            )?,
        ];
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
            .into_iter()
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
        let artifacts = closure.finish(&acquisition_cancellation)?;

        let admitted = SecXbrlTaxonomyRegistry::code_owned().try_admit_captured(
            Arc::clone(&store),
            &sec_source,
            &sec_revision,
            &filing,
            artifacts,
            SecParserLimits::production_defaults(),
            &CancellationToken::new(),
        )?;
        assert_eq!(admitted.validated().artifacts().len(), 6);
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
                "presentation_linkbase",
                "role_definition",
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
        let root_capture_identity =
            SourceObjectCaptureIdentity::try_from_capture(root_material.receipt())?;
        let root_discovery = DiscoveryRequest::try_new(
            SourceIdentifier::try_from(root_locator.as_str())?,
            None,
            NonZeroU16::MIN,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let root_object = SourceObject::try_new_with_capture_identity(
            sec_source.clone(),
            sec_revision.clone(),
            &root_discovery,
            SourceIdentifier::try_from(root_locator.as_str())?,
            SourceIdentifier::try_from("application/xhtml+xml")?,
            ExactPayloadEvidence::from_content_digest(unauthorized_root.evidence()),
            root_capture_identity,
            EffectiveInterval::new(observed_at, None)?,
            None,
            market_squawk_sources::AvailabilityEvidence::LocalFirstObserved { observed_at },
            Some(u64::try_from(unauthorized_root.bytes().len())?),
        )?;
        let root_request = ExtractionRequest::try_new(
            root_object,
            NonZeroU32::MIN,
            NonZeroU64::new(1_000_000).ok_or("root byte bound")?,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let root_payload = Bytes::copy_from_slice(unauthorized_root.bytes());
        let root_batch = ExtractionBatch::try_new(
            &root_request,
            vec![ExtractionRecord::try_new(
                &root_request,
                SourceIdentifier::try_from("sec-filing-root-v1")?,
                ExactPayloadEvidence::from_content_digest(unauthorized_root.evidence()),
                observed_at,
                None,
                market_squawk_sources::AvailabilityEvidence::LocalFirstObserved { observed_at },
                SourceIdentifier::try_from("root-r1")?,
                None,
                root_payload,
            )?],
        )?;
        let mut native = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::SecEdgarV1,
            &root_batch,
        )?;
        native.try_push(&serde_json::json!({"kind": "sec-filing-root-v1"}))?;
        let root_native_lineage = native.finish()?;
        let paths = LocalPaths::prepare(temporary.path().join("sealed-root"))?;
        let sealed_store = paths.sealed_research_journal_store()?;
        let (root_expectation, root_seal_request) = root_material.into_whole_seal_parts();
        let root_token = root_expectation
            .try_rejoin(root_seal_request.seal(&sealed_store)?)?
            .try_into_whole()?;
        let sealed_root = SealedProviderCaptureBinding::try_whole(
            root_token,
            root_batch,
            root_native_lineage,
            vec![0],
        )?;
        assert!(matches!(
            crate::extraction::admit_filing_xbrl_root_from_sealed_binding(
                sealed_root,
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
        Ok(())
    }
}
